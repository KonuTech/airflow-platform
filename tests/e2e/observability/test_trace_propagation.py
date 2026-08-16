"""tests/e2e/observability/test_trace_propagation.py -- OBS-10's live, end-to-end trace-ID proof.

D-12/OBS-10: a real `csv_ingest_customers` DagRun's mapped `ingest`
KubernetesPodOperator task launches a pod carrying a well-formed W3C
`TRACEPARENT` env var (`airflow/dags/_common/tracing_kpo.py`'s
`TracingKubernetesPodOperator`), and the trace-ID segment inside it equals
`meta.ingestion_runs.trace_id` for the SAME run -- proving OBS-10's whole
propagation chain (Airflow task span -> pod env var -> `dataplat.cli`
extraction -> `pipeline.run_ingest`'s own child span -> persisted
`trace_id`) actually works live, not just in isolated unit tests.

Reuses `tests/e2e/slice/`'s own established file-drop-and-poll mechanism
(mirrors `test_smoke_and_idempotency.py`'s shape: upload a small,
uniquely-named CSV to `s3://raw/customers/`, letting the already-unpaused
`csv_ingest_customers` DAG's `S3KeySensor -> discover -> ingest` chain run
naturally -- no second triggering mechanism invented).

**The deletion race, and why it is survivable:**
`airflow/dags/_common/kpo.py`'s `common_kpo_kwargs()` sets
`on_finish_action: "delete_succeeded_pod"`, so the launched `ingest` pod is
deleted shortly after it succeeds -- capturing its spec is a genuine race.
This module wins that race structurally, not by luck:
`dataplat.pipeline.run.run_ingest` writes `trace_id`/`span_id`/
`k8s_pod_name` together in ONE `claim_ingestion_run` UPDATE, near the START
of a run (`packages/dataplat/src/dataplat/pipeline/run.py`), BEFORE any
staging/publish work happens. The moment `poll_trace_claimed` (this
package's own `conftest.py`) observes that row, the pod is therefore very
likely still `Running` -- `_capture_pod_before_deletion` below then
tight-polls `kubectl get pod ... -o json` to catch it before
`on_finish_action` removes it. A genuinely-missed capture raises a named,
diagnostic `AssertionError` distinguishing "test infrastructure lost the
race" from "TRACEPARENT propagation itself is broken", per 07-08-PLAN.md's
own Task 2 acceptance criteria.
"""

from __future__ import annotations

import re
import time
import uuid
from typing import TYPE_CHECKING, Any

import pytest

from tests.e2e.observability.conftest import (
    poll_file_discovered,
    poll_ingestion_run,
    poll_trace_claimed,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    import psycopg

pytestmark = pytest.mark.cluster

_CUSTOMERS_DAG_ID = "csv_ingest_customers"
_CUSTOMERS_DATASET = "customers"
_ETL_NAMESPACE = "etl"

_DISCOVERY_TIMEOUT_SECONDS = 180
_TRACE_CLAIM_TIMEOUT_SECONDS = 180
_RUN_TERMINAL_TIMEOUT_SECONDS = 180

_POD_CAPTURE_TIMEOUT_SECONDS = 30
_POD_CAPTURE_POLL_INTERVAL_SECONDS = 0.2

# W3C Trace Context `traceparent` header format: version-traceid-parentid-flags.
# https://www.w3.org/TR/trace-context/#traceparent-header-field-values
_TRACEPARENT_RE = re.compile(r"^00-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}$")


def _unique_small_csv_bytes() -> bytes:
    """A tiny, uniquely-marked customers CSV -- never collides with a prior run's content hash."""
    marker = uuid.uuid4().hex[:16]
    return (
        "customer_id,name,country,birth_date,event_ts\n"
        f"900201,E2E-TRACE-{marker},PL,1990-01-01,2026-01-01T00:00:00Z\n"
    ).encode()


def _capture_pod_before_deletion(
    kubectl_json_fn: Callable[..., Any],
    *,
    namespace: str,
    name: str,
    timeout: float = _POD_CAPTURE_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """`kubectl get pod <name> -o json`, tight-polled to race `on_finish_action`'s deletion."""
    deadline = time.monotonic() + timeout
    last_error: str | None = None
    while time.monotonic() < deadline:
        try:
            return kubectl_json_fn("-n", namespace, "get", "pod", name)
        except AssertionError as exc:
            last_error = str(exc)
            time.sleep(_POD_CAPTURE_POLL_INTERVAL_SECONDS)
    msg = (
        f"could not capture pod spec for {namespace}/{name} within {timeout}s -- the pod was "
        f"very likely already deleted by on_finish_action=delete_succeeded_pod before this "
        f"test could observe its spec (a test-infrastructure timing gap, not evidence "
        f"TRACEPARENT propagation itself is broken). Last kubectl error: {last_error}"
    )
    raise AssertionError(msg)


def test_ingest_pod_traceparent_matches_persisted_trace_id(
    kubectl: Callable[..., Any],
    kubectl_json: Callable[..., Any],
    s3_client: Callable[[str], Any],
    analytics_connection: psycopg.Connection[Any],
) -> None:
    """D-12/OBS-10: a real ingest pod's TRACEPARENT trace-id segment == the persisted trace_id.

    Never asserts only one side: both the pod-spec `TRACEPARENT` and the
    persisted `trace_id` are read from independent live sources (the
    Kubernetes API and the analytical database) and compared directly.
    """
    unpause = kubectl(
        "-n",
        "airflow",
        "exec",
        "deploy/airflow-api-server",
        "--",
        "airflow",
        "dags",
        "unpause",
        _CUSTOMERS_DAG_ID,
    )
    assert unpause.returncode == 0, f"airflow dags unpause failed:\n{unpause.stderr}"

    app = s3_client("app")
    key = f"customers/e2e-trace-propagation-{uuid.uuid4().hex[:12]}.csv"
    object_uri = f"s3://raw/{key}"
    app.put_object(Bucket="raw", Key=key, Body=_unique_small_csv_bytes())

    file_row = poll_file_discovered(
        analytics_connection,
        dataset=_CUSTOMERS_DATASET,
        object_uri=object_uri,
        timeout=_DISCOVERY_TIMEOUT_SECONDS,
    )

    claimed = poll_trace_claimed(
        analytics_connection,
        file_id=file_row["file_id"],
        timeout=_TRACE_CLAIM_TIMEOUT_SECONDS,
    )
    pod_name = claimed["k8s_pod_name"]
    persisted_trace_id = claimed["trace_id"]

    pod = _capture_pod_before_deletion(kubectl_json, namespace=_ETL_NAMESPACE, name=pod_name)

    containers = pod["spec"]["containers"]
    assert containers, f"pod {pod_name!r} has no containers in its captured spec"
    env_vars = {entry["name"]: entry.get("value") for entry in containers[0].get("env", [])}
    assert "TRACEPARENT" in env_vars, (
        f"pod {pod_name!r}'s first container has no TRACEPARENT env var -- captured env names: "
        f"{sorted(env_vars)}"
    )
    traceparent = env_vars["TRACEPARENT"]

    match = _TRACEPARENT_RE.match(traceparent or "")
    assert match is not None, (
        f"TRACEPARENT {traceparent!r} does not match the W3C format 00-<32 hex>-<16 hex>-<2 hex>"
    )
    pod_trace_id = match.group(1)

    assert pod_trace_id == persisted_trace_id, (
        f"the ingest pod's TRACEPARENT trace-id segment ({pod_trace_id!r}) does not match "
        f"meta.ingestion_runs.trace_id ({persisted_trace_id!r}) for the same run "
        f"(file_id={file_row['file_id']!r}, pod={pod_name!r})"
    )

    outcome = poll_ingestion_run(
        analytics_connection,
        file_id=file_row["file_id"],
        timeout=_RUN_TERMINAL_TIMEOUT_SECONDS,
    )
    assert outcome["status"] == "SUCCEEDED", (
        f"ingestion run for file_id={file_row['file_id']!r} finished {outcome['status']!r}, "
        f"not SUCCEEDED, even though its TRACEPARENT/trace_id already matched"
    )
