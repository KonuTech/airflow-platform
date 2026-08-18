"""tests/e2e/observability/test_trace_propagation.py -- OBS-10's live, end-to-end trace-ID proof.

D-12/OBS-10: a real `csv_ingest_customers` DagRun's mapped `stage`
KubernetesPodOperator task (08.1-12: the trace root, replacing the old
single `ingest` task) launches a pod carrying a well-formed W3C
`TRACEPARENT` env var (`airflow/dags/_common/tracing_kpo.py`'s
`TracingKubernetesPodOperator`), and the trace-ID segment inside it equals
`meta.ingestion_runs.trace_id` for the SAME run -- proving OBS-10's whole
propagation chain (Airflow task span -> pod env var -> `dataplat.cli`
extraction -> `pipeline.run_ingest`'s own child span -> persisted
`trace_id`) actually works live, not just in isolated unit tests.

Reuses `tests/e2e/slice/`'s own established file-drop-and-poll mechanism
(mirrors `test_smoke_and_idempotency.py`'s shape: upload a small,
uniquely-named CSV to `s3://raw/customers/`, letting the already-unpaused
`csv_ingest_customers` DAG's `S3KeySensor -> discover -> stage` chain run
naturally -- no second triggering mechanism invented).

**The deletion race, and why it is survivable:**
`airflow/dags/_common/kpo.py`'s `common_kpo_kwargs()` sets
`on_finish_action: "delete_succeeded_pod"`, so the launched `stage` pod is
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
    poll_lineage_dag_context,
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


def test_ingest_pod_dag_context_matches_persisted_lineage_row(
    kubectl: Callable[..., Any],
    kubectl_json: Callable[..., Any],
    s3_client: Callable[[str], Any],
    analytics_connection: psycopg.Connection[Any],
) -> None:
    """OBS-07 gap closure (07-09): a real, live, Airflow-triggered ingest pod's
    AIRFLOW_CTX_* env vars match meta.ingestion_runs' persisted dag/run/task
    identity, AND that same identity shows up non-NULL in
    meta.v_customers_lineage -- the VIEW itself, the literal OBS-07 surface --
    for the row this run just published.

    Never asserts only one side of any comparison: the pod-spec env vars, the
    `meta.ingestion_runs` row `poll_trace_claimed` returns, and the
    `meta.v_customers_lineage` row `poll_lineage_dag_context` returns are all
    three independent live sources (the Kubernetes API and two separate
    queries against the analytical database), matching this file's own
    established "both independent live sources" convention.
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
    key = f"customers/e2e-dag-context-{uuid.uuid4().hex[:12]}.csv"
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

    pod = _capture_pod_before_deletion(kubectl_json, namespace=_ETL_NAMESPACE, name=pod_name)

    containers = pod["spec"]["containers"]
    assert containers, f"pod {pod_name!r} has no containers in its captured spec"
    env_vars = {entry["name"]: entry.get("value") for entry in containers[0].get("env", [])}

    for env_name in (
        "AIRFLOW_CTX_DAG_ID",
        "AIRFLOW_CTX_TASK_ID",
        "AIRFLOW_CTX_DAG_RUN_ID",
        "AIRFLOW_CTX_MAP_INDEX",
        "AIRFLOW_CTX_K8S_NAMESPACE",
    ):
        assert env_name in env_vars, (
            f"pod {pod_name!r}'s first container has no {env_name} env var -- captured env "
            f"names: {sorted(env_vars)}"
        )

    assert env_vars["AIRFLOW_CTX_DAG_ID"] == claimed["dag_id"], (
        f"pod env AIRFLOW_CTX_DAG_ID ({env_vars['AIRFLOW_CTX_DAG_ID']!r}) does not match "
        f"meta.ingestion_runs.dag_id ({claimed['dag_id']!r}) for the same run "
        f"(file_id={file_row['file_id']!r}, pod={pod_name!r})"
    )
    assert env_vars["AIRFLOW_CTX_TASK_ID"] == claimed["task_id"], (
        f"pod env AIRFLOW_CTX_TASK_ID ({env_vars['AIRFLOW_CTX_TASK_ID']!r}) does not match "
        f"meta.ingestion_runs.task_id ({claimed['task_id']!r}) for the same run "
        f"(file_id={file_row['file_id']!r}, pod={pod_name!r})"
    )
    assert env_vars["AIRFLOW_CTX_DAG_RUN_ID"] == claimed["dag_run_id"], (
        f"pod env AIRFLOW_CTX_DAG_RUN_ID ({env_vars['AIRFLOW_CTX_DAG_RUN_ID']!r}) does not "
        f"match meta.ingestion_runs.dag_run_id ({claimed['dag_run_id']!r}) for the same run "
        f"(file_id={file_row['file_id']!r}, pod={pod_name!r})"
    )

    outcome = poll_ingestion_run(
        analytics_connection,
        file_id=file_row["file_id"],
        timeout=_RUN_TERMINAL_TIMEOUT_SECONDS,
    )
    assert outcome["status"] == "SUCCEEDED", (
        f"ingestion run for file_id={file_row['file_id']!r} finished {outcome['status']!r}, "
        f"not SUCCEEDED, even though its AIRFLOW_CTX_*/dag context already matched"
    )

    # The decisive proof: meta.v_customers_lineage itself -- not merely
    # meta.ingestion_runs -- shows non-NULL dag_id/dag_run_id/task_id for a
    # genuinely live, Airflow-triggered run (07-VERIFICATION.md's own gap).
    lineage = poll_lineage_dag_context(analytics_connection, run_id=claimed["run_id"])
    assert lineage["dag_id"] == claimed["dag_id"] == _CUSTOMERS_DAG_ID
    assert lineage["dag_run_id"] == claimed["dag_run_id"]
    assert lineage["task_id"] == claimed["task_id"] == "stage"
    assert lineage["map_index"] is not None
    assert lineage["k8s_namespace"] == _ETL_NAMESPACE
