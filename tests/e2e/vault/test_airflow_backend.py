"""tests/e2e/vault/test_airflow_backend.py -- SEC-05 live proof, per ROADMAP SC1's literal wording.

ROADMAP Phase 5 success criterion 1: "With the Airflow metadata-DB connection
deleted from the Airflow database and every `AIRFLOW_CONN_*` unset, DAGs
still resolve their connections and run -- proving the Vault backend
actually served them." This module proves all four clauses, in order:

  1. `AIRFLOW_CONN_MINIO_DEFAULT` is gone from every live Airflow component's
     pod spec -- not just the three `helm/values/*/airflow.yaml` files this
     plan edited, the RUNNING cluster.
  2. No `minio_default` row exists in the metadata database's own
     `connection` table -- it was never created by this plan; `VaultBackend`
     is Airflow's documented "fails open at parse time" secrets backend
     (05-RESEARCH.md), so this is not vacuous -- if `VaultBackend` were
     silently broken, `airflow connections get` would raise, not fall back.
  3. `airflow connections get minio_default` still succeeds and names the
     real MinIO endpoint -- with (1) and (2) both true, the ONLY remaining
     source is Vault.
  4. The DAG itself -- not just a CLI probe -- resolves the SAME connection
     through its deferred `S3KeySensor` and runs a real file to a terminal,
     non-FAILED `meta.ingestion_runs` status. This is the "DAGs still...
     run" clause: a CLI success alone would not prove the deferred trigger
     path (which 05-03-PLAN.md's own Interfaces section names as the
     genuinely ambiguous case) resolves anything at all.

Every fixture below is IMPORTED, not redefined: `s3_client` and
`metadata_connection` come from `tests/e2e/cluster/` (the latter is itself a
plain module-level fixture in `test_airflow_workloads.py`, not a conftest,
but pytest resolves an imported `@pytest.fixture` object identically
regardless of which module defines it); `analytics_owner_connection`,
`poll_file_discovered`, `poll_run_for_file` and `poll_ingestion_run` come
from `tests/e2e/slice/conftest.py`. Deliberately NOT imported:
`analytics_connection` (the `etl_app`-role fixture) -- it reads the
`csv-processor-db` Kubernetes Secret plan 05-02 already deleted from the
live cluster, so importing it here would import a fixture guaranteed to
fail at setup (see `.planning/phases/05-vault-secrets-workload-identity/
deferred-items.md`, the `tests/e2e/slice/` architectural gap this plan does
NOT fix). `analytics_owner_connection` (the CNPG-generated
`analytics-db-app` Secret) was never touched by any Vault migration and
reads fine.

This module never triggers `csv_ingest_customers` through Airflow's CLI
(matching `scripts/ingest-demo.py`'s own D-15 prohibition and
`tests/e2e/slice/`'s established convention): it uploads a real file and
polls `meta.files`/`meta.ingestion_runs`, letting the DAG's own schedule +
deferred sensor notice and process it unattended -- the only way to
actually prove "DAGs still resolve their connections and run" rather than a
shortcut around the very sensor this plan's fix concerns.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

import pytest

from tests.e2e.cluster.conftest import s3_client  # noqa: F401 -- re-exported as a pytest fixture
from tests.e2e.cluster.test_airflow_workloads import (  # noqa: F401 -- re-exported as a pytest fixture
    metadata_connection,
)
from tests.e2e.slice.conftest import (  # noqa: F401 -- analytics_owner_connection re-exported as a fixture
    analytics_owner_connection,
    poll_file_discovered,
    poll_ingestion_run,
    poll_run_for_file,
)

if TYPE_CHECKING:
    import subprocess
    from collections.abc import Callable

    import psycopg

pytestmark = pytest.mark.cluster

NAMESPACE = "airflow"
_CUSTOMERS_DATASET = "customers"
_DAG_ID = "csv_ingest_customers"

# These two tuples list only the components whose object kind is FIXED
# across both profiles (matching tests/e2e/cluster/test_airflow_workloads.
# py's own EXPECTED_DEPLOYMENTS/EXPECTED_STATEFULSETS). airflow-scheduler is
# deliberately absent from both: the official Airflow chart renders it as a
# Deployment under local's KubernetesExecutor profile but as a StatefulSet
# under CI's LocalExecutor profile, so it is checked separately below via
# the live-detection helper `_scheduler_kind` -- mirroring the sibling fix
# in tests/e2e/chaos/test_vault_unavailable.py's own `_scheduler_resource_ref`.
# A `PROFILE` environment-variable read is not used instead: `PROFILE` never
# propagates from `make cluster-up` into `make chaos-verify`'s pytest
# process, so reading it here would silently default to "local" even on a
# genuinely CI-profile cluster. The fourth identity this plan's Vault role
# binds, airflow-worker, has no persistent object of its own (KubernetesExecutor
# task-instance pods are ephemeral); it is checked separately below, via the
# rendered pod_template_file.yaml.
_DEPLOYMENTS = ("airflow-api-server", "airflow-dag-processor")
_STATEFULSETS = ("airflow-triggerer",)

# The single source of truth for the scheduler's object name -- referenced by
# `_scheduler_kind`'s two probes and by the test's own follow-up
# `kubectl_json` call below, so the literal is written exactly once in this
# module.
_SCHEDULER_NAME = "airflow-scheduler"

_DISCOVERY_TIMEOUT_SECONDS = 180
_INGEST_TIMEOUT_SECONDS = 180


def _scheduler_kind(
    kubectl_fn: Callable[..., subprocess.CompletedProcess[str]],
) -> str:
    """Live-probe which object kind the connected cluster actually rendered for airflow-scheduler.

    Returns a bare kind string (`"deployment"` or `"statefulset"`), not a
    `kind/name` ref, because this module's own loop
    (`test_airflow_conn_minio_default_is_absent_from_every_component`)
    already keys on `kind` and `name` as separate positional arguments to
    `kubectl_json`. Contrast this with `tests/e2e/chaos/
    test_vault_unavailable.py`'s own `_scheduler_resource_ref`, which
    returns a combined `"deploy/airflow-scheduler"` / `"statefulset/
    airflow-scheduler"` ref string for a different call shape (`kubectl exec
    <ref>`).

    Args:
        kubectl_fn: The `kubectl` fixture callable.

    Returns:
        `"deployment"` if a Deployment named `airflow-scheduler` exists in
        the `airflow` namespace, else `"statefulset"` if a StatefulSet of
        that name exists instead.

    Raises:
        AssertionError: neither a Deployment nor a StatefulSet named
            `airflow-scheduler` exists in the `airflow` namespace on this
            cluster.
    """
    deploy_probe = kubectl_fn(
        "-n", NAMESPACE, "get", "deployment", _SCHEDULER_NAME,
        "-o", "name", "--ignore-not-found",
    )
    if deploy_probe.returncode == 0 and deploy_probe.stdout.strip():
        return "deployment"
    sts_probe = kubectl_fn(
        "-n", NAMESPACE, "get", "statefulset", _SCHEDULER_NAME,
        "-o", "name", "--ignore-not-found",
    )
    if sts_probe.returncode == 0 and sts_probe.stdout.strip():
        return "statefulset"
    msg = (
        "airflow-scheduler exists as neither a Deployment nor a StatefulSet in the airflow "
        "namespace on this cluster"
    )
    raise AssertionError(msg)


def test_airflow_conn_minio_default_is_absent_from_every_component(
    kubectl_json: Callable[..., Any],
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    """D-01/SEC-05: no live Airflow component still carries the retired secretKeyRef env var."""
    offending: list[str] = []
    for kind, names in (("deployment", _DEPLOYMENTS), ("statefulset", _STATEFULSETS)):
        for name in names:
            obj = kubectl_json("-n", NAMESPACE, "get", kind, name)
            containers = obj["spec"]["template"]["spec"]["containers"]
            env_names = {e["name"] for c in containers for e in c.get("env", [])}
            if "AIRFLOW_CONN_MINIO_DEFAULT" in env_names:
                offending.append(f"{kind}/{name}")

    scheduler_kind = _scheduler_kind(kubectl)
    scheduler_obj = kubectl_json("-n", NAMESPACE, "get", scheduler_kind, _SCHEDULER_NAME)
    scheduler_containers = scheduler_obj["spec"]["template"]["spec"]["containers"]
    scheduler_env_names = {e["name"] for c in scheduler_containers for e in c.get("env", [])}
    if "AIRFLOW_CONN_MINIO_DEFAULT" in scheduler_env_names:
        offending.append(f"{scheduler_kind}/{_SCHEDULER_NAME}")

    # airflow-worker's own pod TEMPLATE (no persistent Deployment/
    # StatefulSet of its own) lives inside the airflow-config ConfigMap's
    # rendered pod_template_file.yaml.
    config_map = kubectl_json("-n", NAMESPACE, "get", "configmap", "airflow-config")
    pod_template_text = config_map["data"].get("pod_template_file.yaml", "")
    if "AIRFLOW_CONN_MINIO_DEFAULT" in pod_template_text:
        offending.append("configmap/airflow-config (pod_template_file.yaml -- airflow-worker)")

    assert not offending, (
        f"AIRFLOW_CONN_MINIO_DEFAULT still present on: {offending} -- SEC-05 requires every "
        "AIRFLOW_CONN_* to be unset, not just the metadata DB row deleted"
    )


def test_no_minio_default_row_in_the_metadata_database(
    metadata_connection: psycopg.Connection[Any],  # noqa: F811 -- pytest fixture-injection param name, not a real redefinition
) -> None:
    """SEC-05: no `connection` row named minio_default -- only VaultBackend can be serving it."""
    with metadata_connection.cursor() as cur:
        cur.execute("SELECT conn_id FROM connection WHERE conn_id = %s", ("minio_default",))
        rows = cur.fetchall()
    assert rows == [], f"a minio_default row exists in the metadata database: {rows}"


def test_airflow_connections_get_resolves_minio_default_through_vault_alone(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    """With no env var and no DB row (both proven above), only VaultBackend can serve this."""
    proc = kubectl(
        "-n",
        NAMESPACE,
        "exec",
        "deploy/airflow-api-server",
        "--",
        "airflow",
        "connections",
        "get",
        "minio_default",
        "-o",
        "json",
    )
    assert proc.returncode == 0, (
        f"airflow connections get minio_default failed (exit {proc.returncode}):\n{proc.stderr}"
    )
    assert "minio.data.svc.cluster.local" in proc.stdout, (
        f"resolved connection does not mention the MinIO endpoint host: {proc.stdout!r}"
    )


def test_dag_still_resolves_its_connection_and_runs(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    s3_client: Callable[[str], Any],  # noqa: F811 -- pytest fixture-injection param name, not a real redefinition
    analytics_owner_connection: psycopg.Connection[Any],  # noqa: F811 -- same reasoning as s3_client above
) -> None:
    """ROADMAP SC1, verbatim: DAGs still resolve their connections and run.

    Uploads a fresh, uniquely-marked file to `s3://raw/customers/` and lets
    the REAL, already-scheduled `csv_ingest_customers` DAG notice it through
    its own deferred `S3KeySensor` (never triggered directly -- see module
    docstring), run `discover` and `ingest`, and reach a terminal
    `meta.ingestion_runs` status. A non-FAILED terminal status is only
    reachable if the sensor's OWN poke -- inside whichever of
    airflow-worker (first, synchronous poke) or airflow-triggerer
    (subsequent, deferred polls) actually ran it -- resolved `minio_default`
    successfully, which (per the three tests above) can only have happened
    through `VaultBackend`.
    """
    unpause = kubectl(
        "-n",
        NAMESPACE,
        "exec",
        "deploy/airflow-api-server",
        "--",
        "airflow",
        "dags",
        "unpause",
        _DAG_ID,
    )
    assert unpause.returncode == 0, f"airflow dags unpause {_DAG_ID} failed:\n{unpause.stderr}"

    marker = uuid.uuid4().hex[:12]
    key = f"customers/e2e-sec05-{marker}.csv"
    object_uri = f"s3://raw/{key}"
    payload = (
        "customer_id,name,country,birth_date,event_ts\n"
        f"900001,SEC-05 Probe {marker},US,1990-01-01,2026-01-01T00:00:00Z\n"
    ).encode()

    app = s3_client("app")
    app.put_object(Bucket="raw", Key=key, Body=payload)

    file_row = poll_file_discovered(
        analytics_owner_connection,
        dataset=_CUSTOMERS_DATASET,
        object_uri=object_uri,
        timeout=_DISCOVERY_TIMEOUT_SECONDS,
    )
    run_row = poll_run_for_file(
        analytics_owner_connection,
        file_id=file_row["file_id"],
        timeout=30,
    )
    outcome = poll_ingestion_run(
        analytics_owner_connection,
        run_row["idempotency_key"],
        timeout=_INGEST_TIMEOUT_SECONDS,
    )
    assert outcome["status"] != "FAILED", (
        f"csv_ingest_customers run for {object_uri} finished FAILED -- the deferred "
        "S3KeySensor's own Vault-backed connection resolution (or a downstream step) broke"
    )
    assert outcome["status"] == "SUCCEEDED", (
        f"expected a SUCCEEDED terminal status for {object_uri}, got {outcome['status']!r}"
    )
