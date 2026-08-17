"""``csv_ingest_customers`` -- the vertical slice DAG (ORCH-01..09, D-01..D-04, D-15, D-18).

Thin orchestration only (README Sec 6.4/68): every line below either builds
a Kubernetes API object, wires task dependencies, or logs a scalar summary --
parsing, validation, typing and every DB write happen inside the
``csv-processor`` image's ``dataplat discover``/``dataplat ingest`` CLI
commands, launched only via ``KubernetesPodOperator`` (ORCH-02). Never
imports ``dataplat``/``csv_processor`` directly (ADR-0004) -- reached only
through a pod.

Trigger design (D-01..D-04, D-18): a deferrable ``S3KeySensor`` (30s poke)
wakes the DAG; ``max_active_runs=1`` (D-03) stops two runs racing the same
advisory lock. ``list_matched_keys`` then resolves the real key list (the
sensor pushes none to XCom) and ``integrity_gate`` fans LOAD-10's pre-pod-
launch checks out over it -- ``discover`` never runs for a rejected file,
and otherwise fans out over one frozen manifest per run (D-04, ORCH-08),
never one-file-one-run.

D-15: ``customers_asset`` is declared here and consumed by
``csv_ingest_orders.py``'s ``schedule=[customers_asset]`` -- ``orders`` runs
only once customers' own ``ingest`` (publish) task completes. ORCH-05:
``resolve_window`` proves ``logical_date=None`` (an asset/API-triggered run)
never raises anywhere in this DAG -- independent, unchained, never consumed
by discover/ingest (PITFALLS #8).
"""

from __future__ import annotations

import logging

import pendulum
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import Asset, dag, task
from kubernetes.client import models as k8s

from _common.integrity_gate import integrity_gate, list_matched_keys
from _common.kpo import common_kpo_kwargs
from _common.tracing_kpo import TracingKubernetesPodOperator

log = logging.getLogger(__name__)

_DISCOVER_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "100m", "memory": "128Mi"}, limits={"cpu": "500m", "memory": "256Mi"}
)
_INGEST_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "1Gi"}, limits={"cpu": "2", "memory": "4Gi"}
)
_INGEST_EXTRA_ENV_VARS = [k8s.V1EnvVar(name="DATAPLAT_HEARTBEAT_INTERVAL_SECONDS", value="2")]

# D-15: imported by name and scheduled off in csv_ingest_orders.py.
customers_asset = Asset("s3://normalized/customers")


@task
def resolve_window(dag_run=None) -> dict[str, str | None]:  # noqa: ANN001 -- Airflow-injected context param, untyped upstream too
    """Prove ORCH-05: an asset/API-triggered run (``logical_date=None``) never raises."""
    if dag_run is None or dag_run.logical_date is None:
        return {"logical_date": None, "data_interval_start": None, "data_interval_end": None}
    return {
        "logical_date": dag_run.logical_date.isoformat(),
        "data_interval_start": dag_run.data_interval_start.isoformat(),
        "data_interval_end": dag_run.data_interval_end.isoformat(),
    }


@task
def build_ingest_args(discovered: dict) -> list[list[str]]:
    """Reshape ``discover``'s XCom into one ``ingest`` CLI argv per discovered unit."""
    return [["ingest", "--assignment", unit["assignment_uri"]] for unit in discovered["units"]]


@task
def aggregate_receipts(receipts: list[dict]) -> None:
    """Log one summary line for the run -- orchestration glue, not business logic."""
    total_rows_loaded = sum(r["rows_loaded"] for r in receipts)
    log.info(
        "csv_ingest_customers run summary: %d receipt(s), %d row(s) loaded",
        len(receipts),
        total_rows_loaded,
    )


# */1 * * * *: max_active_runs=1 (D-03) caps concurrency regardless, so a
# short interval just keeps a new sensing opportunity available promptly.
@dag(
    dag_id="csv_ingest_customers",
    schedule="*/1 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["vertical-slice", "customers"],
)
def csv_ingest_customers() -> None:
    """Wire D-01..D-04/D-18's trigger+gate design into the ORCH-01..09 task graph."""
    wait_for_files = S3KeySensor(
        task_id="wait_for_files",
        bucket_name="raw",
        bucket_key="customers/*.csv",
        wildcard_match=True,
        aws_conn_id="minio_default",
        deferrable=True,
        poke_interval=30,
        retries=2,
        retry_exponential_backoff=True,
    )
    resolve_window()

    # D-18: Airflow's own listing of the same prefix (see module docstring).
    matched_keys = list_matched_keys(bucket="raw", prefix="customers/*.csv")

    # Cap fan-out at 3 pods (750m): unbounded integrity_gate expansion
    # starves other DAGs' scheduling on kind's ~700-800m/worker headroom
    # (same cause as `ingest`'s cap below, commit 6ea4129). `.override()`,
    # not `.partial()`: TaskFlow's `.partial()` validates against the
    # decorated function's signature, not BaseOperator fields.
    gate = (
        integrity_gate.override(max_active_tis_per_dag=3)
        .partial(bucket="raw", dataset_name="customers")
        .expand(key=matched_keys)
    )

    discover = KubernetesPodOperator(
        task_id="discover",
        cmds=["dataplat"],
        arguments=["discover", "--dataset", "customers"],
        retries=2,
        retry_exponential_backoff=True,
        **common_kpo_kwargs(resources=_DISCOVER_RESOURCES),
    )
    wait_for_files >> matched_keys >> gate >> discover

    # D-12: ingest is the trace root (OBS-10). max_active_tis_per_dag=1:
    # kind's tight CPU headroom (debug session airflow-scheduler-stuck-
    # tasks). D-15: outlets=[customers_asset] makes `orders` schedulable.
    ingest = TracingKubernetesPodOperator.partial(
        task_id="ingest",
        cmds=["dataplat"],
        retries=3,
        retry_exponential_backoff=True,
        max_active_tis_per_dag=1,
        outlets=[customers_asset],
        **common_kpo_kwargs(resources=_INGEST_RESOURCES, extra_env_vars=_INGEST_EXTRA_ENV_VARS),
    ).expand(arguments=build_ingest_args(discover.output))

    aggregate_receipts(ingest.output)


csv_ingest_customers()
