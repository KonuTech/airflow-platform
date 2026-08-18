"""``csv_ingest_customers`` -- the vertical slice DAG (ORCH-01..09, D-01..D-04, D-15, D-18).

Thin orchestration only (README Sec 6.4/68, ADR-0004): every line builds a Kubernetes API object,
wires task deps, or logs a scalar -- parsing/validation/typing/DB writes/dbt all run inside pods
(ORCH-02). Trigger (D-01..D-04, D-18): deferrable S3KeySensor wakes the DAG (30s poke);
max_active_runs=1 (D-03) stops two runs racing the advisory lock; list_matched_keys resolves the
real key list; integrity_gate fans LOAD-10's pre-pod-launch checks over one frozen manifest per run
(D-04, ORCH-08) -- discover never runs for a rejected file.

08.1-12: graph is discover -> stage -> dbt_build -> publish (D-02: dbt_build its own DAG task,
decoupled from the Python claim/lease/heartbeat mechanism; D-04: single ingest/CLI splits into
exactly these 3). dbt_build runs under its OWN dbt ServiceAccount/Vault role (08.1-03), never csv-
processor's -- only run here, never manually against a live silver schema (T-08.1-29). D-15:
publish, not stage, owns customers_asset's outlet (GOLD's own state; orders waits for a real
publish, not bronze staging).
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
_STAGE_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "1Gi"}, limits={"cpu": "2", "memory": "4Gi"}
)
_INGEST_EXTRA_ENV_VARS = [k8s.V1EnvVar(name="DATAPLAT_HEARTBEAT_INTERVAL_SECONDS", value="2")]

customers_asset = Asset("s3://normalized/customers")  # D-15: referenced by URI in orders DAG


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
def build_stage_args(discovered: dict) -> list[list[str]]:
    """Reshape ``discover``'s XCom into one ``stage`` CLI argv per discovered unit."""
    return [["stage", "--assignment", unit["assignment_uri"]] for unit in discovered["units"]]


@task
def aggregate_receipts(receipts: list[dict]) -> None:
    """Log one summary line for the run -- orchestration glue, not business logic."""
    total_rows_loaded = sum(r["rows_loaded"] for r in receipts)
    log.info(
        "csv_ingest_customers run summary: %d receipt(s), %d row(s) loaded",
        len(receipts),
        total_rows_loaded,
    )


# */1 * * * *: short interval keeps sensing prompt; max_active_runs=1 (D-03) caps concurrency.
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
    matched_keys = list_matched_keys(bucket="raw", prefix="customers/*.csv")  # D-18
    # Cap at 3 pods (kind CPU headroom); .override(), not .partial() (validates fn signature).
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
    # D-12: stage is the trace root (OBS-10). No outlets here (08.1-12): stage only lands bronze.
    stage = TracingKubernetesPodOperator.partial(
        task_id="stage",
        cmds=["dataplat"],
        retries=3,
        retry_exponential_backoff=True,
        max_active_tis_per_dag=1,
        **common_kpo_kwargs(resources=_STAGE_RESOURCES, extra_env_vars=_INGEST_EXTRA_ENV_VARS),
    ).expand(arguments=build_stage_args(discover.output))
    # No cmds/arguments: the dbt image's own ENTRYPOINT resolves secrets and runs `dbt build`.
    dbt_build = KubernetesPodOperator(
        task_id="dbt_build",
        retries=2,
        retry_exponential_backoff=True,
        max_active_tis_per_dag=1,
        **common_kpo_kwargs(
            resources=_DISCOVER_RESOURCES,
            service_account_name="dbt",
            image_variable="dbt_image",
            vault_k8s_role="dbt",
            include_dataplat_credentials=False,
        ),
    )
    publish = KubernetesPodOperator(
        task_id="publish",
        cmds=["dataplat"],
        arguments=["publish", "--dataset", "customers"],
        retries=3,
        retry_exponential_backoff=True,
        outlets=[customers_asset],
        **common_kpo_kwargs(resources=_DISCOVER_RESOURCES),
    )
    stage >> dbt_build >> publish
    aggregate_receipts(stage.output)


csv_ingest_customers()
