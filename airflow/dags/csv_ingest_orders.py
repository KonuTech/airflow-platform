"""``csv_ingest_orders`` -- the second real ingestion DAG (D-14, D-15, D-16).

Mirrors csv_ingest_customers.py's shape exactly (D-14): same trigger design, same D-18
list_matched_keys -> integrity_gate gate ahead of discover, same 08.1-12
discover -> stage -> dbt_build -> publish graph -- dataset="orders" substituted everywhere
"customers" appeared. dbt_build's own construction is IDENTICAL between the two files
(service_account_name="dbt", vault_k8s_role="dbt", image_variable="dbt_image"): dbt's
project is dataset-agnostic, so an unscoped dbt build from either DAG builds BOTH
silver_customers and silver_orders -- re-running it from the OTHER DAG moments later is a
safe, idempotent no-op for whichever model has nothing new (deliberate design, not oversight).

D-15: scheduled off customers_asset, matching customers.py's own publish outlets=[...] BY
URI (Airflow matches Asset scheduling by URI, not object identity -- a cross-file import
would re-register the whole customers module under a second name, T-08-26). D-16: orders
declares no outlets this phase -- it consumes the Asset, nothing depends on orders yet.
Business logic stays inside pods via KubernetesPodOperator (ORCH-02), never imported (ADR-0004).
"""

from __future__ import annotations

import logging

import pendulum
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import Asset, dag, task
from kubernetes.client import models as k8s

from _common.gap_recorder import record_processing_gap_if_empty
from _common.integrity_gate import integrity_gate, list_matched_keys
from _common.kpo import common_kpo_kwargs, stage_pod_resources
from _common.run_stage_recorder import wire_dbt_build_tracking
from _common.tracing_kpo import TracingKubernetesPodOperator

log = logging.getLogger(__name__)

customers_asset = Asset("s3://normalized/customers")  # D-15: same URI, own object (see docstring)

_DISCOVER_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "100m", "memory": "128Mi"}, limits={"cpu": "500m", "memory": "256Mi"}
)
# CPU request is per-profile via the stage_cpu_request Airflow Variable (ci=200m, local=500m);
# see stage_pod_resources()'s own comment (debug/ci-pipeline-ingestion-timeout ROUND 10).
_STAGE_RESOURCES = stage_pod_resources()
_INGEST_EXTRA_ENV_VARS = [k8s.V1EnvVar(name="DATAPLAT_HEARTBEAT_INTERVAL_SECONDS", value="2")]


@task
def resolve_window(dag_run=None) -> dict[str, str | None]:  # noqa: ANN001 -- Airflow-injected context param, untyped upstream too
    """Prove ORCH-05: an asset-triggered run (``logical_date=None``) never raises."""
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
        "csv_ingest_orders run summary: %d receipt(s), %d row(s) loaded",
        len(receipts),
        total_rows_loaded,
    )


# schedule=[customers_asset] (D-15): this DAG only runs after customers' own publish lands GOLD.
# dagrun_timeout=45min: same debug/ci-pipeline-ingestion-timeout ROUND 3 fix as
# csv_ingest_customers.py's own @dag() (see that file's comment for the full rationale).
# max_active_tasks=6: same debug/ci-pipeline-ingestion-timeout ROUND 7 per-DagRun flood guard as
# csv_ingest_customers.py's own @dag() (see that file's comment for the full rationale).
# is_paused_upon_creation=False (debug/ci-pipeline-ingestion-timeout ROUND 13, root cause 17): a
# paused ASSET-scheduled DAG silently drops asset events -- fresh deployments must not start deaf.
@dag(
    dag_id="csv_ingest_orders",
    schedule=[customers_asset],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=6,
    dagrun_timeout=pendulum.duration(minutes=45),
    is_paused_upon_creation=False,
    tags=["vertical-slice", "orders"],
)
def csv_ingest_orders() -> None:
    """Wire D-14/D-15/D-16's trigger+gate design into the ORCH-01..09 task graph."""
    wait_for_files = S3KeySensor(
        task_id="wait_for_files",
        bucket_name="raw",
        bucket_key="orders/*.csv",
        wildcard_match=True,
        aws_conn_id="minio_default",
        deferrable=True,
        poke_interval=30,
        retries=2,
        retry_exponential_backoff=True,
    )
    resolve_window()
    matched_keys = list_matched_keys(bucket="raw", prefix="orders/*.csv")  # D-18
    record_processing_gap_if_empty(matched_keys, dataset_name="orders")  # D-06
    # Cap at 3 pods (kind CPU headroom); .override(), not .partial() (validates fn signature).
    gate = (
        integrity_gate.override(max_active_tis_per_dag=3)
        .partial(bucket="raw", dataset_name="orders")
        .expand(key=matched_keys)
    )
    discover = KubernetesPodOperator(
        task_id="discover",
        cmds=["dataplat"],
        arguments=["discover", "--dataset", "orders"],
        retries=2,
        retry_exponential_backoff=True,
        **common_kpo_kwargs(resources=_DISCOVER_RESOURCES),
    )
    wait_for_files >> matched_keys >> gate >> discover
    # D-12: stage is the trace root (OBS-10). No outlets here (D-16): orders produces no Asset.
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
        arguments=["publish", "--dataset", "orders"],
        retries=3,
        retry_exponential_backoff=True,
        **common_kpo_kwargs(resources=_DISCOVER_RESOURCES),
    )
    wire_dbt_build_tracking("orders", stage, dbt_build, publish)  # LOAD-06 (D-14/D-17/D-19)
    aggregate_receipts(stage.output)


csv_ingest_orders()
