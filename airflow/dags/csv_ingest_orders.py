"""``csv_ingest_orders`` -- the second real ingestion DAG (D-14, D-15, D-16).

Mirrors ``csv_ingest_customers.py``'s shape exactly (D-14): same trigger
design (deferrable ``S3KeySensor``), same D-18 ``list_matched_keys ->
integrity_gate`` gate ahead of ``discover``, same ORCH-01..09 task graph --
``dataset="orders"`` substituted everywhere ``"customers"`` appeared.

D-15: scheduled off ``customers_asset``, an ``Asset("s3://normalized/
customers")`` matching ``csv_ingest_customers.py``'s own ``outlets=[...]``
declaration BY URI -- Airflow's ``DagBag`` gives each DAG file a unique
module name (T-08-26), so a plain cross-file ``from csv_ingest_customers
import customers_asset`` re-executes and re-registers that whole module
under a second name, duplicating the ``csv_ingest_customers`` dag_id; two
independently-constructed ``Asset`` objects sharing a URI schedule
identically without that re-execution. ``orders`` declares no ``outlets``
of its own this phase (D-16): it consumes the customers Asset, it produces
none.

Business logic (parsing/validation/typing/DB writes) stays entirely inside
the ``csv-processor`` image's ``dataplat`` CLI, launched only via
``KubernetesPodOperator`` (ORCH-02) -- identical discipline to
``csv_ingest_customers.py``, never imports ``dataplat``/``csv_processor``
directly (ADR-0004).
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

# D-15: same URI as csv_ingest_customers.py's own `customers_asset` --
# Airflow matches Asset scheduling by URI, not Python object identity, so a
# second, independently-constructed object here is the correct pattern (see
# module docstring for why a cross-file import is NOT).
customers_asset = Asset("s3://normalized/customers")

_DISCOVER_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "100m", "memory": "128Mi"}, limits={"cpu": "500m", "memory": "256Mi"}
)
_INGEST_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "1Gi"}, limits={"cpu": "2", "memory": "4Gi"}
)
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
def build_ingest_args(discovered: dict) -> list[list[str]]:
    """Reshape ``discover``'s XCom into one ``ingest`` CLI argv per discovered unit."""
    return [["ingest", "--assignment", unit["assignment_uri"]] for unit in discovered["units"]]


@task
def aggregate_receipts(receipts: list[dict]) -> None:
    """Log one summary line for the run -- orchestration glue, not business logic."""
    total_rows_loaded = sum(r["rows_loaded"] for r in receipts)
    log.info(
        "csv_ingest_orders run summary: %d receipt(s), %d row(s) loaded",
        len(receipts),
        total_rows_loaded,
    )


# schedule=[customers_asset] (D-15), not a cron string: this DAG only ever
# runs after customers' own `ingest` task publishes. start_date is required
# by the @dag decorator regardless of trigger mechanism (matches
# csv_ingest_customers.py's own literal).
@dag(
    dag_id="csv_ingest_orders",
    schedule=[customers_asset],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
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

    # D-18: Airflow's OWN listing of the same prefix (the sensor pushes no
    # key list to XCom), then the LOAD-10 pre-pod-launch gate fanned out
    # over it -- discover never runs for a file the gate rejects.
    matched_keys = list_matched_keys(bucket="raw", prefix="orders/*.csv")

    # Same kind-worker-node CPU-headroom mitigation as
    # csv_ingest_customers.py's own integrity_gate (see that file for the
    # full rationale) -- integrity_gate has no container_resources
    # override, so an unbounded fan-out over a matched-key backlog would
    # inherit the Helm chart's 250m default worker-pod CPU request per
    # mapped instance and starve other DAGs'/tasks' pod scheduling
    # cluster-wide. `.override(...)`, not the same kwarg passed straight
    # into `.partial(...)` -- see csv_ingest_customers.py's own comment for
    # why.
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

    # Fan-out bounded upstream by batching.max_units_per_run; D-12: ingest is
    # the trace root (OBS-10). max_active_tis_per_dag=1 matches
    # csv_ingest_customers.py's own kind-worker-node CPU-headroom mitigation.
    # No outlets= here (D-16): orders produces no Asset this phase.
    ingest = TracingKubernetesPodOperator.partial(
        task_id="ingest",
        cmds=["dataplat"],
        retries=3,
        retry_exponential_backoff=True,
        max_active_tis_per_dag=1,
        **common_kpo_kwargs(resources=_INGEST_RESOURCES, extra_env_vars=_INGEST_EXTRA_ENV_VARS),
    ).expand(arguments=build_ingest_args(discover.output))

    aggregate_receipts(ingest.output)


csv_ingest_orders()
