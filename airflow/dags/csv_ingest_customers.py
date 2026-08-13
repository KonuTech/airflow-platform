"""``csv_ingest_customers`` -- the vertical slice itself (ORCH-01..09, D-01..D-04).

Thin orchestration only (README Sec 6.4/68): every line below either builds
a Kubernetes API object, wires task dependencies, or logs a scalar summary --
parsing, validation, typing and every DB write happen inside the
``csv-processor`` image's ``dataplat discover``/``dataplat ingest`` CLI
commands, launched only via ``KubernetesPodOperator``'s ``cmds``/``arguments``
(ORCH-02). Never imports ``dataplat``/``csv_processor`` -- that package is
not even installed in the real Airflow image (ADR-0004); reached only
through a pod.

Trigger design (04-CONTEXT.md "File-arrival trigger", D-01..D-04): a
deferrable ``S3KeySensor`` (D-01, D-02: 30s poke) wakes the DAG, never a
plain scheduled poll or a MinIO webhook -- both were explicitly rejected
this phase to keep Vault off the critical path (ROADMAP deviation D3).
``max_active_runs=1`` (D-03) stops two runs racing the same advisory lock.
One run's ``discover`` fans out over every file visible in its own poke
window as a single frozen manifest (D-04, ORCH-08), never one-file-one-run.

ORCH-03/06: ``discover`` runs ONCE per DagRun (not expanded) and folds
``resolve_config``+``discover_files`` into one KPO task -- ``dataplat`` is
never installed in the Airflow image, so no in-process ``@task`` could call
either function directly (04-RESEARCH.md, correcting its own earlier
diagram). ``ingest`` then expands over ``discover``'s frozen manifest,
bounded by ``configs/datasets/customers.yaml``'s own
``batching.max_units_per_run`` (plan 04-03) -- never by anything in this
file -- and comfortably under the platform-level ``[core] max_map_length``
default of 1024.

ORCH-04 backfill posture: ``airflow dags backfill`` here is degenerate-but-
harmless -- no historical window exists; a backfilled run just re-invokes
``wait_for_files -> discover -> ingest`` against the CURRENT
``s3://raw/customers/*.csv`` state, made safe by the same run-claim
idempotency protocol (plans 04-01/04-05), not by DAG-specific backfill logic.

ORCH-05: ``resolve_window`` proves ``logical_date=None`` (an asset/API-
triggered run) never raises ``KeyError`` anywhere in this DAG's task code --
it is an independent, unchained task; its result is not consumed by
discover/ingest, which derive their own identity from file content, never
from Airflow's scheduling clock (PITFALLS #8).
"""

from __future__ import annotations

import logging

import pendulum
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import dag, task
from kubernetes.client import models as k8s

from _common.kpo import common_kpo_kwargs

log = logging.getLogger(__name__)

_DISCOVER_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "100m", "memory": "128Mi"}, limits={"cpu": "500m", "memory": "256Mi"}
)
_INGEST_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "1Gi"}, limits={"cpu": "2", "memory": "4Gi"}
)


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


# */1 * * * *, not @once/a longer interval: max_active_runs=1 (D-03) means only
# ONE DagRun is ever active/deferred at a time regardless of this value -- a
# short real interval keeps a new sensing opportunity available almost
# immediately after the previous run completes, instead of leaving dead time
# where no run is active or deferred between longer schedule ticks.
@dag(
    dag_id="csv_ingest_customers",
    schedule="*/1 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["vertical-slice", "customers"],
)
def csv_ingest_customers() -> None:
    """Wire the D-01..D-04 trigger design into the ORCH-01..09 task graph."""
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

    discover = KubernetesPodOperator(
        task_id="discover",
        cmds=["dataplat"],
        arguments=["discover", "--dataset", "customers"],
        retries=2,
        retry_exponential_backoff=True,
        **common_kpo_kwargs(resources=_DISCOVER_RESOURCES),
    )
    wait_for_files >> discover

    # Fan-out is bounded upstream by discover_files's own
    # batching.max_units_per_run cap (plan 04-03), never by anything in this
    # file; the platform-level [core] max_map_length default (1024) stays
    # untouched and comfortably above the configured cap.
    ingest = KubernetesPodOperator.partial(
        task_id="ingest",
        cmds=["dataplat"],
        retries=3,
        retry_exponential_backoff=True,
        max_active_tis_per_dag=5,
        **common_kpo_kwargs(resources=_INGEST_RESOURCES),
    ).expand(arguments=build_ingest_args(discover.output))

    aggregate_receipts(ingest.output)


csv_ingest_customers()
