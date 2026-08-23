"""``chaos_probe`` -- three throwaway, manually-triggered ``dataplat`` CLI probe DAGs.

11-10-PLAN.md. QUAL-15's `test_oom.py`/`test_task_timeout.py` (Phase 11 chaos suite, Wave 2)
need a genuine, *queryable* Airflow task instance running the real ``dataplat`` entrypoint
under a deliberately undersized memory limit / a deliberately tiny ``execution_timeout`` --
``airflow tasks test`` explicitly does not persist task-instance state ("without checking for
dependencies or recording its state in the database", confirmed live against this cluster's
own installed Airflow 3.3.0), so only a real ``airflow dags trigger`` against a real DAG
produces a state the tests can assert on. Neither fault can be reproduced by editing
``csv_ingest_customers.py`` itself: that would either revert 10-07-PLAN.md's own permanent fix
(the ``publish`` task's resources were deliberately RAISED from the undersized profile that
caused the original OOM) or destabilise the platform's one real, continuously-scheduled
customers pipeline. Three separate, minimal, never-scheduled DAGs here -- built entirely from
``_common/kpo.py``'s existing ``common_kpo_kwargs`` helper, the exact same way every task in
``csv_ingest_customers.py`` already is -- give each test its own clean, single-purpose
``DagRun`` with no unrelated task noise.

``chaos_probe_discover_stage_publish_customers`` mirrors ``csv_ingest_customers.py``'s own
discover/build_stage_args/stage/publish shape (`wait_for_files`/`gate`/`dbt_build` intentionally
omitted: `publish`'s CLI gate is ``mark_dbt_build_done``, an EDGE internal to that OTHER dag's own
graph -- see ``_common/run_stage_recorder.py`` -- never a constraint the ``publish`` CLI itself
enforces, so this probe's bare discover->stage->publish chain publishes correctly with no
dbt-tracking sub-chain at all) -- used by ``test_invalid_encoding.py``, which needs a full live
ingestion of a `customers`-schema file (the one dataset carrying a free-text column) without
waiting behind whatever backlog ``csv_ingest_customers`` itself is working through at test time
(a real, recurring, independently-documented characteristic of this dataset -- see
`tests/e2e/chaos/test_pod_crash.py`'s own module docstring for the identical reasoning, there
applied to `csv_ingest_orders` instead).

``chaos_probe_oom_publish_customers`` runs one `publish --dataset customers` invocation at the
exact undersized memory limit (256Mi) `csv_ingest_customers.py`'s own `publish` task comment cites
as the real, live-observed OOMKilled threshold (10-07-PLAN.md) -- a permanent regression proof for
that finding (`test_oom.py`). `retries=0`: a single clean `failed` outcome is what the test
polls for, not a retry sequence.

``chaos_probe_timeout_publish_customers`` runs one `publish --dataset customers` invocation with
`execution_timeout=5s` -- deliberately far below how long a real dataset-wide publish sweep takes
(`test_task_timeout.py`). `retries=1` so the test can also observe Airflow's own declared-retry
semantics engaging, not just a single failure.

All three: `schedule=None` (never auto-runs), `tags=["chaos-probe"]`, and dataset-wide
(`publish --dataset customers`, never scoped to one run) -- matching the real `publish` CLI's own
documented shape (11-09-SUMMARY.md: "dataset-wide (no run-id scoping in its CLI arguments)"), so a
concurrent legitimate publish (the production DAG's own eventual real attempt) is safe: both
serialize through the SAME `pg_advisory_xact_lock` `dataplat.load.publish` already uses.
"""

from __future__ import annotations

import datetime

import pendulum
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import dag, task
from kubernetes.client import models as k8s

from _common.kpo import common_kpo_kwargs

_START_DATE = pendulum.datetime(2026, 1, 1, tz="UTC")
_TAGS = ["chaos-probe"]

_DISCOVER_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "100m", "memory": "128Mi"}, limits={"cpu": "500m", "memory": "256Mi"}
)
_STAGE_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "1Gi"}, limits={"cpu": "2", "memory": "4Gi"}
)
# The exact undersized profile 10-07-PLAN.md's live sweep found insufficient for `publish`'s
# SCDPublisher in-memory full-history recompute (module docstring) -- test_oom.py's own fault.
_UNDERSIZED_PUBLISH_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "100m", "memory": "128Mi"}, limits={"cpu": "500m", "memory": "256Mi"}
)


@task
def build_stage_args_probe(discovered: dict) -> list[list[str]]:
    """Reshape `discover`'s XCom into one `stage` CLI argv per discovered unit.

    `csv_ingest_customers.py`'s own identical helper, copied here since a probe DAG must not
    import from a sibling DAG module.
    """
    return [["stage", "--assignment", unit["assignment_uri"]] for unit in discovered["units"]]


@dag(
    dag_id="chaos_probe_discover_stage_publish_customers",
    schedule=None,
    start_date=_START_DATE,
    catchup=False,
    tags=_TAGS,
)
def chaos_probe_discover_stage_publish_customers() -> None:
    """A full, standalone discover->stage->publish cycle for `customers`.

    Bypasses that dataset's own busy production DAG entirely (module docstring).
    """
    discover_customers_probe = KubernetesPodOperator(
        task_id="discover_customers_probe",
        cmds=["dataplat"],
        arguments=["discover", "--dataset", "customers"],
        retries=0,
        **common_kpo_kwargs(resources=_DISCOVER_RESOURCES),
    )
    stage_customers_probe = KubernetesPodOperator.partial(
        task_id="stage_customers_probe",
        cmds=["dataplat"],
        retries=0,
        **common_kpo_kwargs(resources=_STAGE_RESOURCES),
    ).expand(arguments=build_stage_args_probe(discover_customers_probe.output))
    publish_customers_probe = KubernetesPodOperator(
        task_id="publish_customers_probe",
        cmds=["dataplat"],
        arguments=["publish", "--dataset", "customers"],
        retries=0,
        **common_kpo_kwargs(resources=_STAGE_RESOURCES),
    )
    discover_customers_probe >> stage_customers_probe >> publish_customers_probe


@dag(
    dag_id="chaos_probe_oom_publish_customers",
    schedule=None,
    start_date=_START_DATE,
    catchup=False,
    tags=_TAGS,
)
def chaos_probe_oom_publish_customers() -> None:
    """One `publish --dataset customers` invocation, deliberately undersized (module docstring)."""
    KubernetesPodOperator(
        task_id="publish_customers_undersized_memory_probe",
        cmds=["dataplat"],
        arguments=["publish", "--dataset", "customers"],
        retries=0,
        **common_kpo_kwargs(resources=_UNDERSIZED_PUBLISH_RESOURCES),
    )


@dag(
    dag_id="chaos_probe_timeout_publish_customers",
    schedule=None,
    start_date=_START_DATE,
    catchup=False,
    tags=_TAGS,
)
def chaos_probe_timeout_publish_customers() -> None:
    """One `publish --dataset customers` invocation, deliberately tiny `execution_timeout`."""
    KubernetesPodOperator(
        task_id="publish_customers_tiny_timeout_probe",
        cmds=["dataplat"],
        arguments=["publish", "--dataset", "customers"],
        retries=1,
        # Short, deliberate override of BaseOperator's own 300s default -- a real dataset-wide
        # publish sweep takes far longer than 5s either way, so there is no reason to wait 5
        # minutes between the two attempts this task's own retries=1 always needs.
        retry_delay=datetime.timedelta(seconds=10),
        execution_timeout=datetime.timedelta(seconds=5),
        **common_kpo_kwargs(resources=_STAGE_RESOURCES),
    )


chaos_probe_discover_stage_publish_customers()
chaos_probe_oom_publish_customers()
chaos_probe_timeout_publish_customers()
