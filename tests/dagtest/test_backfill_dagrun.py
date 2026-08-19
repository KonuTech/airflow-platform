"""`dag.test()` proves backfill-DagRun mechanics for both ingestion DAGs (VALID-08).

This codebase's first `dag.test()`-based test: a genuine `DagRun` executes
in-process against a real (testcontainers) Airflow metadata database, with
`KubernetesPodOperator.execute` mocked (per CLAUDE.md's own documented
pattern) and every S3/`boto3` touchpoint doubled (`conftest.py`'s
`mock_s3_infrastructure` -- this tier stands up no MinIO container).

Scope, per RESEARCH.md Pitfall 3's explicit three-tier split: this tier
proves DAG-level backfill *mechanics* -- correct `logical_date`, correct
task-graph shape, a genuinely different `run_id` per logical date -- not the
real resolution-state-transition logic inside a launched pod (plan 08-03's
own tier) and not a full live-cluster proof (plan 08-14's own tier).
`csv_ingest_customers.py`'s module docstring makes the claim this test
exists to check mechanically: a backfilled run just re-invokes
wait_for_files -> discover -> stage -> dbt_build -> publish against the
CURRENT state -- i.e. a backfill run is not a structurally different code
path from a normal scheduled run (08.1-12: updated for the stage/dbt_build/
publish split that replaced the single `ingest` task).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pendulum
import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

pytestmark = pytest.mark.dagtest

# Two distinct historical logical dates -- neither "now" (this DAG's own
# `start_date=pendulum.datetime(2026, 1, 1, ...)`, so both are valid,
# non-future backfill targets), deliberately different from each other so a
# structural comparison between the two runs is meaningful.
_LOGICAL_DATE_1 = pendulum.datetime(2026, 1, 1, tz="UTC")
_LOGICAL_DATE_2 = pendulum.datetime(2026, 1, 2, tz="UTC")


def _all_task_instances_succeeded(dag_run: Any) -> bool:
    """True when every task instance in `dag_run` reached a real `success` state."""
    task_instances = list(dag_run.get_task_instances())
    return bool(task_instances) and all(ti.state == "success" for ti in task_instances)


def test_backfill_dagrun_customers_succeeds_and_is_structurally_stable(
    load_dag: Callable[[str], Any],
    mock_kpo_execute: list[dict[str, Any]],
    mock_s3_infrastructure: None,  # noqa: ARG001 -- fixture used for its patching side effect only
    mock_run_stage_recorder_db: None,  # noqa: ARG001 -- fixture used for its patching side effect only
) -> None:
    """`dag.test()` against `csv_ingest_customers` proves backfill-DagRun mechanics (VALID-08).

    Args:
        load_dag: `tests/dagtest/conftest.py`'s `DagBag`-backed loader.
        mock_kpo_execute: The recorded-calls list `conftest.py`'s fixture
            yields -- used below to prove `discover`/`stage`/`dbt_build`/
            `publish` genuinely ran through the mock, not skipped by an
            upstream failure.
        mock_s3_infrastructure: Doubles every S3/`boto3` touchpoint the DAG's
            sensor/gate/list tasks use (no return value needed by this test).
        mock_run_stage_recorder_db: Doubles `list_run_ids_pending_dbt_build`/
            `record_dbt_build_stage`'s own DB touchpoints (plan 09-09) -- this
            tier stands up no analytical PostgreSQL container.
    """
    dag = load_dag("csv_ingest_customers")

    dag_run_1 = dag.test(logical_date=_LOGICAL_DATE_1)
    assert dag_run_1.state == "success", (
        f"DagRun did not reach success (state={dag_run_1.state!r}); task states: "
        f"{[(ti.task_id, ti.map_index, ti.state) for ti in dag_run_1.get_task_instances()]}"
    )
    assert _all_task_instances_succeeded(dag_run_1)

    # ORCH-05's own new angle (module docstring): resolve_window's logical_date
    # output is populated for a BACKFILL-triggered run specifically -- the
    # existing ORCH-05 proof covers the logical_date=None asset-triggered case.
    resolve_window_ti = next(
        ti for ti in dag_run_1.get_task_instances() if ti.task_id == "resolve_window"
    )
    resolved_window = resolve_window_ti.xcom_pull(
        task_ids="resolve_window",
        dag_id="csv_ingest_customers",
        run_id=dag_run_1.run_id,
    )
    assert resolved_window is not None
    assert resolved_window["logical_date"] is not None

    dag_run_2 = dag.test(logical_date=_LOGICAL_DATE_2)
    assert dag_run_2.state == "success"
    assert _all_task_instances_succeeded(dag_run_2)

    # A different execution_date produces a DIFFERENT dag_run_id...
    assert dag_run_1.run_id != dag_run_2.run_id
    # ...but resolves the IDENTICAL task graph shape (same task_id set) --
    # proving a backfill run is not a structurally different code path from
    # a normal scheduled run.
    task_ids_1 = {ti.task_id for ti in dag_run_1.get_task_instances()}
    task_ids_2 = {ti.task_id for ti in dag_run_2.get_task_instances()}
    assert task_ids_1 == task_ids_2

    # The sensor/gate/discover/stage/dbt_build/publish chain all "ran," per
    # the mock -- not short-circuited by an upstream failure. mock_kpo_execute
    # accumulates across BOTH dag.test() calls above, so all task_ids must appear.
    mocked_task_ids = {call["task_id"] for call in mock_kpo_execute}
    assert mocked_task_ids == {"discover", "stage", "dbt_build", "publish"}


def test_backfill_dagrun_orders_succeeds(
    load_dag: Callable[[str], Any],
    mock_kpo_execute: list[dict[str, Any]],
    mock_s3_infrastructure: None,  # noqa: ARG001 -- fixture used for its patching side effect only
    mock_run_stage_recorder_db: None,  # noqa: ARG001 -- fixture used for its patching side effect only
) -> None:
    """A parallel, smaller proof for `csv_ingest_orders` (Pitfall 3's own scope limit).

    One solid DAG-executes-cleanly proof is sufficient here -- not every
    assertion from the `customers` test above needs duplicating; this tier
    proves DAG-level mechanics, not the real resolution-state-transition
    logic, which stays in plan 08-03's tier.
    """
    dag = load_dag("csv_ingest_orders")

    dag_run = dag.test(logical_date=_LOGICAL_DATE_1)

    assert dag_run.state == "success", (
        f"DagRun did not reach success (state={dag_run.state!r}); task states: "
        f"{[(ti.task_id, ti.map_index, ti.state) for ti in dag_run.get_task_instances()]}"
    )
    assert _all_task_instances_succeeded(dag_run)

    mocked_task_ids = {call["task_id"] for call in mock_kpo_execute}
    assert mocked_task_ids == {"discover", "stage", "dbt_build", "publish"}
