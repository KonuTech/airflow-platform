"""ORCH-05: `resolve_window` never raises, regardless of how the DagRun was triggered.

Reuses ``tests/unit/conftest.py``'s ``dagbag`` fixture (the one shared
``airflow/dags``-loading mechanism, per 04-07-PLAN.md Task 2) and pulls the
``@task``-decorated function's underlying raw callable off the parsed task
via ``.python_callable`` -- verified directly against this Airflow
version's ``_PythonDecoratedOperator`` -- letting these tests call
``resolve_window`` as a plain function with hand-built stand-ins, no
DagRun/executor machinery.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING

import pendulum

if TYPE_CHECKING:
    from collections.abc import Callable

    from airflow.models import DagBag


def _resolve_window(dagbag: DagBag) -> Callable[..., dict[str, str | None]]:
    task = dagbag.dags["csv_ingest_customers"].get_task("resolve_window")
    return task.python_callable  # type: ignore[no-any-return]


def test_resolve_window_with_no_dag_run(dagbag: DagBag) -> None:
    fn = _resolve_window(dagbag)
    assert fn(dag_run=None) == {
        "logical_date": None,
        "data_interval_start": None,
        "data_interval_end": None,
    }


def test_resolve_window_with_asset_triggered_dag_run(dagbag: DagBag) -> None:
    fn = _resolve_window(dagbag)
    asset_triggered = SimpleNamespace(logical_date=None)
    assert fn(dag_run=asset_triggered) == {
        "logical_date": None,
        "data_interval_start": None,
        "data_interval_end": None,
    }


def test_resolve_window_with_scheduled_dag_run(dagbag: DagBag) -> None:
    fn = _resolve_window(dagbag)
    scheduled = SimpleNamespace(
        logical_date=pendulum.datetime(2026, 1, 2, tz="UTC"),
        data_interval_start=pendulum.datetime(2026, 1, 2, tz="UTC"),
        data_interval_end=pendulum.datetime(2026, 1, 3, tz="UTC"),
    )
    assert fn(dag_run=scheduled) == {
        "logical_date": "2026-01-02T00:00:00+00:00",
        "data_interval_start": "2026-01-02T00:00:00+00:00",
        "data_interval_end": "2026-01-03T00:00:00+00:00",
    }
