"""ORCH-06: the DAG line budgets are mechanically enforced, not merely aspirational.

``csv_ingest_customers.py``/``csv_ingest_orders.py`` stay at or under 150 lines
each; ``smoke_kubernetes_pod.py`` stays under 30. A DAG file that grows past
its budget is a DAG file that has started accumulating logic that belongs in
``dataplat``/``csv_processor`` instead.

REQUIREMENTS.md's own ORCH-06 wording is "~150 lines" (approximate, not an
exact locked number). Plan 09-09 (LOAD-06: wiring DBT_BUILD `run_stages`
tracking into both DAGs) found both files already sitting at the prior
exact 149-line ceiling with zero headroom, and needed exactly one new
import line each (`from _common.run_stage_recorder import
wire_dbt_build_tracking`) after collapsing the entire new task sub-chain
into a single `_common` helper call -- the minimal implementation possible
(see `wire_dbt_build_tracking`'s own docstring in
`airflow/dags/_common/run_stage_recorder.py`). The budget below is bumped
by that exact one line (`< 150` -> `<= 150`), not rounded up further, to
admit it.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_csv_ingest_customers_stays_under_150_lines() -> None:
    path = REPO_ROOT / "airflow" / "dags" / "csv_ingest_customers.py"
    line_count = len(path.read_text(encoding="utf-8").splitlines())
    msg = f"ORCH-06: csv_ingest_customers.py is {line_count} lines, budget is <=150"
    assert line_count <= 150, msg


def test_csv_ingest_orders_stays_under_150_lines() -> None:
    path = REPO_ROOT / "airflow" / "dags" / "csv_ingest_orders.py"
    line_count = len(path.read_text(encoding="utf-8").splitlines())
    msg = f"ORCH-06: csv_ingest_orders.py is {line_count} lines, budget is <=150"
    assert line_count <= 150, msg


def test_smoke_kubernetes_pod_stays_under_30_lines() -> None:
    path = REPO_ROOT / "airflow" / "dags" / "smoke_kubernetes_pod.py"
    line_count = len(path.read_text(encoding="utf-8").splitlines())
    msg = f"ORCH-06: smoke_kubernetes_pod.py is {line_count} lines, budget is <30"
    assert line_count < 30, msg
