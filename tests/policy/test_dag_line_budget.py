"""ORCH-06: the DAG line budgets are mechanically enforced, not merely aspirational.

``csv_ingest_customers.py`` stays under 150 lines; ``smoke_kubernetes_pod.py``
stays under 30. A DAG file that grows past its budget is a DAG file that has
started accumulating logic that belongs in ``dataplat``/``csv_processor``
instead.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_csv_ingest_customers_stays_under_150_lines() -> None:
    path = REPO_ROOT / "airflow" / "dags" / "csv_ingest_customers.py"
    line_count = len(path.read_text(encoding="utf-8").splitlines())
    msg = f"ORCH-06: csv_ingest_customers.py is {line_count} lines, budget is <150"
    assert line_count < 150, msg


def test_smoke_kubernetes_pod_stays_under_30_lines() -> None:
    path = REPO_ROOT / "airflow" / "dags" / "smoke_kubernetes_pod.py"
    line_count = len(path.read_text(encoding="utf-8").splitlines())
    msg = f"ORCH-06: smoke_kubernetes_pod.py is {line_count} lines, budget is <30"
    assert line_count < 30, msg
