"""``platform_retention`` -- D-35's dedicated, structurally-separate retention maintenance DAG.

Wires plan 11-07's pure ``dataplat.retention.policy.evaluate_retention`` into
a real DAG, via ``_common/retention_query.py``'s ``run_retention`` task (the
one place in the whole platform that queries MinIO/PostgreSQL for retention
candidates and, only when a dataset's config opts in via ``enforce: true``,
issues the actual deletes -- see that module's own docstring for the full
ADR-0004-exception reasoning and layer-to-source mapping). This file itself
stays a thin ``@dag`` wrapper, mirroring ``csv_ingest_customers.py``'s own
"DAG file imports and wires a `_common/`-defined task, never defines the
business logic itself" convention (``from _common.integrity_gate import
integrity_gate, list_matched_keys``).

README §64 requires retention to stay structurally separate from ingest
processing (D-35): this DAG has exactly one task, is never imported by and
never imports from ``csv_ingest_customers.py``/``csv_ingest_orders.py``, and
shares no task-graph edge with either.

Schedule: ``@daily`` -- D-37's retention windows are measured in whole days,
so daily is the natural granularity, and D-38's dry-run-by-default guarantee
(``RetentionConfig.enforce`` defaults ``False`` at the Pydantic model level,
plan 11-07) makes a missed run low-stakes, unlike an ingestion DAG's own
schedule.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag

from _common.retention_query import run_retention


@dag(
    dag_id="platform_retention",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["maintenance", "retention"],
)
def platform_retention() -> None:
    """D-35: exactly one task, structurally separate from every ingestion DAG's own graph."""
    run_retention()


platform_retention()
