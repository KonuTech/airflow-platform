"""Grant SELECT on meta.ingestion_runs to analytics_owner -- the 0038/0039/0040 family's next gap.

Same grant-history class migrations 0038 (schema meta USAGE +
meta.files/meta.datasets), 0039 (schema normalized USAGE) and 0040
(meta.dbt_processed_runs SELECT) already closed one table at a time:
`analytics_owner` holds schema `meta` USAGE since 0038, but
`meta.ingestion_runs` itself (migration 0002, granted to `etl_app` only)
never received a SELECT for it. Confirmed live on a fresh ephemeral CI
cluster (debug/ci-pipeline-ingestion-timeout ROUND 16, run 33126343052):
`tests/e2e/slice/test_backfill_reentry.py`'s `_fetch_dagrun_identity` read
of `meta.ingestion_runs.dag_id`/`dag_run_id` via the
`analytics_owner_connection` fixture failed with
`psycopg.errors.InsufficientPrivilege` -- the first test line ever to reach
this read as `analytics_owner` (ROUND 15's run died earlier, at the
since-fixed quarantine). Closes the oversight in scope, not the security
boundary: `etl_app` remains the only write-capable consumer of
`meta.ingestion_runs`.

Revision ID: 0042
Revises: 0041
Create Date: 2026-08-28
"""

from __future__ import annotations

from alembic import op

revision = "0042"
down_revision = "0041"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Grant SELECT on meta.ingestion_runs to analytics_owner (schema USAGE exists since 0038)."""
    op.execute("GRANT SELECT ON meta.ingestion_runs TO analytics_owner")


def downgrade() -> None:
    """Revoke SELECT on meta.ingestion_runs from analytics_owner."""
    op.execute("REVOKE SELECT ON meta.ingestion_runs FROM analytics_owner")
