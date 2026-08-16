"""Grant SELECT on meta.v_customers_lineage to analytics_owner.

Migration 0012 granted SELECT on the OBS-07 lineage view to its two
intended production consumers, `etl_app` and `grafana_reader`, deliberately
never a broader grant. It did not anticipate `analytics_owner` (the
CNPG-generated bootstrap/admin role, already holding unrestricted DDL/DML
on this database) ever needing read access to it directly — but
`tests/e2e/observability/conftest.py`'s `analytics_connection` fixture is
intentionally `analytics_owner`-authenticated (needed for write access in
earlier Phase 7 tests that force freshness breaches), and the new live
lineage E2E proof added in plan 07-09
(`test_ingest_pod_dag_context_matches_persisted_lineage_row`) needs that
same connection to read the view. `analytics_owner` already has
unrestricted DDL/DML on this database, so granting it SELECT on one
specific view is closing an oversight in scope, not widening the view's
security boundary — `etl_app`/`grafana_reader` remain the only
non-admin-tier consumers.

Revision ID: 0013
Revises: 0012
Create Date: 2026-08-16
"""

from __future__ import annotations

from alembic import op

revision = "0013"
down_revision = "0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Grant SELECT on the lineage view to analytics_owner."""
    op.execute("GRANT SELECT ON meta.v_customers_lineage TO analytics_owner")


def downgrade() -> None:
    """Revoke SELECT on the lineage view from analytics_owner."""
    op.execute("REVOKE SELECT ON meta.v_customers_lineage FROM analytics_owner")
