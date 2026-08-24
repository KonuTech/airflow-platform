"""Grant USAGE on schema meta + SELECT on meta.files/meta.datasets to analytics_owner.

Same class of oversight migrations 0013/0018/0019 already fixed for
`meta.v_customers_lineage`, `meta.validation_results`/`meta.rejected_records`
and `normalized.customers`/`normalized.orders`: migrations 0001/0002 granted
SELECT/INSERT/UPDATE on these two tables to `etl_app` only, never
anticipating `analytics_owner` needing direct read access. Confirmed live
against both the persistent local cluster and a fresh ephemeral CI cluster
during plan 11-04/11-05's live E2E verification:
`tests/e2e/vault/test_airflow_backend.py::test_dag_still_resolves_its_connection_and_runs`
and `tests/e2e/observability/test_trace_propagation.py` both hit
`psycopg.errors.InsufficientPrivilege: permission denied for schema meta`
via the `analytics_owner_connection` fixture joining `meta.files`/
`meta.datasets`.

Deeper than migrations 0013/0018/0019's own gap: `\\dn+ meta` on the live
cluster shows schema `meta` is owned by `postgres` (the migration-running
superuser) with `USAGE` already granted to `etl_app`/`grafana_reader`/
`dbt_app` -- but never to `analytics_owner`. Table-level `SELECT` alone is
insufficient without schema `USAGE`; migrations 0013/0018/0019 apparently
worked without it only because the underlying tables/views involved there
already had `USAGE` covered some other way (unconfirmed which). Granting
`USAGE` explicitly here, alongside the table `SELECT`s, closes the oversight
in scope, not the security boundary -- `etl_app` remains the only
write-capable consumer of either table.

Revision ID: 0038
Revises: 0037
Create Date: 2026-08-24
"""

from __future__ import annotations

from alembic import op

revision = "0038"
down_revision = "0037"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Grant USAGE on schema meta + SELECT on meta.files/meta.datasets to analytics_owner."""
    op.execute("GRANT USAGE ON SCHEMA meta TO analytics_owner")
    op.execute("GRANT SELECT ON meta.files TO analytics_owner")
    op.execute("GRANT SELECT ON meta.datasets TO analytics_owner")


def downgrade() -> None:
    """Revoke SELECT on meta.files/meta.datasets and USAGE on schema meta from analytics_owner."""
    op.execute("REVOKE SELECT ON meta.datasets FROM analytics_owner")
    op.execute("REVOKE SELECT ON meta.files FROM analytics_owner")
    op.execute("REVOKE USAGE ON SCHEMA meta FROM analytics_owner")
