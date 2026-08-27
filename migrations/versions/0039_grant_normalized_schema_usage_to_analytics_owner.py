"""Grant USAGE on schema normalized to analytics_owner -- 0019 granted only table SELECTs.

The exact gap migration 0038 closed for schema `meta`, now closed for schema
`normalized`: migration 0019 granted `analytics_owner` table-level `SELECT`
on `normalized.customers`/`normalized.orders`, but schema `normalized` is
owned by the migration-running superuser (`postgres`) and `USAGE` was never
granted to `analytics_owner` -- PostgreSQL gates every table privilege
behind schema `USAGE`, so those table grants were inert on any cluster whose
`normalized` schema `analytics_owner` had no other path into. Confirmed live
on a fresh ephemeral CI cluster (debug/ci-pipeline-ingestion-timeout ROUND
15, run 33103279876): `tests/e2e/slice/test_referential_orphan.py`'s
`analytics_owner_connection` read of `normalized.orders` failed with
`psycopg.errors.InsufficientPrivilege: permission denied for schema
normalized` -- 0038's own docstring had already flagged 0019 as "apparently
worked without it only because ... covered some other way (unconfirmed)";
the "other way" was the persistent local cluster's hand-accumulated state,
which a fresh cluster never has. Closes the oversight in scope, not the
security boundary: `etl_app` remains the only write-capable consumer of
`normalized`.

Revision ID: 0039
Revises: 0038
Create Date: 2026-08-28
"""

from __future__ import annotations

from alembic import op

revision = "0039"
down_revision = "0038"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Grant USAGE on schema normalized to analytics_owner (table SELECTs exist since 0019)."""
    op.execute("GRANT USAGE ON SCHEMA normalized TO analytics_owner")


def downgrade() -> None:
    """Revoke USAGE on schema normalized from analytics_owner."""
    op.execute("REVOKE USAGE ON SCHEMA normalized FROM analytics_owner")
