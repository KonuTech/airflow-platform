"""Grant `etl_app` USAGE on `meta` and `normalized` — migrations 0001/0005 never did.

Discovered live, during Phase 4's own E2E verification (plan 04-08): every
`etl_app` GRANT since 0001 has only ever been table-level (`GRANT SELECT,
INSERT, UPDATE ON meta.datasets TO etl_app`, etc.). PostgreSQL requires
schema-level `USAGE` as a prerequisite gate before a role's table-level
grants take effect at all — without it, every one of those table grants was
inert. `etl_app` could not `SELECT`/`INSERT`/`UPDATE` a single row in either
`meta` or `normalized` on any database that ever ran only 0001-0006, live or
testcontainers; the real deployed pipeline could never have written data end
to end. This was invisible to `tests/integration/test_migrations.py::
test_etl_app_grants` because that test only checks
`information_schema.role_table_grants` for the existence of table-level rows
— it never checks whether the schema itself is usable, which is the exact
gap that let this ship silently through 0001-0006 and three prior phases of
review.

Migration 0007 already got this right for `staging` (`GRANT USAGE, CREATE ON
SCHEMA staging TO etl_app`) — this migration applies the same `USAGE` grant
retroactively to the two schemas 0001/0005 created (`CREATE`, not needed:
`etl_app` never DDLs tables in `meta`/`normalized`, only in `staging`, per
0007's own docstring).

Revision ID: 0008
Revises: 0007
Create Date: 2026-08-13
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Grant `etl_app` USAGE on `meta` and `normalized` (table grants already exist, were inert)."""
    op.execute("GRANT USAGE ON SCHEMA meta TO etl_app")
    op.execute("GRANT USAGE ON SCHEMA normalized TO etl_app")


def downgrade() -> None:
    """Revoke the USAGE grants this migration added. Never drops the schemas."""
    op.execute("REVOKE USAGE ON SCHEMA normalized FROM etl_app")
    op.execute("REVOKE USAGE ON SCHEMA meta FROM etl_app")
