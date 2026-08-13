"""staging — the schema StagingLoader's per-run throwaway tables live in.

ARCHITECTURE.md's schema table names three schemas the analytical database
needs: `meta`, `staging`, `normalized`. Migrations `0001` and `0005` already
created `meta`/`normalized` for their own Alembic-managed tables. `staging`
is different in kind, not just in name: it holds no Alembic-managed table at
all -- `dataplat.load.staging.StagingLoader` creates and drops one
throwaway `staging.<dataset>__r<run_id>` table per ingestion-run attempt,
entirely at runtime (`DROP TABLE IF EXISTS` then `CREATE UNLOGGED TABLE`,
plan 04-04). Because of that, `etl_app` needs `CREATE` on this schema --
not merely the `SELECT, INSERT, UPDATE` on one fixed, Alembic-owned table
every other migration in this set grants -- since `etl_app` is the role
that runs those `CREATE TABLE`/`DROP TABLE` statements itself at ingest
time, not merely a role that reads and writes rows into a table Alembic
already created.

This migration was not anticipated by any 04-* plan text; it is added here
because plan 04-04's `StagingLoader` is the first code in this repository
that actually writes into `staging.*`, and without this migration its very
first `CREATE UNLOGGED TABLE staging....` would fail with `schema "staging"
does not exist` against any freshly-migrated database.

Revision ID: 0007
Revises: 0006
Create Date: 2026-08-13
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create schema `staging`; grant `etl_app` USAGE + CREATE (it DDLs its own tables here)."""
    op.execute("CREATE SCHEMA IF NOT EXISTS staging")
    op.execute("GRANT USAGE, CREATE ON SCHEMA staging TO etl_app")


def downgrade() -> None:
    """Revoke `etl_app`'s grant. Never drops the schema (matches 0001/0005's convention)."""
    op.execute("REVOKE USAGE, CREATE ON SCHEMA staging FROM etl_app")
