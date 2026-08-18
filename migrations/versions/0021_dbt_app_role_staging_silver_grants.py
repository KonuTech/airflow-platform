"""dbt_app — a least-privilege PostgreSQL role for dbt's bronze-read/silver-write boundary.

Phase 08.1's architectural decision (08.1-CONTEXT.md D-08): dbt's own Postgres
role reads bronze (`staging.*`) and writes silver (`silver.*`) only, never
`normalized`/`meta` directly — that boundary stays a hard, physical DB-level
fact, not a convention dbt's `profiles.yml` merely happens to respect. A
narrow, separate `meta.dedup_audit`/`meta.dedup_decisions` grant is a later
plan's job (08.1-05), not this one's.

No password is set here — the migration never embeds a credential literal
(§81). The password is set later, out-of-band, by a Vault-bootstrap script
extension (plan 08.1-03), following the exact `kubectl exec` + `ALTER ROLE`
pattern this codebase already uses for `etl_app`/`grafana_reader`.

`staging.customers`/`staging.orders` do not exist yet when this migration
runs (migration 0022 creates them next) — the `ALTER DEFAULT PRIVILEGES`
statement is what actually makes `dbt_app`'s `SELECT` reach them once they
land, without a second grant re-run after 0022. The direct
`GRANT SELECT ON ALL TABLES IN SCHEMA staging` statement is a no-op today
(the schema is currently empty) but documents intent explicitly and covers
any staging table a future migration adds without its own explicit grant.

`silver` is a brand-new schema, owned outright by `dbt_app` (not merely
USAGE/CREATE-granted, mirroring `staging`/`etl_app`'s own ownership shape
from migration 0007) — dbt DDLs nothing here per this plan's own objective
note (Alembic owns every silver table, migration 0023), but schema ownership
is what lets dbt's `delete+insert` incremental strategy run its DML with no
further grant needed.

Revision ID: 0021
Revises: 0020
Create Date: 2026-08-18
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "0021"
down_revision = "0020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create dbt_app; grant staging SELECT (+default privileges); own the new silver schema."""
    op.execute("CREATE ROLE dbt_app LOGIN")

    # Bronze read access (D-08).
    op.execute("GRANT USAGE ON SCHEMA staging TO dbt_app")
    op.execute("GRANT SELECT ON ALL TABLES IN SCHEMA staging TO dbt_app")
    op.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT SELECT ON TABLES TO dbt_app")

    # Silver write access (D-08/D-14) — dbt_app owns the schema outright.
    op.execute("CREATE SCHEMA IF NOT EXISTS silver")
    op.execute("ALTER SCHEMA silver OWNER TO dbt_app")
    op.execute("GRANT USAGE, CREATE ON SCHEMA silver TO dbt_app")

    # Never grant dbt_app anything on normalized or meta here — D-08's hard
    # constraint. meta.dedup_audit's narrow grant is plan 08.1-05's job.


def downgrade() -> None:
    """Exact reverse of `upgrade()`: silver first (empty once 0023's downgrade has run), then role."""
    op.execute("DROP SCHEMA silver")
    op.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA staging REVOKE SELECT ON TABLES FROM dbt_app")
    op.execute("REVOKE SELECT ON ALL TABLES IN SCHEMA staging FROM dbt_app")
    op.execute("REVOKE USAGE ON SCHEMA staging FROM dbt_app")
    op.execute("DROP ROLE dbt_app")
