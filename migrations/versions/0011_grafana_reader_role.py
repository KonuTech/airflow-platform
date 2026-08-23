"""grafana_reader — a dedicated, SELECT-only PostgreSQL role for Grafana (OBS-08/OBS-01).

The first `CREATE ROLE` migration in this codebase: no `postInitApplicationSQL`
path exists for it (that CNPG `initdb` bootstrap key only runs once, at cluster
creation — the analytical CNPG cluster is already running, per 07-RESEARCH.md
Pitfall 4), so this role is created via `op.execute()`-only DDL, matching
migration 0008's own shape for non-`op.*`-helper statements.

No password is set here — the migration never embeds a credential literal
(§81). The password is set later, out-of-band, by a Vault-bootstrap script
extension (`_ensure_grafana_secrets`, plan 07-06) via `ALTER ROLE grafana_reader
WITH PASSWORD ...`, following the exact `kubectl exec` + `ALTER ROLE` pattern
this codebase already uses for `etl_app` (07-RESEARCH.md Pattern 5).

`CREATE ROLE` is guarded by an `IF NOT EXISTS`-shaped `DO $$` block (plan
11-12) rather than a bare `CREATE ROLE` statement: PostgreSQL roles are
cluster-global, not schema-scoped, so `scripts/rebuild-from-raw.py`'s
`DROP SCHEMA ... CASCADE` never removes this role, and a second
`alembic upgrade head` against a rebuilt-but-not-role-dropped database would
otherwise fail with `DuplicateObject: role "grafana_reader" already exists`
every time.

Grants exactly the schema-USAGE + table-SELECT surface Grafana's dashboard
queries and the freshness alert condition read directly (07-RESEARCH.md
Architecture Patterns, D-03/D-10): `USAGE` on `meta` and `normalized` (the
same two-schema shape migration 0008 established for `etl_app`), then
`SELECT` on exactly `meta.datasets`, `meta.files`, `meta.ingestion_runs`.
Deliberately no broader access: `grafana_reader` never gets
`INSERT`/`UPDATE`/`DELETE` on anything, and never gets a direct table grant
on `normalized.customers` — the lineage view (migration 0012) grants exactly
what it needs to see that table's data, since a Postgres view runs under its
owner's privileges, not the querying role's.

Revision ID: 0011
Revises: 0010
Create Date: 2026-08-16
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "0011"
down_revision = "0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create `grafana_reader`, grant schema USAGE, then SELECT on exactly three tables."""
    op.execute(
        "DO $$ BEGIN "
        "IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_reader') THEN "
        "CREATE ROLE grafana_reader LOGIN; "
        "END IF; "
        "END $$;"
    )
    op.execute("GRANT USAGE ON SCHEMA meta TO grafana_reader")
    op.execute("GRANT USAGE ON SCHEMA normalized TO grafana_reader")
    op.execute("GRANT SELECT ON meta.datasets, meta.files, meta.ingestion_runs TO grafana_reader")


def downgrade() -> None:
    """Revoke every grant this migration added, then drop the role (reverse of `upgrade()`)."""
    op.execute(
        "REVOKE SELECT ON meta.datasets, meta.files, meta.ingestion_runs FROM grafana_reader",
    )
    op.execute("REVOKE USAGE ON SCHEMA normalized FROM grafana_reader")
    op.execute("REVOKE USAGE ON SCHEMA meta FROM grafana_reader")
    op.execute("DROP ROLE grafana_reader")
