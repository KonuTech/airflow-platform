"""etl_app -- read access to `silver.*` (closes 08.1-13's live-cluster gap).

08.1-13's own live-cluster E2E gate is the first place `publish`
(`csv-processor`'s `dataplat publish` CLI, run as the `etl_app` role via its
`DATAPLAT_DB_DSN` Vault credential) ever executes ``SELECT ... FROM
silver.customers`` against the real, Vault-authenticated `etl_app` role --
every earlier plan (08.1-10 onward) proved this against a testcontainers
superuser DSN, never `etl_app` itself (the same category of gap migration
0028's own docstring already documents for `dbt_app`/`meta.datasets`).

Migration 0021 made `dbt_app` the OWNER of schema `silver`, granting it
`USAGE, CREATE` -- `etl_app` was never granted anything on `silver` at all,
so a live `dataplat publish --dataset customers` run fails immediately with
`psycopg.errors.InsufficientPrivilege: permission denied for schema
silver`. This is a straightforward least-privilege read grant, the
DEFAULT-PRIVILEGES-observant sibling of exactly what migration 0021 already
did for `dbt_app` reading `staging.*` -- `etl_app` needs `SELECT` only
(never `INSERT`/`UPDATE`/`DELETE`: `publish` reads FROM silver, it never
writes there -- D-08's silver-is-dbt's-alone write boundary is unaffected),
scoped to `silver.customers`/`silver.orders` (today's two tables) plus a
default-privileges rule so a future `silver` table `dbt_app` creates is
readable by `etl_app` with no follow-up migration, mirroring 0021's own
`ALTER DEFAULT PRIVILEGES IN SCHEMA staging` pattern -- except this one
must be scoped `FOR ROLE dbt_app` (the actual object owner/creator of every
`silver` table), not the unqualified form, since `dbt_app` -- not the
migration-running role -- is who creates future silver tables via its own
`dbt build` DDL.

Revision ID: 0029
Revises: 0028
Create Date: 2026-08-19
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "0029"
down_revision = "0028"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Grant etl_app read-only access to silver (today's tables + any dbt_app creates later)."""
    op.execute("GRANT USAGE ON SCHEMA silver TO etl_app")
    op.execute("GRANT SELECT ON silver.customers, silver.orders TO etl_app")
    op.execute(
        "ALTER DEFAULT PRIVILEGES FOR ROLE dbt_app IN SCHEMA silver "
        "GRANT SELECT ON TABLES TO etl_app"
    )


def downgrade() -> None:
    """Reverse of upgrade(): default privileges rule, then table grants, then schema USAGE."""
    op.execute(
        "ALTER DEFAULT PRIVILEGES FOR ROLE dbt_app IN SCHEMA silver "
        "REVOKE SELECT ON TABLES FROM etl_app"
    )
    op.execute("REVOKE SELECT ON silver.customers, silver.orders FROM etl_app")
    op.execute("REVOKE USAGE ON SCHEMA silver FROM etl_app")
