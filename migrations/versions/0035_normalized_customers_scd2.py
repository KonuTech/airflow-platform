"""normalized.customers -- migrate in place to SCD2 shape (D-07, SCD-01..12).

This is the phase's foundational DDL change: `normalized.customers` moves
from "one row per `customer_id`, uniqueness enforced by a real `UNIQUE`
constraint" (migration 0006) to "many rows per `customer_id` allowed, but
never two whose validity ranges overlap" (D-07/SCD-12).

(a) `CREATE EXTENSION IF NOT EXISTS btree_gist` -- the first extension this
repo has ever installed (`grep -rn "CREATE EXTENSION" migrations/` returns
zero prior matches). Runs as the CNPG superuser via `make migrate-analytics`
(Makefile's `migrate-analytics` target discovers `analytics-db-superuser`),
so no additional GRANT is needed for the extension itself.

(b) Drops migration 0006's `uq_customers_customer_id` UNIQUE constraint --
its own docstring's rationale ("`ON CONFLICT (customer_id)` requires a real
UNIQUE/exclusion constraint as its conflict target") no longer holds once
this table's publication strategy is no longer `INSERT ... ON CONFLICT`
(Phase 10 replaces `MergePublisher` with the SCD Publisher, D-07). Undoing
0006 is mandatory: a real UNIQUE constraint on `customer_id` alone would
reject the second and every subsequent SCD2 version row for the same key.

(c) `event_ts` is tightened to `NOT NULL`. It has been DB-nullable since
migration 0005 even though the app contract (`configs/datasets/customers.yaml`)
already requires it -- a NULL `event_ts` would produce an unbounded-lower
`tstzrange` for that row, silently defeating the exclusion constraint's
overlap detection for any customer whose first-ever row had a null
`event_ts` (D-03: `event_ts` doubles as `valid_from`, so it must always be
present at the DB level, not just the app-contract level, before the
exclusion constraint depends on it).

(d) Adds `valid_to` (default the far-future sentinel
`9999-12-31 00:00:00+00`, dbt's own documented `dbt_valid_to_current`
example value -- deliberately NOT `NULL`, since `NULL` would require
`COALESCE(valid_to, 'infinity')` at every query site that asks "is this row
valid at time T") and `is_current` (default `true`). Because both are added
with `server_default`s, every EXISTING row is backfilled as that customer's
first (and, until a later plan's recompute runs, only) SCD2 version with
zero extra `UPDATE` statement -- exactly D-07's "backfilled as each
customer's first SCD2 version."

(e) Adds `signup_country` (D-13's new Type-0 dimension column, nullable --
older ingested rows never carried it and this migration does not backfill a
value it does not have).

(f) Adds a generated `STORED` `validity tstzrange` column
(`tstzrange(event_ts, valid_to, '[)')`) -- queryable directly
(`WHERE validity @> now()`), debuggable in `psql`, matching this repo's
existing preference for explicit, inspectable columns over hidden
expression-only logic. Raw `op.execute` is required: Alembic's typed column
ops have no `GENERATED ALWAYS AS ... STORED` primitive.

(g) The SCD-12 constraint itself:
`EXCLUDE USING gist (customer_id WITH =, validity WITH &&)` -- two rows for
the same `customer_id` whose `validity` ranges overlap are rejected by
PostgreSQL itself, not merely by application discipline (PITFALLS #2: a
constraint that lives only in Python is not a constraint). This is a
backstop, not the primary mechanism -- later plans' recompute logic is
expected to never actually trigger it in normal operation.

(h) A partial index on `customer_id` `WHERE is_current` -- the SCD
Publisher's own steady-state read pattern ("give me the current row(s) for
key X") should never need to scan historical versions.

(i) `GRANT DELETE ON normalized.customers TO etl_app` (T-10-02, this plan's
threat model) -- scoped to exactly this one table, matching migration 0024's
dbt_app INSERT-only narrow-grant convention. Verified via a live
`aclexplode`/psql `dp` check during Task 1's own acceptance-criteria pass that
etl_app did NOT already carry DELETE before this migration ran (0005 granted
only `SELECT, INSERT, UPDATE`; no later migration widened it).

Every identifier used below (table/column/constraint/index names) is a
literal, hand-written string -- never built from row content or external
input (T-10-01, this plan's threat model) -- matching this repo's existing
migration-authorship convention (0005/0006/0022/0030 above).

Revision ID: 0035
Revises: 0034
Create Date: 2026-08-21
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0035"
down_revision = "0034"
branch_labels = None
depends_on = None

_SENTINEL = "9999-12-31 00:00:00+00"  # dbt's own dbt_valid_to_current example value


def upgrade() -> None:
    """Migrate normalized.customers in place to an SCD2-capable shape."""
    # (a) first CREATE EXTENSION this repo has ever run.
    op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")

    # (b) undo migration 0006 -- ON CONFLICT (customer_id) can no longer be
    # this table's write path once more than one row per key is legal.
    op.drop_constraint(
        "uq_customers_customer_id", "customers", schema="normalized", type_="unique"
    )

    # (c) a NULL event_ts would produce an unbounded-lower tstzrange and
    # silently defeat the exclusion constraint below.
    op.alter_column("customers", "event_ts", nullable=False, schema="normalized")

    # (d) valid_to / is_current -- server_defaults backfill every existing
    # row as a valid first SCD2 version with zero extra UPDATE.
    op.add_column(
        "customers",
        sa.Column(
            "valid_to",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text(f"'{_SENTINEL}'::timestamptz"),
        ),
        schema="normalized",
    )
    op.add_column(
        "customers",
        sa.Column(
            "is_current", sa.Boolean(), nullable=False, server_default=sa.text("true")
        ),
        schema="normalized",
    )

    # (e) D-13's new Type-0 dimension column.
    op.add_column(
        "customers",
        sa.Column("signup_country", sa.Text(), nullable=True),
        schema="normalized",
    )

    # (f) generated STORED validity column -- Alembic has no typed op for
    # GENERATED ALWAYS AS ... STORED, so this is raw DDL.
    op.execute(
        "ALTER TABLE normalized.customers "
        "ADD COLUMN validity tstzrange "
        "GENERATED ALWAYS AS (tstzrange(event_ts, valid_to, '[)')) STORED"
    )

    # (g) the SCD-12 constraint itself.
    op.create_exclude_constraint(
        "excl_customers_business_key_validity",
        "customers",
        ("customer_id", "="),
        ("validity", "&&"),
        schema="normalized",
        using="gist",
    )

    # (h) steady-state "current row(s) for this key" read path.
    op.create_index(
        "ix_customers_is_current",
        "customers",
        ["customer_id"],
        unique=False,
        schema="normalized",
        postgresql_where=sa.text("is_current"),
    )

    # (i) T-10-02: scoped to exactly this table, matching migration 0024's
    # dbt_app INSERT-only narrow-grant convention.
    op.execute("GRANT DELETE ON normalized.customers TO etl_app")


def downgrade() -> None:
    """Reverse every step of upgrade(), in reverse order."""
    op.drop_index("ix_customers_is_current", table_name="customers", schema="normalized")
    # Alembic's `op.drop_constraint(..., type_=...)` only accepts
    # {'check', 'foreignkey', 'primary', 'unique', None} -- verified live
    # (10-RESEARCH.md's own flagged Assumption A1): `type_="exclude"` raises
    # `TypeError` from `schemaobj.generic_constraint`, since Alembic's typed
    # drop-constraint op has no EXCLUDE case. Raw DDL is required here, the
    # same way (f) above needed raw DDL to ADD the generated column.
    op.execute(
        "ALTER TABLE normalized.customers "
        "DROP CONSTRAINT excl_customers_business_key_validity"
    )
    op.execute("ALTER TABLE normalized.customers DROP COLUMN validity")
    op.drop_column("customers", "signup_country", schema="normalized")
    op.drop_column("customers", "is_current", schema="normalized")
    op.drop_column("customers", "valid_to", schema="normalized")
    op.alter_column("customers", "event_ts", nullable=True, schema="normalized")
    op.create_unique_constraint(
        "uq_customers_customer_id", "customers", ["customer_id"], schema="normalized"
    )
    # Note: this migration's GRANT DELETE is intentionally NOT revoked on
    # downgrade -- REVOKE is not part of this repo's existing migration
    # downgrade convention (no prior migration revokes a grant it made), and
    # a stale DELETE grant on a downgraded table is not itself a security
    # regression (the table's shape, not its grants, is what downgrade()
    # exists to restore).
