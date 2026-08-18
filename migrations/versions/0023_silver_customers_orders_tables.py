"""silver.customers / silver.orders — dbt's incremental-model target, DB-owned DDL (D-14).

Phase 08.1's architectural decision (this plan's own objective note, cross-
referencing 08.1-CONTEXT.md D-11/D-14): silver's tables are created by
**Alembic, not dbt's own materialization**. A later migration (08.1-09)
extends `meta.v_customers_lineage` with `LEFT JOIN silver.customers` — a
`CREATE VIEW` that references a table only dbt would create cannot run
before dbt's first execution, and migrations must all apply cleanly at
deploy time, before any DAG has ever run. Alembic owning the silver DDL
(mirroring how it already owns `normalized.customers`/`normalized.orders`,
migrations 0005/0016) also gives D-14's "hard constraint, not just dbt's
`unique_key` logic" its strongest form: the constraint exists from the very
first deploy, not merely after dbt's first successful build.

dbt's own incremental model (plan 08.1-08) targets this pre-existing table
via `contract: enforced: true`, which *validates* the model's column list
against the table rather than creating it.

Column shape mirrors `staging.customers`/`staging.orders` (migration 0022)
exactly — same all-TEXT business columns, same six lineage columns (verbatim
from `migrations/versions/0005_normalized_customers.py`, including their
explicit `ForeignKey`s) — plus two additions: `_dbt_loaded_at` (D-11's dbt
bookkeeping column; nullable, never server-defaulted here, since dbt's own
model SELECT populates it on every write, never Postgres) and a real
`UNIQUE` constraint on the business key (D-14: `customer_id` / `order_id`).

`ALTER TABLE ... OWNER TO dbt_app` after each `CREATE TABLE`: table
ownership, not merely a grant — dbt's `delete+insert` incremental strategy
needs DML rights `dbt_app` already has via schema ownership from migration
0021, but explicit table ownership removes any doubt.

Revision ID: 0023
Revises: 0022
Create Date: 2026-08-18
"""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0023"
down_revision = "0022"
branch_labels = None
depends_on = None


def _lineage_columns() -> tuple[sa.Column[Any], ...]:
    """Return a fresh set of the six embedded lineage columns.

    Mirrors `migrations/versions/0022_staging_customers_orders_durable_tables.py`'s
    own `_lineage_columns()` helper — a `sa.Column` instance binds itself to
    whichever `Table` it is first attached to, so each `op.create_table` call
    below needs its own freshly-constructed set, never a shared module-level
    tuple.
    """
    return (
        sa.Column(
            "_run_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.ingestion_runs.run_id"),
            nullable=False,
        ),
        sa.Column(
            "_file_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.files.file_id"),
            nullable=False,
        ),
        sa.Column(
            "_batch_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.batches.batch_id"),
            nullable=False,
        ),
        sa.Column("_source_row_number", sa.BigInteger(), nullable=False),
        sa.Column("_record_hash", sa.LargeBinary(), nullable=False),
        sa.Column(
            "_record_hash_version",
            sa.SmallInteger(),
            nullable=False,
            server_default=sa.text("1"),
        ),
    )


def upgrade() -> None:
    """Create `silver.customers`/`silver.orders`: same shape as staging + UNIQUE + dbt ownership."""
    op.create_table(
        "customers",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column("customer_id", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("country", sa.Text(), nullable=False),
        sa.Column("birth_date", sa.Text(), nullable=True),
        sa.Column("event_ts", sa.Text(), nullable=True),
        *_lineage_columns(),
        sa.Column(
            "_ingested_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        # D-11: dbt's own bookkeeping column — dbt's model SELECT populates
        # it on every write, never a Postgres server_default.
        sa.Column("_dbt_loaded_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("customer_id", name="uq_silver_customers_customer_id"),
        schema="silver",
    )
    op.create_table(
        "orders",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column("order_id", sa.Text(), nullable=False),
        sa.Column("customer_id", sa.Text(), nullable=False),
        sa.Column("order_date", sa.Text(), nullable=True),
        sa.Column("amount", sa.Text(), nullable=True),
        *_lineage_columns(),
        sa.Column(
            "_ingested_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("_dbt_loaded_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("order_id", name="uq_silver_orders_order_id"),
        schema="silver",
    )
    op.execute("ALTER TABLE silver.customers OWNER TO dbt_app")
    op.execute("ALTER TABLE silver.orders OWNER TO dbt_app")


def downgrade() -> None:
    """Drop both tables, reverse order."""
    op.drop_table("orders", schema="silver")
    op.drop_table("customers", schema="silver")
