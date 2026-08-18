"""staging.customers / staging.orders — durable (LOGGED), cumulative, append-only bronze tables.

Phase 08.1's D-01 (`08.1-CONTEXT.md`): the dbt bronze-to-silver split needs a
real, durable bronze layer dbt can read repeatedly across runs — the existing
`staging.<dataset>__r<run_id>` throwaway tables `StagingLoader` creates and
drops per attempt (migration 0007's docstring, `dataplat.load.staging`) exist
only for the duration of one ingestion-run attempt and are gone before dbt
could ever see them. These two tables are new, separate, Alembic-owned
objects living directly at `staging.customers`/`staging.orders` (no
`__r<run_id>` suffix): `etl_app`'s publish path appends the run's validated
rows into them, once per run, and they simply accumulate forever.

Deliberately `LOGGED` (Postgres's default persistence, Pitfall 4) — never
`UNLOGGED`. `StagingLoader`'s own scratch-buffer DDL uses `UNLOGGED` because
that table is dropped before the transaction commits either way; these
tables must survive a crash and a restart, so `UNLOGGED`'s "may be truncated
after an unclean shutdown" semantics would silently violate durability.

All-TEXT business columns, matching `StagingLoader`'s own convention
(`dataplat/load/staging.py`, `business_columns_ddl`): the values arriving
here are already validated/normalized strings from Phase 6's pipeline, not
yet cast to `normalized.*`'s typed columns — bronze holds them as delivered.

Deliberately **no** UNIQUE/PK constraint on `customer_id`/`order_id` — bronze
is append-only and allowed to carry duplicates across runs (D-07: dedup is
silver/dbt's job, not bronze's). `etl_app` gets `SELECT, INSERT` only — never
`UPDATE`/`DELETE` — extending README §63's raw-immutability language to
bronze (T-08.1-03, this plan's threat model, accepted residual risk).

The six embedded lineage columns are copied verbatim from
`migrations/versions/0005_normalized_customers.py`, including their explicit
`ForeignKey`s — the same table-shape convention every `normalized.*`/
`warehouse.*` table in this codebase already carries (ARCHITECTURE.md §2.3).

Revision ID: 0022
Revises: 0021
Create Date: 2026-08-18
"""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0022"
down_revision = "0021"
branch_labels = None
depends_on = None


def _lineage_columns() -> tuple[sa.Column[Any], ...]:
    """Return a fresh set of the six embedded lineage columns.

    A `sa.Column` instance binds itself to whichever `Table` it is first
    attached to — reusing the same instances across `op.create_table("customers", ...)`
    and `op.create_table("orders", ...)` below would raise
    "Column object ... already assigned to Table", so each table gets its
    own freshly-constructed set.
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
    """Create `staging.customers`/`staging.orders`: LOGGED, cumulative, no business-key UNIQUE."""
    op.create_table(
        "customers",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        # All-TEXT business columns — mirrors StagingLoader's own convention.
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
        schema="staging",
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
        schema="staging",
    )
    op.execute("GRANT SELECT, INSERT ON staging.customers, staging.orders TO etl_app")


def downgrade() -> None:
    """Drop both tables, reverse order."""
    op.drop_table("orders", schema="staging")
    op.drop_table("customers", schema="staging")
