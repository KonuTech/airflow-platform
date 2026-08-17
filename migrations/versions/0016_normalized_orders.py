"""normalized.orders — the second real dataset, proving VALID-07 referential integrity live.

Business columns per D-17: `order_id`, `customer_id`, `order_date`,
`amount`. `customer_id` is deliberately NOT a database-level foreign key to
`normalized.customers` (T-08-02, accepted risk): D-16's default orphan-order
handling is `QUARANTINE_RECORD` — an order referencing a not-yet-loaded
customer must still be able to publish, with the orphan row instead routed
to `meta.rejected_records` (`error_type=REFERENTIAL_ORPHAN`) by a later
plan's application-level `ReferentialIntegrityBarrier`. A DB-level FK here
would make that impossible: PostgreSQL would reject the whole INSERT instead
of letting the barrier stage decide per-row.

The six embedded lineage columns are copied verbatim from migration 0005
(`normalized.customers`) — the same pattern every `normalized.*`/
`warehouse.*` table carries (ARCHITECTURE.md §2.3).

`ix_orders_customer_id` is a plain, non-unique index for the referential
barrier's anti-join performance — not a uniqueness constraint, matching
`normalized.customers.customer_id`'s original (pre-migration-0006) index
shape, since `orders` has no analogous `ON CONFLICT` publish target needing
a real unique constraint here.

Revision ID: 0016
Revises: 0015
Create Date: 2026-08-17
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0016"
down_revision = "0015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create `normalized.orders` (schema `normalized` already exists from migration 0005)."""
    op.create_table(
        "orders",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        # Business columns (D-17).
        sa.Column("order_id", sa.Integer(), nullable=False),
        # Deliberately NOT a DB-level FK to normalized.customers -- see
        # module docstring (D-16, T-08-02).
        sa.Column("customer_id", sa.Integer(), nullable=False),
        sa.Column("order_date", sa.Date(), nullable=True),
        sa.Column("amount", sa.Numeric(), nullable=True),
        # Embedded lineage columns, verbatim from migration 0005.
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
        sa.Column(
            "_ingested_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        schema="normalized",
    )
    op.create_index(
        "ix_orders_customer_id",
        "orders",
        ["customer_id"],
        unique=False,
        schema="normalized",
    )
    op.execute("GRANT SELECT, INSERT, UPDATE ON normalized.orders TO etl_app")


def downgrade() -> None:
    """Drop `normalized.orders`. Never drops the schema."""
    op.drop_table("orders", schema="normalized")
