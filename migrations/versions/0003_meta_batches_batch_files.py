"""meta.batches and meta.batch_files — the arrival grouping, one table even in the slice.

Column shapes are ARCHITECTURE.md §2.1 lines 184-201. Included even though
the vertical slice is one-file-one-batch: it is one table now, versus adding
a `NOT NULL` FK to already-populated tables across three later phases.

Revision ID: 0003
Revises: 0002
Create Date: 2026-08-13
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create `meta.batches` then the `meta.batch_files` join table."""
    op.create_table(
        "batches",
        sa.Column("batch_id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column(
            "dataset_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.datasets.dataset_id"),
            nullable=False,
        ),
        sa.Column("batch_key", sa.Text(), nullable=False),
        sa.Column("business_date", sa.Date(), nullable=True),
        sa.Column("manifest_uri", sa.Text(), nullable=True),
        sa.Column("expected_file_count", sa.Integer(), nullable=True),
        sa.Column("expected_row_count", sa.BigInteger(), nullable=True),
        sa.Column("control_totals", JSONB(), nullable=True),
        sa.Column("completion_marker_uri", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.UniqueConstraint("dataset_id", "batch_key", name="uq_batches_dataset_batch_key"),
        schema="meta",
    )
    op.execute("GRANT SELECT, INSERT, UPDATE ON meta.batches TO etl_app")

    op.create_table(
        "batch_files",
        sa.Column(
            "batch_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.batches.batch_id"),
            primary_key=True,
        ),
        sa.Column(
            "file_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.files.file_id"),
            primary_key=True,
        ),
        sa.Column("sequence_no", sa.Integer(), nullable=True),
        schema="meta",
    )
    op.execute("GRANT SELECT, INSERT, UPDATE ON meta.batch_files TO etl_app")


def downgrade() -> None:
    """Drop `meta.batch_files` then `meta.batches`."""
    op.drop_table("batch_files", schema="meta")
    op.drop_table("batches", schema="meta")
