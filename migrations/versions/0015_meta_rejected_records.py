"""meta.rejected_records — quarantined rows, resolved only via whole-batch backfill (D-01/D-04/D-05).

`resolution_type` disambiguates D-04's exactly-2-state lifecycle
(`PENDING`/`RESOLVED`) into three values: `PENDING`, `REDRIVEN` (resolved via
an Airflow backfill run completing), `DISCARDED` (resolved via an explicit
batch-level operator action). There is deliberately no per-row edit path
anywhere in this codebase — resolution changes are always a whole-batch side
effect (D-04's hard constraint), never a single-row UPDATE issued from an
API/UI convenience.

`batch_id` is a direct, non-deferred FK to `meta.batches` — not in
ARCHITECTURE.md's original speculative sketch, but required here because
`resolve_rejected_records_for_batch` (this phase's Task 2 Protocol addition)
resolves an entire batch's pending rejects in one `WHERE batch_id = %s AND
resolution_type = 'PENDING'` statement; without this column that method has
no batch-scoped predicate to run against.

`resolved_by_run_id` is a direct nullable FK to `meta.ingestion_runs` (not
deferred, unlike migration 0004's `schema_version_id`) since
`meta.ingestion_runs` already exists by this point in the migration chain —
it is how lineage answers "was this row ever fixed, and by which run" (D-05).

Grants are `SELECT, INSERT, UPDATE` only — no `DELETE`, matching migration
0014's identical D-04 rationale.

Revision ID: 0015
Revises: 0014
Create Date: 2026-08-17
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0015"
down_revision = "0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create `meta.rejected_records`."""
    op.create_table(
        "rejected_records",
        sa.Column("rejected_record_id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column(
            "run_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.ingestion_runs.run_id"),
            nullable=False,
        ),
        sa.Column(
            "file_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.files.file_id"),
            nullable=False,
        ),
        # Not in ARCHITECTURE.md's original sketch -- required by
        # resolve_rejected_records_for_batch's own WHERE batch_id = %s clause
        # (this plan's Task 2 Protocol signature).
        sa.Column(
            "batch_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.batches.batch_id"),
            nullable=False,
        ),
        sa.Column("source_row_number", sa.BigInteger(), nullable=False),
        sa.Column("source_byte_offset", sa.BigInteger(), nullable=True),
        sa.Column("raw_line", sa.Text(), nullable=False),
        sa.Column("error_type", sa.Text(), nullable=False),
        sa.Column("error_column", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=False),
        sa.Column(
            "rejected_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        # D-04's 2-state lifecycle (PENDING/RESOLVED) disambiguated into
        # PENDING/REDRIVEN/DISCARDED by this column -- see module docstring.
        sa.Column(
            "resolution_type",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'PENDING'"),
        ),
        # Direct, non-deferred FK -- meta.ingestion_runs already exists by
        # this point (unlike migration 0004's deferred schema_version_id).
        sa.Column(
            "resolved_by_run_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.ingestion_runs.run_id"),
            nullable=True,
        ),
        schema="meta",
    )
    op.create_index(
        "ix_rejected_records_batch_resolution",
        "rejected_records",
        ["batch_id", "resolution_type"],
        schema="meta",
    )
    # D-04: no DELETE, ever.
    op.execute("GRANT SELECT, INSERT, UPDATE ON meta.rejected_records TO etl_app")


def downgrade() -> None:
    """Drop `meta.rejected_records`."""
    op.drop_table("rejected_records", schema="meta")
