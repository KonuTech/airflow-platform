"""meta.ingestion_runs — the central table (§24, §37, §62, §82, §83).

Column shapes and nullability are ARCHITECTURE.md §2.1 lines 203-227, sharpened
by 03-RESEARCH.md Pattern 1's verbatim nullability call for the run-identity
columns. `idempotency_key` is what makes retries free (Q7): a unique
constraint, not merely an index, so a duplicate run attempt fails at the
database rather than racing another writer.

`schema_version_id` is the one deferred foreign key in this phase's
migrations: `meta.schema_versions` is an explicitly post-slice table
(CONTEXT.md D-05, ARCHITECTURE.md §2.4) that a later phase's own migration
creates. The column lands now — nullable, no `ForeignKey` — so the later
migration can `op.create_foreign_key` against it without an `ALTER TABLE ...
ADD COLUMN` first. Every other column here is not deferred.

Revision ID: 0004
Revises: 0003
Create Date: 2026-08-13
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create `meta.ingestion_runs`, with `schema_version_id` deliberately unconstrained."""
    op.create_table(
        "ingestion_runs",
        sa.Column("run_id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column("idempotency_key", sa.Text(), nullable=False, unique=True),
        sa.Column(
            "dataset_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.datasets.dataset_id"),
            nullable=False,
        ),
        sa.Column(
            "file_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.files.file_id"),
            nullable=True,
        ),
        sa.Column(
            "batch_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.batches.batch_id"),
            nullable=True,
        ),
        sa.Column(
            "config_version_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.config_versions.config_version_id"),
            nullable=False,
        ),
        # Deliberately unconstrained: meta.schema_versions (the referent for
        # this column) does not exist until a later phase's migration
        # (CONTEXT.md D-05, ARCHITECTURE.md §2.4). The column lands now so
        # the design stays coherent; a later migration adds the constraint
        # via op.create_foreign_key once that table exists.
        sa.Column("schema_version_id", sa.BigInteger(), nullable=True),
        sa.Column("processor_version", sa.Text(), nullable=False),
        sa.Column("processor_image_digest", sa.Text(), nullable=False),
        sa.Column("dag_id", sa.Text(), nullable=True),
        sa.Column("dag_run_id", sa.Text(), nullable=True),
        sa.Column("task_id", sa.Text(), nullable=True),
        sa.Column("map_index", sa.Integer(), nullable=True),
        # Deliberately excluded from idempotency_key — a retried try of the
        # same logical run must resolve to the same key.
        sa.Column("try_number", sa.Integer(), nullable=True),
        # NULL for asset/manual triggers — never assume a scheduled run.
        sa.Column("logical_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("data_interval_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("data_interval_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("k8s_namespace", sa.Text(), nullable=True),
        sa.Column("k8s_pod_name", sa.Text(), nullable=True),
        sa.Column("k8s_node_name", sa.Text(), nullable=True),
        sa.Column("trace_id", sa.Text(), nullable=True),
        sa.Column("span_id", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        # Heartbeated; enables crashed-pod takeover (§37).
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_ms", sa.BigInteger(), nullable=True),
        sa.Column("rows_read", sa.BigInteger(), nullable=True),
        sa.Column("rows_parsed", sa.BigInteger(), nullable=True),
        sa.Column("rows_valid", sa.BigInteger(), nullable=True),
        sa.Column("rows_invalid", sa.BigInteger(), nullable=True),
        sa.Column("rows_deduplicated", sa.BigInteger(), nullable=True),
        sa.Column("rows_loaded", sa.BigInteger(), nullable=True),
        sa.Column("error_type", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("error_detail", JSONB(), nullable=True),
        sa.Column("report_uri", sa.Text(), nullable=True),
        sa.Column(
            "replay_of_run_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.ingestion_runs.run_id"),
            nullable=True,
        ),
        schema="meta",
    )
    op.execute("GRANT SELECT, INSERT, UPDATE ON meta.ingestion_runs TO etl_app")


def downgrade() -> None:
    """Drop `meta.ingestion_runs`."""
    op.drop_table("ingestion_runs", schema="meta")
