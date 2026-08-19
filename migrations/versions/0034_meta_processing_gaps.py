"""meta.processing_gaps — D-06's explicit, SQL-queryable "no file found" record.

A backfill DagRun that finds zero matching S3 keys for its window is a genuinely different
outcome from a failure: nothing broke, there is simply no file for that historical date. Left
unrecorded, that outcome is indistinguishable from "nobody looked" — silent absence, which the
Core Value forbids. This table exists so that outcome is explicit and queryable by SQL, distinct
from a failure; other dates in the window keep processing regardless.

One row per `(dataset_id, dag_run_id)` that found nothing to process — `UNIQUE(dataset_id,
dag_run_id)` means a retried, still-empty DagRun idempotently upserts onto the SAME row rather
than duplicating it (mirrors `meta.run_stages`' own `UNIQUE(run_id, stage_name)` claim
convention, migration 0025). `backfill_id` is Airflow's own backfill identity (nullable — only a
backfill-triggered DagRun ever populates a gap row in the first place, per `gap_recorder.py`'s own
gating logic; the column stays nullable rather than NOT NULL so a future direct SQL insert is not
artificially constrained, though no code path today writes a NULL here).

Grants: `etl_app` gets `SELECT, INSERT` (this table is written by `gap_recorder.py`'s
`record_processing_gap_if_empty`, never updated or deleted — a gap, once recorded, is a permanent
historical fact); `grafana_reader` gets read-only `SELECT` (mirrors `meta.reconciliation_results`'
own grant shape, migration 0032). Zero grant to `dbt_app` — this is a Python/Airflow-side
observability surface only, matching `meta.run_stages`' own D-02 decoupling precedent.

Revision ID: 0034
Revises: 0033
Create Date: 2026-08-19
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0034"
down_revision = "0033"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create `meta.processing_gaps`, `etl_app`-write / `grafana_reader`-read only."""
    op.create_table(
        "processing_gaps",
        sa.Column("gap_id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column(
            "dataset_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.datasets.dataset_id"),
            nullable=False,
        ),
        sa.Column("dag_id", sa.Text(), nullable=False),
        sa.Column("dag_run_id", sa.Text(), nullable=False),
        # Airflow's own backfill identity, for correlation back to the triggering backfill.
        # Nullable: only a backfill-triggered DagRun ever writes a row here (gap_recorder.py's
        # own gating), but the column itself is not artificially constrained to NOT NULL.
        sa.Column("backfill_id", sa.BigInteger(), nullable=True),
        sa.Column(
            "detected_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint(
            "dataset_id", "dag_run_id", name="uq_processing_gaps_dataset_id_dag_run_id"
        ),
        schema="meta",
    )
    op.execute("GRANT SELECT, INSERT ON meta.processing_gaps TO etl_app")
    op.execute("GRANT SELECT ON meta.processing_gaps TO grafana_reader")
    # Deliberately nothing granted to dbt_app -- matches meta.run_stages' own D-02 decoupling.


def downgrade() -> None:
    """Drop `meta.processing_gaps`."""
    op.drop_table("processing_gaps", schema="meta")
