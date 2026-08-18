"""meta.run_stages — D-17's two-phase claim state machine.

Pulled forward from Phase 9's original ARCHITECTURE.md design (lines
233-236): a 3-task pipeline split (`stage` / `publish`, per plan 08.1-10)
needs somewhere to record progress independently of the stage-load claim
already tracked on `meta.ingestion_runs`. Column shape mirrors
`meta.ingestion_runs`' own heartbeated crash-recovery lease (migration
0004): `status`/`lease_expires_at`/`started_at`/`finished_at`.

This phase writes exactly two `stage_name` values: `"STAGE_LOAD"` and
`"PUBLISH"`. The remaining values from ARCHITECTURE.md's original sketch
(`DISCOVER`/`INSPECT`/`PARSE`/`VALIDATE`/`NORMALIZE`/`DEDUP`/`RECONCILE`) are
reserved for a future phase and never written by this one — `stage_name` is
still plain `Text`, app-validated (never a native Postgres ENUM, matching
`rule_type`/`outcome`'s established convention), so adding one later needs
no migration.

`checkpoint` (JSONB, nullable) is reserved and unused this phase — the
original ARCHITECTURE.md design's future byte-offset-resume slot. No code
path writes it yet.

`UNIQUE(run_id, stage_name)` is the load-bearing constraint: it is what lets
`claim_run_stage` (plan 08.1-07) use `INSERT ... ON CONFLICT (run_id,
stage_name) DO UPDATE` as an atomic claim-or-take-over primitive.

Grants: `SELECT, INSERT, UPDATE` to `etl_app` only (this table is claimed and
heartbeated by `stage_ingest`/`publish_ingest`, both running as `etl_app`,
plan 08.1-10). `dbt_app` gets nothing here — D-02's decoupling keeps dbt's
own task fully separate from the Python claim mechanism, verified by
`test_dbt_app_has_no_grant_on_run_stages`.

Revision ID: 0025
Revises: 0024
Create Date: 2026-08-18
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision = "0025"
down_revision = "0024"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create `meta.run_stages`, `etl_app`-only, `UNIQUE(run_id, stage_name)`."""
    op.create_table(
        "run_stages",
        sa.Column("run_stage_id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column(
            "run_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.ingestion_runs.run_id"),
            nullable=False,
        ),
        # App-validated vocabulary: this phase writes only STAGE_LOAD/PUBLISH
        # -- the remaining ARCHITECTURE.md sketch values are reserved for a
        # future phase. Never a native Postgres ENUM.
        sa.Column("stage_name", sa.Text(), nullable=False),
        # App-validated vocabulary: PENDING/RUNNING/SUCCEEDED/FAILED.
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("pod_name", sa.Text(), nullable=True),
        sa.Column("try_number", sa.Integer(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        # Reserved, unused this phase -- the future byte-offset-resume slot.
        # No code path writes it yet.
        sa.Column("checkpoint", JSONB(), nullable=True),
        sa.UniqueConstraint("run_id", "stage_name", name="uq_run_stages_run_id_stage_name"),
        schema="meta",
    )
    op.execute("GRANT SELECT, INSERT, UPDATE ON meta.run_stages TO etl_app")
    # Deliberately nothing granted to dbt_app -- D-02's decoupling.


def downgrade() -> None:
    """Drop `meta.run_stages`."""
    op.drop_table("run_stages", schema="meta")
