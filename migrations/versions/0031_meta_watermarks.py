"""meta.watermarks / meta.watermark_history — D-01..D-04's observational incremental cursor.

Pulled forward from Phase 9's CONTEXT.md decisions D-01..D-04 (INCR-01/02):
the watermark this migration creates is OBSERVATIONAL ONLY (D-01) — no code
path anywhere in this codebase ever uses `meta.watermarks.cursor_value` to
filter which files or rows a run picks up. It exists purely so an operator
(or a future dashboard) can answer "how fresh is this dataset's published
data?" without re-deriving it from `normalized.<dataset>` by hand.

Grain (D-03): one row per `(dataset_id, target_key)`, with `target_key`
defaulting to the single literal `'default'` this phase ever writes — a
forward-compatible column, unexercised this phase (same "built but
unexercised" pattern already established for `_BATCH_COMPLETE`).

`cursor_value` is advanced using `GREATEST(meta.watermarks.cursor_value,
EXCLUDED.cursor_value)`, never a bare `>` comparison and never an
unconditional overwrite — this is INCR-02's "`>=`, never `>`" rule enforced
structurally by the SQL itself (`record_watermark`, plan 09-02 Task 2), not
by a conditional branch that could be bypassed. A late-arriving file whose
`max(event_ts)`/`max(order_date)` is OLDER than the currently-stored cursor
therefore never regresses `cursor_value` — but D-04 still requires the
attempt to be recorded, which is exactly what `meta.watermark_history`
below is for: an unconditional, append-only audit trail, written on EVERY
`record_watermark` call, whether the cursor actually moved or not.

`stage_name`/`target_key`-style vocabulary columns are always plain
`sa.Text()`, never a native Postgres ENUM (established convention, cited in
migration 0025's own docstring).

Grants: `SELECT, INSERT, UPDATE` on `meta.watermarks` to `etl_app` (the
`ON CONFLICT ... DO UPDATE` upsert needs all three); `SELECT, INSERT` on
`meta.watermark_history` to `etl_app` (append-only, never updated in place);
`SELECT` on both to `grafana_reader`. Deliberately nothing granted to
`dbt_app` on either table — D-01/D-02's decoupling keeps the watermark
entirely Python/`etl_app`-owned, mirroring migration 0025's own "deliberately
nothing granted to dbt_app" comment verbatim.

Revision ID: 0031
Revises: 0030
Create Date: 2026-08-19
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0031"
down_revision = "0030"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create `meta.watermarks` and `meta.watermark_history`, `etl_app`-only."""
    op.create_table(
        "watermarks",
        sa.Column("watermark_id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column(
            "dataset_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.datasets.dataset_id"),
            nullable=False,
        ),
        # D-03: single 'default' target_key per dataset this phase --
        # forward-compat column, unexercised (same "built but unexercised"
        # pattern as _BATCH_COMPLETE).
        sa.Column(
            "target_key",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'default'"),
        ),
        sa.Column("cursor_value", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint(
            "dataset_id",
            "target_key",
            name="uq_watermarks_dataset_id_target_key",
        ),
        schema="meta",
    )
    op.execute("GRANT SELECT, INSERT, UPDATE ON meta.watermarks TO etl_app")
    op.execute("GRANT SELECT ON meta.watermarks TO grafana_reader")
    # Deliberately nothing granted to dbt_app -- D-01/D-02 decoupling.

    # meta.watermark_history: append-only audit (D-04) -- one row per
    # record_watermark call, whether the cursor actually moved or not.
    op.create_table(
        "watermark_history",
        sa.Column(
            "watermark_history_id",
            sa.BigInteger(),
            sa.Identity(always=True),
            primary_key=True,
        ),
        sa.Column(
            "dataset_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.datasets.dataset_id"),
            nullable=False,
        ),
        sa.Column("target_key", sa.Text(), nullable=False),
        sa.Column("old_value", sa.DateTime(timezone=True), nullable=True),
        sa.Column("new_value", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "run_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.ingestion_runs.run_id"),
            nullable=True,
        ),
        sa.Column(
            "changed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        schema="meta",
    )
    op.execute("GRANT SELECT, INSERT ON meta.watermark_history TO etl_app")
    op.execute("GRANT SELECT ON meta.watermark_history TO grafana_reader")
    # Deliberately nothing granted to dbt_app -- D-01/D-02 decoupling.


def downgrade() -> None:
    """Drop both tables (reverse order, FK-safe)."""
    op.drop_table("watermark_history", schema="meta")
    op.drop_table("watermarks", schema="meta")
