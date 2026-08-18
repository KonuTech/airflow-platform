"""meta.dedup_audit / meta.dedup_decisions — dbt's own dedup audit trail (D-09, DEDUP-04).

**Schema-shape correction this migration makes concrete** (plan 08.1-05's own
Objective note, RESEARCH.md Pattern 2): the original ARCHITECTURE.md sketch
(line 238) assumed one `meta.dedup_audit` row per bronze `run_id` — a 1:1
assumption from Phase 9's now-superseded design. D-05's watermark-driven dbt
batching breaks that: one `dbt build` invocation can span multiple bronze
commits. This migration uses dbt's own `invocation_id` (a UUID Jinja exposes)
as the row identity, with a `min_run_id`/`max_run_id` range instead of a
single FK. Neither `min_run_id` nor `max_run_id` carries a `ForeignKey` —
dbt's own transaction writes these, not `etl_app`'s, and the referenced
`meta.ingestion_runs` rows may not even exist yet from dbt's point of view at
write time.

`meta.dedup_decisions.dedup_audit_id` IS a real `ForeignKey` back to
`meta.dedup_audit` — that one is safe since both tables are written by the
SAME dbt transaction. `reason` is app-validated vocabulary (never a native
Postgres ENUM, matching this repo's `rule_type`/`outcome` convention from
migrations 0009/0014): `EXACT_DUP_IN_FILE`, `EXACT_DUP_CROSS_BATCH`,
`SUPERSEDED_BY_NEWER`, `LOWER_SOURCE_PRIORITY`, `SCD_NO_CHANGE`.

Grants are INSERT-only for `dbt_app` (T-08.1-10 mitigation) — a dbt post-hook
only ever appends new audit rows, never revises history, so `dbt_app` never
gets `UPDATE`/`DELETE` on either table. `etl_app`/`grafana_reader` get
read-only `SELECT` for reconciliation/dashboards.

`dbt_app` has no `USAGE` on schema `meta` yet — migration 0021 only granted it
`staging`/`silver`. Without schema-level `USAGE`, every table grant below
would be inert (the exact bug migration 0008's own docstring documents for
`etl_app`/`normalized`, and `test_etl_app_can_actually_use_the_schemas_it_
has_table_grants_in` guards against for `etl_app`) — so this migration grants
it here, scoped to the one schema `dbt_app` now needs a narrow slice of.

Revision ID: 0024
Revises: 0023
Create Date: 2026-08-18
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision = "0024"
down_revision = "0023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create `meta.dedup_audit` and `meta.dedup_decisions`, `dbt_app` INSERT-only."""
    op.create_table(
        "dedup_audit",
        sa.Column("dedup_audit_id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column(
            "dataset_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.datasets.dataset_id"),
            nullable=False,
        ),
        # dbt's own `invocation_id` Jinja variable (a UUID string) -- the row
        # identity replacing the original run_id-FK sketch. See module
        # docstring's "Schema-shape correction" note.
        sa.Column("dbt_invocation_id", sa.Text(), nullable=False),
        sa.Column("model_name", sa.Text(), nullable=False),
        # Plain BigInteger, deliberately NOT a ForeignKey -- dbt's own
        # transaction writes these, and the referenced ingestion_runs rows
        # may not exist yet from dbt's point of view at write time.
        sa.Column("min_run_id", sa.BigInteger(), nullable=True),
        sa.Column("max_run_id", sa.BigInteger(), nullable=True),
        sa.Column("records_received", sa.BigInteger(), nullable=False),
        sa.Column("records_accepted", sa.BigInteger(), nullable=False),
        sa.Column(
            "records_rejected",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("records_deduplicated", sa.BigInteger(), nullable=False),
        sa.Column(
            "run_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        schema="meta",
    )
    op.create_table(
        "dedup_decisions",
        sa.Column(
            "dedup_decision_id",
            sa.BigInteger(),
            sa.Identity(always=True),
            primary_key=True,
        ),
        # Safe FK: both tables are written by the same dbt transaction.
        sa.Column(
            "dedup_audit_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.dedup_audit.dedup_audit_id"),
            nullable=False,
        ),
        sa.Column("record_hash", sa.LargeBinary(), nullable=False),
        sa.Column("business_key", JSONB(), nullable=False),
        sa.Column(
            "kept_file_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.files.file_id"),
            nullable=True,
        ),
        sa.Column("kept_source_row", sa.BigInteger(), nullable=True),
        # Completes the original ARCHITECTURE.md sketch -- a dropped row's
        # file identity is required for the audit to be traceable, symmetric
        # with kept_file_id.
        sa.Column(
            "dropped_file_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.files.file_id"),
            nullable=True,
        ),
        sa.Column("dropped_source_row", sa.BigInteger(), nullable=True),
        # App-validated vocabulary -- never a native Postgres ENUM (this
        # repo's established convention, migrations 0009/0014's own
        # docstrings): EXACT_DUP_IN_FILE, EXACT_DUP_CROSS_BATCH,
        # SUPERSEDED_BY_NEWER, LOWER_SOURCE_PRIORITY, SCD_NO_CHANGE.
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        schema="meta",
    )
    # Without this, the GRANTs below are inert -- see module docstring.
    op.execute("GRANT USAGE ON SCHEMA meta TO dbt_app")
    # INSERT-only (T-08.1-10): a dbt post-hook only ever appends new audit
    # rows, never revises history.
    op.execute("GRANT SELECT, INSERT ON meta.dedup_audit, meta.dedup_decisions TO dbt_app")
    # Read access for reconciliation/dashboards, never write.
    op.execute("GRANT SELECT ON meta.dedup_audit, meta.dedup_decisions TO etl_app, grafana_reader")


def downgrade() -> None:
    """Drop both tables (reverse order) and revoke `dbt_app`'s schema-level USAGE on `meta`."""
    op.drop_table("dedup_decisions", schema="meta")
    op.drop_table("dedup_audit", schema="meta")
    op.execute("REVOKE USAGE ON SCHEMA meta FROM dbt_app")
