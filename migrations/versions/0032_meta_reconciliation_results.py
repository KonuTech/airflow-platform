"""meta.reconciliation_results — D-20..D-24's per-file-per-hop source-to-target accounting.

Grain (D-24): one row per `(file_id, hop)` — `hop` is app-validated
vocabulary (never a native Postgres ENUM, matching this repo's
`stage_name`/`rule_type`/`outcome` convention, cited verbatim in migrations
0025/0024's own docstrings): `'raw_bronze'`, `'bronze_silver'`,
`'silver_gold'`. `file_id` is nullable only defensively — every hop this
phase populates it for real.

**D-22's exact accounting formula** — the one explicit correctness rule from
CONTEXT.md that a schema/migration alone cannot enforce, so it is stated here
verbatim, mirroring how migration 0024's own docstring documents the
schema-shape correction it makes concrete:

    discrepancy = input_count - (output_count + rejected_count + dedup_count)

A non-zero `discrepancy` flags a genuine, UNEXPLAINED loss only AFTER
quarantine (`rejected_count`) and dedup (`dedup_count`) are already netted
out of the naive `input_count - output_count` difference — never a naive
raw input-vs-output diff on its own. `control_total_discrepancy` is a
SEPARATE, VALID-06/D-23-scoped figure (`expected_row_count - output_count`),
populated only at the `raw_bronze` hop when a `_BATCH_COMPLETE` manifest
applied to the file.

Grants (mirrors migration 0024's exact grant pattern, lines 118-124 of that
file): `dbt_app` gets `SELECT, INSERT` ONLY — a dbt post-hook only ever
appends the `bronze_silver` hop's own row, never revises history, so it never
gets `UPDATE`/`DELETE`. `dbt_app` already has `GRANT USAGE ON SCHEMA meta`
from migrations 0021/0024 — this migration does NOT re-grant it (a dead-weight
duplicate statement, the same caution RESEARCH.md flags). `etl_app` gets
`SELECT, INSERT` (it writes the `raw_bronze` and `silver_gold` hops).
`grafana_reader` gets read-only `SELECT`.

Revision ID: 0032
Revises: 0031
Create Date: 2026-08-19
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0032"
down_revision = "0031"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create `meta.reconciliation_results`, `dbt_app` INSERT-only, `etl_app` full read/write."""
    op.create_table(
        "reconciliation_results",
        sa.Column(
            "reconciliation_id",
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
        # Nullable only defensively (D-24's grain is per-file-per-hop) --
        # every hop this phase populates it for real.
        sa.Column(
            "file_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.files.file_id"),
            nullable=True,
        ),
        # App-validated vocabulary -- never a native Postgres ENUM (this
        # repo's established convention): 'raw_bronze' | 'bronze_silver' |
        # 'silver_gold'.
        sa.Column("hop", sa.Text(), nullable=False),
        sa.Column("input_count", sa.BigInteger(), nullable=False),
        sa.Column("output_count", sa.BigInteger(), nullable=False),
        sa.Column(
            "rejected_count",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "dedup_count",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        # No server_default -- every caller computes and supplies this,
        # using D-22's exact formula documented in the module docstring
        # above.
        sa.Column("discrepancy", sa.BigInteger(), nullable=False),
        sa.Column("sum_column", sa.Text(), nullable=True),
        sa.Column("sum_input", sa.Numeric(), nullable=True),
        sa.Column("sum_output", sa.Numeric(), nullable=True),
        sa.Column("checksum_input", sa.Text(), nullable=True),
        sa.Column("checksum_output", sa.Text(), nullable=True),
        sa.Column("min_input", sa.DateTime(timezone=True), nullable=True),
        sa.Column("max_input", sa.DateTime(timezone=True), nullable=True),
        sa.Column("min_output", sa.DateTime(timezone=True), nullable=True),
        sa.Column("max_output", sa.DateTime(timezone=True), nullable=True),
        sa.Column("key_count_input", sa.BigInteger(), nullable=True),
        sa.Column("key_count_output", sa.BigInteger(), nullable=True),
        # VALID-06/D-23: populated only at the raw_bronze hop when a
        # _BATCH_COMPLETE manifest applied to the file.
        sa.Column("expected_row_count", sa.BigInteger(), nullable=True),
        sa.Column("expected_checksum", sa.Text(), nullable=True),
        sa.Column("control_total_discrepancy", sa.BigInteger(), nullable=True),
        sa.Column(
            "checked_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        schema="meta",
    )
    # INSERT-only for dbt_app (mirrors migration 0024's T-08.1-10 precedent):
    # a dbt post-hook only ever appends its own bronze_silver row.
    # GRANT USAGE ON SCHEMA meta TO dbt_app already exists from migrations
    # 0021/0024 -- deliberately NOT re-granted here.
    op.execute("GRANT SELECT, INSERT ON meta.reconciliation_results TO dbt_app")
    # etl_app writes raw_bronze (a later plan) and silver_gold (this plan)
    # hops.
    op.execute("GRANT SELECT, INSERT ON meta.reconciliation_results TO etl_app")
    op.execute("GRANT SELECT ON meta.reconciliation_results TO grafana_reader")


def downgrade() -> None:
    """Drop `meta.reconciliation_results`."""
    op.drop_table("reconciliation_results", schema="meta")
