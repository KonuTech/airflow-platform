"""staging.customers -- add signup_country (D-13's Type-0 bronze column, Rule 2 gap fix).

Plan 10-01 added `signup_country` to `customers.yaml`'s `columns:` block and
to `normalized.customers`'s DDL (migration 0035), but never to the durable
bronze table `staging.customers` (migration 0022) -- a gap D-13
(`10-CONTEXT.md`) names explicitly but does not itself close. This is a
genuine, live-verified blocking gap for plan 10-04's `SCDPublisher`: Finding
F-1's per-key recompute reads its full ordered history (including
`signup_country`, needed for Type-0 dispatch, SCD-01) from
`staging.customers`, not `normalized.customers` -- so the column must exist
on the BRONZE table too, independent of gold's own already-migrated shape.

Nullable, matching migration 0035's own `normalized.customers.signup_country`
precedent -- older bronze rows, ingested before this column existed, never
carried a value and this migration does not attempt to backfill one it does
not have. All-TEXT, matching every other business column on this table
(migration 0022's own convention: bronze holds values as delivered, not yet
cast to a target type).

Deliberately narrow: this migration does NOT touch
`dataplat.pipeline.run._TARGET_COLUMNS_BY_DATASET` (the hardcoded
CSV-to-bronze column list `stage_ingest` uses to build the ephemeral
scratch-table DDL and drive `promote_to_durable_bronze`'s INSERT). That is a
SEPARATE, larger, already-existing gap (confirmed live: the real
`stage_ingest()` path already raises `ValueError` for the `customers`
dataset today, on `main`, independent of this plan, since
`ColumnContract` `signup_country` has had no corresponding `target_columns`
entry since plan 10-01 landed) -- wiring the full CSV-ingestion pipeline
touches `pipeline/run.py` and ripples into several pre-existing
`stage_ingest()`/`publish_ingest()` test fixtures across files well outside
this plan's own declared scope (`test_run_ingest.py`, `test_dated_series.py`
consumers, property tests). This migration unblocks `SCDPublisher`'s own
direct-SQL-seeded test suite only; the full pipeline gap is left as a
documented, deferred item for a future gap-closure round (see
10-04-SUMMARY.md's Issues Encountered).

Revision ID: 0037
Revises: 0036
Create Date: 2026-08-21
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0037"
down_revision = "0036"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add the nullable, all-TEXT `signup_country` column to `staging.customers`."""
    op.add_column(
        "customers",
        sa.Column("signup_country", sa.Text(), nullable=True),
        schema="staging",
    )


def downgrade() -> None:
    """Drop `signup_country` from `staging.customers`."""
    op.drop_column("customers", "signup_country", schema="staging")
