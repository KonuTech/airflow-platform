"""meta.dbt_processed_runs -- the silver models' exact-eligibility claim ledger.

Root cause (debug/ci-pipeline-ingestion-timeout ROUND 16, finding 21, live
run 33103279876): both silver models' incremental filter was `_run_id >
max(_run_id) from {{ this }}` -- a GLOBAL max watermark. Any run whose bronze
rows commit AFTER a higher `_run_id` has already been dbt-built is below the
floor forever and is never selected by any later build: observed live as the
sweep test's late-file replay run (run 42) staged late in the replay wave and
permanently absent from `silver.customers` even though the run itself reached
SUCCEEDED with 50 bronze rows. The same shape silently drops GENUINELY NEW
rows whenever a stage retry or lease-reclaim (the ROUND 15 (20a)
wait-and-reclaim path makes this routine) completes after a higher run's dbt
pass -- a direct no-silent-drops Core Value violation, not merely a lineage
attribution artifact.

Fix shape (this migration is the schema half; dbt/macros/
claim_dbt_processed_runs.sql + the two silver models are the behavior half):
each `dbt build` invocation CLAIMS, via a pre-hook INSERT in the SAME
transaction as the model's own materialization, every bronze `_run_id` not
yet present in this ledger; the model's incremental filter then selects
exactly the rows claimed by ITS OWN transaction (`claimed_txid =
txid_current()` -- transaction-local database identity, deliberately NEVER a
Jinja-rendered `{{ invocation_id }}` literal, which dbt's partial-parsing
cache can freeze stale across invocations; see
reconciliation_post_hook.sql's own empirically-verified docstring point 3).
Exactness: the claim INSERT and the model SELECT run in one transaction, so
a bronze row committed between statements is simply left unclaimed for the
next build -- no committed-between-statements race, no permanent skip.
A rolled-back build rolls its claims back with it.

Grants mirror `meta.dedup_audit`'s own migration-0024 precedent exactly:
`dbt_app` gets SELECT + INSERT (its post-hook-transaction write path), never
UPDATE/DELETE; `etl_app`/`analytics_owner` get read-only visibility.
`dbt_app` already holds USAGE on schema `meta` (migration 0024).

Also adds the `_run_id` btree indexes on `staging.customers`/`staging.orders`
that migration 0022 never created (its FK to `meta.ingestion_runs` does not
auto-index in PostgreSQL): every `_run_id`-scoped read of bronze -- the new
claim NOT EXISTS, the models' eligibility semi-join, and the pre-existing
`_TOUCHED_KEYS_SQL`/`_VANISHED_SQL` `_run_id = ANY(...)` publisher queries --
was a sequential scan over a bronze table that already carries 3M+ rows on a
single CI run.

Revision ID: 0040
Revises: 0039
Create Date: 2026-08-28
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0040"
down_revision = "0039"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create meta.dbt_processed_runs, grant the 0024-precedent surface, index bronze _run_id."""
    op.create_table(
        "dbt_processed_runs",
        sa.Column("dataset_name", sa.Text(), nullable=False),
        sa.Column("run_id", sa.BigInteger(), nullable=False),
        # txid_current() (not pg_current_xact_id()): returns BIGINT directly,
        # still first-class in PG 18, and the value is only ever compared
        # against txid_current() inside the SAME transaction that inserted it.
        sa.Column(
            "claimed_txid",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("txid_current()"),
        ),
        sa.Column(
            "claimed_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("dataset_name", "run_id", name="pk_dbt_processed_runs"),
        schema="meta",
    )
    op.execute("GRANT SELECT, INSERT ON meta.dbt_processed_runs TO dbt_app")
    op.execute("GRANT SELECT ON meta.dbt_processed_runs TO etl_app, analytics_owner")

    op.create_index(
        "ix_staging_customers_run_id", "customers", ["_run_id"], schema="staging"
    )
    op.create_index("ix_staging_orders_run_id", "orders", ["_run_id"], schema="staging")


def downgrade() -> None:
    """Drop the bronze _run_id indexes and the claim ledger (grants fall with the table)."""
    op.drop_index("ix_staging_orders_run_id", table_name="orders", schema="staging")
    op.drop_index("ix_staging_customers_run_id", table_name="customers", schema="staging")
    op.drop_table("dbt_processed_runs", schema="meta")
