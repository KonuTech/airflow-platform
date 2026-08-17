"""Grant SELECT on normalized.customers/normalized.orders to analytics_owner.

Same class of oversight migration 0013 fixed for `meta.v_customers_lineage`
and migration 0018 fixed for `meta.validation_results`/
`meta.rejected_records`: migrations 0005/0016 granted SELECT/INSERT/UPDATE
on these two tables to `etl_app` only, never anticipating `analytics_owner`
needing direct read access. Confirmed live on the deployed cluster running
plan 08-14's E2E slice tests: `test_referential_orphan.py`'s
`analytics_owner_connection` fixture needs to read `normalized.orders`
directly to verify a published (non-orphan) row, and hit
`psycopg.errors.InsufficientPrivilege: permission denied for table orders`.
Granting SELECT on both tables now (`customers` was never exercised this
way before this phase's live tests, but carries the identical gap) closes
the oversight in scope, not the security boundary -- `etl_app` remains the
only write-capable consumer of either table.

Revision ID: 0019
Revises: 0018
Create Date: 2026-08-17
"""

from __future__ import annotations

from alembic import op

revision = "0019"
down_revision = "0018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Grant SELECT on normalized.customers/normalized.orders to analytics_owner."""
    op.execute("GRANT SELECT ON normalized.customers TO analytics_owner")
    op.execute("GRANT SELECT ON normalized.orders TO analytics_owner")


def downgrade() -> None:
    """Revoke SELECT on normalized.customers/normalized.orders from analytics_owner."""
    op.execute("REVOKE SELECT ON normalized.orders FROM analytics_owner")
    op.execute("REVOKE SELECT ON normalized.customers FROM analytics_owner")
