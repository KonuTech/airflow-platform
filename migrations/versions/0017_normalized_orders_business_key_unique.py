"""normalized.orders.order_id — add the UNIQUE constraint `ON CONFLICT (order_id)` requires.

Migration 0016 declared `order_id` as a plain, unconstrained column (no
index, no uniqueness) -- its own docstring only reasoned about
`customer_id` ("orders has no analogous ON CONFLICT publish target needing a
real unique constraint" refers to `customer_id`, which correctly stays
non-unique since many orders share one customer). `order_id` is different:
it is D-17's declared business key (`orders.yaml`'s own
`columns: business_key: true` entry) and this phase's `OrdersMergePublisher`
(08-05) publishes via `pg_advisory_xact_lock` +
`INSERT ... ON CONFLICT (order_id)` -- the exact same `MergePublisher`
precedent migration 0006 fixed for `normalized.customers.customer_id`.
`ON CONFLICT` requires a real `UNIQUE`/exclusion constraint as its conflict
target, which no index (plain or absent) can ever satisfy -- this migration
is that precondition, closing the gap 0016 left open.

No `GRANT` statement is needed here, matching migration 0006's own
reasoning: altering an existing table's indexing does not change
table-level grants (`normalized.orders` already carries
`SELECT, INSERT, UPDATE` for `etl_app` from migration 0016).

Revision ID: 0017
Revises: 0016
Create Date: 2026-08-17
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "0017"
down_revision = "0016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add a real UNIQUE constraint on `order_id`, `ON CONFLICT`'s required conflict target."""
    op.create_unique_constraint(
        "uq_orders_order_id",
        "orders",
        ["order_id"],
        schema="normalized",
    )


def downgrade() -> None:
    """Reverse: drop the UNIQUE constraint."""
    op.drop_constraint(
        "uq_orders_order_id",
        "orders",
        schema="normalized",
        type_="unique",
    )
