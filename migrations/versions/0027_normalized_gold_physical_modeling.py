"""normalized.orders / normalized.customers — gold physical modeling via indexes, not partitioning (D-13).

D-13 asked for "partitioning and indexing" on gold. This migration delivers
indexing only, and the reasoning belongs here — the migration every future
reader will ask "why isn't this partitioned" about — not only in the plan
file that authorized it.

PostgreSQL's own documented rule: a partitioned table's unique constraint
(and its implicit backing index) must include every partition key column,
because the partition key is part of what makes a row's identity unique
*across the whole partitioned table*, not just within one partition.
`MergePublisher`/`OrdersMergePublisher` — which D-13 explicitly requires to
stay unchanged — publish via `pg_advisory_xact_lock` +
`INSERT ... ON CONFLICT (customer_id)` / `ON CONFLICT (order_id)`
(migrations 0006/0017), each depending on a `UNIQUE` constraint on exactly
that ONE column. Range-partitioning `normalized.orders` by `order_date` (the
only genuinely time-range-shaped column available) would force the unique
constraint to become `(order_id, order_date)` — which would silently let a
correction changing an existing order's `order_date` insert a SECOND row
instead of updating the first: a real, first-class duplicate-business-key
bug, the exact failure class this platform's whole architecture exists to
prevent (the same class of discovery that ruled out dbt's `merge` strategy
for silver, ADR-0010 — a real Postgres mechanic conflicting with an existing
correctness guarantee). This choice was escalated to and confirmed by the
user during planning (08.1-CONTEXT.md D-13's "Resolved during planning"
note), not a unilateral planner decision.

The response, consistent with that precedent: choose the mechanism that
preserves correctness (indexes) over the one that would silently violate it
(partitioning).

This migration adds four NEW indexes, and the physical-modeling picture it
completes covers five named access patterns total:

- `ix_orders_order_date` (time-range queries) -- NEW
- `ix_orders_customer_id` (ad-hoc joins back to customers -- also the exact
  column `ReferentialIntegrityBarrier` already anti-joins on) --
  **deliberately NOT created here**: migration 0016 already created this
  exact index (`normalized.orders`'s own original DDL, for the referential
  barrier's anti-join performance). Re-issuing an identical
  `op.create_index("ix_orders_customer_id", ...)` here would raise
  `DuplicateTable` ("relation ... already exists") and abort the whole
  migration chain -- found while executing this plan (Rule 1 auto-fix,
  documented in `08.1-09-SUMMARY.md`). The coverage this index provides is
  already real and already live; this migration does not need to duplicate
  it to complete the physical-modeling picture.
- `ix_orders_order_date_customer_id` (the combined BI-dashboard pattern:
  "orders for customer X in date range Y") -- NEW
- `ix_customers_event_ts` (time-range/freshness queries) -- NEW
- `ix_customers_country` (the one real categorical dimension on this table,
  a natural ad-hoc/exploratory BI slicing column) -- NEW

`uq_customers_customer_id` (migration 0006) and `uq_orders_order_id`
(migration 0017) are never touched: zero DDL on either, zero data
migration, zero risk to `MergePublisher`/`OrdersMergePublisher`'s
`ON CONFLICT` targets.

Revision ID: 0027
Revises: 0026
Create Date: 2026-08-18
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "0027"
down_revision = "0026"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add four NEW gold indexes. No PARTITION BY, no renames, no constraint DDL.

    `ix_orders_customer_id` is deliberately NOT created here -- migration
    0016 already created it. See module docstring.
    """
    op.create_index(
        "ix_orders_order_date",
        "orders",
        ["order_date"],
        schema="normalized",
    )
    op.create_index(
        "ix_orders_order_date_customer_id",
        "orders",
        ["order_date", "customer_id"],
        schema="normalized",
    )
    op.create_index(
        "ix_customers_event_ts",
        "customers",
        ["event_ts"],
        schema="normalized",
    )
    op.create_index(
        "ix_customers_country",
        "customers",
        ["country"],
        schema="normalized",
    )


def downgrade() -> None:
    """Drop the four indexes this migration created, reverse order.

    `ix_orders_customer_id` is never touched -- it belongs to migration
    0016, not this one.
    """
    op.drop_index("ix_customers_country", table_name="customers", schema="normalized")
    op.drop_index("ix_customers_event_ts", table_name="customers", schema="normalized")
    op.drop_index(
        "ix_orders_order_date_customer_id",
        table_name="orders",
        schema="normalized",
    )
    op.drop_index("ix_orders_order_date", table_name="orders", schema="normalized")
