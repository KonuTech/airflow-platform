"""normalized.customers.customer_id — from a plain index to a real UNIQUE constraint.

Migration 0005's docstring reasoned that a uniqueness constraint on
`customer_id` would fight `MERGE ... WHEN MATCHED` and so created a plain,
non-unique index instead. LOAD-09 / PITFALLS.md #14 reject literal SQL
`MERGE` for this table in favor of `pg_advisory_xact_lock` +
`INSERT ... ON CONFLICT (customer_id)` (ARCHITECTURE.md's worked publication
example) — `ON CONFLICT` requires a real `UNIQUE`/exclusion constraint as its
conflict target, which a plain index can never satisfy. This migration is
that precondition: 0005's stated rationale ("a uniqueness constraint would
fight MERGE") no longer applies once the chosen publication strategy is
`ON CONFLICT`, not `MERGE`.

A `UNIQUE` constraint creates its own backing B-tree index, identical in
shape to the one it replaces, so no index coverage is lost by this change —
only the constraint semantics are added.

No `GRANT` statement is needed here: altering an existing table's indexing
does not change table-level grants (`normalized.customers` already carries
`SELECT, INSERT, UPDATE` for `etl_app` from migration 0005) — this is a
deliberate omission, not an oversight.

Revision ID: 0006
Revises: 0005
Create Date: 2026-08-13
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Replace the plain `customer_id` index with a real UNIQUE constraint."""
    op.drop_index("ix_customers_customer_id", table_name="customers", schema="normalized")
    op.create_unique_constraint(
        "uq_customers_customer_id",
        "customers",
        ["customer_id"],
        schema="normalized",
    )


def downgrade() -> None:
    """Reverse: drop the UNIQUE constraint, restore the plain non-unique index."""
    op.drop_constraint(
        "uq_customers_customer_id",
        "customers",
        schema="normalized",
        type_="unique",
    )
    op.create_index(
        "ix_customers_customer_id",
        "customers",
        ["customer_id"],
        unique=False,
        schema="normalized",
    )
