"""meta.rejected_records gains a durable business_key column (VALID-08 gap closure).

D-23/D-24/D-25 (`08-CONTEXT.md`, "Gap closure: VALID-08 backfill resolution scoping")
govern this migration. `08-VERIFICATION.md` live-confirmed that
`resolve_rejected_records_for_batch`'s strict `batch_id` scoping can never resolve a
content-differing correction, because `discover_files`'s `batch_key` is a pure function
of `content_sha256` -- a corrected file's bytes differ, so it always discovers under a
NEW `batch_id`, one the original PENDING row never belonged to.

D-23 replaces that batch-lineage-based identity with the dataset's own business-key
identity (the same concept Phase 4's `ON CONFLICT`/`MERGE` publish path is already built
around): `business_key` records the dataset's configured business/unique key column
value (e.g. `customer_id`) for the rejected row, captured at `record_rejected_records`
insert time (plan 08-17 wires the per-rule extraction; this migration only adds the
column). Resolution then matches on `(dataset_id, business_key)` instead of `batch_id`
(plan 08-16's own `resolve_rejected_records_for_business_keys`), so a backfill run
completing resolves every PENDING reject sharing that business key regardless of which
batch originally rejected it or which batch the correction discovers under.

`business_key` is nullable: a row whose business-key column could not be reliably read
at rejection time (D-25 -- e.g. a structural/ragged-row rejection, where field positions
are unreliable) stores `business_key = NULL`. This is deliberate, not an oversight: a
`NULL` value is structurally never matched by PostgreSQL's `= ANY(%s)` array-membership
operator, which is exactly what makes D-25's "a NULL business_key row is never
auto-resolved" guarantee hold with no extra `WHERE business_key IS NOT NULL` clause
required anywhere in the resolution query.

No `GRANT` statement is needed here: migration 0015's table-level
`GRANT SELECT, INSERT, UPDATE ON meta.rejected_records TO etl_app` already covers this
new column -- PostgreSQL's `GRANT ... ON TABLE` applies to every column, including ones
added later by `ALTER TABLE ... ADD COLUMN`. Confirmed: no per-column `GRANT` exists
anywhere in this codebase's 19 prior migrations.

Revision ID: 0020
Revises: 0019
Create Date: 2026-08-17
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0020"
down_revision = "0019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add `meta.rejected_records.business_key` + its supporting resolution index."""
    op.add_column(
        "rejected_records",
        sa.Column("business_key", sa.Text(), nullable=True),
        schema="meta",
    )
    op.create_index(
        "ix_rejected_records_business_key_resolution",
        "rejected_records",
        ["business_key", "resolution_type"],
        schema="meta",
    )


def downgrade() -> None:
    """Drop the business_key resolution index, then the column."""
    op.drop_index(
        "ix_rejected_records_business_key_resolution",
        table_name="rejected_records",
        schema="meta",
    )
    op.drop_column("rejected_records", "business_key", schema="meta")
