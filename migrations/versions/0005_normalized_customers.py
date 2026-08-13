"""normalized.customers — the vertical-slice target table.

Creates schema `normalized` (the one place this migration set does so) and
the five business columns CONTEXT.md D-02 names (`customer_id`, `name`,
`country`, `birth_date`, `event_ts` — the shape ARCHITECTURE.md's worked
`MERGE` example and config-not-code sample both use), plus the six embedded
lineage columns every `normalized.*`/`warehouse.*` table carries verbatim
from ARCHITECTURE.md §2.3 lines 255-261.

`_record_hash_version` extends D-05's literal text (which names only
`files.content_sha256` and `config_versions.config_hash`) to `_record_hash`:
it is unambiguously a stored hash in PITFALLS.md #1/C6's general sense, and
this is the only phase that mints it — flagged explicitly in 03-RESEARCH.md
Pitfall 3 / 03-PATTERNS.md Cluster K as a genuine (confirmed) extension of a
locked decision, not a silent one.

`customer_id` carries a plain, non-unique index only: uniqueness is enforced
by the later MERGE publication logic (Phase 4), not by this DDL — a target
row's uniqueness constraint here would fight, not support, `MERGE ... WHEN
MATCHED`.

Revision ID: 0005
Revises: 0004
Create Date: 2026-08-13
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create schema `normalized`, then `normalized.customers`."""
    op.execute("CREATE SCHEMA IF NOT EXISTS normalized")

    op.create_table(
        "customers",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        # Business columns (D-02).
        sa.Column("customer_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.Text(), nullable=True),
        sa.Column("country", sa.Text(), nullable=True),
        sa.Column("birth_date", sa.Date(), nullable=True),
        sa.Column("event_ts", sa.DateTime(timezone=True), nullable=True),
        # Embedded lineage columns, verbatim from ARCHITECTURE.md §2.3.
        sa.Column(
            "_run_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.ingestion_runs.run_id"),
            nullable=False,
        ),
        sa.Column(
            "_file_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.files.file_id"),
            nullable=False,
        ),
        sa.Column(
            "_batch_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.batches.batch_id"),
            nullable=False,
        ),
        sa.Column("_source_row_number", sa.BigInteger(), nullable=False),
        sa.Column("_record_hash", sa.LargeBinary(), nullable=False),
        # META-02 extension (see module docstring): _record_hash is the only
        # hash this phase mints new values for, so it gets the same
        # companion-version treatment as content_sha256/config_hash.
        sa.Column(
            "_record_hash_version",
            sa.SmallInteger(),
            nullable=False,
            server_default=sa.text("1"),
        ),
        sa.Column(
            "_ingested_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        schema="normalized",
    )
    op.create_index(
        "ix_customers_customer_id",
        "customers",
        ["customer_id"],
        unique=False,
        schema="normalized",
    )
    op.execute("GRANT SELECT, INSERT, UPDATE ON normalized.customers TO etl_app")


def downgrade() -> None:
    """Drop `normalized.customers`. Never drops the schema."""
    op.drop_table("customers", schema="normalized")
