"""meta.datasets and meta.config_versions — the dataset registry and config history.

Creates schema `meta` (if it does not already exist — no other migration in
this phase does, so this is the one place `CREATE SCHEMA meta` runs) plus the
two tables every other slice table's `dataset_id`/`config_version_id` foreign
keys point at. Column shapes are ARCHITECTURE.md §2.1 lines 141-163,
verbatim.

`config_versions.hash_version` is META-02's first instance: every stored hash
column in this phase's migrations carries a companion `smallint NOT NULL
DEFAULT 1` version column (D-05; PITFALLS.md #1/C6), so a future change to
the hashing recipe can be detected and migrated per-row instead of silently
producing hashes nothing can distinguish from the old ones.

Revision ID: 0001
Revises:
Create Date: 2026-08-13
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create schema `meta`, then `meta.datasets` and `meta.config_versions`."""
    op.execute("CREATE SCHEMA IF NOT EXISTS meta")

    op.create_table(
        "datasets",
        sa.Column("dataset_id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column("dataset_name", sa.Text(), nullable=False, unique=True),
        sa.Column("source_system", sa.Text(), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        schema="meta",
    )
    op.execute("GRANT SELECT, INSERT, UPDATE ON meta.datasets TO etl_app")

    op.create_table(
        "config_versions",
        sa.Column("config_version_id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column(
            "dataset_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.datasets.dataset_id"),
            nullable=False,
        ),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("config_hash", sa.Text(), nullable=False),
        # META-02: hash_version accompanies every stored hash from the first
        # migration that mints one (D-05 names this pair explicitly).
        sa.Column("hash_version", sa.SmallInteger(), nullable=False, server_default=sa.text("1")),
        sa.Column("config_document", JSONB(), nullable=False),
        sa.Column("config_schema_version", sa.Integer(), nullable=False),
        sa.Column("git_commit_sha", sa.Text(), nullable=True),
        sa.Column("git_path", sa.Text(), nullable=True),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_to", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("dataset_id", "version", name="uq_config_versions_dataset_version"),
        sa.UniqueConstraint("dataset_id", "config_hash", name="uq_config_versions_dataset_hash"),
        schema="meta",
    )
    # Partial unique index: at most one CURRENT (valid_to IS NULL) config
    # version per dataset. Not expressible as a plain UniqueConstraint.
    op.create_index(
        "uq_config_versions_current_per_dataset",
        "config_versions",
        ["dataset_id"],
        unique=True,
        schema="meta",
        postgresql_where=sa.text("valid_to IS NULL"),
    )
    op.execute("GRANT SELECT, INSERT, UPDATE ON meta.config_versions TO etl_app")


def downgrade() -> None:
    """Drop `meta.config_versions` then `meta.datasets`. Never drops the schema."""
    op.drop_table("config_versions", schema="meta")
    op.drop_table("datasets", schema="meta")
