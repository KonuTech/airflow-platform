"""meta.schema_versions — schema versioning, hashing and evolution history (SCHEMA-03/04/05).

Column shapes are ARCHITECTURE.md §2.1 line 232, verbatim, and the table's
overall shape mirrors `meta.config_versions` (migration 0001) exactly:
`UNIQUE(dataset_id, version)` plus a partial unique index enforcing at most
one CURRENT (`valid_to IS NULL`) row per dataset. `derived_from` and
`compatibility` are `sa.Text()`, app-validated, never a native Postgres
`ENUM` — no CHECK constraint or native enum type exists anywhere in this
project's migrations; every enum-like column is validated at the
Pydantic/application layer (matches `config/model.py`'s own documented
"config not code, strings not enums" convention).

`hash_version` is META-02's companion column, present on every stored hash
this project's migrations mint (D-05; PITFALLS.md #1/C6).

This migration also closes migration 0004's deliberately-deferred foreign
key: `meta.ingestion_runs.schema_version_id` landed nullable and
unconstrained because `meta.schema_versions` (its referent) did not exist
yet (CONTEXT.md D-05, ARCHITECTURE.md §2.4). 0004's own comment names this
exact moment as the closing action.

Revision ID: 0009
Revises: 0008
Create Date: 2026-08-15
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create `meta.schema_versions`, then close 0004's deferred FK."""
    op.create_table(
        "schema_versions",
        sa.Column("schema_version_id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column(
            "dataset_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.datasets.dataset_id"),
            nullable=False,
        ),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("schema_hash", sa.Text(), nullable=False),
        # META-02: hash_version accompanies every stored hash from the first
        # migration that mints one (D-05 names this pair explicitly).
        sa.Column("hash_version", sa.SmallInteger(), nullable=False, server_default=sa.text("1")),
        # Ordered: name/type/nullable/position/format entries.
        sa.Column("columns", JSONB(), nullable=False),
        # App-validated "CONTRACT" | "INFERRED" -- never a native enum.
        sa.Column("derived_from", sa.Text(), nullable=False),
        # App-validated "COMPATIBLE" | "BREAKING" -- never a native enum.
        sa.Column("compatibility", sa.Text(), nullable=False),
        sa.Column("breaking_changes", JSONB(), nullable=True),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_to", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("dataset_id", "version", name="uq_schema_versions_dataset_version"),
        schema="meta",
    )
    # Partial unique index: at most one CURRENT (valid_to IS NULL) schema
    # version per dataset -- the identical pattern meta.config_versions
    # already uses (migration 0001).
    op.create_index(
        "uq_schema_versions_current_per_dataset",
        "schema_versions",
        ["dataset_id"],
        unique=True,
        schema="meta",
        postgresql_where=sa.text("valid_to IS NULL"),
    )
    op.execute("GRANT SELECT, INSERT, UPDATE ON meta.schema_versions TO etl_app")

    # Closes migration 0004's deliberately-deferred FK (its own comment names
    # this exact moment: "a later migration adds the constraint via
    # op.create_foreign_key once that table exists").
    op.create_foreign_key(
        "fk_ingestion_runs_schema_version_id",
        "ingestion_runs",
        "schema_versions",
        ["schema_version_id"],
        ["schema_version_id"],
        source_schema="meta",
        referent_schema="meta",
    )


def downgrade() -> None:
    """Drop the FK first, then `meta.schema_versions` (reverse of `upgrade()`)."""
    op.drop_constraint(
        "fk_ingestion_runs_schema_version_id",
        "ingestion_runs",
        schema="meta",
        type_="foreignkey",
    )
    op.drop_table("schema_versions", schema="meta")
