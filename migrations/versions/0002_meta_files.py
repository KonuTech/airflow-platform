"""meta.files — the arrival registry, identity split between path and content.

Column shapes are ARCHITECTURE.md §2.1 lines 165-182. Identity is
deliberately split: `object_uri` identifies an *arrival*, `content_sha256`
identifies *content* — the same bytes re-uploaded to a new path is a new
arrival of a known file, a distinct situation from both a genuinely new file
and an intentional backfill (§25). `content_sha256` carries a companion
`hash_version` (META-02), matching `config_versions.hash_version` from 0001.

Revision ID: 0002
Revises: 0001
Create Date: 2026-08-13
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create `meta.files`."""
    op.create_table(
        "files",
        sa.Column("file_id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column(
            "dataset_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.datasets.dataset_id"),
            nullable=False,
        ),
        sa.Column("object_uri", sa.Text(), nullable=False),
        sa.Column("object_etag", sa.Text(), nullable=True),
        sa.Column("content_sha256", sa.LargeBinary(), nullable=False),
        # META-02: companion version for content_sha256, matching 0001's
        # config_versions.hash_version pairing (D-05).
        sa.Column("hash_version", sa.SmallInteger(), nullable=False, server_default=sa.text("1")),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("filename", sa.Text(), nullable=False),
        sa.Column("filename_facets", JSONB(), nullable=True),
        # §8: never auto-assumed from the filename; populated only when a
        # dataset's contract says so.
        sa.Column("business_date", sa.Date(), nullable=True),
        sa.Column("discovered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_last_modified_at", sa.DateTime(timezone=True), nullable=True),
        # Self-FK: set when content_sha256 has already been seen for this
        # dataset. Nullable — most files are not duplicates.
        sa.Column(
            "duplicate_of_file_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.files.file_id"),
            nullable=True,
        ),
        sa.Column("status", sa.Text(), nullable=False),
        sa.UniqueConstraint(
            "dataset_id",
            "object_uri",
            "content_sha256",
            name="uq_files_dataset_uri_content",
        ),
        schema="meta",
    )
    op.create_index(
        "ix_files_dataset_content_sha256",
        "files",
        ["dataset_id", "content_sha256"],
        schema="meta",
    )
    op.execute("GRANT SELECT, INSERT, UPDATE ON meta.files TO etl_app")


def downgrade() -> None:
    """Drop `meta.files`."""
    op.drop_table("files", schema="meta")
