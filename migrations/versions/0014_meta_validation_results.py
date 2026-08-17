"""meta.validation_results — persisted validation-rule findings (VALID-01/02/03/04/09).

Every rule evaluation this phase's validation engine runs (a later plan's
job) writes one row here per rule per run — structural, schema, type,
quality, referential, volume, or file-level. `rule_type`/`outcome` are
app-validated against a fixed vocabulary (`{FILE,STRUCTURAL,SCHEMA,TYPE,
QUALITY,REFERENTIAL,VOLUME}` / `{PASS,PASS_WITH_WARNING,FAIL,QUARANTINE}`),
never a native Postgres ENUM — matches this project's "config not code,
strings not enums" convention (`migrations/versions/0009_meta_schema_versions.py`
module docstring; `config/model.py`'s own documented rationale).

Grants are `SELECT, INSERT, UPDATE` only — deliberately no `DELETE`. This is
D-04's no-per-row-edit constraint enforced at the DB-privilege level: even a
future application bug cannot delete a validation finding, only ever insert
new ones or update run-scoped aggregate fields.

Revision ID: 0014
Revises: 0013
Create Date: 2026-08-17
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision = "0014"
down_revision = "0013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create `meta.validation_results`."""
    op.create_table(
        "validation_results",
        sa.Column("validation_result_id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column(
            "run_id",
            sa.BigInteger(),
            sa.ForeignKey("meta.ingestion_runs.run_id"),
            nullable=False,
        ),
        sa.Column("rule_id", sa.Text(), nullable=False),
        # App-validated {FILE,STRUCTURAL,SCHEMA,TYPE,QUALITY,REFERENTIAL,VOLUME} -- never a native enum.
        sa.Column("rule_type", sa.Text(), nullable=False),
        sa.Column("severity", sa.Text(), nullable=False),
        # App-validated {PASS,PASS_WITH_WARNING,FAIL,QUARANTINE} -- never a native enum.
        sa.Column("outcome", sa.Text(), nullable=False),
        sa.Column("evaluated_count", sa.BigInteger(), nullable=False),
        sa.Column("failed_count", sa.BigInteger(), nullable=False),
        sa.Column("threshold", JSONB(), nullable=True),
        sa.Column("observed", JSONB(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        schema="meta",
    )
    op.create_index(
        "ix_validation_results_run_id",
        "validation_results",
        ["run_id"],
        schema="meta",
    )
    # D-04: no DELETE, ever -- enforced at the database-privilege level, not
    # merely by application logic.
    op.execute("GRANT SELECT, INSERT, UPDATE ON meta.validation_results TO etl_app")


def downgrade() -> None:
    """Drop `meta.validation_results`."""
    op.drop_table("validation_results", schema="meta")
