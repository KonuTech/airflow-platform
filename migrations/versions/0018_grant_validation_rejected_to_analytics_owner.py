"""Grant SELECT on meta.validation_results/meta.rejected_records to analytics_owner.

Migrations 0014/0015 granted SELECT/INSERT/UPDATE on these two tables to
their intended production writer, `etl_app`, deliberately never a broader
grant. Neither anticipated `analytics_owner` (the CNPG-generated
bootstrap/admin role) ever needing direct read access to them -- but
`tests/e2e/slice/conftest.py`'s `analytics_owner_connection` fixture is
intentionally `analytics_owner`-authenticated (same reasoning as migration
0013's own precedent for `meta.v_customers_lineage`), and the live
VALID-07/VALID-08 closing proofs added in plan 08-14
(`test_referential_orphan.py`, `test_backfill_reentry.py`) need that same
connection to read `meta.rejected_records` directly -- confirmed live on
the deployed cluster: `psycopg.errors.InsufficientPrivilege: permission
denied for table rejected_records`. `analytics_owner` already has
unrestricted DDL/DML on this database, so granting it SELECT on these two
tables is closing an oversight in scope, not widening their security
boundary -- `etl_app` remains the only write-capable consumer.

Revision ID: 0018
Revises: 0017
Create Date: 2026-08-17
"""

from __future__ import annotations

from alembic import op

revision = "0018"
down_revision = "0017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Grant SELECT on validation_results/rejected_records to analytics_owner."""
    op.execute("GRANT SELECT ON meta.validation_results TO analytics_owner")
    op.execute("GRANT SELECT ON meta.rejected_records TO analytics_owner")


def downgrade() -> None:
    """Revoke SELECT on validation_results/rejected_records from analytics_owner."""
    op.execute("REVOKE SELECT ON meta.rejected_records FROM analytics_owner")
    op.execute("REVOKE SELECT ON meta.validation_results FROM analytics_owner")
