"""dbt_app -- narrow dataset_name lookup function (closes 08.1-08's documented gap).

`dbt/macros/dedup_audit_post_hook.sql` resolves `dataset_name` (e.g.
'customers') to its surrogate `dataset_id` at hook-EXECUTION time, inside
each silver model's post-hook. Migration 0024 granted `dbt_app` `USAGE` on
schema `meta` and `SELECT, INSERT` on `meta.dedup_audit`/
`meta.dedup_decisions` specifically -- but that macro's own docstring
documents a known gap: no grant on `meta.datasets` itself, so a live,
Vault-authenticated `dbt_app` build fails the lookup with a permission
error. Plan 08.1-08's own integration tests never exercised this because
they run `dbt build` against the testcontainers superuser DSN, not
`dbt_app`.

The naive fix -- `GRANT SELECT ON meta.datasets TO dbt_app` -- was tried
first and rejected: it fails `test_dbt_app_role_is_scoped_correctly`
(migration 0021/plan 08.1-01), which encodes D-08's explicit boundary
("`dbt_app` has zero grant on `normalized.*` or `meta.*` except what a
later plan narrowly adds for `meta.dedup_audit`" -- 08.1-01-PLAN.md). A
table-level `SELECT` grant would let `dbt_app` read every column of
`meta.datasets` (source_system, description, freshness config, ...), not
just the one dataset_name->dataset_id mapping it actually needs -- broader
access than D-08 intends and broader than the lookup requires.

Instead: a single-purpose `SECURITY DEFINER` function, owned by the
migration-running role (the same role that already owns `meta.datasets`),
narrows the interface to exactly `dataset_name -> dataset_id`. `dbt_app`
gets `EXECUTE` on the function, never `SELECT` on the table -- this does
not appear in `information_schema.role_table_grants` at all, so it does
not regress the existing D-08 boundary test, and it is a strictly narrower
capability than the rejected table grant.
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "0028"
down_revision = "0027"
branch_labels = None
depends_on = None

_FUNCTION_SQL = """
CREATE FUNCTION meta.dataset_id_for_name(p_dataset_name text)
RETURNS bigint
LANGUAGE sql
SECURITY DEFINER
SET search_path = meta, pg_temp
STABLE
AS $$
    SELECT dataset_id FROM meta.datasets WHERE dataset_name = p_dataset_name;
$$;
"""


def upgrade() -> None:
    """Create meta.dataset_id_for_name(text) and grant dbt_app EXECUTE on it only."""
    op.execute(_FUNCTION_SQL)
    op.execute(
        "REVOKE ALL ON FUNCTION meta.dataset_id_for_name(text) FROM PUBLIC",
    )
    op.execute(
        "GRANT EXECUTE ON FUNCTION meta.dataset_id_for_name(text) TO dbt_app",
    )


def downgrade() -> None:
    """Drop meta.dataset_id_for_name(text) (grant goes with it)."""
    op.execute("DROP FUNCTION meta.dataset_id_for_name(text)")
