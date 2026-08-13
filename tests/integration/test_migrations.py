"""META-01/META-02 proof: `alembic upgrade head` against a throwaway PostgreSQL 18.

Five properties, each a distinct way the migrations could be wrong even if
`alembic upgrade head` itself exits 0: the wrong table set, a non-idempotent
upgrade, a missing/mistyped `hash_version` companion column, a grant wider
than `SELECT, INSERT, UPDATE`, or an accidental foreign key on
`ingestion_runs.schema_version_id` before `meta.schema_versions` exists.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

import psycopg
from alembic import command
from alembic.config import Config

if TYPE_CHECKING:
    from collections.abc import Callable

REPO_ROOT = Path(__file__).resolve().parents[2]
ALEMBIC_INI = REPO_ROOT / "migrations" / "alembic.ini"

# The complete slice: five slice tables, meta.batch_files, normalized.customers
# (D-05). alembic_version is Alembic's own bookkeeping table, deliberately
# excluded — it lives in `meta` too (version_table_schema="meta") but is not
# one of this phase's business tables.
EXPECTED_TABLES = {
    ("meta", "datasets"),
    ("meta", "config_versions"),
    ("meta", "files"),
    ("meta", "batches"),
    ("meta", "batch_files"),
    ("meta", "ingestion_runs"),
    ("normalized", "customers"),
}

# Every table this phase's migrations GRANT etl_app access to — the same set
# as EXPECTED_TABLES, named separately so a future table added to one without
# the other is a visible diff, not a coincidence of reuse.
GRANTED_TABLES = sorted(EXPECTED_TABLES)

HASH_VERSION_COLUMNS = [
    ("meta", "files", "hash_version"),
    ("meta", "config_versions", "hash_version"),
    ("normalized", "customers", "_record_hash_version"),
]


def _table_set(dsn: str) -> set[tuple[str, str]]:
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            """
            SELECT table_schema, table_name
              FROM information_schema.tables
             WHERE table_schema IN ('meta', 'normalized')
               AND table_type = 'BASE TABLE'
               AND table_name != 'alembic_version'
            """,
        ).fetchall()
    return {(schema, name) for schema, name in rows}


def test_upgrade_head_creates_the_slice_schema(migrated_dsn: str) -> None:
    assert _table_set(migrated_dsn) == EXPECTED_TABLES


def test_upgrade_head_is_idempotent(
    run_migrations: Callable[[str], None],
    migrated_dsn: str,
) -> None:
    before = _table_set(migrated_dsn)
    run_migrations(migrated_dsn)  # second call against the same DB — must not error
    after = _table_set(migrated_dsn)
    assert after == before


def test_hash_version_columns(migrated_dsn: str) -> None:
    with psycopg.connect(migrated_dsn) as conn:
        for schema, table, column in HASH_VERSION_COLUMNS:
            row = conn.execute(
                """
                SELECT data_type, is_nullable, column_default
                  FROM information_schema.columns
                 WHERE table_schema = %s AND table_name = %s AND column_name = %s
                """,
                (schema, table, column),
            ).fetchone()
            assert row is not None, f"{schema}.{table}.{column} does not exist"
            data_type, is_nullable, column_default = row
            assert data_type == "smallint", (
                f"{schema}.{table}.{column} is {data_type!r}, expected smallint"
            )
            assert is_nullable == "NO", f"{schema}.{table}.{column} is nullable"
            assert column_default is not None, f"{schema}.{table}.{column} has no default"
            assert column_default.startswith("1"), (
                f"{schema}.{table}.{column} default is {column_default!r}, expected 1"
            )


def test_etl_app_grants(migrated_dsn: str) -> None:
    with psycopg.connect(migrated_dsn) as conn:
        for schema, table in GRANTED_TABLES:
            rows = conn.execute(
                """
                SELECT privilege_type
                  FROM information_schema.role_table_grants
                 WHERE grantee = 'etl_app' AND table_schema = %s AND table_name = %s
                """,
                (schema, table),
            ).fetchall()
            privileges = {row[0] for row in rows}
            assert privileges == {"SELECT", "INSERT", "UPDATE"}, (
                f"{schema}.{table}: expected exactly SELECT/INSERT/UPDATE, got {privileges}"
            )


def test_etl_app_can_actually_use_the_schemas_it_has_table_grants_in(migrated_dsn: str) -> None:
    """`role_table_grants` rows are inert without schema-level `USAGE` (0008's own bug).

    `test_etl_app_grants` above proves the table-level grant *rows* exist,
    but PostgreSQL gates all table access behind `USAGE` on the containing
    schema first — a role can hold `SELECT, INSERT, UPDATE` on a table and
    still get `permission denied for schema ...` on every single statement
    if `USAGE` was never granted. That is exactly what migrations 0001/0005
    shipped for three phases before 0008 fixed it: `has_schema_privilege`
    is the same check PostgreSQL itself runs, so this is the one query that
    actually proves the schema is usable, not merely that a grant row exists.
    """
    with psycopg.connect(migrated_dsn) as conn:
        for schema in ("meta", "normalized", "staging"):
            usable = conn.execute(
                "SELECT has_schema_privilege('etl_app', %s, 'USAGE')",
                (schema,),
            ).fetchone()
            assert usable is not None
            assert usable[0] is True, f"etl_app lacks USAGE on schema {schema!r}"


def test_ingestion_runs_schema_version_id_has_no_fk(migrated_dsn: str) -> None:
    with psycopg.connect(migrated_dsn) as conn:
        rows = conn.execute(
            """
            SELECT tc.constraint_name
              FROM information_schema.table_constraints tc
              JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
               AND tc.table_schema = kcu.table_schema
             WHERE tc.constraint_type = 'FOREIGN KEY'
               AND tc.table_schema = 'meta'
               AND tc.table_name = 'ingestion_runs'
               AND kcu.column_name = 'schema_version_id'
            """,
        ).fetchall()
    assert rows == [], f"schema_version_id must carry no FK constraint, found: {rows}"


def _customers_customer_id_constraint_types(dsn: str) -> tuple[str, ...]:
    """Return every `table_constraints.constraint_type` covering `customer_id` alone.

    A plain index is not a constraint at all, so this returns an empty tuple
    for it -- distinct from a real `UNIQUE` constraint, which always shows up
    here (`information_schema.table_constraints` joined through
    `key_column_usage`).
    """
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            """
            SELECT tc.constraint_type
              FROM information_schema.table_constraints tc
              JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
               AND tc.table_schema = kcu.table_schema
             WHERE tc.table_schema = 'normalized'
               AND tc.table_name = 'customers'
               AND kcu.column_name = 'customer_id'
            """,
        ).fetchall()
    return tuple(row[0] for row in rows)


def _index_exists(dsn: str, *, schema: str, index_name: str) -> bool:
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            "SELECT 1 FROM pg_indexes WHERE schemaname = %s AND indexname = %s",
            (schema, index_name),
        ).fetchone()
    return row is not None


def test_0006_customer_id_has_a_real_unique_constraint(migrated_dsn: str) -> None:
    """LOAD-09's `ON CONFLICT (customer_id)` needs a real conflict target, not merely an index."""
    assert _customers_customer_id_constraint_types(migrated_dsn) == ("UNIQUE",)
    assert not _index_exists(
        migrated_dsn,
        schema="normalized",
        index_name="ix_customers_customer_id",
    )


def test_0006_downgrade_restores_the_plain_index_and_reupgrade_restores_the_constraint(
    migrated_dsn: str,
) -> None:
    """`alembic downgrade 0005` cleanly reverses 0006; re-`upgrade head` restores it.

    Targets the explicit revision `"0005"` rather than the relative `"-1"`:
    migration `0007` (plan 04-04) added a new head above `0006`, so `"-1"`
    from head would now reverse `0007` instead of `0006` -- an explicit
    target is what this test actually means ("undo exactly 0006's change"),
    and stays correct regardless of how many further migrations are added
    later.

    `migrated_dsn` is session-scoped and shared by every other module in
    `tests/integration/`, so this test restores it to `head` in a `finally`
    block regardless of which assertion (if any) fails.
    """
    alembic_config = Config(str(ALEMBIC_INI))
    previous = os.environ.get("ALEMBIC_DSN")
    os.environ["ALEMBIC_DSN"] = migrated_dsn
    try:
        command.downgrade(alembic_config, "0005")
        assert _customers_customer_id_constraint_types(migrated_dsn) == ()
        assert _index_exists(
            migrated_dsn,
            schema="normalized",
            index_name="ix_customers_customer_id",
        )
    finally:
        command.upgrade(alembic_config, "head")
        if previous is None:
            os.environ.pop("ALEMBIC_DSN", None)
        else:
            os.environ["ALEMBIC_DSN"] = previous

    assert _customers_customer_id_constraint_types(migrated_dsn) == ("UNIQUE",)
    assert not _index_exists(
        migrated_dsn,
        schema="normalized",
        index_name="ix_customers_customer_id",
    )
