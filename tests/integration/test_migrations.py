"""META-01/META-02 proof: `alembic upgrade head` against a throwaway PostgreSQL 18.

Five properties, each a distinct way the migrations could be wrong even if
`alembic upgrade head` itself exits 0: the wrong table set, a non-idempotent
upgrade, a missing/mistyped `hash_version` companion column, a grant wider
than `SELECT, INSERT, UPDATE`, or an accidental foreign key on
`ingestion_runs.schema_version_id` before `meta.schema_versions` exists.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import psycopg

if TYPE_CHECKING:
    from collections.abc import Callable

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
