"""META-01/META-02 proof: `alembic upgrade head` against a throwaway PostgreSQL 18.

Five properties, each a distinct way the migrations could be wrong even if
`alembic upgrade head` itself exits 0: the wrong table set, a non-idempotent
upgrade, a missing/mistyped `hash_version` companion column, a grant wider
than `SELECT, INSERT, UPDATE`, or a missing foreign key on
`ingestion_runs.schema_version_id` now that migration 0009 has created its
referent, `meta.schema_versions`.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

import psycopg
import pytest
from alembic import command
from alembic.config import Config

if TYPE_CHECKING:
    from collections.abc import Callable

# Matches every sibling tests/integration/*.py module's own
# `pytestmark = pytest.mark.integration` idiom (e.g. test_backfill_
# resolution.py, test_volume_anomaly.py) -- this module never carried it
# despite needing the same throwaway-Docker-container fixtures, which made
# `-m integration`-filtered invocations (this plan's own <verify> commands)
# silently select zero tests instead of the intended subset.
pytestmark = pytest.mark.integration

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
    ("meta", "schema_versions"),
    ("meta", "validation_results"),
    ("meta", "rejected_records"),
    # Plan 08.1-05, migration 0024 (dbt's own dedup audit trail, D-09).
    ("meta", "dedup_audit"),
    ("meta", "dedup_decisions"),
    # Plan 08.1-05, migration 0025 (D-17's two-phase claim state machine).
    ("meta", "run_stages"),
    # Plan 09-02, migration 0031 (D-01..D-04's observational watermark).
    ("meta", "watermarks"),
    ("meta", "watermark_history"),
    # Plan 09-02, migration 0032 (D-20..D-24's per-file-per-hop reconciliation).
    ("meta", "reconciliation_results"),
    ("normalized", "customers"),
    ("normalized", "orders"),
}

# Every table this phase's migrations GRANT etl_app exactly SELECT/INSERT/
# UPDATE on — deliberately NOT EXPECTED_TABLES itself: meta.dedup_audit and
# meta.dedup_decisions (migration 0024) are etl_app SELECT-only (dbt_app owns
# the INSERT path there), so they are excluded from this narrower set even
# though they belong in EXPECTED_TABLES. meta.watermark_history (migration
# 0031) and meta.reconciliation_results (migration 0032) are etl_app
# SELECT/INSERT-only (append-only tables, never UPDATE'd in place), so they
# are excluded here too. A future table added to one without the other is a
# visible diff, not a coincidence of reuse.
GRANTED_TABLES = sorted(
    EXPECTED_TABLES
    - {
        ("meta", "dedup_audit"),
        ("meta", "dedup_decisions"),
        ("meta", "watermark_history"),
        ("meta", "reconciliation_results"),
    },
)

HASH_VERSION_COLUMNS = [
    ("meta", "files", "hash_version"),
    ("meta", "config_versions", "hash_version"),
    ("meta", "schema_versions", "hash_version"),
    ("normalized", "customers", "_record_hash_version"),
    ("normalized", "orders", "_record_hash_version"),
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
        for schema in ("meta", "normalized", "staging", "silver"):
            usable = conn.execute(
                "SELECT has_schema_privilege('etl_app', %s, 'USAGE')",
                (schema,),
            ).fetchone()
            assert usable is not None
            assert usable[0] is True, f"etl_app lacks USAGE on schema {schema!r}"


def test_etl_app_can_read_silver_for_publish(migrated_dsn: str) -> None:
    """08.1-13/migration 0029: `publish` (running as `etl_app`) reads FROM `silver.*`.

    Discovered live (not merely in testcontainers): a real, Vault-
    authenticated `etl_app` run of `dataplat publish --dataset customers`
    failed with `psycopg.errors.InsufficientPrivilege: permission denied
    for schema silver` -- migration 0021 made `dbt_app` the sole owner of
    `silver`, and no earlier migration ever granted `etl_app` anything on
    it. `etl_app` gets `SELECT` only, mirroring `dbt_app`'s own
    staging-read boundary (migration 0021) -- never `INSERT`/`UPDATE`/
    `DELETE`, since D-08 keeps `silver` writable by `dbt_app` alone.
    """
    with psycopg.connect(migrated_dsn) as conn:
        for table in ("customers", "orders"):
            rows = conn.execute(
                """
                SELECT privilege_type
                  FROM information_schema.role_table_grants
                 WHERE grantee = 'etl_app' AND table_schema = 'silver' AND table_name = %s
                """,
                (table,),
            ).fetchall()
            privileges = {row[0] for row in rows}
            assert privileges == {"SELECT"}, (
                f"silver.{table}: expected etl_app to hold exactly SELECT, got {privileges}"
            )


def test_ingestion_runs_schema_version_id_has_an_fk_after_0009(migrated_dsn: str) -> None:
    """Migration 0004 deferred this FK; migration 0009 closes it.

    This test's predecessor asserted the opposite (`rows == []`) and passed
    BECAUSE the FK did not exist yet -- once migration 0009 landed it would
    otherwise start failing by design. This inversion is that fix, not an
    incidental side effect.
    """
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
    assert len(rows) == 1, f"schema_version_id must carry exactly one FK constraint, found: {rows}"
    assert rows[0][0] == "fk_ingestion_runs_schema_version_id"


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


def test_grafana_reader_role_exists_and_is_select_only(migrated_dsn: str) -> None:
    """T-07-02 mitigation: `grafana_reader` is SELECT-only, scoped to `expected_objects` below.

    Queries `pg_roles` (existence, LOGIN, no superuser/createrole) and
    `information_schema.role_table_grants` (no `INSERT`/`UPDATE`/`DELETE`
    anywhere; `SELECT` on exactly the objects `expected_objects` names --
    never a direct table grant on `normalized.customers`, whose data the
    lineage view surfaces under its own owner's privileges instead).
    """
    with psycopg.connect(migrated_dsn) as conn:
        role_row = conn.execute(
            """
            SELECT rolcanlogin, rolsuper, rolcreaterole
              FROM pg_roles
             WHERE rolname = 'grafana_reader'
            """,
        ).fetchone()
        assert role_row is not None, "grafana_reader role does not exist"
        rolcanlogin, rolsuper, rolcreaterole = role_row
        assert rolcanlogin is True, "grafana_reader must have LOGIN"
        assert rolsuper is False, "grafana_reader must not be superuser"
        assert rolcreaterole is False, "grafana_reader must not be able to create roles"

        grant_rows = conn.execute(
            """
            SELECT table_schema, table_name, privilege_type
              FROM information_schema.role_table_grants
             WHERE grantee = 'grafana_reader'
            """,
        ).fetchall()

    granted: dict[tuple[str, str], set[str]] = {}
    for schema, table, privilege in grant_rows:
        granted.setdefault((schema, table), set()).add(privilege)

    expected_objects = {
        ("meta", "datasets"),
        ("meta", "files"),
        ("meta", "ingestion_runs"),
        ("meta", "v_customers_lineage"),
        # Migration 0024 (plan 08.1-05): read access for dashboards, never write.
        ("meta", "dedup_audit"),
        ("meta", "dedup_decisions"),
        # Migrations 0031/0032 (plan 09-02): read access for dashboards, never write.
        ("meta", "watermarks"),
        ("meta", "watermark_history"),
        ("meta", "reconciliation_results"),
    }
    assert set(granted.keys()) == expected_objects, (
        f"grafana_reader must hold grants on exactly {expected_objects}, got {set(granted.keys())}"
    )
    for obj, privileges in granted.items():
        assert privileges == {"SELECT"}, f"{obj}: expected exactly SELECT, got {privileges}"


def _silver_constraint_types(dsn: str, *, table: str, column: str) -> tuple[str, ...]:
    """Return every `table_constraints.constraint_type` covering one silver column alone.

    Mirrors `_customers_customer_id_constraint_types` above, generalised to
    `schema="silver"` and a caller-supplied table/column (D-14's business-key
    UNIQUE constraint exists on two different tables here, not one).
    """
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            """
            SELECT tc.constraint_type
              FROM information_schema.table_constraints tc
              JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
               AND tc.table_schema = kcu.table_schema
             WHERE tc.table_schema = 'silver'
               AND tc.table_name = %s
               AND kcu.column_name = %s
            """,
            (table, column),
        ).fetchall()
    return tuple(row[0] for row in rows)


def _seed_minimal_lineage_row(dsn: str, *, dataset_name: str) -> tuple[int, int, int]:
    """Insert one minimal row apiece into datasets/config_versions/files/batches/ingestion_runs.

    Returns `(run_id, file_id, batch_id)` — the three lineage FK targets
    `silver.customers`/`silver.orders` require on every row. Exists so this
    module's UNIQUE-constraint tests can INSERT a real row directly into
    silver — bypassing dbt entirely, per the plan's own acceptance criteria —
    without depending on the full `run_ingest()` pipeline machinery
    `tests/integration/test_run_ingest.py` exercises for a different purpose.
    `dataset_name` should be unique per test to avoid colliding with rows
    other modules seed into the same session-scoped `migrated_dsn`.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        dataset_row = conn.execute(
            """
            INSERT INTO meta.datasets (dataset_name)
            VALUES (%s)
            ON CONFLICT (dataset_name) DO UPDATE SET dataset_name = EXCLUDED.dataset_name
            RETURNING dataset_id
            """,
            (dataset_name,),
        ).fetchone()
        assert dataset_row is not None
        dataset_id = dataset_row[0]

        config_version_row = conn.execute(
            """
            INSERT INTO meta.config_versions
                (dataset_id, version, config_hash, config_document,
                 config_schema_version, valid_from)
            VALUES (%s, 1, %s, '{}'::jsonb, 1, now())
            RETURNING config_version_id
            """,
            (dataset_id, f"{dataset_name}-hash"),
        ).fetchone()
        assert config_version_row is not None
        config_version_id = config_version_row[0]

        file_row = conn.execute(
            """
            INSERT INTO meta.files
                (dataset_id, object_uri, content_sha256, size_bytes, filename, status)
            VALUES (%s, %s, %s, 0, 'test.csv', 'DISCOVERED')
            RETURNING file_id
            """,
            (dataset_id, f"s3://raw/{dataset_name}/test.csv", f"{dataset_name}-sha".encode()),
        ).fetchone()
        assert file_row is not None
        file_id = file_row[0]

        batch_row = conn.execute(
            """
            INSERT INTO meta.batches (dataset_id, batch_key, status)
            VALUES (%s, %s, 'OPEN')
            RETURNING batch_id
            """,
            (dataset_id, f"{dataset_name}-batch"),
        ).fetchone()
        assert batch_row is not None
        batch_id = batch_row[0]

        run_row = conn.execute(
            """
            INSERT INTO meta.ingestion_runs
                (idempotency_key, dataset_id, config_version_id, processor_version,
                 processor_image_digest, status)
            VALUES (%s, %s, %s, 'test', 'sha256:test', 'SUCCEEDED')
            RETURNING run_id
            """,
            (f"{dataset_name}:1", dataset_id, config_version_id),
        ).fetchone()
        assert run_row is not None
        run_id = run_row[0]
    return run_id, file_id, batch_id


def test_silver_customer_id_has_a_real_unique_constraint(migrated_dsn: str) -> None:
    """D-14: `silver.customers.customer_id` carries a real UNIQUE constraint, not just dbt logic."""
    assert _silver_constraint_types(migrated_dsn, table="customers", column="customer_id") == (
        "UNIQUE",
    )

    run_id, file_id, batch_id = _seed_minimal_lineage_row(
        migrated_dsn,
        dataset_name="test_migrations_silver_customers_unique",
    )
    with psycopg.connect(migrated_dsn) as conn:
        conn.execute(
            """
            INSERT INTO silver.customers
                (customer_id, name, country, birth_date, event_ts,
                 _run_id, _file_id, _batch_id, _source_row_number, _record_hash)
            VALUES (%s, 'Ada', 'US', NULL, NULL, %s, %s, %s, 1, %s)
            """,
            ("dup-customer-1", run_id, file_id, batch_id, b"hash-1"),
        )
        conn.commit()

        with pytest.raises(psycopg.errors.UniqueViolation):
            conn.execute(
                """
                INSERT INTO silver.customers
                    (customer_id, name, country, birth_date, event_ts,
                     _run_id, _file_id, _batch_id, _source_row_number, _record_hash)
                VALUES (%s, 'Ada Duplicate', 'US', NULL, NULL, %s, %s, %s, 2, %s)
                """,
                ("dup-customer-1", run_id, file_id, batch_id, b"hash-2"),
            )
        conn.rollback()

        # `silver.customers` is a single, session-shared table with no
        # dataset_id scoping, and `MergePublisher.publish()` (`merge.py`)
        # reads it in full, unconditionally -- a probe row left behind here
        # would otherwise permanently pollute every real publish_ingest()
        # call for the rest of this test session (any other file's own
        # real, unfiltered `silver.customers` read would try to publish
        # this non-numeric `customer_id` and fail). The first INSERT above
        # already committed, so this cleanup needs its own explicit commit.
        conn.execute(
            "DELETE FROM silver.customers WHERE customer_id = %s",
            ("dup-customer-1",),
        )
        conn.commit()


def test_silver_order_id_has_a_real_unique_constraint(migrated_dsn: str) -> None:
    """D-14: `silver.orders.order_id` carries a real UNIQUE constraint, not just dbt logic."""
    assert _silver_constraint_types(migrated_dsn, table="orders", column="order_id") == ("UNIQUE",)

    run_id, file_id, batch_id = _seed_minimal_lineage_row(
        migrated_dsn,
        dataset_name="test_migrations_silver_orders_unique",
    )
    with psycopg.connect(migrated_dsn) as conn:
        conn.execute(
            """
            INSERT INTO silver.orders
                (order_id, customer_id, order_date, amount,
                 _run_id, _file_id, _batch_id, _source_row_number, _record_hash)
            VALUES (%s, 'cust-1', NULL, NULL, %s, %s, %s, 1, %s)
            """,
            ("dup-order-1", run_id, file_id, batch_id, b"hash-1"),
        )
        conn.commit()

        with pytest.raises(psycopg.errors.UniqueViolation):
            conn.execute(
                """
                INSERT INTO silver.orders
                    (order_id, customer_id, order_date, amount,
                     _run_id, _file_id, _batch_id, _source_row_number, _record_hash)
                VALUES (%s, 'cust-1', NULL, NULL, %s, %s, %s, 2, %s)
                """,
                ("dup-order-1", run_id, file_id, batch_id, b"hash-2"),
            )
        conn.rollback()

        # See test_silver_customer_id_has_a_real_unique_constraint's own
        # cleanup comment above -- `silver.orders` is the same
        # session-shared, unscoped shape.
        conn.execute(
            "DELETE FROM silver.orders WHERE order_id = %s",
            ("dup-order-1",),
        )
        conn.commit()


def test_dbt_app_role_is_scoped_correctly(migrated_dsn: str) -> None:
    """D-08: `dbt_app` reads `staging`, owns `silver`, and touches nothing in normalized/meta."""
    with psycopg.connect(migrated_dsn) as conn:
        # Positive: staging USAGE + SELECT on exactly its two tables.
        staging_usable = conn.execute(
            "SELECT has_schema_privilege('dbt_app', 'staging', 'USAGE')",
        ).fetchone()
        assert staging_usable is not None
        assert staging_usable[0] is True, "dbt_app lacks USAGE on schema staging"

        staging_grants = conn.execute(
            """
            SELECT table_schema, table_name, privilege_type
              FROM information_schema.role_table_grants
             WHERE grantee = 'dbt_app' AND table_schema = 'staging'
            """,
        ).fetchall()
        staging_tables = {(schema, name) for schema, name, _ in staging_grants}
        assert staging_tables == {("staging", "customers"), ("staging", "orders")}, staging_tables
        for _, _, privilege in staging_grants:
            assert privilege == "SELECT", f"dbt_app should only ever hold SELECT, got {privilege}"

        # Positive: dbt_app owns both silver tables outright.
        for table in ("customers", "orders"):
            owner_row = conn.execute(
                "SELECT tableowner FROM pg_tables WHERE schemaname = 'silver' AND tablename = %s",
                (table,),
            ).fetchone()
            assert owner_row is not None, f"silver.{table} does not exist"
            assert owner_row[0] == "dbt_app", f"silver.{table} owner is {owner_row[0]!r}"

        # Negative: D-08's hard boundary — zero grants on normalized, and on
        # meta except the narrow meta.dedup_audit/meta.dedup_decisions slice
        # migration 0024 (plan 08.1-05) carves out, plus
        # meta.reconciliation_results (migration 0032, plan 09-02) -- its own
        # INSERT-only bronze_silver-hop post-hook write path.
        forbidden_grants = conn.execute(
            """
            SELECT table_schema, table_name
              FROM information_schema.role_table_grants
             WHERE grantee = 'dbt_app'
               AND (
                   table_schema = 'normalized'
                   OR (table_schema = 'meta'
                       AND table_name NOT IN (
                           'dedup_audit', 'dedup_decisions', 'reconciliation_results'
                       ))
               )
            """,
        ).fetchall()
        assert forbidden_grants == [], (
            f"dbt_app must never be granted on normalized/meta "
            f"(beyond dedup_audit/dedup_decisions/reconciliation_results), found: {forbidden_grants}"
        )


def test_dbt_app_can_insert_dedup_audit_but_not_update_or_delete(migrated_dsn: str) -> None:
    """T-08.1-10: `dbt_app` gets INSERT on dedup_audit/dedup_decisions, never UPDATE/DELETE.

    Connects as the throwaway container's superuser and `SET ROLE dbt_app`
    for the duration of the check — `dbt_app` carries no password in the
    migrations themselves (migration 0021's own docstring: the password is
    set out-of-band, by a Vault-bootstrap script extension), so `SET ROLE`
    (available to a superuser without a password) is how this test exercises
    `dbt_app`'s *actual* grants rather than merely reading
    `information_schema.role_table_grants` rows.
    """
    _seed_minimal_lineage_row(migrated_dsn, dataset_name="test_migrations_dedup_audit_grants")
    with psycopg.connect(migrated_dsn, autocommit=True) as conn:
        dataset_row = conn.execute(
            "SELECT dataset_id FROM meta.datasets WHERE dataset_name = %s",
            ("test_migrations_dedup_audit_grants",),
        ).fetchone()
        assert dataset_row is not None
        dataset_id = dataset_row[0]

        conn.execute("SET ROLE dbt_app")
        try:
            insert_row = conn.execute(
                """
                INSERT INTO meta.dedup_audit
                    (dataset_id, dbt_invocation_id, model_name,
                     records_received, records_accepted, records_deduplicated)
                VALUES (%s, %s, %s, 10, 8, 2)
                RETURNING dedup_audit_id
                """,
                (dataset_id, "11111111-1111-1111-1111-111111111111", "silver_customers"),
            ).fetchone()
            assert insert_row is not None
            dedup_audit_id = insert_row[0]

            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute(
                    "UPDATE meta.dedup_audit SET records_received = 99 WHERE dedup_audit_id = %s",
                    (dedup_audit_id,),
                )

            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute(
                    "DELETE FROM meta.dedup_audit WHERE dedup_audit_id = %s",
                    (dedup_audit_id,),
                )
        finally:
            conn.execute("RESET ROLE")


def test_run_stages_enforces_unique_run_id_stage_name(migrated_dsn: str) -> None:
    """D-17: `meta.run_stages` enforces `UNIQUE(run_id, stage_name)`."""
    run_id, _file_id, _batch_id = _seed_minimal_lineage_row(
        migrated_dsn,
        dataset_name="test_migrations_run_stages_unique",
    )
    with psycopg.connect(migrated_dsn) as conn:
        conn.execute(
            """
            INSERT INTO meta.run_stages (run_id, stage_name, status)
            VALUES (%s, 'STAGE_LOAD', 'PENDING')
            """,
            (run_id,),
        )
        conn.commit()

        with pytest.raises(psycopg.errors.UniqueViolation):
            conn.execute(
                """
                INSERT INTO meta.run_stages (run_id, stage_name, status)
                VALUES (%s, 'STAGE_LOAD', 'PENDING')
                """,
                (run_id,),
            )
        conn.rollback()


_GOLD_INDEXES = (
    ("normalized", "orders", "ix_orders_order_date"),
    # Pre-existing (migration 0016), not created by 0027 -- see 0027's own
    # module docstring. Still part of the five named physical-modeling
    # indexes D-13 asks for, so it belongs in this completeness check.
    ("normalized", "orders", "ix_orders_customer_id"),
    ("normalized", "orders", "ix_orders_order_date_customer_id"),
    ("normalized", "customers", "ix_customers_event_ts"),
    ("normalized", "customers", "ix_customers_country"),
)


def test_gold_indexes_exist_and_business_key_uniqueness_is_unchanged(migrated_dsn: str) -> None:
    """D-13: all five gold physical-modeling indexes exist, uniqueness guarantees are untouched.

    Two independent properties: (1) `pg_indexes` shows all five named
    indexes after `alembic upgrade head`; (2) the pre-existing
    `uq_customers_customer_id`/`uq_orders_order_id` `UNIQUE` constraints
    (migrations 0006/0017) still reject a duplicate business key -- proving
    migration 0027 (indexes only, per its own module docstring) did not
    weaken `MergePublisher`/`OrdersMergePublisher`'s `ON CONFLICT` targets.
    """
    for schema, _table, index_name in _GOLD_INDEXES:
        assert _index_exists(migrated_dsn, schema=schema, index_name=index_name), (
            f"missing index {schema}.{index_name}"
        )

    assert _customers_customer_id_constraint_types(migrated_dsn) == ("UNIQUE",)

    run_id, file_id, batch_id = _seed_minimal_lineage_row(
        migrated_dsn,
        dataset_name="test_migrations_gold_indexes_orders_unique",
    )
    with psycopg.connect(migrated_dsn) as conn:
        conn.execute(
            """
            INSERT INTO normalized.orders
                (order_id, customer_id, order_date, amount,
                 _run_id, _file_id, _batch_id, _source_row_number, _record_hash)
            VALUES (%s, %s, NULL, NULL, %s, %s, %s, 1, %s)
            """,
            (777_001, 1, run_id, file_id, batch_id, b"gold-idx-hash-1"),
        )
        conn.commit()

        with pytest.raises(psycopg.errors.UniqueViolation):
            conn.execute(
                """
                INSERT INTO normalized.orders
                    (order_id, customer_id, order_date, amount,
                     _run_id, _file_id, _batch_id, _source_row_number, _record_hash)
                VALUES (%s, %s, NULL, NULL, %s, %s, %s, 2, %s)
                """,
                (777_001, 1, run_id, file_id, batch_id, b"gold-idx-hash-2"),
            )
        conn.rollback()


def test_dbt_app_has_no_grant_on_run_stages(migrated_dsn: str) -> None:
    """D-02: `etl_app` claims/heartbeats `meta.run_stages`; `dbt_app` never touches it."""
    with psycopg.connect(migrated_dsn) as conn:
        etl_app_grants = conn.execute(
            """
            SELECT privilege_type
              FROM information_schema.role_table_grants
             WHERE grantee = 'etl_app' AND table_schema = 'meta' AND table_name = 'run_stages'
            """,
        ).fetchall()
        assert {row[0] for row in etl_app_grants} == {"SELECT", "INSERT", "UPDATE"}

        dbt_app_grants = conn.execute(
            """
            SELECT privilege_type
              FROM information_schema.role_table_grants
             WHERE grantee = 'dbt_app' AND table_schema = 'meta' AND table_name = 'run_stages'
            """,
        ).fetchall()
        assert dbt_app_grants == [], (
            f"dbt_app must have zero grant on meta.run_stages, found: {dbt_app_grants}"
        )
