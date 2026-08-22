"""Integration tests for D-29's rebuild-from-raw reconciliation building blocks.

Plan 11-11 Tasks 1-2. `11-RESEARCH.md`'s Pitfall 7 and 8 are the reason this module exists:
`_table_checksum` (`packages/dataplat/src/dataplat/pipeline/run.py`) hashes every column of a
table unconditionally, so a literal, unchanged reuse of it before/after a rebuild-from-raw
would ALWAYS report a false mismatch -- the six embedded lineage columns a rebuild
deliberately re-mints (`_run_id`, `_file_id`, `_batch_id`, `_source_row_number`,
`_ingested_at`, `_dbt_loaded_at`) never match across a drop-and-rebuild by design. Task 1
proves the fix (an additive `columns=` parameter) leaves `_table_checksum`'s existing,
unscoped behavior byte-for-byte unchanged AND proves the new scoped path genuinely excludes
whatever columns a caller names. Task 2 proves the snapshot/compare arithmetic built on top
of it (`dataplat.pipeline.rebuild_reconciliation`) is correct in isolation, before plan 11-12
ever wires it into a real drop-and-rebuild.

Mirrors `test_reconciliation.py`'s own fixture shape (`migrated_dsn`, direct
`psycopg.connect(...)` helpers, per-file-duplicated seeding helpers -- this test suite's
established convention, restated in that file's own module docstring) rather than inventing
a different harness.

`test_compare_snapshots_*` below need NO database connection at all -- `compare_snapshots`
is a pure function (Task 2's own action text). They are still collected in this file, under
the SAME `pytest.mark.integration` mark as every other test here, for cohesion with the
`snapshot_table_state`/`snapshot_customers_scd2_state` tests they directly complement
(Task 2's own acceptance criteria explicitly sanctions this tradeoff over a file split). This
is not merely a style choice: `tests/integration/conftest.py`'s `_require_docker` fixture is
`autouse=True` at the whole-directory level, and that conftest module imports
`testcontainers.community.{minio,postgres}` unconditionally at module scope -- any test
physically located under `tests/integration/` already requires the `cluster` dependency
group to even be collected, regardless of its own markers. `make check`'s offline gate
excludes `tests/integration/` by PATH (`Makefile`'s `test`/`check` targets name only
`tests/unit`/`tests/regression`), not by marker filtering, so there is no real offline-gate
benefit to splitting the pure tests into a separate file.
"""

from __future__ import annotations

import dataclasses
import hashlib
import inspect
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import psycopg
import pytest

from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.pipeline import rebuild_reconciliation
from dataplat.pipeline.rebuild_reconciliation import (
    CustomersScd2Snapshot,
    RebuildComparisonResult,
    ScdKeySnapshot,
    TableSnapshot,
    compare_snapshots,
    snapshot_customers_scd2_state,
    snapshot_table_state,
)
from dataplat.pipeline.run import _table_checksum
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    from collections.abc import Sequence

pytestmark = pytest.mark.integration


# --- Shared seeding helpers (duplicated per this suite's own convention) ----


def _insert_silver_customers_row(  # noqa: PLR0913 -- one keyword per silver column, mirrors test_reconciliation.py
    conn: psycopg.Connection[Any],
    *,
    customer_id: str,
    name: str,
    country: str,
    birth_date: str,
    event_ts: str,
    run_id: int,
    file_id: int,
    batch_id: int,
    source_row_number: int,
    record_hash: bytes,
) -> None:
    conn.execute(
        """
        INSERT INTO silver.customers (
            customer_id, name, country, birth_date, event_ts,
            _run_id, _file_id, _batch_id, _source_row_number,
            _record_hash, _record_hash_version
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
        """,
        (
            customer_id,
            name,
            country,
            birth_date,
            event_ts,
            run_id,
            file_id,
            batch_id,
            source_row_number,
            record_hash,
        ),
    )


def _insert_scd2_customer_version(  # noqa: PLR0913 -- one keyword per normalized.customers SCD2 column
    conn: psycopg.Connection[Any],
    *,
    customer_id: int,
    name: str,
    country: str,
    birth_date: str,
    event_ts: str,
    valid_to: str,
    is_current: bool,
    run_id: int,
    file_id: int,
    batch_id: int,
    source_row_number: int,
    record_hash: bytes,
) -> None:
    """Insert one SCD2 version row DIRECTLY into `normalized.customers`.

    Mirrors `test_reconciliation.py`'s own identically-named helper -- bypasses the SCD
    Publisher entirely, fine for this module's own snapshot-only fixture needs.

    `event_ts`/`valid_to` must be genuinely non-overlapping for the SAME `customer_id` --
    migration 0035's `excl_customers_business_key_validity` EXCLUDE constraint rejects an
    overlapping pair.
    """
    conn.execute(
        """
        INSERT INTO normalized.customers (
            customer_id, name, country, birth_date, event_ts, valid_to, is_current,
            _run_id, _file_id, _batch_id, _source_row_number,
            _record_hash, _record_hash_version
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
        """,
        (
            customer_id,
            name,
            country,
            birth_date,
            event_ts,
            valid_to,
            is_current,
            run_id,
            file_id,
            batch_id,
            source_row_number,
            record_hash,
        ),
    )


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Get-or-insert a synthetic, CURRENT `meta.config_versions` row.

    Mirrors `test_reconciliation.py`'s own `_insert_config_version` helper, duplicated
    locally per this suite's established per-file helper convention.
    """
    with psycopg.connect(dsn) as conn:
        existing = conn.execute(
            """
            SELECT config_version_id
              FROM meta.config_versions
             WHERE dataset_id = %(dataset_id)s AND valid_to IS NULL
            """,
            {"dataset_id": dataset_id},
        ).fetchone()
        if existing is not None:
            return int(existing[0])
        row = conn.execute(
            """
            INSERT INTO meta.config_versions (
                dataset_id, version, config_hash, config_document,
                config_schema_version, valid_from
            ) VALUES (
                %(dataset_id)s,
                (
                    SELECT COALESCE(MAX(version), 0) + 1
                    FROM meta.config_versions
                    WHERE dataset_id = %(dataset_id)s
                ),
                %(config_hash)s, %(config_document)s::jsonb, %(config_schema_version)s, now()
            )
            RETURNING config_version_id
            """,
            {
                "dataset_id": dataset_id,
                "config_hash": "synthetic-hash-for-rebuild-recon-test",
                "config_document": json.dumps({"synthetic": True}),
                "config_schema_version": 1,
            },
        ).fetchone()
        assert row is not None
        conn.commit()
        return int(row[0])


def _seed_dataset_file_batch_run(
    migrated_dsn: str, *, dataset_name: str, key_suffix: str
) -> tuple[int, int, int, int]:
    """Create dataset/config_version/file/batch/run rows -- the FK targets lineage columns need.

    Mirrors `test_reconciliation.py`'s own `_seed_staged_run`, duplicated locally per this
    suite's established per-file helper convention. Returns
    `(dataset_id, run_id, file_id, batch_id)`.
    """
    pool = create_pool(migrated_dsn)
    pool.open(wait=True)
    try:
        repository = PostgresMetadataRepository(pool)
        dataset_id = repository.get_or_create_dataset(dataset_name)
        config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id)
        file_id = repository.create_file(
            dataset_id=dataset_id,
            object_uri=f"s3://raw/{dataset_name}/{key_suffix}.csv",
            content_sha256=hashlib.sha256(key_suffix.encode()).digest(),
            hash_version=1,
            size_bytes=10,
            filename=f"{key_suffix}.csv",
            status="DISCOVERED",
        )
        batch_id = repository.create_batch(
            dataset_id=dataset_id,
            batch_key=f"{key_suffix}:2026-08-22:1",
            status="OPEN",
        )
        run_id = repository.create_ingestion_run(
            idempotency_key=f"{key_suffix}:1",
            dataset_id=dataset_id,
            config_version_id=config_version_id,
            processor_version="0.1.0",
            processor_image_digest="sha256:testdigest",
            status="STAGED",
            file_id=file_id,
            batch_id=batch_id,
        )
    finally:
        pool.close()

    return dataset_id, run_id, file_id, batch_id


# --- Task 1: _table_checksum's additive `columns=` parameter -----------------


def test_table_checksum_with_no_columns_arg_is_unchanged(migrated_dsn: str) -> None:
    """No `columns` arg reproduces the exact pre-D-29 query byte-for-byte.

    Independently recomputes the literal pre-change query
    (`SELECT to_hex(bit_xor(...)) FROM {table} t`) here and asserts equality against
    `_table_checksum`'s own output, rather than trusting the function's output circularly.
    """
    _dataset_id, run_id, file_id, batch_id = _seed_dataset_file_batch_run(
        migrated_dsn, dataset_name="rebuild_recon_nochg", key_suffix="checksum_nochg"
    )
    with psycopg.connect(migrated_dsn) as conn:
        _insert_silver_customers_row(
            conn,
            # A pure-digit string, not `9_700_001` with underscores: silver.customers/
            # silver.orders are session-scoped tables shared across this whole directory,
            # and MergePublisher's real publish reads the FULL table unconditionally, so
            # every row's business key must cast to normalized.customers' `integer`
            # customer_id column (tests/integration/conftest.py's
            # `_clean_up_non_numeric_silver_business_keys` docstring) -- a non-numeric key
            # left behind would abort every later real publish for the rest of the session.
            customer_id="9702001",
            name="ChecksumNoChg",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-01-01T00:00:00+00:00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=b"checksum-nochg-hash-000000000000",
        )
        conn.commit()

        golden = conn.execute(
            "SELECT to_hex(bit_xor(('x' || substr(md5(t::text), 1, 16))::bit(64)::bigint)) "
            "FROM silver.customers t"
        ).fetchone()
        assert golden is not None

        result = _table_checksum(conn, "silver.customers")

    assert result == str(golden[0])


def test_table_checksum_scoped_to_columns_excludes_unlisted_columns(migrated_dsn: str) -> None:
    """A column-scoped checksum ignores a lineage-shaped column outside `columns=`.

    Two single-row temp tables hold IDENTICAL business data but a DIFFERENT `_run_id` --
    the scoped checksum (business columns only) must match across both; the unscoped
    checksum (every column) must differ.
    """
    business_columns: Sequence[str] = ("business_a", "business_b")
    with psycopg.connect(migrated_dsn) as conn:
        conn.execute(
            "CREATE TEMPORARY TABLE checksum_scope_row_a AS "
            "SELECT 'alice'::text AS business_a, 'US'::text AS business_b, "
            "111::bigint AS _run_id"
        )
        conn.execute(
            "CREATE TEMPORARY TABLE checksum_scope_row_b AS "
            "SELECT 'alice'::text AS business_a, 'US'::text AS business_b, "
            "222::bigint AS _run_id"
        )

        scoped_a = _table_checksum(conn, "checksum_scope_row_a", columns=business_columns)
        scoped_b = _table_checksum(conn, "checksum_scope_row_b", columns=business_columns)
        unscoped_a = _table_checksum(conn, "checksum_scope_row_a")
        unscoped_b = _table_checksum(conn, "checksum_scope_row_b")

    assert scoped_a is not None
    assert scoped_a == scoped_b
    assert unscoped_a != unscoped_b


def test_table_checksum_columns_arg_is_order_independent_like_the_original(
    migrated_dsn: str,
) -> None:
    """Reordering ROWS (not the `columns` list) doesn't change a column-scoped checksum.

    Mirrors the pre-existing `bit_xor` commutativity property `_table_checksum`'s own
    docstring already documents, proven here specifically through the NEW column-scoped path.
    """
    business_columns: Sequence[str] = ("business_a", "business_b")
    with psycopg.connect(migrated_dsn) as conn:
        conn.execute(
            "CREATE TEMPORARY TABLE checksum_order_fwd "
            "(business_a text, business_b text, _run_id bigint)"
        )
        conn.execute(
            "INSERT INTO checksum_order_fwd VALUES ('a', '1', 10), ('b', '2', 20), ('c', '3', 30)"
        )
        conn.execute(
            "CREATE TEMPORARY TABLE checksum_order_rev "
            "(business_a text, business_b text, _run_id bigint)"
        )
        conn.execute(
            "INSERT INTO checksum_order_rev VALUES ('c', '3', 30), ('b', '2', 20), ('a', '1', 10)"
        )

        fwd = _table_checksum(conn, "checksum_order_fwd", columns=business_columns)
        rev = _table_checksum(conn, "checksum_order_rev", columns=business_columns)

    assert fwd is not None
    assert fwd == rev


# --- Task 2: rebuild_reconciliation.py -- snapshot + compare -----------------


def test_snapshot_table_state_captures_row_count_and_scoped_checksum(migrated_dsn: str) -> None:
    """`snapshot_table_state()` reports `row_count` + a Task-1-column-scoped `checksum`."""
    business_columns = ("customer_id", "name", "country", "birth_date", "event_ts")
    _dataset_id, run_id, file_id, batch_id = _seed_dataset_file_batch_run(
        migrated_dsn, dataset_name="rebuild_recon_snapshot", key_suffix="snapshot_basic"
    )
    with psycopg.connect(migrated_dsn) as conn:
        for i in range(3):
            _insert_silver_customers_row(
                conn,
                # Pure-digit string -- see the identical comment on the nochg test above.
                customer_id=f"970301{i}",
                name=f"Snapshot{i}",
                country="US",
                birth_date="1990-01-01",
                event_ts="2026-01-01T00:00:00+00:00",
                run_id=run_id,
                file_id=file_id,
                batch_id=batch_id,
                source_row_number=i + 1,
                record_hash=f"snapshot-basic-hash-{i}".encode().ljust(32, b"0"),
            )
        conn.commit()

        expected_checksum = _table_checksum(conn, "silver.customers", columns=business_columns)
        expected_row_count = conn.execute("SELECT count(*) FROM silver.customers").fetchone()
        assert expected_row_count is not None

        snapshot = snapshot_table_state(conn, "silver.customers", business_columns=business_columns)

    assert isinstance(snapshot, TableSnapshot)
    assert snapshot.table == "silver.customers"
    assert snapshot.row_count == int(expected_row_count[0])
    assert snapshot.checksum == expected_checksum


def test_snapshot_customers_scd2_state_captures_version_count_and_is_current_state_per_key(
    migrated_dsn: str,
) -> None:
    """A dedicated SCD2-aware snapshot captures per-key version count + current-row validity."""
    business_columns = ("customer_id", "name", "country", "birth_date", "event_ts")
    _dataset_id, run_id, file_id, batch_id = _seed_dataset_file_batch_run(
        migrated_dsn, dataset_name="rebuild_recon_scd2", key_suffix="snapshot_scd2"
    )
    scd2_customer_id = 9704001
    with psycopg.connect(migrated_dsn) as conn:
        _insert_scd2_customer_version(
            conn,
            customer_id=scd2_customer_id,
            name="ScdOld",
            country="US",
            birth_date="1990-01-01",
            event_ts="2020-01-01T00:00:00+00:00",
            valid_to="2026-01-01T00:00:00+00:00",
            is_current=False,
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=b"snapshot-scd2-old-hash-0000000000",
        )
        _insert_scd2_customer_version(
            conn,
            customer_id=scd2_customer_id,
            name="ScdNew",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-01-01T00:00:00+00:00",
            valid_to="9999-12-31T00:00:00+00:00",
            is_current=True,
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=2,
            record_hash=b"snapshot-scd2-new-hash-0000000000",
        )
        conn.commit()

        scd2_snapshot = snapshot_customers_scd2_state(conn, business_columns=business_columns)

    assert isinstance(scd2_snapshot, CustomersScd2Snapshot)
    key_snapshot = next(k for k in scd2_snapshot.keys if k.business_key == str(scd2_customer_id))
    assert key_snapshot.version_count == 2
    assert key_snapshot.current_valid_from == datetime(2026, 1, 1, tzinfo=UTC)
    assert key_snapshot.current_valid_to == datetime(9999, 12, 31, tzinfo=UTC)
    assert key_snapshot.current_is_current is True


def test_compare_snapshots_reports_full_match_for_identical_snapshots() -> None:
    """Comparing an identical pre/post snapshot pair reports a full match, no differences."""
    before = TableSnapshot(table="normalized.customers", row_count=3, checksum="abc123")
    after = TableSnapshot(table="normalized.customers", row_count=3, checksum="abc123")

    result = compare_snapshots(before, after)

    assert isinstance(result, RebuildComparisonResult)
    assert result.matches is True
    assert result.mismatches == ()

    scd2_before = CustomersScd2Snapshot(
        table_snapshot=before,
        keys=(
            ScdKeySnapshot(
                business_key="1",
                version_count=1,
                current_valid_from=datetime(2026, 1, 1, tzinfo=UTC),
                current_valid_to=datetime(9999, 12, 31, tzinfo=UTC),
                current_is_current=True,
            ),
        ),
    )
    scd2_after = dataclasses.replace(scd2_before)

    scd2_result = compare_snapshots(scd2_before, scd2_after)

    assert scd2_result.matches is True
    assert scd2_result.mismatches == ()


def test_compare_snapshots_reports_the_specific_mismatch_for_an_altered_snapshot() -> None:
    """Mutating exactly one field causes `compare_snapshots()` to name exactly that field."""
    before = TableSnapshot(table="normalized.customers", row_count=3, checksum="abc123")

    row_count_mismatch = compare_snapshots(
        before, dataclasses.replace(before, row_count=before.row_count + 1)
    )
    assert row_count_mismatch.matches is False
    assert row_count_mismatch.mismatches == ("row_count",)

    checksum_mismatch = compare_snapshots(
        before, dataclasses.replace(before, checksum="deliberately-altered-checksum")
    )
    assert checksum_mismatch.matches is False
    assert checksum_mismatch.mismatches == ("checksum",)

    scd2_before = CustomersScd2Snapshot(
        table_snapshot=before,
        keys=(
            ScdKeySnapshot(
                business_key="1",
                version_count=1,
                current_valid_from=datetime(2026, 1, 1, tzinfo=UTC),
                current_valid_to=datetime(9999, 12, 31, tzinfo=UTC),
                current_is_current=True,
            ),
        ),
    )
    scd2_after = dataclasses.replace(
        scd2_before,
        keys=(dataclasses.replace(scd2_before.keys[0], version_count=2),),
    )

    scd2_mismatch = compare_snapshots(scd2_before, scd2_after)

    assert scd2_mismatch.matches is False
    assert scd2_mismatch.mismatches == ("scd2_key:1.version_count",)


def test_rebuild_reconciliation_module_performs_no_mutating_sql() -> None:
    """Static guard: the module's source issues no mutating SQL statement (acceptance criteria).

    Checks actual SQL-statement-shaped keywords (``DROP TABLE``/``DROP SCHEMA``/``DELETE
    FROM``/``TRUNCATE``), not a bare ``"DROP "`` substring -- the module's own docstring
    prose (e.g. "reconciles to its PRE-DROP STATE") legitimately contains "DROP" followed by
    a space without being a SQL statement.
    """
    source = inspect.getsource(rebuild_reconciliation).upper()
    for forbidden in ("DROP TABLE", "DROP SCHEMA", "DELETE FROM", "TRUNCATE"):
        assert forbidden not in source
