"""Integration tests for ``dataplat.load.publish.merge.MergePublisher`` (LOAD-09, 04-04 Task 2).

Every positive-path test drives a real ``MergePublisher`` against a real
testcontainers PostgreSQL, migrated to head, publishing hand-built staging
tables (raw SQL, independent of ``dataplat.load.staging.StagingLoader``'s
own implementation -- keeping this task's tests self-contained) into
``normalized.customers``.

The concurrency case PITFALLS C1 names (two overlapping publish attempts
against the same dataset) is deliberately NOT tested here -- 04-RESEARCH.md
assigns it to plan 04-06's integration-test suite; this file's job is only
to prove ``MergePublisher``'s SQL will not need to change for that later
test to pass.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

import psycopg
import pytest

from dataplat.load.publish.merge import MergePublisher
from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.models.identity import RunContext
from dataplat.pipeline.protocol import PipelineContext
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    from collections.abc import Iterator

_STAGING_COLUMNS_DDL = """
    customer_id text, name text, country text, birth_date text, event_ts text,
    _run_id bigint, _file_id bigint, _batch_id bigint,
    _source_row_number bigint, _record_hash bytea, _record_hash_version smallint
"""


def _make_context() -> PipelineContext:
    """A fully placeholder ``PipelineContext`` -- ``MergePublisher.publish()`` uses no field on it.

    Mirrors ``tests/unit/test_pipeline_errors.py``'s ``_make_context()``
    convention -- ``MergePublisher``'s target/columns are hardcoded (see its
    module docstring), so not even ``config`` needs a real value here.
    """
    return PipelineContext(
        run=RunContext(run_id=1, idempotency_key="test-run"),
        config=None,  # type: ignore[arg-type] -- unused by MergePublisher.publish()
        metadata=None,  # type: ignore[arg-type] -- unused by MergePublisher.publish()
        objects=None,  # type: ignore[arg-type] -- unused by MergePublisher.publish()
        db=None,  # type: ignore[arg-type] -- unused by MergePublisher.publish()
        log=None,  # type: ignore[arg-type] -- unused by MergePublisher.publish()
    )


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Insert a synthetic `meta.config_versions` row directly via SQL.

    Mirrors `tests/integration/test_metadata_repository.py`'s helper of the
    same name/shape -- duplicated locally rather than imported, matching
    this test suite's existing per-file helper convention.
    """
    with psycopg.connect(dsn) as conn:
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
                "config_hash": "synthetic-hash-for-test",
                "config_document": json.dumps({"synthetic": True}),
                "config_schema_version": 1,
            },
        ).fetchone()
        assert row is not None
        return int(row[0])


def _seed_run(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
    *,
    key_suffix: str,
) -> tuple[int, int, int]:
    """Create dataset+config_version+file+batch+RUNNING run; return ``(run_id, file_id, batch_id)``.

    ``normalized.customers._run_id``/``_file_id``/``_batch_id`` are real
    foreign keys (migration 0005) -- unlike the staging table, which carries
    none -- so publish tests need real, FK-satisfying rows to publish
    against.
    """
    dataset_id = repository.get_or_create_dataset(f"merge_test_{key_suffix}")
    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id)
    file_id = repository.create_file(
        dataset_id=dataset_id,
        object_uri=f"s3://raw/customers/{key_suffix}.csv",
        content_sha256=hashlib.sha256(key_suffix.encode()).digest(),
        hash_version=1,
        size_bytes=10,
        filename=f"{key_suffix}.csv",
        status="DISCOVERED",
    )
    batch_id = repository.create_batch(
        dataset_id=dataset_id,
        batch_key=f"{key_suffix}:2026-08-13:1",
        status="OPEN",
    )
    run_id = repository.create_ingestion_run(
        idempotency_key=f"{key_suffix}:1",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        status="RUNNING",
        file_id=file_id,
        batch_id=batch_id,
    )
    return run_id, file_id, batch_id


def _create_staging_table(conn: psycopg.Connection[Any], table_name: str) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(f"CREATE UNLOGGED TABLE {table_name} ({_STAGING_COLUMNS_DDL})")


def _insert_staging_row(  # noqa: PLR0913 -- one keyword per staging column, mirrors staging.py's shape
    conn: psycopg.Connection[Any],
    table_name: str,
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
        f"""
        INSERT INTO {table_name} (
            customer_id, name, country, birth_date, event_ts,
            _run_id, _file_id, _batch_id, _source_row_number,
            _record_hash, _record_hash_version
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
        """,  # noqa: S608 -- test-controlled identifier only; every value crosses via %s
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


@pytest.fixture
def repository(migrated_dsn: str) -> Iterator[PostgresMetadataRepository]:
    """A `PostgresMetadataRepository` backed by an opened pool over the migrated database."""
    pool = create_pool(migrated_dsn)
    pool.open(wait=True)
    try:
        yield PostgresMetadataRepository(pool)
    finally:
        pool.close()


def test_publish_inserts_distinct_customer_rows(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="distinct")
    staging_table = "staging.merge_test_distinct"

    with psycopg.connect(migrated_dsn) as conn:
        _create_staging_table(conn, staging_table)
        for i in range(3):
            _insert_staging_row(
                conn,
                staging_table,
                customer_id=str(2000 + i),
                name=f"Name{i}",
                country="US",
                birth_date="1990-01-01",
                event_ts="2026-08-13T10:00:00+00:00",
                run_id=run_id,
                file_id=file_id,
                batch_id=batch_id,
                source_row_number=i + 1,
                record_hash=hashlib.sha256(f"row-{i}".encode()).digest(),
            )
        result = MergePublisher().publish(_make_context(), staging_table, conn)
        conn.commit()

    assert result.outcome == "PUBLISHED"
    assert result.rows_affected == 3

    with psycopg.connect(migrated_dsn) as verify_conn:
        count = verify_conn.execute(
            "SELECT COUNT(*) FROM normalized.customers WHERE _run_id = %s",
            (run_id,),
        ).fetchone()
    assert count is not None
    assert count[0] == 3


def test_publish_deduplicates_same_customer_id_keeping_the_latest_event_ts(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="dedup")
    staging_table = "staging.merge_test_dedup"
    customer_id = "3001"

    with psycopg.connect(migrated_dsn) as conn:
        _create_staging_table(conn, staging_table)
        _insert_staging_row(
            conn,
            staging_table,
            customer_id=customer_id,
            name="Older",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-01-01T00:00:00+00:00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"older").digest(),
        )
        _insert_staging_row(
            conn,
            staging_table,
            customer_id=customer_id,
            name="Newer",
            country="CA",
            birth_date="1990-01-01",
            event_ts="2026-06-01T00:00:00+00:00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=2,
            record_hash=hashlib.sha256(b"newer").digest(),
        )

        # Must not raise "ON CONFLICT DO UPDATE command cannot affect row a
        # second time" (PITFALLS C1) -- the DISTINCT ON in MergePublisher's
        # own SQL is what prevents that.
        result = MergePublisher().publish(_make_context(), staging_table, conn)
        conn.commit()

    assert result.rows_affected == 1

    with psycopg.connect(migrated_dsn) as verify_conn:
        row = verify_conn.execute(
            "SELECT name, country FROM normalized.customers WHERE customer_id = %s",
            (int(customer_id),),
        ).fetchone()
    assert row is not None
    assert row[0] == "Newer"
    assert row[1] == "CA"


def test_republishing_identical_content_is_a_no_op(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="noop_republish")
    customer_id = "4001"
    record_hash = hashlib.sha256(b"identical-content").digest()

    with psycopg.connect(migrated_dsn) as conn:
        first_table = "staging.merge_test_noop_republish_1"
        _create_staging_table(conn, first_table)
        _insert_staging_row(
            conn,
            first_table,
            customer_id=customer_id,
            name="Same",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-08-13T10:00:00+00:00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=record_hash,
        )
        first_result = MergePublisher().publish(_make_context(), first_table, conn)
        conn.commit()
    assert first_result.rows_affected == 1

    with psycopg.connect(migrated_dsn) as conn:
        second_table = "staging.merge_test_noop_republish_2"
        _create_staging_table(conn, second_table)
        _insert_staging_row(
            conn,
            second_table,
            customer_id=customer_id,
            name="Same",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-08-13T10:00:00+00:00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=record_hash,  # identical hash -- the WHERE guard must suppress this write
        )
        second_result = MergePublisher().publish(_make_context(), second_table, conn)
        conn.commit()

    assert second_result.rows_affected == 0

    with psycopg.connect(migrated_dsn) as verify_conn:
        count = verify_conn.execute(
            "SELECT COUNT(*) FROM normalized.customers WHERE customer_id = %s",
            (int(customer_id),),
        ).fetchone()
    assert count is not None
    assert count[0] == 1


def test_older_event_ts_never_clobbers_a_newer_stored_row(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="no_clobber")
    customer_id = "5001"

    with psycopg.connect(migrated_dsn) as conn:
        first_table = "staging.merge_test_no_clobber_1"
        _create_staging_table(conn, first_table)
        _insert_staging_row(
            conn,
            first_table,
            customer_id=customer_id,
            name="Newer",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-06-01T00:00:00+00:00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"newer-content").digest(),
        )
        MergePublisher().publish(_make_context(), first_table, conn)
        conn.commit()

    with psycopg.connect(migrated_dsn) as conn:
        second_table = "staging.merge_test_no_clobber_2"
        _create_staging_table(conn, second_table)
        _insert_staging_row(
            conn,
            second_table,
            customer_id=customer_id,
            name="StaleLateArrival",
            country="ZZ",
            birth_date="1990-01-01",
            event_ts="2026-01-01T00:00:00+00:00",  # OLDER than the already-stored row
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"older-content").digest(),
        )
        result = MergePublisher().publish(_make_context(), second_table, conn)
        conn.commit()

    assert result.rows_affected == 0  # the event_ts guard suppressed the write entirely

    with psycopg.connect(migrated_dsn) as verify_conn:
        row = verify_conn.execute(
            "SELECT name, country FROM normalized.customers WHERE customer_id = %s",
            (int(customer_id),),
        ).fetchone()
    assert row is not None
    assert row[0] == "Newer"
    assert row[1] == "US"


def test_on_conflict_fails_without_the_unique_constraint_migration_0006_adds(
    migrated_dsn: str,
) -> None:
    """Negative case: proves migration 0006's UNIQUE constraint is load-bearing, not decorative.

    Temporarily drops ``uq_customers_customer_id`` (reproducing exactly the
    pre-migration-0006 state migration 0005's own docstring describes) on
    the shared, already-fully-migrated database, asserts the documented
    PostgreSQL failure, then restores the constraint in a ``finally`` block
    -- cheaper than provisioning a second dedicated container for one
    negative assertion, and every other test in this session still sees a
    fully-migrated schema. Safe because ``tests/integration/`` runs
    sequentially, never under ``-n auto`` (pyproject.toml's own
    xdist guidance): no other test can observe the dropped-constraint
    window.
    """
    staging_table = "staging.merge_test_premigration_check"
    with psycopg.connect(migrated_dsn) as conn:
        conn.execute("ALTER TABLE normalized.customers DROP CONSTRAINT uq_customers_customer_id")
        conn.commit()

        try:
            _create_staging_table(conn, staging_table)
            conn.commit()

            with pytest.raises(
                psycopg.errors.ProgrammingError,
                match="no unique or exclusion constraint",
            ):
                MergePublisher().publish(_make_context(), staging_table, conn)
            conn.rollback()
        finally:
            conn.execute(
                "ALTER TABLE normalized.customers "
                "ADD CONSTRAINT uq_customers_customer_id UNIQUE (customer_id)",
            )
            conn.commit()
