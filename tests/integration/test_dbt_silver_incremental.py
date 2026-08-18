"""Integration tests proving DEDUP-02/INCR-03/INCR-04/QUAL-10 against a real `dbt build`.

Seeds one batch, runs `dbt build`, asserts row counts; seeds a SECOND batch
(new `_run_id`) including one row that supersedes an existing silver row and
one genuinely new business key; runs `dbt build` again; asserts silver
reflects both changes; a third, no-op `dbt build` run leaves row counts
unchanged.

Also proves the must-have this whole plan exists for: a late-arriving row
with an OLD `event_ts` but a NEW `_run_id` is picked up by `is_incremental()`
(INCR-03/04) and correctly LOSES against an already-silver-resident row with
a LATER `event_ts` -- never overwriting it.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

import psycopg
import pytest

from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

pytestmark = [pytest.mark.dbt, pytest.mark.integration]


def _get_or_create_config_version(
    conn: psycopg.Connection, *, dataset_id: int, key_suffix: str
) -> int:
    """See `test_dbt_silver_dedup.py`'s identical helper for the full rationale."""
    existing = conn.execute(
        "SELECT config_version_id FROM meta.config_versions "
        "WHERE dataset_id = %s AND valid_to IS NULL",
        (dataset_id,),
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
                SELECT COALESCE(MAX(version), 0) + 1 FROM meta.config_versions
                WHERE dataset_id = %(dataset_id)s
            ),
            %(config_hash)s, %(config_document)s::jsonb, %(config_schema_version)s, now()
        )
        RETURNING config_version_id
        """,
        {
            "dataset_id": dataset_id,
            "config_hash": f"dbt-test-hash-{key_suffix}",
            "config_document": '{"synthetic": true}',
            "config_schema_version": 1,
        },
    ).fetchone()
    assert row is not None
    return int(row[0])


def _seed_ingestion_run(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
    *,
    dataset_name: str,
    key_suffix: str,
    run_number: int,
) -> tuple[int, int, int, int]:
    """See `test_dbt_silver_dedup.py`'s identical helper for the full rationale."""
    dataset_id = repository.get_or_create_dataset(dataset_name)
    with psycopg.connect(migrated_dsn, autocommit=True) as conn:
        config_version_id = _get_or_create_config_version(
            conn, dataset_id=dataset_id, key_suffix=key_suffix
        )
    file_id = repository.create_file(
        dataset_id=dataset_id,
        object_uri=f"s3://raw/{dataset_name}/{key_suffix}-{run_number}.csv",
        content_sha256=hashlib.sha256(f"{key_suffix}-{run_number}".encode()).digest(),
        hash_version=1,
        size_bytes=10,
        filename=f"{key_suffix}-{run_number}.csv",
        status="DISCOVERED",
    )
    batch_id = repository.create_batch(
        dataset_id=dataset_id,
        batch_key=f"{key_suffix}:{run_number}",
        status="OPEN",
    )
    run_id = repository.create_ingestion_run(
        idempotency_key=f"{key_suffix}:{run_number}",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        status="RUNNING",
        file_id=file_id,
        batch_id=batch_id,
    )
    return dataset_id, run_id, file_id, batch_id


def _insert_bronze_customer(  # noqa: PLR0913 -- one keyword per staging column
    conn: psycopg.Connection,
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
        INSERT INTO staging.customers (
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


@pytest.fixture
def repository(migrated_dsn: str) -> Iterator[PostgresMetadataRepository]:
    """A `PostgresMetadataRepository` backed by an opened pool over the migrated database."""
    pool = create_pool(migrated_dsn)
    pool.open(wait=True)
    try:
        yield PostgresMetadataRepository(pool)
    finally:
        pool.close()


def test_incremental_run_picks_up_new_batch_and_a_third_run_is_a_no_op(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
    run_dbt_build: Callable[..., object],
) -> None:
    """DEDUP-02/INCR-03/04/QUAL-10: first load, incremental delta, then a no-op rebuild."""
    _dataset_id, run1_id, file1_id, batch1_id = _seed_ingestion_run(
        repository,
        migrated_dsn,
        dataset_name="customers",
        key_suffix="incr",
        run_number=1,
    )

    with psycopg.connect(migrated_dsn, autocommit=True) as conn:
        _insert_bronze_customer(
            conn,
            customer_id="I1",
            name="Ivy",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-03-02T00:00:00+00:00",
            run_id=run1_id,
            file_id=file1_id,
            batch_id=batch1_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"i1-run1").digest(),
        )
        _insert_bronze_customer(
            conn,
            customer_id="I2",
            name="Ian",
            country="GB",
            birth_date="1985-05-05",
            event_ts="2026-03-01T00:00:00+00:00",
            run_id=run1_id,
            file_id=file1_id,
            batch_id=batch1_id,
            source_row_number=2,
            record_hash=hashlib.sha256(b"i2-run1").digest(),
        )

    run_dbt_build(migrated_dsn, select="silver_customers")

    with psycopg.connect(migrated_dsn) as verify_conn:
        count_after_run1 = verify_conn.execute(
            "SELECT count(*) FROM silver.customers WHERE customer_id IN ('I1', 'I2')",
        ).fetchone()
    assert count_after_run1 is not None
    assert count_after_run1[0] == 2

    # Second batch: I1 gets a LATE-ARRIVING, OLDER event_ts row (a new
    # _run_id, but a business-stale event_ts) -- must LOSE against the
    # already-silver-resident I1 row from run 1 (D-06's must-have). I3 is
    # a genuinely new business key -- must be accepted normally.
    _dataset_id, run2_id, file2_id, batch2_id = _seed_ingestion_run(
        repository,
        migrated_dsn,
        dataset_name="customers",
        key_suffix="incr",
        run_number=2,
    )
    with psycopg.connect(migrated_dsn, autocommit=True) as conn:
        _insert_bronze_customer(
            conn,
            customer_id="I1",
            name="Ivy-stale-correction",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-01-01T00:00:00+00:00",  # OLDER than run 1's 2026-03-02
            run_id=run2_id,
            file_id=file2_id,
            batch_id=batch2_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"i1-run2-stale").digest(),
        )
        _insert_bronze_customer(
            conn,
            customer_id="I3",
            name="Iris",
            country="FR",
            birth_date="1975-03-03",
            event_ts="2026-03-03T00:00:00+00:00",
            run_id=run2_id,
            file_id=file2_id,
            batch_id=batch2_id,
            source_row_number=2,
            record_hash=hashlib.sha256(b"i3-run2").digest(),
        )

    run_dbt_build(migrated_dsn, select="silver_customers")

    with psycopg.connect(migrated_dsn) as verify_conn:
        i1_row = verify_conn.execute(
            "SELECT name, event_ts, _run_id FROM silver.customers WHERE customer_id = %s",
            ("I1",),
        ).fetchone()
        i3_row = verify_conn.execute(
            "SELECT name FROM silver.customers WHERE customer_id = %s",
            ("I3",),
        ).fetchone()
        count_after_run2 = verify_conn.execute(
            "SELECT count(*) FROM silver.customers WHERE customer_id IN ('I1', 'I2', 'I3')",
        ).fetchone()

    assert i1_row is not None
    # The late-arriving, business-stale row must NEVER have overwritten I1.
    assert i1_row == ("Ivy", "2026-03-02T00:00:00+00:00", run1_id)
    assert i3_row is not None
    assert i3_row[0] == "Iris"
    assert count_after_run2 is not None
    assert count_after_run2[0] == 3  # I1, I2 unchanged; I3 newly accepted

    # A third, no-op dbt build (nothing new in bronze) must leave counts
    # unchanged -- QUAL-10's "tested across batches" / idempotency claim.
    run_dbt_build(migrated_dsn, select="silver_customers")

    with psycopg.connect(migrated_dsn) as verify_conn:
        count_after_noop = verify_conn.execute(
            "SELECT count(*) FROM silver.customers WHERE customer_id IN ('I1', 'I2', 'I3')",
        ).fetchone()
    assert count_after_noop is not None
    assert count_after_noop[0] == 3
