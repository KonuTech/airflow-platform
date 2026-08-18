"""Integration tests proving DEDUP-01/DEDUP-03 against a real `dbt build`.

Seeds `staging.customers`/`staging.orders` (migration 0022's durable bronze
tables, migrated_dsn) with a within-file duplicate business key (two rows,
same key, different `_record_hash`, same `_run_id`/`_batch_id`) plus a
cross-batch duplicate (same key, different `_run_id`, different `event_ts`)
for BOTH datasets, then runs a real `dbt build --project-dir dbt
--profiles-dir dbt` via `subprocess.run` (the shared `run_dbt_build`
fixture) against the testcontainers DSN, and asserts `silver.customers`/
`silver.orders` each contain exactly one row per business key with the
correct (latest `event_ts`/`order_date`) winning values.

Mirrors `test_publish_merge.py`'s own hand-built-fixture convention (raw
`psycopg` INSERTs, never `StagingLoader`/the `ingest` CLI) — these tests are
scoped purely to dbt's own behavior over known-good bronze content.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
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
    """Return the dataset's single CURRENT config_version_id, creating one if none exists yet.

    `meta.config_versions` has a partial UNIQUE index allowing at most one
    `valid_to IS NULL` ("current") row per `dataset_id` (migration 0001) —
    unlike `test_publish_merge.py`'s own `_insert_config_version` (which
    always INSERTs a fresh row because every test there uses its OWN,
    never-reused dataset), these dbt tests seed MULTIPLE ingestion runs
    against the SAME dataset (to prove cross-run/incremental behavior), so
    this helper reuses the existing current version instead of colliding
    with it on a second call.
    """
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
    """Create dataset (or reuse)+config_version+file+batch+RUNNING run.

    Returns:
        `(dataset_id, run_id, file_id, batch_id)` -- `staging.customers`/
        `staging.orders`' six lineage columns (migration 0022) are real
        foreign keys, so every seeded bronze row needs FK-satisfying values.
    """
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


def _insert_bronze_customer(  # noqa: PLR0913 -- one keyword per staging column, mirrors staging.py's own shape
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


def _insert_bronze_order(  # noqa: PLR0913 -- one keyword per staging column
    conn: psycopg.Connection,
    *,
    order_id: str,
    customer_id: str,
    order_date: str,
    amount: str,
    run_id: int,
    file_id: int,
    batch_id: int,
    source_row_number: int,
    record_hash: bytes,
) -> None:
    conn.execute(
        """
        INSERT INTO staging.orders (
            order_id, customer_id, order_date, amount,
            _run_id, _file_id, _batch_id, _source_row_number,
            _record_hash, _record_hash_version
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
        """,
        (
            order_id,
            customer_id,
            order_date,
            amount,
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


def test_dbt_silver_models_never_use_select_distinct() -> None:
    """DEDUP-03's literal requirement: the dbt models are never a bare `SELECT DISTINCT`."""
    repo_root = Path(__file__).resolve().parents[2]
    for model_name in ("silver_customers", "silver_orders"):
        model_sql = (repo_root / "dbt" / "models" / "silver" / f"{model_name}.sql").read_text(
            encoding="utf-8"
        )
        assert "SELECT DISTINCT" not in model_sql, (
            f"{model_name}.sql must not use SELECT DISTINCT (DEDUP-03)"
        )


def test_silver_customers_deduplicates_within_file_and_cross_batch(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
    run_dbt_build: Callable[..., object],
) -> None:
    """Within-file AND cross-batch duplicate `customer_id` rows collapse to one winner in silver."""
    _dataset_id, run1_id, file1_id, batch1_id = _seed_ingestion_run(
        repository,
        migrated_dsn,
        dataset_name="customers",
        key_suffix="dedup-customers",
        run_number=1,
    )
    _dataset_id, run2_id, file2_id, batch2_id = _seed_ingestion_run(
        repository,
        migrated_dsn,
        dataset_name="customers",
        key_suffix="dedup-customers",
        run_number=2,
    )

    with psycopg.connect(migrated_dsn, autocommit=True) as conn:
        # Within-file duplicate: same _run_id/_batch_id, two rows, same key.
        _insert_bronze_customer(
            conn,
            customer_id="D1",
            name="Older",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-02-01T00:00:00+00:00",
            run_id=run1_id,
            file_id=file1_id,
            batch_id=batch1_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"d1-older").digest(),
        )
        _insert_bronze_customer(
            conn,
            customer_id="D1",
            name="Newer",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-02-02T00:00:00+00:00",
            run_id=run1_id,
            file_id=file1_id,
            batch_id=batch1_id,
            source_row_number=2,
            record_hash=hashlib.sha256(b"d1-newer").digest(),
        )
        # Cross-batch duplicate: same key, different _run_id/_batch_id/event_ts.
        _insert_bronze_customer(
            conn,
            customer_id="D1",
            name="Newest",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-02-03T00:00:00+00:00",
            run_id=run2_id,
            file_id=file2_id,
            batch_id=batch2_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"d1-newest").digest(),
        )

    run_dbt_build(migrated_dsn, select="silver_customers")

    with psycopg.connect(migrated_dsn) as verify_conn:
        rows = verify_conn.execute(
            "SELECT name, event_ts FROM silver.customers WHERE customer_id = %s",
            ("D1",),
        ).fetchall()
    assert len(rows) == 1, f"expected exactly one silver row per business key, got {rows}"
    assert rows[0] == ("Newest", "2026-02-03T00:00:00+00:00")


def test_silver_orders_deduplicates_within_file_and_cross_batch(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
    run_dbt_build: Callable[..., object],
) -> None:
    """Within-file AND cross-batch duplicate `order_id` rows collapse to one winner in silver."""
    _dataset_id, run1_id, file1_id, batch1_id = _seed_ingestion_run(
        repository,
        migrated_dsn,
        dataset_name="orders",
        key_suffix="dedup-orders",
        run_number=1,
    )
    _dataset_id, run2_id, file2_id, batch2_id = _seed_ingestion_run(
        repository,
        migrated_dsn,
        dataset_name="orders",
        key_suffix="dedup-orders",
        run_number=2,
    )

    with psycopg.connect(migrated_dsn, autocommit=True) as conn:
        _insert_bronze_order(
            conn,
            order_id="OD1",
            customer_id="D1",
            order_date="2026-02-01",
            amount="10.00",
            run_id=run1_id,
            file_id=file1_id,
            batch_id=batch1_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"od1-older").digest(),
        )
        _insert_bronze_order(
            conn,
            order_id="OD1",
            customer_id="D1",
            order_date="2026-02-02",
            amount="15.00",
            run_id=run1_id,
            file_id=file1_id,
            batch_id=batch1_id,
            source_row_number=2,
            record_hash=hashlib.sha256(b"od1-newer").digest(),
        )
        _insert_bronze_order(
            conn,
            order_id="OD1",
            customer_id="D1",
            order_date="2026-02-03",
            amount="20.00",
            run_id=run2_id,
            file_id=file2_id,
            batch_id=batch2_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"od1-newest").digest(),
        )

    run_dbt_build(migrated_dsn, select="silver_orders")

    with psycopg.connect(migrated_dsn) as verify_conn:
        rows = verify_conn.execute(
            "SELECT amount, order_date FROM silver.orders WHERE order_id = %s",
            ("OD1",),
        ).fetchall()
    assert len(rows) == 1, f"expected exactly one silver row per business key, got {rows}"
    assert rows[0] == ("20.00", "2026-02-03")
