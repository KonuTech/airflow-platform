"""Integration tests for ``dataplat.load.publish.merge_orders.OrdersMergePublisher`` (08-05 Task 2).

Mirrors ``tests/integration/test_publish_merge.py``'s structure exactly:
real ``OrdersMergePublisher`` against a real testcontainers PostgreSQL,
migrated to head, publishing hand-built staging tables (raw SQL, independent
of ``dataplat.load.staging.StagingLoader``'s own implementation -- keeping
this test self-contained) into ``normalized.orders``.

This is the end-to-end proof this plan's ``must_haves.truths`` names: a
second real dataset flows through the identical publish machinery
``customers`` already uses, no bypass -- and re-publishing the identical
staged row a second time is a no-op, matching customers' own D-1 idempotency
guarantee (LOAD-01/02/03) extended to ``orders``.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

import psycopg
import pytest

from dataplat.load.publish.merge_orders import OrdersMergePublisher
from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.models.identity import RunContext
from dataplat.pipeline.protocol import PipelineContext
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    from collections.abc import Iterator

_STAGING_COLUMNS_DDL = """
    order_id text, customer_id text, order_date text, amount text,
    _run_id bigint, _file_id bigint, _batch_id bigint,
    _source_row_number bigint, _record_hash bytea, _record_hash_version smallint
"""


def _make_context() -> PipelineContext:
    """A fully placeholder ``PipelineContext`` -- ``OrdersMergePublisher.publish()`` uses no field.

    Mirrors ``test_publish_merge.py``'s ``_make_context()`` convention --
    ``OrdersMergePublisher``'s target/columns are hardcoded (see its module
    docstring), so not even ``config`` needs a real value here.
    """
    return PipelineContext(
        run=RunContext(run_id=1, idempotency_key="test-run"),
        config=None,  # type: ignore[arg-type] -- unused by OrdersMergePublisher.publish()
        metadata=None,  # type: ignore[arg-type] -- unused by OrdersMergePublisher.publish()
        objects=None,  # type: ignore[arg-type] -- unused by OrdersMergePublisher.publish()
        db=None,  # type: ignore[arg-type] -- unused by OrdersMergePublisher.publish()
        log=None,  # type: ignore[arg-type] -- unused by OrdersMergePublisher.publish()
    )


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Insert a synthetic `meta.config_versions` row directly via SQL.

    Duplicated locally rather than imported, matching this test suite's
    existing per-file helper convention (`test_publish_merge.py`'s own
    `_insert_config_version`).
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

    ``normalized.orders._run_id``/``_file_id``/``_batch_id`` are real
    foreign keys (migration 0016) -- unlike the staging table, which carries
    none -- so publish tests need real, FK-satisfying rows to publish
    against. Mirrors `test_publish_merge.py`'s own `_seed_run`.
    """
    dataset_id = repository.get_or_create_dataset(f"orders_test_{key_suffix}")
    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id)
    file_id = repository.create_file(
        dataset_id=dataset_id,
        object_uri=f"s3://raw/orders/{key_suffix}.csv",
        content_sha256=hashlib.sha256(key_suffix.encode()).digest(),
        hash_version=1,
        size_bytes=10,
        filename=f"{key_suffix}.csv",
        status="DISCOVERED",
    )
    batch_id = repository.create_batch(
        dataset_id=dataset_id,
        batch_key=f"{key_suffix}:2026-08-17:1",
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
        f"""
        INSERT INTO {table_name} (
            order_id, customer_id, order_date, amount,
            _run_id, _file_id, _batch_id, _source_row_number,
            _record_hash, _record_hash_version
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
        """,  # noqa: S608 -- test-controlled identifier only; every value crosses via %s
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


@pytest.mark.integration
def test_publish_dedups_same_order_id_keeping_the_later_order_date(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    """Two staged rows sharing one `order_id` (duplicate business key within one batch) -> 1 row.

    The `DISTINCT ON (order_id)` guard (mirroring `MergePublisher`'s own C1
    reasoning) collapses the duplicate, keeping the later `order_date`'s
    values -- matching this plan's own `must_haves.truths` behavior spec.
    """
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="dedup")
    staging_table = "staging.orders_test_dedup"
    order_id = "9001"

    with psycopg.connect(migrated_dsn) as conn:
        _create_staging_table(conn, staging_table)
        _insert_staging_row(
            conn,
            staging_table,
            order_id=order_id,
            customer_id="5001",
            order_date="2026-01-01",
            amount="10.00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"older-order").digest(),
        )
        _insert_staging_row(
            conn,
            staging_table,
            order_id=order_id,
            customer_id="5002",
            order_date="2026-06-01",
            amount="25.50",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=2,
            record_hash=hashlib.sha256(b"newer-order").digest(),
        )

        # Must not raise "ON CONFLICT DO UPDATE command cannot affect row a
        # second time" -- the DISTINCT ON in OrdersMergePublisher's own SQL
        # is what prevents that.
        result = OrdersMergePublisher().publish(_make_context(), staging_table, conn)
        conn.commit()

    assert result.outcome == "PUBLISHED"
    assert result.rows_affected == 1

    with psycopg.connect(migrated_dsn) as verify_conn:
        row = verify_conn.execute(
            "SELECT customer_id, order_date, amount FROM normalized.orders WHERE order_id = %s",
            (int(order_id),),
        ).fetchone()
    assert row is not None
    assert row[0] == 5002
    assert row[1].isoformat() == "2026-06-01"
    assert str(row[2]) == "25.50"


@pytest.mark.integration
def test_republishing_the_identical_staged_row_is_a_no_op(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    """D-1's idempotency guarantee extended to a second dataset: re-publish -> `rows_affected == 0`.

    Matches `test_publish_merge.py`'s own
    `test_republishing_identical_content_is_a_no_op` shape exactly, proving
    the same `WHERE ... IS DISTINCT FROM` no-op-update guard now protects
    `normalized.orders` too.
    """
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="noop_republish")
    order_id = "9101"
    record_hash = hashlib.sha256(b"identical-order-content").digest()

    with psycopg.connect(migrated_dsn) as conn:
        first_table = "staging.orders_test_noop_republish_1"
        _create_staging_table(conn, first_table)
        _insert_staging_row(
            conn,
            first_table,
            order_id=order_id,
            customer_id="5003",
            order_date="2026-03-01",
            amount="99.99",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=record_hash,
        )
        first_result = OrdersMergePublisher().publish(_make_context(), first_table, conn)
        conn.commit()
    assert first_result.rows_affected == 1

    with psycopg.connect(migrated_dsn) as conn:
        second_table = "staging.orders_test_noop_republish_2"
        _create_staging_table(conn, second_table)
        _insert_staging_row(
            conn,
            second_table,
            order_id=order_id,
            customer_id="5003",
            order_date="2026-03-01",
            amount="99.99",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=record_hash,  # identical hash -- the WHERE guard must suppress this write
        )
        second_result = OrdersMergePublisher().publish(_make_context(), second_table, conn)
        conn.commit()

    assert second_result.rows_affected == 0

    with psycopg.connect(migrated_dsn) as verify_conn:
        count = verify_conn.execute(
            "SELECT COUNT(*) FROM normalized.orders WHERE order_id = %s",
            (int(order_id),),
        ).fetchone()
    assert count is not None
    assert count[0] == 1
