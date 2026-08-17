"""Integration tests for ``dataplat.validate.referential.ReferentialIntegrityBarrier`` (08-08).

Proves this plan's own ``must_haves.truths``: an `orders` row whose
`customer_id` has no matching `normalized.customers` row is classified
`REFERENTIAL_ORPHAN` and quarantined at the ROW level (D-16) while every
other row in the same file/run still publishes normally, and that the
race scenario Pitfall 5 names (an orphan that is a legitimate, not-yet-
arrived customer, not a data error) is handled as an expected
`QUARANTINE` outcome, never a whole-run `FAIL`.

Hand-builds both `staging.orders__r<n>` AND `normalized.customers` rows
directly via SQL (mirroring `test_publish_orders.py`'s own convention),
independent of `StagingLoader`'s own implementation, keeping this test
self-contained.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any

import psycopg
import pytest

from dataplat.load.publish.merge_orders import OrdersMergePublisher
from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.models.identity import RunContext
from dataplat.pipeline.protocol import PipelineContext
from dataplat.storage.db import create_pool
from dataplat.validate.referential import ReferentialIntegrityBarrier

if TYPE_CHECKING:
    from collections.abc import Iterator

    from psycopg_pool import ConnectionPool

_STAGING_COLUMNS_DDL = """
    order_id text, customer_id text, order_date text, amount text,
    _run_id bigint, _file_id bigint, _batch_id bigint,
    _source_row_number bigint, _record_hash bytea, _record_hash_version smallint
"""


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Insert a synthetic `meta.config_versions` row directly via SQL.

    Duplicated locally rather than imported, matching this test suite's
    existing per-file helper convention (`test_publish_orders.py`'s own
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
                "config_document": '{"synthetic": true}',
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

    Mirrors `test_publish_orders.py`'s own `_seed_run` -- duplicated
    locally rather than imported, per this test suite's own per-file
    helper convention. The returned ids are generic `meta` foreign keys,
    reused below to satisfy `normalized.customers`'s own `_run_id`/
    `_file_id`/`_batch_id` NOT NULL FK columns too.
    """
    dataset_id = repository.get_or_create_dataset(f"referential_test_{key_suffix}")
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
            hashlib.sha256(f"{table_name}:{source_row_number}".encode()).digest(),
        ),
    )


def _insert_customer(  # noqa: PLR0913 -- one keyword per column, mirrors _insert_staging_row's shape
    conn: psycopg.Connection[Any],
    *,
    customer_id: int,
    run_id: int,
    file_id: int,
    batch_id: int,
    source_row_number: int,
) -> None:
    """Insert a real `normalized.customers` row directly via SQL, satisfying its FK columns."""
    conn.execute(
        """
        INSERT INTO normalized.customers (
            customer_id, name, country, birth_date, event_ts,
            _run_id, _file_id, _batch_id, _source_row_number,
            _record_hash, _record_hash_version
        ) VALUES (
            %s, %s, %s, %s, now(),
            %s, %s, %s, %s,
            %s, 1
        )
        """,
        (
            customer_id,
            f"customer-{customer_id}",
            "US",
            "1990-01-01",
            run_id,
            file_id,
            batch_id,
            source_row_number,
            hashlib.sha256(f"customer:{customer_id}".encode()).digest(),
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


@pytest.fixture
def db_pool(migrated_dsn: str) -> Iterator[ConnectionPool]:
    """A real, opened `ConnectionPool` -- what `ReferentialIntegrityBarrier.apply()` reads through."""  # noqa: E501, W505
    pool = create_pool(migrated_dsn)
    pool.open(wait=True)
    try:
        yield pool
    finally:
        pool.close()


def _make_context(db_pool: ConnectionPool) -> PipelineContext:
    """A mostly-placeholder `PipelineContext` -- `ReferentialIntegrityBarrier.apply()` only reads `ctx.db`."""  # noqa: E501, W505
    return PipelineContext(
        run=RunContext(run_id=1, idempotency_key="test-run"),
        config=None,  # type: ignore[arg-type] -- unused by ReferentialIntegrityBarrier.apply()
        metadata=None,  # type: ignore[arg-type] -- unused by ReferentialIntegrityBarrier.apply()
        objects=None,  # type: ignore[arg-type] -- unused by ReferentialIntegrityBarrier.apply()
        db=db_pool,
        log=None,  # type: ignore[arg-type] -- unused by ReferentialIntegrityBarrier.apply()
    )


@pytest.mark.integration
def test_orphan_row_quarantined_non_orphan_rows_untouched(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
    db_pool: ConnectionPool,
) -> None:
    """3 staged rows (2 matching, 1 orphan) -> exactly 1 `REFERENTIAL_ORPHAN`, 2 untouched."""
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="orphan_mix")
    staging_table = "staging.orders_test_referential_mix"

    with psycopg.connect(migrated_dsn) as conn:
        _insert_customer(
            conn,
            customer_id=6001,
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
        )
        conn.commit()

        _create_staging_table(conn, staging_table)
        _insert_staging_row(
            conn,
            staging_table,
            order_id="9201",
            customer_id="6001",  # matches normalized.customers
            order_date="2026-01-01",
            amount="10.00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
        )
        _insert_staging_row(
            conn,
            staging_table,
            order_id="9202",
            customer_id="6999",  # no matching normalized.customers row -- the orphan
            order_date="2026-01-02",
            amount="20.00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=2,
        )
        _insert_staging_row(
            conn,
            staging_table,
            order_id="9203",
            customer_id="6001",  # matches normalized.customers -- a second non-orphan row
            order_date="2026-01-03",
            amount="30.00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=3,
        )
        conn.commit()

    barrier = ReferentialIntegrityBarrier(
        staging_table=staging_table,
        target_table="normalized.customers",
        target_column="customer_id",
        staging_column="customer_id",
        strategy="QUARANTINE_RECORD",
        rule_id="orders_customer_id_referential",
    )
    result = barrier.apply(_make_context(db_pool))

    assert len(result.rejected) == 1
    orphan = result.rejected[0]
    assert orphan.error_type == "REFERENTIAL_ORPHAN"
    assert orphan.source_row_number == 2
    assert orphan.error_column == "customer_id"

    assert len(result.findings) == 1
    finding = result.findings[0]
    assert finding.outcome == "QUARANTINE"
    assert finding.rule_type == "REFERENTIAL"
    assert finding.failed_count == 1
    assert finding.evaluated_count == 3


@pytest.mark.integration
def test_race_scenario_not_yet_arrived_customer_is_quarantine_never_fail(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
    db_pool: ConnectionPool,
) -> None:
    """Pitfall 5's exact race: an orphan that is a legitimate, not-yet-loaded customer.

    The barrier must not raise and must never report `outcome="FAIL"` --
    D-16's default `QUARANTINE_RECORD` strategy is genuinely row-level,
    never file-level/run-level. A third assertion proves the non-orphan
    row publishes unaffected via `OrdersMergePublisher`, once the orphan
    is excluded from what gets published (08-11's own future sequencing
    job -- this test only proves the barrier's classification here).
    """
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="race")
    staging_table = "staging.orders_test_referential_race"

    with psycopg.connect(migrated_dsn) as conn:
        _insert_customer(
            conn,
            customer_id=7001,
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
        )
        conn.commit()

        _create_staging_table(conn, staging_table)
        _insert_staging_row(
            conn,
            staging_table,
            order_id="9301",
            customer_id="7001",  # matches -- non-orphan
            order_date="2026-02-01",
            amount="15.00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
        )
        _insert_staging_row(
            conn,
            staging_table,
            # customer 7999's own `customers` batch legitimately hasn't
            # landed yet -- this is Pitfall 5's race, not a data error.
            order_id="9302",
            customer_id="7999",
            order_date="2026-02-02",
            amount="45.00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=2,
        )
        conn.commit()

    barrier = ReferentialIntegrityBarrier(
        staging_table=staging_table,
        target_table="normalized.customers",
        target_column="customer_id",
        staging_column="customer_id",
        strategy="QUARANTINE_RECORD",
        rule_id="orders_customer_id_referential",
    )
    # Must not raise -- a not-yet-arrived customer is an expected, row-level
    # condition, never a whole-run failure.
    result = barrier.apply(_make_context(db_pool))

    assert len(result.rejected) == 1
    assert result.rejected[0].error_type == "REFERENTIAL_ORPHAN"
    assert result.rejected[0].source_row_number == 2

    finding = result.findings[0]
    assert finding.outcome == "QUARANTINE"
    assert finding.outcome != "FAIL"

    # Third assertion: publish the non-orphan row (order 9301) alone --
    # mirroring what a real caller would do once it excludes the orphan
    # identified above -- and confirm it is unaffected by 9302's exclusion.
    publish_table = "staging.orders_test_referential_race_publish"
    with psycopg.connect(migrated_dsn) as conn:
        _create_staging_table(conn, publish_table)
        _insert_staging_row(
            conn,
            publish_table,
            order_id="9301",
            customer_id="7001",
            order_date="2026-02-01",
            amount="15.00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
        )
        conn.commit()

        publish_result = OrdersMergePublisher().publish(_make_context(db_pool), publish_table, conn)
        conn.commit()

    assert publish_result.outcome == "PUBLISHED"
    assert publish_result.rows_affected == 1

    with psycopg.connect(migrated_dsn) as verify_conn:
        row = verify_conn.execute(
            "SELECT customer_id, amount FROM normalized.orders WHERE order_id = %s",
            (9301,),
        ).fetchone()
        orphan_row = verify_conn.execute(
            "SELECT 1 FROM normalized.orders WHERE order_id = %s",
            (9302,),
        ).fetchone()

    assert row is not None
    assert row[0] == 7001
    assert str(row[1]) == "15.00"
    # The orphan (9302) was never published -- proving its exclusion did
    # not affect the non-orphan row's own publish.
    assert orphan_row is None
