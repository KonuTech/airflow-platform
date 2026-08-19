"""Integration tests for ``publish_ingest``'s D-01..D-04 watermark advance (plan 09-02 Task 3).

Every test drives a real ``publish_ingest`` against real testcontainers
PostgreSQL, mirroring ``test_publish_ingest.py``'s own fixture/helper shape
(``env``/``_Env``, ``_seed_staged_run``, ``_insert_config_version``) --
duplicated locally rather than imported, matching this test suite's
established per-file helper convention.

``MergePublisher``/``OrdersMergePublisher`` are hardcoded to
``normalized.customers``/``normalized.orders`` regardless of
``ctx.config.load.target`` (their own module docstrings), and only
``silver.customers``/``silver.orders`` physically exist (migration 0023) --
so every test here uses the literal dataset name ``"customers"`` or
``"orders"``, exactly like ``test_publish_ingest.py``'s own Behavior 2/3
constraint. ``silver.customers``/``silver.orders`` both carry a real
``UNIQUE(customer_id)``/``UNIQUE(order_id)`` constraint (migration 0023), so
every business-key value used below is a genuinely fresh, never-reused
value -- a repeat would raise ``UniqueViolation``, not silently upsert.

Test 3 (a dataset with no prior watermark row) uses ``"orders"`` --
deliberately different from Tests 1/2's ``"customers"`` -- so it never
depends on Tests 1/2's own execution order within this file, and stays
correct whether this file runs alone (this task's own acceptance criterion,
``pytest tests/integration/test_watermarks.py -q``) or together with
``test_reconciliation.py`` (the plan's own combined ``<verification>``
command, which lists this file FIRST -- pytest runs explicitly-listed files
in the given order, so ``orders``' watermark is still untouched by
``test_reconciliation.py`` at the point this file's Test 3 runs).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import psycopg
import pytest

from dataplat.config.model import (
    BatchingConfig,
    ColumnContract,
    DatasetConfig,
    DeduplicationConfig,
    LoadConfig,
    SourceConfig,
)
from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.models.identity import RunContext
from dataplat.observability.logging import get_logger
from dataplat.pipeline.protocol import PipelineContext
from dataplat.pipeline.run import publish_ingest
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration


def _make_customers_config() -> DatasetConfig:
    """Mirrors `test_publish_ingest.py`'s own `_make_config()` -- unused fields still required."""
    return DatasetConfig(
        dataset="customers",
        config_schema_version=1,
        source=SourceConfig(
            type="csv",
            bucket="watermark-test",
            path="customers/",
            change_semantics="snapshot",
            duplicate_policy="skip",
        ),
        deduplication=DeduplicationConfig(
            strategy="business_key_latest",
            keys=["customer_id"],
            order_by=["event_ts desc"],
        ),
        load=LoadConfig(strategy="merge", target="normalized.customers"),
        batching=BatchingConfig(max_units_per_run=100),
        columns=[
            ColumnContract(
                name="customer_id",
                type="string",
                nullable=False,
                required=True,
                business_key=True,
                description="Natural business key for a customer record",
            ),
            ColumnContract(name="name", type="string", nullable=False, required=True),
            ColumnContract(name="country", type="string", nullable=False, required=True),
            ColumnContract(
                name="birth_date",
                type="date",
                nullable=True,
                required=True,
                format="%Y-%m-%d",
            ),
            ColumnContract(
                name="event_ts",
                type="timestamp",
                nullable=False,
                required=True,
                format="%Y-%m-%dT%H:%M:%S%z",
            ),
        ],
    )


def _make_orders_config() -> DatasetConfig:
    """Mirrors `_make_customers_config()`, targeting `orders`' own shape (D-02's `order_date`)."""
    return DatasetConfig(
        dataset="orders",
        config_schema_version=1,
        source=SourceConfig(
            type="csv",
            bucket="watermark-test",
            path="orders/",
            change_semantics="snapshot",
            duplicate_policy="skip",
        ),
        deduplication=DeduplicationConfig(
            strategy="business_key_latest",
            keys=["order_id"],
            order_by=["order_date desc"],
        ),
        load=LoadConfig(strategy="merge_orders", target="normalized.orders"),
        batching=BatchingConfig(max_units_per_run=100),
        columns=[
            ColumnContract(
                name="order_id",
                type="string",
                nullable=False,
                required=True,
                business_key=True,
                description="Natural business key for an order record",
            ),
            ColumnContract(name="customer_id", type="string", nullable=False, required=True),
            ColumnContract(
                name="order_date",
                type="date",
                nullable=True,
                required=True,
                format="%Y-%m-%d",
            ),
            ColumnContract(name="amount", type="decimal", nullable=True, required=True),
        ],
    )


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Get-or-insert a synthetic, CURRENT `meta.config_versions` row.

    Mirrors `test_publish_ingest.py`'s own `_insert_config_version` helper.
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
                "config_hash": "synthetic-hash-for-test",
                "config_document": json.dumps({"synthetic": True}),
                "config_schema_version": 1,
            },
        ).fetchone()
        assert row is not None
        return int(row[0])


def _seed_staged_run(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
    *,
    dataset_name: str,
    key_suffix: str,
) -> tuple[int, int, int, int]:
    """Create dataset/config_version/file/batch/STAGED run (mirrors `test_publish_ingest.py`)."""
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
        batch_key=f"{key_suffix}:2026-08-19:1",
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
    return dataset_id, run_id, file_id, batch_id


def _insert_silver_customers_row(  # noqa: PLR0913 -- one keyword per silver column
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


def _insert_silver_orders_row(  # noqa: PLR0913 -- one keyword per silver column
    conn: psycopg.Connection[Any],
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
        INSERT INTO silver.orders (
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


@dataclass
class _Env:
    metadata: PostgresMetadataRepository
    pool: Any
    migrated_dsn: str


@pytest.fixture
def _pool(migrated_dsn: str) -> Iterator[Any]:
    opened_pool = create_pool(migrated_dsn)
    opened_pool.open(wait=True)
    try:
        yield opened_pool
    finally:
        opened_pool.close()


@pytest.fixture
def env(_pool: Any, migrated_dsn: str) -> _Env:
    return _Env(metadata=PostgresMetadataRepository(_pool), pool=_pool, migrated_dsn=migrated_dsn)


def _make_ctx(env: _Env, *, config: DatasetConfig) -> PipelineContext:
    return PipelineContext(
        run=RunContext(run_id=0, idempotency_key="watermark-test-placeholder"),
        config=config,
        metadata=env.metadata,
        objects=None,  # type: ignore[arg-type] -- unused by publish_ingest
        db=env.pool,
        log=get_logger(),
    )


def _read_watermark_history_rows(
    migrated_dsn: str,
    *,
    dataset_id: int,
    target_key: str = "default",
) -> list[tuple[datetime | None, datetime | None, int | None]]:
    with psycopg.connect(migrated_dsn) as conn:
        rows = conn.execute(
            """
            SELECT old_value, new_value, run_id
              FROM meta.watermark_history
             WHERE dataset_id = %s AND target_key = %s
             ORDER BY watermark_history_id ASC
            """,
            (dataset_id, target_key),
        ).fetchall()
    return [(row[0], row[1], row[2]) for row in rows]


def _true_max_event_ts(migrated_dsn: str) -> datetime | None:
    with psycopg.connect(migrated_dsn) as conn:
        row = conn.execute("SELECT max(event_ts::timestamptz) FROM silver.customers").fetchone()
    assert row is not None
    return row[0]


# --- Test 1: a NEWER max(event_ts) advances cursor_value --------------------


def test_newer_publish_advances_watermark_and_logs_history(env: _Env) -> None:
    ctx = _make_ctx(env, config=_make_customers_config())
    dataset_id, run_id, file_id, batch_id = _seed_staged_run(
        env.metadata,
        env.migrated_dsn,
        dataset_name="customers",
        key_suffix="watermark_newer",
    )
    before_cursor = env.metadata.get_current_watermark(dataset_id=dataset_id, target_key="default")

    with psycopg.connect(env.migrated_dsn) as conn:
        _insert_silver_customers_row(
            conn,
            customer_id="9993001",
            name="WatermarkNewer",
            country="US",
            birth_date="1990-01-01",
            event_ts="2099-01-01T00:00:00+00:00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"watermark-newer").digest(),
        )
        conn.commit()

    result = publish_ingest(ctx)
    assert run_id in result["runs_finalized"]

    after_cursor = env.metadata.get_current_watermark(dataset_id=dataset_id, target_key="default")
    assert after_cursor is not None
    true_max = _true_max_event_ts(env.migrated_dsn)
    assert after_cursor == true_max
    assert after_cursor == datetime(2099, 1, 1, tzinfo=UTC)

    history_rows = _read_watermark_history_rows(env.migrated_dsn, dataset_id=dataset_id)
    assert history_rows  # at least one row was appended
    last_old, last_new, last_run_id = history_rows[-1]
    assert last_old == before_cursor
    assert last_new == after_cursor
    assert last_run_id == run_id


# --- Test 2: an OLDER max(event_ts) leaves cursor_value unchanged, --------
# --- but still appends a watermark_history row (D-04) ----------------------


def test_older_publish_never_regresses_watermark_but_still_logs_history(env: _Env) -> None:
    ctx = _make_ctx(env, config=_make_customers_config())

    # Step 1: establish a known high cursor.
    dataset_id, run_id_high, file_id_high, batch_id_high = _seed_staged_run(
        env.metadata,
        env.migrated_dsn,
        dataset_name="customers",
        key_suffix="watermark_older_high",
    )
    with psycopg.connect(env.migrated_dsn) as conn:
        _insert_silver_customers_row(
            conn,
            customer_id="9993002",
            name="WatermarkHigh",
            country="US",
            birth_date="1990-01-01",
            event_ts="2100-01-01T00:00:00+00:00",
            run_id=run_id_high,
            file_id=file_id_high,
            batch_id=batch_id_high,
            source_row_number=1,
            record_hash=hashlib.sha256(b"watermark-older-high").digest(),
        )
        conn.commit()
    result_high = publish_ingest(ctx)
    assert run_id_high in result_high["runs_finalized"]
    cursor_after_high = env.metadata.get_current_watermark(
        dataset_id=dataset_id,
        target_key="default",
    )
    assert cursor_after_high == datetime(2100, 1, 1, tzinfo=UTC)

    # Step 2: publish a genuinely OLDER (late-arriving) row.
    _, run_id_old, file_id_old, batch_id_old = _seed_staged_run(
        env.metadata,
        env.migrated_dsn,
        dataset_name="customers",
        key_suffix="watermark_older_late",
    )
    with psycopg.connect(env.migrated_dsn) as conn:
        _insert_silver_customers_row(
            conn,
            customer_id="9993003",
            name="WatermarkLate",
            country="US",
            birth_date="1990-01-01",
            event_ts="1990-01-01T00:00:00+00:00",
            run_id=run_id_old,
            file_id=file_id_old,
            batch_id=batch_id_old,
            source_row_number=1,
            record_hash=hashlib.sha256(b"watermark-older-late").digest(),
        )
        conn.commit()
    result_old = publish_ingest(ctx)
    assert run_id_old in result_old["runs_finalized"]

    cursor_after_old = env.metadata.get_current_watermark(
        dataset_id=dataset_id,
        target_key="default",
    )
    # GREATEST() semantics: the late file never regresses the stored cursor.
    assert cursor_after_old == cursor_after_high

    history_rows = _read_watermark_history_rows(env.migrated_dsn, dataset_id=dataset_id)
    last_old, last_new, last_run_id = history_rows[-1]
    # D-04: this write was still logged, even though nothing moved.
    assert last_old == cursor_after_high
    assert last_new == cursor_after_high
    assert last_run_id == run_id_old


# --- Test 3: a dataset with no prior watermark row gets one created --------
# --- on its first publish; old_value IS NULL in that first history row -----


def test_first_ever_publish_creates_watermark_row_with_null_old_value(env: _Env) -> None:
    ctx = _make_ctx(env, config=_make_orders_config())
    dataset_id, run_id, file_id, batch_id = _seed_staged_run(
        env.metadata,
        env.migrated_dsn,
        dataset_name="orders",
        key_suffix="watermark_orders_first",
    )
    assert env.metadata.get_current_watermark(dataset_id=dataset_id, target_key="default") is None

    with psycopg.connect(env.migrated_dsn) as conn:
        _insert_silver_orders_row(
            conn,
            order_id="9993101",
            customer_id="1",
            order_date="2026-01-15",
            amount="42.50",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"watermark-orders-first").digest(),
        )
        conn.commit()

    result = publish_ingest(ctx)
    assert run_id in result["runs_finalized"]

    after_cursor = env.metadata.get_current_watermark(dataset_id=dataset_id, target_key="default")
    assert after_cursor is not None

    history_rows = _read_watermark_history_rows(env.migrated_dsn, dataset_id=dataset_id)
    assert len(history_rows) == 1
    old_value, new_value, history_run_id = history_rows[0]
    assert old_value is None
    assert new_value == after_cursor
    assert history_run_id == run_id
