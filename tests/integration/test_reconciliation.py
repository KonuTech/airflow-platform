"""Integration tests for ``publish_ingest``'s D-20..D-24 silver->gold reconciliation.

Plan 09-02 Task 3.

Mirrors ``test_watermarks.py``'s own fixture/helper shape (duplicated
locally, matching this test suite's established per-file helper
convention). Every test here is scoped ``-k silver_gold`` -- the
``raw_bronze``/``bronze_silver`` hop assertions belong to later plans
(09-07/09-08) and do not exist here.

Both ``silver.customers`` (D-14's ``UNIQUE(customer_id)``) and
``normalized.customers`` (migration 0005's own ``UNIQUE(customer_id)``)
enforce a 1:1 business-key relationship, and `publish_ingest`'s own
`MergePublisher` republishes the WHOLE cumulative `silver.customers` table
on every call (never a per-run slice) -- so after any successful publish,
`count(*) FROM silver.customers` and `count(*) FROM normalized.customers`
converge to be EQUAL (every silver row has, by definition, already been
upserted into gold). This is what makes Test 4's `discrepancy == 0`
assertion a general truth of this design, not a coincidence of a
particular row count -- and it is why every business-key value used below
must be genuinely fresh (a repeat raises `UniqueViolation` against
`silver.customers`'s own constraint, never a silent upsert).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import psycopg
import pytest

from dataplat.config.model import (
    BatchingConfig,
    ColumnContract,
    DatasetConfig,
    DeduplicationConfig,
    LoadConfig,
    ReconciliationConfig,
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
    """No `reconciliation:` block -- mirrors `customers.yaml`'s own real shape (D-25)."""
    return DatasetConfig(
        dataset="customers",
        config_schema_version=1,
        source=SourceConfig(
            type="csv",
            bucket="reconciliation-test",
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
    """Declares `reconciliation.sum_columns: [amount]` -- mirrors `orders.yaml`'s own real shape."""
    return DatasetConfig(
        dataset="orders",
        config_schema_version=1,
        source=SourceConfig(
            type="csv",
            bucket="reconciliation-test",
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
        reconciliation=ReconciliationConfig(sum_columns=["amount"]),
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
        run=RunContext(run_id=0, idempotency_key="reconciliation-test-placeholder"),
        config=config,
        metadata=env.metadata,
        objects=None,  # type: ignore[arg-type] -- unused by publish_ingest
        db=env.pool,
        log=get_logger(),
    )


_RECONCILIATION_COLUMNS = (
    "input_count",
    "output_count",
    "rejected_count",
    "dedup_count",
    "discrepancy",
    "sum_column",
    "sum_input",
    "sum_output",
    "checksum_input",
    "checksum_output",
    "min_input",
    "max_input",
    "key_count_input",
    "key_count_output",
)


def _read_reconciliation_rows(
    migrated_dsn: str,
    *,
    dataset_id: int,
    file_id: int,
    hop: str,
) -> list[dict[str, Any]]:
    columns_sql = ", ".join(_RECONCILIATION_COLUMNS)
    with psycopg.connect(migrated_dsn) as conn:
        rows = conn.execute(
            f"""
            SELECT {columns_sql}
              FROM meta.reconciliation_results
             WHERE dataset_id = %s AND file_id = %s AND hop = %s
             ORDER BY reconciliation_id ASC
            """,  # noqa: S608 -- columns_sql is a fixed, hardcoded module-level tuple, never row/user content
            (dataset_id, file_id, hop),
        ).fetchall()
    return [dict(zip(_RECONCILIATION_COLUMNS, row, strict=True)) for row in rows]


# --- Test 4: a clean publish writes one silver_gold row per finalized file, -
# --- with zero discrepancy ---------------------------------------------------


def test_clean_publish_writes_one_silver_gold_row_with_zero_discrepancy(env: _Env) -> None:
    ctx = _make_ctx(env, config=_make_customers_config())
    dataset_id, run_id, file_id, batch_id = _seed_staged_run(
        env.metadata,
        env.migrated_dsn,
        dataset_name="customers",
        key_suffix="reconciliation_clean",
    )
    with psycopg.connect(env.migrated_dsn) as conn:
        _insert_silver_customers_row(
            conn,
            customer_id="9994001",
            name="ReconClean",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-02-01T00:00:00+00:00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"reconciliation-clean").digest(),
        )
        conn.commit()

    result = publish_ingest(ctx)
    assert run_id in result["runs_finalized"]

    rows = _read_reconciliation_rows(
        env.migrated_dsn,
        dataset_id=dataset_id,
        file_id=file_id,
        hop="silver_gold",
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["rejected_count"] == 0
    assert row["dedup_count"] == 0
    assert row["input_count"] == row["output_count"]
    assert row["discrepancy"] == 0


# --- Test 5: orders (declares reconciliation.sum_columns) populates every ---
# --- reconciliation figure; customers (no reconciliation: block) leaves -----
# --- sum_column/sum_input/sum_output NULL but populates everything else -----


def test_orders_reconciliation_populates_sums_customers_does_not(env: _Env) -> None:
    orders_ctx = _make_ctx(env, config=_make_orders_config())
    dataset_id_orders, run_id_orders, file_id_orders, batch_id_orders = _seed_staged_run(
        env.metadata,
        env.migrated_dsn,
        dataset_name="orders",
        key_suffix="reconciliation_orders",
    )
    with psycopg.connect(env.migrated_dsn) as conn:
        _insert_silver_orders_row(
            conn,
            order_id="9994101",
            customer_id="1",
            order_date="2026-02-01",
            amount="100.50",
            run_id=run_id_orders,
            file_id=file_id_orders,
            batch_id=batch_id_orders,
            source_row_number=1,
            record_hash=hashlib.sha256(b"reconciliation-orders").digest(),
        )
        conn.commit()

    result_orders = publish_ingest(orders_ctx)
    assert run_id_orders in result_orders["runs_finalized"]

    orders_rows = _read_reconciliation_rows(
        env.migrated_dsn,
        dataset_id=dataset_id_orders,
        file_id=file_id_orders,
        hop="silver_gold",
    )
    assert len(orders_rows) == 1
    orders_row = orders_rows[0]
    assert orders_row["sum_column"] == "amount"
    assert orders_row["sum_input"] is not None
    assert orders_row["sum_output"] is not None
    assert orders_row["checksum_input"] is not None
    assert orders_row["checksum_output"] is not None
    assert orders_row["min_input"] is not None
    assert orders_row["max_input"] is not None
    assert orders_row["key_count_input"] is not None
    assert orders_row["key_count_output"] is not None

    customers_ctx = _make_ctx(env, config=_make_customers_config())
    dataset_id_customers, run_id_customers, file_id_customers, batch_id_customers = (
        _seed_staged_run(
            env.metadata,
            env.migrated_dsn,
            dataset_name="customers",
            key_suffix="reconciliation_customers",
        )
    )
    with psycopg.connect(env.migrated_dsn) as conn:
        _insert_silver_customers_row(
            conn,
            customer_id="9994201",
            name="ReconCustomers",
            country="CA",
            birth_date="1991-02-02",
            event_ts="2026-02-02T00:00:00+00:00",
            run_id=run_id_customers,
            file_id=file_id_customers,
            batch_id=batch_id_customers,
            source_row_number=1,
            record_hash=hashlib.sha256(b"reconciliation-customers-2").digest(),
        )
        conn.commit()

    result_customers = publish_ingest(customers_ctx)
    assert run_id_customers in result_customers["runs_finalized"]

    customers_rows = _read_reconciliation_rows(
        env.migrated_dsn,
        dataset_id=dataset_id_customers,
        file_id=file_id_customers,
        hop="silver_gold",
    )
    assert len(customers_rows) == 1
    customers_row = customers_rows[0]
    assert customers_row["sum_column"] is None
    assert customers_row["sum_input"] is None
    assert customers_row["sum_output"] is None
    assert customers_row["checksum_input"] is not None
    assert customers_row["checksum_output"] is not None
    assert customers_row["min_input"] is not None
    assert customers_row["max_input"] is not None
    assert customers_row["key_count_input"] is not None
    assert customers_row["key_count_output"] is not None
