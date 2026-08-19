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

from csv_processor.source import CsvSource
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
from dataplat.pipeline.run import publish_ingest, stage_ingest
from dataplat.storage.db import create_pool
from dataplat.storage.objectstore import S3ObjectStore

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
    "expected_row_count",
    "control_total_discrepancy",
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


# --- raw_bronze hop: StagingLoader.promote_to_durable_bronze (plan 09-07) ----
#
# Every test below drives a real ``stage_ingest`` -- claim, stage, quality-
# gate, promote-to-durable-bronze -- against real testcontainers PostgreSQL
# AND MinIO, using a real ``csv_processor.source.CsvSource`` (never a fake/
# in-memory source), mirroring ``test_stage_ingest.py``'s own fixture shape
# (deliberately duplicated locally rather than imported, matching this test
# suite's established per-file helper convention). Each test uses its own
# widely-separated ``customer_id`` range (9_600_0xx) -- ``staging.customers``
# is a shared, session-scoped table across the whole ``tests/integration/``
# collection, and other files already occupy 9_100_0xx-9_502_0xx / 9994xxx.

_RAW_BRONZE_BUCKET = "raw-bronze-reconciliation-test"
_RAW_BRONZE_VALIDATED_BUCKET = "validated"
_RAW_BRONZE_CSV_HEADER = "customer_id,name,country,birth_date,event_ts\n"


def _raw_bronze_row(customer_id: int) -> str:
    return f"{customer_id},Name{customer_id},US,1990-01-01,2026-01-01T00:00:00+00:00\n"


def _raw_bronze_csv_bytes(rows: int, *, start_id: int) -> bytes:
    lines = [
        _RAW_BRONZE_CSV_HEADER,
        *(_raw_bronze_row(start_id + offset) for offset in range(rows)),
    ]
    return "".join(lines).encode("utf-8")


@dataclass
class _RawBronzeEnv:
    metadata: PostgresMetadataRepository
    objects: S3ObjectStore
    pool: Any
    migrated_dsn: str
    s3_client: Any
    scratch_bucket: str


@pytest.fixture
def _raw_bronze_bucket(s3_client: Any) -> str:
    existing = {bucket["Name"] for bucket in s3_client.list_buckets().get("Buckets", [])}
    if _RAW_BRONZE_BUCKET not in existing:
        s3_client.create_bucket(Bucket=_RAW_BRONZE_BUCKET)
    return _RAW_BRONZE_BUCKET


@pytest.fixture
def _raw_bronze_validated_bucket(s3_client: Any) -> str:
    """`_apply_staging_quality_gate_and_persist` writes its report to `s3://validated/...`
    unconditionally -- this bucket must exist before `stage_ingest` runs, mirroring
    `test_stage_ingest.py`'s own `_validated_bucket` fixture.
    """
    existing = {bucket["Name"] for bucket in s3_client.list_buckets().get("Buckets", [])}
    if _RAW_BRONZE_VALIDATED_BUCKET not in existing:
        s3_client.create_bucket(Bucket=_RAW_BRONZE_VALIDATED_BUCKET)
    return _RAW_BRONZE_VALIDATED_BUCKET


@pytest.fixture
def raw_bronze_env(
    _pool: Any,
    migrated_dsn: str,
    s3_client: Any,
    minio_config: dict[str, str],
    _raw_bronze_bucket: str,
    _raw_bronze_validated_bucket: str,
) -> _RawBronzeEnv:
    return _RawBronzeEnv(
        metadata=PostgresMetadataRepository(_pool),
        objects=S3ObjectStore(
            endpoint_url=f"http://{minio_config['endpoint']}",
            access_key=minio_config["access_key"],
            secret_key=minio_config["secret_key"],
        ),
        pool=_pool,
        migrated_dsn=migrated_dsn,
        s3_client=s3_client,
        scratch_bucket=_raw_bronze_bucket,
    )


def _seed_and_build_raw_bronze_ctx(
    env: _RawBronzeEnv,
    *,
    key_suffix: str,
    csv_bytes: bytes,
    batch_expected_row_count: int | None = None,
    batch_expected_checksum: str | None = None,
) -> tuple[PipelineContext, int, int, int]:
    """Seed dataset/config/file/batch/PENDING-run, upload ``csv_bytes``, build a `PipelineContext`.

    Returns:
        `(ctx, run_id, file_id, batch_id)`.
    """
    dataset_id = env.metadata.get_or_create_dataset(f"raw_bronze_recon_{key_suffix}")
    config_version_id = _insert_config_version(env.migrated_dsn, dataset_id=dataset_id)
    object_key = f"customers/{key_suffix}.csv"
    env.s3_client.put_object(Bucket=env.scratch_bucket, Key=object_key, Body=csv_bytes)
    file_id = env.metadata.create_file(
        dataset_id=dataset_id,
        object_uri=f"s3://{env.scratch_bucket}/{object_key}",
        content_sha256=hashlib.sha256(csv_bytes).digest(),
        hash_version=1,
        size_bytes=len(csv_bytes),
        filename=f"{key_suffix}.csv",
        status="DISCOVERED",
    )
    batch_id = env.metadata.create_batch(
        dataset_id=dataset_id,
        batch_key=f"{key_suffix}:2026-08-19:1",
        status="OPEN",
    )
    run_id, _ = env.metadata.get_or_create_ingestion_run(
        idempotency_key=f"raw_bronze_recon_{key_suffix}:1",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        file_id=file_id,
        batch_id=batch_id,
    )
    # `_make_customers_config()`'s `dataset` field is a fixed literal
    # ("customers") -- `stage_ingest`'s own `_TARGET_COLUMNS_BY_DATASET`
    # lookup (`dataplat.pipeline.run`) only knows `"customers"`/`"orders"`,
    # so every raw_bronze test below reuses that dataset name for
    # `ctx.config.dataset` while still registering its OWN, per-test
    # `meta.datasets` row (`raw_bronze_recon_{key_suffix}`) via
    # `get_or_create_dataset` above -- the config object itself is a fresh,
    # locally-constructed test double, never the real `customers.yaml`.
    config = _make_customers_config()
    ctx = PipelineContext(
        run=RunContext(
            run_id=run_id,
            idempotency_key=f"raw_bronze_recon_{key_suffix}:1",
            file_id=file_id,
            batch_id=batch_id,
            batch_expected_row_count=batch_expected_row_count,
            batch_expected_checksum=batch_expected_checksum,
        ),
        config=config,
        metadata=env.metadata,
        objects=env.objects,
        db=env.pool,
        log=get_logger(),
        source=CsvSource(bucket=env.scratch_bucket, key=object_key),
    )
    return ctx, run_id, file_id, batch_id


def _read_raw_bronze_row(
    migrated_dsn: str,
    *,
    dataset_id: int,
    file_id: int,
) -> dict[str, Any]:
    rows = _read_reconciliation_rows(
        migrated_dsn,
        dataset_id=dataset_id,
        file_id=file_id,
        hop="raw_bronze",
    )
    assert len(rows) == 1
    return rows[0]


def test_clean_staging_pass_writes_one_raw_bronze_row_with_zero_discrepancy(
    raw_bronze_env: _RawBronzeEnv,
) -> None:
    """Test 1: nothing rejected -> `input_count == rows_read`, `output_count == rows_parsed`,
    `rejected_count == rows_rejected == 0`, `discrepancy == 0` (D-22's formula holds by
    construction: every read row is either parsed or rejected).
    """
    ctx, _run_id, file_id, _batch_id = _seed_and_build_raw_bronze_ctx(
        raw_bronze_env,
        key_suffix="raw_bronze_clean",
        csv_bytes=_raw_bronze_csv_bytes(3, start_id=9_600_001),
    )
    # `record_reconciliation`'s own `dataset_id` comes from
    # `ctx.metadata.get_or_create_dataset(ctx.config.dataset)` inside
    # `promote_to_durable_bronze` -- `ctx.config.dataset` is always
    # `"customers"` (`_make_customers_config()`'s fixed literal), never the
    # per-test `raw_bronze_recon_{key_suffix}` name used only to seed this
    # test's own file/batch/run rows above. `file_id` alone already scopes
    # every assertion below to this one test.
    dataset_id = raw_bronze_env.metadata.get_or_create_dataset(ctx.config.dataset)

    receipt = stage_ingest(ctx)
    assert receipt.status == "STAGED"

    row = _read_raw_bronze_row(raw_bronze_env.migrated_dsn, dataset_id=dataset_id, file_id=file_id)
    assert row["input_count"] == 3
    assert row["output_count"] == 3
    assert row["rejected_count"] == 0
    assert row["discrepancy"] == 0


def test_raw_bronze_no_batch_complete_marker_leaves_expected_row_count_and_discrepancy_null(
    raw_bronze_env: _RawBronzeEnv,
) -> None:
    """Test 2: no `_BATCH_COMPLETE` manifest -> `expected_row_count`/`control_total_discrepancy`
    stay `NULL` -- `ctx.run.batch_expected_row_count` defaults to `None` when nothing set it.
    """
    ctx, _run_id, file_id, _batch_id = _seed_and_build_raw_bronze_ctx(
        raw_bronze_env,
        key_suffix="raw_bronze_no_marker",
        csv_bytes=_raw_bronze_csv_bytes(2, start_id=9_600_101),
    )
    dataset_id = raw_bronze_env.metadata.get_or_create_dataset(ctx.config.dataset)

    receipt = stage_ingest(ctx)
    assert receipt.status == "STAGED"

    row = _read_raw_bronze_row(raw_bronze_env.migrated_dsn, dataset_id=dataset_id, file_id=file_id)
    assert row["expected_row_count"] is None
    assert row["control_total_discrepancy"] is None


def test_raw_bronze_matching_batch_expected_row_count_writes_zero_control_total_discrepancy(
    raw_bronze_env: _RawBronzeEnv,
) -> None:
    """Test 3: `ctx.run.batch_expected_row_count` matches `rows_parsed` exactly ->
    `control_total_discrepancy == 0`.
    """
    ctx, _run_id, file_id, _batch_id = _seed_and_build_raw_bronze_ctx(
        raw_bronze_env,
        key_suffix="raw_bronze_match",
        csv_bytes=_raw_bronze_csv_bytes(4, start_id=9_600_201),
        batch_expected_row_count=4,
    )
    dataset_id = raw_bronze_env.metadata.get_or_create_dataset(ctx.config.dataset)

    receipt = stage_ingest(ctx)
    assert receipt.status == "STAGED"

    row = _read_raw_bronze_row(raw_bronze_env.migrated_dsn, dataset_id=dataset_id, file_id=file_id)
    assert row["expected_row_count"] == 4
    assert row["control_total_discrepancy"] == 0


def test_raw_bronze_mismatched_batch_expected_row_count_records_discrepancy_and_completes_normally(
    raw_bronze_env: _RawBronzeEnv,
) -> None:
    """Test 4: `ctx.run.batch_expected_row_count` does NOT match `rows_parsed` ->
    a non-zero `control_total_discrepancy` is recorded, AND the call still completes
    normally -- no exception raised, the transaction still commits, staging is not
    blocked (D-22's "record and continue" rule, proven directly here).
    """
    ctx, _run_id, file_id, _batch_id = _seed_and_build_raw_bronze_ctx(
        raw_bronze_env,
        key_suffix="raw_bronze_mismatch",
        csv_bytes=_raw_bronze_csv_bytes(5, start_id=9_600_301),
        batch_expected_row_count=8,  # claims 8, only 5 actually staged
    )
    dataset_id = raw_bronze_env.metadata.get_or_create_dataset(ctx.config.dataset)

    receipt = stage_ingest(ctx)
    assert receipt.status == "STAGED"

    row = _read_raw_bronze_row(raw_bronze_env.migrated_dsn, dataset_id=dataset_id, file_id=file_id)
    assert row["expected_row_count"] == 8
    assert row["control_total_discrepancy"] == 3  # 8 - 5
