"""Integration tests for `run_ingest`'s barrier-stage/D-05/MinIO-report wiring (plan 08-11).

Every test drives a real `run_ingest` against real testcontainers PostgreSQL
+ MinIO, using a real `csv_processor.source.CsvSource` -- proving the FULL
FAIL-vs-QUARANTINE distinction (Pitfall 2), D-05's backfill-resolution
guarantee, and VALID-04's MinIO-artifact half through the actual production
code path, not an isolated unit test.

This file is self-contained (its own `env`/bucket fixtures, its own
`DatasetConfig` with a real `quality:` block) rather than importing
`test_run_ingest.py`'s fixtures, matching this test suite's own established
per-file helper convention (`test_publish_orders.py`/
`test_validation_persistence.py`/`test_backfill_resolution.py` all duplicate
helpers locally rather than sharing them).

Every test uses its own, widely-separated `customer_id` range (9_400_0xx and
up) -- `normalized.customers.customer_id` is unique across the WHOLE table,
not scoped per dataset, and `tests/integration/`'s Postgres container is
session-scoped and shared with every other file in this directory
(`test_run_ingest.py` already occupies 9_100_0xx-9_200_0xx,
`test_lineage_view.py` occupies 9_300_0xx) -- so collisions are a real risk,
not a theoretical one.
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
    QualityConfig,
    QualityRuleConfig,
    SourceConfig,
)
from dataplat.errors import QualityThresholdExceeded
from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.models.identity import RunContext
from dataplat.observability.logging import get_logger
from dataplat.pipeline.protocol import PipelineContext
from dataplat.pipeline.run import run_ingest
from dataplat.storage.db import create_pool
from dataplat.storage.objectstore import S3ObjectStore

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration

_BUCKET = "publish-transaction-wiring-test"
_VALIDATED_BUCKET = "validated"
_CSV_HEADER = "customer_id,name,country,birth_date,event_ts\n"


def _row(customer_id: int, *, empty_name: bool = False) -> str:
    """One well-formed customers CSV row, optionally with an empty `name` (a completeness violation)."""  # noqa: E501, W505
    name = "" if empty_name else f"Name{customer_id}"
    return f"{customer_id},{name},US,1990-01-01,2026-01-01T00:00:00+00:00\n"


def _csv_bytes(*, good_count: int, bad_count: int, start_id: int) -> bytes:
    """`good_count` well-formed rows followed by `bad_count` empty-`name` rows, from `start_id`."""
    lines = [_CSV_HEADER]
    customer_id = start_id
    for _ in range(good_count):
        lines.append(_row(customer_id))
        customer_id += 1
    for _ in range(bad_count):
        lines.append(_row(customer_id, empty_name=True))
        customer_id += 1
    return "".join(lines).encode("utf-8")


def _make_config(*, rejection_rate_threshold: float) -> DatasetConfig:
    """A `customers`-shaped `DatasetConfig` with a real completeness rule + circuit breaker."""
    return DatasetConfig(
        dataset="customers",
        config_schema_version=1,
        source=SourceConfig(
            type="csv",
            bucket=_BUCKET,
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
        quality=QualityConfig(
            rules=[
                QualityRuleConfig(
                    rule_id="wiring_name_completeness",
                    rule_type="QUALITY_COMPLETENESS",
                    strategy="REJECT_RECORD",
                    column="name",
                ),
            ],
            rejection_rate_threshold=rejection_rate_threshold,
        ),
    )


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Insert a synthetic `meta.config_versions` row directly via SQL (this suite's own convention)."""  # noqa: E501, W505
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


def _insert_pending_reject(
    migrated_dsn: str,
    *,
    run_id: int,
    file_id: int,
    batch_id: int,
    source_row_number: int,
) -> None:
    """Seed one PENDING `meta.rejected_records` row directly via SQL (mirrors `test_backfill_resolution.py`)."""  # noqa: E501, W505
    with psycopg.connect(migrated_dsn) as conn:
        conn.execute(
            """
            INSERT INTO meta.rejected_records (
                run_id, file_id, batch_id, source_row_number, raw_line,
                error_type, error_message
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                file_id,
                batch_id,
                source_row_number,
                f"row-{source_row_number}",
                "RAGGED_ROW",
                "seeded directly for D-05 backfill-resolution proof",
            ),
        )
        conn.commit()


@dataclass
class _Env:
    """This file's fixtures, bundled -- mirrors `test_run_ingest.py`'s own `_Env` shape."""

    metadata: PostgresMetadataRepository
    objects: S3ObjectStore
    pool: Any
    migrated_dsn: str
    s3_client: Any
    scratch_bucket: str


@pytest.fixture
def _pool(migrated_dsn: str) -> Iterator[Any]:
    opened_pool = create_pool(migrated_dsn)
    opened_pool.open(wait=True)
    try:
        yield opened_pool
    finally:
        opened_pool.close()


@pytest.fixture
def _scratch_bucket(s3_client: Any) -> str:
    existing = {bucket["Name"] for bucket in s3_client.list_buckets().get("Buckets", [])}
    if _BUCKET not in existing:
        s3_client.create_bucket(Bucket=_BUCKET)
    return _BUCKET


@pytest.fixture
def _validated_bucket(s3_client: Any) -> str:
    """Ensure the real "validated" bucket exists (VALID-04's MinIO-artifact half)."""
    existing = {bucket["Name"] for bucket in s3_client.list_buckets().get("Buckets", [])}
    if _VALIDATED_BUCKET not in existing:
        s3_client.create_bucket(Bucket=_VALIDATED_BUCKET)
    return _VALIDATED_BUCKET


@pytest.fixture
def env(
    _pool: Any,
    migrated_dsn: str,
    s3_client: Any,
    minio_config: dict[str, str],
    _scratch_bucket: str,
    _validated_bucket: str,
) -> _Env:
    return _Env(
        metadata=PostgresMetadataRepository(_pool),
        objects=S3ObjectStore(
            endpoint_url=f"http://{minio_config['endpoint']}",
            access_key=minio_config["access_key"],
            secret_key=minio_config["secret_key"],
        ),
        pool=_pool,
        migrated_dsn=migrated_dsn,
        s3_client=s3_client,
        scratch_bucket=_scratch_bucket,
    )


def _seed_pending_run(
    env: _Env,
    *,
    key_suffix: str,
    csv_bytes: bytes,
) -> tuple[int, int, int, int, str]:
    """Seed dataset/config/file/batch/PENDING-run and upload `csv_bytes`.

    Returns:
        `(dataset_id, run_id, file_id, batch_id, object_key)`.
    """
    dataset_id = env.metadata.get_or_create_dataset(f"wiring_{key_suffix}")
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
        batch_key=f"{key_suffix}:2026-08-17:1",
        status="OPEN",
    )
    run_id, _ = env.metadata.get_or_create_ingestion_run(
        idempotency_key=f"wiring_{key_suffix}:1",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        file_id=file_id,
        batch_id=batch_id,
    )
    return dataset_id, run_id, file_id, batch_id, object_key


def _make_ctx(  # noqa: PLR0913 -- one keyword per identity/config value, mirrors test_run_ingest.py's own shape
    env: _Env,
    *,
    run_id: int,
    file_id: int,
    batch_id: int,
    object_key: str,
    idempotency_key: str,
    rejection_rate_threshold: float,
) -> PipelineContext:
    return PipelineContext(
        run=RunContext(
            run_id=run_id,
            idempotency_key=idempotency_key,
            file_id=file_id,
            batch_id=batch_id,
        ),
        config=_make_config(rejection_rate_threshold=rejection_rate_threshold),
        metadata=env.metadata,
        objects=env.objects,
        db=env.pool,
        log=get_logger(),
        source=CsvSource(bucket=env.scratch_bucket, key=object_key),
    )


def _fetch_validation_results(migrated_dsn: str, *, run_id: int) -> list[tuple[Any, ...]]:
    with psycopg.connect(migrated_dsn) as conn:
        return conn.execute(
            """
            SELECT rule_id, rule_type, outcome, evaluated_count, failed_count
              FROM meta.validation_results
             WHERE run_id = %s
             ORDER BY rule_id
            """,
            (run_id,),
        ).fetchall()


def _fetch_rejected_records(migrated_dsn: str, *, run_id: int) -> list[tuple[Any, ...]]:
    with psycopg.connect(migrated_dsn) as conn:
        return conn.execute(
            """
            SELECT source_row_number, error_type, source_row_number
              FROM meta.rejected_records
             WHERE run_id = %s
             ORDER BY source_row_number
            """,
            (run_id,),
        ).fetchall()


def _customers_count_for_run(migrated_dsn: str, *, run_id: int) -> int:
    with psycopg.connect(migrated_dsn) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM normalized.customers WHERE _run_id = %s",
            (run_id,),
        ).fetchone()
    assert row is not None
    return int(row[0])


def _resolution_state(migrated_dsn: str, *, batch_id: int) -> list[tuple[Any, ...]]:
    with psycopg.connect(migrated_dsn) as conn:
        return conn.execute(
            """
            SELECT source_row_number, resolution_type, resolved_by_run_id
              FROM meta.rejected_records
             WHERE batch_id = %s
             ORDER BY source_row_number
            """,
            (batch_id,),
        ).fetchall()


# --- Test A: circuit breaker trips -> zero rows anywhere for this run ------


def test_circuit_breaker_trip_leaves_zero_rows_for_this_run(env: _Env) -> None:
    """Pitfall 2's exact distinguishing test: `QualityThresholdExceeded` rolls back everything."""
    csv_bytes = _csv_bytes(good_count=9, bad_count=1, start_id=9_400_001)
    dataset_id, run_id, file_id, batch_id, object_key = _seed_pending_run(
        env,
        key_suffix="circuit_breaker_trip",
        csv_bytes=csv_bytes,
    )
    del dataset_id
    ctx = _make_ctx(
        env,
        run_id=run_id,
        file_id=file_id,
        batch_id=batch_id,
        object_key=object_key,
        idempotency_key="wiring_circuit_breaker_trip:1",
        rejection_rate_threshold=0.01,  # 1 bad / 10 total = 10% > 1%
    )

    with pytest.raises(QualityThresholdExceeded):
        run_ingest(ctx)

    assert _fetch_validation_results(env.migrated_dsn, run_id=run_id) == []
    assert _fetch_rejected_records(env.migrated_dsn, run_id=run_id) == []
    assert _customers_count_for_run(env.migrated_dsn, run_id=run_id) == 0


# --- Test B: under-threshold quarantine still SUCCEEDS ---------------------


def test_quarantine_under_threshold_succeeds_and_persists_both(env: _Env) -> None:
    """Some QUARANTINE/REJECT rows under the breaker's threshold -> SUCCEEDED, not FAIL."""
    csv_bytes = _csv_bytes(good_count=9, bad_count=1, start_id=9_401_001)
    dataset_id, run_id, file_id, batch_id, object_key = _seed_pending_run(
        env,
        key_suffix="quarantine_under_threshold",
        csv_bytes=csv_bytes,
    )
    del dataset_id
    ctx = _make_ctx(
        env,
        run_id=run_id,
        file_id=file_id,
        batch_id=batch_id,
        object_key=object_key,
        idempotency_key="wiring_quarantine_under_threshold:1",
        rejection_rate_threshold=0.5,  # 1 bad / 10 total = 10% <= 50%
    )

    receipt = run_ingest(ctx)

    assert receipt.status == "SUCCEEDED"
    assert receipt.rows_quarantined == 1

    rejected_rows = _fetch_rejected_records(env.migrated_dsn, run_id=run_id)
    assert len(rejected_rows) == 1
    assert rejected_rows[0][1] == "COMPLETENESS_VIOLATION"

    assert _customers_count_for_run(env.migrated_dsn, run_id=run_id) == 9

    validation_rows = _fetch_validation_results(env.migrated_dsn, run_id=run_id)
    rule_ids = {row[0] for row in validation_rows}
    assert "rejection_rate_circuit_breaker" in rule_ids


# --- Test C: D-05/D-01/D-02/D-03 -- a backfill run resolves the batch's ----
# --- PENDING rejected_records rows, through run_ingest itself, not the -----
# --- method directly -------------------------------------------------------


def test_backfill_run_resolves_the_batch_pending_rejects(env: _Env) -> None:
    """A SUCCEEDED run sharing a batch_id with 2 seeded PENDING rows flips them to REDRIVEN."""
    dataset_id = env.metadata.get_or_create_dataset("wiring_backfill_resolution")
    config_version_id = _insert_config_version(env.migrated_dsn, dataset_id=dataset_id)
    file_id = env.metadata.create_file(
        dataset_id=dataset_id,
        object_uri="s3://raw/wiring/backfill_resolution.csv",
        content_sha256=hashlib.sha256(b"wiring-backfill-resolution").digest(),
        hash_version=1,
        size_bytes=10,
        filename="backfill_resolution.csv",
        status="DISCOVERED",
    )
    batch_id = env.metadata.create_batch(
        dataset_id=dataset_id,
        batch_key="wiring_backfill_resolution:2026-08-17:1",
        status="OPEN",
    )

    # Simulate a PRIOR run that rejected 2 rows against this SAME batch_id.
    prior_run_id = env.metadata.create_ingestion_run(
        idempotency_key="wiring_backfill_resolution:prior",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        status="FAILED",
        file_id=file_id,
        batch_id=batch_id,
    )
    _insert_pending_reject(
        env.migrated_dsn,
        run_id=prior_run_id,
        file_id=file_id,
        batch_id=batch_id,
        source_row_number=1,
    )
    _insert_pending_reject(
        env.migrated_dsn,
        run_id=prior_run_id,
        file_id=file_id,
        batch_id=batch_id,
        source_row_number=2,
    )

    # The "backfill" run: a SECOND run_ingest call sharing the SAME batch_id
    # (D-01 -- no separate redrive mechanism), an all-good row set (no new
    # rejections of its own, so it SUCCEEDS).
    csv_bytes = _csv_bytes(good_count=3, bad_count=0, start_id=9_402_001)
    object_key = "customers/backfill_resolution_redrive.csv"
    env.s3_client.put_object(Bucket=env.scratch_bucket, Key=object_key, Body=csv_bytes)
    backfill_run_id, _ = env.metadata.get_or_create_ingestion_run(
        idempotency_key="wiring_backfill_resolution:backfill",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        file_id=file_id,
        batch_id=batch_id,
    )
    ctx = _make_ctx(
        env,
        run_id=backfill_run_id,
        file_id=file_id,
        batch_id=batch_id,
        object_key=object_key,
        idempotency_key="wiring_backfill_resolution:backfill",
        rejection_rate_threshold=0.5,
    )

    receipt = run_ingest(ctx)

    assert receipt.status == "SUCCEEDED"

    # D-03/D-05 live proof, through run_ingest itself: both seeded rows now
    # show REDRIVEN, resolved_by_run_id equal to the NEW run's run_id -- D-02
    # is proven by the fact publisher.publish() above ran completely
    # unchanged (the SAME merge/ON CONFLICT strategy every other run uses).
    state = _resolution_state(env.migrated_dsn, batch_id=batch_id)
    assert len(state) == 2
    for _row_number, resolution_type, resolved_by_run_id in state:
        assert resolution_type == "REDRIVEN"
        assert resolved_by_run_id == backfill_run_id


# --- Test D: VALID-04's MinIO-artifact half ---------------------------------


def test_report_artifact_matches_persisted_postgres_rows(env: _Env) -> None:
    """A SUCCEEDED run's report.json (at its own report_uri) mirrors its persisted meta rows."""
    csv_bytes = _csv_bytes(good_count=9, bad_count=1, start_id=9_403_001)
    dataset_id, run_id, file_id, batch_id, object_key = _seed_pending_run(
        env,
        key_suffix="minio_report_artifact",
        csv_bytes=csv_bytes,
    )
    del dataset_id
    ctx = _make_ctx(
        env,
        run_id=run_id,
        file_id=file_id,
        batch_id=batch_id,
        object_key=object_key,
        idempotency_key="wiring_minio_report_artifact:1",
        rejection_rate_threshold=0.5,
    )

    receipt = run_ingest(ctx)

    assert receipt.status == "SUCCEEDED"
    assert receipt.report_uri is not None
    expected_uri = f"s3://validated/customers/{run_id}/report.json"
    assert receipt.report_uri == expected_uri

    key = receipt.report_uri.removeprefix("s3://validated/")
    obj = env.s3_client.get_object(Bucket="validated", Key=key)
    report = json.loads(obj["Body"].read())

    assert report["run_id"] == run_id
    assert report["dataset"] == "customers"

    db_validation_rows = _fetch_validation_results(env.migrated_dsn, run_id=run_id)
    db_rule_ids = sorted(row[0] for row in db_validation_rows)
    report_rule_ids = sorted(finding["rule_id"] for finding in report["validation_results"])
    assert report_rule_ids == db_rule_ids

    db_rejected_rows = _fetch_rejected_records(env.migrated_dsn, run_id=run_id)
    db_error_types = sorted(row[1] for row in db_rejected_rows)
    report_error_types = sorted(record["error_type"] for record in report["rejected_records"])
    assert report_error_types == db_error_types
    assert len(report["rejected_records"]) == 1
