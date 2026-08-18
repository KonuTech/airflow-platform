"""Integration tests for ``dataplat.pipeline.run.stage_ingest`` (plan 08.1-10 Task 1).

Every test drives a real ``stage_ingest`` against real testcontainers
PostgreSQL + MinIO -- the exact claim/stage/quality-gate/promote
orchestration a real ``stage`` pod executes, using a real
``csv_processor.source.CsvSource`` (never a fake/in-memory source), mirroring
``test_run_ingest.py``'s own "prove the FULL, source-to-database path" intent
adapted to this plan's new terminal status.

``_seed_pending_run``/``_make_ctx``/``_Env`` mirror ``test_run_ingest.py``'s
own fixtures closely -- deliberately duplicated locally rather than imported,
matching this test suite's established per-file helper convention
(``test_publish_transaction_wiring.py``'s own module docstring names the same
precedent explicitly).

Every test uses its own, widely-separated ``customer_id`` range (9_500_0xx
and up) -- ``normalized.customers``/``staging.customers`` are shared,
session-scoped tables across the whole ``tests/integration/`` collection, and
``test_run_ingest.py`` already occupies 9_100_0xx-9_200_0xx,
``test_publish_transaction_wiring.py`` occupies 9_400_0xx -- so collisions
are a real risk, not a theoretical one.
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
from dataplat.pipeline.run import stage_ingest
from dataplat.storage.db import create_pool
from dataplat.storage.objectstore import S3ObjectStore

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration

_BUCKET = "stage-ingest-test"
_VALIDATED_BUCKET = "validated"
_CSV_HEADER = "customer_id,name,country,birth_date,event_ts\n"


def _row(customer_id: int, *, empty_name: bool = False) -> str:
    """One well-formed customers CSV row, optionally with an empty `name` (a completeness violation)."""  # noqa: E501, W505
    name = "" if empty_name else f"Name{customer_id}"
    return f"{customer_id},{name},US,1990-01-01,2026-01-01T00:00:00+00:00\n"


def _csv_bytes(rows: int, *, start_id: int) -> bytes:
    """Build a tiny, well-formed customers CSV: `rows` records starting at `start_id`."""
    lines = [_CSV_HEADER, *(_row(start_id + offset) for offset in range(rows))]
    return "".join(lines).encode("utf-8")


def _csv_bytes_with_bad_rows(*, good_count: int, bad_count: int, start_id: int) -> bytes:
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


def _make_config() -> DatasetConfig:
    """A plain `customers`-shaped `DatasetConfig`, no quality rules -- mirrors `test_run_ingest.py`."""  # noqa: E501, W505
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
    )


def _make_config_with_circuit_breaker(*, rejection_rate_threshold: float) -> DatasetConfig:
    """`_make_config()` plus a real completeness rule + run-level circuit breaker.

    Mirrors `test_publish_transaction_wiring.py`'s own `_make_config` shape --
    used only by Test 2 below, which needs a genuine `QualityThresholdExceeded`
    trip.
    """
    base = _make_config()
    return base.model_copy(
        update={
            "quality": QualityConfig(
                rules=[
                    QualityRuleConfig(
                        rule_id="stage_ingest_name_completeness",
                        rule_type="QUALITY_COMPLETENESS",
                        strategy="REJECT_RECORD",
                        column="name",
                    ),
                ],
                rejection_rate_threshold=rejection_rate_threshold,
            ),
        },
    )


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Insert a synthetic `meta.config_versions` row -- mirrors `test_run_ingest.py`."""
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


def _seed_pending_run(env: _Env, *, key_suffix: str, csv_bytes: bytes) -> tuple[int, int, int, str]:
    """Seed dataset/config/file/batch/PENDING-run and upload `csv_bytes`.

    Returns:
        `(run_id, file_id, batch_id, object_key)`.
    """
    dataset_id = env.metadata.get_or_create_dataset(f"stage_ingest_{key_suffix}")
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
        batch_key=f"{key_suffix}:2026-08-18:1",
        status="OPEN",
    )
    run_id, _ = env.metadata.get_or_create_ingestion_run(
        idempotency_key=f"stage_ingest_{key_suffix}:1",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        file_id=file_id,
        batch_id=batch_id,
    )
    return run_id, file_id, batch_id, object_key


def _make_ctx(  # noqa: PLR0913 -- one keyword per identity/config value, mirrors test_publish_transaction_wiring.py's own shape
    env: _Env,
    *,
    run_id: int,
    file_id: int,
    batch_id: int,
    object_key: str,
    key_suffix: str,
    config: DatasetConfig,
) -> PipelineContext:
    """Build a `PipelineContext` wired to a real `CsvSource` over `env`'s scratch bucket."""
    return PipelineContext(
        run=RunContext(
            run_id=run_id,
            idempotency_key=f"stage_ingest_{key_suffix}:1",
            file_id=file_id,
            batch_id=batch_id,
        ),
        config=config,
        metadata=env.metadata,
        objects=env.objects,
        db=env.pool,
        log=get_logger(),
        source=CsvSource(bucket=env.scratch_bucket, key=object_key),
    )


def _seed_and_build_ctx(
    env: _Env,
    *,
    key_suffix: str,
    csv_bytes: bytes,
    config: DatasetConfig | None = None,
) -> tuple[PipelineContext, int, int, int]:
    """`_seed_pending_run` + `_make_ctx` in one call -- every test's setup boils down to this."""
    run_id, file_id, batch_id, object_key = _seed_pending_run(
        env,
        key_suffix=key_suffix,
        csv_bytes=csv_bytes,
    )
    ctx = _make_ctx(
        env,
        run_id=run_id,
        file_id=file_id,
        batch_id=batch_id,
        object_key=object_key,
        key_suffix=key_suffix,
        config=config if config is not None else _make_config(),
    )
    return ctx, run_id, file_id, batch_id


def _read_run_status(migrated_dsn: str, run_id: int) -> str:
    with psycopg.connect(migrated_dsn) as conn:
        row = conn.execute(
            "SELECT status FROM meta.ingestion_runs WHERE run_id = %s",
            (run_id,),
        ).fetchone()
    assert row is not None
    return str(row[0])


def _durable_bronze_count_for_run(migrated_dsn: str, run_id: int) -> int:
    """Count `staging.customers` (the durable, cumulative bronze table, migration 0022) rows."""
    with psycopg.connect(migrated_dsn) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM staging.customers WHERE _run_id = %s",
            (run_id,),
        ).fetchone()
    assert row is not None
    return int(row[0])


def _scratch_table_exists(migrated_dsn: str, run_id: int) -> bool:
    with psycopg.connect(migrated_dsn) as conn:
        row = conn.execute(
            "SELECT to_regclass(%s)",
            (f"staging.customers__r{run_id}",),
        ).fetchone()
    assert row is not None
    return row[0] is not None


# --- Behavior 1: a normal file reaches STAGED, durable bronze holds it -----


def test_normal_file_reaches_staged_with_durable_bronze_rows_and_no_scratch_table(
    env: _Env,
) -> None:
    ctx, run_id, _file_id, _batch_id = _seed_and_build_ctx(
        env,
        key_suffix="happy",
        csv_bytes=_csv_bytes(3, start_id=9_500_001),
    )

    receipt = stage_ingest(ctx)

    assert receipt.status == "STAGED"
    assert receipt.run_id == run_id
    assert receipt.rows_read == 3
    assert receipt.rows_loaded == 0  # staging never writes to gold
    assert receipt.rows_invalid == 0

    assert _read_run_status(env.migrated_dsn, run_id) == "STAGED"
    assert _durable_bronze_count_for_run(env.migrated_dsn, run_id) == 3
    assert not _scratch_table_exists(env.migrated_dsn, run_id)


# --- Behavior 2: the circuit breaker trips -> never STAGED, never bronze ---


def test_circuit_breaker_trip_never_reaches_staged_or_durable_bronze(env: _Env) -> None:
    """Test 1's exact D-11 mirror at staging time: `QualityThresholdExceeded` rolls back
    everything -- the scratch buffer's COPY AND the durable-bronze promotion, since
    `promote_to_durable_bronze` is never even reached on this path.
    """
    csv_bytes = _csv_bytes_with_bad_rows(good_count=9, bad_count=1, start_id=9_501_001)
    ctx, run_id, _file_id, _batch_id = _seed_and_build_ctx(
        env,
        key_suffix="circuit_breaker_trip",
        csv_bytes=csv_bytes,
        config=_make_config_with_circuit_breaker(rejection_rate_threshold=0.01),
    )

    with pytest.raises(QualityThresholdExceeded):
        stage_ingest(ctx)

    status = _read_run_status(env.migrated_dsn, run_id)
    assert status != "STAGED"
    assert status in ("RUNNING", "FAILED")
    assert _durable_bronze_count_for_run(env.migrated_dsn, run_id) == 0
    assert not _scratch_table_exists(env.migrated_dsn, run_id)


# --- Behavior 3: a second call for an already-STAGED run is SKIPPED_DUPLICATE --


def test_second_call_for_an_already_staged_run_returns_skipped_duplicate(env: _Env) -> None:
    """Decision (see this plan's SUMMARY): `_skipped_receipt` treats `'STAGED'` the same as
    `'SUCCEEDED'` -- both mean "this run's own work already genuinely completed".
    `claim_ingestion_run`'s claimability predicate already excludes `'STAGED'` the same way
    it excludes `'SUCCEEDED'` (neither `PENDING`/`FAILED`, nor `RUNNING` with an expired
    lease), so a second `stage_ingest` call for the same idempotency key is refused a claim
    identically either way.
    """
    ctx, run_id, _file_id, _batch_id = _seed_and_build_ctx(
        env,
        key_suffix="already_staged",
        csv_bytes=_csv_bytes(1, start_id=9_502_001),
    )

    first = stage_ingest(ctx)
    assert first.status == "STAGED"

    second = stage_ingest(ctx)
    assert second.status == "SKIPPED_DUPLICATE"
    assert second.run_id == run_id
    assert second.rows_loaded == 0

    # No second promotion happened -- durable bronze still shows exactly the
    # first call's own 1 row, never 2.
    assert _durable_bronze_count_for_run(env.migrated_dsn, run_id) == 1
