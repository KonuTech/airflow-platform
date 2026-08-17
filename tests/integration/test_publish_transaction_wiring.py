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


def _row_with_event_ts(customer_id: int, event_ts: str) -> str:
    """One well-formed customers CSV row with an explicit `event_ts` (CR-01's conflict-guard proof)."""  # noqa: E501, W505
    return f"{customer_id},Name{customer_id},US,1990-01-01,{event_ts}\n"


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
    """Get-or-insert a synthetic, CURRENT `meta.config_versions` row directly via SQL.

    `meta.config_versions` enforces at most one CURRENT (`valid_to IS NULL`) row per
    `dataset_id` (migration 0001's `uq_config_versions_current_per_dataset` partial unique
    index). This file's original per-test-unique-dataset convention never collided with that,
    but Test C/C2 below now share the SAME "customers" dataset_id (D-23's dataset-scoping
    requirement, `meta.batches.dataset_id` join) -- a second call for that same dataset_id must
    REUSE the first call's row, not attempt a second CURRENT insert.
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


def _insert_pending_reject(  # noqa: PLR0913 -- matches meta.rejected_records' own seeded column set
    migrated_dsn: str,
    *,
    run_id: int,
    file_id: int,
    batch_id: int,
    source_row_number: int,
    business_key: str | None = None,
) -> None:
    """Seed one PENDING `meta.rejected_records` row directly via SQL (mirrors `test_backfill_resolution.py`)."""  # noqa: E501, W505
    with psycopg.connect(migrated_dsn) as conn:
        conn.execute(
            """
            INSERT INTO meta.rejected_records (
                run_id, file_id, batch_id, source_row_number, raw_line,
                error_type, error_message, business_key
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                file_id,
                batch_id,
                source_row_number,
                f"row-{source_row_number}",
                "RAGGED_ROW",
                "seeded directly for D-05/D-23 backfill-resolution proof",
                business_key,
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


# --- Test C: D-05/D-01/D-02/D-03/D-23 -- a backfill run resolves an --------
# --- EARLIER, DIFFERENT batch's PENDING rejected_records rows, through -----
# --- run_ingest itself, not the method directly -----------------------------


def test_backfill_run_resolves_the_batch_pending_rejects(env: _Env) -> None:
    """A SUCCEEDED run publishing a business key resolves an EARLIER, DIFFERENT batch's PENDING
    reject sharing that business key -- the exact VALID-08 gap 08-VERIFICATION.md confirmed
    live: `discover_files`'s `batch_key` is a pure function of a file's `content_sha256`, so a
    content-differing correction of a previously-rejected row always discovers under a NEW
    `batch_id`. Only a `(dataset_id, business_key)`-scoped resolve call (D-23) -- never a
    strictly `batch_id`-scoped one -- can ever reach the ORIGINAL batch's PENDING row.
    """
    # D-23's resolution predicate joins on `meta.batches.dataset_id` --
    # everything seeded here MUST belong to the SAME dataset `ctx.config.dataset`
    # ("customers", `_make_config`'s own hardcoded value) resolves to via
    # `get_or_create_dataset` inside `run_ingest` itself, not a distinct
    # `wiring_*`-named dataset (the old batch_id-only resolve never joined
    # on dataset_id, so this never mattered before D-23).
    dataset_id = env.metadata.get_or_create_dataset("customers")
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

    # The seeded PENDING rejects live under a SEPARATE, distinct batch_id
    # from the "backfill" run's own batch below -- this IS the
    # content-differing-correction shape: a corrected file discovers under a
    # NEW batch, yet the OLD batch's PENDING row must still resolve.
    original_batch_id = env.metadata.create_batch(
        dataset_id=dataset_id,
        batch_key="wiring_backfill_resolution:original:1",
        status="OPEN",
    )

    # Simulate a PRIOR run that rejected 2 rows against the ORIGINAL batch,
    # each carrying the business_key of a customer_id the "backfill" run
    # below WILL publish.
    prior_run_id = env.metadata.create_ingestion_run(
        idempotency_key="wiring_backfill_resolution:prior",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        status="FAILED",
        file_id=file_id,
        batch_id=original_batch_id,
    )
    _insert_pending_reject(
        env.migrated_dsn,
        run_id=prior_run_id,
        file_id=file_id,
        batch_id=original_batch_id,
        source_row_number=1,
        business_key="9402001",
    )
    _insert_pending_reject(
        env.migrated_dsn,
        run_id=prior_run_id,
        file_id=file_id,
        batch_id=original_batch_id,
        source_row_number=2,
        business_key="9402002",
    )

    # The "backfill" run: a corrected file discovering under a NEW, SEPARATE
    # batch_id from the original -- publishing customer_ids
    # 9402001-9402003, two of which match the seeded rejects' business_key.
    backfill_batch_id = env.metadata.create_batch(
        dataset_id=dataset_id,
        batch_key="wiring_backfill_resolution:corrected:1",
        status="OPEN",
    )
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
        batch_id=backfill_batch_id,
    )
    ctx = _make_ctx(
        env,
        run_id=backfill_run_id,
        file_id=file_id,
        batch_id=backfill_batch_id,
        object_key=object_key,
        idempotency_key="wiring_backfill_resolution:backfill",
        rejection_rate_threshold=0.5,
    )

    receipt = run_ingest(ctx)

    assert receipt.status == "SUCCEEDED"

    # D-03/D-05/D-23 live proof, through run_ingest itself: the ORIGINAL
    # batch's 2 seeded rows now show REDRIVEN, resolved_by_run_id equal to
    # the NEW, DIFFERENT batch's own run_id -- proving business-key-scoped
    # resolution crosses batch boundaries. D-02 is proven by the fact
    # publisher.publish() above ran completely unchanged (the SAME
    # merge/ON CONFLICT strategy every other run uses).
    state = _resolution_state(env.migrated_dsn, batch_id=original_batch_id)
    assert len(state) == 2
    for _row_number, resolution_type, resolved_by_run_id in state:
        assert resolution_type == "REDRIVEN"
        assert resolved_by_run_id == backfill_run_id


# --- Test C2: CR-01 (phase-08 code review) -- a backfill run must resolve --
# --- ONLY a prior run's PENDING rejects, never its own fresh ones ----------


def test_backfill_run_never_resolves_its_own_fresh_rejects(env: _Env) -> None:
    """The exact CR-01 regression, restated under D-23's business-key-scoped predicate: a run
    must not immediately REDRIVEN its own rejects.

    `resolve_rejected_records_for_business_keys`'s `WHERE ... business_key = ANY(%s) AND
    resolution_type = 'PENDING'` has no run-id exclusion, so calling it AFTER
    `record_rejected_records` for the SAME run would flip this run's own just-inserted rejects
    to REDRIVEN too, IF their business_key happened to appear in `published_business_keys` --
    but it structurally never can: a row this run rejects was never staged (`CompletenessRule`
    rejects it BEFORE staging), so its business_key was never published this run and the
    `SELECT DISTINCT` over the staging table can never surface it. Also proves cross-batch
    resolution still works here too: the prior run's seeded reject, under a SEPARATE batch_id
    but sharing a business_key that IS one of the backfill file's own published customer_ids,
    still resolves -- distinguished from the run's OWN fresh reject (a business_key that was
    NEVER published, since that row failed CompletenessRule before ever reaching staging).
    """
    # Same D-23 dataset-scoping requirement as Test C above -- seeded rows
    # must belong to the SAME "customers" dataset ctx.config.dataset resolves
    # to, not a distinct wiring_*-named one.
    dataset_id = env.metadata.get_or_create_dataset("customers")
    config_version_id = _insert_config_version(env.migrated_dsn, dataset_id=dataset_id)
    file_id = env.metadata.create_file(
        dataset_id=dataset_id,
        object_uri="s3://raw/wiring/backfill_own_rejects.csv",
        content_sha256=hashlib.sha256(b"wiring-backfill-own-rejects").digest(),
        hash_version=1,
        size_bytes=10,
        filename="backfill_own_rejects.csv",
        status="DISCOVERED",
    )

    # The seeded PENDING reject lives under a SEPARATE batch_id from the
    # backfill run's own below -- proving cross-batch resolution still
    # works here too, not just the CR-01 non-self-resolution guarantee.
    original_batch_id = env.metadata.create_batch(
        dataset_id=dataset_id,
        batch_key="wiring_backfill_own_rejects:original:1",
        status="OPEN",
    )

    # Simulate a PRIOR run that rejected 1 row against the ORIGINAL batch,
    # with a business_key matching one of the backfill run's own GOOD,
    # published customer_ids (9404001-9404009) below -- proving cross-batch
    # resolution -- deliberately NOT customer_id 9404010, the backfill run's
    # own rejected row (that distinction is the CR-01 proof further down).
    prior_run_id = env.metadata.create_ingestion_run(
        idempotency_key="wiring_backfill_own_rejects:prior",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        status="FAILED",
        file_id=file_id,
        batch_id=original_batch_id,
    )
    _insert_pending_reject(
        env.migrated_dsn,
        run_id=prior_run_id,
        file_id=file_id,
        batch_id=original_batch_id,
        source_row_number=1,
        business_key="9404005",
    )

    # The "backfill" run: a NEW, SEPARATE batch_id from the original --
    # publishes customer_ids 9404001-9404009 and ALSO rejects one row of
    # its own (customer_id 9404010, empty name -- under threshold, so it
    # still SUCCEEDS) -- the exact scenario CR-01 flags.
    backfill_batch_id = env.metadata.create_batch(
        dataset_id=dataset_id,
        batch_key="wiring_backfill_own_rejects:backfill:1",
        status="OPEN",
    )
    csv_bytes = _csv_bytes(good_count=9, bad_count=1, start_id=9_404_001)
    object_key = "customers/backfill_own_rejects_redrive.csv"
    env.s3_client.put_object(Bucket=env.scratch_bucket, Key=object_key, Body=csv_bytes)
    backfill_run_id, _ = env.metadata.get_or_create_ingestion_run(
        idempotency_key="wiring_backfill_own_rejects:backfill",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        file_id=file_id,
        batch_id=backfill_batch_id,
    )
    ctx = _make_ctx(
        env,
        run_id=backfill_run_id,
        file_id=file_id,
        batch_id=backfill_batch_id,
        object_key=object_key,
        idempotency_key="wiring_backfill_own_rejects:backfill",
        rejection_rate_threshold=0.5,
    )

    receipt = run_ingest(ctx)

    assert receipt.status == "SUCCEEDED"

    # The PRIOR run's row, under the ORIGINAL, DIFFERENT batch: resolved by
    # the backfill run -- cross-batch resolution still holds here too.
    original_state = _resolution_state(env.migrated_dsn, batch_id=original_batch_id)
    assert len(original_state) == 1
    _row_number, prior_resolution_type, prior_resolved_by = original_state[0]
    assert prior_resolution_type == "REDRIVEN"
    assert prior_resolved_by == backfill_run_id

    # This run's OWN fresh reject, under the backfill run's OWN, separate
    # batch: a business key that was never published (this row failed
    # CompletenessRule before ever reaching staging) can never be resolved
    # -- the CR-01 regression proof, now grounded in business-key identity
    # rather than batch_id happenstance.
    own_state = _resolution_state(env.migrated_dsn, batch_id=backfill_batch_id)
    assert len(own_state) == 1
    _own_row_number, own_resolution_type, own_resolved_by = own_state[0]
    assert own_resolution_type == "PENDING"
    assert own_resolved_by is None


# --- Test C3: CR-01 (phase-08 code review) -- a business key that merely ---
# --- STAGED, but that the Publisher's own conflict-guard left "locked but --
# --- unchanged", must NOT resolve its PENDING reject ------------------------


def test_staged_but_conflict_guard_blocked_business_key_stays_pending(env: _Env) -> None:
    """The exact CR-01 false-positive-resolution path, closed.

    Reproduces the review's concrete scenario: a row for `customer_id=9405001` is rejected
    under an ORIGINAL batch for an unrelated reason, `PENDING`, with `business_key="9405001"`.
    Independently, a *different*, legitimately-newer row for the SAME `customer_id`
    (`event_ts=T3`) is ALREADY published (seeded directly, exactly as if an entirely separate
    prior run had legitimately published it -- deliberately NOT run through `run_ingest`
    itself here, since that run's own resolve call would -- correctly, by D-23's own
    business-key-scoped design -- resolve the seeded PENDING reject the moment it actually
    publishes, before this test even reaches its own CR-01 scenario). An operator then uploads
    a "corrected" file that fixes the original violation for the *original*, OLDER `event_ts=T2`
    row -- it now survives streaming validation and stages. `MergePublisher`'s own
    `WHERE ... AND EXCLUDED.event_ts >= normalized.customers.event_ts` conflict guard evaluates
    `false` (`T2 < T3`): the row is "locked but unchanged", nothing is written to
    `normalized.customers`. Before CR-01's fix, `published_business_keys` was read from the
    staging table itself, so `"9405001"` appeared in it regardless -- the original `PENDING`
    reject flipped to `REDRIVEN` even though the target row was never actually corrected. After
    the fix, `published_business_keys` comes from `MergePublisher`'s own `RETURNING customer_id`
    -- populated ONLY by rows the `INSERT ... ON CONFLICT` statement actually affected -- so the
    conflict-guard-blocked row's business key never reaches the resolution call at all, and the
    original reject must still show `PENDING`.
    """
    dataset_id = env.metadata.get_or_create_dataset("customers")
    config_version_id = _insert_config_version(env.migrated_dsn, dataset_id=dataset_id)
    file_id = env.metadata.create_file(
        dataset_id=dataset_id,
        object_uri="s3://raw/wiring/conflict_guard_blocked.csv",
        content_sha256=hashlib.sha256(b"wiring-conflict-guard-blocked").digest(),
        hash_version=1,
        size_bytes=10,
        filename="conflict_guard_blocked.csv",
        status="DISCOVERED",
    )

    # The seeded PENDING reject represents the ORIGINAL, older-event_ts row
    # that was rejected for an unrelated reason (e.g. a pattern violation),
    # under a SEPARATE batch_id from either run below.
    original_batch_id = env.metadata.create_batch(
        dataset_id=dataset_id,
        batch_key="wiring_conflict_guard_blocked:original:1",
        status="OPEN",
    )
    prior_run_id = env.metadata.create_ingestion_run(
        idempotency_key="wiring_conflict_guard_blocked:prior",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        status="FAILED",
        file_id=file_id,
        batch_id=original_batch_id,
    )
    _insert_pending_reject(
        env.migrated_dsn,
        run_id=prior_run_id,
        file_id=file_id,
        batch_id=original_batch_id,
        source_row_number=1,
        business_key="9405001",
    )

    # Step 1: a DIFFERENT, unrelated, legitimately-newer row for the SAME
    # customer_id (event_ts=T3) is ALREADY published -- seeded directly via
    # SQL (see docstring for why this is deliberately NOT a real
    # `run_ingest` call), reusing `prior_run_id`/`file_id`/`original_batch_id`
    # as its lineage FKs purely because they are already valid rows this
    # test seeded -- their identity is otherwise irrelevant here, this row's
    # `event_ts` is the only thing this test's assertion depends on.
    with psycopg.connect(env.migrated_dsn) as conn:
        conn.execute(
            """
            INSERT INTO normalized.customers (
                customer_id, name, country, birth_date, event_ts,
                _run_id, _file_id, _batch_id, _source_row_number,
                _record_hash, _record_hash_version
            ) VALUES (
                9405001, 'NewerUnrelated', 'US', '1990-01-01',
                '2026-06-01T00:00:00+00:00', %s, %s, %s, 1, %s, 1
            )
            """,
            (
                prior_run_id,
                file_id,
                original_batch_id,
                hashlib.sha256(b"newer-unrelated-content").digest(),
            ),
        )
        conn.commit()

    # Step 2: the "corrected" file -- fixing the ORIGINAL violation -- stages
    # an OLDER event_ts (T2 < T3) for the SAME customer_id. It survives
    # streaming validation (no completeness violation this time) and lands
    # in staging, but MergePublisher's conflict guard must leave the
    # existing (newer) target row untouched.
    backfill_batch_id = env.metadata.create_batch(
        dataset_id=dataset_id,
        batch_key="wiring_conflict_guard_blocked:backfill:1",
        status="OPEN",
    )
    older_csv = (
        _CSV_HEADER + _row_with_event_ts(9_405_001, "2025-01-01T00:00:00+00:00")
    ).encode("utf-8")
    backfill_object_key = "customers/conflict_guard_blocked_backfill.csv"
    env.s3_client.put_object(
        Bucket=env.scratch_bucket,
        Key=backfill_object_key,
        Body=older_csv,
    )
    backfill_run_id, _ = env.metadata.get_or_create_ingestion_run(
        idempotency_key="wiring_conflict_guard_blocked:backfill",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        file_id=file_id,
        batch_id=backfill_batch_id,
    )
    backfill_ctx = _make_ctx(
        env,
        run_id=backfill_run_id,
        file_id=file_id,
        batch_id=backfill_batch_id,
        object_key=backfill_object_key,
        idempotency_key="wiring_conflict_guard_blocked:backfill",
        rejection_rate_threshold=0.5,
    )
    backfill_receipt = run_ingest(backfill_ctx)
    assert backfill_receipt.status == "SUCCEEDED"

    # The publish itself was correctly a no-op: the conflict guard left the
    # newer row in place, nothing written by the backfill run.
    assert _customers_count_for_run(env.migrated_dsn, run_id=backfill_run_id) == 0

    # The CR-01 proof: the original reject must NOT have flipped to
    # REDRIVEN -- the business key only staged, it was never actually
    # published by the backfill run.
    original_state = _resolution_state(env.migrated_dsn, batch_id=original_batch_id)
    assert len(original_state) == 1
    _row_number, resolution_type, resolved_by_run_id = original_state[0]
    assert resolution_type == "PENDING"
    assert resolved_by_run_id is None


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
