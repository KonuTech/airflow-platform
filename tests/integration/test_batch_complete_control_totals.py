"""Integration test: VALID-06's control-total comparison proven end to end (plan 09-07 Task 2).

Extends 09-07-PLAN.md Task 1's own `record_reconciliation`-level proof (`test_reconciliation.py`'s
`raw_bronze` tests) one layer up: a genuine `_BATCH_COMPLETE` marker object, uploaded to real
MinIO, parsed via `dataplat.validate.batch_complete_manifest.parse_batch_complete_manifest` (the
exact function `dataplat.discovery._apply_batch_complete_marker_gate` and the `stage` CLI command
already use, per 09-03-PLAN.md), threaded into a real `RunContext.batch_expected_row_count`/
`batch_expected_checksum`, and run through a real `stage_ingest` -- `meta.reconciliation_results`'
`hop='raw_bronze'` row is then queried and asserted on, proving the WHOLE claim: "a source-declared
control total is compared against the loaded target and any discrepancy is recorded, never
blocking" (09-07-PLAN.md's own `<success_criteria>`).

Dataset-name note (mirrors `test_reconciliation.py`'s own `raw_bronze` section): `stage_ingest`'s
own `_TARGET_COLUMNS_BY_DATASET` lookup (`dataplat.pipeline.run`) only has entries for
`"customers"`/`"orders"` -- there is no generic "any dataset" staging path today, so every
`DatasetConfig` below still declares `dataset="customers"`. The plan's own "a throwaway test
dataset config, not customers/orders themselves" instruction is satisfied by this file's
`_make_marker_config` never touching the real, committed `customers.yaml` -- it is a fresh,
locally-constructed `DatasetConfig` Python object, registered under its own, per-test
`meta.datasets` row (`batch_complete_control_totals_{key_suffix}`) via `get_or_create_dataset`,
entirely independent of the production `customers`/`orders` configs Test 3 below explicitly
contrasts against.

Every test uses its own widely-separated `customer_id` range (9_700_0xx) -- `staging.customers`
is a shared, session-scoped table across the whole `tests/integration/` collection, and other
files already occupy 9_100_0xx-9_600_3xx / 9994xxx.
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
    SourceConfig,
)
from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.models.identity import RunContext
from dataplat.observability.logging import get_logger
from dataplat.pipeline.protocol import PipelineContext
from dataplat.pipeline.run import stage_ingest
from dataplat.storage.db import create_pool
from dataplat.storage.objectstore import S3ObjectStore
from dataplat.validate.batch_complete_manifest import parse_batch_complete_manifest

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration

_BUCKET = "batch-complete-control-totals-test"
_VALIDATED_BUCKET = "validated"
_MARKER_SUFFIX = "_BATCH_COMPLETE"
_CSV_HEADER = "customer_id,name,country,birth_date,event_ts\n"


def _row(customer_id: int) -> str:
    return f"{customer_id},Name{customer_id},US,1990-01-01,2026-01-01T00:00:00+00:00\n"


def _csv_bytes(rows: int, *, start_id: int) -> bytes:
    lines = [_CSV_HEADER, *(_row(start_id + offset) for offset in range(rows))]
    return "".join(lines).encode("utf-8")


def _make_marker_config(*, key_suffix: str, batch_complete_marker: str | None) -> DatasetConfig:
    """A fresh, locally-constructed `customers`-shaped `DatasetConfig` -- never the real,
    committed `customers.yaml` (which sets `batch_complete_marker: None`, matching Test 3's own
    "both customers/orders today" claim). `path` is unique per test (`key_suffix`) so this file's
    own marker/data objects never collide with a sibling test's.
    """
    return DatasetConfig(
        dataset="customers",
        config_schema_version=1,
        source=SourceConfig(
            type="csv",
            bucket=_BUCKET,
            path=f"{key_suffix}/",
            change_semantics="snapshot",
            duplicate_policy="skip",
            batch_complete_marker=batch_complete_marker,
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


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Get-or-insert a synthetic, CURRENT `meta.config_versions` row.

    Every test in this file registers under the SAME `dataset_id` ("customers" --
    `stage_ingest`'s `_TARGET_COLUMNS_BY_DATASET` lookup forces it), unlike
    `test_stage_ingest.py`'s own per-test-fresh-dataset shape -- so, mirroring
    `test_reconciliation.py`'s own `_insert_config_version`, this checks for an
    existing CURRENT version first rather than a plain `INSERT`, which would
    otherwise raise `UniqueViolation` against `uq_config_versions_dataset_hash`
    on the second and every subsequent test.
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


@dataclass
class _Env:
    metadata: PostgresMetadataRepository
    objects: S3ObjectStore
    pool: Any
    migrated_dsn: str
    s3_client: Any
    bucket: str


@pytest.fixture
def _pool(migrated_dsn: str) -> Iterator[Any]:
    opened_pool = create_pool(migrated_dsn)
    opened_pool.open(wait=True)
    try:
        yield opened_pool
    finally:
        opened_pool.close()


@pytest.fixture
def _bucket(s3_client: Any) -> str:
    existing = {bucket["Name"] for bucket in s3_client.list_buckets().get("Buckets", [])}
    if _BUCKET not in existing:
        s3_client.create_bucket(Bucket=_BUCKET)
    return _BUCKET


@pytest.fixture
def _validated_bucket(s3_client: Any) -> str:
    """`_apply_staging_quality_gate_and_persist` writes its report to `s3://validated/...`
    unconditionally -- must exist before `stage_ingest` runs (mirrors `test_stage_ingest.py`'s
    own fixture).
    """
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
    _bucket: str,
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
        bucket=_bucket,
    )


def _seed_and_stage(
    env: _Env,
    *,
    key_suffix: str,
    csv_bytes: bytes,
    batch_complete_marker: str | None,
    marker_body: dict[str, object] | None,
) -> tuple[int, int]:
    """Upload `csv_bytes` (and, when `marker_body` is not `None`, a `_BATCH_COMPLETE` marker
    object) to real MinIO, seed dataset/config/file/batch/PENDING-run, parse the marker (mirroring
    `discovery._apply_batch_complete_marker_gate`/the `stage` CLI command's own translation) into
    `RunContext.batch_expected_row_count`/`batch_expected_checksum`, and run a real `stage_ingest`.

    Returns:
        `(dataset_id, file_id)` for the caller's own `meta.reconciliation_results` query.
    """
    config = _make_marker_config(key_suffix=key_suffix, batch_complete_marker=batch_complete_marker)
    dataset_id = env.metadata.get_or_create_dataset("customers")
    config_version_id = _insert_config_version(env.migrated_dsn, dataset_id=dataset_id)

    object_key = f"{key_suffix}/data.csv"
    env.s3_client.put_object(Bucket=env.bucket, Key=object_key, Body=csv_bytes)

    batch_expected_row_count: int | None = None
    batch_expected_checksum: str | None = None
    if marker_body is not None:
        assert batch_complete_marker is not None
        marker_key = config.source.path + batch_complete_marker
        env.s3_client.put_object(
            Bucket=env.bucket,
            Key=marker_key,
            Body=json.dumps(marker_body).encode("utf-8"),
        )
        # The SAME parse function `discovery._apply_batch_complete_marker_gate`/the `stage` CLI
        # command use (09-03-PLAN.md) -- a genuine parse of a genuine uploaded object, not a
        # hand-constructed manifest.
        with env.objects.get_object(env.bucket, marker_key) as marker_stream:
            manifest = parse_batch_complete_manifest(marker_stream.read(), marker_key=marker_key)
        batch_expected_row_count = manifest.expected_row_count
        batch_expected_checksum = manifest.expected_checksum

    file_id = env.metadata.create_file(
        dataset_id=dataset_id,
        object_uri=f"s3://{env.bucket}/{object_key}",
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
        idempotency_key=f"batch_complete_control_totals_{key_suffix}:1",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        file_id=file_id,
        batch_id=batch_id,
    )
    ctx = PipelineContext(
        run=RunContext(
            run_id=run_id,
            idempotency_key=f"batch_complete_control_totals_{key_suffix}:1",
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
        source=CsvSource(bucket=env.bucket, key=object_key),
    )

    receipt = stage_ingest(ctx)
    assert receipt.status == "STAGED"
    return dataset_id, file_id


def _read_raw_bronze_row(migrated_dsn: str, *, dataset_id: int, file_id: int) -> dict[str, Any]:
    with psycopg.connect(migrated_dsn) as conn:
        rows = conn.execute(
            """
            SELECT expected_row_count, control_total_discrepancy
              FROM meta.reconciliation_results
             WHERE dataset_id = %s AND file_id = %s AND hop = 'raw_bronze'
             ORDER BY reconciliation_id ASC
            """,
            (dataset_id, file_id),
        ).fetchall()
    assert len(rows) == 1
    return {"expected_row_count": rows[0][0], "control_total_discrepancy": rows[0][1]}


# --- Test 1: a marker whose expected_row_count matches the real row count ---


def test_marker_matching_real_row_count_produces_zero_control_total_discrepancy(env: _Env) -> None:
    dataset_id, file_id = _seed_and_stage(
        env,
        key_suffix="batch_complete_match",
        csv_bytes=_csv_bytes(6, start_id=9_700_001),
        batch_complete_marker=_MARKER_SUFFIX,
        marker_body={"expected_row_count": 6},
    )

    row = _read_raw_bronze_row(env.migrated_dsn, dataset_id=dataset_id, file_id=file_id)
    assert row["expected_row_count"] == 6
    assert row["control_total_discrepancy"] == 0


# --- Test 2: a marker declaring a WRONG expected_row_count -> non-zero -------
# --- discrepancy, run still reaches STAGED (record and continue) ------------


def test_marker_wrong_row_count_records_discrepancy_and_run_still_reaches_staged(
    env: _Env,
) -> None:
    real_row_count = 7
    wrong_delta = 3
    dataset_id, file_id = _seed_and_stage(
        env,
        key_suffix="batch_complete_wrong",
        csv_bytes=_csv_bytes(real_row_count, start_id=9_700_101),
        batch_complete_marker=_MARKER_SUFFIX,
        marker_body={"expected_row_count": real_row_count + wrong_delta},
    )

    row = _read_raw_bronze_row(env.migrated_dsn, dataset_id=dataset_id, file_id=file_id)
    assert row["expected_row_count"] == real_row_count + wrong_delta
    assert row["control_total_discrepancy"] == wrong_delta

    # `_seed_and_stage` already asserted `receipt.status == "STAGED"` -- this
    # second, independent read of `meta.ingestion_runs` proves the SAME thing
    # end to end from CLI-level staging, not just at the `record_reconciliation`
    # unit level Task 1 already proved (09-07-PLAN.md Task 2's own <behavior>
    # bullet 2 wording).
    with psycopg.connect(env.migrated_dsn) as conn:
        status_row = conn.execute(
            "SELECT status FROM meta.ingestion_runs WHERE file_id = %s",
            (file_id,),
        ).fetchone()
    assert status_row is not None
    assert status_row[0] == "STAGED"


# --- Test 3: no batch_complete_marker configured -> expected_row_count NULL -


def test_no_marker_configured_leaves_expected_row_count_null_throughout(env: _Env) -> None:
    dataset_id, file_id = _seed_and_stage(
        env,
        key_suffix="batch_complete_no_marker",
        csv_bytes=_csv_bytes(2, start_id=9_700_201),
        batch_complete_marker=None,
        marker_body=None,
    )

    row = _read_raw_bronze_row(env.migrated_dsn, dataset_id=dataset_id, file_id=file_id)
    assert row["expected_row_count"] is None
    assert row["control_total_discrepancy"] is None
