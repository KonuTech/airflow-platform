"""Integration tests for `discovery.py`'s D-18 idempotency-key/`replay_of_run_id` extension.

08.1-07 Task 3: `_PIPELINE_VERSION` bumps from an implicit `"1"` (the original
2-stage pipeline) to `"2"` (this phase's 3-stage pipeline), appended as a
sixth idempotency-key term. This file proves two things against a real
testcontainers PostgreSQL + MinIO: (1) the sixth term genuinely changes the
key, producing a real second `meta.ingestion_runs` row for the same file
under a different `_PIPELINE_VERSION`; (2) a historical file that already
`SUCCEEDED` under the OLD (5-term) formula becomes newly claimable under the
CURRENT formula, with `replay_of_run_id` correctly pointing back at the
original `SUCCEEDED` run.

Mirrors `tests/integration/test_discover_files.py`'s own fixture/helper
conventions (`_insert_config_version`, `repository`/`object_store`/`schema`
fixtures, module-scoped `_ensure_buckets`) -- duplicated locally rather than
imported, matching this test suite's existing per-file helper convention.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

import psycopg
import pytest

import dataplat.discovery as discovery_module
from dataplat.config.model import (
    BatchingConfig,
    ColumnContract,
    DatasetConfig,
    DeduplicationConfig,
    LoadConfig,
    SourceConfig,
)
from dataplat.discovery import discover_files
from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.schema.repository import SchemaRepository
from dataplat.storage.db import create_pool
from dataplat.storage.objectstore import S3ObjectStore

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration

_CONFIG_HASH = "config-hash-fixture"
_PROCESSOR_IMAGE = "sha256:test-processor-image"
_PROCESSOR_VERSION = "0.1.0"


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Insert a synthetic `meta.config_versions` row directly via SQL.

    Mirrors `tests/integration/test_discover_files.py`'s helper of the same
    name/shape -- duplicated locally rather than imported, matching this
    test suite's existing per-file helper convention.
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


def _make_config(*, path: str) -> DatasetConfig:
    """A locally-constructed `DatasetConfig`, scoped to one test's own `source.path` prefix.

    Mirrors `tests/integration/test_discover_files.py`'s own `_make_config`.
    """
    return DatasetConfig(
        dataset="customers",
        config_schema_version=1,
        source=SourceConfig(
            type="csv",
            bucket="raw",
            path=path,
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


@pytest.fixture(scope="module", autouse=True)
def _ensure_buckets(s3_client: Any) -> None:
    """Ensure `raw`/`metadata` exist on the shared session MinIO container -- idempotent."""
    existing = {bucket["Name"] for bucket in s3_client.list_buckets().get("Buckets", [])}
    for bucket in ("raw", "metadata"):
        if bucket not in existing:
            s3_client.create_bucket(Bucket=bucket)


@pytest.fixture
def object_store(minio_config: dict[str, str]) -> S3ObjectStore:
    """A real `S3ObjectStore`, built from the same credentials `s3_client` uses."""
    return S3ObjectStore(
        endpoint_url=f"http://{minio_config['endpoint']}",
        access_key=minio_config["access_key"],
        secret_key=minio_config["secret_key"],
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
def schema(migrated_dsn: str) -> Iterator[SchemaRepository]:
    """A `SchemaRepository` backed by an opened pool over the migrated database."""
    pool = create_pool(migrated_dsn)
    pool.open(wait=True)
    try:
        yield SchemaRepository(pool)
    finally:
        pool.close()


def test_pipeline_version_change_produces_a_distinct_idempotency_key_and_run(
    repository: PostgresMetadataRepository,
    object_store: S3ObjectStore,
    migrated_dsn: str,
    schema: SchemaRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two `discover_files` calls over the identical object, at two `_PIPELINE_VERSION`s, differ.

    Proves the sixth idempotency-key term is genuinely load-bearing: a
    different `_PIPELINE_VERSION` produces a different `idempotency_key`
    and a real second `meta.ingestion_runs` row for the SAME `file_id`.
    """
    dataset_name = "backfill_idempotency_pipeline_version"
    dataset_id = repository.get_or_create_dataset(dataset_name)
    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id)
    config = _make_config(path="customers/pipeline_version_proof/")

    object_store.put_object(
        "raw",
        "customers/pipeline_version_proof/a.csv",
        b"customer_id,name\n1,Alice\n",
    )

    def _discover() -> list[Any]:
        return discover_files(
            metadata=repository,
            objects=object_store,
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            config=config,
            config_version_id=config_version_id,
            config_hash=_CONFIG_HASH,
            processor_image=_PROCESSOR_IMAGE,
            processor_version=_PROCESSOR_VERSION,
            schema=schema,
        )

    monkeypatch.setattr(discovery_module, "_PIPELINE_VERSION", "1")
    old_units = _discover()
    assert len(old_units) == 1
    old_key = old_units[0].idempotency_key
    old_run_id = old_units[0].run_id

    monkeypatch.setattr(discovery_module, "_PIPELINE_VERSION", "2")
    new_units = _discover()
    assert len(new_units) == 1
    new_key = new_units[0].idempotency_key
    new_run_id = new_units[0].run_id

    assert old_key != new_key
    assert old_run_id != new_run_id

    with psycopg.connect(migrated_dsn) as conn:
        file_ids = conn.execute(
            "SELECT DISTINCT file_id FROM meta.ingestion_runs WHERE run_id = ANY(%s)",
            ([old_run_id, new_run_id],),
        ).fetchall()
    # Both runs are for the SAME underlying file -- only the pipeline_version
    # term differs.
    assert len(file_ids) == 1

    with psycopg.connect(migrated_dsn) as conn:
        run_count = conn.execute(
            "SELECT COUNT(*) FROM meta.ingestion_runs WHERE dataset_id = %s",
            (dataset_id,),
        ).fetchone()
    assert run_count is not None
    assert run_count[0] == 2


def test_historical_succeeded_file_becomes_replayable_under_the_extended_key(
    repository: PostgresMetadataRepository,
    object_store: S3ObjectStore,
    migrated_dsn: str,
    schema: SchemaRepository,
) -> None:
    """D-18: a file `SUCCEEDED` under the OLD-formula key is newly claimable under the NEW one.

    Hand-seeds `meta.files`/`meta.ingestion_runs` rows exactly as a
    historical Phase 1-8 `discover_files` call would have left them --
    `SUCCEEDED` under the OLD 5-term key -- then proves a fresh
    `discover_files` call at the CURRENT `_PIPELINE_VERSION` returns a real
    `DiscoveredUnit` for that file (not skipped as `ALREADY_SUCCEEDED`), and
    the new run's `replay_of_run_id` equals the original `SUCCEEDED` run's
    `run_id`.
    """
    dataset_name = "backfill_idempotency_replay"
    dataset_id = repository.get_or_create_dataset(dataset_name)
    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id)
    config = _make_config(path="customers/replay_proof/")

    payload = b"customer_id,name\n1,Alice\n"
    object_uri = "s3://raw/customers/replay_proof/historical.csv"
    content_sha256 = hashlib.sha256(payload).digest()
    content_sha256_hex = content_sha256.hex()

    # Hand-seed the file row and its OLD-formula SUCCEEDED run -- simulating
    # a file discovered and successfully processed BEFORE this phase's
    # pipeline_version term existed. schema.get_current() returns None for
    # this brand-new dataset (no schema sync has ever run for it), so the
    # schema_version_term is "" -- matching discover_files' own documented
    # "no schema yet" fallback (Pitfall 5).
    file_id = repository.create_file(
        dataset_id=dataset_id,
        object_uri=object_uri,
        content_sha256=content_sha256,
        hash_version=1,
        size_bytes=len(payload),
        filename="historical.csv",
        status="PROCESSED",
    )
    batch_id = repository.get_or_create_batch(
        dataset_id=dataset_id,
        batch_key=f"{dataset_name}:{content_sha256_hex[:16]}",
        status="PUBLISHED",
    )
    old_formula_key = hashlib.sha256(
        f"{dataset_name}|{content_sha256_hex}|{_CONFIG_HASH}|{_PROCESSOR_IMAGE}|".encode(),
    ).hexdigest()
    old_run_id = repository.create_ingestion_run(
        idempotency_key=old_formula_key,
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version=_PROCESSOR_VERSION,
        processor_image_digest=_PROCESSOR_IMAGE,
        status="SUCCEEDED",
        file_id=file_id,
        batch_id=batch_id,
    )

    # Now upload the SAME bytes under the SAME object_uri and run a real
    # discover_files call at the CURRENT (6-term) formula.
    object_store.put_object("raw", "customers/replay_proof/historical.csv", payload)

    units = discover_files(
        metadata=repository,
        objects=object_store,
        dataset_id=dataset_id,
        dataset_name=dataset_name,
        config=config,
        config_version_id=config_version_id,
        config_hash=_CONFIG_HASH,
        processor_image=_PROCESSOR_IMAGE,
        processor_version=_PROCESSOR_VERSION,
        schema=schema,
    )

    # NOT skipped as ALREADY_SUCCEEDED: the extended key is a genuinely new,
    # unclaimed idempotency_key.
    assert len(units) == 1
    new_run_id = units[0].run_id
    assert new_run_id != old_run_id

    with psycopg.connect(migrated_dsn) as conn:
        row = conn.execute(
            "SELECT replay_of_run_id FROM meta.ingestion_runs WHERE run_id = %s",
            (new_run_id,),
        ).fetchone()
    assert row is not None
    assert row[0] == old_run_id
