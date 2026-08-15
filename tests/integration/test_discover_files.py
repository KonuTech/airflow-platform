"""Integration tests for ``dataplat.discovery.discover_files`` (ORCH-08, 04-06 Task 1).

``tests/unit/test_discovery.py`` already proves ``discover_files``'s behavior
against in-memory fakes (04-03-PLAN.md Task 2's own acceptance criterion).
This file proves the SAME behavior against a real testcontainers Postgres +
MinIO -- the fakes' ``create_batch``/``link_batch_file`` doubles do not
enforce the real ``uq_batches_dataset_batch_key``/``meta.batch_files`` primary
key the way PostgreSQL does, so only a real-database run can prove ORCH-08's
frozen-manifest claim actually holds under the real schema's constraints.

Every test builds its own locally-constructed ``DatasetConfig`` (mirrors
``tests/unit/test_discovery.py``'s ``_skip_config()``/
``tests/integration/test_staging_loader.py``'s ``_make_config()`` convention)
rather than loading ``configs/datasets/customers.yaml``, and gives itself a
dedicated ``source.path`` prefix plus a dedicated dataset name -- the session-
scoped MinIO/Postgres containers are shared with every other module in this
directory, so hermetic per-test prefixes are what keeps one test's uploads
from being listed by another test's ``discover_files`` call.
"""

from __future__ import annotations

import hashlib
import json
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
from dataplat.discovery import discover_files
from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.schema.repository import SchemaRepository
from dataplat.storage.db import create_pool
from dataplat.storage.objectstore import S3ObjectStore

if TYPE_CHECKING:
    from collections.abc import Iterator

_CONFIG_HASH = "config-hash-fixture"
_PROCESSOR_IMAGE = "sha256:test-processor-image"
_PROCESSOR_VERSION = "0.1.0"


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Insert a synthetic `meta.config_versions` row directly via SQL.

    Mirrors `tests/integration/test_metadata_repository.py`'s helper of the
    same name/shape -- duplicated locally rather than imported, matching
    this test suite's existing per-file helper convention.
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


def _make_config(*, path: str, max_units_per_run: int = 100) -> DatasetConfig:
    """A locally-constructed `DatasetConfig`, scoped to one test's own `source.path` prefix.

    columns= is required (06-02 Task 1/3, D-18) -- added here purely to stay
    constructible; discover_files itself never reads DatasetConfig.columns.
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
        batching=BatchingConfig(max_units_per_run=max_units_per_run),
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


def _read_assignment(object_store: S3ObjectStore, assignment_uri: str) -> str:
    """Read back one frozen assignment document's raw JSON text from its `s3://metadata/...` URI."""
    key = assignment_uri.removeprefix("s3://metadata/")
    return object_store.get_object("metadata", key).read()


def test_rerun_produces_identical_manifest(
    repository: PostgresMetadataRepository,
    object_store: S3ObjectStore,
    migrated_dsn: str,
    schema: SchemaRepository,
) -> None:
    """ORCH-08: a rerun over an unchanged object set is frozen (identical), not merely non-crashing.

    Three `discover_files` calls: (1) initial discovery of 2 new files: (2)
    an immediate rerun with NO status change -- 04-03's own documented
    semantics say a still-`PENDING` run IS re-offered, so this must return
    the SAME two units with an IDENTICAL frozen assignment document each,
    and must create zero additional `meta.files`/`meta.ingestion_runs`
    rows; (3) mark both runs `SUCCEEDED`, then rerun again -- only now must
    the returned units be empty, still with zero additional rows.
    """
    dataset_name = "discover_rerun_proof"
    dataset_id = repository.get_or_create_dataset(dataset_name)
    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id)
    config = _make_config(path="customers/rerun_proof/")

    object_store.put_object("raw", "customers/rerun_proof/a.csv", b"customer_id,name\n1,Alice\n")
    object_store.put_object("raw", "customers/rerun_proof/b.csv", b"customer_id,name\n2,Bob\n")

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

    def _row_counts() -> tuple[int, int]:
        with psycopg.connect(migrated_dsn) as conn:
            files = conn.execute(
                "SELECT COUNT(*) FROM meta.files WHERE dataset_id = %s",
                (dataset_id,),
            ).fetchone()
            runs = conn.execute(
                "SELECT COUNT(*) FROM meta.ingestion_runs WHERE dataset_id = %s",
                (dataset_id,),
            ).fetchone()
        assert files is not None
        assert runs is not None
        return int(files[0]), int(runs[0])

    first_units = _discover()
    assert len(first_units) == 2
    first_keys = {unit.idempotency_key for unit in first_units}
    first_run_ids = {unit.run_id for unit in first_units}
    first_assignments = {
        unit.run_id: _read_assignment(object_store, unit.assignment_uri) for unit in first_units
    }
    assert _row_counts() == (2, 2)

    second_units = _discover()
    assert {unit.idempotency_key for unit in second_units} == first_keys
    assert {unit.run_id for unit in second_units} == first_run_ids
    for unit in second_units:
        assert _read_assignment(object_store, unit.assignment_uri) == first_assignments[unit.run_id]
    # The rerun's own row-count check (not merely the returned list) --
    # proves no hidden duplicate row, matching this plan's acceptance
    # criteria for this test.
    assert _row_counts() == (2, 2)

    for run_id in first_run_ids:
        repository.update_ingestion_run_status(run_id=run_id, status="SUCCEEDED")

    third_units = _discover()
    assert third_units == []
    assert _row_counts() == (2, 2)


def test_duplicate_content_is_skipped(
    repository: PostgresMetadataRepository,
    object_store: S3ObjectStore,
    migrated_dsn: str,
    schema: SchemaRepository,
) -> None:
    """D-13: the same bytes under a second `object_uri` is recorded as a duplicate, never run."""
    dataset_name = "discover_dup_proof"
    dataset_id = repository.get_or_create_dataset(dataset_name)
    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id)
    config = _make_config(path="customers/dup_proof/")

    payload = b"customer_id,name\n1,Alice\n"
    object_store.put_object("raw", "customers/dup_proof/original.csv", payload)
    object_store.put_object("raw", "customers/dup_proof/reupload.csv", payload)

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

    assert len(units) == 1

    content_sha256 = hashlib.sha256(payload).digest()
    original_file_id = repository.find_file_by_content_hash(
        dataset_id=dataset_id,
        content_sha256=content_sha256,
    )
    assert original_file_id is not None

    with psycopg.connect(migrated_dsn) as conn:
        rows = conn.execute(
            """
            SELECT object_uri, duplicate_of_file_id FROM meta.files
             WHERE dataset_id = %s ORDER BY object_uri
            """,
            (dataset_id,),
        ).fetchall()
    assert [row[0] for row in rows] == [
        "s3://raw/customers/dup_proof/original.csv",
        "s3://raw/customers/dup_proof/reupload.csv",
    ]
    assert rows[0][1] is None
    assert rows[1][1] == original_file_id


def test_business_date_stays_null(
    repository: PostgresMetadataRepository,
    object_store: S3ObjectStore,
    migrated_dsn: str,
    schema: SchemaRepository,
) -> None:
    """`meta.files.business_date` is never populated by discovery (README §67 determinism)."""
    dataset_name = "discover_bizdate_proof"
    dataset_id = repository.get_or_create_dataset(dataset_name)
    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id)
    config = _make_config(path="customers/bizdate_proof/")
    object_store.put_object("raw", "customers/bizdate_proof/a.csv", b"customer_id,name\n1,Alice\n")

    discover_files(
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

    with psycopg.connect(migrated_dsn) as conn:
        rows = conn.execute(
            "SELECT business_date FROM meta.files WHERE dataset_id = %s",
            (dataset_id,),
        ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] is None


def test_batching_cap_defers_excess(
    repository: PostgresMetadataRepository,
    object_store: S3ObjectStore,
    migrated_dsn: str,
    schema: SchemaRepository,
) -> None:
    """ORCH-03: the cap limits the RETURNED units; the excess is deferred, never dropped."""
    dataset_name = "discover_cap_proof"
    dataset_id = repository.get_or_create_dataset(dataset_name)
    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id)
    config = _make_config(path="customers/cap_proof/", max_units_per_run=1)

    for index in range(3):
        object_store.put_object(
            "raw",
            f"customers/cap_proof/file-{index}.csv",
            f"customer_id,name\n{index},Person{index}\n".encode(),
        )

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

    assert len(units) == 1

    with psycopg.connect(migrated_dsn) as conn:
        pending_count = conn.execute(
            """
            SELECT COUNT(*) FROM meta.ingestion_runs
             WHERE dataset_id = %s AND status = 'PENDING'
            """,
            (dataset_id,),
        ).fetchone()
    assert pending_count is not None
    assert pending_count[0] == 3


def test_three_way_duplicate_content_resolves_deterministically_across_reruns(
    repository: PostgresMetadataRepository,
    object_store: S3ObjectStore,
    migrated_dsn: str,
    schema: SchemaRepository,
) -> None:
    """CR-02 (04-10 gap closure): reproduces the exact scenario behind the live `file_id=10` orphan.

    Three sequential `discover_files` passes grow one duplicate-content group
    from 1 file to 2 to 3 -- the same accumulation shape 04-VERIFICATION.md
    observed producing a live, broken row (`duplicate_of_file_id IS NULL`
    while not being the group's minimum `file_id`). Every non-original file
    in the group must end the third pass pointing at the group's single
    lowest `file_id`, and the lowest-`file_id` row itself must keep
    `duplicate_of_file_id IS NULL`.
    """
    dataset_name = "discover_three_way_dup_proof"
    dataset_id = repository.get_or_create_dataset(dataset_name)
    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id)
    config = _make_config(path="customers/three_way_dup_proof/")
    payload = b"customer_id,name\n1,Alice\n"

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

    object_store.put_object("raw", "customers/three_way_dup_proof/a.csv", payload)
    _discover()
    object_store.put_object("raw", "customers/three_way_dup_proof/b.csv", payload)
    _discover()
    object_store.put_object("raw", "customers/three_way_dup_proof/c.csv", payload)
    _discover()

    with psycopg.connect(migrated_dsn) as conn:
        rows = conn.execute(
            """
            SELECT object_uri, file_id, duplicate_of_file_id FROM meta.files
             WHERE dataset_id = %s ORDER BY file_id
            """,
            (dataset_id,),
        ).fetchall()

    assert len(rows) == 3
    original_file_id = min(row[1] for row in rows)
    for _object_uri, file_id, duplicate_of_file_id in rows:
        if file_id == original_file_id:
            assert duplicate_of_file_id is None
        else:
            assert duplicate_of_file_id == original_file_id
