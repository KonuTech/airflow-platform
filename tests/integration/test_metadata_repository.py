"""META-01 usability proof: a full dataset->file->batch->ingestion_run chain, typed, no hand SQL.

`test_full_slice_round_trip` proves every FK in the slice resolves through
`PostgresMetadataRepository` alone, against the schema plan 03-02's
migrations created -- the mechanical proof that META-01's schema is not
just DDL-valid but genuinely usable.
`test_resolved_env_secret_yields_a_live_metadata_connection` is ROADMAP
Phase 3 success criterion 4 / SEC-15's proof: a connection pool built from
`resolve_secret("env://...")`'s output, not a raw DSN literal, executes a
real query against the migrated database (03-05's Task 3, the first place
in this phase the resolver and the pool factory are proven wired together
end to end).
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import psycopg
import pytest

from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.secrets.resolver import resolve_secret
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    from collections.abc import Iterator


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Insert a synthetic `meta.config_versions` row directly via SQL.

    This test file does not depend on plan 03-04's `ConfigRegistry` — every
    `meta.ingestion_runs` row still requires a real, FK-valid
    `config_version_id`, so each test that creates a run seeds one row here
    for its own freshly-created dataset.
    """
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            """
            INSERT INTO meta.config_versions (
                dataset_id, version, config_hash, config_document,
                config_schema_version, valid_from
            ) VALUES (%s, %s, %s, %s::jsonb, %s, now())
            RETURNING config_version_id
            """,
            (dataset_id, 1, "synthetic-hash-for-test", json.dumps({"synthetic": True}), 1),
        ).fetchone()
        assert row is not None
        return int(row[0])


def _read_ingestion_run_status(dsn: str, run_id: int) -> str:
    """Read back `meta.ingestion_runs.status` directly via SQL (bypassing the repository)."""
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            "SELECT status FROM meta.ingestion_runs WHERE run_id = %s",
            (run_id,),
        ).fetchone()
        assert row is not None
        return str(row[0])


@pytest.fixture
def repository(migrated_dsn: str) -> Iterator[PostgresMetadataRepository]:
    """A `PostgresMetadataRepository` backed by an opened pool over the migrated database."""
    pool = create_pool(migrated_dsn)
    pool.open(wait=True)
    try:
        yield PostgresMetadataRepository(pool)
    finally:
        pool.close()


def test_full_slice_round_trip(repository: PostgresMetadataRepository, migrated_dsn: str) -> None:
    dataset_id_first = repository.get_or_create_dataset("customers")
    dataset_id_second = repository.get_or_create_dataset("customers")
    assert dataset_id_first == dataset_id_second

    content_sha256 = hashlib.sha256(b"synthetic customers file content").digest()
    file_id = repository.create_file(
        dataset_id=dataset_id_first,
        object_uri="s3://raw/customers/2026/08/13/customers_20260813.csv",
        content_sha256=content_sha256,
        hash_version=1,
        size_bytes=1234,
        filename="customers_20260813.csv",
        status="DISCOVERED",
    )
    found_file_id = repository.find_file_by_content_hash(
        dataset_id=dataset_id_first,
        content_sha256=content_sha256,
    )
    assert found_file_id == file_id

    never_inserted_hash = hashlib.sha256(b"content that was never uploaded").digest()
    assert (
        repository.find_file_by_content_hash(
            dataset_id=dataset_id_first,
            content_sha256=never_inserted_hash,
        )
        is None
    )

    batch_id = repository.create_batch(
        dataset_id=dataset_id_first,
        batch_key="customers:2026-08-13:1",
        status="OPEN",
    )
    repository.link_batch_file(batch_id=batch_id, file_id=file_id, sequence_no=1)

    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id_first)

    run_id = repository.create_ingestion_run(
        idempotency_key="customers:2026-08-13:1:attempt-1",
        dataset_id=dataset_id_first,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        status="RUNNING",
        file_id=file_id,
        batch_id=batch_id,
    )
    assert _read_ingestion_run_status(migrated_dsn, run_id) == "RUNNING"

    repository.update_ingestion_run_status(
        run_id=run_id,
        status="SUCCEEDED",
        finished_at=datetime.now(tz=UTC),
    )
    assert _read_ingestion_run_status(migrated_dsn, run_id) == "SUCCEEDED"


def test_update_ingestion_run_status_rejects_unknown_field(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    dataset_id = repository.get_or_create_dataset("rejects_unknown_field")
    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id)
    run_id = repository.create_ingestion_run(
        idempotency_key="rejects_unknown_field:attempt-1",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        status="RUNNING",
    )

    with pytest.raises(ValueError, match="unknown"):
        repository.update_ingestion_run_status(
            run_id=run_id,
            status="SUCCEEDED",
            not_a_real_column="this must be rejected",
        )

    # The rejected call must not have executed any SQL against the row.
    assert _read_ingestion_run_status(migrated_dsn, run_id) == "RUNNING"


def test_resolved_env_secret_yields_a_live_metadata_connection(
    migrated_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ROADMAP success criterion 4 / SEC-15: resolve_secret() -> create_pool(), proven live."""
    monkeypatch.setenv("DATAPLAT_TEST_DB_DSN", migrated_dsn)

    resolved = resolve_secret("env://DATAPLAT_TEST_DB_DSN")
    assert resolved == migrated_dsn

    pool = create_pool(resolved)
    pool.open(wait=True)
    try:
        repository = PostgresMetadataRepository(pool)
        dataset_id = repository.get_or_create_dataset("resolver_wiring_proof")
        assert isinstance(dataset_id, int)
    finally:
        pool.close()
