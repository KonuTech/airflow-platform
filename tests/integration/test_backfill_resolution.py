"""Integration tests for `PostgresMetadataRepository.resolve_rejected_records_for_batch`.

Proves D-03's whole-batch-only granularity and D-04's single-write-path
constraint live, against a real, migrated PostgreSQL: a `batch_id`-scoped
resolution call touches ONLY that batch's PENDING rejects, never a different
batch's rows and never an individual row, and is idempotent on a second call
against an already-resolved batch.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import re
from typing import TYPE_CHECKING, Any

import psycopg
import pytest

from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.metadata.repository import MetadataRepository
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Insert a synthetic `meta.config_versions` row directly via SQL.

    Mirrors `tests/integration/test_publish_merge.py`'s helper of the same
    name/shape -- duplicated locally, per this test suite's own per-file
    helper convention.
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


def _seed_ingestion_run(  # noqa: PLR0913 -- matches create_ingestion_run's own identity/FK column set
    repository: PostgresMetadataRepository,
    *,
    dataset_id: int,
    config_version_id: int,
    file_id: int,
    batch_id: int,
    key_suffix: str,
) -> int:
    return repository.create_ingestion_run(
        idempotency_key=key_suffix,
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        status="RUNNING",
        file_id=file_id,
        batch_id=batch_id,
    )


def _insert_pending_reject(
    migrated_dsn: str,
    *,
    run_id: int,
    file_id: int,
    batch_id: int,
    source_row_number: int,
) -> None:
    """Seed one PENDING `meta.rejected_records` row directly via SQL.

    Deliberately does NOT go through `record_rejected_records` -- this test
    proves `resolve_rejected_records_for_batch` in isolation, matching the
    plan's own "seed ... via direct SQL" instruction.
    """
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
                "seeded directly for backfill-resolution test",
            ),
        )
        conn.commit()


def _fetch_resolution_state(migrated_dsn: str, *, batch_id: int) -> list[tuple[Any, ...]]:
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


@pytest.fixture
def repository(migrated_dsn: str) -> Iterator[PostgresMetadataRepository]:
    pool = create_pool(migrated_dsn)
    pool.open(wait=True)
    try:
        yield PostgresMetadataRepository(pool)
    finally:
        pool.close()


def test_resolution_scoped_to_one_batch_and_idempotent_on_replay(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    """D-03/D-04: a batch-scoped resolve call touches ONLY that batch, and is a no-op replayed."""
    dataset_id = repository.get_or_create_dataset("backfill_resolution_test")
    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id)
    file_id = repository.create_file(
        dataset_id=dataset_id,
        object_uri="s3://raw/backfill/file.csv",
        content_sha256=hashlib.sha256(b"backfill-resolution").digest(),
        hash_version=1,
        size_bytes=10,
        filename="file.csv",
        status="DISCOVERED",
    )
    batch_id_a = repository.create_batch(
        dataset_id=dataset_id,
        batch_key="backfill_resolution:A",
        status="OPEN",
    )
    batch_id_b = repository.create_batch(
        dataset_id=dataset_id,
        batch_key="backfill_resolution:B",
        status="OPEN",
    )
    run_id_original = _seed_ingestion_run(
        repository,
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        file_id=file_id,
        batch_id=batch_id_a,
        key_suffix="backfill_resolution:original",
    )
    run_id_backfill = _seed_ingestion_run(
        repository,
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        file_id=file_id,
        batch_id=batch_id_a,
        key_suffix="backfill_resolution:backfill",
    )

    # 2 PENDING rows in batch A, 1 PENDING row in batch B.
    _insert_pending_reject(
        migrated_dsn,
        run_id=run_id_original,
        file_id=file_id,
        batch_id=batch_id_a,
        source_row_number=1,
    )
    _insert_pending_reject(
        migrated_dsn,
        run_id=run_id_original,
        file_id=file_id,
        batch_id=batch_id_a,
        source_row_number=2,
    )
    _insert_pending_reject(
        migrated_dsn,
        run_id=run_id_original,
        file_id=file_id,
        batch_id=batch_id_b,
        source_row_number=1,
    )

    with psycopg.connect(migrated_dsn) as conn:
        resolved_count = repository.resolve_rejected_records_for_batch(
            conn=conn,
            batch_id=batch_id_a,
            resolved_by_run_id=run_id_backfill,
            resolution_type="REDRIVEN",
        )
        conn.commit()

    assert resolved_count == 2

    batch_a_state = _fetch_resolution_state(migrated_dsn, batch_id=batch_id_a)
    assert len(batch_a_state) == 2
    for _row_number, resolution_type, resolved_by_run_id in batch_a_state:
        assert resolution_type == "REDRIVEN"
        assert resolved_by_run_id == run_id_backfill

    # Batch B's row is untouched -- the whole-batch WHERE scope never leaks
    # across batches (D-03).
    batch_b_state = _fetch_resolution_state(migrated_dsn, batch_id=batch_id_b)
    assert len(batch_b_state) == 1
    _row_number, resolution_type_b, resolved_by_run_id_b = batch_b_state[0]
    assert resolution_type_b == "PENDING"
    assert resolved_by_run_id_b is None

    # A second, identical resolution call against the now-fully-resolved
    # batch is an idempotent no-op: 0 rows affected, never raises.
    with psycopg.connect(migrated_dsn) as conn:
        second_resolved_count = repository.resolve_rejected_records_for_batch(
            conn=conn,
            batch_id=batch_id_a,
            resolved_by_run_id=run_id_backfill,
            resolution_type="REDRIVEN",
        )
        conn.commit()
    assert second_resolved_count == 0


def test_resolve_rejected_records_for_batch_is_the_only_write_path_to_resolution_type() -> None:
    """T-08-08/D-04: no method on `PostgresMetadataRepository` other than the resolver can SET it.

    Scans every method's own source for the literal SQL assignment pattern
    `resolution_type = %s` (an actual mutation, not merely a docstring
    mention of the column name -- `record_rejected_records`'s own docstring
    explains it deliberately never sets `resolution_type`, which would
    otherwise pollute a bare substring count) and asserts exactly one method
    body contains it.
    """
    source = inspect.getsource(PostgresMetadataRepository)
    # Split the class source into per-method chunks at "    def " boundaries
    # (the class-body indent level) -- a simple, source-level structural
    # split, matching this codebase's existing policy-test style.
    method_blocks = re.split(r"\n(?=    def )", source)

    setter_pattern = re.compile(r"resolution_type\s*=\s*%s")
    methods_that_set_it = [block for block in method_blocks if setter_pattern.search(block)]

    assert len(methods_that_set_it) == 1
    (only_setter,) = methods_that_set_it
    assert "def resolve_rejected_records_for_batch" in only_setter

    # Cross-check against the Protocol itself: only one abstract method
    # documents this mutation in its own signature/behavior.
    protocol_source = inspect.getsource(MetadataRepository)
    assert protocol_source.count("resolve_rejected_records_for_batch") >= 1
