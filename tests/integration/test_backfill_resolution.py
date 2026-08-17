"""Integration tests for `PostgresMetadataRepository.resolve_rejected_records_for_business_keys`.

Proves D-23/D-24/D-25 live, against a real, migrated PostgreSQL: a
`(dataset_id, business_key)`-scoped resolution call touches every PENDING row
sharing that identity across DIFFERENT `batch_id`s (the actual VALID-08 gap
this plan closes -- `08-VERIFICATION.md`'s live-confirmed root cause), never a
different business_key in the same dataset, never the SAME business_key in a
DIFFERENT dataset, never a `NULL` business_key row, and is idempotent on a
second call against an already-resolved set.
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


def _insert_pending_reject(  # noqa: PLR0913 -- matches meta.rejected_records' own seeded column set
    migrated_dsn: str,
    *,
    run_id: int,
    file_id: int,
    batch_id: int,
    source_row_number: int,
    business_key: str | None = None,
) -> None:
    """Seed one PENDING `meta.rejected_records` row directly via SQL.

    Deliberately does NOT go through `record_rejected_records` -- this test
    proves `resolve_rejected_records_for_business_keys` in isolation, matching
    the plan's own "seed ... via direct SQL" instruction.
    """
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
                "seeded directly for backfill-resolution test",
                business_key,
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


def test_resolution_scoped_to_business_key_across_batches_and_idempotent_on_replay(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    """D-23/D-24/D-25: the full scoping matrix, proven in one scenario.

    Seeds: 2 PENDING rejects sharing `business_key="BK-A"` under TWO
    DIFFERENT `batch_id`s in dataset X (the load-bearing case -- proves
    cross-batch resolution, the actual VALID-08 gap); 1 PENDING reject with
    `business_key="BK-B"` in the SAME dataset X (must stay untouched --
    business-key scoping); 1 PENDING reject with `business_key="BK-A"` in a
    SEPARATE dataset Y (must stay untouched -- dataset scoping prevents
    cross-dataset business-key collision); 1 PENDING reject with
    `business_key=None` (must stay untouched -- D-25's NULL-never-
    auto-resolves fallback).
    """
    dataset_id_x = repository.get_or_create_dataset("backfill_resolution_test_x")
    dataset_id_y = repository.get_or_create_dataset("backfill_resolution_test_y")
    config_version_id_x = _insert_config_version(migrated_dsn, dataset_id=dataset_id_x)
    config_version_id_y = _insert_config_version(migrated_dsn, dataset_id=dataset_id_y)

    file_id_x = repository.create_file(
        dataset_id=dataset_id_x,
        object_uri="s3://raw/backfill/file-x.csv",
        content_sha256=hashlib.sha256(b"backfill-resolution-x").digest(),
        hash_version=1,
        size_bytes=10,
        filename="file-x.csv",
        status="DISCOVERED",
    )
    file_id_y = repository.create_file(
        dataset_id=dataset_id_y,
        object_uri="s3://raw/backfill/file-y.csv",
        content_sha256=hashlib.sha256(b"backfill-resolution-y").digest(),
        hash_version=1,
        size_bytes=10,
        filename="file-y.csv",
        status="DISCOVERED",
    )

    # Two DIFFERENT batches in dataset X -- the content-differing-correction
    # case that a strictly batch_id-scoped resolve call could never bridge.
    batch_id_x1 = repository.create_batch(
        dataset_id=dataset_id_x,
        batch_key="backfill_resolution:X1",
        status="OPEN",
    )
    batch_id_x2 = repository.create_batch(
        dataset_id=dataset_id_x,
        batch_key="backfill_resolution:X2",
        status="OPEN",
    )
    batch_id_y = repository.create_batch(
        dataset_id=dataset_id_y,
        batch_key="backfill_resolution:Y1",
        status="OPEN",
    )

    run_id_x1 = _seed_ingestion_run(
        repository,
        dataset_id=dataset_id_x,
        config_version_id=config_version_id_x,
        file_id=file_id_x,
        batch_id=batch_id_x1,
        key_suffix="backfill_resolution:x1-original",
    )
    run_id_x2 = _seed_ingestion_run(
        repository,
        dataset_id=dataset_id_x,
        config_version_id=config_version_id_x,
        file_id=file_id_x,
        batch_id=batch_id_x2,
        key_suffix="backfill_resolution:x2-original",
    )
    run_id_y = _seed_ingestion_run(
        repository,
        dataset_id=dataset_id_y,
        config_version_id=config_version_id_y,
        file_id=file_id_y,
        batch_id=batch_id_y,
        key_suffix="backfill_resolution:y-original",
    )
    run_id_backfill = _seed_ingestion_run(
        repository,
        dataset_id=dataset_id_x,
        config_version_id=config_version_id_x,
        file_id=file_id_x,
        batch_id=batch_id_x2,
        key_suffix="backfill_resolution:backfill",
    )

    # Load-bearing case: same business_key "BK-A", dataset X, TWO different
    # batch_ids.
    _insert_pending_reject(
        migrated_dsn,
        run_id=run_id_x1,
        file_id=file_id_x,
        batch_id=batch_id_x1,
        source_row_number=1,
        business_key="BK-A",
    )
    _insert_pending_reject(
        migrated_dsn,
        run_id=run_id_x2,
        file_id=file_id_x,
        batch_id=batch_id_x2,
        source_row_number=1,
        business_key="BK-A",
    )
    # Different business_key, same dataset X -- must stay untouched.
    _insert_pending_reject(
        migrated_dsn,
        run_id=run_id_x1,
        file_id=file_id_x,
        batch_id=batch_id_x1,
        source_row_number=2,
        business_key="BK-B",
    )
    # Same business_key "BK-A", SEPARATE dataset Y -- must stay untouched.
    _insert_pending_reject(
        migrated_dsn,
        run_id=run_id_y,
        file_id=file_id_y,
        batch_id=batch_id_y,
        source_row_number=1,
        business_key="BK-A",
    )
    # NULL business_key, dataset X -- must stay untouched (D-25).
    _insert_pending_reject(
        migrated_dsn,
        run_id=run_id_x1,
        file_id=file_id_x,
        batch_id=batch_id_x1,
        source_row_number=3,
        business_key=None,
    )

    with psycopg.connect(migrated_dsn) as conn:
        resolved_count = repository.resolve_rejected_records_for_business_keys(
            conn=conn,
            dataset_id=dataset_id_x,
            business_keys=["BK-A"],
            resolved_by_run_id=run_id_backfill,
            resolution_type="REDRIVEN",
        )
        conn.commit()

    # Both dataset-X/BK-A rows resolve, across their two different batch_ids.
    assert resolved_count == 2

    batch_x1_state = _fetch_resolution_state(migrated_dsn, batch_id=batch_id_x1)
    state_by_row = {
        row_number: (res_type, run_id) for row_number, res_type, run_id in batch_x1_state
    }
    assert state_by_row[1] == ("REDRIVEN", run_id_backfill)  # BK-A, resolved.
    assert state_by_row[2] == ("PENDING", None)  # BK-B, untouched.
    assert state_by_row[3] == ("PENDING", None)  # NULL business_key, untouched.

    batch_x2_state = _fetch_resolution_state(migrated_dsn, batch_id=batch_id_x2)
    assert len(batch_x2_state) == 1
    _row_number, resolution_type_x2, resolved_by_run_id_x2 = batch_x2_state[0]
    assert resolution_type_x2 == "REDRIVEN"  # Cross-batch: the actual gap this closes.
    assert resolved_by_run_id_x2 == run_id_backfill

    # Dataset Y's BK-A row is untouched -- dataset scoping prevents a
    # cross-dataset business-key collision.
    batch_y_state = _fetch_resolution_state(migrated_dsn, batch_id=batch_id_y)
    assert len(batch_y_state) == 1
    _row_number, resolution_type_y, resolved_by_run_id_y = batch_y_state[0]
    assert resolution_type_y == "PENDING"
    assert resolved_by_run_id_y is None

    # A second, identical resolution call against the now-fully-resolved set
    # is an idempotent no-op: 0 rows affected, never raises.
    with psycopg.connect(migrated_dsn) as conn:
        second_resolved_count = repository.resolve_rejected_records_for_business_keys(
            conn=conn,
            dataset_id=dataset_id_x,
            business_keys=["BK-A"],
            resolved_by_run_id=run_id_backfill,
            resolution_type="REDRIVEN",
        )
        conn.commit()
    assert second_resolved_count == 0


def test_resolve_rejected_records_for_business_keys_is_the_only_write_path_to_resolution_type() -> None:  # noqa: E501
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
    assert "def resolve_rejected_records_for_business_keys" in only_setter

    # Cross-check against the Protocol itself: only one abstract method
    # documents this mutation in its own signature/behavior.
    protocol_source = inspect.getsource(MetadataRepository)
    assert protocol_source.count("resolve_rejected_records_for_business_keys") >= 1
