"""Integration tests for `PostgresMetadataRepository`'s validation/rejected-record writers.

Proves plan 08-03's Task 1 truth directly against a real, migrated
PostgreSQL, with no Airflow, no pipeline, no barrier stage involved: rows
written inside an open transaction are visible once that transaction
commits, and rolled back together if the transaction rolls back — the
transactional contract every later barrier stage and `pipeline/run.py`
wiring builds on without re-deriving it themselves (Pitfall 2's exact
distinguishing case).
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

import psycopg
import pytest

from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.models.record import RejectedRecord
from dataplat.models.report import ValidationResult
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Insert a synthetic `meta.config_versions` row directly via SQL.

    Mirrors `tests/integration/test_publish_merge.py`'s helper of the same
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


def _seed_run(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
    *,
    key_suffix: str,
) -> tuple[int, int, int]:
    """Create dataset+config_version+file+batch+RUNNING run; return ``(run_id, file_id, batch_id)``.

    Mirrors `tests/integration/test_publish_merge.py`'s `_seed_run` --
    duplicated locally rather than imported, per this test suite's own
    per-file helper convention.
    """
    dataset_id = repository.get_or_create_dataset(f"validation_persist_{key_suffix}")
    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id)
    file_id = repository.create_file(
        dataset_id=dataset_id,
        object_uri=f"s3://raw/validation/{key_suffix}.csv",
        content_sha256=hashlib.sha256(key_suffix.encode()).digest(),
        hash_version=1,
        size_bytes=10,
        filename=f"{key_suffix}.csv",
        status="DISCOVERED",
    )
    batch_id = repository.create_batch(
        dataset_id=dataset_id,
        batch_key=f"{key_suffix}:2026-08-17:1",
        status="OPEN",
    )
    run_id = repository.create_ingestion_run(
        idempotency_key=f"{key_suffix}:1",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        status="RUNNING",
        file_id=file_id,
        batch_id=batch_id,
    )
    return run_id, file_id, batch_id


@pytest.fixture
def repository(migrated_dsn: str) -> Iterator[PostgresMetadataRepository]:
    """A `PostgresMetadataRepository` backed by an opened pool over the migrated database."""
    pool = create_pool(migrated_dsn)
    pool.open(wait=True)
    try:
        yield PostgresMetadataRepository(pool)
    finally:
        pool.close()


def _fetch_validation_results(migrated_dsn: str, *, run_id: int) -> list[tuple[Any, ...]]:
    with psycopg.connect(migrated_dsn) as conn:
        return conn.execute(
            """
            SELECT rule_id, rule_type, severity, outcome, evaluated_count,
                   failed_count, threshold, observed
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
            SELECT run_id, file_id, batch_id, source_row_number, raw_line,
                   error_type, error_column, error_message, resolution_type
              FROM meta.rejected_records
             WHERE run_id = %s
             ORDER BY source_row_number
            """,
            (run_id,),
        ).fetchall()


def test_commit_makes_validation_and_rejected_rows_visible(migrated_dsn: str) -> None:
    """Test A (commit path): rows land and are visible on a FRESH connection after commit."""
    pool = create_pool(migrated_dsn)
    pool.open(wait=True)
    try:
        repository = PostgresMetadataRepository(pool)
        run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="commit_path")

        results = [
            ValidationResult(
                rule_id="rule.completeness.email",
                outcome="FAIL",
                message="3 rows missing email",
                rule_type="QUALITY",
                severity="ERROR",
                evaluated_count=100,
                failed_count=3,
                threshold={"max_failed_ratio": 0.01},
                observed={"failed_ratio": 0.03},
            ),
            ValidationResult(
                rule_id="rule.structural.column_count",
                outcome="PASS",
                message="column count matches header",
                rule_type="STRUCTURAL",
                severity="ERROR",
                evaluated_count=100,
                failed_count=0,
                threshold={},
                observed={},
            ),
        ]
        rejected = [
            RejectedRecord(
                source_row_number=17,
                error_type="RAGGED_ROW",
                error_message="expected 5 fields, found 4",
                raw_line="1,2,3,4",
                error_column=None,
            ),
            RejectedRecord(
                source_row_number=42,
                error_type="TYPE_MISMATCH",
                error_message="'abc' is not a valid integer",
                raw_line="abc,2,3,4,5",
                error_column="quantity",
            ),
        ]

        with psycopg.connect(migrated_dsn) as conn:
            repository.record_validation_results(conn=conn, run_id=run_id, results=results)
            repository.record_rejected_records(
                conn=conn,
                run_id=run_id,
                file_id=file_id,
                batch_id=batch_id,
                rejected=rejected,
            )
            conn.commit()
    finally:
        pool.close()

    validation_rows = _fetch_validation_results(migrated_dsn, run_id=run_id)
    assert len(validation_rows) == 2
    completeness_row = validation_rows[0]
    assert completeness_row[0] == "rule.completeness.email"
    assert completeness_row[1] == "QUALITY"
    assert completeness_row[2] == "ERROR"
    assert completeness_row[3] == "FAIL"
    assert completeness_row[4] == 100
    assert completeness_row[5] == 3
    # threshold/observed dicts round-trip through JSONB unchanged.
    assert completeness_row[6] == {"max_failed_ratio": 0.01}
    assert completeness_row[7] == {"failed_ratio": 0.03}

    rejected_rows = _fetch_rejected_records(migrated_dsn, run_id=run_id)
    assert len(rejected_rows) == 2
    first, second = rejected_rows
    assert first[0] == run_id
    assert first[1] == file_id
    assert first[2] == batch_id
    assert first[3] == 17
    assert first[4] == "1,2,3,4"
    assert first[5] == "RAGGED_ROW"
    assert first[6] is None
    assert first[7] == "expected 5 fields, found 4"
    # resolution_type defaults to PENDING -- never set explicitly by
    # record_rejected_records.
    assert first[8] == "PENDING"
    assert second[3] == 42
    assert second[6] == "quantity"
    assert second[8] == "PENDING"


def test_rollback_leaves_no_trace_of_either_write(migrated_dsn: str) -> None:
    """Test B (rollback path): explicit rollback removes both writes -- proves real tx participation."""  # noqa: E501, W505
    pool = create_pool(migrated_dsn)
    pool.open(wait=True)
    try:
        repository = PostgresMetadataRepository(pool)
        run_id, file_id, batch_id = _seed_run(
            repository,
            migrated_dsn,
            key_suffix="rollback_path",
        )

        results = [
            ValidationResult(
                rule_id="rule.rollback.should_not_persist",
                outcome="FAIL",
                message="this must never survive commit",
                rule_type="QUALITY",
                severity="ERROR",
                evaluated_count=1,
                failed_count=1,
            ),
        ]
        rejected = [
            RejectedRecord(
                source_row_number=1,
                error_type="RAGGED_ROW",
                error_message="this must never survive commit",
                raw_line="x,y",
                error_column=None,
            ),
        ]

        with psycopg.connect(migrated_dsn) as conn:
            repository.record_validation_results(conn=conn, run_id=run_id, results=results)
            repository.record_rejected_records(
                conn=conn,
                run_id=run_id,
                file_id=file_id,
                batch_id=batch_id,
                rejected=rejected,
            )
            conn.rollback()
    finally:
        pool.close()

    assert _fetch_validation_results(migrated_dsn, run_id=run_id) == []
    assert _fetch_rejected_records(migrated_dsn, run_id=run_id) == []


def test_empty_lists_are_a_no_op_and_never_raise(migrated_dsn: str) -> None:
    """`results=[]`/`rejected=[]` execute zero INSERTs and do not raise (Task 1's behavior spec)."""
    pool = create_pool(migrated_dsn)
    pool.open(wait=True)
    try:
        repository = PostgresMetadataRepository(pool)
        run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="empty_noop")

        with psycopg.connect(migrated_dsn) as conn:
            repository.record_validation_results(conn=conn, run_id=run_id, results=[])
            repository.record_rejected_records(
                conn=conn,
                run_id=run_id,
                file_id=file_id,
                batch_id=batch_id,
                rejected=[],
            )
            conn.commit()
    finally:
        pool.close()

    assert _fetch_validation_results(migrated_dsn, run_id=run_id) == []
    assert _fetch_rejected_records(migrated_dsn, run_id=run_id) == []
