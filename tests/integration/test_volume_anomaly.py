"""Integration tests for ``dataplat.validate.volume_anomaly.VolumeAnomalyBarrier`` (08-09).

Proves this plan's own ``must_haves.truths`` against a REAL, migrated
PostgreSQL -- no mocked repository, no injected ``ctx_db_query``: seeds real
``meta.ingestion_runs``/``meta.validation_results`` rows and lets the
barrier issue its own ``_HISTORICAL_AVERAGE_SQL`` query through a real,
opened ``ConnectionPool``.

Mirrors ``test_referential_integrity.py``'s own helper/fixture conventions
(``_seed_run``, ``_insert_config_version``, ``_make_context``), duplicated
locally per this test suite's per-file helper convention.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

import psycopg
import pytest

from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.models.identity import RunContext
from dataplat.pipeline.protocol import PipelineContext
from dataplat.storage.db import create_pool
from dataplat.validate.volume_anomaly import VolumeAnomalyBarrier

if TYPE_CHECKING:
    from collections.abc import Iterator

    from psycopg_pool import ConnectionPool

pytestmark = pytest.mark.integration


def _insert_config_version(dsn: str, *, dataset_id: int, key_suffix: str) -> int:
    """Insert a synthetic `meta.config_versions` row directly via SQL.

    Mirrors `test_referential_integrity.py`'s own `_insert_config_version`,
    duplicated locally per this test suite's per-file helper convention --
    but `config_hash` is derived from `key_suffix` (rather than a fixed
    literal) since this file, unlike its precedent, seeds MULTIPLE runs
    (and therefore multiple config_versions) for the SAME `dataset_id`, and
    `uq_config_versions_dataset_hash` is `UNIQUE (dataset_id, config_hash)`.
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
                "config_hash": f"synthetic-hash-for-test-{key_suffix}",
                "config_document": '{"synthetic": true}',
                "config_schema_version": 1,
            },
        ).fetchone()
        assert row is not None
        return int(row[0])


def _seed_succeeded_run(
    repository: PostgresMetadataRepository,
    *,
    dataset_id: int,
    config_version_id: int,
    key_suffix: str,
) -> int:
    """Create a file+batch+SUCCEEDED run for `dataset_id`; return `run_id`.

    `config_version_id` is created ONCE per dataset by the caller and
    reused across every seeded run for that dataset --
    `uq_config_versions_current_per_dataset` allows at most one CURRENT
    (`valid_to IS NULL`) config_version per dataset, and this test seeds
    MULTIPLE runs for the SAME dataset.

    Status is set to `SUCCEEDED` directly at creation (`create_ingestion_run`
    accepts any initial `status`) -- this test only needs a real,
    `dataset_id`-linked, `SUCCEEDED` `meta.ingestion_runs` row for the
    barrier's own join to match against, not a full publish flow.
    """
    file_id = repository.create_file(
        dataset_id=dataset_id,
        object_uri=f"s3://raw/volume/{key_suffix}.csv",
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
    return repository.create_ingestion_run(
        idempotency_key=f"{key_suffix}:1",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        status="SUCCEEDED",
        file_id=file_id,
        batch_id=batch_id,
    )


def _record_volume_result(
    migrated_dsn: str,
    *,
    run_id: int,
    row_count: int,
) -> None:
    """Insert one `rule_type="VOLUME"` `meta.validation_results` row for `run_id`.

    `evaluated_count=row_count` is this same class's own writing convention
    (module docstring of `volume_anomaly.py`): a prior VOLUME row's
    `evaluated_count` IS that prior run's own row count.
    """
    with psycopg.connect(migrated_dsn) as conn:
        conn.execute(
            """
            INSERT INTO meta.validation_results (
                run_id, rule_id, rule_type, severity, outcome,
                evaluated_count, failed_count, threshold, observed
            ) VALUES (
                %s, 'volume_anomaly_barrier', 'VOLUME', 'ERROR', 'PASS',
                %s, 0, '{}'::jsonb, '{}'::jsonb
            )
            """,
            (run_id, row_count),
        )
        conn.commit()


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
def db_pool(migrated_dsn: str) -> Iterator[ConnectionPool]:
    """A real, opened `ConnectionPool` -- what `VolumeAnomalyBarrier.apply()` reads through."""
    pool = create_pool(migrated_dsn)
    pool.open(wait=True)
    try:
        yield pool
    finally:
        pool.close()


def _make_context(db_pool: ConnectionPool) -> PipelineContext:
    """A mostly-placeholder `PipelineContext` -- `VolumeAnomalyBarrier.apply()` only reads `ctx.db`."""  # noqa: E501, W505
    return PipelineContext(
        run=RunContext(run_id=1, idempotency_key="test-run"),
        config=None,  # type: ignore[arg-type] -- unused by VolumeAnomalyBarrier.apply()
        metadata=None,  # type: ignore[arg-type] -- unused by VolumeAnomalyBarrier.apply()
        objects=None,  # type: ignore[arg-type] -- unused by VolumeAnomalyBarrier.apply()
        db=db_pool,
        log=None,  # type: ignore[arg-type] -- unused by VolumeAnomalyBarrier.apply()
    )


def test_anomalous_row_count_flags_against_real_persisted_history(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
    db_pool: ConnectionPool,
) -> None:
    """3 real SUCCEEDED VOLUME rows averaging 90; this run has 1000 (>900) -- flags."""
    dataset_id = repository.get_or_create_dataset("volume_anomaly_test_flag")
    config_version_id = _insert_config_version(
        migrated_dsn, dataset_id=dataset_id, key_suffix="flag"
    )
    for i, row_count in enumerate((80, 90, 100)):
        run_id = _seed_succeeded_run(
            repository,
            dataset_id=dataset_id,
            config_version_id=config_version_id,
            key_suffix=f"flag_{i}",
        )
        _record_volume_result(migrated_dsn, run_id=run_id, row_count=row_count)

    barrier = VolumeAnomalyBarrier(
        dataset_id=dataset_id,
        current_row_count=1000,
        multiplier=10.0,
        rule_id="orders_volume_anomaly",
        strategy="QUARANTINE_FILE",
    )
    result = barrier.apply(_make_context(db_pool))

    assert len(result.findings) == 1
    finding = result.findings[0]
    assert finding.rule_type == "VOLUME"
    assert finding.outcome == "QUARANTINE"
    assert finding.failed_count == 1
    assert finding.observed["historical_average"] == pytest.approx(90.0)
    assert finding.observed["current_row_count"] == 1000


def test_within_bounds_row_count_never_flags_against_real_persisted_history(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
    db_pool: ConnectionPool,
) -> None:
    """3 real SUCCEEDED VOLUME rows averaging 90; this run has 500 (<=900) -- PASS."""
    dataset_id = repository.get_or_create_dataset("volume_anomaly_test_within_bounds")
    config_version_id = _insert_config_version(
        migrated_dsn, dataset_id=dataset_id, key_suffix="within"
    )
    for i, row_count in enumerate((80, 90, 100)):
        run_id = _seed_succeeded_run(
            repository,
            dataset_id=dataset_id,
            config_version_id=config_version_id,
            key_suffix=f"within_{i}",
        )
        _record_volume_result(migrated_dsn, run_id=run_id, row_count=row_count)

    barrier = VolumeAnomalyBarrier(
        dataset_id=dataset_id,
        current_row_count=500,
        multiplier=10.0,
        rule_id="orders_volume_anomaly",
        strategy="QUARANTINE_FILE",
    )
    result = barrier.apply(_make_context(db_pool))

    assert result.findings[0].outcome == "PASS"


def test_a_brand_new_dataset_with_zero_prior_volume_rows_never_flags(
    repository: PostgresMetadataRepository,
    db_pool: ConnectionPool,
) -> None:
    """A genuinely new `dataset_id`, zero prior VOLUME rows -- cold start PASS against REAL query results."""  # noqa: E501, W505
    dataset_id = repository.get_or_create_dataset("volume_anomaly_test_cold_start")

    barrier = VolumeAnomalyBarrier(
        dataset_id=dataset_id,
        current_row_count=999_999,
        multiplier=10.0,
        rule_id="orders_volume_anomaly",
        strategy="QUARANTINE_FILE",
    )
    result = barrier.apply(_make_context(db_pool))

    finding = result.findings[0]
    assert finding.outcome == "PASS"
    assert finding.observed == {"historical_average": None, "prior_run_count": 0}
