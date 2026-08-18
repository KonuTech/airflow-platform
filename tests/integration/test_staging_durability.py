"""Integration tests for ``StagingLoader.promote_to_durable_bronze()`` (D-01, plan 08.1-06 Task 1).

Every test drives a real ``StagingLoader`` + ``promote_to_durable_bronze()``
against a real testcontainers PostgreSQL, migrated to head (so migration
``0022``'s durable ``staging.customers``/``staging.orders`` tables exist).
``_FakeSource``/``_FakeRecordStream``/``_make_config`` mirror
``tests/integration/test_staging_loader.py``'s own conventions exactly --
pure in-memory ``Source``/``RecordStream`` test doubles, duplicated locally
per this test suite's existing per-file helper convention (mirrors
``tests/integration/test_publish_merge.py``'s own ``_seed_run`` duplication
note).

Unlike ``test_staging_loader.py``'s own tests -- which stage into the
throwaway ``staging.<dataset>__r<run_id>`` scratch buffer, whose FK-less
columns tolerate arbitrary integer ``run_id``/``file_id``/``batch_id``
values -- the durable ``staging.customers`` table (migration 0022) carries
REAL foreign keys on its six lineage columns, so every test here seeds a
genuine ``meta.datasets``/``meta.config_versions``/``meta.files``/
``meta.batches``/``meta.ingestion_runs`` row set first, via the same
``_seed_run`` helper shape ``test_publish_merge.py`` already uses.
"""

from __future__ import annotations

import contextlib
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
from dataplat.load.staging import StagingLoader
from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.models.identity import RunContext
from dataplat.models.record import RecordChunk
from dataplat.pipeline.protocol import PipelineContext
from dataplat.sources.protocol import RecordStream, Source
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

TARGET_COLUMNS = ("customer_id", "name", "country", "birth_date", "event_ts")


class _FakeRecordStream(RecordStream):
    """Yields a fixed, pre-built sequence of ``RecordChunk``s -- no real CSV/S3 I/O."""

    def __init__(self, chunks: list[RecordChunk]) -> None:
        self._chunks = chunks

    def chunks(self, *, start_ordinal: int | None = None) -> Iterator[RecordChunk]:  # noqa: ARG002
        """Yield this fake's fixed chunk sequence. ``start_ordinal`` is ignored."""
        yield from self._chunks


class _FakeSource(Source):
    """A ``Source`` test double: ``.open()`` yields a FRESH stream every call."""

    def __init__(self, rows_by_chunk: Sequence[tuple[tuple[str, ...], ...]]) -> None:
        self._rows_by_chunk = rows_by_chunk

    @contextlib.contextmanager
    def open(self, ctx: PipelineContext) -> Iterator[_FakeRecordStream]:  # noqa: ARG002
        """Yield a fresh ``_FakeRecordStream`` built from this fake's rows. ``ctx`` is ignored."""
        ordinal = 0
        chunks = []
        for rows in self._rows_by_chunk:
            chunks.append(
                RecordChunk(
                    rows=rows,
                    first_ordinal=ordinal,
                    expected_field_count=len(TARGET_COLUMNS),
                ),
            )
            ordinal += len(rows)
        yield _FakeRecordStream(chunks)


def _make_config() -> DatasetConfig:
    # dataset="customers" -- this MUST match migration 0022's real
    # `staging.customers` table name, since `promote_to_durable_bronze`
    # builds `durable_table = f"staging.{ctx.config.dataset}"`.
    return DatasetConfig(
        dataset="customers",
        config_schema_version=1,
        source=SourceConfig(
            type="csv",
            bucket="raw",
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


def _make_context(
    *,
    run_id: int,
    source: _FakeSource,
    file_id: int,
    batch_id: int,
) -> PipelineContext:
    """A placeholder ``PipelineContext`` -- only ``run``/``config``/``source`` are real."""
    return PipelineContext(
        run=RunContext(
            run_id=run_id,
            idempotency_key=f"test-run-{run_id}",
            file_id=file_id,
            batch_id=batch_id,
        ),
        config=_make_config(),
        metadata=None,  # type: ignore[arg-type] -- unused by StagingLoader
        objects=None,  # type: ignore[arg-type] -- unused by StagingLoader
        db=None,  # type: ignore[arg-type] -- unused by StagingLoader
        log=None,  # type: ignore[arg-type] -- unused by StagingLoader
        source=source,
    )


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Insert a synthetic ``meta.config_versions`` row directly via SQL.

    Mirrors ``tests/integration/test_publish_merge.py``'s helper of the same
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

    ``staging.customers``' six lineage columns are real foreign keys
    (migration 0022) -- unlike the throwaway scratch buffer, which carries
    none -- so ``promote_to_durable_bronze`` tests need real, FK-satisfying
    rows to promote into it.
    """
    dataset_id = repository.get_or_create_dataset(f"durability_test_{key_suffix}")
    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id)
    file_id = repository.create_file(
        dataset_id=dataset_id,
        object_uri=f"s3://raw/customers/{key_suffix}.csv",
        content_sha256=hashlib.sha256(key_suffix.encode()).digest(),
        hash_version=1,
        size_bytes=10,
        filename=f"{key_suffix}.csv",
        status="DISCOVERED",
    )
    batch_id = repository.create_batch(
        dataset_id=dataset_id,
        batch_key=f"{key_suffix}:2026-08-18:1",
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
    """A ``PostgresMetadataRepository`` backed by an opened pool over the migrated database."""
    pool = create_pool(migrated_dsn)
    pool.open(wait=True)
    try:
        yield PostgresMetadataRepository(pool)
    finally:
        pool.close()


@pytest.fixture
def conn(migrated_dsn: str) -> Iterator[psycopg.Connection[Any]]:
    """One open psycopg connection per test, over the migrated database."""
    with psycopg.connect(migrated_dsn) as connection:
        yield connection


def test_promote_appends_staged_rows_and_drops_the_scratch_buffer(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
    conn: psycopg.Connection[Any],
) -> None:
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="promote_1")
    rows = (
        ("1", "Alice", "US", "1990-01-01", "2026-01-01T00:00:00+00:00"),
        ("2", "Bob", "UK", "1992-02-02", "2026-01-02T00:00:00+00:00"),
    )
    ctx = _make_context(
        run_id=run_id,
        source=_FakeSource([rows]),
        file_id=file_id,
        batch_id=batch_id,
    )
    loader = StagingLoader(target_columns=TARGET_COLUMNS)

    staging_result = loader.load(ctx, conn)
    assert staging_result.rows_parsed == 2

    loader.promote_to_durable_bronze(ctx, conn, staging_result)
    conn.commit()

    with psycopg.connect(migrated_dsn) as verify_conn:
        durable_rows = verify_conn.execute(
            """
            SELECT customer_id, name, country, birth_date, event_ts,
                   _run_id, _file_id, _batch_id, _source_row_number,
                   _record_hash, _record_hash_version
              FROM staging.customers
             WHERE _run_id = %s
             ORDER BY _source_row_number
            """,
            (run_id,),
        ).fetchall()
        scratch_still_exists = verify_conn.execute(
            "SELECT to_regclass(%s)",
            (staging_result.staging_table,),
        ).fetchone()

    assert len(durable_rows) == 2
    assert [row[0] for row in durable_rows] == ["1", "2"]
    for row in durable_rows:
        assert row[5] == run_id
        assert row[6] == file_id
        assert row[7] == batch_id
        assert row[10] == 1  # _record_hash_version

    assert scratch_still_exists is not None
    assert scratch_still_exists[0] is None  # scratch buffer no longer exists


def test_promote_is_cumulative_across_two_runs(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
    conn: psycopg.Connection[Any],
) -> None:
    loader = StagingLoader(target_columns=TARGET_COLUMNS)

    run_id_a, file_id_a, batch_id_a = _seed_run(repository, migrated_dsn, key_suffix="cumulative_a")
    rows_a = (("10", "Carol", "CA", "1985-05-05", "2026-02-01T00:00:00+00:00"),)
    ctx_a = _make_context(
        run_id=run_id_a,
        source=_FakeSource([rows_a]),
        file_id=file_id_a,
        batch_id=batch_id_a,
    )
    staging_result_a = loader.load(ctx_a, conn)
    loader.promote_to_durable_bronze(ctx_a, conn, staging_result_a)
    conn.commit()

    run_id_b, file_id_b, batch_id_b = _seed_run(repository, migrated_dsn, key_suffix="cumulative_b")
    rows_b = (("11", "Dave", "DE", "1980-03-03", "2026-03-01T00:00:00+00:00"),)
    ctx_b = _make_context(
        run_id=run_id_b,
        source=_FakeSource([rows_b]),
        file_id=file_id_b,
        batch_id=batch_id_b,
    )
    staging_result_b = loader.load(ctx_b, conn)
    loader.promote_to_durable_bronze(ctx_b, conn, staging_result_b)
    conn.commit()

    with psycopg.connect(migrated_dsn) as verify_conn:
        run_ids_present = verify_conn.execute(
            "SELECT DISTINCT _run_id FROM staging.customers WHERE _run_id IN (%s, %s)",
            (run_id_a, run_id_b),
        ).fetchall()

    # Cumulative -- BOTH runs' rows present, never truncated between them.
    assert {row[0] for row in run_ids_present} == {run_id_a, run_id_b}


def test_durable_table_is_permanent_never_unlogged(migrated_dsn: str) -> None:
    with psycopg.connect(migrated_dsn) as conn:
        persistence = conn.execute(
            "SELECT relpersistence FROM pg_class WHERE oid = 'staging.customers'::regclass",
        ).fetchone()
    assert persistence is not None
    assert persistence[0] == "p"  # 'p' == permanent (never 'u' unlogged)
