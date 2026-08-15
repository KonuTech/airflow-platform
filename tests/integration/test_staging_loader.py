"""Integration tests for ``dataplat.load.staging.StagingLoader`` (LOAD-05, plan 04-04 Task 1).

Every test drives a real ``StagingLoader`` against a real testcontainers
PostgreSQL, migrated to head (so migration ``0007``'s ``staging`` schema
exists). ``_FakeSource``/``_FakeRecordStream`` stand in for
``ctx.source`` -- pure in-memory ``Source``/``RecordStream`` test doubles
that give each test exact control over chunk boundaries, independent of
``StagingLoader``'s own ``chunk_size`` constructor parameter (which does not
control chunk boundaries -- see ``staging.py``'s docstring: chunking is the
``Source``'s concern, never re-batched here).
"""

from __future__ import annotations

import contextlib
import hashlib
from typing import TYPE_CHECKING, Any

import psycopg
import pytest

from dataplat.config.model import (
    BatchingConfig,
    ColumnContract,
    DatasetConfig,
    DeduplicationConfig,
    LoadConfig,
    NormalizationConfig,
    SourceConfig,
)
from dataplat.load.staging import StagingLoader
from dataplat.models.identity import RunContext
from dataplat.models.record import RecordChunk
from dataplat.pipeline.protocol import PipelineContext
from dataplat.sources.protocol import RecordStream, Source

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
    """A ``Source`` test double: ``.open()`` yields a FRESH stream every call.

    "Fresh every call" matters: the twice-in-a-row retry test below calls
    ``.load()`` twice against the SAME ``ctx`` -- a stream that could only be
    iterated once would silently stage zero rows on the second call, masking
    a real retry bug as a false pass.
    """

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
    # columns= is required (06-02 Task 1/3, D-18) -- added here purely to stay
    # constructible; StagingLoader itself never reads DatasetConfig.columns.
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
    file_id: int = 1,
    batch_id: int = 1,
) -> PipelineContext:
    """A placeholder ``PipelineContext`` -- only ``run``/``config``/``source`` are real.

    Mirrors ``tests/unit/test_pipeline_errors.py``'s ``_make_context()``
    convention: ``StagingLoader.load()`` never dereferences
    ``metadata``/``objects``/``db``/``log``, so those stay ``None``. The
    staging table it creates carries no FK constraint on
    ``_run_id``/``_file_id``/``_batch_id`` (unlike ``normalized.customers``),
    so these can be arbitrary integers -- no ``meta.*`` rows need seeding.
    """
    return PipelineContext(
        run=RunContext(
            run_id=run_id,
            idempotency_key=f"test-run-{run_id}",
            file_id=file_id,
            batch_id=batch_id,
        ),
        config=_make_config(),
        metadata=None,  # type: ignore[arg-type] -- unused by StagingLoader.load()
        objects=None,  # type: ignore[arg-type] -- unused by StagingLoader.load()
        db=None,  # type: ignore[arg-type] -- unused by StagingLoader.load()
        log=None,  # type: ignore[arg-type] -- unused by StagingLoader.load()
        source=source,
    )


@pytest.fixture
def conn(migrated_dsn: str) -> Iterator[psycopg.Connection[Any]]:
    """One open psycopg connection per test, over the migrated database."""
    with psycopg.connect(migrated_dsn) as connection:
        yield connection


def test_ragged_row_is_rejected_and_absent_from_staged_table(
    conn: psycopg.Connection[Any],
) -> None:
    rows = (
        ("1", "Alice", "US", "1990-01-01", "2026-01-01T00:00:00+00:00"),
        ("2", "Bob"),  # ragged -- wrong field count
        ("3", "Carol", "CA", "1985-05-05", "2026-02-01T00:00:00+00:00"),
    )
    ctx = _make_context(run_id=101, source=_FakeSource([rows]))
    loader = StagingLoader(target_columns=TARGET_COLUMNS)

    result = loader.load(ctx, conn)

    assert result.rows_rejected == 1
    assert result.rows_parsed == 2
    assert result.rows_read == 3

    staged = conn.execute(
        f"SELECT customer_id FROM {result.staging_table} ORDER BY customer_id",  # noqa: S608
    ).fetchall()
    assert [row[0] for row in staged] == ["1", "3"]


def test_retry_starts_from_a_clean_staging_table(conn: psycopg.Connection[Any]) -> None:
    rows = (
        ("1", "Alice", "US", "1990-01-01", "2026-01-01T00:00:00+00:00"),
        ("2", "Bob", "UK", "1992-02-02", "2026-01-02T00:00:00+00:00"),
        ("3", "Carol", "CA", "1985-05-05", "2026-02-01T00:00:00+00:00"),
    )
    ctx = _make_context(run_id=102, source=_FakeSource([rows]))
    loader = StagingLoader(target_columns=TARGET_COLUMNS)

    first = loader.load(ctx, conn)
    conn.commit()
    second = loader.load(ctx, conn)
    conn.commit()

    assert first.staging_table == second.staging_table
    count = conn.execute(f"SELECT COUNT(*) FROM {second.staging_table}").fetchone()  # noqa: S608
    assert count is not None
    assert count[0] == 3  # not 6 -- the retry replaced, rather than appended to, the first attempt


def test_staging_table_is_unlogged_and_survives_the_transaction_commit(
    conn: psycopg.Connection[Any],
    migrated_dsn: str,
) -> None:
    rows = (("1", "Alice", "US", "1990-01-01", "2026-01-01T00:00:00+00:00"),)
    ctx = _make_context(run_id=103, source=_FakeSource([rows]))
    loader = StagingLoader(target_columns=TARGET_COLUMNS)

    result = loader.load(ctx, conn)
    conn.commit()

    with psycopg.connect(migrated_dsn) as separate_conn:
        persistence = separate_conn.execute(
            "SELECT relpersistence FROM pg_class WHERE oid = %s::regclass",
            (result.staging_table,),
        ).fetchone()
        assert persistence is not None
        assert persistence[0] == "u"  # 'u' == UNLOGGED (never 'p' permanent, 't' temporary)

        row_count = separate_conn.execute(
            f"SELECT COUNT(*) FROM {result.staging_table}",  # noqa: S608
        ).fetchone()
        assert row_count is not None
        assert row_count[0] == 1


def test_lineage_columns_match_ctx_run_and_hash_is_computed_once_in_python(
    conn: psycopg.Connection[Any],
) -> None:
    rows = (
        ("1", "Alice", "US", "1990-01-01", "2026-01-01T00:00:00+00:00"),
        ("2", "Bob", "UK", "1992-02-02", "2026-01-02T00:00:00+00:00"),
    )
    ctx = _make_context(run_id=104, source=_FakeSource([rows]), file_id=55, batch_id=77)
    loader = StagingLoader(target_columns=TARGET_COLUMNS)

    result = loader.load(ctx, conn)

    staged = conn.execute(
        f"""
        SELECT customer_id, _run_id, _file_id, _batch_id, _source_row_number,
               _record_hash, _record_hash_version
          FROM {result.staging_table}
         ORDER BY _source_row_number
        """,  # noqa: S608
    ).fetchall()
    assert len(staged) == 2
    for staged_row, expected_row, expected_row_number in zip(
        staged,
        rows,
        (1, 2),
        strict=True,
    ):
        customer_id, run_id, file_id, batch_id, source_row_number, record_hash, hash_version = (
            staged_row
        )
        assert run_id == 104
        assert file_id == 55
        assert batch_id == 77
        assert source_row_number == expected_row_number
        assert hash_version == 1
        assert bytes(record_hash) == hashlib.sha256("|".join(expected_row).encode("utf-8")).digest()
        assert customer_id == expected_row[0]


def test_on_progress_fires_once_per_chunk_with_non_decreasing_cumulative_counts(
    conn: psycopg.Connection[Any],
) -> None:
    chunk_rows = [
        (
            ("1", "Alice", "US", "1990-01-01", "2026-01-01T00:00:00+00:00"),
            ("2", "Bob", "UK", "1992-02-02", "2026-01-02T00:00:00+00:00"),
        ),
        (
            ("3", "Carol", "CA", "1985-05-05", "2026-02-01T00:00:00+00:00"),
            ("4", "Dave", "DE", "1980-03-03", "2026-02-02T00:00:00+00:00"),
        ),
        (
            ("5", "Eve", "FR", "1975-04-04", "2026-03-01T00:00:00+00:00"),
            ("6", "Frank", "IT", "1970-05-05", "2026-03-02T00:00:00+00:00"),
        ),
    ]
    ctx = _make_context(run_id=105, source=_FakeSource(chunk_rows))
    loader = StagingLoader(target_columns=TARGET_COLUMNS)
    progress_calls: list[tuple[int, int]] = []

    def _record_progress(rows_read: int, rows_parsed: int) -> None:
        progress_calls.append((rows_read, rows_parsed))

    result = loader.load(ctx, conn, on_progress=_record_progress)

    assert len(progress_calls) == 3
    assert result.rows_read == 6
    assert result.rows_parsed == 6
    read_values = [call[0] for call in progress_calls]
    parsed_values = [call[1] for call in progress_calls]
    assert read_values == sorted(read_values)
    assert parsed_values == sorted(parsed_values)
    assert read_values[-1] == 6
    assert parsed_values[-1] == 6


def test_on_progress_omitted_does_not_raise_and_changes_no_other_behavior(
    conn: psycopg.Connection[Any],
) -> None:
    rows = (("1", "Alice", "US", "1990-01-01", "2026-01-01T00:00:00+00:00"),)
    ctx = _make_context(run_id=106, source=_FakeSource([rows]))
    loader = StagingLoader(target_columns=TARGET_COLUMNS)

    result = loader.load(ctx, conn)  # no on_progress passed

    assert result.rows_parsed == 1
    assert result.rows_rejected == 0


# --- post-wave-5 code review WR-04: NumericNormalizer.null_sentinels wiring ---

_SCORE_TARGET_COLUMNS = ("customer_id", "score")


def _make_score_config() -> DatasetConfig:
    """A non-nullable ``decimal`` column with its own declared ``null_sentinels`` entry.

    ``score`` is ``nullable=False`` -- the exact gap WR-04 identified:
    ``NullTokenNormalizer`` is only constructed for ``nullable: true`` columns
    (``_build_stages``), so a required numeric column's only path to
    recognizing a literal absent-value sentinel (corpus fixture 59's
    documented use case, e.g. ``"N/A"`` meaning "no score recorded" while the
    field itself must still be present) is ``NumericNormalizer``'s own
    ``null_sentinels`` parameter.
    """
    return DatasetConfig(
        dataset="score_normalization_proof",
        config_schema_version=1,
        source=SourceConfig(
            type="csv",
            bucket="raw",
            path="score-proof/",
            change_semantics="snapshot",
            duplicate_policy="skip",
        ),
        deduplication=DeduplicationConfig(
            strategy="business_key_latest",
            keys=["customer_id"],
            order_by=["customer_id desc"],
        ),
        load=LoadConfig(strategy="merge", target="normalized.score_proof"),
        batching=BatchingConfig(max_units_per_run=100),
        normalization=NormalizationConfig(null_sentinels={"score": ["N/A"]}),
        columns=[
            ColumnContract(
                name="customer_id",
                type="string",
                nullable=False,
                required=True,
                business_key=True,
            ),
            ColumnContract(name="score", type="decimal", nullable=False, required=True),
        ],
    )


def _make_score_context(*, run_id: int, source: _FakeSource) -> PipelineContext:
    return PipelineContext(
        run=RunContext(run_id=run_id, idempotency_key=f"score-proof-{run_id}"),
        config=_make_score_config(),
        metadata=None,  # type: ignore[arg-type] -- unused by StagingLoader.load()
        objects=None,  # type: ignore[arg-type] -- unused by StagingLoader.load()
        db=None,  # type: ignore[arg-type] -- unused by StagingLoader.load()
        log=None,  # type: ignore[arg-type] -- unused by StagingLoader.load()
        source=source,
    )


class _ScoreFakeSource(Source):
    """Mirrors ``_FakeSource`` but sizes ``RecordChunk`` for ``_SCORE_TARGET_COLUMNS``."""

    def __init__(self, rows: tuple[tuple[str, ...], ...]) -> None:
        self._rows = rows

    @contextlib.contextmanager
    def open(self, ctx: PipelineContext) -> Iterator[_FakeRecordStream]:  # noqa: ARG002
        chunk = RecordChunk(
            rows=self._rows,
            first_ordinal=0,
            expected_field_count=len(_SCORE_TARGET_COLUMNS),
        )
        yield _FakeRecordStream([chunk])


def test_numeric_normalizer_null_sentinels_are_wired_from_the_real_pipeline(
    conn: psycopg.Connection[Any],
) -> None:
    """The declared ``"N/A"`` sentinel becomes NULL; an undeclared blank value still rejects.

    Before this fix, ``StagingLoader._build_stages`` never passed
    ``null_sentinels=`` to ``NumericNormalizer``, so every real
    ``NumericNormalizer`` ran with the default empty tuple -- ``"N/A"`` would
    have failed to parse as a ``Decimal`` and been rejected as
    ``invalid-numeric-value`` instead of recognized as absent.
    """
    rows = (
        ("1", "42.50"),  # a real value -- parses normally
        ("2", "N/A"),  # the declared sentinel -- must become NULL, not rejected
        ("3", "not-a-number"),  # NOT the declared sentinel -- must still reject
    )
    ctx = _make_score_context(run_id=201, source=_ScoreFakeSource(rows))
    loader = StagingLoader(target_columns=_SCORE_TARGET_COLUMNS)

    result = loader.load(ctx, conn)

    assert result.rows_parsed == 2  # "1" and "2" -- "3" rejected
    assert result.rows_rejected == 1

    staged = conn.execute(
        f"SELECT customer_id, score FROM {result.staging_table} ORDER BY customer_id",  # noqa: S608
    ).fetchall()
    assert staged == [("1", "42.50"), ("2", None)]
