"""Unit tests for ``dataplat.pipeline.engine`` — QUAL-03's errors-as-values proof.

Covers ``RaggedRowGuard`` (a malformed row becomes a ``RejectedRecord``, never
an exception — including the pathological all-rows-ragged case) and
``run_streaming`` (stage sequencing, chunk threading, and the first two real
``metrics``/``tracing`` call sites D-03 requires).

Every ``ctx: PipelineContext`` passed below is a placeholder built by
``_make_context()``: neither ``RaggedRowGuard.apply()`` nor ``run_streaming()``
dereferences any of ``config``/``metadata``/``objects``/``db``/``log`` — only
``chunk`` and the stage sequence matter to the code under test here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dataplat.models.identity import RunContext
from dataplat.models.record import RecordChunk, StageResult
from dataplat.observability import metrics
from dataplat.pipeline.engine import RaggedRowGuard, run_streaming
from dataplat.pipeline.protocol import PipelineContext

if TYPE_CHECKING:
    import pytest

    from dataplat.pipeline.protocol import StreamingStage


def _make_context() -> PipelineContext:
    """Build a placeholder ``PipelineContext`` for stage/engine tests.

    Only ``run`` is populated with a real value; the remaining fields are
    untouched by any code exercised in this file.
    """
    return PipelineContext(
        run=RunContext(run_id=1, idempotency_key="test-run"),
        config=None,  # type: ignore[arg-type] -- unused by the code under test
        metadata=None,  # type: ignore[arg-type] -- unused by the code under test
        objects=None,  # type: ignore[arg-type] -- unused by the code under test
        db=None,  # type: ignore[arg-type] -- unused by the code under test
        log=None,  # type: ignore[arg-type] -- unused by the code under test
    )


def _chunk(
    rows: list[tuple[str, ...]],
    *,
    first_ordinal: int = 0,
    expected_field_count: int = 3,
) -> RecordChunk:
    return RecordChunk(
        rows=tuple(rows),
        first_ordinal=first_ordinal,
        expected_field_count=expected_field_count,
    )


class _UppercaseFirstFieldStage:
    """Fake ``StreamingStage``: uppercases each row's first field."""

    name = "uppercase_first_field"

    def apply(self, ctx: PipelineContext, chunk: RecordChunk) -> StageResult:  # noqa: ARG002
        rows = tuple((row[0].upper(), *row[1:]) for row in chunk.rows)
        return StageResult(chunk=chunk.replace(rows=rows), rejected=[], findings=[])


class _NoOpStage:
    """Fake ``StreamingStage``: returns its input chunk unchanged."""

    name = "noop"

    def apply(self, ctx: PipelineContext, chunk: RecordChunk) -> StageResult:  # noqa: ARG002
        return StageResult(chunk=chunk, rejected=[], findings=[])


# Test 1: a fully well-formed chunk passes through RaggedRowGuard untouched.
def test_ragged_row_guard_passes_through_a_fully_well_formed_chunk() -> None:
    chunk = _chunk([("1", "a", "x"), ("2", "b", "y"), ("3", "c", "z")])
    guard = RaggedRowGuard()

    result = guard.apply(_make_context(), chunk)

    assert result.chunk.rows == chunk.rows
    assert result.rejected == []


# Test 2: exactly the ragged row is rejected, with the correct source_row_number.
def test_ragged_row_guard_rejects_exactly_the_ragged_row_with_correct_ordinal() -> None:
    chunk = _chunk(
        [("1", "a", "x"), ("2", "b"), ("3", "c", "z")],  # index 1 is short a field
        first_ordinal=100,
    )
    guard = RaggedRowGuard()

    result = guard.apply(_make_context(), chunk)

    assert result.chunk.rows == (("1", "a", "x"), ("3", "c", "z"))
    assert len(result.rejected) == 1
    rejected = result.rejected[0]
    assert rejected.error_type == "RAGGED_ROW"
    assert rejected.source_row_number == chunk.first_ordinal + 1


# Test 3: RaggedRowGuard never raises, even when every row is ragged.
def test_ragged_row_guard_never_raises_when_every_row_is_ragged() -> None:
    chunk = _chunk([("only-one",), ("two", "fields"), ("four", "fields", "here", "too")])
    guard = RaggedRowGuard()

    result = guard.apply(_make_context(), chunk)

    assert result.chunk.rows == ()
    assert len(result.rejected) == len(chunk.rows)
    assert all(r.error_type == "RAGGED_ROW" for r in result.rejected)


# Test 4: run_streaming yields one (ordinal, StageResult) per chunk, in order,
# with each stage's output chunk threaded into the next stage.
def test_run_streaming_yields_one_result_per_chunk_with_chunk_threaded_forward() -> None:
    chunks = [
        _chunk([("a", "1", "x")], first_ordinal=0),
        _chunk([("b", "2", "y")], first_ordinal=1),
        _chunk([("c", "3", "z")], first_ordinal=2),
    ]
    stages: list[StreamingStage] = [_UppercaseFirstFieldStage(), _NoOpStage()]

    results = list(run_streaming(_make_context(), chunks, stages))

    assert [ordinal for ordinal, _ in results] == [0, 1, 2]
    # _NoOpStage returning its input unchanged, after _UppercaseFirstFieldStage
    # already ran, proves the second stage received the FIRST stage's output.
    assert [result.chunk.rows for _, result in results] == [
        (("A", "1", "x"),),
        (("B", "2", "y"),),
        (("C", "3", "z"),),
    ]


# Test 5: run_streaming's stage sequence calls metrics.increment at least
# once per processed chunk (via RaggedRowGuard, the first real call site).
def test_run_streaming_increments_metrics_at_least_once_per_chunk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, int]] = []

    def _fake_increment(name: str, value: int = 1, **_labels: str) -> None:
        calls.append((name, value))

    monkeypatch.setattr(metrics, "increment", _fake_increment)

    chunks = [_chunk([("a", "1", "x")]), _chunk([("b", "2", "y")])]
    guard = RaggedRowGuard()

    list(run_streaming(_make_context(), chunks, [guard]))

    assert len(calls) >= len(chunks)
