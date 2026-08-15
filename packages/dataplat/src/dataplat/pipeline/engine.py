"""``RaggedRowGuard`` and ``run_streaming`` — QUAL-03's errors-as-values proof.

``RaggedRowGuard`` is the first concrete ``StreamingStage``: a row whose
field count does not match its chunk's ``expected_field_count`` never raises
and is never padded or truncated (polars #10585, CONTEXT.md D-01) — it
becomes a ``RejectedRecord`` in the returned ``StageResult`` instead.

``run_streaming`` is the generic sequencing loop every later pipeline run
uses: it applies every stage to every chunk in order, threads each stage's
surviving chunk into the next stage, and yields one checkpoint ordinal per
input chunk — the value downstream code records as
``last_committed_chunk_ordinal`` (README §38).

Both threaded observability call sites D-03 requires live here:
``metrics.increment(...)`` inside ``RaggedRowGuard.apply()`` (the first real
call site — everywhere else today, ``dataplat.observability.metrics`` and
``.tracing`` are no-op seams with no caller yet) and
``tracing.start_span(...)`` around each chunk's stage sequence inside
``run_streaming``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dataplat.models.record import RejectedRecord, StageResult
from dataplat.observability import metrics, tracing
from dataplat.pipeline.protocol import StreamingStage

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence

    from dataplat.models.record import RecordChunk
    from dataplat.models.report import ValidationResult
    from dataplat.pipeline.protocol import PipelineContext


class RaggedRowGuard(StreamingStage):
    """Rejects rows whose field count does not match the chunk's expected count.

    The concrete proof of QUAL-03's errors-as-values mechanism: a malformed
    row is data (a ``RejectedRecord``), never an exception.
    """

    name = "ragged_row_guard"

    def __init__(self, *, field_delimiter: str = ",") -> None:
        """Configure the delimiter used to reconstruct a rejected row's text.

        Args:
            field_delimiter: The character ``apply()`` rejoins a rejected
                row's already-parsed fields with, to populate
                ``RejectedRecord.raw_line`` (WR-04). Defaults to ``","``,
                matching this phase's hardcoded comma dialect (D-01). This is
                a constructor parameter, not a read of
                ``csv_processor.source.DIALECT``, because ``dataplat`` (this
                class's package) is source-agnostic and must never import
                ``csv_processor`` (the CSV-specific plugin) --
                ``setup.cfg``'s import-linter contract enforces that
                direction. A future caller that knows the real detected
                delimiter (Phase 6) passes it in here instead.
        """
        self._field_delimiter = field_delimiter

    def apply(self, ctx: PipelineContext, chunk: RecordChunk) -> StageResult:
        """Split ``chunk`` into rows matching its expected field count and rows that don't.

        Never raises, regardless of how malformed ``chunk.rows`` is (proven
        by the all-rows-ragged case in ``tests/unit/test_pipeline_errors.py``).

        Args:
            ctx: The current pipeline context. ``ctx.config.dataset`` labels
                this stage's two ``metrics.increment()`` calls (D-04's
                bounded label set: ``dataset``+``stage``+``status``).
            chunk: The chunk to check.

        Returns:
            A ``StageResult`` whose ``chunk`` holds only the well-formed
            rows and whose ``rejected`` holds one ``RejectedRecord`` per
            ragged row. Each rejected row's ``raw_line`` is a
            *reconstruction* (``self._field_delimiter.join(row)`` over
            already-parsed fields), not necessarily the row's true original
            source text -- see ``RejectedRecord.raw_line``'s docstring
            (WR-04).
        """
        kept: list[tuple[str | bool | None, ...]] = []
        rejected: list[RejectedRecord] = []
        for i, row in enumerate(chunk.rows):
            if len(row) != chunk.expected_field_count:
                rejected.append(
                    RejectedRecord(
                        source_row_number=chunk.first_ordinal + i,
                        error_type="RAGGED_ROW",
                        error_message=(
                            f"expected {chunk.expected_field_count} fields, got {len(row)}"
                        ),
                        # `RecordChunk.rows`'s element type widened to admit
                        # `None`/`bool` (plan 06-11, for normalizers staged
                        # after this guard) -- `str.join` needs `str`, so a
                        # non-str field is defensively rendered rather than
                        # assumed away. In practice a ragged row reaching
                        # this guard is always pre-normalization `str` only
                        # fields; this is belt-and-braces, not a behavior
                        # change for any currently-possible input.
                        raw_line=self._field_delimiter.join(
                            field
                            if isinstance(field, str)
                            else ("" if field is None else str(field))
                            for field in row
                        ),
                    )
                )
                continue  # never pad or truncate (polars #10585, CONTEXT.md D-01)
            kept.append(row)

        # D-04's bounded label set: dataset+stage+status, never an unbounded
        # identity like run_id/file_id/batch_id.
        metrics.increment(
            "rows_rejected",
            len(rejected),
            dataset=ctx.config.dataset,
            stage=self.name,
            status="rejected",
        )
        metrics.increment(
            "rows_kept",
            len(kept),
            dataset=ctx.config.dataset,
            stage=self.name,
            status="kept",
        )
        return StageResult(chunk=chunk.replace(rows=tuple(kept)), rejected=rejected, findings=[])


def run_streaming(
    ctx: PipelineContext,
    chunks: Iterable[RecordChunk],
    stages: Sequence[StreamingStage],
) -> Iterator[tuple[int, StageResult]]:
    """Apply every stage to every chunk in order, yielding one checkpoint per chunk.

    Each stage's surviving ``StageResult.chunk`` becomes the next stage's
    input chunk; every stage's ``rejected``/``findings`` accumulate into one
    merged ``StageResult`` per input chunk.

    Args:
        ctx: The current pipeline context, threaded unchanged into every
            stage's ``apply()`` call.
        chunks: The chunks to process, in order.
        stages: The stages to apply to every chunk, in order.

    Returns:
        An iterator of ``(first_ordinal, StageResult)`` pairs, one per input
        chunk, in input order. ``first_ordinal`` is the checkpoint value
        downstream code records as ``last_committed_chunk_ordinal``.
    """
    for chunk in chunks:
        first_ordinal = chunk.first_ordinal
        current_chunk = chunk
        merged_rejected: list[RejectedRecord] = []
        merged_findings: list[ValidationResult] = []
        with tracing.start_span("pipeline.run_streaming.chunk"):
            for stage in stages:
                result = stage.apply(ctx, current_chunk)
                current_chunk = result.chunk
                merged_rejected.extend(result.rejected)
                merged_findings.extend(result.findings)
        yield (
            first_ordinal,
            StageResult(chunk=current_chunk, rejected=merged_rejected, findings=merged_findings),
        )
