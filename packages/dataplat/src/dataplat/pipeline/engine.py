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

    def apply(self, ctx: PipelineContext, chunk: RecordChunk) -> StageResult:  # noqa: ARG002
        """Split ``chunk`` into rows matching its expected field count and rows that don't.

        Never raises, regardless of how malformed ``chunk.rows`` is (proven
        by the all-rows-ragged case in ``tests/unit/test_pipeline_errors.py``).

        Args:
            ctx: The current pipeline context. Unused: this stage's decision
                depends only on ``chunk``, and the parameter exists to
                satisfy ``StreamingStage``.
            chunk: The chunk to check.

        Returns:
            A ``StageResult`` whose ``chunk`` holds only the well-formed
            rows and whose ``rejected`` holds one ``RejectedRecord`` per
            ragged row.
        """
        kept: list[tuple[str, ...]] = []
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
                        raw_line=",".join(row),
                    )
                )
                continue  # never pad or truncate (polars #10585, CONTEXT.md D-01)
            kept.append(row)

        metrics.increment("rows_rejected", len(rejected))
        metrics.increment("rows_kept", len(kept))
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
