"""``Source``/``RecordStream`` — how a run reads records, independent of the engine.

Deliberately narrower than ``ARCHITECTURE.md`` Q4.3's original sketch, which
attaches two extra schema- and profile-describing attributes to
``RecordStream`` and an ``inspect(ctx)`` method to ``Source`` that returns
the profile one. Neither of those two attributes' types exists anywhere in
this codebase yet — both belong to Phase 6's detection engine
(03-CONTEXT.md's explicit "Out of scope" list) — so this file omits them
rather than depending on types that have no implementation yet. Phase 6 adds
``inspect()`` plus the two attributes once their types exist. This file is
this seam's Phase-3 shape, not its final shape.

``csv_processor.source`` (plan 03-08) is the first concrete ``Source``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Iterator
    from contextlib import AbstractContextManager

    from dataplat.models.record import RecordChunk
    from dataplat.pipeline.protocol import PipelineContext


class RecordStream(Protocol):
    """An open, iterable stream of a source's records, chunked for processing."""

    def chunks(self, *, start_ordinal: int | None = None) -> Iterator[RecordChunk]:
        """Yield this stream's records in bounded chunks.

        Args:
            start_ordinal: The record ordinal to resume from, or ``None`` to
                start from the first record. This is a record ordinal, never
                a byte offset — resuming inside a quoted multiline field is
                not possible (CSV-13, PITFALLS.md #5).

        Returns:
            An iterator of chunks, in ordinal order.
        """
        ...


class Source(Protocol):
    """A pluggable, opaque way to read one dataset's records.

    ``csv_processor.source`` implements this protocol against a CSV file
    today; a future Kafka or database CDC source implements it without
    ``dataplat`` importing anything CSV-specific (ADR-0002, README §29/§95).
    """

    def open(self, ctx: PipelineContext) -> AbstractContextManager[RecordStream]:
        """Open this source for reading, as a context manager.

        Args:
            ctx: The current pipeline context.

        Returns:
            A context manager yielding an open ``RecordStream``.
        """
        ...
