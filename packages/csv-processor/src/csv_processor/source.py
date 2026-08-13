"""The first real ``csv_processor`` code: a minimal, working CSV ``Source``.

Implements ``dataplat.sources.protocol.Source``/``RecordStream`` against a
single hardcoded shape (03-CONTEXT.md D-01): UTF-8 encoding, comma
delimiter, header at row 0. No encoding, dialect or header-row detection
lives here -- that is Phase 6's ``csv_processor/detect/`` territory. This
file's entire purpose is proving CSV-13's substance: one ``csv.reader`` over
an ``io.TextIOWrapper(..., newline="")`` stream, chunked in records --
never lines, never byte offsets (PITFALLS.md E1) -- so an embedded newline
inside a quoted field can never land on a chunk boundary.

``chunked_records()`` adapts 03-RESEARCH.md's verified core loop (lines
596-620) to this module's fixed dialect/encoding and to yield
``RecordChunk`` instead of a bare ``(ordinal, rows)`` tuple. ``CsvSource``/
``CsvRecordStream`` are the first concrete implementations of Wave-3's
``Source``/``RecordStream`` protocol (``dataplat.sources.protocol``), ready
for Phase 4's vertical-slice DAG to plug in.
"""

from __future__ import annotations

import contextlib
import csv
import itertools
from typing import TYPE_CHECKING

from dataplat.models.record import RecordChunk
from dataplat.sources.protocol import RecordStream, Source

if TYPE_CHECKING:
    from collections.abc import Iterator
    from io import TextIOWrapper

    from dataplat.pipeline.protocol import PipelineContext

ENCODING = "utf-8"  # D-01: hardcoded, no encoding detection until Phase 6
DIALECT = csv.excel  # D-01: hardcoded comma dialect, no dialect detection until Phase 6
# 1 MiB, explicit and documented -- never left unbounded (an unbounded field
# limit turns a malformed quote into an out-of-memory kill, PITFALLS.md E1).
# This is a Phase-3 default; Phase 6 makes it a per-dataset contract field.
FIELD_SIZE_LIMIT = 1_048_576


def _strip_nul(text_stream: TextIOWrapper) -> Iterator[str]:
    r"""Yield ``text_stream``'s physical lines with NUL characters removed.

    Iterates ``text_stream`` one physical line at a time -- the exact
    granularity ``csv.reader`` itself expects from its source iterable,
    including physical lines that fall inside an open quoted field -- and
    strips ``\x00`` from each line before handing it onward. UTF-8 never
    encodes ``0x00`` as part of a multi-byte sequence (it only ever decodes
    as the standalone codepoint U+0000), so this character-level filter on
    the already-decoded stream is equivalent to filtering the raw bytes for
    this specific concern.

    A NUL byte reaching ``csv.reader`` unfiltered raises
    ``_csv.Error: line contains NUL`` (cpython #71767) -- this generator is
    the sole place that failure mode is prevented.

    Args:
        text_stream: The already-decoded text stream to filter, opened with
            ``newline=""`` by its caller.

    Yields:
        Each physical line of ``text_stream``, NUL-stripped, with its
        original line ending preserved untranslated.
    """
    for line in text_stream:
        yield line.replace("\x00", "")


def chunked_records(text_stream: TextIOWrapper, *, chunk_size: int) -> Iterator[RecordChunk]:
    r"""Stream ``text_stream`` as CSV records, chunked by record ordinal.

    The one ``csv.reader`` this module ever constructs: header at row 0,
    hardcoded UTF-8/comma (D-01, no encoding/dialect/header detection),
    NUL-filtered via ``_strip_nul``, field-size-bounded via
    ``FIELD_SIZE_LIMIT``. Chunk boundaries are counts of *records*, never
    byte offsets or line counts -- resuming inside a quoted multiline field
    from a byte offset is not possible (CSV-13, PITFALLS.md E1). A row whose
    field count differs from the header's is neither padded nor truncated;
    it is yielded exactly as ``csv.reader`` produced it -- classifying it is
    a downstream stage's job (``RaggedRowGuard``), not this function's.

    Args:
        text_stream: An already-decoded text stream, opened with
            ``newline=""`` by its caller (``open_text_stream`` /
            ``ObjectStore.get_object``) so an embedded ``\\r\\n`` inside a
            quoted field survives unchanged.
        chunk_size: The number of records per yielded chunk. The last chunk
            of a file may hold fewer than ``chunk_size`` records.

    Yields:
        ``RecordChunk`` instances in ordinal order, each carrying up to
        ``chunk_size`` rows, a contiguous non-overlapping ``first_ordinal``,
        and the header-derived ``expected_field_count``.
    """
    csv.field_size_limit(FIELD_SIZE_LIMIT)
    reader = csv.reader(_strip_nul(text_stream), dialect=DIALECT)
    header = next(reader)  # D-01: header at row 0, hardcoded -- no detection
    expected_field_count = len(header)
    ordinal = 0
    for batch in itertools.batched(reader, chunk_size):
        yield RecordChunk(
            rows=tuple(tuple(row) for row in batch),
            first_ordinal=ordinal,
            expected_field_count=expected_field_count,
        )
        ordinal += len(batch)


class CsvRecordStream(RecordStream):
    """The open, chunked record stream over one CSV object's text stream."""

    def __init__(self, text_stream: TextIOWrapper, *, chunk_size: int = 1000) -> None:
        """Wrap an already-open text stream for chunked CSV reading.

        Args:
            text_stream: The already-decoded, ``newline=""`` text stream to
                read records from. Ownership (closing it) stays with the
                caller that opened it -- see ``CsvSource.open``.
            chunk_size: The number of records per yielded chunk. Defaults to
                1000.
        """
        self._text_stream = text_stream
        self._chunk_size = chunk_size

    def chunks(self, *, start_ordinal: int | None = None) -> Iterator[RecordChunk]:
        """Yield this stream's records in bounded chunks.

        CSV cannot seek to an arbitrary record without re-reading from the
        top (PITFALLS.md E1), so resuming at ``start_ordinal`` re-streams
        from the beginning and discards whole chunks that end at or before
        it -- it never re-parses partway through a chunk.

        Args:
            start_ordinal: The record ordinal to resume from, or ``None`` to
                start from the first record.

        Yields:
            Chunks in ordinal order, starting with the first chunk that
            overlaps or follows ``start_ordinal``.
        """
        records = chunked_records(self._text_stream, chunk_size=self._chunk_size)
        if start_ordinal is None:
            yield from records
            return
        for chunk in records:
            if chunk.first_ordinal + len(chunk.rows) <= start_ordinal:
                continue
            yield chunk


class CsvSource(Source):
    """A ``Source`` reading one CSV object from ``ctx.objects`` in bounded chunks."""

    def __init__(self, bucket: str, key: str, *, chunk_size: int = 1000) -> None:
        """Name the CSV object this source reads.

        Args:
            bucket: The bucket the CSV object lives in.
            key: The object's key within ``bucket``.
            chunk_size: The number of records per yielded chunk, passed
                through to ``CsvRecordStream``. Defaults to 1000.
        """
        self.bucket = bucket
        self.key = key
        self.chunk_size = chunk_size

    @contextlib.contextmanager
    def open(self, ctx: PipelineContext) -> Iterator[CsvRecordStream]:
        """Open this source's object for reading, as a context manager.

        Args:
            ctx: The current pipeline context. Only ``ctx.objects`` is read
                -- this ``Source`` has no other dependency on the pipeline
                context's other fields.

        Yields:
            An open ``CsvRecordStream`` over the object's text stream. The
            underlying text stream is closed when the context manager
            exits, even if the body raises.
        """
        text_stream = ctx.objects.get_object(self.bucket, self.key)
        try:
            yield CsvRecordStream(text_stream, chunk_size=self.chunk_size)
        finally:
            text_stream.close()
