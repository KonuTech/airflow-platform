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
import io
import itertools
from typing import TYPE_CHECKING, Final

from csv_processor.compression import detect_compression, open_compressed_stream
from csv_processor.detect.dialect import detect_dialect, to_stdlib_dialect
from csv_processor.detect.encoding import DEFAULT_MIN_CONFIDENCE, decode_strict, detect_encoding
from csv_processor.detect.filename import parse_filename
from csv_processor.detect.header import detect_header
from dataplat.models.profile import CsvProfile
from dataplat.models.record import RecordChunk
from dataplat.observability import metrics
from dataplat.sources.protocol import RecordStream, Source

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping
    from io import TextIOWrapper

    from dataplat.pipeline.protocol import PipelineContext

ENCODING = "utf-8"  # D-01: hardcoded, no encoding detection until Phase 6
DIALECT = csv.excel  # D-01: hardcoded comma dialect, no dialect detection until Phase 6
# 1 MiB, explicit and documented -- never left unbounded (an unbounded field
# limit turns a malformed quote into an out-of-memory kill, PITFALLS.md E1).
# This was a Phase-3 default; Phase 6's `CsvSource.open()` now sources this
# bound from `CsvProfile.max_field_bytes` (the dataset contract's own
# `csv.max_field_bytes`, plan 06-14) instead -- kept defined, and still
# `chunked_records`'s own default, purely for backward-compat reference and
# for any caller that constructs a reader with no detected/contract profile
# at all.
FIELD_SIZE_LIMIT = 1_048_576

# T-06-30 (this plan's own threat register): a FIXED, documented bound on
# `CsvSource.inspect()`'s sample read -- matching every detector's own
# already-established ~64 KiB sample-size convention (STACK.md; the
# `encoding.py`/`dialect.py` module docstrings). A future edit must widen
# this only by changing the literal below, visibly, in code review -- never
# by reading more from inside the loop that consumes it.
_INSPECT_SAMPLE_BYTES: Final[int] = 65_536


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

    Stripping a NUL byte is a silent content mutation unless observed
    somewhere (WR-05: the platform's stated Core Value is that "no data is
    ever silently dropped, duplicated, or corrupted"). Every physical line
    that actually contained a NUL increments the ``lines_with_nul_stripped``
    metric once (mirroring the ``rows_rejected``/``rows_kept`` pattern
    ``dataplat.pipeline.engine.RaggedRowGuard`` already establishes) -- named
    "lines", not "rows", because this function genuinely operates one
    physical line at a time, including continuation lines inside an open
    multiline quoted field, not one parsed CSV record at a time.

    Args:
        text_stream: The already-decoded text stream to filter, opened with
            ``newline=""`` by its caller.

    Yields:
        Each physical line of ``text_stream``, NUL-stripped, with its
        original line ending preserved untranslated.
    """
    for line in text_stream:
        if "\x00" in line:
            metrics.increment("lines_with_nul_stripped")
            yield line.replace("\x00", "")
        else:
            yield line


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

    A genuinely empty (zero-byte) ``text_stream`` -- no header, no rows --
    yields zero chunks rather than raising (CR-02): ``next()`` on the
    underlying reader is guarded explicitly, because letting its
    ``StopIteration`` escape unguarded inside this generator would otherwise
    become an opaque ``RuntimeError: generator raised StopIteration``
    (PEP 479) at whichever caller first drives the generator, instead of the
    empty-input outcome an empty file actually represents.

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
        and the header-derived ``expected_field_count``. Yields nothing at
        all for a zero-byte ``text_stream`` (CR-02).
    """
    csv.field_size_limit(FIELD_SIZE_LIMIT)
    reader = csv.reader(_strip_nul(text_stream), dialect=DIALECT)
    try:
        header = next(reader)  # D-01: header at row 0, hardcoded -- no detection
    except StopIteration:
        # Zero-byte input: no header, no rows. Return (yield nothing) instead
        # of letting StopIteration escape this generator as PEP 479's
        # RuntimeError (CR-02).
        return
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

    def inspect(self, ctx: PipelineContext) -> CsvProfile:
        """Run every detector once, aggregating their findings into one ``CsvProfile``.

        The real implementation of Pattern 1 (06-RESEARCH.md): every
        detector runs here, once, before any ``RecordStream`` exists --
        never per-chunk. Reads a single bounded sample --
        ``_INSPECT_SAMPLE_BYTES`` of the object's DEcompressed bytes
        (T-06-30: a fixed, documented bound) -- via ``ctx.objects.
        get_object``'s own public ``TextIOWrapper.buffer`` attribute, the
        same already-reviewed pattern ``dataplat.discovery.discover_files``
        uses for its own raw-bytes content hash (never ``StreamingBody``'s
        forbidden private internal state; ``storage/objectstore.py``'s
        module docstring). Routing that buffer through ``csv_processor.
        compression.open_compressed_stream`` means a ``.gz``/``.zip``
        object's sample is genuinely decompressed CSV content, never opaque
        archive bytes -- ``open_compressed_stream``'s own ``encoding``
        argument is a throwaway placeholder here: only ``.buffer.read()``
        is ever called on its return value, which reads raw bytes and never
        triggers the text layer's decoding, so the placeholder is never
        actually consulted.

        Runs, in this exact order (the order matters -- do not reorder):

        1. ``detect_compression`` -- from the object key alone.
        2. ``detect_encoding`` -- BOM sniff, then contract/charset-normalizer/
           chardet agreement.
        3. ``decode_strict`` -- decode the sample with the detected encoding.
        4. ``detect_dialect`` -- delimiter/quotechar, contract-fallback aware.
        5. ``detect_header`` -- header/preamble/footer, over the sample split
           into rows by the detected (or contract-fallback) dialect. Skipped
           (an empty candidate-row list) when dialect detection declined
           with no contract fallback -- there is no usable delimiter to
           split rows with; ``CsvSource.open()`` is where that condition
           actually raises (plan 06-05's ``to_stdlib_dialect`` design).
        6. ``parse_filename`` -- only when ``ctx.config.filename`` is set;
           a ``FilenameParsingError`` propagates uncaught (D-09: reject the
           whole file).

        Args:
            ctx: The current pipeline context.

        Returns:
            The aggregated profile.

        Raises:
            EncodingDetectionError: The sample cannot be decoded under the
                detected (or contract-declared) encoding.
            FileInspectionError: The header row (contract-given or
                detected) has an exact or case-variant duplicate name, or
                the object is a corrupted or oversized archive.
            FilenameParsingError: ``ctx.config.filename`` is set and the
                object's filename does not match its configured mask (D-09).
        """
        compression_kind = detect_compression(self.key)
        raw_stream = ctx.objects.get_object(self.bucket, self.key)
        # `encoding="utf-8"` is the throwaway placeholder this method's own
        # docstring names -- `sample_stream.close()` below cascades through
        # every layer `open_compressed_stream` built (`gzip.GzipFile`/
        # `_DecompressionBombGuard`/`zipfile.ZipExtFile`, as applicable) down
        # to `raw_stream`'s own underlying connection; `raw_stream` itself is
        # never separately closed.
        sample_stream = open_compressed_stream(
            raw_stream.buffer, compression=compression_kind, encoding="utf-8"
        )
        try:
            # `bytes(...)`: `TextIOWrapper.buffer.read()` is typed against
            # the general `Buffer` protocol (typeshed's `_WrappedBuffer`),
            # not literally `bytes` -- every real implementation underneath
            # (`_DecompressionBombGuard`, `gzip.GzipFile`, a raw
            # `io.BufferedReader`) already returns genuine `bytes` at
            # runtime, so this is a type-narrowing no-op copy, not a
            # behavior change.
            sample_bytes = bytes(sample_stream.buffer.read(_INSPECT_SAMPLE_BYTES))
        finally:
            sample_stream.close()

        encoding_detection = detect_encoding(
            sample_bytes,
            contract_encoding=ctx.config.csv.encoding,
            min_confidence=DEFAULT_MIN_CONFIDENCE,
        )
        decoded_sample = decode_strict(sample_bytes, encoding_detection)
        dialect_detection = detect_dialect(
            decoded_sample, contract_delimiter=ctx.config.csv.delimiter
        )

        if dialect_detection.delimiter is not None:
            # Never raises here: `to_stdlib_dialect` only raises when
            # declined or `delimiter is None`, and this branch already
            # guards both.
            stdlib_dialect = to_stdlib_dialect(dialect_detection)
            candidate_rows = [
                tuple(row)
                for row in csv.reader(io.StringIO(decoded_sample), dialect=stdlib_dialect)
            ]
        else:
            candidate_rows = []
        header_detection = detect_header(
            candidate_rows,
            contract_header_row=ctx.config.csv.header_row,
            header_trim=ctx.config.csv.header_trim,
        )

        filename_facets: Mapping[str, object]
        if ctx.config.filename is not None:
            base_filename = self.key.rsplit("/", 1)[-1]
            filename_facets = parse_filename(ctx.config.filename, base_filename)
        else:
            filename_facets = {}

        return CsvProfile(
            encoding=encoding_detection.encoding,
            encoding_confidence=encoding_detection.confidence,
            encoding_source=encoding_detection.source,
            delimiter=dialect_detection.delimiter,
            quotechar=dialect_detection.quotechar,
            dialect_declined=dialect_detection.declined,
            header_row_index=header_detection.header_row_index,
            header=header_detection.trimmed_header,
            preamble_row_count=header_detection.preamble_row_count,
            footer_row_count=len(header_detection.footer_row_indices),
            max_field_bytes=ctx.config.csv.max_field_bytes,
            compression=compression_kind,
            filename_facets=filename_facets,
        )
