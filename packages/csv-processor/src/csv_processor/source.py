"""``csv_processor``'s real CSV ``Source``: detection-driven, not hardcoded.

Implements ``dataplat.sources.protocol.Source``/``RecordStream``.
03-CONTEXT.md D-01's original hardcoded UTF-8/comma/header-row-0 shape is
gone: ``CsvSource.inspect()`` (plan 06-14) aggregates every Phase 6
detector -- ``csv_processor.compression.detect_compression``,
``csv_processor.detect.encoding.detect_encoding``, ``csv_processor.detect.
dialect.detect_dialect``, ``csv_processor.detect.header.detect_header`` and,
when configured, ``csv_processor.detect.filename.parse_filename`` -- into
one ``dataplat.models.profile.CsvProfile``, and ``CsvSource.open()`` now
actually constructs its reader from what ``inspect()`` found, decompressing
through ``csv_processor.compression.open_compressed_stream`` when the
object is ``.gz``/``.zip``. This file's other enduring purpose, proven since
Phase 3, still holds: one ``csv.reader`` over an
``io.TextIOWrapper(..., newline="")`` stream, chunked in records -- never
lines, never byte offsets (PITFALLS.md E1) -- so an embedded newline inside
a quoted field can never land on a chunk boundary.

``chunked_records()`` adapts 03-RESEARCH.md's verified core loop (lines
596-620), generalized (plan 06-14) to skip a detected/contract preamble,
read the field-size bound from a caller-supplied ``max_field_bytes`` rather
than the module's own retired ``FIELD_SIZE_LIMIT`` constant, and construct
its ``csv.reader`` from a caller-supplied dialect rather than the module's
own retired ``DIALECT`` constant -- while still yielding ``RecordChunk``
instead of a bare ``(ordinal, rows)`` tuple. ``CsvSource``/``CsvRecordStream``
are the first concrete implementations of Wave-3's ``Source``/
``RecordStream`` protocol (``dataplat.sources.protocol``), first plugged in
by Phase 4's vertical-slice DAG.
"""

from __future__ import annotations

import contextlib
import csv
import io
import itertools
from typing import TYPE_CHECKING, Final

from csv_processor.compression import detect_compression, open_compressed_stream
from csv_processor.detect.dialect import DialectDetection, detect_dialect, to_stdlib_dialect
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
# D-01's original hardcoded comma dialect. `CsvSource.open()` (plan 06-14) no
# longer uses this directly -- it constructs the real detected/contract
# dialect via `csv_processor.detect.dialect.to_stdlib_dialect` instead. Kept
# defined, and still `chunked_records`'s own default parameter value, for any
# caller that constructs a reader with no detected/contract profile at all.
DIALECT = csv.excel
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


def chunked_records(
    text_stream: TextIOWrapper,
    *,
    chunk_size: int,
    dialect: type[csv.Dialect] = DIALECT,
    preamble_rows: int = 0,
    max_field_bytes: int = FIELD_SIZE_LIMIT,
) -> Iterator[RecordChunk]:
    r"""Stream ``text_stream`` as CSV records, chunked by record ordinal.

    The one ``csv.reader`` this module ever constructs: NUL-filtered via
    ``_strip_nul``, field-size-bounded via ``max_field_bytes``. Skips
    ``preamble_rows`` rows (a detected or contract-configured metadata
    preamble, ``CsvProfile.preamble_row_count`` in real use), then treats
    the next row as the header (discarded, its field count becomes
    ``expected_field_count``) and every row after that as data. Chunk
    boundaries are counts of *records*, never byte offsets or line counts --
    resuming inside a quoted multiline field from a byte offset is not
    possible (CSV-13, PITFALLS.md E1). A row whose field count differs from
    the header's is neither padded nor truncated; it is yielded exactly as
    ``csv.reader`` produced it -- classifying it is a downstream stage's job
    (``RaggedRowGuard``), not this function's.

    A genuinely empty (zero-byte) ``text_stream``, or one with fewer rows
    than ``preamble_rows + 1`` -- no header, no rows -- yields zero chunks
    rather than raising (CR-02): every ``next()`` on the underlying reader
    is guarded explicitly, because letting its ``StopIteration`` escape
    unguarded inside this generator would otherwise become an opaque
    ``RuntimeError: generator raised StopIteration`` (PEP 479) at whichever
    caller first drives the generator, instead of the empty-input outcome
    an empty (or too-short) file actually represents.

    Args:
        text_stream: An already-decoded text stream, opened with
            ``newline=""`` by its caller (``open_text_stream`` /
            ``ObjectStore.get_object``) so an embedded ``\\r\\n`` inside a
            quoted field survives unchanged.
        chunk_size: The number of records per yielded chunk. The last chunk
            of a file may hold fewer than ``chunk_size`` records.
        dialect: The ``csv.Dialect`` to construct ``csv.reader`` with.
            Defaults to ``DIALECT`` (D-01's historical hardcoded comma
            dialect) for any caller with no detected/contract profile;
            ``CsvSource.open()`` passes the real profile-detected dialect
            (``csv_processor.detect.dialect.to_stdlib_dialect``'s return
            value) instead.
        preamble_rows: Rows to discard before the header row. Defaults to
            ``0`` (D-01's historical header-at-row-0 assumption);
            ``CsvSource.open()`` passes ``CsvProfile.header_row_index``
            instead.
        max_field_bytes: The field-size bound passed to
            ``csv.field_size_limit``. Defaults to ``FIELD_SIZE_LIMIT`` for
            any caller with no detected/contract profile; ``CsvSource.open()``
            passes ``CsvProfile.max_field_bytes`` (the dataset contract's
            own ``csv.max_field_bytes``) instead -- this is the one source of
            truth for the bound; the module constant is never read here.

    Yields:
        ``RecordChunk`` instances in ordinal order, each carrying up to
        ``chunk_size`` rows, a contiguous non-overlapping ``first_ordinal``,
        and the header-derived ``expected_field_count``. Yields nothing at
        all for a zero-byte (or too-short) ``text_stream`` (CR-02).
    """
    csv.field_size_limit(max_field_bytes)
    reader = csv.reader(_strip_nul(text_stream), dialect=dialect)
    for _ in range(preamble_rows):
        try:
            next(reader)  # Discard a detected/contract-configured preamble row.
        except StopIteration:
            # Fewer rows than the preamble alone -- no header, no rows.
            return
    try:
        header = next(reader)  # The header row, at whatever index detection found it.
    except StopIteration:
        # Zero-byte input (or exactly the preamble, no header): no header, no
        # rows. Return (yield nothing) instead of letting StopIteration
        # escape this generator as PEP 479's RuntimeError (CR-02).
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

    def __init__(
        self,
        text_stream: TextIOWrapper,
        *,
        dialect: type[csv.Dialect] = DIALECT,
        preamble_rows: int = 0,
        max_field_bytes: int = FIELD_SIZE_LIMIT,
        chunk_size: int = 1000,
    ) -> None:
        """Wrap an already-open text stream for chunked CSV reading.

        Args:
            text_stream: The already-decoded, ``newline=""`` text stream to
                read records from. Ownership (closing it) stays with the
                caller that opened it -- see ``CsvSource.open``.
            dialect: The ``csv.Dialect`` to construct ``csv.reader`` with,
                passed through to ``chunked_records``. Defaults to
                ``DIALECT`` (D-01's historical hardcoded comma dialect) for
                any caller that constructs this class directly with no
                detected/contract profile.
            preamble_rows: Rows to discard before the header row, passed
                through to ``chunked_records``. Defaults to ``0`` (D-01's
                historical header-at-row-0 assumption).
            max_field_bytes: The field-size bound passed to
                ``chunked_records``' own ``csv.field_size_limit`` call.
                Defaults to ``FIELD_SIZE_LIMIT`` for any caller with no
                detected/contract profile.
            chunk_size: The number of records per yielded chunk. Defaults to
                1000.
        """
        self._text_stream = text_stream
        self._dialect = dialect
        self._preamble_rows = preamble_rows
        self._max_field_bytes = max_field_bytes
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
        records = chunked_records(
            self._text_stream,
            chunk_size=self._chunk_size,
            dialect=self._dialect,
            preamble_rows=self._preamble_rows,
            max_field_bytes=self._max_field_bytes,
        )
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

        Calls ``self.inspect(ctx)`` internally to resolve the profile this
        method builds its reader from -- the only real call site today
        (``dataplat.load.staging.StagingLoader.load``'s
        ``with source.open(ctx) as stream:``) has no pre-computed
        ``CsvProfile`` to pass in, and the ``Source`` protocol's
        ``open(self, ctx)`` signature has no room for one anyway. This means
        every real ``open()`` call performs two separate
        ``ctx.objects.get_object`` fetches -- one bounded-sample fetch
        inside ``inspect()``, one full-object fetch here -- so the sample's
        own bytes are read twice over the wire. Accepted, per this plan's
        own instruction: the sample is bounded (``_INSPECT_SAMPLE_BYTES``),
        so the duplicated portion of the read is cheap regardless of the
        object's real size.

        Args:
            ctx: The current pipeline context. Only ``ctx.objects``/
                ``ctx.config`` are read -- this ``Source`` has no other
                dependency on the pipeline context's other fields.

        Yields:
            An open ``CsvRecordStream`` over the object's (decompressed, if
            applicable) text stream, constructed from ``inspect()``'s
            detected encoding/dialect/header row -- D-01's former hardcoded
            UTF-8/comma/header-row-0 shape is gone. The underlying stream is
            closed when the context manager exits, even if the body raises.

        Raises:
            CsvDialectDetectionError: No usable delimiter was ever
                determined -- neither detected nor contract-supplied. Never
                silently falls back to ``csv.excel``'s comma default (plan
                06-05's ``to_stdlib_dialect`` design).
        """
        profile = self.inspect(ctx)
        # Reconstructed, not the rich detector result itself: `CsvProfile`
        # deliberately stores only `delimiter`/`quotechar`/`dialect_declined`
        # (dataplat must never hold a `csv_processor.detect.*` type). This
        # is the one place `open()` needs `to_stdlib_dialect`'s raise-on-
        # declined behavior, so it rebuilds the shape that function expects
        # from the profile's own plain fields.
        dialect_detection = DialectDetection(
            delimiter=profile.delimiter,
            quotechar=profile.quotechar,
            declined=profile.dialect_declined,
        )
        stdlib_dialect = to_stdlib_dialect(dialect_detection)

        raw_stream = ctx.objects.get_object(self.bucket, self.key)
        # `raw_stream.buffer` -- the already-open `ObjectStore.get_object`
        # result's own public binary-buffer attribute (never `StreamingBody`'s
        # forbidden private internal state; storage/objectstore.py's module
        # docstring, and dataplat.discovery's own precedent) -- gives
        # `open_compressed_stream` a real byte source regardless of
        # compression, since that function has no other way to reach the
        # object's raw bytes through `ObjectStore`'s already-decoding
        # `get_object` return shape.
        content_stream = open_compressed_stream(
            raw_stream.buffer, compression=profile.compression, encoding=profile.encoding
        )
        try:
            # `preamble_rows` skips rows BEFORE the header; `chunked_records`
            # itself then discards exactly one MORE row as the header, so the
            # total skipped before the first data row is
            # `preamble_rows + 1 == profile.header_row_index + 1` -- D-01's
            # header-at-row-0 hardcoding is gone, `profile.header_row_index`
            # names whichever row detection (or the contract) actually
            # found. `None` (no header ever found) falls back to 0:
            # `chunked_records` still discards exactly one row via its own
            # unconditional header read, matching D-01's original
            # always-discard-row-0 behavior for this specific,
            # untested-by-this-plan edge case (corpus fixture
            # 11_no_header.csv) -- not a regression, an unaddressed gap
            # carried forward unchanged.
            preamble_rows = profile.header_row_index if profile.header_row_index is not None else 0
            yield CsvRecordStream(
                content_stream,
                dialect=stdlib_dialect,
                preamble_rows=preamble_rows,
                max_field_bytes=profile.max_field_bytes,
                chunk_size=self.chunk_size,
            )
        finally:
            content_stream.close()

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
