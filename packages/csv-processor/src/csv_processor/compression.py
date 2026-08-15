"""``open_compressed_stream`` -- CSV-11's compression dispatch layer (D-21/D-22/D-22a).

``.gz`` decompresses in a true single-pass stream by wrapping
``io.BufferedReader(response_body)`` with ``gzip.GzipFile(fileobj=...)`` --
zero compromise, exactly like ``dataplat.storage.objectstore.
open_text_stream``'s existing uncompressed path (06-RESEARCH.md Architecture
Patterns Pattern 4, verified live this session against the pinned stdlib).

``.zip`` cannot do this: ``zipfile.ZipFile`` needs to read its central
directory, which lives at the archive's end, before it can open any member
-- a structural property of the ZIP format itself, not a library gap
(06-RESEARCH.md Common Pitfalls #3, verified live: a raw ``zipfile.ZipFile``
over the same non-random-access stream shape ``.gz`` handles fine raises
``BadZipFile``). Per the user-confirmed D-22a resolution, ``.zip``'s
compressed archive bytes are therefore buffered into ``io.BytesIO`` first --
bounded by the archive's *compressed* size (D-21 scopes every archive to
exactly one CSV member, so this is never the decompressed CSV content, and
never disk). Once open, the CSV content still streams out in small,
genuinely bounded chunks exactly like ``.gz``.

``max_decompressed_bytes`` (Task 2, threat register T-06-02) guards both
paths against a decompression bomb: cumulative decompressed bytes are
tracked across the stream's lifetime, in small bounded chunks, and the
ceiling is enforced incrementally -- never by reading the whole stream first
to check its size, which would defeat the entire point of streaming.
"""

from __future__ import annotations

import gzip
import io
import zipfile
from typing import TYPE_CHECKING, Final

from dataplat.errors import FileInspectionError
from dataplat.storage.objectstore import open_text_stream

if TYPE_CHECKING:
    from collections.abc import Mapping

# Extension -> compression-kind dispatch. 06-CONTEXT.md leaves the exact
# dispatch mechanism (extension vs. magic-byte sniffing) to this plan's
# discretion; extension-based dispatch is the simpler, sufficient choice for
# this platform's synthetic, well-formed corpus (06-RESEARCH.md's "Claude's
# Discretion" list does not resolve this itself).
_EXTENSION_COMPRESSION: Final[Mapping[str, str]] = {
    ".gz": "gzip",
    ".zip": "zip",
}

# A generous but bounded default: comfortably above customers_large.csv's
# ~55 MB (the U3 spike's own measured figure), far below what an actual
# decompression-bomb attack would need to be dangerous. Contract-overridable
# via the max_decompressed_bytes keyword -- this is only the platform
# default, applied when a caller does not override it.
_DEFAULT_MAX_DECOMPRESSED_BYTES: Final[int] = 512 * 1024 * 1024  # 512 MiB

# Every underlying decompressor .read() call is capped to this many bytes,
# regardless of what a caller (including io.TextIOWrapper's own "read
# everything" path) requests -- this is what makes the decompression-bomb
# ceiling enforceable incrementally: a single call can never materialize
# more than this many decompressed bytes before the cumulative check in
# _DecompressionBombGuard runs again.
_BOUNDED_READ_CHUNK_BYTES: Final[int] = 65_536  # 64 KiB


def detect_compression(key: str) -> str | None:
    """Dispatch an object key to a compression kind by file extension.

    Extension-based dispatch (rather than magic-byte sniffing) is this
    plan's chosen, documented resolution of 06-CONTEXT.md's open discretion
    point on this question -- the simpler, sufficient choice for this
    platform's synthetic, well-formed corpus.

    Args:
        key: The object's key (or any filename), e.g. ``"customers/a.csv.gz"``.

    Returns:
        ``"gzip"`` for a ``.gz`` key, ``"zip"`` for a ``.zip`` key, ``None``
        for any other extension (including no extension at all).
    """
    for suffix, compression in _EXTENSION_COMPRESSION.items():
        if key.endswith(suffix):
            return compression
    return None


class _DecompressionBombGuard:
    """Wraps a decompressed binary stream, tripping once cumulative bytes exceed a ceiling.

    Every underlying read is capped at ``_BOUNDED_READ_CHUNK_BYTES``
    (T-06-02) regardless of how many bytes the caller requests -- this is
    what makes the ceiling enforceable incrementally: a single call, even
    ``io.TextIOWrapper``'s own "read everything" path, can never materialize
    more than one bounded chunk of decompressed content before the
    cumulative check below runs again.

    Duck-types enough of a binary stream's surface (``readable``/
    ``writable``/``seekable``/``closed``/``read``/``read1``/``close``) for
    ``io.TextIOWrapper`` to accept it directly, mirroring how
    ``io.BufferedReader`` already accepts a duck-typed ``StreamingBody``
    with no formal ``io.RawIOBase`` subclass
    (``dataplat.storage.objectstore``'s own module docstring) -- verified
    live this session that ``io.TextIOWrapper`` needs exactly this surface,
    nothing more.
    """

    def __init__(self, inner: _ReadCloseable, *, max_decompressed_bytes: int) -> None:
        """Wrap ``inner`` (a ``gzip.GzipFile`` or ``zipfile.ZipExtFile``) with the ceiling check.

        Args:
            inner: The real decompressor to read bounded chunks from.
            max_decompressed_bytes: The cumulative decompressed-byte ceiling.
        """
        self._inner = inner
        self._max_decompressed_bytes = max_decompressed_bytes
        self._bytes_read = 0
        self.closed = False

    def readable(self) -> bool:
        """Report this stream as readable, as ``io.TextIOWrapper`` requires."""
        return True

    def writable(self) -> bool:
        """Report this stream as not writable, as ``io.TextIOWrapper`` requires."""
        return False

    def seekable(self) -> bool:
        """Report this stream as not seekable -- reading is genuinely one-directional."""
        return False

    def _read_one_bounded_chunk(self, requested: int | None) -> bytes:
        """Perform exactly one bounded call to the real decompressor, checking the ceiling.

        Args:
            requested: The caller's requested size, or ``None``/negative for
                "as much as available" -- either way, the actual call to
                ``inner.read()`` is capped at ``_BOUNDED_READ_CHUNK_BYTES``.

        Returns:
            The chunk read, which may be empty at genuine EOF.

        Raises:
            FileInspectionError: Cumulative bytes read across this guard's
                lifetime now exceed ``max_decompressed_bytes``
                (``diagnostic_code="decompression-bomb-exceeded"``).
        """
        want = (
            _BOUNDED_READ_CHUNK_BYTES
            if requested is None or requested < 0
            else min(requested, _BOUNDED_READ_CHUNK_BYTES)
        )
        chunk = self._inner.read(want)
        self._bytes_read += len(chunk)
        if self._bytes_read > self._max_decompressed_bytes:
            msg = (
                f"decompressed content exceeds the configured "
                f"{self._max_decompressed_bytes}-byte ceiling"
            )
            raise FileInspectionError(
                msg,
                context={
                    "diagnostic_code": "decompression-bomb-exceeded",
                    "max_decompressed_bytes": self._max_decompressed_bytes,
                    "bytes_read_before_trip": self._bytes_read,
                },
            )
        return chunk

    def read1(self, size: int = -1) -> bytes:
        """A single bounded read; short reads are allowed (``io.TextIOWrapper``'s fast path).

        Args:
            size: The caller's requested size, or ``-1`` for "as much as
                available in one call".

        Returns:
            Up to ``size`` bytes (never more than
            ``_BOUNDED_READ_CHUNK_BYTES``), possibly fewer even before EOF --
            that short-read behavior is exactly what ``read1`` promises.
        """
        return self._read_one_bounded_chunk(size)

    def read(self, size: int = -1) -> bytes:
        """The full ``read(size)`` contract: loop bounded reads until ``size``/EOF.

        Never delegates a caller's ``size=-1`` ("read everything") straight
        to the real decompressor -- that single call could materialize an
        entire decompression-bomb payload before this guard ever gets a
        chance to check it (T-06-02). Looping bounded reads instead means
        the cumulative ceiling is checked after every
        ``_BOUNDED_READ_CHUNK_BYTES``, never after consuming the whole
        stream.

        Args:
            size: The number of bytes requested, or ``-1``/negative to read
                until EOF.

        Returns:
            Exactly ``size`` bytes (or fewer only at genuine EOF) when
            ``size`` is non-negative; the entire remaining stream when
            ``size`` is negative.
        """
        pieces: list[bytes] = []
        if size is not None and size >= 0:
            remaining = size
            while remaining > 0:
                chunk = self._read_one_bounded_chunk(remaining)
                if not chunk:
                    break
                pieces.append(chunk)
                remaining -= len(chunk)
            return b"".join(pieces)
        while True:
            chunk = self._read_one_bounded_chunk(None)
            if not chunk:
                break
            pieces.append(chunk)
        return b"".join(pieces)

    def close(self) -> None:
        """Close the wrapped decompressor and mark this guard closed."""
        self._inner.close()
        self.closed = True


if TYPE_CHECKING:
    from typing import Protocol

    class _ReadCloseable(Protocol):
        """The minimal surface ``_DecompressionBombGuard`` needs from what it wraps."""

        def read(self, size: int = ...) -> bytes: ...
        def close(self) -> None: ...


def open_compressed_stream(
    response_body: object,
    *,
    compression: str | None,
    encoding: str,
    max_decompressed_bytes: int = _DEFAULT_MAX_DECOMPRESSED_BYTES,
) -> io.TextIOWrapper:
    r"""Open one object's bytes as a text stream, decompressing if declared.

    Args:
        response_body: The raw object body -- a boto3 ``StreamingBody`` (or
            any object exposing the same ``readable()``/``readinto()``/
            ``read()`` surface, e.g. this module's own tests' non-seekable
            test doubles). Never assumed seekable.
        compression: ``None`` to delegate unchanged to
            ``dataplat.storage.objectstore.open_text_stream`` (the existing
            uncompressed path), ``"gzip"`` for a true single-pass ``.gz``
            stream, or ``"zip"`` for D-22a's buffered-archive-bytes
            exception.
        encoding: Text encoding to decode the decompressed content as.
        max_decompressed_bytes: The decompression-bomb ceiling (T-06-02).
            Defaults to a platform-wide sane bound; a dataset contract may
            override it. Ignored when ``compression`` is ``None`` -- an
            uncompressed object's decompressed size already equals its
            on-disk size, so no separate expansion-ratio attack surface
            exists there.

    Returns:
        A text stream over the (possibly decompressed) content, decoded per
        ``encoding``, opened with ``newline=""`` so an embedded ``\r\n``
        inside a quoted CSV field survives unchanged.

    Raises:
        FileInspectionError: ``compression == "zip"`` and the archive is
            corrupted/truncated, or contains anything other than exactly one
            member (both ``diagnostic_code="corrupted-archive"``, D-21); or
            either path's cumulative decompressed bytes exceed
            ``max_decompressed_bytes``
            (``diagnostic_code="decompression-bomb-exceeded"``).
    """
    if compression is None:
        return open_text_stream(response_body, encoding=encoding)

    if compression == "gzip":
        buffered = io.BufferedReader(response_body)  # type: ignore[type-var]
        decompressed = gzip.GzipFile(fileobj=buffered, mode="rb")
        guarded = _DecompressionBombGuard(
            decompressed,
            max_decompressed_bytes=max_decompressed_bytes,
        )
        return io.TextIOWrapper(guarded, encoding=encoding, newline="", errors="strict")  # type: ignore[arg-type]

    if compression == "zip":
        return _open_zip_stream(
            response_body,
            encoding=encoding,
            max_decompressed_bytes=max_decompressed_bytes,
        )

    msg = f"unsupported compression {compression!r}"
    raise FileInspectionError(msg, context={"diagnostic_code": "corrupted-archive"})


def _open_zip_stream(
    response_body: object,
    *,
    encoding: str,
    max_decompressed_bytes: int,
) -> io.TextIOWrapper:
    """Buffer a ``.zip`` archive's compressed bytes, then stream its one member (D-22a).

    Args:
        response_body: The raw object body to read the compressed archive
            bytes from directly (``.read()`` -- never wrapped in
            ``io.BufferedReader``, since the whole archive is about to be
            buffered anyway).
        encoding: Text encoding to decode the decompressed member as.
        max_decompressed_bytes: The decompression-bomb ceiling (T-06-02).

    Returns:
        A text stream over the archive's single member's decompressed
        content.

    Raises:
        FileInspectionError: The archive (or its one member) is corrupted or
            truncated, or the archive holds anything other than exactly one
            member -- both ``diagnostic_code="corrupted-archive"`` (D-21).
    """
    # D-22a: the compressed ARCHIVE bytes are buffered in memory -- bounded
    # by the archive's compressed size (D-21 scopes this to exactly one CSV
    # per archive), never the decompressed CSV content, never disk.
    # zipfile.ZipFile structurally requires random access to the archive's
    # end-of-file member index before it can open anything -- verified live
    # in 06-RESEARCH.md, not a library gap this module can route around
    # without buffering (or a new dependency -- rejected in 06-RESEARCH.md's
    # Alternatives Considered).
    compressed_bytes = response_body.read()  # type: ignore[attr-defined]

    try:
        archive = zipfile.ZipFile(io.BytesIO(compressed_bytes))
    except zipfile.BadZipFile as exc:
        msg = "corrupted or truncated zip archive"
        raise FileInspectionError(
            msg,
            context={"diagnostic_code": "corrupted-archive"},
        ) from exc

    with archive:
        names = archive.namelist()
        if len(names) != 1:
            msg = f"zip archive must contain exactly one member (D-21), found {len(names)}"
            raise FileInspectionError(
                msg,
                context={
                    "diagnostic_code": "corrupted-archive",
                    "member_count": len(names),
                },
            )
        try:
            # archive.open() returns a zipfile.ZipExtFile. Closing `archive`
            # below (this `with` block's own exit) does not invalidate it:
            # verified live this session -- ZipExtFile reads directly from
            # the underlying io.BytesIO, which stays alive via this closure,
            # independent of the parent ZipFile object's own lifecycle.
            member = archive.open(names[0])
        except zipfile.BadZipFile as exc:
            msg = f"zip archive member {names[0]!r} is corrupted"
            raise FileInspectionError(
                msg,
                context={"diagnostic_code": "corrupted-archive"},
            ) from exc

    guarded = _DecompressionBombGuard(member, max_decompressed_bytes=max_decompressed_bytes)
    return io.TextIOWrapper(guarded, encoding=encoding, newline="", errors="strict")  # type: ignore[arg-type]
