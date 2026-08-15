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


def open_compressed_stream(
    response_body: object,
    *,
    compression: str | None,
    encoding: str,
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

    Returns:
        A text stream over the (possibly decompressed) content, decoded per
        ``encoding``, opened with ``newline=""`` so an embedded ``\r\n``
        inside a quoted CSV field survives unchanged.

    Raises:
        FileInspectionError: ``compression == "zip"`` and the archive is
            corrupted/truncated, or contains anything other than exactly one
            member (both ``diagnostic_code="corrupted-archive"``, D-21).
    """
    if compression is None:
        return open_text_stream(response_body, encoding=encoding)

    if compression == "gzip":
        buffered = io.BufferedReader(response_body)  # type: ignore[type-var]
        decompressed = gzip.GzipFile(fileobj=buffered, mode="rb")
        return io.TextIOWrapper(decompressed, encoding=encoding, newline="", errors="strict")

    if compression == "zip":
        return _open_zip_stream(response_body, encoding=encoding)

    msg = f"unsupported compression {compression!r}"
    raise FileInspectionError(msg, context={"diagnostic_code": "corrupted-archive"})


def _open_zip_stream(response_body: object, *, encoding: str) -> io.TextIOWrapper:
    """Buffer a ``.zip`` archive's compressed bytes, then stream its one member (D-22a).

    Args:
        response_body: The raw object body to read the compressed archive
            bytes from directly (``.read()`` -- never wrapped in
            ``io.BufferedReader``, since the whole archive is about to be
            buffered anyway).
        encoding: Text encoding to decode the decompressed member as.

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

    return io.TextIOWrapper(member, encoding=encoding, newline="", errors="strict")
