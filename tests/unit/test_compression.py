"""Unit tests for ``csv_processor.compression`` -- CSV-11's compression dispatch layer.

Every ``<behavior>`` bullet from 06-08-PLAN.md Task 1 has a test here, plus
Task 2's decompression-bomb guard. The ``.gz``/``.zip`` fixtures are the
corpus's own ``61_gzipped.csv.gz``/``71_zipped.csv.zip`` (both wrapping
``01_simple.csv``, generated fresh into a temp dir via
``tools.corpus.generators.generate_corpus`` -- never read from
``tests/fixtures/csv/``, matching this test suite's existing convention
(``tests/unit/test_corpus_semantic_fixtures.py``).

``_NonSeekableStream`` mirrors ``botocore.response.StreamingBody``'s real
shape (``readable()``/``readinto()``/``read()``, no ``seek``/``tell`` --
verified live this session, see ``dataplat.storage.objectstore``'s own
module docstring) so every test here proves the code under test genuinely
never assumes random access, exactly like the real MinIO/S3 response body it
stands in for.
"""

from __future__ import annotations

import csv
import gzip
import io
import zipfile
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from tools.corpus.generators import generate_corpus
from tools.corpus.manifest import load_manifest

from csv_processor.compression import (
    _BOUNDED_READ_CHUNK_BYTES,
    detect_compression,
    open_compressed_stream,
)
from dataplat.errors import FileInspectionError

REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST = REPO_ROOT / "tests" / "fixtures" / "corpus.yaml"

_EXPECTED_HEADER = ["id", "name", "amount"]
_EXPECTED_DATA_ROW_COUNT = 20


class _NonSeekableStream:
    """A response-body test double exposing ONLY the real ``StreamingBody`` surface.

    ``readable()``/``readinto()`` are what ``io.BufferedReader`` needs for
    the ``.gz`` path; ``read()`` is what the ``.zip`` path calls directly.
    Deliberately no ``seek()``/``tell()`` -- proving the code under test
    never assumes random access on the raw object, only on the ``io.BytesIO``
    it explicitly buffers into for the ``.zip`` exception (D-22a).
    """

    def __init__(self, data: bytes) -> None:
        self._buffer = io.BytesIO(data)
        self.closed = False
        self.readinto_calls = 0
        self.read_calls = 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        """Honestly report non-seekability -- deliberately no seek()/tell() at all."""
        return False

    def readinto(self, b: bytearray) -> int:
        self.readinto_calls += 1
        chunk = self._buffer.read(len(b))
        n = len(chunk)
        b[:n] = chunk
        return n

    def read(self, size: int = -1) -> bytes:
        self.read_calls += 1
        return self._buffer.read(size)

    def close(self) -> None:
        self.closed = True


@pytest.fixture(scope="module")
def corpus(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Generate the corpus once, skipping the large-profile fixture."""
    manifest = load_manifest(MANIFEST)
    out_dir = tmp_path_factory.mktemp("compression-corpus")
    generate_corpus(manifest, out_dir, fast=True)
    return out_dir


def _read_records(text: str) -> list[list[str]]:
    """Parse decoded CSV text into records, mirroring the corpus test suite's own helper."""
    return list(csv.reader(io.StringIO(text)))


# --- Task 1: .gz true streaming, .zip's D-22a buffered exception -----------


def test_detect_compression_dispatches_by_extension() -> None:
    assert detect_compression("customers/a.csv.gz") == "gzip"
    assert detect_compression("customers/a.csv.zip") == "zip"
    assert detect_compression("customers/a.csv") is None
    assert detect_compression("customers/a") is None


def test_gzip_streams_correctly_from_a_non_seekable_source_in_genuinely_small_chunks(
    corpus: Path,
) -> None:
    gz_bytes = (corpus / "61_gzipped.csv.gz").read_bytes()
    stream = _NonSeekableStream(gz_bytes)

    text_stream = open_compressed_stream(stream, compression="gzip", encoding="utf-8")
    content = text_stream.read()

    records = _read_records(content)
    assert records[0] == _EXPECTED_HEADER
    assert len(records) - 1 == _EXPECTED_DATA_ROW_COUNT

    # Genuine chunked streaming, not one-shot: io.BufferedReader + GzipFile
    # pull the compressed bytes from the raw non-seekable source across more
    # than one readinto() call -- proving open_compressed_stream never reads
    # the whole compressed (let alone decompressed) content in one call.
    assert stream.readinto_calls > 1
    # And it never called seek()/tell() -- _NonSeekableStream exposes neither,
    # so any attempt would already have raised AttributeError above.


def test_zip_streams_correctly_from_a_non_seekable_source_where_raw_zipfile_would_fail(
    corpus: Path,
) -> None:
    zip_bytes = (corpus / "71_zipped.csv.zip").read_bytes()

    # Contrast case first: a raw zipfile.ZipFile over the SAME non-seekable
    # stream shape genuinely cannot open it -- this is what D-22a's buffered
    # exception exists to work around (06-RESEARCH.md Common Pitfalls #3,
    # verified live there over io.BufferedReader(non_seekable_stream) --
    # matched exactly here, since a bare object with no seek() at all raises
    # a plain AttributeError instead, a weaker and less representative proof).
    with pytest.raises(zipfile.BadZipFile):
        zipfile.ZipFile(io.BufferedReader(_NonSeekableStream(zip_bytes)))  # type: ignore[arg-type]

    stream = _NonSeekableStream(zip_bytes)
    text_stream = open_compressed_stream(stream, compression="zip", encoding="utf-8")
    content = text_stream.read()

    records = _read_records(content)
    assert records[0] == _EXPECTED_HEADER
    assert len(records) - 1 == _EXPECTED_DATA_ROW_COUNT
    # The .zip path reads the whole (compressed) archive via one response_body.read()
    # call, by design (D-22a) -- exactly one read_calls, never seek/tell.
    assert stream.read_calls == 1


def test_zip_archive_with_more_than_one_member_raises_corrupted_archive() -> None:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w") as archive:
        archive.writestr("a.csv", "id,name\n1,a\n")
        archive.writestr("b.csv", "id,name\n2,b\n")
    stream = _NonSeekableStream(buffer.getvalue())

    with pytest.raises(FileInspectionError) as exc_info:
        open_compressed_stream(stream, compression="zip", encoding="utf-8")

    assert exc_info.value.context["diagnostic_code"] == "corrupted-archive"
    assert exc_info.value.context["member_count"] == 2


def test_corrupted_zip_archive_raises_corrupted_archive_never_a_raw_badzipfile() -> None:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w") as archive:
        archive.writestr("a.csv", "id,name\n1,a\n")
    # Truncate to well past any local file header but before the central
    # directory -- a genuinely corrupted/truncated archive, not merely empty.
    truncated = buffer.getvalue()[: len(buffer.getvalue()) // 2]
    stream = _NonSeekableStream(truncated)

    with pytest.raises(FileInspectionError) as exc_info:
        open_compressed_stream(stream, compression="zip", encoding="utf-8")

    assert exc_info.value.context["diagnostic_code"] == "corrupted-archive"


def test_uncompressed_delegates_unchanged_to_open_text_stream(corpus: Path) -> None:
    raw_bytes = (corpus / "01_simple.csv").read_bytes()
    stream = _NonSeekableStream(raw_bytes)

    text_stream = open_compressed_stream(stream, compression=None, encoding="utf-8")
    records = _read_records(text_stream.read())

    assert records[0] == _EXPECTED_HEADER
    assert len(records) - 1 == _EXPECTED_DATA_ROW_COUNT


# --- Task 2: LOAD-07's decompression-bomb bound -----------------------------


def _gzip_of_repeated_bytes(decompressed_size: int) -> bytes:
    """Build a highly-compressible gzip archive of ``decompressed_size`` zero bytes."""
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
        gz.write(b"0" * decompressed_size)
    return buffer.getvalue()


def test_decompression_bomb_is_caught_within_a_small_ceiling_without_full_materialization() -> None:
    # 10,000,000 decompressed bytes of a single repeated byte compress to a
    # tiny archive -- the canonical decompression-bomb shape.
    bomb_bytes = _gzip_of_repeated_bytes(10_000_000)
    ceiling = 100_000  # far below the true 10,000,000-byte payload
    stream = _NonSeekableStream(bomb_bytes)

    text_stream = open_compressed_stream(
        stream,
        compression="gzip",
        encoding="utf-8",
        max_decompressed_bytes=ceiling,
    )

    with pytest.raises(FileInspectionError) as exc_info:
        text_stream.read()

    assert exc_info.value.context["diagnostic_code"] == "decompression-bomb-exceeded"
    # The guard must trip within a handful of bounded chunk reads, never
    # after consuming anywhere near the true 10,000,000-byte payload --
    # bytes_read_before_trip proves it stopped close to the ceiling, not
    # after fully materializing the bomb.
    bytes_read_before_trip = exc_info.value.context["bytes_read_before_trip"]
    assert isinstance(bytes_read_before_trip, int)
    assert bytes_read_before_trip <= ceiling + _BOUNDED_READ_CHUNK_BYTES
    assert bytes_read_before_trip < 1_000_000  # nowhere near the 10,000,000-byte bomb


@settings(max_examples=20)
@given(decompressed_size=st.integers(min_value=1, max_value=2_000_000))
def test_bomb_guard_property_never_exceeds_ceiling_by_more_than_one_bounded_chunk(
    decompressed_size: int,
) -> None:
    """For any decompressed size, the guard either succeeds correctly (under the ceiling)

    or raises having read strictly less than the true payload (over the ceiling) -- proving
    the ceiling is enforced incrementally across the whole size range, not just the one
    fixed 10,000,000-byte case above.
    """
    ceiling = 1_000_000
    payload_bytes = _gzip_of_repeated_bytes(decompressed_size)
    stream = _NonSeekableStream(payload_bytes)

    text_stream = open_compressed_stream(
        stream,
        compression="gzip",
        encoding="utf-8",
        max_decompressed_bytes=ceiling,
    )

    if decompressed_size <= ceiling:
        content = text_stream.read()
        assert len(content) == decompressed_size
    else:
        with pytest.raises(FileInspectionError) as exc_info:
            text_stream.read()
        assert exc_info.value.context["diagnostic_code"] == "decompression-bomb-exceeded"
        bytes_read_before_trip = exc_info.value.context["bytes_read_before_trip"]
        assert isinstance(bytes_read_before_trip, int)
        # Never materializes more than one bounded chunk past the ceiling --
        # this is the guard's actual, load-bearing guarantee (matches the
        # fixed-payload test above).
        assert bytes_read_before_trip <= ceiling + _BOUNDED_READ_CHUNK_BYTES
        if decompressed_size > ceiling + _BOUNDED_READ_CHUNK_BYTES:
            # Only provable once there is genuinely more stream data beyond
            # the one bounded chunk the guard may read past the ceiling.
            # When decompressed_size sits within one chunk of the ceiling,
            # the guard's final bounded read can legitimately drain the
            # entire remaining stream while still tripping -- there is no
            # more data left to prove it stopped strictly "before" the end,
            # and that is not a bomb-safety violation (Hypothesis-found
            # boundary case: decompressed_size=1_000_001, ceiling=1_000_000).
            assert bytes_read_before_trip < decompressed_size
