"""Unit tests for ``csv_processor.compression`` -- CSV-11's compression dispatch layer.

Every ``<behavior>`` bullet from 06-08-PLAN.md Task 1 has a test here. The
``.gz``/``.zip`` fixtures are the corpus's own ``61_gzipped.csv.gz``/
``71_zipped.csv.zip`` (both wrapping ``01_simple.csv``, generated fresh into
a temp dir via ``tools.corpus.generators.generate_corpus`` -- never read
from ``tests/fixtures/csv/``, matching this test suite's existing convention
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
import io
import zipfile
from pathlib import Path

import pytest
from tools.corpus.generators import generate_corpus
from tools.corpus.manifest import load_manifest

from csv_processor.compression import detect_compression, open_compressed_stream
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
