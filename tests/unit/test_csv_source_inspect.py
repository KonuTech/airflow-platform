"""Unit tests for ``csv_processor.source.CsvSource.inspect``/``.open`` (06-14-PLAN.md Task 2).

This is the convergence-point proof named across 06-RESEARCH.md Pattern 1
and this plan's own ``<objective>``: every Wave-2 detector (filename,
encoding, dialect, header, compression) proved itself correct in isolation
against the corpus; this file proves the REAL, wired call chain --
``CsvSource.inspect()`` aggregating all of them into one ``CsvProfile``, and
``CsvSource.open()`` actually consuming that profile to build its reader --
against real corpus fixtures, not detector-level mocks.

Same ``load_manifest``/``generate_corpus`` pattern as ``tests/unit/
test_discovery.py``/``tests/unit/detect/test_header.py``: the corpus is
materialised once per module into a temporary directory, and every
assertion is checked against a fixture's own ``expect:`` block. The fake
``ObjectStore`` below serves a fixture's bytes straight from that directory
via the SAME ``dataplat.storage.objectstore.open_text_stream`` bridge
``test_discovery.py``'s own fake uses -- so ``CsvSource.inspect()``'s
``TextIOWrapper.buffer``-based raw-byte sample read exercises the identical
code path it would against a real MinIO object.

Five fixtures prove this plan's own ``must_haves`` truths:

- ``01_simple.csv`` -- the clean control, encoding/delimiter/header_row_index
  detected correctly, and its rows recovered byte-for-byte through the real
  ``open()`` call path.
- ``06_windows1250.csv`` -- non-UTF-8, semicolon-delimited, proving detection
  (not D-01's old hardcoded UTF-8/comma) actually drives ``open()``.
- ``68_utf8_bom_semicolon_pl_excel.csv`` -- the "everything at once" fixture:
  a UTF-8 mark, a semicolon delimiter and comma-decimal amounts, proving
  dialect detection runs before any numeric interpretation so a
  decimal-comma amount is never split at the comma (T-06-10's mitigation,
  proven at the wired level, not just in ``dialect.py``'s own isolated
  tests).
- ``61_gzipped.csv.gz`` / ``71_zipped.csv.zip`` -- both wrap ``01_simple.csv``
  (CSV-11); both must recover its exact 20 rows through the real ``open()``
  call path, proving compression dispatch is wired end-to-end and not just
  ``open_compressed_stream``'s own already-proven isolated unit tests
  (``tests/unit/test_compression.py``).
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from tools.corpus.generators import generate_corpus
from tools.corpus.manifest import load_manifest

from csv_processor.source import CsvSource
from dataplat.config.model import (
    BatchingConfig,
    DatasetConfig,
    DeduplicationConfig,
    LoadConfig,
    SourceConfig,
)
from dataplat.models.identity import RunContext
from dataplat.pipeline.protocol import PipelineContext
from dataplat.storage.objectstore import ObjectSummary, open_text_stream

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from dataplat.models.profile import CsvProfile
    from dataplat.storage.objectstore import ObjectStore

REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST = REPO_ROOT / "tests" / "fixtures" / "corpus.yaml"

_BUCKET = "raw"

# A BOM-sniffed detection reports the codec that also strips the mark on
# decode ("utf-8-sig") rather than the corpus's simpler "utf-8" vocabulary
# word -- both are the semantically correct answer for a BOM-backed
# detection, not a mismatch (same reconciliation
# tests/unit/detect/test_encoding.py's own `_BOM_SIBLING_NAMES` already
# establishes; scoped down here to the one sibling this file's fixtures
# ever need).
_UTF8_BOM_SIBLING = "utf-8-sig"


@dataclass
class _DiskObjectStore:
    """A minimal ``ObjectStore`` double serving one corpus directory's fixture bytes.

    Uses the SAME ``open_text_stream`` bridge ``test_discovery.py``'s own
    ``_FakeObjectStore`` uses (not a real MinIO -- this is a unit test), so
    ``CsvSource.inspect()``'s raw-byte sample read via ``TextIOWrapper.buffer``
    exercises the identical code path it would against a real object.
    """

    root: Path
    written: dict[tuple[str, str], bytes] = field(default_factory=dict)

    def get_object(self, bucket: str, key: str) -> io.TextIOWrapper:  # noqa: ARG002
        body = (self.root / key).read_bytes()
        return open_text_stream(io.BytesIO(body), encoding="utf-8")

    def list_objects(self, bucket: str, prefix: str) -> Iterator[ObjectSummary]:
        msg = "not used by this test module"
        raise NotImplementedError(msg)

    def put_object(self, bucket: str, key: str, body: bytes) -> None:
        self.written[(bucket, key)] = body


def _config() -> DatasetConfig:
    """A minimal, valid ``DatasetConfig`` with ``csv:``/``filename:`` left at "detect everything".

    ``inspect()``/``open()`` never read ``source``/``deduplication``/``load``/
    ``batching``/``columns`` -- every value here exists purely to satisfy
    ``DatasetConfig``'s required fields and cross-validators, mirroring
    ``tests/unit/test_discovery.py``'s own ``_skip_config`` precedent.
    """
    return DatasetConfig(
        dataset="csv_source_inspect_fixture",
        config_schema_version=1,
        source=SourceConfig(
            type="csv",
            bucket=_BUCKET,
            path="fixtures/",
            change_semantics="snapshot",
            duplicate_policy="skip",
        ),
        deduplication=DeduplicationConfig(strategy="business_key_latest", keys=[], order_by=[]),
        load=LoadConfig(strategy="merge", target="normalized.fixture"),
        batching=BatchingConfig(max_units_per_run=100),
        columns=[],
    )


def _context(store: ObjectStore) -> PipelineContext:
    """Build a minimal ``PipelineContext`` -- only ``objects``/``config`` are read."""
    return PipelineContext(
        run=RunContext(run_id=1, idempotency_key="csv-source-inspect-test"),
        config=_config(),
        metadata=None,  # type: ignore[arg-type]  # unused by the code under test
        objects=store,
        db=None,  # type: ignore[arg-type]  # unused by the code under test
        log=None,  # type: ignore[arg-type]  # unused by the code under test
    )


def _collect_rows(source: CsvSource, ctx: PipelineContext) -> list[tuple[str | bool | None, ...]]:
    """Open ``source`` and flatten every chunk's rows, in ordinal order."""
    rows: list[tuple[str | bool | None, ...]] = []
    with source.open(ctx) as stream:
        for chunk in stream.chunks():
            rows.extend(chunk.rows)
    return rows


def _raw_data_rows(path: Path) -> list[tuple[str, ...]]:
    """Parse ``01_simple.csv`` directly with stdlib ``csv``, dropping its header row.

    An oracle independent of ``CsvSource`` -- mirrors ``tests/unit/detect/
    test_header.py``'s own ``_rows_for`` helper. Only ever called against
    ``01_simple.csv`` itself (encoding ``utf-8``, delimiter ``,``, no mark,
    per its own corpus declaration) -- this file's ``61_gzipped.csv.gz``/
    ``71_zipped.csv.zip`` tests compare their real ``open()`` output against
    THIS same file's rows, since both fixtures wrap it unchanged.
    """
    with path.open(encoding="utf-8", newline="") as handle:
        rows = [tuple(row) for row in csv.reader(handle, delimiter=",")]
    return rows[1:]


@pytest.fixture(scope="module")
def corpus(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Generate the corpus once for this module, skipping the large profile."""
    manifest = load_manifest(MANIFEST)
    out_dir = tmp_path_factory.mktemp("csv-source-inspect-corpus")
    generate_corpus(manifest, out_dir, fast=True)
    return out_dir


@pytest.fixture(scope="module")
def declared() -> Mapping[str, Mapping[str, Any]]:
    """Return each fixture's declared meaning, keyed by fixture name."""
    return {fixture.name: fixture.expect for fixture in load_manifest(MANIFEST).fixtures}


def _matches_declared_encoding(profile: CsvProfile, declared_name: str) -> bool:
    """True if ``profile.encoding`` is ``declared_name``, or its BOM-aware sibling.

    See this module's own docstring / ``_UTF8_BOM_SIBLING`` for why "utf-8"
    and "utf-8-sig" are both correct answers for a BOM-backed detection.
    """
    if profile.encoding == declared_name:
        return True
    return (
        declared_name == "utf-8"
        and profile.encoding == _UTF8_BOM_SIBLING
        and profile.encoding_source == "bom"
    )


@pytest.mark.parametrize(
    "fixture_name",
    ["01_simple.csv", "06_windows1250.csv", "68_utf8_bom_semicolon_pl_excel.csv"],
)
def test_inspect_matches_corpus_declaration(
    fixture_name: str, corpus: Path, declared: Mapping[str, Mapping[str, Any]]
) -> None:
    """``inspect()``'s profile matches its fixture's own declared encoding/delimiter/header."""
    expect = declared[fixture_name]
    store = _DiskObjectStore(root=corpus)
    ctx = _context(store)
    source = CsvSource(bucket=_BUCKET, key=fixture_name)

    profile = source.inspect(ctx)

    if "detected_encoding" in expect:
        assert _matches_declared_encoding(profile, expect["detected_encoding"]), (
            f"{fixture_name}: detected {profile.encoding!r} "
            f"(source={profile.encoding_source!r}), expected {expect['detected_encoding']!r}"
        )
    if "detected_delimiter" in expect:
        assert profile.delimiter == expect["detected_delimiter"], fixture_name
    if "header_row_index" in expect:
        assert profile.header_row_index == expect["header_row_index"], fixture_name


def test_open_recovers_exact_row_content_for_01_simple(corpus: Path) -> None:
    """``CsvSource.open()`` over the plain, uncompressed control recovers every real row."""
    store = _DiskObjectStore(root=corpus)
    ctx = _context(store)
    source = CsvSource(bucket=_BUCKET, key="01_simple.csv")

    rows = _collect_rows(source, ctx)

    expected = _raw_data_rows(corpus / "01_simple.csv")
    assert len(rows) == 20
    assert rows == expected


def test_inspect_leaves_schema_fields_none_without_a_dataset_id(corpus: Path) -> None:
    """``CsvSource`` built with no ``dataset_id`` never touches schema versioning (06-15-PLAN.md).

    ``dataset_id`` defaults to ``None`` -- the same reasoning
    ``dataplat.pipeline.protocol.PipelineContext.source: Source | None =
    None``'s own docstring gives for defaulting a new field to ``None`` --
    so this whole fixture-driven unit-test module (``ctx.db=None``, no real
    database) keeps working unchanged even though ``CsvSource.inspect()``
    now also resolves/classifies schema. ``SchemaRepository`` is never
    constructed at all down this path; the real, live-database proof of
    schema resolution/classification lives in
    ``tests/integration/test_schema_resolution.py``, which supplies a real
    ``dataset_id`` and a real ``ctx.db``.
    """
    store = _DiskObjectStore(root=corpus)
    ctx = _context(store)
    source = CsvSource(bucket=_BUCKET, key="01_simple.csv")

    profile = source.inspect(ctx)

    assert profile.schema_version_id is None
    assert profile.compatibility is None


def test_open_recovers_01_simples_rows_through_gzip(corpus: Path) -> None:
    """``61_gzipped.csv.gz`` recovers exactly ``01_simple.csv``'s 20 rows through ``open()``."""
    store = _DiskObjectStore(root=corpus)
    ctx = _context(store)
    source = CsvSource(bucket=_BUCKET, key="61_gzipped.csv.gz")

    rows = _collect_rows(source, ctx)

    expected = _raw_data_rows(corpus / "01_simple.csv")
    assert len(rows) == 20
    assert rows == expected


def test_open_recovers_01_simples_rows_through_zip(corpus: Path) -> None:
    """``71_zipped.csv.zip`` recovers exactly ``01_simple.csv``'s 20 rows through ``open()``.

    Proves both ``.gz`` and ``.zip`` work through the real, wired
    ``CsvSource.open()`` call path -- not merely ``open_compressed_stream``
    in isolation (``tests/unit/test_compression.py`` already covers that).
    """
    store = _DiskObjectStore(root=corpus)
    ctx = _context(store)
    source = CsvSource(bucket=_BUCKET, key="71_zipped.csv.zip")

    rows = _collect_rows(source, ctx)

    expected = _raw_data_rows(corpus / "01_simple.csv")
    assert len(rows) == 20
    assert rows == expected


def test_open_68_semicolon_delimiter_keeps_decimal_comma_amount_intact(
    corpus: Path, declared: Mapping[str, Mapping[str, Any]]
) -> None:
    """``68``'s ``;`` is chosen as delimiter; every ``kwota`` stays an intact decimal-comma string.

    The "everything at once" proof (T-06-10's mitigation at the wired
    level): a detector that mistook ``,`` for the delimiter would split
    every amount like ``"1234,56"`` into two fields instead of one.
    """
    expect = declared["68_utf8_bom_semicolon_pl_excel.csv"]
    store = _DiskObjectStore(root=corpus)
    ctx = _context(store)
    source = CsvSource(bucket=_BUCKET, key="68_utf8_bom_semicolon_pl_excel.csv")

    profile = source.inspect(ctx)
    assert profile.delimiter == ";"

    rows = _collect_rows(source, ctx)
    assert len(rows) == expect["data_rows"]
    for row in rows:
        # id, klient, kwota -- exactly 3 fields; a comma-as-delimiter bug
        # would instead produce 4 fields for any amount above 999.99, or
        # silently corrupt every amount below it.
        assert len(row) == 3, row
        kwota = row[2]
        assert isinstance(kwota, str)
        assert "," in kwota, f"amount {kwota!r} lost its decimal-comma separator"
