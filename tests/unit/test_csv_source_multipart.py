"""Unit tests for ``csv_processor.source.CsvSource``'s multipart wiring (06-18-PLAN.md Task 2).

Proves ``CsvSource(bucket=..., key=..., additional_keys=(...))`` reads every
part of a CSV-11 multipart group as one logical record stream through the
REAL ``.open()``/``.chunks()`` call path -- not ``dataplat.discovery.
open_multipart_stream`` in isolation (already proven by
``tests/unit/test_discovery.py::test_open_multipart_stream_reassembles_two_real_parts_into_one_twenty_row_dataset``).

Same ``load_manifest``/``generate_corpus`` pattern, and the same
``_DiskObjectStore`` shape, as ``tests/unit/test_csv_source_inspect.py``
(06-14-PLAN.md Task 2) -- duplicated locally rather than imported across test
modules, matching this test suite's own established per-file test-double
convention (``tests/unit/conftest.py``'s own docstring explicitly prefers
duplication over cross-module test imports, to avoid a ruff F811
false-positive; ``tests/unit/test_discovery.py``'s ``_make_config``/
``_insert_config_version`` is the same precedent).
"""

from __future__ import annotations

import io
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

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
from dataplat.errors import FileInspectionError
from dataplat.models.identity import RunContext
from dataplat.pipeline.protocol import PipelineContext
from dataplat.storage.objectstore import ObjectSummary, open_text_stream

if TYPE_CHECKING:
    from collections.abc import Iterator

    from dataplat.storage.objectstore import ObjectStore

REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST = REPO_ROOT / "tests" / "fixtures" / "corpus.yaml"

_BUCKET = "raw"


@dataclass
class _DiskObjectStore:
    """A minimal ``ObjectStore`` double serving one corpus directory's fixture bytes.

    Mirrors ``tests/unit/test_csv_source_inspect.py``'s own fake of the same
    name/shape (see this module's own docstring for why it is duplicated,
    not imported). Uses the SAME ``open_text_stream`` bridge
    ``test_discovery.py``'s own fake uses, so ``CsvSource.open()``'s raw-byte
    reads exercise the identical code path they would against a real object.
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


@dataclass
class _PoisonedObjectStore:
    """An ``ObjectStore`` double whose ``get_object`` raises ``AssertionError`` if ever called.

    Proves ``CsvSource``'s multipart-group-too-large bound check
    (T-06-34) trips at CONSTRUCTION time, before ``.open()`` ever opens a
    single stream -- if a future regression moved (or removed) that check
    so it only trips lazily inside ``.open()``, this store's own
    ``get_object`` would be reached and raise ``AssertionError`` instead,
    which ``pytest.raises(FileInspectionError)`` below does not catch,
    failing the test loudly rather than passing vacuously.
    """

    def get_object(self, bucket: str, key: str) -> io.TextIOWrapper:  # noqa: ARG002
        msg = "get_object must never be called: the bound check must trip first"
        raise AssertionError(msg)

    def list_objects(self, bucket: str, prefix: str) -> Iterator[ObjectSummary]:
        msg = "not used by this test module"
        raise NotImplementedError(msg)

    def put_object(self, bucket: str, key: str, body: bytes) -> None:  # noqa: ARG002
        msg = "put_object must never be called by this test"
        raise AssertionError(msg)


def _config() -> DatasetConfig:
    """A minimal, valid ``DatasetConfig`` with ``csv:``/``filename:`` left at "detect everything".

    Mirrors ``tests/unit/test_csv_source_inspect.py``'s own ``_config()``:
    ``inspect()``/``open()`` never read ``source``/``deduplication``/
    ``load``/``batching``/``columns`` -- every value here exists purely to
    satisfy ``DatasetConfig``'s required fields and cross-validators.
    """
    return DatasetConfig(
        dataset="csv_source_multipart_fixture",
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
        run=RunContext(run_id=1, idempotency_key="csv-source-multipart-test"),
        config=_config(),
        metadata=None,  # type: ignore[arg-type]  # unused by the code under test
        objects=store,
        db=None,  # type: ignore[arg-type]  # unused by the code under test
        log=None,  # type: ignore[arg-type]  # unused by the code under test
    )


@pytest.fixture(scope="module")
def corpus(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Generate the corpus once for this module, skipping the large profile."""
    manifest = load_manifest(MANIFEST)
    out_dir = tmp_path_factory.mktemp("csv-source-multipart-corpus")
    generate_corpus(manifest, out_dir, fast=True)
    return out_dir


def test_open_recovers_all_20_rows_across_the_part_boundary(corpus: Path) -> None:
    """``62_multipart_split``'s two real parts recover ``01_simple.csv``'s exact 20 rows through

    the real ``CsvSource.open()``/``.chunks()`` call path -- the second part's first row is read
    as DATA, never mistaken for a header (the exact failure this fixture exists to catch;
    corpus.yaml's own comment: "a reader that treats each object as a file... silently drops a
    record").
    """
    store = _DiskObjectStore(root=corpus)
    ctx = _context(store)
    source = CsvSource(
        bucket=_BUCKET,
        key="62_multipart_split/part-00000",
        additional_keys=("62_multipart_split/part-00001",),
    )

    rows: list[tuple[str, ...]] = []
    with source.open(ctx) as stream:
        for chunk in stream.chunks():
            rows.extend(chunk.rows)

    assert len(rows) == 20
    # The row immediately after the 10-row part boundary (0-indexed position
    # 10, the 11th data row) is genuine DATA -- matches 01_simple.csv's own
    # deterministic row 11 (zero_padded_int id, start=1 -> "000011"), never a
    # header-shaped row.
    assert rows[10][0] == "000011"
    # The full id sequence is intact and unduplicated across the part
    # boundary.
    assert [row[0] for row in rows] == [f"{n:06d}" for n in range(1, 21)]


def test_open_with_no_additional_keys_is_unaffected_by_this_plan(corpus: Path) -> None:
    """``customers``' real case (no multipart delivery): the single-part path recovers

    ``01_simple.csv``'s rows completely unaffected by this plan's `additional_keys` addition.
    """
    store = _DiskObjectStore(root=corpus)
    ctx = _context(store)
    source = CsvSource(bucket=_BUCKET, key="01_simple.csv")

    rows: list[tuple[str, ...]] = []
    with source.open(ctx) as stream:
        for chunk in stream.chunks():
            rows.extend(chunk.rows)

    assert len(rows) == 20
    assert [row[0] for row in rows] == [f"{n:06d}" for n in range(1, 21)]


def test_construction_with_51_keys_raises_before_any_stream_opens() -> None:
    """T-06-34's mitigation: the multipart-group bound check runs BEFORE any stream ever opens.

    ``1 + 50 == 51`` parts exceeds the documented 50-part ceiling.
    """
    store = _PoisonedObjectStore()
    ctx = _context(store)

    def _construct_and_open() -> None:
        source = CsvSource(
            bucket=_BUCKET,
            key="62_multipart_split/part-00000",
            additional_keys=tuple(f"fake/part-{n:05d}" for n in range(50)),
        )
        with source.open(ctx) as stream:
            list(stream.chunks())

    with pytest.raises(FileInspectionError) as exc_info:
        _construct_and_open()

    assert exc_info.value.context["diagnostic_code"] == "multipart-group-too-large"
    assert exc_info.value.context["part_count"] == 51
