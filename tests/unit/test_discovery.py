"""Unit tests for ``dataplat.discovery.discover_files`` against fake object-store/metadata doubles.

04-03-PLAN.md Task 2's own acceptance criterion permits a unit-level proof
against fakes, with the dedicated ``tests/integration/test_discover_files.py``
(a later plan) proving the same behavior against real testcontainers
Postgres/MinIO. Every ``<behavior>`` bullet from 04-03-PLAN.md Task 2 has a
test here.

The three fake doubles below implement exactly the ``MetadataRepository``/
``ObjectStore``/``SchemaRepository`` surface ``discover_files`` calls -- all
in-memory, single-dataset-scoped, and the object-store fake uses the SAME
``dataplat.storage.objectstore.open_text_stream`` bridge the real
``S3ObjectStore`` uses, so ``discover_files``'s raw-bytes hashing via
``stream.buffer.read(...)`` exercises the identical code path it would
against a real MinIO object. ``_FakeSchemaRepository`` (plan 06-16) is
duck-typed and ``cast()`` at its call sites rather than subclassed:
``dataplat.schema.repository.SchemaRepository`` is a concrete class, not a
``Protocol`` like the other two -- ``cast()`` here is this codebase's own
established narrowing idiom at exactly this kind of test-double boundary.
None of this file's tests need a real schema version -- that is the
Pitfall-5 regression test's own job, exercised directly against the real
formula, not through this fake.
"""

from __future__ import annotations

import csv
import hashlib
import io
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
from tools.corpus.generators import generate_corpus
from tools.corpus.manifest import load_manifest

from dataplat.config.model import (
    BatchingConfig,
    ColumnContract,
    DatasetConfig,
    DeduplicationConfig,
    LoadConfig,
    SourceConfig,
)
from dataplat.discovery import (
    DiscoveredUnit,
    discover_files,
    group_multipart_units,
    open_multipart_stream,
)
from dataplat.errors import FileInspectionError
from dataplat.storage.objectstore import ObjectSummary, open_text_stream

if TYPE_CHECKING:
    from collections.abc import Iterator

    from dataplat.schema.repository import SchemaRepository

REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST = REPO_ROOT / "tests" / "fixtures" / "corpus.yaml"

_DATASET_ID = 1
_DATASET_NAME = "customers"
_CONFIG_VERSION_ID = 7
_CONFIG_HASH = "config-hash-fixture"
_PROCESSOR_IMAGE = "sha256:processor-image-fixture"
_PROCESSOR_VERSION = "0.1.0"
_LAST_MODIFIED = datetime(2026, 8, 13, tzinfo=UTC)


@dataclass
class _FakeSchemaRepository:
    """A minimal schema-repository double: ``get_current()`` always reports "no schema yet".

    ``discover_files`` calls only ``get_current(dataset_id)`` -- this fake
    exists purely so the now-required ``schema`` parameter stays satisfiable
    without a real database; it never returns a populated
    ``SchemaVersionRecord``, so every test in this file computes its
    idempotency key with an empty ``schema_version`` term (Pitfall 5's own
    documented "no schema yet" fallback).
    """

    def get_current(self, dataset_id: int) -> None:
        del dataset_id
        return None


def _fake_schema() -> SchemaRepository:
    """Build a ``_FakeSchemaRepository``, ``cast()`` to ``SchemaRepository`` for callers."""
    return cast("SchemaRepository", _FakeSchemaRepository())


def _skip_config(*, max_units_per_run: int = 100) -> DatasetConfig:
    # columns= is required (06-02 Task 1/3, D-18) -- added here purely to stay
    # constructible; discover_files itself never reads DatasetConfig.columns.
    return DatasetConfig(
        dataset=_DATASET_NAME,
        config_schema_version=1,
        source=SourceConfig(
            type="csv",
            bucket="raw",
            path="customers/",
            change_semantics="snapshot",
            duplicate_policy="skip",
        ),
        deduplication=DeduplicationConfig(
            strategy="business_key_latest",
            keys=["customer_id"],
            order_by=["event_ts desc"],
        ),
        load=LoadConfig(strategy="merge", target="normalized.customers"),
        batching=BatchingConfig(max_units_per_run=max_units_per_run),
        columns=[
            ColumnContract(
                name="customer_id",
                type="string",
                nullable=False,
                required=True,
                business_key=True,
                description="Natural business key for a customer record",
            ),
            ColumnContract(name="name", type="string", nullable=False, required=True),
            ColumnContract(name="country", type="string", nullable=False, required=True),
            ColumnContract(
                name="birth_date",
                type="date",
                nullable=True,
                required=True,
                format="%Y-%m-%d",
            ),
            ColumnContract(
                name="event_ts",
                type="timestamp",
                nullable=False,
                required=True,
                format="%Y-%m-%dT%H:%M:%S%z",
            ),
        ],
    )


@dataclass
class _FakeObjectStore:
    """An in-memory ``ObjectStore`` double: ``{(bucket, key): bytes}``."""

    objects: dict[tuple[str, str], bytes] = field(default_factory=dict)
    written: dict[tuple[str, str], bytes] = field(default_factory=dict)

    def put(self, bucket: str, key: str, body: bytes) -> None:
        """Test-only seeding helper -- NOT part of the ObjectStore Protocol."""
        self.objects[(bucket, key)] = body

    def list_objects(self, bucket: str, prefix: str) -> Iterator[ObjectSummary]:
        for (obj_bucket, key), body in sorted(self.objects.items()):
            if obj_bucket == bucket and key.startswith(prefix):
                yield ObjectSummary(
                    key=key,
                    etag="fake-etag",
                    size_bytes=len(body),
                    last_modified=_LAST_MODIFIED,
                )

    def get_object(self, bucket: str, key: str) -> io.TextIOWrapper:
        body = self.objects[(bucket, key)]
        return open_text_stream(io.BytesIO(body), encoding="utf-8")

    def put_object(self, bucket: str, key: str, body: bytes) -> None:
        self.written[(bucket, key)] = body
        self.objects[(bucket, key)] = body


@dataclass
class _FakeFileRow:
    file_id: int
    content_sha256: bytes
    duplicate_of_file_id: int | None


@dataclass
class _FakeRunRow:
    run_id: int
    status: str


@dataclass
class _FakeMetadataRepository:
    """An in-memory ``MetadataRepository`` double covering exactly what ``discover_files`` calls."""

    files_by_uri: dict[str, _FakeFileRow] = field(default_factory=dict)
    runs_by_key: dict[str, _FakeRunRow] = field(default_factory=dict)
    batches_by_key: dict[tuple[int, str], int] = field(default_factory=dict)
    _next_file_id: int = 1
    _next_batch_id: int = 1
    _next_run_id: int = 1

    def find_file_by_content_hash(self, *, dataset_id: int, content_sha256: bytes) -> int | None:
        del dataset_id
        for row in self.files_by_uri.values():
            if row.content_sha256 == content_sha256:
                return row.file_id
        return None

    def create_file(  # noqa: PLR0913 -- mirrors the real Protocol's column set
        self,
        *,
        dataset_id: int,
        object_uri: str,
        content_sha256: bytes,
        hash_version: int,
        size_bytes: int,
        filename: str,
        status: str,
        duplicate_of_file_id: int | None = None,
    ) -> int:
        del dataset_id, hash_version, size_bytes, filename, status
        existing = self.files_by_uri.get(object_uri)
        if existing is not None:
            existing.duplicate_of_file_id = duplicate_of_file_id
            return existing.file_id
        file_id = self._next_file_id
        self._next_file_id += 1
        self.files_by_uri[object_uri] = _FakeFileRow(
            file_id=file_id,
            content_sha256=content_sha256,
            duplicate_of_file_id=duplicate_of_file_id,
        )
        return file_id

    def get_or_create_batch(self, *, dataset_id: int, batch_key: str, status: str) -> int:
        del status
        existing = self.batches_by_key.get((dataset_id, batch_key))
        if existing is not None:
            return existing
        batch_id = self._next_batch_id
        self._next_batch_id += 1
        self.batches_by_key[(dataset_id, batch_key)] = batch_id
        return batch_id

    def link_batch_file(self, *, batch_id: int, file_id: int, sequence_no: int) -> None:
        del batch_id, file_id, sequence_no

    def get_or_create_ingestion_run(  # noqa: PLR0913 -- mirrors the real Protocol's column set
        self,
        *,
        idempotency_key: str,
        dataset_id: int,
        config_version_id: int,
        processor_version: str,
        processor_image_digest: str,
        file_id: int | None = None,
        batch_id: int | None = None,
    ) -> tuple[int, str]:
        del dataset_id, config_version_id, processor_version, processor_image_digest
        del file_id, batch_id
        existing = self.runs_by_key.get(idempotency_key)
        if existing is not None:
            return existing.run_id, existing.status
        run_id = self._next_run_id
        self._next_run_id += 1
        self.runs_by_key[idempotency_key] = _FakeRunRow(run_id=run_id, status="PENDING")
        return run_id, "PENDING"

    def force_run_status(self, run_id: int, status: str) -> None:
        """Test-only helper: force one run's status, simulating a completed prior attempt."""
        for row in self.runs_by_key.values():
            if row.run_id == run_id:
                row.status = status
                return


def test_discover_files_finds_three_new_objects_and_freezes_three_assignments() -> None:
    objects = _FakeObjectStore()
    objects.put("raw", "customers/a.csv", b"a content")
    objects.put("raw", "customers/b.csv", b"b content")
    objects.put("raw", "customers/c.csv", b"c content")
    metadata = _FakeMetadataRepository()

    units = discover_files(
        metadata=metadata,
        objects=objects,
        dataset_id=_DATASET_ID,
        dataset_name=_DATASET_NAME,
        config=_skip_config(),
        config_version_id=_CONFIG_VERSION_ID,
        config_hash=_CONFIG_HASH,
        processor_image=_PROCESSOR_IMAGE,
        processor_version=_PROCESSOR_VERSION,
        schema=_fake_schema(),
    )

    assert len(units) == 3
    assert all(isinstance(unit, DiscoveredUnit) for unit in units)
    assert len(metadata.files_by_uri) == 3
    assert len(metadata.runs_by_key) == 3
    assert all(row.status == "PENDING" for row in metadata.runs_by_key.values())
    # Exactly one frozen assignment document per unit, written to
    # s3://metadata/assignments/<dataset>/<run_id>.json.
    assert len(objects.written) == 3
    for bucket, key in objects.written:
        assert bucket == "metadata"
        assert key.startswith(f"assignments/{_DATASET_NAME}/")


def test_discover_files_re_offers_still_pending_runs_on_a_second_call_with_no_new_rows() -> None:
    """04-03-PLAN.md Task 2 behavior bullet 2's own self-correction: a still-`PENDING` run

    (never attempted) MUST be re-offered on a second call -- only a `SUCCEEDED` run is
    excluded (see the next test). "Re-run over unchanged objects creates no duplicate
    rows" is proven here by `file_id`/`run_id` identity staying stable across both calls,
    not by the second call returning zero units.
    """
    objects = _FakeObjectStore()
    objects.put("raw", "customers/a.csv", b"a content")
    objects.put("raw", "customers/b.csv", b"b content")
    objects.put("raw", "customers/c.csv", b"c content")
    metadata = _FakeMetadataRepository()

    first_units = discover_files(
        metadata=metadata,
        objects=objects,
        dataset_id=_DATASET_ID,
        dataset_name=_DATASET_NAME,
        config=_skip_config(),
        config_version_id=_CONFIG_VERSION_ID,
        config_hash=_CONFIG_HASH,
        processor_image=_PROCESSOR_IMAGE,
        processor_version=_PROCESSOR_VERSION,
        schema=_fake_schema(),
    )
    assert len(first_units) == 3

    second_units = discover_files(
        metadata=metadata,
        objects=objects,
        dataset_id=_DATASET_ID,
        dataset_name=_DATASET_NAME,
        config=_skip_config(),
        config_version_id=_CONFIG_VERSION_ID,
        config_hash=_CONFIG_HASH,
        processor_image=_PROCESSOR_IMAGE,
        processor_version=_PROCESSOR_VERSION,
        schema=_fake_schema(),
    )

    # Still PENDING -> re-offered: the same 3 run_ids come back, not 0.
    assert {unit.run_id for unit in second_units} == {unit.run_id for unit in first_units}
    # No new meta.files/meta.ingestion_runs rows were created by the re-run
    # -- the row counts stay exactly 3, proving create_file/
    # get_or_create_ingestion_run's idempotent upserts (and this function's
    # own self-duplicate correction) prevented duplication.
    assert len(metadata.files_by_uri) == 3
    assert len(metadata.runs_by_key) == 3
    assert all(row.duplicate_of_file_id is None for row in metadata.files_by_uri.values())


def test_discover_files_re_offers_pending_runs_but_excludes_succeeded_ones() -> None:
    objects = _FakeObjectStore()
    objects.put("raw", "customers/a.csv", b"a content")
    objects.put("raw", "customers/b.csv", b"b content")
    objects.put("raw", "customers/c.csv", b"c content")
    metadata = _FakeMetadataRepository()

    first_units = discover_files(
        metadata=metadata,
        objects=objects,
        dataset_id=_DATASET_ID,
        dataset_name=_DATASET_NAME,
        config=_skip_config(),
        config_version_id=_CONFIG_VERSION_ID,
        config_hash=_CONFIG_HASH,
        processor_image=_PROCESSOR_IMAGE,
        processor_version=_PROCESSOR_VERSION,
        schema=_fake_schema(),
    )
    assert len(first_units) == 3
    # Manually mark exactly one of the three runs SUCCEEDED between calls.
    metadata.force_run_status(first_units[0].run_id, "SUCCEEDED")

    second_units = discover_files(
        metadata=metadata,
        objects=objects,
        dataset_id=_DATASET_ID,
        dataset_name=_DATASET_NAME,
        config=_skip_config(),
        config_version_id=_CONFIG_VERSION_ID,
        config_hash=_CONFIG_HASH,
        processor_image=_PROCESSOR_IMAGE,
        processor_version=_PROCESSOR_VERSION,
        schema=_fake_schema(),
    )

    assert len(second_units) == 2
    assert first_units[0].run_id not in {unit.run_id for unit in second_units}


def test_discover_files_marks_a_content_duplicate_under_a_different_object_uri() -> None:
    objects = _FakeObjectStore()
    objects.put("raw", "customers/original.csv", b"identical content")
    objects.put("raw", "customers/reupload.csv", b"identical content")
    metadata = _FakeMetadataRepository()

    units = discover_files(
        metadata=metadata,
        objects=objects,
        dataset_id=_DATASET_ID,
        dataset_name=_DATASET_NAME,
        config=_skip_config(),
        config_version_id=_CONFIG_VERSION_ID,
        config_hash=_CONFIG_HASH,
        processor_image=_PROCESSOR_IMAGE,
        processor_version=_PROCESSOR_VERSION,
        schema=_fake_schema(),
    )

    # Only the first (sorted-by-key: original.csv) object gets a run; the
    # second is recorded as a duplicate and excluded (D-13 skip policy).
    assert len(units) == 1
    assert len(metadata.files_by_uri) == 2
    original_row = metadata.files_by_uri["s3://raw/customers/original.csv"]
    reupload_row = metadata.files_by_uri["s3://raw/customers/reupload.csv"]
    assert original_row.duplicate_of_file_id is None
    assert reupload_row.duplicate_of_file_id == original_row.file_id
    assert len(metadata.runs_by_key) == 1


def test_discover_files_caps_units_deterministically_and_does_not_lose_the_rest() -> None:
    objects = _FakeObjectStore()
    for index in range(5):
        objects.put("raw", f"customers/file-{index}.csv", f"content {index}".encode())
    metadata = _FakeMetadataRepository()

    units = discover_files(
        metadata=metadata,
        objects=objects,
        dataset_id=_DATASET_ID,
        dataset_name=_DATASET_NAME,
        config=_skip_config(max_units_per_run=2),
        config_version_id=_CONFIG_VERSION_ID,
        config_hash=_CONFIG_HASH,
        processor_image=_PROCESSOR_IMAGE,
        processor_version=_PROCESSOR_VERSION,
        schema=_fake_schema(),
    )

    assert len(units) == 2
    # The excluded remainder still exists, PENDING, in meta.files/
    # meta.ingestion_runs -- upserted, not lost.
    assert len(metadata.files_by_uri) == 5
    assert len(metadata.runs_by_key) == 5
    assert all(row.status == "PENDING" for row in metadata.runs_by_key.values())

    # Deterministic: the two returned units are exactly file-0 and file-1
    # (sorted by object_uri), never an arbitrary two of the five.
    expected_first_two_keys = {"assignments/customers/1.json", "assignments/customers/2.json"}
    returned_keys = {unit.assignment_uri.removeprefix("s3://metadata/") for unit in units}
    assert returned_keys == expected_first_two_keys


def test_discover_files_never_reads_wall_clock_time_and_leaves_business_date_null() -> None:
    """`meta.files.business_date` has no column in this fake, so this test asserts the contract
    the real schema enforces: the fake's `create_file` double never receives (and this repo's
    real `MetadataRepository.create_file` Protocol never accepts) a `business_date` argument at
    all -- there is no code path in `discover_files` that could populate it.
    """
    objects = _FakeObjectStore()
    objects.put("raw", "customers/a.csv", b"a content")
    metadata = _FakeMetadataRepository()

    discover_files(
        metadata=metadata,
        objects=objects,
        dataset_id=_DATASET_ID,
        dataset_name=_DATASET_NAME,
        config=_skip_config(),
        config_version_id=_CONFIG_VERSION_ID,
        config_hash=_CONFIG_HASH,
        processor_image=_PROCESSOR_IMAGE,
        processor_version=_PROCESSOR_VERSION,
        schema=_fake_schema(),
    )

    # If discover_files ever passed business_date=..., this fake's create_file
    # signature (which mirrors the real Protocol exactly) would raise
    # TypeError for the unexpected keyword -- it did not raise, above.


def test_discover_files_hashes_raw_bytes_matching_a_direct_hashlib_computation() -> None:
    """Proves the recorded content hash is the object's real sha256, not a re-encoding artifact."""
    payload = b'line one\r\nline two,with,a,"quoted,comma"\n'
    objects = _FakeObjectStore()
    objects.put("raw", "customers/hash-proof.csv", payload)
    metadata = _FakeMetadataRepository()

    discover_files(
        metadata=metadata,
        objects=objects,
        dataset_id=_DATASET_ID,
        dataset_name=_DATASET_NAME,
        config=_skip_config(),
        config_version_id=_CONFIG_VERSION_ID,
        config_hash=_CONFIG_HASH,
        processor_image=_PROCESSOR_IMAGE,
        processor_version=_PROCESSOR_VERSION,
        schema=_fake_schema(),
    )

    recorded = metadata.files_by_uri["s3://raw/customers/hash-proof.csv"]
    assert recorded.content_sha256 == hashlib.sha256(payload).digest()


def test_idempotency_key_pitfall_5_schema_version_term_changes_the_key() -> None:
    """Pitfall 5: the schema-version term is APPENDED, never reordering the first four.

    Reproduces the pre-06-16 4-term formula inline (not by importing dead
    code -- the old formula no longer exists anywhere in ``discovery.py``)
    and compares it against the new 5-term formula for the IDENTICAL
    ``(dataset_name, content_sha256_hex, config_hash, processor_image)``
    quadruple, with a non-empty ``schema_version`` term. The two keys MUST
    differ -- this is expected and safe, not a correctness regression: a
    file reprocessed after this formula change producing a "new-looking"
    ``meta.ingestion_runs`` row is an accepted, documented consequence
    (T-06-21 in this plan's threat model). Actual data-duplication
    protection comes from LOAD-03's content-hash check
    (``find_file_by_content_hash``) and LOAD-09's ``ON CONFLICT`` merge at
    publish time -- NEITHER depends on ``idempotency_key`` staying
    byte-identical across this change.
    """
    dataset_name = _DATASET_NAME
    content_sha256_hex = "deadbeef" * 8
    config_hash = _CONFIG_HASH
    processor_image = _PROCESSOR_IMAGE
    schema_version = "3"

    old_formula_key = hashlib.sha256(
        f"{dataset_name}|{content_sha256_hex}|{config_hash}|{processor_image}".encode(),
    ).hexdigest()
    new_formula_key = hashlib.sha256(
        f"{dataset_name}|{content_sha256_hex}|{config_hash}|{processor_image}|"
        f"{schema_version}".encode(),
    ).hexdigest()

    assert old_formula_key != new_formula_key


# --- Task 3: multi-part delivery grouping (06-08-PLAN.md) -------------------


_MULTIPART_PATTERN = r"(?P<group>.+)/part-(?P<index>\d+)"


def test_group_multipart_units_groups_the_two_part_corpus_fixture_shape() -> None:
    """`62_multipart_split`'s generated part-key shape (see ``tools/corpus/generators.py::

    output_names``) groups into one ``MultipartGroup`` with both keys in ascending order.
    """
    listed = [
        ObjectSummary(
            key="62_multipart_split/part-00000",
            etag="e0",
            size_bytes=252,
            last_modified=_LAST_MODIFIED,
        ),
        ObjectSummary(
            key="62_multipart_split/part-00001",
            etag="e1",
            size_bytes=248,
            last_modified=_LAST_MODIFIED,
        ),
    ]

    groups = group_multipart_units(listed, pattern=_MULTIPART_PATTERN)

    assert len(groups) == 1
    assert groups[0].group_key == "62_multipart_split"
    assert groups[0].ordered_object_uris == (
        "62_multipart_split/part-00000",
        "62_multipart_split/part-00001",
    )


def test_group_multipart_units_ignores_objects_that_do_not_match_the_pattern() -> None:
    listed = [
        ObjectSummary(
            key="62_multipart_split/part-00000",
            etag="e0",
            size_bytes=252,
            last_modified=_LAST_MODIFIED,
        ),
        ObjectSummary(
            key="customers/unrelated.csv",
            etag="e1",
            size_bytes=10,
            last_modified=_LAST_MODIFIED,
        ),
    ]

    groups = group_multipart_units(listed, pattern=_MULTIPART_PATTERN)

    assert len(groups) == 1
    assert groups[0].group_key == "62_multipart_split"


def test_group_multipart_units_raises_when_a_three_part_group_is_missing_its_middle_part() -> None:
    listed = [
        ObjectSummary(
            key="dataset_x/part-00000", etag="e0", size_bytes=1, last_modified=_LAST_MODIFIED
        ),
        ObjectSummary(
            key="dataset_x/part-00002", etag="e2", size_bytes=1, last_modified=_LAST_MODIFIED
        ),
    ]

    with pytest.raises(FileInspectionError) as exc_info:
        group_multipart_units(listed, pattern=_MULTIPART_PATTERN)

    assert exc_info.value.context["diagnostic_code"] == "multipart-group-incomplete"
    assert exc_info.value.context["group_key"] == "dataset_x"
    assert exc_info.value.context["indices"] == [0, 2]


def test_open_multipart_stream_reassembles_two_real_parts_into_one_twenty_row_dataset(
    tmp_path: Path,
) -> None:
    """``62_multipart_split``'s two real generated parts become one 20-row logical file,

    the same way ``csv.reader`` already streams a single object -- header consumed
    once (from part 0 only), and part 1's first row (a genuine data row, not a
    header) is never dropped.
    """
    manifest = load_manifest(MANIFEST)
    generate_corpus(manifest, tmp_path, fast=True)
    part_dir = tmp_path / "62_multipart_split"
    stream0 = (part_dir / "part-00000").open(encoding="utf-8", newline="")
    stream1 = (part_dir / "part-00001").open(encoding="utf-8", newline="")
    try:
        combined = open_multipart_stream([stream0, stream1])
        rows = list(csv.reader(combined))
    finally:
        stream0.close()
        stream1.close()

    header, *data_rows = rows
    assert header == ["id", "name", "amount"]
    assert len(data_rows) == 20
    # The second part's first row (000011) is present as DATA -- the exact
    # failure this fixture exists to catch (corpus.yaml's own comment: "a
    # reader that treats each object as a file ... silently drops a
    # record").
    assert data_rows[10][0] == "000011"
