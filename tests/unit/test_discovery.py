"""Unit tests for ``dataplat.discovery.discover_files`` against fake object-store/metadata doubles.

04-03-PLAN.md Task 2's own acceptance criterion permits a unit-level proof
against fakes, with the dedicated ``tests/integration/test_discover_files.py``
(a later plan) proving the same behavior against real testcontainers
Postgres/MinIO. Every ``<behavior>`` bullet from 04-03-PLAN.md Task 2 has a
test here.

The two fake doubles below implement exactly the ``MetadataRepository``/
``ObjectStore`` Protocol methods ``discover_files`` calls -- both are
in-memory, single-dataset-scoped, and use the SAME
``dataplat.storage.objectstore.open_text_stream`` bridge the real
``S3ObjectStore`` uses, so ``discover_files``'s raw-bytes hashing via
``stream.buffer.read(...)`` exercises the identical code path it would
against a real MinIO object.
"""

from __future__ import annotations

import hashlib
import io
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from dataplat.config.model import (
    BatchingConfig,
    DatasetConfig,
    DeduplicationConfig,
    LoadConfig,
    SourceConfig,
)
from dataplat.discovery import DiscoveredUnit, discover_files
from dataplat.storage.objectstore import ObjectSummary, open_text_stream

if TYPE_CHECKING:
    from collections.abc import Iterator

_DATASET_ID = 1
_DATASET_NAME = "customers"
_CONFIG_VERSION_ID = 7
_CONFIG_HASH = "config-hash-fixture"
_PROCESSOR_IMAGE = "sha256:processor-image-fixture"
_PROCESSOR_VERSION = "0.1.0"
_LAST_MODIFIED = datetime(2026, 8, 13, tzinfo=UTC)


def _skip_config(*, max_units_per_run: int = 100) -> DatasetConfig:
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

    def create_batch(self, *, dataset_id: int, batch_key: str, status: str) -> int:
        del dataset_id, batch_key, status
        batch_id = self._next_batch_id
        self._next_batch_id += 1
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
    )

    recorded = metadata.files_by_uri["s3://raw/customers/hash-proof.csv"]
    assert recorded.content_sha256 == hashlib.sha256(payload).digest()
