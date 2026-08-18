"""Unit tests for ``discover_files``' opt-in ``_BATCH_COMPLETE`` marker gate (LOAD-11/D-19).

08-06-PLAN.md Task 1's own three ``<behavior>`` bullets, proven here:

1. A dataset configured with ``source.batch_complete_marker`` discovers
   nothing (``[]``) from a batch directory missing that marker object --
   AND no discovery bookkeeping of any kind happens for any object this
   call, proven via ``unittest.mock.Mock`` call-count assertions on
   ``MetadataRepository``/``ObjectStore``/``SchemaRepository``, not merely
   by asserting the returned list is empty.
2. The identical call against a listing that DOES include the marker
   discovers exactly as if ``batch_complete_marker`` were ``None`` -- and
   the marker object itself is never treated as a candidate data file.
3. ``batch_complete_marker=None`` (the ``customers``/``orders`` default) is
   unaffected -- ``tests/unit/test_discovery.py``'s own suite already
   proves this by continuing to pass unmodified; this file adds one direct
   regression test of the same claim, scoped to this new fixture shape.

Fixture-shape note (08-06-PLAN.md Task 1's own conditional guidance):
``tools/corpus/generators.py`` only has ``tabular``/``literal``/
``literal_unicode``/``wrapper``/``multipart`` generator kinds -- every one
of them writes CSV-shaped byte *content* for a single named file, with no
concept of a directory-listing corpus or a non-CSV sentinel/marker object.
There is nothing to extend there for this feature, so per the plan's own
documented fallback, this fixture is built directly here via lightweight
in-memory ``ObjectStore``/``MetadataRepository`` doubles -- the same shape
``tests/unit/test_discovery.py`` already established for
``discover_files``. ``tools/corpus/generators.py`` and
``tests/fixtures/slice-corpus.yaml`` are therefore untouched by this plan.
"""

from __future__ import annotations

import io
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast
from unittest.mock import Mock

from dataplat.config.model import (
    BatchingConfig,
    ColumnContract,
    DatasetConfig,
    DeduplicationConfig,
    LoadConfig,
    SourceConfig,
)
from dataplat.discovery import DiscoveredUnit, discover_files
from dataplat.metadata.repository import MetadataRepository
from dataplat.storage.objectstore import ObjectStore, ObjectSummary, open_text_stream

if TYPE_CHECKING:
    from collections.abc import Iterator

    from dataplat.schema.repository import SchemaRepository

_DATASET_ID = 1
_DATASET_NAME = "marker_dataset"
_CONFIG_VERSION_ID = 1
_CONFIG_HASH = "config-hash-fixture"
_PROCESSOR_IMAGE = "sha256:processor-image-fixture"
_PROCESSOR_VERSION = "0.1.0"
_LAST_MODIFIED = datetime(2026, 8, 17, tzinfo=UTC)
_MARKER_SUFFIX = "_BATCH_COMPLETE"
_BATCH_PATH = "batch/"
_MARKER_KEY = _BATCH_PATH + _MARKER_SUFFIX


@dataclass
class _FakeSchemaRepository:
    """Minimal schema-repository double: ``get_current()`` always reports "no schema yet".

    Mirrors ``tests/unit/test_discovery.py``'s own ``_FakeSchemaRepository``
    -- ``SchemaRepository`` is a concrete class, not a ``Protocol``, so this
    is duck-typed and ``cast()`` at its call site rather than subclassed or
    ``Mock(spec=...)``'d.
    """

    def get_current(self, dataset_id: int) -> None:
        del dataset_id


def _fake_schema() -> SchemaRepository:
    return cast("SchemaRepository", _FakeSchemaRepository())


def _marker_config(*, batch_complete_marker: str | None) -> DatasetConfig:
    """A minimal, constructible ``DatasetConfig`` with the marker gate under test.

    ``discover_files`` itself never reads ``DatasetConfig.columns`` -- see
    ``tests/unit/test_discovery.py``'s own ``_skip_config`` precedent -- the
    two columns here exist purely to satisfy ``DatasetConfig``'s own
    construction requirements.
    """
    return DatasetConfig(
        dataset=_DATASET_NAME,
        config_schema_version=1,
        source=SourceConfig(
            type="csv",
            bucket="raw",
            path=_BATCH_PATH,
            change_semantics="snapshot",
            duplicate_policy="skip",
            batch_complete_marker=batch_complete_marker,
        ),
        deduplication=DeduplicationConfig(
            strategy="business_key_latest",
            keys=["id"],
            order_by=["id"],
        ),
        load=LoadConfig(strategy="merge", target="normalized.marker_dataset"),
        batching=BatchingConfig(max_units_per_run=100),
        columns=[
            ColumnContract(
                name="id",
                type="string",
                nullable=False,
                required=True,
                business_key=True,
                description="Natural business key",
            ),
            ColumnContract(name="value", type="string", nullable=False, required=True),
        ],
    )


@dataclass
class _FakeObjectStore:
    """An in-memory ``ObjectStore`` double, matching ``tests/unit/test_discovery.py``'s shape."""

    objects: dict[tuple[str, str], bytes] = field(default_factory=dict)
    written: dict[tuple[str, str], bytes] = field(default_factory=dict)

    def put(self, bucket: str, key: str, body: bytes) -> None:
        """Test-only seeding helper -- NOT part of the ``ObjectStore`` Protocol."""
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
    # 08.1-07 (D-18): tracked so find_latest_succeeded_run_for_file below can
    # look a prior SUCCEEDED run up by file_id, mirroring the real
    # PostgresMetadataRepository's own SELECT.
    file_id: int | None = None
    replay_of_run_id: int | None = None


@dataclass
class _FakeMetadataRepository:
    """An in-memory ``MetadataRepository`` double.

    Matching ``tests/unit/test_discovery.py``'s own shape.
    """

    files_by_uri: dict[str, _FakeFileRow] = field(default_factory=dict)
    runs_by_key: dict[str, _FakeRunRow] = field(default_factory=dict)
    batches_by_key: dict[tuple[int, str], int] = field(default_factory=dict)
    batch_file_links: list[tuple[int, int, int]] = field(default_factory=list)
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
        self.batch_file_links.append((batch_id, file_id, sequence_no))

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
        replay_of_run_id: int | None = None,
    ) -> tuple[int, str]:
        del dataset_id, config_version_id, processor_version, processor_image_digest
        del batch_id
        existing = self.runs_by_key.get(idempotency_key)
        if existing is not None:
            return existing.run_id, existing.status
        run_id = self._next_run_id
        self._next_run_id += 1
        self.runs_by_key[idempotency_key] = _FakeRunRow(
            run_id=run_id,
            status="PENDING",
            file_id=file_id,
            replay_of_run_id=replay_of_run_id,
        )
        return run_id, "PENDING"

    def find_latest_succeeded_run_for_file(self, *, file_id: int) -> int | None:
        """08.1-07 (D-18): mirrors the real repository's `ORDER BY run_id DESC LIMIT 1`."""
        matches = [
            row.run_id
            for row in self.runs_by_key.values()
            if row.file_id == file_id and row.status == "SUCCEEDED"
        ]
        return max(matches) if matches else None


def test_discover_files_withholds_the_whole_batch_when_marker_object_is_absent() -> None:
    """Behavior bullet 1: real data files ARE present, but the marker is not -- the whole

    batch is withheld, and NO discovery bookkeeping happens for anything this call --
    proven by call-count assertions, not merely by the returned list being empty.
    """
    objects = Mock(spec=ObjectStore)
    objects.list_objects.return_value = iter(
        [
            ObjectSummary(
                key=_BATCH_PATH + "data1.csv",
                etag="e1",
                size_bytes=10,
                last_modified=_LAST_MODIFIED,
            ),
            ObjectSummary(
                key=_BATCH_PATH + "data2.csv",
                etag="e2",
                size_bytes=10,
                last_modified=_LAST_MODIFIED,
            ),
        ],
    )
    metadata = Mock(spec=MetadataRepository)
    schema = Mock()
    schema.get_current = Mock(return_value=None)

    units = discover_files(
        metadata=metadata,
        objects=objects,
        dataset_id=_DATASET_ID,
        dataset_name=_DATASET_NAME,
        config=_marker_config(batch_complete_marker=_MARKER_SUFFIX),
        config_version_id=_CONFIG_VERSION_ID,
        config_hash=_CONFIG_HASH,
        processor_image=_PROCESSOR_IMAGE,
        processor_version=_PROCESSOR_VERSION,
        schema=cast("SchemaRepository", schema),
    )

    assert units == []
    # No object was ever hashed (get_object never called) -- "before any
    # parsing occurs" (LOAD-11), not merely "the file count differs".
    objects.get_object.assert_not_called()
    objects.put_object.assert_not_called()
    # No meta.files/meta.batches/meta.ingestion_runs bookkeeping happened at
    # all for either real data file.
    metadata.create_file.assert_not_called()
    metadata.get_or_create_batch.assert_not_called()
    metadata.link_batch_file.assert_not_called()
    metadata.get_or_create_ingestion_run.assert_not_called()
    # The gate returns before the schema-version resolution step too --
    # this call never even reaches it.
    schema.get_current.assert_not_called()


def test_discover_files_discovers_normally_once_marker_object_is_present() -> None:
    """Behavior bullet 2: with the marker object present, discovery proceeds exactly as if

    ``batch_complete_marker`` were ``None`` -- same discovered object set, same file/batch/
    run bookkeeping -- and the marker object itself is never registered as a data file.
    """
    with_marker_objects = _FakeObjectStore()
    with_marker_objects.put("raw", _BATCH_PATH + "data1.csv", b"id,value\n1,a\n")
    with_marker_objects.put("raw", _BATCH_PATH + "data2.csv", b"id,value\n2,b\n")
    with_marker_objects.put("raw", _MARKER_KEY, b"")
    with_marker_metadata = _FakeMetadataRepository()

    with_marker_units = discover_files(
        metadata=with_marker_metadata,
        objects=with_marker_objects,
        dataset_id=_DATASET_ID,
        dataset_name=_DATASET_NAME,
        config=_marker_config(batch_complete_marker=_MARKER_SUFFIX),
        config_version_id=_CONFIG_VERSION_ID,
        config_hash=_CONFIG_HASH,
        processor_image=_PROCESSOR_IMAGE,
        processor_version=_PROCESSOR_VERSION,
        schema=_fake_schema(),
    )

    no_marker_objects = _FakeObjectStore()
    no_marker_objects.put("raw", _BATCH_PATH + "data1.csv", b"id,value\n1,a\n")
    no_marker_objects.put("raw", _BATCH_PATH + "data2.csv", b"id,value\n2,b\n")
    no_marker_metadata = _FakeMetadataRepository()

    no_marker_units = discover_files(
        metadata=no_marker_metadata,
        objects=no_marker_objects,
        dataset_id=_DATASET_ID,
        dataset_name=_DATASET_NAME,
        config=_marker_config(batch_complete_marker=None),
        config_version_id=_CONFIG_VERSION_ID,
        config_hash=_CONFIG_HASH,
        processor_image=_PROCESSOR_IMAGE,
        processor_version=_PROCESSOR_VERSION,
        schema=_fake_schema(),
    )

    # Same two data files discovered in both runs; the marker call is not
    # missing or gaining any real data-file unit relative to the marker=None
    # baseline.
    assert len(with_marker_units) == 2
    assert len(no_marker_units) == 2
    assert all(isinstance(unit, DiscoveredUnit) for unit in with_marker_units)

    with_marker_uris = set(with_marker_metadata.files_by_uri)
    no_marker_uris = set(no_marker_metadata.files_by_uri)
    assert (
        with_marker_uris
        == no_marker_uris
        == {
            "s3://raw/" + _BATCH_PATH + "data1.csv",
            "s3://raw/" + _BATCH_PATH + "data2.csv",
        }
    )
    # The marker object was stripped before the per-object loop -- it was
    # never hashed, registered, batched or assigned as its own file.
    assert "s3://raw/" + _MARKER_KEY not in with_marker_metadata.files_by_uri
    assert len(with_marker_objects.written) == 2  # two assignment docs, not three


def test_discover_files_with_no_batch_complete_marker_configured_is_unaffected() -> None:
    """Behavior bullet 3: ``batch_complete_marker=None`` (the ``customers``/``orders`` default)

    is completely unaffected by this gate -- direct regression proof scoped to this file's
    own fixture shape, alongside ``tests/unit/test_discovery.py``'s own unmodified suite.
    """
    objects = _FakeObjectStore()
    objects.put("raw", _BATCH_PATH + "data1.csv", b"id,value\n1,a\n")
    metadata = _FakeMetadataRepository()

    units = discover_files(
        metadata=metadata,
        objects=objects,
        dataset_id=_DATASET_ID,
        dataset_name=_DATASET_NAME,
        config=_marker_config(batch_complete_marker=None),
        config_version_id=_CONFIG_VERSION_ID,
        config_hash=_CONFIG_HASH,
        processor_image=_PROCESSOR_IMAGE,
        processor_version=_PROCESSOR_VERSION,
        schema=_fake_schema(),
    )

    assert len(units) == 1
    assert len(metadata.files_by_uri) == 1
