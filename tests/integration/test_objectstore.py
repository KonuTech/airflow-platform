"""`S3ObjectStore` proven against a real testcontainers MinIO, not a mock.

Proves 03-RESEARCH.md finding 1's corrected `StreamingBody` bridge: an
embedded-newline CSV field survives `get_object()` unchanged (`newline=""`
was honored, not silently overridden by universal-newline translation), and
a boto3 `ClientError` never escapes `get_object()` as itself — it always
surfaces as `dataplat.errors.StorageError`.

`test_list_objects_*`/`test_put_object_*` (04-01 Task 3) prove the same
seam's list/write half: `list_objects` pages transparently past the 1000-key
`ListObjectsV2` truncation boundary is not exercised here (that would be a
slow test for no additional proof) but empty-prefix and multi-object
behavior both are; `put_object` followed by `get_object` round-trips exact
bytes through the same boto3 client `get_object` already uses.
"""

from __future__ import annotations

from typing import Any

import pytest

from dataplat.errors import StorageError
from dataplat.storage.objectstore import S3ObjectStore

_BUCKET = "objectstore-test"
_KEY = "customers/embedded-newline.csv"
# One field, quoted, containing an embedded \r\n -- proves newline="" was
# honored: a universal-newline translation would have silently rewritten it.
_CSV_BYTES = b'header_a,header_b,header_c\n"line1\r\nline2",b,c\n'


@pytest.fixture
def object_store(minio_config: dict[str, str]) -> S3ObjectStore:
    """A real `S3ObjectStore`, built from the same credentials `s3_client` uses."""
    return S3ObjectStore(
        endpoint_url=f"http://{minio_config['endpoint']}",
        access_key=minio_config["access_key"],
        secret_key=minio_config["secret_key"],
    )


@pytest.fixture
def scratch_bucket(s3_client: Any) -> str:
    """Ensure this module's scratch bucket exists; return its name.

    Idempotent so it is safe to call once per test even though the
    session-scoped MinIO container is shared across the whole
    `tests/integration/` collection.
    """
    existing = {bucket["Name"] for bucket in s3_client.list_buckets().get("Buckets", [])}
    if _BUCKET not in existing:
        s3_client.create_bucket(Bucket=_BUCKET)
    return _BUCKET


def test_get_object_round_trips_embedded_newline_unchanged(
    object_store: S3ObjectStore,
    s3_client: Any,
    scratch_bucket: str,
) -> None:
    s3_client.put_object(Bucket=scratch_bucket, Key=_KEY, Body=_CSV_BYTES)

    stream = object_store.get_object(scratch_bucket, _KEY)
    text = stream.read()

    assert '"line1\r\nline2"' in text


def test_get_object_missing_key_raises_storage_error(
    object_store: S3ObjectStore,
    scratch_bucket: str,
) -> None:
    with pytest.raises(StorageError):
        object_store.get_object(scratch_bucket, "does/not/exist.csv")


def test_list_objects_yields_every_object_under_the_prefix(
    object_store: S3ObjectStore,
    s3_client: Any,
    scratch_bucket: str,
) -> None:
    prefix = "list-objects-test/"
    expected_keys = {f"{prefix}a.csv", f"{prefix}b.csv", f"{prefix}c.csv"}
    for key in expected_keys:
        s3_client.put_object(Bucket=scratch_bucket, Key=key, Body=b"x")
    s3_client.put_object(Bucket=scratch_bucket, Key="outside-the-prefix.csv", Body=b"x")

    summaries = list(object_store.list_objects(scratch_bucket, prefix))

    assert {summary.key for summary in summaries} == expected_keys
    assert len(summaries) == 3
    assert all(summary.size_bytes == 1 for summary in summaries)


def test_list_objects_yields_nothing_for_an_absent_prefix(
    object_store: S3ObjectStore,
    scratch_bucket: str,
) -> None:
    summaries = list(object_store.list_objects(scratch_bucket, "no-such-prefix-anywhere/"))

    assert summaries == []


def test_put_object_then_get_object_round_trips_exact_bytes(
    object_store: S3ObjectStore,
    scratch_bucket: str,
) -> None:
    key = "put-object-test/roundtrip.csv"

    object_store.put_object(scratch_bucket, key, _CSV_BYTES)
    stream = object_store.get_object(scratch_bucket, key)

    assert stream.read() == _CSV_BYTES.decode("utf-8")
