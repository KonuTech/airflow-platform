"""`S3ObjectStore` proven against a real testcontainers MinIO, not a mock.

Proves 03-RESEARCH.md finding 1's corrected `StreamingBody` bridge: an
embedded-newline CSV field survives `get_object()` unchanged (`newline=""`
was honored, not silently overridden by universal-newline translation), and
a boto3 `ClientError` never escapes `get_object()` as itself — it always
surfaces as `dataplat.errors.StorageError`.
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
