"""ObjectStore — the corrected boto3 StreamingBody-to-text-stream bridge.

``open_text_stream`` wraps a real MinIO/S3 ``GetObject`` response body
directly in ``io.BufferedReader``/``io.TextIOWrapper``. No hand-written
adapter subclassing Python's raw binary-I/O base class exists anywhere in
this module: against the pinned boto3 1.43.68, ``botocore.response.
StreamingBody`` already implements ``readable()``/``readinto()`` directly —
verified this session by source inspection of the installed botocore plus
an executable round-trip test at chunk sizes 1, 2 and 3 (03-RESEARCH.md
finding 1) — so ``io.BufferedReader`` accepts it as-is. Reaching into
``StreamingBody``'s private internal-stream attribute is forbidden: doing
so is both unnecessary against this boto3 version and liable to break
without notice.
"""

from __future__ import annotations

import io
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from dataplat.errors import StorageError

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import datetime

    from mypy_boto3_s3 import S3Client


def open_text_stream(
    response_body: object,
    *,
    encoding: str,
    newline: str = "",
    errors: str = "strict",
) -> io.TextIOWrapper:
    r"""Wrap an S3/MinIO ``GetObject`` response body as a text stream.

    No hand-written raw-binary-I/O adapter subclass is used or needed here
    (see module docstring). Do not reach into ``response_body``'s private
    internal-stream attribute.

    Args:
        response_body: The ``Body`` value from a boto3 ``get_object()``
            response (a ``botocore.response.StreamingBody``), kept untyped
            here so this function carries no hard boto3 import-time
            dependency in its signature.
        encoding: Text encoding to decode the stream as.
        newline: Passed through to ``io.TextIOWrapper`` unchanged. Defaults
            to ``""`` (universal-newline translation disabled) so an
            embedded ``\r\n`` inside a quoted CSV field survives the round
            trip byte-for-byte.
        errors: Decoding error-handling mode passed through to
            ``io.TextIOWrapper``. Defaults to ``"strict"`` — silently
            replacing undecodable bytes would violate the platform's
            never-silently-discard rule (README §51).

    Returns:
        A text stream wrapping ``response_body``.
    """
    buffered = io.BufferedReader(response_body)  # type: ignore[type-var]
    return io.TextIOWrapper(buffered, encoding=encoding, newline=newline, errors=errors)


@dataclass(frozen=True, slots=True)
class ObjectSummary:
    """One object's identity/size, as returned by ``ObjectStore.list_objects``.

    Deliberately minimal (STACK.md's boto3 guidance): ``key`` is what
    ``dataplat.discovery`` needs to construct an ``object_uri``, and
    ``size_bytes`` is what it needs for ``meta.files.size_bytes``. ``etag``
    is carried for observability only -- it must never be trusted as
    ``content_sha256``: MinIO's ETag is an MD5 of implementation-dependent
    multipart chunking, not this platform's content hash. Content hashing is
    always done separately, via ``get_object()`` plus a streaming sha256
    over the actual bytes.

    Attributes:
        key: The object's key within its bucket.
        etag: The object's S3 ETag, as reported by ``ListObjectsV2`` --
            observability only, never a content hash (see above).
        size_bytes: The object's size, in bytes.
        last_modified: The object's last-modified timestamp, as reported by
            ``ListObjectsV2``.
    """

    key: str
    etag: str
    size_bytes: int
    last_modified: datetime


class ObjectStore(Protocol):
    """The object-store read/list/write surface ``dataplat`` code depends on.

    ``get_object`` proves the StreamingBody-to-text-stream bridge
    (03-RESEARCH.md finding 1, Phase 3). ``list_objects``/``put_object``
    (Phase 4) are the seam ``dataplat.discovery`` lists newly arrived files
    through, and a future write path uses -- both cross into the same boto3
    client ``get_object`` already constructs, never a second one.
    """

    def get_object(self, bucket: str, key: str) -> io.TextIOWrapper:
        """Return a text stream over one object's bytes.

        Args:
            bucket: The bucket the object lives in.
            key: The object's key within ``bucket``.

        Returns:
            A UTF-8 text stream over the object's bytes, opened with
            ``newline=""`` so embedded line endings inside quoted fields
            survive unchanged.
        """
        ...

    def list_objects(self, bucket: str, prefix: str) -> Iterator[ObjectSummary]:
        """List every object under ``bucket``/``prefix``, paginated transparently.

        Args:
            bucket: The bucket to list within.
            prefix: The key prefix to list under, e.g. ``"customers/"``.

        Returns:
            An iterator of ``ObjectSummary``, one per object found under
            ``prefix``, across every page -- never truncated at 1000 keys.
            Empty (not an exception) when ``prefix`` matches no objects.
        """
        ...

    def put_object(self, bucket: str, key: str, body: bytes) -> None:
        """Write ``body`` to ``bucket``/``key``, overwriting any existing object there.

        Args:
            bucket: The bucket to write into.
            key: The object key to write to.
            body: The object's full contents.
        """
        ...


class S3ObjectStore(ObjectStore):
    """The real ``ObjectStore``, backed by a boto3 S3 client against MinIO (or AWS S3).

    Encoding is fixed at UTF-8 for this phase's minimal scope — CONTEXT.md
    D-01 hardcodes UTF-8; encoding detection is Phase 6's.
    """

    def __init__(self, *, endpoint_url: str, access_key: str, secret_key: str) -> None:
        """Construct the boto3 S3 client this store wraps.

        Args:
            endpoint_url: The S3-compatible endpoint to talk to, e.g.
                ``http://minio.localtest.me`` or a testcontainers MinIO URL.
            access_key: The access key credential.
            secret_key: The secret key credential.
        """
        self._client: S3Client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

    def get_object(self, bucket: str, key: str) -> io.TextIOWrapper:
        """Fetch one object and wrap its body as a UTF-8 text stream.

        Args:
            bucket: The bucket the object lives in.
            key: The object's key within ``bucket``.

        Returns:
            A UTF-8 text stream over the object's bytes.

        Raises:
            StorageError: ``bucket``/``key`` do not resolve to an existing
                object, any other ``botocore.exceptions.ClientError`` (an
                S3-service-level error) occurs, or a
                ``botocore.exceptions.BotoCoreError`` (a connectivity
                failure -- endpoint unreachable, DNS failure, connect/read
                timeout -- raised when MinIO/S3 itself cannot be reached at
                all) occurs. ``ClientError`` and ``BotoCoreError`` are
                disjoint exception hierarchies (WR-01: neither is a subclass
                of the other), so both are caught explicitly. The raw
                boto3/botocore exception type never escapes this method.
        """
        try:
            response = self._client.get_object(Bucket=bucket, Key=key)
        except (ClientError, BotoCoreError) as exc:
            msg = "failed to get object from object storage"
            raise StorageError(msg, context={"bucket": bucket, "key": key}) from exc
        return open_text_stream(response["Body"], encoding="utf-8")

    def list_objects(self, bucket: str, prefix: str) -> Iterator[ObjectSummary]:
        """Fetch every object under ``bucket``/``prefix``, across every ``ListObjectsV2`` page.

        Collects every page eagerly, inside the same try/except shape
        ``get_object`` uses above (WR-01) -- so a ``ClientError``/
        ``BotoCoreError`` raises synchronously from this call, not lazily
        from the returned iterator's first ``next()``.

        Args:
            bucket: The bucket to list within.
            prefix: The key prefix to list under.

        Returns:
            An iterator of ``ObjectSummary``, one per object found.

        Raises:
            StorageError: Any ``botocore.exceptions.ClientError`` or
                ``botocore.exceptions.BotoCoreError`` occurs. Same
                disjoint-hierarchy handling as ``get_object`` (WR-01).
        """
        try:
            paginator = self._client.get_paginator("list_objects_v2")
            summaries = [
                ObjectSummary(
                    key=entry["Key"],
                    etag=entry["ETag"],
                    size_bytes=entry["Size"],
                    last_modified=entry["LastModified"],
                )
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
                for entry in page.get("Contents", [])
            ]
        except (ClientError, BotoCoreError) as exc:
            msg = "failed to list objects from object storage"
            raise StorageError(msg, context={"bucket": bucket, "prefix": prefix}) from exc
        return iter(summaries)

    def put_object(self, bucket: str, key: str, body: bytes) -> None:
        """Write ``body`` to ``bucket``/``key``, overwriting any existing object there.

        Args:
            bucket: The bucket to write into.
            key: The object key to write to.
            body: The object's full contents.

        Raises:
            StorageError: Any ``botocore.exceptions.ClientError`` or
                ``botocore.exceptions.BotoCoreError`` occurs. Same
                disjoint-hierarchy handling as ``get_object`` (WR-01).
        """
        try:
            self._client.put_object(Bucket=bucket, Key=key, Body=body)
        except (ClientError, BotoCoreError) as exc:
            msg = "failed to put object to object storage"
            raise StorageError(msg, context={"bucket": bucket, "key": key}) from exc
