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
from typing import TYPE_CHECKING, Protocol

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from dataplat.errors import StorageError

if TYPE_CHECKING:
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


class ObjectStore(Protocol):
    """The object-store read surface ``dataplat`` code depends on.

    One method today: this phase proves only the StreamingBody-to-text-
    stream bridge (03-RESEARCH.md finding 1). Write paths and additional
    operations belong to whichever later phase first needs them.
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
