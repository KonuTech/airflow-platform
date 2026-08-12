"""tests/e2e/cluster/test_minio_buckets.py — INFRA-05 and §63 proved through boto3.

Honest limit: this proves the five buckets are reachable through an ordinary
S3 client (boto3 — never MinIO's own `mc` tooling, D-16) and that the
deny-delete statement on `raw` is enforced by the SERVER, for both the
application credential (denied) and the admin credential (permitted, the
positive control that distinguishes "denies everyone" from "denies the
application"). It does not prove the platform is byte-compatible with AWS S3
in every API corner — only the object/bucket operations this platform's
pipeline actually uses.

D-07: no address is hardcoded here — `s3_client` (tests/e2e/cluster/conftest.py)
resolves the endpoint from `S3_ENDPOINT_URL`, defaulting to the ingress host.
D-08/§63: `raw`'s versioning-plus-deny shape is the server-side mechanism
under test; it is not re-derived here, only observed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from collections.abc import Callable

pytestmark = pytest.mark.cluster

ALL_BUCKETS = frozenset({"raw", "validated", "processed", "quarantine", "metadata"})
# The `etl-app` policy's Allow statements name only these two (plan 02-04
# Task 2, helm/values/*/minio.yaml) — the pipeline's working buckets. The
# other three are reached with the admin credential in this suite, matching
# how later phases write to those layers.
APP_READABLE_BUCKETS = frozenset({"raw", "validated"})


def test_all_five_buckets_exist(s3_client: Callable[[str], Any]) -> None:
    """Every declared bucket exists, and no accidental sixth one does."""
    admin = s3_client("admin")
    app = s3_client("app")

    names = {b["Name"] for b in admin.list_buckets()["Buckets"]}
    assert names == ALL_BUCKETS, (
        f"expected exactly {sorted(ALL_BUCKETS)}, live cluster has {sorted(names)}"
    )

    for bucket in ALL_BUCKETS:
        client = app if bucket in APP_READABLE_BUCKETS else admin
        client.head_bucket(Bucket=bucket)  # raises ClientError on failure


def test_round_trip_through_s3_uri(s3_client: Callable[[str], Any]) -> None:
    """A byte payload written to s3://raw/<key> reads back identically."""
    app = s3_client("app")
    admin = s3_client("admin")

    bucket, key = "raw", "e2e/minio-buckets/round-trip.txt"
    payload = b"airflow-platform e2e round trip through s3://raw/e2e/minio-buckets/round-trip.txt"

    try:
        app.put_object(Bucket=bucket, Key=key, Body=payload)
        got = app.get_object(Bucket=bucket, Key=key)["Body"].read()
        assert got == payload, f"round trip through s3://{bucket}/{key} corrupted the payload"
    finally:
        # The app credential cannot delete from `raw` (§63) — cleanup uses admin.
        admin.delete_object(Bucket=bucket, Key=key)


def test_raw_versioning_is_enabled(s3_client: Callable[[str], Any]) -> None:
    """`raw` is versioned (D-08); `validated` is not — the divergence is deliberate."""
    admin = s3_client("admin")

    raw_status = admin.get_bucket_versioning(Bucket="raw").get("Status")
    validated_status = admin.get_bucket_versioning(Bucket="validated").get("Status")

    assert raw_status == "Enabled", f"expected raw versioning Enabled, got {raw_status!r}"
    assert validated_status != "Enabled", (
        f"expected validated versioning NOT Enabled (raw's divergence would be meaningless "
        f"otherwise), got {validated_status!r}"
    )


def test_raw_delete_is_denied_for_app_credential(s3_client: Callable[[str], Any]) -> None:
    """The negative case that carries §63: the pipeline's own credential cannot delete from raw."""
    app = s3_client("app")
    admin = s3_client("admin")

    bucket, key = "raw", "e2e/minio-buckets/deny-delete.txt"
    payload = b"must survive an app-credential delete attempt"

    try:
        app.put_object(Bucket=bucket, Key=key, Body=payload)

        with pytest.raises(ClientError) as exc_info:
            app.delete_object(Bucket=bucket, Key=key)
        error_code = exc_info.value.response.get("Error", {}).get("Code")
        assert error_code == "AccessDenied", (
            f"expected AccessDenied from the app credential's delete attempt on "
            f"s3://{bucket}/{key}, got {error_code!r}: {exc_info.value}"
        )

        # An error code that left the object deleted anyway would pass a naive
        # assertion — prove it is still there, byte for byte.
        still_there = app.get_object(Bucket=bucket, Key=key)["Body"].read()
        assert still_there == payload, (
            f"s3://{bucket}/{key} was NOT deleted (as expected) but its content changed"
        )
    finally:
        admin.delete_object(Bucket=bucket, Key=key)


def test_raw_delete_is_permitted_for_admin_credential(s3_client: Callable[[str], Any]) -> None:
    """The positive control: a policy denying everyone is as wrong as one denying nobody."""
    admin = s3_client("admin")

    bucket, key = "raw", "e2e/minio-buckets/admin-delete.txt"
    admin.put_object(Bucket=bucket, Key=key, Body=b"admin retains delete on raw")

    admin.delete_object(Bucket=bucket, Key=key)

    with pytest.raises(ClientError) as exc_info:
        admin.get_object(Bucket=bucket, Key=key)
    error_code = exc_info.value.response.get("Error", {}).get("Code")
    assert error_code in {"NoSuchKey", "404"}, (
        f"expected s3://{bucket}/{key} to be gone after the admin credential's delete, "
        f"got {error_code!r}: {exc_info.value}"
    )
