"""tests/e2e/vault/test_positive_auth.py -- SEC-06/SEC-07 live proof.

The `csv-processor` ServiceAccount (namespace `etl`) authenticates to Vault
via Kubernetes auth against its own role, then reads exactly its own two KV
paths -- proving the SAME Vault-side authorization boundary
`dataplat.secrets.resolver._vault_client()`/`resolve_secret("vault://...")`
exercises inside a real KPO pod. A full live pod run is deliberately NOT
required here (05-02-PLAN.md Task 3's own acceptance note): the
KV-read-through-the-role assertion below exercises the identical Vault-side
authorization boundary SEC-06/SEC-07 concern; pod resolution of the SAME
``vault://`` references is proven transitively by 05-03-PLAN.md Task 2's own
live DAG trigger.

Reads back the Kubernetes Secrets `scripts/vault-bootstrap.py` itself
sourced the KV values FROM (`csv-processor-db`/`csv-processor-s3`), so this
test proves genuine value equality, not merely "some non-empty string came
back".
"""

from __future__ import annotations

import base64
from typing import TYPE_CHECKING

import hvac
import pytest

if TYPE_CHECKING:
    import subprocess
    from collections.abc import Callable

pytestmark = pytest.mark.cluster


def _kubectl_create_token(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    *,
    service_account: str,
    namespace: str,
) -> str:
    """Obtain a fresh projected token for `service_account` in `namespace`."""
    proc = kubectl("create", "token", service_account, "-n", namespace)
    assert proc.returncode == 0, (
        f"kubectl create token {service_account} -n {namespace} failed "
        f"(exit {proc.returncode}):\n{proc.stderr}"
    )
    return proc.stdout.strip()


def _kubectl_get_secret_field(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    *,
    namespace: str,
    name: str,
    key: str,
) -> str:
    """Read one base64-decoded field from a live Kubernetes Secret.

    Same mechanism `scripts/vault-bootstrap.py`'s own
    `_kubectl_get_secret_field` uses, reimplemented here so this test's
    equality proof does not depend on importing a `scripts/` module.
    """
    proc = kubectl("get", "secret", "-n", namespace, name, "-o", f"jsonpath={{.data.{key}}}")
    assert proc.returncode == 0, (
        f"kubectl get secret -n {namespace} {name} failed (exit {proc.returncode}):\n{proc.stderr}"
    )
    return base64.b64decode(proc.stdout).decode("utf-8")


def test_csv_processor_reads_its_own_two_vault_paths(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    vault_addr: str,
) -> None:
    """SEC-06/SEC-07: csv-processor authenticates via its own role, reads
    exactly its own two KV paths, and the values match what
    `scripts/vault-bootstrap.py` sourced them from.
    """
    csv_processor_jwt = _kubectl_create_token(
        kubectl,
        service_account="csv-processor",
        namespace="etl",
    )

    client = hvac.Client(url=vault_addr)
    client.auth.kubernetes.login(role="csv-processor", jwt=csv_processor_jwt)

    analytics_secret = client.secrets.kv.v2.read_secret_version(
        mount_point="etl",
        path="analytics-db",
    )
    dsn = analytics_secret["data"]["data"]["dsn"]
    assert isinstance(dsn, str)
    assert dsn
    expected_dsn = _kubectl_get_secret_field(
        kubectl,
        namespace="etl",
        name="csv-processor-db",
        key="dsn",
    )
    assert dsn == expected_dsn, "etl/analytics-db#dsn does not match csv-processor-db's own dsn"

    minio_secret = client.secrets.kv.v2.read_secret_version(mount_point="etl", path="minio")
    access_key = minio_secret["data"]["data"]["access_key"]
    secret_key = minio_secret["data"]["data"]["secret_key"]
    assert access_key == "etl-app"
    assert isinstance(secret_key, str)
    assert secret_key
    expected_secret_key = _kubectl_get_secret_field(
        kubectl,
        namespace="etl",
        name="csv-processor-s3",
        key="secret_key",
    )
    assert secret_key == expected_secret_key, (
        "etl/minio#secret_key does not match csv-processor-s3's own secret_key"
    )
