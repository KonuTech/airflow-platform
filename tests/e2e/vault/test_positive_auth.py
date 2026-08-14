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

This test asserts the values Vault returns are well-formed and non-empty --
it deliberately does NOT compare them against the `csv-processor-db`/
`csv-processor-s3` Kubernetes Secrets `scripts/vault-bootstrap.py` originally
sourced them from. This same plan's own Task 3 deletes both Secrets from the
live cluster once this test passes (D-01's prove-then-remove sequencing), so
a comparison against them would make this test permanently unable to pass on
any run after that deletion -- including `make vault-verify`, the standing
"after every plan wave" gate (05-VALIDATION.md) every later wave in this
phase reruns. The value-equality proof (Vault's copy byte-identical to the
Secret it was migrated from) was performed once, live, immediately before
that deletion; it is not a standing invariant this file can keep
re-checking once the source of truth it would compare against no longer
exists.
"""

from __future__ import annotations

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


def test_csv_processor_reads_its_own_two_vault_paths(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    vault_addr: str,
) -> None:
    """SEC-06/SEC-07: csv-processor authenticates via its own role and reads
    exactly its own two KV paths, each returning a well-formed, non-empty
    value -- proving the Vault-side authorization boundary
    `resolve_secret("vault://...")` depends on is real and reachable.
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
    assert dsn.startswith("postgresql://"), (
        f"etl/analytics-db#dsn is not a postgresql:// DSN: {dsn!r}"
    )

    minio_secret = client.secrets.kv.v2.read_secret_version(mount_point="etl", path="minio")
    access_key = minio_secret["data"]["data"]["access_key"]
    secret_key = minio_secret["data"]["data"]["secret_key"]
    assert access_key == "etl-app"
    assert isinstance(secret_key, str)
    assert secret_key
