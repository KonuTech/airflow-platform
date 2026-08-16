"""tests/e2e/vault/test_grafana_secrets.py -- OBS-01/OBS-09 grafana-secrets live proof.

Grafana (unlike Airflow's native `VaultBackend` or `hvac`-in-pod for ETL --
Phase 5's only two established Vault-consumer tiers) has no Vault client at
all, so its two credentials (the `grafana_reader` PostgreSQL password and the
operator-supplied alert-webhook URL) must land in a Kubernetes Secret instead
(07-RESEARCH.md Pattern 5). This module proves, against the live,
already-bootstrapped cluster, all three behaviors `scripts/vault-bootstrap.py`'s
`_ensure_grafana_secrets()` must have:

  1. Vault has neither `grafana/analytics-db` nor `grafana/alert-webhook` yet,
     and `.secrets/grafana-webhook-url` exists: a fresh `grafana_reader`
     password is rotated live against the analytical cluster, both Vault KV
     paths are populated, and the `grafana-alert-webhook` Kubernetes Secret
     is materialized in namespace `monitoring` with exactly the two expected
     keys -- neither value ever appears in the subprocess's captured output.
  2. `grafana/alert-webhook` does not exist yet AND
     `.secrets/grafana-webhook-url` does not exist either: `bootstrap()`
     fails closed with a clear, named `RuntimeError` citing the exact file
     path to create -- it never invents or guesses a webhook destination.
  3. Re-running bootstrap against an already-populated Vault is a genuine
     no-op for both Grafana KV paths (their `metadata.version` integers are
     unchanged) while still re-applying (idempotent no-op PATCH) the
     Kubernetes Secret -- matching `test_dev_secrets_reproducible.py`'s own
     "version didn't move" idempotency-proof style, translated to this
     plan's own two new KV paths.

**Separate file, not an extension of `test_dev_secrets_reproducible.py`:**
that sibling module's whole docstring and fixture story is scoped to SEC-13
(dev-secrets reproducibility). This is a different requirement pair
(OBS-01/OBS-09) and a different Vault-consumer shape (a materialized K8s
Secret, not a Vault-native `VaultBackend`/`hvac` read) -- a new file matches
this directory's own one-concern-per-file convention (07-06-PLAN.md Task 2's
own explicit fallback), confirmed after reading the sibling file's fixture
setup first.

**On the placeholder webhook URL these tests use:** no real webhook target
was supplied for this local dev environment (`.secrets/grafana-webhook-url`
does not exist on this machine -- the operator explicitly deferred providing
one). These tests therefore use a single, obviously-non-functional
placeholder value (the RFC 2606 reserved `.invalid` TLD, which is guaranteed
to never resolve) to exercise the write path structurally, and never invent
a real-looking third-party URL. **Because `_ensure_grafana_secrets()`
deliberately never rotates an already-present Vault value (the same
never-rotate-once-set discipline `_ensure_etl_secrets` already established
for `etl_app`), this placeholder is left live in Vault's
`grafana/alert-webhook` path after this module runs** -- matching every
other secret this script bootstraps (permanent, live state, not
test-cleaned-up), and matching this plan's own `<verify>` section, which
expects `kubectl -n monitoring get secret grafana-alert-webhook` to show two
populated keys when a human runs it after this plan completes. Actual
webhook *deliverability* (a real HTTP POST reaching a real endpoint) is
explicitly out of this module's scope -- it belongs to plan 07-08's own E2E
alert-delivery test. See `07-06-SUMMARY.md` for the exact operator follow-up
needed (clearing the placeholder from Vault before a real URL will ever be
picked up).
"""

from __future__ import annotations

import base64
import contextlib
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import hvac.exceptions
import pytest

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

pytestmark = pytest.mark.cluster

REPO_ROOT = Path(__file__).resolve().parents[3]
_BOOTSTRAP_SCRIPT = REPO_ROOT / "scripts" / "vault-bootstrap.py"
_WEBHOOK_FILE = REPO_ROOT / ".secrets" / "grafana-webhook-url"

# RFC 2606 reserved TLD -- guaranteed to never resolve to a real host. Never
# a real-looking third-party endpoint (07-06-PLAN.md parallel_execution note).
_PLACEHOLDER_WEBHOOK_URL = "https://grafana-alert-webhook.invalid/07-06-test-placeholder"

_MONITORING_NAMESPACE = "monitoring"
_SECRET_NAME = "grafana-alert-webhook"  # noqa: S105 -- a K8s Secret's metadata.name, not a credential
_GRAFANA_MOUNT = "grafana"

_SUBPROCESS_TIMEOUT_SECONDS = 60


def _run_bootstrap() -> subprocess.CompletedProcess[str]:
    """Run `scripts/vault-bootstrap.py` end-to-end as a subprocess, matching every sibling test."""
    return subprocess.run(  # noqa: S603
        [sys.executable, str(_BOOTSTRAP_SCRIPT)],
        capture_output=True,
        text=True,
        check=False,
        timeout=_SUBPROCESS_TIMEOUT_SECONDS,
    )


def _delete_grafana_kv_path(client: hvac.Client, path: str) -> None:
    """Fully remove a `grafana/<path>` KV v2 secret (metadata + all versions), if present.

    Idempotent-safe to call even when the path (or the whole `grafana/`
    mount) does not exist yet -- Vault's metadata-delete endpoint returns
    success regardless, and this is wrapped defensively besides, since this
    helper's only job is to force a genuinely clean precondition for a test.
    """
    with contextlib.suppress(hvac.exceptions.VaultError):
        client.secrets.kv.v2.delete_metadata_and_all_versions(
            mount_point=_GRAFANA_MOUNT,
            path=path,
        )


def _grafana_kv_secret(client: hvac.Client, path: str) -> dict[str, Any] | None:
    """Read a `grafana/<path>` KV v2 secret's full response, or None if it does not exist."""
    try:
        response: dict[str, Any] = client.secrets.kv.v2.read_secret_version(
            mount_point=_GRAFANA_MOUNT,
            path=path,
        )
    except hvac.exceptions.InvalidPath:
        return None
    return response


@pytest.fixture
def _webhook_file_absent() -> Iterator[None]:
    """Guarantee `.secrets/grafana-webhook-url` is absent for a test, restoring any real file after.

    Same atomic-backup-and-restore discipline `test_dev_secrets_reproducible.py`'s
    own `test_unseal_fails_closed_when_init_file_is_missing` established for
    `.secrets/vault-init.json` -- this file lives in the same gitignored
    `.secrets/` directory and deserves the same care, even though (unlike
    the Vault init file) losing it has no destructive consequence: it is
    only ever a local, regeneratable, plain-text URL.
    """
    backup: bytes | None = None
    if _WEBHOOK_FILE.exists():
        backup = _WEBHOOK_FILE.read_bytes()
        _WEBHOOK_FILE.unlink()
    try:
        yield
    finally:
        if backup is not None:
            _WEBHOOK_FILE.parent.mkdir(parents=True, exist_ok=True)
            _WEBHOOK_FILE.write_bytes(backup)
        elif _WEBHOOK_FILE.exists():
            _WEBHOOK_FILE.unlink()


@pytest.fixture
def _webhook_file_with_placeholder(_webhook_file_absent: None) -> None:
    """Write the placeholder webhook URL to `.secrets/grafana-webhook-url` for a test's duration.

    Depends on `_webhook_file_absent` purely for setup/teardown ordering --
    that fixture's own `finally` block already restores or removes the file,
    so nothing further needs to happen after this fixture's body runs.
    """
    _WEBHOOK_FILE.parent.mkdir(parents=True, exist_ok=True)
    _WEBHOOK_FILE.write_text(_PLACEHOLDER_WEBHOOK_URL, encoding="utf-8")


@pytest.mark.usefixtures("_webhook_file_absent")
def test_ensure_grafana_secrets_raises_when_webhook_path_and_file_are_both_absent(
    vault_root_client: hvac.Client,
) -> None:
    """OBS-09: no Vault value and no local file -- fails closed, never invents a destination."""
    _delete_grafana_kv_path(vault_root_client, "alert-webhook")
    assert not _WEBHOOK_FILE.exists()
    assert _grafana_kv_secret(vault_root_client, "alert-webhook") is None

    proc = _run_bootstrap()

    assert proc.returncode != 0, (
        f"expected a non-zero exit when {_WEBHOOK_FILE} is missing and Vault has no "
        f"grafana/alert-webhook value yet; got exit 0. stdout={proc.stdout!r}"
    )
    assert str(_WEBHOOK_FILE) in proc.stderr, (
        f"expected the exact missing file path in stderr; got: {proc.stderr!r}"
    )
    assert "does not exist" in proc.stderr, (
        f"expected a clear, actionable reason in stderr; got: {proc.stderr!r}"
    )
    # The alert-webhook half must still be absent -- the function must not
    # have silently created it from nothing.
    assert _grafana_kv_secret(vault_root_client, "alert-webhook") is None


@pytest.mark.usefixtures("_webhook_file_with_placeholder")
def test_ensure_grafana_secrets_creates_password_webhook_and_k8s_secret(
    vault_root_client: hvac.Client,
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    kubectl_json: Callable[..., Any],
) -> None:
    """OBS-01/OBS-08: a genuinely fresh Vault state creates both secrets and the K8s Secret."""
    _delete_grafana_kv_path(vault_root_client, "analytics-db")
    _delete_grafana_kv_path(vault_root_client, "alert-webhook")
    kubectl(
        "-n",
        _MONITORING_NAMESPACE,
        "delete",
        "secret",
        _SECRET_NAME,
        "--ignore-not-found=true",
    )

    proc = _run_bootstrap()

    assert proc.returncode == 0, f"bootstrap failed (exit {proc.returncode}):\n{proc.stderr}"

    password_secret = _grafana_kv_secret(vault_root_client, "analytics-db")
    assert password_secret is not None, "grafana/analytics-db was not created"
    password = password_secret["data"]["data"]["password"]
    assert isinstance(password, str)
    assert len(password) > 0

    webhook_secret = _grafana_kv_secret(vault_root_client, "alert-webhook")
    assert webhook_secret is not None, "grafana/alert-webhook was not created"
    assert webhook_secret["data"]["data"]["url"] == _PLACEHOLDER_WEBHOOK_URL

    # Never printed, in either stream.
    assert password not in proc.stdout
    assert password not in proc.stderr
    assert _PLACEHOLDER_WEBHOOK_URL not in proc.stdout
    assert _PLACEHOLDER_WEBHOOK_URL not in proc.stderr

    secret_manifest = kubectl_json("-n", _MONITORING_NAMESPACE, "get", "secret", _SECRET_NAME)
    assert secret_manifest["type"] == "Opaque"
    data = secret_manifest["data"]
    assert set(data.keys()) == {"GRAFANA_DB_PASSWORD", "GRAFANA_ALERT_WEBHOOK_URL"}
    decoded_password = base64.b64decode(data["GRAFANA_DB_PASSWORD"]).decode("utf-8")
    decoded_url = base64.b64decode(data["GRAFANA_ALERT_WEBHOOK_URL"]).decode("utf-8")
    assert decoded_password == password
    assert decoded_url == _PLACEHOLDER_WEBHOOK_URL


@pytest.mark.usefixtures("_webhook_file_with_placeholder")
def test_ensure_grafana_secrets_is_idempotent_across_a_second_bootstrap_run(
    vault_root_client: hvac.Client,
    kubectl_json: Callable[..., Any],
) -> None:
    """OBS-01/OBS-08: a second bootstrap run rewrites neither Grafana KV path's version."""
    # Guarantee both paths exist first (idempotent-safe regardless of prior
    # state) so this test is self-contained and order-independent.
    setup_proc = _run_bootstrap()
    assert setup_proc.returncode == 0, (
        f"setup bootstrap run failed (exit {setup_proc.returncode}):\n{setup_proc.stderr}"
    )

    def _versions() -> dict[str, int]:
        versions: dict[str, int] = {}
        for path in ("analytics-db", "alert-webhook"):
            secret = _grafana_kv_secret(vault_root_client, path)
            assert secret is not None, f"grafana/{path} unexpectedly absent after setup run"
            versions[path] = secret["data"]["metadata"]["version"]
        return versions

    before = _versions()

    proc = _run_bootstrap()

    assert proc.returncode == 0, (
        f"second bootstrap run failed (exit {proc.returncode}):\n{proc.stderr}"
    )
    after = _versions()
    assert after == before, (
        f"re-running bootstrap rewrote a grafana/* KV secret's version: "
        f"before={before!r} after={after!r}"
    )

    secret_manifest = kubectl_json("-n", _MONITORING_NAMESPACE, "get", "secret", _SECRET_NAME)
    assert set(secret_manifest["data"].keys()) == {
        "GRAFANA_DB_PASSWORD",
        "GRAFANA_ALERT_WEBHOOK_URL",
    }
