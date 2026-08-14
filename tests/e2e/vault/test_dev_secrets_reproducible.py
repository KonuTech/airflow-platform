"""tests/e2e/vault/test_dev_secrets_reproducible.py -- SEC-13 live proof.

SEC-13: "Development secrets are clearly marked, never committed, isolated
from production, and reproducible when rebuilding the local environment"
(REQUIREMENTS.md DoD 102). This module proves, against the live,
already-bootstrapped cluster:

  1. Re-running `scripts/vault-bootstrap.py` against an already-bootstrapped
     Vault is a safe no-op -- every auth method, secrets engine mount,
     audit device, tracked role definition, and tracked KV secret's VERSION
     number is IDENTICAL before and after, proving no value is silently
     rotated or rewritten by a re-run (matching `scripts/etl-secrets.sh`'s
     own "must not rotate mid-lifetime" discipline, carried into its Vault
     successor).
  2. Re-running `scripts/vault-unseal.py` against an already-unsealed Vault
     is also a no-op: it prints `"already unsealed"` and never writes
     `.secrets/vault-init.json` (its mtime is unchanged).
  3. `.secrets/vault-init.json` is provably isolated from git: gitignored,
     and nothing under `.secrets/` is tracked.
  4. Non-vacuity for (2): with `.secrets/vault-init.json` temporarily
     UNAVAILABLE, `scripts/vault-unseal.py` fails CLOSED with a clear, named
     error -- it never silently re-initializes a fresh Vault over live data.

**Safety note on test 4 (`test_unseal_fails_closed_when_init_file_is_missing`):**
this is the one test in this module that touches the REAL,
already-bootstrapped cluster's ONLY copy of its unseal key/root token
(`.secrets/vault-init.json` -- this project's own STATE.md documents an
earlier, unrelated session-1 loss of exactly this file via worktree
isolation, recovered only by destroying and re-bootstrapping Vault from
scratch). This test therefore never DELETES that file -- it renames
(`os.replace`) it aside for the exact duration of one subprocess call, an
atomic filesystem operation that can never produce a state where the data
does not exist SOMEWHERE on disk, restores it from that renamed path in a
`finally` block, and falls back to an in-memory byte-for-byte copy captured
before any mutation if the renamed path is ever unexpectedly missing. A
final assertion confirms the restored bytes are identical to the original.

**The other half of SEC-13, deliberately NOT run here (documented, not
performed):** full reproducibility from a COMPLETELY FRESH cluster (`kind
delete cluster` + recreate) is disproportionately slow for a per-wave gate
-- this module proves re-run idempotency against an already-live cluster
only. The manual verification instruction -- `make cluster-down && make
cluster-up && make vault-unseal && make vault-bootstrap && make
vault-verify` from a clean checkout, confirming all Vault e2e tests pass
with no manual intervention beyond those commands -- is recorded in
`05-VALIDATION.md`'s "Manual-Only Verifications" table (SEC-13 row) and
surfaced for a future operator in `docs/secrets-architecture.md` (plan
05-05).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    import hvac

pytestmark = pytest.mark.cluster

REPO_ROOT = Path(__file__).resolve().parents[3]
VAULT_INIT_FILE = REPO_ROOT / ".secrets" / "vault-init.json"
_BOOTSTRAP_SCRIPT = REPO_ROOT / "scripts" / "vault-bootstrap.py"
_UNSEAL_SCRIPT = REPO_ROOT / "scripts" / "vault-unseal.py"

# The roles/KV paths this phase's own scripts/vault-bootstrap.py manages --
# matching that script's own (a)-(i) step list exactly, so a snapshot
# genuinely covers everything a re-run could silently change.
_ROLE_NAMES = ("csv-processor", "airflow")
_KV_PATHS = (
    ("etl", "analytics-db"),
    ("etl", "minio"),
    ("airflow", "connections/minio_default"),
)

_SUBPROCESS_TIMEOUT_SECONDS = 60


def _snapshot(client: hvac.Client) -> dict[str, Any]:
    """Capture the live Vault state a bootstrap re-run must leave unchanged.

    Deliberately captures only STRUCTURE and VERSION NUMBERS -- auth
    methods, secrets-engine mounts, audit devices, each tracked role's
    definition (`bound_service_account_names`/policies/TTLs, never a
    credential), and each tracked KV path's current version integer -- NO
    secret VALUE is ever read here, so this snapshot is always safe to
    compare or print in a failure message.

    Args:
        client: A root-token-authenticated `hvac.Client`.

    Returns:
        A dict covering every piece of state `scripts/vault-bootstrap.py`
        could conceivably rewrite.
    """
    return {
        "auth_methods": client.sys.list_auth_methods()["data"],
        "secrets_engines": client.sys.list_mounted_secrets_engines()["data"],
        "audit_devices": client.sys.list_enabled_audit_devices()["data"],
        "roles": {name: client.auth.kubernetes.read_role(name=name) for name in _ROLE_NAMES},
        "kv_versions": {
            f"{mount}/{path}": client.secrets.kv.v2.read_secret_version(
                mount_point=mount,
                path=path,
            )["data"]["metadata"]["version"]
            for mount, path in _KV_PATHS
        },
    }


def test_rerunning_vault_bootstrap_against_a_live_vault_changes_nothing(
    vault_root_client: hvac.Client,
) -> None:
    """SEC-13: re-running scripts/vault-bootstrap.py against a live Vault is a no-op."""
    before = _snapshot(vault_root_client)

    proc = subprocess.run(  # noqa: S603
        [sys.executable, str(_BOOTSTRAP_SCRIPT)],
        capture_output=True,
        text=True,
        check=False,
        timeout=_SUBPROCESS_TIMEOUT_SECONDS,
    )
    assert proc.returncode == 0, (
        f"scripts/vault-bootstrap.py re-run failed (exit {proc.returncode}):\n{proc.stderr}"
    )

    after = _snapshot(vault_root_client)
    assert after == before, (
        "re-running scripts/vault-bootstrap.py against an already-bootstrapped Vault "
        f"changed live state.\nbefore={before!r}\nafter={after!r}"
    )


def test_rerunning_vault_unseal_against_an_already_unsealed_vault_is_a_noop() -> None:
    """SEC-13: re-running scripts/vault-unseal.py against an already-unsealed Vault is a no-op."""
    assert VAULT_INIT_FILE.is_file(), (
        f"{VAULT_INIT_FILE} does not exist -- run `make vault-unseal` first"
    )
    mtime_before = VAULT_INIT_FILE.stat().st_mtime_ns

    proc = subprocess.run(  # noqa: S603
        [sys.executable, str(_UNSEAL_SCRIPT)],
        capture_output=True,
        text=True,
        check=False,
        timeout=_SUBPROCESS_TIMEOUT_SECONDS,
    )
    assert proc.returncode == 0, (
        f"scripts/vault-unseal.py re-run failed (exit {proc.returncode}):\n{proc.stderr}"
    )
    assert "already unsealed" in proc.stdout, (
        f"expected 'already unsealed' in stdout, got: {proc.stdout!r}"
    )

    mtime_after = VAULT_INIT_FILE.stat().st_mtime_ns
    assert mtime_after == mtime_before, (
        f"{VAULT_INIT_FILE} mtime changed on a no-op re-run: {mtime_before} -> {mtime_after}"
    )


def test_vault_init_file_is_isolated_from_git() -> None:
    """SEC-13: the local unseal-key/root-token material is provably isolated from git."""
    check_ignore = subprocess.run(  # noqa: S603
        ["git", "check-ignore", str(VAULT_INIT_FILE)],  # noqa: S607
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=15,
    )
    assert check_ignore.returncode == 0, (
        f"{VAULT_INIT_FILE} is NOT reported as gitignored (git check-ignore exited "
        f"{check_ignore.returncode}) -- this local secret material could be committed:\n"
        f"{check_ignore.stderr}"
    )

    ls_files = subprocess.run(
        ["git", "ls-files", ".secrets"],  # noqa: S607
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
        timeout=15,
    )
    assert ls_files.stdout.strip() == "", f"git tracks file(s) under .secrets/: {ls_files.stdout!r}"


def test_unseal_fails_closed_when_init_file_is_missing(
    vault_root_client: hvac.Client,  # noqa: ARG001 -- ensures Vault is live/bootstrapped before this risks the real init file
) -> None:
    """SEC-13 non-vacuity: a MISSING .secrets/vault-init.json must fail closed, never re-initialize.

    See this module's docstring for the full safety design (atomic rename,
    never delete; in-memory byte backup as a second line of defence;
    restore verified by a final equality assertion).
    """
    assert VAULT_INIT_FILE.is_file(), f"{VAULT_INIT_FILE} does not exist -- nothing to back up"
    backup_path = VAULT_INIT_FILE.with_name(f"{VAULT_INIT_FILE.name}.test-backup")
    assert not backup_path.exists(), (
        f"{backup_path} already exists -- a PREVIOUS run of this test may have failed to "
        "restore the original file. Resolve this by hand (compare it against the live "
        "Vault's expectations) before re-running this test."
    )

    original_bytes = VAULT_INIT_FILE.read_bytes()
    original_mode = VAULT_INIT_FILE.stat().st_mode

    VAULT_INIT_FILE.replace(backup_path)
    try:
        assert not VAULT_INIT_FILE.exists(), "Path.replace did not remove the original path"

        proc = subprocess.run(  # noqa: S603
            [sys.executable, str(_UNSEAL_SCRIPT)],
            capture_output=True,
            text=True,
            check=False,
            timeout=_SUBPROCESS_TIMEOUT_SECONDS,
        )
        assert proc.returncode != 0, (
            "scripts/vault-unseal.py exited 0 with .secrets/vault-init.json missing -- it "
            "must fail closed, never silently re-initialize a fresh Vault over live data"
        )
        assert "already initialized" in proc.stderr, (
            f"expected a clear, named error mentioning 'already initialized'; "
            f"got stderr: {proc.stderr!r}"
        )
        assert "does not exist" in proc.stderr, (
            f"expected a clear, named error mentioning 'does not exist'; "
            f"got stderr: {proc.stderr!r}"
        )
    finally:
        if backup_path.exists():
            backup_path.replace(VAULT_INIT_FILE)
        elif not VAULT_INIT_FILE.exists():
            # Unreachable given Path.replace's atomicity, but never leave the
            # live cluster's only unseal-key copy unrecovered if it somehow
            # is: rewrite from the bytes captured before this test touched
            # anything.
            VAULT_INIT_FILE.write_bytes(original_bytes)
        VAULT_INIT_FILE.chmod(original_mode)

        restored_bytes = VAULT_INIT_FILE.read_bytes()
        assert restored_bytes == original_bytes, (
            "CRITICAL: .secrets/vault-init.json was not correctly restored after this "
            "non-vacuity test. Do NOT run `make vault-unseal` blindly -- it would "
            "re-initialize a NEW Vault root token/unseal key over live data. Recover "
            f"the original bytes this test captured in memory, or from {backup_path} "
            "if it still exists, before doing anything else."
        )
