"""tests/e2e/vault/test_audit_log.py -- SEC-08 live proof: auditable, no secret values logged.

SEC-08: "Secret access is auditable -- which workload read which path, when,
and whether it succeeded -- without logging secret values" (REQUIREMENTS.md
DoD 97). This module proves both halves live, against the SAME persistent
audit log file `scripts/vault-audit-tail.py` (D-04, this plan's Task 2)
renders for humans:

  1. **Positive half:** the audit log contains at least one entry recording
     a SUCCESSFUL `auth/kubernetes/login` for the `csv-processor` role
     (reusing `test_positive_auth.py`'s own login call), and at least one
     entry recording a DENIED login for the same path (reusing
     `test_negative_auth.py`'s own `default`-ServiceAccount attempt) --
     "which workload... whether it succeeded" is not merely configured, it
     is OBSERVED in the log this test reads.
  2. **Negative half (T-05-04):** the raw log text, searched as a WHOLE
     (not per-field, since Vault's HMAC-hashing is applied per string value
     and this test must not assume it knows every field that could carry
     one), never contains the CURRENT plaintext value of the analytical DB
     DSN, the MinIO secret key, the MinIO access key, or the Airflow
     `minio_default` connection URI -- all four read fresh via
     `vault_root_client` as part of this test's own setup, immediately
     before the comparison, so this is not a check against a stale or
     assumed value.

A non-vacuity control (a KNOWN-PRESENT string asserted present) proves the
containment check itself is not silently always-true -- see
`test_no_plaintext_secret_value_appears_in_the_audit_log`'s own body.

The audit log is read via the SAME `kubectl exec -i -n vault vault-0 --
tail -n <N> /vault/audit/audit.log` mechanism `scripts/vault-audit-tail.py`
uses, duplicated here (this repository's established small-helper
convention, `tests/e2e/vault/conftest.py`'s own docstring) rather than
imported -- `scripts/` and `tests/` are deliberately independent call
surfaces. `_tail_audit_log` below mirrors `scripts/vault-audit-tail.py`'s
own `_tail_audit_log` + `render`'s parse step exactly; see the comment atop
it.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import hvac
import hvac.exceptions
import pytest

if TYPE_CHECKING:
    import subprocess
    from collections.abc import Callable

pytestmark = pytest.mark.cluster

_VAULT_NAMESPACE = "vault"
_VAULT_POD = "vault-0"
_AUDIT_LOG_PATH = "/vault/audit/audit.log"

# Generous relative to scripts/vault-audit-tail.py's own default (200): this
# module triggers its own two login attempts immediately before tailing, but
# the live cluster also carries ambient background traffic (this phase's own
# STATE.md documents a still-draining DagRun backlog generating its own
# Vault logins/reads) -- a wider window makes it very unlikely those two
# freshly-triggered entries fall outside the tail before this test reads it.
_TAIL_LINES = 1000

_LOGIN_PATH = "auth/kubernetes/login"


def _kubectl_create_token(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    *,
    service_account: str,
    namespace: str,
) -> str:
    """Obtain a fresh projected token for `service_account` in `namespace`.

    Duplicated verbatim from `test_positive_auth.py`/`test_negative_auth.py`
    (this repository's small-helper convention).

    Args:
        kubectl: The session-scoped kubectl helper fixture.
        service_account: The ServiceAccount name to mint a token for.
        namespace: The ServiceAccount's namespace.

    Returns:
        The projected JWT, stripped of trailing whitespace.
    """
    proc = kubectl("create", "token", service_account, "-n", namespace)
    assert proc.returncode == 0, (
        f"kubectl create token {service_account} -n {namespace} failed "
        f"(exit {proc.returncode}):\n{proc.stderr}"
    )
    return proc.stdout.strip()


def _tail_audit_log(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    lines: int = _TAIL_LINES,
) -> tuple[list[dict[str, Any]], str]:
    """Read and parse the last `lines` entries of Vault's persistent audit log.

    Mirrors `scripts/vault-audit-tail.py`'s own `_tail_audit_log` (the exec
    invocation) and `render` (the per-line `json.loads`, skip-on-failure
    parse step) -- duplicated, not imported, per this module's own
    docstring.

    Args:
        kubectl: The session-scoped kubectl helper fixture.
        lines: How many trailing audit-log lines to read.

    Returns:
        A `(parsed_entries, raw_text)` tuple. `parsed_entries` skips any
        line that fails to parse as JSON. `raw_text` is the complete,
        unparsed stdout -- the SEC-08 negative (plaintext-absence) check
        below searches this AS A WHOLE, not per-field.
    """
    proc = kubectl(
        "exec",
        "-i",
        "-n",
        _VAULT_NAMESPACE,
        _VAULT_POD,
        "--",
        "tail",
        "-n",
        str(lines),
        _AUDIT_LOG_PATH,
    )
    assert proc.returncode == 0, (
        f"kubectl exec -i -n {_VAULT_NAMESPACE} {_VAULT_POD} -- tail -n {lines} "
        f"{_AUDIT_LOG_PATH} failed (exit {proc.returncode}):\n{proc.stderr}"
    )
    entries: list[dict[str, Any]] = []
    for line in proc.stdout.splitlines():
        if not line.strip():
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return entries, proc.stdout


@pytest.fixture(scope="module")
def audit_log_after_known_attempts(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    vault_addr: str,
) -> tuple[list[dict[str, Any]], str]:
    """Trigger one known successful login and one known denied login, then tail once.

    Module-scoped so every test function below shares the SAME tailed
    snapshot -- the two logins are only triggered once for this whole
    module, not once per test function.

    The successful login reuses `test_positive_auth.py`'s own call shape
    (`csv-processor` role, `etl` namespace); the denied login reuses
    `test_negative_auth.py`'s own (`default` ServiceAccount, `etl`
    namespace, against the SAME `csv-processor` role it is not bound to).

    Args:
        kubectl: The session-scoped kubectl helper fixture.
        vault_addr: The session-scoped, port-forwarded Vault base URL.

    Returns:
        The `(parsed_entries, raw_text)` tuple `_tail_audit_log` returns,
        read immediately after both login attempts.
    """
    csv_processor_jwt = _kubectl_create_token(
        kubectl,
        service_account="csv-processor",
        namespace="etl",
    )
    success_client = hvac.Client(url=vault_addr)
    success_client.auth.kubernetes.login(role="csv-processor", jwt=csv_processor_jwt)

    default_jwt = _kubectl_create_token(kubectl, service_account="default", namespace="etl")
    denied_client = hvac.Client(url=vault_addr)
    with pytest.raises(hvac.exceptions.VaultError):
        denied_client.auth.kubernetes.login(role="csv-processor", jwt=default_jwt)

    return _tail_audit_log(kubectl)


def test_audit_log_records_the_known_successful_login(
    audit_log_after_known_attempts: tuple[list[dict[str, Any]], str],
) -> None:
    """SEC-08 positive half: a successful login names its identity and outcome."""
    entries, _raw_text = audit_log_after_known_attempts
    successes = [
        entry
        for entry in entries
        if entry.get("request", {}).get("path") == _LOGIN_PATH
        and not entry.get("error")
        and entry.get("auth", {}).get("metadata", {}).get("service_account_name") == "csv-processor"
    ]
    assert successes, (
        "no successful auth/kubernetes/login entry for the csv-processor "
        "ServiceAccount was found in the tailed audit log"
    )


def test_audit_log_records_the_known_denied_login(
    audit_log_after_known_attempts: tuple[list[dict[str, Any]], str],
) -> None:
    """SEC-08 positive half: a denied login is recorded with a non-empty error, same path."""
    entries, _raw_text = audit_log_after_known_attempts
    denials = [
        entry
        for entry in entries
        if entry.get("request", {}).get("path") == _LOGIN_PATH and entry.get("error")
    ]
    assert denials, "no denied auth/kubernetes/login entry was found in the tailed audit log"


def test_no_plaintext_secret_value_appears_in_the_audit_log(
    audit_log_after_known_attempts: tuple[list[dict[str, Any]], str],
    vault_root_client: hvac.Client,
) -> None:
    """SEC-08/T-05-04 negative half: no secret value this phase manages ever appears in the clear.

    Reads the CURRENT plaintext value of every credential this phase
    migrated to Vault, then asserts each is absent from the raw tailed log
    text -- a behavioural proof (the value is genuinely not there), not
    merely an inspection that redaction is configured. A non-vacuity
    control (a string KNOWN to be present) proves the containment check
    itself can actually detect a match, so the absence assertions below
    are not silently vacuous.
    """
    _entries, raw_text = audit_log_after_known_attempts

    assert _LOGIN_PATH in raw_text, (
        "non-vacuity check failed: a string known to be present "
        f"({_LOGIN_PATH!r}) was not found in the tailed log text -- the "
        "containment check itself is broken, so the absence assertions below "
        "would prove nothing"
    )

    dsn = vault_root_client.secrets.kv.v2.read_secret_version(
        mount_point="etl",
        path="analytics-db",
    )["data"]["data"]["dsn"]
    minio_secret = vault_root_client.secrets.kv.v2.read_secret_version(
        mount_point="etl",
        path="minio",
    )["data"]["data"]
    access_key = minio_secret["access_key"]
    secret_key = minio_secret["secret_key"]
    airflow_conn_uri = vault_root_client.secrets.kv.v2.read_secret_version(
        mount_point="airflow",
        path="connections/minio_default",
    )["data"]["data"]["conn_uri"]

    for value, name in (
        (dsn, "etl/analytics-db#dsn"),
        (access_key, "etl/minio#access_key"),
        (secret_key, "etl/minio#secret_key"),
        (airflow_conn_uri, "airflow/connections/minio_default#conn_uri"),
    ):
        assert value not in raw_text, (
            f"{name}'s current plaintext value was found in the audit log text -- "
            "SEC-08 requires it never appear unredacted"
        )
