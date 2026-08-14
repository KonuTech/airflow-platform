"""tests/e2e/vault/test_rotation.py -- D-03 live rotation proof, no restart required.

ROADMAP/05-CONTEXT.md D-03: rotate a credential's value in Vault, then
assert a RUNNING workload's *next* read of that path returns the new value
with no pod restart required -- proof over prose, matching this project's
Core Value (traceable, trusted, verifiable). This is a proof of the
*mechanism*, not an exhaustive rotation test of every credential Vault now
serves (D-03's own scope note) -- one credential path, `minio_default`, is
enough to demonstrate it.

**The read-once-vs-read-per-use distinction this test exists to draw:**
KPO/ETL pods (`csv_processor.cli._build_common()`, plan 05-02/05-03)
resolve every `vault://` reference exactly ONCE, at process start, inside a
short-lived pod. For that tier, "rotation" observed with no restart is
trivially true in the wrong way -- there is no long-running process to
demonstrate against, only a NEW pod for a NEW task run, which always reads
whatever is current at ITS OWN start. Airflow's own long-running
components (api-server, triggerer, worker) are different: `VaultBackend`
(`providers-hashicorp`, wired in plan 05-03) is Airflow's secrets BACKEND,
consulted on every `BaseHook.get_connection()` call, and
`AIRFLOW__SECRETS__USE_CACHE` defaults to `False` (confirmed via
`airflow.secrets.cache` and the official docs, this plan's own Interfaces
section) -- so every single lookup of `minio_default` is a live, uncached
Vault read, with no code change needed to make that true. This module
proves exactly that: a value rotated in Vault is picked up by the SAME
already-running `airflow-api-server` pod's very next CLI-driven connection
lookup, with no pod deleted, restarted, or redeployed anywhere in this
test.

The rotated value is deliberately NOT a change to `login`/`password`/
`endpoint_url`/`region_name` (the fields a real S3 client would actually
consume) -- it appends one new, harmless, unknown query parameter to the
existing `conn_uri`'s query string. Verified live, this session, against
the actually-installed `apache-airflow-providers-amazon` provider:
`AwsConnectionWrapper.__post_init__` calls
`self._get_credentials(**extra)`, and `_get_credentials`'s signature ends
in `**kwargs` -- an unrecognised extra key is silently absorbed, never
raised on. This keeps the connection fully functional throughout the
test (including for any concurrently-running `S3KeySensor` poke against
the SAME live connection, e.g. from the background DagRun backlog
documented in this phase's own STATE.md), while still producing an
observably different `airflow connections get` result -- exactly what D-03
needs proven, with zero risk to concurrently-running pipeline traffic.
"""

from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    import subprocess
    from collections.abc import Callable

    import hvac

pytestmark = pytest.mark.cluster

NAMESPACE = "airflow"
_CONN_ID = "minio_default"
_VAULT_MOUNT = "airflow"
_VAULT_PATH = "connections/minio_default"

# `password` and `get_uri` both embed the connection's real MinIO secret
# key -- see this module's own docstring and the codebase's established
# convention (tests/e2e/vault/test_positive_auth.py's own docstring: never
# construct an assertion whose FAILURE message could print a credential
# value). Every comparison/failure message in this file operates on a
# `_sanitized()` copy, never the raw row.
_SECRET_BEARING_FIELDS = ("password", "get_uri")


def _read_minio_default(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    """Read `minio_default` via the CLI, against the live `airflow-api-server` pod.

    Args:
        kubectl: The session-scoped kubectl helper fixture.

    Returns:
        The single parsed row `airflow connections get -o json` prints.
    """
    proc = kubectl(
        "-n",
        NAMESPACE,
        "exec",
        "deploy/airflow-api-server",
        "--",
        "airflow",
        "connections",
        "get",
        _CONN_ID,
        "-o",
        "json",
    )
    assert proc.returncode == 0, (
        f"airflow connections get {_CONN_ID} failed (exit {proc.returncode}):\n{proc.stderr}"
    )
    rows = json.loads(proc.stdout)
    assert len(rows) == 1, f"expected exactly one row for {_CONN_ID}, got {len(rows)} rows"
    return rows[0]


def _sanitized(connection: dict[str, Any]) -> dict[str, Any]:
    """Redact secret-bearing fields before any comparison or failure message.

    Args:
        connection: A row `_read_minio_default` returned.

    Returns:
        A shallow copy with every field named in `_SECRET_BEARING_FIELDS`
        replaced by the literal string `"<redacted>"`.
    """
    return {
        key: ("<redacted>" if key in _SECRET_BEARING_FIELDS else value)
        for key, value in connection.items()
    }


def test_rotating_minio_default_is_observed_with_no_restart(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    vault_root_client: hvac.Client,
) -> None:
    """D-03: a value rotated in Vault is reflected on the SAME pod's next read.

    Rotates `airflow/connections/minio_default`'s `conn_uri` field to a new
    KV version (a harmless, ignorable query parameter appended -- see
    module docstring), reads `minio_default` via the CLI before and after
    against the SAME already-running `deploy/airflow-api-server`, and
    restores the original value in a `finally` block regardless of
    outcome, confirmed by re-reading it.
    """
    before_secret = vault_root_client.secrets.kv.v2.read_secret_version(
        mount_point=_VAULT_MOUNT,
        path=_VAULT_PATH,
    )
    original_conn_uri: str = before_secret["data"]["data"]["conn_uri"]

    before_read = _read_minio_default(kubectl)
    if "rotation_probe" in before_read.get("extra_dejson", {}):
        pytest.fail(
            "airflow/connections/minio_default already carries a leftover "
            "'rotation_probe' key from a previous, incompletely-restored run of this "
            "test -- resolve manually (read the raw conn_uri from Vault, strip the "
            "trailing rotation_probe query parameter, write it back) rather than "
            "compounding the mess by proceeding",
        )

    marker = uuid.uuid4().hex[:12]
    separator = "&" if "?" in original_conn_uri else "?"
    rotated_conn_uri = f"{original_conn_uri}{separator}rotation_probe={marker}"

    try:
        vault_root_client.secrets.kv.v2.create_or_update_secret(
            mount_point=_VAULT_MOUNT,
            path=_VAULT_PATH,
            secret={"conn_uri": rotated_conn_uri},
        )

        after_read = _read_minio_default(kubectl)

        before_sanitized = _sanitized(before_read)
        after_sanitized = _sanitized(after_read)
        assert after_sanitized != before_sanitized, (
            "the second CLI read (against the SAME running pod, no restart) is "
            f"unchanged after rotation (redacted): {after_sanitized!r}"
        )

        observed_marker = after_read.get("extra_dejson", {}).get("rotation_probe")
        assert observed_marker == marker, (
            f"the second read does not reflect the newly-written rotation marker "
            f"(got {observed_marker!r}, expected {marker!r})"
        )
    finally:
        vault_root_client.secrets.kv.v2.create_or_update_secret(
            mount_point=_VAULT_MOUNT,
            path=_VAULT_PATH,
            secret={"conn_uri": original_conn_uri},
        )
        restored_secret = vault_root_client.secrets.kv.v2.read_secret_version(
            mount_point=_VAULT_MOUNT,
            path=_VAULT_PATH,
        )
        restored_conn_uri = restored_secret["data"]["data"]["conn_uri"]
        assert restored_conn_uri == original_conn_uri, (
            "failed to restore airflow/connections/minio_default to its original "
            "value -- this test does NOT leave a rotated credential behind on success, "
            "but this assertion firing means that guarantee just broke"
        )
