#!/usr/bin/env python3
r"""scripts/vault-unseal.py -- D-02: single-command init-or-unseal.

`make vault-unseal` runs this. Against a freshly-installed Vault (never
initialized), it performs a single-share, threshold-1 Shamir's-secret-
sharing initialization ceremony (`secret_shares=1, secret_threshold=1`) --
D-02's explicit choice: real seal/unseal mechanics, only the multi-key
ceremony itself is skipped for local convenience -- and writes the one-time
unseal key and root token to `.secrets/vault-init.json` (gitignored,
`chmod 600`). Against an already-initialized-but-sealed Vault (e.g. after a
`vault-0` pod restart), it reads that same file and submits the unseal key.
Against an already-unsealed Vault, it does nothing but report status.

This is explicitly a LOCAL-DEV-ONLY convenience, not the production design:
a real deployment would use auto-unseal (cloud KMS / transit) or a genuine
multi-key-holder ceremony, never a single local key file (SEC-14's
production-substitution documentation, plan 05-05, records this).

Never prints the unseal key or root token value -- only status lines
("initialized", "unsealed", "already unsealed"/"already initialized").

Run location: this script runs from a developer's own host machine, the
same place `scripts/ingest-demo.py` and `tests/e2e/cluster/` run from --
never from inside the cluster. It reaches Vault the same way
`scripts/ingest-demo.py`'s `_port_forwarded_analytics` reaches the
analytical PostgreSQL cluster: a torn-down-on-exit `kubectl port-forward`,
never a direct in-cluster address (source: scripts/ingest-demo.py, this
session).
"""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

import hvac
import hvac.exceptions

if TYPE_CHECKING:
    from collections.abc import Iterator

_REPO_ROOT = Path(__file__).resolve().parent.parent
_VERSIONS_ENV = _REPO_ROOT / "helm" / "versions.env"
_INIT_FILE = _REPO_ROOT / ".secrets" / "vault-init.json"

_VAULT_NAMESPACE = "vault"
_VAULT_SERVICE = "vault"
_VAULT_PORT = 8200

# D-02: a single-share, threshold-1 ceremony. Real Shamir's-secret-sharing
# mechanics still apply (Vault genuinely splits its master key and requires
# a submitted share to unseal) -- only the "multiple key holders" part of a
# production ceremony is skipped, deliberately, for local convenience.
_SECRET_SHARES = 1
_SECRET_THRESHOLD = 1


def _versions_env_variable(name: str) -> str:
    """Read a `KEY=value` line from `helm/versions.env` (the single source, plan 02-01).

    Args:
        name: The variable name to look up.

    Returns:
        The variable's value, with surrounding whitespace stripped.

    Raises:
        RuntimeError: `name` is not defined in `helm/versions.env`.
    """
    text = _VERSIONS_ENV.read_text(encoding="utf-8")
    for line in text.splitlines():
        if line.startswith(f"{name}="):
            return line.split("=", 1)[1].strip()
    msg = f"helm/versions.env does not define {name}"
    raise RuntimeError(msg)


def _kubectl_context() -> str:
    """Return the kubectl context kind registers for this cluster: `kind-<name>`.

    Same convention as `scripts/ingest-demo.py`'s `_kubectl_context` --
    never the ambient current-context.

    Returns:
        The `kind-<CLUSTER_NAME>` context string.
    """
    return f"kind-{_versions_env_variable('CLUSTER_NAME')}"


def _require_kubectl() -> str:
    """Resolve the absolute path to the `kubectl` binary on `PATH`.

    Returns:
        The absolute path to `kubectl`.

    Raises:
        RuntimeError: `kubectl` is not found on `PATH`.
    """
    kubectl_bin = shutil.which("kubectl")
    if kubectl_bin is None:
        msg = "kubectl not found on PATH"
        raise RuntimeError(msg)
    return kubectl_bin


def _free_local_port() -> int:
    """Ask the OS for an unused TCP port, then release it immediately.

    Same shape as `scripts/ingest-demo.py`'s `_free_local_port`.

    Returns:
        A locally free TCP port number.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextlib.contextmanager
def _port_forwarded_vault(kubectl_context: str) -> Iterator[int]:
    """Port-forward `svc/vault` (namespace `vault`, port 8200) to a free local port.

    Same shape as `scripts/ingest-demo.py`'s `_port_forwarded_analytics` --
    torn down on exit via `proc.terminate()` in `finally`.

    Args:
        kubectl_context: The kubectl context to port-forward through.

    Yields:
        The local port the tunnel is listening on.

    Raises:
        RuntimeError: The port-forward process exits before ever accepting a
            connection, or never accepts one within 30s.
    """
    kubectl_bin = _require_kubectl()
    local_port = _free_local_port()
    proc = subprocess.Popen(  # noqa: S603
        [
            kubectl_bin,
            "--context",
            kubectl_context,
            "-n",
            _VAULT_NAMESPACE,
            "port-forward",
            f"svc/{_VAULT_SERVICE}",
            f"{local_port}:{_VAULT_PORT}",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        deadline = time.monotonic() + 30
        connected = False
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                output = proc.stdout.read() if proc.stdout else ""
                msg = f"kubectl port-forward for svc/{_VAULT_SERVICE} exited early:\n{output}"
                raise RuntimeError(msg)
            with (
                contextlib.suppress(OSError),
                socket.create_connection(("127.0.0.1", local_port), timeout=1),
            ):
                connected = True
                break
            time.sleep(0.5)
        if not connected:
            msg = (
                f"kubectl port-forward for svc/{_VAULT_SERVICE} never "
                "accepted a connection within 30s"
            )
            raise RuntimeError(msg)
        yield local_port
    finally:
        proc.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=10)


def _write_init_file(unseal_key: str, root_token: str) -> None:
    """Write `.secrets/vault-init.json` with mode 0600, creating its parent directory.

    Never logs or prints `unseal_key`/`root_token`.

    Args:
        unseal_key: The single Shamir's-secret-sharing key share.
        root_token: Vault's one-time initialization root token.
    """
    _INIT_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {"unseal_key": unseal_key, "root_token": root_token}
    _INIT_FILE.write_text(json.dumps(payload), encoding="utf-8")
    os.chmod(_INIT_FILE, 0o600)  # noqa: PTH101


def _read_init_file() -> dict[str, str]:
    """Read `.secrets/vault-init.json`.

    Returns:
        The parsed `{"unseal_key": ..., "root_token": ...}` mapping.

    Raises:
        RuntimeError: The file does not exist -- Vault is already
            initialized but the local key material is missing, which is
            unrecoverable without a fresh Vault (a new PVC).
    """
    if not _INIT_FILE.is_file():
        msg = (
            f"Vault is already initialized, but {_INIT_FILE} does not exist. "
            "The unseal key is only ever revealed once, at initialization time -- "
            "without it, this Vault cannot be unsealed. Recovery requires a fresh "
            "Vault (delete the data-vault-0 PVC and reinstall)."
        )
        raise RuntimeError(msg)
    data: dict[str, str] = json.loads(_INIT_FILE.read_text(encoding="utf-8"))
    return data


def unseal(client: hvac.Client) -> None:
    """Initialize (if needed) and unseal (if needed) the Vault reachable via `client`.

    Args:
        client: An `hvac.Client` already pointed at the forwarded local port.
    """
    if not client.sys.is_initialized():
        response = client.sys.initialize(
            secret_shares=_SECRET_SHARES,
            secret_threshold=_SECRET_THRESHOLD,
        )
        unseal_key = response["keys"][0]
        root_token = response["root_token"]
        _write_init_file(unseal_key, root_token)
        print("initialized")
    else:
        unseal_key = _read_init_file()["unseal_key"]

    if client.sys.is_sealed():
        client.sys.submit_unseal_key(unseal_key)
        print("unsealed")
    else:
        print("already unsealed")


def main() -> int:
    """Port-forward to Vault, then initialize-or-unseal it.

    Returns:
        `0` on success; `1` if the port-forward or the Vault API calls fail.
    """
    kubectl_context = _kubectl_context()
    try:
        with _port_forwarded_vault(kubectl_context) as local_port:
            client = hvac.Client(url=f"http://127.0.0.1:{local_port}")
            unseal(client)
    except (RuntimeError, hvac.exceptions.VaultError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
