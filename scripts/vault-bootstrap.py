#!/usr/bin/env python3
r"""scripts/vault-bootstrap.py -- idempotent hvac-based Vault admin bootstrap.

`make vault-bootstrap` runs this against an already-UNSEALED Vault (run
`make vault-unseal` first -- this script FAILS FAST on a sealed Vault rather
than silently unsealing it itself; unseal is D-02's own deliberate, separate
step). It creates, if not already present:

  (a) KV v2 mounts `etl` and `airflow`
  (b) the `kubernetes` auth method, configured against the in-cluster API
      server
  (c) policy `csv-processor` (read access to its own two KV paths)
  (d) role `csv-processor`, bound to ServiceAccount `csv-processor` in
      namespace `etl` -- FINAL, not a guess (kubernetes/rbac-etl.yaml,
      plan 04-02, already fixes this identity)
  (e) policy `airflow` (read access to `airflow/data/connections/*`)
  (f) role `airflow`, bound to ServiceAccount `airflow-api-server` in
      namespace `airflow` -- a DOCUMENTED BEST GUESS (05-RESEARCH.md
      Pitfall 1: which Airflow component actually performs the
      `VaultBackend` login is genuinely unverified), to be empirically
      corrected by plan 05-03 using this same audit-log-reading discipline
      -- never widened to a wildcard or multiple SAs as a shortcut
  (g) a `file` audit device at `/vault/audit/audit.log`

Every step is idempotent: re-running this script against an already-
bootstrapped Vault performs zero writes and prints "already present" for
each step. This mirrors `scripts/etl-secrets.sh`'s
`_secret_exists`-before-`_apply_secret` shape -- read first, write only if
missing.

The root token, read once from `.secrets/vault-init.json`, is the ONLY
place in this codebase that value is ever read. It is never logged,
printed, or passed to a subprocess as an argument. No step here ever
prints a secret value, token, or key.
"""

from __future__ import annotations

import contextlib
import json
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

_KUBERNETES_HOST = "https://kubernetes.default.svc.cluster.local:443"

_CSV_PROCESSOR_POLICY = """\
path "etl/data/analytics-db" { capabilities = ["read"] }
path "etl/data/minio" { capabilities = ["read"] }
"""

_AIRFLOW_POLICY = """\
path "airflow/data/connections/*" { capabilities = ["read"] }
"""

_AUDIT_DEVICE_PATH = "/vault/audit/audit.log"


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

    Same convention as `scripts/vault-unseal.py`'s `_kubectl_context` --
    kept in sync as siblings; never the ambient current-context.

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

    Same shape as `scripts/vault-unseal.py`'s `_free_local_port`.

    Returns:
        A locally free TCP port number.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextlib.contextmanager
def _port_forwarded_vault(kubectl_context: str) -> Iterator[int]:
    """Port-forward `svc/vault` (namespace `vault`, port 8200) to a free local port.

    Sibling of `scripts/vault-unseal.py`'s `_port_forwarded_vault` -- kept
    in sync as a deliberate duplication (this repository's convention: small
    helpers are copied, not shared through a library module).

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


def _read_root_token() -> str:
    """Read the root token from `.secrets/vault-init.json`.

    This is the ONLY place in this codebase the root token is ever read.
    Never logged or printed by this function or any caller.

    Returns:
        The root token string.

    Raises:
        RuntimeError: The init file does not exist -- Vault has not been
            unsealed yet.
    """
    if not _INIT_FILE.is_file():
        msg = f"{_INIT_FILE} does not exist -- run `make vault-unseal` first"
        raise RuntimeError(msg)
    data: dict[str, str] = json.loads(_INIT_FILE.read_text(encoding="utf-8"))
    return data["root_token"]


def _ensure_kv_v2_mounts(client: hvac.Client) -> None:
    """(a) Enable KV v2 mounts `etl` and `airflow`, if not already mounted."""
    existing = client.sys.list_mounted_secrets_engines()["data"]
    for mount in ("etl", "airflow"):
        if f"{mount}/" in existing:
            print(f"mount {mount}/: already present")
            continue
        client.sys.enable_secrets_engine(backend_type="kv", path=mount, options={"version": "2"})
        print(f"mount {mount}/: created (kv-v2)")


def _ensure_kubernetes_auth_method(client: hvac.Client) -> None:
    """(b) Enable and configure the `kubernetes` auth method, if not already enabled."""
    existing = client.sys.list_auth_methods()["data"]
    if "kubernetes/" in existing:
        print("auth method kubernetes/: already present")
        return
    client.sys.enable_auth_method("kubernetes")
    client.auth.kubernetes.configure(kubernetes_host=_KUBERNETES_HOST)
    print("auth method kubernetes/: created and configured")


def _ensure_policy(client: hvac.Client, name: str, policy_hcl: str) -> None:
    """(c)/(e) Write a policy, if not already present under `name`."""
    existing = client.sys.list_policies()["data"]["policies"]
    if name in existing:
        print(f"policy {name}: already present")
        return
    client.sys.create_or_update_policy(name=name, policy=policy_hcl)
    print(f"policy {name}: created")


def _ensure_kubernetes_role(
    client: hvac.Client,
    *,
    name: str,
    bound_service_account_names: list[str],
    bound_service_account_namespaces: list[str],
    policies: list[str],
) -> None:
    """(d)/(f) Write a kubernetes-auth role, if not already present under `name`.

    `list_roles()` raises `hvac.exceptions.InvalidPath` both when the
    kubernetes auth method has no roles at all yet AND when (in principle)
    the mount itself is absent -- verified empirically against a live
    Vault (05-01), since Vault's LIST semantics return 404 for a truly
    empty collection rather than an empty list. Both cases mean "this role
    does not exist yet" here, since `_ensure_kubernetes_auth_method` always
    runs first.
    """
    try:
        existing = client.auth.kubernetes.list_roles()
        existing_names = existing["keys"] if existing else []
    except hvac.exceptions.InvalidPath:
        existing_names = []

    if name in existing_names:
        print(f"role {name}: already present")
        return

    client.auth.kubernetes.create_role(
        name=name,
        bound_service_account_names=bound_service_account_names,
        bound_service_account_namespaces=bound_service_account_namespaces,
        policies=policies,
        ttl="20m",
        max_ttl="1h",
    )
    print(f"role {name}: created")


def _ensure_audit_device(client: hvac.Client) -> None:
    """(g) Enable a persistent `file` audit device, if not already enabled."""
    existing = client.sys.list_enabled_audit_devices()["data"]
    if "file/" in existing:
        print("audit device file/: already present")
        return
    client.sys.enable_audit_device(device_type="file", options={"file_path": _AUDIT_DEVICE_PATH})
    print("audit device file/: created")


def bootstrap(client: hvac.Client) -> None:
    """Run every idempotent bootstrap step against an authenticated, unsealed `client`.

    Args:
        client: An `hvac.Client` authenticated with the root token.

    Raises:
        RuntimeError: `client`'s Vault is sealed -- bootstrap never
            unseals; that is `scripts/vault-unseal.py`'s job alone.
    """
    if client.sys.is_sealed():
        msg = "Vault is sealed -- run `make vault-unseal` first (bootstrap never unseals)"
        raise RuntimeError(msg)

    _ensure_kv_v2_mounts(client)
    _ensure_kubernetes_auth_method(client)

    _ensure_policy(client, "csv-processor", _CSV_PROCESSOR_POLICY)
    # FINAL binding: the KPO pod's ServiceAccount identity is already fixed
    # by kubernetes/rbac-etl.yaml (plan 04-02) -- no ambiguity analogous to
    # Airflow's multiple candidate ServiceAccounts (see the role below).
    _ensure_kubernetes_role(
        client,
        name="csv-processor",
        bound_service_account_names=["csv-processor"],
        bound_service_account_namespaces=["etl"],
        policies=["csv-processor"],
    )

    _ensure_policy(client, "airflow", _AIRFLOW_POLICY)
    # DOCUMENTED BEST GUESS (05-RESEARCH.md Pitfall 1): "API server is the
    # sole metadata-DB access point for tasks and workers" is suggestive,
    # not confirmed, for secrets-backend lookups. Plan 05-03 empirically
    # corrects this binding by reading the Vault audit log this step's own
    # audit device (g) makes possible -- never widened to a wildcard or
    # multiple SAs as a shortcut in the meantime.
    _ensure_kubernetes_role(
        client,
        name="airflow",
        bound_service_account_names=["airflow-api-server"],
        bound_service_account_namespaces=["airflow"],
        policies=["airflow"],
    )

    _ensure_audit_device(client)


def main() -> int:
    """Port-forward to Vault, authenticate with the root token, then bootstrap it.

    Returns:
        `0` on success; `1` if the port-forward, root-token read, or any
        Vault API call fails.
    """
    kubectl_context = _kubectl_context()
    try:
        root_token = _read_root_token()
        with _port_forwarded_vault(kubectl_context) as local_port:
            client = hvac.Client(url=f"http://127.0.0.1:{local_port}", token=root_token)
            bootstrap(client)
    except (RuntimeError, hvac.exceptions.VaultError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
