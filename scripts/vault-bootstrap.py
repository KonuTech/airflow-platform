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
  (f) role `airflow`, bound to ServiceAccounts `airflow-api-server`,
      `airflow-triggerer`, `airflow-worker` and `airflow-scheduler` in
      namespace `airflow` -- EMPIRICALLY CORRECTED (plan 05-03) from plan
      05-01's original single-SA best guess (05-RESEARCH.md Pitfall 1); see
      this role's own definition in `bootstrap()` for the per-SA evidence
      -- never widened to a wildcard or "every candidate" as a shortcut
  (g) a `file` audit device at `/vault/audit/audit.log`
  (h) the two `etl` KV secret VALUES (plan 05-02) -- `etl/analytics-db`
      (`dsn`) and `etl/minio` (`access_key`/`secret_key`) -- sourced from
      the live `csv-processor-db`/`csv-processor-s3` Kubernetes Secrets
      `scripts/etl-secrets.sh` already created (Phase 4), so the value the
      KPO pod reads through `vault://` is identical to the value it read
      through `secretKeyRef` before this plan's swap
  (i) the `airflow/connections/minio_default` KV secret VALUE (plan 05-03)
      -- field name `conn_uri`, matching `providers-hashicorp`'s own
      `VaultBackend.get_connection` convention (`response.get("conn_uri")`,
      verified against the installed provider's source: it prioritizes
      `conn_uri` if present, falling back to `Connection(conn_id,
      **response)` otherwise) -- sourced from the live
      `airflow-minio-connection` Kubernetes Secret `scripts/etl-secrets.sh`
      already created (Phase 4), so the value `VaultBackend` resolves is
      identical to the value the Secret carried before this plan's swap

Every step is idempotent: re-running this script against an already-
bootstrapped Vault performs zero writes and prints "already present" for
each step whose live state already matches its target. This mirrors
`scripts/etl-secrets.sh`'s `_secret_exists`-before-`_apply_secret` shape --
read first, write only if missing. One step, (f), also self-corrects DRIFT
(not just absence): a kubernetes-auth role whose live
`bound_service_account_names` no longer matches this script's own target
list is RE-WRITTEN, not skipped -- this is the exact mechanism plan 05-03
uses to correct plan 05-01's original best guess in place, on a Vault that
was already bootstrapped once.

The root token, read once from `.secrets/vault-init.json`, is the ONLY
place in this codebase that value is ever read. It is never logged,
printed, or passed to a subprocess as an argument. No step here ever
prints a secret value, token, or key.
"""

from __future__ import annotations

import base64
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

# (h) The two live Kubernetes Secrets `scripts/etl-secrets.sh` already
# created (Phase 4) -- the SOURCE of the KV values this step writes, never
# printed. `_MINIO_APP_ACCESS_KEY` is the same fixed, non-secret literal
# `scripts/etl-secrets.sh`'s own `MINIO_APP_ACCESS_KEY` hardcodes.
_ETL_NAMESPACE = "etl"
_DB_SECRET_NAME = "csv-processor-db"  # noqa: S105 -- a K8s Secret's `metadata.name`, not a credential
_S3_SECRET_NAME = "csv-processor-s3"  # noqa: S105 -- a K8s Secret's `metadata.name`, not a credential
_MINIO_APP_ACCESS_KEY = "etl-app"

# (i) The live Kubernetes Secret `scripts/etl-secrets.sh` already created
# (Phase 4) -- the SOURCE of the `airflow/connections/minio_default` KV
# value this step writes, never printed.
_AIRFLOW_NAMESPACE = "airflow"
_AIRFLOW_MINIO_SECRET_NAME = "airflow-minio-connection"  # noqa: S105 -- a K8s Secret's `metadata.name`, not a credential


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
    """(d)/(f) Write a kubernetes-auth role, creating it if absent or correcting drift if not.

    `list_roles()` raises `hvac.exceptions.InvalidPath` both when the
    kubernetes auth method has no roles at all yet AND when (in principle)
    the mount itself is absent -- verified empirically against a live
    Vault (05-01), since Vault's LIST semantics return 404 for a truly
    empty collection rather than an empty list. Both cases mean "this role
    does not exist yet" here, since `_ensure_kubernetes_auth_method` always
    runs first.

    Drift correction (plan 05-03): the ORIGINAL shape of this function only
    ever checked "does a role named `name` exist" and skipped
    unconditionally if so -- which meant a role's `bound_service_account_
    names` could never actually be corrected by re-running this idempotent
    bootstrap once the role already existed. Plan 05-03's whole point is
    correcting the `airflow` role's plan-05-01-era best guess
    (`["airflow-api-server"]`) to the empirically observed set, so this now
    reads the EXISTING role's live binding and re-writes it (Vault's own
    `create_role` on an existing name is a full replace, not a merge) only
    when it differs from what the caller now passes. A role whose binding
    already matches its target performs zero writes, unchanged from
    before -- this never affects the `csv-processor` role, whose binding is
    FINAL and never drifts.
    """
    try:
        existing = client.auth.kubernetes.list_roles()
        existing_names = existing["keys"] if existing else []
    except hvac.exceptions.InvalidPath:
        existing_names = []

    already_exists = name in existing_names
    if already_exists:
        # `read_role()` returns the role's data UNWRAPPED (no top-level
        # "data" envelope) -- verified live, this plan: unlike several raw
        # `hvac.Client.<module>` calls elsewhere in this file (e.g.
        # `sys.list_mounted_secrets_engines()["data"]`), this particular
        # `auth.kubernetes` wrapper method already unwraps Vault's response
        # for the caller.
        current = client.auth.kubernetes.read_role(name=name)
        current_sas = sorted(current.get("bound_service_account_names") or [])
        target_sas = sorted(bound_service_account_names)
        if current_sas == target_sas:
            print(f"role {name}: already present")
            return
        print(
            f"role {name}: bound_service_account_names drifted "
            f"{current_sas} -> {target_sas} -- correcting",
        )

    client.auth.kubernetes.create_role(
        name=name,
        bound_service_account_names=bound_service_account_names,
        bound_service_account_namespaces=bound_service_account_namespaces,
        policies=policies,
        ttl="20m",
        max_ttl="1h",
    )
    print(f"role {name}: {'updated' if already_exists else 'created'}")


def _ensure_audit_device(client: hvac.Client) -> None:
    """(g) Enable a persistent `file` audit device, if not already enabled."""
    existing = client.sys.list_enabled_audit_devices()["data"]
    if "file/" in existing:
        print("audit device file/: already present")
        return
    client.sys.enable_audit_device(device_type="file", options={"file_path": _AUDIT_DEVICE_PATH})
    print("audit device file/: created")


def _kubectl_get_secret_field(kubectl_context: str, *, namespace: str, name: str, key: str) -> str:
    """Read one base64-decoded field from a live Kubernetes Secret.

    Same read mechanism `scripts/etl-secrets.sh`'s own
    `_read_minio_app_secret_key` uses -- `kubectl get secret ... -o
    jsonpath=...` piped through a base64 decode -- reimplemented here via
    `subprocess.run` rather than shelling out to that script, since this is
    the one place D-01's migration-source credentials (the `etl` namespace's
    two dev credentials, plan 05-02; `airflow-minio-connection`, plan 05-03)
    are read to populate Vault. Never prints the decoded value.

    Args:
        kubectl_context: The kubectl context to read through.
        namespace: The Secret's namespace.
        name: The Secret's name.
        key: The Secret's `.data` key to read.

    Returns:
        The field's decoded value.

    Raises:
        RuntimeError: The `kubectl get` call fails (e.g. the Secret or key
            does not exist).
    """
    kubectl_bin = _require_kubectl()
    proc = subprocess.run(  # noqa: S603
        [
            kubectl_bin,
            "--context",
            kubectl_context,
            "get",
            "secret",
            "-n",
            namespace,
            name,
            "-o",
            f"jsonpath={{.data.{key}}}",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    if proc.returncode != 0:
        msg = f"kubectl get secret -n {namespace} {name} (field {key!r}) failed: {proc.stderr}"
        raise RuntimeError(msg)
    return base64.b64decode(proc.stdout).decode("utf-8")


def _ensure_etl_secrets(client: hvac.Client, kubectl_context: str) -> None:
    """(h) Populate `etl/analytics-db` and `etl/minio`'s KV secret VALUES, if not already present.

    Guard: attempt `read_secret_version` first; a successful read means the
    secret is already present (skip, unchanged). On `hvac.exceptions.
    InvalidPath` (Vault's 404-shaped "not found"), source the value from the
    live `csv-processor-db`/`csv-processor-s3` Kubernetes Secrets
    `scripts/etl-secrets.sh` already created (Phase 4) and write it. Never
    prints either value.

    Args:
        client: An `hvac.Client` authenticated with the root token.
        kubectl_context: The kubectl context to read the source Secrets
            through.
    """
    try:
        client.secrets.kv.v2.read_secret_version(mount_point="etl", path="analytics-db")
        print("secret etl/analytics-db: already present")
    except hvac.exceptions.InvalidPath:
        dsn = _kubectl_get_secret_field(
            kubectl_context,
            namespace=_ETL_NAMESPACE,
            name=_DB_SECRET_NAME,
            key="dsn",
        )
        client.secrets.kv.v2.create_or_update_secret(
            mount_point="etl",
            path="analytics-db",
            secret={"dsn": dsn},
        )
        print("secret etl/analytics-db: created")

    try:
        client.secrets.kv.v2.read_secret_version(mount_point="etl", path="minio")
        print("secret etl/minio: already present")
    except hvac.exceptions.InvalidPath:
        secret_key = _kubectl_get_secret_field(
            kubectl_context,
            namespace=_ETL_NAMESPACE,
            name=_S3_SECRET_NAME,
            key="secret_key",
        )
        client.secrets.kv.v2.create_or_update_secret(
            mount_point="etl",
            path="minio",
            secret={"access_key": _MINIO_APP_ACCESS_KEY, "secret_key": secret_key},
        )
        print("secret etl/minio: created")


def _ensure_airflow_secrets(client: hvac.Client, kubectl_context: str) -> None:
    """(i) Populate `airflow/connections/minio_default`'s KV secret VALUE, if not already present.

    Same read-then-skip-or-write shape as `_ensure_etl_secrets`. Guard:
    attempt `read_secret_version` first; a successful read means the secret
    is already present (skip, unchanged). On `hvac.exceptions.InvalidPath`
    (Vault's 404-shaped "not found"), source the value from the live
    `airflow-minio-connection` Kubernetes Secret `scripts/etl-secrets.sh`
    already created (Phase 4) and write it under the field name `conn_uri`
    -- matching `providers-hashicorp`'s own `VaultBackend.get_connection`
    convention (`response.get("conn_uri")`, verified against the installed
    provider's source: it prioritizes `conn_uri` if present, falling back
    to `Connection(conn_id, **response)` otherwise). Never prints the
    value.

    Args:
        client: An `hvac.Client` authenticated with the root token.
        kubectl_context: The kubectl context to read the source Secret
            through.
    """
    try:
        client.secrets.kv.v2.read_secret_version(
            mount_point="airflow",
            path="connections/minio_default",
        )
        print("secret airflow/connections/minio_default: already present")
    except hvac.exceptions.InvalidPath:
        conn_uri = _kubectl_get_secret_field(
            kubectl_context,
            namespace=_AIRFLOW_NAMESPACE,
            name=_AIRFLOW_MINIO_SECRET_NAME,
            key="AIRFLOW_CONN_MINIO_DEFAULT",
        )
        client.secrets.kv.v2.create_or_update_secret(
            mount_point="airflow",
            path="connections/minio_default",
            secret={"conn_uri": conn_uri},
        )
        print("secret airflow/connections/minio_default: created")


def bootstrap(client: hvac.Client, kubectl_context: str) -> None:
    """Run every idempotent bootstrap step against an authenticated, unsealed `client`.

    Args:
        client: An `hvac.Client` authenticated with the root token.
        kubectl_context: The kubectl context `_ensure_etl_secrets` reads the
            source Kubernetes Secrets through.

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
    # EMPIRICALLY CORRECTED (plan 05-03) from plan 05-01's original
    # single-SA best guess (05-RESEARCH.md Pitfall 1: "API server is the
    # sole metadata-DB access point for tasks and workers" turned out to be
    # suggestive, not confirmed, for secrets-backend lookups). Four
    # ServiceAccounts actually need to authenticate once every
    # AIRFLOW_CONN_MINIO_DEFAULT secretKeyRef fallback is removed -- each
    # justified by name, by its own evidence class, never widened to a
    # wildcard or "every candidate" as a shortcut (T-05-01):
    #   - airflow-api-server: LIVE-OBSERVED via the Vault audit log
    #     (`kubectl exec -i -n vault vault-0 -- tail ... /vault/audit/
    #     audit.log`, `auth/kubernetes/login` success,
    #     service_account_name=airflow-api-server) immediately after
    #     `airflow connections get minio_default` / `airflow config
    #     get-value secrets backend*` -- this SA alone resolved the
    #     connection with no DB row (confirmed 0 rows) and no env var on
    #     its own pod, so plan 05-01's original guess is CONFIRMED, not
    #     just carried forward.
    #   - airflow-triggerer: LIVE-OBSERVED by directly invoking
    #     VaultBackend.get_connection("minio_default") inside the running
    #     triggerer pod, using its own projected SA token -- this is the
    #     identity Airflow's deferred-trigger resume path uses (the
    #     triggerer process re-polls S3KeySensor.poke() itself, never
    #     proxied through the API server). The FAILURE this fix corrects
    #     was reproduced first, under the plan-05-01-era binding: "Forbidden
    #     service account name not authorized."
    #   - airflow-worker: confirmed by reading the ACTUALLY INSTALLED
    #     apache-airflow-providers-amazon S3KeySensor.execute() source live
    #     (not assumed): `if not self.poke(context=context): self._defer()`
    #     -- the KubernetesExecutor task-instance pod (ServiceAccount
    #     `airflow-worker`, per this chart's own pod template) performs ONE
    #     synchronous poke, resolving minio_default itself, BEFORE ever
    #     deferring to the triggerer. Independently corroborated by this
    #     plan's own live end-to-end DAG trigger
    #     (tests/e2e/vault/test_airflow_backend.py).
    #   - airflow-scheduler: the one addition NOT live-observed on today's
    #     cluster (PROFILE=local runs KubernetesExecutor, under which the
    #     scheduler never executes task code itself) -- included on
    #     documented architectural necessity instead: this repo's own
    #     helm/values/{local,ci}/airflow.yaml already established (plan
    #     04-02, D-01) that CI's LocalExecutor profile runs task code
    #     in-process inside the scheduler, "regardless of executor, for
    #     consistency" -- and this same plan removes CI's own
    #     scheduler.env fallback in the same change, so the scheduler's
    #     identity must be able to authenticate once that fallback is gone,
    #     or CI regresses the next time it runs under LocalExecutor.
    #   - airflow-dag-processor is DELIBERATELY EXCLUDED: it only parses
    #     and serializes DAG files, never executes task or trigger code,
    #     and has never carried this connection via any delivery mechanism
    #     (no secretKeyRef block for it ever existed) -- no code path in it
    #     ever calls BaseHook.get_connection.
    _ensure_kubernetes_role(
        client,
        name="airflow",
        bound_service_account_names=[
            "airflow-api-server",
            "airflow-triggerer",
            "airflow-worker",
            "airflow-scheduler",
        ],
        bound_service_account_namespaces=["airflow"],
        policies=["airflow"],
    )

    _ensure_audit_device(client)

    _ensure_etl_secrets(client, kubectl_context)
    _ensure_airflow_secrets(client, kubectl_context)


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
            bootstrap(client, kubectl_context)
    except (RuntimeError, hvac.exceptions.VaultError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
