#!/usr/bin/env python3
r"""scripts/vault-bootstrap.py -- idempotent hvac-based Vault admin bootstrap.

`make vault-bootstrap` runs this against an already-UNSEALED Vault (run
`make vault-unseal` first -- this script FAILS FAST on a sealed Vault rather
than silently unsealing it itself; unseal is D-02's own deliberate, separate
step). It creates, if not already present:

  (a) KV v2 mounts `etl`, `airflow` and `grafana`
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
  (h) the two `etl` KV secret VALUES (plan 05-02, retargeted plan 05-06) --
      `etl/analytics-db` (`dsn`) and `etl/minio` (`access_key`/
      `secret_key`). `etl/analytics-db`'s DSN is generated FRESH on every
      first-ever bootstrap of an empty Vault -- a `kubectl exec`-driven
      `ALTER ROLE etl_app WITH PASSWORD ...` against the CNPG
      `analytics-db` Cluster's own current primary pod, restoring the
      mechanism the now-deleted `scripts/etl-secrets.sh` originally used
      (git history only, `git show 6d86cb8:scripts/etl-secrets.sh`) -- see
      `_ensure_etl_secrets`. `etl/minio`'s value is read from the live
      `data/minio-app` Kubernetes Secret
  (i) the `airflow/connections/minio_default` KV secret VALUE (plan 05-03,
      retargeted plan 05-06) -- field name `conn_uri`, matching
      `providers-hashicorp`'s own `VaultBackend.get_connection` convention
      (`response.get("conn_uri")`, verified against the installed
      provider's source: it prioritizes `conn_uri` if present, falling
      back to `Connection(conn_id, **response)` otherwise) -- assembled
      from the same live `data/minio-app` Kubernetes Secret `etl/minio`
      reads (see `_ensure_airflow_secrets`)
  (j) the `grafana` KV secret VALUES and the `grafana-alert-webhook`
      Kubernetes Secret in namespace `monitoring` (plan 07-06) --
      `grafana/analytics-db` (`password`, from a fresh `kubectl exec`-driven
      `ALTER ROLE grafana_reader WITH PASSWORD ...`, the same mechanism (h)
      uses for `etl_app`) and `grafana/alert-webhook` (`url`, read from the
      operator-provisioned `.secrets/grafana-webhook-url` file). Grafana has
      no Vault client at all (unlike Airflow's native `VaultBackend` or
      `hvac`-in-pod for ETL), so this step also materializes both values as
      a Kubernetes Secret Grafana's Helm values reference by name -- see
      `_ensure_grafana_secrets`

Every step is idempotent: re-running this script against an already-
bootstrapped Vault performs zero writes and prints "already present" for
each step whose live state already matches its target. This mirrors the
now-deleted `scripts/etl-secrets.sh`'s own `_secret_exists`-before-
`_apply_secret` shape (git history only) -- read first, write only if
missing. One step, (f), also self-corrects DRIFT (not just absence): a
kubernetes-auth role whose live `bound_service_account_names` no longer
matches this script's own target list is RE-WRITTEN, not skipped -- this
is the exact mechanism plan 05-03 uses to correct plan 05-01's original
best guess in place, on a Vault that was already bootstrapped once. Steps
(c) and (e) are held to the same standard as of plan 05-06 (CR-02):
`_ensure_policy` now re-applies a policy whose live body has drifted from
its target HCL, not only a policy that was entirely absent.

The root token, read once from `.secrets/vault-init.json`, is the ONLY
place in this codebase that value is ever read. It is never logged,
printed, or passed to a subprocess as an argument. No step here ever
prints a secret value, token, or key -- including the `etl_app` password
`_ensure_etl_secrets` now generates and the DSN it assembles from it.
"""

from __future__ import annotations

import base64
import contextlib
import json
import secrets
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import quote

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

# (h)/(i) The live Kubernetes Secret and CNPG `Cluster` these two steps
# source from (plan 05-06) -- see `_ensure_etl_secrets`/
# `_ensure_airflow_secrets` for the full mechanism, restored from git
# history (`git show 6d86cb8:scripts/etl-secrets.sh`, the ORIGINAL,
# pre-deletion version of the script plans 05-02/05-03 later deleted once
# its Secrets migrated to Vault). Never printed. `_MINIO_APP_ACCESS_KEY` is
# the same fixed, non-secret literal `scripts/minio-credentials.sh`'s own
# `cmd_show` hardcodes.
_DATA_NAMESPACE = "data"
_ANALYTICS_CLUSTER = "analytics-db"
_ANALYTICS_DATABASE = "analytics"
_ANALYTICS_APP_ROLE = "etl_app"
_MINIO_APP_SECRET_NAME = "minio-app"  # noqa: S105 -- a K8s Secret's `metadata.name`, not a credential
_MINIO_APP_ACCESS_KEY = "etl-app"

# (j) Grafana (plan 07-06) -- a third Vault-consumer shape, distinct from
# both of Phase 5's tiers (Airflow's native VaultBackend; hvac-in-pod for
# ETL): Grafana's own container has no Vault client at all, so its two
# credentials materialize into a Kubernetes Secret instead
# (07-RESEARCH.md Pattern 5). `_GRAFANA_READER_ROLE` is a SIBLING constant
# to `_ANALYTICS_APP_ROLE`, not a reuse of it -- `grafana_reader` (migration
# 0011) is a separate, SELECT-only role.
_MONITORING_NAMESPACE = "monitoring"
_GRAFANA_READER_ROLE = "grafana_reader"
_GRAFANA_SECRET_NAME = "grafana-alert-webhook"  # noqa: S105 -- a K8s Secret's `metadata.name`
_GRAFANA_WEBHOOK_FILE = _REPO_ROOT / ".secrets" / "grafana-webhook-url"


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
    """(a) Enable KV v2 mounts `etl`, `airflow` and `grafana`, if not already mounted.

    `grafana` (plan 07-06) is a Vault-write TARGET, not a Vault-read
    consumer -- Grafana itself has no Vault client (07-RESEARCH.md
    Pattern 5) -- but `create_or_update_secret(mount_point="grafana", ...)`
    still requires the mount to exist first: verified empirically against a
    live Vault, writing to an unmounted prefix raises the same
    `hvac.exceptions.InvalidPath` a missing PATH does, so without this the
    mount itself (not just the path) would silently need creating on every
    first-ever `_ensure_grafana_secrets` call.
    """
    existing = client.sys.list_mounted_secrets_engines()["data"]
    for mount in ("etl", "airflow", "grafana"):
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
    """(c)/(e) Write a policy if absent, or re-apply it if its live body has drifted.

    CR-02 fix (05-REVIEW.md, plan 05-06): the original shape of this
    function returned unconditionally once `name` was merely present in
    `list_policies()`, so a policy body edited in this file (e.g.
    narrowing access after discovering a mistake) was never re-applied on
    a later idempotent bootstrap run against an already-bootstrapped
    Vault -- silently staying at its old, possibly-wider body forever
    (T-05-15). This now reads the live body back via `read_policy()` and
    compares it against `policy_hcl` (both `.strip()`-ed) before deciding
    to skip -- the same "read first, re-write only on drift" convergence
    shape `_ensure_kubernetes_role` already established for role bindings.

    `read_policy()` calls the legacy `GET /v1/sys/policy/{name}` endpoint.
    Its response IS wrapped in a `data` envelope, keyed `rules` (never
    `policy`, and never a flat top-level key) -- confirmed by reading the
    installed hvac 2.4.0 source directly rather than guessing: `hvac/v1/
    __init__.py`'s own `Client.get_policy()` convenience wrapper does
    exactly `self.sys.read_policy(name=name)["data"]["rules"]`.

    Args:
        client: An `hvac.Client` authenticated with the root token.
        name: The policy's name.
        policy_hcl: The target policy body (HCL).
    """
    existing = client.sys.list_policies()["data"]["policies"]
    already_exists = name in existing
    if already_exists:
        live_body = client.sys.read_policy(name=name)["data"]["rules"]
        if live_body.strip() == policy_hcl.strip():
            print(f"policy {name}: already present")
            return
        print(f"policy {name}: body drifted -- correcting")

    client.sys.create_or_update_policy(name=name, policy=policy_hcl)
    print(f"policy {name}: {'updated' if already_exists else 'created'}")


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

    Same read mechanism the now-deleted `scripts/etl-secrets.sh`'s own
    `_read_minio_app_secret_key` used (git history only, `git show
    6d86cb8:scripts/etl-secrets.sh`) -- `kubectl get secret ... -o
    jsonpath=...` piped through a base64 decode -- reimplemented here via
    `subprocess.run` rather than shelling out to that script. This is the
    one place the live `data/minio-app` Kubernetes Secret is read to
    populate `etl/minio` and `airflow/connections/minio_default` (plan
    05-06). Never prints the decoded value.

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


def _kubectl_cluster_primary_pod(kubectl_context: str, *, namespace: str, cluster: str) -> str:
    """Resolve a CNPG `Cluster`'s current primary pod name.

    Restores the exact resolution step the now-deleted `scripts/
    etl-secrets.sh`'s own `_ensure_csv_processor_db_secret` performed (git
    history only, `git show 6d86cb8:scripts/etl-secrets.sh`), reimplemented
    via `subprocess.run` in the same shape as `_kubectl_get_secret_field`.

    Args:
        kubectl_context: The kubectl context to read through.
        namespace: The `Cluster` resource's namespace.
        cluster: The `Cluster` resource's name.

    Returns:
        The current primary pod's name.

    Raises:
        RuntimeError: The `kubectl get` call fails, or the Cluster has no
            `.status.currentPrimary` yet.
    """
    kubectl_bin = _require_kubectl()
    proc = subprocess.run(  # noqa: S603
        [
            kubectl_bin,
            "--context",
            kubectl_context,
            "get",
            "cluster",
            "-n",
            namespace,
            cluster,
            "-o",
            "jsonpath={.status.currentPrimary}",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    if proc.returncode != 0:
        msg = f"kubectl get cluster -n {namespace} {cluster} (currentPrimary) failed: {proc.stderr}"
        raise RuntimeError(msg)
    primary_pod = proc.stdout.strip()
    if not primary_pod:
        msg = f"Cluster/{cluster} (namespace {namespace}) has no currentPrimary yet"
        raise RuntimeError(msg)
    return primary_pod


def _kubectl_exec_psql(
    kubectl_context: str,
    *,
    namespace: str,
    pod: str,
    database: str,
    sql: str,
) -> None:
    """Run `sql` against `database` inside `pod` via `kubectl exec ... psql`, stdin only.

    `sql` is passed via `input=` (stdin), never as an argv element, so it
    never appears in `ps`/`/proc/<pid>/cmdline` -- restoring the now-deleted
    `scripts/etl-secrets.sh`'s own identical `kubectl exec -i ... psql`
    pattern (git history only, `git show 6d86cb8:scripts/etl-secrets.sh`).
    `-v ON_ERROR_STOP=1` makes a SQL error a non-zero exit rather than a
    silently-ignored one.

    Args:
        kubectl_context: The kubectl context to exec through.
        namespace: The pod's namespace.
        pod: The pod name to exec into.
        database: The `-d` database name `psql` connects to.
        sql: The SQL text piped to `psql` on stdin.

    Raises:
        RuntimeError: The `psql` exec fails. The error message includes
            only `proc.stderr` -- psql does not echo stdin SQL to stderr on
            an `ON_ERROR_STOP` failure, so this cannot leak `sql`'s
            contents.
    """
    kubectl_bin = _require_kubectl()
    proc = subprocess.run(  # noqa: S603
        [
            kubectl_bin,
            "--context",
            kubectl_context,
            "exec",
            "-i",
            "-n",
            namespace,
            pod,
            "--",
            "psql",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            "postgres",
            "-d",
            database,
        ],
        input=sql,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    if proc.returncode != 0:
        msg = f"kubectl exec -n {namespace} {pod} -- psql -d {database} failed: {proc.stderr}"
        raise RuntimeError(msg)


def _ensure_etl_secrets(client: hvac.Client, kubectl_context: str) -> None:
    """(h) Populate `etl/analytics-db` and `etl/minio`'s KV secret VALUES, if not already present.

    Guard: attempt `read_secret_version` first; a successful read means the
    secret is already present (skip, unchanged -- a credential already
    handed to a caller must never be silently rotated out from under it).
    On `hvac.exceptions.InvalidPath` (Vault's 404-shaped "not found"):

    - `etl/analytics-db`: regenerate `etl_app`'s PostgreSQL password ON THE
      SPOT, via `kubectl exec` into the CNPG `analytics-db` Cluster's
      current primary pod (peer/local trust, the developer's own
      kubeconfig context) running `ALTER ROLE etl_app WITH PASSWORD
      '<fresh value>';`, then assemble and write the resulting DSN. This
      restores the exact mechanism the now-deleted `scripts/etl-secrets.sh`'s
      own `_ensure_csv_processor_db_secret` used (git history only, `git
      show 6d86cb8:scripts/etl-secrets.sh`) -- retargeted to write straight
      to Vault KV instead of staging through the now-deleted, phase-4-era
      Kubernetes Secret this exact DSN previously lived in. Deliberately
      NOT sourced from CNPG's own auto-generated `analytics-db-app` Secret
      (05-REVIEW.md CR-01's literal suggestion) -- that Secret holds
      `analytics_owner`, a DIFFERENT, MORE PRIVILEGED role than `etl_app`
      (`helm/values/local/cnpg-analytics.yaml`'s `initdb.owner`), and using
      it would silently widen the ETL pipeline's database privilege level
      (T-05-17).
    - `etl/minio`: read the live `data/minio-app` Secret's `secretKey`
      field (never a Secret staged specifically for this one purpose, as
      plan 05-02's original design used).

    Never prints either value.

    Args:
        client: An `hvac.Client` authenticated with the root token.
        kubectl_context: The kubectl context to read/exec through.
    """
    try:
        client.secrets.kv.v2.read_secret_version(mount_point="etl", path="analytics-db")
        print("secret etl/analytics-db: already present")
    except hvac.exceptions.InvalidPath:
        primary_pod = _kubectl_cluster_primary_pod(
            kubectl_context,
            namespace=_DATA_NAMESPACE,
            cluster=_ANALYTICS_CLUSTER,
        )
        # secrets.token_hex's charset is pure [0-9a-f] -- it cannot contain
        # the `'` character the SQL string literal below depends on, so
        # this is not an injection vector (T-05-13). Documented explicitly
        # so a future charset change (e.g. switching to token_urlsafe)
        # cannot silently reopen it.
        password = secrets.token_hex(32)
        _kubectl_exec_psql(
            kubectl_context,
            namespace=_DATA_NAMESPACE,
            pod=primary_pod,
            database=_ANALYTICS_DATABASE,
            sql=f"ALTER ROLE {_ANALYTICS_APP_ROLE} WITH PASSWORD '{password}';",
        )
        encoded_password = quote(password, safe="")
        dsn = (
            f"postgresql://{_ANALYTICS_APP_ROLE}:{encoded_password}"
            f"@{_ANALYTICS_CLUSTER}-rw.{_DATA_NAMESPACE}:5432/{_ANALYTICS_DATABASE}"
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
            namespace=_DATA_NAMESPACE,
            name=_MINIO_APP_SECRET_NAME,
            key="secretKey",
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
    (Vault's 404-shaped "not found"), read the live `data/minio-app`
    Secret's `secretKey` field -- the same source `_ensure_etl_secrets`
    reads for `etl/minio`, never a pre-built connection-URI Secret staged
    specifically for this one purpose, as plan 05-03's original design
    used -- and ASSEMBLE the URI exactly as the now-deleted `scripts/
    etl-secrets.sh`'s own `_ensure_airflow_minio_connection_secret` did
    (git history only, `git show 6d86cb8:scripts/etl-secrets.sh`), then
    write it under the field name `conn_uri` -- matching
    `providers-hashicorp`'s own `VaultBackend.get_connection` convention
    (`response.get("conn_uri")`, verified against the installed provider's
    source: it prioritizes `conn_uri` if present, falling back to
    `Connection(conn_id, **response)` otherwise). Never prints the value.

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
        secret_key = _kubectl_get_secret_field(
            kubectl_context,
            namespace=_DATA_NAMESPACE,
            name=_MINIO_APP_SECRET_NAME,
            key="secretKey",
        )
        encoded_secret_key = quote(secret_key, safe="")
        conn_uri = (
            f"aws://{_MINIO_APP_ACCESS_KEY}:{encoded_secret_key}@/"
            "?endpoint_url=http%3A%2F%2Fminio.data.svc.cluster.local%3A9000"
            "&region_name=us-east-1"
        )
        client.secrets.kv.v2.create_or_update_secret(
            mount_point="airflow",
            path="connections/minio_default",
            secret={"conn_uri": conn_uri},
        )
        print("secret airflow/connections/minio_default: created")


def _apply_kubernetes_secret(
    kubectl_context: str,
    *,
    namespace: str,
    name: str,
    string_data: dict[str, str],
) -> None:
    """Apply a `type: Opaque` Kubernetes Secret via `kubectl apply -f -`, stdin only.

    Python translation of `scripts/airflow-metadata-secret.sh`'s own
    `_apply_secret` shape (plan 07-06's own Interfaces section names this
    exact mapping): the manifest text -- which contains every value in
    `string_data` -- crosses only via `input=` (stdin), never as an argv
    element (T-05-13), the same discipline `_kubectl_exec_psql` already
    applies to its own `sql` parameter.

    Unlike `airflow-metadata-secret.sh`'s own raw `printf` interpolation
    (safe there only because every value it writes is
    already pre-encoded -- URL-quoted, hex or base64, none of which can
    contain a YAML-significant character), this function's `string_data`
    values are NOT guaranteed pre-encoded: an operator-supplied webhook URL
    (`_ensure_grafana_secrets`) is arbitrary text that could contain `#`,
    `:`, leading/trailing whitespace or other YAML-significant characters.
    Each value is therefore emitted via `json.dumps()` -- a valid JSON
    string literal is also a valid YAML flow scalar, so this safely quotes
    and escapes without pulling in a full YAML-serialization dependency.

    Args:
        kubectl_context: The kubectl context to apply through.
        namespace: The Secret's namespace.
        name: The Secret's name.
        string_data: The Secret's `stringData` key/value pairs. Never
            logged or printed by this function.

    Raises:
        RuntimeError: The `kubectl apply` call fails.
    """
    kubectl_bin = _require_kubectl()
    lines = [
        "apiVersion: v1",
        "kind: Secret",
        "metadata:",
        f"  name: {name}",
        f"  namespace: {namespace}",
        "type: Opaque",
        "stringData:",
    ]
    lines.extend(f"  {key}: {json.dumps(value)}" for key, value in string_data.items())
    manifest = "\n".join(lines) + "\n"

    proc = subprocess.run(  # noqa: S603
        [kubectl_bin, "--context", kubectl_context, "apply", "-f", "-"],
        input=manifest,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    if proc.returncode != 0:
        msg = f"kubectl apply Secret {namespace}/{name} failed: {proc.stderr}"
        raise RuntimeError(msg)


def _ensure_grafana_secrets(client: hvac.Client, kubectl_context: str) -> None:
    """(j) Populate `grafana/analytics-db` and `grafana/alert-webhook`, materialize their Secret.

    Grafana (unlike Airflow's native `VaultBackend` or `hvac`-in-pod for ETL
    -- Phase 5's only two established Vault-consumer tiers) has no Vault
    client at all, so its two credentials must land in a Kubernetes Secret
    instead of being read directly at runtime (07-RESEARCH.md Pattern 5).

    Same read-then-skip-or-write shape as `_ensure_etl_secrets` for each of
    the two Vault paths:

    - `grafana/analytics-db`: on `InvalidPath`, generate a fresh
      `grafana_reader` PostgreSQL password via `secrets.token_hex(32)`,
      `ALTER ROLE grafana_reader WITH PASSWORD ...` against the analytical
      cluster's current primary pod (the identical `kubectl exec` mechanism
      `_ensure_etl_secrets` already uses for `etl_app`), then write
      `{"password": password}` to Vault KV.
    - `grafana/alert-webhook`: on `InvalidPath`, read the webhook URL from
      `_GRAFANA_WEBHOOK_FILE` if it exists, else raise a `RuntimeError`
      naming the exact path to create -- this function never invents or
      guesses a webhook destination (the operator's `user_setup` step,
      07-06-PLAN.md). Write `{"url": webhook_url}` to Vault KV.

    Unconditionally (whether either path above was just created or already
    existed): read both values back from Vault KV and apply the
    `grafana-alert-webhook` Kubernetes Secret in namespace `monitoring`
    (keys `GRAFANA_DB_PASSWORD`/`GRAFANA_ALERT_WEBHOOK_URL`) via
    `_apply_kubernetes_secret`. This step is NOT guarded by a
    "was anything just created" check: the Secret itself can independently
    go missing (e.g. after a `cluster-down`/`cluster-up` that wipes
    Kubernetes state but not Vault's PVC-backed KV data) even when both
    Vault paths are already long-lived -- `kubectl apply` on an unchanged
    manifest is itself an idempotent no-op PATCH.

    Never prints either value.

    Args:
        client: An `hvac.Client` authenticated with the root token.
        kubectl_context: The kubectl context to read/exec/apply through.

    Raises:
        RuntimeError: `grafana/alert-webhook` does not exist yet in Vault
            AND `_GRAFANA_WEBHOOK_FILE` does not exist on disk either.
    """
    try:
        client.secrets.kv.v2.read_secret_version(mount_point="grafana", path="analytics-db")
        print("secret grafana/analytics-db: already present")
    except hvac.exceptions.InvalidPath:
        primary_pod = _kubectl_cluster_primary_pod(
            kubectl_context,
            namespace=_DATA_NAMESPACE,
            cluster=_ANALYTICS_CLUSTER,
        )
        # secrets.token_hex's charset is pure [0-9a-f] -- it cannot contain
        # the `'` character the SQL string literal below depends on, so
        # this is not an injection vector (T-05-13), the same reasoning
        # _ensure_etl_secrets documents for etl_app's own password.
        password = secrets.token_hex(32)
        _kubectl_exec_psql(
            kubectl_context,
            namespace=_DATA_NAMESPACE,
            pod=primary_pod,
            database=_ANALYTICS_DATABASE,
            sql=f"ALTER ROLE {_GRAFANA_READER_ROLE} WITH PASSWORD '{password}';",
        )
        client.secrets.kv.v2.create_or_update_secret(
            mount_point="grafana",
            path="analytics-db",
            secret={"password": password},
        )
        print("secret grafana/analytics-db: created")

    try:
        client.secrets.kv.v2.read_secret_version(mount_point="grafana", path="alert-webhook")
        print("secret grafana/alert-webhook: already present")
    except hvac.exceptions.InvalidPath:
        if not _GRAFANA_WEBHOOK_FILE.is_file():
            msg = (
                f"{_GRAFANA_WEBHOOK_FILE} does not exist -- create it, containing the "
                "Grafana alert webhook URL as a single line of plain text, then re-run "
                "`make vault-bootstrap`"
            )
            raise RuntimeError(msg) from None
        webhook_url = _GRAFANA_WEBHOOK_FILE.read_text(encoding="utf-8").strip()
        client.secrets.kv.v2.create_or_update_secret(
            mount_point="grafana",
            path="alert-webhook",
            secret={"url": webhook_url},
        )
        print("secret grafana/alert-webhook: created")

    password = client.secrets.kv.v2.read_secret_version(
        mount_point="grafana",
        path="analytics-db",
    )["data"]["data"]["password"]
    webhook_url = client.secrets.kv.v2.read_secret_version(
        mount_point="grafana",
        path="alert-webhook",
    )["data"]["data"]["url"]
    _apply_kubernetes_secret(
        kubectl_context,
        namespace=_MONITORING_NAMESPACE,
        name=_GRAFANA_SECRET_NAME,
        string_data={
            "GRAFANA_DB_PASSWORD": password,
            "GRAFANA_ALERT_WEBHOOK_URL": webhook_url,
        },
    )
    print(f"Secret {_MONITORING_NAMESPACE}/{_GRAFANA_SECRET_NAME}: applied")


def bootstrap(client: hvac.Client, kubectl_context: str) -> None:
    """Run every idempotent bootstrap step against an authenticated, unsealed `client`.

    Args:
        client: An `hvac.Client` authenticated with the root token.
        kubectl_context: The kubectl context `_ensure_etl_secrets`/
            `_ensure_airflow_secrets` read Secrets and exec SQL through.

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
    _ensure_grafana_secrets(client, kubectl_context)


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
