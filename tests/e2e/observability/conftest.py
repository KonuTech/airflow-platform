"""Shared fixtures for tests/e2e/observability/ — the live Grafana provisioning harness.

`repo_root`, `cluster_name`, `kubectl_context`, `_require_cluster`, `kubectl`,
`kubectl_json` and `s3_client` are imported directly from
`tests/e2e/cluster/conftest.py` — this repository's established convention
for these specific, already-stable, non-underscore-prefixed fixtures (see
`tests/e2e/slice/conftest.py`'s own docstring, which does the same thing for
the same reason).

Everything else here is a DELIBERATE, MINIMAL duplication rather than an
import from `tests/e2e/slice/conftest.py` (07-07-PLAN.md Task 3's own
instruction: "Mirror ... this project's own established `tests/e2e/*/`
convention ... is that each package keeps its own copy of small
cross-cutting helpers rather than importing across directories"). This
package uses the `analytics_owner`-authenticated `analytics_connection` (never
`etl_app`'s Vault-authenticated role — the CNPG-generated `owner` role,
requiring no Vault dependency at all, is the simpler, sufficient choice for a
test harness) and the polling helpers that bridge "I uploaded a file" to "the
run reached SUCCEEDED"/"the run's trace was claimed". Through 07-07 this
suite only ever read through that connection; plan 07-08's Task 3 is the
first to also WRITE through it (a dedicated, never-`customers` test
dataset's `meta.datasets` row, forced then restored in a `finally` block) --
`analytics_owner` already has the privilege for this, so no new connection
tier was needed. `grafana_addr` is new to this module: a session-scoped
`kubectl port-forward` tunnel to `svc/monitoring-grafana` (namespace
`monitoring`, port 80), mirroring `tests/e2e/vault/conftest.py`'s own
`vault_addr` shape exactly. Plan 07-08 (Task 1) adds two more: `vault_root_client`
(the same duplicated shape, reusing this file's own `_port_forward` helper)
and `webhook_receiver`, a function-scoped fixture that deploys a throwaway
in-cluster HTTP receiver Pod+Service -- Grafana's Alerting engine runs
in-cluster and cannot reach a pytest-process-local listener, so a real
contact-point delivery proof (D-20) needs a real, cluster-reachable target.
"""

from __future__ import annotations

import base64
import contextlib
import json
import shutil
import socket
import subprocess
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

import hvac
import psycopg
import pytest
import requests

from tests.e2e.cluster.conftest import (  # noqa: F401 -- re-exported as pytest fixtures below
    _require_cluster,
    cluster_name,
    kubectl,
    kubectl_context,
    kubectl_json,
    repo_root,
    s3_client,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

_DATA_NAMESPACE = "data"
_ANALYTICS_CLUSTER = "analytics-db"
_MONITORING_NAMESPACE = "monitoring"
_GRAFANA_SERVICE = "monitoring-grafana"
_GRAFANA_PORT = 80
_GRAFANA_ADMIN_SECRET = "monitoring-grafana"  # noqa: S105 -- a K8s Secret's metadata.name

# Task 3 (plan 07-08): the same Vault tunnel shape tests/e2e/vault/conftest.py
# establishes, duplicated here per this directory's own established
# convention (module docstring above) -- reusing THIS file's own
# `_port_forward` helper rather than re-deriving the raw subprocess logic a
# second time.
_VAULT_NAMESPACE = "vault"
_VAULT_SERVICE = "vault"
_VAULT_PORT = 8200
_VAULT_INIT_FILE = Path(__file__).resolve().parents[3] / ".secrets" / "vault-init.json"

# Terminal statuses `dataplat.pipeline.run.run_ingest`/`csv_processor.cli.ingest`
# ever write to `meta.ingestion_runs.status` — copied from
# tests/e2e/slice/conftest.py's own constant of the same name (see module
# docstring: a deliberate, minimal duplication, not an import).
_TERMINAL_RUN_STATUSES = frozenset(
    {"SUCCEEDED", "FAILED", "SKIPPED_DUPLICATE", "SKIPPED_CONCURRENT"},
)

_POLL_INTERVAL_SECONDS = 0.5


def _free_local_port() -> int:
    """Ask the OS for an unused TCP port, then release it immediately."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextlib.contextmanager
def _port_forward(
    kubectl_context_value: str,
    *,
    namespace: str,
    service: str,
    remote_port: int,
) -> Iterator[int]:
    """Port-forward `svc/<service>` to a free local port for this test process only.

    Same shape as `tests/e2e/vault/conftest.py`'s `vault_addr` fixture body
    and `tests/e2e/slice/conftest.py`'s `_port_forwarded_analytics` —
    generalised here to an arbitrary namespace/service/port so this one
    helper covers both the Grafana and analytical-Postgres tunnels this
    package needs, rather than writing the same ~25 lines three times.
    """
    kubectl_bin = shutil.which("kubectl")
    assert kubectl_bin, "kubectl not found on PATH"

    local_port = _free_local_port()
    proc = subprocess.Popen(  # noqa: S603
        [
            kubectl_bin,
            "--context",
            kubectl_context_value,
            "-n",
            namespace,
            "port-forward",
            f"svc/{service}",
            f"{local_port}:{remote_port}",
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
                msg = f"kubectl port-forward for svc/{service} exited early:\n{output}"
                raise RuntimeError(msg)
            try:
                with socket.create_connection(("127.0.0.1", local_port), timeout=1):
                    connected = True
                    break
            except OSError:
                time.sleep(_POLL_INTERVAL_SECONDS)
        if not connected:
            msg = f"kubectl port-forward for svc/{service} never accepted a connection within 30s"
            raise RuntimeError(msg)
        yield local_port
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest.fixture(scope="session")
def grafana_addr(kubectl_context: str) -> Iterator[str]:  # noqa: F811 -- fixture-injection param name
    """Port-forward `svc/monitoring-grafana` for the whole test session.

    Yields:
        `"http://127.0.0.1:<local_port>"`.
    """
    with _port_forward(
        kubectl_context,
        namespace=_MONITORING_NAMESPACE,
        service=_GRAFANA_SERVICE,
        remote_port=_GRAFANA_PORT,
    ) as local_port:
        yield f"http://127.0.0.1:{local_port}"


@pytest.fixture(scope="session")
def grafana_admin_auth(
    kubectl_json: Callable[..., Any],  # noqa: F811 -- fixture-injection param name
) -> tuple[str, str]:
    """The chart's own default admin credential (never a new one this plan invents).

    Reads the `monitoring-grafana` Secret the kube-prometheus-stack chart
    itself creates (`admin-user`/`admin-password` keys) — verified via
    `helm template` this session (the Grafana Deployment's own `GF_SECURITY_
    ADMIN_USER`/`GF_SECURITY_ADMIN_PASSWORD` env vars source from exactly
    this Secret).

    Returns:
        `(username, password)`.
    """
    secret = kubectl_json("-n", _MONITORING_NAMESPACE, "get", "secret", _GRAFANA_ADMIN_SECRET)
    data = secret["data"]
    user = base64.b64decode(data["admin-user"]).decode("utf-8")
    password = base64.b64decode(data["admin-password"]).decode("utf-8")
    return user, password


@pytest.fixture
def grafana_api(
    grafana_addr: str,
    grafana_admin_auth: tuple[str, str],
) -> Callable[..., Any]:
    """A small `requests`-based Grafana REST API helper, pre-authenticated with HTTP basic auth.

    `grafana_api("GET", "/api/datasources")` / `grafana_api("POST",
    "/api/datasources/1/health")` -> parsed JSON. Raises with the response
    body attached on any non-2xx status, so a provisioning failure surfaces
    as a readable assertion message rather than a bare `HTTPError`.
    """
    user, password = grafana_admin_auth

    def _call(method: str, path: str, **kwargs: Any) -> Any:
        response = requests.request(
            method,
            f"{grafana_addr}{path}",
            auth=(user, password),
            timeout=30,
            **kwargs,
        )
        assert response.ok, (
            f"{method} {path} failed (status {response.status_code}): {response.text}"
        )
        return response.json()

    return _call


@pytest.fixture(scope="session")
def vault_addr(kubectl_context: str) -> Iterator[str]:  # noqa: F811 -- fixture-injection param name
    """Port-forward `svc/vault` (namespace `vault`, port 8200) for the whole test session.

    Duplicated from `tests/e2e/vault/conftest.py`'s own fixture of the same
    name, per this directory's own established convention (module
    docstring) -- reuses THIS file's own `_port_forward` helper rather than
    re-deriving the raw subprocess logic a second time.

    Yields:
        `"http://127.0.0.1:<local_port>"`.
    """
    with _port_forward(
        kubectl_context,
        namespace=_VAULT_NAMESPACE,
        service=_VAULT_SERVICE,
        remote_port=_VAULT_PORT,
    ) as local_port:
        yield f"http://127.0.0.1:{local_port}"


@pytest.fixture(scope="session")
def vault_root_client(vault_addr: str) -> hvac.Client:
    """An `hvac.Client` authenticated with the root token from `.secrets/vault-init.json`.

    Task 3 needs this to temporarily override the `grafana/alert-webhook`
    Vault secret for the duration of the live webhook-delivery proof (D-20),
    then restore it. Skips the whole module with a clear reason if that file
    does not exist yet -- Vault has not been bootstrapped on this checkout.
    """
    if not _VAULT_INIT_FILE.is_file():
        pytest.skip(
            f"{_VAULT_INIT_FILE} does not exist -- run `make vault-unseal && "
            "make vault-bootstrap` first",
        )
    root_token = json.loads(_VAULT_INIT_FILE.read_text(encoding="utf-8"))["root_token"]
    return hvac.Client(url=vault_addr, token=root_token)


_WEBHOOK_RECEIVER_HANDLER_SOURCE = """\
import http.server


class Handler(http.server.BaseHTTPRequestHandler):
    def _handle(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8", errors="replace")
        print(f"WEBHOOK_RECEIVED: {self.command} {self.path} {body}", flush=True)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    do_GET = _handle
    do_POST = _handle
    do_PUT = _handle

    def log_message(self, *args):
        pass


http.server.HTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
"""

_WEBHOOK_RECEIVER_START_TIMEOUT_SECONDS = 90


def _webhook_receiver_manifests(name: str, namespace: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build the throwaway receiver's Pod + Service manifests as plain dicts.

    `python:3.12-slim` (already this project's own base image family --
    `docker/csv-processor/Dockerfile`) with its entrypoint overridden
    (`command:`) to run one inline `http.server`-based handler -- no image
    build needed for a resource that lives for one test's duration.
    """
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {"app": name, "purpose": "e2e-webhook-receiver"},
        },
        "spec": {
            "containers": [
                {
                    "name": "receiver",
                    "image": "python:3.12-slim",
                    "command": ["python3", "-c", _WEBHOOK_RECEIVER_HANDLER_SOURCE],
                    "ports": [{"containerPort": 8080}],
                    "resources": {
                        "requests": {"cpu": "10m", "memory": "32Mi"},
                        "limits": {"cpu": "100m", "memory": "64Mi"},
                    },
                },
            ],
        },
    }
    service = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "type": "ClusterIP",
            "selector": {"app": name},
            "ports": [{"port": 8080, "targetPort": 8080}],
        },
    }
    return pod, service


def _wait_for_pod_running(
    kubectl_fn: Callable[..., subprocess.CompletedProcess[str]],
    *,
    namespace: str,
    name: str,
    timeout: float,
) -> None:
    """Poll a Pod's `status.phase` until `Running` -- never a fixed sleep."""
    deadline = time.monotonic() + timeout
    last_phase: str | None = None
    while time.monotonic() < deadline:
        proc = kubectl_fn("-n", namespace, "get", "pod", name, "-o", "jsonpath={.status.phase}")
        if proc.returncode == 0:
            last_phase = proc.stdout.strip() or None
            if last_phase == "Running":
                return
            if last_phase == "Failed":
                msg = (
                    f"webhook receiver pod {name!r} reached phase Failed while waiting for Running"
                )
                raise AssertionError(msg)
        time.sleep(_POLL_INTERVAL_SECONDS)
    msg = (
        f"webhook receiver pod {name!r} did not reach Running within {timeout}s "
        f"(last observed phase: {last_phase!r})"
    )
    raise AssertionError(msg)


@pytest.fixture
def webhook_receiver(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],  # noqa: F811 -- fixture-injection param name
    tmp_path: Path,
) -> Iterator[tuple[str, Callable[[], str | None]]]:
    """Deploy a throwaway in-cluster HTTP receiver Pod+Service for one test's duration.

    WHY THIS EXISTS: Grafana's Alerting engine runs entirely in-cluster, so a
    real contact-point delivery can never reach a pytest-process-local
    `localhost` listener -- RESEARCH.md's own Wave 0 Gaps section names this
    constraint explicitly. This fixture stands up a real, cluster-reachable
    target instead: a `python:3.12-slim` Pod running a one-file
    `http.server` handler (no image build needed) plus a matching ClusterIP
    Service, both named uniquely per invocation (`webhook-receiver-<uuid>`)
    so a leftover pod from an interrupted prior run is identifiable and never
    collides with a fresh one (T-07-24). Torn down unconditionally in this
    fixture's own `finally` block -- even when the test itself raises.

    Yields:
        `(webhook_url, read_logs)`:
          - `webhook_url`: the Service's in-cluster DNS URL
            (`http://webhook-receiver-<id>.monitoring.svc.cluster.local:8080/webhook`).
          - `read_logs()`: returns the receiver Pod's captured stdout via
            `kubectl logs` (each request logged as one single-line
            `WEBHOOK_RECEIVED: <method> <path> <body>` entry), or `None` if
            the pod cannot currently be found.
    """
    name = f"webhook-receiver-{uuid.uuid4().hex[:8]}"
    pod_manifest, service_manifest = _webhook_receiver_manifests(name, _MONITORING_NAMESPACE)

    pod_file = tmp_path / "webhook-receiver-pod.json"
    pod_file.write_text(json.dumps(pod_manifest), encoding="utf-8")
    service_file = tmp_path / "webhook-receiver-service.json"
    service_file.write_text(json.dumps(service_manifest), encoding="utf-8")

    pod_created = False
    service_created = False
    try:
        proc = kubectl("-n", _MONITORING_NAMESPACE, "apply", "-f", str(pod_file))
        assert proc.returncode == 0, (
            f"failed to create webhook receiver pod {name!r} "
            f"(exit {proc.returncode}):\n{proc.stderr}"
        )
        pod_created = True

        _wait_for_pod_running(
            kubectl,
            namespace=_MONITORING_NAMESPACE,
            name=name,
            timeout=_WEBHOOK_RECEIVER_START_TIMEOUT_SECONDS,
        )

        proc = kubectl("-n", _MONITORING_NAMESPACE, "apply", "-f", str(service_file))
        assert proc.returncode == 0, (
            f"failed to create webhook receiver service {name!r} "
            f"(exit {proc.returncode}):\n{proc.stderr}"
        )
        service_created = True

        webhook_url = f"http://{name}.{_MONITORING_NAMESPACE}.svc.cluster.local:8080/webhook"

        def _read_logs() -> str | None:
            log_proc = kubectl("-n", _MONITORING_NAMESPACE, "logs", name)
            if log_proc.returncode != 0:
                return None
            return log_proc.stdout

        yield webhook_url, _read_logs
    finally:
        if service_created:
            kubectl(
                "-n",
                _MONITORING_NAMESPACE,
                "delete",
                "service",
                name,
                "--ignore-not-found=true",
                "--timeout=30s",
                timeout=45,
            )
        if pod_created:
            kubectl(
                "-n",
                _MONITORING_NAMESPACE,
                "delete",
                "pod",
                name,
                "--ignore-not-found=true",
                "--timeout=30s",
                timeout=45,
            )


def _read_secret_data(
    kubectl_json_fn: Callable[..., Any],
    namespace: str,
    name: str,
) -> dict[str, str]:
    """Read and base64-decode any Kubernetes Secret's `data` block.

    Copied in shape from `tests/e2e/slice/conftest.py`'s own helper of the
    same name (module docstring: a deliberate, minimal duplication).
    """
    secret = kubectl_json_fn("-n", namespace, "get", "secret", name)
    return {key: base64.b64decode(value).decode("utf-8") for key, value in secret["data"].items()}


@contextlib.contextmanager
def _analytics_owner_connection(
    kubectl_context_value: str,
    kubectl_json_fn: Callable[..., Any],
) -> Iterator[psycopg.Connection[Any]]:
    """Open a read-only, `analytics_owner`-authenticated connection to the analytical cluster.

    The CNPG-generated `analytics-db-app` Secret, read directly (no Vault
    dependency at all — this suite never writes, so `etl_app`'s Vault-backed
    credential machinery `tests/e2e/slice/conftest.py` needs is more than
    this package requires).
    """
    creds = _read_secret_data(kubectl_json_fn, _DATA_NAMESPACE, f"{_ANALYTICS_CLUSTER}-app")
    with _port_forward(
        kubectl_context_value,
        namespace=_DATA_NAMESPACE,
        service=f"{_ANALYTICS_CLUSTER}-rw",
        remote_port=5432,
    ) as local_port:
        conn = psycopg.connect(
            host="127.0.0.1",
            port=local_port,
            dbname=creds["dbname"],
            user=creds["user"],
            password=creds["password"],
            connect_timeout=10,
        )
        try:
            yield conn
        finally:
            conn.close()


@pytest.fixture
def analytics_connection(
    kubectl_context: str,  # noqa: F811 -- fixture-injection param name
    kubectl_json: Callable[..., Any],  # noqa: F811 -- fixture-injection param name
) -> Iterator[psycopg.Connection[Any]]:
    """A live connection to the analytical cluster, authenticated as `analytics_owner`.

    Every test through plan 07-07 only ever reads through this connection;
    plan 07-08's Task 3 is the first to also write through it (see this
    module's own docstring) -- `analytics_owner` already has unrestricted
    DDL/DML, so this fixture's shape did not need to change.
    """
    with _analytics_owner_connection(kubectl_context, kubectl_json) as conn:
        yield conn


def poll_file_discovered(
    conn: psycopg.Connection[Any],
    *,
    dataset: str,
    object_uri: str,
    timeout: float = 120,
) -> dict[str, Any]:
    """Poll `meta.files` for `object_uri` until discovery has registered it.

    Copied in shape from `tests/e2e/slice/conftest.py`'s own helper of the
    same name (module docstring: a deliberate, minimal duplication).
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT f.file_id, f.duplicate_of_file_id "
                "FROM meta.files f "
                "JOIN meta.datasets d ON d.dataset_id = f.dataset_id "
                "WHERE d.dataset_name = %s AND f.object_uri = %s",
                (dataset, object_uri),
            )
            row = cur.fetchone()
        if row is not None:
            return {"file_id": row[0], "duplicate_of_file_id": row[1]}
        time.sleep(_POLL_INTERVAL_SECONDS)
    msg = (
        f"meta.files has no row for dataset={dataset!r} object_uri={object_uri!r} "
        f"within {timeout}s -- discovery never registered it"
    )
    raise AssertionError(msg)


def poll_ingestion_run(
    conn: psycopg.Connection[Any],
    *,
    file_id: int,
    timeout: float = 180,
) -> dict[str, Any]:
    """Poll `meta.ingestion_runs` for `file_id`'s run until it reaches a terminal status.

    Folds `tests/e2e/slice/conftest.py`'s own `poll_run_for_file` +
    `poll_ingestion_run` pair into one call — this package only ever needs
    "did the run for this file finish, and how", never the intermediate
    `idempotency_key` handoff the two-call split exists for elsewhere.
    """
    deadline = time.monotonic() + timeout
    last_status: str | None = None
    while time.monotonic() < deadline:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM meta.ingestion_runs WHERE file_id = %s",
                (file_id,),
            )
            row = cur.fetchone()
        if row is not None:
            last_status = row[0]
            if last_status in _TERMINAL_RUN_STATUSES:
                return {"status": last_status}
        time.sleep(_POLL_INTERVAL_SECONDS)
    msg = (
        f"meta.ingestion_runs[file_id={file_id}] did not reach a terminal status within "
        f"{timeout}s (last observed status: {last_status!r})"
    )
    raise AssertionError(msg)


def poll_trace_claimed(
    conn: psycopg.Connection[Any],
    *,
    file_id: int,
    timeout: float = 180,
) -> dict[str, Any]:
    """Poll `meta.ingestion_runs` for `file_id`'s run until its trace/pod columns are claimed.

    `dataplat.pipeline.run.run_ingest` writes `trace_id`/`span_id`/
    `k8s_pod_name` together, in the SAME `claim_ingestion_run` UPDATE
    (`packages/dataplat/src/dataplat/metadata/postgres.py`), near the START
    of a run -- before any staging/publish work. So the moment this
    returns, the launched `ingest` pod is very likely still `Running`,
    giving `test_trace_propagation.py` its best chance to `kubectl get pod`
    it before `on_finish_action: delete_succeeded_pod` removes it. Waits
    for BOTH columns together (they are written in one UPDATE, so they only
    ever become non-NULL atomically) rather than polling for `trace_id`
    alone and hoping `k8s_pod_name` is also already set.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT trace_id, k8s_pod_name FROM meta.ingestion_runs WHERE file_id = %s",
                (file_id,),
            )
            row = cur.fetchone()
        if row is not None and row[0] is not None and row[1] is not None:
            return {"trace_id": row[0], "k8s_pod_name": row[1]}
        time.sleep(_POLL_INTERVAL_SECONDS)
    msg = (
        f"meta.ingestion_runs[file_id={file_id}] never got a claimed trace_id/k8s_pod_name "
        f"within {timeout}s -- either the run never started, or OBS-10's TRACEPARENT-derived "
        f"span was never valid (dataplat.pipeline.run.run_ingest only writes a non-NULL "
        f"trace_id when otel_trace.get_current_span().get_span_context().is_valid)"
    )
    raise AssertionError(msg)
