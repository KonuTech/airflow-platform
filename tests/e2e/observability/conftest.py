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
package only needs a read-only connection to the analytical database (never
`etl_app`'s Vault-authenticated role — this suite never writes, so the
CNPG-generated `owner` role, requiring no Vault dependency at all, is the
simpler, sufficient choice) and the two polling helpers that bridge
"I uploaded a file" to "the run reached SUCCEEDED". `grafana_addr` is new to
this module: a session-scoped `kubectl port-forward` tunnel to `svc/
monitoring-grafana` (namespace `monitoring`, port 80), mirroring
`tests/e2e/vault/conftest.py`'s own `vault_addr` shape exactly.
"""

from __future__ import annotations

import base64
import contextlib
import shutil
import socket
import subprocess
import time
from typing import TYPE_CHECKING, Any

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
    """A live, read-only connection to the analytical cluster (the `analytics_owner` role)."""
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
