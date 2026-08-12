"""tests/e2e/cluster/test_postgres_topology.py — INFRA-03/INFRA-04 proved on the live cluster.

Honest limit: this proves the two servers report the intended majors and
share no `Cluster` resource, node or PVC — a mechanical snapshot of the live
cluster at test time. It does not prove that no future values change could
co-locate them; that guarantee is `helm/values/{local,ci}/cnpg-{airflow,
analytics}.yaml`'s explicit `nodeSelector` plus the human review process
around changing it, not something a test can enforce for all time.

Every credential this module uses is read from the live cluster's CNPG-
generated `<cluster>-app` Secret at test time (D-14) — never a literal in
this file or anywhere in the working tree. Connections go through
`kubectl port-forward`: there is no ingress for raw PostgreSQL in this phase
(D-05/D-07 cover HTTP ingress and S3 only), so a ClusterIP Service is not
otherwise reachable from the test host running pytest outside the cluster.
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

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

pytestmark = pytest.mark.cluster

NAMESPACE = "data"
ALLOWED_SCHEMAS = {"pg_catalog", "information_schema", "public", "pg_toast"}


def _free_local_port() -> int:
    """Ask the OS for an unused TCP port, then release it immediately.

    A small race (something else grabs the port before `kubectl
    port-forward` binds it) is possible in principle but was not observed in
    practice, and is the same tradeoff every "find a free port for a test
    server" helper makes.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _read_app_secret(
    kubectl_json: Callable[..., Any],
    cluster: str,
) -> dict[str, str]:
    """Read and base64-decode a CNPG-generated `<cluster>-app` Secret.

    02-RESEARCH.md Pattern 4 verified this Secret's key list end-to-end:
    dbname, fqdn-jdbc-uri, fqdn-uri, host, jdbc-uri, password, pgpass, port,
    uri, user, username. This function never writes a decoded value to disk
    or to a log — callers must not either.
    """
    secret = kubectl_json("-n", NAMESPACE, "get", "secret", f"{cluster}-app")
    return {key: base64.b64decode(value).decode("utf-8") for key, value in secret["data"].items()}


@contextlib.contextmanager
def _port_forwarded_postgres(kubectl_context: str, cluster: str) -> Iterator[int]:
    """Port-forward `<cluster>-rw` to a free local port for this test process only.

    Torn down unconditionally in the `finally` block — a leaked
    `kubectl port-forward` process is exactly the kind of thing that makes a
    second test run mysteriously fail on "address already in use".
    """
    kubectl_bin = shutil.which("kubectl")
    assert kubectl_bin, "kubectl not found on PATH"

    local_port = _free_local_port()
    proc = subprocess.Popen(  # noqa: S603
        [
            kubectl_bin,
            "--context",
            kubectl_context,
            "-n",
            NAMESPACE,
            "port-forward",
            f"svc/{cluster}-rw",
            f"{local_port}:5432",
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
                msg = f"kubectl port-forward for {cluster}-rw exited early:\n{output}"
                raise AssertionError(msg)
            with (
                contextlib.suppress(OSError),
                socket.create_connection(
                    ("127.0.0.1", local_port),
                    timeout=1,
                ),
            ):
                connected = True
                break
            time.sleep(0.5)
        if not connected:
            msg = f"kubectl port-forward for {cluster}-rw never accepted a connection within 30s"
            raise AssertionError(msg)
        yield local_port
    finally:
        proc.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=10)


@contextlib.contextmanager
def _cluster_connection(
    kubectl_context: str,
    kubectl_json: Callable[..., Any],
    cluster: str,
) -> Iterator[psycopg.Connection[Any]]:
    """Open a psycopg connection to `<cluster>-rw` via a torn-down-on-exit port-forward."""
    creds = _read_app_secret(kubectl_json, cluster)
    with _port_forwarded_postgres(kubectl_context, cluster) as local_port:
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
def metadata_connection(
    kubectl_context: str,
    kubectl_json: Callable[..., Any],
) -> Iterator[psycopg.Connection[Any]]:
    """A live connection to the Airflow metadata cluster (`airflow-db`)."""
    with _cluster_connection(kubectl_context, kubectl_json, "airflow-db") as conn:
        yield conn


@pytest.fixture
def analytics_connection(
    kubectl_context: str,
    kubectl_json: Callable[..., Any],
) -> Iterator[psycopg.Connection[Any]]:
    """A live connection to the analytical cluster (`analytics-db`)."""
    with _cluster_connection(kubectl_context, kubectl_json, "analytics-db") as conn:
        yield conn


def _server_version(conn: psycopg.Connection[Any]) -> str:
    with conn.cursor() as cur:
        cur.execute("show server_version")
        row = cur.fetchone()
        assert row is not None
        return str(row[0])


def _schema_names(conn: psycopg.Connection[Any]) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("select schema_name from information_schema.schemata")
        return {str(row[0]) for row in cur.fetchall()}


def _database_exists(conn: psycopg.Connection[Any], name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("select 1 from pg_catalog.pg_database where datname = %s", (name,))
        return cur.fetchone() is not None


def _primary_pod(kubectl_json: Callable[..., Any], cluster: str) -> dict[str, Any]:
    pods = kubectl_json(
        "-n",
        NAMESPACE,
        "get",
        "pods",
        "-l",
        f"cnpg.io/cluster={cluster},cnpg.io/instanceRole=primary",
    )["items"]
    assert len(pods) == 1, (
        f"expected exactly one primary pod for cluster '{cluster}', found {len(pods)}"
    )
    return dict(pods[0])


def _pod_pvc_names(pod: dict[str, Any]) -> set[str]:
    return {
        volume["persistentVolumeClaim"]["claimName"]
        for volume in pod["spec"].get("volumes", [])
        if "persistentVolumeClaim" in volume
    }


def test_metadata_is_pg17(metadata_connection: psycopg.Connection[Any]) -> None:
    version = _server_version(metadata_connection)
    assert version.startswith("17."), (
        f"expected the metadata cluster to report 17.x, got {version!r}"
    )


def test_analytics_is_pg18(analytics_connection: psycopg.Connection[Any]) -> None:
    version = _server_version(analytics_connection)
    assert version.startswith("18."), (
        f"expected the analytical cluster to report 18.x, got {version!r}"
    )


def test_two_distinct_clusters_no_shared_storage(
    kubectl_json: Callable[..., Any],
) -> None:
    """D-03's physical separation, proved: two Clusters, two nodes, disjoint PVCs.

    Also asserts the primaries' node placement matches the
    `airflow-platform/role` labels declared in kind/cluster.yaml, so a
    scheduler that silently co-located them — or a values file that dropped
    the nodeSelector — is reported as a failure rather than tolerated.
    """
    clusters = kubectl_json("-n", NAMESPACE, "get", "cluster")["items"]
    names = sorted(c["metadata"]["name"] for c in clusters)
    assert names == ["airflow-db", "analytics-db"], (
        f"expected exactly Cluster resources 'airflow-db' and 'analytics-db', found {names}"
    )

    metadata_pod = _primary_pod(kubectl_json, "airflow-db")
    analytics_pod = _primary_pod(kubectl_json, "analytics-db")
    metadata_node = metadata_pod["spec"]["nodeName"]
    analytics_node = analytics_pod["spec"]["nodeName"]
    assert metadata_node != analytics_node, (
        f"both primaries are scheduled on the same node {metadata_node!r} — "
        f"D-03 requires the metadata and analytical clusters on different nodes"
    )

    node_labels = {
        node["metadata"]["name"]: node["metadata"].get("labels", {})
        for node in kubectl_json("get", "nodes")["items"]
    }
    assert node_labels[metadata_node].get("airflow-platform/role") == "storage", (
        f"metadata primary is on node {metadata_node!r}, which is not labelled "
        f"airflow-platform/role=storage"
    )
    assert node_labels[analytics_node].get("airflow-platform/role") == "analytics", (
        f"analytics primary is on node {analytics_node!r}, which is not labelled "
        f"airflow-platform/role=analytics"
    )

    metadata_pvcs = _pod_pvc_names(metadata_pod)
    analytics_pvcs = _pod_pvc_names(analytics_pod)
    assert metadata_pvcs, "the metadata primary pod has no PersistentVolumeClaim attached"
    assert analytics_pvcs, "the analytics primary pod has no PersistentVolumeClaim attached"
    assert metadata_pvcs.isdisjoint(analytics_pvcs), (
        f"the metadata and analytical primaries share a PVC: {metadata_pvcs & analytics_pvcs}"
    )


def test_metadata_cluster_holds_no_analytical_objects(
    metadata_connection: psycopg.Connection[Any],
    analytics_connection: psycopg.Connection[Any],
) -> None:
    """INFRA-03's "used by nothing else", made mechanically checkable at this stage."""
    assert not _database_exists(metadata_connection, "analytics"), (
        "the Airflow metadata cluster hosts an 'analytics' database — it must not"
    )
    assert not _database_exists(analytics_connection, "airflow"), (
        "the analytical cluster hosts an 'airflow' database — it must not"
    )


def test_no_extra_schemas_exist(
    metadata_connection: psycopg.Connection[Any],
    analytics_connection: psycopg.Connection[Any],
) -> None:
    """D-15: DDL has exactly one home (Alembic, Phase 3) — no schema exists yet."""
    for conn, label in ((metadata_connection, "metadata"), (analytics_connection, "analytical")):
        extra = _schema_names(conn) - ALLOWED_SCHEMAS
        assert not extra, f"the {label} cluster carries unexpected schema(s): {sorted(extra)}"
