"""tests/e2e/cluster/test_airflow_workloads.py — INFRA-02 proved on the live cluster.

Honest limit: this proves the four Airflow components exist as four
separately schedulable workloads reporting Ready, with the kinds the pinned
chart actually renders, and that the running image is Airflow 3.3.0. It does
not prove any DAG executes — that is Phase 4's claim, not this phase's.

D-16 CORRECTION: the triggerer renders as a **StatefulSet**, not a fourth
Deployment (02-RESEARCH.md Anti-Patterns; verified against
`helm template helm/values/local/airflow.yaml`). D-16's own wording ("four
Airflow workloads Ready as separate deployments") is corrected here — do not
"fix" the assertions below back to four Deployments; that would break on a
correct cluster.

Every credential this module uses is read from the live cluster's CNPG-
generated `airflow-db-app` Secret at test time (D-14) — never a literal in
this file. The metadata connection goes through `kubectl port-forward`,
mirroring `tests/e2e/cluster/test_postgres_topology.py`: there is no ingress
for raw PostgreSQL in this phase.
"""

from __future__ import annotations

import base64
import contextlib
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request
from typing import TYPE_CHECKING, Any

import psycopg
import pytest

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

pytestmark = pytest.mark.cluster

NAMESPACE = "airflow"
DATA_NAMESPACE = "data"
METADATA_CLUSTER = "airflow-db"

# The three Deployments and the one StatefulSet the pinned chart 1.22.0
# actually renders for this values shape (KubernetesExecutor, statsd/celery/
# flower/pgbouncer/redis all off) — read from `helm template`, not assumed.
EXPECTED_DEPLOYMENTS = frozenset(
    {"airflow-api-server", "airflow-dag-processor", "airflow-scheduler"},
)
EXPECTED_STATEFULSETS = frozenset({"airflow-triggerer"})


def test_four_workloads_are_ready(kubectl_json: Callable[..., Any]) -> None:
    """Exactly three Deployments Available, exactly one StatefulSet Ready — not four Deployments."""
    deployments = kubectl_json("-n", NAMESPACE, "get", "deployment")["items"]
    deployment_names = {d["metadata"]["name"] for d in deployments}
    assert deployment_names == EXPECTED_DEPLOYMENTS, (
        f"expected exactly the Deployments {sorted(EXPECTED_DEPLOYMENTS)}, "
        f"found {sorted(deployment_names)}"
    )
    for deployment in deployments:
        conditions = deployment.get("status", {}).get("conditions", [])
        available = next((c for c in conditions if c.get("type") == "Available"), None)
        assert available is not None, (
            f"Deployment/{deployment['metadata']['name']} reported no Available condition: "
            f"{conditions}"
        )
        assert available.get("status") == "True", (
            f"Deployment/{deployment['metadata']['name']} is not Available: {conditions}"
        )

    statefulsets = kubectl_json("-n", NAMESPACE, "get", "statefulset")["items"]
    statefulset_names = {s["metadata"]["name"] for s in statefulsets}
    assert statefulset_names == EXPECTED_STATEFULSETS, (
        f"expected exactly the StatefulSets {sorted(EXPECTED_STATEFULSETS)}, "
        f"found {sorted(statefulset_names)}"
    )
    for statefulset in statefulsets:
        status = statefulset.get("status", {})
        ready_replicas = status.get("readyReplicas", 0)
        replicas = status.get("replicas", 0)
        assert replicas > 0, f"StatefulSet/{statefulset['metadata']['name']} declares 0 replicas"
        assert ready_replicas == replicas, (
            f"StatefulSet/{statefulset['metadata']['name']} is not Ready: "
            f"readyReplicas={ready_replicas}, replicas={replicas}"
        )


def test_components_are_separate_workloads(kubectl_json: Callable[..., Any]) -> None:
    """INFRA-02's "separate workloads" claim: four distinct objects, four distinct pods."""
    all_names = EXPECTED_DEPLOYMENTS | EXPECTED_STATEFULSETS
    assert len(all_names) == 4, "the four expected component names collided — fix the test data"

    pods = kubectl_json("-n", NAMESPACE, "get", "pods")["items"]
    component_pods: dict[str, list[str]] = {name: [] for name in all_names}
    for pod in pods:
        pod_name = pod["metadata"]["name"]
        owner_prefixes = [name for name in all_names if pod_name.startswith(f"{name}-")]
        for prefix in owner_prefixes:
            component_pods[prefix].append(pod_name)

    for name, owned_pods in component_pods.items():
        assert owned_pods, f"no pod found belonging to component {name!r}"

    # Every component's pod set is disjoint from every other's — this is what
    # "four separate workloads" actually means, as opposed to four containers
    # sharing one pod.
    seen: set[str] = set()
    for name, owned_pods in component_pods.items():
        overlap = seen & set(owned_pods)
        assert not overlap, f"component {name!r} shares pod(s) {overlap} with another component"
        seen |= set(owned_pods)


def test_running_airflow_version_is_3_3_0(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    """`airflow version` inside the running api-server proves the image override beat 3.2.2."""
    proc = kubectl(
        "-n",
        NAMESPACE,
        "exec",
        "deploy/airflow-api-server",
        "--",
        "airflow",
        "version",
    )
    assert proc.returncode == 0, (
        f"`airflow version` failed inside deploy/airflow-api-server (exit {proc.returncode}):\n"
        f"{proc.stderr}"
    )
    assert "3.3.0" in proc.stdout, (
        f"expected 'airflow version' to report 3.3.0, got: {proc.stdout!r}"
    )


def _free_local_port() -> int:
    """Ask the OS for an unused TCP port, then release it immediately."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _read_app_secret(kubectl_json: Callable[..., Any], cluster: str) -> dict[str, str]:
    """Read and base64-decode the CNPG-generated `<cluster>-app` Secret (never logged)."""
    secret = kubectl_json("-n", DATA_NAMESPACE, "get", "secret", f"{cluster}-app")
    return {key: base64.b64decode(value).decode("utf-8") for key, value in secret["data"].items()}


@contextlib.contextmanager
def _port_forwarded_postgres(kubectl_context: str, cluster: str) -> Iterator[int]:
    """Port-forward `<cluster>-rw` to a free local port, torn down unconditionally on exit."""
    kubectl_bin = shutil.which("kubectl")
    assert kubectl_bin, "kubectl not found on PATH"

    local_port = _free_local_port()
    proc = subprocess.Popen(  # noqa: S603
        [
            kubectl_bin,
            "--context",
            kubectl_context,
            "-n",
            DATA_NAMESPACE,
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
                socket.create_connection(("127.0.0.1", local_port), timeout=1),
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


@pytest.fixture
def metadata_connection(
    kubectl_context: str,
    kubectl_json: Callable[..., Any],
) -> Iterator[psycopg.Connection[Any]]:
    """A live connection to the Airflow metadata cluster, torn down on exit."""
    creds = _read_app_secret(kubectl_json, METADATA_CLUSTER)
    with _port_forwarded_postgres(kubectl_context, METADATA_CLUSTER) as local_port:
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


def test_metadata_schema_migrated(metadata_connection: psycopg.Connection[Any]) -> None:
    """The migration job actually ran: an alembic_version row exists and public is non-trivial.

    Deliberately does not assert an exact table count or an exact revision
    hash from memory (02-RESEARCH.md measured 71 tables / `d2f4e1b3c5a7` on
    a specific chart+image combination, and that number is not this test's
    contract) — it reads what is there and asserts it is non-trivial and
    self-consistent instead.
    """
    with metadata_connection.cursor() as cur:
        cur.execute("select version_num from alembic_version")
        rows = cur.fetchall()
        assert len(rows) == 1, f"expected exactly one alembic_version row, found {len(rows)}"
        version_num = rows[0][0]
        assert isinstance(version_num, str), (
            f"alembic_version.version_num is not a string: {version_num!r}"
        )
        assert version_num, "alembic_version.version_num is empty"

        cur.execute(
            "select count(*) from information_schema.tables where table_schema = 'public'",
        )
        table_count = cur.fetchone()[0]
        # 02-RESEARCH.md measured 71 tables for this exact chart/image
        # combination — a floor well below that (order-of-magnitude, not
        # exact) proves a real migration ran without pinning to that number.
        assert table_count > 30, (
            f"expected a non-trivial migrated schema in 'public' (order of magnitude of "
            f"dozens of tables), found only {table_count}"
        )


def test_ui_answers_through_the_ingress() -> None:
    """A GET to http://airflow.localtest.me/ returns a non-error status through the ingress."""
    url = "http://airflow.localtest.me/"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:  # http, not user input
            status = resp.status
    except urllib.error.HTTPError as exc:
        status = exc.code
    except OSError as exc:
        pytest.fail(
            f"connection to {url} failed outright rather than reaching the Airflow "
            f"api-server through the ingress: {exc}",
        )
    assert 200 <= status < 400, (
        f"expected a 2xx or 3xx from {url} through the ingress, got {status}"
    )


def test_no_bundled_postgres_is_running(kubectl_json: Callable[..., Any]) -> None:
    """postgresql.enabled: false held — no airflow-namespace pod runs a Bitnami legacy image."""
    pods = kubectl_json("-n", NAMESPACE, "get", "pods")["items"]
    offending: list[str] = []
    for pod in pods:
        containers = pod["spec"].get("containers", []) + pod["spec"].get("initContainers", [])
        for container in containers:
            image = container.get("image", "")
            if "bitnamilegacy" in image or "bitnami/postgresql" in image:
                offending.append(f"{pod['metadata']['name']}/{container['name']}: {image}")
    assert not offending, (
        "a values regression re-enabled the bundled subchart — bitnamilegacy image(s) found:\n"
        + "\n".join(offending)
    )
