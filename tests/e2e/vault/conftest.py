"""Shared fixtures for tests/e2e/vault/ — the live-cluster Vault verification harness.

`repo_root`, `cluster_name`, `kubectl_context`, `_require_cluster`, `kubectl`
and `kubectl_json` are duplicated VERBATIM from
`tests/e2e/cluster/conftest.py` (this repository's established convention:
`tests/e2e/*/conftest.py` each carry their own copy of small helpers rather
than sharing a library module, per 05-01-PLAN.md's own Interfaces section).

`vault_addr` and `vault_root_client` are new to this module: a single,
session-scoped `kubectl port-forward` tunnel to `svc/vault` (namespace
`vault`, port 8200), and an `hvac.Client` authenticated with the root token
read from `.secrets/vault-init.json`. Tests that themselves restart the
`vault-0` pod (test_unseal_survives_restart.py) cannot reuse this session
tunnel across the restart — a `kubectl port-forward` process is bound to
the specific pod IP it connected to and does not follow a Service to a
freshly-recreated backing pod — so that module opens its own fresh
tunnel(s) around the restart, duplicating the same port-forward shape
locally rather than depending on this fixture's tunnel surviving a pod
delete.

`poll_pod_running` is a plain function, not a fixture (this repository's
established convention for substantial reusable poll/wait logic — see
`tests/e2e/slice/conftest.py`'s `poll_file_discovered`/`poll_ingestion_run`/
`poll_run_for_file`, which are imported explicitly by name rather than
auto-injected, since pytest only auto-provides `@pytest.fixture`-decorated
names even from a same-directory conftest.py). It replaces the bare
`kubectl wait --for=jsonpath={.status.phase}=Running pod/<name>` pattern
that both `test_unseal_survives_restart.py` (this directory) and
`tests/e2e/chaos/test_vault_unavailable.py` (which copied the pattern,
believing it already-proven-working — see that module's own docstring) used
to delete `vault-0` and wait for the StatefulSet-recreated replacement.
`kubectl wait` on a NAMED resource (not a label selector) fails FAST with
`NotFound` if the object does not exist yet at the moment the command
starts — it polls the CONDITION of an object that already exists, never the
object's own (re-)creation — a documented kubectl limitation
(kubernetes/kubectl#1675 covers the label-selector case; this project
confirmed live, this same debug session, that a NAMED resource has the
identical fail-fast-on-NotFound behavior). This is the same bug class
`tests/e2e/chaos/conftest.py`'s own `_poll_all_pods_ready` already fixed for
label-selector queries with a hand-rolled `deadline = time.monotonic() +
timeout` poll loop; `poll_pod_running` is that same established idiom
applied to a single named pod instead of a label selector, living here
(not in `tests/e2e/chaos/conftest.py`) because vault-0 restart-and-recreate
is a vault-owned concern, imported directly by every caller — this
repository's own convention, not re-exported as a fixture.
"""

from __future__ import annotations

import json
import shutil
import socket
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import hvac
import pytest

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

REPO_ROOT = Path(__file__).resolve().parents[3]
VERSIONS_ENV = REPO_ROOT / "helm" / "versions.env"
VAULT_INIT_FILE = REPO_ROOT / ".secrets" / "vault-init.json"

VAULT_NAMESPACE = "vault"
VAULT_SERVICE = "vault"
VAULT_PORT = 8200


def _versions_env_variable(name: str) -> str:
    """Read a `KEY=value` line from `helm/versions.env` (the single source, plan 02-01)."""
    text = VERSIONS_ENV.read_text(encoding="utf-8")
    for line in text.splitlines():
        if line.startswith(f"{name}="):
            return line.split("=", 1)[1].strip()
    msg = f"helm/versions.env does not define {name}"
    raise AssertionError(msg)


@pytest.fixture(scope="session")
def repo_root() -> Path:
    """Return the absolute path of the repository root."""
    return REPO_ROOT


@pytest.fixture(scope="session")
def cluster_name() -> str:
    """The kind cluster name, read from helm/versions.env — never hardcoded here."""
    return _versions_env_variable("CLUSTER_NAME")


@pytest.fixture(scope="session")
def kubectl_context(cluster_name: str) -> str:
    """The kubectl context kind registers for this cluster: `kind-<name>`."""
    return f"kind-{cluster_name}"


@pytest.fixture(scope="session", autouse=True)
def _require_cluster(kubectl_context: str) -> None:
    """Skip the whole suite, with a named reason, when no live cluster answers."""
    kubectl_bin = shutil.which("kubectl")
    if kubectl_bin is None:
        pytest.skip("kubectl not found on PATH — tests/e2e/vault/ needs a live cluster")
    proc = subprocess.run(  # noqa: S603
        [kubectl_bin, "--context", kubectl_context, "get", "nodes", "-o", "name"],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    if proc.returncode != 0:
        pytest.skip(
            f"no live cluster reachable at context '{kubectl_context}' "
            f"(kubectl exited {proc.returncode}) — run `make cluster-up` first:\n{proc.stderr}",
        )


@pytest.fixture(scope="session")
def kubectl(kubectl_context: str) -> Callable[..., subprocess.CompletedProcess[str]]:
    """Session-scoped kubectl helper: kubectl("get", "nodes") -> CompletedProcess.

    Always names `--context kubectl_context` explicitly — never the ambient
    current-context, which a developer's shell could have pointed anywhere.
    """
    kubectl_bin = shutil.which("kubectl")
    assert kubectl_bin, "kubectl not found on PATH"

    def _run(*args: str, timeout: int = 30) -> subprocess.CompletedProcess[str]:
        cmd = [kubectl_bin, "--context", kubectl_context, *args]
        return subprocess.run(  # noqa: S603
            cmd,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )

    return _run


@pytest.fixture(scope="session")
def kubectl_json(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
) -> Callable[..., Any]:
    """Session-scoped kubectl helper that shells out and returns PARSED JSON."""

    def _get(*args: str) -> Any:
        proc = kubectl(*args, "-o", "json")
        assert proc.returncode == 0, (
            f"kubectl {' '.join(args)} failed (exit {proc.returncode}):\n{proc.stderr}"
        )
        return json.loads(proc.stdout)

    return _get


def _free_local_port() -> int:
    """Ask the OS for an unused TCP port, then release it immediately."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


_POD_RUNNING_POLL_INTERVAL_SECONDS = 2.0


def poll_pod_running(
    kubectl_fn: Callable[..., subprocess.CompletedProcess[str]],
    *,
    namespace: str,
    pod_name: str,
    timeout: float,
) -> None:
    """Poll a NAMED pod until it reports `status.phase == "Running"` (see module docstring).

    Tolerates the pod object not existing yet -- the exact race a bare
    `kubectl wait --for=jsonpath=... pod/<name>` loses when invoked
    immediately after `kubectl delete pod <name>`: the StatefulSet
    controller has not necessarily recreated the object by the time `wait`
    starts, and `kubectl wait` on a NAMED resource fails FAST with
    `NotFound` instead of polling for the object's own (re-)creation. This
    function polls a plain `kubectl get pod <name>` instead (mirroring
    `tests/e2e/chaos/conftest.py`'s own `_poll_all_pods_ready` idiom -- a
    hand-rolled `deadline = time.monotonic() + timeout` loop, not `kubectl
    wait`) and treats EVERY non-zero exit (NotFound while the pod is being
    recreated, or a transient API-server hiccup) as "not there yet, keep
    polling" rather than a hard failure -- unlike a label-selector query
    (where a non-zero exit is unambiguously a query error, since "zero
    matches" is itself a normal, exit-0 result), a NAMED-resource query has
    no exit-0 way to represent "does not exist yet", so tolerating non-zero
    here is the only way to survive the exact race this function exists to
    fix. A genuine, non-transient problem (wrong namespace, wrong context,
    RBAC denial, ...) still surfaces: its error text is captured as
    `last_seen` and reported in the timeout message below, rather than
    silently swallowed.

    Args:
        kubectl_fn: The `kubectl` fixture callable.
        namespace: The namespace the pod lives in.
        pod_name: The pod's name -- a single, named resource, not a label
            selector.
        timeout: Maximum seconds to wait.

    Raises:
        AssertionError: `timeout` elapses without observing `phase=Running`.
    """
    deadline = time.monotonic() + timeout
    last_seen = "no observation yet"
    while time.monotonic() < deadline:
        proc = kubectl_fn(
            "-n",
            namespace,
            "get",
            "pod",
            pod_name,
            "-o",
            "jsonpath={.status.phase}",
        )
        if proc.returncode == 0:
            phase = proc.stdout.strip()
            last_seen = phase or "pod exists, phase not yet set"
            if phase == "Running":
                return
        else:
            last_seen = (
                f"kubectl get pod/{pod_name} failed (exit {proc.returncode}, likely still "
                f"being recreated): {proc.stderr.strip()}"
            )
        time.sleep(_POD_RUNNING_POLL_INTERVAL_SECONDS)
    msg = (
        f"pod/{pod_name} in namespace {namespace!r} did not reach phase=Running within "
        f"{timeout}s (last observed: {last_seen})"
    )
    raise AssertionError(msg)


@pytest.fixture(scope="session")
def vault_addr(kubectl_context: str) -> Iterator[str]:
    """Port-forward `svc/vault` (namespace `vault`, port 8200) for the whole test session.

    Same shape as `scripts/ingest-demo.py`'s `_port_forwarded_analytics`,
    adapted: one tunnel held open for the fixture's scope (HTTP requests
    through it are short-lived and stateless, unlike the raw PostgreSQL
    wire protocol that motivated `ingest-demo.py`'s one-tunnel-per-
    connection choice). Torn down on session exit via `proc.terminate()`.

    Yields:
        `"http://127.0.0.1:<local_port>"`.
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
            VAULT_NAMESPACE,
            "port-forward",
            f"svc/{VAULT_SERVICE}",
            f"{local_port}:{VAULT_PORT}",
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
                msg = f"kubectl port-forward for svc/{VAULT_SERVICE} exited early:\n{output}"
                raise RuntimeError(msg)
            try:
                with socket.create_connection(("127.0.0.1", local_port), timeout=1):
                    connected = True
                    break
            except OSError:
                time.sleep(0.5)
        if not connected:
            msg = (
                f"kubectl port-forward for svc/{VAULT_SERVICE} never "
                "accepted a connection within 30s"
            )
            raise RuntimeError(msg)
        yield f"http://127.0.0.1:{local_port}"
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest.fixture(scope="session")
def vault_root_client(vault_addr: str) -> hvac.Client:
    """An `hvac.Client` authenticated with the root token from `.secrets/vault-init.json`.

    Skips the whole module with a clear reason if that file does not exist
    — Vault has not been bootstrapped yet.
    """
    if not VAULT_INIT_FILE.is_file():
        pytest.skip(
            f"{VAULT_INIT_FILE} does not exist — run `make vault-unseal && "
            "make vault-bootstrap` first",
        )
    root_token = json.loads(VAULT_INIT_FILE.read_text(encoding="utf-8"))["root_token"]
    return hvac.Client(url=vault_addr, token=root_token)
