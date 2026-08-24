"""tests/e2e/vault/test_unseal_survives_restart.py — INFRA-06 proved on the live cluster.

Honest limit: this proves the ONE restart-survival claim ROADMAP's SC3
names -- a `vault-0` pod restart reseals Vault without losing previously-
written data, and `scripts/vault-unseal.py`'s unseal path (not a fresh
`sys.initialize()`) is what restores service. It does not exercise HA,
auto-unseal, or any production seal/storage backend -- this is a single-
node, file-storage, Shamir-threshold-1 Vault, exactly as
helm/values/local/vault.yaml deploys it.

`vault-0` is deleted via a live `kubectl delete pod` invocation performed
directly by this test, matching `tests/e2e/cluster/test_airflow_workloads.py`'s
own `kubectl port-forward`/`kubectl exec` use: `tests/` is outside
`tests/policy/test_no_manual_kubectl_surgery.py`'s scanned `SCAN_DIRS`.

The session-scoped `vault_addr`/`vault_root_client` fixtures (conftest.py)
are used ONLY for the pre-restart setup write, where the pod is stable.
`kubectl port-forward` binds to the specific pod IP it connected to and
does not follow a Kubernetes Service to a freshly-recreated backing pod, so
every check performed AFTER the restart opens its own fresh tunnel via
`_port_forwarded_vault` below (duplicated from `scripts/vault-unseal.py`'s
helper of the same name and shape, per this repository's small-helper-
duplication convention).
"""

from __future__ import annotations

import contextlib
import json
import shutil
import socket
import subprocess
import sys
import time
from typing import TYPE_CHECKING

import hvac
import hvac.exceptions
import pytest

from tests.e2e.vault.conftest import poll_pod_running

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

pytestmark = pytest.mark.cluster

_VAULT_NAMESPACE = "vault"
_VAULT_SERVICE = "vault"
_VAULT_PORT = 8200
_VAULT_POD = "vault-0"

_RESTART_PROBE_MOUNT = "etl"
_RESTART_PROBE_PATH = "_restart_probe"
_RESTART_PROBE_VALUE = {"value": "airflow-platform e2e restart probe (plan 05-01)"}

_POD_RESTART_TIMEOUT_SECONDS = 180


def _free_local_port() -> int:
    """Ask the OS for an unused TCP port, then release it immediately."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextlib.contextmanager
def _port_forwarded_vault(kubectl_context: str) -> Iterator[int]:
    """Port-forward `svc/vault` (namespace `vault`, port 8200) to a FRESH local port.

    Duplicated from `scripts/vault-unseal.py`'s `_port_forwarded_vault` (same
    name, same shape) -- this module's own copy, opened fresh whenever the
    caller needs a tunnel guaranteed to target the CURRENT `vault-0` pod,
    which the session-scoped `vault_addr` fixture cannot guarantee across a
    pod restart.

    Args:
        kubectl_context: The kubectl context to port-forward through.

    Yields:
        The local port the tunnel is listening on.

    Raises:
        RuntimeError: The port-forward process exits before ever accepting a
            connection, or never accepts one within 30s.
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
            try:
                with socket.create_connection(("127.0.0.1", local_port), timeout=1):
                    connected = True
                    break
            except OSError:
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
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def test_pod_restart_reseals_and_unseal_restores_service(
    vault_root_client: hvac.Client,
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    kubectl_context: str,
    repo_root: Path,
) -> None:
    """INFRA-06 / SC3: a `vault-0` restart reseals Vault; `make vault-unseal` alone restores it.

    Args:
        vault_root_client: Session-scoped authenticated client (conftest.py)
            — used only for the pre-restart setup write, while the pod is
            still stable.
        kubectl: Session-scoped kubectl helper (conftest.py).
        kubectl_context: The kubectl context (conftest.py).
        repo_root: The repository root (conftest.py) — used to locate
            `scripts/vault-unseal.py`.
    """
    # Setup: write a throwaway KV secret while Vault is unsealed and stable.
    vault_root_client.secrets.kv.v2.create_or_update_secret(
        mount_point=_RESTART_PROBE_MOUNT,
        path=_RESTART_PROBE_PATH,
        secret=_RESTART_PROBE_VALUE,
    )

    try:
        # Delete vault-0 -- a live, direct kubectl mutation performed by the
        # TEST itself (tests/ is outside test_no_manual_kubectl_surgery.py's
        # scanned SCAN_DIRS).
        delete_proc = kubectl("-n", _VAULT_NAMESPACE, "delete", "pod", _VAULT_POD)
        assert delete_proc.returncode == 0, (
            f"kubectl delete pod/{_VAULT_POD} failed (exit {delete_proc.returncode}):\n"
            f"{delete_proc.stderr}"
        )

        # Bounded poll for the StatefulSet-recreated pod to be Running again
        # -- never Ready (Vault's own readinessProbe fails while sealed, the
        # exact reason scripts/wait-for.sh's wait_for_pod_running exists).
        # Uses conftest.py's poll_pod_running, NOT a bare `kubectl wait` --
        # `kubectl wait` on a NAMED resource fails fast with NotFound if the
        # StatefulSet controller has not recreated the pod object yet at the
        # moment `wait` is invoked, rather than polling for its creation
        # (see poll_pod_running's own docstring for the full mechanism).
        poll_pod_running(
            kubectl,
            namespace=_VAULT_NAMESPACE,
            pod_name=_VAULT_POD,
            timeout=_POD_RESTART_TIMEOUT_SECONDS,
        )

        # Test 1 (05-01-PLAN.md <behavior>): the restart is real, not a
        # no-op -- Vault reports sealed, and the previously-written data is
        # unreadable while sealed. A fresh tunnel is required here: the
        # tunnel vault_root_client used above is bound to the pod IP that no
        # longer exists.
        with _port_forwarded_vault(kubectl_context) as local_port:
            resealed_client = hvac.Client(url=f"http://127.0.0.1:{local_port}")
            assert resealed_client.sys.is_sealed() is True, (
                "vault-0 did not report sealed immediately after being "
                "recreated -- the restart-then-reseal this test exists to "
                "prove did not actually happen"
            )
            with pytest.raises(hvac.exceptions.VaultError):
                resealed_client.secrets.kv.v2.read_secret_version(
                    mount_point=_RESTART_PROBE_MOUNT,
                    path=_RESTART_PROBE_PATH,
                    raise_on_deleted_version=True,
                )

        # Test 2 (05-01-PLAN.md <behavior>): scripts/vault-unseal.py's
        # unseal path (not a fresh sys.initialize()) restores service. Run
        # exactly as the Makefile invokes it (sys.executable, same
        # interpreter/venv this test itself runs under).
        unseal_script = repo_root / "scripts" / "vault-unseal.py"
        unseal_proc = subprocess.run(  # noqa: S603
            [sys.executable, str(unseal_script)],
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )
        assert unseal_proc.returncode == 0, (
            f"scripts/vault-unseal.py exited {unseal_proc.returncode}:\n"
            f"stdout={unseal_proc.stdout}\nstderr={unseal_proc.stderr}"
        )
        assert "initialized" not in unseal_proc.stdout, (
            "scripts/vault-unseal.py re-initialized an already-initialized "
            f"Vault instead of taking the unseal-only path:\n{unseal_proc.stdout}"
        )

        with _port_forwarded_vault(kubectl_context) as local_port:
            unsealed_client = hvac.Client(url=f"http://127.0.0.1:{local_port}")
            assert unsealed_client.sys.is_sealed() is False, (
                "vault-0 still reports sealed after scripts/vault-unseal.py "
                "exited 0 -- the documented unseal procedure did not "
                "actually restore service"
            )
            root_token = json.loads(
                (repo_root / ".secrets" / "vault-init.json").read_text(encoding="utf-8"),
            )["root_token"]
            unsealed_client.token = root_token
            response = unsealed_client.secrets.kv.v2.read_secret_version(
                mount_point=_RESTART_PROBE_MOUNT,
                path=_RESTART_PROBE_PATH,
                raise_on_deleted_version=True,
            )
            assert response["data"]["data"] == _RESTART_PROBE_VALUE, (
                "the throwaway KV secret did not read back byte-identical "
                f"after the restart+unseal cycle: {response['data']['data']!r} "
                f"!= {_RESTART_PROBE_VALUE!r}"
            )
    finally:
        # Cleanup runs whether or not the restart happened, but by this
        # point the pod MAY already have been recreated -- `vault_root_client`
        # fixture's session tunnel is bound to whichever pod IP was live
        # when it first connected, which is stale after a restart (the same
        # reason every post-restart check above opens its own fresh
        # tunnel). A brand-new tunnel always reaches whatever `vault-0`
        # currently exists, restarted or not.
        with (
            contextlib.suppress(RuntimeError, hvac.exceptions.VaultError),
            _port_forwarded_vault(kubectl_context) as local_port,
        ):
            cleanup_client = hvac.Client(url=f"http://127.0.0.1:{local_port}")
            root_token = json.loads(
                (repo_root / ".secrets" / "vault-init.json").read_text(encoding="utf-8"),
            )["root_token"]
            cleanup_client.token = root_token
            cleanup_client.secrets.kv.v2.delete_metadata_and_all_versions(
                mount_point=_RESTART_PROBE_MOUNT,
                path=_RESTART_PROBE_PATH,
            )
