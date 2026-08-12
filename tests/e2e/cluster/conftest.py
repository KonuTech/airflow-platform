"""Shared fixtures for tests/e2e/cluster/ — the live-cluster verification harness (D-16).

Honest limit: this file proves nothing about the cluster by itself. It
provides three things every test in this directory needs — a way to skip
cleanly when no cluster is reachable, a `kubectl` helper that always names
this project's own context explicitly (never the ambient current-context,
which could be pointed anywhere), and an `s3_client` factory that stays a
named skip until plan 02-04 deploys MinIO. The actual assertions live in each
`test_*.py` module.

The repository root is resolved once, from this file's own location
(`parents[3]`: cluster -> e2e -> tests -> repo root), so a test never depends
on the working directory pytest happened to be started from — the same
convention every `tests/policy/*.py` module uses.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Any

# Imported here, at module top level, and otherwise unused in this file on
# purpose (02-RESEARCH.md Open Question 1): tests/e2e/cluster's dependency on
# the `cluster` uv group must be a COLLECTION-time fact, not a runtime
# surprise. conftest.py collects first, so a missing `--group cluster` shows
# up as one clear wall of import errors here rather than a confusing failure
# deep inside whichever test happens to touch S3 or Postgres first. boto3 is
# what D-16's own e2e assertions use (via s3_client below); psycopg is the
# driver plan 02-03's e2e Postgres tests need — the `cluster` group's promise
# ("both packages import inside the suite") must hold from this plan's first
# commit, not from whichever later plan happens to use psycopg first.
import boto3  # noqa: F401
import psycopg  # noqa: F401
import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

REPO_ROOT = Path(__file__).resolve().parents[3]
VERSIONS_ENV = REPO_ROOT / "helm" / "versions.env"


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
    """Skip the whole suite, with a named reason, when no live cluster answers.

    A developer without a cluster running should see one clear skip message,
    not a wall of connection-refused errors from every test in this
    directory. `autouse=True` at session scope means this runs once, before
    the first test in tests/e2e/cluster/ collects its other fixtures.
    """
    kubectl_bin = shutil.which("kubectl")
    if kubectl_bin is None:
        pytest.skip("kubectl not found on PATH — tests/e2e/cluster/ needs a live cluster")
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
    Returns the raw CompletedProcess (not check=True) so a caller decides
    whether a non-zero exit is itself the assertion under test.
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
    """Session-scoped kubectl helper that shells out and returns PARSED JSON.

    `kubectl_json("get", "nodes")` is `kubectl get nodes -o json`, decoded.
    Asserts a zero exit itself, so a caller only ever sees a successful
    kubectl invocation's parsed output — a failed one is a test failure at
    the fixture boundary, with the real kubectl stderr attached.
    """

    def _get(*args: str) -> Any:
        proc = kubectl(*args, "-o", "json")
        assert proc.returncode == 0, (
            f"kubectl {' '.join(args)} failed (exit {proc.returncode}):\n{proc.stderr}"
        )
        return json.loads(proc.stdout)

    return _get


@pytest.fixture(scope="session")
def s3_client() -> Any:
    """Boto3 client built from live cluster credentials (D-16, D-07).

    MinIO is not deployed until plan 02-04 (D-14: credentials are generated
    into the cluster at `cluster-up` time and never written to the working
    tree). This fixture exists now so every later e2e test in this directory
    can request `s3_client` uniformly, but it skips with a named reason until
    that plan lands — a real implementation will read a Kubernetes Secret at
    fixture setup and construct `boto3.client("s3", endpoint_url=...)`,
    never echoing the credential value anywhere a test log could capture it.
    """
    pytest.skip("MinIO not yet deployed — plan 02-04 makes s3_client live (D-16, D-07)")
