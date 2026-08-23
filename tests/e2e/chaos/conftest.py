"""Shared fixtures for tests/e2e/chaos/ — the live fault-injection harness (11-09-PLAN.md).

`repo_root`/`cluster_name`/`kubectl_context`/`kubectl`/`kubectl_json`/`s3_client`/
`_require_cluster` are imported directly from `tests.e2e.cluster.conftest` and re-exported as
pytest fixtures below -- this repository's established convention (`tests/e2e/slice/conftest.py`
does the identical thing for the identical reason: `tests/e2e/chaos` is a SIBLING of
`tests/e2e/cluster`, not a child of it, so pytest's own directory-based conftest inheritance
cannot supply them for free). `vault_addr` is re-exported from `tests.e2e.vault.conftest` the
same way, and `analytics_connection`/`analytics_owner_connection`/`slice_fixtures_dir`/
`_unpause_slice_dags`/`airflow_metadata_connection` from `tests.e2e.slice.conftest` -- every chaos
test that uploads a file and polls `meta.*` for it needs the identical DB-connection/DAG-unpause
machinery `tests/e2e/slice`'s own suite already built, and this repository's convention is to
import it, never re-derive a second, divergent copy. `airflow_metadata_connection` (added by
11-10-PLAN.md, `test_oom.py`/`test_task_timeout.py`) is the one addition beyond what 11-09-PLAN.md
originally needed: both of those tests must directly query `task_instance.state` on the real
Airflow metadata database, the only DB-queryable proof that a killed/timed-out task genuinely
reached a clean terminal state.

`cnpg_hibernation_fault` is the ONE new fixture 11-09-PLAN.md's own Task 1 action text added. It is
deliberately NOT the `network_fault`-via-`NetworkPolicy` mechanism the plan originally specified —
see its own docstring for why, and this module's `_NETWORK_POLICY_ENFORCEMENT_FINDING` constant for
the live evidence.
"""

from __future__ import annotations

import contextlib
import time
from typing import TYPE_CHECKING

import pytest

from tests.e2e.cluster.conftest import (  # noqa: F401 -- re-exported as pytest fixtures below
    _require_cluster,
    cluster_name,
    kubectl,
    kubectl_context,
    kubectl_json,
    repo_root,
    s3_client,
)
from tests.e2e.slice.conftest import (  # noqa: F401 -- re-exported as pytest fixtures below
    _unpause_slice_dags,
    airflow_metadata_connection,
    analytics_connection,
    analytics_owner_connection,
    slice_fixtures_dir,
)
from tests.e2e.vault.conftest import (
    vault_addr,  # noqa: F401 -- re-exported as a pytest fixture below
)

if TYPE_CHECKING:
    import subprocess
    from collections.abc import Callable, Iterator

# Live finding, 11-09-PLAN.md Task 1 execution (2026-08-22): this cluster's CNI
# (`kindest/kindnetd:v20260528-9350166c`, per `kind/cluster.yaml`) does NOT enforce Kubernetes
# NetworkPolicy. Verified directly against the real cluster, twice: (1) a NetworkPolicy denying
# egress from every pod in `etl` to the analytical CNPG cluster's pod IP (an `ipBlock`/`except`
# rule) had ZERO effect -- a probe pod's `curl` to `analytics-db-rw.data.svc.cluster.local:5432`
# completed in ~3ms, identical to the pre-policy baseline; (2) a BLANKET deny-all-egress policy
# (`policyTypes: [Egress]`, empty `egress: []`) selecting every pod in the namespace ALSO had
# zero effect -- the identical ~3ms round-trip. NetworkPolicy objects are accepted and stored by
# the API server (it is a built-in resource, not a CRD needing an admission controller to
# validate), but nothing on this cluster actually enforces them at the packet level. This is a
# genuine platform constraint, not a policy-authoring mistake (the object was confirmed present
# and correctly scoped via `kubectl get networkpolicy -o yaml` both times) -- kept here as a
# permanent record so a future chaos scenario (plan 11-10) does not repeat the same dead end.
_NETWORK_POLICY_ENFORCEMENT_FINDING = (
    "kindnetd on this cluster does not enforce NetworkPolicy -- see this constant's own comment"
)

# Live finding, this same Task 1 execution, discovered via this fixture's own required
# throwaway-failing-assertion acceptance-criteria proof (11-09-PLAN.md Task 1): a bare
# `kubectl wait --for=condition=Ready pod -l <selector>` only evaluates pods that ALREADY EXIST
# at the moment it is invoked -- it does not wait for a not-yet-created pod to appear. Verified
# directly: `kubectl wait` against a label selector matching zero pods exits 1 immediately
# ("error: no matching resources found"), it does not block. Immediately after removing the
# `cnpg.io/hibernation` annotation, CNPG's own reconcile loop needs a moment to actually
# terminate the old instance pod and create a fresh one; a single `kubectl wait` call issued in
# that narrow window can race one of two ways -- hitting zero matching pods (a hard, immediate
# failure) or, if the reconcile loop had not yet started tearing down the ORIGINAL pod (which,
# for a fault held only briefly, may still be genuinely Ready and not yet even touched), a
# false-positive immediate success against a pod this fixture never actually confirmed survived
# the fault/recovery cycle. `_poll_all_pods_ready` below replaces the single-shot `kubectl wait`
# with a real poll loop (this codebase's own established `deadline = time.monotonic() + timeout`
# idiom, e.g. `tests/e2e/slice/conftest.py`'s pollers) that keeps retrying -- tolerating a
# transient zero-match or mid-recycle window -- until every CURRENTLY matching pod is genuinely
# Ready, which is what "guaranteed restoration" (T-11-22) actually requires.
_POD_READY_POLL_INTERVAL_SECONDS = 2.0


def _poll_all_pods_ready(
    kubectl_fn: Callable[..., subprocess.CompletedProcess[str]],
    *,
    namespace: str,
    label_selector: str,
    timeout: float,
) -> None:
    """Poll until every pod matching `label_selector` reports `Ready=True` (see comment above).

    Args:
        kubectl_fn: The `kubectl` fixture callable.
        namespace: The namespace to query.
        label_selector: The `-l` selector identifying the pod(s) to watch.
        timeout: Maximum seconds to wait.

    Raises:
        AssertionError: `timeout` elapses without observing at least one pod, all Ready.
    """
    deadline = time.monotonic() + timeout
    last_seen = "no pods observed yet"
    # `-o` requires the `jsonpath=` FORMAT PREFIX, not a bare template string -- verified live:
    # a bare template (no prefix) fails every single invocation with "unable to match a printer
    # suitable for the output format ..." (exit 1), which this poller's own `proc.returncode == 0`
    # guard would otherwise silently treat as just "not ready yet" and keep retrying, masking a
    # deterministic query bug as a transient timeout for the ENTIRE `timeout` window. Caught by
    # this fixture's own required throwaway-failing-assertion proof (11-09-PLAN.md Task 1).
    jsonpath = (
        "jsonpath={range .items[*]}{.metadata.name}="
        '{.status.conditions[?(@.type=="Ready")].status};{end}'
    )
    while time.monotonic() < deadline:
        proc = kubectl_fn("-n", namespace, "get", "pods", "-l", label_selector, "-o", jsonpath)
        if proc.returncode == 0:
            entries = [entry for entry in proc.stdout.strip().split(";") if entry]
            last_seen = "; ".join(entries) if entries else "0 matching pods"
            if entries and all(entry.endswith("=True") for entry in entries):
                return
        else:
            # A non-zero exit is a QUERY failure (bad jsonpath, wrong context, ...), never a
            # legitimate "not ready yet" -- surfaced distinctly so it is never mistaken for
            # ordinary in-progress recovery in the failure message below.
            last_seen = f"kubectl query itself failed (exit {proc.returncode}): {proc.stderr}"
        time.sleep(_POD_READY_POLL_INTERVAL_SECONDS)
    msg = (
        f"pods matching -l {label_selector!r} in {namespace!r} never all reported Ready within "
        f"{timeout}s (last observed: {last_seen})"
    )
    raise AssertionError(msg)


@pytest.fixture
def cnpg_hibernation_fault(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],  # noqa: F811 -- pytest fixture-injection param name, not a real redefinition
) -> Callable[..., contextlib.AbstractContextManager[None]]:
    """Factory fixture: a transient CNPG hibernation fault, guaranteed reversed (T-11-22).

    Call as `with cnpg_hibernation_fault(namespace="data", cluster="analytics-db"):` — the
    genuinely-live-verified substitute for the plan's originally-specified NetworkPolicy
    mechanism (see `_NETWORK_POLICY_ENFORCEMENT_FINDING` above: confirmed non-functional on
    this cluster, twice, including with a blanket deny-all-egress policy).

    `cnpg.io/hibernation: "on"` is a CNPG-native annotation on the `Cluster` CR (live-verified
    this session against the installed CloudNativePG `1.30.0` operator): the operator terminates
    every instance pod while retaining its PVC untouched. This is at least as faithful a
    reproduction of "database unavailable" as a network partition would have been — the
    analytical cluster's Service (`analytics-db-rw`) genuinely has zero backing endpoints while
    hibernated (`kubectl get endpoints analytics-db-rw` returns an empty `subsets`), so a
    connection attempt gets a real, immediate `ECONNREFUSED` from kube-proxy's own
    empty-endpoints handling — live-measured via a probe pod: `curl` failed in ~1ms with
    "Failed to connect ... Could not connect to server", a CLEANER signature than a
    NetworkPolicy DROP would have produced (which would have caused a silent multi-minute TCP
    SYN-retry timeout instead of an immediate, attributable refusal).

    Reversal removes the annotation entirely (`cnpg.io/hibernation-`, not merely setting it to
    `"off"`) so the `Cluster` CR's own annotations end up byte-identical to their pre-fault
    state, then POLLS (`_poll_all_pods_ready`, not a single `kubectl wait` call — see that
    function's own module-level comment for the live-verified race a bare `kubectl wait` has
    here) until the SAME PVC's pod is Ready again — live-verified: ~20s from annotation removal
    to `Ready` for a fault held longer than CNPG's own reconcile latency, zero data loss (bound
    to the identical `pvc-...` volume throughout). This wait is the fixture's own "guaranteed
    restoration" claim made concrete, matching `test_minio_unavailable.py`'s equivalent "wait
    for the pod to become Ready again" requirement — a caller never observes a false-Ready
    window where the annotation is gone but the database genuinely is not back yet.

    Args:
        kubectl: The `kubectl` fixture callable (this module's own re-export).

    Returns:
        A callable `_fault(*, namespace: str, cluster: str)` — call it to get a context manager;
        entering applies the fault, exiting (even via an exception) removes it and waits for
        the cluster's pod to be Ready again.
    """

    @contextlib.contextmanager
    def _fault(*, namespace: str, cluster: str) -> Iterator[None]:
        annotate_on = kubectl(
            "-n",
            namespace,
            "annotate",
            "cluster",
            cluster,
            "cnpg.io/hibernation=on",
            "--overwrite",
        )
        assert annotate_on.returncode == 0, (
            f"kubectl annotate cluster {cluster!r} -n {namespace!r} cnpg.io/hibernation=on "
            f"failed (exit {annotate_on.returncode}):\n{annotate_on.stderr}"
        )
        try:
            yield
        finally:
            remove = kubectl(
                "-n",
                namespace,
                "annotate",
                "cluster",
                cluster,
                "cnpg.io/hibernation-",
            )
            assert remove.returncode == 0, (
                f"kubectl annotate cluster {cluster!r} -n {namespace!r} cnpg.io/hibernation- "
                f"(removal) failed (exit {remove.returncode}):\n{remove.stderr}"
            )
            _poll_all_pods_ready(
                kubectl,
                namespace=namespace,
                label_selector=f"cnpg.io/cluster={cluster}",
                timeout=180,
            )

    return _fault
