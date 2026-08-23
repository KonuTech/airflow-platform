"""tests/e2e/chaos/test_oom.py — QUAL-15 scenario: a publish pod OOMKills cleanly, no corrupted
state.

Live reproduction of the known publish-task OOM class: `airflow/dags/csv_ingest_customers.py`'s own
`publish` task carries a permanent code comment (10-07-PLAN.md) recording a REAL, live-observed
`OOMKilled` (exit code 137) at a 256Mi memory limit, root-caused to `SCDPublisher`'s Step C
recomputing each touched key's FULL bronze history in memory (Phase 10's SCD implementation). That
production task's own resources were permanently RAISED after the fix (now `_STAGE_RESOURCES`,
4Gi) — this test must never edit `csv_ingest_customers.py` to reintroduce the undersized limit
(that would revert a real fix and destabilise the platform's one continuously-scheduled customers
pipeline). Instead it targets `chaos_probe_oom_publish_customers`
(`airflow/dags/chaos_probe.py`, added by this same plan): one throwaway, never-scheduled DAG
running the identical `dataplat publish --dataset customers` command at the EXACT undersized 256Mi
limit the original bug used — a permanent, live-triggerable regression proof for that finding,
without ever touching the production DAG file. See `chaos_probe.py`'s own module docstring for why
a dedicated probe DAG is the only way to get a real, DB-queryable Airflow task instance under a
custom resource limit (`airflow tasks test` explicitly does not persist task-instance state).

`publish --dataset customers` is dataset-wide (11-09-SUMMARY.md: "no run-id scoping in its CLI
arguments") -- it processes whatever this live cluster's `customers` dataset genuinely has in
`STAGED` status at trigger time, not merely rows this test itself uploaded. This is a deliberate,
honest characteristic, not a test defect: the ORIGINAL bug itself was found "during [10-07's] own
live sweep" against real, already-staged production-scale data, not a synthetic fixture -- a tiny
synthetic-only payload's own "touched keys" would have a trivially small bronze history and would
not reliably reproduce the SAME memory-pressure shape. This test's own "zero corrupted rows"
assertion is therefore scoped precisely to what ITS OWN triggered pod actually claimed (joined via
`meta.ingestion_runs.k8s_pod_name`, set by `claim_ingestion_run` the instant a run is claimed) --
concurrency-safe and immune to any OTHER legitimate publish activity happening elsewhere on this
shared cluster at the same time, unlike a naive total-row-count diff would be.
"""

from __future__ import annotations

import contextlib
import time
import uuid
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    import subprocess
    from collections.abc import Callable

    import psycopg

pytestmark = [pytest.mark.cluster, pytest.mark.chaos]

_OOM_DAG_ID = "chaos_probe_oom_publish_customers"
_OOM_TASK_ID = "publish_customers_undersized_memory_probe"
_ETL_NAMESPACE = "etl"
_POD_LABEL_SELECTOR = f"dag_id={_OOM_DAG_ID},task_id={_OOM_TASK_ID}"

# Generous under this cluster's own documented, live-observed CPU-contention latency (project
# memory: kubectl/Airflow CLI round-trips have taken minutes, not seconds, under a heavy backlog).
_POD_APPEAR_TIMEOUT_SECONDS = 300
_POD_TERMINATED_TIMEOUT_SECONDS = 300
_TASK_INSTANCE_TERMINAL_TIMEOUT_SECONDS = 600
_POLL_INTERVAL_SECONDS = 2.0


def _poll_pod_name(
    kubectl_fn: Callable[..., subprocess.CompletedProcess[str]],
    *,
    timeout: float,
) -> str:
    """Poll for the real probe pod's name via its dag_id/task_id labels (KPO auto-labels every
    pod)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        proc = kubectl_fn(
            "-n",
            _ETL_NAMESPACE,
            "get",
            "pods",
            "-l",
            _POD_LABEL_SELECTOR,
            "-o",
            "jsonpath={.items[0].metadata.name}",
        )
        if proc.returncode == 0 and proc.stdout.strip():
            return proc.stdout.strip()
        time.sleep(_POLL_INTERVAL_SECONDS)
    msg = (
        f"no pod matching -l {_POD_LABEL_SELECTOR!r} appeared in namespace {_ETL_NAMESPACE!r} "
        f"within {timeout}s"
    )
    raise AssertionError(msg)


def _poll_oomkilled_reason(
    kubectl_fn: Callable[..., subprocess.CompletedProcess[str]],
    *,
    pod_name: str,
    timeout: float,
) -> str:
    """Poll the pod's container status (current OR last) for a `terminated.reason`, any reason.

    Checks BOTH `.status.containerStatuses[0].state.terminated.reason` (the pod may still be in
    this exact terminal state when observed) AND `.lastState.terminated.reason` (Kubernetes may
    already be restarting/replacing the container's reported state by the time this polls,
    depending on `restartPolicy` -- KPO pods default to `Never`, so `state.terminated` should
    persist until `on_finish_action: delete_pod` removes the pod entirely, but both are checked for
    robustness against exactly when this test happens to observe it).

    Returns:
        The observed `terminated.reason` string (expected `"OOMKilled"`).

    Raises:
        AssertionError: `timeout` elapses with the pod never reporting a `terminated` reason at
            all (from either field) -- most likely the pod was deleted before this could observe
            it, or genuinely never terminated.
    """
    deadline = time.monotonic() + timeout
    last_query_output = "no query ever returned a non-empty reason"
    while time.monotonic() < deadline:
        for jsonpath in (
            "jsonpath={.status.containerStatuses[0].state.terminated.reason}",
            "jsonpath={.status.containerStatuses[0].lastState.terminated.reason}",
        ):
            proc = kubectl_fn("-n", _ETL_NAMESPACE, "get", "pod", pod_name, "-o", jsonpath)
            if proc.returncode == 0 and proc.stdout.strip():
                return proc.stdout.strip()
            last_query_output = (
                f"exit={proc.returncode} stdout={proc.stdout!r} stderr={proc.stderr!r}"
            )
        time.sleep(_POLL_INTERVAL_SECONDS)
    msg = (
        f"pod {pod_name!r} in {_ETL_NAMESPACE!r} never reported a container terminated.reason "
        f"(current or last) within {timeout}s (last query: {last_query_output})"
    )
    raise AssertionError(msg)


def _poll_task_instance_terminal_state(
    conn: psycopg.Connection[Any],
    *,
    dag_id: str,
    task_id: str,
    run_id: str,
    timeout: float,
) -> str:
    """Poll `task_instance.state` for `(dag_id, task_id, run_id)` until it reaches a terminal
    state."""
    terminal = {"success", "failed", "up_for_retry", "upstream_failed", "skipped"}
    deadline = time.monotonic() + timeout
    last_state: str | None = None
    while time.monotonic() < deadline:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT state FROM task_instance "
                "WHERE dag_id = %s AND task_id = %s AND run_id = %s",
                (dag_id, task_id, run_id),
            )
            row = cur.fetchone()
        last_state = None if row is None else str(row[0])
        if last_state in terminal:
            return last_state
        time.sleep(_POLL_INTERVAL_SECONDS)
    msg = (
        f"task_instance[dag_id={dag_id!r}, task_id={task_id!r}, run_id={run_id!r}] never reached a "
        f"terminal state within {timeout}s (last observed: {last_state!r})"
    )
    raise AssertionError(msg)


def test_oom_pod_dies_cleanly_and_leaves_no_partial_published_rows(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    analytics_owner_connection: psycopg.Connection[Any],
    airflow_metadata_connection: psycopg.Connection[Any],
) -> None:
    """ORCH-04/META-03, live: a real 256Mi OOMKill leaves the Airflow task failed, no run wrongly
    SUCCEEDED.

    Triggers `chaos_probe_oom_publish_customers`, confirms the real pod's container was genuinely
    `OOMKilled`, confirms the Airflow task instance reaches a clean terminal state (`failed`, since
    this probe task's own `retries=0`), and — the acceptance-critical assertion — confirms NO
    `meta.ingestion_runs` row this specific pod claimed (`k8s_pod_name` = the pod's own name) was
    ever left in a corrupted state: every such row must show `status != 'SUCCEEDED'` (an OOMKilled
    process cannot run any commit/cleanup code — `os.kill(SIGKILL)` gives it zero chance to update
    its own claimed row past whatever `claim_ingestion_run` itself already wrote, so any claimed
    row must still show `status='RUNNING'`, never a wrongly-committed `SUCCEEDED`).
    """
    run_id_marker = f"e2e-chaos-oom-{uuid.uuid4().hex[:12]}"

    unpause = kubectl(
        "-n",
        "airflow",
        "exec",
        "deploy/airflow-api-server",
        "--",
        "airflow",
        "dags",
        "unpause",
        _OOM_DAG_ID,
    )
    assert unpause.returncode == 0, f"airflow dags unpause failed:\n{unpause.stderr}"

    trigger = kubectl(
        "-n",
        "airflow",
        "exec",
        "deploy/airflow-api-server",
        "--",
        "airflow",
        "dags",
        "trigger",
        _OOM_DAG_ID,
        "--run-id",
        run_id_marker,
    )
    assert trigger.returncode == 0, f"airflow dags trigger failed:\n{trigger.stderr}"

    pod_name = _poll_pod_name(kubectl, timeout=_POD_APPEAR_TIMEOUT_SECONDS)

    reason = _poll_oomkilled_reason(
        kubectl, pod_name=pod_name, timeout=_POD_TERMINATED_TIMEOUT_SECONDS
    )
    assert reason == "OOMKilled", (
        f"expected pod {pod_name!r}'s container to terminate with reason='OOMKilled', got "
        f"{reason!r} -- if this is a genuine, different failure, the 256Mi limit may no longer "
        f"be undersized for this cluster's current staged-data volume; check "
        f"`kubectl logs {pod_name} -n etl`"
    )

    task_state = _poll_task_instance_terminal_state(
        airflow_metadata_connection,
        dag_id=_OOM_DAG_ID,
        task_id=_OOM_TASK_ID,
        run_id=run_id_marker,
        timeout=_TASK_INSTANCE_TERMINAL_TIMEOUT_SECONDS,
    )
    assert task_state == "failed", (
        f"expected the Airflow task instance to reach a clean 'failed' state (retries=0), got "
        f"{task_state!r} -- a stuck/hanging state here would be the real regression this test "
        f"guards against"
    )

    with contextlib.suppress(Exception):
        # Best-effort: a killed pod is already gone once on_finish_action=delete_pod fires; this
        # is not the test's own correctness signal, only cleanup of anything left behind.
        kubectl(
            "-n",
            _ETL_NAMESPACE,
            "delete",
            "pod",
            pod_name,
            "--wait=false",
            "--ignore-not-found=true",
        )

    with analytics_owner_connection.cursor() as cur:
        cur.execute(
            "SELECT run_id, status, rows_loaded FROM meta.ingestion_runs WHERE k8s_pod_name = %s",
            (pod_name,),
        )
        claimed_runs = cur.fetchall()

    for claimed_run_id, status, rows_loaded in claimed_runs:
        assert status != "SUCCEEDED", (
            f"run_id={claimed_run_id!r} (claimed by the OOMKilled pod {pod_name!r}) shows "
            f"status='SUCCEEDED' with rows_loaded={rows_loaded!r} -- an OOMKilled process cannot "
            f"have committed this itself; if SUCCEEDED, some OTHER process must have completed it "
            f"afterward, which is legitimate ONLY if it re-claimed via an expired lease, never a "
            f"silent partial commit from the killed attempt itself"
        )
        if status == "RUNNING":
            assert rows_loaded is None, (
                f"run_id={claimed_run_id!r} is still 'RUNNING' (expected, post-OOMKill) but "
                f"already shows a non-NULL rows_loaded={rows_loaded!r} -- a partial write of the "
                f"summary row itself, violating the all-or-nothing publish guarantee (META-03)"
            )
