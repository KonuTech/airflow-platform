"""tests/e2e/chaos/test_task_timeout.py — QUAL-15 scenario: a task exceeding execution_timeout fails
cleanly.

Targets `chaos_probe_timeout_publish_customers` (`airflow/dags/chaos_probe.py`, added by this same
plan): one throwaway, never-scheduled DAG running `dataplat publish --dataset customers` with a
deliberately tiny `execution_timeout=5s` and `retries=1` -- the interfaces block's own instruction
(11-10-PLAN.md): "target `execution_timeout` (the KPO/task-level ceiling on how long a task may
run) ... confirm Airflow marks it failed (or retries, per that task's own declared `retries`)
rather than hanging indefinitely." A real dataset-wide `publish` sweep (Vault auth handshake + DB
connection + enumerating every `STAGED` run) reliably takes longer than 5 seconds, so this task's
own `execution_timeout` is expected to fire on BOTH attempts (`retries=1` means 2 total attempts),
proving both halves of the interfaces block's own requirement in one live run: the timeout firing
at all, and Airflow's own declared-retry semantics genuinely engaging afterward (`try_number`
reaching 2, not staying stuck at 1).

Same reasoning as `test_oom.py`'s own module docstring for why this targets a dedicated probe DAG
rather than `csv_ingest_customers.py` itself (backlog + `airflow tasks test`'s own documented
"does not record state in the database" limitation) — read that module's docstring first.
"""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    import subprocess
    from collections.abc import Callable

    import psycopg

pytestmark = [pytest.mark.cluster, pytest.mark.chaos]

_TIMEOUT_DAG_ID = "chaos_probe_timeout_publish_customers"
_TIMEOUT_TASK_ID = "publish_customers_tiny_timeout_probe"

_TASK_INSTANCE_TERMINAL_TIMEOUT_SECONDS = 600
_POLL_INTERVAL_SECONDS = 2.0
# A genuine `execution_timeout` firing on both of this task's 2 attempts should reach 'failed'
# within a couple of minutes (5s timeout + up to 10s retry_delay + pod-launch/kill overhead each
# attempt) -- generous headroom over that for this cluster's own documented CPU-contention
# latency, but still tight enough to distinguish "the timeout fired promptly" from "the task
# eventually failed for some unrelated, much slower reason".
_EXPECTED_MAX_WALL_CLOCK_SECONDS = 480


def _poll_task_instance(
    conn: psycopg.Connection[Any],
    *,
    dag_id: str,
    task_id: str,
    run_id: str,
    timeout: float,
) -> tuple[str, int | None]:
    """Poll `task_instance.(state, try_number)` for `(dag_id, task_id, run_id)` until terminal."""
    deadline = time.monotonic() + timeout
    last: tuple[str | None, int | None] = (None, None)
    while time.monotonic() < deadline:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT state, try_number FROM task_instance "
                "WHERE dag_id = %s AND task_id = %s AND run_id = %s",
                (dag_id, task_id, run_id),
            )
            row = cur.fetchone()
        if row is not None:
            last = (str(row[0]), None if row[1] is None else int(row[1]))
            # 'up_for_retry' is itself terminal for ONE attempt but not for the whole task -- only
            # stop polling once it reaches a state that will not itself transition again without
            # external action.
            if last[0] in ("success", "failed", "upstream_failed", "skipped"):
                return last[0], last[1]
        time.sleep(_POLL_INTERVAL_SECONDS)
    msg = (
        f"task_instance[dag_id={dag_id!r}, task_id={task_id!r}, run_id={run_id!r}] never reached a "
        f"final terminal state within {timeout}s (last observed: state={last[0]!r}, "
        f"try_number={last[1]!r})"
    )
    raise AssertionError(msg)


def test_execution_timeout_fires_and_retry_semantics_engage(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    airflow_metadata_connection: psycopg.Connection[Any],
) -> None:
    """ORCH-04, live: a task whose `execution_timeout` fires twice reaches `failed` at
    `try_number=2`.

    Triggers `chaos_probe_timeout_publish_customers`, polls `task_instance` until it reaches a
    final terminal state, and asserts: (1) the final state is `failed` (both of this task's 2
    attempts are expected to time out — a real publish sweep cannot complete in 5s); (2)
    `try_number == 2`, the direct, DB-queryable proof that Airflow's own declared `retries=1`
    semantics genuinely re-ran the task after the first `AirflowTaskTimeout`, rather than the task
    simply failing once and stopping; (3) the whole sequence completed well under the wall-clock
    bound a task that was genuinely HANGING (not timing out cleanly) would blow past — the
    regression this test exists to catch.
    """
    run_id_marker = f"e2e-chaos-timeout-{uuid.uuid4().hex[:12]}"

    unpause = kubectl(
        "-n",
        "airflow",
        "exec",
        "deploy/airflow-api-server",
        "--",
        "airflow",
        "dags",
        "unpause",
        _TIMEOUT_DAG_ID,
    )
    assert unpause.returncode == 0, f"airflow dags unpause failed:\n{unpause.stderr}"

    trigger_start = time.monotonic()
    trigger = kubectl(
        "-n",
        "airflow",
        "exec",
        "deploy/airflow-api-server",
        "--",
        "airflow",
        "dags",
        "trigger",
        _TIMEOUT_DAG_ID,
        "--run-id",
        run_id_marker,
    )
    assert trigger.returncode == 0, f"airflow dags trigger failed:\n{trigger.stderr}"

    state, try_number = _poll_task_instance(
        airflow_metadata_connection,
        dag_id=_TIMEOUT_DAG_ID,
        task_id=_TIMEOUT_TASK_ID,
        run_id=run_id_marker,
        timeout=_TASK_INSTANCE_TERMINAL_TIMEOUT_SECONDS,
    )
    elapsed = time.monotonic() - trigger_start

    assert state == "failed", (
        f"expected the task instance to reach a clean 'failed' state after its execution_timeout "
        f"fired on both attempts, got {state!r} (try_number={try_number!r}) -- a stuck/hanging "
        f"state here would be the real regression this test guards against"
    )
    assert try_number == 2, (
        f"expected try_number=2 (retries=1 means exactly 2 total attempts, both timing out), got "
        f"{try_number!r} -- a value of 1 would mean the retry never actually engaged"
    )
    assert elapsed < _EXPECTED_MAX_WALL_CLOCK_SECONDS, (
        f"the run took {elapsed:.1f}s to reach 'failed' -- expected well under "
        f"{_EXPECTED_MAX_WALL_CLOCK_SECONDS}s for a genuinely-firing 5s execution_timeout across 2 "
        f"attempts; a much longer duration suggests the timeout did not fire promptly and the task "
        f"instead ran to some other, slower failure mode"
    )
