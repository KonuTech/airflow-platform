"""tests/e2e/chaos/test_vault_unavailable.py — QUAL-15 scenario 4: Vault sealed, then unsealed.

Deliberately reproduces the exact real incident already diagnosed at
`.planning/debug/resolved/wait-for-files-stuck-task.md`: `vault-0` sealing makes `VaultBackend`
unable to resolve the `minio_default` connection `wait_for_files` (`S3KeySensor(aws_conn_id=
"minio_default", deferrable=True)`) needs, surfacing as a connection-resolution failure that
exhausts the sensor's own retry budget — a `TaskInstance` transitioning to `up_for_retry`, never
a crash. That incident's own root-cause evidence (`.planning/debug/resolved/wait-for-files-
stuck-task.md`'s "Evidence" section) was a live `GET /execution/connections/minio_default ...
status_code=404` on the `airflow-api-server`'s own HTTP access log, immediately followed by the
TaskInstance's own state transition — this test reproduces the SAME `TaskInstance` state
transition live, on the SAME sensor, for the SAME underlying reason (Vault sealed), and recovers
via the SAME documented fix: `scripts/vault-unseal.py` (what `make vault-unseal` wraps).

Sealing mechanism: `kubectl delete pod vault-0`, NOT `vault operator seal` -- live-verified this
session (2026-08-23) that a bare `kubectl exec vault-0 -- vault operator seal` fails with a
403 ("permission denied"): the CLI inside the pod has no `VAULT_TOKEN` set, so the request is
unauthenticated and `sys/seal` requires a real token. `tests/e2e/vault/
test_unseal_survives_restart.py` (already read in full) already established the CORRECT,
already-proven-working pattern for this exact repository: delete `vault-0` and wait for the
StatefulSet-recreated pod to reach `phase=Running` (never `condition=Ready` -- Vault's own
readinessProbe fails while sealed). This is also a MORE faithful reproduction of the real
incident than an explicit `operator seal` call would have been: D-02's own design note (single-
key Shamir + file storage, no auto-unseal) states a pod/host restart is exactly what reseals
Vault in production use of this platform -- `.planning/debug/resolved/wait-for-files-stuck-
task.md`'s own root cause was literally "vault-0 restarted... and came back Sealed", not an
operator running `vault operator seal` by hand. No `hvac`/root-token authentication is needed
anywhere in this file: `vault status` (used to confirm the post-unseal state) is Vault's own
public, unauthenticated endpoint -- verified live via `kubectl exec vault-0 -- vault status`
succeeding with no token configured, both before and after this fix.

Targets `csv_ingest_orders`/`orders` via manual trigger — see `test_pod_crash.py`'s own module
docstring for the full live-cluster reasoning behind this DAG choice. `wait_for_files` itself
(not `discover`) is the deliberate target here: it is the FIRST task in the graph, and the ONE
Airflow resolves a Vault-backed connection for before any pod launches — exactly matching the
original incident's own failure point.
"""

from __future__ import annotations

import contextlib
import random
import subprocess
import sys
import time
import uuid
from typing import TYPE_CHECKING, Any

import pytest

from tests.e2e.slice.conftest import poll_file_discovered, poll_ingestion_run, poll_run_for_file

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    import psycopg

pytestmark = [pytest.mark.cluster, pytest.mark.chaos]

_ORDERS_DAG_ID = "csv_ingest_orders"
_ORDERS_DATASET = "orders"
_VAULT_NAMESPACE = "vault"
_VAULT_POD = "vault-0"

# `test_unseal_survives_restart.py`'s own already-live-verified figure for how long a
# StatefulSet-recreated vault-0 pod takes to reach `phase=Running` again (never `condition=Ready`
# -- Vault's own readinessProbe fails while sealed, exactly the state this test needs).
_POD_RESTART_TIMEOUT_SECONDS = "180s"

# Disjoint from every sibling chaos/slice module's own order_id range — see test_pod_crash.py's
# own comment for why this matters on a shared, concurrently-active cluster.
_ORDER_ID_LOW = 2_600_000_000
_ORDER_ID_HIGH = 2_700_000_000
_ROW_COUNT = 20

# `wait_for_files` (`poke_interval=30`) should attempt its first connection resolution well
# within this window once sealed -- the incident's own evidence shows the 404 landing well
# under a second after the poke, so this budgets for scheduler/triggerer dispatch latency, not
# for any Vault-side slowness.
_WAIT_FOR_FILES_UP_FOR_RETRY_TIMEOUT_SECONDS = 120

# `wait_for_files`'s retry_delay is 5 minutes with retry_exponential_backoff=True (same base as
# every other task's default in this codebase -- no `default_args` override in either DAG file).
# Recovery therefore waits out Airflow's own backoff timer PLUS whatever `dbt_build`'s own
# independent KubernetesJobWatcher request-timeout race adds on top: live-observed this session
# (unrelated to this test's own fault) to hit BOTH of its own allowed retries in a single run
# (~20-25min total across three real attempts), pushing a 1800s budget to time out by mere
# moments while `publish` was already running -- see `test_pod_crash.py`'s own
# `_RUN_TERMINAL_TIMEOUT_SECONDS` comment for the full live evidence. 3600s gives the full
# exhaustion sequence generous headroom on top of `wait_for_files`'s own recovery wait.
_RECOVERY_TIMEOUT_SECONDS = 3600

_TASK_POLL_INTERVAL_SECONDS = 2.0


def _existing_customer_ids(conn: psycopg.Connection[Any], *, count: int) -> list[int]:
    """Return `count` genuinely-present `normalized.customers.customer_id` values.

    Identical shape to `test_referential_orphan.py`'s own helper of the same name.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id FROM normalized.customers LIMIT %s", (count,))
        rows = cur.fetchall()
    return [int(row[0]) for row in rows]


def _build_orders_csv(*, order_ids: list[int], customer_ids: list[int]) -> bytes:
    """Build a minimal `orders.yaml`-shaped CSV: header + one valid row per (order_id, customer_id).

    Identical shape to `test_pod_crash.py`'s own helper of the same name.
    """
    lines = ["order_id,customer_id,order_date,amount"]
    lines.extend(
        f"{order_id},{customer_id},2026-01-15,199.99"
        for order_id, customer_id in zip(order_ids, customer_ids, strict=True)
    )
    return ("\n".join(lines) + "\n").encode("utf-8")


def _poll_task_instance_state(  # noqa: PLR0913 -- one keyword per genuinely distinct input; see test_pod_kill_retry.py's own _write_u3_spike_doc precedent for the same carve-out reasoning
    kubectl_fn: Callable[..., subprocess.CompletedProcess[str]],
    *,
    dag_id: str,
    run_id: str,
    task_id: str,
    want_states: tuple[str, ...],
    timeout: float,
) -> str:
    """Poll a specific TaskInstance's state via a read-only query against the Airflow metadata DB.

    Duplicated from `test_minio_unavailable.py`'s own helper of the same name (this
    repository's established convention: small helpers are copied per test tier/file, not
    shared through a library module).

    Args:
        kubectl_fn: The `kubectl` fixture callable.
        dag_id: The DAG to query.
        run_id: The specific DagRun's `run_id`.
        task_id: The specific task to watch.
        want_states: Any state in this tuple is accepted as a match.
        timeout: Maximum seconds to wait.

    Returns:
        The first matching state observed.

    Raises:
        AssertionError: `timeout` elapses first — names the last-observed state.
    """
    script = (
        "from airflow.models import TaskInstance\n"
        "from airflow.utils.session import create_session\n"
        "with create_session() as session:\n"
        "    ti = session.query(TaskInstance).filter(\n"
        f"        TaskInstance.dag_id=='{dag_id}',\n"
        f"        TaskInstance.run_id=='{run_id}',\n"
        f"        TaskInstance.task_id=='{task_id}',\n"
        "    ).first()\n"
        "    print(ti.state if ti else 'NONE')\n"
    )
    deadline = time.monotonic() + timeout
    last_state = "NONE"
    while time.monotonic() < deadline:
        proc = kubectl_fn(
            "-n",
            "airflow",
            "exec",
            "deploy/airflow-scheduler",
            "-c",
            "scheduler",
            "--",
            "python3",
            "-c",
            script,
            timeout=30,
        )
        if proc.returncode == 0:
            last_state = proc.stdout.strip()
            if last_state in want_states:
                return last_state
        time.sleep(_TASK_POLL_INTERVAL_SECONDS)
    msg = (
        f"{dag_id}/{run_id}/{task_id} never reached any of {want_states!r} within {timeout}s "
        f"(last observed: {last_state!r})"
    )
    raise AssertionError(msg)


def _run_vault_unseal_script(repo_root: Path) -> subprocess.CompletedProcess[str]:
    """Run `scripts/vault-unseal.py` exactly as `make vault-unseal` invokes it.

    Same shape as `test_unseal_survives_restart.py`'s own invocation.
    """
    unseal_script = repo_root / "scripts" / "vault-unseal.py"
    return subprocess.run(  # noqa: S603
        [sys.executable, str(unseal_script)],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )


def test_vault_sealed_stalls_wait_for_files_then_unseal_recovers(
    s3_client: Callable[[str], Any],
    analytics_connection: psycopg.Connection[Any],
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    repo_root: Path,
) -> None:
    """Sealing Vault stalls `wait_for_files` on a connection-resolution failure; unsealing recovers.

    Reproduces `.planning/debug/resolved/wait-for-files-stuck-task.md` live: (1) deleting
    `vault-0` (a real pod restart -- see module docstring for why this, not `vault operator
    seal`, is both the working AND the more faithful mechanism) reseals Vault, confirmed via
    `vault status`; with Vault sealed, a fresh `csv_ingest_orders` run's `wait_for_files`
    TaskInstance reaches `up_for_retry` — a connection-resolution failure exhausting its own
    retry attempt, never a crash — within a bounded window; (2) `scripts/vault-unseal.py` (the
    same script `make vault-unseal` wraps) restores service, confirmed via `vault status`
    reporting `Sealed=false` and NOT having re-initialized; (3) the SAME run recovers via
    Airflow's own automatic retry (no second upload, no manual DAG intervention) and reaches
    `SUCCEEDED`; (4) `normalized.orders` ends up with exactly this run's own row count. Vault is
    guaranteed left unsealed afterward even on assertion failure (a `finally` block re-running
    the unseal script unconditionally).
    """
    app = s3_client("app")
    admin = s3_client("admin")

    customer_ids = _existing_customer_ids(analytics_connection, count=_ROW_COUNT)
    assert len(customer_ids) == _ROW_COUNT, (
        f"normalized.customers has fewer than {_ROW_COUNT} rows on this live cluster -- this "
        f"test needs prior customers ingestion to have already happened"
    )
    order_id_base = random.SystemRandom().randint(_ORDER_ID_LOW, _ORDER_ID_HIGH)
    order_ids = [order_id_base + i for i in range(_ROW_COUNT)]
    payload = _build_orders_csv(order_ids=order_ids, customer_ids=customer_ids)

    marker = uuid.uuid4().hex[:12]
    key = f"orders/e2e-chaos-vaultsealed-{marker}.csv"
    object_uri = f"s3://raw/{key}"
    run_id_marker = f"e2e-chaos-vaultsealed-{marker}"

    try:
        unpause = kubectl(
            "-n",
            "airflow",
            "exec",
            "deploy/airflow-api-server",
            "--",
            "airflow",
            "dags",
            "unpause",
            _ORDERS_DAG_ID,
        )
        assert unpause.returncode == 0, f"airflow dags unpause failed:\n{unpause.stderr}"

        app.put_object(Bucket="raw", Key=key, Body=payload)

        # Reseal by deleting the pod (see module docstring: `vault operator seal` fails with a
        # live-verified 403 -- no VAULT_TOKEN is set for the bare CLI inside the pod -- and a
        # pod restart is the more faithful reproduction of the real incident anyway).
        delete_proc = kubectl("-n", _VAULT_NAMESPACE, "delete", "pod", _VAULT_POD)
        assert delete_proc.returncode == 0, (
            f"kubectl delete pod/{_VAULT_POD} -n {_VAULT_NAMESPACE} failed "
            f"(exit {delete_proc.returncode}):\n{delete_proc.stderr}"
        )
        wait_running = kubectl(
            "-n",
            _VAULT_NAMESPACE,
            "wait",
            "--for=jsonpath={.status.phase}=Running",
            f"--timeout={_POD_RESTART_TIMEOUT_SECONDS}",
            f"pod/{_VAULT_POD}",
            timeout=200,
        )
        assert wait_running.returncode == 0, (
            f"pod/{_VAULT_POD} -n {_VAULT_NAMESPACE} did not reach phase=Running within "
            f"{_POD_RESTART_TIMEOUT_SECONDS} of being deleted (exit "
            f"{wait_running.returncode}):\n{wait_running.stderr}"
        )
        seal_status = kubectl("-n", _VAULT_NAMESPACE, "exec", _VAULT_POD, "--", "vault", "status")
        sealed_after_restart = next(
            (line for line in seal_status.stdout.splitlines() if line.strip().startswith("Sealed")),
            "",
        )
        assert sealed_after_restart.split() == ["Sealed", "true"], (
            f"vault-0 did not report Sealed=true immediately after being recreated -- the "
            f"restart-then-reseal this test relies on (D-02) did not occur. Observed line: "
            f"{sealed_after_restart!r}. Full output:\n{seal_status.stdout}"
        )

        trigger = kubectl(
            "-n",
            "airflow",
            "exec",
            "deploy/airflow-api-server",
            "--",
            "airflow",
            "dags",
            "trigger",
            _ORDERS_DAG_ID,
            "--run-id",
            run_id_marker,
        )
        assert trigger.returncode == 0, f"airflow dags trigger failed:\n{trigger.stderr}"

        observed_state = _poll_task_instance_state(
            kubectl,
            dag_id=_ORDERS_DAG_ID,
            run_id=run_id_marker,
            task_id="wait_for_files",
            want_states=("up_for_retry", "failed"),
            timeout=_WAIT_FOR_FILES_UP_FOR_RETRY_TIMEOUT_SECONDS,
        )
        assert observed_state in ("up_for_retry", "failed"), (
            f"wait_for_files reached an unexpected state {observed_state!r} while Vault was "
            f"sealed -- expected a connection-resolution failure (up_for_retry/failed), not a "
            f"silent hang or an unrelated crash"
        )

        unseal_proc = _run_vault_unseal_script(repo_root)
        assert unseal_proc.returncode == 0, (
            f"scripts/vault-unseal.py exited {unseal_proc.returncode}:\n"
            f"stdout={unseal_proc.stdout}\nstderr={unseal_proc.stderr}"
        )
        assert "initialized" not in unseal_proc.stdout, (
            "scripts/vault-unseal.py re-initialized an already-initialized Vault instead of "
            f"taking the unseal-only path:\n{unseal_proc.stdout}"
        )

        status = kubectl("-n", _VAULT_NAMESPACE, "exec", _VAULT_POD, "--", "vault", "status")
        sealed_line = next(
            (line for line in status.stdout.splitlines() if line.strip().startswith("Sealed")),
            "",
        )
        assert sealed_line.split() == ["Sealed", "false"], (
            f"vault status did not report 'Sealed false' after scripts/vault-unseal.py exited "
            f"0 -- observed line: {sealed_line!r}. Full output:\n{status.stdout}"
        )

        file_row = poll_file_discovered(
            analytics_connection,
            dataset=_ORDERS_DATASET,
            object_uri=object_uri,
            timeout=_RECOVERY_TIMEOUT_SECONDS,
        )
        assert file_row["duplicate_of_file_id"] is None, (
            f"the freshly-marked fixture was already flagged a duplicate of file_id="
            f"{file_row['duplicate_of_file_id']!r} -- the uuid marker did not make this "
            f"content genuinely new"
        )

        run_row = poll_run_for_file(analytics_connection, file_id=file_row["file_id"], timeout=60)

        outcome = poll_ingestion_run(
            analytics_connection,
            run_row["idempotency_key"],
            timeout=_RECOVERY_TIMEOUT_SECONDS,
        )
        assert outcome["status"] == "SUCCEEDED", (
            f"after unsealing Vault, run {run_row['idempotency_key']!r} finished "
            f"{outcome['status']!r}, not SUCCEEDED"
        )

        with analytics_connection.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM normalized.orders WHERE order_id BETWEEN %s AND %s",
                (order_ids[0], order_ids[-1]),
            )
            row = cur.fetchone()
            assert row is not None
            total = row[0]
        assert total == _ROW_COUNT, (
            f"expected exactly {_ROW_COUNT} rows in this run's own order_id window "
            f"[{order_ids[0]}, {order_ids[-1]}], found {total} -- a value below means rows "
            f"went missing after the Vault-unavailable recovery; a value above means "
            f"duplication"
        )
    finally:
        # Unconditional, regardless of whether the test body ever reached its own unseal call
        # or raised before getting there -- this scenario's own equivalent of every other
        # module's `finally`-guaranteed-restoration fixture, inlined here (Task 2's own file
        # scope: conftest.py is not modified by this task).
        with contextlib.suppress(Exception):
            _run_vault_unseal_script(repo_root)
        with contextlib.suppress(Exception):
            admin.delete_object(Bucket="raw", Key=key)
