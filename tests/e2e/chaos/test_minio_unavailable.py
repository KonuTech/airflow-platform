"""tests/e2e/chaos/test_minio_unavailable.py — QUAL-15 scenario 3: MinIO unreachable, then restored.

`kubectl -n data scale deployment/minio --replicas=0` — NOT `scale statefulset/minio`, despite
11-09-PLAN.md's original Task 2 wording. Live-verified this session (`kubectl -n data get
statefulset,deployment`): the official `minio/minio` chart's `mode: standalone`
(`helm/values/local/minio.yaml`) renders a **Deployment** named `minio`, not a StatefulSet — the
plan's own `<read_first>` cited that values file for "StatefulSet naming/namespace" without the
object kind itself ever having been live-verified. Restoration therefore waits on
`--for=condition=Available deployment/minio` (matching `scripts/stages/60-minio.sh`'s own
`wait_for_deploy_available data minio` — the SAME condition this platform's own deploy script
already uses for this exact object), not a StatefulSet `readyReplicas` condition.

Targets `csv_ingest_orders`/`orders` via manual trigger — see `test_pod_crash.py`'s own module
docstring for the full live-cluster reasoning behind this DAG choice.

Sequencing matters here specifically because `wait_for_files` (the `S3KeySensor`) ALSO needs
MinIO to poke the uploaded file's existence, and its own `retries=2`/5-minute `retry_delay`
would make a "scale MinIO down immediately after triggering" design race against — and
potentially get stuck behind — that sensor's own slow backoff, exactly the kind of
multi-minute-per-attempt wait this test does not need to exercise (the DB-unavailable test
already proves that class of "Airflow's own retry backoff" recovery). Instead, this test lets
`wait_for_files` succeed FIRST (file already uploaded, MinIO still up), THEN takes MinIO down —
`list_matched_keys` (`_common/integrity_gate.py`, a plain `@task` with no `retries` override,
i.e. Airflow's default `retries=0`) is the next task in the graph, and is the one genuinely
exercised here: a `boto3`/`botocore` call against a now-zero-endpoint MinIO Service fails on its
FIRST attempt, no retry backoff to wait out, propagating the whole DagRun to `failed` quickly.
Since `gate`/`discover` never ran in that failed attempt, `meta.files` has no row for the
uploaded object yet — recovery is therefore a genuinely FRESH second trigger of the same
never-consumed file, not the same DagRun's own automatic retry (this scenario's own honest
difference from `test_database_unavailable.py`'s recovery shape).
"""

from __future__ import annotations

import contextlib
import random
import time
import uuid
from typing import TYPE_CHECKING, Any

import pytest

from tests.e2e.slice.conftest import poll_file_discovered, poll_ingestion_run, poll_run_for_file

if TYPE_CHECKING:
    import subprocess
    from collections.abc import Callable

    import psycopg

pytestmark = [pytest.mark.cluster, pytest.mark.chaos]

_ORDERS_DAG_ID = "csv_ingest_orders"
_ORDERS_DATASET = "orders"
_MINIO_NAMESPACE = "data"
_MINIO_DEPLOYMENT = "minio"

# Disjoint from every sibling chaos/slice module's own order_id range — see test_pod_crash.py's
# own comment for why this matters on a shared, concurrently-active cluster.
_ORDER_ID_LOW = 2_400_000_000
_ORDER_ID_HIGH = 2_500_000_000
_ROW_COUNT = 20

# `wait_for_files` pokes every 30s (`poke_interval=30`); generous margin for scheduler-loop
# latency on top of that.
_WAIT_FOR_FILES_SUCCESS_TIMEOUT_SECONDS = 120

# `list_matched_keys` has no `retries` override (Airflow default `retries=0`) -- a single failed
# `boto3` call against a zero-endpoint MinIO Service, no backoff to wait out. Generous margin for
# scheduler-loop dispatch latency, not for any retry cycle.
_TASK_FAILURE_TIMEOUT_SECONDS = 120
# NOT just scheduler-loop latency margin, despite the name: live-discovered this session
# (2026-08-23, 11-09-PLAN.md Task 2 execution -- see deferred-items.md's Plan 11-09 entry for
# the full root-cause writeup) that `publish`'s only real gate is `mark_dbt_build_done`
# (`_common/run_stage_recorder.py`'s `wire_dbt_build_tracking`), NOT `stage`/`discover` directly
# -- so `publish` can start (and, with its own `retries=3`, independently retry-cycle on the
# same pre-existing KubernetesJobWatcher race documented throughout this suite) even though
# `list_matched_keys`/`gate`/`discover`/`stage` have ALL already reached a genuine, permanent
# `failed`/`upstream_failed` terminal state. Live-observed: every other task in the graph reached
# a terminal state within seconds of `list_matched_keys` failing, but the DagRun itself stayed
# `running` for multiple minutes waiting out `publish`'s own independent retry backoff before
# finally reaching `failed`. 900s (15 min) covers one such retry-backoff cycle with margin; it
# does not fully eliminate the (separately tracked, out-of-scope-to-fix-here) risk that `publish`
# needs more than one retry, the same way `_RECOVERY_TIMEOUT_SECONDS` below already accounts for.
_DAGRUN_FAILED_TIMEOUT_SECONDS = 900

# MinIO's own Deployment rollout back to Available -- live-observed elsewhere on this cluster to
# be well under a minute for a single-replica standalone Deployment with an already-resident
# image; generous margin regardless.
_MINIO_RESTORE_TIMEOUT_SECONDS = 180

# Also budgets for `dbt_build`'s own independent KubernetesJobWatcher request-timeout race
# (live-observed this session, unrelated to this test's own fault): it can hit BOTH of its own
# allowed retries in a single run (~20-25min total across three real attempts), pushing a 1800s
# budget to time out by mere moments while `publish` was already running -- see
# `test_pod_crash.py`'s own `_RUN_TERMINAL_TIMEOUT_SECONDS` comment for the full live evidence.
# Live-observed this session (2026-08-23, 11-09-PLAN.md Task 2 execution): under this cluster's
# own unusually heavy same-day contention, `publish` (retries=3) hit the SAME race on ALL FOUR of
# its own attempts, exhausting its own retries entirely (exponential backoff 5/10/20-plus-min
# gaps between attempts) -- a genuine DagRun failure, not a timeout artifact, taking ~36 minutes
# for `publish`'s own exhaustion sequence ALONE, on top of `dbt_build`'s own ~20-25min sequence,
# for a combined ~86 minutes observed end-to-end for one real run. 3600s (60 min) is not
# generous enough to cover this specific observed compounding; 5400s (90 min) is.
_RECOVERY_TIMEOUT_SECONDS = 5400

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

    Runs entirely inside the scheduler pod (`kubectl exec ... python3 -c "..."`) — a plain
    SQLAlchemy `SELECT`, no mutation, the same read-only diagnostic shape
    `.planning/debug/resolved/wait-for-files-stuck-task.md`'s own investigation used. This
    suite's other pollers query `meta.*` directly (a live `psycopg` connection is already a
    fixture); this one queries Airflow's OWN metadata DB instead, since a plain `@task`'s
    pass/fail state is not observable from the analytical database at all.

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


def _poll_dagrun_state(
    kubectl_fn: Callable[..., subprocess.CompletedProcess[str]],
    *,
    dag_id: str,
    run_id: str,
    want_states: tuple[str, ...],
    timeout: float,
) -> str:
    """Poll a specific DagRun's own state — same shape as `_poll_task_instance_state`."""
    script = (
        "from airflow.models import DagRun\n"
        "from airflow.utils.session import create_session\n"
        "with create_session() as session:\n"
        "    r = session.query(DagRun).filter(\n"
        f"        DagRun.dag_id=='{dag_id}', DagRun.run_id=='{run_id}',\n"
        "    ).first()\n"
        "    print(r.state if r else 'NONE')\n"
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
        f"{dag_id}/{run_id} never reached any of {want_states!r} within {timeout}s "
        f"(last observed: {last_state!r})"
    )
    raise AssertionError(msg)


def test_minio_unavailable_fails_the_run_clearly_then_recovers(  # noqa: PLR0915 -- linear two-phase (fail, then recover) live proof; splitting it would scatter one test's own narrative across helpers with no reuse value, same reasoning test_pod_kill_retry.py's own long test bodies already establish
    s3_client: Callable[[str], Any],
    analytics_connection: psycopg.Connection[Any],
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    """Scaling MinIO to zero replicas fails the run clearly; scaling back up recovers it.

    Confirms, in order: (1) with the file already uploaded and `wait_for_files` already
    successful, scaling MinIO to zero replicas fails `list_matched_keys` on its first attempt
    (no retry backoff — `retries=0`), and the whole DagRun reaches `failed` within a bounded
    window; (2) once MinIO is scaled back to one replica and reports `Available` again, a FRESH
    trigger of the SAME never-consumed file succeeds; (3) `normalized.orders` ends up with
    exactly this run's own row count — no duplicate, no missing row. MinIO's replica count is
    guaranteed restored to 1 in a `finally` block even if an assertion fails partway through.
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
    key = f"orders/e2e-chaos-miniodown-{marker}.csv"
    object_uri = f"s3://raw/{key}"
    first_run_id = f"e2e-chaos-miniodown-{marker}-1"
    second_run_id = f"e2e-chaos-miniodown-{marker}-2"

    minio_scaled_down = False
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

        trigger1 = kubectl(
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
            first_run_id,
        )
        assert trigger1.returncode == 0, f"airflow dags trigger failed:\n{trigger1.stderr}"

        _poll_task_instance_state(
            kubectl,
            dag_id=_ORDERS_DAG_ID,
            run_id=first_run_id,
            task_id="wait_for_files",
            want_states=("success",),
            timeout=_WAIT_FOR_FILES_SUCCESS_TIMEOUT_SECONDS,
        )

        scale_down = kubectl(
            "-n",
            _MINIO_NAMESPACE,
            "scale",
            f"deployment/{_MINIO_DEPLOYMENT}",
            "--replicas=0",
        )
        assert scale_down.returncode == 0, (
            f"kubectl scale deployment/{_MINIO_DEPLOYMENT} --replicas=0 failed "
            f"(exit {scale_down.returncode}):\n{scale_down.stderr}"
        )
        minio_scaled_down = True

        failed_task_state = _poll_task_instance_state(
            kubectl,
            dag_id=_ORDERS_DAG_ID,
            run_id=first_run_id,
            task_id="list_matched_keys",
            want_states=("failed", "up_for_retry"),
            timeout=_TASK_FAILURE_TIMEOUT_SECONDS,
        )
        assert failed_task_state in ("failed", "up_for_retry"), (
            f"list_matched_keys reached an unexpected state {failed_task_state!r} while MinIO "
            f"was scaled to zero replicas"
        )

        _poll_dagrun_state(
            kubectl,
            dag_id=_ORDERS_DAG_ID,
            run_id=first_run_id,
            want_states=("failed",),
            timeout=_DAGRUN_FAILED_TIMEOUT_SECONDS,
        )

        with analytics_connection.cursor() as cur:
            cur.execute(
                "SELECT f.file_id FROM meta.files f "
                "JOIN meta.datasets d ON d.dataset_id = f.dataset_id "
                "WHERE d.dataset_name = %s AND f.object_uri = %s",
                (_ORDERS_DATASET, object_uri),
            )
            premature_row = cur.fetchone()
        assert premature_row is None, (
            f"meta.files already has a row for {object_uri!r} even though MinIO was down for "
            f"the whole first run -- discover should never have reached it"
        )

        scale_up = kubectl(
            "-n",
            _MINIO_NAMESPACE,
            "scale",
            f"deployment/{_MINIO_DEPLOYMENT}",
            "--replicas=1",
        )
        assert scale_up.returncode == 0, (
            f"kubectl scale deployment/{_MINIO_DEPLOYMENT} --replicas=1 failed "
            f"(exit {scale_up.returncode}):\n{scale_up.stderr}"
        )
        wait_available = kubectl(
            "-n",
            _MINIO_NAMESPACE,
            "wait",
            "--for=condition=Available",
            f"deployment/{_MINIO_DEPLOYMENT}",
            f"--timeout={_MINIO_RESTORE_TIMEOUT_SECONDS}s",
            timeout=_MINIO_RESTORE_TIMEOUT_SECONDS + 20,
        )
        assert wait_available.returncode == 0, (
            f"deployment/{_MINIO_DEPLOYMENT} -n {_MINIO_NAMESPACE} did not report Available "
            f"within {_MINIO_RESTORE_TIMEOUT_SECONDS}s of scaling back to 1 replica (exit "
            f"{wait_available.returncode}):\n{wait_available.stderr}"
        )
        minio_scaled_down = False

        trigger2 = kubectl(
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
            second_run_id,
        )
        assert trigger2.returncode == 0, f"airflow dags trigger failed:\n{trigger2.stderr}"

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
            f"after MinIO recovered, run {run_row['idempotency_key']!r} finished "
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
            f"went missing after the MinIO-unavailable recovery; a value above means "
            f"duplication"
        )
    finally:
        if minio_scaled_down:
            with contextlib.suppress(Exception):
                kubectl(
                    "-n",
                    _MINIO_NAMESPACE,
                    "scale",
                    f"deployment/{_MINIO_DEPLOYMENT}",
                    "--replicas=1",
                )
                kubectl(
                    "-n",
                    _MINIO_NAMESPACE,
                    "wait",
                    "--for=condition=Available",
                    f"deployment/{_MINIO_DEPLOYMENT}",
                    f"--timeout={_MINIO_RESTORE_TIMEOUT_SECONDS}s",
                    timeout=_MINIO_RESTORE_TIMEOUT_SECONDS + 20,
                )
        with contextlib.suppress(Exception):
            admin.delete_object(Bucket="raw", Key=key)
