"""tests/e2e/slice/test_pod_kill_retry.py — D-09/D-10/D-11/D-18's proof, and the U3 baseline.

Repointed from customers to ORDERS by debug/ci-pipeline-ingestion-timeout
ROUND 16 (finding 19-A): every mechanism under proof here is
dataset-agnostic -- the stage claim/lease/heartbeat machinery
(`meta.ingestion_runs`), Airflow's own task retries relaunching a genuinely
new pod, `meta.run_stages`' DBT_BUILD tracking, `meta.v_run_recovery`'s
single-query recovery answer, and the exactly-once row-count guarantee under
a real `kubectl delete pod`. What is NOT dataset-agnostic is the fixture's
delivery shape: a lone large file honors orders' contract, while against
customers' full-snapshot contract the same lone file is -- correctly -- a
mass-delete breaker trip (ROUND 15's live-confirmed finding 19). Fixtures
are generated in-test (`build_orders_csv_bytes`), each run in its own fresh,
randomly-offset `order_id` window so repeat runs never contend for the same
`normalized.orders` keys, and each references live-sampled real
`normalized.customers` parents (orders' REFERENTIAL rule -- see
`conftest.existing_customer_ids`). Raw uploads are deliberately NOT deleted
afterwards: raw is append-only (section 63/ADR-0011) and rebuild-from-raw's
premise requires published data's raw files to persist.

Honest limit: `docs/spikes/U3-throughput-baseline.md`'s peak-RSS figure is a
sampled MAXIMUM of `/sys/fs/cgroup/memory.current` polled every few seconds
during the run, not a true kernel-tracked peak. This cluster's containers
were verified (live, via `kubectl exec ... cat /sys/fs/cgroup/memory.peak`)
to NOT expose the cgroup v2 `memory.peak` file the plan's own text
preferred, and `kubectl top pod` was verified (live) to fail outright --
this cluster has no metrics-server installed. Both are documented as
genuine, live-verified environment facts, not assumptions; the sampled-max
fallback is 04-08-PLAN.md's own explicitly anticipated alternative ("OR
reads /sys/fs/cgroup/memory.peak ... taking the MAX observed value" --
adapted here to `memory.current`, the file that actually exists).
"""

from __future__ import annotations

import contextlib
import datetime
import random
import threading
import time
import uuid
from typing import TYPE_CHECKING, Any

import pytest

from tests.e2e.slice.conftest import (
    _poll_dbt_build_running_signal,
    build_orders_csv_bytes,
    existing_customer_ids,
    poll_file_discovered,
    poll_ingestion_run,
    poll_run_for_file,
    trigger_orders_dagrun,
    wait_for_orders_dagrun_queue_idle,
)

if TYPE_CHECKING:
    import subprocess
    from collections.abc import Callable
    from pathlib import Path

    import psycopg

pytestmark = pytest.mark.cluster

_ORDERS_DATASET = "orders"

# ~1,000,000 rows -- the scale the mid-load KILL-WINDOW proof is
# load-bearing at: a smaller file's stage window can complete before the
# kill lands, quietly turning a mid-load kill test into a no-op. Kept at 1M
# deliberately (debug/ci-pipeline-ingestion-timeout ROUND 17, finding 25-B
# right-sizing survey): at the U3-measured local rate (~42k rows/s) a 250k
# file stages in ~6s -- too slim against the 0.5s DB-poll + kubectl-delete
# latency -- and ROUND 15's (20a) leg-2 proof (a genuine 1M-row re-stage
# after lease expiry, distinct_keys=1M, zero duplicates) is only meaningful
# at real scale.
_LARGE_ORDERS_ROWS = 1_000_000

# The U3 throughput/peak-RSS baseline's own fixture size (ROUND 17, finding
# 25-B): shrunk from 1M -- U3's assertions are RATE-based (throughput > 0,
# peak > 0, and the committed doc's 5x-regression policy compares rows/sec,
# which is scale-independent once steady-state dominates; at CI-contended
# rates a 250k COPY runs for minutes, far past warmup). Unlike the
# kill-window above, nothing in U3 needs the file to outlast an external
# event, so 250k buys a ~750k-row cut in the retained raw corpus that the
# rebuild-from-raw capstone must re-queue (raw is append-only, section 63 --
# fixtures are never deleted, so every retained row is paid for again by
# every later rebuild).
_U3_FIXTURE_ROWS = 250_000

# The small-fixture row count the dbt-kill test uses (the kill target there
# is the dbt_build pod, not the stage COPY -- a big file buys nothing).
_SMALL_ORDERS_ROWS = 120

# `order_id` window base range: same [2_000_000, 1_000_000_000) convention
# as test_smoke_and_idempotency.py's own comment documents (disjoint from
# test_referential_orphan.py's [1e9, 1.499e9) band).
_ORDER_ID_LOW = 2_000_000
_ORDER_ID_HIGH = 1_000_000_000

# The KPO stage pod's configured Kubernetes memory LIMIT
# (airflow/dags/_common/kpo.py's `stage_pod_resources()`, used by BOTH
# ingestion DAGs' stage tasks -- read directly from that file at the time
# this test was written; verify there if this ever looks stale) -- recorded
# in the U3 doc for context, not enforced by this test itself.
_INGEST_POD_MEMORY_LIMIT = "4Gi"

# Airflow's own `retries=3` on the `stage` task (csv_ingest_orders.py)
# means a genuinely NEW pod launches after a kill: Airflow must notice the
# failed try and requeue (up to ~1 scheduler loop), a fresh pod must be
# scheduled and its image pulled (~10-60s on this cluster's local registry),
# and the retry re-stages the WHOLE file from scratch -- StagingLoader's own
# `DROP TABLE IF EXISTS` first, never a resume (plus ROUND 15's
# wait-and-reclaim: a SIGKILLed claim's lease must expire, ~5min, before the
# retry can genuinely re-stage) -- meaning a 1,000,000-row COPY effectively
# runs to completion an EXTRA time beyond the killed attempt's partial,
# discarded work.
#
# 900s, raised from 600 by debug/ci-pipeline-ingestion-timeout ROUND 18 on
# ROUND 17's live measurement: the full kill -> lease-expiry wait (~5.5min,
# DESIGNED behavior, fix 20a) -> 1M restage -> dbt_build+publish (<=2min
# with ROUND 17's O(delta) publish) cycle completed end-to-end in ~11.1min
# on CI -- missing the old 600s deadline by ~66s with the mechanism proven
# sound. 900s budgets the designed arithmetic (330s lease + ~340s measured
# restage/dbt/publish) with honest headroom instead of a whisker.
_RETRY_TIMEOUT_SECONDS = 900

# Sampling interval for the U3 peak-memory poller -- frequent enough to
# catch a multi-second-to-multi-minute run's actual peak, cheap enough
# (~1 `kubectl exec` per tick) not to itself perturb the pod's own resource
# usage materially.
_MEMORY_SAMPLE_INTERVAL_SECONDS = 3.0


def _sampled_parent_ids(conn: psycopg.Connection[Any]) -> list[int]:
    """Live-sample real parents for an orders fixture, with the shared failure message."""
    parent_ids = existing_customer_ids(conn, count=100)
    assert parent_ids, (
        "normalized.customers is empty on this live cluster -- orders fixtures need real "
        "parent customer_ids (the sweep corpus on CI, or any earlier customers ingest "
        "locally, populates it)"
    )
    return parent_ids


def _poll_mid_load_signal(
    conn: psycopg.Connection[Any],
    idempotency_key: str,
    *,
    timeout: float,
) -> dict[str, Any]:
    """Poll `meta.ingestion_runs` for D-11's mid-load signal: `rows_read > 0 AND status='RUNNING'`.

    Populated live by `run_ingest`'s heartbeat thread (`packages/dataplat/
    src/dataplat/pipeline/run.py`), sourced from `StagingLoader.load()`'s
    own `on_progress` callback after each COPY chunk -- see 04-08-PLAN.md's
    Interfaces section for the full provenance chain. `k8s_pod_name` is
    guaranteed non-NULL by the time this condition is true: the SAME
    `claim_ingestion_run` UPDATE that first sets `status='RUNNING'` also
    sets `k8s_pod_name` in that one statement (`packages/dataplat/src/
    dataplat/metadata/postgres.py`).

    Args:
        conn: An open connection to the analytical database.
        idempotency_key: The run's idempotency key.
        timeout: Maximum seconds to wait.

    Returns:
        `{"status": "RUNNING", "rows_read": ..., "k8s_pod_name": ...}`.

    Raises:
        AssertionError: `timeout` elapses first.
    """
    deadline = time.monotonic() + timeout
    last_status: str | None = None
    last_rows_read: int | None = None
    while time.monotonic() < deadline:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, rows_read, k8s_pod_name "
                "FROM meta.ingestion_runs WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            row = cur.fetchone()
        if row is not None:
            last_status, last_rows_read, pod_name = row
            if last_status == "RUNNING" and last_rows_read and last_rows_read > 0 and pod_name:
                return {
                    "status": last_status,
                    "rows_read": last_rows_read,
                    "k8s_pod_name": pod_name,
                }
        time.sleep(0.5)
    msg = (
        f"never observed rows_read > 0 AND status = 'RUNNING' for idempotency_key="
        f"{idempotency_key!r} within {timeout}s (last observed: status={last_status!r}, "
        f"rows_read={last_rows_read!r})"
    )
    raise AssertionError(msg)


def _poll_pod_name(
    conn: psycopg.Connection[Any],
    idempotency_key: str,
    *,
    timeout: float,
) -> str:
    """Poll `meta.ingestion_runs.k8s_pod_name` until the claiming pod is known.

    Unlike `_poll_mid_load_signal`, this does not wait for `rows_read > 0`
    -- U3's memory sampler wants to start observing as EARLY as possible,
    including the pod's own startup/connection-pool-warmup memory, not only
    once the first heartbeat has fired.

    Args:
        conn: An open connection to the analytical database.
        idempotency_key: The run's idempotency key.
        timeout: Maximum seconds to wait.

    Returns:
        The claiming pod's name.

    Raises:
        AssertionError: `timeout` elapses with no pod name recorded yet.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT k8s_pod_name FROM meta.ingestion_runs WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            row = cur.fetchone()
        if row is not None and row[0]:
            return str(row[0])
        time.sleep(0.5)
    msg = f"k8s_pod_name never appeared for idempotency_key={idempotency_key!r} within {timeout}s"
    raise AssertionError(msg)


def test_pod_kill_mid_load_produces_no_duplicates(
    s3_client: Callable[[str], Any],
    analytics_connection: psycopg.Connection[Any],
    analytics_owner_connection: psycopg.Connection[Any],
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    """D-09/D-10/D-11: a REAL `kubectl delete pod` mid-load leaves no duplicate or missing rows.

    The platform's single largest correctness risk (silent duplication
    after a genuine crash), proven against the real deployed pipeline: the
    killed pod's partial staging work is discarded (never resumed --
    `StagingLoader`'s own `DROP TABLE IF EXISTS`-first retry semantics, via
    ROUND 15's wait-out-the-lease-then-reclaim path for the SIGKILL class),
    a fresh pod re-stages and re-publishes the whole file, and the final row
    count for this run's own order_id window matches the fixture's row
    count EXACTLY -- migration 0016's `UNIQUE(order_id)` constraint already
    makes a literal SQL-level duplicate impossible, so the meaningful
    assertion is that nothing is missing OR doubled.
    """
    app = s3_client("app")

    parent_ids = _sampled_parent_ids(analytics_connection)
    offset = random.SystemRandom().randint(_ORDER_ID_LOW, _ORDER_ID_HIGH)
    payload = build_orders_csv_bytes(
        order_id_start=offset,
        row_count=_LARGE_ORDERS_ROWS,
        customer_ids=parent_ids,
    )
    marker = uuid.uuid4().hex[:12]
    key = f"orders/e2e-podkill-{marker}.csv"
    object_uri = f"s3://raw/{key}"

    app.put_object(Bucket="raw", Key=key, Body=payload)
    trigger_orders_dagrun(kubectl, run_id=f"e2e-podkill-{marker}")

    file_row = poll_file_discovered(
        analytics_connection,
        dataset=_ORDERS_DATASET,
        object_uri=object_uri,
        timeout=180,
    )
    assert file_row["duplicate_of_file_id"] is None, (
        f"the freshly-windowed large fixture was marked a duplicate of file_id="
        f"{file_row['duplicate_of_file_id']!r} -- the order_id window did not make "
        f"this content genuinely new"
    )

    run_row = poll_run_for_file(analytics_connection, file_id=file_row["file_id"], timeout=60)
    idempotency_key = run_row["idempotency_key"]

    mid_load = _poll_mid_load_signal(analytics_connection, idempotency_key, timeout=300)
    pod_name = mid_load["k8s_pod_name"]

    delete = kubectl("-n", "etl", "delete", "pod", pod_name, "--wait=false")
    assert delete.returncode == 0, (
        f"kubectl delete pod {pod_name!r} -n etl failed (exit {delete.returncode}):\n"
        f"{delete.stderr}"
    )

    outcome = poll_ingestion_run(
        analytics_connection,
        idempotency_key,
        timeout=_RETRY_TIMEOUT_SECONDS,
    )
    assert outcome["status"] == "SUCCEEDED", (
        f"after killing pod {pod_name!r} mid-load, the run finished "
        f"{outcome['status']!r}, not SUCCEEDED (error_type={outcome['error_type']!r}, "
        f"error_message={outcome['error_message']!r}) -- check `kubectl logs` for the retry pod"
    )

    with analytics_owner_connection.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM normalized.orders WHERE order_id BETWEEN %s AND %s",
            (offset, offset + _LARGE_ORDERS_ROWS - 1),
        )
        row = cur.fetchone()
        assert row is not None
        total = row[0]
    assert total == _LARGE_ORDERS_ROWS, (
        f"expected exactly {_LARGE_ORDERS_ROWS} rows in this run's own order_id "
        f"window [{offset}, {offset + _LARGE_ORDERS_ROWS - 1}], found {total} -- "
        f"a value below the fixture's row count means rows went missing after the kill; "
        f"a value above is impossible under migration 0016's UNIQUE(order_id) but "
        f"would mean the constraint itself regressed"
    )


_DBT_BUILD_LABEL_SELECTOR = "dag_id=csv_ingest_orders,task_id=dbt_build"

# max_active_runs=1 (csv_ingest_orders.py) + max_active_tis_per_dag=1 (dbt_build itself)
# together guarantee at most one dbt_build pod for THIS dag_id is ever in flight at a time
# (T-09-18's accepted mitigation for the label selector's own imprecision -- see 09-10-PLAN.md's
# threat model) -- polling for the pod to APPEAR is still needed because mark_dbt_build_running
# (meta.run_stages.status='RUNNING') lands before the KPO pod itself is scheduled (and, since
# ROUND 16's finding-23 fix, the eligibility list itself is computed AFTER this DagRun's own
# stage completes, so the staging DagRun's own mark_running covers the freshly-staged run).
_DBT_BUILD_POD_APPEAR_TIMEOUT_SECONDS = 120

# Same generous ceiling as _RETRY_TIMEOUT_SECONDS -- a killed dbt_build pod's retry (Airflow's
# own retries=4 on that task, ROUND 22 -- was retries=2) must be requeued, scheduled,
# image-pulled, and re-run `dbt build` to completion (dbt's own idempotent re-run, D-18) before
# meta.v_run_recovery reports 'complete'.
_DBT_BUILD_RECOVERY_TIMEOUT_SECONDS = 600

# debug/ci-pipeline-ingestion-timeout ROUND 22: was a bare `timeout=300` inline at the call site
# below -- a stale test-suite constant, never rebalanced alongside ROUND 20/21's DAG-level
# arithmetic and flagged in ROUND 21's own decision-checkpoint as "even more clearly
# under-sized" than the queue-idle budget. This poll waits for `stage` to fully SUCCEED (all of
# this test's fixture's stage map indices) before `dbt_build` can even start -- i.e. it is
# bounded by `stage`'s own worst-case-to-terminal time, not a fixed constant of its own. ROUND 22
# bumps orders.stage's retries 3 -> 4 (matching customers -- see `_common/kpo.py`'s
# HEAVY_TASK_EXECUTION_TIMEOUT comment), which raises `stage`'s own theoretical worst case to
# 1920s/32.0min (5 attempts x 360s execution_timeout + 4 x 30s retry_delay, the CONFIRMED
# constant-delay model). This round's OWN live evidence (dbtkill's real DagRun this test drives
# reached stage try=4/state=failed -- genuine retry-exhaustion under real CPU-starvation
# contention, not a hypothetical) confirms the THEORETICAL worst case is the right basis here,
# not just the smaller real-observed sample (~9-10min this round, under the OLD retries=3
# budget) -- retries can and do genuinely stack toward it under contention. 2400s gives
# 480s/25% real margin over the 1920s theoretical ceiling (same derivation and same value as
# `wait_for_orders_dagrun_queue_idle`'s own ROUND 22 rebalance -- see that function's docstring).
# If `stage` itself exhausts its retries and never succeeds, this poll correctly still fails
# (dbt_build can structurally never start) -- at 2400s instead of masking it faster, but legibly,
# naming the last-observed status, exactly as this test's other pollers already do.
_DBT_BUILD_POLL_TIMEOUT_SECONDS = 2400


def _poll_dbt_build_pod_name(
    kubectl_fn: Callable[..., subprocess.CompletedProcess[str]],
    *,
    timeout: float,
) -> str:
    """Poll for the real `dbt_build` pod's name via Airflow's own `dag_id`/`task_id` pod labels.

    Airflow's KPO auto-labels every launched pod with `dag_id`/`task_id`/`try_number` --
    `_DBT_BUILD_LABEL_SELECTOR`'s own comment explains why this selector is precise enough on
    this cluster without needing `meta.run_stages.pod_name` populated (09-10-PLAN.md's own
    Interfaces section).

    Args:
        kubectl_fn: The `kubectl` fixture callable.
        timeout: Maximum seconds to wait.

    Returns:
        The real pod's name.

    Raises:
        AssertionError: `timeout` elapses with no matching pod found.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        proc = kubectl_fn(
            "-n",
            "etl",
            "get",
            "pods",
            "-l",
            _DBT_BUILD_LABEL_SELECTOR,
            "-o",
            "jsonpath={.items[0].metadata.name}",
        )
        if proc.returncode == 0 and proc.stdout.strip():
            return proc.stdout.strip()
        time.sleep(0.5)
    msg = (
        f"no pod matching -l {_DBT_BUILD_LABEL_SELECTOR} appeared in namespace etl within "
        f"{timeout}s"
    )
    raise AssertionError(msg)


def _poll_run_recovery_complete(
    conn: psycopg.Connection[Any],
    run_id: int,
    *,
    timeout: float,
) -> str:
    """Poll `meta.v_run_recovery` for `run_id` until `next_action = 'complete'` (D-18).

    Same `deadline`/`time.sleep(0.5)` loop shape as this module's other pollers.

    Args:
        conn: An open connection to the analytical database.
        run_id: The `meta.ingestion_runs.run_id` to watch.
        timeout: Maximum seconds to wait.

    Returns:
        `"complete"`, once observed.

    Raises:
        AssertionError: `timeout` elapses first -- names the last-observed `next_action`.
    """
    deadline = time.monotonic() + timeout
    last_next_action: str | None = None
    while time.monotonic() < deadline:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT next_action FROM meta.v_run_recovery WHERE run_id = %s", (run_id,)
            )
            row = cur.fetchone()
        if row is not None:
            last_next_action = row[0]
            if last_next_action == "complete":
                return last_next_action
        time.sleep(0.5)
    msg = (
        f"meta.v_run_recovery[run_id={run_id!r}] never reached next_action='complete' within "
        f"{timeout}s (last observed: {last_next_action!r})"
    )
    raise AssertionError(msg)


def test_pod_kill_mid_dbt_build_produces_no_duplicates(
    s3_client: Callable[[str], Any],
    analytics_connection: psycopg.Connection[Any],
    airflow_metadata_connection: psycopg.Connection[Any],
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    """D-18: a REAL `kubectl delete pod` mid-`dbt_build` recovers with no duplicate or missing rows.

    `dbt_build` is the one stage Phase 4/8's pod-kill proofs never covered (new in Phase 08.1,
    with no `rows_read`-style heartbeat of its own) -- this extends the SAME live-kill mechanism
    `test_pod_kill_mid_load_produces_no_duplicates` already proved for `stage`/`publish` to the
    third pipeline stage. The mid-flight signal is plan 09-04's own `mark_dbt_build_running`
    write (`meta.run_stages.status='RUNNING'` for `stage_name='DBT_BUILD'` -- written by the
    staging DagRun itself since ROUND 16's finding-23 fix wired the eligibility query after
    `stage`), and recovery is confirmed via `meta.v_run_recovery` (plan 09-06) reporting
    `next_action='complete'` -- D-15's own single-query recovery answer, never a hand-rolled
    3-way join in this test.
    """
    app = s3_client("app")

    parent_ids = _sampled_parent_ids(analytics_connection)
    offset = random.SystemRandom().randint(_ORDER_ID_LOW, _ORDER_ID_HIGH)
    payload = build_orders_csv_bytes(
        order_id_start=offset,
        row_count=_SMALL_ORDERS_ROWS,
        customer_ids=parent_ids,
    )
    marker = uuid.uuid4().hex[:12]
    key = f"orders/e2e-dbtkill-{marker}.csv"
    object_uri = f"s3://raw/{key}"

    # ROUND 18 (finding 25/R17 adjudication): start the 180s discovery budget
    # honestly -- this test's R16/R17 failures were pure queue-drain latency
    # behind the podkill test's own 10.5-min max_active_runs=1 occupancy.
    wait_for_orders_dagrun_queue_idle(airflow_metadata_connection)

    app.put_object(Bucket="raw", Key=key, Body=payload)
    trigger_orders_dagrun(kubectl, run_id=f"e2e-dbtkill-{marker}")

    file_row = poll_file_discovered(
        analytics_connection,
        dataset=_ORDERS_DATASET,
        object_uri=object_uri,
        timeout=180,
    )
    assert file_row["duplicate_of_file_id"] is None, (
        f"the freshly-windowed small fixture was marked a duplicate of file_id="
        f"{file_row['duplicate_of_file_id']!r} -- the order_id window did not make "
        f"this content genuinely new"
    )

    run_row = poll_run_for_file(analytics_connection, file_id=file_row["file_id"], timeout=60)
    run_id = run_row["run_id"]

    _poll_dbt_build_running_signal(
        analytics_connection, run_id, timeout=_DBT_BUILD_POLL_TIMEOUT_SECONDS
    )
    pod_name = _poll_dbt_build_pod_name(kubectl, timeout=_DBT_BUILD_POD_APPEAR_TIMEOUT_SECONDS)

    delete = kubectl("-n", "etl", "delete", "pod", pod_name, "--wait=false")
    assert delete.returncode == 0, (
        f"kubectl delete pod {pod_name!r} -n etl failed (exit {delete.returncode}):\n"
        f"{delete.stderr}"
    )

    _poll_run_recovery_complete(
        analytics_connection, run_id, timeout=_DBT_BUILD_RECOVERY_TIMEOUT_SECONDS
    )

    outcome = poll_ingestion_run(analytics_connection, run_row["idempotency_key"], timeout=60)
    assert outcome["status"] == "SUCCEEDED", (
        f"after killing dbt_build pod {pod_name!r}, run {run_id!r} finished "
        f"{outcome['status']!r}, not SUCCEEDED (error_type={outcome['error_type']!r}, "
        f"error_message={outcome['error_message']!r})"
    )

    with analytics_connection.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM normalized.orders WHERE order_id BETWEEN %s AND %s",
            (offset, offset + _SMALL_ORDERS_ROWS - 1),
        )
        row = cur.fetchone()
        assert row is not None
        normalized_total = row[0]
    assert normalized_total == _SMALL_ORDERS_ROWS, (
        f"expected exactly {_SMALL_ORDERS_ROWS} rows in normalized.orders for this run's own "
        f"order_id window [{offset}, {offset + _SMALL_ORDERS_ROWS - 1}], found "
        f"{normalized_total} -- a value below means rows went missing after the dbt_build "
        f"kill; a value above means duplication"
    )

    with analytics_connection.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM silver.orders WHERE order_id::int BETWEEN %s AND %s",
            (offset, offset + _SMALL_ORDERS_ROWS - 1),
        )
        row = cur.fetchone()
        assert row is not None
        silver_total = row[0]
    assert silver_total == _SMALL_ORDERS_ROWS, (
        f"expected exactly {_SMALL_ORDERS_ROWS} rows in silver.orders for this run's own "
        f"order_id window [{offset}, {offset + _SMALL_ORDERS_ROWS - 1}], found "
        f"{silver_total} -- dbt's own idempotent re-run (plus Airflow's retries=2 on "
        f"dbt_build) should have produced exactly one row per business key, not fewer or more"
    )


def _read_run_metrics(conn: psycopg.Connection[Any], idempotency_key: str) -> tuple[int, int]:
    """Read and validate a completed run's `rows_loaded`/`duration_ms`.

    Args:
        conn: An open connection to the analytical database.
        idempotency_key: The completed run's idempotency key.

    Returns:
        `(rows_loaded, duration_ms)`, both confirmed non-NULL and positive.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT rows_loaded, duration_ms FROM meta.ingestion_runs WHERE idempotency_key = %s",
            (idempotency_key,),
        )
        metrics_row = cur.fetchone()
    assert metrics_row is not None
    rows_loaded, duration_ms = metrics_row
    assert rows_loaded is not None, "expected a non-NULL rows_loaded for the U3 baseline run"
    assert rows_loaded > 0, f"expected a nonzero rows_loaded, got {rows_loaded!r}"
    assert duration_ms is not None, "expected a non-NULL duration_ms for the U3 baseline run"
    assert duration_ms > 0, f"expected a nonzero duration_ms, got {duration_ms!r}"
    return int(rows_loaded), int(duration_ms)


def test_u3_throughput_and_peak_rss_baseline(
    s3_client: Callable[[str], Any],
    analytics_connection: psycopg.Connection[Any],
    airflow_metadata_connection: psycopg.Connection[Any],
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    repo_root: Path,
) -> None:
    """U3: a measured streaming-throughput and peak-RSS baseline, committed for future comparison.

    A SEPARATE, non-killed full run of a `_U3_FIXTURE_ROWS`-sized fixture
    (its own fresh order_id window -- module docstring; right-sized from 1M
    to 250k by ROUND 17 finding 25-B, see `_U3_FIXTURE_ROWS`'s own comment)
    -- killing mid-load is not the right run to measure steady-state
    throughput from.
    Peak memory is the running MAXIMUM of `/sys/fs/cgroup/memory.current`
    sampled every `_MEMORY_SAMPLE_INTERVAL_SECONDS` while the run is in
    flight, from a background thread stopped (and joined) before this
    function reads its result -- module docstring documents why this is a
    sampled approximation, not `memory.peak`, on this specific cluster.
    """
    app = s3_client("app")

    parent_ids = _sampled_parent_ids(analytics_connection)
    offset = random.SystemRandom().randint(_ORDER_ID_LOW, _ORDER_ID_HIGH)
    payload = build_orders_csv_bytes(
        order_id_start=offset,
        row_count=_U3_FIXTURE_ROWS,
        customer_ids=parent_ids,
    )
    marker = uuid.uuid4().hex[:12]
    key = f"orders/e2e-u3-{marker}.csv"
    object_uri = f"s3://raw/{key}"

    # ROUND 18 (finding 25/R17 adjudication): start the 180s discovery budget
    # honestly -- same queue-drain knock-on class as the dbtkill test above,
    # and a clean-queue start also keeps this baseline's own steady-state
    # throughput measurement uncontaminated by a draining backlog.
    wait_for_orders_dagrun_queue_idle(airflow_metadata_connection)

    app.put_object(Bucket="raw", Key=key, Body=payload)
    trigger_orders_dagrun(kubectl, run_id=f"e2e-u3-{marker}")

    file_row = poll_file_discovered(
        analytics_connection,
        dataset=_ORDERS_DATASET,
        object_uri=object_uri,
        timeout=180,
    )
    assert file_row["duplicate_of_file_id"] is None

    run_row = poll_run_for_file(analytics_connection, file_id=file_row["file_id"], timeout=60)
    idempotency_key = run_row["idempotency_key"]

    pod_name = _poll_pod_name(analytics_connection, idempotency_key, timeout=120)

    samples: list[int] = []
    stop_sampling = threading.Event()

    def _sample_loop() -> None:
        while not stop_sampling.is_set():
            proc = kubectl(
                "-n",
                "etl",
                "exec",
                pod_name,
                "--",
                "cat",
                "/sys/fs/cgroup/memory.current",
            )
            if proc.returncode == 0:
                with contextlib.suppress(ValueError):
                    samples.append(int(proc.stdout.strip()))
            stop_sampling.wait(_MEMORY_SAMPLE_INTERVAL_SECONDS)

    sampler = threading.Thread(target=_sample_loop, name="u3-memory-sampler", daemon=True)
    sampler.start()
    try:
        outcome = poll_ingestion_run(
            analytics_connection,
            idempotency_key,
            timeout=_RETRY_TIMEOUT_SECONDS,
        )
    finally:
        stop_sampling.set()
        sampler.join(timeout=10)

    assert outcome["status"] == "SUCCEEDED", (
        f"U3 baseline run finished {outcome['status']!r}, not SUCCEEDED "
        f"(error_type={outcome['error_type']!r}, error_message={outcome['error_message']!r})"
    )

    rows_loaded, duration_ms = _read_run_metrics(analytics_connection, idempotency_key)
    throughput_rows_per_sec = rows_loaded / (duration_ms / 1000)
    peak_bytes = max(samples) if samples else 0

    assert throughput_rows_per_sec > 0
    assert peak_bytes > 0, (
        "no memory.current sample was ever captured -- the pod may have completed and "
        "been deleted before the first sample landed (on_finish_action=delete_succeeded_pod)"
    )

    _write_u3_spike_doc(
        repo_root,
        rows_loaded=rows_loaded,
        duration_ms=duration_ms,
        throughput_rows_per_sec=throughput_rows_per_sec,
        peak_bytes=peak_bytes,
        sample_count=len(samples),
    )


def _write_u3_spike_doc(  # noqa: PLR0913 -- six independently-named metrics; a dataclass for one call site adds nothing
    repo_root: Path,
    *,
    rows_loaded: int,
    duration_ms: int,
    throughput_rows_per_sec: float,
    peak_bytes: int,
    sample_count: int,
) -> None:
    """Regenerate `docs/spikes/U3-throughput-baseline.md` from this test's own live measurement.

    Automated from the test itself, mirroring `test_smoke_and_idempotency.
    py`'s `_write_u1_spike_doc` -- never hand-edited.
    """
    path = repo_root / "docs" / "spikes" / "U3-throughput-baseline.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    measured_at = datetime.datetime.now(tz=datetime.UTC).isoformat()
    peak_mib = peak_bytes / (1024 * 1024)
    content = f"""# U3 — streaming CSV throughput and peak-RSS baseline

**Regenerated automatically by
`tests/e2e/slice/test_pod_kill_retry.py::test_u3_throughput_and_peak_rss_baseline`
— do not hand-edit.**

- Measured at: {measured_at}
- Fixture: a generated `orders` CSV ({_U3_FIXTURE_ROWS:,} rows in a fresh
  `order_id` window, parents live-sampled from `normalized.customers` --
  `tests/e2e/slice/conftest.py::build_orders_csv_bytes`; repointed from the
  customers manifest fixture by debug/ci-pipeline-ingestion-timeout ROUND 16,
  finding 19-A, so the measured run includes orders' REFERENTIAL barrier)
- Stage pod configured memory limit:
  `{_INGEST_POD_MEMORY_LIMIT}` (`airflow/dags/_common/kpo.py`'s
  `stage_pod_resources()`, shared by both ingestion DAGs)
- `rows_loaded`: {rows_loaded:,}
- `duration_ms`: {duration_ms:,}
- **Throughput: {throughput_rows_per_sec:,.0f} rows/sec**
- **Peak RSS (sampled): {peak_mib:,.1f} MiB** ({sample_count} sample(s) of
  `/sys/fs/cgroup/memory.current`, every {_MEMORY_SAMPLE_INTERVAL_SECONDS:.0f}s)

## Measurement method, and its honest limits

Throughput is `rows_loaded / (duration_ms / 1000)` read directly from the
completed run's own `meta.ingestion_runs` row — the same wall-clock the
pipeline itself reports, not a value derived independently in this test.

Peak memory is the running MAXIMUM of `/sys/fs/cgroup/memory.current`,
polled by `kubectl exec`-ing into the stage pod on a fixed interval while
the run is in flight. This cluster's containers were verified live to NOT
expose the cgroup v2 `memory.peak` file (`cat
/sys/fs/cgroup/memory.peak` → `No such file or directory`), and `kubectl
top pod` was verified live to fail (`error: Metrics API not available` —
no metrics-server is installed on this cluster). The sampled maximum is
therefore a LOWER BOUND on the true peak, not an exact figure: a spike
between two samples (up to
{_MEMORY_SAMPLE_INTERVAL_SECONDS:.0f}s apart) would not be observed, and a
spike in the pod's final moments before it is deleted
(`on_finish_action=delete_succeeded_pod`) could be missed entirely if it
happens after the last successful sample.

## Regression policy

A future run of this same test producing a throughput figure more than 5x
worse than the number above should be treated as a bug, not a mystery
(ROADMAP.md's own words) — investigate before assuming the hardware or
cluster is merely "slower today."
"""
    path.write_text(content, encoding="utf-8")
