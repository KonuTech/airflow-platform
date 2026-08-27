"""tests/e2e/slice/test_concurrent_select.py — D-12's dedicated atomicity proof, live.

`tests/integration/test_publish_merge.py::test_atomic_commit` (04-06)
already proves the publish transaction is atomic under a same-process,
two-connection simulation against testcontainers PostgreSQL. This test
proves the SAME property survives the REAL deployment topology -- a live
analytical PostgreSQL, a real network hop through `kubectl port-forward`,
and rows arriving via a genuine `KubernetesPodOperator` pod running
`dataplat ingest`, not a function call in the same process. Success
criterion 3's own wording ("a concurrent SELECT never observes a half-loaded
table") is a platform-level claim; this is that claim's real-topology proof,
not a restatement of 04-06's mechanism-level one.

Repointed from customers to ORDERS by debug/ci-pipeline-ingestion-timeout
ROUND 16 (finding 19-A): the atomicity property under proof is
dataset-agnostic (`OrdersMergePublisher`'s publish is one INSERT..SELECT
upsert inside one transaction, `MergePublisher`'s exact shape), and a
lone-file delivery honors orders' contract -- against customers'
full-snapshot contract the same lone file is, correctly, a mass-delete
breaker trip (ROUND 15's live-confirmed finding 19). The fixture is
generated in-test (`build_orders_csv_bytes`) at 250,000 rows: the property
is publish atomicity, not scale (the 1M-row scale proofs live in
`test_pod_kill_retry.py`'s podkill/U3 tests), and the smaller file bounds
what `make rebuild-from-raw` later reprocesses from the retained raw
history. The raw upload is deliberately NOT deleted afterwards: raw is
append-only (section 63/ADR-0011) and rebuild-from-raw's premise requires
published data's raw files to persist.

Honest limit: "strictly during the run's RUNNING window" is measured from
this test's own polling resolution (~0.2s), bounded by the moment `status`
is first observed as `RUNNING` and the moment it is first observed as
terminal -- not from a server-side event stream. A transition faster than
one poll tick would not be caught as a distinct sample, but PostgreSQL's own
MVCC guarantees (proven by the assertion below, not assumed) mean no such
transition could ever produce a genuinely partial count regardless of
sampling luck.
"""

from __future__ import annotations

import random
import threading
import time
import uuid
from typing import TYPE_CHECKING, Any

import pytest

from tests.e2e.slice.conftest import (
    build_orders_csv_bytes,
    existing_customer_ids,
    poll_file_discovered,
    poll_ingestion_run,
    poll_run_for_file,
    trigger_orders_dagrun,
)

if TYPE_CHECKING:
    import contextlib
    import subprocess
    from collections.abc import Callable

    import psycopg

pytestmark = pytest.mark.cluster

_ORDERS_DATASET = "orders"

# 250,000 rows -- see the module docstring's sizing rationale (atomicity,
# not scale, is the property; the 1M-row scale proofs live in
# test_pod_kill_retry.py).
_CONCURRENT_FIXTURE_ROWS = 250_000

# `order_id` window base range: same [2_000_000, 1_000_000_000) convention
# as test_smoke_and_idempotency.py's own comment documents (disjoint from
# test_referential_orphan.py's [1e9, 1.499e9) band).
_ORDER_ID_LOW = 2_000_000
_ORDER_ID_HIGH = 1_000_000_000

# The observer thread's own poll interval -- frequent enough to have a real
# chance of landing samples throughout a multi-second-or-longer RUNNING
# window without generating so much load that it perturbs the very
# publish transaction it is observing.
_OBSERVER_POLL_INTERVAL_SECONDS = 0.2

_MIN_MID_RUN_SAMPLES = 3


def _orders_window_count(conn: psycopg.Connection[Any], *, low: int, high: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM normalized.orders WHERE order_id BETWEEN %s AND %s",
            (low, high),
        )
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


def _wait_for_running(
    conn: psycopg.Connection[Any],
    idempotency_key: str,
    *,
    timeout: float,
) -> float:
    """Poll until `status` is first observed as `RUNNING`, returning that tick's `time.monotonic()`.

    A tighter proxy for "the RUNNING window's start" than
    `poll_run_for_file`'s own return (that only proves the row exists,
    which happens at `PENDING`, before the ingest pod ever claims it).

    Args:
        conn: An open connection to the analytical database.
        idempotency_key: The run's idempotency key.
        timeout: Maximum seconds to wait.

    Returns:
        The `time.monotonic()` value at the poll tick `status` first read
        as `RUNNING`.

    Raises:
        AssertionError: `timeout` elapses first.
    """
    deadline = time.monotonic() + timeout
    last_status: str | None = None
    while time.monotonic() < deadline:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM meta.ingestion_runs WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            row = cur.fetchone()
        if row is not None:
            last_status = row[0]
            if last_status == "RUNNING":
                return time.monotonic()
        time.sleep(0.2)
    msg = (
        f"status for idempotency_key={idempotency_key!r} never reached RUNNING within "
        f"{timeout}s (last observed: {last_status!r})"
    )
    raise AssertionError(msg)


def test_concurrent_select_never_observes_partial_publish(
    s3_client: Callable[[str], Any],
    analytics_connection: psycopg.Connection[Any],
    open_etl_app_connection: Callable[
        [], contextlib.AbstractContextManager[psycopg.Connection[Any]]
    ],
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    """A concurrent SELECT throughout an in-flight publish sees only the pre- or post-count.

    `analytics_connection` (fixture setup runs before this function's own
    body, so it is genuinely opened BEFORE ingestion is triggered) becomes
    the long-lived "observer" connection, switched to autocommit so every
    query it runs sees the latest COMMITTED snapshot rather than a
    transaction-pinned one. A background thread polls it on a fixed
    interval for the ROW COUNT in this test's own randomly-windowed
    `order_id` range -- scoped narrowly (never a bare `COUNT(*)`) so
    concurrent cluster activity, or a repeat run of this same suite, cannot
    introduce noise this test would misread as a partial count. The main
    thread drives discovery/ingest polling on a SEPARATE connection
    (`open_analytics_connection`): psycopg connections are not safe for
    concurrent use from two threads.
    """
    analytics_connection.autocommit = True

    parent_ids = existing_customer_ids(analytics_connection, count=100)
    assert parent_ids, (
        "normalized.customers is empty on this live cluster -- orders fixtures need real "
        "parent customer_ids (the sweep corpus on CI, or any earlier customers ingest "
        "locally, populates it)"
    )

    offset = random.SystemRandom().randint(_ORDER_ID_LOW, _ORDER_ID_HIGH)
    low, high = offset, offset + _CONCURRENT_FIXTURE_ROWS - 1

    pre_upload_count = _orders_window_count(analytics_connection, low=low, high=high)
    assert pre_upload_count == 0, (
        f"order_id window [{low}, {high}] already has {pre_upload_count} row(s) before "
        f"upload -- the random window collided with a prior run's data"
    )

    app = s3_client("app")
    payload = build_orders_csv_bytes(
        order_id_start=offset,
        row_count=_CONCURRENT_FIXTURE_ROWS,
        customer_ids=parent_ids,
    )
    marker = uuid.uuid4().hex[:12]
    key = f"orders/e2e-concurrent-select-{marker}.csv"
    object_uri = f"s3://raw/{key}"

    samples: list[tuple[float, int]] = []
    stop_observing = threading.Event()

    def _observe_loop() -> None:
        while not stop_observing.is_set():
            count = _orders_window_count(analytics_connection, low=low, high=high)
            samples.append((time.monotonic(), count))
            stop_observing.wait(_OBSERVER_POLL_INTERVAL_SECONDS)

    observer = threading.Thread(
        target=_observe_loop,
        name="concurrent-select-observer",
        daemon=True,
    )

    try:
        app.put_object(Bucket="raw", Key=key, Body=payload)
        trigger_orders_dagrun(kubectl, run_id=f"e2e-concurrent-select-{marker}")
        observer.start()

        with open_etl_app_connection() as main_conn:
            file_row = poll_file_discovered(
                main_conn,
                dataset=_ORDERS_DATASET,
                object_uri=object_uri,
                timeout=180,
            )
            assert file_row["duplicate_of_file_id"] is None

            run_row = poll_run_for_file(main_conn, file_id=file_row["file_id"], timeout=60)
            idempotency_key = run_row["idempotency_key"]

            running_ts = _wait_for_running(main_conn, idempotency_key, timeout=120)
            outcome = poll_ingestion_run(main_conn, idempotency_key, timeout=600)
            terminal_ts = time.monotonic()
    finally:
        stop_observing.set()
        observer.join(timeout=10)

    assert outcome["status"] == "SUCCEEDED", f"run finished {outcome['status']!r}, not SUCCEEDED"

    post_upload_count = _orders_window_count(analytics_connection, low=low, high=high)
    assert post_upload_count == _CONCURRENT_FIXTURE_ROWS, (
        f"expected exactly {_CONCURRENT_FIXTURE_ROWS} rows in [{low}, {high}] after a "
        f"SUCCEEDED run, found {post_upload_count}"
    )

    observed_counts = {count for _, count in samples}
    partial_counts = observed_counts - {pre_upload_count, post_upload_count}
    assert not partial_counts, (
        f"observed count(s) that were neither the pre-upload count ({pre_upload_count}) nor "
        f"the post-upload count ({post_upload_count}): {sorted(partial_counts)} -- this would "
        f"mean a concurrent SELECT saw a half-loaded table"
    )

    mid_run_samples = [ts for ts, _ in samples if running_ts <= ts <= terminal_ts]
    assert len(mid_run_samples) >= _MIN_MID_RUN_SAMPLES, (
        f"only {len(mid_run_samples)} sample(s) landed strictly during the RUNNING window "
        f"({running_ts:.1f}s .. {terminal_ts:.1f}s monotonic) -- need at least "
        f"{_MIN_MID_RUN_SAMPLES} to prove the observer actually caught the in-flight state, "
        f"not just before/after (a test with zero mid-run samples would pass vacuously)"
    )
