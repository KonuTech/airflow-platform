"""tests/e2e/chaos/test_pod_crash.py — QUAL-15 scenario 1: a killed task pod recovers cleanly.

Read `tests/e2e/slice/test_pod_kill_retry.py` in full (11-09-PLAN.md's own Interfaces section)
before writing this module — that file ALREADY implements two real `kubectl delete pod` mid-run
proofs against the `csv_ingest_customers` DAG, idempotency-proven since Phase 4/08.1:
`test_pod_kill_mid_load_produces_no_duplicates` kills the `stage` task pod mid-COPY, and
`test_pod_kill_mid_dbt_build_produces_no_duplicates` kills the `dbt_build` task pod mid-run. Its
own module docstring names exactly those two tasks as covered — `discover` and `publish` are
not, and `wait_for_files` is a DEFERRED `S3KeySensor` with no task pod to kill while deferred at
all (nothing for `kubectl delete pod` to target).

This module extends coverage to `discover`: the FIRST real `KubernetesPodOperator` in either
ingestion DAG's graph (`wait_for_files >> matched_keys >> gate >> discover`), which registers
`meta.files`/`meta.batches`/`meta.ingestion_runs` rows and freezes an `AssignmentDocument` to
MinIO (`dataplat.discovery.discover_files`). Every write that function makes is independently
idempotent by construction — `_hash_and_register_file`'s `create_file` is an upsert,
`get_or_create_batch`/`link_batch_file`/`get_or_create_ingestion_run` are all idempotent by name
— so a killed-and-retried `discover` is EXPECTED to converge to the identical result a
never-killed run would reach. This test proves that live, not merely by code inspection,
extending real "pod crashes" coverage to a task-kill angle `test_pod_kill_retry.py` genuinely
does not exercise, per its own interfaces-block decision framework (11-09-PLAN.md).

Targets `csv_ingest_orders`/`orders`, NOT `csv_ingest_customers`/`customers` — a live-cluster
finding during this plan's own execution (2026-08-22): `csv_ingest_customers` had a pre-existing,
unrelated scheduling backlog at test-authoring time (a `scheduled__2026-08-22T11:53:00` DagRun's
`stage` task stuck retrying several mapped indices for hours, plus four queued backfill runs from
the day before — all entirely unrelated to this plan's own changes, matching the exact
"self-inflicted Airflow scheduling backlog" pattern PROJECT.md's own Standing Facts already
document as a recurring, accepted characteristic of this DAG). `csv_ingest_customers`'s
`max_active_runs=1` meant a fresh file could not even be discovered until that backlog cleared —
an unbounded wait entirely out of this plan's own scope to fix. `csv_ingest_orders` mirrors
`csv_ingest_customers`'s graph shape exactly (`airflow/dags/csv_ingest_orders.py`'s own module
docstring: "Mirrors csv_ingest_customers.py's shape exactly") and was genuinely idle at the same
moment (zero running/queued DagRuns) — manually triggering it via `airflow dags trigger` (the
SAME established pattern `tests/e2e/slice/test_referential_orphan.py` already uses for this exact
DAG, not a new invention) sidesteps `csv_ingest_customers`'s own `max_active_runs=1` budget
entirely, since the two DAGs' run-concurrency is tracked independently by dag_id.
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
_DISCOVER_LABEL_SELECTOR = "dag_id=csv_ingest_orders,task_id=discover"

# order_id/customer_id ranges disjoint from every other live suite's own choices
# (test_referential_orphan.py's own module docstring names its [1_000_000_000, 1_999_000_000)
# window; slice's large-fixture offset trick uses [2_000_000, 1_000_000_000) for customer_id) —
# chosen here so a concurrently-running slice-suite test can never collide with this file's own
# order_id/customer_id choices.
_ORDER_ID_LOW = 2_000_000_000
_ORDER_ID_HIGH = 2_100_000_000
_ROW_COUNT = 20

# `discover` only launches after `gate` (`integrity_gate.expand(key=matched_keys)`) has
# processed EVERY key `list_matched_keys` finds under `raw/orders/*.csv` -- live-observed this
# session: the shared cluster's `raw` bucket carries ~20 accumulated `orders/*.csv` objects from
# earlier E2E sessions (this suite's own upload is one MORE match, on top of that), and
# `integrity_gate.override(max_active_tis_per_dag=3)` processes them 3-at-a-time. Live-measured
# TWICE this session: once ~100s, once ~350s+ for the same ~20-object fan-out (this cluster's own
# tight, shared CPU budget -- kind/cluster.yaml's own documented 3-CPU/node ceiling -- makes
# per-batch latency genuinely variable, not a fixed constant). 480s budgets well above the
# slower observed run, not the faster one. `discover`'s OWN task duration is comparatively small
# once `gate` finally clears (~10-20s live, measured directly against this cluster's Airflow
# metadata DB for the sibling `csv_ingest_customers` DAG's byte-identical `discover` task shape).
_POD_APPEAR_TIMEOUT_SECONDS = 480
_POD_APPEAR_POLL_INTERVAL_SECONDS = 0.2

# Generous: a killed discover's retry (Airflow's own retries=2, retry_exponential_backoff=True,
# retry_delay=5min -- live-read from this cluster's own SerializedDagModel, 2026-08-22) must be
# requeued, scheduled, rerun to completion, and the DAG must then run stage->dbt_build->publish
# to a terminal state. Live-observed this session (unrelated to this test's own pod-kill target):
# `dbt_build`'s own KubernetesJobWatcher request-timeout race (the SAME documented, accepted
# flakiness `csv_ingest_customers.py`'s own code comments describe) hit BOTH of its own allowed
# retries in a single run (three real attempts observed: fail ~T+0, fail ~T+8min, fail ~T+20min,
# each gap itself a 5-10min `retry_delay` wait), pushing the FIRST live attempt at this budget
# (1800s) to time out by mere moments while `publish` was already running -- `mark_dbt_build_
# done`'s `trigger_rule="all_done"` always lets `publish` proceed once dbt_build reaches ANY
# terminal state (success or permanently failed), so this is purely a wall-clock budget question,
# not a correctness one. 3600s gives the full dbt_build exhaustion sequence (up to ~20-25min
# observed) generous headroom on top of discover's own recovery wait, without depending on
# exactly how many times dbt_build happens to race on a given run.
_RUN_TERMINAL_TIMEOUT_SECONDS = 3600


def _existing_customer_ids(conn: psycopg.Connection[Any], *, count: int) -> list[int]:
    """Return `count` genuinely-present `normalized.customers.customer_id` values.

    Identical shape to `test_referential_orphan.py`'s own helper of the same name -- plain
    `LIMIT` (no `ORDER BY random()`): `normalized.customers` carries millions of rows live, and
    any `count` already-loaded rows are equally valid for this test's own referential-integrity
    needs.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id FROM normalized.customers LIMIT %s", (count,))
        rows = cur.fetchall()
    return [int(row[0]) for row in rows]


def _build_orders_csv(*, order_ids: list[int], customer_ids: list[int]) -> bytes:
    """Build a minimal `orders.yaml`-shaped CSV: header + one valid row per (order_id, customer_id).

    Column order (`order_id,customer_id,order_date,amount`) matches
    `configs/datasets/orders.yaml`'s own `columns:` block verbatim, same as
    `test_referential_orphan.py`'s own `_build_orders_csv` -- every row here references an
    EXISTING `customer_id` (unlike that function's own deliberate one-orphan-row design), since
    this test's own row-count assertion needs every row to publish cleanly, not partially
    quarantine.
    """
    lines = ["order_id,customer_id,order_date,amount"]
    lines.extend(
        f"{order_id},{customer_id},2026-01-15,199.99"
        for order_id, customer_id in zip(order_ids, customer_ids, strict=True)
    )
    return ("\n".join(lines) + "\n").encode("utf-8")


def _poll_discover_pod_name(
    kubectl_fn: Callable[..., subprocess.CompletedProcess[str]],
    *,
    timeout: float,
) -> str:
    """Poll for the real `discover` pod's name via Airflow's own dag_id/task_id pod labels.

    Same shape as `test_pod_kill_retry.py`'s own `_poll_dbt_build_pod_name` — see that
    function's docstring for why this label selector is precise enough on this cluster (KPO
    auto-labels every launched pod with dag_id/task_id/try_number, and `discover` is never
    `.expand()`'d, so exactly one pod matches per DagRun).

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
            _DISCOVER_LABEL_SELECTOR,
            "-o",
            "jsonpath={.items[0].metadata.name}",
        )
        if proc.returncode == 0 and proc.stdout.strip():
            return proc.stdout.strip()
        time.sleep(_POD_APPEAR_POLL_INTERVAL_SECONDS)
    msg = (
        f"no pod matching -l {_DISCOVER_LABEL_SELECTOR} appeared in namespace etl within {timeout}s"
    )
    raise AssertionError(msg)


def test_discover_pod_kill_recovers_via_airflow_retry(
    s3_client: Callable[[str], Any],
    analytics_connection: psycopg.Connection[Any],
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    """A real `kubectl delete pod` mid-`discover` recovers via Airflow's own retry (retries=2).

    Kills the first real pod to appear for a manually-triggered `csv_ingest_orders` run's
    `discover` task, then confirms: (1) a NEW pod launches and the run proceeds all the way to
    `SUCCEEDED`, (2) `normalized.orders` ends up with exactly this run's own row count — no
    duplicate, no missing row — even though the killed attempt's own
    `meta.files`/`meta.batches`/`meta.ingestion_runs` writes may have partially landed before the
    kill (see this module's own docstring: every one of those writes is independently
    idempotent).
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
    key = f"orders/e2e-chaos-podcrash-{marker}.csv"
    object_uri = f"s3://raw/{key}"
    run_id_marker = f"e2e-chaos-podcrash-{marker}"

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

        pod_name = _poll_discover_pod_name(kubectl, timeout=_POD_APPEAR_TIMEOUT_SECONDS)

        delete = kubectl("-n", "etl", "delete", "pod", pod_name, "--wait=false")
        assert delete.returncode == 0, (
            f"kubectl delete pod {pod_name!r} -n etl failed (exit {delete.returncode}):\n"
            f"{delete.stderr}"
        )

        file_row = poll_file_discovered(
            analytics_connection,
            dataset=_ORDERS_DATASET,
            object_uri=object_uri,
            timeout=_POD_APPEAR_TIMEOUT_SECONDS + 120,
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
            timeout=_RUN_TERMINAL_TIMEOUT_SECONDS,
        )
        assert outcome["status"] == "SUCCEEDED", (
            f"after killing the discover pod {pod_name!r}, run "
            f"{run_row['idempotency_key']!r} finished {outcome['status']!r}, not SUCCEEDED -- "
            f"check `kubectl logs` for the retry pod"
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
            f"[{order_ids[0]}, {order_ids[-1]}], found {total} -- a value below means rows went "
            f"missing after the discover-pod kill; a value above means duplication"
        )
    finally:
        with contextlib.suppress(Exception):
            admin.delete_object(Bucket="raw", Key=key)
