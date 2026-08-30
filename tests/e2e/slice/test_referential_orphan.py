"""tests/e2e/slice/test_referential_orphan.py — VALID-07's real, no-shortcuts proof.

Every earlier plan in this phase proved `ReferentialIntegrityBarrier` in
isolation (`tests/unit/validate/`) or against testcontainers
(`tests/integration/test_referential_integrity.py`). This is the one place
the WHOLE chain -- real MinIO, a real deployed `csv_ingest_orders` DAG, a
real `KubernetesPodOperator` `ingest` pod, real PostgreSQL -- proves D-16
live: an `orders` row referencing a `customer_id` that does not (yet) exist
in `normalized.customers` is quarantined at the ROW level
(`error_type='REFERENTIAL_ORPHAN'`) while the rest of the same file's rows
genuinely publish, and the run itself still reaches `SUCCEEDED` (D-16: an
orphan is an expected, row-level condition, never a whole-run failure --
matching Pitfall 5, 08-RESEARCH.md).

`csv_ingest_orders` is triggered here via a plain `airflow dags trigger`
(matching `test_smoke_and_idempotency.py`'s own convention for
`smoke_kubernetes_pod`), not by waiting on its `schedule=[customers_asset]`
Dataset/Asset coupling -- Airflow accepts a manual trigger for an
asset-scheduled DAG exactly like any other DAG (a `run_type='manual'`
DagRun), and waiting on the SAME-process customers cron actually publishing
first would make this test's own timing depend on an entirely different
DAG's schedule, which is D-15's own concern (08-13's `dagtest` tier), not
this one's.

Honest limit: this test observes ONE upload / ONE triggered run against
whatever `csv_processor_image` Variable and `orders`/`customers`
`DatasetConfig`s are live on the cluster at test time -- same honesty
boundary `test_smoke_and_idempotency.py`'s own module docstring already
documents for this suite.
"""

from __future__ import annotations

import contextlib
import random
import uuid
from typing import TYPE_CHECKING, Any

import pytest

from tests.e2e.slice.conftest import (
    poll_file_discovered,
    poll_ingestion_run,
    poll_run_for_file,
    wait_for_orders_dagrun_admitted,
    wait_for_orders_dagrun_queue_idle,
)

if TYPE_CHECKING:
    import subprocess
    from collections.abc import Callable

    import psycopg

pytestmark = pytest.mark.cluster

_ORDERS_DAG_ID = "csv_ingest_orders"
_ORDERS_DATASET = "orders"
_DAG_RUN_TIMEOUT_SECONDS = 180
_INGEST_TIMEOUT_SECONDS = 180

# order_id/customer_id are both `sa.Integer()` (migrations 0005/0016) --
# stay comfortably inside int32 (max 2,147,483,647). This range is disjoint
# from `large_csv_with_offset_customer_ids`'s own `[2_000_000, 1_000_000_000)`
# window (test_pod_kill_retry.py/test_concurrent_select.py) so a concurrently
# -running slice-suite test can never collide with this file's own
# customer_id/order_id choices.
_ORPHAN_CUSTOMER_ID_LOW = 1_500_000_000
_ORPHAN_CUSTOMER_ID_HIGH = 1_999_000_000
_ORDER_ID_LOW = 1_000_000_000
_ORDER_ID_HIGH = 1_499_000_000
_ABSENCE_CHECK_ATTEMPTS = 5


def _existing_customer_ids(conn: psycopg.Connection[Any], *, count: int) -> list[int]:
    """Return `count` genuinely-present `normalized.customers.customer_id` values.

    Plain `LIMIT` (no `ORDER BY random()`): `normalized.customers` carries
    millions of rows live (PROJECT.md's own Phase 4 note: 10,000,122 rows
    at last live count) -- `ORDER BY random()` would force a full-table
    sort for no reason when any `count` already-loaded rows prove the
    non-orphan case equally well.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id FROM normalized.customers LIMIT %s", (count,))
        rows = cur.fetchall()
    return [int(row[0]) for row in rows]


def _pick_absent_customer_id(conn: psycopg.Connection[Any]) -> int:
    """Return a `customer_id` verified, live, to have no `normalized.customers` row.

    A random pick from `_ORPHAN_CUSTOMER_ID_LOW..HIGH` is already
    astronomically unlikely to collide with any real or fixture-generated
    customer_id (STACK.md's own corpus generators stay well below this
    range), but this function still queries first (the plan's own
    documented alternative to "use a clearly-synthetic out-of-range id")
    rather than assuming -- a live cluster carrying unknown prior E2E/demo
    traffic is exactly the case this defensive check exists for.
    """
    rng = random.SystemRandom()
    for _attempt in range(_ABSENCE_CHECK_ATTEMPTS):
        candidate = rng.randint(_ORPHAN_CUSTOMER_ID_LOW, _ORPHAN_CUSTOMER_ID_HIGH)
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM normalized.customers WHERE customer_id = %s", (candidate,))
            found = cur.fetchone() is not None
        if not found:
            return candidate
    msg = (
        f"could not find a customer_id absent from normalized.customers after "
        f"{_ABSENCE_CHECK_ATTEMPTS} attempts in range "
        f"[{_ORPHAN_CUSTOMER_ID_LOW}, {_ORPHAN_CUSTOMER_ID_HIGH}] -- suspiciously unlikely, "
        f"investigate before assuming this range is merely exhausted"
    )
    raise AssertionError(msg)


def _build_orders_csv(
    *,
    valid_order_ids: list[int],
    valid_customer_ids: list[int],
    orphan_order_id: int,
    orphan_customer_id: int,
) -> bytes:
    """Build a minimal `orders.yaml`-shaped CSV: header + N valid rows + 1 orphan row.

    Column order (`order_id,customer_id,order_date,amount`) matches
    `configs/datasets/orders.yaml`'s own `columns:` block verbatim --
    `CsvSource` has no header-to-column-name mapping, only positional
    correspondence (04-04-SUMMARY.md's own documented precedent, reused by
    `tests/fixtures/slice-corpus.yaml`'s own module docstring).
    """
    lines = ["order_id,customer_id,order_date,amount"]
    for order_id, customer_id in zip(valid_order_ids, valid_customer_ids, strict=True):
        lines.append(f"{order_id},{customer_id},2026-01-15,199.99")
    lines.append(f"{orphan_order_id},{orphan_customer_id},2026-01-16,49.50")
    return ("\n".join(lines) + "\n").encode("utf-8")


def test_orphan_order_quarantined_while_valid_rows_publish(
    s3_client: Callable[[str], Any],
    analytics_connection: psycopg.Connection[Any],
    analytics_owner_connection: psycopg.Connection[Any],
    airflow_metadata_connection: psycopg.Connection[Any],
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    """VALID-07/D-16, live: one real orphan row quarantines, the rest genuinely publish.

    Uploads a real `orders` CSV referencing 2 customer_ids known (live,
    queried first) to already exist in `normalized.customers`, plus one
    customer_id verified absent -- then asserts, via direct SQL against the
    real analytical database, that: the run reaches SUCCEEDED (quarantine
    alone never fails a run, D-16); exactly one `meta.rejected_records` row
    exists for the orphan order with `error_type='REFERENTIAL_ORPHAN'`; and
    the two non-orphan rows are genuinely present in `normalized.orders`
    while the orphan row is genuinely absent from it.
    """
    app = s3_client("app")
    admin = s3_client("admin")

    valid_customer_ids = _existing_customer_ids(analytics_connection, count=2)
    assert len(valid_customer_ids) == 2, (
        "normalized.customers has fewer than 2 rows on this live cluster -- this test needs "
        "prior customers ingestion to have already happened (test_smoke_and_idempotency.py's "
        "own test_idempotent_reupload, or ordinary cron traffic, both populate it)"
    )
    orphan_customer_id = _pick_absent_customer_id(analytics_connection)

    rng = random.SystemRandom()
    order_id_base = rng.randint(_ORDER_ID_LOW, _ORDER_ID_HIGH)
    valid_order_ids = [order_id_base + 1, order_id_base + 2]
    orphan_order_id = order_id_base + 3

    payload = _build_orders_csv(
        valid_order_ids=valid_order_ids,
        valid_customer_ids=valid_customer_ids,
        orphan_order_id=orphan_order_id,
        orphan_customer_id=orphan_customer_id,
    )

    marker = uuid.uuid4().hex[:12]
    key = f"orders/e2e-orphan-{marker}.csv"
    object_uri = f"s3://raw/{key}"

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

        # ROUND 18 (debug/ci-pipeline-ingestion-timeout, finding 25/R17
        # adjudication): start the 180s discovery budget honestly -- this
        # test's R16/R17 failures were queue-drain latency behind earlier
        # tests' serialized max_active_runs=1 backlog (post-rebuild era in
        # R17), not a discovery defect. Sited AFTER the unpause: a paused
        # DAG's queued DagRuns never drain, so the reverse order could only
        # burn the drain budget waiting on a queue that cannot move.
        wait_for_orders_dagrun_queue_idle(airflow_metadata_connection)

        app.put_object(Bucket="raw", Key=key, Body=payload)

        run_id_marker = f"e2e-orphan-{marker}"
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

        # ROUND 25 (debug/ci-pipeline-ingestion-timeout): close the residual race
        # `wait_for_orders_dagrun_queue_idle`'s own docstring accepted but ROUND 24 Track B
        # proved can cost up to ~15-32min, not ~50s -- wait for THIS run_id specifically to be
        # admitted before starting the discovery budget below.
        wait_for_orders_dagrun_admitted(airflow_metadata_connection, dag_run_id=run_id_marker)

        file_row = poll_file_discovered(
            analytics_connection,
            dataset=_ORDERS_DATASET,
            object_uri=object_uri,
            timeout=_DAG_RUN_TIMEOUT_SECONDS,
        )
        assert file_row["duplicate_of_file_id"] is None, (
            f"the freshly-marked orders file was already flagged a duplicate of file_id="
            f"{file_row['duplicate_of_file_id']!r} -- the uuid marker did not make this "
            f"content genuinely new"
        )

        run_row = poll_run_for_file(analytics_connection, file_id=file_row["file_id"], timeout=60)
        outcome = poll_ingestion_run(
            analytics_connection,
            run_row["idempotency_key"],
            timeout=_INGEST_TIMEOUT_SECONDS,
        )
        assert outcome["status"] == "SUCCEEDED", (
            f"orphan-order run finished {outcome['status']!r}, not SUCCEEDED "
            f"(error_type={outcome['error_type']!r}, "
            f"error_message={outcome['error_message']!r}) -- D-16 says a "
            f"row-level REFERENTIAL_ORPHAN quarantine must never fail the whole run; check "
            f"`kubectl logs` for the ingest pod"
        )

        with analytics_owner_connection.cursor() as cur:
            cur.execute(
                """
                SELECT source_row_number, error_column
                  FROM meta.rejected_records
                 WHERE run_id = %s AND error_type = 'REFERENTIAL_ORPHAN' AND raw_line = %s
                """,
                (run_row["run_id"], str(orphan_order_id)),
            )
            orphan_rejects = cur.fetchall()
        assert len(orphan_rejects) == 1, (
            f"expected exactly one meta.rejected_records row for order_id={orphan_order_id!r} "
            f"(error_type='REFERENTIAL_ORPHAN') under run_id={run_row['run_id']!r}, found "
            f"{len(orphan_rejects)}: {orphan_rejects!r}"
        )
        assert orphan_rejects[0][1] == "customer_id", (
            f"expected the orphan reject's error_column to be 'customer_id', got "
            f"{orphan_rejects[0][1]!r}"
        )

        with analytics_owner_connection.cursor() as cur:
            cur.execute(
                "SELECT order_id FROM normalized.orders WHERE order_id = ANY(%s)",
                (valid_order_ids,),
            )
            published_order_ids = {int(row[0]) for row in cur.fetchall()}
        assert published_order_ids == set(valid_order_ids), (
            f"expected both non-orphan orders {valid_order_ids!r} to be published in "
            f"normalized.orders, found {sorted(published_order_ids)!r} -- the orphan row's "
            f"quarantine must not have blocked its file-mates from publishing"
        )

        with analytics_owner_connection.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM normalized.orders WHERE order_id = %s",
                (orphan_order_id,),
            )
            orphan_published = cur.fetchone() is not None
        assert not orphan_published, (
            f"order_id={orphan_order_id!r} (the orphan row, quarantined for a missing "
            f"customer_id) was found in normalized.orders -- REFERENTIAL_ORPHAN quarantine "
            f"must exclude the row from publish, not merely report it"
        )
    finally:
        with contextlib.suppress(Exception):
            admin.delete_object(Bucket="raw", Key=key)
