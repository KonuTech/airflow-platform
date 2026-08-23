"""tests/e2e/chaos/test_malformed_csv.py — QUAL-15 scenario: a malformed CSV quarantines, not
crashes.

Reuses `tests/fixtures/corpus.yaml`'s `17_malformed_rows.csv` (VALID-01/VALID-03) as the
specification for this test's own malformation shape — not its literal bytes. That fixture's own
header is `id,name,amount` (3 columns), a shape shared by NO dataset currently configured on this
live cluster (`configs/datasets/customers.yaml` declares 5-6 columns, `configs/datasets/
orders.yaml` declares 4, both verified by reading those files directly). Uploading the fixture's
own literal bytes to either dataset's `raw/` prefix would never reach `RaggedRowGuard` at all:
`CsvSource.inspect()` (`packages/csv-processor/src/csv_processor/source.py`) raises
`IncompatibleSchemaError` — a whole-FILE BREAKING-schema rejection — the moment a file's header is
missing a contract-declared column, which is exactly what a 3-column file looks like against a
4-or-5-column dataset. That is a materially DIFFERENT, and wrong, scenario for this test's own
purpose (quarantining specific BAD ROWS while GOOD rows in the same well-formed-header file still
load).

So this test reconstructs `17_malformed_rows.csv`'s own two structural malformation types --
`field-count-below-header` and `field-count-above-header` (that fixture's own `expect:` block,
lines 1276-1300 of `tests/fixtures/corpus.yaml`) — using `orders.yaml`'s real 4-column header
(`order_id,customer_id,order_date,amount`), so the file can flow through a genuinely live
`csv_ingest_orders` run end to end. `17_malformed_rows.csv`'s THIRD case (row 2's
quote-in-unquoted-field near-miss, which parses fine and is deliberately NOT rejected) is
intentionally omitted here: that fixture's own module-level detector docstring
(`packages/csv-processor/src/csv_processor/detect/encoding.py`) and dialect-detection docstring
both note that Phase 6's LIVE detector pipeline (`clevercsv`-driven dialect/header detection over a
bounded sample) is a materially different code path from the fixture's own unit-level, fixed-comma-
dialect harness — a single stray, unquoted `"` character in a 10-row live sample risks perturbing
dialect/header detection in a way this test has no need to accept just to prove `RaggedRowGuard`'s
own live behaviour.

Targets `csv_ingest_orders`/`orders`, not `csv_ingest_customers`/`customers`: at this plan's own
execution time (2026-08-23), `csv_ingest_customers` had a large, pre-existing, unrelated `stage`
backlog (dozens of mapped task instances, `max_active_tis_per_dag=1`) occupying its own
`max_active_runs=1` budget for hours — the exact same live-cluster finding `test_pod_crash.py`'s
own module docstring already documents (11-09-PLAN.md) and worked around the identical way.
`orders.yaml` has no `rejection_rate_threshold` declared at all (confirmed by reading the file:
only a `REFERENTIAL` quality rule, no `rejection_rate_threshold` key), so
`RejectionRateCircuitBreaker` never runs for this dataset (`total_rows_rejected/read` ratio is
never evaluated) — this test's own 2-bad/10-total shape needs no threshold headroom calculation at
all, unlike a `customers`-targeted version would.
"""

from __future__ import annotations

import contextlib
import random
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

# Disjoint from every other live suite's own documented order_id window (test_pod_crash.py:
# [2_000_000_000, 2_100_000_000); test_referential_orphan.py's own order_id choices) -- this test
# picks its own fresh window so a concurrently-running suite can never collide with it.
_ORDER_ID_LOW = 2_200_000_000
_ORDER_ID_HIGH = 2_300_000_000

_DISCOVERY_TIMEOUT_SECONDS = 480
_INGEST_TIMEOUT_SECONDS = 1800


def _existing_customer_ids(conn: psycopg.Connection[Any], *, count: int) -> list[int]:
    """Return `count` genuinely-present `normalized.customers.customer_id` values.

    Identical shape to `test_pod_crash.py`/`test_referential_orphan.py`'s own helper of the same
    name — plain `LIMIT` (no `ORDER BY random()`): `normalized.customers` carries millions of rows
    live, any `count` already-loaded rows are equally valid for this test's own referential-
    integrity needs (every row here, well-formed or malformed, must reference a REAL customer_id so
    a `REFERENTIAL_ORPHAN` quarantine never masks the `RAGGED_ROW` structural rejection this test
    is actually proving).
    """
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id FROM normalized.customers LIMIT %s", (count,))
        rows = cur.fetchall()
    return [int(row[0]) for row in rows]


def _build_malformed_orders_csv(*, order_ids: list[int], customer_ids: list[int]) -> bytes:
    """Build an `orders.yaml`-shaped CSV: 8 good rows + 1 short row + 1 long row (module docstring).

    Column order (`order_id,customer_id,order_date,amount`) matches `configs/datasets/orders.yaml`
    verbatim. Row 5 (0-indexed data row 4) omits `amount` entirely (3 fields, matching
    `17_malformed_rows.csv`'s own `field-count-below-header` case); row 7 (0-indexed data row 6)
    carries one extra trailing field (5 fields, matching that fixture's own
    `field-count-above-header` case). Every other row is well-formed (4 fields) and references a
    real, existing `customer_id`.
    """
    assert len(order_ids) == 10
    assert len(customer_ids) == 10
    lines = ["order_id,customer_id,order_date,amount"]
    for i, (order_id, customer_id) in enumerate(zip(order_ids, customer_ids, strict=True)):
        if i == 4:
            lines.append(f"{order_id},{customer_id},2026-01-15")
        elif i == 6:
            lines.append(f"{order_id},{customer_id},2026-01-15,199.99,unexpected-extra-field")
        else:
            lines.append(f"{order_id},{customer_id},2026-01-15,199.99")
    return ("\n".join(lines) + "\n").encode("utf-8")


def test_malformed_rows_quarantine_while_good_rows_still_load(
    s3_client: Callable[[str], Any],
    analytics_connection: psycopg.Connection[Any],
    analytics_owner_connection: psycopg.Connection[Any],
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    """VALID-01/VALID-03, live: a run with 2 ragged rows out of 10 SUCCEEDS, quarantining only
    those 2.

    Uploads the malformed file to a live-triggered `csv_ingest_orders` run, waits for the run to
    reach `SUCCEEDED` (never `FAILED`: `RaggedRowGuard`'s own rejections are per-ROW, and this
    dataset's own circuit breaker never runs at all — module docstring), then asserts: (1) exactly
    2 `meta.rejected_records` rows exist for this run, both `error_type='RAGGED_ROW'`, at the
    correct 0-indexed `source_row_number`s (4 and 6); (2) exactly 8 rows landed in
    `normalized.orders` for this run's own `order_id` window — the 2 ragged rows never partially
    or corruptly loaded.
    """
    app = s3_client("app")
    admin = s3_client("admin")

    customer_ids = _existing_customer_ids(analytics_connection, count=10)
    assert len(customer_ids) == 10, (
        "normalized.customers has fewer than 10 rows on this live cluster -- this test needs prior "
        "customers ingestion to have already happened"
    )
    order_id_base = random.SystemRandom().randint(_ORDER_ID_LOW, _ORDER_ID_HIGH)
    order_ids = [order_id_base + i for i in range(10)]
    payload = _build_malformed_orders_csv(order_ids=order_ids, customer_ids=customer_ids)

    marker = uuid.uuid4().hex[:12]
    key = f"orders/e2e-chaos-malformed-{marker}.csv"
    object_uri = f"s3://raw/{key}"
    run_id_marker = f"e2e-chaos-malformed-{marker}"

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

        file_row = poll_file_discovered(
            analytics_connection,
            dataset=_ORDERS_DATASET,
            object_uri=object_uri,
            timeout=_DISCOVERY_TIMEOUT_SECONDS,
        )
        assert file_row["duplicate_of_file_id"] is None, (
            f"the freshly-marked fixture was already flagged a duplicate of file_id="
            f"{file_row['duplicate_of_file_id']!r} -- the uuid marker did not make this content "
            f"genuinely new"
        )

        run_row = poll_run_for_file(analytics_connection, file_id=file_row["file_id"], timeout=60)
        outcome = poll_ingestion_run(
            analytics_connection,
            run_row["idempotency_key"],
            timeout=_INGEST_TIMEOUT_SECONDS,
        )
        assert outcome["status"] == "SUCCEEDED", (
            f"run {run_row['idempotency_key']!r} finished {outcome['status']!r}, not SUCCEEDED -- "
            f"a 20% structural-rejection rate should never fail this dataset's own run (no "
            f"rejection_rate_threshold configured for orders.yaml at all)"
        )

        with analytics_owner_connection.cursor() as cur:
            cur.execute(
                "SELECT source_row_number, error_type, error_column "
                "FROM meta.rejected_records WHERE run_id = %s ORDER BY source_row_number",
                (run_row["run_id"],),
            )
            rejected = cur.fetchall()
        assert len(rejected) == 2, (
            f"expected exactly 2 meta.rejected_records rows for run_id={run_row['run_id']!r}, "
            f"found {len(rejected)}: {rejected!r}"
        )
        source_row_numbers = {row[0] for row in rejected}
        assert source_row_numbers == {4, 6}, (
            f"expected the ragged rows at 0-indexed source_row_number 4 (short) and 6 (long), got "
            f"{source_row_numbers!r}"
        )
        for _source_row_number, error_type, _error_column in rejected:
            assert error_type == "RAGGED_ROW", (
                f"expected every rejected row's error_type to be 'RAGGED_ROW', got {error_type!r}"
            )

        with analytics_connection.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM normalized.orders WHERE order_id BETWEEN %s AND %s",
                (order_ids[0], order_ids[-1]),
            )
            row = cur.fetchone()
            assert row is not None
            total = row[0]
        assert total == 8, (
            f"expected exactly 8 good rows loaded into normalized.orders for this run's own "
            f"order_id window [{order_ids[0]}, {order_ids[-1]}], found {total} -- a value below 8 "
            f"means a good row was wrongly dropped; a value above means a ragged row leaked "
            f"through or the good rows were duplicated"
        )
    finally:
        with contextlib.suppress(Exception):
            admin.delete_object(Bucket="raw", Key=key)
