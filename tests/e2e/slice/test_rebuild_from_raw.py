"""tests/e2e/slice/test_rebuild_from_raw.py -- 11-12-PLAN.md's live D-29 four-part proof + D-34.

Plan 11-12's capstone: proves, against the REAL live cluster, that `scripts/rebuild-from-raw.py`
(invoked exactly as `make rebuild-from-raw`/a real disaster-recovery operator would run it, D-32)
genuinely reconciles the analytical warehouse back to its pre-drop state on all four D-29
dimensions, and that D-34's "previously-resolved quarantine records return to PENDING" property
holds as an explicit, asserted PASS condition -- never silently excluded from the comparison.

Per D-30, this test does NOT seed a large corpus -- it seeds a SMALL, fully-traceable pair of
customers files of its own (mirroring `test_backfill_reentry.py`'s own `_build_customers_csv`
shape) specifically so the D-34 assertion has a known, controlled business key to track across
the drop, rather than depending on whatever quarantine-resolution history the rest of the suite
happened to leave lying around. Neither file is ever deleted -- and since
debug/ci-pipeline-ingestion-timeout ROUND 16 (finding 19-A) NO test in this suite deletes a raw
upload whose data published (section 63/ADR-0011 raw immutability): this test's OWN premise
(INCR-07's whole point) requires the FULL raw history to still be present in `raw/` for the
rebuild's own reprocessing, since `rebuild-from-raw.py` reconstructs strictly from raw plus
versioned configuration (D-28), never from anything this suite deletes out from under it.

## Why the correction resolves to REDRIVEN before the drop, but reverts to PENDING after it

Both files share one lexicographically-ordered filename pair: `...-corrected.csv` sorts BEFORE
`...-original.csv` (`c` < `o`). Before the drop, the ORIGINAL (bad) file is uploaded and processed
FIRST in real time (producing a PENDING reject), and the CORRECTED file is uploaded and processed
SECOND (its publish resolves that reject to REDRIVEN via `resolve_rejected_records_for_business_
keys`, D-23) -- real-time upload order, not filename order, drove processing order.

After the drop, `rebuild-from-raw.py`'s own `_trigger_backfills` triggers ONE bucket-wide,
date-agnostic backfill covering the dataset's WHOLE `raw/` history (Pitfall 1: `discover_files` is
never date-scoped) -- and `list_matched_keys`/`discover_files`' own S3 listing enumerates objects
in lexicographic key order. That means the REBUILD's own reprocessing sees the CORRECTED file
BEFORE the ORIGINAL file: the correction publishes cleanly (no PENDING reject exists yet to
resolve), and only THEN does the original bad file publish, producing a FRESH PENDING reject that
nothing downstream ever resolves again. This filename ordering is deliberate, not incidental -- it
is what makes D-34's "quarantine-resolution history is lost on rebuild" property concretely
observable with a single, controlled pair of files rather than a matter of chance.

## Connection-lock discipline (T-11-31's own concern, applied here)

`analytics_owner_connection`/`analytics_connection` are psycopg connections opened once per test
(session default: NOT autocommit) -- every `SELECT` inside an open, uncommitted transaction holds
an `AccessShareLock` on the tables it touched until `.commit()`/`.rollback()`. `rebuild-from-raw.py`
issues `DROP SCHEMA ... CASCADE` against those exact schemas from an ENTIRELY SEPARATE connection
(its own fresh port-forward, per this plan's own Task 1 port-forward fix) -- `DROP SCHEMA` needs an
`ACCESS EXCLUSIVE` lock, which conflicts with any lock this test's own connections are still
holding. This test explicitly `.commit()`s both analytics connections immediately before invoking
the rebuild subprocess, so the drop is never left waiting on a lock this test itself forgot to
release.
"""

from __future__ import annotations

import subprocess
import sys
import time
import uuid
from typing import TYPE_CHECKING, Any

import pytest

from dataplat.pipeline.rebuild_reconciliation import (
    compare_snapshots,
    snapshot_customers_scd2_state,
    snapshot_table_state,
)
from dataplat.pipeline.run import _TARGET_COLUMNS_BY_DATASET  # reuse, never re-derive (T-09-03)
from tests.e2e.slice.conftest import (
    poll_file_discovered,
    poll_ingestion_run,
    poll_run_for_file,
    snapshot_complete_customers_csv,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    import psycopg

pytestmark = pytest.mark.cluster

_CUSTOMERS_DAG_ID = "csv_ingest_customers"
_CUSTOMERS_DATASET = "customers"

_CUSTOMERS_BUSINESS_COLUMNS = _TARGET_COLUMNS_BY_DATASET["customers"]
_ORDERS_BUSINESS_COLUMNS = _TARGET_COLUMNS_BY_DATASET["orders"]

_DISCOVERY_TIMEOUT_SECONDS = 180
_INGEST_TIMEOUT_SECONDS = 180
_POLL_INTERVAL_SECONDS = 1.0

# The rebuild's own backfill re-processes this cluster's ENTIRE customers raw/ history (not just
# this test's own 2 files) -- this session's own live timing (11-12-SUMMARY.md) observed a single
# ~2-minute-window backfill taking 5-10+ minutes end to end under this host's documented CPU
# contention (STATE.md's own recurring "node CPU budget" notes). 30 minutes gives real headroom
# without masking a genuine hang.
_BACKFILL_SETTLE_TIMEOUT_SECONDS = 1800.0
_REBUILD_SUBPROCESS_TIMEOUT_SECONDS = 1800.0

# customer_id is `sa.Integer()` (migration 0005, signed 4-byte -- max 2_147_483_647). Disjoint
# from every other live-cluster customer_id range this suite's other files already use:
# test_pod_kill_retry.py/test_concurrent_select.py [2_000_000, 1_000_000_000),
# test_referential_orphan.py [1_500_000_000, 1_999_000_000), test_backfill_reentry.py
# [2_000_000_000, 2_100_000_000) -- this file's own band starts just above that last one and
# stays comfortably under the int4 ceiling.
_CUSTOMER_ID_LOW = 2_101_000_000
_CUSTOMER_ID_HIGH = 2_140_000_000


def _build_customers_csv(
    conn: psycopg.Connection[Any],
    *,
    base_customer_id: int,
    bad_row_name: str,
) -> bytes:
    """Build a SNAPSHOT-COMPLETE customers CSV: roster echo + 3 valid rows + 1 empty-`name` row.

    Verbatim shape of `test_backfill_reentry.py`'s own `_build_customers_csv` (same column
    order, same live-proven `QUALITY_COMPLETENESS` trip on `name`, same
    `conftest.snapshot_complete_customers_csv` roster echo -- debug/ci-pipeline-ingestion-
    timeout ROUND 16, finding 19-A: customers' full-snapshot contract makes a lone 4-row
    file a -- correct -- mass-delete breaker trip, ROUND 15's live run 764) -- not
    re-derived, copied, matching this codebase's own per-tier-copy convention for small
    test-fixture builders.

    NOTE the rebuild-time property this preserves: during the rebuild's own backfill the
    raw history reprocesses in lexicographic key order with the schemas EMPTY, so each
    echo fixture is a superset snapshot of everything staged before it (corpus files sort
    before `e2e-*` keys) -- vanished stays 0 during reprocessing too.
    """
    rows = [
        (base_customer_id, "Anna Kowalski", "PL", "1950-03-14", "2026-01-05T08:15:00Z"),
        (base_customer_id + 1, "James Smith", "US", "1962-12-25", "2026-02-02T05:03:27Z"),
        (base_customer_id + 2, "Sophie Muller", "GB", "1974-03-19", "2026-03-16T22:37:52Z"),
        (base_customer_id + 3, bad_row_name, "PL", "1988-12-01", "2026-04-13T16:49:05Z"),
    ]
    return snapshot_complete_customers_csv(
        conn,
        extra_rows=[
            (str(cid), name, country, bdate, ets) for cid, name, country, bdate, ets in rows
        ],
    )


def _dataset_id(conn: psycopg.Connection[Any], name: str) -> int:
    """Resolve `meta.datasets.dataset_id` for `name` -- re-resolved AFTER the rebuild too, since
    `meta` (and therefore `meta.datasets`) is dropped and its Identity PK sequence restarts.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT dataset_id FROM meta.datasets WHERE dataset_name = %s", (name,))
        row = cur.fetchone()
    assert row is not None, f"meta.datasets has no row for dataset_name={name!r}"
    return int(row[0])


def _fetch_pending_completeness_reject(
    conn: psycopg.Connection[Any],
    *,
    run_id: int,
) -> dict[str, Any]:
    """Same shape as `test_backfill_reentry.py`'s own helper of the same name."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT rejected_record_id, resolution_type, resolved_by_run_id, business_key
              FROM meta.rejected_records
             WHERE run_id = %s AND error_type = 'COMPLETENESS_VIOLATION' AND error_column = 'name'
            """,
            (run_id,),
        )
        rows = cur.fetchall()
    assert len(rows) == 1, (
        f"expected exactly one COMPLETENESS_VIOLATION reject (column='name') for run_id="
        f"{run_id!r}, found {len(rows)}: {rows!r}"
    )
    rejected_record_id, resolution_type, resolved_by_run_id, business_key = rows[0]
    return {
        "rejected_record_id": rejected_record_id,
        "resolution_type": resolution_type,
        "resolved_by_run_id": resolved_by_run_id,
        "business_key": business_key,
    }


def _fetch_resolution_type_for_business_key(
    conn: psycopg.Connection[Any],
    *,
    dataset_id: int,
    business_key: str,
) -> str | None:
    """The post-rebuild lookup for D-34: `meta.rejected_records` joined through `meta.batches`
    (D-23's own `(dataset_id, business_key)` matching predicate -- `rejected_records` itself
    carries no direct `dataset_id` column, mirroring `resolve_rejected_records_for_business_
    keys`'s own `UPDATE ... FROM meta.batches` join in `metadata/postgres.py`).

    Returns the most recent matching row's `resolution_type`, or `None` if no row exists at all
    for this `(dataset_id, business_key)` pair (a genuinely different, and equally informative,
    failure mode from "exists but not PENDING").
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT rr.resolution_type
              FROM meta.rejected_records rr
              JOIN meta.batches b ON b.batch_id = rr.batch_id
             WHERE b.dataset_id = %s AND rr.business_key = %s
             ORDER BY rr.rejected_record_id DESC
             LIMIT 1
            """,
            (dataset_id, business_key),
        )
        row = cur.fetchone()
    return None if row is None else str(row[0])


def _wait_for_business_key_pending(
    conn: psycopg.Connection[Any],
    *,
    dataset_id: int,
    business_key: str,
    timeout: float,
) -> str | None:
    """Poll for a `meta.rejected_records` row to exist for `(dataset_id, business_key)`.

    The rebuild's own triggered backfill re-processes the ORIGINAL bad file asynchronously (it is
    not this test's own synchronous action) -- this polls (never a blind `sleep`) until either a
    row appears or `timeout` elapses, returning the last-observed `resolution_type` (`None` if no
    row was ever observed).
    """
    deadline = time.monotonic() + timeout
    last: str | None = None
    while time.monotonic() < deadline:
        last = _fetch_resolution_type_for_business_key(
            conn, dataset_id=dataset_id, business_key=business_key
        )
        if last is not None:
            return last
        time.sleep(_POLL_INTERVAL_SECONDS)
    return last


def _reconciliation_row_count_for_dataset(conn: psycopg.Connection[Any], *, dataset_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM meta.reconciliation_results WHERE dataset_id = %s",
            (dataset_id,),
        )
        row = cur.fetchone()
    assert row is not None  # pragma: no cover -- count(*) always returns exactly one row
    return int(row[0])


def _latest_backfill_id(conn: psycopg.Connection[Any], *, dag_id: str) -> int | None:
    """Same shape as `test_backfill_2year_sweep.py`'s own helper of the same name."""
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM backfill WHERE dag_id = %s ORDER BY id DESC LIMIT 1", (dag_id,))
        row = cur.fetchone()
    return None if row is None else int(row[0])


def _wait_for_new_backfill_completed(
    conn: psycopg.Connection[Any],
    *,
    dag_id: str,
    since_backfill_id: int | None,
    timeout: float,
) -> int:
    """Same shape as `test_backfill_2year_sweep.py`'s own helper of the same name."""
    deadline = time.monotonic() + timeout
    last_seen: tuple[int, Any] | None = None
    while time.monotonic() < deadline:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, completed_at FROM backfill WHERE dag_id = %s ORDER BY id DESC LIMIT 1",
                (dag_id,),
            )
            row = cur.fetchone()
        if row is not None:
            backfill_id, completed_at = row
            if since_backfill_id is None or backfill_id > since_backfill_id:
                last_seen = (backfill_id, completed_at)
                if completed_at is not None:
                    return int(backfill_id)
        time.sleep(_POLL_INTERVAL_SECONDS)
    msg = (
        f"no NEW backfill row for dag_id={dag_id!r} (since_backfill_id={since_backfill_id!r}) "
        f"reached completed_at within {timeout}s (last observed: {last_seen!r})"
    )
    raise AssertionError(msg)


def _wait_for_all_raw_files_settled(
    conn: psycopg.Connection[Any],
    s3_app: Any,
    *,
    dataset: str,
    prefix: str,
    timeout: float,
) -> None:
    """Poll until EVERY raw `.csv` object under `prefix` is discovered AND terminal in meta.

    Added by debug/ci-pipeline-ingestion-timeout ROUND 16 (finding 19-A's
    rebuild leg): `rebuild-from-raw.py` only *triggers* the customers
    backfill -- `orders` rebuilds via the ASSET CASCADE (the script's own
    `_dry_run_supports_backfill` probe skips asset-scheduled DAGs by
    design), so a post-rebuild snapshot taken the moment the customers
    `backfill.completed_at` lands RACES the still-draining orders (and any
    trailing customers) reprocessing. This wait closes that race honestly:
    the authoritative "everything raw has been reprocessed" signal is every
    raw object having a `meta.files` row that is either a DUPLICATE
    (`duplicate_of_file_id` set -- no run is ever allocated for one,
    discovery's own D-13 semantics) or whose NEWEST `meta.ingestion_runs`
    row is terminal. Query shape copied from
    `test_backfill_2year_sweep.py::_wait_for_dataset_files_terminal`
    (DISTINCT ON + `run_id DESC` -- the newest attempt is the one that
    matters), extended with the duplicate carve-out because this wait runs
    against the WHOLE bucket prefix, not a manifest of known-unique files.
    """
    paginator = s3_app.get_paginator("list_objects_v2")
    filenames = [
        obj["Key"].rsplit("/", 1)[-1]
        for page in paginator.paginate(Bucket="raw", Prefix=prefix)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".csv")
    ]
    if not filenames:
        # A dataset with no raw history has nothing to settle (e.g. a bare
        # cluster where nothing ever uploaded orders) -- an empty prefix is
        # a legitimate no-op here, not a failure: this test's OWN seeded
        # files guarantee the customers call is never empty.
        return

    deadline = time.monotonic() + timeout
    pending: dict[str, str | None] = {}
    while time.monotonic() < deadline:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (f.filename)
                       f.filename, f.duplicate_of_file_id, ir.status
                  FROM meta.files f
                  JOIN meta.datasets d ON d.dataset_id = f.dataset_id
                  LEFT JOIN meta.ingestion_runs ir ON ir.file_id = f.file_id
                 WHERE d.dataset_name = %s AND f.filename = ANY(%s)
                 ORDER BY f.filename, ir.run_id DESC NULLS LAST
                """,
                (dataset, filenames),
            )
            rows = cur.fetchall()
        settled: set[str] = set()
        pending = {}
        for filename, duplicate_of_file_id, status in rows:
            if duplicate_of_file_id is not None or status in _SETTLE_TERMINAL_RUN_STATUSES:
                settled.add(filename)
            else:
                pending[filename] = status
        missing = set(filenames) - settled - pending.keys()
        if not pending and not missing:
            return
        for name in missing:
            pending[name] = "<undiscovered>"
        time.sleep(_POLL_INTERVAL_SECONDS)
    msg = (
        f"dataset={dataset!r}: {len(pending)} of {len(filenames)} raw files never settled "
        f"within {timeout}s after the rebuild backfill completed -- still pending: "
        f"{dict(sorted(pending.items()))}"
    )
    raise AssertionError(msg)


# Same terminal set as conftest._TERMINAL_RUN_STATUSES (copied per this file's own
# already-established local-copy convention for sweep helpers).
_SETTLE_TERMINAL_RUN_STATUSES = frozenset(
    {"SUCCEEDED", "FAILED", "SKIPPED_DUPLICATE", "SKIPPED_CONCURRENT", "QUARANTINED"},
)


def test_rebuild_from_raw_reconciles_and_reverts_quarantine_to_pending(  # noqa: PLR0915
    repo_root: Path,
    s3_client: Callable[[str], Any],
    analytics_connection: psycopg.Connection[Any],
    analytics_owner_connection: psycopg.Connection[Any],
    airflow_metadata_connection: psycopg.Connection[Any],
) -> None:
    # PLR0915 (too many statements): this test's 6 numbered steps (module docstring) each need
    # their own named local + explicit assertion per this plan's own acceptance criteria ("each
    # its own explicit assertion, not folded into one opaque boolean") -- matches
    # test_backfill_2year_sweep.py's own identical PLR0915 exception for the same reason.
    """11-12-PLAN.md's live D-29 four-part proof, plus D-34's explicit revert-to-PENDING proof.

    Structure (module docstring explains the WHY behind each choice):

    0. Seed one small, fully-traceable customers correction pair (never deleted) -- upload the
       bad file, wait for its PENDING reject, upload the corrected file, wait for the reject to
       resolve to REDRIVEN. This is the "previously-resolved quarantine record" D-34 requires.
    1. Snapshot pre-drop state: `normalized.customers` (SCD2-aware) + `normalized.orders`
       (D-29 points 1-3), plus the one business key's REDRIVEN state (D-34's own pre-drop fact).
    2. Commit both analytics connections (module docstring's lock-discipline note), then invoke
       the REAL `scripts/rebuild-from-raw.py` as a subprocess -- the exact same script `make
       rebuild-from-raw`/a real operator invokes (D-32).
    3. Wait for the rebuild's own triggered `csv_ingest_customers` backfill to reach a terminal
       state (never a blind sleep).
    4. Snapshot post-rebuild state the same way, `compare_snapshots()` it against step 1 --
       row counts, checksum, SCD2 per-key state are each their own explicit assertion (D-29
       points 1-3).
    5. Assert `meta.reconciliation_results` gained real rows for the customers dataset during the
       rebuild's own backfill (D-29 point 4 -- a side effect of step 3, not computed here).
    6. Assert the one business key from step 0 is back to PENDING (D-34) -- an explicit pass
       condition, never filtered out of steps 4/5's comparisons.
    """
    app = s3_client("app")

    marker = uuid.uuid4().hex[:12]
    id_span = _CUSTOMER_ID_HIGH - _CUSTOMER_ID_LOW
    base_customer_id = _CUSTOMER_ID_LOW + (uuid.uuid4().int % id_span)
    bad_customer_id = base_customer_id + 3
    bad_business_key = str(bad_customer_id)

    original_payload = _build_customers_csv(
        analytics_connection, base_customer_id=base_customer_id, bad_row_name=""
    )

    # Lexicographic "corrected" < "original" is load-bearing -- see module docstring's "Why the
    # correction resolves ... reverts to PENDING" section.
    original_key = f"customers/e2e-rebuild-{marker}-original.csv"
    corrected_key = f"customers/e2e-rebuild-{marker}-corrected.csv"
    original_uri = f"s3://raw/{original_key}"
    corrected_uri = f"s3://raw/{corrected_key}"

    # --- Step 0: seed the one controlled, fully-traceable correction pair ------------------
    app.put_object(Bucket="raw", Key=original_key, Body=original_payload)

    original_file = poll_file_discovered(
        analytics_connection,
        dataset=_CUSTOMERS_DATASET,
        object_uri=original_uri,
        timeout=_DISCOVERY_TIMEOUT_SECONDS,
    )
    assert original_file["duplicate_of_file_id"] is None, (
        f"the freshly-marked original customers file was already flagged a duplicate of "
        f"file_id={original_file['duplicate_of_file_id']!r} -- the uuid marker did not make "
        f"this content genuinely new"
    )

    original_run = poll_run_for_file(
        analytics_connection, file_id=original_file["file_id"], timeout=60
    )
    original_outcome = poll_ingestion_run(
        analytics_connection, original_run["idempotency_key"], timeout=_INGEST_TIMEOUT_SECONDS
    )
    assert original_outcome["status"] == "SUCCEEDED", (
        f"original bad-row run finished {original_outcome['status']!r}, not SUCCEEDED"
    )

    pending_reject = _fetch_pending_completeness_reject(
        analytics_owner_connection, run_id=original_run["run_id"]
    )
    assert pending_reject["resolution_type"] == "PENDING"
    assert pending_reject["business_key"] == bad_business_key, (
        f"expected the rejected row's business_key to be {bad_business_key!r} (the raw CSV "
        f"customer_id value), got {pending_reject['business_key']!r}"
    )

    corrected_payload = _build_customers_csv(
        analytics_connection, base_customer_id=base_customer_id, bad_row_name="Corrected Name"
    )
    app.put_object(Bucket="raw", Key=corrected_key, Body=corrected_payload)

    corrected_file = poll_file_discovered(
        analytics_connection,
        dataset=_CUSTOMERS_DATASET,
        object_uri=corrected_uri,
        timeout=_DISCOVERY_TIMEOUT_SECONDS,
    )
    assert corrected_file["duplicate_of_file_id"] is None

    corrected_run = poll_run_for_file(
        analytics_connection, file_id=corrected_file["file_id"], timeout=60
    )
    corrected_outcome = poll_ingestion_run(
        analytics_connection, corrected_run["idempotency_key"], timeout=_INGEST_TIMEOUT_SECONDS
    )
    assert corrected_outcome["status"] == "SUCCEEDED", (
        f"corrected-file run finished {corrected_outcome['status']!r}, not SUCCEEDED"
    )

    resolved_reject = _fetch_pending_completeness_reject(
        analytics_owner_connection, run_id=original_run["run_id"]
    )
    assert resolved_reject["resolution_type"] == "REDRIVEN", (
        f"expected the seeded reject to resolve to REDRIVEN via publish-time business-key-scoped "
        f"resolution before this test's own drop, got {resolved_reject['resolution_type']!r} -- "
        f"see module docstring's 'Why the correction resolves to REDRIVEN' section"
    )
    assert resolved_reject["resolved_by_run_id"] == corrected_run["run_id"]

    # --- Step 1: pre-drop snapshots (D-29 points 1-3) -------------------------------------
    customers_before = snapshot_customers_scd2_state(
        analytics_owner_connection, business_columns=_CUSTOMERS_BUSINESS_COLUMNS
    )
    orders_before = snapshot_table_state(
        analytics_owner_connection,
        "normalized.orders",
        business_columns=_ORDERS_BUSINESS_COLUMNS,
        business_key_column="order_id",
    )
    since_backfill_id = _latest_backfill_id(airflow_metadata_connection, dag_id=_CUSTOMERS_DAG_ID)

    # --- Step 2: release every lock this test's own connections hold, then rebuild --------
    analytics_owner_connection.commit()
    analytics_connection.commit()

    rebuild_script = repo_root / "scripts" / "rebuild-from-raw.py"
    proc = subprocess.run(  # noqa: S603
        [sys.executable, str(rebuild_script)],
        capture_output=True,
        text=True,
        check=False,
        timeout=_REBUILD_SUBPROCESS_TIMEOUT_SECONDS,
    )
    assert proc.returncode == 0, (
        f"scripts/rebuild-from-raw.py failed (exit {proc.returncode}):\n"
        f"{proc.stdout}\n{proc.stderr}"
    )

    # --- Step 3: wait for the rebuild's own triggered backfill to settle ------------------
    _wait_for_new_backfill_completed(
        airflow_metadata_connection,
        dag_id=_CUSTOMERS_DAG_ID,
        since_backfill_id=since_backfill_id,
        timeout=_BACKFILL_SETTLE_TIMEOUT_SECONDS,
    )
    # ROUND 16 (finding 19-A): the customers backfill completing does NOT mean the whole
    # rebuild has drained -- orders reprocesses via the asset cascade, trailing the
    # customers publishes, and late customers claims can trail the backfill's own DagRuns
    # too (capped claim batches). Snapshotting before BOTH datasets settle races the
    # cascade -- see `_wait_for_all_raw_files_settled`'s own docstring.
    _wait_for_all_raw_files_settled(
        analytics_connection,
        app,
        dataset=_CUSTOMERS_DATASET,
        prefix="customers/",
        timeout=_BACKFILL_SETTLE_TIMEOUT_SECONDS,
    )
    _wait_for_all_raw_files_settled(
        analytics_connection,
        app,
        dataset="orders",
        prefix="orders/",
        timeout=_BACKFILL_SETTLE_TIMEOUT_SECONDS,
    )

    # --- Step 4: post-rebuild snapshots + explicit, individually-named comparisons --------
    customers_dataset_id_after = _dataset_id(analytics_owner_connection, _CUSTOMERS_DATASET)
    customers_after = snapshot_customers_scd2_state(
        analytics_owner_connection, business_columns=_CUSTOMERS_BUSINESS_COLUMNS
    )
    orders_after = snapshot_table_state(
        analytics_owner_connection,
        "normalized.orders",
        business_columns=_ORDERS_BUSINESS_COLUMNS,
        business_key_column="order_id",
    )

    customers_comparison = compare_snapshots(customers_before, customers_after)
    assert customers_comparison.matches, (
        f"normalized.customers did not reconcile to its pre-drop state -- named mismatches: "
        f"{customers_comparison.mismatches!r}"
    )
    orders_comparison = compare_snapshots(orders_before, orders_after)
    assert orders_comparison.matches, (
        f"normalized.orders did not reconcile to its pre-drop state -- named mismatches: "
        f"{orders_comparison.mismatches!r}"
    )

    # Each D-29 point 1-3 dimension asserted individually and explicitly (acceptance criteria:
    # never folded into one opaque boolean), even though compare_snapshots() above already
    # covers all of them -- these re-state the SAME facts by name, not a new computation.
    assert customers_before.table_snapshot.row_count == customers_after.table_snapshot.row_count, (
        "D-29 point 1 (row count): normalized.customers row_count changed across the rebuild"
    )
    assert customers_before.table_snapshot.checksum == customers_after.table_snapshot.checksum, (
        "D-29 point 2 (checksum): normalized.customers business-column checksum changed across "
        "the rebuild"
    )
    assert customers_before.keys == customers_after.keys, (
        "D-29 point 3 (SCD2 state): normalized.customers per-customer_id version_count/"
        "valid_from/valid_to/is_current state changed across the rebuild"
    )

    # --- Step 5: D-29 point 4 -- the rebuild's own backfill re-exercised reconciliation, with
    # zero new bespoke per-file accounting code (a side effect of step 3, asserted here). -----
    reconciliation_rows_after = _reconciliation_row_count_for_dataset(
        analytics_owner_connection, dataset_id=customers_dataset_id_after
    )
    assert reconciliation_rows_after > 0, (
        f"expected meta.reconciliation_results to gain rows for the customers dataset "
        f"(dataset_id={customers_dataset_id_after!r}) as a side effect of the rebuild's own "
        f"backfill re-processing every raw file -- found {reconciliation_rows_after} rows"
    )

    # --- Step 6: D-34 -- the previously-REDRIVEN reject is back to PENDING, asserted as an
    # explicit pass condition, never excluded from steps 4/5's comparisons above. `meta`'s own
    # Identity PK sequence was dropped and re-migrated (very likely restarting), so this lookup
    # is deliberately scoped to `customers_dataset_id_after` -- the CURRENT, post-rebuild id --
    # never a pre-drop id this test never captured.
    post_rebuild_resolution_type = _wait_for_business_key_pending(
        analytics_owner_connection,
        dataset_id=customers_dataset_id_after,
        business_key=bad_business_key,
        timeout=_BACKFILL_SETTLE_TIMEOUT_SECONDS,
    )
    assert post_rebuild_resolution_type == "PENDING", (
        f"D-34: expected business_key={bad_business_key!r} (customer_id={bad_customer_id!r}) to "
        f"be back to resolution_type='PENDING' after the rebuild reprocessed both the corrected "
        f"file (lexicographically first) and the original bad file (lexicographically second) "
        f"from raw/ -- got {post_rebuild_resolution_type!r}. This is D-34's own expected "
        f"quarantine-resolution-history-loss property; it must PASS as a positive assertion, "
        f"never be worked around by excluding this row from the comparison"
    )
