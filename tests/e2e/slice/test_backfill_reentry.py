"""tests/e2e/slice/test_backfill_reentry.py — VALID-08's real, no-shortcuts proof.

D-01 locks "backfill" (never "redrive") as the ONLY re-entry mechanism for a
previously-rejected row: a corrected file re-ingests by triggering the SAME
ingestion DAG as a genuine Airflow backfill run. `tests/dagtest/
test_backfill_dagrun.py` (08-13) already proves a backfill DagRun's
MECHANICS (correct `logical_date`, correct task graph) with
`KubernetesPodOperator.execute` mocked -- this is the tier that proof
explicitly leaves open (08-13-PLAN.md's own docstring, citing
08-RESEARCH.md's Pitfall 3): the REAL resolution-state-transition logic
inside a REAL pod, against the REAL live cluster.

CLI shape, confirmed live against THIS cluster's own installed Airflow
before being locked here (the plan's own explicit instruction): Airflow
3.3.0 removed `airflow dags backfill` entirely (`airflow dags backfill
--help` -> "Command `dags backfill` has been removed. Please use `airflow
backfill create`"). The real command is::

    airflow backfill create --dag-id <dag_id> --from-date <iso> \
        --to-date <iso> --reprocess-behavior completed

`--reprocess-behavior completed` is required, not optional: `dag_run`
carries a live `UNIQUE (dag_id, logical_date)` constraint (verified via
`\\d dag_run` against this cluster's own Airflow metadata database) --
without it, backfill would refuse to touch a logical_date that already has
a terminal (`success`) `dag_run` row, which is exactly the case here (the
original, bad-row run already reached SUCCEEDED). Because of that same
uniqueness constraint, a backfill re-run of an already-existing
`logical_date` reuses the SAME `dag_run.run_id`/`id` (cleared and
re-executed, `dag_run.clear_number` incremented) rather than allocating a
brand-new one -- this test polls `clear_number` advancing past its
pre-backfill value as the "a genuinely new execution happened" signal,
never a new Airflow-level `run_id`.

`meta.ingestion_runs.run_id` (the analytical database's OWN run identity,
assigned per `idempotency_key` -- content hash + config hash + processor
image + schema version, `dataplat.discovery`'s own documented formula) is a
COMPLETELY SEPARATE identifier space from Airflow's `dag_run.run_id`/`.id`.
`meta.rejected_records.resolved_by_run_id` is an FK into the FORMER, never
the latter -- this test's resolution assertion below queries and asserts
against that analytical-database run identity exclusively.

Honest limit, stated plainly rather than hidden: `resolve_rejected_records_
for_batch` (D-05) is scoped by `meta.batches.batch_id`, and `batch_key` is a
pure function of the file's own `content_sha256`
(`dataplat.discovery.discover_files`'s own documented formula,
`f"{dataset_name}:{content_sha256_hex[:16]}"`). Correcting a row necessarily
changes the file's bytes, so the corrected re-upload discovers under a NEW
`file_id`/`batch_id`, distinct from the original bad file's. This test still
asserts the plan's own literal claim (the previously-PENDING row flips to
`REDRIVEN`, linked to the new run) exactly as written, because that is the
documented, LOCKED intent (08-CONTEXT.md D-05: "a batch's PENDING rows may
belong to a PRIOR run") -- if this specific assertion ever fails against a
fully-deployed live cluster, that is real, valuable information about a
content-hash/batch-scoping gap between the locked intent and the current
`discovery.py` implementation, not a flaw in this test. See this plan's own
SUMMARY.md for the full architecture analysis this docstring summarizes.

`airflow backfill create` itself carries a documented, live-confirmed
transient: `_create_backfill_dag_run_non_partitioned`'s `SELECT ... FOR
UPDATE SKIP LOCKED` on the target `dag_run` row can lose its lock race
against a concurrent transaction (almost certainly the scheduler's own
periodic `dag_run`-row locking), recording
`backfill_dag_run.exception_reason = 'in flight'` with CLI exit code 0 and
no re-execution at all -- Airflow's own code has no retry for a lost
`skip_locked` race. `.planning/debug/backfill-does-not-redrive-rejected-
row.md` confirms this live (source-traced against this cluster's own
installed `apache-airflow==3.3.0`, plus a manual re-invocation of the
identical CLI command that succeeded immediately on the second try, no
code change). `_run_backfill_and_wait_for_reexecution` below retries the
CLI invocation on this exact transient (bounded attempts, short backoff)
before falling through to its normal re-execution wait, and surfaces
`backfill_dag_run.exception_reason` in every failure message so a genuine
failure is never confused with this known-transient race again.
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
    import datetime
    import subprocess
    from collections.abc import Callable

    import psycopg

pytestmark = pytest.mark.cluster

_CUSTOMERS_DAG_ID = "csv_ingest_customers"
_CUSTOMERS_DATASET = "customers"
_DISCOVERY_TIMEOUT_SECONDS = 180
_INGEST_TIMEOUT_SECONDS = 180
_DAGRUN_LOOKUP_TIMEOUT_SECONDS = 60
_BACKFILL_DAGRUN_TIMEOUT_SECONDS = 300
_POLL_INTERVAL_SECONDS = 0.5

# .planning/debug/backfill-does-not-redrive-rejected-row.md: a lost
# `SELECT ... FOR UPDATE SKIP LOCKED` race inside Airflow's own
# `airflow backfill create` records `backfill_dag_run.exception_reason =
# 'in flight'` (exit code 0, no re-execution) with no retry anywhere in
# Airflow's own code. These three constants bound this test's own retry of
# that exact known-transient outcome.
_BACKFILL_CREATE_MAX_ATTEMPTS = 3
_BACKFILL_CREATE_RETRY_BACKOFF_SECONDS = 5.0
_BACKFILL_ROW_SETTLE_TIMEOUT_SECONDS = 15.0

# customer_id is `sa.Integer()` (migration 0005) -- disjoint from
# test_referential_orphan.py's own `[1_500_000_000, 1_999_000_000)` window
# and test_pod_kill_retry.py's `[2_000_000, 1_000_000_000)` window, so a
# concurrently-running slice-suite test can never collide with this file's
# own customer_id choices.
_CUSTOMER_ID_LOW = 2_000_000_000
_CUSTOMER_ID_HIGH = 2_100_000_000


def _build_customers_csv(*, base_customer_id: int, bad_row_name: str) -> bytes:
    """Build a `customers.yaml`-shaped CSV: 3 valid rows + 1 row with an empty `name`.

    Column order (`customer_id,name,country,birth_date,event_ts`) matches
    `configs/datasets/customers.yaml`'s own `columns:` block and
    `tests/fixtures/slice-corpus.yaml`'s own header, verbatim -- positional
    correspondence only (04-04-SUMMARY.md precedent). A 1-in-4 (25%)
    rejection rate stays comfortably under `customers.yaml`'s own
    `rejection_rate_threshold: 0.5` circuit breaker -- this test needs the
    run to reach SUCCEEDED with one genuine PENDING reject, not FAIL (which
    would roll back the reject row too, D-11).

    Args:
        base_customer_id: The first of 4 consecutive customer_ids this file
            uses.
        bad_row_name: The `name` value for the 4th (bad) row -- `""` for
            the original upload, a real name for the "corrected" re-upload.

    Returns:
        The CSV bytes.
    """
    rows = [
        (base_customer_id, "Anna Kowalski", "PL", "1950-03-14", "2026-01-05T08:15:00Z"),
        (base_customer_id + 1, "James Smith", "US", "1962-12-25", "2026-02-02T05:03:27Z"),
        (base_customer_id + 2, "Sophie Muller", "GB", "1974-03-19", "2026-03-16T22:37:52Z"),
        (base_customer_id + 3, bad_row_name, "PL", "1988-12-01", "2026-04-13T16:49:05Z"),
    ]
    lines = ["customer_id,name,country,birth_date,event_ts"]
    lines.extend(f"{cid},{name},{country},{bdate},{ets}" for cid, name, country, bdate, ets in rows)
    return ("\n".join(lines) + "\n").encode("utf-8")


def _fetch_pending_completeness_reject(
    conn: psycopg.Connection[Any],
    *,
    run_id: int,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT rejected_record_id, batch_id, resolution_type, resolved_by_run_id
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
    rejected_record_id, batch_id, resolution_type, resolved_by_run_id = rows[0]
    return {
        "rejected_record_id": rejected_record_id,
        "batch_id": batch_id,
        "resolution_type": resolution_type,
        "resolved_by_run_id": resolved_by_run_id,
    }


def _fetch_latest_backfill_dag_run_row(
    conn: psycopg.Connection[Any],
    *,
    dag_id: str,
    logical_date: datetime.datetime,
) -> tuple[bool, str | None]:
    """Return (row_found, exception_reason) for the most recent backfill invocation.

    `.planning/debug/backfill-does-not-redrive-rejected-row.md` (live SQL +
    source trace of the installed `apache-airflow==3.3.0`) is the source of
    this schema/behavior: `backfill` holds one row per `airflow backfill
    create` invocation (`id` autoincrements -- a retried invocation for the
    SAME dag_id/logical_date gets a NEW `backfill.id`, confirmed live);
    `backfill_dag_run` holds one row per `(backfill_id, logical_date)` pair,
    with `exception_reason` set to the literal string `"in flight"` on a
    lost `SELECT ... FOR UPDATE SKIP LOCKED` race and left NULL on success.
    Ordering by `b.id DESC, bdr.id DESC` and taking the first row returns
    the outcome of the MOST RECENT invocation only.

    `row_found` is returned separately from `exception_reason` because both
    "no row yet" and "row exists with exception_reason NULL (success)" would
    otherwise collapse to the same `None` return value -- a caller polling
    for "did this invocation register" cannot tell those two states apart
    from `exception_reason` alone (this project's own code review, WR-01/
    WR-02, flagged the resulting ambiguity when they were collapsed).

    Args:
        conn: The `airflow_metadata_connection` fixture.
        dag_id: The target DAG.
        logical_date: The target `dag_run.logical_date` value.

    Returns:
        `(True, exception_reason)` if a `backfill_dag_run` row exists for
        this dag_id/logical_date (`exception_reason` is `"in flight"` on a
        lost lock race, `None` on success); `(False, None)` if no such row
        exists yet.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT bdr.exception_reason
              FROM backfill_dag_run bdr
              JOIN backfill b ON b.id = bdr.backfill_id
             WHERE b.dag_id = %s AND bdr.logical_date = %s
             ORDER BY b.id DESC, bdr.id DESC
             LIMIT 1
            """,
            (dag_id, logical_date),
        )
        row = cur.fetchone()
    if row is None:
        return (False, None)
    return (True, row[0])


def _wait_for_backfill_dag_run_row(
    conn: psycopg.Connection[Any],
    *,
    dag_id: str,
    logical_date: datetime.datetime,
    attempt: int,
) -> str | None:
    """Poll until a `backfill_dag_run` row is observed, then return its `exception_reason`.

    Settle loop: breaks as soon as a row is observed at all (`row_found`),
    regardless of `exception_reason` -- NOT on "exception_reason is not
    None" (that conflated "no row yet" with "row found, NULL/success",
    this project's own code review WR-01/WR-02). This is a defensive safety
    margin, not a confirmed async-write requirement: the debug log's own
    manual reproduction shows the row is written synchronously before the
    CLI process exits, so this loop should normally observe `row_found`
    True on its very first check.

    Args:
        conn: The `airflow_metadata_connection` fixture.
        dag_id: The target DAG.
        logical_date: The target `dag_run.logical_date` value.
        attempt: The current 1-indexed retry attempt (for the error message
            only).

    Returns:
        The observed `exception_reason` (`"in flight"` or `None` for
        success).

    Raises:
        AssertionError: No `backfill_dag_run` row was observed within
            `_BACKFILL_ROW_SETTLE_TIMEOUT_SECONDS` -- a distinct failure
            mode from the documented "in flight" race, not retried
            automatically.
    """
    settle_deadline = time.monotonic() + _BACKFILL_ROW_SETTLE_TIMEOUT_SECONDS
    row_found = False
    exception_reason: str | None = None
    while time.monotonic() < settle_deadline:
        row_found, exception_reason = _fetch_latest_backfill_dag_run_row(
            conn,
            dag_id=dag_id,
            logical_date=logical_date,
        )
        if row_found:
            return exception_reason
        time.sleep(_POLL_INTERVAL_SECONDS)
    msg = (
        f"airflow backfill create for dag_id={dag_id!r} logical_date={logical_date!r} "
        f"reported CLI success (attempt {attempt}/{_BACKFILL_CREATE_MAX_ATTEMPTS}) but no "
        f"backfill_dag_run row was observed within {_BACKFILL_ROW_SETTLE_TIMEOUT_SECONDS}s -- "
        f"this is a distinct failure mode from the documented 'in flight' race (see "
        f".planning/debug/backfill-does-not-redrive-rejected-row.md) and is NOT retried "
        f"automatically; the logical_date-matching precondition this query depends on may have "
        f"silently broken"
    )
    raise AssertionError(msg)


def _invoke_backfill_create_once(
    kubectl_fn: Callable[..., subprocess.CompletedProcess[str]],
    *,
    dag_id: str,
    logical_date_iso: str,
) -> subprocess.CompletedProcess[str]:
    """Invoke `airflow backfill create` exactly once via `kubectl exec`, asserting success.

    Args:
        kubectl_fn: The `kubectl` fixture callable.
        dag_id: The target DAG.
        logical_date_iso: The `--from-date`/`--to-date` value, ISO 8601.

    Returns:
        The completed `kubectl exec ... airflow backfill create ...` process.

    Raises:
        AssertionError: The CLI invocation itself fails (non-zero exit).
    """
    backfill = kubectl_fn(
        "-n",
        "airflow",
        "exec",
        "deploy/airflow-api-server",
        "--",
        "airflow",
        "backfill",
        "create",
        "--dag-id",
        dag_id,
        "--from-date",
        logical_date_iso,
        "--to-date",
        logical_date_iso,
        "--reprocess-behavior",
        "completed",
    )
    assert backfill.returncode == 0, (
        f"airflow backfill create --dag-id {dag_id} --from-date {logical_date_iso} "
        f"--to-date {logical_date_iso} --reprocess-behavior completed failed "
        f"(exit {backfill.returncode}):\n{backfill.stdout}\n{backfill.stderr}"
    )
    return backfill


def _run_backfill_and_wait_for_reexecution(  # noqa: PLR0913 -- eight independently-named identity/timing values, a dataclass for one call site adds nothing
    kubectl_fn: Callable[..., subprocess.CompletedProcess[str]],
    airflow_conn: psycopg.Connection[Any],
    *,
    dag_id: str,
    dag_run_id: str,
    logical_date: datetime.datetime,
    logical_date_iso: str,
    pre_backfill_clear_number: int,
    timeout: float = _BACKFILL_DAGRUN_TIMEOUT_SECONDS,
) -> None:
    """Invoke the real `airflow backfill create` CLI, then wait for it to genuinely re-execute.

    Retries the CLI invocation itself (bounded by
    `_BACKFILL_CREATE_MAX_ATTEMPTS`, short fixed backoff
    `_BACKFILL_CREATE_RETRY_BACKOFF_SECONDS`) whenever
    `backfill_dag_run.exception_reason` observes the literal string
    `"in flight"` -- Airflow's own documented-transient lost `SELECT ...
    FOR UPDATE SKIP LOCKED` row-lock race (`.planning/debug/backfill-does-
    not-redrive-rejected-row.md`, confirmed live: a manual re-invocation of
    the identical CLI command with no code change succeeded immediately on
    the second try). Only once the retry loop observes an outcome OTHER
    than `"in flight"` (a `None` exception_reason -- Airflow's own success
    case -- or, defensively, any other non-`"in flight"` value) does this
    fall through to the pre-existing re-execution wait: polling
    `clear_number` advancing past its pre-backfill value (module
    docstring: the `UNIQUE (dag_id, logical_date)` constraint means
    backfilling an already-`success` logical_date reuses the SAME
    `dag_run` row, cleared and re-run, never a new one) is the "a
    genuinely new execution happened" signal this polls for, alongside
    `state == 'success'`.

    Args:
        kubectl_fn: The `kubectl` fixture callable.
        airflow_conn: The `airflow_metadata_connection` fixture.
        dag_id: The target DAG.
        dag_run_id: The target `dag_run.run_id` (Airflow's own identity,
            NOT `meta.ingestion_runs.run_id` -- module docstring).
        logical_date: The target `dag_run.logical_date` value, used for the
            `backfill_dag_run.exception_reason` lookup.
        logical_date_iso: The `--from-date`/`--to-date` value, ISO 8601.
        pre_backfill_clear_number: `dag_run.clear_number` observed BEFORE
            invoking backfill.
        timeout: Maximum seconds to wait for re-execution to `success`.

    Raises:
        AssertionError: The CLI invocation fails, every retry attempt still
            observes `"in flight"`, or re-execution never reaches
            `success` within `timeout`.
    """
    for attempt in range(1, _BACKFILL_CREATE_MAX_ATTEMPTS + 1):
        # WR-03 (code review): retry the CLI invocation itself, not just the
        # DB-observed "in flight" race -- a transient kubectl/CLI-level
        # failure is plausible on this cluster's own documented CPU/
        # scheduler contention and deserves the same retry budget.
        try:
            _invoke_backfill_create_once(
                kubectl_fn,
                dag_id=dag_id,
                logical_date_iso=logical_date_iso,
            )
        except AssertionError:
            if attempt == _BACKFILL_CREATE_MAX_ATTEMPTS:
                raise
            time.sleep(_BACKFILL_CREATE_RETRY_BACKOFF_SECONDS)
            continue

        exception_reason = _wait_for_backfill_dag_run_row(
            airflow_conn,
            dag_id=dag_id,
            logical_date=logical_date,
            attempt=attempt,
        )

        if exception_reason != "in flight":
            break

        if attempt < _BACKFILL_CREATE_MAX_ATTEMPTS:
            time.sleep(_BACKFILL_CREATE_RETRY_BACKOFF_SECONDS)
    else:
        msg = (
            f"airflow backfill create for dag_id={dag_id!r} logical_date={logical_date!r} "
            f"still observed backfill_dag_run.exception_reason='in flight' after "
            f"{_BACKFILL_CREATE_MAX_ATTEMPTS} attempts -- Airflow's own documented-transient "
            f"lost row-lock race (see .planning/debug/backfill-does-not-redrive-rejected-row.md) "
            f"did not clear within the retry budget"
        )
        raise AssertionError(msg)

    deadline = time.monotonic() + timeout
    last_state: str | None = None
    last_clear_number = pre_backfill_clear_number
    while time.monotonic() < deadline:
        with airflow_conn.cursor() as cur:
            cur.execute(
                "SELECT state, clear_number FROM dag_run WHERE dag_id = %s AND run_id = %s",
                (dag_id, dag_run_id),
            )
            row = cur.fetchone()
        if row is not None:
            last_state, last_clear_number = row
            if last_clear_number > pre_backfill_clear_number and last_state == "success":
                return
        time.sleep(_POLL_INTERVAL_SECONDS)
    _, latest_exception_reason = _fetch_latest_backfill_dag_run_row(
        airflow_conn,
        dag_id=dag_id,
        logical_date=logical_date,
    )
    msg = (
        f"dag_run[dag_id={dag_id!r}, run_id={dag_run_id!r}] never re-executed to 'success' "
        f"within {timeout}s after 'airflow backfill create' (pre-backfill clear_number="
        f"{pre_backfill_clear_number!r}, last observed: state={last_state!r}, "
        f"clear_number={last_clear_number!r}, latest backfill_dag_run.exception_reason="
        f"{latest_exception_reason!r})"
    )
    raise AssertionError(msg)


def _assert_row_resolved(
    conn: psycopg.Connection[Any],
    *,
    rejected_record_id: int,
    expected_resolved_by_run_id: int,
) -> None:
    """Assert the original rejected row now shows `resolution_type='REDRIVEN'`, linked correctly.

    Args:
        conn: An open connection to the analytical database.
        rejected_record_id: The original `meta.rejected_records.
            rejected_record_id` to re-query.
        expected_resolved_by_run_id: The corrected file's own
            `meta.ingestion_runs.run_id`.

    Raises:
        AssertionError: The row is missing, not `REDRIVEN`, or linked to a
            different run than expected.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT resolution_type, resolved_by_run_id FROM meta.rejected_records "
            "WHERE rejected_record_id = %s",
            (rejected_record_id,),
        )
        resolved_row = cur.fetchone()
    assert resolved_row is not None, (
        f"expected a meta.rejected_records row for rejected_record_id={rejected_record_id!r}, "
        f"found none"
    )
    resolution_type, resolved_by_run_id = resolved_row
    assert resolution_type == "REDRIVEN", (
        f"expected the original rejected_record_id={rejected_record_id!r} to show "
        f"resolution_type='REDRIVEN' after the backfill run, got {resolution_type!r} -- D-05 "
        f"requires a completed backfill run to resolve the batch's PENDING rejects; see this "
        f"module's own docstring for the documented content-hash/batch-scoping caveat if this "
        f"assertion is the one that fails"
    )
    assert resolved_by_run_id == expected_resolved_by_run_id, (
        f"expected resolved_by_run_id={expected_resolved_by_run_id!r} (the corrected file's "
        f"own meta.ingestion_runs.run_id), got {resolved_by_run_id!r}"
    )


def _fetch_dagrun_identity(
    conn: psycopg.Connection[Any],
    *,
    run_id: int,
    timeout: float = _DAGRUN_LOOKUP_TIMEOUT_SECONDS,
) -> tuple[str, str]:
    """Poll `meta.ingestion_runs` for the `(dag_id, dag_run_id)` a run was claimed under.

    Populated by `claim_ingestion_run`'s own `dag_id`/`dag_run_id` columns
    (Phase 7's `dag_id`/`dag_run_id`/`task_id` wiring, `PROJECT.md`'s own
    "Phase 7 complete" note) -- non-NULL only once a real KPO pod has
    claimed the run, which may lag a moment behind the run reaching
    SUCCEEDED in `poll_ingestion_run`'s own eyes on a fast pod.
    """
    deadline = time.monotonic() + timeout
    last: tuple[Any, Any] | None = None
    while time.monotonic() < deadline:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT dag_id, dag_run_id FROM meta.ingestion_runs WHERE run_id = %s",
                (run_id,),
            )
            row = cur.fetchone()
        if row is not None:
            last = (row[0], row[1])
            if row[0] and row[1]:
                return str(row[0]), str(row[1])
        time.sleep(_POLL_INTERVAL_SECONDS)
    msg = (
        f"meta.ingestion_runs.dag_id/dag_run_id never populated for run_id={run_id!r} within "
        f"{timeout}s (last observed: {last!r})"
    )
    raise AssertionError(msg)


def test_backfill_resolves_previously_rejected_row(
    s3_client: Callable[[str], Any],
    analytics_connection: psycopg.Connection[Any],
    analytics_owner_connection: psycopg.Connection[Any],
    airflow_metadata_connection: psycopg.Connection[Any],
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    """VALID-08/D-01/D-05, live: a real `airflow backfill create` re-entry, real resolution.

    Uploads a `customers` CSV with one row failing `customers.yaml`'s real
    `QUALITY_COMPLETENESS` rule on `name` -- waits for the run to SUCCEED
    with that row `PENDING` in `meta.rejected_records` -- looks up the
    Airflow `dag_run` that processed it (via `meta.ingestion_runs`'
    `dag_id`/`dag_run_id` columns) -- uploads a corrected version of the
    same 4 rows -- invokes a genuine `airflow backfill create` for that
    SAME `dag_id`/`logical_date` (`--reprocess-behavior completed`, the
    live-confirmed CLI shape this module's own docstring documents) --
    waits for the backfilled `dag_run` to re-execute to `success`
    (`clear_number` advancing is the "genuinely re-ran" signal, module
    docstring) -- then asserts, via direct SQL, that the corrected row is
    published to `normalized.customers` and that the original `PENDING`
    reject now shows `resolution_type='REDRIVEN'` with `resolved_by_run_id`
    pointing at the corrected file's own new `meta.ingestion_runs.run_id`.
    """
    app = s3_client("app")
    admin = s3_client("admin")

    rng = random.SystemRandom()
    base_customer_id = rng.randint(_CUSTOMER_ID_LOW, _CUSTOMER_ID_HIGH)
    bad_customer_id = base_customer_id + 3

    original_payload = _build_customers_csv(base_customer_id=base_customer_id, bad_row_name="")
    corrected_payload = _build_customers_csv(
        base_customer_id=base_customer_id,
        bad_row_name="Corrected Name",
    )

    marker = uuid.uuid4().hex[:12]
    original_key = f"customers/e2e-backfill-{marker}-original.csv"
    corrected_key = f"customers/e2e-backfill-{marker}-corrected.csv"
    original_uri = f"s3://raw/{original_key}"
    corrected_uri = f"s3://raw/{corrected_key}"

    try:
        app.put_object(Bucket="raw", Key=original_key, Body=original_payload)

        original_file = poll_file_discovered(
            analytics_connection,
            dataset=_CUSTOMERS_DATASET,
            object_uri=original_uri,
            timeout=_DISCOVERY_TIMEOUT_SECONDS,
        )
        assert original_file["duplicate_of_file_id"] is None, (
            f"the freshly-marked original customers file was already flagged a duplicate of "
            f"file_id={original_file['duplicate_of_file_id']!r} -- the uuid marker did not "
            f"make this content genuinely new"
        )

        original_run = poll_run_for_file(
            analytics_connection,
            file_id=original_file["file_id"],
            timeout=60,
        )
        original_outcome = poll_ingestion_run(
            analytics_connection,
            original_run["idempotency_key"],
            timeout=_INGEST_TIMEOUT_SECONDS,
        )
        assert original_outcome["status"] == "SUCCEEDED", (
            f"original bad-row run finished {original_outcome['status']!r}, not SUCCEEDED -- "
            f"a single QUALITY_COMPLETENESS/REJECT_RECORD violation out of 4 rows (25%) must "
            f"stay under customers.yaml's own rejection_rate_threshold (0.5) and SUCCEED"
        )

        pending_reject = _fetch_pending_completeness_reject(
            analytics_owner_connection,
            run_id=original_run["run_id"],
        )
        assert pending_reject["resolution_type"] == "PENDING", (
            f"expected the freshly-rejected row's resolution_type to be 'PENDING' before any "
            f"backfill, got {pending_reject['resolution_type']!r}"
        )
        assert pending_reject["resolved_by_run_id"] is None, (
            f"expected the freshly-rejected row's resolved_by_run_id to be NULL before any "
            f"backfill, got {pending_reject['resolved_by_run_id']!r}"
        )

        dag_id, dag_run_id = _fetch_dagrun_identity(
            analytics_owner_connection,
            run_id=original_run["run_id"],
        )
        assert dag_id == _CUSTOMERS_DAG_ID, (
            f"expected the original run to have been claimed under dag_id={_CUSTOMERS_DAG_ID!r}, "
            f"got {dag_id!r}"
        )

        with airflow_metadata_connection.cursor() as cur:
            cur.execute(
                "SELECT logical_date, clear_number FROM dag_run WHERE dag_id = %s AND run_id = %s",
                (dag_id, dag_run_id),
            )
            dagrun_row = cur.fetchone()
        assert dagrun_row is not None, (
            f"no dag_run row found for dag_id={dag_id!r} run_id={dag_run_id!r} -- cannot "
            f"determine the logical_date to target with a real backfill"
        )
        logical_date, pre_backfill_clear_number = dagrun_row
        assert logical_date is not None, (
            f"dag_run for dag_id={dag_id!r} run_id={dag_run_id!r} has a NULL logical_date -- "
            f"cannot target it with 'airflow backfill create --from-date/--to-date'"
        )

        app.put_object(Bucket="raw", Key=corrected_key, Body=corrected_payload)

        _run_backfill_and_wait_for_reexecution(
            kubectl,
            airflow_metadata_connection,
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            logical_date=logical_date,
            logical_date_iso=logical_date.isoformat(),
            pre_backfill_clear_number=pre_backfill_clear_number,
        )

        corrected_file = poll_file_discovered(
            analytics_connection,
            dataset=_CUSTOMERS_DATASET,
            object_uri=corrected_uri,
            timeout=_DISCOVERY_TIMEOUT_SECONDS,
        )
        assert corrected_file["duplicate_of_file_id"] is None, (
            f"expected the corrected file {corrected_uri!r} to discover as a genuinely new "
            f"file (duplicate_of_file_id=None), got duplicate_of_file_id="
            f"{corrected_file['duplicate_of_file_id']!r} -- content_sha256-based dedup "
            f"treated it as a repeat of an existing file"
        )

        corrected_run = poll_run_for_file(
            analytics_connection,
            file_id=corrected_file["file_id"],
            timeout=60,
        )
        corrected_outcome = poll_ingestion_run(
            analytics_connection,
            corrected_run["idempotency_key"],
            timeout=_INGEST_TIMEOUT_SECONDS,
        )
        assert corrected_outcome["status"] == "SUCCEEDED", (
            f"corrected-file backfill run finished {corrected_outcome['status']!r}, not SUCCEEDED"
        )

        with analytics_owner_connection.cursor() as cur:
            cur.execute(
                "SELECT name FROM normalized.customers WHERE customer_id = %s",
                (bad_customer_id,),
            )
            published_row = cur.fetchone()
        assert published_row is not None, (
            f"customer_id={bad_customer_id!r} (the corrected row) was never published to "
            f"normalized.customers after the backfill run"
        )
        assert published_row[0] == "Corrected Name", (
            f"expected normalized.customers.name={'Corrected Name'!r} for customer_id="
            f"{bad_customer_id!r}, got {published_row[0]!r}"
        )

        _assert_row_resolved(
            analytics_owner_connection,
            rejected_record_id=pending_reject["rejected_record_id"],
            expected_resolved_by_run_id=corrected_run["run_id"],
        )
    finally:
        for key in (original_key, corrected_key):
            with contextlib.suppress(Exception):
                admin.delete_object(Bucket="raw", Key=key)
