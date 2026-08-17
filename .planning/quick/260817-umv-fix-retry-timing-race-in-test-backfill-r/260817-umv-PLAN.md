---
phase: quick-260817-umv-fix-retry-timing-race-in-test-backfill-reentry
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - tests/e2e/slice/test_backfill_reentry.py
autonomous: true
requirements:
  - ".planning/debug/resolved/backfill-does-not-redrive-rejected-row.md 'Live Re-Verification (2026-08-17T19:52Z)' finding: plan 08-15's retry logic (commits 1de6a22/cb56e15) detects backfill_dag_run.exception_reason but does not wait for the prior attempt's own backfill.completed_at before firing the next 'airflow backfill create' attempt, colliding with Airflow's own AlreadyRunningBackfill guard under live-observed 10-20s+ completion latency"

must_haves:
  truths:
    - "_fetch_latest_backfill_dag_run_row returns the owning backfill row's completed_at alongside row_found and exception_reason (3-tuple, not 2-tuple)"
    - "_wait_for_backfill_dag_run_row's settle loop does not return until the polled row is BOTH found AND its owning backfill.completed_at is non-NULL -- not merely 'row observed'"
    - "The settle timeout window is 45.0s (3x the live-observed ~20s worst case), not the prior 15.0s"
    - "The pre-existing 'no backfill_dag_run row observed within timeout' AssertionError message is preserved verbatim for the row_found=False timeout case"
    - "A new, distinct AssertionError message covers the row_found=True-but-completed_at-still-None timeout case, referencing .planning/debug/resolved/backfill-does-not-redrive-rejected-row.md"
    - "The one other call site unpacking _fetch_latest_backfill_dag_run_row's return value (the failure-diagnostics line in _run_backfill_and_wait_for_reexecution) is updated to unpack 3 values"
    - "_run_backfill_and_wait_for_reexecution's outer retry-count/backoff loop (attempt range, sleep calls, _BACKFILL_CREATE_MAX_ATTEMPTS/_BACKFILL_CREATE_RETRY_BACKOFF_SECONDS constants) and _invoke_backfill_create_once are untouched"
  artifacts:
    - path: "tests/e2e/slice/test_backfill_reentry.py"
      provides: "completion-gated settle loop closing the AlreadyRunningBackfill collision race"
      contains: "completed_at is not None"
  key_links:
    - from: "_wait_for_backfill_dag_run_row's settle loop"
      to: "_fetch_latest_backfill_dag_run_row's new completed_at column"
      via: "row_found, exception_reason, completed_at = _fetch_latest_backfill_dag_run_row(...)"
      pattern: "row_found, exception_reason, completed_at = _fetch_latest_backfill_dag_run_row"
    - from: "_run_backfill_and_wait_for_reexecution's failure-diagnostics call"
      to: "_fetch_latest_backfill_dag_run_row's 3-tuple return"
      via: "3-value unpack at the timeout-diagnostics call site"
      pattern: "_, latest_exception_reason, _ = _fetch_latest_backfill_dag_run_row"
---

<objective>
Close the retry-timing race in `tests/e2e/slice/test_backfill_reentry.py` diagnosed live in `.planning/debug/resolved/backfill-does-not-redrive-rejected-row.md`'s "Live Re-Verification (2026-08-17T19:52Z...)" section: `_wait_for_backfill_dag_run_row` currently returns as soon as a `backfill_dag_run` row is *observed*, but that attempt's own owning `backfill.completed_at` can lag ~20s behind row-appearance under live contention. Because the caller only sleeps a fixed 5s before firing the next `airflow backfill create` attempt, the next attempt can fire while the prior attempt's backfill is still active, colliding with Airflow's own one-backfill-per-DAG guard (`AlreadyRunningBackfill`) and burning the retry budget on a third, previously-unanticipated failure mode.

Purpose: make the settle loop wait for the polled attempt's own backfill to genuinely finish (not just register) before returning, so the next retry attempt (if any) never fires while a still-active backfill exists for this DAG.

Output: `_fetch_latest_backfill_dag_run_row` returns a 3-tuple including `completed_at`; `_wait_for_backfill_dag_run_row` gates its return on `completed_at is not None`; the settle timeout is raised to 45.0s; the one other call site is updated for the new return shape. No other function, call site, or file changes.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/STATE.md
@.planning/debug/resolved/backfill-does-not-redrive-rejected-row.md
@tests/e2e/slice/test_backfill_reentry.py

<current_shapes_to_modify>
`_fetch_latest_backfill_dag_run_row` (tests/e2e/slice/test_backfill_reentry.py, currently lines ~185-237): queries `bdr.exception_reason` only, via `SELECT bdr.exception_reason FROM backfill_dag_run bdr JOIN backfill b ON b.id = bdr.backfill_id WHERE b.dag_id = %s AND bdr.logical_date = %s ORDER BY b.id DESC, bdr.id DESC LIMIT 1`. Returns `tuple[bool, str | None]`: `(False, None)` if no row, `(True, row[0])` if found.

`_wait_for_backfill_dag_run_row` (currently lines ~240-296): polls `_fetch_latest_backfill_dag_run_row` in a `while time.monotonic() < settle_deadline` loop, `return exception_reason` immediately once `row_found` is True. On deadline expiry with `row_found` still False, raises an `AssertionError` citing a debug-doc path (verify the exact string literal currently in the file -- it may or may not already include the `resolved/` path segment the doc has since moved under -- and keep whatever convention the existing code uses consistent with the new message added in this plan).

`_BACKFILL_ROW_SETTLE_TIMEOUT_SECONDS = 15.0` (module-level constant, near the other `_BACKFILL_CREATE_*` constants).

The one other unpacking call site is near the end of `_run_backfill_and_wait_for_reexecution`, in its final timeout-diagnostics block: `_, latest_exception_reason = _fetch_latest_backfill_dag_run_row(airflow_conn, dag_id=dag_id, logical_date=logical_date)`.

`_run_backfill_and_wait_for_reexecution`'s call to `_wait_for_backfill_dag_run_row(airflow_conn, dag_id=dag_id, logical_date=logical_date, attempt=attempt)` (single return value assigned to `exception_reason`) is UNCHANGED in shape -- only `_wait_for_backfill_dag_run_row`'s internal wait condition changes.
</current_shapes_to_modify>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Gate the settle loop on the owning backfill's own completion, not just row-appearance</name>
  <files>tests/e2e/slice/test_backfill_reentry.py</files>
  <action>
    Make seven changes, all within this one file, all inside the two existing helper functions plus their one caller's diagnostics line -- do not touch `_invoke_backfill_create_once`, the test function body, or `_run_backfill_and_wait_for_reexecution`'s outer `for attempt in range(1, _BACKFILL_CREATE_MAX_ATTEMPTS + 1):` retry/backoff structure.

    1. In `_fetch_latest_backfill_dag_run_row`: extend the SQL `SELECT` to also select `b.completed_at` (same `backfill_dag_run bdr JOIN backfill b ON b.id = bdr.backfill_id` join already in place -- just add the column). Change the function's return type annotation from `tuple[bool, str | None]` to `tuple[bool, str | None, datetime.datetime | None]`. Update both return statements: the "no row" branch returns `(False, None, None)`; the "row found" branch returns `(True, row[0], row[1])` (or equivalent unpacking of both selected columns). Update the docstring's "Returns:" section to document the third element (`completed_at` -- the owning `backfill.completed_at`, `None` until that backfill finishes, non-`None` once it has).

    2. In `_wait_for_backfill_dag_run_row`: change the settle loop so each poll iteration unpacks all three values (`row_found, exception_reason, completed_at = _fetch_latest_backfill_dag_run_row(...)`), and change the loop's return condition from `if row_found: return exception_reason` to only return once `row_found and completed_at is not None`. Track `completed_at` across iterations the same way `row_found`/`exception_reason` are already tracked (initialize before the loop, e.g. `completed_at: datetime.datetime | None = None`).

    3. Preserve the existing timeout `AssertionError` for the case where the deadline expires with `row_found` still `False` -- keep its message text exactly as it is today (do not reword it), still citing whatever debug-doc path string the existing code already uses.

    4. Add a NEW, separate `AssertionError` branch for the case where the deadline expires with `row_found=True` but `completed_at` is still `None` (the row registered, but this attempt's own owning backfill never finished within the settle window). Word it in the same style/convention as the existing message (f-string, includes `dag_id`, `logical_date`, `attempt`/`_BACKFILL_CREATE_MAX_ATTEMPTS`, the settle timeout value, and the last-observed `exception_reason`), and explicitly reference `.planning/debug/resolved/backfill-does-not-redrive-rejected-row.md` (the "Live Re-Verification" section is the source of this exact race) as the origin of this failure mode. Make clear in the message that this is DIFFERENT from the "no row observed" case -- the row exists, but the backfill it belongs to never completed in time.

    5. Update the function's docstring "Raises:" section to document both failure modes distinctly (the pre-existing "no row observed" case, and the new "row observed but never completed" case).

    6. Bump `_BACKFILL_ROW_SETTLE_TIMEOUT_SECONDS` from `15.0` to `45.0`. Update its inline/shared comment to note this covers the live-observed ~20s worst-case completion latency (`.planning/debug/resolved/backfill-does-not-redrive-rejected-row.md`, "Live Re-Verification" section) with headroom, now that the loop waits for full completion rather than mere row-appearance.

    7. Update the ONE other call site (the failure-diagnostics line near the end of `_run_backfill_and_wait_for_reexecution`, currently `_, latest_exception_reason = _fetch_latest_backfill_dag_run_row(...)`) to unpack 3 values instead of 2, e.g. `_, latest_exception_reason, _ = _fetch_latest_backfill_dag_run_row(...)`.

    Do not modify `_run_backfill_and_wait_for_reexecution`'s call to `_wait_for_backfill_dag_run_row(...)` itself (same arguments, same single-value assignment to `exception_reason`) -- only the internals of the two functions listed above change shape. If a brief clarifying comment at that call site is useful (e.g. noting the wait now also covers full completion, not just row-appearance), add one, but do not restructure the call.
  </action>
  <verify>
    <automated>cd /home/konutec/projects/airflow-platform && .venv/bin/ruff check tests/e2e/slice/test_backfill_reentry.py && .venv/bin/mypy tests/e2e/slice/test_backfill_reentry.py && .venv/bin/pytest tests/e2e/slice/test_backfill_reentry.py --collect-only -q</automated>
  </verify>
  <done>`_fetch_latest_backfill_dag_run_row` returns `(row_found, exception_reason, completed_at)`; `_wait_for_backfill_dag_run_row` only returns once `row_found and completed_at is not None`, with two distinct, correctly-worded `AssertionError` branches on timeout; `_BACKFILL_ROW_SETTLE_TIMEOUT_SECONDS == 45.0`; the one other call site unpacks 3 values; `ruff check`, `mypy`, and `pytest --collect-only` all pass cleanly with zero errors/warnings introduced.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|--------------|
| Test helper -> live Airflow metadata DB | `_fetch_latest_backfill_dag_run_row`/`_wait_for_backfill_dag_run_row` read `backfill`/`backfill_dag_run` rows via a live `psycopg` connection (`airflow_metadata_connection` fixture) against the real cluster's Airflow metadata database; this plan only widens what is read (one added column) and how long polling waits, no new write path or trust boundary is introduced. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|------------------|
| T-quick-01 | Denial of Service (test-suite self-inflicted) | `_wait_for_backfill_dag_run_row`'s settle loop | accept | Raising the settle timeout to 45.0s increases worst-case wall-clock time for a single retry attempt inside this already-cluster-gated (`pytest.mark.cluster`), non-CI-blocking E2E test. Accepted: this is strictly a test-timing budget, not production code, and closes a real correctness gap (colliding with `AlreadyRunningBackfill`) that was actively causing false test failures. |
| T-quick-02 | Tampering (accidental logic regression) | `_fetch_latest_backfill_dag_run_row`'s new 3-tuple return shape | mitigate | Static verification (`ruff`, `mypy --strict`-equivalent project config, `pytest --collect-only`) catches unpacking-arity mismatches at the one other call site before any live run is attempted; this plan's own scope explicitly excludes running the live-cluster test, deferring that proof to the orchestrator's manual follow-up per the task instructions. |
</threat_model>

<verification>
1. `ruff check tests/e2e/slice/test_backfill_reentry.py` reports zero issues.
2. `mypy tests/e2e/slice/test_backfill_reentry.py` reports zero errors (the 3-tuple return type and both unpacking call sites are type-consistent).
3. `pytest tests/e2e/slice/test_backfill_reentry.py --collect-only -q` collects the module with zero collection errors (proves the file is syntactically valid and importable, without touching the live cluster).
4. `grep -c "completed_at" tests/e2e/slice/test_backfill_reentry.py` reports a nonzero count reflecting the new column/parameter threaded through both functions.
</verification>

<success_criteria>
- `_fetch_latest_backfill_dag_run_row` returns `tuple[bool, str | None, datetime.datetime | None]`, selecting `b.completed_at` alongside the existing `bdr.exception_reason`.
- `_wait_for_backfill_dag_run_row` only returns once `row_found` is `True` AND `completed_at is not None`, with the pre-existing "no row observed" `AssertionError` preserved verbatim and a new, distinct "row observed but backfill never completed" `AssertionError` added, both documented in the docstring's "Raises:" section.
- `_BACKFILL_ROW_SETTLE_TIMEOUT_SECONDS` is `45.0`.
- The one other unpacking call site in `_run_backfill_and_wait_for_reexecution` correctly unpacks the new 3-tuple.
- `_invoke_backfill_create_once`, the outer retry/backoff loop structure in `_run_backfill_and_wait_for_reexecution`, and `test_backfill_resolves_previously_rejected_row` itself are byte-for-byte unchanged.
- `ruff check`, `mypy`, and `pytest --collect-only` all pass cleanly on the modified file. No live cluster contact occurs as part of this plan's own verification.
</success_criteria>

<output>
Create `.planning/quick/260817-umv-fix-retry-timing-race-in-test-backfill-r/260817-umv-SUMMARY.md` when done.
</output>
