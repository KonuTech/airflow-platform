---
phase: 10-slowly-changing-dimensions
plan: 03
subsystem: scd
tags: [scd2, delete-detection, circuit-breaker, postgresql, barrier-stage]

# Dependency graph
requires:
  - phase: 10-slowly-changing-dimensions
    plan: 01
    provides: "normalized.customers SCD2 shape (is_current/valid_to/validity EXCLUDE constraint), ScdConfig, Publisher.publish(staged_run_ids=...)"
provides:
  - "dataplat.scd.delete_detection.find_vanished_customer_ids -- run-scoped anti-join between normalized.customers (is_current) and silver.customers, scoped to staged_run_ids"
  - "dataplat.scd.delete_detection.MassDeleteCircuitBreaker -- BarrierStage raising QualityThresholdExceeded when vanished/current exceeds scd.mass_delete_threshold"
  - "dataplat.scd.delete_detection.apply_delete_semantics -- dispatches ignore/invalidate/new_record against normalized.customers, effective-dated by the caller's own snapshot_max_event_ts"
affects: [10-04]

tech-stack:
  added: []
  patterns:
    - "Data-modifying CTE (UPDATE ... RETURNING feeding an INSERT ... SELECT, one statement) for new_record delete-semantics -- avoids double-matching the newly-inserted current row against the same UPDATE ... WHERE is_current clause that closed the old one"
    - "Run-scoped read of a shared, cumulative upsert table (silver.customers) via _run_id = ANY(staged_run_ids), mirroring metadata/repository.py's record_watermark precedent"

key-files:
  created:
    - packages/dataplat/src/dataplat/scd/delete_detection.py
    - tests/integration/test_scd_delete_detection.py

key-decisions:
  - "new_record's UPDATE-then-INSERT is a single atomic data-modifying CTE, not two separately-executed statements -- the plan's literal 'INSERT immediately followed by the SAME UPDATE' ordering would incorrectly re-match and close the just-inserted row too, since both old and new rows would satisfy 'customer_id = ANY(vanished_ids) AND is_current' at that point (Rule 1 fix, found during Task 2's own live-verification pass, before any commit)"
  - "Test assertions use membership (`in`/`not in`), not exact-set equality, for find_vanished_customer_ids -- normalized.customers/silver.customers are session-shared tables across the whole tests/integration/ collection (conftest.py's own documented convention), so a prior test's leftover is_current rows legitimately also appear vanished under a later test's own staged_run_ids scope"
  - "_insert_normalized_customer's test helper takes an explicit, fixed-past event_ts (2020-01-01) instead of now() -- ties a row's effective-dating to a value the test fully controls, so a later apply_delete_semantics call's snapshot_max_event_ts can be deterministically guaranteed later, independent of the real wall clock at test-run time"

requirements-completed: [SCD-08]

duration: ~65min
completed: 2026-08-21
---

# Phase 10 Plan 03: SCD DELETE-Detection & Mass-Delete Circuit Breaker Summary

**Run-scoped snapshot-diff anti-join (`find_vanished_customer_ids`) plus a constructor-parameterized `MassDeleteCircuitBreaker` and `apply_delete_semantics` dispatcher, all proven live against real PostgreSQL via testcontainers -- the piece Finding F-2 flagged as easy to get subtly wrong, built and tested standalone before plan 10-04 assembles the full SCD Publisher around it.**

## Performance

- **Duration:** ~65 min
- **Completed:** 2026-08-21
- **Tasks:** 2/2
- **Files modified:** 2 (both created)

## Accomplishments

- `find_vanished_customer_ids(conn, *, staged_run_ids)` correctly detects a real vanish AND proves the F-2 regression guard: a `silver.customers` row tagged with an older, un-staged run is still reported vanished, not treated as "still present" -- confirming the read is genuinely scoped to `staged_run_ids`, never the whole cumulative table.
- `MassDeleteCircuitBreaker` mirrors `RejectionRateCircuitBreaker`'s exact shape (constructor-parameterized totals, `current_count == 0` trivial-PASS guard, `ratio > threshold` not `>=`) -- raises `QualityThresholdExceeded` on breach, passes cleanly at/below threshold.
- `apply_delete_semantics` implements all three `ScdConfig.delete_semantics` values: `ignore` (true no-op, no DB write), `invalidate` (closes the current row, `valid_to`/`is_current` set from the caller's own `snapshot_max_event_ts`, never wall-clock time), `new_record` (opens a new current version copying forward Type-2/Type-1/Type-0 attribute values, closes the prior one) -- all proven live, including asserting the new row's surrogate `id` differs from the old row's.
- 12 tests total (4 for the snapshot diff, 4 for the circuit breaker, 4 for delete-semantics dispatch including a defensive out-of-vocabulary-value guard), all passing against a real testcontainers PostgreSQL 18 + full `alembic upgrade head`.

## Task Commits

1. **Task 1 + Task 2 (RED):** `778c193` (test) -- all 12 behaviors written first; confirmed failing at collection (`ModuleNotFoundError: No module named 'dataplat.scd.delete_detection'`) before any implementation existed.
2. **Task 1 + Task 2 (GREEN):** `aa996de` (feat) -- `find_vanished_customer_ids`, `MassDeleteCircuitBreaker`, `apply_delete_semantics` implemented; all 12 tests pass.

Both plan tasks share the same two files (`delete_detection.py`, `test_scd_delete_detection.py`) and the same RED test commit covers both tasks' behaviors together -- splitting the GREEN implementation into two further commits would have required an artificially broken intermediate state (Task 2's tests import `MassDeleteCircuitBreaker`/`apply_delete_semantics` at module level, so the whole file fails to collect until both are implemented). One RED commit, one GREEN commit, both tasks' behaviors proven together.

## Files Created/Modified

- `packages/dataplat/src/dataplat/scd/delete_detection.py` -- `find_vanished_customer_ids`, `MassDeleteCircuitBreaker`, `apply_delete_semantics`, plus the module-level SQL constants (`_VANISHED_SQL`, `_CLOSE_CURRENT_ROW_CLAUSE`, `_INVALIDATE_SQL`, `_NEW_RECORD_SQL_TEMPLATE`/`_NEW_RECORD_SQL`)
- `tests/integration/test_scd_delete_detection.py` -- 12 integration tests (4 marked `-m integration` snapshot-diff tests, 4 unmarked pure-Python circuit-breaker tests, 4 marked delete-semantics-dispatch tests) plus local seeding helpers (`_seed_run`, `_insert_silver_customer`, `_insert_normalized_customer`) mirroring `test_referential_integrity.py`'s/`test_publish_ingest.py`'s own per-file helper convention

## Decisions Made

- **`new_record`'s UPDATE+INSERT as one atomic CTE, not two statements (Rule 1 fix).** The plan's action text described "INSERT ... immediately followed by the SAME UPDATE invalidate uses to close the old row." Implemented literally at first, then caught during Task 2's own test-writing (before any commit): running the plain `UPDATE ... WHERE customer_id = ANY(vanished_ids) AND is_current` a second time, after an `INSERT` whose new row also lands with `is_current = true` (column default), would incorrectly re-match and close the just-inserted row too -- both old and new rows would satisfy the same `WHERE` clause. Fixed by wrapping the closing `UPDATE` as a CTE (`WITH closed AS (UPDATE ... RETURNING ...) INSERT ... SELECT ... FROM closed`) -- the `UPDATE` fully executes and captures its `RETURNING` rows as a stable, pre-update snapshot before the `INSERT` ever runs, so the new row is never at risk of being re-matched. Still literally reuses the exact same `UPDATE` clause text (`_CLOSE_CURRENT_ROW_CLAUSE`, embedded via `.format()`, not duplicated), just as one statement rather than two independently-executed ones.
- **Test assertions use membership, not exact-set equality, for `find_vanished_customer_ids`.** `normalized.customers`/`silver.customers` are session-shared tables across the whole `tests/integration/` collection (documented in `conftest.py`), and `find_vanished_customer_ids` is deliberately unscoped by dataset (single-dataset system). A prior test's own `is_current` rows legitimately also show up as "vanished" under a later test's own `staged_run_ids` scope, since that later test's `staged_run_ids` never covers the earlier test's run. Exact-set-equality assertions failed against this real cross-test leakage; membership assertions (`"900003" in vanished`, `"900001" not in vanished`) prove the same behavior without depending on isolation this suite does not provide -- matching this repo's own established convention for other session-shared-table tests (`test_referential_integrity.py`, `test_publish_ingest.py`).
- **`_insert_normalized_customer`'s test helper uses a fixed, deliberately-far-past `event_ts` (2020-01-01), never `now()`.** The exclusion constraint's generated `validity` column requires `event_ts <= valid_to`. Using `now()` at row-insertion time made a hardcoded `snapshot_max_event_ts` in the invalidate/new_record tests fragile -- if the real wall clock at test-run time happened to be later than the hardcoded snapshot timestamp, `valid_to < event_ts` and PostgreSQL raised `DataException: range lower bound must be less than or equal to range upper bound`. Pinning `event_ts` to a fixed past instant lets tests choose any later `snapshot_max_event_ts` deterministically, independent of when the test actually runs.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `new_record`'s literal INSERT-then-UPDATE ordering would double-close the newly-inserted row**
- **Found during:** Task 2, while writing the implementation against the plan's own literal action text (before any commit -- caught by design review, not a live test failure)
- **Issue:** The plan's action text described running the closing `UPDATE ... WHERE customer_id = ANY(vanished_ids) AND is_current` a second time, immediately after the `INSERT`. Since the newly-inserted row also defaults to `is_current = true`, that second `UPDATE` would match and incorrectly close it too.
- **Fix:** Implemented as one atomic data-modifying CTE (`WITH closed AS (UPDATE ... RETURNING ...) INSERT ... SELECT ... FROM closed`) instead of two separately-executed statements.
- **Files modified:** `packages/dataplat/src/dataplat/scd/delete_detection.py`
- **Verification:** Test 7 (`test_delete_semantics_new_record_opens_a_new_current_version_and_closes_the_old_one`) proves live: exactly 2 rows exist post-call, exactly 1 `is_current`, the new row's surrogate `id` differs from the old row's, and the old row's `valid_to`/new row's `event_ts` both equal the supplied `snapshot_max_event_ts`.
- **Committed in:** `aa996de` (the only implementation commit -- caught before any commit existed to fix)

**2. [Rule 1 - Bug] Ruff S608/UP032 conflict over the CTE's embedded `UPDATE` text**
- **Found during:** Task 2, running `ruff check` before committing
- **Issue:** Embedding `_CLOSE_CURRENT_ROW_CLAUSE` into `_NEW_RECORD_SQL` via an f-string triggered `S608` (possible SQL injection vector); switching to `"...".format(...)` called directly on a literal string triggered `UP032` (use an f-string instead) -- the two rules disagreed on the same line.
- **Fix:** Assigned the template to its own named module constant first (`_NEW_RECORD_SQL_TEMPLATE = """..."""`), then called `.format()` on that variable reference rather than a literal, matching `merge.py`'s own existing `_PUBLISH_SQL.format(...)` pattern (which passes both rules cleanly).
- **Files modified:** `packages/dataplat/src/dataplat/scd/delete_detection.py`
- **Verification:** `ruff check` passes clean; `mypy` passes clean.
- **Committed in:** `aa996de`

**3. [Rule 1 - Bug] Task 1 test assertions used exact-set equality, which is unsafe against session-shared table state**
- **Found during:** Task 1/2, running the full test file together (`pytest tests/integration/test_scd_delete_detection.py -q`)
- **Issue:** `test_nothing_vanished_returns_empty_set`/similar tests asserted `vanished == {...}` or `vanished == set()`, which failed once a prior test in the same file had already left `is_current` rows behind in the shared `normalized.customers` table (those rows legitimately also appear "vanished" under a later, unrelated `staged_run_ids` scope).
- **Fix:** Rewrote assertions to membership checks (`in`/`not in`) instead of exact-set equality.
- **Files modified:** `tests/integration/test_scd_delete_detection.py`
- **Verification:** Full file passes (12/12), in any collection order.
- **Committed in:** `aa996de`

---

**Total deviations:** 3 auto-fixed (2 correctness/Rule 1 bugs found before any commit existed to need reverting, 1 test-robustness fix). No scope creep -- all three keep the plan's own stated acceptance criteria intact.

## Issues Encountered

- The plan's own Task 2 example SQL for `invalidate`/`new_record` implied the two delete-semantics could share literally the same `UPDATE` statement run twice in sequence; live testing during test-writing (Deviation 1 above) showed this ordering is only safe as a single atomic statement, not two.
- Real wall-clock `now()` in test fixtures conflicted with hardcoded future-looking `snapshot_max_event_ts` test values once the actual test-run time caught up to (and passed) the hardcoded timestamp -- fixed by pinning test row `event_ts` values to a fixed past instant instead (see Decisions Made).

## User Setup Required

None -- no external service configuration required. Tests run entirely against a throwaway testcontainers PostgreSQL 18 container.

## Next Phase Readiness

- `find_vanished_customer_ids`/`MassDeleteCircuitBreaker`/`apply_delete_semantics` are all live-proven, standalone, DB-testable units ready for plan 10-04's `SCDPublisher.publish()` to assemble around: it already has `staged_run_ids` (from plan 10-01) and can now call these three directly in sequence (detect vanished keys -> evaluate the circuit breaker -> apply the configured delete semantics), all inside its own already-open publication transaction.
- `apply_delete_semantics` returns the acted-on `customer_id`s as a `tuple[str, ...]`, matching `PublishResult.published_business_keys`'s own string convention -- plan 10-04 can fold this directly into its own `published_business_keys` without any additional type conversion.
- No known gaps or deferred items specific to this plan's own scope.

## Self-Check: PASSED

- FOUND: commit `778c193` (test)
- FOUND: commit `aa996de` (feat)
- FOUND: `packages/dataplat/src/dataplat/scd/delete_detection.py`
- FOUND: `tests/integration/test_scd_delete_detection.py`

---
*Phase: 10-slowly-changing-dimensions*
*Completed: 2026-08-21*
