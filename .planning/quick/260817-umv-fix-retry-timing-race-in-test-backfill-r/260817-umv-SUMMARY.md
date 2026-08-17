---
phase: quick-260817-umv-fix-retry-timing-race-in-test-backfill-reentry
plan: 01
subsystem: testing
tags: [airflow, backfill, e2e, pytest, race-condition]

requires:
  - phase: quick-260817-rvq-trim-monitoring-stack-helm-values-cpu
    provides: "Real CPU headroom, which let the live re-test get far enough to expose this race"
provides:
  - "_fetch_latest_backfill_dag_run_row returns (row_found, exception_reason, completed_at) instead of a 2-tuple"
  - "_wait_for_backfill_dag_run_row's settle loop gates on completed_at IS NOT NULL, not just row-appearance, closing the AlreadyRunningBackfill collision race"
  - "_BACKFILL_ROW_SETTLE_TIMEOUT_SECONDS raised 15.0 -> 45.0"
affects: [phase-08-verification, valid-08]

tech-stack:
  added: []
  patterns: ["When gating a retry on a DB-observed row, distinguish 'row exists' from 'the operation the row belongs to has actually finished' -- the two can diverge under contention even when writes are otherwise synchronous"]

key-files:
  created: []
  modified:
    - tests/e2e/slice/test_backfill_reentry.py

key-decisions:
  - "Bumped the settle timeout to 45s (3x the live-observed ~20s worst case) rather than a smaller margin, since this value gates correctness (a too-short timeout reintroduces the exact race) not just speed."
  - "Kept two DISTINCT AssertionError messages for the two timeout cases (no row ever found vs. row found but never completed) rather than merging them, since they point at different root causes and the message itself is the debugging aid on a live cluster."

patterns-established: []

requirements-completed: []

duration: ~25min
completed: 2026-08-17
---

# Quick Task 260817-umv: Fix Retry-Timing Race in test_backfill_reentry.py Summary

**Gates the backfill retry settle loop on `backfill.completed_at IS NOT NULL`, not just `backfill_dag_run` row-appearance, closing an `AlreadyRunningBackfill` collision found via live cluster re-testing.**

## Performance

- **Duration:** ~25 min
- **Tasks:** 1
- **Files modified:** 1

## Accomplishments

- `_fetch_latest_backfill_dag_run_row` now returns a 3-tuple `(row_found, exception_reason, completed_at)`, selecting `b.completed_at` in the same existing join.
- `_wait_for_backfill_dag_run_row`'s settle loop only returns once `row_found AND completed_at is not None` — waiting for the attempt's own backfill to genuinely finish, not merely register.
- Two distinct `AssertionError` messages on timeout: "no row ever observed" (unchanged from before) vs. a new "row observed but never completed" case, so a future live failure is immediately diagnosable as one or the other.
- `_BACKFILL_ROW_SETTLE_TIMEOUT_SECONDS` raised `15.0` → `45.0` (3x the live-observed ~20s worst-case completion latency), since the loop now waits for full completion under contention, not just a fast row-write.
- The one other call site unpacking `_fetch_latest_backfill_dag_run_row` (the final failure-diagnostics line) updated for the 3-tuple shape.

## Task Commits

1. **Task 1: Gate the settle loop on backfill.completed_at** — `441a51a` (fix)

**Worktree merge:** `6da33e9` (chore: merge quick task worktree)

## Files Created/Modified

- `tests/e2e/slice/test_backfill_reentry.py` — `_fetch_latest_backfill_dag_run_row`, `_wait_for_backfill_dag_run_row`, `_BACKFILL_ROW_SETTLE_TIMEOUT_SECONDS`, and one downstream call site

## Decisions Made

- See `key-decisions` in frontmatter.

## Deviations from Plan

None — plan executed exactly as written. `_invoke_backfill_create_once` and the outer retry-count/backoff loop structure were left untouched, per the plan's explicit scope boundary.

## Issues Encountered

The executor's own SUMMARY.md was written but never committed (per this quick task's own constraint — docs artifacts are committed by the orchestrator afterward), and the orchestrator removed the worktree without first rescuing that uncommitted file, losing it. This file recreates it from the diff (independently reviewed post-merge) and a fresh re-run of all three static verification commands on the merged `main` tree — same results as the executor originally reported (`ruff` clean, `mypy` clean, 1 test collected).

## Next Phase Readiness

- Static verification only, per this task's explicit scope — **not yet proven against the live cluster**. The orchestrator will run `pytest tests/e2e/slice/test_backfill_reentry.py -m cluster` separately to confirm the fix actually closes the race observed live earlier this session.

---
*Quick task: 260817-umv*
*Completed: 2026-08-17*
