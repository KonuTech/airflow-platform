---
phase: 09-etl-correctness-dedup-incremental-backfill-recovery
plan: 06
subsystem: database
tags: [postgresql, alembic, sql-view, recovery, metadata-control-plane]

# Dependency graph
requires:
  - phase: 09-02
    provides: meta.run_stages table (STAGE_LOAD/DBT_BUILD/PUBLISH lifecycle), claim_run_stage/complete_run_stage/get_run_stage_status repository methods
provides:
  - meta.v_run_recovery SQL view spanning meta.ingestion_runs + meta.run_stages (all 3 stage names)
  - get_run_recovery_status(*, run_id) read helper on MetadataRepository Protocol and PostgresMetadataRepository
affects: [09-09, observability, grafana-dashboards]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "drop + recreate whole view" migration pattern (0012/0026/0030) applied to a first-creation view too, for future-migration consistency
    - dict-keyed read helper built from cursor.description, mirroring get_run_stage_status's own read-only contract

key-files:
  created:
    - migrations/versions/0033_meta_v_run_recovery.py
    - tests/integration/test_run_recovery_view.py
  modified:
    - packages/dataplat/src/dataplat/metadata/repository.py
    - packages/dataplat/src/dataplat/metadata/postgres.py

key-decisions:
  - "next_action CASE branches are ordered SUCCEEDED-complete first, then STAGE_LOAD, then DBT_BUILD, then PUBLISH -- each branch treats FAILED and PENDING/missing identically as 'not yet done', collapsing 4 raw stage-status values into 3 possible verdicts per stage"
  - "Test seeding uses raw SQL INSERT into meta.run_stages rather than claim_run_stage, since claim_run_stage is gated on the owning run's own status='STAGED' -- direct seeding gives independent control over run_status x stage_status combinations, matching the plan's own guidance"

requirements-completed: [LOAD-06]

duration: 25min
completed: 2026-08-19
---

# Phase 9 Plan 06: meta.v_run_recovery Summary

**One SQL view (`meta.v_run_recovery`) answers "what succeeded, what remains, what's next" across STAGE_LOAD/DBT_BUILD/PUBLISH, with `next_action` always reading `'retry stage X'` or `'complete'`, never implying rollback.**

## Performance

- **Duration:** 25 min
- **Started:** 2026-08-19T14:46:00Z
- **Completed:** 2026-08-19T15:10:59Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments
- `meta.v_run_recovery` view created (migration 0033), joining `meta.ingestion_runs` with 3 aliased `LEFT JOIN`s over `meta.run_stages` (one per `stage_name`), closing 08.1's documented blind spot that recovery visibility previously stopped at 2 of 3 pipeline stages
- `next_action` column deterministically resolves to `'complete'`, `'retry stage STAGE_LOAD'`, `'retry stage DBT_BUILD'`, `'retry stage PUBLISH'`, or `'in progress'` — proven via 6 integration tests to never contain the substring "rollback" (D-15)
- `get_run_recovery_status(*, run_id)` read helper added to the `MetadataRepository` Protocol and `PostgresMetadataRepository`, giving callers a single typed entry point instead of hand-writing the view's `SELECT`

## Task Commits

Each task was committed atomically (Task 2 followed TDD: test → feat):

1. **Task 1: meta.v_run_recovery migration** - `7b662e7` (feat)
2. **Task 2 (RED): failing test for get_run_recovery_status** - `52875b3` (test)
3. **Task 2 (GREEN): get_run_recovery_status implementation** - `7fa32f0` (feat)

**Plan metadata:** (this commit) — SUMMARY.md

## Files Created/Modified
- `migrations/versions/0033_meta_v_run_recovery.py` - Creates `meta.v_run_recovery`, `GRANT SELECT` to `etl_app`/`grafana_reader`, zero grant to `dbt_app`
- `tests/integration/test_run_recovery_view.py` - 6 tests: complete, retry-DBT_BUILD (missing row), retry-STAGE_LOAD (no rows), retry-PUBLISH (failed), rollback-never-appears (4 scenarios), nonexistent run_id returns None
- `packages/dataplat/src/dataplat/metadata/repository.py` - `get_run_recovery_status` Protocol method
- `packages/dataplat/src/dataplat/metadata/postgres.py` - Implementation: `SELECT * FROM meta.v_run_recovery WHERE run_id = %s`, dict built from `cursor.description`

## Decisions Made
- The view's `CASE` treats a stage's `FAILED` and `PENDING`/missing-row status identically (both mean "not yet successfully done, needs retry") — matches the plan's exact SQL and D-15's retry-only framing; there is no separate "explain why it failed" branch here, that's `meta.run_stages.status`/`meta.ingestion_runs.error_message` territory, not this view's job.
- Test seeding bypasses `claim_run_stage` (which requires the owning run to already be `'STAGED'`) in favor of direct `INSERT INTO meta.run_stages`, exactly as the plan's acceptance criteria specify ("no live DAG needed").

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] mypy union-attr error on `cursor.description`**
- **Found during:** Task 2 verification (`uv run mypy packages/dataplat/src/dataplat/metadata/`)
- **Issue:** `psycopg`'s `cursor.description` is typed `list[Column] | None`; iterating it directly without a narrowing check fails strict mypy.
- **Fix:** Added `assert cursor.description is not None  # noqa: S101` immediately after confirming `row is not None` (a fetched row structurally guarantees a non-`None` description) — matches the exact `# noqa: S101` convention already used in `packages/dataplat/src/dataplat/validate/volume_anomaly.py`.
- **Files modified:** `packages/dataplat/src/dataplat/metadata/postgres.py`
- **Verification:** `uv run mypy packages/dataplat/src/dataplat/metadata/` now reports `Success: no issues found in 3 source files`.
- **Committed in:** `7fa32f0` (Task 2 GREEN commit)

**2. [Rule 1 - Bug] ruff lint failures in the new test file**
- **Found during:** Post-Task-2 lint pass (`uv run ruff check`, run proactively per CLAUDE.md's lint/type conventions even though the plan's own `<verification>` only names pytest+mypy)
- **Issue:** Unused `datetime`/`UTC` import (leftover from an earlier draft using timestamps), 4 lines over the 100-char limit (one long docstring, 3 lines in a scenario-table test).
- **Fix:** Removed the unused import; reformatted the long docstring with the codebase's established `# noqa: E501, W505` convention (matching `test_stage_ingest.py`/`test_publish_transaction_wiring.py` precedent); reflowed the scenario list and a call site across multiple lines instead of one long line.
- **Files modified:** `tests/integration/test_run_recovery_view.py`
- **Verification:** `uv run ruff check` on all 4 touched files reports `All checks passed!`; `pytest tests/integration/test_run_recovery_view.py -q` still 6/6 passing after the reformat.
- **Committed in:** `7fa32f0` (Task 2 GREEN commit, bundled with the implementation since both landed in the same verification pass)

---

**Total deviations:** 2 auto-fixed (2 Rule 1 bugs, both caught by this plan's own stated verification tools)
**Impact on plan:** Both fixes are pure correctness/tooling-compliance — no scope creep, no behavior change beyond making the stated `<verification>` command (`mypy`) and the project's standard lint gate pass cleanly.

## Issues Encountered
None beyond the two auto-fixed deviations above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- `meta.v_run_recovery` and `get_run_recovery_status` are ready for plan 09-09's DAG-side recovery wiring and for a future Grafana panel (the view already grants `SELECT` to `grafana_reader`).
- No blockers. The view is proven correct in isolation from any live DAG or dbt invocation, so 09-09 can wire real `STAGE_LOAD`/`DBT_BUILD`/`PUBLISH` transitions against it with confidence the read side already works.

---
*Phase: 09-etl-correctness-dedup-incremental-backfill-recovery*
*Completed: 2026-08-19*
