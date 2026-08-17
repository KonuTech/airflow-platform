---
phase: 08-validation-quarantine-metadata-control-plane-completion
plan: 03
subsystem: database
tags: [postgresql, psycopg, metadata-repository, validation, quarantine, jsonb, transactions]

# Dependency graph
requires:
  - phase: 08-01
    provides: "meta.validation_results/meta.rejected_records DDL (migrations 0014/0015), widened ValidationResult, MetadataRepository Protocol stubs for record_validation_results/record_rejected_records/resolve_rejected_records_for_batch"
provides:
  - "PostgresMetadataRepository.record_validation_results — bulk-inserts ValidationResult rows inside the caller's own open transaction, threshold/observed JSONB round-trip via psycopg.types.json.Jsonb"
  - "PostgresMetadataRepository.record_rejected_records — bulk-inserts RejectedRecord rows inside the caller's own open transaction, resolution_type left at its PENDING column default"
  - "PostgresMetadataRepository.resolve_rejected_records_for_batch — the ONLY write path to resolution_type/resolved_by_run_id anywhere in the codebase, whole-batch-only (WHERE batch_id = %s AND resolution_type = 'PENDING'), returns cursor.rowcount, idempotent (0 on replay)"
  - "tests/integration/test_validation_persistence.py — proves commit-path visibility and rollback-path atomicity for both writers, plus the empty-list no-op case"
  - "tests/integration/test_backfill_resolution.py — proves cross-batch isolation, idempotent re-resolution, and single-write-path enforcement (source-scan assertion)"
affects: [08-08-referential-integrity-barrier, 08-09-volume-anomaly-detection, 08-11-publish-transaction-wiring, pipeline/run.py]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "conn-scoped MetadataRepository methods (never opens self._pool, never commits/rolls back) — the finalize_publication exception, now extended to all three validation/quarantine writers so they land inside the caller's own atomic publish transaction (Pattern 3/D-11)"
    - "Whole-batch-only mutation proven via source-level regex scan (resolution_type = %s pattern) rather than a runtime assertion — a permanent, cheap structural guard against a future per-row resolution method being added"

key-files:
  created:
    - tests/integration/test_validation_persistence.py
    - tests/integration/test_backfill_resolution.py
  modified:
    - packages/dataplat/src/dataplat/metadata/postgres.py

key-decisions:
  - "Worktree branch was stale (branched before wave 1's 08-01/08-02 merged to main) — fast-forward merged main into the worktree branch before starting, since depends_on: [\"08-01\"] made the Protocol stubs, migrations and ValidationResult/RejectedRecord models a hard prerequisite for this plan's own work"
  - "resolve_rejected_records_for_batch's single-write-path proof uses a regex match on the literal SQL mutation pattern 'resolution_type = %s' per method body, not a bare substring count of 'resolution_type' — the latter would false-positive against record_rejected_records's own docstring prose explaining it deliberately never sets that column"

requirements-completed: [VALID-04, VALID-08]

# Metrics
duration: 8min
completed: 2026-08-17
---

# Phase 8 Plan 03: MetadataRepository Validation/Quarantine Persistence Summary

**Implemented `PostgresMetadataRepository`'s three validation/quarantine write methods (bulk validation-finding insert, bulk reject insert, whole-batch-only resolution) and proved their transactional and single-write-path contracts directly against real PostgreSQL — no Airflow, no pipeline, no barrier stage involved.**

## Performance

- **Duration:** ~8 min (commit-to-commit; task work itself, excluding file reads)
- **Started:** 2026-08-17T10:00:19+02:00 (worktree merge point)
- **Completed:** 2026-08-17T10:07:35+02:00
- **Tasks:** 2/2 completed
- **Files modified:** 3 (1 modified, 2 created)

## Accomplishments

- `record_validation_results`/`record_rejected_records`/`resolve_rejected_records_for_batch` implemented against real PostgreSQL, all three `conn`-scoped (never open their own connection, never commit/rollback), matching `finalize_publication`'s documented exception
- Proved live, on a real testcontainers PostgreSQL 18: rows written by either writer inside an open transaction are visible on a fresh connection after commit, and vanish together on rollback (Pitfall 2's exact distinguishing case)
- Proved live: `resolve_rejected_records_for_batch` resolves only its own batch's `PENDING` rows (a sibling batch is provably untouched), is idempotent on replay (second identical call returns `0`, never raises), and is structurally the ONLY method on `PostgresMetadataRepository` whose source contains the `resolution_type = %s` mutation pattern — D-03/D-04's whole-batch-only, single-write-path constraints made concrete and permanently guarded

## Task Commits

Each task was committed atomically:

1. **Task 1: PostgresMetadataRepository — implement the three new methods** - `c1fe2a5` (feat)
2. **Task 2: Integration tests — transactional persistence + whole-batch-only resolution** - `b2a253b` (test)

**Plan metadata:** (this commit, docs: complete plan)

## Files Created/Modified

- `packages/dataplat/src/dataplat/metadata/postgres.py` - Implemented `record_validation_results`, `record_rejected_records`, `resolve_rejected_records_for_batch`
- `tests/integration/test_validation_persistence.py` - Commit-path, rollback-path, and empty-list no-op proofs for the two bulk writers
- `tests/integration/test_backfill_resolution.py` - Cross-batch isolation, idempotent replay, and single-write-path source-scan proof for the resolver

## Decisions Made

- **Worktree branch was stale relative to its declared dependency.** This plan's `depends_on: ["08-01"]`, but the worktree branch had been created from the commit immediately before wave 1 (08-01, 08-02) merged into `main` — `repository.py`'s Protocol stubs, the `ValidationResult`/`RejectedRecord` models' final shape, and migrations 0014/0015 were all absent at session start (`grep` for the three method names in `repository.py` returned zero matches, contradicting the plan's stated assumption that 08-01 was "already available in the worktree base"). Resolved with a plain fast-forward `git merge main` (no conflicts — the worktree had made zero commits of its own yet), which is the only way to satisfy a hard plan dependency that arrived on `main` after this worktree branched. No destructive git operation was used; verified via `git merge-base --is-ancestor` before merging that this was a genuine fast-forward.
- **Single-write-path proof uses a mutation-pattern regex, not a bare substring count.** The plan's own action text suggested "grep the class source for `resolution_type` and assert exactly one method body references it," but a literal substring count would be 2, not 1 — `record_rejected_records`'s own docstring explains in prose that it deliberately never sets `resolution_type`. The test instead searches each method's source for the literal SQL assignment pattern `resolution_type = %s` (present only in `resolve_rejected_records_for_batch`'s `UPDATE ... SET` clause, and not in its own `WHERE ... = 'PENDING'` predicate, which uses a literal string, not `%s`), which is what the plan's own truth statement is actually asserting.

## Deviations from Plan

**1. [Rule 3 - Blocking] Merged main into the worktree branch to obtain plan 08-01's dependency commits**
- **Found during:** Session start (before Task 1)
- **Issue:** The worktree branch (`worktree-agent-a208eead3cacb9408`) was created from the merge-base with `main` at a commit predating wave 1's merge — `MetadataRepository`'s Protocol stubs for the three methods this plan implements, migrations 0014/0015, and the widened `ValidationResult`/new `RejectedRecord` models did not exist in the worktree. This blocked Task 1 entirely (nothing to implement against).
- **Fix:** `git merge main --no-edit`, a clean fast-forward (`c037263..5031e73`, 17 files, no conflicts) since the worktree had no commits of its own at that point.
- **Files modified:** None directly by the merge beyond what wave 1 already introduced (migrations, `repository.py`, `report.py`, `config/model.py`, `errors.py`, `airflow/dags/_common/integrity_gate.py`, associated tests) — this plan's own Task 1/2 commits land cleanly on top.
- **Verification:** `git merge-base --is-ancestor a493e20 HEAD` confirmed fast-forward eligibility before merging; `git log --oneline -3` after the merge confirmed the expected commit graph; the merge preserved a clean working tree (`git status` showed nothing to commit immediately after).
- **Committed in:** N/A (merge commit is a fast-forward, no new commit object created; visible as the parent of `c1fe2a5`)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Necessary to unblock Task 1 entirely; no scope creep — no code beyond the plan's own two tasks was touched.

## Issues Encountered

None beyond the dependency-merge issue documented above.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `record_validation_results`/`record_rejected_records`/`resolve_rejected_records_for_batch` are now real, live-proven implementations that plans 08-08 (referential integrity barrier), 08-09 (volume anomaly detection), and 08-11 (publish-transaction wiring) can call with confidence, without re-deriving the transactional or whole-batch-only contract themselves.
- No blockers. `tests/integration/` remains fully green (95/95) after this plan's additions.
- `pipeline/run.py`'s actual call sites for these three methods (wiring the barrier stages into the atomic publish transaction) are explicitly out of this plan's scope, per its own objective — that wiring belongs to the plans listed under `affects` above.

---
*Phase: 08-validation-quarantine-metadata-control-plane-completion*
*Completed: 2026-08-17*

## Self-Check: PASSED

- FOUND: packages/dataplat/src/dataplat/metadata/postgres.py
- FOUND: tests/integration/test_validation_persistence.py
- FOUND: tests/integration/test_backfill_resolution.py
- FOUND commit: c1fe2a5 (feat(08-03): implement validation/rejected-records persistence + batch resolution)
- FOUND commit: b2a253b (test(08-03): prove transactional persistence and whole-batch-only resolution)
- Both integration test files pass against real testcontainers PostgreSQL 18 (5/5); full `tests/integration/` suite remains green (95/95)
