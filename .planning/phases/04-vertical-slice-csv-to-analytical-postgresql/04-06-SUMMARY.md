---
phase: 04-vertical-slice-csv-to-analytical-postgresql
plan: 06
subsystem: testing
tags: [postgresql, testcontainers, minio, psycopg, pytest, discovery, publish, advisory-lock, idempotency]

# Dependency graph
requires:
  - phase: 04-01
    provides: MetadataRepository/ObjectStore/ConfigRegistry interface contracts, migrations 0001-0006
  - phase: 04-03
    provides: discover_files (frozen-manifest authoring), AssignmentDocument/BatchAssignment models
  - phase: 04-04
    provides: StagingLoader, MergePublisher (advisory-lock + ON CONFLICT publication)
provides:
  - Integration-level proof (real testcontainers Postgres + MinIO) that discovery is genuinely idempotent/frozen across reruns
  - Integration-level proof that publication is atomic (META-03), single-writer-safe under real concurrency (LOAD-09), and lineage-queryable (LOAD-04)
  - A real, previously-latent discover_files rerun crash fixed (get_or_create_batch), closing a bug 04-03 had already found and deliberately deferred to this plan
  - Documented, evidence-based finding that MergePublisher's INSERT...ON CONFLICT does not independently require pg_advisory_xact_lock for its own single-arbiter-index correctness, while explaining why the lock is kept anyway
affects: [04-05, 04-07, 04-08, 04-09]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "get_or_create_* idempotent-upsert idiom (INSERT ... ON CONFLICT DO UPDATE, status/mutable fields excluded from the conflict SET clause) extended from datasets/ingestion_runs to batches"
    - "link tables made idempotent via ON CONFLICT (composite_pk) DO NOTHING when their parent row is now resolved via a get_or_create_* call"
    - "concurrency test pattern: threading.Event + a lock held open by the test's own un-committed transaction (not sleep) to deterministically prove a blocking window"
    - "negative-case verification for concurrency tests: temporarily disable the mechanism under test, run repeatedly, document the empirical result in the test's own docstring — including when the result is 'passes anyway' and why"

key-files:
  created:
    - tests/integration/test_discover_files.py
  modified:
    - tests/integration/test_publish_merge.py
    - packages/dataplat/src/dataplat/discovery.py
    - packages/dataplat/src/dataplat/metadata/repository.py
    - packages/dataplat/src/dataplat/metadata/postgres.py
    - tests/unit/test_discovery.py
    - .planning/phases/04-vertical-slice-csv-to-analytical-postgresql/deferred-items.md
    - .planning/REQUIREMENTS.md

key-decisions:
  - "Fixed discover_files' rerun crash by adding MetadataRepository.get_or_create_batch (mirroring get_or_create_dataset's idiom) rather than making create_batch itself idempotent — this plan's own Task 2 requires create_batch to still raise on a literal duplicate call, so the two methods now serve two different, documented callers"
  - "Kept pg_advisory_xact_lock in test_advisory_lock_serializes_concurrent_publishers despite the required negative-case check showing the test still passes without it — documented the root cause (INSERT ... ON CONFLICT's native unique-index handling already serializes a single-arbiter-index, single-statement, fixed-ORDER-BY publisher) directly in the test's docstring rather than silently declaring the check satisfied"
  - "Made link_batch_file idempotent (ON CONFLICT DO NOTHING) as a companion fix to get_or_create_batch, since reusing an existing batch_id on rerun would otherwise collide on its own composite primary key"

patterns-established:
  - "Idempotent metadata-repository writes always exclude mutable/status-like columns from their ON CONFLICT SET clause, so a rediscovery can never regress a row that has already progressed past its initial state"

requirements-completed: [ORCH-08, META-03, LOAD-04, LOAD-08, LOAD-09, QUAL-05]

# Metrics
duration: 40min
completed: 2026-08-13
---

# Phase 4 Plan 06: Discovery Rerun and Publish Concurrency/Atomicity Integration Tests Summary

**Proved ORCH-08/META-03/LOAD-04/LOAD-08/LOAD-09 against a real testcontainers Postgres+MinIO, and along the way fixed a real `discover_files` rerun crash that the unit-level fakes could never have caught.**

## Performance

- **Duration:** ~40 min
- **Started:** 2026-08-13 (worktree base corrected to `fb9a5dc` before work began)
- **Completed:** 2026-08-13T17:42:56+02:00 (last task commit)
- **Tasks:** 2 (both `type="auto"`)
- **Files created:** 1
- **Files modified:** 7

## Accomplishments

- `tests/integration/test_discover_files.py` (new, 4 tests): proves discovery reruns over an unchanged object set are genuinely frozen (identical assignment manifest, zero additional `meta.files`/`meta.ingestion_runs` rows) both before and after the underlying runs reach `SUCCEEDED`; proves D-13 duplicate-content-skip; proves `meta.files.business_date` is never populated by discovery; proves the `max_units_per_run` cap defers rather than drops excess units.
- **Found and fixed a real bug**: the first test above reproduced `discover_files` crashing with `psycopg.errors.UniqueViolation` on `meta.batches` when called a second time over an unchanged file set — a bug 04-03's own executor had already found and explicitly deferred to this plan (`deferred-items.md`'s "From Plan 04-03" section), since proving rerun-safety against a real database was this plan's job, not 04-03's. Fixed by adding `MetadataRepository.get_or_create_batch` and making `link_batch_file` idempotent.
- `tests/integration/test_publish_merge.py` (extended, +4 tests alongside 04-04's existing 5): proves the publish+finalize transaction is invisible until committed then becomes visible as one indivisible transition (META-03); proves two concurrent publishers with overlapping `customer_id`s genuinely serialize with no error (LOAD-09); proves every loaded row's lineage columns are independently SQL-queryable (LOAD-04); proves a duplicate `(dataset_id, batch_key)` is rejected by the database (LOAD-08).
- Performed the plan's required negative-case check for the concurrency test, found (and documented, precisely) that it does not discriminate for this specific single-arbiter-index publisher — a genuine finding, not a shortcut taken.

## Task Commits

Each task was committed atomically (Task 1's commit also carries its Rule 1 bug fix, since the fix was a precondition for the test passing at all):

1. **Task 1: test_discover_files.py + discover_files rerun-safety fix** - `36ca08a` (fix)
2. **Task 2: test_publish_merge.py atomicity/concurrency/lineage/batch-uniqueness tests** - `f1300d9` (test)
3. **Deferred-items.md cross-reference + findings log** - `e839012` (docs)

## Files Created/Modified

- `tests/integration/test_discover_files.py` - New: 4 integration tests proving ORCH-08's frozen-manifest/rerun-idempotency guarantee
- `tests/integration/test_publish_merge.py` - Extended (04-04's file): +4 tests for META-03/LOAD-09/LOAD-04/LOAD-08
- `packages/dataplat/src/dataplat/discovery.py` - `discover_files` now calls `get_or_create_batch` instead of `create_batch`
- `packages/dataplat/src/dataplat/metadata/repository.py` - Added `get_or_create_batch` to the `MetadataRepository` Protocol; `link_batch_file`'s docstring updated for its new idempotency
- `packages/dataplat/src/dataplat/metadata/postgres.py` - Implemented `get_or_create_batch`; `link_batch_file` now `ON CONFLICT (batch_id, file_id) DO NOTHING`
- `tests/unit/test_discovery.py` - Fake repository's `create_batch` renamed to `get_or_create_batch`, made idempotent (keyed by `(dataset_id, batch_key)`) to match the real method it now stands in for
- `.planning/phases/04-vertical-slice-csv-to-analytical-postgresql/deferred-items.md` - Marked the 04-03 batch-idempotency item RESOLVED; logged two new findings (pre-existing out-of-gate mypy gap, advisory-lock negative-case result)
- `.planning/REQUIREMENTS.md` - `ORCH-08` and `QUAL-05` marked complete (META-03/LOAD-04/LOAD-08/LOAD-09 were already complete from 04-04)

## Decisions Made

- **`get_or_create_batch`, not an idempotent `create_batch`**: Task 2's own `test_duplicate_batch_key_rejected` requires `create_batch` to raise `psycopg.errors.UniqueViolation` on a literal duplicate call, proving `uq_batches_dataset_batch_key` is real. Making `create_batch` itself idempotent would have silently broken that requirement. Adding a sibling method (mirroring the existing `create_ingestion_run`/`get_or_create_ingestion_run` pair) serves both callers correctly.
- **`status` excluded from `get_or_create_batch`'s `ON CONFLICT ... SET` clause**: a rediscovery of a file whose batch has already progressed past `OPEN` (e.g. to `PUBLISHED`) must never be silently reset back to the caller's `status="OPEN"` argument. This follows `create_file`'s own precedent (its `DO UPDATE` clause never touches `status` either).
- **Kept the advisory lock in the concurrency test despite the negative-case check passing without it**: rather than treat "the test doesn't fail without the lock" as a problem to hide or a reason to drop the lock, documented the exact mechanism (PostgreSQL's native unique-index insert-conflict blocking already serializes a single-arbiter-index, fixed-`ORDER BY` publisher — the specific bug class literal `MERGE` has, per PostgreSQL BUG #18279, that `INSERT ... ON CONFLICT` was designed not to have) directly in the test's own docstring and in `deferred-items.md`. The lock stays because it is the documented `merge.py` caller contract and remains load-bearing the moment a future publisher adds a second statement or a second unique index.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug, previously deferred by 04-03] `discover_files` crashed on rerun over an unchanged object set**
- **Found during:** Task 1, `test_rerun_produces_identical_manifest` (first empirical run, before any fix)
- **Issue:** `discover_files` called `metadata.create_batch(...)` unconditionally on every non-duplicate object, every call. `create_batch` is a plain, non-idempotent `INSERT ... RETURNING` (no `ON CONFLICT`). Since `batch_key` is a pure function of the file's content hash, a second `discover_files` call over the *same unchanged file* reaches `create_batch` again with the identical `(dataset_id, batch_key)`, raising `psycopg.errors.UniqueViolation` against `uq_batches_dataset_batch_key`. `tests/unit/test_discovery.py`'s fake repository never caught this because its own `create_batch` double always minted a fresh `batch_id`, never enforcing the real constraint. 04-03's own executor had already found and documented this exact gap in `deferred-items.md`, correctly deferring it since proving rerun-safety against a real database was explicitly this plan's job (per `04-RESEARCH.md`: "04-RESEARCH.md assigns it to plan 04-06's integration-test suite").
- **Fix:** Added `MetadataRepository.get_or_create_batch` (Protocol + `PostgresMetadataRepository`), an `INSERT ... ON CONFLICT (dataset_id, batch_key) DO UPDATE SET batch_key = EXCLUDED.batch_key RETURNING batch_id` mirroring `get_or_create_dataset`'s idiom exactly, with `status` deliberately excluded from the conflict `SET` clause. `discovery.py` now calls this instead of `create_batch`. Also made `link_batch_file` idempotent (`ON CONFLICT (batch_id, file_id) DO NOTHING`), since reusing an existing `batch_id` on rerun would otherwise collide on its own composite primary key. `create_batch` itself is untouched — still a plain, raising `INSERT ... RETURNING` — since this plan's own Task 2 test depends on that exact behavior.
- **Files modified:** `packages/dataplat/src/dataplat/discovery.py`, `packages/dataplat/src/dataplat/metadata/repository.py`, `packages/dataplat/src/dataplat/metadata/postgres.py`, `tests/unit/test_discovery.py` (fake updated to match)
- **Verification:** `test_rerun_produces_identical_manifest` passes; full `tests/unit` (126), `tests/integration` (52), `make lint`/`make format`/`make typecheck`/`make imports` all pass; `make policy` shows only the two pre-existing, already-documented import-linter output-format failures (unrelated, confirmed via `git stash` re-run)
- **Committed in:** `36ca08a`

---

**Total deviations:** 1 auto-fixed (1 Rule 1 bug fix, previously identified and deliberately deferred by an earlier plan to this one).
**Impact on plan:** Necessary for correctness — this plan's own must-have truth ("discovery rerun over an unchanged object set... creates zero additional meta.files/meta.ingestion_runs rows") cannot be proven true while the underlying crash exists. No scope creep: the fix is the exact one 04-03 already suggested, applied only to the discovery call path, and `create_batch`'s own raising behavior (needed by this plan's own Task 2) is untouched.

## Issues Encountered

- **`test_advisory_lock_serializes_concurrent_publishers`'s required negative-case check did not fail as anticipated.** Per this plan's own acceptance criteria, temporarily removed both `pg_advisory_xact_lock` calls and re-ran the test repeatedly — it passed every time, no flake, no constraint violation. Root cause (traced directly in `merge.py`'s `_PUBLISH_SQL`): the publisher arbitrates on exactly one unique index via a single `INSERT ... SELECT ... ORDER BY customer_id ...` statement, so PostgreSQL's own unique-index insert-conflict handling already forces a second writer to block on the same row until the first transaction resolves, deterministically, with no deadlock possible (both statements always process overlapping keys in the same fixed order). This is not a defect — it is the documented reason `INSERT ... ON CONFLICT` was chosen over literal `MERGE` (PostgreSQL BUG #18279, `merge.py`'s own module docstring, PITFALLS.md C1). Resolved by documenting the finding precisely in the test's own docstring and in `deferred-items.md`, and keeping the lock as the still-correct, still-necessary documented caller contract and defense-in-depth for any future multi-statement or multi-index publisher. See `## Known Findings` below.

## Known Findings

Not stubs, not incomplete work — two evidence-based findings surfaced during required verification steps, both fully documented in place (test docstrings + `deferred-items.md`) rather than left implicit:

1. **`pg_advisory_xact_lock` is not independently load-bearing for `MergePublisher`'s current single-arbiter-index, single-statement shape**, though it remains the documented, correct caller contract and defense-in-depth. See `tests/integration/test_publish_merge.py::test_advisory_lock_serializes_concurrent_publishers`'s own docstring for the full mechanism trace.
2. **`tests/unit/test_discovery.py`'s fake `_FakeMetadataRepository` does not structurally satisfy `mypy`'s `MetadataRepository` Protocol check** (missing `create_ingestion_run`/`claim_ingestion_run`/`finalize_publication`/`update_ingestion_run_status` stubs) — pre-existing since 04-03, confirmed unrelated to this plan's diff via `git stash`, and outside `Makefile`'s enforced `TYPECHECK_PATHS` (which excludes `tests/` entirely). Logged in `deferred-items.md`, not fixed (out of scope).

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- ORCH-08, META-03, LOAD-04, LOAD-08, LOAD-09 and QUAL-05 are now proven against a real database/object store, not merely asserted — plan 04-05 (`run_ingest` orchestration) can build its publication transaction on `MergePublisher`/`finalize_publication` with confidence the atomicity and concurrency properties actually hold, and should follow this plan's `test_advisory_lock_serializes_concurrent_publishers` docstring's guidance: keep taking `pg_advisory_xact_lock` immediately before `publish()`, inside the same transaction, exactly as `merge.py`'s own module docstring already specifies.
- `discover_files` is now genuinely safe to call repeatedly over an unchanged object set — this was a precondition 04-05's orchestration and any later scheduled/retried DAG run implicitly depends on.
- No blockers for wave 4 (`04-07`, the DAG files) or wave 5 (`04-08`/`04-09`, E2E and demo).

---
*Phase: 04-vertical-slice-csv-to-analytical-postgresql*
*Completed: 2026-08-13*

## Self-Check: PASSED

- FOUND: `tests/integration/test_discover_files.py`
- FOUND: `.planning/phases/04-vertical-slice-csv-to-analytical-postgresql/04-06-SUMMARY.md`
- FOUND: commit `36ca08a` (Task 1: discover_files rerun-safety tests + fix)
- FOUND: commit `f1300d9` (Task 2: publish atomicity/concurrency/lineage/batch-uniqueness tests)
- FOUND: commit `e839012` (deferred-items.md cross-reference + findings)
- FOUND: commit `d691dc7` (SUMMARY.md + REQUIREMENTS.md)
- Plan-level verification re-run: `uv run --group cluster pytest tests/integration/test_discover_files.py tests/integration/test_publish_merge.py -x -q` → 13 passed
- Full regression check: `make lint`, `make format`, `make typecheck`, `make imports`, `make test` (126 unit+regression) all pass; `make policy` shows only the two pre-existing, already-documented import-linter output-format failures (confirmed unrelated via `git stash`)
