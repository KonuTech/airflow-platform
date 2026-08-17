---
phase: 08-validation-quarantine-metadata-control-plane-completion
plan: 18
subsystem: database
tags: [postgresql, run_ingest, metadata-repository, quarantine, backfill, business-key]

# Dependency graph
requires:
  - phase: 08 (plan 08-16)
    provides: meta.rejected_records.business_key column, RejectedRecord.business_key
      field, MetadataRepository.resolve_rejected_records_for_business_keys
      (dataset_id, business_key)-scoped repository implementation
  - phase: 08 (plan 08-17)
    provides: business_key extraction wired into every RejectedRecord-creation
      site (streaming quality rules + ReferentialIntegrityBarrier), threaded
      from StagingLoader
provides:
  - "run_ingest's real business-key derivation: _apply_post_publish_barriers_and_persist
    queries staging_result.staging_table for the distinct business-key column
    values this run actually staged, replacing 08-16's business_keys=[] placeholder"
  - "test_publish_transaction_wiring.py's Test C/C2 un-skipped and rewritten to
    prove cross-batch business-key resolution AND CR-01 self-protection through
    real run_ingest execution"
affects: [08-HUMAN-UAT]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "dataset_id computed exactly once at the top of
      _apply_post_publish_barriers_and_persist, reused by both the VOLUME
      barrier branch and the resolution call -- never queried twice per run"
    - "SELECT DISTINCT {business_key_column} FROM {staging_table} WHERE
      {business_key_column} IS NOT NULL, read on the SAME conn/transaction
      the rest of the publish barrier writes through, before the trailing
      DROP TABLE -- identifier-only interpolation (T-04-01/T-08-15 precedent)"
    - "get-or-insert meta.config_versions helper (test-suite-only): reuses an
      existing CURRENT (valid_to IS NULL) config_version_id per dataset_id
      instead of inserting a second one, required once two tests in the same
      file deliberately seed under the SAME dataset_id"

key-files:
  created: []
  modified:
    - packages/dataplat/src/dataplat/pipeline/run.py
    - tests/unit/test_run_ingest_trace.py
    - tests/integration/test_publish_transaction_wiring.py

key-decisions:
  - "Test C/C2 must seed their batches/files/rejects under the SAME dataset_id
    ctx.config.dataset ('customers', _make_config's own hardcoded value)
    resolves to via get_or_create_dataset inside run_ingest -- not a distinct
    wiring_*-named dataset, which was harmless under the OLD strictly
    batch_id-scoped resolve (no dataset join existed) but breaks D-23's new
    (dataset_id, business_key) join if the seeded rows live under a different
    dataset_id than the one run_ingest itself resolves. Found live via a
    debug print showing published_business_keys correctly populated with the
    expected values while the resolve call still affected zero rows --
    the join's dataset_id predicate was the actual cause, not the business-key
    matching logic."
  - "_insert_config_version became get-or-insert (checks for an existing
    CURRENT row first) rather than blindly inserting, once Test C and Test C2
    both seed under the shared 'customers' dataset_id -- meta.config_versions'
    own uq_config_versions_current_per_dataset partial unique index (migration
    0001) permits at most one CURRENT config_version per dataset, so a second
    unconditional insert for the same dataset_id would always collide."
  - "Test C2's seeded cross-batch business_key changed from the plan's literal
    business_key='9404999' (deliberately outside the backfill file's own
    published customer_id range) to '9404005' (one of the backfill run's own
    published customer_ids) -- the plan's own instruction text was internally
    contradictory: a business_key that matches NONE of the published rows can
    never resolve under D-23's (dataset_id, business_key)-scoped predicate (the
    row was never staged, so SELECT DISTINCT can never surface it), yet the
    same instruction sentence required asserting it 'resolves to REDRIVEN.'
    Fixed as a Rule 1 auto-fix (a literally impossible acceptance criterion is
    a bug, not a design choice) by choosing a business_key that IS published
    -- this is what makes the cross-batch-resolution half of the test
    logically coherent, while the CR-01 half (this run's OWN fresh reject,
    business_key=9404010, genuinely never published) is unchanged and proves
    the distinct, separate guarantee."

requirements-completed: []

# Metrics
duration: 13min
completed: 2026-08-18
---

# Phase 08 Plan 18: business-key-scoped resolution wiring + Test C/C2 rewrite Summary

**`run_ingest`'s post-publish resolution step now queries its own just-staged rows for the dataset's configured business-key column and resolves every matching PENDING reject across the whole dataset -- replacing 08-16's `business_keys=[]` placeholder with the real derivation, proven through two integration tests that exercise real `run_ingest` execution across genuinely different batches.**

## Performance

- **Duration:** 13 min
- **Started:** 2026-08-17T22:08:45Z (worktree base reset)
- **Completed:** 2026-08-17T22:20:59Z
- **Tasks:** 2 of 3 (`type="auto"`) completed; Task 3 (`type="checkpoint:human-verify"`,
  `gate="blocking"`) reached and NOT attempted -- live-cluster deployment/test-run
  is out of scope for an autonomous worktree agent
- **Files modified:** 3

## Accomplishments
- `_apply_post_publish_barriers_and_persist` hoists `dataset_id =
  ctx.metadata.get_or_create_dataset(ctx.config.dataset)` to the top of the
  function, computed exactly once, reused by both the pre-existing VOLUME
  barrier branch and the new resolution call
- The real business-key derivation: queries `staging_result.staging_table`
  (still live at this point in the transaction, before the trailing `DROP
  TABLE`) for the distinct, non-null values of the dataset's configured
  business-key column, and passes them into
  `resolve_rejected_records_for_business_keys` -- replacing 08-16's
  documented `business_keys=[]` no-op placeholder
- `resolve_rejected_records_for_business_keys` now genuinely resolves PENDING
  rejects sharing a business key this run published, across ANY prior
  `batch_id` within the same dataset -- the exact VALID-08 gap
  08-VERIFICATION.md confirmed live
- `test_publish_transaction_wiring.py`'s Test C
  (`test_backfill_run_resolves_the_batch_pending_rejects`) and Test C2
  (`test_backfill_run_never_resolves_its_own_fresh_rejects`), both un-skipped
  and rewritten to seed PENDING rejects under a batch_id SEPARATE from the
  run that resolves them -- proving cross-batch resolution AND CR-01
  self-protection through real `run_ingest` execution, not a direct
  repository call

## Task Commits

Each task was committed atomically:

1. **Task 1: Wire business-key-scoped resolution into run_ingest's publish transaction** - `463ca8d` (feat)
2. **Task 2: Rewrite test_publish_transaction_wiring.py's Test C/C2 to prove cross-batch resolution** - `ffecf93` (feat)

**Plan metadata:** (this commit, docs: complete plan)

Task 3 (checkpoint) NOT executed -- see "Next Phase Readiness" / the returned
checkpoint state for the exact live-cluster steps required.

## Files Created/Modified
- `packages/dataplat/src/dataplat/pipeline/run.py` - Hoisted `dataset_id`, real business-key derivation query, `resolve_rejected_records_for_business_keys` call with actual published business keys
- `tests/unit/test_run_ingest_trace.py` - `_FakeCursor.fetchall()` stub, corrected `_make_config()` docstring
- `tests/integration/test_publish_transaction_wiring.py` - `_insert_pending_reject` gains `business_key`; `_insert_config_version` becomes get-or-insert; Test C/C2 un-skipped and rewritten for cross-batch proof

## Decisions Made
- See `key-decisions` in frontmatter: (1) Test C/C2 must seed under the SAME
  "customers" dataset_id `ctx.config.dataset` resolves to, not a distinct
  `wiring_*`-named dataset -- found live via a debug print showing correctly
  computed `published_business_keys` while the resolve call still affected
  zero rows, root-caused to the dataset_id join mismatch, not the
  business-key matching logic itself; (2) `_insert_config_version` became
  get-or-insert once two tests share a dataset_id, to respect
  `meta.config_versions`' own `uq_config_versions_current_per_dataset`
  constraint; (3) Test C2's seeded business_key changed from the plan's
  literal (and internally contradictory) `"9404999"` to `"9404005"` -- an
  actually-published value, since a never-published business key can
  structurally never resolve under D-23's own predicate.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Test C/C2 seeded batches/rejects under a dataset_id different from the one `run_ingest` itself resolves, silently making the new resolve call a no-op**
- **Found during:** Task 2 verification -- Test C failed with the seeded reject staying `PENDING` despite `published_business_keys` (confirmed via a temporary debug `print`) correctly containing the matching value
- **Issue:** `_make_config()` hardcodes `dataset="customers"`, so `run_ingest`'s own `ctx.metadata.get_or_create_dataset(ctx.config.dataset)` always resolves the `"customers"` dataset_id. The plan's own instruction text (and the pre-existing, now-un-skipped test bodies) seeded batches/files/rejects under a separately-named dataset (`"wiring_backfill_resolution"`/`"wiring_backfill_own_rejects"`) -- harmless under the OLD strictly `batch_id`-scoped resolve (no dataset join existed at all), but D-23's new `(dataset_id, business_key)`-scoped join requires the seeded rows' `dataset_id` to match `"customers"`'s own
- **Fix:** Changed both tests' `get_or_create_dataset(...)` call to `get_or_create_dataset("customers")`, matching what `ctx.config.dataset` will resolve to inside `run_ingest`
- **Files modified:** tests/integration/test_publish_transaction_wiring.py
- **Verification:** `pytest tests/integration/test_publish_transaction_wiring.py -q -m integration` -- both tests pass
- **Committed in:** ffecf93 (Task 2 commit)

**2. [Rule 1 - Bug] Sharing the "customers" dataset_id across Test C and Test C2 collided on `meta.config_versions`' own uniqueness constraint**
- **Found during:** Task 2 verification, immediately after fix #1 above -- `test_backfill_run_never_resolves_its_own_fresh_rejects` failed with `UniqueViolation: uq_config_versions_current_per_dataset`
- **Issue:** `meta.config_versions` (migration 0001) permits at most one CURRENT (`valid_to IS NULL`) row per `dataset_id`. `_insert_config_version` unconditionally inserted a new row every call; once both tests shared the `"customers"` dataset_id (fix #1), the second call always collided
- **Fix:** Rewrote `_insert_config_version` as get-or-insert: SELECT an existing CURRENT `config_version_id` for the dataset first, return it if found, else insert as before
- **Files modified:** tests/integration/test_publish_transaction_wiring.py
- **Verification:** `pytest tests/integration/test_publish_transaction_wiring.py -q -m integration` -- 5/5 pass
- **Committed in:** ffecf93 (Task 2 commit)

**3. [Rule 1 - Bug] Plan's literal Test C2 business_key value ("9404999") made the acceptance criterion internally impossible to satisfy**
- **Found during:** Task 2, while drafting Test C2 from the plan's literal action text
- **Issue:** The plan instructed seeding the cross-batch reject with a `business_key` "deliberately OUTSIDE" the backfill run's own published customer_id range, then asserting that SAME row "resolves to REDRIVEN" -- but under D-23's `(dataset_id, business_key)`-scoped predicate, a business key that was never published can structurally never resolve (it was never staged, so `SELECT DISTINCT` over the staging table can never surface it). The two halves of the instruction directly contradict each other
- **Fix:** Used `business_key="9404005"` -- one of the backfill run's own genuinely-published customer_ids -- for the cross-batch-resolution half of the proof, while the CR-01 half (this run's OWN fresh reject, `business_key=9404010`, truly never published) is unchanged and proves the separate, distinct guarantee
- **Files modified:** tests/integration/test_publish_transaction_wiring.py
- **Verification:** `pytest tests/integration/test_publish_transaction_wiring.py -q -m integration` -- Test C2 passes, both its cross-batch-resolution and CR-01 assertions hold
- **Committed in:** ffecf93 (Task 2 commit)

---

**Total deviations:** 3 auto-fixed (all Rule 1 -- bugs surfaced by, and fixed during, this plan's own live test execution; none discovered after the fact)
**Impact on plan:** All three fixes were necessary to make the plan's own acceptance criteria achievable at all -- none invented new production business logic beyond what the plan specified, and none touched `packages/dataplat/src/dataplat/pipeline/run.py` (Task 1's file) beyond its already-committed state. The fixes are entirely confined to test-seeding correctness in `tests/integration/test_publish_transaction_wiring.py`.

## Issues Encountered
- Same worktree-environment quirk documented in 08-16-SUMMARY.md/08-17-SUMMARY.md: the venv at the main repo's `.venv` is an editable install pointing at the MAIN repo's `packages/dataplat/src`, not this worktree's copy. `PYTHONPATH=<worktree>/packages/dataplat/src` was prepended for every `mypy`/`pytest` verification command in this session too. Not a code issue.
- Docker/testcontainers were available in this environment (`docker ps` confirmed a live `kind` cluster's containers already running), so `tests/integration/` (testcontainers-backed) ran successfully in this worktree session. `tests/e2e/slice -m cluster` (Task 3's own scope) was deliberately NOT attempted -- it targets the real, shared, resource-constrained kind cluster and requires deploying this plan's own migration/image first, which is explicitly the checkpoint's job, not an autonomous task's.

## User Setup Required

**Task 3 is a `checkpoint:human-verify` gated task (`gate="blocking"`) -- live-cluster deployment and verification, not yet performed.** See the checkpoint state returned alongside this summary for the exact steps: deploy Alembic migrations through `0020`, rebuild and redeploy the `csv-processor` image with this plan's `pipeline/run.py` changes baked in, then run `pytest tests/e2e/slice/test_backfill_reentry.py -x -m cluster` to a genuine completion and update that test module's stale docstring once it passes.

## Next Phase Readiness
- `run_ingest`'s business-key-scoped resolution is fully wired and proven at
  two test tiers: `tests/unit/test_run_ingest_trace.py` (offline, fakes) and
  `tests/integration/test_publish_transaction_wiring.py` (real testcontainers
  Postgres/MinIO, real `run_ingest` execution, cross-batch business-key
  matching AND CR-01 self-protection both proven live)
- Task 3's live-cluster proof against `tests/e2e/slice/test_backfill_reentry.py
  -m cluster` remains the ONE outstanding item this plan's own success
  criteria require before Phase 8's roadmap success criterion 3 ("Corrected
  quarantined records re-enter the pipeline through the documented re-drive
  path and land in the warehouse") can be marked closed for real
- `08-HUMAN-UAT.md`'s own test 1 (the exact live re-verification this plan's
  Task 3 performs) should be re-attempted now that this plan's code wiring is
  complete and committed -- deploying migration `0020` + the rebuilt image is
  the only remaining step

---
*Phase: 08-validation-quarantine-metadata-control-plane-completion*
*Completed: 2026-08-18*

## Self-Check: PASSED

- FOUND: packages/dataplat/src/dataplat/pipeline/run.py
- FOUND: tests/unit/test_run_ingest_trace.py
- FOUND: tests/integration/test_publish_transaction_wiring.py
- FOUND: .planning/phases/08-validation-quarantine-metadata-control-plane-completion/08-18-SUMMARY.md
- FOUND: commit 463ca8d (Task 1)
- FOUND: commit ffecf93 (Task 2)
- FOUND: commit da73277 (this SUMMARY.md commit)
