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
  - "Live-cluster proof: migration 0020 applied to analytics-db, csv-processor
    image localhost:5001/csv-processor:99171c3 deployed, test_backfill_reentry.py
    -m cluster passed genuinely -- VALID-08's gap closed and confirmed against
    the real running platform, not just automated test tiers"
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
    - tests/e2e/slice/test_backfill_reentry.py

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

requirements-completed: [VALID-08]

# Metrics
duration: 13min (Tasks 1-2, autonomous) + live-cluster deployment/verification (Task 3, orchestrator)
completed: 2026-08-18
---

# Phase 08 Plan 18: business-key-scoped resolution wiring + Test C/C2 rewrite + live-cluster proof Summary

**`run_ingest`'s post-publish resolution step now queries its own just-staged rows for the dataset's configured business-key column and resolves every matching PENDING reject across the whole dataset -- replacing 08-16's `business_keys=[]` placeholder with the real derivation, proven through two integration tests that exercise real `run_ingest` execution across genuinely different batches, AND now proven live against the real kind cluster: `test_backfill_resolves_previously_rejected_row` passed, confirming exactly one PENDING reject transitioned to REDRIVEN via the business-key-scoped path. VALID-08 is closed for real.**

## Performance

- **Duration:** 13 min (Tasks 1-2, this worktree agent) + live-cluster deployment/test-run (Task 3, performed by the orchestrator directly against the shared kind cluster, not by an autonomous worktree agent)
- **Started:** 2026-08-17T22:08:45Z (worktree base reset)
- **Completed:** 2026-08-18 (Task 3 live-cluster proof + docstring update)
- **Tasks:** 3 of 3 complete. Tasks 1-2 (`type="auto"`) completed by this worktree
  agent on 2026-08-17. Task 3 (`type="checkpoint:human-verify"`, `gate="blocking"`)
  was satisfied by the orchestrator: migration 0020 deployed live, csv-processor
  image rebuilt/redeployed, `pytest tests/e2e/slice/test_backfill_reentry.py -x -m
  cluster` run to a genuine PASS, and the module's stale docstring/failure-message
  updated by this continuation agent to match.
- **Files modified:** 4 (adds `tests/e2e/slice/test_backfill_reentry.py`'s
  docstring/failure-message update, Task 3's remaining paperwork)

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
3. **Task 3: Live-cluster proof + docstring update** - live-cluster deployment/test-run performed
   by the orchestrator directly against the shared kind cluster (not a worktree-agent commit);
   the remaining docstring/failure-message paperwork was committed by a continuation worktree
   agent - `94dec58` (docs)

**Plan metadata:** (this commit, docs: complete plan)

### Task 3: Live-cluster proof -- what was performed and confirmed

Performed by the orchestrator directly against the live kind cluster (outside
any worktree-isolated agent, per this task's own `checkpoint:human-verify`
gate):

1. **Migration deployed:** Alembic migrations run through `0020` against the
   live `analytics-db`. Confirmed via direct psql: `SELECT version_num FROM
   meta.alembic_version` -> `0020`. `meta.rejected_records.business_key`
   column and `ix_rejected_records_business_key_resolution` index confirmed
   present live.
2. **Image deployed:** `csv-processor` image rebuilt with this plan's
   `pipeline/run.py` changes baked in, pushed as
   `localhost:5001/csv-processor:99171c3`, and the `csv_processor_image`
   Airflow Variable updated to point at it.
3. **Live test run, genuine pass:**
   `pytest tests/e2e/slice/test_backfill_reentry.py -x -m cluster -v` against
   the live kind cluster --
   ```
   1 passed, 1 warning in 257.29s (0:04:17)
   ```
   `test_backfill_resolves_previously_rejected_row` PASSED, reaching and
   passing its final `_assert_row_resolved` call: the original PENDING
   reject flipped to `REDRIVEN` via the new business-key-scoped resolution
   path (`resolve_rejected_records_for_business_keys`), `resolved_by_run_id`
   correctly linked to the corrected file's own new
   `meta.ingestion_runs.run_id`.
4. **Live DB confirmation, post-run:**
   ```sql
   SELECT resolution_type, count(*) FROM meta.rejected_records GROUP BY resolution_type;
   ```
   returned `PENDING: 8`, `REDRIVEN: 1` -- exactly one row transitioned to
   `REDRIVEN`, matching this single test run's own single corrected reject.
   This is the first live confirmation that the VALID-08 gap
   (08-VERIFICATION.md's live-confirmed batch_id-scoping failure) is
   genuinely closed against the real, running platform -- not merely at the
   unit/integration test tiers.
5. **Docstring/failure-message paperwork (this continuation agent,
   `94dec58`):** `tests/e2e/slice/test_backfill_reentry.py`'s module
   docstring paragraph describing the OLD `resolve_rejected_records_for_
   batch` (D-05) batch_id-scoping caveat, and `_assert_row_resolved`'s
   failure-message string referencing that same caveat as an open question,
   both replaced with an accurate description of the NEW, now-proven
   `resolve_rejected_records_for_business_keys` (D-23) mechanism.
   Documentation-only -- no test logic or assertions modified (those were
   already correctly rewritten in Tasks 1-2 of this plan).

## Files Created/Modified
- `packages/dataplat/src/dataplat/pipeline/run.py` - Hoisted `dataset_id`, real business-key derivation query, `resolve_rejected_records_for_business_keys` call with actual published business keys
- `tests/unit/test_run_ingest_trace.py` - `_FakeCursor.fetchall()` stub, corrected `_make_config()` docstring
- `tests/integration/test_publish_transaction_wiring.py` - `_insert_pending_reject` gains `business_key`; `_insert_config_version` becomes get-or-insert; Test C/C2 un-skipped and rewritten for cross-batch proof
- `tests/e2e/slice/test_backfill_reentry.py` - Module docstring paragraph and `_assert_row_resolved`'s failure-message string updated from the superseded D-05 batch_id-scoping caveat to the live-proven D-23 business-key-scoped mechanism (Task 3, documentation-only)

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
- Docker/testcontainers were available in this environment (`docker ps` confirmed a live `kind` cluster's containers already running), so `tests/integration/` (testcontainers-backed) ran successfully in this worktree session. `tests/e2e/slice -m cluster` (Task 3's own scope) was deliberately NOT attempted by this worktree agent -- it targets the real, shared, resource-constrained kind cluster and requires deploying this plan's own migration/image first, which is the checkpoint's job, not an autonomous task's. It was subsequently performed by the orchestrator directly (see "Task 3" above) -- migration 0020 and the rebuilt image deployed live, and `test_backfill_resolves_previously_rejected_row` passed genuinely (`1 passed, 1 warning in 257.29s`).

## User Setup Required

**None remaining.** Task 3's `checkpoint:human-verify` gate (`gate="blocking"`) has been satisfied: the orchestrator deployed migration `0020` and the rebuilt `csv-processor` image (`localhost:5001/csv-processor:99171c3`) to the live kind cluster, ran `pytest tests/e2e/slice/test_backfill_reentry.py -x -m cluster` to a genuine pass, and confirmed via direct DB query that exactly one `meta.rejected_records` row transitioned `PENDING -> REDRIVEN`. This continuation agent completed the checkpoint's remaining paperwork (the module docstring/failure-message update) and committed it.

## Next Phase Readiness
- `run_ingest`'s business-key-scoped resolution is fully wired and proven at
  three test tiers: `tests/unit/test_run_ingest_trace.py` (offline, fakes),
  `tests/integration/test_publish_transaction_wiring.py` (real testcontainers
  Postgres/MinIO, real `run_ingest` execution, cross-batch business-key
  matching AND CR-01 self-protection both proven live), and now
  `tests/e2e/slice/test_backfill_reentry.py -m cluster` (the real live kind
  cluster, a genuine `airflow backfill create` re-execution, real
  `csv-processor` pods)
- Task 3's live-cluster proof is COMPLETE: `test_backfill_resolves_
  previously_rejected_row` passed (`1 passed, 1 warning in 257.29s
  (0:04:17)`), and `meta.rejected_records` shows `PENDING: 8, REDRIVEN: 1`
  post-run -- Phase 8's roadmap success criterion 3 ("Corrected quarantined
  records re-enter the pipeline through the documented re-drive path and
  land in the warehouse") is now closed for real, with live-cluster proof,
  matching the same standard 08-14/08-15 were held to
- `08-HUMAN-UAT.md`'s own test 1 (the exact live re-verification this plan's
  Task 3 performs) has now been genuinely re-verified live -- VALID-08's
  requirement row is ready for the orchestrator to mark complete in
  REQUIREMENTS.md post-merge (not done here; this worktree agent does not
  edit REQUIREMENTS.md/STATE.md/ROADMAP.md per its own scope)

---
*Phase: 08-validation-quarantine-metadata-control-plane-completion*
*Completed: 2026-08-18*

## Self-Check: PASSED

- FOUND: packages/dataplat/src/dataplat/pipeline/run.py
- FOUND: tests/unit/test_run_ingest_trace.py
- FOUND: tests/integration/test_publish_transaction_wiring.py
- FOUND: tests/e2e/slice/test_backfill_reentry.py
- FOUND: .planning/phases/08-validation-quarantine-metadata-control-plane-completion/08-18-SUMMARY.md
- FOUND: commit 463ca8d (Task 1)
- FOUND: commit ffecf93 (Task 2)
- FOUND: commit da73277 (Task 1-2 plan-metadata commit)
- FOUND: commit 94dec58 (Task 3 docstring/failure-message update)
- Live-cluster proof (not a repo artifact, recorded here for traceability): `pytest tests/e2e/slice/test_backfill_reentry.py -x -m cluster -v` -> `1 passed, 1 warning in 257.29s (0:04:17)`; `meta.rejected_records` post-run: `PENDING: 8, REDRIVEN: 1`
