---
phase: 09-etl-correctness-dedup-incremental-backfill-recovery
plan: 07
subsystem: database
tags: [reconciliation, postgres, staging, etl, batch-complete, control-total, psycopg]

# Dependency graph
requires:
  - phase: 09-etl-correctness-dedup-incremental-backfill-recovery
    provides: "record_reconciliation (plan 09-02) and RunContext.batch_expected_row_count/batch_expected_checksum plumbing (plan 09-03)"
provides:
  - "promote_to_durable_bronze writes one meta.reconciliation_results row per call, hop='raw_bronze', in the same transaction as its INSERT/DROP TABLE"
  - "VALID-06's control-total comparison proven end-to-end from a real, uploaded _BATCH_COMPLETE manifest through stage_ingest to a persisted, non-blocking discrepancy record"
affects: [09-08, verify-phase-09]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "raw_bronze reconciliation hop written inline with promote_to_durable_bronze, same conn/transaction as the durable-bronze INSERT (D-21)"
    - "control_total_discrepancy computed in SQL only when expected_row_count is not NULL; a discrepancy is recorded and the run continues, never blocking (D-22/VALID-06)"

key-files:
  created:
    - tests/integration/test_batch_complete_control_totals.py
  modified:
    - packages/dataplat/src/dataplat/load/staging.py
    - tests/integration/test_reconciliation.py

key-decisions:
  - "raw_bronze reconciliation tests drive a real stage_ingest against testcontainers Postgres+MinIO with a real CsvSource, mirroring test_stage_ingest.py's own fixture shape, rather than calling promote_to_durable_bronze in isolation"
  - "Task 2's 'throwaway test dataset config, not customers/orders themselves' is satisfied as a locally-constructed DatasetConfig Python object (never the committed customers.yaml/orders.yaml), still declaring dataset=\"customers\" -- stage_ingest's _TARGET_COLUMNS_BY_DATASET lookup only has entries for customers/orders, so no third dataset name can reach the real staging path"
  - "Task 2's marker parsing calls the same parse_batch_complete_manifest function discovery._apply_batch_complete_marker_gate/the stage CLI command already use, applied directly to a real uploaded MinIO object, rather than re-running discover_files' own fuller pipeline"

requirements-completed: [VALID-05, VALID-06]

# Metrics
duration: 25min
completed: 2026-08-19
---

# Phase 09 Plan 07: Raw-to-Bronze Reconciliation + Control-Total Comparison Summary

**`StagingLoader.promote_to_durable_bronze` now writes a quarantine-aware `raw_bronze` reconciliation row on every call, and a real `_BATCH_COMPLETE` control total is compared against the actually-staged row count end-to-end through `stage_ingest`, never blocking on a mismatch.**

## Performance

- **Duration:** ~25 min
- **Tasks:** 2
- **Files modified:** 3 (1 source, 2 tests — 1 new)

## Accomplishments

- `promote_to_durable_bronze` resolves `dataset_id` and calls `ctx.metadata.record_reconciliation(hop="raw_bronze", ...)` in the same transaction as its own `INSERT INTO staging.<dataset>` / `DROP TABLE` — D-21's placement requirement.
- `input_count`/`output_count`/`rejected_count` come straight from `StagingResult` (D-22's quarantine-aware formula: `rows_read - (rows_parsed + rows_rejected)` is `0` by construction).
- `ctx.run.batch_expected_row_count`/`batch_expected_checksum` (plan 09-03's plumbing) thread straight through to `record_reconciliation`'s `expected_row_count`/`expected_checksum`; `control_total_discrepancy` is computed in SQL only when a control total is present.
- Proven live (real testcontainers Postgres + MinIO, real `CsvSource`, real `stage_ingest`) at two levels: `record_reconciliation`-level (4 tests in `test_reconciliation.py`) and full end-to-end from a genuinely uploaded, genuinely parsed `_BATCH_COMPLETE` marker object (3 tests in the new `test_batch_complete_control_totals.py`).
- A mismatched control total (Test 4 / Test 2 respectively) records a non-zero `control_total_discrepancy` and the run still reaches `STAGED` — D-22's "record and continue" rule proven directly, not just asserted by design.

## Task Commits

Each task was committed atomically (Task 1 as a genuine TDD RED→GREEN pair):

1. **Task 1: Wire raw->bronze reconciliation + control-total comparison**
   - `b28b0f2` (test) — 4 failing `raw_bronze` tests added to `test_reconciliation.py`, confirmed RED against pre-change `staging.py`
   - `efa6001` (feat) — `promote_to_durable_bronze` wired to `record_reconciliation`; confirmed GREEN
2. **Task 2: Full VALID-06 control-total integration test**
   - `b0f7526` (test) — new `tests/integration/test_batch_complete_control_totals.py`, 3 tests proving VALID-06 end-to-end, plus a trivial ruff line-length fix carried over from Task 1's file

## Files Created/Modified

- `packages/dataplat/src/dataplat/load/staging.py` — `promote_to_durable_bronze` now resolves `dataset_id` and calls `record_reconciliation(hop="raw_bronze", ...)` between its `INSERT` and `DROP TABLE`
- `tests/integration/test_reconciliation.py` — added a `raw_bronze` hop test section (4 tests) driving real `stage_ingest`, plus extended `_RECONCILIATION_COLUMNS` with `expected_row_count`/`control_total_discrepancy`
- `tests/integration/test_batch_complete_control_totals.py` (new) — 3 tests proving VALID-06 end-to-end from a real, uploaded, genuinely-parsed `_BATCH_COMPLETE` manifest through `stage_ingest`

## Decisions Made

- **raw_bronze tests drive `stage_ingest`, not `promote_to_durable_bronze` in isolation.** Matches the plan's own `<behavior>` framing ("a clean staging pass ... writes one `meta.reconciliation_results` row") and mirrors `test_stage_ingest.py`'s established real-`CsvSource`-over-real-MinIO fixture shape, rather than inventing an isolated-method-call test style this test suite doesn't otherwise use.
- **Task 2's "throwaway test dataset" is a locally-constructed `DatasetConfig` object, still named `"customers"`.** `stage_ingest`'s `_TARGET_COLUMNS_BY_DATASET` lookup (`dataplat/pipeline/run.py`) only has entries for `"customers"`/`"orders"` — a third literal dataset name raises `DataPlatformError` before staging ever begins. The plan's own intent ("not customers/orders themselves") is satisfied by never touching the real, committed `customers.yaml`/`orders.yaml` files; the test's `DatasetConfig` is a fresh Python object with its own `meta.datasets` row (`get_or_create_dataset("customers")` still resolves the shared "customers" dataset_id, since that's the only identity `record_reconciliation` itself ever resolves via `ctx.config.dataset`).
- **Task 2 parses the marker via `parse_batch_complete_manifest` directly, not via `discover_files`.** The plan's action text says "run `stage_ingest`", not "run `discover_files` then `stage_ingest`" — `discover_files`' own marker-parsing path is already covered by plan 09-03's unit tests (`tests/unit/validate/test_batch_complete_marker.py`). This test proves the SAME parse function, applied to a genuinely uploaded MinIO object, threaded into a real `RunContext`, run through a real `stage_ingest` — the layer this plan actually changes.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed `dataset_id` mismatch in raw_bronze test helper**
- **Found during:** Task 1 (GREEN verification)
- **Issue:** `_seed_and_build_raw_bronze_ctx` originally resolved `dataset_id` from a per-test synthetic dataset name (`raw_bronze_recon_{key_suffix}`) for the test's own read-back assertions, but `promote_to_durable_bronze` itself resolves `dataset_id` via `ctx.metadata.get_or_create_dataset(ctx.config.dataset)` — `ctx.config.dataset` is always the fixed literal `"customers"` (`_make_customers_config()`). The test was querying `meta.reconciliation_results` under the wrong `dataset_id` and asserting `0 == 1` even though the correct row existed.
- **Fix:** All 4 raw_bronze tests now resolve `dataset_id` via `get_or_create_dataset(ctx.config.dataset)`, matching the production code path exactly.
- **Files modified:** `tests/integration/test_reconciliation.py`
- **Verification:** All 4 raw_bronze tests pass; RED/GREEN re-confirmed against a genuine `staging.py` revert/reapply cycle.
- **Committed in:** `b28b0f2` (test commit, fixed before the RED/GREEN cycle was finalized — no separate follow-up commit needed since the bug was caught before committing)

**2. [Rule 1 - Bug] Added a missing `validated` bucket fixture for both new test files**
- **Found during:** Task 1 (initial GREEN attempt)
- **Issue:** `stage_ingest`'s quality gate (`_apply_staging_quality_gate_and_persist`) unconditionally writes its validation report to `s3://validated/...`. Neither new test file created that bucket, so the very first `stage_ingest` call raised `StorageError`.
- **Fix:** Added a `_raw_bronze_validated_bucket`/`_validated_bucket` fixture to each file (mirrors `test_stage_ingest.py`'s own `_validated_bucket` fixture), wired as a dependency of the shared `env` fixture.
- **Files modified:** `tests/integration/test_reconciliation.py`, `tests/integration/test_batch_complete_control_totals.py`
- **Verification:** Both test files' full suites pass.
- **Committed in:** `b28b0f2`, `b0f7526`

**3. [Rule 1 - Bug] Fixed a `UniqueViolation` in `test_batch_complete_control_totals.py`'s config-version helper**
- **Found during:** Task 2 (initial run)
- **Issue:** `_insert_config_version` did a plain `INSERT` (copied from `test_stage_ingest.py`'s own per-test-fresh-dataset shape), but every test in this new file shares the SAME `dataset_id` (`"customers"`, forced by `_TARGET_COLUMNS_BY_DATASET`). The second and third test's `INSERT` collided with `uq_config_versions_dataset_hash`.
- **Fix:** Rewrote `_insert_config_version` as get-or-insert (SELECT the CURRENT version first), mirroring `test_reconciliation.py`'s own established helper of the same name.
- **Files modified:** `tests/integration/test_batch_complete_control_totals.py`
- **Verification:** All 3 tests pass together in one file run.
- **Committed in:** `b0f7526`

**4. [Rule 3 - Blocking] Discovered the shared venv's editable `dataplat` install pointed at the main repo tree, not this worktree**
- **Found during:** Task 1 (first GREEN attempt kept showing 0 reconciliation rows despite a correct implementation)
- **Issue:** `.venv/bin/python` (the shared, main-repo `.venv` on `PATH`) resolves `dataplat`/`csv-processor` via an editable install pointing at `/home/konutec/projects/airflow-platform/packages/...` — the MAIN repo's source tree, not this worktree's own copy. Every `pytest` invocation via that interpreter silently exercised the main repo's (unmodified) `staging.py`, never this worktree's edit, producing a false "still failing after the fix" signal.
- **Fix:** Used `uv run --frozen --group cluster pytest ...` from within the worktree for every test invocation from this point on — `uv run` resolves the workspace against the worktree's own `pyproject.toml`/`uv.lock` and builds a worktree-local `.venv`, correctly picking up this worktree's `staging.py`/test edits.
- **Verification:** Confirmed via `uv run python -c "import dataplat.load.staging as m; print(m.__file__)"` resolving to the worktree path; RED/GREEN cycle re-run and re-confirmed under the correct interpreter.
- **No files modified** — this was a local invocation-environment issue, not a code change.

---

**Total deviations:** 4 auto-fixed (3 test-code bugs, 1 blocking local-environment issue).
**Impact on plan:** All four were necessary to reach a genuinely verified GREEN state; none touched the plan's own scope or the production `staging.py` change beyond what the plan specified. No scope creep.

## Issues Encountered

- The shared, main-repo `.venv` on `PATH` masked this worktree's own source edits for several test runs (see Deviation 4 above) — resolved by always invoking tests via `uv run --frozen --group cluster pytest ...` from the worktree root, matching this repo's own `Makefile`'s `RUN_CLUSTER` convention.

## User Setup Required

None — no external service configuration required.

## Next Phase Readiness

- Raw→bronze reconciliation (D-21) is now wired; combined with the existing silver→gold hop (plan 09-02), two of the three reconciliation hops named in the phase's threat model/decision register are live. The remaining bronze→silver hop belongs to plan 09-08.
- VALID-05/VALID-06 are both proven: source-to-target reconciliation exists at every staged hop this plan owns, and a source-declared control total is compared against the loaded target with a persisted, non-blocking discrepancy record — never trusted as ground truth on its own (T-09-13's threat register entry, mitigated: the comparison direction is fixed server-side in SQL, a malicious manifest cannot make a genuine loss disappear).
- No blockers for plan 09-08.

---
*Phase: 09-etl-correctness-dedup-incremental-backfill-recovery*
*Completed: 2026-08-19*

## Self-Check: PASSED

- FOUND: `packages/dataplat/src/dataplat/load/staging.py`
- FOUND: `tests/integration/test_reconciliation.py`
- FOUND: `tests/integration/test_batch_complete_control_totals.py`
- FOUND commit: `b28b0f2` (test)
- FOUND commit: `efa6001` (feat)
- FOUND commit: `b0f7526` (test)
