---
phase: 08-validation-quarantine-metadata-control-plane-completion
plan: 09
subsystem: database
tags: [validation, postgresql, psycopg, barrier-stage, volume-anomaly]

# Dependency graph
requires:
  - phase: 08-03
    provides: "meta.validation_results DDL (migration 0014) and MetadataRepository.record_validation_results()"
  - phase: 08-04
    provides: "ValidationResult/StageResult models and BarrierStage protocol shape"
provides:
  - "VolumeAnomalyBarrier -- a third concrete BarrierStage flagging a run whose row count is >10x its dataset's historical average"
  - "VALIDATION_RULE_REGISTRY['VOLUME'] entry resolving a dataset config's rule_type key to VolumeAnomalyBarrier"
affects: [08-11-pipeline-wiring, phase-9-historical-trend-anomaly-detection]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "BarrierStage with an internal testing seam (ctx_db_query) letting unit tests exercise SQL-comparison arithmetic without a live PostgreSQL connection, while real callers always issue the real query against ctx.db"
    - "A ValidationResult's own evaluated_count doubling as the metric a LATER run's own historical-average query will read -- no separate row-count write path"

key-files:
  created:
    - packages/dataplat/src/dataplat/validate/volume_anomaly.py
    - tests/unit/validate/test_volume_anomaly.py
    - tests/integration/test_volume_anomaly.py
  modified:
    - packages/dataplat/src/dataplat/validate/registry.py

key-decisions:
  - "VolumeAnomalyBarrier accepts an optional ctx_db_query testing seam so unit tests can inject (historical_average, prior_run_count) directly, keeping the real per-run SQL query the only code path a live caller ever exercises"
  - "Strategy-to-outcome mapping is a small local dict (QUARANTINE_FILE/RECORD -> QUARANTINE, FAIL_FILE -> FAIL, WARN_AND_CONTINUE -> PASS_WITH_WARNING), matching circuit_breaker.py/referential.py's own precedent"
  - "Cold start threshold is <2 prior SUCCEEDED VOLUME rows -- a structural PASS with observed={'historical_average': None, 'prior_run_count': N}, never a false positive"

patterns-established:
  - "Pattern: a barrier stage's own findings output IS the persisted history a later run's comparison reads -- self-referential by design, no separate metric-write path"

requirements-completed: [VALID-09]

# Metrics
duration: 10min
completed: 2026-08-17
---

# Phase 8 Plan 9: VolumeAnomalyBarrier Summary

**A third concrete `BarrierStage` (`VolumeAnomalyBarrier`) flags a run whose row count exceeds 10x its dataset's persisted historical average via one plain SQL comparison, with a structural cold-start guard for datasets with fewer than 2 prior successful runs — no forecasting, no ML.**

## Performance

- **Duration:** ~10 min
- **Started:** 2026-08-17T08:50:16Z
- **Completed:** 2026-08-17T09:00:00Z
- **Tasks:** 2
- **Files modified:** 4 (3 created, 1 modified)

## Accomplishments
- `VolumeAnomalyBarrier` queries `avg(evaluated_count)`/`count(*)` over prior `SUCCEEDED` `VOLUME` `meta.validation_results` rows for a dataset (joined through `meta.ingestion_runs`), bound via a `%s` placeholder
- Cold start (0 or 1 prior VOLUME rows) is a structural `PASS` — never a false positive, regardless of `current_row_count`
- An anomalous run (`current_row_count > historical_average * multiplier`) is reported through the same strategy-to-outcome mapping convention `circuit_breaker.py`/`referential.py` already established
- Registered `"VOLUME": VolumeAnomalyBarrier` in `VALIDATION_RULE_REGISTRY`, completing the phase's three-plan (08-07/08-08/08-09) registry build-out
- 8 unit tests (anomaly flag, all 3 strategy-outcome mappings, within-bounds, at-threshold-boundary, 0-prior, 1-prior, placeholder-guard) + 3 integration tests against real, migrated PostgreSQL (anomaly, within-bounds, cold-start against a real empty query result) — 11/11 passing

## Task Commits

Each task was committed atomically (TDD: RED then GREEN):

1. **Task 1: VolumeAnomalyBarrier + registry entry**
   - `3869797` (test) — failing unit test, `dataplat.validate.volume_anomaly` did not exist yet
   - `1001865` (feat) — `VolumeAnomalyBarrier` implementation + `VALIDATION_RULE_REGISTRY["VOLUME"]` entry; all 8 unit tests green
2. **Task 2: Integration test — real persisted history drives the comparison** - `49fcc9a` (test)

**Plan metadata:** (this commit, docs: complete plan)

## Files Created/Modified
- `packages/dataplat/src/dataplat/validate/volume_anomaly.py` — `VolumeAnomalyBarrier(BarrierStage)`: the SQL comparison, cold-start guard, and strategy-to-outcome mapping
- `packages/dataplat/src/dataplat/validate/registry.py` — added `"VOLUME": VolumeAnomalyBarrier` entry
- `tests/unit/validate/test_volume_anomaly.py` — 8 tests exercising the arithmetic via the `ctx_db_query` testing seam
- `tests/integration/test_volume_anomaly.py` — 3 tests seeding real `meta.ingestion_runs`/`meta.validation_results` rows and driving the barrier's own real SQL query through a real `ConnectionPool`

## Decisions Made
- `ctx_db_query` testing seam: unit tests inject `(historical_average, prior_run_count)` directly rather than mocking `ctx.db`/psycopg cursor plumbing, keeping the real query path (`ctx.db.connection()` + `_HISTORICAL_AVERAGE_SQL`) as the ONLY path any real caller (plan 08-11's future wiring) ever exercises
- Integration test's `_seed_succeeded_run` creates one `config_version` per dataset and reuses it across multiple seeded runs — `uq_config_versions_current_per_dataset` (migration 0001) allows at most one CURRENT (`valid_to IS NULL`) config_version per dataset, and this test's 3-runs-per-dataset shape would otherwise violate it. `config_hash` is derived from `key_suffix` for the same reason (`uq_config_versions_dataset_hash`).

## Deviations from Plan

None — plan executed exactly as written. The `ctx_db_query` testing seam and strategy-to-outcome mapping were both explicitly specified in the plan's own `<action>` text, not an addition.

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed duplicate `config_hash`/current-config_version constraint violations in the new integration test**
- **Found during:** Task 2, first `pytest -m integration` run
- **Issue:** `_insert_config_version` (copied from `test_referential_integrity.py`'s single-run-per-dataset precedent) used a fixed `config_hash` literal and was called once per seeded run — this file seeds 3 runs for the SAME dataset, so the second call collided with `uq_config_versions_dataset_hash`, and even after making the hash unique, the third call collided with `uq_config_versions_current_per_dataset` (only one CURRENT config_version allowed per dataset).
- **Fix:** `_insert_config_version` now derives `config_hash` from `key_suffix`; `_seed_succeeded_run` now accepts a pre-created `config_version_id` instead of creating its own, so each test creates exactly one config_version per dataset and reuses it across all seeded runs.
- **Files modified:** `tests/integration/test_volume_anomaly.py`
- **Verification:** `pytest tests/integration/test_volume_anomaly.py -x -m integration` — 3/3 passing
- **Committed in:** `49fcc9a` (Task 2 commit — fixed before the single Task 2 commit was made, so no separate fix commit was needed)

---

**Total deviations:** 1 auto-fixed (1 bug, discovered and fixed entirely within Task 2's own test-authoring work before that task's single commit)
**Impact on plan:** No scope creep — this was a test-fixture bug in code this plan itself was writing, fixed before commit, not a change to the plan's design.

## Issues Encountered
None beyond the auto-fixed issue above.

## User Setup Required

None — no external service configuration required.

## Next Phase Readiness

- `VOLUME` is now a complete `VALIDATION_RULE_REGISTRY` entry alongside `CIRCUIT_BREAKER` (08-07) and `REFERENTIAL` (08-08) — plan 08-11's pipeline wiring can construct all three from dataset config without further registry changes
- `VolumeAnomalyBarrier`'s own `findings` output already carries the `evaluated_count=current_row_count` shape a later run's historical-average query needs — once 08-11 wires `record_validation_results()` to persist this barrier's findings inside the run's own transaction, the self-referential history chain (this run's output = next run's input) is live with zero additional code
- No blockers. Phase 9's VALID-05/06 historical-trend/forecasting work builds on top of this plan's persisted `VOLUME` rows without needing to touch this barrier's own comparison logic

---
*Phase: 08-validation-quarantine-metadata-control-plane-completion*
*Completed: 2026-08-17*

## Self-Check: PASSED

- FOUND: packages/dataplat/src/dataplat/validate/volume_anomaly.py
- FOUND: tests/unit/validate/test_volume_anomaly.py
- FOUND: tests/integration/test_volume_anomaly.py
- FOUND: commit 3869797 (test, RED)
- FOUND: commit 1001865 (feat, GREEN)
- FOUND: commit 49fcc9a (test, integration)
- FOUND: `"VOLUME": VolumeAnomalyBarrier` entry in registry.py
