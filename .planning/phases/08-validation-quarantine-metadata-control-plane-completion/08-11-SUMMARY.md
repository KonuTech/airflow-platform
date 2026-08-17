---
phase: 08-validation-quarantine-metadata-control-plane-completion
plan: 11
subsystem: database
tags: [postgresql, minio, psycopg, quality-rules, circuit-breaker, referential-integrity, backfill]

# Dependency graph
requires:
  - phase: 08-03
    provides: record_validation_results/record_rejected_records/resolve_rejected_records_for_batch on PostgresMetadataRepository
  - phase: 08-07
    provides: RejectionRateCircuitBreaker (D-10/D-11)
  - phase: 08-08
    provides: ReferentialIntegrityBarrier (D-16)
  - phase: 08-09
    provides: VolumeAnomalyBarrier (VALID-09)
  - phase: 08-10
    provides: StrategyDispatchStage + streaming quality-rule wiring in StagingLoader
provides:
  - run_ingest's publish transaction now runs every barrier stage (referential, circuit breaker, volume anomaly), persists validation/rejection rows on the SAME conn as the Publisher's own write, unconditionally calls resolve_rejected_records_for_batch (D-05), and writes a report.json artifact to MinIO's validated bucket before finalize_publication
  - Receipt.rows_quarantined and a genuine, non-hardcoded Receipt.report_uri
  - customers.yaml's real quality: block (completeness/pattern/uniqueness + circuit breaker) proving the full VALID-01..04 chain against the platform's one real cycling dataset
affects: [phase-09-cdc-scd, phase-10-dr-recovery]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "BarrierStage wiring order inside a publish transaction: referential (pre-publish, deletes orphans via the same conn) -> publisher.publish() -> circuit breaker -> volume anomaly -> record_validation_results -> record_rejected_records -> resolve_rejected_records_for_batch (unconditional) -> MinIO report.json write -> finalize_publication"
    - "A run-level helper split purely to stay under ruff PLR0915, taking the transaction's own conn as a parameter rather than opening a second connection"

key-files:
  created:
    - tests/integration/test_publish_transaction_wiring.py
  modified:
    - packages/dataplat/src/dataplat/pipeline/run.py
    - packages/dataplat/src/dataplat/load/staging.py
    - packages/dataplat/src/dataplat/models/receipt.py
    - packages/csv-processor/src/csv_processor/cli.py
    - configs/datasets/customers.yaml
    - configs/datasets/orders.yaml
    - tests/integration/test_run_ingest.py
    - tests/integration/test_lineage_view.py
    - tests/unit/test_run_ingest_trace.py
    - tests/unit/test_assignment_document.py
    - tests/unit/test_csv_processor_cli.py

key-decisions:
  - "ReferentialIntegrityBarrier's target_table is resolved from the quality rule's own params.target_table, raising ConfigurationError when absent -- orders.yaml (not in this plan's files_modified, but wired live for the first time by this plan) needed a proactive params.target_table addition (Rule 2) to keep working, since the plan's own two documented options were 'params, defaulting absent to a ConfigurationError' vs 'hardcode normalized.customers'; declaring it explicitly in orders.yaml keeps the barrier config-driven and forward-compatible instead of silently defaulting"
  - "customers.yaml's rejection_rate_threshold is 0.5, deliberately permissive -- this is the platform's one real cycling dataset, and a low threshold would make ordinary demo/E2E traffic noise trip the circuit breaker"

patterns-established:
  - "A BarrierStage's orphan-row DELETE always happens at the call site (run.py), using the SAME conn as the rest of the publish transaction -- never inside the barrier's own apply(), which only reads and reports"

requirements-completed: [VALID-01, VALID-02, VALID-03, VALID-04, VALID-07, VALID-09]

# Metrics
duration: ~50min
completed: 2026-08-17
---

# Phase 8 Plan 11: Wire Barrier Stages, D-05 Batch Resolution and MinIO Report into run_ingest Summary

**`run_ingest`'s publish transaction now runs referential/circuit-breaker/volume-anomaly barriers, persists validation/rejection rows, calls `resolve_rejected_records_for_batch` unconditionally, and writes a `report.json` artifact to MinIO's `validated` bucket -- all inside the same transaction, all before `finalize_publication` -- turning seven previously-isolated unit-tested classes into one real, atomic ingestion-run property.**

## Performance

- **Duration:** ~50 min
- **Completed:** 2026-08-17
- **Tasks:** 2 completed
- **Files modified:** 11 (2 tasks: 8 in Task 1, 2 in Task 2, `deferred-items.md` updated separately)

## Accomplishments

- `StagingLoader.load()`'s `StagingResult` now carries the actual `RejectedRecord` list, not just a count, so `run_ingest` can persist and report on the exact objects staging-time rules rejected.
- `run_ingest`'s publish transaction wires in `ReferentialIntegrityBarrier` (pre-publish, deleting orphans from staging on the same `conn`), `RejectionRateCircuitBreaker` and `VolumeAnomalyBarrier` (post-publish) -- a raised `QualityThresholdExceeded` rolls back the entire transaction, proving D-11 live.
- `resolve_rejected_records_for_batch` (D-05) is now called from exactly one production code path -- every successful `run_ingest` call, unconditionally, regardless of whether that dataset declares `quality:` at all -- making a real Airflow backfill's "resolve the batch it supersedes" guarantee reachable, not just unit-tested in isolation.
- A `report.json` artifact (VALID-04's MinIO-artifact half) is written to `s3://validated/<dataset>/<run_id>/report.json` for every successful run, and `Receipt.report_uri`/`finalize_publication`'s own `report_uri` argument both carry that real URI -- previously hardcoded `None` at both call sites.
- `customers.yaml` gained a real `quality:` block (completeness on `name`, pattern on `country`, uniqueness on `customer_id`, all `REJECT_RECORD`, plus a `rejection_rate_threshold: 0.5` circuit breaker) -- the ONE real cycling dataset now proves the full VALID-01/02/03/04 chain live, not just in fixtures.
- `Receipt` gained `rows_quarantined`, and every existing `Receipt(...)` construction site (production and test) was updated.

## Task Commits

Each task was committed atomically:

1. **Task 1: StagingResult carries rejected records; run_ingest wires barriers + persistence + D-05 batch resolution + MinIO report artifact + rollback** - `780f73a` (feat)
2. **Task 2: customers.yaml's real quality: block + FAIL-vs-QUARANTINE integration proof + D-05 backfill-resolution proof + MinIO report-artifact proof** - `768bf82` (test)

## Files Created/Modified

- `packages/dataplat/src/dataplat/pipeline/run.py` - `_find_quality_rule`, `_apply_referential_barrier`, `_apply_post_publish_barriers_and_persist` helpers; `run_ingest`'s publish transaction now runs every barrier stage, persists, resolves the batch, and writes the report artifact before `finalize_publication`
- `packages/dataplat/src/dataplat/load/staging.py` - `StagingResult.rejected_records: list[RejectedRecord]` (new field, `default_factory=list`); `load()` accumulates it per chunk
- `packages/dataplat/src/dataplat/models/receipt.py` - `Receipt.rows_quarantined: int` (new required field)
- `packages/csv-processor/src/csv_processor/cli.py` - `_failure_receipt` passes `rows_quarantined=0`
- `configs/datasets/customers.yaml` - real `quality:` block (D-09)
- `configs/datasets/orders.yaml` - `params.target_table: normalized.customers` added to the existing REFERENTIAL rule (Rule 2 fix, see Deviations)
- `tests/integration/test_publish_transaction_wiring.py` - new: Test A (circuit-breaker trip rolls back everything), Test B (quarantine under threshold SUCCEEDS), Test C (D-05 backfill-resolution proof through `run_ingest` itself), Test D (VALID-04 MinIO-artifact proof)
- `tests/integration/test_run_ingest.py` - `_validated_bucket` fixture added (create-if-absent), wired into `env`
- `tests/integration/test_lineage_view.py` - imports `_validated_bucket` too (reuses `test_run_ingest.py`'s `env` fixture chain)
- `tests/unit/test_run_ingest_trace.py` - `_FakeObjectStore.put_object`, `_FakeMetadataRepository.record_validation_results`/`record_rejected_records`/`resolve_rejected_records_for_batch` added
- `tests/unit/test_assignment_document.py`, `tests/unit/test_csv_processor_cli.py` - `Receipt(...)` construction sites updated with `rows_quarantined=0`

## Decisions Made

- `ReferentialIntegrityBarrier.target_table` is resolved via `rule.params.get("target_table")`, raising `ConfigurationError` when absent, rather than silently hardcoding `"normalized.customers"` -- keeps the barrier genuinely config-driven for a future second referential relationship, at the cost of requiring `orders.yaml` to declare it explicitly (done, see Deviations).
- `customers.yaml`'s three new quality rules all use `REJECT_RECORD` (never `FAIL_FILE`/`QUARANTINE_FILE`), matching T-08-26/08-10's threat model -- those two strategies stay proven only by unit coverage, never exercised against this real cycling dataset in this phase.
- The barrier/persistence/report logic was split into two module-level helper functions (`_apply_referential_barrier`, `_apply_post_publish_barriers_and_persist`) purely to keep `run_ingest` under ruff's `PLR0915` statement-count threshold -- no behavior change, both take the transaction's own `conn` as a parameter rather than opening a second connection.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] `orders.yaml` needed `params.target_table` for the newly-wired REFERENTIAL barrier**
- **Found during:** Task 1 (implementing `_apply_referential_barrier`)
- **Issue:** `orders.yaml`'s existing `REFERENTIAL` rule (from an earlier plan) declared no `params.target_table`. This plan is the first to actually wire `ReferentialIntegrityBarrier` into a live `run_ingest` call, and the barrier requires `target_table` to know what to anti-join against. Without a fix, every real `orders` ingestion run would fail with `ConfigurationError` the moment this plan merged.
- **Fix:** Added `params: {target_table: normalized.customers}` to `orders.yaml`'s existing `REFERENTIAL` rule entry.
- **Files modified:** `configs/datasets/orders.yaml`
- **Verification:** `DatasetConfig.model_validate` on the updated `orders.yaml` succeeds; `tests/integration/test_publish_orders.py`'s existing suite (unaffected, since it calls `OrdersMergePublisher.publish()` directly, not `run_ingest`) still passes.
- **Committed in:** `780f73a` (Task 1 commit)

**2. [Rule 1 - Bug] Pre-existing `test_run_ingest.py`/`test_lineage_view.py`/`test_run_ingest_trace.py` broke under the new unconditional MinIO write + metadata calls**
- **Found during:** Task 1, running the existing integration/unit suites after wiring `run_ingest`
- **Issue:** Every successful `run_ingest` call now unconditionally writes a `report.json` to the `validated` MinIO bucket and calls three new `MetadataRepository` methods. `test_run_ingest.py`'s `env` fixture never created a `validated` bucket (`NoSuchBucket` on every previously-passing test); `test_lineage_view.py` imports that same fixture chain and needed the same fix; `test_run_ingest_trace.py`'s hand-written `_FakeMetadataRepository`/`_FakeObjectStore` doubles lacked the three new methods and `put_object`, raising `AttributeError`.
- **Fix:** Added a `_validated_bucket` fixture (create-if-absent, mirroring `_scratch_bucket`'s own pattern) to `test_run_ingest.py`, wired into `env`; re-exported it from `test_lineage_view.py`'s existing fixture-import block; added the three metadata methods and `put_object` to `test_run_ingest_trace.py`'s fakes.
- **Files modified:** `tests/integration/test_run_ingest.py`, `tests/integration/test_lineage_view.py`, `tests/unit/test_run_ingest_trace.py`
- **Verification:** `pytest tests/integration/test_run_ingest.py tests/integration/test_lineage_view.py tests/unit/test_run_ingest_trace.py` all pass; full `pytest tests/integration -q` (110 tests) and `pytest tests/unit tests/regression -q` (480 tests) both green.
- **Committed in:** `780f73a` (Task 1 commit)

---

**Total deviations:** 2 auto-fixed (1 missing critical, 1 bug-fix cascade across 3 test files)
**Impact on plan:** Both were necessary for correctness and to keep the existing test suite genuinely green after this plan's own required, unconditional new behavior (D-05's resolve call, VALID-04's report write). No scope creep -- neither changes what this plan's own tasks deliver.

## Issues Encountered

- `tests/policy/test_gates_actually_fail.py::test_the_main_gate_does_not_lint_the_bad_samples` fails because `make lint` itself is red on `tests/integration/test_publish_orders.py:263` (a 103-char line from an earlier plan's commit, `8490926`) -- confirmed pre-existing via `git log`/`git status` (zero diff on that file from this session). Logged to `deferred-items.md`, not fixed (out of this plan's `files_modified` scope, Scope Boundary rule).
- `tests/policy/test_dag_line_budget.py::test_csv_ingest_customers_stays_under_150_lines` fails (162 lines) -- already a known, previously-logged `deferred-items.md` entry from plan 08-05, reconfirmed still open, not caused by this plan.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- VALID-01 through VALID-04, VALID-07 and VALID-09 are now genuine properties of a real ingestion run, not isolated unit-tested classes.
- D-05's backfill-resolution mechanism has exactly one production call site (`run_ingest`), proven live end-to-end (Test C).
- Two pre-existing, out-of-scope gate failures remain open (see Issues Encountered) -- neither blocks this plan's own success criteria, both tracked in `deferred-items.md` for a future gap-closure/cleanup plan.

---
*Phase: 08-validation-quarantine-metadata-control-plane-completion*
*Completed: 2026-08-17*

## Self-Check: PASSED

- FOUND: tests/integration/test_publish_transaction_wiring.py
- FOUND: packages/dataplat/src/dataplat/pipeline/run.py
- FOUND: commit 780f73a (Task 1)
- FOUND: commit 768bf82 (Task 2)
