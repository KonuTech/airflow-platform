---
phase: 08-validation-quarantine-metadata-control-plane-completion
plan: 08
subsystem: database
tags: [postgresql, psycopg, referential-integrity, quarantine, barrier-stage, orders]

# Dependency graph
requires:
  - phase: 08-validation-quarantine-metadata-control-plane-completion
    provides: "orders dataset config + OrdersMergePublisher (08-04/08-05), first concrete BarrierStage precedent (08-07's RejectionRateCircuitBreaker)"
provides:
  - "ReferentialIntegrityBarrier -- the second concrete BarrierStage, anti-joining a run's staged customer_id values against normalized.customers"
  - "configs/datasets/orders.yaml's quality: block with its one REFERENTIAL rule (D-16)"
  - "VALIDATION_RULE_REGISTRY['REFERENTIAL'] entry"
affects: [08-09, 08-10, 08-11]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "BarrierStage anti-join for referential integrity: LEFT JOIN staging -> target table, WHERE target.column IS NULL, identifiers-only interpolation (mirrors merge_orders.py's T-04-01 precedent)"

key-files:
  created:
    - packages/dataplat/src/dataplat/validate/referential.py
    - tests/integration/test_referential_integrity.py
  modified:
    - packages/dataplat/src/dataplat/validate/registry.py
    - configs/datasets/orders.yaml

key-decisions:
  - "ReferentialIntegrityBarrier's SELECT list is deliberately hardcoded to orders' own customer_id/order_id columns (single-dataset, matching OrdersMergePublisher's own precedent) even though staging_table/target_table/target_column/staging_column stay config-driven for the JOIN condition."
  - "Row queried via a dict_row cursor (ctx.db's pool has no configured row_factory, default is tuple_row) so the plan's literal row['customer_id']-style access works without changing the pool's global default."

patterns-established:
  - "Second concrete BarrierStage: mirrors RejectionRateCircuitBreaker's shape (apply(ctx) -> StageResult, no chunk param, placeholder empty RecordChunk) but reads live target-table state through a NEW ctx.db.connection() instead of constructor-supplied totals."

requirements-completed: [VALID-07]

duration: 25min
completed: 2026-08-17
---

# Phase 8 Plan 08: ReferentialIntegrityBarrier Summary

**`ReferentialIntegrityBarrier` (the platform's second concrete `BarrierStage`) anti-joins staged `orders.customer_id` values against real `normalized.customers` rows, quarantining only the orphan row while every other row in the same run still publishes -- proven live against the exact race condition (a not-yet-loaded customer) that would otherwise become a false-alarm whole-run failure.**

## Performance

- **Duration:** ~25 min
- **Completed:** 2026-08-17T10:46:10+02:00
- **Tasks:** 2/2
- **Files modified:** 4 (2 created, 2 modified)

## Accomplishments
- `ReferentialIntegrityBarrier.apply(ctx)` runs a single parameterized-identifier anti-join query (`LEFT JOIN normalized.customers ... WHERE t.customer_id IS NULL`) against a run's real staged table, classifying every unmatched row `REFERENTIAL_ORPHAN` and leaving every matched row untouched.
- `configs/datasets/orders.yaml` gained its D-16 `quality:` block (`orders_customer_id_referential`, `rule_type: REFERENTIAL`, `strategy: QUARANTINE_RECORD`) -- validated clean against `DatasetConfig.model_validate`.
- `VALIDATION_RULE_REGISTRY["REFERENTIAL"]` now resolves to `ReferentialIntegrityBarrier`, alongside the sibling-added `CIRCUIT_BREAKER`/`QUALITY_UNIQUENESS` entries from 08-07 -- verified the merged registry contains all six entries with no collisions.
- Two integration tests against real testcontainers PostgreSQL (migrated to head) prove both required behaviors live: (1) a 3-row mix (2 matching + 1 orphan) yields exactly one `REFERENTIAL_ORPHAN` and a `QUARANTINE` finding with correct `evaluated_count`/`failed_count`; (2) Pitfall 5's exact race (an order referencing a customer whose own batch legitimately hasn't landed yet) never raises and never reports `outcome="FAIL"`, only `QUARANTINE` -- followed by publishing the non-orphan row via `OrdersMergePublisher` to confirm it lands in `normalized.orders` unaffected by the excluded orphan.

## Task Commits

Each task was committed atomically:

1. **Task 1: ReferentialIntegrityBarrier + orders.yaml quality: REFERENTIAL rule** - `512d76d` (feat)
2. **Task 2: Integration test -- orphan quarantine + non-orphan still publishes + the race scenario** - `8aba30f` (test)

**Plan metadata:** (this commit)

## Files Created/Modified
- `packages/dataplat/src/dataplat/validate/referential.py` - `ReferentialIntegrityBarrier(BarrierStage)`: anti-join query, orphan-to-`RejectedRecord` mapping, PASS/QUARANTINE `ValidationResult`
- `packages/dataplat/src/dataplat/validate/registry.py` - adds `"REFERENTIAL": ReferentialIntegrityBarrier` to `VALIDATION_RULE_REGISTRY` (registry already carried 08-07's `CIRCUIT_BREAKER`/`QUALITY_UNIQUENESS` entries; both preserved)
- `configs/datasets/orders.yaml` - new `quality:` block with the one `REFERENTIAL` rule (D-16)
- `tests/integration/test_referential_integrity.py` - two `@pytest.mark.integration` tests proving orphan classification and the Pitfall 5 race scenario, both against real testcontainers PostgreSQL

## Decisions Made
- `ReferentialIntegrityBarrier`'s anti-join SELECT list names `customer_id`/`order_id` literally (not fully column-generic) -- matches `OrdersMergePublisher`'s own documented "deliberately single-dataset" precedent; only the JOIN condition's column names stay config-driven. Documented in the module docstring so a future generic-barrier refactor knows this was deliberate, not an oversight.
- Read the anti-join/count queries through `conn.cursor(row_factory=dict_row)` rather than changing `create_pool`'s pool-wide default, since `ctx.db` (the shared `ConnectionPool`) has no configured `row_factory` anywhere else in the codebase and changing it globally would be out of this plan's scope.

## Deviations from Plan

None - plan executed exactly as written. The plan's literal SQL/behavior spec (anti-join shape, `RejectedRecord`/`ValidationResult` field mapping, both integration test scenarios) was implemented as given; no bugs, missing functionality, or blockers were found during execution.

## Issues Encountered
None. Sibling plan 08-07's concurrent additions to `registry.py` (`CIRCUIT_BREAKER`, `QUALITY_UNIQUENESS`) were already merged to `main` before this plan started (confirmed via `git log`), so no merge conflict occurred -- this plan's single new registry line landed cleanly alongside them.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- `ReferentialIntegrityBarrier` is ready for plan 08-11's pipeline wiring (constructing it with the real `staging_result.staging_table` after `StagingLoader.load()`, sequencing it relative to the publish transaction per Pattern 2/3's ordering requirements).
- `VALIDATION_RULE_REGISTRY` now has all three of this wave's rule-family entries (`CIRCUIT_BREAKER`, `QUALITY_UNIQUENESS`, `REFERENTIAL`) plus the four earlier ones -- no gaps expected for 08-11's config-driven dispatch.
- No blockers. Sibling plan 08-09 (also touching `registry.py`) can proceed independently; this plan's own registry edit is a single additive line with no shared-line conflict risk beyond the usual append-only convention already documented in the module docstring.

---
*Phase: 08-validation-quarantine-metadata-control-plane-completion*
*Completed: 2026-08-17*

## Self-Check: PASSED

- FOUND: packages/dataplat/src/dataplat/validate/referential.py
- FOUND: tests/integration/test_referential_integrity.py
- FOUND: .planning/phases/08-validation-quarantine-metadata-control-plane-completion/08-08-SUMMARY.md
- FOUND: 512d76d (feat commit)
- FOUND: 8aba30f (test commit)
