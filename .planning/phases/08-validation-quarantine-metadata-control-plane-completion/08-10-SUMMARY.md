---
phase: 08-validation-quarantine-metadata-control-plane-completion
plan: 10
subsystem: data-quality
tags: [validation, strategy-pattern, staging-loader, quality-rules, csv-processor]

# Dependency graph
requires:
  - phase: 08 (plans 08-04, 08-07)
    provides: CompletenessRule/ValidityRangeRule/PatternRule/UniquenessRule (VALID-02), VALIDATION_RULE_REGISTRY, QualityThresholdExceeded
provides:
  - StrategyDispatchStage -- generic D-07 per-rule-type strategy-outcome wrapper, reusable by every current and future StreamingStage rule
  - StagingLoader._build_stages wired to construct and dispatch every configured quality.rules entry through StrategyDispatchStage
  - Real, config-addressable data-quality enforcement during staging -- a dataset's quality:// YAML block now has genuine runtime effect
affects: [08-11 (customers.yaml real quality: block, run-level circuit breaker wiring, publish-transaction barrier stages)]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Strategy-dispatch decorator: one generic StreamingStage wrapper (StrategyDispatchStage) turns a stored-but-inert `strategy` string into real per-rule-type behavior, instead of every rule class re-implementing its own branching"
    - "_build_stages split into two smaller methods (_build_quality_stages/_build_one_quality_stage) purely to keep cyclomatic complexity bounded (ruff C901/PLR0912)"

key-files:
  created:
    - packages/dataplat/src/dataplat/validate/strategy_dispatch.py
    - tests/unit/validate/test_strategy_dispatch.py
    - tests/integration/test_staging_quality_rules.py
  modified:
    - packages/dataplat/src/dataplat/load/staging.py

key-decisions:
  - "REJECT_RECORD and QUARANTINE_RECORD produce byte-identical StageResult passthroughs in this phase -- no 'quarantine vs hard-reject' distinction exists below meta.rejected_records itself yet"
  - "FAIL_FILE and QUARANTINE_FILE deliberately produce the identical concrete outcome (raise QualityThresholdExceeded before publish) -- this phase's architecture has no per-file skip-and-continue loop inside run_ingest (D-01: one file -> one run)"
  - "A STRUCTURAL-typed quality.rules entry is a documented no-op in _build_stages -- RaggedRowGuard() is already unconditionally first per D-08, never duplicated"
  - "REFERENTIAL/CIRCUIT_BREAKER/VOLUME rule_type entries are silently skipped by _build_stages -- they are BarrierStages wired into the publish transaction by plan 08-11, never into this streaming stage list"

patterns-established:
  - "Strategy-dispatch decorator pattern for StreamingStage rules -- any future quality rule type gets D-07 strategy behavior for free by being wrapped in StrategyDispatchStage, no per-rule branching logic needed"

requirements-completed: [VALID-01, VALID-02, VALID-03]

duration: 25min
completed: 2026-08-17
---

# Phase 8 Plan 10: Strategy Dispatch & Quality Rule Wiring Summary

**StrategyDispatchStage turns D-07's 5 bad-record strategies into real per-rule outcomes, and StagingLoader now constructs every configured quality.rules entry through it during a real staged load.**

## Performance

- **Duration:** ~25 min
- **Tasks:** 3
- **Files modified:** 4 (1 modified, 3 created)

## Accomplishments
- `StrategyDispatchStage` -- a single, reusable `StreamingStage` decorator wrapping any other `StreamingStage` rule, dispatching D-07's 5 strategy values (`REJECT_RECORD`/`QUARANTINE_RECORD`/`WARN_AND_CONTINUE`/`FAIL_FILE`/`QUARANTINE_FILE`) into genuinely distinct outcomes, not just stored labels
- `StagingLoader._build_stages` now constructs every streaming-scoped `ctx.config.quality.rules` entry (`QUALITY_COMPLETENESS`/`UNIQUENESS`/`VALIDITY_RANGE`/`PATTERN`) via `VALIDATION_RULE_REGISTRY`, resolves each rule's column by name against `target_columns`, and wraps it in `StrategyDispatchStage` -- appended after the existing normalizer stages so quality rules only ever see fully-normalized values
- A dataset with no `quality:` block behaves byte-for-byte as before this plan (proven at both the `_build_stages` stage-list level and a real staged load)
- Real integration proof: a row failing a configured `REJECT_RECORD` completeness rule never lands in the staging table; the same row under `WARN_AND_CONTINUE` genuinely survives to the staging table -- proving the wiring, not just the bare rule

## Task Commits

Each task was committed atomically:

1. **Task 1: StrategyDispatchStage -- the generic D-07 per-rule-type outcome wrapper** - `f27c73c` (feat)
2. **Task 2: _build_stages dispatches ctx.config.quality's streaming rules through StrategyDispatchStage** - `d6b4b05` (feat)
3. **Task 3: Integration test -- a configured rule genuinely rejects a bad row during a real staged load** - `3e27975` (test)

## Files Created/Modified
- `packages/dataplat/src/dataplat/validate/strategy_dispatch.py` - `StrategyDispatchStage`: the generic strategy-outcome wrapper
- `packages/dataplat/src/dataplat/load/staging.py` - `_build_stages` extended with a fourth section (`_build_quality_stages`/`_build_one_quality_stage`) that dispatches `ctx.config.quality.rules` through the registry, wrapped in `StrategyDispatchStage`
- `tests/unit/validate/test_strategy_dispatch.py` - VALID-03's own named proof: one test per D-07 strategy value, plus zero-violation and construction-time validation coverage (11 tests)
- `tests/integration/test_staging_quality_rules.py` - real testcontainers PostgreSQL proof: `_build_stages` stage-list shape (quality=None regression, one rule wrapped correctly, barrier-scoped rule_types skipped) and a real staged load's REJECT_RECORD/WARN_AND_CONTINUE outcomes (8 tests)

## Decisions Made
- `REJECT_RECORD`/`QUARANTINE_RECORD` and `FAIL_FILE`/`QUARANTINE_FILE` are each pairwise identical in this phase (see key-decisions above) -- both pairs are documented, deliberate simplifications directly in `StrategyDispatchStage`'s own module docstring, not oversights.
- `_build_stages` was split into `_build_quality_stages`/`_build_one_quality_stage` purely to keep cyclomatic complexity under ruff's C901/PLR0912 thresholds -- no behavior change, pure extraction.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `StrategyDispatchStage.name` as a read-only `@property` was an invalid Protocol override**
- **Found during:** Task 2 (running mypy after wiring `staging.py`)
- **Issue:** `StreamingStage.name` is a plain writeable `str` attribute in the protocol; overriding it with a read-only `@property` in the subclass is an invalid override (`mypy` `[override]`).
- **Fix:** Replaced the `@property` with a plain instance attribute computed once in `__init__` (`self.name = f"strategy_dispatch[{inner.name}]"`), matching every other rule class's own class-attribute convention.
- **Files modified:** `packages/dataplat/src/dataplat/validate/strategy_dispatch.py`
- **Verification:** `mypy` clean; all 11 unit tests still pass unchanged (the computed value is identical).
- **Committed in:** `d6b4b05` (part of Task 2 commit, since the fix was discovered while verifying Task 2's wiring)

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Pure type-correctness fix, no behavior change. No scope creep.

## Issues Encountered
None beyond the auto-fixed mypy issue above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Plan 08-11 can now wire `customers.yaml`'s real `quality:` block and prove the full FAIL/QUARANTINE run-level outcome against a real dataset -- the streaming half of D-07's strategy contract (this plan) is complete and proven; the barrier-stage half (`REFERENTIAL`/`CIRCUIT_BREAKER`/`VOLUME`, publish-transaction wiring) remains 08-11's job, as this plan's own `_STREAMING_RULE_TYPES` gate deliberately documents.
- No blockers identified.

---
*Phase: 08-validation-quarantine-metadata-control-plane-completion*
*Completed: 2026-08-17*

## Self-Check: PASSED

All claimed files (`strategy_dispatch.py`, `test_strategy_dispatch.py`, `test_staging_quality_rules.py`, `staging.py`, this SUMMARY.md) confirmed present on disk. All three task commits (`f27c73c`, `d6b4b05`, `3e27975`) confirmed present in `git log`.
