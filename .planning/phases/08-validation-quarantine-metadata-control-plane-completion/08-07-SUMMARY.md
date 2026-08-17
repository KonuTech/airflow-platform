---
phase: 08-validation-quarantine-metadata-control-plane-completion
plan: 07
subsystem: data-quality
tags: [validation, barrier-stage, streaming-stage, circuit-breaker, uniqueness, dataplat]

# Dependency graph
requires:
  - phase: 08-04
    provides: VALIDATION_RULE_REGISTRY (dataplat/validate/registry.py) — the append-only registry every rule plan adds its own key to
provides:
  - RejectionRateCircuitBreaker — this codebase's first concrete BarrierStage, D-10's run-level rejection-rate threshold check, raising QualityThresholdExceeded on breach
  - UniquenessRule — VALID-02's fifth rule family, within-chunk business-key uniqueness detection
  - Two new VALIDATION_RULE_REGISTRY entries ("CIRCUIT_BREAKER", "QUALITY_UNIQUENESS")
affects: [08-10, 08-11]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "First concrete BarrierStage implementation — constructed with already-known run totals (total_rows_read/total_rows_rejected) rather than deriving them from PipelineContext, since PipelineContext has no row-count field"
    - "Within-chunk-only uniqueness scope — a rule's local 'seen' set is rebuilt fresh on every apply() call, never persisted across chunks; documented as a deliberate scope limit, not a bug"

key-files:
  created:
    - packages/dataplat/src/dataplat/validate/circuit_breaker.py
    - packages/dataplat/src/dataplat/validate/uniqueness.py
    - tests/unit/validate/test_circuit_breaker.py
    - tests/unit/validate/test_uniqueness.py
  modified:
    - packages/dataplat/src/dataplat/validate/registry.py

key-decisions:
  - "RejectionRateCircuitBreaker's constructor accepts total_rows_read/total_rows_rejected directly rather than reading them from ctx, per the plan's explicit interface note (BarrierStage.apply(ctx) has no row-count field) — a fresh instance is constructed per run by the future 08-11 caller, after StagingLoader.load() has already returned its totals"
  - "UniquenessRule's docstring explicitly cites deduplication.strategy: business_key_latest (wired since Phase 4) as the actual whole-run uniqueness enforcement mechanism, framing this rule as a pre-publish diagnostic surface only"

patterns-established:
  - "BarrierStage totals-at-construction-time pattern: any future barrier needing run-wide aggregates (not yet known to PipelineContext) should follow RejectionRateCircuitBreaker's shape rather than trying to widen PipelineContext"

requirements-completed: [VALID-02, VALID-03]

# Metrics
duration: 25min
completed: 2026-08-17
---

# Phase 8 Plan 07: Circuit Breaker & Uniqueness Rule Summary

**RejectionRateCircuitBreaker (first concrete BarrierStage, D-10) and UniquenessRule (within-chunk StreamingStage, VALID-02) built and unit-tested in isolation, both registered in VALIDATION_RULE_REGISTRY, with no pipeline wiring yet.**

## Performance

- **Duration:** ~25 min
- **Started:** 2026-08-17T08:10:00Z (approx.)
- **Completed:** 2026-08-17T08:34:24Z
- **Tasks:** 2 completed
- **Files modified:** 5 (2 created source, 2 created test, 1 modified registry)

## Accomplishments

- `RejectionRateCircuitBreaker`: this codebase's first concrete `BarrierStage`. Raises `QualityThresholdExceeded` (with `observed_ratio`/`threshold`/`total_rows_read`/`total_rows_rejected` in `context`) when a run's aggregate rejected/total ratio exceeds its configured threshold; a zero-row run is a trivial PASS, never a `ZeroDivisionError`. This is the actual mechanism plan 08-11 will wire into the publish transaction to make D-11 ("nothing publishes on FAIL") real.
- `UniquenessRule`: VALID-02's fifth rule family. Detects a duplicate business-key value within a single chunk, rejects every occurrence after the first, never raises. Its docstring explicitly documents the within-chunk-only scope limit and points to `deduplication.strategy: business_key_latest` as the real whole-run enforcement mechanism.
- Both registered additively in `VALIDATION_RULE_REGISTRY` (`"CIRCUIT_BREAKER"`, `"QUALITY_UNIQUENESS"`) alongside the existing four entries — no existing keys touched, keeping the registry merge-friendly for sibling plans 08-08/08-09 landing immediately after.
- 9 new unit tests (4 circuit breaker, 5 uniqueness), all passing; full `tests/unit/validate/` suite (29 tests) green; `ruff check` and `mypy` clean on all touched files.

## Task Commits

Each task followed the TDD RED/GREEN split (test commit, then implementation commit):

1. **Task 1: RejectionRateCircuitBreaker** — `6f6aef6` (test), `5758bfd` (feat)
2. **Task 2: UniquenessRule** — `48b7dbf` (test), `60793d7` (feat), `6c63bbe` (fix — ruff E501 line-length)

**Plan metadata:** committed separately (see final commit below)

## Files Created/Modified

- `packages/dataplat/src/dataplat/validate/circuit_breaker.py` — `RejectionRateCircuitBreaker(BarrierStage)`, threshold arithmetic, `QualityThresholdExceeded` raise site
- `packages/dataplat/src/dataplat/validate/uniqueness.py` — `UniquenessRule(StreamingStage)`, within-chunk duplicate detection
- `packages/dataplat/src/dataplat/validate/registry.py` — added `"CIRCUIT_BREAKER"` and `"QUALITY_UNIQUENESS"` entries
- `tests/unit/validate/test_circuit_breaker.py` — 4 tests: breach raises with context, under-threshold PASS, at-threshold-exactly PASS, zero-rows-read never divides by zero
- `tests/unit/validate/test_uniqueness.py` — 5 tests: first-kept/second-rejected, all-distinct keeps all, triple-duplicate rejects two, cross-chunk scope proof, never-raises row accounting

## Decisions Made

- `RejectionRateCircuitBreaker.apply(ctx)` accepts and discards `ctx` (documented `del ctx` with rationale) rather than omitting the parameter, since it must still satisfy the `BarrierStage` Protocol's exact signature.
- Both barrier and streaming stage return a placeholder/kept-rows chunk respectively per the plan's explicit worked shape — no deviation from the action block's specified `StageResult` construction.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug/Lint] Shortened `UniquenessRule.apply` docstring line exceeding ruff's 100-char limit (E501/W505)**
- **Found during:** Task 2 lint verification (post-commit)
- **Issue:** The first docstring line of `apply()` was 101 characters, tripping `ruff check`
- **Fix:** Reworded without changing meaning ("...already occurred earlier in this chunk." → "...already occurred earlier this chunk.")
- **Files modified:** `packages/dataplat/src/dataplat/validate/uniqueness.py`
- **Verification:** `ruff check` and `mypy` both clean afterward; full `tests/unit/validate/` suite (29 tests) still green
- **Committed in:** `6c63bbe`

---

**Total deviations:** 1 auto-fixed (1 lint/style, Rule 1)
**Impact on plan:** Purely cosmetic; no scope creep, no behavior change.

## Issues Encountered

None.

## User Setup Required

None — no external service configuration required.

## Next Phase Readiness

- `RejectionRateCircuitBreaker` and `UniquenessRule` are both proven correct in isolation and ready for pure-plumbing wiring: plan 08-11 wires the circuit breaker into the publish transaction; plan 08-10 wires the uniqueness rule into `StagingLoader`.
- `VALIDATION_RULE_REGISTRY` now has 6 entries (`STRUCTURAL`, `QUALITY_COMPLETENESS`, `QUALITY_UNIQUENESS`, `QUALITY_VALIDITY_RANGE`, `QUALITY_PATTERN`, `CIRCUIT_BREAKER`), additive and merge-friendly for sibling plans 08-08 (referential integrity) and 08-09 (volume anomaly) landing next in the same wave.
- No blockers.

---
*Phase: 08-validation-quarantine-metadata-control-plane-completion*
*Completed: 2026-08-17*

## Self-Check: PASSED

All 6 created/modified files confirmed present on disk; all 6 commit hashes
(6f6aef6, 5758bfd, 48b7dbf, 60793d7, 6c63bbe, c639a27) confirmed present in
git log.
