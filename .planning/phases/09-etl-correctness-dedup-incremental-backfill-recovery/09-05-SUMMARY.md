---
phase: 09-etl-correctness-dedup-incremental-backfill-recovery
plan: 05
subsystem: testing
tags: [corpus-generator, fixtures, determinism, backfill, incremental]

# Dependency graph
requires:
  - phase: 01-repository-toolchain-ci-skeleton
    provides: "tools/corpus/generators.py's stream_for() and R1-R10 determinism discipline, docs/adr/0005 (fixture corpus generated from a seed)"
provides:
  - "generate_dated_series() + BackfillCorpusManifest — a pure, deterministic day-per-file CSV generator for a 2-year backfill corpus with a gap day, a schema-change boundary and a late/out-of-order event"
affects: [09-11, backfill-testing, fixture-generation]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Day-per-file generator distinct from tools/corpus's Fixture/Manifest declarative machinery — reuses stream_for() for R1 but is new generator code, not a manifest entry"
    - "BackfillCorpusManifest records exactly where each injected anomaly (gap/schema-change/late-event) lives, so downstream tests assert against known values instead of re-deriving them"

key-files:
  created:
    - tools/corpus/dated_series.py
    - tests/unit/test_dated_series.py
  modified: []

key-decisions:
  - "Built a new generator function rather than extending Fixture/Manifest, because the declarative model has no concept of 'N days of dated files with an injected boundary' (RESEARCH.md Open Question 2, confirmed by direct read of generators.py/manifest.py this session)"
  - "Hardcoded each dataset's column list (customer_id/name/country/birth_date/event_ts and order_id/customer_id/order_date/amount) rather than loading configs/datasets/*.yaml at runtime, keeping the generator a pure function with no filesystem/config-loader dependency, while still matching the real schema exactly"
  - "orders' customer_id values are drawn from a synthetic fixed pool, not cross-referenced against a matching customers series generated in the same call — this plan's behavior tests do not require referential integrity between the two datasets' generated corpora, and plan 09-11 owns making uploaded datasets mutually consistent if needed"

patterns-established:
  - "A pure generator function (files: dict[str, bytes], no I/O) paired with a frozen manifest dataclass recording anomaly locations is the shape for any future 'many dated files, one combined property set' fixture need"

requirements-completed: [INCR-06, QUAL-11]

# Metrics
duration: 25min
completed: 2026-08-19
---

# Phase 09 Plan 05: Dated Backfill Corpus Generator Summary

**`generate_dated_series()` produces a byte-deterministic, ~730-day CSV corpus per dataset combining a regular cadence, one missing-file gap, one schema-change boundary and one late/out-of-order event, with a `BackfillCorpusManifest` recording exactly where each lives.**

## Performance

- **Duration:** 25 min
- **Started:** 2026-08-19T14:09:00Z
- **Completed:** 2026-08-19T14:34:45Z
- **Tasks:** 1
- **Files modified:** 2 (both created)

## Accomplishments
- `tools/corpus/dated_series.py`: a new, purpose-built generator distinct from `tools/corpus`'s existing `Fixture`/`Manifest` declarative machinery, reusing `generators.stream_for` for R1 (per-file random streams) and following R2 (arithmetic-only randomness), R3/R4 (binary-mode encoding, explicit `\r\n` terminator) and R6 (no wall-clock calls)
- `BackfillCorpusManifest` frozen dataclass records `gap_day_index`, `schema_change_day_index`, `late_event_day_index`/`late_event_row_index` and every generated filename, so plan 09-11's live sweep can assert against known values
- 5 unit tests prove determinism, the gap/cadence combination, the schema-change header boundary, the late-event row's backdated date column, and a fail-loud rejection of an unknown dataset name
- Confirmed `tests/policy/test_generator_determinism_rules.py` already covers this new module for R2/R6 by directory walk (`GENERATOR_PACKAGE.rglob("*.py")`) — no separate wiring needed, contrary to the plan's action text assuming an extension would be required

## Task Commits

Each task was committed atomically:

1. **Task 1: generate_dated_series — deterministic, schema-change/gap/late-event corpus** - `13d0c13` (feat)

**Plan metadata:** (this commit, docs: complete plan)

## Files Created/Modified
- `tools/corpus/dated_series.py` - `generate_dated_series()` + `BackfillCorpusManifest`; pure function, no S3/MinIO/network I/O
- `tests/unit/test_dated_series.py` - 5 tests: determinism, gap/cadence, schema-change boundary, late event, unknown-dataset rejection

## Decisions Made
- Column lists for `customers`/`orders` are hardcoded in the generator (matching `configs/datasets/*.yaml` exactly) rather than loaded from YAML at generation time, keeping the module dependency-free and unit-testable in isolation
- `orders.customer_id` values are drawn from a synthetic fixed pool (`CUST-000001`..`CUST-000030`), not cross-referenced against a customers series generated in the same call — no behavior test in this plan requires cross-dataset referential integrity, and plan 09-11 is responsible for upload-time consistency if it needs that

## Deviations from Plan

None — plan executed exactly as written. One clarification worth recording: the plan's Task 1 action text described Test 5 (R6 compliance) as needing `tests/policy/test_generator_determinism_rules.py`'s scan "extended... to also scan `tools/corpus/dated_series.py`". On inspection, that test already walks the entire `tools/corpus/` package via `GENERATOR_PACKAGE.rglob("*.py")`, so the new module was automatically in scope with zero test-file changes — confirmed by running the policy suite unmodified, which passed (5/5) with `dated_series.py` present.

## Issues Encountered
None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

`generate_dated_series()` is ready for plan 09-11 to call for both `customers` and `orders`, generate their ~730-day corpora, and upload the resulting bytes to MinIO ahead of a live 2-year backfill sweep. The returned `BackfillCorpusManifest` gives 09-11 the exact gap day, schema-change boundary day and late-event day/row to assert against without re-deriving them from the generated bytes. No blockers.

---
*Phase: 09-etl-correctness-dedup-incremental-backfill-recovery*
*Completed: 2026-08-19*

## Self-Check: PASSED

- FOUND: tools/corpus/dated_series.py
- FOUND: tests/unit/test_dated_series.py
- FOUND: .planning/phases/09-etl-correctness-dedup-incremental-backfill-recovery/09-05-SUMMARY.md
- FOUND commit 13d0c13 (Task 1: feat(09-05))
- FOUND commit 74012e5 (docs(09-05): complete plan)
