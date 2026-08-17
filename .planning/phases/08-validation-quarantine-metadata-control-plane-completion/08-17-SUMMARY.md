---
phase: 08-validation-quarantine-metadata-control-plane-completion
plan: 17
subsystem: validation
tags: [quality-rules, referential-integrity, business-key, staging-loader]

# Dependency graph
requires:
  - phase: 08 (plan 08-16)
    provides: meta.rejected_records.business_key column, RejectedRecord.business_key
      field, MetadataRepository.resolve_rejected_records_for_business_keys
provides:
  - "business_key_index constructor kwarg + _extract_business_key helper on
    CompletenessRule, PatternRule, ValidityRangeRule, UniquenessRule"
  - "business_key=order_id on every ReferentialIntegrityBarrier REFERENTIAL_ORPHAN
    RejectedRecord (the row's own identity, never the customer_id it failed
    against)"
  - "StagingLoader._build_quality_stages computes business_key_index once per
    run from ctx.config.columns and threads it into every streaming quality
    rule it constructs"
affects: [08-18]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Per-file duplicated _extract_business_key helper (mirrors this
      codebase's existing _reconstruct_raw_line duplication convention across
      completeness.py/pattern.py/validity_range.py/uniqueness.py) -- returns
      None for business_key_index=None or an empty/absent value (D-25), else
      str()-coerced value"
    - "business_key_index computed exactly ONCE per run in
      StagingLoader._build_quality_stages (not per rule), using the same
      self._target_columns.index(...) idiom _build_one_quality_stage already
      uses for its own column_index, then threaded unconditionally into every
      streaming rule's rule_kwargs"

key-files:
  created: []
  modified:
    - packages/dataplat/src/dataplat/validate/completeness.py
    - packages/dataplat/src/dataplat/validate/pattern.py
    - packages/dataplat/src/dataplat/validate/validity_range.py
    - packages/dataplat/src/dataplat/validate/uniqueness.py
    - packages/dataplat/src/dataplat/validate/referential.py
    - packages/dataplat/src/dataplat/load/staging.py
    - tests/unit/validate/test_quality_rules.py
    - tests/unit/validate/test_uniqueness.py
    - tests/integration/test_referential_integrity.py

key-decisions:
  - "RaggedRowGuard (pipeline/engine.py) deliberately untouched -- D-25
    requires a structurally-ragged row's business_key to stay None, and
    RejectedRecord.business_key already defaults to None, so simply never
    passing the keyword there is correct and needs no code change. Confirmed
    via git diff --stat showing zero changes to that file."
  - "UniquenessRule's real customers.yaml usage sets business_key_index equal
    to column_index (the rule targets customer_id itself) -- proven directly
    by a dedicated unit test rather than assumed."

requirements-completed: [VALID-08]

# Metrics
duration: 25min
completed: 2026-08-18
---

# Phase 08 Plan 17: business_key extraction at every RejectedRecord-creation site Summary

**Every row-level quality-rule rejection and referential-orphan rejection now populates `RejectedRecord.business_key` when the dataset's configured business-key column value is reliably present on the row -- the "capture-at-reject-time" half of the VALID-08 gap-closure design, wired end to end from `StagingLoader` through all four streaming rules and the referential barrier.**

## Performance

- **Duration:** 25 min
- **Started:** 2026-08-18T00:20:44Z (worktree base reset, post-08-16 merge)
- **Completed:** 2026-08-18T00:45:00Z
- **Tasks:** 2 (both `type="auto"`)
- **Files modified:** 9 (6 source, 3 test)

## Accomplishments
- `CompletenessRule`, `PatternRule`, `ValidityRangeRule`, `UniquenessRule` each
  gained a keyword-only `business_key_index: int | None = None` constructor
  parameter, a per-file `_extract_business_key` helper (duplicated across the
  four files, matching this codebase's established `_reconstruct_raw_line`
  duplication convention), and `business_key=_extract_business_key(row,
  self._business_key_index)` threaded into every `RejectedRecord(...)`
  construction site -- including both of `ValidityRangeRule`'s two sites
  (`VALIDITY_RANGE_UNPARSEABLE` and `VALIDITY_RANGE_VIOLATION`)
- `ReferentialIntegrityBarrier.apply()`'s `REFERENTIAL_ORPHAN` `RejectedRecord`
  now captures `business_key=str(row["order_id"])` -- the orphan row's OWN
  identity, never the `customer_id` it failed against -- using data the
  anti-join query already selects, no new query needed
- `StagingLoader._build_quality_stages` computes `business_key_index` exactly
  once per run (not once per rule) by finding the single `ColumnContract`
  with `business_key: True` in `ctx.config.columns` and resolving its
  position via `self._target_columns.index(...)` (the same idiom
  `_build_one_quality_stage` already uses for `column_index`), threading it
  unconditionally into every streaming rule's `rule_kwargs`
- `RaggedRowGuard` (`pipeline/engine.py`) is provably untouched -- confirmed
  by `git diff --stat` returning empty for that file throughout both tasks
- 8 new unit tests (2 per rule class: `CompletenessRule`, `ValidityRangeRule`,
  `PatternRule`, plus 1 for `UniquenessRule`) prove business-key extraction
  reaches `RejectedRecord.business_key` end to end, both when
  `business_key_index` is configured (capturing the correct distinct-column
  value) and when it defaults to `None`
- 1 integration-test assertion extended (`test_referential_integrity.py`)
  proves a real live `REFERENTIAL_ORPHAN` row's `business_key` equals its own
  `order_id` (`"9202"`), explicitly asserting it is NOT the `customer_id`
  (`"8699"`) it failed against

## Task Commits

Each task was committed atomically:

1. **Task 1: business_key extraction in the four streaming rules + ReferentialIntegrityBarrier** - `83e5f44` (feat)
2. **Task 2: Thread business_key_index from StagingLoader + unit test proof** - `30ebf90` (feat)

**Plan metadata:** (this commit, docs: complete plan)

## Files Created/Modified
- `packages/dataplat/src/dataplat/validate/completeness.py` - `business_key_index` kwarg + `_extract_business_key` helper + threaded into `RejectedRecord`
- `packages/dataplat/src/dataplat/validate/pattern.py` - Same, plus `# noqa: PLR0913` on the now-six-argument constructor (matching `validity_range.py`'s existing precedent)
- `packages/dataplat/src/dataplat/validate/validity_range.py` - Same, threaded into BOTH `RejectedRecord` sites
- `packages/dataplat/src/dataplat/validate/uniqueness.py` - Same
- `packages/dataplat/src/dataplat/validate/referential.py` - Inline `business_key=str(row["order_id"])` on the orphan `RejectedRecord`
- `packages/dataplat/src/dataplat/load/staging.py` - `business_key_index` computed once in `_build_quality_stages`, threaded into `_build_one_quality_stage`'s signature and `rule_kwargs`
- `tests/unit/validate/test_quality_rules.py` - 6 new tests (2 each for `CompletenessRule`/`ValidityRangeRule`/`PatternRule`)
- `tests/unit/validate/test_uniqueness.py` - 1 new test proving `business_key_index == column_index` for the real `customer_id` case
- `tests/integration/test_referential_integrity.py` - Extended the existing orphan-mix test with `business_key == "9202"` / `!= "8699"` assertions

## Decisions Made
- `pattern.py`'s constructor crossed ruff's `PLR0913` (max-args) threshold once
  `business_key_index` was added as its sixth keyword-only parameter. Added
  `# noqa: PLR0913` with the same justification comment `validity_range.py`
  already carries for its own six-then-seven-argument constructor (all fields
  load-bearing, keyword-only). Not a deviation from the plan's design --
  purely a lint-compliance addition to satisfy the plan's own `ruff check`
  acceptance criterion.
- `business_key_index` was positioned in each constructor's parameter list
  after `rule_id` and before rule-specific params (`pattern`/`minimum`/
  `maximum`), exactly as the plan specified -- since every parameter is
  keyword-only (`*,`), this affects only docstring/readability ordering, not
  call-site behavior.

## Deviations from Plan

None - plan executed exactly as written. All acceptance criteria and the
plan's top-level `<verification>` block passed on first attempt with no
auto-fixes required.

## Issues Encountered
- Same worktree-environment quirk documented in 08-16-SUMMARY.md: the venv at
  the main repo's `.venv` is an editable install pointing at the MAIN repo's
  `packages/dataplat/src`, not this worktree's copy. `PYTHONPATH=<worktree>/packages/dataplat/src`
  was prepended for every `mypy`/`pytest` verification command in this
  session too. Not a code issue.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Plan 08-18 (wiring the real business-key derivation into `run_ingest`'s
  `resolve_rejected_records_for_business_keys` call site, replacing 08-16's
  documented `business_keys=[]` placeholder, plus rebuilding the two
  `test_publish_transaction_wiring.py` tests 08-16 skipped) builds directly
  on this plan's extraction work: every `RejectedRecord` this platform's real
  datasets (`customers`, `orders`) can produce now carries a real,
  extractable `business_key` value ready to be read back and matched against
  a future successful batch's own published business keys.
- `StagingResult.rejected_records` (already threaded since an earlier plan)
  now carries `RejectedRecord` instances with populated `business_key` values
  for every quality-rule/referential rejection this run produced -- 08-18's
  `run_ingest` wiring can read these directly rather than deriving business
  keys from a separate query.

---
*Phase: 08-validation-quarantine-metadata-control-plane-completion*
*Completed: 2026-08-18*
