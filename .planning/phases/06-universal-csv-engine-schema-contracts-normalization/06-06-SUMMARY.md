---
phase: 06-universal-csv-engine-schema-contracts-normalization
plan: 06
subsystem: etl
tags: [csv, header-detection, footer-detection, duplicate-detection, schema-contracts, corpus-testing]

# Dependency graph
requires:
  - phase: 06-universal-csv-engine-schema-contracts-normalization (plan 02)
    provides: "CsvParsingConfig.{header_row,skip_footer_rows,header_trim,header_case_sensitive}, dataplat.errors.FileInspectionError, dataplat.diagnostics.DIAGNOSTIC_CODES (including \"duplicate-header-names\")"
provides:
  - "detect_header(rows, *, contract_header_row=None, header_trim=False, skip_footer_rows=0, footer_patterns=()) -> HeaderDetection"
  - "HeaderDetection frozen dataclass: header_row_index, raw_header, trimmed_header, preamble_row_count, has_header, footer_row_indices, repeated_header_row_indices"
  - "Unconditional case-folded duplicate-header-name rejection raising FileInspectionError(diagnostic_code=\"duplicate-header-names\")"
affects: [csv_processor.source (streaming reader wiring), csv_processor.detect.schema (schema-contract validation), wave-3 pipeline-assembly plans]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Detector shape: pure function -> frozen dataclass result, no I/O, no side effects (mirrors dataplat.config.hashing::hash_config)"
    - "Corpus-fixture-parametrized detector tests: generate the corpus once per module, assert only against expect: keys a fixture actually declares"

key-files:
  created:
    - packages/csv-processor/src/csv_processor/detect/header.py
    - tests/unit/detect/test_header.py
  modified: []

key-decisions:
  - "Value uniqueness (STACK.md §11's 4th scoring signal) is scored conceptually but never gates header acceptance, so fixtures 14/48 (duplicate-valued header rows) are still detected as the header and reach duplicate-name rejection instead of being silently skipped as non-header rows."
  - "Duplicate-header-name detection uses one case-folded (str.casefold()) grouping for both exact and case-variant collisions, since an exact duplicate is trivially also a case-insensitive duplicate -- no separate exact-match pass is needed."
  - "CsvParsingConfig.header_case_sensitive is deliberately NOT a parameter of detect_header: per the plan's own explicit instruction, duplicate-name rejection is unconditional (fixture 48 rejects regardless of that setting, since PostgreSQL folds unquoted identifiers to lower case) -- the setting governs a later, different concern (header-to-columns:-contract name matching), not detection here. Documented at length in the module docstring so this doesn't read as an oversight."
  - "Footer detection walks backward from the end of the data rows and stops at the first row (from the end) that matches neither the differing-field-count nor the footer_patterns signal -- guarantees a footer can never be \"found\" in the middle of real data (verified against both 13_footer.csv's 3-row footer and 64's single-row footer)."
  - "Duplicate-name rejection runs before footer/repeated-header detection, so a file with duplicate header names fails fast without ever scoring its data rows -- matches the corpus's \"outcome: rejected-file\" framing (whole file fails, nothing loads)."

patterns-established:
  - "Detector modules under csv_processor/detect/ return a frozen, slotted dataclass carrying every field a downstream consumer (streaming reader, schema validation) needs, rather than raising for anything but a genuinely fatal condition."

requirements-completed: [CSV-07, CSV-08, SCHEMA-02, QUAL-04]

duration: 35min
completed: 2026-08-15
---

# Phase 06 Plan 06: Header/Metadata-Preamble/Footer Detector Summary

**`detect_header` scores each candidate row on emptiness/non-numeric-shape/modal-field-count-match to find the header and metadata preamble, then detects trailing footer rows and mid-file repeated headers against it, with unconditional case-folded duplicate-header-name rejection.**

## Performance

- **Duration:** 35 min (estimated -- no precise start timestamp was captured before the worktree-branch-check step)
- **Started:** 2026-08-15 (session start)
- **Completed:** 2026-08-15T10:08:30Z
- **Tasks:** 2
- **Files modified:** 2 (both newly created)

## Accomplishments

- `detect_header` correctly resolves all ten CSV-07/CSV-08 corpus fixtures (01, 11, 12, 13, 18, 19, 48, 49, 63, 64) against their own declared `expect:` blocks -- no expectation restated independently in test code.
- A metadata preamble (comment lines + a blank line) is excluded and the real header found at its correct row index, even though the preamble itself contains a candidate delimiter character (a colon) that must never influence detection.
- Footer rows are identified purely by their differing field count (or a contract regex), walking backward from the end so a footer can never be misidentified mid-file; a `skip_footer_rows` override skips exactly that many trailing rows unconditionally when non-zero.
- A concatenated export's repeated header line (an interior row whose values exactly equal the header's) is found and recorded, never mistaken for a data row whose `amount` field is the literal string `"amount"`.
- Exact (`14_duplicate_columns.csv`) and case-variant (`48_duplicate_header_names_case_variant.csv`) duplicate header names both raise `FileInspectionError` with a `"duplicate-header-names"` diagnostic code and the actual colliding names -- never a silent rename or last-wins resolution.

## Task Commits

Each task was committed atomically:

1. **Task 1: detect_header -- scoring the header row index and the metadata preamble** - `d0c31fc` (feat)
2. **Task 2: Footer detection and duplicate-header-name rejection** - `9011063` (feat)

_Note: Task 2's commit necessarily also modified Task 1's two files in place (extending `HeaderDetection`/`detect_header`, and moving fixture 48 out of the generic "matches corpus declaration" test loop into the new duplicate-rejection test) -- this is the plan's own designed sequencing, not scope creep._

## Files Created/Modified

- `packages/csv-processor/src/csv_processor/detect/header.py` - `detect_header()` + `HeaderDetection`; header/preamble scoring, footer detection, repeated-header detection, unconditional duplicate-name rejection
- `tests/unit/detect/test_header.py` - Corpus-parametrized suite over the ten CSV-07/CSV-08 fixtures, plus targeted unit tests for the header-shaped-like-data, empty-input, contract-header-row, header_trim-default, skip_footer_rows-override and footer_patterns-override cases

## Decisions Made

See `key-decisions` in the frontmatter above for the full list with rationale. In short: value uniqueness is scored but never gates header acceptance (so duplicate-valued header rows still reach the duplicate-name check); duplicate detection uses a single case-folded pass covering both exact and case-variant collisions; `header_case_sensitive` is deliberately not threaded through `detect_header` because the plan's own task text makes duplicate rejection unconditional; footer detection is a backward walk that stops at the first genuinely-data-shaped row from the end; duplicate-name rejection runs before footer/repeated-header detection so a rejected file fails fast.

## Deviations from Plan

None - plan executed exactly as written. One point worth flagging explicitly rather than silently: this plan's `must_haves.truths` names `csv.header_case_sensitive` alongside the other three contract overrides as something that "actually overrides detection when supplied." `detect_header` does not accept this parameter at all -- per Task 2's own action text ("raise for either exact OR case-insensitive duplicates found," "independent of `header_case_sensitive`'s own default"), duplicate-name rejection is unconditional, so threading an inert parameter through the function would have been dead code the codebase's own conventions explicitly warn against ("a subclass with no raise site is dead code wearing a design decision's clothes" -- the same reasoning applies to a parameter with no behavioral effect). This is a literal-instruction-following choice, not an oversight; documented at length in the module docstring.

## Issues Encountered

- mypy's flow-sensitive narrowing did not cleanly merge `header_row_index`'s type (`int | None`) across the contract-override/auto-detect branches when each branch used its own separately-inferred local assignment. Resolved by declaring `header_row_index: int | None` and `raw_header: tuple[str, ...]` explicitly at the top of `detect_header` and adding one shared `if header_row_index is None: return _not_found()` narrowing point after both branches converge, rather than nesting the check inside only one branch. Also extracted the scoring loop into `_find_header_row()` to keep the function body readable after this change.
- The corpus-parametrized generic test (`test_detect_header_matches_corpus_declaration`) was written in Task 1 asserting a normal (non-raising) return for all ten fixtures, including `48_duplicate_header_names_case_variant.csv`. Once Task 2 added duplicate-name rejection, fixture 48 started raising instead of returning -- expected, since Task 2's own action text says to extend "fixtures 13, 14, 48, 63, 64" with new assertions. Resolved by removing 48 from the generic loop's fixture tuple and covering it (alongside 14) in the new `test_detect_header_rejects_duplicate_header_names` test, exactly as Task 2's action text specifies.
- `exc_info.value.context["colliding_names"]` is typed `object` (from `DataPlatformError.context: dict[str, object]`), which mypy rejected as an argument to `set()`. Resolved with an explicit `assert isinstance(colliding, list)` narrowing check before use -- a genuine runtime safety check, not just a type-checker workaround.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `detect_header`/`HeaderDetection` are ready for a later wiring plan to consume: the streaming reader (to skip footer/repeated-header rows and slice off the preamble) and schema-contract validation (to match `trimmed_header`/`raw_header` against a dataset's `columns:` contract, including where `header_case_sensitive` actually applies).
- All ten CSV-07/CSV-08 corpus fixtures pass; the full unit suite (178 tests) passes with no regressions from this plan's changes.
- No blockers. `csv.footer_patterns` has no corresponding `CsvParsingConfig` field yet (out of this plan's declared file scope, which was limited to `header.py`/`test_header.py`) -- `detect_header`'s `footer_patterns` parameter is ready to receive it whenever a later plan adds that field and wires it through.

---
*Phase: 06-universal-csv-engine-schema-contracts-normalization*
*Completed: 2026-08-15*

## Self-Check: PASSED

- FOUND: `packages/csv-processor/src/csv_processor/detect/header.py`
- FOUND: `tests/unit/detect/test_header.py`
- FOUND: commit `d0c31fc` (Task 1)
- FOUND: commit `9011063` (Task 2)
