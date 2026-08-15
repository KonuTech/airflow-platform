---
phase: 06-universal-csv-engine-schema-contracts-normalization
plan: 03
subsystem: csv-processor
tags: [csv-01, filename-mask, regex, strptime, detection]

# Dependency graph
requires:
  - phase: 06-02
    provides: FilenameMaskConfig (dataplat.config.model), FilenameParsingError (dataplat.errors), DIAGNOSTIC_CODES incl. "filename-does-not-match-mask" (dataplat.diagnostics)
provides:
  - "compile_mask/match_filename/parse_filename — the CSV-01 filename mask compiler, matcher and D-09-compliant entry point"
  - "A corpus-independent, self-contained unit-test oracle for filename masks (no fixture dependency; customers.yaml deliberately declares no mask per D-10)"
affects: [csv_processor.source (a future Source.inspect() integration plan), any dataset that opts into filename.mask]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Single-pass regex scanner (one finditer over one combined alternation pattern) for building a compiled sub-pattern from a mini-DSL, instead of chaining two independent re.sub passes over the same text — the second pass would re-match regex syntax the first pass just generated"
    - "Frozen dataclass compiled-artifact result (CompiledMask: pattern + per-facet re-parse rules) mirroring dataplat/config/hashing.py's 'pure function -> typed, frozen result' shape"

key-files:
  created:
    - packages/csv-processor/src/csv_processor/detect/filename.py
    - tests/unit/detect/test_filename.py
  modified: []

key-decisions:
  - "Rewrote 06-RESEARCH.md's Pattern 3 chained-re.sub sketch as a single left-to-right scan (_SCAN_RE.finditer) after live-verifying the sketch's two-pass design self-corrupts: expanding [_{seq:03d}] first produces \\d{3} in the intermediate string, and a second, unscoped token-substitution pass over the WHOLE result re-matches the literal {3} as an unexpanded {name} token, raising re.error: bad character in group name '03'."
  - "Escaped literal mask text via re.escape() during the scan — the research sketch left literal characters (e.g. the '.' in a '.csv' suffix) unescaped, which would let a bare '.' match ANY character in the compiled pattern rather than only a literal dot."
  - "Tracked which facets need a strptime vs. int() re-parse as two disjoint sets on CompiledMask (formats, int_facets), populated at compile time — rejected a reverse-engineering-from-the-pattern approach as fragile."

requirements-completed: [CSV-01, QUAL-04]

# Metrics
duration: 15min
completed: 2026-08-15
---

# Phase 6 Plan 3: Filename Mask Compiler Summary

**Hand-rolled strptime-token-to-regex filename mask compiler (compile_mask/match_filename/parse_filename), built as a single-pass scanner after catching and fixing a self-corrupting bug in the research sketch's chained-substitution design.**

## Performance

- **Duration:** 15 min
- **Started:** 2026-08-15T09:47:18Z (worktree setup)
- **Completed:** 2026-08-15T10:01:49Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- `compile_mask()` compiles a strptime-style token mask (`{dataset}_{country}_{business_date:%Y%m%d}[_{seq:03d}].csv`) into a single whole-string-anchored regex, with bracket-wrapped segments (D-08) becoming non-capturing optional groups
- `match_filename()` runs the compiled regex and re-parses captured groups into typed values (`date` for strptime-formatted facets, `int` for zero-padded-integer facets, `str` otherwise), returning `None` — never a partial dict — on no match
- `parse_filename()` wraps `match_filename` with D-09's reject-on-no-match contract, raising `FilenameParsingError` with `context={"diagnostic_code": "filename-does-not-match-mask", "filename": ..., "mask": ...}`, and documents D-11's fallback-only priority rule for the returned `business_date` facet
- Found and fixed a real, self-corrupting bug in 06-RESEARCH.md's own Pattern 3 sketch before it shipped (see Deviations) — verified live, not just reasoned about
- 18 unit tests covering every `<behavior>` case in the plan plus the malformed-mask, every-strptime-directive, two-independent-optional-segments, and literal-escaping edge cases; full `tests/unit` suite (176 tests) green, no regressions

## Task Commits

Each task was committed atomically. Task 1 (`tdd="true"`) followed the RED/GREEN gate sequence explicitly — the already-designed, already-manually-verified implementation was temporarily removed from the source tree to prove a genuine RED (import failure) before being restored for GREEN:

1. **Task 1: compile_mask / match_filename — the token-to-regex compiler**
   - RED: `6bdbfd3` (`test(06-03): add failing test for filename mask compiler`) — test file only, confirmed failing (`ModuleNotFoundError: No module named 'csv_processor.detect.filename'`) before commit
   - GREEN: `6174f04` (`feat(06-03): implement filename mask compiler`) — implementation restored + committed, confirmed all 14 Task-1 tests pass, plus a test-docstring line-length fix caught by `ruff`
2. **Task 2: Wire the reject-on-no-match diagnostic (D-09) and business_date-facet extraction (D-11)** - `0f86647` (`test`) — `parse_filename` was already implemented as part of Task 1's single-module design pass (see Deviations); this commit adds Task 2's own dedicated test coverage (no-match raises with the correct diagnostic code, business_date typed as `date`, plus a D-24 catalog-drift guard test)

**Plan metadata:** (this commit, `docs(06-03): complete plan`, created after this summary)

_Note: Task 1 is a TDD task; Task 2 is not (`type="auto"`, no `tdd` attribute) — its commit is test-only because the corresponding production code already existed from Task 1._

## Files Created/Modified
- `packages/csv-processor/src/csv_processor/detect/filename.py` - `CompiledMask` (frozen dataclass), `compile_mask`, `match_filename`, `parse_filename` — the complete CSV-01 filename-mask engine
- `tests/unit/detect/test_filename.py` - 18 tests: facet extraction, optional-segment presence/absence, no-match/prefix-match rejection, malformed-mask compile-time errors (unclosed/unmatched/nested brackets, unrecognized format spec), every supported strptime directive, two independent optional segments, literal-character escaping, and `parse_filename`'s diagnostic/business_date behavior

## Decisions Made
- Rewrote the mask compiler as a single left-to-right scan (one `_SCAN_RE.finditer` pass) instead of 06-RESEARCH.md Pattern 3's literal chained-`re.sub` sketch, after live-reproducing that the sketch corrupts its own output (see Deviations #1)
- Escaped all literal mask text via `re.escape()` during the scan (see Deviations #2)
- Represented "which facets need which re-parse rule" as two disjoint sets on `CompiledMask` (`formats: dict[str, str]`, `int_facets: frozenset[str]`) populated at compile time, rather than inferring the rule later from the compiled pattern's text
- Front-loaded Task 2's `parse_filename` into the same implementation pass as Task 1's `compile_mask`/`match_filename`, since all three functions were designed together for correctness (see Deviations #3) — Task 2's own commit therefore adds only its required test coverage

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed a self-corrupting chained-`re.sub` design inherited from the research sketch**
- **Found during:** Task 1, while manually verifying the implementation against the plan's own acceptance-criteria example before writing tests
- **Issue:** 06-RESEARCH.md's Architecture Patterns Pattern 3 sketch (explicitly marked "sketch, not yet verified against a full corpus of masks") expands bracket-optional segments with one `re.sub` pass, then runs a second, unscoped token-substitution `re.sub` pass over the *entire* resulting string. The first pass's output for `[_{seq:03d}]` is `(?:_(?P<seq>\d{3}))?` — which itself contains the substring `{3}`. The second pass's token pattern `\{(\w+)(?::([^}]+))?\}` matches that `{3}` as if it were an unexpanded `{name}` token (digits are valid `\w` characters), attempting to build a capture group named `"03"` — reproduced live: `re.error: bad character in group name '03'` at `re.compile()` time, which would have made the exact mask in the plan's own Task 1 acceptance criteria (`'{dataset}_{country}_{business_date:%Y%m%d}[_{seq:03d}].csv'`) fail to compile.
- **Fix:** Replaced the two chained `re.sub` passes with a single left-to-right scan (`_SCAN_RE.finditer`) using one combined alternation pattern (token | `[` | `]` | literal-run), so no already-generated regex fragment is ever re-scanned by a later pass.
- **Files modified:** `packages/csv-processor/src/csv_processor/detect/filename.py`
- **Verification:** `python -c "...compile_mask('{dataset}_{country}_{business_date:%Y%m%d}[_{seq:03d}].csv')..."` now compiles and matches correctly (reproduced the failure first, then confirmed the fix); full test suite green.
- **Committed in:** `6174f04` (Task 1 GREEN commit)

**2. [Rule 2 - Missing Critical] Escaped literal mask text with `re.escape()`**
- **Found during:** Task 1, same design review pass
- **Issue:** Neither the plan text nor the research sketch escaped literal characters between tokens (e.g. the `.` before `csv` in every example mask). An un-escaped `.` in a compiled regex matches *any* single character, not just a literal dot — so a mask intended to require a literal `.csv` suffix would have silently accepted e.g. `customersXcsv`, a correctness gap directly relevant to this plan's own "never a filename silently accepted under a guessed/loosened match" framing (D-09).
- **Fix:** Every run of literal mask text is passed through `re.escape()` during the single-pass scan before being appended to the compiled pattern.
- **Files modified:** `packages/csv-processor/src/csv_processor/detect/filename.py`
- **Verification:** Added `test_literal_mask_characters_are_escaped_not_treated_as_regex_metacharacters`, asserting `match_filename(compile_mask("{dataset}.csv"), "customersXcsv") is None`.
- **Committed in:** `6174f04` (Task 1 GREEN commit)

**3. [Process note, not a Rule 1-4 deviation] Task 2's `parse_filename` was implemented alongside Task 1's functions, not as a separate code change**
- **Found during:** Task 1 design — the plan's own interface block for Task 2 (`parse_filename` signature, exact `context` dict shape) was fully specified up front, and designing `compile_mask`/`match_filename`/`parse_filename` as one coherent module in a single pass (rather than writing `parse_filename` in a second, separate editing pass) avoided a redundant round-trip through the same file.
- **Effect on plan structure:** Task 1's GREEN commit (`6174f04`) already contains `parse_filename`. Task 2's commit (`0f86647`) is therefore test-only — it adds the two tests the plan's Task 2 action text specifies (no-match diagnostic-code assertion, business_date-typed-as-`date` assertion) plus a D-24 catalog-drift guard test, and verifies them against already-present, already-correct production code rather than against new code.
- **Files modified:** `tests/unit/detect/test_filename.py` only (Task 2 commit)
- **Verification:** Task 2's exact acceptance-criteria CLI probe (the `try/except FilenameParsingError` snippet) reproduced verbatim, printed `ok`.

---

**Total deviations:** 2 auto-fixed (1 bug, 1 missing-critical), plus 1 process note (no code/behavior impact).
**Impact on plan:** Both auto-fixes were necessary for the plan's own acceptance criteria to even compile/pass — this is not scope creep, it is closing a gap the plan's cited research sketch explicitly flagged as unverified ("sketch, not yet verified against a full corpus of masks"). The process note has zero behavioral impact; both tasks' required functionality and test coverage exist exactly as specified.

## Issues Encountered
None beyond the two auto-fixed items above, both caught during design/manual verification before any test was written against the buggy behavior.

## User Setup Required
None - no external service configuration required.

## Requirements Note

`QUAL-04` ("Unit tests cover filename parsing, encoding/dialect/header
detection, schema inference, structural and type validation, normalization,
deduplication, incremental logic and validation reports") is declared in
this plan's frontmatter alongside 9 other Wave 2 plans' frontmatter
(06-04/05/06/07/08/09/10/11/12/13 — confirmed by reading each plan file
directly in this worktree) — a deliberate shared-requirement pattern this
project already uses elsewhere (e.g. Phase 4's `ORCH-01`..`ORCH-09` across
11 plans). This plan's own contribution to QUAL-04 is exactly one clause:
unit tests for filename parsing. `requirements mark-complete` was run per
the standard protocol and flips the shared checkbox on first contact
(idempotent on every subsequent sibling plan's own call) — the phase
verifier is the actual gate that confirms every other named capability
(encoding/dialect/header detection, schema inference, normalization,
deduplication, incremental logic, validation reports) is genuinely covered
by its own plan before treating QUAL-04 as truly done, not just checked.

## Next Phase Readiness
- `compile_mask`/`match_filename`/`parse_filename` are ready for a future `Source.inspect()` integration plan to call `parse_filename` against a discovered object key and a dataset's `FilenameMaskConfig`
- No dataset in this codebase currently declares `filename.mask` (D-10: `customers.yaml` deliberately does not) — this capability's evidence is entirely this plan's own corpus-independent unit tests, matching CONTEXT.md's explicit instruction
- No blockers. This plan touched only its own two declared files (`packages/csv-processor/src/csv_processor/detect/filename.py`, `tests/unit/detect/test_filename.py`) and shares no file with any sibling Wave 2 detector plan (06-04 through 06-13)

---
*Phase: 06-universal-csv-engine-schema-contracts-normalization*
*Completed: 2026-08-15*

## Self-Check: PASSED

- FOUND: `packages/csv-processor/src/csv_processor/detect/filename.py`
- FOUND: `tests/unit/detect/test_filename.py`
- FOUND: `.planning/phases/06-universal-csv-engine-schema-contracts-normalization/06-03-SUMMARY.md`
- FOUND commit: `6bdbfd3` (test — RED gate)
- FOUND commit: `6174f04` (feat — GREEN gate)
- FOUND commit: `0f86647` (test — Task 2 coverage)
