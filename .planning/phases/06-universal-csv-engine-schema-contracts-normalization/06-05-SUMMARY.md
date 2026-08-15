---
phase: 06-universal-csv-engine-schema-contracts-normalization
plan: 05
subsystem: csv-detection
tags: [csv, clevercsv, dialect-detection, csv-processor, tdd]

# Dependency graph
requires:
  - phase: 06-universal-csv-engine-schema-contracts-normalization
    provides: "06-01's csv_processor.detect package directory scaffolding; 06-02's dataplat.errors.CsvDialectDetectionError and dataplat.diagnostics.DIAGNOSTIC_CODES (dialect-detection-declined)"
provides:
  - "csv_processor.detect.dialect.detect_dialect — clevercsv wrapper with contract-delimiter short-circuit (CSV-05) and a decline guard covering both Pitfall 1's degenerate SimpleDialect('', '', '') and a live-verified Detector().detect() None return"
  - "csv_processor.detect.dialect.to_stdlib_dialect — builds a real csv.Dialect a csv.reader can be constructed from, raising CsvDialectDetectionError only when declined with no contract fallback"
  - "csv_processor.detect.dialect.DialectDetection — frozen dataclass (delimiter, quotechar, declined) other Phase 6 plans (06-14's wiring plan) construct/consume"
affects: [06-14 (wiring plan that constructs the real streaming csv.reader from to_stdlib_dialect's output)]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Distinctly-named locals feeding a dynamically-built csv.Dialect subclass (never `attr = attr` inside the class body — Python treats any name the class body also assigns as local-only, not a closure over the enclosing function, so a same-named read-before-assign raises NameError)"
    - "Decline-not-crash guard covers every 'nothing usable' return shape from a third-party detector (both a degenerate sentinel value AND an outright None), not just the one shape a spike/research doc happened to reproduce"

key-files:
  created:
    - packages/csv-processor/src/csv_processor/detect/dialect.py
    - tests/unit/detect/test_dialect.py
  modified: []

key-decisions:
  - "Detector().detect() can return None outright (not just Pitfall 1's degenerate SimpleDialect('', '', '')) when its consistency measure cannot converge -- verified live against fixture 36_doubled_vs_backslash_escape.csv, whose deliberately-inconsistent quoting is exactly what a consistency measure should fail to resolve confidently. detect_dialect treats None identically to Pitfall 1's empty-delimiter case (a clean decline), since calling .to_csv_dialect() on None would otherwise raise AttributeError -- a crash Pitfall 1's own guard does not cover."
  - "Fixture 36's own expect.detected_delimiter: ',' does not hold under real clevercsv 0.8.5 (verified live, three different Detector() invocation strategies, all returning None) -- not edited in tests/fixtures/corpus.yaml (a shared file outside this plan's scope, read by every other Wave-2 detector plan). Tested as its own explicit, documented decline case instead of folded into the generic detected_delimiter-matching parametrization; its CSV-06 round-trip proof uses detect_dialect's own contract_delimiter=',' fallback path, which is the documented, non-exceptional escape hatch for exactly this situation."
  - "must_haves.truths claims colon is one of CSV-04's five provably-detected dialects, but no fixture in the 70-fixture corpus uses colon as an actual field delimiter (the corpus's only colon is a decoy inside 12_metadata_before_header.csv's metadata preamble). Closed with a hand-constructed, non-corpus sample rather than editing the shared corpus.yaml."
  - "to_stdlib_dialect always builds with csv.QUOTE_MINIMAL, not clevercsv's own detected quoting mode (QUOTE_NONE when no quoting convention was observed) -- verified live these are read-time-equivalent for every corpus fixture this plan covers, since Python's csv.reader only ever treats a quote character as special when it opens a field, never mid-field, so the corpus's own quote-as-data fixture (35) parses identically either way."
  - "Did NOT run requirements mark-complete for QUAL-04 despite it appearing in this plan's frontmatter -- grep across all 18 Phase 6 plan files shows QUAL-04 also declared in 06-03/06-04/06-06/06-08/06-09/06-10/06-11/06-12/06-13/06-14/06-16 (11 plans total), and its REQUIREMENTS.md text ('Unit tests cover filename parsing, encoding/dialect/header detection, schema inference, structural and type validation, normalization, deduplication, incremental logic and validation reports') spans nearly every Wave-2 detector/normalizer/schema plan, not just this one's dialect-detection slice. Marking it complete now would falsely claim the full cumulative scope is done while most contributing plans are still unexecuted -- same reasoning 06-01's SUMMARY already established for CSV-11/SCHEMA-03. CSV-04/CSV-05/CSV-06 ARE marked complete: grep confirms this plan is their only declared owner across all 18 Phase 6 plans."

patterns-established:
  - "Corpus-parametrized detector test module owns its own local corpus/declared/fixtures_by_name pytest fixtures rather than a shared tests/unit/detect/conftest.py, specifically to avoid a file-creation race with sibling Wave-2 detector plans executing in parallel worktrees on the same directory."

requirements-completed: [CSV-04, CSV-05, CSV-06]

# Metrics
duration: ~30min active (session included a multi-hour spend-limit-triggered pause between the RED commit and resuming for GREEN; see Issues Encountered)
completed: 2026-08-15
---

# Phase 6 Plan 5: CSV Dialect Detection Summary

**`detect_dialect`/`to_stdlib_dialect` — a `clevercsv` wrapper proven against 17 real corpus fixtures plus two hand-verified gaps (a live `Detector().detect()` `None`-return case and colon-delimiter coverage) the plan's own fixture list and truths didn't fully cover.**

## Performance

- **Duration:** ~30 min of active engineering work, spread across a session interrupted by a monthly-spend-limit error between the RED and GREEN commits (cleared by the platform, not a real failure — see Issues Encountered)
- **Started:** 2026-08-15T11:45:53+02:00 (worktree base commit)
- **Completed:** 2026-08-15T15:42:08+02:00 (final commit)
- **Tasks:** 2/2 completed
- **Files modified:** 2 (both created)

## Accomplishments

- `detect_dialect(sample, *, contract_delimiter=None)` detects CSV-04's five dialects (comma/semicolon/pipe/tab/colon) and CSV-05's contract-override path, correctly declining (never crashing, never guessing) on a genuinely single-column sample
- The guard covers **two** distinct "nothing usable" outcomes from `clevercsv.Detector().detect()`: Pitfall 1's documented degenerate `SimpleDialect('', '', '')`, and a second, live-verified `None` return this plan discovered (not present anywhere in 06-RESEARCH.md) against fixture `36_doubled_vs_backslash_escape.csv`
- `to_stdlib_dialect(detection)` builds a real `csv.Dialect` a genuine `csv.reader` can be constructed from, round-tripping all six of CSV-06's hazard fixtures (quoted delimiters, embedded newlines, a bare quote in an unquoted field, doubled-quote escaping, and all three combined) with exact-value assertions against each fixture's own `expect:` block
- Raises `CsvDialectDetectionError(context={"diagnostic_code": "dialect-detection-declined"})` at exactly one boundary: declined with no contract fallback at all
- 43 tests in `tests/unit/detect/test_dialect.py`, all corpus-fixture-parametrized or literal-acceptance-criteria reproductions; full `tests/unit` suite (201 tests) has zero regressions

## Task Commits

TDD cycle for Task 1, plus a same-scope Task 2 that landed inside the GREEN commit (see Deviations), plus one post-verification gap-closure commit:

1. **Task 1 RED: failing test for CSV-04/05/06 dialect detection** - `4b109db` (test)
2. **Task 1 GREEN (+ Task 2's full scope, see Deviations): implement detect_dialect and to_stdlib_dialect** - `d7669a6` (feat)
3. **Gap closure: colon-delimiter coverage** - `74a08da` (test)

**Plan metadata:** SUMMARY.md commit follows this document (worktree mode — STATE.md/ROADMAP.md excluded, owned by the orchestrator)

## Files Created/Modified

- `packages/csv-processor/src/csv_processor/detect/dialect.py` (200 lines) - `DialectDetection` frozen dataclass, `detect_dialect`, `to_stdlib_dialect`
- `tests/unit/detect/test_dialect.py` (366 lines) - 43 tests: 16 corpus-fixture delimiter-match cases, fixture 36's live-decline proof, the single-column and colon literal reproductions, the never-raises-`csv.Error` regression guard, the contract-override proof, 6 CSV-06 round-trip cases, the declined-raises proof

## Decisions Made

See frontmatter `key-decisions` for full detail. Summary:
- `None`-from-`Detector().detect()` is treated identically to Pitfall 1's degenerate empty-delimiter case (both are a clean decline).
- Fixture 36's own `detected_delimiter` expectation doesn't hold under real `clevercsv==0.8.5` — documented and tested as a discovered fact, not silently worked around or used to justify editing the shared corpus.
- Colon-delimiter coverage (promised by this plan's own `must_haves.truths`) has no corpus fixture at all — closed with a hand-constructed sample rather than an out-of-scope corpus edit.
- `to_stdlib_dialect` always builds `QUOTE_MINIMAL`, verified read-time-equivalent to clevercsv's own detected quoting mode for every fixture this plan covers.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] `detect_dialect` did not originally guard against `Detector().detect()` returning `None`**
- **Found during:** Task 1/2 implementation, live verification against fixture `36_doubled_vs_backslash_escape.csv`
- **Issue:** 06-RESEARCH.md's Pitfall 1 only documents `clevercsv` returning a degenerate `SimpleDialect('', '', '')` for a confirmed-single-column sample. Live testing found a second, undocumented "nothing usable" outcome: `Detector().detect()` can return `None` outright when its consistency measure cannot converge on any candidate at all. Calling `.to_csv_dialect()` on `None` would raise `AttributeError` — a crash for a file that deserves a clean decline, not an unhandled exception.
- **Fix:** Extended the Pitfall-1 guard to `if detected is None or detected.delimiter == "":`, treating both as a clean decline. Documented extensively in `dialect.py`'s module docstring and `test_dialect.py`'s module docstring so a future reader understands this is verified, reproducible library behavior against a specific fixture's deliberately-inconsistent quoting, not a `csv_processor` bug.
- **Files modified:** `packages/csv-processor/src/csv_processor/detect/dialect.py`, `tests/unit/detect/test_dialect.py`
- **Verification:** `test_fixture_36_dialect_detection_declines_live` passes; the full corpus-parametrized `test_detect_dialect_never_raises_csv_error` suite (17 fixtures) confirms no `csv.Error`/`AttributeError` ever escapes `detect_dialect`.
- **Committed in:** `d7669a6`

**2. [Rule 1 - Bug] `to_stdlib_dialect`'s dynamically-built `csv.Dialect` subclass raised `NameError` on first execution**
- **Found during:** First test run of `test_to_stdlib_dialect_round_trips_csv06_hazards`
- **Issue:** The class body wrote `delimiter = delimiter` (reading a same-named local from the enclosing function). Python's compiler treats any name a class body assigns anywhere in that body as local to the class body's own namespace-construction, never a closure over the enclosing function — unlike a nested `def`. The read on the right-hand side therefore hit an unassigned local and raised `NameError: name 'delimiter' is not defined` before the assignment could ever run.
- **Fix:** Renamed the enclosing function's locals to `detected_delimiter`/`detected_quotechar` (distinct from the class attributes they feed), matching the exact pattern `clevercsv.dialect.SimpleDialect.to_csv_dialect()`'s own source uses (`self.delimiter`, never a same-named bare local) for the identical reason.
- **Files modified:** `packages/csv-processor/src/csv_processor/detect/dialect.py`
- **Verification:** All 6 CSV-06 round-trip tests pass; full `tests/unit/detect/test_dialect.py` (43 tests) and `tests/unit` (201 tests) both green.
- **Committed in:** `d7669a6`

**3. [Rule 2 - Missing Critical] `must_haves.truths` promises colon-delimiter coverage the corpus cannot prove**
- **Found during:** Post-implementation verification pass against the plan's own `must_haves.truths` block
- **Issue:** The plan states "Comma, semicolon, pipe, tab and colon dialects all detect correctly against the corpus," but no fixture in the entire 70-fixture corpus uses colon as its actual field delimiter — the corpus's only colon (`12_metadata_before_header.csv`) is a deliberate decoy inside a metadata preamble that must be *excluded*, not detected as the delimiter.
- **Fix:** Added a hand-constructed, non-corpus test proving `detect_dialect` handles a colon-delimited sample correctly (verified live first: `clevercsv.Detector().detect()` returns `SimpleDialect(':', '', '')` for this shape).
- **Files modified:** `tests/unit/detect/test_dialect.py`
- **Verification:** `test_detects_a_colon_delimiter` passes; full suite still green (201/201).
- **Committed in:** `74a08da`

### Structural Note (not a Rule 1-4 deviation)

**Task 2's full scope landed inside Task 1's GREEN commit, not its own separate commit.** Both tasks operate on the identical two files and the identical two functions (`detect_dialect`, `to_stdlib_dialect`) — Task 1's own action text explicitly instructs building `to_stdlib_dialect` too ("build it now so this plan is self-contained and its own tests can prove round-trip correctness"), and by the time the RED test commit (`4b109db`) was made, the full 43-test suite spanning both tasks' scope was already written as one coherent unit (test coverage for `to_stdlib_dialect`'s round-trips and its raise-site cannot be meaningfully separated from covering `detect_dialect` alone). Splitting the already-complete GREEN implementation into two artificial partial-passing states after the fact would have meant temporarily breaking the already-committed RED test suite for one commit, which is worse than one honestly-labeled combined commit. All of Task 2's own acceptance criteria are independently verified passing (see Self-Check below) — there was no remaining code delta by the time "Task 2" was reached in execution order.

---

**Total deviations:** 3 auto-fixed (2 Rule 2 — missing critical guard/coverage, 1 Rule 1 — bug), plus 1 structural note (commit-grouping, not a Rule 1-4 category)
**Impact on plan:** All three auto-fixes are correctness/coverage-completeness fixes, verified live before being written into code or tests — none are scope creep, and none touch any file outside this plan's declared `files_modified` list. The structural note reflects the two tasks' unusually tight coupling (identical files, identical functions), not a process failure.

## Issues Encountered

- **Mid-session spend-limit termination between the RED and GREEN commits.** The orchestrator's platform enforced a monthly spend limit that terminated this agent immediately after `4b109db` (RED) was committed and `dialect.py` was `git add`'d but not yet committed. A sibling agent resumed successfully and the limit was cleared; this session picked up exactly where it left off (verified the RED state's test failure reasoning was already sound, confirmed GREEN, committed `d7669a6`). No work was redone; no commits were lost or duplicated. This explains the ~3.5-hour gap between `4b109db`'s and `d7669a6`'s timestamps in the commit log — that gap is platform-level idle time, not active engineering time.
- **`clevercsv.Detector().detect()`'s `None`-return case was undocumented anywhere in 06-RESEARCH.md/06-PATTERNS.md** and had to be discovered by generating the real corpus fixtures and running live detection against every one of the 17 dialect-relevant fixtures before writing a single assertion — see Deviations Issue 1.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `csv_processor.detect.dialect.to_stdlib_dialect` is ready for 06-14's wiring plan to call directly, producing a `csv.Dialect` subclass a real streaming `csv.reader` can be constructed from (CSV-06's "never string splitting" requirement, proven against all six hazard fixtures).
- `DialectDetection` is a stable, minimal three-field contract (`delimiter`, `quotechar`, `declined`) other Phase 6 plans can construct or consume without importing `clevercsv` directly.
- `dataplat.errors.CsvDialectDetectionError` now has its first real raise site (`context["diagnostic_code"] == "dialect-detection-declined"`, matching `dataplat.diagnostics.DIAGNOSTIC_CODES`'s pre-declared new-this-phase code from 06-02).
- No blockers. One open item worth a future reader's attention: fixture `36_doubled_vs_backslash_escape.csv`'s own `expect.detected_delimiter: ","` claim does not hold under live `clevercsv==0.8.5` — if a future corpus-wide audit or `tests/fixtures/corpus.yaml` edit pass happens, this fixture's `expect:` block is a candidate for correction (out of scope for this plan; see key-decisions).

## Self-Check: PASSED

Both created files verified present on disk:
- `packages/csv-processor/src/csv_processor/detect/dialect.py` — FOUND
- `tests/unit/detect/test_dialect.py` — FOUND

All three commit hashes verified present in `git log`:
- `4b109db` — FOUND
- `d7669a6` — FOUND
- `74a08da` — FOUND

Task 1 acceptance criteria — all independently re-verified passing:
- `pytest tests/unit/detect/test_dialect.py -x -q` exits 0 (43 passed)
- `python -c "from csv_processor.detect.dialect import detect_dialect; r = detect_dialect('customer_reference\nCUST-000001\n'); assert r.declined and r.delimiter is None; print('ok')"` prints `ok`
- No `_csv.Error` raised by `detect_dialect` for any corpus fixture's content (explicit regression test, 17 fixtures)

Task 2 acceptance criteria — all independently re-verified passing:
- `pytest tests/unit/detect/test_dialect.py -x -q` exits 0
- Fixture 66's round-trip test asserts the exact string `'a,b\nc"d'` for the payload field
- The literal `to_stdlib_dialect`/`CsvDialectDetectionError` reproduction snippet prints `ok`

Full `tests/unit` suite: 201 passed, 0 failed, 0 regressions.

---
*Phase: 06-universal-csv-engine-schema-contracts-normalization*
*Completed: 2026-08-15*
