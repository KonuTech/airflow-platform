---
phase: 06-universal-csv-engine-schema-contracts-normalization
plan: 11
subsystem: normalization
tags: [python, mypy, streaming-stage, unicode, nfc, boolean, null-tokens, dataplat]

# Dependency graph
requires:
  - phase: 06-universal-csv-engine-schema-contracts-normalization
    provides: "plan 06-02's diagnostics.py DIAGNOSTIC_CODES catalog (unmapped-boolean-token pre-seeded), NormalizationConfig fields (null_tokens/boolean_true_tokens/boolean_false_tokens)"
provides:
  - "NullTokenNormalizer — exact-match NULL-token replacement (never substring) producing the platform-wide None-as-absent convention"
  - "BooleanNormalizer — locale-specific true/false token mapping to Python bool, rejecting any unmapped value as unmapped-boolean-token, never defaulting"
  - "UnicodeNormalizer — unconditional NFC pass over every str field, the D-15 platform rule every hash computation must sit downstream of"
  - "RecordChunk.rows widened to str | bool | None — the shared type contract plans 06-09/06-10/06-16 build on"
affects: [06-09, 06-10, 06-16]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Per-column StreamingStage: one instance handles one column via column_index/column_name constructor injection, mirroring RaggedRowGuard exactly"
    - "Platform-wide None-as-absent convention: an absent field is Python None in the row tuple, documented once on RecordChunk and referenced by every later normalizer"
    - "None/non-str passthrough guard as the first branch in apply(), before any parse/transform logic"

key-files:
  created:
    - packages/dataplat/src/dataplat/normalize/boolean_null.py
    - packages/dataplat/src/dataplat/normalize/unicode.py
    - tests/unit/normalize/test_boolean_null.py
    - tests/unit/normalize/test_unicode.py
  modified:
    - packages/dataplat/src/dataplat/models/record.py
    - packages/dataplat/src/dataplat/pipeline/engine.py
    - packages/dataplat/src/dataplat/load/staging.py

key-decisions:
  - "Widened RecordChunk.rows's declared type to str | bool | None (not just str | None as the plan's literal wording said) so BooleanNormalizer's True/False outputs are honestly typed and comparable without mypy comparison-overlap errors downstream"
  - "UnicodeNormalizer guards on isinstance(field, str) rather than field is not None, so a bool field (BooleanNormalizer's output) is equally never passed to unicodedata.normalize(), which raises TypeError on any non-str argument"
  - "Defensively patched RaggedRowGuard's raw_line reconstruction (pipeline/engine.py) and StagingLoader's hash computation (load/staging.py, via a local cast) so both stay mypy-clean under the widened type, with zero runtime behavior change for today's all-str rows"

patterns-established:
  - "Pattern 1: Per-column normalizer stages take column_index/column_name/tokens via constructor keyword-only args, never read config from a global"
  - "Pattern 2: A None field is the platform's one absent-value representation everywhere in the pipeline, checked first in every apply() before any transform"

requirements-completed: [CSV-10, CSV-12, QUAL-04]

# Metrics
duration: ~35min (across two sessions; interrupted by a monthly spend-limit pause between the Task 1 and Task 2 commits, resumed cleanly with no rework)
completed: 2026-08-15
---

# Phase 6 Plan 11: Boolean/NULL + Unicode NFC Normalizers Summary

**NullTokenNormalizer, BooleanNormalizer and UnicodeNormalizer as exact-match, never-substring, never-silently-defaulting `StreamingStage`s, plus the platform-wide `None`-as-absent convention that widens `RecordChunk.rows` to `str | bool | None`**

## Performance

- **Duration:** ~35 min of active work (session interrupted by a monthly spend-limit error after Task 1's commit; resumed cleanly, no rework needed)
- **Started:** 2026-08-15 (base commit 517f3d2)
- **Completed:** 2026-08-15T13:39:45Z (commit d1db324)
- **Tasks:** 2 completed
- **Files modified:** 7 (4 created, 3 modified)

## Accomplishments

- `NullTokenNormalizer` replaces a field with `None` on exact NULL-token match only — proven against fixture 24, including the deliberate `"NULL Industries"` substring trap surviving as real data
- `BooleanNormalizer` maps locale-specific true/false tokens to Python `bool`, rejecting any unmapped value (`"Maybe"`, bare `"0"`/`"1"` with no declared tokens) as `unmapped-boolean-token` — never silently defaulting to `False` (the `"O"`-means-French-`Oui` inversion trap from fixture 60)
- `UnicodeNormalizer` applies an unconditional, non-configurable NFC pass (D-15) — proven to collapse fixture 44's NFC/NFD pair to byte-identical strings, and proven NOT to strip fixture 42's zero-width/bidi marks (that's explicitly out of scope)
- Established and documented the platform-wide `None`-as-absent convention on `RecordChunk` that plans 06-09/06-10/06-16 depend on, widening the row element type to `str | bool | None`

## Task Commits

Each task was committed atomically:

1. **Task 1: NullTokenNormalizer + BooleanNormalizer — exact-match tokens, never substring, never a default** - `fd3c97d` (feat)
2. **Task 2: UnicodeNormalizer — unconditional NFC, the hard ordering edge** - `d1db324` (feat)

**Plan metadata:** (this commit, following SUMMARY.md write)

## Files Created/Modified

- `packages/dataplat/src/dataplat/normalize/boolean_null.py` - `NullTokenNormalizer` + `BooleanNormalizer` `StreamingStage`s
- `packages/dataplat/src/dataplat/normalize/unicode.py` - `UnicodeNormalizer` `StreamingStage`
- `tests/unit/normalize/test_boolean_null.py` - Unit tests covering every behavior in Task 1's spec, both classes
- `tests/unit/normalize/test_unicode.py` - Unit tests covering every behavior in Task 2's spec
- `packages/dataplat/src/dataplat/models/record.py` - Widened `RecordChunk.rows` to `tuple[tuple[str | bool | None, ...], ...]`, documented why (plan action text explicitly required this; see Deviations)
- `packages/dataplat/src/dataplat/pipeline/engine.py` - Defensive fix: `RaggedRowGuard`'s `raw_line` reconstruction and `kept` list now tolerate the widened row type (zero behavior change for today's all-str rows)
- `packages/dataplat/src/dataplat/load/staging.py` - Local `cast()` narrowing `surviving_rows` back to `tuple[str, ...]` at the one call site that is still, today, guaranteed all-`str` (only `RaggedRowGuard` runs before it; normalizer wiring is plan 06-16)

## Decisions Made

- **Widened the shared type beyond the plan's literal wording.** The plan's action text said "widening `RecordChunk.rows`'s element type from `tuple[str, ...]` to `tuple[str | None, ...]`", but `BooleanNormalizer` (same task) also writes real Python `True`/`False` into the row. Declaring the type as only `str | None` would have made `bool`-typed reads elsewhere (including this plan's own tests, e.g. `result.chunk.rows[0][1] is True`) trip mypy's `strict_equality`/`comparison-overlap` check. Widened to `str | bool | None` instead — the accurate type for what this plan's own two tasks produce.
- **`UnicodeNormalizer` guards on `isinstance(field, str)`, not `field is not None`.** The plan's Task 2 behavior spec only names `None` explicitly, but its own stated rationale ("`unicodedata.normalize()` raises `TypeError` on a non-`str` argument") applies equally to the `bool` fields `BooleanNormalizer` (this same plan's Task 1) now produces. Implementing the narrower, literal guard would have left a live `TypeError` crash risk on any boolean column once plan 06-16 wires both stages into one pipeline. Chose the more defensive, still-fully-compliant implementation and added an extra test (`test_unicode_normalizer_passes_a_bool_field_through_unchanged_never_raises`) proving it.
- **Touched two files outside the frontmatter's declared `files_modified` list** (`pipeline/engine.py`, `load/staging.py`) because the plan's own `<action>` text explicitly instructed: "Check `RaggedRowGuard`'s and `StagingLoader`'s existing `tuple[str, ...]` assumptions elsewhere in this codebase and confirm nothing downstream... assumes every field is a `str` without checking." Running `mypy` after the type widening found two real, concrete breaks (both `str.join()` calls over what was now a wider-typed row) — documented in full below.
- **`CSV-10`'s requirement checkbox is now complete, but this plan only builds its boolean/NULL half.** The requirement text bundles numeric/boolean/NULL normalization into one line, and the plan's own frontmatter lists `CSV-10` as a completed requirement (as does sibling plan 06-10, which builds the numeric half). Followed the plan's declared `requirements:` field as instructed; flagging here so a reader of REQUIREMENTS.md knows the numeric half's evidence lives in plan 06-10's SUMMARY, not this one.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug, caused by this plan's own necessary type change] `RecordChunk.rows`'s widened type broke mypy in two files outside this plan's declared scope**
- **Found during:** Task 1, after widening `RecordChunk.rows` to `tuple[tuple[str | bool | None, ...], ...]` per the plan's own `<action>` text, then running `mypy packages/dataplat/src packages/csv-processor/src tools` to verify (as the same action text explicitly instructed: "confirm nothing downstream... assumes every field is a `str` without checking").
- **Issue:** Two existing call sites called `str.join()` (which requires `Iterable[str]`) directly over a `chunk.rows` row: `RaggedRowGuard.apply()`'s `RejectedRecord.raw_line` reconstruction (`pipeline/engine.py:97`), and `StagingLoader.load()`'s content-hash computation (`load/staging.py:210`). Both are Phase 3/4 code, both are outside this plan's declared `files_modified`, and both are guaranteed to see only `str` fields *today* (no normalizer is wired into the live pipeline yet — that's plan 06-16's job) — but the widened *static* type made mypy correctly flag that this is no longer provable from the type alone.
- **Fix:** `RaggedRowGuard.apply()` — its `raw_line` reconstruction now renders a non-`str` field defensively (`"" if field is None else str(field)`), byte-identical output for the all-`str` case that is the only case currently reachable. `StagingLoader.load()` — narrowed `surviving_rows` back to `tuple[str, ...]` via a local `cast()` (a true no-op at runtime) with a comment explaining the invariant holds because `stages=[RaggedRowGuard()]` is the only stage in that call, three lines above; chose `cast()` over rewriting the hash-computation line itself because the hash is genuinely sensitive, already-proven-at-10M-rows production code, and a `cast()` changes zero characters of that logic.
- **Files modified:** `packages/dataplat/src/dataplat/pipeline/engine.py`, `packages/dataplat/src/dataplat/load/staging.py`
- **Verification:** `mypy packages/dataplat/src packages/csv-processor/src tools` → `Success: no issues found in 58 source files`. `pytest tests/unit -x -q` → `172 passed` (includes `test_pipeline_errors.py`'s `RaggedRowGuard` tests, unchanged behavior). `load/staging.py` is covered by `tests/integration/test_staging_loader.py` (testcontainers/Docker-based, outside this plan's own `<verification>` block and not run this session); the fix there is a static-only `cast()` with proven zero runtime effect, so this is a low-risk, reasoned gap rather than a live-tested one.
- **Committed in:** `fd3c97d` (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (Rule 1/Rule 3 — a bug directly caused by this plan's own plan-instructed type widening, fixed to keep the mypy gate green)
**Impact on plan:** Necessary consequence of doing the plan's own Task 1 instruction correctly. Zero behavior change for any currently-possible input; both fixes are purely defensive/type-level. No scope creep beyond what the plan's own action text asked me to check.

## Issues Encountered

- **Monthly spend-limit termination between Task 1 and Task 2's commit.** Task 2's files (`unicode.py`, `tests/unit/normalize/test_unicode.py`) were already written and `git add`-staged when the session was terminated by a spend-limit error. On resume: re-read both staged files in full to confirm no truncation/corruption, re-ran `ruff check`/`mypy`/`pytest tests/unit` from scratch to confirm the environment was still green, then committed Task 2 exactly as planned — no rework was needed since the staged content was already correct and complete.
- **Zero-width/bidi test literals initially embedded raw invisible Unicode characters in the test source** (copy-pasted while drafting) instead of the explicit `\u200b`/`\u200e` escape-sequence text (backslash-u-2-0-0-b, six literal ASCII characters, not the code point itself) — caught before committing by inspecting the raw bytes (`cat -A`), and fixed via a small Python rewrite script rather than risking another copy-paste of the same invisible characters through an editor tool. Final committed source uses explicit escape-sequence text only, auditable in any diff viewer with no invisible bytes present.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `NullTokenNormalizer`, `BooleanNormalizer`, `UnicodeNormalizer` are unit-proven in isolation against every CSV-10-boolean/null and CSV-12-tagged corpus fixture (24, 60, 42, 44) and ready for plan 06-16 to wire into the real per-dataset stage sequence (`NullTokenNormalizer` before `BooleanNormalizer`/`DateNormalizer`/`NumericNormalizer` for the same nullable column; `UnicodeNormalizer` unconditionally last, before any hash computation).
- The platform-wide `None`-as-absent convention on `RecordChunk.rows` is now a settled, documented contract other Wave-2 plans (06-09 `DateNormalizer`, 06-10 `NumericNormalizer`) explicitly build on by name in their own plan text.
- No blockers. `packages/dataplat/src/dataplat/load/staging.py`'s integration-test coverage (testcontainers, Docker) was not re-run this session for the `cast()`-based fix there; recommend a normal `make test-integration` pass before/at the next full-suite gate, though the change is provably runtime-inert.

---
*Phase: 06-universal-csv-engine-schema-contracts-normalization*
*Completed: 2026-08-15*

## Self-Check: PASSED

- FOUND: `packages/dataplat/src/dataplat/normalize/boolean_null.py`
- FOUND: `packages/dataplat/src/dataplat/normalize/unicode.py`
- FOUND: `tests/unit/normalize/test_boolean_null.py`
- FOUND: `tests/unit/normalize/test_unicode.py`
- FOUND: commit `fd3c97d` (Task 1)
- FOUND: commit `d1db324` (Task 2)
