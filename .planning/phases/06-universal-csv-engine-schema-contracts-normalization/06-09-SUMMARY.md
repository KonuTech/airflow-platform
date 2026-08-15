---
phase: 06-universal-csv-engine-schema-contracts-normalization
plan: 09
subsystem: etl-normalization
tags: [datetime, zoneinfo, pep495, strptime, dst, streaming-stage, hypothesis]

# Dependency graph
requires:
  - phase: 06-02
    provides: "diagnostics.py's DIAGNOSTIC_CODES catalog and StreamingStage/RaggedRowGuard conventions this plan mirrors exactly"
provides:
  - "DateNormalizer(StreamingStage) — explicit-format date/timestamp parsing, two-digit-year pivot, spreadsheet-serial epoch arithmetic, DST gap/overlap classification"
  - "classify_naive_local(naive, zone) — the verified zoneinfo+PEP495 fold classification primitive, reusable by any future timezone-aware stage"
  - "One new diagnostic code: ambiguous-local-time-requires-a-declared-fold-policy"
affects: [06-16, phase-9-cdc-scd]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "StreamingStage normalizer shape: name class attr + apply(ctx, chunk) -> StageResult, never raises for a row-level problem, constructor-injected runtime params (mirrors RaggedRowGuard exactly)"
    - "cast('T', ...) narrowing at the exact point a value's real runtime type is wider than a shared model's declared type (RecordChunk.rows' None-carrying convention), rather than widening the shared model out of this plan's file scope"

key-files:
  created:
    - packages/dataplat/src/dataplat/normalize/dates.py
    - tests/unit/normalize/test_dates.py
    - tests/property/test_dst_correctness.py
  modified:
    - packages/dataplat/src/dataplat/diagnostics.py

key-decisions:
  - "raw_line on a DateNormalizer RejectedRecord is the single invalid field value, not the whole reconstructed row (unlike RaggedRowGuard) — no field_delimiter param exists on this stage, and reconstructing a full row risks crashing on a sibling already-None-normalized column in the same row"
  - "A format with time components (%H/%M/%S) and no %z/%Z and no declared timezone is unconditionally rejected as naive-timestamp-without-a-declared-zone — the platform never defaults an instant's zone, matching fixture 56's explicit framing"
  - "New diagnostic code ambiguous-local-time-requires-a-declared-fold-policy added to diagnostics.py's pre-seeded catalog, authorized explicitly by this plan's own Task 2 text since the ten pre-seeded new-this-phase codes did not cover the ambiguous-without-policy case distinctly"

requirements-completed: [CSV-09, QUAL-17, QUAL-04]

duration: ~15min active (across two sessions; see Issues Encountered)
completed: 2026-08-15
---

# Phase 6 Plan 9: Explicit-Format Date/Timestamp Normalizer Summary

**`DateNormalizer` StreamingStage — strptime-only date/timestamp parsing with contract-declared two-digit-year pivots, spreadsheet-epoch arithmetic, and zoneinfo+PEP495 DST gap/overlap classification, proven against 8 corpus fixtures plus a Hypothesis property test.**

## Performance

- **Duration:** ~15 min of active tool-execution time (git commit timestamps span 2026-08-15T12:03Z–13:48Z UTC, but execution was interrupted by a monthly spend-limit error between Task 1's RED and GREEN commits and resumed by a continuation agent — see Issues Encountered)
- **Started:** 2026-08-15T10:03:10Z
- **Completed:** 2026-08-15T13:48:01Z
- **Tasks:** 3
- **Files modified:** 4

## Accomplishments

- `DateNormalizer(StreamingStage)` parses a contract-declared column as either a rendered `strptime`-format date/timestamp or a bare spreadsheet-serial integer under a declared epoch — never both, never neither, never a guessed format
- Invalid dates (`2026-02-30`, `31/02/2026`, `not-a-date`) always produce an explicit `RejectedRecord(error_type="invalid-calendar-date")`, never a coerced or silently-accepted value
- Two-digit years are resolved only via a contract-declared pivot (`_apply_two_digit_year_pivot`), re-deriving the century from `strptime`'s own parsed value rather than trusting CPython's inherited default — a missing pivot fails loudly at construction time, before any row is processed
- Excel/Lotus 1900-epoch spreadsheet serials resolve correctly on both sides of the phantom 1900-02-29 leap day (serial 60 rejected outright); the 1904 epoch produces a different, correct date for the identical serial, proving the epoch — not the serial — decides
- `classify_naive_local` (copied verbatim from 06-RESEARCH.md's verified code, reproducing every value in corpus fixture 55 exactly) classifies a naive local datetime as nonexistent/ambiguous/unambiguous; wired into `DateNormalizer` via a new `timezone`/`ambiguous_time_policy` constructor axis
- A nonexistent local time (DST spring-forward gap) is always rejected; an ambiguous local time (DST autumn-overlap) is rejected by default and only resolves under an explicitly declared `"earliest"`/`"latest"` fold policy — never silently takes the first fold
- A naive timestamp column with no declared zone and no self-describing `%z`/`%Z` offset is always rejected as `naive-timestamp-without-a-declared-zone`; an offset-aware format (`%z`, including the `"Z"` designator) resolves directly to UTC without needing `timezone` declared at all
- A `None` field (already null-normalized upstream) passes through unchanged in all three branches (plain format, spreadsheet epoch, timezone-aware) — never parsed, never rejected
- QUAL-17's Hypothesis property test proves the DST gap/overlap classification generally across arbitrary generated local times in two bounded four-hour windows around Europe/Warsaw's real 2026 transitions, not just the corpus's three fixed rows — empirically verified non-vacuous (both classification outcomes occur in each window) before being committed

## Task Commits

Each task was committed atomically, following RED → GREEN for the two `tdd="true"` tasks:

1. **Task 1: DateNormalizer — explicit strptime parsing, invalid-date rejection, two-digit-year pivot, spreadsheet epoch**
   - `f182c5d` (test — RED): failing tests for fixtures 22/23/52/53/54/59
   - `f646a68` (feat — GREEN): `DateNormalizer` plain-format and spreadsheet-serial branches
2. **Task 2: Naive-timestamp DST classification (fixtures 55/56) — wired into DateNormalizer**
   - `15b6674` (test — RED): failing tests for fixtures 55/56
   - `a28aa3b` (feat — GREEN): `classify_naive_local`, `timezone`/`ambiguous_time_policy`, new diagnostic code
3. **Task 3: QUAL-17's DST-correctness property test** - `7f27e89` (test)

**Plan metadata:** this commit (docs: complete plan) — see final commit below

_TDD gate sequence verified: each `test(...)` commit precedes its `feat(...)` commit for both TDD tasks; no REFACTOR commit was needed (code was ruff/mypy-clean at each GREEN commit)._

## Files Created/Modified

- `packages/dataplat/src/dataplat/normalize/dates.py` - `DateNormalizer(StreamingStage)`, `classify_naive_local`, and the private pivot/epoch/field-replacement helpers
- `packages/dataplat/src/dataplat/diagnostics.py` - added `"ambiguous-local-time-requires-a-declared-fold-policy"` to the pre-seeded `_NEW_THIS_PHASE_CODES` catalog
- `tests/unit/normalize/test_dates.py` - 31 unit tests covering every fixture-driven and constructor-validation behavior in the plan
- `tests/property/test_dst_correctness.py` - 2 Hypothesis property tests generalizing DST classification across arbitrary local times

## Decisions Made

- **`RejectedRecord.raw_line` holds the single invalid field, not the whole row.** `RaggedRowGuard` reconstructs `raw_line` by rejoining an entire row with a configured `field_delimiter`; `DateNormalizer` has no such delimiter parameter (not specified anywhere in the plan's interfaces), and reconstructing a full row risks a `TypeError` if a sibling column in the same row is already `None`-normalized by an upstream stage. Using just the offending field's raw string is simpler, always safe, and the plan's own acceptance criteria never assert on `raw_line`'s exact content.
- **`_parse_plain_format` unconditionally rejects a time-bearing naive result with no declared zone.** Rather than adding an opt-out, any column whose format has `%H`/`%M`/`%S` but neither `%z`/`%Z` nor a declared `timezone` is always refused — matching fixture 56's explicit framing ("must not inherit... must not default to UTC... must not default to the server's zone") as an unconditional platform rule, not a per-column toggle.
- **A defensive `__init__` guard rejects `timezone` combined with a `%z`/`%Z` format** (not explicitly required by any fixture, but a Rule 2 correctness addition): declaring both would silently overwrite the format's own parsed offset with the declared zone, a confusing and easy-to-misconfigure combination with no legitimate use case in this platform's design.
- **The new diagnostic code lives in `diagnostics.py`'s pre-seeded catalog**, even though that file is outside this plan's `files_modified` frontmatter — explicitly pre-authorized by Task 2's own action text ("add ONE new kebab-case entry there in this same task, since no other Wave 2 plan is concurrently editing diagnostics.py's exact same lines for this specific case").

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `RecordChunk.rows`'s declared type (`tuple[str, ...]`) is narrower than the None-carrying runtime convention this plan requires**
- **Found during:** Task 1, first `mypy` run against the source file
- **Issue:** `RecordChunk.rows` is declared `tuple[tuple[str, ...], ...]` (Phase 3, unmodified this phase). Reading a field and checking `is None` against it is statically impossible under that type, so mypy strict's `warn_unreachable` flagged the `None`-passthrough branch (and, in tests, every line following an `is None` assertion) as unreachable.
- **Fix:** Used `cast("str | None", row[self.column_index])` at the exact point the value is read, in both `dates.py` and `test_dates.py`, making the narrower-than-declared runtime reality explicit for mypy instead of silently widening the shared `RecordChunk` model (out of this plan's file scope, and a likely point of contention with sibling Wave 2 normalizer plans touching the same file).
- **Files modified:** `packages/dataplat/src/dataplat/normalize/dates.py`, `tests/unit/normalize/test_dates.py`
- **Verification:** `mypy` clean on both files; `pytest` green
- **Committed in:** `f646a68`, `15b6674` (part of each task's commit)

**2. [Rule 1 - Bug] `# type: ignore[code] -- comment` syntax is invalid for mypy 2.3.0's parser**
- **Found during:** Task 1, mypy run against the new test file (mirroring `tests/unit/test_pipeline_errors.py`'s existing comment style)
- **Issue:** mypy 2.3.0 rejects a trailing free-text comment after `# type: ignore[code]` unless it starts with a second `#`. The pre-existing `tests/unit/test_pipeline_errors.py` has this exact same defect (confirmed by running mypy against it directly) — a pre-existing, out-of-scope bug, not something this plan introduces.
- **Fix:** Used `# type: ignore[code]  # comment` (two `#`s) in this plan's own new test file only. `tests/unit/test_pipeline_errors.py` was left untouched (out of scope) and is logged below as a deferred item.
- **Files modified:** `tests/unit/normalize/test_dates.py`
- **Verification:** `mypy tests/unit/normalize/test_dates.py` and `mypy tests/property/test_dst_correctness.py` both clean
- **Committed in:** `f182c5d`

---

**Total deviations:** 2 auto-fixed (both Rule 1 — bugs surfaced by the actual mypy/ruff toolchain, not guessed)
**Impact on plan:** Both fixes are narrow, type-checker-only corrections with no behavioral change. No scope creep — `models/record.py` was deliberately left untouched per the plan's own file scope.

## Issues Encountered

- **Mid-execution interruption.** This session was terminated partway through Task 1's GREEN implementation by a monthly spend-limit error (unrelated to this plan's content), then resumed by a continuation agent starting from the verified git/working-tree state (1 RED commit made, `dates.py` implementation in progress but uncommitted). The continuation agent independently re-verified the coordinator's resume message against actual `git log`/`git status` output before proceeding, found it fully consistent, and continued the same RED→GREEN sequence without redoing any already-correct work. No functional impact; noted here only because it explains the gap between the first and remaining commit timestamps.
- **`packages/dataplat/pyproject.toml`'s `mypy` `TYPECHECK_PATHS` (Makefile) never includes `tests/`.** Confirmed by reading the `Makefile` directly: the project's real `make typecheck` gate only scans `packages/dataplat/src`, `packages/csv-processor/src`, and `tools`. Test-file mypy compliance (this plan achieved it anyway, for both new test files) is good hygiene but not itself gated — worth knowing so a future plan doesn't assume otherwise.
- **Worktree base drift.** At startup, this worktree's HEAD (`54c8822`, Phase 5 completion) was behind the orchestrator's declared base commit (`517f3d2`, "docs(phase-06): update tracking after wave 1"). Per the executor's own `worktree_branch_check` protocol, `git merge-base` confirmed HEAD was a strict ancestor of the expected base (working tree clean, no risk of losing local commits), so `git reset --hard` to the expected base was the explicitly sanctioned recovery — not a deviation, the documented procedure working as intended.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `DateNormalizer` and `classify_naive_local` are ready to be wired into the live pipeline stage sequence for `customers.birth_date`/`event_ts` — explicitly deferred to plan 06-16 (this plan proves the normalizer correct in isolation, per its own Objective).
- `classify_naive_local` is now a reusable, independently-tested primitive any future timezone-aware stage (e.g., Phase 9/10's CDC/SCD event-time handling) can import directly from `dataplat.normalize.dates`.
- Fixture 52's ambiguous-format behavior is proven at the `DateNormalizer` parsing level (same raw values, two different individually-correct outputs depending on the declared format) but format *detection/decline* itself is explicitly a different plan's territory (CSV-05/schema-profiling) — not started here, not blocking.
- No blockers for sibling Wave 2 plans: `models/record.py` and `pipeline/engine.py` were read but not modified, and the one cross-file edit (`diagnostics.py`'s new catalog entry) was pre-authorized by this plan's own text specifically to avoid a Wave 2 merge collision.

---
*Phase: 06-universal-csv-engine-schema-contracts-normalization*
*Completed: 2026-08-15*
