---
phase: 06-universal-csv-engine-schema-contracts-normalization
plan: 10
subsystem: normalization
tags: [decimal, csv-10, numeric-normalization, locale-parsing, streaming-stage, dataplat]

# Dependency graph
requires:
  - phase: 06 (wave 1, plan 06-02)
    provides: RaggedRowGuard/StreamingStage shape (pipeline/engine.py, pipeline/protocol.py) this plan mirrors exactly, plus the DIAGNOSTIC_CODES catalog (scientific-notation-identifier-unrecoverable, fixed-width-identifier-below-declared-width, invalid-numeric-value) and NormalizationConfig/ColumnContract fields this stage's constructor consumes
provides:
  - "NumericNormalizer: a StreamingStage doing locale-aware decimal.Decimal parsing (comma/point decimal separators, thousands-separator grouping), accounting negative-style handling (parentheses/trailing-minus/leading-minus), currency-symbol and percent stripping"
  - "Two UNRECOVERABLE-damage rejections that never silently coerce: scientific-notation identifier truncation and fixed-width leading-zero stripping"
  - "Contract-declared numeric null sentinels (exact-match only) and None-field passthrough for the platform-wide null convention"
  - "tests/unit/normalize/test_numeric.py: every CSV-10-numeric corpus fixture (20, 21, 50, 51, 57, 58, 59) proven against its own tests/fixtures/corpus.yaml expect: block"
affects: [06-16 (stage-ordering/pipeline assembly consumes this StreamingStage), 06-11 (sibling plan; None-convention/RecordChunk widening this stage is written to be forward-compatible with)]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "StreamingStage constructor-injection: every locale/contract parameter arrives via keyword-only constructor args, already resolved to one column by the pipeline assembler (RaggedRowGuard precedent) -- never a config object or global read"
    - "object-typed read + cast('str', ...) narrow for a RecordChunk field that may already be None (upstream NullTokenNormalizer, plan 06-11's not-yet-merged type widening) -- avoids a stale # type: ignore that would trip mypy's warn_unused_ignores once 06-11 lands"
    - "Percent-as-fraction handled as a two-phase operation: '%' stripped and recorded as a flag during string cleaning, then an actual Decimal division by 100 inside the same localcontext() block used for the parse itself -- never a naive string/decimal-point shift"

key-files:
  created:
    - packages/dataplat/src/dataplat/normalize/numeric.py
    - tests/unit/normalize/test_numeric.py
  modified: []

key-decisions:
  - "Both plan tasks (locale/negative-style parsing, then unrecoverable-damage rejection + null sentinels + None passthrough) landed in a single commit rather than two -- the tasks interleave inside the same apply() guard sequence in the same two files, and were built as one cohesive unit in this session; splitting them after the fact would mean fabricating an artificial diff, not reconstructing real history."
  - "_replace_field (module-level helper) is the ONE place a RecordChunk.rows-widening type mismatch is bridged with a single, well-commented # type: ignore[arg-type] -- RecordChunk.rows stays tuple[str, ...] in this plan's own file scope (record.py is plan 06-11 Task 1's file, out of scope here); every other None-touching read site uses an object-typed local + cast('str', ...) instead, so mypy strict stays clean with zero other ignores."
  - "Scientific-notation and fixed-width guards run against the RAW field value (before currency/negative-style/separator cleaning) -- matches the plan's literal guard ordering (0 None -> 1 null-sentinel -> 2 scientific-notation -> 3 fixed-width -> 4 decimal parse) and keeps the two UNRECOVERABLE-damage checks independent of a column's locale profile."

patterns-established:
  - "Pattern: normalize/*.py StreamingStage None-handling -- read the field through an `object`-typed local, `is None` check, then `cast('str', ...)` to narrow -- forward-compatible with plan 06-11's RecordChunk.rows widening with no ignore needed on the read side."

requirements-completed: [CSV-10, QUAL-04]

# Metrics
duration: unavailable -- session was interrupted mid-task by a monthly spend-limit error and resumed; no reliable start timestamp survived the interruption
completed: 2026-08-15
---

# Phase 06 Plan 10: NumericNormalizer Summary

**CSV-10's numeric half: a StreamingStage doing locale-aware `decimal.Decimal` parsing (comma/point separators, accounting negative styles, currency/percent), plus two unrecoverable-damage rejections (scientific-notation identifiers, fixed-width leading-zero loss) that reject rather than silently coerce.**

## Performance

- **Duration:** unavailable (see frontmatter `duration` note -- the executing session was terminated mid-task by a monthly spend-limit error, cleared, and resumed from the untracked, already-substantially-complete working files; no reliable elapsed-time signal survived that gap)
- **Completed:** 2026-08-15
- **Tasks:** 2 completed (both landed in one commit -- see Task Commits and Decisions Made)
- **Files modified:** 2 (both newly created)

## Accomplishments

- `NumericNormalizer` (`packages/dataplat/src/dataplat/normalize/numeric.py`): a `StreamingStage` mirroring `RaggedRowGuard`'s exact shape and constructor-injection convention, doing locale-aware `Decimal` parsing with a stage-local `decimal.Context` (never the global/thread-local default), accounting negative-style rewriting, currency/percent stripping, and two named UNRECOVERABLE-damage rejections.
- Every CSV-10-numeric-tagged corpus fixture (20, 21, 50, 51, 57, 58, 59) passes against its own `tests/fixtures/corpus.yaml` `expect:` block, proven in `tests/unit/normalize/test_numeric.py` (15 tests).
- Fixture 20/21 cross-equality explicitly proven: the comma-locale and point-locale files denote the SAME four `Decimal` quantities, not just individually-plausible ones.
- Fixture 57's naive-implementation trap explicitly disproven in-test: a strip-non-numeric-characters approach would sign-flip `"(123.45)"`/`"123.45-"` into `Decimal("123.45")`; this implementation's actual output (`Decimal("-123.45")`) is asserted to differ from that wrong value, not just to equal the right one.
- Fixture 50 ("THE MOST IMPORTANT DECLARATION IN THIS PLAN") and 51 both reject rather than coerce, and the tests assert the wrongly-expanded/re-padded values never appear anywhere in the `RejectedRecord`, as if silently accepted.
- Fixture 59's null-sentinel matching proven EXACT-match-only: `"-1.50"` is never treated as absent by the declared `"-1"` sentinel (no substring/prefix match), and `"0"` stays a real value unless explicitly declared.
- A field already `None` (the platform-wide null convention plan 06-11 Task 1 formalizes) passes through `apply()` unchanged -- never crashing, never coerced to `"0"`/`Decimal("0")`, never re-rejected -- proven with a synthetic row constructed exactly as an upstream `NullTokenNormalizer` would leave it.

## Task Commits

Both plan tasks were implemented and verified together in this session and landed in one commit (see Decisions Made for why they were not split into two):

1. **Task 1 + Task 2: NumericNormalizer (locale/negative-style parsing, unrecoverable-damage rejection, null sentinels, None passthrough)** - `3b298bc` (feat)

_No separate plan-metadata commit: SUMMARY.md is committed in the worktree-mode metadata commit that follows this one (STATE.md/ROADMAP.md are excluded and owned by the orchestrator after wave merge)._

## Files Created/Modified

- `packages/dataplat/src/dataplat/normalize/numeric.py` - `NumericNormalizer` `StreamingStage`: locale-aware `Decimal` parsing, negative-style/currency/percent handling, scientific-notation and fixed-width unrecoverable-damage rejection, null-sentinel and `None`-passthrough handling
- `tests/unit/normalize/test_numeric.py` - 15 tests covering every CSV-10-numeric-tagged corpus fixture (20, 21, 50, 51, 57, 58, 59) plus two supporting cases (percent_as_fraction=False, an unparseable value)

## Decisions Made

- **Single commit for both tasks.** Task 1 (locale/negative-style parsing) and Task 2 (unrecoverable-damage rejection, null sentinels, `None` passthrough) interleave inside the same `apply()` guard sequence in the same two files. Both were built together in this session; the commit reflects that real history rather than an artificially reconstructed two-commit split. Both tasks' behavior is independently verifiable in the test file (grouped under `# --- Task 1 ...` / `# --- Task 2 ...` section comments) and both are covered in full.
- **`_replace_field` is the single, deliberately-scoped bridge for a cross-plan type tension.** `RecordChunk.rows` (`dataplat.models.record`, owned by sibling plan 06-11 Task 1, out of this plan's file scope) is still typed `tuple[tuple[str, ...], ...]` in this worktree. Plan 06-11 widens it to `tuple[str | None, ...]` platform-wide once merged. Rather than leave the plan's explicit `None`-passthrough/null-sentinel requirements unimplemented, or scatter `# type: ignore` comments, every READ of a possibly-`None` field goes through an `object`-typed local + `cast("str", ...)` narrow (zero ignores needed, correct under both the current and the post-merge type), and the one WRITE site that can produce a `None` field (`_replace_field`) carries a single, thoroughly-commented `# type: ignore[arg-type]`. Verified clean: `mypy packages/dataplat/src packages/csv-processor/src` reports zero issues across all 48 source files with this change in place.
- **Percent-as-fraction is a two-phase operation, not a string-level shift.** The trailing `%` is stripped and recorded as a flag during string cleaning (alongside currency/negative-style/separator cleaning); the actual division by 100 happens on the parsed `Decimal`, inside the same `localcontext()` block used for the parse itself. This keeps rounding/precision/traps consistent between the parse and the percent division, and was verified against fixture 58's three percent rows (`"12,5 %"` -> `0.125`, `"7,25 %"` -> `0.0725`, `"100 %"` -> `1`).
- **Guard order matches the plan's literal sequence exactly:** (0) `None` passthrough, (1) null-sentinel exact match, (2) scientific-notation rejection, (3) fixed-width rejection, (4) locale-aware `Decimal` parse. Guards 2/3 run against the RAW field value (before currency/negative-style/separator cleaning), matching fixtures 50/51's raw values directly and keeping the two UNRECOVERABLE-damage checks independent of a column's locale profile.
- **No `field_delimiter` constructor parameter.** Unlike `RaggedRowGuard`, this plan's own `<interfaces>`/`<acceptance_criteria>` pin an exact 10-keyword-only-parameter constructor with no delimiter parameter. `RejectedRecord.raw_line` reconstruction uses a hardcoded `","` (D-01's dialect convention, matching `RaggedRowGuard`'s own default), documented at its module-level constant rather than threaded through the constructor.

## Deviations from Plan

None - plan executed exactly as written. (The single-commit-for-two-tasks choice above is a commit-granularity practicality, not a scope or behavior deviation -- every task requirement, interface, and acceptance criterion in `06-10-PLAN.md` is implemented and independently tested.)

## Issues Encountered

- **Shared-venv editable-install staleness (documented, known environment issue).** The activated `.venv` (`VIRTUAL_ENV=/home/konutec/projects/airflow-platform/.venv`) has an editable install of `dataplat` pointing at the MAIN tree's absolute path (`_editable_impl_dataplat.pth` -> `/home/konutec/projects/airflow-platform/packages/dataplat/src`), not this worktree's copy. Confirmed directly (the main tree's `dataplat/normalize/` has no `numeric.py` at all) and worked around throughout by prefixing `PYTHONPATH=packages/dataplat/src:packages/csv-processor/src` on every `pytest`/`mypy`/`python -c` invocation, per this session's own environment-issue note.
- **mypy strict tension on `tests/` (informational, not a gate).** Running `mypy` directly against `tests/unit/normalize/test_numeric.py` surfaces `Invalid "type: ignore" comment [syntax]` on the `_make_context()` helper's `# type: ignore[arg-type] -- ...` lines. Confirmed this is a PRE-EXISTING pattern already present, identically, in `tests/unit/test_pipeline_errors.py` (Phase 3, already-shipped code) -- not something this plan introduced. Confirmed the project's actual gate (`make typecheck` / `TYPECHECK_PATHS` in the `Makefile`) only covers `packages/dataplat/src`, `packages/csv-processor/src`, and `tools` -- `tests/` is deliberately excluded, so this is a pre-existing, out-of-gate-scope condition, left as-is to match established precedent rather than deviate from the copied `_make_context()` pattern the plan explicitly directs mirroring. The `_chunk()` test helper's own "list is invariant" mypy friction (a genuine, low-risk issue in code this plan DID own) was fixed by widening its parameter to `Sequence[tuple[str, ...]]` (covariant), consistent with mypy's own suggested fix.
- **Session interruption (monthly spend-limit error, not a real failure).** Reported by the coordinator: the executing agent was terminated mid-fix (partway through resolving the `_chunk()`/`numeric.py` mypy issues above), then resumed. On resume, both working files were found intact and untracked (no commits had landed yet), matching exactly the state described in the coordinator's resume message. Verified the worktree branch/base were still correct (`git merge-base --is-ancestor` against the expected wave-2 base commit) before continuing, then finished the mypy fixes, re-ran the full verification suite (pytest, ruff, mypy) from scratch, and committed.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `NumericNormalizer` is ready to be wired into the pipeline's `stages` sequence by plan 06-16 (stage-ordering/assembly), alongside `dates.py`/`boolean_null.py`/`unicode.py` from sibling wave-2 plans.
- This stage's `None`-passthrough behavior is written to be forward-compatible with plan 06-11 Task 1's `RecordChunk.rows` type widening and `NullTokenNormalizer` ordering guarantee (plan 06-16) -- no further code change in `numeric.py` should be needed once both land; only the one `# type: ignore[arg-type]` in `_replace_field` is expected to become unnecessary (a trivial cleanup, not a behavior change).
- No blockers. `customers` (this phase's only live dataset) has no numeric/currency columns (D-12's consequence, noted in the plan objective), so this capability's only evidence is its own corpus-fixture-driven unit tests -- exactly as the plan anticipated; a future multi-dataset phase exercising a real numeric column in production would be the first live-wiring proof beyond these tests.

---
*Phase: 06-universal-csv-engine-schema-contracts-normalization*
*Completed: 2026-08-15*
