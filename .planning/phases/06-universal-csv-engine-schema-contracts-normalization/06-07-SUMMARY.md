---
phase: 06-universal-csv-engine-schema-contracts-normalization
plan: 07
subsystem: detection
tags: [type-inference, schema-bootstrap, csv, decimal, strptime, pydantic]

# Dependency graph
requires:
  - phase: 06-universal-csv-engine-schema-contracts-normalization
    provides: "06-02's ColumnContract (dataplat.config.model) — the round-trip target suggest_column_contracts's output dicts are proven to construct without raising"
provides:
  - "csv_processor.detect.schema.TypeInference — frozen dataclass carrying a suggested_type, confidence_evidence and red_flags tuple"
  - "csv_processor.detect.schema.infer_column_type(values, *, candidate_date_formats=()) — conservative, bootstrap-only type inference over one column's sampled raw string values"
  - "csv_processor.detect.schema.infer_schema(header, sample_rows) — infer_column_type applied per column, values gathered positionally"
  - "csv_processor.detect.schema.suggest_column_contracts(header, sample_rows) — shapes infer_schema's output into draft ColumnContract-field-matching dicts, a human-readable starting point never wired into any pipeline"
affects: [06-14 (wave-3 wiring plan that depends on 06-07 alongside 06-03..06-06/06-08), any future schema-evolution classifier needing an inference fallback signal]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Orchestrator function delegates to small `_infer_X(sample) -> TypeInference | None` helpers, each either committing to an answer or declining (returning None) so the next check in priority order gets a turn — keeps per-function branch/return counts low under ruff's PLR0911/PLR0912 and keeps each check's red-flag reasoning colocated with the check itself"
    - "A regex red flag (scientific notation) is matched with `.fullmatch()`, never `.search()`, specifically to avoid a substring false-positive (e.g. \"Room2E5\") triggering a red flag meant only for a value that IS, as a whole, shaped like the damage pattern"
    - "`str.isdecimal()`, not `str.isdigit()`, gates 'is this value made only of characters int() can consume' — isdigit() also accepts non-ASCII digit-like characters (e.g. superscripts) that int() cannot actually parse"

key-files:
  created:
    - packages/csv-processor/src/csv_processor/detect/schema.py
    - tests/unit/detect/test_schema.py
  modified: []

key-decisions:
  - "Boolean inference's closed set is deliberately whole-word-only ({true,false,yes,no,0,1}), excluding single-letter tokens like Y/N/O — corpus fixture 60_boolean_localized.csv proves single letters collide across locales (French \"O\" reads as English zero/off), and this function has no per-dataset locale context to disambiguate them. A distinctive word (true/false/yes/no) must also be present — a bare 0/1 sample infers integer, never boolean, per D-14/CSV-10."
  - "Scientific notation is a universal, unconditional red flag for both integer and decimal inference, not merely 'flagged when mixed with clean values' — a single scientific-notation-shaped value downgrades the whole sampled column to string even when a clean control value is present in the same sample, matching corpus fixture 50's declared outcome exactly."
  - "The leading-zero red flag is scoped to genuinely digit-only values (no decimal point) — a decimal value's own leading zero (e.g. \"0.50\") is ordinary notation, not evidence of a damaged identifier, so it is never flagged."
  - "infer_schema tolerates a sample row shorter than the header (skips that column for that row) instead of raising IndexError — messy, ragged sample data is exactly the kind of real-world input this bootstrap helper exists to survive without crashing."

requirements-completed: [SCHEMA-01, QUAL-04]

# Metrics
duration: ~25min
completed: 2026-08-15
---

# Phase 6 Plan 7: Conservative Type Inference (SCHEMA-01) Summary

**`infer_column_type` — a five-step (boolean → date/timestamp → integer → decimal → string) conservative type-inference function over sampled CSV column values, biased hard toward `string` via three named red flags (`leading-zero`, `scientific-notation`, `mixed-parseable`), plus `infer_schema`/`suggest_column_contracts` shaping its output into a draft `ColumnContract` starting point.**

## Performance

- **Duration:** ~25 min
- **Completed:** 2026-08-15
- **Tasks:** 2 completed
- **Files modified:** 2 (2 created, 0 modified)

## Accomplishments

- `TypeInference` (frozen dataclass) + `infer_column_type` implement SCHEMA-01's conservative inference exactly: `001234`-style zero-padded identifiers stay `string` (never silently become `integer`), a sample mixing scientific-notation-damaged values with a clean control value stays `string` (matching corpus fixture 50's declared, unrecoverable outcome), and a mixed decimal/non-decimal sample stays `string` rather than silently dropping the values it could not fit.
- Boolean inference never fires on a bare `0`/`1` sample (D-14/CSV-10's "1/0 must never become boolean absent evidence") — it requires at least one of `true`/`false`/`yes`/`no` to be present, and deliberately excludes single-letter tokens (`Y`/`N`/`O`) because corpus fixture 60 proves they collide across locales.
- Date/timestamp inference only ever confirms a caller-supplied candidate `strptime` format against every sampled value — it never guesses or invents one. A format string containing a time-of-day directive (`%H`/`%M`/`%S`/etc.) infers `timestamp`; every other fitting format infers `date`.
- `infer_schema`/`suggest_column_contracts` (Task 2) apply `infer_column_type` across a whole header/sample-rows pair and shape the result into dicts matching `ColumnContract`'s field names (`name`/`type`/`nullable`/`required`) — proven, by test, to construct a real `ColumnContract` without raising. Explicitly documented as a hand-authoring starting point, never wired into any pipeline.
- 23 unit tests: every `<behavior>` block case, plus corpus-grounded assertions generating the real corpus (via `tools.corpus.generators.generate_corpus`, fast profile) and reading actual values from `01_simple.csv`, `50_excel_scientific_notation_ids.csv`, and `60_boolean_localized.csv`.

## Task Commits

Each task was committed atomically:

1. **Task 1: infer_column_type — conservative type inference with named red flags** - `270c7e3` (feat)
2. **Task 2: Bootstrap suggestion shape — infer a whole row's column types together** - `d9e6a0d` (feat)

_(Worktree mode: this SUMMARY.md's own metadata commit follows, per orchestrator convention — no STATE.md/ROADMAP.md commit from this agent.)_

## Files Created/Modified

- `packages/csv-processor/src/csv_processor/detect/schema.py` - `TypeInference`, `infer_column_type`, `infer_schema`, `suggest_column_contracts`, and their private `_infer_*`/`_is_*`/`_has_*`/`_parses_*` helpers (344 lines)
- `tests/unit/detect/test_schema.py` - 23 tests: behavior-block cases, corpus-grounded assertions (fixtures 01/50/60), and the `infer_schema`/`suggest_column_contracts` round-trip proof (262 lines)

## Decisions Made

See `key-decisions` in frontmatter — all four are inline design choices made while implementing the plan's own `<action>` text (boolean closed-set scope, scientific-notation veto strength, leading-zero scope, ragged-row tolerance), not deviations from what the plan specified.

## Deviations from Plan

None - plan executed exactly as written. Two additions beyond the plan's literal `<behavior>` block, both within Rule 2 (robustness) and explicitly called out as deliberate, tested design choices rather than silent scope creep:

- An empty `values` sequence infers `"string"` with red flag `"empty-sample"` (not in the plan's `<behavior>` block, but the plan's own `TypeInference.red_flags` docstring phrasing ("e.g." — not an exhaustive list) covers additional named reasons; a crash or `None`-typed result on empty input would violate the "never authoritative, always answers with a suggestion" contract this module exists to guarantee).
- `infer_schema` tolerates a sample row shorter than `header` rather than raising `IndexError` — messy, ragged sample data is exactly the kind of real-world input a bootstrap detection helper must survive.

Both are covered by dedicated tests (`test_empty_sample_infers_string`, `test_infer_schema_tolerates_a_ragged_sample_row_without_raising`).

## Issues Encountered

None. The worktree's `.venv` did not exist at session start; `uv run --frozen` created it fresh and its editable installs for `dataplat`/`csv_processor` were verified (via `.venv/lib/python3.12/site-packages/_editable_impl_{dataplat,csv_processor}.pth`) to point at this worktree's own paths, not the main tree — the stale-editable-install issue a sibling plan (06-02) flagged did not reproduce here, most likely because no pre-existing `.venv` was inherited into this worktree.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `infer_column_type`/`TypeInference`/`infer_schema`/`suggest_column_contracts` are all importable from `csv_processor.detect.schema` exactly as this plan's `<interfaces>`/acceptance criteria specify.
- No `dateutil` reference anywhere in either file (verified via `grep`), and the module is never called from any load-path code in this plan — it is purely a standalone, addressable bootstrap helper, matching SCHEMA-01/SCHEMA-02's "contract wins" locked design.
- Ready for 06-14 (the wave-3 wiring plan) or any future schema-evolution classifier to import `infer_column_type` as a fallback signal for a column a contract does not yet declare, per 06-CONTEXT.md's own framing.
- Full repo-wide `ruff check .`, `mypy` (strict, all 57 source files), `ruff format --check`, and `lint-imports` (both contracts kept) all pass with this plan's files included. Full `tests/unit` suite: 181 passed.

---
*Phase: 06-universal-csv-engine-schema-contracts-normalization*
*Completed: 2026-08-15*
