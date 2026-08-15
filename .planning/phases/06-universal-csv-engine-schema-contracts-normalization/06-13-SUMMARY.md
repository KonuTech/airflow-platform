---
phase: 06-universal-csv-engine-schema-contracts-normalization
plan: 13
subsystem: schema
tags: [schema-evolution, dlt-3x4-matrix, pure-function, errors-as-values, dataplat]

# Dependency graph
requires:
  - phase: 06 (plan 06-02)
    provides: "IncompatibleSchemaError (dataplat.errors), DIAGNOSTIC_CODES (schema-column-disappeared/schema-column-retyped), ColumnContract shape, DatasetConfig.schema_evolution_on_* policy fields"
provides:
  - "classify_schema_change — pure, DB-independent compatible/breaking schema-evolution classifier (the dlt 3x4-matrix columns/data_type half)"
  - "SchemaChangeFinding — errors-as-values value object for the COMPATIBLE (column_added) outcome"
affects: ["06-15 (wires schema versioning/evolution into CsvSource.inspect())"]

# Tech tracking
tech-stack:
  added: []
  patterns: ["dlt 3x4 schema-contract matrix (columns/data_type half, evolve vs freeze)", "errors-as-values for COMPATIBLE / raise for BREAKING (QUAL-03 split), mirrored at schema level from RejectedRecord/DataPlatformError"]

key-files:
  created: [packages/dataplat/src/dataplat/schema/evolution.py, tests/unit/schema/test_evolution.py]
  modified: []

key-decisions:
  - "Compare old_columns/new_columns by name, never by position, so a reordered-but-otherwise-identical column list is correctly classified as no change (SCHEMA-04's 'reordered' case falls out of a name-keyed lookup with no extra code)"
  - "Check every old column for a breaking condition (disappearance or retype) BEFORE computing or returning any compatible finding, iterating old_columns in given order, so a rename's coincidental addition never rescues the disappearance it structurally is (D-02 dominance) and the tie-break between two different breaking columns is deterministic"

patterns-established:
  - "Pure per-file classifier with zero shared state proves D-05 (no cross-file blocking) by construction and by a dedicated back-to-back-calls test, not by any run-level gating code"

requirements-completed: [SCHEMA-04, SCHEMA-05, QUAL-12, QUAL-04]

# Metrics
duration: ~10min
completed: 2026-08-15
---

# Phase 06 Plan 13: Schema Evolution Classification Summary

**`classify_schema_change` — a pure dlt-3x4-matrix compatible/breaking classifier comparing two column lists by name, returning `SchemaChangeFinding` values for new columns and raising `IncompatibleSchemaError` for disappearance/rename/retype.**

## Performance

- **Duration:** ~10 min
- **Started:** 2026-08-15T09:58:02Z
- **Completed:** 2026-08-15T10:06:02Z
- **Tasks:** 2 completed
- **Files modified:** 2 (both created)

## Accomplishments
- `dataplat/schema/evolution.py`: `classify_schema_change(old_columns, new_columns) -> list[SchemaChangeFinding]`, the dlt 3x4-matrix `columns`/`data_type` classifier (SCHEMA-04/SCHEMA-05), pure and DB-independent per the plan's objective
- A genuinely new column is classified COMPATIBLE (`SchemaChangeFinding(change_type="column_added", ...)`), never raises — D-01
- Disappearance, rename (structurally indistinguishable from disappearance + coincidental addition) and retype are all classified BREAKING and raise `IncompatibleSchemaError` before any row is staged — D-02/D-04, with `context["diagnostic_code"]` (`"schema-column-disappeared"`/`"schema-column-retyped"`) and the offending column (plus both types for a retype)
- D-05 (no cross-file blocking) documented in the module docstring and proven by a dedicated test showing two back-to-back calls — one breaking, one compatible, in both orders — never contaminate each other
- QUAL-12's compatible-and-breaking coverage made explicitly locatable via `test_compatible_change_is_tested`/`test_breaking_change_is_tested`, plus a comment distinguishing this function's header-level column-list scope from `16_extra_columns.csv`'s row-level surplus-field scope (a different concern owned by `RaggedRowGuard`)
- 9 unit tests total, all green; ruff (`select = ["ALL"]`) and mypy `--strict` both clean on both files

## Task Commits

Each task was committed atomically (Task 1 used TDD: RED then GREEN):

1. **Task 1a (RED): failing test for schema evolution classifier** - `c69c6c8` (test)
2. **Task 1b (GREEN): implement schema evolution classifier** - `7d704b7` (feat)
3. **Task 2: D-05 statelessness proof + QUAL-12 coverage sweep** - `bdc684b` (test)

**Plan metadata:** commit hash filled in after this SUMMARY is committed (see below).

_Task 1 (`tdd="true"`) produced two commits (RED, GREEN); no REFACTOR commit was needed — ruff/mypy were already clean after GREEN. Task 2 (`type="auto"`, no tdd) produced one commit since it only added a docstring note and tests, no behavior change._

## Files Created/Modified
- `packages/dataplat/src/dataplat/schema/evolution.py` - `SchemaChangeFinding` (frozen/slots dataclass mirroring `RejectedRecord`) and `classify_schema_change` (the classifier itself), 156 lines including a module docstring citing SCHEMA-04/05 and D-01/D-02/D-04/D-05 verbatim
- `tests/unit/schema/test_evolution.py` - 9 unit tests covering every `<behavior>` case plus D-05's statelessness proof and QUAL-12's explicitly-named compatible/breaking pair, 187 lines

## Decisions Made
- Compare by column `name`, never by position — a side effect is that SCHEMA-04's "reordered" case is correctly treated as no change with no dedicated code path, since a name-keyed lookup is inherently order-independent.
- Run the full breaking check (iterating `old_columns` in given order) to completion before computing any compatible finding, so D-02's dominance rule ("a compatible addition never partially rescues a breaking file") holds structurally rather than by an extra guard.
- Used `.get("type")` (not `["type"]`) for the optional retype comparison, so a column mapping that only carries a `name` key (no `type` declared) does not crash — it simply cannot trigger a retype classification. `name` itself is still accessed via `[...]`, since a column with no name cannot be compared at all.

## Deviations from Plan

None - plan executed exactly as written. Both tasks' `<behavior>`/`<action>` blocks matched the codebase's existing conventions and interfaces (`IncompatibleSchemaError`, `DIAGNOSTIC_CODES`'s `schema-column-disappeared`/`schema-column-retyped`, `RejectedRecord`'s frozen/slots shape) exactly as the plan's `<interfaces>` block described — no missing dependency, no architectural surprise, no auto-fix needed.

One small in-scope adjustment during Task 2: the plan's acceptance criteria asked for "an explicit comment distinguishing this function's header-level scope from `16_extra_columns.csv`'s row-level scope." The first draft of that comment (plus the QUAL-12 explanatory comment above it) tripped ruff's `ERA001` ("commented-out code") false-positive on prose that happened to look like a function call (`IDENTIFIER (...)`) and a YAML-like bracket list. Reworded to plain prose with no parenthesized-identifier or bracket-list patterns; the same information (fixture name, row-level vs. header-level distinction, corpus-independence rationale) is preserved. Not logged as a Rule 1/2/3 deviation since no behavior or test coverage changed — purely a lint-clean wording fix within the same task's own file.

## Issues Encountered

**Shared `.venv` stale editable install (documented environment issue, not a bug in this plan's code).** The worktree has no local `.venv`; the shared venv at the main tree root has `dataplat`/`csv-processor` installed editable, pointing at the main tree's absolute `packages/*/src` paths, not this worktree's copies. Every verification command (`pytest`, `ruff`, `mypy`) was run with `PYTHONPATH=packages/dataplat/src:packages/csv-processor/src` prefixed (relative to the worktree root, which is the default cwd for each tool call) to force imports to resolve against this worktree's actual files. Confirmed working: the RED-phase run correctly reported `ModuleNotFoundError: No module named 'dataplat.schema.evolution'` (proving it was reading the worktree's `dataplat/schema/` package, which only had `__init__.py` at that point, not some cached/main-tree copy that might already have had a different `evolution.py`).

## User Setup Required

None - no external service configuration required. Pure Python, stdlib-only, no new dependencies.

## Next Phase Readiness

- `classify_schema_change`/`SchemaChangeFinding` are ready for plan 06-15 to wire into `CsvSource.inspect()` (Wave 4, blocked on Wave 3) — the function's signature (`Sequence[Mapping[str, object]]` in, `list[SchemaChangeFinding]` out or raise) is intentionally DB-independent, so 06-15's wiring plan owns translating `meta.schema_versions` rows and a file's detected header into this shape, and translating a `SchemaChangeFinding` into whatever `meta.schema_versions` persistence 06-12's repository module defines.
- No blockers. This plan's only dependency (06-02, for `IncompatibleSchemaError`/`DIAGNOSTIC_CODES`/`ColumnContract`) was already merged into this worktree's base before execution started.

---
*Phase: 06-universal-csv-engine-schema-contracts-normalization*
*Completed: 2026-08-15*

## Self-Check: PASSED

- FOUND: `packages/dataplat/src/dataplat/schema/evolution.py`
- FOUND: `tests/unit/schema/test_evolution.py`
- FOUND: `.planning/phases/06-universal-csv-engine-schema-contracts-normalization/06-13-SUMMARY.md`
- FOUND commit: `c69c6c8` (test: RED phase)
- FOUND commit: `7d704b7` (feat: GREEN phase)
- FOUND commit: `bdc684b` (test: Task 2)
