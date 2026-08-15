---
phase: 06-universal-csv-engine-schema-contracts-normalization
plan: 02
subsystem: config
tags: [pydantic, dataset-config, schema-contracts, diagnostics, exception-hierarchy, csv]

# Dependency graph
requires:
  - phase: 03-dataplat-core-library-metadata-control-plane
    provides: DatasetConfig/SourceConfig/DeduplicationConfig/LoadConfig/BatchingConfig base shape, dataplat.errors.DataPlatformError hierarchy, config/loader.py's shallow-merge load_config
  - phase: 04-vertical-slice-csv-to-analytical-postgresql
    provides: configs/datasets/customers.yaml's existing source/deduplication/load/batching shape; the discover_files/StagingLoader/run_ingest consumers whose fixtures needed the new required field
provides:
  - ColumnContract, FilenameMaskConfig, NormalizationConfig, CsvParsingConfig Pydantic models on dataplat.config.model
  - DatasetConfig.columns (required)/filename/normalization/csv/schema_evolution_on_new_column/schema_evolution_on_missing_or_retyped_column fields
  - Two DatasetConfig model_validator(mode="after") checks — delimiter/decimal_separator collision, deduplication.keys/business_key cross-check
  - dataplat.diagnostics.DIAGNOSTIC_CODES — 24-code catalog (14 corpus-derived, 10 new-this-phase), each corpus-derived code cited to its originating fixture
  - dataplat.errors.SourceError (+5 subclasses) and SchemaError (+2 subclasses), matching ARCHITECTURE.md Section 4.5 exactly
  - configs/datasets/customers.yaml's real columns: block (5 entries); configs/defaults.yaml's schema-evolution policy defaults
affects: [06-01, 06-03, 06-04, 06-05, 06-06, 06-07, 06-08, 06-09, 06-10, 06-11, all Wave-2 detector/normalizer/schema-versioning/compression plans]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Cross-field validation via @model_validator(mode=\"after\") on DatasetConfig (first instance in this file; prior validators were per-field types only)"
    - "Diagnostic-code catalog sourced verbatim from an existing test-fixture corpus, with per-code fixture-of-origin citations kept as module comments for human auditability"

key-files:
  created:
    - packages/dataplat/src/dataplat/diagnostics.py
    - tests/unit/test_dataset_config_columns.py
    - tests/unit/test_diagnostics.py
    - .planning/phases/06-universal-csv-engine-schema-contracts-normalization/deferred-items.md
  modified:
    - packages/dataplat/src/dataplat/config/model.py
    - packages/dataplat/src/dataplat/errors.py
    - configs/defaults.yaml
    - configs/datasets/customers.yaml
    - tests/unit/test_batching_config.py
    - tests/unit/test_config_hashing.py
    - tests/unit/test_discovery.py
    - tests/integration/test_staging_loader.py
    - tests/integration/test_discover_files.py
    - tests/integration/test_run_ingest.py

key-decisions:
  - "columns: is required (never defaulted) on DatasetConfig, matching BatchingConfig's precedent (D-18) — this broke five pre-existing DatasetConfig-constructing test fixtures outside the plan's declared files_modified list; all five were fixed inline with the identical mechanical columns: block, verified via a diffed mypy baseline against the pre-task commit to confirm zero new regressions were introduced"
  - "diagnostics.py's corpus-derived/new-this-phase split stays module-private (_CORPUS_DERIVED_CODES/_NEW_THIS_PHASE_CODES); only DIAGNOSTIC_CODES is exported, matching the plan's interfaces contract exactly. test_diagnostics.py transcribes the 14-code corpus-derived list independently rather than importing the private set, so the drift guard cannot become a tautology against its own module"
  - "RAGGED_ROW's SCREAMING_SNAKE_CASE is grandfathered, not renamed, per D-25 — documented in diagnostics.py's module docstring rather than touching Phase 3's pipeline/engine.py"

patterns-established:
  - "New DatasetConfig sub-models each get a full Google-style Attributes docstring citing the CONTEXT.md decision (D-NN) or requirement ID that shaped every field, mirroring the existing SourceConfig/DeduplicationConfig template exactly"
  - "A diagnostic-code catalog cites its oracle (corpus fixture name + line) inline as a module comment, not just in the module docstring, so a human can audit the corpus-vs-catalog link without re-running the grep that produced it"

requirements-completed: [SCHEMA-02, SCHEMA-05]

# Metrics
duration: 35min
completed: 2026-08-15
---

# Phase 6 Plan 2: Schema Contracts, Diagnostic Catalog & Exception Families Summary

**Extended `DatasetConfig` with `columns`/`filename`/`normalization`/`csv` Pydantic sub-models plus two cross-field validators, added a 24-code `DIAGNOSTIC_CODES` catalog cited to its corpus origin, and added the `SourceError`/`SchemaError` exception families — the shared, read-only contract surface all eleven Wave 2 plans in this phase import from.**

## Performance

- **Duration:** ~35 min
- **Started:** 2026-08-15T11:05:00+02:00 (approximate — context loading before first commit)
- **Completed:** 2026-08-15T11:42:00+02:00
- **Tasks:** 3 completed
- **Files modified:** 14 (5 created, 9 modified)

## Accomplishments

- `DatasetConfig` now carries `columns: list[ColumnContract]` (required, D-18's source of truth), opt-in `filename`/`normalization`, a `csv: CsvParsingConfig` structural-override block, and two flat schema-evolution-policy fields — all four new sub-models use the file's existing `ConfigDict(extra="forbid", frozen=True)` + Google-Attributes-docstring template
- Two new `@model_validator(mode="after")` checks on `DatasetConfig`: a `csv.delimiter`/`normalization.decimal_separator` collision guard (STACK.md Section 15, mirroring `tools/corpus/manifest.py`'s identical corpus-level check) and a `deduplication.keys`/`columns[].business_key` cross-check (D-18)
- `dataplat/diagnostics.py` is new: `DIAGNOSTIC_CODES` (24 entries — 14 verbatim from `tests/fixtures/corpus.yaml`'s `quarantine_reason*` values, each cited to its originating fixture; 10 new-this-phase codes pre-declared for Wave 2's detector/schema plans)
- `dataplat/errors.py` gained `SourceError` (+ `FileInspectionError`, `FilenameParsingError`, `EncodingDetectionError`, `CsvDialectDetectionError`, `CsvParsingError`) and `SchemaError` (+ `SchemaValidationError`, `IncompatibleSchemaError`) — the exact eight names `ARCHITECTURE.md` Section 4.5 already fixed, each pre-declared with no raise site yet (Wave 2 adds those)
- `configs/datasets/customers.yaml` now has a real 5-column `columns:` block (customer_id/name/country/birth_date/event_ts); `filename:`/`normalization:` stay deliberately absent (D-10/D-12)
- `configs/defaults.yaml` gained the two flat schema-evolution-policy defaults (D-03/D-04, Pitfall 7's shallow-merge-safe flat shape)

## Task Commits

Each task was committed atomically:

1. **Task 1: Extend DatasetConfig with columns/filename/normalization/csv and the two validators** - `1ebb77c` (feat)
2. **Task 2: Create diagnostics.py catalog and extend errors.py with SourceError/SchemaError** - `47b0f35` (feat)
3. **Task 3: Activate the contract for customers; schema-evolution defaults; tests for both new files** - `17e4c36` (feat, includes the Rule 1/3 fixture-fix deviation below)

_(Worktree mode: SUMMARY.md/metadata commit follows this document, per orchestrator convention — no separate STATE.md/ROADMAP.md commit from this agent.)_

## Files Created/Modified

- `packages/dataplat/src/dataplat/config/model.py` - `ColumnContract`, `FilenameMaskConfig`, `NormalizationConfig`, `CsvParsingConfig`; `DatasetConfig` gains 6 new fields + 2 model validators
- `packages/dataplat/src/dataplat/diagnostics.py` - new: `DIAGNOSTIC_CODES` catalog (24 codes)
- `packages/dataplat/src/dataplat/errors.py` - `SourceError`/`SchemaError` families (8 new subclasses)
- `configs/defaults.yaml` - two new flat schema-evolution-policy default keys
- `configs/datasets/customers.yaml` - real `columns:` block (5 entries)
- `tests/unit/test_dataset_config_columns.py` - new: happy path + 4 failure-mode tests for Task 1's validators
- `tests/unit/test_diagnostics.py` - new: D-24 corpus-vs-catalog drift guard (2 tests)
- `tests/unit/test_batching_config.py` - fixture fix: `_VALID_DOCUMENT` gains a `columns:` block
- `tests/unit/test_config_hashing.py` - fixture fix: both `_CUSTOMERS_DOCUMENT`/`_CUSTOMERS_DOCUMENT_REORDERED` gain identical `columns:` blocks
- `tests/unit/test_discovery.py` - fixture fix: `_skip_config()` gains `columns=[...]`
- `tests/integration/test_staging_loader.py` - fixture fix: `_make_config()` gains `columns=[...]`
- `tests/integration/test_discover_files.py` - fixture fix: `_make_config()` gains `columns=[...]`
- `tests/integration/test_run_ingest.py` - fixture fix: `_make_config()` gains `columns=[...]`
- `.planning/phases/06-universal-csv-engine-schema-contracts-normalization/deferred-items.md` - new: 4 pre-existing, unrelated mypy findings logged (not fixed, out of scope)

## Decisions Made

- Kept `diagnostics.py`'s corpus-derived/new-this-phase split as private module constants; only `DIAGNOSTIC_CODES` is exported, matching the plan's `<interfaces>` contract exactly (five downstream plans are planned against that exact surface). `test_diagnostics.py` transcribes its own 14-code expectation list rather than importing the private set, so the drift guard tests the real corpus file, not the module's agreement with itself.
- Used `PYTHONPATH=packages/dataplat/src:packages/csv-processor/src` for every verification command in this session. The shared worktree `.venv`'s editable install for `dataplat`/`csv_processor` points at the **main tree's** absolute path (`/home/konutec/projects/airflow-platform/packages/...`), not this worktree's copy — confirmed by inspecting `.venv/lib/python3.12/site-packages/_editable_impl_dataplat.pth`. Without the override, `python -c "import dataplat..."` and bare `pytest` would have silently tested the wrong (main-tree, unmodified) files. This is an environment quirk of the parallel-worktree setup, not a codebase issue — no repo files were changed to work around it.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1/3 - Bug/Blocking] `columns:` required field broke 5 pre-existing test fixtures outside this plan's file scope**
- **Found during:** Task 1 verification (immediately) and Task 3's full-suite regression check
- **Issue:** Task 1's own `<behavior>` spec requires `columns:` to be a required `DatasetConfig` field (`raises pydantic.ValidationError when columns: is omitted`), matching `BatchingConfig`'s "required, never defaulted" precedent. But `tests/unit/test_batching_config.py` (Task 1's own acceptance criterion: "still exits 0") and four other pre-existing `DatasetConfig`-constructing test files (`tests/unit/test_config_hashing.py`, `tests/unit/test_discovery.py`, `tests/integration/test_staging_loader.py`, `tests/integration/test_discover_files.py`, `tests/integration/test_run_ingest.py`) — none in this plan's `files_modified` list — construct `DatasetConfig` documents/objects with no `columns:` at all, and would fail loudly the moment `columns:` became required.
- **Fix:** Added the identical 5-column `columns:` block (customer_id/name/country/birth_date/event_ts, matching `configs/datasets/customers.yaml`'s real shape) to each of the six affected fixtures — as a `columns:` dict-list literal for `.model_validate()`-based fixtures, and as `columns=[ColumnContract(...), ...]` for direct-constructor fixtures (`test_discovery.py`, all three integration files).
- **Files modified:** `tests/unit/test_batching_config.py`, `tests/unit/test_config_hashing.py`, `tests/unit/test_discovery.py`, `tests/integration/test_staging_loader.py`, `tests/integration/test_discover_files.py`, `tests/integration/test_run_ingest.py`
- **Verification:** Full unit suite (`tests/unit`, 158 tests) and full integration suite (`tests/integration`, 64 tests, real testcontainers PostgreSQL+MinIO) both pass, zero failures. `mypy` diffed against the pre-Task-3 baseline (commit `47b0f35`, extracted via `git show`) on all five affected files confirms each fix eliminated exactly the "Missing named argument columns" error it targeted and introduced zero new errors — the only mypy findings remaining in these files are pre-existing and unrelated (logged to `deferred-items.md`).
- **Committed in:** `1ebb77c` (test_batching_config.py, alongside Task 1), `17e4c36` (the other four files, alongside Task 3)

---

**Total deviations:** 1 auto-fixed (Rule 1/3, touching 6 files outside the plan's declared scope)
**Impact on plan:** Necessary consequence of faithfully implementing Task 1's own explicit "columns: required" spec — not scope creep. No design change; every fix is the identical mechanical fixture-shape addition the plan itself specifies for `customers.yaml`. All pre-existing tests remain green; no test assertions were weakened or removed.

## Issues Encountered

- **Worktree `.venv` shares the main tree's editable-install path** (see Decisions Made above). Worked around locally via `PYTHONPATH`; no code change. Flagging for the orchestrator/verifier: any other parallel worktree agent in this wave should apply the same `PYTHONPATH` override rather than trusting a bare `python`/`pytest` invocation, or its verification may silently target the wrong tree.
- Docker and a live kind cluster were already running in this environment, so `tests/integration` (testcontainers PostgreSQL+MinIO) could be run directly rather than deferred — used this to get real execution proof for the fixture-fix deviation above rather than relying on static reasoning alone.
- A self-initiated, out-of-scope `tests/policy -m "not manifests"` run (not required by this plan's verification block, ~4 min — mostly corpus-generation determinism checks) was also run for extra confidence: 124 passed, 10 deselected, zero failures.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Every field/validator named in this plan's `<interfaces>` block is implemented exactly as specified and importable: `ColumnContract`, `FilenameMaskConfig`, `NormalizationConfig`, `CsvParsingConfig`, `DatasetConfig`'s six new fields, `DIAGNOSTIC_CODES`, and all eight new `SourceError`/`SchemaError` subclasses.
- Wave 2's eleven plans (5 detectors, 3 normalizers, 2 schema-versioning, 1 compression) can now import this contract surface read-only, per the plan's stated purpose as the phase's Interface-First plan.
- `customers.yaml` validates against the extended `DatasetConfig` with a real `columns:` block; `filename:`/`normalization:` remain correctly absent, matching D-10/D-12.
- No blockers. The one open item is the still-private `_CORPUS_DERIVED_CODES`/`_NEW_THIS_PHASE_CODES` split in `diagnostics.py` — if a later Wave 2 plan needs to distinguish "corpus-derived" from "new" codes at runtime (not just in tests), it will need either a new public export or its own local list; nothing in this phase's `<interfaces>` block calls for that distinction to be runtime-visible today.

---
*Phase: 06-universal-csv-engine-schema-contracts-normalization*
*Completed: 2026-08-15*

## Self-Check: PASSED

All claimed created files verified present:
- `packages/dataplat/src/dataplat/diagnostics.py` — FOUND
- `tests/unit/test_dataset_config_columns.py` — FOUND
- `tests/unit/test_diagnostics.py` — FOUND
- `.planning/phases/06-universal-csv-engine-schema-contracts-normalization/deferred-items.md` — FOUND
- `.planning/phases/06-universal-csv-engine-schema-contracts-normalization/06-02-SUMMARY.md` — FOUND

All claimed commit hashes verified present in `git log --oneline --all`:
- `1ebb77c` — FOUND
- `47b0f35` — FOUND
- `17e4c36` — FOUND
