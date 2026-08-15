---
phase: 06-universal-csv-engine-schema-contracts-normalization
plan: 15
subsystem: database
tags: [schema-versioning, postgres, csv, psycopg, pydantic, pytest]

# Dependency graph
requires:
  - phase: 06-universal-csv-engine-schema-contracts-normalization (plan 06-14)
    provides: CsvSource.inspect() aggregating every detector into one CsvProfile
  - phase: 06-universal-csv-engine-schema-contracts-normalization (plan 06-12)
    provides: SchemaRepository (sync()/get_current()/resolve_by_hash()) over meta.schema_versions
  - phase: 06-universal-csv-engine-schema-contracts-normalization (plan 06-13)
    provides: classify_schema_change (COMPATIBLE findings / raises IncompatibleSchemaError)
provides:
  - CsvSource.inspect() resolves/classifies/records a file's schema against its dataset's history for real, populating CsvProfile.schema_version_id/compatibility
  - CsvSource gains an opt-in dataset_id constructor parameter (default None — no existing construction breaks)
  - StagingLoader.load() stages a file whose header is wider than its dataset's contract using only the known/contract columns (D-01, previously unimplemented — no prior plan gave it this mechanism)
  - ingest() CLI command threads a real dataset_id into CsvSource, firing schema-sync in production (closing a gap plan 06-16's discover_files idempotency-key formula explicitly anticipated)
affects: [06-16, 06-17, 06-18, schema-versioning consumers in later phases]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Opt-in constructor parameter defaulting to None to avoid breaking existing callers (mirrors PipelineContext.source's own precedent)"
    - "Distinguish 'matches CURRENT version' (sync() no-op) from 'matches an OLDER historical version' (resolve_by_hash()) by comparing the observed hash against SchemaRepository.get_current()'s own hash before deciding which repository method to call"

key-files:
  created: []
  modified:
    - packages/csv-processor/src/csv_processor/source.py
    - packages/dataplat/src/dataplat/models/profile.py
    - packages/dataplat/src/dataplat/load/staging.py
    - packages/csv-processor/src/csv_processor/cli.py
    - tests/unit/test_csv_source_inspect.py
    - tests/integration/test_schema_resolution.py

key-decisions:
  - "dataset_id defaults to None on CsvSource, skipping schema resolution entirely when absent, rather than being a required parameter — keeps every pre-existing CsvSource(...) call site (cli.py discover path aside, test_run_ingest.py, test_staging_normalization.py, the pure fixture-driven tests in test_csv_source_inspect.py) working with zero changes"
  - "Zero-findings branch checks the observed hash against the CURRENT version's hash before deciding sync() vs resolve_by_hash() — needed for SCHEMA-06: a file matching the CONTRACT exactly could otherwise wrongly manufacture a spurious new version when the current version has since evolved past the contract"
  - "StagingLoader.load() truncates a row wider than target_columns to exactly target_columns' length, right before _record_hash is computed — a fix not in this plan's declared file scope, but required for the plan's own must_haves.truths to be achievable at all (Rule 2)"
  - "ingest() CLI now resolves and passes a real dataset_id to CsvSource — a fix not in this plan's declared file scope, applied because dataplat/discovery.py's own module docstring explicitly names 'plan 06-15's schema-sync wiring' as the call site meta.schema_versions depends on being populated by (Rule 2)"

patterns-established:
  - "Schema-versioning helper method (_resolve_schema) kept private to CsvSource, returning a plain (schema_version_id, compatibility) tuple rather than a richer type — CsvProfile itself may only ever hold plain primitives (dataplat/csv_processor import-linter boundary)"

requirements-completed: [SCHEMA-03, SCHEMA-04, SCHEMA-05, SCHEMA-06]

# Metrics
duration: ~75min (estimated — start time not captured at session start)
completed: 2026-08-15
---

# Phase 6 Plan 15: Wire Schema Resolution into CsvSource.inspect() Summary

**`CsvSource.inspect()` now classifies and records a file's schema against its dataset's history for real — a compatible new column loads using known columns and records exactly one proposal row, a breaking change raises before any row stages, and a file matching a historical schema resolves to that historical version — all proven against a live PostgreSQL database, not just pure-function unit tests.**

## Performance

- **Duration:** ~75 min (estimated)
- **Tasks:** 2 (both completed), plus 2 in-scope deviations required to make Task 2's own must-haves achievable
- **Files modified:** 6

## Accomplishments

- `CsvSource.inspect()` gains a private `_resolve_schema` step: on a dataset's first-ever file it bootstraps `version=1` unconditionally; otherwise it calls `classify_schema_change` — a genuinely new column records a `COMPATIBLE` proposal via `SchemaRepository.sync()` (D-01: detect + record only, never auto-DDL), a disappeared/retyped column lets `IncompatibleSchemaError` propagate uncaught before any row stages (D-02), and a file matching an OLDER, non-current historical schema resolves via `SchemaRepository.resolve_by_hash()` instead of manufacturing a spurious new version (SCHEMA-06)
- `CsvProfile` gains `schema_version_id`/`compatibility` fields, populated for real by the above
- `CsvSource` gains an opt-in `dataset_id: int | None = None` constructor parameter — schema resolution is skipped entirely when absent, so no existing `CsvSource(...)` construction anywhere in the codebase needed to change
- Fixed a real gap `StagingLoader.load()` had no prior mechanism for: a file whose header is wider than its dataset's contract (the exact COMPATIBLE case above) now stages successfully using only its known/contract columns — the literal, live proof this plan's own `must_haves.truths` require
- Wired `ingest()`'s real `CsvSource` construction to a real `dataset_id`, closing a gap `dataplat/discovery.py`'s own module docstring explicitly named as depending on "plan 06-15's schema-sync wiring"
- 12 tests in `tests/integration/test_schema_resolution.py` (7 pre-existing `SchemaRepository`-only tests + 5 new tests driving the real `CsvSource.inspect()`/`StagingLoader.load()` call chain against a live database) all pass, alongside the full `tests/unit` (375), `tests/integration` (77), and `tests/policy` (124) suites, `ruff check .`, `ruff format --check`, `mypy` (70 source files), and `lint-imports`

## Task Commits

Each task was committed atomically, plus two in-scope deviation commits:

1. **Task 1: Wire schema resolution/classification into CsvSource.inspect()** - `2c7752e` (feat)
2. **Deviation (Rule 2): StagingLoader.load() truncates a row wider than target_columns** - `14d755b` (fix)
3. **Deviation (Rule 2): thread a real dataset_id into ingest()'s CsvSource** - `dbb0989` (feat)
4. **Task 2: Live proof — compatible/breaking classification and SCHEMA-06 historical resolution against a real database** - `d7fac19` (test)

## Files Created/Modified

- `packages/csv-processor/src/csv_processor/source.py` - `CsvSource.__init__` gains `dataset_id: int | None = None`; `inspect()` calls a new private `_resolve_schema` helper that builds `old_columns`/`new_columns` (name/type/position dicts), calls `classify_schema_change`, and calls `SchemaRepository.sync()`/`get_current()`/`resolve_by_hash()` as appropriate
- `packages/dataplat/src/dataplat/models/profile.py` - `CsvProfile` gains `schema_version_id: int | None` / `compatibility: str | None` fields
- `packages/dataplat/src/dataplat/load/staging.py` - `StagingLoader.load()` truncates a row wider than `target_columns` to exactly `target_columns`' length before the hash/COPY, so a genuinely new trailing column's value never reaches the staged table or `_record_hash`
- `packages/csv-processor/src/csv_processor/cli.py` - `ingest()` resolves `dataset_id = metadata.get_or_create_dataset(doc.dataset)` (idempotent — `discover()` already created the row) and passes it to `CsvSource`
- `tests/unit/test_csv_source_inspect.py` - adds a test proving the `dataset_id=None` default leaves `CsvProfile.schema_version_id`/`compatibility` both `None`, keeping this pure fixture-driven, DB-free test module unchanged in every other respect
- `tests/integration/test_schema_resolution.py` - adds a dedicated `_E2E_DATASET` ("schema_resolution_csv_source") section driving `CsvSource.inspect()`/`StagingLoader.load()` live: 5 new tests across 4 scenarios (matching-contract no-op, compatible-new-column + staging proof, breaking missing-column, SCHEMA-06 historical resolution)

## Decisions Made

- `dataset_id` defaults to `None` on `CsvSource` rather than being required, mirroring `PipelineContext.source: Source | None = None`'s own documented reasoning ("no existing construction breaks") — this kept `tests/integration/test_run_ingest.py` and `tests/integration/test_staging_normalization.py` (neither in this plan's file scope) working with zero edits, since their `CsvSource(...)` calls simply keep opting out of schema resolution.
- The "zero findings" branch (observed schema equals the CONTRACT) checks the observed hash against `SchemaRepository.get_current()`'s own hash *before* deciding whether to call `sync()` or `resolve_by_hash()`. Without this check, a file matching the contract exactly — but uploaded *after* the dataset's current version had evolved past the contract via a compatible proposal — would have caused `sync()` to wrongly manufacture a spurious new version instead of resolving to the correct historical one (SCHEMA-06). This branch is the concrete form of Task 2's own instruction to "add this branch to Task 1's logic if it is not already implicit."
- Scenario 4 (Task 2, SCHEMA-06) implements exactly what the plan's Task-level action text and acceptance criteria describe: two total schema versions (`version=1` original, `version=2` with one compatible addition), resolving a file matching `version=1`'s shape back to `version=1` while `version=2` is current. The plan's higher-level `must_haves.truths` bullet uses the phrase "three versions ago" — this is illustrative/aspirational language at the summary level, not a literal instruction; the detailed Task 2 text is unambiguous ("after scenario 2 has produced version=2 as current... resolves this file's schema_version_id back to the version=1 row") and its acceptance criteria explicitly say "not the current version=2." The underlying mechanism (`resolve_by_hash` matching ANY historical row by hash, regardless of how many versions separate it from current) generalizes to any distance; the 2-version proof exercises the identical code path a 4-version proof would.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical Functionality] `StagingLoader.load()` had no mechanism to stage a row wider than `target_columns`**
- **Found during:** Task 2, while designing scenario 2's "drive this file all the way through `StagingLoader.load()`" requirement
- **Issue:** A file whose header has one genuinely new trailing column (the exact COMPATIBLE case this plan exists to prove) produces rows as wide as the file's own header (6 fields for a 6-column header). `StagingLoader.load()`'s `COPY` statement only ever named `target_columns` (5) plus 6 lineage columns (11 total) — a structural field-count mismatch that would have raised a Postgres `COPY` error the moment such a row was staged. No prior plan (`RaggedRowGuard` explicitly documents "never pad or truncate" for its own, different concern — a row's consistency with its OWN file header, not with the dataset's contract) gave `StagingLoader` any way to reconcile a row wider than the dataset's known columns.
- **Fix:** `StagingLoader.load()` now truncates a surviving row to `len(target_columns)` fields immediately before `_record_hash` is computed (and before the row is appended to `enriched_rows`) whenever it is wider than `target_columns`. A narrower row (a missing contract column) is left untouched — that case is already a `IncompatibleSchemaError` raised upstream in `CsvSource.inspect()`, so `StagingLoader.load()` never runs for it at all.
- **Files modified:** `packages/dataplat/src/dataplat/load/staging.py`
- **Verification:** New integration test `test_staging_loads_the_wider_file_using_only_its_known_columns` proves the staged table has no `loyalty_tier` column and every other column's value is correct. Purely additive for every pre-existing caller: `tests/integration/test_staging_loader.py` (6/6), `test_staging_normalization.py` (1/1) and `test_run_ingest.py` (7/7) all still pass — their rows are always exactly `len(target_columns)` wide, so the new truncation branch never triggers for them.
- **Committed in:** `14d755b`

**2. [Rule 2 - Missing Critical Functionality] `ingest()`'s real `CsvSource` never received a `dataset_id`**
- **Found during:** Task 1, while confirming this plan's wiring would actually take effect in production
- **Issue:** `dataplat/discovery.py`'s own module docstring (governing `discover_files`'s idempotency-key formula, already landed by an earlier wave) explicitly reads: *"A dataset with no current schema version yet (its very first discovery run, before plan 06-15's schema-sync wiring has ever run for it) contributes an empty string for this term... [it] establishes its own baseline schema via a later wiring point"* — i.e. an earlier plan already built a READER of `meta.schema_versions` state in anticipation of this plan being the WRITER. Without threading a real `dataset_id` into `ingest()`'s own `CsvSource` construction, this plan's entire schema-resolution mechanism would be dead code in production — exercised only by tests, never by a real `ingest` pod — and `discover_files`' idempotency key would forever see "no current schema version."
- **Fix:** `ingest()` now resolves `dataset_id = metadata.get_or_create_dataset(doc.dataset)` (idempotent — `discover()` already created this row before any assignment document naming this dataset can exist) and passes it to `CsvSource(bucket=source_bucket, key=source_key, dataset_id=dataset_id)`.
- **Files modified:** `packages/csv-processor/src/csv_processor/cli.py`
- **Verification:** `tests/unit/test_csv_processor_cli.py` (11/11, both tests mock `_build_common` to fail before reaching this new code, unaffected) and `tests/unit/test_cli_error_handling.py` continue to pass; `ruff`/`mypy` clean. Not independently proven live end-to-end (that needs a live kind cluster, out of this plan's scope) — the underlying mechanism it wires (`CsvSource.inspect()` with a real `dataset_id`) is itself proven live by Task 2's own integration tests.
- **Committed in:** `dbb0989`

---

**Total deviations:** 2 auto-fixed (both Rule 2 — missing critical functionality)
**Impact on plan:** Both fixes were prerequisites for this plan's own declared `must_haves.truths` to be achievable and provable at all, and for the schema-versioning feature to actually take effect outside of tests. No scope creep beyond what closing this plan's own stated loop required — both are documented here precisely because they touch files outside the plan's declared `files_modified` list.

## Issues Encountered

None beyond the two deviations documented above, which were identified and resolved during design rather than discovered as test failures.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `SCHEMA-03`, `SCHEMA-04`, `SCHEMA-05`, `SCHEMA-06` are validated live against a real database via `CsvSource.inspect()` — the schema-versioning track (06-12/06-13/06-14/06-15) is now fully wired end to end, including the production `ingest` CLI path.
- `CSV-11` (compressed + multi-part support) and `QUAL-04` (broader unit-test coverage spanning dedup/incremental/validation-reports) are explicitly NOT touched or marked complete by this plan — CSV-11's multi-part delivery wiring remains deferred to plan 06-18 (Wave 5); QUAL-04's remaining scope belongs to later phases (deduplication, incremental processing, validation reports do not exist yet in Phase 6). Per this wave's orchestrator instructions, neither is marked complete here even though this plan's own requirements frontmatter is scoped to SCHEMA-03/04/05/06 only (no risk of accidental re-marking from this plan).
- A `meta.schema_versions` compatible-proposal row is directly SQL-queryable (`SELECT * FROM meta.schema_versions WHERE dataset_id = ... AND valid_to IS NULL`) — D-06's "no new developer tooling built this phase to surface a pending proposal" holds; a human reviewing a compatible schema-evolution proposal today does so via SQL, matching the platform's existing lineage-is-queryable-by-SQL philosophy.

---
*Phase: 06-universal-csv-engine-schema-contracts-normalization*
*Completed: 2026-08-15*

## Self-Check: PASSED

All 7 claimed files verified present on disk (source.py, profile.py,
staging.py, cli.py, test_csv_source_inspect.py, test_schema_resolution.py,
this SUMMARY.md). All 5 claimed commit hashes verified present in
`git log --oneline --all` (2c7752e, 14d755b, dbb0989, d7fac19, c8bc648).
`requirements.mark-complete SCHEMA-03 SCHEMA-04 SCHEMA-05 SCHEMA-06` confirmed
all 4 already marked complete in REQUIREMENTS.md (no-op, `updated: false`) —
REQUIREMENTS.md left untouched, matching `git status --short` showing no
diff for it.
