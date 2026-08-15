---
phase: 06-universal-csv-engine-schema-contracts-normalization
plan: 12
subsystem: database
tags: [postgres, psycopg, schema-versioning, hashing, meta.schema_versions]

# Dependency graph
requires:
  - phase: 06-01
    provides: migration 0009 (meta.schema_versions table, partial unique index, FK closure)
  - phase: 06-02
    provides: DatasetConfig.columns (ColumnContract) shape schema/versioning.py's column-dict input mirrors
provides:
  - "hash_schema — canonical-JSON sha256 recipe for a resolved, ordered column list (SCHEMA_HASH_VERSION=1)"
  - "SchemaRepository — meta.schema_versions CRUD: sync() (versioned upsert), get_current(), resolve_by_hash() (SCHEMA-06's D-16 historical-resolution mechanism)"
affects: [06-13, 06-schema-evolution-classification, any-later-wiring-plan-that-calls-resolve_by_hash]

# Tech tracking
tech-stack:
  added: []
  patterns: [versioned-upsert-sibling-of-ConfigRegistry, row-lock-for-update-serialization]

key-files:
  created:
    - packages/dataplat/src/dataplat/schema/versioning.py
    - packages/dataplat/src/dataplat/schema/repository.py
    - tests/unit/schema/test_versioning.py
    - tests/integration/test_schema_resolution.py
  modified: []

key-decisions:
  - "hash_schema uses json.dumps(..., sort_keys=False, ...) — the deliberate divergence from hash_config's sort_keys=True, since column list order AND each column dict's own key order are both semantically load-bearing for a CSV schema, never an accident to normalize away (per plan Task 1 action text)."
  - "SchemaRepository.sync() takes an already-resolved dataset_id (int), not a dataset_name (str) like ConfigRegistry.sync() — meta.datasets rows are always created by config-sync first (ARCHITECTURE.md §5.1), so schema-sync never needs its own first-write dataset-row-creation race protection."
  - "T-06-19 mitigated via SELECT ... FOR UPDATE on the meta.datasets row inside sync()'s transaction (not an INSERT ... ON CONFLICT upsert like ConfigRegistry._resolve_dataset_id) — FOR UPDATE is sufficient and correct here because SchemaRepository.sync() only ever operates on an already-existing dataset row, exactly the case ConfigRegistry's own docstring says FOR UPDATE alone is sufficient for."
  - "Verified tests/integration/test_schema_resolution.py without the plan-specified -m integration filter, following 06-01-SUMMARY.md's established precedent: no 'integration' pytest marker exists anywhere in this codebase (confirmed via repo-wide grep), so -m integration silently deselects all 7 tests and exits 0 having run nothing. Empirically reproduced this exact behavior (7 deselected, exit 0) before using the same invocation make test-integration performs instead."

patterns-established:
  - "dataplat/schema/*.py as ConfigRegistry's sibling: same constructor-injected-pool shape, same versioned-upsert SQL shape (SELECT current WHERE valid_to IS NULL -> hash match no-op / hash differs close+insert), same private _require_row copy per file (06-PATTERNS.md Cluster 7's explicit sanction)."

requirements-completed: [SCHEMA-03, SCHEMA-06, QUAL-04]

# Metrics
duration: ~20min
completed: 2026-08-15
---

# Phase 06 Plan 12: Schema Hashing & Versioned Repository Summary

**`hash_schema`/`SchemaRepository` — canonical-JSON schema hashing and a `meta.schema_versions` versioned-upsert repository with hash-match historical resolution, transposed line-for-line from `ConfigRegistry`'s already-proven pattern**

## Performance

- **Duration:** ~20 min
- **Tasks:** 2 completed
- **Files modified:** 4 (all created, 0 modified)

## Accomplishments
- `hash_schema(columns) -> (hash, hash_version)` — deterministic, order-sensitive (both list order and each column dict's own key order), version-tagged canonical-JSON sha256 hash for a resolved column list, with the divergence from `hash_config`'s key-reordering tolerance explicitly documented and tested
- `SchemaRepository.sync()` — the exact `ConfigRegistry.sync()` no-op/close-and-insert versioned-upsert rule, transposed onto `meta.schema_versions`, proven live against a real migrated PostgreSQL 18 (via migration 0009)
- `SchemaRepository.resolve_by_hash()` — SCHEMA-06's D-16 mechanism: a file's re-derived structural hash resolves back to its own historical `meta.schema_versions` row even when that row has since been superseded (closed), proven with an explicit `valid_to IS NOT NULL` assertion on the resolved row
- `SchemaRepository.get_current()` — the dataset's open schema version, or `None` for a dataset with no schema history yet

## Task Commits

Each task was committed atomically:

1. **Task 1: hash_schema — the canonical-JSON recipe, mirroring config/hashing.py** - `3da771f` (feat)
2. **Task 2: SchemaRepository — versioned upsert + D-16's hash-match historical resolution** - `7a5b343` (feat)

**Plan metadata:** committed alongside this SUMMARY (see final commit)

## Files Created/Modified
- `packages/dataplat/src/dataplat/schema/versioning.py` - `hash_schema`/`SCHEMA_HASH_VERSION`, the canonical-JSON sha256 recipe for an ordered column list
- `packages/dataplat/src/dataplat/schema/repository.py` - `SchemaRepository`/`SchemaVersionRecord`, `meta.schema_versions` CRUD (sync/get_current/resolve_by_hash)
- `tests/unit/schema/test_versioning.py` - same-hash-twice, list-reorder-changes-hash, dict-key-reorder-changes-hash, one-field-change-changes-hash, version-constant tests (5 tests)
- `tests/integration/test_schema_resolution.py` - versioned-upsert no-op/version-on-change proof, `get_current` proof, `resolve_by_hash` historical-row proof (with explicit closed-row assertion) and unrecorded-hash `StorageError` proof (7 tests, against a real testcontainers PostgreSQL 18 + `alembic upgrade head`)

## Decisions Made
- `hash_schema` deliberately uses `sort_keys=False` (unlike `hash_config`'s `sort_keys=True`) because column list order AND each column dict's own key order are both part of a CSV schema's identity, never an accident to silently normalize away. Documented explicitly in the module docstring and proven by a dedicated test (`test_reordering_one_columns_own_keys_changes_the_hash`) that goes beyond the plan's literal acceptance-criteria script to directly validate this specific implementation choice against a hypothetical `sort_keys=True` alternative that would otherwise still pass the plan's other required tests.
- `SchemaRepository.sync()` takes `dataset_id: int` rather than a dataset name, since (unlike `ConfigRegistry`) it never needs to create the `meta.datasets` row itself — that row always already exists by the time schema-sync runs (config-sync creates it first, per ARCHITECTURE.md §5.1). This is a real signature difference from `ConfigRegistry.sync(dataset_name, config)`, called out explicitly in the module docstring so a future reader isn't surprised by the asymmetry with its sibling.
- T-06-19 (the plan's own threat-model entry for concurrent `sync()` races) is mitigated with `SELECT ... FROM meta.datasets WHERE dataset_id = %s FOR UPDATE` as the first statement inside `sync()`'s transaction, rather than literally reusing `ConfigRegistry._resolve_dataset_id`'s `INSERT ... ON CONFLICT DO UPDATE` upsert syntax. This is a deliberate, reasoned transposition, not a shortcut: `ConfigRegistry`'s own docstring explains that `FOR UPDATE` alone is insufficient only for a *brand-new* dataset row (because there's nothing yet to lock), and that the `ON CONFLICT` upsert exists specifically to close that gap. `SchemaRepository.sync()` is never called before the dataset row exists, so it never hits that gap — a plain `FOR UPDATE` row lock, held for the remainder of the transaction, provides the identical serialization guarantee `ConfigRegistry`'s own docstring attributes to "the UPDATE half of an upsert" for an already-existing row. `_require_row` raises `StorageError` if the lock query finds no row at all (a genuine caller error — a bogus `dataset_id`), giving a clear diagnostic instead of a confusing later FK-violation.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking, following established precedent] Verified integration tests without the plan-specified `-m integration` filter**
- **Found during:** Task 2 (writing and verifying `tests/integration/test_schema_resolution.py`)
- **Issue:** The plan's own `<verification>` block and Task 2's `<acceptance_criteria>` specify `pytest tests/integration/test_schema_resolution.py -m integration -x -q`. No `integration` pytest marker is registered in `pyproject.toml`'s `markers` list, and no test anywhere in this codebase applies it via decorator (confirmed via repo-wide grep). Empirically running the literal command produces `7 deselected in 0.18s`, exit code 0 — a false-positive pass that runs zero tests.
- **Fix:** Verified the suite via the same invocation `make test-integration` actually uses instead: `pytest tests/integration/test_schema_resolution.py -x -q` — 7/7 passed against a real testcontainers PostgreSQL 18 with `alembic upgrade head` applied.
- **Files modified:** None (verification-only; no source change needed). This is the exact same codebase-wide gap plan 06-01 already found and documented in `06-01-SUMMARY.md` (its own "Deviations" section) — not a new issue, just the same known quirk resurfacing in this plan's own verification text, resolved the identical way for consistency.
- **Verification:** `pytest tests/integration/test_schema_resolution.py -x -q` → 7 passed. `pytest tests/integration/test_schema_resolution.py -m integration -x -q` → 7 deselected, exit 0 (reproduced, confirming the gap is real and not something this plan should silently paper over by adding the marker to a shared config file outside its declared `files_modified`).
- **Committed in:** N/A (no code change — this is a verification-methodology note, not an auto-fix to committed code)

---

**Total deviations:** 1 (verification-methodology only, no code change, consistent with an already-documented codebase-wide precedent from plan 06-01)
**Impact on plan:** None on scope or correctness — both tasks' actual acceptance criteria (unit test file, `python -c` import checks, the integration test module's explicit closed-row proof) are satisfied exactly as written. The only adjustment is *how* the integration suite was invoked to get real pass/fail signal instead of a silent no-op.

## Issues Encountered
- The worktree's shared `.venv` initially raised `ModuleNotFoundError: No module named 'dataplat.schema.versioning'` when running `pytest` without `PYTHONPATH` set — the documented known environment issue (stale editable install). Resolved by prefixing `PYTHONPATH=packages/dataplat/src:packages/csv-processor/src` on every verification command, as instructed in this session's parallel-execution context.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- `hash_schema` and `SchemaRepository` are ready for Wave 2's schema-evolution classification plan (06-13, a pure function with no DB dependency of its own) to persist its COMPATIBLE/BREAKING outcomes through `sync()`'s `derived_from`/`compatibility`/`breaking_changes` parameters.
- `resolve_by_hash()` is ready for a later wiring plan to answer "does this dataset have a `meta.schema_versions` row matching this exact structure, from any point in history" with one function call — no filename-mask dependency, works for `customers` (which declares none, per D-10).
- Per D-17 (explicitly re-affirmed in this plan's `must_haves.truths`), the general `config_policy` replay knob (`AS_OF_LOGICAL_DATE`/`LATEST`/`PINNED`) remains deliberately unbuilt — out of scope for this plan and for Phase 6 generally, deferred to whichever phase first needs a human-selectable replay policy (likely Phase 9's backfill work).
- No blockers identified for downstream Wave 2 plans consuming this repository.

---
*Phase: 06-universal-csv-engine-schema-contracts-normalization*
*Completed: 2026-08-15*

## Self-Check: PASSED

All claimed files verified present on disk:
- `packages/dataplat/src/dataplat/schema/versioning.py` — FOUND
- `packages/dataplat/src/dataplat/schema/repository.py` — FOUND
- `tests/unit/schema/test_versioning.py` — FOUND
- `tests/integration/test_schema_resolution.py` — FOUND
- `.planning/phases/06-universal-csv-engine-schema-contracts-normalization/06-12-SUMMARY.md` — FOUND

All claimed commit hashes verified present in `git log --oneline --all`:
- `3da771f` (Task 1) — FOUND
- `7a5b343` (Task 2) — FOUND
- `9209f1a` (SUMMARY) — FOUND
