---
phase: 03-dataplat-core-library-metadata-control-plane
plan: 04
subsystem: config
tags: [pydantic, config-as-data, sha256, psycopg, postgres, config-versioning]

# Dependency graph
requires:
  - phase: 03-dataplat-core-library-metadata-control-plane
    provides: "dataplat.errors (ConfigurationError, StorageError) and dataplat.storage.db.create_pool() from plan 03-01; meta.datasets/meta.config_versions schema and tests/integration/conftest.py's migrated_dsn fixture from plan 03-02"
provides:
  - "dataplat.config.model.DatasetConfig — extra=\"forbid\"/frozen=True pydantic model every configs/datasets/*.yaml validates against"
  - "dataplat.config.hashing.hash_config() — the canonical-JSON sha256 recipe (CONFIG_HASH_VERSION=1) every later hash in the project is measured against"
  - "dataplat.config.loader.load_config() — merge-defaults-then-validate-then-wrap-errors pipeline"
  - "dataplat.config.registry.ConfigRegistry.sync() — the Postgres-backed half of config-sync (ARCHITECTURE.md §5.1): create/no-op/version meta.config_versions rows"
  - "configs/defaults.yaml and configs/datasets/customers.yaml — the first real, non-illustrative dataset config in the repo"
  - "schemas/dataset-config.schema.json — generated JSON Schema for DatasetConfig"
affects: [phase-04-vertical-slice, phase-06-csv-detection-engine, phase-10-scd-cdc]

# Tech tracking
tech-stack:
  added: []  # pydantic and PyYAML were already packages/dataplat/pyproject.toml dependencies; nothing new installed
  patterns:
    - "Canonical-JSON hash recipe: model.model_dump(mode=\"json\") -> json.dumps(sort_keys=True, separators=(\",\", \":\"), ensure_ascii=False) -> hashlib.sha256(...).hexdigest() — never hash raw YAML text"
    - "Config-not-code: DatasetConfig fields that select behavior (source.type, deduplication.strategy, load.strategy) are plain str, resolved through string-keyed registries elsewhere — never a Python enum"
    - "Postgres *_versions sync pattern: SELECT ... FOR UPDATE on the parent row (meta.datasets) + hash-compare-then-{no-op | close-old-and-insert-max+1} inside one transaction"
    - "psycopg3 jsonb parameter binding requires psycopg.types.json.Jsonb(...) — a raw dict is not auto-adapted"

key-files:
  created:
    - packages/dataplat/src/dataplat/config/__init__.py
    - packages/dataplat/src/dataplat/config/model.py
    - packages/dataplat/src/dataplat/config/hashing.py
    - packages/dataplat/src/dataplat/config/loader.py
    - packages/dataplat/src/dataplat/config/registry.py
    - configs/defaults.yaml
    - configs/datasets/customers.yaml
    - schemas/dataset-config.schema.json
    - tests/unit/test_config_hashing.py
    - tests/integration/test_config_registry.py
  modified: []

key-decisions:
  - "Wrapped config_document in psycopg.types.json.Jsonb(...) before binding — psycopg3 does not auto-adapt a plain dict to jsonb"
  - "Applied a narrow `# type: ignore[import-untyped]` to loader.py's `import yaml`, matching the existing tools/corpus/manifest.py:45 precedent (PyYAML ships no py.typed; types-PyYAML is not in the dev group and adding it was out of this plan's file scope)"
  - "Removed configs/.gitkeep and schemas/.gitkeep now that both directories hold real, committed content, matching the repo's established .gitkeep-removal convention from Phase 2"
  - "Left meta.config_versions.git_commit_sha/git_path NULL — not in Task 3's specified INSERT column list and nullable in migration 0001; git provenance is the Airflow-side config-sync DAG's job (CONTEXT.md D-02), out of this phase's scope"

patterns-established:
  - "Pattern: any future *_versions table (schema_versions, watermark history, etc.) with a valid_from/valid_to shape should follow ConfigRegistry.sync()'s FOR-UPDATE-then-hash-compare-then-version-or-noop shape"
  - "Pattern: config/model.py-style Pydantic models are ConfigDict(extra=\"forbid\", frozen=True) with string-keyed strategy fields, never enums"

requirements-completed: [SCHEMA-07]

# Metrics
duration: ~25min
completed: 2026-08-13
---

# Phase 3 Plan 04: Config-Not-Code — DatasetConfig, Canonical Hashing, ConfigRegistry Summary

**Pydantic `DatasetConfig` (extra=forbid/frozen), a canonical-JSON sha256 config hash recipe, and a Postgres-backed `ConfigRegistry.sync()` implementing ARCHITECTURE.md §5.1's create/no-op/version rule, proven against a real migrated testcontainers PostgreSQL.**

## Performance

- **Duration:** ~25 min
- **Started:** 2026-08-13T04:36:00Z (approx.)
- **Completed:** 2026-08-13T04:57:36Z
- **Tasks:** 3 completed
- **Files modified:** 12 (10 created, 2 `.gitkeep` removed)

## Accomplishments
- `DatasetConfig` (plus nested `SourceConfig`/`DeduplicationConfig`/`LoadConfig`) validates `configs/datasets/customers.yaml` merged over `configs/defaults.yaml` with zero errors; rejects an unknown key and rejects post-construction mutation
- `hash_config()` implements the exact canonicalization recipe (`model_dump(mode="json")` → sorted, whitespace-free JSON → sha256) that every later hash in the project (`content_sha256`, SCD2's `_record_hash`) will be measured against — proven key-order-independent and value-change-sensitive
- `ConfigRegistry.sync()` creates the first `meta.config_versions` row for a fresh dataset, no-ops on an unchanged config, and versions (`max+1`, closing the prior row) on a changed one — all three proven against a real, `alembic upgrade head`-migrated PostgreSQL via testcontainers
- `configs/datasets/customers.yaml` is the first real, non-illustrative dataset config committed to the repo (CONTEXT.md D-02)

## Task Commits

Each task was committed atomically:

1. **Task 1: DatasetConfig model and the one real dataset config** - `df89e14` (feat)
2. **Task 2: Canonical-JSON hashing and the load/merge/validate pipeline** - `0e0b32a` (test, RED) → `aa0fc96` (feat, GREEN)
3. **Task 3: ConfigRegistry — the Postgres-backed config-sync half** - `69efef9` (feat)

_Task 2 used TDD (`tdd="true"`): RED commit `0e0b32a` fails on `ModuleNotFoundError` (hashing.py/loader.py did not exist yet); GREEN commit `aa0fc96` makes all 6 tests pass. No REFACTOR commit was needed — the GREEN implementation was already minimal._

**Plan metadata:** (this commit, immediately following)

## Files Created/Modified
- `packages/dataplat/src/dataplat/config/__init__.py` - package marker, shallow re-export convention
- `packages/dataplat/src/dataplat/config/model.py` - `DatasetConfig`, `SourceConfig`, `DeduplicationConfig`, `LoadConfig` — all `extra="forbid", frozen=True`
- `packages/dataplat/src/dataplat/config/hashing.py` - `hash_config()`, `CONFIG_HASH_VERSION` module constant
- `packages/dataplat/src/dataplat/config/loader.py` - `load_config(path, *, defaults_path)` — merge, validate, wrap errors
- `packages/dataplat/src/dataplat/config/registry.py` - `ConfigRegistry.sync()`, `ConfigVersionRecord`
- `configs/defaults.yaml` - platform-wide defaults merged under every dataset config
- `configs/datasets/customers.yaml` - the first real dataset config (customers, merge into `normalized.customers`)
- `schemas/dataset-config.schema.json` - generated `DatasetConfig.model_json_schema()` output, marked `$comment: regenerate, never hand-edit`
- `tests/unit/test_config_hashing.py` - 6 tests: hash stability, key-order independence, value-change sensitivity, return-shape, load_config round trip, ConfigurationError wrapping
- `tests/integration/test_config_registry.py` - 3 tests: create-first-version, no-op-on-unchanged, version-on-changed, against a real migrated PostgreSQL

## Decisions Made
- `psycopg.types.json.Jsonb(...)` wraps `config.model_dump(mode="json")` before binding as the `config_document` INSERT parameter — psycopg3 requires this wrapper for `jsonb` columns; a raw `dict` is not auto-adapted.
- `loader.py`'s `import yaml` carries a narrow `# type: ignore[import-untyped]`, matching the existing `tools/corpus/manifest.py:45` precedent (PyYAML 6.0.3 ships no `py.typed`; adding `types-PyYAML` to the dev group was outside this plan's declared file scope).
- Removed `configs/.gitkeep` and `schemas/.gitkeep` now that both directories hold real, committed content — matches the repo's established convention (Phase 2 removed `.gitkeep` files the same way as directories gained real content).
- `meta.config_versions.git_commit_sha`/`git_path` were left `NULL` on every inserted row: Task 3's action explicitly enumerates the INSERT column list and these two are absent from it (they're nullable in migration 0001); populating them is the Airflow-side `config-sync` DAG's job, explicitly out of scope for this phase per CONTEXT.md D-02.

## Deviations from Plan

None — plan executed exactly as written. No Rule 1-4 auto-fixes were needed; the one `# type: ignore[import-untyped]` addition follows an existing, already-established repo convention rather than introducing a new pattern, so it is documented above as a decision rather than a deviation.

## Issues Encountered
None. `hash_config()`, `load_config()` and `ConfigRegistry.sync()` all passed their verification blocks on the first implementation attempt; `make check` and `make test-integration` were both run in full afterward and stayed green.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- `ConfigRegistry` is ready for Phase 4's vertical-slice DAG to call at run start (pinning `config_version_id` once per DAG run, per ARCHITECTURE.md §5.4's `config_policy` knob — that knob itself is Phase 4's, not built here).
- `hash_config()`'s canonicalization recipe is now the fixed precedent for `meta.files.content_sha256` and any future SCD2 `_record_hash` work (Phase 9/10) to follow, per PITFALLS.md C6.
- `configs/datasets/customers.yaml` plus `normalized.customers`'s embedded lineage columns (plan 03-02) together give Phase 4 a real, non-illustrative dataset to build the vertical slice against.
- No blockers. `dataplat.config.registry` module currently shows 0% coverage under `make check`'s unit-only run (by design — D-04 routes it to `tests/integration`, exercised only under `make test-integration`); this is expected, not a gap.

---
*Phase: 03-dataplat-core-library-metadata-control-plane*
*Completed: 2026-08-13*

## Self-Check: PASSED

All 10 claimed created files verified present on disk (`ls`), and all 5
commit hashes (`df89e14`, `0e0b32a`, `aa0fc96`, `69efef9`, `d5fba6f`)
verified present in `git log --oneline --all`. No missing items.
