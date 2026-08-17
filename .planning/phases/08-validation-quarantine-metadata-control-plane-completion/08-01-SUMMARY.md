---
phase: 08-validation-quarantine-metadata-control-plane-completion
plan: 01
subsystem: database
tags: [alembic, postgresql, pydantic, dataplat, metadata-repository, validation]

# Dependency graph
requires:
  - phase: 03-dataplat-core-library-metadata-control-plane
    provides: meta.ingestion_runs/meta.files/meta.batches schema, MetadataRepository Protocol convention, DatasetConfig pydantic shape
  - phase: 04-vertical-slice-csv-to-analytical-postgresql
    provides: atomic staging->publish transaction pattern (UNLOGGED staging, pg_advisory_xact_lock) that record_validation_results/record_rejected_records must share a connection with
provides:
  - meta.validation_results DDL (migration 0014) — the coordinate-once point Wave E's two parallel streams (validation engine, metadata completion) both build on
  - meta.rejected_records DDL (migration 0015), including resolution_type PENDING/REDRIVEN/DISCARDED and the batch_id FK resolve_rejected_records_for_batch needs
  - normalized.orders DDL (migration 0016), D-17 business columns + 6 lineage columns, customer_id deliberately not a DB-level FK (D-16)
  - QualityThresholdExceeded/PublicationError exception classes (errors.py)
  - Widened ValidationResult dataclass (rule_type/severity/evaluated_count/failed_count/threshold/observed)
  - MetadataRepository Protocol stubs for record_validation_results/record_rejected_records/resolve_rejected_records_for_batch (implementation deferred to plan 08-03)
  - QualityRuleConfig/QualityConfig pydantic models and DatasetConfig.quality field
  - SourceConfig.batch_complete_marker field (LOAD-11/D-19, opt-in and unexercised)
affects: [08-02, 08-03, 08-04, 08-05, validation-engine-plans, metadata-completion-plans, orders-dataset-plans]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "conn-scoped MetadataRepository methods (record_validation_results/record_rejected_records/resolve_rejected_records_for_batch) mirror finalize_publication's own contract: caller-supplied, already-open connection, never committed/rolled back inside the method — keeps validation findings and publish atomic in the same transaction"
    - "resolution_type disambiguates a 2-state lifecycle (D-04 PENDING/RESOLVED) into 3 values (PENDING/REDRIVEN/DISCARDED) at the column level, with the whole-batch UPDATE as the only write path"

key-files:
  created:
    - migrations/versions/0014_meta_validation_results.py
    - migrations/versions/0015_meta_rejected_records.py
    - migrations/versions/0016_normalized_orders.py
    - tests/unit/test_quality_config.py
  modified:
    - packages/dataplat/src/dataplat/errors.py
    - packages/dataplat/src/dataplat/models/report.py
    - packages/dataplat/src/dataplat/metadata/repository.py
    - packages/dataplat/src/dataplat/config/model.py
    - tests/integration/test_migrations.py
    - tests/integration/conftest.py

key-decisions:
  - "meta.rejected_records gets a direct, non-deferred batch_id FK not in ARCHITECTURE.md's original sketch, because resolve_rejected_records_for_batch's own WHERE batch_id = %s predicate requires it"
  - "normalized.orders.customer_id is deliberately NOT a database-level FK to normalized.customers (D-16, T-08-02 accepted risk) — an orphan order must still publish under QUARANTINE_RECORD, which a DB FK would make impossible"
  - "Fixed a pre-existing, unrelated gap in tests/integration/conftest.py: the postgres_dsn fixture never created the analytics_owner role that migration 0013 (phase 7) started granting to, breaking alembic upgrade head for every migration since 0013 — blocking this plan's own verification (Rule 3 auto-fix)"

patterns-established:
  - "Exception subclass added by the phase that first raises it, with the raise site landing in a later plan of the same phase (errors.py's own documented convention, now applied to QualityThresholdExceeded/PublicationError)"
  - "quality:/batch_complete_marker are opt-in DatasetConfig/SourceConfig fields, unexercised by any live dataset until a later plan populates them — same precedent as freshness: and filename: blocks"

requirements-completed: [VALID-01, VALID-02, VALID-03, VALID-04, VALID-07, VALID-09, LOAD-11]

# Metrics
duration: 10min
completed: 2026-08-17
---

# Phase 8 Plan 1: Foundational DDL & Shared Contracts Summary

**Three new Alembic migrations (meta.validation_results, meta.rejected_records, normalized.orders) plus the widened ValidationResult/MetadataRepository Protocol/quality: config surface every later Phase 8 plan builds on, with zero behavior wired in yet.**

## Performance

- **Duration:** ~10 min (commit-to-commit)
- **Started:** 2026-08-17T09:48:23+02:00
- **Completed:** 2026-08-17T09:53:33+02:00
- **Tasks:** 2
- **Files modified:** 11 (4 created migrations/tests, 7 modified)

## Accomplishments
- Landed `meta.validation_results`/`meta.rejected_records` DDL — the exact coordinate-once point ROADMAP's Wave E guidance names for the validation-engine and metadata-completion streams
- Landed `normalized.orders` DDL, proving D-17's shape ahead of the `csv_ingest_orders` DAG a later plan builds
- Widened `ValidationResult` from its "minimal D-05" shape to the real, enum-vocabulary-carrying shape backing `meta.validation_results`
- Added the 3 `MetadataRepository` Protocol methods every later validation/backfill plan will implement against and call, with an explicit `conn`-scoped transactional contract matching `finalize_publication`'s own precedent
- Added `QualityRuleConfig`/`QualityConfig`/`SourceConfig.batch_complete_marker` — the config surface `customers.yaml`'s real `quality:` block (D-09, a later plan) and LOAD-11's manifest-marker capability both need
- Found and fixed a genuine, pre-existing `alembic upgrade head` regression in `tests/integration/conftest.py` (missing `analytics_owner` role, dating to phase 7's migration 0013) that was silently blocking every migration test since — not caused by this plan, but blocking its own verification

## Task Commits

Each task was committed atomically:

1. **Task 1: Three new migrations — validation_results, rejected_records, normalized.orders** - `dcb2258` (feat)
2. **Task 2: Widen contracts — errors.py, report.py, repository.py Protocol, config/model.py quality: block** - `4eba318` (feat)

_Note: Task 2's commit also includes a small lint-formatting follow-up to the migration files created in Task 1 (line-length fixes discovered running ruff after Task 2)._

## Files Created/Modified
- `migrations/versions/0014_meta_validation_results.py` - meta.validation_results DDL, SELECT/INSERT/UPDATE grant only (D-04)
- `migrations/versions/0015_meta_rejected_records.py` - meta.rejected_records DDL, resolution_type PENDING/REDRIVEN/DISCARDED, batch_id FK
- `migrations/versions/0016_normalized_orders.py` - normalized.orders DDL, D-17 columns + 6 lineage columns, customer_id not a DB FK
- `tests/integration/test_migrations.py` - EXPECTED_TABLES/HASH_VERSION_COLUMNS extended for the 3 new tables
- `tests/integration/conftest.py` - fixed pre-existing analytics_owner role gap (Rule 3)
- `packages/dataplat/src/dataplat/errors.py` - QualityThresholdExceeded/PublicationError added
- `packages/dataplat/src/dataplat/models/report.py` - ValidationResult widened to 9 fields
- `packages/dataplat/src/dataplat/metadata/repository.py` - 3 new Protocol method stubs
- `packages/dataplat/src/dataplat/config/model.py` - QualityRuleConfig/QualityConfig/DatasetConfig.quality/SourceConfig.batch_complete_marker
- `tests/unit/test_quality_config.py` - new, 5 tests covering the quality: block and batch_complete_marker

## Decisions Made
- `meta.rejected_records.batch_id` is a real, non-deferred FK (not speculative in ARCHITECTURE.md) because `resolve_rejected_records_for_batch`'s own signature requires a batch-scoped WHERE predicate — documented in the migration's own docstring.
- `normalized.orders.customer_id` stays a plain indexed column, never a DB-level FK, so D-16's default `QUARANTINE_RECORD` orphan-handling can publish rows referencing a not-yet-loaded customer — referential integrity is an application-level barrier stage, a later plan's job.
- `QualityRuleConfig.rule_type`/`.strategy` are plain `str` (registry-resolved), matching `SourceConfig.type`'s convention rather than `ColumnContract.type`'s closed-`Literal` convention, since rule types are a genuine extension point (08-RESEARCH.md Pattern 1).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Fixed pre-existing analytics_owner role gap blocking alembic upgrade head**
- **Found during:** Task 1, first verification run of `pytest tests/integration/test_migrations.py`
- **Issue:** `alembic upgrade head` failed at migration 0013 (`GRANT SELECT ON meta.v_customers_lineage TO analytics_owner`, landed in phase 7) with `UndefinedObject: role "analytics_owner" does not exist`. `tests/integration/conftest.py`'s `postgres_dsn` fixture only ever created `etl_app`, never `analytics_owner` — a real CNPG cluster auto-creates the latter via `initdb.owner`, but the testcontainers fixture was never updated when migration 0013 landed (confirmed via `git log`: the migration's own commit touched no other file). This blocked every `tests/integration/test_migrations.py` test, including this plan's own acceptance criteria, not something this plan's new migrations caused.
- **Fix:** Added `cur.execute("CREATE ROLE analytics_owner LOGIN")` alongside the existing `etl_app` creation in `postgres_dsn`, with a comment explaining the CNPG parity this reproduces.
- **Files modified:** `tests/integration/conftest.py`
- **Verification:** `pytest tests/integration/test_migrations.py -x` — 9/9 passed (was failing at collection-time DB setup before the fix)
- **Committed in:** `dcb2258` (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Necessary to prove this plan's own acceptance criteria (`alembic upgrade head` exits 0 against a throwaway PostgreSQL 18). No scope creep — the fix is a single line in a fixture, not a redesign.

## Issues Encountered
- The plan's `<verify>` command for Task 1 (`pytest tests/integration/test_migrations.py -x -m integration`) deselects all 9 tests to 0 selected (exit 0, vacuously "green") because most files under `tests/integration/` — including `test_migrations.py` — are not individually decorated with `@pytest.mark.integration`; only `test_metrics_otlp.py` carries that marker explicitly. The repo's actual integration-test invocation convention is `make test-integration` → `pytest tests/integration -q` (no `-m` filter) or a direct un-filtered `pytest tests/integration/test_migrations.py -x`. Verified against the real, unfiltered invocation instead (9/9 passed) since the `-m integration` form as written would have produced a false-positive "pass" with zero tests actually run. Not a code defect — a pre-existing plan/repo convention mismatch, noted here rather than silently worked around.

## Next Phase Readiness
- Wave 2's validation-engine and metadata-completion streams can both build directly on `meta.validation_results`/`meta.rejected_records` DDL, the widened `ValidationResult`, the 3 `MetadataRepository` stubs, and the `quality:`/`batch_complete_marker` config surface — no plan in this phase needs to touch this plan's files again per the plan's own success criteria.
- `PostgresMetadataRepository` (`packages/dataplat/src/dataplat/metadata/postgres.py`) does NOT yet implement the 3 new Protocol methods — deliberately deferred to plan 08-03, confirmed not to break `mypy` (Protocol subclassing does not require overriding stub methods with no `@abstractmethod`).
- No blockers for downstream plans.

---
*Phase: 08-validation-quarantine-metadata-control-plane-completion*
*Completed: 2026-08-17*

## Self-Check: PASSED

All 10 created/modified files confirmed present on disk; both task commits
(`dcb2258`, `4eba318`) confirmed present in `git log --oneline`.
