---
phase: 04-vertical-slice-csv-to-analytical-postgresql
plan: 01
subsystem: database
tags: [postgresql, alembic, psycopg, boto3, s3, pydantic, metadata-repository, objectstore, config-registry]

# Dependency graph
requires:
  - phase: 03-core-library-and-metadata-control-plane
    provides: meta/normalized Alembic schema, MetadataRepository/PostgresMetadataRepository, ObjectStore/S3ObjectStore, ConfigRegistry, PipelineContext/RunContext dataclasses
provides:
  - "migration 0006: normalized.customers.customer_id carries a real UNIQUE constraint (uq_customers_customer_id), replacing the plain index migration 0005 created"
  - "MetadataRepository.get_or_create_ingestion_run -- idempotent, no-op-upsert, discovery-time run pre-allocation"
  - "MetadataRepository.claim_ingestion_run -- conditional UPDATE...WHERE, pod-startup-time exclusive claim with expired-lease takeover"
  - "MetadataRepository.finalize_publication -- the one method that shares a caller-supplied, already-open transaction (META-03)"
  - "MetadataRepository.create_file -- now an idempotent upsert, gained duplicate_of_file_id"
  - "ObjectStore.list_objects / put_object -- S3ObjectStore implementations through the existing boto3 client"
  - "ConfigRegistry.get_by_id -- resolve a historical DatasetConfig by meta.config_versions.config_version_id"
  - "PipelineContext.source (Source | None) and RunContext.file_id/batch_id (int | None) fields"
affects: [04-02, 04-03, 04-04, 04-05, 04-06, 04-07, 04-08, 04-09]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Two distinct SQL statements for two distinct ingestion-run lifecycle moments: a no-op INSERT...ON CONFLICT upsert for discovery-time pre-allocation vs. a conditional UPDATE...WHERE for pod-startup-time exclusive claim -- never conflated into one query (Pitfall 5)"
    - "Transaction-sharing repository method: finalize_publication takes a caller-supplied Connection[Any] instead of opening its own from the pool, so its updates land in the same transaction as Publisher.publish's own write (META-03)"
    - "Idempotent business-identity upsert: INSERT ... ON CONFLICT (<business-identity columns>) DO UPDATE ... RETURNING <id>, used for both meta.ingestion_runs (idempotency_key) and meta.files (dataset_id, object_uri, content_sha256)"

key-files:
  created:
    - migrations/versions/0006_normalized_customers_business_key_unique.py
  modified:
    - packages/dataplat/src/dataplat/pipeline/protocol.py
    - packages/dataplat/src/dataplat/models/identity.py
    - packages/dataplat/src/dataplat/metadata/repository.py
    - packages/dataplat/src/dataplat/metadata/postgres.py
    - packages/dataplat/src/dataplat/storage/objectstore.py
    - packages/dataplat/src/dataplat/config/registry.py
    - tests/integration/test_migrations.py
    - tests/integration/test_metadata_repository.py
    - tests/integration/test_objectstore.py
    - tests/integration/test_config_registry.py

key-decisions:
  - "get_or_create_ingestion_run and claim_ingestion_run stay two separate methods/SQL statements per Pitfall 5, documented in both docstrings as never-conflatable"
  - "finalize_publication never opens its own connection -- caller-supplied conn only -- so files/batches/ingestion_runs updates share Publisher.publish's transaction (META-03); this is the one MetadataRepository method with that exception, called out prominently in both the Protocol and implementation docstrings"
  - "create_file's ON CONFLICT target is the real uq_files_dataset_uri_content UNIQUE constraint added by migration 0002 -- confirmed by reading the migration before relying on it, not assumed"
  - "list_objects collects every ListObjectsV2 page eagerly inside its try/except (not a lazy generator), so a ClientError/BotoCoreError raises synchronously from the call itself, matching get_object's existing synchronous-raise contract"

patterns-established:
  - "Idempotent business-identity upsert (INSERT ... ON CONFLICT ... DO UPDATE ... RETURNING) as the standard shape for any meta.* row keyed by a natural/business identity, not a surrogate the caller doesn't yet know"
  - "A Protocol method that intentionally breaks the 'every method opens its own pool connection' convention must say so in its own docstring, name the exact transaction it must share, and name the threat register entry (T-04-06) governing who may supply that connection"

requirements-completed: [LOAD-09, LOAD-08, LOAD-04, META-03]

# Metrics
duration: ~35min
completed: 2026-08-13
---

# Phase 4 Plan 01: Data-Access-Layer Contracts Summary

**Idempotent ingestion-run upserts (discovery-time pre-allocation vs. pod-startup-time exclusive claim), a transaction-sharing `finalize_publication`, and `ObjectStore`/`ConfigRegistry` list/write/by-ID seams -- the exact data-access signatures every later Phase 4 plan (staging, publish, discovery, CLI orchestration, DAGs) calls against.**

## Performance

- **Duration:** ~35 min
- **Started:** 2026-08-13T13:42:00Z (approx.)
- **Completed:** 2026-08-13T14:17:10Z
- **Tasks:** 3 (Task 2 executed as TDD RED -> GREEN)
- **Files modified:** 10 (1 created, 9 modified) across the 3 task commits, plus REQUIREMENTS.md and this SUMMARY.md in the metadata commit

## Accomplishments

- Migration 0006 replaces `normalized.customers`' plain `ix_customers_customer_id` index with a real `uq_customers_customer_id` UNIQUE constraint, so a later plan's `INSERT ... ON CONFLICT (customer_id)` publisher has a legal conflict target (LOAD-09) -- upgrade/downgrade/re-upgrade all proven against a fresh testcontainers Postgres.
- `MetadataRepository`/`PostgresMetadataRepository` gained the two distinct ingestion-run upserts (`get_or_create_ingestion_run` for discovery, `claim_ingestion_run` for pod-startup exclusivity with expired-lease takeover), `finalize_publication` (the one method sharing the caller's own transaction, proving META-03's atomicity by test), and a duplicate-aware, idempotent `create_file`.
- `ObjectStore`/`S3ObjectStore` can now list (`list_objects`, paginated past the 1000-key `ListObjectsV2` boundary) and write (`put_object`), through the exact same boto3 client `get_object` already built.
- `ConfigRegistry.get_by_id` resolves a dataset's config exactly as it was at a specific `config_version_id`, without reading `configs/*.yaml` from disk -- the mechanism historical reprocessing needs.
- `PipelineContext.source` and `RunContext.file_id`/`batch_id` close the identity-wiring gap plans 04-04/04-05 would otherwise each answer differently.

## Task Commits

Each task was committed atomically:

1. **Task 1: Migration 0006 and PipelineContext.source field** - `e7f0092` (feat)
2. **Task 2: MetadataRepository extension (TDD)** - `efc415e` (test, RED) then `c4d17a0` (feat, GREEN)
3. **Task 3: ObjectStore listing/writing and ConfigRegistry.get_by_id** - `3db928a` (feat)

**Plan metadata:** committed alongside this SUMMARY.md (see final commit in this plan's range)

_Task 2 was `tdd="true"`: RED committed 10 failing tests (confirmed `AttributeError`/`TypeError` against the pre-existing `PostgresMetadataRepository`) before any implementation landed; GREEN made all 10 pass with zero changes to the RED-phase test file. No REFACTOR commit was needed -- the GREEN implementation required no follow-up cleanup._

## Files Created/Modified

- `migrations/versions/0006_normalized_customers_business_key_unique.py` - drops the plain index, adds `uq_customers_customer_id`; reversible
- `packages/dataplat/src/dataplat/pipeline/protocol.py` - `PipelineContext.source: Source | None = None`
- `packages/dataplat/src/dataplat/models/identity.py` - `RunContext.file_id`/`batch_id: int | None = None`
- `packages/dataplat/src/dataplat/metadata/repository.py` - `get_or_create_ingestion_run`, `claim_ingestion_run`, `finalize_publication` Protocol methods; `create_file` gains `duplicate_of_file_id`
- `packages/dataplat/src/dataplat/metadata/postgres.py` - `PostgresMetadataRepository` implementations of all four
- `packages/dataplat/src/dataplat/storage/objectstore.py` - `ObjectSummary` dataclass; `ObjectStore.list_objects`/`put_object` Protocol methods and `S3ObjectStore` implementations
- `packages/dataplat/src/dataplat/config/registry.py` - `ConfigRegistry.get_by_id`
- `tests/integration/test_migrations.py` - migration 0006 constraint + downgrade/re-upgrade round trip
- `tests/integration/test_metadata_repository.py` - 10 new tests covering every `<behavior>` bullet for Task 2
- `tests/integration/test_objectstore.py` - `list_objects`/`put_object` tests
- `tests/integration/test_config_registry.py` - `get_by_id` round-trip and not-found tests
- `.planning/phases/04-vertical-slice-csv-to-analytical-postgresql/deferred-items.md` - logs one pre-existing, out-of-scope test failure found during full-gate verification (see Issues Encountered)
- `.planning/REQUIREMENTS.md` - LOAD-09, LOAD-08, LOAD-04, META-03 marked complete (this plan's frontmatter `requirements` list)

## Decisions Made

- `get_or_create_ingestion_run` and `claim_ingestion_run` are kept as two distinct methods/SQL statements (never merged), matching Pitfall 5 exactly -- each docstring cross-references the other and states why conflating them would be wrong.
- `finalize_publication` is the one `MetadataRepository` method that never opens its own pool connection; it takes a caller-supplied, already-open `Connection[Any]` so its three UPDATEs land inside the same transaction as `Publisher.publish`'s own `INSERT ... ON CONFLICT` (META-03). Proven by test: a second connection cannot see any of the three updates until the caller's own connection commits.
- Before relying on `ON CONFLICT (dataset_id, object_uri, content_sha256)` for `create_file`, the plan's instruction to "confirm this is a real conflict target, not merely an index" was followed by reading migration 0002 directly -- `uq_files_dataset_uri_content` is a genuine `UniqueConstraint`, not an index.
- `list_objects` eagerly collects every `ListObjectsV2` page inside its `try`/`except` rather than yielding lazily from a generator, so a `ClientError`/`BotoCoreError` surfaces synchronously from the call -- matching `get_object`'s existing contract, per the plan's "copy the exact try/except shape" instruction.

## Deviations from Plan

None - plan executed exactly as written. All three tasks' acceptance criteria, `<behavior>` bullets (Task 2), and the plan-level `<verification>` block were met without any Rule 1-4 auto-fixes.

## Issues Encountered

- **Pre-existing, out-of-scope test failure found during full-gate verification (`make check`):** `tests/policy/test_gates_actually_fail.py::test_forbidden_import_is_rejected` and `::test_good_forbidden_import_is_accepted` both fail because the pinned `import-linter==2.13` now renders its KEPT/BROKEN status word with an inline ANSI color escape sequence, breaking a plain-substring assertion written against an earlier rendering. This file was last touched by Phase 1 (commit `edf4756`), is unrelated to every file this plan modifies, and the real Contract 1 (`dataplat core must not depend on the CSV plugin`) is independently confirmed `KEPT` throughout this plan's execution. Not auto-fixed per the scope-boundary rule; logged with full detail and a suggested resolution in `.planning/phases/04-vertical-slice-csv-to-analytical-postgresql/deferred-items.md`.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Every signature 04-CONTEXT.md's canonical-refs and the plan's own `<interfaces>` section named is now live and integration-tested against real testcontainers PostgreSQL/MinIO: 04-02 through 04-09 can call `get_or_create_ingestion_run`, `claim_ingestion_run`, `finalize_publication`, the duplicate-aware `create_file`, `list_objects`/`put_object`, `get_by_id`, and read `ctx.run.file_id`/`batch_id` / `ctx.source` without inventing or re-deriving any of them independently.
- No blockers. `alembic upgrade head` is at `0006`; every Phase 3 integration/unit test still passes (31 integration, 111 unit); `mypy --strict`, `ruff check`/`format`, and `import-linter` all pass on every file this plan touches.
- One unrelated, pre-existing gate failure (see Issues Encountered) is documented for a future plan or maintenance pass to pick up; it does not block Phase 4's critical path.

---
*Phase: 04-vertical-slice-csv-to-analytical-postgresql*
*Completed: 2026-08-13*
