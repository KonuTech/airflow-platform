---
phase: 04-vertical-slice-csv-to-analytical-postgresql
plan: 04
subsystem: database

# Dependency graph
requires:
  - phase: 04-vertical-slice-csv-to-analytical-postgresql
    provides: "plan 04-01's RunContext.file_id/batch_id fields and migration 0006's UNIQUE(customer_id) constraint on normalized.customers"
provides:
  - "StagingLoader — chunked COPY from any Source into a clean, retry-safe, UNLOGGED staging.<dataset>__r<run_id> table"
  - "MergePublisher — the first concrete Publisher: pg_advisory_xact_lock (caller-side) + INSERT ... ON CONFLICT ... WHERE, never MERGE"
  - "PUBLISHER_REGISTRY / resolve_publisher — strategy-key lookup for the load layer"
  - "migration 0007 — the staging schema, previously missing"
affects: ["04-05 (run_ingest orchestration consumes StagingLoader + resolve_publisher directly)", "04-06 (concurrency test against this plan's unmodified publish SQL)"]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Staging tables: all-TEXT business columns + real-typed lineage columns; DROP TABLE IF EXISTS then CREATE UNLOGGED per attempt (never ON COMMIT DROP, which is invalid syntax for UNLOGGED tables)"
    - "Publication: pg_advisory_xact_lock taken by the caller immediately before Publisher.publish(), never inside publish() itself; INSERT ... ON CONFLICT ... WHERE with DISTINCT ON as the structural in-batch dedup guard, never literal SQL MERGE"

key-files:
  created:
    - packages/dataplat/src/dataplat/load/staging.py
    - packages/dataplat/src/dataplat/load/publish/merge.py
    - packages/dataplat/src/dataplat/load/publish/registry.py
    - migrations/versions/0007_staging_schema.py
    - tests/integration/test_staging_loader.py
    - tests/integration/test_publish_merge.py
    - tests/unit/test_publisher_registry.py
  modified:
    - tests/integration/test_migrations.py

key-decisions:
  - "Added migration 0007 (CREATE SCHEMA staging + GRANT USAGE,CREATE TO etl_app) -- no prior migration created this schema, and ARCHITECTURE.md names it as one of three required analytical schemas; without it StagingLoader's first CREATE TABLE call would fail"
  - "StagingLoader.chunk_size is stored but not used to re-batch rows: one COPY runs per chunk ctx.source itself yields, so chunk/on_progress/log-line granularity stays entirely the Source's concern, matching the plan's literal per-chunk COPY recipe"
  - "MergePublisher hardcodes normalized.customers and its column list rather than resolving from ctx.config.load.target -- deliberately single-dataset for this vertical-slice phase, per the plan's Interfaces section"
  - "The pre-migration-0006 negative test drops/restores normalized.customers's UNIQUE constraint on the shared, already-migrated database instead of provisioning a second dedicated container -- cheaper, and safe because tests/integration/ runs sequentially"

patterns-established:
  - "Positional (not name-based) row-to-target_columns correspondence for this phase's naive CsvSource -- header-to-column mapping is Phase 6 territory"
  - "_record_hash = hashlib.sha256('|'.join(row).encode('utf-8')).digest(), computed exactly once in Python during staging, carried unchanged through publish"

requirements-completed: [LOAD-05, LOAD-08, LOAD-09, LOAD-12]

# Metrics
duration: ~55min
completed: 2026-08-13
---

# Phase 04 Plan 04: Load Layer — StagingLoader and MergePublisher Summary

**Chunked COPY staging plus `pg_advisory_xact_lock` + `INSERT ... ON CONFLICT ... WHERE` publication (never literal SQL `MERGE`), proven against a real testcontainers PostgreSQL including the dedup, no-clobber, no-op-republish and pre-migration-0006 negative cases.**

## Performance

- **Duration:** ~55 min
- **Completed:** 2026-08-13T15:04:00Z
- **Tasks:** 2
- **Files modified:** 8 (7 created, 1 modified)

## Accomplishments

- `StagingLoader` streams any `Source` through `RaggedRowGuard` into a clean, `UNLOGGED`, all-TEXT-plus-lineage staging table — retry-safe (`DROP TABLE IF EXISTS` first), never silently loses a ragged row, computes `_record_hash` exactly once in Python, and reports live progress via `on_progress` for a future heartbeat.
- `MergePublisher` implements the corrected publication statement ARCHITECTURE.md's own `MERGE` example got wrong: `INSERT ... ON CONFLICT (customer_id) DO UPDATE ... WHERE`, with `DISTINCT ON (customer_id)` structurally preventing the documented `ON CONFLICT DO UPDATE command cannot affect row a second time` failure mode.
- Discovered and fixed a genuine precondition gap: no migration created the `staging` schema `StagingLoader` needs. Added migration `0007`, and fixed the one existing test (`test_0006_downgrade_restores...`) whose relative `downgrade -1` broke as a direct consequence of a new head revision existing above it.

## Task Commits

Each task was committed atomically:

1. **Task 1: StagingLoader — chunked COPY into an all-TEXT staging table** - `f6b032a` (feat)
2. **Task 2: PUBLISHER_REGISTRY and MergePublisher — the corrected publication statement** - `d789c83` (feat)

## Files Created/Modified

- `packages/dataplat/src/dataplat/load/staging.py` - `StagingLoader`/`StagingResult`: chunked COPY into `staging.<dataset>__r<run_id>`
- `packages/dataplat/src/dataplat/load/publish/merge.py` - `MergePublisher`: the corrected `pg_advisory_xact_lock` + `ON CONFLICT` publication SQL
- `packages/dataplat/src/dataplat/load/publish/registry.py` - `PUBLISHER_REGISTRY`/`resolve_publisher`
- `migrations/versions/0007_staging_schema.py` - creates schema `staging`; grants `etl_app` `USAGE, CREATE`
- `tests/integration/test_staging_loader.py` - 6 tests against real testcontainers Postgres
- `tests/integration/test_publish_merge.py` - 5 tests, including the pre-migration-0006 negative case
- `tests/unit/test_publisher_registry.py` - 3 DB-free registry lookup/error-path tests
- `tests/integration/test_migrations.py` - `downgrade -1` → explicit `downgrade "0005"` (fix for migration 0007's regression)

## Decisions Made

- **`StagingLoader.chunk_size` does not control chunk boundaries.** The plan's constructor signature names it explicitly, but the plan's own `load()` recipe issues one `COPY` per chunk `ctx.source` yields, with no re-batching step described. Rather than inventing an undocumented re-batching behavior, I kept `chunk_size` stored (mirrors `CsvSource`'s own knob, same default) and documented in both the class and `load()` docstrings that chunk/`on_progress`/log-line granularity is entirely the `Source`'s concern. The "force 3 chunks" behavior test achieves this via a `Source` test double with 3 pre-built chunks, independent of `StagingLoader.chunk_size`.
- **Business-column-to-`target_columns` correspondence is positional, not name-based.** This phase's `CsvSource` has no header-to-column mapping (naive, hardcoded dialect), so `StagingLoader` documents this as this phase's contract explicitly, rather than silently assuming it.
- **`MergePublisher` is single-dataset, not generic.** `normalized.customers` and its column list are hardcoded in the SQL rather than resolved from `ctx.config.load.target` — matches the plan's Interfaces section literally ("parameterize `staging_table` via an f-string ONLY for the table identifier"), and a generic upsert-any-table publisher is explicitly out of this vertical slice's scope.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical Functionality] Added migration 0007 to create the `staging` schema**
- **Found during:** Task 1, before writing `staging.py`'s tests — confirmed via `grep` across `migrations/versions/` that no schema named `staging` was ever created (only `meta` and `normalized` exist), and `helm/values/local/cnpg-analytics.yaml`'s own comment states "No schema: every schema is Alembic's."
- **Issue:** `ARCHITECTURE.md` names `meta`/`staging`/`normalized` as the three schemas the analytical database needs, but nothing in migrations `0001`–`0006` (or any other 04-* plan) creates `staging`. Without it, `StagingLoader.load()`'s first `CREATE UNLOGGED TABLE staging....` would fail with `schema "staging" does not exist` against every freshly-migrated database — a total blocker for this plan's entire purpose.
- **Fix:** Added `migrations/versions/0007_staging_schema.py`: `CREATE SCHEMA IF NOT EXISTS staging` plus `GRANT USAGE, CREATE ON SCHEMA staging TO etl_app` (this schema differs from `meta`/`normalized` in kind, not just name — `etl_app` DDLs its own throwaway tables here at runtime, so it needs `CREATE` on the schema itself, not merely `SELECT, INSERT, UPDATE` on one fixed table). Downgrade revokes the grant and deliberately does not drop the schema, matching migrations `0001`/`0005`'s own established convention.
- **Files modified:** `migrations/versions/0007_staging_schema.py`
- **Verification:** `alembic upgrade head` (via `migrated_dsn`) succeeds; all 6 `test_staging_loader.py` tests pass against the resulting schema; the full `tests/integration/` suite (42 tests) passes.
- **Committed in:** `f6b032a` (Task 1 commit)

**2. [Rule 1 - Bug] Fixed a regression in `test_0006_downgrade_restores_the_plain_index_and_reupgrade_restores_the_constraint`**
- **Found during:** Task 1, running the full `tests/integration/test_migrations.py` suite after adding migration 0007.
- **Issue:** That pre-existing test called `command.downgrade(alembic_config, "-1")`, relying on `0006` being the current head so that "one step back" landed on `0005` (reversing exactly migration 0006's change, which is what the test's name and assertions actually check). Adding migration `0007` as the new head broke this: `"-1"` now reverses `0007` instead, leaving the `UNIQUE` constraint from `0006` still in place — the test's own assertion (`_customers_customer_id_constraint_types(migrated_dsn) == ()`) then failed with `assert ('UNIQUE',) == ()`, confirmed by direct reproduction.
- **Fix:** Changed the call to `command.downgrade(alembic_config, "0005")` — an explicit target revision expresses the test's actual intent ("undo exactly 0006's change") and stays correct regardless of how many further migrations are added above it later.
- **Files modified:** `tests/integration/test_migrations.py`
- **Verification:** `tests/integration/test_migrations.py`'s full 7-test suite passes; the whole `tests/integration/` suite (42 tests) passes.
- **Committed in:** `f6b032a` (Task 1 commit, bundled with the migration that caused the regression)

**3. [Rule 2 - Missing Critical Functionality] Added a unit test for `PUBLISHER_REGISTRY`/`resolve_publisher`**
- **Found during:** Task 2, reviewing the plan's `<done>` criterion ("`PUBLISHER_REGISTRY` resolves `"merge"` to it") against my own test coverage — no `<behavior>` bullet named the registry explicitly, but the `<done>` criterion and the ACTION text's explicit request for `resolve_publisher`'s `ConfigurationError` path were otherwise unproven.
- **Fix:** Added `tests/unit/test_publisher_registry.py` — 3 DB-free tests proving `PUBLISHER_REGISTRY["merge"]` is a `MergePublisher`, `resolve_publisher("merge")` returns that same instance, and `resolve_publisher("does-not-exist")` raises `ConfigurationError` with the expected `context`.
- **Files modified:** `tests/unit/test_publisher_registry.py`
- **Verification:** All 3 tests pass; `tests/unit`+`tests/regression` (114 tests total) pass.
- **Committed in:** `d789c83` (Task 2 commit)

---

**Total deviations:** 3 auto-fixed (2 missing-critical-functionality, 1 bug-fix)
**Impact on plan:** All three were necessary for correctness or genuinely proving what the plan's own `<done>` criteria claim. No scope creep — nothing beyond what this plan's two tasks require.

## Issues Encountered

- The plan's acceptance-criteria grep (`"MERGE INTO\|WHEN MATCHED\|WHEN NOT MATCHED"`) is broader than the `<verify>` block's actual automated command (`grep -c "MERGE INTO" ... | grep -qx 0`). My first draft of `merge.py`'s module docstring explained *why* literal `MERGE`'s `WHEN MATCHED`/`WHEN NOT MATCHED` branching is unsafe, which itself matched the broader pattern even though no SQL `MERGE` was ever used. Reworded the explanation to avoid those literal phrases while keeping the same technical content, so both the acceptance-criteria grep and the `<verify>` command return zero matches unambiguously.
- Ruff's `S608` (hardcoded-SQL-expression) rule did not fire on this plan's `CREATE TABLE`/`DROP TABLE`/`COPY` f-strings (only genuinely `SELECT`-shaped ones in the test files needed `# noqa: S608`) — an early round of defensive `# noqa: S608` comments on the DDL/COPY statements were flagged as unused directives and removed via `ruff check --fix`.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Plan 04-05's `run_ingest` orchestration can now call `StagingLoader(target_columns=(...)).load(ctx, conn, on_progress=...)` and `resolve_publisher(ctx.config.load.strategy)` exactly as sketched in 04-RESEARCH.md's pod-entrypoint pseudocode — both constructor/call shapes were built to match that sketch precisely.
- Plan 04-06's concurrency test (two overlapping publish attempts against the same dataset) can be written directly against `MergePublisher.publish()` without touching this plan's SQL — the `pg_advisory_xact_lock`/`ON CONFLICT` design was built specifically so that later test would pass unmodified.
- No blockers. The one open design note (`StagingLoader.chunk_size`'s stored-but-unused status) is documented in both the source and this summary so a future reader is not confused by it.

---
*Phase: 04-vertical-slice-csv-to-analytical-postgresql*
*Completed: 2026-08-13*
