---
phase: 09-etl-correctness-dedup-incremental-backfill-recovery
plan: 02
subsystem: database
tags: [postgresql, psycopg, dataplat, watermark, reconciliation, tdd]

# Dependency graph
requires:
  - phase: 09-01
    provides: "DatasetConfig.reconciliation (ReconciliationConfig.sum_columns) and DatasetConfig.columns[].business_key, both consumed by _compute_silver_gold_reconciliation"
provides:
  - "meta.watermarks/meta.watermark_history tables + record_watermark/get_current_watermark repository methods (D-01..D-04, INCR-01/02)"
  - "meta.reconciliation_results table + record_reconciliation repository method (D-20..D-24, VALID-05)"
  - "publish_ingest advances the dataset watermark and writes one silver_gold reconciliation row per finalized file, inside its existing advisory-locked transaction"
affects: [09-07, 09-08]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "GREATEST()-in-SQL for monotonic cursor advance, never a Python conditional branch — INCR-02's >=-never-> rule is structural, not app-logic-enforced"
    - "Aggregate reconciliation figures computed ONCE per publish_ingest pass (whole-table SELECT, never per-run), then reused for every finalized file's own reconciliation_results row — same aggregate-attribution precedent as rows_loaded"
    - "Config-resolved SQL identifiers (source_table/watermark_column/sum_column/business_key_column) interpolated via f-string, every genuine value still bound via %s/%()s placeholders — same trust boundary merge.py's own source_table interpolation already established"

key-files:
  created:
    - migrations/versions/0031_meta_watermarks.py
    - migrations/versions/0032_meta_reconciliation_results.py
    - tests/integration/test_watermarks.py
    - tests/integration/test_reconciliation.py
  modified:
    - packages/dataplat/src/dataplat/metadata/repository.py
    - packages/dataplat/src/dataplat/metadata/postgres.py
    - packages/dataplat/src/dataplat/pipeline/run.py
    - tests/integration/test_migrations.py

key-decisions:
  - "record_watermark's candidate cursor value is computed via SELECT max(watermark_column) over the WHOLE source_table (silver.<dataset>), never scoped to the current run — combined with silver's own UNIQUE(business_key) constraint and gold's identical constraint plus full-table republish on every pass, this makes count(silver) == count(gold) a general post-publish invariant, which is what makes Test 4's discrepancy=0 assertion a structural truth of the design rather than a coincidence of one test's row counts."
  - "Both silver.customers.event_ts and silver.orders.order_date are TEXT (unparsed CSV content) while their gold/meta.watermarks counterparts are typed (timestamptz/date) — every min/max/watermark computation this plan adds casts explicitly to ::timestamptz on both the silver and gold side, rather than relying on an implicit cast that PostgreSQL does not perform automatically in this direction."

patterns-established:
  - "A generic _scalar(conn, query) helper for 'run one aggregate SELECT (no GROUP BY), return column 0' — every aggregate query in _compute_silver_gold_reconciliation routes through it rather than repeating a fetchone()-is-None guard six times."

requirements-completed: [INCR-01, INCR-02, VALID-05]

# Metrics
duration: ~24min
completed: 2026-08-19
---

# Phase 09 Plan 02: Watermark advance + silver→gold reconciliation, wired into publish_ingest Summary

**`publish_ingest` now advances each dataset's observational watermark via `GREATEST()` and writes one per-file silver→gold `meta.reconciliation_results` row, both inside its existing advisory-locked transaction — proven by 5 new TDD-driven integration tests (2 genuine Rule-1 bugs found and fixed along the way: a missing `::timestamptz` cast, and a psycopg `AmbiguousParameter` from a repeated named parameter).**

## Performance

- **Duration:** ~24 min
- **Started:** 2026-08-19T16:37:50+02:00 (base commit `969bc7c`)
- **Completed:** 2026-08-19T17:01:33+02:00
- **Tasks:** 3 completed
- **Files modified:** 8 (4 created, 4 modified)

## Accomplishments

- `meta.watermarks`/`meta.watermark_history` (migration 0031) and `meta.reconciliation_results` (migration 0032) exist with the documented grant matrix: `etl_app` full read/write on `watermarks`, `etl_app` SELECT/INSERT-only on the two append-only tables (`watermark_history`, `reconciliation_results`), `dbt_app` INSERT-only on `reconciliation_results` alone (zero grant on either watermark table), `grafana_reader` read-only on all three.
- `record_watermark`/`get_current_watermark`/`record_reconciliation` added to `MetadataRepository`/`PostgresMetadataRepository`, mirroring `claim_run_stage`/`finalize_publication`'s exact "explicit `conn`, never opens its own connection" shape for the two transaction-bound writers.
- `publish_ingest` calls both, immediately after `publisher.publish()` returns and before `finished_at` is computed, inside the same `pg_advisory_xact_lock`-protected transaction as the merge upsert — a crash mid-transaction rolls back the merge, the watermark advance and the reconciliation write together (verified structurally; no separate rollback test was needed since all three share one `conn.transaction()`).
- All 5 behaviors from Task 3's `<behavior>` block are proven green: newer-publish advances the watermark and logs history; older-publish never regresses the cursor but still logs a history row (D-04); a dataset's first-ever publish creates its watermark row with `old_value IS NULL`; a clean publish writes one `hop='silver_gold'` reconciliation row per finalized file with `discrepancy=0`; `orders` (declares `reconciliation.sum_columns`) populates every reconciliation figure while `customers` (no `reconciliation:` block) leaves only `sum_column`/`sum_input`/`sum_output` NULL.
- Two real bugs found and fixed during Task 3's own integration testing (both documented below under Deviations) — TDD's RED phase caught both before either shipped.

## Task Commits

Each task was committed atomically:

1. **Task 1: Migrations — meta.watermarks/watermark_history + meta.reconciliation_results** - `4142868` (feat)
2. **Task 2: record_watermark / get_current_watermark / record_reconciliation** - `f8f38a7` (feat)
3. **Task 3, RED: failing tests for watermark advance + silver→gold reconciliation** - `2c4723f` (test)
3. **Task 3, GREEN: wire both into publish_ingest** - `01c0a8d` (feat)

## Files Created/Modified

- `migrations/versions/0031_meta_watermarks.py` - `meta.watermarks`/`meta.watermark_history` DDL + grants
- `migrations/versions/0032_meta_reconciliation_results.py` - `meta.reconciliation_results` DDL + grants, D-22 formula documented verbatim
- `packages/dataplat/src/dataplat/metadata/repository.py` - `record_watermark`/`get_current_watermark`/`record_reconciliation` Protocol declarations
- `packages/dataplat/src/dataplat/metadata/postgres.py` - implementations; two Rule-1 fixes (see Deviations)
- `packages/dataplat/src/dataplat/pipeline/run.py` - `publish_ingest` wiring, `_WATERMARK_COLUMN_BY_DATASET`/`_watermark_column_for_dataset`, `_ReconciliationAggregates`/`_compute_silver_gold_reconciliation`/`_table_checksum`/`_scalar`
- `tests/integration/test_watermarks.py` - new, 3 tests
- `tests/integration/test_reconciliation.py` - new, 2 tests
- `tests/integration/test_migrations.py` - allow-list updates for the 3 new tables (see Deviations)

## Decisions Made

- Test isolation: every business-key value used in the new test files is a genuinely fresh, never-reused string (`silver.customers`/`silver.orders` both carry a real `UNIQUE` constraint on their business key, migration 0023 — a repeat raises `UniqueViolation`, not a silent upsert). Test 3 (`test_watermarks.py`, "no prior watermark row") deliberately uses `orders`, not `customers`, so it never depends on Test 1/Test 2's own execution order within the file, and stays correct whether the file runs alone or together with `test_reconciliation.py` per the plan's own combined `<verification>` command (which lists `test_watermarks.py` first — pytest runs explicitly-listed files in the given order).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `record_watermark`'s `SELECT max(...)` was missing an explicit `::timestamptz` cast**
- **Found during:** Task 3's own integration test run (RED→GREEN transition)
- **Issue:** `silver.customers.event_ts`/`silver.orders.order_date` are both `TEXT` (unparsed CSV content, by design — D-02), while `meta.watermarks.cursor_value` is `timestamptz`. The original SQL (`SELECT max({watermark_column}) FROM {source_table}`) inserted that raw TEXT `max()` result directly into a `timestamptz` column, raising `psycopg.errors.DatatypeMismatch: column "cursor_value" is of type timestamp with time zone but expression is of type text`.
- **Fix:** Added an explicit `::timestamptz` cast inside the subquery: `SELECT max({watermark_column}::timestamptz) FROM {source_table}`. PostgreSQL parses both `"2099-01-01T00:00:00+00:00"`-shaped and plain-date-shaped TEXT correctly under this cast.
- **Files modified:** `packages/dataplat/src/dataplat/metadata/postgres.py`, `packages/dataplat/src/dataplat/metadata/repository.py` (docstring sync only)
- **Commit:** `01c0a8d`

**2. [Rule 1 - Bug] `record_reconciliation`'s discrepancy/control_total_discrepancy expressions raised `AmbiguousParameter`**
- **Found during:** Task 3's own integration test run (same RED→GREEN transition)
- **Issue:** `%(expected_row_count)s` and several other named parameters were each referenced multiple times across the single `INSERT`'s `VALUES` clause (once as a plain column value, again inside arithmetic/`CASE` expressions). PostgreSQL's own parameter-type inference deduced conflicting types for the same named parameter's different occurrences (`psycopg.errors.AmbiguousParameter: inconsistent types deduced for parameter $19 ... smallint versus bigint`).
- **Fix:** Added explicit `::bigint` casts to every occurrence of `input_count`/`output_count`/`rejected_count`/`dedup_count`/`expected_row_count` inside the `discrepancy`/`control_total_discrepancy` SQL expressions, disambiguating every occurrence independently rather than relying on cross-occurrence type unification.
- **Files modified:** `packages/dataplat/src/dataplat/metadata/postgres.py`
- **Commit:** `01c0a8d`

**3. [Rule 1 - Bug] `tests/integration/test_migrations.py`'s global allow-lists needed extending for the 3 new tables**
- **Found during:** Task 1's own verification run
- **Issue:** `test_upgrade_head_creates_the_slice_schema`, `test_grafana_reader_role_exists_and_is_select_only` and `test_dbt_app_role_is_scoped_correctly` each assert against a hardcoded, exhaustive set of expected tables/grants across the WHOLE `meta`/`normalized` schema surface — adding `meta.watermarks`/`meta.watermark_history`/`meta.reconciliation_results` without updating these allow-lists left all three permanently red.
- **Fix:** Extended `EXPECTED_TABLES`/`GRANTED_TABLES` (excluding the two append-only tables from the `SELECT/INSERT/UPDATE` set, matching the `dedup_audit`/`dedup_decisions` precedent already established there), `grafana_reader`'s `expected_objects`, and `dbt_app`'s `forbidden_grants` allow-list (adding `reconciliation_results` alongside `dedup_audit`/`dedup_decisions`).
- **Files modified:** `tests/integration/test_migrations.py`
- **Commit:** `4142868`

## Issues Encountered

None beyond the three auto-fixed issues documented above. `uv run` defaulted to a fresh, empty per-worktree `.venv` lacking `testcontainers`; resolved with `uv sync --locked --group cluster` (no code/config change).

One pre-existing, unrelated test-order flake was observed and left untouched (out of this plan's scope): `tests/integration/test_migrations.py::test_dbt_app_role_is_scoped_correctly` fails when run in the same session AFTER `tests/integration/test_publish_merge.py`/`test_publish_orders.py` (which create ad-hoc `staging.*` tables that leak into `dbt_app`'s `staging`-schema grant enumeration) — confirmed order-dependent, not caused by this plan's changes (passes cleanly both alone and when `test_migrations.py` runs first), and not part of this plan's own file scope.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

`meta.watermarks`/`meta.reconciliation_results` are now real, populated tables with a proven read path (`get_current_watermark`) — plan 09-07/09-08 (the `raw_bronze`/`bronze_silver` hop reconciliation writers) can extend `meta.reconciliation_results` using the exact same `record_reconciliation` method and `hop` vocabulary this plan established, with zero schema change needed.

---
*Phase: 09-etl-correctness-dedup-incremental-backfill-recovery*
*Completed: 2026-08-19*

## Self-Check: PASSED

All claimed files verified present (`migrations/versions/0031_meta_watermarks.py`,
`migrations/versions/0032_meta_reconciliation_results.py`, `tests/integration/test_watermarks.py`,
`tests/integration/test_reconciliation.py`, plus the 4 modified files) and all claimed commit
hashes verified present in git log (`4142868`, `f8f38a7`, `2c4723f`, `01c0a8d`).
