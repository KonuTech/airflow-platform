---
phase: 10-slowly-changing-dimensions
plan: 01
subsystem: database
tags: [alembic, postgresql, scd2, exclude-constraint, pydantic, publisher-protocol]

# Dependency graph
requires:
  - phase: 09-etl-correctness-dedup-incremental-backfill-recovery
    provides: normalized.customers (one-row-per-customer_id shape), Publisher protocol, publish_ingest orchestration
provides:
  - "migration 0035: normalized.customers migrated in place to an SCD2-capable shape (btree_gist EXCLUDE constraint, valid_to/is_current/signup_country/validity columns, etl_app DELETE grant)"
  - "ColumnContract.scd_type (type_0/type_1/type_2) and ScdConfig (delete_semantics/mass_delete_threshold) in DatasetConfig"
  - "customers.yaml carries signup_country, per-column scd_type tags, and a scd: block"
  - "Publisher.publish() accepts staged_run_ids everywhere it is implemented or called"
affects: [10-02, 10-03, 10-04, 10-05, 10-06]

tech-stack:
  added: []
  patterns:
    - "PostgreSQL EXCLUDE USING gist as the SCD2 non-overlapping-validity backstop, generated STORED tstzrange column"
    - "Publisher.publish() keyword-only staged_run_ids threaded from publish_ingest's own staged list, computed once before the transaction opens"

key-files:
  created:
    - migrations/versions/0035_normalized_customers_scd2.py
  modified:
    - packages/dataplat/src/dataplat/config/model.py
    - configs/datasets/customers.yaml
    - packages/dataplat/src/dataplat/load/publish/protocol.py
    - packages/dataplat/src/dataplat/load/publish/merge.py
    - packages/dataplat/src/dataplat/load/publish/merge_orders.py
    - packages/dataplat/src/dataplat/pipeline/run.py
    - tests/integration/test_migrations.py

key-decisions:
  - "downgrade() drops the EXCLUDE constraint via raw ALTER TABLE DDL, not op.drop_constraint(type_=...) -- Alembic 1.19.1's typed drop-constraint op only supports {check, foreignkey, primary, unique, None}, confirmed by a live TypeError during the downgrade/upgrade round-trip verification"
  - "_customers_customer_id_constraint_types (test_migrations.py) now queries pg_constraint directly instead of information_schema -- EXCLUDE constraints have no information_schema representation at all, and PostgreSQL 18 additionally catalogues NOT NULL as a real pg_constraint row (contype 'n'), both confirmed live via failing assertions before the fix"
  - "MergePublisher/OrdersMergePublisher accept and ignore staged_run_ids (unused by their whole-table ON CONFLICT statements) rather than being touched further -- test_publish_merge.py/test_publish_orders.py/test_referential_integrity.py/test_run_ingest.py's own call sites are deliberately left unfixed, since 10-RESEARCH.md's Finding F-4 and this plan's own <files> scope explicitly defer the full consumer sweep to plans 10-04/10-05 (both depends_on: [10-01])"

patterns-established:
  - "SCD2 dimension migration shape: btree_gist EXCLUDE (business_key, generated STORED tstzrange) as a DB-level backstop, never a Python check-then-act guard"

requirements-completed: [SCD-03, SCD-12]

duration: ~50min
completed: 2026-08-21
---

# Phase 10 Plan 01: SCD2 Foundation (DDL, Config Schema, Publisher Protocol) Summary

**Migrated `normalized.customers` in place to an SCD2 shape with a live-verified `btree_gist` EXCLUDE constraint, added `ColumnContract.scd_type`/`ScdConfig` to `DatasetConfig`, and threaded `staged_run_ids` through the `Publisher` protocol.**

## Performance

- **Duration:** ~50 min (dominated by two live GiST-index builds over `normalized.customers`'s ~10M live rows, ~90s each)
- **Completed:** 2026-08-21
- **Tasks:** 3/3
- **Files modified:** 8 (1 created, 7 modified)

## Accomplishments

- Migration 0035 applies cleanly against the live analytical PostgreSQL and reverses/reapplies cleanly (`alembic downgrade -1 && alembic upgrade head`), both verified live against the real cluster (not testcontainers)
- `normalized.customers` can now hold more than one row per `customer_id`; PostgreSQL itself rejects overlapping validity ranges for the same key via `excl_customers_business_key_validity`
- `ColumnContract.scd_type`/`ScdConfig`/`DatasetConfig.scd` are live in the config model; `customers.yaml` declares `signup_country` (Type-0), `name`/`country` (Type-2), `birth_date` (Type-1), and a `scd:` block
- `Publisher.publish()`'s signature change is threaded end-to-end: protocol → `MergePublisher`/`OrdersMergePublisher` → `publish_ingest`'s single computation site
- `tests/integration/test_migrations.py` fully reflects the new EXCLUDE-constraint shape: rewritten 0006-era tests, a split gold-indexes test, and two new tests (DELETE grant, exclusion-constraint-rejects-overlap)

## Task Commits

1. **Task 1: Migration 0035 -- normalized.customers in-place SCD2 migration** - `1faa9a3` (feat)
2. **Task 2: Config schema -- ColumnContract.scd_type, ScdConfig, customers.yaml additions** - `6e0101c` (feat)
3. **Task 3: Publisher protocol -- staged_run_ids, plus test_migrations.py rewrite for the new constraint shape** - `c1a01cc` (feat)

_Note: no separate plan-metadata commit in worktree mode -- the orchestrator commits SUMMARY.md centrally after merge._

## Files Created/Modified

- `migrations/versions/0035_normalized_customers_scd2.py` - btree_gist extension, drop UNIQUE(customer_id), event_ts NOT NULL, valid_to/is_current/signup_country/validity columns, EXCLUDE USING gist, partial is_current index, GRANT DELETE to etl_app
- `packages/dataplat/src/dataplat/config/model.py` - `_SCD_TYPES` Literal, `ColumnContract.scd_type`, `ScdConfig`, `DatasetConfig.scd`
- `configs/datasets/customers.yaml` - `signup_country` column, `scd_type` tags on name/country/birth_date, `scd:` block
- `packages/dataplat/src/dataplat/load/publish/protocol.py` - `Publisher.publish()` gains keyword-only `staged_run_ids: Sequence[int]`
- `packages/dataplat/src/dataplat/load/publish/merge.py` - `MergePublisher.publish()` accepts and ignores `staged_run_ids`
- `packages/dataplat/src/dataplat/load/publish/merge_orders.py` - `OrdersMergePublisher.publish()` accepts and ignores `staged_run_ids`
- `packages/dataplat/src/dataplat/pipeline/run.py` - `publish_ingest` computes `staged_run_ids` once, right after `staged` is assigned, passes it into `publisher.publish()`
- `tests/integration/test_migrations.py` - `_customers_customer_id_constraint_types` rewritten to query `pg_constraint`; two 0006-era tests rewritten for EXCLUDE shape; gold-indexes test split (orders unchanged / customers replaced); three new tests (customers SCD2 shape, etl_app DELETE grant, exclusion-constraint-rejects-overlap); `test_etl_app_grants` special-cases `normalized.customers`'s new DELETE grant

## Decisions Made

- `downgrade()` for the EXCLUDE constraint uses raw `ALTER TABLE ... DROP CONSTRAINT` DDL, not `op.drop_constraint(type_="exclude")` -- confirmed live that Alembic 1.19.1's typed op raises `TypeError` for `'exclude'` (only `check`/`foreignkey`/`primary`/`unique`/`None` are accepted). This was 10-RESEARCH.md's own flagged Assumption A1, now resolved by live verification rather than assumed.
- `_customers_customer_id_constraint_types` now queries `pg_constraint` directly (not `information_schema.table_constraints`/`key_column_usage`), and explicitly excludes `contype = 'n'` -- confirmed live that PostgreSQL 18 catalogues `NOT NULL` as a real `pg_constraint` row, which the naive `pg_constraint` rewrite initially also picked up (`customer_id nullable=False` since migration 0005), producing a spurious extra tuple element caught by a failing assertion during verification.
- `MergePublisher`/`OrdersMergePublisher`'s call sites in `test_publish_merge.py`/`test_publish_orders.py`/`test_referential_integrity.py`/`test_run_ingest.py` are deliberately left unfixed by this plan, even though they would now raise `TypeError` (missing required keyword argument) if run. This plan's own `<files>` frontmatter and `<verification>` section scope verification to `test_migrations.py` + `tests/unit` only; 10-RESEARCH.md's Finding F-4 explicitly frames the full "find every consumer" sweep as later-plan work, and plans 10-04/10-05 both declare `depends_on: ["10-01"]` and are confirmed (via grep) to reference these exact test files.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `op.drop_constraint(type_="exclude")` is not a valid Alembic op**
- **Found during:** Task 1 (downgrade/upgrade round-trip verification, run live against the analytical cluster per the plan's own acceptance criteria)
- **Issue:** `alembic downgrade -1` raised `TypeError: 'type' can be one of 'check', 'foreignkey', 'primary', 'unique', None` when reaching the EXCLUDE-constraint drop step.
- **Fix:** Replaced with raw `op.execute("ALTER TABLE normalized.customers DROP CONSTRAINT excl_customers_business_key_validity")`.
- **Files modified:** migrations/versions/0035_normalized_customers_scd2.py
- **Verification:** Re-ran the full downgrade -1 / upgrade head round-trip live against the analytical cluster; both directions succeeded.
- **Committed in:** 1faa9a3 (Task 1 commit)

**2. [Rule 1 - Bug] `_customers_customer_id_constraint_types`'s `pg_constraint` rewrite initially also matched PostgreSQL 18's catalogued NOT NULL constraint**
- **Found during:** Task 3 (running the plan's own verify command, `pytest tests/integration/test_migrations.py -k "exclusion or customer_id or gold_indexes"`)
- **Issue:** `test_0006_customer_id_has_a_real_unique_constraint` failed with `('n', 'u') != ('u',)` -- PostgreSQL 18 now catalogues `NOT NULL` as a real `pg_constraint` row (`contype = 'n'`), which the naive `pg_constraint` query (needed to see the EXCLUDE constraint at all, since `information_schema` has no EXCLUDE representation) also matched.
- **Fix:** Added `AND con.contype != 'n'` to the helper's query.
- **Files modified:** tests/integration/test_migrations.py
- **Verification:** Re-ran the full `test_migrations.py -m integration` suite; 20/20 passed.
- **Committed in:** c1a01cc (Task 3 commit)

---

**Total deviations:** 2 auto-fixed (both Rule 1 - bugs surfaced by live verification, both explicitly anticipated as open verification questions in 10-RESEARCH.md).
**Impact on plan:** Both fixes were required for the plan's own stated acceptance criteria (clean downgrade/upgrade round-trip; full test_migrations.py suite passing). No scope creep.

## Issues Encountered

- `kubectl port-forward` to the analytical PostgreSQL service died mid-command twice during live verification (`connection reset by peer` from the CNI network namespace) -- a transient WSL2/kind networking hiccup, not a migration bug. Each time, the DB was confirmed left in a clean, fully-committed state (Alembic's transactional DDL rolled back the interrupted step), and the operation was simply retried with a fresh port-forward.
- The plan's own Task 2 `<verify>` command (`load_config('configs/datasets/customers.yaml')`, one positional arg) does not match `load_config`'s real signature (`path`, keyword-only `defaults_path`) -- verified instead with the correct call shape; all stated acceptance criteria passed.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `normalized.customers` is SCD2-capable and live-proven; plans 10-02 through 10-06 (all `depends_on: ["10-01"]` where applicable) can build the recompute/DELETE-detection/SCD-Publisher machinery on top of this shape.
- `ColumnContract.scd_type`/`ScdConfig` give later plans the Type-0/1/2 dispatch and DELETE-semantics config surface they need without touching `DatasetConfig` again.
- `Publisher.publish()`'s `staged_run_ids` parameter is live everywhere the protocol is implemented or called, ready for the SCD Publisher (plan 10-04) to consume for its run-scoped DELETE-detection sweep.
- Known, deliberately-deferred gap: `test_publish_merge.py`/`test_publish_orders.py`/`test_referential_integrity.py`/`test_run_ingest.py` still call `Publisher.publish()` without `staged_run_ids` and will raise `TypeError` if run as-is -- plans 10-04/10-05 (both `depends_on: ["10-01"]`) are the planned fix point per 10-RESEARCH.md's Finding F-4.

---
*Phase: 10-slowly-changing-dimensions*
*Completed: 2026-08-21*
