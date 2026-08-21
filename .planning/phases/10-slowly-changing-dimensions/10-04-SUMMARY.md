---
phase: 10-slowly-changing-dimensions
plan: 04
subsystem: database
tags: [scd, scd2, publisher, postgresql, dbt, exclusion-constraint]

# Dependency graph
requires:
  - phase: 10-slowly-changing-dimensions (plan 10-01)
    provides: normalized.customers SCD2 shape (EXCLUDE constraint, is_current/valid_to/signup_country columns), ScdConfig, Publisher.publish(staged_run_ids=...)
  - phase: 10-slowly-changing-dimensions (plan 10-02)
    provides: dataplat.scd.recompute.recompute_version_chain (pure function, Type-0/1/2 dispatch), tracked_attribute_hash
  - phase: 10-slowly-changing-dimensions (plan 10-03)
    provides: dataplat.scd.delete_detection.find_vanished_customer_ids/MassDeleteCircuitBreaker/apply_delete_semantics
provides:
  - "dataplat.load.publish.scd.SCDPublisher -- the real 'scd' Publisher: DELETE-detection -> per-key full-bronze-history recompute -> atomic per-key DELETE+INSERT replace"
  - "PUBLISHER_REGISTRY['scd'] = SCDPublisher(); customers.yaml's load.strategy is now 'scd', not 'merge'"
  - "migration 0037: staging.customers (durable bronze) gains a nullable signup_country column -- a Rule 2 gap-fix plan 10-01 left open"
  - "tests/integration/test_publish_scd.py -- 7 live-proven SCDPublisher behaviors (basic publish, Finding-F1 bronze-vs-silver proof, SCD-07 late-correction ordering, SCD-09 idempotent replay, SCD-06 effective dating, SCD-11 backfill-safety, SCD-01/02 Type-0/1/2 dispatch matching recompute_version_chain's own oracle)"
affects: [10-06, 10-07, 10-08, 10-09]

tech-stack:
  added: []
  patterns:
    - "SCDPublisher independently re-derives, per emitted VersionRow, its lineage-source bronze row by reproducing recompute_version_chain's own tracked_attribute_hash grouping rule externally (_select_lineage_rows) -- VersionRow itself deliberately carries no lineage columns (plan 10-02's settled interface, unchanged)"
    - "Test-only ScdConfig(delete_semantics='ignore', mass_delete_threshold=1.0) as the safe default for any test file writing to the session-shared normalized.customers table through a REAL SCDPublisher.publish() call -- Step A's DELETE-detection is unscoped by dataset (single-dataset system), so a non-ignore semantics in a shared-table test suite would act on other test files' own rows"

key-files:
  created:
    - packages/dataplat/src/dataplat/load/publish/scd.py
    - migrations/versions/0037_staging_customers_signup_country.py
    - tests/integration/test_publish_scd.py
  modified:
    - packages/dataplat/src/dataplat/load/publish/registry.py
    - configs/datasets/customers.yaml
    - tests/integration/test_publish_ingest.py
    - tests/unit/test_publisher_registry.py
  deleted:
    - tests/integration/test_publish_merge.py

key-decisions:
  - "Migration 0037 adds signup_country to staging.customers but deliberately does NOT touch dataplat.pipeline.run._TARGET_COLUMNS_BY_DATASET or the CSV corpus-to-bronze wiring -- that is a separate, larger, already-existing gap (stage_ingest() already raises ValueError for the customers dataset on main today, independent of this plan) that ripples into several pre-existing stage_ingest()/publish_ingest() test fixtures outside this plan's declared scope. Documented as a deferred item below, not silently expanded into."
  - "test_publish_ingest.py's own _make_config switched from strategy='merge' to strategy='scd' (Rule 1 fix) -- MergePublisher is unconditionally, permanently broken against normalized.customers since migration 0035 (PostgreSQL rejects ON CONFLICT DO UPDATE against an exclusion-constraint arbiter outright), live-confirmed by running this exact test pre-fix. This is a materially larger fix than the plan's own 'cardinality-aware _normalized_customers_count' framing anticipated, but was required for the plan's own stated acceptance criterion (this file passes in full)."
  - "Investigated (per orchestrator instruction) whether 10-05's 5 xfail(strict=True) tests should now be un-xfailed now that SCDPublisher is registered and customers.yaml's live strategy: NO. Both test_reconciliation.py and test_run_ingest.py's own local _make_config() fixtures hardcode strategy='merge' independent of customers.yaml -- registering SCDPublisher does not change their behavior at all. MergePublisher's incompatibility with the exclusion-constraint shape is permanent, not merely 'until 10-04 lands' as the xfail reason text implied. Left un-xfailed and out of scope, since fixing them requires the same scd-strategy + staging.customers-seeding rewrite this plan applied to test_publish_ingest.py, in two much larger files this plan does not declare."

patterns-established:
  - "A Publisher reading from literal, hardcoded table names (never the source_table argument) documents this explicitly in both its class and method docstrings, matching MergePublisher's own 'source_table is the ONLY dynamic identifier' precedent inverted -- SCDPublisher's precedent is 'source_table is accepted but never read'."

requirements-completed: [SCD-03, SCD-06, SCD-07, SCD-09, SCD-11]

duration: ~100min
completed: 2026-08-21
---

# Phase 10 Plan 04: SCD Publisher Assembly & Registration Summary

**Real `SCDPublisher` assembling DELETE-detection + full-bronze-history recompute + atomic per-key replace, registered as `customers.yaml`'s live `"scd"` publication strategy, proven by 7 live-PostgreSQL behaviors including Finding F-1's bronze-vs-silver proof and SCD-07's late-correction chronological ordering.**

## Performance

- **Duration:** ~100 min
- **Completed:** 2026-08-21
- **Tasks:** 2/2
- **Files modified:** 8 (3 created, 4 modified, 1 deleted)

## Accomplishments

- `SCDPublisher.publish()` (packages/dataplat/src/dataplat/load/publish/scd.py) assembles plans 10-02/10-03's already-proven building blocks into the real vertical slice: Step A (DELETE-detection + circuit breaker + delete-semantics dispatch), Step B (touched-key discovery scoped to `staged_run_ids`), Step C (per-key FULL bronze history recompute, deliberately unscoped to `staged_run_ids` -- Finding F-1), Step D (atomic per-key `DELETE` + bulk `INSERT` replace, with lineage columns independently re-derived per version via `_select_lineage_rows`)
- `tests/integration/test_publish_scd.py` proves all 7 of this plan's own `must_haves.truths` against real testcontainers PostgreSQL, all passing on the first live run: basic publish, Finding F-1 (recompute reads `staging.customers`, never the collapsed `silver.customers`), SCD-07 late-arriving correction landing chronologically by `event_ts` (not arrival order, despite arriving with the HIGHEST `_source_row_number`), SCD-09 idempotent replay, SCD-06 effective dating (every `valid_from`/`valid_to` traces to a real bronze `event_ts`, proven to never fall inside the call's own wall-clock window), SCD-11 backfill-safety (an untouched key's rows are byte-identical before/after an unrelated key's publish), and SCD-01/02 Type-0/1/2 dispatch independently cross-checked against `recompute_version_chain`'s own oracle
- `PUBLISHER_REGISTRY["scd"]` resolves to a real `SCDPublisher` instance; `configs/datasets/customers.yaml`'s `load.strategy` is now `scd`, live-verified via `load_config()` against the real file
- `tests/integration/test_publish_merge.py` deleted (RESEARCH.md Finding F-4's explicit "should be deleted" guidance) -- `test_publish_scd.py` is its replacement
- Found and fixed (Rule 2) a genuine blocking gap left by plan 10-01: `staging.customers` (durable bronze) never gained a `signup_country` column even though `customers.yaml`/`normalized.customers` both did -- SCDPublisher's own Type-0 dispatch could not have read it at all without migration 0037
- Found and fixed (Rule 1) that `test_publish_ingest.py`'s own `_make_config` hardcoded `strategy="merge"`, which is unconditionally, permanently broken against `normalized.customers` since migration 0035 -- live-confirmed via `InvalidColumnReference` before the fix; switched to `strategy="scd"` with a session-shared-table-safe `ScdConfig(delete_semantics="ignore", mass_delete_threshold=1.0)`, added bronze-row seeding, and made `_normalized_customers_count` cardinality-aware (`COUNT(DISTINCT customer_id)`)

## Task Commits

1. **Task 1: SCDPublisher -- assemble DELETE-detection, per-key recompute, and atomic replace** - `b778433` (feat)
2. **Task 2: Registry wiring, customers.yaml strategy flip, retire the obsolete MergePublisher test** - `7866721` (feat)

_Note: no separate plan-metadata commit in worktree mode -- the orchestrator commits SUMMARY.md centrally after merge._

## Files Created/Modified

- `packages/dataplat/src/dataplat/load/publish/scd.py` - `SCDPublisher(Publisher)`, `name = "scd"`; `_select_lineage_rows` helper reproducing `recompute_version_chain`'s own grouping rule externally to source lineage columns
- `migrations/versions/0037_staging_customers_signup_country.py` - nullable `signup_country` TEXT column on `staging.customers` (Rule 2 gap fix)
- `tests/integration/test_publish_scd.py` - 7 integration tests proving this plan's `must_haves.truths`, all passing live
- `packages/dataplat/src/dataplat/load/publish/registry.py` - `"scd": SCDPublisher()` added to `PUBLISHER_REGISTRY`; docstring updated to "three entries"
- `configs/datasets/customers.yaml` - `load.strategy` `merge` -> `scd` (the only line changed)
- `tests/integration/test_publish_ingest.py` - `_make_config` switched to `strategy="scd"` + `ScdConfig(delete_semantics="ignore", mass_delete_threshold=1.0)`; new `_insert_bronze_row` helper; `_normalized_customers_count` made cardinality-aware
- `tests/unit/test_publisher_registry.py` - registry-shape assertions updated for the new 3-entry registry; new `resolve_publisher("scd")` singleton test
- `tests/integration/test_publish_merge.py` - deleted (obsolete, per plan)

## Decisions Made

See `key-decisions` in frontmatter. The two most consequential:

1. **Migration 0037's scope was kept deliberately narrow.** It adds `signup_country` to `staging.customers` (required for THIS plan's own SCDPublisher tests to even run) but does NOT touch `dataplat.pipeline.run._TARGET_COLUMNS_BY_DATASET` or the CSV-to-bronze pipeline wiring -- live-confirmed that gap already exists independently on `main` (the real `stage_ingest()` call already raises `ValueError` for the `customers` dataset today, since plan 10-01 added `signup_country` to `customers.yaml`'s `columns:` block but never to this hardcoded target-columns table). Closing that gap would ripple into several pre-existing test fixtures across files well outside this plan's declared scope (`test_run_ingest.py`, property tests, etc.) -- documented as a deferred item, not silently expanded into.
2. **test_publish_ingest.py needed a materially larger fix than the plan's own action text anticipated.** The plan described a narrow "cardinality-aware `_normalized_customers_count`" fix, but live-running this file's own test against unmodified `main` reproduced the SAME `InvalidColumnReference` MergePublisher regression 10-05 found -- this file was never actually covered by 10-05's own xfail sweep. Fixed by switching its `_make_config` to `strategy="scd"` (matching customers.yaml's real production shape) and seeding `staging.customers` to match, since `SCDPublisher` reads bronze directly rather than the caller-supplied `source_table`.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing critical] `staging.customers` never gained the `signup_country` column plan 10-01 added everywhere else**
- **Found during:** Task 1, reading `staging.customers`'s DDL (migration 0022) before writing `SCDPublisher`'s Step C SQL
- **Issue:** Plan 10-01's D-13 added `signup_country` to `customers.yaml`'s `columns:` block and to `normalized.customers`'s DDL (migration 0035), but never to the durable bronze table `staging.customers` -- `SCDPublisher`'s own Step C (`SELECT ... signup_country ... FROM staging.customers`) would raise `UndefinedColumn` for every call, blocking this plan's own Test 7 (Type-0/1/2 dispatch, SCD-01) entirely.
- **Fix:** Migration 0037 -- nullable `signup_country TEXT` column on `staging.customers`, matching migration 0035's own nullable precedent for older, pre-existing rows.
- **Files modified:** `migrations/versions/0037_staging_customers_signup_country.py`
- **Verification:** `alembic upgrade head` applies cleanly against a fresh testcontainers PostgreSQL (exercised automatically by every `migrated_dsn`-fixtured test in this session); `test_publish_scd.py`'s Test 7 passes.
- **Committed in:** `b778433` (Task 1 commit)

**2. [Rule 1 - Bug] `test_publish_ingest.py`'s own config hardcoded `strategy="merge"`, unconditionally broken against `normalized.customers` since migration 0035**
- **Found during:** Task 2, running this plan's own required verify command (`pytest tests/integration/test_publish_ingest.py -q -m integration`) against unmodified `main` BEFORE making any Task 2 changes, to establish a baseline
- **Issue:** `psycopg.errors.InvalidColumnReference: there is no unique or exclusion constraint matching the ON CONFLICT specification` -- the identical regression 10-05 found and xfail-quarantined in `test_reconciliation.py`/`test_run_ingest.py`, but `test_publish_ingest.py` was never covered by that sweep and was genuinely, silently broken on `main` already.
- **Fix:** `_make_config`'s `load.strategy` switched to `"scd"`, `scd=ScdConfig(delete_semantics="ignore", mass_delete_threshold=1.0)` added (see key-decisions for why `"ignore"`, not the real `"invalidate"`), new `_insert_bronze_row` helper seeding `staging.customers` (which `SCDPublisher` reads directly), `_normalized_customers_count` made cardinality-aware.
- **Files modified:** `tests/integration/test_publish_ingest.py`
- **Verification:** `pytest tests/integration/test_publish_ingest.py -q -m integration` -- 2 passed (was 1 failed/1 passed pre-fix).
- **Committed in:** `7866721` (Task 2 commit)

**3. [Rule 1 - Bug] `tests/unit/test_publisher_registry.py`'s registry-shape assertions hardcoded the old two-entry shape**
- **Found during:** Task 2, running `pytest tests/unit -q` after adding `"scd"` to `PUBLISHER_REGISTRY`
- **Issue:** `test_publisher_registry_resolves_merge_to_a_merge_publisher_instance` asserted `set(PUBLISHER_REGISTRY) == {"merge", "merge_orders"}`; `test_resolve_publisher_raises_configuration_error_for_an_unknown_strategy` asserted the sorted `known` list without `"scd"` -- both now stale, a direct, necessary consequence of Task 2's own registry change.
- **Fix:** Updated both assertions for the 3-entry registry; added a `resolve_publisher("scd")` singleton test mirroring the existing `merge_orders` one.
- **Files modified:** `tests/unit/test_publisher_registry.py`
- **Verification:** `pytest tests/unit -q` -- 548 passed.
- **Committed in:** `7866721` (Task 2 commit)

---

**Total deviations:** 3 auto-fixed (1 Rule 2 - missing critical functionality, 2 Rule 1 - bugs). All three were required for this plan's own stated acceptance criteria. No scope creep into `MergePublisher` itself, `test_reconciliation.py`, `test_run_ingest.py`, or the `_TARGET_COLUMNS_BY_DATASET` CSV-pipeline gap (all explicitly investigated and deliberately left out of scope -- see Issues Encountered).

## Issues Encountered

- **Baseline-compared the full `pytest tests/integration -q -m "not cluster and not manifests and not dbt"` run** (via a temporary, read-only `git worktree add --detach` at this plan's own base commit, removed immediately after) to distinguish this plan's own impact from pre-existing breakage: **25 failures at baseline** (before any of this plan's changes) vs **16 failures after** both tasks landed -- a net improvement, not a regression, even though 16 failures remain. Every one of the 16 remaining failures is confirmed, by name, present in the baseline run too (same test, same or a related root cause): `test_config_registry.py`, `test_lineage_view.py`, `test_publish_orders.py`, `test_publish_transaction_wiring.py`, `test_referential_integrity.py`, `test_staging_normalization.py`, `test_watermarks.py` all fail due to the SAME pre-existing `staged_run_ids`-signature gap plan 10-01's own SUMMARY explicitly named as deferred to "plans 10-04/10-05" but which those two plans' own declared `<files>` never actually covered for these specific files (only `test_publish_merge.py` [deleted], `test_publish_orders.py` [still broken], `test_referential_integrity.py` [still broken], `test_run_ingest.py` [10-05's own scope] were named). None of these seven files are in THIS plan's declared `<files>` scope either.
- One test (`test_publish_ingest.py::test_two_staged_runs_finalize_together_and_a_second_call_is_idempotent`) passes cleanly in isolation (this plan's own required verify command) but still fails in the FULL-suite context -- root-caused to a DIFFERENT, unrelated issue than the one this plan fixes: `publish_ingest`'s own `list_staged_run_ids(dataset_id=...)` sweeps up EVERY currently-`STAGED` run for the literal dataset name `"customers"`, including ones left dangling by OTHER, unrelated test files (`test_watermarks.py`, `test_publish_transaction_wiring.py`) whose own `staged_run_ids`-signature `TypeError`s (the same pre-existing gap above) leave their own seeded `"customers"`-dataset runs permanently stuck in `STAGED`. This is a genuine, real cross-test-isolation gap in the test suite's own shared-dataset-name convention, unrelated to `SCDPublisher`/customers.yaml's strategy, and well outside this plan's scope to fix.
- **Deferred, out of this plan's scope:** `dataplat.pipeline.run._TARGET_COLUMNS_BY_DATASET["customers"]` still omits `signup_country` -- the real `stage_ingest()` CSV-ingestion path (not this plan's own direct-SQL-seeded tests) will raise `ValueError` for any real customers CSV file carrying a `signup_country` column (which `tools/corpus/dated_series.py`, plan 10-06, now generates). This pre-existing gap (introduced by plan 10-01, never closed by any subsequent plan) needs its own fix -- likely surfaces for real during plan 10-07's live 2-year backfill sweep.

## User Setup Required

None - no external service configuration required. Tests run entirely against a throwaway testcontainers PostgreSQL 18 container.

## Next Phase Readiness

- `SCDPublisher` is registered, real, and live-proven against real PostgreSQL; `customers.yaml`'s `load.strategy: scd` resolves through the registry correctly (`resolve_publisher("scd").name == "scd"`, live-verified).
- A real `publish_ingest()` call against `customers.yaml` now produces genuine SCD2 output for the first time in this phase.
- **Important for plan 10-06 onward:** `_TARGET_COLUMNS_BY_DATASET["customers"]` (packages/dataplat/src/dataplat/pipeline/run.py) still omits `signup_country` -- a real `stage_ingest()` call against a `signup_country`-carrying CSV file (which plan 10-06's corpus generator now produces) will raise `ValueError`. This is NOT this plan's own regression (confirmed pre-existing on `main` before this plan started) but will block plan 10-07's live 2-year backfill sweep until fixed.
- **Important for a future gap-closure round:** 10-05's 5 `xfail(strict=True)`-marked tests in `test_reconciliation.py`/`test_run_ingest.py` should NOT be un-xfailed yet -- both files' own local fixtures hardcode `strategy="merge"`, independent of `customers.yaml`, so this plan's changes do not affect them. Un-xfailing requires the same `scd`-strategy + `staging.customers`-seeding rewrite this plan applied to `test_publish_ingest.py`, in two larger files outside this plan's declared scope.
- **Also deferred:** `test_publish_orders.py`/`test_referential_integrity.py`'s own `OrdersMergePublisher.publish()` call sites are still missing the `staged_run_ids` keyword argument (a pre-existing gap from plan 10-01, confirmed present at baseline and unrelated to `customers`/SCD work) -- these fail with `TypeError`, not an SCD-related error, and are out of this plan's scope.

## Self-Check: PASSED

- FOUND: `packages/dataplat/src/dataplat/load/publish/scd.py`
- FOUND: `migrations/versions/0037_staging_customers_signup_country.py`
- FOUND: `tests/integration/test_publish_scd.py`
- FOUND: `packages/dataplat/src/dataplat/load/publish/registry.py` (modified, contains `"scd"`)
- FOUND: `configs/datasets/customers.yaml` (modified, `load.strategy: scd`)
- MISSING: `tests/integration/test_publish_merge.py` (intentionally deleted, per plan)
- FOUND commit: `b778433` (Task 1)
- FOUND commit: `7866721` (Task 2)

---
*Phase: 10-slowly-changing-dimensions*
*Completed: 2026-08-21*
