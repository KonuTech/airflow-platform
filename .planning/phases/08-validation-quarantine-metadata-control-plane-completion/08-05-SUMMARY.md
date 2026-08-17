---
phase: 08-validation-quarantine-metadata-control-plane-completion
plan: 05
subsystem: database
tags: [postgresql, psycopg, pydantic, alembic, orders, publisher, config-driven]

# Dependency graph
requires:
  - phase: 08-01
    provides: normalized.orders DDL (migration 0016), QualityConfig/quality: block on DatasetConfig, widened MetadataRepository Protocol, PublicationError/QualityThresholdExceeded exceptions
provides:
  - "orders as a second, fully-working dataset through the existing config-driven Source->Stage->Publisher vertical slice"
  - "OrdersMergePublisher (the second concrete Publisher, mirroring MergePublisher's advisory-lock + ON CONFLICT pattern)"
  - "dataset-keyed target-columns lookup in run_ingest (_TARGET_COLUMNS_BY_DATASET), replacing the single hardcoded customers-only constant"
  - "migration 0017: the UNIQUE constraint on normalized.orders.order_id that ON CONFLICT requires (closes a gap migration 0016 left open)"
  - "configs/datasets/orders.yaml: D-13..D-17's minimal orders dataset config"
affects: [08-08 (ReferentialIntegrityBarrier, quality: block for orders.yaml), 08-12 (csv_ingest_orders live DAG)]

# Tech tracking
tech-stack:
  added: []
  patterns: ["dataset-keyed registry/lookup dict instead of a single hardcoded per-dataset constant", "second Publisher registered under its own load.strategy key (merge_orders), never sharing the first Publisher's hardcoded target"]

key-files:
  created:
    - packages/dataplat/src/dataplat/load/publish/merge_orders.py
    - configs/datasets/orders.yaml
    - tests/integration/test_publish_orders.py
    - migrations/versions/0017_normalized_orders_business_key_unique.py
  modified:
    - packages/dataplat/src/dataplat/pipeline/run.py
    - packages/dataplat/src/dataplat/load/publish/registry.py
    - tests/unit/test_publisher_registry.py

key-decisions:
  - "Added migration 0017 (UNIQUE constraint on normalized.orders.order_id) as a Rule-3 blocking-issue fix: migration 0016 (08-01) left order_id with no index/constraint at all, but OrdersMergePublisher's INSERT ... ON CONFLICT (order_id) cannot function without a real UNIQUE/exclusion constraint as its conflict target -- discovered via a genuine psycopg.errors.InvalidColumnReference while proving Task 2's own tests, not a hypothetical."
  - "orders.yaml deliberately has no quality: block yet, per the plan's own text -- ReferentialIntegrityBarrier and D-16's orphan handling land in plan 08-08, not here."
  - "No normalization: block on orders.yaml -- StagingLoader falls back to NumericNormalizer's own defaults (decimal_separator=\".\") whenever DatasetConfig.normalization is None, which already suits a plain \"123.45\"-shaped amount value; confirmed by reading load/staging.py directly rather than assuming."
  - "Extracted _target_columns_for_dataset() as a small helper in pipeline/run.py rather than inlining the try/except at the staging call site -- keeps run_ingest under ruff's PLR0915 statement-count threshold with zero behavior change."

patterns-established:
  - "A second real dataset's Publisher gets its OWN registry key (merge_orders, not a parameterized merge) -- each Publisher stays single-dataset/hardcoded against its own target table's real schema, matching MergePublisher's own established precedent, not a premature generic upsert-any-table design."

requirements-completed: [VALID-07]

# Metrics
duration: ~30min
completed: 2026-08-17
---

# Phase 8 Plan 05: Orders Second Dataset Vertical Slice Summary

**`orders` now publishes real rows through the identical staging->publish pipeline `customers` uses -- a new `OrdersMergePublisher` (advisory-lock + `INSERT ... ON CONFLICT`), a dataset-keyed `run_ingest` target-columns lookup, and `configs/datasets/orders.yaml`, proven idempotent against real PostgreSQL.**

## Performance

- **Duration:** ~30 min
- **Completed:** 2026-08-17
- **Tasks:** 2 completed
- **Files modified:** 8 (5 modified/created for Task 1, 2 created for Task 2, 1 new deferred-items.md doc)

## Accomplishments
- `run_ingest`'s single hardcoded `_CUSTOMERS_TARGET_COLUMNS` constant is now a dataset-keyed `_TARGET_COLUMNS_BY_DATASET` lookup; an unrecognized dataset raises a named `DataPlatformError` (via a new `_target_columns_for_dataset()` helper), never a bare `KeyError` or a silent fallback onto another dataset's columns
- `OrdersMergePublisher` (`load/publish/merge_orders.py`) is the second concrete `Publisher`, mirroring `MergePublisher`'s exact structure: `DISTINCT ON (order_id)` dedup-within-batch guard, `WHERE ... IS DISTINCT FROM` no-op-republish guard, `EXCLUDED.order_date >= normalized.orders.order_date` no-clobber guard
- `PUBLISHER_REGISTRY` gains a `"merge_orders"` entry resolving to a stateless `OrdersMergePublisher` singleton, proven via `resolve_publisher("merge_orders") is` the same instance every call
- `configs/datasets/orders.yaml` validates with zero errors against `DatasetConfig` (via `dataplat.config.loader.load_config`) -- D-13..D-17's minimal `order_id`/`customer_id`/`order_date`/`amount` schema, `load.strategy: merge_orders`
- `tests/integration/test_publish_orders.py` proves, against real testcontainers PostgreSQL, both the dedup-within-batch guard (2 rows sharing one `order_id` -> 1 row, later `order_date` wins) and the idempotent-republish guard (`rows_affected == 0` on an identical second publish) -- customers' own D-1 idempotency guarantee extended to a second real dataset

## Task Commits

Each task was committed atomically:

1. **Task 1: Dataset-aware target columns in run_ingest + OrdersMergePublisher + registry entry** - `ba251ee` (feat)
2. **Task 2: configs/datasets/orders.yaml + end-to-end publish integration proof** - `ef42f01` (feat)

## Files Created/Modified
- `packages/dataplat/src/dataplat/pipeline/run.py` - `_TARGET_COLUMNS_BY_DATASET` dataset-keyed lookup + `_target_columns_for_dataset()` helper, replacing the single hardcoded customers-only constant
- `packages/dataplat/src/dataplat/load/publish/merge_orders.py` - `OrdersMergePublisher`, the second concrete `Publisher`, targeting `normalized.orders`
- `packages/dataplat/src/dataplat/load/publish/registry.py` - `PUBLISHER_REGISTRY["merge_orders"]` entry
- `migrations/versions/0017_normalized_orders_business_key_unique.py` - `uq_orders_order_id` UNIQUE constraint (Rule-3 blocking fix, see Deviations)
- `configs/datasets/orders.yaml` - D-13..D-17's minimal orders dataset config
- `tests/integration/test_publish_orders.py` - dedup + idempotent-republish proof against real PostgreSQL
- `tests/unit/test_publisher_registry.py` - extended for the new `merge_orders` registry entry
- `.planning/phases/08-validation-quarantine-metadata-control-plane-completion/deferred-items.md` - logs two pre-existing, out-of-scope gaps found while verifying (see Deviations)

## Decisions Made
- `orders.yaml` deliberately omits `quality:`/`normalization:`/`filename:` blocks, per the plan's own text and by confirming `StagingLoader`'s numeric-normalization defaults already suit a plain decimal `amount` value -- no new dataset-config capability was needed for this plan's scope.
- Extracted the `_TARGET_COLUMNS_BY_DATASET` lookup into a small `_target_columns_for_dataset()` helper (rather than inlining a `try`/`except` at the staging call site) purely to keep `run_ingest` under ruff's `PLR0915` statement-count threshold -- no behavior change.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Added migration 0017: UNIQUE constraint on `normalized.orders.order_id`**
- **Found during:** Task 2 (proving `test_publish_orders.py` against real PostgreSQL)
- **Issue:** Migration 0016 (landed by plan 08-01, merged into this worktree's base before this plan started) created `normalized.orders.order_id` as a plain, unconstrained column -- no index, no uniqueness. `OrdersMergePublisher`'s `INSERT ... ON CONFLICT (order_id)` cannot execute without a real `UNIQUE`/exclusion constraint as its conflict target; the first test run failed with `psycopg.errors.InvalidColumnReference: there is no unique or exclusion constraint matching the ON CONFLICT specification`.
- **Fix:** Added `migrations/versions/0017_normalized_orders_business_key_unique.py`, mirroring migration 0006's identical fix for `normalized.customers.customer_id` -- `op.create_unique_constraint("uq_orders_order_id", "orders", ["order_id"], schema="normalized")`.
- **Files modified:** `migrations/versions/0017_normalized_orders_business_key_unique.py` (new)
- **Verification:** `tests/integration/test_publish_orders.py -m integration` passes (2/2); `tests/integration/test_migrations.py` unaffected; `alembic upgrade head` applies cleanly through 0017.
- **Committed in:** `ba251ee` (Task 1 commit -- bundled there since `OrdersMergePublisher` cannot function correctly without this precondition, even though the failure surfaced while proving Task 2)

**2. [Rule 1 - Bug] Updated `tests/unit/test_publisher_registry.py` for the new registry entry**
- **Found during:** Task 1's own `<verify>` command (`pytest tests/unit/ -k "publish or registry"`)
- **Issue:** `test_publisher_registry_resolves_merge_to_a_merge_publisher_instance` asserted `set(PUBLISHER_REGISTRY) == {"merge"}`, which is now genuinely false once `"merge_orders"` is added -- a direct, expected consequence of Task 1's own behavior spec, not a design flaw.
- **Fix:** Updated the set-equality assertion to `{"merge", "merge_orders"}`; added a new test proving the `resolve_publisher("merge_orders") is` singleton acceptance criterion explicitly.
- **Files modified:** `tests/unit/test_publisher_registry.py`
- **Verification:** `pytest tests/unit/ -k "publish or registry" -x` passes (5/5).
- **Committed in:** `ba251ee` (Task 1 commit)

**3. [Rule 1 - Bug] Fixed 3 line-length lint violations (ruff E501/W505)**
- **Found during:** Post-implementation `make lint`/`ruff check .` sweep (part of the CLAUDE.md-enforcement pre-commit check)
- **Issue:** Three docstring lines exceeded the 100-char limit: `merge_orders.py`'s class docstring, `test_publish_orders.py`'s `_make_context()` docstring, `test_publisher_registry.py`'s new test's docstring.
- **Fix:** Shortened each to fit under 100 chars without losing meaning.
- **Files modified:** `packages/dataplat/src/dataplat/load/publish/merge_orders.py`, `tests/integration/test_publish_orders.py`, `tests/unit/test_publisher_registry.py`
- **Verification:** `ruff check .` -> "All checks passed!"
- **Committed in:** `ba251ee`/`ef42f01` (bundled with each file's own task commit)

**4. [Rule 1 - Bug] Extracted `_target_columns_for_dataset()` to fix ruff PLR0915**
- **Found during:** Post-implementation `ruff check .` sweep
- **Issue:** Inlining the dataset-lookup `try`/`except` directly at `run_ingest`'s staging call site pushed the function's statement count to 52, exceeding ruff's `PLR0915` threshold of 50 (confirmed absent before this plan's change via `git stash`/`ruff check` on the pre-change state).
- **Fix:** Extracted the lookup into a module-level `_target_columns_for_dataset()` helper function; the call site now reads `StagingLoader(target_columns=_target_columns_for_dataset(ctx.config.dataset))`. No behavior change.
- **Files modified:** `packages/dataplat/src/dataplat/pipeline/run.py`
- **Verification:** `ruff check .` -> "All checks passed!"; `pytest tests/unit/ -k "publish or registry"` still 5/5 green.
- **Committed in:** `ba251ee`

---

**Total deviations:** 4 auto-fixed (1 blocking/Rule 3, 3 bug/Rule 1)
**Impact on plan:** All four were necessary for correctness (the missing UNIQUE constraint would have made `OrdersMergePublisher` non-functional against any real database) or for keeping the codebase's existing lint/test gates green. No scope creep -- every fix stayed within this plan's own files or a directly-caused ripple effect (the registry test).

## Issues Encountered

**Worktree base was stale relative to `main`.** At session start, this worktree's branch (`worktree-agent-aedb118f82dcc35c4`) was still based on the pre-08-01/08-02 commit, despite the orchestrator's context stating "plan 08-01 ... has already merged to main and is available in your worktree base." `git merge-base HEAD main` showed my own `HEAD` as the merge-base (i.e., `main` was strictly ahead with zero divergent commits on my branch), so I fast-forward-merged `main` into my branch (`git merge main --ff-only`) before starting any work -- a safe, non-destructive operation given the clean, non-diverged state. This pulled in migrations 0014-0016, the widened `QualityConfig`/`DatasetConfig`, and the widened `errors.py`/`MetadataRepository` Protocol that this plan's Task 2 (`orders.yaml` validation, `normalized.orders` table) genuinely depends on.

**Pre-existing, out-of-scope gaps found while verifying (logged, not fixed):**
1. `PostgresMetadataRepository` cannot be instantiated (`mypy`/`make typecheck` fails) -- three Protocol methods added by 08-01 (`record_rejected_records`, `record_validation_results`, `resolve_rejected_records_for_batch`) have no concrete implementation yet. Confirmed pre-existing via `git stash`/mypy re-run against the pre-08-05 commit. Not in this plan's `files_modified` scope.
2. `airflow/dags/csv_ingest_customers.py` is 162 lines, over ORCH-06's 150-line budget -- introduced by plan 08-02's `integrity_gate` task addition. Not in this plan's `files_modified` scope.

Both logged to `.planning/phases/08-validation-quarantine-metadata-control-plane-completion/deferred-items.md` per the executor's scope-boundary rule (only fix issues directly caused by the current task's own changes).

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

`orders` is now a real, working second dataset through the full staging->publish pipeline. Plan 08-08 (`ReferentialIntegrityBarrier`, D-16's orphan-order `quality:` rule) and plan 08-12 (the live `csv_ingest_orders` DAG) can build directly on `OrdersMergePublisher`/`configs/datasets/orders.yaml` with no further pipeline-plumbing changes, per this plan's own success criterion.

Two pre-existing, out-of-scope gaps (see Issues Encountered) remain open for whichever later plan owns them -- neither blocks this plan's own deliverables, both are documented in `deferred-items.md`.

---
*Phase: 08-validation-quarantine-metadata-control-plane-completion*
*Completed: 2026-08-17*

## Self-Check: PASSED

All created files verified present on disk (`merge_orders.py`, `orders.yaml`,
`test_publish_orders.py`, migration `0017`, `deferred-items.md`, this
`SUMMARY.md`). Both task commits (`ba251ee`, `ef42f01`) verified present in
`git log`.
