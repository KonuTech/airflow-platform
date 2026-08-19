---
phase: 09-etl-correctness-dedup-incremental-backfill-recovery
plan: 01
subsystem: database
tags: [pydantic, config, dataplat, reconciliation, deduplication]

# Dependency graph
requires:
  - phase: 08.1-dbt-silver-transformation-layer
    provides: "D-07/D-28 decision that deduplication is dbt-owned, making DatasetConfig.deduplication a vestigial required field"
provides:
  - "ReconciliationConfig Pydantic model (sum_columns: list[str]) for D-25's dataset-conditional reconciliation sum-check"
  - "DatasetConfig.deduplication is now Optional (defaults to None); DatasetConfig.reconciliation is new and Optional"
  - "customers.yaml and orders.yaml free of the vestigial deduplication: block; orders.yaml declares reconciliation.sum_columns: [amount]"
affects: [09-02, 09-07]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Opt-in DatasetConfig block convention (FreshnessConfig/QualityConfig/ReconciliationConfig): field is `X | None = None`, absence means 'not applicable to this dataset', consumers branch on None rather than an empty/sentinel value"

key-files:
  created: []
  modified:
    - packages/dataplat/src/dataplat/config/model.py
    - configs/datasets/customers.yaml
    - configs/datasets/orders.yaml

key-decisions:
  - "Adopted the MINIMAL reading of D-28 (per plan's own explicit framing): made DatasetConfig.deduplication Optional and deleted the YAML block, left DeduplicationConfig itself and its 13 dependent test files untouched — a full purge of DeduplicationConfig is out of scope and would be a separate follow-up."

patterns-established:
  - "ReconciliationConfig follows FreshnessConfig/QualityConfig's exact opt-in shape (ConfigDict(extra='forbid', frozen=True), Optional field on DatasetConfig defaulting to None) — future dataset-conditional config surfaces should copy this same shape rather than inventing a new one."

requirements-completed: [VALID-05]

# Metrics
duration: ~15min
completed: 2026-08-19
---

# Phase 09 Plan 01: Config surface for reconciliation, deduplication made optional Summary

**Added `ReconciliationConfig` (sum_columns) to `DatasetConfig` for D-25's per-dataset reconciliation sum-check, and made the never-consumed `deduplication:` block optional and removed it from both committed dataset YAMLs (D-28).**

## Performance

- **Duration:** ~15 min
- **Started:** 2026-08-19T16:12:30+02:00 (base commit)
- **Completed:** 2026-08-19T16:29:26+02:00
- **Tasks:** 2 completed
- **Files modified:** 3

## Accomplishments
- `DatasetConfig.deduplication` is now `DeduplicationConfig | None = None` (was required), matching the `filename`/`normalization`/`freshness`/`quality` Optional convention already on the class; `_check_deduplication_keys_are_business_key_columns` short-circuits when `deduplication is None`.
- New `ReconciliationConfig` model (`sum_columns: list[str]`, `extra="forbid"`, `frozen=True`) wired onto `DatasetConfig.reconciliation: ReconciliationConfig | None = None`, giving VALID-05's dataset-conditional sum check its declared input.
- `customers.yaml` and `orders.yaml` both had their vestigial `deduplication:` block deleted; `orders.yaml` gained a `reconciliation: sum_columns: [amount]` block (customers.yaml declares none, per D-25 — it has no natural numeric column).
- Live-verified via `dataplat.config.loader.load_config`: `customers` → `deduplication=None, reconciliation=None`; `orders` → `deduplication=None, reconciliation=sum_columns=['amount']`.

## Task Commits

Each task was committed atomically:

1. **Task 1: Make deduplication Optional, add ReconciliationConfig** - `e8d094d` (feat)
2. **Task 2: Remove vestigial deduplication block, add orders reconciliation block, regress** - `37c97c4` (docs)

_Note: Task 2 is classified `docs` because its only content changes are the two YAML config files; no Python behavior changed in that commit._

## Files Created/Modified
- `packages/dataplat/src/dataplat/config/model.py` - Added `ReconciliationConfig`; made `DatasetConfig.deduplication` Optional; added `DatasetConfig.reconciliation`; guarded `_check_deduplication_keys_are_business_key_columns` against `None`
- `configs/datasets/customers.yaml` - Removed vestigial `deduplication:` block (no `reconciliation:` block added — no natural numeric column)
- `configs/datasets/orders.yaml` - Removed vestigial `deduplication:` block; added `reconciliation: sum_columns: [amount]` block after `batching:`

## Decisions Made
- Adopted the plan's pre-declared MINIMAL reading of D-28: `DeduplicationConfig` itself and its 13 dependent test files (RESEARCH.md Pitfall 4) were left untouched; only `DatasetConfig.deduplication`'s optionality and the two YAML documents changed. This was the plan's own stated scope, not a deviation.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

`uv run` in this worktree defaulted to a fresh, empty per-worktree `.venv` (mismatched against the active `VIRTUAL_ENV`), which lacked `testcontainers` and made the integration-test conftest fail to import. Resolved by using `uv run --active` (as `uv`'s own warning suggested) to target the already-populated environment — no code or config change, purely a local test-invocation adjustment. Full 104-test regression suite (the 16 files listed in Task 2's acceptance criteria, `tests/unit/test_csv_processor_cli.py` through `tests/integration/test_publish_ingest.py`) then passed cleanly, as did `uv run --active mypy packages/dataplat/src/dataplat/` (62 source files, 0 errors) and `uv run --active ruff check packages/dataplat/src/dataplat/config/model.py`.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

`ctx.config.reconciliation.sum_columns` is now a valid, populated attribute path for `orders` (and a safe `None` for `customers`) — plans 09-02 and 09-07, which the plan's objective names as consumers of this field, can proceed. `DatasetConfig.deduplication` remains a valid but now-optional attribute for any code path that still reads it (none of the two committed dataset YAMLs populate it any longer; the 13 test files that construct `DeduplicationConfig` directly are unaffected).

---
*Phase: 09-etl-correctness-dedup-incremental-backfill-recovery*
*Completed: 2026-08-19*
