# Deferred Items — Phase 04 (vertical-slice-csv-to-analytical-postgresql)

Out-of-scope issues discovered during execution, logged rather than fixed
in-place (SCOPE BOUNDARY: only fix issues directly caused by the current
task's own changes).

## From Plan 04-03

| Category | Item | Status | Deferred At |
|----------|------|--------|-------------|
| Design gap (inherited from 04-01-PLAN.md's interface) | `discover_files` calls `metadata.create_batch(...)` unconditionally on every non-duplicate object, every call — including on re-discovery of an already-`PENDING` or already-`SUCCEEDED` run. `create_batch` is not idempotent (plain `INSERT ... RETURNING`, no `ON CONFLICT`), so a batch row is created on every re-discovery, orphaning the previous batch (only the run's original `batch_id`, set once at first `INSERT`, stays linked to `meta.ingestion_runs`; later batches get a `meta.batch_files` row but no `ingestion_runs` reference). Does not affect this plan's own behavior guarantees (file identity, dedup, run re-offering/exclusion, and the fan-out cap are all unaffected — proven by `tests/unit/test_discovery.py`), but is a real, silently-accumulating metadata inefficiency worth fixing before batches carry more meaning (e.g. multi-file batches in a later phase). A fix would need either an idempotent `create_batch`/`get_or_create_batch` (keyed on `batch_key`, mirroring `create_file`/`get_or_create_ingestion_run`'s upsert pattern) or reordering `discover_files` to only create a batch on a run's first-ever allocation. | Deferred | 2026-08-13, plan 04-03 Task 2 |
