# Deferred Items — Phase 11

Out-of-scope discoveries found during plan execution, logged but not fixed (scope boundary
rule: only auto-fix issues directly caused by the current task's own changes).

| Category | Item | Status | Deferred At |
|----------|------|--------|-------------|
| Pre-existing bug | `tests/integration/test_reconciliation.py`'s four `raw_bronze` tests (`test_clean_staging_pass_writes_one_raw_bronze_row_with_zero_discrepancy` and 3 siblings) fail with `psycopg.errors.InvalidTextRepresentation: invalid input syntax for type bigint` on the `_source_row_number` column during `COPY INTO staging.customers__r<N>` — the value being written looks like a `_record_hash` hex string, suggesting a column-count/ordering mismatch in `StagingLoader`'s `COPY` column list vs. its value tuples (`packages/dataplat/src/dataplat/load/staging.py`), unrelated to `stage_ingest`'s reconciliation-writing step. Confirmed pre-existing and out of scope for plan 11-11: `_table_checksum`/`_compute_silver_gold_reconciliation` (the only functions plan 11-11 touches) are called exclusively from `publish_ingest`, never from `stage_ingest` (the function these 4 failing tests exercise) — this plan's diff makes no change reachable from that code path. Reproducer: `uv run --group cluster pytest tests/integration/test_reconciliation.py -q`. | Open | 2026-08-22 (plan 11-11) |
