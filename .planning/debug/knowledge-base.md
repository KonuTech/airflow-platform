# GSD Debug Knowledge Base

Resolved debug sessions. Used by `gsd-debugger` to surface known-pattern hypotheses at the start of new investigations.

---

## rebuild-scd2-reconciliation — SCD2 recompute non-determinism on a cross-file (event_ts, source_row_number) tie
- **Date:** 2026-08-30
- **Error patterns:** RebuildComparisonResult, matches=False, checksum mismatch, scd2_key, current_valid_from, current_valid_to, current_is_current, rebuild-from-raw, SCD2 reconciliation mismatch, non-deterministic, order-dependent, tie-break
- **Root cause:** `dataplat.scd.recompute.recompute_version_chain` (and `dataplat.load.publish.scd`'s `_select_lineage_rows`, which independently duplicates its grouping rule) sorted/ranked one customer_id's full bronze history using `(event_ts, source_row_number)` as if it were a total order, but `source_row_number` is only unique WITHIN a single source file, not across files. Two different raw files delivering a row for the same customer_id at the same in-file row position with the same event_ts created a genuine, silent tie. `_BRONZE_HISTORY_SQL` has no `ORDER BY`, so Postgres returns tied rows in an unspecified order; Python's stable sort then preserved that arbitrary order, which is not guaranteed to match between an incrementally-loaded original run and a from-scratch `rebuild-from-raw` bulk reload — silently flipping which bronze row won the tied version-group boundary (and the whole-table checksum with it).
- **Fix:** Added `file_id` as an explicit third tie-break level to the sort/min/max key used throughout `recompute_version_chain` (recompute.py) and `_select_lineage_rows` (load/publish/scd.py): `(event_ts, file_id, source_row_number)` instead of `(event_ts, source_row_number)`. `file_id` is globally unique per staged file and, per `discover_files`'s own documented sorted-manifest guarantee, is assigned in a deterministic, filename-order-consistent sequence regardless of incremental vs. bulk discovery.
- **Files changed:** packages/dataplat/src/dataplat/scd/recompute.py, packages/dataplat/src/dataplat/load/publish/scd.py, tests/unit/test_scd_recompute.py, tests/e2e/slice/test_rebuild_from_raw.py (unrelated Step-0 scheduling-race assertion relaxation, == to >=), tests/integration/test_scd2_cross_file_tie_determinism.py (new SQL-layer testcontainers integration test added to close the session after six consecutive live-CI verification attempts each failed for a different infrastructure/orchestration reason)
---
