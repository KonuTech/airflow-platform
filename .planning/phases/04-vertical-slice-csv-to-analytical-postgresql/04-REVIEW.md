---
phase: 04-vertical-slice-csv-to-analytical-postgresql
reviewed: 2026-08-14T06:52:48Z
depth: standard
files_reviewed: 9
files_reviewed_list:
  - packages/csv-processor/src/csv_processor/cli.py
  - packages/dataplat/src/dataplat/metadata/postgres.py
  - packages/dataplat/src/dataplat/metadata/repository.py
  - packages/dataplat/src/dataplat/pipeline/run.py
  - scripts/repair-duplicate-file-lineage.py
  - tests/integration/test_discover_files.py
  - tests/integration/test_metadata_repository.py
  - tests/integration/test_run_ingest.py
  - tests/unit/test_csv_processor_cli.py
findings:
  critical: 0
  warning: 7
  info: 5
  total: 12
status: issues_found
---

# Phase 4: Code Review Report

**Reviewed:** 2026-08-14T06:52:48Z
**Depth:** standard
**Files Reviewed:** 9
**Status:** issues_found

## Summary

This is a **gap-closure re-review**, not a full-phase review. Plans `04-10` and `04-11` were executed specifically to close `CR-01`, `CR-02` and `WR-01` from the prior review (`.planning/phases/04-vertical-slice-csv-to-analytical-postgresql/04-REVIEW.md` as it read at commit `603a9a2`, dated 2026-08-13). This review's scope is exactly the 9 files those two plans touched — it does not re-audit the other 54 files the original 63-file review covered.

**Methodology beyond static reading:** for every claimed fix, I read the current, full file content (not just the diff), then independently verified with tooling rather than trusting the plan summaries:
- `git diff 603a9a2..HEAD` for these 9 files, to see precisely what changed since the prior review.
- `ruff check` against all 9 files (including a targeted `--select BLE001` run against `cli.py` to verify a claim made in a code comment) — **all checks passed**.
- `mypy` against the 5 non-test source files — **no issues found**.
- Live execution of the new/updated tests against real testcontainers PostgreSQL 18 + MinIO (not mocks): all 3 integration test files (`test_metadata_repository.py`, `test_discover_files.py`, `test_run_ingest.py` — 30 tests total) and the new unit test file (`test_csv_processor_cli.py` — 2 tests) pass cleanly, including every test specifically written to reproduce the original CR-01/CR-02/WR-01 defects.

**Verdict on the three targeted fixes: all three are genuinely and correctly closed**, not superficially. Details and evidence below in "Verified Fixes."

**New findings from this round:** the fixes themselves introduce no regressions in the paths they touch, but the review surfaced quality gaps in the code these plans *added* — primarily in the brand-new `scripts/repair-duplicate-file-lineage.py` backfill tool (2 Warnings: its diagnostic/repair queries are narrower than they present themselves as being) plus two lower-severity Info items (a residual, low-impact heartbeat race distinct from CR-01, and zero test coverage for the new repair script).

**Carried-over findings:** `WR-02` through `WR-06` and `IN-01` through `IN-03` from the prior review were not part of this round's assignment. Where their cited code happens to live in one of this round's 9 files (`cli.py`, `postgres.py`, `run.py`), I incidentally re-observed the current code while reading these files for the primary task and confirmed the underlying condition is still present — see "Carried Over" below for exactly which ones that applies to. Where their cited code lives entirely outside this round's 9 files, I have no new evidence either way and list them as pure pointers, per the review's scope.

## Verified Fixes (This Round)

### CR-01 (prior review) — Heartbeat thread reverting a `SUCCEEDED` run back to `RUNNING` — CLOSED

**Fix location:** `packages/dataplat/src/dataplat/metadata/postgres.py:365-390` (new `heartbeat_ingestion_run` method, `WHERE run_id = %s AND status = 'RUNNING'` guard), `packages/dataplat/src/dataplat/metadata/repository.py:320-357` (matching `Protocol` declaration), `packages/dataplat/src/dataplat/pipeline/run.py:202-208` (`_heartbeat_loop` now calls `heartbeat_ingestion_run` instead of the unconditional `update_ingestion_run_status`).

This is a correct fix, not a cosmetic one. The race window itself still exists (a heartbeat tick can still land after the publish transaction commits `SUCCEEDED` and before `stop_heartbeat.set()` runs) — but the write that tick performs is now a guarded `UPDATE ... WHERE status = 'RUNNING'`, so it silently affects zero rows once the run is terminal, instead of unconditionally overwriting `status` back to `RUNNING` with a fresh 5-minute lease. I confirmed the `MetadataRepository` Protocol and the `PostgresMetadataRepository` implementation have byte-for-byte matching keyword-only signatures (`run_id`, `lease_expires_at`, `rows_read`, `rows_parsed`), so there is no call-site/type mismatch.

**Empirically verified**, live against testcontainers PostgreSQL 18: `test_heartbeat_ingestion_run_updates_a_running_row`, `test_heartbeat_ingestion_run_is_a_noop_once_the_run_is_no_longer_running` (`test_metadata_repository.py`), `test_heartbeat_loop_tick_against_a_terminal_run_never_regresses_status`, `test_heartbeat_writes_a_live_nonzero_rows_read_while_running_before_return` (`test_run_ingest.py`) — all 4 pass. The terminal-status test in particular drives `_heartbeat_loop` directly on its own thread against an already-`SUCCEEDED` run for up to 5 seconds and asserts `status` never leaves `SUCCEEDED` on every poll — this is a genuine regression test for the exact race, not a happy-path check.

### CR-02 (prior review) — Non-deterministic `find_file_by_content_hash` silently excluding a legitimate file — CLOSED

**Fix location:** `packages/dataplat/src/dataplat/metadata/postgres.py:162-192` (`ORDER BY file_id ASC` added before `LIMIT 1`, line 187), `packages/dataplat/src/dataplat/metadata/repository.py:89-119` (matching Protocol docstring), plus a new one-off, dataset-agnostic live-data repair tool, `scripts/repair-duplicate-file-lineage.py`, for rows the old non-deterministic behavior already left corrupted before this fix was deployed.

I traced `dataplat/discovery.py`'s (unchanged, out-of-scope-but-read-for-context) rediscovery-correction logic against the new deterministic query by hand for a 3-file duplicate-content group (first file, then a second, then a third arriving across separate `discover_files` passes) and confirmed it now converges correctly: the lowest `file_id` in a content-hash group is always `find_file_by_content_hash`'s stable answer, so the "is this a rediscovery of my own row, or a genuine duplicate of an earlier row" branch in `discovery.py` gets a consistent answer on every call, for every file in the group.

**Empirically verified**, live: `test_find_file_by_content_hash_resolves_to_the_lowest_file_id_deterministically` (`test_metadata_repository.py`, asserts the same answer across 5 repeated calls) and `test_three_way_duplicate_content_resolves_deterministically_across_reruns` (`test_discover_files.py`, the actual `discover_files` reproduction of the exact accumulation shape that produced the live `file_id=10` orphan) — both pass.

### WR-01 (prior review) — `ingest()` not writing a Receipt for non-`DataPlatformError` exceptions — CLOSED

**Fix location:** `packages/csv-processor/src/csv_processor/cli.py:188-214` (new `_failure_receipt(doc)` helper, deduplicating the `Receipt(...)` construction), `:277-296` (`except DataPlatformError:` retained first, new `except Exception:` added second, both calling `_failure_receipt(doc)` then `raise`).

Confirmed the except-clause ordering is correct (Python evaluates clauses top-to-bottom; `DataPlatformError` — itself an `Exception` subclass, confirmed in `dataplat/errors.py` — is listed first, so it is never shadowed by the broader clause below it) and that the new clause never intercepts `BaseException`-only families (`KeyboardInterrupt`, `SystemExit`) since it only catches `Exception`. Also confirmed `dataplat.cli.main()` (the actual process entry point) catches *only* `DataPlatformError` plus click's own control-flow exceptions — any other exception, including the `RuntimeError` these new tests inject, propagates all the way out of `main()` uncaught — so this fix in `csv_processor.cli.ingest()` is the only place in the call chain that could ever write a Receipt for that failure class; there is no double-write or redundant-catch concern with the outer boundary.

One inline comment in the new code makes a specific, checkable claim: *"No blind-except lint suppression is needed here: ruff's BLE001 check does not fire on a branch that always re-raises rather than swallowing the exception."* I verified this directly (`uv run ruff check cli.py --select BLE001` → `All checks passed!`) — the claim is accurate, not just plausible-sounding.

**Empirically verified**, live: `test_ingest_writes_a_failed_receipt_for_a_non_dataplatformerror_exception` (a raw `RuntimeError`, not wrapped by `DataPlatformError`, still results in a `status="FAILED"`/`run_id=-1` Receipt on disk, then still propagates so Airflow observes the non-zero exit) and `test_ingest_dataplatformerror_path_is_unaffected_by_the_new_except_clause` (the pre-existing `DataPlatformError` path is byte-for-byte unaffected) — both pass, via the real `dataplat.cli.main()` entry point (not a mocked Click runner).

## Carried Over From Prior Review (Not Re-Assessed)

The following are unchanged from the prior review dated 2026-08-13 (commit `603a9a2`). Per this round's assignment, they are out of scope for fresh investigation. Where I incidentally re-read their cited file this round for the primary task, I note that below; where I did not, I list them as a pure pointer only.

- **WR-02** — No code path ever writes `meta.ingestion_runs.status = 'FAILED'`. *Incidentally re-confirmed*: `postgres.py`'s `claim_ingestion_run` (line 345) still treats `status IN ('PENDING', 'FAILED')` as reclaimable, and `run.py`/`cli.py` (both read in full this round) still contain no call that ever sets `status="FAILED"` anywhere — WR-01's fix writes a `Receipt` to the XCom *file*, which is a different, parallel concern from the `meta.ingestion_runs` *database row*'s status; it does not touch the latter. WR-02 remains genuinely open.
- **WR-03** — `attempt`/`try_number` is always `1` (`AIRFLOW_TASK_TRY_NUMBER` is never set by any KPO invocation). *Incidentally re-confirmed on the `cli.py` side*: `attempt=int(os.environ.get("AIRFLOW_TASK_TRY_NUMBER", "1")))` is still present, now at line 259. The other half of this finding (`airflow/dags/_common/kpo.py`) was not read this round — no new evidence there either way.
- **WR-04** — A single row that fails to cast at publish time aborts the entire file's publish (`load/publish/merge.py`, `load/staging.py`, `csv_processor/source.py`). None of those files are in this round's scope — pure pointer, no new evidence.
- **WR-05** — SQL identifiers built from an unvalidated config field, `DatasetConfig.dataset` (`config/model.py`, `load/staging.py`, `load/publish/merge.py`). None of those files are in this round's scope — pure pointer, no new evidence.
- **WR-06** — `--dataset` CLI argument builds a filesystem path with no traversal guard. *Incidentally re-confirmed*: `Path(f"configs/datasets/{dataset}.yaml")` (`cli.py:150`, inside `discover()`) is unchanged and still has no validation ahead of it.
- **IN-01** — `Receipt.rows_deduplicated`'s docstring doesn't capture what the field actually measures (`models/receipt.py`, `pipeline/run.py`). *Incidentally re-confirmed on the `run.py` side*: the `rows_deduplicated = max(staging_result.rows_parsed - result.rows_affected, 0)` computation and its caveat-laden inline comment are unchanged (now lines 349-357); I also re-read `models/receipt.py` for cross-reference and its docstring at line 32 still reads "Number of rows collapsed by deduplication" with no caveat, even though it is outside this round's formal 9-file scope.
- **IN-02** — `_build_common()` doesn't close the pool if `pool.open()` raises after `create_pool()` succeeds (`cli.py:71-87`). *Incidentally re-confirmed*: the function is byte-for-byte unchanged. While re-reading it this round I also noticed the same leak class is broader than the original wording: if `S3ObjectStore(...)` (the very next line after `pool.open(wait=True)` succeeds) raises instead, the already-opened `pool` is *also* never assigned to the caller's own `pool` variable in `ingest()`/`discover()` (since `pool, metadata, objects = _build_common()` fails atomically), so it leaks the same way. The original fix suggestion (wrap `_build_common()`'s own body in try/except-and-close) already covers this broader case too, with no change needed to the suggested fix itself.
- **IN-03** — Stale comment in `scripts/ingest-demo.py` contradicts the current implementation. That file is not in this round's scope — pure pointer, no new evidence.

## Warnings

### WR-07: The new repair script only detects `duplicate_of_file_id IS NULL`, not an existing wrong (non-`NULL`) value

**File:** `scripts/repair-duplicate-file-lineage.py:111-122` (`_DIAGNOSTIC_SQL`), `:124-136` (`_REPAIR_SQL`)

**Issue:** Both the diagnostic `SELECT` and the repair `UPDATE` filter on `f.duplicate_of_file_id IS NULL`:

```sql
WHERE f.file_id <> g.original_file_id
  AND f.duplicate_of_file_id IS NULL
```

This is narrower than the actual defect class the old, non-deterministic `LIMIT 1` (no `ORDER BY`) could produce. Before the fix, `find_file_by_content_hash` could return *any* matching row, not only `NULL` vs. the true original. Concretely: with a 3-file content group `[5, 7, 9]`, if file `9` was discovered while `find_file_by_content_hash` happened to return `7` (a genuine duplicate, but not the group minimum) instead of `5`, `9.duplicate_of_file_id` would be persisted as `7` — a real value, not `NULL`, but still the *wrong* one (it should point at `5`, the true original, per this script's own `MIN(file_id)` convention and per the fixed `find_file_by_content_hash`'s new `ORDER BY file_id ASC`). This script's diagnostic query cannot see that row at all (it isn't `NULL`), so it is neither reported as an "orphan" nor repaired, and the tool's own closing message — `"Re-verified: zero orphaned duplicate files remain."` — would be printed even though a real mis-attribution still exists in `meta.files`.

This is not a hypothetical edge case invented for this review: it is a direct, provable consequence of the exact non-determinism this whole script exists to clean up after (`LIMIT 1` with no `ORDER BY` can return *any* matching row, not just `NULL`-vs-original). There is currently zero automated test coverage for this script (see IN-05) that would have caught the gap.

**Fix:** widen the match condition to catch both shapes — "unset" and "set to the wrong file" — using `IS DISTINCT FROM`, which correctly treats `NULL` as distinct from any non-null value without the three-valued-logic pitfalls of `<>`/`!=`:

```sql
-- both _DIAGNOSTIC_SQL and _REPAIR_SQL:
 WHERE f.file_id <> g.original_file_id
   AND f.duplicate_of_file_id IS DISTINCT FROM g.original_file_id
```

### WR-08: The repair script's correctness invariant silently assumes every dataset's `duplicate_policy` is `"skip"`

**File:** `scripts/repair-duplicate-file-lineage.py:29-32` (module docstring's "generic and dataset-agnostic ... on any dataset" claim), `:84-100` (`_CONTENT_GROUPS_CTE`)

**Issue:** `_CONTENT_GROUPS_CTE` treats *every* `(dataset_id, content_sha256)` group with more than one row as a group whose non-original members *should* have `duplicate_of_file_id` set to `MIN(file_id)`:

```sql
WITH content_groups AS (
    SELECT dataset_id, content_sha256, MIN(file_id) AS original_file_id, COUNT(*) AS group_size
      FROM meta.files
     GROUP BY dataset_id, content_sha256
    HAVING COUNT(*) > 1
)
```

That invariant only holds for datasets configured with `duplicate_policy: skip` (`dataplat.discovery.discover_files` only sets `duplicate_of_file_id` — ever — when `config.source.duplicate_policy == "skip"`; see `discovery.py:182`). `SourceConfig.duplicate_policy` (`dataplat/config/model.py`) is a free-form `str`, and its own docstring explicitly anticipates more values later ("the only value *this phase* defines is `\"skip\"`"). The query has no join to dataset config and no per-dataset awareness at all — it operates on raw `meta.files` rows across every dataset in the cluster. Today this is **not exploitable** — the one committed dataset config (`configs/datasets/customers.yaml`) uses `duplicate_policy: skip`, so the invariant happens to hold everywhere it could currently run. But the script's own docstring explicitly advertises itself as "generic and dataset-agnostic ... it repairs every orphaned duplicate-content group it finds, on any dataset" — that claim is only true as long as every dataset ever onboarded keeps `duplicate_policy: skip`, which nothing in this script (or the schema) enforces or even checks. The very first dataset onboarded with a different policy would have any of its legitimately-independent, same-content files force-linked by a future run of this tool.

**Fix:** either scope the query to datasets actually using the `skip` policy, or fail loudly rather than silently when it can't determine that. A minimal, cheap guard given this script's manual, one-off nature — surface a loud warning naming the affected datasets rather than assuming:

```python
# Before repairing, cross-check duplicate_policy for every affected dataset
# against configs/datasets/<name>.yaml (or meta.config_versions' stored
# config_document); refuse (or --force) if any affected dataset's current
# policy is not "skip", since this script's whole correctness invariant
# depends on it.
```

## Info

### IN-04: Heartbeat write has no claim-owner fencing (narrower, lower-impact cousin of CR-01)

**File:** `packages/dataplat/src/dataplat/metadata/postgres.py:365-390`, `packages/dataplat/src/dataplat/pipeline/run.py:163-208`

**Issue:** CR-01's fix correctly guards `heartbeat_ingestion_run` against a *terminal*-status regression (`WHERE run_id = %s AND status = 'RUNNING'`). It does not, and was not designed to, guard against a *same*-status write from a stale claim generation: if a pod's heartbeat thread survives long enough (process alive, but its main staging/publish work stalled) for its own claim's lease to expire and a retrying pod to reclaim the same `run_id` (still setting `status='RUNNING'`, now under a new `k8s_pod_name`), the original pod's next heartbeat tick still matches this `WHERE` clause and will overwrite the new claimant's `rows_read`/`rows_parsed` (and refresh `lease_expires_at`, harmlessly extending it) with the old pod's own, possibly-stale progress numbers.

This is scored as Info rather than Warning because: (1) `rows_read`/`rows_parsed` are documented, explicitly, as live-progress-only fields for an external poller — not the authoritative record, which is `rows_loaded` written once, atomically, by `finalize_publication` inside the publish transaction; (2) `lease_expires_at` can only ever be pushed *forward* by a stale write (`datetime.now(tz=UTC) + _LEASE_DURATION` computed fresh at write time), never backward, so it cannot cause a lease to expire early or a claim to be incorrectly granted/refused; (3) triggering it requires a fairly specific double fault (a pod's main thread stalls long enough to lose its lease while its heartbeat thread and process both remain alive) that is architecturally narrower than CR-01's window (which fired on every single successful run, deterministically, once the timing lined up).

**Fix (optional hardening, not required for correctness of the current fix):** fence the `UPDATE` by claim owner as well as status, e.g. add `AND k8s_pod_name = %s` (passing the current pod's own identity, already resolved in `run_ingest` via `os.environ.get("HOSTNAME", "unknown")` for `claim_ingestion_run`) to `heartbeat_ingestion_run`'s `WHERE` clause:

```sql
UPDATE meta.ingestion_runs
   SET lease_expires_at = %s, rows_read = %s, rows_parsed = %s
 WHERE run_id = %s AND status = 'RUNNING' AND k8s_pod_name = %s
```

### IN-05: The new repair script has zero automated test coverage

**File:** `scripts/repair-duplicate-file-lineage.py` (whole file)

**Issue:** No test file anywhere in the repository references this script (`grep -rl "repair-duplicate-file-lineage" --include="*.py"` matches only the script itself). Its own module docstring references "the idempotency proof in this module's own acceptance criteria," which — if it exists — lives in planning artifacts (`04-10-PLAN.md`), not in `pytest`-collected, CI-enforced code. This is precisely why the two completeness gaps in WR-07/WR-08 were not caught before landing: nothing regression-tests `_find_orphans`/`_repair_orphans`'s SQL against a seeded multi-row duplicate-content group.

**Fix:** `_find_orphans`/`_repair_orphans` already take a plain `psycopg.Connection` and contain no `kubectl`/port-forward logic themselves — only `main()` and the `_port_forwarded_analytics`/`_read_analytics_credentials` helpers need a live cluster. Add `tests/integration/test_repair_duplicate_file_lineage.py` exercising `_find_orphans`/`_repair_orphans` directly against the existing `migrated_dsn` testcontainers fixture: seed a 3-row content-duplicate group with one row's `duplicate_of_file_id` deliberately `NULL` and (after fixing WR-07) one deliberately wrong-non-null, assert the diagnostic finds both, the repair UPDATE fixes both, and a second `_find_orphans` call afterward returns empty (the same idempotency property `--dry-run` and the module docstring already claim but do not currently prove in CI).

---

_Reviewed: 2026-08-14T06:52:48Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
