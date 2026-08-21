---
phase: 09-etl-correctness-dedup-incremental-backfill-recovery
plan: 11
subsystem: etl-recovery
tags: [airflow, backfill, watermark, dbt, reconciliation, vault, e2e, live-cluster]

# Dependency graph
requires:
  - phase: 09-etl-correctness-dedup-incremental-backfill-recovery
    provides: "09-05 (backfill CLI wiring), 09-07 (raw_bronze reconciliation + rejection dedup), 09-08 (bronze->silver reconciliation post-hook), 09-10 (processing_gaps + STAGE_LOAD tracking)"
provides:
  - "tests/e2e/slice/test_backfill_2year_sweep.py — the phase's live capstone proof, green against the real cluster: dry-run sizing, pilot window, full 2-year sweep (both datasets, all 7 D-05..D-22 assertions), idempotent re-run (QUAL-11), live+backfill concurrency (D-12/D-13)"
  - "record_watermark's MAX() subquery scoped to WHERE _run_id = ANY(run_ids) — a genuine, previously-unknown live correctness bug (unscoped MAX() over the whole cumulative silver.<dataset> table let ANY stray row from ANY run permanently poison a dataset's watermark, since GREATEST() can never regress it back down)"
  - "Live-confirmed discovery: the deployed dbt image (localhost:5001/dbt:faa7533) was one commit stale, missing dbt/macros/reconciliation_post_hook.sql entirely — meta.reconciliation_results had NEVER recorded a single bronze_silver row in the platform's history despite plan 09-08 being marked complete, because its own live verification never happened against the actually-deployed image"
  - "Live-confirmed discovery: Vault's file-storage backend can accumulate null-byte-corrupted lease bookkeeping entries from a hard container kill (host/Docker restart), which crash-and-reseal Vault on every subsequent unseal attempt until the corrupted file(s) are removed — beyond the previously-documented 'just re-run vault-unseal' remedy"
affects: [future-live-cluster-debugging, future-dbt-image-deploy-workflow]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Live-verification-before-image-deploy: a code fix committed to the worktree is NOT live until its image is rebuilt (make image-<name>) AND the corresponding Airflow Variable is re-registered — this plan found the dbt image had drifted one commit behind the repo with no CI gate to catch it"
    - "Force-fresh-reprocess repair pattern: to exercise a fixed code path against already-published data without deleting ingestion history, re-upload the SAME raw file with byte-different-but-business-identical content (a duplicated trailing row) via the s3_client test fixture — content_sha256 changes, discover_files treats it as a genuinely new file version (matches §63 raw-immutability: corrections arrive as new files, never overwrites), and the full pipeline re-runs for real"

key-files:
  created: []
  modified:
    - tests/e2e/slice/test_backfill_2year_sweep.py
    - packages/dataplat/src/dataplat/metadata/repository.py
    - packages/dataplat/src/dataplat/metadata/postgres.py
    - packages/dataplat/src/dataplat/pipeline/run.py

key-decisions:
  - "Fixed record_watermark's MAX() scoping bug in scope (touches dataplat, outside this plan's declared files_modified) rather than deferring — confirmed with the user first. Deferring would have meant D-01/D-02's own watermark correctness guarantee could never be honestly proven live, since the poisoned value was blocking the capstone test's own watermark assertion."
  - "Rebuilt and redeployed the dbt image (localhost:5001/dbt:46da94a) after discovering it was one commit stale and missing reconciliation_post_hook.sql entirely — root-caused via a manual dbt build --debug run against the live DB from a throwaway debug pod (in-cluster, avoiding this environment's documented port-forward instability), which proved the SQL itself was correct and the gap was purely a stale deployment artifact."
  - "Repaired two already-poisoned/missing pieces of live data via direct SQL after each root-cause fix, always with explicit user confirmation first: (1) deleted the poisoned meta.watermarks row for customers so a fresh scoped publish could recreate it; (2) when that row turned out to need a genuinely fresh publish pass to regenerate (already-published data doesn't re-trigger under idempotency), force-reprocessed one file via the duplicate-row technique instead of a raw literal INSERT, to exercise the real code path live rather than fake the result."
  - "Removed two null-byte-corrupted Vault lease bookkeeping files (auth/kubernetes/login/*) after Vault kept crash-resealing on every unseal attempt during post-unseal lease restoration — confirmed via od that both files were pure null bytes (classic hard-kill-mid-write corruption), confirmed they were stale/already-expired k8s-auth leases (not secrets, policies, or the encryption keyring), and got explicit user confirmation before each deletion."
  - "Cleared an orphaned backfill (AlreadyRunningBackfill blocking new backfill creation) via the documented direct-DB-mutation remedy — UPDATE dag_run SET state='failed' for its stuck running/queued runs, UPDATE backfill SET completed_at=now() — after confirming with the user, since a prior host-restart interruption had left it without ever reaching Airflow's own completion bookkeeping despite its actual work finishing."
  - "Widened Task 3's wait timeouts (900s->3600s, 600s->1800s, 600s->2700s, 300s->600s) after live-observing that a re-triggered no-op dagrun still pays the full per-file KPO pod-startup cost (idempotency is checked inside discover, after pods already spun up) — 'zero new rows' does not mean 'fast' under this cluster's real CPU contention. The dagrun that timed out at the old 900s completed naturally 2 minutes later, confirming a timeout-sizing issue, not a functional bug."
  - "Bumped the test corpus's _MASTER_SEED (v2->v3) to exercise the whole 26-file corpus fresh under both fixes, rather than reusing data already published under the pre-fix code — customer_id/order_id values are a deterministic day-index formula (unaffected by the seed), only cosmetic fields change, so this was safe against every other assertion's own id-range assumptions."
  - "Both csv_ingest_customers and csv_ingest_orders left UNPAUSED at the end (the plan's own required end state) — but discovered live that leaving customers unpaused unattended for many hours (while this session was deep in unrelated Vault debugging) let its */1 * * * * schedule accumulate a large backlog of queued DagRuns; paused it deliberately mid-session to stop the growth, then unpaused again for the final proof runs and left it unpaused per the plan's requirement."

requirements-completed: [INCR-05, INCR-06, QUAL-11]

# Metrics
duration: "~3h of active work across 3 sessions spanning 2026-08-19 21:53 to 2026-08-21 08:48 CEST (Task 1 committed 08-19; Task 2/3 root-causing, fixing and live proof spanned a very long 08-20->08-21 session with two live-cluster incidents — a silent host restart and a Vault storage-corruption crash-loop — that consumed the majority of the elapsed wall time"
completed: 2026-08-21
---

# Phase 9 Plan 11: Live 2-year backfill capstone proof — Task 2 & 3 Summary

**The full live capstone proof (dry-run sizing, pilot, 2-year sweep across both datasets with all 7 D-05..D-22 assertions, idempotent re-run, live+backfill concurrency) passes against the real cluster, after finding and fixing two genuine previously-unknown correctness/deployment bugs (watermark MAX() scoping, a stale dbt image missing its own reconciliation macro) and recovering from two live-infrastructure incidents (host restart, Vault storage corruption).**

## Performance

- **Duration:** ~3h of active engineering work, spread across a much longer elapsed wall-clock window due to two genuine live-infrastructure incidents mid-session
- **Started:** 2026-08-19T21:53:52+02:00 (Task 1)
- **Completed:** 2026-08-21T10:24:00+02:00 (Task 3's final live proof)
- **Tasks:** 3/3 (Task 1 committed in a prior session; Task 2 and Task 3 completed this session)
- **Files modified:** 4 (1 test file, 3 dataplat source files)

## Accomplishments

- `tests/e2e/slice/test_backfill_2year_sweep.py`'s full suite is green against the real kind cluster: dry-run sizing, pilot window, the 2-year sweep's all 7 correctness assertions (dedup ordering, historical schema resolution, gap handling, both datasets' watermark advancement, all-3-hop reconciliation with D-22's exact accounting formula, live-DagRun-through-the-same-task-graph), the idempotent re-run (QUAL-11), and live+backfill concurrency with no corruption (D-12/D-13).
- Found and fixed a genuine, previously-unknown live correctness bug: `record_watermark`'s `MAX()` subquery read the WHOLE cumulative `silver.<dataset>` table instead of scoping to the current pass's own staged runs, so a single stray/out-of-order row from ANY run (this run or an unrelated one) could permanently poison a dataset's watermark, since `GREATEST()` can never regress it back down.
- Found and fixed a genuine deployment gap: the live dbt image was one commit stale and missing `dbt/macros/reconciliation_post_hook.sql` entirely — `meta.reconciliation_results` had never recorded a single `bronze_silver` row in the platform's history, despite plan 09-08 (which added that macro) being marked complete, because its own live verification never actually ran against the deployed image.
- Recovered from two genuine live-infrastructure incidents mid-session: a silent host/Docker-Desktop restart (this session's own documented recurring failure mode) that broke kind's DAG hostPath mount, and — beyond the previously-documented remedy — a Vault file-storage corruption (two null-byte-corrupted lease bookkeeping files from the hard container kill) that crash-resealed Vault on every unseal attempt until removed.

## Task Commits

Each task was committed atomically:

1. **Task 1: dry-run sizing + pilot window** - `d285ac8` (test) — committed in a prior session
2. **Referential-barrier connection-reuse fix** (found live during Task 2, out of declared scope, user-confirmed) - `0f7e3b2` (fix) — committed in a prior session
3. **Task 2 live fixes: row-count scoping, max_active_runs=1, timeout tuning** - `d2744fd` (test) — committed in a prior session
4. **record_watermark MAX() scoping fix** (found live this session, out of declared scope, user-confirmed) - `46da94a` (fix)
5. **Task 2: live 2-year sweep proof passes** - `5c60368` (test)
6. **Task 3: widen wait timeouts to match observed live cluster pace** - `1c40360` (test)

**Plan metadata:** (this commit — docs: complete plan)

## Files Created/Modified

- `tests/e2e/slice/test_backfill_2year_sweep.py` - The live capstone proof (all tasks); `_MASTER_SEED` bumped v2->v3; Task 3's wait timeouts widened based on live-observed cluster pace
- `packages/dataplat/src/dataplat/metadata/repository.py` - `MetadataRepository.record_watermark` Protocol: added `run_ids: Sequence[int]` parameter
- `packages/dataplat/src/dataplat/metadata/postgres.py` - `PostgresMetadataRepository.record_watermark`: `MAX()` subquery scoped by `WHERE _run_id = ANY(%(run_ids)s)`
- `packages/dataplat/src/dataplat/pipeline/run.py` - `publish_ingest`'s call site: passes the full `staged_run_ids` list, not just its max

## Decisions Made

See `key-decisions` in the frontmatter above — six live, user-confirmed decisions covering the watermark fix scope, the dbt image redeploy, three separate live-data repairs (watermark row, corrupted Vault lease files, orphaned backfill bookkeeping), and the Task 3 timeout widening.

## Deviations from Plan

### Auto-fixed Issues (all user-confirmed live, not auto-applied silently)

**1. [Correctness] `record_watermark`'s unscoped `MAX()` permanently poisons a dataset's watermark**
- **Found during:** Task 2's live re-run, investigating why the customers watermark assertion kept failing
- **Issue:** `MAX({watermark_column}) FROM {source_table}` read the whole cumulative table; a single stray row (from this run or any other, past or future) sets a wrong high-water mark that `GREATEST()` can never lower again
- **Fix:** Scoped the subquery to `WHERE _run_id = ANY(%(run_ids)s)` — only the rows this specific publish pass staged
- **Files modified:** `packages/dataplat/src/dataplat/metadata/{repository,postgres}.py`, `packages/dataplat/src/dataplat/pipeline/run.py`
- **Verification:** All existing `tests/integration/test_watermarks.py` (3/3) and related integration suites (24/24 across publish/reconciliation/referential-integrity/lineage/claim-lease/run-ingest) pass; full unit suite (511/511) passes; live-verified via the capstone sweep's own watermark assertion
- **Committed in:** `46da94a`

**2. [Deployment gap] dbt image missing `reconciliation_post_hook.sql`**
- **Found during:** Task 2's live re-run, assertion (6) — `meta.reconciliation_results` had zero `bronze_silver` rows ever, for either dataset
- **Issue:** The deployed `localhost:5001/dbt:faa7533` image predated commit `4d6a99a` (plan 09-08, which added the macro and wired it into both silver models) by exactly one commit
- **Fix:** `make image-dbt` rebuilt and redeployed from current HEAD; re-registered the `dbt_image` Airflow Variable
- **Files modified:** none (deployment-only; the macro file already existed correctly in the repo)
- **Verification:** Manual `dbt build --debug` from an in-cluster throwaway debug pod against the live DB, confirming both post-hook statements execute and commit; live-verified via the capstone sweep's own reconciliation assertion (all 3 hops, both datasets)
- **Committed in:** N/A (deployment action, not a code change)

**3. [Infra] Vault file-storage corruption beyond the documented remedy**
- **Found during:** Recovering from a silent host restart mid-session (a previously-documented, recurring failure mode) — `make vault-unseal` reported success but Vault immediately crash-resealed itself
- **Issue:** Two lease bookkeeping files under `/vault/data/sys/expire/id/auth/kubernetes/login/` were pure null bytes (hard-kill-mid-write corruption); Vault's post-unseal lease-restoration step hit invalid JSON and did a hard shutdown+reseal rather than skipping the bad entry
- **Fix:** Removed the two corrupted files (confirmed stale/already-expired k8s-auth leases, not secrets/policies/keyring) after user confirmation; re-ran `make vault-unseal`
- **Files modified:** none (live Vault storage, not repo state)
- **Verification:** `vault status` stable (`Sealed: false`) with no further crash-loop; `vault-0` restart count unchanged (confirming this was Vault's own graceful reseal, not a container crash)

---

**Total deviations:** 3 fixes beyond the plan's declared scope, all live-discovered, all user-confirmed before any code change or data mutation. Plus 2 further user-confirmed live-data repairs (a poisoned watermark row cleared; one orphaned backfill's stuck bookkeeping closed out) that were operational corrections, not code changes.
**Impact on plan:** All were necessary for the plan's own success criteria to be honestly provable live — deferring any of them would have left the capstone test permanently red for reasons outside this plan's own test code. No scope creep beyond what live verification itself demanded.

## Issues Encountered

- **Silent host/Docker-Desktop restart mid-session** (this project's own documented recurring failure mode, STATE.md) — broke kind's `/mnt/dags` hostPath mount (fell back to `tmpfs`) and orphaned several DagRuns/task pods. Resolved via the documented remedy (`docker restart` on all 3 kind node containers, `make vault-unseal`, zombie pod cleanup).
- **`csv_ingest_customers` scheduling backlog** — leaving the DAG unpaused (required for the test suite's own autouse fixture) unattended for many hours while debugging the Vault incident let its `*/1 * * * *` schedule accumulate a queue of DagRuns. Paused it deliberately to stop growth once noticed; the backlog itself drained without incident (idempotent, no data corruption) before being re-unpaused for the final test runs.
- **Orphaned backfill blocking new backfill creation (`AlreadyRunningBackfill`)** — a backfill interrupted by the host restart never reached Airflow's own `completed_at` bookkeeping despite its actual dagruns finishing. Closed out via the documented direct-DB remedy after user confirmation.
- **Already-published data doesn't naturally exercise a fresh fix** — after fixing `record_watermark`, the SAME already-succeeded files simply idempotency-skip on re-run (by design), so the fix never got a chance to actually write a fresh value. Worked around by force-reprocessing one file via a byte-different-but-business-identical re-upload (duplicated trailing row) — exercises the real code path live rather than a synthetic repair.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Phase 9's full plan set (09-01 through 09-11) is now complete. All ROADMAP Phase 9 success criteria are live-proven: dedup ordering and reconciliation, incremental watermark correctness (with a genuine bug found and fixed), backfill idempotency (QUAL-11), live+backfill concurrency (D-12/D-13), and recovery from pod-kill/host-restart/Vault-corruption incidents. No known blockers for closing out the phase.

---
*Phase: 09-etl-correctness-dedup-incremental-backfill-recovery*
*Completed: 2026-08-21*
