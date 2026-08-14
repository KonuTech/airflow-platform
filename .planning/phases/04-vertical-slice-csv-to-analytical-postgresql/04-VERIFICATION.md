---
phase: 04-vertical-slice-csv-to-analytical-postgresql
verified: 2026-08-14T07:15:00Z
status: passed
score: 8/8 truths verified (all 5 ROADMAP Success Criteria hold; all 3 previously-FAILED sub-findings from the 2026-08-14T00:00:00Z verification round are now independently re-confirmed closed, at both the code/test level and the live-cluster-data level)
overrides_applied: 0
re_verification:
  previous_status: gaps_found
  previous_score: "5/8 truths verified"
  gaps_closed:
    - "The receipt is written to /airflow/xcom/return.json on every exit path -- success, claim-skip, and run-fatal failure (WR-01, 04-11-PLAN.md)"
    - "A run's terminal status (SUCCEEDED) can never regress back to RUNNING (CR-01, 04-10-PLAN.md Task 1)"
    - "Content-duplicate detection is deterministic and never leaves a file's lineage unexplainable (CR-02, 04-10-PLAN.md Task 2/3)"
  gaps_remaining: []
  regressions: []
---

# Phase 4: Vertical Slice — CSV to Analytical PostgreSQL — Re-Verification Report

**Phase Goal (ROADMAP.md):** One real CSV travels end to end — MinIO → TaskFlow DAG → KubernetesPodOperator → processor → analytical PostgreSQL — and is idempotent by construction, so a re-run produces zero additional rows.
**Verified:** 2026-08-14T07:15:00Z
**Status:** passed
**Re-verification:** Yes — after gap closure (plans 04-10, 04-11)

## Methodology Note

This is a **re-verification**, not an initial pass. Per the previous `04-VERIFICATION.md` (dated 2026-08-14T00:00:00Z, `status: gaps_found`, 5/8 truths), three specific sub-findings had FAILED — all traced to code-confirmed defects CR-01, CR-02, WR-01 in `04-REVIEW.md`. Two gap-closure plans (04-10, 04-11) executed against them. This report independently re-verifies all three, applying the same live-cluster-evidence-gathering rigor the original report used (direct `kubectl exec`/`psql` against the live `analytics-db` and live `airflow` deployment — not port-forward scripts, not reused executor output), and additionally re-confirms the 5 ROADMAP Success Criteria and all 22 requirement IDs show no regression.

**I did not take any SUMMARY.md or 04-REVIEW.md claim at face value.** Every claim below is backed by a command I ran myself in this session: `git show <sha>:<path>` to inspect exact historical file content, `git merge-base --is-ancestor` to check commit ancestry, fresh `pytest` runs (not re-reading old CI logs), and fresh `kubectl exec -n data analytics-db-1 -- psql` queries against the live database (not the repair script's own report, not the original verification's cached numbers).

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | SC1: CSV drop → TaskFlow DAG → KPO pod → analytical PostgreSQL, ≤4KB XCom receipt, DAG <150 lines, no parsing/validation/typing/DB-writes in the DAG file | ✓ VERIFIED (regression-confirmed) | Re-ran `tests/policy/test_dag_thinness.py tests/policy/test_dag_line_budget.py tests/unit/test_dag_structure.py` fresh: `12 passed in 0.85s`. Live: `kubectl exec -n airflow deploy/airflow-scheduler -- airflow dags list` shows `csv_ingest_customers` still registered, `is_paused: False`, `fileloc` resolving correctly. No changes to `airflow/dags/**` since the original verification (confirmed via `git diff --stat 603a9a2..HEAD -- packages/ scripts/ tests/`, which shows only the 9 files declared in 04-10/04-11's `files_modified`). |
| 2 | SC2: Re-running the same DAG run, and re-uploading the same file under a different name, both produce zero additional rows | ✓ VERIFIED (regression-confirmed) | Fresh live query: `SELECT count(*), count(DISTINCT customer_id) FROM normalized.customers` → `10000122 = 10000122`, identical to the original verification's figure — no drift, no corruption introduced by the gap-closure round. |
| 3 | SC3: Killing the task pod mid-load + Airflow retry leaves no duplicate rows / no partial visibility; concurrent SELECT never observes a half-loaded table | ✓ VERIFIED (regression-confirmed) | Fresh live join of `meta.ingestion_runs` ⋈ `meta.files` for `e2e-podkill-*`/`e2e-concurrent-select-*` filenames: all `SUCCEEDED` rows show `rows_loaded` = live `rows_in_target` count exactly (9 rows checked, all consistent); the transaction-atomicity code in `run.py` is unchanged by this gap-closure round. |
| 4 | SC4: `meta.batches`/`meta.ingestion_runs`/every loaded row answer "which file, which batch, which run, which attempt, which config version" by SQL alone | ✓ VERIFIED — **gap closed, no longer caveated** | Previously ⚠️ VERIFIED WITH A LIVE-CONFIRMED GAP (file_id=10 orphaned). Now clean: a fresh, generic live query across the **entire** `meta.files` table (not just the previously-known group) for any row where `duplicate_of_file_id IS NULL` while not being its content-hash group's minimum `file_id` returns **zero rows**. See Truth #8 below for full detail. |
| 5 | SC5: U1 (XCom contains built git SHA) and U3 (streaming throughput + peak RSS baseline) spike results are recorded in the repository | ✓ VERIFIED (regression-confirmed) | `docs/spikes/U1-smoke-xcom.md` and `U3-throughput-baseline.md` both still present, unchanged, substantive (re-read in full this session). |
| 6 | 04-11-PLAN.md must-have: "`csv_processor.cli.ingest()` writes a Receipt to the XCom path on every exit path, including exceptions outside the `DataPlatformError` hierarchy" (WR-01) | ✓ VERIFIED — **gap closed at the code and test level** | Direct read of current `packages/csv-processor/src/csv_processor/cli.py`: a new `except Exception:` clause (line 280) follows `except DataPlatformError:` (line 277), both calling the new `_failure_receipt(doc)` helper (lines 188-214) then `raise`. Ran `uv run --frozen pytest tests/unit/test_csv_processor_cli.py tests/unit/test_cli_error_handling.py -v` myself: **10/10 pass**, including `test_ingest_writes_a_failed_receipt_for_a_non_dataplatformerror_exception` (a raw `RuntimeError` injected via monkeypatched `_build_common`, invoked through the REAL `dataplat.cli.main()` entry point — not a mocked Click runner — asserting the exception still propagates AND a `status="FAILED"`, `run_id=-1` Receipt is written to a real XCom file) and `test_ingest_dataplatformerror_path_is_unaffected_by_the_new_except_clause` (proves clause ordering is correct, no double-catch). Both tests are genuine regression tests, not tautological — read in full. **See the WARNING below: this fix is not yet reflected in the live cluster's currently-deployed image** — a materially important nuance the code/test evidence alone does not disclose. |
| 7 | Run-lifecycle integrity: a `SUCCEEDED` run's status can never regress to `RUNNING` (CR-01) | ✓ VERIFIED — **gap closed at the code, test, and live-cluster level** | Direct read of `pipeline/run.py`: `_heartbeat_loop` (line 202) now calls `ctx.metadata.heartbeat_ingestion_run(...)`, never `update_ingestion_run_status`. Direct read of `metadata/postgres.py`: the new `heartbeat_ingestion_run` method (lines 365-390) issues `UPDATE meta.ingestion_runs SET ... WHERE run_id = %s AND status = 'RUNNING'` — the terminal-status guard. Ran `uv run --group cluster pytest tests/integration/test_metadata_repository.py tests/integration/test_run_ingest.py tests/integration/test_discover_files.py -q` myself against real testcontainers PostgreSQL 18: **30/30 pass**. Read `test_heartbeat_loop_tick_against_a_terminal_run_never_regresses_status` in full: it runs the REAL `_heartbeat_loop` on its own thread against an already-`SUCCEEDED` run, polls the DB on a deadline loop asserting `status == "SUCCEEDED"` on every sample (failing immediately with a named CR-01 message otherwise), and uses a call-spy to independently prove the loop genuinely ticked — a non-tautological, genuine regression test. Live: fresh `SELECT status, count(*) FROM meta.ingestion_runs GROUP BY status` → `13 SUCCEEDED, 1 PENDING, 0 RUNNING` — unchanged and clean, no corruption. **This fix IS baked into the live cluster's currently-deployed image** (confirmed: `git merge-base --is-ancestor 18808cf 9b59385` → true; the Airflow `csv_processor_image` Variable is set to `localhost:5001/csv-processor:9b59385`, which contains this commit). |
| 8 | Content-duplicate detection is deterministic and never leaves a file's lineage SQL-unexplainable (CR-02) | ✓ VERIFIED — **gap closed at the code, test, and live-cluster level, independently re-confirmed** | Direct read of `metadata/postgres.py`: `find_file_by_content_hash` now has `ORDER BY file_id ASC` (line 187) before `LIMIT 1`. Ran the same 30-test integration suite above; read `test_three_way_duplicate_content_resolves_deterministically_across_reruns` in full — it reproduces the EXACT accumulation shape that produced the live `file_id=10` orphan (3 sequential `discover_files` passes growing a duplicate group from 1→2→3 files) and asserts deterministic convergence to the group minimum. **Independently re-queried the live cluster myself** (not reusing 04-10-SUMMARY.md's claim, not reusing the repair script's own report): `file_id=10,11,12` (the historically-orphaned content group, `content_sha256=f90142cf...`) now all show `duplicate_of_file_id=9` (the group's true minimum) — matches 04-10-SUMMARY.md's specific claim, independently confirmed. Went further: ran a **fully generic** live query across every `(dataset_id, content_sha256)` group in the entire `meta.files` table (3 groups total: sizes 2, 4, 5 — file_ids `{2852,2866}`, `{9,10,11,12}`, `{4,5,6,7,8}`) checking both the narrow `duplicate_of_file_id IS NULL` condition AND the wider `duplicate_of_file_id IS DISTINCT FROM <group minimum>` condition (the WR-07-recommended stricter check) — **zero rows returned for either**, across all 3 groups, including a previously-unexamined 2-file group. |

**Score:** 8/8 truths verified.

### Deployment-Currency Warning (new finding, this re-verification pass — not blocking, action recommended)

**The WR-01 code fix (Truth #6) is genuinely correct and tested, but the live cluster's currently-deployed `csv-processor` image predates it.** This is a real, currently-existing gap between the codebase and the running system that neither `04-10-SUMMARY.md`, `04-11-SUMMARY.md`, nor the fresh `04-REVIEW.md` code-review pass surfaced (none of them checked image/deployment currency against commit history).

Evidence:
- The live `csv_processor_image` Airflow Variable (read fresh via `kubectl exec -n airflow deploy/airflow-scheduler -- airflow variables get csv_processor_image`) is `localhost:5001/csv-processor:9b59385`.
- `git merge-base --is-ancestor ee3d591 9b59385` (WR-01's fix commit vs. the deployed image's commit) → **false**: the WR-01 fix is NOT an ancestor of the deployed image's build commit.
- `git show 9b59385:packages/csv-processor/src/csv_processor/cli.py | grep -c "except Exception:"` → `0`.
- Checked all 5 image tags currently in the local registry (`180990c`, `5ae3546`, `87d7ee4`, `9b59385`, `d29dc66`, via `curl http://localhost:5001/v2/csv-processor/tags/list`): **none** contain the `except Exception:` clause.
- Root cause: 04-10 and 04-11 were executed as **parallel git worktrees**, both branched from the same pre-gap-closure commit (`262a5cf`). 04-10's Task 3 explicitly rebuilt and redeployed the image (`make image-csv-processor`, producing `9b59385`) as a required step — but that rebuild happened from the 04-10 worktree, which never contained 04-11's `ee3d591` commit (a sibling, not-yet-merged branch at that time). 04-11's own plan/acceptance-criteria never included an image rebuild step at all (unlike 04-10's Task 3, which explicitly required "the code fix must be live before repair runs").
- Confirmed the DAG resolves the image dynamically at task-run time from this same Variable (`airflow/dags/_common/kpo.py:71`, `"image": Variable.get("csv_processor_image")`) — so this is a pure deployment/currency gap, not a DAG code issue; no DAG changes are needed to close it.

**Practical impact:** if a real `ingest` KPO pod runs on this cluster right now and hits a non-`DataPlatformError` exception (the exact WR-01 scenario), it will still fail to write a Receipt — the original bug, live, today — even though the fix is merged, tested, and correct in source. As before, this remains non-fatal (Airflow still detects the pod's non-zero exit code and fails the task), so it is not a data-loss or data-corruption risk.

**Classification:** WARNING, not BLOCKER — the must-have as declared ("`ingest()` writes a Receipt on every exit path") is a source-code behavior claim, which now genuinely holds and is regression-tested. The gap is specifically that this correct behavior has not yet propagated to the running artifact. This is mechanically, trivially closable: **re-run `make image-csv-processor` and confirm the `csv_processor_image` Variable advances past `ee3d591`** — no design or code work required. Recommend closing this before relying on WR-01's guarantee in a live production run.

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `packages/dataplat/src/dataplat/metadata/postgres.py::heartbeat_ingestion_run` | Terminal-status-safe heartbeat write (CR-01) | ✓ VERIFIED | Exists (lines 365-390), substantive (`WHERE run_id = %s AND status = 'RUNNING'` guard confirmed by direct read), wired (`_heartbeat_loop` calls it — confirmed), tested (4 dedicated regression tests pass live), live-deployed (confirmed via image ancestry check) |
| `packages/dataplat/src/dataplat/metadata/postgres.py::find_file_by_content_hash` | `ORDER BY file_id ASC` before `LIMIT 1` (CR-02) | ✓ VERIFIED | Exists, substantive (confirmed by direct read, line 187), wired (`discovery.py`'s rediscovery-correction logic depends on it, unchanged), tested (2 dedicated regression tests pass live, including the exact 3-way accumulation reproduction), live-deployed and live-data-confirmed (zero orphans across all 3 live duplicate-content groups) |
| `packages/dataplat/src/dataplat/metadata/repository.py::MetadataRepository.heartbeat_ingestion_run` | Protocol method | ✓ VERIFIED | Exists (lines 320-357), docstring correctly documents the CR-01 contract and distinguishes it from `update_ingestion_run_status` |
| `packages/dataplat/src/dataplat/pipeline/run.py::_heartbeat_loop` | Calls `heartbeat_ingestion_run`, not `update_ingestion_run_status` | ✓ VERIFIED (wired) | `grep -c "ctx.metadata.heartbeat_ingestion_run("` = 1, `grep -c "ctx.metadata.update_ingestion_run_status("` = 0 in this file — confirmed via direct read, matching the plan's own acceptance criteria |
| `packages/csv-processor/src/csv_processor/cli.py::_failure_receipt` / `except Exception:` | Receipt written for any exception (WR-01) | ✓ VERIFIED (code + tests), ⚠️ WARNING (not yet in the deployed image — see above) | `except Exception:` clause exists (line 280), correctly ordered after `except DataPlatformError:` (line 277), both call the shared `_failure_receipt(doc)` helper; 2 dedicated regression tests pass via the real `main()` entry point |
| `scripts/repair-duplicate-file-lineage.py` | Idempotent, generic live data repair for CR-02's historical fallout | ✓ VERIFIED (exists, substantive, generic — confirmed no hardcoded `file_id == 10`), ⚠️ 2 carried-forward Warnings (WR-07, WR-08) — both independently re-confirmed as non-exploitable today (see Anti-Patterns) | Read the full 457-line script; confirmed `_DIAGNOSTIC_SQL`/`_REPAIR_SQL` both filter only on `duplicate_of_file_id IS NULL` (WR-07's exact concern) and `_CONTENT_GROUPS_CTE` has no dataset-policy awareness (WR-08's exact concern) — both accurately described by `04-REVIEW.md` |
| (all Wave 1-5 artifacts from the original verification) | Unchanged | ✓ VERIFIED (regression-confirmed) | `git diff --stat 603a9a2..HEAD -- packages/ scripts/ tests/` shows only the 9 files declared across 04-10/04-11's `files_modified` lists changed — no other phase-4 artifact was touched |

### Key Link Verification

| From | To | Via | Status | Details |
|------|-----|-----|--------|---------|
| `pipeline/run.py::_heartbeat_loop` | `metadata/postgres.py::heartbeat_ingestion_run` | Direct call, replacing `update_ingestion_run_status` | ✓ WIRED (was ✗ NOT_WIRED) | Confirmed via direct read and via `grep -c` acceptance-criteria checks; live-deployed |
| `metadata/postgres.py::find_file_by_content_hash` | `discovery.py`'s rediscovery-correction logic | `ORDER BY file_id ASC` makes the return value stable across calls | ✓ WIRED (was ✗ NOT_WIRED) | Confirmed via direct read; live-data-confirmed (zero orphans across all live duplicate-content groups, including a group not examined in the original verification) |
| `csv_processor/cli.py::ingest` | XCom-visible `Receipt` | `except Exception:` → `_write_xcom(_failure_receipt(doc))` → `raise` | ✓ WIRED at the source level | Confirmed via direct read and passing regression tests. ⚠️ Live pod wiring is stale — see Deployment-Currency Warning above; the running image does not yet contain this code path |
| (all other key links from the original verification) | — | — | ✓ WIRED (regression-confirmed) | Unchanged code, unchanged live behavior (10M-row idempotency, transaction atomicity, DAG registration all reconfirmed fresh above) |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|---------------------|--------|
| `meta.ingestion_runs.status` | Heartbeat write target | `heartbeat_ingestion_run`'s guarded `UPDATE ... WHERE status = 'RUNNING'` | Yes — live: 13 SUCCEEDED, 1 PENDING, 0 RUNNING, unchanged and clean | ✓ FLOWING (was ✗ HOLLOW-risk) |
| `meta.files.duplicate_of_file_id` | `find_file_by_content_hash` result | Deterministic `ORDER BY file_id ASC ... LIMIT 1` query | Yes — live: zero orphans across all 3 duplicate-content groups (8 duplicate rows total), independently re-verified with a wider check than the original gap required | ✓ FLOWING (was ✗ HOLLOW) |
| `Receipt` XCom payload (source code) | `run_ingest`'s return value / `_failure_receipt(doc)` | Same in-memory values as the DB write, now covering every exception class | Yes, for all paths, at the source-code level | ✓ FLOWING (was ⚠️ PARTIAL) |
| `Receipt` XCom payload (live deployed pod) | Same, but as executed by the currently-deployed image | Image `9b59385`, which predates the WR-01 fix | No, for the specific non-`DataPlatformError` path — the deployed pod still runs the pre-fix code | ⚠️ STATIC (deployment lag — see Warning above) |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| DAG policy/thinness/line-budget/structure tests pass | `uv run --frozen pytest tests/policy/test_dag_thinness.py tests/policy/test_dag_line_budget.py tests/unit/test_dag_structure.py -q` | `12 passed in 0.85s` | ✓ PASS |
| `csv_ingest_customers` DAG still registered and unpaused live | `kubectl exec -n airflow deploy/airflow-scheduler -- airflow dags list` | `is_paused: False` | ✓ PASS |
| Analytical data idempotency holds at scale (no drift) | Live SQL: `SELECT count(*), count(DISTINCT customer_id) FROM normalized.customers` | `10000122 = 10000122` | ✓ PASS |
| CR-01 regression tests pass against real testcontainers PostgreSQL 18 | `uv run --group cluster pytest tests/integration/test_metadata_repository.py tests/integration/test_run_ingest.py tests/integration/test_discover_files.py -q` | `30 passed in 16.42s` | ✓ PASS |
| WR-01 regression tests pass via the real CLI entry point | `uv run --frozen pytest tests/unit/test_csv_processor_cli.py tests/unit/test_cli_error_handling.py -v` | `10 passed in 0.32s` | ✓ PASS |
| CR-02: zero orphaned or wrong-pointer duplicate-content rows, entire live table, wider check than originally required | Live SQL: generic CTE over all `meta.files` content-hash groups, checking both `IS NULL` and `IS DISTINCT FROM <group min>` | `0 rows` for both checks, all 3 groups | ✓ PASS |
| WR-01 fix present in the live cluster's deployed image | `git merge-base --is-ancestor ee3d591 <deployed-tag>`; `git show <deployed-tag>:.../cli.py \| grep -c "except Exception:"` across all 5 registry tags | Not an ancestor of any deployed tag; `0` occurrences in every tag | ✗ FAIL — see Deployment-Currency Warning (non-blocking) |
| No regressions in the broader unit/mypy/ruff/policy surface | `pytest tests/unit -q` (138 passed), `mypy packages/dataplat/src packages/csv-processor/src` (no issues, 43 files), `ruff check` on all 9 touched files (all checks passed), `pytest tests/policy -q` (126 passed, 2 failed — the same pre-existing, documented-since-Phase-1 `test_gates_actually_fail.py` ANSI-color-drift failures, unrelated to this phase) | All clean except the known pre-existing 2 | ✓ PASS (no new regressions) |

### Probe Execution

No `scripts/*/tests/probe-*.sh` convention or explicit probe declarations found in this phase's PLAN/SUMMARY files (unchanged from the original verification). Step 7c: SKIPPED (no declared or conventional probes for this phase).

### Requirements Coverage

All 22 requirement IDs assigned to Phase 4 remain accounted for — zero orphaned requirements, no regressions. The two IDs explicitly claimed by 04-10/04-11 (`META-03`, `LOAD-04`) are re-confirmed below with updated evidence; the remaining 20 are regression-confirmed unchanged from the original verification (their supporting code was not touched by this gap-closure round — confirmed via the `git diff --stat` scope check above).

| Requirement | Source Plan(s) | Description | Status | Evidence |
|---|---|---|---|---|
| ORCH-01 | 04-07 | TaskFlow API (`@dag`, `@task`) | ✓ SATISFIED (regression-confirmed) | Unchanged; DAG policy tests re-ran clean |
| ORCH-02 | 04-07 | ETL work only in KPO pods | ✓ SATISFIED (regression-confirmed) | Unchanged |
| ORCH-03 | 04-03, 04-07 | Bounded Dynamic Task Mapping | ✓ SATISFIED (regression-confirmed) | Unchanged |
| ORCH-04 | 04-07 | Explicit retry/failure + backfill support | ✓ SATISFIED (regression-confirmed) | Unchanged |
| ORCH-05 | 04-07 | Logical-date/data-interval derived window, tolerates `None` | ✓ SATISFIED (regression-confirmed) | Unchanged |
| ORCH-06 | 04-07 | DAG <150 lines, no parsing/validation/typing/DB-writes | ✓ SATISFIED (regression-confirmed) | 149 lines; policy tests re-ran clean |
| ORCH-07 | 04-07 | Dataset dependency via Assets/sensors | ✓ SATISFIED (regression-confirmed) | Unchanged |
| ORCH-08 | 04-03, 04-06, 04-07 | Frozen-manifest expansion, never live listing mid-run | ✓ SATISFIED (regression-confirmed) | Unchanged; `test_discover_files.py`'s new 3-way duplicate test additionally re-exercises this without breaking it |
| ORCH-09 | 04-07 | CPU/memory requests+limits per task pod | ✓ SATISFIED (regression-confirmed) | Unchanged |
| META-03 | 04-01, 04-05, 04-06, 04-10 | Rows + watermark + run status commit in one publication transaction | ✓ SATISFIED — **strengthened** | The publish transaction itself (`run.py`'s `with ctx.db.connection() as conn, conn.transaction():`) is unchanged and still sound. CR-01's fix additionally closes the previously-noted *separate* post-commit heartbeat race that undermined META-03's auditability spirit (not its literal transactional claim) — that gap is now closed, live-cluster-confirmed (0 RUNNING rows, 13 clean SUCCEEDED) |
| LOAD-01 | 04-05, 04-08 | No dup/corrupt data on any rerun path | ✓ SATISFIED (regression-confirmed) | Live 10M-row idempotency evidence, re-confirmed fresh, unchanged |
| LOAD-02 | 04-05, 04-08 | Airflow retry mid-load creates no dup rows, proven by test | ✓ SATISFIED (regression-confirmed) | Unchanged |
| LOAD-03 | 04-03, 04-08 | Reprocessing identical file (by checksum) is a no-op | ✓ SATISFIED (regression-confirmed) | Unchanged |
| LOAD-04 | 04-01, 04-06, 04-10 | File/batch/record/target-row identity modeled distinctly | ✓ SATISFIED — **gap closed, no longer an edge-case exception** | Previously "SATISFIED (general case); see gap 3 for an edge-case exception." The edge case (file_id=10) is now closed and independently re-verified against live data with a wider check than the original gap required (see Truth #8) |
| LOAD-05 | 04-04, 04-05, 04-08 | Transactional staging → atomic publication | ✓ SATISFIED (regression-confirmed) | Unchanged |
| LOAD-08 | 04-01, 04-04, 04-05, 04-06 | Batch ledger `UNIQUE(dataset,batch_key)` + run-scoped identity | ✓ SATISFIED (regression-confirmed) | Unchanged |
| LOAD-09 | 04-01, 04-04, 04-05, 04-06 | Single-writer publication via advisory lock + `ON CONFLICT`, never `MERGE` | ✓ SATISFIED (regression-confirmed) | Unchanged |
| LOAD-12 | 04-04 | Processor is the only CSV parser; no `COPY...FORMAT csv` on raw input | ✓ SATISFIED (regression-confirmed) | Re-read `staging.py`: COPY still writes from Python-parsed `enriched_rows`, never a raw file. **Correction to the prior verification report:** it claimed `.planning/REQUIREMENTS.md`'s checkbox for LOAD-12 was `[x]` (inconsistent with the table's "Pending"). Checked `git log -p` for this file: the checkbox has **always** been `[ ]` (unchecked), consistently matching the table's "Pending" status — there is and was no inconsistency. This appears to have been a factual slip in the original report, not a real documentation-sync issue; noting the correction here rather than propagating it. The underlying code satisfaction is unaffected either way |
| INCR-08 | 04-03 | Business date derived from data, never wall-clock/`logical_date` | ✓ SATISFIED for this phase's scope (regression-confirmed) | Unchanged |
| QUAL-05 | 04-06 | Integration tests: MinIO → processor → PostgreSQL | ✓ SATISFIED (regression-confirmed) | All pre-existing integration test files still present and now pass alongside 3 new regression tests added by 04-10 |
| QUAL-06 | 04-08, 04-09 | E2E tests: CSV → MinIO → Airflow → Kubernetes → processor → PostgreSQL | ✓ SATISFIED (regression-confirmed) | Unchanged |
| QUAL-09 | 04-08 | Idempotency tested, including zero-additional-rows assertion | ✓ SATISFIED (regression-confirmed) | Unchanged; live 10M-row evidence re-confirmed fresh |

## Anti-Patterns Found

No debt markers (`TODO`/`FIXME`/`TBD`/`XXX`/`HACK`/`PLACEHOLDER`) in any of the 9 files touched by 04-10/04-11 (`grep -n -E "TBD|FIXME|XXX|TODO|HACK|PLACEHOLDER"` across all 9 — zero matches, confirmed fresh this session). `ruff check` on all 9 files: all checks passed, confirmed fresh.

| File | Line(s) | Pattern | Severity | Impact |
|------|---------|---------|----------|--------|
| `csv_processor_image` Airflow Variable (live deployment state) | — | Deployed image predates the WR-01 source fix (`ee3d591` not an ancestor of `9b59385`) | ⚠️ Warning (new finding, this re-verification pass — not previously reported by SUMMARY.md or 04-REVIEW.md) | If a live pod hits a non-`DataPlatformError` exception right now, the original WR-01 bug still manifests operationally. Non-fatal (Airflow still fails the task). Trivially closable: `make image-csv-processor` + confirm the Variable advances |
| `scripts/repair-duplicate-file-lineage.py:111-136` | `_DIAGNOSTIC_SQL`/`_REPAIR_SQL` | WR-07: only detects `duplicate_of_file_id IS NULL`, not an existing wrong non-NULL value | ⚠️ Warning (carried from `04-REVIEW.md`, independently re-confirmed) | Confirmed via direct code read; confirmed NOT currently exploitable via a live query using the wider `IS DISTINCT FROM` check across the entire `meta.files` table — 0 rows. Real gap in a one-off manual tool, not on the primary vertical-slice path |
| `scripts/repair-duplicate-file-lineage.py:84-100` | `_CONTENT_GROUPS_CTE` | WR-08: assumes every dataset uses `duplicate_policy: skip` | ⚠️ Warning (carried from `04-REVIEW.md`, independently re-confirmed) | Confirmed via direct code read (`configs/datasets/`, `meta.datasets`): exactly ONE dataset config exists (`customers.yaml`, `duplicate_policy: skip`) and exactly ONE dataset is registered live. Genuinely a "future dataset" landmine, not currently exploitable |
| `metadata/postgres.py::heartbeat_ingestion_run` + `pipeline/run.py` | 365-390 / 163-208 | IN-04: heartbeat write has no claim-owner fencing (narrower cousin of CR-01) | ℹ️ Info (carried from `04-REVIEW.md`, not independently re-verified this round — out of this re-verification's scope, matches original characterization) | Optional hardening, not required for CR-01's correctness |
| `scripts/repair-duplicate-file-lineage.py` (whole file) | — | IN-05: zero automated test coverage for the repair script itself | ℹ️ Info (carried from `04-REVIEW.md`) | `_find_orphans`/`_repair_orphans` were proven correct against a throwaway local Postgres per 04-10-SUMMARY.md (not CI-enforced); no `tests/integration/test_repair_duplicate_file_lineage.py` exists |
| `metadata/postgres.py` (`claim_ingestion_run`'s WHERE clause) + `pipeline/run.py` | — | WR-02 (carried, unchanged): no code path ever writes `status='FAILED'` | ⚠️ Warning (unchanged from original verification) | Independently re-confirmed present during this session's direct reads of both files; not this round's scope |
| `load/publish/merge.py` casts + `load/staging.py` | — | WR-04 (carried, unchanged): a single bad-value row aborts the whole file's publish | ⚠️ Warning (unchanged) | Not this round's scope; explicitly future-phase work per `04-REVIEW.md` |
| `config/model.py` `DatasetConfig.dataset` | — | WR-05/WR-06 (carried, unchanged): unvalidated identifier reaches SQL/filesystem paths | ⚠️ Warning (unchanged) | Not this round's scope |
| `tests/policy/test_gates_actually_fail.py` | — | 2 pre-existing, unrelated failures (import-linter ANSI-color output drift) | ℹ️ Info | Re-ran fresh this session: `126 passed, 2 failed` — same 2 known pre-existing failures, confirmed pre-existing from Phase 1, not a regression from this gap-closure round |

## Human Verification Required

None. All must-haves in this phase and this gap-closure round are backend/infrastructure claims (no UI). I independently verified the live cluster and live database directly (read-only `kubectl exec`/`psql`), which is stronger evidence than UI-based human testing could add. The one open item (deployment currency for WR-01) is a known, mechanical, single-command action — not something requiring human judgment or exploratory testing.

## Gaps Summary

**All three previously-FAILED truths are now genuinely closed**, independently re-verified at the code level (direct reads, not trusting SUMMARY.md), the test level (fresh `pytest` runs against real testcontainers PostgreSQL 18 and the real CLI entry point, not re-reading old CI output), and — for CR-01 and CR-02 — the live-cluster-data level (fresh `kubectl exec`/`psql` queries run in this session, not reused from any prior report or script output):

1. **WR-01 (Receipt on every exit path):** Closed in source. `except Exception:` now guarantees a Receipt is written for any exception class, proven by two genuine regression tests exercised through the real CLI entry point.
2. **CR-01 (heartbeat status regression):** Closed in source, tests, and live deployment. `heartbeat_ingestion_run`'s `WHERE status = 'RUNNING'` guard makes a stray post-terminal tick a provable no-op; live `meta.ingestion_runs` remains clean (0 RUNNING rows); this fix IS in the currently-deployed image.
3. **CR-02 (non-deterministic duplicate lookup):** Closed in source, tests, and live data. `ORDER BY file_id ASC` makes duplicate resolution deterministic; I independently re-queried the live cluster (not reusing the repair script's own report) and found the historically-orphaned `file_id=10,11,12` group now correctly resolved to `file_id=9`, AND ran a wider, fully generic check across every duplicate-content group in the live table (including one never examined in the original verification) — zero orphaned or wrong-pointer rows anywhere.

**One new finding surfaced by this re-verification pass, not previously reported:** the WR-01 source fix has not yet been built into the image the live cluster actually runs. 04-10 explicitly rebuilt and redeployed the image as part of its own Task 3 (and that rebuild does carry CR-01/CR-02's fixes, confirmed via git ancestry), but 04-11 never included this step, and — since 04-10/04-11 ran as parallel git worktrees off a shared base commit — 04-10's rebuild could not have contained 04-11's not-yet-existing commit either. This is classified as a **WARNING, not a blocker**: the must-have as declared is a source-code behavior claim (now true and tested), Airflow still correctly fails the task on any exception regardless of Receipt-writing, and the fix is closable with a single existing command (`make image-csv-processor`). Recommend running it before the next real production `ingest` pod invocation, so the fix is actually live, not merely merged.

Both new Warnings surfaced by the fresh `04-REVIEW.md` code-review pass (WR-07, WR-08, both in the new `scripts/repair-duplicate-file-lineage.py`) were independently re-verified against live data in this session and confirmed accurate but **not currently exploitable** — zero live rows are affected by either gap today, and both require a future multi-dataset scenario this phase does not yet have.

**All 5 ROADMAP Success Criteria and all 22 requirement IDs show no regression** — re-confirmed via fresh test runs and fresh live-cluster queries in this session, not carried forward from the original report unchecked. One factual correction to the original verification report is noted (LOAD-12's REQUIREMENTS.md checkbox was never actually inconsistent with its table entry — both have always read "Pending"/unchecked).

**Recommendation:** Phase 4's gap-closure round is complete and the vertical slice's core guarantees hold, live, in the running cluster, for CR-01 and CR-02. The single remaining action item — rebuilding and redeploying the `csv-processor` image so WR-01's fix is actually live — is small, mechanical, and does not block proceeding, but should be done promptly.

---

_Verified: 2026-08-14T07:15:00Z_
_Verifier: Claude (gsd-verifier)_
