---
phase: 08-validation-quarantine-metadata-control-plane-completion
verified: 2026-08-17T20:10:00Z
status: human_needed
score: 4/5 roadmap success criteria fully VERIFIED, 1/5 (VALID-08 re-drive audit trail) still genuinely unproven live; the 1 regression this verification found (ORCH-06 DAG line-budget policy test) was fixed post-verification, commit a78680e
overrides_applied: 0
post_verification_fix:
  regression: "tests/policy/test_dag_line_budget.py::test_csv_ingest_customers_stays_under_150_lines"
  commit: a78680e
  detail: "csv_ingest_customers.py trimmed 173 -> 149 lines (docstring/comment condensation only, no functional change). Re-confirmed green: pytest tests/unit tests/property tests/policy -q -m \"not integration and not dagtest and not cluster and not manifests\" -> 614 passed, 12 deselected (was 613 passed, 1 failed). Full make test (484) and make policy (124) also re-run clean."
human_verification:
  - item: "Run tests/e2e/slice/test_backfill_reentry.py -m cluster to a genuine completion (pass or fail, not another environmental timeout)"
    expected: "Either the test passes cleanly (proving resolve_rejected_records_for_batch's batch_id scoping correctly resolves a content-differing correction's PENDING row, closing VALID-08's audit-trail proof), or it fails specifically at _assert_row_resolved (proving the batch_key/content_sha256 architecture concern is real, needing a Rule-4-territory design decision per deferred-items.md -- not a quick fix)."
    why_human: "Requires a live cluster window free of the already-known, already-deferred CPU-contention issue (kind cluster node CPU budget, physical host ceiling, tracked in STATE.md's Blockers) -- 3 attempts this session all failed before ever reaching this code path for that unrelated reason."
re_verification:
  previous_status: human_needed
  previous_score: "4/5 fully verified, 1/5 uncertain"
  gaps_closed:
    - "VALID-07 (referential-orphan quarantine + non-orphan publish) confirmed live end-to-end: 08-HUMAN-UAT.md documents one full clean pass of test_orphan_order_quarantined_while_valid_rows_publish (discover -> ingest -> SUCCEEDED -> orphan quarantine verified) against the real cluster; this verifier independently confirms the underlying code/integration tests are unchanged since that pass. Promoted from ORPHANED/ERROR to VERIFIED."
    - "The deployment gaps blocking both e2e tests (migrations 0014-0017 not applied, csv_ingest_orders absent from DAG bundle, Vault sealed) are closed: this verifier independently re-queried the live cluster and confirms analytics-db is now at migration 0019, both csv_ingest_customers and csv_ingest_orders are listed by `airflow dags list`, and `vault status` reports Sealed: false."
    - "The test-robustness gap in test_backfill_reentry.py (single CLI invocation, no retry for Airflow's own documented-transient backfill_dag_run.exception_reason='in flight' row-lock race) is closed at the code level: plan 08-15 (commit 1de6a22) plus a follow-up code-review addendum (commit cb56e15, 5 findings all resolved) added a bounded 3-attempt retry with 5s backoff and self-diagnosing failure messages. This verifier independently re-ran ruff/mypy/pytest --collect-only against the current code and confirms all pass cleanly."
  gaps_remaining:
    - "VALID-08's core open question -- does resolve_rejected_records_for_batch (D-05) genuinely flip a content-differing correction's PENDING meta.rejected_records row to REDRIVEN, given batch_key is a pure function of content_sha256 -- remains completely untested. This verifier independently queried the live analytics-db: meta.rejected_records currently has 5 PENDING rows and ZERO REDRIVEN rows, and the airflow-db's backfill/backfill_dag_run tables show only the 2 manual CLI reproductions from the debug session (backfill_id=1 'in flight', backfill_id=2 dag_run_id=2781 success) -- no genuine pytest -m cluster run of test_backfill_reentry.py has ever completed on this cluster. This is the single most important remaining truth for this phase's goal and it has never been observed, neither passing nor failing."
  regressions:
    - "tests/policy/test_dag_line_budget.py::test_csv_ingest_customers_stays_under_150_lines now FAILS (173 lines, budget <150) -- NEW finding, not present in the previous verification run (which independently ran the identical command and got a clean 613/613 pass). Introduced by quick task 260817-mvp (commit ea5a38e, 2026-08-17, after the previous verification), which added a `.override(max_active_tis_per_dag=3)` fix plus a ~19-line explanatory comment to csv_ingest_customers.py -- a required phase-08 artifact -- without running `tests/policy` (its own SUMMARY.md only claims tests/unit's 484 tests were checked). This breaks `make policy` / `make check`'s Local gate."
gaps: []
# The one gap this verification found (csv_ingest_customers.py over the ORCH-06
# 150-line budget) was fixed immediately after this report was written --
# see post_verification_fix above. Original gap text preserved in `git log -p`
# for this file's prior revision, for audit trail.
---

# Phase 8: Validation, Quarantine & Metadata Control-Plane Completion Verification Report

**Phase Goal:** No data is ever silently dropped — every rejected record is retained with a reason, reportable, and has a documented path back into the pipeline
**Verified:** 2026-08-17T20:10:00Z
**Status:** human_needed
**Re-verification:** Yes — after gap closure (plan 08-15 + live-cluster UAT session). The single regression this pass found (DAG line-budget) was fixed immediately after (commit `a78680e`) — see `post_verification_fix` in frontmatter.

**Mode note (carried forward, unchanged):** ROADMAP.md marks this phase `mode: mvp`, but the phase goal text does not match the User Story shape (`gsd-sdk query user-story.validate` returns `valid: false`). The phase carries five detailed, testable roadmap Success Criteria, which this report verifies against as the richer contract. Flagged as a process inconsistency, not a verification blocker.

## Summary of This Verification's Independent Findings

This is a re-verification following a live-cluster UAT session (08-HUMAN-UAT.md, deferred-items.md) and a targeted gap-closure plan (08-15 + its code-review addendum). Rather than trust those artifacts' own narration, this verifier re-derived the current state independently:

- **Re-ran** ruff, mypy, `pytest --collect-only`, the full unit/property/policy suite, the integration suite, and the dagtest suite from scratch.
- **Queried the live kind cluster directly** (it was reachable this session, unlike the prior verification's attempt): `analytics-db`'s `alembic_version`, `airflow dags list`, `vault status`, node CPU/memory allocation, and — going further than the prior verification — the live `backfill`/`backfill_dag_run` and `meta.rejected_records` tables to see what has actually happened on this cluster, not just whether the tooling can reach it.
- **Found one new regression** the previous verification and the 08-15 gap-closure session both missed: `tests/policy/test_dag_line_budget.py` currently fails. This was not caused by phase 08's own plans; it was introduced by a post-phase quick task (`260817-mvp`, commit `ea5a38e`) that modified a phase-08 required artifact without running the full policy suite.
- **Confirmed** the prior verification's `human_verification` item 1 (deployment gaps) is now closed, and VALID-07's live proof is genuine (one full clean e2e pass, documented in 08-HUMAN-UAT.md, corroborated by unchanged passing integration tests).
- **Did not attempt** to run the live `-m cluster` e2e tests myself. Live node CPU allocation is currently 100%/78% across the two worker nodes (independently confirmed this session), matching the exact resource-pressure signature that caused all 3 prior live attempts of `test_backfill_reentry.py` to fail before ever reaching the code this phase's fix touches. Per this verifier's own constraints ("keep verification fast, don't run the app") and the risk of disrupting a shared long-running demo cluster with a multi-minute test that has already failed 3/3 times for environmental reasons, I judged this out of scope for this verification pass. The live DB query below (zero `REDRIVEN` rows, only 2 manual CLI reproductions in `backfill`/`backfill_dag_run`) is offered as the strongest available substitute evidence: it proves definitively that no successful end-to-end proof of VALID-08's re-drive path exists anywhere on this cluster today, closing the ambiguity about whether "maybe it silently already passed somewhere."

## Goal Achievement

### Observable Truths (Roadmap Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | A file with malformed rows loads the good rows and writes quarantine records naming source file, row number, column, error type, run and timestamp — nothing silently discarded | VERIFIED | Unchanged since prior verification. `RaggedRowGuard`/`CompletenessRule`/`PatternRule`/`ValidityRangeRule`/`UniquenessRule` each convert a row-level violation into a `RejectedRecord` (never raise, QUAL-03). `meta.rejected_records` (migration `0015`, live-confirmed applied — `alembic_version=0019`) carries `run_id`, `file_id`, `source_row_number`, `error_column`, `error_type`. This verifier re-ran the full 116-test integration suite: all pass. |
| 2 | A validation report exists as PostgreSQL rows and a MinIO artifact; a threshold-breaching dataset reports FAIL/QUARANTINE, an under-threshold one reports PASS_WITH_WARNING | VERIFIED | Unchanged since prior verification; code and tests re-confirmed passing this session (`test_report_artifact_matches_persisted_postgres_rows`, part of the 116 passing integration tests). |
| 3 | Corrected quarantined records re-enter the pipeline through the documented re-drive path and land in the warehouse | UNCERTAIN (see human_verification) | The "land in the warehouse" half remains proven (`ON CONFLICT DO UPDATE` publishers, batch-independent). The "documented re-drive path" half: the test-robustness fix (retry on Airflow's own `exception_reason='in flight'` transient) is code-complete, twice-reviewed, and statically clean — this verifier independently re-confirmed `ruff check`/`mypy`/`pytest --collect-only` all pass on the current `tests/e2e/slice/test_backfill_reentry.py`. But the actual behavior this truth depends on — whether `resolve_rejected_records_for_batch`'s `batch_id` scoping correctly (or incorrectly) resolves a content-differing correction's PENDING row — has NEVER been observed on this cluster. Live query this session: `meta.rejected_records` has 5 `PENDING` rows, 0 `REDRIVEN`; `backfill`/`backfill_dag_run` show only the 2 manual CLI reproductions from the debug session, not a single completed run of the actual pytest test. Genuinely unresolved, not merely "assumed fine." |
| 4 | A truncated/still-uploading file (checksum mismatch, size mismatch, wrong extension, empty, missing `_BATCH_COMPLETE`) is refused before any parsing occurs | VERIFIED | Unchanged since prior verification. `integrity_gate.py` wired upstream of `discover` in both DAGs; `tests/unit/test_integrity_gate.py` and `test_batch_complete_marker.py` re-confirmed passing this session. |
| 5 | A file at 10× historical baseline row count is flagged as a volume anomaly; an orphan foreign key produces the dataset's configured fail/quarantine/warn outcome | VERIFIED | The volume-anomaly half is unchanged/re-confirmed via passing integration tests. The referential-orphan half is now upgraded from "integration-tested only" to **live-proven**: 08-HUMAN-UAT.md documents `test_orphan_order_quarantined_while_valid_rows_publish` achieving one full clean server-side pass (discover → ingest → SUCCEEDED → `REFERENTIAL_ORPHAN` reject row confirmed → non-orphan rows published, orphan row absent from `normalized.orders`) against the real cluster, after fixing 4 real deployment gaps (psycopg dependency, 2 missing GRANTs, stale hostPath mount) along the way. This verifier independently confirms the underlying `ReferentialIntegrityBarrier` code and its integration tests are unchanged since that live pass, and the live cluster is currently deployed with the same code (migration `0019`, both DAGs present). |

**Score:** 4/5 fully VERIFIED (one upgraded from ORPHANED to VERIFIED this session), 1/5 genuinely UNCERTAIN — not because of missing effort, but because the specific behavior it depends on has never actually run to completion, pass or fail, anywhere.

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `migrations/versions/0014_meta_validation_results.py` … `0017_normalized_orders_business_key_unique.py` | `meta.validation_results`/`meta.rejected_records`/`normalized.orders` DDL | VERIFIED | Live-confirmed applied: `SELECT version_num FROM meta.alembic_version` → `0019` (this session; was `0013` at the prior verification). |
| `packages/dataplat/src/dataplat/validate/{completeness,pattern,validity_range,uniqueness,circuit_breaker,referential,volume_anomaly}.py` | VALID-02/03/07/09 rule classes | VERIFIED | Unchanged, all registered, unit/property/integration tests re-confirmed passing this session. |
| `packages/dataplat/src/dataplat/validate/strategy_dispatch.py` | D-07 5-strategy outcome wrapper | VERIFIED | Unchanged, re-confirmed. |
| `packages/dataplat/src/dataplat/metadata/postgres.py` | `record_validation_results`/`record_rejected_records`/`resolve_rejected_records_for_batch` | VERIFIED | Unchanged; CR-01 ordering fix still present and covered by regression test. |
| `airflow/dags/_common/integrity_gate.py` | LOAD-10 pre-pod-launch gate | VERIFIED | Wired upstream of `discover` in both DAGs; live-confirmed deployed (both DAGs listed by `airflow dags list`). |
| `airflow/dags/csv_ingest_orders.py` | Second live dataset DAG | VERIFIED | 159 lines, deployed and listed live (`csv_ingest_orders` now present, was absent at prior verification). No line-budget test covers this file, so its growth (145→159 since prior verification) is not itself a policy violation. |
| `airflow/dags/csv_ingest_customers.py` | Phase 8's LOAD-10 gate wiring, first live dataset DAG | VERIFIED | Functionally correct and deployed (integrity_gate wiring, concurrency cap all present and live-confirmed working per 08-HUMAN-UAT.md); was 173 lines against ORCH-06's <150-line budget at verification time, trimmed to 149 lines immediately after (commit `a78680e`, docstring/comment condensation only) — `tests/policy/test_dag_line_budget.py` now passes. |
| `packages/dataplat/src/dataplat/load/publish/merge_orders.py` | `OrdersMergePublisher` | VERIFIED | Unchanged, WR-04 NULL-safety fix still present. |
| `configs/datasets/orders.yaml`, `configs/datasets/customers.yaml` | Real `quality:` blocks | VERIFIED | Unchanged, deployed (live e2e pass for VALID-07 exercised the deployed configs). |
| `tests/dagtest/test_backfill_dagrun.py` | Backfill DagRun mechanics proof | VERIFIED | Unchanged, 2/2 passing this session. |
| `tests/e2e/slice/test_referential_orphan.py` | VALID-07's live closing proof | VERIFIED | Ruff/mypy/collect-only clean this session; live-proven with one full clean pass per 08-HUMAN-UAT.md; not re-run live this session (see Summary above for why). |
| `tests/e2e/slice/test_backfill_reentry.py` | VALID-08's live closing proof | CODE VERIFIED, LIVE-UNPROVEN | Ruff/mypy/collect-only clean this session (independently re-confirmed); retry logic reviewed twice (08-REVIEW.md + Addendum, 5/5 findings fixed, commit `cb56e15`); has never completed a full `-m cluster` run — 3 attempts across sessions all failed before reaching this file's own code, for a separate, known, deliberately-deferred infra reason (node CPU budget). |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `wait_for_files` (S3KeySensor) | `discover` (KPO) | `matched_keys >> gate >> discover` | WIRED | Unchanged, re-confirmed via `test_integrity_gate_upstream_of_discover`. |
| `pipeline/run.py` barrier stages | `meta.validation_results`/`meta.rejected_records` | `ctx.metadata.record_*` inside the publish transaction | WIRED | Unchanged, re-confirmed. |
| `pipeline/run.py` | MinIO `validated` bucket | `ctx.objects.put_object` | WIRED | Unchanged, re-confirmed. |
| `StagingLoader._build_stages` | `ctx.config.quality` rule declarations | `resolve_validation_rule` + `StrategyDispatchStage` | WIRED | Unchanged, re-confirmed. |
| `ReferentialIntegrityBarrier` | `normalized.orders` publish exclusion + `meta.rejected_records` | Anti-join delete from staging before publish | WIRED, live-proven | 08-HUMAN-UAT.md's clean pass; unchanged code since. |
| Airflow `airflow backfill create` | `meta.rejected_records.resolution_type` flip | `resolve_rejected_records_for_batch` scoped by `batch_id` | UNCERTAIN for content-differing corrections | Live query this session: 0 `REDRIVEN` rows exist on this cluster; this link has never fired for real content-differing correction data, only for the debug session's own manual same-content re-invocation (which never reached `resolve_rejected_records_for_batch` either — it only proved the Airflow-level `DAG.clear()` mechanism itself works). |

### Data-Flow Trace (Level 4) — the VALID-08 chain specifically

Traced independently this session, live, on the running cluster:

1. `meta.rejected_records` (live query): 5 rows, all `resolution_type='PENDING'`, 0 `resolution_type='REDRIVEN'`.
2. `backfill`/`backfill_dag_run` (live query, airflow-db): 2 rows total, both from the debug session's manual `airflow backfill create` CLI reproduction (`backfill_id=1`, `exception_reason='in flight'`; `backfill_id=2`, `dag_run_id=2781`, no exception — i.e. a genuine re-execution of the DAG happened once) — neither originates from an actual `pytest -m cluster` run of `test_backfill_reentry.py`, and neither is a content-differing-correction scenario (the debug session reused the identical, unmodified file).
3. **Conclusion:** the data-flow chain this truth depends on (`corrected file discovers → new batch_id → resolve_rejected_records_for_batch(new batch_id) → does it or doesn't it touch the OLD batch's PENDING row`) has literally never executed on this cluster. This is DISCONNECTED in the sense of "never observed," not "observed and broken" — an important distinction from the original code-level concern raised in the first verification pass, which speculated this would fail; the debug session's own investigation shows the earlier failure (300s timeout) was NOT this concern at all, but an unrelated Airflow row-lock race that has since been fixed at the test level.

### Requirements Coverage

| Requirement | Source Plan(s) | Description | Status | Evidence |
|-------------|----------------|--------------|--------|----------|
| VALID-01 | 08-01, 08-04, 08-10 | Structural validation | SATISFIED | Unchanged. REQUIREMENTS.md still shows `[ ]`/"Pending" — stale doc, confirmed again this session. |
| VALID-02 | 08-01, 08-04, 08-07, 08-10, 08-11 | Completeness/uniqueness/validity-range/pattern/referential | SATISFIED | Unchanged. |
| VALID-03 | 08-01, 08-04, 08-07, 08-10 | Quarantine per configurable strategy | SATISFIED | Unchanged. |
| VALID-04 | 08-01, 08-03, 08-11 | Machine-readable reports in Postgres + MinIO | SATISFIED | Unchanged. REQUIREMENTS.md stale, confirmed again. |
| VALID-07 | 08-05, 08-08, 08-12, 08-14 | Referential integrity, configurable fail/quarantine/warn | **SATISFIED, now live-proven** | Upgraded this session from "blocked on deployment" to fully satisfied — see Truth #5. |
| VALID-08 | 08-03, 08-12, 08-13, 08-14, 08-15 | Documented re-drive path after correction | UNCERTAIN | Test-robustness gap closed at code level (plan 08-15); the underlying behavior remains unproven live — see Truth #3. |
| VALID-09 | 08-01, 08-09, 08-11 | Volume/quality anomalies against persisted baselines, no ML | SATISFIED | Unchanged. |
| LOAD-10 | 08-02, 08-12 | File integrity verified before processing | SATISFIED | Unchanged. REQUIREMENTS.md stale, confirmed again. |
| LOAD-11 | 08-01, 08-06 | Optional `_BATCH_COMPLETE` manifest support | SATISFIED | Unchanged. REQUIREMENTS.md stale, confirmed again. |

**Note on REQUIREMENTS.md staleness (unchanged from prior verification):** 5 of 9 phase-08 requirement IDs (VALID-01, VALID-04, VALID-08, LOAD-10, LOAD-11) are still shown `[ ]`/"Pending" in `.planning/REQUIREMENTS.md` despite substantive, tested implementations (VALID-08 excepted per the caveat above). Should be corrected, but is a documentation lag, not a code gap.

**Orphaned requirements:** None. Unchanged from prior verification.

### Anti-Patterns Found

No `TBD`/`FIXME`/`XXX`/`TODO`/`HACK`/`PLACEHOLDER` markers found in this phase's key files. No stub implementations found in the validation rule classes or barrier stages.

**Found this session, fixed immediately after:** `airflow/dags/csv_ingest_customers.py` had grown from 149 to 173 lines (quick task `260817-mvp`, commit `ea5a38e`, applied 2026-08-17 after the prior verification), breaking `tests/policy/test_dag_line_budget.py::test_csv_ingest_customers_stays_under_150_lines`. The change itself (a `max_active_tis_per_dag=3` concurrency cap plus a long rationale comment) is functionally correct and live-verified working — it fixed a genuine cluster-wide CPU-starvation bug — but the quick task's own SUMMARY only ran `tests/unit` (484 tests), never `tests/policy`, so this regression went uncaught until this verification pass. Trimmed back to 149 lines immediately after this report was drafted (commit `a78680e`); `make policy`/`make check` are green again.

Carried forward from 08-REVIEW.md (unchanged): CR-01 and WR-04 fixed and re-confirmed present; WR-01/WR-02/WR-03 remain deliberately deferred, documented, non-blocking (metric-accuracy, dormant-strategy-gap, dormant-type-cast — none causes silent data loss today).

### Behavioral Spot-Checks / Test Suite Execution

Independently re-run this session (not trusting any prior SUMMARY/VERIFICATION claim):

| Suite | Command | Result | Status |
|-------|---------|--------|--------|
| unit + property + policy | `pytest tests/unit tests/property tests/policy -q -m "not integration and not dagtest and not cluster and not manifests"` | 613 passed, 1 failed, 12 deselected at verification time; re-run after the line-budget fix (commit `a78680e`): **614 passed, 12 deselected** | PASS (post-fix) |
| ruff (targeted, backfill test) | `ruff check tests/e2e/slice/test_backfill_reentry.py tests/e2e/slice/test_referential_orphan.py` | All checks passed! | PASS |
| mypy (in-scope) | `mypy packages/dataplat/src packages/csv-processor/src` | Success: no issues found in 72 source files | PASS |
| pytest --collect-only (e2e slice) | `pytest tests/e2e/slice/test_backfill_reentry.py tests/e2e/slice/test_referential_orphan.py --collect-only -q` | 2 tests collected | PASS |
| integration (testcontainers Postgres/MinIO) | `pytest tests/integration -q` | 116 passed | PASS |
| dagtest (testcontainers Airflow metadata DB) | `pytest tests/dagtest -q` | 2 passed | PASS |
| e2e cluster (live, this session) | Not run — see Summary above for reasoning | N/A | SKIPPED (deliberate — see rationale above) |

### Live-Cluster State (independently confirmed by this verifier, this session)

```
$ kubectl -n data exec analytics-db-1 -- psql -U postgres -d analytics -c "SELECT version_num FROM meta.alembic_version"
 version_num
-------------
 0019
```
Fully current (was `0013`, 4 behind, at the prior verification).

```
$ kubectl -n airflow exec deploy/airflow-api-server -- airflow dags list
csv_ingest_customers | ...
csv_ingest_orders    | ...
smoke_kubernetes_pod | ...
```
Both ingestion DAGs deployed (was missing `csv_ingest_orders` at the prior verification).

```
$ kubectl -n vault exec vault-0 -- vault status
Sealed    false
```
Unsealed (was sealed at the prior verification).

```
$ kubectl -n data exec airflow-db-1 -- psql -U postgres -d airflow -c "SELECT b.id, b.dag_id, b.reprocess_behavior, bdr.logical_date, bdr.dag_run_id, bdr.exception_reason FROM backfill b JOIN backfill_dag_run bdr ON bdr.backfill_id = b.id WHERE b.dag_id='csv_ingest_customers' ORDER BY b.id DESC;"
 id | dag_run_id | exception_reason
----+------------+------------------
  2 |       2781 | (null)
  1 |            | in flight
(2 rows)

$ kubectl -n data exec analytics-db-1 -- psql -U postgres -d analytics -c "SELECT resolution_type, count(*) FROM meta.rejected_records GROUP BY resolution_type;"
 resolution_type | count
------------------+-------
 PENDING          |     5
(1 row)
```
Confirms: only 2 backfill invocations have ever occurred on this cluster (both manual debug-session reproductions, not from the actual test suite), and zero `meta.rejected_records` rows have ever been marked `REDRIVEN`. VALID-08's re-drive path has never been observed to succeed OR fail end-to-end on this cluster.

```
$ kubectl describe nodes | grep -A5 "Allocated resources"
airflow-platform-worker:   cpu 3 (100%)
airflow-platform-worker2:  cpu 2360m (78%)
```
Node CPU allocation remains tight, consistent with STATE.md's own already-documented, deliberately-deferred blocker (kind cluster node CPU budget — physical host ceiling, needs cluster recreation to resolve).

### Human Verification Required

1. **Once the deliberately-deferred kind/cluster.yaml node-CPU-budget decision is actioned (or the cluster is otherwise free of contention for a sustained window), run `pytest tests/e2e/slice/test_backfill_reentry.py -x -m cluster` to a genuine completion** (pass or fail, not another environmental timeout).
   **Expected:** Either the test passes cleanly (proving `resolve_rejected_records_for_batch`'s `batch_id` scoping correctly resolves a content-differing correction's PENDING row, closing VALID-08's audit-trail proof), or it fails specifically at `_assert_row_resolved` (proving the batch_key/content_sha256 architecture concern is real, which would then need a Rule-4-territory design decision per deferred-items.md — NOT a quick fix).
   **Why human:** Requires a live cluster window free of the already-known, already-deferred CPU-contention issue; this verifier deliberately did not attempt this run itself (see Summary above).

### Gaps Summary

**One new, real regression was found this session and fixed immediately after:** `tests/policy/test_dag_line_budget.py::test_csv_ingest_customers_stays_under_150_lines` failed (173 vs. <150 lines), breaking `make policy`/`make check`. Introduced by a post-phase quick task that touched a phase-08 required artifact without running the full policy suite. It never threatened the phase's core data-integrity goal (the underlying concurrency-cap fix was always functionally correct and live-verified working) — it was a project-convention/architecture-hygiene violation (ORCH-06: DAG files stay thin). Fixed by trimming the added comment block (commit `a78680e`, docstring/comment condensation only, no functional change); `make policy` (124 tests) and `make test` (484 tests) both re-confirmed clean.

**Real progress since the prior verification:** the deployment gaps that previously blocked live verification are now closed (migrations current, both DAGs deployed, Vault unsealed), and VALID-07 is now genuinely live-proven (one full clean e2e pass). The test-robustness fix for VALID-08's e2e proof (plan 08-15 + review addendum) is code-complete and doubly reviewed.

**What remains genuinely open:** VALID-08's specific re-drive audit-trail behavior for a content-differing correction has never executed on this cluster — not proven to work, not proven to fail. This is an honest "unknown," backed by direct live-database evidence (0 REDRIVEN rows, 0 completed test runs), not a documentation gap or a pessimistic guess. It requires a live cluster window unblocked by the separately-tracked node-CPU-budget issue to resolve either way. Tracked as the sole `human_verification` item in frontmatter.

**Recommendation:** Phase 8 is otherwise complete — all formal gaps closed, all code-level work done, 4/5 success criteria fully verified, the 5th verified in every respect except a live run that infra contention has prevented 3 times running. Mark the phase complete with the outstanding live-cluster confirmation tracked as a standing human-verification item (gated on the cluster-capacity decision already recorded in STATE.md's Blockers section), not a phase-blocking gap.

---

_Verified: 2026-08-17T20:10:00Z_
_Verifier: Claude (gsd-verifier)_
