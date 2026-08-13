---
phase: 04-vertical-slice-csv-to-analytical-postgresql
verified: 2026-08-14T00:00:00Z
status: gaps_found
score: 5/8 truths verified (5 ROADMAP success criteria hold at their literal wording; 3 precise sub-findings, drawn from plan must-haves and independently confirmed in code and/or live cluster data, are FAILED)
mode_note: "ROADMAP.md sets mode: mvp for this phase, but the phase goal text does not conform to User Story format (gsd-sdk query user-story.validate returned valid:false — 'Must begin with As a ', 'Must contain , I want to ', etc.). This is true for ALL 11 phases in this ROADMAP, not a phase-4-specific authoring slip, so MVP-mode narrowing (User Flow Coverage table) was not applied per the guard in verify-mvp-mode.md. Standard goal-backward verification against the ROADMAP's 5 explicit Success Criteria plus PLAN frontmatter must_haves was used instead — this is a strictly more thorough check than MVP narrowing would have been. Recommend either clearing mode: mvp for infra/platform phases in this ROADMAP or rephrasing goals as User Stories if MVP mode is intentionally desired going forward."
gaps:
  - truth: "The receipt is written to /airflow/xcom/return.json on every exit path -- success, claim-skip, and run-fatal failure (04-05-PLAN.md must-have; underpins ROADMAP SC1's XCom-receipt claim)"
    status: failed
    reason: "csv_processor.cli.ingest()'s try/except only catches `except DataPlatformError:` before writing the failure Receipt via _write_xcom. Any other exception class (e.g. a raw psycopg.errors.DataError from a publish-time cast failure -- see WR-04 in 04-REVIEW.md -- or a network error, MemoryError, etc.) propagates past this handler with NO receipt ever written. Independently confirmed by direct read of packages/csv-processor/src/csv_processor/cli.py: the only except clause before `finally:` is `except DataPlatformError:` (line 247); no generic `except Exception:` exists. Airflow still detects the pod's non-zero exit and fails the task regardless, so this is not silent data corruption -- but it is a literal, confirmed violation of the plan's own stated contract."
    artifacts:
      - path: "packages/csv-processor/src/csv_processor/cli.py"
        issue: "ingest()'s docstring (line ~199-201) states a Receipt is written 'on every exit path, success or failure' but the except clause only covers DataPlatformError (line 247)"
    missing:
      - "A second except clause (or a broadened one) that writes the failure Receipt for any exception, not only DataPlatformError, before re-raising"
  - truth: "A run's terminal status (SUCCEEDED) can never regress back to RUNNING after the publish transaction has committed"
    status: failed
    reason: "packages/dataplat/src/dataplat/pipeline/run.py's _heartbeat_loop (lines 190-197) calls metadata.update_ingestion_run_status(status='RUNNING', ...) unconditionally on every interval tick. stop_heartbeat.set() is only called in run_ingest's finally block (line 330), which runs AFTER the publish transaction commits status='SUCCEEDED' (~line 322) and AFTER the trailing DROP TABLE (line 328). postgres.py's update_ingestion_run_status (lines 401-429) builds a raw UPDATE with NO WHERE status = 'RUNNING' guard. If the heartbeat's interval elapses inside that commit-to-stop-signal window, it silently overwrites the just-committed SUCCEEDED status back to RUNNING with a fresh 5-minute lease -- confirmed by direct, independent reading of both files (not merely trusting 04-REVIEW.md's CR-01 finding, though it matches). csv_ingest_customers.py's ingest task sets DATAPLAT_HEARTBEAT_INTERVAL_SECONDS=2, which materially increases the odds of landing in this window. Impact: claim_ingestion_run's reclaim predicate (status='RUNNING' AND lease_expires_at < now()) can later re-claim and fully re-execute an already-succeeded run, and finalize_publication's second write overwrites rows_loaded/duration_ms with the second run's (likely near-zero, since MergePublisher's _record_hash guard suppresses the no-op republish) numbers -- corrupting the audit trail for a run that genuinely succeeded. Live meta.ingestion_runs snapshot at verification time showed 0 RUNNING rows and 13 clean SUCCEEDED rows, so this has NOT yet visibly manifested in the current cluster session, but the race window is real and code-confirmed, not theoretical."
    artifacts:
      - path: "packages/dataplat/src/dataplat/pipeline/run.py"
        issue: "stop_heartbeat.set() (line 330) fires only after commit + DROP TABLE, not before/around the commit"
      - path: "packages/dataplat/src/dataplat/metadata/postgres.py"
        issue: "update_ingestion_run_status (lines 401-429) has no WHERE status = 'RUNNING' guard, so a stray post-terminal heartbeat write silently regresses status"
    missing:
      - "A dedicated heartbeat-only repository method (e.g. heartbeat_ingestion_run) whose UPDATE carries WHERE run_id = %s AND status = 'RUNNING', making a stray post-terminal write a no-op instead of a regression"
  - truth: "meta.batches, meta.ingestion_runs and every loaded row answer 'which file, which batch, which run...' by SQL alone (ROADMAP SC4), specifically: content-duplicate detection is deterministic and never leaves a file's lineage unexplainable"
    status: failed
    reason: "packages/dataplat/src/dataplat/metadata/postgres.py's find_file_by_content_hash (lines 162-178) is `SELECT file_id FROM meta.files WHERE dataset_id = %s AND content_sha256 = %s LIMIT 1` with NO ORDER BY -- PostgreSQL's documented behavior is that which row LIMIT 1 returns is unspecified once more than one row matches. discovery.py's own 'rediscovery correction' logic (lines 196-217) depends on this call returning the SAME row on repeated calls for the same object; that assumption breaks once 2+ files share identical content (an explicitly designed-for, tested D-13 scenario). THIS IS NOT JUST A CODE-REVIEW HYPOTHESIS -- I independently confirmed it has already manifested live in the running cluster: `meta.files` currently has 3 groups of rows sharing identical content_sha256 (counts 2, 4 and 5). For the content group with 5 files (file_ids 4,5,6,7,8), all 4 duplicates correctly resolve to the true original (file_id=4) -- fine. But for the content group with 3 files (file_ids 9,10,11,12 -- content hash f90142cf...), file_id=10 is live, right now, in a broken, SQL-unexplainable state: `duplicate_of_file_id` is NULL (not pointing at the true original, file_id=9, which is PROCESSED/SUCCEEDED), file status is stuck at DISCOVERED (never PROCESSED), AND `SELECT count(*) FROM meta.ingestion_runs WHERE file_id = 10` returns 0 -- no ingestion_run row references it at all, even though it IS linked to a batch (batch_id=5, PUBLISHED). The most consistent explanation, traced through discover_files/get_or_create_ingestion_run: a later discovery pass's find_file_by_content_hash returned file 10's OWN row instead of file 9's, tripping the rediscovery-correction branch and wrongly clearing its duplicate_of_file_id; discover_files then treated it as 'new' and called get_or_create_ingestion_run, whose idempotency_key is a pure function of content_sha256 -- identical to file 9's already-SUCCEEDED run -- so the ON CONFLICT(idempotency_key) branch fired and returned file 9's existing SUCCEEDED run without ever creating or linking a row to file_id=10. Net effect: file_id=10 cannot currently be explained by SQL alone (a human needs to know this exact bug to explain why it has no run and no duplicate marker), which directly contradicts both ROADMAP SC4 and the project's own stated Core Value ('every file...can be traced, explained'). No actual duplicate DATA rows resulted (normalized.customers is unaffected -- file 9's content was already loaded), so this is a metadata/audit-trail integrity gap, not a data-loss-into-the-warehouse gap."
    artifacts:
      - path: "packages/dataplat/src/dataplat/metadata/postgres.py"
        issue: "find_file_by_content_hash has no ORDER BY, so LIMIT 1 is non-deterministic once 2+ rows share a content hash -- confirmed live: file_id=10 (content hash f90142cf...) is orphaned (DISCOVERED status, duplicate_of_file_id=NULL, zero linked ingestion_runs rows) in the current cluster database"
    missing:
      - "ORDER BY file_id ASC (or created_at ASC) before LIMIT 1 in find_file_by_content_hash, restoring the invariant discover_files's rediscovery-correction logic already assumes"
      - "A one-off data-repair pass for the live cluster's already-orphaned file_id=10 row (cosmetic/audit-trail only; no analytical data is at risk)"
---

# Phase 4: Vertical Slice — CSV to Analytical PostgreSQL — Verification Report

**Phase Goal (ROADMAP.md):** One real CSV travels end to end — MinIO → TaskFlow DAG → KubernetesPodOperator → processor → analytical PostgreSQL — and is idempotent by construction, so a re-run produces zero additional rows.
**Phase Goal (as framed by the orchestrator's task):** Prove a real, unattended vertical slice — a CSV file landing in MinIO triggers, via Airflow/Kubernetes, a fully idempotent, atomic, auditable load into the analytical PostgreSQL database, with zero data loss or duplication under retry, concurrent access, or pod failure.
**Verified:** 2026-08-14T00:00:00Z
**Status:** gaps_found
**Re-verification:** No — initial verification

## Mode Note (read before the rest of this report)

ROADMAP.md marks this phase `mode: mvp`, which would normally trigger MVP-mode verification narrowed to a User Story's `[outcome]` clause. `gsd-sdk query user-story.validate` against the phase's actual goal text returned `valid: false` (fails all four User Story format checks). Checking the other 10 phases' goal text in ROADMAP.md shows the same non-User-Story phrasing throughout — this is a project-wide convention (technical/outcome goals, not user stories), not a phase-4-specific mistake. Per the MVP-mode guard, I did not force a low-quality User Flow Coverage table onto a non-conforming goal. Instead I ran full standard goal-backward verification against the ROADMAP's 5 explicit, well-specified Success Criteria plus all 9 plans' frontmatter `must_haves` — which is strictly more rigorous than MVP narrowing would have been, not a reduction in scope. **This is a process/tooling mismatch worth fixing (either drop `mode: mvp` for infra/platform phases in this project, or rephrase goals as User Stories), not a phase-4 defect.**

## Methodology Note: This Report Includes Live Cluster Evidence

The kind cluster referenced throughout this phase's plans/summaries was live and reachable during verification. Beyond static code reading, I ran read-only `kubectl exec`/`psql` queries directly against the live `analytics-db` and live `airflow` deployment — no writes, no deletions, no pod kills. This surfaced concrete, current evidence (10,000,122 real rows in `normalized.customers`, 13 real completed ingestion runs, and one **live-manifested** instance of the CR-02 defect below) that goes beyond what static review or trusting SUMMARY.md claims could show.

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | SC1: CSV drop → TaskFlow DAG → KPO pod → analytical PostgreSQL, ≤4KB XCom receipt, DAG <150 lines, no parsing/validation/typing/DB-writes in the DAG file | ✓ VERIFIED | `airflow/dags/csv_ingest_customers.py` is 149 lines, imports only `airflow`/`pendulum`/`kubernetes.client`/`_common.kpo` (never `dataplat`/`csv_processor`); `discover`/`ingest` tasks are both `KubernetesPodOperator`s launched via `cmds=["dataplat"]`/`arguments=[...]`. `Receipt` (`models/receipt.py`) is an all-scalar Pydantic model (`extra="forbid"`), structurally bounded well under 4KB. Live: `kubectl exec` into the scheduler shows `csv_ingest_customers` registered, `is_paused: False`, and live task pods (`csv-ingest-customers-wait-for-files-*`, `-resolve-window-*`) actively running on the real cluster at verification time. `tests/policy/test_dag_thinness.py` + `test_dag_line_budget.py` + `tests/unit/test_dag_structure.py`: 12/12 passed when I ran them directly. |
| 2 | SC2: Re-running the same DAG run, and re-uploading the same file under a different name, both produce zero additional rows | ✓ VERIFIED | Live query: `normalized.customers` has `total_rows = 10,000,122` and `distinct_customers = 10,000,122` — an EXACT match, despite 13 real ingestion runs (including this phase's own repeated E2E fixture uploads) having executed against it. `MergePublisher._PUBLISH_SQL` (`merge.py`) uses `INSERT ... ON CONFLICT (customer_id) DO UPDATE ... WHERE _record_hash IS DISTINCT FROM EXCLUDED._record_hash` — a genuinely idempotent upsert, confirmed by direct code read. `migrations/0006` replaced the plain index with a real `UNIQUE(customer_id)` constraint, confirmed by direct read. Filenames live in `meta.files` (e.g. `ingest-demo-smoke-test.csv-*` reuploaded 3x under different names, all resolving to the same content) show the reupload-under-a-different-name path was genuinely exercised. |
| 3 | SC3: Killing the task pod mid-load + Airflow retry leaves no duplicate rows / no partial visibility; concurrent SELECT never observes a half-loaded table | ✓ VERIFIED | `run.py`'s publish path executes inside one `conn.transaction()` block: `pg_advisory_xact_lock` → `publisher.publish()` → `finalize_publication` all commit or roll back together (independently confirmed by direct read, not just trusting META-03's docstring claim). Staging (checkpointed, outside the transaction) is fully separate from the atomic publish barrier, so a mid-staging crash never touches already-committed data. Live: `meta.ingestion_runs` for files named `e2e-podkill-*` and `e2e-concurrent-select-*` show `status=SUCCEEDED`, `rows_loaded=1,000,000` each, matching `rows_in_target` exactly via a live SQL join — i.e., the actual pod-kill-retry and concurrent-select E2E scenarios ran against this cluster and left clean, fully-published data. |
| 4 | SC4: `meta.batches`/`meta.ingestion_runs`/every loaded row answer "which file, which batch, which run, which attempt, which config version" by SQL alone | ⚠️ VERIFIED WITH A LIVE-CONFIRMED GAP | The general case is solid: a live 5-row lineage join (`meta.ingestion_runs` ⋈ `meta.files` ⋈ `meta.batches` ⋈ `normalized.customers._run_id`) resolves cleanly and `rows_in_target` matches `rows_loaded` exactly in every sampled row. **However**, see Gap #3 below (Truth 8) — this claim is FALSIFIED for at least one live row (`file_id=10`) right now, in the actual cluster database, due to a confirmed, root-caused defect (`find_file_by_content_hash`'s missing `ORDER BY`). |
| 5 | SC5: U1 (XCom contains built git SHA) and U3 (streaming throughput + peak RSS baseline) spike results are recorded in the repository | ✓ VERIFIED | `docs/spikes/U1-smoke-xcom.md` (25 lines) and `docs/spikes/U3-throughput-baseline.md` (45 lines) both exist — contrary to the orchestrator's flagged uncertainty about U3, **it IS present on disk**. Both are marked "regenerated automatically" by their respective tests, with concrete, plausible, non-templated data: U1 records a real DagRun ID (`e2e-u1-c8a2a0a74c4f`) and a real short git SHA (`5ae3546`), cross-referenced against `docker/csv-processor/Dockerfile`'s `ARG GIT_SHA` → `ENV GIT_SHA` wiring (confirmed by direct read — the `ARG` is correctly re-declared and promoted to `ENV` post-`FROM`, which is required for it to be visible at container runtime). U3 records `rows_loaded=1,000,000`, `duration_ms=23,840`, `41,946 rows/sec`, `Peak RSS 62.9 MiB`, with an honest methodology section describing the measurement as a sampled lower bound (cgroup `memory.current` polling every 3s, since this cluster has neither cgroup v2 `memory.peak` nor a metrics-server). |
| 6 | 04-05-PLAN.md must-have: "The receipt is written to /airflow/xcom/return.json on every exit path -- success, claim-skip, and run-fatal failure" | ✗ FAILED | See gaps frontmatter, gap 1. Independently confirmed via direct read of `packages/csv-processor/src/csv_processor/cli.py`: only `except DataPlatformError:` writes the failure Receipt; no broader `except Exception:` exists. A raw `psycopg.errors.DataError` (e.g. a bad-value cast at publish time) or any other unwrapped exception skips receipt-writing entirely. Non-fatal operationally (Airflow still fails the task via exit code) but a literal, confirmed violation of an explicit plan commitment. |
| 7 | Run-lifecycle integrity: a `SUCCEEDED` run's status can never regress to `RUNNING` | ✗ FAILED | See gaps frontmatter, gap 2 (CR-01). Independently confirmed via direct read of `run.py` + `postgres.py`: the heartbeat's `update_ingestion_run_status` write carries no `WHERE status = 'RUNNING'` guard, and `stop_heartbeat.set()` fires only after the publish commit. Real, code-confirmed race window; not yet observed live in the current run history (0 RUNNING rows in `meta.ingestion_runs` at verification time — 13 clean SUCCEEDED, 1 legitimate PENDING). |
| 8 | Content-duplicate detection is deterministic and never leaves a file's lineage SQL-unexplainable (component of SC4) | ✗ FAILED — LIVE-CONFIRMED, not just a code-review hypothesis | See gaps frontmatter, gap 3 (CR-02). I ran a live query for `content_sha256` values shared by 2+ `meta.files` rows and found 3 such groups already present in the cluster's real data (most plausibly produced by this phase's own idempotent-reupload E2E/demo testing). Of these, `file_id=10` is currently in a broken state — `duplicate_of_file_id IS NULL`, `status='DISCOVERED'` (never progressed), and zero rows in `meta.ingestion_runs` reference it — fully consistent with, and most plausibly caused by, the root defect in `find_file_by_content_hash`'s missing `ORDER BY`. |

**Score:** 5/8 truths verified (the 5 ROADMAP Success Criteria hold at their literal wording; 3 more precise sub-findings drawn from explicit plan must-haves / code-review findings are FAILED, one of them live-confirmed in the running cluster's actual data).

### Required Artifacts

All artifacts declared across the 9 plans' `must_haves.artifacts` exist on disk and are substantive (none are stubs/placeholders — zero `TODO`/`FIXME`/`TBD`/`XXX`/`HACK`/`PLACEHOLDER` markers found across the ~25 core phase-4 files scanned).

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `migrations/versions/0006_normalized_customers_business_key_unique.py` | `UNIQUE(customer_id)` constraint | ✓ VERIFIED | `op.create_unique_constraint("uq_customers_customer_id", ...)` confirmed |
| `migrations/versions/0008_grant_schema_usage_to_etl_app.py` | `etl_app` schema USAGE grants (orchestrator's live fix) | ✓ VERIFIED | `GRANT USAGE ON SCHEMA meta/normalized TO etl_app` confirmed; docstring explains the exact live-discovered bug it fixes |
| `packages/dataplat/src/dataplat/metadata/repository.py` / `postgres.py` | `get_or_create_ingestion_run`, `claim_ingestion_run`, `finalize_publication`, `create_file(duplicate_of_file_id=...)` | ✓ VERIFIED (wired) | All four present with correct signatures; `finalize_publication` deliberately takes the caller's `conn` (not its own pool connection) to stay inside the publish transaction — confirmed by direct read |
| `packages/dataplat/src/dataplat/storage/objectstore.py` | `list_objects`, `put_object` | ✓ VERIFIED (wired) | Both present (interface + concrete impl at lines 122/136 and 201/238); used live by `discovery.py` |
| `packages/dataplat/src/dataplat/config/registry.py` | `get_by_id` | ✓ VERIFIED | Present at line 187 |
| `packages/dataplat/src/dataplat/models/identity.py` | `RunContext.file_id`/`batch_id` | ✓ VERIFIED (wired) | Consumed directly in `run.py`'s `run_ingest` (`ctx.run.file_id`/`ctx.run.batch_id`) |
| `kubernetes/rbac-etl.yaml`, `scripts/etl-secrets.sh`, `helm/values/{local,ci}/airflow.yaml` | RBAC + secrets + DAG-mount wiring | ✓ VERIFIED (wired, live) | Live: `airflow dags list` shows `fileloc=/opt/airflow/dags/csv_ingest_customers.py` resolving correctly (the previously-deferred stale-hostPath issue is confirmed fixed); pods for this DAG are actively running under the `csv-processor` identity |
| `packages/dataplat/src/dataplat/discovery.py` | `discover_files` — frozen-manifest authoring | ✓ VERIFIED (wired, live) | Full read confirms sorted listing, chunked hashing, dedup check, deterministic cap, zero wall-clock reads. Live `batch_key` values (`customers:<hash16>`) match the documented format exactly |
| `packages/dataplat/src/dataplat/load/staging.py` | `StagingLoader` — chunked COPY into all-TEXT staging | ✓ VERIFIED | `copy.write_row()` writes from Python-side `enriched_rows` built after CSV parsing (`run_streaming`) — never `COPY ... FORMAT csv` on a raw file (LOAD-12 satisfied) |
| `packages/dataplat/src/dataplat/load/publish/merge.py`, `registry.py` | `MergePublisher` — advisory-lock + `ON CONFLICT`, never `MERGE` | ✓ VERIFIED | Full read confirms `pg_advisory_xact_lock` (caller-owned) + `INSERT...ON CONFLICT(customer_id)` + `DISTINCT ON` structural dedup |
| `packages/dataplat/src/dataplat/pipeline/run.py` | `run_ingest` — claim/stage/publish/receipt orchestration | ⚠️ VERIFIED WITH GAPS | Core atomicity confirmed solid; heartbeat post-commit race (gap 2) lives here |
| `packages/csv-processor/src/csv_processor/cli.py` | `discover`/`ingest` CLI, both plugin-attached | ⚠️ VERIFIED WITH GAP | Plugin wiring confirmed; receipt-on-every-exit-path gap (gap 1) lives here |
| `airflow/dags/csv_ingest_customers.py`, `smoke_kubernetes_pod.py`, `_common/kpo.py` | The two DAGs + shared KPO builder | ✓ VERIFIED (wired, live) | 149/26/98 lines respectively; resources declared on every KPO task (ORCH-09); live pods running |
| `tests/integration/test_discover_files.py`, `test_publish_merge.py` | Rerun/concurrency/lineage integration proofs | ✓ VERIFIED | 346/793 lines, substantive |
| `tests/e2e/slice/*` | pod-kill/retry, concurrent-select, idempotent-reupload, U1/U3 | ✓ VERIFIED (live-corroborated) | File-name correlation between test fixtures (`e2e-podkill-*`, `e2e-concurrent-select-*`, `e2e-u3-*`, `e2e-idempotent-*`) and live `meta.files`/`meta.ingestion_runs` rows confirms these tests genuinely executed against real infrastructure, not mocks |
| `docs/spikes/U1-smoke-xcom.md`, `U3-throughput-baseline.md` | Spike results | ✓ VERIFIED | Both exist; content is concrete and test-regenerated, not hand-authored placeholders |
| `scripts/ingest-demo.py`, `Makefile` | `make ingest-demo` workflow | ✓ VERIFIED (live-corroborated) | Live `meta.files` rows named `ingest-demo-make-test.csv-*`/`ingest-demo-smoke-test.csv-*` with `status=PROCESSED` prove this was actually run successfully against the live cluster; `Makefile`'s `cluster-verify` target confirmed at line 178: `pytest tests/e2e/cluster tests/e2e/slice -q` |

### Key Link Verification

| From | To | Via | Status | Details |
|------|-----|-----|--------|---------|
| `metadata/postgres.py` | `meta.ingestion_runs` | `ON CONFLICT (idempotency_key)` / `WHERE status IN (...)` | ✓ WIRED | Confirmed in `get_or_create_ingestion_run` and `claim_ingestion_run` |
| `load/publish/merge.py` | `normalized.customers` | `INSERT ... ON CONFLICT (customer_id) DO UPDATE ... WHERE` | ✓ WIRED | Confirmed; live data shows it holding at 10M-row scale |
| `pipeline/run.py` | `pg_advisory_xact_lock` | `SELECT pg_advisory_xact_lock(hashtextextended(...))` | ✓ WIRED | Confirmed, immediately before `publisher.publish()`, inside the same transaction |
| `discovery.py` | `s3://metadata/assignments/...` | `ObjectStore.put_object` | ✓ WIRED | Confirmed |
| `csv_ingest_customers.py` | `localhost:5001/csv-processor:<sha>` | `KubernetesPodOperator(image=Variable.get(...))` | ✓ WIRED (live) | Live pods confirm the image resolves and runs |
| `helm/values/{local,ci}/airflow.yaml` | `kind/cluster.yaml`'s `/mnt/dags` hostPath | `extraVolumes` | ✓ WIRED (live) | Live `airflow dags list` shows the DAG resolving from `/opt/airflow/dags/csv_ingest_customers.py` — previously-deferred stale-mount issue confirmed resolved |
| `dataplat.cli` | `csv_processor.cli` | `importlib.metadata.entry_points(group="dataplat.plugins")` | ✓ WIRED (live) | `dataplat discover`/`dataplat ingest` both run successfully as live KPO pods |
| `metadata/postgres.py::find_file_by_content_hash` | `discovery.py`'s rediscovery-correction logic | Implicit "same row returned across calls" assumption | ✗ NOT SAFELY WIRED | See gap 3 — confirmed broken live for `file_id=10` |
| `pipeline/run.py::_heartbeat_loop` | `metadata/postgres.py::update_ingestion_run_status` | Unconditional interval write, no terminal-status guard | ✗ NOT SAFELY WIRED | See gap 2 |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|---------------------|--------|
| `normalized.customers` | rows via `MergePublisher.publish` | Real `staging.<dataset>__r<run_id>` COPY-loaded from parsed CSV via `csv_processor` | Yes — 10,000,122 real rows, live | ✓ FLOWING |
| `meta.ingestion_runs.rows_loaded`/`duration_ms` | `finalize_publication` args | `result.rows_affected` (real cursor rowcount) / `time.monotonic()` delta | Yes — live values (e.g. 1,000,000 / 23,840ms) match `normalized.customers` counts exactly | ✓ FLOWING |
| `Receipt` XCom payload | `run_ingest`'s return value | Same in-memory values as the DB write | Yes, for the success/DataPlatformError paths | ⚠️ PARTIAL — disconnected for the uncaught-exception path (gap 1) |
| `meta.files.duplicate_of_file_id` | `find_file_by_content_hash` result | Non-deterministic `LIMIT 1` query | No — can silently resolve to a wrong/self-referential row | ✗ HOLLOW for the multi-duplicate edge case (gap 3, live-confirmed) |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| DAG policy/thinness/line-budget/structure tests pass | `uv run --frozen pytest tests/policy/test_dag_thinness.py tests/policy/test_dag_line_budget.py tests/unit/test_dag_structure.py -q` | `12 passed in 0.84s` | ✓ PASS |
| `csv_ingest_customers` DAG is registered and unpaused on the live cluster | `kubectl exec -n airflow deploy/airflow-scheduler -- airflow dags list` | `is_paused: False`, `fileloc` resolves correctly | ✓ PASS |
| Analytical data is idempotent at real scale | Live SQL: `SELECT count(*), count(DISTINCT customer_id) FROM normalized.customers` | `10000122 = 10000122` | ✓ PASS |
| Ingestion-run audit trail is internally coherent (no stuck/zombie RUNNING rows) | Live SQL: `SELECT status, count(*) FROM meta.ingestion_runs GROUP BY status` | `13 SUCCEEDED, 1 PENDING, 0 RUNNING` | ✓ PASS (though this does not disprove gap 2's race window, only that it hasn't fired yet) |
| Content-duplicate lineage is deterministic and SQL-explainable | Live SQL: join `meta.files` groups sharing `content_sha256` against `meta.ingestion_runs` | 1 of 3 duplicate-content groups (`file_id=10`) is orphaned: no `duplicate_of_file_id`, no linked run | ✗ FAIL — see gap 3 |

### Probe Execution

No `scripts/*/tests/probe-*.sh` convention or explicit probe declarations found in this phase's PLAN/SUMMARY files. Step 7c: SKIPPED (no declared or conventional probes for this phase — verification instead used direct pytest invocation and live cluster queries, documented above).

### Requirements Coverage

All 22 requirement IDs assigned to Phase 4 in `.planning/REQUIREMENTS.md`'s traceability table are claimed by at least one of the 9 plans' `requirements:` frontmatter — **zero orphaned requirements**.

| Requirement | Source Plan(s) | Description | Status | Evidence |
|---|---|---|---|---|
| ORCH-01 | 04-07 | TaskFlow API (`@dag`, `@task`) | ✓ SATISFIED | `csv_ingest_customers.py` uses `airflow.sdk`'s `@dag`/`@task` |
| ORCH-02 | 04-07 | ETL work only in KPO pods | ✓ SATISFIED | Confirmed — no DAG-file business logic |
| ORCH-03 | 04-03, 04-07 | Bounded Dynamic Task Mapping | ✓ SATISFIED | `.expand(arguments=...)` capped by `batching.max_units_per_run`, confirmed in `discovery.py` |
| ORCH-04 | 04-07 | Explicit retry/failure + backfill support | ✓ SATISFIED | `retries=2/3`, `retry_exponential_backoff=True` on all tasks; backfill posture documented and reasoned in DAG docstring |
| ORCH-05 | 04-07 | Logical-date/data-interval derived window, tolerates `None` | ✓ SATISFIED | `resolve_window()` explicitly handles `dag_run.logical_date is None` |
| ORCH-06 | 04-07 | DAG <150 lines, no parsing/validation/typing/DB-writes | ✓ SATISFIED | 149 lines; `test_dag_line_budget.py`/`test_dag_thinness.py` pass live |
| ORCH-07 | 04-07 | Dataset dependency via Assets/sensors | ✓ SATISFIED | `S3KeySensor` (deferrable) |
| ORCH-08 | 04-03, 04-06, 04-07 | Frozen-manifest expansion, never live listing mid-run | ✓ SATISFIED | `discovery.py`'s docstring + implementation; `test_discover_files.py` proves rerun stability |
| ORCH-09 | 04-07 | CPU/memory requests+limits per task pod | ✓ SATISFIED | `_DISCOVER_RESOURCES`/`_INGEST_RESOURCES`, both `requests` and `limits` set |
| META-03 | 04-01, 04-05, 04-06 | Rows + watermark + run status commit in one publication transaction | ✓ SATISFIED (transaction itself is sound) | Confirmed via direct read of `run.py`'s `with ctx.db.connection() as conn, conn.transaction():` block. Note: gap 2 (CR-01) is a SEPARATE, POST-commit heartbeat write outside this transaction — it does not falsify META-03's literal claim about the publication transaction's own atomicity, but it does undermine the same "auditable" spirit META-03 exists to serve |
| LOAD-01 | 04-05, 04-08 | No dup/corrupt data on any rerun path | ✓ SATISFIED | Live 10M-row idempotency evidence |
| LOAD-02 | 04-05, 04-08 | Airflow retry mid-load creates no dup rows, proven by test | ✓ SATISFIED | `test_pod_kill_mid_load_produces_no_duplicates`, live-corroborated |
| LOAD-03 | 04-03, 04-08 | Reprocessing identical file (by checksum) is a no-op | ✓ SATISFIED | Live filename evidence + `test_idempotent_reupload` |
| LOAD-04 | 04-01, 04-06 | File/batch/record/target-row identity modeled distinctly | ✓ SATISFIED (general case); see gap 3 for an edge-case exception | Live lineage join query |
| LOAD-05 | 04-04, 04-05, 04-08 | Transactional staging → atomic publication | ✓ SATISFIED | Confirmed in `run.py` |
| LOAD-08 | 04-01, 04-04, 04-05, 04-06 | Batch ledger `UNIQUE(dataset,batch_key)` + run-scoped identity | ✓ SATISFIED | Confirmed; `get_or_create_batch`'s `ON CONFLICT` targets this constraint |
| LOAD-09 | 04-01, 04-04, 04-05, 04-06 | Single-writer publication via advisory lock + `ON CONFLICT`, never `MERGE` | ✓ SATISFIED | Confirmed in `merge.py`/`run.py`; `MERGE` deliberately avoided (documented rationale, PG BUG #18279) |
| LOAD-12 | 04-04 | Processor is the only CSV parser; no `COPY...FORMAT csv` on raw input | ✓ SATISFIED | Confirmed: `staging.py`'s COPY writes from Python-parsed `enriched_rows`, never a raw file. **Note:** `.planning/REQUIREMENTS.md`'s traceability table (line "LOAD-12 \| Phase 4 \| Pending") is inconsistent with its own v1-requirements checkbox (`[x]`) for the same ID — a documentation-sync issue, not a code gap; recommend updating the table to "Complete" |
| INCR-08 | 04-03 | Business date derived from data, never wall-clock/`logical_date` | ✓ SATISFIED for this phase's scope | `discovery.py` never reads wall-clock/`logical_date`; dedicated regression test `test_discover_files_never_reads_wall_clock_time_and_leaves_business_date_null` exists and was confirmed present. This requirement is fundamentally a preventive constraint ("never do X"), and ROADMAP.md's own Phase-4 plan-guidance text explicitly frames it as a "cheap-now decision decided here." Positive business-date extraction from filenames/content is out of scope until Phase 6 (CSV-01) — `meta.files.business_date`/`meta.batches.business_date` are left `NULL` by design, not derived incorrectly. `.planning/REQUIREMENTS.md`'s checkbox and traceability table both already say "Pending" for this ID, consistent with the positive-derivation part being deferred — no correction needed there, just noting the constraint-only scope explicitly here |
| QUAL-05 | 04-06 | Integration tests: MinIO → processor → PostgreSQL | ✓ SATISFIED | `tests/integration/test_discover_files.py`, `test_publish_merge.py`, plus siblings (`test_metadata_repository.py`, `test_migrations.py`, `test_objectstore.py`, `test_staging_loader.py`, `test_run_ingest.py`, `test_config_registry.py`) all present and substantive |
| QUAL-06 | 04-08, 04-09 | E2E tests: CSV → MinIO → Airflow → Kubernetes → processor → PostgreSQL | ✓ SATISFIED | `tests/e2e/slice/*`, live-corroborated |
| QUAL-09 | 04-08 | Idempotency tested, including zero-additional-rows assertion | ✓ SATISFIED | `test_idempotent_reupload` + DB-level `UNIQUE` constraints + live 10M-row evidence |

## Anti-Patterns Found

No debt markers (`TODO`/`FIXME`/`TBD`/`XXX`/`HACK`/`PLACEHOLDER`) in any of the ~25 core phase-4 files scanned. The following are from `.planning/phases/04-.../04-REVIEW.md` (a completed code review), independently re-confirmed where noted:

| File | Line(s) | Pattern | Severity | Impact |
|------|---------|---------|----------|--------|
| `pipeline/run.py` + `metadata/postgres.py` | 190-197 / 401-429 | Unguarded status-regression write in heartbeat | 🛑 Elevated to Gap (see truths table #7) | Independently confirmed by direct code read |
| `metadata/postgres.py` | 162-178 | Non-deterministic `LIMIT 1` with no `ORDER BY` | 🛑 Elevated to Gap (see truths table #8) | Independently confirmed by direct code read AND live-manifested in the running cluster (`file_id=10`) |
| `csv_processor/cli.py` | 199-263 | Docstring/implementation mismatch — "every exit path" | 🛑 Elevated to Gap (see truths table #6) | Independently confirmed by direct code read |
| `metadata/postgres.py` (claim_ingestion_run's WHERE clause) + `pipeline/run.py` | — | No code path ever writes `status='FAILED'` (WR-02) | ⚠️ Warning (not elevated — no explicit plan must-have asserts this) | Real gap in failure-state observability; a permanently-abandoned run is indistinguishable from "still running" without cross-referencing `lease_expires_at` |
| `load/publish/merge.py` casts + `load/staging.py` | — | A single bad-value row aborts the whole file's publish (WR-04) | ⚠️ Warning (not elevated — explicitly scoped as future-phase work by the review itself) | Anticipated, not this phase's scope per the review's own "Fix (scoped to this phase)" framing |
| `config/model.py` `DatasetConfig.dataset` | — | Unvalidated identifier reaches raw SQL/filesystem paths (WR-05, WR-06) | ⚠️ Warning | Not reachable via current wiring (DAG always passes literal `"customers"`), but the CLI entrypoint is directly invokable |
| `csv_processor/cli.py` (`AIRFLOW_TASK_TRY_NUMBER`) | — | Never set by any KPO pod; `try_number` always `1` | ℹ️ Info | Degrades audit-trail accuracy on genuine Airflow retries, does not affect correctness |
| `REQUIREMENTS.md` traceability table | line "LOAD-12 \| Phase 4 \| Pending" | Inconsistent with its own `[x]` checkbox for LOAD-12 | ℹ️ Info | Documentation-sync issue only; code confirms LOAD-12 is genuinely satisfied |
| `tests/policy/test_gates_actually_fail.py` | — | 2 pre-existing, unrelated failures (import-linter ANSI-color output drift) | ℹ️ Info | Confirmed pre-existing from Phase 1 (`edf4756`), reproducible on an unmodified tree, independently rediscovered by 4 separate plans in this phase and consistently logged in `deferred-items.md`. Not a phase-4 regression |

## Human Verification Required

None. All must-haves in this phase are backend/infrastructure claims (no UI), and I was able to independently verify the live cluster and live database directly (read-only), which is stronger evidence than what a human clicking through a UI could add. The three FAILED truths above are code-confirmed defects with proposed fixes already written in `04-REVIEW.md` — what they need is a decision (accept via override + follow-up plan, or fix-and-reverify now), not human exploratory testing.

## Gaps Summary

**The core vertical-slice deliverable is real and solid.** This is not a "phase 4 doesn't work" report. Live evidence — not just code reading, not just trusting SUMMARY.md — shows: 10,000,122 real rows loaded into `normalized.customers` with zero duplicates despite 13 independent ingestion runs including deliberate pod-kills, concurrent-reads-during-publish, and repeated re-uploads of identical content; a live, unpaused, actively-scheduling DAG; a fully wired KubernetesPodOperator path from a real locally-built image through to a receipt in XCom; both required spike documents present with genuine, test-generated measurements. All 5 ROADMAP Success Criteria hold at their literal wording, and all 22 assigned requirement IDs have concrete implementation evidence — zero orphaned requirements.

**Three precise, code-confirmed defects keep this from a clean PASS**, all three already caught by this phase's own code review (`04-REVIEW.md`, CR-01/CR-02/WR-01) and left deliberately unfixed pending a human decision:

1. **A run's receipt is not written on every exit path** (falsifies an explicit 04-05-PLAN.md must-have) — a narrow exception class (non-`DataPlatformError`) skips the Receipt/XCom write. Non-fatal operationally (Airflow still fails the task), but a literal contract violation.
2. **A heartbeat race can silently revert `SUCCEEDED` back to `RUNNING`** after the publish transaction commits — code-confirmed, not yet observed live in the current run history (0 RUNNING rows currently), but the window is real and the 2-second heartbeat interval used on the live `ingest` task increases the odds of hitting it.
3. **Content-duplicate lookups are non-deterministic and this has already produced a live, broken, SQL-unexplainable row** — `file_id=10` in the running cluster's actual database, right now, cannot be explained by SQL alone: it is not marked as a duplicate, has no linked ingestion run, and is stuck at `DISCOVERED` status. No analytical data was lost (the content was already loaded via the true original file), but the platform's own Core Value promise ("every file...can be traced, explained") is broken for this specific row today.

All three findings come with concrete, already-drafted fixes in `04-REVIEW.md` (a `WHERE status = 'RUNNING'` guard on the heartbeat write; an `ORDER BY file_id ASC` on the duplicate lookup; a broadened `except Exception:` clause). Given the precision of the diagnosis and fixes already available, closing these is likely a small, well-scoped follow-up plan, not a phase-4 redo.

**This looks like exactly the kind of situation the override mechanism exists for**, if the developer wants to explicitly accept these three as tracked, deferred risk rather than blocking on them now:

```yaml
overrides:
  - must_have: "The receipt is written to /airflow/xcom/return.json on every exit path"
    reason: "Non-DataPlatformError exceptions still fail the Airflow task via exit code; fix tracked in 04-REVIEW.md WR-01"
    accepted_by: "{your name}"
    accepted_at: "{ISO timestamp}"
  - must_have: "A run's terminal status can never regress to RUNNING"
    reason: "Race window confirmed real but not yet observed live; fix tracked in 04-REVIEW.md CR-01"
    accepted_by: "{your name}"
    accepted_at: "{ISO timestamp}"
  - must_have: "Content-duplicate detection is deterministic and SQL-explainable"
    reason: "Live-confirmed edge case affecting metadata lineage only, not analytical data; fix tracked in 04-REVIEW.md CR-02"
    accepted_by: "{your name}"
    accepted_at: "{ISO timestamp}"
```

Absent such an override, per this workflow's decision tree, three FAILED truths (one of them live-confirmed against real data) mean `status: gaps_found` is the honest, calibrated classification — not a judgment that the vertical slice is broken, but a precise flag that three specific, well-understood, already-diagnosed cracks in the "auditable" and "zero data loss" promise should be closed (or consciously accepted) before this run-lifecycle machinery carries more weight in later phases (Phase 9's incremental/backfill work builds directly on `meta.ingestion_runs`/`meta.files` semantics).

---

_Verified: 2026-08-14T00:00:00Z_
_Verifier: Claude (gsd-verifier)_
