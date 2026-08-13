---
phase: 4
slug: vertical-slice-csv-to-analytical-postgresql
status: planned
nyquist_compliant: true
wave_0_complete: true
created: 2026-08-13
---

# Phase 4 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest `9.1.1` (already pinned, `[tool.pytest.ini_options]` in root `pyproject.toml`) |
| **Config file** | `pyproject.toml` (`[tool.pytest.ini_options]`); markers already include `cluster: requires a live kind cluster` |
| **Quick run command** | `uv run --frozen pytest tests/unit tests/regression -q` (existing `make test`) |
| **Full suite command** | `make test-integration` (testcontainers, existing, extended by plans 04-01/04-04/04-05/04-06) + `make cluster-verify` (extended by plan 04-09 to also collect `tests/e2e/slice`) |
| **Estimated runtime** | ~5s quick (no Docker) / ~90s integration (testcontainers) / cluster-gated E2E (pod-kill, concurrent-SELECT, 1M-row U3 spike) is materially longer and only runs before `/gsd:verify-work`, never per-commit |

---

## Sampling Rate

- **After every task commit:** `uv run --frozen pytest tests/unit -k <touched module> -x`
- **After every plan wave:** `make check && make test-integration`
- **Before `/gsd:verify-work`:** Full suite green, plus `make cluster-verify` (pod-kill/retry, concurrent-SELECT, idempotent-reupload, smoke-XCom) green at least once against the live kind cluster — this genuinely needs the cluster and cannot run in the offline gate
- **Max feedback latency:** ~90 seconds for the offline gate (testcontainers startup dominates); the cluster-gated E2E tier is phase-gate-only, not part of per-commit latency budget

---

## Per-Task Verification Map

Threat refs correspond to the Known Threat Patterns identified in `04-RESEARCH.md`'s Security Domain
section: T-04-01 (SQL injection via CSV field values reaching a query), T-04-02 (tampered/malformed
assignment JSON), T-04-03 (overly-broad RBAC), T-04-04 (sensitive detail leaking via XCom receipt
or `error_message`), T-04-05 (unbounded Dynamic Task Mapping fan-out as DoS). Additional threats
identified during planning (T-04-06 through T-04-14) are recorded in each plan's own
`<threat_model>` block.

| Task | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | Status |
|------|------|------|-------------|------------|------------------|-----------|--------------------|--------|
| T2 | 04-07 | 4 | ORCH-01 | — | DAG files import only from `airflow.sdk`, never `airflow.decorators`/`airflow.models`; both DAGs use `@dag`/`@task` | unit (structural) | `pytest tests/unit/test_dag_structure.py -x` | ⬜ pending |
| T2 | 04-07 | 4 | ORCH-02 | T-04-01 | No parsing/validation/typing/DB writes in DAG files — DAGs call only `dataplat`/`csv_processor` via KPO `cmds`/`arguments` | unit (policy) | `pytest tests/policy/test_dag_thinness.py -x` | ⬜ pending |
| T1 | 04-03 | 2 | ORCH-03 | T-04-05 | `batching.max_units_per_run` config field required, validated, and enforced by `discover_files`'s truncation logic | unit + integration | `pytest tests/unit/test_batching_config.py -x` | ⬜ pending |
| T2 | 04-07 | 4 | ORCH-04 | — | KPO tasks declare `retries` (2 for discover, 3 with exponential backoff for ingest); sensor-DAG backfill treated as a documented degenerate case | unit (structural) | `pytest tests/unit/test_dag_structure.py::test_retries_set -x` | ⬜ pending |
| T2 | 04-07 | 4 | ORCH-05 | — | `resolve_window` tolerates `logical_date=None` (asset/API-triggered runs) without `KeyError` | unit | `pytest tests/unit/test_resolve_window.py -x` | ⬜ pending |
| T2 | 04-07 | 4 | ORCH-06 | — | Both DAG files stay under budget (150 / 30 lines) | unit (policy) | `pytest tests/policy/test_dag_line_budget.py -x` | ⬜ pending |
| T2 | 04-07 | 4 | ORCH-07 | — | Dataset dependency expressed via the deferrable `S3KeySensor` task (D-01, locked), not a hidden Python check | unit (structural) | `pytest tests/unit/test_dag_structure.py::test_uses_s3_key_sensor -x` | ⬜ pending |
| T2 | 04-03 | 2 | ORCH-08 | — | `discover_files` writes `meta.files` + assignment JSON to MinIO *before* `ingest.expand()` reads only identifiers back | unit (behavior spec) | `pytest tests/unit -k discovery -x` | ⬜ pending |
| T1 | 04-06 | 3 | ORCH-08 | — | A rerun over an unchanged object set produces zero additional `meta.files`/`meta.ingestion_runs` rows | integration | `pytest tests/integration/test_discover_files.py::test_rerun_produces_identical_manifest -x` | ⬜ pending |
| T2 | 04-07 | 4 | ORCH-09 | — | Every KPO task sets `container_resources` (CPU/memory requests+limits), matching `workers.kubernetes.resources` precedent | unit (structural) | `pytest tests/unit/test_dag_structure.py::test_kpo_resources -x` | ⬜ pending |
| T1 | 04-01 | 1 | META-03 | — | `finalize_publication` accepts a caller-supplied `conn`, never opens its own connection or commits — the precondition for one atomic transaction | integration | `pytest tests/integration/test_metadata_repository.py -x` | ⬜ pending |
| T1 | 04-05 | 3 | META-03 | — | `run_ingest`'s publish transaction: advisory lock, publish, finalize_publication, commit — all-or-nothing | integration | `pytest tests/integration/test_run_ingest.py -x` | ⬜ pending |
| T2 | 04-06 | 3 | META-03 | — | Rows + file/batch/run status updates are invisible to a second connection until commit, then all visible simultaneously | integration | `pytest tests/integration/test_publish_merge.py::test_atomic_commit -x` | ⬜ pending |
| T2 | 04-08 | 5 | LOAD-01 / LOAD-02 | T-04-01 | A real `kubectl delete pod` mid-load, followed by Airflow's own retry, produces no duplicate rows | e2e | `pytest tests/e2e/slice/test_pod_kill_retry.py::test_pod_kill_mid_load_produces_no_duplicates -x` (D-09..D-11) | ⬜ pending |
| T1 | 04-08 | 5 | LOAD-03 | — | Re-uploading identical content under a new filename is a no-op via `meta.files.duplicate_of_file_id` + the `skip` policy (D-13, locked) | e2e | `pytest tests/e2e/slice/test_smoke_and_idempotency.py::test_idempotent_reupload -x` (D-07 fixture) | ⬜ pending |
| T2 | 04-06 | 3 | LOAD-04 | — | `_run_id`/`_file_id`/`_batch_id`/`_source_row_number` populated distinctly per loaded row, independently queryable | integration | `pytest tests/integration/test_publish_merge.py::test_lineage_columns_populated -x` | ⬜ pending |
| T1 | 04-04 | 2 | LOAD-05 | — | Staging → publish is transactional at the SQL level (StagingLoader's UNLOGGED table + MergePublisher's guarded upsert) | integration | `pytest tests/integration/test_staging_loader.py -x` | ⬜ pending |
| T3 | 04-08 | 5 | LOAD-05 | — | A concurrent `SELECT` running throughout an in-flight publish observes only the pre-publish or fully-published row count | e2e | `pytest tests/e2e/slice/test_concurrent_select.py -x` (D-12) | ⬜ pending |
| T2 | 04-06 | 3 | LOAD-08 | — | `meta.batches` `UNIQUE(dataset_id, batch_key)` (already migrated) rejects a duplicate batch at the database | integration | `pytest tests/integration/test_publish_merge.py::test_duplicate_batch_key_rejected -x` | ⬜ pending |
| T2 | 04-04 | 2 | LOAD-09 | T-04-01 | Publication is `pg_advisory_xact_lock` + `INSERT ... ON CONFLICT (customer_id) DO UPDATE ... WHERE` — never literal `MERGE` | integration | `pytest tests/integration/test_publish_merge.py -x` + `grep -c "MERGE INTO" .../merge.py` (expect 0) | ⬜ pending |
| T2 | 04-06 | 3 | LOAD-09 | T-04-11 | Two concurrent publish attempts against overlapping `customer_id`s serialize through the advisory lock with no constraint violation | integration | `pytest tests/integration/test_publish_merge.py::test_advisory_lock_serializes_concurrent_publishers -x` | ⬜ pending |
| — | 04-04 | 2 | LOAD-12 | — | The processor remains the only CSV parser; staging's all-TEXT design structurally reinforces the existing Phase-1 policy gate | policy (pre-existing, Phase 1) | `pytest tests/policy/test_no_postgres_csv_parsing.py -x` | ✅ (already passing, no new work) |
| T2 | 04-03 | 2 | INCR-08 | — | `discovery.py` contains no `datetime.now()`/`date.today()`/`logical_date` read | unit (grep-based structural check) | `grep -c "datetime.now\|date.today" .../discovery.py` (expect 0) | ⬜ pending |
| T1 | 04-06 | 3 | INCR-08 | — | `meta.files.business_date` is `NULL` on every row `discover_files` creates | integration | `pytest tests/integration/test_discover_files.py::test_business_date_stays_null -x` | ⬜ pending |
| all | 04-01, 04-03, 04-04, 04-05, 04-06 | 1-3 | QUAL-05 | — | MinIO → processor → PostgreSQL integration: staging, publish, discovery, run orchestration | integration | `make test-integration` (extended) | ⬜ pending |
| all | 04-08 | 5 | QUAL-06 | — | Full CSV → MinIO → Airflow → Kubernetes → processor → PostgreSQL, cluster-gated | e2e | `make cluster-verify` (extended by 04-09 to include `tests/e2e/slice`) | ⬜ pending |
| T1, T2 | 04-08 | 5 | QUAL-09 | — | Re-running the same DAG run (pod-kill/retry) and re-uploading the same content both produce zero additional rows | e2e | `pytest tests/e2e/slice/test_pod_kill_retry.py tests/e2e/slice/test_smoke_and_idempotency.py -x` | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

All Wave-0 gaps identified during research are now covered by a concrete plan/task/wave assignment
above — none remain unassigned. Test-file creation itself happens as part of each plan's own tasks
(Interface-First ordering), not as a separate pre-planning Wave 0 pass:

- [x] `tests/e2e/slice/__init__.py`, `conftest.py` — plan 04-08, Task 1, Wave 5
- [x] `tests/unit/test_dag_structure.py` — plan 04-07, Task 2, Wave 4
- [x] `tests/policy/test_dag_thinness.py` — plan 04-07, Task 2, Wave 4
- [x] `tests/policy/test_dag_line_budget.py` — plan 04-07, Task 2, Wave 4
- [x] `tests/unit/test_resolve_window.py` — plan 04-07, Task 2, Wave 4
- [x] `tests/unit/test_batching_config.py` — plan 04-03, Task 1, Wave 2
- [x] `tests/integration/test_discover_files.py` — plan 04-06, Task 1, Wave 3
- [x] `tests/integration/test_publish_merge.py` — plan 04-04, Task 2 (created) then extended by plan 04-06, Task 2, Wave 3
- [x] `tests/integration/test_metadata_repository.py` (extended) — plan 04-01, Task 2, Wave 1
- [x] `tests/integration/test_staging_loader.py` — plan 04-04, Task 1, Wave 2
- [x] `tests/integration/test_run_ingest.py` — plan 04-05, Task 1, Wave 3
- [x] `tests/e2e/slice/test_pod_kill_retry.py` — plan 04-08, Task 2, Wave 5
- [x] `tests/e2e/slice/test_concurrent_select.py` — plan 04-08, Task 3, Wave 5
- [x] `tests/e2e/slice/test_smoke_and_idempotency.py` — plan 04-08, Task 1, Wave 5
- [x] Framework install: none — pytest, testcontainers, boto3, psycopg are already present via the `dev`/`cluster` dependency groups

*`tests/policy/test_no_postgres_csv_parsing.py` (LOAD-12) already exists from Phase 1 — no new work
needed for that row; plan 04-04 reinforces it structurally.*

---

## Manual-Only Verifications

*None — every phase behavior has an automated verification path, including the two spikes:
U1 (XCom contains the built git SHA — plan 04-08, Task 1) and U3 (streaming throughput + peak RSS
baseline — plan 04-08, Task 2) are both recorded as committed, machine-readable artifacts
(`docs/spikes/U1-smoke-xcom.md`, `docs/spikes/U3-throughput-baseline.md`) per ROADMAP success
criterion 5, not left to a one-off manual check.*

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 90s (offline gate); cluster-gated E2E tier explicitly exempted as phase-gate-only
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** planned — every row above maps to a real plan/task/wave; execution not yet started.
