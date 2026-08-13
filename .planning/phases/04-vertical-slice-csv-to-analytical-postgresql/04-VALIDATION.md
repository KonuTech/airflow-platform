---
phase: 4
slug: vertical-slice-csv-to-analytical-postgresql
status: draft
nyquist_compliant: false
wave_0_complete: false
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
| **Full suite command** | `make test-integration` (testcontainers, existing) + a **new** cluster-gated target for this phase's E2E tests, e.g. `$(RUN_CLUSTER) pytest tests/e2e/cluster tests/e2e/slice -q`, extending the existing `cluster-verify` pattern |
| **Estimated runtime** | ~5s quick (no Docker) / ~90s integration (testcontainers) / cluster-gated E2E (pod-kill, concurrent-SELECT, 1M-row U3 spike) is materially longer and only runs before `/gsd:verify-work`, never per-commit |

---

## Sampling Rate

- **After every task commit:** `uv run --frozen pytest tests/unit -k <touched module> -x`
- **After every plan wave:** `make check && make test-integration`
- **Before `/gsd:verify-work`:** Full suite green, plus the new cluster-gated E2E target (pod-kill/retry, concurrent-SELECT, idempotent-reupload) green at least once against the live kind cluster — this genuinely needs the cluster and cannot run in the offline gate
- **Max feedback latency:** ~90 seconds for the offline gate (testcontainers startup dominates); the cluster-gated E2E tier is phase-gate-only, not part of per-commit latency budget

---

## Per-Task Verification Map

Task ID / Plan / Wave columns are filled in by the planner as it creates `PLAN.md` files — this
table pre-registers the requirement → test mapping the planner's tasks must satisfy. Threat refs
correspond to the Known Threat Patterns identified in `04-RESEARCH.md`'s Security Domain section:
T-04-01 (SQL injection via CSV field values reaching a query), T-04-02 (tampered/malformed
assignment JSON), T-04-03 (overly-broad RBAC), T-04-04 (sensitive detail leaking via XCom receipt
or `error_message`), T-04-05 (unbounded Dynamic Task Mapping fan-out as DoS).

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|------------------|-----------|--------------------|-------------|--------|
| TBD | TBD | TBD | ORCH-01 | — | DAG files import only from `airflow.sdk`, never `airflow.decorators`/`airflow.models`; both DAGs use `@dag`/`@task` | unit (structural) | `pytest tests/unit/test_dag_structure.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | ORCH-02 | T-04-01 | No parsing/validation/typing/DB writes in DAG files — DAGs call only `dataplat`/`csv_processor` functions | unit (policy/import-linter) | `pytest tests/policy/test_dag_thinness.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | ORCH-03 | T-04-05 | `max_map_length` set; `batching.max_units_per_run` bounds the frozen manifest below the Airflow ceiling | unit + integration | `pytest tests/unit/test_batching_config.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | ORCH-04 | — | KPO task declares `retries=3, retry_exponential_backoff=True`; sensor-DAG backfill behavior explicitly documented (degenerate-but-harmless case) | unit (structural) | `pytest tests/unit/test_dag_structure.py::test_retries_set -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | ORCH-05 | — | `resolve_window`-style guard tolerates `logical_date=None` (asset/API-triggered runs) without `KeyError` | unit | `pytest tests/unit/test_resolve_window.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | ORCH-06 | — | Both DAG files stay under 150 lines each | unit (policy) | `pytest tests/policy/test_dag_line_budget.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | ORCH-07 | — | Dataset dependency expressed via the deferrable `S3KeySensor` task (D-01, locked), not a hidden Python check | unit (structural) | `pytest tests/unit/test_dag_structure.py::test_uses_s3_key_sensor -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | ORCH-08 | — | `discover_files` writes `meta.files` + assignment JSON to MinIO *before* `.expand()` runs; a rerun over the same window produces the identical manifest | integration | `pytest tests/integration/test_discover_files.py::test_rerun_same_manifest -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | ORCH-09 | — | KPO task sets `container_resources` (CPU/memory requests+limits), matching `workers.kubernetes.resources` precedent in `helm/values/local/airflow.yaml` | unit (structural, mocked `.execute`) | `pytest tests/unit/test_dag_structure.py::test_kpo_resources -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | META-03 | — | Rows, run-status update and batch/file status commit in exactly one publication transaction — resolved against the real migrated schema (no `meta.watermarks` table exists yet this phase) | integration | `pytest tests/integration/test_publish_merge.py::test_atomic_commit -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | LOAD-01 / LOAD-02 | T-04-01 | An Airflow retry mid-load, via the run-claim protocol + `pg_advisory_xact_lock`, produces no duplicate rows | e2e | `pytest tests/e2e/slice/test_pod_kill_retry.py -x` (D-09..D-11) | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | LOAD-03 | — | Re-uploading identical content under a new filename is a no-op via `meta.files.duplicate_of_file_id` + the `skip` policy (D-13, locked) | e2e | `pytest tests/e2e/slice/test_idempotent_reupload.py -x` (D-07 fixture) | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | LOAD-04 | — | `_run_id`/`_file_id`/`_batch_id`/`_source_row_number` populated distinctly per loaded row (columns already migrated; this phase is first to populate them) | integration | `pytest tests/integration/test_publish_merge.py::test_lineage_columns_populated -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | LOAD-05 | — | Staging → validate → atomic publish; a concurrent `SELECT` during publish never observes a partial row count | e2e | `pytest tests/e2e/slice/test_concurrent_select.py -x` (D-12) | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | LOAD-08 | — | `meta.batches` `UNIQUE(dataset_id, batch_key)` (already migrated) rejects a duplicate batch; every staged/loaded row carries `run_id`+`try_number`-scoped identity | integration | `pytest tests/integration/test_publish_merge.py::test_duplicate_batch_key_rejected -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | LOAD-09 | T-04-01 | Publication is single-writer: `pg_advisory_xact_lock(hashtext(target))` held for the transaction + `INSERT ... ON CONFLICT (customer_id) DO UPDATE ... WHERE` — never literal `MERGE` | integration | `pytest tests/integration/test_publish_merge.py::test_advisory_lock_serializes_concurrent_publishers -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | LOAD-12 | — | The processor is the only CSV parser; `COPY ... FORMAT csv` on raw input remains prohibited | policy (existing, Phase 1) | `pytest tests/policy/test_no_postgres_csv_parsing.py -x` | ✅ | ⬜ pending |
| TBD | TBD | TBD | INCR-08 | — | `meta.files.business_date` is never derived from `datetime.now()`/`logical_date`; legitimately stays `NULL` this phase (no business-date-bearing source field yet) | unit | `pytest tests/unit/test_discover_files.py::test_business_date_not_derived_from_clock -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | QUAL-05 | — | MinIO → processor → PostgreSQL integration, including staging/publish/quarantine paths | integration | `make test-integration` (extended) | ❌ W0 (extends existing dir) | ⬜ pending |
| TBD | TBD | TBD | QUAL-06 | — | Full CSV → MinIO → Airflow → Kubernetes → processor → PostgreSQL, cluster-gated | e2e | new cluster-gated target (`tests/e2e/slice/`), see Test Infrastructure above | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | QUAL-09 | — | Re-running the same DAG run produces zero additional rows (idempotency) | e2e | `pytest tests/e2e/slice/test_idempotent_reupload.py -x` (shared with LOAD-03) | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/e2e/slice/__init__.py`, `conftest.py` — new directory, needs a fixture analogous to `tests/e2e/cluster/conftest.py`'s `_require_cluster` skip-with-reason pattern
- [ ] `tests/unit/test_dag_structure.py` — `DagBag(dag_folder="airflow/dags", include_examples=False)`, assert `import_errors == {}`, plus structural assertions (retries, resources, sensor presence, no top-level heavy imports)
- [ ] `tests/policy/test_dag_thinness.py` — grep/import-linter policy that DAG files contain no parsing/validation/typing/DB-write code
- [ ] `tests/policy/test_dag_line_budget.py` — line-count assertion per DAG file, consistent with the existing `tests/policy/` convention
- [ ] `tests/unit/test_resolve_window.py` — `logical_date=None` tolerance
- [ ] `tests/unit/test_batching_config.py` — `max_map_length` / `batching.max_units_per_run`
- [ ] `tests/integration/test_discover_files.py` — frozen-manifest rerun proof (ORCH-08)
- [ ] `tests/integration/test_publish_merge.py` — atomic commit (META-03), lineage columns (LOAD-04), batch-key uniqueness (LOAD-08), advisory-lock serialization (LOAD-09) — one file, multiple tests
- [ ] `tests/unit/test_discover_files.py::test_business_date_not_derived_from_clock` — INCR-08 (may co-locate in the same file as the ORCH-08 test)
- [ ] `tests/e2e/slice/test_pod_kill_retry.py` — `kubectl delete pod` mid-load, polls `meta.ingestion_runs.rows_read` (D-11, never `sleep`)
- [ ] `tests/e2e/slice/test_concurrent_select.py` — concurrent connection polling `normalized.customers` during an in-flight publish (D-12)
- [ ] `tests/e2e/slice/test_idempotent_reupload.py` — rerun + re-upload-under-different-name, both zero-additional-rows (D-07 fixture)
- [ ] Framework install: none — pytest, testcontainers, boto3, psycopg are already present via the `dev`/`cluster` dependency groups

*`tests/policy/test_no_postgres_csv_parsing.py` (LOAD-12) already exists from Phase 1 — no Wave 0
work needed for that row.*

---

## Manual-Only Verifications

*None — every phase behavior has an automated verification path, including the two spikes:
U1 (XCom contains the built git SHA) and U3 (streaming throughput + peak RSS baseline) are both
recorded as committed, machine-readable artifacts per ROADMAP success criterion 5, not left to a
one-off manual check.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 90s (offline gate); cluster-gated E2E tier explicitly exempted as phase-gate-only
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
