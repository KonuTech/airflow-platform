---
phase: 8
slug: validation-quarantine-metadata-control-plane-completion
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-08-17
---

# Phase 8 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 9.1.1 + hypothesis 6.165.3 + testcontainers 4.15.0 (all already pinned and in use) |
| **Config file** | `pyproject.toml` `[tool.pytest.ini_options]` (existing — `markers` list needs one addition: `dagtest`, see Wave 0 Requirements) |
| **Quick run command** | `pytest tests/unit tests/property -q` |
| **Full suite command** | `pytest tests/unit tests/property tests/integration tests/dagtest -q -m "not cluster"` (CI-safe); `pytest -m cluster` separately against a live kind cluster for the genuine E2E backfill/orphan proof |
| **Estimated runtime** | ~90s quick (no Docker) / ~6-8min full (testcontainers Postgres x2 + MinIO) |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/unit tests/property -q`
- **After every plan wave:** Run `pytest tests/unit tests/property tests/integration -q -m integration` plus `pytest tests/dagtest -q` (new tier — needs Docker for its own testcontainers Airflow-metadata Postgres, run it in a dedicated CI job/stage separate from `tests/integration`'s analytical-DB containers to stay inside the 4 CPU/16GB CI runner budget — three concurrent testcontainers Postgres instances plus the pytest process risks exceeding it)
- **Before `/gsd:verify-work`:** Full suite (`tests/unit`, `tests/property`, `tests/integration`, `tests/dagtest`) must be green; `tests/e2e/slice` (`cluster`-marked) run separately against a live kind cluster as this phase's genuine end-to-end proof of VALID-07/VALID-08's "real" success criteria (ROADMAP success criteria #3 and #5 explicitly require live proof, not fixtures-only)
- **Max feedback latency:** 90s (quick gate)

---

## Per-Task Verification Map

Tasks are not yet assigned (planning has not run) — this maps each phase requirement to its
test surface per 08-RESEARCH.md's "Phase Requirements → Test Map". The planner must bind each
row to concrete task IDs when PLAN.md files are created; do not leave any row unbound.

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| TBD | TBD | TBD | VALID-01 | — | Structural rule variants each produce a `RejectedRecord` with row/column/error_type | unit | `pytest tests/unit/validate/test_structural_rules.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | VALID-02 | — | Completeness/uniqueness/validity-range/pattern rules evaluate correct PASS/PASS_WITH_WARNING/FAIL/QUARANTINE outcome | unit + property | `pytest tests/unit/validate/test_quality_rules.py tests/property/test_quality_rules_never_raise.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | VALID-03 | — | Each of the 5 strategies dispatches to the correct row/run-level action; never silently discards | unit | `pytest tests/unit/validate/test_strategy_dispatch.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | VALID-04 | V5 | `meta.validation_results`/`meta.rejected_records` rows exist after a run AND a MinIO report artifact exists at `report_uri`, same run | integration | `pytest tests/integration/test_validation_persistence.py -x -m integration` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | VALID-07 | — | Orphan `customer_id` in `orders` quarantined (`REFERENTIAL_ORPHAN`), non-orphan rows in same file still publish | integration | `pytest tests/integration/test_referential_integrity.py -x -m integration` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | VALID-07 (live) | — | Real orphan order, raced against not-yet-loaded `customers` batch, proven against real deployed DAGs | e2e, cluster | `pytest tests/e2e/slice/test_referential_orphan.py -x -m cluster` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | VALID-08 | V4 | Backfill DagRun resolves batch's `PENDING` rejected_records to `RESOLVED`/`REDRIVEN`, linked to new `run_id`; no per-row edit path exists | integration | `pytest tests/integration/test_backfill_resolution.py -x -m integration` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | VALID-08 (DAG shape) | — | `csv_ingest_orders`/`csv_ingest_customers` execute correctly as a backfill DagRun (KPO mocked) | dag.test() | `pytest tests/dagtest/test_backfill_dagrun.py -x` | ❌ W0, new tier | ⬜ pending |
| TBD | TBD | TBD | VALID-08 (live) | — | Real `airflow dags backfill`, real corrected file, `meta.rejected_records` row genuinely flips to `RESOLVED` | e2e, cluster | `pytest tests/e2e/slice/test_backfill_reentry.py -x -m cluster` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | VALID-09 (minimal, this phase) | — | `row_count` metric persisted per run; single-file-vs-persisted-baseline comparison flags 10x volume anomaly (no forecasting/ML, no historical trend — that's Phase 9's VALID-05/06) | unit + integration | `pytest tests/unit/validate/test_volume_anomaly.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | LOAD-10 | V4 | `integrity_gate` rejects a file whose two HEAD calls disagree, wrong-extension, or empty — before any pod launches; rejection recorded on `meta.files.status` via narrow inline DB call (no `dataplat` import into Airflow image, ADR-0004) | unit | `pytest tests/unit/test_integrity_gate.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | LOAD-10 (DAG shape) | — | `integrity_gate` task exists upstream of `discover` in both DAGs; `discover` never runs when gate fails | DagBag-structural | `pytest tests/unit/test_dag_structure.py -x` (extend existing) | ✅ extend | ⬜ pending |
| TBD | TBD | TBD | LOAD-11 | — | `_BATCH_COMPLETE` marker honored when present in fixture/corpus data (opt-in, unexercised by live `customers`/`orders` configs per D-19) | unit + corpus | `pytest tests/unit/validate/test_batch_complete_marker.py -x` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/unit/validate/` — new directory, mirrors `tests/unit/detect/`/`tests/unit/normalize/`/`tests/unit/schema/`'s existing per-domain layout; covers VALID-01/02/03/09/LOAD-11
- [ ] `tests/integration/test_validation_persistence.py`, `test_referential_integrity.py`, `test_backfill_resolution.py` — extend existing `tests/integration/conftest.py` fixtures (`migrated_dsn`, `s3_client`)
- [ ] `tests/dagtest/` — NEW top-level test tier for `dag.test()`-based behavioral DAG tests; own `conftest.py` with a session-scoped testcontainers PostgreSQL for the Airflow *metadata* DB (distinct from `tests/integration/conftest.py`'s *analytical* DB fixture — CLAUDE.md §4 constraint), `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`, `AIRFLOW__CORE__EXECUTOR=LocalExecutor`, and a fixture patching `KubernetesPodOperator.execute`
- [ ] `pyproject.toml` `[tool.pytest.ini_options]` `markers` — add `dagtest: needs a local Docker daemon (testcontainers PostgreSQL for Airflow metadata); excluded from the offline gate`
- [ ] `tests/e2e/slice/test_referential_orphan.py`, `test_backfill_reentry.py` — extend existing `tests/e2e/slice/conftest.py` fixtures, `cluster`-marked
- [ ] `configs/datasets/orders.yaml` fixture/corpus test data — new synthetic CSV fixture set under `tests/fixtures/` for both orphan and non-orphan rows
- [ ] Framework install: none — pytest/hypothesis/testcontainers already pinned; `dag.test()` needs no new dependency

---

## Manual-Only Verifications

*None — all phase behaviors have automated verification (unit/property/integration/dagtest/e2e tiers).*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 90s (quick gate)
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
