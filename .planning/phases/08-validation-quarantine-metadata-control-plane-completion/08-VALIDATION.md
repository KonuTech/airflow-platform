---
phase: 8
slug: validation-quarantine-metadata-control-plane-completion
status: draft
nyquist_compliant: true
wave_0_complete: true
created: 2026-08-17
---

# Phase 8 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 9.1.1 + hypothesis 6.165.3 + testcontainers 4.15.0 (all already pinned and in use) |
| **Config file** | `pyproject.toml` `[tool.pytest.ini_options]` (existing — `markers` list needs one addition: `dagtest`, added by plan 08-13, see Wave 0 Requirements) |
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

Bound to the 14 PLAN.md files created for this phase (revision iteration 1 — VALID-03's row now
points at the real `StrategyDispatchStage` proof added to plan 08-10; VALID-08's D-05 wiring row
now reflects that `resolve_rejected_records_for_batch` is genuinely called by `run_ingest`, plan
08-11, not just proven in isolation by plan 08-03).

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 08-04.T1 | 08-04 | 2 | VALID-01 | — | Structural rule variants each produce a `RejectedRecord` with row/column/error_type | unit | `pytest tests/unit/validate/test_structural_rules.py -x` | ❌ new | ⬜ pending |
| 08-04.T1/T2 | 08-04 | 2 | VALID-02 | — | Completeness/uniqueness/validity-range/pattern rules evaluate correct PASS/PASS_WITH_WARNING/FAIL/QUARANTINE outcome | unit + property | `pytest tests/unit/validate/test_quality_rules.py tests/property/test_quality_rules_never_raise.py -x` | ❌ new | ⬜ pending |
| 08-10.T1 | 08-10 | 4 | VALID-03 | — | `StrategyDispatchStage` proves each of the 5 strategies (`FAIL_FILE`/`REJECT_RECORD`/`QUARANTINE_FILE`/`QUARANTINE_RECORD`/`WARN_AND_CONTINUE`) dispatches to the correct row/run-level action; never silently discards | unit | `pytest tests/unit/validate/test_strategy_dispatch.py -x` | ❌ new (revision-added) | ⬜ pending |
| 08-03.T2 | 08-03 | 2 | VALID-04 (persistence half) | V5 | `meta.validation_results`/`meta.rejected_records` rows exist after a run, inside the same transaction as publish | integration | `pytest tests/integration/test_validation_persistence.py -x -m integration` | ❌ new | ⬜ pending |
| — | — | — | VALID-04 (MinIO artifact half) | V5 | A machine-readable validation report artifact is written to MinIO at `report_uri`, same run | — | — | ❌ NOT COVERED by any of this phase's 14 plans — every `finalize_publication`/`Receipt` call site in plan 08-11 passes `report_uri=None`. Flagged here, not silently marked done; carry forward to the next revision iteration or an explicit scope decision, not fixed in this pass (out of the two blockers this revision targeted). | ⬜ gap |
| 08-08.T? | 08-08 | 3 | VALID-07 | — | Orphan `customer_id` in `orders` quarantined (`REFERENTIAL_ORPHAN`), non-orphan rows in same file still publish | integration | `pytest tests/integration/test_referential_integrity.py -x -m integration` | ❌ new | ⬜ pending |
| 08-14.T1 | 08-14 | 7 | VALID-07 (live) | — | Real orphan order, raced against not-yet-loaded `customers` batch, proven against real deployed DAGs | e2e, cluster | `pytest tests/e2e/slice/test_referential_orphan.py -x -m cluster` | ❌ new | ⬜ pending |
| 08-03.T2 + 08-11.T1/T2 | 08-03 / 08-11 | 2 / 5 | VALID-08 | V4 | `resolve_rejected_records_for_batch` (08-03) resolves batch's `PENDING` rejected_records to `RESOLVED`/`REDRIVEN`, linked to new `run_id`; `run_ingest` (08-11) is the real, sole production caller (D-05 wiring, revision-fixed — previously unreachable); no per-row edit path exists | integration | `pytest tests/integration/test_backfill_resolution.py tests/integration/test_publish_transaction_wiring.py -x -m integration` | ❌ new | ⬜ pending |
| 08-13.T? | 08-13 | 7 | VALID-08 (DAG shape) | — | `csv_ingest_orders`/`csv_ingest_customers` execute correctly as a backfill DagRun (KPO mocked) | dag.test() | `pytest tests/dagtest/test_backfill_dagrun.py -x` | ❌ new tier | ⬜ pending |
| 08-14.T2 | 08-14 | 7 | VALID-08 (live) | — | Real `airflow dags backfill`, real corrected file, `meta.rejected_records` row genuinely flips to `RESOLVED`/`REDRIVEN`, now backed by a real production call path (D-05) | e2e, cluster | `pytest tests/e2e/slice/test_backfill_reentry.py -x -m cluster` | ❌ new | ⬜ pending |
| 08-09.T? | 08-09 | 3 | VALID-09 (minimal, this phase) | — | `row_count` metric persisted per run; single-file-vs-persisted-baseline comparison flags 10x volume anomaly (no forecasting/ML, no historical trend — that's Phase 9's VALID-05/06) | unit + integration | `pytest tests/unit/validate/test_volume_anomaly.py -x` | ❌ new | ⬜ pending |
| 08-02.T2 | 08-02 | 1 | LOAD-10 | V4 | `integrity_gate` rejects a file whose two HEAD calls disagree, wrong-extension, or empty — before any pod launches; EVERY rejection path (including the ones with no knowable file content) recorded on `meta.files.status` via narrow inline DB call with a real or sentinel `content_sha256` (revision-fixed D-20 NOT NULL conflict; no `dataplat` import into Airflow image, ADR-0004) | unit | `pytest tests/unit/test_integrity_gate.py -x` | ❌ new | ⬜ pending |
| 08-12.T2 | 08-12 | 6 | LOAD-10 (DAG shape) | — | `list_matched_keys -> integrity_gate` chain exists upstream of `discover` in both DAGs; `discover` never runs when the gate fails for a matched key | DagBag-structural | `pytest tests/unit/test_dag_structure.py -x` (extend existing) | ✅ extend | ⬜ pending |
| 08-06.T? | 08-06 | 2 | LOAD-11 | — | `_BATCH_COMPLETE` marker honored when present in fixture/corpus data (opt-in, unexercised by live `customers`/`orders` configs per D-19) | unit + corpus | `pytest tests/unit/validate/test_batch_complete_marker.py -x` | ❌ new | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [x] `tests/unit/validate/` — new directory, mirrors `tests/unit/detect/`/`tests/unit/normalize/`/`tests/unit/schema/`'s existing per-domain layout; covers VALID-01/02/03/09/LOAD-11 (plans 08-04, 08-07, 08-09, 08-10, 08-06)
- [x] `tests/integration/test_validation_persistence.py`, `test_referential_integrity.py`, `test_backfill_resolution.py` — extend existing `tests/integration/conftest.py` fixtures (`migrated_dsn`, `s3_client`) (plans 08-03, 08-08)
- [x] `tests/dagtest/` — NEW top-level test tier for `dag.test()`-based behavioral DAG tests; own `conftest.py` (plan 08-13)
- [x] `pyproject.toml` `[tool.pytest.ini_options]` `markers` — add `dagtest: needs a local Docker daemon (testcontainers PostgreSQL for Airflow metadata); excluded from the offline gate` (plan 08-13)
- [x] `tests/e2e/slice/test_referential_orphan.py`, `test_backfill_reentry.py` — extend existing `tests/e2e/slice/conftest.py` fixtures, `cluster`-marked (plan 08-14)
- [x] `configs/datasets/orders.yaml` fixture/corpus test data — new synthetic CSV fixture set under `tests/fixtures/` for both orphan and non-orphan rows (plans 08-05, 08-08, 08-12)
- [x] Framework install: none — pytest/hypothesis/testcontainers already pinned; `dag.test()` needs no new dependency

---

## Manual-Only Verifications

*None — all phase behaviors have automated verification (unit/property/integration/dagtest/e2e tiers).*

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 90s (quick gate)
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** pending — VALID-04's MinIO-artifact half is a known, explicitly flagged gap (see table row), carried forward rather than silently marked covered; not one of this revision's two required blockers, left for the orchestrator/next iteration to scope.
