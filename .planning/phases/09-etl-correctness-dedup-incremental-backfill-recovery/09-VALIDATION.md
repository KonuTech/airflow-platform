---
phase: 09
slug: etl-correctness-dedup-incremental-backfill-recovery
status: final
nyquist_compliant: true
wave_0_complete: true
created: 2026-08-19
updated: 2026-08-19
---

# Phase 09 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 9.1.1 (config: `pyproject.toml` `[tool.pytest.ini_options]`), plus `testcontainers[postgres,minio] 4.15.0` for the `integration`/`dbt` marker tiers and a live kind cluster for the `cluster` marker tier |
| **Config file** | `pyproject.toml` (`markers = ["slow", "regression", "cluster", "manifests", "integration", "dagtest", "dbt"]`, `addopts = "-ra --strict-markers --strict-config"`) |
| **Quick run command** | `pytest tests/unit tests/regression -q --cov --cov-report=term-missing` |
| **Full suite command** | `pytest tests/integration -q` followed by `pytest tests/e2e/cluster tests/e2e/slice tests/e2e/observability -q -m cluster` |
| **Estimated runtime** | ~5 min quick / ~45+ min full (live 2-year backfill sweep dominates, per D-27 live-first target) |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/unit tests/regression -q --cov --cov-report=term-missing`
- **After every plan wave:** Run `pytest tests/integration -q -m "not dbt"` then `pytest tests/integration -q -m dbt`; for waves touching live-cluster proof (waves 5-6), also `pytest tests/e2e/slice -q -m cluster`
- **Before `/gsd:verify-work`:** Full suite must be green — `tests/integration` + `tests/e2e/slice` + `tests/e2e/cluster` + `tests/e2e/observability`, all `-m cluster` tiers included, per D-27's live-first target
- **Max feedback latency:** 300 seconds (quick tier); live-cluster tier is inherently slower and sampled per-wave, not per-commit

---

## Per-Task Verification Map

Real plan IDs and wave numbers, superseding this table's earlier `TBD-NN`/`10a-10d` placeholder
draft (written before planning). Wave numbers reflect the final dependency-corrected graph (09-02
depends on 09-01's `ReconciliationConfig`, cascading three plans one wave later than the initial
draft).

| Plan | Wave | Requirement(s) | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|------|------|-----------------|-----------------|-----------|-------------------|-------------|--------|
| 09-01 | 1 | VALID-05 | `deduplication` Optional, `ReconciliationConfig.sum_columns` config surface exists | unit | `pytest tests/unit/test_csv_processor_cli.py ... -q` (13-file regression list) | ✅ plan-created | ⬜ pending execution |
| 09-02 | 2 | INCR-01, INCR-02, VALID-05 | `meta.watermarks` records `max(event_ts)`/`max(order_date)` per dataset, `GREATEST()`-enforced `>=`; silver→gold reconciliation row written | integration | `pytest tests/integration/test_watermarks.py tests/integration/test_reconciliation.py -q -k "not raw_bronze and not bronze_silver"` | ✅ plan-created | ⬜ pending execution |
| 09-03 | 1 | VALID-06 | `_BATCH_COMPLETE` manifest body read/parsed, threaded onto `RunContext` | unit | `pytest tests/unit/validate/test_batch_complete_manifest.py tests/unit/validate/test_batch_complete_marker.py -q` | ✅ plan-created | ⬜ pending execution |
| 09-04 | 1 | LOAD-06 | `DBT_BUILD` status recordable via plain-psycopg ADR-0004 exception | dagtest | `pytest tests/dagtest/test_run_stage_recorder.py -q` | ✅ plan-created | ⬜ pending execution |
| 09-05 | 1 | INCR-06, QUAL-11 | 2-year corpus generator, deterministic, schema-change/gap/late-event combined | unit | `pytest tests/unit/test_dated_series.py -q` | ✅ plan-created | ⬜ pending execution |
| 09-06 | 3 | LOAD-06 | `meta.v_run_recovery` answers "what succeeded/remains, retry stage X" across all 3 stages | integration | `pytest tests/integration/test_run_recovery_view.py -q` | ✅ plan-created | ⬜ pending execution |
| 09-07 | 3 | VALID-05, VALID-06 | raw→bronze reconciliation (D-22 quarantine-aware) + control-total comparison (D-23), record-and-continue | integration | `pytest tests/integration/test_reconciliation.py -q -k raw_bronze` and `pytest tests/integration/test_batch_complete_control_totals.py -q` | ✅ plan-created | ⬜ pending execution |
| 09-08 | 3 | VALID-05 | bronze→silver reconciliation, per-file grain (D-24), macro + `severity: warn` test (D-26) | `dbt` marker | `pytest tests/integration/test_dbt_reconciliation.py -q -m dbt` | ✅ plan-created | ⬜ pending execution |
| 09-09 | 4 | LOAD-06 | `DBT_BUILD` tracked in both DAGs; exhausted-retry Grafana alert (D-19) | dagtest + config | `pytest tests/dagtest/ -q` | ✅ plan-created | ⬜ pending execution |
| 09-10 | 5 | INCR-06, LOAD-06 | explicit gap record for missing backfill file (D-06); live `dbt_build` pod-kill recovery (D-18) | live (cluster) | `pytest tests/e2e/slice/test_pod_kill_retry.py -q -m cluster -k dbt_build` | ✅ plan-created | ⬜ pending execution |
| 09-11 | 6 | INCR-05, INCR-06, QUAL-11 | full 2-year sweep, no bypass, idempotent re-run, live+backfill concurrency | live (cluster), testcontainers fallback (D-27) | `pytest tests/e2e/slice/test_backfill_2year_sweep.py -q -m cluster` | ✅ plan-created | ⬜ pending execution |

*Status column tracks EXECUTION, not planning — all rows are "pending execution" until
`/gsd:execute-phase 09` runs; every plan already carries a real `<automated>` verify command per
task (Nyquist rule satisfied at planning time).*

*Status legend: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements — all satisfied by the final plan set

- [x] `tests/integration/test_watermarks.py` — covers INCR-01/INCR-02 (plan 09-02, Task 3)
- [x] `tests/integration/test_run_recovery_view.py` — covers LOAD-06 (plan 09-06, Task 2)
- [x] `tests/integration/test_reconciliation.py` — covers VALID-05's raw→bronze (plan 09-07) and silver→gold (plan 09-02) Python-side hops
- [x] `tests/integration/test_dbt_reconciliation.py` — covers VALID-05's bronze→silver dbt-macro hop, D-24's per-file grain, and D-26's `severity: warn` test (plan 09-08)
- [x] `tests/integration/test_batch_complete_control_totals.py` — covers VALID-06 (plan 09-07, Task 2)
- [x] `tests/e2e/slice/test_backfill_2year_sweep.py` — covers INCR-05/INCR-06/QUAL-11's live 2-year proof (plan 09-11)
- [x] 2-year fixture corpus generator — `tools/corpus/dated_series.py` (plan 09-05); RESEARCH.md Open Question 2 resolved (existing `generators.py` cannot express this, confirmed new work)
- [x] `tests/e2e/slice/conftest.py` polling-helper addition for D-18's `dbt_build` pod-kill test (plan 09-10, Task 2)

---

## Manual-Only Verifications

*None — all phase behaviors have automated verification. RESEARCH.md's Open Questions (backfill
window sizing, fixture-generator capability) are both RESOLVED at planning time — see
`09-RESEARCH.md`'s `## Open Questions (RESOLVED)` section — as planning-time-to-execution-time
deferrals with a concrete, automated implementation (09-11 Task 1's dry-run/pilot sequence; 09-05's
`dated_series.py`), not unverifiable behaviors.*

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies — confirmed: every task across all
      11 final plans carries an `<automated>` command (`gsd-sdk query verify.plan-structure`
      returned zero warnings/errors for all 11 `09-*-PLAN.md` files)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify — every single task has
      one, so this is vacuously satisfied
- [x] Wave 0 covers all MISSING references — all 8 files RESEARCH.md's Wave 0 Gaps section flagged
      as `❌ Wave 0` now have an owning plan/task (see table above)
- [x] No watch-mode flags — no `--watch`/`-w` flag appears in any `<automated>` command across the
      11 plans
- [x] Feedback latency < 300s (quick tier) — `pytest tests/unit tests/regression` tier tasks are
      all sub-300s; live-cluster tier tasks (waves 5-6) are explicitly sampled per-wave, not
      per-commit, per this document's own Sampling Rate section
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** granted (planning-time compliance confirmed; execution-time status tracked separately
per-plan in the table above)
