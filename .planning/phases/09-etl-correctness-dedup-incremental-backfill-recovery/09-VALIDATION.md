---
phase: 09
slug: etl-correctness-dedup-incremental-backfill-recovery
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-08-19
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
- **After every plan wave:** Run `pytest tests/integration -q -m "not dbt"` then `pytest tests/integration -q -m dbt`; for waves touching live-cluster proof (10b/10c per ROADMAP's Wave F ordering), also `pytest tests/e2e/slice -q -m cluster`
- **Before `/gsd:verify-work`:** Full suite must be green — `tests/integration` + `tests/e2e/slice` + `tests/e2e/cluster` + `tests/e2e/observability`, all `-m cluster` tiers included, per D-27's live-first target
- **Max feedback latency:** 300 seconds (quick tier); live-cluster tier is inherently slower and sampled per-wave, not per-commit

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| TBD-01 | 10a | 1 | INCR-01 | — | `meta.watermarks` records `max(event_ts)`/`max(order_date)` per dataset, observational only | integration | `pytest tests/integration/test_watermarks.py -q` | ❌ W0 | ⬜ pending |
| TBD-02 | 10a | 1 | INCR-02 | — | Watermark advances only from committed values inside publish tx, `>=` never `>`; unchanged on kill | integration + live | `pytest tests/integration/test_watermarks.py -q -k transaction`; live: extend `test_pod_kill_retry.py` | ❌ W0 (both) | ⬜ pending |
| TBD-03 | 10b | 2 | INCR-05 | — | Backfill runs identical discover→stage→dbt_build→publish graph, no bypass, for customers + orders | live (cluster) | `pytest tests/e2e/slice/test_backfill_reentry.py -q -m cluster` (extend for orders) | ✅ exists, extend | ⬜ pending |
| TBD-04 | 10b | 2 | INCR-06 | — | Backfill idempotent, resolves historical schema versions, records explicit gap for missing file | live (cluster), testcontainers fallback | live: `pytest tests/e2e/slice/test_backfill_2year_sweep.py -q -m cluster`; fallback: `pytest tests/integration/test_backfill_idempotency.py tests/integration/test_schema_resolution.py -q` | ❌ new live file (W0); ✅ existing integration to extend | ⬜ pending |
| TBD-05 | 10c | 3 | LOAD-06 | — | `meta.v_run_recovery` answers "what succeeded/remains, retry stage X" across all 3 stages; dbt_build recoverable from pod kill | integration + live | integration: `pytest tests/integration/test_run_recovery_view.py -q`; live: extend `pytest tests/e2e/slice/test_pod_kill_retry.py -q -m cluster -k dbt_build` | ❌ new integration file (W0); ✅ existing live file to extend | ⬜ pending |
| TBD-06 | 10d | 1 | VALID-05 | — | `meta.reconciliation_results` populated at all 3 hops, quarantine-aware (nets out rejected_records/dedup_audit) | integration (`dbt` marker for bronze→silver) | Python hops: `pytest tests/integration/test_reconciliation.py -q`; dbt hop: `pytest tests/integration/test_dbt_reconciliation.py -q -m dbt` | ❌ W0 (both new) | ⬜ pending |
| TBD-07 | 10d | 1 | VALID-06 | — | `_BATCH_COMPLETE` manifest carries expected_row_count/checksum, compared against loaded target, discrepancy recorded not blocked | integration | `pytest tests/integration/test_batch_complete_control_totals.py -q` | ❌ W0 (new) | ⬜ pending |
| TBD-08 | 10b | 2 | QUAL-11 | — | Re-running full 2-year backfill sweep produces zero additional rows; old fixture file parses under its historical schema version | live (cluster) primary, testcontainers fallback | live: `pytest tests/e2e/slice/test_backfill_2year_sweep.py -q -m cluster -k idempotent`; fallback: `pytest tests/integration/test_backfill_idempotency.py -q` | ❌ new live file (W0, shared with INCR-06); ✅ existing integration to extend | ⬜ pending |

*Task IDs are placeholders — the planner assigns real `{plan}-{wave}-{seq}` IDs; this table's Req/Test mapping carries forward unchanged.*

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/integration/test_watermarks.py` — covers INCR-01/INCR-02 (watermark advance, `GREATEST()`/`>=` semantics, same-transaction placement)
- [ ] `tests/integration/test_run_recovery_view.py` — covers LOAD-06 (`meta.v_run_recovery` next-action logic across all 3 `run_stages` values)
- [ ] `tests/integration/test_reconciliation.py` — covers VALID-05's raw→bronze and silver→gold Python-side hops
- [ ] `tests/integration/test_dbt_reconciliation.py` — covers VALID-05's bronze→silver dbt-macro hop and D-26's `severity: warn` test; mirror `tests/integration/test_dbt_dedup_audit.py`'s existing structure
- [ ] `tests/integration/test_batch_complete_control_totals.py` — covers VALID-06 (manifest body read/parse/compare); extends `tests/unit/validate/test_batch_complete_marker.py`'s presence-only coverage
- [ ] `tests/e2e/slice/test_backfill_2year_sweep.py` — covers INCR-06/QUAL-11's live 2-year proof (schema-version boundary, gap, late/out-of-order event, idempotent re-run); shares fixtures/polling helpers with `test_backfill_reentry.py`
- [ ] 2-year fixture corpus generator/manifest work (RESEARCH.md Open Question 2) — confirm `tools/corpus/generators.py` expresses "daily cadence over N days with injected schema-version boundary + gap + late event," or add missing generator function(s), plus a new `tests/fixtures/backfill-corpus.yaml` manifest
- [ ] `tests/e2e/slice/conftest.py` polling-helper addition for D-18's `dbt_build` pod-kill test — new poll helper keyed on `meta.run_stages` reaching `stage_name='DBT_BUILD', status='RUNNING'` (once D-14 lands), mirroring `_poll_mid_load_signal`'s existing shape

---

## Manual-Only Verifications

*None — all phase behaviors have automated verification (per RESEARCH.md's Phase Requirements → Test Map, every requirement maps to a pytest command; the two "unclear" items in Open Questions — backfill window sizing and fixture-generator capability — are planning-time sizing decisions, not unverifiable behaviors).*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 300s (quick tier)
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
