---
phase: 10
slug: slowly-changing-dimensions
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-08-21
---

# Phase 10 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 9.1.1 (`pyproject.toml` `[tool.pytest.ini_options]`, `minversion = "9.0"`, `testpaths = ["tests"]`, `--strict-markers --strict-config`) |
| **Config file** | `pyproject.toml` |
| **Quick run command** | `pytest tests/unit -q` |
| **Full suite command** | `pytest tests/unit tests/integration -q -m "not cluster and not manifests and not dbt"` locally (testcontainers PostgreSQL); `pytest tests/e2e/slice -q -m cluster` against the live kind cluster for the phase gate |
| **Estimated runtime** | ~5 min quick / ~45+ min full (extended 2-year backfill sweep re-run twice, per D-12, dominates) |

---

## Sampling Rate

- **After every task commit:** `pytest tests/unit -q`
- **After every plan wave:** `pytest tests/unit tests/integration -q -m "not cluster and not manifests and not dbt"`
- **Before `/gsd:verify-work`:** `pytest tests/e2e/slice -q -m cluster` (extended `test_backfill_2year_sweep.py`) green against the live kind cluster
- **Max feedback latency:** 300 seconds (quick tier); live-cluster tier sampled per-wave, not per-commit

---

## Per-Task Verification Map

Draft — written before planning. Real plan IDs and wave numbers supersede the `TBD` placeholder
below once `/gsd:plan-phase 10` runs the planner.

| Task ID | Plan | Wave | Requirement | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------------|-----------|--------------------|-------------|--------|
| TBD | TBD | TBD | SCD-01 | `signup_country` (D-13) retains its original value; a correction's incoming value is silently ignored | unit | `pytest tests/unit/test_scd_recompute.py -k type_zero -x` | ❌ Wave 0 | ⬜ pending |
| TBD | TBD | TBD | SCD-02 | `birth_date` overwritten in place on the current row, no history | unit | `pytest tests/unit/test_scd_recompute.py -k type_one -x` | ❌ Wave 0 | ⬜ pending |
| TBD | TBD | TBD | SCD-03 | `valid_from`/`valid_to`/`is_current` correct on a real `name`/`country` change | integration | `pytest tests/integration/test_publish_scd.py -k version_boundary -x -m integration` | ❌ Wave 0 | ⬜ pending |
| TBD | TBD | TBD | SCD-04 | Surrogate key (`BigInteger`+`Identity`) independent of the change hash, distinct from business key | unit | `pytest tests/unit/test_scd_recompute.py -k surrogate_independence -x` | ❌ Wave 0 | ⬜ pending |
| TBD | TBD | TBD | SCD-05 | Deterministic normalized-hash change detection (`check` strategy, reuses `dataplat.normalize.unicode`) | unit + property | `pytest tests/unit/test_scd_hashing.py -x` | ❌ Wave 0 | ⬜ pending |
| TBD | TBD | TBD | SCD-06 | Effective dating (`event_ts`, D-03) never defaults to ingestion time | integration | `pytest tests/integration/test_publish_scd.py -k effective_dating -x -m integration` | ❌ Wave 0 | ⬜ pending |
| TBD | TBD | TBD | SCD-07 | Late correction reads `staging.customers` (bronze, Finding F-1) and recomputes the full chain, never in-place surgery | integration + e2e | `pytest tests/integration/test_publish_scd.py -k late_correction -x -m integration`; live proof in extended `test_backfill_2year_sweep.py` (D-11) | ❌ Wave 0 (integration); extends existing file (e2e) | ⬜ pending |
| TBD | TBD | TBD | SCD-08 | Snapshot-diff DELETE detection scoped to `staged_run_ids` (Finding F-2), `invalidate` default, mass-delete circuit breaker (D-06) | integration | `pytest tests/integration/test_scd_delete_detection.py -x -m integration` | ❌ Wave 0 | ⬜ pending |
| TBD | TBD | TBD | SCD-09 | Replayed identical batch produces exactly one logical version | integration | `pytest tests/integration/test_publish_scd.py -k idempotent_replay -x -m integration` | ❌ Wave 0 | ⬜ pending |
| TBD | TBD | TBD | SCD-10 | Idempotent under re-application — full 2-year backfill re-run (D-12) asserts zero new SCD2 versions | e2e | extended `test_backfill_2year_sweep.py`'s idempotent-rerun pattern | extends existing file | ⬜ pending |
| TBD | TBD | TBD | SCD-11 | Backfill-safe, per-key recompute never blindly overwrites current state | e2e | same D-11/D-12 corpus extension | extends existing file | ⬜ pending |
| TBD | TBD | TBD | SCD-12 | `btree_gist` exclusion constraint rejects an overlapping validity interval | integration | `pytest tests/integration/test_migrations.py -k exclusion_constraint -x -m integration` (direct `INSERT`, expect `psycopg.errors.ExclusionViolation`) | ❌ Wave 0 (extends existing file) | ⬜ pending |
| TBD | TBD | TBD | Concurrency (D-10) | Live attribute change racing a backfill/correction for the same `customer_id` serializes correctly, no corruption | e2e (cluster) | dedicated test in extended `test_backfill_2year_sweep.py` | ❌ Wave 0 | ⬜ pending |
| TBD | TBD | TBD | Consumer fixes (D-08) | `meta.v_customers_lineage` and silver→gold reconciliation correct under multi-row-per-key cardinality | integration | `pytest tests/integration/test_reconciliation.py -q` (reworked); lineage view integration test | Rework of existing files (Finding F-4) | ⬜ pending |
| TBD | TBD | TBD | QUAL-14 | SCD tested incl. late corrections + idempotent re-application | e2e | covered by SCD-07/SCD-09/SCD-10 commands above | — | ⬜ pending |

*Status column tracks EXECUTION, not planning — all rows are "pending" until `/gsd:execute-phase
10` runs.*

*Status legend: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/unit/test_scd_recompute.py` — pure-function unit tests for the recompute logic (Type-0/1/2 dispatch, `LAG()`-based change-point detection); no DB needed if the function accepts plain ordered records.
- [ ] `tests/unit/test_scd_hashing.py` — normalized-hash determinism for `name`/`country`, reusing `dataplat.normalize.unicode`.
- [ ] `tests/integration/test_publish_scd.py` — real `SCDPublisher` against real PostgreSQL (testcontainers): version-boundary creation, effective-dating, late-correction recompute, idempotent replay.
- [ ] `tests/integration/test_scd_delete_detection.py` — snapshot-diff + mass-delete circuit breaker, real PostgreSQL.
- [ ] Extend `tests/integration/test_migrations.py` with a direct exclusion-constraint-rejects-overlap assertion.
- [ ] Extend `tests/e2e/slice/test_backfill_2year_sweep.py` per D-11/D-12: attribute-change events, one late/out-of-order correction, one missing-customer fixture (invalidate), one deliberately-bad/truncated snapshot fixture (circuit-breaker trip, D-06 discretion point), a rewritten no-duplicates corruption assertion (Finding F-4/Pattern 4), and the D-10 dedicated concurrency test.
- [ ] Fix or replace test-blast-radius files (Finding F-4): `tests/integration/test_publish_merge.py` (replace — tests the exact constraint this phase's migration drops), `tests/integration/test_publish_ingest.py`, `tests/integration/test_run_ingest.py`, `tests/integration/test_reconciliation.py`, plus an explicit review pass over the remaining files a grep for `normalized.customers` surfaces across `tests/`.
- [ ] Add `signup_country` to `customers.yaml`, the fixture/corpus generator, and `normalized.customers`'s DDL (D-13).

*Every item above traces to a `❌ Wave 0` cell in the Per-Task Verification Map or to Finding
F-4/D-13 — none are speculative additions.*

---

## Manual-Only Verifications

*None identified — every phase behavior maps to an automatable test per the table above. One
caveat carried from `10-RESEARCH.md` Assumption A2: the recompute SQL shape (Pattern 2) is a
reasoned synthesis, not a verified/copied pattern, and should be validated with a small
spike/prototype early in Wave 0 before the full task breakdown commits to it — this is a
within-Wave-0 verification step, not a manual-only gap.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies — pending planner output
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify — pending planner output
- [ ] Wave 0 covers all MISSING references — draft table above enumerates every `❌ Wave 0` cell from `10-RESEARCH.md`'s own Phase Requirements → Test Map and Wave 0 Gaps sections
- [ ] No watch-mode flags — pending planner output
- [ ] Feedback latency < 300s (quick tier) — pending planner output
- [ ] `nyquist_compliant: true` set in frontmatter — pending, will flip once the planner's real plan IDs replace the `TBD` placeholders above

**Approval:** pending
