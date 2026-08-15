---
phase: 7
slug: observability-metrics-tracing-lineage
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-08-15
---

# Phase 7 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest `9.1.1` (already pinned, root `pyproject.toml`) |
| **Config file** | `pyproject.toml` `[tool.pytest.ini_options]` — existing markers `cluster`, `manifests`, `integration`, `slow`, `regression` already cover this phase's needs; no new marker required |
| **Quick run command** | `uv run --frozen pytest tests/unit tests/property -q` |
| **Full suite command** | `uv run --frozen pytest -m cluster tests/e2e -q` (requires the live cluster) |
| **Estimated runtime** | TBD — every test file this phase needs is Wave 0 net-new (see below); baseline measured once Wave 0 lands |

---

## Sampling Rate

- **After every task commit:** `uv run --frozen pytest tests/unit -q`
- **After every plan wave:** `uv run --frozen pytest -m "cluster or integration" tests/e2e tests/integration -q`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** TBD pending Wave 0 baseline (unit tier expected to stay in the existing <10s offline-gate convention; `cluster`-marked e2e tests are wave-gate-only, not part of per-commit latency budget)

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| TBD (planning) | TBD | TBD | OBS-07 | — | `meta.v_customers_lineage` returns every named column (source file, object path, checksum, batch, ingestion timestamp, DAG/run/task ID, processor version, schema version, config version) for a real ingested row | integration | `pytest tests/integration/test_lineage_view.py -x` | ❌ Wave 0 | ⬜ pending |
| TBD (planning) | TBD | TBD | OBS-08 | V4 (access control, `grafana_reader` role) | `dataplat.observability.metrics.increment()` actually reaches the OTel Collector with bounded `dataset+stage+status` labels | integration | `pytest tests/integration/test_metrics_otlp.py -x` | ❌ Wave 0 | ⬜ pending |
| TBD (planning) | TBD | TBD | OBS-09 | V5 (`FreshnessConfig` input validation) | A dataset with `expected_frequency IS NULL` never appears in the freshness alert query's result set; one with a stale `expected_frequency` does | integration (SQL-only, testcontainers Postgres) | `pytest tests/integration/test_freshness_query.py -x` | ❌ Wave 0 | ⬜ pending |
| TBD (planning) | TBD | TBD | OBS-10 | — | A `TRACEPARENT` env var appears in a real launched KPO pod's spec, and `dataplat`'s first span is a child of it | e2e, `cluster` marker (real pod — D-19's "proof over prose" bar) | `pytest tests/e2e/observability/test_trace_propagation.py -m cluster -x` | ❌ Wave 0 | ⬜ pending |
| TBD (planning) | TBD | TBD | D-20 (freshness alert webhook delivery, CONTEXT.md) | Info. Disclosure (webhook URL sourced from Vault, not runtime-editable) | A real freshness breach causes Grafana to actually POST to a cluster-reachable webhook receiver | e2e, `cluster` marker | `pytest tests/e2e/observability/test_alert_webhook_delivery.py -m cluster -x` | ❌ Wave 0 | ⬜ pending |

*Task ID/Plan/Wave columns are filled in by the planner once plans exist — this draft fixes the requirement→test contract so the planner has a coverage target it cannot silently narrow.*

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/e2e/observability/__init__.py`, `tests/e2e/observability/conftest.py` — new test package, no shared fixtures exist yet for this domain
- [ ] `tests/e2e/observability/test_alert_webhook_delivery.py` — covers D-20. **Key design constraint from research:** the webhook receiver must be reachable from *inside* the kind cluster's pod network (Grafana's Alerting engine runs in-cluster and cannot reach a pytest-process-local `localhost` listener) — plan for a minimal receiver Pod+Service deployed into the cluster for the test's duration, asserting delivery via `kubectl exec`/log inspection, not a host-local HTTP server
- [ ] `tests/integration/test_lineage_view.py`, `tests/integration/test_metrics_otlp.py`, `tests/integration/test_freshness_query.py` — no existing files
- [ ] Framework install: none — pytest/testcontainers already present via the `cluster` dependency group

---

## Manual-Only Verifications

*None identified — all 5 phase behaviors (OBS-07/08/09/10, D-20) have an automated-test path per the map above, including the live-proof bar CONTEXT.md D-19/D-20 require. If planning finds one of these disproportionately expensive to automate, record it here explicitly rather than silently dropping coverage.*

---

## Validation Sign-Off

- [ ] All tasks have `<acceptance_criteria>` with automated commands/behavior assertions
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < TBD s (baseline pending Wave 0)
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending — finalized during planning once real Task/Plan/Wave IDs are assigned
