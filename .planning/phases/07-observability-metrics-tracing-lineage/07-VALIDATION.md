---
phase: 7
slug: observability-metrics-tracing-lineage
status: planned
nyquist_compliant: true
wave_0_complete: true
created: 2026-08-15
updated: 2026-08-15
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
| **Estimated runtime** | ~5–10s for `tests/unit` (offline, no Docker — matches this project's existing convention); `tests/integration` (`test_lineage_view.py`, `test_metrics_otlp.py`, `test_freshness_query.py`, plus Plan 07-05's trace round-trip test) requires testcontainers Postgres, typically well under a minute total; the `cluster`-marked e2e tier (`test_trace_propagation.py`, `test_alert_webhook_delivery.py`) requires the live kind cluster and — for the webhook-delivery test specifically — waiting out at least one Grafana alert-rule-group evaluation cycle (`interval: 5m`, Plan 07-07 Task 2), so it is materially longer and phase-gate-only |

---

## Sampling Rate

- **After every task commit:** `uv run --frozen pytest tests/unit -q`
- **After every plan wave:** `uv run --frozen pytest -m "cluster or integration" tests/e2e tests/integration -q`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** ~10s for the offline unit-tier gate (matches this project's existing convention); the `cluster`-marked e2e tier's own floor is dominated by the alert rule group's `interval: 5m` evaluation cadence (Plan 07-07) — phase-gate-only, never part of per-commit latency budget

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| T3 | 07-01 | 1 | OBS-07 | T-07-01 | `meta.v_customers_lineage` returns every OBS-07-named column (source file, object path, checksum, batch, ingestion timestamp, DAG/run/task ID, processor version, schema version, config version) for a genuinely published row, produced via a real `run_ingest()` call — never a hand-seeded row | integration | `pytest tests/integration/test_lineage_view.py -m integration -x -q` | ✅ (07-01 T3) | ⬜ pending |
| T3 | 07-02 | 1 | OBS-08 | T-07-05 | `dataplat.observability.metrics.increment()` reaches a real OTLP/HTTP receiver carrying exactly the D-04 bounded label set (`dataset`+`stage`+`status`) — no fourth/unbounded label key ever reaches the wire | integration | `pytest tests/integration/test_metrics_otlp.py -m integration -x -q` | ✅ (07-02 T3) | ⬜ pending |
| T3 | 07-01 | 1 | OBS-01 / OBS-09 | T-07-04 | A dataset with `expected_frequency IS NULL` never appears in the freshness-breach SQL condition's result set (never configured, stays quiet); a dataset with a configured `expected_frequency` and no prior file history (cold start, via `meta.datasets.created_at`) correctly appears as stale | integration (SQL-only, testcontainers Postgres) | `pytest tests/integration/test_freshness_query.py -m integration -x -q` | ✅ (07-01 T3) | ⬜ pending |
| T3 | 07-04 | 2 | OBS-10 | T-07-12 | `TracingKubernetesPodOperator.build_pod_request_obj()` injects a well-formed, per-execution W3C `traceparent` env var into `ingest`'s launched pod spec only when an Airflow-managed span is active, and injects nothing when tracing is disabled; `discover` stays untouched (D-12) | unit (in-memory tracer, no live cluster) | `pytest tests/unit -k tracing_kpo -q` | ✅ (07-04 T3) | ⬜ pending |
| T1-T3 | 07-05 | 2 | OBS-10 | T-07-14 | `dataplat.cli.main()` extracts an incoming `TRACEPARENT` before any span is created; `run_ingest()`'s own span (and its nested `pipeline.publish` child span, closing OBS-10's "→ PostgreSQL" segment) are genuine children of that context, and `meta.ingestion_runs.trace_id` equals the incoming trace ID while `span_id` is a genuinely new value — proven first offline (unit), then against a real database (integration) | unit + integration | `pytest tests/unit -k "cli_trace or run_ingest_trace" -q && pytest tests/integration/test_run_ingest.py -m integration -x -q -k trace` | ✅ (07-05 T1-T3) | ⬜ pending |
| T2 | 07-08 | 4 | OBS-10 | — | A real `csv_ingest_customers` DagRun's `ingest` pod carries a well-formed `TRACEPARENT` in its live spec, and `meta.ingestion_runs.trace_id` for that run equals the trace ID encoded in it — proof over prose, against the real cluster | e2e, `cluster` marker (real pod) | `pytest tests/e2e/observability/test_trace_propagation.py -m cluster -x -q` | ✅ (07-08 T2) | ⬜ pending |
| T3 | 07-08 | 4 | OBS-01 / OBS-09 / D-20 | T-07-23 | A real, forced freshness breach (fail-tier per Plan 07-07's two-severity alert rules) causes Grafana's Alerting engine to actually POST to an in-cluster webhook receiver with a payload naming the breaching dataset — not merely an assertion that the underlying SQL predicate is true; all mutated live state (Vault secret, `meta.datasets` row, Kubernetes Secret) restored in `finally` | e2e, `cluster` marker | `pytest tests/e2e/observability/test_alert_webhook_delivery.py -m cluster -x -q` | ✅ (07-08 T3) | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

All Wave-0 gaps identified during research are now covered by a concrete plan/task/wave assignment
above — none remain unassigned. Test-file creation happens as part of each plan's own implementing
task (the same-plan route: the plan that wires a capability also creates the test proving it), not
as a separate pre-planning pass:

- [x] `tests/e2e/observability/__init__.py`, `tests/e2e/observability/conftest.py` (including the in-cluster `webhook_receiver` fixture) — plan 07-08, Task 1, Wave 4
- [x] `tests/e2e/observability/test_trace_propagation.py` — plan 07-08, Task 2, Wave 4
- [x] `tests/e2e/observability/test_alert_webhook_delivery.py` — plan 07-08, Task 3, Wave 4
- [x] `tests/integration/test_lineage_view.py` — plan 07-01, Task 3, Wave 1
- [x] `tests/integration/test_freshness_query.py` — plan 07-01, Task 3, Wave 1
- [x] `tests/integration/test_metrics_otlp.py` — plan 07-02, Task 3, Wave 1
- [x] Framework install: none — pytest/testcontainers already present via the `cluster` dependency group

---

## Manual-Only Verifications

*None — all 5 phase behaviors (OBS-07/08/09/10, D-20) have an automated-test path per the map
above, confirmed against the real plans now that they exist, including the live-proof bar
CONTEXT.md D-19/D-20 require.*

---

## Validation Sign-Off

- [x] All tasks have `<acceptance_criteria>` with automated commands/behavior assertions
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < ~10s (offline unit-tier gate); `cluster`-marked e2e tier explicitly exempted as phase-gate-only, bounded instead by Plan 07-07's `interval: 5m` alert-evaluation cadence
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** finalized during planning (`gsd-planner`, revision iteration 1, 2026-08-15) — real
Task/Plan/Wave IDs assigned above for all 8 plans across 4 waves, covering all 5 phase requirement
IDs (OBS-01, OBS-07, OBS-08, OBS-09, OBS-10) plus D-20, with zero gaps.
