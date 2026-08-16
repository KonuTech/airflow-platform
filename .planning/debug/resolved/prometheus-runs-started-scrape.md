---
status: resolved
trigger: "tests/e2e/observability/test_grafana_provisioning.py::test_prometheus_scrapes_dataplat_metrics_via_the_otel_collector fails: a real csv_processor ingestion run reaches SUCCEEDED, but a Prometheus query for `runs_started` via Grafana's proxied datasource returns an empty result vector even after polling for 180s."
created: 2026-08-16
updated: 2026-08-16T23:05:00Z
resolution:
  root_cause: "The E2E test queries PromQL for `runs_started`, the raw dataplat OTel counter instrument name. The OTel Collector's Prometheus exporter (otelcol-contrib 0.158.0, default add_metric_suffixes: true) appends `_total` to every monotonic counter per the OTel-to-Prometheus naming convention, so the actual series in Prometheus is `runs_started_total`. The full metrics pipeline (dataplat -> OTLP -> Collector -> Prometheus exporter -> ServiceMonitor -> Prometheus -> Grafana proxy) is healthy end-to-end; only the test's query string targeted the wrong series name."
  fix: "Changed the PromQL query in test_prometheus_scrapes_dataplat_metrics_via_the_otel_collector from \"runs_started\" to \"runs_started_total\" (both in the poll loop and the failure message), with an inline comment explaining the OTel Collector's counter-suffixing behavior so this isn't mistaken for a pipeline bug in the future."
  verification: "Re-ran the exact originally-failing test live against the cluster: `uv run --frozen --group cluster pytest tests/e2e/observability/test_grafana_provisioning.py::test_prometheus_scrapes_dataplat_metrics_via_the_otel_collector -m cluster -q` -> `1 passed in 72.00s`. Full `tests/e2e/observability/` suite regression re-run started in background to confirm no adjacent breakage; result pending at time of checkpoint."
  files_changed:
    - tests/e2e/observability/test_grafana_provisioning.py
  independent_confirmation: "Orchestrator-side confirmation: full `tests/e2e/observability/` suite re-run (uv run --frozen --group cluster pytest tests/e2e/observability/ -m cluster -q) -> **7 passed, 0 failed** in 491.18s. Fully green -- combined with the wait-for-files-stuck-task fix earlier this session, Phase 7's E2E observability suite now passes cleanly and repeatably."
---

## Symptoms

**Expected behavior:** After a `csv_ingest_customers` DagRun produces a `SUCCEEDED` ingestion run, Prometheus (scraped via the OTel Collector from `csv_processor`/dataplat pod metrics) should expose a `runs_started` metric with a non-empty result vector when queried through Grafana's proxied datasource (`/api/datasources/proxy/uid/prometheus/api/v1/query?query=runs_started`), within the test's 180s poll window.

**Actual behavior:** The probe ingestion run completes and reaches `SUCCEEDED` (proven — the test asserts on this itself before polling Prometheus). The subsequent Prometheus query for `runs_started` returns `{"status": "success", "data": {"resultType": "vector", "result": []}}` — an empty vector — for the entire 180s poll window, causing `pytest.fail`.

**Error messages:**
```
Failed: Prometheus never returned a result vector for `runs_started` within 180s of a SUCCEEDED run (file_id=94751) -- last response: {'status': 'success', 'data': {'resultType': 'vector', 'result': []}}. If the registered csv_processor_image Variable predates plans 07-02/07-05's OTel wiring, run `make image-csv-processor` and retry; otherwise this indicates a genuinely broken additionalServiceMonitors entry.
```
(from `tests/e2e/observability/test_grafana_provisioning.py:238`, function `test_prometheus_scrapes_dataplat_metrics_via_the_otel_collector`)

**Timeline:** Newly surfaced 2026-08-16 during a full `tests/e2e/observability/` re-run (6 passed, 1 failed) performed to confirm the fix for a separate, now-resolved issue (`.planning/debug/resolved/wait-for-files-stuck-task.md` — Vault resealing after restart). That prior issue was masking file discovery entirely, so this may be the first time this specific test has actually reached its Prometheus-polling phase against a genuinely `SUCCEEDED` run rather than failing earlier on "discovery never registered it." Not yet known whether this is a pre-existing latent gap (never actually exercised until now) or a fresh regression — needs investigation, not assumed either way.

**Reproduction:** Run `uv run --frozen --group cluster pytest tests/e2e/observability/test_grafana_provisioning.py::test_prometheus_scrapes_dataplat_metrics_via_the_otel_collector -m cluster -q`. Last run: failed as above. Underlying pipeline is currently healthy and cycling normally (`csv_ingest_customers` DAG has had 10+ consecutive clean successes), so a fresh run should reliably reach the same Prometheus-polling point.

**Candidate causes flagged by the test's own failure message (not yet verified):**
1. The registered `csv_processor_image` Airflow Variable may point at an image predating plans 07-02/07-05's OTel/metrics wiring — i.e. the actual running `csv_processor` container may not emit `runs_started` at all.
2. The `additionalServiceMonitors` entry for scraping dataplat/csv-processor pod metrics into Prometheus may be broken or misconfigured (label selector, port, path, namespace mismatch, etc.).

**Context ruling some things in/out:** Other `tests/e2e/observability/` tests pass, including ones that depend on the OTel Collector and on Prometheus/Grafana being reachable and correctly proxied (`test_forced_freshness_breach_delivers_a_real_webhook_post`, trace propagation, lineage tests) — so the OTel Collector process itself, Prometheus server, and the Grafana proxy datasource are presumably basically healthy. This narrows the likely fault to something specific to `runs_started` / the `csv_processor` pod's own metrics emission or scrape target, not the whole metrics pipeline.

## Evidence

- timestamp: 2026-08-16T21:20:00Z
  checked: helm/values/local/monitoring.yaml additionalServiceMonitors block (lines 759-793) and its own inline comments
  found: This entry was already fixed and live-reverified in a prior session (comment trail shows it was previously nested under the wrong `prometheus.prometheusSpec.` key, moved to top-level `prometheus.additionalServiceMonitors`, and re-verified via `/api/v1/targets` showing an `otel-collector` job with `health: up`). Selector `app.kubernetes.io/name: opentelemetry-collector`, namespace `monitoring`, port `prometheus` (8889), path `/metrics` -- all correct.
  implication: Candidate cause 2 (broken additionalServiceMonitors) is ruled out -- this was already fixed before this debug session started.

- timestamp: 2026-08-16T21:25:00Z
  checked: packages/dataplat/src/dataplat/observability/metrics.py and cli.py's main() finally block
  found: metrics.configure(otlp_endpoint=...) is called from OTEL_EXPORTER_OTLP_ENDPOINT env var at cli.py:164-166, and metrics.flush() is called in a `finally` block (cli.py:207-230) on every exit path, with an extensive comment trail documenting this exact "short-lived batch pod never flushes before exit" bug was already found and fixed in a prior session (plan 07-07).
  implication: The flush-before-exit bug is already fixed. Candidate cause 1 (stale csv_processor_image) needs live verification, not just code reading.

- timestamp: 2026-08-16T21:28:00Z
  checked: airflow/dags/_common/kpo.py env var wiring, and live pod spec for a fresh csv-ingest-customers-ingest-* pod (kubectl get pod -o jsonpath)
  found: OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector-opentelemetry-collector.monitoring.svc.cluster.local:4318 is correctly present on the live ingest pod's env, matching the OTel Collector's real Service DNS.
  implication: Env wiring into the pod is correct; the registered csv_processor_image IS running OTel-instrumented code (metrics really are being emitted).

- timestamp: 2026-08-16T21:32:00Z
  checked: 'Triggered a fresh csv_ingest_customers run manually, then curled http://otel-collector-opentelemetry-collector.monitoring.svc.cluster.local:8889/metrics directly (in-cluster) immediately after the ingest pod completed'
  found: 'The OTel Collector''s Prometheus exporter DOES expose the dataplat counters, but under SUFFIXED names: `runs_started_total`, `runs_finished_total`, `rows_kept_total`, `rows_rejected_total` -- not the raw instrument names (`runs_started`, etc.) the code creates via `create_counter(name)`.'
  implication: 'This is standard, expected OTel-Collector-to-Prometheus-exporter behavior (monotonic Sum/Counter instruments get a `_total` suffix appended per the OpenTelemetry-to-Prometheus metric-naming convention, exporter default `add_metric_suffixes: true`) -- not a bug in the exporter or the collector config.'

- timestamp: 2026-08-16T21:34:00Z
  checked: 'Queried Prometheus directly (in-cluster, bypassing Grafana proxy) for both `runs_started_total` and `runs_started`'
  found: '`runs_started_total` returns a real, non-empty result vector (value "1", full label set including dataset=customers, stage=run_ingest, status=running, job=otel-collector-opentelemetry-collector). `runs_started` (no suffix) returns `{"resultType":"vector","result":[]}` -- an empty vector, byte-for-byte matching the test failure symptom.'
  implication: 'CONFIRMED ROOT CAUSE. The test (tests/e2e/observability/test_grafana_provisioning.py:231) queries PromQL `runs_started`, but Prometheus only has the series under `runs_started_total`. The metric pipeline (dataplat -> OTLP -> Collector -> Prometheus exporter -> ServiceMonitor -> Prometheus -> Grafana proxy) is fully healthy end-to-end; only the test''s query string is wrong.'

- timestamp: 2026-08-16T21:36:00Z
  checked: 'grep for other consumers of the raw metric name (grafana dashboards, other tests, docs)'
  found: 'No Grafana dashboard JSON exists in the repo yet. tests/unit/observability/test_metrics.py and tests/integration/test_metrics_otlp.py assert on the dataplat-side instrument name (correctly, no suffix -- that is the real OTel metric name pre-export). Only tests/e2e/observability/test_grafana_provisioning.py queries the Prometheus-exposition-format name and gets it wrong.'
  implication: 'Fix is scoped to this one test file; no other code/config needs to change.'

## Eliminated

- hypothesis: "The registered csv_processor_image Variable predates the OTel/metrics wiring (plans 07-02/07-05), so the running container never emits runs_started at all."
  evidence: "Live curl of the OTel Collector's own /metrics (8889) immediately after a fresh triggered run shows runs_started_total=1 with dataset=customers, stage=run_ingest, status=running labels -- the metric IS being emitted, received, and exported correctly."
  timestamp: 2026-08-16T21:32:00Z

- hypothesis: "The additionalServiceMonitors entry is broken/misconfigured (label selector, port, path, namespace mismatch)."
  evidence: "Comment trail in helm/values/local/monitoring.yaml shows this was already fixed and live-reverified in a prior session; Prometheus successfully scrapes the otel-collector job (proven by runs_started_total being queryable in Prometheus itself, not just at the collector's raw /metrics endpoint)."
  timestamp: 2026-08-16T21:34:00Z

## Current Focus

reasoning_checkpoint:
  hypothesis: "The E2E test queries PromQL for the raw dataplat instrument name `runs_started`, but the OTel Collector's Prometheus exporter (default `add_metric_suffixes: true`) exposes monotonic counters with a `_total` suffix appended per the OTel-to-Prometheus naming convention, so the actual series in Prometheus is named `runs_started_total`. The bare `runs_started` name has zero matching series, producing the empty result vector the test observes -- forever, regardless of poll duration, because the series genuinely never exists under that name."
  confirming_evidence:
    - "Direct in-cluster curl of the OTel Collector's /metrics (8889) after a fresh triggered run shows `runs_started_total{dataset=\"customers\",stage=\"run_ingest\",status=\"running\"} 1` -- no bare `runs_started` series present anywhere in that same scrape output."
    - "Direct Prometheus API query for `runs_started_total` returns a real, populated result vector; the identical query for `runs_started` on the same live Prometheus, same instant, returns an empty vector -- isolating the discrepancy to the metric name alone, with everything else (scrape health, label set, timing) held constant."
  falsification_test: "If changing only the test's query string from `runs_started` to `runs_started_total` did NOT make the test pass (with no other change), this hypothesis would be wrong -- e.g. if there were also a real scrape/pipeline problem. Verified live above that `runs_started_total` already returns data right now, before any fix is applied, which is the strongest possible confirmation."
  fix_rationale: "The fix corrects the test's PromQL query to match the name Prometheus actually assigns to this counter (an inherent, correct, standard property of exporting OTel Sum instruments to Prometheus) rather than changing any application/infra code -- the pipeline itself is healthy end-to-end, so there is nothing else to fix. This addresses the root cause (query targets the wrong series name) directly, not a symptom."
  blind_spots: "Have not checked whether any future Grafana dashboard or alert rule (not yet created) would need the same `_total`-suffix awareness -- noting this in the fix commit/comment so it isn't rediscovered later. Have not exhaustively confirmed every OTel Collector version always adds this suffix (this is documented default behavior for the pinned otelcol-contrib 0.158.0 image observed live in this cluster, which is sufficient for this fix)."
next_action: "Edit tests/e2e/observability/test_grafana_provisioning.py: change the PromQL query string from \"runs_started\" to \"runs_started_total\" (and add a short comment noting the OTel Collector's Prometheus exporter counter-suffixing behavior, so this isn't mistaken for a pipeline bug again). Then re-run the test to verify it passes."
