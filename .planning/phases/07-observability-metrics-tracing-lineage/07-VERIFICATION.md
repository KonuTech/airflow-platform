---
phase: 07-observability-metrics-tracing-lineage
verified: 2026-08-16T12:55:00Z
status: gaps_found
score: 13/14 must-haves verified
overrides_applied: 0
gaps:
  - truth: "One SQL query returns, for any warehouse row, its source file, object path, checksum, batch, ingestion timestamp, DAG/run/task ID, processor version, schema version and config version (ROADMAP Phase 7 Success Criterion 2 / OBS-07's literal wording / 07-01-PLAN.md's own must_haves truth #1)"
    status: failed
    reason: >
      meta.v_customers_lineage structurally SELECTs dag_id/dag_run_id/task_id (and
      map_index/k8s_namespace), and 10 of the ~13 named lineage facts (source file,
      object path, checksum, batch, ingestion timestamp, processor version, schema
      version, config version, run id, k8s_pod_name) are genuinely populated and
      correct -- independently confirmed live. But dag_id/dag_run_id/task_id
      specifically are NEVER populated by any code path, for any run, including a
      real, live, Airflow-KubernetesExecutor-triggered production run verified
      directly against the running cluster during this verification pass (run_id
      33613/33614, dag_id/dag_run_id/task_id all empty/NULL). This is not a test
      artifact of an out-of-Airflow fixture -- it reproduces for genuine live
      production data. tests/integration/test_lineage_view.py codifies this as
      expected (`assert row["dag_id"] is None`, same for dag_run_id/task_id) rather
      than flagging it as unresolved, and no D-08..D-20 decision in 07-CONTEXT.md
      documents this as an intentional deferral -- D-13 assumed OBS-07 was fully
      satisfied because the *columns* already existed (migration 0004), without
      verifying they are ever *written*. No later phase (8-11) claims this gap in
      its own goal or success criteria.
    artifacts:
      - path: "packages/dataplat/src/dataplat/metadata/postgres.py"
        issue: "claim_ingestion_run()'s UPDATE SET clause sets status/try_number/k8s_pod_name/trace_id/span_id/started_at/lease_expires_at but never dag_id/dag_run_id/task_id/map_index/k8s_namespace -- even though all five already exist in the same file's _INGESTION_RUN_UPDATABLE_FIELDS frozenset (line ~30-58) and finalize_publication() also never sets them."
      - path: "packages/dataplat/src/dataplat/models/identity.py"
        issue: "RunContext.dag_id/.dag_run_id/.task_id are declared fields (docstring: 'when the run was triggered by Airflow') but nothing anywhere in packages/dataplat/ ever constructs a RunContext with them populated -- no AIRFLOW_CTX_*/dag-context env var or CLI argument is read anywhere in the package."
      - path: "airflow/dags/_common/kpo.py"
        issue: "common_kpo_kwargs()'s env_vars list carries only static values (S3/Vault/OTEL_EXPORTER_OTLP_ENDPOINT). Confirmed live against a running ingest pod's full env list: Airflow's KubernetesPodOperator does NOT auto-inject AIRFLOW_CTX_DAG_ID/AIRFLOW_CTX_TASK_ID/AIRFLOW_CTX_DAG_RUN_ID (zero matches). Nothing carries dag_id/dag_run_id/task_id across the pod boundary the way TracingKubernetesPodOperator (07-04) already does for TRACEPARENT."
    missing:
      - "A mechanism analogous to TracingKubernetesPodOperator's TRACEPARENT injection (reading the Airflow task context already available inside build_pod_request_obj()/execute() and appending explicit env vars for dag_id/dag_run_id/task_id/map_index) -- or an equally explicit alternative -- that actually carries this identity across the pod boundary"
      - "dataplat.cli.main() or run_ingest() reading those env vars and threading them into claim_ingestion_run (widened the same way trace_id/span_id were in plan 07-05) or a call to update_ingestion_run_status (whose _INGESTION_RUN_UPDATABLE_FIELDS already accepts these column names, so no schema/DB-layer change is needed)"
      - "tests/integration/test_lineage_view.py updated to prove dag_id/dag_run_id/task_id ARE populated (via a simulated Airflow-context env), replacing its current `assert ... is None` assertions"
---

# Phase 7: Observability, Metrics, Tracing & Lineage Verification Report

**Phase Goal:** The question "where did this row come from, and is the feed healthy?" is answerable by SQL and by dashboard, and a single trace spans Airflow task to PostgreSQL
**Verified:** 2026-08-16T12:55:00Z
**Status:** gaps_found
**Re-verification:** No — initial verification

## Verification Approach Note

ROADMAP.md sets `**Mode:** mvp` on Phase 7, but the phase goal does not match the User Story format (`As a ..., I want to ..., so that ....`) — confirmed via `gsd-sdk query user-story.validate` (`valid: false`). This is a uniform, repository-wide default (every phase in this ROADMAP carries `Mode: mvp` regardless of shape) rather than a deliberate per-phase choice, consistent with how phases 5 and 6's own verification reports already handled this exact situation. This report proceeds with **standard goal-backward verification** against ROADMAP's four numbered Success Criteria and the eight PLAN.md frontmatter `must_haves` blocks (07-01 through 07-08), not the MVP User Flow Coverage format.

## Methodology Note — This Was Not a Documentation Review

A live kind cluster (`kind-airflow-platform`) was reachable throughout this verification. Rather than trusting SUMMARY.md narratives, the majority of this report's findings were derived by directly querying the running cluster: `kubectl exec`-ing into the analytical PostgreSQL pod, port-forwarding to Grafana/Prometheus/Tempo/the OTel Collector and calling their own APIs, and inspecting live pod specs. Where a specific automated test could not reasonably be re-run (a live cluster currently has a large, genuine backlog of pending `ingest` pods — see below), the underlying claim was independently re-derived through direct inspection instead of being accepted on the SUMMARY's word. One genuine, previously-undocumented gap was found this way (see Gaps below) that no SUMMARY.md, the code review (07-REVIEW.md), or any of the 8 plans' own acceptance criteria caught.

**A live, currently-active cluster condition, confirmed present during this verification:** the `etl` namespace has ~40 `ingest` pods stuck `Pending`/`NotReady` (both worker nodes at 91-95% CPU allocation), reproducing exactly the resource-exhaustion pattern already diagnosed in `deferred-items.md` under "From plan 07-08" (a `base` container completing while its `airflow-xcom-sidecar` never exits, permanently pinning the pod's resource request). This is a pre-existing, out-of-phase-scope infrastructure issue, not a Phase 7 code defect — but it does mean `tests/e2e/observability/test_trace_propagation.py` would very likely fail to schedule a fresh run if re-run right now, for the same reason it failed in the 07-08 executor's own session. Rather than burn the same amount of time reproducing that known failure, this verifier used the still-running cluster's *existing* recent state to independently prove the same underlying claim (see Truth #3 below) — which turned out to be strong enough to catch a completed run from ~20 minutes prior to this session, produced by the exact HEAD-equivalent code (see evidence).

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | **(ROADMAP SC1)** A Grafana dashboard shows all 8 named metrics (`files_processed`, `files_failed`, `rows_processed`, `rows_invalid`, `rows_deduplicated`, `processing_duration`, `validation_failures`, `data_freshness`), and Prometheus label cardinality stays bounded | ✓ VERIFIED | Live `GET /api/dashboards/uid/platform-observability` returns 11 panels, titled exactly the 8 named metrics + 3 D-03 live gauges. Live `GET /api/v1/query_range?query=rows_kept_total` (Prometheus) returns real series labeled only `dataset="customers", stage="ragged_row_guard", status="kept"` — no `run_id`/`file_id`/`batch_id`, confirming D-04's bounded set reaches the wire for real. |
| 2 | **(ROADMAP SC2 / OBS-07)** One SQL query returns, for any warehouse row, its source file, object path, checksum, batch, ingestion timestamp, DAG/run/task ID, processor version, schema version and config version | ✗ **FAILED** | `meta.v_customers_lineage` genuinely exists, is `SELECT`-granted to `grafana_reader`/`etl_app`, and returns real, correct values for 10 of ~13 named facts (verified live: `object_uri`, `content_sha256`, `batch_id`/`batch_key`, `run_started_at`, `processor_version`, `config_version`/`config_hash`, `run_id`, `k8s_pod_name`). **`dag_id`/`dag_run_id`/`task_id` are always NULL — for every row, including genuine live Airflow-triggered production runs (see Gaps below).** |
| 3 | **(ROADMAP SC3 / OBS-10)** A single trace spans Airflow task → task pod → processor → PostgreSQL for one ingestion run, with the context crossing the pod boundary | ✓ VERIFIED | **Independently reproduced live, right now, going beyond the blocked pytest file.** Live pod `ingest-h89qbfgz`'s env carries `TRACEPARENT=00-b162b63ff1e350786c2483188e15dbc3-136d209db528010b-01`. `meta.ingestion_runs` (run_id 33613, same pod, `SUCCEEDED`) shows `trace_id=b162b63ff1e350786c2483188e15dbc3` (exact match) and `span_id=1e2e5967ed0151c2` (genuinely different from the parent's `span_id`, proving a real child span, not a copy). Tempo's own `GET /api/traces/b162b63ff1e350786c2483188e15dbc3` returns a real trace containing `pipeline.run_ingest` (spanId matches the DB's `span_id`) with `parentSpanId` pointing at a span this verifier did not create (the Airflow task span), plus `pipeline.publish` and `pipeline.run_streaming.chunk` correctly nested as children of `pipeline.run_ingest`. Confirmed this evidence reflects current code: the deployed `csv-processor` image is built from commit `186fded`, and `git log --oneline 186fded..HEAD -- packages/dataplat/` shows only a docstring-only commit since. |
| 4 | **(ROADMAP SC4 / OBS-01/OBS-09)** A dataset whose file is overdue reports "expected but missing", a dataset with no expected arrival reports "none available" and stays quiet, each with configurable warn-or-fail behaviour | ✓ VERIFIED | Live: `meta.datasets` has `expected_frequency=1 day`, `freshness_warn_after=02:00:00`, `freshness_fail_after=06:00:00` for `customers`, populated via a genuine `ConfigRegistry.sync()` call (not seeded). Live Grafana alert rules `freshness-warn` (`severity=warning`) and `freshness-fail` (`severity=critical`) embed the exact `WHERE d.expected_frequency IS NOT NULL` / cold-start-`COALESCE` SQL text, byte-identical to `tests/integration/test_freshness_query.py`'s tested query (re-run live by this verifier, passing). The structural NULL-exclusion (`expected_frequency IS NULL` never enters the breach condition) is proven by that same integration test. |
| 5 | `dataplat.observability.metrics.increment()`/`tracing.start_span()` are genuine no-ops until `configure()` sees a real endpoint; when configured, real bounded-label data reaches OTLP | ✓ VERIFIED | Code read in full (`metrics.py`/`tracing.py`): module-owned provider singleton, `NoOpTracerProvider`/`NoOpMeterProvider` when unconfigured. `uv run pytest tests/unit/observability -q` re-run by this verifier: 9/9 passing. Live Prometheus data (Truth #1) independently proves the "configured" path reaches the wire with real, bounded labels. |
| 6 | OTel Collector + Tempo are running, persistent, and the Collector accepts OTLP (4317/4318) and exposes a Prometheus-scrapeable endpoint | ✓ VERIFIED | Live: `otel-collector-opentelemetry-collector-...` and `tempo-0` both `Running` in `monitoring`. Prometheus `/api/v1/targets` shows job `otel-collector-opentelemetry-collector` `health: up`. Tempo genuinely served a real stored trace on direct query (Truth #3). |
| 7 | Custom Airflow image genuinely contains `opentelemetry`; `TracingKubernetesPodOperator` injects `TRACEPARENT` only into `ingest`'s pods; `discover` stays a plain `KubernetesPodOperator` | ✓ VERIFIED | Live: `kubectl exec` into the running `airflow-scheduler` pod's `pip list` shows `opentelemetry-sdk 1.43.0` etc. `airflow/dags/csv_ingest_customers.py` (current source) shows `discover = KubernetesPodOperator(...)` and `ingest = TracingKubernetesPodOperator.partial(...)`. Live ingest pods carry `TRACEPARENT`; no live `discover` pod was observed carrying one (none currently running to check directly, but the source-code split is unambiguous and `tracing_kpo.py` is the only place `TRACEPARENT` is ever appended). |
| 8 | `dataplat.cli.main()` extracts `TRACEPARENT` before any span/plugin load; `run_ingest()`'s own span is a genuine child; `runs_started`/`runs_finished` emitted on every claimed-run exit path (never on a refused claim) | ✓ VERIFIED | `cli.py` read in full: `tracing.configure`/`metrics.configure`/`_extract_incoming_trace_context()` all run before the `entry_points` loop; `finally` block flushes both providers unconditionally. `run.py` read in full: `with tracing.start_span("pipeline.run_ingest")` wraps the claim call; `run_status="failed"` default with `finally`-only emission (never `except`) matches the "catches nothing" contract. `uv run pytest tests/unit -k "trace or tracing" -q`: 21/21 passing (independently re-run). |
| 9 | `make vault-bootstrap` materializes `grafana-alert-webhook` (K8s Secret, `monitoring`) with `GRAFANA_DB_PASSWORD`/`GRAFANA_ALERT_WEBHOOK_URL` sourced from Vault KV; idempotent | ✓ VERIFIED | Live: Grafana's `analytics-postgres` datasource passes its own health check (proves the DB password resolved correctly from the Secret); the `platform-webhook` contact point resolves a real (placeholder) URL from the same Secret, not a literal. `tests/e2e/vault/test_grafana_secrets.py` exists (idempotency + fail-closed + never-print assertions per 07-06-SUMMARY.md); not independently re-run live this session (would rotate/touch live Vault state for a fact already corroborated by the working datasource). |
| 10 | A live Grafana has exactly 3 healthy datasources (Postgres/Prometheus/Tempo); Prometheus genuinely scrapes the OTel Collector's exporter with real, non-empty data | ✓ VERIFIED | Live `GET /api/datasources`: exactly `analytics-postgres`/`prometheus`/`tempo`. All 3 pass `GET .../health` with `status: OK`. Prometheus historically ingested `runs_started_total`/`rows_kept_total`/etc. with real bounded labels (Truth #1); the scrape target is `up` right now. |
| 11 | Two freshness alert rules (warn + fail severity, distinct labels) and 3 live-gauge rules exist, all routed to one webhook contact point resolved from the Vault-backed Secret | ✓ VERIFIED | Live `GET /api/v1/provisioning/alert-rules`: 5 rules — `freshness-warn` (`severity: warning`), `freshness-fail` (`severity: critical`), plus 3 gauge rules. `GET /api/v1/provisioning/policies`: default policy routes to `platform-webhook`. Contact point exists, type `webhook`. |
| 12 | A real, forced freshness breach causes Grafana Alerting to deliver a genuine HTTP POST to an in-cluster receiver (D-20), with all mutated state restored afterward | ✓ VERIFIED (strong corroboration, not independently re-executed) | `tests/e2e/observability/test_alert_webhook_delivery.py` (467 lines) is a real, substantive force/observe/restore-in-`finally` test — read in full: it parses Grafana's actual webhook JSON structurally (`alerts[].labels.severity`), never a fragile substring match. 07-08-SUMMARY.md documents a specific, detailed live pass (`1 passed, 3 warnings in 472.95s`). This verifier independently confirmed the POST-test state is consistent with successful restoration: the live contact point right now resolves the *original* placeholder URL, not the test's throwaway in-cluster receiver — which is exactly what a correctly-executed `finally` restore would leave behind. Not re-executed live this session (an ~8-minute, live-state-mutating test; the corroborating evidence above was judged sufficient given the weight of independent live evidence gathered elsewhere in this pass). |
| 13 | No stub/placeholder/anti-pattern code across the phase's touched files; automated test suites are genuinely green, not merely claimed | ✓ VERIFIED | `grep -n -E "TBD\|FIXME\|XXX\|TODO\|HACK\|PLACEHOLDER\|not yet implemented"` across 18 core phase-touched files: zero matches. Independently re-run by this verifier: `tests/unit tests/regression` 412/412 passing; `tests/integration/{test_lineage_view,test_freshness_query,test_config_registry,test_migrations,test_metrics_otlp,test_run_ingest}.py` 28/28 passing; `tests/unit -k "trace or tracing"` 21/21 passing; `tests/unit/observability` 9/9 passing. |
| 14 | `meta.ingestion_runs`/`meta.v_customers_lineage` never leak `error_detail`; `grafana_reader` is strictly `SELECT`-only on exactly the tables/view it needs | ✓ VERIFIED | Live `\dp meta.datasets`/`meta.files`/`meta.ingestion_runs`/`meta.v_customers_lineage`: `grafana_reader` holds `r` (SELECT) only, on exactly those four objects — no `INSERT`/`UPDATE`/`DELETE` anywhere. `grep -n "error_detail" migrations/versions/0012_meta_v_customers_lineage.py`: no match inside the `CREATE VIEW` SELECT list. |

**Score:** 13/14 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `migrations/versions/0010_meta_datasets_freshness.py` | 3 nullable freshness interval columns | ✓ VERIFIED | Live `\d meta.datasets` shows all 3, `interval`, nullable, populated for `customers`. |
| `migrations/versions/0011_grafana_reader_role.py` | `grafana_reader` role, `SELECT`-only | ✓ VERIFIED | Live `\du`/`\dp` confirm role + exact grant set. |
| `migrations/versions/0012_meta_v_customers_lineage.py` | `meta.v_customers_lineage` view | ✓ VERIFIED (with the DAG/run/task ID gap noted above) | Live `\d`/`SELECT * ... LIMIT 1` confirm the view's full column set and real data for most columns. |
| `packages/dataplat/src/dataplat/config/model.py` (`FreshnessConfig`) | Opt-in nested config block | ✓ VERIFIED | `class FreshnessConfig` present, `extra="forbid"`/`frozen=True`; threaded into `DatasetConfig.freshness`. |
| `packages/dataplat/src/dataplat/observability/{metrics,tracing}.py` | Real OTLP-backed backends behind Phase-3 no-op signatures | ✓ VERIFIED | Full source read; substantive, non-stub; live-proven data flow. |
| `packages/dataplat/src/dataplat/pipeline/engine.py` | D-04 bounded labels on `RaggedRowGuard.apply()` | ✓ VERIFIED | `dataset=`/`stage=`/`status=` on both `rows_rejected`/`rows_kept` calls. |
| `docker/airflow/Dockerfile` | `apache-airflow[otel]` custom image | ✓ VERIFIED | Live: deployed scheduler pod's `pip list` shows real `opentelemetry-*` packages. |
| `airflow/dags/_common/tracing_kpo.py` | `TracingKubernetesPodOperator.build_pod_request_obj()` override | ✓ VERIFIED | Substantive, correctly scoped override; live pods carry the injected `TRACEPARENT`. |
| `packages/dataplat/src/dataplat/cli.py` | TRACEPARENT extraction + configure + flush wiring | ✓ VERIFIED | Confirmed ordering and `finally`-flush live in source; live Tempo/Prometheus data proves it executes correctly. |
| `packages/dataplat/src/dataplat/pipeline/run.py` | Span wrap, trace/span capture, `runs_started`/`runs_finished`, nested `pipeline.publish` span | ✓ VERIFIED | Full source read; matches live Tempo trace structure exactly. |
| `scripts/vault-bootstrap.py` (`_ensure_grafana_secrets`) | Grafana Vault-backed secrets | ✓ VERIFIED | Live Secret + working datasource/contact-point resolution. |
| `helm/values/{local,ci}/monitoring.yaml` | 3 datasources, dashboard, alerting-as-code, ServiceMonitor | ✓ VERIFIED | Fully live-deployed and queried via Grafana's own API. |
| `tests/e2e/observability/{conftest,test_trace_propagation,test_alert_webhook_delivery,test_grafana_provisioning}.py` | Live-cluster E2E proofs | ✓ VERIFIED (substantive; not all re-executed) | All 4 files read/sized; real, non-stub logic. `test_trace_propagation.py`'s specific claim independently re-derived by this verifier via direct inspection (see Truth #3); `test_alert_webhook_delivery.py` corroborated but not independently re-executed (see Truth #12). |

### Key Link Verification

| From | To | Via | Status | Details |
|------|-----|-----|--------|---------|
| `config/registry.py` | `meta.datasets` | `_resolve_dataset_id`'s `INSERT ... ON CONFLICT` | ✓ WIRED | Live data shows real `timedelta`-equivalent interval values from a genuine `sync()` call. |
| `migrations/0012` | `migrations/0011` | `down_revision` chain + `GRANT` | ✓ WIRED | Live: `alembic_version=0012`; grants present exactly as migration 0012 specifies. |
| `pipeline/engine.py` | `observability/metrics.py` | `metrics.increment(..., dataset=...)` | ✓ WIRED | Live Prometheus series carry these exact labels. |
| `helm/values/local/otel-collector.yaml` | `helm/values/local/tempo.yaml` | OTLP exporter → Tempo Service DNS | ✓ WIRED | Live: a real trace produced by `dataplat` landed in Tempo, proving the Collector→Tempo hop genuinely works, not just renders. |
| `csv_ingest_customers.py` | `tracing_kpo.py` | `TracingKubernetesPodOperator.partial(...)` | ✓ WIRED | Source-confirmed; live pods carry the resulting `TRACEPARENT`. |
| `helm/values/local/airflow.yaml` | `docker/airflow/Dockerfile` | `defaultAirflowTag` → built image | ✓ WIRED | Live: deployed image `localhost:5001/airflow:7403b96` is a real, git-SHA-tagged build containing OTel packages. |
| `pipeline/run.py` | `metadata/postgres.py` | `claim_ingestion_run(..., trace_id=...)` | ✓ WIRED | Live DB rows show `trace_id`/`span_id` populated exactly as this call would produce. |
| `pipeline/run.py` | `load/publish/protocol.py` | `pipeline.publish` span wrapping the transaction | ✓ WIRED | Live Tempo trace shows `pipeline.publish` as a genuine child of `pipeline.run_ingest`. |
| `helm/values/local/monitoring.yaml` | `grafana_reader` credential | `envFromSecret: grafana-alert-webhook` | ✓ WIRED | Live: `analytics-postgres` datasource health check passes. |
| `helm/values/local/monitoring.yaml` | `helm/values/local/otel-collector.yaml` | `additionalServiceMonitors` | ✓ WIRED | Live: Prometheus target `health: up`; historical scraped series confirm real end-to-end data flow. |
| `airflow/dags/_common/kpo.py` | `meta.ingestion_runs.dag_id/.dag_run_id/.task_id` | *(no such link exists)* | ✗ **NOT WIRED** | See Gaps — nothing carries Airflow DAG/run/task identity across the pod boundary or into the metadata DB. |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|---------------------|--------|
| Grafana dashboard "Platform Observability" (8 metrics panels) | Postgres query results | `meta.ingestion_runs`/`meta.datasets` via `analytics-postgres` datasource | Yes — real rows exist (71 total ingestion_runs, 4 with trace context) | ✓ FLOWING |
| Grafana dashboard (3 live-gauge panels) | Prometheus query results | `runs_started_total`/`runs_finished_total`/`rows_rejected_total`/`rows_kept_total` via `additionalServiceMonitors` scrape | Yes — historical real data points confirmed with correct D-04 labels; scrape target `up` | ✓ FLOWING (currently outside the 5-min instant-query lookback window due to the live cluster's ingest backlog — not a wiring defect, confirmed via range query) |
| `meta.v_customers_lineage` | `normalized.customers` + 5-table join | Real `run_ingest()` writes | Yes, for 10/13 named columns | ⚠️ PARTIAL — `dag_id`/`dag_run_id`/`task_id` structurally present but never populated (see Gaps) |
| Tempo trace store | OTel spans | `dataplat.observability.tracing` → OTel Collector → Tempo | Yes — a real, currently-stored trace was retrieved and inspected directly | ✓ FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Trace-tracing unit tests | `uv run pytest tests/unit -k "trace or tracing" -q` | `21 passed` | ✓ PASS |
| Observability unit tests | `uv run pytest tests/unit/observability -q` | `9 passed` | ✓ PASS |
| Full unit+regression suite | `uv run pytest tests/unit tests/regression -q --no-cov` | `412 passed` | ✓ PASS |
| Lineage/freshness/config/migrations/metrics/run_ingest integration | `uv run pytest tests/integration/{test_lineage_view,test_freshness_query,test_config_registry,test_migrations,test_metrics_otlp,test_run_ingest}.py -q --no-cov` | `28 passed` | ✓ PASS |
| Grafana datasource health (live) | `GET /api/datasources/uid/{analytics-postgres,prometheus,tempo}/health` | 3x `status: OK` | ✓ PASS |
| Prometheus scrape target health (live) | `GET /api/v1/targets` | `job=otel-collector-opentelemetry-collector, health=up` | ✓ PASS |
| Airflow image OTel packages (live) | `kubectl exec ... airflow-scheduler -- pip list \| grep opentelemetry` | 10 `opentelemetry-*` packages listed | ✓ PASS |
| Ingest pod carries TRACEPARENT (live) | `kubectl get pod ingest-h89qbfgz/-gicpbr67 -o json` | Well-formed `TRACEPARENT` present on both | ✓ PASS |
| Trace_id round-trip into DB (live) | `psql -c "SELECT trace_id, span_id FROM meta.ingestion_runs WHERE trace_id='b162b63ff1e350786c2483188e15dbc3'"` | 2 rows, trace_id matches pod env exactly, span_ids distinct from parent | ✓ PASS |
| Tempo stores the same trace (live) | `GET /api/traces/b162b63ff1e350786c2483188e15dbc3` | Real trace with correctly-nested `pipeline.run_ingest`/`pipeline.publish`/`pipeline.run_streaming.chunk` spans | ✓ PASS |
| `dag_id`/`dag_run_id`/`task_id` populated for a real run (live) | `psql -c "SELECT dag_id, dag_run_id, task_id FROM meta.ingestion_runs WHERE run_id IN (33613,33614)"` | All 6 values empty/NULL | ✗ **FAIL** (see Gaps) |

### Probe Execution

SKIPPED — no `scripts/*/tests/probe-*.sh` files exist and none of this phase's PLAN/SUMMARY files declare a probe-based verification mechanism.

### Requirements Coverage

| Requirement | Source Plan(s) | Description | Status | Evidence |
|-------------|-----------------|--------------|--------|----------|
| OBS-01 | 07-01, 07-06, 07-07, 07-08 | Data freshness is tracked — last received, last successful processing, expected frequency and processing delay | ✓ SATISFIED | `meta.datasets` freshness columns live-populated; WARN-tier SQL computes `processing_delay`/`last_received_at`/`last_success_at` directly; dashboard `data_freshness` panel live. |
| OBS-07 | 07-01 | Lineage is queryable by SQL — source file, object path, checksum, batch, ingestion timestamp, DAG/run/task ID, processor version, schema version, config version | ✗ **BLOCKED** | `meta.v_customers_lineage` delivers 10/13 named facts correctly and live; `dag_id`/`dag_run_id`/`task_id` are structurally present but never populated by any code path — see Gaps. |
| OBS-08 | 07-02, 07-03, 07-05, 07-07 | Platform metrics exposed with bounded label cardinality, unbounded identity in the metadata DB | ✓ SATISFIED | Live Prometheus data confirms bounded `dataset`/`stage`/`status` labels only; dashboard shows all 8 named metrics + 3 gauges. |
| OBS-09 | 07-01, 07-06, 07-07, 07-08 | "No file currently available" distinguished from "file expected but missing", configurable warn/fail | ✓ SATISFIED | Structural `expected_frequency IS NULL` exclusion proven by a passing integration test; live 2-tier alert rules (`severity: warning`/`critical`) with the exact tested SQL. |
| OBS-10 | 07-02, 07-03, 07-04, 07-05, 07-08 | Distributed traces span Airflow task → task pod → processor → PostgreSQL, via explicit W3C `traceparent` propagation | ✓ SATISFIED | Independently, directly reproduced live this session (Truth #3) — pod TRACEPARENT, DB trace_id, and Tempo's own nested span hierarchy all agree, on current (HEAD-equivalent) code. |

No orphaned requirements: all 5 IDs declared in ROADMAP.md's Phase 7 `Requirements` field are claimed by at least one of the 8 plans' frontmatter `requirements` field, and REQUIREMENTS.md's phase-mapping table lists all 5 under `Phase 7`.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `tests/policy/test_manifest_resources.py` | 217, 238 | `spec.get(field, 1)` does not substitute the default when the key is present with an explicit `null` — an uncaught `TypeError` if a future chart ever renders `replicas: null`/`instances: null` | ⚠️ Warning (from 07-REVIEW.md, independently corroborated by reading the file) | Currently dormant (neither values profile triggers it); a CI-gating policy test with an unhelpful failure mode if it ever is. |
| `packages/dataplat/src/dataplat/config/model.py` | 361-365 | `FreshnessConfig.fail_after`'s docstring claims `warn_after <= fail_after` ordering is "enforced by PostgreSQL at query time" — no such CHECK constraint or query-level comparison exists anywhere | ⚠️ Warning (from 07-REVIEW.md) | A misconfigured dataset (`warn_after > fail_after`) is silently accepted; the FAIL-tier alert could fire before the WARN-tier one. Documentation/consistency issue, not a functional blocker for this phase's own success criteria. |
| `packages/dataplat/src/dataplat/observability/{metrics,tracing}.py` | ~47-73 | `configure()` replaces the module-owned provider without shutting down the outgoing one first, leaking its background export thread if called twice with a real endpoint in one process | ⚠️ Warning (from 07-REVIEW.md; independently confirmed dormant — `cli.py` is the only call site, called exactly once per process) | Currently dormant in production; contradicts the module's own "safely re-callable" docstring claim. |
| `migrations/versions/0011_grafana_reader_role.py` | 46 | `GRANT USAGE ON SCHEMA normalized TO grafana_reader` is granted but never exercised (no direct table grant in `normalized` exists for this role) | ℹ️ Info (from 07-REVIEW.md) | Inert today; a minor least-privilege tidiness note. |
| `packages/dataplat/src/dataplat/metadata/postgres.py` / `models/identity.py` / `airflow/dags/_common/kpo.py` | see Gaps | `dag_id`/`dag_run_id`/`task_id` declared throughout the schema/model/interfaces but never populated by any write path | 🛑 **Blocker** (found independently by this verifier, not in 07-REVIEW.md) | Directly breaks ROADMAP Success Criterion 2 / OBS-07's literal wording for every real run. See Gaps for full detail. |

### Human Verification Required

None. Every must-have in this phase was verifiable programmatically — either through static code inspection, the offline test suite (independently re-run), or direct live-cluster inspection (`kubectl`, SQL, and the Grafana/Prometheus/Tempo HTTP APIs). No PLAN.md in this phase declared a deferred `<human-check>` block, and this verifier's own analysis did not surface any genuinely subjective (visual/UX/real-time-feel) item that automated evidence could not settle.

### Gaps Summary

**One genuine, previously-undocumented gap was found: `meta.ingestion_runs.dag_id`/`.dag_run_id`/`.task_id` (and therefore `meta.v_customers_lineage`'s same three columns) are never populated by any code path, for any run — including real, live, Airflow-triggered production runs independently confirmed during this verification pass.**

This directly fails the literal wording of ROADMAP Phase 7 Success Criterion 2 ("...DAG/run/task ID...") and OBS-07's own REQUIREMENTS.md description, and contradicts 07-01-PLAN.md's own must-have truth #1, which explicitly promises "get that row's ... DAG/run/task ID ...". The rest of the lineage view (10 of ~13 named facts) is genuinely, correctly, and live-provenly populated — this is a narrow but real and explicitly-named gap, not a wholesale failure of OBS-07.

Root cause: nothing in this codebase ever reads Airflow's task-identity context (confirmed live: `KubernetesPodOperator` does not auto-inject `AIRFLOW_CTX_*` env vars into launched pods) and carries it across the pod boundary the way Phase 7 already solved for `TRACEPARENT` (`TracingKubernetesPodOperator`) and could just as easily solve for `dag_id`/`dag_run_id`/`task_id` using the same, already-proven pattern. The DB-layer plumbing to accept these values already exists (`_INGESTION_RUN_UPDATABLE_FIELDS` already lists `dag_id`/`dag_run_id`/`task_id`/`map_index`) — only the "read Airflow context → inject env var → read env var → pass through" chain is missing. `tests/integration/test_lineage_view.py` currently asserts these fields ARE `None`, which will need updating alongside the fix.

This gap was not caught by: any of the 8 plans' own acceptance criteria, the 55-file code review (07-REVIEW.md, 0 critical/3 warning/1 info), or any SUMMARY.md's own self-check. It surfaced only from combining a live production data query against the running cluster with a full-repo grep for every write path touching these three columns.

**Everything else in this phase is exceptionally well-evidenced.** The trace-propagation mechanism (OBS-10, the hardest requirement in this phase) was independently, directly re-derived live — going beyond what the still-blocked `test_trace_propagation.py` pytest file alone would have proven — by cross-referencing a live pod's `TRACEPARENT` env var, the corresponding `meta.ingestion_runs` row, and Tempo's own stored trace, confirmed to reflect current (HEAD-equivalent) code via git ancestry of the deployed image tag. The metrics pipeline, freshness alerting, Grafana provisioning, and Vault-backed secrets were all independently confirmed live via their own APIs, not merely re-read from SUMMARY.md prose.

---

_Verified: 2026-08-16T12:55:00Z_
_Verifier: Claude (gsd-verifier)_
