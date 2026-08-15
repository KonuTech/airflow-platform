# Phase 7: Observability, Metrics, Tracing & Lineage - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-08-15
**Phase:** 7-observability-metrics-tracing-lineage
**Areas discussed:** Runtime metrics backend, Data freshness tracking, Tracing backend & trace scope, Lineage query interface, CI vs local Helm profile, Alerting engine consolidation, Automated proof depth

---

## Runtime Metrics Backend

| Question | Options | Selected |
|---|---|---|
| Should `metrics.increment()` push via StatsD or OTLP? | OTLP / StatsD / You decide | **OTLP** |
| Airflow's own internal metrics — StatsD or OTel? | StatsD for Airflow / OTel for Airflow too / You decide | **StatsD for Airflow** |
| 8 named dashboard metrics — Postgres-only or hybrid? | Postgres-only, all 8 / Hybrid: DB for history, live gauges for a few / You decide | **Hybrid** |
| Which signals get a live gauge? (multi-select) | Runs currently in-flight / Recent failure rate / Row-level reject rate | **All three selected** |
| Bounded label set for `dataplat`'s counters? | dataset + stage only / dataset + stage + status / You decide | **dataset + stage + status** |
| Dashboard panels only, or real alert rules with notifications? | Dashboard panels only / Real alert rules with notifications | **Real alert rules with notifications** |
| Which notification channel? | Generic webhook / Slack webhook specifically / You decide | **Generic webhook** |

**User's choice:** `dataplat` metrics → OTLP; Airflow's own metrics stay on StatsD (two pipelines, both feeding Prometheus); the 8 dashboard metrics are DB-sourced with three live-gauge exceptions; labels bounded to dataset+stage+status; real alerting via a generic webhook, credential through Vault.
**Notes:** The OTLP choice for `dataplat` directly matches ROADMAP.md's own plan-guidance text ("runtime metrics push via OTLP"), surfaced during the question. The alert-rules decision here set the direction later confirmed/extended in Data Freshness Tracking and Alerting Engine Consolidation.

---

## Data Freshness Tracking

| Question | Options | Selected |
|---|---|---|
| Where should expected delivery frequency be declared? | New YAML field, synced to meta.datasets / New dedicated table, versioned like config_versions / You decide | **New YAML field, synced to meta.datasets** |
| Should a freshness breach fire through the same alert path as failure-rate? | Yes, same alert path / Dashboard-only for freshness / You decide | **Yes, same alert path** |
| Who evaluates "is this dataset stale"? | Grafana Alerting queries Postgres directly / A dedicated Airflow DAG evaluates and records / You decide | **Grafana Alerting queries Postgres directly** |

**User's choice:** A new optional `freshness:` block in dataset YAML, synced to `meta.datasets`; breaches alert through the same webhook path as the metrics area; evaluated by Grafana Alerting directly against Postgres, no new DAG.
**Notes:** Optionality of the YAML field is what structurally satisfies OBS-09's "no expected arrival stays quiet" vs. "expected but missing" distinction — a dataset with no `freshness:` block simply never enters evaluation.

---

## Tracing Backend & Trace Scope

| Question | Options | Selected |
|---|---|---|
| Which tracing backend — Tempo or Jaeger? | Grafana Tempo (single-binary) / Jaeger / You decide | **Grafana Tempo (single-binary)** |
| Trace root: mapped per-file KPO task, or the sensor/whole DAG run? | Root at the mapped per-file KPO task / Root at the sensor, whole DAG run in one trace / You decide | **Root at the mapped per-file KPO task** |

**User's choice:** Tempo as the tracing backend; one trace per `meta.ingestion_runs` row, rooted at the mapped per-file KPO task instance.
**Notes:** The trace-scope choice was framed against `trace_id`'s actual schema shape (one column per run row) — the user's pick matches what the schema already implied rather than introducing new plumbing.

---

## Lineage Query Interface

| Question | Options | Selected |
|---|---|---|
| One wide cross-table view or narrower composable views? | One wide view / Narrower composable views / You decide | **Freeform — asked Claude to consult research docs and analyze rather than pick blind** |
| (Follow-up) Does the per-table wide-view pattern (synthesized from ARCHITECTURE.md §2.3) match what you had in mind? | Yes, lock that pattern / Something else — let me explain | **Yes, lock that pattern** (confirmed as consistent with "option 1" from the original framing) |
| Ship a convenience CLI/make target now, or stay SQL-only? | SQL-only, defer the CLI / Ship a convenience make target now | **SQL-only, defer the CLI** |

**User's choice:** A `meta.v_<table>_lineage`-style wide view per target table (starting with `meta.v_customers_lineage`), following ARCHITECTURE.md §2.3's embedded-lineage-columns design; the opt-in `meta.record_lineage` table stays deferred; no convenience CLI this phase.
**Notes:** This was the one area where the user explicitly declined to pick between the two originally-framed options and asked for research-backed analysis first ("Maybe add a different data source to be more sure which strategy is better... Think, and analyze"). Claude read `.planning/research/ARCHITECTURE.md` §2.3 live during the discussion, found it recommends embedding lineage as columns on target tables (with an opt-in `record_lineage` table reserved for a later phase), and synthesized a refined "per-table wide view" recommendation, which the user then confirmed.

---

## CI vs Local Helm Profile

| Question | Options | Selected |
|---|---|---|
| Should the monitoring stack exist only in `helm/values/local/*`, or does CI need a lint/template check? | Local-only, CI skips entirely / CI gets a template/lint-only check / You decide | **You decide** |
| Should Prometheus/Tempo/Grafana data persist across cluster-down/up? | Persistent, survives restarts / Ephemeral, rebuilt each time / You decide | **Persistent, survives restarts** |
| Retention window for persistent monitoring data? | Short (Prometheus ~15d, Tempo ~7d) / Longer (Prometheus ~90d, Tempo ~30d) / You decide | **Short (Prometheus ~15d, Tempo ~7d)** |

**User's choice:** Persistence and short retention were explicit picks; the CI validation approach itself was left to Claude's discretion.
**Notes:** Persistence choice mirrors Vault's own restart-survival treatment from Phase 5, motivated by this project's documented WSL2/Docker restart risk.

---

## Alerting Engine Consolidation

| Question | Options | Selected |
|---|---|---|
| One Grafana-native alert engine for both signal types, or two engines (Alertmanager + Grafana)? | One engine: Grafana native alerting for both / Two engines: Alertmanager for Prometheus, Grafana for Postgres / You decide | **One engine: Grafana native alerting for both** |

**User's choice:** A single Grafana native alerting engine handles both the Postgres-sourced freshness condition and the Prometheus-sourced live gauges.
**Notes:** This closed a question implicitly opened by the Data Freshness Tracking area's D-10 decision (Grafana Alerting evaluating Postgres directly) — the user chose consolidation over kube-prometheus-stack's separate bundled Alertmanager.

---

## Automated Proof Depth

| Question | Options | Selected |
|---|---|---|
| Automated test for observability mechanisms, or visual verification? | Automated test, same bar as Phase 4/5 / Visual/manual verification is enough here / You decide | **Automated test, same bar as Phase 4/5** |
| Should the alert path get a live webhook-delivery test, or just assert the SQL condition? | Live webhook delivery test / Assert the SQL condition only / You decide | **Live webhook delivery test** |

**User's choice:** A permanent automated test suite proving traces/metrics/lineage/alerts genuinely work, including a live end-to-end webhook-delivery test for the alert path.
**Notes:** Explicitly framed as continuing the "proof over prose" bar Phase 4 (real `kubectl delete pod`) and Phase 5 (live credential rotation) already set for this project — the user confirmed this framing rather than treating observability as a lower-rigor exception.

---

## Claude's Discretion

- Exact mechanism for CI Helm validation of the monitoring stack (template/lint-only vs. nothing) — user deferred; Claude leans toward a `helm template` + `kubeconform` check.
- Exact W3C `traceparent` injection code shape (which function, how the span context is read).
- Exact Grafana dashboard panel design/layout beyond the data-source split.
- Exact schema shape (column names/types) of the new `meta.datasets` freshness columns.
- Cold-start grace-period behavior for a newly-configured dataset with no ingestion history yet.
- Exact retention/replica sizing numbers beyond the day-count targets already given.
- Standalone vs. Airflow-chart-bundled OTel Collector — standalone is the far more likely fit given `dataplat`/`csv-processor` pods live outside the Airflow Helm release.

## Deferred Ideas

- `meta.record_lineage` (opt-in target-row≠source-row lineage table) — explicitly out of scope, belongs to a later "Operations phase" per ARCHITECTURE.md.
- A convenience CLI/make target for lineage lookups — deferred, following Phase 6's own precedent.
- kube-prometheus-stack's bundled Alertmanager — considered, not used; Grafana native alerting consolidates both signal types instead.
- Longer Prometheus/Tempo retention (~90d/~30d) — considered, user chose the shorter window.
- OpenLineage export — already explicitly v2 per PROJECT.md/ROADMAP.md; not re-raised.

None of the above are scope creep — discussion stayed entirely within Phase 7's domain throughout.
