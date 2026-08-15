# Phase 7: Observability, Metrics, Tracing & Lineage - Context

**Gathered:** 2026-08-15
**Status:** Ready for planning

<domain>
## Phase Boundary

The question "where did this row come from, and is the feed healthy?" becomes answerable by SQL and by dashboard, and a single trace spans Airflow task → task pod → processor → PostgreSQL (ROADMAP goal). Concretely: wire the existing no-op `dataplat.observability.metrics`/`tracing` seams to real backends (OBS-08/OBS-10); deploy Prometheus + Grafana + a tracing backend + an OTel Collector via Helm; write a SQL lineage view over the already-complete `meta`/`normalized` schema (OBS-07); add net-new data-freshness tracking with configurable warn/fail behavior (OBS-01/OBS-09); and inject W3C trace context across the Airflow→KPO-pod boundary, which is explicitly not automatic.

Phase 3 deliberately built this phase's landing zone in advance (03-CONTEXT.md D-03): `metrics.increment("rows_rejected"/"rows_kept", n)` and `tracing.start_span("pipeline.run_streaming.chunk")` are real, stable call sites already threaded through `pipeline/engine.py` — they do nothing today. Phase 3's schema design also embedded every lineage/trace column this phase needs directly on `meta.ingestion_runs` and `normalized.customers` (verified live against the actual migrations during this discussion) — so OBS-07 requires zero new columns, only a view.

**Out of scope** — belongs to other phases: `meta.record_lineage` (the opt-in table for target-row≠source-row lineage — aggregations, SCD2 collapses; ARCHITECTURE.md places it in a later "Operations phase," likely alongside Phase 10's SCD work); operational runbooks (OBS-06 — Phase 11); OpenLineage export (V2-OBS-01, explicitly v2 in PROJECT.md/ROADMAP.md); any pipeline/parsing/validation logic changes (Phase 6/8 territory).

</domain>

<decisions>
## Implementation Decisions

### Runtime Metrics Backend (OBS-08)
- **D-01:** `dataplat.observability.metrics.increment()` wires to **OTLP**, not StatsD. Matches ROADMAP.md's own Phase 7 plan guidance verbatim ("runtime metrics push via OTLP") — reuses the OTel Collector this phase deploys anyway for tracing (OBS-10), so one collector receives both traces and `dataplat`'s own metrics.
- **D-02:** Airflow's own internal metrics (scheduler heartbeat, DAG parse time, etc.) stay on **StatsD → statsd-exporter → Prometheus**, not OTel — two separate pipelines converge on the same Prometheus rather than one unified OTel pipeline. Rationale (user-selected, following STACK.md's explicit reasoning): "Mature path, existing community Grafana dashboards, a well-understood mapping config. OTel metrics from Airflow work but you would be re-deriving every dashboard for no gain." Rejected alternative: one unified OTel pipeline for both Airflow and `dataplat` — forfeits the pre-built StatsD-mapping community dashboards for a simpler mental model.
- **D-03:** The 8 named dashboard metrics (`files_processed`, `files_failed`, `rows_processed`, `rows_invalid`, `rows_deduplicated`, `processing_duration`, `validation_failures`, `data_freshness`) are **hybrid**: historical/exact figures come from Postgres via a **Grafana Postgres datasource** (all 8 are already derivable from existing `meta.ingestion_runs`/`meta.datasets` columns — `rows_read`/`rows_valid`/`rows_invalid`/`rows_deduplicated`/`rows_loaded`/`duration_ms`/`started_at`/`finished_at`/`status`), **plus** three specific signals also get a live OTLP/Prometheus gauge for lower-latency visibility: **runs currently in-flight**, **recent failure rate**, and **row-level reject rate** (the already-threaded `rows_rejected`/`rows_kept` counters in `RaggedRowGuard.apply()`).
- **D-04:** Bounded label set for `dataplat`'s OTLP counters/gauges is **`dataset` + `stage` + `status`** — still bounded (status is a small fixed enum) per PITFALLS #12's rule that unbounded identity (file_id/run_id/batch_id) must never become a metric label and lives only in the metadata DB.

### Alerting (spans metrics + freshness)
- **D-05:** Real alert rules with actual notifications are **in scope** for this phase — not just visual dashboard panels — covering both the live metrics gauges (D-03's three signals) and data-freshness breaches (below).
- **D-06:** Notification channel is a **generic webhook**, not a vendor-specific integration — works with Slack/Discord/ntfy.sh/a local script/log-catcher without committing to one vendor. The webhook URL is a **Vault-backed credential via the `vault://` scheme** (Phase 5's `SecretsResolver`) — never hardcoded, per §81.
- **D-07:** **One alerting engine, not two**: Grafana's native unified alerting evaluates BOTH the Postgres-datasource freshness condition AND the Prometheus-datasource live gauges, through the same rule engine and the same webhook contact point. kube-prometheus-stack's bundled Alertmanager is **not used** — Grafana Alerting can already evaluate rules against any configured datasource, making a second alerting engine redundant.

### Data Freshness Tracking (OBS-01, OBS-09)
- **D-08:** Expected delivery frequency is declared as a **new field in `configs/datasets/*.yaml`** (e.g. `freshness: {expected_frequency, warn_after, fail_after}`), flowing through the existing `ConfigRegistry.sync()` merge into new **nullable** columns on `meta.datasets`. Optional per dataset — a dataset with no `freshness:` block configured never triggers a freshness check at all, which is what makes OBS-09's "no file currently available" (stays quiet) structurally distinct from "file expected but missing" (alerts), not a special-cased flag. Rejected alternative: a separate versioned table mirroring `config_versions` — no current dataset needs SLA history, so this is unbuilt until one does.
- **D-09:** Freshness breaches fire through the **same alert path** as D-05/D-06/D-07 — one alerting mechanism for both "pipeline broke" and "feed went quiet."
- **D-10:** Freshness is evaluated by a **Grafana Alert rule querying Postgres directly** (a scheduled SQL condition against `meta.datasets`/`meta.ingestion_runs`) — no new Airflow DAG, no new evaluation code. Rejected alternative: a dedicated `check_freshness` DAG writing an explicit status row — more consistent with "everything auditable in the metadata DB," but adds a DAG and evaluation code this phase would otherwise skip, and isn't required by any requirement's literal wording.

### Tracing Backend & Trace Scope (OBS-10)
- **D-11:** Tracing backend is **Grafana Tempo** (single-binary chart) — native Grafana integration (trace↔log↔metric correlation without a second product to learn), receives OTLP from the same OTel Collector `dataplat`'s metrics use (D-01). Not Jaeger.
- **D-12:** Trace root is the **mapped per-file KubernetesPodOperator task instance**, not the `S3KeySensor`/`discover_files` task. One trace per `meta.ingestion_runs` row — matching `trace_id`'s actual schema shape (one column per run row, not per DAG run) — keeps each file's trace independently searchable in Tempo even when Dynamic Task Mapping fans one DAG run out to many files in parallel. Directly matches the Core Value framing ("where did **this row** come from").

### Lineage Query Interface (OBS-07)
- **D-13:** Lineage is exposed as **one wide SQL view per target table** (e.g. `meta.v_customers_lineage` today), joining that table's embedded lineage columns (`_run_id`/`_file_id`/`_batch_id`, per ARCHITECTURE.md §2.3's "embed lineage as columns on target tables" design) out to the full `meta.*` chain (`ingestion_runs` → `files` → `batches` → `config_versions` → `schema_versions`) in one query — directly satisfies OBS-07's literal wording ("one SQL query returns... for any row..."). This is a **repeatable pattern**, not a one-off: each future `normalized.*`/`warehouse.*` table gets the same shape without new design work. Verified live during this discussion: no new schema is needed — every column OBS-07 names already exists (migrations 0002/0004/0005). This decision was reached by consulting ARCHITECTURE.md §2.3 directly at the user's request rather than picking between the two originally-framed options blind.
- **D-14:** The opt-in `meta.record_lineage` table (ARCHITECTURE.md §2.3, for the target-row≠source-row case — aggregations, SCD2 collapses) is **explicitly out of Phase 7 scope**. ARCHITECTURE.md's own phase table places it in a later "Operations phase," and no current table needs it — `customers` is a 1:1 row mapping.
- **D-15:** **No convenience CLI/make target** for lineage lookups this phase — stays SQL-only (`meta.v_customers_lineage` is directly queryable via psql/any SQL client/Grafana Explore). Matches Phase 6's own D-06 precedent exactly ("SQL-queryable directly... a convenience CLI/make target... considered and deferred") and OBS-07's literal wording ("queryable by SQL," no more).

### Helm Profile, Persistence & Retention
- **D-16 (Claude's discretion — see below):** Monitoring-stack CI treatment was left to researcher/planner.
- **D-17:** Prometheus/Tempo/Grafana data is **persistent** (PVCs), surviving a `cluster-down`/`cluster-up` cycle — same treatment as Vault's storage, consistent with this project's own concern for restart-survival on a host where a WSL2/Docker restart is a documented realistic event.
- **D-18:** Retention is **short — Prometheus ~15 days, Tempo ~7 days**. Rationale: `meta.ingestion_runs` and its neighbors are the actual permanent record per this project's own architecture; Prometheus/Tempo/Grafana only need to cover a recent operational window, not serve as a second source of truth. Matches ROADMAP.md's own Phase 7 plan guidance verbatim ("Prometheus/Grafana/Tempo hold no data-correctness state"). Rejected alternative: a longer ~90d/~30d window for historical trend comparison — more disk pressure on a long-running local dev box for a capability nothing currently needs.

### Automated Proof Depth
- **D-19:** Phase 7 gets a **permanent automated test suite** proving the observability mechanisms actually work, at the same bar Phase 4 (04-CONTEXT.md D-10, real `kubectl delete pod`) and Phase 5 (05-CONTEXT.md D-03, live credential rotation) already set for this project — "proof over prose." Not just visual Grafana verification.
- **D-20:** The alerting path specifically gets a **live webhook-delivery test**: force a real freshness breach, wait for Grafana Alerting to actually fire, assert an HTTP POST arrives at a test-local webhook receiver with the expected payload. Not just an assertion that the underlying SQL predicate is true — proves the entire chain (Postgres condition → Grafana evaluation → contact point → webhook delivery), matching the "real, not simulated" bar Phase 4/5 established.

### Claude's Discretion
- **D-16's actual mechanism:** user explicitly deferred. Lean toward a template/lint-only CI check (`helm template` + `kubeconform` against a trimmed monitoring values file) rather than actually deploying the stack on the CI runner — consistent with how other charts are already validated in CI per `.claude/CLAUDE.md` §I, while still respecting PROJECT.md's constraint that the CI profile itself has monitoring disabled (no actual Prometheus/Grafana/Tempo pods on the 4 CPU/16 GB runner).
- Exact W3C `traceparent` injection mechanics — which function performs it (most likely extending `airflow/dags/_common/kpo.py`'s `common_kpo_kwargs()`, which already injects `vault://` env-var references per Phase 5) and how the mapped task's own span context is read via `airflow.sdk.observability.trace`. CLAUDE.md/STACK.md already establish this must be DIY env-var injection since cross-process propagation is not automatic; the exact code shape is implementation detail.
- Confirmed, not re-litigated: modifying `airflow/dags/_common/kpo.py` to inject a `TRACEPARENT` env var does **not** conflict with Phase 3 D-03's "no pipeline code changes" framing — that promise was about `dataplat`/`csv_processor` PIPELINE library internals specifically (a different codebase per `docs/adr/0004-two-images-two-dependency-sets.md`), not the Airflow DAG orchestration layer, which necessarily needs to inject trace context to satisfy success criterion 3.
- Exact Grafana dashboard panel design beyond the data-source split already locked (D-03) — panel layout, chart types, one dashboard vs. multiple tabs/views.
- Exact schema shape of the new `meta.datasets` freshness columns (D-08) — column names/types (likely `interval`/duration types for `expected_frequency`/`warn_after`/`fail_after`), and whether `config_schema_version` needs bumping.
- Cold-start behavior for a dataset with freshness configured but zero `ingestion_runs` history yet (no prior file ever received) — a reasonable grace-period default (e.g., measured from `meta.datasets.created_at`) is left to planner, not re-litigated here.
- Exact retention/replica sizing numbers for the kube-prometheus-stack/Tempo Helm values beyond the day-count targets in D-18.
- Whether the OTel Collector is deployed standalone (`open-telemetry/opentelemetry-collector` chart, pinned `0.169.0` in `.claude/CLAUDE.md`'s Installation section) versus the Airflow chart's own optional bundled collector — the standalone chart is the far more likely fit since `dataplat`/`csv-processor` pods live outside the Airflow Helm release entirely and need a cluster-reachable collector Service, but the exact values wiring is implementation detail.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase scope, requirements and success criteria
- `.planning/ROADMAP.md` § "Phase 7: Observability, Metrics, Tracing & Lineage" — goal, 4 success criteria, requirements list (OBS-01, OBS-07, OBS-08, OBS-09, OBS-10), and the full plan-guidance block (StatsD-XOR-OTel constraint for Airflow, "business metrics via Postgres datasource, runtime metrics push via OTLP" — the exact text D-01/D-02/D-03 follow, no Prometheus Pushgateway, lineage-is-a-view-not-a-store framing, OpenLineage explicitly v2).
- `.planning/REQUIREMENTS.md` — OBS-01 (line 168), OBS-07 (174), OBS-08 (175), OBS-09 (176), OBS-10 (177), plus the traceability table (385–394) confirming OBS-02/03/04/05 already validated in Phase 3 and OBS-06 (runbooks) belongs to Phase 11, not this one.

### Stack pins and the StatsD/OTel/Tempo decision space
- `.claude/CLAUDE.md` §H "Observability" — `kube-prometheus-stack` 88.2.0 pin, Grafana chart repo URL change (`grafana-community.github.io`), `statsd-exporter` v0.30.0, the StatsD-vs-OTel mutual exclusion for Airflow's own metrics with rationale, "business metrics → analytical DB → Grafana Postgres datasource" framing, the Tempo-or-Jaeger tracing-backend note ("budget for it"), the explicit "trace context does NOT propagate into KubernetesPodOperator pods automatically... you must do it yourself" warning, and "there is no tracing backend in kube-prometheus-stack."
- `.planning/research/ARCHITECTURE.md` §2.3 "Record-level lineage — the one place to resist a table" (lines 247–276) — "embed lineage as columns on target tables" recommendation D-13 follows directly; the opt-in `meta.record_lineage` table D-14 explicitly defers, including its placement in a later "Operations phase" (line 276).
- `.planning/research/PITFALLS.md` — item #12 (unbounded identity must live only in the metadata DB, never a Prometheus/metric label) — grounds D-04's bounded label-set decision.
- `.planning/research/SUMMARY.md` — deviation D5 ("Observability promoted to an explicit stage... the seams (`observability/{logging,metrics,tracing}.py`) belong in the library from the start as no-ops; the stack is a parallel stage after the slice") — the design decision this entire phase fulfills.
- `.planning/PROJECT.md` — Key Decisions: "Prometheus + Grafana + OpenTelemetry tracing" row ("User chose the most complete observability tier... justified by 'foundation for real work'"); CI runner constraint ("a trimmed single-node CI profile (monitoring disabled...)") informing D-16.

### Prior-phase decisions this phase must respect, not re-decide
- `.planning/phases/03-dataplat-core-library-metadata-control-plane/03-CONTEXT.md` D-03 — the no-op-seam design: "Phase 7 becomes a pure backend-wiring phase — no pipeline code changes, only `metrics.py`/`tracing.py` internals swap from no-op to StatsD-exporter / OTel Collector." (D-01 in this document narrows the metrics half — `dataplat` wires to OTLP specifically, per ROADMAP.md's own more specific plan guidance written after 03-CONTEXT.md.)
- `.planning/phases/04-vertical-slice-csv-to-analytical-postgresql/04-CONTEXT.md` D-09/D-10/D-11 — established the "real, not simulated" proof bar (real `kubectl delete pod`, permanent automated E2E test, polling metadata rather than sleeping) that D-19/D-20 extend to observability.
- `.planning/phases/05-vault-secrets-workload-identity/05-CONTEXT.md` D-03 — "proof over prose," live-demonstrated-mechanism bar; and the `vault://` `SecretsResolver` scheme D-06's webhook credential must use.
- `.planning/phases/06-universal-csv-engine-schema-contracts-normalization/06-CONTEXT.md` D-06 — the "SQL-queryable directly... a convenience CLI/make target... considered and deferred" precedent D-15 follows exactly.

### Existing code this phase wires (not replaces)
- `packages/dataplat/src/dataplat/observability/metrics.py`, `tracing.py`, `__init__.py` — the no-op seams this phase gives real backends.
- `packages/dataplat/src/dataplat/pipeline/engine.py` (lines 14–27, 116–117, 148) — existing call sites: `metrics.increment("rows_rejected"/"rows_kept", ...)` in `RaggedRowGuard.apply()`, `tracing.start_span("pipeline.run_streaming.chunk")` around each chunk's stage sequence.
- `packages/dataplat/src/dataplat/models/identity.py` (`RunContext`, lines 87–110) — `trace_id`/`span_id` fields already modeled, currently always `None`.
- `packages/dataplat/src/dataplat/metadata/postgres.py` (lines 45–48, ~340) — where `ingestion_runs`' `k8s_pod_name`/`trace_id`/`span_id` columns are persisted from `RunContext`.
- `airflow/dags/_common/kpo.py` — `common_kpo_kwargs()`, the existing env-var-injection helper (already used for `vault://` refs per Phase 5) this phase extends to inject a W3C `traceparent` env var into KPO pods.
- `airflow/dags/csv_ingest_customers.py` — the DAG whose mapped per-file KPO task becomes the trace root (D-12).
- `configs/datasets/customers.yaml` — gets the new `freshness:` block (D-08); currently has zero frequency-related fields.
- `migrations/versions/0001_meta_datasets_config_versions.py` — `meta.datasets`, target for the new freshness columns (D-08).
- `migrations/versions/0002_meta_files.py` — `meta.files` (`object_uri`, `content_sha256`) — a join target for the lineage view (D-13).
- `migrations/versions/0004_meta_ingestion_runs.py` — `meta.ingestion_runs` already has every lineage/trace column this phase needs (`dag_id`, `task_id`, `k8s_pod_name`, `trace_id`, `span_id`, `processor_version`, `config_version_id`, `schema_version_id`, `rows_*`, `duration_ms`, `started_at`/`finished_at`, `status`) — zero new columns needed.
- `migrations/versions/0005_normalized_customers.py` — `normalized.customers` already embeds `_run_id`/`_file_id`/`_batch_id`/`_source_row_number`/`_record_hash`/`_ingested_at` (ARCHITECTURE.md §2.3's lineage-columns design, live in the DDL) — the join source for `meta.v_customers_lineage` (D-13).
- `helm/values/{local,ci}/{airflow,cnpg-operator,minio}.yaml` — the existing two-profile Helm values pattern this phase's monitoring-stack values file(s) join; no `helm/values/*/monitoring.yaml`-equivalent exists yet.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `dataplat.observability.metrics.increment()` / `tracing.start_span()` — real, stable call sites already threaded through `pipeline/engine.py`; wiring their internals is this phase's core deliverable for OBS-08/OBS-10, no caller changes needed.
- `airflow/dags/_common/kpo.py`'s `common_kpo_kwargs()` — the established env-var-injection pattern (already carries `vault://` refs) this phase extends for W3C `traceparent` propagation.
- `meta.ingestion_runs`' already-complete lineage/trace column set — nothing to add, only to populate (`trace_id`/`span_id` are currently always `NULL`) and query.

### Established Patterns
- Config-not-code + the existing `ConfigRegistry.sync()` merge (`configs/datasets/*.yaml` → `meta.datasets`) — D-08's freshness fields ride this same existing sync mechanism, no new config-loading logic needed.
- "SQL-queryable directly, convenience CLI deferred" — Phase 5 (`make vault-audit-tail`) and Phase 6 (D-06, schema-evolution proposals) both establish this project weighs CLI convenience against real need; D-15 follows the "defer" side of that same precedent.
- "Real, live-demonstrated proof, not documentation" — Phase 4 (`kubectl delete pod`) and Phase 5 (live rotation) both established this bar; D-19/D-20 extend it to observability specifically.

### Integration Points
- The OTel Collector — most likely the standalone `open-telemetry/opentelemetry-collector` chart (`.claude/CLAUDE.md`'s pinned `0.169.0`), not the Airflow chart's own optional bundled collector, since `dataplat`/`csv-processor` pods live entirely outside the Airflow Helm release and need a cluster-reachable collector Service — is the single point both Airflow's traces and `dataplat`'s OTLP metrics/traces converge on.
- Grafana's Postgres datasource plugin is the read path for all 8 dashboard metrics (D-03) and for freshness evaluation (D-10) — points directly at the analytical PostgreSQL, no new API layer.
- Grafana native unified alerting (D-07) is the single alert-rule engine for both datasources, POSTing to the generic webhook contact point (D-06) whose URL resolves through `vault://`.

</code_context>

<specifics>
## Specific Ideas

- The lineage view-shape decision (D-13) was reached by the user explicitly asking for research-backed synthesis rather than a raw pick between the two originally-framed options ("Maybe add a different data source to be more sure which strategy is better... Think, and analyze"). Consulting `ARCHITECTURE.md` §2.3 directly resolved it into the per-table wide-view pattern, which the user then confirmed against the original framing ("if you are still recommending option 1... then ok"). Downstream agents should treat D-13's phrasing as the authoritative resolution, not the earlier two-option framing in isolation.
- The "real, not simulated" proof bar (D-19/D-20) was explicitly framed by the user as continuing what Phase 4 and Phase 5 already established for themselves, not a new standard invented for this phase — planning should read this as a strong signal to keep matching that bar in later phases too.

</specifics>

<deferred>
## Deferred Ideas

- **`meta.record_lineage`** (the opt-in target-row≠source-row lineage table) — explicitly out of scope per D-14; ARCHITECTURE.md places it in a later "Operations phase." Revisit whenever a table with target-row≠source-row semantics first ships (aggregations, or Phase 10's SCD2 collapses).
- **A convenience CLI/make target for lineage lookups** — deferred per D-15, following Phase 6's own precedent. Revisit if direct SQL querying proves painful in practice.
- **kube-prometheus-stack's bundled Alertmanager** — considered and explicitly not used (D-07); Grafana's native alerting supersedes it for this phase's needs. Revisit only if a future need specifically requires Alertmanager's own routing-tree features that Grafana's native alerting can't express.
- **Longer Prometheus/Tempo retention (~90d/~30d)** — considered, user chose the shorter window (D-18). Revisit if a real historical-trend-comparison need appears.
- **OpenLineage export** — already explicitly v2 per `PROJECT.md`/`ROADMAP.md` (`V2-OBS-01`); not re-raised in this discussion.

None of the above are scope creep in the "new capability" sense — discussion stayed entirely within Phase 7's domain. These are implementation alternatives explicitly considered and not chosen, recorded so they aren't silently re-litigated later.

</deferred>

---

*Phase: 7-observability-metrics-tracing-lineage*
*Context gathered: 2026-08-15*
