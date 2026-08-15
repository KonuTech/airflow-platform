# Phase 7: Observability, Metrics, Tracing & Lineage - Research

**Researched:** 2026-08-15
**Domain:** OpenTelemetry (traces + metrics) wiring across Airflow/Kubernetes pod boundaries, Prometheus/Grafana/Tempo Helm deployment, Grafana-native alerting-as-code, and a SQL lineage view over an already-complete metadata schema.
**Confidence:** MEDIUM (HIGH on verified facts pulled from the live cluster, the pinned CNPG CRD schema, and a live `docker pull`/`pip list` check; MEDIUM on Helm chart values wiring, which was researched via WebSearch/WebFetch synthesis and should be spot-checked with `helm show values` at execution time)

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**Runtime Metrics Backend (OBS-08)**
- **D-01:** `dataplat.observability.metrics.increment()` wires to **OTLP**, not StatsD. Matches ROADMAP.md's own Phase 7 plan guidance verbatim ("runtime metrics push via OTLP") — reuses the OTel Collector this phase deploys anyway for tracing (OBS-10), so one collector receives both traces and `dataplat`'s own metrics.
- **D-02:** Airflow's own internal metrics (scheduler heartbeat, DAG parse time, etc.) stay on **StatsD → statsd-exporter → Prometheus**, not OTel — two separate pipelines converge on the same Prometheus rather than one unified OTel pipeline. Rationale (user-selected, following STACK.md's explicit reasoning): "Mature path, existing community Grafana dashboards, a well-understood mapping config. OTel metrics from Airflow work but you would be re-deriving every dashboard for no gain." Rejected alternative: one unified OTel pipeline for both Airflow and `dataplat` — forfeits the pre-built StatsD-mapping community dashboards for a simpler mental model.
- **D-03:** The 8 named dashboard metrics (`files_processed`, `files_failed`, `rows_processed`, `rows_invalid`, `rows_deduplicated`, `processing_duration`, `validation_failures`, `data_freshness`) are **hybrid**: historical/exact figures come from Postgres via a **Grafana Postgres datasource** (all 8 are already derivable from existing `meta.ingestion_runs`/`meta.datasets` columns — `rows_read`/`rows_valid`/`rows_invalid`/`rows_deduplicated`/`rows_loaded`/`duration_ms`/`started_at`/`finished_at`/`status`), **plus** three specific signals also get a live OTLP/Prometheus gauge for lower-latency visibility: **runs currently in-flight**, **recent failure rate**, and **row-level reject rate** (the already-threaded `rows_rejected`/`rows_kept` counters in `RaggedRowGuard.apply()`).
- **D-04:** Bounded label set for `dataplat`'s OTLP counters/gauges is **`dataset` + `stage` + `status`** — still bounded (status is a small fixed enum) per PITFALLS #12's rule that unbounded identity (file_id/run_id/batch_id) must never become a metric label and lives only in the metadata DB.

**Alerting (spans metrics + freshness)**
- **D-05:** Real alert rules with actual notifications are **in scope** for this phase — not just visual dashboard panels — covering both the live metrics gauges (D-03's three signals) and data-freshness breaches (below).
- **D-06:** Notification channel is a **generic webhook**, not a vendor-specific integration — works with Slack/Discord/ntfy.sh/a local script/log-catcher without committing to one vendor. The webhook URL is a **Vault-backed credential via the `vault://` scheme** (Phase 5's `SecretsResolver`) — never hardcoded, per §81.
- **D-07:** **One alerting engine, not two**: Grafana's native unified alerting evaluates BOTH the Postgres-datasource freshness condition AND the Prometheus-datasource live gauges, through the same rule engine and the same webhook contact point. kube-prometheus-stack's bundled Alertmanager is **not used** — Grafana Alerting can already evaluate rules against any configured datasource, making a second alerting engine redundant.

**Data Freshness Tracking (OBS-01, OBS-09)**
- **D-08:** Expected delivery frequency is declared as a **new field in `configs/datasets/*.yaml`** (e.g. `freshness: {expected_frequency, warn_after, fail_after}`), flowing through the existing `ConfigRegistry.sync()` merge into new **nullable** columns on `meta.datasets`. Optional per dataset — a dataset with no `freshness:` block configured never triggers a freshness check at all, which is what makes OBS-09's "no file currently available" (stays quiet) structurally distinct from "file expected but missing" (alerts), not a special-cased flag. Rejected alternative: a separate versioned table mirroring `config_versions` — no current dataset needs SLA history, so this is unbuilt until one does.
- **D-09:** Freshness breaches fire through the **same alert path** as D-05/D-06/D-07 — one alerting mechanism for both "pipeline broke" and "feed went quiet."
- **D-10:** Freshness is evaluated by a **Grafana Alert rule querying Postgres directly** (a scheduled SQL condition against `meta.datasets`/`meta.ingestion_runs`) — no new Airflow DAG, no new evaluation code. Rejected alternative: a dedicated `check_freshness` DAG writing an explicit status row — more consistent with "everything auditable in the metadata DB," but adds a DAG and evaluation code this phase would otherwise skip, and isn't required by any requirement's literal wording.

**Tracing Backend & Trace Scope (OBS-10)**
- **D-11:** Tracing backend is **Grafana Tempo** (single-binary chart) — native Grafana integration (trace↔log↔metric correlation without a second product to learn), receives OTLP from the same OTel Collector `dataplat`'s metrics use (D-01). Not Jaeger.
- **D-12:** Trace root is the **mapped per-file KubernetesPodOperator task instance**, not the `S3KeySensor`/`discover_files` task. One trace per `meta.ingestion_runs` row — matching `trace_id`'s actual schema shape (one column per run row, not per DAG run) — keeps each file's trace independently searchable in Tempo even when Dynamic Task Mapping fans one DAG run out to many files in parallel. Directly matches the Core Value framing ("where did **this row** come from").

**Lineage Query Interface (OBS-07)**
- **D-13:** Lineage is exposed as **one wide SQL view per target table** (e.g. `meta.v_customers_lineage` today), joining that table's embedded lineage columns (`_run_id`/`_file_id`/`_batch_id`, per ARCHITECTURE.md §2.3's "embed lineage as columns on target tables" design) out to the full `meta.*` chain (`ingestion_runs` → `files` → `batches` → `config_versions` → `schema_versions`) in one query — directly satisfies OBS-07's literal wording ("one SQL query returns... for any row..."). This is a **repeatable pattern**, not a one-off: each future `normalized.*`/`warehouse.*` table gets the same shape without new design work. Verified live during this discussion: no new schema is needed — every column OBS-07 names already exists (migrations 0002/0004/0005). This decision was reached by consulting ARCHITECTURE.md §2.3 directly at the user's request rather than picking between the two originally-framed options blind.
- **D-14:** The opt-in `meta.record_lineage` table (ARCHITECTURE.md §2.3, for the target-row≠source-row case — aggregations, SCD2 collapses) is **explicitly out of Phase 7 scope**. ARCHITECTURE.md's own phase table places it in a later "Operations phase," and no current table needs it — `customers` is a 1:1 row mapping.
- **D-15:** **No convenience CLI/make target** for lineage lookups this phase — stays SQL-only (`meta.v_customers_lineage` is directly queryable via psql/any SQL client/Grafana Explore). Matches Phase 6's own D-06 precedent exactly ("SQL-queryable directly... a convenience CLI/make target... considered and deferred") and OBS-07's literal wording ("queryable by SQL," no more).

**Helm Profile, Persistence & Retention**
- **D-16 (Claude's discretion — see below):** Monitoring-stack CI treatment was left to researcher/planner.
- **D-17:** Prometheus/Tempo/Grafana data is **persistent** (PVCs), surviving a `cluster-down`/`cluster-up` cycle — same treatment as Vault's storage, consistent with this project's own concern for restart-survival on a host where a WSL2/Docker restart is a documented realistic event.
- **D-18:** Retention is **short — Prometheus ~15 days, Tempo ~7 days**. Rationale: `meta.ingestion_runs` and its neighbors are the actual permanent record per this project's own architecture; Prometheus/Tempo/Grafana only need to cover a recent operational window, not serve as a second source of truth. Matches ROADMAP.md's own Phase 7 plan guidance verbatim ("Prometheus/Grafana/Tempo hold no data-correctness state"). Rejected alternative: a longer ~90d/~30d window for historical trend comparison — more disk pressure on a long-running local dev box for a capability nothing currently needs.

**Automated Proof Depth**
- **D-19:** Phase 7 gets a **permanent automated test suite** proving the observability mechanisms actually work, at the same bar Phase 4 (04-CONTEXT.md D-10, real `kubectl delete pod`) and Phase 5 (05-CONTEXT.md D-03, live credential rotation) already set for this project — "proof over prose." Not just visual Grafana verification.
- **D-20:** The alerting path specifically gets a **live webhook-delivery test**: force a real freshness breach, wait for Grafana Alerting to actually fire, assert an HTTP POST arrives at a test-local webhook receiver with the expected payload. Not just an assertion that the underlying SQL predicate is true — proves the entire chain (Postgres condition → Grafana evaluation → contact point → webhook delivery), matching the "real, not simulated" bar Phase 4/5 established.

### Claude's Discretion
- **D-16's actual mechanism:** user explicitly deferred. Lean toward a template/lint-only CI check (`helm template` + `kubeconform` against a trimmed monitoring values file) rather than actually deploying the stack on the CI runner — consistent with how other charts are already validated in CI per `.claude/CLAUDE.md` §I, while still respecting PROJECT.md's constraint that the CI profile itself has monitoring disabled (no actual Prometheus/Grafana/Tempo pods on the 4 CPU/16 GB runner).
- Exact W3C `traceparent` injection mechanics — which function performs it (most likely extending `airflow/dags/_common/kpo.py`'s `common_kpo_kwargs()`, which already injects `vault://` env-var references per Phase 5) and how the mapped task's own span context is read via `airflow.sdk.observability.trace`. CLAUDE.md/STACK.md already establish this must be DIY env-var injection since cross-process propagation is not automatic; the exact code shape is implementation detail. **Research finding that refines this note:** `common_kpo_kwargs()` alone cannot carry the dynamic value (see Architecture Patterns → Pattern 1/Pitfall 2) — it still supplies every static piece unchanged, but the DAG must additionally use a `KubernetesPodOperator` subclass for the `ingest` task specifically.
- Confirmed, not re-litigated: modifying `airflow/dags/_common/kpo.py` to inject a `TRACEPARENT` env var does **not** conflict with Phase 3 D-03's "no pipeline code changes" framing — that promise was about `dataplat`/`csv_processor` PIPELINE library internals specifically (a different codebase per `docs/adr/0004-two-images-two-dependency-sets.md`), not the Airflow DAG orchestration layer, which necessarily needs to inject trace context to satisfy success criterion 3.
- Exact Grafana dashboard panel design beyond the data-source split already locked (D-03) — panel layout, chart types, one dashboard vs. multiple tabs/views.
- Exact schema shape of the new `meta.datasets` freshness columns (D-08) — column names/types (likely `interval`/duration types for `expected_frequency`/`warn_after`/`fail_after`), and whether `config_schema_version` needs bumping.
- Cold-start behavior for a dataset with freshness configured but zero `ingestion_runs` history yet (no prior file ever received) — a reasonable grace-period default (e.g., measured from `meta.datasets.created_at`) is left to planner, not re-litigated here.
- Exact retention/replica sizing numbers for the kube-prometheus-stack/Tempo Helm values beyond the day-count targets in D-18.
- Whether the OTel Collector is deployed standalone (`open-telemetry/opentelemetry-collector` chart, pinned `0.169.0` in `.claude/CLAUDE.md`'s Installation section) versus the Airflow chart's own optional bundled collector — the standalone chart is the far more likely fit since `dataplat`/`csv-processor` pods live outside the Airflow Helm release entirely and need a cluster-reachable collector Service, but the exact values wiring is implementation detail.

### Deferred Ideas (OUT OF SCOPE)
- **`meta.record_lineage`** (the opt-in target-row≠source-row lineage table) — explicitly out of scope per D-14; ARCHITECTURE.md places it in a later "Operations phase." Revisit whenever a table with target-row≠source-row semantics first ships (aggregations, or Phase 10's SCD2 collapses).
- **A convenience CLI/make target for lineage lookups** — deferred per D-15, following Phase 6's own precedent. Revisit if direct SQL querying proves painful in practice.
- **kube-prometheus-stack's bundled Alertmanager** — considered and explicitly not used (D-07); Grafana's native alerting supersedes it for this phase's needs. Revisit only if a future need specifically requires Alertmanager's own routing-tree features that Grafana's native alerting can't express.
- **Longer Prometheus/Tempo retention (~90d/~30d)** — considered, user chose the shorter window (D-18). Revisit if a real historical-trend-comparison need appears.
- **OpenLineage export** — already explicitly v2 per `PROJECT.md`/`ROADMAP.md` (`V2-OBS-01`); not re-raised in this discussion.

Also out of scope for this phase (from CONTEXT.md's Phase Boundary): operational runbooks (OBS-06 — Phase 11), and any pipeline/parsing/validation logic changes (Phase 6/8 territory).
</user_constraints>

## Project Constraints (from CLAUDE.md)

Directives from `.claude/CLAUDE.md` that directly bind this phase's plan (not a re-statement of the whole file — only what's actionable for observability/metrics/tracing/lineage work):

- **No Prometheus Pushgateway** (explicit "What NOT to Use" entry) — short-lived task pods must never push to a Pushgateway; `dataplat`'s runtime metrics push via OTLP to the Collector instead, and business metrics live in the analytical DB, never in Prometheus directly.
- **StatsD-XOR-OTel for Airflow's own metrics, mandatory:** "Enabling `otelCollector.metricsEnabled` disables statsd" (Version Compatibility Matrix). Airflow's chart-level `statsd.enabled` and any OTel-metrics toggle for Airflow itself are mutually exclusive — this phase must not enable both, and per D-02, Airflow's metrics stay on StatsD.
- **"There is no tracing backend in kube-prometheus-stack. You must add Grafana Tempo... or Jaeger. Budget for it."** — confirms Tempo (D-11) is genuinely new infrastructure this phase adds, not something already bundled.
- **"Trace context does NOT propagate into `KubernetesPodOperator` pods automatically... You must do it yourself."** — the explicit charter for this phase's Pattern 1/Pattern 2 work; CLAUDE.md flags this as a real gap, not an oversight to design around differently.
- **No credential may exist in Git, Python source, Dockerfiles, Kubernetes manifests, Airflow Variables or CI workflow files (§81). Runtime injection only.** — governs the webhook URL and the new `grafana_reader` password (Pattern 5): both must resolve at runtime via Vault, never as a literal in any committed file.
- **Deployment style: pinned upstream Helm charts with committed values files; no hand-rolled infra manifests.** — the new `helm/values/{local,ci}/{monitoring,otel-collector,tempo}.yaml` files must follow this project's existing one-values-file-per-chart convention, not a bespoke Kubernetes manifest.
- **Never `:latest`; images tagged by git SHA.** — applies directly to the new `docker/airflow/Dockerfile` build (must follow the exact `image-csv-processor` Makefile pattern: tag `<component>:$(GIT_SHA)`, push to the local registry, never `:latest`).
- **CI runner sizing:** GitHub-hosted runners are 4 CPU/16 GB; a trimmed CI profile with monitoring disabled must exist from the start. This phase's `helm/values/ci/*.yaml` additions must keep this stack absent/disabled in CI, matching D-16's discretion resolution.
- **`kube-prometheus-stack` 88.2.0, Grafana subchart 12.10.4 (repo `grafana-community.github.io`, not the old `grafana.github.io`), `statsd-exporter` v0.30.0, OTel Collector chart `0.169.0`** — already-pinned versions (HIGH confidence in CLAUDE.md itself) that this phase's Helm values must target; do not silently drift to a newer version without an explicit decision.
- **Determinism constraint (§67):** not directly applicable to observability infrastructure itself, but the freshness-config addition to `configs/datasets/*.yaml` must stay config-not-code (Pydantic model, `extra="forbid"`/`frozen=True`) to match every other config surface in this codebase.

## Summary

This phase wires four real backends behind seams that already exist as no-ops (`dataplat.observability.metrics`/`tracing`), adds one new SQL view over a schema that needs zero new columns, and adds one new small set of nullable columns plus a Grafana-evaluated condition for freshness. The stack pins in `.claude/CLAUDE.md` §H are already HIGH confidence for kube-prometheus-stack, Vault, and the OTel Collector version; this research fills in the parts CLAUDE.md explicitly left as "budget for it" / "DIY": the exact Helm values keys, the exact Python packages, and — the single most consequential finding — **the stock `apache/airflow:3.3.0-python3.12` image does not contain the OpenTelemetry packages Airflow's own tracing feature needs.** This was verified live in this session (`docker pull` + `pip list`, not just documentation), and it means Phase 7 is the phase that finally has to fill in `docker/airflow/Dockerfile`, which every prior phase deliberately left as `.gitkeep` because nothing needed it yet.

The second major finding is a genuine architecture gotcha in the W3C trace-context injection design: `KubernetesPodOperator.env_vars` is documented as Jinja-templated but has a multi-year history of that not actually working reliably (multiple open/historical Airflow GitHub issues), and more fundamentally, `common_kpo_kwargs()` (the function CONTEXT.md's own discretion notes point at) is called at **DAG-parse time**, before any span exists — a per-execution trace ID cannot be baked into a dict built once when the DAG file is imported. The concrete, verified-viable mechanism is to override `KubernetesPodOperator.build_pod_request_obj()`, which runs inside `execute()` at task-run time, and inject the env var into the already-built `V1Pod` object using `opentelemetry.propagate.inject()`.

The third finding is that this phase introduces a genuinely new category of Vault consumer. Phase 5 established exactly two tiers deliberately ("do NOT deploy the Agent Injector, the CSI driver, VSO, or ESO for the first milestone") — Airflow's native `VaultBackend`, and `hvac`-in-Python for ETL pods. Grafana is neither. The correct, precedent-consistent answer (found by reading `scripts/vault-bootstrap.py` directly) is to extend the exact same script-based pattern already used for `etl_app`'s password: a Python script does `kubectl exec ... ALTER ROLE ... WITH PASSWORD`, writes the result to Vault KV, and — the one genuinely new step — a small bootstrap step materializes a `kubernetes.io/basic-auth` Kubernetes Secret from that Vault value, because Grafana's own container has no Vault client at all.

**Primary recommendation:** Build `docker/airflow/Dockerfile` (extends `apache/airflow:3.3.0-python3.12`, adds `apache-airflow[otel]` under Airflow's own constraints file), wire `dataplat.observability.{metrics,tracing}` to `opentelemetry-sdk` + OTLP-HTTP exporters, deploy the standalone `open-telemetry/opentelemetry-collector` chart (already pinned 0.169.0) as the single OTLP ingress point for both Airflow's and `dataplat`'s telemetry, deploy Tempo (single-binary chart) and let kube-prometheus-stack's bundled Grafana read Postgres directly via a new least-privileged `grafana_reader` role, and implement trace propagation via a `KubernetesPodOperator` subclass overriding `build_pod_request_obj()` — not via `common_kpo_kwargs()`'s static dict or Jinja templating.

## Architectural Responsibility Map

This project has no browser/CDN tier (Airflow's and Grafana's own UIs are pre-built products, not something this project authors). The tiers below are adapted to this platform's actual architecture: **Orchestration** (Airflow), **Processing** (KPO pods / `dataplat`), **Database** (analytical PostgreSQL), and a new **Observability** tier this phase introduces (Prometheus/Grafana/Tempo/OTel Collector — infrastructure-only, holds no data-correctness state per ROADMAP's own framing).

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| `dataplat` runtime metrics (3 live gauges: in-flight runs, failure rate, reject rate) | Processing | Observability | Emitted from inside the KPO pod via OTLP; Observability tier only stores/serves them |
| Airflow internal metrics (scheduler heartbeat, DAG parse time) | Orchestration | Observability | Airflow emits StatsD; `statsd-exporter` (Observability tier) converts to Prometheus format — two independent pipelines converge on one Prometheus, never merged upstream |
| 8 named dashboard metrics (`files_processed`, `rows_invalid`, etc.) | Database | Observability | Values already live in `meta.ingestion_runs`/`meta.datasets`; Observability tier (Grafana) only queries, never computes or stores them |
| Distributed trace span creation | Orchestration + Processing | Observability | Airflow's worker pod creates the root span; the KPO pod's `dataplat` code creates child spans; Observability tier (OTel Collector → Tempo) only collects and stores |
| W3C trace-context propagation across the pod boundary | Orchestration | Processing | The Orchestration tier (a `KubernetesPodOperator` subclass) is the only place that can read the active span AND write the launched pod's spec — this is a one-way handoff, not a shared responsibility |
| Lineage query (OBS-07) | Database | — | A SQL view only; zero new store, zero new tier, matches ROADMAP's explicit "a query, not a new store" framing |
| Data freshness evaluation (OBS-01/OBS-09) | Observability | Database | Grafana Alerting (Observability tier) evaluates a SQL condition against the Database tier directly — no new Orchestration-tier code (no new DAG), per D-10 |
| Alert notification delivery | Observability | — | Grafana's native unified alerting owns rule evaluation AND webhook delivery; kube-prometheus-stack's bundled Alertmanager is explicitly unused (D-07) |

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| OBS-01 | Data freshness is tracked — last received, last successful processing, expected frequency and processing delay | New nullable `meta.datasets` columns (Architecture Patterns → Freshness) fed by `configs/datasets/*.yaml`; a Grafana Alert rule query derives last-received/last-success/delay by joining `meta.files`/`meta.ingestion_runs` at query time — no new stored "processing delay" column needed |
| OBS-07 | Lineage is queryable by SQL — source file, object path, checksum, batch, ingestion timestamp, DAG/run/task ID, processor version, schema version, config version | `meta.v_customers_lineage` view, exact SQL drafted in Code Examples from the real column names in migrations 0002/0003/0004/0005/0009 |
| OBS-08 | Platform metrics exposed with bounded label cardinality, unbounded identity in the metadata DB | `dataplat.observability.metrics` OTLP wiring with `dataset`+`stage`+`status` labels only (D-04); 8 dashboard metrics sourced from Postgres, not OTLP, avoiding cardinality risk entirely for the historical figures |
| OBS-09 | "No file currently available" distinguished from "file expected but missing", configurable warn/fail | `expected_frequency IS NULL` (no check at all) vs. `IS NOT NULL` (Grafana Alert rule evaluates it) — structural distinction per D-08, SQL drafted in Code Examples |
| OBS-10 | Distributed traces span Airflow task → task pod → processor → PostgreSQL via explicit W3C `traceparent` propagation | `KubernetesPodOperator.build_pod_request_obj()` override (Architecture Patterns → Trace Propagation); `apache-airflow[otel]` custom image requirement (verified live, Common Pitfalls); `opentelemetry.propagate.inject()`/`extract()` API confirmed |
</phase_requirements>

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `opentelemetry-sdk` | 1.44.0 | Real `TracerProvider`/`MeterProvider` backing `dataplat.observability.{tracing,metrics}` | Official OTel Python SDK; same package family Airflow's own `[otel]` extra uses. `[VERIFIED: PyPI registry]` via `pip index versions` this session; also independently listed in CLAUDE.md's own Sources section (PyPI JSON API, prior research session) |
| `opentelemetry-api` | 1.44.0 | Transitive requirement of `opentelemetry-sdk`; explicit pin keeps `dataplat`'s own `pyproject.toml` self-describing | `[VERIFIED: PyPI registry]` |
| `opentelemetry-exporter-otlp-proto-http` | 1.44.0 | OTLP/HTTP exporter — primary recommendation for `dataplat`'s csv-processor image | Pure-protobuf-over-HTTP, no `grpcio` C-extension dependency added to the "stays slim" csv-processor image (ADR-0004). `[VERIFIED: PyPI registry]` |
| `opentelemetry-exporter-otlp-proto-grpc` | 1.44.0 | OTLP/gRPC exporter — alternative if throughput ever matters | Heavier (`grpcio`) but lower overhead per span at high volume; not needed at this project's scale. `[VERIFIED: PyPI registry]` |

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `opentelemetry-instrumentation-psycopg` | 0.65b0 (beta) | Auto-instrument psycopg v3 DB calls as child spans (the "→ PostgreSQL" segment of OBS-10) | Optional. Still pre-1.0 (`bNN` suffix) from `open-telemetry/opentelemetry-python-contrib` — the officially-correct package name for psycopg **v3** (not `-psycopg2`, confirmed distinct). Given this project's existing convention of manually threading `tracing.start_span()` around explicit stages (`pipeline/engine.py`'s own docstring: "Both threaded observability call sites D-03 requires live here"), the **primary recommendation is a manual span** around `PostgresMetadataRepository`/`Publisher.publish()` DB calls instead — zero new beta dependency, consistent with the codebase's existing manual-instrumentation style. Use the contrib package only if manual spans prove insufficient. |
| `apache-airflow[otel]` (extra, not a package) | matches pinned `apache-airflow==3.3.0` | Installs Airflow's own OTel tracing dependencies | `[CITED: airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/traces.html]` — "Required for OpenTelemetry metrics/traces", `pip install 'apache-airflow[otel]'`. The exact transitive package list is not enumerated in the docs; resolved independently under Airflow's own constraints file (see Common Pitfalls — this is fine, not a version-skew risk). |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `opentelemetry-exporter-otlp-proto-http` | `opentelemetry-exporter-otlp-proto-grpc` | gRPC has marginally lower per-span overhead but adds `grpcio` (a compiled C-extension) to the csv-processor image; not justified at this project's local-dev scale |
| Manual `tracing.start_span()` around DB calls | `opentelemetry-instrumentation-psycopg` (auto) | Auto-instrumentation is less code but is a pre-1.0 contrib package; manual spans match this project's existing convention and avoid a beta dependency |
| A custom `docker/airflow/Dockerfile` | `_PIP_ADDITIONAL_REQUIREMENTS` env var | Documented anti-pattern by the Airflow project itself ("very bad and dangerous... useful only when iterating and debugging") — re-resolves packages from PyPI on every pod start with no lockfile, non-deterministic across restarts. Rejected. |

**Installation:**
```bash
# packages/dataplat/pyproject.toml — new [project.dependencies] entries
uv add --project packages/dataplat opentelemetry-sdk opentelemetry-exporter-otlp-proto-http

# docker/airflow/Dockerfile — new file (see Common Pitfalls for why this must exist now)
# RUN pip install --no-cache-dir "apache-airflow[otel]==${AIRFLOW_VERSION}" \
#       --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.0/constraints-3.12.txt"
```

**Version verification performed this session:**
```bash
pip index versions opentelemetry-sdk                     # 1.44.0 (latest)
pip index versions opentelemetry-exporter-otlp-proto-http # 1.44.0 (latest)
pip index versions opentelemetry-instrumentation-psycopg --pre  # 0.65b0 (latest, still beta)
docker pull apache/airflow:3.3.0-python3.12 && \
  docker run --rm apache/airflow:3.3.0-python3.12 pip list | grep -iE "opentelemetry|otel"
# -> zero matches (exit 1) — VERIFIED live, not from documentation
```

## Package Legitimacy Audit

| Package | Registry | Age/Maturity | Source Repo | slopcheck | Disposition |
|---------|----------|---------------|-------------|-----------|-------------|
| `opentelemetry-sdk` | PyPI | Mature (1.x since 2023, official OTel project) | `github.com/open-telemetry/opentelemetry-python` | [OK] (flagged "-sdk suffix looks LLM-bait" but auto-cleared: "package is established") | Approved |
| `opentelemetry-api` | PyPI | Mature | `github.com/open-telemetry/opentelemetry-python` | [OK] (same auto-cleared naming note) | Approved |
| `opentelemetry-exporter-otlp-proto-http` | PyPI | Mature | `github.com/open-telemetry/opentelemetry-python` | [OK] | Approved |
| `opentelemetry-exporter-otlp-proto-grpc` | PyPI | Mature | `github.com/open-telemetry/opentelemetry-python` | [OK] | Approved |
| `opentelemetry-instrumentation-psycopg` | PyPI | Pre-1.0 (`0.65b0`), active | `github.com/open-telemetry/opentelemetry-python-contrib` | [OK] | Approved, discretionary (see Standard Stack) |

**Packages removed due to slopcheck [SLOP] verdict:** none
**Packages flagged as suspicious [SUS]:** none

All five ran through `slopcheck install <pkgs> --ecosystem pypi` this session; all returned `[OK]`. This is real registry-check output (not `[ASSUMED]`), and each package's existence was additionally cross-verified via `pip index versions` and, for the four non-instrumentation packages, is independently corroborated in `.claude/CLAUDE.md`'s own Sources section as already-verified via PyPI JSON API. Tagged `[VERIFIED: PyPI registry]` throughout this document.

## Architecture Patterns

### System Architecture Diagram

```
┌─────────────────────────── Orchestration tier (Airflow) ───────────────────────────┐
│                                                                                       │
│  S3KeySensor ──► discover (KPO) ──► ingest (KPO, mapped, ×N files)                  │
│                                          │                                            │
│                                          │ 1. Airflow Task SDK creates the            │
│                                          │    "Airflow-managed task span" HERE        │
│                                          │    (this pod = ServiceAccount              │
│                                          │    airflow-worker, imports the DAG,        │
│                                          │    runs KubernetesPodOperator.execute())   │
│                                          │                                            │
│                                          │ 2. TracingKubernetesPodOperator.           │
│                                          │    build_pod_request_obj() reads the       │
│                                          │    active span via opentelemetry.trace,    │
│                                          │    injects TRACEPARENT env var into the    │
│                                          │    V1Pod spec it is about to launch        │
│                                          ▼                                            │
└──────────────────────────────────────────┼──────────────────────────────────────────┘
                                            │  pod boundary — NOT automatic (OBS-10)
┌───────────────────────────────────────────▼─────────── Processing tier (KPO pod) ───┐
│  csv-processor container                                                             │
│                                                                                       │
│  dataplat.cli entrypoint reads TRACEPARENT from os.environ,                          │
│  opentelemetry.propagate.extract() sets it as the parent context                     │
│                          │                                                            │
│                          ▼                                                            │
│  tracing.start_span("pipeline.run_streaming.chunk")  ◄── already threaded, Phase 3   │
│                          │                                                            │
│                          ├──► metrics.increment("rows_rejected"/"rows_kept", ...,     │
│                          │      dataset=..., stage=..., status=...)  [D-04 labels]    │
│                          │                                                            │
│                          ▼                                                            │
│  PostgresMetadataRepository / Publisher.publish()                                    │
│      manual span (or opentelemetry-instrumentation-psycopg, optional)                │
└──────────────────────────┬───────────────────────────────┬───────────────────────────┘
                            │ OTLP/HTTP                     │ psycopg
                            ▼                                ▼
┌──────────────── Observability tier ────────────┐  ┌── Database tier ───────────────┐
│                                                  │  │                                 │
│  OTel Collector (standalone chart, 0.169.0)     │  │  meta.ingestion_runs            │
│    receivers: otlp (4317 grpc / 4318 http)      │  │    .trace_id / .span_id         │
│    exporters: otlp (→ Tempo), prometheus (scrape)│  │    (already-modeled columns,   │
│         │                    │                   │  │     currently always NULL)     │
│         ▼                    ▼                   │  │                                 │
│    Tempo (single-binary)   Prometheus            │  │  meta.v_customers_lineage       │
│    (traces, ~7d PVC)       (kube-prometheus-     │  │    (new SQL view, OBS-07)       │
│         │                   stack, ~15d PVC)      │  │                                 │
│         │                    │                    │  │  meta.datasets                 │
│         │                    │  ◄── statsd-exporter│  │    + freshness columns (OBS-01)│
│         │                    │      ◄── Airflow    │  └────────────┬────────────────────┘
│         │                    │          StatsD     │               │
│         ▼                    ▼                     │               │ grafana_reader
│  Grafana (Tempo datasource + Postgres datasource + Prometheus datasource)            │
│    - dashboards: 8 named metrics from Postgres, 3 live gauges from Prometheus         │
│    - alerting: freshness rule (Postgres) + live-gauge rules (Prometheus),             │
│      one contact point = generic webhook, URL via envFromSecret ◄── Vault (bootstrap  │
│                                                                        script, new     │
│                                                                        3rd Vault tier) │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Recommended Project Structure

```
docker/
├── airflow/
│   └── Dockerfile              # NEW this phase — was .gitkeep since Phase 1
├── csv-processor/
│   └── Dockerfile              # unchanged
helm/values/{local,ci}/
├── airflow.yaml                 # + env: OTEL_EXPORTER_OTLP_ENDPOINT, config.traces.otel_on
├── monitoring.yaml               # NEW — kube-prometheus-stack values (Grafana datasources,
│                                  #        alerting provisioning, additionalServiceMonitors)
├── otel-collector.yaml           # NEW — standalone opentelemetry-collector chart values
├── tempo.yaml                    # NEW — single-binary Tempo chart values
├── cnpg-analytics.yaml           # unchanged (grafana_reader created via migration, not initdb)
packages/dataplat/src/dataplat/observability/
├── metrics.py                   # no-op → real OTLP Meter
├── tracing.py                    # no-op → real OTLP TracerProvider + context extraction
airflow/dags/_common/
├── kpo.py                        # common_kpo_kwargs() unchanged (static kwargs only)
├── tracing_kpo.py                # NEW — TracingKubernetesPodOperator subclass
migrations/versions/
├── 0010_meta_datasets_freshness.py         # NEW
├── 0011_grafana_reader_role.py             # NEW
├── 0012_meta_v_customers_lineage.py        # NEW
scripts/
├── vault-bootstrap.py            # extended: _ensure_grafana_secrets()
├── grafana-db-secret.sh          # NEW — mirrors airflow-metadata-secret.sh's pattern,
│                                  #        materializes the K8s Secret from Vault
```

### Pattern 1: W3C traceparent injection via operator subclass, not static kwargs or Jinja

**What:** A `KubernetesPodOperator` subclass overrides `build_pod_request_obj(context)` — which Airflow calls from inside `execute()`, i.e. at task-run time inside the process holding the active Airflow-managed span — to append a `TRACEPARENT` env var to the already-built pod spec.

**When to use:** Any KPO task that must propagate trace context into its launched pod. For this phase, only the `ingest` task (D-12's trace root), not `discover`.

**Why not `common_kpo_kwargs()` or Jinja templating:** `common_kpo_kwargs()` is called once, at DAG-parse time, when `csv_ingest_customers.py`'s `KubernetesPodOperator(...)`/`.partial(...)` calls are evaluated — no span exists yet at that point, so no per-execution trace ID can be baked into its returned dict. `env_vars` **is** a documented `template_fields` member of `KubernetesPodOperator`, but Jinja templating of that specific field has a multi-year history of not working reliably (`apache/airflow` issue #13348 "env_vars of KubernetesPodOperator are not truly templated"; discussion #25841 "documented to be templated, but it doesn't work"), and no Airflow Jinja macro exposes `trace_id`/`span_id` regardless.

**Example:**
```python
# Source: pattern synthesized from confirmed facts —
#   airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/
#   logging-monitoring/traces.html (custom span API, no-op-safe when otel_on=False)
#   + opentelemetry.io/docs/languages/python/propagation/ (inject/extract API)
#   + airflow-providers-cncf-kubernetes source (build_pod_request_obj called from execute())
# [CITED, MEDIUM confidence — verify empirically early, given env_vars' own templating history]
from __future__ import annotations

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from opentelemetry import propagate


class TracingKubernetesPodOperator(KubernetesPodOperator):
    """KPO that injects the active span's W3C traceparent into the launched pod (OBS-10)."""

    def build_pod_request_obj(self, context=None):
        pod = super().build_pod_request_obj(context)
        carrier: dict[str, str] = {}
        propagate.inject(carrier)  # no-op-safe: writes nothing if otel_on=False / no active span
        if "traceparent" in carrier:
            pod.spec.containers[0].env.append(
                k8s.V1EnvVar(name="TRACEPARENT", value=carrier["traceparent"]),
            )
        return pod
```

### Pattern 2: dataplat.observability.tracing — extract on entry, start_span already threaded

**What:** The KPO pod's entrypoint (`dataplat.cli`) reads `TRACEPARENT` from `os.environ` once at process start and sets it as the initial OTel context, so the first `tracing.start_span()` call inside `run_streaming()` (already threaded per Phase 3) becomes a **child** of the Airflow-managed span — satisfying "the context crossing the pod boundary" literally.

**Example:**
```python
# Source: opentelemetry.io/docs/languages/python/propagation/ [CITED]
import os

from opentelemetry import context as otel_context
from opentelemetry import propagate


def _extract_incoming_trace_context() -> None:
    traceparent = os.environ.get("TRACEPARENT")
    if traceparent:
        ctx = propagate.extract({"traceparent": traceparent})
        otel_context.attach(ctx)  # subsequent start_span() calls nest under this context
```

### Pattern 3: Freshness — config-driven columns on meta.datasets, not a new table

**What:** `configs/datasets/*.yaml` gets an optional `freshness:` block; `ConfigRegistry.sync()` is extended (not just the migration) to write three new nullable `meta.datasets` columns on every sync, because its current upsert only ever sets `dataset_name = EXCLUDED.dataset_name` (a deliberate no-op today).

**Important correction to a stale prior-research artifact:** `.planning/research/ARCHITECTURE.md` §2.2 speculatively named a separate `meta.dataset_sla` table (`expected_frequency interval`, `grace_period interval`, `last_received_at`, `last_success_at`, `on_missing`) for this exact purpose, written before this phase's CONTEXT.md discussion happened. **CONTEXT.md D-08 supersedes this** — the locked decision is columns directly on `meta.datasets`, not a new table, and `last_received_at`/`last_success_at` are derived at query time from `meta.files`/`meta.ingestion_runs` (per D-10's "no new evaluation code"), never stored redundantly. Do not build `meta.dataset_sla`.

**Example (migration):**
```python
# Source: pattern matches migrations 0001/0009's existing hash_version-column style
def upgrade() -> None:
    op.add_column("datasets", sa.Column("expected_frequency", sa.Interval(), nullable=True), schema="meta")
    op.add_column("datasets", sa.Column("freshness_warn_after", sa.Interval(), nullable=True), schema="meta")
    op.add_column("datasets", sa.Column("freshness_fail_after", sa.Interval(), nullable=True), schema="meta")
```

**Example (`ConfigRegistry._resolve_dataset_id`, extended):**
```python
# Current code (config/registry.py) only sets dataset_name on conflict — this
# must widen to also carry freshness fields from the just-validated DatasetConfig.
row = cur.execute(
    """
    INSERT INTO meta.datasets (dataset_name, expected_frequency, freshness_warn_after, freshness_fail_after)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (dataset_name) DO UPDATE
        SET expected_frequency = EXCLUDED.expected_frequency,
            freshness_warn_after = EXCLUDED.freshness_warn_after,
            freshness_fail_after = EXCLUDED.freshness_fail_after
    RETURNING dataset_id
    """,
    (dataset_name, freq, warn_after, fail_after),
).fetchone()
```

### Pattern 4: Grafana alerting-as-code via the chart's native `alerting:` key, not the sidecar

**What:** kube-prometheus-stack's bundled `grafana` subchart (12.10.4) supports two different file-provisioning mechanisms: (a) a `sidecar.alerts` ConfigMap-watcher requiring a separately-labeled ConfigMap resource, and (b) a native `alerting: {}` values key that renders directly into `/etc/grafana/provisioning/alerting/*.yaml` with no extra Kubernetes resource. **Use (b)** — it keeps every provisioning file inside the one already-committed Helm values file (matches INFRA-07's "no manual kubectl surgery" and this project's existing one-values-file-per-concern convention), rather than introducing a second Kubernetes object (a ConfigMap) that a separate mechanism watches.

**Example:**
```yaml
# Source: raw.githubusercontent.com/grafana-community/helm-charts/main/charts/grafana/values.yaml
# [CITED, MEDIUM confidence — the exact YAML-under-YAML nesting for policies/rules/
#  contact-points should be spot-checked against `helm show values` before use]
grafana:
  envFromSecret: grafana-alert-webhook   # NEW K8s Secret, materialized by scripts/ (Pattern 5)
  additionalDataSources:
    - name: analytics-postgres
      type: postgres
      access: proxy
      url: analytics-db-rw.data.svc.cluster.local:5432
      user: grafana_reader
      jsonData:
        database: analytics
        sslmode: disable
        postgresVersion: 1800
      secureJsonData:
        password: $GRAFANA_DB_PASSWORD   # single-$ form only — see Common Pitfalls
  alerting:
    contactpoints.yaml:
      apiVersion: 1
      contactPoints:
        - orgId: 1
          name: platform-webhook
          receivers:
            - uid: platform-webhook-1
              type: webhook
              settings:
                url: $GRAFANA_ALERT_WEBHOOK_URL
    policies.yaml:
      apiVersion: 1
      policies:
        - orgId: 1
          receiver: platform-webhook
    rules.yaml:
      apiVersion: 1
      groups:
        - orgId: 1
          name: freshness
          folder: platform
          interval: 5m
          rules: []  # see Code Examples for the actual SQL condition
```

### Pattern 5: A third Vault-consumer tier — script-bootstrapped K8s Secret, not Agent Injector/VSO

**What:** `.planning/research/STACK.md`'s "two-tier pattern" (Airflow-native VaultBackend; hvac-in-pod for ETL) explicitly says not to deploy the Agent Injector, CSI driver, VSO, or ESO "for the first milestone." Grafana fits neither tier — its container has no Vault client at all. The precedent-consistent answer, found by reading `scripts/vault-bootstrap.py`'s existing `_ensure_etl_secrets()` directly, is to extend that exact script: generate a random password, `kubectl exec` into the CNPG primary to `ALTER ROLE grafana_reader WITH PASSWORD '<value>'`, write it to Vault KV, and add one new small step that reads it back out of Vault and creates a `kubernetes.io/basic-auth`-shaped Kubernetes Secret (`username`+`password` keys) that Grafana's Helm values reference by name via `envFromSecret`. The raw credential still never appears in git or in a Helm values file — only the Secret's **name** does, matching the existing `airflow-metadata`/`fernetKeySecretName` convention exactly.

**Alternative considered:** CNPG's `Cluster` CRD supports fully declarative role management (`spec.managed.roles[].passwordSecret`) — `[VERIFIED: helm/schemas/cnpg/cluster_v1.json]`, this project's own locally-vendored CRD schema, confirmed present with `name`/`login`/`passwordSecret.name` (requiring `kubernetes.io/basic-auth` format) fields. This would let CNPG's operator reconcile the role continuously instead of a one-shot script. **Not recommended as primary** because `etl_app` (the only precedent in this codebase) was NOT created this way — it uses `postInitApplicationSQL` (initdb-time only) plus the manual `kubectl exec` password-rotation script. Introducing `managed.roles` now would be a second, inconsistent role-management mechanism living alongside the first. Worth a footnote/ADR if the planner prefers the more declarative path, but the manual-script extension is lower-risk and immediately consistent.

**Why NOT `postInitApplicationSQL`:** `[VERIFIED: helm/values/local/cnpg-analytics.yaml]` — this key only runs once, at cluster `initdb` time. The analytical CNPG cluster is **already running** (3+ days old, confirmed live this session via `kubectl get ns`), so a role added here would never actually be created. **Use an Alembic migration** (`CREATE ROLE grafana_reader LOGIN;`) instead — migrations run against a live cluster at any time, unlike `postInitApplicationSQL`.

### Anti-Patterns to Avoid

- **Injecting `TRACEPARENT` inside `common_kpo_kwargs()`:** No span exists at DAG-parse time; this function is evaluated once when the DAG file is imported, not once per task execution. Use the `build_pod_request_obj()` override instead (Pattern 1).
- **Sourcing all 8 dashboard metrics from OTLP/Prometheus:** D-03's entire design point is that historical/exact figures come from Postgres (already correct, already durable) and only 3 specific live signals get a Prometheus path. Routing all 8 through OTLP would silently reintroduce the cardinality risk PITFALLS #12 exists to prevent.
- **A new `check_freshness` Airflow DAG:** Explicitly rejected in D-10. Freshness is a Grafana-evaluated SQL condition, not new orchestration code.
- **`meta.dataset_sla` as a separate table:** Superseded by D-08 (see Pattern 3). Reading only ARCHITECTURE.md without CONTEXT.md would lead here incorrectly.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| W3C trace-context serialization/parsing | A custom `trace_id-span_id-flags` string formatter | `opentelemetry.propagate.inject()`/`extract()` | The W3C Trace Context spec has exact hex-padding, flag-byte, and validity rules; the SDK's propagator is the reference implementation and already used correctly elsewhere in the OTel ecosystem this project depends on |
| Freshness rule evaluation | A new Airflow DAG polling `meta.datasets` on a schedule | Grafana Alerting evaluating a SQL condition against the Postgres datasource | D-10's explicit choice — Grafana already has a production-grade, persistent, retry-aware rule evaluation engine; a DAG would duplicate it with less maturity |
| Alert routing / notification delivery | Custom webhook-POST code in a DAG or script | Grafana's native unified alerting + webhook contact point | Handles retries, silences, mute timings, and templating already; D-07's explicit "one alerting engine" decision |
| Metrics aggregation across pods | A custom Redis/in-memory counter server | OTel Collector's `batch` processor + Prometheus's own scrape-time aggregation | Reinventing a metrics pipeline is explicitly what "No Prometheus Pushgateway" (ROADMAP, this project's Out of Scope table) already warns against in spirit — don't build the bespoke equivalent |

**Key insight:** Every "don't hand-roll" item above already has a production-grade implementation this project is already deploying (Grafana, OTel Collector) — the discipline this phase requires is wiring, not building.

## Common Pitfalls

### Pitfall 1: The stock Airflow image cannot emit OTel traces (VERIFIED live this session)
**What goes wrong:** A plan assumes `[traces] otel_on = True` + `OTEL_EXPORTER_OTLP_ENDPOINT` env vars are sufficient, because that's all the official docs page shows. Every Airflow pod then fails to import `opentelemetry` modules, or (more insidiously) `otel_on` silently has no effect because the code path that would use it never loads.
**Why it happens:** `apache-airflow[otel]` is an opt-in extra. It is confirmed absent from the production image's own default `AIRFLOW_EXTRAS` build arg (`aiobotocore,amazon,async,celery,cncf-kubernetes,...` — no `otel`), and this was independently confirmed live: `docker pull apache/airflow:3.3.0-python3.12 && docker run --rm ... pip list | grep -i otel` returned **zero matches**.
**How to avoid:** Build `docker/airflow/Dockerfile` (currently `.gitkeep` since Phase 1; `docs/README.md` already documents its intended purpose: "Installs providers under Airflow's own constraints file — deliberately outside the uv workspace") that layers `pip install "apache-airflow[otel]==3.3.0" --constraint <the pinned constraints URL>` on top of the stock image, tag/push it exactly like `image-csv-processor`'s existing Makefile pattern, and point `defaultAirflowRepository`/`defaultAirflowTag` (or `images.airflow.*`) at the custom image in both values profiles — but only enable `[traces] otel_on` in the **local** profile; CI has no OTel Collector deployed (D-16) so it should stay off there.
**Warning signs:** `ModuleNotFoundError: No module named 'opentelemetry'` in scheduler/worker pod logs; `otel_on: True` set but zero traces ever arrive at the Collector.

### Pitfall 2: DAG-parse-time vs. task-execute-time confusion for dynamic env vars
**What goes wrong:** Trace injection code is added inside `common_kpo_kwargs()`, which looks like the natural place (it already handles `env_vars`) but runs exactly once, when Airflow's DAG processor imports `csv_ingest_customers.py` — long before any task attempt, and therefore before any span exists.
**Why it happens:** `KubernetesPodOperator(...)`/`.partial(...)` calls inside a `@dag`-decorated function body execute at **parse** time; only the operator's `execute()` method (and anything it calls, including `build_pod_request_obj()`) runs at **task-run** time.
**How to avoid:** Use Pattern 1 (subclass + `build_pod_request_obj()` override). `common_kpo_kwargs()` keeps doing exactly what it does today (static resources, SA, `vault://` env vars) — it is additive, not replaced.
**Warning signs:** Every task instance's launched pod gets the *same* `TRACEPARENT` value (or none at all) regardless of which DAG run or file it belongs to.

### Pitfall 3: `env_vars` Jinja templating on KubernetesPodOperator has a documented history of not working
**What goes wrong:** A plan relies on `{{ ti.xcom_pull(...) }}`-style templating inside `env_vars` to carry a dynamic value, expecting it to render at execute time like other templated fields.
**Why it happens:** `env_vars` is listed in `template_fields`, but multiple Airflow GitHub issues/discussions (#13348 "not truly templated", #25841 "documented to be templated, but it doesn't work") describe this not behaving as expected across several Airflow versions; there was a fix commit but the safest posture for a load-bearing correctness requirement (OBS-10) is not to depend on it at all.
**How to avoid:** Use the `build_pod_request_obj()` override (Pattern 1), which mutates the already-rendered pod spec directly rather than depending on template rendering.
**Warning signs:** Env var literally contains an unrendered `{{ ... }}` string inside the launched pod.

### Pitfall 4: `postInitApplicationSQL` cannot add a role to an already-running cluster
**What goes wrong:** A new `grafana_reader` role is added to `helm/values/local/cnpg-analytics.yaml`'s `initdb.postInitApplicationSQL` list, `helm upgrade` is run, and nothing happens — the role never appears.
**Why it happens:** `postInitApplicationSQL` (and all of CNPG's `initdb.*` bootstrap keys) only execute once, at cluster creation. The analytical CNPG cluster is already running (confirmed live this session, `Active 3d6h`).
**How to avoid:** Create the role via a new Alembic migration (`op.execute("CREATE ROLE grafana_reader LOGIN")`), matching how the schema-level `USAGE` grants for `etl_app` were themselves retrofitted in migration 0008 after being discovered missing on a live cluster.
**Warning signs:** `helm diff`/`helm upgrade` reports no changes to the CNPG `Cluster` resource's already-applied `initdb` block (it's intentionally immutable post-creation).

### Pitfall 5: Grafana provisioning's `${VAR}` (double-brace) syntax double-expands `$` inside the value
**What goes wrong:** A webhook URL or generated password containing a literal `$` character (plausible for a `secrets.token_hex`-style random value only if it happened to be interpreted as text containing `$`, or for a webhook URL with a `$`-containing query token) gets mangled — Grafana's own documentation describes exactly this failure mode for `${VAR}`-style references.
**How to avoid:** Use the single-dollar `$VARNAME` form (not `${VARNAME}`) in provisioning YAML, as shown in Pattern 4's example, and prefer `secrets.token_hex` (hex charset only, no `$`) for anything landing in a provisioning-file-referenced secret.
**Warning signs:** Contact point test-send fails with a URL that looks truncated or mangled partway through.

### Pitfall 6: ServiceMonitor silently never scraped due to label mismatch
**What goes wrong:** The OTel Collector's Prometheus-exporter endpoint is registered via a `ServiceMonitor`, but Prometheus never scrapes it — no error, just permanently-missing metrics.
**Why it happens:** kube-prometheus-stack's Prometheus Operator installs with a `serviceMonitorSelector` that, by default, only matches ServiceMonitors carrying a specific label (commonly `release: <helm-release-name>`). A `ServiceMonitor` without that exact label is silently ignored — this is Prometheus Operator's most commonly reported "invisible" failure mode.
**How to avoid:** Use kube-prometheus-stack's own `prometheus.additionalServiceMonitors` values list (keeps the label-matching concern inside the same chart's own values, avoiding a cross-chart label-coordination problem) rather than a hand-authored standalone `ServiceMonitor` manifest.
**Warning signs:** `kubectl get servicemonitor` shows the resource exists; the Prometheus UI's Targets page never shows it.

### Pitfall 7: OTel SDK version skew between the two images is expected, not a bug
**What goes wrong:** A reviewer notices the Airflow image's `apache-airflow[otel]`-resolved `opentelemetry-sdk` version differs from `dataplat`'s own pinned `1.44.0` and treats it as a defect to reconcile.
**Why it happens:** ADR-0004 ("two images, two dependency sets") means Airflow's constraints file resolves its own OTel packages entirely independently of `dataplat`'s `uv.lock`.
**How to avoid:** OTLP is a stable wire protocol across SDK versions within the same major (1.x); do not attempt to force version parity between the two images. Document this explicitly if it comes up in review, the same way ADR-0004 already documents the psycopg2-vs-psycopg3 split as intentional.

## Code Examples

### `meta.v_customers_lineage` (OBS-07) — exact SQL from real column names

```sql
-- Source: migrations/versions/0002_meta_files.py, 0003_meta_batches_batch_files.py,
--         0004_meta_ingestion_runs.py, 0005_normalized_customers.py, 0009_meta_schema_versions.py
-- [VERIFIED: read directly from this session's Read calls against the actual migration files]
CREATE VIEW meta.v_customers_lineage AS
SELECT
    c.id                    AS customer_row_id,
    c.customer_id,
    c._source_row_number,
    c._record_hash,
    c._record_hash_version,
    c._ingested_at,
    f.file_id,
    f.object_uri,
    f.content_sha256,
    f.hash_version          AS file_hash_version,
    f.filename,
    f.business_date,
    b.batch_id,
    b.batch_key,
    r.run_id,
    r.idempotency_key,
    r.dag_id,
    r.dag_run_id,
    r.task_id,
    r.map_index,
    r.try_number,
    r.k8s_namespace,
    r.k8s_pod_name,
    r.trace_id,
    r.span_id,
    r.processor_version,
    r.processor_image_digest,
    r.started_at             AS run_started_at,
    r.finished_at             AS run_finished_at,
    cv.version                AS config_version,
    cv.config_hash,
    sv.version                AS schema_version,
    sv.schema_hash
FROM normalized.customers c
JOIN meta.ingestion_runs r        ON r.run_id = c._run_id
JOIN meta.files f                 ON f.file_id = c._file_id
JOIN meta.batches b                ON b.batch_id = c._batch_id
JOIN meta.config_versions cv       ON cv.config_version_id = r.config_version_id
LEFT JOIN meta.schema_versions sv  ON sv.schema_version_id = r.schema_version_id;
-- LEFT JOIN on schema_versions only: meta.ingestion_runs.schema_version_id is nullable
-- (migration 0004's own deliberate design — see its docstring). Every other join is a
-- plain INNER JOIN because _run_id/_file_id/_batch_id/config_version_id are all
-- NOT NULL on their respective tables, verified directly in the DDL.

GRANT SELECT ON meta.v_customers_lineage TO etl_app;
GRANT SELECT ON meta.v_customers_lineage TO grafana_reader;
```

### Freshness Grafana Alert condition (OBS-01/OBS-09) — the SQL a Postgres-datasource rule evaluates

```sql
-- Source: derived from meta.datasets (new columns, migration 0010) + meta.files +
-- meta.ingestion_runs (existing columns). [ASSUMED shape — exact column names are
-- Claude's discretion per CONTEXT.md; this SQL demonstrates the structural pattern
-- that makes "no freshness configured" and "expected but missing" mutually exclusive.]
SELECT
    d.dataset_id,
    d.dataset_name,
    d.expected_frequency,
    d.freshness_warn_after,
    d.freshness_fail_after,
    COALESCE(MAX(f.discovered_at), d.created_at)                        AS last_received_at,
    MAX(r.finished_at) FILTER (WHERE r.status = 'SUCCEEDED')            AS last_success_at,
    now() - COALESCE(MAX(f.discovered_at), d.created_at)                AS processing_delay
FROM meta.datasets d
LEFT JOIN meta.files f          ON f.dataset_id = d.dataset_id
LEFT JOIN meta.ingestion_runs r ON r.dataset_id = d.dataset_id
WHERE d.expected_frequency IS NOT NULL   -- OBS-09: NULL here means "stays quiet", structurally
GROUP BY d.dataset_id, d.dataset_name, d.expected_frequency,
         d.freshness_warn_after, d.freshness_fail_after, d.created_at
HAVING now() - COALESCE(MAX(f.discovered_at), d.created_at)
       > d.expected_frequency + COALESCE(d.freshness_warn_after, interval '0');
-- A separate, stricter HAVING clause (using freshness_fail_after) distinguishes
-- warn vs. fail severity as two Grafana alert rules, or one rule with two thresholds.
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|---------------|--------|
| Airflow tracing via a third-party provider (`airflow_otel_provider` or similar community packages) | Native `[traces] otel_on` + `airflow.sdk.observability.trace` | Airflow 2.7.0 added OTel metrics; Airflow 3.x's Task SDK made custom spans a first-class public API | No community provider needed; the official mechanism is sufficient, but cross-pod-boundary propagation is still explicitly DIY (unchanged from Airflow 2.x) |
| Grafana alerting via legacy dashboard alerts | Grafana **unified alerting**, provisioned as code via `alerting: {}` | Unified alerting has been Grafana's only alerting engine since v9-era releases; file-provisioning for it has been stable since Grafana 9.1 | Directly enables D-07's "one alerting engine" design without any legacy-alerting migration concern |
| `minio/minio` Python SDK / MinIO-specific tooling for object storage | N/A for this phase — not touched | — | Not relevant to Phase 7's scope; noted only because CLAUDE.md's own "What NOT to Use" table is adjacent context the planner may cross-reference |

**Deprecated/outdated:**
- Airflow's `otel_host`/`otel_port`/`otel_service`/`otel_debugging_on` config keys: deprecated in favor of standard `OTEL_EXPORTER_OTLP_*` environment variables. Already correctly noted in `.claude/CLAUDE.md` §H; repeated here because it's easy to find older blog posts (including some surfaced in this session's own WebSearch results) still showing the deprecated keys.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Exact YAML nesting shape for `grafana.alerting.{contactpoints,policies,rules}.yaml` under the kube-prometheus-stack `grafana:` block | Pattern 4 | If the actual nesting differs, `helm template` will render an empty or malformed provisioning ConfigMap; low risk since `helm template` + `helm lint` (already in this project's CI gate) will catch a structural YAML error before any cluster deploy |
| A2 | `meta.datasets` freshness column names (`expected_frequency`, `freshness_warn_after`, `freshness_fail_after`) and type (`interval`) | Pattern 3, Code Examples | Purely cosmetic if wrong — CONTEXT.md explicitly left exact naming to Claude's discretion; no functional risk, just a rename if the planner prefers different names |
| A3 | `build_pod_request_obj()` is the correct override point (vs. `execute()` itself) and is called early enough in `execute()` that the Airflow-managed span is already active | Pattern 1 | MEDIUM risk — if the active-span assumption is wrong (e.g., the span is created *after* `build_pod_request_obj()` runs), `opentelemetry.propagate.inject()` would write nothing and the trace would have no propagated parent. Recommend an early spike/prototype task (this project's own established pattern per STATE.md's "spikes with pre-declared pass criteria") to confirm empirically before building the full pipeline on this assumption |
| A4 | Tempo (single-binary, non-distributed) chart's current canonical Helm repo URL — some ArtifactHub listings show it under `grafana/grafana`, others show the sibling `tempo-distributed` chart has moved to `grafana-community/grafana-community` (mirroring the Grafana dashboard chart's own already-documented repo move in CLAUDE.md) | Standard Stack, Architecture Patterns | LOW risk, purely mechanical — `helm search repo tempo` against both candidate repo URLs at execution time resolves this in seconds; flagged only so the planner doesn't assume the un-moved URL without checking |
| A5 | OTel Collector chart's `prometheus` exporter + kube-prometheus-stack's `additionalServiceMonitors` is preferable to `prometheusremotewrite` + `enableRemoteWriteReceiver` | Common Pitfalls (Pitfall 6) | LOW risk — both are valid, well-documented patterns; the recommendation is a preference for staying inside kube-prometheus-stack's own idiomatic mechanism, not a correctness claim about the alternative being wrong |

**If this table is empty:** N/A — see entries above. Every `[ASSUMED]`-tagged claim in this document is captured here.

## Open Questions

1. **Should the custom Airflow image also carry `otel` for the CI (LocalExecutor) profile, or is CI exempt entirely?**
   - What we know: D-16 (Claude's discretion, already exercised in ROADMAP-level planning) leans toward CI staying template/lint-only with monitoring disabled; CI's `helm/values/ci/airflow.yaml` should therefore NOT set `otel_on: True` regardless of what the image contains.
   - What's unclear: whether CI's `manifests` job (which does `helm template` + `kubeconform`) needs the custom image reference to exist at all, or whether CI can keep referencing the stock image tag since it never actually deploys it.
   - Recommendation: build ONE custom image, reference it in both values profiles' `defaultAirflowTag` (consistency, avoids a second image to maintain), but leave `otel_on` unset/False in the CI values file. The image build itself does not need to run in CI unless CI later gains a live-cluster job that exercises tracing end to end (not currently planned per D-16).

2. **Does `apache-airflow[otel]`'s actual dependency list include a psycopg-instrumentation package that would auto-instrument Airflow's OWN metadata-DB queries?**
   - What we know: the `otel` extra's exact package list is not enumerated in Airflow's own docs (only "Required for OpenTelemetry metrics/traces" is stated).
   - What's unclear: whether installing it has any incidental effect on Airflow's own metadata-DB query visibility (out of scope for this phase either way, since Airflow's metadata DB is explicitly not part of this phase's trace scope).
   - Recommendation: not worth resolving before planning; verify by inspecting the built image's `pip list` output once `docker/airflow/Dockerfile` exists (cheap, one build).

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Docker | Building `docker/airflow/Dockerfile`, verifying image contents | ✓ | confirmed usable this session (`docker pull`/`docker run` succeeded) | — |
| `tools/bin/helm` | Deploying kube-prometheus-stack, Tempo, OTel Collector, updated Airflow chart | ✓ | pinned per `helm/versions.env` (not independently re-verified this session; project's own pinned-binary convention, stamped `tools/bin/.helm.stamp`) | — |
| `tools/bin/kind` | N/A directly (cluster already running, not recreated this phase) | ✓ | pinned per `helm/versions.env` | — |
| `kubectl` | Live-cluster verification, `kubectl exec`-based role password rotation (Pattern 5) | ✓ | v1.36.1 client, confirmed reaching a live cluster this session | — |
| Live kind cluster | All of this phase's deployment work | ✓ | Confirmed live this session — `airflow`, `data`, `etl`, `vault`, `cnpg-system`, `ingress-nginx` namespaces all `Active`, oldest 3d6h | — |
| Network egress (pull chart tarballs, OTel Collector/Tempo images, PyPI packages) | Everything in Standard Stack | ✓ | Confirmed this session (WebSearch, WebFetch, `pip index versions`, `docker pull` all succeeded) | — |

**Missing dependencies with no fallback:** none identified.
**Missing dependencies with fallback:** none identified — this phase's tooling dependencies are a strict subset of what Phases 1-6 already require and already have working.

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest 9.1.1 (already pinned, `pyproject.toml`) |
| Config file | `pyproject.toml` `[tool.pytest.ini_options]` — existing markers `cluster`, `manifests`, `integration`, `slow`, `regression` already cover this phase's needs; no new marker required |
| Quick run command | `uv run --frozen pytest tests/unit tests/property -q` |
| Full suite command | `uv run --frozen pytest -m cluster tests/e2e -q` (requires the live cluster confirmed available above) |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|---------------------|-------------|
| OBS-07 | `meta.v_customers_lineage` returns every named column for a real ingested row | integration | `pytest tests/integration/test_lineage_view.py -x` | ❌ Wave 0 |
| OBS-08 | `dataplat.observability.metrics.increment()` actually reaches the OTel Collector with bounded labels | integration | `pytest tests/integration/test_metrics_otlp.py -x` | ❌ Wave 0 |
| OBS-09 | A dataset with `expected_frequency IS NULL` never appears in the freshness alert query's result set; one with a stale `expected_frequency` does | integration (SQL-only, testcontainers Postgres) | `pytest tests/integration/test_freshness_query.py -x` | ❌ Wave 0 |
| OBS-10 | A `TRACEPARENT` env var appears in a real launched KPO pod's spec, and `dataplat`'s first span is a child of it | e2e, `cluster` marker (real pod, D-19's "proof over prose" bar) | `pytest tests/e2e/observability/test_trace_propagation.py -m cluster -x` | ❌ Wave 0 |
| D-20 (freshness alert webhook delivery) | A real freshness breach causes Grafana to actually POST to a test-reachable webhook receiver | e2e, `cluster` marker | `pytest tests/e2e/observability/test_alert_webhook_delivery.py -m cluster -x` | ❌ Wave 0 |

### Sampling Rate

- **Per task commit:** `uv run --frozen pytest tests/unit -q`
- **Per wave merge:** `uv run --frozen pytest -m "cluster or integration" tests/e2e tests/integration -q`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps

- [ ] `tests/e2e/observability/__init__.py`, `conftest.py` — new test package, no shared fixtures exist yet for this domain
- [ ] `tests/e2e/observability/test_alert_webhook_delivery.py` — covers D-20. **Key design constraint discovered this session:** the webhook receiver must be reachable from *inside* the kind cluster's pod network (Grafana's Alerting engine runs in-cluster and cannot reach a pytest-process-local `localhost` listener). Recommend a minimal receiver Pod+Service deployed into the cluster for the test's duration, with the test asserting delivery via `kubectl exec`/log inspection against that pod — not a host-local HTTP server. This is the most novel testing mechanism this phase introduces and deserves early prototyping.
- [ ] `tests/integration/test_lineage_view.py`, `test_metrics_otlp.py`, `test_freshness_query.py` — no existing files
- [ ] Framework install: none — pytest/testcontainers already present via the `cluster` dependency group

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | This phase adds no new end-user-facing authentication surface; Grafana's own auth is pre-existing and untouched |
| V3 Session Management | No | Not applicable — no new session-bearing interface |
| V4 Access Control | Yes | New `grafana_reader` PostgreSQL role: `SELECT`-only, no `INSERT`/`UPDATE`/`DELETE`, scoped to `meta`+`normalized` schemas only — mirrors the existing `etl_app` least-privilege pattern exactly |
| V5 Input Validation | Yes | New `FreshnessConfig` Pydantic model (`config/model.py`) with `extra="forbid"`/`frozen=True`, matching every other config model in this codebase; add a model validator if `warn_after`/`fail_after` ordering needs enforcing (warn ≤ fail) |
| V6 Cryptography | No new concern | Webhook URL and the new `grafana_reader` password both flow through Vault KV, inheriting Phase 5's already-established encryption-at-rest and TLS-in-transit posture — no new crypto surface introduced |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| A malicious/compromised alert rule definition exfiltrates data via a crafted webhook URL | Information Disclosure | The webhook contact point's URL is sourced from a single Vault-controlled Secret referenced by name in a committed, code-reviewed Helm values file — not editable at runtime through Grafana's UI in this IaC-only setup (no `grafana.ini` `[alerting] disabled_labels`/UI-edit path is enabled by this phase's design) |
| `meta.ingestion_runs.error_detail` (raw JSONB, may contain exception text/stack context) surfaced un-redacted through a future dashboard panel or the lineage view | Information Disclosure | `error_detail` is deliberately excluded from `meta.v_customers_lineage`'s column list in this research's drafted SQL (Code Examples) — flag this explicitly in the plan so a future "let's also show the error" addition doesn't reintroduce raw exception text into a Grafana panel without the same redaction discipline OBS-05 already applies at the logging layer |
| Over-privileged Grafana DB role (e.g., reusing `etl_app` or `analytics_owner` credentials for convenience) | Elevation of Privilege | New dedicated `grafana_reader` role, `SELECT`-only — explicitly do not reuse `etl_app` (which has `INSERT`/`UPDATE`) or `analytics_owner` (superuser-adjacent, owns the schemas) |
| Postgres datasource `sslmode: disable` in a values file lands as a template a future production deployment copies verbatim | Tampering (in transit) | Acceptable for this project's local-kind-cluster, in-cluster-only traffic (matches existing `etl_app` DSN's own posture — see `scripts/vault-bootstrap.py`'s DSN construction, no `sslmode` override there either); worth a one-line comment in the new values file noting this is a local-dev posture, consistent with how other local-only shortcuts are already commented elsewhere in this codebase |

## Sources

### Primary (HIGH confidence)
- Live `docker pull apache/airflow:3.3.0-python3.12` + `pip list` — this session, confirms zero `opentelemetry`/`otel` packages present
- Live `kubectl get ns` against the running kind cluster — this session, confirms `airflow`/`data`/`etl`/`vault`/`cnpg-system` all `Active`
- `helm/schemas/cnpg/cluster_v1.json` (this repo's own vendored CNPG CRD schema) — confirmed `spec.managed.roles[].{name,login,passwordSecret}` structure
- `scripts/vault-bootstrap.py` (read directly, lines 560-634) — confirmed the exact `kubectl exec` + `ALTER ROLE ... WITH PASSWORD` + Vault KV write pattern for `etl_app`
- `helm/values/local/cnpg-analytics.yaml` (read directly) — confirmed `postInitApplicationSQL` is the only role-creation mechanism currently used, and confirmed it is initdb-time-only by its own chart semantics
- `migrations/versions/0001,0002,0003,0004,0005,0008,0009*.py` (read directly) — every column name in the drafted `meta.v_customers_lineage` SQL and the `grafana_reader` GRANT pattern
- `packages/dataplat/src/dataplat/observability/{metrics,tracing,logging}.py`, `pipeline/engine.py`, `models/identity.py`, `metadata/postgres.py` (read directly) — confirmed existing no-op seams, threaded call sites, and already-modeled `trace_id`/`span_id` columns
- `docs/adr/0004-two-images-two-dependency-sets.md` (read directly) — grounds the "version skew is expected" pitfall and the two-images constraint on the new Dockerfile
- `slopcheck install <5 packages> --ecosystem pypi` — this session, all `[OK]`
- `pip index versions <package>` — this session, for all 5 new Python packages

### Secondary (MEDIUM confidence)
- `airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/traces.html` — `apache-airflow[otel]` extra, `airflow.sdk.observability.trace` custom-span API, no-op-safe-when-disabled behavior
- `airflow.apache.org/docs/apache-airflow/stable/extra-packages-ref.html` — `otel` extra confirmed present, package list not enumerated
- `raw.githubusercontent.com/apache/airflow/3.3.0/Dockerfile` — default `AIRFLOW_EXTRAS` list (cross-verified twice, consistent both times)
- `raw.githubusercontent.com/apache/airflow/helm-chart/1.22.0/chart/values.yaml` — `env`/`extraEnv`/`workers.kubernetes.env`/`scheduler.env`/`triggerer.env` keys
- `raw.githubusercontent.com/grafana-community/helm-charts/main/charts/grafana/values.yaml` — `alerting: {}`, `sidecar.alerts`, `envFromSecret`, `persistence` keys
- `raw.githubusercontent.com/prometheus-community/helm-charts/main/charts/kube-prometheus-stack/values.yaml` — `grafana.additionalDataSources` key and example shape
- `grafana.com/docs/grafana/latest/datasources/postgres/configure/` — `type: postgres` confirmed current, full provisioning example
- GitHub issues/discussions `apache/airflow#13348`, `#25841`, `#38522`, commit `85bc9af` — `env_vars` templating history

### Tertiary (LOW confidence, flagged for validation — see Assumptions Log)
- Tempo chart's exact current repo URL/org (`grafana/grafana` vs. a possible `grafana-community` move) — A4
- OTel Collector `prometheus`-exporter + `additionalServiceMonitors` vs. `prometheusremotewrite` preference — A5
- `grafana.alerting` exact nested YAML-under-YAML shape for multi-document provisioning files — A1

## Metadata

**Confidence breakdown:**
- Standard stack (Python packages): HIGH — every package verified via `pip index versions` + `slopcheck`, cross-corroborated with CLAUDE.md's own prior PyPI JSON API research
- Airflow image / otel extra requirement: HIGH — verified live via `docker pull`/`pip list`, not documentation alone
- Trace propagation mechanics (build_pod_request_obj override): MEDIUM — grounded in confirmed API facts (template_fields, propagate.inject/extract, execute()-time call ordering) but the end-to-end flow was not executed live this session; flagged for an early spike (A3)
- Helm chart values wiring (Grafana alerting, datasources, OTel Collector, Tempo): MEDIUM — WebFetch/WebSearch synthesis of current chart source, not independently rendered with `helm template` this session
- Freshness/lineage SQL and schema design: HIGH — every column name read directly from this project's own migration files
- Vault third-tier pattern for Grafana: HIGH — directly extends a pattern read verbatim from this project's own `scripts/vault-bootstrap.py`

**Research date:** 2026-08-15
**Valid until:** ~14 days for the Helm chart values specifics (fast-moving: kube-prometheus-stack/Grafana/OTel Collector chart versions advance roughly monthly per CLAUDE.md's own version table); ~30 days for the Python package and architecture-pattern findings (more stable, tied to Airflow 3.3.0/opentelemetry 1.44.x's own release cadence)
