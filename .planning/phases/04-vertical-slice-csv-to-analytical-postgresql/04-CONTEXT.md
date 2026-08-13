# Phase 4: Vertical Slice — CSV to Analytical PostgreSQL - Context

**Gathered:** 2026-08-13
**Status:** Ready for planning

<domain>
## Phase Boundary

One real CSV travels end to end — MinIO (`s3://raw/customers/`) → TaskFlow DAG → `KubernetesPodOperator` pod → `dataplat`/`csv_processor` → `normalized.customers` in analytical PostgreSQL — and is idempotent by construction, so a re-run produces zero additional rows.

This is the strictly serial critical path (ROADMAP "Wave B, ~15% of effort"). Scope is deliberately narrow: **one dataset (customers), one encoding (UTF-8), one delimiter (comma), no header edge cases.** Vault, encoding/dialect/header detection, schema inference, quality rules, quarantine, dedup strategies beyond exact-hash/business-key, CDC, SCD, Prometheus/Grafana/OTel export, and Dynamic Task Mapping beyond degree 1 are all explicitly out of scope (ARCHITECTURE.md's own "Out of scope" note for the slice).

</domain>

<decisions>
## Implementation Decisions

### File-arrival trigger
- **D-01:** The `csv_ingest_customers` DAG is woken by a **deferrable `S3KeySensor`** watching `s3://raw/customers/*.csv` (wildcard_match), not a plain scheduled polling DAG and not a MinIO-webhook-to-Airflow-API integration. Rationale discussed live: a scheduled poll wastes cycles even when nothing changed and has coarser (schedule-interval-bound) latency; a webhook is more "production-like" but requires a webhook receiver plus an Airflow API credential that has nowhere real to live before Vault lands in Phase 5 (ROADMAP explicitly protects this phase from that kind of scope creep — "Vault comes after the slice... putting [it] on the critical path... is how the slice slips"). The deferrable sensor uses the triggerer (already deployed in Phase 2), holds zero worker slots while idle, and needs no new infrastructure or credentials.
- **D-02:** Poke interval: **30 seconds**.
- **D-03:** DAG has `max_active_runs=1` — a new sensor trigger cannot overlap an in-progress load of the same dataset. (The run-claim idempotency protocol would still prevent duplicate rows without this, but capping runs avoids two DAG runs racing pointlessly over the same advisory lock.)
- **D-04:** If several files land within the same poke window, **one DAG run processes all currently-visible new files** — `discover_files` lists everything new since the last successful watermark and builds one frozen manifest (ORCH-08), with each file becoming one Dynamic-Task-Mapping unit. Not one-file-one-run.

### Slice CSV content & volume
- **D-05:** CSV content is **synthetic, Faker-style data**, generated from a seed — consistent with the Phase 1 corpus policy (QUAL-08: "corpus is the specification... generated from a seed rather than committed en masse").
- **D-06:** A **separate ~1M row fixture** is generated specifically for the U3 streaming-throughput + peak-RSS spike baseline — large enough to force multiple staging chunks past the default `checkpoint_threshold_rows` (500k, ARCHITECTURE.md Q7) and produce a meaningful sustained measurement.
- **D-07:** A **separate small fixture (~50–200 rows)** is used for fast E2E/idempotency assertions (rerun-same-DAG-run, re-upload-under-different-name) that run on every CI pass. The 1M-row fixture is spike-only and is not exercised on every CI job.
- **D-08:** Fixture generation **extends `tools/corpus/`** (the existing seeded, byte-identical-regeneration framework from Phase 1) rather than introducing a second generator mechanism.

### Pod-kill / retry demonstration (success criterion #3)
- **D-09:** The deliberate mid-load pod kill is a **real `kubectl delete pod`** against a pod loading the ~1M row fixture (not a self-kill via a test-only crash env var) — exercises the genuine Kubernetes reschedule + Airflow retry + run-claim lease-takeover path, not a self-inflicted process exit.
- **D-10:** This becomes a **permanent automated E2E test** (`tests/e2e/`), not a one-off manual proof — matches QUAL-06/QUAL-09 and the project's QUAL-07 policy that important behaviors get a permanent regression test.
- **D-11:** The test detects "pod is mid-load" by **polling `meta.ingestion_runs.rows_read` with a timeout** (never `sleep N` — PITFALLS.md explicitly flags `sleep` in E2E tests as a permanent-flakiness trap), reusing the platform's own heartbeat/lease mechanism (`lease_expires_at`) rather than watching pod logs for a marker string.
- **D-12:** Success criterion #3's second half — "a concurrent SELECT never observes a half-loaded table" — gets its **own dedicated test**: a concurrent connection polls `normalized.customers` during an in-flight publish and asserts it only ever observes the pre-publish or fully-published row count, never a partial state. Not left as an inference from the retry test.
- **D-13:** `configs/datasets/customers.yaml` gets an explicit **duplicate-file-content policy of `skip`** — when a re-uploaded file's content hash (`content_sha256`) matches a file already known for this dataset, it is recorded (`duplicate_of_file_id` set) but never (re)processed. This is what makes success criterion #2's "re-uploading the same file under a different name produces zero additional rows" true by early-exit rather than by relying on deeper record/publish-layer guards.

### Local dev/demo workflow
- **D-14:** A **Makefile target**, `make ingest-demo FILE=<path>`, is the developer-facing way to exercise the slice — consistent with this repo's existing `make cluster-up` / `make doctor` / `make fixtures` convention.
- **D-15:** The target does **not** bypass the sensor by also triggering the DAG via CLI. Explicit user instruction: "Do not take shortcuts for demo, quick tests... let sensor do its job." The demo must exercise the real unattended path, not a dev-only shortcut — `mc cp` the file in and let the `S3KeySensor` notice it.
- **D-16:** While waiting, the target **polls `meta.ingestion_runs` (with a timeout, not a blind sleep) and prints the receipt** (`run_id`, `status`, `rows_loaded`, `duration_ms`, etc.) once the run reaches a terminal status — self-contained feedback without needing to switch to the Airflow UI.

### Claude's Discretion
- Exact Makefile target implementation details (how it resolves the run row for a given uploaded file, exact receipt formatting).
- Whether `tools/corpus/`'s existing generator needs structural changes to support realistic (non-edge-case) Faker-style data, versus adding a new generation path within that same package.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase scope, requirements and success criteria
- `.planning/ROADMAP.md` § "Phase 4: Vertical Slice — CSV to Analytical PostgreSQL" — goal, success criteria, requirements list (ORCH-01..09, META-03, LOAD-01/02/03/04/05/08/09/12, INCR-08, QUAL-05/06/09), and the detailed plan guidance (spikes U1/U3, pod amplification cap, XCom receipt rules, cheap-now decisions PITFALLS #3/#4/#8/#9/#12/#14).
- `.planning/REQUIREMENTS.md` — full text of every Phase 4 requirement.

### Architecture — the primary design source for this phase
- `.planning/research/ARCHITECTURE.md` — §"Vertical slice = exactly these components" (in/out of scope), the dependency-ordered component table (items 1–12), §6.2 assignment document shape, §6.3 receipt shape and XCom rules, §6.4 guardrails table (XCom bloat, fan-out ceiling, runaway concurrency, orphaned pods, stale RUNNING runs), §6.5 the `logical_date = None` asset/API-trigger trap, Question 7 (Idempotency Mechanics — all 4 layers: file identity, run identity/claim protocol, record identity, target-row identity), the checkpointing × transactions table, and the concurrency/races section (`pg_advisory_xact_lock`, run-claim protocol, Airflow pools).
- `.planning/research/PITFALLS.md` — line ~1995 (poll until the DAG is parsed and unpaused, never `sleep`), line ~2168 (`sleep` in E2E tests is a permanent-flakiness trap — directly informs D-11).
- `.planning/research/SUMMARY.md` — deviation D1 (idempotency inside the vertical slice) and D3 (Vault after the slice, behind `SecretsResolver`) — directly informs D-01's rationale.
- `.planning/research/STACK.md` §C (PostgreSQL — `psycopg` COPY/MERGE guidance), §B (Airflow — KubernetesExecutor/KPO settings).

### ADRs from Phase 3 this phase builds on directly
- `docs/adr/0008-pipeline-composition-seam.md` — the `Source` → `RecordChunk` → `Stage` → `Publisher` protocol set this phase's `merge` Publisher implements.
- `docs/adr/0002-dataplat-core-with-csv-processor-plugin.md` — the `dataplat`/`csv_processor` package split.
- `docs/adr/0004-two-images-two-dependency-sets.md` — why `csv_processor` is never installed into the Airflow image.

### Existing code this phase extends (not replaces)
- `packages/dataplat/src/dataplat/load/publish/protocol.py` — `Publisher` protocol; this phase writes the first concrete implementation (`merge`, using `INSERT ... ON CONFLICT` per LOAD-09/PITFALLS #14, not literal SQL `MERGE`).
- `packages/dataplat/src/dataplat/pipeline/engine.py` — `run_streaming` sequencing loop and `RaggedRowGuard`; this phase's staging/publish stages plug into this.
- `packages/dataplat/src/dataplat/sources/protocol.py`, `packages/csv-processor/src/csv_processor/source.py` — the naive `CsvSource`/`CsvRecordStream` (hardcoded UTF-8/comma/header-row-0) this phase's pod entrypoint reads through.
- `packages/dataplat/src/dataplat/cli.py` — the `ingest` subcommand this phase adds attaches to the existing `cli` click group and inherits its structured-logging/catch-once error boundary.
- `migrations/versions/0001..0005_*.py` — `meta.datasets`, `meta.config_versions`, `meta.files`, `meta.batches`/`meta.batch_files`, `meta.ingestion_runs`, `normalized.customers` — all schema this phase's runs read and write; `meta.batches` already carries `UNIQUE (dataset_id, batch_key)` (LOAD-08) and `meta.ingestion_runs` already carries `idempotency_key UNIQUE` (the run-claim mechanism).
- `configs/datasets/customers.yaml` — the dataset config this phase's DAG resolves and pins per run; this phase adds the duplicate-file-content policy (D-13).
- `kubernetes/namespaces.yaml` — existing `airflow`/`data`/`data-etl` namespaces the KPO pod and its service account are declared into.
- `tools/corpus/` — the seeded fixture generator this phase's slice fixtures extend (D-08).

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `dataplat.pipeline.engine.run_streaming` — the generic chunk-sequencing loop every stage (including this phase's staging COPY stage) plugs into; already threads tracing spans and checkpoint ordinals.
- `dataplat.config.registry.ConfigRegistry` / `dataplat.config.loader` — resolves and hashes `configs/datasets/customers.yaml` into a `config_version_id`; the DAG's `resolve_config` task calls this directly rather than reimplementing config resolution.
- `dataplat.metadata.repository.MetadataRepository` and `dataplat.storage.objectstore.ObjectStore` — the metadata/S3 access points `PipelineContext` composes; already proven against testcontainers in Phase 3.
- `dataplat.secrets.resolver.SecretsResolver` — resolves the dev-only DSN this phase's pod uses via `env://`/`file://`; no Vault-specific code path to write.
- `dataplat.cli.cli` click group + catch-once error boundary — the `ingest` subcommand this phase adds inherits structured logging and exit-code handling for free.

### Established Patterns
- **`Source` → `RecordChunk` → `StreamingStage`/`BarrierStage` → `Publisher`** (ADR-0008) — this phase's staging write is a `StreamingStage` (checkpointed per chunk); publication is a `BarrierStage`/`Publisher` (never checkpointed — the atomicity boundary per ARCHITECTURE.md's checkpointing × transactions table).
- **Errors as values, not exceptions** (QUAL-03, `RaggedRowGuard` precedent) — row-level problems become `RejectedRecord`s; only run-fatal conditions raise `DataPlatformError` subclasses.
- **`hash_version` companion column on every stored hash** (META-02) — `normalized.customers._record_hash_version` already exists; this phase is the first to actually populate `_record_hash` at runtime.

### Integration Points
- `airflow/dags/` is currently empty (`.gitkeep` only) — this phase adds the first real DAG file(s): the smoke DAG (U1, `dataplat --version` via KPO) and `csv_ingest_customers`.
- `docker/csv-processor/` already builds a non-root, git-SHA-tagged image (Phase 3) — this phase's `ingest` subcommand ships inside that same image; no new image is needed.
- `meta.ingestion_runs` already has `dag_id`/`dag_run_id`/`task_id`/`map_index`/`k8s_pod_name`/`trace_id` columns ready to receive Airflow-side identifiers the moment a real DAG exists.

</code_context>

<specifics>
## Specific Ideas

- The demo/dev workflow must exercise the *real* unattended path end to end — no CLI-trigger shortcut, even for convenience during active development (D-15, explicit user instruction).
- The pod-kill retry test should feel like a genuine crash (`kubectl delete pod`), not a simulated one, and should reuse the platform's own metadata (heartbeat/lease) as its synchronization point rather than any side channel like log-scraping or sleeps.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope. (MinIO-webhook-based triggering was considered and explicitly rejected for this phase per D-01, not deferred as a future idea — ROADMAP's Phase 11 ops/runbook work or a later phase would be the natural place to revisit it if ever wanted.)

</deferred>

---

*Phase: 4-Vertical Slice — CSV to Analytical PostgreSQL*
*Context gathered: 2026-08-13*
