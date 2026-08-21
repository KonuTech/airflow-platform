# Requirements: Airflow ETL Platform

**Defined:** 2026-08-11
**Core Value:** Every file, batch and record that enters the platform can be traced, explained, reprocessed and trusted.

## Provenance

Requirements derive from three sources, all traceable:

- **`DoD n`** — item *n* of README §94's 114-item Definition of Done. All 114 are mapped, exactly once each — verified arithmetically: no duplicates, no omissions, no extraneous citations.
- **`Gap n`** — one of 16 capabilities the README specifies in prose but never made checkable (see `.planning/research/FEATURES.md`). These are additions, not inventions: §82 observability and §83 lineage have no DoD items at all, and compression appears nowhere in the 95 sections despite being ubiquitous in real feeds.
- **`PITFALLS #n` / architecture decisions** — 12 requirements encoding constraints that are cheap to satisfy now and unrecoverable later. These exist because research identified them as the project's dominant rework and data-corruption risks, and a constraint that lives only in a research document does not get built. They are: `META-02`, `META-03`, `INFRA-09`, `INFRA-10`, `SEC-15`, `ORCH-08`, `CSV-13`, `LOAD-08`, `LOAD-12`, `INCR-08`, `SCD-12`, `OBS-10`.

**Total: 138 v1 requirements** across 14 categories (114 DoD + 16 gaps + 12 research-derived constraints, minus 4 DoD items moved to Out of Scope — see CDC below).

Scope is the full platform — every DoD item is v1. The v2 section holds only capabilities that were never in the README, and Out of Scope holds explicit exclusions.

---

## v1 Requirements

### META — Control-Plane Metadata Schema

> Research called this "the single strongest structural recommendation" and marked it CRITICAL. Watermarks, dedup audit, validation results, the schema registry, the batch ledger and lineage are all writes into **one** schema. Accreting it capability-by-capability guarantees inconsistent foreign keys and six migrations.

- [x] **META-01**: A single coherent `meta` schema in analytical PostgreSQL is designed up front and migrated via Alembic, covering datasets, config versions, files, batches, ingestion runs, run stages, schema versions, watermarks, dedup audit, validation results and reconciliation results — *(Gap 1)*
- [x] **META-02**: Every stored content hash carries a `hash_version` column, so the hash recipe can change without invalidating history or making every dimension appear to change at once — *(PITFALLS #1 cheap-now decision)*
- [x] **META-03**: Data rows, watermark advancement and run status commit inside a single publication transaction, or none of them do — *(ARCHITECTURE claim 4)*

### INFRA — Cluster, Services, Infrastructure as Code

- [x] **INFRA-01**: A multi-node kind cluster (control-plane + 2 workers) can be destroyed and recreated reproducibly from a committed `kind/cluster.yaml` — *(DoD 1)*
- [x] **INFRA-02**: Airflow 3.x runs in Kubernetes with API server, scheduler, DAG processor and triggerer as separate workloads — *(DoD 2)*
- [x] **INFRA-03**: A PostgreSQL instance dedicated exclusively to Airflow metadata is deployed and used by nothing else — *(DoD 3)*
- [x] **INFRA-04**: A physically separate PostgreSQL instance serves analytical workloads — *(DoD 4)*
- [x] **INFRA-05**: MinIO provides S3-compatible object storage with `raw`, `validated`, `processed`, `quarantine` and `metadata` layers — *(DoD 5)*
- [x] **INFRA-06**: HashiCorp Vault is deployed in-cluster as the secrets manager and survives cluster restart without manual data loss — *(DoD 6)*
- [x] **INFRA-07**: All infrastructure is defined as code — cluster config, Helm values and manifests are committed and no step requires manual `kubectl` surgery — *(DoD 7)*
- [x] **INFRA-08**: Container images are reproducible and versioned by git SHA, never deployed as `:latest` — *(DoD 8)*
- [x] **INFRA-09**: Kubelet reservations, `maxPods` and `extraMounts` are set in the kind config at creation time, because changing them later requires destroying the cluster — *(PITFALLS #10, #11)*
- [x] **INFRA-10**: Two Helm values profiles (`values-local.yaml`, `values-ci.yaml`) exist from the first infrastructure commit, since the full stack does not fit a 4 CPU / 16 GB CI runner — *(PROJECT.md constraint)*
- [ ] **INFRA-11**: Configurable retention policies are enforced for raw files, processed files, quarantine, validation reports, ingestion metadata and logs, separately from processing logic — *(Gap 11)*

### SEC — Secrets Management and Workload Identity

- [x] **SEC-01**: A dedicated secrets-management solution is deployed and is the only source of runtime credentials — *(DoD 90)*
- [x] **SEC-02**: No secret exists anywhere in git history or the working tree — *(DoD 91)*
- [x] **SEC-03**: No credential is hard-coded in Python source — *(DoD 92)*
- [x] **SEC-04**: No secret is baked into any container image — *(DoD 93)*
- [x] **SEC-05**: Airflow resolves connections through the Vault secrets backend, verified with the metadata-DB connection deleted and `AIRFLOW_CONN_*` unset — the backend fails open, so a test that skips this proves nothing — *(DoD 94)*
- [x] **SEC-06**: Task pods obtain only the credentials their workload requires, via explicit `namespace` and `service_account_name` matched to a Vault role — *(DoD 95)*
- [x] **SEC-07**: Workloads authenticate with least-privilege Kubernetes service-account identities, not a shared root token — *(DoD 96)*
- [x] **SEC-08**: Secret access is auditable — which workload read which path, when, and whether it succeeded — without logging secret values — *(DoD 97)*
- [x] **SEC-09**: Secret rotation is documented, including which credentials need a workload restart and which refresh dynamically — *(DoD 98)*
- [x] **SEC-10**: CI/CD holds no unnecessary long-lived credentials, and secrets are never printed during CI execution — *(DoD 99)*
- [x] **SEC-11**: Automated secret scanning runs in CI and fails the build on a detected credential — *(DoD 100)*
- [x] **SEC-12**: A negative test proves an unauthorized service account is *denied* access to another workload's secrets — if this test is awkward to write, the identity model is not real — *(DoD 101)*
- [x] **SEC-13**: Development secrets are clearly marked, never committed, isolated from production, and reproducible when rebuilding the local environment — *(DoD 102)*
- [x] **SEC-14**: The secrets architecture is documented end-to-end — injection mechanism, trust boundaries, and how a production secrets manager would substitute without application code changes — *(DoD 111)*
- [x] **SEC-15**: The ETL library resolves credentials through an opaque `SecretsResolver` reference (`env://`, `file://`) and never learns which backend served it — making the Kubernetes-Secrets→Vault swap a configuration change — *(ARCHITECTURE deviation D3)*

### ORCH — Airflow Orchestration

- [x] **ORCH-01**: DAGs are written with the TaskFlow API (`@dag`, `@task`) — *(DoD 9)*
- [x] **ORCH-02**: ETL workloads execute in Kubernetes task pods via `KubernetesPodOperator`, never in the scheduler or DAG processor — *(DoD 10)*
- [x] **ORCH-03**: Dynamic Task Mapping fans work across pods, with a bounded map length — *(DoD 11)*
- [x] **ORCH-04**: DAGs declare explicit retry and failure behaviour, and support backfill — *(DoD 12)*
- [x] **ORCH-05**: DAGs derive their processing window from logical date and data interval, never wall-clock time — and tolerate `logical_date` being `None` in asset-triggered runs — *(DoD 13)*
- [x] **ORCH-06**: DAG files stay under ~150 lines and contain no parsing, validation, typing or database writes — *(DoD 14)*
- [x] **ORCH-07**: Dataset dependencies are expressed in Airflow via Assets or sensors, not hidden inside Python — *(DoD 58)*
- [x] **ORCH-08**: Dynamic Task Mapping expands over a frozen manifest, never a live object-storage listing, so reruns and backfills produce identical work — *(PITFALLS #4)*
- [x] **ORCH-09**: Every task pod declares CPU and memory requests and limits, configurable per workload and dataset — *(Gap 15)*

### CSV — Parsing, Detection and Normalization

- [x] **CSV-01**: Filenames are parsed via configurable masks and regular expressions, extracting dataset, source, country, business date, version, batch and sequence where present — without assuming any date found is the business date — *(DoD 15)*
- [x] **CSV-02**: UTF-8, UTF-8 BOM, UTF-16 LE/BE, Windows-1250, Windows-1252, ISO-8859 variants and ASCII files all parse correctly — *(DoD 16)*
- [x] **CSV-03**: Encoding detection returns an encoding with a confidence score, and never claims determinism it does not have — *(DoD 17)*
- [x] **CSV-04**: Comma, semicolon, pipe, tab and colon dialects all parse correctly — *(DoD 18)*
- [x] **CSV-05**: Delimiter detection is supported and can be overridden by contract — *(DoD 19)*
- [x] **CSV-06**: Quoted delimiters, escaped quotes, multiline fields and inconsistent quoting are handled by a real CSV parser — never by string splitting — *(DoD 20)*
- [x] **CSV-07**: Header detection handles header-present, header-absent and header-at-a-later-row cases — *(DoD 21)*
- [x] **CSV-08**: Metadata preambles, comments, blank lines, report titles, footers and totals rows are detected and excluded from data — *(DoD 22)*
- [x] **CSV-09**: Invalid dates (`2026-02-30`, `31/02/2026`, `2026-13-01`, `not-a-date`) produce explicit validation errors and are never silently coerced or dropped — *(DoD 28)*
- [x] **CSV-10**: Numeric, boolean and NULL values normalize per configuration — decimal comma/point, thousands separators, parenthesised negatives, currency, percentages, scientific notation, `Y/N`, `T/F`, `N/A` — without `1/0` becoming boolean absent evidence — *(DoD 29)*
- [x] **CSV-11**: Compressed inputs (`.gz`, `.zip`) and multi-part datasets are supported — absent from all 95 README sections, ubiquitous in real feeds — *(Gap 13)*
- [x] **CSV-12**: Unicode normalization (NFC/NFD) is applied before hashing, since NFC/NFD variants of the same value otherwise break deduplication and produce phantom SCD2 versions — *(Gap 16)*
- [x] **CSV-13**: Files stream through a single `csv.reader` over a `newline=""` wrapper and are chunked in *records*, never by lines or byte offsets, so embedded-newline fields survive — *(PITFALLS #5)*

### SCHEMA — Inference, Contracts, Versioning and Drift

- [x] **SCHEMA-01**: Types are inferred conservatively — `001234` stays a string when it may be an identifier — *(DoD 23)*
- [ ] **SCHEMA-02**: Explicit YAML data contracts declare types, nullability, required columns, business keys and semantics, and incoming data is validated against them — *(DoD 24)*
- [x] **SCHEMA-03**: Schemas are versioned, and each batch records dataset, schema version, schema hash, processor version and processing timestamp — *(DoD 25)*
- [x] **SCHEMA-04**: Added, removed, renamed, reordered and retyped columns are classified as compatible or breaking, per a configurable per-dataset policy — *(DoD 26)*
- [x] **SCHEMA-05**: Drift against previously observed schemas is detected and *reported*, never silently adapted to — *(DoD 27)*
- [x] **SCHEMA-06**: Historical files process under their historical schema version rather than being forced through the newest — *(DoD 51)*
- [x] **SCHEMA-07**: Processing configuration is versioned and hashed, and every run records which config version produced it — *(Gap 9)*

### VALID — Validation, Quality, Quarantine and Reconciliation

- [x] **VALID-01**: Structural validation reports expected vs actual column count, malformed rows, unclosed quotes and missing delimiters with row number, column where possible, error type and diagnostics — *(DoD 30)*
- [x] **VALID-02**: Data-quality validation covers completeness, uniqueness, validity ranges, patterns and referential integrity, with configurable thresholds producing PASS / PASS_WITH_WARNING / FAIL / QUARANTINE — *(DoD 31)*
- [x] **VALID-03**: Invalid data is quarantined per configurable strategy (`FAIL_FILE`, `REJECT_RECORD`, `QUARANTINE_FILE`, `QUARANTINE_RECORD`, `WARN_AND_CONTINUE`), retaining source file, row number, error, run and timestamp — and never silently discarded — *(DoD 32)*
- [x] **VALID-04**: Machine-readable validation reports are produced and persisted as rows in PostgreSQL as well as artifacts in MinIO — *(DoD 33)*
- [x] **VALID-05**: Source-to-target reconciliation compares record counts, sums, checksums, min/max and key counts, reporting discrepancies explicitly — *(DoD 55)*
- [ ] **VALID-06**: Source-provided control totals are validated against the loaded target — *(DoD 56)*
- [x] **VALID-07**: Referential integrity between datasets is validated, with configurable `fail` / `quarantine` / `warn` behaviour on orphan records — *(DoD 57)*
- [x] **VALID-08**: Quarantined data has a documented re-drive path back into the pipeline after correction — quarantine without an exit is a data graveyard — *(Gap 7)*
- [x] **VALID-09**: Volume and quality anomalies are detected against configurable statistical thresholds using persisted historical baselines — no ML — *(Gap 12)*

### LOAD — Identity, Idempotency and Transactional Loading

- [x] **LOAD-01**: Re-running any DAG, task, file, batch or record produces no duplicate or corrupted data — across Airflow retries, pod restarts, DAG reruns, backfills, manual reprocessing and re-uploaded files — *(DoD 34)*
- [x] **LOAD-02**: An Airflow retry mid-load creates no duplicate rows, proven by test — *(DoD 35)*
- [x] **LOAD-03**: Reprocessing an identical file is a no-op, identified by content checksum rather than filename — *(DoD 36)*
- [x] **LOAD-04**: File identity, batch identity, record identity and target-row identity are modelled distinctly and never conflated — *(DoD 37)*
- [x] **LOAD-05**: Loads are transactional — staging table, validation, then atomic publication — so consumers never observe a partially loaded dataset — *(DoD 52)*
- [ ] **LOAD-06**: After a partial failure the platform determines what succeeded, what remains, and whether retry or rollback is required, without manual log inspection — *(DoD 53)*
- [x] **LOAD-07**: Files larger than container memory process in bounded memory with configurable batch size and maximum field/row length — *(DoD 54)*
- [x] **LOAD-08**: A batch ledger with `UNIQUE (dataset, batch_key)` plus run-scoped identity (`run_id`, `attempt`) on every staged and loaded row exists from the vertical slice onward — *(PITFALLS #3, research deviation D1)*
- [x] **LOAD-09**: Publication is single-writer via `pg_advisory_xact_lock` per dataset, using `INSERT … ON CONFLICT` arbitrating on the natural key — `MERGE` is not concurrency-safe — *(Gap 10, PITFALLS #14)*
- [x] **LOAD-10**: File integrity is verified before processing — checksum, size, extension, object metadata, transfer completion and optional control file — so partially uploaded files are never ingested — *(Gap 4)*
- [x] **LOAD-11**: Optional batch manifests and completion markers (`_BATCH_COMPLETE`) are supported, and the manifest may be the authoritative input to a run — *(Gap 5)*
- [ ] **LOAD-12**: The ETL processor is the only component that parses CSV — PostgreSQL `COPY … FORMAT csv` on raw input is prohibited, since it would load rows validation never saw — *(PITFALLS #9)*

### DEDUP — Deduplication and Audit

- [x] **DEDUP-01**: Duplicate records within a single file are detected — *(DoD 38)*
- [x] **DEDUP-02**: Duplicate records across files, batches and ingestion runs are detected — *(DoD 39)*
- [x] **DEDUP-03**: Deduplication strategy is selected per dataset through a strategy interface supporting exact-row hash and business-key at minimum, extensible to key+timestamp, latest-wins, source-priority and batch-aware — never `SELECT DISTINCT` — *(DoD 40)*
- [x] **DEDUP-04**: Deduplication is auditable — source file, batch, dataset, records received/accepted/rejected/deduplicated, strategy and duplicate count are retained, with enough information to explain why a record was removed — *(DoD 41)*

### INCR — Incremental Processing, Late Data and Backfills

- [ ] **INCR-01**: Incremental processing works without full dataset reloads, via timestamp/watermark, monotonic ID, batch ID or file-based strategies — *(DoD 42)*
- [ ] **INCR-02**: Watermarks advance only from observed *committed* cursor values, lagged, inside the publication transaction — and use `>=` with idempotent merge, never `>` — *(DoD 43)*
- [x] **INCR-03**: Records arriving after their expected window are routed to the correct historical partition and never discarded — *(DoD 47)*
- [x] **INCR-04**: Records arriving out of event-time order are handled correctly — *(DoD 48)*
- [ ] **INCR-05**: Backfills run through the same pipeline as normal ingestion — discovery, inspection, validation, normalization, deduplication, load, lineage — with no simplified bypass path — *(DoD 49)*
- [ ] **INCR-06**: Backfills are idempotent, use correct historical files, respect historical schema versions and handle missing files explicitly — *(DoD 50)*
- [ ] **INCR-07**: Analytical data can be rebuilt from the immutable raw layer — *(DoD 114)*
- [ ] **INCR-08**: Business date is derived from the data, never from wall-clock or `logical_date`, so backfilled rows do not carry today's effective date — *(PITFALLS #8)*

### SCD — Slowly Changing Dimensions

> CDC (Change Data Capture) was dropped from v1 — see **Out of Scope** below for DoD 44/45/46/87 and reasoning. SCD 0/1/2 build entirely from CSV batches and do not depend on it.

- [ ] **SCD-01**: SCD Type 0 retains original values — *(DoD 60)*
- [ ] **SCD-02**: SCD Type 1 overwrites without history — *(DoD 61)*
- [ ] **SCD-03**: SCD Type 2 maintains historical versions with `valid_from`, `valid_to` and `is_current` — *(DoD 62)*
- [ ] **SCD-04**: Business/natural keys and surrogate keys are distinct, with the surrogate key independent of the change hash so late corrections cannot mutate it — *(DoD 63)*
- [ ] **SCD-05**: Change detection is deterministic via a normalized hash of tracked attributes — normalization strictly precedes hashing — *(DoD 64)*
- [ ] **SCD-06**: Effective dating distinguishes source effective time, business effective time, event time, ingestion time and processing time, and never defaults to ingestion time — *(DoD 65)*
- [ ] **SCD-07**: Late-arriving changes correct historical validity intervals per dataset policy, by recomputing a key's history from an ordered event log rather than in-place interval surgery — *(DoD 66)*
- [ ] **SCD-08**: Configurable DELETE semantics (`ignore | invalidate | new_record`) apply when a business key disappears from a full snapshot — *(DoD 67, re-scoped: source is full-snapshot CSV batches, not a CDC feed)*
- [ ] **SCD-09**: Repeated or replayed identical events produce exactly one logical version — *(DoD 68)*
- [ ] **SCD-10**: SCD processing is idempotent under re-application — *(DoD 69)*
- [ ] **SCD-11**: SCD processing supports backfills without blindly overwriting current dimension state — *(DoD 70)*
- [ ] **SCD-12**: Every SCD2 dimension carries a `btree_gist` exclusion constraint on `(business_key, validity range)` in its creating migration — once overlapping intervals exist the constraint can never be added and every as-of query is silently wrong — *(PITFALLS #2)*

### OBS — Observability, Lineage and Operations

- [x] **OBS-01**: Data freshness is tracked — last received, last successful processing, expected frequency and processing delay — *(DoD 59)*
- [x] **OBS-02**: All Python code uses structured application logging that works in local, Docker, Kubernetes and Airflow task-pod contexts — *(DoD 74)*
- [x] **OBS-03**: No `print()` is used for operational logging — enforced in CI — *(DoD 75)*
- [x] **OBS-04**: Logs carry contextual fields — filename, object path, dataset, stage, row number, schema version, validation status, duration and Airflow identifiers — *(DoD 76)*
- [x] **OBS-05**: Passwords, keys, tokens, secrets, unnecessary PII and whole sensitive records are never logged — *(DoD 77)*
- [ ] **OBS-06**: Operational runbooks document symptoms, diagnosis, recovery, reprocessing and verification for each §89 scenario — *(DoD 112)*
- [x] **OBS-07**: Lineage is queryable by SQL — for any row, its source file, object path, checksum, batch, ingestion timestamp, DAG/run/task ID, processor version, schema version and config version — *(Gap 2)*
- [x] **OBS-08**: Platform metrics are exposed — `files_processed`, `files_failed`, `rows_processed`, `rows_invalid`, `rows_deduplicated`, `processing_duration`, `validation_failures`, `data_freshness` — with bounded label cardinality, unbounded identity living in the metadata DB — *(Gap 3)*
- [x] **OBS-09**: "No file currently available" is distinguished from "file expected but missing", with configurable warning or failure behaviour per expected frequency — *(Gap 6)*
- [x] **OBS-10**: Distributed traces span Airflow task → task pod → processor → PostgreSQL, via explicit W3C `traceparent` propagation — it is not automatic across the pod boundary — *(PROJECT.md decision)*

### QUAL — Engineering Standards and Test Coverage

- [x] **QUAL-01**: Type hints are used consistently across arguments, returns, classes, public APIs, configuration and data models, verified by mypy in CI — *(DoD 71)*
- [x] **QUAL-02**: Public classes, functions and methods carry docstrings describing purpose, parameters, returns, assumptions, exceptions and side effects — *(DoD 72)*
- [x] **QUAL-03**: Error handling is explicit via a domain exception hierarchy for run-fatal conditions, with row-level data problems flowing as values rather than exceptions, and no silent swallowing — *(DoD 73)*
- [ ] **QUAL-04**: Unit tests cover filename parsing, encoding/dialect/header detection, schema inference, structural and type validation, normalization, deduplication, incremental logic and validation reports — *(DoD 78)*
- [x] **QUAL-05**: Integration tests exercise MinIO → processor → PostgreSQL including storage operations, transactions and quarantine — *(DoD 79)*
- [x] **QUAL-06**: End-to-end tests exercise CSV → MinIO → Airflow → Kubernetes → processor → PostgreSQL — *(DoD 80)*
- [x] **QUAL-07**: Every important discovered bug gains a permanent regression test — *(DoD 81)*
- [x] **QUAL-08**: A CSV edge-case fixture corpus exists, generated from a seed rather than committed en masse, and grows as cases are discovered — the corpus is the specification — *(DoD 82)*
- [x] **QUAL-09**: Idempotency is tested, including the assertion that a re-run produces zero additional rows — *(DoD 83)*
- [x] **QUAL-10**: Deduplication is tested within files, across files and across batches — *(DoD 84)*
- [ ] **QUAL-11**: Backfills are tested for idempotency and historical schema resolution — *(DoD 85)*
- [x] **QUAL-12**: Schema evolution is tested for compatible and breaking changes — *(DoD 86)*
- [ ] **QUAL-14**: SCD is tested including late-arriving corrections and idempotent re-application — *(DoD 88)*
- [ ] **QUAL-15**: Failure and recovery scenarios from §84 are tested — pod crash, database unavailable, MinIO unavailable, Vault unavailable, malformed CSV, invalid encoding, OOM, task timeout, duplicate batch, secret rotation, unauthorized secret access — *(DoD 89)*
- [x] **QUAL-16**: A property test asserts determinism — identical source data, configuration and processor version produce an identical output hash — *(Gap 8)*
- [x] **QUAL-17**: Timezone and DST correctness is tested as a property, including DST gap and overlap timestamps — *(Gap 14)*

### CICD — Continuous Integration and Delivery

- [x] **CICD-01**: GitHub Actions provides CI/CD — *(DoD 103)*
- [x] **CICD-02**: Pull requests automatically run the full quality gate — *(DoD 104)*
- [x] **CICD-03**: Linting runs automatically via ruff — *(DoD 105)*
- [x] **CICD-04**: Type checking runs automatically via mypy — *(DoD 106)*
- [ ] **CICD-05**: Unit, integration and E2E tests run automatically with coverage reporting — *(DoD 107)*
- [ ] **CICD-06**: Container images build automatically and are tagged by git SHA — *(DoD 108)*
- [x] **CICD-07**: Kubernetes manifests and Helm charts are validated in CI — *(DoD 109)*
- [ ] **CICD-08**: Image vulnerability and dependency scanning run in CI — *(DoD 110)*
- [ ] **CICD-09**: An ephemeral kind cluster in CI deploys the stack and runs E2E, proving the environment can be recreated from the repository — *(DoD 113)*

---

## v2 Requirements

Deferred. Not in the current roadmap — none of these appear in README §94.

### Observability

- **V2-OBS-01**: OpenLineage export as an additive lineage feed, with a custom run facet carrying `run_id`, `file_sha256` and `config_version_id` so external graphs join to the `meta` schema. Cannot be the system of record — an HTTP emitter cannot enlist in a PostgreSQL transaction.

### CSV

- **V2-CSV-01**: Byte-offset resume within a single file. Superseded for v1 by `last_committed_chunk_ordinal` on the batch ledger, which delivers §38 as a byproduct. Build only if a fixture demands it.
- **V2-CSV-02**: Multi-row and hierarchical header flattening. v1 detects and rejects these with a clear diagnostic, since no canonical flattening exists.

### Deduplication

- **V2-DEDUP-01**: The remaining four dedup strategies (key+timestamp, latest-wins, source-priority, batch-aware). v1 ships the strategy interface plus exact-row and business-key; the rest are implementations of a solved interface, not new architecture.

### Validation

- **V2-VALID-01**: The `defer` outcome for referential integrity failures. v1 ships `fail`, `quarantine` and `warn`.

---

## Out of Scope

Explicitly excluded, with reasoning, to prevent scope creep.

| Feature | Reason |
|---------|--------|
| Cloud deployment (AWS/GCP/Azure) | Local kind is the target. Swap-out seams (MinIO→S3, Vault→cloud KMS) are preserved, but porting is not this milestone |
| dbt as an end-to-end (bronze→gold) transformation framework | As of Phase 08.1 (ADR-0010), dbt owns bronze-to-silver transformation narrowly (DEDUP-01..04, INCR-03, INCR-04, QUAL-10 — see Traceability table's Phase 08.1 rows for those IDs); gold publish and SCD2 remain explicitly Python-owned, per PG BUG #18279 and META-03's single-transaction guarantee |
| Streaming ingestion (Kafka, Kinesis, Debezium runtime) | No broker is deployed. This is also why exactly-once cannot be claimed |
| CDC (Change Data Capture), including a CSV-delivered change feed — event model, ordering barrier, delivery-semantics documentation, and its test coverage | *(DoD 44, 45, 46, 87)*. No upstream system produces a change feed yet, and SCD 0/1/2 (Phase 10) build entirely from CSV batches without one — dropped 2026-08-21 rather than built for a format with no real producer. The `Source`/`Publisher` seam (Phase 3 ADR) still admits a CDC `Source` later without redesign |
| Real production datasets or PII | Corpus is synthetic by construction, so fixtures are committable, reproducible and safe to scan |
| Docker Compose as a workload platform | Forbidden by README §3.1. May appear only as a developer convenience for isolated unit-test dependencies |
| ML-based anomaly detection | README §53 mandates simple configurable statistical thresholds first |
| Multi-tenancy or a bespoke UI | Airflow's own UI is the only interface |
| Supporting every theoretical CSV variant | README §7 sets the bar at a robust extensible framework for a broad range of *real-world* inputs |
| Prometheus Pushgateway | Short-lived task pods produce permanent staleness and cardinality explosion. Business metrics live in the metadata DB; runtime metrics push via OTLP |
| Typed staging tables | Staging is all-TEXT so structural and type validation happen in the processor, where errors can be attributed to a row |

---

## Traceability

Each v1 requirement maps to exactly one phase in `.planning/ROADMAP.md`.

**Phase legend:**

- **Phase 1** — Repository, Toolchain & CI Skeleton
- **Phase 2** — kind Cluster & Core Infrastructure
- **Phase 3** — `dataplat` Core Library & Metadata Control Plane
- **Phase 4** — Vertical Slice — CSV to Analytical PostgreSQL
- **Phase 5** — Vault Secrets & Workload Identity
- **Phase 6** — Universal CSV Engine, Schema Contracts & Normalization
- **Phase 7** — Observability, Metrics, Tracing & Lineage
- **Phase 8** — Validation, Quarantine & Metadata Control-Plane Completion
- **Phase 9** — ETL Correctness — Dedup, Incremental, Backfill & Recovery
- **Phase 10** — Slowly Changing Dimensions
- **Phase 11** — CI/CD Completion & Operations

| Requirement | Phase | Status |
|-------------|-------|--------|
| META-01 | Phase 3 | Complete |
| META-02 | Phase 3 | Complete |
| META-03 | Phase 4 | Complete |
| INFRA-01 | Phase 2 | Complete |
| INFRA-02 | Phase 2 | Complete |
| INFRA-03 | Phase 2 | Complete |
| INFRA-04 | Phase 2 | Complete |
| INFRA-05 | Phase 2 | Complete |
| INFRA-06 | Phase 5 | Complete |
| INFRA-07 | Phase 2 | Complete |
| INFRA-08 | Phase 3 | Complete |
| INFRA-09 | Phase 2 | Complete |
| INFRA-10 | Phase 2 | Complete |
| INFRA-11 | Phase 11 | Pending |
| SEC-01 | Phase 5 | Complete |
| SEC-02 | Phase 1 | Complete |
| SEC-03 | Phase 5 | Complete |
| SEC-04 | Phase 5 | Complete |
| SEC-05 | Phase 5 | Complete |
| SEC-06 | Phase 5 | Complete |
| SEC-07 | Phase 5 | Complete |
| SEC-08 | Phase 5 | Complete |
| SEC-09 | Phase 5 | Complete |
| SEC-10 | Phase 1 | Complete |
| SEC-11 | Phase 1 | Complete |
| SEC-12 | Phase 5 | Complete |
| SEC-13 | Phase 5 | Complete |
| SEC-14 | Phase 5 | Complete |
| SEC-15 | Phase 3 | Complete |
| ORCH-01 | Phase 4 | Complete |
| ORCH-02 | Phase 4 | Complete |
| ORCH-03 | Phase 4 | Complete |
| ORCH-04 | Phase 4 | Complete |
| ORCH-05 | Phase 4 | Complete |
| ORCH-06 | Phase 4 | Complete |
| ORCH-07 | Phase 4 | Complete |
| ORCH-08 | Phase 4 | Complete |
| ORCH-09 | Phase 4 | Complete |
| CSV-01 | Phase 6 | Complete |
| CSV-02 | Phase 6 | Complete |
| CSV-03 | Phase 6 | Complete |
| CSV-04 | Phase 6 | Complete |
| CSV-05 | Phase 6 | Complete |
| CSV-06 | Phase 6 | Complete |
| CSV-07 | Phase 6 | Complete |
| CSV-08 | Phase 6 | Complete |
| CSV-09 | Phase 6 | Complete |
| CSV-10 | Phase 6 | Complete |
| CSV-11 | Phase 6 | Complete |
| CSV-12 | Phase 6 | Complete |
| CSV-13 | Phase 3 | Complete |
| SCHEMA-01 | Phase 6 | Complete |
| SCHEMA-02 | Phase 6 | Pending |
| SCHEMA-03 | Phase 6 | Complete |
| SCHEMA-04 | Phase 6 | Complete |
| SCHEMA-05 | Phase 6 | Complete |
| SCHEMA-06 | Phase 6 | Complete |
| SCHEMA-07 | Phase 3 | Complete |
| VALID-01 | Phase 8 | Complete |
| VALID-02 | Phase 8 | Complete |
| VALID-03 | Phase 8 | Complete |
| VALID-04 | Phase 8 | Complete |
| VALID-05 | Phase 9 | Complete |
| VALID-06 | Phase 9 | Pending |
| VALID-07 | Phase 8 | Complete |
| VALID-08 | Phase 8 | Complete |
| VALID-09 | Phase 8 | Complete |
| LOAD-01 | Phase 4 | Complete |
| LOAD-02 | Phase 4 | Complete |
| LOAD-03 | Phase 4 | Complete |
| LOAD-04 | Phase 4 | Complete |
| LOAD-05 | Phase 4 | Complete |
| LOAD-06 | Phase 9 | Pending |
| LOAD-07 | Phase 6 | Complete |
| LOAD-08 | Phase 4 | Complete |
| LOAD-09 | Phase 4 | Complete |
| LOAD-10 | Phase 8 | Complete |
| LOAD-11 | Phase 8 | Complete |
| LOAD-12 | Phase 4 | Pending |
| DEDUP-01 | Phase 08.1 | Complete |
| DEDUP-02 | Phase 08.1 | Complete |
| DEDUP-03 | Phase 08.1 | Complete |
| DEDUP-04 | Phase 08.1 | Complete |
| INCR-01 | Phase 9 | Pending |
| INCR-02 | Phase 9 | Pending |
| INCR-03 | Phase 08.1 | Complete |
| INCR-04 | Phase 08.1 | Complete |
| INCR-05 | Phase 9 | Pending |
| INCR-06 | Phase 9 | Pending |
| INCR-07 | Phase 11 | Pending |
| INCR-08 | Phase 4 | Pending |
| SCD-01 | Phase 10 | Pending |
| SCD-02 | Phase 10 | Pending |
| SCD-03 | Phase 10 | Pending |
| SCD-04 | Phase 10 | Pending |
| SCD-05 | Phase 10 | Pending |
| SCD-06 | Phase 10 | Pending |
| SCD-07 | Phase 10 | Pending |
| SCD-08 | Phase 10 | Pending |
| SCD-09 | Phase 10 | Pending |
| SCD-10 | Phase 10 | Pending |
| SCD-11 | Phase 10 | Pending |
| SCD-12 | Phase 10 | Pending |
| OBS-01 | Phase 7 | Complete |
| OBS-02 | Phase 3 | Complete |
| OBS-03 | Phase 1 | Complete |
| OBS-04 | Phase 3 | Complete |
| OBS-05 | Phase 3 | Complete |
| OBS-06 | Phase 11 | Pending |
| OBS-07 | Phase 7 | Complete |
| OBS-08 | Phase 7 | Complete |
| OBS-09 | Phase 7 | Complete |
| OBS-10 | Phase 7 | Complete |
| QUAL-01 | Phase 1 | Complete |
| QUAL-02 | Phase 1 | Complete |
| QUAL-03 | Phase 3 | Complete |
| QUAL-04 | Phase 6 | Pending |
| QUAL-05 | Phase 4 | Complete |
| QUAL-06 | Phase 4 | Complete |
| QUAL-07 | Phase 1 | Complete |
| QUAL-08 | Phase 1 | Complete |
| QUAL-09 | Phase 4 | Complete |
| QUAL-10 | Phase 08.1 | Complete |
| QUAL-11 | Phase 9 | Pending |
| QUAL-12 | Phase 6 | Complete |
| QUAL-14 | Phase 10 | Pending |
| QUAL-15 | Phase 11 | Pending |
| QUAL-16 | Phase 6 | Complete |
| QUAL-17 | Phase 6 | Complete |
| CICD-01 | Phase 1 | Complete |
| CICD-02 | Phase 1 | Complete |
| CICD-03 | Phase 1 | Complete |
| CICD-04 | Phase 1 | Complete |
| CICD-05 | Phase 11 | Pending |
| CICD-06 | Phase 11 | Pending |
| CICD-07 | Phase 2 | Complete |
| CICD-08 | Phase 11 | Pending |
| CICD-09 | Phase 11 | Pending |

**Coverage:**

- v1 requirements: 142 total (114 README §94 DoD + 16 specification gaps + 12 research-derived constraints)
- Mapped to phases: 142 ✓
- Unmapped: 0 ✓ — no orphans, no duplicates (validated during roadmap creation)

**Per-category counts:** QUAL 16 · SEC 15 · CSV 13 · SCD 12 · LOAD 12 · INFRA 11 · OBS 10 · VALID 9 · ORCH 9 · CICD 9 · INCR 8 · SCHEMA 7 · DEDUP 4 · META 3

**Per-phase counts:** P1 12 · P2 9 · P3 10 · P4 22 · P5 12 · P6 23 · P7 5 · P8 9 · P9 15 · P10 17 · P11 8 = 142

---
*Requirements defined: 2026-08-11*
*Last updated: 2026-08-11 after roadmap creation — traceability populated*
