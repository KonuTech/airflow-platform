# Architecture Research

**Domain:** Metadata-driven ETL/data platform on local Kubernetes (kind) — Airflow orchestration, containerized CSV ingestion, S3-compatible lake, analytical PostgreSQL warehouse
**Researched:** 2026-08-11
**Confidence:** MEDIUM

> Reference vocabulary: `§N` = section N of `README.md` (the master specification).

---

## Executive Position

Four architectural claims drive everything below. If the roadmap takes nothing else from this document, take these.

1. **The metadata control plane is the product, not a side-effect.** PROJECT.md's Core Value is *"every file, batch and record can be traced, explained, reprocessed and trusted."* That is a statement about `meta.*` tables, not about CSV parsing. §92 sequences metadata (idempotency, lineage, watermarks) into Phase 8. That is backwards: the identity model is load-bearing for every table created before it, and retrofitting `UNIQUE(idempotency_key)` after six phases of accumulated schema is a migration project. **A minimal metadata schema belongs in the vertical slice.**

2. **The seam that makes §29/§95 ("add sources without redesign") true is `Source → RecordChunk → Publisher`.** Not "a CSV pipeline and later a CDC pipeline." CDC is a `Source`; SCD is a `Publisher`. Everything between them (validate, normalize, dedupe) is shared and source-agnostic. §68's proposed package layout does not contain this seam and will not deliver §95 as written.

3. **Row-level data problems are data, not exceptions.** The §71 exception hierarchy is for *run-fatal* conditions. A bad date on row 41,203 is a `ValidationResult` + a `RejectedRecord`, flowing through the pipeline as a value. Conflating the two is the single most common way ETL frameworks end up silently discarding records (§27, §51).

4. **The publication transaction is the atomicity boundary for data *and* metadata.** Data rows, watermark advance, and run-status update commit together or not at all. This is what makes §24 (idempotency), §28 (watermarks advance only after commit) and §37 (recovery without reading logs) a single mechanism rather than three. It is also the decisive reason OpenLineage cannot be the system of record — an HTTP event emitter cannot enlist in a PostgreSQL transaction.

---

## Standard Architecture

### System Overview

```
┌───────────────────────────────────────────────────────────────────────────┐
│                          CONTROL PLANE (kind cluster)                      │
├───────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────┐   ┌──────────────┐  ┌────────────┐  │
│  │           Airflow 3.x            │   │    Vault     │  │ Prometheus │  │
│  │  api-server │ scheduler          │   │  k8s auth    │  │  Grafana   │  │
│  │  dag-proc   │ triggerer          │   │  kv-v2       │  │  OTel Col. │  │
│  │  (KubernetesExecutor)            │   └──────┬───────┘  └─────▲──────┘  │
│  └───────┬──────────────────┬───────┘          │ identity       │ scrape  │
│          │ metadata         │ creates pods     │ + secrets      │ + OTLP  │
│          ▼                  │                  │                │         │
│  ┌───────────────┐          │                  │                │         │
│  │ Airflow       │          │                  │                │         │
│  │ PostgreSQL    │          │                  │                │         │
│  │ (§4.1 ONLY)   │          │                  │                │         │
│  └───────────────┘          │                  │                │         │
├─────────────────────────────┼──────────────────┼────────────────┼─────────┤
│                        EXECUTION PLANE          │                │         │
│                             ▼                  ▼                │         │
│      ┌──────────────────────────────────────────────────────────┴──────┐  │
│      │  ETL Task Pod  (sa: csv-processor, ns: data-etl)                │  │
│      │  ┌────────────────┐ ┌─────────────────┐ ┌──────────────────┐   │  │
│      │  │ vault-agent    │ │  dataplat CLI   │ │ airflow-xcom-    │   │  │
│      │  │ (init+sidecar) │ │  + csv_processor│ │ sidecar          │   │  │
│      │  │ →/vault/secrets│ │                 │ │ →return.json     │   │  │
│      │  └────────────────┘ └────┬───────┬────┘ └──────────────────┘   │  │
│      └──────────────────────────┼───────┼──────────────────────────────┘  │
├─────────────────────────────────┼───────┼─────────────────────────────────┤
│                          DATA PLANE      │                                 │
│                                 │        │                                 │
│   ┌─────────────────────────────▼──┐  ┌──▼──────────────────────────────┐ │
│   │  MinIO  (S3 API)               │  │  Analytical PostgreSQL (§4.2)   │ │
│   │  raw/        ← IMMUTABLE (§63) │  │  ┌───────────────────────────┐  │ │
│   │  validated/                    │  │  │ meta       control plane  │  │ │
│   │  processed/                    │  │  │ staging    per-run, TEXT  │  │ │
│   │  quarantine/                   │  │  │ quarantine rejected rows  │  │ │
│   │  metadata/   assignments,      │  │  │ normalized typed, deduped │  │ │
│   │              reports, configs  │  │  │ warehouse  SCD dims/facts │  │ │
│   │              xcom-overflow     │  │  │ analytics  views/marts    │  │ │
│   └────────────────────────────────┘  │  └───────────────────────────┘  │ │
│                                        └─────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Owns | Explicitly does NOT own | Interface to neighbours |
|-----------|------|--------------------------|--------------------------|
| **Airflow scheduler / DAG processor** | When work runs, dependency order, retry policy, backfill windows, fan-out degree | Any parsing, validation, typing, loading, or DB writes to analytical PG | Kubernetes API (creates pods); Airflow metadata PG |
| **DAG (`@dag`/`@task`)** | Discovery call, config-version pinning, assignment authoring, fan-out, receipt aggregation | Business logic (§6.4). Total DAG file target: **< 150 lines** | Calls `dataplat` library functions; emits assignment URIs via XCom |
| **ETL task pod** | One work assignment, end to end, in bounded memory | Knowing about other pods, other files, or Airflow internals beyond the identifiers handed to it | Reads assignment JSON from MinIO; writes run rows to `meta`; returns a ≤4 KB receipt |
| **`dataplat` core package** | Pipeline engine, config, metadata repo, validation, normalization, dedup, publication, observability, secrets resolution | CSV specifics; anything source-format-aware | Python protocols (`Source`, `Stage`, `Publisher`, `MetadataRepository`) |
| **`csv_processor` package** | Filename parsing, encoding/dialect/header detection, streaming CSV reads | Validation rules, typing policy, dedup, loading | Implements `dataplat.sources.Source`; registered via entry point |
| **MinIO** | Immutable raw objects, quarantine artifacts, validation reports, work assignments, XCom overflow | Any authoritative processing state | S3 API only (`s3://bucket/key`), never a filesystem path (§5) |
| **Analytical PostgreSQL** | Control plane (`meta`), staged/normalized/warehouse data, all constraints that enforce idempotency | Airflow's own metadata (§4.1 separation is absolute) | SQL over pinned roles; DDL via Alembic |
| **Vault** | Workload identity, credential issuance, access audit | Application config (that is a ConfigMap's job) | Kubernetes auth; Agent-rendered files on tmpfs |
| **Prometheus / Grafana / OTel** | Metric storage, dashboards, distributed traces | Data correctness state — that lives in `meta` and is queried by SQL | `/metrics` scrape + OTLP to collector |

---

## Question 1 — The Vertical Slice: Minimum Component Set

### The slice boundary, stated as a hard scope fence

> **Vertical slice = exactly these components.** A UTF-8, comma-delimited, header-in-row-1 CSV with a stable known schema lands in `s3://raw/customers/YYYY/MM/DD/`. A TaskFlow DAG discovers it, writes one assignment document, launches one `KubernetesPodOperator` pod, and that pod loads the rows into `normalized.customers` in the analytical database with full run metadata. Re-running the DAG loads zero additional rows.
>
> **In scope:** kind cluster, local registry, MinIO, analytical PostgreSQL, Airflow PostgreSQL, Airflow (KubernetesExecutor), processor image, 5 metadata tables, 1 target table, 1 DAG, 1 CLI entrypoint, 1 E2E test.
>
> **Out of scope — and this is the point:** Vault, encoding detection, dialect detection, header detection, schema inference, schema evolution, quality rules, quarantine, dedup strategies beyond the exact-hash primary key, CDC, SCD, checkpointing, reconciliation, Prometheus, Grafana, OpenTelemetry export, Dynamic Task Mapping beyond degree 1.

### What must exist, in dependency order

| # | Component | Why the slice genuinely cannot work without it | Can be built in parallel with |
|---|-----------|-----------------------------------------------|-------------------------------|
| 1 | kind cluster (control-plane + 2 workers) + **local container registry wired into the cluster** | Pods must pull the processor image. `kind load docker-image` re-uploads the whole image on every code change; a registry container makes the inner loop tolerable | 3, 4, 5, 6, 7 |
| 2 | Namespaces (`airflow`, `data`, `data-etl`), RBAC, service accounts | The pod needs an identity; the Airflow scheduler needs RBAC to create pods in `data-etl` | 3–7 |
| 3 | MinIO + bucket bootstrap Job (`raw`,`validated`,`processed`,`quarantine`,`metadata`) | The source of the data and the home of the assignment document | 1, 2, 4, 5, 6, 7 |
| 4 | Analytical PostgreSQL + roles + `meta`/`staging`/`normalized` schemas | The destination and the control plane | 1, 2, 3, 5, 6, 7 |
| 5 | Alembic migrations: `datasets`, `config_versions`, `files`, `batches`, `ingestion_runs` + `normalized.customers` | Without these the run is untraceable and non-idempotent — i.e. not this project's slice | 1, 2, 3, 6, 7 |
| 6 | `dataplat` core: models, errors, logging, config loader+hasher, object store, db, pipeline engine, metadata repo, `SecretsResolver` | The processing substrate. **Testable with docker-only fixtures — no cluster required** | 1, 2, 3, 4, 7 |
| 7 | `csv_processor` naive reader + `dataplat` CLI + Dockerfile (non-root) | The pod entrypoint | 1, 2, 3, 4 (needs 6's protocols first) |
| 8 | Airflow PostgreSQL (Helm) | Airflow's own store | 1–7 |
| 9 | Airflow (Helm, KubernetesExecutor, DAGs via image or git-sync from ext4) | The orchestrator | needs 8 |
| 10 | Smoke DAG: one `KubernetesPodOperator` running `dataplat --version`, `do_xcom_push=True` | **Proves the riskiest integration in isolation**: image pull, SA, resource limits, XCom sidecar, log streaming | needs 1,2,7,9 |
| 11 | `csv_ingest` DAG: `resolve_config` → `discover_files` → write assignments → `expand()` → aggregate receipts | The slice itself | needs 3,5,6,7,10 |
| 12 | E2E test: upload → trigger → assert rows + `SUCCEEDED` run row → **re-trigger → assert row count unchanged** | The slice is not done until re-running is proven safe | needs 11 |

### The two ordering insights that matter most

**Insight A — items 1–4 (infrastructure) and 5–7 (the Python library) are fully parallel.** The library is developed and unit/integration-tested against `testcontainers` (or plain Docker) MinIO + PostgreSQL. It never needs a Kubernetes cluster to be correct. Roughly half the slice's work can proceed on two independent tracks. Roadmap phases should reflect this rather than serializing infra → library.

**Insight B — item 10 is a separate, deliberately trivial step.** The highest-risk unknown in this project is not CSV parsing; it is *whether an Airflow 3 `KubernetesPodOperator` in a kind cluster can pull a locally-built image, run as a non-root SA, and return an XCom*. Debugging that while simultaneously debugging a CSV pipeline is how a week disappears. Prove it with `--version` and nothing else.

### Deviation from §92, stated up front

§92 orders: `1 kind → 2 infra → 3 airflow+k8s → 4 secrets → 5 basic CSV pipeline`. Vault sits **before** the slice closes.

**Recommendation: move Vault to immediately after the slice.** Rationale:
- The slice needs *credentials*, not a *secrets manager*. Kubernetes Secrets satisfy the slice.
- Vault Agent injection introduces a mutating webhook, a Kubernetes auth mount, TokenReview permissions and policy debugging — all onto the critical path of "does anything work end to end at all?"
- The retrofit cost is bounded **if and only if** the `SecretsResolver` seam is built in step 6. The processor resolves an opaque reference (`env://ANALYTICAL_DB_DSN` or `file:///vault/secrets/analytical-db`) and never learns which. Swapping is then a ConfigMap change plus pod annotations — no application code changes, which is precisely what §81 demands ("the local implementation can be replaced ... without changing application code").
- Cost of the deviation: the slice temporarily has a DSN in a Kubernetes Secret. Mitigation: it is a development-only credential (§81.10), the follow-on phase removes it, and secret scanning (Stage 0 CI) prevents it reaching Git.

---

## Question 2 — The Metadata Model

All tables live in schema `meta` of the **analytical** PostgreSQL. Types are PostgreSQL. `id` columns are `bigint GENERATED ALWAYS AS IDENTITY` unless stated.

### 2.1 Slice tables (build first — 5 tables)

#### `meta.datasets` — the registry
| Column | Type | Notes |
|---|---|---|
| `dataset_id` | bigint PK | surrogate |
| `dataset_name` | text NOT NULL UNIQUE | `customers`, `transactions` |
| `source_system` | text | `SAP`, `crm-export` |
| `description` | text | |
| `is_active` | boolean NOT NULL DEFAULT true | |
| `created_at` / `updated_at` | timestamptz NOT NULL DEFAULT now() | |

#### `meta.config_versions` — §66, §62
| Column | Type | Notes |
|---|---|---|
| `config_version_id` | bigint PK | |
| `dataset_id` | bigint NOT NULL REFERENCES datasets | |
| `version` | int NOT NULL | monotonic per dataset |
| `config_hash` | text NOT NULL | sha256 of **canonicalized resolved** config |
| `config_document` | jsonb NOT NULL | the full resolved config — this is what replay reads |
| `config_schema_version` | int NOT NULL | version of the *config format*, for loader migration |
| `git_commit_sha` / `git_path` | text | provenance back to the repo |
| `valid_from` | timestamptz NOT NULL | |
| `valid_to` | timestamptz NULL | NULL = current |
| | | `UNIQUE(dataset_id, version)`, `UNIQUE(dataset_id, config_hash)`, partial `UNIQUE(dataset_id) WHERE valid_to IS NULL` |

#### `meta.files` — §21, §25, §40, §42
| Column | Type | Notes |
|---|---|---|
| `file_id` | bigint PK | |
| `dataset_id` | bigint NOT NULL REFERENCES datasets | |
| `object_uri` | text NOT NULL | `s3://raw/customers/2026/08/11/customers_20260811.csv` |
| `object_etag` | text | cheap change detection |
| `content_sha256` | bytea NOT NULL | **the real file identity** (§24: "do not rely solely on filenames") |
| `size_bytes` | bigint NOT NULL | |
| `filename` | text NOT NULL | |
| `filename_facets` | jsonb | §8 parse output: source, country, business_date, version, batch, seq, full/incremental |
| `business_date` | date NULL | §8: *never* auto-assumed from filename; set only when config says so |
| `discovered_at` / `source_last_modified_at` | timestamptz | |
| `duplicate_of_file_id` | bigint NULL REFERENCES files | set when `content_sha256` already seen for this dataset |
| `status` | text NOT NULL | `DISCOVERED\|CLAIMED\|PROCESSED\|QUARANTINED\|FAILED\|SUPERSEDED` |
| | | `UNIQUE(dataset_id, object_uri, content_sha256)`; index on `(dataset_id, content_sha256)` |

> **Design note (§25).** Identity is deliberately split: `object_uri` identifies *an arrival*, `content_sha256` identifies *content*. The same bytes re-uploaded to a new path is a **new arrival of a known file** — a distinct situation from a genuinely new file and from an intentional backfill. A single natural key on either column alone conflates them, which §25 forbids.

#### `meta.batches` — §25, §41, §43, §46
| Column | Type | Notes |
|---|---|---|
| `batch_id` | bigint PK | |
| `dataset_id` | bigint NOT NULL | |
| `batch_key` | text NOT NULL | from manifest, filename facets, or derived `<dataset>:<business_date>:<seq>` |
| `business_date` | date | |
| `manifest_uri` | text NULL | §41 |
| `expected_file_count` | int NULL | §41/§44 |
| `expected_row_count` | bigint NULL | §21/§46 |
| `control_totals` | jsonb NULL | §46, e.g. `{"amount": 12345678.90}` |
| `completion_marker_uri` | text NULL | §43 `_BATCH_COMPLETE` |
| `status` | text NOT NULL | `OPEN\|COMPLETE\|PROCESSING\|PUBLISHED\|FAILED\|QUARANTINED` |
| | | `UNIQUE(dataset_id, batch_key)` |

Plus join table `meta.batch_files(batch_id, file_id, sequence_no)` with PK `(batch_id, file_id)`.

> Include `batches` in the slice **even though the slice is one-file-one-batch**. It is one table now; adding a `NOT NULL` FK to populated tables across three later phases is not.

#### `meta.ingestion_runs` — §24, §37, §62, §82, §83 — *the central table*
| Column | Type | Notes |
|---|---|---|
| `run_id` | bigint PK | |
| `idempotency_key` | text NOT NULL **UNIQUE** | see Q7 — the constraint that makes retries free |
| `dataset_id`, `file_id`, `batch_id` | bigint FKs | |
| `config_version_id`, `schema_version_id` | bigint FKs | §62/§66 — exactly what was used |
| `processor_version` | text NOT NULL | package version |
| `processor_image_digest` | text NOT NULL | `sha256:…` — §67 determinism anchor; tags lie, digests do not |
| `dag_id`, `dag_run_id`, `task_id` | text | §83 |
| `map_index` | int | dynamic task mapping position |
| `try_number` | int | **deliberately excluded from `idempotency_key`** |
| `logical_date` | timestamptz NULL | NULL for asset/manual triggers — see Q6 warning |
| `data_interval_start` / `_end` | timestamptz NULL | |
| `k8s_namespace`, `k8s_pod_name`, `k8s_node_name` | text | §82 |
| `trace_id`, `span_id` | text | OTel correlation |
| `status` | text NOT NULL | `PENDING\|RUNNING\|SUCCEEDED\|FAILED\|QUARANTINED\|SKIPPED_DUPLICATE` |
| `lease_expires_at` | timestamptz NULL | heartbeated; enables crashed-pod takeover (§37) |
| `started_at`, `finished_at`, `duration_ms` | | |
| `rows_read`, `rows_parsed`, `rows_valid`, `rows_invalid`, `rows_deduplicated`, `rows_loaded` | bigint | §27, §82 |
| `error_type`, `error_message` | text | §71 class name + message |
| `error_detail` | jsonb | structured context, redacted |
| `report_uri` | text | full JSON report in `s3://metadata/reports/…` |
| `replay_of_run_id` | bigint NULL REFERENCES ingestion_runs | §62 |

### 2.2 Post-slice tables

| Table | Purpose (§) | Key columns |
|---|---|---|
| `meta.schema_versions` | §12, §13, §52 | `schema_version_id`, `dataset_id`, `version`, `schema_hash`, `columns jsonb` (ordered: name/type/nullable/position/format), `derived_from ∈ {CONTRACT, INFERRED}`, `compatibility ∈ {COMPATIBLE, BREAKING}`, `breaking_changes jsonb`, `valid_from/valid_to`. `UNIQUE(dataset_id, version)` |
| `meta.run_stages` | §37, §38 | `run_id`, `stage_name ∈ {DISCOVER,INSPECT,PARSE,VALIDATE,NORMALIZE,DEDUP,STAGE_LOAD,PUBLISH,RECONCILE}`, `status`, `started_at/finished_at`, `checkpoint jsonb` (`{"byte_offset":…, "rows_committed":…, "chunk_seq":…}`), `UNIQUE(run_id, stage_name)` |
| `meta.watermarks` | §28 | `dataset_id`, `target_key` (default `'default'`, or a partition key), `strategy ∈ {EVENT_TIMESTAMP,MONOTONIC_ID,BATCH_ID,FILE_SEQUENCE}`, `value_ts`, `value_num`, `value_text`, `advanced_by_run_id`, `updated_at`, `UNIQUE(dataset_id, target_key)` |
| `meta.watermark_history` | §28 audit | append-only: `dataset_id`, `target_key`, `old_value`, `new_value`, `run_id`, `changed_at` |
| `meta.validation_results` | §19, §20, §23, §50 | `run_id`, `rule_id`, `rule_type ∈ {FILE,STRUCTURAL,SCHEMA,TYPE,QUALITY,REFERENTIAL}`, `severity`, `outcome ∈ {PASS,PASS_WITH_WARNING,FAIL,QUARANTINE}`, `evaluated_count`, `failed_count`, `threshold jsonb`, `observed jsonb` |
| `meta.rejected_records` | §51 | `run_id`, `file_id`, `source_row_number`, `source_byte_offset`, `raw_line text`, `error_type`, `error_column`, `error_message`, `rejected_at`. Overflow beyond `config.quarantine.max_inline_rows` spills to `s3://quarantine/…` with a pointer |
| `meta.dedup_audit` | §27 | `run_id`, `strategy`, `keys jsonb`, `records_received`, `records_accepted`, `records_rejected`, `records_deduplicated` |
| `meta.dedup_decisions` | §27 detail | `run_id`, `record_hash bytea`, `business_key jsonb`, `kept_file_id`, `kept_source_row`, `dropped_source_row`, `reason ∈ {EXACT_DUP_IN_FILE, EXACT_DUP_CROSS_BATCH, SUPERSEDED_BY_NEWER, LOWER_SOURCE_PRIORITY, SCD_NO_CHANGE}`. Retention-controlled; sampled or full per config |
| `meta.quality_metrics` | §49, §53, §82 | `dataset_id`, `run_id`, `business_date`, `metric_name` (`row_count`, `null_rate.customer_id`, `sum.amount`), `metric_value numeric`, `computed_at`. This *is* the anomaly-detection baseline — §53's "normal ~1,000,000 rows/day" is a query over history, not a configured constant |
| `meta.reconciliation_results` | §45, §46 | `run_id`, `batch_id`, `check_name`, `check_type ∈ {ROW_COUNT,SUM,MIN,MAX,KEY_COUNT,CHECKSUM,CONTROL_TOTAL}`, `source_value`, `target_value`, `delta`, `tolerance`, `outcome ∈ {MATCH,WITHIN_TOLERANCE,MISMATCH}` |
| `meta.dataset_sla` | §44, §49 | `dataset_id`, `expected_frequency interval`, `grace_period interval`, `last_received_at`, `last_success_at`, `on_missing ∈ {WARN,FAIL}` |
| `meta.retention_policies` | §64, §91 | `scope ∈ {RAW,VALIDATED,PROCESSED,QUARANTINE,REPORTS,RUNS,REJECTED_RECORDS,DEDUP_DECISIONS}`, `dataset_id NULL` (NULL = global default), `retain_for interval`, `action ∈ {DELETE,ARCHIVE,COMPACT}` |
| `meta.cdc_offsets` | §29, §30 | `source_id`, `source_table`, `last_txid`, `last_lsn`, `last_sequence`, `last_source_ts`, `advanced_by_run_id` |
| `meta.record_lineage` | §83 — **opt-in only** | see below |

### 2.3 Record-level lineage — the one place to resist a table

§83 demands per-record traceability. The naive reading is a `record_lineage` row per ingested record: at 1 M rows/day that is 365 M rows/year of pure overhead.

**Recommendation: embed lineage as columns on target tables.**

```sql
-- every table in normalized.* and warehouse.* carries:
_run_id            bigint      NOT NULL REFERENCES meta.ingestion_runs,
_file_id           bigint      NOT NULL REFERENCES meta.files,
_batch_id          bigint      NOT NULL REFERENCES meta.batches,
_source_row_number bigint      NOT NULL,
_record_hash       bytea       NOT NULL,
_ingested_at       timestamptz NOT NULL DEFAULT now()
```

One join to `meta.ingestion_runs` then yields object path, checksum, DAG/run/task, pod, processor version, schema version and config version — every item §83 lists — at zero extra storage beyond 40 bytes/row.

Keep `meta.record_lineage(target_table, target_row_key jsonb, run_id, file_id, source_row_number, record_hash)` as an **opt-in** table, enabled per dataset only where target row ≠ source row (aggregations, many-to-one merges, SCD collapses) and the embedded columns therefore cannot express the relationship.

### 2.4 Slice vs. later — summary

| Phase | Tables |
|---|---|
| **Vertical slice** | `datasets`, `config_versions`, `files`, `batches`, `ingestion_runs` + `batch_files` + lineage columns on the target |
| **Validation phase** | `schema_versions`, `validation_results`, `rejected_records` |
| **Correctness phase** | `run_stages`, `watermarks`, `watermark_history`, `dedup_audit`, `dedup_decisions`, `reconciliation_results` |
| **Observability phase** | `quality_metrics`, `dataset_sla` |
| **CDC/SCD phase** | `cdc_offsets`, `scd_change_log`, warehouse dimension tables |
| **Operations phase** | `retention_policies`, `record_lineage` (opt-in) |

---

## Question 3 — Schema Layout and Atomic Publication

### 3.1 Schema boundaries

| Schema | Contents | Durability | Write access | What moves in | What moves out |
|---|---|---|---|---|---|
| `meta` | Control plane (§2 above) | Permanent, never truncated | `etl_writer` (INS/UPD only; no DELETE except retention role) | Every stage writes here | Nothing — it is queried, never drained |
| `staging` | `staging.<dataset>__r<run_id>`: **all columns `text`**, plus lineage columns. `UNLOGGED` | Ephemeral, dropped after publication+retention window | `etl_writer` (full DDL within this schema) | `COPY` from the parsed stream | Typed `MERGE` into `normalized` |
| `quarantine` | Rejected rows and quarantined batches, loosely typed, with error metadata | Retention-governed | `etl_writer` | Validation failures | Manual reprocessing only |
| `normalized` | Typed, conformed, deduplicated entity tables. Business keys enforced. Partitioned by `business_date` where volume warrants | Permanent (rebuildable from raw — §90) | `etl_writer` | `MERGE` from `staging` | Read by warehouse DAGs |
| `warehouse` | Dimensional model: SCD0/1/2 dimensions with surrogate keys, fact tables | Permanent | `warehouse_writer` (distinct role) | SCD/fact publishers reading `normalized` | Read by `analytics` |
| `analytics` | Views, materialized views, marts. **No base tables** | Fully derived, always rebuildable | `warehouse_writer` | `CREATE VIEW`/`REFRESH` | `bi_reader` (SELECT only) |

**Why staging is all-TEXT — an opinionated call.** If the staging table is typed, a single unparseable date aborts the whole `COPY` and you learn only "invalid input syntax for type date". With TEXT staging the load always succeeds, and typing becomes a *set-based SQL validation pass* that reports the offending row number and column for **every** bad value at once (§19 demands row + column + error type). It also means §13 schema evolution never requires altering the staging table. The cost is one extra pass; the benefit is that §51 ("never silently discard") is structurally achievable.

**Why `UNLOGGED` staging.** No WAL, materially faster bulk load. The tradeoff — contents are lost on crash — is exactly right here: raw is immutable (§63) and the run is replayable (§62), so the recovery action is "re-stage", not "recover staging".

### 3.2 Atomic publication, concretely (§35, §36)

Three publication strategies, selected by `load.strategy` in the dataset config.

**Strategy A — `merge` (default; incremental and idempotent)**

```sql
-- Phase 1: OUTSIDE the publication transaction. Chunked, checkpointed, restartable.
--   COPY staging.customers__r8123 FROM STDIN ...   (repeated per chunk)
--   set-based type/quality validation over staging, writing meta.validation_results
--   rows failing validation are moved to quarantine.customers and deleted from staging

-- Phase 2: the publication transaction. Everything or nothing.
BEGIN;

SELECT pg_advisory_xact_lock(hashtext('normalized.customers:2026-08-11'));  -- §87

MERGE INTO normalized.customers AS t
USING (
    SELECT DISTINCT ON (customer_id)
           customer_id::int, name, country, birth_date::date, event_ts::timestamptz,
           _run_id, _file_id, _batch_id, _source_row_number, _record_hash
    FROM   staging.customers__r8123
    ORDER  BY customer_id, event_ts DESC, _source_row_number DESC   -- §26 latest-wins
) AS s
ON  t.customer_id = s.customer_id
WHEN MATCHED AND t._record_hash <> s._record_hash
                AND s.event_ts >= t.event_ts            -- §32 late data must not clobber newer
     THEN UPDATE SET name = s.name, country = s.country, birth_date = s.birth_date,
                     event_ts = s.event_ts, _record_hash = s._record_hash,
                     _run_id = s._run_id, _file_id = s._file_id,
                     _batch_id = s._batch_id, _source_row_number = s._source_row_number
WHEN NOT MATCHED
     THEN INSERT (...) VALUES (...);

UPDATE meta.watermarks
   SET value_ts = :max_event_ts, advanced_by_run_id = :run_id, updated_at = now()
 WHERE dataset_id = :dataset_id AND target_key = 'default'
   AND (value_ts IS NULL OR value_ts < :max_event_ts);          -- §28 monotonic only

INSERT INTO meta.watermark_history (...) VALUES (...);

UPDATE meta.ingestion_runs
   SET status='SUCCEEDED', finished_at=now(), rows_loaded=:n, report_uri=:uri
 WHERE run_id = :run_id;

COMMIT;
```

Everything §24/§28/§36/§37 needs is in that one transaction: rows, watermark, run status. A crash at any point rolls back all three, so the run row still reads `RUNNING` with an expired lease — unambiguous, and diagnosable by SQL rather than by log archaeology.

`WHEN MATCHED AND t._record_hash <> s._record_hash` suppresses no-op writes, which keeps `_ingested_at` meaningful and avoids gratuitous bloat when the same file is legitimately reprocessed.

**Strategy B — `partition_replace` (full refresh of one business date)**

Target is `PARTITION BY RANGE (business_date)`. Build the replacement as a standalone table carrying a `CHECK` constraint that already proves the partition bound — PostgreSQL then skips the validation scan on `ATTACH`:

```sql
CREATE UNLOGGED TABLE staging.customers__p20260811 (LIKE normalized.customers INCLUDING ALL);
ALTER TABLE staging.customers__p20260811
  ADD CONSTRAINT ck_bound CHECK (business_date >= DATE '2026-08-11'
                             AND business_date <  DATE '2026-08-12');
-- ... load and validate ...
BEGIN;
  ALTER TABLE normalized.customers DETACH PARTITION normalized.customers_p20260811;
  ALTER TABLE normalized.customers
        ATTACH PARTITION staging.customers__p20260811
        FOR VALUES FROM ('2026-08-11') TO ('2026-08-12');
  UPDATE meta.watermarks ...;
  UPDATE meta.ingestion_runs ...;
COMMIT;
DROP TABLE normalized.customers_p20260811;   -- after the transaction settles
```

Note: `DETACH PARTITION CONCURRENTLY` **cannot run inside a transaction block** and is disallowed when a `DEFAULT` partition exists. Use plain `DETACH` inside the transaction — it takes a brief `ACCESS EXCLUSIVE` lock, which is entirely acceptable at this scale and is what buys the atomicity.

**Strategy C — `full_swap` (small reference datasets only)**

```sql
BEGIN;
  ALTER TABLE normalized.countries      RENAME TO countries__old;
  ALTER TABLE staging.countries__r8123  SET SCHEMA normalized;
  ALTER TABLE normalized.countries__r8123 RENAME TO countries;
COMMIT;
```
Safe (DDL is transactional) but **foreign keys, views and composite types keep pointing at the renamed old table**. Restrict this to leaf tables with no dependents, and drop `countries__old` only after in-flight transactions drain.

**Guidance:** default to A. Use B only when the dataset is genuinely a per-date full snapshot. Use C only for small dependency-free lookup tables. Do not build C in the slice.

---

## Question 4 — Python Package Architecture

### 4.1 Honest critique of §68

§68 proposes `csv_processor/{filename,detector,parser,validation,normalization,deduplication,incremental,cdc,scd,storage,models}`. It is a reasonable **taxonomy of CSV concerns**. It is not an architecture. Six specific problems:

| # | Problem | Consequence |
|---|---|---|
| 1 | **CSV is at the root of the namespace.** `cdc/` and `scd/` sit *inside* a package named `csv_processor` | §29/§95 ("add non-CSV sources without redesigning") is violated by the import path on day one. A Kafka CDC source importing `csv_processor.cdc` is an architecture smell that will be permanent |
| 2 | **No composition seam.** The modules are a bag of utilities; nothing says how a run is assembled, ordered, checkpointed or aborted | The composition logic will accrete somewhere — most likely in `cli.py` or, worse, in the DAG, violating §6.4 |
| 3 | **No config layer**, despite §65/§66 making config the centre of gravity | Config parsing scatters across every module; §66 versioning has nowhere to live |
| 4 | **No metadata/control-plane package**, despite §24/§37/§62/§82/§83 | The largest subsystem in the project has no home |
| 5 | `storage/{minio,postgres}` conflates two unrelated concerns | Object-store reads and warehouse publication end up coupled; publication strategies (§36) have nowhere to go |
| 6 | `models/` as a peer leaf invites cycles | `validation` imports `models`, `models` grows a validation helper, and the graph knots |

### 4.2 Recommended structure

Two distributions in one repo. `dataplat` is source-agnostic; `csv_processor` is a plugin.

```
src/
├── dataplat/                        # source-agnostic core (installable, importable alone)
│   ├── models/                      # frozen dataclasses. DEPENDS ON NOTHING internal.
│   │   ├── identity.py              # DatasetRef, FileIdentity, BatchIdentity, RunContext
│   │   ├── record.py                # RecordChunk (columnar), ChangeEnvelope
│   │   ├── schema.py                # ColumnSpec, DatasetSchema, SchemaVersion, SchemaDiff
│   │   ├── profile.py               # SourceProfile (encoding, dialect, header layout)
│   │   └── report.py                # ValidationResult, RejectedRecord, RunReport
│   ├── errors.py                    # §71 hierarchy — RUN-FATAL only
│   ├── config/
│   │   ├── model.py                 # DatasetConfig (pydantic) — the §65 YAML shape
│   │   ├── loader.py                # load + merge defaults + resolve to canonical form
│   │   ├── hashing.py               # canonical JSON -> sha256
│   │   └── registry.py              # ConfigRegistry protocol; Postgres + filesystem impls
│   ├── pipeline/
│   │   ├── protocol.py              # Stage, StreamingStage, BarrierStage, PipelineContext
│   │   ├── engine.py                # sequencing, chunk loop, checkpoints, error policy
│   │   └── stages/                  # generic stages, source-agnostic
│   ├── sources/
│   │   ├── protocol.py              # Source, RecordStream
│   │   └── registry.py              # entry-point plugin discovery
│   ├── validation/                  # structural, schema, types, quality, rules, thresholds
│   ├── normalization/               # strings, numbers, dates, booleans, nulls, whitespace
│   ├── deduplication/               # strategies + record hashing
│   ├── incremental/                 # watermark read/advance
│   ├── load/
│   │   ├── staging.py               # StagingLoader (COPY, chunked, checkpointed)
│   │   └── publish/                 # Publisher protocol: merge, append, partition_replace,
│   │                                #   full_swap, scd0, scd1, scd2, cdc_apply
│   ├── storage/
│   │   ├── objectstore.py           # ObjectStore protocol + S3/MinIO impl
│   │   └── db.py                    # engine/session factory, transaction helpers
│   ├── metadata/
│   │   ├── repository.py            # MetadataRepository protocol
│   │   ├── postgres.py              # implementation
│   │   └── migrations/              # alembic
│   ├── observability/
│   │   ├── logging.py               # §70 structlog config, context binding, redaction
│   │   ├── metrics.py               # §82 counters/histograms
│   │   └── tracing.py               # OTel spans
│   ├── secrets/
│   │   └── resolver.py              # SecretRef: env:// | file:// | vault://
│   └── cli.py                       # THE POD ENTRYPOINT
│
└── csv_processor/                   # CSV source plugin — depends on dataplat, not vice versa
    ├── filename/                    # §8 masks, regex, facet extraction
    ├── detect/                      # §9 encoding, §10 dialect, §11 header/footer, §12 inference
    ├── read/                        # §39 streaming reader, bounded memory
    └── source.py                    # implements dataplat Source; registered via entry point
```

**On the naming deviation.** PROJECT.md names the package `csv_processor`. The split above keeps that name for the CSV component and introduces `dataplat` for the core. If a single distribution is preferred, the cheap equivalent is `csv_processor/core/…` + `csv_processor/sources/csv/…` — same seams, uglier imports. **Recommend the two-package split**; the cost is one extra `pyproject` entry and the benefit is that §95 is structurally true rather than aspirational.

### 4.3 The core abstractions

```python
# dataplat/sources/protocol.py
class RecordStream(Protocol):
    schema: DatasetSchema
    profile: SourceProfile
    def chunks(self, *, start_offset: int | None = None) -> Iterator[RecordChunk]: ...
    #                    ^^^^^^^^^^^^ this parameter is what makes §38 resume possible

class Source(Protocol):
    def inspect(self, ctx: PipelineContext) -> SourceProfile: ...
    def open(self, ctx: PipelineContext) -> AbstractContextManager[RecordStream]: ...
```

```python
# dataplat/pipeline/protocol.py
@dataclass(frozen=True)
class PipelineContext:
    run: RunContext                    # run_id, idempotency_key, dag/task/pod/trace ids
    config: DatasetConfig              # resolved, carries config_hash
    schema: SchemaVersion
    metadata: MetadataRepository
    objects: ObjectStore
    db: Database
    log: BoundLogger

@dataclass
class StageResult:
    chunk: RecordChunk                 # what survives
    rejected: list[RejectedRecord]     # §51 — data, not exceptions
    findings: list[ValidationResult]   # §23
    metrics: Counter                   # rows_* deltas

class StreamingStage(Protocol):        # runs once per chunk — bounded memory (§39)
    name: str
    def apply(self, ctx: PipelineContext, chunk: RecordChunk) -> StageResult: ...

class BarrierStage(Protocol):          # runs once per run, after all chunks are staged
    name: str
    def apply(self, ctx: PipelineContext) -> StageResult: ...
```

**The streaming/barrier split is the load-bearing distinction.** Parse, structural-validate, type-cast, normalize and intra-file dedup are streaming. Cross-batch dedup, threshold evaluation, publication and reconciliation are barriers, because they need the whole run. Making this explicit in the type system is what keeps §38 (checkpointing), §39 (bounded memory) and §36 (atomic publication) from fighting each other: **checkpoints only ever occur between streaming chunks; barriers are never checkpointed.**

```python
# dataplat/load/publish/protocol.py
class Publisher(Protocol):
    name: str
    def publish(self, ctx: PipelineContext, staging_table: str,
                conn: Connection) -> PublishResult: ...
    # receives an OPEN transaction — the engine owns the transaction boundary,
    # so watermark + run-status updates commit with the data (Q3, §28)
```

### 4.4 Config-not-code (§65)

A new dataset is added by:
1. `configs/datasets/<name>.yaml` in the repo
2. an entry in `meta.datasets`
3. one Alembic migration for the target table

Zero new Python. The mechanism: `DatasetConfig` names stages and strategies by **string keys** resolved through registries.

```yaml
# configs/datasets/transactions.yaml
dataset: transactions
config_schema_version: 1
source:
  type: csv                     # -> SOURCE_REGISTRY["csv"]
  bucket: raw
  path: transactions/
  change_semantics: snapshot    # snapshot | cdc
deduplication:
  strategy: business_key_latest # -> DEDUP_REGISTRY["business_key_latest"]
  keys: [transaction_id]
  order_by: [event_timestamp desc]
load:
  strategy: merge               # -> PUBLISHER_REGISTRY["merge"]
  target: normalized.transactions
```

Registries are populated from Python entry points, so a new source or publisher is a package, not a core edit.

### 4.5 Errors (§71) and logging (§70), threaded

**Errors.** `dataplat.errors` defines only run-fatal conditions:

```
DataPlatformError
├── ConfigurationError        (bad config, unknown strategy, missing dataset)
├── SourceError
│   ├── FileInspectionError
│   ├── FilenameParsingError
│   ├── EncodingDetectionError
│   ├── CsvDialectDetectionError
│   └── CsvParsingError        (stream-fatal, e.g. unterminated quote at EOF)
├── SchemaError
│   ├── SchemaValidationError
│   └── IncompatibleSchemaError   (§13 BREAKING change under a strict policy)
├── QualityThresholdExceeded   (§50 FAIL — the run dies, records were still recorded)
├── StorageError               (MinIO/PG unreachable, permission denied)
├── PublicationError           (constraint violation, lock timeout)
└── SecretResolutionError
```

Every one carries `context: dict` populated from `PipelineContext` and is caught **once**, in `cli.py`, which writes `error_type`/`error_message`/`error_detail` to `meta.ingestion_runs` and exits non-zero. No `except Exception` anywhere else.

Row-level problems never raise. A malformed row becomes a `RejectedRecord` inside `StageResult.rejected`. `QualityThresholdExceeded` is raised only by the barrier stage that evaluates §50 thresholds — after the counts are already persisted.

**Logging.** `structlog` with `contextvars`. Bound once at pipeline entry:

```python
observability.logging.configure(json=in_cluster, level=cfg.log_level)
bind_contextvars(dataset=..., run_id=..., idempotency_key=...,
                 file_id=..., dag_id=..., dag_run_id=..., task_id=...,
                 map_index=..., pod=..., trace_id=..., config_version=...,
                 schema_version=..., processor_version=...)
```

Every subsequent line inherits that context with no plumbing — §70's contextual-logging requirement satisfied without threading a logger through 40 functions. Stages add `stage=`, row-level events add `row=`. A **redaction processor** in the chain drops keys matching a secret pattern and truncates any `raw_line`/`record` field to `N` chars unless `log_sensitive_records` is explicitly enabled. JSON renderer in-cluster, console renderer locally. `print()` is banned by a `ruff` rule (`T201`), enforced in CI from Stage 0.

---

## Question 5 — The Config System

### 5.1 Where configs live

**Git authors; PostgreSQL is the runtime system of record.**

```
configs/datasets/*.yaml   →  CI validates + hashes  →  meta.config_versions  →  pod reads by ID
     (authoring)                (gate)                  (system of record)       (execution)
```

| Option | Verdict | Reason |
|---|---|---|
| Repo YAML only | Insufficient alone | Cannot answer "which config did run 8123 use?" after a redeploy; not joinable with run metadata in SQL |
| Kubernetes ConfigMap only | **Rejected** | No version history, 1 MiB limit, not queryable alongside `meta`, and a `helm upgrade` silently rewrites history |
| Database only (UI-edited) | **Rejected** | No code review, no CI validation, violates §79/GitOps |
| **Repo + synced to DB (recommended)** | **Adopt** | Review and CI on the way in; queryable, versioned, immutable once written on the way out |

A `config-sync` job (an Airflow DAG on a schedule *and* a post-deploy Kubernetes Job) walks `configs/datasets/`, resolves and canonicalizes each config, hashes it, and:
- hash matches the current version → no-op
- hash differs → `UPDATE ... SET valid_to = now() WHERE valid_to IS NULL`, then `INSERT` version `max+1` with `valid_from = now()`
- config absent from repo → mark dataset inactive, never delete versions

### 5.2 Versioning and hashing

Hash the **canonicalized resolved** config, not the raw YAML text:

```python
resolved = DatasetConfig.model_validate(merge(DEFAULTS, raw_yaml))
canonical = json.dumps(resolved.model_dump(mode="json"),
                       sort_keys=True, separators=(",", ":"), ensure_ascii=False)
config_hash = hashlib.sha256(canonical.encode()).hexdigest()
```

Consequences, both intentional:
- reordering keys or editing a comment produces **no** new version;
- changing a platform default produces a new version for **every** dataset it touches — which is correct, because processing behaviour genuinely changed.

`config_schema_version` versions the *format*, so `loader.py` can migrate a v1 document into the v3 model when replaying an old run.

### 5.3 How a run records its config

`meta.ingestion_runs.config_version_id` is a NOT NULL FK, written when the run row is created — *before* any processing. Combined with `schema_version_id`, `processor_image_digest` and `files.content_sha256`, that is the complete §62 replay tuple.

### 5.4 Which config version a run uses — the `config_policy` knob

This is where §33 (backfills) and §66 (config versioning) collide, and it needs an explicit answer.

The DAG's **first** task resolves the config version once and pins it into every assignment. A DAG parameter selects the policy:

| Policy | Resolution | Use when |
|---|---|---|
| `AS_OF_LOGICAL_DATE` (default for backfills) | `WHERE valid_from <= logical_date AND (valid_to IS NULL OR valid_to > logical_date)` | Reproducing history faithfully (§33: "respect historical schema versions") |
| `LATEST` (default for scheduled runs) | `WHERE valid_to IS NULL` | Normal forward processing |
| `PINNED:<version>` | exact version | Replay (§62), or a corrective backfill |

The third row is why this must be a knob rather than a rule: you often backfill *precisely because* the historical config was wrong, and `AS_OF_LOGICAL_DATE` would faithfully reproduce the bug.

**Pinning once per DAG run is essential.** If each mapped pod resolved the config independently, a `config-sync` landing mid-run would split one logical run across two configurations — undetectably.

### 5.5 Replay (§62)

```
replay DAG (params: run_id, config_policy=PINNED, target_schema=normalized_replay)
  → read meta.ingestion_runs[run_id]
  → fetch config_versions.config_document, schema_versions.columns
  → verify files.content_sha256 still matches the object in raw/  (§63 immutability check)
  → launch the pod at the recorded processor_image_digest
  → write a NEW run with replay_of_run_id = <original>
```

Replaying into a shadow schema and diffing against the original output is the concrete test that §67 determinism holds.

---

## Question 6 — The Airflow ↔ Processor Interface Contract

### 6.1 The contract in one sentence

> **Airflow hands the pod a URI to a durable work assignment and receives back a receipt, never a payload.**

### 6.2 Downstream: Airflow → pod

```
┌──────────────────────────────────────────────────────────────────────────┐
│  @task resolve_config(dataset, logical_date, config_policy)              │
│     → {"config_version_id": 7, "config_hash": "…", "schema_version_id": 3}│
│         (small — travels in XCom)                                        │
└───────────────────────────────┬──────────────────────────────────────────┘
                                ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  @task discover_files(cfg, data_interval)                                │
│     thin wrapper over dataplat.discovery:                                │
│       list s3://raw/<path>/  ×  LEFT JOIN meta.files                     │
│       → classify NEW | KNOWN | MODIFIED | DUPLICATE | LATE | MISSING (§40)│
│       → INSERT rows into meta.files                                      │
│       → group into work units per config.batching                        │
│       → write ONE assignment JSON per unit to MinIO                      │
│       → pre-allocate meta.ingestion_runs rows (status=PENDING)           │
│     returns list[{"assignment_uri", "idempotency_key", "run_id"}]        │
│         (~200 bytes each — 1000 units ≈ 200 KB of XCom)                  │
└───────────────────────────────┬──────────────────────────────────────────┘
                                ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  KubernetesPodOperator.partial(                                          │
│      task_id="ingest", image=PROCESSOR_IMAGE_DIGEST, cmds=["dataplat"],  │
│      namespace="data-etl", service_account_name="csv-processor",         │
│      env_from=[ConfigMapEnvSource("dataplat-runtime")],                  │
│      container_resources=cfg.resources,                                  │
│      do_xcom_push=True, on_finish_action="delete_succeeded_pod",         │
│      retries=3, retry_exponential_backoff=True,                          │
│  ).expand(arguments=[["ingest", "--assignment", u] for u in uris])       │
└───────────────────────────────┬──────────────────────────────────────────┘
                                ▼
                     one pod per work unit
```

**Why `arguments` rather than env vars for the assignment.** The command line is visible in `kubectl describe pod`, in the Airflow UI's rendered-template view, and in the pod spec captured by `dry_run()` in tests. Env vars carry *environment* identity (endpoints, log level, namespace) which is identical across all mapped instances and therefore belongs on `.partial()` via `env_from`.

**Why an assignment document rather than inline arguments.** A work unit may reference many files with checksums, control totals, resource hints and the full run identity — hundreds of bytes to kilobytes. Putting it in MinIO makes it (a) durable, so an Airflow retry replays the *identical* work order, (b) addressable, so §62 replay and §82 forensics can read exactly what the pod was told, and (c) size-unbounded.

**Assignment document shape** (`s3://metadata/assignments/<dataset>/<logical_date>/<dag_run_id>/unit-0003.json`):

```json
{
  "assignment_version": 1,
  "run_id": 8123,
  "idempotency_key": "a3f1…",
  "dataset": "customers",
  "config_version_id": 7,
  "config_hash": "9c2e…",
  "schema_version_id": 3,
  "batch": {"batch_key": "customers:2026-08-11:001", "batch_id": 442},
  "files": [
    {"file_id": 9911,
     "object_uri": "s3://raw/customers/2026/08/11/customers_20260811.csv",
     "content_sha256": "7d1a…", "size_bytes": 18234421}
  ],
  "target": {"schema": "normalized", "table": "customers", "partition": "2026-08-11"},
  "airflow": {"dag_id": "csv_ingest_customers", "dag_run_id": "manual__2026-08-11T10:00:00+00:00",
              "task_id": "ingest", "map_index": 3, "try_number": 1,
              "logical_date": "2026-08-11T00:00:00+00:00",
              "data_interval_start": "2026-08-11T00:00:00+00:00",
              "data_interval_end": "2026-08-12T00:00:00+00:00"},
  "policy": {"config_policy": "LATEST", "on_bad_record": "REJECT_RECORD", "dry_run": false}
}
```

### 6.3 Upstream: pod → Airflow

`KubernetesPodOperator` returns data only through the XCom sidecar: `do_xcom_push=True` injects `airflow-xcom-sidecar`, the main container writes valid JSON to `/airflow/xcom/return.json`, and **XComs are pushed only for tasks that reach `State.SUCCESS`.**

That last clause is the design constraint. Therefore:

- **The pod writes its run row to `meta.ingestion_runs` before doing any work, and updates it at the end.** The database — not XCom — is the authoritative status channel. This is exactly what §37 demands ("recovery must not require manual inspection of logs") and it is the only mechanism that survives the pod being OOM-killed.
- The receipt is a **≤4 KB summary**, never a payload:

```json
{"run_id": 8123, "status": "SUCCEEDED", "rows_read": 182734, "rows_loaded": 182722,
 "rows_invalid": 0, "rows_deduplicated": 12, "duration_ms": 41022,
 "quarantined": false, "report_uri": "s3://metadata/reports/customers/8123.json"}
```

- The full validation report (§23) goes to MinIO `metadata/reports/` and to `meta.validation_results`.
- Downstream tasks aggregate receipts for alerting and freshness; anything needing detail queries `meta` or fetches `report_uri`.

### 6.4 Guardrails that must be configured

| Concern | Mechanism |
|---|---|
| XCom bloat from large fan-outs | `[core] xcom_backend = airflow.providers.common.io.xcom.backend.XComObjectStorageBackend` with `xcom_objectstorage_path = s3://minio_default@metadata/xcom`, `xcom_objectstorage_threshold = 65536`, `xcom_objectstorage_compression = gzip`. Small values stay in the DB, large ones spill to MinIO with a DB reference — hybrid, and it keeps the Airflow metadata DB (§4.1) clean |
| Fan-out ceiling | `[core] max_map_length` defaults to **1024**; a discovery task returning a longer list **fails the task**. Dataset configs must set `batching.max_units_per_run` below that and the discovery task must group files into units rather than emitting one unit per file unconditionally |
| Runaway concurrency | `max_active_tis_per_dag` on the mapped task (note: it applies across *all* active DagRuns, not per run) plus an Airflow pool per dataset |
| Orphaned pods | `on_finish_action="delete_succeeded_pod"` (keep failed pods for forensics) + a reaper CronJob for pods whose owning task instance no longer exists |
| Stale `RUNNING` runs | `lease_expires_at` heartbeat on `meta.ingestion_runs` (see Q7) |

### 6.5 Airflow 3 trap the roadmap must plan around

For DAG runs triggered by an **Asset event** or via the **REST API without an explicit `logical_date`**, Airflow 3 sets `logical_date = None`, the run has **no data interval**, and reading `logical_date` / `data_interval_start` / `data_interval_end` from the task context raises **`KeyError`**.

Since §48 mandates Airflow Assets for dataset dependencies, every downstream (warehouse/SCD) DAG *will* be asset-triggered. Rule for this codebase:

```python
@task
def resolve_window(dag_run=None, data_interval_start=None, data_interval_end=None):
    if dag_run.logical_date is None:                 # asset- or API-triggered
        return derive_window_from(dag_run.conf, triggering_asset_events)
    return {"start": data_interval_start, "end": data_interval_end}
```

Never read `logical_date` directly from the context in a DAG that can be asset-triggered. Add this to the DAG-authoring checklist in Stage 4, not after the first production surprise.

---

## Question 7 — Idempotency Mechanics

Four nested layers. Each one has a database constraint behind it — none rely on application discipline alone (§24: "use database constraints and upsert/merge mechanisms").

### Layer 1 — File identity
```sql
UNIQUE (dataset_id, object_uri, content_sha256)          -- an arrival
CREATE INDEX ON meta.files (dataset_id, content_sha256); -- content
```
Discovery computes the SHA-256 (streaming, never loading the file) and sets `duplicate_of_file_id` when the content is already known for the dataset. Config decides what happens: `skip`, `reprocess`, or `fail`. §25 preserved — duplicate file, duplicate record, overlapping batch and intentional backfill remain four distinct states.

### Layer 2 — Run identity (the key mechanism)
```
idempotency_key = sha256(
    dataset_name  ‖ file.content_sha256 ‖ config_hash ‖
    schema_version ‖ processor_image_digest ‖ target_partition ‖ policy_digest
)
```

**`try_number` and `dag_run_id` are deliberately absent.** An Airflow retry therefore produces the *same* key and collapses onto the same run row — which is what makes retries free.

Claim protocol, executed by the pod at startup:

```sql
INSERT INTO meta.ingestion_runs (idempotency_key, status, started_at, lease_expires_at, ...)
VALUES (:key, 'RUNNING', now(), now() + interval '5 minutes', ...)
ON CONFLICT (idempotency_key) DO UPDATE
   SET status           = 'RUNNING',
       try_number       = EXCLUDED.try_number,
       k8s_pod_name     = EXCLUDED.k8s_pod_name,
       lease_expires_at = now() + interval '5 minutes'
 WHERE meta.ingestion_runs.status IN ('PENDING','FAILED')
    OR (meta.ingestion_runs.status = 'RUNNING'
        AND meta.ingestion_runs.lease_expires_at < now())      -- crashed pod takeover
RETURNING run_id, status;
```

Three outcomes:
- **row returned** → this pod owns the run; proceed (possibly resuming from `run_stages.checkpoint`)
- **no row, existing status `SUCCEEDED`** → already done; write `SKIPPED_DUPLICATE`, emit a receipt, `exit 0`
- **no row, existing status `RUNNING` with a live lease** → another pod owns it; `exit 0` with a `CONCURRENT_RUN` receipt (§87)

The pod heartbeats `lease_expires_at` from a background thread. A pod killed mid-run leaves a `RUNNING` row with an expiring lease, which is both the recovery signal (§37) and a monitorable condition — no log reading required.

### Layer 3 — Record identity
```
_record_hash = sha256( canonical_encode(normalized_value(c)) for c in tracked_columns )
```
Hashed **after** normalization, which is why dedup must sit downstream of normalization in the stage order. Determinism (§67) requires a canonical encoding: fixed column order from the schema version, explicit NULL sentinel distinct from empty string, decimals at fixed scale, timestamps in UTC ISO-8601. Get this wrong and §60 (repeated identical CDC events must not create SCD versions) silently fails.

### Layer 4 — Target-row identity
Every target table carries a business-key unique index. Publication is `MERGE` (or `INSERT … ON CONFLICT`) against it, with two guards:

```sql
WHEN MATCHED AND t._record_hash <> s._record_hash   -- suppress no-op writes
                AND s.event_ts   >= t.event_ts      -- §32: a late old record must not clobber a newer one
```

For SCD2 targets, add the constraint that makes overlapping validity intervals impossible at the storage layer:

```sql
ALTER TABLE warehouse.dim_customer
  ADD CONSTRAINT dim_customer_no_overlap
  EXCLUDE USING gist (customer_id WITH =, tstzrange(valid_from, valid_to) WITH &&);
```

### Checkpointing × transactions (§38 × §35 × §37)

| File size | Staging | Publication | Checkpoints |
|---|---|---|---|
| < `checkpoint_threshold_rows` (default 500 k) | one transaction | one transaction | none |
| ≥ threshold | one transaction **per chunk** into `staging.<ds>__r<run_id>` | still **one** transaction | `run_stages.checkpoint` updated in the *same* transaction as the chunk |

The rule that reconciles §38 with §36: **checkpoints exist only inside the staging phase; publication is never checkpointed.** Staging is private to the run, so partial visibility there is harmless; `normalized` is public, so it flips once.

Resume: read `run_stages.checkpoint.byte_offset`, seek the reader (safe because the raw object is immutable — §63), continue. If the stream is not seekable (gzip, UTF-16 with a stateful decoder), truncate staging and restart from row 0 — still correct, just slower. **Document per-source which mode applies**, and record `resume_supported` on the profile so operators can predict the cost.

### Concurrency and races (§86, §87)
- `pg_advisory_xact_lock(hashtext(target || partition))` held for the publication transaction serializes concurrent publishers to the same target.
- The run-claim protocol above serializes two DAG runs racing on the same file.
- Airflow pools cap per-dataset parallelism; `max_active_runs=1` per ingestion DAG unless the dataset is genuinely partition-disjoint.

---

## Question 8 — CDC and SCD Placement

### The placement rule

> **CDC is a `Source`. SCD is a `Publisher`. Neither is a pipeline.**

```
        ┌──────────────┐
        │ csv Source   │──┐
        ├──────────────┤  │
        │ cdc-over-csv │──┤          ┌───────────────────────────────┐        ┌─────────────────┐
        ├──────────────┤  ├─ Record ─▶ validate → normalize → dedup ─▶ staging ─▶  Publisher     │
        │ debezium/    │──┤   Chunk  └───────────────────────────────┘        │  ┌────────────┐ │
        │ jdbc Source  │──┘   (+ChangeEnvelope)      SHARED, SOURCE-AGNOSTIC  │  │ append     │ │
        └──────────────┘                                                      │  │ merge      │ │
                                                                              │  │ scd0/1/2   │ │
                                                                              │  │ cdc_apply  │ │
                                                                              │  │ partition  │ │
                                                                              │  └────────────┘ │
                                                                              └─────────────────┘
```

Every source emits `RecordChunk`. When `source.change_semantics: cdc`, each record additionally carries a `ChangeEnvelope`:

```python
@dataclass(frozen=True)
class ChangeEnvelope:
    op: Literal["INSERT", "UPDATE", "DELETE"]
    key: Mapping[str, Any]
    source_ts: datetime
    txid: str | None
    sequence: int | None          # LSN / offset — §30 ordering
    source_table: str | None
    before: Mapping[str, Any] | None
    after:  Mapping[str, Any] | None
```

### Why this makes §59 (CDC feeds SCD) free

`source: cdc` + `load.strategy: scd2` is a **config combination**, not a code path. The four cells of the matrix all work with no additional integration:

| | `merge`/`scd1` | `scd2` |
|---|---|---|
| **snapshot source** | current-state upsert | snapshot-diff historisation |
| **cdc source** | apply latest change per key | one dimension version per tracked-attribute change (§59) |

### Ordering (§30) — a barrier stage, not a source concern

`OrderByChangeSequence` is a `BarrierStage` operating on staged rows: partition by `key`, order by `COALESCE(sequence, txid_numeric, source_ts)`, then collapse to the terminal state per key for `scd1`, or preserve the full ordered sequence for `scd2`. Doing this in SQL over the staging table (rather than in Python over the stream) is what allows a CDC batch to arrive across multiple files out of order and still apply correctly.

**Delivery semantics, stated honestly (§30 forbids unearned claims):**
- Source → MinIO/staging: **at-least-once**
- Apply to target: **idempotent** (record hash + business key + exclusion constraint)
- Therefore end-to-end: **effectively-once**. Not exactly-once — there is no distributed transaction between the object store and PostgreSQL. Write this in `docs/csv-processing.md`.

### §60 — replayed identical events

The `scd2` publisher compares the incoming `_record_hash` (over **tracked attributes only**) against the current version's hash. Equal → `SCD_NO_CHANGE`, recorded in `meta.dedup_decisions`, no new version. Three identical `UPDATE customer 123 → DE` events produce one version, and the *reason* is auditable.

### §58 — late-arriving SCD corrections

`scd2.late_arrival ∈ {reject, append_only, correct_intervals}`. In `correct_intervals`, the publisher locates the version whose `[valid_from, valid_to)` contains the late record's effective time, splits it, inserts the corrected version, and rewrites the neighbouring bounds — all inside the publication transaction, with the GiST exclusion constraint as the safety net that makes a bug loud instead of silent.

### §57 — effective dating

`scd.effective_time_source` is **mandatory** in config, with no default. The permitted values are `event_time`, `business_time`, `source_effective_time` and `ingestion_time`, and the last must be chosen explicitly. §57 says "do not automatically use ingestion time as the effective date" — the way to guarantee that is to refuse to have a default.

### Where SCD runs

`warehouse` is fed by a **separate DAG**, triggered by an Airflow Asset that the ingestion DAG produces (§48). Ingestion and dimensional modelling then retry independently, and a warehouse bug never forces a re-ingest of raw data. §63's layering (`RAW → STAGING → NORMALIZED → WAREHOUSE`) becomes a DAG boundary rather than a comment.

---

## Question 9 — Secrets Flow

### 9.1 Credential path to the CSV processor pod

```
 1. KubernetesPodOperator creates the pod
        serviceAccountName: csv-processor    namespace: data-etl
        annotations:
          vault.hashicorp.com/agent-inject: "true"
          vault.hashicorp.com/role: "csv-processor"
          vault.hashicorp.com/agent-inject-secret-analytical-db: "kv/data/etl/analytical-db"
          vault.hashicorp.com/agent-inject-template-analytical-db: |
            {{- with secret "kv/data/etl/analytical-db" -}}
            postgresql://{{ .Data.data.username }}:{{ .Data.data.password }}@analytical-db.data:5432/analytics
            {{- end }}
          vault.hashicorp.com/agent-inject-secret-minio: "kv/data/etl/minio"
        ▼
 2. Vault mutating admission webhook injects `vault-agent-init` + `vault-agent` sidecar
        ▼   ── TRUST BOUNDARY B1 ──
 3. Agent reads the projected SA token at
        /var/run/secrets/kubernetes.io/serviceaccount/token
    and POSTs to  auth/kubernetes/login  {role: "csv-processor", jwt: <token>}
        ▼
 4. Vault validates the JWT via the Kubernetes TokenReview API
    (Vault's k8s auth mount is configured with a reviewer SA + the cluster's
     issuer; on kind, `issuer` must match `kubectl get --raw /.well-known/openid-configuration`)
        ▼   ── TRUST BOUNDARY B2 ──
 5. Role `csv-processor`:
        bound_service_account_names:      ["csv-processor"]
        bound_service_account_namespaces: ["data-etl"]
        token_policies:                   ["csv-processor"]
        token_ttl: 20m   token_max_ttl: 1h
    Policy `csv-processor`:
        path "kv/data/etl/analytical-db" { capabilities = ["read"] }
        path "kv/data/etl/minio"         { capabilities = ["read"] }
        # and NOTHING else — §81.6 least privilege
        ▼
 6. Agent renders templates to /vault/secrets/* on a **tmpfs** emptyDir
    shared with the app container; the sidecar renews the lease and re-renders
        ▼   ── TRUST BOUNDARY B3 ──
 7. dataplat SecretsResolver reads  file:///vault/secrets/analytical-db
    The processor never sees a Vault token, never calls Vault, and never has the
    credential in an environment variable
        ▼   ── TRUST BOUNDARY B4 ──
 8. Connects to PostgreSQL as role `etl_writer` (schema-scoped grants)
```

**Why files rather than env vars.** Three concrete reasons: (a) env vars appear in `kubectl describe pod` output and in crash dumps of `/proc/<pid>/environ`; (b) env vars are fixed at process start, so rotation requires a restart, whereas the connection factory re-reading `/vault/secrets/analytical-db` on each new connection picks up a rotated credential without one (§81.7); (c) tmpfs never touches disk.

### 9.2 Airflow's own path

```ini
[secrets]
backend = airflow.providers.hashicorp.secrets.vault.VaultBackend
backend_kwargs = {"connections_path": "connections",
                  "variables_path": null,
                  "mount_point": "airflow",
                  "url": "http://vault.vault.svc:8200",
                  "auth_type": "kubernetes",
                  "kubernetes_role": "airflow",
                  "kv_engine_version": 2}
```

- `"variables_path": null` **disables Variable lookups entirely** — otherwise every `Variable.get` in every DAG parse round-trips to Vault. Non-secret configuration belongs in a ConfigMap.
- `auth_type: kubernetes` uses the pod's SA token at the default path — the Airflow components authenticate as workload identity, not with a distributed token (§81.6).
- Airflow's SA maps to Vault role `airflow` → policy `airflow` → `kv/data/airflow/*` only. Verifying that `airflow` cannot read `kv/data/etl/analytical-db` is a required negative test (§81.12).

### 9.3 The chicken-and-egg exception — must be documented (§81.5)

`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` and the Fernet key **cannot** come from the secrets backend: Airflow needs them before the backend is loaded. They come from a Kubernetes Secret. §81.5 explicitly requires documenting why an intermediate Kubernetes Secret exists, how it is populated, its lifecycle and limitations:

| | |
|---|---|
| **Why** | Bootstrap ordering — the secrets backend is itself configured by Airflow config |
| **Populated by** | `scripts/initialize-secrets.sh` locally (§81.10); Vault Secrets Operator or External Secrets Operator syncing from Vault in a production analogue |
| **Lifecycle** | Created at install, rotated by re-running the sync + rolling the Airflow deployments |
| **Limitation** | Base64, not encrypted, at rest in etcd; readable by anyone with `get secrets` RBAC in the `airflow` namespace |
| **Production difference** | ESO/VSO reconciles it continuously and it never exists in the repo or in `helm values` |

The same note applies to the Fernet key and the JWT/webserver secret key: they must be **generated once and persisted**, not regenerated by `helm upgrade`, or every stored connection becomes undecryptable.

### 9.4 Trust boundaries, summarized

| ID | Boundary | Trusted on the strength of | Failure mode | Test (§81.12) |
|---|---|---|---|---|
| B1 | Kubernetes API ↔ Vault | Vault's k8s auth reviewer SA + issuer config | Compromise of the reviewer SA compromises **all** workload identity | Assert login fails with a token from an unbound namespace |
| B2 | Pod SA ↔ Vault policy | `bound_service_account_names/namespaces` | Over-broad policy leaks unrelated credentials | Run a pod as SA `airflow`; assert 403 on `kv/data/etl/*` |
| B3 | Agent tmpfs ↔ app container | Pod isolation only — anyone who can `exec` reads the secret | `kubectl exec` grants credential access | Restrict `pods/exec` RBAC; non-root, read-only rootfs |
| B4 | Processor ↔ PostgreSQL | DB role grants | A compromised processor could drop `warehouse` | `etl_writer` has no rights on `warehouse`; `bi_reader` is SELECT-only on `analytics` |

DB roles are the **second independent line of defence** and matter precisely because B3 is weak. §88's "database roles" line should not be treated as decoration.

---

## Question 10 — Build Order

### 10.1 Dependency-ordered sequence

```
S0 ─ Repo & toolchain + CI skeleton
     │
     ├──────────────────────────────┬────────────────────────────────┐
     ▼                              ▼                                ▼
S1 ─ kind cluster + registry   S3 ─ dataplat core + metadata     (fixture corpus
     │                              schema + naive csv reader      grows from here)
     ▼                              + Dockerfile
S2 ─ Infrastructure                 │
     ├─ 2a MinIO                    │  (S2 and S3 are FULLY PARALLEL —
     ├─ 2b Analytical PG            │   S3 tests against docker, not kind)
     ├─ 2c Airflow PG               │
     └─ 2d Airflow ◀── needs 2c     │
     │                              │
     └──────────────┬───────────────┘
                    ▼
S4 ─ Airflow↔K8s smoke  (KubernetesPodOperator runs `dataplat --version`)
                    ▼
╔═══════════════════════════════════════════════════════════════════════════╗
║ S5 ─ VERTICAL SLICE CLOSES  (§93)                                         ║
║      discover → assignment → pod → staging → MERGE → run row + E2E test   ║
║      including the re-run-produces-no-duplicates assertion                ║
╚═══════════════════════════════════════════════════════════════════════════╝
                    │
     ┌──────────────┼──────────────┬───────────────────┐
     ▼              ▼              ▼                   ▼
S6 ─ Vault     S7 ─ Universal  S11 ─ Observability  (S6, S7, S11 PARALLEL)
     secrets         CSV engine       stack
                     ├ 7a filename
                     ├ 7b encoding      (7a–7e are pure functions over
                     ├ 7c dialect        the fixture corpus — mutually
                     ├ 7d header/footer  independent, ideal for parallel
                     ├ 7e inference      agents/contributors)
                     └ 7f streaming
                          │
              ┌───────────┴───────────┐
              ▼                       ▼
S8 ─ Validation & quarantine   S9 ─ Metadata control plane completion
              │                       │   (schema_versions, watermarks,
              └───────────┬───────────┘    run_stages, dedup_audit,
                          ▼                 recon, config-sync job)
S10 ─ ETL correctness
      ├ 10a dedup + incremental/watermarks   ─┐
      ├ 10b backfill + late/out-of-order      │ 10a and 10d parallel;
      ├ 10c recovery + checkpoint + lease     │ 10b needs 10a
      └ 10d reconciliation + control totals  ─┘
                          ▼
S12 ─ CDC + SCD
      ├ 12a SCD 0/1/2 + exclusion constraint + late corrections ─┐ parallel
      ├ 12b CDC source + ChangeEnvelope + ordering barrier      ─┘
      └ 12c CDC→SCD integration  ◀── needs both
                          ▼
S13 ─ CI/CD completion (ephemeral kind E2E, image publish, scanning, deploy)
                          ▼
S14 ─ Operations (runbooks, retention jobs, DR rebuild-from-raw, OpenLineage export)
```

### 10.2 Parallelization map

| Wave | Can run concurrently | Why it is safe | Rough share of total effort |
|---|---|---|---|
| A | **S1+S2** (infra) ‖ **S3** (library) | Different artifacts, different test harnesses. S3 needs only Docker | ~25% |
| B | S4 → S5 | Strictly serial. This is the critical path — protect it | ~15% |
| C | **S6** (Vault) ‖ **S7** (CSV engine) ‖ **S11** (observability stack) | Vault touches manifests; CSV touches `csv_processor/`; observability touches Helm + `dataplat/observability`. Almost no file overlap | ~20% |
| D | 7a ‖ 7b ‖ 7c ‖ 7d ‖ 7e | Pure functions with a shared read-only fixture corpus. **The single best parallelization opportunity in the project** | (within C) |
| E | **S8** (validation) ‖ **S9** (metadata completion) | S8 writes `validation/`; S9 writes `metadata/` + migrations. Coordinate only on `meta.validation_results` DDL | ~10% |
| F | 10a ‖ 10d, then 10b, then 10c | 10b needs watermarks from 10a | ~15% |
| G | 12a ‖ 12b → 12c | Publisher work vs. Source work | ~10% |
| H | S13 ‖ S14 | Independent | ~5% |

### 10.3 Reconciliation with README §92

| §92 stage | Recommendation | Verdict | Justification |
|---|---|---|---|
| P1 Kubernetes | S1 | **Agree** | — |
| P2 Infrastructure | S2 | **Agree**, with Vault removed and the CI-profile values files written now | PROJECT.md warns that retrofitting profile-parameterized Helm values is expensive |
| P3 Airflow+K8s | S4 | **Agree**, and keep the smoke DAG *trivial* | Isolates the highest-risk integration from CSV logic |
| P4 Secrets | **Moved to S6, after the slice** | **Deviate** | The slice needs credentials, not a secrets manager. Vault adds a webhook + auth + policy debugging loop onto the critical path of first end-to-end success. The `SecretsResolver` seam (built in S3) makes the later swap a config change, satisfying §81's "replaceable without changing application code". Cost: a dev-only DSN in a Kubernetes Secret for one phase, removed in S6, with secret scanning active from S0 |
| P5 Basic CSV pipeline | **S5 — and idempotency moves here from P8** | **Deviate** | §92 defers idempotency, checksums and dedup to Phase 8. But `files.content_sha256`, `ingestion_runs.idempotency_key` and the target business key are structural — every table built in P6/P7 would need migrating to accommodate them later. The marginal cost *now* is two unique constraints and a claim query; the cost in P8 is a migration across six phases of accumulated schema. **The slice must include: content hashing, the idempotency key, the claim protocol, and an E2E test asserting that a re-run loads zero rows.** |
| P6 Universal CSV | S7 | **Agree**, and note 7a–7e are heavily parallelizable | — |
| P7 Validation | S8 | **Agree** | — |
| P8 Production-like data engineering | S9 + S10 (minus idempotency, now in S5) | **Split** | P8 bundles ~16 unrelated capabilities into one stage — too coarse for a roadmap phase. Split into metadata-completion (S9) and correctness (S10a–d) |
| P9 CDC + SCD | S12 | **Agree** on content; **add** that placement is `Source`/`Publisher`, not a new pipeline | Without that framing, §29/§95 extensibility will not hold |
| P10 CI/CD | **CI skeleton at S0; full CD at S13** | **Deviate** | §93 requires every capability to ship with "CI validation". A CI pipeline created in the final phase cannot have gated any earlier code. Start with lint + mypy + unit tests + secret scanning on day one; add integration tests at S5, ephemeral-kind E2E at S13 |
| — | **Observability (S11) added as an explicit stage** | **Add** | §82/§83 and the PROJECT.md Prometheus/Grafana/OTel decision have no home in §92 at all. The *seams* (`observability/{logging,metrics,tracing}.py`) belong in S3 as no-ops; the *stack* is a parallel stage after the slice |

**Summary of deviations: four.** (1) Vault after the slice, not before. (2) Idempotency inside the slice, not deferred. (3) CI skeleton first, not last. (4) Observability promoted to an explicit stage. Deviations 2 and 3 are the ones that cost real money if ignored.

---

## Data Flow

### Vertical-slice flow (S5)

```
  developer / test
        │  aws s3 cp customers_20260811.csv s3://raw/customers/2026/08/11/
        ▼
  ┌──────────────┐
  │ MinIO raw/   │  IMMUTABLE (§63)
  └──────┬───────┘
         │  list + stat
         ▼
  ┌──────────────────────────────────────────────────────────┐
  │ Airflow DAG  csv_ingest_customers                        │
  │   @task resolve_config   → config_version_id             │
  │   @task discover_files   → sha256, INSERT meta.files,    │
  │                            INSERT meta.ingestion_runs,   │
  │                            PUT assignment JSON to MinIO  │
  │   KubernetesPodOperator.expand(arguments=[…])            │
  │   @task aggregate_receipts                               │
  └──────┬───────────────────────────────────────────────────┘
         │  creates pod (args: ingest --assignment s3://metadata/…)
         ▼
  ┌──────────────────────────────────────────────────────────┐
  │ ETL pod                                                  │
  │   1. GET assignment  ────────────────────► MinIO         │
  │   2. claim run (ON CONFLICT idempotency_key) ──► meta    │
  │   3. GET object (streaming) ─────────────► MinIO raw/    │
  │   4. parse → chunks (bounded memory)                     │
  │   5. COPY  ─────────────────────────────► staging.*      │
  │   6. BEGIN; MERGE → normalized.customers;                │
  │             UPDATE meta.ingestion_runs;  COMMIT;         │
  │   7. PUT report ────────────────────────► MinIO metadata/│
  │   8. echo receipt > /airflow/xcom/return.json            │
  └──────────────────────────────────────────────────────────┘
         │
         ▼
  ┌──────────────────────────────────────────────────────────┐
  │ Analytical PostgreSQL                                    │
  │   normalized.customers  (+ _run_id, _file_id, …)         │
  │   meta.ingestion_runs   status=SUCCEEDED                 │
  └──────────────────────────────────────────────────────────┘
```

### Full pipeline flow (target state)

```
 SOURCES              DISCOVERY            PROCESSING (in pod)              PUBLICATION
┌────────┐         ┌───────────────┐    ┌──────────────────────┐        ┌─────────────────┐
│ CSV    │         │ list objects  │    │ inspect  (profile)   │        │ staging.<ds>    │
│ CDC    │────────▶│ × meta.files  │───▶│ parse    (stream)    │───────▶│  all TEXT       │
│ future │         │ classify §40  │    │ structural validate  │        │  UNLOGGED       │
└────────┘         │ manifest §41  │    │ type cast + normalize│        └────────┬────────┘
                   │ control file  │    │ intra-file dedup     │                 │
                   │ §43           │    │  ── STREAMING ──     │      set-based validation
                   └───────┬───────┘    └──────────┬───────────┘       + quality thresholds
                           │                       │                            │
                    assignment JSON         ┌──────▼───────────┐        ┌───────▼─────────┐
                    → s3://metadata/        │ cross-batch dedup│        │ quarantine.<ds> │
                           │                │ CDC ordering §30 │        │ + meta.rejected │
                           │                │ referential §47  │        └─────────────────┘
                           │                │  ── BARRIER ──   │                 │
                           │                └──────┬───────────┘                 │
                           │                       │                             │
                           │           ┌───────────▼──────────────────────────┐  │
                           │           │  PUBLICATION TRANSACTION (one txn)   │  │
                           │           │   MERGE / partition swap / SCD apply │  │
                           │           │   + advance meta.watermarks          │  │
                           │           │   + update meta.ingestion_runs       │  │
                           │           └───────────┬──────────────────────────┘  │
                           │                       ▼                             │
                           │              normalized.<entity>                    │
                           │                       │  emits Airflow Asset (§48)  │
                           │                       ▼                             │
                           │              warehouse DAG → SCD dims + facts       │
                           │                       ▼                             │
                           │              analytics views / marts                │
                           ▼                       ▼                             ▼
                    ╔═══════════════════════════════════════════════════════════════╗
                    ║  meta.*  — written at EVERY step; queried for lineage,        ║
                    ║  recovery, reconciliation, freshness, replay and audit        ║
                    ╚═══════════════════════════════════════════════════════════════╝
```

---

## Recommended Repository Structure

```
airflow-etl-platform/
├── airflow/
│   ├── dags/                      # THIN. Target < 150 lines each.
│   │   ├── _common/               # shared DAG factories, NOT business logic
│   │   ├── csv_ingest_factory.py  # one factory; one DAG per dataset config
│   │   ├── warehouse_scd.py       # asset-triggered
│   │   ├── config_sync.py
│   │   ├── retention.py
│   │   └── replay.py
│   └── config/                    # airflow.cfg overlays (non-secret)
├── src/
│   ├── dataplat/                  # source-agnostic core (see Q4)
│   └── csv_processor/             # CSV source plugin
├── configs/
│   ├── defaults.yaml              # platform defaults merged into every dataset
│   └── datasets/*.yaml            # §65 — the config-not-code surface
├── schemas/
│   ├── dataset-config.schema.json # generated from pydantic; validated in CI
│   └── contracts/*.yaml           # §22 explicit data contracts
├── migrations/                    # alembic — meta + normalized + warehouse
├── tests/
│   ├── unit/                      # pure, no I/O
│   ├── integration/               # testcontainers: MinIO + PostgreSQL
│   ├── e2e/                       # against a live kind cluster
│   ├── property/                  # hypothesis: parsing + normalization
│   └── fixtures/csv/              # §73 corpus — the interface for all detectors
├── docker/{airflow,csv-processor}/
├── kubernetes/{namespaces,rbac,vault-policies,network-policies}/
├── helm/
│   ├── values/{local,ci}/         # profile split from day one (PROJECT.md constraint)
│   └── charts/                    # pinned upstream chart versions
├── scripts/                       # cluster-up, initialize-secrets, load-fixtures
├── docs/                          # §75 set + adr/
└── .github/workflows/
```

### Structure rationale

- **`airflow/dags/` is a leaf.** It imports `dataplat`; nothing imports it. Enforce with an import-linter contract in CI — this is the mechanical guarantee behind §6.4, rather than a code-review convention.
- **`src/` layout, not flat.** Prevents accidentally testing the source tree instead of the installed package — which matters here because the pod runs the *installed* package.
- **`configs/` separate from `schemas/`.** `configs/` is per-dataset instances; `schemas/` is the meta-schema plus data contracts. §22 contracts and §65 configs are different artifacts with different owners.
- **`migrations/` at the root, not inside `dataplat`.** The analytical database is a platform asset shared by the processor, the warehouse DAGs and BI. Burying its migrations in one consumer implies false ownership.
- **`helm/values/{local,ci}/` from the first commit.** PROJECT.md flags that GitHub runners (4 CPU / 16 GB) cannot host the full stack and that retrofitting the profile split is expensive.

---

## Architectural Patterns

### Pattern 1: Assignment document as the unit of work

**What:** Airflow writes a durable JSON work order to object storage and passes only its URI to the pod.
**When:** Any orchestrator→worker boundary where the work description exceeds a few hundred bytes or must be auditable.
**Trade-offs:** One extra object-store round trip per task; in exchange the retry replays byte-identical work, the work order is forensically available, and there is no size ceiling.

```python
# in the DAG — thin
units = discover_files(cfg=cfg, window=window)          # returns [{assignment_uri, ...}]
ingest = KubernetesPodOperator.partial(
    task_id="ingest", image=PROCESSOR_DIGEST, cmds=["dataplat"],
    do_xcom_push=True, service_account_name="csv-processor",
).expand(arguments=units.map(lambda u: ["ingest", "--assignment", u["assignment_uri"]]))
```

### Pattern 2: Streaming stages vs. barrier stages

**What:** Two distinct stage protocols. Streaming stages see one chunk; barrier stages see the whole staged run.
**When:** Any pipeline that must simultaneously satisfy bounded memory, resumability and atomic publication.
**Trade-offs:** Two protocols instead of one; in exchange checkpointing has an unambiguous legal position (between streaming chunks only) and publication atomicity is never at risk.

### Pattern 3: Errors-as-values for row-level problems

**What:** `StageResult.rejected: list[RejectedRecord]`. Exceptions are reserved for run-fatal conditions.
**When:** Whenever a spec says "never silently discard records" (§27, §51).
**Trade-offs:** More verbose signatures; in exchange it is *structurally impossible* to lose a record to a swallowed exception, and every rejection carries row number, column and reason.

```python
def apply(self, ctx, chunk) -> StageResult:
    kept, rejected, findings = [], [], []
    for i, row in enumerate(chunk.rows()):
        try:
            kept.append(self._cast(row))
        except ValueCastError as e:                       # LOCAL, narrow, expected
            rejected.append(RejectedRecord(
                source_row_number=chunk.base_row + i, error_column=e.column,
                error_type="TYPE_CAST", error_message=str(e), raw_line=row.raw))
    return StageResult(chunk.replace(kept), rejected, findings, ...)
```

### Pattern 4: The publication transaction as the metadata commit point

**What:** Data rows, watermark advance and run-status update share one transaction.
**When:** Always, for any incremental pipeline with a watermark.
**Trade-offs:** Requires the metadata store and the target to be the same database — which is exactly why `meta` lives in the analytical PostgreSQL and not in a separate service. This constraint is the reason OpenLineage cannot be the system of record.

### Pattern 5: Config-keyed strategy registries

**What:** `deduplication.strategy: business_key_latest` resolves through a registry populated by entry points.
**When:** Whenever §65's "config not code" is a requirement.
**Trade-offs:** Indirection costs discoverability (`grep` no longer finds the call site). Mitigate by generating a strategy catalogue into the docs from the registry in CI.

---

## Anti-Patterns

### AP1: Business logic in DAG files
**What people do:** `@task` functions that parse CSVs, build SQL, or apply validation rules.
**Why it's wrong:** Runs in the DAG processor (§6.2 forbids), unreachable by unit tests, and re-parsed on every DAG-processing loop.
**Instead:** DAGs call `dataplat` functions only. Enforce with an import-linter contract in CI.

### AP2: Typed staging tables
**What people do:** `CREATE TABLE staging.customers (customer_id int, birth_date date, …)`.
**Why it's wrong:** One bad value aborts the whole `COPY` with no row number. §19 (row + column + error type) becomes unimplementable.
**Instead:** All-TEXT staging; type-cast as a validating pass that reports every failure at once.

### AP3: Filename as identity
**What people do:** `WHERE filename NOT IN (SELECT filename FROM processed)`.
**Why it's wrong:** §24 explicitly forbids it. A corrected re-upload under the same name is silently skipped; the same content under a new name is silently duplicated.
**Instead:** `content_sha256` for content identity, `object_uri` for arrival identity, both recorded.

### AP4: Advancing the watermark outside the publication transaction
**What people do:** Load, commit, then `UPDATE meta.watermarks` in a second statement.
**Why it's wrong:** A crash in the gap advances the watermark past unloaded data — permanent silent data loss, and §28 is violated.
**Instead:** Same transaction. Always.

### AP5: Returning data through XCom
**What people do:** Push parsed rows, or a full validation report, to `/airflow/xcom/return.json`.
**Why it's wrong:** Bloats the Airflow metadata DB (§4.1 says it is for Airflow metadata), and XCom is pushed **only on success** — so failure detail never arrives.
**Instead:** Receipts only (≤4 KB). Full state to `meta` and MinIO. Configure `XComObjectStorageBackend` as a safety net.

### AP6: One mapped task per file, uncapped
**What people do:** `expand()` over an unbounded discovery list.
**Why it's wrong:** `[core] max_map_length` defaults to 1024; exceeding it **fails the source task**. 1024 concurrent pods would also saturate a kind cluster.
**Instead:** Group files into work units; cap via `batching.max_units_per_run`; throttle with `max_active_tis_per_dag` and an Airflow pool.

### AP7: `SELECT DISTINCT` as deduplication
**What people do:** `INSERT INTO target SELECT DISTINCT * FROM staging`.
**Why it's wrong:** §26 forbids it. It cannot express business-key, latest-wins, source-priority or batch-aware semantics, and it produces zero audit trail (§27).
**Instead:** Explicit strategy objects writing `meta.dedup_audit` and `meta.dedup_decisions`.

### AP8: Deferring the metadata schema
**What people do:** "Get the pipeline working, add lineage later."
**Why it's wrong:** Lineage columns, the idempotency key and business-key constraints are structural. Retrofitting them means migrating every table built in the interim, plus backfilling values that no longer exist.
**Instead:** Five metadata tables and six lineage columns in the vertical slice.

### AP9: Ingestion time as the SCD effective date
**What people do:** `valid_from = now()`.
**Why it's wrong:** §57 forbids it; it makes backfills (§61) and late-arriving corrections (§58) produce wrong history.
**Instead:** `scd.effective_time_source` is mandatory in config, with **no default**.

### AP10: Reading `logical_date` in an asset-triggerable DAG
**What people do:** `def f(logical_date): ...` in a warehouse DAG scheduled on an Asset.
**Why it's wrong:** In Airflow 3, asset- and API-triggered runs have `logical_date = None` and no data interval; touching those context keys raises `KeyError`.
**Instead:** `dag_run.logical_date` with an explicit fallback derived from `triggering_asset_events` or `dag_run.conf`.

### AP11: Config only in a ConfigMap
**What people do:** Mount `configs/` as a ConfigMap and read it in the pod.
**Why it's wrong:** No version history, 1 MiB ceiling, not joinable with run metadata, and `helm upgrade` silently rewrites it — §66 becomes unanswerable.
**Instead:** Git authors, `meta.config_versions` is the runtime system of record, run rows carry `config_version_id`.

### AP12: Regenerating the Fernet key on every deploy
**What people do:** Let the Airflow Helm chart generate `fernetKey` / `webserverSecretKey` per install.
**Why it's wrong:** Every previously stored connection becomes undecryptable after `helm upgrade`.
**Instead:** Generate once, store in Vault, sync to a Kubernetes Secret, reference it in values.

---

## Scaling Considerations

Reframed from "users" to data volume, since this is not a user-facing system.

| Scale | Architecture adjustments |
|---|---|
| **Slice: 1 file, ~10 k rows** | Single pod, no chunking, no checkpoints, `merge` publication. Everything above is already sufficient |
| **~100 files/day, ≤1 M rows each** | Dynamic Task Mapping with grouped work units; per-dataset Airflow pools; `normalized` partitioned by `business_date`; chunked staging with checkpoints; `UNLOGGED` staging |
| **Files > pod memory (§39)** | Streaming reader with bounded chunk size; `COPY` per chunk; `run_stages.checkpoint` for resume; explicit `container_resources` per dataset (§85) |
| **Dozens of datasets** | DAG factory over `configs/datasets/`; Airflow Assets for cross-dataset dependencies (§48); `meta.quality_metrics` history driving §53 anomaly baselines |
| **Beyond the local cluster** | The seams that make this portable already exist: `ObjectStore` (MinIO→S3), `SecretsResolver` (Vault→cloud KMS), Helm value profiles (local/ci→cloud), `Publisher` (PostgreSQL→Snowflake/BigQuery). No application rewrite |

### Scaling priorities — what breaks first, in order

1. **The Airflow metadata database, via XCom.** Large discovery lists and mapped-task metadata are the first thing to bloat §4.1. Fix: `XComObjectStorageBackend` with a 64 KiB threshold, configured before it hurts.
2. **`max_map_length = 1024`.** The fan-out ceiling arrives sooner than expected. Fix: group files into work units.
3. **Pod memory during parsing.** Fix: enforce chunked reading from the first CSV implementation — retrofitting streaming into a `read_all` design is a rewrite.
4. **Publication lock contention.** Concurrent runs on the same target serialize on the advisory lock. Fix: partition-scoped lock keys, then `partition_replace` for disjoint dates.
5. **`meta.rejected_records` / `dedup_decisions` growth.** Fix: `meta.retention_policies` with a scheduled retention DAG, plus spill-to-MinIO above a per-run inline cap.

---

## Integration Points

### External services

| Service | Integration pattern | Gotchas |
|---|---|---|
| **MinIO** | `boto3`/`s3fs` behind the `ObjectStore` protocol; `s3://` URIs only (§5) | Path-style addressing required in-cluster; MinIO returns different ETags for multipart uploads, so **never treat ETag as a content hash** — compute SHA-256 |
| **Analytical PostgreSQL** | SQLAlchemy Core + `psycopg` `COPY`; Alembic for DDL | `MERGE` requires PG 15+ (PG 17/18 for `WHEN NOT MATCHED BY SOURCE` and `RETURNING`); `ON CONFLICT` needs a real unique index |
| **Kubernetes API** | `KubernetesPodOperator` with in-cluster config | Scheduler SA needs `create/get/list/watch/delete` on pods and `get` on `pods/log` in `data-etl` |
| **Vault** | Agent injector for pods; `VaultBackend` for Airflow | On kind, the k8s auth `issuer` must match the cluster's OIDC discovery document or every login fails with an opaque `permission denied` |
| **Prometheus** | `/metrics` on a sidecar port — but ETL pods are short-lived | Short-lived pods are a genuinely bad scrape target. Prefer OTLP push to an OpenTelemetry Collector that exposes an aggregated scrape endpoint; a Pushgateway is the fallback and carries known staleness pitfalls |
| **OpenLineage** *(optional, S14)* | `apache-airflow-providers-openlineage` + transport config | Additive only — see the evaluation below |

### Internal boundaries

| Boundary | Communication | Notes |
|---|---|---|
| DAG ↔ pod | Assignment URI in `arguments`; receipt via XCom sidecar | The one interface that must be versioned (`assignment_version`) |
| DAG ↔ `dataplat` | Direct Python import (discovery, config resolution only) | Enforce direction with import-linter |
| `csv_processor` ↔ `dataplat` | Implements `Source`; discovered via entry point | Dependency is one-way. `dataplat` must never import `csv_processor` |
| Pipeline ↔ metadata | `MetadataRepository` protocol | Protocol enables an in-memory fake for unit tests without a database |
| Pipeline ↔ target DB | `Publisher` protocol, handed an **open transaction** | The engine owns the transaction boundary; publishers must not commit |
| `normalized` ↔ `warehouse` | Airflow Asset (§48) | Deliberately a DAG boundary, not a function call — independent retry |
| Processor ↔ secrets | `SecretsResolver` over opaque `SecretRef` | The seam that makes the Vault swap a config change |

---

## OpenLineage: Honest Evaluation (§83)

The README does not mention OpenLineage. It is worth an explicit decision because it is the obvious "buy" alternative to the bespoke `meta` schema.

**What it gives cheaply.** `apache-airflow-providers-openlineage` registers an `AirflowPlugin` and a listener that fires on DAG/TaskInstance start/complete/fail, emitting standard `RunEvent`s over a configurable transport. Zero DAG changes. Marquez (the reference implementation) provides a graph UI for free. `custom_run_facets` allows attaching project-specific metadata.

**Why it cannot be the system of record here — three reasons, in order of weight.**

1. **It cannot participate in the publication transaction.** The platform's correctness rests on watermark advance, run status and data rows committing atomically (Q3, Q7). An asynchronous HTTP event emitter cannot enlist in a PostgreSQL transaction. Anything derived from OpenLineage events is eventually-consistent commentary, not state.
2. **Wrong granularity.** OpenLineage models job / run / dataset. PROJECT.md's Core Value is record-level: *"where did this row come from, and is it correct?"* Answering that needs `_run_id`/`_file_id`/`_source_row_number` on the target row.
3. **It has no concept of the control plane.** No idempotency keys, no watermarks, no dedup audit, no rejected records, no reconciliation results, no config versions. Those are queried *during* processing to make decisions, not emitted afterwards for humans.

Secondary cost: Marquez adds a third PostgreSQL and a web application to a laptop cluster already running two PostgreSQL instances, MinIO, Vault, Prometheus, Grafana and Airflow.

**Recommendation — adopt as an additive export in S14, not as the lineage system of record.**
- Keep `meta` as the system of record.
- Enable the provider late with an HTTP or file transport.
- Register a **custom run facet** carrying `run_id`, `file_sha256`, `config_version_id`, `schema_version_id` so the OpenLineage graph joins back to `meta` on `run_id`.
- Deploy Marquez only if the graph UI is genuinely wanted; the facets are useful even with a file transport.

Cost: roughly one config block plus a small facet class. Benefit: a standards-compliant export path and a lineage UI without owning either. Flag to the roadmap as an **addition beyond the README** — accept or reject explicitly.

---

## Confidence Assessment

| Area | Confidence | Basis |
|---|---|---|
| Airflow ↔ pod interface (XCom sidecar, `expand`, `max_map_length`, object-storage XCom backend) | MEDIUM | Verified against Airflow 3.1.6 documentation via Context7 |
| Airflow 3 asset/backfill semantics (`logical_date = None` trap, scheduler-managed backfills) | MEDIUM | Verified against Airflow 3.1.6 release notes via Context7 |
| Vault secrets backend + Kubernetes auth configuration | MEDIUM | Verified against the `hashicorp` provider docs via Context7; the kind-specific issuer caveat is inference, unverified on this cluster |
| PostgreSQL publication primitives (`MERGE`, `ON CONFLICT`, `ATTACH`/`DETACH PARTITION`, transactional DDL) | MEDIUM | Verified against PostgreSQL 18 documentation via Context7 |
| Metadata schema design | MEDIUM | Shape corroborated by convergent industry ingestion-framework patterns (web, LOW individually); column-level design is an opinionated proposal, unvalidated against this workload |
| Python package architecture | MEDIUM | Reasoned design; the `Source`/`Stage`/`Publisher` seams are standard but the specific decomposition is untested here |
| Build order and parallelization | MEDIUM | Derived from the dependency graph; effort percentages are estimates, not measurements |
| OpenLineage evaluation | MEDIUM | Mechanism verified (web + provider docs); the fit judgement is reasoned from this project's transactional requirement |

**Overall: MEDIUM.** The integration mechanics are documentation-verified. The metadata model, package decomposition and build order are opinionated designs that the first two phases will test. Highest-risk unvalidated assumptions, in order: (1) that a locally-built image can be pulled and run by `KubernetesPodOperator` on kind without registry friction; (2) that the Vault Kubernetes auth issuer configuration works on kind without manual JWT-issuer overrides; (3) that streaming CSV parsing with per-chunk `COPY` hits acceptable throughput inside a kind pod's resource limits.

---

## Sources

- Apache Airflow 3.1.6 — `providers/cncf/kubernetes/docs/operators.rst` (KubernetesPodOperator XCom sidecar, `/airflow/xcom/return.json`, `do_xcom_push`, `dry_run()`) — via Context7
- Apache Airflow 3.1.6 — `airflow-core/docs/authoring-and-scheduling/dynamic-task-mapping.rst` and `task-sdk/docs/dynamic-task-mapping.rst` (`expand`/`partial`, `max_map_length`, `max_active_tis_per_dag`, lazy proxies) — via Context7
- Apache Airflow 3.1.6 — `RELEASE_NOTES.rst` (scheduler-managed backfills; `logical_date = None` for asset/API-triggered runs) — via Context7
- Apache Airflow 3.1.6 — `airflow-core/docs/authoring-and-scheduling/asset-scheduling.rst` (`triggering_asset_events`) — via Context7
- Apache Airflow 3.1.6 — `providers/common/io/docs/xcom_backend.rst` and `airflow-core/docs/core-concepts/xcoms.rst` (`XComObjectStorageBackend`, threshold, compression) — via Context7
- Apache Airflow 3.1.6 — `providers/hashicorp/docs/secrets-backends/hashicorp-vault.rst` and `providers/hashicorp/docs/connections/vault.rst` (`VaultBackend` kwargs, `variables_path: null`, `auth_type: kubernetes`, `kubernetes_jwt_path`) — via Context7
- PostgreSQL 18 — `sql-merge.html`, `sql-insert.html`, `sql-altertable.html` (MERGE clauses, `ON CONFLICT`/`EXCLUDED`, `ATTACH`/`DETACH PARTITION` constraints and `CONCURRENTLY` limitations) — via Context7
- [OpenLineage Airflow integration structure](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/guides/structure.html) and [configuration reference](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/configurations-ref.html)
- [AIP-53 OpenLineage in Airflow](https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-53+OpenLineage+in+Airflow)
- [Integrate OpenLineage and Airflow with Marquez — Astronomer](https://www.astronomer.io/docs/learn/marquez)
- [MIND: a metadata-driven ingestion design pattern — ScienceDirect](https://www.sciencedirect.com/science/article/pii/S2214579625000693)
- [Efficient Data Ingestion in Cloud-based Architecture — arXiv 2503.16079](https://arxiv.org/pdf/2503.16079)
- [Metadata-Driven ETL Framework in Databricks — Databricks Community](https://community.databricks.com/t5/technical-blog/metadata-driven-etl-framework-in-databricks-part-1/ba-p/92666)
- [PostgreSQL mailing list: transactional swap of tables](https://www.postgresql.org/message-id/1373639089.44097.YahooMailNeo@web162905.mail.bf1.yahoo.com)
- [Use RENAME to hot-swap two tables — jbranchaud/til](https://github.com/jbranchaud/til/blob/master/postgres/use-rename-to-hot-swap-two-tables.md)
- `README.md` §2–§95 (project master specification) and `.planning/PROJECT.md`

---
*Architecture research for: metadata-driven ETL platform on local Kubernetes*
*Researched: 2026-08-11*
