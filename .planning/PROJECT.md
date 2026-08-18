# Airflow ETL Platform

## What This Is

A local, production-like ETL/data platform running on a multi-node **kind** Kubernetes cluster: Apache Airflow orchestrating containerized ETL workloads, MinIO as the S3-compatible data lake, two separate PostgreSQL instances (Airflow metadata vs. analytical warehouse), and HashiCorp Vault for secrets. Its first workload is a **metadata-driven universal CSV ingestion engine** that discovers, inspects, parses, validates, normalizes, deduplicates and transactionally loads real-world messy CSV files — with schema evolution, incremental processing, CDC and SCD support.

It is explicitly **not** an Airflow tutorial, a CSV parser, a bag of scripts, or a Docker Compose dev environment. It is a platform whose architecture lets additional ETL workloads be added later without redesign.

Built as a **foundation for real work** — the intent is to actually run ETL on it and port its patterns into production systems.

## Core Value

**Every file, batch and record that enters the platform can be traced, explained, reprocessed and trusted** — ingestion is idempotent, auditable and replayable, and no data is ever silently dropped, duplicated or corrupted.

If the platform ingests fast but cannot answer *"where did this row come from, and is it correct?"*, it has failed.

## Requirements

### Validated

<!-- Shipped and confirmed valuable. -->

**Infrastructure** (Phase 2, 2026-08-12)

- [x] Reproducible multi-node kind cluster, destroyable and recreatable from the repo — INFRA-01, INFRA-09
- [x] Airflow 3.x deployed via upstream Helm chart (API server, scheduler, DAG processor, triggerer) — INFRA-02
- [x] Dedicated Airflow metadata PostgreSQL, strictly separate from analytical PostgreSQL — INFRA-03, INFRA-04
- [x] MinIO providing S3-compatible object storage with `raw/validated/processed/quarantine/metadata` layers — INFRA-05
- [x] Infrastructure defined as code; no manual `kubectl` surgery; CI-sized values profile validated in CI — INFRA-07, INFRA-10, CICD-07

**Core Library & Metadata Control Plane** (Phase 3, 2026-08-13)

- [x] Coherent `meta`/`normalized` schema (datasets, config_versions, files, batches, batch_files, ingestion_runs, normalized.customers) hand-migrated via Alembic; every stored hash column carries a companion `hash_version` — META-01, META-02
- [x] Container images reproducible, versioned by git SHA, never `:latest` — csv-processor Dockerfile, non-root, git-SHA ENTRYPOINT-tagged — INFRA-08
- [x] `SecretsResolver` resolves credentials through opaque `env://`/`file://` references; no code path names Vault or Kubernetes Secrets — the Kubernetes-Secrets→Vault swap in Phase 5 becomes a configuration change — SEC-15
- [x] Files stream through one `csv.reader` over a `newline=""` wrapper, chunked in records via `itertools.batched` — embedded LF/CRLF fields survive at chunk sizes 1/2/3, proven by unit and hypothesis property tests — CSV-13
- [x] Dataset processing configuration is versioned and canonically hashed (`ConfigRegistry`); every run records which config version produced it — SCHEMA-07
- [x] Structured JSON (in-cluster) / console (local) logging works identically in local, Docker, Kubernetes and Airflow task-pod contexts; contextual fields propagate via contextvars without re-passing at each call site; a credential resolved by `SecretsResolver` never appears in a captured log line — OBS-02, OBS-04, OBS-05
- [x] Domain exception hierarchy (`DataPlatformError` + 3 leaves) for run-fatal conditions; row-level problems flow as values (`RejectedRecord`/`StageResult`), never exceptions — QUAL-03

**Vertical Slice — CSV to Analytical PostgreSQL** (Phase 4, 2026-08-14)

- [x] A TaskFlow DAG (`@dag`/`@task`) triggers a `KubernetesPodOperator` pod that loads a real CSV into analytical PostgreSQL and returns a ≤4KB receipt via XCom; DAG file stays under 150 lines with zero parsing/validation/typing/DB-writes — ORCH-01, ORCH-02, ORCH-06
- [x] Bounded Dynamic Task Mapping, explicit retry/backfill posture, logical-date/data-interval-derived windows (tolerates `None`), dataset dependency via a deferrable `S3KeySensor`, per-task CPU/memory requests+limits — ORCH-03, ORCH-04, ORCH-05, ORCH-07, ORCH-09
- [x] Frozen-manifest discovery — list once, hash once, freeze once, never re-derive from a live listing mid-run — ORCH-08
- [x] Re-running the same DAG run, and re-uploading the same file under a different name, both produce zero additional rows — proven live at 10M-row scale (`normalized.customers`: 10,000,122 rows = 10,000,122 distinct customers across 13 real ingestion runs, including deliberate pod-kills and concurrent-reads-during-publish) — LOAD-01, LOAD-02, LOAD-03
- [x] File / batch / record / target-row identity modeled distinctly; every loaded row traceable to its file/batch/run/attempt/config-version by SQL alone — LOAD-04, LOAD-08
- [x] Transactional staging (`UNLOGGED`, `ON COMMIT DROP`) → single-writer atomic publication via `pg_advisory_xact_lock` + `INSERT ... ON CONFLICT`, deliberately never `MERGE` (PG BUG #18279) — LOAD-05, LOAD-09
- [x] `csv_processor` is the only CSV parser; no `COPY ... FORMAT csv` on raw input — LOAD-12
- [x] Business date derivation never reads wall-clock or `logical_date` (positive extraction from filenames/content deferred to Phase 6) — INCR-08
- [x] Integration tests (MinIO → processor → PostgreSQL) and E2E tests (CSV → MinIO → Airflow → Kubernetes → processor → PostgreSQL) proven against a live kind cluster; idempotency tested including a zero-additional-rows assertion — QUAL-05, QUAL-06, QUAL-09
- [x] U1 (XCom carries the built git SHA) and U3 (streaming throughput 41,946 rows/sec, peak RSS 62.9 MiB) spike results recorded in `docs/spikes/`
- [x] Run-lifecycle integrity hardened after live-confirmed gap closure: a `SUCCEEDED` run's status can never regress to `RUNNING` (heartbeat write now guarded `WHERE status = 'RUNNING'`); duplicate-content file resolution is deterministic (`ORDER BY file_id ASC`, restoring a live-orphaned row); a `Receipt` is written to XCom on every `ingest()` exit path, not only `DataPlatformError` — META-03

**Vault Secrets & Workload Identity** (Phase 5, 2026-08-14)

- [x] Vault deployed as a persistent, non-dev StatefulSet in both Helm values profiles; scripted unseal restores service after a restart with no data loss — INFRA-06
- [x] `csv-processor` and Airflow authenticate to Vault via direct ServiceAccount-token Kubernetes-auth login (no Agent Injector/CSI/VSO sidecar); an unauthorized ServiceAccount is provably denied another workload's secrets — SEC-06, SEC-07, SEC-12
- [x] Airflow's native `VaultBackend` serves the `minio_default` connection; DAGs still resolve and run with the connection deleted from the metadata DB and every `AIRFLOW_CONN_*` unset — SEC-05
- [x] All three Phase-4-era Kubernetes Secrets (`csv-processor-db`, `csv-processor-s3`, `airflow-minio-connection`) retired; a permanent policy test guards against any of the three ever reappearing as a Secret-creation target — SEC-01, SEC-03, SEC-04
- [x] Vault's audit log shows which workload read which path, when, and whether it succeeded, with no plaintext secret values present — SEC-08
- [x] Rotating a credential in Vault is observed by Airflow's already-running process on its next read, no restart required — SEC-09
- [x] Development secrets are marked, isolated from production, and reproducible on a genuinely fresh local rebuild — proven via a scoped Vault-release-and-PVC reinstall against a real empty Vault (16/16 e2e tests, a real DAG run reaching `SUCCEEDED` on freshly-generated credentials) plus an offline regression guard — SEC-13
- [x] Secrets architecture documented end-to-end (injection mechanism, trust boundaries, what's-where, rotation, audit, production substitution), including ADR-0009 (Vault's BUSL-1.1 licence, OpenBao named as the OSI-licensed migration target) — SEC-14

Validated in Phase 5: INFRA-06, SEC-01, SEC-03, SEC-04, SEC-05, SEC-06, SEC-07, SEC-08, SEC-09, SEC-12,
SEC-13, SEC-14 — 12/12.

**Validation, Quarantine & Metadata Control-Plane Completion** (Phase 8, 2026-08-18)

- [x] Structural validation reports expected vs actual column count, malformed rows, unclosed quotes and missing delimiters with row number, column, error type, run and timestamp — nothing silently discarded — VALID-01
- [x] Data-quality rules (completeness, uniqueness, validity ranges, patterns, referential integrity) each declare a configurable per-rule-type bad-record strategy, producing PASS / PASS_WITH_WARNING / FAIL / QUARANTINE — VALID-02, VALID-03
- [x] Machine-readable validation reports exist as rows in PostgreSQL (`meta.validation_results`) and as an artifact in MinIO — VALID-04
- [x] Referential integrity between `orders`→`customers` validated live end-to-end against a real second dataset and a real orphan scenario, configurable `fail`/`quarantine`/`warn` — VALID-07
- [x] Quarantined data has a genuinely working, live-proven re-drive path: a corrected file's Airflow backfill re-execution resolves its original PENDING reject via business-key-scoped resolution (`resolve_rejected_records_for_business_keys`, migration 0020) — not merely designed but proven twice against the real cluster after a first live-verification pass found and a gap-closure round (`/gsd:plan-phase 8 --gaps`) fixed the original batch_id-scoped design (D-23/D-24/D-25) — VALID-08
- [x] Volume anomalies (10× historical baseline) flagged against persisted statistics, no ML — VALID-09
- [x] File integrity (checksum, size, extension, object-stability, `_BATCH_COMPLETE` manifest) verified Airflow-side before any pod launches — LOAD-10, LOAD-11
- [x] Same-round code review found and fixed a genuine correctness bug (CR-01): resolution was scoped to what a run *staged*, not what its publish statement actually wrote — a business key whose `ON CONFLICT` conflict-guard silently no-op'd could still be marked resolved. Fixed via `RETURNING` + `PublishResult.published_business_keys`, proven by a regression test confirmed to fail pre-fix and pass post-fix, then re-verified live.

Validated in Phase 8: VALID-01, VALID-02, VALID-03, VALID-04, VALID-07, VALID-08, VALID-09, LOAD-10,
LOAD-11 — 9/9.

### Active

<!-- Current scope. All are hypotheses until shipped. Detailed REQ-IDs live in REQUIREMENTS.md. -->

**Infrastructure**

- [ ] Analytical PostgreSQL with staging / warehouse / analytics separation

**Universal CSV Engine**

- [ ] Configurable filename mask/regex parsing (dataset, source, country, business date, version, batch, sequence)
- [ ] Encoding detection with confidence scores (UTF-8, BOM, UTF-16 LE/BE, Windows-1250/1252, ISO-8859, ASCII)
- [ ] CSV dialect detection — delimiter, quote, escape, line ending, quoting and whitespace behavior
- [ ] Correct handling of quoted delimiters, escaped quotes, multiline fields, inconsistent quoting
- [ ] Header/metadata/footer detection — header absent, at a later row, preceded by metadata, followed by totals
- [ ] Conservative schema inference plus explicit YAML data contracts
- [ ] Schema versioning, schema hashing, and compatible-vs-breaking evolution policy per dataset
- [ ] Historical files processable under their historical schema version
- [ ] Normalization of dates, numerics, booleans, NULLs and whitespace with locale awareness
- [ ] Streaming/chunked processing of files larger than container memory, with bounded memory

**Data Quality & Validation**

- [ ] Source-to-target reconciliation and control-total validation against loaded target (VALID-05, VALID-06 — Phase 9)

**ETL Correctness**

- [ ] All ETL idempotent across retries, pod restarts, reruns, backfills and re-uploaded files (retries/pod-restarts/reruns/re-uploads proven at 10M-row live scale in Phase 4 — LOAD-01, LOAD-02, LOAD-03; backfill execution itself remains unproven)
- [ ] Dataset-specific deduplication (exact-hash, business key, key+timestamp, latest-wins, source-priority, batch-aware), within and across batches
- [ ] Deduplication auditable — counts and reasons retained
- [ ] Incremental processing with watermarks that advance only after successful commit
- [ ] Late-arriving and out-of-order data routed to the correct historical partition, never discarded
- [ ] Backfills as a first-class capability using the same pipeline, no bypass path
- [ ] Partial-failure recovery determinable without reading logs; checkpointing for very large files
- [ ] Source-to-target reconciliation and control-total validation
- [ ] Full replayability from immutable raw data plus versioned configuration

**Warehouse / Historical**

- [ ] SCD Type 0, 1 and 2 with surrogate keys distinct from business keys
- [ ] Deterministic hash-based change detection; repeated identical events create one logical version
- [ ] Effective dating that distinguishes source/business/event/ingestion/processing time
- [ ] Late-arriving SCD corrections that repair historical validity intervals
- [ ] CDC framework (INSERT/UPDATE/DELETE) with ordering by sequence/offset/transaction ID
- [ ] Documented delivery semantics — no unearned exactly-once claims
- [ ] CDC feeding SCD processing; SCD processing idempotent and backfill-safe

**Observability**

- [ ] Lineage and run metadata answering "where did this data come from?" via SQL
- [ ] Structured contextual logging at correct levels, no `print()`, no secrets or PII
- [ ] Prometheus + Grafana in-cluster with dashboards for the §82 metric set
- [ ] OpenTelemetry distributed tracing across Airflow task → pod → processor → PostgreSQL
- [ ] Data freshness tracking against expected frequency

**Engineering & CI/CD**

- [ ] Reusable typed `csv_processor` Python package with domain-specific exception hierarchy
- [ ] Unit, integration, E2E, regression and property-based tests
- [ ] Comprehensive CSV edge-case fixture corpus, grown as cases are found
- [ ] Explicit failure-scenario tests (pod crash, DB/MinIO/Vault down, OOM, duplicate batch, CDC ordering, secret rotation)
- [ ] GitHub Actions: lint, type check, tests, coverage, image build, manifest/Helm validation, security and secret scanning
- [ ] Ephemeral kind cluster in CI running full E2E, proving the environment rebuilds from the repo
- [ ] Operational runbooks with symptoms, diagnosis, recovery, reprocessing and verification

### Out of Scope

- **Cloud deployment (AWS/GCP/Azure)** — local kind is the target; the architecture keeps swap-out paths open (MinIO→S3, Vault→cloud KMS) but porting is not this milestone
- **dbt as an end-to-end (bronze→gold) transformation framework** — as of Phase 08.1 (ADR-0010), dbt owns bronze-to-silver transformation narrowly; gold publish (`MergePublisher`'s `INSERT ... ON CONFLICT`, avoiding PG BUG #18279) and SCD2 remain explicitly Python-owned and out of dbt's scope, so this exclusion is narrowed, not lifted
- **Streaming ingestion (Kafka/Kinesis)** — CDC is supported *architecturally* as a pluggable source, but no streaming broker is deployed
- **Real production datasets or PII** — corpus is fully synthetic, so fixtures are committable and reproducible
- **Docker Compose as a workload platform** — explicitly forbidden by README §3.1; Compose may only appear as a developer convenience for isolated unit-test dependencies
- **ML-based anomaly detection** — README §53 mandates simple configurable statistical thresholds first
- **Multi-tenancy / user-facing UI** — Airflow's own UI is the only interface; no bespoke frontend
- **Supporting every theoretical CSV variant** — README §7 sets the bar at a robust extensible framework for a broad range of *real-world* inputs

## Context

**Source of truth.** `README.md` is a 3,386-line master specification with 95 numbered sections, a 10-stage development roadmap (§92), and a 114-item Definition of Done (§94). It is unusually complete — this project is an execution problem, not a discovery problem. Section numbers are the shared vocabulary; requirements trace back to them.

**Airflow version.** The architecture diagram shows API Server + DAG Processor + Triggerer as separate components and references "Airflow Assets/Datasets" — both Airflow 3.x signals. README also directs using most-recent stable versions. Exact versions to be pinned during research via Context7.

**Sequencing.** README §93 is emphatic: build the vertical slice first (CSV → MinIO → TaskFlow DAG → K8s pod → processor → analytical PostgreSQL), then layer complexity. The full 114-item DoD is the milestone target, but the roadmap must reach a working end-to-end pipeline early rather than building horizontal layers.

**Environment (measured 2026-08-11).** WSL2 on Linux 6.18. 32 CPUs, 47 GB RAM, 16 GB swap. Docker 29.7.2 with 50 GB available to the daemon. kubectl v1.36.1, Python 3.12.3, uv 0.8.11, Poetry 1.8.2, Node 24.14.1, gh 2.45.0. **kind and helm are not yet installed** — Phase 1 work. Resources are ample for a 3-node kind cluster running the full stack.

**Repository relocated.** The repo was moved from `/mnt/c/Users/borow/VSC/projects/airflow-platform` to `/home/user/projects/airflow-platform` during initialization. `/mnt/c` is a 9p mount, not a real disk. Measured penalty: creating 2,000 small files took 3.01 s vs 0.06 s on ext4; stat+read took 4.76 s vs 0.08 s; sequential write 211 MB/s vs 3.8 GB/s. The cost is per-syscall, so SSD hardware does not mitigate it. This would have degraded pytest collection, uv installs, Docker build contexts and Airflow's DAG-parsing loop for the project's whole life. Open via VS Code Remote-WSL. A pointer file remains at the old path.

**Prior work.** No spikes or sketches. Greenfield: one commit, README only.

## Constraints

- **Platform**: Local multi-node kind cluster (control-plane + 2 workers) — README §3.1 mandates it and forbids Docker Compose as the workload platform. Reason: the local environment must resemble production Kubernetes.
- **Database topology**: Two physically separate PostgreSQL deployments — Airflow metadata must never host analytical data (§4). Separation stays visible even inside one cluster.
- **Storage access**: Applications address data as `s3://bucket/path`, never local filesystem paths (§5), so MinIO can be replaced by S3 without code changes.
- **Raw immutability**: The raw layer is append-only. Corrections arrive as new files/versions/reprocessing events, never overwrites (§63).
- **Logic placement**: Business logic lives in the `csv_processor` package. DAGs orchestrate and delegate (§6.4, §68). Heavy processing runs in task pods, never the scheduler.
- **Secrets**: No credential may exist in Git, Python source, Dockerfiles, Kubernetes manifests, Airflow Variables or CI workflow files (§81). Runtime injection only.
- **Determinism**: Same source data + configuration + processor version yields the same logical result. Uncontrolled dependence on wall-clock time, randomness or filesystem ordering is disallowed; unavoidable non-determinism must be documented (§67).
- **Deployment style**: Upstream Helm charts (pinned, with committed values files) for Airflow, MinIO, Vault, Postgres and the monitoring stack. Engineering effort concentrates on the ETL library and platform glue, not on re-implementing chart logic.
- **CI runner sizing**: GitHub-hosted runners are 4 CPU / 16 GB — too small for the full local stack. Helm values must be profile-parameterized from the start: a trimmed single-node CI profile (monitoring disabled, minimal replicas) for ephemeral-kind E2E, and the full multi-node profile locally. Retrofitting this later is expensive.
- **Filesystem**: Repo must stay on WSL ext4. Do not hostPath-mount `dags/` from `/mnt/c` into kind — the DAG processor's periodic re-stat loop over 9p would be pathological.
- **No secrets in fixtures**: The CSV corpus is synthetic by construction, so it is safe to commit and fully reproducible.

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Full 114-item DoD as one milestone, sequenced vertical-slice-first | User wants the complete platform, but §93 forbids breadth-first. Roadmap reaches a working E2E pipeline early, then layers capability | — Pending |
| Foundation for real work, not a portfolio piece | Prioritizes operational robustness, runbooks and clean swap-out paths over demonstrable breadth | — Pending |
| Upstream Helm charts for infrastructure; hand-rolled ETL library | The README's genuine depth is in ETL correctness. Re-implementing Airflow 3's Kubernetes wiring by hand adds risk without adding insight | — Pending |
| Synthetic-only CSV corpus | Committable, reproducible, PII-free, and edge cases can be constructed deliberately rather than waited for | — Pending |
| Prometheus + Grafana + OpenTelemetry tracing | User chose the most complete observability tier. Largest optional addition in the project; justified by "foundation for real work" | — Pending |
| Ephemeral kind cluster in GitHub Actions for E2E | Only way CI can prove §113 "environment can be recreated from the repository". Requires the trimmed CI cluster profile above | — Pending |
| Repository moved to WSL ext4 | Measured 50–60× penalty on small-file operations over the 9p `/mnt/c` mount; compounds across pytest, uv, Docker builds and DAG parsing | ✓ Good |
| dbt scoped to bronze→silver only (superseded 08.1) | dbt's `merge` incremental strategy compiles to literal PostgreSQL `MERGE`, the same concurrency hazard (PG BUG #18279) `MergePublisher` was built to avoid; dbt's own per-model transactions cannot participate in META-03's single-transaction publish guarantee. Gold and SCD2 stay Python-owned | ✓ Good (ADR-0010) |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---

## Current State

**Phase 8 complete (2026-08-18)** — Validation, Quarantine & Metadata Control-Plane Completion.

Persisted validation-rule engine (structural/quality/referential, each with a configurable
per-rule-type bad-record strategy), machine-readable reports in PostgreSQL + MinIO, a real
second dataset (`orders`→`customers`) proving referential integrity live, and an Airflow-side
pre-pod-launch file-integrity gate — all 18 plans across 11 waves (14 original + 3 gap-closure),
live-verified against the real cluster. The standout story: the first verification pass (4/5
roadmap criteria) found VALID-08's re-drive path genuinely FAILING live — `resolve_rejected_
records_for_batch` scoped strictly by `batch_id`, but a content-differing correction always
discovers under a new `batch_id`, so it could never resolve the original PENDING reject. A
gap-closure round (`/gsd:plan-phase 8 --gaps`, plans 08-16/08-17/08-18) redesigned resolution as
business-key-scoped (`resolve_rejected_records_for_business_keys`, migration 0020, D-23/D-24/D-25)
and proved it live — `test_backfill_reentry.py -m cluster` passed against the real cluster. The
same round's own code review then found and fixed a second, related correctness bug (CR-01):
resolution was reading what a run *staged*, not what its publish statement actually wrote,
so a conflict-guard-blocked "locked but unchanged" row could still be marked resolved with
nothing published. Fixed via `RETURNING` + `PublishResult.published_business_keys`, proven by a
regression test confirmed to fail pre-fix/pass post-fix, then re-verified live a second time.
Re-verification: 5/5 roadmap success criteria VERIFIED, all 9 requirement IDs satisfied.

**Phase 7 complete (2026-08-16)** — Observability, Metrics, Tracing & Lineage.

Grafana dashboard (8 metrics + 3 live gauges), bounded-cardinality OTel metrics/traces
via OTel Collector + Tempo, two-tier freshness alerting to a Vault-backed webhook, and
`meta.v_customers_lineage` for SQL-queryable lineage — all live-verified against the
real cluster. Gap closure (07-09) wired `dag_id`/`dag_run_id`/`task_id` end-to-end
(mirroring the TRACEPARENT pod-boundary mechanism from 07-04), proven correct via
real-Postgres integration tests and a byte-identical live code deployment. One item
remains open under a developer-accepted override: live confirmation that a genuinely
Airflow-triggered row shows non-NULL dag/run/task ID is blocked by an unrelated,
independently-confirmed Airflow KubernetesExecutor scheduling defect (tasks stuck
`queued`/`up_for_retry` indefinitely, reproducing since before this phase's own work
began) — tracked in STATE.md's Blockers/Concerns, needs its own `/gsd:debug` session.

**Phase 6 complete (2026-08-15)** — Universal CSV Engine, Schema Contracts & Normalization.

The full detection → normalization → schema-versioning pipeline is real and wired
end-to-end: 5 detectors (filename, encoding, dialect, header/footer, schema-type
inference), compression + multi-part delivery, 4 value-level normalizers (dates,
numeric, boolean/null, Unicode NFC), and schema versioning/evolution classification,
all built as 18 plans across 5 waves (11-plan Wave 2 was the phase's parallelization
peak) then merged and integration-tested together. A code review after Wave 5 found
and the orchestrator fixed a genuine blocker: `CsvSource._resolve_schema` silently
accepted a column-reordered CSV as schema-compatible and staged its rows into the
wrong target columns, since `StagingLoader` maps by position only — now rejected with
a named `schema-columns-reordered` diagnostic before any row stages. Phase
verification then found and the orchestrator fixed two more gaps: the resolved
`schema_version_id` was computed but never persisted to `meta.ingestion_runs` (now
wired through `StagingResult`/`finalize_publication`), and three named encodings
(Windows-1252, ISO-8859, UTF-16 BE) had zero test coverage (now covered — including
the discovery that blind statistical detection genuinely cannot disambiguate
Windows-1252 from near-identical codepages at short sample sizes, itself now pinned
as a real, asserted detector characteristic).

**Phase 5 complete (2026-08-14)** — Vault Secrets & Workload Identity.

Vault is now the only source of runtime credentials for this platform, and workload
identity is real: both `csv-processor` (via a `vault://` scheme in `SecretsResolver`)
and Airflow (via its native `VaultBackend`) authenticate with direct ServiceAccount-
token Kubernetes-auth logins, an unauthorized ServiceAccount is automatically-tested
denied, and all three Phase-4-era Kubernetes Secrets are retired with a permanent
regression guard against their return. Built across 6 plans — 5 in sequence (each
wave depending on the identity/wiring the previous one proved live), plus one
gap-closure plan (05-06) after the first verification pass found a real, code-
confirmed blocker.

That gap-closure plan is the notable story of this phase: `05-VERIFICATION.md`'s
first pass found `scripts/vault-bootstrap.py`'s credential-sourcing functions still
depended on three Kubernetes Secrets this same phase's own earlier plans had already
deleted — meaning a genuinely fresh cluster rebuild would have left Vault
structurally bootstrapped but with **zero credential values ever written**, directly
contradicting SEC-01. Plan 05-06 fixed the root cause (restoring the original,
least-privileged `etl_app`-scoped credential mechanism from git history, rejecting
a reviewer-suggested fix that would have silently widened database privilege) and
set out to prove it live. That live proof — a scoped Vault-release-and-PVC reinstall
against a genuinely empty Vault — surfaced a second, entirely unrelated infrastructure
fault: a Docker Desktop/WSL2-level restart had broken the DAGs `hostPath` mount on
all 3 kind nodes, silently freezing Airflow's scheduler *for every DAG, cluster-wide*
(not scoped to this phase's work at all). A dedicated debug session root-caused and
fixed it (`.planning/debug/resolved/dagrun-scheduler-stall.md`). A second, independent
verification pass then re-derived every claim from first principles rather than
trusting the gap-closure plan's own SUMMARY — re-running the live e2e Vault suite
fresh (16/16, up from an initially-reported 15/16, confirming the one prior failure
really was self-resolving backlog depth as claimed) and independently querying the
live analytical database for the cited `SUCCEEDED` ingestion row.

Validated in Phase 5: INFRA-06, SEC-01, SEC-03, SEC-04, SEC-05, SEC-06, SEC-07, SEC-08,
SEC-09, SEC-12, SEC-13, SEC-14 — 12/12.

Standing facts later phases inherit:
- **An idempotent bootstrap's credential-sourcing function must source from the same
  live system its predecessor did — never a coincidentally-similar but
  differently-privileged one.** The gap-closure fix restored `etl_app`'s original
  `kubectl exec`+`ALTER ROLE` mechanism rather than following a code-review
  suggestion to read CNPG's own `analytics-db-app` Secret, which holds the
  more-privileged `analytics_owner` role — that shortcut would have been a real,
  easy-to-miss privilege escalation dressed up as a bug fix.
- **A WSL2/Docker Desktop restart or suspend/resume can silently break kind's
  hostPath DAG mount and freeze Airflow scheduling entirely, with zero exceptions
  logged.** Symptom: `DagModel.is_stale` never clears because the DAG processor's
  mount silently falls back to an empty tmpfs; the scheduler's own query then
  excludes every DagRun from ever being scheduled again. Fix is always `docker
  restart` on the affected kind node(s) — nothing inside Kubernetes/Airflow can
  force the reattachment. Fast diagnostic: `docker exec <node> mount | grep
  /mnt/dags`. Saved to project memory (`host_hardware_context.md`) since this is a
  host-level risk, not phase-specific.
- **A live-cluster proof step can surface bugs entirely unrelated to what it's
  proving — don't let the unrelated finding block or get conflated with the actual
  result.** The scheduler stall had nothing to do with Vault/credentials, but
  discovering it required distinguishing "the thing I'm testing is broken" from "the
  environment I'm testing it in just broke" — the same discipline this phase's own
  citation-driven documentation already enforces elsewhere.

---

**Phase 4 complete (2026-08-14)** — Vertical Slice: CSV to Analytical PostgreSQL.

One real CSV now travels the full, unattended path this milestone's critical
path exists to prove: `s3://raw/` → TaskFlow DAG → `KubernetesPodOperator` →
`dataplat`/`csv_processor` → analytical PostgreSQL, live on the actual kind
cluster, not just in tests. Phase verification ran direct `kubectl
exec`/`psql` queries against the live `analytics-db` and live Airflow
deployment rather than trusting SUMMARY.md claims, and found the vertical
slice genuinely solid at scale — 10,000,122 rows in `normalized.customers`
matching 10,000,122 distinct customers across 13 real ingestion runs — but
also found 3 precise, code-confirmed defects (a heartbeat race that could
silently revert a `SUCCEEDED` run back to `RUNNING`; a non-deterministic
duplicate-file lookup that had already orphaned one real row live; a
Receipt-on-every-exit-path contract violated for non-`DataPlatformError`
exceptions), all three already caught by a same-day code review
(`04-REVIEW.md`, CR-01/CR-02/WR-01). Two gap-closure plans (04-10, 04-11)
closed all three — independently re-verified true at the code, test, AND
live-cluster-data level by a second verification pass, which additionally
caught and closed a deployment-currency gap the fix authors themselves
missed (the parallel-worktree image rebuild only carried one of the two
plans' commits; `make image-csv-processor` was re-run from the merged `main`
to bake in both).

Validated in Phase 4: ORCH-01 through ORCH-09, META-03, LOAD-01, LOAD-02,
LOAD-03, LOAD-04, LOAD-05, LOAD-08, LOAD-09, LOAD-12, INCR-08, QUAL-05,
QUAL-06, QUAL-09 — 22/22.

Standing facts later phases inherit:
- **A guarded, narrower sibling method beats adding a `WHERE` clause to a
  shared setter.** `heartbeat_ingestion_run` (new, `WHERE run_id = %s AND
  status = 'RUNNING'`) is deliberately separate from
  `update_ingestion_run_status` (unchanged, unconditional) — periodic
  background writers that must never regress a terminal state get their own
  narrow method; one-shot intentional status transitions keep the generic
  one. Phase 9's CDC/SCD status machinery should follow the same split if it
  grows a similar periodic-write path.
- **`LIMIT 1` with no `ORDER BY` is a live landmine, not a theoretical one.**
  `find_file_by_content_hash`'s missing `ORDER BY file_id ASC` produced a
  real orphaned row in the running cluster before this phase closed. Any
  future "resolve one canonical row from a group that can have 2+ members"
  query (dedup, SCD-original lookup, latest-wins resolution) needs an
  explicit, documented tie-break column from the start.
- **Two plans in the same wave that both touch a runtime-deployed image must
  not assume either one's rebuild covers the other's commit.** Parallel git
  worktrees branch from the same base and don't see each other's
  not-yet-merged commits; a rebuild step inside one plan's worktree can only
  ever bake in that worktree's own history. When a wave's plans jointly
  change code that ships in a container image, the rebuild belongs after the
  merge, not inside either plan.
- Known, deliberately-not-yet-fixed findings carried forward from
  `04-REVIEW.md` for a future phase: no code path ever writes
  `meta.ingestion_runs.status = 'FAILED'` (WR-02); `try_number` is always
  `1` since no KPO pod sets `AIRFLOW_TASK_TRY_NUMBER` (WR-03); a single
  bad-value row aborts an entire file's publish, explicitly scoped as
  later-phase quarantine work (WR-04); `DatasetConfig.dataset` reaches raw
  SQL/filesystem paths unvalidated (WR-05, WR-06); the new
  `scripts/repair-duplicate-file-lineage.py` only detects
  `duplicate_of_file_id IS NULL` (not an existing wrong non-`NULL` value)
  and assumes every dataset uses `duplicate_policy: skip` (WR-07, WR-08) —
  none currently exploitable, all tracked for the multi-dataset phase that
  first stresses them.

---

**Phase 3 complete (2026-08-13)** — `dataplat` Core Library & Metadata Control Plane.

The metadata control plane and pipeline engine now exist as one coherent,
testable Python library — proven entirely with testcontainers (PostgreSQL 18
+ MinIO), no Kubernetes cluster required. Built across 8 plans in 5 waves,
each executed in an isolated git worktree and merged back; a repo-wide scan
found zero duplicate definitions between `dataplat` and `csv_processor` from
the parallel execution. All 5 ROADMAP success criteria were independently
re-verified live (not just read from SUMMARY.md claims): `alembic upgrade
head` builds the whole `meta`/`normalized` schema with `hash_version`
columns on every stored hash; the full test suite (99 tests, including a
hypothesis property test) passes against testcontainers alone;
`docker run csv-processor:<git-sha> dataplat --version` prints the version
from a non-root, git-SHA-tagged image; `SecretsResolver` resolves opaque
`env://`/`file://` references with no code path naming Vault or Kubernetes
Secrets; every log line is structured JSON with contextvars-propagated
fields, and a credential passed through the resolver never appears in a
captured log line.

Validated in Phase 3: META-01, META-02, INFRA-08, SEC-15, CSV-13, SCHEMA-07,
OBS-02, OBS-04, OBS-05, QUAL-03 — 10/10.

Standing facts later phases inherit:
- `PipelineContext` composes `RunContext`/`DatasetConfig`/`MetadataRepository`/
  `ObjectStore`/a psycopg `ConnectionPool` into one frozen object — every
  future `Source`/`Publisher` implementation (Phase 4's `merge`, Phase 10's
  CDC/SCD) is written against this one settled contract, recorded permanently
  as ADR-0008.
- `csv_processor.source.CsvSource`/`CsvRecordStream` are the plugin's first
  real code: one `csv.reader` over a `newline=""` stream, chunked by record
  ordinal (never lines or byte offsets), hardcoded UTF-8/comma/header-row-0.
  Encoding and dialect detection are deliberately out of scope — Phase 6's
  `csv_processor/detect/` territory.
- A standard-depth code review across all 61 changed files found 3 Critical
  robustness defects worth triaging before Phase 4 builds on top, documented
  in `03-REVIEW.md` and independently reproduced by the phase verifier: the
  CLI crashes with a raw traceback on a bare/invalid invocation
  (`standalone_mode=False` bypasses click's own usage-error handling);
  `chunked_records()` raises an untyped `RuntimeError` on a genuinely empty
  CSV file (PEP 479); and `get_or_create_dataset`/`ConfigRegistry` have an
  unguarded TOCTOU race on a dataset's first-ever insert. None falsify a
  must-have — all are edge-case/robustness gaps, not missing functionality.
- `make test-integration` (testcontainers) could not be re-run by the
  orchestrator immediately post-merge because the local `docker` CLI's `info`
  subcommand was hanging — a client-side quirk following a host power event,
  not a daemon problem (the raw Docker Engine API and `docker ps` both worked
  correctly throughout). Each contributing plan had already proven its own
  integration tests green during isolated worktree execution.

---
*Last updated: 2026-08-18 after Phase 8*
