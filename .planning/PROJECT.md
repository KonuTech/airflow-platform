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

(None yet — greenfield. Ship to validate.)

### Active

<!-- Current scope. All are hypotheses until shipped. Detailed REQ-IDs live in REQUIREMENTS.md. -->

**Infrastructure**

- [ ] Reproducible multi-node kind cluster, destroyable and recreatable from the repo
- [ ] Airflow 3.x deployed via upstream Helm chart (API server, scheduler, DAG processor, triggerer)
- [ ] Dedicated Airflow metadata PostgreSQL, strictly separate from analytical PostgreSQL
- [ ] Analytical PostgreSQL with staging / warehouse / analytics separation
- [ ] MinIO providing S3-compatible object storage with `raw/validated/processed/quarantine/metadata` layers
- [ ] HashiCorp Vault deployed in-cluster as the secrets manager
- [ ] All infrastructure defined as code; containers reproducible, versioned, non-root

**Secrets & Security**

- [ ] Airflow resolves connections through an external Vault secrets backend
- [ ] Task pods authenticate to Vault via Kubernetes service-account identity, not a shared root token
- [ ] Least-privilege policies: each workload reaches only its own secrets
- [ ] Runtime secret injection — nothing secret in Git, Python, images, or manifests
- [ ] Secret access auditable; rotation documented; automated secret scanning in CI
- [ ] Tests prove unauthorized workloads are denied

**Orchestration**

- [ ] DAGs use the TaskFlow API (`@dag` / `@task`) and stay thin — orchestration only
- [ ] ETL executes in Kubernetes task pods via `KubernetesPodOperator`, not in the scheduler
- [ ] Dynamic Task Mapping fans work out across pods
- [ ] DAGs are idempotent, retry-safe, and driven by logical date / data interval — never wall-clock
- [ ] Dataset dependencies expressed in Airflow (assets / sensors), not hidden in Python
- [ ] Per-workload CPU/memory requests and limits; concurrency and pools configured

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

- [ ] Structural validation with row/column-level diagnostics
- [ ] File-level validation — size, checksum, extension, emptiness, row thresholds, expected arrival
- [ ] Data quality rules — completeness, uniqueness, validity ranges, patterns, referential integrity
- [ ] Configurable thresholds producing PASS / PASS_WITH_WARNING / FAIL / QUARANTINE
- [ ] Configurable bad-record strategies; rejected records retained with reason, never silently discarded
- [ ] Machine-readable validation reports persisted to MinIO and PostgreSQL
- [ ] Schema drift detection and volume/quality anomaly detection via statistical thresholds

**ETL Correctness**

- [ ] All ETL idempotent across retries, pod restarts, reruns, backfills and re-uploaded files
- [ ] File / batch / record / target-row identity explicitly distinguished
- [ ] Dataset-specific deduplication (exact-hash, business key, key+timestamp, latest-wins, source-priority, batch-aware), within and across batches
- [ ] Deduplication auditable — counts and reasons retained
- [ ] Incremental processing with watermarks that advance only after successful commit
- [ ] Late-arriving and out-of-order data routed to the correct historical partition, never discarded
- [ ] Backfills as a first-class capability using the same pipeline, no bypass path
- [ ] Transactional loading via staging tables and atomic publication — no partially visible datasets
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
- **dbt or an external transformation framework** — README §36/§54 model transformation and SCD logic in the Python layer; adding dbt would fork the transformation story. Revisit if the warehouse layer outgrows Python
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
| dbt excluded | README models transformation and SCD in Python (§36, §54–61). Introducing dbt would split the transformation story across two paradigms | — Pending |

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

**Phase 1 complete (2026-08-11)** — Repository, Toolchain & CI Skeleton.

Every commit is now gated by ruff, mypy strict, pytest and gitleaks, and the
gate is *enforcing* rather than advisory: branch protection on `main` requires
both CI jobs, and a pull request carrying a deliberate `print()` was observed
being refused (`mergeable_state: blocked`), not merely marked red.

Validated in Phase 1: QUAL-01, QUAL-02, QUAL-07, QUAL-08, CICD-01, CICD-02,
CICD-03, CICD-04, SEC-02, SEC-10, SEC-11, OBS-03 — 12/12.

Standing facts later phases inherit:
- `make` is the only gate definition; CI calls it and nothing else, so the local
  and CI gates cannot drift. `make check` is offline; `make ci` adds the secret
  scan.
- The 69-declaration CSV corpus regenerates byte-identically from a seed and is
  never committed. It is the specification Phase 6's engine is measured against.
- `enforce_admins: false` is deliberate — GSD commits directly to `main`, so
  admin enforcement would self-lockout the project (T-01-43b).
- `.planning/WINDOWS.md` carries 5 open defects, none blocking. Phase 3 owes
  wiring `tests/property`/`integration`/`e2e` into a target that can run them:
  `make check` names test paths explicitly, so a new test directory is silently
  uncollected until named. That exact defect made QUAL-07 partial.

Phase 1 found five defects *after* all nine plans reported success — four of
them gates that passed on broken input. Review and verification earned their
place as steps separate from execution.

---
*Last updated: 2026-08-11 after Phase 1*
