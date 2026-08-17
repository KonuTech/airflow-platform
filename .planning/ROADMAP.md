# Roadmap: Airflow ETL Platform

## Overview

The platform is built as a narrow **vertical slice first**, then widened. A CI skeleton and a
reproducible toolchain land on day one so every later commit is gated. Infrastructure (kind, MinIO,
two CloudNativePG clusters, Airflow 3.3) and the pure-Python `dataplat`/`csv_processor` library are
built on two parallel tracks that share no files. They meet at a deliberately trivial
Airflow↔Kubernetes smoke test, and then at the vertical slice — one UTF-8 comma CSV travelling
`CSV → MinIO → TaskFlow DAG → KubernetesPodOperator → processor → analytical PostgreSQL` — which
closes only when a re-run produces **zero additional rows**. Identity, the batch ledger and content
hashing live *inside* that slice, not after it, because Airflow retries are on by default and every
phase built on a non-idempotent loader silently duplicates data. From there the platform widens on
mostly-parallel tracks: Vault behind the `SecretsResolver` seam, the universal CSV engine, the
observability stack, validation and quarantine, ETL correctness (dedup, watermarks, backfills,
recovery, reconciliation), then CDC and SCD — the hardest correctness work, deliberately last but
one. It finishes with an ephemeral-kind E2E pipeline proving the whole environment rebuilds from the
repository, and runbooks written against failure modes that were actually observed.

This structure follows the consolidated research adjudication in `.planning/research/SUMMARY.md`
(stages S0–S14) rather than README §92, which four research documents independently found wrong in
five expensive ways. The five preserved deviations are: idempotency inside the slice (D1); the
metadata control plane designed coherently up front (D2); Vault after the slice (D3); CI skeleton
first (D4); observability as an explicit stage (D5).

## Phases

**Phase Numbering:**

- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Repository, Toolchain & CI Skeleton** - Gated repo from day one: uv, ruff, mypy, pytest, gitleaks in GitHub Actions, plus a seed-generated CSV fixture corpus (completed 2026-08-11)
- [x] **Phase 2: kind Cluster & Core Infrastructure** - A destroyable/recreatable 3-node kind cluster running MinIO, two separate PostgreSQL clusters and Airflow 3.3, all from committed files (completed 2026-08-12)
- [x] **Phase 3: `dataplat` Core Library & Metadata Control Plane** - The `meta` schema, pipeline engine, `SecretsResolver` and record-chunked CSV reader, tested with testcontainers and no cluster (completed 2026-08-13)
- [x] **Phase 4: Vertical Slice — CSV to Analytical PostgreSQL** - The end-to-end pipeline closes, idempotent by construction; a re-run produces zero additional rows (completed 2026-08-13)
- [x] **Phase 5: Vault Secrets & Workload Identity** - Vault becomes the only source of runtime credentials, with positive and negative service-account identity tests (completed 2026-08-14)
- [x] **Phase 6: Universal CSV Engine, Schema Contracts & Normalization** - Real-world messy CSVs parse correctly: encoding, dialect, header/footer, inference, contracts, versioning and locale-aware normalization (completed 2026-08-15)
- [x] **Phase 7: Observability, Metrics, Tracing & Lineage** - Prometheus/Grafana, OTel traces across the pod boundary, SQL-queryable lineage and freshness tracking (completed 2026-08-16)
- [ ] **Phase 8: Validation, Quarantine & Metadata Control-Plane Completion** - Nothing is silently dropped: structural and quality validation, quarantine with a re-drive path, machine-readable reports, anomaly detection
- [ ] **Phase 9: ETL Correctness — Dedup, Incremental, Backfill & Recovery** - Deduplication with audit, committed-cursor watermarks, first-class backfills, partial-failure recovery and reconciliation
- [ ] **Phase 10: CDC & Slowly Changing Dimensions** - SCD 0/1/2 with database-enforced non-overlapping history, late-arriving corrections by history recomputation, and a CDC `Source` with an ordering barrier
- [ ] **Phase 11: CI/CD Completion & Operations** - Ephemeral kind E2E in CI proving the environment rebuilds from the repo, plus runbooks, retention and rebuild-from-raw

## Phase Details

### Phase 1: Repository, Toolchain & CI Skeleton

**Goal**: Every future commit is gated by lint, type checking, unit tests and secret scanning, and the CSV fixture corpus that specifies the engine is reproducible from a seed
**Mode:** mvp
**Depends on**: Nothing (first phase)
**Requirements**: QUAL-01, QUAL-02, QUAL-07, QUAL-08, CICD-01, CICD-02, CICD-03, CICD-04, SEC-02, SEC-10, SEC-11, OBS-03
**Success Criteria** (what must be TRUE):

  1. Opening a pull request runs ruff, mypy, unit tests and gitleaks automatically, and a commit containing a fake credential fails the build.
  2. `make fixtures` regenerates the entire CSV edge-case corpus byte-identically from a recorded seed on a clean checkout — no corpus files are committed en masse.
  3. Adding a `print()` to library code, an untyped public function, or an undocumented public API fails CI.
  4. A developer clones the repo and runs `uv sync && make check` successfully with no cluster, no credentials and no network services.
  5. A scan of full git history reports zero secrets, and no CI job echoes a secret value into its log.

**Plans**: 9/9 plans executed in 7 waves

Plans:
**Wave 1**

- [x] 01-01-PLAN.md — Tracer: one commit through the whole gate (uv workspace, Makefile, CI quality-gate job)

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 01-02-PLAN.md — Secret scanning: scoped allowlists, full-history job, and the negative proof
- [x] 01-03-PLAN.md — Corpus generator: determinism framework and the committed digest oracle
- [x] 01-04-PLAN.md — ADRs 0001–0005 and the regression-test policy

**Wave 3** *(blocked on Wave 2 completion)*

- [x] 01-05-PLAN.md — Policy tests: every gate observed to fail, CI/local parity, gate-strength drift

**Wave 4** *(blocked on Wave 3 completion)*

- [x] 01-06-PLAN.md — Fixture corpus I: byte-level-hard (16 fixtures, 21 of 69 cumulative)

**Wave 5** *(blocked on Wave 4 completion)*

- [x] 01-07-PLAN.md — Fixture corpus II: structural, dialect and header (31 fixtures, 52 of 69)

**Wave 6** *(blocked on Wave 5 completion)*

- [x] 01-08-PLAN.md — Fixture corpus III: semantic and type damage (17 fixtures, 69 of 69 — complete)

**Wave 7** *(blocked on Wave 6 completion)*

- [x] 01-09-PLAN.md — Branch protection and end-to-end CI acceptance

**Cross-cutting constraints:**

- Adding these fixtures leaves every previously-committed digest line unchanged

**Research stage**: S0. **Skip `--research-phase`** — STACK.md pins ruff `0.16.2`, mypy `2.3.0`, pytest `9.1.1`, uv `0.12.3` and gitleaks `8.30.1` with commands.

**Plan guidance**:

- This phase blocks nothing and depends on nothing (FEATURES §4: "the `CICD` lint/typecheck/unit workflow has zero dependencies and can land day one"). It comes first — deviation **D4** — because a pipeline created in the final phase cannot have gated any earlier code.
- Fixture-corpus authoring should **lead** CSV implementation: the corpus *is* the specification. Generate from a seed (PITFALLS #15) — committing thousands of files bloats Docker build contexts, invites globally disabling the secret scanner, and makes the oversized-file memory test impossible.
- src-layout + uv workspace. Two dependency sets are coming (PITFALLS G5: Airflow 3.3.0 constraints pin `pandas==2.1.4`, `psycopg2-binary`, `polars==1.42.1`) — do **not** plan to install `csv_processor` into the Airflow image.
- Add the CI grep forbidding `COPY … FORMAT csv` now, so it is live before the first loader exists (it enforces LOAD-12 in Phase 4).
- Establish `tests/regression/` and the policy that every discovered bug gains a permanent test (QUAL-07) while the test tree is still empty.
- **Cheap-now decision decided here**: PITFALLS #15 — fixtures generated from a seed, not committed en masse.

### Phase 2: kind Cluster & Core Infrastructure

**Goal**: A production-like Kubernetes data platform that can be destroyed and recreated reproducibly from committed files, with the CI-sized profile written from the first infrastructure commit
**Mode:** mvp
**Depends on**: Phase 1
**Requirements**: INFRA-01, INFRA-02, INFRA-03, INFRA-04, INFRA-05, INFRA-07, INFRA-09, INFRA-10, CICD-07
**Success Criteria** (what must be TRUE):

  1. `make cluster-down && make cluster-up` recreates a 3-node kind cluster and redeploys MinIO, both PostgreSQL clusters and Airflow from committed files, with no manual `kubectl` surgery at any step.
  2. The Airflow UI is reachable and shows API server, scheduler, DAG processor and triggerer running as separate workloads.
  3. `psql` reports PostgreSQL 17 on the Airflow metadata cluster and PostgreSQL 18 on the analytical cluster, as two physically separate CloudNativePG `Cluster` resources with no shared storage.
  4. MinIO serves `raw`, `validated`, `processed`, `quarantine` and `metadata` over the S3 API, addressable as `s3://bucket/key`.
  5. `helm template -f values-ci.yaml` renders a stack sized for a 4 CPU / 16 GB runner, and CI fails on an invalid manifest or chart.

**Plans**: 8 plans in 6 waves

Plans:

- [x] 02-01-PLAN.md — Tracer: create, reach over `:80`, and destroy a 3-node kind cluster with a local registry, the five namespaces and ingress-nginx (wave 1)
- [x] 02-02-PLAN.md — `make doctor` fail-closed preflight, the `tests/e2e/cluster/` harness, and timed `cluster-rebuild` (wave 2)
- [x] 02-03-PLAN.md — CloudNativePG operator and two physically separate PostgreSQL clusters, 17 for Airflow metadata and 18 for analytics (wave 3)
- [x] 02-04-PLAN.md — MinIO with five buckets, versioning on `raw`, and a server-enforced deny-delete for the application credential (wave 3)
- [x] 02-05-PLAN.md — ADR-0006 (three unmaintained upstream artifacts) and ADR-0007 (Helm 4 over Helm 3) (wave 3)
- [x] 02-06-PLAN.md — Airflow 3.3.0 with four separate workloads on the metadata cluster, reachable through the ingress (wave 4)
- [x] 02-07-PLAN.md — Offline manifest validation: render both profiles, vendored CNPG CRD schemas, `kubeconform -strict` in CI (wave 5)
- [x] 02-08-PLAN.md — The D-12 sizing and QoS tests, the divergence-axis rule, and the infrastructure-as-code and credential-literal detectors (wave 6)

**Research stage**: S1 + S2. **Use `/gsd-plan-phase --research-phase`** — SUMMARY flags this phase: the Helm 4.2.3-against-Helm-3-charts call is the MEDIUM-confidence judgement in STACK.md, and the MinIO fork image plus CNPG chart defaults must be read off *pinned chart values*, not documentation.

**Plan guidance**:

- **Runs fully in parallel with Phase 3** (SUMMARY parallelization wave A, ~25% of total effort). Different artifacts, different harnesses — Phase 3 needs only Docker, never a cluster. If executing with worktrees, these two phases share no files.
- Internally parallel: 2a MinIO ‖ 2b analytical PG (CNPG, PG 18) ‖ 2c Airflow PG (CNPG, PG 17) → 2d Airflow (needs 2c only).
- **Vault is deliberately NOT in this phase** (deviation **D3**). It arrives in Phase 5, behind the `SecretsResolver` seam.
- Pin `kindest/node:v1.35.5` — kind's *default* 1.36.1 is outside Airflow 3.3.0's supported 1.30–1.35 range. Airflow 3.3.0 supports PostgreSQL 13–17 only, which is why the split is PG 17 (Airflow) / PG 18 (analytical).
- Set `postgresql.enabled: false` on the Airflow chart (its bundled subchart points at `bitnamilegacy/postgresql`). The CNPG `cluster` chart defaults to PG 16 — override explicitly, do not trust the default.
- Use a **local registry**, not `kind load` — per-node loading is a known friction for the Phase 4 U1 spike.
- Raise `fs.inotify.max_user_watches` / `max_user_instances` via `/etc/sysctl.d/99-kind.conf` and `/etc/wsl.conf`; exhaustion surfaces here, not in Phase 1. WSL2's `ext4.vhdx` never shrinks, so **disk is the real ceiling, not RAM**. Add `make doctor`.
- Write the ADR naming SeaweedFS as the MinIO migration target — `pgsty/minio` is a single-vendor dependency and the S3 client must stay a hard abstraction seam.
- **Cheap-now decisions decided here — changing either later requires destroying the cluster**: PITFALLS #10 (PV persistence and `extraMounts` in `kind/cluster.yaml`) and PITFALLS #11 (kubelet reservations and `maxPods` in the kind config, plus requests/limits on every chart). Without them the scheduler over-packs and the *host* OOM killer arbitrates.
- **INFRA-10 gates Phase 11**: `values-ci.yaml` must exist now even though the ephemeral-kind E2E consumes it nine phases later. Retrofitting profile parameterization is expensive.

### Phase 3: `dataplat` Core Library & Metadata Control Plane

**Goal**: The metadata control plane and the pipeline engine exist as one coherent, testable Python library — the platform's traceability guarantee made concrete before any pipeline runs
**Mode:** mvp
**Depends on**: Phase 1 (runs in parallel with Phase 2)
**Requirements**: META-01, META-02, INFRA-08, SEC-15, CSV-13, SCHEMA-07, OBS-02, OBS-04, OBS-05, QUAL-03
**Success Criteria** (what must be TRUE):

  1. `alembic upgrade head` against a throwaway PostgreSQL creates the whole `meta` schema from one coherent design, and every stored hash column has a companion `hash_version`.
  2. The library's entire test suite passes against testcontainers-provided PostgreSQL and MinIO with **no Kubernetes cluster present**.
  3. `docker run csv-processor:<git-sha> dataplat --version` prints the version, from an image tagged by git SHA — never `:latest`.
  4. A processor run resolves its database credential from an opaque reference (`env://…`, `file://…`) and no code path names Vault or Kubernetes Secrets.
  5. Every library log line is structured JSON carrying dataset, stage, object path and run identifiers; a credential passed through the resolver never appears in any log; and a bad value on row 41,203 surfaces as a `ValidationResult` value rather than an exception.

**Plans**: 8 plans in 5 waves

Plans:

**Wave 1**

- [x] 03-01-PLAN.md — Core data contracts: exception hierarchy, frozen value objects, the psycopg connection-pool factory (wave 1)

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 03-02-PLAN.md — Metadata schema migrations, testcontainers harness, `make test-integration` (wave 2)
- [x] 03-03-PLAN.md — Structured logging (JSON/console, contextvars, redaction) and the SecretsResolver (wave 2)

**Wave 3** *(blocked on Wave 2 completion)*

- [x] 03-04-PLAN.md — Config-not-code: DatasetConfig, canonical-JSON hashing, ConfigRegistry (wave 3)
- [x] 03-05-PLAN.md — ObjectStore (StreamingBody bridge) and MetadataRepository (wave 3)
- [x] 03-07-PLAN.md — CLI (--version, catch-once error boundary) and the csv-processor Docker image (wave 3)

**Wave 4** *(blocked on Wave 3 completion)*

- [x] 03-06-PLAN.md — PipelineContext, Source/Publisher protocols, the sequencing engine, ADR-0008 (wave 4)

**Wave 5** *(blocked on Wave 4 completion)*

- [x] 03-08-PLAN.md — csv_processor's minimal Source: record-ordinal CSV chunking (wave 5)

**Research stage**: S3. **Skip `--research-phase`** — pure Python over a fixture corpus; STACK.md has already chosen every library and rejected the alternatives with reasons.

**Plan guidance**:

- **Runs fully in parallel with Phase 2** (wave A). This is the "library track": Docker/testcontainers only, no cluster.
- **Deviation D2 is the point of this phase.** The `meta` schema is designed *coherently up front* — datasets, config versions, files, batches, ingestion runs, run stages, schema versions, watermarks, dedup audit, validation results, reconciliation results. Phases 8–10 *populate* tables whose design already exists; they do not invent new ones. Land the five tables the slice needs (`datasets`, `config_versions`, `files`, `batches`, `ingestion_runs`) plus `normalized.customers` against that complete design. FEATURES calls this "the single strongest structural recommendation"; accreting it capability-by-capability guarantees six migrations and inconsistent foreign keys.
- Establish the seam that makes README §29/§95 extensibility true: protocols `Source` → `RecordChunk` → `Publisher`, plus `Stage` and `MetadataRepository`. **README §68's proposed package layout does not contain this seam** — record the departure as an ADR now so it is not re-litigated at Phase 10.
- Observability lives here as **no-op seams** (`observability/{logging,metrics,tracing}.py`); the stack arrives in Phase 7.
- `SecretsResolver` (SEC-15) is what makes deviation D3 safe — the Kubernetes-Secrets→Vault swap in Phase 5 must be a configuration change and nothing more.
- Staging tables are **all-TEXT** by design, so structural and type errors stay attributable to a row.
- **Cheap-now decisions decided here**: PITFALLS #1 — `hash_version` alongside every stored hash (the first migration that stores a hash is in this phase); PITFALLS #5 — one `csv.reader` over a `newline=""` text wrapper, chunked in **records**, never lines or byte offsets. Getting #5 wrong corrupts every embedded-newline record *and* forces a redesign of the checkpoint model.
- Use **record-ordinal checkpoints, not byte offsets** — `last_committed_chunk_ordinal` on the batch ledger row delivers README §38 as a byproduct.
- Pre-filter NUL bytes before the stdlib csv reader (cpython #71767). Ragged rows are errors — never pad or truncate (polars #10585).

### Phase 4: Vertical Slice — CSV to Analytical PostgreSQL

**Goal**: One real CSV travels end to end — MinIO → TaskFlow DAG → KubernetesPodOperator → processor → analytical PostgreSQL — and is idempotent by construction, so a re-run produces zero additional rows
**Mode:** mvp
**Depends on**: Phase 2 and Phase 3
**Requirements**: ORCH-01, ORCH-02, ORCH-03, ORCH-04, ORCH-05, ORCH-06, ORCH-07, ORCH-08, ORCH-09, META-03, LOAD-01, LOAD-02, LOAD-03, LOAD-04, LOAD-05, LOAD-08, LOAD-09, LOAD-12, INCR-08, QUAL-05, QUAL-06, QUAL-09
**Success Criteria** (what must be TRUE):

  1. Dropping a UTF-8 comma-delimited CSV into `s3://raw/` triggers a TaskFlow DAG that runs a `KubernetesPodOperator` pod, which loads the rows into analytical PostgreSQL and returns a ≤ 4 KB receipt through XCom — with the DAG file under 150 lines and containing no parsing, validation, typing or database writes.
  2. Re-running the same DAG run, and separately re-uploading the same file under a different name, both produce **zero additional rows** — asserted by an automated test.
  3. Killing the task pod mid-load and letting Airflow retry leaves no duplicate rows and no partially visible dataset; a concurrent `SELECT` never observes a half-loaded table.
  4. `meta.batches`, `meta.ingestion_runs` and every loaded row answer "which file, which batch, which run, which attempt, which config version" by SQL alone, with file, batch, record and target-row identity stored distinctly.
  5. Spike results are recorded in the repository: U1 — the XCom payload contains the git SHA that was built; U3 — a measured streaming throughput *and peak RSS* baseline for per-chunk `COPY` under the pod's memory limit.

**Plans**: 9 plans in 5 waves

Plans:
**Wave 1**

- [x] 04-01-PLAN.md — Migration 0006 (UNIQUE constraint) and the metadata/objectstore/config-registry interface contracts
- [x] 04-02-PLAN.md — etl namespace RBAC, dev-only credential Secrets, Helm DAG-mount wiring, image build/push

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 04-03-PLAN.md — AssignmentDocument/Receipt models and discover_files (frozen-manifest authoring)
- [x] 04-04-PLAN.md — StagingLoader and the corrected MergePublisher (advisory-lock + ON CONFLICT, never MERGE)

**Wave 3** *(blocked on Wave 2 completion)*

- [x] 04-05-PLAN.md — run_ingest orchestration and the discover/ingest CLI wiring (entry-point plugin fix)
- [x] 04-06-PLAN.md — Integration tests: discovery rerun, publish atomicity/concurrency/lineage

**Wave 4** *(blocked on Wave 3 completion)*

- [x] 04-07-PLAN.md — The two DAG files (smoke + csv_ingest_customers) and their structural/policy tests

**Wave 5** *(blocked on Wave 4 completion)*

- [x] 04-08-PLAN.md — E2E suite: pod-kill/retry, concurrent-select, idempotent reupload, U1/U3 spike results
- [x] 04-09-PLAN.md — make ingest-demo (D-14/D-15/D-16)

**Research stage**: S4 + S5. **Skip `--research-phase` for the smoke DAG** (S4 is deliberately trivial and the experiment is fully specified). Consider targeted research only for the publication-transaction shape.

**Plan guidance**:

- **This is the critical path (wave B, ~15% of effort) and it is strictly serial. Protect it.** Do not widen scope: one dataset, one encoding, one delimiter, no header edge cases.
- **Keep the S4 smoke step separate and deliberately trivial.** The highest-risk unknown in the project is not CSV parsing — it is whether an Airflow 3 `KubernetesPodOperator` on kind can pull a locally-built image, run as a non-root service account and return an XCom. Debugging that *while* debugging a CSV pipeline is how a week disappears.
- **Spike U1 (S4, MEDIUM risk, under an hour)**: build `csv-processor:<git-sha>` printing its own version, push to the local registry, run via KPO with `do_xcom_push=True` writing `/airflow/xcom/return.json`. **Pass criteria: the XCom contains the SHA that was built.** Three frictions are near-certain and all cheap to pre-empt — per-node image availability, a stale image because the tag already existed, and pull time exceeding `startup_timeout_seconds` (default 120 s against a 2 GB image). This becomes the permanent platform smoke test.
- **Spike U3 (S5, genuinely unvalidated — needs an experiment, not an argument)**: streaming CSV throughput with per-chunk `COPY` under pod limits. **Do not run it with `executemany`** — that is 10–100× slower than `COPY` and a false negative would change the architecture for no reason. **Measure peak RSS as well as throughput**; the real risk is an implementation that is "streaming" in shape but accumulates somewhere. Record the baseline in the repo and treat a later 5× regression as a bug, not a mystery.
- **Deviation D1 is why idempotency is here and not in README §92 stage 8.** Marginal cost now: two unique constraints, one claim query, one table. Cost later: a migration across six phases of accumulated schema — and in the meantime Airflow retries are **on by default**, so the platform silently duplicates data every phase, while the tests that would catch it (QUAL-09) do not yet exist. FEATURES calls this "the single largest avoidable rework risk in the project"; Delta Lake's `(txnAppId, txnVersion)` is proof the idempotency token is small.
- Pod amplification is real: KubernetesExecutor + KubernetesPodOperator is **two pods per task**. Cap `max_map_length` — uncapped Dynamic Task Mapping is how a 32-CPU host falls over.
- The XCom sidecar has four separate ways to fail and it is on the critical path for the receipt contract: write the receipt in a `finally`, use a local sidecar image, stay Pod Security Standards compatible. **Never return data through XCom** — overflow goes to MinIO.
- Declare `namespace` and `service_account_name` explicitly on task pods now (PITFALLS #13), so Phase 5 can match a Vault role to them instead of widening the role later.
- The publication transaction is the atomicity boundary for **data and metadata together** (META-03): rows, watermark advance and run status commit together or not at all. This is also the decisive reason OpenLineage cannot be the system of record — an HTTP emitter cannot enlist in a PostgreSQL transaction.
- A dev-only DSN in a Kubernetes Secret is acceptable for this phase only; it is removed in Phase 5, with secret scanning active since Phase 1.
- **Cheap-now decisions decided here**: PITFALLS #3 (run-scoped identity `run_id`/`attempt` on every staged and loaded row, plus `UNIQUE (dataset, batch_key)` on the batch ledger); #4 (Dynamic Task Mapping expands over a **frozen manifest**, never a live object-storage listing, or reruns and backfills silently produce different work); #8 (business date comes from the **data**, never the clock or `logical_date` — `logical_date` is `None` in asset-triggered Airflow 3 runs anyway); #9 (the processor is the **only** CSV parser — `COPY … FORMAT csv` on raw input would put rows in the warehouse validation never saw, making every later guarantee decorative); #12 (metric labels bounded, unbounded identity in the metadata DB — the rule, enforced in Phase 7); #14 (single-writer publication via `pg_advisory_xact_lock` + `INSERT … ON CONFLICT` on the natural key — `MERGE` is not concurrency-safe).

### Phase 5: Vault Secrets & Workload Identity

**Goal**: Vault is the only source of runtime credentials, and workload identity is real enough that an unauthorized service account is provably denied
**Mode:** mvp
**Depends on**: Phase 4
**Requirements**: INFRA-06, SEC-01, SEC-03, SEC-04, SEC-05, SEC-06, SEC-07, SEC-08, SEC-09, SEC-12, SEC-13, SEC-14
**Success Criteria** (what must be TRUE):

  1. With the Airflow metadata-DB connection deleted from the Airflow database and every `AIRFLOW_CONN_*` unset, DAGs still resolve their connections and run — proving the Vault backend actually served them.
  2. The `csv-processor` service account in the `etl` namespace reads its own Vault path, **and** the `default` service account is denied another workload's secrets — both asserted by automated tests.
  3. Restarting the kind cluster leaves Vault's data intact, and the documented unseal procedure restores service without data loss.
  4. Vault's audit log shows which workload read which path, when, and whether it succeeded — with no secret values present in the log.
  5. No credential exists in git history, Python source, Dockerfiles, Kubernetes manifests, Airflow Variables or CI workflow files; development secrets are marked, isolated and reproducible on a fresh local rebuild.

**Plans**: 5 plans in 5 waves

Plans:

**Wave 1**

- [x] 05-01-PLAN.md — Vault deployed: namespace, both Helm values profiles, scripted unseal (D-02), idempotent bootstrap (mounts, auth method, both roles/policies, audit device), restart-persistence proof (wave 1)

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 05-02-PLAN.md — ETL identity: the vault:// scheme in resolve_secret(), KPO pod wiring, positive/negative auth proof (SEC-06/07/12), csv-processor-db + csv-processor-s3 retired (wave 2)

**Wave 3** *(blocked on Wave 2 completion)*

- [x] 05-03-PLAN.md — Airflow identity: VaultBackend wiring, empirical ServiceAccount correction, SEC-05 proof, airflow-minio-connection retired (wave 3)

**Wave 4** *(blocked on Wave 3 completion)*

- [x] 05-04-PLAN.md — D-03 rotation proof, D-04 make vault-audit-tail, SEC-08 audit-content proof, SEC-13 reproducibility proof (wave 4)

**Wave 5** *(blocked on Wave 4 completion)*

- [x] 05-05-PLAN.md — Permanent SEC-01 structural guard, SEC-14 secrets-architecture documentation, ADR-0009 (OpenBao) (wave 5)

**Research stage**: S6. **Use `/gsd-plan-phase --research-phase`** — the kind-specific JWT-issuer caveat in ARCHITECTURE is explicitly flagged as *inference, unverified on this cluster*, and `auth_type: kubernetes` is present in the Airflow hashicorp provider code but undocumented on its docs page. Verify against pinned provider source, not the docs page.

**Plan guidance**:

- **Wave C: runs in parallel with Phase 6 and Phase 7** (~20% of effort combined). Vault touches manifests, the CSV engine touches `csv_processor/`, observability touches Helm plus `dataplat/observability` — almost no file overlap.
- **Deviation D3**: Vault comes *after* the slice, not before it. The slice needed *credentials*, not a *secrets manager*; putting a mutating webhook, a Kubernetes auth mount, TokenReview permissions and policy debugging on the critical path of "does anything work end to end at all?" is how the slice slips. The retrofit is a ConfigMap change **if and only if** `SecretsResolver` (Phase 3) exists — and it does.
- **Spike U2 (LOW risk, under half a day)**: `make vault-bootstrap` creates the auth method, policy, role and Kubernetes RBAC from one variable set. **Pass criteria: both tests pass — positive (own path readable) and negative (`default` SA denied).** *If the negative test is awkward to write, the identity model is not real yet — that is itself the finding.*
- `disable_iss_validation` has defaulted true since Vault 1.9 because TokenReview performs the same validation; the famous `claim "iss" is invalid` error is a Kubernetes-1.21-era artefact. The real remaining risks are: the token reviewer lacking `system:auth-delegator`, an audience mismatch, `kubernetes_host` copied from an outside-cluster tutorial, and — most likely — the role binding a ServiceAccount that KPO does not actually use.
- **Check `date` before diagnosing anything auth-related.** WSL2 clock drift after host sleep produces `permission denied` on valid tokens and x509 failures that "fix themselves"; it will otherwise be misdiagnosed as a Vault problem.
- Vault dev mode loses everything on restart — on WSL2 that means every morning — and a sealed Vault after restart is a *manual* step unless planned for. The Airflow Vault secrets backend **fails open at parse time**, which is exactly why SEC-05's verification must delete the connection rather than merely assert it resolves.
- Document the substitution path to a production secrets manager (SEC-14) while the design is fresh.
- **Cheap-now decision completed here**: PITFALLS #13 — explicit `namespace` + `service_account_name` on task pods matched to the Vault role. The usual "fix" when they do not match is to widen the Vault role, which silently voids least privilege.

### Phase 6: Universal CSV Engine, Schema Contracts & Normalization

**Goal**: Real-world messy CSV files parse, type, version and normalize correctly — or fail with a named diagnostic — and nothing is ever silently coerced
**Mode:** mvp
**Depends on**: Phase 4
**Requirements**: CSV-01, CSV-02, CSV-03, CSV-04, CSV-05, CSV-06, CSV-07, CSV-08, CSV-09, CSV-10, CSV-11, CSV-12, SCHEMA-01, SCHEMA-02, SCHEMA-03, SCHEMA-04, SCHEMA-05, SCHEMA-06, LOAD-07, QUAL-04, QUAL-12, QUAL-16, QUAL-17
**Success Criteria** (what must be TRUE):

  1. Every file in the edge-case corpus — UTF-8 BOM, UTF-16 LE/BE, Windows-1250/1252, ISO-8859, semicolon/pipe/tab/colon dialects, embedded newlines, escaped quotes, inconsistent quoting, metadata preambles, a header at row 7, totals footers, `.gz` and `.zip` — either parses to the expected records or produces a named diagnostic identifying the row.
  2. `001234` stays a string, `2026-02-30` and `31/02/2026` produce explicit validation errors rather than coerced dates, and `1,234.56` / `1.234,56` / `(1234)` / `45%` normalize per the dataset's locale configuration — with `1/0` never becoming boolean absent evidence.
  3. Adding a column is classified compatible and processed; renaming a business key is classified breaking and *reported* as drift rather than silently adapted to.
  4. A file from three schema versions ago reprocesses under its historical schema version, not the newest, and its batch records dataset, schema version, schema hash, processor version and timestamp.
  5. Processing the same file twice yields an identical output hash, DST gap and overlap timestamps round-trip correctly, and a file larger than the pod's memory limit loads in bounded memory.

**Plans**: 18 plans in 5 waves

Plans:

**Wave 1**

- [x] 06-01-PLAN.md — Foundations: detection-library deps, .zip corpus generator support, meta.schema_versions migration
- [x] 06-02-PLAN.md — Shared contracts: DatasetConfig columns/filename/normalization/csv extensions, diagnostics catalog, SourceError/SchemaError hierarchy, customers.yaml activation

**Wave 2** *(blocked on Wave 1 completion — eleven plans, the phase's best parallelization opportunity)*

- [x] 06-03-PLAN.md — Filename mask detector (CSV-01)
- [x] 06-04-PLAN.md — Encoding detector (CSV-02/03)
- [x] 06-05-PLAN.md — Dialect detector (CSV-04/05/06)
- [x] 06-06-PLAN.md — Header/metadata/footer detector (CSV-07/08, SCHEMA-02)
- [x] 06-07-PLAN.md — Schema type inference (SCHEMA-01)
- [x] 06-08-PLAN.md — Compression (.gz/.zip) and multi-part delivery grouping (CSV-11, LOAD-07)
- [x] 06-09-PLAN.md — Date/timestamp normalizer + DST-correctness property test (CSV-09, QUAL-17)
- [x] 06-10-PLAN.md — Numeric normalizer (CSV-10 numeric half)
- [x] 06-11-PLAN.md — Boolean/NULL normalizer + Unicode NFC normalizer (CSV-10 boolean/null half, CSV-12)
- [x] 06-12-PLAN.md — Schema versioning + repository + historical hash-match resolution (SCHEMA-03, SCHEMA-06)
- [x] 06-13-PLAN.md — Schema evolution classification (SCHEMA-04, SCHEMA-05, QUAL-12)

**Wave 3** *(blocked on Wave 2 completion)*

- [x] 06-14-PLAN.md — Wire the five detectors + compression into CsvSource.inspect()/open()
- [x] 06-16-PLAN.md — Wire the four normalizers into StagingLoader; discovery.py idempotency-key/business-date extensions

**Wave 4** *(blocked on Wave 3 completion)*

- [x] 06-15-PLAN.md — Wire schema versioning/evolution into CsvSource.inspect()

**Wave 5** *(blocked on Wave 4 completion)*

- [x] 06-17-PLAN.md — Determinism property test (QUAL-16)
- [x] 06-18-PLAN.md — Wire multi-part delivery grouping into discover_files/CsvSource (CSV-11 real-pipeline closure)

**Research stage**: S7. **Skip `--research-phase`** — pure Python over a fixture corpus; STACK.md has chosen `charset-normalizer` `3.4.9` + `chardet` `7.5.1` (behind a BOM sniff and a contract override) and `clevercsv` `0.8.5` for detection only.

**Plan guidance**:

- **Wave C ‖ D — this phase contains the single best parallelization opportunity in the project.** The five detectors are mutually independent pure functions over a shared **read-only** fixture corpus: 6a filename ‖ 6b encoding ‖ 6c dialect ‖ 6d header/footer ‖ 6e inference. Plan them as concurrent plans with no shared mutable state. The date/number/boolean/null/whitespace normalizers are likewise independent pure functions.
- Streaming (6f, LOAD-07) depends on the record-chunking rule already fixed in Phase 3 (CSV-13) and the U3 baseline from Phase 4. Configurable batch size plus maximum field and row length.
- **Hard ordering edge: normalization MUST precede hashing.** Both deduplication (exact-row hash, Phase 9) and SCD change detection (Phase 10) hash *normalized* content. If normalization lands after either, both produce phantom differences — phantom duplicates and phantom SCD2 versions. Unicode NFC/NFD normalization (CSV-12) is part of this, and README §18 covers whitespace only.
- **Schema versioning gates two later capabilities**: drift detection and historical backfill resolution (Phase 9's INCR-06). It is a prerequisite, not a nice-to-have.
- Adopt dlt's 3×4 schema-contract matrix (`{tables, columns, data_type}` × `{evolve, freeze, discard_row, discard_value}`) rather than a boolean evolve/freeze flag.
- **Reduce ambition deliberately**: multi-row and hierarchical headers are **detected and rejected with a clear diagnostic** — no canonical flattening exists, and v1 does not invent one.
- Encoding detection returns a confidence score and never claims determinism it does not have; the data contract can always override detection.
- The corpus grows here as cases are discovered — every new edge case becomes a fixture and, if it was a bug, a regression test (QUAL-07 policy from Phase 1).

### Phase 7: Observability, Metrics, Tracing & Lineage

**Goal**: The question "where did this row come from, and is the feed healthy?" is answerable by SQL and by dashboard, and a single trace spans Airflow task to PostgreSQL
**Mode:** mvp
**Depends on**: Phase 4 (consumes schema versions from Phase 6)
**Requirements**: OBS-01, OBS-07, OBS-08, OBS-09, OBS-10
**Success Criteria** (what must be TRUE):

  1. A Grafana dashboard shows `files_processed`, `files_failed`, `rows_processed`, `rows_invalid`, `rows_deduplicated`, `processing_duration`, `validation_failures` and `data_freshness`, and Prometheus label cardinality stays bounded as the file count grows.
  2. One SQL query returns, for any warehouse row, its source file, object path, checksum, batch, ingestion timestamp, DAG/run/task ID, processor version, schema version and config version.
  3. A single trace spans Airflow task → task pod → processor → PostgreSQL for one ingestion run, with the context crossing the pod boundary.
  4. A dataset whose file is overdue against its expected frequency reports "expected but missing", while a dataset with no expected arrival reports "none available" and stays quiet — each with configurable warn-or-fail behaviour.

**Plans**: 8 plans in 4 waves

Plans:

**Wave 1**

- [x] 07-01-PLAN.md — Control-plane completion: freshness columns, grafana_reader role, meta.v_customers_lineage (OBS-07 in full)
- [x] 07-02-PLAN.md — dataplat.observability real OTel SDK backends: metrics.py/tracing.py, bounded D-04 labels
- [x] 07-03-PLAN.md — OTel Collector + Tempo infrastructure: monitoring namespace, both chart pairs, offline CI validation

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 07-04-PLAN.md — Custom Airflow image (apache-airflow[otel]) + TracingKubernetesPodOperator (build_pod_request_obj override)
- [x] 07-05-PLAN.md — Pod-side trace extraction/capture + runs_started/runs_finished live-gauge counters
- [x] 07-06-PLAN.md — Grafana Vault-backed credentials: grafana_reader password + webhook URL, the third Vault-consumer tier

**Wave 3** *(blocked on Wave 2 completion)*

- [x] 07-07-PLAN.md — Grafana: three datasources, ServiceMonitor scrape wiring, the 8-metric+3-gauge dashboard, alerting-as-code

**Wave 4** *(blocked on Wave 3 completion)*

- [x] 07-08-PLAN.md — Live-cluster proof: real trace propagation (OBS-10) and real alert webhook delivery (D-20)

**Research stage**: S11. **Use `/gsd-plan-phase --research-phase`** — STACK rates metrics and traces MEDIUM. Cross-process trace propagation into KPO pods is **not** built in; the W3C `traceparent` injection recipe is DIY. Airflow's StatsD-XOR-OTel constraint shapes the whole design.

**Plan guidance**:

- **Wave C: runs in parallel with Phase 5 and Phase 6.** This phase touches Helm values and `dataplat/observability`; almost no file overlap with the other two.
- **Deviation D5**: observability is an explicit stage because README §82 (metrics) and §83 (lineage) have **no DoD items at all** and no home in §92. The *seams* already exist as no-ops from Phase 3; this phase supplies the *stack*.
- Airflow emits metrics to StatsD **XOR** OTel, never both — pick once and document why. Business metrics live in the analytical database and reach Grafana through a Postgres datasource; runtime metrics push via OTLP.
- **No Prometheus Pushgateway.** Short-lived task pods produce permanent staleness and cardinality explosion; that is why unbounded identity lives in the metadata DB (PITFALLS #12, the rule fixed in Phase 4).
- Lineage (OBS-07) is a SQL view over the `meta` schema designed in Phase 3 — it is a *query*, not a new store. Prometheus/Grafana/Tempo hold no data-correctness state.
- Tracing is valuable and expensive, and its value depends on everything else already emitting context — which is why it sits here rather than earlier.
- OpenLineage export is explicitly v2: an HTTP emitter cannot enlist in the publication transaction, so it can never be the system of record.

### Phase 8: Validation, Quarantine & Metadata Control-Plane Completion

**Goal**: No data is ever silently dropped — every rejected record is retained with a reason, reportable, and has a documented path back into the pipeline
**Mode:** mvp
**Depends on**: Phase 6
**Requirements**: VALID-01, VALID-02, VALID-03, VALID-04, VALID-07, VALID-08, VALID-09, LOAD-10, LOAD-11
**Success Criteria** (what must be TRUE):

  1. A file with 12 malformed rows loads the good rows and writes 12 quarantine records naming source file, row number, column where possible, error type, run and timestamp — with nothing silently discarded.
  2. A machine-readable validation report for that run exists both as rows in PostgreSQL and as an artifact in MinIO, and a dataset breaching its configured threshold reports FAIL or QUARANTINE while an under-threshold one reports PASS_WITH_WARNING.
  3. Corrected quarantined records re-enter the pipeline through the documented re-drive path and land in the warehouse.
  4. A truncated or still-uploading file — checksum mismatch, size mismatch, wrong extension, empty, or missing its `_BATCH_COMPLETE` marker — is refused before any parsing occurs.
  5. A file whose row count is 10× its historical baseline is flagged as a volume anomaly against persisted statistics, and an orphan foreign key produces the dataset's configured fail / quarantine / warn outcome.

**Plans**: 14 plans in 7 waves

Plans:

**Wave 1**

- [x] 08-01-PLAN.md — Foundational DDL + contracts: migrations 0014/0015/0016, errors.py, report.py, repository.py Protocol, config/model.py quality: block (wave 1)
- [x] 08-02-PLAN.md — LOAD-10 integrity_gate.py: extension/empty/stability/checksum checks + D-20 rejection write (wave 1)

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 08-03-PLAN.md — metadata/postgres.py: record_validation_results/record_rejected_records/resolve_rejected_records_for_batch (wave 2)
- [x] 08-04-PLAN.md — validate/ registry + CompletenessRule/ValidityRangeRule/PatternRule (wave 2)
- [x] 08-05-PLAN.md — orders substrate: dataset-aware target columns, OrdersMergePublisher, orders.yaml minimal (wave 2)
- [x] 08-06-PLAN.md — LOAD-11 discover_files' opt-in _BATCH_COMPLETE gate (wave 2)

**Wave 3** *(blocked on Wave 2 completion)*

- [x] 08-07-PLAN.md — RejectionRateCircuitBreaker (D-10) + UniquenessRule (wave 3)
- [ ] 08-08-PLAN.md — ReferentialIntegrityBarrier (VALID-07) + orders.yaml quality: REFERENTIAL rule (wave 3)
- [ ] 08-09-PLAN.md — VolumeAnomalyBarrier (VALID-09 minimal slice) (wave 3)

**Wave 4** *(blocked on Wave 3 completion)*

- [ ] 08-10-PLAN.md — StagingLoader._build_stages dispatches ctx.config.quality's streaming rules (wave 4)

**Wave 5** *(blocked on Wave 4 completion)*

- [ ] 08-11-PLAN.md — run_ingest publish-transaction wiring: barrier stages + persistence + D-11 rollback + customers.yaml quality: block (wave 5)

**Wave 6** *(blocked on Wave 5 completion)*

- [ ] 08-12-PLAN.md — DAG layer: integrity_gate wiring + outlets on csv_ingest_customers, new csv_ingest_orders DAG (wave 6)

**Wave 7** *(blocked on Wave 6 completion)*

- [ ] 08-13-PLAN.md — tests/dagtest/ new tier: dag.test() proves backfill DagRun mechanics (wave 7)
- [ ] 08-14-PLAN.md — Live-cluster proof: real orphan-order race (VALID-07) + real airflow dags backfill re-entry (VALID-08) (wave 7)

**Research stage**: S8 + S9. **Skip `--research-phase`** — shapes are specified in ARCHITECTURE Q2 and FEATURES §3.2/§3.3.

**Plan guidance**:

- **Wave E: validation (S8) ‖ metadata completion (S9)** — they coordinate only on the `meta.validation_results` DDL, which was already designed in Phase 3.
- This phase *populates* control-plane tables whose design already exists (`schema_versions`, `run_stages`, `dedup_audit`, `validation_results`, `reconciliation_results`, `watermarks`) plus the config-sync job. It does not redesign the schema — that was deviation D2's whole purpose.
- **Validation rule types are independent of each other** except referential integrity, which needs a multi-dataset load. Plan completeness, uniqueness, validity-range and pattern rules as parallel work.
- **Persisted validation results gate anomaly detection**: VALID-09's statistical baselines are computed from VALID-04's stored rows, so ordering inside the phase is rules → persistence → baselines → anomalies.
- Quarantine without an exit is a data graveyard — VALID-08's re-drive path is a first-class deliverable, not documentation.
- Great Expectations' pattern of persisting validation results as **rows**, not just artifacts, is the model adopted here. (Note: the claim that GX/Soda lack record-level quarantine came from search-result summaries and is LOW confidence — do not cite it as justification without verifying.)
- Statistical thresholds only. **No ML anomaly detection** — README §53 mandates simple configurable thresholds first, and PROJECT.md holds that line.
- File integrity (LOAD-10) and manifests (LOAD-11) gate ingestion *before* parsing; the manifest may be the authoritative input to a run, pairing with the frozen-manifest rule from Phase 4.

### Phase 9: ETL Correctness — Dedup, Incremental, Backfill & Recovery

**Goal**: The platform processes only what is new, never loses late data, recovers from partial failure without reading logs, and can prove target matches source
**Mode:** mvp
**Depends on**: Phase 8
**Requirements**: DEDUP-01, DEDUP-02, DEDUP-03, DEDUP-04, INCR-01, INCR-02, INCR-03, INCR-04, INCR-05, INCR-06, LOAD-06, VALID-05, VALID-06, QUAL-10, QUAL-11
**Success Criteria** (what must be TRUE):

  1. The same records delivered again — within one file, across files, and across batches — result in one stored row per business key, with `meta.dedup_audit` explaining every removal by strategy, count and reason.
  2. An incremental run processes only new data and its watermark advances only after the publication transaction commits; killing the run mid-flight leaves the watermark exactly where it was.
  3. A backfill over a two-year window runs through the same discovery → validate → normalize → dedupe → load → lineage path as a normal run, with no bypass, resolves each file's historical schema version, handles a missing file explicitly, and produces an identical result when run twice.
  4. A record arriving three months late lands in its correct historical partition rather than today's, and out-of-event-time-order records produce the correct final state.
  5. After a deliberately interrupted load, one query reports what succeeded, what remains and whether retry or rollback is required — and reconciliation reports source-vs-target record counts, sums, checksums, min/max and key counts, flagging a deliberately corrupted control total as a discrepancy.

**Plans**: TBD

**Research stage**: S10. **Use `/gsd-plan-phase --research-phase` for the recovery/checkpoint/lease work (10c)** — the interaction of checkpointing × transactions × concurrency (README §38 × §35 × §37 × §86/§87) is the least-settled area of ARCHITECTURE.

**Plan guidance**:

- **Wave F: 10a (dedup + incremental/watermarks) ‖ 10d (reconciliation + control totals) → 10b (backfill + late/out-of-order) → 10c (recovery + checkpoint + lease).** 10b needs watermarks from 10a; 10d is independent of all three.
- **Hard ordering already satisfied**: normalization (Phase 6) precedes exact-row hashing here. Idempotency (Phase 4) precedes retries and backfills — retries depend on idempotency, never the reverse.
- **Reduce ambition deliberately**: build the dedup **strategy interface** plus exact-row hash and business key. The remaining four strategies (key+timestamp, latest-wins, source-priority, batch-aware) are implementations of a solved interface, not new architecture, and are deferred to v2. **Never `SELECT DISTINCT`.**
- **Cheap-now decision decided here**: PITFALLS #7 — advance the watermark only from **observed committed cursor values, lagged**, inside the publication transaction, using `>=` with an idempotent merge, never `>`. Watermarks advanced by wall-clock or max-seen mean rows committed out of timestamp order are never seen again — and only control totals (VALID-06, also in this phase) can detect it.
- PITFALLS #14 hardening lands here: single-writer publication under the concurrency README §86 actually requires.
- Intra-file checkpointing is `last_committed_chunk_ordinal` on the batch ledger row — a byproduct of the ledger, not a new mechanism. Byte-offset resume within a file is v2; build it only if a fixture demands it.
- Backfills use the *same* pipeline with no simplified bypass path — that is a correctness property, not a convenience.

### Phase 10: CDC & Slowly Changing Dimensions

**Goal**: Historical truth is maintained correctly — non-overlapping validity intervals enforced by the database, late corrections repaired by recomputation, and CDC events feeding SCD without a parallel pipeline
**Mode:** mvp
**Depends on**: Phase 9
**Requirements**: SCD-01, SCD-02, SCD-03, SCD-04, SCD-05, SCD-06, SCD-07, SCD-08, SCD-09, SCD-10, SCD-11, SCD-12, CDC-01, CDC-02, CDC-03, QUAL-13, QUAL-14
**Success Criteria** (what must be TRUE):

  1. A changed tracked attribute produces a new SCD2 version with correct `valid_from` / `valid_to` / `is_current`, while an unchanged re-delivery — or a replayed identical event — produces exactly one logical version and no new row.
  2. Attempting to store an overlapping validity interval for a business key is rejected by the **database**, not by application code, and surrogate keys remain independent of the change hash.
  3. A late-arriving correction dated between two existing versions rebuilds that key's history correctly from the ordered event log, and applying it twice yields the same result.
  4. A CSV-delivered CDC feed with shuffled sequence numbers is reordered by the ordering barrier and yields the same dimension state as an in-order feed; a DELETE applies the dataset's configured semantics; and the documented delivery semantics claim at-least-once source→platform with no unearned exactly-once claim.
  5. Effective dates come from source, business or event time as configured and never default to ingestion time — verified on a backfilled batch, where SCD Type 0 retains originals and Type 1 overwrites without history.

**Plans**: TBD

**Research stage**: S12. **Use `/gsd-plan-phase --research-phase`** — this is the hardest correctness work in the project (SCD2 late-arriving corrections, CDC ordering, tombstones and resurrection). PITFALLS C7–C9 are dense but the design space is still open.

**Plan guidance**:

- **Wave G: 12a (SCD) ‖ 12b (CDC) → 12c (CDC→SCD).** Publisher work versus Source work; they meet only at the end.
- **CDC does NOT gate SCD.** SCD Types 0/1/2 build from CSV batches alone; only SCD-08 needs CDC. Do not let CDC block SCD — SUMMARY calls this out explicitly.
- **Placement is `Source` / `Publisher`, not a new pipeline.** CDC is a `Source` implementation; SCD is a `Publisher`. If they become a parallel pipeline, README §29/§95 extensibility will not hold — which is exactly why the seam was established in Phase 3 and recorded as an ADR.
- **Cheap-now decisions decided here**: PITFALLS #2 — every SCD2 dimension carries a `btree_gist` exclusion constraint on `(business_key, validity range)` **in its creating migration**. Once overlapping intervals exist the constraint can never be added and every as-of query is silently wrong. PITFALLS #6 — SCD corrections **recompute** a key's history from an ordered event log rather than performing in-place interval surgery; in-place surgery is not idempotent and, with at-least-once CDC, drifts permanently.
- Change detection hashes **normalized** content (Phase 6) and every stored hash carries its `hash_version` (Phase 3) — so the recipe can change later without making every dimension appear to change at once.
- Adopt dbt's SCD2 vocabulary: surrogate key **independent** of the change hash, both `timestamp` and `check` change-detection strategies, `hard_deletes = ignore | invalidate | new_record`, and a `valid_to_current` sentinel rather than NULL.
- **Reduce ambition deliberately**: define the CDC event model and prove it with a CSV-delivered feed (operation column + sequence + key). Before-images wait until a real source produces one (v2). Exactly-once is a transport property and no broker is deployed — cite Debezium's at-least-once default, but **re-verify it first-hand** (the official docs page 403'd during research).

### Phase 11: CI/CD Completion & Operations

**Goal**: The whole environment provably rebuilds from the repository, every catalogued failure mode has a passing test, and someone who did not build the platform can operate it
**Mode:** mvp
**Depends on**: Phase 10
**Requirements**: CICD-05, CICD-06, CICD-08, CICD-09, QUAL-15, OBS-06, INFRA-11, INCR-07
**Success Criteria** (what must be TRUE):

  1. A pull request spins up an ephemeral kind cluster in GitHub Actions, deploys the stack from the repository using `values-ci.yaml`, and runs unit, integration and E2E suites green with coverage reported.
  2. Images build and publish tagged by git SHA on every merge, and trivy image and dependency scanning fails the build on a high-severity finding.
  3. Every README §84 failure scenario — pod crash, PostgreSQL unavailable, MinIO unavailable, Vault unavailable, malformed CSV, invalid encoding, OOM, task timeout, duplicate batch, CDC ordering, secret rotation, unauthorized secret access — has a passing test.
  4. The analytical warehouse is dropped and rebuilt from the immutable raw layer plus versioned configuration, and reconciles to its pre-drop state.
  5. Each runbook scenario can be followed by someone who did not build the platform to reach diagnosis, recovery and verification; retention policies prune raw files, processed files, quarantine, validation reports, ingestion metadata and logs independently of processing logic.

**Plans**: TBD

**Research stage**: S13 + S14. **Skip `--research-phase`** — the CI patterns are standard and `values-ci.yaml` already exists from Phase 2.

**Plan guidance**:

- **Wave H: S13 (CI/CD) ‖ S14 (operations) — fully independent**, ~5% of effort. Plan them as concurrent tracks.
- **CI-profile Helm values gate the ephemeral-kind E2E.** This is why Phase 2 wrote both profiles: the full stack does not fit GitHub's 4 CPU / 16 GB runner. Use LocalExecutor in CI, KubernetesExecutor locally. Watch Docker Hub anonymous pull limits — they look like network flakes.
- **Runbooks trail everything by design.** They document *real observed* failure modes; writing them early produces fiction. Every incident from Phases 4–10 should already be a candidate entry.
- INCR-07 (rebuild-from-raw) is the ultimate proof of the Core Value statement — it exercises raw immutability, config versioning, historical schema resolution, idempotency and reconciliation in a single operation. Treat it as the milestone's capstone test.
- Retention (INFRA-11) is enforced separately from processing logic, per README §64/§91, and must not be able to delete raw data that a rebuild still needs.
- Recalibrate the S/M/L/XL effort estimates here — they were judgement, not measurement.

## Requirement Coverage

All **142** v1 requirements map to exactly one phase. No orphans, no duplicates.

| Phase | Requirements | Count |
|-------|--------------|-------|
| 1. Repository, Toolchain & CI Skeleton | QUAL-01, QUAL-02, QUAL-07, QUAL-08, CICD-01, CICD-02, CICD-03, CICD-04, SEC-02, SEC-10, SEC-11, OBS-03 | 12 |
| 2. kind Cluster & Core Infrastructure | INFRA-01, INFRA-02, INFRA-03, INFRA-04, INFRA-05, INFRA-07, INFRA-09, INFRA-10, CICD-07 | 9 |
| 3. `dataplat` Core Library & Metadata Control Plane | META-01, META-02, INFRA-08, SEC-15, CSV-13, SCHEMA-07, OBS-02, OBS-04, OBS-05, QUAL-03 | 10 |
| 4. Vertical Slice — CSV to Analytical PostgreSQL | ORCH-01…ORCH-09, META-03, LOAD-01, LOAD-02, LOAD-03, LOAD-04, LOAD-05, LOAD-08, LOAD-09, LOAD-12, INCR-08, QUAL-05, QUAL-06, QUAL-09 | 22 |
| 5. Vault Secrets & Workload Identity | INFRA-06, SEC-01, SEC-03, SEC-04, SEC-05, SEC-06, SEC-07, SEC-08, SEC-09, SEC-12, SEC-13, SEC-14 | 12 |
| 6. Universal CSV Engine, Schema Contracts & Normalization | CSV-01…CSV-12, SCHEMA-01…SCHEMA-06, LOAD-07, QUAL-04, QUAL-12, QUAL-16, QUAL-17 | 23 |
| 7. Observability, Metrics, Tracing & Lineage | OBS-01, OBS-07, OBS-08, OBS-09, OBS-10 | 5 |
| 8. Validation, Quarantine & Metadata Control-Plane Completion | VALID-01, VALID-02, VALID-03, VALID-04, VALID-07, VALID-08, VALID-09, LOAD-10, LOAD-11 | 9 |
| 9. ETL Correctness — Dedup, Incremental, Backfill & Recovery | DEDUP-01…DEDUP-04, INCR-01…INCR-06, LOAD-06, VALID-05, VALID-06, QUAL-10, QUAL-11 | 15 |
| 10. CDC & Slowly Changing Dimensions | SCD-01…SCD-12, CDC-01, CDC-02, CDC-03, QUAL-13, QUAL-14 | 17 |
| 11. CI/CD Completion & Operations | CICD-05, CICD-06, CICD-08, CICD-09, QUAL-15, OBS-06, INFRA-11, INCR-07 | 8 |
| **Total** | | **142** |

## Parallelization Map

`parallelization: true`. Waves from ARCHITECTURE Q10.2, mapped onto phases.

| Wave | Concurrent work | Why safe | Effort share |
|------|-----------------|----------|--------------|
| A | **Phase 2 (infra) ‖ Phase 3 (library)** | Different artifacts, different harnesses; Phase 3 needs only Docker | ~25% |
| B | Phase 4 (smoke → slice) | **Strictly serial — the critical path. Protect it.** | ~15% |
| C | **Phase 5 (Vault) ‖ Phase 6 (CSV) ‖ Phase 7 (observability)** | Manifests vs. `csv_processor/` vs. Helm + `dataplat/observability` — almost no file overlap | ~20% |
| D | Within Phase 6: filename ‖ encoding ‖ dialect ‖ header/footer ‖ inference | Pure functions over a shared read-only fixture corpus — **the single best parallelization opportunity in the project** | (within C) |
| E | Within Phase 8: validation ‖ metadata completion | Coordinate only on `meta.validation_results` DDL | ~10% |
| F | Within Phase 9: dedup+watermarks ‖ reconciliation → backfill → recovery | Backfill needs watermarks | ~15% |
| G | Within Phase 10: SCD ‖ CDC → CDC→SCD | Publisher work vs. Source work | ~10% |
| H | Within Phase 11: CI/CD ‖ operations | Independent | ~5% |

**Strictly sequential chains (cannot be parallelized):**
`INFRA` → `ORCH` → any E2E · `INFRA`(Vault) → `SEC`(K8s auth + policies) → `SEC`(Airflow backend) ·
`META` schema → {watermarks, dedup audit, validation results, lineage, batch ledger} ·
`CSV` parse → `SCHEMA` → `CSV` normalize → `VALID` → `DEDUP` → `LOAD` ·
`LOAD`(ledger) → idempotency → retries → backfills ·
`LOAD`(staging + merge) → `DEDUP`(cross-batch) → `SCD`(merge) ·
`INCR`(watermark) → backfill correctness · `VALID`(persisted results) → anomaly baselines ·
`INFRA`(CI-profile values) → `CICD`(ephemeral-kind E2E) · everything → `OBS`(runbooks).

## Spikes

| Spike | Phase | Experiment | Pass criteria |
|-------|-------|------------|---------------|
| **U1** — locally-built image pulls and runs via `KubernetesPodOperator` on kind | 4 | Build `csv-processor:<git-sha>` printing its own version; push to the local registry; run via KPO with `do_xcom_push=True` writing `/airflow/xcom/return.json` | **XCom contains the SHA that was built.** Becomes the permanent platform smoke test. Under an hour |
| **U3** — streaming CSV throughput with per-chunk `COPY` under pod limits | 4 | PITFALLS E7. **Not** with `executemany` (10–100× slower than `COPY`; a false negative would change the architecture for no reason). Measure **peak RSS as well as throughput** | A baseline number recorded in the repository; a later 5× regression is treated as a bug, not a mystery |
| **U2** — Vault Kubernetes auth on kind without JWT-issuer overrides | 5 | `make vault-bootstrap` creates auth method, policy, role and K8s RBAC from one variable set | **Both** tests pass: the `csv-processor` SA in `etl` reads its own path (positive) **and** the `default` SA is denied (negative). If the negative test is awkward to write, the identity model is not real. Under half a day. **Check `date` first** — WSL2 clock drift mimics auth failure |

## Cheap-Now / Unrecoverable-Later Decisions by Phase

| # | Decision | Severity | Decided in |
|---|----------|----------|-----------|
| 10 | PV persistence and `extraMounts` in `kind/cluster.yaml` | REWORK (cluster recreation) | **Phase 2** |
| 11 | Kubelet reservations and `maxPods` in the kind config; requests/limits on every chart | REWORK (cluster recreation) | **Phase 2** |
| 1 | `hash_version` alongside every stored change hash | DATA CORRUPTION | **Phase 3** |
| 5 | Stream and chunk in **records** via one `csv.reader` over `newline=""` | DATA CORRUPTION | **Phase 3** |
| 3 | Run-scoped identity (`run_id`, `attempt`) on every staged and loaded row; `UNIQUE (dataset, batch_key)` | DATA CORRUPTION | **Phase 4** |
| 4 | Dynamic Task Mapping expands over a **frozen manifest**, never a live listing | DATA CORRUPTION | **Phase 4** |
| 8 | Business date comes from the **data**, never the clock or `logical_date` | DATA CORRUPTION | **Phase 4** |
| 9 | The processor is the **only** CSV parser (`COPY … FORMAT csv` prohibited) | DATA CORRUPTION | **Phase 4** |
| 12 | Metric labels bounded; unbounded identity in the metadata DB | REWORK | **Phase 4** (rule) → enforced Phase 7 |
| 14 | Single-writer publication via advisory lock + `ON CONFLICT` on the natural key | DATA CORRUPTION | **Phase 4** (shape) → hardened Phase 9 |
| 13 | Explicit `namespace` + `service_account_name` on task pods matched to the Vault role | REWORK + security | **Phase 4** (declared) → Phase 5 (matched) |
| 15 | Fixtures generated from a seed, not committed en masse | REWORK | **Phase 1** |
| 7 | Advance the watermark only from observed **committed** cursor values, lagged | DATA CORRUPTION | **Phase 9** |
| 2 | `btree_gist` exclusion constraint on `(business_key, validity range)` in the creating migration | DATA CORRUPTION | **Phase 10** |
| 6 | SCD corrections **recompute** history from an ordered event log | DATA CORRUPTION | **Phase 10** |

Eleven of the fifteen are *"make the bad state unrepresentable"* rather than *"remember to handle the bad case"* — PITFALLS argues this should be the roadmap's explicit design bias.

## Progress

**Execution Order:** Phases execute in numeric order: 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10 → 11

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Repository, Toolchain & CI Skeleton | 9/9 | Complete    | 2026-08-11 |
| 2. kind Cluster & Core Infrastructure | 0/TBD | Not started | - |
| 3. `dataplat` Core Library & Metadata Control Plane | 0/TBD | Not started | - |
| 4. Vertical Slice — CSV to Analytical PostgreSQL | 0/9 | Planned | - |
| 5. Vault Secrets & Workload Identity | 2/5 | In Progress | - |
| 6. Universal CSV Engine, Schema Contracts & Normalization | 0/17 | Planned | - |
| 7. Observability, Metrics, Tracing & Lineage | 0/8 | Planned | - |
| 8. Validation, Quarantine & Metadata Control-Plane Completion | 0/TBD | Not started | - |
| 9. ETL Correctness — Dedup, Incremental, Backfill & Recovery | 0/TBD | Not started | - |
| 10. CDC & Slowly Changing Dimensions | 0/TBD | Not started | - |
| 11. CI/CD Completion & Operations | 0/TBD | Not started | - |

---
*Roadmap created: 2026-08-11 · Granularity: fine · Mode: mvp · Source: `.planning/research/SUMMARY.md` stages S0–S14*
