<!-- GSD:project-start source:PROJECT.md -->

## Project

**Airflow ETL Platform**

A local, production-like ETL/data platform running on a multi-node **kind** Kubernetes cluster: Apache Airflow orchestrating containerized ETL workloads, MinIO as the S3-compatible data lake, two separate PostgreSQL instances (Airflow metadata vs. analytical warehouse), and HashiCorp Vault for secrets. Its first workload is a **metadata-driven universal CSV ingestion engine** that discovers, inspects, parses, validates, normalizes, deduplicates and transactionally loads real-world messy CSV files — with schema evolution, incremental processing, CDC and SCD support.

It is explicitly **not** an Airflow tutorial, a CSV parser, a bag of scripts, or a Docker Compose dev environment. It is a platform whose architecture lets additional ETL workloads be added later without redesign.

Built as a **foundation for real work** — the intent is to actually run ETL on it and port its patterns into production systems.

**Core Value:** **Every file, batch and record that enters the platform can be traced, explained, reprocessed and trusted** — ingestion is idempotent, auditable and replayable, and no data is ever silently dropped, duplicated or corrupted.

If the platform ingests fast but cannot answer *"where did this row come from, and is it correct?"*, it has failed.

### Constraints

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

<!-- GSD:project-end -->

<!-- GSD:stack-start source:research/STACK.md -->

## Technology Stack

## TL;DR — The Stack

| Layer | Choice | Pinned version | Confidence |
|---|---|---|---|
| Local Kubernetes | kind | `v0.32.0`, node image `kindest/node:v1.35.5` | HIGH |
| Package manager | Helm | `4.2.3` (fallback `3.21.3`) | MEDIUM |
| Orchestrator | Apache Airflow | `3.3.0` on chart `1.22.0` (tag override) | HIGH |
| Executor | KubernetesExecutor (local) / LocalExecutor (CI) | — | HIGH |
| PostgreSQL | CloudNativePG operator | operator `1.30.0`, chart `0.29.0`; PG **17** (Airflow) / PG **18** (analytical) | HIGH |
| Object store | MinIO API via `pgsty/minio` fork image + official chart `5.4.0` | `RELEASE.2026-08-04T00-00-00Z` | MEDIUM |
| Secrets | HashiCorp Vault | chart `0.34.0`, Vault `2.0.3` | HIGH |
| Secret delivery | Vault **Kubernetes auth**, direct SA-token login (Airflow `VaultBackend` + `hvac` in ETL pods) | provider `4.8.0`, `hvac 2.4.0` | HIGH |
| Python | CPython | **3.12** (both images) | HIGH |
| Packaging | uv + src-layout | `uv 0.12.3` | HIGH |
| CSV parsing | stdlib `csv` (streaming) | — | HIGH |
| Encoding detect | BOM sniff → contract → `charset-normalizer` + `chardet` | `3.4.9` / `7.5.1` | HIGH |
| Dialect detect | `clevercsv` (detect only) | `0.8.5` | HIGH |
| Config/contracts | Pydantic v2 (config only, **not** per-row) | `2.13.4` | HIGH |
| DB driver | `psycopg[binary,pool]` v3 — COPY BINARY → staging → MERGE | `3.3.4` | HIGH |
| S3 client | `boto3` | `1.43.68` | HIGH |
| Migrations | Alembic (hand-written revisions) | `1.19.1` | HIGH |
| Testing | pytest + hypothesis + testcontainers | `9.1.1` / `6.165.3` / `4.15.0` | HIGH |
| Metrics | StatsD-exporter → kube-prometheus-stack; business metrics via analytical DB → Grafana Postgres datasource | chart `88.2.0` | MEDIUM |
| Traces | OTel Collector `0.169.0` chart + Grafana Tempo; DIY W3C context injection into KPO pods | — | MEDIUM |
| Lint/type | ruff + mypy | `0.16.2` / `2.3.0` | HIGH |
| Security | trivy + gitleaks (binary, not the action) | `0.73.0` / `8.30.1` | HIGH |

## A. Kubernetes Platform

### Core Technologies

| Technology | Version | Purpose | Why Recommended |
|---|---|---|---|
| kind | `v0.32.0` (2026-06-02) | Local multi-node Kubernetes | Only mandated option (§3.1). 0.32.0 is required to `kind load` the newest node images (containerd config v4). |
| kind node image | `kindest/node:v1.35.5@sha256:ce977ae6d65918d0b58a5f8b5e940429c2ce42fa3a5619ec2bbc60b949c0ac95` | Kubernetes 1.35.5 | **Do NOT take kind 0.32.0's default of v1.36.1.** Airflow 3.3.0's own prerequisites page lists supported Kubernetes as **1.30–1.35**. Pinning 1.35.5 keeps you inside Airflow's tested matrix while staying current. |
| Helm | `4.2.3` (2026-07-09) | Chart installation | Helm 3's final feature release is 2026-09-09 and security support ends Feb 2027 — starting a months-long project on Helm 3 buys an immediate migration debt. The Helm project states Helm-3 charts are deployable by Helm 4. |
| kubectl | `1.36.1` (already installed) | Cluster CLI | Within ±1 minor of server 1.35 — supported skew. |

### kind cluster shape

# kind/cluster.yaml — LOCAL profile

### `kind load docker-image` vs a local registry — **use the local registry**

| | `kind load docker-image` | Local registry container |
|---|---|---|
| Per-iteration cost | Re-tars and re-imports the **whole image** into every node (~2 GB Airflow image × 3 nodes) | Pushes only changed layers, once |
| Multi-node | Loads to each node serially | One push, nodes pull on demand |
| Digest/tag hygiene | Images are node-local; `imagePullPolicy: IfNotPresent` required, easy to serve a stale image silently | Real registry semantics; immutable `:<git-sha>` tags work as in production |
| Production fidelity | None — no such mechanism exists in prod | Mirrors GHCR exactly |

## B. Apache Airflow

### Versions (verified 2026-08-11)

| Artifact | Version | Released | Source |
|---|---|---|---|
| `apache-airflow` | **3.3.0** | 2026-07-06 | PyPI JSON API |
| Official Helm chart | **1.22.0** | 2026-06-13 (docs say 2026-06-01) | `apache/airflow` GitHub tag `helm-chart/1.22.0`; `Chart.yaml` `version: 1.22.0`, `appVersion: 3.2.2` |
| `apache-airflow-task-sdk` | `1.3.0` | 2026-07-06 | PyPI |
| `apache-airflow-providers-cncf-kubernetes` | `10.21.0` (Airflow 3.3.0 constraints pin `10.19.0`) | 2026-08-10 | PyPI + constraints |
| `apache-airflow-providers-hashicorp` | `4.8.0` (constraints pin `4.7.1`) | 2026-08-08 | PyPI + constraints |
| `apache-airflow-providers-postgres` | `7.0.1` | 2026-08-08 | PyPI |
| Docker image | `apache/airflow:3.3.0-python3.12` | — | Docker Hub; **3.12 is the default Python for 3.3.0** |
| Constraints | `https://raw.githubusercontent.com/apache/airflow/constraints-3.3.0/constraints-3.12.txt` | 2026-07-06 | GitHub |

### Architecture — the README diagram is correct for Airflow 3

| Component | Required? | Notes |
|---|---|---|
| **API Server** | Yes | Serves the REST API **and** the UI **and** the Task Execution API. In Airflow 3 it is *the sole metadata-DB access point for tasks and workers*. |
| **Scheduler** | Yes | Executor logic runs **inside** the scheduler process. There is no separate executor process. |
| **DAG Processor** | Yes (now a first-class standalone component; in Airflow 2 it was optional) | Parses + serialises DAGs. |
| **Triggerer** | Optional — but **enable it** | Needed for deferrable operators. `KubernetesPodOperator(deferrable=True)` moves the "wait for pod" phase off worker slots entirely. |
| **Workers** | Depends on executor | KubernetesExecutor → ephemeral pods; Celery → long-running Deployment. |
| Metadata DB | Yes | PostgreSQL 13–17 (see §C). |
| DAG Bundle | Yes | Airflow 3 concept — where DAG files live (git bundle / PVC / baked into image). |

### Airflow 3 migration gotchas that matter for a **greenfield** build

### EXECUTOR — recommendation

| Executor | Local (32 core / 47 GB) | CI (4 CPU / 16 GB) | Verdict |
|---|---|---|---|
| **KubernetesExecutor** | One worker pod per Airflow task, plus the KPO pod → 2 pods per task. Zero idle cost, full isolation, per-task `pod_override` resources, exercises the exact RBAC a production K8s Airflow needs. ~10–20 s pod-start latency per task. | 2 pods/task × Airflow image (~2 GB) is heavy; image pull + disk are the binding constraints, not CPU. | ✅ **Local default** |
| **LocalExecutor** | Task processes run *inside the scheduler*. Airflow 3's Task-SDK isolation means they still cannot reach the DB, so the classic objection is weaker than in Airflow 2. Combined with `KubernetesPodOperator(deferrable=True)`, the scheduler-resident process does almost nothing. | Smallest possible footprint: one scheduler pod. Removes 1 pod + 1 image pull per task. | ✅ **CI default** |
| **CeleryExecutor** | Adds Redis (chart pins `redis:7.2-bookworm` *because of the Redis licence change* — a second dead-catalog trap), long-running worker Deployments consuming RAM while idle, Flower, and KEDA if you want scaling. Buys lower task-start latency you do not need. | Strictly worse. | ❌ Reject |

### KubernetesPodOperator settings that matter here

## C. PostgreSQL

### The Bitnami situation — verified, not assumed

- Bitnami's public catalog switched to a limited hardened subset on **2025-08-28**; versioned Debian images were archived to `docker.io/bitnamilegacy` with **no further updates, fixes or support**. Packaged OCI charts at `docker.io/bitnamicharts` stopped receiving updates.
- `https://charts.bitnami.com/bitnami` still resolves, and the Airflow chart 1.22.0 still declares `postgresql 13.2.24` from it as a subchart — **but** its values now read literally:

### Recommendation: CloudNativePG

| Technology | Version | Released | Purpose |
|---|---|---|---|
| CloudNativePG operator | **`1.30.0`** | 2026-06-29 | PostgreSQL operator |
| `cloudnative-pg` Helm chart (operator) | **`0.29.0`** | 2026-06-29 | Installs the operator + CRDs |
| `cluster` Helm chart | **`0.8.1`** | 2026-07-20 | Optional convenience wrapper for a `Cluster` CR |
| | CloudNativePG 1.30.0 | Zalando postgres-operator 2.0.1 |
|---|---|---|
| Governance | CNCF project, ~2-month minor cadence, published support policy (minor supported until 3 months after N+1) | Single-vendor; **2.0.0 shipped 2026-07-27, 2.0.1 two days later** — a brand-new major, three weeks old |
| Architecture | No external DCS. Uses the Kubernetes API itself for consensus; instance manager is PID 1 in the Postgres pod | Patroni + etcd/K8s DCS + Spilo image — more moving parts |
| Failover primitive | Native `Cluster` CR, declarative `instances:` count | Patroni-mediated |
| Fit for kind | Excellent — `instances: 1` is a first-class supported topology, low memory floor | Heavier baseline |
| Bootstrap ergonomics | `bootstrap.initdb` creates database + owner + `postInitSQL` in one CR — perfect for creating `airflow` DB and the `staging/warehouse/analytics` schemas | Comparable but via a different CRD shape |
| Risk today | Mature, boring | Adopting a 3-week-old x.0.0 major in a project whose value is *correctness* is unforced risk |

### PostgreSQL major versions — **two different majors, deliberately**

| Instance | Version | Why |
|---|---|---|
| **Airflow metadata** | **PostgreSQL 17** | Airflow 3.3.0's prerequisites page lists supported PostgreSQL as **13, 14, 15, 16, 17**. **PG 18 is not on Airflow's supported list.** Take the newest *supported* major. Running an unsupported major under Airflow's Alembic migrations is precisely the kind of unforced risk that produces an unrecoverable metadata DB. |
| **Analytical** | **PostgreSQL 18** | Nothing constrains it, and PG 18 brings four things this workload directly uses: **`uuidv7()`** (time-ordered surrogate keys and batch IDs — §56, §25); **`OLD`/`NEW` in `RETURNING`** for `INSERT`/`UPDATE`/`DELETE`/**`MERGE`** (deduplication auditability §27 and SCD2 change capture §55 become one statement instead of a read-then-write race); **temporal constraints** — `PRIMARY KEY … WITHOUT OVERLAPS` and `FOREIGN KEY … PERIOD` (this is a *database-enforced* SCD2 non-overlapping-validity-interval guarantee, §54/§58, which is otherwise an application invariant you have to test for); **asynchronous I/O** (up to ~3× on sequential and bitmap heap scans — relevant to reconciliation queries §45). |

### Cluster shape

# Airflow metadata

# Analytical

## D. MinIO — **the biggest conflict between the README and 2026 reality**

### Verified facts

| Fact | Evidence |
|---|---|
| `minio/minio` GitHub repo is **archived** | GitHub API: `"archived": true`, `pushed_at: 2026-04-24`. Marked "no longer maintained" 2026-02-12, archived 2026-04-25. |
| Last GitHub release tag | `RELEASE.2025-10-15T17-29-55Z` |
| Last **Docker Hub** community image | `minio/minio:RELEASE.2025-09-07T16-13-09Z` (pushed 2025-09-07). The Oct tag was never published as an image. `minio/minio:latest` has not moved since 2025-09-07. |
| Web console removed from CE | May 2025 — CE retains only a bare-bones object browser; user management, bucket policies, ACLs, lifecycle management were removed. |
| Official Helm chart | `charts.min.io` still serves, but the newest entry is **chart `5.4.0` / appVersion `RELEASE.2024-12-18T13-15-44Z`** (created 2025-01-02). Stale by ~19 months. |
| MinIO Operator | last release **`v7.1.1`, 2025-04-23**. Effectively dead. |
| `quay.io/minio/minio` | Still receives `…hotfix.<sha>` tags (e.g. `RELEASE.2025-09-07T16-13-09Z.hotfix.7aa24e772`, Apr 2026) — these are **commercial-subscriber hotfix builds**, not community artifacts. Do not build on them. |
| Licence | Unchanged: server is still AGPLv3. This is a **maintenance/distribution** death, not a relicensing. |

### Recommendation: keep MinIO, but pin the maintained community fork

- Chart: official `minio/minio` chart **`5.4.0`** from `https://charts.min.io` (it is a plain StatefulSet/Deployment chart; its staleness is tolerable because you override the image).
- Image: **`pgsty/minio:RELEASE.2026-08-04T00-00-00Z`** — the Pigsty community fork (created 2025-10-25 in response to MinIO halting binary distribution). Verified on Docker Hub: `RELEASE.2026-08-04T00-00-00Z` pushed 2026-08-04, with `-amd64`/`-arm64` variants, plus 2026-06-18 and 2026-04-17 releases before it. It rebuilds the last known-good MinIO source, **restores the full admin console**, and applies CVE patches (incl. CVE-2025-62506). Drop-in: swap `minio/minio` for `pgsty/minio`.
- Bucket bootstrap: use the chart's `buckets:` / post-install `mc` Job to create `raw`, `validated`, `processed`, `quarantine`, `metadata`, and set the `raw` bucket to **object-lock / versioning + a deny-delete policy** to enforce §63 immutability at the storage layer rather than by convention.

| Trap | Avoidance |
|---|---|
| Using `minio/minio:latest` | Frozen at 2025-09-07 and will silently never update. Always pin an explicit `RELEASE.*` tag. |
| Expecting the admin console | Gone from CE since May 2025. The `pgsty` fork restores it; if you stay on official images, you must administer via `mc` CLI only. |
| Using the MinIO **Operator** | Last release Apr 2025, dead. Use the plain Helm chart. |
| Using the `minio` **Python SDK** | See §F — it couples your code to MinIO and violates §5's swap-out goal. Use `boto3`. |
| Depending on chart features added after 5.4.0 | There are none; the chart is frozen. Pin `--version 5.4.0`. |
| Single-maintainer fork risk | Real. The `pgsty` fork lives on one person's attention. Mitigation: your §5 abstraction means the escape hatch is a values-file image swap, not a code change. Add a Phase-1 ADR recording this. |

### Alternatives considered (report, do not adopt)

| Option | Assessment |
|---|---|
| Official `minio/minio:RELEASE.2025-09-07T16-13-09Z` | Safest provenance (published by MinIO Inc.), but no console, no CVE patches after Sept 2025. Acceptable fallback if the fork is unacceptable. **This is the conservative choice.** |
| **SeaweedFS** | Apache-2.0, actively developed, integrated S3 API. The strongest genuinely-maintained alternative. Rejected only because §5 names MinIO. Keep as the documented migration target. |
| **Garage** | Very lightweight, good for small clusters. Weaker S3 API surface (limited multipart/versioning edge cases) — risky for §63 immutability via object lock. |
| **Ceph RGW / Rook** | Most production-credible S3, but the operational weight is wildly disproportionate for a kind cluster. |
| **LocalStack S3** | Explicitly a *test double*, not storage. Would fail §90 (data must survive and be rebuildable) and the "production-like" mandate. **Do not use as the data lake.** It is, however, a reasonable extra fixture for *unit* tests where you do not want a container. |

## E. HashiCorp Vault

### Versions (verified 2026-08-11)

| Artifact | Version | Notes |
|---|---|---|
| `hashicorp/vault` Helm chart | **`0.34.0`** (2026-07-02) | `Chart.yaml`: `appVersion: 2.0.3`, `kubeVersion: ">= 1.20.0-0"`. Copyright header now reads **"IBM Corp. 2018, 2026"**. |
| Vault server | **`2.0.3`** (chart default) — latest binary is `2.0.4` (2026-08-04) | Vault went **major to 2.0.0 on 2026-04-14**. This is not a typo; do not assume 1.x. |
| `vault-k8s` (Agent Injector) | `1.7.5` | chart values |
| `vault-csi-provider` | `1.7.3` | chart values |
| Vault Secrets Operator (VSO) | `1.5.0` (2026-07-23) | separate chart |
| External Secrets Operator | `2.9.0` / chart `2.9.0` (2026-08-07/08) | separate project |
| `hvac` (Python client) | `2.4.0` (2025-10-30) | Also what Airflow's constraints pin. |

### Secret delivery — comparison

| Mechanism | How | Fit here |
|---|---|---|
| **Vault Agent Injector** (annotations) | Mutating webhook adds a sidecar that renders secrets to a shared memory volume | Highest resource cost (one Vault client process *per pod*, per-pod Vault connections), highest Vault load. Secret lifecycle tied to pod lifecycle. **Works for KPO pods** because KPO can set pod annotations — but a sidecar on every short-lived ETL pod is 2× the pods and adds startup latency. |
| **Secrets Store CSI Driver + `vault-csi-provider`** | Ephemeral CSI volume mounts secrets as files | Vendor-neutral (good if you had multiple secret stores — you don't). Requires the CSI driver *and* the provider *and* a `SecretProviderClass` per workload. Two extra components for no benefit. |
| **Vault Secrets Operator (VSO)** | CRDs (`VaultStaticSecret`, `VaultDynamicSecret`) sync Vault → Kubernetes Secret; one client per cluster | Lowest Vault load, lowest resource consumption, secret lifecycle **decoupled** from pod lifecycle (supports rotation without pod restart → §81.7). But it materialises a Kubernetes Secret, which §81.5 says must be justified in writing. |
| **External Secrets Operator** | Same shape as VSO, multi-backend | ESO is the better choice only if you need non-Vault backends. You don't. |
| **Direct SA-token login from the app** (`hvac` + Kubernetes auth) | The pod reads `/var/run/secrets/kubernetes.io/serviceaccount/token` and calls `auth/kubernetes/login` | Zero extra infrastructure. Demonstrates workload identity most cleanly. Secret never lands in a K8s Secret at all — the strongest possible answer to §81.5 and §81.13.2/3. |

### **Recommendation: a two-tier pattern**

## F. Python ETL Library

### Python version: **3.12** — for both images

### Packaging: **uv**, src-layout, two dependency sets

| Tool | Version | Verdict |
|---|---|---|
| **uv** | `0.12.3` (2026-08-07) | ✅ **Use it.** Note: **you have 0.8.11 installed — that is 4 minor versions stale; upgrade in Phase 1.** |
| Poetry | `2.4.1` (2026-05-09) | ❌ Reject. **You have 1.8.2 — two majors behind.** Poetry 2.x changed lockfile format and plugin API. |

### CSV parsing engine — **stdlib `csv`**, and this is not a compromise

| Candidate | Version | Verdict for §9–§21, §39 |
|---|---|---|
| **stdlib `csv`** | — | ✅ **Primary engine.** C-implemented RFC-4180 parser (~1–3 M simple rows/s). Row-at-a-time by construction → §39 bounded memory is *structural*, not configured. Full control of `delimiter`/`quotechar`/`escapechar`/`doublequote`/`quoting`/`skipinitialspace`. Handles quoted delimiters, escaped quotes and **multiline fields** correctly (§10). Returns every field as `str`, which is exactly what §12 ("`001234` must not become `1234`"), §14, §15, §16, §17 and §18 require — you own every coercion. And it lets you attach a **row number** to every record for §19's diagnostics, which none of the columnar engines will give you. |
| pandas (chunked) | 3.0.5 | ❌ Type inference is aggressive and hard to fully disable; `low_memory` chunk-boundary dtype inconsistency is a classic silent-corruption source; error rows are dropped or raise for the whole chunk; `dtype=str` everywhere makes it a slow `csv` module. Also: Airflow pins 2.1.4, so having pandas in *both* images invites confusion. |
| Polars (lazy/streaming) | 1.43.2 | ⚠️ Excellent engine, wrong tool. The new streaming engine (from 1.31.1) does handle larger-than-memory via `sink_*`, but `sink_parquet` is still documented as **unstable** ("may change at any point without it being considered a breaking change"), and `scan_csv` assumes a well-formed file with the header at row 0. No per-row error capture. |
| PyArrow `csv` | 25.0.1 | ❌ Very fast, but `ReadOptions`/`ParseOptions` cannot express header-at-row-N + footer + inconsistent quoting, and a single bad row aborts the batch. |
| DuckDB `read_csv` | 1.5.5 | ⚠️ **The one genuinely interesting alternative.** `read_csv(..., store_rejects=true)` populates `reject_scans`/`reject_errors` tables with row-level failure detail — a direct match for §19/§51. But it cannot do encoding detection, arbitrary header offsets, metadata/footer stripping, or conservative typing, and it adds a whole embedded database to the ETL image. |

- Open with `newline=""` — otherwise universal newlines mangle embedded `\r\n` inside quoted fields.
- `csv.field_size_limit(N)` must be set **explicitly** (§39 max field length). The default is 128 KiB and raises `_csv.Error: field larger than field limit (131072)`. Do not `sys.maxsize` it; make it a contract parameter and treat exceeding it as a §51 bad record.
- Streaming from S3: `boto3` `get_object()["Body"]` is a `StreamingBody`; wrap as
- `csv.reader` is ~2× faster than `csv.DictReader`. Use `reader` + a positional index map built once from the detected header.

### Encoding detection (§9)

| Library | Version | Maintained? | Role |
|---|---|---|---|
| **BOM sniff (stdlib)** | — | — | **Step 1, always.** `codecs.BOM_UTF8/BOM_UTF16_LE/BOM_UTF16_BE/BOM_UTF32_*`. A BOM is deterministic evidence; never let a probabilistic detector override it. Confidence `1.0`. |
| **`charset-normalizer`** | `3.4.9` (2026-07-07) | ✅ Yes (repo active 2026-08-05). **Airflow itself pins 3.4.7.** | Primary detector. MIT, pure Python, no build step. Exposes `chaos` (mess) and `coherence` (language) scores. |
| **`chardet`** | `7.5.1` (2026-08-06) | ✅ **Yes — actively maintained again**, repo pushed 2026-08-08. Contradicts the common "chardet is abandoned" belief. | Secondary/cross-check. Returns `{"encoding": ..., "confidence": 0.97}` — **this is literally the JSON shape §9 specifies**, so use chardet's number as the reported `confidence`. |
| `cchardet` | `2.1.7` (2020) | ❌ Dead | Do not use. |
| `faust-cchardet` | `3.2.0` (2026-08-10) | ✅ Maintained fork | Only if profiling proves detection is a bottleneck. It won't be — you detect on a bounded sample. |

### Dialect detection (§10)

| Option | Verdict |
|---|---|
| stdlib `csv.Sniffer` | ❌ **Do not rely on it.** Documented failure modes: raises `_csv.Error: Could not determine delimiter` on single-column files; misidentifies delimiters that appear inside quoted fields; needs a "large enough" sample with no defined bound; `has_header()` is a crude heuristic that fails on all-string headers and on §11's metadata-before-header case. |
| **`clevercsv`** `0.8.5` (2026-05-11, repo active 2026-08-10) | ✅ **Use it — for detection only.** From the Alan Turing Institute; implements a published *data consistency measure* rather than a heuristic, and benchmarks substantially above `Sniffer` on messy real-world files. Drop-in API (`clevercsv.Detector().detect(sample)` → a `SimpleDialect`). |
| Custom | ⚠️ Needed as a thin layer regardless — CleverCSV does not solve §11 (header at row N, metadata block, footer). |

- **Delimiter / quotechar / escapechar** → `clevercsv.Detector` over a bounded sample (first ~64 KiB **plus** a mid-file sample, so a metadata preamble doesn't dominate). Convert the `SimpleDialect` into a stdlib `csv.Dialect` and hand it to `csv.reader` for the actual streaming parse. **Never run CleverCSV over the whole file** — its consistency measure is superlinear-ish and it is a detector, not a parser.
- **Line ending** → sniff `\r\n` vs `\n` vs `\r` from the raw sample; report it but always parse with `newline=""`.
- **Header / metadata / footer (§11)** → your own `detector/header.py`. Strategy: for each candidate row index 0..N, score the row on (a) all fields non-empty, (b) all fields non-numeric, (c) field count equal to the modal field count of the following K rows, (d) uniqueness of values. Pick the first row above threshold; everything before it is the metadata block. Footer detection: trailing rows whose field count differs from the modal count, or whose first field matches a contract-configured `footer_patterns` regex list. Every one of these is contract-overridable (`csv.header_row: 4`, `csv.skip_footer_rows: 1`) — §11 says "configurable **or** detected".

### Data modelling / validation

| Use | Model | Why |
|---|---|---|
| Dataset contracts (§22), YAML configs (§65), quality thresholds (§50) | **Pydantic `BaseModel`**, `model_config = ConfigDict(extra="forbid", frozen=True)` | YAML → validated typed object with precise error locations. `extra="forbid"` catches config typos, which is the single most common ETL outage cause. `frozen=True` supports §67 determinism. |
| Validation reports (§23) | **Pydantic `BaseModel`** + `model_dump_json()` | §23 demands *machine-readable* reports. `model_json_schema()` gives you a publishable JSON Schema for free, and the report shape becomes a versioned contract. |
| Schema/config **hashing** (§13, §55, §66) | `hashlib.sha256(model.model_dump_json(exclude_none=True).encode())` over a canonicalised dump | Deterministic schema hash + config version, required by §13 and §62. Sort keys explicitly. |
| **Per-row records (the hot path)** | ❌ **NOT Pydantic** | Constructing a Pydantic model per row costs ~1–3 µs; at 10 M rows that's 10–30 s of pure overhead plus GC pressure, and it fights §51 (Pydantic wants to raise, you want to collect and continue). Use `tuple[str, ...]` from `csv.reader` plus a hand-written per-column validator table, and `@dataclass(slots=True, frozen=True)` for the small number of per-row *error* objects you actually materialise. |

### Date & time (§14) — **explicit formats, never inference**

| Library | Version | Role |
|---|---|---|
| `datetime.strptime` (stdlib) | — | ✅ **The parser.** Strict by construction: rejects `2026-02-30`, `2026-13-01`, `31/02/2026` with `ValueError` — precisely §14's requirement that invalid dates "must produce explicit validation errors". |
| **`pendulum`** | `3.2.0` | ✅ Timezone arithmetic, DST-correct interval maths, and Airflow's own datetime type. Use for §31's five time concepts and for anything touching `logical_date`/`data_interval`. **Not** for parsing untrusted strings. |
| `python-dateutil` | `2.9.0.post0` | ⚠️ **Never in the load path.** `dateutil.parser.parse` *guesses*: `03/04/2026` is March 4 or April 3 depending on `dayfirst`, and it happily accepts garbage. That is the exact behaviour §14 forbids. Permitted in exactly one place: `detector/schema.py` schema **profiling**, to *suggest* candidate formats to a human writing a contract — and the suggestion must be recorded, reviewed, and written into the contract, never applied silently. |

### Numerics (§15) — `decimal.Decimal`, applied selectively

- Use `decimal.Decimal` for every contract column typed `decimal`. Never `float` for money or identifiers. `psycopg` v3 adapts `Decimal` ↔ PostgreSQL `numeric` natively in both directions, including in `COPY`.
- Set an explicit `decimal.Context(prec=…, traps=[InvalidOperation, DivisionByZero])` and never mutate the global context (it is thread-local and a global-mutation hazard).
- Normalisation is a **contract-declared locale profile**, not `locale.setlocale` (which is process-global and not thread-safe):
- §15's warning "do not confuse CSV delimiters with decimal separators" is handled structurally: dialect detection happens *before* numeric normalisation, and a contract whose `delimiter` equals a column's `decimal_separator` must be **rejected at contract-validation time**. Make that an explicit Pydantic model validator.
- Performance note: `Decimal` arithmetic is roughly 10–20× slower than `float`. That is fine because you only *parse* per row and never aggregate in Python — aggregation happens in PostgreSQL (§45/§46).
- Map to `NUMERIC(p,s)` in DDL. §12's `001234` case: contract type `string` with `NOT NULL`, stored as `text`/`varchar` — never `integer`.

### PostgreSQL driver & bulk loading

| Option | Verdict |
|---|---|
| **psycopg 3** | ✅ Native `COPY` via `cursor.copy()` as a context manager with `write_row()` — including **`FORMAT BINARY`** with automatic Python→PG type adaptation. Server-side cursors, client-side binding, real async, first-class `Decimal`/`datetime`/`UUID` adapters, `ConnectionPool`. Actively developed. |
| psycopg 2 | ❌ Maintenance-only. (Airflow's own constraints still pin `psycopg2-binary==2.9.12` for its metadata connection — irrelevant to you, because that lives in a *different image*.) |
| asyncpg `0.31.0` | ❌ Async-only, which buys nothing for a batch ETL that is I/O-bound on one big COPY. `copy_records_to_table` exists but the type-adaptation story for `Decimal`/`numeric` is weaker, and mixing an async driver into a synchronous streaming pipeline adds real complexity for zero measured gain. |
| SQLAlchemy `2.0.51` | ⚠️ Use it for **DDL, Alembic metadata and query construction** — not for row loading. `session.add_all()` / `bulk_insert_mappings` is 10–50× slower than COPY and is the classic ETL performance trap. |
| `connectorx` `0.4.5` | ❌ Read-optimised (DB → DataFrame). Wrong direction. |

- `UNLOGGED` staging table: ~2–3× faster COPY, no WAL, and it is transient by definition. `ON COMMIT DROP` guarantees cleanup even on crash (§37).
- `FORMAT BINARY` is faster but requires *exact* type agreement between what you write and the column types — it will not coerce. Start with text-format COPY (safer, easier to debug), and switch a column-typed fast path to BINARY once the schema is stable and covered by tests. Note this in the plan; do not start with BINARY.
- `MERGE … RETURNING merge_action()` (PG 17) + `OLD`/`NEW` (PG 18) turn §27 deduplication auditability and §55 SCD change detection into a single statement that reports exactly what it did. This is a genuinely large simplification versus the read-compare-write loop most ETL code uses.
- Idempotency key (§24): a `UNIQUE (dataset, batch_id, source_record_id)` constraint on the staging/ledger table plus `ON CONFLICT DO NOTHING` is the belt to MERGE's braces.

### S3 client — **`boto3` 1.43.68**

| Option | Verdict |
|---|---|
| **`boto3`** | ✅ **Use it.** §5 mandates S3 semantics *and* explicitly wants MinIO → AWS S3 swappable. boto3 with `endpoint_url` is the canonical way to express that: change one env var and you're on real S3. `get_object()["Body"]` is a true streaming `StreamingBody`; `upload_fileobj` does multipart automatically. It is also what `apache-airflow-providers-amazon` uses, so an Airflow `aws_conn_id` maps directly onto the same credentials. |
| `minio` SDK `7.2.20` | ❌ **Reject.** Couples your code to MinIO — the exact opposite of §5's stated goal — and MinIO is now unmaintained upstream. Last release 2025-11-27. |
| `s3fs` `2026.7.0` + fsspec | ⚠️ Optional convenience only. It adds an fsspec caching layer whose read-ahead behaviour can quietly break §39's bounded-memory guarantee. Useful if you later hand Parquet paths to Polars/DuckDB; never for the primary streaming reader. |
| Airflow `ObjectStoragePath` (`airflow.sdk`) | ⚠️ Nice for **DAG-side** discovery (§40) since it lives in the Airflow image. Not available in the csv-processor image (no Airflow dependency). Use boto3 in the library, `ObjectStoragePath` in DAGs if convenient. |

### Migrations for the analytical DB — **Alembic `1.19.1`**

| Option | Verdict |
|---|---|
| **Alembic** | ✅ Same language and toolchain as everything else; autogenerate against SQLAlchemy metadata; and it is what Airflow itself uses, so the operational vocabulary (`upgrade head`, `downgrade`, `stamp`) is already in the project. |
| sqitch | ❌ Excellent tool, but Perl — a second runtime in the image and in CI for no gain. |
| plain SQL + version table | ❌ You will re-implement Alembic, badly, and without downgrade paths. |

- **Hand-write every revision.** Use `alembic revision --autogenerate` only to produce a *draft*. Autogenerate will not emit: partial indexes, exclusion constraints, `PRIMARY KEY … WITHOUT OVERLAPS` (your SCD2 temporal guarantee), `CHECK` constraints with expressions, or any `MERGE`-supporting index. All of those are load-bearing here.
- **Never point Alembic at the Airflow metadata DB.** Two separate Alembic environments would be a §4 violation waiting to happen; the analytical Alembic env should hard-fail if its connection string resolves to the Airflow database.
- Run migrations as a Kubernetes **Job** (or an Argo-style pre-install hook) gated in CI, not from inside a DAG task. §35's transactional guarantees do not extend to DDL you run mid-pipeline.
- Guard against §13 schema evolution racing migrations: the `metadata.schema_versions` table is *data* (managed by the ETL) and is distinct from the Alembic revision (which is *structure*).

### Logging (§70)

## G. Testing

### Core tools

| Tool | Version | Purpose |
|---|---|---|
| `pytest` | `9.1.1` (2026-06-19) | Runner. Note this is a **major bump** — check plugin compatibility. |
| `pytest-cov` | `7.1.0` | Coverage (§76). |
| `pytest-xdist` | `3.8.0` | `-n auto` on a 32-core box makes the unit suite near-instant. **Do not** use it for testcontainers integration tests without container-per-worker isolation. |
| `hypothesis` | `6.165.3` | Property-based tests (§72). |
| `testcontainers[postgres,minio]` | `4.15.0` (2026-07-24) | Integration tests. Both `postgres` and `minio` extras confirmed present in the 47 published extras. |
| `syrupy` | current | Snapshot-assert the §23 validation report JSON. Report shape is a contract; snapshots catch accidental changes. |
| `time-machine` | current | Freeze time for §67 determinism tests. Faster and more correct than `freezegun`. |

### Property-based tests worth writing (§72)

### Testing Airflow 3 DAGs

- **Import/structure tests (fast, no DB):** `DagBag(dag_folder="dags", include_examples=False)` and assert `import_errors == {}`, plus structural assertions (every task has `retries`, every KPO has `container_resources`, no `datetime.now()` in default_args). Run these on every PR — they catch 80 % of DAG bugs in <2 s.
- **Behavioural tests:** **`dag.test()`** is the Airflow 3 entry point (CLI equivalent: `airflow dags test <dag_id> <logical_date>`). It executes a real DagRun in-process. It needs a metadata DB — provide one via a session-scoped `testcontainers` PostgreSQL and `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`, plus `AIRFLOW__CORE__EXECUTOR=LocalExecutor`.
- **`pytest-airflow`: do not use.** There is no maintained plugin; the Airflow project's own guidance is `dag.test()` plus plain pytest.
- Set `filterwarnings = ["error::DeprecationWarning:airflow.*"]` so any Airflow-2-era import fails the build (see §B gotcha 1).
- Mock the KPO in unit tests: patch `KubernetesPodOperator.execute` and assert on the constructed pod spec (image tag, SA name, resources, env). Real pod execution belongs in E2E only.

### E2E against kind in CI

## H. Observability — honest assessment

### Metrics

| Component | Version | Notes |
|---|---|---|
| `kube-prometheus-stack` | **`88.2.0`** (2026-08-07) | `appVersion: v0.93.0` (Prometheus Operator). Bundles Grafana subchart **`12.10.4`**. |
| ⚠️ Grafana chart repo moved | `https://grafana-community.github.io/helm-charts` | Not `grafana.github.io`. Update any pinned repo URL. |
| `statsd-exporter` | `v0.30.0` | Shipped by the Airflow chart. |

- **Airflow's own metrics → StatsD → `statsd-exporter` → Prometheus.** (`statsd.enabled: true`, `otelCollector.metricsEnabled: false`.) Mature path, existing community Grafana dashboards, a well-understood mapping config. OTel metrics from Airflow work but you would be re-deriving every dashboard for no gain.
- **Business metrics (§82: `files_processed`, `rows_invalid`, `rows_deduplicated`, `data_freshness`…) → the analytical database, surfaced through Grafana's PostgreSQL datasource.** This is the important call:

### Traces — what is real vs aspirational

- `pip install 'apache-airflow[otel]'`, then `[traces] otel_on = True`, `otel_application = airflow`.
- The SDK is configured with **standard OTel environment variables** (`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_PROTOCOL`, …). The older `otel_host`/`otel_port`/`otel_service`/`otel_debugging_on` keys are **deprecated**.
- **Airflow-managed task spans are created automatically.**
- A **public** DAG-author API exists: `from airflow.sdk.observability import trace` → `trace.get_tracer(__name__)` → `tracer.start_as_current_span(...)`. Custom spans auto-nest under the Airflow task span. No-op tracer when disabled, so zero overhead when off.
- The Airflow chart 1.22.0 ships an **optional bundled OTel Collector** (`otelCollector.tracesEnabled: true`, image `otel/opentelemetry-collector-contrib:0.70.0`, with a `config:` override).
- **Trace context does NOT propagate into `KubernetesPodOperator` pods automatically.** The docs cover only in-task custom spans. You must do it yourself.
- Scheduler/DAG-processor internal span coverage is thin. Do not expect a full "why was this task queued for 4 minutes" trace out of the box.
- There is **no tracing backend in `kube-prometheus-stack`.** You must add **Grafana Tempo** (`grafana/tempo-distributed` or the single-binary `tempo` chart) or Jaeger. Budget for it.

# In the TaskFlow task, before launching the pod:

# In the csv-processor entrypoint:

## I. CI/CD

### Actions & tools (latest verified 2026-08-11)

| Tool | Version |
|---|---|
| `helm/kind-action` | **`v1.14.0`** (2026-02-17) |
| `actions/checkout` | `v7.0.1` |
| `actions/setup-python` | `v7.0.0` |
| `astral-sh/setup-uv` | `v9.0.0` |
| `docker/setup-buildx-action` | `v4.2.0` |
| `docker/build-push-action` | `v7.3.0` |
| `azure/setup-helm` | `v5.0.1` |
| `aquasecurity/trivy-action` | `v0.36.0` (trivy `0.73.0`) |
| `kubeconform` | **`0.8.0`** (2026-06-04) |
| `gitleaks` | **`8.30.1`** (2026-03-21) |
| `trufflehog` | `3.96.0` (2026-07-24) |
| `ruff` | **`0.16.2`** (2026-08-07) |
| `mypy` | **`2.3.0`** (2026-07-13) |

### Ephemeral kind on a 4 CPU / 16 GB runner

### Manifest validation

| Tool | Verdict |
|---|---|
| **`kubeconform 0.8.0`** | ✅ **Use it.** Fast, actively maintained, supports OpenAPI schema locations for CRDs. |
| `kubeval` | ❌ **Effectively abandoned.** The `instrumenta` project has not shipped a release in years and its bundled schemas are stale; it cannot validate modern API versions. Do not use. |
| `helm template \| kubectl apply --dry-run=server` | ✅ **Also use it — it catches different things.** Server-side dry-run validates against the *live* API server including CRDs and admission webhooks. Run it inside the ephemeral kind job. |
| kubeconform -strict -summary -kubernetes-version 1.35.5 \

### Linting & typing

### Security scanning

| Concern | Tool | Recommendation |
|---|---|---|
| Image vulnerabilities (§88, §110) | **trivy `0.73.0`** | `trivy image --severity HIGH,CRITICAL --exit-code 1 --ignore-unfixed` on both images. Also `trivy fs .` for dependency CVEs and `trivy config .` for Dockerfile/K8s misconfigurations (catches root user, missing resource limits, privileged containers — direct §77/§85 checks). Maintain a dated, justified `.trivyignore`. |
| Secret scanning (§81.11) | **gitleaks `8.30.1`** as the blocking gate | Fast, scans **full git history** (`--log-opts` / `detect`), deterministic, easy `.gitleaks.toml` allowlist for the synthetic fixture corpus. ⚠️ **Trap:** `gitleaks/gitleaks-action@v3` requires a `GITLEAKS_LICENSE` for organization-owned repos (free for individuals/public repos). **Sidestep it entirely — download and run the gitleaks binary in a `run:` step.** No licence, no rate limit, pinned version. |
| Secret verification | **trufflehog `3.96.0`** | Complements rather than replaces gitleaks: it *verifies* candidate credentials against live APIs, so `--only-verified` has near-zero false positives. Slower and needs egress. Run it on a **scheduled** full-history job, not on every PR. |
| Dependency CVEs | GitHub **Dependabot** + `trivy fs` | Dependabot is zero-infrastructure and opens PRs against `uv.lock`. `pip-audit` is redundant alongside trivy fs. |
| §81.9 CI credentials | GHCR + `GITHUB_TOKEN` with `permissions: {packages: write, contents: read}` | No long-lived registry credentials at all — this satisfies §81.9/§94.99 by construction. Add OIDC only if you ever deploy off-runner. |

### Container build (§77)

# docker/csv-processor/Dockerfile

- Multi-stage: build tools never reach the runtime layer.
- `USER 1000` (numeric, so Kubernetes `runAsNonRoot` can verify it) → §77, §88.
- `--mount=type=cache` for uv + `cache-from/to: type=gha` in buildx → fast CI.
- Tag `ghcr.io/<owner>/csv-processor:<git-sha>` **and** a semver tag on release. **Never `:latest`** (§77).
- Add OCI labels: `org.opencontainers.image.revision=<sha>`, `.source`, `.version` — these are what make §62 replayability ("which processor version produced this row?") actually answerable, and the label value should be baked into the `metadata.ingestion_runs.processor_version` column.
- No `apache-airflow` in this image. No secrets, no `.env`, no fixture credentials (§77, §81.13.4). Verify with a CI step: `trivy image --scanners secret --exit-code 1`.

## What NOT to Use

| Avoid | Why | Use Instead |
|---|---|---|
| Bitnami charts / `docker.io/bitnami` images | Free catalog removed 2025-08-28; `bitnamilegacy` receives no updates or CVE fixes; `bitnamicharts` OCI charts frozen | CloudNativePG for Postgres; official upstream charts elsewhere |
| Airflow chart's bundled `postgresql` subchart | Points at `bitnamilegacy/postgresql:16.1.0-debian-11-r15`; upstream itself says "not recommended for production"; also violates §4 | `postgresql.enabled: false` + CNPG `Cluster` CRs |
| `minio/minio:latest` or any unpinned MinIO tag | Frozen at `RELEASE.2025-09-07T16-13-09Z`; repo archived | Pinned `pgsty/minio:RELEASE.2026-08-04T00-00-00Z` |
| MinIO **Operator** | Last release v7.1.1 (Apr 2025); abandoned | Plain MinIO Helm chart `5.4.0` with image override |
| `minio` Python SDK | Couples code to MinIO, defeating §5's swap-out requirement; upstream unmaintained | `boto3` with `endpoint_url` |
| LocalStack as the data lake | It is a test double, not storage — fails §90 rebuildability and "production-like" | MinIO (or SeaweedFS) |
| `CeleryExecutor` | Adds Redis (itself pinned to `7.2-bookworm` due to Redis's licence change), idle workers, Flower — for latency you don't need | KubernetesExecutor (local) / LocalExecutor (CI) |
| `SequentialExecutor` | Removed in Airflow 3 | LocalExecutor |
| `csv.Sniffer` as the dialect detector | Fails on single-column files, quoted delimiters, small samples; `has_header()` breaks on §11's metadata-before-header case | `clevercsv.Detector` for detection, stdlib `csv` for parsing |
| `dateutil.parser.parse` on production data | Guesses ambiguous formats — exactly what §14 forbids | Contract-declared `strptime` format lists |
| `cchardet` | Last release 2020; no wheels for modern Python | `charset-normalizer` + `chardet` (both maintained), or `faust-cchardet` if speed ever matters |
| Pydantic models per CSV row | ~1–3 µs/row construction cost; raises instead of collecting, fighting §51 | Pydantic for config/contracts/reports only; tuples + hand-written validators in the hot path |
| pandas / Polars / PyArrow as the primary CSV reader | Type inference fights §12; no row-level error capture for §19; header-at-row-0 assumption breaks §11 | stdlib `csv`; DuckDB later for reconciliation only |
| SQLAlchemy ORM for row loading | 10–50× slower than COPY | `psycopg` `cursor.copy()` → staging → `MERGE` |
| psycopg **pipeline mode** for bulk load | PostgreSQL does not support `COPY` in pipeline mode (verified) | `COPY` in an explicit transaction |
| `asyncpg` | Async-only, weaker Decimal/COPY ergonomics, no benefit for batch ETL | `psycopg[binary,pool]` v3 |
| Prometheus **Pushgateway** for ETL metrics | Short-lived pods → permanently stale series; per-file labels explode cardinality | Persist run metadata to the analytical DB; Grafana PostgreSQL datasource |
| `kubeval` | Abandoned; stale schemas | `kubeconform 0.8.0` + `kubectl apply --dry-run=server` |
| `gitleaks/gitleaks-action@v3` for org repos | Requires a paid `GITLEAKS_LICENSE` | Run the gitleaks binary in a `run:` step |
| `pytest-airflow` | Unmaintained | `dag.test()` + plain pytest |
| Poetry 1.8.2 (currently installed) | Two majors behind (2.4.1); slow resolution; poor fit for Airflow constraints files | `uv 0.12.3` |
| `ty` (Astral type checker) | `0.0.70` — pre-1.0 | `mypy 2.3.0` |
| Airflow's own metadata DB for ETL metadata | §4 violation, and Airflow 3 forbids task-side DB access anyway | `metadata.*` schema in the analytical DB |
| Hand-rolled infra manifests | Explicitly excluded by your locked decisions | Pinned upstream charts + committed values files |

## Version Compatibility Matrix

| Component | Pin | Compatible with | Notes / gotchas |
|---|---|---|---|
| Airflow `3.3.0` | image `apache/airflow:3.3.0-python3.12` | Python 3.10–3.14; PostgreSQL **13–17**; Kubernetes **1.30–1.35** | **PG 18 is NOT supported for the metadata DB.** |
| Airflow chart `1.22.0` | `appVersion: 3.2.2` | Airflow 2.11+ and 3.0+ | Override to 3.3.0; verify the migration Job. Chart `2.0.0`/`app 3.3.0` is on `main`. Documented min Helm **3.19.0**. |
| kind `v0.32.0` | node `v1.35.5@sha256:ce977ae6d…` | K8s 1.33–1.36 images shipped | Default is **1.36.1 — do not use it** (outside Airflow's range). kubeadm `v1beta3` patches for ≤1.35, `v1beta4` for ≥1.36. |
| kubectl `1.36.1` | server 1.35.5 | ±1 minor skew — OK | |
| Helm `4.2.3` | Helm-3 charts | Generally compatible | Server-side apply is the new default; `--atomic`→`--rollback-on-failure`, `--force`→`--force-replace`. **Verify in Phase 1.** |
| CNPG `1.30.0` / chart `0.29.0` | PostgreSQL 13–18 | Minor supported until 3 months after N+1 | `cluster` chart `0.8.1` defaults to PG **16** — override. |
| Vault chart `0.34.0` | Vault `2.0.3`, `kubeVersion >= 1.20.0-0` | vault-k8s `1.7.5`, csi-provider `1.7.3` | Vault major is **2.x**, not 1.x. BUSL-1.1. |
| `providers-hashicorp 4.8.0` | Vault KV v1/v2 | `auth_type: kubernetes` supported in code, **undocumented on the docs page** | Requires `kubernetes_role`; JWT path defaults to the SA token. |
| `providers-cncf-kubernetes 10.21.0` | Airflow 3.3.0 (constraints pin `10.19.0`) | `kubernetes==36.0.2` | Pin to the constraints version in the Airflow image unless you need a newer feature. |
| `psycopg 3.3.4` | PostgreSQL 17 & 18 | `COPY` ✅, pipeline ✅ — but **not together** | `[binary]` extra avoids needing libpq headers. |
| Python `3.12` | Everything above | `charset-normalizer 3.4.9`, `pydantic 2.13.4`, `boto3 1.43.68` | Airflow constraints exist for 3.10–3.14; 3.12 is the image default. |
| `kube-prometheus-stack 88.2.0` | Prometheus Operator `v0.93.0` | Grafana subchart `12.10.4` from `grafana-community.github.io` | Repo URL changed. |
| Airflow metrics | StatsD **XOR** OTel | — | Enabling `otelCollector.metricsEnabled` disables statsd. |
| Airflow constraints `3.3.0` | pins `pandas==2.1.4`, `psycopg2-binary==2.9.12`, `polars==1.42.1`, `hvac==2.4.0` | — | **Do not install `csv_processor` into the Airflow image** — these pins would become yours. |

## Conflicts Between the README and 2026 Ecosystem Reality

| README requirement | Reality | Resolution | Severity |
|---|---|---|---|
| §5 "Use MinIO" | `minio/minio` archived 2026-04-25; last CE image 2025-09-07; console removed May 2025; Operator dead | Pin `pgsty/minio` fork image behind the official chart; treat the S3 client as a hard abstraction seam; write an ADR naming SeaweedFS as the migration target | **HIGH — needs an explicit decision** |
| §3 "most recent and stable versions" (README line 3) | kind's newest node image (K8s 1.36.1) is outside Airflow 3.3.0's supported range (1.30–1.35) | Pin `kindest/node:v1.35.5` — "most recent **supported**", not "most recent" | MEDIUM |
| §92 Phase 2 "Deploy Airflow + Airflow PostgreSQL" | The chart's bundled Postgres is `bitnamilegacy` and self-disclaimed | `postgresql.enabled: false` + CNPG; two explicit `Cluster` CRs | MEDIUM (also *helps* §4) |
| §81.1 "HashiCorp Vault" | Vault is BUSL-1.1 and now IBM-owned; major version is 2.x | Fine for local/non-competing use. ADR noting OpenBao as an API-compatible OSI-licensed escape hatch | LOW |
| §82/§109 OTel tracing | Task spans + a custom-span API are real in 3.3.0; **cross-process propagation into KPO pods is not** | Implement W3C `traceparent` injection via `env_vars` explicitly (recipe in §H) | MEDIUM |
| §82 metrics list (`files_processed`, `rows_invalid`, …) | Airflow can only emit to one metrics backend; ETL pods are too short-lived to scrape | Airflow → StatsD → Prometheus; business metrics → analytical DB → Grafana Postgres datasource | LOW (this is the better design anyway) |
| §72 "Docker/Compose forbidden as workload platform" + CI E2E | Full local stack does not fit a 4 CPU / 16 GB runner | Two Helm values profiles from day one (`values-local.yaml` / `values-ci.yaml`); single-node kind + LocalExecutor + no monitoring in CI | MEDIUM (already anticipated in PROJECT.md) |
| §77 "avoid `:latest`" + Helm | Helm 3 EOL Feb 2027 | Adopt Helm 4.2.3 now, with a Phase-1 compatibility gate | LOW |

## Installation

# --- Host tooling (WSL, ext4 repo path) ---

# (poetry 1.8.2 can be uninstalled; it is not used)

# --- Helm repositories (pinned on install, never `helm repo update` blindly) ---

# --- Pinned chart versions (put these in a single Makefile/`versions.env`) ---

# apache-airflow/airflow            1.22.0   (image tag override -> 3.3.0)

# cnpg/cloudnative-pg               0.29.0   (operator 1.30.0)

# minio/minio                       5.4.0    (image override -> pgsty/minio:RELEASE.2026-08-04T00-00-00Z)

# hashicorp/vault                   0.34.0   (vault 2.0.3)

# prometheus/kube-prometheus-stack  88.2.0

# open-telemetry/opentelemetry-collector 0.169.0

# --- Python: ETL library (csv-processor image) ---

# --- Python: Airflow image (separate Dockerfile, constraints-resolved) ---

# uv pip install "apache-airflow==3.3.0" \

#   "apache-airflow-providers-cncf-kubernetes==10.19.0" \

#   "apache-airflow-providers-hashicorp==4.7.1" \

#   "apache-airflow-providers-postgres" "apache-airflow-providers-standard" \

#   --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.0/constraints-3.12.txt

## Sources

- PyPI JSON API (`https://pypi.org/pypi/<pkg>/json`) — `apache-airflow` 3.3.0 (`requires_python: !=3.15,>=3.10`), all provider packages, psycopg 3.3.4, polars 1.43.2, duckdb 1.5.5, pyarrow 25.0.1, charset-normalizer 3.4.9, chardet 7.5.1, clevercsv 0.8.5, pydantic 2.13.4, boto3 1.43.68, s3fs 2026.7.0, minio 7.2.20, sqlalchemy 2.0.51, alembic 1.19.1, hypothesis 6.165.3, testcontainers 4.15.0 (+ extras list), pytest 9.1.1, ruff 0.16.2, mypy 2.3.0, pendulum 3.2.0, python-dateutil 2.9.0.post0, hvac 2.4.0, uv 0.12.3, poetry 2.4.1, structlog 26.1.0, opentelemetry-* 1.44.0, cchardet 2.1.7, faust-cchardet 3.2.0, ty 0.0.70
- GitHub Releases API — `apache/airflow` (3.3.0 @ 2026-07-06; `helm-chart/1.22.0` @ 2026-06-13), `kubernetes-sigs/kind` v0.32.0 + full release-notes body (node-image digests, kubeadm v1beta4, Envoy LB), `helm/helm` 4.2.3 / 3.21.3, `helm/kind-action` v1.14.0, `cloudnative-pg/cloudnative-pg` v1.30.0, `cloudnative-pg/charts` cloudnative-pg-v0.29.0 / cluster-v0.8.1, `zalando/postgres-operator` v2.0.1 (2026-07-29), `hashicorp/vault-helm` v0.34.0, `hashicorp/vault` v2.0.4, `hashicorp/vault-secrets-operator` v1.5.0, `external-secrets/external-secrets` v2.9.0, `minio/operator` v7.1.1 (2025-04-23), `minio/minio` RELEASE.2025-10-15, `prometheus-community/helm-charts` kube-prometheus-stack-88.2.0, `open-telemetry/opentelemetry-helm-charts` 0.169.0, `aquasecurity/trivy` v0.73.0, `gitleaks/gitleaks` v8.30.1, `trufflesecurity/trufflehog` v3.96.0, `yannh/kubeconform` v0.8.0, plus all GitHub Action `releases/latest`
- GitHub Repos API — `minio/minio` `"archived": true`, `pushed_at: 2026-04-24`; `chardet/chardet` active (`pushed_at: 2026-08-08`); `jawah/charset_normalizer` active; `alan-turing-institute/CleverCSV` active (`2026-08-10`)
- Raw file reads — `apache/airflow` `chart/Chart.yaml` @ `helm-chart/1.22.0` (`version: 1.22.0`, `appVersion: 3.2.2`, bitnami postgresql `13.2.24` dependency); `chart/values.yaml` (`executor: "CeleryExecutor"` default, `postgresql.image.repository: bitnamilegacy/postgresql`, `otelCollector.{tracesEnabled,metricsEnabled}`, the statsd/OTel mutual-exclusion comment, `redis 7.2-bookworm` licence pin); `constraints-3.3.0/constraints-3.12.txt` (pandas 2.1.4, psycopg2-binary 2.9.12, providers pins); `hashicorp/vault-helm` `Chart.yaml` @ v0.34.0 (`appVersion: 2.0.3`, IBM copyright) and `values.yaml` (vault-k8s 1.7.5, csi-provider 1.7.3); `providers/hashicorp/.../vault_client.py` @ `providers-hashicorp/4.8.0` (**`auth_type: kubernetes` with `kubernetes_role` / `kubernetes_jwt_path` — verified in source**); `cloudnative-pg/charts` `cluster` values (`postgresql: "16"` default); `kube-prometheus-stack` Chart.yaml (`appVersion v0.93.0`, grafana subchart 12.10.4 from grafana-community.github.io)
- Registry APIs — Docker Hub `minio/minio` tags (newest `RELEASE.2025-09-07T16-13-09Z`, 2025-09-07); Docker Hub `pgsty/minio` tags (newest `RELEASE.2026-08-04T00-00-00Z`, 2026-08-04); quay.io `minio/minio` (hotfix-only tags); `https://charts.min.io/index.yaml` (max chart 5.4.0 / appVersion RELEASE.2024-12-18)
- `https://www.postgresql.org/versions.json` — PG 18.4 `current`; 17/16/15 supported; 14 EOL 2026-11-12
- `airflow.apache.org/docs/apache-airflow/stable/installation/prerequisites.html` — Python 3.10–3.14; **PostgreSQL 13–17**; **Kubernetes 1.30–1.35**
- `.../logging-monitoring/traces.html` — `[traces] otel_on`, standard `OTEL_*` env vars, deprecated keys, `airflow.sdk.observability.trace`, auto-nesting custom spans
- `.../core-concepts/executor/index.html` — executor taxonomy; executor runs inside the scheduler; multi-executor since 2.10
- `.../core-concepts/overview.html` — Airflow 3 required/optional components; api-server as sole metadata-DB access point
- `.../installation/upgrading_to_airflow3.html` — `airflow.sdk` imports, DB access restrictions, `execution_date` removal, Dataset→Asset, SubDAG/SLA removal
- `airflow.apache.org/docs/helm-chart/stable/release_notes.html` — chart 1.22.0 (2026-06-01, app 3.2.2), 1.21.0, 1.20.0, 1.19.0, 1.18.0
- `kind.sigs.k8s.io/docs/user/local-registry/` — registry container + `containerdConfigPatches` + `certs.d/hosts.toml`
- `developer.hashicorp.com/vault/docs/deploy/kubernetes/comparisons` (Apr 2026) — VSO lowest load/consumption + pod-independent secret lifecycle; Agent Injector highest
- `psycopg.org/psycopg3/docs/{basic/copy,advanced/pipeline}.html` — COPY BINARY; **COPY unsupported in pipeline mode**
- `helm.sh` — Helm 3 EOL (final feature release 2026-09-09, security to Feb 2027); Helm 4 chart compatibility; server-side-apply default; flag renames
- `docs.pola.rs` — new streaming engine from 1.31.1; `sink_parquet` documented as unstable
- `postgresql.org` PG 18 release notes — uuidv7(), OLD/NEW in RETURNING, temporal constraints (`WITHOUT OVERLAPS` / `PERIOD`), async I/O
- `blog.vonng.com/en/db/minio-resurrect/` (Pigsty) — console removed May 2025; `pgsty/minio` fork rationale, restored console, CVE-2025-62506 patch
- `github.com/bitnami/charts#35164`, `bitnami/containers#83267` — 2025-08-28 catalog change; `bitnamilegacy` / `bitnamisecure` split
- MinIO archival coverage (itsfoss, thecloudsupportengineer, elest.io) — "no longer maintained" 2026-02-12, archived 2026-04-25
- Helm 4.2.3 rendering the Airflow chart 1.22.0 cleanly (no public certification found — **must be tested**)
- The exact OTel `traceparent` extraction code for the ETL pod (the *absence* of built-in propagation is HIGH confidence; the recipe is MEDIUM)
- `ubuntu-latest` free-disk figures on GitHub-hosted runners (varies by runner image release)
- Whether the Airflow chart's `run-airflow-migrations` Job succeeds against an image tag of 3.3.0 while `appVersion` is 3.2.2 (supported per the chart's stated 3.0+ compatibility, but **verify in Phase 2**)

<!-- GSD:stack-end -->

<!-- GSD:conventions-start source:CONVENTIONS.md -->

## Conventions

Conventions not yet established. Will populate as patterns emerge during development.
<!-- GSD:conventions-end -->

<!-- GSD:architecture-start source:ARCHITECTURE.md -->

## Architecture

Architecture not yet mapped. Follow existing patterns found in the codebase.
<!-- GSD:architecture-end -->

<!-- GSD:skills-start source:skills/ -->

## Project Skills

No project skills found. Add skills to any of: `.claude/skills/`, `.agents/skills/`, `.cursor/skills/`, `.github/skills/`, or `.codex/skills/` with a `SKILL.md` index file.
<!-- GSD:skills-end -->

<!-- GSD:workflow-start source:GSD defaults -->

## GSD Workflow Enforcement

Before using Edit, Write, or other file-changing tools, start work through a GSD command so planning artifacts and execution context stay in sync.

Use these entry points:

- `/gsd-quick` for small fixes, doc updates, and ad-hoc tasks
- `/gsd-debug` for investigation and bug fixing
- `/gsd-execute-phase` for planned phase work

Do not make direct repo edits outside a GSD workflow unless the user explicitly asks to bypass it.
<!-- GSD:workflow-end -->

<!-- GSD:profile-start -->

## Developer Profile

> Profile not yet configured. Run `/gsd-profile-user` to generate your developer profile.
> This section is managed by `generate-claude-profile` -- do not edit manually.
<!-- GSD:profile-end -->
