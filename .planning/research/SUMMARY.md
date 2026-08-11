# Project Research Summary

**Project:** Airflow ETL Platform (local kind Kubernetes + metadata-driven universal CSV ingestion engine)
**Domain:** Production-like data-ingestion platform — Kubernetes orchestration + bespoke Python ETL correctness engine
**Researched:** 2026-08-11
**Confidence:** MEDIUM-HIGH (stack versions HIGH; architecture and build order MEDIUM-opinionated)

> **Read "Recommended Build Order" first.** All four research documents independently concluded that README §92's 10-stage sequence is wrong in four specific, expensive ways. Consolidating those deviations is the single most valuable output of this research.

## Executive Summary

This is an *execution* problem, not a discovery problem. README.md is a 3,386-line specification with 95 sections, a 10-stage roadmap (§92) and a 114-item Definition of Done (§94); the feature set is already fixed and the user has committed to all of it. Research therefore did not propose features — it (a) pinned a 2026-current stack against three ecosystem shocks that invalidate typical training data, (b) organized the 114 DoD items into a 14-prefix REQ-ID taxonomy and found 16 specification gaps, (c) designed the component seams that make the README's extensibility claims (§29/§95) actually hold, and (d) catalogued the decisions that are cheap now and unrecoverable later.

The recommended approach: build a deliberately narrow **vertical slice** (§93) — one UTF-8 comma CSV, discovered by a thin TaskFlow DAG, processed in one `KubernetesPodOperator` pod by a `dataplat`/`csv_processor` library, loaded transactionally into analytical PostgreSQL — but with **idempotency inside the slice**, not deferred to §92 Phase 8. The metadata control plane (`meta.*` tables) is the product, not a side-effect: PROJECT.md's Core Value ("every file, batch and record can be traced, explained, reprocessed and trusted") is a statement about those tables. Around that slice, two tracks run genuinely in parallel — the infrastructure track (kind, MinIO, CloudNativePG, Airflow) and the pure-Python library track (testable against Docker/testcontainers, no cluster required), roughly 25% of total effort.

The dominant risks are not CSV parsing. They are: (1) retrofitting identity/idempotency into six phases of accumulated schema; (2) `KubernetesPodOperator` ↔ kind integration friction (image pull, XCom sidecar, service-account identity) debugged simultaneously with pipeline logic; (3) silent data corruption from chunking CSVs by line rather than by record, from unversioned change hashes, from overlapping SCD2 validity intervals, and from watermarks advanced out of commit order. Every one is prevented by a decision costing minutes today. Mitigation is structural: make bad states unrepresentable via constraints (UNIQUE on `(dataset, batch_key)`, `btree_gist` exclusion on `(business_key, validity_range)`, `hash_version` columns) rather than by remembering to handle bad cases.

## Key Findings

### Recommended Stack

Source: `STACK.md` (HIGH for versions — verified 2026-08-11 against PyPI JSON API, GitHub Releases, Artifact Hub chart indexes; MEDIUM for observability wiring and the Helm-4 call).

**Three ecosystem shocks since typical training data — each changes a README mandate:**

| Shock | Reality | Resolution |
|---|---|---|
| **MinIO CE is dead** | `minio/minio` archived 2026-04-25; last CE image `RELEASE.2025-09-07`; console stripped May 2025; Operator dead | Keep official chart `5.4.0`, override image to the maintained fork `pgsty/minio:RELEASE.2026-08-04`. Treat the S3 client as a hard abstraction seam. ADR naming SeaweedFS as migration target. **HIGH — needs an explicit decision** |
| **Bitnami free catalog gone** | The Airflow chart's bundled Postgres subchart points at `bitnamilegacy/postgresql:16.1.0` and self-disclaims "not recommended for production" | `postgresql.enabled: false` + **CloudNativePG** with two explicit `Cluster` CRs. This also *helps* §4's separation mandate |
| **Helm 3 is EOL-ing** | Final Helm 3 feature release 2026-09-09; security patches end Feb 2027 | Adopt Helm **4.2.3** now behind a Phase-1 compatibility gate (`--atomic`→`--rollback-on-failure`, `--force`→`--force-replace`, server-side apply default). Fallback `3.21.3` |

**Pinned versions:**

| Layer | Choice | Pin |
|---|---|---|
| Local Kubernetes | kind | `v0.32.0`, node `kindest/node:v1.35.5` |
| Package manager | Helm | `4.2.3` (fallback `3.21.3`) |
| Orchestrator | Apache Airflow | `3.3.0` on chart `1.22.0` (image tag override) |
| Executor | KubernetesExecutor (local) / LocalExecutor (CI) | — |
| PostgreSQL | CloudNativePG operator `1.30.0`, chart `0.29.0` | PG **17** (Airflow) / PG **18** (analytical) |
| Object store | MinIO chart `5.4.0` + `pgsty/minio` fork image | `RELEASE.2026-08-04T00-00-00Z` |
| Secrets | HashiCorp Vault chart `0.34.0` | Vault `2.0.3` (major is 2.x, BUSL-1.1, IBM-owned) |
| Secret delivery | Vault Kubernetes auth, direct SA-token login | provider `4.8.0`, `hvac 2.4.0` |
| Python | CPython | **3.12** (both images) |
| Packaging | uv + src-layout | `uv 0.12.3` (Poetry unused) |
| CSV parsing | stdlib `csv`, streaming | — |
| Encoding detect | BOM sniff → contract → `charset-normalizer` `3.4.9` + `chardet` `7.5.1` | — |
| Dialect detect | `clevercsv` `0.8.5` (detect only) | — |
| Config/contracts | Pydantic v2 `2.13.4` (config only, **never** per-row) | — |
| DB driver | `psycopg[binary,pool]` `3.3.4` — COPY BINARY → staging → MERGE | — |
| S3 client | `boto3` `1.43.68` | — |
| Migrations | Alembic `1.19.1`, hand-written revisions | — |
| Testing | pytest `9.1.1` + hypothesis `6.165.3` + testcontainers `4.15.0` | — |
| Metrics | StatsD-exporter → kube-prometheus-stack `88.2.0`; business metrics via analytical DB → Grafana Postgres datasource | — |
| Traces | OTel Collector chart `0.169.0` + Tempo; DIY W3C `traceparent` injection into KPO pods | — |
| Lint/type | ruff `0.16.2` + mypy `2.3.0` | — |
| Security | trivy `0.73.0` + gitleaks `8.30.1` (binaries, not the actions) | — |

**Hard compatibility constraints the roadmap must respect:**

- **Airflow 3.3.0 supports PostgreSQL 13–17 only — PG 18 is NOT supported for the metadata DB.** Hence the deliberate two-major split (PG 17 Airflow / PG 18 analytical).
- kind's *default* node image is K8s 1.36.1, **outside Airflow's supported 1.30–1.35 range**. Pin 1.35.5 — "most recent *supported*", a direct narrowing of README line 3.
- **Do not install `csv_processor` into the Airflow image.** Airflow 3.3.0 constraints pin `pandas==2.1.4`, `psycopg2-binary`, `polars==1.42.1` — those pins would become yours. Two images, two dependency sets (PITFALLS G5).
- Airflow emits metrics to StatsD **XOR** OTel, never both.
- `psycopg` supports `COPY` and pipeline mode, but **not together**.
- CNPG `cluster` chart `0.8.1` defaults to PG 16 — must be overridden.

**Two Helm values profiles from day one** (`values-local.yaml` / `values-ci.yaml`): the full stack does not fit GitHub's 4 CPU / 16 GB runner. Retrofitting profile parameterization is expensive.

### Expected Features

Source: `FEATURES.md` (taxonomy HIGH — derived from README, arithmetic verified; comparative-tool claims MEDIUM).

**The 14-category REQ-ID taxonomy — verbatim prefixes and counts. Every one of the 114 DoD items lands in exactly one; sum verified = 114.** REQUIREMENTS.md is generated directly from this.

| # | Prefix | Category | DoD items | Count | Effort |
|---|--------|----------|-----------|-------|--------|
| 1 | `INFRA` | Cluster, deployed services, IaC, container build | 1–8 | 8 | L |
| 2 | `SEC` | Secrets management, workload identity, security testing | 90–102, 111 | 14 | L |
| 3 | `ORCH` | Airflow orchestration, TaskFlow, K8s execution, dataset deps | 9–14, 58 | 7 | M |
| 4 | `CSV` | Filename/encoding/dialect/header/footer detection + normalization | 15–22, 28, 29 | 10 | XL |
| 5 | `SCHEMA` | Inference, contracts, versioning, compatibility, drift, historical resolution | 23–27, 51 | 6 | L |
| 6 | `VALID` | Structural + quality validation, quarantine, reports, reconciliation, RI | 30–33, 55–57 | 7 | L |
| 7 | `LOAD` | Identity, idempotency, transactional/atomic loading, recovery, streaming | 34–37, 52–54 | 7 | L |
| 8 | `DEDUP` | Deduplication strategies and audit | 38–41 | 4 | M |
| 9 | `INCR` | Watermarks, late/out-of-order data, backfills, rebuild-from-raw | 42, 43, 47–50, 114 | 7 | L |
| 10 | `CDC` | CDC event model, ordering, delivery semantics | 44–46 | 3 | M |
| 11 | `SCD` | SCD 0/1/2, keys, change detection, effective dating, late arrivals, CDC↔SCD | 60–70 | 11 | XL |
| 12 | `OBS` | Freshness, structured logging, metrics/lineage, runbooks | 59, 74–77, 112 | 6 | L |
| 13 | `QUAL` | Python code standards + full test pyramid + fixture corpus | 71–73, 78–89 | 15 | L |
| 14 | `CICD` | GitHub Actions pipeline, image build, manifest validation, rebuildability | 103–110, 113 | 9 | M |

**Optional split:** `QUAL` (15, the largest) bisects cleanly into `PYENG-01..03` (DoD 71–73) + `TEST-01..12` (DoD 78–89) → 15 prefixes, no other change. Similarly `NORM-01..05` can split out of `CSV` (DoD 28/29) if normalization warrants its own plan.

**Ambiguity calls already made — record so they are not re-litigated:** DoD 51→`SCHEMA` (not `INCR`); 58→`ORCH`; 59→`OBS`; 77→`OBS` (redaction is a logging-layer control; cross-reference in `SEC`); 111→`SEC` (restatement of 90–102); 114→`INCR` (rebuild the *data*; 113 in `CICD` rebuilds the *environment*); 28/29→`CSV`.

**The 16 catalogued README gaps** — specified in prose with **no corresponding DoD item**, or absent entirely. Requirements generation must fill these:

| # | Gap | README ref | Severity | Proposed REQ |
|---|---|---|---|---|
| G1 | **Control-plane metadata schema as a deliverable** — §13/§23/§27/§28/§62/§82/§83 all describe metadata; nothing says "design one coherent schema" | cross-cutting | **CRITICAL** | `META-01` |
| G2 | **Lineage is queryable** — §83 mandates lineage; §94 has **no item at all** | §83 | HIGH | `OBS-xx` |
| G3 | **Metrics are exposed** — §82 lists a metric set; **no DoD item at all** | §82 | HIGH | `OBS-xx` |
| G4 | **File integrity / still-uploading detection** — mandated, no DoD item. Classic production incident | §42 | HIGH | `LOAD-xx` |
| G5 | **Manifest / control-file support** — specified, no DoD item. Table stakes for file ingestion | §41, §43 | HIGH | `LOAD-xx` / `ORCH-xx` sensor |
| G6 | **Missing-expected-file detection** ("none available" vs "expected but missing") | §44 | MEDIUM | `OBS-xx` (pairs with freshness, DoD 59) |
| G7 | **Quarantine re-drive path** — quarantine specified; getting data *out* is not | §51 | HIGH | `VALID-xx` |
| G8 | **Deterministic processing** — mandated, no DoD item; highly testable (same input twice ⇒ identical output hash) | §67 | MEDIUM | `QUAL-xx` property test |
| G9 | **Configuration versioning** — mandated; DoD 24 covers contracts only | §66 | MEDIUM | `SCHEMA-xx` |
| G10 | **Concurrency / race protection** — needs advisory locks or unique constraints per `(dataset, batch)` | §86, §87 | MEDIUM | `LOAD-xx` |
| G11 | **Data retention enforcement** — specified *twice*, no DoD item | §64, §91 | MEDIUM | `OBS-xx` / `INFRA-xx` |
| G12 | **Anomaly detection** — specified; DoD 31 only implies it. Depends on G1/G3 | §53 | MEDIUM | `VALID-xx` |
| G13 | **Compressed input (`.gz`/`.zip`) and multi-part datasets** — **never mentioned in any of the 95 sections**; ubiquitous in real feeds | — | HIGH | `CSV-xx` |
| G14 | **Timezone/DST correctness as a tested property** — §14 mandates DST handling; nothing tests it | §14 | MEDIUM | `QUAL-xx` |
| G15 | **Resource requests/limits per workload** — only indirect via DoD 10 | §85 | LOW | `ORCH-xx` |
| G16 | **Unicode normalization (NFC/NFD)** — §18 covers whitespace only; breaks dedup and SCD hashing | §18 | MEDIUM | `CSV-xx` |

**Must have (table stakes / P1):** kind+MinIO+PG+Airflow vertical slice; `META` control-plane schema (G1); batch ledger + content-hash identity; core CSV parse (encoding, dialect, header); TaskFlow + `KubernetesPodOperator`; schema contracts + versioning; staging + atomic publish + merge; fixture corpus (the corpus *is* the spec); Vault + K8s auth + Airflow secrets backend.

**Should have (P2):** structural + quality validation, quarantine, machine-readable reports; dedup strategies + audit; watermarks + backfill; SCD 0/1/2 + surrogate keys + deterministic change detection; full CI pipeline + ephemeral-kind E2E.

**Defer / reduce ambition (over-engineering candidates, FEATURES §6):**

- **Intra-file 250k-row offset checkpointing (§38)** → get it free: commit in chunks, record `last_committed_chunk_ordinal` on the batch ledger row. Build offset-level resume only if a fixture demands it.
- **Multi-row/hierarchical headers (§11)** → detect and **reject with a clear diagnostic**; no canonical flattening exists.
- **CDC before-image/after-image (§29)** → define the event model, prove it with a CSV-delivered CDC feed (op column + sequence + key). Defer before-image until a source produces one.
- **`defer` outcome for referential integrity (§47)** → ship `fail`/`quarantine`/`warn` only.
- **Full six-strategy dedup matrix (§26)** → build the *strategy interface* + exact-row + business-key; the other four are implementations of a solved interface, not new architecture.
- **OpenTelemetry tracing (PROJECT.md, not README)** → valuable and expensive; **sequence last**, its value depends on everything else already emitting context.
- **ML anomaly detection (§53)** → already correctly scoped to statistical thresholds. Hold that line.

**Design references adopted from comparable tools** (none adopted as dependencies — the README mandates a bespoke engine): dlt's 3×4 schema-contract matrix (`{tables, columns, data_type}` × `{evolve, freeze, discard_row, discard_value}`) rather than a boolean; dbt's SCD2 column vocabulary with the surrogate key **independent** of the change hash, both `timestamp` and `check` change-detection strategies, `hard_deletes = ignore|invalidate|new_record`, and a `valid_to_current` sentinel instead of NULL; Delta Lake's `(txnAppId, txnVersion)` as proof the idempotency token is small; Debezium's at-least-once default as the direct citation for §30 (exactly-once is a transport property, unavailable without a broker); Great Expectations' persistence of validation results as **rows**, not just artifacts. Ragged rows are errors — never pad or truncate (polars #10585); pre-filter NUL bytes before the stdlib csv reader (cpython #71767). Use `>=` cursors plus idempotent merge, never `>`.

### Architecture Approach

Source: `ARCHITECTURE.md` (overall MEDIUM — integration mechanics documentation-verified via Context7; the metadata model, package decomposition and build order are opinionated designs the first two phases will test).

Four claims drive the design. **(1) The metadata control plane is the product, not a side-effect** — Core Value is a statement about `meta.*` tables, so a minimal metadata schema belongs *in* the vertical slice. **(2) The seam that makes §29/§95 ("add sources without redesign") true is `Source → RecordChunk → Publisher`** — CDC is a `Source`, SCD is a `Publisher`, and everything between (validate, normalize, dedupe) is shared and source-agnostic; §68's proposed package layout does not contain this seam and will not deliver §95 as written. **(3) Row-level data problems are data, not exceptions** — the §71 exception hierarchy is for *run-fatal* conditions; a bad date on row 41,203 is a `ValidationResult` + `RejectedRecord` flowing through as a value. **(4) The publication transaction is the atomicity boundary for data *and* metadata** — rows, watermark advance and run-status commit together or not at all, which makes §24, §28 and §37 one mechanism rather than three. That is also the decisive reason OpenLineage cannot be the system of record: an HTTP event emitter cannot enlist in a PostgreSQL transaction.

**Major components and ownership boundaries:**

1. **Airflow scheduler / DAG processor** — when work runs, dependency order, retry policy, backfill windows, fan-out degree. Owns **no** parsing, validation, typing or analytical DB writes.
2. **DAG (`@dag`/`@task`)** — discovery call, config-version pinning, assignment authoring, fan-out, receipt aggregation. **Target: < 150 lines per DAG file.**
3. **ETL task pod** — one work assignment end-to-end in bounded memory; knows nothing of other pods or Airflow internals. Reads assignment JSON from MinIO, writes run rows to `meta`, returns a **≤ 4 KB receipt**.
4. **`dataplat` core package** — pipeline engine, config, metadata repo, validation, normalization, dedup, publication, observability, `SecretsResolver`. Exposes protocols `Source` / `Stage` / `Publisher` / `MetadataRepository`. Format-agnostic.
5. **`csv_processor` package** — filename parsing, encoding/dialect/header detection, streaming CSV reads. Implements `dataplat.sources.Source` via entry point. Owns no validation, typing, dedup or loading.
6. **MinIO** — immutable raw objects, quarantine artifacts, validation reports, work assignments, XCom overflow. Addressed only as `s3://bucket/key` (§5). Holds **no** authoritative processing state.
7. **Analytical PostgreSQL** — `meta` control plane, `staging`/`normalized`/`warehouse` data, and all constraints that *enforce* idempotency. DDL via Alembic.
8. **Vault** — workload identity, credential issuance, access audit. Not application config (a ConfigMap's job).
9. **Prometheus / Grafana / OTel** — metrics and traces. **Not** data-correctness state: that lives in `meta` and is queried by SQL.

**Key patterns:** assignment document as the unit of work; streaming stages vs. explicit barrier stages (ordering is a barrier, not a source concern); errors-as-values for row-level problems; the publication transaction as the metadata commit point; config-keyed strategy registries.

**Anti-patterns to encode as review gates:** business logic in DAG files; typed staging tables; filename as identity; advancing the watermark outside the publication transaction; returning data through XCom; one uncapped mapped task per file; `SELECT DISTINCT` as deduplication; deferring the metadata schema; ingestion time as the SCD effective date; reading `logical_date` in an asset-triggerable DAG (it is `None` in Airflow 3); config only in a ConfigMap; regenerating the Fernet key on every deploy.

### Critical Pitfalls

Source: `PITFALLS.md` (MEDIUM-HIGH, per-entry tags). Severity scale: `DATA CORRUPTION` > `REWORK` > `ANNOYANCE`.

1. **Chunking a CSV by lines destroys every record containing an embedded newline (E1).** Use ONE `csv.reader` over a `newline=""` text wrapper for the whole object; chunk in **records**, downstream of the parser. Getting this wrong also forces a redesign of the §38 checkpoint model.
2. **`MERGE` is not concurrency-safe; `INSERT … ON CONFLICT` is (C1).** Under the concurrency §86 explicitly requires, `MERGE` fails or duplicates. Single-writer publication via advisory lock + `ON CONFLICT` on the natural key.
3. **Letting PostgreSQL parse the CSV voids the entire product (C3).** `COPY … FORMAT csv` puts rows in the warehouse that validation never saw, making every §19–§27 guarantee decorative. Enforce with a CI grep for `FORMAT csv`.
4. **Watermarks advanced by wall-clock or max-seen rather than by observed *committed* cursor values, lagged (C10).** Rows committed out of timestamp order are never seen again; only control totals can detect it.
5. **kind nodes lie about their capacity (A2).** Without kubelet reservations and `maxPods` in `kind/cluster.yaml`, the scheduler over-packs and the *host* OOM killer arbitrates — and changing kubelet config requires recreating the cluster.
6. **Pod amplification (B1): KubernetesExecutor + KubernetesPodOperator is two pods per task.** With uncapped Dynamic Task Mapping this is how a 32-CPU host falls over.
7. **The XCom sidecar has four separate ways to fail (B2) and it is on the critical path** for the receipt contract. Write the receipt in `finally`; local sidecar image; Pod Security Standards compatible.
8. **Vault dev mode loses everything on restart (D1)** — on WSL2 that means every morning — and a sealed Vault after restart is a *manual* step (D2) unless planned for. The Airflow Vault secrets backend **fails open, at parse time** (D4).

**The fifteen cheap-now / unrecoverable-later decisions, with the phase each must be decided by.** Eleven of fifteen are *"make the bad state unrepresentable"* rather than *"remember to handle the bad case"* — PITFALLS argues this should be the roadmap's explicit design bias.

| # | Decision | Cost if deferred | Severity | Decide by |
|---|---|---|---|---|
| 1 | Store a **`hash_version`** alongside every change hash (C6) | Changing the hash recipe invalidates all stored hashes; every dimension appears to change at once; history may be unrecoverable | DATA CORRUPTION | The first migration that stores a hash |
| 2 | SCD2 dimensions get a **`btree_gist` exclusion constraint** on `(business_key, validity range)` (C7) | Once overlapping intervals exist the constraint cannot be added and every as-of query is silently wrong | DATA CORRUPTION | The dimension's creating migration |
| 3 | **Run-scoped identity (`run_id`, `attempt`) on every staged and loaded row** (C5) | Retrofit means rewriting the loader and back-filling identity; duplicates cannot be attributed or removed | DATA CORRUPTION | Slice (§92 P5) |
| 4 | Dynamic Task Mapping expands over a **frozen manifest, never a live listing** (B6) | Reruns and backfills silently produce different work; §62/§67 claims become false | DATA CORRUPTION | Slice — before mapping exists |
| 5 | Stream and chunk in **records** via one `csv.reader` over `newline=""` (E1) | Every embedded-newline record corrupted; checkpoint model must be redesigned | DATA CORRUPTION | Slice |
| 6 | SCD corrections **recompute** a key's history from an ordered event log (C8) | In-place interval surgery is not idempotent; with at-least-once CDC this drifts permanently | DATA CORRUPTION | Design at §92 P8 / build at P9 |
| 7 | Advance the watermark only from **observed committed cursor values, lagged** (C10) | Out-of-order-committed rows never seen again | DATA CORRUPTION | §92 P8 |
| 8 | **Business date comes from the data**, never the clock or `logical_date` (B7) | Backfilled rows carry today's effective date, corrupting SCD2 history invisibly | DATA CORRUPTION | Slice (the rule) |
| 9 | The processor is the **only** CSV parser (C3) | Unvalidated rows in the warehouse | DATA CORRUPTION | Slice (first load) |
| 10 | Decide PV persistence and put **`extraMounts` in `kind/cluster.yaml`** (A4, B10) | Adding them later requires recreating the cluster, destroying the state you wanted to keep | REWORK | Phase 1 |
| 11 | **Kubelet reservations and `maxPods`** in the kind config; requests/limits on every chart (A2) | Host OOM killer arbitrates; changing kubelet config means cluster recreation | REWORK | Phase 1 / 2 |
| 12 | **Metric labels bounded**; unbounded identity lives in the metadata DB (F2) | Prometheus OOMs; dashboards, alerts and recording rules must be rewritten | REWORK | Slice (the rule) |
| 13 | Explicit **`namespace` + `service_account_name`** on task pods, matched to the Vault role (B5, D3) | The usual "fix" is to widen the Vault role, silently voiding §81 least privilege | REWORK + security | §92 P3 / P4 |
| 14 | **Single-writer publication via advisory lock**, `ON CONFLICT` on the natural key (C1) | `MERGE` fails or duplicates under required concurrency | DATA CORRUPTION | Slice (shape) / P8 (hardening) |
| 15 | Fixtures **generated from a seed**, not committed en masse (G3, E6) | Build contexts bloat, secret scanner gets globally disabled, oversized-file memory test impossible | REWORK | §92 P6 |

**Also carried forward:** use **record-ordinal checkpoints, not byte offsets**. Byte offsets are meaningless once chunking happens in records downstream of a single parser; `last_committed_chunk_ordinal` on the batch ledger row delivers §38 as a byproduct of the ledger.

**Environment-level traps that invalidate everything above them (Section A):** inotify and file-descriptor exhaustion appears in Phase 2, not Phase 1 (set `fs.inotify.max_user_watches`/`max_user_instances` via `/etc/sysctl.d/99-kind.conf` + `/etc/wsl.conf`); WSL2's `ext4.vhdx` never shrinks, so **disk is the real ceiling, not RAM**; nothing survives `kind delete cluster` and local-path PVs are node-bound; mutable image tags + `IfNotPresent` means testing yesterday's code; Docker Hub anonymous pull limits look like CI network flakes; WSL2 clock drift after host sleep produces x509/token failures that "fix themselves" — check `date` before diagnosing anything auth-related.

## Implications for Roadmap

### Recommended Build Order

**This section is the consolidated adjudication of all four documents against README §92. It is the single most important output of this research.**

README §92 orders: `1 kind → 2 infra → 3 airflow+k8s → 4 secrets → 5 basic CSV pipeline → 6 universal CSV → 7 validation → 8 production-like data engineering → 9 CDC+SCD → 10 CI/CD`.

**Five deviations, converged on independently by multiple documents:**

| # | Deviation | Documents converging | Reasoning |
|---|---|---|---|
| D1 | **Idempotency / batch ledger / content hashing move INTO the vertical slice** (out of §92 P8) | **FEATURES.md and ARCHITECTURE.md concluded this independently**; PITFALLS #3/#14 reinforce | Marginal cost now: two unique constraints, one claim query, one table. Cost in P8: a migration across six phases of accumulated schema. In between, Airflow retries are **on by default**, so the platform silently duplicates data every phase, and the tests that would catch it (DoD 83) do not exist yet. FEATURES calls this "the single largest avoidable rework risk in the project"; Delta Lake's `(txnAppId, txnVersion)` is proof the token is small |
| D2 | **The metadata control plane is designed coherently up front, not accreted** | ARCHITECTURE (claim 1, AP8), FEATURES (gap G1, "the critical serialization point") | Watermarks, dedup audit, validation results, schema registry, batch ledger and lineage are all writes into **one** control-plane schema. Accreting it capability-by-capability guarantees six migrations and inconsistent foreign keys. FEATURES: "the single strongest structural recommendation in this document" |
| D3 | **Vault moves AFTER the slice, behind a `SecretsResolver` seam** | ARCHITECTURE (Q1, Q10.3), STACK (two-tier pattern) | The slice needs *credentials*, not a *secrets manager*; Kubernetes Secrets satisfy it. Vault adds a mutating webhook, a K8s auth mount, TokenReview permissions and policy debugging onto the critical path of "does anything work end to end at all?". The retrofit is a ConfigMap change **if and only if** `SecretsResolver` exists in the library from the start — the processor resolves an opaque reference (`env://…` or `file:///vault/secrets/…`) and never learns which. That is precisely what §81 demands. Cost: a dev-only DSN in a K8s Secret for one phase, removed next phase, with secret scanning active from Stage 0 |
| D4 | **CI skeleton first, not last** (§92 P10 → split S0 + S13) | ARCHITECTURE (Q10.3), FEATURES (`CICD` lint subset is P1) | §93 requires every capability to ship with "CI validation". A pipeline created in the final phase cannot have gated any earlier code. Lint + mypy + unit tests + secret scanning on day one; integration tests at the slice; ephemeral-kind E2E at the end |
| D5 | **Observability promoted to an explicit stage** | ARCHITECTURE (Q10.3 "Add"), FEATURES (gaps G2/G3) | §82 (metrics) and §83 (lineage) have **no DoD items at all** and no home in §92. The *seams* (`observability/{logging,metrics,tracing}.py`) belong in the library from the start as no-ops; the *stack* is a parallel stage after the slice |

ARCHITECTURE's own summary: "**Deviations 2 and 3 are the ones that cost real money if ignored**" — i.e. idempotency-in-slice and CI-first.

**The resulting recommended stage sequence:**

| Stage | Name | Content |
|---|---|---|
| **S0** | Repo & toolchain + **CI skeleton** | src-layout, uv, ruff, mypy, pytest; GH Actions running lint + typecheck + unit + **gitleaks** on day one. Fixture-corpus generation starts here |
| **S1** | kind cluster + local registry | `kind/cluster.yaml` with kubelet reservations, `maxPods`, `extraMounts` decided **now** (A2, A4, B10). Local registry, not `kind load`. sysctl bootstrap + `make doctor` |
| **S2** ‖ | Infrastructure | 2a MinIO · 2b analytical PG (CNPG, PG 18) · 2c Airflow PG (CNPG, PG 17) · 2d Airflow (needs 2c). **Vault removed from this stage.** Both Helm values profiles written now |
| **S3** ‖ | `dataplat` core + metadata schema + naive CSV reader + Dockerfile | Models, errors, logging, config loader+hasher, object store, db, pipeline engine, metadata repo, **`SecretsResolver`**, observability no-op seams. 5 metadata tables (`datasets`, `config_versions`, `files`, `batches`, `ingestion_runs`) + `normalized.customers`. **Tested against Docker/testcontainers — no cluster required** |
| **S4** | Airflow ↔ K8s smoke | One `KubernetesPodOperator` running `dataplat --version` with `do_xcom_push=True`. Deliberately trivial |
| **S5** | **VERTICAL SLICE CLOSES (§93)** | discover → assignment → pod → staging → MERGE → run row + E2E test, **including the re-run-produces-zero-additional-rows assertion**. Includes content hashing, idempotency key, claim protocol |
| **S6** ‖ | Vault secrets | K8s auth, policies, positive **and negative** identity tests; swap `SecretsResolver` backing |
| **S7** ‖ | Universal CSV engine | 7a filename · 7b encoding · 7c dialect · 7d header/footer · 7e inference · 7f streaming |
| **S11** ‖ | Observability stack | Prometheus/Grafana, StatsD exporter, business metrics via Grafana Postgres datasource |
| **S8** ‖ | Validation & quarantine | Structural + quality rules, quarantine, machine-readable reports |
| **S9** ‖ | Metadata control-plane completion | `schema_versions`, `watermarks`, `run_stages`, `dedup_audit`, reconciliation, config-sync job |
| **S10** | ETL correctness | 10a dedup + incremental/watermarks · 10b backfill + late/out-of-order · 10c recovery + checkpoint + lease · 10d reconciliation + control totals |
| **S12** | CDC + SCD | 12a SCD 0/1/2 + exclusion constraint + late corrections ‖ 12b CDC `Source` + `ChangeEnvelope` + ordering barrier → 12c CDC→SCD |
| **S13** | CI/CD completion | Ephemeral kind E2E, image publish, trivy scanning, deploy |
| **S14** | Operations | Runbooks, retention jobs, DR rebuild-from-raw, OpenLineage export |

**Full §92 reconciliation** (ARCHITECTURE Q10.3): P1→S1 agree · P2→S2 agree *with Vault removed and CI-profile values written now* · P3→S4 agree, keep the smoke DAG trivial · **P4 Secrets → moved to S6 (deviate)** · **P5 → S5 with idempotency moved here from P8 (deviate)** · P6→S7 agree (7a–7e heavily parallelizable) · P7→S8 agree · **P8 → split into S9 + S10 minus idempotency** (P8 bundles ~16 unrelated capabilities — too coarse for one roadmap phase) · P9→S12 agree on content, **add** that placement is `Source`/`Publisher` not a new pipeline, else §29/§95 extensibility will not hold · **P10 → CI skeleton at S0, full CD at S13 (deviate)** · **Observability added as S11**.

### Phase Ordering Rationale

- **Two ordering insights dominate.** *(a)* Infrastructure (S1–S2) and the Python library (S3) are **fully parallel** — the library needs only Docker, never a cluster. Roughly half the slice's work runs on two independent tracks; the roadmap should reflect that rather than serializing infra → library. *(b)* The smoke DAG (S4) is a separate, deliberately trivial step, because the highest-risk unknown is not CSV parsing — it is whether an Airflow 3 `KubernetesPodOperator` on kind can pull a locally-built image, run as a non-root SA and return an XCom. Debugging that while also debugging a CSV pipeline is how a week disappears.
- **Normalization must precede hashing.** Both `DEDUP` (exact-row hash) and `SCD` (change hash) hash *normalized* content. If normalization lands after either, both produce phantom differences. Hard ordering edge.
- **Retries depend on idempotency, not the reverse.** This is the mechanical justification for D1.
- **`SCHEMA` versioning gates two later things** — drift detection and historical backfill resolution (DoD 51). Prerequisite, not nice-to-have.
- **CDC does not gate SCD.** SCD 0/1/2 build from CSV batches alone; only DoD 67/68 need CDC. Do not let CDC block SCD.
- **Runbooks trail everything** — they document real observed failure modes; writing them early produces fiction.
- **CI-profile Helm values gate ephemeral-kind E2E** (DoD 113). This is why S2 must write both profiles even though S13 consumes them.

### Parallelization Map

The project runs with `parallelization: true`. Waves from ARCHITECTURE Q10.2 with effort shares:

| Wave | Concurrent | Why safe | Share |
|---|---|---|---|
| A | **S1+S2 (infra) ‖ S3 (library)** | Different artifacts, different harnesses; S3 needs only Docker | ~25% |
| B | S4 → S5 | **Strictly serial — this is the critical path; protect it** | ~15% |
| C | **S6 (Vault) ‖ S7 (CSV) ‖ S11 (observability)** | Vault touches manifests, CSV touches `csv_processor/`, observability touches Helm + `dataplat/observability`. Almost no file overlap | ~20% |
| D | **7a ‖ 7b ‖ 7c ‖ 7d ‖ 7e** | Pure functions over a shared read-only fixture corpus. **The single best parallelization opportunity in the project** | (within C) |
| E | **S8 (validation) ‖ S9 (metadata completion)** | Coordinate only on `meta.validation_results` DDL | ~10% |
| F | 10a ‖ 10d → 10b → 10c | 10b needs watermarks from 10a | ~15% |
| G | 12a ‖ 12b → 12c | Publisher work vs. Source work | ~10% |
| H | S13 ‖ S14 | Independent | ~5% |

**Additional independent tracks (FEATURES §4):** the `CICD` lint/typecheck/unit workflow has zero dependencies and can land day one; fixture-corpus authoring should *lead* CSV implementation (the corpus is the spec); the four detectors (encoding, dialect, header, filename) are mutually independent pure functions; date/number/boolean/null/whitespace normalizers are independent pure functions; `SCD` Type 0/1 are independent of Type 2; `VALID` rule types are independent of each other except referential integrity (needs multi-dataset load); `OBS` logging standards are a cross-cutting convention adoptable immediately.

**Strictly sequential chains (cannot be parallelized):** `INFRA`→`ORCH`→any E2E · `INFRA`(Vault)→`SEC`(K8s auth+policies)→`SEC`(Airflow backend) · `META` schema→{watermarks, dedup audit, validation results, lineage, batch ledger} · `CSV`parse→`SCHEMA`→`CSV`normalize→`VALID`→`DEDUP`→`LOAD` · `LOAD`(ledger)→idempotency→retries→backfills · `LOAD`(staging+merge)→`DEDUP`(cross-batch)→`SCD`(merge) · `INCR`(watermark)→backfill correctness · `VALID`(persisted results)→anomaly-detection baselines · `INFRA`(CI-profile values)→`CICD`(ephemeral-kind E2E) · everything→`OBS`(runbooks).

### Spikes with Pre-Declared Pass Criteria

ARCHITECTURE named three unvalidated assumptions; PITFALLS converted each into an experiment.

| Spike | Stage | Risk verdict | Experiment | Pass criteria |
|---|---|---|---|---|
| **U1 — locally-built image pulls and runs via `KubernetesPodOperator` on kind** | S4 (§92 P3) | **MEDIUM** — mechanism works; three frictions near-certain, all cheap to pre-empt (per-node `kind load`; stale image because the tag already exists; pull time exceeding `startup_timeout_seconds`, default 120 s vs a 2 GB image) | Build `csv-processor:<git-sha>` that prints its own version; push to local registry; run via KPO with `do_xcom_push=True` writing `/airflow/xcom/return.json` | **XCom contains the SHA that was built.** Exercises registry, pull policy, tag scheme, sidecar (B2) and receipt contract at once; becomes the permanent platform smoke test. Under an hour |
| **U2 — Vault K8s auth on kind without JWT-issuer overrides** | S6 (§92 P4) | **LOW — largely resolved by PITFALLS.** `disable_iss_validation` has defaulted true since Vault 1.9 because TokenReview does the same validation; the famous `claim "iss" is invalid` error is a K8s-1.21-era artefact. Real remaining risks (all D3): token reviewer lacking `system:auth-delegator`; audience mismatch; `kubernetes_host` copied from an outside-K8s tutorial; and most likely — the role binding a ServiceAccount that KPO does not actually use (B5) | `make vault-bootstrap` creates auth method, policy, role and K8s RBAC from one variable set | **Both** tests pass: the `csv-processor` SA in `etl` reads its own path (positive) **and** the `default` SA is denied (negative). *If the negative test is awkward to write, the identity model is not real yet — that is itself the finding.* Under half a day. **Check `date` first** — A7 clock drift produces `permission denied` on valid tokens and will be misdiagnosed as this |
| **U3 — streaming CSV throughput with per-chunk `COPY` under pod limits** | S5 (§92 P5) | **Genuinely unvalidated — needs an experiment, not an argument** | PITFALLS E7. **Do not run it with `executemany`** (10–100× slower than `COPY` per C11 — a false negative would change the architecture for no reason). **Measure peak RSS as well as throughput** — the real risk is E6, an implementation "streaming" in shape that accumulates somewhere | Record the baseline number in the repository; treat a later 5× regression as a bug rather than a mystery |

### Research Flags

**Phases likely needing `/gsd-plan-phase --research-phase` during planning:**

- **S1 / S2 (kind + infra):** Helm 4.2.3 vs. Helm-3 charts is the MEDIUM-confidence call in STACK.md. The MinIO fork image and CNPG chart defaults must be read off *pinned chart values*, not documentation.
- **S6 (Vault):** the kind-specific issuer caveat in ARCHITECTURE is explicitly flagged as *inference, unverified on this cluster*; `auth_type: kubernetes` is supported in provider code but undocumented on the docs page.
- **S11 (observability):** STACK rates metrics/traces MEDIUM. Cross-process trace propagation into KPO pods is **not** built in — the W3C `traceparent` injection recipe is DIY. Airflow's StatsD-XOR-OTel constraint shapes the whole design.
- **S12 (CDC + SCD):** the hardest correctness work (SCD2 late-arriving corrections, CDC ordering, tombstones and resurrection). PITFALLS C7–C9 are dense but the design space is still open.
- **S10c (recovery / checkpoint / lease):** the interaction of checkpointing × transactions × concurrency (§38 × §35 × §37 × §86/§87) is the least-settled area of ARCHITECTURE.

**Phases with standard patterns (skip research-phase):**

- **S0 (repo + CI skeleton):** ruff/mypy/pytest/uv/GH Actions fully pinned in STACK.md with commands.
- **S3 (library core), S7a–S7e (detectors):** pure Python over a fixture corpus; STACK has already chosen every library and rejected the alternatives with reasons.
- **S4 (smoke DAG):** deliberately trivial by design, experiment fully specified above.
- **S8 (validation), S9 (metadata completion):** shapes specified in ARCHITECTURE Q2 and FEATURES §3.2/§3.3.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | **HIGH** | All versions verified 2026-08-11 against PyPI JSON API, GitHub Releases API, Artifact Hub / chart index YAML and official docs. MEDIUM only for the Helm-4 compatibility call and observability wiring, where the ecosystem is genuinely in motion |
| Features | **MEDIUM-HIGH** | Taxonomy and DoD mapping derived directly from the README with arithmetic verified (114/114) = HIGH. Dependency graph = HIGH (structural, not opinion). dlt/dbt semantics via Context7 against official repos = MEDIUM. Effort estimates = MEDIUM (judgement, not measurement — calibrate after the first phase) |
| Architecture | **MEDIUM** | Integration mechanics (XCom sidecar, `expand`/`max_map_length`, object-storage XCom backend, asset/backfill semantics, PostgreSQL `MERGE`/`ON CONFLICT`/`ATTACH PARTITION`) documentation-verified via Context7. The metadata model, package decomposition and build order are **opinionated designs the first two phases will test**; effort percentages are estimates |
| Pitfalls | **MEDIUM-HIGH** | Per-entry tags. HIGH where reproduced against official docs or upstream issue trackers (kind known-issues, cpython #71767, polars #10585, Airflow provider changelogs); LOW entries explicitly flagged as reasoned inference |

**Overall confidence: MEDIUM-HIGH.** The stack is pinned and verified. The build order is well-reasoned and internally consistent across four documents, but remains a design hypothesis until S5 closes.

### Gaps to Address

- **Helm 4 against Helm-3 charts.** Generally compatible, but server-side apply is now the default and flags changed. **Handle:** explicit Phase-1 compatibility gate; keep `3.21.3` as a documented fallback. Read behaviour off pinned chart values, not blog posts.
- **The MinIO fork is a single-vendor dependency.** `pgsty/minio` is maintained by one project. **Handle:** the S3 client stays a hard abstraction seam; write the ADR naming SeaweedFS as migration target during S2.
- **Streaming throughput under kind pod limits (U3)** is genuinely unmeasured. **Handle:** the E7 spike at S5 with a recorded baseline.
- **Vault kind issuer configuration** — ARCHITECTURE's caveat is inference, unverified on this cluster. **Handle:** the positive+negative test pair at S6.
- **`auth_type: kubernetes` is undocumented on the Airflow hashicorp-provider docs page** though present in code. **Handle:** verify against the pinned provider source, not the docs page.
- **CNPG `cluster` chart `0.8.1` defaults to PG 16.** Must be explicitly overridden; do not trust the chart default.
- **GX / Soda record-level quarantine** — FEATURES rates this LOW: the negative claim ("no built-in record quarantine") came from search-result summaries and was not confirmed in vendor docs. **Handle:** verify before citing it as justification for building bespoke.
- **Debezium delivery semantics** — the official docs page 403'd the fetcher; the at-least-once claim is corroborated but not read first-hand. **Handle:** re-verify before writing §30's documented-semantics statement.
- **Effort estimates (S/M/L/XL) are judgement, not measurement.** **Handle:** recalibrate after S1/S3 complete — they run in parallel and give two independent data points.
- **§68's proposed package layout does not contain the `Source`/`Publisher` seam.** A deliberate departure from the README. **Handle:** record as an ADR early so it is not re-litigated at S12.

## Sources

### Primary (HIGH confidence)

- PyPI JSON API, GitHub Releases API, Artifact Hub / Helm chart index YAML — all version pins, verified 2026-08-11 (`STACK.md`)
- kind — Known Issues (inotify limits, WSL2 cgroup v2, disk/memory pressure, `kind load --name`): https://kind.sigs.k8s.io/docs/user/known-issues/
- Apache Airflow `KubernetesPodOperator` operator guide + Task SDK `BaseOperator` + cncf/kubernetes provider changelog, via Context7 (XCom sidecar, `do_xcom_push` default, namespace resolution)
- Apache Airflow 3.3.0 constraints `constraints-3.12.txt` — the pandas/psycopg2/polars pins that mandate two images
- PostgreSQL 18 documentation via Context7 — `MERGE`, `INSERT … ON CONFLICT`, `ATTACH`/`DETACH PARTITION`, transactional DDL
- cpython #71767 (stdlib csv hard-fails on NUL bytes); polars #10585 (ragged rows silently truncated)
- MinIO GitHub archival record (2026-02-12 unmaintained, 2026-04-25 archived); Bitnami catalog migration to `bitnamilegacy` (2025-08-28)

### Secondary (MEDIUM confidence)

- dlt — schema contracts and SCD2 merge: `dlt-hub/dlt` (`schema-contracts.md`, `merge-loading.md`, `common/schema/schema.py`, `extract/incremental/transform.py`), via Context7
- dbt — snapshots / SCD2: `dbt-labs/docs.getdbt.com` (`hard-deletes.md`, `dbt_valid_to_current.md`, `snapshot_meta_column_names.md`), via Context7
- Airbyte — Incremental Append + Deduped sync modes documentation
- HashiCorp Vault + `apache-airflow-providers-hashicorp` docs via Context7 (Kubernetes auth; the kind-specific issuer caveat is inference)
- Delta Lake `(txnAppId, txnVersion)` idempotent-write documentation
- Singer/Meltano STATE bookmark semantics

### Tertiary (LOW confidence — needs validation)

- Great Expectations / Soda record-level quarantine capability — search-result summaries only; the negative claim was not confirmed in vendor docs
- Debezium delivery semantics — official docs page 403'd; corroborated across secondary sources
- Metadata schema column-level design — an opinionated proposal corroborated only by convergent industry patterns, unvalidated against this workload

### Detailed research documents

- `.planning/research/STACK.md` — versions, rejected alternatives, install commands, README-vs-2026 conflict table
- `.planning/research/FEATURES.md` — REQ-ID taxonomy, full DoD mapping, dependency graph, 16 gaps, prioritization matrix, comparative tool analysis
- `.planning/research/ARCHITECTURE.md` — component design, metadata model, package structure, Airflow↔pod contract, idempotency layers, build order, anti-patterns
- `.planning/research/PITFALLS.md` — 15 cheap-now decisions, 3 assumption spikes, sections A–H, pitfall-to-phase mapping

---
*Research completed: 2026-08-11*
*Ready for roadmap: yes*
