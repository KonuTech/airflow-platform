# Phase 4: Vertical Slice — CSV to Analytical PostgreSQL - Research

**Researched:** 2026-08-13
**Domain:** Airflow 3.3 orchestration (TaskFlow, deferrable sensors, `KubernetesPodOperator`, Dynamic Task Mapping) over a single-writer PostgreSQL publication transaction; DAG↔pod contract design
**Confidence:** HIGH — nearly every claim below was verified against either the live repository (migrations, protocols, Helm values actually committed, the live kind cluster's current state) or an official/authoritative source (PostgreSQL docs, Airflow docs, the pinned Airflow 3.3.0 constraints file). The few claims that rest on reasoning alone are flagged `[ASSUMED]` and listed in the Assumptions Log.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**File-arrival trigger**
- **D-01:** The `csv_ingest_customers` DAG is woken by a **deferrable `S3KeySensor`** watching `s3://raw/customers/*.csv` (wildcard_match), not a plain scheduled polling DAG and not a MinIO-webhook-to-Airflow-API integration. Rationale discussed live: a scheduled poll wastes cycles even when nothing changed and has coarser (schedule-interval-bound) latency; a webhook is more "production-like" but requires a webhook receiver plus an Airflow API credential that has nowhere real to live before Vault lands in Phase 5 (ROADMAP explicitly protects this phase from that kind of scope creep — "Vault comes after the slice... putting [it] on the critical path... is how the slice slips"). The deferrable sensor uses the triggerer (already deployed in Phase 2), holds zero worker slots while idle, and needs no new infrastructure or credentials.
- **D-02:** Poke interval: **30 seconds**.
- **D-03:** DAG has `max_active_runs=1` — a new sensor trigger cannot overlap an in-progress load of the same dataset. (The run-claim idempotency protocol would still prevent duplicate rows without this, but capping runs avoids two DAG runs racing pointlessly over the same advisory lock.)
- **D-04:** If several files land within the same poke window, **one DAG run processes all currently-visible new files** — `discover_files` lists everything new since the last successful watermark and builds one frozen manifest (ORCH-08), with each file becoming one Dynamic-Task-Mapping unit. Not one-file-one-run.

**Slice CSV content & volume**
- **D-05:** CSV content is **synthetic, Faker-style data**, generated from a seed — consistent with the Phase 1 corpus policy (QUAL-08: "corpus is the specification... generated from a seed rather than committed en masse").
- **D-06:** A **separate ~1M row fixture** is generated specifically for the U3 streaming-throughput + peak-RSS spike baseline — large enough to force multiple staging chunks past the default `checkpoint_threshold_rows` (500k, ARCHITECTURE.md Q7) and produce a meaningful sustained measurement.
- **D-07:** A **separate small fixture (~50–200 rows)** is used for fast E2E/idempotency assertions (rerun-same-DAG-run, re-upload-under-different-name) that run on every CI pass. The 1M-row fixture is spike-only and is not exercised on every CI job.
- **D-08:** Fixture generation **extends `tools/corpus/`** (the existing seeded, byte-identical-regeneration framework from Phase 1) rather than introducing a second generator mechanism.

**Pod-kill / retry demonstration (success criterion #3)**
- **D-09:** The deliberate mid-load pod kill is a **real `kubectl delete pod`** against a pod loading the ~1M row fixture (not a self-kill via a test-only crash env var) — exercises the genuine Kubernetes reschedule + Airflow retry + run-claim lease-takeover path, not a self-inflicted process exit.
- **D-10:** This becomes a **permanent automated E2E test** (`tests/e2e/`), not a one-off manual proof — matches QUAL-06/QUAL-09 and the project's QUAL-07 policy that important behaviors get a permanent regression test.
- **D-11:** The test detects "pod is mid-load" by **polling `meta.ingestion_runs.rows_read` with a timeout** (never `sleep N` — PITFALLS.md explicitly flags `sleep` in E2E tests as a permanent-flakiness trap), reusing the platform's own heartbeat/lease mechanism (`lease_expires_at`) rather than watching pod logs for a marker string.
- **D-12:** Success criterion #3's second half — "a concurrent SELECT never observes a half-loaded table" — gets its **own dedicated test**: a concurrent connection polls `normalized.customers` during an in-flight publish and asserts it only ever observes the pre-publish or fully-published row count, never a partial state. Not left as an inference from the retry test.
- **D-13:** `configs/datasets/customers.yaml` gets an explicit **duplicate-file-content policy of `skip`** — when a re-uploaded file's content hash (`content_sha256`) matches a file already known for this dataset, it is recorded (`duplicate_of_file_id` set) but never (re)processed. This is what makes success criterion #2's "re-uploading the same file under a different name produces zero additional rows" true by early-exit rather than by relying on deeper record/publish-layer guards.

**Local dev/demo workflow**
- **D-14:** A **Makefile target**, `make ingest-demo FILE=<path>`, is the developer-facing way to exercise the slice — consistent with this repo's existing `make cluster-up` / `make doctor` / `make fixtures` convention.
- **D-15:** The target does **not** bypass the sensor by also triggering the DAG via CLI. Explicit user instruction: "Do not take shortcuts for demo, quick tests... let sensor do its job." The demo must exercise the real unattended path, not a dev-only shortcut — `mc cp` the file in and let the `S3KeySensor` notice it.
- **D-16:** While waiting, the target **polls `meta.ingestion_runs` (with a timeout, not a blind sleep) and prints the receipt** (`run_id`, `status`, `rows_loaded`, `duration_ms`, etc.) once the run reaches a terminal status — self-contained feedback without needing to switch to the Airflow UI.

### Claude's Discretion
- Exact Makefile target implementation details (how it resolves the run row for a given uploaded file, exact receipt formatting).
- Whether `tools/corpus/`'s existing generator needs structural changes to support realistic (non-edge-case) Faker-style data, versus adding a new generation path within that same package.

### Deferred Ideas (OUT OF SCOPE)
None — discussion stayed within phase scope. (MinIO-webhook-based triggering was considered and explicitly rejected for this phase per D-01, not deferred as a future idea — ROADMAP's Phase 11 ops/runbook work or a later phase would be the natural place to revisit it if ever wanted.)
</user_constraints>

## Summary

This research resolves the six concrete gaps the phase description named, plus five more that only became visible by reading the actual committed code and the live cluster rather than the design documents alone. The single most consequential finding: **ARCHITECTURE.md's own worked publication-transaction example uses `MERGE`, but this contradicts LOAD-09 and PITFALLS.md's own C1 entry, which the ROADMAP plan guidance for this exact phase explicitly cites — `MERGE` is not concurrency-safe under PostgreSQL's snapshot semantics (`BUG #18279`; both transactions take the `WHEN NOT MATCHED` branch and the loser raises a unique-violation) and `INSERT … ON CONFLICT` is the verified-correct primitive** `[VERIFIED: postgresql.org — INSERT/UPSERT semantics fetched this session]`. This is resolved concretely below, against `normalized.customers`'s real columns — including a **migration gap nobody had flagged**: the table currently has only a plain, non-unique index on `customer_id` (migration `0005`'s own docstring explains this was deliberate, written *for* `MERGE`), so `ON CONFLICT (customer_id)` will fail with `there is no unique or exclusion constraint matching the ON CONFLICT specification` until a new migration adds a real `UNIQUE` constraint. A second, independently-verified correction: `ARCHITECTURE.md`/`STACK.md`'s staging-table snippet uses `CREATE UNLOGGED TABLE … ON COMMIT DROP`, but `ON COMMIT` is documented as applying only to `TEMPORARY` tables — an `UNLOGGED` table needs an explicit `DROP TABLE` after publication, not an `ON COMMIT` clause.

Three further findings materially change what this phase must build. First, **DAG source distribution to Airflow is not wired up yet** — `kind/cluster.yaml` already bind-mounts `airflow/dags/` into every node at `/mnt/dags` with an explicit code comment ("wired... in Phase 4"), but neither `helm/values/local/airflow.yaml` nor the CI counterpart configures `extraVolumes`/`extraVolumeMounts` to actually mount it into any Airflow component pod — this phase cannot run a DAG at all until that Helm-values wiring lands. Second, **no new Airflow container image is needed**: the stock `apache/airflow:3.3.0-python3.12` image (already deployed by Phase 2, confirmed live on the cluster) bundles the `amazon` and `cncf-kubernetes` provider extras by default, so `S3KeySensor` and `KubernetesPodOperator` are importable with zero Dockerfile work — `docker/airflow/` can stay empty through this phase. Third, the canonical references list the KPO pod's namespace as `data-etl`; the actual, live `kubernetes/namespaces.yaml` names it **`etl`** — every RBAC/ServiceAccount/Vault-role decision in this phase and Phase 5 must use `etl`, and the live cluster confirms it exists and is currently empty (no ServiceAccount, no RBAC, no pods).

**Primary recommendation:** build two DAG files (not one) — a permanent, deliberately trivial smoke DAG (U1) and `csv_ingest_customers.py` — against the stock Airflow image; wire the hostPath DAG mount via Helm `extraVolumes`/`extraVolumeMounts`; add a migration converting `normalized.customers`'s plain index to a real `UNIQUE(customer_id)` constraint; implement publication as `pg_advisory_xact_lock` + `INSERT … ON CONFLICT (customer_id) DO UPDATE … WHERE` (never `MERGE`); and declare the `etl` namespace + a `csv-processor` ServiceAccount + narrowly-scoped RBAC now, so Phase 5 only has to bind a Vault role to an identity that already exists.

## Architectural Responsibility Map

This platform has no browser/frontend tier — Airflow's own UI is the only interface (README/CLAUDE.md constraint) — so the standard web-app tier vocabulary is adapted to this ETL platform's real architecture: **Orchestration** (Airflow control plane: scheduler, DAG processor, triggerer — decides *when* and *what*, never *how*) stands in for "Frontend Server"; **Execution** (Kubernetes task pods running `dataplat`/`csv_processor`) stands in for "API/Backend" (where business logic actually runs); **Object Storage** (MinIO) stands in for "CDN/Static" (durable, addressable artifacts); **Database** (analytical PostgreSQL) is unchanged.

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| File-arrival detection (`S3KeySensor`) | Orchestration | Object Storage | The sensor is Airflow-native control flow; it reads MinIO but owns no data |
| Config version resolution (`resolve_config`) | Orchestration | Database | Thin `@task` wrapper over `dataplat.config.registry.ConfigRegistry`, already a library call, not DAG-authored SQL |
| File discovery + frozen manifest authoring | Orchestration | Object Storage + Database | Writes `meta.files` rows and one assignment JSON per unit — orchestration-owned bookkeeping, not CSV business logic |
| CSV parsing, normalization, hashing | Execution | — | `csv_processor`/`dataplat` inside the KPO pod; ORCH-02/ORCH-06 forbid this in the DAG or scheduler |
| Staging COPY (chunked) | Execution | Database | Runs inside the pod; writes to `staging.*`, never inside the publication transaction |
| Publication transaction (advisory lock + `ON CONFLICT` + run/file/batch status) | Execution | Database | The pod holds the transaction; the database enforces the constraint that makes it safe under concurrency |
| Run-claim / idempotency-key upsert | Execution | Database | The pod claims its own run row at startup; the database's `UNIQUE(idempotency_key)` is the actual enforcement point |
| Receipt construction (≤4 KB XCom) | Execution | Orchestration | Written by the pod, read by the DAG's aggregation task |
| Pod identity (namespace, ServiceAccount, RBAC) | Orchestration | Execution | Declared by the DAG's KPO args and by cluster-level RBAC manifests, but the *effect* (what the pod is permitted to touch) lands on the Execution tier |
| Developer demo tooling (`make ingest-demo`) | — (external, dev workstation) | Object Storage | A local script that uploads to MinIO and polls the database; not part of the running platform |

## Phase Requirements

<phase_requirements>
| ID | Description | Research Support |
|----|-------------|------------------|
| ORCH-01 | DAGs written with the TaskFlow API (`@dag`, `@task`) | §"Two DAG files" pattern below; import from `airflow.sdk`, never `airflow.decorators`/`airflow.models` (STACK.md gotcha 1, still current for 3.3.0) |
| ORCH-02 | ETL workloads execute in KPO pods, never scheduler/DAG processor | `csv_ingest_customers.py` structure below; AP1 in Common Pitfalls |
| ORCH-03 | Dynamic Task Mapping fans work across pods, bounded map length | `[core] max_map_length` (default 1024) + `batching.max_units_per_run` config knob, covered under Don't Hand-Roll and Common Pitfalls |
| ORCH-04 | Explicit retry/failure behaviour; supports backfill | `retries=3, retry_exponential_backoff=True` on the KPO task; backfill-of-a-sensor-DAG nuance flagged in Open Questions |
| ORCH-05 | Derive processing window from logical date/data interval, tolerate `logical_date=None` | `resolve_window`-style guard, Code Examples; reasoning on why this DAG needs it even though it also carries a real schedule, Open Questions |
| ORCH-06 | DAG files <150 lines, no parsing/validation/typing/DB writes | Two-DAG-file line-budget estimate below; both DAGs call only `dataplat`/`csv_processor` functions |
| ORCH-07 | Dataset dependencies via Assets or sensors | `S3KeySensor`, D-01 (locked) |
| ORCH-08 | Dynamic Task Mapping expands over a frozen manifest, never a live listing | `discover_files` writes `meta.files` + assignment JSON *before* `.expand()` reads only identifiers back; Don't Hand-Roll |
| ORCH-09 | Every task pod declares CPU/memory requests+limits | `container_resources` on the KPO task, Code Examples; existing `workers.kubernetes.resources` precedent in `helm/values/local/airflow.yaml` |
| META-03 | Rows, watermark advance, run status commit in one transaction or none do | §"What META-03 concretely means for this phase" — resolved against the *real* migrated schema (no `meta.watermarks` table exists yet) |
| LOAD-01/02 | No duplicate/corrupted data across retries | Run-claim protocol + advisory lock, Code Examples |
| LOAD-03 | Reprocessing an identical file is a no-op, by content checksum | D-13 (locked) + `meta.files.duplicate_of_file_id`, already migrated |
| LOAD-04 | File/batch/record/target-row identity modelled distinctly | Already migrated (`meta.files`, `meta.batches`, `_source_row_number`, business-key `UNIQUE`) — this phase populates it |
| LOAD-05 | Transactional loads: staging → validate → atomic publication | Staging/publication split, Code Examples; `UNLOGGED` + explicit `DROP TABLE` correction |
| LOAD-08 | Batch ledger `UNIQUE(dataset,batch_key)` + run-scoped identity on every row | Already migrated; this phase is first to populate `_run_id`/`_file_id`/`_batch_id`/`_source_row_number` |
| LOAD-09 | Single-writer publication via `pg_advisory_xact_lock` + `ON CONFLICT`; `MERGE` rejected | §Summary's headline finding; full SQL in Code Examples |
| LOAD-12 | Processor is the only CSV parser; `COPY … FORMAT csv` prohibited | Already enforced by a Phase-1 CI grep (per ROADMAP); restated in Common Pitfalls |
| INCR-08 | Business date from data, never clock/`logical_date` | §"What INCR-08 concretely means for this phase" — `meta.files.business_date` legitimately stays `NULL` this phase |
| QUAL-05 | Integration tests: MinIO → processor → PostgreSQL | `tests/integration/` extension pattern, Validation Architecture |
| QUAL-06 | E2E tests: CSV → MinIO → Airflow → Kubernetes → processor → PostgreSQL | New `tests/e2e/slice/` directory, Validation Architecture |
| QUAL-09 | Idempotency tested; re-run produces zero additional rows | D-07 fixture + Validation Architecture test map |
</phase_requirements>

## Project Constraints (from CLAUDE.md)

Directives that bind this phase's plan, extracted from `.claude/CLAUDE.md`:

- **Platform**: kind multi-node cluster only; Docker Compose forbidden as a workload platform. *(Already satisfied — the live cluster is the target for every test in this phase.)*
- **Database topology**: Airflow metadata PostgreSQL must never host analytical data. *(This phase writes exclusively to the analytical PostgreSQL's `meta`/`staging`/`normalized` schemas; `PostgresMetadataRepository` already asserts this separation structurally by construction — it only ever receives the analytical pool.)*
- **Storage access**: applications address data as `s3://bucket/path`, never a filesystem path. *(`ObjectStore`/`S3ObjectStore` already enforce this; the KPO pod must never receive a hostPath into `raw/`.)*
- **Raw immutability**: append-only; corrections are new files/versions, never overwrites. *(Satisfied by construction — this phase never writes to `raw/`, only reads.)*
- **Logic placement**: business logic lives in `dataplat`/`csv_processor`; DAGs orchestrate and delegate; heavy processing runs in task pods, never the scheduler. *(ORCH-02/ORCH-06, directly enforced by the two-DAG-file design below.)*
- **Secrets**: no credential in Git, Python source, Dockerfiles, manifests, Airflow Variables or CI files. *(This phase's dev-only DSN lives in a Kubernetes Secret, referenced by name only — `SecretsResolver`'s `env://`/`file://` schemes, already built in Phase 3 — never a literal.)*
- **Determinism**: same source + config + processor version → same result; unavoidable non-determinism must be documented. *(Directly relevant to the Faker-vs-deterministic-corpus-generator question resolved below.)*
- **CI runner sizing**: the two-profile (`values-local.yaml`/`values-ci.yaml`) split already exists and must not regress; this phase's Helm-values additions (DAG mount, KPO resource defaults) must be added to *both* files, per the project's own D-06 divergence-axis discipline already established in those files.
- **Filesystem**: repo stays on WSL ext4; never hostPath-mount `dags/` from `/mnt/c`. *(Already satisfied — `kind/cluster.yaml`'s `extraMounts` point at `/home/konutec/projects/airflow-platform/airflow/dags`, confirmed on ext4.)*
- **No secrets in fixtures**: the CSV corpus is synthetic by construction. *(Directly governs the Faker-style fixture design below — no real Faker package, no non-reproducible data.)*

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `apache-airflow` | `3.3.0` (image `apache/airflow:3.3.0-python3.12`) | Orchestrator | Already deployed (Phase 2, confirmed live: `airflow-api-server`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer` all `Running` in namespace `airflow`) `[VERIFIED: live cluster, kubectl get pods -n airflow]` |
| `apache-airflow-providers-cncf-kubernetes` | `10.19.0` (pinned by Airflow 3.3.0's own constraints file; PyPI latest is `10.21.0`) | `KubernetesPodOperator`, XCom sidecar | Already bundled in the stock image's default extras — no image rebuild needed `[VERIFIED: constraints-3.3.0/constraints-3.12.txt, fetched this session]` |
| `apache-airflow-providers-amazon` | `9.31.0` (pinned by Airflow 3.3.0's constraints file; PyPI latest `9.34.0`) | `S3KeySensor` (`airflow.providers.amazon.aws.sensors.s3.S3KeySensor`) | **New to this phase's Standard Stack** — not previously named in CLAUDE.md's STACK section. Also bundled in the stock image's default extras (`amazon` is one of the documented default Docker-image extras) — no Dockerfile change needed `[VERIFIED: constraints-3.3.0/constraints-3.12.txt + official Docker image default-extras list]` |
| `psycopg[binary,pool]` | `3.3.4` | Staging `COPY`, publication transaction | Already a `dataplat` dependency (`packages/dataplat/pyproject.toml`); this phase is the first to actually call `cursor.copy()` |
| `boto3` | `1.43.68` | Object-store reads (already used) and this phase's new upload path (`make ingest-demo`) | Already a `dataplat` dependency; recommended over introducing `mc`/`aws-cli` — see Don't Hand-Roll |

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `structlog` | `26,<27` (already pinned) | Per-chunk heartbeat/progress logging inside the pod | Already wired via `dataplat.observability.logging`; this phase's staging loop should emit one log line per chunk (PITFALLS B4: silence is indistinguishable from death when `get_logs=True` streaming breaks) |
| `pydantic` | `2.13,<3` (already pinned) | Validating the assignment JSON document the pod reads | New model needed this phase: `AssignmentDocument` (or similar), `extra="forbid"`, matching the existing `DatasetConfig` convention — see Security Domain |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `S3KeySensor` (deferrable) | MinIO event notification → Airflow REST API webhook | Rejected by D-01: needs a webhook receiver plus an Airflow API credential with nowhere to live before Vault (Phase 5) |
| `S3KeySensor` (deferrable) | Plain scheduled polling DAG (`schedule=timedelta(...)`, no sensor) | Rejected by D-01: coarser, schedule-interval-bound latency; wastes a full DagRun+worker cycle even when nothing changed |
| `INSERT … ON CONFLICT` | `MERGE` (PostgreSQL 15+) | Rejected — not concurrency-safe (PostgreSQL BUG #18279; both transactions can take `WHEN NOT MATCHED` against the same snapshot). See Summary and Code Examples |
| Building `docker/airflow/Dockerfile` now | Use the stock `apache/airflow:3.3.0-python3.12` image as-is | Recommended: stock image already bundles `amazon`+`cncf-kubernetes`; no phase requirement needs anything the stock image lacks |
| `mc` (MinIO client CLI) for `make ingest-demo`'s upload step | `boto3` (already a pinned dependency, already wrapped by `dataplat.storage.objectstore`) | Recommended: `mc` is not installed on this workstation, not vendored anywhere in `tools/bin` (unlike `kind`/`helm`/`kubeconform`), and not referenced by any existing script — introducing it adds a new external binary dependency for no capability boto3 lacks |
| Real `Faker` PyPI package for slice fixtures | Extend `tools/corpus/manifest.py`'s `ColumnSpec` union with new deterministic column kinds | Rejected — see "Faker-style fixtures" in Architecture Patterns |

**Installation** (only one new package versus what's already pinned):
```bash
# Airflow image: apache-airflow-providers-amazon is ALREADY present by default —
# no Dockerfile change and no `uv add` needed. Verify with:
#   kubectl exec -n airflow deploy/airflow-scheduler -c scheduler -- \
#     python -c "import airflow.providers.amazon; print(airflow.providers.amazon.__file__)"
```

**Version verification:** confirmed against the authoritative constraints file for the exact pinned Airflow release, not PyPI's "latest":
```bash
curl -s https://raw.githubusercontent.com/apache/airflow/constraints-3.3.0/constraints-3.12.txt \
  | grep -i "^apache-airflow-providers-amazon\|^apache-airflow-providers-cncf-kubernetes"
# apache-airflow-providers-amazon==9.31.0
# apache-airflow-providers-cncf-kubernetes==10.19.0
```

## Package Legitimacy Audit

Only one externally-new package enters this phase's scope (`apache-airflow-providers-amazon`); every other library used is already a dependency of an existing, committed `pyproject.toml`.

| Package | Registry | Age | Downloads | Source Repo | slopcheck | Disposition |
|---------|----------|-----|-----------|-------------|-----------|-------------|
| `apache-airflow-providers-amazon` | PyPI | Years (part of the `apache/airflow` monorepo's provider set since Airflow 2.x) | Very high (core Airflow provider, millions/month at the `apache-airflow` project level) | `github.com/apache/airflow` (monorepo) | `[OK]` | Approved |

```
$ slopcheck install apache-airflow-providers-amazon
  [OK] apache-airflow-providers-amazon (pypi)
  scanned 1 packages — 1 OK
```

**Packages removed due to slopcheck `[SLOP]` verdict:** none.
**Packages flagged as suspicious `[SUS]`:** none.

Package-name provenance note: `apache-airflow-providers-amazon` was first surfaced via WebSearch, then independently confirmed against the **official, pinned `constraints-3.3.0/constraints-3.12.txt`** (an authoritative source — the exact file Airflow's own installation instructions require) and against the **official Apache Airflow Docker image's documented default extras list**. Per the provenance rule, this combination of official-source confirmation + passing `slopcheck` qualifies it for `[VERIFIED]`, not merely `[ASSUMED]`.

## Architecture Patterns

### System Architecture Diagram

```
 developer / make ingest-demo                 kubectl delete pod (D-09, permanent E2E test)
        │  boto3 PutObject                            │
        ▼                                              ▼
 ┌─────────────────────┐                     ┌──────────────────────────┐
 │  MinIO  raw/         │  (versioned,        │  KPO pod (mid-load)      │
 │  customers/*.csv     │   deny-delete)      │  killed → K8s reschedules│
 └──────────┬───────────┘                     └────────────┬─────────────┘
            │ 30s poke (deferred to triggerer, 0 worker slots)
            ▼
 ┌──────────────────────────────────────────────────────────────────────┐
 │ Airflow DAG  csv_ingest_customers   (max_active_runs=1)              │
 │                                                                       │
 │  S3KeySensor(deferrable=True) ──▶ @task resolve_config               │
 │        │                                │                            │
 │        │                                ▼                            │
 │        │                        @task discover_files                 │
 │        │                          • sha256 each new object            │
 │        │                          • INSERT/lookup meta.files          │
 │        │                          • pre-allocate meta.ingestion_runs  │
 │        │                            (status=PENDING, upsert-shaped)   │
 │        │                          • PUT one assignment JSON per file  │
 │        │                            to s3://metadata/assignments/…    │
 │        │                          • returns [{assignment_uri,        │
 │        │                             idempotency_key, run_id}, …]    │
 │        │                                │                            │
 │        │                                ▼                            │
 │        │               KubernetesPodOperator.partial(…).expand(…)    │
 │        │                 namespace=etl, sa=csv-processor,             │
 │        │                 container_resources=…, do_xcom_push=True    │
 │        │                                │  one pod per file           │
 │        │                                ▼                            │
 │        │                    ┌───────────────────────────────────┐   │
 │        │                    │ ETL pod (dataplat ingest --assign…)│  │
 │        │                    │ 1. GET assignment JSON  ◀── MinIO   │  │
 │        │                    │ 2. claim run: ON CONFLICT upsert    │  │
 │        │                    │    (idempotency_key) ── meta        │  │
 │        │                    │ 3. GET object, stream, chunk        │  │
 │        │                    │ 4. per-chunk COPY ── staging.*      │  │
 │        │                    │    (heartbeat lease_expires_at)     │  │
 │        │                    │ 5. BEGIN;                           │  │
 │        │                    │    pg_advisory_xact_lock(…);        │  │
 │        │                    │    INSERT…ON CONFLICT…WHERE…;       │  │
 │        │                    │    UPDATE meta.files.status;        │  │
 │        │                    │    UPDATE meta.batches.status;      │  │
 │        │                    │    UPDATE meta.ingestion_runs;      │  │
 │        │                    │    COMMIT;   ◀── the ONE transaction│  │
 │        │                    │ 6. DROP TABLE staging.* (explicit)  │  │
 │        │                    │ 7. echo receipt ≤4KB ── XCom sidecar│  │
 │        │                    └───────────────────────────────────┘   │
 │        │                                │                            │
 │        │                                ▼                            │
 │        │                       @task aggregate_receipts              │
 │        ▼                                │                            │
 │  (sensor re-arms for the next          ▼                            │
 │   DagRun once this one completes) DAG run ends                       │
 └──────────────────────────────────────────────────────────────────────┘
                          │
                          ▼
        ┌───────────────────────────────────────┐
        │ Analytical PostgreSQL                  │
        │  normalized.customers  (+_run_id, …)   │
        │  meta.ingestion_runs   SUCCEEDED        │◀── concurrent SELECT test (D-12)
        │  meta.files            PROCESSED        │    polls row-count during publish,
        │  meta.batches          PUBLISHED        │    asserts only pre/post counts seen
        └───────────────────────────────────────┘
```

### Recommended Project Structure

```
airflow/dags/
├── smoke_kubernetes_pod.py        # U1 — permanent platform smoke test, <30 lines
├── csv_ingest_customers.py        # the slice itself, target <150 lines (ORCH-06)
└── _common/                       # ONLY if the 150-line budget gets tight — shared
                                    # KPO-builder helper, NEVER business logic (ARCHITECTURE.md
                                    # structure rationale). Not needed unless measured.

packages/dataplat/src/dataplat/
├── load/
│   ├── staging.py                 # NEW — StagingLoader: CREATE/DROP staging.<ds>__r<run_id>,
│   │                               #        chunked COPY, per-chunk heartbeat log line
│   └── publish/
│       ├── protocol.py            # EXISTING (Phase 3) — Publisher protocol, unchanged
│       ├── registry.py            # NEW — PUBLISHER_REGISTRY: dict[str, Publisher], NOT
│       │                          #        entry-points yet (see Don't Hand-Roll)
│       └── merge.py               # NEW — the FIRST concrete Publisher: "merge" strategy,
│                                  #        implements advisory-lock + ON CONFLICT (NOT MERGE)
├── sources/
│   ├── protocol.py                # EXISTING (Phase 3) — Source protocol, unchanged
│   └── registry.py                # NEW — SOURCE_REGISTRY: dict[str, Source]; registers
│                                  #        csv_processor.source.CsvSource under "csv"
├── metadata/
│   ├── repository.py              # EXTEND — add claim_ingestion_run() and
│   │                              #        get_or_create_ingestion_run() (see Code Examples)
│   └── postgres.py                # EXTEND — implementations of the above
└── cli.py                         # EXTEND — new `ingest` subcommand (attaches to existing
                                   #        `cli` click group per 04-CONTEXT.md's Integration Points)

migrations/versions/
└── 0006_normalized_customers_business_key_unique.py   # NEW — required before ON CONFLICT works

kubernetes/
└── rbac-etl.yaml                  # NEW — ServiceAccount csv-processor in ns etl; Role granting
                                   #        get/list/watch/create/delete on pods, pods/log in
                                   #        etl; RoleBinding to the Airflow scheduler's SA

helm/values/{local,ci}/airflow.yaml   # EXTEND — extraVolumes/extraVolumeMounts wiring the
                                       #        kind hostPath DAG mount into scheduler +
                                       #        dagProcessor + workers.kubernetes pod template

tools/corpus/
└── manifest.py, generators.py     # EXTEND — new deterministic ColumnSpec kinds for
                                   #        Faker-style (not literal Faker) realistic data
tests/fixtures/
└── slice-corpus.yaml              # NEW — a SEPARATE manifest from tests/fixtures/corpus.yaml
                                   #        (that one is the edge-case corpus; this one is
                                   #        realistic uniform data), same generate/verify CLI

tests/e2e/
└── slice/                         # NEW — pod-kill retry test (D-09..D-11), concurrent-SELECT
                                   #        atomicity test (D-12), rerun/re-upload idempotency
```

### Pattern 1: The publication transaction, exactly — `pg_advisory_xact_lock` + `INSERT … ON CONFLICT`, never `MERGE`

**What:** Single-writer publication per dataset, arbitrating on the real business-key uniqueness constraint.
**When:** Every publish call this phase's `merge` `Publisher` makes.
**Why not `MERGE`:** `[VERIFIED: postgresql.org/docs/current/sql-insert.html, fetched this session]` — `ON CONFLICT`'s `conflict_target` "must reference a unique index... or `NOT DEFERRABLE` unique constraint"; a row whose `WHERE` clause on `DO UPDATE` evaluates false is "locked but left unchanged" and correctly excluded from `RETURNING`; `EXCLUDED` is accessible inside that `WHERE` clause. This is exactly the mechanism PITFALLS.md's C1 entry names as the fix for `MERGE`'s documented concurrency failure (`PostgreSQL BUG #18279`; both concurrent transactions evaluate `WHEN NOT MATCHED` against their own snapshot and both attempt `INSERT`).

**Precondition this phase must create — migration `0006`:**
```python
# migrations/versions/0006_normalized_customers_business_key_unique.py
"""normalized.customers.customer_id needs a UNIQUE constraint before
INSERT ... ON CONFLICT (customer_id) can arbitrate on it.

Migration 0005's own docstring documents that only a plain, non-unique
index was created deliberately, "since a target row's uniqueness constraint
here would fight, not support, MERGE ... WHEN MATCHED". LOAD-09/PITFALLS #14
reject MERGE for this exact table, so that reasoning no longer applies:
ON CONFLICT requires the unique constraint MERGE was avoiding.
"""
def upgrade() -> None:
    op.drop_index("ix_customers_customer_id", table_name="customers", schema="normalized")
    op.create_unique_constraint(
        "uq_customers_customer_id", "customers", ["customer_id"], schema="normalized"
    )

def downgrade() -> None:
    op.drop_constraint("uq_customers_customer_id", "customers", schema="normalized", type_="unique")
    op.create_index("ix_customers_customer_id", "customers", ["customer_id"], schema="normalized")
```
(A `UNIQUE` constraint creates its own backing B-tree index, so this migration loses nothing the plain index provided.)

**The publication statement**, translating both of ARCHITECTURE.md's `WHEN MATCHED` guards into `ON CONFLICT ... WHERE`:
```sql
BEGIN;

SELECT pg_advisory_xact_lock(hashtextextended('publish:customers', 0));  -- single writer per dataset

INSERT INTO normalized.customers (
    customer_id, name, country, birth_date, event_ts,
    _run_id, _file_id, _batch_id, _source_row_number,
    _record_hash, _record_hash_version
)
SELECT DISTINCT ON (customer_id)
       customer_id::int, name, country, birth_date::date, event_ts::timestamptz,
       _run_id, _file_id, _batch_id, _source_row_number,
       _record_hash, _record_hash_version
FROM   staging.customers__r8123
ORDER  BY customer_id, event_ts DESC, _source_row_number DESC   -- deterministic tiebreak (C1/C9)
ON CONFLICT (customer_id) DO UPDATE
   SET name = EXCLUDED.name, country = EXCLUDED.country,
       birth_date = EXCLUDED.birth_date, event_ts = EXCLUDED.event_ts,
       _record_hash = EXCLUDED._record_hash,
       _record_hash_version = EXCLUDED._record_hash_version,
       _run_id = EXCLUDED._run_id, _file_id = EXCLUDED._file_id,
       _batch_id = EXCLUDED._batch_id, _source_row_number = EXCLUDED._source_row_number
 WHERE normalized.customers._record_hash IS DISTINCT FROM EXCLUDED._record_hash  -- suppress no-op writes
   AND EXCLUDED.event_ts >= normalized.customers.event_ts;                       -- late data never clobbers newer

UPDATE meta.files    SET status = 'PROCESSED' WHERE file_id  = :file_id;
UPDATE meta.batches  SET status = 'PUBLISHED' WHERE batch_id = :batch_id;
UPDATE meta.ingestion_runs
   SET status='SUCCEEDED', finished_at=now(), rows_loaded=:n, report_uri=:uri
 WHERE run_id = :run_id;

COMMIT;
```
`rows_loaded` is `cur.rowcount` after the `INSERT ... ON CONFLICT` executes — no need for `MERGE`'s `RETURNING merge_action()` this phase; per-row insert/update auditing (`meta.dedup_decisions`) is explicitly a Phase 9 table (ARCHITECTURE.md §2.4), out of this phase's scope.

**The `DISTINCT ON` dedup step happening *inside* the publish `SELECT`, always** — this is required even though `configs/datasets/customers.yaml` already declares `deduplication.strategy: business_key_latest`: per PITFALLS C1, an `ON CONFLICT DO UPDATE` whose source contains duplicate keys raises `ON CONFLICT DO UPDATE command cannot affect row a second time` — a CSV batch containing the same `customer_id` twice is not an edge case, it is what the dedup config exists to handle, so the guard must be structural, not merely configured correctly upstream.

### Pattern 2: Two different upserts against `meta.ingestion_runs` — do not conflate them

ARCHITECTURE.md's Q7 shows the pod's *own* claim-at-startup upsert in detail but only says the discovery task "pre-allocate[s]... rows (status=PENDING)" without giving its shape. Reading the real schema and the already-implemented `PostgresMetadataRepository` surfaces that **these are two different SQL statements with two different jobs**, and conflating them will either raise `UniqueViolation` on a re-run of `discover_files`, or silently let the pod's claim clobber a concurrently-running attempt's status.

**1. Discovery-time pre-allocation** (`discover_files`, called once per file, must tolerate being re-run for a file it already registered a run for): reuse the exact idiom `PostgresMetadataRepository.get_or_create_dataset` and `ConfigRegistry._resolve_dataset_id` **already establish** in this codebase — a no-op `DO UPDATE` purely so `RETURNING` yields the existing row:
```python
# NEW method on MetadataRepository/PostgresMetadataRepository — get_or_create_ingestion_run
row = conn.execute(
    """
    INSERT INTO meta.ingestion_runs (idempotency_key, dataset_id, file_id, batch_id,
                                      config_version_id, processor_version,
                                      processor_image_digest, status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, 'PENDING')
    ON CONFLICT (idempotency_key) DO UPDATE
        SET idempotency_key = EXCLUDED.idempotency_key   -- no-op; only makes RETURNING work
    RETURNING run_id, status
    """,
    (idempotency_key, dataset_id, file_id, batch_id, config_version_id,
     processor_version, processor_image_digest),
).fetchone()
```

**2. The pod's own claim at startup** (transitions `PENDING`/`FAILED`/dead-`RUNNING` → `RUNNING`, or detects it should exit early) is ARCHITECTURE.md's Q7 SQL, column-names verified against migration `0004` — every column it names (`idempotency_key`, `status`, `started_at`, `lease_expires_at`, `try_number`, `k8s_pod_name`) exists exactly as spelled:
```python
# NEW method — claim_ingestion_run
row = conn.execute(
    """
    UPDATE meta.ingestion_runs
       SET status = 'RUNNING', try_number = %(try_number)s,
           k8s_pod_name = %(pod_name)s, started_at = COALESCE(started_at, now()),
           lease_expires_at = now() + interval '5 minutes'
     WHERE idempotency_key = %(key)s
       AND (status IN ('PENDING', 'FAILED')
            OR (status = 'RUNNING' AND lease_expires_at < now()))
    RETURNING run_id, status
    """,
    {"key": idempotency_key, "try_number": try_number, "pod_name": pod_name},
).fetchone()
# row is None and a SUCCEEDED row exists ⇒ SKIPPED_DUPLICATE, exit 0
# row is None and a live-leased RUNNING row exists ⇒ CONCURRENT_RUN, exit 0
```

### Pattern 3: Wiring the already-declared hostPath DAG mount

`kind/cluster.yaml` already bind-mounts `airflow/dags/` (WSL ext4) into every node at `/mnt/dags`, read-only, with the comment "wired (mounted into the scheduler/dag-processor pods) in Phase 4" — but neither `helm/values/local/airflow.yaml` nor `helm/values/ci/airflow.yaml` (both fully read this session) contains a `dags:` key or `extraVolumes`/`extraVolumeMounts`. The chart's *own* `dags:` default is `persistence.enabled: true` (a 100Gi PVC) — the wrong mechanism entirely for a node-local hostPath. `[VERIFIED: raw helm-chart/1.22.0 chart/values.yaml, fetched this session]` confirms `extraVolumes`/`extraVolumeMounts` exist as a per-component pattern on scheduler, dagProcessor, workers, and the API server.

```yaml
# helm/values/{local,ci}/airflow.yaml — ADD
dags:
  persistence:
    enabled: false        # explicitly turn off the chart's own PVC mechanism
  gitSync:
    enabled: false         # already implicitly false; stated for clarity

extraVolumes:
  - name: dags
    hostPath: { path: /mnt/dags, type: Directory }   # the path kind's extraMounts already bind

extraVolumeMounts:
  - name: dags
    mountPath: /opt/airflow/dags     # the chart's default dags_folder
    readOnly: true

# scheduler and dagProcessor inherit `extraVolumes`/`extraVolumeMounts` if the chart applies
# them globally; VERIFY against the pinned chart 1.22.0 values.yaml whether these keys are
# truly global or must be repeated per-component (scheduler.extraVolumeMounts,
# dagProcessor.extraVolumeMounts). MEDIUM confidence on this specific point — see Open Questions.
```
**Which components actually need the mount, and why (reasoned from Airflow 3's architecture, not merely asserted):** the DAG processor parses and serializes DAG structure to the metadata DB; the API server serves the UI/graph from that serialized structure, not from the files directly, so it likely does not strictly need the mount (low-cost to add anyway, to remove ambiguity). The scheduler needs it because `KubernetesExecutor` is a mode of the scheduler process, and the scheduler decides task readiness from serialized state — but **the ephemeral worker pod `KubernetesExecutor` spawns per task instance (a distinct pod from the KPO pod your DAG code launches) executes `airflow tasks run`, which must import the DAG's Python module to locate the task's callable.** That pod's pod-template, generated by the chart from `workers.*` values, must carry the same hostPath mount. Confirm the exact chart key (`workers.kubernetes.extraVolumes` per a webseach-summarized snippet — not independently grep-verified against the pinned chart's source this session) before relying on it; flagged in Open Questions.

### Pattern 4: `S3KeySensor`, deferrable, against MinIO

`[VERIFIED: airflow.apache.org/docs/apache-airflow-providers-amazon, fetched this session]` — import path `airflow.providers.amazon.aws.sensors.s3.S3KeySensor`; constructor accepts `bucket_key`, `bucket_name`, `wildcard_match`, `deferrable`, `aws_conn_id`. No MinIO-specific incompatibility surfaced in official docs; the mechanism is identical to real S3 — `endpoint_url` is set on the underlying Airflow **Connection** (`aws_conn_id`), in its `Extra` field, not on the sensor itself.

```python
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

wait_for_files = S3KeySensor(
    task_id="wait_for_files",
    bucket_name="raw",
    bucket_key="customers/*.csv",
    wildcard_match=True,
    aws_conn_id="minio_default",   # Connection's Extra: {"endpoint_url": "http://minio.data.svc:9000"}
    deferrable=True,
    poke_interval=30,               # D-02
    timeout=None,                   # or a bound — see Open Questions re: backfill semantics
)
```
The `aws_conn_id="minio_default"` naming choice matches the XCom-overflow config (`xcom_objectstorage_path = s3://minio_default@metadata/xcom`) already named in ARCHITECTURE.md §6.4 — reuse the same Connection ID rather than inventing a second one.

### Pattern 5: KubernetesPodOperator settings, confirmed against official docs this session

`[VERIFIED: airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/operators.html, fetched this session]`:
- **`on_finish_action` default is `delete_pod`**, not `delete_succeeded_pod` — STACK.md's recommendation is an explicit *override* of the default, not a restatement of it. Must be set explicitly.
- **`do_xcom_push`**: the docs describe the sidecar mechanism without stating a hard default; PITFALLS.md B2's "historically `False`" claim stands — set it explicitly regardless of source.
- The sidecar writes to `/airflow/xcom/return.json`; an invalid-JSON write fails the task even if the main container succeeded (confirmed: `echo 'hello' > return.json` fails, `echo '"hello"' > return.json` works) — write the receipt as the container entrypoint's unconditional final statement, valid JSON on every exit path.
- XComs are pushed **only for tasks reaching `State.SUCCESS`** — confirmed verbatim.
- `xcom_sidecar_container_security_context` exists as both a Connection-level default and a per-task override — needed under Pod Security Standards.

```python
ingest = KubernetesPodOperator.partial(
    task_id="ingest",
    image="localhost:5001/csv-processor:<git-sha>",   # local registry, never :latest
    cmds=["dataplat"],
    namespace="etl",                     # VERIFIED live: kubernetes/namespaces.yaml names it
                                          # `etl`, NOT `data-etl` (canonical_refs' text is wrong)
    service_account_name="csv-processor",  # NEW — does not exist yet; see kubernetes/rbac-etl.yaml
    do_xcom_push=True,                     # explicit — do not rely on either class's default
    on_finish_action="delete_succeeded_pod",  # explicit override of the chart default (delete_pod)
    get_logs=True,
    retries=3,
    retry_exponential_backoff=True,
    container_resources=k8s.V1ResourceRequirements(
        requests={"cpu": "500m", "memory": "1Gi"},
        limits={"cpu": "2", "memory": "4Gi"},
    ),
).expand(arguments=[["ingest", "--assignment", u["assignment_uri"]] for u in units])
```

### Faker-style fixtures: extend `tools/corpus/`, do not import the `Faker` package

`04-CONTEXT.md` explicitly leaves this to Claude's discretion, so this is a recommendation, not a locked decision. `[VERIFIED: faker.readthedocs.io, fetched this session]` — Faker's own documentation states plainly: *"Results are not guaranteed to be consistent across patch versions [because] datasets are kept updated."* Combined with `tools/corpus/generators.py`'s own R1/R2/R6 determinism rules (byte-identical regeneration from a seed, enforced in CI via `fixtures-verify` against a committed oracle), the actual `Faker` PyPI package is disqualified regardless of pinning its version exactly — a routine dependency refresh could silently re-baseline every downstream fixture with no code change to explain the diff. This directly contradicts the project's own Determinism constraint (CLAUDE.md) and QUAL-16 (a later phase's determinism property test).

**Recommendation:** add new `ColumnSpec` variants to `tools/corpus/manifest.py`'s existing union (`ZeroPaddedIntColumn | PickColumn | DecimalColumn | RepeatColumn`) that produce Faker-*style* (realistic-looking) values using the same `Random.random()`-only discipline the existing `PickColumn`/`_decimal_renderer` already follow — e.g. a composite name column (independent first/last `PickColumn`-style lists combined), and a date/timestamp column rendered via pure integer day-offset arithmetic (mirroring `_decimal_renderer`'s "convert bounds once, do integer arithmetic per row" pattern, never `datetime` object construction per row, R10-consistent). Country can already be expressed with the existing `PickColumn` (a small fixed ISO-code list).

**Where the slice fixtures live:** **not** as new entries in `tests/fixtures/corpus.yaml` — that manifest is QUAL-08's *edge-case* corpus (`covers: [REQ-IDs]` for CSV-parsing requirements; every fixture there specifies dialect/encoding pathologies). The customers slice fixtures are realistic, uniform, well-formed data for an E2E pipeline test — a different purpose. Recommend a **second, separate manifest** (`tests/fixtures/slice-corpus.yaml` or similar) driven through the *same*, already-generic `generate_corpus()`/`load_manifest()` functions (`__main__.py`'s `--manifest`/`--out` flags already make this trivial — no code change needed to support a second manifest file, only new fixture declarations and, if the discretion call above is taken, new `ColumnSpec` kinds).

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|--------------|-----|
| Run-claim / idempotency | An application-level "check if already processed" query-then-insert | `UNIQUE(idempotency_key)` + `INSERT ... ON CONFLICT ... WHERE ... RETURNING` | A race between the check and the insert is exactly the bug class this constraint exists to make impossible (C1, C5) |
| Publication upsert | Read-compare-write loop, or `MERGE` | `pg_advisory_xact_lock` + `INSERT ... ON CONFLICT` | See Summary/Pattern 1 — `MERGE` is not concurrency-safe here |
| CSV streaming reader | A new reader | `csv_processor.source.CsvSource`/`CsvRecordStream` (already built, Phase 3) | Already proven against the E1 embedded-newline/chunk-boundary hazard; this phase's pod entrypoint reads through it unchanged |
| Connection pooling | A new pool factory | `dataplat.storage.db.create_pool()` (already built) | The one place a `ConnectionPool` is constructed in the runtime path; `ConfigRegistry`/`PostgresMetadataRepository` already depend on this invariant |
| Config resolution + hashing | Re-implementing YAML merge/hash logic in the DAG | `dataplat.config.registry.ConfigRegistry.sync()` (already built) | Canonical-JSON sha256 hashing and version-bumping already implemented and tested |
| Structured logging | `print()` or ad hoc logging setup in the pod entrypoint | `dataplat.observability.logging.configure()` (already built, called once in `cli.py`) | OBS-02/OBS-03/OBS-05 already satisfied; new subcommand inherits it for free |
| Strategy resolution (`SOURCE_REGISTRY`/`PUBLISHER_REGISTRY`) | A full Python entry-points plugin system | A plain `dict[str, Source]` / `dict[str, Publisher]` module-level constant | ARCHITECTURE.md Q4.4 names entry points as the *eventual* mechanism, but nothing in this codebase implements it yet (no `[project.entry-points]` section exists in either `pyproject.toml`) and this phase has exactly one `Source` and one `Publisher`. Building the plugin-discovery machinery now, for a registry with one entry, is exactly the kind of premature generality this codebase's own `errors.py` docstring explicitly rejects ("a subclass with no raise site is dead code wearing a design decision's clothes") — a plain dict is trivially upgraded to entry points in Phase 10 when a second `Source`/`Publisher` actually exists |
| Dynamic Task Mapping fan-out cap | A DAG-level `if len(files) > N: raise` check | `[core] max_map_length` (platform-level, default 1024) + a `batching.max_units_per_run` dataset-config knob that groups files into units before `.expand()` | Exceeding `max_map_length` fails the *expansion task itself*, not a single mapped instance — the cap must be respected before `.expand()` is ever called, not discovered after |
| MinIO upload for the demo target | Shelling out to `mc` (not installed on this workstation; not vendored anywhere in this repo's `tools/bin`, unlike `kind`/`helm`/`kubeconform`) | `boto3` via a small Python helper (or reuse `S3ObjectStore`-adjacent code) | Zero new external binary; consistent with every other script in `scripts/` and `tests/integration/conftest.py`, none of which invoke `mc` |

**Key insight:** every "don't hand-roll" item above already has a home in this codebase or a documented, load-bearing reason for its absence (registries, entry points). The temptation this phase specifically invites is re-deriving the run-claim/publication SQL from ARCHITECTURE.md's worked example verbatim — but that example predates the LOAD-09/PITFALLS-C1 correction and must not be copied as-is.

## Common Pitfalls

### Pitfall 1: Copying ARCHITECTURE.md's `MERGE`-based publication SQL verbatim
**What goes wrong:** The Question-3/Question-7/Data-Flow sections of ARCHITECTURE.md all show `MERGE INTO normalized.customers ... WHEN MATCHED ... WHEN NOT MATCHED`. A planner or implementer working section-by-section from that document, without cross-referencing LOAD-09/PITFALLS.md C1/the ROADMAP's own Phase 4 plan guidance, will build exactly the concurrency bug the requirement exists to prevent.
**Why it happens:** ARCHITECTURE.md and PITFALLS.md are separate research documents from the same research pass; ARCHITECTURE.md's Q3 example was written before PITFALLS.md's C1 finding was reconciled back into it.
**How to avoid:** Use Pattern 1's SQL above, not ARCHITECTURE.md's. Add the concurrency test (two overlapping batches of the same dataset, both attempting `WHEN NOT MATCHED`-equivalent inserts) *before* the publication logic is complex enough to hide the bug, per C1's own recommendation.
**Warning signs:** Intermittent `duplicate key value violates unique constraint` or `cardinality_violation` under a concurrent-batch test that "shouldn't" be able to conflict.

### Pitfall 2: `CREATE UNLOGGED TABLE ... ON COMMIT DROP` — invalid, and semantically wrong even if it parsed
**What goes wrong:** STACK.md's own PostgreSQL driver section and ARCHITECTURE.md's §3.1 both describe `ON COMMIT DROP` as the staging-table cleanup mechanism. `[VERIFIED: postgresql.org/docs/current/sql-createtable.html]` — `ON COMMIT` is documented as controlling only `TEMPORARY` table behavior; the grammar presents `{TEMPORARY|TEMP}` and `UNLOGGED` as alternatives in the same bracketed choice, not combinable. Independent of the syntax question, `ON COMMIT DROP` would be semantically wrong for this design even if it worked: the staging table (`staging.<dataset>__r<run_id>`) must survive across potentially *multiple* chunked-`COPY` transactions and into the *separate* publication transaction that reads from it — an end-of-transaction drop would destroy the table after the very first `COPY` chunk commits.
**How to avoid:** Staging tables are `UNLOGGED` (for `COPY` speed) and are dropped by an **explicit `DROP TABLE`** issued by application code after the publication transaction commits. Each attempt should also begin with `DROP TABLE IF EXISTS staging.<dataset>__r<run_id>` before `CREATE`, which is simultaneously the fix for this pitfall and the concrete implementation of C5's "every attempt begins with an idempotent undo of its own prior partial work" rule — a retry always starts from a clean staging table regardless of what a crashed prior attempt left behind.
**Warning signs:** A syntax error on the very first `CREATE UNLOGGED TABLE` statement tested against a real PostgreSQL instance — cheap to catch, but only if tested against real PostgreSQL rather than assumed correct from the research documents.

### Pitfall 3: Assuming a new Airflow image build is needed
**What goes wrong:** Spending a plan wave building `docker/airflow/Dockerfile`, extending Airflow's constraints, etc., when the stock `apache/airflow:3.3.0-python3.12` image (already deployed, confirmed live) already bundles both `amazon` and `cncf-kubernetes` as default extras.
**How to avoid:** Verify first — `kubectl exec -n airflow deploy/airflow-scheduler -c scheduler -- python -c "import airflow.providers.amazon"` should succeed against the already-running deployment before any Dockerfile work is planned.
**Warning signs:** None yet observed — this is a preventive note based on this session's verified finding that neither provider needs installing.

### Pitfall 4: DAG-mount wiring silently missing, discovered only when the smoke DAG (U1) fails to appear in the UI
**What goes wrong:** `kind/cluster.yaml`'s `extraMounts` only makes `/mnt/dags` visible **on each node**; nothing inside any Airflow pod mounts it without the Helm-values `extraVolumes`/`extraVolumeMounts` addition. Building the smoke DAG file first and expecting it to "just appear" (because the hostPath comment in `kind/cluster.yaml` says "Phase 4") will fail with the DAG simply absent from `airflow dags list` — no error, no crash, just silence, which PITFALLS B11 already flags as the general failure mode for DAG-visibility problems.
**How to avoid:** Wire the Helm values (Pattern 3) as the *first* task of this phase, before writing either DAG file, and confirm with `kubectl exec -n airflow deploy/airflow-dag-processor -- ls /opt/airflow/dags` showing the mounted file.
**Warning signs:** `airflow dags list-import-errors` empty *and* `airflow dags list` also not showing the new `dag_id` — the DAG was never parsed at all, not merely parsed with an error.

### Pitfall 5: Conflating discovery-time pre-allocation with the pod's own claim upsert
**What goes wrong:** Using the *pod's* claim SQL (Pattern 2, part 2) inside `discover_files`, or using the *discovery* no-op upsert (Pattern 2, part 1) as the pod's claim mechanism. The first produces a run row that's immediately marked `RUNNING` before any pod exists to hold the lease, defeating the crashed-pod-takeover detection. The second lets two concurrent pod attempts both "succeed" the claim (the no-op `DO UPDATE` never checks `status`), destroying the exclusivity the whole idempotency design depends on.
**How to avoid:** Two distinct `MetadataRepository` methods, per Pattern 2 — never the same SQL statement for both call sites.
**Warning signs:** A pod-kill retry test where the *second* (surviving) pod also thinks it "won" the claim and both write rows.

### Pitfall 6: Forgetting the `etl` namespace correction
**What goes wrong:** The phase's own canonical references (`04-CONTEXT.md`'s Integration Points) name the namespace `data-etl`. The live, actual `kubernetes/namespaces.yaml` (read directly this session) creates exactly five namespaces: `cnpg-system`, `data`, `airflow`, `etl`, `ingress-nginx` — there is no `data-etl`. Using the wrong name anywhere (KPO `namespace=` argument, RBAC manifest, Vault role in Phase 5) produces a pod that lands in a namespace with no RBAC grant, misread as a Vault or RBAC bug when it is a typo.
**How to avoid:** Use `etl` everywhere. Confirmed live: `kubectl get pods -n etl` currently returns "No resources found" — the namespace exists and is empty, ready for this phase's first ServiceAccount and pods.
**Warning signs:** `Error from server (Forbidden): pods is forbidden: User "system:serviceaccount:airflow:..." cannot create resource "pods" in API group "" in the namespace "data-etl"` — the namespace literally does not exist, so this would actually surface as `NotFound`/webhook rejection, not a permissions error, which is itself the tell.

### Pitfall 7: No RBAC yet for the scheduler to create pods in `etl`
**What goes wrong:** `KubernetesExecutor` (already the local profile's executor) needs the *scheduler's own ServiceAccount* to have `create`/`get`/`list`/`watch`/`delete` on `pods` and `get` on `pods/log` in whatever namespace it's asked to launch pods into. No such Role/RoleBinding exists yet anywhere in the repo (confirmed by grep — `kubernetes/` contains only `namespaces.yaml`).
**How to avoid:** A new manifest (`kubernetes/rbac-etl.yaml`) granting this to the Airflow release's actual scheduler ServiceAccount — **verify the exact SA name against the live release** (`kubectl get sa -n airflow`, release name is `airflow` per `scripts/stages/70-airflow.sh`) rather than assuming a name; do not grant `cluster-admin` or a wildcard role to "make it work."
**Warning signs:** KPO tasks stuck in `queued` with a `403 Forbidden` in the scheduler log when it tries to create the pod.

### Pitfall 8: Business-date derivation appearing to be blocked by missing filename parsing
**What goes wrong:** INCR-08 ("business date is derived from the data, never wall-clock or `logical_date`") reads as if this phase must implement real business-date derivation — but `meta.files.filename_facets`/`business_date` population depends on CSV-01 (filename mask parsing), which is explicitly Phase 6 scope, not built yet. A planner might either (a) skip INCR-08 entirely as "not applicable yet," silently deferring a locked requirement, or (b) improvise a `now()`/`logical_date` fallback to "have something," which is precisely the corrupting shortcut PITFALLS B7 names.
**How to avoid:** INCR-08 is satisfied for this phase by **leaving `meta.files.business_date` `NULL`** (the column is already nullable, migration `0002`) rather than deriving it incorrectly — "never guess" is itself compliant. Do not add a `datetime.now()` or `logical_date` fallback anywhere in this phase's code.
**Warning signs:** A `business_date` column populated with today's date on every row — the exact bug INCR-08/B7 exist to prevent, and it would pass every test in this phase's narrow scope while silently corrupting Phase 6+'s eventual historical-backfill guarantees.

### Pitfall 9: Staging table "all-TEXT" applied to the lineage columns too
**What goes wrong:** ARCHITECTURE.md §3.1 says staging is "all columns `text`, plus lineage columns," which reads ambiguously as including the lineage columns in the all-TEXT rule. The lineage columns (`_run_id`, `_file_id`, `_batch_id`, `_source_row_number`, `_record_hash`, `_record_hash_version`) are populated entirely by the loader in Python, never parsed from unreliable source text — there is no structural-validation reason to weaken their types, and doing so only adds unnecessary `::bigint`/`::bytea` casts at publish time.
**How to avoid:** Business columns (`customer_id`, `name`, `country`, `birth_date`, `event_ts`) are `text` in staging; lineage columns keep their real types (`bigint`, `bytea`, `smallint`) in staging, matching their final target types exactly.
**Warning signs:** None functionally — this is a design-clarity note, not a correctness bug either way, but typing lineage columns correctly in staging removes a class of avoidable cast bugs at publish time.

### Pitfall 10: `_record_hash` computed in SQL "to double-check"
**What goes wrong:** PITFALLS C6 already forbids this generally ("compute the hash in exactly one place — Python... never recompute a hash in SQL"), but it is a specific temptation here because the publish `SELECT` already touches every business column and it looks convenient to add a `digest(...)` call there.
**How to avoid:** `_record_hash` must be computed once, in Python, as each row streams through the staging write (canonical-encoded, fixed column order from the config, explicit NULL sentinel, `Decimal`-string not float per C6), and carried through staging → publish unchanged.
**Warning signs:** Two different hash values for logically-identical rows depending on which code path computed them — `hashlib.sha256` and PostgreSQL's `digest()`/`md5()` do not agree on encoding/normalization by default.

## Code Examples

### The pod entrypoint's high-level sequence (ties every pattern above together)
```python
# packages/dataplat/src/dataplat/cli.py — new `ingest` subcommand, sketch
@cli.command()
@click.option("--assignment", required=True)
def ingest(assignment: str) -> None:
    ctx = build_pipeline_context(assignment_uri=assignment)   # GET assignment JSON, validate shape
    claimed = ctx.metadata.claim_ingestion_run(...)             # Pattern 2, part 2
    if claimed is None:
        write_receipt(status="SKIPPED_DUPLICATE_OR_CONCURRENT")
        return
    run_id, _ = claimed
    heartbeat = start_heartbeat_thread(ctx, run_id)             # background lease_expires_at refresh
    try:
        staging_table = stage_chunks(ctx, run_id)               # DROP IF EXISTS; CREATE UNLOGGED;
                                                                  # chunked COPY; per-chunk log line
        with ctx.db.connection() as conn, conn.transaction():
            publisher = PUBLISHER_REGISTRY[ctx.config.load.strategy]   # "merge"
            result = publisher.publish(ctx, staging_table, conn)       # Pattern 1's SQL
            ctx.metadata.update_ingestion_run_status(
                run_id=run_id, status="SUCCEEDED",
                finished_at=..., rows_loaded=result.rows_affected,
            )
        conn.execute(f"DROP TABLE IF EXISTS {staging_table}")   # Pitfall 2's fix — outside the txn
    finally:
        heartbeat.stop()
    write_receipt(run_id=run_id, status="SUCCEEDED", rows_loaded=result.rows_affected, ...)
```

### `resolve_window` — defensive `logical_date=None` handling (AP10)
```python
# airflow/dags/csv_ingest_customers.py — ORCH-05
@task
def resolve_window(dag_run=None) -> dict[str, str | None]:
    # Never read logical_date/data_interval_start/_end directly from task context
    # (they raise KeyError when dag_run.logical_date is None — asset/API-triggered runs).
    if dag_run is None or dag_run.logical_date is None:
        return {"logical_date": None, "data_interval_start": None, "data_interval_end": None}
    return {
        "logical_date": dag_run.logical_date.isoformat(),
        "data_interval_start": dag_run.data_interval_start.isoformat(),
        "data_interval_end": dag_run.data_interval_end.isoformat(),
    }
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|-------------------|---------------|--------|
| `MERGE` for upsert publication (ARCHITECTURE.md's own worked example) | `pg_advisory_xact_lock` + `INSERT ... ON CONFLICT` | Corrected this session against PostgreSQL BUG #18279 and LOAD-09/PITFALLS C1 | Changes every publish statement this phase writes; requires a new migration this phase must also add |
| `airflow.decorators`/`airflow.models` imports | `airflow.sdk` imports (`dag`, `task`, `Asset`, `BaseSensorOperator`) | Airflow 3.0 (still current for 3.3.0, STACK.md gotcha 1) | Both DAG files in this phase must import from `airflow.sdk` exclusively |
| `execution_date`, `tomorrow_ds`, etc. | `logical_date`, `data_interval_start`/`_end`, with explicit `None` handling | Airflow 3.0 | Directly affects `resolve_window` above |
| `airflow.datasets.Dataset` | `airflow.sdk.Asset` | Airflow 3.0 | Not used by this phase's DAGs directly (they use a sensor, not an Asset schedule) but relevant if a future phase adds Asset-based downstream triggering off this phase's output |
| Chart's own `dags.persistence` PVC as "the" DAG-source mechanism | `extraVolumes`/`extraVolumeMounts` with a `hostPath` volume, bypassing chart-managed persistence entirely | This phase's specific infrastructure choice, driven by `kind/cluster.yaml`'s pre-existing `extraMounts` | Simpler than provisioning a PV/PVC through `local-path-provisioner`; no dynamic-provisioning dependency |

**Deprecated/outdated:**
- Treating ARCHITECTURE.md's Q3/Q7/Data-Flow SQL examples as copy-paste-ready — they predate the MERGE→ON CONFLICT correction documented here.
- `mc`/`aws-cli` as the assumed tool for the demo target's upload step — neither is installed or vendored in this repository; `boto3` already is.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `workers.kubernetes.extraVolumes`/`extraVolumeMounts` (or an equivalently-named key) is the chart-1.22.0-correct way to propagate the DAG hostPath mount into `KubernetesExecutor`'s per-task-instance worker pod template | Architecture Patterns, Pattern 3 | If the actual key differs, the KPO-launching worker pod (not the KPO pod itself) fails to import the DAG at task-execution time — surfaces immediately as a task failure with a clear `ModuleNotFoundError`/`DAG not found`, cheap to diagnose and fix once encountered, low risk of silent corruption |
| A2 | The `csv_ingest_customers` DAG needs a real (non-`None`) `schedule` to produce repeated DagRuns for the deferrable sensor to re-arm in, rather than a self-triggering or `schedule=None` pattern | Open Questions | If wrong, the "wakes up" behavior D-01 describes doesn't actually repeat after the first file/batch — would surface immediately in the very first manual test of the slice (no second file ever gets picked up), not a subtle bug |
| A3 | The Airflow chart release's scheduler ServiceAccount (not a separate "worker" SA) is the identity that needs the new `etl`-namespace RBAC grant, since `KubernetesExecutor` pod-creation calls originate from the scheduler process itself | Common Pitfalls #7 | If wrong (e.g., the chart actually uses a distinct executor-facing SA), the RBAC grant targets the wrong principal and pod creation still fails with 403 — diagnosable via `kubectl get sa -n airflow` and re-pointing the RoleBinding, not a silent-corruption risk |
| A4 | apiServer (webserver) does not strictly need the DAG hostPath mount, since Airflow 3's UI reads DAG structure from the serialized representation in the metadata DB rather than the files directly | Architecture Patterns, Pattern 3 | If wrong, only the UI's code-view/graph rendering degrades — no correctness impact on ingestion itself |

**If this table is empty:** N/A — see rows above. All other claims in this document were verified against either the live repository, the live cluster, or an official documentation source fetched this session.

## Open Questions (RESOLVED)

1. **How does `csv_ingest_customers` get a new DagRun after each cycle completes?**
   - What we know: D-01/D-02/D-03/D-04 lock the sensor's own behavior (deferrable, 30s poke, `max_active_runs=1`, one run processes all currently-visible new files) but say nothing about the DAG's own `schedule=` value — the mechanism that determines whether a *second* DagRun ever starts after the first one finishes.
   - What's unclear: whether a short, real `schedule` (e.g. every 1–5 minutes) is intended, versus a self-triggering pattern, versus something else.
   - Recommendation: use a real, short `schedule` (this document's Assumption A2) — it is the standard, idiomatic Airflow shape for "keep creating opportunities to sense," composes cleanly with `max_active_runs=1`, and needs no extra machinery. Treat the exact interval value as an implementation detail for the plan to fix (not phase-blocking).
   - **RESOLVED:** 04-07-PLAN.md fixes the value -- `schedule="*/1 * * * *"` (every 1 minute), with the rationale (a short, real interval so a new sensing opportunity exists almost immediately after the previous run completes, since `max_active_runs=1` otherwise leaves dead time) recorded as an inline comment in the DAG file itself, not left silent.

2. **What does "supports backfill" (ORCH-04) mean for a sensor-first DAG?**
   - What we know: ORCH-04 is a locked Phase 4 requirement; ARCHITECTURE.md/PITFALLS.md's backfill discussion (B7) is written primarily about downstream, window-based DAGs, not a file-arrival sensor.
   - What's unclear: whether "backfill" for `csv_ingest_customers` should mean anything beyond "the mechanical `airflow dags backfill` CLI command does not crash" — a backfilled run of a sensor-first DAG has no historical window to speak of; it would just re-poke the *current* state of `s3://raw/customers/*.csv`.
   - Recommendation: treat this as a degenerate but harmless case (idempotent by the same run-claim protocol as any other run) and document that decision explicitly in the plan rather than silently deciding it; meaningful historical-window backfill semantics belong to future batch-oriented DAGs, not this one.
   - **RESOLVED:** 04-07-PLAN.md's `csv_ingest_customers.py` module docstring states this explicitly: `airflow dags backfill` against this sensor-first DAG is a degenerate-but-harmless case with no historical window -- a backfilled run re-invokes the same `wait_for_files` -> `discover` -> `ingest` chain against the CURRENT state of `s3://raw/customers/*.csv`, made safe by the same run-claim idempotency protocol (04-01/04-05) every other run relies on, not by DAG-specific backfill logic.

3. **Exact NULL-handling for the `event_ts >= t.event_ts` publication guard.**
   - What we know: `normalized.customers.event_ts` is nullable; SQL comparisons against `NULL` evaluate to `NULL`, which `WHERE` treats as false, meaning a row with `NULL` `event_ts` would never update an existing row under the guard as written in Pattern 1.
   - What's unclear: whether this edge case is even reachable given the phase's synthetic, no-edge-case fixture scope (D-05's Faker-style data will very likely always populate `event_ts`).
   - Recommendation: leave the guard as written (NULL-safe via `IS DISTINCT FROM` elsewhere, plain comparison here is acceptable) unless a fixture actually exercises a NULL `event_ts`; if one does, decide explicitly (e.g. `COALESCE(EXCLUDED.event_ts, 'infinity') >= COALESCE(t.event_ts, '-infinity')`) rather than leaving it as an accidental default.
   - **RESOLVED:** not reachable in this phase's test surface -- 04-08-PLAN.md's `slice-corpus.yaml` fixture (`event_ts: {kind: pick, values: [...]}`) always populates `event_ts` from a fixed, non-empty value list, so the NULL-`event_ts` edge case this question raised is never exercised; the guard is left as written per the original recommendation, and revisiting it is deferred until a fixture actually needs a NULL `event_ts`.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Docker daemon | `tests/integration/` (testcontainers), local image builds | ✓ | 29.6.2 | — |
| `kubectl` | E2E tests, `make cluster-verify`-style targets, RBAC/manifest application | ✓ | v1.36.1 (client) | — |
| Live kind cluster (`airflow-platform`) | Every E2E test this phase adds; the smoke DAG (U1) | ✓ | 3-node, all `Ready`; `kind`/`v1.35.5` per node | — |
| Airflow workloads (api-server, scheduler, dag-processor, triggerer) | The DAG itself | ✓ | Confirmed `Running` in namespace `airflow` (with periodic restarts consistent with WSL2 sleep/resume — not phase-blocking) | — |
| MinIO, both PostgreSQL clusters | Publication tests, staging | ✓ | Confirmed `Running` in namespace `data` | — |
| `etl` namespace | KPO pods, this phase's ServiceAccount/RBAC | ✓ (empty) | — | — |
| `uv` | Local dev, CI | ✓ | 0.12.3 — matches the pinned `UV_REQUIRED_VERSION` exactly | — |
| `helm`, `kind`, `kubeconform` | Manifest rendering/validation | ✓ | Vendored at `tools/bin/` per the project's own convention | — |
| `psql` | Ad hoc debugging | ✓ | 16.14 client (server majors are 17/18 — client version is not required to match for basic queries) | — |
| `mc` (MinIO client CLI) | *Not actually required* — see Don't Hand-Roll | ✗ | — | `boto3`, already a pinned dependency, already wrapped by `dataplat.storage.objectstore` |
| `aws` CLI | *Not required anywhere in this phase's design* | ✗ | — | N/A — no code path in this phase's plan needs it |

**Missing dependencies with no fallback:** none.
**Missing dependencies with fallback:** `mc`/`aws` CLI — both absent, both avoidable by using the already-available `boto3` for the one place a file needs to be uploaded outside the platform itself (`make ingest-demo`).

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest `9.1.1` (already pinned, `[tool.pytest.ini_options]` in root `pyproject.toml`) |
| Config file | `pyproject.toml` (`[tool.pytest.ini_options]`), markers already include `cluster: requires a live kind cluster` |
| Quick run command | `uv run --frozen pytest tests/unit tests/regression -q` (existing `make test`) |
| Full suite command | `make test-integration` (testcontainers, existing) + a **new** cluster-gated target for this phase's E2E tests, e.g. `$(RUN_CLUSTER) pytest tests/e2e/cluster tests/e2e/slice -q` extending the existing `cluster-verify` pattern |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|---------------------|-------------|
| ORCH-01 | DAG uses TaskFlow API, imports only from `airflow.sdk` | unit (structural) | `pytest tests/unit/test_dag_structure.py -x` | ❌ Wave 0 |
| ORCH-02 | No parsing/validation/DB writes in DAG files | unit (import-linter or grep-policy) | `pytest tests/policy/test_dag_thinness.py -x` | ❌ Wave 0 |
| ORCH-03 | Map length bounded | unit + integration | `pytest tests/unit/test_batching_config.py -x` | ❌ Wave 0 |
| ORCH-04 | Retries/backfill declared | unit (structural) | `pytest tests/unit/test_dag_structure.py::test_retries_set -x` | ❌ Wave 0 |
| ORCH-05 | `logical_date=None` tolerated | unit | `pytest tests/unit/test_resolve_window.py -x` | ❌ Wave 0 |
| ORCH-06 | DAG files <150 lines | unit (policy) | `pytest tests/policy/test_dag_line_budget.py -x` | ❌ Wave 0 |
| ORCH-08 | Frozen manifest, not live listing | integration | `pytest tests/integration/test_discover_files.py::test_rerun_same_manifest -x` | ❌ Wave 0 |
| ORCH-09 | CPU/mem requests+limits on the KPO task | unit (structural, mocked `.execute`) | `pytest tests/unit/test_dag_structure.py::test_kpo_resources -x` | ❌ Wave 0 |
| META-03 | Rows + file/batch/run status commit atomically | integration | `pytest tests/integration/test_publish_merge.py::test_atomic_commit -x` | ❌ Wave 0 |
| LOAD-01/02/09 | Retry mid-load: no duplicates | e2e | `pytest tests/e2e/slice/test_pod_kill_retry.py -x` (D-09..D-11) | ❌ Wave 0 |
| LOAD-05 | Concurrent SELECT never sees a partial table | e2e | `pytest tests/e2e/slice/test_concurrent_select.py -x` (D-12) | ❌ Wave 0 |
| LOAD-03/QUAL-09 | Re-upload under new name: zero additional rows | e2e | `pytest tests/e2e/slice/test_idempotent_reupload.py -x` (D-07 fixture) | ❌ Wave 0 |
| QUAL-05 | MinIO → processor → PostgreSQL | integration | `make test-integration` (extended) | ❌ Wave 0 (extends existing dir) |
| QUAL-06 | Full CSV → MinIO → Airflow → K8s → processor → PostgreSQL | e2e | new cluster-gated target, see above | ❌ Wave 0 |

### Sampling Rate
- **Per task commit:** `uv run --frozen pytest tests/unit -k <touched module> -x`
- **Per wave merge:** `make test` + `make test-integration`
- **Phase gate:** the new cluster-gated E2E target (pod-kill, concurrent-SELECT, idempotent-reupload) green before `/gsd:verify-work` — this genuinely needs the live cluster and cannot run in the offline gate

### Wave 0 Gaps
- [ ] `tests/e2e/slice/__init__.py`, `conftest.py` — new directory, needs a fixture analogous to `tests/e2e/cluster/conftest.py`'s `_require_cluster` skip-with-reason pattern
- [ ] `tests/unit/test_dag_structure.py` — `DagBag(dag_folder="airflow/dags", include_examples=False)`, assert `import_errors == {}`, plus the structural assertions (retries, resources, no top-level heavy imports)
- [ ] `tests/policy/test_dag_line_budget.py` — line-count assertion per DAG file, consistent with the existing `tests/policy/` convention
- [ ] Framework install: none — pytest, testcontainers, boto3, psycopg are all already present via the `dev`/`cluster` dependency groups

## Security Domain

### Applicable ASVS Categories (Level 1, per `.planning/config.json`)

| ASVS Category | Applies | Standard Control |
|---------------|---------|-------------------|
| V2 Authentication | No | No user-facing authentication in this phase; Airflow's own auth and Vault are out of scope (Phase 5) |
| V3 Session Management | No | N/A |
| V4 Access Control | Yes | Kubernetes RBAC: `csv-processor` ServiceAccount in `etl`, least-privilege Role (not `cluster-admin`, not a wildcard); scheduler SA granted only `pods`/`pods/log` create/get/list/watch/delete in `etl`, nothing broader |
| V5 Input Validation | Yes | Every SQL statement this phase adds must use parameterized queries via `%s` placeholders (the existing `PostgresMetadataRepository` convention — never string interpolation); the assignment JSON document the pod reads from MinIO must be validated against a `pydantic` model (`extra="forbid"`, matching `DatasetConfig`'s convention) before use, since it is technically attacker-influenceable input to the pod even though this phase's own writer (`discover_files`) is trusted — defense in depth for the day a second writer exists |
| V6 Cryptography | Marginal | `content_sha256`/`_record_hash` are integrity hashes (`hashlib.sha256`, stdlib), not confidentiality — already the established pattern, no new crypto surface this phase |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|-----------------------|
| SQL injection via CSV field values reaching a query | Tampering | Parameterized queries only, throughout the new staging/publish code — already the established convention in `PostgresMetadataRepository`; extend it, never break it |
| Tampered/malformed assignment JSON causing the pod to target the wrong table/dataset | Tampering, Elevation of Privilege | Validate the assignment document's shape with a strict `pydantic` model before any field is used to build SQL identifiers or object-store paths |
| Overly-broad RBAC ("just grant `cluster-admin` to make the 403 go away") | Elevation of Privilege | Named, narrow Role scoped to `etl` only, per Common Pitfalls #7; write the negative test now if reasonable, or explicitly defer it to Phase 5 where the Vault-identity negative test already exists |
| Sensitive detail leaking through the ≤4KB XCom receipt or `meta.ingestion_runs.error_message` | Information Disclosure | Already structurally mitigated — `dataplat.errors.DataPlatformError.context` and the redaction processor in `dataplat.observability.logging` already exist; ensure the new `ingest` subcommand's exception handling stays inside `cli.py`'s single catch-once boundary rather than adding a second `except Exception` |
| Unbounded Dynamic Task Mapping fan-out as a denial-of-service against the kind cluster | Denial of Service | `max_map_length` + `batching.max_units_per_run`, already covered under Don't Hand-Roll |

## Sources

### Primary (HIGH confidence)
- Live repository reads (this session): `.planning/phases/04.../04-CONTEXT.md`, `.planning/REQUIREMENTS.md`, `.planning/STATE.md`, `.planning/ROADMAP.md`, `.planning/research/{ARCHITECTURE,PITFALLS,STACK}.md`, `docs/adr/{0002,0004,0008}-*.md`, every file under `packages/dataplat/src/dataplat/` and `packages/csv-processor/src/csv_processor/source.py` named in the task, all five `migrations/versions/0001..0005_*.py`, `configs/datasets/customers.yaml`, `configs/defaults.yaml`, `kubernetes/namespaces.yaml`, `kind/cluster.yaml`, `helm/values/{local,ci}/airflow.yaml`, `helm/versions.env`, `Makefile`, root/`packages/*/pyproject.toml`, `tools/corpus/{generators,manifest,__main__}.py`, `tests/integration/conftest.py`, `docker/csv-processor/Dockerfile`, `scripts/stages/{60-minio,70-airflow}.sh`, `.github/workflows/ci.yml` (job names only)
- Live cluster inspection (this session): `kubectl get nodes`, `kubectl get pods -A` against the actual running `airflow-platform` kind cluster — confirmed Airflow/MinIO/both-PostgreSQL workloads `Running`, `etl` namespace exists and is empty
- `https://raw.githubusercontent.com/apache/airflow/constraints-3.3.0/constraints-3.12.txt` — fetched directly this session; exact pinned versions of `apache-airflow-providers-amazon` (9.31.0), `apache-airflow-providers-cncf-kubernetes` (10.19.0), `apache-airflow-providers-hashicorp` (4.7.1), `apache-airflow-providers-postgres` (6.8.0), `apache-airflow-providers-standard` (1.15.0)
- `https://www.postgresql.org/docs/current/sql-insert.html` — `ON CONFLICT` grammar, arbiter-index requirement, `WHERE`-clause semantics on `DO UPDATE`, `EXCLUDED` visibility, the "deterministic statement"/cardinality-violation rule — fetched this session
- `https://www.postgresql.org/docs/current/sql-createtable.html` — `ON COMMIT` applies only to `TEMPORARY` tables; `TEMPORARY`/`UNLOGGED` presented as alternative table-level options — fetched this session
- `https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html` — `KubernetesPodOperator` XCom sidecar mechanics, `on_finish_action` default (`delete_pod`) and allowed values, deferrable-mode support, `xcom_sidecar_container_security_context` — fetched this session
- `https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/sensors/s3/index.html` — `S3KeySensor` import path and constructor parameters — fetched this session
- `https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html` — Airflow 3's default `LocalDagBundle` behavior, "does not support versioning" caveat — fetched this session
- `https://faker.readthedocs.io/` — Faker's own documented non-reproducibility across patch versions — fetched this session
- `slopcheck install apache-airflow-providers-amazon` (this session) — `[OK]`; `pip index versions apache-airflow-providers-amazon` — confirms PyPI presence, latest `9.34.0`

### Secondary (MEDIUM confidence)
- WebSearch summary of the Apache Airflow Docker image's default extras list (`amazon`, `cncf-kubernetes`, `hashicorp`, ... among ~30 documented defaults) — corroborated by, and consistent with, the independently-fetched constraints file's inclusion of exactly those provider packages at matching versions
- WebFetch-summarized (not independently grep-verified against the raw chart source) claim that `workers.kubernetes.extraVolumes`/`extraVolumeMounts` is the correct chart-1.22.0 key for propagating volumes into `KubernetesExecutor`'s per-task worker pod template — flagged as Assumption A1

### Tertiary (LOW confidence)
- None retained without escalation — every claim initially sourced from WebSearch alone was either cross-verified against an official document/the live repository in this session, or explicitly demoted to the Assumptions Log above.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — every version claim traced to the pinned constraints file or the live, already-deployed environment, not to training-data recall
- Architecture (publication transaction, run-claim protocol, DAG-mount wiring): HIGH for the SQL/PostgreSQL-semantics claims (independently verified against official docs this session, and against the real migrated schema); MEDIUM for the exact Helm-chart key names controlling `KubernetesExecutor` worker-pod volumes (Assumption A1) — cheap to confirm in the first implementation wave, low blast radius if wrong
- Pitfalls: HIGH — every pitfall in this document either reproduces a project-internal contradiction found by cross-reading two of the project's own research documents against each other and against the live schema, or was independently confirmed against official PostgreSQL/Airflow documentation this session

**Research date:** 2026-08-13
**Valid until:** 30 days for the Airflow/PostgreSQL semantics (stable, versioned APIs); re-verify provider version pins if the Airflow patch version changes before this phase executes
