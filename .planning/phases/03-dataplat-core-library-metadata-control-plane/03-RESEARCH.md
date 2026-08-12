# Phase 3: `dataplat` Core Library & Metadata Control Plane - Research

**Researched:** 2026-08-12
**Domain:** Python ETL library architecture, PostgreSQL metadata schema design (Alembic), pipeline
engine protocols, testcontainers-based testing, structured logging
**Confidence:** HIGH

## Summary

This phase turns three already-adjudicated research documents (ARCHITECTURE.md Q2–Q5/Q9,
PITFALLS.md #1/#5/C5/C6/E1, STACK.md §F) into a concrete build plan. Per the roadmap's own framing,
almost every *library choice* here is already locked in `.claude/CLAUDE.md` — this research pass
therefore does not re-litigate psycopg/Alembic/structlog/testcontainers/Pydantic, and instead
closes the gaps between "here is the design" and "here is exactly how to build it": migration
sequencing and FK deferral, the real boto3↔`csv.reader` bridge, testcontainers fixture shape, a CLI
framework recommendation (genuinely undecided — not in STACK.md), and the actual state Phase 2 left
the analytical PostgreSQL cluster in.

Three findings materially change or sharpen what CONTEXT.md and PITFALLS.md say, all verified this
session rather than assumed:

1. **The custom `io.RawIOBase` adapter PITFALLS.md E1 calls for is unnecessary against the pinned
   boto3 1.43.68.** `StreamingBody.readinto()`/`.readable()` were added to botocore in response to
   [boto/botocore#3108](https://github.com/boto/botocore/issues/3108) (opened 2024-01-29,
   specifically to make `io.BufferedReader(body)` and `hashlib.file_digest()` work). Verified two
   ways this session: source inspection of the installed `botocore==1.43.70` (`readinto` delegates
   to `self._raw_stream.readinto(b)`), and an executable round-trip test proving
   `io.TextIOWrapper(io.BufferedReader(response["Body"]), newline="")` correctly preserves an
   embedded-newline CSV field with no custom class at all. This does **not** change CSV-13's
   substance (still: one `csv.reader`, `newline=""`, record-ordinal chunking, explicit
   `field_size_limit`) — only the "you must write a ~15-line adapter" implementation detail, which
   is now "you don't need to."
2. **Phase 2 already resolved the `meta` vs `metadata` schema-naming question in `dataplat`'s
   favor**, and left the analytical database in a very specific, verifiable state:
   `helm/values/local/cnpg-analytics.yaml` runs exactly `CREATE ROLE etl_app LOGIN;` as its only
   `postInitApplicationSQL` — no schema, no password, no grants (D-15, confirmed live:
   `select rolname from pg_roles where rolname='etl_app'`). Every schema this phase needs
   (`meta`, `normalized`) is Alembic's to create from nothing, and **Alembic's migrations must also
   `GRANT` `etl_app` the privileges it needs** — that grant has no other home. `etl_app` has no
   password yet; that is out of scope for this phase (testcontainers tests don't touch the live
   cluster) but is a fact the plan should record so a later phase does not assume it already works.
3. **`meta.ingestion_runs.schema_version_id` cannot be a real foreign key in this phase's
   migrations.** ARCHITECTURE.md §2.1 specifies it as a FK to `meta.schema_versions`, but
   `schema_versions` is explicitly a post-slice table (CONTEXT.md D-05, ARCHITECTURE.md §2.4) that
   a later phase creates. The column must land now (nullable, no constraint); the FK constraint is
   added by whichever later migration creates `schema_versions`. Every other FK column on the five
   slice tables *can* be a real constraint now — this is the only deferral needed.

**Primary recommendation:** Build `dataplat` as a single coherent Alembic-migrated schema
(`meta` + `normalized.customers`) plus a small set of Python protocols (`Source`, `Stage`,
`Publisher`, `MetadataRepository`) proven entirely against testcontainers Postgres/MinIO, with
`click` (not yet decided anywhere — recommended here) as the CLI framework, `structlog` wired for
real from day one, and `boto3`/`psycopg[binary,pool]` added as genuine `dataplat` runtime
dependencies for the first time — which has a direct, documented consequence for the repository's
existing `cluster` dependency-group split that the plan must address explicitly (see Common
Pitfalls).

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| META-01 | Single coherent `meta` schema, Alembic-migrated | ARCHITECTURE.md §2 (full column-level design for all ~19 tables); this doc's "Migration Sequencing" pattern resolves the FK-deferral and grant-placement gaps §2 leaves open |
| META-02 | Every stored hash carries a `hash_version` column | PITFALLS.md #1/C6; this doc names the exact columns (`files.hash_version`, `config_versions.hash_version`, and recommends extending to `normalized.customers._record_hash_version`) |
| INFRA-08 | Images reproducible, versioned by git SHA, never `:latest` | STACK.md §I "Container build"; ADR-0004's `--no-install-workspace --frozen` → `--locked` ordering, made concrete in Code Examples |
| SEC-15 | `SecretsResolver` over opaque `env://`/`file://` refs | ARCHITECTURE.md Q9.1/9.3; this doc's Code Examples give the concrete resolver shape and the redaction-processor pairing OBS-05 needs |
| CSV-13 | Stream via one `csv.reader`, `newline=""`, chunk by records | PITFALLS.md E1 (substance unchanged); **this doc corrects the StreamingBody-adapter implementation detail**, verified empirically against the pinned boto3 version |
| SCHEMA-07 | Config versioned and hashed; every run records its config version | ARCHITECTURE.md Q5 (canonicalization algorithm, `config_versions` table); this doc scopes what's testcontainers-testable now vs. the Phase-4+ `config-sync` DAG |
| OBS-02 | Structured logging works in local/Docker/K8s/Airflow-task contexts | STACK.md §F "Logging"; Code Examples give the dual-renderer `structlog.configure()` |
| OBS-04 | Logs carry contextual fields (dataset, stage, path, run IDs) | ARCHITECTURE.md §4.5 "Logging"; `structlog.contextvars.bind_contextvars` pattern, verified against current structlog docs |
| OBS-05 | Secrets/PII never logged | ARCHITECTURE.md §4.5's redaction processor; Code Examples give a concrete drop/truncate processor |
| QUAL-03 | Domain exception hierarchy; row-level problems as values, never exceptions | ARCHITECTURE.md §4.5, CONTEXT.md D-06 (scoped hierarchy); Architecture Patterns → Pattern 3 |

</phase_requirements>

## Project Constraints (from CLAUDE.md)

Directives from `.claude/CLAUDE.md` that bind this phase's plan (already locked; not re-litigated
here):

- **Logic placement**: business logic lives in `dataplat`/`csv_processor`; DAGs (Phase 4+) orchestrate
  and delegate only. Heavy processing never runs in the scheduler.
- **Secrets**: no credential in Git, Python source, Dockerfiles, K8s manifests, Airflow Variables or
  CI files — runtime injection only. `SecretsResolver` is the mechanism this phase builds for it.
- **Determinism**: same source + config + processor version ⇒ same logical result. Hash recipes and
  chunking must be reproducible; this is why `hash_version` and record-ordinal (not byte-offset)
  checkpoints matter.
- **Filesystem**: repo stays on WSL ext4; irrelevant to this phase's own file operations (no
  hostPath mounts touched here) but binding on where `make`/`uv`/pytest run.
- **Pinned versions for this phase's real new dependencies** (from CLAUDE.md's stack table,
  verified HIGH confidence, dated 2026-08-11 — one day before this research):
  `psycopg[binary,pool]==3.3.4`, `alembic==1.19.1`, `structlog` (`26.1.0`),
  `testcontainers[postgres,minio]==4.15.0`, `hypothesis==6.165.3`, `pydantic>=2.13,<3`,
  `boto3==1.43.68`, `PyYAML` (already present). mypy strict, ruff, `T20` print-ban all already
  enforced repo-wide.
- **What NOT to use, relevant to this phase**: `csv.Sniffer` (N/A — Phase 3's reader is hardcoded,
  no detection); `dateutil.parser.parse` (N/A this phase); Pydantic models per CSV row (config/
  contracts/reports only); SQLAlchemy ORM for row loading (COPY only); `asyncpg`; psycopg pipeline
  mode for bulk load (not used together with `COPY` — irrelevant here since this phase does not yet
  build the loader, but `storage/db.py`'s design must not paint into that corner); the Airflow
  metadata DB for ETL metadata (a §4/INFRA-04 violation — `meta.*` lives only in the analytical DB).

## Architectural Responsibility Map

> This project has no Browser/Client, Frontend-Server(SSR), or CDN/Static tier — it is a backend
> data platform with a Kubernetes control plane, not a web application. The table below is adapted
> accordingly: "API/Backend" covers all `dataplat`/`csv_processor` library code (this phase's
> primary output), "Database/Storage" covers the Alembic-owned schema. The distinction that matters
> for this phase is **library code vs. database schema vs. orchestration** — the last of which
> (Airflow DAGs) is explicitly Phase 4's and must not appear here.

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| `meta` schema + `normalized.customers` (DDL, constraints, grants) | Database/Storage | — | Alembic-owned; must never be created or altered by application code at runtime |
| `MetadataRepository` (typed read/write access to `meta.*`) | API/Backend | Database/Storage | Backend owns the query/write logic; DB owns constraint enforcement (idempotency key uniqueness, FK integrity) |
| Pipeline engine (`Source`/`Stage`/`Publisher` protocols, chunk loop, checkpoints) | API/Backend | — | Pure library code — must never live in a DAG file (that boundary is Phase 4's to keep, but this phase must not pre-violate it) |
| `SecretsResolver` (`env://`, `file://`) | API/Backend | — | Resolves credentials for the backend process at runtime; no build-time or infra-manifest role |
| Structured logging (`structlog` config, redaction processor) | API/Backend | — | Cross-cutting library concern, threaded through every stage |
| Minimal `csv_processor.Source` | API/Backend | — | A `Source` plugin implementation; backend-only, no I/O boundary beyond MinIO reads |
| CLI entrypoint (`dataplat.cli`, the pod's `ENTRYPOINT`) | API/Backend | — | Equivalent to a backend service's `main()`; this is what `docker run ... dataplat --version` invokes |
| Config system (`ConfigRegistry`, loader, canonical-JSON hasher) | API/Backend | Database/Storage | Loader/hasher is backend logic; `meta.config_versions` is the DB-side system of record it writes to |
| Object storage access (S3/MinIO reads via boto3) | API/Backend | — | `dataplat.storage.objectstore`; MinIO itself is external storage, not a platform code tier |
| Container image (`docker/csv-processor/Dockerfile`) | API/Backend | — | Packaging of the backend service for deployment; grouped here since it has no distinct tier of its own |

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `psycopg[binary,pool]` | 3.3.4 | PostgreSQL driver — COPY, connection pooling | Already locked (CLAUDE.md). Natively typed (PEP 484, checked with mypy `--strict` upstream — [VERIFIED: psycopg.org/psycopg3/docs/advanced/typing.html] no external stubs needed |
| `alembic` | 1.19.1 | Hand-written schema migrations | Already locked. Supports `version_table_schema` for placing `alembic_version` inside `meta` [VERIFIED: alembic.sqlalchemy.org/en/latest/api/runtime.html via WebSearch summary, cross-referenced with the Alembic Cookbook multi-schema recipe] |
| `structlog` | 26.1.0 | Structured, contextual logging | Already locked. `structlog.contextvars.merge_contextvars` + `bind_contextvars`/`clear_contextvars` verified against current docs [VERIFIED: www.structlog.org/en/stable/contextvars.html] |
| `pydantic` | ≥2.13,<3 | Config/contract models only, never per-row | Already locked |
| `boto3` | 1.43.68 | S3/MinIO client | Already locked. `StreamingBody` in this exact version implements `readinto()`/`readable()` — [VERIFIED: source inspection of installed botocore 1.43.70 + executable round-trip test, this session] |
| `PyYAML` | ≥6 | Config file parsing | Already present in `dataplat`'s `pyproject.toml` |
| `click` | 8.4.2 (latest verified) | CLI framework for `dataplat.cli` | **Not previously decided anywhere in STACK.md or CONTEXT.md.** Recommended here — see rationale below. [ASSUMED — needs confirmation; see Assumptions Log] |

**On the CLI framework (genuinely open, not in any prior research):** `argparse` (stdlib, zero
dependency) is the minimal option; `click` and `typer` are the two real libraries. Confirmed via
`pip show` this session: `click` has **zero required dependencies**; `typer` requires
`annotated-doc`, `rich`, `shellingham` (three extra packages) because it is built as a type-hint
layer *on top of* click. Given ADR-0004's explicit goal of a slim csv-processor image ("a Python
base plus a wheel, not a build environment"), and that this phase's CLI already needs `--version`
now and will grow an `ingest --assignment <uri>` subcommand in Phase 4 (ARCHITECTURE.md §6.2)
followed by a `replay` command later (§5.5) — enough surface to justify a real library over hand-
rolled `argparse` subparsers, but not enough to justify typer's extra three dependencies —
**recommend `click`**. It is also what `apache-airflow` itself depends on, for whatever weak
ecosystem-consistency signal that carries (not binding: the Airflow image and csv-processor image
share no dependencies per ADR-0004).

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `testcontainers[postgres,minio]` | 4.15.0 | Integration-test fixtures | `tests/integration/` only (D-04's new gate), never in `tests/unit` |
| `hypothesis` | 6.165.3 | Property-based tests | The chunking-never-drops-or-splits-records property (Claude's Discretion item), config-hash canonicalization properties |
| `boto3-stubs[s3]` | 1.43.70 | mypy type stubs for boto3 | `[s3]` extra only — boto3-stubs ships ~350 AWS service stub packages; scoping to `s3` keeps install light. Confirmed current on PyPI this session |
| `sqlalchemy` | ≥2.0.51 | DDL/query construction for Alembic only | STACK.md is explicit: "not for row loading." Scope to the migration environment, not `dataplat`'s runtime deps — see Common Pitfalls |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `click` for CLI | `argparse` (stdlib) | Zero dependencies, but hand-rolled subcommand/option boilerplate that only grows across Phase 3→4→5; `click.testing.CliRunner` gives clean CLI unit tests argparse does not |
| `click` for CLI | `typer` | Nicer type-hint-driven DX, but pulls `rich`+`shellingham`+`annotated-doc` into the slim csv-processor image for no functional gain at this CLI's complexity |
| Custom `io.RawIOBase` adapter over `StreamingBody` | Direct `io.BufferedReader(response["Body"])` | The adapter class was necessary pre-2024 botocore; against the pinned 1.43.68 it is dead code with its own test burden. See Code Examples |
| `psycopg_pool.ConnectionPool` | Bare `psycopg.connect()` per call | CLAUDE.md pins the `[pool]` extra deliberately; a short-lived pod still benefits from `min_size=1, max_size=2` (PITFALLS C12) over reconnecting per query within one run |

**Installation:**
```bash
# packages/dataplat/pyproject.toml [project.dependencies] — REAL runtime deps, ship in the image:
uv add --package dataplat "psycopg[binary,pool]>=3.3.4,<4" "boto3>=1.43.68,<2" \
    "pydantic>=2.13,<3" "structlog>=26,<27" "click>=8.4,<9"

# Root pyproject.toml [dependency-groups] dev — migration/test tooling, NOT shipped in the image:
uv add --dev "alembic>=1.19.1,<2" "sqlalchemy>=2.0.51,<3"
uv add --dev "testcontainers[postgres,minio]>=4.15,<5" "hypothesis>=6.165,<7" \
    "boto3-stubs[s3]>=1.43,<2"
```

**Version verification (this session, 2026-08-12):**
```
psycopg (3.3.4)          -- pip index versions psycopg        [VERIFIED: PyPI]
alembic (1.19.1)          -- pip index versions alembic         [VERIFIED: PyPI]
structlog (26.1.0)        -- pip index versions structlog       [VERIFIED: PyPI]
testcontainers (4.15.0)   -- pip index versions testcontainers  [VERIFIED: PyPI]
click (8.4.2)              -- pip index versions click           [VERIFIED: PyPI]
hypothesis (6.165.4)      -- pip index versions hypothesis      [VERIFIED: PyPI] (6.165.3 pinned in CLAUDE.md still resolves; 6.165.4 is one patch newer)
pydantic (2.13.4)          -- pip index versions pydantic        [VERIFIED: PyPI]
boto3-stubs (1.43.70)     -- pip index versions boto3-stubs     [VERIFIED: PyPI]
```
All match or are compatible with CLAUDE.md's pins. No drift found.

## Package Legitimacy Audit

Verified via `slopcheck install <pkgs>` this session (all 9 packages checked against the PyPI
registry).

| Package | Registry | slopcheck | Disposition |
|---------|----------|-----------|-------------|
| `psycopg` | PyPI | [OK] | Approved — already in CLAUDE.md's locked stack |
| `alembic` | PyPI | [OK] | Approved — already locked |
| `structlog` | PyPI | [OK] | Approved — already locked |
| `testcontainers` | PyPI | [OK] | Approved — already locked |
| `hypothesis` | PyPI | [OK] | Approved — already locked |
| `pydantic` | PyPI | [OK] | Approved — already locked |
| `boto3` | PyPI | [OK] | Approved — already locked |
| `click` | PyPI | [OK] | Approved — new recommendation this phase, see rationale above |
| `typer` | PyPI | [OK] | Evaluated, not recommended (heavier dependency footprint) — not added to the plan |

**Packages removed due to slopcheck [SLOP] verdict:** none.
**Packages flagged as suspicious [SUS]:** none.

All nine packages returned `[OK]`. `boto3-stubs`/`sqlalchemy` were not run through slopcheck
directly (dev-only tooling, already extremely well-established); if the planner wants belt-and-
braces coverage, `slopcheck install boto3-stubs sqlalchemy` costs seconds and is safe to add as a
verification step in the same task that adds these to `pyproject.toml`.

## Architecture Patterns

### System Architecture Diagram

This phase builds library code and schema only — no live orchestrator, no live pod. The diagram
below shows the **test-time** data flow this phase must prove (testcontainers), which is also
structurally the same flow Phase 4's real pod will exercise:

```
┌─────────────────────────────────────────────────────────────────────────┐
│  pytest process (tests/integration/)                                     │
│                                                                            │
│   ┌────────────────┐        ┌──────────────────┐                        │
│   │ PostgresContainer│      │  MinioContainer    │   testcontainers,     │
│   │ (throwaway PG)   │      │  (throwaway MinIO)  │  started per session  │
│   └────────┬─────────┘      └─────────┬──────────┘                       │
│            │ conninfo                  │ endpoint+creds                  │
│            ▼                           ▼                                 │
│   ┌──────────────────────────────────────────────────────────────┐      │
│   │  alembic upgrade head   (migrations/, hand-written revisions) │      │
│   │     -> CREATE SCHEMA meta, normalized                         │      │
│   │     -> 5 slice tables + batch_files + normalized.customers    │      │
│   │     -> GRANT ... TO etl_app                                   │      │
│   └──────────────────────────┬───────────────────────────────────┘      │
│                              ▼                                          │
│   ┌──────────────────────────────────────────────────────────────┐      │
│   │  dataplat (library under test)                                │      │
│   │                                                                 │      │
│   │  SecretsResolver(env://|file://) ──> psycopg conninfo/boto3 creds     │
│   │                                                                 │      │
│   │  csv_processor.Source.open() ──> ObjectStore.get(bucket,key)  │      │
│   │       │ returns io.TextIOWrapper(io.BufferedReader(body))     │      │
│   │       ▼                                                        │      │
│   │  csv.reader over newline="" ──> itertools.batched(chunk_size) │      │
│   │       │ RecordChunk(rows, first_ordinal)                      │      │
│   │       ▼                                                        │      │
│   │  Stage.apply(ctx, chunk) ──> StageResult(chunk, rejected, findings)   │
│   │       │                                                        │      │
│   │       ▼                                                        │      │
│   │  MetadataRepository ──> writes meta.ingestion_runs, meta.files│      │
│   │       │                                                        │      │
│   │  structlog ──> JSON/console event, contextvars-bound,         │      │
│   │                redaction processor strips secret-pattern keys │      │
│   └──────────────────────────┬───────────────────────────────────┘      │
│                              ▼                                          │
│                  assertions against meta.* rows + normalized.customers   │
└─────────────────────────────────────────────────────────────────────────┘
```

A reader can trace: MinIO object → text stream → CSV rows → chunks → stage result → metadata
write, exactly the path Phase 4's real pod will run, minus the Kubernetes/Airflow layers around it.

### Recommended Project Structure

Confirmed against ADR-0002/0003 and the existing `packages/` skeleton (do not re-derive — this is
what already exists plus what this phase adds):

```
packages/dataplat/src/dataplat/
├── __init__.py                # EXISTS (Phase 1) — do not remove the no-csv_processor-import docstring
├── version.py                 # EXISTS (Phase 1) — resolve_version() is the processor_version source
├── py.typed                   # EXISTS
├── errors.py                  # NEW — DataPlatformError + ConfigurationError/StorageError/SecretResolutionError only (D-06)
├── models/
│   ├── identity.py            # NEW — DatasetRef, FileIdentity, BatchIdentity, RunContext
│   ├── record.py               # NEW — RecordChunk, StageResult, RejectedRecord (@dataclass(slots=True, frozen=True))
│   └── report.py               # NEW — ValidationResult (minimal; full shape is Phase 8's)
├── config/
│   ├── model.py                # NEW — DatasetConfig (pydantic, extra="forbid", frozen=True)
│   ├── loader.py                # NEW — load + merge defaults + resolve to canonical form
│   ├── hashing.py               # NEW — canonical JSON -> sha256 (ARCHITECTURE.md §5.2)
│   └── registry.py              # NEW — ConfigRegistry protocol; Postgres impl (filesystem impl optional)
├── pipeline/
│   ├── protocol.py              # NEW — PipelineContext, StageResult, StreamingStage/BarrierStage protocols
│   └── engine.py                 # NEW — sequencing, chunk loop, checkpoint recording (record ordinals)
├── sources/
│   └── protocol.py              # NEW — Source, RecordStream protocols (no concrete impl here — csv_processor implements)
├── load/
│   └── publish/
│       └── protocol.py          # NEW — Publisher protocol ONLY (no concrete `merge` — that's Phase 4)
├── storage/
│   ├── objectstore.py           # NEW — ObjectStore protocol + boto3/MinIO impl; returns text streams, never leaks boto3 types
│   └── db.py                     # NEW — psycopg connection/pool factory, transaction helpers
├── metadata/
│   ├── repository.py             # NEW — MetadataRepository protocol
│   ├── postgres.py               # NEW — psycopg-backed implementation
│   └── fake.py                   # NEW (Claude's Discretion) — in-memory fake for fast unit tests
├── observability/
│   ├── logging.py                # NEW — structlog configure(), redaction processor
│   ├── metrics.py                 # NEW — no-op seam, real call sites (D-03)
│   └── tracing.py                 # NEW — no-op seam, real call sites (D-03)
├── secrets/
│   └── resolver.py                # NEW — SecretRef parsing; env:// and file:// only (vault:// is Phase 5)
└── cli.py                         # NEW — click app; `--version`; catches DataPlatformError once (D-06)

packages/csv-processor/src/csv_processor/
├── __init__.py                    # EXISTS (Phase 1)
└── source.py                       # NEW — minimal Source impl: UTF-8, comma, header row 0, no detection (D-01)

migrations/                         # NEW (currently .gitkeep only)
├── env.py                          # Alembic environment; hard-fails if resolved DB name != expected analytical DB
├── script.py.mako
└── versions/
    ├── 0001_meta_datasets_config_versions.py
    ├── 0002_meta_files.py
    ├── 0003_meta_batches_batch_files.py
    ├── 0004_meta_ingestion_runs.py
    └── 0005_normalized_customers.py

configs/
├── defaults.yaml                    # NEW
└── datasets/customers.yaml          # NEW (D-02)

docker/csv-processor/Dockerfile       # NEW — see Code Examples

tests/
├── unit/                            # dataplat/csv_processor unit tests (fakes/mocks only, no live services)
├── integration/                      # NEW real content — testcontainers Postgres+MinIO (D-04)
└── property/                         # hypothesis chunking + hashing properties (Claude's Discretion)
```

### Pattern 1: Migration Sequencing with Deferred Foreign Keys

**What:** Land all five slice tables' *columns* per ARCHITECTURE.md §2's complete design, but only
constrain FKs whose target table exists in this phase's migrations. `ingestion_runs.schema_version_id`
is the one column that must be nullable-and-unconstrained now.

**When to use:** Any time a "coherent design done up front, built incrementally" schema (D-05's
literal instruction) has a column whose target table is intentionally a later phase's.

**Example:**
```python
# migrations/versions/0004_meta_ingestion_runs.py
# Source: reasoned from ARCHITECTURE.md §2.1 + CONTEXT.md D-05 (schema_versions is post-slice)

def upgrade() -> None:
    op.create_table(
        "ingestion_runs",
        sa.Column("run_id", sa.BigInteger, primary_key=True),
        sa.Column("idempotency_key", sa.Text, nullable=False, unique=True),
        sa.Column("dataset_id", sa.BigInteger,
                  sa.ForeignKey("meta.datasets.dataset_id"), nullable=False),
        sa.Column("file_id", sa.BigInteger,
                  sa.ForeignKey("meta.files.file_id"), nullable=True),
        sa.Column("batch_id", sa.BigInteger,
                  sa.ForeignKey("meta.batches.batch_id"), nullable=True),
        sa.Column("config_version_id", sa.BigInteger,
                  sa.ForeignKey("meta.config_versions.config_version_id"), nullable=False),
        # NOT a ForeignKey yet: meta.schema_versions does not exist until a later
        # phase's migration. Column lands now (coherent design); constraint is
        # added there via op.create_foreign_key against this existing column.
        sa.Column("schema_version_id", sa.BigInteger, nullable=True),
        sa.Column("processor_version", sa.Text, nullable=False),
        sa.Column("status", sa.Text, nullable=False),
        # ... remaining columns per ARCHITECTURE.md §2.1
        schema="meta",
    )
    op.execute("GRANT SELECT, INSERT, UPDATE ON meta.ingestion_runs TO etl_app")
```

### Pattern 2: The Alembic "Wrong Database" Guard

**What:** STACK.md requires the analytical Alembic environment to "hard-fail if its connection
string resolves to the Airflow database." Implement as an assertion in `env.py` before any DDL runs.

**When to use:** Always, for this project's `migrations/env.py` — it is the one guard that turns a
catastrophic mistake (running analytical DDL against Airflow's metadata DB, an INFRA-04/§4
violation) into an immediate, loud failure instead of a silent one.

**Example:**
```python
# migrations/env.py
# Source: reasoned pattern; current_database() check is standard PostgreSQL,
# not Alembic-specific — [ASSUMED, MEDIUM confidence: not found verbatim in
# any fetched source this session, but a direct, low-risk application of a
# documented psycopg/PostgreSQL primitive]

EXPECTED_DATABASE = "analytics"  # matches helm/values/*/cnpg-analytics.yaml initdb.database

def run_migrations_online() -> None:
    connectable = engine_from_config(config.get_section(config.config_ini_section))
    with connectable.connect() as connection:
        actual_db = connection.execute(sa.text("SELECT current_database()")).scalar()
        if actual_db != EXPECTED_DATABASE:
            msg = (
                f"Refusing to run analytical migrations against database "
                f"'{actual_db}' (expected '{EXPECTED_DATABASE}'). This guard exists "
                f"specifically to prevent migrating the Airflow metadata database "
                f"(INFRA-04 / README §4)."
            )
            raise RuntimeError(msg)
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table_schema="meta",  # keeps `public` schema empty (see Open Questions)
        )
        with context.begin_transaction():
            context.run_migrations()
```

### Pattern 3: Errors-as-Values for Row-Level Problems (QUAL-03)

**What:** `StageResult.rejected: list[RejectedRecord]`. `dataplat.errors.DataPlatformError` and its
three D-06 subclasses are reserved for run-fatal conditions only, caught exactly once in `cli.py`.

**When to use:** Every stage in this phase's pipeline engine, from the first line of code —
retrofitting this after a stage has grown `except Exception: pass` somewhere is exactly the failure
mode QUAL-03 exists to prevent.

**Example:**
```python
# Source: ARCHITECTURE.md §4.5 Pattern 3, adapted
def apply(self, ctx: PipelineContext, chunk: RecordChunk) -> StageResult:
    kept: list[tuple[str, ...]] = []
    rejected: list[RejectedRecord] = []
    for i, row in enumerate(chunk.rows):
        if len(row) != chunk.expected_field_count:
            rejected.append(RejectedRecord(
                source_row_number=chunk.first_ordinal + i,
                error_type="RAGGED_ROW",
                error_message=f"expected {chunk.expected_field_count} fields, got {len(row)}",
                raw_line=",".join(row),
            ))
            continue  # never pad or truncate (polars #10585, CONTEXT.md D-01)
        kept.append(row)
    return StageResult(chunk=chunk.replace(rows=kept), rejected=rejected, findings=[])
```

### Anti-Patterns to Avoid

- **Writing the custom `io.RawIOBase` StreamingBody adapter PITFALLS.md E1 describes** — dead code
  against the pinned boto3 version; see Code Examples for the ~3-line replacement.
- **Putting `alembic`/`sqlalchemy` in `dataplat`'s runtime `[project.dependencies]`** — they are
  migration-time tooling; STACK.md itself says "run migrations as a Kubernetes Job... not from
  inside a DAG task," and the same logic means they don't belong in the deployed pod's dependency
  closure. Keep them in the root `dev` group (see Common Pitfalls).
- **Giving `meta.ingestion_runs.schema_version_id` a real FK constraint now** — the target table
  doesn't exist; Alembic will fail at `upgrade head` if you try.
- **Reaching into `StreamingBody._raw_stream`** — private, explicitly warned against by CONTEXT.md,
  and unnecessary now that `readinto()`/`readable()` are public methods.
- **Testing `dataplat.storage.objectstore` or `db.py` against the live kind cluster** — the whole
  point of this phase (ROADMAP success criterion 2) is that testcontainers proves it with no cluster
  present. If a test needs `kubectl`, it has drifted into `tests/e2e/cluster/` territory.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Schema migration tracking/versioning | A hand-rolled `schema_migrations` table + runner | Alembic (`alembic upgrade head`, `alembic_version` table) | Already locked; autogenerate-as-draft-only, hand-write every revision (STACK.md) |
| PostgreSQL/MinIO test fixtures | Custom Docker Compose or manual container lifecycle scripts | `testcontainers[postgres,minio]` `PostgresContainer`/`MinioContainer` context managers | Verified exact API this session — handles port mapping, wait-for-ready, teardown |
| Contextual log field propagation | Threading a logger + kwargs through 40 functions | `structlog.contextvars.bind_contextvars()` + `merge_contextvars` processor | Verified this session — bind once per run/file, every subsequent event inherits it |
| CSV↔text-stream bridging over S3 | A custom `io.RawIOBase` adapter class (what PITFALLS.md originally called for) | `io.TextIOWrapper(io.BufferedReader(response["Body"]), newline="")` directly | Verified: `StreamingBody` already satisfies the duck-typed interface `io.BufferedReader` needs |
| CLI argument/subcommand parsing | Hand-rolled `sys.argv` parsing or bare `argparse` subparsers for a growing multi-command CLI | `click` | Zero-dependency-of-its-own, mature, `click.testing.CliRunner` for clean unit tests |
| Secret redaction in logs | Manually calling `.replace()` on secret values before every log call | A `structlog` processor that drops keys matching a secret-pattern and truncates `raw_line`/`record` fields | One choke point instead of N call sites remembering to redact (OBS-05) |
| Content/config hashing | `hash()`, `md5`, or hashing in SQL | `hashlib.sha256` over a canonicalized (`sort_keys=True`, explicit separators) encoding, in Python only | PITFALLS C6: Postgres `digest()` will disagree with `hashlib` on encoding/normalization if both exist |

**Key insight:** every item in this table already has a mature, well-scoped tool that exactly fits
the phase's actual need — the risk in Phase 3 specifically is *reintroducing* complexity PITFALLS.md
warned about (the StreamingBody adapter) where the ecosystem has since made it unnecessary, not
under-tooling a genuinely hard problem.

## Common Pitfalls

### Pitfall 1: The `cluster` dependency-group split no longer means what Phase 1 wrote

**What goes wrong:** The root `pyproject.toml`'s `cluster` group (`boto3`, `psycopg[binary]`) exists
today specifically so that `make check`'s environment (built from the `dev` group, which already
installs `dataplat` and `csv-processor` as workspace members) never imports `boto3`/`psycopg` — the
comment says explicitly: "either would put boto3 into the environment `make check`'s offline gate
builds." That was true when `dataplat` had zero real dependencies (Phase 1 skeleton). **This phase
makes it false**: once `packages/dataplat/pyproject.toml` lists `boto3`/`psycopg` as genuine
`[project.dependencies]` (which it must, to have an `ObjectStore`/`MetadataRepository` at all),
installing `dataplat` — which `dev` already does — necessarily pulls them into `make check`'s own
environment too, regardless of the separate `cluster` group's continued existence.

**Why it happens:** the `cluster` group's original justification assumed a permanently-thin
`dataplat`. Phase 3 is precisely the phase that ends that assumption.

**How to avoid:** treat this as an explicit task, not a silent side effect. Recommended resolution:
the `cluster` group's remaining, *real* purpose narrows to gating `testcontainers[postgres,minio]`
(the genuinely heavy, Docker-orchestrating dependency that a clean-clone `make check` still must
never need) — not to gating `boto3`/`psycopg` importability, which is now unavoidable. Update the
`pyproject.toml` comment and the Makefile's `RUN_CLUSTER` rationale comment to say this plainly, so
a future reader does not conclude the offline claim silently broke. `make check`'s actual "no
services running" property is preserved as long as `tests/unit/` never *instantiates* a real boto3/
psycopg client (only `tests/integration/` does, gated behind the new D-04 target) — merely having
these packages importable does not violate "no network/services needed to pass."

**Warning signs:** a reviewer asserting `make check` is "no longer offline" because `boto3` shows up
in `pip list` inside its environment — that is expected and fine; what would be a real regression is
a *unit* test that opens a real S3/DB connection.

### Pitfall 2: `etl_app` has no password and no grants — don't assume Phase 3 can prove anything against the live cluster

**What goes wrong:** ROADMAP success criterion 1 ("`alembic upgrade head` against a throwaway
PostgreSQL") and criterion 2 ("no Kubernetes cluster present") are easy to read as merely a *style*
preference. They are not — `helm/values/local/cnpg-analytics.yaml`'s `postInitApplicationSQL` is
literally `CREATE ROLE etl_app LOGIN;` with no `PASSWORD` clause and no `GRANT`. `etl_app` cannot
currently authenticate with password auth at all, and has no privileges on anything. A plan task
that says "verify migrations against the live analytics-db cluster" will fail for reasons that have
nothing to do with the migration SQL.

**How to avoid:** every migration and every test in this phase targets testcontainers-provided
PostgreSQL exclusively (a throwaway superuser-ish role the container provides). Do not add a task
that runs `alembic upgrade head` against the live `analytics-db` CNPG cluster — that is out of this
phase's scope by design (no `analytics-db-credentials.sh`-style script exists yet, unlike
`minio-credentials.sh`/`airflow-metadata-secret.sh`; creating one is not this phase's job either,
per CONTEXT.md's explicit Vault/Phase-5 deferral). If the plan wants a forward-looking note, record
it as a fact for Phase 4/5, not a Phase 3 task.

**Warning signs:** any task text mentioning `kubectl`, `analytics-db`, or a live cluster in
Phase 3's plan is very likely scope creep into Phase 4/5 territory.

### Pitfall 3: `_record_hash`'s `hash_version` wasn't named in D-05's literal text — decide explicitly, don't skip it by omission

**What goes wrong:** D-05 names exactly two columns needing a companion `hash_version`:
`files.content_sha256` and `config_versions.config_hash`. `normalized.customers._record_hash` (one
of the six embedded lineage columns, ARCHITECTURE.md §2.3) is not named, but it is unambiguously "a
stored hash" in PITFALLS #1/C6's general sense, and this phase is the *only* phase that creates
`normalized.customers`. If the planner reads D-05 as an exhaustive list rather than two worked
examples of the general PITFALLS #1 rule, `_record_hash` ships without a version column and the
insurance policy PITFALLS.md calls "the single cheapest ... in the project" is skipped for the one
hash this phase actually mints new values for.

**How to avoid:** add `_record_hash_version smallint NOT NULL DEFAULT 1` (or equivalent) to the
`normalized.customers` migration. Flagged in the Assumptions Log below since it extends, rather
than merely implements, a locked decision — confirm with the user/planner rather than silently
deciding.

### Pitfall 4: mixing up which `boto3`/`psycopg` a given piece of code needs

**What goes wrong:** now that `dataplat` has real `boto3`/`psycopg` dependencies (Standard Stack,
above) *and* the root `dev`/`cluster` groups also reference them (test-time), it's easy to write
`migrations/env.py` importing `dataplat.storage.db` for a connection factory when Alembic actually
needs a **SQLAlchemy** engine (`sqlalchemy.create_engine`/`engine_from_config`), not a raw `psycopg`
connection — STACK.md is explicit that SQLAlchemy is "for DDL, Alembic metadata and query
construction... not for row loading," and psycopg's own COPY/query path is what the *application*
(`MetadataRepository`) uses.

**How to avoid:** keep two clearly-separated code paths from the first commit: `migrations/env.py`
uses SQLAlchemy + the `postgresql+psycopg://` dialect URL (psycopg3 is SQLAlchemy 2.0's supported
driver for that dialect); `dataplat/storage/db.py` uses raw `psycopg`/`psycopg_pool` with a plain
`postgresql://` conninfo. Never let one import the other's connection factory.

## Code Examples

### The StreamingBody → text stream bridge (corrected from PITFALLS.md E1)

```python
# dataplat/storage/objectstore.py
# Source: verified this session — source inspection of botocore 1.43.70's
# StreamingBody.readinto()/.readable() (both delegate to the underlying
# urllib3 stream, added per https://github.com/boto/botocore/issues/3108),
# plus an executable round-trip test proving embedded-newline CSV fields
# survive unchanged through this exact wrapping, at chunk sizes 1, 2 and 3.
import io
from collections.abc import Iterator

def open_text_stream(
    response_body: object,  # botocore.response.StreamingBody, kept untyped here
    *,
    encoding: str,
    newline: str = "",       # NEVER "" -> universal-newline translation; this IS "" deliberately (raw)
    errors: str = "strict",  # silent replace would violate "never silently discard" (§51)
) -> io.TextIOWrapper:
    """Wrap an S3/MinIO GetObject body as a text stream, no custom adapter needed.

    No custom io.RawIOBase subclass is required against boto3 >= (whatever
    shipped botocore's StreamingBody.readinto fix, comfortably before the
    1.43.68 pin) -- StreamingBody already implements readable()/readinto()
    directly. Do NOT reach into a private ._raw_stream attribute.
    """
    buffered = io.BufferedReader(response_body)  # type: ignore[arg-type]
    return io.TextIOWrapper(buffered, encoding=encoding, newline=newline, errors=errors)
```

### Record-ordinal chunking over the text stream (CSV-13)

```python
# dataplat/pipeline/engine.py (or csv_processor/source.py's chunks())
# Source: CONTEXT.md D-01 + verified empirically this session (chunk sizes
# 1, 2, 3 against an embedded-newline fixture all preserve exact field content)
import csv
import itertools
from collections.abc import Iterator

def chunked_records(
    text_stream: io.TextIOWrapper,
    *,
    dialect: csv.Dialect | type[csv.Dialect],
    chunk_size: int,
    field_size_limit: int,  # explicit, documented bound -- never sys.maxsize
) -> Iterator[tuple[int, list[tuple[str, ...]]]]:
    csv.field_size_limit(field_size_limit)
    reader = csv.reader(text_stream, dialect=dialect)
    header = next(reader)  # D-01: header at row 0, hardcoded (no detection)
    ordinal = 0
    for batch in itertools.batched(reader, chunk_size):
        yield ordinal, list(batch)     # checkpoint value: a RECORD ORDINAL, never a byte offset
        ordinal += len(batch)
```

### testcontainers fixtures (Postgres + MinIO), session-scoped

```python
# tests/integration/conftest.py
# Source: verified against testcontainers-python docs this session
# (PostgresContainer / MinioContainer exact constructor + methods)
import pytest
from testcontainers.postgres import PostgresContainer
from testcontainers.minio import MinioContainer

@pytest.fixture(scope="session")
def postgres_dsn() -> str:
    with PostgresContainer("postgres:18-bookworm", driver="psycopg") as pg:
        # get_connection_url(driver="psycopg") -> "postgresql+psycopg://..." for
        # SQLAlchemy/Alembic. Strip the SQLAlchemy dialect suffix for raw psycopg:
        sqlalchemy_url = pg.get_connection_url()
        yield sqlalchemy_url.replace("postgresql+psycopg://", "postgresql://")

@pytest.fixture(scope="session")
def minio_config() -> dict[str, str]:
    with MinioContainer() as minio:
        # get_client() returns the forbidden `minio` SDK client (STACK.md rejects
        # it) -- use get_config() and build a boto3 client instead, exactly as
        # dataplat's own ObjectStore does against real MinIO.
        yield minio.get_config()  # {"endpoint": ..., "access_key": ..., "secret_key": ...}

@pytest.fixture(scope="session")
def s3_client(minio_config: dict[str, str]):
    import boto3
    from botocore.config import Config
    return boto3.client(
        "s3",
        endpoint_url=f"http://{minio_config['endpoint']}",
        aws_access_key_id=minio_config["access_key"],
        aws_secret_access_key=minio_config["secret_key"],
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
```

### structlog configuration: dual renderer, contextvars, redaction (OBS-02/04/05)

```python
# dataplat/observability/logging.py
# Source: verified against current structlog docs this session
# (contextvars API, processor chain shape)
import structlog

_SECRET_KEY_PATTERN = ("password", "secret", "token", "credential", "dsn", "conninfo")
_TRUNCATE_KEYS = ("raw_line", "record")
_TRUNCATE_AT = 200

def _redact(_logger: object, _name: str, event_dict: dict) -> dict:
    for key in list(event_dict):
        if any(p in key.lower() for p in _SECRET_KEY_PATTERN):
            event_dict[key] = "***REDACTED***"
        elif key in _TRUNCATE_KEYS and isinstance(event_dict[key], str):
            value = event_dict[key]
            if len(value) > _TRUNCATE_AT:
                event_dict[key] = value[:_TRUNCATE_AT] + f"...[{len(value)} chars total]"
    return event_dict

def configure(*, in_cluster: bool, level: str = "INFO") -> None:
    renderer = structlog.processors.JSONRenderer() if in_cluster else structlog.dev.ConsoleRenderer()
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,  # MUST be first
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            _redact,                                   # OBS-05 — one choke point
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level.upper()),
        logger_factory=structlog.PrintLoggerFactory(),
    )

# Bound once at pipeline entry (ARCHITECTURE.md §4.5):
# structlog.contextvars.bind_contextvars(dataset=..., run_id=..., idempotency_key=...,
#     file_id=..., processor_version=...)
```

### `SecretsResolver` — `env://` and `file://` only (SEC-15)

```python
# dataplat/secrets/resolver.py
# Source: ARCHITECTURE.md Q9.1/9.3 design, this session's implementation shape
from __future__ import annotations
from pathlib import Path
from urllib.parse import urlsplit

class SecretResolutionError(Exception):
    """Run-fatal: a SecretRef could not be resolved (D-06 branch)."""

def resolve_secret(ref: str) -> str:
    """Resolve an opaque secret reference. The caller never learns which
    backend served it -- this is the whole point of SEC-15 / D3.
    vault:// is Phase 5's; anything else here is a ConfigurationError, not
    silently ignored.
    """
    parsed = urlsplit(ref)
    if parsed.scheme == "env":
        import os
        value = os.environ.get(parsed.netloc or parsed.path.lstrip("/"))
        if value is None:
            raise SecretResolutionError(f"env var not set for ref {ref!r}")
        return value
    if parsed.scheme == "file":
        try:
            return Path(parsed.path).read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise SecretResolutionError(f"cannot read {ref!r}: {exc}") from exc
    msg = f"unsupported secret ref scheme {parsed.scheme!r} in {ref!r}"
    raise SecretResolutionError(msg)
```

### Dockerfile — ADR-0004's `--no-install-workspace --frozen` → `--locked` ordering

```dockerfile
# docker/csv-processor/Dockerfile
# Source: STACK.md §I pattern + ADR-0004's explicit ordering requirement
FROM ghcr.io/astral-sh/uv:0.12.3 AS uv
FROM python:3.12-slim-bookworm AS builder
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# Dependency layer only -- workspace member SOURCE is not copied in yet, so
# --frozen (not --locked): --locked would try to validate member pyproject
# files whose src/ trees aren't present, and fail.
COPY pyproject.toml uv.lock ./
COPY packages/dataplat/pyproject.toml packages/dataplat/pyproject.toml
COPY packages/csv-processor/pyproject.toml packages/csv-processor/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-workspace --no-dev

# Now the member sources exist -- switch to --locked (full lockfile re-verified).
COPY packages/ packages/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

FROM python:3.12-slim-bookworm AS runtime
RUN groupadd -r app -g 1000 && useradd -r -g app -u 1000 -m app
COPY --from=builder --chown=app:app /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
USER 1000
# `dataplat` is the console-script name (packages/dataplat/pyproject.toml
# [project.scripts]), NOT `csv-processor` -- that's only the image's name
# (ADR-0004). Success criterion 3 literally invokes `dataplat --version`.
ENTRYPOINT ["dataplat"]
```

```toml
# packages/dataplat/pyproject.toml -- ADD this table
[project.scripts]
dataplat = "dataplat.cli:main"
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|---------------|--------|
| Custom `io.RawIOBase` adapter over `StreamingBody.read(n)` | Direct `io.BufferedReader(response["Body"])` | botocore added `readinto()`/`readable()` in response to [boto/botocore#3108](https://github.com/boto/botocore/issues/3108) (opened 2024-01-29) | One fewer custom class, its tests, and its mypy-strict typing burden; PITFALLS.md E1's *implementation detail* (not its substance) is stale |

**Deprecated/outdated in the inputs to this phase:**
- PITFALLS.md E1 and CONTEXT.md D-01's "a small `io.RawIOBase` adapter... is required" claim, for
  the pinned boto3 version. The surrounding guidance (never split by lines, `newline=""`, record-
  ordinal checkpoints, explicit `field_size_limit`, pre-filter NUL bytes, never pad/truncate ragged
  rows) is unaffected and still fully correct.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|----------------|
| A1 | `click` is the right CLI framework for `dataplat.cli` | Standard Stack, Code Examples | Low — swapping to `argparse` or `typer` later touches only `cli.py` and its tests, not the pipeline engine or schema. Confirm before Phase 4 grows the `ingest` subcommand on top of it |
| A2 | `normalized.customers._record_hash` should get a companion `hash_version` column, extending D-05's two named examples | Common Pitfalls #3, Migration Sequencing | Low-medium — if wrong, it's an extra unused column (cheap to have and not need); if omitted and later needed, it's the exact DATA CORRUPTION scenario PITFALLS #1/C6 describes. Confirm with the user given D-05 didn't name it explicitly |
| A3 | `version_table_schema="meta"` (rather than the Alembic default, `public`) for the `alembic_version` tracking table | Migration Sequencing Pattern 2 | Low — cosmetic/organizational; reversible via a `MIGRATION_TABLE` note if the planner prefers `public` (consistent with plain-Alembic default) |
| A4 | The Alembic "wrong database" guard should check `current_database()` against a literal `"analytics"` | Migration Sequencing Pattern 2 | Low-medium — the mechanism (fail loudly before DDL) is sound and directly requested by STACK.md; the exact check implementation is this session's design, not sourced from an authoritative doc. A config-driven expected-name (rather than a hardcoded literal) is a reasonable refinement the planner may prefer |
| A5 | `psycopg_pool.ConnectionPool(conninfo, min_size=1, max_size=2, open=False)` + explicit `.open()`/`.wait()` is the current recommended pattern | Standard Stack (Alternatives Considered) | Low — the psycopg docs page 403'd this session's fetch attempt; the `min/max_size` shape is standard, stable psycopg3 knowledge, but the `open=False` nuance should be re-verified against current psycopg_pool docs at implementation time |

**If this table is empty:** N/A — see entries above. Everything else in this document is either
[VERIFIED] this session (PyPI, slopcheck, source inspection, executable tests, official docs fetch)
or [CITED] from the already-adjudicated ARCHITECTURE.md/PITFALLS.md/STACK.md/CONTEXT.md, none of
which are themselves flagged `[ASSUMED]` at the specific claims this document relies on.

## Open Questions (RESOLVED)

All three questions below were resolved during planning — each recommendation was adopted as
written, and all three land in plan `03-02-PLAN.md` Task 1. Left in place as a record of the
reasoning; see the plan for the implementing detail.

1. **Should the `cluster` dependency-group's scope and its `pyproject.toml`/Makefile comments be
   updated explicitly in this phase's plan, or left as a known-stale comment for a later cleanup?**
   - What we know: the comment's literal claim ("keeps boto3 out of `make check`'s environment")
     becomes false the moment `dataplat` gets real `boto3`/`psycopg` dependencies, which this phase
     must do.
   - What's unclear: whether the planner wants this as an explicit task (touch two comments, no
     behavior change) or accepts the drift silently until someone notices.
   - Recommendation: make it an explicit, small task — it costs minutes now, and the alternative is
     a future contributor trusting a comment that is actively wrong about what `make check` does.
   - **Resolved:** `03-02-PLAN.md` Task 1 rewrites the `cluster` group's comment so it no longer
     claims boto3/psycopg importability is gated by group membership.

2. **Does `_record_hash_version` belong in this phase's migration, or is D-05's two-column list
   exhaustive by design?**
   - What we know: PITFALLS #1/C6 states the general principle for "every stored hash"; D-05 names
     two examples that happen to be the two hash columns *this phase's migrations create in `meta`*
     — it does not discuss `normalized.customers` hash columns at all.
   - What's unclear: whether the omission was deliberate (record-level hash versioning belongs to
     whichever phase actually needs to change the hash recipe, i.e. never, if the recipe is frozen)
     or incidental (D-05 simply wasn't thinking about the `normalized` schema when it wrote the list).
   - Recommendation: add it — the cost of an unused column is near zero; the cost of needing it later
     without it is the exact DATA CORRUPTION scenario the entire pitfall exists to prevent.
   - **Resolved:** `03-02-PLAN.md` Task 1 adds `_record_hash_version smallint NOT NULL DEFAULT 1` to
     `normalized.customers` in `migrations/versions/0005_normalized_customers.py`, alongside the six
     embedded lineage columns. CONTEXT.md D-05's citation was updated to name this explicitly.

3. **Where exactly should `alembic`/`sqlalchemy` live in the dependency graph** — root `dev` group
   (this document's recommendation), a new dedicated group, or actually inside `dataplat`'s runtime
   deps after all (if a future phase wants the pod to be able to self-migrate)?
   - What we know: STACK.md says migrations run as a Kubernetes Job, not from a DAG task or (by
     extension) the ingest pod itself.
   - What's unclear: whether some future phase wants an `dataplat migrate` CLI subcommand that runs
     inside the same image, which would pull alembic back into the runtime deps.
   - Recommendation: keep it out of runtime deps now (matches ADR-0004's slim-image goal and
     STACK.md's stated migration-execution model); this is cheap to revisit later since it's an
     additive change, not a structural one.
   - **Resolved:** `03-02-PLAN.md` Task 1 adds `alembic`/`sqlalchemy` to the root `dev` group (not
     `dataplat`'s runtime deps), as recommended. `testcontainers[postgres,minio]` was separately
     corrected during pattern-mapping/planning to live in `cluster`, not `dev` (this document's own
     Installation code block was stale on that point — `dev` must stay Docker-free per CONTEXT.md
     D-04; `hypothesis` was already correctly placed in `dev`).

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Docker daemon | `tests/integration/` (testcontainers) | ✓ | 29.6.2, daemon confirmed live (`docker ps` succeeded) this session | — |
| Python | Everything | ✓ | 3.12.3 | — |
| uv | Package management, `make install`/`check` | ✓ | 0.12.3 (matches `UV_REQUIRED_VERSION` pin exactly) | — |
| kind cluster | NOT required by this phase | ✓ (already running, 3 nodes, from Phase 2) | — | Irrelevant — success criterion 2 explicitly requires this phase to work with **no** cluster present; do not add a task that depends on it |
| Live analytical PostgreSQL (`analytics-db` CNPG cluster) | NOT required by this phase | ✓ reachable in principle, but `etl_app` has no password (Pitfall 2) | — | testcontainers-provided PostgreSQL is the actual target for every migration/test this phase runs |

**Missing dependencies with no fallback:** none.
**Missing dependencies with fallback:** none — everything this phase needs is present and verified.

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest 9.1.1 (already configured, `[tool.pytest.ini_options]` in root `pyproject.toml`) |
| Config file | `pyproject.toml` (`[tool.pytest.ini_options]`, `testpaths = ["tests"]`) |
| Quick run command | `uv run --frozen pytest tests/unit -q` |
| Full suite command | `make check && make test-integration` (new target this phase adds, per D-04) |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| META-01 | `alembic upgrade head` creates the complete slice schema | integration | `pytest tests/integration/test_migrations.py -x` | ❌ Wave 0 |
| META-02 | `files`/`config_versions` (and recommended `normalized.customers`) carry `hash_version`/`_record_hash_version` | integration | `pytest tests/integration/test_migrations.py::test_hash_version_columns -x` | ❌ Wave 0 |
| INFRA-08 | Built image tag equals `git rev-parse --short HEAD`, never `latest` | integration/build | `pytest tests/policy/test_no_latest_image_tag.py -x` (mirrors existing `tests/policy/` style) | ❌ Wave 0 |
| SEC-15 | `resolve_secret()` handles `env://`/`file://`; never returns/logs the raw ref scheme mismatch silently | unit | `pytest tests/unit/test_secrets_resolver.py -x` | ❌ Wave 0 |
| SEC-15 | A credential value passed through the resolver never appears in a captured log line | unit | `pytest tests/unit/test_logging_redaction.py -x` | ❌ Wave 0 |
| CSV-13 | Embedded-newline records survive at chunk sizes 1, 2, 3 | unit + property | `pytest tests/unit/test_csv_chunking.py tests/property/test_chunking_properties.py -x` | ❌ Wave 0 |
| SCHEMA-07 | Identical config (reordered keys) hashes identically; changed content hashes differently | unit | `pytest tests/unit/test_config_hashing.py -x` | ❌ Wave 0 |
| SCHEMA-07 | Config-sync round trip writes the expected `meta.config_versions` row | integration | `pytest tests/integration/test_config_registry.py -x` | ❌ Wave 0 |
| OBS-02/04 | `structlog` emits JSON in-cluster, console locally; bound context appears on every subsequent event | unit | `pytest tests/unit/test_logging_config.py -x` | ❌ Wave 0 |
| OBS-05 | Redaction processor drops secret-pattern keys and truncates `raw_line`/`record` | unit | `pytest tests/unit/test_logging_redaction.py -x` (shared file with SEC-15's log test) | ❌ Wave 0 |
| QUAL-03 | A malformed row produces a `RejectedRecord`, never raises | unit | `pytest tests/unit/test_pipeline_errors.py -x` | ❌ Wave 0 |
| QUAL-03 | `DataPlatformError` subclasses carry `context: dict`; caught exactly once in `cli.py` | unit | `pytest tests/unit/test_cli_error_handling.py -x` (uses `click.testing.CliRunner`) | ❌ Wave 0 |

### Sampling Rate

- **Per task commit:** `uv run --frozen pytest tests/unit -q` (fast, no Docker)
- **Per wave merge:** `make check && make test-integration` (full suite, Docker required)
- **Phase gate:** Full suite green before `/gsd:verify-work`, plus `alembic upgrade head` proven
  fresh against a throwaway testcontainers Postgres (not a warm/reused one) at least once.

### Wave 0 Gaps

- [ ] `tests/integration/conftest.py` — `postgres_dsn`, `minio_config`, `s3_client` fixtures (Code
      Examples above give the exact shape)
- [ ] `tests/integration/test_migrations.py` — the META-01/META-02 proof
- [ ] `tests/unit/test_secrets_resolver.py`, `test_logging_redaction.py`, `test_logging_config.py`
- [ ] `tests/unit/test_csv_chunking.py` + `tests/property/test_chunking_properties.py` — the
      embedded-newline-survives-chunking property (Claude's Discretion item; this document's own
      empirical test this session is a ready-made starting point for the fixture shape)
- [ ] `tests/unit/test_config_hashing.py`, `tests/integration/test_config_registry.py`
- [ ] `tests/unit/test_pipeline_errors.py`, `test_cli_error_handling.py`
- [ ] Makefile target: `test-integration` (D-04) — `$(RUN_CLUSTER) pytest tests/integration -q`
      (reusing the existing `RUN_CLUSTER` variable is natural now that `dataplat` itself needs
      `boto3`/`psycopg`; see Common Pitfalls #1 for why the group's *comment* still needs updating)
- [ ] `.github/workflows/ci.yml`: new `integration` job (Docker is available on `ubuntu-latest`
      runners by default — no `docker:dind` service needed) running `make test-integration`,
      structured like the existing `manifests`/`secrets` jobs (separate from `check`, per D-04)

*(Everything above is new — this phase has no pre-existing test infrastructure for its own domain,
only the repo-wide pytest/ruff/mypy scaffolding Phase 1 built, which this phase's tests plug into.)*

## Security Domain

### Applicable ASVS Categories

(`security_asvs_level: 1`, `security_block_on: "high"` per `.planning/config.json`)

| ASVS Category | Applies | Standard Control |
|---------------|---------|-------------------|
| V2 Authentication | Partial | Not this phase's DB/S3 auth itself (testcontainers roles are throwaway); relevant only in that `SecretsResolver` must never itself become a credential store |
| V3 Session Management | No | Batch ETL library, no sessions |
| V4 Access Control | Yes | `GRANT` statements in migrations give `etl_app` least-privilege access (SELECT/INSERT/UPDATE, never DDL rights, never superuser) — direct continuation of Phase 2's D-15 role design |
| V5 Input Validation | Yes | Pydantic `DatasetConfig` (`extra="forbid"`), `csv.field_size_limit` explicit bound, NUL-byte pre-filtering before the csv reader, ragged rows rejected (never padded/truncated) |
| V6 Cryptography | Yes | SHA-256 (`hashlib.sha256`) for content/config/record hashing — never hand-rolled, never hashed in SQL (PITFALLS C6) |
| V7 Error Handling / Logging | Yes | `DataPlatformError` hierarchy (D-06) + the redaction processor (OBS-05) are this ASVS category made concrete |
| V9 Communications | Partial | psycopg/boto3 TLS is a testcontainers-vs-production distinction outside this phase's scope (testcontainers doesn't need TLS); note for Phase 4/5 when real cluster connections are wired |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|----------------------|
| Credential leakage via log lines | Information Disclosure | `structlog` redaction processor (OBS-05); never construct a log event that interpolates a raw secret value before the processor chain runs |
| SQL injection via dynamic schema/table names in Alembic `op.execute()` | Tampering | All schema/table identifiers in this phase's migrations are static Python string literals, never user- or config-derived; if a future phase parameterizes them, use `psycopg.sql.Identifier`, never f-string interpolation |
| Overly broad DB role grants ("just widen it until it works") | Elevation of Privilege | Explicit `GRANT SELECT, INSERT, UPDATE ON <schema>.<table> TO etl_app` per table in the migration that creates it — never `GRANT ALL` or a schema-wide blanket grant (mirrors Phase 2's D-15 precedent and PITFALLS #13's warning about the "usual fix" of widening a role) |
| Config-hash collision/canonicalization ambiguity used to smuggle a different config past a hash check | Tampering | `sort_keys=True`, explicit `separators`, canonical JSON dump per ARCHITECTURE.md §5.2 — deterministic and reviewed, not ad hoc |
| Secret reference scheme confusion (e.g. a `vault://` ref silently falling through to being treated as a literal) | Tampering / Information Disclosure | `resolve_secret()` raises `SecretResolutionError` on any unrecognized scheme — fails closed, never silently passes an un-resolved string through as if it were a value |

## Sources

### Primary (HIGH confidence)

- `.claude/CLAUDE.md` — project-pinned stack table (psycopg, alembic, structlog, testcontainers,
  hypothesis, pydantic, boto3 versions), constraints, "What NOT to Use"
- `.planning/research/ARCHITECTURE.md` Q2 (meta schema), Q3 (schema boundaries), Q4 (package
  architecture, `Source`/`Stage`/`Publisher`), Q5 (config system), Q9 (secrets flow), Recommended
  Repository Structure, Anti-Patterns
- `.planning/research/PITFALLS.md` #1 (hash_version), #5/E1 (CSV chunking), C5/C6 (idempotency,
  change-hash design), C12 (connection budgets, wrong-database accident)
- `.planning/research/STACK.md` §F (Python ETL library), §I (CI/CD, container build)
- `docs/adr/0002-dataplat-core-with-csv-processor-plugin.md`,
  `docs/adr/0003-uv-workspace-members-under-packages.md`,
  `docs/adr/0004-two-images-two-dependency-sets.md`
- `helm/values/local/cnpg-analytics.yaml`, `helm/values/local/cnpg-airflow.yaml` — live confirmation
  of Phase 2's actual `postInitApplicationSQL` (schema-free, `etl_app` role only)
- `.planning/phases/02-kind-cluster-core-infrastructure/02-03-PLAN.md`, `02-RESEARCH.md` — D-14/D-15
  decision text and verification evidence
- `pyproject.toml`, `setup.cfg`, `Makefile`, `.github/workflows/ci.yml` — current repository
  conventions (dependency groups, import-linter contract, `make check`/`make ci` gate shape)
- `packages/dataplat/`, `packages/csv-processor/` — existing Phase-1 skeleton, read directly
- `tests/conftest.py`, `tests/e2e/cluster/conftest.py`, `tests/policy/test_no_postgres_csv_parsing.py`
  — existing fixture and policy-test conventions this phase's new tests should match
- [www.structlog.org/en/stable/contextvars.html](https://www.structlog.org/en/stable/contextvars.html) — `bind_contextvars`/`merge_contextvars` API, fetched this session
- [www.structlog.org/en/stable/processors.html](https://www.structlog.org/en/stable/processors.html) — processor chain shape, fetched this session
- [testcontainers-python.readthedocs.io — postgres module](https://testcontainers-python.readthedocs.io/en/latest/modules/postgres/README.html), [minio module](https://testcontainers-python.readthedocs.io/en/latest/modules/minio/README.html) — exact constructor/method signatures, fetched this session
- [docs.aws.amazon.com/botocore/latest/reference/response.html](https://docs.aws.amazon.com/botocore/latest/reference/response.html) — `StreamingBody` method signatures, fetched this session
- Direct source inspection of installed `botocore==1.43.70`'s `StreamingBody.readinto`/`.readable`,
  this session (`inspect.getsource`)
- Executable round-trip test (embedded-newline CSV through `io.BufferedReader(StreamingBody)`,
  chunk sizes 1/2/3), this session, against boto3 1.43.68 in an isolated venv
- [github.com/boto/botocore/issues/3108](https://github.com/boto/botocore/issues/3108) — "Add
  'readinto' shim to botocore.response.StreamingBody," confirming when/why this was added
- `pip index versions` for psycopg, alembic, structlog, testcontainers, click, typer, hypothesis,
  pydantic, boto3-stubs, mypy-extensions — run this session against the live PyPI index
- `slopcheck install <9 packages>` — run this session, all 9 returned `[OK]`
- `pip show typer` / `pip show click` — dependency-footprint comparison, run this session

### Secondary (MEDIUM confidence)

- Alembic `version_table_schema` and multi-schema cookbook guidance — WebSearch summary of
  [alembic.sqlalchemy.org/en/latest/cookbook.html](https://alembic.sqlalchemy.org/en/latest/cookbook.html) and [api/runtime.html](https://alembic.sqlalchemy.org/en/latest/api/runtime.html), cross-referenced across two independent search results rather than a single direct fetch
- `psycopg.org/psycopg3/docs/advanced/typing.html` (native typing, mypy strict) — WebSearch summary
  (direct WebFetch to `psycopg.org` was 403'd this session)
- `psycopg_pool.ConnectionPool` `open=False` pattern — general psycopg3 knowledge, not freshly
  fetched this session (the pool docs page 403'd); flagged in Assumptions Log

### Tertiary (LOW confidence — none used as load-bearing claims)

- None. Every claim in this document is either project-internal (grep/read of this repository),
  freshly verified via tool this session, or cited from the already-adjudicated prior research
  documents.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — every version either already locked+dated in CLAUDE.md or freshly
  confirmed via `pip index versions`/slopcheck this session; only the CLI framework choice is a
  genuinely new recommendation, clearly flagged
- Architecture: HIGH — built directly on ARCHITECTURE.md's already-MEDIUM-confidence design, with
  this session's job being to resolve its remaining implementation ambiguities (FK deferral,
  StreamingBody bridging, migration sequencing) against concrete, verified facts
- Pitfalls: HIGH — the StreamingBody correction and the `cluster`-group tension are both verified
  via direct source/tool inspection this session, not inference

**Research date:** 2026-08-12
**Valid until:** 30 days for the architectural guidance (stable); the boto3/botocore-specific
finding should be re-checked if the pinned boto3 version ever moves backward (unlikely) or if a
much later boto3 major changes `StreamingBody`'s interface again.
