# Phase 3: `dataplat` Core Library & Metadata Control Plane - Context

**Gathered:** 2026-08-12
**Status:** Ready for planning

<domain>
## Phase Boundary

A coherently-designed metadata control plane — the Alembic-migrated `meta` schema in analytical
PostgreSQL — plus the source-agnostic pipeline engine (`Source`/`Stage`/`Publisher` protocols,
the sequencing/chunk-loop/checkpoint executor, `SecretsResolver`, structured logging) as one
testable Python library. Proven entirely via testcontainers-provided PostgreSQL and MinIO —
**no live Kubernetes cluster** is used or required to pass this phase.

This phase's job is to make README §68's aspiration concrete in code where research found it
under-specified: the `dataplat` core / `csv_processor` plugin split (locked in Phase 1 via
ADR-0002) gets its actual composition seam (`Source` → `RecordChunk` → `Publisher`), its
metadata home (`MetadataRepository`, the `meta` schema), its config layer (`ConfigRegistry`,
hashing, versioning), and its cross-cutting concerns (errors, logging) — the six gaps
ARCHITECTURE.md §4.1 found in a literal reading of §68.

**Out of scope — belongs to other phases:**

- **Full CSV detection engine** (encoding/dialect/header/footer detection, type inference,
  normalization) — Phase 6. CSV-01 through CSV-12 all map there. Phase 3 owns only CSV-13 (the
  streaming/chunking *rule*), delivered as a minimal, hardcoded `csv_processor.Source` (see
  Decisions).
- **Any real DAG, `KubernetesPodOperator`, XCom, or task pod spec** — Phase 4. Phase 3 produces
  a library the Phase 4 DAG imports; it does not touch `airflow/dags/`.
- **Vault, and any concrete `vault://` `SecretsResolver` implementation** — Phase 5. Phase 3
  proves `env://` and `file://` only.
- **Publisher implementations** (`merge`, `partition_replace`, `full_swap`, SCD/CDC publishers)
  — the `Publisher` protocol is defined here; concrete strategies arrive with the phases that
  need them (Phase 4 for `merge`, Phase 10 for SCD/CDC).
- **Real observability backends** (Prometheus, OTel Collector, Grafana Tempo) — Phase 7. Phase 3
  ships real structured logging (required now) plus no-op `metrics`/`tracing` call sites (see
  Decisions).
- **Post-slice `meta.*` tables** (`schema_versions`, `run_stages`, `watermarks`,
  `validation_results`, `dedup_audit`, `quality_metrics`, `reconciliation_results`,
  `dataset_sla`, `cdc_offsets`, `record_lineage`, etc.) — each lands in the migration of the
  phase that first populates it (see Decisions).
- **kind cluster, Helm, any Kubernetes manifest** — Phase 2's territory, already complete and
  explicitly hands-off to `packages/`.

</domain>

<decisions>
## Implementation Decisions

### CSV reading capability landed in this phase

- **D-01:** Phase 3 builds a **minimal, working `csv_processor.Source`** implementing the
  `Source`/`RecordStream` protocol — hardcoded UTF-8 encoding, comma delimiter, header at row 0,
  **no encoding/dialect/header detection**. Proven against a fixture. This is what makes CSV-13
  concrete rather than abstract: a single `csv.reader` over an `io.TextIOWrapper(..., newline="")`
  wrapper, chunked in records via `itertools.batched`/`islice`, never by lines or byte offsets
  (PITFALLS E1 / cheap-now decision #5).
  — **Reason:** Phase 4 (the vertical slice) has **zero CSV-\* requirements of its own** and is
  explicitly "the critical path... strictly serial... protect it. Do not widen scope." Phase 4
  must have a real, working reader to plug into its `KubernetesPodOperator` pod without absorbing
  CSV-reading work itself. The full detection engine (CSV-01–12) still waits for Phase 6, which
  depends on Phase 4 and explicitly builds on "the record-chunking rule already fixed in Phase 3
  (CSV-13)."
  — **boto3's `StreamingBody` is not a `BufferedIOBase`**: `io.TextIOWrapper` will not accept it
  directly. A small `io.RawIOBase` adapter implementing `readinto()` over `body.read(n)`, wrapped
  in `io.BufferedReader`, is required — this is the seam ARCHITECTURE.md already wants around
  boto3. Do not reach into `body._raw_stream` (private, will break).
  — Checkpoints are **record ordinals** (`last_committed_chunk_ordinal`), never byte offsets —
  resuming CSV parsing from a byte offset inside a quoted field is not possible.
  — `csv.field_size_limit` must be set to an explicit, documented bound — never `sys.maxsize`
  (an unbounded field limit turns a malformed quote into an OOM kill).
  — Pre-filter NUL bytes before the stdlib csv reader (cpython #71767). Ragged rows are errors —
  never pad or truncate (polars #10585).

### Vertical-slice demo dataset

- **D-02:** The target table is `normalized.customers`: `customer_id` (business key), `name`,
  `country`, `birth_date`, `event_ts` — the shape ARCHITECTURE.md uses throughout its worked
  examples (the `MERGE` publication strategy, the config-not-code sample). Plus the six embedded
  lineage columns from ARCHITECTURE.md §2.3 on every target table: `_run_id`, `_file_id`,
  `_batch_id`, `_source_row_number`, `_record_hash`, `_ingested_at`. No opt-in
  `meta.record_lineage` table — target row = source row here, so embedded columns are sufficient.
  — **Reason:** keeps every downstream phase's worked examples (in research and in this
  discussion) consistent with the actual schema; zero new modeling work.
  — **Consequence:** since SCHEMA-07 (config versioning/hashing) is a Phase 3 requirement, this
  phase seeds one real `configs/datasets/customers.yaml` and proves the config-sync round trip
  (load → canonicalize → hash → `meta.config_versions` row) against it — there must be a real
  document to hash, and this is the one. The Airflow-side `config-sync` DAG (ARCHITECTURE.md
  §5.1) is **not** built here — only the library-side loader/hasher/registry.

### Observability seam depth

- **D-03:** Real structured logging **now** — `structlog` with `contextvars`, JSON renderer
  in-cluster / console renderer locally, a redaction processor dropping secret-pattern keys and
  truncating `raw_line`/`record` fields. This is not optional or seam-only: success criterion 5
  requires it directly (OBS-02, OBS-04, OBS-05).
  `metrics.py` and `tracing.py` are **no-op seams with real call sites already threaded through
  the pipeline stages** (e.g., a stage genuinely calls `metrics.increment("rows_loaded", n)`,
  which does nothing until Phase 7 wires a real backend). Phase 7 becomes a pure backend-wiring
  phase — no pipeline code changes, only `metrics.py`/`tracing.py` internals swap from no-op to
  StatsD-exporter / OTel Collector.
  — **Reason:** matches PROJECT.md's already-committed "most complete observability tier"
  stance (Key Decisions: "User chose the most complete observability tier... justified by
  'foundation for real work'"). Threading call sites now means Phase 7 never has to go back and
  find every place a metric or span *should* have been emitted.

### Test suite gating for testcontainers

- **D-04:** The testcontainers-based integration suite (`tests/integration/`) gets its **own
  target** (e.g. `make test-integration`), mirroring Phase 2's `cluster-verify` precedent.
  `make check` — the existing local + CI gate — stays Docker-free and fast, exactly as it has
  been since Phase 1.
  — **This is not merely a preference — Phase 1 left a standing instruction.** `Makefile` (the
  `test:` target, lines 94–97) already states: *"tests/property, tests/integration and tests/e2e
  are deliberately NOT here: they are empty today and will need testcontainers or a live
  cluster. **Phase 3 must add them to a target that can provide those, and must not assume `make
  check` already collects them.**"* D-04 satisfies that instruction literally.
  — The new target still runs automatically in CI (as its own job/step, consistent with CICD-02
  "PRs automatically run the full quality gate," already complete) — it is separated from
  `make check` for local-dev speed and Docker-optionality, not exempted from CI.

### Metadata schema migration granularity

- **D-05:** Alembic migrations in this phase create **only the five slice tables** —
  `meta.datasets`, `meta.config_versions`, `meta.files`, `meta.batches`, `meta.ingestion_runs` —
  plus the `meta.batch_files` join table and `normalized.customers`. The ~14 remaining
  `meta.*` tables from ARCHITECTURE.md §2.2 (`schema_versions`, `run_stages`, `watermarks`,
  `watermark_history`, `validation_results`, `rejected_records`, `dedup_audit`,
  `dedup_decisions`, `quality_metrics`, `reconciliation_results`, `dataset_sla`,
  `retention_policies`, `cdc_offsets`, `record_lineage`) are each created by the migration of the
  phase that first populates them, per ARCHITECTURE.md §2.4's phase mapping.
  — **Reason:** this is what the roadmap's own plan-guidance bullet says literally ("Land the
  five tables the slice needs... plus `normalized.customers` against that complete design").
  "Coherent design" (deviation D2) refers to the schema having been **designed** up front
  (already done, in ARCHITECTURE.md §2 — every FK target and column shape for all ~19 tables is
  already specified) so that later migrations never need to redesign a foreign key or discover
  an inconsistency — not to all DDL being applied in one phase. Every stored hash column landed
  in this phase's migrations (`files.content_sha256`, `config_versions.config_hash`) carries a
  companion `hash_version` column, per success criterion 1's literal text and PITFALLS #1/C6.
  — Success criterion 1's "creates the whole meta schema" is satisfied as: "creates the complete
  slice of the meta schema this phase owns, from a design that is whole." Do not read it as
  "every meta table across all 11 phases."

### Exception hierarchy scope

- **D-06:** `dataplat.errors` defines the base `DataPlatformError` plus only the branches this
  phase's own code can actually raise: `ConfigurationError`, `StorageError`,
  `SecretResolutionError`. Branches owned by later phases (`SourceError` family beyond what CSV-13
  needs, `SchemaError` family, `QualityThresholdExceeded`, `PublicationError`) are added by the
  phase that first raises them, alongside that code.
  — **Reason:** consistent with the general project instruction against building for
  hypothetical future requirements. An exception subclass with no raise site and no test
  exercising it is dead code wearing a design decision's clothes.
  — Every exception still carries `context: dict` populated from `PipelineContext` per
  ARCHITECTURE.md §4.5, and is caught exactly once (in `cli.py`), which writes
  `error_type`/`error_message`/`error_detail` to `meta.ingestion_runs`. Row-level problems never
  raise — a malformed row becomes a `RejectedRecord` inside `StageResult.rejected` (Pattern 3).

### Claude's Discretion

The user delegated several sub-decisions explicitly ("Let Claude decide") — resolved above by
taking the recommended option in each case (D-01, D-03, D-04, D-05, D-06), for the reasons
stated. Additional latitude within those decisions:

- Exact fixture CSV used to prove the minimal `csv_processor.Source` (row count, specific values)
  — must be UTF-8/comma/header-row-0 per D-01, matching `normalized.customers`' shape per D-02.
- Whether a fast in-memory fake `MetadataRepository` exists alongside the Postgres-backed one,
  for pure-unit tests that don't need testcontainers. Not required by any success criterion, but
  a reasonable speed optimization within D-04's separation.
- Whether a property test (hypothesis) proves the chunking logic never drops, reorders, or splits
  records across arbitrary chunk sizes. Not required by a Phase 3 requirement (QUAL-16 is Phase
  6), but the fixture corpus's boundary-case guidance (PITFALLS E1: "parameterised over chunk
  sizes 1, 2, 3") already anticipates exactly this kind of test.
- Whether `RejectedRecord` and other per-row error objects use `@dataclass(slots=True,
  frozen=True)` per STACK.md's general per-row-error-object guidance.
- Exact `normalized.customers` column types/constraints beyond the five named business columns —
  e.g. `customer_id::int` as shown in ARCHITECTURE.md's `MERGE` example, PK/uniqueness shape.
- The §68-departure ADR the roadmap requires ("record the departure as an ADR now so it is not
  re-litigated at Phase 10") — exact ADR number and wording; the decision itself (the
  `Source`/`RecordChunk`/`Publisher` seam replacing §68's flat taxonomy) is already made by
  ADR-0002 plus this phase's implementation.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Adjudicated research — the authority for this phase
- `.planning/research/ARCHITECTURE.md` **Question 2** (lines 135–278) — the complete `meta`
  schema design, all ~19 tables, slice-vs-later table mapping (§2.4). **Question 3** (280–386) —
  schema boundaries (`meta`/`staging`/`quarantine`/`normalized`/`warehouse`/`analytics`) and why
  staging is all-TEXT/`UNLOGGED`. **Question 4** (388–583) — the honest critique of README §68,
  the recommended `dataplat`/`csv_processor` package structure, the `Source`/`Stage`/`Publisher`
  protocols, config-not-code, the error hierarchy and structlog threading. **Question 5**
  (585–658) — the config system: repo-authors/DB-system-of-record, canonicalized-JSON hashing,
  the `config_policy` knob (`AS_OF_LOGICAL_DATE`/`LATEST`/`PINNED`). **Question 9** (936–1027) —
  the full secrets trust-boundary chain from KPO pod through Vault to `SecretsResolver`
  (Phase 3 builds only the `SecretsResolver` end of this: `env://`/`file://`). Recommended
  Repository Structure (1211–1258). Anti-Patterns AP2, AP3, AP8, AP11 (1323–1356, 1368–1371) —
  typed staging tables, filename-as-identity, deferring the metadata schema, config-only-in-a-
  ConfigMap — all directly relevant to this phase's migrations and config layer.
- `.planning/research/PITFALLS.md` — the fifteen-decision table (lines 30–58): **#1** (`
  hash_version` alongside every hash, D-05), **#5** (record-ordinal chunking via one `csv.reader`
  over `newline=""`, D-01), **#13** (explicit `namespace`/`service_account_name` matched to Vault
  role — decide-by Phase 3/4, informs how `SecretsResolver` refs are shaped even though pods
  aren't declared until Phase 4). **E1** (1491–1548) — the three ways naive chunking corrupts
  embedded-newline records, and the `io.RawIOBase`/`io.BufferedReader` adapter needed over
  boto3's `StreamingBody`. **C6** (1038–1074) — change-hash design rules (field-boundary
  ambiguity, NULL≠empty-string, never hash a float, compute in Python only) — relevant if this
  phase's `content_sha256`/`config_hash` hashing sets any precedent later hashing follows.
- `.planning/research/STACK.md` § F "Python ETL Library" — every pinned library for this phase:
  `psycopg[binary,pool]` 3.3.4 (COPY, staging→MERGE), Alembic 1.19.1 (hand-write every revision;
  autogenerate only for a draft; never point Alembic at the Airflow metadata DB — hard-fail if
  the connection string resolves to it), `structlog` (via ARCHITECTURE.md, D-03), Pydantic v2 for
  config/contracts only (never per-row), `testcontainers[postgres,minio]` 4.15.0,
  `hypothesis` 6.165.3, `boto3` 1.43.68 with `endpoint_url` for the MinIO/S3 swap-out.
- `.planning/research/SUMMARY.md` — deviation **D2** ("the metadata control plane designed
  coherently up front") and the wave-A parallelization map (Phase 2 ‖ Phase 3, no shared files).

### Phase scope and requirements
- `.planning/ROADMAP.md` § "Phase 3: `dataplat` Core Library & Metadata Control Plane" (lines
  153–182) — goal, five success criteria, and the full plan guidance this CONTEXT.md's decisions
  are grounded in.
- `.planning/ROADMAP.md` § "Phase 4" plan guidance (183–213) — confirms Phase 4 has no CSV-\*
  requirements and depends on what Phase 3 delivers; confirms the frozen-manifest, run-scoped-
  identity and single-writer-publication decisions belong to Phase 4, not this one.
- `.planning/ROADMAP.md` § "Phase 6" plan guidance (244–271) — confirms Phase 6 streaming
  "depends on the record-chunking rule already fixed in Phase 3 (CSV-13)."
- `.planning/REQUIREMENTS.md` — META-01, META-02 (lines 26–28), INFRA-08 (39), SEC-15 (60),
  CSV-13 (88), SCHEMA-07 (98), OBS-02/04/05 (169, 171–172), QUAL-03 (183). Traceability table
  rows confirming all ten map to Phase 3 (279–280, 289, 307, 329, 336, 386, 388–389, 397).
- `.planning/PROJECT.md` § Constraints — logic placement (`csv_processor`/`dataplat` package,
  DAGs orchestrate only), secrets (runtime injection only), determinism (same input + config +
  processor version ⇒ same output). § Key Decisions — "Prometheus + Grafana + OpenTelemetry...
  justified by 'foundation for real work'" (grounds D-03).

### Repository conventions and structural decisions already locked
- `docs/adr/0002-dataplat-core-with-csv-processor-plugin.md` — the `dataplat`/`csv_processor`
  split this phase implements against; locks the naming and import direction.
- `docs/adr/0003-uv-workspace-members-under-packages.md` — workspace shape.
- `docs/adr/0004-two-images-two-dependency-sets.md` — Airflow is never a workspace member;
  the Phase 4 Dockerfile consequence (`--no-install-workspace --frozen` → `--locked`) is recorded
  there for whoever writes `docker/csv-processor/Dockerfile` in this phase.
- `setup.cfg` — the live import-linter contract (`dataplat` forbidden from importing
  `csv_processor`) this phase's code must keep satisfying.
- `pyproject.toml` — `[tool.uv.workspace]` members, the `cluster` dependency group (boto3,
  psycopg — already split out of `dev` specifically so `make check` stays offline; this phase's
  `test-integration` target is the consumer D-04 anticipates), `[tool.coverage.run] source =
  ["dataplat", "csv_processor"]` (already wired, ready for this phase's code).
- `Makefile` lines 87–98 — the `test:` target's own comment mandating D-04.
- `.planning/phases/02-kind-cluster-core-infrastructure/02-CONTEXT.md` — confirms Phase 2 is
  fully hands-off `packages/`, `migrations/`, `configs/`, `schemas/`, `docker/csv-processor/`;
  this phase has a clean slate in all of them.

### Master specification
- `README.md` §68 (package layout — the taxonomy this phase's structure deliberately departs
  from, per ADR-0002 and the roadmap-mandated departure ADR), §6.4 (DAGs orchestrate, logic lives
  in the library), §29/§95 (extensibility to non-CSV sources — the reason `dataplat` must not
  import `csv_processor`), §70 (contextual logging), §71 (error handling), §65/§66 (config as
  data, versioned), §81.5 (documenting any unavoidable secret-bootstrap exception — not triggered
  in this phase, since `SecretsResolver` here only reads `env://`/`file://`, not Vault).

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets

- **`packages/dataplat/src/dataplat/`** — already has `__init__.py` (with the explicit "must
  never import `csv_processor`" docstring) and `version.py` (`resolve_version()` reading
  installed distribution metadata via `importlib.metadata`, never a hardcoded literal — this is
  precisely the `processor_version` mechanism `meta.ingestion_runs` needs). Both are Phase-1
  skeleton; this phase fills in everything else under `dataplat/`.
- **`packages/csv-processor/src/csv_processor/`** — `__init__.py` only, explicitly documented as
  "Phase 1 ships the package marker only." D-01's minimal `Source` is the first real code here.
- **`packages/dataplat/pyproject.toml`** — already `hatchling`-built, already depends on nothing
  but PyYAML. This phase adds `psycopg[binary,pool]`, `alembic`, `structlog`, `pydantic` etc. as
  real dependencies (and `boto3`/`testcontainers` land in dev/cluster groups per the existing
  workspace convention).
- **`setup.cfg`**'s import-linter contract — write code that keeps `imports` (the Makefile
  target at line 84) green from the first commit; do not discover the violation in CI.
- **`Makefile`** — `install-cluster` (line 127) already installs the `cluster` group
  (boto3, psycopg); this phase's testcontainers-based tests are the first real consumer.
  `uv-guard`-style version assertions are the established pattern for any new pinned-tool check
  this phase needs (e.g. asserting the installed Alembic version).
- **`docs/adr/README.md`** and `0000-template.md` — MADR format; next free ADR number is **0008**
  (0001–0007 exist).

### Established Patterns

- **`make` is the sole gate definition**; CI calls `make` and nothing else (locked since Phase
  1). This phase's new targets (migrations, `test-integration`, image build) must join the
  Makefile, not sit beside it.
- **Two Helm-values-style profile separation generalizes to test tiers here**: exactly as
  `values-local.yaml`/`values-ci.yaml` diverge on named axes only (Phase 2, D-06), `make check`
  vs. the new integration target diverge on exactly one axis — Docker/testcontainers dependency
  — not on what they assert.
- **Every pinned tool/version is asserted at runtime, not just documented** (`uv-guard`
  precedent). Alembic, psycopg, structlog versions should follow the same shape if this phase
  adds a version-sensitive behavior.
- **ADRs record structural departures at the moment they're made**, not retroactively (0001–0007
  precedent). The §68-departure ADR belongs in this phase's own commits, not deferred.

### Integration Points

- `migrations/` — currently `.gitkeep` only; this phase's Alembic environment lives here (per
  ARCHITECTURE.md's Recommended Repository Structure, migrations sit at the repo root, not inside
  `dataplat`, because the analytical DB is a platform asset shared by more than the processor).
- `configs/` — currently `.gitkeep` only; `configs/datasets/customers.yaml` (D-02) and
  `configs/defaults.yaml` land here.
- `schemas/` — currently `.gitkeep` only; a generated `dataset-config.schema.json` (from the
  Pydantic `DatasetConfig` model) belongs here per the recommended structure, validated in CI.
- `docker/csv-processor/` — currently `.gitkeep` only; this phase's Dockerfile, built per
  ADR-0004's `--no-install-workspace --frozen` → `--locked` ordering, satisfying success
  criterion 3 (`docker run csv-processor:<git-sha> dataplat --version`).
- `tests/integration/`, `tests/unit/`, `tests/property/` — `unit` and `property` already exist as
  directories (used by other phases' policy/regression tests); `integration/` exists empty,
  waiting for this phase's testcontainers suite per D-04.

</code_context>

<specifics>
## Specific Ideas

- **The Makefile's own Phase-1-authored comment is binding, not just informative** — it explicitly
  names Phase 3 and states what must not be assumed (`make check` does not collect
  `tests/integration`). Treat this the same as a locked decision, not merely a hint.
- **`normalized.customers` and its config file are the one dataset this phase needs to be
  concrete about** — every other dataset-shaped example in research (`transactions` in the
  config-not-code sample) stays illustrative only; this phase does not need to build anything for
  it.
- **The recommended option was taken on every delegated ("Let Claude decide") sub-question** —
  D-01, D-03, D-04, D-05, D-06 all reflect the option presented as "(recommended)" during
  discussion, chosen for the stated reasons rather than arbitrarily.

</specifics>

<deferred>
## Deferred Ideas

- **Whole-`meta`-schema-now migrations** — considered and explicitly rejected in D-05. Revisit
  only if a later phase's migration genuinely fights the incremental approach (e.g., an FK
  inconsistency the up-front design didn't anticipate) — which the design work in
  ARCHITECTURE.md §2 is intended to prevent.
- **Full exception hierarchy now** — considered and explicitly rejected in D-06. Each later phase
  adds its own branches alongside the code that raises them.
- **`vault://` `SecretsResolver` implementation** — explicitly Phase 5's, behind the same
  opaque-reference interface this phase defines the `env://`/`file://` half of.
- **Concrete `Publisher` strategies** (`merge`, SCD/CDC, `partition_replace`, `full_swap`) —
  protocol only in this phase; `merge` arrives in Phase 4, SCD/CDC publishers in Phase 10.
- **Real metrics/tracing backends** — Phase 7. This phase only threads the no-op call sites
  (D-03).
- **Airflow-side `config-sync` DAG** — ARCHITECTURE.md §5.1's scheduled/post-deploy sync job is
  Airflow-side machinery; out of scope until a DAG exists to host it (Phase 4+).

</deferred>

---

*Phase: 3-dataplat-core-library-metadata-control-plane*
*Context gathered: 2026-08-12*
