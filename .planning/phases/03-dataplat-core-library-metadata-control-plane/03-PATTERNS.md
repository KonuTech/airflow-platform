# Phase 3: `dataplat` Core Library & Metadata Control Plane - Pattern Map

**Mapped:** 2026-08-12
**Files analyzed:** 54 (new: 49, modified: 5)
**Analogs found:** 30 / 54 direct repository matches; the remaining 24 have **no** in-repo
precedent (this is the first phase that writes Pydantic models, `typing.Protocol` classes, a
`click` CLI, psycopg/Alembic code, or `structlog` config anywhere in the repository) and must be
built from the session-verified code in `03-RESEARCH.md` / `.planning/research/ARCHITECTURE.md`
instead — each is called out explicitly below, with the exact source lines to copy.

**How to read this document.** This is a greenfield phase: `packages/dataplat/src/dataplat/` and
`packages/csv-processor/src/csv_processor/` currently hold only Phase-1 skeleton files. There is
almost no same-package precedent, so two different kinds of "analog" appear below:

1. **Repository conventions** — the Makefile's target shape, the CI job shape, the ADR format,
   the frozen-dataclass-with-`Attributes:`-docstring style, the "`where`-qualified error message"
   style, the `main(argv) -> int` CLI shape. These are real files, cited with line numbers, and the
   *shape* (not the domain content) is what to copy.
2. **Research-verified code** — `03-RESEARCH.md`'s "Code Examples" section and
   `.planning/research/ARCHITECTURE.md` Q4.3/Q5.2's protocol and hashing shapes were written and,
   in several cases, executed and verified *this session* against the pinned library versions. For
   files with no repository precedent at all (protocols, Pydantic models, the CLI, psycopg/Alembic
   code), these are the closest thing to an analog that exists, and they are quoted in full below
   rather than merely referenced, so this document stays self-contained.

## File Classification

### `dataplat` — errors, models (role: model / utility)

| New File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `packages/dataplat/src/dataplat/errors.py` | utility (exception hierarchy) | transform | `tools/corpus/manifest.py`'s `ManifestError` convention + `tools/security/gitleaks_selftest.py`'s `SelfTestError` | partial |
| `packages/dataplat/src/dataplat/models/identity.py` | model | transform | `tools/corpus/manifest.py`'s frozen dataclasses (`Fixture`, `Splice`) | exact |
| `packages/dataplat/src/dataplat/models/record.py` | model | streaming | `tools/corpus/manifest.py`'s frozen dataclasses | exact |
| `packages/dataplat/src/dataplat/models/report.py` | model | transform | `tools/corpus/manifest.py`'s frozen dataclasses | exact |

### `dataplat/config` (role: config)

| New File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `packages/dataplat/src/dataplat/config/model.py` | config | transform | none — first Pydantic model in the repo | none |
| `packages/dataplat/src/dataplat/config/loader.py` | config | transform | `tools/corpus/manifest.py`'s `load_manifest`/`parse_manifest` (load → validate → freeze) | role-match |
| `packages/dataplat/src/dataplat/config/hashing.py` | utility | transform | `tools/corpus/digests.py`'s `sha256_file` | exact |
| `packages/dataplat/src/dataplat/config/registry.py` | provider + store | CRUD | none — sibling of `metadata/repository.py` built in this same phase | none |

### `dataplat/pipeline`, `dataplat/sources`, `dataplat/load/publish` (role: provider / service)

| New File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `packages/dataplat/src/dataplat/pipeline/protocol.py` | provider (Protocol) | streaming | none — first `typing.Protocol` use in the repo | none |
| `packages/dataplat/src/dataplat/pipeline/engine.py` | service | streaming | none | none |
| `packages/dataplat/src/dataplat/sources/protocol.py` | provider (Protocol) | streaming | none | none |
| `packages/dataplat/src/dataplat/load/publish/protocol.py` | provider (Protocol) | CRUD | none | none |

### `dataplat/storage`, `dataplat/metadata` (role: service / provider / store)

| New File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `packages/dataplat/src/dataplat/storage/objectstore.py` | service | file-I/O | `tests/e2e/cluster/conftest.py`'s `s3_client` boto3 construction | role-match |
| `packages/dataplat/src/dataplat/storage/db.py` | service | CRUD | none | none |
| `packages/dataplat/src/dataplat/metadata/repository.py` | provider (Protocol) | CRUD | none | none |
| `packages/dataplat/src/dataplat/metadata/postgres.py` | store | CRUD | none | none |
| `packages/dataplat/src/dataplat/metadata/fake.py` | store | CRUD | none | none |

### `dataplat/observability`, `dataplat/secrets`, `dataplat/cli.py`

| New File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `packages/dataplat/src/dataplat/observability/logging.py` | utility | event-driven | none | none |
| `packages/dataplat/src/dataplat/observability/metrics.py` | utility | event-driven | none | none |
| `packages/dataplat/src/dataplat/observability/tracing.py` | utility | event-driven | none | none |
| `packages/dataplat/src/dataplat/secrets/resolver.py` | service | request-response | none | none |
| `packages/dataplat/src/dataplat/cli.py` | controller | request-response | `tools/corpus/__main__.py` + `tools/security/gitleaks_selftest.py`'s catch-once shape | role-match |

### `csv_processor` and `migrations`

| New File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `packages/csv-processor/src/csv_processor/source.py` | provider (`Source` impl) | streaming | none in-repo; `03-RESEARCH.md` Code Examples were written specifically for this file | role-match |
| `migrations/env.py` | migration/config | batch | none | none |
| `migrations/script.py.mako` | migration/config (template) | batch | none (Alembic's own default template) | none |
| `migrations/versions/0001_meta_datasets_config_versions.py` | migration | batch | none | none |
| `migrations/versions/0002_meta_files.py` | migration | batch | none | none |
| `migrations/versions/0003_meta_batches_batch_files.py` | migration | batch | none | none |
| `migrations/versions/0004_meta_ingestion_runs.py` | migration | batch | none | none |
| `migrations/versions/0005_normalized_customers.py` | migration | batch | none | none |

### `configs`, `schemas`, `docker`

| New File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `configs/defaults.yaml` | config | transform | `tests/fixtures/corpus.yaml` (committed spec with explanatory header comment) | role-match |
| `configs/datasets/customers.yaml` | config | transform | `tests/fixtures/corpus.yaml` (same convention) | role-match |
| `schemas/dataset-config.schema.json` | config (generated) | transform | none — trivial `DatasetConfig.model_json_schema()` dump | none |
| `docker/csv-processor/Dockerfile` | config | batch | none in-repo; `03-RESEARCH.md` gives the complete, ADR-0004-compliant file | none |

### `tests/unit` (new files)

| New File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `tests/unit/test_secrets_resolver.py` | test | request-response | `tests/unit/test_dataplat_public_api.py` | exact |
| `tests/unit/test_logging_redaction.py` | test | event-driven | `tests/unit/test_dataplat_public_api.py` | exact |
| `tests/unit/test_logging_config.py` | test | event-driven | `tests/unit/test_dataplat_public_api.py` | exact |
| `tests/unit/test_csv_chunking.py` | test | streaming | `tests/unit/test_dataplat_public_api.py` | exact |
| `tests/unit/test_config_hashing.py` | test | transform | `tests/unit/test_dataplat_public_api.py` | exact |
| `tests/unit/test_pipeline_errors.py` | test | streaming | `tests/unit/test_dataplat_public_api.py` | exact |
| `tests/unit/test_cli_error_handling.py` | test | request-response | `tests/unit/test_dataplat_public_api.py` (click adds `CliRunner`) | exact |

### `tests/integration`, `tests/property`, `tests/policy` (new files)

| New File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `tests/integration/__init__.py` | test | n/a | `tests/unit/__init__.py`, `tests/policy/__init__.py` | exact |
| `tests/integration/conftest.py` | test | file-I/O + CRUD | `tests/e2e/cluster/conftest.py` | role-match |
| `tests/integration/test_migrations.py` | test | batch | none directly; shape from `03-RESEARCH.md` Phase Requirements → Test Map | partial |
| `tests/integration/test_config_registry.py` | test | CRUD | none directly | partial |
| `tests/property/__init__.py` | test | n/a | `tests/unit/__init__.py` | exact |
| `tests/property/test_chunking_properties.py` | test | streaming | none — no `hypothesis` usage exists in the repo yet | none |
| `tests/policy/test_no_latest_image_tag.py` | test | batch | `tests/policy/test_supply_chain_guards.py`'s `MUTABLE_TAG_VALUES` section | exact |

### Modified files

| Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `Makefile` (new `test-integration` target, `install-cluster`-style docker-build/migrate targets) | config | batch | `cluster-verify` (154–166), `install-cluster` (127–130), `manifest-policy` (237–255) — same file | exact |
| `pyproject.toml` (root — `cluster` group comment/scope, `dev` group additions) | config | n/a | same file, lines 40–52 (the comment this phase makes literally false) | exact |
| `packages/dataplat/pyproject.toml` (real runtime deps + `[project.scripts]`) | config | n/a | `packages/csv-processor/pyproject.toml`'s `dependencies = ["dataplat"]` shape | exact |
| `.github/workflows/ci.yml` (new `integration` job) | config | batch | the `manifests` job (64–76) — same file | exact |
| `docs/README.md` (directory-map row touch-ups, e.g. `docker/csv-processor/` says "Phase 4" today) | config | n/a | its own existing table rows (lines 13–21) | exact |

**New file:**

| New File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `docs/adr/0008-<slug>.md` (the §68/pipeline-seam departure ADR the roadmap requires) | config (doc) | n/a | `docs/adr/0002-...md`, `docs/adr/0004-...md`, `docs/adr/0000-template.md` | exact |

---

## Pattern Assignments

### Cluster A — `errors.py` (utility, transform)

**Analog:** `tools/corpus/manifest.py` lines 121–126 (custom exception) and its `where`-qualified
message convention throughout; `tools/security/gitleaks_selftest.py` lines 82–84 and 380–399
(catch-once-in-`main`, map to exit code).

**Repository convention to copy** — one narrow exception subclassing the nearest builtin, raised
via a `msg = f"..."` local then `raise Error(msg)` (never `raise Error(f"...")` inline — this repo
lints with `select = ["ALL"]`, which includes `EM101`/`EM102`):

```python
# tools/corpus/manifest.py:121-126
class ManifestError(ValueError):
    """A manifest is malformed, self-contradictory or unsupported.

    Raised at load time so that a corrupt manifest fails loudly instead of
    producing a corrupt corpus.
    """
```

**Catch-exactly-once shape to copy** (this is D-06's literal requirement — "caught exactly once in
`cli.py`"):

```python
# tools/security/gitleaks_selftest.py:380-399
def main(argv: list[str]) -> int:
    ...
    try:
        selftest(binary)
    except SelfTestError as error:
        _LOG.error("SEC-11 self-test FAILED: %s", error)
        return 1

    _LOG.info("SEC-11 self-test passed: the scanner is live and its allowlists are scoped")
    return 0
```

**What is genuinely new (no repo precedent): the hierarchy shape itself.** Per CONTEXT.md D-06,
build **only**:

```
DataPlatformError                  # base; carries context: dict populated from PipelineContext
├── ConfigurationError
├── StorageError
└── SecretResolutionError
```

— not the full `ARCHITECTURE.md` §4.5 tree (`SourceError`, `SchemaError`,
`QualityThresholdExceeded`, `PublicationError` are later phases' branches; a subclass with no raise
site is dead code per D-06's own reasoning). Source: `ARCHITECTURE.md` lines 547–567 for the full
tree this phase's slice is carved from; CONTEXT.md D-06 (lines 144–157) for the carve-out.

---

### Cluster B — `models/identity.py`, `models/record.py`, `models/report.py` (model, transform/streaming)

**Analog:** `tools/corpus/manifest.py` — every model in that file is `@dataclass(frozen=True,
slots=True)` with a Google-style docstring carrying an `Attributes:` section, one line per field:

```python
# tools/corpus/manifest.py:250-282 (Fixture — the richest example; also see Splice at 206-230)
@dataclass(frozen=True, slots=True)
class Fixture:
    """One declared fixture: how to build it and what it is expected to mean.

    Attributes:
        name: File name written under the corpus output directory.
        generator: Which generator kind builds it.
        covers: Requirement IDs this fixture exercises.
        ...
    """
    name: str
    generator: GeneratorKind
    covers: tuple[str, ...]
    ...
```

Apply this exact shape to `RecordChunk`, `RejectedRecord`, `StageResult` (models/record.py),
`DatasetRef`/`FileIdentity`/`BatchIdentity`/`RunContext` (models/identity.py) and
`ValidationResult` (models/report.py). CONTEXT.md's Claude's-Discretion item ("whether
`RejectedRecord` ... use `@dataclass(slots=True, frozen=True)`") is answered by this convention:
yes, it is already how every frozen value object in this repository is written.

**Field content shape** comes from `ARCHITECTURE.md` lines 463–502 (`RecordChunk`, `StageResult`
inside `PipelineContext`) and `ARCHITECTURE.md` lines 429–446 (RESEARCH.md's `RejectedRecord`
fields: `source_row_number`, `error_type`, `error_message`, `raw_line`).

**Note on mutability:** `StageResult` in `ARCHITECTURE.md`'s own listing (line 488) is `@dataclass`
without `frozen=True` — it accumulates `rejected`/`findings` lists during a stage. Follow the
`manifest.py` convention (`frozen=True, slots=True`) for `RecordChunk`, `RejectedRecord` and the
identity/report models, which are genuinely immutable value objects; `StageResult` is the one
model in this cluster that is legitimately a mutable-until-returned builder, matching
`ARCHITECTURE.md`'s own declaration.

---

### Cluster C — `config/hashing.py` (utility, transform)

**Analog:** `tools/corpus/digests.py` lines 37–50 — chunked, memory-bounded SHA-256:

```python
# tools/corpus/digests.py:37-50
def sha256_file(path: Path) -> str:
    """Hash a file without reading it into memory.
    ...
    """
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_READ_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()
```

This is directly reusable for `meta.files.content_sha256` (hash the object body as it streams
through, per PITFALLS.md C5 point 4 — never trust the S3 `ETag`).

**Config-hash canonicalization** has no repo precedent; copy verbatim from `ARCHITECTURE.md`
lines 610–617:

```python
# ARCHITECTURE.md:610-617 — the canonical-JSON hash recipe for meta.config_versions.config_hash
resolved = DatasetConfig.model_validate(merge(DEFAULTS, raw_yaml))
canonical = json.dumps(resolved.model_dump(mode="json"),
                       sort_keys=True, separators=(",", ":"), ensure_ascii=False)
config_hash = hashlib.sha256(canonical.encode()).hexdigest()
```

**Design rules that are load-bearing and easy to violate accidentally** (`PITFALLS.md` lines
1049–1072, cited directly because `config/hashing.py` and `_record_hash` are the first hash
recipes this project ships and the ones every later phase's hashing will be compared against):
- Store a `hash_version` column beside every hash from the first row (`PITFALLS.md` #1, line 1049).
- Never concatenate fields with a separator that could appear in a value; length-prefix or hash a
  canonical JSON encoding instead (line 1053).
- The column set/order is the **contract's** attribute list, never `df.columns`/`row.keys()`
  (line 1057).
- NULL must not equal empty string (line 1061).
- Never hash a `float` — hash the normalized `Decimal` string (line 1063).
- Compute the hash in exactly one place, Python, never recomputed in SQL (line 1066).

---

### Cluster D — `config/loader.py` (config, transform)

**Analog:** `tools/corpus/manifest.py`'s `load_manifest`/`parse_manifest` pair (lines 339–421) —
the load → validate → build-frozen-result shape, and specifically its `where`-threading convention
for error messages (every validation helper takes a `where: str` context string and prefixes every
raised message with it, e.g. `_require_str`, lines 1113–1119):

```python
# tools/corpus/manifest.py:1113-1119
def _require_str(data: Mapping[str, Any], key: str, where: str) -> str:
    """Require a string field."""
    value = data.get(key)
    if not isinstance(value, str):
        msg = f"{where}: {key} must be a string, got {type(value).__name__}"
        raise ManifestError(msg)
    return value
```

`config/loader.py` will use Pydantic (`DatasetConfig.model_validate`) rather than hand-rolled
`_require_*` helpers — Pydantic's own `ValidationError` already names the offending field — but the
outer shape (`load_config(path) -> DatasetConfig`, raising `ConfigurationError` with the path and
field context wrapped around Pydantic's error) should read the same way `load_manifest` does: fail
loudly, name the file, name the field. Merge-with-defaults resolution is `ARCHITECTURE.md` lines
592–594 (`configs/datasets/*.yaml` merged over `configs/defaults.yaml` before validation).

---

### Cluster E — `config/model.py` (config, transform) — no repo analog

**No analog found.** This is the first Pydantic model anywhere in this repository. Use the exact
shape `.claude/CLAUDE.md`'s "Data modelling / validation" table already specifies (this is
authoritative, not a suggestion):

```python
model_config = ConfigDict(extra="forbid", frozen=True)
```

`extra="forbid"` catches config typos (the single most common ETL outage cause, per CLAUDE.md);
`frozen=True` supports the determinism constraint. The field shape is `ARCHITECTURE.md`'s
`configs/datasets/transactions.yaml` example (lines 525–541) reflected into a model — `dataset`,
`config_schema_version`, `source` (nested: `type`, `bucket`, `path`, `change_semantics`),
`deduplication` (nested: `strategy`, `keys`, `order_by`), `load` (nested: `strategy`, `target`).
Strategy/source/publisher fields are resolved through **string-keyed registries**
(`ARCHITECTURE.md` line 523), not enums baked into the model — that indirection is what makes
"config not code" (§65) literally true.

---

### Cluster F — `config/registry.py`, `metadata/repository.py`, `metadata/postgres.py`, `metadata/fake.py` — no repo analog

**No analog found** — no protocol-plus-implementation pair exists anywhere in the repo yet. These
four files are internally cross-referential (built in the same phase, same shape) rather than
each having an independent external analog:

- `metadata/repository.py` defines the `MetadataRepository` `Protocol` — see Cluster G below for
  the `Protocol` convention itself (`ARCHITECTURE.md` Q4.3).
- `metadata/postgres.py` is the psycopg-backed implementation; its SQL shape (INSERT/UPDATE
  patterns for `meta.datasets`, `meta.files`, `meta.batches`, `meta.ingestion_runs`) comes directly
  from the column designs in `ARCHITECTURE.md` §2.1 (lines 141–227) — see Cluster K for the
  matching migration DDL, which is the same table shape from the other side.
- `config/registry.py` is `ConfigRegistry` — the Postgres-backed system of record for
  `meta.config_versions`, per `ARCHITECTURE.md` §5.1 (lines 587–606): hash matches current version
  → no-op; hash differs → close the old row (`valid_to = now()`) and insert `version = max+1`.
  This is the one piece of `config-sync` this phase builds (the library-side registry, not the
  Airflow DAG — CONTEXT.md D-02's explicit boundary).
- `metadata/fake.py` (Claude's Discretion item) is an in-memory implementation of the exact same
  `MetadataRepository` protocol, for fast unit tests. No repo precedent for a fake-vs-real pair
  exists; the only structural requirement is that `fake.py` and `postgres.py` implement literally
  the same `Protocol`, so a test written against one can run against the other unmodified — verify
  this the same way `tests/policy/test_pinned_tool_versions_agree.py` proves "every source is
  load-bearing" (lines 253–268): a test that only ever exercises `fake.py` and never `postgres.py`
  against the same assertions is a fake that has drifted unnoticed.

---

### Cluster G — `pipeline/protocol.py`, `sources/protocol.py`, `load/publish/protocol.py` — no repo analog

**No analog found** — zero `typing.Protocol` usage anywhere in this repository today (confirmed by
`grep -rl "typing.Protocol"` returning nothing). Copy these verbatim from `ARCHITECTURE.md` Q4.3
(lines 463–514) — they are this phase's single most load-bearing pieces of code, and the roadmap
explicitly says the ADR recording this seam (Cluster T below) must be written in the same commits:

```python
# ARCHITECTURE.md:465-474 — dataplat/sources/protocol.py
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
# ARCHITECTURE.md:478-502 — dataplat/pipeline/protocol.py
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

```python
# ARCHITECTURE.md:508-514 — dataplat/load/publish/protocol.py
class Publisher(Protocol):
    name: str
    def publish(self, ctx: PipelineContext, staging_table: str,
                conn: Connection) -> PublishResult: ...
    # receives an OPEN transaction — the engine owns the transaction boundary,
    # so watermark + run-status updates commit with the data (Q3, §28)
```

**This phase defines the `Publisher` protocol only — no concrete strategy.** `merge` arrives in
Phase 4, SCD/CDC publishers in Phase 10 (CONTEXT.md, deferred section, and ROADMAP Phase 3 plan
guidance line 175).

---

### Cluster H — `pipeline/engine.py` — no repo analog, but a concrete Pattern-3 template exists

**No analog found** for the sequencing/checkpoint loop itself. The errors-as-values shape it must
implement (QUAL-03) has a concrete, ready-to-adapt template in `03-RESEARCH.md` lines 430–446
(sharper than `ARCHITECTURE.md`'s generic version at lines 1289–1300 because it already matches
D-01's ragged-row handling):

```python
# 03-RESEARCH.md:430-446 — Pattern 3, adapted for this phase's minimal Source
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

**The chunking/checkpoint loop itself** — record-ordinal chunking, never byte offsets, over a
`newline=""` text stream — is `03-RESEARCH.md` lines 606–620 (see Cluster L below; the same
function is the natural home for either `pipeline/engine.py` or `csv_processor/source.py`
depending on how the plan splits generic-chunking from CSV-specific chunking).

---

### Cluster I — `storage/objectstore.py` (service, file-I/O)

**Analog:** `tests/e2e/cluster/conftest.py` lines 182–229 — the only place in the repository a
boto3 S3 client is already constructed against MinIO, including the path-style-addressing
workaround this project's MinIO ingress needs:

```python
# tests/e2e/cluster/conftest.py:220-227
return boto3.client(
    "s3",
    endpoint_url=endpoint_url,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name="us-east-1",
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)
```

That file is a **test fixture** (live-cluster, session-scoped), not library code, so
`storage/objectstore.py` should not import it — but the `boto3.client("s3", endpoint_url=...,
config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))` construction is exactly
what `ObjectStore`'s real implementation needs, parameterized from `SecretsResolver`-resolved
credentials instead of `scripts/minio-credentials.sh`.

**The StreamingBody → text-stream bridge** has no repo precedent and is fully verified this
session — copy verbatim from `03-RESEARCH.md` lines 575–594:

```python
# 03-RESEARCH.md:578-594
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

**Do not** write the `io.RawIOBase` adapter class `PITFALLS.md` E1 originally called for — it is
dead code against the pinned boto3 1.43.68 (verified by source inspection and an executable
round-trip test this session; see `03-RESEARCH.md`'s "State of the Art" table, lines 780–782).

---

### Cluster J — `storage/db.py` — no repo analog

**No analog found.** No psycopg code exists in the repository yet. Follow `STACK.md`'s explicit
guidance (already locked, not re-litigated): `psycopg_pool.ConnectionPool(conninfo, min_size=1,
max_size=2, open=False)` with an explicit `.open()`/`.wait()` — flagged `03-RESEARCH.md` Assumption
A5 as needing a live-docs re-check at implementation time (the psycopg docs page 403'd mid-session).
Keep this file's connection factory **completely separate** from `migrations/env.py`'s SQLAlchemy
engine — see Cluster K; `03-RESEARCH.md` Pitfall 4 (lines 549–562) names this exact confusion as a
likely mistake.

---

### Cluster K — `migrations/env.py` and `migrations/versions/000{1..5}_*.py` (migration, batch)

**No analog found** in the repository (no Alembic environment exists yet), but both files have a
complete, session-reasoned template.

**`migrations/env.py`** — the "wrong database" guard (`03-RESEARCH.md` lines 389–418, Pattern 2),
with the expected values already confirmed live against Phase 2's cluster
(`helm/values/local/cnpg-analytics.yaml` lines 60–68: database `analytics`, role `etl_app`, no
password/grants yet):

```python
# 03-RESEARCH.md:396-418
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

**`migrations/versions/0004_meta_ingestion_runs.py`** — the deferred-FK pattern (`03-RESEARCH.md`
lines 355–378, Pattern 1) — every column lands now, but `schema_version_id` stays nullable and
unconstrained because `meta.schema_versions` is a later phase's table:

```python
# 03-RESEARCH.md:355-378
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
        # phase's migration.
        sa.Column("schema_version_id", sa.BigInteger, nullable=True),
        sa.Column("processor_version", sa.Text, nullable=False),
        sa.Column("status", sa.Text, nullable=False),
        # ... remaining columns per ARCHITECTURE.md §2.1
        schema="meta",
    )
    op.execute("GRANT SELECT, INSERT, UPDATE ON meta.ingestion_runs TO etl_app")
```

Every migration in this cluster needs the matching `GRANT ... TO etl_app` line — there is no other
place in the codebase this grant can come from (`03-RESEARCH.md` finding 2, lines 33–41: Phase 2's
`postInitApplicationSQL` is literally `CREATE ROLE etl_app LOGIN;` with nothing else).

**Column designs for 0001–0003** come from `ARCHITECTURE.md` §2.1 verbatim:
- `meta.datasets` — lines 141–149 (`dataset_id` PK, `dataset_name UNIQUE NOT NULL`,
  `source_system`, `description`, `is_active`, `created_at`/`updated_at`).
- `meta.config_versions` — lines 151–163 (`config_hash`, `config_document jsonb`,
  `config_schema_version`, `valid_from`/`valid_to`, three uniqueness constraints).
- `meta.files` — lines 165–182 (`content_sha256 bytea NOT NULL` — "the real file identity";
  `duplicate_of_file_id`; `UNIQUE(dataset_id, object_uri, content_sha256)`).
- `meta.batches` + `meta.batch_files` — lines 184–201 (include `batches` even though the slice is
  one-file-one-batch — "it is one table now; adding a `NOT NULL` FK to populated tables across
  three later phases is not").

**`0005_normalized_customers.py`** additionally needs the six embedded lineage columns verbatim
from `ARCHITECTURE.md` lines 253–261:

```sql
-- ARCHITECTURE.md:255-261 — every table in normalized.* and warehouse.* carries:
_run_id            bigint      NOT NULL REFERENCES meta.ingestion_runs,
_file_id           bigint      NOT NULL REFERENCES meta.files,
_batch_id          bigint      NOT NULL REFERENCES meta.batches,
_source_row_number bigint      NOT NULL,
_record_hash       bytea       NOT NULL,
_ingested_at       timestamptz NOT NULL DEFAULT now()
```

Plus, per `03-RESEARCH.md` Common Pitfall 3 / Assumption A2 (lines 533–547, 795), a companion
`_record_hash_version smallint NOT NULL DEFAULT 1` — D-05's text names only `files.content_sha256`
and `config_versions.config_hash` explicitly, but `_record_hash` is unambiguously "a stored hash"
in PITFALLS.md #1/C6's general sense and this is the only phase that mints it. Flagged here as a
genuine extension of a locked decision, not a silent one — confirm with the planner/user rather
than deciding unilaterally.

---

### Cluster L — `csv_processor/source.py` (provider, streaming)

**No same-package analog** (the package is a marker file today), but `03-RESEARCH.md` wrote and
verified this file's core loop this session (executable round-trip test at chunk sizes 1, 2, 3
against an embedded-newline fixture):

```python
# 03-RESEARCH.md:606-620
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

Combine with Cluster I's `open_text_stream` to implement the `Source`/`RecordStream` protocol from
Cluster G. Hardcode UTF-8, comma delimiter, header row 0, per CONTEXT.md D-01 — no detection logic
belongs here (that is Phase 6's `csv_processor/detect/`). Pre-filter NUL bytes before the reader
(cpython #71767) and treat ragged rows as `RejectedRecord`s via Cluster H's pattern, never
pad/truncate (polars #10585).

---

### Cluster M — `observability/logging.py` (utility, event-driven)

**No analog found** — `structlog` is not used anywhere in the repo yet. Copy verbatim from
`03-RESEARCH.md` lines 667–695 (verified against current structlog docs this session):

```python
# 03-RESEARCH.md:669-695
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
```

**Connects to an existing repo-wide rule:** `print()` is already banned by ruff's `T20` selection
(`pyproject.toml`'s `select = ["ALL"]`, carved out only for `scripts/**` and
`tools/corpus/__main__.py` — see `pyproject.toml` lines 46–48). `structlog.PrintLoggerFactory()`
above is not a violation (it is `structlog`'s own sink, not a bare `print()` call), but any
temptation to `print()` for debugging inside `dataplat`/`csv_processor` will already fail `make
lint` — this repo has no carve-out for the new package.

`observability/metrics.py` and `observability/tracing.py` have no analog and no code to copy — per
CONTEXT.md D-03 they are no-op functions with the real call-site signatures Phase 7 will need
(e.g. `metrics.increment("rows_loaded", n)` doing nothing), threaded through `pipeline/engine.py`
and `csv_processor/source.py` from the first commit.

---

### Cluster N — `secrets/resolver.py` (service, request-response)

**No analog found.** Copy verbatim from `03-RESEARCH.md` lines 711–733:

```python
# 03-RESEARCH.md:714-733
def resolve_secret(ref: str) -> str:
    """Resolve an opaque secret reference. ..."""
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

Note `SecretResolutionError` should end up defined in `dataplat.errors` (Cluster A), not locally in
this module, once `errors.py` exists — the Code Example defines it locally because it was written
standalone; the real file should `from dataplat.errors import SecretResolutionError`. Any
unrecognized scheme (including `vault://`, which is Phase 5's) must raise, never silently pass the
raw string through — this is SEC-15's literal requirement and is already reflected above.

---

### Cluster O — `cli.py` (controller, request-response)

**Analog for the overall shape** (parse → dispatch → catch domain error once → exit code):
`tools/corpus/__main__.py` lines 151–163 and 166–168:

```python
# tools/corpus/__main__.py:151-167
def main(argv: list[str] | None = None) -> int:
    """Run the corpus CLI. ..."""
    args = build_parser().parse_args(argv)
    if args.command == "generate":
        return command_generate(args)
    return command_verify(args)


if __name__ == "__main__":
    sys.exit(main())
```

`tools/security/gitleaks_selftest.py` lines 380–399 (already quoted in Cluster A) is the
closer analog for the **error-handling half** specifically — `try: ... except <DomainError>: log,
return 1`.

**What differs:** `03-RESEARCH.md` recommends `click` over `argparse` for `dataplat.cli` (Standard
Stack, lines 137–150) — this is a genuinely new library for the repo (zero dependencies of its own,
unlike `typer`), justified by the CLI's growth path (`--version` now, `ingest --assignment <uri>`
in Phase 4, `replay` later). Structure as a `click.group()` with subcommands, catching
`DataPlatformError` once at the group's invocation boundary (Click's own `except` hook or a thin
wrapper around `main`), writing `error_type`/`error_message`/`error_detail` to
`meta.ingestion_runs` per D-06, and exiting non-zero. `[project.scripts] dataplat =
"dataplat.cli:main"` — see Cluster S. Unit-test with `click.testing.CliRunner`, per
`03-RESEARCH.md`'s Phase Requirements → Test Map row for `test_cli_error_handling.py` (line 878).

---

### Cluster P — `configs/defaults.yaml`, `configs/datasets/customers.yaml` (config, transform)

**Analog:** `tests/fixtures/corpus.yaml` — this repository's established convention for a
committed, human-authored YAML specification: a long header comment block explaining what the file
is, why it is committed, and what invariant depends on it (`tests/fixtures/corpus.yaml` lines
1–15):

```yaml
# tests/fixtures/corpus.yaml:1-9
# tests/fixtures/corpus.yaml — COMMITTED. The only specification of the corpus.
#
# The corpus itself is generated, never committed (QUAL-08, ROADMAP criterion 2).
# Two files are tracked: this manifest, which states what every fixture *means*,
# and CORPUS.sha256, which is the oracle proving the bytes have not drifted.
#
# `make fixtures` materialises the corpus and rewrites the oracle.
# `make fixtures-verify` regenerates into a temporary directory and only reads
# the oracle. That asymmetry is what makes a generator change a reviewable diff.
```

Apply the same "explain the file's role and what depends on it" convention to
`configs/defaults.yaml` and `configs/datasets/customers.yaml`. Content shape (not style) comes
from `ARCHITECTURE.md`'s worked `transactions.yaml` example, lines 526–541, adapted to
`normalized.customers`' shape (CONTEXT.md D-02: `customer_id`, `name`, `country`, `birth_date`,
`event_ts`; `load.strategy: merge`; `load.target: normalized.customers`).

---

### Cluster Q — `docker/csv-processor/Dockerfile` (config, batch)

**No repository analog** — no Dockerfile exists anywhere in the repo yet
(`docker/csv-processor/.gitkeep` and `docker/airflow/.gitkeep` are both still empty). `03-RESEARCH.md`
lines 738–770 gives the complete file, already ordered per ADR-0004's requirement
(`--no-install-workspace --frozen` for the dependency-only layer, then `--locked` once member
sources are copied):

```dockerfile
# 03-RESEARCH.md:741-770 (verbatim — copy this file directly, see docs/adr/0004 for why the
# --frozen -> --locked ordering matters and must not be collapsed)
FROM ghcr.io/astral-sh/uv:0.12.3 AS uv
FROM python:3.12-slim-bookworm AS builder
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

COPY pyproject.toml uv.lock ./
COPY packages/dataplat/pyproject.toml packages/dataplat/pyproject.toml
COPY packages/csv-processor/pyproject.toml packages/csv-processor/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-workspace --no-dev

COPY packages/ packages/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

FROM python:3.12-slim-bookworm AS runtime
RUN groupadd -r app -g 1000 && useradd -r -g app -u 1000 -m app
COPY --from=builder --chown=app:app /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
USER 1000
ENTRYPOINT ["dataplat"]
```

`docs/README.md` line 21 currently says `docker/csv-processor/` arrives "Phase 4" — this phase's
own success criterion 3 (`docker run csv-processor:<git-sha> dataplat --version`) contradicts that;
the planner should fix that one table cell alongside adding the Dockerfile.

---

### Cluster R — `tests/integration/conftest.py` (test, file-I/O + CRUD)

**Analog:** `tests/e2e/cluster/conftest.py` — the repo's only existing "shared fixtures for a
gated test tier" file. Copy its conventions, not its content: session-scoped fixtures,
`REPO_ROOT` resolved once via `Path(__file__).resolve().parents[N]` (line 47), an `autouse=True`
session fixture that skips the whole directory with a named reason when the dependency is absent
(lines 79–102), and helper fixtures that build a client from freshly-resolved config rather than
caching a global (lines 182–229).

**The testcontainers fixtures themselves** are `03-RESEARCH.md` lines 628–659, verified against
the `testcontainers-python` docs this session:

```python
# 03-RESEARCH.md:632-658
@pytest.fixture(scope="session")
def postgres_dsn() -> str:
    with PostgresContainer("postgres:18-bookworm", driver="psycopg") as pg:
        sqlalchemy_url = pg.get_connection_url()
        yield sqlalchemy_url.replace("postgresql+psycopg://", "postgresql://")

@pytest.fixture(scope="session")
def minio_config() -> dict[str, str]:
    with MinioContainer() as minio:
        # get_client() returns the forbidden `minio` SDK client — use get_config()
        # and build a boto3 client instead, exactly as dataplat's own ObjectStore does.
        yield minio.get_config()

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

Postgres version: `postgres:18-bookworm` above is the **analytical** database's major
(CLAUDE.md — analytical is PG 18, Airflow metadata is capped at PG 17); do not accidentally pin 17
here.

---

### Cluster S — Makefile, root `pyproject.toml`, `packages/dataplat/pyproject.toml`, `.github/workflows/ci.yml` (modified, config/batch)

**Makefile — new `test-integration` target.** Analog: `cluster-verify` (lines 154–166), which is
the existing template for "a Docker/cluster-gated target, excluded from `check`/`ci`, using a
dedicated dependency group":

```makefile
# Makefile:154-166 — cluster-verify, the shape to mirror for test-integration
cluster-verify:                 ## D-16: run tests/e2e/cluster against the live cluster [plan 02-02]
	# $(RUN_CLUSTER), NOT $(RUN): boto3/psycopg live in the `cluster` group...
	$(RUN_CLUSTER) pytest tests/e2e/cluster -q
```

`03-RESEARCH.md`'s Wave 0 Gaps already names the exact new target line (line 898):
`$(RUN_CLUSTER) pytest tests/integration -q`. Add `test-integration` to `.PHONY` (line 45–48) and
run it as its own CI job (not folded into `check`/`ci`), per D-04 and per the Makefile's own
Phase-1-authored binding comment at lines 94–97.

**Root `pyproject.toml` — the `cluster` group comment is about to become false.** The exact text
that needs updating is lines 40–48 (quoted in full in the File Classification table above); its
literal claim — "either would put boto3 into the environment `make check`'s offline gate builds" —
becomes false the moment `packages/dataplat/pyproject.toml` lists `boto3`/`psycopg` as real
`[project.dependencies]`, which this phase must do. `03-RESEARCH.md` Pitfall 1 (lines 483–510) is
explicit about the resolution: the `cluster` group's surviving purpose narrows to gating
`testcontainers[postgres,minio]` specifically (the Docker-orchestrating dependency `make check`
must still never need), not to gating `boto3`/`psycopg` importability.

**A discrepancy inside `03-RESEARCH.md` itself, worth flagging to the planner directly:** its own
"Installation" code block (lines 176–179) shows `uv add --dev "testcontainers[...]"
"hypothesis>=6.165,<7" "boto3-stubs[s3]>=1.43,<2"` — i.e., testcontainers into `dev`, not
`cluster`. That contradicts Pitfall 1's reasoning two pages later in the same document. Pitfall 1's
argument is the more carefully worked-through one and is what D-04 actually needs (`make
check`/`dev` staying Docker-orchestration-free); recommend `testcontainers[postgres,minio]` joins
the **`cluster`** group, not `dev`. Separately, `hypothesis>=6.165,<7` is **already present** in
`dev` (`pyproject.toml` line 33) — nothing to add there. `alembic`/`sqlalchemy` and
`boto3-stubs[s3]` are the genuinely new `dev`-group additions (migration/typing tooling, never
shipped in the image, per `03-RESEARCH.md` Anti-Patterns, lines 452–455).

**`packages/dataplat/pyproject.toml` — real runtime deps.** Analog:
`packages/csv-processor/pyproject.toml`'s `dependencies = ["dataplat"]` (already the pattern for
"this package's dependency list is genuinely small and explicit"). Exact additions from
`03-RESEARCH.md` line 172–175:

```bash
uv add --package dataplat "psycopg[binary,pool]>=3.3.4,<4" "boto3>=1.43.68,<2" \
    "pydantic>=2.13,<3" "structlog>=26,<27" "click>=8.4,<9"
```

Plus the `[project.scripts]` table from `03-RESEARCH.md` lines 773–776:

```toml
[project.scripts]
dataplat = "dataplat.cli:main"
```

**`.github/workflows/ci.yml` — new `integration` job.** Analog: the `manifests` job (lines 64–76)
— checkout (pinned SHA) → `astral-sh/setup-uv` (pinned SHA) → `make install` → one `make <target>`
line, with a comment block above the job explaining which requirement it proves and why it is a
separate job rather than folded into `check`:

```yaml
# .github/workflows/ci.yml:64-76 — shape to mirror (swap manifest-policy for test-integration)
manifests:
  name: Manifest validation (offline of any cluster)
  runs-on: ubuntu-latest
  timeout-minutes: 15
  steps:
    - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
    - uses: astral-sh/setup-uv@c771a70e6277c0a99b617c7a806ffedaca235ff9 # v9.0.0
      with:
        version: ${{ env.UV_VERSION }}
        enable-cache: true
        cache-dependency-glob: "uv.lock"
    - run: make install
    - run: make manifest-policy
```

Docker is available by default on `ubuntu-latest` runners — `03-RESEARCH.md`'s Wave 0 Gaps (line
901) confirms no `docker:dind` service is needed for the new `integration` job.

---

### Cluster T — `docs/adr/0008-<slug>.md` (new)

**Analog:** `docs/adr/0002-dataplat-core-with-csv-processor-plugin.md` (full file) and
`docs/adr/0004-two-images-two-dependency-sets.md` (full file), both MADR format per
`docs/adr/0000-template.md`:

```markdown
# docs/adr/0000-template.md:1-30 (the structure every ADR in this repo follows)
---
status: {proposed | accepted | rejected | deprecated | superseded by ADR-00NN}
date: YYYY-MM-DD
---

# ADR-00NN: {short title, a decision phrased as a claim}

## Context and Problem Statement
{What forces are in play? Which README section or research finding does this touch?}

## Considered Options
* Option A
* Option B

## Decision Outcome
Chosen option: "{A}", because {justification}.

### Consequences
* Good, because …
* Bad, because …
* Neutral, because …

## Migration trigger
{What observable event would make us revisit this? "None — this is permanent" is a
valid answer and must be written explicitly rather than left blank.}

## References
* README §NN
* .planning/research/{FILE}.md §{section}
```

**Content:** this ADR is *not* a restatement of ADR-0002 (which already settled the
`dataplat`/`csv_processor` package split). It records the specific fact ROADMAP's Phase 3 plan
guidance calls out (line 175): "README §68's proposed package layout does not contain this seam"
— i.e., the `Source → RecordChunk → Publisher` composition protocol plus `Stage` and
`MetadataRepository` (Cluster G) is a structural addition beyond what §68 or ADR-0002 describe, and
recording it now prevents it being re-litigated at Phase 10 when concrete `Publisher` strategies
arrive. Next free number is **0008** (0001–0007 exist — confirmed via `docs/adr/README.md`).
Exact wording and number are explicitly left to Claude's discretion by CONTEXT.md.

---

### Cluster U — `tests/policy/test_no_latest_image_tag.py` (test, batch)

**Analog:** `tests/policy/test_supply_chain_guards.py`'s image-pin-agreement section (lines
314–472) is the closest thing to a direct precedent in the whole repository — it already
implements "no values file selects a mutable tag" for Helm-chart image tags:

```python
# tests/policy/test_supply_chain_guards.py:323, 423-438
MUTABLE_TAG_VALUES = frozenset({"", "latest", "main", "master", "edge", "nightly"})

def mutable_tag_problems(doc: dict[str, Any], label: str) -> list[str]:
    """Report any `tag`-shaped leaf holding a mutable value, anywhere in `doc`."""
    problems: list[str] = []
    for path, value in _flatten_for_image_scan(doc).items():
        leaf = path.rsplit(".", 1)[-1]
        if leaf.lower() != "tag" and leaf != "defaultAirflowTag":
            continue
        if isinstance(value, str) and value.strip().lower() in MUTABLE_TAG_VALUES:
            problems.append(f"{label}: {path} selects a mutable tag ({value!r})")
    return problems
```

INFRA-08's requirement is narrower and more specific than the Helm-chart case above: the
csv-processor **image tag** the build/CI recipe assigns must be `git rev-parse --short HEAD` (or
equivalent), never `latest`. This is a Makefile/CI-recipe scan (grep the docker-build recipe for a
hardcoded `:latest` and confirm it instead interpolates a git-SHA variable), closer in shape to
`tests/policy/test_pinned_tool_versions_agree.py`'s Makefile-recipe-body regex checks (e.g.
`test_the_makefile_scanner_target_defers_to_the_pinned_installer`, lines 233–250) than to the YAML
tree-walk above. Combine both idioms: reuse `MUTABLE_TAG_VALUES`-style literal matching against
whatever the build recipe hardcodes, plus a positive assertion that a git-SHA-producing expression
(`git rev-parse --short HEAD` or `$(GIT_SHA)`) appears in the same recipe.

---

## Shared Patterns

### Module docstrings explain WHY, not just WHAT
**Source:** every file read for this map (`tools/corpus/manifest.py` lines 1–27,
`tools/corpus/digests.py` lines 1–15, `dataplat/version.py` lines 1–6,
`tools/security/gitleaks_selftest.py` lines 1–41). **Apply to:** every new file in this phase.
The opening docstring names the requirement ID(s) it satisfies, the failure mode it prevents, and
any honest limitation, before any code appears. `dataplat/version.py` lines 1–6 is the shortest
good example already inside the package this phase extends:
```python
"""Version resolution for the installed ``dataplat`` distribution.

Every loaded row must be attributable to the processor version that produced it,
so the version is read from installed distribution metadata rather than being
duplicated as a module-level literal that can drift from ``pyproject.toml``.
"""
```

### `from __future__ import annotations` on every module
**Source:** universal across the repository (`dataplat/__init__.py` line 8, `dataplat/version.py`
line 8, every `tools/` module). **Apply to:** all 22 new `dataplat`/`csv_processor` files.

### Frozen dataclasses with an `Attributes:` docstring section
**Source:** `tools/corpus/manifest.py` (the only `@dataclass(frozen=True` usage in the repo today
— confirmed by repo-wide grep). **Apply to:** `models/identity.py`, `models/record.py`,
`models/report.py`, and any other pure value object (Cluster B).

### Custom exception per concern, `where`-qualified / `msg = f"..."; raise X(msg)` messages
**Source:** `tools/corpus/manifest.py`'s `ManifestError` + every `_require_*`/`_validate_*`
helper; `tools/corpus/digests.py`'s `DigestFormatError`; `tools/security/gitleaks_selftest.py`'s
`SelfTestError`. **Apply to:** `dataplat/errors.py` and every raise site in this phase — this repo
lints with `ruff`'s full `ALL` rule set, which includes `EM101`/`EM102` (no f-string or literal
directly inside a `raise`), so the `msg = f"..."` local variable, then `raise Error(msg)`, is not
optional style — it is what `make lint` requires.

### `main(argv) -> int`, one catch, one exit code
**Source:** `tools/corpus/__main__.py` lines 151–167; `tools/security/gitleaks_selftest.py` lines
380–399. **Apply to:** `dataplat/cli.py` (Cluster O) — this is also the concrete mechanism behind
D-06's "caught exactly once."

### Makefile target additions: `.PHONY`, `##` help text, a comment block naming the decision
**Source:** `Makefile` lines 45–48 (`.PHONY` list), every target's `##` suffix consumed by `help:`
(line 52–53), and the explanatory comment above `cluster-verify` (150–153) /
`install-cluster` (127–129) / `check`/`ci` (257–267). **Apply to:** the new `test-integration`
target and any new `migrate`/image-build target this phase adds — update `.PHONY`, add a `##`
line, and add a comment block citing the CONTEXT.md decision ID the way every existing
Docker/cluster-gated target does.

### CI jobs: pinned-SHA checkout + pinned-SHA setup-uv + `make install` + one `make <target>` line
**Source:** `.github/workflows/ci.yml`'s `manifests` (64–76) and `secrets` (91–136) jobs — no job
in this workflow ever invokes a linter/tool directly; every job's substantive step is exactly one
`make` target. **Apply to:** the new `integration` job (Cluster S) — never add a second `run:`
step that duplicates what a Makefile target already does; if the CI job needs an extra step, that
step belongs in the Makefile target instead (`tests/policy/test_ci_invokes_make_only.py` already
enforces this repo-wide, and it will collect the new job automatically).

### ADR format: MADR + a mandatory, explicit "Migration trigger" section
**Source:** `docs/adr/0000-template.md` (full), `docs/adr/0002` (full), `docs/adr/0004` (full).
**Apply to:** the new `docs/adr/0008-*.md` (Cluster T). `docs/adr/README.md` line 52: numbering is
monotonic and never reused; next free number is 0008.

### Testcontainers/live-dependency fixtures: session scope, resolve `REPO_ROOT` once, skip with a named reason
**Source:** `tests/e2e/cluster/conftest.py` lines 47, 79–102, 105–127. **Apply to:**
`tests/integration/conftest.py` (Cluster R) — even though `03-RESEARCH.md`'s testcontainers
fixtures don't need a live-cluster skip (`PostgresContainer`/`MinioContainer` start their own
throwaway containers rather than reaching for something that might not exist), the
`REPO_ROOT = Path(__file__).resolve().parents[N]` convention and "never rely on an ambient
default" discipline (explicit `region_name`, explicit `addressing_style`) still apply.

### Test files under `tests/**` get relaxed lint rules, and every test subdirectory is its own package
**Source:** `pyproject.toml` line 88 (`"tests/**" = ["S101", "PLR2004", "ANN", "D"]`); every
existing `tests/*/` subdirectory (`unit`, `policy`, `regression`, `e2e`, `e2e/cluster`) has its own
`__init__.py`. **Apply to:** `tests/integration/__init__.py` and `tests/property/__init__.py` are
missing today and should be added alongside the first real test file in each directory, matching
every sibling.

---

## No Analog Found

Files with no close match anywhere in the repository — the planner should build these from the
`03-RESEARCH.md` Code Examples / `ARCHITECTURE.md` sections cited in the matching Pattern
Assignment cluster above (all of them are quoted in full there, not merely referenced):

| File | Role | Data Flow | Use instead |
|---|---|---|---|
| `config/model.py` | config | transform | Cluster E — CLAUDE.md's `ConfigDict(extra="forbid", frozen=True)` + `ARCHITECTURE.md` §4.4 |
| `config/registry.py` | provider+store | CRUD | Cluster F — `ARCHITECTURE.md` §5.1 (config-sync resolution rules) |
| `pipeline/protocol.py` | provider | streaming | Cluster G — `ARCHITECTURE.md` Q4.3, lines 478–502 |
| `pipeline/engine.py` | service | streaming | Cluster H — `03-RESEARCH.md` lines 430–446, 606–620 |
| `sources/protocol.py` | provider | streaming | Cluster G — `ARCHITECTURE.md` Q4.3, lines 465–474 |
| `load/publish/protocol.py` | provider | CRUD | Cluster G — `ARCHITECTURE.md` Q4.3, lines 508–514 |
| `storage/db.py` | service | CRUD | Cluster J — STACK.md `psycopg_pool.ConnectionPool` guidance |
| `metadata/repository.py` | provider | CRUD | Cluster F — `ARCHITECTURE.md` §2.1 column designs |
| `metadata/postgres.py` | store | CRUD | Cluster F / Cluster K (same table shape, DB side) |
| `metadata/fake.py` | store | CRUD | Cluster F — implement the same `Protocol` as `postgres.py` |
| `observability/logging.py` | utility | event-driven | Cluster M — `03-RESEARCH.md` lines 667–695 (complete) |
| `observability/metrics.py`, `tracing.py` | utility | event-driven | Cluster M — CONTEXT.md D-03 (no-op seam, real call sites) |
| `secrets/resolver.py` | service | request-response | Cluster N — `03-RESEARCH.md` lines 711–733 (complete) |
| `migrations/env.py` | migration | batch | Cluster K — `03-RESEARCH.md` lines 389–418 (complete) |
| `migrations/script.py.mako` | migration | batch | Alembic's own default template (`alembic init`) |
| `migrations/versions/0001-0005*.py` | migration | batch | Cluster K — `ARCHITECTURE.md` §2.1/§2.3 + `03-RESEARCH.md` Pattern 1 |
| `schemas/dataset-config.schema.json` | config | transform | `DatasetConfig.model_json_schema()`, dumped to file |
| `docker/csv-processor/Dockerfile` | config | batch | Cluster Q — `03-RESEARCH.md` lines 741–770 (complete) |
| `tests/property/test_chunking_properties.py` | test | streaming | `hypothesis`'s own `@given` docs + CONTEXT.md's "chunk sizes 1, 2, 3" guidance + `03-RESEARCH.md`'s own executable round-trip test as a starting fixture shape |

## Metadata

**Analog search scope:** `packages/`, `tests/`, `tools/`, `docs/adr/`, `Makefile`,
`pyproject.toml`, `setup.cfg`, `.github/workflows/`, `helm/values/local/cnpg-analytics.yaml`
(read-only throughout; no source file was modified).
**Files scanned directly:** 33 (full or targeted reads) plus repo-wide greps for `pydantic`,
`click`, `dataclass(frozen=True`, and `typing.Protocol` usage (all returned zero hits outside
`tools/corpus/manifest.py`'s dataclass usage, confirming the "no analog" calls above).
**Canonical research documents consulted:** `03-CONTEXT.md` (full), `03-RESEARCH.md` (full, 1004
lines), `.planning/research/ARCHITECTURE.md` §§2–5, 9, Recommended Repository Structure,
Architectural Patterns (lines 135–658, 936–1027, 1211–1327), `.planning/research/PITFALLS.md`
lines 30–58, 995–1074, `.planning/ROADMAP.md` Phase 3/4 sections (lines 153–213).
**Pattern extraction date:** 2026-08-12
