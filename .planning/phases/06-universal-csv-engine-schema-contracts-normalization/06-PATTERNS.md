# Phase 6: Universal CSV Engine, Schema Contracts & Normalization - Pattern Map

**Mapped:** 2026-08-15
**Files analyzed:** 52 (18 new source modules, 8 modified source modules, 2 config/dependency files, 3 corpus-tooling files, 2 unlisted-but-required test updates found during mapping, 19 new test files)
**Analogs found:** 46 / 52 (6 are genuinely new territory with no behavioral precedent — see "No Analog Found")

All line numbers below were read directly from the current committed source this session (2026-08-15), not from summaries.

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `csv_processor/detect/__init__.py` | config (pkg marker) | n/a | `dataplat/sources/__init__.py` | exact |
| `csv_processor/detect/filename.py` | utility (detector) | transform | `csv_processor/source.py` (conventions only) | partial |
| `csv_processor/detect/encoding.py` | utility (detector) | transform | `csv_processor/source.py` (conventions only) | partial |
| `csv_processor/detect/dialect.py` | utility (detector) | transform | `csv_processor/source.py` (conventions only) | partial |
| `csv_processor/detect/header.py` | utility (detector) | transform | `csv_processor/source.py` (conventions only) | partial |
| `csv_processor/detect/schema.py` | utility (detector) | transform | `csv_processor/source.py` (conventions only) | partial |
| `csv_processor/compression.py` | utility (I/O layer) | streaming, file-I/O | `dataplat/storage/objectstore.py` | role-match |
| `csv_processor/source.py` (MODIFIED — `inspect()`) | provider | streaming | itself + `dataplat/sources/protocol.py` | exact |
| `csv-processor/pyproject.toml` (MODIFIED — new deps) | config | n/a | itself | exact |
| `dataplat/sources/protocol.py` (MODIFIED — `inspect()`) | provider (protocol) | streaming | itself | exact |
| `dataplat/normalize/__init__.py` | config (pkg marker) | n/a | `dataplat/sources/__init__.py` | exact |
| `dataplat/normalize/dates.py` | middleware (StreamingStage) | streaming, transform | `dataplat/pipeline/engine.py::RaggedRowGuard` | exact |
| `dataplat/normalize/numeric.py` | middleware (StreamingStage) | streaming, transform | `dataplat/pipeline/engine.py::RaggedRowGuard` | exact |
| `dataplat/normalize/boolean_null.py` | middleware (StreamingStage) | streaming, transform | `dataplat/pipeline/engine.py::RaggedRowGuard` | exact |
| `dataplat/normalize/unicode.py` | middleware (StreamingStage) | streaming, transform | `dataplat/pipeline/engine.py::RaggedRowGuard` | exact |
| `dataplat/pipeline/engine.py` (MODIFIED — extension point) | middleware (orchestrator) | streaming | itself | exact (likely near-zero diff — see Cluster 13) |
| `dataplat/schema/__init__.py` | config (pkg marker) | n/a | `dataplat/config/__init__.py` | exact |
| `dataplat/schema/versioning.py` | utility (hasher) | transform | `dataplat/config/hashing.py` | exact |
| `dataplat/schema/evolution.py` | service (classifier) | transform | `dataplat/errors.py` + `dataplat/models/record.py` (dual) | partial |
| `dataplat/schema/repository.py` | store (Postgres CRUD) | CRUD | `dataplat/config/registry.py::ConfigRegistry` | exact |
| `dataplat/diagnostics.py` | utility (constants catalog) | transform | `dataplat/errors.py::_RESERVED_CONTEXT_KEYS` | partial |
| `dataplat/errors.py` (MODIFIED — `SourceError`/`SchemaError`) | utility (exception hierarchy) | n/a | itself | exact |
| `dataplat/config/model.py` (MODIFIED — `columns:`/`filename:`/`normalization:`/policy) | model (Pydantic config) | transform | itself | exact |
| `dataplat/discovery.py` (MODIFIED — idempotency key) | service | batch | itself | exact |
| `dataplat/models/record.py` (MODIFIED — consumption only) | model | streaming | itself | exact (no structural diff expected) |
| `migrations/versions/0009_meta_schema_versions.py` | migration | batch | `migrations/0001` + `migrations/0004` | exact |
| `configs/defaults.yaml` (MODIFIED — evolution-policy defaults) | config | n/a | itself | exact |
| `configs/datasets/customers.yaml` (deliberately **NOT** modified) | config | n/a | itself | n/a — see Cluster 15 |
| `tests/fixtures/corpus.yaml` (MODIFIED — `.zip` fixture) | test (fixture data) | file-I/O | itself (`61_gzipped.csv.gz` block) | exact |
| `tools/corpus/manifest.py` (MODIFIED — `_COMPRESSIONS`) | utility (fixture tooling) | transform | itself | exact |
| `tools/corpus/generators.py` (MODIFIED — `_write_wrapper`) | utility (fixture tooling) | file-I/O | itself | exact |
| `tests/unit/test_corpus_semantic_fixtures.py` (MODIFIED — count assertion; **unlisted, found this session**) | test | n/a | itself | exact |
| `tests/integration/test_migrations.py` (MODIFIED — FK assertion must flip; **unlisted, found this session**) | test | CRUD | itself | exact |
| `tests/unit/detect/__init__.py` | test | n/a | `tests/unit/__init__.py` | exact |
| `tests/unit/detect/test_filename.py` | test | transform | `tests/unit/test_corpus_semantic_fixtures.py` | role-match |
| `tests/unit/detect/test_encoding.py` | test | transform | `tests/unit/test_corpus_semantic_fixtures.py` | role-match |
| `tests/unit/detect/test_dialect.py` | test | transform | `tests/unit/test_corpus_semantic_fixtures.py` | role-match |
| `tests/unit/detect/test_header.py` | test | transform | `tests/unit/test_corpus_semantic_fixtures.py` | role-match |
| `tests/unit/detect/test_schema.py` | test | transform | `tests/unit/test_corpus_semantic_fixtures.py` | role-match |
| `tests/unit/normalize/__init__.py` | test | n/a | `tests/unit/__init__.py` | exact |
| `tests/unit/normalize/test_dates.py` | test | streaming | `tests/unit/test_pipeline_errors.py` | exact |
| `tests/unit/normalize/test_numeric.py` | test | streaming | `tests/unit/test_pipeline_errors.py` | exact |
| `tests/unit/normalize/test_boolean_null.py` | test | streaming | `tests/unit/test_pipeline_errors.py` | exact |
| `tests/unit/normalize/test_unicode.py` | test | streaming | `tests/unit/test_pipeline_errors.py` | exact |
| `tests/unit/schema/__init__.py` | test | n/a | `tests/unit/__init__.py` | exact |
| `tests/unit/schema/test_versioning.py` | test | transform | `tests/unit/test_config_hashing.py` | exact |
| `tests/unit/schema/test_evolution.py` | test | transform | `tests/unit/test_errors.py` | role-match |
| `tests/unit/test_dataset_config_columns.py` | test | transform | `tests/unit/test_batching_config.py` | exact |
| `tests/unit/test_compression.py` | test | file-I/O | `tests/unit/test_csv_chunking.py` | role-match |
| `tests/property/test_determinism.py` | test | transform | `tests/property/test_chunking_properties.py` | exact |
| `tests/property/test_dst_correctness.py` | test | transform | `tests/property/test_chunking_properties.py` | exact |
| `tests/integration/test_schema_resolution.py` | test | CRUD | `tests/integration/test_config_registry.py` | exact |

## Pattern Assignments

### Cluster 1 — The five detectors: `csv_processor/detect/{filename,encoding,dialect,header,schema}.py` (NEW)

**No behavioral precedent exists in this codebase** — Phase 3's `csv_processor/source.py` explicitly hardcoded away detection ("no detection lives here — that is Phase 6's territory", `source.py:6`). The closest analog is `source.py` itself for **module conventions**, plus `dataplat/config/hashing.py` for the "pure function → typed, frozen result" shape every detector should follow.

**Module docstring / "why" convention** (`csv_processor/source.py` lines 1-18):
```python
"""The first real ``csv_processor`` code: a minimal, working CSV ``Source``.
...
No encoding, dialect or header-row detection lives here -- that is Phase 6's
``csv_processor/detect/`` territory.
"""

from __future__ import annotations

import contextlib
import csv
import itertools
from typing import TYPE_CHECKING

from dataplat.models.record import RecordChunk
from dataplat.observability import metrics
from dataplat.sources.protocol import RecordStream, Source

if TYPE_CHECKING:
    from collections.abc import Iterator
    from io import TextIOWrapper

    from dataplat.pipeline.protocol import PipelineContext
```
Every detector module should follow this exact shape: `from __future__ import annotations` first, `TYPE_CHECKING`-guarded imports for types-only dependencies, a module-level docstring that states *why* (cites requirement IDs / decision IDs), not just what.

**"Pure function → frozen dataclass result" shape** (`dataplat/config/hashing.py`, full file, 54 lines — this is the exact shape to mirror for `detect/encoding.py`, `detect/dialect.py`, etc.):
```python
CONFIG_HASH_VERSION: int = 1

def hash_config(config: DatasetConfig) -> tuple[str, int]:
    """Hash a validated ``DatasetConfig`` via ARCHITECTURE.md §5.2's canonical-JSON recipe.
    ...
    Returns:
        A ``(config_hash, hash_version)`` tuple...
    """
    canonical = json.dumps(config.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    config_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return config_hash, CONFIG_HASH_VERSION
```
A pure input→output function, module-level version constant, no side effects, no I/O. `detect/*.py` functions should follow this: e.g. `detect_encoding(sample: bytes, *, contract_encoding: str | None, min_confidence: float) -> EncodingDetection`.

**Frozen-dataclass result-value convention** — mirror `dataplat/models/record.py`'s `RecordChunk`/`RejectedRecord` shape (`models/record.py` lines 20-36, 60-92): `@dataclass(frozen=True, slots=True)`, every field typed, a docstring per field. RESEARCH.md's `EncodingDetection` sketch (06-RESEARCH.md lines 321-326, already verified live against the pinned library versions) follows this exact convention:
```python
@dataclass(frozen=True, slots=True)
class EncodingDetection:
    encoding: str
    confidence: float
    source: str  # "contract" | "bom" | "detected" | "undetermined"
```
Note `source` is a plain `str` with a comment enumerating the values, not a `Literal`/`Enum` — matches this project's "strings not enums" convention (see Shared Patterns).

**Verified library usage** — do not re-derive these; `06-RESEARCH.md` Architecture Patterns 2-4 (lines 300-455) and Common Pitfalls 1-3, 6, 9 (lines 479-540) contain live-executed, pinned-version-exact code for: BOM sniff + `charset-normalizer`/`chardet` agreement algorithm, `clevercsv` single-column crash guard, the filename-mask token→regex compiler, and the `.gz`/`.zip` decompression split. Copy those verified snippets directly; they are more reliable than re-deriving from library docs.

**Possible touch point, not a required one:** `dataplat/models/report.py`'s `ValidationResult` (full file, 30 lines) — `rule_id`/`outcome`/`message`, deliberately a plain string `outcome` (D-05's "minimal shape") — is what `detect/schema.py`'s inference findings or `detect/header.py`'s ambiguous-header findings would populate if a detector needs to report a non-fatal finding rather than raise. Not in the orchestrator's given file list; flag to the planner as optional.

---

### Cluster 2 — `csv_processor/compression.py` (NEW)

**Analog:** `dataplat/storage/objectstore.py::open_text_stream` (lines 35-67) — this is the exact seam the decompression layer wraps, one level simpler than expected (see "Notable Findings" — no hand-written `RawIOBase` adapter exists to extend).

**Imports + the seam being wrapped** (`objectstore.py` lines 16-26, 35-67):
```python
import io
...
def open_text_stream(
    response_body: object, *, encoding: str, newline: str = "", errors: str = "strict",
) -> io.TextIOWrapper:
    buffered = io.BufferedReader(response_body)  # type: ignore[type-var]
    return io.TextIOWrapper(buffered, encoding=encoding, newline=newline, errors=errors)
```
`compression.py`'s `.gz` path wraps `io.BufferedReader(response_body)` with `gzip.GzipFile(fileobj=buffered, mode="rb")` **before** the `io.TextIOWrapper` — verified live this session (RESEARCH.md Pattern 4, lines 420-432). No new adapter class needed.

**Exception-wrapping pattern for the `.zip` in-memory-buffer path** — mirror `S3ObjectStore.get_object` (`objectstore.py` lines 172-199):
```python
try:
    response = self._client.get_object(Bucket=bucket, Key=key)
except (ClientError, BotoCoreError) as exc:
    msg = "failed to get object from object storage"
    raise StorageError(msg, context={"bucket": bucket, "key": key}) from exc
```
`compression.py`'s `.zip` path should catch `zipfile.BadZipFile` the same way and re-raise as a new `SourceError` subclass (e.g. `FileInspectionError`), with `context={"key": ..., "diagnostic_code": ...}` — never let the raw `zipfile`/`gzip` exception escape, matching this project's WR-01 "disjoint hierarchies caught explicitly, never a bare exception crosses a module boundary" discipline.

**D-22a's scoped exception is already precisely worked out** in `06-RESEARCH.md` Architecture Patterns Pattern 4 (lines 434-455) — the `io.BytesIO(compressed_bytes)` buffering approach, with the exact `ZipExtFile` streaming-read verification. Copy directly.

---

### Cluster 3 — `csv_processor/source.py::Source.inspect()` + `dataplat/sources/protocol.py` (MODIFIED)

**Analog:** both files, in the exact place their own docstrings say to extend them.

`sources/protocol.py`'s module docstring (lines 1-14) is the literal instruction:
```python
"""``Source``/``RecordStream`` — how a run reads records, independent of the engine.

Deliberately narrower than ``ARCHITECTURE.md`` Q4.3's original sketch, which
attaches two extra schema- and profile-describing attributes to
``RecordStream`` and an ``inspect(ctx)`` method to ``Source`` that returns
the profile one. Neither of those two attributes' types exists anywhere in
this codebase yet ... Phase 6 adds ``inspect()`` plus the two attributes
once their types exist. This file is this seam's Phase-3 shape, not its
final shape.
"""
```
The current `Source` Protocol (lines 46-63) has exactly one method (`open`); add `inspect(self, ctx: PipelineContext) -> CsvProfile: ...` beside it, following the identical Google-style docstring shape already used for `open()`.

`CsvSource` in `source.py` (lines 184-219) is the concrete class `inspect()` lands on — same `__init__`/`open()` shape as today, one new method added. **Import-linter constraint** (`setup.cfg` lines 22-28, contract 1: "dataplat core must not depend on the CSV plugin"): `CsvProfile`'s type must live in `dataplat` (not `csv_processor`), because `dataplat.sources.protocol.Source` references it — `csv_processor.source.CsvSource.inspect()` constructs it and returns it, exactly the same direction `CsvSource` already imports `dataplat.models.record.RecordChunk` (source.py line 27). Candidate location: a new `dataplat/models/profile.py` sibling of `models/record.py`, following the same `@dataclass(frozen=True, slots=True)` shape — or reuse `AssignmentDocument`'s Pydantic `BaseModel` shape (`dataplat/models/assignment.py` line 28 on) if `CsvProfile` ever needs JSON persistence. This exact module/location is one of CONTEXT.md's "Claude's Discretion" points — both analogs are viable; planner should pick one and be consistent with whichever value-object convention (dataclass vs. BaseModel) the rest of this phase's new types use.

---

### Cluster 4 — The four normalizers: `dataplat/normalize/{dates,numeric,boolean_null,unicode}.py` (NEW)

**Analog: `dataplat/pipeline/engine.py::RaggedRowGuard`, verbatim** — this is the single strongest analog in the entire phase; CONTEXT.md and RESEARCH.md both name it explicitly as the template.

Full class (`pipeline/engine.py` lines 38-105):
```python
class RaggedRowGuard(StreamingStage):
    """Rejects rows whose field count does not match the chunk's expected count.

    The concrete proof of QUAL-03's errors-as-values mechanism: a malformed
    row is data (a ``RejectedRecord``), never an exception.
    """

    name = "ragged_row_guard"

    def __init__(self, *, field_delimiter: str = ",") -> None:
        ...
        self._field_delimiter = field_delimiter

    def apply(self, ctx: PipelineContext, chunk: RecordChunk) -> StageResult:  # noqa: ARG002
        kept: list[tuple[str, ...]] = []
        rejected: list[RejectedRecord] = []
        for i, row in enumerate(chunk.rows):
            if len(row) != chunk.expected_field_count:
                rejected.append(
                    RejectedRecord(
                        source_row_number=chunk.first_ordinal + i,
                        error_type="RAGGED_ROW",
                        error_message=f"expected {chunk.expected_field_count} fields, got {len(row)}",
                        raw_line=self._field_delimiter.join(row),
                    )
                )
                continue  # never pad or truncate (polars #10585, CONTEXT.md D-01)
            kept.append(row)

        metrics.increment("rows_rejected", len(rejected))
        metrics.increment("rows_kept", len(kept))
        return StageResult(chunk=chunk.replace(rows=tuple(kept)), rejected=rejected, findings=[])
```
Every one of `dates.py`/`numeric.py`/`boolean_null.py`/`unicode.py` should define a class with: a `name: str` class attribute (stable, e.g. `"date_normalizer"`), an `apply(self, ctx: PipelineContext, chunk: RecordChunk) -> StageResult` method that never raises for a row-level problem, a `metrics.increment(...)` call site, and `RejectedRecord`s for values that fail (e.g. an invalid date under CSV-09) rather than exceptions. `unicode.py`'s NFC normalizer is the one exception to "may reject a row" — it is a pure, always-succeeding transform (D-15), so its `StageResult.rejected` will always be empty; it should still return the same `StageResult` shape for consistency with `run_streaming`'s threading contract.

**The `StreamingStage` protocol it implements** (`pipeline/protocol.py` lines 78-103):
```python
class StreamingStage(Protocol):
    """A stage that runs once per chunk — bounded memory by construction (README §39)."""
    name: str
    def apply(self, ctx: PipelineContext, chunk: RecordChunk) -> StageResult: ...
```

**Ordering constraint (hard edge, not optional):** ROADMAP's plan guidance and D-15 require normalization (including NFC) to run **before** any hash computation. `unicode.py`'s stage must be ordered last among the four (or at minimum after `dates.py`/`numeric.py`/`boolean_null.py`), since those may still be operating on not-yet-NFC-normalized raw strings for their own token matching (e.g. boolean-token lookup) — verify this ordering decision explicitly in the plan, it is not encoded anywhere in the type system.

**Constructor-injection precedent for a "future caller passes in the real detected value"** — `RaggedRowGuard.__init__`'s `field_delimiter` parameter (`pipeline/engine.py` lines 47-63) is the exact precedent for how `dates.py`'s date-format list, `numeric.py`'s decimal/thousands separators, and `boolean_null.py`'s token lists should be threaded in: as constructor parameters supplied by whatever wires the pipeline together (from the resolved `DatasetConfig`), never read from a global or re-derived internally.

---

### Cluster 5 — `dataplat/schema/versioning.py` (NEW)

**Analog: `dataplat/config/hashing.py`, structurally identical.** Full file already quoted in Cluster 1. Reuse:
- A module-level `..._HASH_VERSION: int = 1` constant, bumped only when the canonicalization recipe itself changes (PITFALLS.md C6 — every stored hash gets a companion version).
- `json.dumps(..., sort_keys=True, separators=(",", ":"), ensure_ascii=False)` then `hashlib.sha256(...).hexdigest()` — the platform's one canonical-JSON hashing recipe; do not invent a second one for schemas.
- Return a `(hash, hash_version)` tuple, matching `hash_config`'s signature exactly, so `schema/repository.py` can consume it the same way `config/registry.py::sync()` consumes `hash_config()` (see Cluster 7).

---

### Cluster 6 — `dataplat/schema/evolution.py` (NEW)

**No single analog** — this file's job (classify compatible vs. breaking, per D-01/D-02/D-04) genuinely splits across two existing patterns depending on the classification outcome:

**Compatible path → errors-as-values, mirror `RejectedRecord`/`StageResult`** (`models/record.py` lines 60-92, 95-113): a compatible change is *data* (a proposal), never an exception. Consider a `SchemaChangeFinding` value object following `RejectedRecord`'s exact shape (`@dataclass(frozen=True, slots=True)`, a stable `change_type` string, a human message) rather than raising anything.

**Breaking path → raise, mirror `dataplat/errors.py`'s subclass shape** (lines 75-99):
```python
class ConfigurationError(DataPlatformError):
    """Bad or missing configuration, or an unknown registry key.

    Raised when a dataset config fails validation, a config file cannot be
    found or parsed, or a config names a source/deduplication/publisher
    strategy key that has no registry entry.
    """
```
`IncompatibleSchemaError` (a new `SchemaError` subclass, added in `dataplat/errors.py` per Cluster 10) needs exactly this shape: no custom `__init__`, a one-paragraph docstring stating precisely when it's raised, `context` populated by the raise site (e.g. `context={"diagnostic_code": "column-disappeared", "column": "birth_date"}`) via the inherited `DataPlatformError.__init__`.

**Test-shape precedent for the dual behavior**: `tests/unit/test_pipeline_errors.py` (errors-as-values path) + `tests/unit/test_errors.py` (raise path) — see Cluster 18.

---

### Cluster 7 — `dataplat/schema/repository.py` (NEW)

**Analog: `dataplat/config/registry.py::ConfigRegistry`, explicitly named as this module's sibling by both CONTEXT.md and RESEARCH.md.** Full file read (259 lines). Key excerpts:

**Constructor + pool ownership** (`config/registry.py` lines 89-107):
```python
class ConfigRegistry:
    """The Postgres-backed system of record for ``meta.config_versions``.

    Constructed with a pool the caller builds via
    ``dataplat.storage.db.create_pool()`` — this class never constructs its
    own ``ConnectionPool``...
    """
    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool
```
`SchemaRepository` (or whatever name planning picks) should take the identical constructor shape.

**The versioned-upsert pattern** (`sync()`, lines 109-185) — hash matches current → no-op; hash differs → close old row (`valid_to = now()`) + insert `version = max + 1`:
```python
current = cur.execute(
    """
    SELECT config_version_id, version, config_hash
      FROM meta.config_versions
     WHERE dataset_id = %s AND valid_to IS NULL
    """,
    (dataset_id,),
).fetchone()

if current is not None and current[2] == config_hash:
    return ConfigVersionRecord(..., is_new=False)

if current is not None:
    cur.execute("UPDATE meta.config_versions SET valid_to = now() WHERE config_version_id = %s", (current[0],))

new_row = cur.execute(
    """
    INSERT INTO meta.config_versions (dataset_id, version, config_hash, hash_version, config_document, config_schema_version, valid_from)
    VALUES (%s, COALESCE((SELECT MAX(version) FROM meta.config_versions WHERE dataset_id = %s) + 1, 1), %s, %s, %s, %s, now())
    RETURNING config_version_id, version
    """,
    (...),
).fetchone()
```
`meta.schema_versions` (migration 0009, Cluster 14) has the identical shape — `dataset_id`, `version`, a hash column, `valid_from`/`valid_to`, a partial-unique "current" index — so this exact SQL pattern transposes directly, table name and column list changed only.

**Historical resolution (`get_by_id`, lines 187-219)** — this is the direct precedent for SCHEMA-06's D-16 hash-match resolution:
```python
def get_by_id(self, config_version_id: int) -> DatasetConfig:
    """Re-resolve one dataset's config exactly as it was at a specific version.
    ...
    """
    with self._pool.connection() as conn, conn.cursor() as cur:
        row = cur.execute(
            "SELECT config_document FROM meta.config_versions WHERE config_version_id = %s",
            (config_version_id,),
        ).fetchone()
    found = _require_row(row, f"no meta.config_versions row for config_version_id={config_version_id}")
    return DatasetConfig.model_validate(found[0])
```
`SchemaRepository` needs the SCHEMA-06 mirror-image lookup: given a re-derived structural hash, find the matching historical `meta.schema_versions` row for a dataset (`SELECT ... WHERE dataset_id = %s AND schema_hash = %s`), raising a `StorageError` (via the same `_require_row` helper, lines 68-86) when no match exists.

**The `_require_row` helper** (lines 68-86) — copy verbatim, it is a small, generic "narrow a possibly-`None` fetched row" guard with no `ConfigRegistry`-specific logic in it; a second private copy in `schema/repository.py` is fine (it's 6 lines), or planning may choose to promote it to a shared location — not required by any decision.

**Atomic upsert precedent for concurrent-first-write races** (`_resolve_dataset_id`, lines 221-259) — `INSERT ... ON CONFLICT (...) DO UPDATE SET x = EXCLUDED.x RETURNING ...`, never `SELECT ... FOR UPDATE` then `INSERT`. Relevant if `schema/repository.py` needs the same "first schema version for a brand-new dataset" race protection.

---

### Cluster 8 — `dataplat/diagnostics.py` (NEW)

**No whole-module analog exists** (nothing in this codebase is a standalone constants catalog today) — the precedent is a single frozenset constant, scaled up.

`dataplat/errors.py` lines 32-36:
```python
# Keys `cli.py`'s catch-once handler passes as fixed keyword arguments
# alongside a spread `**context` (WR-03) — reserved so a raise site can
# never collide with them and crash the handler whose entire purpose is to
# never crash.
_RESERVED_CONTEXT_KEYS = frozenset({"error_type", "error_message"})
```
`dataplat/cli.py` line 40 uses the identical shape: `_LOG_JSON_TRUTHY = frozenset({"1", "true", "yes", "on"})`. `dataplat/diagnostics.py`'s catalog should be a module-level `Final[frozenset[str]]` (or a small set of named string constants — this project never uses `Enum` for string-keyed vocabularies, see Shared Patterns), populated **verbatim** from `tests/fixtures/corpus.yaml`'s existing `quarantine_reason` strings (D-24) — e.g. `"nul-byte-in-text-field"`, `"field-exceeds-max-field-bytes"`, `"undecodable-bytes"`, `"empty-file"`, `"duplicate-header-names"`, `"field-count-below-header"`, `"field-count-above-header"`, `"unclosed-quote-at-eof"`, `"scientific-notation-identifier-unrecoverable"`, `"fixed-width-identifier-below-declared-width"`, `"spreadsheet-serial-date-does-not-exist"`, `"nonexistent-local-time"`, `"naive-timestamp-without-a-declared-zone"`, `"unmapped-boolean-token"` — all confirmed present via `grep -n "quarantine_reason"` this session (`tests/fixtures/corpus.yaml` lines 363, 396, 591, 1041, 1092, 1204, 1226, 1249, 1366, 1614, 1659, 1972, 2031, 2073, 2142, 2193).

**`RejectedRecord.error_type`'s own docstring** (`models/record.py` lines 68-70) is the row-level consumption contract this catalog must satisfy: *"A short, stable, machine-readable reason code, e.g. `"RAGGED_ROW"`."*

---

### Cluster 9 — `dataplat/config/model.py` extension (MODIFIED)

**Analog: itself.** The module docstring (lines 17-23) is a standing, already-committed instruction naming the exact validator this phase must add:
```python
"""
This model intentionally carries no ``delimiter``/``decimal_separator``
collision validator: no such fields exist on ``DatasetConfig`` today...
Phase 6, which introduces ``delimiter``/``decimal_separator`` fields, must
add the STACK.md §15 "do not confuse CSV delimiters with decimal separators"
collision check at that point — not before.
"""
```

**Template for every new sub-model** (`SourceConfig`, lines 31-55, and `DeduplicationConfig`, lines 57-72 — pick either, they're the same shape):
```python
class DeduplicationConfig(BaseModel):
    """How a dataset's deduplication stage collapses duplicate records.

    Attributes:
        strategy: Deduplication strategy key resolved through
            ``DEDUP_REGISTRY``, e.g. ``"business_key_latest"``.
        keys: Business-key column names that identify one logical record.
        order_by: Column expressions (e.g. ``"event_ts desc"``) that decide
            which duplicate wins.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    strategy: str
    keys: list[str]
    order_by: list[str]
```
The new `ColumnContract` (for `columns:`), `FilenameMaskConfig` (for `filename:`), `NormalizationConfig` (for `normalization:`), and schema-evolution-policy fields must all use this exact `model_config = ConfigDict(extra="forbid", frozen=True)` + fully-Google-docstringed-Attributes shape. D-18's cross-model validator (`deduplication.keys` must reference a `columns:` entry marked `business_key: true`) is a `@model_validator` on `DatasetConfig` itself — there is no existing precedent for a cross-field `model_validator` in this file today (only per-field types), so this is genuinely new but stays inside Pydantic's standard idiom.

**Where `DatasetConfig` composes its sub-models** (lines 107-136) — add the new fields the same way `batching: BatchingConfig` was added (a plain, non-optional, required field — no `| None` defaults for anything that must fail loudly when omitted, matching `BatchingConfig.max_units_per_run`'s own precedent, lines 91-104, "Required, never defaulted — a missing cap must fail config validation loudly").

---

### Cluster 10 — `dataplat/errors.py` extension (MODIFIED)

**Analog: itself.** The three existing subclasses (lines 75-99) are the exact template — no custom `__init__`, one docstring, nothing else:
```python
class StorageError(DataPlatformError):
    """A storage backend is unreachable, refuses access, or fails a guard.

    Raised when MinIO/S3 or PostgreSQL cannot be reached, a permission is
    denied, or a wrong-database guard trip is detected (e.g. an analytical-DB
    operation aimed at the Airflow metadata database).
    """
```
Add `SourceError` (base) with subclasses `FileInspectionError`, `FilenameParsingError`, `EncodingDetectionError`, `CsvDialectDetectionError`, `CsvParsingError`, and `SchemaError` (base) with subclasses `SchemaValidationError`, `IncompatibleSchemaError` — exact names already fixed by `ARCHITECTURE.md` §4.5 (cited in CONTEXT.md canonical_refs) — each a one-paragraph docstring, zero new code beyond the class statement + docstring. The module docstring's own rule (lines 5-7) applies: *"each is added by the phase that first raises it... A subclass with no raise site is dead code wearing a design decision's clothes."* Do not add a subclass this phase does not actually raise somewhere.

**The `context` dict + reserved-key guard** (lines 39-72) is inherited automatically — no changes needed to `DataPlatformError.__init__` itself; every new subclass gets `context: dict[str, object]` for free, which is where a diagnostic code from `diagnostics.py` (Cluster 8) lands, e.g. `raise IncompatibleSchemaError(msg, context={"diagnostic_code": "column-disappeared", "column": "birth_date"})`.

---

### Cluster 11 — `dataplat/discovery.py` extension (MODIFIED)

**Analog: itself — the exact lines the module docstring names.** Module docstring (lines 9-26) is a standing instruction:
```python
"""
Idempotency key (ARCHITECTURE.md Q7, verbatim...)::

    idempotency_key = sha256(
        dataset_name | file.content_sha256 | config_hash |
        schema_version | processor_image_digest | target_partition | policy_digest
    )

``try_number`` and ``dag_run_id`` are DELIBERATELY ABSENT...
schema_version_id``/``target_partition``/``policy_digest`` have no populated
value yet -- no schema-versioning concept exists until Phase 6...
so this module computes ``idempotency_key = sha256(f"{dataset_name}|
{content_sha256_hex}|{config_hash}|{processor_image}").hexdigest()``. A
later phase EXTENDS this formula by appending the missing terms once they
have real values; it does not replace it.
"""
```
The actual computation to extend (lines 249-251):
```python
idempotency_key = hashlib.sha256(
    f"{dataset_name}|{content_sha256_hex}|{config_hash}|{processor_image}".encode(),
).hexdigest()
```
Append `|{schema_version}` (or whatever the resolved term is called) — **append, never reorder or replace** the existing terms (the docstring is explicit about this). `target_partition`/`policy_digest` stay absent; nothing in this phase gives them real values either (D-17 explicitly defers the `config_policy` knob).

**Scope check, saves a rabbit hole:** `MetadataRepository.get_or_create_ingestion_run` (`metadata/repository.py` lines 233-243) takes `idempotency_key: str` as an already-computed opaque string — its signature needs **no change**; the formula extension is entirely local to `discovery.py`'s own `hashlib.sha256(...)` call. Confirmed by reading the full method signature this session.

**Required companion test, per RESEARCH.md Pitfall 5** (not itself a new file in the given list, but worth flagging to the planner): a test asserting that a file processed before this formula change, if reprocessed after, is *expected* to compute a different `idempotency_key` and is *not* a correctness bug — `tests/unit/test_discovery.py` (426 lines, existing) is where this assertion belongs, following its own existing test shapes.

---

### Cluster 12 — `dataplat/models/record.py` (MODIFIED — consumption only)

**Analog: itself.** `RejectedRecord.error_type` (lines 60-92) already exists with the exact docstring this phase's diagnostic vocabulary must satisfy — no field addition, no type change expected. The "modification" this phase makes is behavioral (new stages populate `error_type` with values drawn from `diagnostics.py`'s catalog instead of ad hoc strings), not structural. Flag to planner: confirm no code change to this file is actually required before scheduling a task against it — it may turn out to be a zero-diff file, consumed rather than edited.

---

### Cluster 13 — `dataplat/pipeline/engine.py` (MODIFIED — extension point)

**Analog: itself.** `run_streaming` (lines 108-144) already accepts `stages: Sequence[StreamingStage]` generically:
```python
def run_streaming(
    ctx: PipelineContext,
    chunks: Iterable[RecordChunk],
    stages: Sequence[StreamingStage],
) -> Iterator[tuple[int, StageResult]]:
    for chunk in chunks:
        ...
        for stage in stages:
            result = stage.apply(ctx, current_chunk)
            current_chunk = result.chunk
            merged_rejected.extend(result.rejected)
            merged_findings.extend(result.findings)
        yield (first_ordinal, StageResult(...))
```
**This function likely needs zero code changes** — the four new normalizers (Cluster 4) are just additional `StreamingStage` implementations that get constructed and passed into this same `stages` sequence by whatever assembles the pipeline. **Gap worth flagging:** `grep` of `dataplat/pipeline/run.py` this session found no existing stage-list assembly (`[RaggedRowGuard(), ...]` is not wired anywhere yet in committed code) — the real extension point a plan needs to touch may be wherever the `ingest` CLI or DAG task constructs the `stages` list for the first time, not `engine.py` itself. Confirm this file scope with the planner before assigning a task to "modify `engine.py`" — the modification may really belong to a not-yet-written orchestration call site.

---

### Cluster 14 — `migrations/versions/0009_meta_schema_versions.py` (NEW) + the test it must flip

**Analog: `migrations/0001` (table + hash_version + partial-unique-"current"-index shape) and `migrations/0004` (the deferred-FK-closing shape this migration completes).**

`migrations/0001` table/index shape (lines 60-93):
```python
op.create_table(
    "config_versions",
    sa.Column("config_version_id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
    sa.Column("dataset_id", sa.BigInteger(), sa.ForeignKey("meta.datasets.dataset_id"), nullable=False),
    sa.Column("version", sa.Integer(), nullable=False),
    sa.Column("config_hash", sa.Text(), nullable=False),
    sa.Column("hash_version", sa.SmallInteger(), nullable=False, server_default=sa.text("1")),
    sa.Column("config_document", JSONB(), nullable=False),
    ...
    sa.UniqueConstraint("dataset_id", "version", name="uq_config_versions_dataset_version"),
    sa.UniqueConstraint("dataset_id", "config_hash", name="uq_config_versions_dataset_hash"),
    schema="meta",
)
op.create_index(
    "uq_config_versions_current_per_dataset", "config_versions", ["dataset_id"],
    unique=True, schema="meta", postgresql_where=sa.text("valid_to IS NULL"),
)
op.execute("GRANT SELECT, INSERT, UPDATE ON meta.config_versions TO etl_app")
```

`migrations/0004`'s deferred-FK comment (lines 64-68) is exactly what `0009` must resolve:
```python
# Deliberately unconstrained: meta.schema_versions (the referent for
# this column) does not exist until a later phase's migration
# (CONTEXT.md D-05, ARCHITECTURE.md §2.4). The column lands now so
# the design stays coherent; a later migration adds the constraint
# via op.create_foreign_key once that table exists.
sa.Column("schema_version_id", sa.BigInteger(), nullable=True),
```

`06-RESEARCH.md`'s Code Examples section (lines 622-674) already provides a verified-against-these-exact-conventions draft migration — `create_table("schema_versions", ...)` with the `ARCHITECTURE.md` line-232 column shape, the same partial-unique-index pattern, plus the `op.create_foreign_key("fk_ingestion_runs_schema_version_id", "ingestion_runs", "schema_versions", ["schema_version_id"], ["schema_version_id"], source_schema="meta", referent_schema="meta")` call that closes 0004's deferred FK. Use it directly; it was built by reading `0001`/`0004` first, not independently invented.

**Critical unlisted finding — a test must be inverted, not just extended:** `tests/integration/test_migrations.py` lines 142-157 currently asserts the **opposite** of what migration 0009 creates:
```python
def test_ingestion_runs_schema_version_id_has_no_fk(migrated_dsn: str) -> None:
    with psycopg.connect(migrated_dsn) as conn:
        rows = conn.execute(
            """
            SELECT tc.constraint_name
              FROM information_schema.table_constraints tc
              JOIN information_schema.key_column_usage kcu ...
             WHERE tc.constraint_type = 'FOREIGN KEY'
               AND tc.table_schema = 'meta'
               AND tc.table_name = 'ingestion_runs'
               AND kcu.column_name = 'schema_version_id'
            """,
        ).fetchall()
    assert rows == [], f"schema_version_id must carry no FK constraint, found: {rows}"
```
This test currently passes *because* the FK doesn't exist yet — it will start failing the moment migration 0009 lands, by design (it is proving the pre-0009 state). It must be updated to assert the FK **does** exist (rename to something like `test_ingestion_runs_schema_version_id_has_an_fk_after_0009`, or invert the assertion in place) as part of this phase's migration task. This file was not named anywhere in CONTEXT.md or RESEARCH.md's file lists — found only by reading `test_migrations.py` directly this session.

**No conftest change needed:** `tests/integration/conftest.py`'s `migrated_dsn` fixture (lines 134-146) already runs `alembic upgrade head` against every revision under `migrations/versions/` — `0009` is picked up automatically the moment the file exists; `tests/integration/test_schema_resolution.py` (Cluster 18) can depend on `migrated_dsn` directly with zero fixture-file changes.

---

### Cluster 15 — `configs/defaults.yaml` (MODIFIED) vs. `configs/datasets/customers.yaml` (deliberately untouched)

**Analog: itself.** Current full content (16 lines):
```yaml
# configs/defaults.yaml — COMMITTED. Platform-wide defaults merged UNDER every
# dataset config (ARCHITECTURE.md Q4.4, lines 592-594).
# ...
config_schema_version: 1
```
D-03 lands the schema-evolution policy defaults here. **Pitfall 7 applies directly** (`06-RESEARCH.md` lines 521-526): `config/loader.py::load_config` merges with `merged = {**defaults, **dataset}` — a **top-level-only** dict spread (`config/loader.py` line 63). A dataset that wants to override one sub-key of a new nested `schema_evolution:` block would lose every sibling default. Recommended shape (per RESEARCH.md's own recommendation): flatten new default keys to the top level (e.g. `schema_evolution_on_new_column: evolve`) rather than nesting, matching this file's existing flat shape exactly — do not add a nested block without re-checking this collision first.

`configs/datasets/customers.yaml` (39 lines, read in full) — **do not add `filename:` or `normalization:` blocks to this file.** D-10 and the Locale decisions' "Consequence, not a fresh decision" note both explicitly state `customers` needs neither block; its current shape (`source`/`deduplication`/`load`/`batching`, no `columns:` yet either — though `columns:` becomes the new source of truth per D-18, so this file likely *does* need a `columns:` block added even though it needs no `filename:`/`normalization:` block). Flag this nuance to the planner: `customers.yaml` is not fully frozen this phase (D-18's `columns:` contract applies to every dataset including this one), only its filename-mask and locale axes are frozen.

---

### Cluster 16 — Corpus fixture + tooling: `corpus.yaml`, `manifest.py`, `generators.py` (MODIFIED) + `test_corpus_semantic_fixtures.py` (MODIFIED, unlisted)

**Analog: itself — the `.gz` wrapper is the literal template for the new `.zip` wrapper, everywhere.**

`corpus.yaml`'s existing `61_gzipped.csv.gz` fixture (lines 520-534) is the template for the new `.zip` fixture:
```yaml
  - name: "61_gzipped.csv.gz"
    covers: [CSV-11]
    generator: wrapper
    wraps: "01_simple.csv"
    compression: gzip
    gzip_mtime: 0
    gzip_filename: ""
    expect:
      decompresses_to: "01_simple.csv"
      data_rows: 20
```
A new fixture (e.g. `63_zipped.csv.zip`, next available number after 62) should follow this exact shape with `compression: zip` and no `gzip_mtime`/`gzip_filename` fields (zip has no equivalent embedded-timestamp footgun the way gzip does — R5's concern is gzip-specific, confirm no zip-equivalent nondeterminism exists before dropping those fields, e.g. zip's own internal timestamps default to a fixed epoch when writer options are held constant).

`tools/corpus/manifest.py`'s compression whitelist (line 63) is a one-line change:
```python
_COMPRESSIONS: Final[tuple[str, ...]] = ("gzip",)
```
→ `("gzip", "zip")`. The `_validate_wrapper` function (lines 650-657) needs no change — it already only checks `fixture.compression is not None`, generic across compression kinds.

`tools/corpus/generators.py::_write_wrapper` (lines 391-421) is the literal template for the new zip branch:
```python
def _write_wrapper(fixture: Fixture, out_dir: Path, path: Path) -> None:
    """Compress an already-materialised fixture with deterministic headers."""
    ...
    if fixture.compression != "gzip":  # pragma: no cover - guarded at load time
        msg = f"{fixture.name}: unsupported compression {fixture.compression!r}"
        raise GeneratorError(msg)

    with (
        path.open("wb") as raw,
        target.open("rb") as source,
        gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=_GZIP_LEVEL, mtime=fixture.gzip_mtime, filename=fixture.gzip_filename),
    ) as gz:
        ...
```
Add an `elif fixture.compression == "zip":` branch using `zipfile.ZipFile(raw, mode="w")` + `zf.writestr(member_name, source.read())` (or `zf.write`), replacing the single unconditional `gzip`-only check with a dispatch over `_COMPRESSIONS`'s now-two values. **Determinism note (mirrors R5's gzip mtime/filename concern):** `zipfile.ZipInfo` also embeds a `date_time` field defaulting to the current wall-clock time — pin it explicitly (e.g. `ZipInfo(filename, date_time=(1980, 1, 1, 0, 0, 0))`, ZIP's own minimum representable date) or the new fixture will be exactly as non-reproducible across runs as an unpinned gzip fixture would have been. This is not spelled out in CONTEXT.md/RESEARCH.md — found by reasoning from `_write_wrapper`'s own R5 comment (`generators.py` lines 409-411) applied to the new format.

**Unlisted, found this session:** `tests/unit/test_corpus_semantic_fixtures.py`'s `README_SEVENTY_THREE` tuple (lines 67-95+) and its "sixty-nine" framing (module docstring lines 50-55, "THE CORPUS IS COMPLETE AS OF PHASE 1... asserted, not claimed") **must be updated** the moment a 70th fixture is added — this is a hardcoded count assertion, not something that infers from `corpus.yaml` automatically (explicitly by design, per the file's own comment: "Deriving them from the manifest would make this test a tautology"). RESEARCH.md's Pitfall 8 (lines 528-533) already names this file as needing an update; it does not appear in CONTEXT.md's or the orchestrator's given file list.

---

### Cluster 17 — `packages/csv-processor/pyproject.toml` (MODIFIED)

**Analog: itself.** Current `dependencies` line (line 15):
```toml
dependencies = ["dataplat", "click>=8.4,<9"]
```
Extend to `["dataplat", "click>=8.4,<9", "charset-normalizer>=3.4.9,<4", "chardet>=7.5.1,<8", "clevercsv>=0.8.5,<1"]`, matching the comment convention immediately above the existing `click` entry (lines 10-14) that explains *why* each dependency is direct rather than transitive. `06-RESEARCH.md` lines 187-196 already confirms these three are absent today and install cleanly at the pinned versions.

---

### Cluster 18 — Tests, grouped by shape

**Corpus-parametrized detector tests** (`tests/unit/detect/test_{filename,encoding,dialect,header,schema}.py`) — analog `tests/unit/test_corpus_semantic_fixtures.py` (95+ lines read): loads the manifest via `tools.corpus.manifest.load_manifest` + `tools.corpus.generators.generate_corpus` into a temp dir, then asserts against each fixture's own `expect:` block rather than restating expectations in Python:
```python
from tools.corpus.generators import generate_corpus
from tools.corpus.manifest import load_manifest

REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST = REPO_ROOT / "tests" / "fixtures" / "corpus.yaml"
```
The module docstring's own framing note applies directly: *"Phase 6's detector tests are a parametrised loop over these declarations"* (`corpus.yaml` line 17-18) — build one shared conftest fixture that parametrizes over every fixture whose `covers:` list includes the relevant requirement ID (e.g. `CSV-02`/`CSV-03` for `test_encoding.py`), per RESEARCH.md's own Wave 0 Gaps recommendation (line 772).

**StreamingStage tests** (`tests/unit/normalize/test_{dates,numeric,boolean_null,unicode}.py`) — analog `tests/unit/test_pipeline_errors.py` (full `_make_context()`/`_chunk()` helper pattern, lines 1-50+ read):
```python
def _make_context() -> PipelineContext:
    """Build a placeholder ``PipelineContext`` for stage/engine tests.

    Only ``run`` is populated with a real value; the remaining fields are
    untouched by any code exercised in this file.
    """
    return PipelineContext(
        run=RunContext(run_id=1, idempotency_key="test-run"),
        config=None,  # type: ignore[arg-type] -- unused by the code under test
        ...
    )
```
Reuse this exact placeholder-context pattern for testing each normalizer's `apply()` in isolation, without a real database/object-store/config.

**Hashing tests** (`tests/unit/schema/test_versioning.py`) — analog `tests/unit/test_config_hashing.py` (full file, 144 lines read): same-hash-twice, key-reordering-does-not-change-hash, one-field-change-changes-hash, and `isinstance`/length/version-constant assertions:
```python
def test_hashing_the_same_config_twice_returns_the_same_hash() -> None:
    cfg = _customers_config()
    first_hash, _ = hash_config(cfg)
    second_hash, _ = hash_config(cfg)
    assert first_hash == second_hash
```

**Evolution tests** (`tests/unit/schema/test_evolution.py`) — dual analog: `tests/unit/test_errors.py` (full file, 77 lines — the raise-path pattern, `pytest.raises(ValueError, match=...)`) for breaking-change assertions, plus `tests/unit/test_pipeline_errors.py`'s errors-as-values pattern for compatible-change assertions (assert a returned finding/proposal object, not an exception).

**Config-model tests** (`tests/unit/test_dataset_config_columns.py`) — analog `tests/unit/test_batching_config.py` (full file, 47 lines): a valid-document happy path plus a "required field omitted → `ValidationError`, loudly" test:
```python
def test_dataset_config_fails_loudly_when_batching_is_omitted() -> None:
    document_without_batching = {k: v for k, v in _VALID_DOCUMENT.items() if k != "batching"}
    with pytest.raises(ValidationError, match="batching"):
        DatasetConfig.model_validate(document_without_batching)
```
The new file should add the D-18 cross-validator test too: a `deduplication.keys` entry that names a column absent from (or not `business_key: true` in) `columns:` must fail validation.

**Compression tests** (`tests/unit/test_compression.py`) — analog `tests/unit/test_csv_chunking.py` (155 lines, streaming-reader test shape) for the `.gz` true-streaming assertions, plus `tests/property/test_chunking_properties.py`'s Hypothesis-over-`io.BytesIO` pattern (full file, 96 lines read) for a bounded-memory property test.

**Property tests** (`tests/property/test_determinism.py`, `test_dst_correctness.py`) — analog `tests/property/test_chunking_properties.py` in full:
```python
@settings(max_examples=200)
@given(table=_csv_table(), chunk_size=st.integers(min_value=1, max_value=10))
def test_chunking_preserves_record_set_and_order(...) -> None:
    ...
```
The `@settings(max_examples=...)` budget-comment convention (lines 58-61, citing the project's "~90-second feedback-latency budget") should carry over; `test_dst_correctness.py` should additionally use the already-verified `classify_naive_local` function and `st.datetimes(timezones=..., allow_imaginary=True)` strategy from `06-RESEARCH.md` Code Examples (lines 546-602), which are proven to reproduce every value in corpus fixture 55 exactly.

**Integration schema-resolution test** (`tests/integration/test_schema_resolution.py`) — analog `tests/integration/test_config_registry.py` (70+ lines read):
```python
@pytest.fixture(scope="module")
def registry(migrated_dsn: str) -> Iterator[ConfigRegistry]:
    with create_pool(migrated_dsn) as pool:
        yield ConfigRegistry(pool)

def test_sync_creates_dataset_and_first_version(
    registry: ConfigRegistry, customers_config: DatasetConfig, migrated_dsn: str,
) -> None:
    record = registry.sync("customers", customers_config)
    assert record.is_new is True
    assert record.version == 1
```
Depends on the same session-scoped `migrated_dsn` fixture from `tests/integration/conftest.py` (lines 134-146) — no conftest changes needed (Cluster 14). Tests run in file order against one shared dataset row, matching this file's own documented narrative-style execution order (module docstring lines 6-10).

## Shared Patterns

### Pydantic config model shape
**Source:** `dataplat/config/model.py` (every class), lines 3-24 (module docstring) + lines 48, 68, 85, 102, 128 (`model_config = ConfigDict(extra="forbid", frozen=True)`)
**Apply to:** every new Pydantic model this phase adds (`ColumnContract`, `FilenameMaskConfig`, `NormalizationConfig`, schema-evolution-policy fields).
```python
model_config = ConfigDict(extra="forbid", frozen=True)
```
`extra="forbid"` turns a config typo into a validation-time error; `frozen=True` supports the platform's determinism constraint. No exceptions found anywhere in the codebase.

### "Strings not enums" for config-facing vocabularies
**Source:** `dataplat/config/model.py` lines 10-15 ("Strategy/source fields... are plain `str`, resolved through string-keyed registries elsewhere... never a Python `Enum`"); confirmed by `grep -rn "StrEnum\|(Enum)\|Literal\["` across `packages/dataplat/src/dataplat/` returning zero matches.
**Apply to:** `EncodingDetection.source`, schema-evolution policy values (`evolve`/`freeze`/`discard_row`/`discard_value`), `derived_from`/`compatibility` columns in `meta.schema_versions` (already `sa.Text()` in the RESEARCH.md-verified migration draft, app-validated, not a native Postgres enum — matches every existing migration's convention, confirmed against `migrations/0001`/`0004`, neither of which uses a `CHECK` constraint or native `ENUM` type for any status-like column).

### `hash_version` companion column discipline
**Source:** `dataplat/config/hashing.py` line 30 (`CONFIG_HASH_VERSION`), `migrations/0001` line 73 (`hash_version` column, `server_default=sa.text("1")`), `dataplat/discovery.py` line 53 (`_FILE_HASH_VERSION`).
**Apply to:** `schema/versioning.py`'s new hash constant + `meta.schema_versions.hash_version` column in migration 0009 (already present in the RESEARCH.md-verified draft, line 647).

### Errors-as-values vs. run-fatal raise — the QUAL-03 split
**Source:** `dataplat/models/record.py` (`RejectedRecord`/`StageResult`) for row/proposal-level; `dataplat/errors.py` (`DataPlatformError` hierarchy) for run-fatal. Module docstring of `errors.py` (lines 9-13): *"Row-level problems never raise any of these — a malformed row becomes a `RejectedRecord`... instead."*
**Apply to:** every detector (raises a `SourceError` subclass on total failure, e.g. undecodable file) and `schema/evolution.py` (compatible = value/proposal, breaking = raise `IncompatibleSchemaError`).

### `context: dict[str, object]` + reserved-key guard
**Source:** `dataplat/errors.py` lines 39-72 (`DataPlatformError.__init__`), reserved keys `error_type`/`error_message` (WR-03).
**Apply to:** every new `SourceError`/`SchemaError` raise site — populate `context` with the diagnostic code and relevant identifiers (file key, column name, row number), never invent a second structured-detail mechanism.

### Package marker (`__init__.py`) convention — docstring only, no re-exports
**Source:** `dataplat/config/__init__.py`, `dataplat/sources/__init__.py`, `dataplat/load/publish/__init__.py` — all three read in full, all three state the identical rule in their own words:
```python
"""...
Callers import from the submodule directly, e.g.
``from dataplat.sources.protocol import Source`` — this package marker
re-exports nothing, matching ``dataplat/config/__init__.py``'s shallow
re-export convention.
"""

from __future__ import annotations
```
**Apply to:** `csv_processor/detect/__init__.py`, `dataplat/normalize/__init__.py`, `dataplat/schema/__init__.py` — each should be a docstring + `from __future__ import annotations` only, explicitly naming this convention and citing one existing `__init__.py` as precedent (the existing files each cite each other, forming a chain — pick any one).

### Docstring convention — Google-style Args/Returns/Raises, "why" over "what"
**Source:** universal across every file read this session — e.g. `discovery.py::discover_files` (lines 98-149), `objectstore.py::open_text_stream` (lines 42-65), `config/registry.py::sync` (lines 110-126). Every public function/class docstring cites the requirement ID, decision ID (`D-NN`), or architecture-doc section driving a non-obvious choice, not just parameter types.
**Apply to:** every new function in this phase — a bare parameter-type docstring is inconsistent with the rest of the codebase's documentation depth.

### Diagnostic-vocabulary vs. legacy `error_type` naming — a real inconsistency to resolve, not copy
**Source:** `pipeline/engine.py` line 93, `error_type="RAGGED_ROW"` (SCREAMING_SNAKE_CASE) vs. `corpus.yaml`'s `quarantine_reason` values (kebab-case, e.g. `"field-exceeds-max-field-bytes"`, D-24's adopted vocabulary for `diagnostics.py`).
**Flag to planner:** `RAGGED_ROW` predates D-24's corpus-derived catalog and uses a different casing convention. D-25 says the vocabulary is "unified across row-level and file/run-level failures... one shared catalog" — decide explicitly whether `RAGGED_ROW` gets renamed to match the corpus's kebab-case convention (a breaking change to any code/test asserting the literal string, e.g. `tests/unit/test_pipeline_errors.py` lines 102, 115) or is grandfathered as a documented pre-existing exception. Do not silently introduce a second casing convention alongside it without addressing this.

## No Analog Found

Files/behaviors with no close precedent in the codebase — planner should lean on `06-RESEARCH.md`'s verified library sketches instead of a codebase analog:

| File/Behavior | Role | Data Flow | Reason |
|---|---|---|---|
| `csv_processor/detect/filename.py` (mask compiler logic itself) | utility | transform | No filename-mask parsing exists anywhere in this codebase; RESEARCH.md Architecture Patterns Pattern 3 (lines 365-416) is the verified reference instead |
| `csv_processor/detect/encoding.py` (BOM/charset-normalizer/chardet combination logic itself) | utility | transform | No encoding detection exists yet (Phase 3 hardcoded UTF-8); RESEARCH.md Pattern 2 (lines 300-363) is the verified, pitfall-corrected reference |
| `csv_processor/detect/dialect.py` (clevercsv wrapping + single-column guard) | utility | transform | No dialect detection exists yet; RESEARCH.md Pitfall 1 (lines 479-484) is the verified guard logic |
| `csv_processor/detect/header.py` (header/metadata/footer scoring) | utility | transform | No header detection exists yet (Phase 3 hardcoded row 0); STACK.md's `detector/header.py` design (cited in RESEARCH.md phase_requirements CSV-07/08) is descriptive, not yet code |
| `dataplat/schema/evolution.py` (the dlt 3×4 compatible/breaking classification matrix itself) | service | transform | Genuinely new domain logic; ROADMAP's plan guidance names the matrix shape but no prior phase implements any classification logic to model against |
| `csv_processor/compression.py` (`.zip` in-memory-buffer-then-stream exception) | utility | file-I/O | The `.gz` half has a direct analog (`objectstore.py`); the `.zip` half's seek-requirement workaround is a new pattern, fully verified live in RESEARCH.md Pattern 4/Pitfall 3 (lines 434-455, 493-498) but with no codebase precedent |

## Metadata

**Analog search scope:** `packages/csv-processor/src/csv_processor/`, `packages/dataplat/src/dataplat/` (all subpackages), `migrations/versions/`, `configs/`, `tests/unit/`, `tests/property/`, `tests/integration/`, `tests/fixtures/corpus.yaml`, `tools/corpus/`, `setup.cfg`, both `pyproject.toml` files.
**Files scanned (read in full or targeted-range):** 34 source/config/migration files, 3 corpus-tooling files, 1 fixture manifest (2199 lines, targeted reads), 9 test files.
**Pattern extraction date:** 2026-08-15
