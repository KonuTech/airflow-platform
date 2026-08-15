# Phase 6: Universal CSV Engine, Schema Contracts & Normalization - Context

**Gathered:** 2026-08-15
**Status:** Ready for planning

<domain>
## Phase Boundary

Real-world messy CSV files parse, type, version and normalize correctly — or fail with a named diagnostic — and nothing is ever silently coerced (ROADMAP goal). Concretely: filename mask/regex parsing (CSV-01); encoding detection with confidence scores (CSV-02/03); CSV dialect detection — delimiter, quote, escape, line ending (CSV-04/05/06); header/metadata/footer detection (CSV-07/08); explicit-format date validation (CSV-09); locale-aware numeric/boolean/NULL normalization (CSV-10); `.gz`/`.zip`/multi-part support (CSV-11); Unicode NFC normalization before hashing (CSV-12); conservative type inference plus explicit YAML data contracts (SCHEMA-01/02); schema versioning, hashing, and compatible-vs-breaking evolution policy (SCHEMA-03/04/05); historical-schema reprocessing (SCHEMA-06); configurable streaming batch size and max field/row length (LOAD-07); and the corresponding unit/property test coverage (QUAL-04/12/16/17).

This phase replaces Phase 3's deliberately minimal `csv_processor.Source` (hardcoded UTF-8/comma/header-row-0, "no detection lives here — that is Phase 6's `csv_processor/detect/` territory") with the real detection engine. `tests/fixtures/corpus.yaml`'s 69 already-committed fixtures, each with an `expect:` block, are this phase's test oracle — largely written before this phase began.

**Out of scope** — belongs to other phases: quarantine strategy execution / `FAIL_FILE`/`REJECT_RECORD`/`QUARANTINE_*` routing and re-drive (VALID-03/08, Phase 8); actual DDL migration of a target table in response to a schema-evolution proposal (no phase currently owns auto-migration — it stays a human-authored Alembic revision, by design, per D-01); the general `config_policy` replay knob (`AS_OF_LOGICAL_DATE`/`LATEST`/`PINNED`, Phase 9-or-later); deduplication strategy implementations beyond what already exists (Phase 9); CDC/SCD (Phase 10); control-total/referential-integrity validation (VALID-06/07, Phase 8/9).

</domain>

<decisions>
## Implementation Decisions

### Schema Evolution Policy (SCHEMA-04/05)
- **D-01:** A COMPATIBLE change (a new column appears) is **detect + record only** — never auto-DDL. The file still loads successfully using its known columns; the new column is classified compatible and captured as a proposal in `meta.schema_versions`, but its values are **not persisted anywhere** in the target table until a human adds a real Alembic migration and updates the contract. Rationale: `.planning/research/FEATURES.md` line 142 names auto-ALTER as an explicit anti-feature ("a single bad file permanently widens the warehouse... rollback is manual"); the recommended alternative is exactly "a proposal + alert, not an ALTER." Two alternatives were explicitly considered and **rejected**: auto-widening the target table via `ALTER TABLE ADD COLUMN`, and stashing unmapped values in a catch-all `_unmapped_fields jsonb` column.
- **D-02:** A BREAKING change (business-key rename, column disappearance, or data-type retype — see D-04) makes the **whole file fail, nothing loads**. Raises `IncompatibleSchemaError` (already named in `ARCHITECTURE.md`'s exception hierarchy, §4.5) before any row is staged. Rationale: "reported... never silently adapted to" taken literally — no row from a breaking file reaches the target table under a guessed mapping. Rejected alternatives: best-effort load under the old contract (risks silently-wrong values), and classify-but-no-run-consequence (would leave SCHEMA-04/05 passing on paper with no real enforcement until Phase 8 exists).
- **D-03:** Default policy values live in **`configs/defaults.yaml`** (today only `config_schema_version: 1`), inherited by every dataset via the loader's existing shallow-merge; a dataset overrides only if it needs different behavior. Matches the merge mechanism the config system was already designed around, even though nothing exercises it yet.
- **D-04:** A column **disappearing** (present historically, absent from this file) and a column's **data type changing** (e.g. int → decimal) are **both classified breaking (freeze)** by default — same treatment as a rename. Only "a genuinely new column appears" is evolve; every other structural change freezes the file.
- **D-05:** A breaking classification in one file's task **never blocks sibling files** in the same batch/run — matches Phase 4's existing architecture (each discovered file is already its own independent Dynamic-Task-Mapping unit with independent staging). No new run-level gating logic is needed.
- **D-06:** **No new developer tooling** to surface pending compatible-schema-evolution proposals this phase. `meta.schema_versions` is SQL-queryable directly, consistent with the platform's "lineage is queryable by SQL" philosophy (OBS-07). A convenience CLI/make target (mirroring Phase 5's `make vault-audit-tail`) was considered and deferred — see Deferred Ideas.

### Filename Mask Syntax (CSV-01)
- **D-07:** Mask syntax is **strptime-style named tokens** — e.g. `{dataset}_{country}_{business_date:%Y%m%d}_{seq:03d}.csv` — not raw named-group regex, not a token+regex-escape-hatch hybrid. Chosen for human authorability/reviewability over raw regex expressiveness, consistent with this project's other hand-authored YAML contracts.
- **D-08:** Individual facets can be marked **optional within one mask using bracket syntax** — e.g. `[_{seq:03d}]`. Matches CSV-01's literal "where present" wording and real delta-vs-snapshot delivery shapes (a dataset that sometimes has a sequence number and sometimes doesn't).
- **D-09:** A file that doesn't match its dataset's configured mask **at all** is **rejected with a named diagnostic** — not processed with the unmatched facets left null. Matches the phase goal's own wording ("parse correctly — or fail with a named diagnostic").
- **D-10:** Filename masks are **opt-in per dataset**. `customers.yaml` does **not** declare one. Reason: `scripts/ingest-demo.py` (line ~684) and the existing E2E fixtures currently upload files shaped like `customers/<basename>-<unix-timestamp>.csv` — no dataset/country/date/seq structure at all. Forcing a mask onto `customers` now would require rewriting working Phase 4 infrastructure for a capability the one real dataset doesn't actually need yet. CSV-01 still ships as a real, corpus-tested capability for any dataset that *does* opt in.
- **D-11:** The filename's extracted `business_date` facet (for any dataset that declares a mask) is a **fallback only** — consulted strictly when a file's data/content carries no derivable date, per `PITFALLS.md` line 683's literal priority order ("the business date comes from the data, in priority order — filename mask..."). It never overrides a data-derived business date. Rejected alternatives: pure cross-check/anomaly-flag-only, and pure lineage-metadata-only (never consulted by derivation logic at all) — both under-use the priority-order wording.

### Locale/Normalization Profile Scope (CSV-10, CSV-12)
- **D-12:** **One locale/normalization profile per dataset** (decimal separator, thousands separator, currency/percent handling, boolean tokens) — not per-column overridable. Covers the existing corpus's decimal/NBSP fixtures (06, 43, 68) as-is; no evidence in this project of a genuinely mixed-locale-per-column file.
- **D-13:** Locale fields are **explicit per dataset — no named-preset registry** (no `locale: pl-PL` shorthand). Matches the project's `extra="forbid"`, nothing-implicit philosophy; avoids building preset-registry machinery for a platform with one real dataset today.
- **D-14:** Default NULL-token set is **empty string only**. Any dataset needing `N/A`, `NULL`, `-`, `NA`, etc. treated as NULL must declare that list explicitly in its own contract — no implicit platform-wide NULL-token list. Directly guards against CSV-10's named risk ("1/0 must never become boolean absent evidence").
- **D-15:** Unicode **NFC normalization (CSV-12) is a fixed, non-configurable platform rule** — every string value is NFC-normalized before hashing/comparison, for every dataset, unconditionally. No per-dataset NFC/NFD/none choice. Directly protects Phase 9/10's hash-based dedup/SCD2 change detection from phantom differences.
- **Consequence, not a fresh decision:** `customers.yaml` needs no CSV-10 locale block at all today (no numeric/currency columns: `customer_id`, `name`, `country`, `birth_date`, `event_ts`), but will still need explicit `strptime` date-format declarations for `birth_date`/`event_ts` (CSV-09) — this follows mechanically from the already-locked "never `dateutil.parser.parse`, explicit format lists" rule (STACK.md §F), not a new decision made in this discussion.

### Historical Schema Resolution (SCHEMA-06)
- **D-16:** A file/run is matched to its historical schema version by **re-deriving the file's structure and hash-matching against `meta.schema_versions` history** for that dataset — self-contained, and independent of whether the dataset declares a filename mask (which `customers` currently doesn't, per D-10). Rejected alternatives: filename version-facet-as-authoritative (would leave the mechanism unexercised by the platform's only real dataset), and explicit-backfill-parameter-only (doesn't satisfy SCHEMA-06 for the ordinary, non-backfill case of an old file arriving late).
- **D-17:** Phase 6 builds **only this specific hash-match mechanism** — not the general 3-way `config_policy` replay knob (`AS_OF_LOGICAL_DATE`/`LATEST`/`PINNED`) that `ARCHITECTURE.md` §5.4 designed but never implemented. That knob is not named in any Phase 6 success criterion and stays a documented future capability for whichever phase (likely Phase 9's backfill work) first needs a human-selectable replay policy.

### Column Contract Shape (SCHEMA-02)
- **D-18:** A new `columns:` section in `DatasetConfig` becomes the **source of truth** for per-column type, nullability, required-ness, business-key marking and semantics. The existing `DeduplicationConfig.keys` field is **kept unchanged** (no rework of Phase 4's working dedup/merge code) but a new Pydantic model validator enforces that every name in `deduplication.keys` must reference a column marked `business_key: true` in `columns:` — the two can never silently disagree. Rejected alternative: fully removing `deduplication.keys` and deriving it from `columns:` (touches already-working Phase 4 code for no behavioral gain).
- **D-19:** A column's "semantics" (SCHEMA-02's literal wording) is expressed as a **free-text description field now**; controlled tags (e.g. `pii: true`, `role: identifier`) are **deferred** — see Deferred Ideas. Matches this project's standing instruction against building for hypothetical future requirements: no PII exists in this synthetic-corpus platform today, so there is nothing for a controlled vocabulary to act on yet.
- **D-20:** `required` (must the column appear in the file structurally) and `nullable` (can a present column's value be empty) are **two distinct fields**, not collapsed into one. A column that's `required: false` and absent from a file is exactly the "column disappearance" case already classified breaking/freeze under D-04.

### Compression/Archive Scope (CSV-11, Gap 13)
- **D-21:** Exactly **one CSV per archive** for both `.gz` and `.zip` — no multi-member zip support. A `.zip` is treated as just another compression wrapper around a single file, the same shape `.gz` already has via corpus fixture `61_gzipped.csv.gz`.
- **D-22:** Decompression must be **streaming, in-line over the compressed byte stream** — never by decompressing to a temp file first. This wraps the same `io.RawIOBase`/`io.BufferedReader` adapter pattern Phase 3 already built for boto3's `StreamingBody` (see `03-CONTEXT.md` D-01), extended with a decompression layer (`gzip.GzipFile`/`zipfile.ZipFile` reading incrementally), so LOAD-07's bounded-memory guarantee holds through the compression layer too. This was the user's explicit correction during discussion — do not implement "download and decompress to disk" even as an initial/simple version.
  - **D-22a (resolved post-research, 2026-08-15):** `06-RESEARCH.md` verified live that `.zip` cannot honor a pure single-pass stream the way `.gz` can — this is a structural property of the ZIP format (its member index sits at the archive's end, so `zipfile.ZipFile` requires a seekable stream and raises `BadZipFile` otherwise; `gzip.GzipFile` has no such requirement and needs no exception). Resolution, confirmed with the user: `.gz` stays a true zero-compromise stream exactly as D-22 states. For `.zip` only, the compressed archive bytes are buffered into memory before opening (bounded by the archive's *compressed* size — e.g. ~5.6KB for a ~19.7KB CSV in research's test — never the decompressed CSV content, and never disk). Once open, the CSV content still streams out in small chunks exactly like `.gz`. This satisfies D-22's actual intent (bounded memory, nothing written to disk) while being honest that `.zip`'s archive container bytes specifically are buffered, not purely streamed — a distinction the original discussion had no visibility into. Rejected alternatives: a custom ranged-read seekable wrapper over MinIO `GetObject` (no new dependency, meaningfully more code for zero current consumers), and the `stream-unzip` third-party library (avoids buffering entirely, but a new dependency for a capability no real dataset uses today).

### Diagnostic Code Convention
- **D-23:** Every detector/validation failure carries a **stable, documented diagnostic code** (a small catalog) — not just a message string and a free-form context dict. Matches the phase goal's literal wording ("fail with a **named** diagnostic").
- **D-24:** The catalog is **built from the corpus's already-declared strings** — `tests/fixtures/corpus.yaml`'s existing `quarantine_reason` and similar values (e.g. `"nul-byte-in-text-field"`, `"field-exceeds-max-field-bytes"`, `"undecodable-bytes"`) *are* the catalog, not a separately/independently designed vocabulary. A test should assert every code the corpus expects is actually raised somewhere, and every code raised in code exists in the corpus or catalog — so the oracle and the implementation can never quietly drift apart.
- **D-25:** The diagnostic vocabulary is **unified across row-level and file/run-level failures** — one shared catalog, not two parallel systems. `RejectedRecord.error_type` (Phase 3, already documented as "a short, stable, machine-readable reason code, e.g. `RAGGED_ROW`") is the row-level half; new `SourceError`/`SchemaError` exception context codes are the file/run-level half; both draw from the same vocabulary D-24 establishes.

### Claude's Discretion
- Exact type-token vocabulary for `columns:[].type` (a small closed set — string/integer/decimal/date/timestamp/boolean — implied by CSV-09/CSV-10/SCHEMA-01's requirement text, not independently re-litigated in this discussion).
- Exact regex-anchoring semantics for filename masks (whole-string anchor vs. prefix match) — no real dataset exercises this yet since `customers` doesn't declare one (D-10).
- Whether decompression dispatch (`.gz`/`.zip` detection) uses file extension or magic-byte sniffing.
- Whether a corrupted/truncated-archive corpus fixture is added proactively now or only once encountered — QUAL-08's existing "grow the corpus as cases are discovered" policy covers either path.
- Exact module/location for the diagnostic-code catalog (e.g. a new `dataplat/diagnostics.py` frozen set/`Enum` vs. constants colocated with each exception subclass) and whether one exception class maps to many codes via its `context` dict (the natural fit, given the existing `DataPlatformError.context` pattern) or needs a dedicated field.
- Whether a schema-authoring bootstrap tool (a CLI that infers a draft `columns:` block from a sample file, per `FEATURES.md`'s "inference is bootstrap-only" framing) gets built — not required by any named requirement; build only if planning finds it cheap alongside the inference engine itself.
- Multi-row/hierarchical header handling stays **detected and rejected with a clear diagnostic** — already locked by `ROADMAP.md`'s Phase 6 plan guidance itself ("no canonical flattening exists, and v1 does not invent one"), not re-decided in this discussion.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase scope, requirements and success criteria
- `.planning/ROADMAP.md` § "Phase 6: Universal CSV Engine, Schema Contracts & Normalization" (lines 316–343) — goal, 5 success criteria, requirements list, and the plan-guidance block (Wave C‖D parallelization opportunity across the 5 detectors, dlt 3×4 schema-contract matrix, the hard ordering edge "normalization MUST precede hashing," multi-row-header reject-not-flatten, encoding confidence never claims determinism).
- `.planning/REQUIREMENTS.md` — CSV-01 through CSV-12 (lines 76–88), SCHEMA-01 through SCHEMA-07 (90–98), LOAD-07 (120), QUAL-04/12/16/17 (184, 192, 196–197) — full text of every Phase 6 requirement.

### Research — schema design, error hierarchy, config system
- `.planning/research/ARCHITECTURE.md` line 232 — `meta.schema_versions` table shape: `schema_version_id`, `dataset_id`, `version`, `schema_hash`, `columns jsonb` (ordered: name/type/nullable/position/format), `derived_from ∈ {CONTRACT, INFERRED}`, `compatibility ∈ {COMPATIBLE, BREAKING}`, `breaking_changes jsonb`, `valid_from`/`valid_to`, `UNIQUE(dataset_id, version)`.
- `.planning/research/ARCHITECTURE.md` §4.5 "Errors, threaded" (lines 545–583) — the full exception hierarchy this phase adds branches to: `SourceError` (`FileInspectionError`, `FilenameParsingError`, `EncodingDetectionError`, `CsvDialectDetectionError`, `CsvParsingError`) and `SchemaError` (`SchemaValidationError`, `IncompatibleSchemaError` — "§13 BREAKING change under a strict policy," which D-02 makes this dataset's only policy).
- `.planning/research/ARCHITECTURE.md` §4.4 "Config-not-code" (lines 516–544) — the worked `transactions.yaml` example `DatasetConfig` extends from.
- `.planning/research/ARCHITECTURE.md` §5.4 (line 629) — the `config_policy` knob (`AS_OF_LOGICAL_DATE`/`LATEST`/`PINNED`), explicitly **not** built this phase per D-17.
- `.planning/research/ARCHITECTURE.md` Anti-Patterns AP2/AP3/AP8 (lines 1323–1358) — typed staging tables, filename-as-identity, deferring the metadata schema.

### Research — feature calibration and anti-features
- `.planning/research/FEATURES.md` line 142 — "Auto-creating/auto-ALTERing target tables from inferred schema" anti-feature: "DDL becomes an uncontrolled side effect of arriving data... Engine validates against the target, never migrates. Schema evolution produces a *proposal + alert*, not an `ALTER`." Directly grounds D-01.
- `.planning/research/FEATURES.md` line 143 — the argument for the dlt 3×4 matrix shape over a boolean `strict_mode`.
- `.planning/research/FEATURES.md` line 106 — filename mask parsing gap note: "§8's own warning: a date in a filename is not automatically the business date." Grounds D-11.
- `.planning/research/FEATURES.md` line 79 — "Schema drift detection and reporting (not auto-adaptation)... Contract is the default; inference is bootstrap-only."
- `.planning/research/SUMMARY.md` line 132 — the dlt 3×4 schema-contract matrix citation (`{tables, columns, data_type} × {evolve, freeze, discard_row, discard_value}`).

### Research — pitfalls
- `.planning/research/PITFALLS.md` line 683 — "the business date comes from **the data**, in priority order — filename mask..." — the literal priority-order wording D-11 follows.
- `.planning/research/PITFALLS.md` lines 36–58 — the 15-item decision table; none are Phase-6-specific beyond #15 (fixtures generated from a seed), already satisfied by Phase 1.

### Research — stack pins for CSV/encoding/dialect/dates/numerics
- `.claude/CLAUDE.md` §F "Python ETL Library" / `.planning/research/STACK.md` §F — `charset-normalizer` 3.4.9 + `chardet` 7.5.1 behind a BOM sniff (encoding detection), `clevercsv` 0.8.5 for dialect detection only (never the full-file parse), `datetime.strptime` never `dateutil.parser.parse` (grounds the CSV-09 date-format consequence noted under Locale decisions), `decimal.Decimal` for numerics, and the explicit instruction that a contract whose `delimiter` equals a column's `decimal_separator` must be rejected at contract-validation time.

### Existing test oracle — the corpus IS the specification
- `tests/fixtures/corpus.yaml` — 69 declared fixtures with `expect:` blocks that are this phase's detector test oracle. Specifically: the `quarantine_reason`/diagnostic strings D-24 adopts as the diagnostic-code catalog (fixtures `32_nul_bytes.csv`, `39_utf8_invalid_sequences.csv`, `67_row_exceeding_field_size_limit.csv`, and others); the compression fixture `61_gzipped.csv.gz` D-21/D-22 extend; the multi-part discovery-grouping fixture `62_multipart_split` ("discovery must group the parts into one dataset before parsing").
- `docs/adr/0005-fixture-corpus-generated-from-a-seed.md` — why the corpus is generated, not committed en masse (QUAL-08).
- `tools/corpus/` (`generators.py`, `manifest.py`, `digests.py`) — the seeded generator framework any new corpus fixture (e.g. a `.zip` fixture, a corrupted-archive fixture) must extend.

### Prior-phase decisions this phase must respect, not re-decide
- `.planning/phases/03-dataplat-core-library-metadata-control-plane/03-CONTEXT.md` D-01 — the minimal `csv_processor.Source` (hardcoded UTF-8/comma/header-row-0) this phase replaces with real detection; explicitly deferred "Full CSV detection engine (encoding/dialect/header/footer detection, type inference, normalization) — Phase 6" in that phase's own Deferred Ideas.
- `.planning/phases/03-dataplat-core-library-metadata-control-plane/03-CONTEXT.md` D-06 — the exception-hierarchy-grows-with-its-first-raise-site rule this phase's `SourceError`/`SchemaError` additions follow.
- `.planning/phases/04-vertical-slice-csv-to-analytical-postgresql/04-CONTEXT.md` D-13 — `customers.yaml`'s existing duplicate-file-content policy (`source.duplicate_policy: skip`), untouched by this phase's filename-mask work.
- `packages/dataplat/src/dataplat/config/model.py` module docstring — an explicit standing instruction already committed to the codebase: this phase adds `delimiter`/`decimal_separator` fields to `DatasetConfig` and **must** add the "do not confuse CSV delimiters with decimal separators" collision validator at that point, not before.

### Existing code this phase extends
- `packages/csv-processor/src/csv_processor/source.py` — `CsvSource`/`CsvRecordStream`/`chunked_records`, the hardcoded-dialect reader this phase's real detection engine replaces (`ENCODING`/`DIALECT` module constants, `FIELD_SIZE_LIMIT` → becomes a per-dataset contract field per LOAD-07).
- `packages/dataplat/src/dataplat/config/model.py` — `DatasetConfig`/`SourceConfig`/`DeduplicationConfig`/`LoadConfig`/`BatchingConfig`, all `ConfigDict(extra="forbid", frozen=True)`; this phase adds `columns:`, an opt-in `filename:`, a `normalization:` block, and schema-evolution-policy fields.
- `packages/dataplat/src/dataplat/errors.py` — `DataPlatformError`/`ConfigurationError`/`StorageError`/`SecretResolutionError`; this phase adds the `SourceError` and `SchemaError` families.
- `packages/dataplat/src/dataplat/models/record.py` — `RejectedRecord.error_type`, already documented as "a short, stable, machine-readable reason code" — the row-level half of D-25's unified diagnostic vocabulary.
- `packages/dataplat/src/dataplat/models/report.py` — `ValidationResult.rule_id`, the Phase-8-deferred "minimal D-05 shape" (per `03-CONTEXT.md`) this phase's detector findings likely populate.
- `configs/datasets/customers.yaml`, `configs/defaults.yaml` — the one real dataset config and the platform-defaults file (currently only `config_schema_version: 1`) that D-03's schema-evolution defaults and D-13's locale fields land in.
- `scripts/ingest-demo.py` (line ~684), `tests/fixtures/slice-corpus.yaml` — the actual current `customers` filename shape (`customers/<basename>-<unix-timestamp>.csv`) and fixture names (`customers_small.csv`, `customers_large.csv`), the reason D-10 makes filename masks opt-in rather than mandatory.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- The `io.RawIOBase`/`io.BufferedReader` adapter Phase 3 built to make boto3's `StreamingBody` acceptable to `io.TextIOWrapper` (see `03-CONTEXT.md` D-01) — D-22's streaming decompression wraps this same seam with a `gzip.GzipFile`/`zipfile.ZipFile` layer, rather than inventing a new I/O adapter pattern.
- `dataplat.errors.DataPlatformError.context: dict[str, object]` — already the mechanism a diagnostic code (D-23/D-24) most naturally lands in, without a new field.
- `dataplat.models.record.RejectedRecord.error_type` — already the row-level diagnostic-code mechanism (D-25); this phase's row-level detector findings should populate it with the same catalog file/run-level exceptions use.
- `dataplat.config.loader`'s existing shallow-merge of `configs/datasets/<name>.yaml` over `configs/defaults.yaml` — D-03's schema-evolution-policy defaults and D-13's explicit locale fields both ride this existing mechanism; no new merge logic needed.

### Established Patterns
- Every `dataplat.config.model` model is `ConfigDict(extra="forbid", frozen=True)` — the new `columns:`/`filename:`/`normalization:`/policy sections must follow the same shape.
- "config not code" — strategy/source fields are plain `str` resolved through registries, never a Python `Enum` baked into the model (README §65) — worth checking whether the schema-evolution policy values (`evolve`/`freeze`/`discard_row`/`discard_value`) should follow the same string-key convention rather than a literal enum, for consistency with `SourceConfig.type`/`DeduplicationConfig.strategy`/`LoadConfig.strategy`.
- Exception subclasses are added by the phase that first raises them, never speculatively (Phase 3 D-06) — this phase's `SourceError`/`SchemaError` additions are the next real instance of that rule, already anticipated by name in `ARCHITECTURE.md`.

### Integration Points
- `packages/csv-processor/src/csv_processor/` currently has only `__init__.py`, `cli.py`, `source.py` — the detection engine (`csv_processor/detect/` — filename, encoding, dialect, header/footer, schema inference) is new code, not a modification of existing detection logic (there is none yet).
- `meta.schema_versions` does not exist as a migration yet — this phase's Alembic migration is the first to create it, following `ARCHITECTURE.md`'s already-complete design (line 232) exactly, per the same "coherent design up front, incremental migration" discipline Phase 3 established (D-05 in `03-CONTEXT.md`).

</code_context>

<specifics>
## Specific Ideas

- **Streaming decompression is a hard requirement, stated explicitly by the user, not a nice-to-have:** "python libraries should allow to read compressed files without decompression" — i.e. `.gz`/`.zip` must be decompressed in-line over the byte stream (D-22), never by writing a fully-decompressed temp file first. This was a direct correction during discussion and should be treated as a locked constraint, not a suggestion — matches this project's existing bounded-memory streaming discipline (CSV-13, LOAD-07, the U3 spike).
- **The corpus's existing diagnostic strings are binding vocabulary, not just precedent:** D-24 means a raise site should reuse `tests/fixtures/corpus.yaml`'s own words (e.g. `"nul-byte-in-text-field"`) rather than inventing a differently-cased or differently-worded equivalent — treat a mismatch between a new diagnostic code and an existing corpus string as a bug, not a stylistic choice.
- **`customers` is the platform's only real dataset and deliberately exercises the narrower path** on two axes discussed this session: no filename mask (D-10) and no locale/normalization block (consequence noted under Locale decisions). Downstream planning should not assume `customers.yaml` needs updating as evidence that CSV-01/CSV-10 work — the corpus fixtures are that evidence; `customers.yaml` staying unchanged on those two axes is itself the correct outcome, not a gap.

</specifics>

<deferred>
## Deferred Ideas

- **Auto-widening the target table (`ALTER TABLE ADD COLUMN`) on a compatible schema change** — considered and explicitly rejected in D-01, not merely deferred. `FEATURES.md` names this an anti-feature; do not revisit unless that research finding itself is overturned.
- **A catch-all `_unmapped_fields jsonb` column to capture compatible-but-undeclared column values** — considered and explicitly rejected in D-01 in favor of "detect + record only, no persistence." Revisit only if losing those values in practice (not just in principle) turns out to matter for a real dataset.
- **Controlled-vocabulary column semantic tags** (`pii: true`, `role: identifier`, etc.) — deferred per D-19. Revisit once a real, non-hypothetical need for machine-actionable column semantics appears (e.g. an automated redaction pass) — none exists today since the corpus is synthetic by construction.
- **Per-column locale overrides** (mixed decimal separators within one file) — deferred per D-12. Revisit only if a real dataset genuinely needs it; no corpus fixture or planned dataset currently does.
- **The general `config_policy` replay knob** (`AS_OF_LOGICAL_DATE`/`LATEST`/`PINNED`, designed in `ARCHITECTURE.md` §5.4 but never built) — deferred per D-17. Belongs to whichever phase first needs a human-selectable replay policy — likely Phase 9's backfill work (INCR-05/06).
- **Multi-member `.zip` archive support** (several CSVs bundled in one archive, each becoming its own discovered file) — deferred per D-21. Revisit only if a real feed delivers bundled archives; the discovery-layer grouping this would need is architecturally similar to the already-proven multi-part (`62_multipart_split`) case, so it would not require new foundational work.
- **A convenience CLI/make target for pending schema-evolution proposals** (e.g. `make schema-proposals`) — deferred per D-06. Revisit if SQL-only querying of `meta.schema_versions` proves too painful in practice, following the developer-experience bar Phase 5 set with `make vault-audit-tail`.
- **A schema-authoring bootstrap CLI** (infer a draft `columns:` block from a sample file) — not requested, not required by any named requirement. Left to planning's discretion to build cheaply alongside the inference engine if it falls out naturally, but not a committed deliverable of this phase.

</deferred>

---

*Phase: 6-universal-csv-engine-schema-contracts-normalization*
*Context gathered: 2026-08-15*
