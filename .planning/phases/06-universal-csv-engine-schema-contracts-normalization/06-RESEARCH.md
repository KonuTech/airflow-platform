# Phase 6: Universal CSV Engine, Schema Contracts & Normalization - Research

**Researched:** 2026-08-15
**Domain:** CSV encoding/dialect/header detection, schema contracts & versioning, locale-aware normalization, streaming decompression, DST-correct timestamp handling
**Confidence:** HIGH

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**Schema Evolution Policy (SCHEMA-04/05)**
- **D-01:** A COMPATIBLE change (a new column appears) is **detect + record only** — never auto-DDL. The file still loads successfully using its known columns; the new column is classified compatible and captured as a proposal in `meta.schema_versions`, but its values are **not persisted anywhere** in the target table until a human adds a real Alembic migration and updates the contract. Rationale: `.planning/research/FEATURES.md` line 142 names auto-ALTER as an explicit anti-feature ("a single bad file permanently widens the warehouse... rollback is manual"); the recommended alternative is exactly "a proposal + alert, not an ALTER." Two alternatives were explicitly considered and **rejected**: auto-widening the target table via `ALTER TABLE ADD COLUMN`, and stashing unmapped values in a catch-all `_unmapped_fields jsonb` column.
- **D-02:** A BREAKING change (business-key rename, column disappearance, or data-type retype — see D-04) makes the **whole file fail, nothing loads**. Raises `IncompatibleSchemaError` (already named in `ARCHITECTURE.md`'s exception hierarchy, §4.5) before any row is staged. Rationale: "reported... never silently adapted to" taken literally — no row from a breaking file reaches the target table under a guessed mapping. Rejected alternatives: best-effort load under the old contract (risks silently-wrong values), and classify-but-no-run-consequence (would leave SCHEMA-04/05 passing on paper with no real enforcement until Phase 8 exists).
- **D-03:** Default policy values live in **`configs/defaults.yaml`** (today only `config_schema_version: 1`), inherited by every dataset via the loader's existing shallow-merge; a dataset overrides only if it needs different behavior. Matches the merge mechanism the config system was already designed around, even though nothing exercises it yet.
- **D-04:** A column **disappearing** (present historically, absent from this file) and a column's **data type changing** (e.g. int → decimal) are **both classified breaking (freeze)** by default — same treatment as a rename. Only "a genuinely new column appears" is evolve; every other structural change freezes the file.
- **D-05:** A breaking classification in one file's task **never blocks sibling files** in the same batch/run — matches Phase 4's existing architecture (each discovered file is already its own independent Dynamic-Task-Mapping unit with independent staging). No new run-level gating logic is needed.
- **D-06:** **No new developer tooling** to surface pending compatible-schema-evolution proposals this phase. `meta.schema_versions` is SQL-queryable directly, consistent with the platform's "lineage is queryable by SQL" philosophy (OBS-07). A convenience CLI/make target (mirroring Phase 5's `make vault-audit-tail`) was considered and deferred — see Deferred Ideas.

**Filename Mask Syntax (CSV-01)**
- **D-07:** Mask syntax is **strptime-style named tokens** — e.g. `{dataset}_{country}_{business_date:%Y%m%d}_{seq:03d}.csv` — not raw named-group regex, not a token+regex-escape-hatch hybrid. Chosen for human authorability/reviewability over raw regex expressiveness, consistent with this project's other hand-authored YAML contracts.
- **D-08:** Individual facets can be marked **optional within one mask using bracket syntax** — e.g. `[_{seq:03d}]`. Matches CSV-01's literal "where present" wording and real delta-vs-snapshot delivery shapes (a dataset that sometimes has a sequence number and sometimes doesn't).
- **D-09:** A file that doesn't match its dataset's configured mask **at all** is **rejected with a named diagnostic** — not processed with the unmatched facets left null. Matches the phase goal's own wording ("parse correctly — or fail with a named diagnostic").
- **D-10:** Filename masks are **opt-in per dataset**. `customers.yaml` does **not** declare one. Reason: `scripts/ingest-demo.py` (line ~684) and the existing E2E fixtures currently upload files shaped like `customers/<basename>-<unix-timestamp>.csv` — no dataset/country/date/seq structure at all. Forcing a mask onto `customers` now would require rewriting working Phase 4 infrastructure for a capability the one real dataset doesn't actually need yet. CSV-01 still ships as a real, corpus-tested capability for any dataset that *does* opt in.
- **D-11:** The filename's extracted `business_date` facet (for any dataset that declares a mask) is a **fallback only** — consulted strictly when a file's data/content carries no derivable date, per `PITFALLS.md` line 683's literal priority order ("the business date comes from the data, in priority order — filename mask..."). It never overrides a data-derived business date. Rejected alternatives: pure cross-check/anomaly-flag-only, and pure lineage-metadata-only (never consulted by derivation logic at all) — both under-use the priority-order wording.

**Locale/Normalization Profile Scope (CSV-10, CSV-12)**
- **D-12:** **One locale/normalization profile per dataset** (decimal separator, thousands separator, currency/percent handling, boolean tokens) — not per-column overridable. Covers the existing corpus's decimal/NBSP fixtures (06, 43, 68) as-is; no evidence in this project of a genuinely mixed-locale-per-column file.
- **D-13:** Locale fields are **explicit per dataset — no named-preset registry** (no `locale: pl-PL` shorthand). Matches the project's `extra="forbid"`, nothing-implicit philosophy; avoids building preset-registry machinery for a platform with one real dataset today.
- **D-14:** Default NULL-token set is **empty string only**. Any dataset needing `N/A`, `NULL`, `-`, `NA`, etc. treated as NULL must declare that list explicitly in its own contract — no implicit platform-wide NULL-token list. Directly guards against CSV-10's named risk ("1/0 must never become boolean absent evidence").
- **D-15:** Unicode **NFC normalization (CSV-12) is a fixed, non-configurable platform rule** — every string value is NFC-normalized before hashing/comparison, for every dataset, unconditionally. No per-dataset NFC/NFD/none choice. Directly protects Phase 9/10's hash-based dedup/SCD2 change detection from phantom differences.
- **Consequence, not a fresh decision:** `customers.yaml` needs no CSV-10 locale block at all today (no numeric/currency columns: `customer_id`, `name`, `country`, `birth_date`, `event_ts`), but will still need explicit `strptime` date-format declarations for `birth_date`/`event_ts` (CSV-09) — this follows mechanically from the already-locked "never `dateutil.parser.parse`, explicit format lists" rule (STACK.md §F), not a new decision made in this discussion.

**Historical Schema Resolution (SCHEMA-06)**
- **D-16:** A file/run is matched to its historical schema version by **re-deriving the file's structure and hash-matching against `meta.schema_versions` history** for that dataset — self-contained, and independent of whether the dataset declares a filename mask (which `customers` currently doesn't, per D-10). Rejected alternatives: filename version-facet-as-authoritative (would leave the mechanism unexercised by the platform's only real dataset), and explicit-backfill-parameter-only (doesn't satisfy SCHEMA-06 for the ordinary, non-backfill case of an old file arriving late).
- **D-17:** Phase 6 builds **only this specific hash-match mechanism** — not the general 3-way `config_policy` replay knob (`AS_OF_LOGICAL_DATE`/`LATEST`/`PINNED`) that `ARCHITECTURE.md` §5.4 designed but never implemented. That knob is not named in any Phase 6 success criterion and stays a documented future capability for whichever phase (likely Phase 9's backfill work) first needs a human-selectable replay policy.

**Column Contract Shape (SCHEMA-02)**
- **D-18:** A new `columns:` section in `DatasetConfig` becomes the **source of truth** for per-column type, nullability, required-ness, business-key marking and semantics. The existing `DeduplicationConfig.keys` field is **kept unchanged** (no rework of Phase 4's working dedup/merge code) but a new Pydantic model validator enforces that every name in `deduplication.keys` must reference a column marked `business_key: true` in `columns:` — the two can never silently disagree. Rejected alternative: fully removing `deduplication.keys` and deriving it from `columns:` (touches already-working Phase 4 code for no behavioral gain).
- **D-19:** A column's "semantics" (SCHEMA-02's literal wording) is expressed as a **free-text description field now**; controlled tags (e.g. `pii: true`, `role: identifier`) are **deferred** — see Deferred Ideas. Matches this project's standing instruction against building for hypothetical future requirements: no PII exists in this synthetic-corpus platform today, so there is nothing for a controlled vocabulary to act on yet.
- **D-20:** `required` (must the column appear in the file structurally) and `nullable` (can a present column's value be empty) are **two distinct fields**, not collapsed into one. A column that's `required: false` and absent from a file is exactly the "column disappearance" case already classified breaking/freeze under D-04.

**Compression/Archive Scope (CSV-11, Gap 13)**
- **D-21:** Exactly **one CSV per archive** for both `.gz` and `.zip` — no multi-member zip support. A `.zip` is treated as just another compression wrapper around a single file, the same shape `.gz` already has via corpus fixture `61_gzipped.csv.gz`.
- **D-22:** Decompression must be **streaming, in-line over the compressed byte stream** — never by decompressing to a temp file first. This wraps the same `io.RawIOBase`/`io.BufferedReader` adapter pattern Phase 3 already built for boto3's `StreamingBody` (see `03-CONTEXT.md` D-01), extended with a decompression layer (`gzip.GzipFile`/`zipfile.ZipFile` reading incrementally), so LOAD-07's bounded-memory guarantee holds through the compression layer too. This was the user's explicit correction during discussion — do not implement "download and decompress to disk" even as an initial/simple version. **Research correction (this document): the actual current `objectstore.py` has no hand-written adapter — see Common Pitfalls #4 — and `.zip` specifically cannot honor a pure single-pass read regardless of adapter shape, a structural ZIP-format constraint neither the discussion nor this decision's text anticipated — see Common Pitfalls #3 and Open Questions #1 for the scoped exception this research recommends.**

**Diagnostic Code Convention**
- **D-23:** Every detector/validation failure carries a **stable, documented diagnostic code** (a small catalog) — not just a message string and a free-form context dict. Matches the phase goal's literal wording ("fail with a **named** diagnostic").
- **D-24:** The catalog is **built from the corpus's already-declared strings** — `tests/fixtures/corpus.yaml`'s existing `quarantine_reason` and similar values (e.g. `"nul-byte-in-text-field"`, `"field-exceeds-max-field-bytes"`, `"undecodable-bytes"`) *are* the catalog, not a separately/independently designed vocabulary. A test should assert every code the corpus expects is actually raised somewhere, and every code raised in code exists in the corpus or catalog — so the oracle and the implementation can never quietly drift apart.
- **D-25:** The diagnostic vocabulary is **unified across row-level and file/run-level failures** — one shared catalog, not two parallel systems. `RejectedRecord.error_type` (Phase 3, already documented as "a short, stable, machine-readable reason code, e.g. `RAGGED_ROW`") is the row-level half; new `SourceError`/`SchemaError` exception context codes are the file/run-level half; both draw from the same vocabulary D-24 establishes.

### Claude's Discretion
- Exact type-token vocabulary for `columns:[].type` (a small closed set — string/integer/decimal/date/timestamp/boolean — implied by CSV-09/CSV-10/SCHEMA-01's requirement text, not independently re-litigated in this discussion).
- Exact regex-anchoring semantics for filename masks (whole-string anchor vs. prefix match) — no real dataset exercises this yet since `customers` doesn't declare one (D-10). **Research recommendation: whole-string anchor — see Architecture Patterns Pattern 3.**
- Whether decompression dispatch (`.gz`/`.zip` detection) uses file extension or magic-byte sniffing.
- Whether a corrupted/truncated-archive corpus fixture is added proactively now or only once encountered — QUAL-08's existing "grow the corpus as cases are discovered" policy covers either path.
- Exact module/location for the diagnostic-code catalog (e.g. a new `dataplat/diagnostics.py` frozen set/`Enum` vs. constants colocated with each exception subclass) and whether one exception class maps to many codes via its `context` dict (the natural fit, given the existing `DataPlatformError.context` pattern) or needs a dedicated field.
- Whether a schema-authoring bootstrap tool (a CLI that infers a draft `columns:` block from a sample file, per `FEATURES.md`'s "inference is bootstrap-only" framing) gets built — not required by any named requirement; build only if planning finds it cheap alongside the inference engine itself.
- Multi-row/hierarchical header handling stays **detected and rejected with a clear diagnostic** — already locked by `ROADMAP.md`'s Phase 6 plan guidance itself ("no canonical flattening exists, and v1 does not invent one"), not re-decided in this discussion.

### Deferred Ideas (OUT OF SCOPE)
- **Auto-widening the target table (`ALTER TABLE ADD COLUMN`) on a compatible schema change** — considered and explicitly rejected in D-01, not merely deferred. `FEATURES.md` names this an anti-feature; do not revisit unless that research finding itself is overturned.
- **A catch-all `_unmapped_fields jsonb` column to capture compatible-but-undeclared column values** — considered and explicitly rejected in D-01 in favor of "detect + record only, no persistence." Revisit only if losing those values in practice (not just in principle) turns out to matter for a real dataset.
- **Controlled-vocabulary column semantic tags** (`pii: true`, `role: identifier`, etc.) — deferred per D-19. Revisit once a real, non-hypothetical need for machine-actionable column semantics appears (e.g. an automated redaction pass) — none exists today since the corpus is synthetic by construction.
- **Per-column locale overrides** (mixed decimal separators within one file) — deferred per D-12. Revisit only if a real dataset genuinely needs it; no corpus fixture or planned dataset currently does.
- **The general `config_policy` replay knob** (`AS_OF_LOGICAL_DATE`/`LATEST`/`PINNED`, designed in `ARCHITECTURE.md` §5.4 but never built) — deferred per D-17. Belongs to whichever phase first needs a human-selectable replay policy — likely Phase 9's backfill work (INCR-05/06).
- **Multi-member `.zip` archive support** (several CSVs bundled in one archive, each becoming its own discovered file) — deferred per D-21. Revisit only if a real feed delivers bundled archives; the discovery-layer grouping this would need is architecturally similar to the already-proven multi-part (`62_multipart_split`) case, so it would not require new foundational work.
- **A convenience CLI/make target for pending schema-evolution proposals** (e.g. `make schema-proposals`) — deferred per D-06. Revisit if SQL-only querying of `meta.schema_versions` proves too painful in practice, following the developer-experience bar Phase 5 set with `make vault-audit-tail`.
- **A schema-authoring bootstrap CLI** (infer a draft `columns:` block from a sample file) — not requested, not required by any named requirement. Left to planning's discretion to build cheaply alongside the inference engine if it falls out naturally, but not a committed deliverable of this phase.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-------------------|
| CSV-01 | Filenames parsed via configurable masks/regex, extracting dataset/source/country/business date/version/batch/sequence where present, without assuming any date found is the business date | Architecture Patterns Pattern 3 (hand-rolled compiler, verified `parse` library gap); Code Examples (compiler sketch) |
| CSV-02 | UTF-8, UTF-8 BOM, UTF-16 LE/BE, Windows-1250/1252, ISO-8859 variants, ASCII all parse correctly | Architecture Patterns Pattern 2 (BOM table, verified live); Common Pitfalls #6, #9 |
| CSV-03 | Encoding detection returns an encoding with a confidence score, never claims determinism it does not have | Common Pitfalls #2 (verified chardet-confidence correction); Architecture Patterns Pattern 2; Open Questions #2 |
| CSV-04 | Comma, semicolon, pipe, tab, colon dialects all parse correctly | Don't Hand-Roll (clevercsv verified against fixture-68-shaped sample) |
| CSV-05 | Delimiter detection supported, overridable by contract | Standard Stack (clevercsv); existing STACK.md contract-override design, unchanged |
| CSV-06 | Quoted delimiters, escaped quotes, multiline fields, inconsistent quoting via a real parser | Common Pitfalls #1 (verified clevercsv single-column crash + fix); existing stdlib `csv` design, unchanged |
| CSV-07 | Header detection: present, absent, header-at-later-row | Recommended Project Structure (`detect/header.py`); existing STACK.md `detector/header.py` design, unchanged |
| CSV-08 | Metadata preambles, comments, blank lines, footers/totals excluded from data | Same as CSV-07; PITFALLS.md E4 (footer/header silent-plausible-row trap), unchanged from prior research |
| CSV-09 | Invalid dates produce explicit validation errors, never silently coerced | Code Examples (verified DST classification function); existing `strptime`-only design, unchanged |
| CSV-10 | Numeric/boolean/NULL normalization per configuration, `1/0` never silently boolean | Don't Hand-Roll; Common Pitfalls #7 (shallow-merge nuance for locale/policy config) |
| CSV-11 | `.gz`/`.zip`/multi-part support | Architecture Patterns Pattern 4; Common Pitfalls #3, #4, #8; Open Questions #1, #3 |
| CSV-12 | Unicode NFC normalization before hashing | Architectural Responsibility Map (hard ordering edge, `StreamingStage`); D-15 (locked, unchanged) |
| SCHEMA-01 | Conservative type inference (`001234` stays a string) | Recommended Project Structure (`detect/schema.py`, bootstrap-only); existing PITFALLS.md E5 design, unchanged |
| SCHEMA-02 | Explicit YAML contracts: types, nullability, required, business keys, semantics | D-18/D-19/D-20 (locked, unchanged); Recommended Project Structure |
| SCHEMA-03 | Schemas versioned; batch records dataset/schema version/hash/processor version/timestamp | Code Examples (`meta.schema_versions` migration, verified against existing migration conventions) |
| SCHEMA-04 | Added/removed/renamed/reordered/retyped columns classified compatible or breaking, per configurable policy | D-01/D-02/D-04 (locked, unchanged); dlt 3×4 matrix (ROADMAP guidance, unchanged) |
| SCHEMA-05 | Drift detected and reported, never silently adapted to | D-01/D-02 (locked, unchanged) |
| SCHEMA-06 | Historical files process under their historical schema version | D-16/D-17 (locked, unchanged); Architectural Responsibility Map |
| LOAD-07 | Files larger than container memory process in bounded memory, configurable batch size and max field/row length | Architecture Patterns Pattern 4 (bounded-memory scoping for `.zip`); Common Pitfalls #3 |
| QUAL-04 | Unit tests cover filename parsing, encoding/dialect/header detection, schema inference, structural/type validation, normalization | Validation Architecture (full Req→Test map) |
| QUAL-12 | Schema evolution tested for compatible and breaking changes | Validation Architecture (SCHEMA-04/05 row) |
| QUAL-16 | Property test asserts determinism (identical hash for identical input/config/version) | Code Examples (determinism property test shape) |
| QUAL-17 | Timezone/DST correctness tested as a property, including gap/overlap | Code Examples (verified Hypothesis DST strategy + classification function, exactly reproduces corpus fixture 55) |
</phase_requirements>

## Summary

This phase has almost no open "which library" questions — `.planning/research/STACK.md` §F already pins the stack and 06-CONTEXT.md already locks 25 implementation decisions. What genuinely needed research was **exact, current library APIs verified by direct execution against the pinned versions**, and **grounding every recommendation in the actual current code**, not the summaries of it. Both were done: every library claim below was either executed live against the exact pinned version in this session, or read directly from the installed package/project source.

That verification surfaced five findings that change how the plan should be written, beyond what CONTEXT.md or STACK.md already say:

1. **`clevercsv.Detector.detect()` returns a dialect that crashes on conversion for single-column files.** `SimpleDialect(delimiter='', ...).to_csv_dialect()` raises `_csv.Error: "delimiter" must be a 1-character string` — verified live against fixture-38's exact shape (`38_single_column_no_delimiter.csv`). This needs an explicit special case, not documented anywhere in STACK.md.
2. **STACK.md's literal encoding-confidence algorithm would incorrectly quarantine the corpus's own `06_windows1250.csv` fixture.** `chardet 7.5.1`'s raw `confidence` for a *correctly* detected `cp1250` sample was empirically 0.042 (4.2%) — far below the algorithm's stated `min_confidence` default of 0.85. "Use chardet's confidence when both agree" is not implementable as literally written; a different confidence derivation is needed (recommendation below, Common Pitfalls #2).
3. **`.zip` cannot be decompressed by a single sequential pass the way `.gz` can — this is a structural property of the ZIP format, not an implementation gap.** Verified live: `gzip.GzipFile` reads correctly over a non-seekable stream; `zipfile.ZipFile` raises `BadZipFile` over the identical stream shape, because ZIP's central directory lives at the end of the archive and opening a `ZipFile` requires `seek()`. This directly touches locked decision D-22 ("streaming, in-line... never decompress to a temp file first") — the *file* part (never touch disk) is still fully achievable, but "streaming, in-line" needs a documented, scoped exception for `.zip` specifically. See Common Pitfalls #3 and Open Questions #1.
4. **The actual current `dataplat.storage.objectstore.py` does *not* contain the hand-written `io.RawIOBase` adapter 06-CONTEXT.md's `code_context` section describes.** Reading the real file (not the summary) shows `botocore.response.StreamingBody` already implements `readable()`/`readinto()` directly against the pinned boto3 1.43.68, so `io.BufferedReader(response_body)` wraps it with no adapter at all. The decompression layer for CSV-11 wraps at a *simpler* point than CONTEXT.md assumed. See Common Pitfalls #4.
5. **`dataplat.discovery.discover_files`'s `idempotency_key` formula contains an explicit, already-committed instruction to append the schema-version term once Phase 6 exists** ("*A later phase EXTENDS this formula by appending the missing terms once they have real values; it does not replace it.*") — this is a real, concrete Phase 6 code change not named anywhere in 06-CONTEXT.md's decisions or canonical_refs. See Common Pitfalls #5.

**Primary recommendation:** Build the five detectors and the normalizers exactly as CONTEXT.md/STACK.md/ROADMAP already specify, using the verified API shapes and pitfall fixes in this document; treat the `.zip` streaming question (Open Questions #1) as needing one explicit planner decision before Wave C/D starts, since it is the one place a locked decision meets a hard physical constraint neither the user nor prior research could have known about.

## Architectural Responsibility Map

This is a batch ETL platform with no browser/web tier — "tier" below maps to this project's real architectural layers (Airflow orchestration, the ETL task pod's two packages, PostgreSQL, MinIO), not a web app's client/server/CDN split.

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Filename mask parsing (CSV-01) | ETL Task Pod — `csv_processor` | Database — `meta.files.business_date` | Runs once per discovered file, in-pod, before streaming; result persisted as file/run metadata, never a live external call |
| Encoding detection (CSV-02/03) | ETL Task Pod — `csv_processor` | — | Pure in-pod computation over a bounded byte sample; no service call |
| Dialect detection (CSV-04/05/06) | ETL Task Pod — `csv_processor` | — | Same |
| Header/metadata/footer detection (CSV-07/08) | ETL Task Pod — `csv_processor` | — | Same |
| Compression/archive handling (CSV-11) | ETL Task Pod — `csv_processor` I/O layer | Object Storage — MinIO (source bytes) | Wraps the existing `StreamingBody → io.BufferedReader` seam with a decompression layer; MinIO never decompresses |
| Type inference (SCHEMA-01) | ETL Task Pod — `csv_processor`/`dataplat` | Database — `meta.schema_versions` (proposal record) | Computed in-pod as a bootstrap aid; never applied automatically to the load path (locked: contract wins) |
| Schema contract validation (SCHEMA-02) | ETL Task Pod — `dataplat` | Database (contract sourced from `configs/datasets/*.yaml` via the existing config-sync path) | Validation runs per-file in-pod against the already-versioned config |
| Schema versioning & hashing (SCHEMA-03) | ETL Task Pod — `dataplat` | Database — `meta.schema_versions` (new Alembic migration, this phase) | Hash computed in Python (existing canonical-JSON sha256 recipe pattern), persisted as the system of record |
| Schema evolution classification (SCHEMA-04/05) | ETL Task Pod — `dataplat` | Database — `meta.schema_versions` | Detect + record only (D-01/D-02) — never DDL; the pod never writes outside its own run's rows |
| Historical schema resolution (SCHEMA-06) | ETL Task Pod (re-derive structure) + Database (hash-match query) | — | In-pod structural re-derivation, matched against `meta.schema_versions` history already in Postgres |
| Date/numeric/boolean/null normalization (CSV-09/10) | ETL Task Pod — `dataplat` `StreamingStage` | — | Per-chunk pure functions in the existing `run_streaming` pipeline; no external call |
| Unicode NFC normalization (CSV-12) | ETL Task Pod — `dataplat` `StreamingStage` | — | Must run before any hash is computed (hard ordering edge — Phase 9/10 consume this) |
| Streaming batch size / field-row limits (LOAD-07) | ETL Task Pod — `dataplat` config + `csv_processor` reader | — | Contract-declared (`csv.max_field_bytes`, batch size), enforced in the streaming reader exactly like the existing `FIELD_SIZE_LIMIT` |

## Package Legitimacy Audit

Ran `slopcheck install charset-normalizer chardet clevercsv parse` (PyPI ecosystem, auto-detected) — all four packages this phase's research touched came back clean, and all four are additionally verified via direct PyPI JSON API queries and (for the three that will actually be added as dependencies) live installation and execution in this session.

| Package | Registry | Age | Downloads | Source Repo | slopcheck | Disposition |
|---------|----------|-----|-----------|-------------|-----------|-------------|
| `clevercsv` | PyPI | 7 yrs (first release 2019-04-29) | high (Turing Institute project, widely cited) | github.com/alan-turing-institute/CleverCSV | [OK] | Approved — will be added to `csv-processor` deps |
| `charset-normalizer` | PyPI | 7 yrs (first release 2019-08-03) | very high (bundled transitively via `requests`/`pip`) | github.com/jawah/charset_normalizer | [OK]* | Approved — will be added to `csv-processor` deps |
| `chardet` | PyPI | 20 yrs (first release 2006-12-23) | very high (long-standing standard tool) | github.com/chardet/chardet | [OK] | Approved — will be added to `csv-processor` deps |
| `parse` | PyPI | 15 yrs (first release 2011-11-17) | high | github.com/r1chardj0n3s/parse | [OK] | **Not recommended for adoption** (see Architecture Patterns — Pattern 3) — audited in case planning chooses it anyway; safe if used |

\* slopcheck's only note on `charset-normalizer` was "No source repository linked [in PyPI metadata]. Harder to verify what this code actually does" — this is PyPI project-URL metadata incompleteness, not a code-provenance concern: the GitHub repo (`jawah/charset_normalizer`) is real, active (pushed 2026-08-05 per STACK.md's own sources), and this package is a transitive dependency of `pip` itself.

**Packages removed due to slopcheck [SLOP] verdict:** none.
**Packages flagged as suspicious [SUS]:** none.

**Version note (verify at plan time, low-impact drift):** STACK.md pinned `charset-normalizer==3.4.9` and `chardet==7.5.1` on 2026-08-11. As of this research session, PyPI's latest is `charset-normalizer 3.5.0` (already resolved into this project's `.venv`, likely a transitive pull) and `chardet 7.6.0` (released 2026-08-14, one day before this research). `clevercsv==0.8.5` is still current — no drift. All API behavior verified in this document was tested against `clevercsv 0.8.5`, `chardet 7.5.1` (the exact pin), and `charset-normalizer 3.5.0` (one minor ahead of the pin; the `from_bytes`/`CharsetMatch` API used here has been stable across this range). Recommendation: keep STACK.md's exact pins for consistency with the rest of the already-committed research; the newer patch releases are a low-risk optional bump, not a requirement.

## Standard Stack

### Core (already pinned by STACK.md §F — verified current and API-correct this session)

| Library | Version | Purpose | Verified |
|---------|---------|---------|----------|
| `charset-normalizer` | 3.4.9 | Primary encoding detector (`from_bytes(...).best()`) | [VERIFIED: live execution, this session, v3.5.0] |
| `chardet` | 7.5.1 | Secondary encoding cross-check (`chardet.detect(...)`) | [VERIFIED: live execution, this session, exact pinned version] |
| `clevercsv` | 0.8.5 | Dialect **detection only** (`Detector().detect(sample_str)`) | [VERIFIED: live execution, this session, exact pinned version] |
| stdlib `csv` | 3.12 | Streaming parse (already in use, `source.py`) | [VERIFIED: existing code] |
| stdlib `gzip` | 3.12 | `.gz` streaming decompression | [VERIFIED: live execution, this session] |
| stdlib `zipfile` | 3.12 | `.zip` decompression (needs a seekable buffer — see Pitfall #3) | [VERIFIED: live execution, this session] |
| stdlib `zoneinfo` + PEP 495 `fold` | 3.12 | DST gap/overlap classification (QUAL-17) | [VERIFIED: live execution, exactly reproduces fixture 55] |
| `datetime.strptime` | stdlib | Explicit-format date/timestamp parsing (already locked) | [VERIFIED: existing STACK.md guidance, unchanged] |
| `decimal.Decimal` | stdlib | Locale-aware numeric normalization (already locked) | [VERIFIED: existing STACK.md guidance, unchanged] |
| `hypothesis` | 6.165.3 | Property tests for QUAL-16/17 | [VERIFIED: installed, `st.datetimes(timezones=..., allow_imaginary=True)` confirmed working] |
| `alembic` | 1.19.1 | `meta.schema_versions` migration | [VERIFIED: installed, matches pin exactly] |

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `codecs` (stdlib) | 3.12 | `codecs.lookup(name).name` to canonicalize encoding aliases before comparing two detectors' output | Every place charset-normalizer's and chardet's encoding strings are compared for agreement (see Pitfall #6 — `"cp1250"` vs `"Windows-1250"` are the same codec, different strings) |
| `pendulum` | 3.2.0 | Already pinned platform-wide for `logical_date`/`data_interval` arithmetic | Not used for CSV-09/QUAL-17's naive-local-time classification — `zoneinfo` + `fold` is the verified, correct, dependency-free tool for that specific job (see Code Examples) |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Hand-rolled filename-mask token→regex compiler | `parse` 1.22.1 (PyPI, verified, supports `{name:%Y%m%d}` strptime tokens natively since 1.20.0) | `parse` has **no support for D-08's bracket-optional segments** (`[_{seq:03d}]`) — verified via its own README; adopting it would still require hand-writing the optional-segment layer around it, so it buys less than it costs. See Architecture Patterns Pattern 3. |
| Buffering `.zip` archive bytes fully in memory (`io.BytesIO`) before use | `stream-unzip` (PyPI, third-party) — parses ZIP local file headers sequentially without requiring `seek()` | Genuinely avoids materializing the compressed archive; costs a new dependency this project has generally avoided adding casually, and only pays off if real `.zip` archives turn out to be large enough that in-memory buffering of the *compressed* bytes is itself a problem (no evidence of this yet — no real dataset uses `.zip`). See Open Questions #1. |
| `chardet`'s raw `confidence` as THE reported confidence | `1 - CharsetMatch.chaos` (charset-normalizer) as the primary confidence signal, corroborated by detector agreement | chardet's confidence is empirically far too conservative for exactly the single-byte Central-European encodings this project's own corpus stresses (verified: 0.042 for a *correct* `cp1250` detection) — see Common Pitfalls #2 |

**Installation** (new dependencies for `packages/csv-processor/pyproject.toml` — confirmed absent today via `grep`):
```bash
# Added to packages/csv-processor/pyproject.toml [project.dependencies]
# charset-normalizer>=3.4.9,<4
# chardet>=7.5.1,<8
# clevercsv>=0.8.5,<1
```
No changes needed to `packages/dataplat/pyproject.toml` — detection is exclusively `csv_processor`'s territory per the existing import-linter contract (`dataplat` may not import `csv_processor`, confirmed in `setup.cfg`; the reverse already holds).

**Version verification performed:** `clevercsv==0.8.5` installed and imported directly in an isolated scratch environment; `chardet==7.5.1` installed and imported directly; `charset-normalizer` (3.5.0, one minor ahead of STACK.md's 3.4.9 pin) already present in the project's own `.venv`. All three APIs used below were exercised against real bytes, not read from documentation alone.

## Architecture Patterns

### System Architecture Diagram

```
                    ┌─────────────────────────────────────────────────────┐
                    │  ETL Task Pod (one discovered file / archive member) │
                    │                                                       │
 MinIO object  ───► │  1. Source.inspect(ctx)  [NEW — Phase 6]             │
 (raw bucket,       │     ├─ decompression sniff (.gz/.zip? magic bytes)   │
 possibly .gz/.zip) │     ├─ filename mask match  → business_date, facets  │
                    │     ├─ BOM sniff (deterministic, wins outright)      │
                    │     ├─ encoding detection (charset-normalizer        │
                    │     │    + chardet, canonicalized comparison)        │
                    │     ├─ dialect detection (clevercsv, decoded sample) │
                    │     ├─ header/metadata/footer detection (own logic) │
                    │     └─ schema inference + contract validation        │
                    │           → CsvProfile (encoding, dialect, header    │
                    │             row, schema_version_id, compatibility)   │
                    │                                                       │
                    │  2. Source.open(ctx)  [existing seam, extended]      │
                    │     StreamingBody → io.BufferedReader                │
                    │       → [NEW] decompression layer (gzip/zip)         │
                    │       → io.TextIOWrapper(encoding=detected, newline="")│
                    │       → csv.reader(dialect=detected)                 │
                    │       → chunked_records()  (existing, CSV-13)        │
                    │                                                       │
                    │  3. run_streaming(ctx, chunks, stages)  [existing]   │
                    │     RaggedRowGuard (existing)                        │
                    │       → [NEW] date/number/boolean/null normalizers   │
                    │       → [NEW] Unicode NFC normalizer                 │
                    │       → ... (Phase 9/10 stages, hash on NORMALIZED   │
                    │             values only — hard ordering edge)        │
                    │                                                       │
                    │  4. meta.schema_versions write [NEW — this phase]    │
                    │     compatible → proposal row, file still loads      │
                    │     breaking   → IncompatibleSchemaError, nothing    │
                    │                  loads for this file                 │
                    └─────────────────────────────────────────────────────┘
                                          │
                                          ▼
                         PostgreSQL: meta.schema_versions (new table)
                         meta.ingestion_runs.schema_version_id (FK, deferred
                         since migration 0004 — this phase closes it)
```

### Recommended Project Structure

```
packages/csv-processor/src/csv_processor/
├── source.py              # existing — extended: Source.inspect(), decompression layer
├── detect/                # NEW — this phase's five detectors, independent pure functions
│   ├── __init__.py
│   ├── filename.py        # CSV-01: mask compiler + matcher
│   ├── encoding.py        # CSV-02/03: BOM sniff + charset-normalizer + chardet
│   ├── dialect.py         # CSV-04/05/06: clevercsv wrapper + single-column guard
│   ├── header.py          # CSV-07/08: header/metadata/footer scoring
│   └── schema.py          # SCHEMA-01: conservative type inference (bootstrap only)
├── compression.py         # NEW — CSV-11: gzip/zip streaming decompression layer
└── cli.py                 # existing

packages/dataplat/src/dataplat/
├── normalize/              # NEW — this phase's normalizers, StreamingStage implementations
│   ├── __init__.py
│   ├── dates.py            # CSV-09: explicit-format parsing + DST classification (QUAL-17)
│   ├── numeric.py          # CSV-10: decimal/thousands/parentheses/percent/scientific
│   ├── boolean_null.py     # CSV-10: contract-declared token lists
│   └── unicode.py          # CSV-12: NFC, must precede any hash computation
├── schema/                 # NEW — SCHEMA-03/04/05/06
│   ├── __init__.py
│   ├── versioning.py       # hash + version a resolved schema (mirrors config/hashing.py)
│   ├── evolution.py        # dlt 3×4-matrix classification (compatible/breaking)
│   └── repository.py       # meta.schema_versions CRUD, sibling of ConfigRegistry
├── diagnostics.py          # NEW — D-23/D-24 shared code catalog (row-level + file/run-level)
└── config/model.py         # existing — extended: columns:, filename:, normalization:, evolution policy

migrations/versions/
└── 0009_meta_schema_versions.py   # NEW — closes migration 0004's deferred FK

tests/unit/detect/                 # NEW — parametrized over tests/fixtures/corpus.yaml's expect: blocks
tests/unit/normalize/              # NEW
tests/property/test_determinism.py # NEW — QUAL-16
tests/property/test_dst_correctness.py  # NEW — QUAL-17
```

### Pattern 1: Detection is a one-time `Source.inspect()`, not a per-chunk `StreamingStage`

**What:** The five detectors (filename, encoding, dialect, header/footer, schema inference) all run exactly once per file, *before* `csv.reader` can even be constructed — you cannot chunk-stream a file whose encoding/dialect/header-row is still unknown. `dataplat.sources.protocol.Source`'s current docstring (read directly from source, not from any research doc) already anticipates this: *"Phase 6 adds `inspect()` plus the two attributes once their types exist. This file is this seam's Phase-3 shape, not its final shape."* `RecordStream` needs the same treatment (its docstring cites the same deferred attributes).

**When to use:** Every one of the five detectors. Do **not** implement them as `dataplat.pipeline.protocol.StreamingStage`/`BarrierStage` — those run per-chunk or per-run *after* a `RecordStream` already exists, which is too late.

**Contrast — the normalizers ARE `StreamingStage`s:** date/number/boolean/null/NFC normalization run per-chunk, after chunking has already started, fitting `StreamingStage.apply(ctx, chunk) -> StageResult` exactly as `RaggedRowGuard` (the one existing example) already does. `dataplat.pipeline.engine.run_streaming()` threads a `Sequence[StreamingStage]` through every chunk in order — this is the literal, existing extension point for the five normalizers (source: `pipeline/engine.py`, read directly).

```python
# Source: dataplat/sources/protocol.py, verified current state, plus this phase's extension
class Source(Protocol):
    def open(self, ctx: PipelineContext) -> AbstractContextManager[RecordStream]: ...
    def inspect(self, ctx: PipelineContext) -> CsvProfile:  # NEW this phase
        """Run all five detectors once; return everything open()/chunking need."""
        ...
```

### Pattern 2: Encoding detection — BOM sniff, then agreement-first (not confidence-first) combination

**What:** STACK.md's original algorithm ("if they agree, report chardet's confidence") is not viable as written — see Common Pitfalls #2 for the verified numbers. The corrected algorithm:

```python
# Verified live this session against clevercsv==0.8.5, chardet==7.5.1, charset-normalizer==3.5.0
import codecs
from dataclasses import dataclass

import chardet
from charset_normalizer import from_bytes

_BOM_TABLE = (
    (codecs.BOM_UTF8, "utf-8-sig"),
    (codecs.BOM_UTF32_LE, "utf-32"),   # must precede UTF-16 (BOM_UTF32_LE starts with BOM_UTF16_LE's bytes)
    (codecs.BOM_UTF32_BE, "utf-32"),
    (codecs.BOM_UTF16_LE, "utf-16"),
    (codecs.BOM_UTF16_BE, "utf-16"),
)


@dataclass(frozen=True, slots=True)
class EncodingDetection:
    encoding: str
    confidence: float
    source: str  # "contract" | "bom" | "detected" | "undetermined"


def detect_encoding(
    sample: bytes, *, contract_encoding: str | None, min_confidence: float
) -> EncodingDetection:
    if contract_encoding is not None:
        return EncodingDetection(contract_encoding, 1.0, "contract")

    for bom, name in _BOM_TABLE:
        if sample.startswith(bom):
            return EncodingDetection(name, 1.0, "bom")

    cn_best = from_bytes(sample).best()
    cd = chardet.detect(sample)

    if cn_best is None or cd["encoding"] is None:
        return EncodingDetection("undetermined", 0.0, "undetermined")

    # Canonicalize BEFORE comparing — "cp1250" (charset-normalizer) and
    # "Windows-1250" (chardet) are the SAME codec, different alias strings.
    # Verified: codecs.lookup("Windows-1250").name == codecs.lookup("cp1250").name == "cp1250".
    cn_name = codecs.lookup(cn_best.encoding).name
    cd_name = codecs.lookup(cd["encoding"]).name

    if cn_name == cd_name:
        # Agreement is itself the primary evidence. `1 - chaos` is a better
        # confidence proxy than chardet's raw number for exactly the
        # single-byte CE encodings this project cares about — chardet's own
        # confidence was verified at 0.042 for a CORRECT cp1250 detection.
        confidence = round(1.0 - cn_best.chaos, 4)
        if confidence < min_confidence:
            return EncodingDetection(cn_name, confidence, "undetermined")
        return EncodingDetection(cn_name, confidence, "detected")

    return EncodingDetection("undetermined", 0.0, "undetermined")
```

**`min_confidence`'s default value must be tuned empirically against the corpus, not guessed.** Run this function against fixtures 05/06/07/40/68 and confirm it reproduces every `detected_encoding`/`encoding_confidence_min`/`encoding_confidence_max` value before picking a default — do not hand-pick 0.85 (STACK.md's number) without checking it against real `1 - chaos` outputs, which run lower than chardet's own confidence scale.

### Pattern 3: Filename mask — hand-rolled token→regex compiler, not the `parse` library

**What:** D-07 (strptime-style named tokens) and D-08 (bracket-optional segments) together are not fully covered by any existing library. `parse` 1.22.1 (verified via its README) supports `{name:%Y%m%d}` strptime tokens directly, but has **no concept of optional segments** — `[_{seq:03d}]` has no `parse` equivalent. Adopting `parse` would still require hand-writing the optional-segment expansion layer around it, so it buys less than it costs; recommend a small (~60-80 line) hand-rolled compiler, consistent with this project's stated aversion to adding a dependency for a narrowly-scoped need with zero real consumers today (D-10: `customers` doesn't use masks).

**Design (two-pass: regex extracts substrings, `strptime` re-parses them):**

```python
# csv_processor/detect/filename.py — sketch, not yet verified against a full
# corpus of masks; verify the token->regex table against every strptime
# directive this project actually needs (%Y %m %d %H %M %S at minimum).
import re
from datetime import datetime

_TOKEN_RE = re.compile(r"\{(\w+)(?::([^}]+))?\}")
_STRPTIME_CHAR_CLASSES = {
    "%Y": r"\d{4}", "%m": r"\d{2}", "%d": r"\d{2}",
    "%H": r"\d{2}", "%M": r"\d{2}", "%S": r"\d{2}",
}


def compile_mask(mask: str) -> "CompiledMask":
    """Compile a mask like '{dataset}_{business_date:%Y%m%d}[_{seq:03d}]'
    into a single anchored regex plus a field->format map, so strptime can
    re-parse the extracted substrings using the SAME format string the
    regex was built from (no duplicated parsing logic)."""
    formats: dict[str, str] = {}

    def _expand_field(m: re.Match[str]) -> str:
        name, spec = m.group(1), m.group(2)
        if spec and spec.startswith("%"):
            formats[name] = spec
            pattern = "".join(_STRPTIME_CHAR_CLASSES.get(spec[i : i + 2], r"\d{2}")
                               for i in range(0, len(spec), 2))
            return f"(?P<{name}>{pattern})"
        if spec and spec.lstrip("0").isdigit():  # e.g. "03d" -> zero-padded int
            width = int(spec.rstrip("d"))
            return f"(?P<{name}>\\d{{{width}}})"
        return f"(?P<{name}>[^_./]+)"

    # Bracket-optional segments become non-capturing optional groups.
    def _expand_optional(m: re.Match[str]) -> str:
        return f"(?:{_TOKEN_RE.sub(_expand_field, m.group(1))})?"

    body = re.sub(r"\[([^\]]+)\]", _expand_optional, mask)
    body = _TOKEN_RE.sub(_expand_field, body)
    # Whole-string anchor recommended (Claude's Discretion, CONTEXT.md): a
    # prefix match would silently accept a truncated or extra-suffix
    # filename, contradicting "fail with a named diagnostic".
    return CompiledMask(re.compile(f"^{body}$"), formats)
```

**Recommendation on the open discretion point (whole-string vs. prefix anchor):** whole-string anchor. A file matching only a prefix of its dataset's mask is exactly the "doesn't match at all" case D-09 says must reject with a named diagnostic — a prefix match would silently accept a malformed filename.

### Pattern 4: Streaming decompression — `.gz` wraps the existing seam directly; `.zip` needs a documented exception

**What (`.gz`, no compromise needed):**

```python
# Verified live this session: gzip.GzipFile reads correctly, in small
# chunks, over a non-seekable io.BufferedReader — no adapter needed.
import gzip

buffered = io.BufferedReader(response_body)  # exactly today's open_text_stream() shape
decompressed = gzip.GzipFile(fileobj=buffered, mode="rb")
text_stream = io.TextIOWrapper(decompressed, encoding=detected, newline="", errors="strict")
```

This is a direct, minimal extension of `dataplat.storage.objectstore.open_text_stream` — **not** the `io.RawIOBase`/`io.BufferedReader` "adapter" 06-CONTEXT.md's `code_context` section describes (see Common Pitfalls #4: that adapter does not exist in the current code; `io.BufferedReader` already wraps `StreamingBody` directly).

**What (`.zip`, needs an explicit, scoped exception — see Open Questions #1 for the decision this needs):**

```python
# Verified live this session: zipfile.ZipFile requires seek() (raises
# BadZipFile otherwise) because the central directory lives at the
# archive's end. Recommended shape given D-21 (one CSV per archive):
import io
import zipfile

compressed_bytes = response_body.read()  # bounded by D-21's "one CSV per
                                          # archive" scope; documented as
                                          # bounded-by-COMPRESSED-size, not
                                          # unbounded — see Open Questions #1
with zipfile.ZipFile(io.BytesIO(compressed_bytes)) as zf:
    (member_name,) = zf.namelist()  # D-21: exactly one member, verify this
    with zf.open(member_name) as member:  # ZipExtFile — verified: true
        # chunked reads of the DECOMPRESSED content (308 x 64-byte reads
        # observed for a 19,686-byte member, never read-all-at-once)
        text_stream = io.TextIOWrapper(member, encoding=detected, newline="", errors="strict")
```

Once the archive is open, `ZipExtFile` streams the *decompressed* content in genuinely bounded chunks exactly like the `.gz` path — the memory-bounding compromise is scoped **only** to the compressed container bytes, not the CSV content itself.

### Anti-Patterns to Avoid

- **Using `chardet`'s raw `confidence` field as the reported confidence when detectors agree:** verified empirically wrong for this project's own corpus (Common Pitfalls #2). Use `1 - chaos` from charset-normalizer instead.
- **Comparing `charset_normalizer`'s and `chardet`'s encoding names as raw strings:** `"cp1250" != "Windows-1250"` as strings, but they are the same codec. Canonicalize with `codecs.lookup(name).name` first (verified, Pattern 2).
- **Calling `SimpleDialect.to_csv_dialect()` unconditionally:** crashes on single-column files (Common Pitfalls #1). Guard on `dialect.delimiter == ""` first.
- **Assuming `.zip` can stream exactly like `.gz`:** it structurally cannot (Common Pitfalls #3). Do not write code that assumes `zipfile.ZipFile` will accept a raw `StreamingBody`-backed stream — it will fail at runtime, not at review time.
- **Implementing the five detectors as `StreamingStage`s:** they run once per file, before any `RecordStream` exists — they belong on `Source.inspect()`, not in the `run_streaming()` chunk loop (Pattern 1).

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| CSV dialect detection | A hand-rolled delimiter-frequency heuristic | `clevercsv.Detector().detect(sample_str)` | Verified live: correctly separates `;` delimiter from `,` decimal separator on a fixture-68-shaped sample where a naive heuristic would guess `,` and corrupt every amount column |
| Single-byte encoding detection (Windows-1250 vs 1252 vs ISO-8859-*) | A hand-rolled byte-frequency table | `charset-normalizer` (primary) + `chardet` (cross-check) | Verified live: correctly identified `cp1250` on a realistic 10-row Polish-city sample where the two encodings are genuinely close; building this from scratch is exactly PITFALLS.md E2's documented trap |
| DST-aware local-time classification (nonexistent / ambiguous / unambiguous) | Hand-rolled UTC-offset-table arithmetic | stdlib `zoneinfo` + PEP 495 `fold` | Verified live: a 12-line function using only `zoneinfo`/`fold` reproduces **every single value** in corpus fixture 55 exactly, including both UTC-under-fold-0/1 pairs — no third-party timezone library needed for this specific classification |
| `.gz`/`.zip` decompression algorithms | Hand-rolled DEFLATE/gzip parsing | stdlib `gzip`/`zipfile` | Battle-tested; the only genuinely hard part (ZIP needing `seek()`) is a format-level constraint no library, hand-rolled or otherwise, can route around without either buffering or ranged reads |
| Ambiguous-date resolution | A "smart" fallback that guesses `dayfirst`/`monthfirst` | Contract-declared explicit `strptime` format lists (already locked in STACK.md) | `dateutil.parser.parse`-style guessing is exactly what CSV-09 forbids; this phase does not reopen that decision, only implements it |

**Key insight:** every detector in this phase has a well-maintained, actively-developed library behind it (verified: all four candidate packages have 7-20 year histories and recent commits). The only place hand-rolling is recommended (filename masks) is a case where the *available* library (`parse`) verifiably does not cover the actual requirement (bracket-optional segments), not a case of reinventing something that already exists.

## Common Pitfalls

### Pitfall 1: `clevercsv`'s single-column dialect crashes on conversion
**What goes wrong:** `Detector().detect(sample)` on a genuinely single-column file (no delimiter present anywhere) returns `SimpleDialect('', '', '')` — not `None`, not an exception. Calling `.to_csv_dialect()` on that result raises `_csv.Error: "delimiter" must be a 1-character string`, because Python's `csv` module requires a real 1-character delimiter even when nothing will ever match it.
**Why it happens:** `clevercsv` deliberately returns a degenerate-but-valid `SimpleDialect` for the "no delimiter" case (unlike stdlib `csv.Sniffer`, which raises) — but the conversion helper doesn't special-case it.
**How to avoid:** check `dialect.delimiter == ""` immediately after `detect()` returns, before calling `to_csv_dialect()`. Treat this as "confirmed single-column" (matches corpus fixture `38_single_column_no_delimiter.csv`), and either read the stream as one field per physical line, or construct a `csv.Dialect` manually with a placeholder delimiter guaranteed absent from the sample (e.g. `\x1f`, ASCII Unit Separator).
**Warning signs:** `_csv.Error: "delimiter" must be a 1-character string` raised from dialect-conversion code, always on genuinely single-column input.
**Confidence:** HIGH — verified live, this session, against the exact pinned version (0.8.5).

### Pitfall 2: `chardet`'s raw confidence is too conservative to gate on directly
**What goes wrong:** Following STACK.md's literal algorithm ("if both detectors agree, report chardet's confidence; quarantine if confidence < 0.85") against a realistic 10-row `cp1250` sample shaped like corpus fixture `06_windows1250.csv` produces `chardet.detect() -> {'encoding': 'Windows-1250', 'confidence': 0.042, ...}` — a **correct** detection reported at 4.2% confidence. Implemented literally, every non-ASCII single-byte-encoded file in the corpus would be incorrectly quarantined, even though `06_windows1250.csv`'s own `expect:` block requires a successful (non-quarantined) detection.
**Why it happens:** chardet's confidence model is tuned around language-frequency statistics that need much more text than a typical CSV sample provides to reach high confidence, even when the underlying candidate is unambiguously correct.
**How to avoid:** don't gate on chardet's raw number. Use detector *agreement* (after canonicalizing encoding names — see Pitfall 6) as the primary signal, and `1 - charset_normalizer_best.chaos` as the reported confidence value when they agree (verified: 0.914 for this exact fixture-06-shaped sample — comfortably above a sane gate, comfortably below the corpus's `encoding_confidence_max: 0.99` ceiling). See Architecture Patterns Pattern 2 for the full corrected algorithm.
**Warning signs:** a `min_confidence` gate set per STACK.md's literal text quarantines files the corpus's own fixtures say should succeed.
**Confidence:** HIGH — verified live, this session, exact pinned `chardet==7.5.1`.

### Pitfall 3: `.zip` cannot be opened from a non-seekable stream — this is structural, not a library gap
**What goes wrong:** `zipfile.ZipFile(io.BufferedReader(non_seekable_stream))` raises `BadZipFile: File is not a zip file`, even though the bytes are a perfectly valid zip archive. `gzip.GzipFile` over the identical non-seekable stream shape works fine.
**Why it happens:** ZIP's central directory (the index of members) lives at the *end* of the archive. Opening a `ZipFile` requires seeking to the end to find it, then seeking again to read the target member's local header — `zipfile` cannot avoid this with any input, because the format itself is not a sequential stream format the way gzip is.
**How to avoid:** for `.zip` specifically, either (a) buffer the compressed archive bytes into `io.BytesIO` before opening (bounded by the archive's *compressed* size, not the CSV's decompressed size — verified: 5,631 bytes compressed for a 19,686-byte CSV in one test, ~3.5x), documenting this as a deliberate, scoped exception to "streaming, in-line," or (b) implement a custom seekable wrapper over ranged `GetObject` calls (no new dependency, more code), or (c) adopt `stream-unzip` (new dependency, but the standard tool for genuinely sequential ZIP reads). This is a real, physical constraint the original discussion behind D-22 did not have visibility into — see Open Questions #1.
**Warning signs:** `BadZipFile: File is not a zip file` raised on a byte-for-byte-valid zip, always when the underlying stream is a boto3 `StreamingBody` (or any other non-seekable source).
**Confidence:** HIGH — verified live, this session, both directions (gzip succeeds, zipfile fails), independently corroborated by community reports (boto/boto3#2966, RaRe-Technologies/smart_open#134) surfaced via web search.

### Pitfall 4: The `io.RawIOBase` adapter 06-CONTEXT.md describes does not exist in the current code
**What goes wrong:** Planning the decompression layer around "wrap the existing hand-written `RawIOBase` adapter" (as 06-CONTEXT.md's `code_context` section states) would target code that isn't there. The actual `dataplat/storage/objectstore.py` (read directly this session) wraps `response_body` — the raw `botocore.response.StreamingBody` — **directly** in `io.BufferedReader`, with an explicit module docstring stating no hand-written adapter exists or is needed against the pinned boto3 1.43.68, because `StreamingBody` already implements `readable()`/`readinto()` itself.
**Why it happens:** 06-CONTEXT.md's `code_context` section was written from institutional memory of what Phase 3 was expected to build, not from re-reading the file as it stands today after later revisions.
**How to avoid:** the decompression layer wraps `io.BufferedReader(response_body)` — the existing, real line in `open_text_stream` — with `gzip.GzipFile(fileobj=...)`/`zipfile.ZipFile(...)`, one level simpler than "extend the RawIOBase adapter." No new adapter class is needed for the `.gz` path at all.
**Warning signs:** a task description referencing a `RawIOBase` subclass in `objectstore.py` that a `grep` will not find.
**Confidence:** HIGH — read directly from the current, committed source file this session.

### Pitfall 5: `discovery.py`'s idempotency-key formula has a standing instruction to extend, and extending it is not risk-free
**What goes wrong:** `dataplat/discovery.py` (read directly) computes `idempotency_key = sha256(f"{dataset_name}|{content_sha256_hex}|{config_hash}|{processor_image}")` today, with an explicit code comment: *"schema_version_id/target_partition/policy_digest have no populated value yet — no schema-versioning concept exists until Phase 6 ... A later phase EXTENDS this formula by appending the missing terms once they have real values; it does not replace it."* This is a concrete Phase 6 action item that appears **nowhere** in 06-CONTEXT.md's decisions or canonical_refs. Naively appending the term changes every future `idempotency_key` value versus what a file would have hashed to under the old 4-term formula — a file processed before this change, if reprocessed after, computes a *different* key and looks like a "new" run rather than a duplicate of the old one.
**Why it happens:** the instruction was embedded directly in the code by the Phase 3 author (anticipating this exact moment) rather than surfaced as a cross-phase requirement anywhere in the planning documents.
**How to avoid:** implement the append (not a redesign) per the code's own instruction. Explicitly decide and document whether a file reprocessed after this change creating a "new-looking" `meta.ingestion_runs` row (rather than being recognized as a repeat of its pre-Phase-6 run) is acceptable — it very likely is, since actual data-duplication protection comes from LOAD-03's content-hash check and LOAD-09's `ON CONFLICT` merge, not from `idempotency_key` matching across a schema-versioning-formula change. Write a test asserting this explicitly rather than assuming it.
**Warning signs:** none at implementation time — this is a silent design gap, not a runtime error. The test named above is the only way to catch it.
**Confidence:** HIGH — read directly from the current, committed source file this session; the extension instruction is verbatim in the code, not inferred.

### Pitfall 6: Comparing detector-reported encoding names as raw strings produces false "disagreement"
**What goes wrong:** For an identical, correctly-decoded sample, `charset_normalizer`'s `CharsetMatch.encoding` reports `"cp1250"` while `chardet.detect()["encoding"]` reports `"Windows-1250"` — the same codec, different alias strings (verified live, both detectors agreeing on the same fixture-06-shaped sample). A naive `cn_encoding == cd_encoding` string comparison reports disagreement where there is none, forcing an incorrect quarantine.
**Why it happens:** the two libraries use different naming conventions (Python codec names vs. IANA/Windows alias names) for the same underlying codec.
**How to avoid:** canonicalize both names through stdlib `codecs.lookup(name).name` before comparing (verified: `codecs.lookup("Windows-1250").name == codecs.lookup("cp1250").name == "cp1250"`).
**Warning signs:** the "both detectors agree" path never triggers for any single-byte Central European encoding, even on samples where both detectors are visibly correct when inspected manually.
**Confidence:** HIGH — verified live, this session, stdlib-only.

### Pitfall 7: `configs/defaults.yaml` merges shallowly — a nested policy block cannot be partially overridden
**What goes wrong:** `dataplat.config.loader.load_config` merges with `merged = {**defaults, **dataset}` — a **top-level-only** dict spread (read directly from source). If `defaults.yaml` grows a `schema_evolution:` block (D-03) with several sub-keys, a dataset that wants to override just one of them (e.g., only `on_column_retype`) must repeat the *entire* block in its own YAML, or its override silently replaces the whole default block including keys it didn't intend to touch.
**Why it happens:** the merge is a one-level `dict` spread by design (documented as intentional in `ARCHITECTURE.md` lines 592-594), not a recursive/deep merge — this was correct for Phase 3/4's flat config shape but becomes a sharp edge once nested policy blocks (`schema_evolution:`, `normalization:`) are introduced.
**How to avoid:** either (a) keep every new nested config section's sub-keys individually overridable by flattening them to top-level keys (e.g., `schema_evolution_on_new_column: evolve` instead of a nested block), matching the existing shallow-merge exactly, or (b) explicitly extend `load_config`'s merge to be recursive for specific known keys, documenting the change. Do not assume nested per-key inheritance works without checking — it does not today.
**Warning signs:** a dataset config that overrides one field inside a nested block unexpectedly loses every sibling default value for that block.
**Confidence:** HIGH — read directly from the current, committed source file this session.

### Pitfall 8: A `.zip` corpus fixture and generator support do not exist yet — CSV-11 is not fully covered by the existing 69 fixtures
**What goes wrong:** `tests/fixtures/corpus.yaml`'s 69 declared fixtures include `61_gzipped.csv.gz` (compression: gzip) but **no `.zip` fixture at all**. `tools/corpus/manifest.py` hard-codes `_COMPRESSIONS: Final[tuple[str, ...]] = ("gzip",)` and `tools/corpus/generators.py`'s `_write_wrapper` explicitly raises on any compression other than `"gzip"` — both read directly from source this session.
**Why it happens:** the corpus was declared "complete" against README §73 + FEATURES.md's fixture list before CSV-11's `.zip` half was scoped as its own thing in this phase's discussion (D-21/D-22).
**How to avoid:** this phase must (1) add `"zip"` to `manifest.py`'s `_COMPRESSIONS`, (2) extend `generators.py`'s `_write_wrapper` to build a one-member zip via `zipfile.ZipFile` (mirroring the existing gzip branch), (3) add a new fixture entry to `corpus.yaml` (e.g. covering CSV-11 the way `61_gzipped.csv.gz` does for gzip), and (4) update `tests/unit/test_corpus_semantic_fixtures.py`'s hardcoded fixture-count assertion (currently asserts exactly 69). This is necessary baseline coverage, not the "proactive corrupted-archive fixture" CONTEXT.md already flagged as discretionary.
**Warning signs:** none until someone tries to declare a `.zip` fixture and `make fixtures` fails with `unsupported compression 'zip'`.
**Confidence:** HIGH — read directly from the current, committed source files this session (`corpus.yaml`, `manifest.py`, `generators.py`).

### Pitfall 9: UTF-16-without-BOM detection quality is sample-dependent — don't over-trust either claim
**What goes wrong:** STACK.md states UTF-16 without a BOM is "poorly detected by both libraries (they frequently guess a single-byte codec)." A direct live test this session against a realistic ~65-byte UTF-16-LE-no-BOM sample (shaped like corpus fixture `40_utf16_no_bom.csv`) found the **opposite**: both `chardet` (confidence 0.95) and `charset-normalizer` (chaos 0.0) correctly and confidently identified it, because roughly 50% of the bytes are `0x00`, a very strong signal.
**Why it happens:** likely sample-size- and content-dependent — STACK.md's claim may hold for very short samples or content with a different ASCII/multi-byte ratio; this test used a full data row, not a single short string.
**How to avoid:** don't hard-code either claim into the implementation or its tests. Verify actual behavior against the real corpus fixture `40_utf16_no_bom.csv` once generated, and keep STACK.md's documented mitigation (the >30% NUL-byte-ratio heuristic) as defensive insurance regardless of what the primary detectors report, since it is cheap and the failure mode (silently decoding UTF-16 as a single-byte codec) is severe.
**Warning signs:** none — this is a "don't assume" note, not a bug.
**Confidence:** MEDIUM — one verified test contradicts one documented claim; sample-size sensitivity is plausible but not itself independently verified across many sample shapes.

## Code Examples

### DST gap/overlap classification (QUAL-17) — verified to exactly reproduce corpus fixture 55

```python
# Source: verified live this session, stdlib zoneinfo + PEP 495 fold only.
# Reproduces EVERY value in tests/fixtures/corpus.yaml's 55_dst_gap_and_overlap.csv
# expect: block exactly, including both fold-0/fold-1 UTC pairs.
import datetime as dt
from zoneinfo import ZoneInfo


def classify_naive_local(naive: dt.datetime, zone: ZoneInfo) -> str:
    """Classify a naive local datetime as nonexistent / ambiguous / unambiguous."""
    aware_fold0 = naive.replace(tzinfo=zone, fold=0)
    utc0 = aware_fold0.astimezone(dt.timezone.utc)
    roundtrip0 = utc0.astimezone(zone).replace(tzinfo=None)
    if roundtrip0 != naive:
        return "nonexistent"
    utc1 = naive.replace(tzinfo=zone, fold=1).astimezone(dt.timezone.utc)
    return "ambiguous" if utc0 != utc1 else "unambiguous"


# Verified against fixture 55's three rows:
# classify_naive_local(dt(2026,3,29,2,30), Warsaw)  -> "nonexistent"  (spring-forward gap)
# classify_naive_local(dt(2026,10,25,2,30), Warsaw) -> "ambiguous"    (autumn overlap)
#   fold=0 UTC: 2026-10-25T00:30:00+00:00 -- matches fixture's row_2_utc_under_fold_0
#   fold=1 UTC: 2026-10-25T01:30:00+00:00 -- matches fixture's row_2_utc_under_fold_1
# classify_naive_local(dt(2026,1,15,12,0), Warsaw)  -> "unambiguous"
#   UTC: 2026-01-15T11:00:00+00:00 -- matches fixture's row_3_utc
```

### Hypothesis strategy for DST-gap/overlap property tests (QUAL-17)

```python
# Source: verified live this session against hypothesis==6.165.3.
# st.datetimes(timezones=..., allow_imaginary=True) genuinely generates
# imaginary (nonexistent) local times, and ordinary sampling over an
# ambiguous-hour range naturally produces both fold=0 and fold=1 values.
import datetime as dt
from zoneinfo import ZoneInfo

import hypothesis.strategies as st
from hypothesis import given

warsaw = ZoneInfo("Europe/Warsaw")

dst_gap_strategy = st.datetimes(
    min_value=dt.datetime(2026, 3, 29, 0, 0),
    max_value=dt.datetime(2026, 3, 29, 4, 0),
    timezones=st.just(warsaw),
    allow_imaginary=True,
)


@given(dst_gap_strategy)
def test_nonexistent_times_never_silently_resolve(candidate: dt.datetime) -> None:
    naive = candidate.replace(tzinfo=None)
    # ... assert classify_naive_local(naive, warsaw) behavior matches the
    # pipeline's actual resolver, for every generated example.
```

### Determinism property test shape (QUAL-16)

```python
# Pattern only — not yet wired to a real pipeline entry point (that entry
# point does not exist until later this phase's tasks land). Mirrors
# STACK.md's already-documented "Property-based tests worth writing" #5
# (load(batch); load(batch) leaves the target byte-identical) but for the
# NORMALIZED OUTPUT HASH specifically, which is what QUAL-16 names.
from hypothesis import given, settings

@given(source_bytes=..., config=..., processor_version=...)
@settings(max_examples=50)  # normalization + hashing is not free; keep this bounded
def test_identical_input_yields_identical_output_hash(source_bytes, config, processor_version):
    hash_1 = run_pipeline_and_hash(source_bytes, config, processor_version)
    hash_2 = run_pipeline_and_hash(source_bytes, config, processor_version)
    assert hash_1 == hash_2
```

### `meta.schema_versions` Alembic migration — mirrors the existing `config_versions` shape exactly

```python
# Source: pattern verified by reading migrations/versions/0001 and 0004
# directly (this session) — no CHECK constraints or native ENUM types exist
# anywhere in this project's migrations; every enum-like column is
# sa.Text(), validated at the Pydantic/application layer (matches
# config/model.py's own documented "config not code, strings not enums"
# convention). UNIQUE(dataset_id, version) + a partial unique index for
# "current" mirrors meta.config_versions exactly.
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0009"
down_revision = "0008"


def upgrade() -> None:
    op.create_table(
        "schema_versions",
        sa.Column("schema_version_id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column("dataset_id", sa.BigInteger(), sa.ForeignKey("meta.datasets.dataset_id"), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("schema_hash", sa.Text(), nullable=False),
        sa.Column("hash_version", sa.SmallInteger(), nullable=False, server_default=sa.text("1")),  # META-02
        sa.Column("columns", JSONB(), nullable=False),  # ordered: name/type/nullable/position/format
        sa.Column("derived_from", sa.Text(), nullable=False),   # "CONTRACT" | "INFERRED" -- app-validated
        sa.Column("compatibility", sa.Text(), nullable=False),  # "COMPATIBLE" | "BREAKING" -- app-validated
        sa.Column("breaking_changes", JSONB(), nullable=True),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_to", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("dataset_id", "version", name="uq_schema_versions_dataset_version"),
        schema="meta",
    )
    op.create_index(
        "uq_schema_versions_current_per_dataset", "schema_versions", ["dataset_id"],
        unique=True, schema="meta", postgresql_where=sa.text("valid_to IS NULL"),
    )
    op.execute("GRANT SELECT, INSERT, UPDATE ON meta.schema_versions TO etl_app")
    # Closes migration 0004's deliberately-deferred FK (its own comment names
    # this exact moment: "a later migration adds the constraint via
    # op.create_foreign_key once that table exists").
    op.create_foreign_key(
        "fk_ingestion_runs_schema_version_id", "ingestion_runs", "schema_versions",
        ["schema_version_id"], ["schema_version_id"], source_schema="meta", referent_schema="meta",
    )


def downgrade() -> None:
    op.drop_constraint("fk_ingestion_runs_schema_version_id", "ingestion_runs", schema="meta", type_="foreignkey")
    op.drop_table("schema_versions", schema="meta")
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|---------------|--------|
| `chardet` widely considered abandoned | Actively maintained again, 7.6.0 released 2026-08-14 | Confirmed by repo push history (STACK.md sources) and this session's fresh PyPI query | Safe to depend on; version drift from STACK.md's 7.5.1 pin is one day old and patch-level only |
| MinIO Python SDK as the S3 client | `boto3` with `endpoint_url` (already locked, unrelated to this phase directly, but relevant to the decompression layer's I/O seam) | Already decided platform-wide | Confirms `StreamingBody` is the actual object every detector/decompression layer wraps |

**Deprecated/outdated in the specific sense relevant here:** none of this phase's stack has moved since STACK.md's 2026-08-11 research date in any way that changes API shape — only patch-version drift (charset-normalizer, chardet), already noted above.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|----------------|
| A1 | `1 - CharsetMatch.chaos` is a better confidence proxy than chardet's raw `confidence` for this project's corpus | Architecture Patterns Pattern 2, Pitfall 2 | If wrong, `min_confidence` tuning against the corpus (explicitly recommended as a required empirical step) will surface it before it ships — low risk, self-correcting by design |
| A2 | Buffering a `.zip` archive's compressed bytes fully into `io.BytesIO` is an acceptable, scoped exception to D-22's "streaming, in-line" wording | Common Pitfalls #3, Open Questions #1 | Medium — this reinterprets a locked decision's literal wording for a sub-case the original discussion didn't have visibility into; needs explicit planner/user confirmation, not silent adoption |
| A3 | Reprocessing a file under the schema-version-extended `idempotency_key` formula (Pitfall 5) safely relies on LOAD-03/LOAD-09's downstream safety nets rather than needing the old key preserved | Common Pitfalls #5 | Medium — plausible from reading the existing architecture, but not independently tested this session; the recommended explicit test would catch a wrong assumption before it ships |
| A4 | Whole-string anchoring is the right default for filename mask matching (vs. prefix match) | Architecture Patterns Pattern 3 | Low — explicitly named as Claude's Discretion in CONTEXT.md, so a different planner choice here is not a deviation from a locked decision |
| A5 | A hand-rolled filename-mask compiler is lower total cost than adopting `parse` + hand-writing only the optional-segment layer | Architecture Patterns Pattern 3, Alternatives Considered | Low — both paths still require hand-written code for D-08; the difference is one dependency, not a functional gap |

## Open Questions (RESOLVED)

**Status (updated during planning, 2026-08-15): all three questions below are resolved by the plan set. Each item's Resolution line names the deciding decision/plan; the original What-we-know/What's-unclear/Recommendation text is left intact below it for the record.**

1. **How should `.zip` decompression actually be implemented, given it cannot be a pure single-pass stream?**
   - **Resolution:** confirmed with the user post-research as CONTEXT.md's **D-22a** — `.gz` stays a true zero-compromise stream; `.zip`'s compressed archive bytes are buffered into `io.BytesIO` before opening (bounded by the archive's compressed size, never the decompressed CSV content, never disk), exactly this question's Recommendation below. Implemented by plan **06-08** Task 1 (`open_compressed_stream`).
   - What we know: verified live that `zipfile.ZipFile` requires a seekable underlying stream (raises `BadZipFile` otherwise), while `gzip.GzipFile` has no such requirement. D-21 scopes `.zip` to exactly one CSV member per archive.
   - What's unclear: whether a real `.zip`-delivering dataset will ever produce archives large enough that buffering the *compressed* bytes in memory (the simplest, zero-new-dependency option) becomes a problem. No real dataset uses `.zip` today (only `customers`, which uses neither compression format).
   - Recommendation: adopt the in-memory-buffered-compressed-bytes approach (Architecture Patterns Pattern 4 option a) as the v1 default, explicitly documented as bounded-by-compressed-size rather than truly O(1); flag the ranged-read or `stream-unzip` alternatives as the escape hatch if a real oversized `.zip` dataset appears later. This should be confirmed with the user before Wave C/D starts, since it touches locked decision D-22's literal wording, even though it satisfies D-22's actual underlying intent (never write decompressed data to disk).

2. **What should `min_confidence`'s actual default value be?**
   - **Resolution:** not pre-picked by research or planning, by design — plan **06-04** Task 1 requires the executor to empirically derive a `DEFAULT_MIN_CONFIDENCE` module constant by running the corrected detection algorithm against every encoding-tagged corpus fixture (05, 06, 07, 26, 27, 40, 41, 68) and recording the smallest threshold that reproduces every fixture's own `encoding_confidence_min`/`encoding_confidence_max` bound — exactly this question's Recommendation below, carried out at implementation time rather than guessed at plan time.
   - What we know: chardet's raw confidence is unusable as the gating signal (Pitfall 2); `1 - chaos` produced 0.914 for one realistic cp1250 sample.
   - What's unclear: the right threshold across the *whole* corpus (fixtures 05, 06, 07, 26, 40, 41, 68 all exercise encoding detection) — one data point isn't enough to pick a number.
   - Recommendation: run the corrected algorithm (Pattern 2) against every encoding-related corpus fixture during implementation and pick the threshold that passes every fixture's `encoding_confidence_min`/`encoding_confidence_max` bound, rather than picking a number in advance.

3. **Does the `.zip` corpus fixture need a distinct name/number, and does it need a corrupted-archive sibling now or later?**
   - **Resolution:** plan **06-01** Task 2 adds the baseline `.zip` fixture (`71_zipped.csv.zip`, wrapping `01_simple.csv`, growing the declared corpus from 69 to 70 fixtures) as a required task — exactly this question's Recommendation. The corrupted-archive sibling stays discretionary/deferred, as CONTEXT.md already scoped it; it is not added this phase.
   - What we know: no `.zip` fixture exists in the current 69; `_COMPRESSIONS`/`_write_wrapper` need extending regardless (Pitfall 8) for CSV-11's baseline behavior to be provable at all.
   - What's unclear: whether the corrupted/truncated-archive case (already flagged as Claude's Discretion in CONTEXT.md) should land in the same task.
   - Recommendation: add the baseline `.zip` fixture as a required Wave 0/1 task (it's not optional — CSV-11 needs it to be testable at all); leave the corrupted-archive fixture as CONTEXT.md already scoped it (discretionary, "grow the corpus as cases are discovered").

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Python | Everything in this phase | Yes | 3.12.3 | — |
| `clevercsv` | Dialect detection (CSV-04/05/06) | Not yet a declared dependency; installs cleanly | 0.8.5 verified | — |
| `charset-normalizer` | Encoding detection (CSV-02/03) | Present in project `.venv` already (transitive); not yet a declared dependency of `csv-processor` | 3.5.0 present, 3.4.9 pinned | — |
| `chardet` | Encoding detection (CSV-02/03) | Not yet a declared dependency; installs cleanly | 7.5.1 verified (7.6.0 latest) | — |
| `hypothesis` | QUAL-16/17 property tests | Yes (dev group) | 6.165.3 | — |
| `alembic` | `meta.schema_versions` migration | Yes (dev group) | 1.19.1 | — |
| `sqlalchemy` | Alembic engine | Yes (dev group) | 2.0.52 | — |
| `psycopg` | Migration testing against real Postgres | Yes | 3.3.4 | — |
| Docker | testcontainers-based integration tests of the new migration | Yes | — | — |
| `slopcheck` | Package legitimacy audit (this research session) | Installed successfully this session | — | — |

**Missing dependencies with no fallback:** none — every dependency this phase needs is either already present or installs cleanly, confirmed by direct installation this session.

**Missing dependencies with fallback:** none.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest 9.1.1 |
| Config file | `pyproject.toml` `[tool.pytest.ini_options]` (root) |
| Quick run command | `pytest tests/unit -x -q` |
| Full suite command | `pytest tests/unit tests/property tests/regression -q` (integration/e2e/cluster tiers stay behind their existing `make test-integration`/`cluster` marker gates, unchanged by this phase) |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|---------------------|-------------|
| CSV-01 | Filename mask parsing, bracket-optional, reject-on-no-match | unit | `pytest tests/unit/detect/test_filename.py -x` | ❌ Wave 0 |
| CSV-02/03 | Encoding detection across the corpus + confidence contract | unit, parametrized over `corpus.yaml` | `pytest tests/unit/detect/test_encoding.py -x` | ❌ Wave 0 |
| CSV-04/05/06 | Dialect detection, quoted/escaped/multiline fields, override | unit, parametrized over `corpus.yaml` | `pytest tests/unit/detect/test_dialect.py -x` | ❌ Wave 0 |
| CSV-07/08 | Header/metadata/footer detection | unit, parametrized over `corpus.yaml` | `pytest tests/unit/detect/test_header.py -x` | ❌ Wave 0 |
| CSV-09 | Explicit-format dates, DST classification | unit + property (QUAL-17) | `pytest tests/unit/normalize/test_dates.py tests/property/test_dst_correctness.py -x` | ❌ Wave 0 |
| CSV-10 | Numeric/boolean/NULL normalization, locale profile | unit, parametrized over `corpus.yaml` | `pytest tests/unit/normalize/test_numeric.py tests/unit/normalize/test_boolean_null.py -x` | ❌ Wave 0 |
| CSV-11 | `.gz`/`.zip`/multipart | unit + integration (real stream shapes) | `pytest tests/unit/test_compression.py -x` | ❌ Wave 0 — also needs corpus generator extension (Pitfall 8) |
| CSV-12 | NFC before hashing | unit + property | `pytest tests/unit/normalize/test_unicode.py -x` | ❌ Wave 0 |
| SCHEMA-01 | Conservative type inference | unit | `pytest tests/unit/detect/test_schema.py -x` | ❌ Wave 0 |
| SCHEMA-02 | Contract validation, `columns:` cross-check against `deduplication.keys` | unit | `pytest tests/unit/test_dataset_config_columns.py -x` | ❌ Wave 0 |
| SCHEMA-03 | Schema hashing/versioning | unit | `pytest tests/unit/schema/test_versioning.py -x` | ❌ Wave 0 |
| SCHEMA-04/05 | Compatible/breaking classification, detect+record, `IncompatibleSchemaError` | unit (QUAL-12) | `pytest tests/unit/schema/test_evolution.py -x` | ❌ Wave 0 |
| SCHEMA-06 | Historical schema hash-match resolution | integration (needs `meta.schema_versions`) | `pytest tests/integration/test_schema_resolution.py -m integration` | ❌ Wave 0 |
| LOAD-07 | Bounded memory, configurable batch/field/row limits | unit + nightly memory test (mirrors existing E6 pattern) | `pytest tests/unit/test_compression.py -k bounded -x` | ❌ Wave 0 |
| QUAL-04 | Unit coverage of every detector/normalizer | unit | (covered by the above) | ❌ Wave 0 |
| QUAL-12 | Schema evolution compatible/breaking tested | unit | (covered by SCHEMA-04/05 above) | ❌ Wave 0 |
| QUAL-16 | Determinism property (identical hash) | property | `pytest tests/property/test_determinism.py -x` | ❌ Wave 0 |
| QUAL-17 | DST/timezone correctness property | property | `pytest tests/property/test_dst_correctness.py -x` | ❌ Wave 0 |

### Sampling Rate
- **Per task commit:** `pytest tests/unit -x -q` (fast, no DB/MinIO — matches the existing offline-gate convention)
- **Per wave merge:** `pytest tests/unit tests/property tests/regression -q`, plus `pytest tests/integration -m integration` for SCHEMA-06 and the migration
- **Phase gate:** full suite green, plus `make fixtures-verify` (the corpus's own digest-oracle check) before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `packages/csv-processor/pyproject.toml` — add `charset-normalizer`, `chardet`, `clevercsv` to `[project.dependencies]`
- [ ] `tests/unit/detect/__init__.py`, `tests/unit/normalize/__init__.py`, `tests/unit/schema/__init__.py` — new test package dirs
- [ ] A shared conftest fixture that parametrizes over `tests/fixtures/corpus.yaml`'s declared fixtures + `expect:` blocks — corpus.yaml's own header comment states this is the intended shape ("Phase 6's detector tests are a parametrised loop over these declarations")
- [ ] `tools/corpus/manifest.py` — extend `_COMPRESSIONS` to include `"zip"` (Pitfall 8)
- [ ] `tools/corpus/generators.py` — extend `_write_wrapper` to build zip archives (Pitfall 8)
- [ ] A new `.zip` fixture entry in `tests/fixtures/corpus.yaml`, and the corresponding count-assertion update in `tests/unit/test_corpus_semantic_fixtures.py` (Pitfall 8)
- [ ] `migrations/versions/0009_meta_schema_versions.py` — new migration (Code Examples)

*(No gaps beyond the above — existing test infrastructure, `corpus.yaml`'s fixture framework, and the `dataplat`/`csv_processor` package layout cover everything else this phase needs.)*

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-------------------|
| V2 Authentication | No | This phase adds no auth surface; inherits Phase 5's Vault/Kubernetes SA identity unchanged |
| V3 Session Management | No | No session concept in a batch ETL pod |
| V4 Access Control | No | `meta.schema_versions` gets the same `GRANT ... TO etl_app` pattern as every existing `meta.*` table — no new role or privilege boundary |
| V5 Input Validation | Yes | Contract-declared types + `errors="strict"` decoding (already established) + explicit `strptime` format lists (already locked, never `dateutil.parser.parse`) + Pydantic `extra="forbid"` for every new `columns:`/`filename:`/`normalization:` config section (matches existing `DatasetConfig` convention) + `yaml.safe_load` only (already established in `config/loader.py`, confirmed this session — never `yaml.load`/`unsafe_load`) |
| V6 Cryptography | No (narrow) | `sha256` for schema/content hashing is content-addressing, not a secret-protection cryptographic use; no new crypto surface this phase introduces |
| V12 File and Resources | Yes | Bounded field/row size (`csv.max_field_bytes`, extending the existing `FIELD_SIZE_LIMIT` pattern) and a new decompression-bomb bound (recommended below) |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|------------------------|
| Decompression bomb (`.gz`/`.zip` expanding to unbounded size) | Denial of Service | Cap total decompressed bytes read per archive member; fail with a named diagnostic once exceeded — same shape as the existing `FIELD_SIZE_LIMIT`/`csv.field_size_limit` guard. Not currently in STACK.md/CONTEXT.md; recommended new control for CSV-11. |
| ReDoS via filename-mask-derived regex | Denial of Service | Keep the strptime-token→regex mapping to fixed-width bounded character classes (`\d{4}`, `\d{2}` — see Pattern 3's compiler) — never `.*`/nested quantifiers. Masks are config-authored, not attacker-supplied, so exposure is inherently low, but the pattern costs nothing to keep. |
| Encoding-confusion / homograph mismatch breaking downstream hash-based dedup | Tampering (data integrity) | D-15's fixed, mandatory NFC normalization before any hash computation (already locked) directly prevents two visually-identical strings from producing different hash keys |
| Malformed/adversarial CSV or archive causing unbounded memory growth | Denial of Service | The existing bounded-streaming architecture (CSV-13) plus this phase's `csv.max_field_bytes`/decompression-bomb cap |
| YAML config deserialization of arbitrary objects | Elevation of Privilege | Already mitigated — `yaml.safe_load` only (verified in `config/loader.py`), Pydantic `extra="forbid"` rejects unrecognized keys at validation time |

## Sources

### Primary (HIGH confidence — verified by direct execution or direct source reading this session)
- `clevercsv==0.8.5` — installed in an isolated scratch environment, `Detector.detect()`/`SimpleDialect.to_csv_dialect()` exercised against fixture-06/38/68-shaped samples
- `chardet==7.5.1` — installed and exercised against realistic cp1250/ASCII/UTF-16LE-no-BOM samples
- `charset-normalizer` (3.5.0, present in project `.venv`) — `from_bytes(...).best()` exercised against the same samples
- Python 3.12 stdlib `gzip`, `zipfile`, `zoneinfo`, `codecs` — behavior verified by direct execution (non-seekable-stream test, PEP 495 fold classification against every value in corpus fixture 55, encoding-alias canonicalization)
- `hypothesis==6.165.3` (installed) — `st.datetimes(timezones=..., allow_imaginary=True)` verified to generate genuine imaginary/ambiguous local times for `Europe/Warsaw`
- `packages/dataplat/src/dataplat/storage/objectstore.py`, `discovery.py`, `config/loader.py`, `config/hashing.py`, `config/registry.py`, `errors.py`, `models/record.py`, `models/report.py`, `sources/protocol.py`, `pipeline/protocol.py`, `pipeline/engine.py`, `config/model.py` — read directly, this session
- `packages/csv-processor/src/csv_processor/source.py` — read directly, this session
- `migrations/versions/0001_meta_datasets_config_versions.py`, `0004_meta_ingestion_runs.py`, `0005_normalized_customers.py` — read directly, this session, for Alembic/schema conventions
- `tests/fixtures/corpus.yaml` (all 69 fixtures' names and several full `expect:` blocks, including 05, 06, 07, 61, 62, 67, 68, 55, 56) — read directly, this session
- `tools/corpus/manifest.py`, `tools/corpus/generators.py` — read directly, this session (compression whitelist finding)
- `setup.cfg` (import-linter contracts) — read directly, this session
- `slopcheck install charset-normalizer chardet clevercsv parse` — run this session, all four `[OK]`
- PyPI JSON API (`pypi.org/pypi/<pkg>/json`) for `clevercsv`, `charset-normalizer`, `chardet`, `parse` — version/date verification this session

### Secondary (MEDIUM confidence — WebFetch verified against official sources)
- `github.com/r1chardj0n3s/parse` README (via WebFetch) — confirmed `{name:%Y%m%d}` strptime-token support since 1.20.0, confirmed no optional-segment support
- `github.com/alan-turing-institute/CleverCSV` source (`detect.py`, `dialect.py`, via WebFetch of raw GitHub content) — cross-verified against the locally-installed 0.8.5 source, found identical
- `pypi.org/project/chardet` (via WebFetch) — 7.6.0 release date and `detect()` return shape, cross-verified against locally-installed 7.5.1 execution

### Tertiary (LOW confidence — WebSearch only, used for corroboration not as a primary claim)
- WebSearch on "python zipfile ZipFile requires seekable stream" — corroborated (did not establish) the zipfile-seek finding, which was independently verified by direct execution first; surfaced `stream-unzip` and `boto/boto3#2966`/`smart_open#134` as community-documented instances of the same constraint

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — every library API claim executed live against the pinned (or one-minor-ahead) version, not taken from documentation alone
- Architecture: HIGH — every architectural claim about integration points (`Source.inspect()`, `StreamingStage`, the decompression seam, the idempotency-key formula) is grounded in the actual current source, read directly, not summarized secondhand
- Pitfalls: HIGH — every pitfall in this document reproduces a concrete, executed failure or a concrete, verified discrepancy against either the corpus's own `expect:` values or the current code; none are speculative

**Research date:** 2026-08-15
**Valid until:** 30 days (2026-09-14) for the architectural/pitfall findings (grounded in this project's own code, which only this project's own commits can move); 7 days for the exact patch-version numbers cited (charset-normalizer/chardet release cadence is fast — re-check `pip index versions` at plan time if this research is consumed after 2026-08-22)
