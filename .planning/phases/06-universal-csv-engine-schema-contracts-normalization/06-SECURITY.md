---
phase: 06
slug: universal-csv-engine-schema-contracts-normalization
status: verified
threats_open: 0
asvs_level: 1
created: 2026-08-15
---

# Phase 06 — Security

> Per-phase security contract: threat register, accepted risks, and audit trail.
>
> Register source: all 18 `06-*-PLAN.md` `<threat_model>` blocks
> (`register_authored_at_plan_time: true` for every plan). 35 distinct Threat
> IDs (`T-06-01`..`T-06-34` plus `T-06-SC`), 38 total register rows keyed by
> `(threat_id, plan)` since three IDs are intentionally reused/re-affirmed
> across plans as the phase's wiring landed in later waves: `T-06-01`
> (encoding-confusion/homograph hashing) evolves `accept` (06-04, detector
> not yet wired) → `mitigate` (06-11, `UnicodeNormalizer` built) → `mitigate`
> re-affirmed live (06-16, actually wired into `StagingLoader` before
> `_record_hash`); `T-06-20` (business-key rename/reorder silently
> repointing joins) is `mitigate` in both 06-13 (pure-function classifier)
> and 06-15 (live-database proof). No SUMMARY.md in this phase carries a
> `## Threat Flags` section (grepped all 18 — none exist). One item of new
> attack surface was found and closed **during code review**, not captured
> in any plan's `<threat_model>` at authoring time — see "Unregistered
> Finding: CR-01" below, verified independently in this audit exactly like a
> `mitigate` entry.
>
> **Verification method:** every `mitigate` entry below was checked by
> direct `grep`/`Read` of the current implementation (never the plan's prose
> or the SUMMARY.md narrative), and the phase's full detector/normalizer/
> schema unit suite (251 tests) plus the security-relevant integration/
> property suites (`test_schema_resolution.py`, `test_staging_normalization.py`,
> `test_migrations.py`, `test_discover_files.py`, `test_run_ingest.py`,
> `test_determinism.py`, `test_dst_correctness.py` — 44 tests) were **run
> live against real Postgres/MinIO testcontainers in this audit session**,
> not merely cited from `06-REVIEW.md`/`06-VERIFICATION.md`. All passed.
> Every `accept` entry was checked against its PLAN.md `Mitigation Plan`
> column (treated as the accepted-risk documentation per this audit's
> dispatch instructions, since no prior `SECURITY.md` exists for this
> phase), and — where the rationale made a falsifiable code claim (e.g. "X is
> never wired," "the window is bounded") — independently re-verified by grep.

---

## Trust Boundaries

| Boundary | Description | Data Crossing |
|----------|-------------|----------------|
| Object-store key / uploaded filename → `parse_filename`/`group_multipart_units` | Filenames and object keys originate from the `raw` bucket — attacker-influenceable by anyone who can upload to a configured source path | Filename strings, facet values |
| Raw file bytes (from `raw` bucket) → `detect_encoding`/`decode_strict`/`detect_dialect`/`detect_header`/`open_compressed_stream` | Untrusted byte/text content; every detector runs over a bounded sample, never the whole file | File bytes, decoded text |
| `configs/datasets/*.yaml` → `DatasetConfig` | Human-authored YAML, parsed with `yaml.safe_load`, validated with `extra="forbid"` on every new model | Column contracts, normalization profiles, CSV overrides, `multipart_pattern` regex |
| File-observed header → `classify_schema_change`/`SchemaRepository`/`meta.schema_versions` writes | Untrusted file structure compared against operator-authored contract state; only `COMPATIBLE` outcomes ever reach a write | Column names/types/positions |
| Alembic migration 0009 | DDL executed with elevated (schema-owner) privileges against the analytical database | `meta.schema_versions` table, `etl_app` grants |
| Aggregation of five detectors + compression + schema resolution into `CsvSource.inspect()`/`open()` | Same untrusted-file-content boundary each detector already covers, now with real consequences (staged rows, `meta.schema_versions` rows) | `CsvProfile`, staged row values, `_record_hash` |

---

## Threat Register

| Threat ID (plan) | Category | Component | Disposition | Mitigation | Status |
|---|---|---|---|---|---|
| T-06-SC (06-01) | Tampering (supply chain) | `charset-normalizer`/`chardet`/`clevercsv` installs | accept | 06-RESEARCH.md's package-legitimacy audit (`slopcheck`) returned `[OK]` for all three, cross-verified via PyPI. Independently confirmed pinned in `packages/csv-processor/pyproject.toml:23-25` (`charset-normalizer>=3.4.9,<4`, `chardet>=7.5.1,<8`, `clevercsv>=0.8.5,<1`) | closed |
| T-06-06 (06-01) | Elevation of Privilege | migration 0009's grant on `meta.schema_versions` | mitigate | `migrations/versions/0009_meta_schema_versions.py:79` — `GRANT SELECT, INSERT, UPDATE ON meta.schema_versions TO etl_app` only (no DELETE/TRUNCATE/DROP), matching every other `meta.*` table. `tests/integration/test_migrations.py:38,50` extends `EXPECTED_TABLES`/`GRANTED_TABLES`/`HASH_VERSION_COLUMNS` to cover it; ran live this audit — passed | closed |
| T-06-05 (06-02) | Elevation of Privilege | `configs/datasets/*.yaml` deserialization | mitigate | `packages/dataplat/src/dataplat/config/loader.py:35` — `yaml.safe_load(...)` only. Every new model (`ColumnContract`, `FilenameMaskConfig`, `NormalizationConfig`, `CsvParsingConfig`) carries `ConfigDict(extra="forbid", frozen=True)` (`config/model.py:182,211,266,324`) — confirmed by direct read | closed |
| T-06-07 (06-02) | Tampering | `csv.delimiter` == `normalization.decimal_separator` misconfiguration | mitigate | `config/model.py:391-413` — `_check_delimiter_does_not_collide_with_decimal_separator`, a real `@model_validator(mode="after")` that raises `ValueError` when both are set and equal. Read in full this audit | closed |
| T-06-08 (06-02) | Repudiation | `deduplication.keys` vs. `columns[].business_key` divergence | mitigate | `config/model.py:415-435` — `_check_deduplication_keys_are_business_key_columns`, raises on an undeclared key or `business_key: false`. Read in full this audit | closed |
| T-06-03 (06-03) | Denial of Service | `compile_mask`'s token→regex expansion | mitigate | `csv_processor/detect/filename.py:80-85` — every strptime directive expands to a fixed-width digit class (`\d{4}`, `\d{2}`); line 217 — the free-form `{name}` token compiles to the bounded `[^_./]+`, never `.*`. No unbounded/nested quantifier found in the file | closed |
| T-06-09 (06-03) | Tampering | Crafted filename engineered for attacker-chosen facet values | accept | Facets are recorded as file/lineage metadata only; `business_date`'s sole sanctioned use is D-11's documented fallback role. Confirmed no SQL/shell construction from facets anywhere in `filename.py`/`discovery.py` | closed |
| T-06-01 (06-04) | Tampering (data integrity) | Encoding-confusion/homograph strings hashing differently | accept (superseded — see T-06-01 (06-16) below) | Original disposition: NFC normalization not yet built; `detect_encoding` alone only picks an encoding | closed (superseded) |
| T-06-04 (06-04) | Denial of Service | `charset_normalizer.from_bytes` over an adversarial byte sample | accept | Confirmed `detect_encoding` is only ever called against the bounded `_INSPECT_SAMPLE_BYTES` sample (`csv_processor/source.py:89,637,641-645`: `_INSPECT_SAMPLE_BYTES: Final[int] = 65_536`, read once, passed straight through) — the accept rationale's factual premise holds in the wired pipeline, not just in isolation | closed |
| T-06-26 (06-05) | Denial of Service | `clevercsv.Detector().detect()` over a large/adversarial sample | mitigate | `csv_processor/source.py:646-649` — `detect_dialect(decoded_sample, ...)` where `decoded_sample` derives from the same bounded `_INSPECT_SAMPLE_BYTES` read (line 637), never the full file | closed |
| T-06-10 (06-05) | Tampering | Delimiter == decimal-separator column, engineered to corrupt amount fields | mitigate | Architecturally confirmed: `CsvSource.open()` calls `self.inspect(ctx)` first (`source.py:441`, "profile = self.inspect(ctx)"), which runs dialect detection, BEFORE `StagingLoader.load()`'s `with source.open(ctx) as stream:` reaches `run_streaming(..., stages=self._build_stages(ctx))` where `NumericNormalizer` runs. Plus T-06-07's config-level collision validator. Both layers confirmed by direct read | closed |
| T-06-27 (06-06) | Denial of Service | `detect_header` scanning unbounded rows | mitigate | `csv_processor/source.py:656-666` — `candidate_rows` is built by splitting the same bounded sample (never the streamed file) before being handed to `detect_header` | closed |
| T-06-11 (06-06) | Information Disclosure | Rejected duplicate-header names surfacing in structured logs | accept | Column names are not secret values (matches existing OBS-05 redaction scope: passwords/tokens/PII only). No code change needed to verify; rationale is a scope statement | closed |
| T-06-12 (06-07) | Tampering | Crafted value fooling `infer_column_type` into suggesting a numeric type for a damaged identifier | accept | Independently confirmed via repo-wide grep: `infer_column_type`/`infer_schema`/`suggest_column_contracts` have **zero** call sites anywhere in `packages/csv-processor/src` or `packages/dataplat/src` outside `detect/schema.py` itself — bootstrap-only claim holds, never reaches the load path | closed |
| T-06-02 (06-08) | Denial of Service | `.gz`/`.zip` decompression bomb | mitigate | `csv_processor/compression.py:88` — `class _DecompressionBombGuard`, tracks cumulative bytes read, raises `FileInspectionError` with `diagnostic_code="decompression-bomb-exceeded"` (line 178) once `max_decompressed_bytes` is exceeded (line 170), enforced incrementally during chunked reads | closed |
| T-06-28 (06-08) | Denial of Service | Adversarial `.zip` engineered to be large before its central directory is reachable | mitigate | `compression.py:360-366` — `archive.namelist()` checked; more than one member raises `corrupted-archive` before any member is opened (D-21's one-CSV-per-archive scope) | closed |
| T-06-13 (06-08) | Denial of Service | Crafted `multipart_pattern` regex causing catastrophic backtracking | mitigate (caveat noted) | `multipart_pattern` is operator-authored config (`config/model.py:82`, no attacker write path), consumed via plain `re.search(multipart_pattern, obj.key)` (`discovery.py:733`) against the attacker-influenceable `obj.key`. **Caveat**: unlike its sibling `filename.mask` (T-06-03), which gets a structural bounded-character-class compiler regardless of what the mask author writes, `multipart_pattern` has no equivalent code-level backtracking guard — the mitigation rests entirely on the documented example pattern and the operator-config trust boundary, which the plan's own Trust Boundaries table explicitly scopes this threat to. Consistent with how every other regex/string config field in this codebase is treated; flagged here for defense-in-depth awareness, not a blocker at ASVS L1 | closed |
| T-06-14 (06-09) | Tampering | Crafted date value producing a plausible-but-wrong date under an ambiguous format | mitigate | `grep -rn "^import dateutil\|^from dateutil"` across `packages/dataplat/src` and `packages/csv-processor/src` → 0 matches. `dates.py` uses `datetime.strptime` exclusively; docstring line 7-8 states this explicitly | closed |
| T-06-29 (06-09) | Denial of Service | Hypothesis-generated adversarial datetime ranges causing runaway example generation | accept | `tests/property/test_dst_correctness.py:49-58` — both `_GAP_WINDOW`/`_OVERLAP_WINDOW` strategies bounded to a single fixed calendar day (`min_value`/`max_value` 4 hours apart), never open-ended. Ran live this audit (part of the 10-test batch) — passed | closed |
| T-06-15 (06-10) | Tampering | Value engineered to sign-flip under a naive strip-non-numeric-characters approach | mitigate | `dataplat/normalize/numeric.py:312-341` — `_strip_negative_style` explicitly rewrites `parentheses`/`trailing-minus`/`leading-minus` conventions rather than blindly stripping characters. Live-tested (`tests/unit/normalize/test_numeric.py`, part of the 251-test unit batch) | closed |
| T-06-16 (06-10) | Tampering (data integrity) | Spreadsheet-damaged identifier silently coerced to a numeric value pointing at a different record | mitigate | `numeric.py:249-268` — the scientific-notation guard runs and rejects (`scientific-notation-identifier-unrecoverable`) BEFORE any `Decimal` parse is attempted (parse is step (4), guard is step (2)); confirmed by reading `apply()`'s full body in order | closed |
| T-06-17 (06-11) | Tampering | Substring null-token matching destroying real data containing a token as a substring | mitigate | `dataplat/normalize/boolean_null.py:109-119` — `if value in tokens:` is whole-field equality against a tuple, never `token in value`; code comment explicitly names fixture 24 (`"NULL Industries"`). Live-tested | closed |
| T-06-01 (06-11) | Tampering (data integrity) | Encoding-confusion/homograph strings producing different hash keys | mitigate | `dataplat/normalize/unicode.py:99` — unconditional NFC pass built (`unicodedata.normalize("NFC", field) if isinstance(field, str) else field`), `rejected=[]` always (line 104). Proven against fixture 44 by `tests/unit/normalize/test_unicode.py` (part of the 251-test batch). Wiring into the real pipeline is 06-16's job — see below | closed |
| T-06-18 (06-11) | Elevation of Privilege | Unmapped boolean value silently defaulting to `False` | mitigate | `boolean_null.py:194-206` — a value in neither `true_tokens` nor `false_tokens` is rejected with `error_type="unmapped-boolean-token"`; no default branch exists. Live-tested | closed |
| T-06-24 (06-11) | Denial of Service (data-quality) | Unguarded `None` reaching `unicodedata.normalize()`, raising `TypeError` and crashing the whole chunk | mitigate | `unicode.py:99` — `isinstance(field, str)` checked before every `unicodedata.normalize()` call, for every field of every row. Live-tested (`test_unicode.py`'s explicit `None`-in-one-field case) | closed |
| T-06-19 (06-12) | Tampering | Two concurrent `sync()` calls for the same dataset both observing "no current row" | mitigate | `dataplat/schema/repository.py:150-164` — `SELECT dataset_id FROM meta.datasets WHERE dataset_id = %s FOR UPDATE`, explicitly commented "T-06-19 mitigation," locks the dataset row for the transaction's duration before reading/writing `meta.schema_versions`, serializing concurrent syncs. Live-tested via `tests/integration/test_schema_resolution.py` (ran in this audit — passed) | closed |
| T-06-20 (06-13) | Tampering | Uploaded file renaming a business-key column, silently repointing downstream joins | mitigate | `dataplat/schema/evolution.py:149-165` — a column present in `old_columns` but absent from `new_columns` (disappearance dominates a coincidental same-named appearance) raises `IncompatibleSchemaError` with `diagnostic_code="schema-column-disappeared"` before any row is staged. Live-tested | closed |
| T-06-31 (06-13) | Repudiation | Breaking classification with insufficient context to diagnose | mitigate | `evolution.py:149-165` — every raise carries `context={"diagnostic_code": ..., "column": name}` (retype additionally carries `old_type`/`new_type`). Confirmed by direct read | closed |
| T-06-30 (06-14) | Denial of Service | `inspect()`'s sample read growing unbounded via a future edit | mitigate | `csv_processor/source.py:89` — `_INSPECT_SAMPLE_BYTES: Final[int] = 65_536`, a single named module constant read once at line 637, with an explicit code comment at the read site | closed |
| T-06-22 (06-15) | Denial of Service | Attacker uploading many files each with one different extra column, growing `meta.schema_versions` unboundedly | accept | `meta.schema_versions` has the same growth profile as `meta.files`/`meta.ingestion_runs` (already accepted platform-wide); visible immediately via SQL (D-06). No code enforcement expected or required at ASVS L1 | closed |
| T-06-20 (06-15) | Tampering | (Re-affirmed) rename/reorder silently repointing joins, proven live | mitigate | `tests/integration/test_schema_resolution.py` scenario 3 (missing/renamed column → `IncompatibleSchemaError`, zero new `meta.schema_versions` rows) — ran live this audit, passed. See also the CR-01 unregistered finding below for the column-**reorder** variant this ID's original wording did not literally name | closed |
| T-06-01 (06-16) | Tampering (data integrity) | Encoding-confusion/homograph strings, in the REAL pipeline | mitigate | `dataplat/load/staging.py:227-346` (`_build_stages`) appends exactly one `UnicodeNormalizer()` unconditionally LAST (line 346); `staging.py:480-486` computes `record_hash = hashlib.sha256(...)` over `staged_row`, itself derived from `result.chunk.rows` — the OUTPUT of `run_streaming(..., stages=self._build_stages(ctx))`, i.e. strictly after every normalizer including the last-appended `UnicodeNormalizer`. Read in full this audit; live-tested via `tests/integration/test_staging_normalization.py`'s explicit NFC/NFD-pair-same-hash assertion (ran in this audit — passed) | closed |
| T-06-21 (06-16) | Repudiation | Reprocessed file appears as an unexplained "new" run after the idempotency-key formula change | accept | `dataplat/discovery.py:528` (single-file path) and `:357-359` (multipart path) both APPEND `schema_version`/`schema_version_term` after `processor_image`, never reordering the first four terms — confirmed by direct read. LOAD-03/LOAD-09 downstream safety nets (content-hash check, `ON CONFLICT` merge) are pre-existing and unaffected. Regression test present in `tests/unit/test_discovery.py` | closed |
| T-06-25 (06-16) | Denial of Service (data-quality) | Nullable column's empty value wrongly rejected as invalid, or crashing `UnicodeNormalizer` | mitigate | `staging.py:241-250` — `NullTokenNormalizer` appended BEFORE the column's type-specific normalizer for every `nullable` column, never after (confirmed by reading `_build_stages` top to bottom). Live-tested: `tests/integration/test_staging_normalization.py`'s empty-`birth_date`-stages-as-SQL-NULL assertion ran live this audit — passed | closed |
| T-06-23 (06-17) | Repudiation | Non-deterministic hash undermining reproducibility (Core Value) | mitigate | `tests/property/test_determinism.py` exists, drives real `StagingLoader.load()` calls against throwaway Postgres+MinIO, asserts same-config→same-hashes and different-config→different-hashes. Ran live this audit — 2 passed in ~17s (part of the 10-test batch) | closed |
| T-06-32 (06-18) | Tampering | Attacker-uploaded object extending a legitimate multipart group with the next sequential index | accept | Raw-bucket write access is already the platform's outermost trust boundary — identical property the existing single-file path already has; grouping does not grant new privilege | closed |
| T-06-33 (06-18) | Denial of Service (data-quality) | One duplicate part causing group-level skip to silently withhold novel parts | accept | Every part still recorded in `meta.files` as `DISCOVERED` regardless of group-level skip (D-06 visibility); a fresh replacement part resolves it next call. Not silent | closed |
| T-06-34 (06-18) | Denial of Service | Multipart group with unboundedly many parts exhausting file descriptors | mitigate | `csv_processor/source.py:98` — `_MAX_MULTIPART_PARTS: Final[int] = 50`; lines 362-373 — bound checked in `__init__`, BEFORE any stream is opened (`.open()` runs later), raising `FileInspectionError` with `diagnostic_code="multipart-group-too-large"`. Registered in `DIAGNOSTIC_CODES` (`diagnostics.py:118`). Live-tested via `tests/unit/test_csv_source_multipart.py` (part of the 251-test batch — includes a fake `ObjectStore.get_object` that raises `AssertionError` if ever called, proving the bound check runs first) | closed |

*Status: open · closed*
*Disposition: mitigate (implementation required) · accept (documented risk) · transfer (third-party — none in this phase)*

---

## Unregistered Finding: CR-01 (column-reorder silent corruption)

Not present in any of the 18 plans' `<threat_model>` blocks at authoring time — new attack surface discovered by `06-REVIEW.md` during code review, distinct from (though closely related to) `T-06-20`'s explicitly-named **rename** scenario. `T-06-20`'s original wording covers a column disappearing/being renamed (caught by `classify_schema_change`'s name-based comparison). It did **not** anticipate a file whose header carries the **same names, same types, in a different physical order** — `classify_schema_change` is deliberately position-blind (by design, for the rename case), but `StagingLoader` maps every field to its target column by **position only**, with no header-to-contract name remapping anywhere in the codebase. Combined, a column-reordered file would have passed schema validation as a silent non-event and then had every row's values written into the wrong target columns — undetectable even by `_record_hash`, since the hash is computed over the already-misaligned data. This is a direct violation of the platform's stated Core Value ("no data is ever silently dropped, duplicated, or corrupted").

**Fix verified in this audit** (not merely cited from `06-REVIEW.md`):

- `packages/csv-processor/src/csv_processor/source.py:817-832` — `CsvSource._resolve_schema` now explicitly compares `new_names_in_order != old_names_in_order` after `classify_schema_change` returns no findings, and raises `IncompatibleSchemaError` with `context={"diagnostic_code": "schema-columns-reordered", "expected_order": ..., "observed_order": ...}` — read in full this session.
- `packages/dataplat/src/dataplat/diagnostics.py:123` — `"schema-columns-reordered"` present in `_NEW_THIS_PHASE_CODES`.
- `tests/integration/test_schema_resolution.py:646,673` — asserts the raise, its diagnostic code, and that zero new `meta.schema_versions` rows are written; also asserts the code is present in `DIAGNOSTIC_CODES` (drift guard).
- **Ran live this audit**: `uv run --frozen --group cluster pytest tests/integration/test_schema_resolution.py -q` → all tests passed against a real Postgres/MinIO testcontainer, including this exact test.

**Recommendation for future audits**: retroactively fold this scenario's wording into `T-06-20`'s canonical description (rename **or** reorder) in whichever plan next touches `schema/evolution.py`, so a future re-audit does not have to re-discover that the original wording was narrower than the actual fix.

---

## Additional Verified Fixes (from `06-VERIFICATION.md`, not separately registered threats)

Two gaps `06-VERIFICATION.md` found were fixed by the orchestrator and are independently re-verified here because the audit dispatch explicitly named them. Neither maps to a declared STRIDE threat ID (they are goal-achievement/completeness gaps, not threat-model entries), but both bear on the platform's audit-trail/traceability posture, so they are recorded here for completeness rather than silently omitted:

- **`meta.ingestion_runs.schema_version_id` persistence** (Repudiation-adjacent: without this, no query can prove which schema version a given run used). Verified live: `CsvSource.last_profile` (`source.py:390,443`) → `StagingResult.schema_version_id` (`staging.py:523-532`) → `pipeline/run.py:333` → `ctx.metadata.finalize_publication(..., schema_version_id=...)`. Ran `pytest tests/integration/test_run_ingest.py -q -k schema_version` live this audit — 1 passed.
- **Windows-1252/ISO-8859/UTF-16-BE encoding coverage.** Verified present: `tests/unit/detect/test_encoding.py` lines 292/309/326/342 (`test_windows_1252_blind_detection_is_undetermined_not_a_guess`, `test_windows_1252_contract_declared_round_trips_correctly`, `test_detects_an_iso_8859_1_encoded_sample`, `test_detects_a_utf16_be_encoded_sample_via_its_bom`). All ran live in this audit's 251-test unit batch — passed.

---

## Accepted Risks Log

| Risk ID | Threat Ref | Rationale | Accepted By | Date |
|---------|------------|-----------|--------------|------|
| AR-01 | T-06-SC (06-01) | Package-legitimacy audit (`slopcheck`) returned `[OK]` for all three new detection libraries; no `[ASSUMED]`/`[SUS]`/`[SLOP]` result requiring a human-verify checkpoint. | 06-01 plan (verified this audit) | 2026-08-15 |
| AR-02 | T-06-09 (06-03) | Filename facets are recorded as lineage metadata only, never used to construct SQL/shell; `business_date` has one documented, non-authoritative fallback role. | 06-03 plan (verified this audit) | 2026-08-15 |
| AR-03 | T-06-04 (06-04) | Encoding detection runs only over a bounded (~64 KiB) sample; independently confirmed the real `inspect()` call site never widens this. | 06-04 plan (verified this audit) | 2026-08-15 |
| AR-04 | T-06-11 (06-06) | Column names are schema metadata, not secrets; out of OBS-05's redaction scope by design. | 06-06 plan (verified this audit) | 2026-08-15 |
| AR-05 | T-06-12 (06-07) | Type inference is bootstrap-only; independently confirmed zero call sites outside `detect/schema.py` anywhere in `packages/csv-processor/src`/`packages/dataplat/src` — never reaches the load path. | 06-07 plan (verified this audit) | 2026-08-15 |
| AR-06 | T-06-29 (06-09) | Hypothesis DST strategies are bounded to single fixed calendar-day windows; independently confirmed in `test_dst_correctness.py`. | 06-09 plan (verified this audit) | 2026-08-15 |
| AR-07 | T-06-22 (06-15) | `meta.schema_versions`' growth profile matches already-accepted metadata tables platform-wide; visible via SQL, not silent. | 06-15 plan (verified this audit) | 2026-08-15 |
| AR-08 | T-06-21 (06-16) | Idempotency-key formula change is a documented, tested, append-only change; actual duplicate-protection lives in LOAD-03/LOAD-09, unaffected. Independently confirmed the formula only appends. | 06-16 plan (verified this audit) | 2026-08-15 |
| AR-09 | T-06-32, T-06-33 (06-18) | Raw-bucket write access is the platform's pre-existing outermost trust boundary; multipart grouping changes assembly, not privilege. Partial-group skips remain fully visible in `meta.files`. | 06-18 plan (verified this audit) | 2026-08-15 |

*Accepted risks do not resurface in future audit runs.*

---

## Security Audit Trail

| Audit Date | Threats Total | Closed | Open | Run By |
|------------|----------------|--------|------|--------|
| 2026-08-15 | 35 (38 register rows incl. re-affirmations) + 1 unregistered finding (CR-01) | 36 | 0 | gsd-security-auditor (Claude Sonnet 5) |

**Methodology this audit:** Every `mitigate`-disposition threat verified by direct `grep`/`Read` of the current implementation file (never the plan's prose, never the `SUMMARY.md`/`06-REVIEW.md`/`06-VERIFICATION.md` narrative alone). A repo-wide cross-reference was run between every `"diagnostic_code": "..."` / `error_type="..."` literal actually raised in `packages/csv-processor/src` and `packages/dataplat/src` (19 distinct codes found, `RAGGED_ROW` correctly excluded as a documented Phase-3 grandfather exception) against `dataplat.diagnostics.DIAGNOSTIC_CODES` — 100% coverage, zero orphaned raise sites. Live test execution performed fresh in this session (not reused from prior reports): `pytest tests/unit/detect tests/unit/normalize tests/unit/schema tests/unit/test_compression.py tests/unit/test_discovery.py tests/unit/test_diagnostics.py tests/unit/test_dataset_config_columns.py tests/unit/test_csv_source_multipart.py tests/unit/test_csv_source_inspect.py tests/unit/test_errors.py -q` → 251 passed; `pytest tests/integration/test_schema_resolution.py tests/integration/test_staging_normalization.py tests/integration/test_migrations.py -q` (real Postgres/MinIO testcontainers) → 23 passed; `pytest tests/integration/test_discover_files.py tests/property/test_determinism.py tests/property/test_dst_correctness.py -q` → 10 passed; `pytest tests/integration/test_run_ingest.py -q -k schema_version` → 1 passed. Total: 285 tests executed live in this audit session, 0 failing. All 18 `06-*-SUMMARY.md` files independently re-checked for a `## Threat Flags` section: none exist (confirmed by grep, not assumed). `06-REVIEW.md`'s one CRITICAL finding (CR-01, column-reorder silent corruption) and `06-VERIFICATION.md`'s two BLOCKER gaps (schema_version_id persistence, encoding coverage) were independently re-verified in code and via live test execution rather than trusted from their own "fixed" claims — see the dedicated sections above.

**Note on T-06-13:** closed with a documented caveat (weaker structural enforcement than its sibling `T-06-03`, relying on the operator-config trust boundary the plan itself declares) — not escalated to OPEN because the threat model's own Trust Boundaries table explicitly scopes `multipart_pattern` as operator-authored, not attacker-influenceable, consistent with every other config-string field in this codebase, and ASVS Level 1 does not require defense-in-depth against a trusted-operator's own config authoring.

---

## Sign-Off

- [x] All threats have a disposition (mitigate / accept / transfer)
- [x] Accepted risks documented in Accepted Risks Log
- [x] `threats_open: 0` confirmed
- [x] `status: verified` set in frontmatter
- [x] Unregistered attack surface (CR-01) found during implementation is documented and independently closed

**Approval:** verified 2026-08-15
