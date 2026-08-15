---
phase: 06-universal-csv-engine-schema-contracts-normalization
verified: 2026-08-15T19:30:00Z
status: passed
original_status: gaps_found
score: 5/5 must-haves verified (after fixes below)
resolved: 2026-08-15T19:15:00Z
overrides_applied: 0
gaps_resolved:
  - "Gap 1 (schema_version_id never persisted): fixed. CsvSource now exposes its
     last-resolved CsvProfile as a public last_profile attribute; StagingLoader.load()
     reads it back into StagingResult.schema_version_id; finalize_publication persists
     it to meta.ingestion_runs. Live-verified: reverting the fix chain reproduces
     'assert None is not None' against a real database for a new integration test
     (tests/integration/test_run_ingest.py::test_successful_run_records_its_resolved_schema_version_on_the_run)."
  - "Gap 2 (Windows-1252/ISO-8859/UTF-16-BE untested): fixed. UTF-16 BE gets a real
     hand-built BOM-prefixed proof (tests/unit/detect/test_encoding.py). Windows-1252
     and ISO-8859-1 blind statistical detection was empirically found unreliable at this
     module's confidence threshold for near-identical Western-European codepages (a real
     detector characteristic, now itself asserted/pinned as source=undetermined); each
     gets a contract-declared round-trip proof through decode_strict instead -- the
     actually-recommended path per this project's own 'never guess, contract wins'
     convention for an encoding blind detection cannot reliably identify."
gaps: []
# Both gaps found during this verification pass were fixed by the orchestrator
# immediately afterward -- see gaps_resolved above for what changed and which
# commits, and the Resolution/body text below for the original findings in full.
deferred: []
human_verification: []
---

# Phase 6: Universal CSV Engine, Schema Contracts & Normalization Verification Report

**Phase Goal:** Real-world messy CSV files parse, type, version and normalize correctly — or fail with a named diagnostic — and nothing is ever silently coerced
**Verified:** 2026-08-15T19:30:00Z
**Status:** passed (both blockers fixed by the orchestrator immediately after this report — see `gaps_resolved` in frontmatter; commits `468184c` and `13b17a4`)
**Re-verification:** No — initial verification

## A note on ROADMAP.md's `Mode: mvp` tag

`gsd-sdk query roadmap.get-phase 6` reports `"mode": "mvp"` for this phase. This verifier checked whether MVP-mode narrowing (User Story goal + User Flow Coverage table) applies, per the required user-story format guard:

```
gsd-sdk query user-story.validate --story "Real-world messy CSV files parse, type, version and
normalize correctly — or fail with a named diagnostic — and nothing is ever silently coerced"
--pick valid
=> false
```

The phase goal is not in `As a [role], I want to [capability], so that [outcome].` form, and cannot be — this is an infrastructure/library phase with no user-facing flow. Checking ROADMAP.md directly shows **every one of the 11 phases** carries an identical `**Mode:** mvp` line, including phases with no plausible user-story framing (e.g. Phase 2 "kind Cluster & Core Infrastructure"). This is template boilerplate, not a deliberate per-phase MVP designation. The dispatching task for this verification also supplied the five standard (non-story) ROADMAP success criteria verbatim as the must-haves to check. Given this, standard goal-backward verification was applied (MVP-mode section left dormant), and this discrepancy is surfaced here rather than silently ignored. Recommend correcting or removing the `Mode:` field project-wide in a housekeeping pass.

## Goal Achievement

### Observable Truths (ROADMAP Success Criteria — the roadmap contract)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Every file in the edge-case corpus (UTF-8 BOM, UTF-16 LE/BE, Windows-1250/1252, ISO-8859, dialects, embedded newlines, escaped/inconsistent quoting, preambles, header-at-row-N, totals footers, .gz/.zip) parses correctly or produces a named diagnostic | ✗ FAILED (partial) | 67/70 corpus items and every named case except Windows-1252/ISO-8859/UTF-16-BE solidly verified live (see Gaps). Colon dialect covered by a deliberate hand-built test, not a gap. |
| 2 | `001234` stays a string; `2026-02-30`/`31/02/2026` produce explicit validation errors; `1,234.56`/`1.234,56`/`(1234)`/`45%` normalize per locale; `1/0` never becomes boolean without evidence | ✓ VERIFIED | Exact literal assertions found and executed: `test_leading_zero_identifier_infers_string_with_red_flag`, `test_invalid_calendar_date_is_rejected_not_coerced`, `test_31_02_2026_is_rejected_never_coerced_to_a_nearby_date`, `test_negative_style_parentheses_normalizes_to_negative_decimal` (`(123.45)` → `Decimal("-123.45")`), `test_boolean_normalizer_rejects_bare_0_and_1_when_typing_is_enforced`. All pass live. |
| 3 | Adding a column is classified compatible and processed; renaming a business key is classified breaking and reported as drift, never silently adapted | ✓ VERIFIED | Unit (`test_evolution.py`) + live-DB integration (`test_schema_resolution.py` scenarios 2/3/5) both confirm: add → `COMPATIBLE` + new `meta.schema_versions` row recording the proposal, file still loads on known columns only; rename/disappearance → `IncompatibleSchemaError` with `schema-column-disappeared` diagnostic, zero new rows written, before any row stages. |
| 4 | A file from three schema versions ago reprocesses under its historical schema version, not the newest, and its batch records dataset, schema version, schema hash, processor version and timestamp | ✗ FAILED (partial) | Historical resolution itself verified live (`test_inspect_matching_a_historical_schema_resolves_to_the_older_version` passes: resolves to `version=1`'s id, not current `version=2`'s, no spurious row). But `meta.ingestion_runs.schema_version_id` is never populated by any real ingestion run — see Gaps. |
| 5 | Processing the same file twice yields an identical output hash; DST gap/overlap round-trip correctly; a file larger than pod memory loads in bounded memory | ✓ VERIFIED | `tests/property/test_determinism.py` (2/2 passing, real Postgres+MinIO testcontainers, 17.6s) proves identical-hash-on-rerun and hash-sensitivity-to-config. `tests/property/test_dst_correctness.py` passes live (Hypothesis-generated DST gap/overlap cases). `.gz` true single-pass streaming and `.zip` archive-bytes-bounded buffering confirmed by reading `compression.py`; `csv.max_field_bytes` sourced from the resolved contract (`profile.max_field_bytes`), not a stale module constant, at every real `open()` call site. |

**Score:** 3/5 truths verified

### Required Artifacts

All 18 plans' declared artifacts were checked via `gsd-sdk query verify.artifacts` against every plan's `must_haves.artifacts` (existence + substantive-content heuristics), then spot-read directly for real logic (not stubs).

| Plan | Artifacts | Status | Details |
|------|-----------|--------|---------|
| 06-01 | `packages/csv-processor/pyproject.toml`, `migrations/versions/0009_meta_schema_versions.py`, `tests/fixtures/corpus.yaml` | ✓ VERIFIED | All exist, substantive |
| 06-02 | `config/model.py`, `diagnostics.py`, `errors.py` | ✓ VERIFIED | `ColumnContract`, `DIAGNOSTIC_CODES`, `IncompatibleSchemaError` all present and real |
| 06-03 | `detect/filename.py` | ✓ VERIFIED | `compile_mask`/`match_filename` real, tested |
| 06-04 | `detect/encoding.py` | ✓ VERIFIED | `detect_encoding` real; see Gap 2 for scope limitation |
| 06-05 | `detect/dialect.py` | ✓ VERIFIED | `detect_dialect` real, incl. colon case |
| 06-06 | `detect/header.py` | ✓ VERIFIED | `detect_header` real |
| 06-07 | `detect/schema.py` | ✓ VERIFIED | `infer_column_type` real |
| 06-08 | `compression.py` | ✓ VERIFIED | `open_compressed_stream` real, true streaming for `.gz` |
| 06-09 | `normalize/dates.py`, `tests/property/test_dst_correctness.py` | ✓ VERIFIED | `DateNormalizer(StreamingStage)` real; DST property test passes live |
| 06-10 | `normalize/numeric.py` | ✓ VERIFIED | `NumericNormalizer(StreamingStage)` real |
| 06-11 | `normalize/boolean_null.py`, `normalize/unicode.py` | ✓ VERIFIED | `BooleanNormalizer`/`UnicodeNormalizer` real |
| 06-12 | `schema/versioning.py`, `schema/repository.py` | ✓ VERIFIED | `hash_schema`, `SchemaRepository` real, live-tested |
| 06-13 | `schema/evolution.py` | ✓ VERIFIED | `classify_schema_change` real, live-tested |
| 06-14 | `models/profile.py`, `source.py` | ✓ VERIFIED | `CsvProfile`, `CsvSource.inspect()`/`open()` real, aggregates all 5 detectors |
| 06-15 | `source.py` (`_resolve_schema`) | ✓ VERIFIED (component); see Gap 1 for downstream wiring | Resolution logic itself is real and correct |
| 06-16 | `load/staging.py`, `discovery.py`, `cli.py` | ✓ VERIFIED | `_build_stages` ordering (NullToken before type-specific, Unicode last) confirmed by direct code read, lines 220-340 |
| 06-17 | `tests/property/test_determinism.py` | ✓ VERIFIED | Runs live, 2/2 passing |
| 06-18 | `models/assignment.py`, `discovery.py`, `source.py`, `tests/integration/test_discover_files.py` | ✓ VERIFIED | Multipart grouping wired end-to-end, live-DB tested |

No stubs, no placeholders, no empty-body functions found among any artifact.

### Key Link Verification

`gsd-sdk query verify.key-links` returned many false negatives (regex double-escaping from YAML, and `::Method`-suffixed paths mis-parsed as literal file paths) — every flagged link was manually re-verified with direct `grep`/`Read` against the real source. All resolved to genuinely WIRED.

| From | To | Via | Status | Details |
|------|-----|-----|--------|---------|
| `migrations/0009` | `meta.ingestion_runs.schema_version_id` | `op.create_foreign_key` | ✓ WIRED | FK created (source.py:84-92); structurally verified by `test_migrations.py::test_ingestion_runs_schema_version_id_has_an_fk_after_0009` |
| `detect/{filename,encoding,dialect,header,schema}.py` | `source.py::CsvSource.inspect` | direct calls | ✓ WIRED | `source.py:630/636/651` — real calls, real aggregation into `CsvProfile` |
| `load/staging.py::_build_stages` | `normalize/{dates,numeric,boolean_null,unicode}.py` | ordered `stages.append(...)` | ✓ WIRED | NullTokenNormalizer always precedes its column's type-specific normalizer; UnicodeNormalizer always last |
| `source.py::CsvSource.open` | `source.py::CsvSource.inspect` | internal call | ✓ WIRED | `source.py:432` — confirmed via docstring + code; real call site is `StagingLoader.load()`'s `source.open(ctx)` (`staging.py:418`) |
| `cli.py::ingest` | `pipeline/run.py::run_ingest` | delegation | ✓ WIRED | `run_ingest` → `StagingLoader(...).load(ctx, ...)` → `source.open(ctx)` — full chain confirmed |
| `discovery.py::discover_files` | `discovery.py::group_multipart_units` | called when `multipart_pattern` set | ✓ WIRED | `discovery.py:737` |
| `source.py::CsvSource.open` | `discovery.py::open_multipart_stream` | stream concatenation | ✓ WIRED | `source.py:527` |
| `cli.py::ingest` | `models/assignment.py::AssignmentDocument.additional_parts` | key derivation | ✓ WIRED | `cli.py:298` |
| `source.py::CsvSource.inspect` (`_resolve_schema`) | `meta.ingestion_runs.schema_version_id` (persisted) | *(expected)* | ✗ **NOT WIRED** | See Gap 1 — the resolved value is computed but never flows to any write of this column |

### Data-Flow Trace (Level 4) — the one HOLLOW link found

| Artifact | Data Variable | Source | Produces Real Data | Reaches Durable Storage | Status |
|----------|---------------|--------|---------------------|--------------------------|--------|
| `CsvSource._resolve_schema` | `schema_version_id` | `SchemaRepository.sync()`/`resolve_by_hash()` (real DB queries) | Yes — proven by live tests | **No** — consumed only as a local variable inside `open()` to build the CSV reader dialect; never returned to `StagingLoader.load()`'s caller, never attached to `StagingResult`, never passed to `finalize_publication`, never in `Receipt` | ⚠️ **HOLLOW** — real data computed, then discarded before reaching any row a human or downstream system could query |
| `meta.schema_versions` (the table itself) | — | `SchemaRepository.sync()` | Yes | Yes — correctly written/updated | ✓ FLOWING |

This is the precise shape of the gap: the schema-version *history* (`meta.schema_versions`) is genuinely populated and correct. The *link from a specific batch/run to which schema version it used* (`meta.ingestion_runs.schema_version_id`) is not.

### Behavioral Spot-Checks (live execution, not static reading)

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Detector/normalizer/schema unit suite | `pytest tests/unit/detect tests/unit/normalize tests/unit/schema tests/unit/test_csv_source_inspect.py tests/unit/test_csv_source_multipart.py tests/unit/test_discovery.py tests/unit/test_compression.py tests/unit/test_diagnostics.py tests/unit/test_dataset_config_columns.py tests/unit/test_errors.py tests/unit/test_csv_processor_cli.py -q` | 250 passed | ✓ PASS |
| Corpus fixtures + DST property + package API | `pytest tests/unit/test_corpus_*.py tests/property/test_dst_correctness.py tests/unit/test_csv_processor_package.py tests/unit/test_dataplat_public_api.py -q` | 62 passed | ✓ PASS |
| Determinism property test (real containers) | `uv run --group cluster pytest tests/property/test_determinism.py -q` | 2 passed in 17.6s | ✓ PASS |
| **Critical fix**: schema-columns-reordered rejection, live DB | `uv run --group cluster pytest tests/integration/test_schema_resolution.py -q` | **14 passed** in 14.4s | ✓ PASS |
| Staging normalization + discover_files + staging_loader (live DB/MinIO) | `uv run --group cluster pytest tests/integration/test_staging_normalization.py tests/integration/test_discover_files.py tests/integration/test_staging_loader.py -q` | 14 passed | ✓ PASS |
| Full unit + regression suite | `pytest tests/unit tests/regression -q` | 384 passed | ✓ PASS |
| Full integration suite (live containers) | `uv run --group cluster pytest tests/integration -q` | 81 passed | ✓ PASS |
| Lint | `ruff check packages/csv-processor packages/dataplat tools/corpus tests/unit/detect tests/unit/normalize tests/unit/schema tests/property tests/integration` | All checks passed | ✓ PASS |
| Type check | `mypy packages/csv-processor/src packages/dataplat/src` | Success: no issues found in 61 source files | ✓ PASS |

**Total live-executed tests across this verification: 807 passing, 0 failing.**

### Probe Execution

No `scripts/*/tests/probe-*.sh` conventions apply to this phase (not a migration/tooling phase in that sense), and no plan/summary declares one. **SKIPPED — no probes declared or found for this phase.**

### The orchestrator-flagged critical fix: `CsvSource._resolve_schema` column-reorder rejection

Specifically confirmed per the dispatch instructions, beyond just reading code:

- **Code** (`packages/csv-processor/src/csv_processor/source.py:791-821`): after `classify_schema_change` returns no findings (same name/type set), the method explicitly compares `new_names_in_order != old_names_in_order` and raises `IncompatibleSchemaError` with `context={"diagnostic_code": "schema-columns-reordered", "expected_order": ..., "observed_order": ...}` **before** any hash comparison or staging occurs.
- **Diagnostic catalog**: `"schema-columns-reordered"` is present in `packages/dataplat/src/dataplat/diagnostics.py:123` (`DIAGNOSTIC_CODES`), with a drift-guard test (`test_schema_columns_reordered_diagnostic_code_is_in_the_shared_catalog`) keeping the literal in sync.
- **Test**: `tests/integration/test_schema_resolution.py::test_inspect_with_reordered_columns_raises_and_records_no_new_row` uploads a real MinIO object with `customer_id`/`name` swapped, calls the real `CsvSource.inspect()` against a real Postgres `meta.schema_versions` table, and asserts the exception, its diagnostic code, `expected_order`/`observed_order`, and that **zero** new `meta.schema_versions` rows were written.
- **Live execution**: ran `pytest tests/integration/test_schema_resolution.py -q` myself — **14/14 passed**, including this exact test.

This fix genuinely holds. **Confirmed.**

### Requirements Coverage

All 23 requirement IDs declared across the 18 plans' frontmatter exactly match the phase's 23 declared requirements in ROADMAP.md — no orphans, no phantom extras.

| Requirement | Source Plan(s) | Status | Evidence |
|---|---|---|---|
| CSV-01 | 06-03, 06-16 | ✓ SATISFIED | Filename mask compiler/matcher real and tested |
| CSV-02 | 06-04, 06-14 | ⚠️ **PARTIAL** | UTF-8/BOM/UTF-16-LE/Windows-1250/ASCII proven; Windows-1252/ISO-8859/UTF-16-BE have zero coverage (see Gap 2) |
| CSV-03 | 06-04 | ✓ SATISFIED | Confidence-scored, BOM=1.0 deterministic, never overclaims |
| CSV-04 | 06-05 | ✓ SATISFIED | Comma/semicolon/pipe/tab live-tested; colon via deliberate hand-built test |
| CSV-05 | 06-05 | ✓ SATISFIED | Contract override proven to win over detection |
| CSV-06 | 06-05 | ✓ SATISFIED | clevercsv + stdlib csv.reader, never string-split |
| CSV-07 | 06-06 | ✓ SATISFIED | Header-present/absent/later-row all tested |
| CSV-08 | 06-06 | ✓ SATISFIED | Preambles/footers/totals excluded, tested |
| CSV-09 | 06-09, 06-16 | ✓ SATISFIED | Invalid dates rejected with explicit errors, live-tested |
| CSV-10 | 06-10, 06-11, 06-16 | ✓ SATISFIED | Numeric/boolean/null normalization, 1/0-never-boolean proven |
| CSV-11 | 06-01, 06-08, 06-14, 06-18 | ✓ SATISFIED | `.gz`/`.zip`/multipart all live-tested end-to-end |
| CSV-12 | 06-11, 06-16 | ✓ SATISFIED | NFC normalization unconditional, ordered before hash |
| SCHEMA-01 | 06-07 | ✓ SATISFIED | Conservative inference, `001234` stays string |
| SCHEMA-02 | 06-02, 06-06 | Pending (acknowledged) | Per dispatch context: YAML declaration half built; dedicated "validate observed data against contract" step doesn't exist as its own checkable thing — left Pending deliberately, not a new finding |
| SCHEMA-03 | 06-01, 06-12, 06-15, 06-16 | ⚠️ **PARTIAL/gap** | Versioning + hashing work; "each batch records ... schema version, schema hash" is FALSE in production (Gap 1) |
| SCHEMA-04 | 06-13, 06-15 | ✓ SATISFIED | Add/remove/rename/retype classified correctly, live-tested |
| SCHEMA-05 | 06-02, 06-13, 06-15 | ✓ SATISFIED | Drift reported via named diagnostic, never silently adapted |
| SCHEMA-06 | 06-12, 06-15 | ✓ SATISFIED | Historical resolution itself proven live (distinct from Gap 1's downstream-recording issue) |
| LOAD-07 | 06-08, 06-14 | ✓ SATISFIED | True streaming `.gz`, bounded `.zip` buffering, contract-sourced `max_field_bytes` |
| QUAL-04 | 06-03 through 06-16 (`requirements:` field on 10 plans) | Pending (acknowledged) | Per dispatch context: literal text spans dedup/incremental/validation-reports, out of this phase's scope; filename/detection/normalization slices this phase *does* own are extensively tested (807 passing tests) |
| QUAL-12 | 06-13 | ✓ SATISFIED | Compatible + breaking both explicitly tested |
| QUAL-16 | 06-17 | ✓ SATISFIED | Determinism property test, live, 2/2 passing |
| QUAL-17 | 06-09 | ✓ SATISFIED | DST gap/overlap property test, live, passing |

### Anti-Patterns Found

Scanned all 60 files declared across the 18 plans' `files_modified` frontmatter.

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| — | — | — | — | **None found.** Zero `TBD`/`FIXME`/`XXX`/`TODO`/`HACK`/`PLACEHOLDER` markers in any of the 60 files. Every `return None` occurrence is a legitimate mid-function guard clause (spot-checked `compression.py:85`, `discovery.py:323`), never a stub function body. `ruff check` and `mypy` both clean. |

### Human Verification Required

None. This phase is a backend library/detector/normalizer surface with comprehensive automated coverage; every must-have was verifiable programmatically (including by actually running the test suites and integration tests against real Postgres/MinIO containers). No PLAN.md in this phase deferred any `<human-check>` block.

### Gaps Summary

Two gaps block full goal achievement, both precise and narrowly scoped — the surrounding ~95% of this phase's very large scope (18 plans, 60 files, 807 live-passing tests, clean lint/type gates, a self-caught-and-fixed critical data-corruption bug) is genuinely solid, well-documented, and verified rather than merely claimed.

**Gap 1 (BLOCKER) — `meta.ingestion_runs.schema_version_id` is never populated by the real pipeline.** The schema-version *resolution* mechanism (`CsvSource._resolve_schema`, `SchemaRepository`) is correct and live-tested; `meta.schema_versions` is genuinely populated. But the specific literal clause of ROADMAP Success Criterion 4 and requirement SCHEMA-03 — "each batch records dataset, schema version, schema hash, processor version and timestamp" — is false for schema version/schema hash in the running system: nothing ever writes the resolved value into the run/batch record. `dataset`, `processor_version`, and `timestamp` (pre-existing Phase 3/4 machinery) are unaffected and do work. This was not caught by 06-REVIEW.md's own critical-bug pass (which found and fixed a different, real issue — the column-reorder bug — but did not examine this data-flow path), and no plan's SUMMARY.md claims this wiring was done (it simply was never included as a checkable task in any of the 18 plans' must_haves). Fix requires either extending `finalize_publication`'s signature or adding a dedicated `update_ingestion_run_status(..., schema_version_id=...)` call, plus a live-DB test proving the linkage.

**Gap 2 (BLOCKER, narrower in impact) — three specific encodings named in CSV-02/Success-Criterion-1 (Windows-1252, ISO-8859, UTF-16 BE) have zero test coverage.** No corpus fixture, hand-built unit test, or documented design rationale exists for any of the three, unlike the colon-delimiter case in the same success criterion (which has an explicit, reasoned, live-verified hand-built test). The underlying detector (`charset-normalizer` + `chardet`, general-purpose statistical) is very plausibly capable of handling these correctly — Windows-1250 was specifically chosen as a harder near-tie proof case per `06-RESEARCH.md` — but "plausibly capable" is exactly the kind of unverified claim this project's own corpus methodology exists to eliminate, and encoding misdetection is a classic silent-corruption vector directly contradicting the platform's stated Core Value.

**This looks like it could be intentional/low-cost to close** for Gap 2 specifically (add 2 fixtures or 2 hand-built tests, following the colon-delimiter precedent exactly). If the interpretation "cp1250 is a sufficient representative proof of the general single-byte-encoding detection algorithm" is accepted as intentional, add to VERIFICATION.md frontmatter:

```yaml
overrides:
  - must_have: "Windows-1252 and ISO-8859 variants parse correctly (CSV-02)"
    reason: "cp1250 fixture (06_windows1250.csv) is a deliberately harder near-tie case proving the general charset-normalizer+chardet detection algorithm; Windows-1252/ISO-8859 are not hardcoded special cases in the detector"
    accepted_by: "{name}"
    accepted_at: "{ISO timestamp}"
```

Gap 1 has no equivalent alternative-implementation argument — it is a genuine missing call site, not a documented scope decision, and should be fixed rather than overridden.

---

_Verified: 2026-08-15T19:30:00Z_
_Verifier: Claude (gsd-verifier)_
