---
phase: 6
slug: universal-csv-engine-schema-contracts-normalization
status: final
nyquist_compliant: true
wave_0_complete: true
created: 2026-08-15
updated: 2026-08-15
---

# Phase 6 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest `9.1.1` (`[tool.pytest.ini_options]`, root `pyproject.toml`) |
| **Config file** | `pyproject.toml` (root) |
| **Quick run command** | `pytest tests/unit -x -q` |
| **Full suite command** | `pytest tests/unit tests/property tests/regression -q`, plus `pytest tests/integration -m integration` for SCHEMA-03/06, the new `meta.schema_versions` migration, and QUAL-16's determinism property (integration/e2e/cluster tiers stay behind their existing `make test-integration`/`cluster` marker gates, unchanged by this phase) |
| **Estimated runtime** | ~5–10s for `tests/unit` (no Docker, no cluster — mirrors the existing offline-gate convention); integration tier requires testcontainers Postgres + MinIO |

---

## Sampling Rate

- **After every task commit:** `pytest tests/unit -x -q`
- **After every plan wave:** `pytest tests/unit tests/property tests/regression -q` plus `pytest tests/integration -m integration` for schema-versioning/normalization-wiring plans
- **Before `/gsd:verify-work`:** Full suite green, plus `make fixtures-verify` (the corpus's own digest-oracle check) and `python -m importlinter --config setup.cfg` (import-linter contract 1 stays green — `dataplat` never imports `csv_processor`)
- **Max feedback latency:** ~10s for the offline unit gate; integration tier is wave-gate-only, not part of per-commit latency budget

---

## Per-Task Verification Map

| Task | Plan | Wave | Requirement | Secure Behavior | Test Type | Automated Command | Status |
|------|------|------|-------------|------------------|-----------|--------------------|--------|
| T1-T3 | 06-01 | 1 | CSV-11, SCHEMA-03 (foundations) | Detection deps installed; `.zip` corpus support; `meta.schema_versions` migration closes 0004's deferred FK | unit + integration | `pytest tests/unit/test_corpus_semantic_fixtures.py -x -q` / `pytest tests/integration/test_migrations.py -m integration -x -q` | ⬜ pending |
| T1-T3 | 06-02 | 1 | SCHEMA-02, SCHEMA-05 (contracts) | `columns:`/`filename:`/`normalization:`/`csv:` shapes; delimiter/decimal collision + dedup-keys cross-validators; diagnostics catalog; SourceError/SchemaError hierarchy; customers.yaml activated | unit | `pytest tests/unit/test_dataset_config_columns.py tests/unit/test_diagnostics.py tests/unit/test_errors.py -x -q` | ⬜ pending |
| T1-T2 | 06-03 | 2 | CSV-01, QUAL-04 | Filename mask parsing (strptime tokens, bracket-optional facets), mismatch rejects with a named diagnostic | unit | `pytest tests/unit/detect/test_filename.py -x -q` | ⬜ pending |
| T1-T2 | 06-04 | 2 | CSV-02, CSV-03, QUAL-04 | Encoding detection across the corpus (BOM sniff, charset-normalizer + chardet agreement) with a confidence contract that never claims determinism it doesn't have | unit, parametrized over `corpus.yaml` | `pytest tests/unit/detect/test_encoding.py -x -q` | ⬜ pending |
| T1-T2 | 06-05 | 2 | CSV-04, CSV-05, CSV-06, QUAL-04 | Dialect detection (clevercsv), quoted/escaped/multiline fields, single-column guard, contract override | unit, parametrized over `corpus.yaml` | `pytest tests/unit/detect/test_dialect.py -x -q` | ⬜ pending |
| T1-T2 | 06-06 | 2 | CSV-07, CSV-08, SCHEMA-02, QUAL-04 | Header/metadata/footer detection, duplicate-name rejection, configurable or detected | unit, parametrized over `corpus.yaml` | `pytest tests/unit/detect/test_header.py -x -q` | ⬜ pending |
| T1-T2 | 06-07 | 2 | SCHEMA-01, QUAL-04 | Conservative type inference (`001234` stays a string); bootstrap-only, contract always wins | unit | `pytest tests/unit/detect/test_schema.py -x -q` | ⬜ pending |
| T1-T3 | 06-08 | 2 | CSV-11, LOAD-07, QUAL-04 | `.gz` true streaming; `.zip` compressed-bytes-buffered exception (D-22a); decompression-bomb bound; multi-part discovery grouping | unit | `pytest tests/unit/test_compression.py tests/unit/test_discovery.py -x -q` | ⬜ pending |
| T1-T3 | 06-09 | 2 | CSV-09, QUAL-17, QUAL-04 | Explicit-format dates only, invalid dates produce explicit errors, DST gap/overlap classification | unit + property | `pytest tests/unit/normalize/test_dates.py tests/property/test_dst_correctness.py -x -q` | ⬜ pending |
| T1-T2 | 06-10 | 2 | CSV-10, QUAL-04 | Numeric normalization per the dataset's explicit locale profile; unrecoverable-damage rejection (scientific notation, fixed-width) | unit | `pytest tests/unit/normalize/test_numeric.py -x -q` | ⬜ pending |
| T1-T2 | 06-11 | 2 | CSV-10, CSV-12, QUAL-04 | Boolean/NULL exact-token matching, never substring, never a default; unconditional NFC before hashing | unit | `pytest tests/unit/normalize/test_boolean_null.py tests/unit/normalize/test_unicode.py -x -q` | ⬜ pending |
| T1-T2 | 06-12 | 2 | SCHEMA-03, SCHEMA-06, QUAL-04 | Schema hashing/versioning; historical hash-match resolution | unit + integration | `pytest tests/unit/schema/test_versioning.py -x -q` / `pytest tests/integration/test_schema_resolution.py -m integration -x -q` | ⬜ pending |
| T1-T2 | 06-13 | 2 | SCHEMA-04, SCHEMA-05, QUAL-12, QUAL-04 | Compatible (evolve, detect+record) vs. breaking (freeze, `IncompatibleSchemaError`, whole file fails) classification | unit | `pytest tests/unit/schema/test_evolution.py -x -q` | ⬜ pending |
| T1-T2 | 06-14 | 3 | CSV-02, CSV-11, LOAD-07, QUAL-04 | Five detectors + compression aggregated into a real `CsvProfile`; `CsvSource.open()` consumes it | unit | `pytest tests/unit/test_csv_source_inspect.py -x -q` | ⬜ pending |
| T1-T3 | 06-16 | 3 | CSV-01, CSV-09, CSV-10, CSV-12, SCHEMA-03, QUAL-04 | Normalizer stages wired into `StagingLoader.load()`; idempotency-key schema-version extension (with its required `schema` parameter wired into `csv_processor.cli.discover()`'s real call and `tests/integration/test_discover_files.py`'s pre-existing suite in the same plan); business_date fallback wiring | unit + integration | `pytest tests/unit/test_discovery.py -x -q` / `pytest tests/integration/test_staging_normalization.py tests/integration/test_discover_files.py -m integration -x -q` | ⬜ pending |
| T1-T2 | 06-15 | 4 | SCHEMA-03, SCHEMA-04, SCHEMA-05, SCHEMA-06 | Schema resolution/classification live in `CsvSource.inspect()`; compatible/breaking/historical proven against a real database | integration | `pytest tests/integration/test_schema_resolution.py -m integration -x -q` | ⬜ pending |
| T1 | 06-17 | 5 | QUAL-16 | Determinism property: identical source + config + processor version → identical `_record_hash` set | property (integration-tier) | `pytest tests/property/test_determinism.py -m integration -x -q` | ⬜ pending |
| T1-T3 | 06-18 | 5 | CSV-11 | Multipart delivery (`part-00000`/`part-00001`) groups into ONE ingestion run/`AssignmentDocument`; `CsvSource` reads every part as one logical stream via the real `discover_files`→`CsvSource.open()` call chain; an oversized group is rejected before any file descriptor opens | unit + integration | `pytest tests/unit/test_discovery.py tests/unit/test_csv_source_multipart.py -x -q` / `pytest tests/integration/test_discover_files.py -m integration -x -q` | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

All identified by `06-RESEARCH.md`'s Wave 0 Gaps section — every item below is satisfied by plan
`06-01` (Wave 1), which every other plan in the phase transitively depends on for the package
directories it creates, even where no direct `depends_on` edge is declared:

- [x] `packages/csv-processor/pyproject.toml` — `charset-normalizer`, `chardet`, `clevercsv` added (06-01 Task 1)
- [x] `packages/csv-processor/src/csv_processor/detect/__init__.py`, `packages/dataplat/src/dataplat/normalize/__init__.py`, `packages/dataplat/src/dataplat/schema/__init__.py`, `tests/unit/detect/__init__.py`, `tests/unit/normalize/__init__.py`, `tests/unit/schema/__init__.py` — created up front to avoid Wave 2's eleven parallel plans racing on the same file (06-01 Task 1)
- [x] `tools/corpus/manifest.py` — `_COMPRESSIONS` extended to include `"zip"` (06-01 Task 2)
- [x] `tools/corpus/generators.py` — `_write_wrapper` extended to build zip archives (06-01 Task 2)
- [x] A new `.zip` fixture entry in `tests/fixtures/corpus.yaml` (`71_zipped.csv.zip`), plus the corresponding count-assertion update in `tests/unit/test_corpus_semantic_fixtures.py` (06-01 Task 2)
- [x] `migrations/versions/0009_meta_schema_versions.py` — new migration, closes migration 0004's deferred FK (06-01 Task 3)
- [x] `dataplat/config/model.py`, `dataplat/diagnostics.py`, `dataplat/errors.py` shared contracts — the second Wave 0/1 dependency every Wave 2 plan needs, not originally itemized in 06-RESEARCH.md's Wave 0 Gaps but identified during planning as an equally load-bearing prerequisite (06-02, all tasks)

---

## Manual-Only Verifications

*None — all phase behaviors have automated verification. Schema-evolution proposals surfaced via
SQL query (D-06, no new tooling this phase) are inspectable but not a distinct "manual verification"
step — `meta.schema_versions` row assertions in `tests/integration/test_schema_resolution.py` cover
the same behavior automatically.*

---

## Validation Sign-Off

- [x] All tasks have `<acceptance_criteria>` containing automated commands/behavior assertions (this phase's tasks use `<acceptance_criteria>` per the planner's deep-work-rules convention, superseding the older `<verify>`/`<done>` tag pair — every criterion is independently checkable via a test command, CLI output, or source assertion)
- [x] Sampling continuity: no 3 consecutive tasks without an automated command in `<acceptance_criteria>`
- [x] Wave 0 covers all MISSING references (dependencies, corpus tooling, migration, shared contracts)
- [x] No watch-mode flags anywhere in this phase's commands
- [x] Feedback latency < 10s (unit gate); integration tier explicitly exempted as wave-gate-only
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** finalized during planning (`gsd-planner`, 2026-08-15) — real Task/Plan/Wave IDs assigned
above for all 18 plans across 5 waves, covering all 23 phase requirement IDs (CSV-01…12, SCHEMA-01…06,
LOAD-07, QUAL-04/12/16/17) with zero gaps, confirmed by an explicit coverage diff at planning time.
