---
phase: 6
slug: universal-csv-engine-schema-contracts-normalization
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-08-15
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
| **Full suite command** | `pytest tests/unit tests/property tests/regression -q`, plus `pytest tests/integration -m integration` for SCHEMA-06 and the new `meta.schema_versions` migration (integration/e2e/cluster tiers stay behind their existing `make test-integration`/`cluster` marker gates, unchanged by this phase) |
| **Estimated runtime** | ~5–10s for `tests/unit` (no Docker, no cluster — mirrors the existing offline-gate convention); integration tier requires testcontainers Postgres |

---

## Sampling Rate

- **After every task commit:** `pytest tests/unit -x -q`
- **After every plan wave:** `pytest tests/unit tests/property tests/regression -q` plus `pytest tests/integration -m integration` for schema-versioning work
- **Before `/gsd:verify-work`:** Full suite green, plus `make fixtures-verify` (the corpus's own digest-oracle check)
- **Max feedback latency:** ~10s for the offline unit gate; integration tier is wave-gate-only, not part of per-commit latency budget

---

## Per-Task Verification Map

Task/Plan/Wave assignments are filled in once `gsd-planner` creates real plan and task IDs. This
draft maps each phase requirement to its verified test shape and command, from `06-RESEARCH.md`'s
own Validation Architecture section — the planner assigns these to concrete tasks/waves.

| Task | Plan | Wave | Requirement | Secure Behavior | Test Type | Automated Command | Status |
|------|------|------|-------------|------------------|-----------|--------------------|--------|
| TBD | TBD | TBD | CSV-01 | Filename mask parsing (strptime tokens, bracket-optional facets), mismatch rejects with a named diagnostic | unit | `pytest tests/unit/detect/test_filename.py -x` | ⬜ pending |
| TBD | TBD | TBD | CSV-02/03 | Encoding detection across the corpus (BOM sniff, charset-normalizer + chardet agreement) with a confidence contract that never claims determinism it doesn't have | unit, parametrized over `corpus.yaml` | `pytest tests/unit/detect/test_encoding.py -x` | ⬜ pending |
| TBD | TBD | TBD | CSV-04/05/06 | Dialect detection (clevercsv), quoted/escaped/multiline fields, single-column guard, contract override | unit, parametrized over `corpus.yaml` | `pytest tests/unit/detect/test_dialect.py -x` | ⬜ pending |
| TBD | TBD | TBD | CSV-07/08 | Header/metadata/footer detection, configurable or detected | unit, parametrized over `corpus.yaml` | `pytest tests/unit/detect/test_header.py -x` | ⬜ pending |
| TBD | TBD | TBD | CSV-09 | Explicit-format dates only, invalid dates produce explicit errors, DST gap/overlap classification (QUAL-17) | unit + property | `pytest tests/unit/normalize/test_dates.py tests/property/test_dst_correctness.py -x` | ⬜ pending |
| TBD | TBD | TBD | CSV-10 | Numeric/boolean/NULL normalization per the dataset's explicit locale profile; `1/0` never becomes boolean absent evidence; empty-string-only default NULL tokens | unit, parametrized over `corpus.yaml` | `pytest tests/unit/normalize/test_numeric.py tests/unit/normalize/test_boolean_null.py -x` | ⬜ pending |
| TBD | TBD | TBD | CSV-11 | `.gz` true streaming; `.zip` compressed-bytes-buffered exception (D-22a); multi-part discovery grouping | unit + integration | `pytest tests/unit/test_compression.py -x` | ⬜ pending |
| TBD | TBD | TBD | CSV-12 | Unconditional NFC normalization before any hash computation (D-15) | unit + property | `pytest tests/unit/normalize/test_unicode.py -x` | ⬜ pending |
| TBD | TBD | TBD | SCHEMA-01 | Conservative type inference (`001234` stays a string); bootstrap-only, contract always wins | unit | `pytest tests/unit/detect/test_schema.py -x` | ⬜ pending |
| TBD | TBD | TBD | SCHEMA-02 | `columns:` contract (type/nullable/required/business_key/semantics), cross-checked against `deduplication.keys` (D-18) | unit | `pytest tests/unit/test_dataset_config_columns.py -x` | ⬜ pending |
| TBD | TBD | TBD | SCHEMA-03 | Schema hashing/versioning; batch records dataset, schema version, hash, processor version, timestamp | unit | `pytest tests/unit/schema/test_versioning.py -x` | ⬜ pending |
| TBD | TBD | TBD | SCHEMA-04/05 | Compatible (evolve, detect+record) vs. breaking (freeze, `IncompatibleSchemaError`, whole file fails) classification per D-01/D-02/D-04 | unit | `pytest tests/unit/schema/test_evolution.py -x` | ⬜ pending |
| TBD | TBD | TBD | SCHEMA-06 | Historical schema hash-match resolution against `meta.schema_versions` history (D-16) | integration | `pytest tests/integration/test_schema_resolution.py -m integration` | ⬜ pending |
| TBD | TBD | TBD | LOAD-07 | Bounded memory through the decompression layer; configurable batch size, max field/row length | unit + nightly/large-fixture memory test | `pytest tests/unit/test_compression.py -k bounded -x` | ⬜ pending |
| TBD | TBD | TBD | QUAL-04 | Unit coverage of every detector/normalizer | unit | (covered by the rows above) | ⬜ pending |
| TBD | TBD | TBD | QUAL-12 | Schema evolution tested for both compatible and breaking changes | unit | (covered by SCHEMA-04/05 row) | ⬜ pending |
| TBD | TBD | TBD | QUAL-16 | Determinism property: identical source + config + processor version → identical output hash | property | `pytest tests/property/test_determinism.py -x` | ⬜ pending |
| TBD | TBD | TBD | QUAL-17 | Timezone/DST correctness property, including gap and overlap timestamps | property | `pytest tests/property/test_dst_correctness.py -x` | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Identified by `06-RESEARCH.md`'s own Wave 0 Gaps section — none of these exist yet, and several
block other tasks (dependency additions, corpus generator extensions) rather than being deferrable:

- [ ] `packages/csv-processor/pyproject.toml` — add `charset-normalizer>=3.4.9,<4`, `chardet>=7.5.1,<8`, `clevercsv>=0.8.5,<1` to `[project.dependencies]`
- [ ] `tests/unit/detect/__init__.py`, `tests/unit/normalize/__init__.py`, `tests/unit/schema/__init__.py` — new test package directories
- [ ] A shared conftest fixture parametrizing over `tests/fixtures/corpus.yaml`'s declared fixtures + `expect:` blocks — the corpus's own header comment states this is the intended shape ("Phase 6's detector tests are a parametrised loop over these declarations")
- [ ] `tools/corpus/manifest.py` — extend `_COMPRESSIONS` to include `"zip"` (needed before a `.zip` fixture can be declared at all — CSV-11 baseline coverage)
- [ ] `tools/corpus/generators.py` — extend `_write_wrapper` to build one-member zip archives (mirrors the existing gzip branch)
- [ ] A new `.zip` fixture entry in `tests/fixtures/corpus.yaml`, plus the corresponding count-assertion update in `tests/unit/test_corpus_semantic_fixtures.py` (currently hardcoded to 69)
- [ ] `migrations/versions/0009_meta_schema_versions.py` — new Alembic migration, closes migration 0004's deliberately-deferred FK

---

## Manual-Only Verifications

*None — all phase behaviors have automated verification. Schema-evolution proposals surfaced via
SQL query (D-06, no new tooling this phase) are inspectable but not a distinct "manual verification"
step — `meta.schema_versions` row assertions in `tests/unit/schema/test_evolution.py` cover the
same behavior automatically.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 10s (unit gate); integration tier explicitly exempted as wave-gate-only
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending — draft created from `06-RESEARCH.md`'s Validation Architecture section before
planning; the planner assigns real Task/Plan/Wave IDs to each row above.
