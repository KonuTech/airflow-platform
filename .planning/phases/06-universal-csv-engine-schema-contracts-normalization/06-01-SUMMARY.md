---
phase: 06-universal-csv-engine-schema-contracts-normalization
plan: 01
subsystem: infra
tags: [csv, charset-normalizer, chardet, clevercsv, alembic, postgresql, corpus-fixtures, zip, schema-versioning]

# Dependency graph
requires:
  - phase: 03-dataplat-core-library-metadata-control-plane
    provides: dataplat/csv_processor package layout conventions (config/__init__.py, sources/__init__.py shallow re-export pattern), migrations 0001 (meta.config_versions shape) and 0004 (schema_version_id's deferred FK)
  - phase: 01-repository-toolchain-ci-skeleton
    provides: tools/corpus/ seeded fixture generator framework (manifest.py, generators.py, digests.py) and the 69-fixture corpus.yaml oracle
provides:
  - charset-normalizer, chardet, clevercsv as direct csv_processor dependencies (installed, importable)
  - Six empty package directories (csv_processor.detect, dataplat.normalize, dataplat.schema + their tests/unit/ mirrors) for Wave 2's eleven plans to populate without __init__.py creation races
  - .zip compression support in the corpus generator (tools/corpus/manifest.py, generators.py) plus fixture 71_zipped.csv.zip (the corpus's 70th fixture)
  - meta.schema_versions table (migration 0009), with meta.ingestion_runs.schema_version_id's FK closed
affects: [06-02 through 06-18 (Phase 6 Wave 2+ plans consuming the detect/normalize/schema package dirs, the .zip fixture, and meta.schema_versions)]

# Tech tracking
tech-stack:
  added: [charset-normalizer 3.4.9 (already-transitive, now direct), chardet 7.6.0, clevercsv 0.8.5]
  patterns: ["Wrapper compression dispatch in tools/corpus/generators.py: one _write_wrapper entry point branches on fixture.compression, each branch pinning its own format-native determinism field (gzip mtime=0/filename=\"\"; zip ZipInfo.date_time=1980-01-01) per rule R5", "Deferred-FK-closing migration: a column lands nullable/unconstrained pointing at a not-yet-existing table; a later migration creates that table and closes the FK via op.create_foreign_key, with downgrade() reversing in strict opposite order"]

key-files:
  created:
    - packages/csv-processor/src/csv_processor/detect/__init__.py
    - packages/dataplat/src/dataplat/normalize/__init__.py
    - packages/dataplat/src/dataplat/schema/__init__.py
    - tests/unit/detect/__init__.py
    - tests/unit/normalize/__init__.py
    - tests/unit/schema/__init__.py
    - migrations/versions/0009_meta_schema_versions.py
  modified:
    - packages/csv-processor/pyproject.toml
    - uv.lock
    - tools/corpus/manifest.py
    - tools/corpus/generators.py
    - tests/fixtures/corpus.yaml
    - tests/fixtures/CORPUS.sha256
    - tests/unit/test_corpus_semantic_fixtures.py
    - tests/integration/test_migrations.py

key-decisions:
  - "tests/unit/{detect,normalize,schema}/__init__.py were written non-empty with real docstrings, not literally empty like tests/unit/__init__.py's current trivial shape -- the plan's own acceptance criteria explicitly requires all six __init__.py files non-empty, and tests/e2e/__init__.py / tests/regression/__init__.py already establish a non-empty-docstring convention for test-package markers (ruff INP001 + pytest import-mode collision avoidance)"
  - "Did NOT run requirements mark-complete for CSV-11/SCHEMA-03 despite them appearing in this plan's frontmatter -- grep across all 18 Phase 6 plan files shows CSV-11 also declared in 06-08/06-14/06-18 and SCHEMA-03 also declared in 06-12/06-15/06-16, confirming both are cumulative, multi-plan requirements. The mark-complete verb is an unconditional, immediate checkbox flip with no cross-plan awareness (read from milestone.cjs source); flipping it now -- after only Wave-0 scaffolding with zero actual .zip-reading or schema-version-recording logic -- would falsely claim completion while 5-6 more contributing plans remain unexecuted. REQUIREMENTS.md is left untouched (both stay Pending) pending whichever later plan is the real closing point."
  - "Ran tests/integration/test_migrations.py without the -m integration filter specified in the plan's own verification text -- repo-wide grep confirms no 'integration' pytest marker is registered in pyproject.toml's markers list or applied anywhere via decorator, so -m integration silently deselects all 8 tests and exits 0 without running any of them. Ran the suite via the same invocation make test-integration actually uses (pytest tests/integration/test_migrations.py -x -q, --group cluster) to get real pass/fail signal: 8/8 passed."

patterns-established:
  - "Wrapper compression dispatch (gzip/zip) in tools/corpus/generators.py, extensible to future compression kinds by adding another elif branch plus a _COMPRESSIONS entry in manifest.py"
  - "Deferred-FK-closing migration shape, reusable for any future column deliberately landed unconstrained ahead of its referent table"

requirements-completed: []

# Metrics
duration: ~22min
completed: 2026-08-15
---

# Phase 6 Plan 01: Wave-0 Groundwork Summary

**Declared charset-normalizer/chardet/clevercsv as csv_processor dependencies, taught the corpus generator to build `.zip` fixtures, and added the `meta.schema_versions` migration that closes migration 0004's deferred FK — pure prep work for Phase 6's eleven parallel Wave 2 plans.**

## Performance

- **Duration:** ~22 min (estimate — no explicit start timestamp was captured before the first file read; based on the first task commit at 11:23:27+02:00 through the third at 11:32:26+02:00 plus setup/verification time either side)
- **Completed:** 2026-08-15T09:33:30Z
- **Tasks:** 3/3 completed
- **Files modified:** 15 (7 created, 8 modified)

## Accomplishments

- `packages/csv-processor` now directly depends on `charset-normalizer>=3.4.9,<4`, `chardet>=7.5.1,<8`, `clevercsv>=0.8.5,<1` (resolved to 7.6.0/0.8.5 respectively; `uv.lock` relocked, `uv sync` installs cleanly), and six new empty package directories exist so Wave 2's eleven parallel plans never race to create the same `__init__.py`
- `tools/corpus/generators.py`'s `_write_wrapper` now dispatches on `gzip`/`zip`, with the zip branch pinning `ZipInfo.date_time` to `1980-01-01` for the same byte-identity reproducibility guarantee gzip's `mtime=0`/`filename=""` already provides; the corpus grew to 70 fixtures with `71_zipped.csv.zip` giving CSV-11's `.zip` half real coverage
- `meta.schema_versions` exists via migration 0009, mirroring `meta.config_versions`'s shape exactly (`UNIQUE(dataset_id, version)` + partial unique index for the current row, `hash_version` companion column), and `meta.ingestion_runs.schema_version_id` now carries a real foreign key closing migration 0004's explicitly-deferred constraint

## Task Commits

Each task was committed atomically:

1. **Task 1: Declare the three detection libraries as dependencies; create every new package directory** - `544414d` (feat)
2. **Task 2: Teach the corpus generator to build .zip fixtures; add the .zip fixture; fix the fixture-count assertion** - `1e3371c` (feat)
3. **Task 3: meta.schema_versions migration; flip the pre-0009 FK-absence test to FK-presence** - `5494873` (feat)

**Plan metadata:** SUMMARY.md commit follows (this commit, worktree mode — STATE.md/ROADMAP.md excluded, owned by the orchestrator)

## Files Created/Modified

- `packages/csv-processor/pyproject.toml` - adds the three detection library pins as direct dependencies, with a comment explaining why they belong to csv_processor and not dataplat
- `uv.lock` - relocked; resolved chardet 7.6.0, clevercsv 0.8.5 (charset-normalizer was already transitively present)
- `packages/csv-processor/src/csv_processor/detect/__init__.py` - new empty package marker, home to Wave 2's CSV-01/02/03/04/05/06/07/08 detectors
- `packages/dataplat/src/dataplat/normalize/__init__.py` - new empty package marker, home to Wave 2's CSV-09/10/12 `StreamingStage` normalizers
- `packages/dataplat/src/dataplat/schema/__init__.py` - new empty package marker, home to Wave 2's SCHEMA-03/04/05/06 versioning/evolution/repository modules
- `tests/unit/detect/__init__.py`, `tests/unit/normalize/__init__.py`, `tests/unit/schema/__init__.py` - test-package mirrors of the three above
- `tools/corpus/manifest.py` - `_COMPRESSIONS` now accepts `"zip"` alongside `"gzip"`
- `tools/corpus/generators.py` - `_write_wrapper` dispatches gzip/zip; zip branch uses `zipfile.ZipFile`/`zipfile.ZipInfo` with a pinned `date_time`
- `tests/fixtures/corpus.yaml` - adds fixture `71_zipped.csv.zip` (the corpus's 70th) plus one sentence in the header comment noting the Phase 6 growth
- `tests/fixtures/CORPUS.sha256` - regenerated; one new digest line appended for the new fixture, no reordering
- `tests/unit/test_corpus_semantic_fixtures.py` - `71_zipped.csv.zip` added to `FEATURES_THREE_FOUR`; `sixty-nine`→`seventy` count references updated
- `migrations/versions/0009_meta_schema_versions.py` - new: `meta.schema_versions` table + closes `ingestion_runs.schema_version_id`'s deferred FK
- `tests/integration/test_migrations.py` - `test_ingestion_runs_schema_version_id_has_no_fk` renamed to `test_ingestion_runs_schema_version_id_has_an_fk_after_0009` with an inverted assertion; `EXPECTED_TABLES`/`HASH_VERSION_COLUMNS` extended for `meta.schema_versions`

## Decisions Made

- **Resolved a plan-internal ambiguity on the three `tests/unit/*/` `__init__.py` files in favor of the acceptance criteria.** The plan's action text said "mirror `tests/unit/__init__.py`'s existing (trivial) shape exactly" (that file is 0 bytes), but the same task's acceptance criteria requires "All six `__init__.py` files exist and each is non-empty (a real docstring, not a placeholder)." Followed the acceptance criteria and the established precedent of `tests/e2e/__init__.py`/`tests/regression/__init__.py` (both non-empty, both explaining they exist to satisfy ruff's `INP001` and avoid pytest's same-basename import-mode collision).
- **Did not mark CSV-11 or SCHEMA-03 complete in REQUIREMENTS.md**, despite both appearing in this plan's frontmatter `requirements:` field. See the frontmatter `key-decisions` entry above for the full reasoning — both IDs recur across multiple not-yet-executed Phase 6 plans, and this plan's own scope is explicitly "no detection logic, no normalization logic, nothing dataset-facing." Left both at `Pending` in REQUIREMENTS.md; no REQUIREMENTS.md edit was made by this plan.
- **Verified `tests/integration/test_migrations.py` without the plan-specified `-m integration` filter**, since no such pytest marker exists anywhere in this codebase (confirmed via repo-wide grep) — that filter silently deselects all tests and exits 0 having run nothing. Used the same invocation `make test-integration` performs instead (`--group cluster`, no marker filter): 8/8 passed against a real throwaway PostgreSQL 18 container.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Removed the literal old test name from the new test's docstring**
- **Found during:** Task 3, self-verification against the plan's own acceptance criteria
- **Issue:** The plan's acceptance criteria requires `grep -n "test_ingestion_runs_schema_version_id_has_no_fk" tests/integration/test_migrations.py` to return no match. My first draft of the renamed test's docstring referenced the old function name in backticks for context, which itself matched that grep.
- **Fix:** Rephrased the docstring to describe "this test's predecessor" instead of naming it literally.
- **Files modified:** `tests/integration/test_migrations.py`
- **Verification:** `grep -n "test_ingestion_runs_schema_version_id_has_no_fk" tests/integration/test_migrations.py` now exits 1 (no match); full suite still 8/8 passing.
- **Committed in:** `5494873` (Task 3 commit)

**2. [Rule 1 - Bug] Fixed the module docstring's now-inverted fifth property claim**
- **Found during:** Task 3
- **Issue:** `tests/integration/test_migrations.py`'s module docstring described the fifth tested property as "an accidental foreign key on `ingestion_runs.schema_version_id` before `meta.schema_versions` exists" — exactly backwards after this task inverts the test to require the FK's presence.
- **Fix:** Reworded to "a missing foreign key on `ingestion_runs.schema_version_id` now that migration 0009 has created its referent."
- **Files modified:** `tests/integration/test_migrations.py`
- **Verification:** Read back; matches the actual (now-inverted) test behavior.
- **Committed in:** `5494873` (Task 3 commit)

**3. [Rule 1 - Bug] Fixed two ruff E501/W505 line-length violations in new docstrings**
- **Found during:** Task 1, pre-commit lint pass
- **Issue:** `packages/dataplat/src/dataplat/normalize/__init__.py` and `tests/unit/schema/__init__.py`'s first docstring lines exceeded the project's 100-char limit by one character.
- **Fix:** Reworded/reflowed the summary lines (also fixed a resulting D205 "blank line required" violation from the first attempt).
- **Files modified:** `packages/dataplat/src/dataplat/normalize/__init__.py`, `tests/unit/schema/__init__.py`
- **Verification:** `ruff check` clean on both files afterward; full-repo `ruff check .` also clean.
- **Committed in:** `544414d` (Task 1 commit)

---

**Total deviations:** 3 auto-fixed (all Rule 1 — bugs in my own first-draft text, caught before commit via self-verification against the plan's stated acceptance criteria and grep patterns)
**Impact on plan:** No scope creep. All three are corrections to files already in this plan's declared scope, each verified against the plan's own explicit acceptance criteria or standard lint gates.

## Issues Encountered

- **`packages/csv-processor/pyproject.toml`'s dependency comment cites `setup.cfg`'s import-linter contract 1** — verified this file exists at the repo root before writing the comment, to avoid asserting an inaccurate cross-reference.
- **`chardet` resolved to `7.6.0`, not `7.5.1`** — within the plan's own declared range (`>=7.5.1,<8`) and matches 06-RESEARCH.md's "State of the Art" note that 7.6.0 was released 2026-08-14, one day before this session. No action needed.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Wave 2's eleven parallel plans can now write into `csv_processor/detect/`, `dataplat/normalize/`, `dataplat/schema/` (and their `tests/unit/` mirrors) without any `__init__.py` creation race — each directory already exists with a docstring naming the requirement IDs it will host.
- `charset_normalizer`, `chardet`, `clevercsv` are installed and importable from the `csv-processor` package; any Wave 2 detector plan can `import` them immediately.
- `meta.schema_versions` exists with the exact `ARCHITECTURE.md` §2.1 shape and a real FK from `meta.ingestion_runs.schema_version_id` — ready for whichever Wave 2 plan builds the write path (`dataplat.schema.repository`, per 06-RESEARCH.md's package layout).
- The corpus is 70 fixtures, including `.zip` coverage for CSV-11; `make fixtures-verify` passes against the regenerated oracle.
- **Outstanding for a later plan/phase-verification step:** decide which specific later plan(s) are the actual closing point for CSV-11 (also declared in 06-08, 06-14, 06-18) and SCHEMA-03 (also declared in 06-12, 06-15, 06-16) before calling `requirements mark-complete` on either ID — this plan deliberately left both `Pending`.

## Self-Check: PASSED

All 7 created files verified present on disk; all 3 task commit hashes
(`544414d`, `1e3371c`, `5494873`) verified present in `git log`.

---
*Phase: 06-universal-csv-engine-schema-contracts-normalization*
*Completed: 2026-08-15*
