---
phase: 06-universal-csv-engine-schema-contracts-normalization
plan: 17
subsystem: testing
tags: [hypothesis, property-testing, determinism, postgresql, minio, testcontainers, pydantic]

# Dependency graph
requires:
  - phase: 06-universal-csv-engine-schema-contracts-normalization
    provides: "StagingLoader.load() wired with real normalizers (plan 06-16) and CsvSource with real encoding/dialect/header detection (plans 06-14/06-15) -- the real pipeline entry point this property drives"
provides:
  - "tests/property/test_determinism.py -- QUAL-16's own property test: identical source bytes + identical DatasetConfig staged twice via real StagingLoader.load() calls produce an identical ordered _record_hash list, proven over Hypothesis-generated customers-shaped tables"
  - "A second, deterministic proof that the hash is not vacuously constant: a genuine NormalizationConfig.null_sentinels difference changes what gets staged/hashed"
  - "The `integration` pytest marker, now registered in pyproject.toml, available to any future Docker-dependent property/unit test"
affects: [06-VERIFICATION, phase-6-closure]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Cross-directory pytest fixture re-export: `from tests.integration.conftest import (X, Y, Z)  # noqa: F401` inside a sibling tests/property/ module, mirroring tests/e2e/slice/conftest.py's existing precedent for reaching a sibling directory's session-scoped testcontainers fixtures"
    - "Hypothesis @given combined with function-scoped pytest fixtures (real DB/MinIO connections), explicitly via `suppress_health_check=[HealthCheck.function_scoped_fixture]` -- intentional fixture reuse across internal examples, safe because every example writes to its own uniquely-named staging table"
    - "Reusing an existing fixture manifest's own value pools (tests/fixtures/slice-corpus.yaml, parsed via yaml.safe_load) as a Hypothesis strategy's generation source, rather than hand-typing a duplicate list"

key-files:
  created:
    - tests/property/test_determinism.py
  modified:
    - pyproject.toml
    - .planning/phases/06-universal-csv-engine-schema-contracts-normalization/deferred-items.md

key-decisions:
  - "Used NormalizationConfig.null_sentinels (not decimal_separator) for the non-vacuousness proof -- customers has no numeric/currency column, so decimal_separator (06-RESEARCH.md's own named example) cannot be exercised; null_sentinels is still a genuine NormalizationConfig field and directly exercises customers' one real nullable column (birth_date)"
  - "Registered a new `integration` pytest marker in pyproject.toml (Rule 3 auto-fix) -- the plan's own verification commands (`-m integration`) cannot collect under --strict-markers without it, and a full-repo grep found zero prior uses of the marker the plan assumed already existed"
  - "Two test functions, not one -- the required test_identical_input_yields_identical_output_hash is a Hypothesis-driven universal property (25 examples); the non-vacuousness proof is a separate, deterministic single-example test, since it is an existence claim ('some config change moves the hash'), not a universal one"

requirements-completed: [QUAL-16]

# Metrics
duration: 35min
completed: 2026-08-15
---

# Phase 6 Plan 17: Determinism Property Test Summary

**QUAL-16's determinism property proven against the real, fully-wired pipeline: two Hypothesis-driven `StagingLoader.load()` calls over identical source bytes/config produce an identical `_record_hash` list, and a genuine `NormalizationConfig` change is proven to move at least one hash — the Core Value's "same input, same result" claim made literally testable.**

## Performance

- **Duration:** ~35 min
- **Started:** 2026-08-15 (session start)
- **Completed:** 2026-08-15T15:49:54Z
- **Tasks:** 1/1
- **Files modified:** 3 (1 created, 2 modified)

## Accomplishments

- `tests/property/test_determinism.py::test_identical_input_yields_identical_output_hash` — a Hypothesis property (`max_examples=25`, `deadline=None`) generating 2-10-row `customers`-shaped tables from `tests/fixtures/slice-corpus.yaml`'s own value pools, staging each table twice via real `StagingLoader.load()` calls (fresh `run_id`/staging table each time) against a throwaway testcontainers PostgreSQL + MinIO, and asserting the two ordered `_record_hash` lists are byte-identical
- `tests/property/test_determinism.py::test_a_genuine_normalization_config_change_yields_a_different_hash_set` — a focused, deterministic proof that the hash is not vacuously constant: a `null_sentinels={"birth_date": ["N/A"]}` config difference turns a row DateNormalizer would otherwise reject (`"N/A"` matches no default null token) into a row that stages successfully with `birth_date` SQL `NULL` — the resulting hash sets provably differ, and the untouched clean row's hash provably does not
- Live-verified: both tests pass (2/2, 20.76s wall time, 14.24s of which is one-time testcontainers startup — well inside the project's feedback-latency conventions), and the full offline sweep (`tests/unit tests/property tests/regression`, 380 tests) stays green

## Task Commits

Each task was committed atomically:

1. **Task 1: Determinism property — identical input+config+version yields an identical hash set** - `620fbd5` (feat)

_No separate plan-metadata commit: this plan runs inside a parallel worktree (isolation="worktree"); per the orchestrator's explicit instruction, STATE.md/ROADMAP.md are never touched here, and this task's own commit already carries SUMMARY.md's companion files (pyproject.toml, deferred-items.md) alongside the test file itself._

## Files Created/Modified

- `tests/property/test_determinism.py` - The two properties described above, plus their shared helpers (`_customers_rows` Hypothesis strategy, `_csv_bytes`/`_upload`/`_stage_and_hash`), fixtures (`object_store`, `conn`, `base_config`, `_ensure_raw_bucket`), and fixtures re-exported from `tests/integration/conftest.py` (`_require_docker`, `postgres_dsn`, `run_migrations`, `migrated_dsn`, `minio_config`, `s3_client`)
- `pyproject.toml` - Registered the `integration` pytest marker (Rule 3 auto-fix; see Deviations)
- `.planning/phases/06-universal-csv-engine-schema-contracts-normalization/deferred-items.md` - Logged the plan's third verification command's pre-existing marker gap (see Deviations)

## Decisions Made

- **`null_sentinels` over `decimal_separator` for the non-vacuousness proof.** 06-RESEARCH.md's own Code Examples section named `decimal_separator` as the example config field to vary, but `customers` (the platform's one real dataset) has no numeric/currency column — nothing in its schema would ever route through `NumericNormalizer`. `null_sentinels` is still a genuine `NormalizationConfig` field, and it directly exercises `customers`' one real nullable column (`birth_date`), producing a deterministic, always-correct proof rather than one that depends on which values a Hypothesis-generated table happens to contain.
- **A deterministic single example for the non-vacuousness test, not a Hypothesis range.** The "same config twice" property is a universal claim (true for every valid input) and belongs under `@given`. "A genuine config change moves the hash" is an existence claim (true for at least one input) — a single, well-reasoned hand-picked table (one clean row, one `"N/A"`-birth_date row) proves it cleanly and avoids a subtle correctness trap: several other candidate config differences (e.g. reinterpreting a date format's day/month order) only produce a genuinely different value for SOME Hypothesis-drawn dates and not others, which would make a Hypothesis-driven version of this specific property flaky.
- **Cross-directory fixture re-export via direct import**, not a new `tests/property/conftest.py`. `tests/property/` and `tests/integration/` are siblings, so pytest's own conftest inheritance cannot reach `tests/integration/conftest.py`'s testcontainers fixtures for free. `tests/e2e/slice/conftest.py` already establishes the exact pattern needed (`from tests.e2e.cluster.conftest import (X, Y, Z)  # noqa: F401 -- re-exported as pytest fixtures below`) for the identical reason; this plan's own `files_modified` scope (`tests/property/test_determinism.py` only) made importing directly into the test module itself — rather than adding a new conftest.py — the natural fit.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Registered the missing `integration` pytest marker**
- **Found during:** Task 1, first `ruff`/`pytest` verification pass
- **Issue:** The plan's `<action>` text and `<verification>` section both assume "the project's existing `@pytest.mark.integration` convention (matching every other Docker-dependent test in this repository)" — but a full-repo grep found zero prior uses of this marker anywhere, and it was not registered in `pyproject.toml`'s `markers` list. Under `--strict-markers` (already set), using an unregistered marker fails collection outright — the plan's own `pytest tests/property/test_determinism.py -m integration -x -q` acceptance-criteria command could not even run.
- **Fix:** Added `"integration: needs a local Docker daemon (testcontainers PostgreSQL/MinIO); excluded from the offline gate (06-17-PLAN.md)"` to `pyproject.toml`'s `markers` list, and applied `@pytest.mark.integration` to both new test functions.
- **Files modified:** `pyproject.toml`
- **Verification:** `pytest tests/property/test_determinism.py -m integration -x -q` — 2 passed in 20.76s.
- **Committed in:** `620fbd5` (Task 1 commit)

**2. [Rule 1 - Bug] Fixed ruff/mypy findings in the new test file**
- **Found during:** Task 1, static-analysis verification pass
- **Issue:** Two docstring lines exceeded the project's 100-char limit (E501/W505); three locally-defined fixtures (`_ensure_raw_bucket`, `object_store`, `conn`) took a re-exported fixture name as their own parameter (`s3_client`, `minio_config`, `migrated_dsn`), which pyflakes flags as F811 "redefinition" even though this is the correct, required pytest fixture-injection shape; `import yaml` has no bundled type stubs.
- **Fix:** Shortened the two docstring lines; added `# noqa: F811 -- pytest fixture-injection param name, not a real redefinition` on the three parameters, matching the exact precedent already established in `tests/e2e/slice/conftest.py` and `tests/e2e/vault/test_airflow_backend.py`; added `# type: ignore[import-untyped]` on the `yaml` import, matching `dataplat/config/loader.py`'s own identical suppression for the same reason.
- **Files modified:** `tests/property/test_determinism.py`
- **Verification:** `ruff check`, `ruff format --check`, and `mypy tests/property/test_determinism.py` all clean (the only remaining mypy output is two pre-existing, out-of-scope `testcontainers.community.*` stub errors inside `tests/integration/conftest.py` itself — confirmed present when running `mypy` directly against that file too, unrelated to this plan, and outside `make typecheck`'s own `TYPECHECK_PATHS` in any case).
- **Committed in:** `620fbd5` (Task 1 commit)

---

**Total deviations:** 2 auto-fixed (1 blocking/Rule 3, 1 bug/Rule 1)
**Impact on plan:** Both fixes were necessary for the plan's own stated acceptance criteria to be achievable at all. No scope creep — no other files were touched.

## Issues Encountered

- **The plan's third `<verification>` command does not select any tests.** `pytest tests/integration -m integration -q` returns `77 deselected` (exit code 5): none of the 77 pre-existing tests across 8 files in `tests/integration/` carry the `@pytest.mark.integration` marker this plan's text assumed was already a repo-wide convention. The REAL, actually-established convention for `tests/integration/` is folder-based (`make test-integration` runs `pytest tests/integration -q`, no marker filter). This is out of this plan's declared `files_modified: [tests/property/test_determinism.py]` scope — fixing it would mean touching 8 unrelated files (~77 tests). Logged to `deferred-items.md` for a future cleanup pass; does not affect this plan's own acceptance criteria (all of which reference `tests/property/test_determinism.py` directly and pass). Also noted: `make test-integration`'s Makefile recipe (`pytest tests/integration -q`, a folder path) would never collect `tests/property/test_determinism.py` either way — also logged, also out of scope.

## User Setup Required

None - no external service configuration required. (The test needs a local Docker daemon, already verified present and working in this environment; `tests/integration/conftest.py`'s own `_require_docker` fixture, re-exported into this file, skips gracefully with a named reason wherever Docker is unavailable.)

## Next Phase Readiness

- This is the last plan of Phase 6 (per the plan's own `<objective>`). `tests/property/test_determinism.py` is the phase's own closing proof that detection → normalization → hashing (plans 06-14/06-15/06-16) behave deterministically end-to-end, live-verified against a real pipeline, not asserted from unit-level pieces alone.
- No blockers for phase closure introduced by this plan. The one open item (the `-m integration` marker gap across the other 8 `tests/integration/` files) is pre-existing, independent of this plan's own correctness, and already logged for a future cleanup pass rather than blocking anything.

---
*Phase: 06-universal-csv-engine-schema-contracts-normalization*
*Completed: 2026-08-15*
