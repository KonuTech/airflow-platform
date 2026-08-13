---
phase: 03-dataplat-core-library-metadata-control-plane
plan: 02
subsystem: database
tags: [alembic, postgresql, sqlalchemy, testcontainers, migrations, metadata-schema]

# Dependency graph
requires:
  - phase: 03-dataplat-core-library-metadata-control-plane (plan 03-01)
    provides: "dataplat's real runtime dependencies (psycopg, boto3, pydantic, structlog, click) already wired in packages/dataplat/pyproject.toml — this plan adds alembic/sqlalchemy/testcontainers alongside them at the root workspace level"
provides:
  - "A hand-written Alembic environment (migrations/) with a wrong-database guard that fails loudly before any DDL runs against a database that is not the analytical PostgreSQL"
  - "Five revisions creating the complete vertical-slice meta schema: meta.datasets, meta.config_versions, meta.files, meta.batches, meta.batch_files, meta.ingestion_runs, plus normalized.customers with its six embedded lineage columns"
  - "Every stored hash (content_sha256, config_hash, _record_hash) carries a companion hash_version smallint NOT NULL DEFAULT 1 column (META-02)"
  - "meta.ingestion_runs.schema_version_id lands nullable and unconstrained — the one deliberately deferred FK, since meta.schema_versions is a later phase's table"
  - "Per-table GRANT SELECT, INSERT, UPDATE TO etl_app on every one of the seven tables this phase creates — never GRANT ALL, never schema-wide"
  - "tests/integration/conftest.py — session-scoped testcontainers PostgreSQL 18 + MinIO fixtures, plus an in-process alembic upgrade head helper, that every later Phase-3 integration test can reuse"
  - "make test-integration — its own Make target and CI job, never a prerequisite of check/ci (D-04)"
affects: [03-03, 03-04, 03-05, 03-06, 03-07, 03-08]

# Tech tracking
tech-stack:
  added:
    - "alembic 1.19.1 (root dev group) — hand-written revisions only, autogenerate never used for committed output"
    - "sqlalchemy 2.0.52 (root dev group) — Alembic's own engine only, never for application row loading"
    - "boto3-stubs[s3] 1.43.70 (root dev group) — mypy stubs, not yet consumed by any typechecked module"
    - "testcontainers[postgres,minio] 4.15.0 (root cluster group) — PostgresContainer/MinioContainer via the non-deprecated testcontainers.community.* import path"
  patterns:
    - "Alembic wrong-database guard: SELECT current_database() checked against an EXPECTED_DATABASE constant, raising RuntimeError before context.configure() — see migrations/env.py"
    - "Deferred foreign key: a column lands in the migration that a coherent up-front design calls for, but stays nullable/unconstrained when its target table belongs to a later phase; the constraint is added later via op.create_foreign_key"
    - "hash_version smallint NOT NULL DEFAULT 1 beside every stored hash column, applied here to all three hashes this phase mints (content_sha256, config_hash, _record_hash)"
    - "testcontainers session-scoped fixtures with an autouse _require_docker skip-guard, mirroring tests/e2e/cluster/conftest.py's _require_cluster convention"

key-files:
  created:
    - migrations/alembic.ini
    - migrations/env.py
    - migrations/script.py.mako
    - migrations/README
    - migrations/versions/0001_meta_datasets_config_versions.py
    - migrations/versions/0002_meta_files.py
    - migrations/versions/0003_meta_batches_batch_files.py
    - migrations/versions/0004_meta_ingestion_runs.py
    - migrations/versions/0005_normalized_customers.py
    - tests/integration/__init__.py
    - tests/integration/conftest.py
    - tests/integration/test_migrations.py
  modified:
    - pyproject.toml
    - uv.lock
    - Makefile
    - .github/workflows/ci.yml

key-decisions:
  - "env.py explicitly ensures schema meta exists (committed) before context.configure()/context.run_migrations() — Alembic creates its own alembic_version bookkeeping table in version_table_schema before running any revision's upgrade(), so the schema must already exist even though revision 0001 also (idempotently) creates it"
  - "test_etl_app_grants and GRANTED_TABLES check all seven tables (six meta.* plus normalized.customers), a strict superset of the plan's literal 'six tables' wording — normalized.customers carries the identical GRANT shape and the threat model's T-03-04 mitigation names 'every migration's GRANT statement', not a subset"
  - "postgres_dsn fixture passes dbname='analytics' explicitly to PostgresContainer — testcontainers' own default ('test') would (and during development, did) trip the wrong-database guard, which was the guard doing its job correctly against the wrong input, not a guard bug"
  - "testcontainers.community.postgres/testcontainers.community.minio used instead of the plan-quoted testcontainers.postgres/testcontainers.minio — the non-community paths are deprecated as of testcontainers 4.15.0 (DeprecationWarning at import time); identical API, no behavior change"
  - "pyproject.toml gains a migrations/** per-file-ignore for ruff's INP001 (implicit namespace package) — migrations/ is Alembic's own module-loading mechanism (env.py execs each versions/*.py by file path), never a real Python package, matching the existing airflow/dags/** carve-out"
  - "cluster group's comment rewritten rather than removing boto3/psycopg from it now that dataplat depends on them directly — tests/e2e/cluster/conftest.py (D-16) still imports them at its own conftest module level independent of dataplat's resolution, so removing them would couple that test tier's install requirement to dataplat's dependency list by accident"

patterns-established:
  - "Alembic wrong-database guard (migrations/env.py) — every future migration in this environment inherits the same protection without any per-revision code"
  - "hash_version companion column — the shape every later phase's new stored hash should copy"
  - "Deferred FK column (schema_version_id) — the shape for any future column whose target table belongs to a phase that hasn't landed yet"
  - "testcontainers session-scoped fixture set in tests/integration/conftest.py — postgres_dsn, run_migrations, migrated_dsn, minio_config, s3_client are all designed for reuse by 03-04 through 03-08's own integration tests, not just this plan's own test_migrations.py"

requirements-completed: [META-01, META-02]

# Metrics
duration: 245min
completed: 2026-08-13
---

# Phase 3 Plan 2: Metadata Control Plane — Alembic Migrations & Testcontainers Harness Summary

**Five hand-written Alembic revisions creating the complete vertical-slice `meta`/`normalized` schema (7 tables, hash_version everywhere, one deliberately deferred FK, per-table etl_app grants), proven against real testcontainers PostgreSQL 18 by a five-test suite plus a new `make test-integration` target and CI job.**

## Performance

- **Duration:** 245 min (includes a ~9 min background `tests/policy` run and multiple `docker info` round-trips measured at ~10s each in this environment)
- **Started:** 2026-08-13T00:22:00Z
- **Completed:** 2026-08-13T04:27:31Z
- **Tasks:** 3
- **Files modified:** 18 (12 created, 4 modified, 2 deleted)

## Accomplishments

- `migrations/env.py`'s wrong-database guard (`SELECT current_database()` vs `EXPECTED_DATABASE = "analytics"`) proven empirically during this execution: it correctly rejected testcontainers' default `test` database before any DDL ran, and the fix (passing `dbname="analytics"`) was to the test fixture, not the guard
- All five revisions (`0001`–`0005`) create exactly the vertical slice — `meta.datasets`, `meta.config_versions`, `meta.files`, `meta.batches`, `meta.batch_files`, `meta.ingestion_runs`, `normalized.customers` — and nothing else; the ~14 remaining `meta.*` tables from ARCHITECTURE.md §2.2 stay absent, deferred to whichever phase first populates them
- `meta.files.hash_version`, `meta.config_versions.hash_version` and `normalized.customers._record_hash_version` all exist as `smallint NOT NULL DEFAULT 1` (META-02) — `_record_hash_version` extends D-05's two named examples to the one hash this phase itself mints
- `meta.ingestion_runs.schema_version_id` lands nullable with no FK constraint, proven by a dedicated test querying `information_schema.table_constraints`/`key_column_usage`
- Every migration's `GRANT` is `SELECT, INSERT, UPDATE` only, per table, never `GRANT ALL`/schema-wide — proven by `test_etl_app_grants`, and the test's own regression-catching power verified directly (a scratch `GRANT ... DELETE` was added, confirmed caught, then discarded via `git checkout --`)
- `tests/integration/conftest.py` gives every later Phase-3 plan session-scoped `postgres_dsn`, `run_migrations`, `migrated_dsn`, `minio_config` and `s3_client` fixtures for free
- `make test-integration` runs in its own CI job (`integration`), reachable from neither `check` nor `ci`, exactly as D-04 requires — verified structurally (grep against the Makefile and workflow) and behaviorally (`make test-integration` executed against real Docker, all 5 tests passed)
- Full remaining `make check` gate (lint, format, mypy strict, import-linter, 105 `tests/policy` tests, 60 `tests/unit`+`tests/regression` tests, fixtures-verify) all pass with every change in this plan present

## Task Commits

Each task was committed atomically:

1. **Task 1: Alembic environment, dependency wiring, and the first two tables** - `42384f6` (feat)
2. **Task 2: The remaining four tables — files, batches, ingestion_runs (deferred FK), normalized.customers** - `112280f` (feat)
3. **Task 3: Testcontainers harness, the META-01/META-02 proof, and make test-integration** - `4d0d138` (feat)

_No TDD tasks in this plan — all three are `type="auto"` with inline shell verification plus a genuine end-to-end run against real Docker containers, per the plan's own text._

## Files Created/Modified

- `pyproject.toml` — `dev` gains `alembic`, `sqlalchemy`, `boto3-stubs[s3]`; `cluster` gains `testcontainers[postgres,minio]`; `cluster` group comment rewritten (the old "keeps boto3/psycopg out of make check" claim is false since dataplat itself now depends on them); new `migrations/**` per-file-ignore for ruff's `INP001`
- `uv.lock` — regenerated; alembic, sqlalchemy, testcontainers and their transitive dependencies resolved (including the `minio` SDK package as testcontainers' own transitive dependency for its MinIO container readiness check — never imported by this project's own code)
- `migrations/alembic.ini` — `script_location = %(here)s`, blank `sqlalchemy.url` (DSN comes from `ALEMBIC_DSN` at runtime)
- `migrations/env.py` — the wrong-database guard; ensures schema `meta` exists (committed) before Alembic's own `alembic_version` bookkeeping table creation; imports only sqlalchemy/alembic
- `migrations/script.py.mako` — Alembic's own default template, unmodified (generated via `alembic init`)
- `migrations/README` — Alembic's own default scaffold marker, kept as generated
- `migrations/versions/0001_meta_datasets_config_versions.py` — `CREATE SCHEMA IF NOT EXISTS meta`, then `meta.datasets` and `meta.config_versions` (with `hash_version`)
- `migrations/versions/0002_meta_files.py` — `meta.files`, with `content_sha256`/`hash_version` and the self-FK `duplicate_of_file_id`
- `migrations/versions/0003_meta_batches_batch_files.py` — `meta.batches` and the `meta.batch_files` join table
- `migrations/versions/0004_meta_ingestion_runs.py` — the central run table; every column from ARCHITECTURE.md §2.1, with `schema_version_id` deliberately unconstrained
- `migrations/versions/0005_normalized_customers.py` — `CREATE SCHEMA IF NOT EXISTS normalized`, then `normalized.customers` with all six lineage columns plus `_record_hash_version`
- `tests/integration/__init__.py` — empty package marker
- `tests/integration/conftest.py` — testcontainers PostgreSQL/MinIO fixtures, the in-process `alembic upgrade head` helper, `_require_docker` skip guard
- `tests/integration/test_migrations.py` — the five META-01/META-02 proof tests
- `Makefile` — new `test-integration` target (`RUN_CLUSTER`, D-04), added to `.PHONY`
- `.github/workflows/ci.yml` — new `integration` job, offline of any live cluster

## Decisions Made

- **env.py's schema-bootstrap fix (Task 3).** Alembic creates its own `alembic_version` bookkeeping table in `version_table_schema` ("meta") *before* running any revision's `upgrade()` — including revision 0001, which is otherwise the one place `CREATE SCHEMA meta` lives. Against a genuinely empty database this made `alembic upgrade head` fail immediately with `InvalidSchemaName`. Fixed by having `env.py` ensure schema `meta` exists (and commit it) ahead of `context.configure()`. Revision 0001 still issues its own idempotent `CREATE SCHEMA IF NOT EXISTS meta` as defense in depth, per the plan's literal instruction — the two are complementary, not redundant.
- **`GRANTED_TABLES`/`test_etl_app_grants` cover all seven tables, not six.** The plan's acceptance-criteria prose says "six tables" (matching the six `meta.*` tables), but `normalized.customers` carries the identical `GRANT SELECT, INSERT, UPDATE` shape and the threat model's own T-03-04 mitigation text says "every migration's GRANT statement," not a subset. Testing all seven is a strict superset of what the plan's wording asks for, not a deviation from it.
- **`dbname="analytics"` added to the `postgres_dsn` fixture's `PostgresContainer` call.** testcontainers' own default database name is `test`, which is exactly the input `migrations/env.py`'s wrong-database guard is designed to reject — and did, during this execution, before the fixture was corrected. This is documented in the fixture's own docstring as proof the guard works, not merely a config tweak.
- **`testcontainers.community.postgres`/`testcontainers.community.minio` used instead of the plan-quoted `testcontainers.postgres`/`testcontainers.minio`.** The non-`community` import paths are deprecated as of testcontainers 4.15.0 (confirmed via a `DeprecationWarning` at import time, this session); the `community` paths expose the identical `PostgresContainer`/`MinioContainer` classes and constructor signatures — verified via `inspect.signature` before switching.
- **`_require_docker`'s `docker info` timeout raised from 10s to 30s.** Measured directly in this environment: `docker info` alone consistently takes ~10 seconds, so a 10-second ceiling produced `TimeoutExpired` (a false-negative skip reason) rather than a real "daemon unreachable" signal.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] `migrations/**` needed a ruff `INP001` per-file-ignore**
- **Found during:** Task 1
- **Issue:** `migrations/env.py` and every `migrations/versions/*.py` file triggered ruff's `INP001` ("part of an implicit namespace package") — `migrations/` has no `__init__.py` by design (Alembic loads each `versions/*.py` by file path, never via `import migrations.versions...`), the same reasoning already carved out for `airflow/dags/**`.
- **Fix:** Added `"migrations/**" = ["INP001"]` to `pyproject.toml`'s `[tool.ruff.lint.per-file-ignores]`, with a comment explaining why (and noting ruff's `N999` does not fire on non-package modules, so revision filenames' leading digits need no separate carve-out).
- **Files modified:** `pyproject.toml`
- **Verification:** `ruff check migrations/` exits 0
- **Committed in:** `42384f6` (Task 1 commit)

**2. [Rule 3 - Blocking] A migration's own explanatory comment false-positived Task 2's literal verify grep**
- **Found during:** Task 2
- **Issue:** `0004_meta_ingestion_runs.py`'s comment explaining the deferred FK read "NOT a ForeignKey yet: meta.schema_versions does not exist..." — a single physical line containing both the literal substrings `ForeignKey` and `schema_versions`, which is exactly what the task's own `<verify>` command (`! grep -q "ForeignKey.*schema_versions"`) is designed to reject when it appears in a real `sa.ForeignKey(...)` call. The comment was prose, not code, but the check is a text grep, not an AST check.
- **Fix:** Reworded the comment to describe the same fact ("the referent for this column does not exist until a later phase's migration") without putting the word `ForeignKey` and the substring `schema_versions` on the same line.
- **Files modified:** `migrations/versions/0004_meta_ingestion_runs.py`
- **Verification:** `grep -n "ForeignKey.*schema_versions" migrations/versions/0004_meta_ingestion_runs.py` now returns no match; the column definition itself was never affected (it never had a `ForeignKey` call)
- **Committed in:** `112280f` (Task 2 commit)

**3. [Rule 1 - Bug] `alembic upgrade head` failed against a genuinely empty database**
- **Found during:** Task 3 (first real run of `tests/integration` against Docker)
- **Issue:** `version_table_schema="meta"` makes Alembic try to `CREATE TABLE meta.alembic_version` before running any revision — including 0001, the one revision that creates schema `meta`. Against a brand-new database this raised `psycopg.errors.InvalidSchemaName`.
- **Fix:** `env.py` now runs `CREATE SCHEMA IF NOT EXISTS meta` (committed) immediately after the wrong-database guard passes and before `context.configure()`.
- **Files modified:** `migrations/env.py`
- **Verification:** All 5 `tests/integration/test_migrations.py` tests pass against real testcontainers PostgreSQL 18; `make test-integration` exits 0
- **Committed in:** `4d0d138` (Task 3 commit)

**4. [Rule 1 - Bug] `postgres_dsn` fixture's default database name tripped the wrong-database guard**
- **Found during:** Task 3
- **Issue:** `PostgresContainer(...)` without an explicit `dbname` defaults to `test`; `migrations/env.py`'s guard correctly refused to migrate a database named `test` when it expects `analytics`.
- **Fix:** Passed `dbname="analytics"` explicitly, matching `helm/values/*/cnpg-analytics.yaml`.
- **Files modified:** `tests/integration/conftest.py`
- **Verification:** Same test run as deviation 3 — all 5 tests pass
- **Committed in:** `4d0d138` (Task 3 commit)

**5. [Rule 1 - Bug] `testcontainers.postgres`/`testcontainers.minio` are deprecated in the pinned version**
- **Found during:** Task 3
- **Issue:** Importing `PostgresContainer`/`MinioContainer` from `testcontainers.postgres`/`testcontainers.minio` (as the research doc's code example shows) emits a `DeprecationWarning` at import time under testcontainers 4.15.0.
- **Fix:** Switched to `testcontainers.community.postgres`/`testcontainers.community.minio` — confirmed identical constructor signatures via `inspect.signature` before switching; no behavior change.
- **Files modified:** `tests/integration/conftest.py`
- **Verification:** No `DeprecationWarning` on import; all 5 tests pass
- **Committed in:** `4d0d138` (Task 3 commit)

**6. [Rule 1 - Bug] `_require_docker`'s 10-second `docker info` timeout was too short in this environment**
- **Found during:** Task 3
- **Issue:** First test run failed every test at fixture setup with `subprocess.TimeoutExpired` on `docker info`, even though Docker was reachable — measured directly, `docker info` alone takes ~10 seconds in this WSL2/Docker Desktop-backed environment, right at the timeout boundary.
- **Fix:** Raised the timeout to 30 seconds, documented with the measurement in a comment.
- **Files modified:** `tests/integration/conftest.py`
- **Verification:** `_require_docker` no longer times out; all 5 tests pass
- **Committed in:** `4d0d138` (Task 3 commit)

---

**Total deviations:** 6 auto-fixed (2 blocking/lint-gate, 4 bugs found via real execution against Docker)
**Impact on plan:** All six were necessary for the plan's own stated correctness (a working `alembic upgrade head`, a working test suite, a green `make check`). None widened scope beyond what Task 1–3's `<action>` text already specified; deviations 3–6 were only discoverable by actually running the migrations against a real database, which this plan's own verification step requires.

## Issues Encountered

- The background `tests/policy -q -m "not manifests"` run (105 tests) took roughly 9 minutes, dominated by `tests/policy/test_doctor_fails_closed.py`'s ~8 separate `make doctor` invocations, each paying `scripts/doctor.sh`'s two `docker info` calls (~10s each, measured) — compounded by CPU contention with the sibling parallel executor (plan 03-03) building in a separate worktree on the same host. This is pre-existing Phase 1/2 test cost, not something this plan's changes affected; confirmed by re-running the specific deviation regression checks and `make test-integration` afterward, both fast (11–19s).
- No other issues. Every task's automated `<verify>` block, the plan-level `## Verification` section, and every `must_haves` truth/artifact/key_link/prohibition were checked explicitly and passed.

## User Setup Required

None — no external service configuration required. `etl_app` on the live `analytics-db` CNPG cluster still has no password and no grants (03-RESEARCH.md Pitfall 2); this plan deliberately never touches that cluster, proving everything against throwaway testcontainers PostgreSQL/MinIO instead, exactly as ROADMAP Phase 3 success criterion 2 requires.

## Next Phase Readiness

- `tests/integration/conftest.py`'s `postgres_dsn`, `run_migrations`, `migrated_dsn`, `minio_config` and `s3_client` fixtures are ready for reuse by 03-04 through 03-08's own `tests/integration/` modules without any further scaffolding.
- The complete vertical-slice `meta`/`normalized` schema exists and is proven idempotent — any later plan's `MetadataRepository`/`ConfigRegistry` implementation (03-04/03-05+) can now be written and tested against a real, migrated schema shape rather than a guess.
- `meta.ingestion_runs.schema_version_id`'s deferred-FK shape is the template for whichever later phase creates `meta.schema_versions` and adds the constraint via `op.create_foreign_key`.
- No blockers. `make check`'s full chain (lint, format, typecheck, imports, policy, test, fixtures-verify) and `make test-integration` both pass with this plan's commits present.

## Self-Check: PASSED

- FOUND: migrations/alembic.ini
- FOUND: migrations/env.py
- FOUND: migrations/script.py.mako
- FOUND: migrations/versions/0001_meta_datasets_config_versions.py
- FOUND: migrations/versions/0002_meta_files.py
- FOUND: migrations/versions/0003_meta_batches_batch_files.py
- FOUND: migrations/versions/0004_meta_ingestion_runs.py
- FOUND: migrations/versions/0005_normalized_customers.py
- FOUND: tests/integration/__init__.py
- FOUND: tests/integration/conftest.py
- FOUND: tests/integration/test_migrations.py
- FOUND commit: 42384f6 (Task 1)
- FOUND commit: 112280f (Task 2)
- FOUND commit: 4d0d138 (Task 3)

---
*Phase: 03-dataplat-core-library-metadata-control-plane*
*Completed: 2026-08-13*
