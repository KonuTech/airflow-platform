---
phase: 03-dataplat-core-library-metadata-control-plane
plan: 05
subsystem: database
tags: [boto3, minio, s3, psycopg, postgresql, protocol, typing, testcontainers]

# Dependency graph
requires:
  - phase: 03-01
    provides: dataplat.storage.db.create_pool() — the one psycopg connection-pool factory
  - phase: 03-02
    provides: the meta schema Alembic migrations (datasets, files, batches, batch_files, ingestion_runs)
  - phase: 03-03
    provides: dataplat.errors (StorageError) and dataplat.secrets.resolver.resolve_secret()
provides:
  - "dataplat.storage.objectstore: ObjectStore Protocol + S3ObjectStore + open_text_stream(), the corrected adapter-free StreamingBody-to-text-stream bridge"
  - "dataplat.metadata.repository: MetadataRepository Protocol, the typed CRUD surface for the five meta.* slice tables"
  - "dataplat.metadata.postgres: PostgresMetadataRepository, the psycopg-backed implementation"
  - "tests/integration/test_objectstore.py and test_metadata_repository.py, proving both against real testcontainers MinIO/Postgres, including the resolve_secret()-to-create_pool() wiring"
affects: [03-06, 03-07, 03-08, phase-04-vertical-slice]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Protocol + concrete-implementation pair where the implementation explicitly subclasses the Protocol, so mypy's abstract-instantiation check mechanically proves full method coverage at typecheck time (no separate runtime isinstance test needed)"
    - "boto3 StreamingBody wrapped directly via io.BufferedReader/io.TextIOWrapper — no custom io.RawIOBase adapter, since botocore >= 1.43.68 implements readable()/readinto() natively"
    - "Allow-listed dynamic SQL SET-clause construction: caller-supplied **fields keys checked against a frozenset module constant before being woven into the query's column-name shape; values always cross via %s placeholders"

key-files:
  created:
    - packages/dataplat/src/dataplat/storage/objectstore.py
    - packages/dataplat/src/dataplat/metadata/__init__.py
    - packages/dataplat/src/dataplat/metadata/repository.py
    - packages/dataplat/src/dataplat/metadata/postgres.py
    - tests/integration/test_objectstore.py
    - tests/integration/test_metadata_repository.py
  modified: []

key-decisions:
  - "S3ObjectStore and PostgresMetadataRepository explicitly subclass their Protocols (ObjectStore, MetadataRepository) rather than relying purely on structural typing, so mypy enforces complete method coverage as an abstract-instantiation error if anything is missing"
  - "update_ingestion_run_status validates **fields against a fixed _INGESTION_RUN_UPDATABLE_FIELDS allow-list (module constant) before building its SET clause; values are still always parameterized"
  - "Rewrote ObjectStore's explanatory docstrings to avoid the literal substrings 'RawIOBase' and '_raw_stream' — the plan's own <action> asked for a docstring naming them explicitly, but the plan's <verify> greps to confirm neither string appears in the file; resolved in favor of the automated check while preserving the same explanation in different words"

requirements-completed: [META-01, SEC-15]

# Metrics
duration: 29min
completed: 2026-08-13
---

# Phase 3 Plan 5: ObjectStore & MetadataRepository Summary

**StreamingBody-to-text-stream bridge (no custom adapter) plus a psycopg-backed typed CRUD layer over the five `meta.*` slice tables, both proven against real testcontainers MinIO/Postgres — including the `resolve_secret()`-to-`create_pool()` wiring for SEC-15.**

## Performance

- **Duration:** ~29 min
- **Started:** 2026-08-13T04:34:39Z (worktree base established)
- **Completed:** 2026-08-13T05:03:24Z
- **Tasks:** 3/3 completed
- **Files modified:** 6 (all newly created; 805 lines total)

## Accomplishments

- `dataplat.storage.objectstore.open_text_stream()` wraps a real boto3 `GetObject` response body directly in `io.BufferedReader`/`io.TextIOWrapper` — no custom `io.RawIOBase`-style adapter class anywhere, closing out 03-RESEARCH.md finding 1 as committed, tested code
- `S3ObjectStore` implements `ObjectStore` against a real boto3 S3 client (sigv4, path-style addressing), translating `botocore.exceptions.ClientError` into `dataplat.errors.StorageError` so the raw boto3 exception type never escapes
- `MetadataRepository` (Protocol) + `PostgresMetadataRepository` (implementation) give typed CRUD over `meta.datasets`/`files`/`batches`/`batch_files`/`ingestion_runs` — every SQL statement is a single parameterized query, no string interpolation of values anywhere
- A full `dataset -> file -> batch -> batch_files -> ingestion_run` chain is created, linked and read back through typed code alone against a real migrated PostgreSQL, proving META-01's schema is genuinely usable, not just DDL-valid
- `test_resolved_env_secret_yields_a_live_metadata_connection` proves ROADMAP Phase 3 success criterion 4 / SEC-15 mechanically: a connection pool built from `resolve_secret("env://...")`'s output (never a literal DSN) executes a real `MetadataRepository` query against the migrated database

## Task Commits

Each task was committed atomically:

1. **Task 1: ObjectStore — the corrected StreamingBody bridge, proven against real MinIO** - `858b105` (feat)
2. **Task 2: MetadataRepository — the typed CRUD surface for the five slice tables** - `134d342` (feat)
3. **Task 3: Prove the round trip against real Postgres, and prove the resolve_secret()-to-create_pool() wiring** - `386b859` (test)

**Plan metadata:** (this commit, immediately following)

## Files Created/Modified

- `packages/dataplat/src/dataplat/storage/objectstore.py` (135 lines) - `ObjectStore` Protocol, `open_text_stream()`, `S3ObjectStore`
- `packages/dataplat/src/dataplat/metadata/__init__.py` (9 lines) - package marker, re-exports nothing (matches `secrets/__init__.py` convention)
- `packages/dataplat/src/dataplat/metadata/repository.py` (170 lines) - `MetadataRepository` Protocol, 7 methods, each documenting its `meta.*` table/column mapping
- `packages/dataplat/src/dataplat/metadata/postgres.py` (243 lines) - `PostgresMetadataRepository`, the psycopg-backed implementation, plus the `_INGESTION_RUN_UPDATABLE_FIELDS` allow-list
- `tests/integration/test_objectstore.py` (68 lines) - embedded-`\r\n` round-trip proof + missing-key `StorageError` proof, against real testcontainers MinIO
- `tests/integration/test_metadata_repository.py` (180 lines) - full slice round trip, unknown-field rejection, and the `resolve_secret()`-to-`create_pool()` wiring proof, against real testcontainers PostgreSQL

## Decisions Made

- Made `S3ObjectStore(ObjectStore)` and `PostgresMetadataRepository(MetadataRepository)` explicitly subclass their Protocols. This turns "does the implementation cover every Protocol method" into a mechanically-enforced mypy check (`error: Cannot instantiate abstract class ... with abstract attribute ...` if anything were missing) rather than a manual review — verified by deliberately writing an incomplete subclass in scratch and confirming mypy rejects it before settling on this shape. This satisfies Task 2's acceptance criterion ("structural check: isinstance-style duck-typing test, or explicit method presence assertions") through the `mypy packages/dataplat/src` gate that was already part of the task's own `<verify>` command, rather than adding a separate runtime test.
- `update_ingestion_run_status` builds its `SET` clause via plain string concatenation of allow-listed column names (never an f-string), with a `# noqa: S608` and an explanatory comment — ruff's bandit-derived SQL-injection heuristic flags any dynamically-assembled query text regardless of mechanism, but the column names are checked against `_INGESTION_RUN_UPDATABLE_FIELDS` first and every value still crosses via `%s` placeholders (T-03-11's mitigation, matching the plan's explicit instruction).
- `find_file_by_content_hash` uses `LIMIT 1` even though the plan's round-trip test only ever inserts one matching row — the underlying index (`ix_files_dataset_content_sha256`) is not unique, so a future caller could legitimately have more than one file share a content hash within a dataset (re-upload to a different path).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Reworded ObjectStore's docstrings to satisfy a contradiction between the plan's own `<action>` and `<verify>` steps**
- **Found during:** Task 1
- **Issue:** Task 1's `<action>` explicitly instructs the module docstring to "state explicitly that no custom `io.RawIOBase` subclass is used or needed... and that reaching into `response_body._raw_stream` is forbidden" — but Task 1's `<verify>` runs `! grep -rn "RawIOBase\|_raw_stream" .../objectstore.py`, which requires those exact substrings to appear *nowhere* in the file. Following the action's literal wording guaranteed the verify command would fail.
- **Fix:** Kept the same explanation (no custom raw-binary-I/O adapter subclass exists; do not reach into the response body's private internal-stream attribute) but rephrased both the module and function docstrings to avoid the literal tokens `RawIOBase` and `_raw_stream`, e.g. "a hand-written adapter subclassing Python's raw binary-I/O base class" and "reaching into `StreamingBody`'s private internal-stream attribute." The behavioral prohibition (no such adapter class, no such private-attribute access) is unchanged and independently true of the actual code.
- **Files modified:** `packages/dataplat/src/dataplat/storage/objectstore.py`
- **Verification:** `grep -rn "RawIOBase\|_raw_stream" packages/dataplat/src/dataplat/storage/objectstore.py` exits 1 (no match); `uv run --frozen --group cluster pytest tests/integration/test_objectstore.py -q` still passes; `uv run --frozen mypy packages/dataplat/src` passes.
- **Committed in:** `858b105` (Task 1 commit)

**2. [Rule 3 - Blocking] Added targeted lint suppressions for lint rules that conflict with the plan-mandated interface shape**
- **Found during:** Task 2
- **Issue:** `create_file`/`create_ingestion_run`'s signatures are specified verbatim by the plan (7 and 8 keyword-only parameters respectively, matching `meta.files`/`meta.ingestion_runs`' column sets) and trip ruff's `PLR0913` (too-many-arguments, threshold 5) in both `repository.py` and `postgres.py`. Separately, `update_ingestion_run_status`'s allow-listed dynamic `SET` clause (explicitly requested by the plan's own action text) trips ruff's `S608` (possible SQL injection via string-built query) even though the query is built from a checked allow-list, not unchecked input.
- **Fix:** Added `# noqa: PLR0913` (with a one-line reason referencing the column set it mirrors) on the four affected method signatures, and `# noqa: S608` (with a multi-line rationale comment above it) on the one dynamically-assembled query line. No suppression is blanket — each is scoped to the exact line the rule fires on.
- **Files modified:** `packages/dataplat/src/dataplat/metadata/repository.py`, `packages/dataplat/src/dataplat/metadata/postgres.py`
- **Verification:** `uv run --frozen ruff check packages/dataplat/src/dataplat/metadata/` passes with zero errors; `uv run --frozen ruff check .` (full repo, via `make check`) also passes, confirming no unused-noqa (`RUF100`) fired.
- **Committed in:** `134d342` (Task 2 commit)

---

**Total deviations:** 2 auto-fixed (both Rule 3 — blocking issues in the plan's own verification commands / lint-vs-interface conflicts)
**Impact on plan:** Both fixes are cosmetic/mechanical (docstring wording, scoped lint suppressions) — no behavioral, architectural, or interface change from what the plan specified. No scope creep.

## Issues Encountered

- The interface snippet's `# type: ignore[arg-type]` on `io.BufferedReader(response_body)` was the wrong mypy error code against the installed stub set — mypy reported `unused-ignore` alongside the real `type-var` error (`Value of type variable "_BufferedReaderStreamT" of "BufferedReader" cannot be "object"`). Corrected to `# type: ignore[type-var]`; no behavioral change, verified by `uv run --frozen mypy packages/dataplat/src` passing clean.

## User Setup Required

None - no external service configuration required. Both tests run against ephemeral testcontainers (MinIO, PostgreSQL 18) that require only a local Docker daemon, already verified present in this environment.

## Next Phase Readiness

- `ObjectStore`/`S3ObjectStore` and `MetadataRepository`/`PostgresMetadataRepository` are ready for the CSV source (plan 03-06/03-07/03-08, per the wave plan) and the Phase 4 vertical-slice DAG to consume directly — no further scaffolding needed on either seam.
- `metadata/fake.py` was deliberately not built, per CONTEXT.md's Claude's-Discretion note and this plan's own prohibition list — revisit only if a later phase's unit-test speed genuinely needs an in-memory double with no testcontainers dependency.
- `S3ObjectStore`'s own credentials still arrive as caller-supplied strings, not routed through `resolve_secret()` — this is T-03-12's accepted disposition in this plan's threat model, not a gap; a future phase can wire it if S3 credentials need the same opaque-reference treatment SEC-15 gave the Postgres pool here.
- No blockers. `make check` (the offline gate) passes unaffected — both new test files live under `tests/integration/`, collected only by `make test-integration`, exactly as D-04/the plan's Verification section requires.

## Self-Check: PASSED

All 6 created files verified present on disk; all 3 task commits (`858b105`, `134d342`, `386b859`) verified present in git history.

---
*Phase: 03-dataplat-core-library-metadata-control-plane*
*Completed: 2026-08-13*
