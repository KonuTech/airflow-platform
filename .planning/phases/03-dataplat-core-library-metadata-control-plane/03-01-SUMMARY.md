---
phase: 03-dataplat-core-library-metadata-control-plane
plan: 01
subsystem: dataplat-core
tags: [psycopg, psycopg-pool, dataclasses, exceptions, uv-workspace, value-objects]

# Dependency graph
requires:
  - phase: 01-repository-toolchain-ci-skeleton
    provides: "the packages/dataplat uv workspace member skeleton (__init__.py, version.py, pyproject.toml with only PyYAML), the import-linter contract forbidding dataplat -> csv_processor, and the ruff/mypy-strict gate this plan's code must pass cleanly"
provides:
  - "dataplat's first real runtime dependencies: psycopg[binary,pool], boto3, pydantic, structlog, click"
  - "dataplat.errors — the QUAL-03 exception hierarchy (DataPlatformError + 3 D-06 leaf subclasses), each carrying context: dict"
  - "dataplat.models.identity — DatasetRef, FileIdentity, BatchIdentity, RunContext"
  - "dataplat.models.record — RecordChunk (with .replace()), RejectedRecord, StageResult"
  - "dataplat.models.report — ValidationResult (D-05 minimal shape)"
  - "dataplat.storage.db.create_pool() — the one psycopg_pool.ConnectionPool factory for the whole runtime path"
affects: [03-02, 03-03, 03-04, 03-05, 03-06, 03-07, 03-08]

# Tech tracking
tech-stack:
  added:
    - "psycopg[binary,pool] 3.3.4 (+ psycopg-pool 3.3.1) — used by dataplat.storage.db"
    - "boto3 1.43.68 — dependency wired, not yet imported by any file (lands in a later plan's objectstore.py)"
    - "pydantic 2.13.4 (+ pydantic-core, annotated-types, typing-inspection) — dependency wired, not yet imported"
    - "structlog 26.1.0 — dependency wired, not yet imported"
    - "click 8.4 — dependency wired, not yet imported"
  patterns:
    - "Frozen, slotted dataclasses with Google-style Attributes: docstrings for every value object (tools/corpus/manifest.py's convention, extended into dataplat)"
    - "Row-level problems become RejectedRecord data inside StageResult.rejected, never an exception (QUAL-03 errors-as-values)"
    - "One connection-pool factory (create_pool) for the whole dataplat runtime path — no other module constructs psycopg_pool.ConnectionPool directly"
    - "DataPlatformError.context: dict[str, object] threaded through every subclass via the inherited constructor, never overridden per-subclass"

key-files:
  created:
    - packages/dataplat/src/dataplat/errors.py
    - packages/dataplat/src/dataplat/models/__init__.py
    - packages/dataplat/src/dataplat/models/identity.py
    - packages/dataplat/src/dataplat/models/record.py
    - packages/dataplat/src/dataplat/models/report.py
    - packages/dataplat/src/dataplat/storage/__init__.py
    - packages/dataplat/src/dataplat/storage/db.py
  modified:
    - packages/dataplat/pyproject.toml
    - uv.lock

key-decisions:
  - "Cast RecordChunk.replace()'s **object kwargs to dict[str, Any] only at the dataclasses.replace() call site, to satisfy mypy's special-cased checking of that call without weakening the public, object-typed method signature"
  - "Kept the psycopg.OperationalError handler in create_pool() exactly as instructed, despite empirically proving (against psycopg_pool 3.3.1) that open=False skips all conninfo validation, so the handler is currently unreachable for a malformed DSN — documented as defensive/forward-compatible rather than removed"
  - "Did not run requirements.mark-complete for QUAL-03: three plans in this phase (03-01, 03-06, 03-07) each declare QUAL-03 in their own frontmatter, and this plan's own text states it delivers only the exception-hierarchy third; the tool flips Pending->Complete unconditionally with no cross-plan awareness, so calling it now would falsely mark a three-plan requirement done after the first plan"
  - "No persisted pytest test files added beyond the plan's declared files_modified — the plan's own Verification section states construction-time behaviour is proven by the inline shell checks, and real pytest coverage of these types arrives with 03-06 (pipeline stage exercising StageResult/RejectedRecord) and 03-07 (cli.py exercising the exception hierarchy end-to-end)"

patterns-established:
  - "Frozen, slotted dataclasses (@dataclass(frozen=True, slots=True)) with Google-style Attributes: docstrings for every immutable value object"
  - "Errors-as-values for row-level problems (RejectedRecord/StageResult); the DataPlatformError hierarchy is reserved for run-fatal conditions only"
  - "A single factory function per external resource type (create_pool for psycopg_pool.ConnectionPool) rather than ad hoc construction at each call site"

requirements-completed: [QUAL-03]

# Metrics
duration: 32min
completed: 2026-08-13
---

# Phase 3 Plan 1: dataplat Foundation — Exceptions, Value Objects, Connection Pool Summary

**QUAL-03's exception hierarchy (DataPlatformError + 3 leaf subclasses), the frozen RecordChunk/RejectedRecord/StageResult/identity value objects, and the one psycopg_pool.ConnectionPool factory every later Phase 3 plan imports.**

## Performance

- **Duration:** 32 min
- **Started:** 2026-08-12T23:48:04Z
- **Completed:** 2026-08-13T00:19:43Z
- **Tasks:** 3
- **Files modified:** 9 (7 created, 2 modified)

## Accomplishments

- `packages/dataplat/pyproject.toml` carries dataplat's first real runtime dependencies (`psycopg[binary,pool]`, `boto3`, `pydantic`, `structlog`, `click`); `uv.lock` regenerated and committed, `uv lock --check` green
- `dataplat.errors` exports exactly `DataPlatformError`, `ConfigurationError`, `StorageError`, `SecretResolutionError` — no other branch exists yet (D-06), each carrying a `context: dict[str, object]`
- `dataplat.models.identity`/`record`/`report` export the seven value types ARCHITECTURE.md Q4.3 and 03-RESEARCH.md Pattern 3 specify: frozen where the design calls for it, `StageResult` mutable to match ARCHITECTURE.md's own declaration
- `dataplat.storage.db.create_pool()` — the only place a `psycopg_pool.ConnectionPool` is constructed in the runtime path, unopened by default, sized 1/2, wrapping construction failures as `StorageError` without ever leaking DSN credentials
- Full `make check` (lint, format, mypy strict, import-linter, policy tests, unit+regression tests with coverage, corpus verify) passes clean with all three new modules in the tree

## Task Commits

Each task was committed atomically:

1. **Task 1: Wire dataplat's real runtime dependencies, then the exception hierarchy** - `327562d` (feat)
2. **Task 2: Frozen value objects — identity, record chunk, rejected record, stage result, validation result** - `10aa622` (feat)
3. **Task 3: The one psycopg connection-pool factory** - `3e1fd75` (feat)

_No TDD tasks in this plan — all three are `type="auto"` with inline shell verification, per the plan's own text._

## Files Created/Modified

- `packages/dataplat/pyproject.toml` - adds `psycopg[binary,pool]>=3.3.4,<4`, `boto3>=1.43.68,<2`, `pydantic>=2.13,<3`, `structlog>=26,<27`, `click>=8.4,<9` to `[project.dependencies]`
- `uv.lock` - regenerated; 6 net-new packages resolved (annotated-types, psycopg-pool, pydantic, pydantic-core, structlog, typing-inspection); boto3/click/psycopg were already present via the root `cluster`/`dev` groups
- `packages/dataplat/src/dataplat/errors.py` - `DataPlatformError` base + `ConfigurationError`/`StorageError`/`SecretResolutionError`, each documenting which later condition it is for and which branches are deliberately absent
- `packages/dataplat/src/dataplat/models/__init__.py` - empty package marker, module docstring only
- `packages/dataplat/src/dataplat/models/identity.py` - `DatasetRef`, `FileIdentity`, `BatchIdentity`, `RunContext` (frozen, slotted)
- `packages/dataplat/src/dataplat/models/record.py` - `RecordChunk` (frozen, `.replace()`), `RejectedRecord` (frozen), `StageResult` (mutable builder)
- `packages/dataplat/src/dataplat/models/report.py` - `ValidationResult` (frozen, the D-05 minimal shape)
- `packages/dataplat/src/dataplat/storage/__init__.py` - empty package marker, module docstring only
- `packages/dataplat/src/dataplat/storage/db.py` - `create_pool(dsn, *, min_size=1, max_size=2)`

## Decisions Made

- **`RecordChunk.replace()` internal cast (Task 2).** mypy's special-cased type-checking of `dataclasses.replace(obj, **kwargs)` unifies each keyword argument against its field's declared type, and rejects an `object`-typed `**kwargs` outright — even though every field type is itself a valid `object`. Fixed by casting only at the `dataclasses.replace()` call site (`cast("dict[str, Any]", changes)`), keeping the public method signature exactly as the plan specifies (`**changes: object`). Verified empirically against a minimal reproduction before touching the real file.
- **`create_pool()`'s exception handler, kept as specified despite an empirical finding (Task 3).** Downloaded and read `psycopg_pool` 3.3.1's actual source and confirmed by direct construction that `ConnectionPool(dsn, open=False)` performs **no conninfo validation at all** — even a syntactically invalid DSN string constructs without error, because `open=False` skips the code path that would parse it. This means the `except psycopg.OperationalError` handler the plan specifies is not reachable today for a malformed DSN specifically. Implemented it anyway, exactly as instructed: it is forward-compatible (protects against a future psycopg_pool version that validates eagerly), costs nothing, and none of the task's acceptance criteria or automated verify command exercise that branch either way. Documented the finding in the module's construction-failure docstring and the task commit message so a future reader does not assume the branch is currently exercised.
- **Did not mark QUAL-03 complete in `REQUIREMENTS.md` (see Requirements Tracking below).**
- **No new pytest test files.** The plan's own `## Verification` section says explicitly: *"No test yet exercises `create_pool()` against a live database... this plan proves only construction-time behaviour"* — via the inline `<verify><automated>` shell commands, which all three tasks specify and this execution ran and confirmed passing. `files_modified` in the plan's frontmatter lists exactly the 9 files touched here, none of them tests. Persisted regression coverage for these types is explicit in later plans' own requirement mappings (03-06 exercises `StageResult`/`RejectedRecord` from a real pipeline stage; 03-07 exercises the exception hierarchy from `cli.py`'s catch-once handler).

## Requirements Tracking

This plan's frontmatter declares `requirements: [QUAL-03]`, and the SUMMARY frontmatter above copies that mechanically per convention. **`REQUIREMENTS.md`'s traceability table was deliberately NOT flipped to `Complete` for QUAL-03.** Three plans in this phase each declare `QUAL-03` in their own frontmatter — `03-01` (this plan, the exception hierarchy), `03-06` (errors-as-values proven by a real pipeline stage), and `03-07` (the cli.py catch-once handler + `error_type`/`error_message`/`error_detail` write to `meta.ingestion_runs`). The plan's own `## Success criteria` section states this explicitly: *"QUAL-03's exception-hierarchy half is complete; the errors-as-values half is proven by plan 03-06."* Inspecting the `requirements.mark-complete` SDK verb's implementation confirmed it does a blind, unconditional `Pending -> Complete` regex replace with no cross-plan awareness — calling it now would create a false completion record for a requirement that is only one-third delivered. Leave `QUAL-03` `Pending` until 03-07 (the last of the three contributing plans) completes it.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] mypy rejects `dataclasses.replace(self, **changes)` when `**changes` is typed `object`**
- **Found during:** Task 2 (`RecordChunk.replace()`)
- **Issue:** `uv run --frozen mypy packages/dataplat/src` failed with two `arg-type` errors: mypy's built-in special-casing for `dataclasses.replace()` calls checks each substituted keyword against the dataclass's declared field type, and `object` does not unify with `tuple[tuple[str, ...], ...]` or `int` even though both are valid `object`s.
- **Fix:** Cast the unpacked kwargs to `dict[str, Any]` only inside the call: `dataclasses.replace(self, **cast("dict[str, Any]", changes))`. Verified the fix against a standalone minimal reproduction first (matching field/kwarg shape) before applying it to `record.py`, confirming both that `object` genuinely fails and that the narrow cast genuinely resolves it while an `Any`-typed public signature (an alternative fix) was not necessary.
- **Files modified:** `packages/dataplat/src/dataplat/models/record.py`
- **Verification:** `uv run --frozen mypy packages/dataplat/src` exits 0; `uv run --frozen ruff check packages/dataplat/src/dataplat/models` exits 0 (ruff's `TC006` additionally required the cast's type expression to be quoted, applied)
- **Committed in:** `10aa622` (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking/wrong-types)
**Impact on plan:** Necessary for the plan's own "mypy exits 0" acceptance criterion; the public API (`RecordChunk.replace(self, **changes: object) -> RecordChunk`) is unchanged from what the plan specifies. No scope creep.

## Issues Encountered

- **`psycopg_pool.ConnectionPool`'s `open=False` behavior differs from the plan's stated rationale.** The plan's action text for Task 3 says a malformed DSN is "the realistic failure at construction time, since `open=False` defers the actual TCP/auth attempt" — implying conninfo *parsing* still happens, just not the network attempt. Downloading and reading `psycopg_pool` 3.3.1's actual source (`ConnectionPool.__init__`) and testing empirically showed `open=False` skips validation entirely; `_check_size()` only validates `min_size`/`max_size` are sane integers. This did not require a code change (the task's acceptance criteria and automated verify command do not exercise the exception path), but is recorded here and in the module docstring/commit message so a later reader — or the plan-checker for a future phase — does not assume the `except psycopg.OperationalError` branch is currently under test.
- No other issues. All three tasks' automated verify blocks, plus the plan-level `## Verification` section and the full `make check` gate, passed on the first attempt after the one mypy fix above.

## User Setup Required

None - no external service configuration required. This plan touches no live database, object store, or cluster (by design — see the plan's own Verification section).

## Next Phase Readiness

- Wave 2 plans `03-02` (metadata schema/migrations) and `03-03` (secrets/logging) both declare `depends_on: ["03-01"]` and can now `from dataplat.errors import ...`, `from dataplat.models.record import ...`, `from dataplat.models.identity import ...`, and `from dataplat.storage.db import create_pool` without any further scaffolding — verified directly as part of this plan's own execution.
- Wave 3 plans `03-04` and `03-05` transitively depend on this plan through `03-02`/`03-03`.
- `QUAL-03` stays `Pending` in `REQUIREMENTS.md` until `03-07` lands (see Requirements Tracking above) — this is expected, not a blocker.
- No blockers. `make check` is green on the merge-base commit plus this plan's three commits.

## Self-Check: PASSED

- FOUND: packages/dataplat/pyproject.toml
- FOUND: uv.lock
- FOUND: packages/dataplat/src/dataplat/errors.py
- FOUND: packages/dataplat/src/dataplat/models/__init__.py
- FOUND: packages/dataplat/src/dataplat/models/identity.py
- FOUND: packages/dataplat/src/dataplat/models/record.py
- FOUND: packages/dataplat/src/dataplat/models/report.py
- FOUND: packages/dataplat/src/dataplat/storage/__init__.py
- FOUND: packages/dataplat/src/dataplat/storage/db.py
- FOUND commit: 327562d (Task 1)
- FOUND commit: 10aa622 (Task 2)
- FOUND commit: 3e1fd75 (Task 3)

---
*Phase: 03-dataplat-core-library-metadata-control-plane*
*Completed: 2026-08-13*
