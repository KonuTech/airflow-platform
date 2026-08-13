---
phase: 03-dataplat-core-library-metadata-control-plane
plan: 09
subsystem: testing
tags: [pytest, psycopg, postgres, test-isolation, integration-tests]

# Dependency graph
requires:
  - phase: 03-dataplat-core-library-metadata-control-plane
    provides: "PostgresMetadataRepository (03-05) and ConfigRegistry (03-04), both exercised against the same session-scoped migrated_dsn Postgres testcontainer"
provides:
  - "test_metadata_repository.py collision-free against any other tests/integration/ file sharing the session-scoped Postgres fixture, present or future"
  - "_insert_config_version() test helper that derives the next config_versions.version from existing rows instead of hardcoding version=1"
affects: []

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Test-only SQL helpers that insert into shared-fixture tables must derive natural-key-adjacent values (e.g. a per-parent sequence column) from existing rows rather than hardcoding them, so the helper stays correct under whole-directory pytest collection sharing one database."
    - "Synthetic dataset names used only to exercise FK chains in integration tests should carry a distinguishing suffix (e.g. `_slice_proof`) to guarantee non-collision with other files' fixture data under a shared session-scoped container."

key-files:
  created: []
  modified: [tests/integration/test_metadata_repository.py]

key-decisions:
  - "Renamed the round-trip test's dataset from the literal \"customers\" to \"customers_slice_proof\" per 03-UAT.md's suggested fix, rather than renaming test_config_registry.py's dataset — the UAT root-cause analysis identified test_metadata_repository.py as the file with the incorrect (hardcoded version=1) assumption, so the fix stays confined to it."
  - "_insert_config_version() derives the next version via a SELECT COALESCE(MAX(version), 0) + 1 subquery scoped to the same dataset_id, inside the single INSERT statement (one round-trip, named psycopg params to avoid duplicating the dataset_id positionally) — closing the class of bug (any two files creating config_versions rows for the same dataset_id) rather than just this one collision."

requirements-completed: [META-01, SCHEMA-07]

# Metrics
duration: 3min
completed: 2026-08-13
---

# Phase 03 Plan 09: Close UAT test-isolation collision Summary

**Fixed `tests/integration/test_metadata_repository.py`'s `UniqueViolation` on `uq_config_versions_dataset_version` by deriving `_insert_config_version()`'s version number from existing rows and renaming the round-trip test's dataset to avoid colliding with `test_config_registry.py`'s "customers" fixture data under the shared session-scoped Postgres container**

## Performance

- **Duration:** 3 min
- **Started:** 2026-08-13T10:07:52Z
- **Completed:** 2026-08-13T10:10:23Z
- **Tasks:** 1
- **Files modified:** 1

## Accomplishments
- `tests/integration/test_config_registry.py` and `tests/integration/test_metadata_repository.py` now pass together in pytest's default alphabetical collection order (the exact order `make test-integration` and CI's `integration` job use), with zero `UniqueViolation` on `uq_config_versions_dataset_version`.
- `test_metadata_repository.py::test_full_slice_round_trip` still proves the complete dataset -> file -> batch -> batch_files link -> config_version -> ingestion_run -> status-update FK chain end to end; only its dataset identity and the helper's version-derivation logic changed.
- `_insert_config_version()` is now correct regardless of whether another test file has already inserted a `config_versions` row for the same `dataset_id` under the shared session-scoped fixture — a structural fix, not a one-off name change.
- Whole-directory `pytest tests/integration/` (14 tests, one session) passes cleanly.

## Task Commits

Each task was committed atomically:

1. **Task 1: Make test_metadata_repository.py collision-free under whole-directory pytest runs** - `1063a04` (test)

**Plan metadata:** committed alongside this SUMMARY.md (docs: complete plan)

## Files Created/Modified
- `tests/integration/test_metadata_repository.py` - Renamed `test_full_slice_round_trip`'s dataset from `"customers"` to `"customers_slice_proof"`; `_insert_config_version()` now derives the next `version` via `COALESCE(MAX(version), 0) + 1` scoped to `dataset_id` instead of hardcoding `1`.

## Decisions Made
- Confined the fix entirely to `test_metadata_repository.py`, per 03-UAT.md's root-cause analysis — `get_or_create_dataset()` was already correctly idempotent (`ON CONFLICT DO UPDATE`) and `test_config_registry.py` was not itself buggy; it was simply first alphabetically and thus first to claim `(dataset_id=1, version=1)`.
- Left the S3 `object_uri`, CSV `filename`, `batch_key`, and `idempotency_key` strings that reference "customers" unchanged — they are free-form batch/file identity, not dataset identity, and 03-UAT.md's fix targets only the dataset name and the version-derivation logic, per the plan's "minimal, targeted diff" instruction.
- Used named psycopg params (`%(dataset_id)s` etc.) instead of positional `%s` for the modified INSERT, since `dataset_id` now appears twice in the same statement (once for the INSERT value, once inside the `MAX(version)` subquery's `WHERE` clause) and psycopg's positional-tuple binding cannot express a repeated parameter without duplicating it in the tuple.

## Deviations from Plan

None - plan executed exactly as written. The one addition beyond the plan's literal action text was using named (dict) params instead of a positional tuple in the modified `INSERT`, which the plan itself anticipated as an acceptable implementation choice ("a SELECT subquery or a CTE ... either is acceptable, keep it a single round-trip").

## Issues Encountered
- Verification commands must be run from inside the worktree (`/home/konutec/projects/airflow-platform/.claude/worktrees/agent-a9b968b1c32bfd3c8`) with `uv run --group cluster pytest ...` — the `cluster` dependency group (not the default `dev` group) is what supplies `testcontainers`, matching the Makefile's `test-integration` target (`$(RUN_CLUSTER) pytest tests/integration -q`). An initial run against the wrong working directory (the main repo checkout) exercised stale, pre-edit code and reproduced the original failure; re-running with the correct worktree cwd and dependency group produced the expected all-green result.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- `make test-integration` and CI's `integration` job now run `tests/integration/` cleanly end to end (14/14 passing) with no cross-file test-isolation collisions.
- This was the sole open gap from 03-UAT.md; phase 03's UAT-reported issues are now fully closed.

---
*Phase: 03-dataplat-core-library-metadata-control-plane*
*Completed: 2026-08-13*

## Self-Check: PASSED

- FOUND: tests/integration/test_metadata_repository.py
- FOUND: .planning/phases/03-dataplat-core-library-metadata-control-plane/03-09-SUMMARY.md
- FOUND: 1063a04 (task commit)
- FOUND: 6ab67b9 (summary commit)
