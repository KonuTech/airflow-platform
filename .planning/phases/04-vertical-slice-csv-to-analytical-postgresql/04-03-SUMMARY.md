---
phase: 04-vertical-slice-csv-to-analytical-postgresql
plan: 03
subsystem: database
tags: [pydantic, discovery, idempotency, s3, postgres, metadata]

# Dependency graph
requires:
  - phase: 03-core-library-metadata-control-plane
    provides: MetadataRepository/ObjectStore Protocols, DatasetConfig, PipelineContext conventions
provides:
  - "AssignmentDocument/FileAssignment/BatchAssignment (dataplat.models.assignment) -- the frozen manifest crossing the Airflow-DAG-to-pod boundary"
  - "Receipt (dataplat.models.receipt) -- the <=4KB XCom-budget outcome document"
  - "SourceConfig.duplicate_policy and DatasetConfig.batching (BatchingConfig.max_units_per_run), required config fields"
  - "dataplat.discovery.discover_files() and DiscoveredUnit -- the complete list/hash/dedup-check/freeze/cap mechanism"
  - "MetadataRepository.get_or_create_ingestion_run and create_file(duplicate_of_file_id=...) -- implemented ahead of 04-01's own execution (see Deviations)"
  - "ObjectStore.list_objects/put_object and ObjectSummary -- implemented ahead of 04-01's own execution (see Deviations)"
affects: [04-04, 04-05, 04-06, 04-07, 04-08, 04-09]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Idempotent upsert via INSERT ... ON CONFLICT ... DO UPDATE ... RETURNING for metadata rows discovery re-runs over (create_file, get_or_create_ingestion_run) -- mirrors get_or_create_dataset's established no-op-update idiom"
    - "Raw-bytes content hashing via io.TextIOWrapper.buffer.read(n) rather than decode-then-re-encode -- avoids any encoding-roundtrip assumption when computing content_sha256"
    - "Self-duplicate correction: find_file_by_content_hash cannot distinguish a genuine cross-object_uri duplicate from a same-object_uri rediscovery; discover_files compares create_file's returned file_id against the pre-check's existing_file_id to tell them apart"

key-files:
  created:
    - packages/dataplat/src/dataplat/models/assignment.py
    - packages/dataplat/src/dataplat/models/receipt.py
    - packages/dataplat/src/dataplat/discovery.py
    - tests/unit/test_assignment_document.py
    - tests/unit/test_batching_config.py
    - tests/unit/test_discovery.py
  modified:
    - packages/dataplat/src/dataplat/config/model.py
    - configs/datasets/customers.yaml
    - packages/dataplat/src/dataplat/metadata/repository.py
    - packages/dataplat/src/dataplat/metadata/postgres.py
    - packages/dataplat/src/dataplat/storage/objectstore.py
    - tests/integration/test_metadata_repository.py
    - tests/integration/test_objectstore.py
    - tests/unit/test_config_hashing.py

key-decisions:
  - "Implemented 04-01-PLAN.md's get_or_create_ingestion_run/create_file(duplicate_of_file_id)/list_objects/put_object interfaces myself (Rule 3 deviation) rather than blocking, since this worktree's base commit predates 04-01's own execution despite the declared depends_on=[\"04-01\"]; followed 04-01's already-reviewed spec verbatim to minimize eventual merge conflict"
  - "Content hashing reads raw bytes via TextIOWrapper.buffer rather than the plan's literal 'objects.get_object(...) read in chunks through hashlib' prose (which would decode-then-re-encode text) -- more correct, still uses only ObjectStore's existing public surface"
  - "Fixed a self-duplicate bug in the plan's literal action-text sequence: without correction, find_file_by_content_hash matches a rediscovered file's OWN prior row, permanently mislabeling it a duplicate of itself on every re-run"

requirements-completed: [ORCH-08, ORCH-03, LOAD-03, INCR-08]

# Metrics
duration: 21min
completed: 2026-08-13
---

# Phase 04 Plan 03: Discovery Contracts and discover_files Summary

**`discover_files()` implements the complete ORCH-08 frozen-manifest mechanism -- list once, hash once (raw bytes via `TextIOWrapper.buffer`), content-hash-dedup-check (D-13), freeze an `AssignmentDocument` per surviving unit, cap at `batching.max_units_per_run` -- backed by two new Pydantic cross-boundary contracts (`AssignmentDocument`, `Receipt`) and two required `DatasetConfig` fields.**

## Performance

- **Duration:** 21 min (tool-call span; total session including codebase exploration was longer)
- **Started:** 2026-08-13T14:43:28Z
- **Completed:** 2026-08-13T15:04:40Z
- **Tasks:** 2 completed (both `type="auto" tdd="true"`)
- **Files modified:** 15 (6 created, 9 modified, across 3 task commits + this docs commit)

## Accomplishments

- `AssignmentDocument`/`FileAssignment`/`BatchAssignment` (`dataplat.models.assignment`) and `Receipt` (`dataplat.models.receipt`): the two `extra="forbid"`, `frozen=True` data contracts crossing the Airflow-DAG-to-pod boundary, adapted from ARCHITECTURE.md Sec 6.2/6.3 to this phase's populated fields only
- `SourceConfig.duplicate_policy` (D-13) and `DatasetConfig.batching.max_units_per_run` (ORCH-03, required not defaulted) added to the config model; `configs/datasets/customers.yaml` carries both
- `dataplat.discovery.discover_files()` + `DiscoveredUnit`: lists a bucket/prefix deterministically, streams raw-byte content hashes in bounded chunks, dedup-checks against `meta.files` (D-13 skip policy), pre-allocates idempotent ingestion runs (re-offering `PENDING`/`FAILED`/`RUNNING`, excluding only `SUCCEEDED`), writes a frozen `AssignmentDocument` per surviving unit, and caps the fan-out at `batching.max_units_per_run` without losing the excess
- Implemented the `MetadataRepository`/`ObjectStore` Protocol extensions `discover_files` depends on (`get_or_create_ingestion_run`, `create_file(duplicate_of_file_id=...)`, `list_objects`, `put_object`, `ObjectSummary`) -- these were designed by 04-01-PLAN.md but had not landed in this worktree's base; see Deviations
- 18 new tests (11 unit for Task 1's contracts/config, 7 unit for `discover_files` against fake doubles) plus 5 new integration tests (testcontainers Postgres/MinIO) added for the data-layer deviation, proving the new SQL/S3 operations against real backends, not just fakes

## Task Commits

Each task was committed atomically:

1. **Task 1: AssignmentDocument, Receipt, and the two new config fields** - `3184c28` (feat)
2. **[Deviation] MetadataRepository/ObjectStore Protocol extensions Task 2 depends on** - `3d6fdf9` (fix) -- see Deviations
3. **Task 2: discover_files — list, hash, dedup-check, freeze, cap** - `09eaada` (feat)

**Plan metadata:** (this commit) `docs(04-03): complete discovery contracts and discover_files plan`

## Files Created/Modified

- `packages/dataplat/src/dataplat/models/assignment.py` - `AssignmentDocument`/`FileAssignment`/`BatchAssignment`
- `packages/dataplat/src/dataplat/models/receipt.py` - `Receipt`
- `packages/dataplat/src/dataplat/config/model.py` - `SourceConfig.duplicate_policy`, new `BatchingConfig`, `DatasetConfig.batching`
- `configs/datasets/customers.yaml` - carries `source.duplicate_policy: skip` and `batching.max_units_per_run: 100`
- `packages/dataplat/src/dataplat/discovery.py` - `DiscoveredUnit`, `discover_files()`
- `packages/dataplat/src/dataplat/metadata/repository.py` / `metadata/postgres.py` - `get_or_create_ingestion_run` (new), `create_file` gains `duplicate_of_file_id` and idempotent `ON CONFLICT` semantics
- `packages/dataplat/src/dataplat/storage/objectstore.py` - `ObjectSummary`, `ObjectStore.list_objects`/`put_object`, `S3ObjectStore` implementations
- `tests/unit/test_assignment_document.py`, `tests/unit/test_batching_config.py`, `tests/unit/test_discovery.py` - new unit coverage
- `tests/integration/test_metadata_repository.py`, `tests/integration/test_objectstore.py` - new integration coverage for the data-layer deviation
- `tests/unit/test_config_hashing.py` - fixture documents updated (Rule 1: the two new required `DatasetConfig` fields broke its hardcoded documents)

## Decisions Made

- **Content hashing reads raw bytes, not decoded text.** The plan's action text says to hash via "`objects.get_object(...)` read in chunks through `hashlib.sha256()`", but `get_object` returns a decoded `io.TextIOWrapper`. Rather than decode-then-re-encode (correct only assuming lossless UTF-8 round-trip, and fragile if the assumption is ever wrong), `discover_files` reads through `stream.buffer` -- `TextIOWrapper`'s own public, documented binary-buffer attribute, not a reach into `StreamingBody`'s forbidden private state. This produces a content hash that is unconditionally correct, independent of encoding.
- **Self-duplicate correction.** `find_file_by_content_hash` cannot distinguish "a different `object_uri` already holds this content" (a real D-13 duplicate) from "this `object_uri` was already discovered before" (a rediscovery) -- both match by content hash alone. Without a fix, every rediscovery would mark a file a duplicate of itself and permanently stop offering it. Fixed by comparing `create_file`'s returned `file_id` against the pre-check's `existing_file_id`; a match means self-rediscovery, corrected with a follow-up idempotent `create_file` call.
- **Corrected reading of Task 2's own behavior bullet 2.** The bullet's first clause claims a second call "returns 0 `DiscoveredUnit`s", then self-corrects mid-sentence ("wait: re-discovery of a still-`PENDING` run SHOULD be re-offered") and gives the authoritative worked example (mark one of three `SUCCEEDED`, expect exactly 2 back). Implemented and tested the corrected behavior: `PENDING`/`FAILED`/`RUNNING` runs are re-offered on every call; only `SUCCEEDED` is excluded.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Implemented 04-01-PLAN.md's `MetadataRepository`/`ObjectStore` extensions**
- **Found during:** Task 2 (`discover_files`)
- **Issue:** `discover_files` calls `metadata.get_or_create_ingestion_run(...)`, `metadata.create_file(..., duplicate_of_file_id=...)`, `objects.list_objects(...)` and `objects.put_object(...)`. None of these existed in this worktree's base commit -- `04-01-PLAN.md` (wave 1, this plan's own declared `depends_on: ["04-01"]`) designed them, but 04-01's own execution had not merged into this wave-2 worktree's base when this plan started. This is a genuine cross-plan interface gap, not a task-local bug: `discover_files` cannot type-check or run without these Protocol methods existing.
- **Fix:** Implemented exactly the subset `discover_files` needs -- `MetadataRepository.get_or_create_ingestion_run` (idempotent `INSERT ... ON CONFLICT (idempotency_key) DO UPDATE ... RETURNING run_id, status`), `create_file`'s new `duplicate_of_file_id` parameter and its change from a raising `INSERT` to an idempotent `INSERT ... ON CONFLICT (dataset_id, object_uri, content_sha256) DO UPDATE`, and `ObjectStore.list_objects`/`put_object` plus the new `ObjectSummary` value object -- following 04-01-PLAN.md's already-reviewed interface verbatim (exact SQL shapes, idempotency semantics, error handling) so a later merge against 04-01's own execution should be a same-change/no-op, not a real conflict. Deliberately did NOT implement `claim_ingestion_run`, `finalize_publication`, `ConfigRegistry.get_by_id`, migration 0006, or `PipelineContext.source`/`RunContext.file_id`/`batch_id` -- none of those are called by this plan's own code, and implementing them would be assuming responsibility for another plan's full deliverable.
- **Files modified:** `packages/dataplat/src/dataplat/metadata/repository.py`, `packages/dataplat/src/dataplat/metadata/postgres.py`, `packages/dataplat/src/dataplat/storage/objectstore.py`, `tests/integration/test_metadata_repository.py`, `tests/integration/test_objectstore.py`
- **Verification:** New integration tests added and run against real testcontainers PostgreSQL 18 + MinIO this session (not just fakes) -- `create_file` idempotency including the self-FK constraint, `get_or_create_ingestion_run`'s first-call/repeat-call/status-changed behavior, `list_objects` pagination/prefix-filtering/empty-prefix, `put_object`→`get_object` round-trip. Full `tests/integration` suite re-verified green (19 passed) after these changes; no regression in pre-existing tests.
- **Committed in:** `3d6fdf9`

**2. [Rule 1 - Bug] Self-duplicate correction in `discover_files`**
- **Found during:** Task 2, while writing the re-discovery unit tests
- **Issue:** The plan's literal action-text sequence (`existing_file_id = find_file_by_content_hash(...)`; `duplicate_of_file_id = existing_file_id if ... else None`) does not account for `find_file_by_content_hash` matching the SAME object's own prior row on a second `discover_files` call. Without a fix, every file would be marked `duplicate_of_file_id = <its own file_id>` on the second and every subsequent call, permanently and silently excluding it from being offered again -- contradicting the plan's own behavior spec (still-`PENDING` runs must be re-offered).
- **Fix:** Compare `create_file`'s returned `file_id` against the pre-check's `existing_file_id`; equality means this is a rediscovery of the same object, not a cross-object duplicate -- corrected with a follow-up idempotent `create_file(..., duplicate_of_file_id=None)` call.
- **Files modified:** `packages/dataplat/src/dataplat/discovery.py`
- **Verification:** `tests/unit/test_discovery.py::test_discover_files_re_offers_still_pending_runs_on_a_second_call_with_no_new_rows` and `::test_discover_files_marks_a_content_duplicate_under_a_different_object_uri` both pass, proving same-object rediscovery and cross-object duplication are handled distinctly.
- **Committed in:** `09eaada`

**3. [Rule 1 - Bug] Updated `tests/unit/test_config_hashing.py`'s hardcoded fixture documents**
- **Found during:** Task 1, first test run
- **Issue:** Adding `SourceConfig.duplicate_policy` and `DatasetConfig.batching` as required (non-defaulted) fields broke this pre-existing Phase 3 test file's own hardcoded `_CUSTOMERS_DOCUMENT`/`_CUSTOMERS_DOCUMENT_REORDERED` dicts, which predate these fields.
- **Fix:** Added `duplicate_policy: skip` to both documents' `source` blocks and a `batching: {max_units_per_run: 100}` top-level key, matching `configs/datasets/customers.yaml`'s own values.
- **Files modified:** `tests/unit/test_config_hashing.py`
- **Verification:** All 6 tests in the file pass; full unit+regression suite (116 tests at that point) re-verified green.
- **Committed in:** `3184c28`

---

**Total deviations:** 3 auto-fixed (1 Rule 3 blocking, 2 Rule 1 bugs)
**Impact on plan:** All three necessary for `discover_files` to exist, type-check, and behave correctly per this plan's own behavior spec. No scope creep beyond what Task 2 concretely required -- the Protocol extensions were scoped to exactly the four methods called, not 04-01's full deliverable.

## Issues Encountered

- **Self-referential grep trap:** the plan's own acceptance criterion greps `discovery.py` for `datetime.now`/`date.today`/`logical_date` and requires zero matches -- my first draft's module/function docstrings *described* this constraint using those literal substrings, which would have failed the plan's own verification command. Rephrased the prose to describe the constraint without spelling out the disallowed patterns; both `grep -n "datetime.now\|date.today\|logical_date"` and the plan's exact `grep -c "datetime.now\|date.today" ... | grep -qx 0` now pass.
- **Pre-existing, unrelated test failures discovered during broad verification** (not caused by this plan, not fixed -- see `deferred-items.md`): `tests/policy/test_gates_actually_fail.py::test_forbidden_import_is_rejected` and `::test_good_forbidden_import_is_accepted` fail against the installed `import-linter==2.13`. Both build a synthetic, self-contained `gatecheck` fixture package (unrelated to `dataplat`/`csv_processor`) to meta-test the import-linter gate mechanism itself, and assert on `lint-imports`' exact CLI output substring. `git log` confirms this test file was last touched by a Phase 1 commit; this plan's diff never touches `import-linter`, `setup.cfg`, or this test file. This plan's own import-linter contract (`dataplat` must not depend on `csv_processor`) passes cleanly.
- **Batch proliferation on re-discovery** (logged to `deferred-items.md`, not fixed -- inherited from 04-01-PLAN.md's own `create_batch` design, out of this plan's scope): `create_batch` is not idempotent, so re-running `discover_files` over a still-open (not yet `SUCCEEDED`) file creates a new, orphaned `meta.batches` row every time. Does not affect any of this plan's own behavior guarantees.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `discover_files` is ready for a later plan's CLI/DAG task to invoke directly -- it needs only a `MetadataRepository`, `ObjectStore`, resolved `DatasetConfig`, and the dataset/config/processor identifiers already threaded through `PipelineContext`-adjacent code elsewhere in this phase.
- `AssignmentDocument`/`Receipt` are ready to be read (not just written) by a later plan's `ingest` CLI -- the SAME model validates both directions, per the module docstrings.
- **Known gap for whichever plan lands 04-01's remaining scope:** `claim_ingestion_run`, `finalize_publication`, `ConfigRegistry.get_by_id`, migration 0006 (the `normalized.customers.customer_id` `UNIQUE` constraint), and `PipelineContext.source`/`RunContext.file_id`/`RunContext.batch_id` are NOT yet implemented anywhere in this worktree's lineage. A later plan (04-04/04-05, per 04-01-PLAN.md's own stated consumers) will need these and currently has no source to depend on beyond 04-01-PLAN.md's text itself, unless the orchestrator's merge sequencing brings in 04-01's own execution first.
- **Merge risk to flag to the orchestrator:** this plan's `3d6fdf9` commit modifies `packages/dataplat/src/dataplat/metadata/repository.py`, `metadata/postgres.py` and `storage/objectstore.py` -- files NOT in this plan's declared `files_modified` frontmatter, and files 04-01's own worktree (if it executes independently) will also modify. Both should converge on near-identical content (04-01-PLAN.md's spec was followed verbatim), so a merge conflict here should be trivial to resolve as "keep either side" rather than requiring a real reconciliation -- but it is a real conflict surface the orchestrator should expect.

---
*Phase: 04-vertical-slice-csv-to-analytical-postgresql*
*Completed: 2026-08-13*

## Self-Check: PASSED

All claimed created files verified present on disk (`packages/dataplat/src/dataplat/models/assignment.py`, `models/receipt.py`, `discovery.py`, `tests/unit/test_assignment_document.py`, `test_batching_config.py`, `test_discovery.py`, `deferred-items.md`). All three task/deviation commit hashes (`3184c28`, `3d6fdf9`, `09eaada`) verified present in `git log --oneline --all`.
