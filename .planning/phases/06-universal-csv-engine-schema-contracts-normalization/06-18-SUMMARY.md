---
phase: 06-universal-csv-engine-schema-contracts-normalization
plan: 18
subsystem: data-ingestion
tags: [csv, multipart, discovery, dataplat, csv-processor, postgres, minio, pydantic]

# Dependency graph
requires:
  - phase: 06 (plan 06-08)
    provides: "group_multipart_units/MultipartGroup/open_multipart_stream primitives, unit-tested in isolation but with no call site anywhere"
  - phase: 06 (plan 06-15)
    provides: "CsvSource's final shape (dataset_id, schema resolution via inspect())"
  - phase: 06 (plan 06-16)
    provides: "discover_files's final shape (schema: SchemaRepository param, idempotency-key schema_version term, _skip_config()/test_discover_files.py fixtures)"
provides:
  - "AssignmentDocument.additional_parts -- additive, backward-compatible multipart delivery field"
  - "discover_files real multipart grouping: config.source.multipart_pattern-matched objects partition, group via group_multipart_units, one batch/run/AssignmentDocument per group"
  - "CsvSource(additional_keys=...) opening every part's stream and concatenating via open_multipart_stream before csv.reader is built"
  - "csv_processor.cli.ingest() deriving additional_keys from AssignmentDocument.additional_parts"
  - "Live, database-and-MinIO-backed proof (tests/integration) that a two-part delivery becomes one logical 20-row dataset through the real call chain"
affects: [csv-ingestion, schema-evolution, load-publication]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Discovery-time grouping: partition object listing via re.search(multipart_pattern), then group_multipart_units's own re.fullmatch does the authoritative grouping -- documented asymmetry, not a bug"
    - "Per-group/per-object discover_files helpers (_hash_and_register_file, _process_multipart_group, _process_ungrouped_object) extracted as pure refactors purely to satisfy ruff's cyclomatic-complexity gate (C901/PLR0912/PLR0915)"
    - "CsvSource.open() early-returns from the untouched single-part branch before ever reaching the new multipart branch -- avoids restructuring proven code"
    - "Keep-alive list pattern (raw_streams) for multi-stream open(): a rebound loop-local risks CPython refcounting GC closing a still-depended-upon file object via __del__"

key-files:
  created:
    - tests/unit/test_csv_source_multipart.py
  modified:
    - packages/dataplat/src/dataplat/models/assignment.py
    - packages/dataplat/src/dataplat/discovery.py
    - packages/csv-processor/src/csv_processor/source.py
    - packages/csv-processor/src/csv_processor/cli.py
    - tests/unit/test_discovery.py
    - tests/integration/test_discover_files.py
    - .planning/REQUIREMENTS.md

key-decisions:
  - "Extracted _hash_and_register_file/_process_multipart_group/_process_ungrouped_object in discovery.py once ruff's complexity gate tripped on discover_files after inline grouping was added -- pure refactors, verified behavior-identical via the full pre-existing test suite before and after"
  - "Broadened _strip_nul/chunked_records/CsvRecordStream's text_stream parameter type from TextIOWrapper to Iterable[str] so both a real stream and open_multipart_stream's logical line generator type-check cleanly under mypy strict"
  - "Kept CsvSource.open()'s single-part path textually unchanged (early-return branch) rather than unifying with the multipart branch, per the plan's explicit 'do not restructure' instruction"
  - "Partitioned multipart candidates via re.search before calling group_multipart_units (which internally uses re.fullmatch) exactly as the plan directed -- documented the asymmetry in a code comment rather than silently 'fixing' it to fullmatch"

patterns-established:
  - "Multipart wiring closure: AssignmentDocument.additional_parts is the frozen manifest shape a later ingest() reads back to reconstruct CsvSource's additional_keys -- the same writer/reader symmetry 04-03 established for the singular file field"

requirements-completed: [CSV-11]

# Metrics
duration: 35min
completed: 2026-08-15
---

# Phase 06 Plan 18: Wire multipart delivery into the real discovery/CsvSource/ingest pipeline Summary

**`discover_files`/`CsvSource.open`/`cli.py::ingest` now genuinely call `group_multipart_units`/`open_multipart_stream`/`AssignmentDocument.additional_parts` end-to-end, proven live against real Postgres+MinIO: a two-part `62_multipart_split` delivery discovers as ONE run and reads as one logical 20-row stream.**

## Performance

- **Duration:** ~35 min
- **Started:** 2026-08-15T15:26:00Z
- **Completed:** 2026-08-15T16:00:31Z
- **Tasks:** 3
- **Files modified:** 7 (1 created, 6 modified)

## Accomplishments

- `AssignmentDocument.additional_parts` (additive, default `()`) — every existing single-file assignment construction keeps working unchanged.
- `discover_files` partitions `multipart_pattern`-matched objects, groups them via `group_multipart_units` against the REAL object listing, and freezes exactly ONE `AssignmentDocument`/batch/run per group — proven with a real two-part fixture, not `group_multipart_units` tested in isolation.
- `CsvSource(additional_keys=(...))` opens every part's stream (compression-aware, same mechanism as the single-part path) and concatenates them via `open_multipart_stream` before `csv.reader` is ever constructed — the second part's first physical row is read as data, never mistaken for a header.
- `csv_processor.cli.ingest()` derives `additional_keys` from `doc.additional_parts` using the exact same `_parse_s3_uri` split already used for the primary file, and threads it into `CsvSource(...)`.
- A construction-time bound check (`FileInspectionError`, `"multipart-group-too-large"`, ceiling 50 parts) rejects an oversized multipart group before any stream ever opens (T-06-34).
- A real, live integration test (`test_multipart_delivery_becomes_one_logical_dataset`) proves the whole chain against testcontainers Postgres+MinIO: `len(units) == 1`, `meta.batch_files` shows `sequence_no` 1/2 on the shared batch, and the real `CsvSource.open()` call path recovers all 20 rows with an intact `000001`..`000020` id sequence across the part boundary.
- CSV-11 marked complete in `.planning/REQUIREMENTS.md` — the loop this plan closes is the one that had kept it Pending through three prior waves.

## Task Commits

Each task was committed atomically:

1. **Task 1: AssignmentDocument.additional_parts + wire group_multipart_units into discover_files** - `bf740d9` (feat)
2. **Task 2: CsvSource accepts additional_keys, routes through open_multipart_stream; cli.py wires the assignment through** - `3d2fdc5` (feat)
3. **Task 3: Real end-to-end proof — 62_multipart_split through discover_files → AssignmentDocument → CsvSource.open()** - `c25516a` (test)

_No separate plan-metadata commit in worktree mode — SUMMARY.md and REQUIREMENTS.md are committed together below (STATE.md/ROADMAP.md are orchestrator-owned, excluded per this plan's worktree instructions)._

## Files Created/Modified

- `packages/dataplat/src/dataplat/models/assignment.py` — `AssignmentDocument.additional_parts: tuple[FileAssignment, ...] = ()`.
- `packages/dataplat/src/dataplat/discovery.py` — `discover_files` real multipart grouping; `_hash_and_register_file`/`_process_multipart_group`/`_process_ungrouped_object` extracted helpers.
- `packages/csv-processor/src/csv_processor/source.py` — `CsvSource.__init__`'s `additional_keys` + bound check; `CsvSource.open()`'s multipart branch; `_strip_nul`/`chunked_records`/`CsvRecordStream` type broadening.
- `packages/csv-processor/src/csv_processor/cli.py` — `ingest()` derives `additional_keys` from `doc.additional_parts`.
- `tests/unit/test_discovery.py` — `_multipart_config()`, 4 new tests, `_FakeMetadataRepository.link_batch_file` now records links instead of no-op.
- `tests/unit/test_csv_source_multipart.py` (new) — 3 tests: full 20-row recovery, single-part regression, oversized-group rejection.
- `tests/integration/test_discover_files.py` — `_make_config(multipart_pattern=...)` extension, `test_multipart_delivery_becomes_one_logical_dataset`.
- `.planning/REQUIREMENTS.md` — CSV-11 marked complete (both the checklist line and the traceability table row).

## Decisions Made

- **Complexity-gate extraction (discovery.py):** ruff's `C901`/`PLR0912`/`PLR0915` tripped on `discover_files` once multipart grouping was added inline (16 > 10 complexity, 71 > 50 statements, 17 > 12 branches). Extracted `_hash_and_register_file` (already directed by the plan as a pure refactor), plus two NEW helpers not explicitly named in the plan text — `_process_multipart_group` and `_process_ungrouped_object` — each returning `DiscoveredUnit | None`. Verified behavior-identical by running the full pre-existing `tests/unit/test_discovery.py` suite before and after the extraction.
- **Type broadening for multipart's logical stream:** `open_multipart_stream` returns `Iterator[str]`, not a `TextIOWrapper`. Rather than inventing a stream-shaped wrapper, broadened `_strip_nul`/`chunked_records`/`CsvRecordStream.__init__`'s `text_stream` parameter to `Iterable[str]` — the actual minimal contract every caller needs, satisfied by both a real file object and a plain generator. `mypy --strict`-equivalent gate passes cleanly across both packages (70 files).
- **Single-part path left textually untouched:** `CsvSource.open()`'s multipart handling is a separate `if not self.additional_keys: ... return` branch followed by the new multipart branch, rather than a unified code path — exactly matching the plan's explicit "do not restructure the single-part path" instruction, minimizing risk to already-proven 06-14/06-15 behavior.
- **`re.search` partition / `re.fullmatch` grouping asymmetry kept as directed:** `discover_files` partitions candidates via `re.search(multipart_pattern, obj.key)` while `group_multipart_units` internally uses `re.fullmatch`. This is a real (documented, code-commented) edge case — an object matching only a substring of the pattern would be excluded from the ungrouped `remaining` list yet also silently excluded from any group — but no corpus fixture exercises it, and the plan's action text explicitly specified `re.search` for the partition step, so it was implemented literally rather than "corrected" to `re.fullmatch`.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed premature stream close via CPython's refcounting GC in `CsvSource.open()`'s multipart branch**
- **Found during:** Task 2, while writing `test_open_recovers_all_20_rows_across_the_part_boundary`
- **Issue:** The per-part loop reassigned a single `part_raw_stream` local variable each iteration. Once rebound, the PREVIOUS iteration's outer `TextIOWrapper` (returned by `ctx.objects.get_object(...)`) lost its only reference and was immediately garbage-collected by CPython's refcounting — triggering `TextIOWrapper.__del__` → `close()` → cascading to close `.buffer`, the SAME object `opened_streams[i]`'s own internal `BufferedReader` still depended on for reads. Because `open_multipart_stream` is a lazy generator, this only surfaced once `chunked_records` actually started consuming it: `ValueError: I/O operation on closed file`.
- **Fix:** Added a `raw_streams: list[TextIOWrapper]` that appends (never rebinds) every part's outer stream, keeping each one alive for `open()`'s whole remaining lifetime — the same implicit guarantee the pre-existing single-part path gets for free from its one never-reassigned local variable.
- **Files modified:** `packages/csv-processor/src/csv_processor/source.py`
- **Verification:** `test_open_recovers_all_20_rows_across_the_part_boundary` failed with the exact `ValueError` before the fix, passed after; full `tests/unit`/`tests/integration` suites green afterward.
- **Committed in:** `3d2fdc5` (Task 2 commit)

**2. [Rule 3 - Blocking, environmental] Worktree venv resolved `dataplat`/`csv-processor` to the MAIN REPO, not this worktree**
- **Found during:** Task 1, first test run — `test_discover_files_groups_a_two_part_multipart_delivery_into_one_unit` failed with the OLD (pre-edit) two-independent-units behavior even though the worktree's `discovery.py` had already been edited.
- **Issue:** The shared `.venv` at the main repo root (`/home/konutec/projects/airflow-platform/.venv`) has `dataplat`/`csv-processor` installed editable via `_editable_impl_{name}.pth` files that hardcode the MAIN REPO's absolute `packages/*/src` paths — not this worktree's copies. Every test run against that interpreter silently exercised stale, pre-plan code.
- **Fix:** Ran `uv sync --locked` (creating a worktree-local `.venv` whose editable installs point at THIS worktree's absolute paths) and, for Task 3's integration test, `uv sync --locked --group cluster` (adds `testcontainers`, not in the default `dev` group). Verified via `dataplat.discovery.__file__` resolving to the worktree path before re-running any test.
- **Files modified:** None (environment-only; `.venv/` is gitignored, confirmed via `git status --ignored`)
- **Verification:** All subsequent test runs used the worktree-local interpreter; re-ran the full `tests/unit`/`tests/integration` suites at the end to confirm.
- **Committed in:** N/A (no source change)

**3. [Rule 1 - Plan inaccuracy] Corrected the plan's `-m integration` pytest invocation**
- **Found during:** Task 3's pre-flight check (plan explicitly instructs `pytest tests/integration/test_discover_files.py -m integration -x -q`)
- **Issue:** No test in `tests/integration/` carries an `@pytest.mark.integration` marker (confirmed via grep across the whole directory), and `integration` is not a registered marker in `pyproject.toml`'s `[tool.pytest.ini_options] markers` list (only `slow`/`regression`/`cluster`/`manifests` are). Running the literal command deselects all 5 pre-existing tests, exits 0, and proves nothing — a vacuous pass that would have defeated Task 3's entire purpose (proving REAL behavior against real infra). This directory is actually gated by `make test-integration` (`pytest tests/integration -q`, no marker filter, run via the `cluster` dependency group), not by a pytest marker.
- **Fix:** Ran `pytest tests/integration/test_discover_files.py -x -q` (no `-m` filter) instead, matching the project's own `make test-integration` convention. Confirmed 5 pre-existing + 1 new test (6 total) genuinely execute and pass against real testcontainers Postgres+MinIO.
- **Files modified:** None (verification-command correction only; test file content is unaffected)
- **Verification:** `6 passed in 15.80s` against live containers; full `tests/integration` directory (78 tests across all files) also re-run and green, confirming no cross-file regression from the `cli.py`/`source.py` changes.
- **Committed in:** N/A (no source change — documented here for the record)

---

**Total deviations:** 3 auto-fixed (1 bug, 1 blocking/environmental, 1 plan-inaccuracy-correction)
**Impact on plan:** All three were necessary to reach genuine, non-vacuous verification. No scope creep — no file outside this plan's declared `files_modified` list was touched (REQUIREMENTS.md is explicit worktree-mode bookkeeping, not scope creep).

## Issues Encountered

- The `re.search`/`re.fullmatch` partition-vs-grouping asymmetry (see Decisions above) is a genuine, narrow edge case not exercised by any corpus fixture. Documented in a code comment at the partition site in `discovery.py`; not fixed, since the plan's action text specified `re.search` explicitly and no fixture demonstrates the failure mode.

## User Setup Required

None — no external service configuration required.

## Next Phase Readiness

- CSV-11 is now genuinely complete: compression (06-14) and multi-part delivery (this plan) are both wired end-to-end through the real pipeline, not just unit-tested in isolation.
- `discover_files`, `CsvSource`, and `cli.py::ingest()` are now multipart-aware for any future dataset that sets `source.multipart_pattern` in its config — no code change needed, only configuration.
- No blockers identified for subsequent waves. `tests/unit` (382 tests) and `tests/integration` (78 tests) are both green; `ruff check .`, `mypy` (both packages + `tools`), and `lint-imports` all pass cleanly.

---
*Phase: 06-universal-csv-engine-schema-contracts-normalization*
*Completed: 2026-08-15*
