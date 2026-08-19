---
phase: 09-etl-correctness-dedup-incremental-backfill-recovery
plan: 03
subsystem: etl-correctness
tags: [pydantic, control-total, batch-complete-marker, minio, assignment-document, run-context]

# Dependency graph
requires:
  - phase: 08-validation-quarantine-metadata-control-plane-completion
    provides: "The presence-only `_BATCH_COMPLETE` marker gate (`_apply_batch_complete_marker_gate`, LOAD-11/D-19) this plan extends to actually read the object body"
provides:
  - "`BatchCompleteManifest` Pydantic model + `parse_batch_complete_manifest()` -- a validated, bounded parser for the marker object's JSON body"
  - "`_apply_batch_complete_marker_gate` now reads and parses the marker body, withholding the batch on any malformed content"
  - "`AssignmentDocument.batch_complete_manifest` (additive optional field) carrying the parsed manifest from discovery to the stage pod"
  - "`RunContext.batch_expected_row_count`/`batch_expected_checksum` -- the control total threaded all the way into the staging pod, ready for a later plan's reconciliation comparison"
affects: [09-07]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Marker-body-parse-failure withholds the whole batch identically to marker-absence (never a distinct code path) -- 'never trust unread content'"
    - "Additive-optional-field-with-default precedent (mirrors `additional_parts`) for threading a new value through a frozen, extra=forbid manifest model without touching any existing construction site"

key-files:
  created:
    - packages/dataplat/src/dataplat/validate/batch_complete_manifest.py
    - tests/unit/validate/test_batch_complete_manifest.py
  modified:
    - packages/dataplat/src/dataplat/discovery.py
    - packages/dataplat/src/dataplat/models/assignment.py
    - packages/dataplat/src/dataplat/models/identity.py
    - packages/csv-processor/src/csv_processor/cli.py
    - tests/unit/validate/test_batch_complete_marker.py
    - tests/unit/test_csv_processor_cli.py

key-decisions:
  - "ConfigurationError (not a new error subclass) wraps a malformed manifest's pydantic.ValidationError -- it is a config-shaped validation failure, matching AssignmentDocument's own T-04-02 precedent, and no dedicated subclass exists for this narrower case"
  - "BatchCompleteManifest import in models/assignment.py is a genuine runtime import (# noqa: TC001), not TYPE_CHECKING-only -- pydantic v2 resolves field annotations at class-creation time and needs the real class in scope"

requirements-completed: [VALID-06]

# Metrics
duration: ~35min
completed: 2026-08-19
---

# Phase 09 Plan 03: Read and Thread the `_BATCH_COMPLETE` Manifest Body Summary

**Extended the previously presence-only `_BATCH_COMPLETE` marker gate to actually fetch and parse its JSON body via `BatchCompleteManifest`, threading the resulting control total (`expected_row_count`/`expected_checksum`) end-to-end from `discover_files` through `AssignmentDocument` into the staging pod's `RunContext`, with a malformed body withholding the whole batch instead of crashing discovery.**

## Performance

- **Duration:** ~35 min
- **Tasks:** 3/3 completed
- **Files modified:** 6 (2 created, 6 modified — see key-files above; assignment.py, discovery.py, identity.py, cli.py + 2 test files, plus 1 new source file and 1 new test file)

## Accomplishments

- `BatchCompleteManifest` (Pydantic, `extra="forbid"`/`frozen=True`) and `parse_batch_complete_manifest()` give the marker's body a real, validated, bounded shape for the first time — `expected_row_count` non-negative, `expected_checksum` optional and length-bounded, an unrecognized key or malformed JSON caught and re-raised as a structured `ConfigurationError` rather than a raw `pydantic.ValidationError`.
- `_apply_batch_complete_marker_gate` now calls `objects.get_object(...)` when the marker is found, matching this codebase's "never trust unread content" discipline for every other CSV validation barrier — a present-but-unparseable body is treated identically to "marker absent" (batch withheld, `[]` returned, no exception escapes `discover_files`).
- `AssignmentDocument.batch_complete_manifest` (additive, default `None`) and `RunContext.batch_expected_row_count`/`batch_expected_checksum` (additive, default `None`) thread the parsed value all the way to the staging pod without touching any pre-existing single-file/multipart construction site — proven by the untouched `AssignmentDocument(...)` calls throughout `discovery.py` continuing to type-check and pass unchanged.

## Task Commits

Each task was committed atomically (TDD RED/GREEN for Task 1, single commits for Tasks 2/3):

1. **Task 1: BatchCompleteManifest model + parser** — `755cfa1` (test, RED) / `a4f12ca` (feat, GREEN)
2. **Task 2: Read the marker body in discover_files, thread it onto AssignmentDocument** — `863a120` (feat, includes a Rule 1 lint fix to Task 1's file)
3. **Task 3: RunContext fields + stage command wiring** — `3824c1b` (feat)

**Plan metadata:** this commit (docs: complete plan)

## Files Created/Modified

- `packages/dataplat/src/dataplat/validate/batch_complete_manifest.py` — new: `BatchCompleteManifest` model + `parse_batch_complete_manifest()`
- `packages/dataplat/src/dataplat/discovery.py` — `_apply_batch_complete_marker_gate` reads/parses the marker body (3-tuple return), both `AssignmentDocument(...)` construction sites (`_process_multipart_group`, `_process_ungrouped_object`) thread the resolved manifest
- `packages/dataplat/src/dataplat/models/assignment.py` — `AssignmentDocument.batch_complete_manifest: BatchCompleteManifest | None = None`
- `packages/dataplat/src/dataplat/models/identity.py` — `RunContext.batch_expected_row_count`/`batch_expected_checksum`
- `packages/csv-processor/src/csv_processor/cli.py` — `stage` command's `RunContext(...)` construction populates the two new fields from `doc.batch_complete_manifest`
- `tests/unit/validate/test_batch_complete_manifest.py` — new: 6 tests covering the model/parser's 5 documented behaviors
- `tests/unit/validate/test_batch_complete_marker.py` — extended: marker+valid-body now asserts every produced `AssignmentDocument` carries the manifest; added a marker+malformed-body case (whole batch withheld, no exception)
- `tests/unit/test_csv_processor_cli.py` — added a manifest-bearing assignment fixture and two tests proving both the populated and absent-manifest (byte-for-byte-unchanged) `RunContext` cases

## Decisions Made

- **`ConfigurationError` for manifest parse failures.** No dedicated exception subclass exists for "an attacker-influence-adjacent config-shaped body failed validation" narrower than `ConfigurationError` itself; reusing it matches `AssignmentDocument`'s own `stage()`-command precedent (cli.py's existing `ConfigurationError` wrap of a `ValidationError` on the assignment document itself).
- **Genuine runtime import, not `TYPE_CHECKING`, for `BatchCompleteManifest` in `assignment.py`.** Ruff's TC001 initially flagged the import as type-annotation-only; suppressed with an inline `# noqa: TC001` plus an explanatory comment, since pydantic v2 resolves `AssignmentDocument`'s string annotations (from `from __future__ import annotations`) against real, imported names at class-creation time — a `TYPE_CHECKING`-only import would break model construction at import time.

## Deviations from Plan

None — plan executed exactly as written. The one existing test (`test_discover_files_discovers_normally_once_marker_object_is_present`) needed updating because its marker fixture body (`b""`, empty bytes) became invalid JSON under the new parse-the-body behavior this plan intentionally introduces — this was anticipated by Task 2's own acceptance criteria ("new cases added: marker present + valid body...") rather than an unplanned deviation.

## Known Stubs

None — no stub/placeholder data introduced. `batch_expected_row_count`/`batch_expected_checksum` are `None` by design whenever no marker is configured or none is present, which is the documented, tested default behavior, not a stub standing in for missing functionality.

## Threat Flags

None — every new surface (the marker body read, the new Pydantic model, the two additive fields) is explicitly covered by this plan's own `<threat_model>` (T-09-06 Tampering, T-09-07 DoS, T-09-08 Information Disclosure), all addressed as designed: T-09-06 by treating `expected_*` as an unverified claim (comparison logic deferred to plan 09-07, never silently trusted here); T-09-07 by `max_length=128` + `extra="forbid"` + a flat two-field schema; T-09-08 by logging only `marker_key`/`dataset`, never the raw body.

## Verification

`pytest tests/unit/validate/test_batch_complete_manifest.py tests/unit/validate/test_batch_complete_marker.py tests/unit/test_csv_processor_cli.py -q` — 20 passed.
`uv run mypy packages/dataplat/src/dataplat/validate/ packages/dataplat/src/dataplat/discovery.py packages/dataplat/src/dataplat/models/` — Success: no issues found in 19 source files.
Full regression sweep: `pytest tests/unit -q` — 506 passed (no other test broken by the additive `AssignmentDocument`/`RunContext` fields).
`ruff check` on every created/modified file — all checks passed.

## Self-Check: PASSED

All 8 created/modified files verified present on disk (`ls -la`); all 4 task commits
(`755cfa1`, `a4f12ca`, `863a120`, `3824c1b`) verified present in `git log`.
