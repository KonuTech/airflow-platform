# Deferred Items — Phase 8

Out-of-scope findings discovered during plan execution, deliberately NOT
auto-fixed (SCOPE BOUNDARY: only fix issues directly caused by the current
task's own changes).

## From plan 08-06

- **`make typecheck` fails on `packages/csv-processor/src/csv_processor/cli.py:109`**
  (`error: Cannot instantiate abstract class "PostgresMetadataRepository" with
  abstract attributes "record_rejected_records", "record_validation_results"
  and "resolve_rejected_records_for_batch"`). Root cause: plan 08-01 (merged
  to `main` as part of Phase 8 Wave 1) widened the `MetadataRepository`
  `Protocol` with three new methods
  (`packages/dataplat/src/dataplat/metadata/repository.py` lines 512-595+),
  but `PostgresMetadataRepository`'s concrete implementation of those methods
  evidently lands in a different Wave-2 plan (likely 08-03/08-04/08-05,
  executing in parallel sibling worktrees), not yet merged into this plan's
  branch at the time of this session. Plan 08-06 never touches
  `csv_processor/cli.py` or `PostgresMetadataRepository` at all — this error
  is present in the merged `main` baseline this plan branched from,
  independent of any change 08-06 made. Verify it self-resolves once the
  sibling wave-2 plan(s) implementing those methods merge; if it does not,
  it needs its own fix in a future plan/gap-closure.
