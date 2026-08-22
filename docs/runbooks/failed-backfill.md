# Failed backfill — lost row-lock race, or a rejected row that never redrives

Sourced from [`.planning/debug/resolved/backfill-does-not-redrive-rejected-row.md`](../../.planning/debug/resolved/backfill-does-not-redrive-rejected-row.md)
(2026-08-17). That investigation found two distinct, sequential failure modes behind the same
symptom class — both are documented here, in the order you are likely to hit them. The `##
Reprocessing` section also cites INCR-06's historical-schema-resolution mechanism, since a failed
backfill's correct re-drive path depends on it.

## Symptoms

**Mode 1 — the backfill appears to do nothing.** `airflow backfill create` exits `0`, but
`dag_run.clear_number` never advances and `dag_run.state` stays at its old value (typically
`success`) for the whole polling window. No error is printed anywhere.

**Mode 2 — the backfill genuinely re-executes, but a corrected row stays rejected.** The backfill's
`DagRun` really does clear and reach `success` again (`clear_number` advanced), the corrected file
really did load, but the original `meta.rejected_records` row for that business key is still
`resolution_type = 'PENDING'`, not `'REDRIVEN'`.

## Diagnosis

**For Mode 1**, query the backfill's own bookkeeping tables first:

```sql
SELECT b.id, b.dag_id, b.completed_at,
       bdr.dag_run_id, bdr.exception_reason
  FROM backfill b
  JOIN backfill_dag_run bdr ON bdr.backfill_id = b.id
 WHERE b.dag_id = '<dag_id>'
 ORDER BY b.id DESC, bdr.id DESC
 LIMIT 5;
```

`exception_reason = 'in flight'` with `dag_run_id IS NULL` means Airflow 3.3.0's own
`airflow backfill create` lost a `SELECT ... FOR UPDATE SKIP LOCKED` row-lock race against
concurrent scheduler activity on the target `dag_run` row — a real, documented, **no-retry-by-
default** behavior in Airflow's own `_create_backfill_dag_run_non_partitioned`, not a `dataplat`
defect. The CLI exits `0` regardless, because Airflow raises no exception for this outcome.

**For Mode 2**, the reject was never eligible to redrive at all. Resolution matches
`(dataset_id, business_key)` — **never** `batch_id` — because `discover_files`'s `batch_key` is a
pure function of file content (`content_sha256`): a corrected file's bytes differ from the
original, so it always discovers under a **new** `batch_id`, one the original PENDING row never
belonged to. Check the reject's own `business_key` column:

```sql
SELECT rejected_record_id, dataset_id, business_key, resolution_type, error_type
  FROM meta.rejected_records
 WHERE rejected_record_id = <id>;
```

A `business_key IS NULL` row (a structural rejection — e.g. a ragged row, where field positions
were unreliable at rejection time) is **deliberately never auto-resolved** by
`resolve_rejected_records_for_business_keys` — this is D-25's designed behavior, not a bug. Only a
row with a real, non-NULL `business_key` is eligible for automatic redrive.

## Recovery

**Mode 1:** before retrying `airflow backfill create`, confirm the *prior* attempt's own
`completed_at IS NOT NULL`, not merely that its `exception_reason` was observed — under contention
the gap between "exception_reason written" and "backfill fully completed" can be 10-20+ seconds,
wide enough for an eager retry to collide with `AlreadyRunningBackfill` (Airflow allows only one
active backfill per `dag_id` at a time). Wait for `completed_at`, then retry.

**Mode 2:** if the reject's `business_key` is non-NULL and the corrected file genuinely ingested
under the same `dataset_id`, re-check that the correction actually reached
`resolve_rejected_records_for_business_keys` — normal operation requires no manual SQL. If
`business_key IS NULL`, this row structurally cannot auto-redrive; it needs direct human review of
the corrected output rather than another backfill attempt.

## Reprocessing

A backfill always runs through the exact same pipeline as normal ingestion — discovery, structural/
quality validation, normalization, deduplication, load, lineage — with no simplified bypass path
(INCR-05). Critically, a historical file reprocesses under **its own** historical schema version,
never the dataset's current one: `SchemaRepository.resolve_by_hash`
(`packages/dataplat/src/dataplat/schema/repository.py`) matches a re-derived file structure against
**any** historical `meta.schema_versions` row for the dataset, not only the current one — so a file
whose structure matches a schema from three versions ago resolves to that historical version rather
than being forced through today's schema (INCR-06, SCHEMA-06's own D-16 mechanism). This is what
makes a backfill of an old file safe to re-run without first widening or rewriting the dataset's
current contract.

## Verification

1. `SELECT completed_at, exception_reason FROM backfill WHERE id = <latest id>` shows
   `completed_at IS NOT NULL` and `exception_reason IS NULL` (not `'in flight'`).
2. `dag_run.clear_number` incremented and `dag_run.state = 'success'` for the target logical date.
3. For a business-key-eligible reject: `meta.rejected_records.resolution_type = 'REDRIVEN'` and
   `resolved_by_run_id` points at the backfill's own run.
4. The corrected row is visible in the target table with the expected values.
