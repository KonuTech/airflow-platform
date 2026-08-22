# CSV malformed — structural validation and quarantine

Documents an already-built feature (VALID-01, VALID-08), not an incident reconstruction. See
`REQUIREMENTS.md`'s `VALID-01` entry for the exact requirement text.

## Symptoms

An operator sees a run's validation report at `FAIL` or `QUARANTINE` status in
`meta.validation_results`, or individual rows appear in `meta.rejected_records` — never a silent
row-count shortfall with no explanation. Common `error_type` values include `RAGGED_ROW` (field
count does not match the file's expected column count) alongside unclosed-quote and
missing-delimiter diagnostics.

## Diagnosis

```sql
SELECT * FROM meta.validation_results WHERE run_id = <run_id>;

SELECT source_row_number, error_type, error_column, error_message, raw_line
  FROM meta.rejected_records
 WHERE run_id = <run_id>
 ORDER BY source_row_number;
```

`meta.validation_results` reports expected-vs-actual column count and overall run status;
`meta.rejected_records` carries per-row detail, including a reconstruction of the offending row's
text (`raw_line`) for eyeballing. This platform's own `RaggedRowGuard`
(`packages/dataplat/src/dataplat/pipeline/engine.py`) never pads, truncates, or silently coerces a
malformed row — every row whose field count does not match its chunk's expected count becomes a
`RejectedRecord`, never an exception and never a silent drop (QUAL-03's errors-as-values
discipline).

## Recovery

This platform never auto-repairs row content. Fix the malformation at its source (the delimiter,
quoting, or line-ending problem that produced the ragged/unclosed row) and re-upload a corrected
file under the dataset's normal ingestion path.

## Reprocessing

A corrected file re-discovers and redrives any `PENDING` reject sharing the same
`(dataset_id, business_key)` — see [`failed-backfill.md`](failed-backfill.md) for the full
mechanism and its one deliberate exception: a **structurally** rejected row (`business_key IS
NULL` — e.g. a ragged row, where field positions were unreliable at rejection time) is never
auto-resolved by design (D-25). A structural reject needs direct review of the corrected file's
output rather than relying on the automatic redrive.

## Verification

1. Re-running the corrected file produces `PASS` or `PASS_WITH_WARNING` in
   `meta.validation_results`, not `FAIL`/`QUARANTINE`.
2. For a reject with a non-NULL `business_key`: `meta.rejected_records.resolution_type` flips from
   `PENDING` to `REDRIVEN`.
3. The corrected rows are visible in the target table with the expected values.
