# Late-arriving data — routed by event time, never by arrival order

Documents an already-built feature (INCR-03, INCR-04), not an incident reconstruction. See
`REQUIREMENTS.md`'s `INCR-03`/`INCR-04` entries for the exact requirement text. As of Phase 08.1
(ADR-0010), this mechanism lives in the dbt bronze-to-silver layer, not hand-written Python.

## Symptoms

A batch contains rows whose business/event time is older than data already processed for the same
key, or rows arrive out of event-time order within or across batches — an operator wants
confirmation these were routed correctly rather than discarded or allowed to silently overwrite
newer state.

## Diagnosis

`dbt/models/silver/silver_customers.sql`'s incremental model resolves this via
`existing_silver_contenders`: every candidate row's `event_ts` is compared against what is already
in `silver`, so a late-arriving bronze row with an OLD `event_ts` can never overwrite a correct,
later-`event_ts` silver row merely because it happens to *arrive* after it. Query the model's
`_dbt_loaded_at` column to distinguish "when this row was processed" (arrival/ingestion time) from
"when the event actually happened" (`event_ts`, business/source effective time) for any row in
question.

## Recovery

No manual intervention is needed for correctly out-of-order data — the incremental model is
specifically designed so arrival order never determines correctness, only `event_ts` does. If a row
appears to have won or lost incorrectly, first confirm which `event_ts` each contending row
actually carries; a genuine defect here would be a `dataplat`/dbt-model bug, not an operational
condition to work around.

## Reprocessing

Late-arriving rows route through the exact same pipeline as any other batch — discovery, structural
and quality validation, normalization, deduplication, load — with no simplified bypass path. The
dbt model naturally reconciles them into the correct historical position on its own next scheduled
run; no special "late data" re-drive procedure exists or is needed.

## Verification

1. After the late row loads, the silver-layer table shows the highest-`event_ts` value winning for
   the affected business key, regardless of which batch or arrival order it came from.
2. No row was discarded — `meta.files`/`meta.batches` records the late file as processed, not
   dropped or skipped.
3. `_dbt_loaded_at` on the winning row reflects the actual processing time, distinct from
   `event_ts`, confirming the two concepts were not conflated.
