# SCD correction — recomputing history from ordered bronze, never in-place surgery

Documents an already-built feature (SCD-07), not an incident reconstruction. See
`REQUIREMENTS.md`'s `SCD-07` entry for the exact requirement text.

## Symptoms

A dimension's historical validity intervals (`valid_from`/`valid_to`/`is_current`) need correction
because a late-arriving change record affects a period that was already published — for example, a
customer attribute change whose true source-effective time predates the most recently loaded
version for that key.

## Diagnosis

`SCDPublisher` (`packages/dataplat/src/dataplat/load/publish/scd.py`) recomputes affected keys' FULL
version chain from `staging.customers` — the durable, cumulative, **never-deduplicated** bronze
table — never from `silver.customers`, which dbt's own `delete+insert` incremental strategy
collapses to exactly one row per business key and would make a late correction unrecoverable if
used as the source. Confirm the affected key is genuinely in this run's touched-key set:

```sql
SELECT DISTINCT customer_id FROM staging.customers WHERE _run_id = ANY(<staged_run_ids>);
```

## Recovery

No manual SQL surgery on `normalized.customers` — direct interval edits are exactly what this
mechanism exists to avoid. `recompute_version_chain`
(`packages/dataplat/src/dataplat/scd/recompute.py`, a pure function with no I/O) deterministically
rebuilds the affected key's **entire** version chain from its full ordered bronze history, then
`SCDPublisher` performs an atomic `DELETE` + `INSERT` replace for that key inside the same
transaction as everything else in the run — never a partial, in-place patch of one interval.

## Reprocessing

Re-running with the same staged content recomputes an identical chain — SCD-09's idempotent-replay
guarantee makes the `DELETE`+`INSERT` a no-op in effect (not merely in intent), so it is always safe
to retry a correction run rather than needing to reason about whether it already applied.

## Verification

1. `normalized.customers` shows the corrected `valid_from`/`valid_to`/`is_current` sequence for the
   affected `customer_id`, with no overlapping validity intervals.
2. Re-running the same correction produces no further row changes for that key.
3. Every other, unaffected key's version chain is unchanged — the replace is scoped strictly to the
   touched-key set, not a full-table rebuild.
