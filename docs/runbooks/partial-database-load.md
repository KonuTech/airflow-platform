# Partial database load — one query answers what to retry

Documents an already-built feature. `REQUIREMENTS.md`'s traceability table still marks `LOAD-06` as
"Pending" — that is **stale documentation** predating this feature's actual delivery in Phase 9
(plan 09-06); updating that table is out of scope for this runbook. The real, currently-built
mechanism is `meta.v_run_recovery`, proven by `tests/integration/test_run_recovery_view.py`.

## Symptoms

A run stopped mid-pipeline — a pod crash, a node eviction, an unexpected process exit — and an
operator needs to know what already succeeded, what remains, and whether to retry or roll back,
without reading logs.

## Diagnosis

```sql
SELECT * FROM meta.v_run_recovery WHERE run_id = <run_id>;
```

This single view (migration `0033`) joins `meta.ingestion_runs` with `meta.run_stages` across all
three pipeline stages — `STAGE_LOAD`, `DBT_BUILD`, `PUBLISH` — and returns a `next_action` column
that always reads one of exactly two shapes: `'retry stage <NAME>'` or `'complete'`. It is grantable
to `grafana_reader` too, so the same answer is available from a dashboard, not only `psql`.

## Recovery

Recovery here is **always retry-only** — never rollback. Every stage either commits atomically or
does not commit at all (META-03's single-transaction publish guarantee), so there is never a
partially-committed stage to roll back from. The literal word "rollback" never appears in any
`next_action` value the view can produce — proven directly by
`tests/integration/test_run_recovery_view.py::test_next_action_never_implies_rollback` across every
scenario the view's test suite exercises. Re-trigger exactly the stage `next_action` names; do not
re-run earlier, already-`SUCCEEDED` stages.

## Reprocessing

Retrying the named stage is always safe — every stage in this pipeline is idempotent under retry by
construction, so re-running `STAGE_LOAD`, `DBT_BUILD`, or `PUBLISH` for a run that partially
completed never double-applies work or produces duplicate rows.

## Verification

1. `meta.v_run_recovery`'s `next_action` for the run reads `'complete'` once the named stage has
   been successfully retried.
2. `tests/integration/test_run_recovery_view.py` passes — its 5 scenarios (all-succeeded, a missing
   stage row, no stage rows at all, a failed publish stage, and the rollback-never-appears
   non-vacuity check) are this platform's own standing proof of the view's join logic.
