---
created: 2026-08-29
title: Investigate u3's new replay/duplicate ingestion_runs row + poll_run_for_file's missing ORDER BY/LIMIT
area: platform
files:
  - tests/e2e/slice/conftest.py
  - packages/dataplat/src/dataplat/pipeline/run.py
  - packages/dataplat/src/dataplat/metadata/postgres.py
  - packages/dataplat/src/dataplat/metadata/repository.py
---

## Problem

`tests/e2e/slice/test_pod_kill_retry.py::test_u3_throughput_and_peak_rss_baseline` --
previously reliably PASSING, most recently confirmed passing in the combined-verification
run at 12:44:10Z ("the queue-idle-budget fix is CONFIRMED working for this specific test") --
FAILED for the FIRST TIME in this debug session's history during ROUND 23's live-verification
run (`e2e-full.yml` run 33255828661, headSha 61df231, 2026-08-29 15:54:22Z):

```
AssertionError: expected a nonzero rows_loaded, got 0
```

This is qualitatively different from every other failure mechanism this session has found
(dbtkill/scd_concurrent/orphan are all timeout/scheduling races) -- u3's own preceding
`assert outcome['status'] == 'SUCCEEDED'` PASSED, meaning `poll_ingestion_run` genuinely
observed a terminal `SUCCEEDED` status for the idempotency_key the test itself captured
(`a7fa8b8fad3d1821b8877b18263ad979058da67a6bfd546de826bbb43145dd18`). A run reporting
`SUCCEEDED` with `rows_loaded=0` is a DATA-shape failure on an ostensibly-successful run, not
a timing/scheduling failure.

The test itself also took an anomalous ~41m43s (14:12:39Z->15:54:22Z) versus its historically
fast, isolated baseline-measurement duration -- most of that gap is independently explained by
`wait_for_orders_dagrun_queue_idle` draining the SAME run's own dbtkill test's orphaned
background DagRun (`e2e-dbtkill-aa98d8ce5edc`, which kept retrying `stage` in the background
after the dbtkill test's own pytest assertion gave up at 15:12:39Z, not reaching its own
terminal FAILED state until 15:38:20Z) -- itself a symptom of the SAME already-ticketed
CPU-starvation/queue-backlog condition (see
`2026-08-29-investigate-cpu-starvation-headroom.md`), not something to re-investigate here.

### Direct evidence (from the ROUND 23 end-of-job `meta.ingestion_runs -> meta.files` mapping dump)

`file_id=104` (`s3://raw/orders/e2e-u3-1de5563e87fc.csv`, this test's own single,
uniquely-marked upload) has TWO `meta.ingestion_runs` rows at session end:

- `run_id=64`: `status=SUCCEEDED`, `idem_prefix=32f65e82d7b244eaa6d1`, `replay_of_run_id`
  blank.
- `run_id=280`: `status=STAGED`, `idem_prefix=a7fa8b8fad3d1821b887`, `replay_of_run_id=64`.

`run_id=280`'s full idempotency_key is the EXACT value the failing test's own pytest
traceback captured as its own polled run -- confirming the test WAS reading the correct
(non-stale) run, not a wrong row picked up by a missing `ORDER BY`/`LIMIT` (see the second,
separate gap below -- that gap is real but did NOT cause this specific failure).

Two puzzles this round's evidence could not fully resolve without deeper live querying (the
ephemeral cluster was already gone by the time this was investigated):

1. How did `poll_ingestion_run` observe `status='SUCCEEDED'` for `run_id=280` during the test
   (~15:54:xx) when the SAME `run_id` shows `status='STAGED'` in the end-of-job snapshot taken
   ~1 hour later (16:56:19Z)? A terminal status should never revert -- so either this is a
   genuine state-machine violation, or two distinct physical writes are involved that this
   round's evidence does not disambiguate.
2. Airflow's OWN DagRun for this test's trigger (`e2e-u3-1de5563e87fc`, started 15:54:51Z)
   shows `stage` retried 5 times and FAILED at 16:09:44Z (`dbt_build` upstream_failed), yet
   `publish` still reported SUCCESS at 16:10:08Z -- consistent with `run_id=280` ending up
   orphaned at its last real application-level status (`STAGED`), the SAME
   "orphaned-at-last-real-status" class this debug session's ROUND 18 root-caused for
   dagrun_timeout/retry-exhaustion scenarios, now apparently recurring for u3 specifically for
   the first time.

### Separate, independent gap: `poll_run_for_file` has no `ORDER BY`/`LIMIT`

`poll_run_for_file()` (`tests/e2e/slice/conftest.py:1074`) queries
`meta.ingestion_runs WHERE file_id = %s` with `cur.fetchone()` and NO `ORDER BY`/`LIMIT` --
when a `file_id` legitimately has multiple rows (a "replay", evidenced here by the populated
`replay_of_run_id` column, a mechanism this platform's own schema clearly anticipates), this
query's result is non-deterministic between calls, independent of whichever specific
mechanism is producing the replay in the first place. This round's evidence confirms the
failing test happened to read the correct (newest) row this time, but the query itself
provides no guarantee of that in general -- any test relying on `poll_run_for_file` against a
file with multiple ingestion runs is exposed to this non-determinism.

## Solution

Not investigated this round (deliberately captured as a todo per the ROUND 24 user decision --
Track C is scope-excluded from ROUND 24's charter). Start a new, separate `/gsd:debug` or
`/gsd:quick` investigation once `debug/ci-pipeline-ingestion-timeout` closes out, or fold it in
as an explicit later round if that session is still open when this is picked up.

Candidate directions (not mutually exclusive):

1. **Root-cause the replay itself.** Determine what produces a SECOND `meta.ingestion_runs`
   row (`run_id=280`, `replay_of_run_id=64`) for a file (`file_id=104`) that u3 uploads
   exactly once per test run -- is this a legitimate replay mechanism firing unexpectedly
   (e.g. discovery re-processing the same S3 key under contention), or a genuine duplicate-row
   defect?
2. **Resolve the status-reversion puzzle.** Confirm (via live querying on a fresh run, with
   the ephemeral cluster still up) whether `run_id=280` genuinely transitioned
   `SUCCEEDED -> STAGED` (which would be a real state-machine violation worth its own root
   cause), or whether the `SUCCEEDED` `poll_ingestion_run` observed belongs to a DIFFERENT
   physical write than the `STAGED` row the end-of-job snapshot later captured.
3. **Fix `poll_run_for_file`'s missing `ORDER BY`/`LIMIT`.** Independent of (1)/(2): add
   `ORDER BY run_id DESC LIMIT 1` (or equivalent, matching this codebase's own established
   "newest row wins" convention used elsewhere, e.g. `_latest_backfill_id`/
   `_wait_for_new_dag_run_terminal`) so this helper's result is deterministic even when a
   `file_id` legitimately has multiple `meta.ingestion_runs` rows. Low-risk, mechanical, but
   worth doing regardless of how (1)/(2) resolve -- a test-harness query should never be
   silently non-deterministic.
4. Re-check whether this reproduces on a fresh live run before assuming it is a stable,
   deterministic defect (like ROUND 22's scd_concurrent finding, this was observed exactly
   once as of this writing) -- CPU-contention level and the specific interleaving with
   dbtkill's own background retries may be load-bearing to reproduction, not incidental.
