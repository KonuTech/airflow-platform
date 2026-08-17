---
status: resolved
trigger: "backfill-does-not-redrive-rejected-row: airflow backfill create against csv_ingest_customers for a content-differing corrected re-upload does not trigger genuine re-execution / does not flip PENDING rejected_records to REDRIVEN"
created: 2026-08-17T00:00:00Z
updated: 2026-08-17T19:00:00Z
---

## Live Verification (2026-08-17T18:36Z, orchestrator follow-up)

Ran the two SQL queries this session's own `next_action` called for, live, against `airflow-db-1` (namespace `data`):

```sql
SELECT b.id, b.dag_id, b.reprocess_behavior, b.created_at, b.completed_at,
       bdr.logical_date, bdr.dag_run_id, bdr.exception_reason
FROM backfill b JOIN backfill_dag_run bdr ON bdr.backfill_id = b.id
WHERE b.dag_id='csv_ingest_customers' ORDER BY b.id DESC, bdr.id DESC LIMIT 10;
```

Result: `backfill_id=1`, `dag_run_id=NULL`, **`exception_reason='in flight'`**. This refutes BOTH standing hypotheses (zero-task_instance-rows via `DAG.clear()`, and the batch_key/content_sha256 architecture concern) — `dag_run_id` being NULL means `_handle_clear_run`/`DAG.clear()` was never even reached.

Traced the actual branch in the installed `apache-airflow==3.3.0` `airflow/models/backfill.py` (`_create_backfill_dag_run_non_partitioned`): for `dr.state=SUCCESS` + `reprocess_behavior=COMPLETED`, `_get_dag_run_no_create_reason()` correctly returns `None` (no early exit). Execution proceeds to:

```python
lock = session.execute(with_row_locks(
    query=select(DagRun).where(DagRun.logical_date == info.logical_date, DagRun.dag_id == dag.dag_id),
    session=session, skip_locked=True,
))
if lock:
    _handle_clear_run(...)
else:
    # records exception_reason = IN_FLIGHT
```

`SELECT ... FOR UPDATE SKIP LOCKED` (`skip_locked=True`) lost its lock race against a concurrent transaction — almost certainly the scheduler's own periodic dag_run-row locking, plausibly worsened by this cluster's already-documented CPU/scheduler contention (STATE.md 2026-08-16/17 entries). There is **no retry** anywhere in this code path — a single lost race permanently records `IN_FLIGHT` for that backfill invocation, with no exception surfaced to the CLI caller (`exit 0`).

**Proof the mechanism itself is NOT broken:** manually re-ran the identical CLI command a second time, no code change:

```
airflow backfill create --dag-id csv_ingest_customers \
  --from-date 2026-08-17T14:55:00+00:00 --to-date 2026-08-17T14:55:00+00:00 \
  --reprocess-behavior completed
```

This time: `backfill_id=2`, `dag_run_id=2781`, `exception_reason=NULL`. Confirmed via direct `dag_run` query: `clear_number` `0 -> 1`, `state` `'success' -> 'running'`, `run_type` `'scheduled' -> 'backfill'`. The clear-and-reprocess mechanism (`_handle_clear_run` -> `DAG.clear()` -> `clear_task_instances()`) works correctly once the row lock is actually acquired.

**Conclusion:** Root cause is a one-shot, no-retry race in Airflow 3.3.0's own `airflow backfill create` (`with_row_locks(skip_locked=True)` losing to concurrent scheduler activity), NOT a `dataplat`/DAG code defect and NOT the batch_key/content_sha256 architecture concern (that remains a separate, legitimate, still-untested downstream question — re-test once a run genuinely re-executes). `test_backfill_resolves_previously_rejected_row` makes exactly one CLI invocation with no retry/backoff for this documented-transient Airflow-level outcome, so it is a real test-robustness gap worth a targeted fix (retry `airflow backfill create` — or poll `backfill_dag_run.exception_reason` and re-invoke — on `IN_FLIGHT` before declaring failure), not a `dataplat` architecture fix.

## Live Re-Verification (2026-08-17T19:52Z, post plan 08-15 + monitoring CPU trim)

Re-ran `pytest tests/e2e/slice/test_backfill_reentry.py -x -m cluster` live, twice, after deploying the monitoring-stack CPU trim (quick task `260817-rvq`) which freed real headroom (`airflow-platform-worker` 91-100%→79%, `worker2` 87%→77%).

**Attempt 1** (customers DAG mistakenly paused by the orchestrator to "reduce contention," following `test_referential_orphan.py`'s unrelated recommendation): failed immediately at `poll_file_discovered` for the ORIGINAL file — `discover` never ran because pausing `csv_ingest_customers` also disabled the very cron this test depends on to discover its own uploaded file. Orchestrator error, not a code or environment defect; DAG unpaused within ~4 minutes, no lasting effect.

**Attempt 2** (DAG correctly unpaused): progressed **much further than any previous attempt this session** — discover, ingest, PENDING-reject assertion, dag_run/logical_date resolution, and the corrected-file upload all succeeded cleanly. Failed only at the retry loop plan 08-15 itself added: `_invoke_backfill_create_once` raised `AlreadyRunningBackfill: Another backfill is running for Dag csv_ingest_customers` on its 3rd/final attempt, exhausting the retry budget.

Live SQL after the failure showed the full sequence: attempt 1 created `backfill_id=3` (created 19:49:11, `exception_reason='in flight'`, `completed_at` 19:49:13 — 2s); attempt 2 created `backfill_id=4` (created 19:49:24, **also** `exception_reason='in flight'`, but `completed_at` not until 19:49:44 — 20s, much longer than attempt 1). Attempt 3 fired ~19:49:34 — while `backfill_id=4`'s `completed_at` was STILL NULL — and Airflow's own `_create_backfill` guard (`SELECT count(*) FROM backfill WHERE dag_id=X AND completed_at IS NULL`, `airflow/models/backfill.py` ~line 654) correctly rejected it: only one active backfill per DAG is allowed at a time.

**New, distinct finding:** plan 08-15's retry logic detects `backfill_dag_run.exception_reason` (written early/synchronously) but does NOT wait for the *prior* attempt's own `backfill.completed_at` before firing the next `airflow backfill create` — under contention, the gap between "exception_reason observed" and "backfill fully completed" can be 10-20s+ (not the ~2s the original debug session's clean-cluster reproduction showed), wide enough for a 5s-backoff retry to collide with `AlreadyRunningBackfill`. This is a real gap in the fix committed this session (`1de6a22`/`cb56e15`), not the batch_key/content_sha256 concern (still never reached) and not pure resource starvation (though contention widens the race window that exposes it).

**Also notable:** BOTH attempts hit `'in flight'` — the lock race recurred on every single try this run, not as a rare one-off. Worth watching whether this is a persistent property of this specific dag_id/logical_date's `dag_run` row (e.g. ongoing scheduler attention from the concurrently-running `*/1 * * * *` cron) rather than pure chance.

**Suggested next fix:** before retrying, poll the *specific* `backfill_id` just created for its own `completed_at IS NOT NULL` (not just `backfill_dag_run.exception_reason`) before firing the next `airflow backfill create` attempt — this closes the exact race observed here. Cluster left clean afterward: no active backfill (`completed_at IS NULL` count = 0), `csv_ingest_customers` unpaused, cron running normally.

## Current Focus

hypothesis: CONFIRMED (via direct source read of installed apache-airflow 3.3.0) that the batch_key/content_sha256 hypothesis is NOT the proximate cause -- the test never gets far enough to exercise it. The real proximate cause lives entirely inside Airflow's own `airflow backfill create` mechanism: `DAG.clear()` (serialization/definitions/dag.py) returns `0` and does nothing observable (no exception, no dag_run.state change, no clear_number increment) whenever its underlying `_get_task_instances(run_id=dr.run_id)` query finds zero task_instance rows for that run_id -- see Evidence below for the exact code path traced.
test: Traced the full call chain in the installed airflow package (.venv/lib/python3.12/site-packages/airflow/{models/backfill.py, serialization/definitions/dag.py, models/taskinstance.py}) matching this cluster's exact pinned version (3.3.0). Empirically verified CronDataIntervalTimetable boundary/inclusivity behavior locally with a throwaway script.
expecting: N/A -- this is a find_root_cause_only session, diagnosis only.
next_action: Return ROOT CAUSE FOUND. A future fix/verify session should run against the live cluster: (1) `SELECT * FROM backfill WHERE dag_id='csv_ingest_customers' ORDER BY id DESC LIMIT 1;` and `SELECT * FROM backfill_dag_run WHERE backfill_id=<id>;` to see whether `exception_reason` is NULL (meaning `_handle_clear_run` genuinely ran) or set (meaning the create-vs-clear branch never got reached), and (2) `SELECT count(*) FROM task_instance WHERE dag_id='csv_ingest_customers' AND run_id='<dag_run_id>';` for the ORIGINAL run, both immediately before and after invoking backfill, to confirm/refute the "zero task_instance rows found" theory directly.

## Symptoms

expected: |
  test_backfill_resolves_previously_rejected_row (tests/e2e/slice/test_backfill_reentry.py) expects: after a content-differing corrected file is uploaded and `airflow backfill create` is run against the target logical_date, the DagRun's clear_number advances (proving genuine re-execution occurred) and the previously-PENDING meta.rejected_records row for that batch flips to REDRIVEN via resolve_rejected_records_for_batch (D-05).
actual: |
  `airflow backfill create` returned exit 0, but dag_run.clear_number never advanced past its pre-backfill value of 0 within the test's 300s timeout, and dag_run.state stayed at its OLD 'success' value throughout — i.e. the backfill CLI invocation did not appear to trigger any observable re-execution of the target logical_date at all. Test never even reached the point of checking the REDRIVEN flip. This was NOT a live-cluster resource-contention issue this time — the DagRun that failed to re-execute was not blocked by other cluster traffic during this specific test run.
errors: None captured yet — the CLI returned exit 0 with no error surfaced. Root cause needs to be established via direct investigation (SQL/CLI state, Airflow backfill/clear semantics, and the batch_key/content_sha256 architecture).
reproduction: |
  Test 2 in .planning/phases/08-validation-quarantine-metadata-control-plane-completion/08-HUMAN-UAT.md (test_backfill_resolves_previously_rejected_row in tests/e2e/slice/test_backfill_reentry.py, requires live kind cluster, pytest -m cluster).
started: Discovered during live-cluster UAT of phase 08 (2026-08-17).

## Eliminated

- hypothesis: "discover_files's batch_key-is-a-function-of-content_sha256 vs resolve_rejected_records_for_batch's batch_id scoping (deferred-items.md 'From plan 08-14') is the cause of the observed test-2 failure."
  evidence: |
    Read tests/e2e/slice/test_backfill_reentry.py in full. The test never reaches the
    REDRIVEN assertion (_assert_row_resolved) at all -- it fails earlier, inside
    _run_backfill_and_wait_for_reexecution, polling dag_run.clear_number/state, which
    times out at 300s with clear_number still 0 and state still 'success'. The
    batch_key/content_sha256 concern is real and separately documented (and would
    surface AFTER a genuine re-execution happens, when comparing rejected_record's
    batch_id to the corrected file's new batch_id) but it cannot be the cause of THIS
    observed symptom, since the code path it concerns (resolve_rejected_records_for_batch)
    is never even invoked in this failure. This hypothesis remains a legitimate, separate,
    downstream concern that should be re-tested once the Airflow-level blocker below is
    fixed -- not eliminated as an architecture concern, only eliminated as the explanation
    for the CURRENT observed failure.
  timestamp: 2026-08-17T00:00:00Z

## Evidence

- timestamp: 2026-08-17T00:00:00Z
  checked: "tests/e2e/slice/test_backfill_reentry.py (full file) and tests/dagtest/test_backfill_dagrun.py (full file)"
  found: |
    test_backfill_reentry.py's module docstring claims the "reuses the SAME dag_run.run_id,
    cleared and re-executed, dag_run.clear_number incremented" behavior was "confirmed live
    against THIS cluster's own installed Airflow before being locked here" -- but re-reading
    that claim precisely, only the CLI *syntax* ("airflow backfill create ... has been
    removed, use ...") was confirmed live; the clear_number-increments-on-reprocess claim is
    sourced from reading the UNIQUE(dag_id, logical_date) constraint plus AIP-78 design intent,
    not from an observed live proof. Additionally, tests/dagtest/test_backfill_dagrun.py (08-13,
    cited as the tier that proves backfill DagRun mechanics) does NOT actually invoke `airflow
    backfill create` at all -- it calls `dag.test(logical_date=...)` twice with two DIFFERENT
    logical dates and asserts they get different dag_run.run_ids. This proves logical-date-driven
    run_id generation works, but provides ZERO coverage of the specific "clear an existing
    SUCCESS dag_run via backfill's own reprocess_behavior=completed path" mechanism that test 2
    depends on. That mechanism was therefore never actually exercised/proven anywhere in this
    codebase's test suite before being relied upon in test_backfill_reentry.py -- a real gap in
    the test pyramid, not just bad luck.
  implication: "The assumption underlying test 2's core polling mechanism (clear_number will advance) was never empirically validated pre-live-cluster. This raises the prior probability that the assumption itself has a subtle flaw."

- timestamp: 2026-08-17T00:00:00Z
  checked: "Installed apache-airflow 3.3.0 package (this repo's own .venv, matching the cluster's pinned version): airflow/models/backfill.py"
  found: |
    Traced _create_backfill -> _create_runs_non_partitioned -> _create_backfill_dag_run_non_partitioned.
    Given reprocess_behavior=ReprocessBehavior.COMPLETED and the target dag_run's state=SUCCESS:
    _get_dag_run_no_create_reason(dr, reprocess_behavior) returns None (none of its three
    branches -- IN_FLIGHT for non-terminal state, ALREADY_EXISTS for NONE, ALREADY_EXISTS for
    FAILED-and-not-failed -- match SUCCESS+COMPLETED), so the function proceeds past the
    "skip, record exception_reason" branch into the row-lock + _handle_clear_run branch.
    _handle_clear_run calls `dag.clear(run_id=dr.run_id, dag_run_state=DagRunState.QUEUED,
    session=session, dry_run=False, run_on_latest_version=run_on_latest)` then a raw SQL
    UPDATE setting backfill_id/run_type/triggered_by on the SAME dag_run row (matched by
    logical_date+dag_id). No exception is raised anywhere in this path for the SUCCESS+COMPLETED
    case -- this matches the observed exit code 0.
  implication: "Mechanistically, Airflow's backfill code SHOULD attempt to clear the target dag_run given these preconditions. The failure must be either in DAG.clear() itself finding nothing to clear, or in an earlier step (info/logical_date matching) silently diverting to a different/no-op path -- not in an exception being thrown and swallowed."

- timestamp: 2026-08-17T00:00:00Z
  checked: "Installed apache-airflow 3.3.0: airflow/serialization/definitions/dag.py DAG.clear() and _get_task_instances(), and airflow/models/taskinstance.py's clear_task_instances()"
  found: |
    DAG.clear(run_id=...) builds a TaskInstance query filtered by
    `TaskInstance.dag_id == self.dag_id` and `TaskInstance.run_id == run_id` (via
    _get_task_instances), materializes it into `tis`, and if `count := len(tis) == 0: return 0`
    -- WITHOUT calling clear_task_instances() at all. Separately, in
    airflow/models/taskinstance.py's clear_task_instances() (lines ~409-439): `dr.clear_number
    += 1` and the dag_run.state transition to the requested dag_run_state ONLY happen inside
    `if dag_run_state is not False and tis:` -- i.e. only when the caller-supplied tis list is
    non-empty. Both of these independently confirm: if DAG.clear(run_id=dr.run_id) finds ZERO
    task_instance rows for that run_id, NOTHING observable changes on the dag_run row (no state
    change, no clear_number increment) and NO exception is raised anywhere in the call chain back
    up through _create_backfill to the CLI's @cli_utils.action_cli wrapper -- the CLI process
    exits 0 exactly as if everything succeeded.
  implication: "This is the single place in the entire traced call chain where the exact combination of symptoms observed (exit 0 AND clear_number flat AND state flat AND zero errors) can occur simultaneously. This is now the primary root-cause candidate: DAG.clear()'s underlying task_instance lookup for the target run_id returned zero rows at the moment backfill ran, OR (unconfirmed, lower-probability alternative) the earlier logical_date-matching step (_get_latest_dag_run_row_query) silently diverged onto a different/new dag_run rather than finding the target row at all -- also produces no exception, also produces the same observed symptom on the ORIGINAL dag_run row (which would then be left completely untouched while a different, uninteresting dag_run got backfilled instead)."

- timestamp: 2026-08-17T00:00:00Z
  checked: "Local throwaway script against installed airflow.timetables.interval.CronDataIntervalTimetable('*/1 * * * *', UTC) via next_dagrun_info_v2 with TimeRestriction(earliest=latest=target, catchup=True)"
  found: |
    For an exact minute-aligned target datetime T, iter_dagrun_infos_between(T, T) yields exactly
    one DagRunInfo whose data_interval.start (== logical_date for this timetable) is exactly T --
    confirming the base library's documented inclusive-both-endpoints behavior holds for this
    DAG's actual schedule string. This rules out a simple off-by-one/exclusive-boundary
    explanation for why backfill's info-matching step would silently target the wrong
    logical_date, assuming the `logical_date_iso` value the test passes round-trips through
    Python's datetime.isoformat() -> Airflow CLI's date parser with no precision loss.
  implication: "The date-matching step is very unlikely to be silently off-by-one in the general case. This somewhat increases confidence that the zero-task_instance-rows theory (rather than a logical_date mismatch theory) is the more likely explanation, though this was tested with a synthetic target date, not the live cluster's exact DB value, and does not rule out a live environment-specific precision/serialization issue."

- timestamp: 2026-08-17T00:00:00Z
  checked: "airflow/dags/csv_ingest_customers.py (full file)"
  found: |
    schedule=\"*/1 * * * *\" (a plain cron string), which airflow/sdk/definitions/dag.py resolves
    to the legacy CronDataIntervalTimetable (NOT the newer CronTriggerTimetable) -- confirmed via
    grep showing the cron-string branch explicitly instantiates CronDataIntervalTimetable(interval,
    timezone). max_active_runs=1. The DAG is a normal cron-scheduled DAG with a deferrable
    S3KeySensor gate, list_matched_keys, a concurrency-capped integrity_gate.expand(), discover,
    and ingest.expand() -- nothing DAG-structurally prevents backfill/clear from working in
    principle (no partitioned timetable, no depends_on_past tasks visible in this file).
  implication: "Nothing about this specific DAG's own definition explains an inherent backfill incompatibility (e.g. non-periodic schedule, partitioned timetable) -- reinforces that the failure is in the live runtime data state (task_instance rows / dag_run row matching) rather than a structural DAG-authoring defect."

## Resolution

root_cause: |
  PROXIMATE cause (high confidence, from direct source-code tracing of the installed
  apache-airflow 3.3.0 package, this repo's exact pinned cluster version -- NOT yet
  independently confirmed against live cluster DB state, see next_action for the exact
  queries that would close that gap):

  `airflow backfill create --reprocess-behavior completed` against a dag_id/logical_date that
  already has a `success` dag_run correctly enters Airflow's clear-and-reprocess code path
  (`_create_backfill_dag_run_non_partitioned` -> `_handle_clear_run` -> `DAG.clear(run_id=dr.run_id,
  ...)`), but `DAG.clear()`'s underlying task_instance lookup
  (`_get_task_instances(run_id=dr.run_id)`) returns zero rows for that run_id, at which point
  `DAG.clear()` returns `0` and exits WITHOUT calling `clear_task_instances()` -- meaning
  `dag_run.clear_number` is never incremented and `dag_run.state` is never touched (per
  `clear_task_instances()`'s own `if dag_run_state is not False and tis:` guard). No exception
  is raised anywhere in this path, so the CLI exits 0. This reproduces every observed symptom
  simultaneously: exit code 0, clear_number flat at its pre-backfill value, state unchanged from
  'success', for the full 300s poll window.

  This is a DIFFERENT and EARLIER failure than the previously-flagged batch_key/content_sha256
  vs. resolve_rejected_records_for_batch(batch_id) architecture concern in deferred-items.md
  ("From plan 08-14") -- that concern is real but UNREACHED: the test never gets past the
  re-execution-detection step to exercise the REDRIVEN-resolution code path at all. That
  hypothesis is not confirmed OR refuted by this session; it remains a legitimate, separate,
  downstream question to re-test once this Airflow-level blocker is resolved.

  Two remaining candidate explanations for WHY the task_instance lookup returns zero rows
  (both consistent with all observed evidence; live-cluster confirmation needed to
  distinguish them -- see next_action):
    (a) The target dag_run's task_instance rows genuinely do not exist under its `run_id` at
        backfill time (e.g. some cleanup/retention mechanism, or a run_id-format mismatch
        between what was originally written and what backfill's logical_date-based lookup
        resolves to) -- less likely given the original run completed only minutes earlier in
        the SAME test, but not ruled out.
    (b) `_get_latest_dag_run_row_query`'s logical_date match silently diverges from the
        target row (e.g. an unaccounted precision/timezone difference between the DB-read
        logical_date and what backfill's date-range CLI args resolve to on THIS cluster's
        actual data, as opposed to the synthetic date tested locally) -- causing backfill to
        create/touch a DIFFERENT dag_run while leaving the original, polled-on target row
        completely untouched. Local testing with a synthetic date ruled out a *generic*
        off-by-one, but did not rule out a live-data-specific precision issue.
  Also independently noteworthy: the "clear_number will advance" assumption that test 2's
  whole mechanism depends on was never actually proven live anywhere in this codebase before
  being relied upon (tests/dagtest/test_backfill_dagrun.py, cited as covering this, in fact
  never invokes `airflow backfill create` at all -- it only proves `dag.test()` produces
  distinct run_ids for distinct logical_dates, a different and much weaker claim).
fix: ""
verification: ""
files_changed: []
