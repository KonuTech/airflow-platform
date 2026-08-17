---
status: partial
phase: 08-validation-quarantine-metadata-control-plane-completion
source: [08-VERIFICATION.md]
started: 2026-08-17T13:40:00Z
updated: 2026-08-18T02:00:00Z
---

## Current Test

[Gap 2 (backfill re-drive, VALID-08) is now RESOLVED — see the resolution note appended to its Gaps entry below. A gap-closure round (plans 08-16/08-17/08-18, `/gsd:plan-phase 8 --gaps`) implemented business-key-scoped resolution (D-23/D-24/D-25) and live-proved it twice against the real cluster: `pytest tests/e2e/slice/test_backfill_reentry.py -x -m cluster` PASSED both times (before and after a same-round code-review fix, CR-01, that closed a related false-positive-resolution path). `meta.rejected_records` now shows `REDRIVEN: 2` where it showed `REDRIVEN: 0` at this file's last update.

Gap 1 (E2E deploy / ingest starvation) remains open and is NOT a phase-8 code gap: the specific application bug it was chasing (unbounded integrity_gate fan-out) was fixed and verified in this same UAT session (quick task 260817-mvp, commit `ea5a38e`); what remains is the already-known, already-deferred `kind/cluster.yaml` node CPU/memory budget question, tracked separately in STATE.md's Blockers as an infrastructure capacity decision requiring cluster recreation — outside this phase's scope. VALID-07 itself (the requirement test 1 exercises) is independently confirmed live-proven via a separate clean run documented in `deferred-items.md`'s "From live-cluster UAT deployment" section and reconfirmed in 08-VERIFICATION.md.]

## Tests

### 1. Deploy this phase's artifacts to the live kind cluster and run the E2E slice tests
expected: Deploy this phase's artifacts to the live kind cluster (apply migrations, rebuild/redeploy images, ensure the DAG bundle picks up csv_ingest_orders, unseal Vault), then run the two E2E slice tests.
result: issue
reported: |
  Deployment succeeded after fixing four real gaps along the way (all committed): a stale kind
  DAGs hostPath mount (infra, pre-existing), a missing psycopg dependency in the Airflow image
  (commit 020d0c2), and two missing analytics_owner GRANTs closed by migrations 0018/0019
  (commits 1cdcb48/f6e7c95). With those fixed, test_orphan_order_quarantined_while_valid_rows_
  publish achieved one full clean pass server-side (discover -> ingest -> SUCCEEDED -> orphan
  quarantine verified), proving VALID-07 genuinely works end-to-end on this cluster.

  A follow-up session root-caused the original "discover intermittently registers zero rows"
  symptom precisely: csv_ingest_customers'/csv_ingest_orders' integrity_gate TaskFlow task was
  dynamically mapped with NO concurrency cap, so a file backlog could fan out to 8-19+ concurrent
  ~250m-CPU pods, exhausting kind worker nodes' tight CPU budget and starving scheduling for
  EVERY other task's pod cluster-wide (including wait_for_files, discover itself, and other
  DAGs) -- caught live via kubectl describe showing FailedScheduling: Insufficient cpu. Fixed via
  quick task 260817-mvp: integrity_gate.override(max_active_tis_per_dag=3) in both DAG files
  (commit ea5a38e). Verified live: this specific starvation chain is eliminated -- a fresh
  re-run's wait_for_files -> resolve_window -> list_matched_keys -> integrity_gate -> discover ->
  build_ingest_args all reached success cleanly for the first time this session, and zero new
  FailedScheduling events occurred for any non-integrity_gate task in the ~9min post-fix window
  (vs. 12 such events in the preceding ~1hr).

  However, re-running the full pytest suite immediately after (same session) still failed --
  now at the ingest task itself, one step further down the pipeline. Confirmed this is the SAME
  underlying structural cause (kind/cluster.yaml's node CPU/memory budget), not a regression from
  the fix or a new code defect: at the time of failure, one worker node was at 100% CPU
  allocated, the other at 95% CPU / 91% memory, and FailedScheduling events now cite BOTH
  Insufficient cpu AND Insufficient memory even for the now-capped integrity_gate pods. ingest
  (a single 500m-CPU pod, not dynamically mapped) sat queued for ~8.5min then ran but failed
  after ~2min (cause not yet determined -- resource pressure vs. an app-level error was not
  distinguished before the pod was deleted by on_finish_action), entering up_for_retry; the retry
  had not been scheduled by the session's own default 5min retry_delay + several more minutes,
  consistent with continued node saturation.

  Net: the SPECIFIC phase-8 application bug this test was chasing is fixed and verified. What
  remains is the already-known, already-deferred structural node-CPU-budget question
  (kind/cluster.yaml) -- now confirmed to affect ingest (and potentially other single-pod tasks)
  in addition to integrity_gate's fan-out, once the fan-out itself was capped and stopped masking
  it. This is an infrastructure capacity decision, not a phase-8 code gap.
severity: minor
blocked_by: other

### 2. Investigate whether a content-differing "corrected" file re-upload actually flips its predecessor's meta.rejected_records row from PENDING to REDRIVEN
expected: Either the assertion in test_backfill_resolves_previously_rejected_row holds (VALID-08's documented re-drive path is genuinely proven end-to-end), or it fails because meta.batches.batch_key is a pure function of content_sha256 while resolve_rejected_records_for_batch resolves PENDING rows strictly by batch_id.
result: issue
reported: |
  Ran to completion this time (no longer blocked by test 1's stuck DagRun -- test 2 targets
  csv_ingest_customers' own backfill re-execution, an independent DAG/run from test 1's orders
  target). Failed with a specific, now well-characterized signal: `airflow backfill create`
  returned exit 0, but dag_run.clear_number never advanced past its pre-backfill value of 0
  within the test's 300s timeout, and dag_run.state stayed at its OLD 'success' value throughout
  -- i.e. the backfill CLI invocation did not appear to trigger any genuine re-execution of the
  target logical_date at all, not merely a slow one. This is consistent with (though not yet
  conclusively proven to be) the batch_key/content_sha256 architecture question already flagged
  in deferred-items.md: if meta.batches.batch_key is a pure function of content_sha256, and
  resolve_rejected_records_for_batch resolves PENDING rows strictly by batch_id, a backfill of
  the SAME unchanged file may be getting recognized as already-fully-processed and skipped by
  Airflow itself (dag_run reuse per the UNIQUE (dag_id, logical_date) constraint) rather than
  genuinely cleared and re-run.
severity: minor
blocked_by: other

## Summary

total: 2
passed: 0
issues: 2
pending: 0
skipped: 0
blocked: 0

## Gaps

- truth: "test_orphan_order_quarantined_while_valid_rows_publish passes cleanly on a fresh pytest invocation"
  status: failed
  reason: "The originally-suspected 'discover silently fails' bug is fixed and verified (quick task 260817-mvp, commit ea5a38e). The test still fails one step later, at ingest, due to the same class of root cause (kind cluster CPU/memory budget) now unmasked rather than a new defect."
  severity: minor
  test: 1
  root_cause: "kind/cluster.yaml's node CPU/memory allocatable budget is too tight for this cluster's current baseline load (5 days of accumulated historical CSV fixture traffic + per-minute customers cron + platform components) -- confirmed live: one worker node at 100% CPU, the other at 95% CPU/91% memory, FailedScheduling citing both Insufficient cpu and Insufficient memory. This is the same structural question already flagged in STATE.md's blockers as deliberately deferred (would need cluster recreation)."
  artifacts: ["airflow/dags/csv_ingest_customers.py", "airflow/dags/csv_ingest_orders.py", ".planning/quick/260817-mvp-cap-concurrency-on-csv-ingest-customers-/"]
  missing:
    - "A decision on the kind/cluster.yaml node CPU/memory budget itself (increase allocatable resources, requires cluster recreation) -- out of scope for a quick task, needs a deliberate phase or milestone-level decision"
    - "Determine whether ingest's specific failure (not just the retry delay) was resource pressure (OOM/CPU throttle) or an app-level error -- the pod was deleted by on_finish_action before logs could be captured; a future attempt should capture logs live the way the integrity_gate investigation did"
    - "Consider whether clearing/archiving the accumulated historical fixture backlog (not a code or infra change) would relieve enough baseline load to get a clean pass without touching kind/cluster.yaml"
  debug_session: ""
- truth: "test_backfill_resolves_previously_rejected_row demonstrates a content-differing corrected file's backfill re-drive flips the original PENDING reject to REDRIVEN"
  status: failed
  confirmed: "2026-08-17T21:15:00Z -- live, definitive, not environmental. The retry-timing sub-issue below IS resolved (mechanism proven working); the underlying truth itself is now CONFIRMED FALSE as currently designed. See resolution field for full detail."
  reason: "airflow backfill create did not trigger any observable re-execution (clear_number stayed at its pre-backfill value, dag_run.state never left its old 'success') within 300s -- test never even reached the point of checking the REDRIVEN flip."
  severity: minor
  test: 2
  root_cause: "CONFIRMED via live SQL + source trace (installed apache-airflow==3.3.0 airflow/models/backfill.py): NOT the batch_key/content_sha256 architecture concern (refuted -- the test never gets far enough to reach resolve_rejected_records_for_batch at all). The actual proximate cause: backfill_dag_run.exception_reason='in flight' -- _create_backfill_dag_run_non_partitioned's SELECT ... FOR UPDATE SKIP LOCKED on the target dag_run row lost its lock race against a concurrent transaction (almost certainly the scheduler's own periodic dag_run-row locking, worsened by this cluster's already-documented CPU/scheduler contention), and Airflow's own code has NO retry for a lost skip_locked race -- a single occurrence permanently records IN_FLIGHT with exit 0 and no surfaced error. Live-proven the mechanism itself works: manually re-invoking the IDENTICAL `airflow backfill create --dag-id csv_ingest_customers --from-date 2026-08-17T14:55:00+00:00 --to-date 2026-08-17T14:55:00+00:00 --reprocess-behavior completed` a second time (no code change) succeeded immediately -- exception_reason NULL, dag_run.clear_number 0->1, state 'success'->'running', run_type 'scheduled'->'backfill'. This is a one-shot Airflow-level race, not a dataplat defect; the batch_key/content_sha256 concern remains real but untested (would only surface AFTER a genuine re-execution)."
  artifacts:
    - path: "tests/e2e/slice/test_backfill_reentry.py"
      issue: "The helper that invokes 'airflow backfill create' and polls for clear_number/state (module-level, ~line 167) makes exactly one CLI call with no retry/backoff for Airflow's own documented-transient 'in flight' exception_reason race, so a lost skip_locked race (observed live on this cluster) is indistinguishable from a genuine failure and burns the full 300s timeout before surfacing as a hard test failure."
  missing: []
  resolution: "SUPERSEDED by live re-verification, same session (2026-08-17T20:35-21:15Z). Plan 08-15 (1de6a22/cb56e15) plus two further live-driven refinements (441a51a: gate the settle loop on backfill.completed_at, not just row-appearance; 368c83c: only require completed_at on the 'in flight' path, not on a genuine success) fully resolved the retry-timing mechanism -- proven live, no timing errors, clean backfill re-execution (clear_number advanced, state reached success). After clearing a 54-object S3 backlog under raw/customers/ (blocking discover on every attempt until removed), the test finally ran end-to-end past every step -- discover, ingest, PENDING reject, real backfill, corrected row published to normalized.customers -- and failed ONLY at the final assertion (_assert_row_resolved): the original reject stays PENDING because it is scoped to batch_id=43120 while the corrected file's own run (run_id=43121) lands under a DIFFERENT batch_id. This is the batch_key/content_sha256 architecture concern (deferred-items.md, 'From plan 08-14'), now CONFIRMED live for the first time, not refuted as previously reported, not merely theoretical. discover_files's batch_key is a pure function of content_sha256, so a content-differing correction always discovers under a new batch; resolve_rejected_records_for_batch scopes strictly by batch_id, so it can never touch the old batch's PENDING row. Full narrative: .planning/debug/resolved/backfill-does-not-redrive-rejected-row.md. This needs a real design decision (Rule 4 territory per deferred-items.md: content-addressed batching vs. row-level correction), not a quick fix -- e.g. resolving PENDING rejects by business-key+dataset rather than strictly by batch_id."
  debug_session: ".planning/debug/resolved/backfill-does-not-redrive-rejected-row.md"
  resolution_update: "RESOLVED, 2026-08-18T02:00:00Z. Gap-closure round (/gsd:plan-phase 8 --gaps, plans 08-16/08-17/08-18) implemented the exact candidate fix named above: resolve_rejected_records_for_batch replaced by resolve_rejected_records_for_business_keys, scoped by (dataset_id, business_key) instead of batch_id, per locked decisions D-23/D-24/D-25 in 08-CONTEXT.md. Migration 0020 adds meta.rejected_records.business_key; extraction wired into every RejectedRecord construction site. Live-proved TWICE against the real cluster: pytest tests/e2e/slice/test_backfill_reentry.py -x -m cluster PASSED (test_backfill_resolves_previously_rejected_row, including _assert_row_resolved) both before and after a same-round code-review fix (CR-01: the original wiring resolved by what merely staged, not what the publish statement actually wrote -- a related false-positive-resolution path, fixed via RETURNING + PublishResult.published_business_keys). meta.rejected_records: PENDING 8 / REDRIVEN 2 (was PENDING 5 / REDRIVEN 0). Full detail: 08-VERIFICATION.md (status: passed), 08-REVIEW.md, 08-REVIEW-FIX.md."
