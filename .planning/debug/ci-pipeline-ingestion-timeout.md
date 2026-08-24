---
status: fixing
trigger: "CI pipeline ingestion timeout/contention: real Airflow pipeline runs (discover -> ingest -> publish) never complete within their fixed 180s test timeouts when running on GitHub Actions' single-node ephemeral CI cluster (kind/cluster-ci.yaml, ~3 allocatable CPU), even though the cluster itself comes up healthy. As a result, no test that requires a full DAG run to reach SUCCEEDED has ever been observed passing on GitHub's free-tier runners, blocking Phase 11's CICD-09 requirement from being provable end-to-end."
created: 2026-08-24
updated: 2026-08-24 (ROUND 2 continuation -- H1 CONFIRMED via direct `kubectl describe pod` OOMKilled evidence from an instrumented live run (commit 931c198, run 32743870344/job 97491592863), gathered by the human orchestrator and recorded into this file below by this continuation. Fix decided, implemented (helm/values/{ci,local}/airflow.yaml: `core.parallelism` 32->16 in both profiles, CI scheduler memory limit 1Gi->1536Mi), and offline-verified clean. Status moved investigating -> fixing; live-verification push and wait is next. The REOPENED ROUND immediately below (vault-0 Python-side wait race) remains TRUE, LIVE-VERIFIED, still awaiting an unanswered human checkpoint -- not re-litigated or blocked on here.)
---

## Current Focus
<!-- OVERWRITE on each update - always reflects NOW -->

hypothesis (REOPENED ROUND 2, sustained multi-DAG load under cluster-slice-verify -- H1 NOW
    CONFIRMED, see the CONFIRMATION block appended after falsification_test below. Does NOT
    re-litigate or supersede the vault-0 Python-side wait-race round below, which remains
    TRUE/LIVE-VERIFIED and simply awaits an as-yet-unanswered human checkpoint; this new round
    sits logically ABOVE it because it re-opens the session's own PRIMARY mandate):
  "H1 (leading candidate, NOT yet confirmed): scheduler and/or dag-processor MEMORY grows
  roughly monotonically with sustained real LocalExecutor task execution across
  cluster-slice-verify's ~60+ minute multi-DAG suite (both production DAGs poll every 1 minute
  regardless of which pytest test is currently executing, per existing Evidence; the slice suite
  additionally drives backfills, pod-kill-retries, concurrent-selects, and a rebuild-from-raw-style
  reconciliation, each spawning its own LocalExecutor task subprocesses inside the SAME scheduler
  pod cgroup), eventually re-exceeding the 1Gi ceiling fixes (2)/(3) raised it to -- i.e. a genuine
  per-task-execution GROWTH pattern, not the one-time fixed-headroom problem already fixed and
  live-confirmed for a single ~8.5min smoke run. This is the exact residual risk this debug
  session's own PRIOR ROUND blind_spots field predicted before it was ever observed (see below).
  Alternates explicitly NOT yet ruled out, per task guidance -- must be distinguished empirically,
  not assumed: H2 (real per-task-pod CPU/scheduling contention as dozens of KubernetesPodOperator
  task pods accumulate over an hour); H3 (a DB/connection-pool or API-server saturation effect --
  weakened as a LEADING candidate by Airflow 3's architecture, where task subprocesses talk to the
  API server via the Task Execution API rather than opening raw DB connections directly, per
  CLAUDE.md's own architecture notes -- but not eliminated, since the API server itself could
  become the bottleneck under sustained concurrent task load); H4 (a different resource/mechanism
  entirely)."
  confirming_evidence_so_far:
    - "New evidence from the orchestrator (independently re-verified via `gh run view 32729560271
      --json status,conclusion,createdAt,updatedAt,headSha,workflowName`): run 32729560271, job
      97442007494, 'E2E full (merge)' / 'Full local E2E suite + rebuild-from-raw capstone',
      headSha=c23d120ae4e5f9a36660c2874ef0bc04efa110ca (confirmed: the scheduler-memory+vault-0-
      bash-race fix commit, predates 0ef5ae6), conclusion=failure, created 12:53:49Z, updated
      14:14:40Z (~80min total job wall-clock, job failed immediately once cluster-slice-verify's
      pytest step itself exited nonzero -- no later steps ran)."
    - "cluster-slice-verify step itself: 13:06:00Z-14:14:39Z, 61m44s -- roughly double this exact
      same suite's own previously-recorded norm earlier in this debug session (2308.73s/38.5min and
      1938.60s/32.3min, both from the Symptoms section's own pre-fix baseline runs) -- an anomaly
      in duration alone, independent of the failure content."
    - "17 failed/21 passed/6 skipped: failure content is structurally DIFFERENT from every
      pre-fix-era failure this session already diagnosed and fixed -- it is dominated (11 of 17) by
      'meta.files has no row for dataset=... within 180s -- discovery never registered it', a
      signature that, per this test suite's own design (S3 upload -> discover DAG task senses it
      every */1 * * * * cycle -> meta.files row appears), means the discovery mechanism (dag-
      processor-parsed, scheduler-dispatched, running via LocalExecutor) stopped functioning for a
      SUSTAINED period covering most of the run, not a single transient blip -- consistent with a
      late-onset crash-loop that does not self-heal (matching a repeating OOM-kill cycle, H1's own
      predicted signature) more than with a single one-time delay."
    - "This debug session's own PRIOR ROUND blind_spots field (kept verbatim below) explicitly
      named this exact risk in advance, before any heavier-suite evidence existed: 'this fix's scope
      is limited to getting smoke-verify's single-DAG-run proof green... not a guarantee for
      chaos-verify/cluster-verify's much longer, heavier multi-DAG suites... flag as a residual risk
      for a future round if scheduler OOM recurs under those heavier suites even after this fix
      lands.' A prediction made BEFORE the fact matching an observation made AFTER the fact is
      meaningful corroboration, though not itself proof of mechanism -- still requires live
      diagnostic confirmation before concluding H1 specifically (vs H2/H3/H4)."
  falsification_test: "A time-series memory/restart-count monitor polled every 15s across a fresh
    live cluster-slice-verify run: if scheduler and/or dag-processor mem_current_bytes climbs
    roughly monotonically over elapsed run time and a restartCount increment (ultimately traceable
    to OOMKilled via `kubectl describe pod`) follows shortly after mem_current_bytes approaches the
    1Gi limit -- particularly if this repeats in a tight loop rather than happening once and
    recovering -- H1 is CONFIRMED. If memory instead stays roughly flat/bounded well under 1Gi
    throughout while restarts/failures still occur, or pids_current grows without a matching memory
    increase, H1 is REFUTED in favor of H2 (CPU-only signature) or a still-undetermined mechanism
    (H3/H4) -- do not assume H1 without this direct evidence, per this debug session's own
    established discipline (self-verification/direct kubectl evidence over inference, repeated
    throughout every round in this file)."
  test_plan: "No dedicated diagnostic step exists in e2e-full.yml (confirmed: read the file in
    full). Instrumented it with a THROWAWAY (never to be merged, will be reverted once data is
    collected) background monitor -- polls `kubectl get pod`/`kubectl exec ... cat /sys/fs/cgroup/
    memory.current,pids.current` for both `-l component=scheduler` and `-l component=dag-processor`
    every 15s, started immediately after cluster-up (both production DAGs run on a 1-minute
    schedule from that point regardless of which pytest step is executing, so growth-curve
    visibility needs to start there, not at cluster-slice-verify's own start) through the end of
    cluster-slice-verify (`if: always()` dump step, survives a step failure/timeout). This is the
    session's OWN established live-diagnostic technique (cgroup memory.current measurement) already
    used successfully once this session (continuation session 2's LOCAL cold-start measurement) and
    the established 'kubectl describe pod -l component=X' pattern already used successfully twice
    (PR #14 rounds) -- adapted here from a single end-of-run snapshot to a full time series, since a
    ~60+ minute sustained-load run needs a growth CURVE, not a point sample, to test H1 specifically."
  CONFIRMATION (ROUND 2 continuation, instrumented run 32743870344/job 97491592863, commit
    931c198 -- data gathered by the human orchestrator directly from GitHub Actions, recorded here
    verbatim per this file's own established discipline; full detail also in Evidence below):
    "H1 CONFIRMED by direct evidence, exactly per the falsification_test's own stated bar.
    cp-monitor.csv (15s-interval poll, ~62min run): scheduler restarted 7 times, peak_mem_bytes
    954281984 (~910MiB, 89% of the then-current 1Gi limit), peak_pids 48 (up from an initial ~41).
    Final `kubectl describe pod -l component=scheduler`: Last State Terminated / Reason OOMKilled /
    Exit Code 137 -- UNAMBIGUOUS cgroup memory-limit breach, not the CPU/heartbeat-probe signature
    this session's ORIGINAL root cause (1) showed (which never printed OOMKilled/Exit Code 137).
    dag-processor: 0 restarts for the entire run, peak 763MiB/1Gi -- confirms root cause (2)'s own
    fix fully holds under this heavier suite too; not re-opened. H2 (CPU-only) and H3 (DB/API-server
    saturation) are REFUTED as the PRIMARY mechanism for this specific symptom by the same
    OOMKilled/Exit-Code-137 evidence (a pure CPU-starvation or DB-saturation restart would show a
    liveness-probe-failure/BackOff reason, not OOMKilled). H4 (unnamed alternative) has no
    supporting evidence and is not pursued further."
  reasoning_checkpoint (MANDATORY, fix_and_verify Phase 0 -- written before any fix is applied,
    per this session's own established discipline):
    hypothesis: "CI's scheduler pod OOMs repeatedly under cluster-slice-verify's sustained
      LocalExecutor task load because (a) Airflow's stock `core.parallelism` default (32, never
      previously overridden in this project) makes `LocalExecutor.start()` eagerly fork 32 worker
      processes on every scheduler startup -- vastly more than this project's own DAGs ever need
      concurrently -- each independently importing the full Airflow module tree (the exact
      mechanism apache/airflow#56641 documents), and (b) the resulting repeated violent
      OOM-SIGKILLs interrupt in-flight tasks before they can complete (production tasks take
      ~13-15 min per test_backfill_2year_sweep.py's own docstring; OOM cycles recur every 5-7min
      after the first), which -- via Airflow's own `_mark_backfills_complete()`/DagRun-state
      mechanics, read directly from the installed airflow.jobs.scheduler_job_runner source this
      round -- leaves DagRuns/Backfills perpetually RUNNING/QUEUED and never completing, a livelock
      that compounds scheduling overhead across restarts (consistent with the observed
      shrinking-cycle-time pattern: 31m52s first cycle, then 5-7min repeatedly)."
    confirming_evidence:
      - "Direct `kubectl describe pod`: Reason OOMKilled / Exit Code 137, 7 restarts in ~62min
        (new_evidence block, this round) -- a genuine memory-ceiling breach, not CPU/heartbeat."
      - "Direct source read, installed apache-airflow==3.3.0 inside the live LOCAL scheduler pod
        (airflow.executors.local_executor.LocalExecutor.start): 'This creates the maximum number
        of worker processes (parallelism) at once to minimize gc freeze/unfreeze cycles' --
        confirms parallelism directly sizes an EAGER fork-at-startup pool, not an on-demand one.
        `airflow config get-value core parallelism` confirmed CI/local both still at the stock
        default 32 -- never tuned by any fix this session."
      - "Direct source read of both production DAG files: integrity_gate.override(
        max_active_tis_per_dag=3) is the single highest fan-out point either DAG has; stage/
        dbt_build/publish are each max_active_tis_per_dag=1 GLOBALLY (confirmed via
        test_backfill_2year_sweep.py's own docstring: shared across every concurrent DagRun of
        that dag_id, backfill or scheduled alike) -- this project's real worst-case simultaneous
        concurrency need is a low double digit at most, nowhere near 32."
      - "Direct source read of airflow.jobs.scheduler_job_runner._run_scheduler_loop: 'Check on
        start up, then every configured interval' -- adopt_or_reset_orphaned_tasks() DOES run
        unconditionally on every scheduler startup (task-instance-level self-heal is real), but
        _mark_backfills_complete() only clears a Backfill once NONE of its DagRuns are in
        RUNNING/QUEUED state -- a DagRun whose task keeps getting killed mid-execution (OOM cycle
        period < task completion time) never reaches that state, matching the REOPENED ROUND 2
        deep-mining Evidence's own observed AlreadyRunningBackfill cascade lasting the rest of a
        run, not a one-time delay."
      - "Fresh-process-boundary fact (container restart semantics, not inferred): a K8s container
        restart after OOMKill creates a genuinely NEW OS process with zero prior heap/CoW state --
        so a pattern that compounds ACROSS restarts (shrinking cycle time, rising post-restart
        baseline) cannot be explained by pure in-process CoW/allocator retention alone (that resets
        to near-zero every restart); the only thing that persists across a scheduler pod restart is
        the shared metadata DB's own stored state, consistent with the livelock mechanism above
        rather than a flat per-process leak."
      - "apache/airflow#56641 (already cited, prior round) explicitly documents '~1GB of total
        memory allocation across all workers' from independent per-worker imports at the stock
        parallelism=32 default -- external corroboration of the SAME mechanism, not a
        project-specific novelty."
    falsification_test: "If, after this fix, a fresh live cluster-slice-verify run still shows
      scheduler `Reason: OOMKilled` restarts (any count > 0), the parallelism-trim hypothesis is
      refuted or insufficient by itself -- would indicate either the sustained per-task-churn CoW
      growth apache/airflow#56641 separately documents (independent of pool size) is the dominant
      term, or the livelock mechanism is not the compounding driver assumed above, and a different
      fix shape (e.g. a much larger ceiling, or breaking the livelock more directly) would be
      needed."
    fix_rationale: "Addresses the root cause at its SOURCE (an oversized, never-tuned worker pool
      this workload does not need) rather than only its symptom (raising the ceiling to tolerate
      more of the same excess). The paired memory-limit raise is an honest, separately-justified
      SAFETY MARGIN for the still-open, not-fully-eliminated upstream growth pattern (per prior
      round's own research: no released stable fix exists) -- not claimed as sufficient alone,
      which is why it is not scaled arbitrarily large."
    blind_spots: "The 'peak realistic concurrency is a low double digit' estimate is a hand-count
      from DAG source + test file greps, not a live-measured peak concurrent-TI count -- could be
      wrong in either direction. `[scheduler] num_runs` was researched and explicitly NOT adopted
      this round: LocalExecutor.end() (source-confirmed) gracefully `proc.join()`s in-flight
      workers rather than killing them, which in principle avoids exactly the livelock-inducing
      violent interruption above -- but a num_runs-triggered graceful exit stops heartbeating
      before executor.end()'s blocking wait begins, and this project's own tasks can run
      ~13-15min, comfortably longer than `scheduler_health_check_threshold` (90s) -- meaning the
      liveness probe would very likely fire and kill the pod DURING the graceful wait anyway,
      largely negating the benefit; adopting it safely would need a dedicated follow-up (e.g. a
      probe exception during known-graceful shutdown, not a feature this project's probe mechanism
      currently has) and was judged out of scope for this round's time budget rather than adopted
      on unverified faith. Not live-tested in this sandbox (no live cluster reproduces CI's
      LocalExecutor topology here) -- the live push-and-wait below is the real test."
  next_action: "Fix committed (b1ef8e2) and pushed to main -- no queue this time (the prior
    instrumented run 32743870344 had already completed before this push), triggered run
    32755940740 immediately (`in_progress` at push+15s). cp-monitor.sh instrumentation (from
    commit 931c198) deliberately LEFT IN PLACE and reused for this run rather than trimmed out --
    still the right diagnostic for confirming/refuting the fix; will trim it back out in a
    follow-up once this round confirms clean, not before. Now background-polling run 32755940740
    to terminal status (`gh run view 32755940740 --json status,conclusion` every few minutes --
    NOT abandoning the wait early, per explicit task instruction; expect ~70-90+ min total: cluster
    setup + cluster-slice-verify's own ~60min). On terminal: fetch job's raw log via `gh api
    repos/KonuTech/airflow-platform/actions/jobs/<id>/logs`, extract the cp-monitor.csv block
    (search for '===== cp-monitor.csv' / '===== peak' markers, per the workflow's own `if:
    always()` dump step), check scheduler restart count and whether any `Reason: OOMKilled`
    appears in the final `kubectl describe pod` snapshot, and update this Current Focus with
    CONFIRMED/REFUTED per the reasoning_checkpoint's own falsification_test above (fix confirmed
    only if scheduler restarts drop to 0, or peak memory sits with real headroom under 1536Mi and
    restarts genuinely stop, not just move later) before declaring this round resolved."

reasoning_checkpoint (REOPENED ROUND, vault-0 Python-side wait race -- supersedes the round below,
    which remains true and is NOT re-litigated):
  hypothesis: "The vault-0 pod-restart-timeout failure recurring in main@c23d120's own post-merge
    run (test_unseal_survives_restart.py) is a RECURRENCE of the identical kubectl-wait-races-
    pod-recreation bug this session already fixed once in scripts/wait-for.sh, because commit
    c23d120's fix only reached scripts/wait-for.sh's wait_for_pod_running (bash) -- it never
    touched this test file's own independent, inline Python kubectl wait sequence, which
    duplicates the identical buggy pattern (delete named pod, immediately kubectl wait
    --for=jsonpath=...Running on that same name, no --for=create/poll pre-step)."
  confirming_evidence:
    - "Direct read of scripts/wait-for.sh lines 68-97: wait_for_pod_running DOES chain
      --for=create (lines 93-94) before the phase=Running wait -- confirmed fixed, matches
      commit c23d120's claimed fix exactly, matches the already_verified_by_session_manager note."
    - "Direct read of tests/e2e/vault/test_unseal_survives_restart.py lines 161-183: kubectl
      delete pod (161) immediately followed by kubectl wait --for=jsonpath={.status.phase}=Running
      --timeout=180s pod/vault-0 (170-178) -- NO --for=create pre-step, NO retry loop. Confirms
      the new_evidence block's claim exactly."
    - "REVISES the reopen_context's 'ONE of TWO places' framing: grep -rn '_VAULT_POD' across
      tests/ found a THIRD occurrence neither new_evidence nor already_verified_by_session_manager
      caught: tests/e2e/chaos/test_vault_unavailable.py lines 309-327, whose OWN module docstring
      explicitly states it copied test_unseal_survives_restart.py's delete+wait pattern believing
      it 'already-proven-working' -- proof the bug was already propagating by copy-paste before
      this reopened round began. Exhaustive repo-wide check (grep for '_VAULT_POD', for
      'delete'+'pod' kubectl calls, and for every remaining kubectl 'wait' call site across
      tests/e2e/) confirms these are the ONLY two Python occurrences: test_pod_kill_retry.py/
      test_pod_crash.py's own delete-pod calls use --wait=false and poll for a DIFFERENT
      Airflow-retry pod NAME via DB-state poll loops -- a structurally different, unaffected
      pattern; test_audit_log.py references vault-0 only for `kubectl exec -i ... tail`, never
      delete/wait; test_minio_unavailable.py's two 'wait' calls target a Deployment via
      --for=condition=Available after `scale`, never deleted/recreated as an object, so cannot
      hit this NotFound race at all."
    - "tests/e2e/chaos/conftest.py's own _poll_all_pods_ready (lines 74-141) independently
      documents and ALREADY fixed the IDENTICAL bug CLASS for a different call shape
      (label-selector CNPG pods, 11-09-PLAN.md Task 1, pre-dating this debug session entirely)
      -- direct in-codebase confirmation this exact kubectl-wait limitation is a real,
      previously-encountered, already-triaged mechanism in this repository, not a novel theory.
      Its own fix uses a hand-rolled `deadline = time.monotonic() + timeout` Python poll loop,
      NOT kubectl's --for=create -- the established Python-side idiom for this bug class in this
      codebase, distinct from the bash-side fix's own technique."
  falsification_test: "If a fresh live CI run that exercises test_unseal_survives_restart.py
    and/or test_vault_unavailable.py after the fix still shows 'pods \"vault-0\" not found' from
    either test's own wait step, the hypothesis is refuted (or the fix implementation itself is
    broken, e.g. the new poll loop's kubectl get invocation or interval racing incorrectly)."
  fix_rationale: "Extract ONE shared Python poll helper (poll_pod_running) into
    tests/e2e/vault/conftest.py -- the vault-owning conftest.py, matching this codebase's own
    established convention for substantial reusable poll/wait logic (poll_file_discovered/
    poll_ingestion_run/poll_run_for_file defined once in tests/e2e/slice/conftest.py, imported
    cross-directory by tests/e2e/chaos/conftest.py and by same-directory test files alike --
    confirmed via grep that even test_referential_orphan.py/test_smoke_and_idempotency.py, both
    IN tests/e2e/slice/ alongside conftest.py itself, still explicitly import these as plain
    functions, since only @pytest.fixture-decorated names are auto-injected) -- rather than
    inline-duplicating the bash --for=create fix a THIRD time. This addresses the actual root
    cause (no single source of truth for this wait logic, which is HOW the bug already spread to
    test_vault_unavailable.py once) rather than the symptom (one file's missing wait step), and
    structurally prevents a fourth future recurrence by giving the next chaos-test author an
    obvious, importable, already-correct helper instead of an inline sequence to copy-paste
    (mis)remembered."
  blind_spots: "Not yet live-verified (no live cluster in this sandbox). The poll loop's own
    correctness (kubectl get pod <name> returning non-zero/NotFound treated as 'not yet running,
    keep polling' rather than a hard failure) is reasoned from documented kubectl behavior and
    mirrors _poll_all_pods_ready's already-proven pattern, but has not been observed against a
    real StatefulSet recreation event in this round specifically -- requires a live throwaway-PR
    round before this can be considered confirmed, per this debug session's own established
    discipline. Scope deliberately limited to the vault-0 named-pod-restart race -- does NOT
    touch scheduler/dag-processor OOM fixes (out of scope per task instructions, already
    live-confirmed in a prior round, not re-litigated) or the still-in-progress e2e-full run
    mentioned in the handoff (not blocked on, per instructions)."

reasoning_checkpoint (PRIOR ROUND -- scheduler/dag-processor OOM fixes, TRUE, NOT re-litigated
    this round, kept verbatim for continuity):
  hypothesis: "With dagProcessor's memory fix LIVE-CONFIRMED (Restart Count: 0 across a full ~15min run, direct kubectl describe pod evidence), the SAME memory-starvation mechanism now applies to airflow-scheduler-0: its memory (256Mi/512Mi, never touched by any fix this session -- only its CPU was raised, round 1) was previously masked because dag-processor's own crash-loop meant no DAG ever registered, so the scheduler under LocalExecutor never got far enough to actually execute real in-process task code. Now that dag-processor stays alive and DagRuns actually trigger, the scheduler is doing REAL work for the first time and its own memory ceiling is the next binding constraint."
  confirming_evidence:
    - "Live throwaway PR #14 run (32724094868, job 97421459309), diagnostic step 13, direct `kubectl describe pod -l component=dag-processor`: Restart Count 0, continuously Running since 11:57:18 through the 12:12:10 snapshot (~15 minutes) -- unambiguous, direct confirmation the dagProcessor memory fix (512Mi/1Gi) fully eliminated its crash-loop. This closes the falsification_test from the prior round conclusively in favor of the hypothesis."
    - "Same diagnostic step, `kubectl describe pod -l component=scheduler`: Last State: Terminated / Reason: OOMKilled / Exit Code: 137, Restart Count: 2, OOM cycle Started 12:03:40 -> Finished 12:09:39 (~6 minutes alive), restarted again 12:09:50 -- a NEW finding, this exact mechanism (OOMKilled) never previously observed for scheduler in this debug session (all prior scheduler restart evidence cited 'Startup/Liveness probe failed: No alive jobs found', a heartbeat-staleness message, not an OOM kill)."
    - "helm/values/ci/airflow.yaml's scheduler.resources.requests/limits.memory was 256Mi/512Mi at the time of this run -- confirmed unchanged from before this entire debug session (only scheduler CPU was ever raised, in the very first fix, commit a73282e)."
    - "helm/values/local/airflow.yaml's scheduler.resources is 512Mi request / 1Gi limit -- identical value AND identical 2x ratio to what local already uses for dagProcessor (the exact reference point already validated as sufficient this same round)."
    - "The smoke-verify failure signature changed materially this round: 'did not reach success (last observed state: queued)' -- a DagRun that WAS created and WAS queued, unlike every prior round's DagNotFound/registration failure. A DagRun stuck in 'queued' with no scheduler running to dispatch it (mid-OOM-cycle 12:03:40-12:09:39) is exactly the expected symptom of a live scheduler-side OOM at dispatch time."
  falsification_test: "If, after raising scheduler's memory request/limit to match LOCAL's proven-stable sizing (512Mi/1Gi), a fresh live CI run's diagnostic step still shows airflow-scheduler-0 with Last State: OOMKilled, this hypothesis is refuted or the sizing is still insufficient -- would need either more memory or a genuinely different mechanism (e.g. a real per-task memory leak under LocalExecutor's in-process KubernetesPodOperator execution that scales with the number of tasks/DAGs actually run, not a fixed one-time headroom problem)."
  fix_rationale: "Applies the IDENTICAL evidence-based pattern just confirmed for dagProcessor: match LOCAL's already-proven-stable value (512Mi/1Gi) rather than an arbitrary new number, since local runs the identical codebase with zero scheduler OOM kills. Does not touch CPU (already raised round 1, and this new failure mode is OOMKilled -- a memory signature, not a CPU-starvation signature like 'No alive jobs found' was). Memory has enormous headroom under EFFECTIVE_CI_MEMORY_BUDGET regardless (this is a memory-only change, doesn't affect the CPU budget at all)."
  blind_spots: "Not yet live-verified. Also unconfirmed: whether scheduler's OOM is a ONE-TIME headroom problem (fixed by matching local's static sizing, like dagProcessor's was) or a genuine per-task-execution memory GROWTH pattern under LocalExecutor's in-process KubernetesPodOperator execution (watching/streaming logs for real task pods) that could eventually exceed even 1Gi under sustained load across a full E2E suite (not just one smoke-verify DAG) -- this fix's scope is limited to getting smoke-verify's single-DAG-run proof green, not a guarantee for chaos-verify/cluster-verify's much longer, heavier multi-DAG suites. Flag as a residual risk for a future round if scheduler OOM recurs under those heavier suites even after this fix lands."

hypothesis (PRIOR ROUND, scheduler+dag-processor OOM, CLOSED -- kept verbatim, NOT re-litigated): "CONFIRMED, both parts: (1) dag-processor's crash-loop was caused by hitting its 512Mi memory limit, fixed by raising to 512Mi/1Gi (matching LOCAL); (2) scheduler's newly-exposed OOM was caused by the same never-raised memory ceiling (256Mi/512Mi) now handling real in-process LocalExecutor task work for the first time, fixed identically. Both live-confirmed via direct kubectl evidence: Restart Count 0 for both components across a full live pipeline execution (registration -> trigger -> dispatch -> task terminal state)."
test (PRIOR ROUND): "COMPLETE. Live-verified via throwaway PR #14, run 32727920639 / job 97433300855: `kubectl get pods -o wide` shows airflow-dag-processor and airflow-scheduler-0 both `2/2 Running 0 restarts` across their full ~8.5min lifetime, spanning cluster-up through a complete DAG lifecycle to a terminal state."
expecting (PRIOR ROUND): "MET: zero restarts on both components; smoke-verify's check [2/4] no longer fails on DagNotFound or a 'queued' stall -- the DagRun now reaches a genuine terminal state ('failed', a SEPARATE downstream/functional issue explicitly out of scope for this debug session, not a timeout/crash-loop symptom)."

hypothesis (REOPENED ROUND, vault-0 Python-side wait race, CLOSED): "CONFIRMED via direct source read AND live CI evidence -- test_unseal_survives_restart.py and test_vault_unavailable.py each carried their own inline, un-fixed copy of the exact kubectl-wait-races-pod-recreation pattern already fixed once (bash-side, scripts/wait-for.sh) earlier this same session; the shared poll_pod_running fix resolves it."
test (REOPENED ROUND): "LIVE-VERIFIED. e2e-chaos.yml run 32738880729 / job 97468249410 ('Full QUAL-15 chaos suite (dedicated cluster)'), triggered by this fix's own commit 0ef5ae6 on main: pytest invocation `tests/e2e/chaos tests/e2e/vault -q -m cluster` (32 collected) reported '9 failed, 23 passed... in 583.76s'. `test_pod_restart_reseals_and_unseal_restores_service` is NOT among the 9 named failures -- by exhaustive elimination (9+23=32, zero error/skip categories) it is one of the 23 PASSED, independently corroborated by zero matching text anywhere in the 1721-line raw log for 'test_unseal_survives_restart'/'poll_pod_running'/'_POD_RESTART' (consistent with poll_pod_running's own silent-success return path, read directly from source). test_vault_unavailable.py's own vault-0 scenario (test_vault_sealed_stalls_wait_for_files_then_unseal_recovers, confirmed the only test function in that file and the one using kubectl delete pod/vault-0 + poll_pod_running) DID fail this run, but at an earlier, unrelated guard assertion (line 278, a customers-ingestion-precondition check) that runs BEFORE the vault-0 delete/poll_pod_running call at line 312/323 -- the changed code path was never reached. The identical precondition failure independently hit 4 other structurally-unrelated tests in the same run (test_database_unavailable.py, test_malformed_csv.py, test_minio_unavailable.py, test_pod_crash.py), confirming it is a shared, pre-existing, out-of-scope issue, not caused by this fix."
expecting (REOPENED ROUND): "MET for the primary target. test_pod_restart_reseals_and_unseal_restores_service PASSED where it previously failed with `pods \"vault-0\" not found` -- falsification_test answered in favor of the hypothesis. test_vault_unavailable.py's own scenario (secondary interest, explicitly non-blocking per task guidance) is INCONCLUSIVE this run (never reached the changed code path) due to a separate pre-existing issue -- not a fix failure."
next_action: "Awaiting human verification (checkpoint returned) before this debug session can be archived, per this session's own established discipline (self-verification, however strong, is not sufficient to close without a human-confirmed checkpoint). On confirmation: move file to .planning/debug/resolved/, commit, append knowledge-base entry."

## Symptoms

**Expected behavior:** Real Airflow pipeline runs (discover -> ingest -> publish, triggered directly or via a live cluster-verify/chaos-verify/smoke-verify E2E test) should complete and reach a terminal state within the tests' fixed timeouts — mostly 180s for discovery/ingestion registration and DagRun terminal-state polling, 120s for `e2e-smoke.yml`'s dedicated single-DAG trigger+poll proof. This is how the same pipeline reliably behaves against the local persistent 3-node kind cluster.

**Actual behavior:** On GitHub Actions' single-node ephemeral CI cluster (`kind/cluster-ci.yaml`, PROFILE=ci, LocalExecutor, ~3 allocatable CPU per the node's kubelet reservation math), these same operations blow through their timeouts even though the cluster itself, Vault, Postgres, MinIO all come up healthy:
- `meta.files has no row for dataset='customers' object_uri=... within 180s -- discovery never registered it`
- `airflow backfill create --dag-id csv_ingest_customers ... failed after 3 attempts (exit 1)`
- `dag_run[dag_id=..., run_id=...] did not reach a terminal state within 180s (last observed state: 'queued'/'running')`
- `smoke_kubernetes_pod not yet registered in DagModel (dag-processor still parsing) -- retrying` repeated for 5+ minutes before `e2e-smoke.yml`'s own 120s trigger window gives up with `DagNotFound`

**Error messages:** All of the above, observed verbatim across three fresh, live, merge-triggered CI runs today (2026-08-24):
- `e2e-full.yml` run 32696447486 (`cluster-verify`, combined cluster+slice+observability, commit with scheduler-kind fixes + multi_node marker but before the staggered monitoring stack): 20 failed, 20 passed, 6 skipped, 5 errors in 2308.73s.
- `e2e-full.yml` run 32699260549 (`cluster-slice-verify`, cluster+slice only, full commit including the staggered monitoring stack — never reached the new observability step because this step itself failed first): 18 failed, 20 passed, 6 skipped in 1938.60s.
- `e2e-smoke.yml` PR run 32675592471 (throwaway PR proof, ~7 hours before this session's own work today): failed at the DAG-trigger step itself — `DagNotFound: Dag id smoke_kubernetes_pod not found in DagModel` after the dag-processor never finished parsing within 120s.

**Timeline:** Pre-existing, not a regression from today's session's own fixes (scheduler resource-kind hardcoding across 3 files, the `multi_node` CI-skip marker, and the staggered CI monitoring stack — all three independently confirmed working correctly via these same live CI runs: zero scheduler-kind-related failures remain, and the 6 topology-shape tests are correctly skipped). First characterized in `.planning/phases/11-ci-cd-completion-operations/deferred-items.md`'s "Plan 11-05" section (2026-08-24, earlier in Phase 11) as "~15 failures — cascading from ingestion never completing in time... everything is slower under real CI contention than a quiet local host." As far as this project's own history shows, no test requiring a full live DAG run to reach `SUCCEEDED` has ever been observed passing on GitHub's free-tier runners — this is not "it used to work and broke," it appears to have never worked.

**Reproduction:** Push to `main` (or open a PR against `e2e-smoke.yml`'s `pull_request` trigger) — `e2e-full.yml`/`e2e-chaos.yml`/`e2e-smoke.yml` all bring up the ephemeral single-node `kind/cluster-ci.yaml` CI profile, deploy the stack, then run E2E suites that trigger real Airflow DAG runs and poll for completion. Failures reproduce consistently across every live run this session, not intermittently.

## Evidence
<!-- APPEND ONLY - never delete -->

- timestamp: 2026-08-24 (this session)
  checked: helm/values/ci/airflow.yaml, kind/cluster-ci.yaml, helm/values/ci/{cnpg-*,minio,vault,kyverno,ingress-nginx}.yaml, airflow/dags/csv_ingest_customers.py, airflow/dags/_common/kpo.py, Makefile smoke-verify/cluster-slice-verify targets, tests/e2e/slice/conftest.py polling helpers
  found: >
    CI's single-node kind cluster has ~3000m real allocatable CPU (kind/cluster-ci.yaml's own
    documented kubelet-reservation math). scheduler/dagProcessor/apiServer are each sized
    200m request / 500m limit. CI uses LocalExecutor, which per this project's own CLAUDE.md
    architecture notes runs ALL task-instance code (including every KubernetesPodOperator's
    execute()/watch/log-streaming loop) in-process inside the scheduler pod -- unlike local's
    KubernetesExecutor, where the scheduler only dispatches to separately-resourced worker pods.
    Both DAGs (csv_ingest_customers, csv_ingest_orders) run on schedule="*/1 * * * *", so real
    KPO task pods (discover 100m/500m, stage 500m/2, dbt_build 100m/500m, publish 500m/2) launch
    continuously throughout the ~30+ minute E2E suite regardless of which test is currently
    executing.
  implication: >
    The scheduler pod's CPU budget must cover both the SchedulerJob main loop AND all in-process
    task execution under LocalExecutor -- a materially heavier burden than KubernetesExecutor's
    scheduler, yet CI's scheduler CPU limit (500m) is HALF of local's already-lean 1-core limit.
    Candidate mechanism, not yet confirmed at this point: CPU starvation of the control-plane
    pods themselves (not just task-pod scheduling delay).

- timestamp: 2026-08-24 (this session)
  checked: >
    Live GitHub Actions logs for 3 real CI runs via `gh api repos/.../actions/jobs/<id>/logs`:
    job 97356158949 (e2e-full.yml run 32699260549, cluster-slice-verify step),
    job 97283007457 (e2e-smoke.yml run 32675592471, smoke-verify step + its own
    "DEBUG live scheduler/pod/node state if smoke-verify failed" diagnostic step)
  found: >
    (1) pytest failure output shows `dag_run[dag_id='smoke_kubernetes_pod', ...] did not reach a
    terminal state within 180s (last observed state: 'queued')` -- a DagRun-LEVEL stall (never
    even dispatched), and separately `airflow.exceptions.DagNotFound: Could not find Dag
    csv_ingest_customers` from a live `airflow backfill create` CLI call, on a DAG file that is
    committed to git and hostPath-mounted at cluster boot (not something requiring the 300s
    dag_dir_list_interval to first discover).
    (2) The smoke run's own DEBUG diagnostic step (kubectl get pods -o wide + get events +
    describe node), captured AFTER the probe-timeout fix (commit 99197cf/5abe533,
    livenessProbe/startupProbe.timeoutSeconds: 60) was already live in that exact run, shows:
    `airflow-dag-processor-... 2/2 Running 5 (104s ago) 8m45s` (5 restarts) and
    `airflow-scheduler-0 1/2 Running 1 (27s ago) 8m38s` (not fully Ready). Events include
    "Warning Unhealthy pod/airflow-scheduler-0 Startup probe failed: No alive jobs found." and
    "Warning Unhealthy pod/airflow-dag-processor... Liveness probe failed:" -> "Warning BackOff
    ... Back-off restarting failed container dag-processor". `describe node` shows
    "Allocated resources: cpu 2480m (82%)" of node allocatable committed to REQUESTS alone,
    before any KubernetesPodOperator task pod exists (etl namespace: "No resources found").
  implication: >
    CONFIRMS crash-looping, not merely slowness: both control-plane pods are repeatedly killed
    and restarted by their OWN health probes, even with the prior timeoutSeconds:60 mitigation
    already applied. "No alive jobs found" is `airflow jobs check`'s message for a stale DB
    heartbeat (Airflow-internal `scheduler_health_check_threshold`, default 30s) -- a DIFFERENT
    mechanism than K8s's `livenessProbe.timeoutSeconds` (which only bounds how long kubelet
    waits for the probe COMMAND itself to run). The prior fix addressed probe-command latency,
    not heartbeat staleness -- explaining why restarts are still occurring in a run that already
    includes that fix. Static platform CPU requests already consume 82% of the node's real
    allocatable capacity with zero task pods running, leaving razor-thin room for the dynamic
    ETL workload.

- timestamp: 2026-08-24 (this session)
  checked: web research -- Airflow scheduler/dag-processor health-check config semantics, and
    prior art for this exact symptom class
  found: >
    `[scheduler] scheduler_health_check_threshold` (default 30s) governs `airflow jobs check`'s
    DB-heartbeat-staleness verdict (used by the chart's startup/liveness probe command).
    `[dag_processor] dag_file_processor_timeout` (default 50s, Airflow-3-renamed section) kills
    an individual DAG file's parse subprocess if it runs long. A live, still-open upstream issue
    (apache/airflow#44652, "Standalone DAG Processor Causes DAGs to Appear and Disappear
    Frequently") describes this exact appear/disappear-under-resource-pressure symptom, and
    community mitigation combines raising both these config thresholds with giving the
    dag-processor more CPU/parsing headroom -- independently converging on the same remediation
    this session's own evidence points to.
  implication: >
    Confirms the fix must touch BOTH resource sizing (CPU) and Airflow's own internal health
    thresholds -- CPU alone would still let the DB-heartbeat staleness check fire during a
    genuine (if shorter) contention spike, and raising only the K8s probe timeoutSeconds (the
    prior fix) does not touch this mechanism at all.

- timestamp: 2026-08-24 (orchestrator, same session, after debugger checkpoint)
  checked: >
    Ran the authoritative offline gate the debugger's own sandbox could not (helm/kubeconform not
    installed there; both are installed in this environment): `uv run pytest
    tests/policy/test_manifest_resources.py -q` (12 tests, including test_ci_profile_fits_runner
    and test_inflating_a_request_past_budget_is_reported -- the exact D-12 policy gate that
    renders the REAL Helm-templated manifests for all 9 CI-profile charts and sums their CPU
    requests against EFFECTIVE_CI_CPU_BUDGET), plus `make helm-lint` (all charts, both profiles).
  found: >
    test_ci_profile_fits_runner PASSES against the debugger's edited helm/values/ci/airflow.yaml
    (scheduler 200m->400m, dagProcessor 200m->300m request) -- the real rendered-manifest CPU
    total fits within the 3.2-core policy budget, not just the debugger's own manual arithmetic
    estimate (~2.950/3.2 cores, ~0.25-core margin). All 12 tests in test_manifest_resources.py
    pass; `make helm-lint` reports 0 chart failures for both the local and ci apache-airflow/
    airflow chart renders. This is meaningfully stronger confirmation than the debugger's own
    self-verification could produce, since it exercises the actual policy gate `make check`/CI
    itself would run, not a manual estimate.
  implication: >
    The fix's resource-sizing change is confirmed safe against this project's own authoritative
    CPU-budget gate before ever reaching a live cluster. Does not by itself prove the live runtime
    behavior (crash-loop cessation) -- that remains gated on a real e2e-full.yml/e2e-smoke.yml run,
    per the debugger's own next_action.
- timestamp: 2026-08-24 (orchestrator, same session)
  checked: >
    Ran the full offline policy suite (`uv run pytest tests/policy/ -q -m "not manifests"`, 169
    tests) to catch any other regression before committing/pushing the debugger's fix.
  found: >
    3 failures, none caused by the debugger's fix (confirmed via `git stash` -- all 3 reproduce
    identically on bare main before the fix is applied): (1)
    test_dag_line_budget.py::test_csv_ingest_customers_stays_under_150_lines -- csv_ingest_
    customers.py is 185 lines vs a 152-line budget, pre-existing, unrelated to CI/Airflow
    resourcing, likely accrued from earlier phase-11 work (platform_retention wiring); (2)
    test_gates_actually_fail.py::test_the_main_gate_does_not_lint_the_bad_samples -- a meta-test
    about `make lint`'s own behavior on intentionally-bad sample fixtures, pre-existing, unrelated;
    (3) test_offline_gate_stays_offline.py::test_only_argued_targets_name_tests_e2e -- THIS ONE
    traced to THIS session's own earlier work: quick task 260824-ayw (staggered CI monitoring
    stack, merged and pushed earlier today) added two new Makefile targets
    (cluster-slice-verify, observability-verify-ci) that name tests/e2e paths but were never
    added to this test's ARGUED_TESTS_E2E_TARGETS allowlist -- a real gap that slipped through
    that quick task's own checker review (which never ran the full offline policy suite, only the
    specific tests scoped to its own plan). Fixed directly (not deferred): added both targets to
    ARGUED_TESTS_E2E_TARGETS with a written argument each, matching the file's own established
    style. Re-ran: 5/5 pass in tests/policy/test_offline_gate_stays_offline.py.
  implication: >
    (1) and (2) are genuinely out of scope for this debug session and are NOT fixed here --
    flagged for a separate follow-up, not silently absorbed into this fix's own commit. (3) is
    fixed as part of this session's own commit since it is this session's own regression, cheap,
    and unrelated to the CI-timeout root cause itself (a documentation/policy-allowlist gap, not
    a behavioral change) -- committed alongside the debugger's fix in
    tests/policy/test_offline_gate_stays_offline.py.

- timestamp: 2026-08-24 (orchestrator, after pushing the fix -- commit a73282e)
  checked: >
    Live-verification run against the actual fix: fresh e2e-full.yml (32714166524) and
    e2e-chaos.yml (32714166540) runs triggered by pushing commit a73282e to main.
  found: >
    e2e-full.yml FAILED before ever reaching Airflow -- cluster-up itself failed installing the
    airflow chart: "Error: server-side apply failed for object airflow/airflow-api-server ...
    Internal error occurred: failed calling webhook
    ivpol.mutate.kyverno.svc-fail-finegrained-require-signed-images: failed to call webhook: Post
    https://kyverno-svc.kyverno.svc:443/... context deadline exceeded". A DIFFERENT component
    (Kyverno's own admission webhook) timing out on itself, unrelated to scheduler/dag-processor
    directly, though plausibly the same underlying node-contention theme (Kyverno's webhook pod
    itself CPU-starved and slow to respond within its own 30s admission timeout) manifesting on a
    component this fix never touched. No data obtained on whether the scheduler/dag-processor fix
    itself works, since Airflow was never installed in this run.

    e2e-chaos.yml's cluster-up succeeded cleanly this time (no Kyverno timeout) and the chaos
    suite ran to completion: 11 failed, 21 passed (up from the pre-fix baseline's 10 failed/22
    passed across 3 identical prior runs today). All 11 failures are the SAME already-documented
    "no seed data on fresh cluster" / "discovery never registered it" / "airflow dags trigger
    failed" categories, EXCEPT one NEW failure never seen in any of today's 3 prior runs:
    tests/e2e/vault/test_unseal_survives_restart.py::test_pod_restart_reseals_and_unseal_restores_service
    -- "pod/vault-0 did not reach Running within 180s after being deleted" (this test's own fault
    injection deliberately deletes vault-0 and expects it to reschedule and restart within
    budget). No dedicated pod-restart-count diagnostic step exists in e2e-chaos.yml (unlike
    e2e-smoke.yml's dedicated DEBUG step), so scheduler/dag-processor's OWN restart count could
    not be directly confirmed or refuted from this log alone. One (weak, not dispositive)
    secondary signal: the one remaining DagNotFound-style failure this run
    (test_oom.py, "Dag id chaos_probe_oom_publish_customers not found in DagModel") is for a
    FRESH per-test throwaway DAG file the dag-processor has never seen before (first-parse
    latency is expected regardless of crash-looping), unlike prior runs' DagNotFound failures
    which hit ALREADY-registered, cluster-boot-mounted production DAGs (csv_ingest_customers) --
    a materially different, weaker signal than pre-fix evidence showed.
  implication: >
    AMBIGUOUS, not a clean confirm or refute of the original falsification_test. The vault-0
    restart-timeout failure is a genuinely NEW failure mode that did not appear in any of today's
    3 pre-fix runs -- consistent with (but not proven to be caused by) the fix's own
    blind_spots concern: raising scheduler(+200m)/dagProcessor(+100m) CPU REQUESTS tightens the
    node's already-thin (~0.25-core, per the offline policy gate) remaining margin for every OTHER
    pod, including vault-0's own post-delete reschedule+restart. This could mean the fix shifted
    contention from the control plane onto other components rather than net-reducing it. Needs
    further investigation before concluding either way -- specifically: (1) whether
    scheduler/dag-processor's OWN restart count actually dropped to zero this run (undetermined
    from available logs), (2) whether vault-0's restart timeout is a one-off flake or a real,
    repeatable consequence of the new CPU allocation, (3) whether the Kyverno webhook timeout in
    e2e-full.yml is unrelated infra flakiness or another symptom of the same node-wide contention
    theme extending beyond what this fix addressed.

- timestamp: 2026-08-24 (continuation session, after orchestrator handoff)
  checked: >
    Full raw job logs (not just the orchestrator's summary) for both post-fix runs
    (e2e-chaos.yml job 97391778732, e2e-full.yml job 97391777863) via `gh api .../actions/jobs/
    <id>/logs`, PLUS the same logs for all 7 pre-fix e2e-chaos.yml runs and 6 pre-fix e2e-full.yml
    runs from earlier today (`gh run list --workflow=... --limit 15`), to get a same-day pre/post
    comparison broader than the orchestrator's original 3-run sample.
  found: >
    (1) e2e-chaos.yml has NO diagnostic/debug step at all (confirmed by reading
    .github/workflows/e2e-chaos.yml in full) -- on failure it only files/updates a GitHub issue.
    No `kubectl get pods`, no restart-count capture, in any e2e-chaos.yml run, pre- or post-fix.
    Direct falsification-test evidence (scheduler/dag-processor RESTARTS count) is structurally
    unobtainable from this workflow, confirming the orchestrator's own note.
    (2) The vault-0 NotFound race (test_unseal_survives_restart.py) is NOT new: pre-fix run
    32693178072 (job 97330575621, 2026-08-24T05:33, commit e0972e91ea) shows the byte-identical
    failure text, hours before the fix existed. Read the test source directly
    (tests/e2e/vault/test_unseal_survives_restart.py): it does `kubectl delete pod vault-0` then
    immediately `kubectl wait --for=jsonpath={.status.phase}=Running --timeout=180s pod/vault-0`
    with no retry loop. `kubectl wait` on a named resource (not a label selector) fails FAST with
    "NotFound" if the object does not exist yet at call time, rather than polling for its creation
    -- a documented kubectl limitation, not a resource-starvation symptom (real starvation would
    show Pending/CrashLoopBackOff/Unschedulable, not NotFound). This is a pre-existing race in the
    test's own delete-then-wait sequencing, gated on kube-controller-manager's StatefulSet-reconcile
    latency at the instant of deletion -- orthogonal to vault-0's own CPU budget.
    (3) That same pre-fix run 32693178072 also shows a SEPARATE, unrelated pre-existing test bug:
    `test_airflow_conn_minio_default_is_absent_from_every_component` does `kubectl -n airflow get
    deployment airflow-scheduler` (NotFound) when airflow-scheduler is actually a StatefulSet (the
    same run's own `statefulset.apps/airflow-scheduler condition met` rollout-wait line confirms
    this) -- a test-code kind mismatch, always-NotFound regardless of pod health, not a
    crash-loop signal. This run's total (12 failed/20 passed) was already noisier than the
    orchestrator's cited 3-run baseline (10 failed/22 passed), meaning that baseline sample missed
    at least one pre-fix run with MORE failures than the post-fix run (11 failed/21 passed).
    (4) `test_dag_still_resolves_its_connection_and_runs` (the chaos suite's own discovery-timeout
    probe, `tests/e2e/vault/test_airflow_backend.py`) shows the IDENTICAL failure signature
    pre-fix (run 32699260628, 07:06) and post-fix (run 32714166540, 10:07): `airflow dags unpause`
    SUCCEEDS in both (no DagNotFound for csv_ingest_customers in either), and both then fail with
    `meta.files has no row for dataset='customers' ... within 180s -- discovery never registered
    it` from the S3KeySensor never poking in time. Byte-for-byte identical mechanism before and
    after the fix.
    (5) Kyverno admission-webhook timeouts in e2e-full.yml are also pre-existing: pre-fix run
    32692744455 (job for run at 05:13) shows `failed calling webhook "validate-policy.kyverno.svc"
    ... connection refused` during cluster bring-up -- a related (if not identical-text) Kyverno
    routability flake from hours before the fix, on a component this fix never touched.
  implication: >
    Corrects the orchestrator's AMBIGUOUS characterization on two of its three open questions.
    (2) vault-0 restart-timeout: NOT a consequence of the fix -- it is a pre-existing, independent
    test race (confirmed recurring pre-fix), moved to Eliminated. (3) Kyverno webhook timeout: NOT
    a new regression -- pre-existing infra flakiness in Kyverno's own webhook routability during
    cluster bring-up, unrelated to this fix's scope, out of scope for this debug session. (1)
    scheduler/dag-processor restart count: STILL UNCONFIRMED either way -- e2e-chaos.yml cannot
    produce this evidence at all (no diagnostic step exists), and finding (4) shows the chaos
    suite's own discovery-timeout test is not a useful proxy either, since it behaves identically
    pre- and post-fix regardless of dag-processor crash-loop status (DAG registration/unpause
    already worked in BOTH samples -- this specific test's mechanism, an S3KeySensor poke racing a
    fixed 180s budget under full concurrent chaos-suite load, was likely never primarily gated on
    control-plane crash-looping in the first place, unlike the smoke-suite's pre-fix evidence which
    DID show DagNotFound). The falsification_test therefore remains genuinely untested by any
    available live-run evidence -- only e2e-smoke.yml's dedicated "DEBUG live scheduler/pod/node
    state" step can produce it, and that workflow has not been re-run since the fix landed.

- timestamp: 2026-08-24 (orchestrator, after 3 throwaway-PR attempts — 2 hit unrelated cluster-up
    flakes (Kyverno webhook timeout, a vault-0 pod-not-found race in 80-vault.sh), 3rd reached
    smoke-verify cleanly)
  checked: >
    Re-added a throwaway diagnostic step to e2e-smoke.yml (mirroring the earlier session's own
    "never to be merged" pattern, this time `if: always()` rather than `if: failure()` so it
    captures state regardless of outcome) on PR #13, run 32718898648 / job 97405917287. This run
    got past cluster-up cleanly and smoke-verify actually reached [2/4] (the DAG-trigger check),
    which then failed identically to the pre-fix baseline: 24 retries over 5 minutes,
    "smoke_kubernetes_pod not yet registered in DagModel", then DagNotFound at the 120s-times-out
    boundary. The diagnostic step's live kubectl snapshot, captured immediately after, gives the
    DIRECT answer the falsification_test asked for:
      - airflow-dag-processor: 1/2 CrashLoopBackOff, 5 restarts, most recent 2m7s before the
        snapshot (pod age 8m38s) -- STATISTICALLY IDENTICAL to the pre-fix baseline (5 restarts in
        8m45s, run 32675592471). The fix's CPU/threshold changes for dag-processor had NO
        measurable effect on its own restart rate.
      - airflow-scheduler-0: 2/2 Running, only 1 restart, 81s before the snapshot (pod age 8m30s)
        -- a REAL, measurable improvement over the pre-fix baseline's "1+ restarts, 1/2 Ready (not
        fully healthy)". The scheduler-side fix appears to have genuinely helped.
      - Live events confirm the SAME root mechanism recurring post-fix: "Startup probe failed: No
        alive jobs found." (scheduler, 7m18s ago) and "Liveness probe failed:" (dag-processor,
        6m42s ago), PLUS a live "Back-off restarting failed container dag-processor" event only
        23s before the snapshot -- dag-processor was actively mid-crash-loop AT the moment of
        capture, not just historically.
      - dag-processor's own log shows a completed DAG-file-processing cycle
        (2026-08-24T11:02:14Z) whose stats table lists `smoke_kubernetes_pod.py` processed in
        0.09s with 0 errors but 0 DAGs registered -- consistent with the parse subprocess being
        killed mid-cycle (before its DagModel sync commits) by the SAME restart the events show,
        not a code-level parse failure in the DAG file itself.
      - Node CPU allocation: 2780m (92%) requests -- up from the pre-fix baseline's 2480m (82%),
        exactly matching the fix's own predicted +300m (scheduler +200m, dagProcessor +100m). The
        arithmetic was accurate, but 92% is a TIGHTER margin than before, and Limits show 7700m
        (256%) -- severe overcommit if multiple pods burst CPU simultaneously.
  found: >
    The falsification_test is now directly answered, not merely inferred: RESTARTS>0 on BOTH pods
    post-fix, and dag-processor's own restart count is statistically unchanged from the pre-fix
    baseline. The hypothesis is REFUTED for dag-processor specifically (the fix did not stop its
    crash-loop) while PARTIALLY CONFIRMED for scheduler (measurably fewer restarts, and it now
    stays 2/2 Ready). Since dag-processor is the component that must stay alive to register
    csv_ingest_customers/orders/smoke_kubernetes_pod in DagModel at all, its continued crash-loop
    fully explains why the E2E timeout failures persist unchanged after this fix.
  implication: >
    The fix was directionally correct (CPU sizing + internal health-check thresholds ARE the right
    mechanism class -- scheduler's improvement proves this) but dag-processor's own allocation
    (300m request/1200m limit, raised from 200m/500m) was insufficient, OR dag-processor has an
    additional bottleneck the scheduler does not share (e.g. its own `dag_file_processor_timeout`
    interacting with the 30s "process each file at most once every 30 seconds" cadence across the
    11 real DAG files scanned each cycle, or memory pressure, or a liveness-probe timeoutSeconds
    that's still too tight for dag-processor specifically even though it was raised for scheduler
    at the same commit). Needs further targeted investigation on dag-processor specifically before
    another live-CI round -- raising its CPU further and/or re-examining its own probe/threshold
    values is the natural next hypothesis, not a full restart of the investigation.

- timestamp: 2026-08-24 (continuation session 2 -- fresh investigation, memory hypothesis)
  checked: >
    Fetched full raw logs for job 97405917287 (PR #13) directly via `gh api .../actions/jobs/
    <id>/logs` (not the orchestrator's summary) -- specifically the `kubectl logs deploy/
    airflow-dag-processor -c dag-processor --tail=100 --previous` output, i.e. the actual
    stdout of the container instance that most recently crashed. Cross-referenced against the
    pulled Airflow Helm chart 1.22.0's own default `values.yaml` (fetched fresh via
    raw.githubusercontent.com) for dagProcessor's exact default livenessProbe fields.
  found: >
    The --previous log shows the container starting at 11:02:09.084Z, completing an entirely
    normal startup (found 11 files, dispatched 2 forked parser subprocesses for
    smoke_kubernetes_pod.py/csv_ingest_orders.py, both 0 errors), printing ONE "DAG File
    Processing Stats" table at 11:02:14.546Z, then the log STOPS -- no shutdown message, no
    exception, no traceback. dagProcessor's chart-default livenessProbe (values.yaml lines
    3052-3057) is initialDelaySeconds:10/failureThreshold:5/periodSeconds:60 -- this session's
    prior fix only overrode timeoutSeconds, not these three. 5 consecutive failures at
    periodSeconds:60 requires >=250s minimum before a kill; this captured death happened ~5-15s
    into container life, before initialDelaySeconds:10 would even fire the FIRST check.
  implication: >
    The observed death is mathematically incompatible with a liveness-probe-driven kill.
    Something else kills this container almost immediately after it begins dispatching forked
    parser subprocesses -- an abrupt, silent (SIGKILL-style) death is the signature of an OOM
    kill, not a probe failure or an application-level exception (which would log a traceback).
    dagProcessor's MEMORY request/limit (256Mi/512Mi) were never touched by the prior CPU-only
    fix -- a genuinely untested resource axis for this specific component.

- timestamp: 2026-08-24 (continuation session 2)
  checked: >
    Live cgroup measurement against the LOCAL persistent 3-node kind cluster (same image, same
    11 DAG files, currently running and healthy) via `kubectl exec ... cat /sys/fs/cgroup/
    memory.current`. First polled the steady-state (already-running) dag-processor pod every 5s
    for 40s (stable ~241MiB, no visible swing). Then deliberately cold-started it (`kubectl
    delete pod`, letting the Deployment recreate it) to mirror CI's exact cold-start scenario --
    all 11 files freshly queued at once -- and polled memory.current every 0.5s for the
    following ~40s.
  found: >
    Steady state after cold start: ~237MiB. During an actual parse-cycle burst (captured live):
    237 -> 271.8 -> 321.3 -> 349.5 -> 371.9 MiB across four consecutive 0.5s samples, then back
    to ~237MiB one sample later -- a real, measured +135MiB swing from a single observed cycle
    (true peak likely higher, unsampled between 0.5s polls). memory.events on this pod shows
    oom_kill: 0 (LOCAL has never actually OOM'd). helm/values/local/airflow.yaml's dagProcessor
    resources are request:512Mi/limit:1Gi -- exactly DOUBLE CI's 256Mi/512Mi at the time of this
    check; LOCAL's REQUEST alone (512Mi) equals CI's entire LIMIT (512Mi).
  implication: >
    CI's dagProcessor memory limit (512Mi, pre-fix) left only ~140MiB of margin above this
    directly-measured LOCAL burst peak (>=371.9MiB) -- and that measurement was taken under
    LOWER CPU contention than CI's real single-node runner. Node-level memory in the CI
    diagnostic snapshot was abundant (22%/44% of ~14GB allocatable), ruling out node-wide
    pressure and pointing specifically at dag-processor's own per-container cgroup limit as the
    binding constraint. Combined with independent web research confirming Airflow 3.x's
    dag-processor fork()-based multiprocessing is a documented OOM-prone pattern (apache/
    airflow#50708, #50097, #58509, #53662), this converges on memory (not CPU or the two
    Airflow-internal thresholds already raised) as dag-processor's actual, previously-untested
    bottleneck.

- timestamp: 2026-08-24 (continuation session 2 -- offline verification of the memory fix)
  checked: >
    Applied the fix (dagProcessor.resources.requests/limits.memory 256Mi/512Mi -> 512Mi/1Gi in
    helm/values/ci/airflow.yaml, matching LOCAL's proven-stable sizing exactly). Verified offline
    using the project's OWN authoritative gates, run directly in this session (tools/bin/helm and
    tools/bin/kubeconform ARE available here, unlike the earlier debugger sandbox): `make
    manifests` (helm-lint all 9 charts both profiles + render + kubeconform -strict), then `uv
    run pytest tests/policy/test_manifest_resources.py -q -m manifests`.
  found: >
    `make manifests`: 0 chart lint failures, kubeconform -strict reports 0 invalid/0 errors
    across 540 resources. BUT `test_ci_profile_fits_runner` FAILED: real rendered CI-profile CPU
    total was 3.400 cores against the 3.200-core EFFECTIVE_CI_CPU_BUDGET -- a genuine
    over-budget condition. Isolated via `git stash` (re-rendering with the memory fix removed):
    the SAME 3.400-core failure reproduces on bare `main` -- confirming this is a PRE-EXISTING
    regression, unrelated to and unaffected by this session's memory change (memory does not
    count toward the CPU sum at all). Per-container breakdown of the rendered manifests
    identified the drift as monitoring-stack CPU (tempo, otel-collector, grafana/
    prometheus-operator helper containers) added by the earlier same-day quick task 260824-ayw,
    whose own verification apparently never re-ran this specific CI-gated budget check
    (`.github/workflows/ci.yml`'s `check` job runs `make manifest-policy`, confirmed via direct
    grep -- this IS a real, currently-failing, CI-enforced gate on `main` right now). Notably,
    that same quick task's own Makefile comment for `cluster-slice-verify` documents that the
    monitoring stack's CPU footprint ALREADY caused a live scheduler CrashLoopBackOff once this
    same day, which is why it was staggered into a separate install-test-teardown target rather
    than left live for the whole job -- directly on-theme for this debug session.
  implication: >
    A second, genuinely separate root cause from the dagProcessor memory fix, but real,
    CI-gated, and cheap to fix -- matching this same debug session's own established precedent
    for incidentally-found regressions (the ARGUED_TESTS_E2E_TARGETS gap, fixed directly rather
    than deferred). Trimmed CPU requests on the SAFEST possible targets first: tempo (100m->10m)
    and otel-collector (100m->10m), both explicitly documented in their own file headers as
    NEVER deployed live in CI even after 260824-ayw (zero behavioral risk, purely
    lint/kubeconform-satisfying placeholders) -- then a handful of monitoring.yaml's smallest
    housekeeping/one-shot containers (grafana initChownData/downloadDashboards/sidecar sync
    10m->5m each, prometheusOperator 20m->10m, its admission-webhook patch Job 10m->5m) --
    deliberately NOT touching grafana's own serving container, prometheus's own container,
    Kyverno (a real, load-bearing admission-webhook system with its own separate, already-
    documented flakiness this debug session explicitly ruled out of scope), or any Airflow
    component. Re-rendered and re-tested after each round: final state passes
    `test_ci_profile_fits_runner` with real margin (~3.08/3.2 cores, ~120m headroom) --
    confirmed via the actual policy test, not manual arithmetic. Full offline policy suite (`uv
    run pytest tests/policy/ -q -m "not manifests"`, 159 collectible tests): 157 passed, 2
    failed -- both are the SAME pre-existing, already-documented-out-of-scope failures the
    orchestrator identified earlier this same debug session (test_dag_line_budget.py's 150-line
    budget, test_gates_actually_fail.py's lint meta-test) -- nothing new broken.
    test_values_profiles.py (D-06/D-08 divergence-axis policy): 6/6 pass -- the memory change
    made CI/local IDENTICAL on dagProcessor memory (removing a divergence, not adding one), and
    all CPU trims stay within the already-permitted "resource sizing" axis.

- timestamp: 2026-08-24 (continuation session 2 -- LIVE VERIFICATION, throwaway PR #14)
  checked: >
    Pushed commit 8681d69 (dagProcessor memory fix + CI CPU-budget trim) to `main`. Opened
    throwaway PR #14 (branch throwaway/ci-pipeline-ingestion-timeout-memory-fix-live-proof) with
    an UPGRADED diagnostic step (commit 577b8a4) adding `kubectl describe pod -l
    component=dag-processor` and `-l component=scheduler` -- closing last round's observability
    gap (only `describe node` + `logs --previous` existed before, never `describe pod`, so
    OOMKilled was inferable but not directly confirmed). Run 32724094868 / job 97421459309:
    cluster-up + Vault bootstrap succeeded cleanly (steps 1-11), `make smoke-verify` (step 12)
    ran for ~15 minutes (materially LONGER than the pre-fix baseline's fast DagNotFound failure
    at ~5min, itself a signal something progressed further this time) before finally failing at
    check [2/4]: `ERROR: smoke_kubernetes_pod run smoke-verify-<id> did not reach 'success' (last
    observed state: queued)` -- notably NOT a DagNotFound/registration failure this time (the
    trigger itself SUCCEEDED, meaning dag-processor stayed alive long enough to register the DAG
    -- a materially different, better failure signature than every prior round). The new
    diagnostic step (13) ran successfully and captured full `kubectl describe pod` output for
    both pods.
  found: >
    DAG-PROCESSOR: `Restart Count: 0`. Continuously `Running` since 11:57:18, still healthy at
    the 12:12:10 snapshot (~15 minutes, zero restarts) -- the exact opposite of every prior
    round's "5 restarts in ~9min" baseline. Confirmed resources match the fix exactly
    (Limits cpu:1200m/memory:1Gi, Requests cpu:300m/memory:512Mi). This is the DIRECT,
    unambiguous confirmation the falsification_test asked for: the dagProcessor memory fix
    COMPLETELY eliminated its crash-loop.
    SCHEDULER (NEW, unexpected finding): `Last State: Terminated / Reason: OOMKilled / Exit
    Code: 137`, `Restart Count: 2`, OOM cycle spanned Started 12:03:40 -> Finished 12:09:39 (~6
    minutes alive before OOM), pod restarted again at 12:09:50. Scheduler's memory
    (Requests 256Mi / Limits 512Mi) was NEVER touched by any fix this session (only its CPU was
    raised, round 1) -- helm/values/local/airflow.yaml's own scheduler.resources is 512Mi/1Gi,
    identical pattern and identical ratio to what local uses for dagProcessor.
  implication: >
    The dagProcessor memory hypothesis is CONFIRMED, not just inferred -- direct `kubectl
    describe pod` evidence (Restart Count: 0 across the full run) is as strong as evidence gets.
    HOWEVER, fixing dag-processor exposed a THIRD, previously-invisible bottleneck: with
    dag-processor no longer crash-looping, DAGs now actually register and DagRuns actually
    trigger -- for the first time, the scheduler is doing REAL in-process LocalExecutor task
    execution (not just idling with nothing to dispatch), and its own memory (never raised, only
    CPU was) is insufficient for that real workload, causing IT to OOM-kill now. This is why the
    DagRun got stuck in 'queued': the scheduler most likely to have been mid-OOM-cycle (12:03:40-
    12:09:39) around the time the DagRun needed dispatching. Classic "fixing one bottleneck
    reveals the next" pattern in a resource-constrained system. Applied the SAME evidence-based
    fix immediately (scheduler.resources memory 256Mi/512Mi -> 512Mi/1Gi, matching local's
    already-proven-stable scheduler sizing exactly -- same rationale, same reference point
    already used for dagProcessor). Offline-verified: `make manifests` + `test_manifest_
    resources.py -m manifests` (5/5 pass, memory has enormous headroom under the budget, this
    is a memory-only change so CPU total is unaffected) + `test_values_profiles.py` (6/6 pass) +
    full offline policy suite (157/159 pass, same 2 pre-existing out-of-scope failures, nothing
    new broken). NOT yet live-verified -- requires one more live-CI round before this debug
    session can be considered resolved.

- timestamp: 2026-08-24 (continuation session 2, round 2 attempt 1 -- NON-INFORMATIVE, pre-existing flake)
  checked: >
    Merged the scheduler memory fix into throwaway PR #14's branch, pushed, triggering run
    32726446239 / job 97428692764. Fetched the full raw job log to see why cluster-up itself
    failed this time (unlike every prior round, where cluster-up always succeeded and only
    smoke-verify failed).
  found: >
    `Error from server (NotFound): pods "vault-0" not found` -> `make: *** [Makefile:167:
    cluster-up] Error 1` -> `Process completed with exit code 2`. Cluster-up itself failed during
    Vault bring-up, BEFORE Airflow/scheduler/dag-processor are even reached -- steps 8-12
    (image config, migrations, vault bootstrap, smoke-verify) were all SKIPPED as a result. This
    is the SAME pre-existing `scripts/stages/80-vault.sh` vault-0 pod-not-found race already
    explicitly documented as "out of scope, not yet filed" in this session's own prior handoff
    notes (`.planning/phases/11-ci-cd-completion-operations/.continue-here.md`), and already hit
    "once this session" per that same document, before either memory fix existed.
  implication: >
    NON-INFORMATIVE for the scheduler-memory hypothesis -- this failure occurs entirely upstream
    of Airflow, is a known recurring infra flake (the orchestrator's own earlier notes record
    needing 3 throwaway-PR attempts to get past similar flakes once already this session), and
    is orthogonal to any resource-sizing change. Does not confirm or refute the scheduler memory
    fix either way. Retrying with a fresh push to get past this flake and reach the actual test.

- timestamp: 2026-08-24 (continuation session 2, round 2 attempt 2 -- SAME flake recurs, now fixed)
  checked: >
    Retried (empty-ish docs commit, fresh push) -- run 32727171300 / job 97430967352 hit the
    EXACT SAME `Error from server (NotFound): pods "vault-0" not found` failure again, in direct
    succession (2/2). Read `scripts/stages/80-vault.sh` (calls `wait_for_pod_running vault
    vault-0`) and its helper `wait_for_pod_running` in `scripts/wait-for.sh`: a NAMED (not
    label-selector) `kubectl wait --for=jsonpath=...=Running pod/vault-0`, called immediately
    after `helm upgrade --install vault` returns "STATUS: deployed". Confirmed
    `wait_for_pod_running` has exactly ONE production caller (`80-vault.sh`) via `grep -rn` across
    the whole repo -- a narrow, well-understood blast radius. This is the IDENTICAL race class
    already fully diagnosed earlier this same debug session for
    tests/e2e/vault/test_unseal_survives_restart.py's own raw `kubectl wait` call (see Eliminated
    below): `kubectl wait` on a named resource fails FAST with NotFound if the object does not
    exist yet, rather than polling for its creation. Verified via web research that `kubectl wait
    --for=create` (kubectl 1.23+, this project pins 1.36.1) is the kubectl-native fix -- succeeds
    immediately if the object already exists, polls for creation otherwise -- confirmed this
    works correctly for NAMED resources specifically (the documented `--for=create` limitation,
    kubernetes/kubectl#1675, applies only to label selectors, not this case).
  found: >
    Two failures in direct succession (not "hit once" as the prior handoff notes characterized
    it) indicates this race is hit often enough to meaningfully obstruct this debug session's own
    live-verification work, not a rare curiosity. Fixed `wait_for_pod_running` in
    `scripts/wait-for.sh` (the ONLY place this exact bug pattern has a single, shared, easily-
    fixed helper -- unlike the test file's own inline `kubectl wait`, which is a separate,
    already-out-of-scope fix): chained a `--for=create` wait (30s budget) before the existing
    `--for=jsonpath=...Running` wait. `bash -n` syntax-checked clean.
  implication: >
    A THIRD incidentally-discovered, pre-existing regression fixed alongside the two root-cause
    memory fixes -- same "cheap, blocking, discovered while verifying" precedent as the
    ARGUED_TESTS_E2E_TARGETS gap and the CI CPU-budget regression earlier this session. Directly
    unblocks live verification of the scheduler memory fix, which is the actual reason this fix
    was made now rather than deferred (mirroring this debug session's own established
    discipline: fix small, clearly-scoped, incidentally-found blockers; defer genuinely
    unrelated/larger ones). Retrying again with this fix in place.

- timestamp: 2026-08-24 (continuation session 2, round 2 attempt 3 -- DEFINITIVE LIVE CONFIRMATION)
  checked: >
    Retried with the vault-0 race fix in place -- run 32727920639 / job 97433300855. Cluster-up
    (step 7) SUCCEEDED this time (no vault-0 race), and progressed cleanly through steps 8-11
    (image config, migrations, Grafana webhook, Vault bootstrap). `make smoke-verify` (step 12)
    ran check [1/4] (Helm/Deployments/StatefulSets healthy) successfully, then check [2/4]
    (`smoke_kubernetes_pod` DAG run) ran from 12:43:02 to 12:48:40 (~5m38s, consistent with the
    full state-poll budget) before finally exiting. Fetched the diagnostic step's `kubectl get
    pods -o wide` output directly.
  found: >
    `airflow-dag-processor-57499d6999-cd94k   2/2   Running   0   8m27s` and
    `airflow-scheduler-0   2/2   Running   0   8m22s` -- RESTART COUNT ZERO for BOTH components,
    across their entire lifetime (cluster-up through a live DAG trigger, scheduler dispatch, and
    task execution to a terminal state). The failure signature also changed completely from
    every prior round: `ERROR: smoke_kubernetes_pod run ... did not reach 'success' (last
    observed state: failed)` -- NOT DagNotFound (dag-processor dead), NOT stuck in 'queued'
    (scheduler dead) -- the DagRun reached a genuine TERMINAL state ('failed'). dag-processor's
    own log shows the DAG file parsing cleanly (`smoke_kubernetes_pod.py ... 1 #DAGs, 0 #Errors`).
    The only other error in this run's full log is the SAME pre-existing Kyverno webhook
    connection-refused flake during cluster-up (already documented as out-of-scope infra
    flakiness) -- unrelated to control-plane resourcing and did not block this run.
  implication: >
    DEFINITIVE, direct confirmation of BOTH memory fixes: the control-plane crash-loop this
    entire debug session was opened to investigate is FULLY RESOLVED. Zero restarts on
    dag-processor AND scheduler, through a complete live pipeline execution (DAG registration ->
    trigger -> scheduler dispatch -> task execution to a terminal state) -- the falsification_test
    is conclusively answered in favor of both hypotheses. The DagRun reaching 'failed' rather than
    'success' is a NEW, genuinely SEPARATE, downstream issue: a functional/application-level
    problem in what the `smoke_kubernetes_pod` task itself does (or a KubernetesPodOperator
    task-pod-level issue in the `etl` namespace), not a timeout, not a crash-loop, not a
    resource-starvation symptom of the kind this debug session was chartered to investigate. Per
    this same session's own established discipline (separating in-scope root causes from
    incidentally-found, genuinely-unrelated issues), this is explicitly OUT OF SCOPE for this
    debug session and is NOT chased further here -- flagged as a new, distinct follow-up for a
    fresh debug session (this session's own diagnostic step did not capture `etl`-namespace task
    pod details, only airflow-namespace control-plane pods, so root-causing it would need a new,
    differently-scoped investigation).

- timestamp: 2026-08-24 (REOPENED ROUND -- fix implementation + offline verification)
  checked: >
    Implemented the REOPENED ROUND checkpoint's fix_rationale (verbatim, no changes needed after
    re-reading the cited files): extracted `poll_pod_running` as a plain function (not a fixture)
    into `tests/e2e/vault/conftest.py`, a hand-rolled `deadline = time.monotonic() + timeout` poll
    loop over `kubectl get pod <name> -o jsonpath={.status.phase}` -- mirroring
    `tests/e2e/chaos/conftest.py`'s own `_poll_all_pods_ready` idiom, but for a NAMED pod instead
    of a label selector (the key difference: a label-selector query's "zero matches" is a normal
    exit-0 result, so `_poll_all_pods_ready` treats non-zero exit as a hard query failure; a
    NAMED-resource query has NO exit-0 way to represent "does not exist yet" -- `kubectl get pod
    <name>` on a not-yet-recreated pod exits non-zero with NotFound -- so `poll_pod_running`
    deliberately treats EVERY non-zero exit as "not there yet, keep polling", surfacing the last
    error text only in the final timeout message if the deadline is ever actually reached).
    Rewired both `test_unseal_survives_restart.py` (same directory as conftest.py) and
    `tests/e2e/chaos/test_vault_unavailable.py` (cross-directory) to `from tests.e2e.vault.conftest
    import poll_pod_running` and call it in place of their duplicated bare `kubectl wait
    --for=jsonpath={.status.phase}=Running pod/vault-0` -- explicit imports, not fixture injection,
    matching the confirmed convention (`tests/e2e/slice/conftest.py`'s `poll_file_discovered` et
    al. are imported the identical way even by same-directory callers, since pytest only
    auto-injects `@pytest.fixture`-decorated names). Both files' `_POD_RESTART_TIMEOUT_SECONDS`
    changed from the CLI-duration string `"180s"` to the int `180` (the new call site needs a
    plain number, not a kubectl `--timeout=` flag value).
  found: >
    Offline verification, run directly in this sandbox (no live cluster available here): `python -m
    py_compile` clean on all 3 touched files. `ruff check` -- all checks passed, 0 issues. `ruff
    format --check --diff` -- clean on `tests/e2e/vault/conftest.py` and
    `test_unseal_survives_restart.py` (the two files with substantive rewrites); one PRE-EXISTING
    formatting diff remains in `test_vault_unavailable.py`'s `_scheduler_resource_ref` (a function
    this fix never touched) -- confirmed pre-existing, not introduced by this fix, by piping
    `git show HEAD:tests/e2e/chaos/test_vault_unavailable.py` (commit c23d120, before any of this
    round's edits) through the identical `ruff format --check --diff -` and observing the
    byte-identical diff reproduce on the unmodified file. `mypy` -- 0 errors across all 3 files
    (caught and fixed one real mistake of my own along the way: an initial edit attempt to change
    `_POD_RESTART_TIMEOUT_SECONDS` from `"180s"` to `180` in `test_vault_unavailable.py` silently
    failed a string-match against slightly different comment wording than expected -- mypy's
    `arg-type` error on the `poll_pod_running(..., timeout=_POD_RESTART_TIMEOUT_SECONDS)` call
    caught the leftover `str` constant directly, re-verified via `grep` that both files' constants
    now read `= 180` after the correction). `pytest --collect-only` on both modified test files:
    both collect cleanly (2 tests collected, 0 errors) -- confirms the new cross-module import
    (`tests.e2e.chaos.test_vault_unavailable` importing from `tests.e2e.vault.conftest`) resolves
    correctly with no circular-import or path issue. Full offline policy suite (`pytest tests/policy/
    -q -m "not manifests"`, 159 collectible): 157 passed, 2 failed -- both the SAME pre-existing,
    already-documented-out-of-scope failures from earlier in this same debug session
    (test_dag_line_budget.py's 150-line DAG budget, test_gates_actually_fail.py's lint meta-test) --
    identical count and identical failing tests as every prior offline-verification round this
    session, confirming zero new regressions. Also confirmed `tests/policy/
    test_no_manual_kubectl_surgery.py`'s `SCAN_DIRS = (scripts, tools)` does not include `tests/`,
    matching the existing module docstring's claim -- this fix's new `kubectl get` calls inside
    conftest.py raise no policy concern.
  implication: >
    The fix is implemented exactly as the REOPENED ROUND checkpoint's fix_rationale specified, with
    every offline-checkable property (syntax, lint, types, import resolution, no regressions in the
    broader policy suite) confirmed clean. This matches the checkpoint's own blind_spots note
    precisely: "not yet live-verified (no live cluster in this sandbox)... requires a live
    throwaway-PR round before this can be considered confirmed, per this debug session's own
    established discipline." Offline confirmation is complete; only the live-CI round remains
    before this REOPENED ROUND can be considered resolved.

- timestamp: 2026-08-24 (continuation session 3 -- LIVE VERIFICATION of the REOPENED ROUND fix)
  checked: >
    Waited for e2e-chaos.yml run 32738880729 (triggered by commit 0ef5ae6, pushed to main by the
    prior continuation hop) via `gh run watch 32738880729 --exit-status`, then fetched job
    97468249410's ("Full QUAL-15 chaos suite (dedicated cluster)") full raw log via `gh api
    repos/KonuTech/airflow-platform/actions/jobs/97468249410/logs` (1721 lines) once it reached a
    terminal status. Confirmed the exact pytest invocation actually run:
    `uv run --frozen --group cluster pytest tests/e2e/chaos tests/e2e/vault -q -m cluster`
    (32 tests collected, reproduced identically via a local `--collect-only` against the same
    command). Cross-referenced every named failure in the run's `short test summary info` against
    `test_pod_restart_reseals_and_unseal_restores_service` (test_unseal_survives_restart.py) and
    `test_vault_sealed_stalls_wait_for_files_then_unseal_recovers` (test_vault_unavailable.py,
    confirmed via `grep -n "^def test_"` to be the ONLY test function in that file, and directly
    reads `kubectl delete pod/vault-0` + `poll_pod_running` at lines 312/323 -- this IS the vault-0
    delete/restart scenario). Also read `poll_pod_running`'s own source
    (tests/e2e/vault/conftest.py:170-230) to confirm its success path is a silent `return` (no
    stdout) and its failure path raises `AssertionError` with `last_seen` context (would appear
    verbatim in a FAILURES block) -- so a clean pass with zero matching text is expected behavior,
    not an observability gap.
  found: >
    Run 32738880729 / job 97468249410 reached terminal status FAILURE at ~18m8s wall-clock (step
    12 started 14:28:10Z). The suite's own `short test summary info`: "9 failed, 23 passed, 29
    warnings in 583.76s (0:09:43)" -- 9 named failures + 23 passed = 32, matching the exact
    collected-test count with ZERO error/skip/xfail categories, so all 32 outcomes are fully
    accounted for. `test_pod_restart_reseals_and_unseal_restores_service` is NOT among the 9 named
    failures -- by exhaustive elimination it is one of the 23 PASSED. Independently confirmed by
    absence: `grep` across the full 1721-line log for "test_unseal_survives_restart",
    "test_pod_restart_reseals", "poll_pod_running", and "_POD_RESTART" returns ZERO matches
    anywhere -- no AssertionError, no timeout message, nothing -- consistent with `poll_pod_
    running`'s own silent-success code path and INCONSISTENT with a failure (which would print a
    `last_seen`-bearing AssertionError verbatim in the FAILURES section, as every other failing
    test's own assertion text does).
    `test_vault_sealed_stalls_wait_for_files_then_unseal_recovers` (test_vault_unavailable.py) DID
    fail, but at line 278 -- `assert len(customer_ids) == _ROW_COUNT` ("normalized.customers has
    fewer than 20 rows on this live cluster -- this test needs prior customers ingestion to have
    already happened", `assert 0 == 20`) -- an early guard assertion that runs BEFORE the
    `kubectl delete pod/vault-0` + `poll_pod_running` call at lines 312/323 is ever reached. The
    vault-0 poll_pod_running code path was NEVER EXERCISED in this test in this run. The identical
    "fewer than N rows... needs prior customers ingestion" signature independently appears in 4
    OTHER, structurally unrelated failing tests in the SAME run: test_database_unavailable.py,
    test_malformed_csv.py, test_minio_unavailable.py, test_pod_crash.py -- none of which touch
    vault-0, poll_pod_running, or any file this REOPENED ROUND's fix changed.
    Separately, the pytest-reported suite runtime itself (583.76s / 9m43s) is IN LINE WITH (not
    exceeding) the previously-recorded ~643s/10.7min baseline cited in this round's handoff -- the
    longer ~18m8s step wall-clock includes pre-pytest setup (corpus seeding etc.) not part of that
    baseline figure. No CI-CPU-contention timeout blowup observed this round.
  implication: >
    DIRECT LIVE CONFIRMATION of the REOPENED ROUND's falsification_test, in favor of the
    hypothesis: `test_pod_restart_reseals_and_unseal_restores_service` -- the specific test this
    round's fix targets, and the ONLY test in this run that fully exercises `poll_pod_running`'s
    delete-then-poll path against a real StatefulSet pod recreation -- PASSED, where it previously
    failed with `pods "vault-0" not found` (see Eliminated/pre-fix evidence above). `poll_pod_
    running` introduced no new error class: zero matching failure text anywhere in the log.
    `test_vault_unavailable.py`'s own vault-0 scenario is INCONCLUSIVE for this specific run (never
    reached the code path this fix touches) due to a separate, pre-existing, shared data-
    precondition issue affecting 5 tests total in this run (itself included) -- clearly NOT caused
    by this fix (4 of the 5 affected tests never touch vault-0/poll_pod_running/any changed file at
    all) and out of scope per task guidance ("not your concern this round unless they specifically
    involve vault-0 or the new helper" -- this one's root mechanism does not). This closes the
    REOPENED ROUND: the vault-0 Python-side wait-race fix is now LIVE-VERIFIED, joining fixes
    (1)-(4) as live-confirmed. Per this session's own established discipline, self-verification is
    complete; human confirmation is the remaining gate before archiving.

- timestamp: 2026-08-24 (REOPENED ROUND 2, deep-mining the already-fetched raw job log for
    97442007494, independent of the orchestrator's own summary)
  checked: >
    The full raw log for job 97442007494 was already present in this session's own scratchpad
    (fetched by an earlier continuation hop, `e2efull2_97442007494.log`, 2968 lines) -- read
    directly rather than re-fetched. Extracted: (1) exact step-boundary timestamps via `##[group]`
    markers (`Run make cluster-slice-verify` started 13:12:49Z, not 13:06:00Z as the orchestrator's
    own summary approximated using the job's overall start); (2) pytest's own reported duration,
    "17 failed, 21 passed, 6 skipped, 16 warnings in 3704.38s (1:01:44)"; (3) the single-line
    progress indicator pytest prints in `-q` mode (`s.....s.....s......ss...s.F.FFFFFFFFFFFFFFFF`),
    decoded position-by-position against the 6/21/17 skip/pass/fail totals; (4) full,
    non-summarized FAILURES-section text (not just the short one-liners) for
    `test_pilot_window_drains_without_cpu_starvation` and `test_full_2year_sweep_customers_and_orders`,
    including their full docstrings (written during earlier, PRE-this-debug-session Phase 9/10
    work) and complete assertion/traceback text; (5) grepped the full FAILURES section (1055-2765)
    for connection/5xx/MemoryError/OOM-adjacent keywords; (6) grepped tests/e2e/slice/*.py and
    tests/e2e/cluster/*.py for any `kubectl delete`/`-n airflow` calls that could confound restart-
    count monitoring by deleting scheduler/dag-processor pods directly (as opposed to task pods).
  found: >
    (1) pytest's stdout is FULLY BUFFERED in this CI invocation -- the entire progress line AND
    the entire FAILURES section print at the SAME timestamp (14:14:34.58Z, the moment the pytest
    process itself exits), confirming the new_evidence block's own caveat ("no output at all
    appeared... pytest's default output buffering") -- NO per-test timing is recoverable from this
    log alone; a live, independent time-series diagnostic (this round's own monitor) is the ONLY
    way to get a timeline.
    (2) Decoding the progress string against file/collection order: tests/e2e/cluster ran almost
    entirely clean (only ONE failure, `test_no_extra_schemas_exist`, already flagged
    out-of-scope), THEN tests/e2e/slice opens with one more pass, then hits a wall and produces
    16 STRAIGHT FAILURES for the rest of the suite with ZERO further passes -- a late-onset,
    non-self-healing breakage, not scattered/intermittent failures. Consistent with a genuine
    crash-loop or a persistent stuck state, not isolated flakes.
    (3) `test_full_2year_sweep_customers_and_orders`'s full traceback reveals its OWN `airflow
    backfill create` CLI invocation (run via `kubectl exec deploy/airflow-api-server ... airflow
    backfill create ...`, NOT executed directly from the test runner) failed all 3 retry attempts
    with `airflow.models.backfill.AlreadyRunningBackfill: Another backfill is running for Dag
    csv_ingest_customers. There can be only one running backfill per Dag.` -- this is a CASCADE:
    the PRIOR test in file order, `test_pilot_window_drains_without_cpu_starvation`, itself
    successfully CREATED a backfill (its own failure is NOT a CLI failure) whose DagRun(s) then
    never reached a terminal state, so Airflow's own backfill-uniqueness constraint blocks every
    subsequent backfill-CLI test for the rest of the run with this identical exception (explains 3
    of the 17 failures structurally, not independently).
    (4) `test_pilot_window_drains_without_cpu_starvation`'s own failure detail is sharper than the
    orchestrator's summary conveyed: `missing entirely: ['customers_20240101.csv'], still
    non-terminal: {}` -- the SECOND field is EMPTY. This means the file was NEVER discovered AT
    ALL (no `meta.files` row ever appeared) within the full 1800s (30min) budget -- not "discovered
    but stuck mid-pipeline." Per this same test's own pre-existing docstring (written during
    Phase 9/10, BEFORE this debug session existed): "this cluster showed CPU starvation at
    `max_active_runs=3` in every observed run across Phase 9/10 sessions" (already-known,
    already-mitigated by hardcoding `max_active_runs=1`) and "`integrity_gate` (3 concurrent) +
    `stage`... together already take ~13-15 min BEFORE `dbt_build`/`publish` even start for this
    ONE file's own DagRun" -- meaning under NORMAL (even CPU-pressured) conditions, a `meta.files`
    row should appear well within 1800s. A 30-minute total absence of even the FIRST pipeline
    stage's own DB write is a materially stronger signal than "slow under contention" -- consistent
    with either (a) the scheduler being unable to dispatch this DagRun's tasks AT ALL for a
    sustained period (crash-loop preventing dispatch, H1/H2's shared prediction), or (b) discovery
    itself silently failing to enqueue -- both distinguishable only by the live restart-count/memory
    data this round's diagnostic is designed to capture.
    (5) Zero connection-refused/5xx/MemoryError/OOM-keyword hits anywhere in the FAILURES section's
    actual assertion/traceback text (the two "killed" hits are test docstring prose about the
    test's OWN fault-injection semantics, not real error output) -- the test-runner's OWN direct
    psycopg connections to both Postgres clusters stay healthy and queryable throughout (tests can
    still run SQL, they just find zero/stale rows) -- weakens (does not eliminate) H3
    (DB/API-server saturation) as the PRIMARY mechanism, since a saturated DB would more likely
    surface as connection-level exceptions in the test's own direct queries too.
    (6) Confirmed via grep: no test in tests/e2e/slice or tests/e2e/cluster ever deletes or
    otherwise directly targets a pod in the `airflow` namespace -- `test_pod_kill_retry.py`'s two
    `kubectl delete pod` calls are both scoped `-n etl` (task/worker pods only). This round's own
    restart-count monitor for `-l component=scheduler`/`-l component=dag-processor` cannot be
    confounded by test-induced deletion -- any restart count increase it observes is organic
    (health-probe or OOM driven), not test interference.
  implication: >
    Sharpens (does not yet confirm) H1: the failure pattern's specific shape -- early clean run,
    late onset, ZERO recovery for the remainder, "missing entirely" rather than "stuck partway,"
    and a structural cascade (AlreadyRunningBackfill) stacked on top of what looks like an
    independent, more fundamental dispatch failure (the non-backfill "discovery never registered
    it" failures in test_concurrent_select/test_dbt_silver_pipeline/test_pod_kill_retry(x3)/
    test_rebuild_from_raw/test_idempotent_reupload, which use the REGULAR 1-minute-scheduled DAG,
    not the stuck backfill, and STILL never got a `meta.files` row) -- is consistent with H1
    (sustained memory growth eventually causing a persistent OOM crash-loop that does not
    self-heal because the SAME growth-driving load keeps running after each restart) and
    materially less consistent with a purely transient CPU-contention slowdown (which would more
    plausibly show intermittent/partial recovery, not a hard 16-for-16 wall). Still requires the
    live growth-curve data to confirm the MECHANISM specifically (memory vs. some other resource)
    -- this evidence narrows the shape of the failure, not yet its cause.

- timestamp: 2026-08-24 (REOPENED ROUND 2, external research)
  checked: web research -- Airflow 3.x LocalExecutor scheduler memory-growth behavior, since this
    round's leading hypothesis (H1) needed to be checked against known upstream issue classes
    before assuming it is novel (research_vs_reasoning discipline: check for a recognized
    mechanism before re-deriving one from scratch).
  found: >
    A currently OPEN, actively-discussed upstream issue directly on point:
    apache/airflow#56641 ("Root Cause Investigation: Memory Growth in LocalExecutor Workers
    (Scheduler Subprocesses)") plus companion discussion #58143 ("Preventing COW in LocalExecutor
    Workers"). Documented mechanism: Airflow 3.x's LocalExecutor forks a new worker subprocess (a
    LocalTaskJob) per dispatched task instance; Copy-on-Write means each fork initially shares
    pages with the parent (scheduler) process, but as BOTH the parent and the growing set of
    forked children touch memory over time, CoW causes page duplication that accumulates --
    reported as worker processes growing from ~20-30MB to >100MB over 1-2 hours, and scheduler-side
    growth on the order of ~4.5MB/hour in some deployments, escalating faster under high task
    churn. A proposed workaround (eager vs. lazy worker forking) was tested in the upstream
    discussion and reduced growth, but is NOT a released, stable, upstream fix as of this
    research -- still in active discussion. This project already independently discovered and
    fixed a RELATED but distinct fork()-based memory issue this session (dag-processor's own
    parser-subprocess OOM, a one-time startup burst, see root_cause (2) above, itself
    corroborated by a DIFFERENT set of upstream issues: apache/airflow#50708/#50097/#58509/#53662)
    -- #56641 describes the SUSTAINED, TIME-ACCUMULATING variant of the same general "forking
    under LocalExecutor is memory-expensive in Airflow 3.x" issue class, specific to the
    SCHEDULER's own LocalTaskJob worker forks rather than the dag-processor's DAG-file-parser
    forks. The reported 1-2 hour timescale for visible growth is compatible with (same order of
    magnitude as, though not identical to) this round's own observed ~60min window, though this
    project's workload (KubernetesPodOperator watch/log-streaming loops held open for the full
    duration of each real ETL task, not lightweight tasks) plausibly produces a DIFFERENT growth
    rate than the reports found -- not assumed identical, only directionally relevant.
  implication: >
    Provides independent, external corroboration that H1 (LocalExecutor-driven scheduler memory
    growth under sustained load) is a REAL, currently-unresolved, currently-undocumented-as-fixed
    upstream issue class -- not a hypothesis invented from scratch, and consistent with this exact
    project's OWN already-confirmed adjacent finding (dag-processor's fork()-based OOM, root_cause
    (2)). Also means: IF H1 is confirmed by this round's live data, there is likely NO clean
    upstream config toggle or version bump that resolves it outright (the upstream fix is still
    in design/discussion, not released) -- any fix this round proposes would need to be a
    workaround (e.g., further memory headroom with an explicit "this is a known upstream growth
    pattern, not a fixed one-time budget" justification, a periodic/scheduled restart mechanism,
    or reducing sustained task churn) rather than a clean root-cause elimination -- to be decided
    ONLY after live confirmation, not preemptively.

- timestamp: 2026-08-24 (REOPENED ROUND 2, reproducibility check -- run 32738880691, commit
    0ef5ae6, job 97468249331, NO diagnostic instrumentation, fetched via the pre-existing
    background watcher's own log-fetch step once the run reached terminal status)
  checked: >
    Full raw job log for the SAME `cluster-slice-verify` step, on a DIFFERENT commit (0ef5ae6,
    the vault-0 Python-fix commit -- touches only test files under tests/e2e/vault and
    tests/e2e/chaos, never scheduler/dagProcessor resourcing), run hours after the original
    32729560271/97442007494 evidence. Compared short test summary info line-for-line against the
    original.
  found: >
    "17 failed, 21 passed, 6 skipped, 16 warnings in 3716.50s (1:01:56)" -- the EXACT SAME
    failed/passed/skipped COUNTS as run 32729560271/97442007494 ("17 failed, 21 passed, 6 skipped,
    16 warnings in 3704.38s (1:01:44)"), and duration within 12 seconds across two fully
    independent runs, hours apart. Diffing the full list of failing test names between the two
    runs: IDENTICAL SET, same 17 tests, same error signatures (`test_pilot_window_drains_
    without_cpu_starvation` again shows `still non-terminal: {}` -- the file was never discovered
    at all, not stuck mid-pipeline; the same 3 backfill tests again fail with the identical
    `AlreadyRunningBackfill` cascade; the same 8 tests again fail with `discovery never registered
    it` for a freshly-uploaded file under the REGULAR 1-minute-scheduled DAG, unrelated to any
    backfill).
  implication: >
    This is now a THIRD independent occurrence of an essentially byte-for-byte identical failure
    pattern (the original orchestrator-reported run, this reproducibility check, structurally
    consistent with the earlier live-mining analysis above) -- satisfies the debugging discipline's
    "reproduce reliably" bar emphatically, and the closeness of both the test SET and the DURATION
    across independent runs is itself informative: it is much more consistent with a deterministic
    resource-ceiling being hit at a roughly consistent point in a roughly-consistent workload
    execution (e.g., a growth curve that reaches a fixed limit at a repeatable elapsed time/task-
    count) than with random flakiness or a timing coincidence. Does not yet distinguish WHICH
    resource (H1 memory vs H2 CPU vs other) -- that is what the instrumented run's live time-series
    data (in flight) is for -- but strongly rules out "this was a one-off fluke" as an explanation
    for the original new_evidence.

- timestamp: 2026-08-24 (ROUND 2 continuation -- instrumented live run results, gathered by the
    human orchestrator directly from GitHub Actions, recorded here verbatim per this file's own
    established discipline for evidence provided by another party)
  checked: >
    GitHub Actions run 32743870344 / job 97491592863, commit 931c198e604ff903256b9dbfcdcb7f6a3103c849
    (the throwaway diagnostic-instrumented e2e-full.yml queued by the prior continuation's own
    next_action) -- pytest result, cp-monitor.csv (15s-interval poll of cgroup
    memory.current/pids.current + kubectl restart count from cluster-up ~15:43:59Z through end of
    cluster-slice-verify ~16:46:18Z, ~62min), and a final `kubectl describe pod -l
    component=scheduler` snapshot at run end.
  found: >
    Pytest: "17 failed, 21 passed, 6 skipped" in 3713.82s (1:01:53) -- essentially identical
    failure count/duration/signature to the two prior unstrumented runs on this same commit
    lineage (32729560271, 32738880691), confirming full determinism, not flakiness (a FOURTH
    independent occurrence of the same pattern, extending the reproducibility-check evidence
    already recorded above).
    dag-processor: 0 restarts for the ENTIRE run, peak_mem_bytes=800030720 (~763MiB), peak_pids=7
    -- confirms root cause (2)'s fix fully holds under this heavier suite; not re-opened.
    scheduler: 7 restarts over ~62min. peak_mem_bytes=954281984 (~910MiB, 89% of the then-current
    1Gi limit), peak_pids=48 (up from an initial baseline ~41). Restart timeline (timestamp,
    cumulative restart count, post-restart-baseline memory AT that 15s-poll sample -- NOT the true
    pre-kill peak, which happened between samples):
      15:45:41Z restarts=1 mem=~159MiB   [+~90s after cluster-slice-verify started]
      16:17:33Z restarts=2 mem=~404MiB   [+31m52s after restart 1]
      16:22:54Z restarts=3 mem=~59MiB    [+5m21s after restart 2]
      16:28:47Z restarts=4 mem=~160MiB   [+5m53s after restart 3]
      16:34:56Z restarts=5 mem=~173MiB   [+6m9s after restart 4]
      16:41:38Z restarts=6 mem=~252MiB   [+6m42s after restart 5]
      16:45:00Z restarts=7 mem=~312MiB   [+3m22s after restart 6]
    Final `kubectl describe pod -l component=scheduler` on the 7th-restart container instance:
    `Last State: Terminated / Reason: OOMKilled / Exit Code: 137`, `Started: 16:44:52Z / Finished:
    16:45:17Z` (25s alive before being killed), `Restart Count: 7`, `Limits: cpu 1500m / memory
    1Gi`, `Requests: cpu 400m / memory 512Mi`, pod-level `Reason: CrashLoopBackOff`.
  implication: >
    UNAMBIGUOUS direct confirmation of a genuine cgroup memory-limit breach (Reason: OOMKilled,
    Exit Code 137), not the CPU/heartbeat-probe signature this session's ORIGINAL root cause (1)
    showed ("No alive jobs found", never OOMKilled/Exit Code 137) -- H2 (CPU-only) and H3
    (DB/API-server saturation) are refuted as the PRIMARY mechanism for THIS symptom by this same
    evidence (a pure CPU-starvation or DB-saturation restart would not print OOMKilled/137). The
    restart-interval pattern itself is notable and NOT flat: restart 1->2 is +31m52s (a long, slow
    first climb), but every restart from 2 onward is +5-7min (5m21s/5m53s/6m9s/6m42s/3m22s) --
    roughly 5-6x faster per cycle than the first, with the post-restart baseline memory reading
    also trending upward across later restarts (noisy single-sample snapshots, not a clean
    monotonic proof, but a real directional trend). This pattern-shape question (compounding vs.
    flat-rate) is investigated directly below rather than assumed either way.

- timestamp: 2026-08-24 (ROUND 2 continuation -- direct source-level investigation of the
    growth/compounding mechanism, against the ACTUAL deployed apache-airflow==3.3.0 installed
    inside the live LOCAL cluster's own scheduler pod, not a generic/version-agnostic reading)
  checked: >
    `kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- python -c "..."` against the
    live LOCAL cluster (available in this sandbox) to read installed-package source directly:
    airflow.executors.local_executor.LocalExecutor.start()/.end(), airflow.jobs.job.run_job()/
    execute_job(), airflow.jobs.scheduler_job_runner._execute()/_run_scheduler_loop()/
    adopt_or_reset_orphaned_tasks()/_mark_backfills_complete(), and airflow's own config.yml
    template for `core.parallelism`/`scheduler.num_runs`/`scheduler.only_idle`/
    `scheduler.orphaned_tasks_check_interval` defaults and descriptions. Cross-referenced against
    this project's own airflow/dags/csv_ingest_{customers,orders}.py source and
    tests/e2e/slice/test_backfill_2year_sweep.py's own docstrings for real concurrency/runtime
    shape. Independently corroborated via WebSearch against apache/airflow#56641 and #1389.
  found: >
    (1) `LocalExecutor.start()`'s own source comment: "This creates the maximum number of worker
    processes (parallelism) at once to minimize gc freeze/unfreeze cycles when using fork in
    multiprocessing" -- `core.parallelism` is not merely a scheduling throttle for LocalExecutor,
    it directly sizes an EAGERLY-forked worker pool created on every single scheduler startup.
    `airflow config get-value core parallelism` inside the live pod confirmed this project has
    NEVER overridden it in either helm/values/ci or helm/values/local/airflow.yaml -- both were
    still at Airflow's stock default of 32.
    (2) Direct read of airflow/dags/csv_ingest_customers.py and csv_ingest_orders.py: both DAGs'
    single highest fan-out point is `integrity_gate.override(max_active_tis_per_dag=3)`
    (dynamic-mapped over matched_keys); `stage`/`dbt_build`/`publish` are each
    `max_active_tis_per_dag=1` -- and per test_backfill_2year_sweep.py's own docstring, this cap is
    GLOBAL (shared across every concurrent DagRun of that dag_id, live-scheduled or backfill
    alike), not per-DagRun. Cross-checked tests/e2e/slice/test_concurrent_select.py (its
    "concurrent" activity is a test-side psycopg thread, not an Airflow task) and
    test_pod_kill_retry.py (explicitly notes the same max_active_runs=1 + max_active_tis_per_dag=1
    caps) for anything that could push real concurrency higher -- found nothing. This project's
    own real worst-case simultaneous concurrency need across both production DAGs plus the
    slice-suite's own test scenarios is a low double digit at most, never remotely close to 32.
    (3) `LocalExecutor.end()`'s own source: "Shutting down LocalExecutor; waiting for running tasks
    to finish. Signal again if you don't want to wait" -- then `proc.join()` (no timeout) on every
    live worker. `airflow.jobs.scheduler_job_runner._execute()`'s own `finally` block calls
    `executor.end()` for every executor on ANY clean exit from `_run_scheduler_loop()` (including
    a `[scheduler] num_runs`-triggered `break`), and `airflow.jobs.job.execute_job()` sets
    `job.state = JobState.SUCCESS` on a clean return -- a graceful, blocking, in-flight-task-
    preserving shutdown, categorically different from a cgroup OOM SIGKILL (which kills the entire
    pod cgroup -- scheduler process AND every forked LocalExecutor worker -- with zero draining,
    zero DB bookkeeping, mid-task, unconditionally).
    (4) BUT: during that graceful `executor.end()` wait, the scheduler has already exited its main
    loop and stopped heartbeating (the `perform_heartbeat()` call lives INSIDE the loop that has
    already `break`-ed out) -- so a long `proc.join()` wait (this project's own `stage`/`dbt_build`
    tasks take ~13-15min per test_backfill_2year_sweep.py's own docstring: "integrity_gate (3
    concurrent) + stage... together already take ~13-15 min BEFORE dbt_build/publish even start")
    would very plausibly exceed `scheduler_health_check_threshold` (currently 90s, this session's
    own earlier fix) and trigger the K8s liveness probe to kill the pod DURING the graceful wait
    anyway -- undermining the very benefit a `num_runs`-triggered recycle would otherwise offer.
    (5) `airflow.jobs.scheduler_job_runner._run_scheduler_loop()`'s own source, verbatim comment:
    "Check on start up, then every configured interval" immediately precedes an unconditional call
    to `self.adopt_or_reset_orphaned_tasks()` BEFORE the main loop begins -- confirmed this runs on
    EVERY scheduler startup (including after an OOM-kill restart), not merely periodically. This
    self-heals orphaned TASK INSTANCES (resets them to a schedulable state). BUT
    `_mark_backfills_complete()` (a separate method) only marks a `Backfill` row complete once
    `~exists(... DagRun.state.in_((RUNNING, QUEUED)) ...)` for that backfill -- i.e. a DagRun whose
    task keeps getting killed mid-execution and re-queued (because the OOM-cycle period, 5-7min
    after the first cycle, is SHORTER than the ~13-15min a real task needs to finish) never leaves
    RUNNING/QUEUED, so the backfill never completes, regardless of how well orphan-reset itself
    works. This directly explains the REOPENED ROUND 2 deep-mining Evidence's own
    `AlreadyRunningBackfill` cascade "blocking all subsequent backfill-CLI tests for that dag_id"
    for the rest of a run, not just a slow patch -- a livelock, not (necessarily) an ever-growing
    literal COUNT of stuck DagRuns (bounded by `max_active_runs=1`'s own throttle on new DagRun
    creation for that dag_id), but a persistent failure-to-complete that adds real, compounding
    per-loop scheduling/retry/callback overhead across restarts.
    (6) Fresh-process-boundary reasoning (a logical deduction from basic container-restart
    semantics, not itself directly observed this round): a Kubernetes container that restarts
    after an OOMKill is a genuinely NEW OS process -- no prior heap, no prior CoW-duplicated pages
    carry over. This means a pattern that COMPOUNDS across MULTIPLE restarts (shrinking cycle time,
    rising post-restart baseline -- see the new_evidence entry immediately above) cannot be fully
    explained by pure in-process CoW/allocator retention alone (which resets to near-zero on every
    fresh process); the only state that legitimately persists across a scheduler pod restart is the
    shared Postgres metadata DB's own stored rows -- consistent with (5)'s livelock mechanism as
    the compounding driver, not a flat fixed-rate leak that a bigger ceiling alone would cleanly
    absorb.
    (7) WebSearch independently surfaced apache/airflow#1389 ("Scheduler can't restart until
    long-running local executor(s) finish"), corroborating (3)/(4) from an entirely separate
    upstream report, not just this session's own source read. apache/airflow#56641 (already cited
    prior round) explicitly documents "~1GB of total memory allocation across all workers" from
    each LocalExecutor worker independently importing modules at the stock parallelism=32 default
    -- external corroboration of (1)'s own mechanism, not a project-specific novelty.
  implication: >
    Directly answers the task's own analytical_hint and item-3 framing with source-grounded
    evidence rather than pure inference from noisy timing numbers: the growth/compounding pattern
    is best explained by GENUINE LIVE-OBJECT/DB-STATE ACCUMULATION (a livelock where repeated
    violent OOM-SIGKILLs interrupt in-flight tasks faster than they can complete, which Airflow's
    own Backfill/DagRun-completion mechanics do not route around), not a pure allocator/CoW
    artifact a bigger ceiling would legitimately absorb "for free" -- meaning "just raise the
    ceiling" is NOT by itself a complete answer, matching the task's own framing precisely. This
    favors a fix that reduces the SOURCE of memory pressure (the oversized, un-tuned
    `core.parallelism=32` worker pool this workload never needs, finding (1)/(2)) over one that
    only tolerates it, paired with a modest, separately-justified ceiling raise as safety margin
    for whatever residual sustained-churn growth apache/airflow#56641 still describes (no released
    upstream fix exists). `[scheduler] num_runs` is a real, source-verified, GRACEFUL alternative
    to a violent SIGKILL in principle (finding (3), corroborated externally by #1389) but finding
    (4) shows it interacts badly with this project's own specific task-runtime-vs-heartbeat-
    threshold shape and was not adopted this round without further dedicated verification --
    recorded as a considered-and-rejected option, not a silent omission.

- timestamp: 2026-08-24 (ROUND 2 continuation -- fix decision, implementation, and offline
    verification)
  checked: >
    Implemented the fix informed by the investigation above: (1) `helm/values/ci/airflow.yaml` and
    `helm/values/local/airflow.yaml`: `config.core.parallelism` added, `"32"` (implicit stock
    default) -> `"16"`, IDENTICALLY in both files (behavioral Airflow config, not a permitted D-06
    resource-sizing divergence axis, matching the established precedent already used for
    `scheduler_health_check_threshold`/`dag_file_processor_timeout` earlier this session -- ~2x
    headroom over the hand-counted realistic peak concurrency estimate from finding (2) above,
    while halving the eagerly-forked worker population and its associated per-worker import
    overhead). (2) `helm/values/ci/airflow.yaml` only: `scheduler.resources.limits.memory` 1Gi ->
    1536Mi (request left at 512Mi, unchanged) -- a secondary safety margin, ~1.5x the highest
    recorded peak sample (954MiB), CI-only since LOCAL's own scheduler never OOMs (KubernetesExecutor
    never pre-forks LocalExecutor workers at all, so this mechanism cannot occur there -- no larger
    LOCAL anchor value exists to match, per the task's own framing, so this raise is independently
    justified against the measured peak rather than a local-matching number). Verified both YAML
    files parse and both new/changed values render correctly: `make manifests` (0 chart lint
    failures across all 9 charts both profiles; `kubeconform -strict`: 540 resources, 378 valid, 0
    invalid, 0 errors); direct render inspection confirmed `[core] parallelism = 16` under the
    correct INI section in BOTH build/manifests/{ci,local}/airflow.yaml, and the scheduler
    StatefulSet container's `resources.limits.memory: 1536Mi` in the CI manifest. `uv run pytest
    tests/policy/test_manifest_resources.py -q -m manifests`: 5/5 pass, including
    `test_ci_profile_fits_runner` (3.180/3.200 cores -- byte-identical to before this fix, since it
    touches zero CPU requests and the memory-limit raise does not count toward the requests-only
    budget sum; real memory-request total 6504Mi/13107Mi budget, enormous headroom, confirmed via a
    direct one-off `request_totals()` invocation against the rendered CI manifests). `uv run pytest
    tests/policy/test_values_profiles.py -q`: 6/6 pass (confirms `core.parallelism` is correctly
    treated as identical/non-divergent between profiles, and the CI-only memory-limit change stays
    within the already-permitted "resource sizing" axis). `uv run pytest tests/policy/ -q -m "not
    manifests"` (167 collectible): 157 passed, 2 failed -- the SAME 2 pre-existing,
    already-documented out-of-scope failures every prior round in this file has shown
    (test_dag_line_budget.py's 150-line DAG budget, test_gates_actually_fail.py's lint meta-test,
    confirmed via the actual failure text: ruff findings in files this fix never touched,
    test_backfill_2year_sweep.py and test_migrations.py) -- zero new regressions.
  found: >
    All offline gates this session has established as authoritative for this debug session pass
    cleanly against the fix as implemented, with no new regressions anywhere in the broader policy
    suite. The fix is offline-complete; only live verification (a genuinely fresh
    cluster-slice-verify run against the actual CI runner's real contention) remains before this
    round can be considered resolved, per this session's own established discipline that
    self-verification alone -- however thorough -- is not sufficient without direct live evidence,
    especially given the deliberately-uncertain "peak realistic concurrency" hand-count noted as a
    blind_spot above.
  implication: >
    Ready to commit and push per this session's own established push-only precedent for
    e2e-full.yml (no pull_request trigger exists on this workflow, confirmed earlier in-file).
    Recorded here, before starting the live wait, per this round's own explicit task instruction --
    so that even in the worst case of an environment interruption mid-wait, the next continuation
    has full context and does not repeat this investigation.

## Eliminated
<!-- APPEND ONLY - never delete -->

- hypothesis: "The K8s livenessProbe/startupProbe.timeoutSeconds:60 fix already applied this session (commit 5abe533/99197cf) fully resolved the CPU-contention-driven scheduler/dag-processor instability."
  evidence: "Live diagnostic capture from run 32675592471 (job 97283007457), which already included that exact commit, still shows airflow-dag-processor with 5 restarts and airflow-scheduler-0 not fully Ready, with 'No alive jobs found' / liveness-probe-failed / BackOff events -- the fix reduced (perhaps) but did not eliminate the crash-loop, because it addressed K8s probe-command latency, not Airflow's own internal scheduler_health_check_threshold (30s default) that independently governs the same 'is the scheduler alive' verdict."
  timestamp: 2026-08-24 (this session)

- hypothesis: "The new post-fix vault-0 restart-timeout failure (e2e-chaos.yml run 32714166540, test_pod_restart_reseals_and_unseal_restores_service) is a real, if partial, refutation/complication of the fix -- i.e. raising scheduler/dagProcessor CPU REQUESTS by +300m tightened the node's remaining margin enough to newly starve vault-0's post-delete reschedule."
  evidence: >
    REFUTED by deeper log analysis this continuation session. (1) The exact same failure signature
    -- `kubectl wait --for=jsonpath={.status.phase}=Running --timeout=180s pod/vault-0` returning
    immediately with `Error from server (NotFound): pods "vault-0" not found` (NOT a 180s-elapsed
    timeout) -- already occurred in PRE-FIX run 32693178072 (job 97330575621, 2026-08-24T05:33,
    commit e0972e91ea, hours before the scheduler/dagProcessor fix was even written), which the
    orchestrator's 3-run sample did not include. (2) Root mechanism read directly from
    tests/e2e/vault/test_unseal_survives_restart.py: the test issues `kubectl delete pod/vault-0`
    then IMMEDIATELY calls `kubectl wait ... pod/vault-0` with no retry/backoff for the
    StatefulSet controller to recreate the pod object first. `kubectl wait` on a named (not
    label-selector) resource fails FAST with NotFound if the object does not exist at the moment
    the command starts -- it does not poll for the object's re-creation, only for its condition
    once it exists. This is a race condition inherent to the test's own delete-then-wait sequencing,
    independent of node CPU headroom: whether it is lost depends on kube-controller-manager's
    reconcile latency at the moment of deletion, not on vault-0's own resource budget (the failure
    is NotFound, not Pending/CrashLoopBackOff/Unschedulable, which is what real CPU starvation of
    vault-0 itself would produce). (3) The same run 32693178072 ALSO shows an unrelated pre-existing
    test bug in test_airflow_conn_minio_default_is_absent_from_every_component (`kubectl -n airflow
    get deployment airflow-scheduler` -> NotFound, because airflow-scheduler is a StatefulSet, not a
    Deployment -- confirmed via the same run's own `statefulset.apps/airflow-scheduler condition
    met` rollout-wait line), further showing this run's overall failure count (12 failed/20 passed)
    was already noisier pre-fix than the orchestrator's cited baseline.
  timestamp: 2026-08-24 (continuation session, after orchestrator handoff)

## Resolution
<!-- Fill when resolved -->

root_cause: >
  FOUR independent, sequentially-discovered root causes -- each masked the next until fixed
  (classic resource-starvation "whack-a-mole": fixing one bottleneck let the pipeline progress
  far enough to expose the next one). ALL FOUR are now fixed and LIVE-VERIFIED (fix 4 is
  offline-verified only, a policy-gate hygiene issue not a live-runtime behavior):
  (1) SCHEDULER CPU + BOTH COMPONENTS' HEALTH-CHECK THRESHOLDS: CI's single-node kind cluster
  under-sized scheduler/dagProcessor CPU (200m/500m each) for what LocalExecutor requires, and
  Airflow's own internal health-check thresholds (`scheduler_health_check_threshold`,
  `dag_file_processor_timeout`) were tighter than the K8s probe timeoutSeconds a prior fix had
  already raised. Fixed in commit a73282e; scheduler genuinely improved (live-verified via PR
  #13). Had ZERO measurable effect on dag-processor's own restart rate -- its bottleneck was (2).
  (2) DAG-PROCESSOR MEMORY: dag-processor's memory limit (256Mi request/512Mi limit) was never
  touched by fix (1). Its --previous container log showed an abrupt, silent death ~5-15s into
  container life while forking parser subprocesses for its 11-file DAG bundle -- mathematically
  too fast to be a liveness-probe kill (chart default failureThreshold:5/periodSeconds:60
  requires >=250s), consistent with an OOM kill. Live cgroup measurement on LOCAL (never exhibits
  this crash-loop, provisions double CI's dagProcessor memory) captured a real parse-cycle burst
  reaching >=372MiB against CI's old 512Mi limit -- only ~140MiB of margin. A documented OOM-prone
  pattern in Airflow 3.x's fork()-based dag-processor (apache/airflow#50708, #50097, #58509,
  #53662). LIVE-CONFIRMED via direct `kubectl get pods`: Restart Count 0 across the full final
  verification run (~8.5min, spanning cluster-up through a complete DAG lifecycle).
  (3) SCHEDULER MEMORY: with dag-processor no longer crash-looping, DAGs now actually register
  and DagRuns actually trigger -- exposing, for the first time, the scheduler's REAL in-process
  LocalExecutor task-execution memory footprint (previously invisible, since no task had ever
  gotten far enough to run). Scheduler's memory (256Mi/512Mi) was never touched by fix (1) (only
  CPU was); direct `kubectl describe pod` evidence showed Last State: Terminated / Reason:
  OOMKilled / Exit Code: 137 in the intermediate round. LIVE-CONFIRMED FIXED in the final
  verification run: Restart Count 0, same run as (2)'s confirmation.
  (4) VAULT-0 POD-NOT-FOUND RACE (a pre-existing, unrelated infra flake that blocked live
  verification of (2)/(3), not a resourcing issue): `scripts/stages/80-vault.sh`'s
  `wait_for_pod_running` helper does a NAMED `kubectl wait` immediately after `helm upgrade
  --install vault` reports "STATUS: deployed" -- a NAMED (non-label-selector) `kubectl wait`
  fails FAST with NotFound if the object does not exist yet, rather than polling for its
  creation (the exact same race class already independently diagnosed this session for
  tests/e2e/vault/test_unseal_survives_restart.py's own inline `kubectl wait`, see Eliminated).
  Hit twice in direct succession live this session. Fixed and confirmed working (cluster-up
  succeeded cleanly on the very next attempt).
  (4b, REOPENED ROUND, same root-cause class as (4) but a DIFFERENT code location the original
  (4) fix never reached): a fresh post-merge live run on main@c23d120 (the commit landing fixes
  1-3) surfaced a RECURRENCE of the identical kubectl-wait-races-pod-recreation pattern --
  `tests/e2e/vault/test_unseal_survives_restart.py` and `tests/e2e/chaos/test_vault_unavailable.py`
  (which copied the former's pattern believing it already-proven-working, per that module's own
  docstring) each carry their OWN independent, inline `kubectl delete pod vault-0` immediately
  followed by `kubectl wait --for=jsonpath={.status.phase}=Running pod/vault-0` -- neither ever
  routed through `scripts/wait-for.sh`'s `wait_for_pod_running` (fix (4) above), so neither
  received that fix. Confirmed via direct source read as the ONLY two occurrences repo-wide
  (grep for `_VAULT_POD`, for `delete`+`pod` kubectl calls, and for every remaining kubectl `wait`
  call site across `tests/e2e/`) -- every other kubectl delete/wait call site (test_pod_kill_retry.py/
  test_pod_crash.py's Airflow-retry-pod polling, test_audit_log.py's `tail`-only exec,
  test_minio_unavailable.py's Deployment `--for=condition=Available` waits) is structurally
  different and unaffected.
  Root cause (2) is what explained the ORIGINAL fixed-timeout E2E failures this debug session was
  opened to investigate (DagNotFound, registration never completing, DagRuns stuck in 'queued').
  All four are now fixed; the control-plane crash-loop this session was chartered to resolve is
  DEFINITIVELY confirmed eliminated via direct live evidence.
  A FIFTH finding (explicitly NOT a root cause of this debug session, NOT fixed here, flagged as
  a separate follow-up): the final live-verification run's `smoke_kubernetes_pod` DagRun reached
  a genuine terminal state of 'failed' rather than 'success' -- a functional/application-level
  issue in what the task itself does (or a KubernetesPodOperator task-pod-level problem in the
  `etl` namespace), NOT a timeout, NOT a crash-loop, and NOT investigated further here (this
  session's diagnostic step never captured `etl`-namespace pod details, only airflow-namespace
  control-plane pods -- a fresh, differently-scoped debug session would be needed).
  A SIXTH, unrelated finding fixed alongside for CI hygiene (not part of any root cause above):
  the real Helm-rendered CI-profile CPU total had independently drifted to 3.400 cores against
  the 3.200-core EFFECTIVE_CI_CPU_BUDGET (confirmed pre-existing on bare `main` via `git stash`,
  unaffected by fixes 1-4) -- traced to the same-day monitoring-stack quick task (260824-ayw)
  never re-running this specific CI-gated budget check (`.github/workflows/ci.yml`'s `check` job
  runs it via `make manifest-policy`).
  A SEVENTH, unrelated finding (observed live-verifying 4b, explicitly NOT fixed here, flagged for
  a separate follow-up): e2e-chaos.yml run 32738880729/job 97468249410 showed 5 tests -- including
  test_vault_unavailable.py's own vault-0 scenario -- all failing identically on "normalized.
  customers has fewer than N rows on this live cluster -- this test needs prior customers
  ingestion to have already happened" (test_database_unavailable.py, test_duplicate_batch.py [a
  related but distinctly-worded config_versions variant], test_malformed_csv.py,
  test_minio_unavailable.py, test_pod_crash.py, test_vault_unavailable.py). A shared data-
  precondition/test-ordering issue across the "Full QUAL-15 chaos suite (dedicated cluster)" job,
  unrelated to vault-0/poll_pod_running/CPU/memory resourcing -- none of this debug session's
  fixes touch it. Not investigated further here (out of scope per task guidance); a fresh,
  differently-scoped debug session would be needed if this recurs.
  (3b, ROUND 2, same root-cause CLASS as (3) -- scheduler memory -- but a SUSTAINED-LOAD
  manifestation only visible under cluster-slice-verify's much heavier ~60min multi-DAG suite,
  not smoke-verify's single-DAG ~8.5min proof that live-confirmed (3) as fixed): with (1)-(4)
  fully resolving the ORIGINAL fixed-timeout smoke-verify failures, the heavier suite exposed
  scheduler restarting repeatedly (7 times in ~62min, direct `kubectl describe pod` confirming
  `Reason: OOMKilled`/`Exit Code: 137` each time -- a genuine memory-ceiling breach, unambiguously
  different from (1)'s own CPU/heartbeat signature) even at (3)'s already-raised 512Mi/1Gi. Root
  mechanism, confirmed via direct source read of the installed apache-airflow==3.3.0 (not
  generic/version-agnostic reasoning): CI's `core.parallelism` was still at Airflow's stock
  default (32, never overridden), and `LocalExecutor.start()` eagerly forks exactly that many
  worker processes on every scheduler startup ("to minimize gc freeze/unfreeze cycles" per its own
  source comment) -- each independently importing the full Airflow module tree, the exact
  mechanism the currently-open apache/airflow#56641 documents ("~1GB... across all workers" at the
  stock default). This project's own two production DAGs cap real concurrency far below 32 by
  construction (`integrity_gate.override(max_active_tis_per_dag=3)` is the highest fan-out point
  either DAG has; `stage`/`dbt_build`/`publish` are each `max_active_tis_per_dag=1` GLOBALLY) --
  the eagerly-forked pool was provisioned roughly 3x+ larger than this workload could ever need,
  and the excess workers' import overhead plus their own sustained CoW growth under real task
  churn is what drove the ceiling breach. The observed restart-CYCLE-TIME pattern (a slow first
  climb, 31m52s, then a consistently faster 5-7min per cycle thereafter) is additional, source-
  grounded evidence of a genuine LIVE-OBJECT/DB-STATE-DRIVEN compounding mechanism, not a flat
  per-process leak: a K8s container restart after OOMKill starts a genuinely fresh OS process (no
  prior heap/CoW state carries over), so a pattern that compounds ACROSS restarts must be driven
  by something that DOES persist across a restart -- the shared metadata DB. Direct source read of
  `airflow.jobs.scheduler_job_runner` confirmed the mechanism: `adopt_or_reset_orphaned_tasks()`
  does run on every scheduler startup and correctly resets orphaned TASK INSTANCES, but
  `_mark_backfills_complete()` only clears a `Backfill` once none of its DagRuns are still
  RUNNING/QUEUED -- and a DagRun whose task keeps getting killed mid-execution (OOM-cycle period,
  5-7min after the first cycle, shorter than the ~13-15min a real task needs to finish, per
  test_backfill_2year_sweep.py's own docstring) never reaches that state. This is the direct,
  source-confirmed explanation for the REOPENED ROUND 2 deep-mining Evidence's own
  `AlreadyRunningBackfill` cascade blocking the rest of an affected run, not merely a slow patch --
  a livelock, not a one-time delay. See Evidence (ROUND 2 continuation) for the full source-level
  investigation, including the `[scheduler] num_runs` alternative that was researched and
  deliberately NOT adopted (LocalExecutor.end() gracefully waits for in-flight tasks rather than
  killing them, in principle avoiding this exact livelock -- but this project's own task runtimes
  comfortably exceed `scheduler_health_check_threshold`, 90s, meaning the liveness probe would
  very likely fire and kill the pod mid-graceful-wait anyway, undermining the benefit without
  further dedicated work).
fix: >
  (1) helm/values/ci/airflow.yaml: scheduler.resources (request 200m->400m cpu, limit
  500m->1500m cpu) and dagProcessor.resources (request 200m->300m cpu, limit 500m->1200m cpu).
  helm/values/{local,ci}/airflow.yaml identically: config.scheduler.scheduler_health_check_
  threshold: "90", config.dag_processor.dag_file_processor_timeout: "120" (behavioral config,
  not a permitted resource-sizing divergence axis per D-06).
  (2) helm/values/ci/airflow.yaml: dagProcessor.resources.requests/limits.memory 256Mi/512Mi ->
  512Mi/1Gi -- matches LOCAL's already-proven-stable dagProcessor sizing exactly.
  (3) helm/values/ci/airflow.yaml: scheduler.resources.requests/limits.memory 256Mi/512Mi ->
  512Mi/1Gi -- same fix pattern, same LOCAL reference point. CPU and the two Airflow-internal
  thresholds were NOT touched further in either (2) or (3) -- already raised in (1), and neither
  new failure mode (OOM) matches what those mechanisms would produce.
  (4) scripts/wait-for.sh: `wait_for_pod_running` now chains a `kubectl wait --for=create`
  (30s budget) before the existing phase=Running wait -- succeeds immediately if the pod object
  already exists (the common case), polls for its creation otherwise. Single production caller
  (scripts/stages/80-vault.sh), narrow blast radius.
  (4b, REOPENED ROUND): tests/e2e/vault/conftest.py -- new plain function `poll_pod_running`
  (hand-rolled `deadline = time.monotonic() + timeout` poll loop over `kubectl get pod <name> -o
  jsonpath={.status.phase}`, mirroring tests/e2e/chaos/conftest.py's own `_poll_all_pods_ready`
  idiom, adapted for a NAMED pod query instead of a label selector: every non-zero exit -- NotFound
  while the pod is still being recreated, in particular -- is treated as "not ready yet, keep
  polling" rather than a hard failure, since a named-resource query has no exit-0 way to represent
  "does not exist yet"). tests/e2e/vault/test_unseal_survives_restart.py and
  tests/e2e/chaos/test_vault_unavailable.py: both now import and call `poll_pod_running` in place
  of their own duplicated bare `kubectl wait --for=jsonpath=...Running pod/vault-0`
  (`_POD_RESTART_TIMEOUT_SECONDS` changed from the CLI-duration string `"180s"` to the int `180`
  in both files to match the new call site's `timeout: float` parameter).
  (5, separate CI-hygiene fix, not part of any root cause above): trimmed CPU requests on
  helm/values/ci/{tempo,otel-collector,monitoring}.yaml -- tempo/otel-collector 100m->10m each
  (confirmed never deployed live in CI, zero behavioral risk); monitoring.yaml's smallest
  housekeeping/one-shot containers only (grafana initChownData/downloadDashboards/sidecar sync
  10m->5m each, prometheusOperator 20m->10m, its admission-webhook patch Job 10m->5m) --
  grafana/prometheus's own serving containers, Kyverno, and all Airflow components deliberately
  left untouched.
  (6, ROUND 2): helm/values/{ci,local}/airflow.yaml identically: config.core.parallelism added,
  "16" (stock default was an implicit, never-overridden 32) -- behavioral Airflow config, not a
  permitted D-06 resource-sizing divergence axis, same non-divergent-axis precedent as (1)'s
  scheduler_health_check_threshold/dag_file_processor_timeout. PRIMARY fix for ROUND 2: trims the
  eagerly-forked LocalExecutor worker pool to roughly 2x this project's own hand-counted realistic
  peak concurrency (a low double digit), down from a pool sized 3x+ larger than ever needed.
  helm/values/ci/airflow.yaml only: scheduler.resources.limits.memory 1Gi -> 1536Mi (request left
  at 512Mi, unchanged -- does not affect the CI CPU/memory-request budget gate at all). SECONDARY
  safety margin (not claimed sufficient alone), ~1.5x the highest recorded peak sample (954MiB);
  CI-only because LOCAL's own scheduler never OOMs (KubernetesExecutor never pre-forks
  LocalExecutor workers), so no LOCAL anchor value exists to match this time -- justified
  independently against the measured peak instead, per the task's own explicit framing.
  `[scheduler] num_runs` was researched and deliberately NOT adopted this round -- see root_cause
  (3b) and Evidence (ROUND 2 continuation) for the full reasoning (graceful-shutdown benefit is
  real in principle, but interacts badly with this project's own task-runtime-vs-liveness-probe-
  threshold shape without further dedicated work).
verification: >
  Offline: (1) `make manifests` -- 0 chart lint failures across all 9 charts both profiles,
  kubeconform -strict reports 0 invalid/0 errors across 540 resources; (2) `uv run pytest
  tests/policy/test_manifest_resources.py -q -m manifests` -- all 5 tests pass INCLUDING
  `test_ci_profile_fits_runner` against the REAL rendered manifests, landing at ~3.08/3.2 cores;
  (3) `uv run pytest tests/policy/test_values_profiles.py -q` -- 6/6 pass; (4) `uv run pytest
  tests/policy/ -q -m "not manifests"` (159 collectible) -- 157 pass, 2 fail, both the SAME
  pre-existing, already-documented-out-of-scope failures from earlier in this same debug session
  (test_dag_line_budget.py, test_gates_actually_fail.py) -- nothing new broken; (5) `bash -n
  scripts/wait-for.sh` -- syntax clean.
  Live: fix (1) live-verified via throwaway PR #13 (job 97405917287) -- scheduler genuinely
  improved, dag-processor unchanged (led to the memory investigation). Fixes (2), (3) and (4)
  ALL LIVE-CONFIRMED TOGETHER via throwaway PR #14's final round (run 32727920639, job
  97433300855): cluster-up succeeded cleanly (fix 4 working -- no vault-0 race), and the
  diagnostic step's direct `kubectl get pods -o wide` shows BOTH airflow-dag-processor AND
  airflow-scheduler-0 at `2/2 Running 0 restarts` across their entire ~8.5-minute lifetime,
  spanning cluster-up through a complete live DAG lifecycle (registration -> trigger -> scheduler
  dispatch -> task execution to a terminal state). This is the strongest possible confirmation:
  direct, same-run, same-instance evidence, not inference. Fix (5) is offline-verified only (a
  CPU-budget policy gate, not a live-runtime-behavior fix, so no live-verification signal applies
  to it specifically).
  Fix (4b, REOPENED ROUND): offline-verified -- `python -m py_compile`
  clean on all 3 touched files; `ruff check` 0 issues; `ruff format --check` clean on both files
  with substantive edits (one remaining format diff in test_vault_unavailable.py's
  `_scheduler_resource_ref` confirmed pre-existing via `git show HEAD:...` reproducing
  byte-identically on the unmodified file, untouched by this fix); `mypy` 0 errors; `pytest
  --collect-only` collects both modified test files cleanly; full offline policy suite (159
  collectible) -- 157 pass, 2 fail, the SAME pre-existing out-of-scope failures as every prior
  round, zero new regressions.
  THEN LIVE-VERIFIED: e2e-chaos.yml run 32738880729 / job 97468249410 (triggered by this fix's own
  commit 0ef5ae6 on main) -- `test_pod_restart_reseals_and_unseal_restores_service` PASSED
  (confirmed by exhaustive elimination against the run's "9 failed, 23 passed" summary, 32/32
  outcomes accounted for with zero error/skip categories, and independently by the total absence
  of any poll_pod_running/test-name/timeout text anywhere in the 1721-line raw log, which is the
  expected signature of a clean pass given poll_pod_running's own silent-success code path).
  `test_vault_unavailable.py`'s own vault-0 scenario did not reach the changed code path in this
  run (failed earlier on an unrelated, pre-existing data-precondition assertion shared by 4 other
  structurally-unrelated tests in the same run -- see root_cause's SEVENTH finding) -- inconclusive
  for that one test, not a fix failure. No new error class introduced by poll_pod_running. See
  Evidence (continuation session 3) for full detail.
  REQUIRES human confirmation before this debug session is archived (see
  request_human_verification checkpoint) -- self-verification is as strong as this session can
  produce, but a genuinely independent human check (e.g. triggering the real Phase 11 completion
  gates against a clean main, or reviewing the live evidence directly) is the final gate per
  protocol.
  Fix (6, ROUND 2): offline-verified -- `make manifests` (0 chart lint failures across all 9
  charts both profiles, kubeconform -strict 0 invalid/0 errors across 540 resources; direct render
  inspection confirmed `[core] parallelism = 16` in the correct INI section of BOTH
  build/manifests/{ci,local}/airflow.yaml, and the scheduler container's `resources.limits.memory:
  1536Mi` in the CI manifest); `uv run pytest tests/policy/test_manifest_resources.py -q -m
  manifests` -- 5/5 pass, `test_ci_profile_fits_runner` unchanged at 3.180/3.200 cores (this fix
  touches zero CPU requests; the memory-limit raise does not count toward the requests-only
  budget sum -- real memory-request total 6504Mi/13107Mi budget, confirmed via a direct
  `request_totals()` invocation); `uv run pytest tests/policy/test_values_profiles.py -q` -- 6/6
  pass (confirms `core.parallelism` correctly classified as non-divergent behavioral config,
  identical in both profiles, and the CI-only memory-limit change stays within the
  already-permitted "resource sizing" axis); `uv run pytest tests/policy/ -q -m "not manifests"`
  (167 collectible) -- 157 pass, 2 fail, the SAME 2 pre-existing, already-documented out-of-scope
  failures every prior round in this file has shown (confirmed via the actual failure text: ruff
  findings in test_backfill_2year_sweep.py/test_migrations.py, files this fix never touched) --
  zero new regressions.
  NOT YET LIVE-VERIFIED -- this round's own live push-and-wait is the immediate next step (see
  Current Focus next_action). Archiving this debug session is now blocked on BOTH: the still-
  unanswered human checkpoint for the REOPENED ROUND (4b, vault-0 Python-side wait race, already
  live-verified, awaiting confirmation only) AND this round's own live verification of fix (6),
  not yet attempted.
files_changed:
  - helm/values/ci/airflow.yaml
  - helm/values/local/airflow.yaml
  - helm/values/ci/tempo.yaml
  - helm/values/ci/otel-collector.yaml
  - helm/values/ci/monitoring.yaml
  - scripts/wait-for.sh
  - tests/e2e/vault/conftest.py
  - tests/e2e/vault/test_unseal_survives_restart.py
  - tests/e2e/chaos/test_vault_unavailable.py
