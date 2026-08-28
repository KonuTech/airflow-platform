---
status: investigating
round18_status: "ROUND 18 POST-RUN ANALYSIS COMPLETE on run 33164806655 (headSha 4818867,
  conclusion FAILURE, job 10:49:25->13:58:41 = 3h09m16s = 189.3min, self-terminated 0.7min
  inside the 190-min ceiling -- the closest margin of the session): census 7 failed / 31
  passed / 6 skipped in 10866.63s (3:01:06) -- SAME COUNT as R17 but a MATERIALLY DIFFERENT
  composition, not a re-run of the same 7. Fix (24) WORKED: sweep assert-4 now PASSES
  (normalized.customers query change confirmed live) -- but the test proceeds further and
  fails at a LATER assertion (10, DELETE-detection) never previously reached (masked by
  assert-4 in every prior round) -- STRONG hypothesis, not yet forensically confirmed: the
  same 'test assumes one-file-per-publish-pass' mismatch (24) was, now recurring one
  assertion downstream, because fix (21)'s claim-ALL-currently-STAGED-runs batching can
  fold the final day's file into the SAME pass as an earlier day that still had the
  customer. Disposition (3)/(4)/(5) all fired as DESIGNED (900s wait, drain helper, stall
  assertion) and correctly demoted 5 of R17's messy failures into clean, fast, honestly-
  labeled ones -- but podkill's own DagRun genuinely NEVER reached a terminal ETL status
  this run (STAGED for the full 900s, error_type/error_message both None), a QUALITATIVELY
  WORSE outcome than R17's 66s-miss. ROOT CAUSE (new, strong, source-confirmed, not yet
  100% direct-evidence-closed): dagrun_timeout=45min almost certainly fired CORRECTLY
  (Airflow's own DagRun-level safety net, confirmed sound by direct source read of the
  installed 3.3.0 scheduler_job_runner.py/dagrun.py on the live LOCAL cluster) -- but
  there is NO mechanism connecting an Airflow-level dagrun_timeout/SKIPPED-task outcome
  back to meta.ingestion_runs, so the run is permanently orphaned at its last real status
  (STAGED) with zero error ever written at the application layer. Direct etl-monitor
  evidence shows a genuine publish pod FAILURE ~11min post-kill (publish-n1l2gcsv, Failed
  phase, ~12:25:35) -- publish's own retries=3/exponential-backoff plausibly never got a
  second attempt inside the 45min window, explaining why THIS run wedged where R17's
  (same mechanism, no publish-pod failure) did not. (26)'s error-bearing dump correctly
  printed ZERO rows (this failure shape has no FAILED/QUARANTINED/error-message row to
  find) -- (26)'s own query needs widening to also catch stale-STAGED rows, a real,
  now-confirmed diagnostics gap. Cascade (CONFIRMED, direct evidence): dbtkill/u3/orphan/
  idempotent_reupload's NEW drain-helper ALL correctly attribute their failure to
  podkill's DagRun still 'running' (or, for idempotent_reupload -- the session's OWN
  backlog after podkill's eventual dagrun_timeout release) -- pure downstream noise, no
  independent bug. Rebuild's NEW monotonic-stall assertion also fired CORRECTLY (zero
  progress for 600s, 14/14 orders files unsettled) -- same root cause, the global
  stage/dbt_build/publish slot was still recovering from podkill's ~45min (not R17's
  ~11min) occupancy. Guards fully green: Kyverno 0 (only the deliberate PASSED unsigned-
  image test), restarts 0 all roles, scheduler peak 1804MiB = 70.5% of 2560Mi (stable vs
  R17's 1776MiB) -- ZERO OOM/crash-loop this round, ruling out the ROUND-3-era livelock
  (M1) as this round's mechanism. DECISION CHECKPOINT returned (root cause strong but not
  forensically closed -- proposing a diagnostics-first ROUND 19, not a blind fix). See
  Current Focus ROUND 18 OUTCOME + Evidence."
round17_status: "ROUND 17 POST-RUN ANALYSIS COMPLETE on run 33147620963 (headSha 79dd299,
  conclusion FAILURE, job 06:21:03->09:06:11 = 2h45m08s, finished under its own steam,
  well inside the 190-min ceiling): census 7 failed / 31 passed / 6 skipped in 9454.05s
  (2:37:34) -- BEST census of the session (R16: 9/29/6). Newly green: reentry (0042 grant
  live-proven by the previously-fatal read now passing) + no_extra_schemas (allowlist) --
  criterion (c) MET. Zero new failures (7 subset of R16's 9) -- (d) MET. Guards all green
  ((e) MET): Kyverno 0, restarts 0, ZERO failed TIs (1492 customers TIs), 66/66 customers
  DagRuns success, scheduler peak 1776MiB = 69.4% of 2560Mi. Criterion (b) MET AND
  ADJUDICATED: sweep assert-4 fired WITH the forensics rider streamed -- finding (24) is a
  TEST-LAYER ARTIFACT, platform CORRECT (bronze has run 39's 50 rows, run SUCCEEDED,
  ledger claimed it; every late-file key sits in silver with the business-ts winner from
  the day-12/13 snapshots -- the corpus reuses the SAME 50 customer_ids daily, so the
  backdated day-8 rows deterministically lose every one-row-per-key silver slot; the
  test's own comment says the proof belongs in normalized.customers but the code queries
  silver.customers = wrong table; secondary: waits drained on pilot-era terminal state,
  backfill-3's own runs started 11s AFTER the failure). Criterion (a) NOT met but
  materially advanced: podkill's 1M DagRun now COMPLETES end-to-end in ~11.1min -- its
  publish finished ~07:52:40, just ~66s past the 600s deadline (R16: never completed,
  starved everything for 70+min) = the pre-registered residual risk fired by a whisker;
  dbtkill/u3 discovery starvation = knock-on (podkill's DagRun held the slot ~50s of
  dbtkill's window, then the ~5-run asset-event queue backlog from its 10.5-min occupancy
  drained serially -- zero FailedScheduling in that window, queue latency not CPU);
  rebuild improved 16/16 -> 15/16 unsettled with 9/16 STAGED (vs R16's 2) at -33% requeue
  mass -- the staged tail's serialized dbt/publish still exceeds 1800s; orphan = same
  starvation class post-rebuild. NEW FINDING (26): idempotent_reupload's failure MORPHED
  -- its first upload's run 439 went terminally FAILED (not starvation), same signature
  as dbtkill's-file runs 62+438 (all three FAILED pre-schema, schema_version_id NULL,
  zero failed TIs / pod warnings; both distinct FAILED files are exactly the 120-row
  fixtures while 50-row/250k/1M files staged clean) -- UNADJUDICABLE: the ingestion_runs
  dump omits the error column (named diagnostics gap). Criterion (f) MET at mechanism
  level (O(delta) publish live-measured: post-restage dbt+publish of 1M in <=2min), NOT
  at suite level (+24.7min total, explained: scd_concurrent +11.4 variance, reentry +5.1
  now running to full green, rebuild +3.9). DECISION CHECKPOINT returned. See Current
  Focus ROUND 17 OUTCOME + Evidence."
round16_status: "ROUND 16 POST-RUN ANALYSIS COMPLETE on run 33126343052 (headSha 0a69dec,
  conclusion FAILURE at 2h20m16s, self-terminated, 74% of the 190-min ceiling): census
  9 failed / 29 passed / 6 skipped in 2:12:50 -- BEST census of the session (R15: 10/28/6),
  and the QUARANTINED-not-SUCCEEDED signature is GONE (zero QUARANTINED failures anywhere).
  Fix-in-force proofs: all 3 images at 0a69dec (publish pushed 23:28-23:29, BEFORE
  cluster-up pulled 23:31); migrations 0039/0040/0041 all applied at cluster-up; fix (23)'s
  stage>>pending edge proven live in TI history (every DagRun: all stages -> dbt_build ->
  publish, strictly ordered); (19)-A proven live (concurrent_select orders 250k PASSED
  end-to-end, dbt_silver snapshot-complete PASSED, reentry's original run SUCCEEDED
  un-quarantined); (22)'s 0039 proven live BY failure #1 (normalized now visible in
  analytics_owner's information_schema, tripping the stale test allowlist); (20b) proven
  by mass_delete PASSED with exclusion predicates in force. Failure taxonomy: 2 one-line
  stragglers ((22b) analytics_owner lacks SELECT on meta.ingestion_runs -- reentry died
  at a line R15 never reached; (22c) test_no_extra_schemas allowlist missing 'normalized');
  NEW FINDING (25) owns 6 failures: orders serialized-pipeline throughput collapse under
  retained 1M-row fixtures (podkill 600s terminal timeout with run STAGED post-restage,
  then dbtkill/u3/orphan/idempotent 180s discovery starvation behind the backlog, then
  rebuild 16/16 orders files unsettled in 1800s with the node CPU-saturated -- the
  pre-registered (g) risk fired, wider than predicted); NEW FINDING (24): sweep assert 4
  STILL red (silver has no rows for _run_id=31) with the claim ledger provably live --
  per the pre-registered falsification this refutes the watermark as the SOLE mechanism;
  forensics destroyed by rebuild's meta reset (diagnostics gap: end-of-job dumps are
  post-rebuild). Guards: Kyverno 0, restarts 0, ZERO failed TIs (best ever), scheduler
  peak 1786MiB = 69.8% of the new 2560Mi (18b vindicated: = 91.5% of the old limit),
  SKIPPED_CONCURRENT 0, fixes 16/17/18/20/20a all hold. DECISION CHECKPOINT returned.
  See Current Focus ROUND 16 OUTCOME + Evidence."
round15_status: "ROUND 15 POST-RUN ANALYSIS COMPLETE on run 33103279876 (headSha 25b6eb0,
  conclusion FAILURE at 1h44m12s -- FIRST run of the whole session to finish under its
  own steam, 45% of the 190-min ceiling, 86min headroom): fixes (20)+(20a)
  LIVE-CONFIRMED IN FULL -- the RUNNING-wedge signature is ABSENT (every e2e run
  terminal, every 5-col fixture STAGED with bronze rows, zero SKIPPED_CONCURRENT in
  10,603 log lines), and (20a) LEG 2 was live-exercised by the podkill test's real
  SIGKILL: stage try=2 waited out the live lease 5m29s then genuinely re-staged 1M
  rows (distinct_keys=1M, zero duplicates). PRE-REGISTERED PREDICTION (19) FIRED
  EXACTLY: all 8 lone-customers-file e2e runs (544/584/612/640/668/736/764/809)
  terminal QUARANTINED, tests failed FAST and LEGIBLY (1.5-10.5min each, ~43.5min
  total vs R14's 73.5min of timeout burn) with streamed tracebacks. FIRST COMPLETE
  census: 28 passed / 10 failed / 6 skipped in 96.6min; all 17 baseline node-IDs
  finished. Failure taxonomy: 8x (19)-owned QUARANTINED-not-SUCCEEDED, 1x NEW legible
  (22) orphan-test InsufficientPrivilege (analytics_owner denied on schema
  normalized), 1x (21) sweep D-05 late-row lineage (silver has no rows for
  _run_id=42 despite run SUCCEEDED w/ 50 bronze rows -- both dataset waits DRAINED,
  assertions 1-3 passed; R14's sweep failure adjudicated as this, deeper than R14
  reached). Guards: Kyverno 0, restarts 0, breaker collateral 0, publish try=1
  outside the designed idem-rerun try=2s, ONE failed TI all session
  (integrity_gate[15] = teardown-deletes-fixture race at the exact second dbtkill's
  teardown ran); scheduler peak 1923MiB = 93.9% of 2048Mi NEW HIGH-WATER (18b).
  DECISION CHECKPOINT returned: (19) is now a live design decision. See Current
  Focus ROUND 15 OUTCOME + Evidence + Resolution."
round19_status: "ROUND 19 IN PROGRESS (diagnostics-only, no production fix): five
  instrumentation items implemented per user-confirmed charter -- orders DagRun/TI dump
  (mirrors ROUND 5 customers dump), raw unfiltered scheduler-log signature capture (exact
  'has timed-out'/'Error scheduling DAG run'/'Exception when executing SchedulerJob' text
  confirmed via direct source read of the installed apache-airflow==3.3.0 on the LOCAL
  persistent cluster), a new fast (2s) pod-termination watcher for stage/dbt_build/publish
  pods (closes the KPO on_finish_action=delete_pod race ROUND 18 named), (26)'s query widened
  to catch stale non-terminal (PENDING/RUNNING/STAGED, >20min old) rows, and (24)'s forensics-
  rider pattern extended to sweep assertion (10) via a new _delete_detection_forensics()
  helper (finished_at-equality pass-membership reconstruction, bronze presence check, last-
  seen normalized.customers row). Offline battery clean (manifests/kubeconform, 564 unit,
  14 dagtest, policy 157 passed + 2 pre-existing failures unrelated to this round, collection
  + bash/python syntax checks all pass). Pushed; live-verification run ID pending -- see
  Current Focus ROUND 19 + live_verification_state."
trigger: "CI pipeline ingestion timeout/contention: real Airflow pipeline runs (discover -> ingest -> publish) never complete within their fixed 180s test timeouts when running on GitHub Actions' single-node ephemeral CI cluster (kind/cluster-ci.yaml, ~3 allocatable CPU), even though the cluster itself comes up healthy. As a result, no test that requires a full DAG run to reach SUCCEEDED has ever been observed passing on GitHub's free-tier runners, blocking Phase 11's CICD-09 requirement from being provable end-to-end."
created: 2026-08-24
updated: 2026-08-28 (ROUND 19 diagnostics-only round implemented and pushed; see Current Focus ROUND 19)
updated_prior_round15: 2026-08-27 (ROUND 14 POST-RUN ANALYSIS COMPLETE on run 33080823061 (headSha a247b67,
  conclusion CANCELLED at the NEW 150-min ceiling after 2h31m01s -- FIRST legible per-test
  census of the session via the -v rider, 28 result lines survived): fix (18) LIVE-CONFIRMED
  IN FULL -- criteria (b)/(c)/(e) MET: the mass-delete fixture was claimed by ONE cron pass
  and terminally QUARANTINED (run 421, the only QUARANTINED row; test PASSED in 2m12s vs
  R13's ~40min collateral; 53 wall-to-wall cron DagRuns, zero gaps/wedges/+45:00 deaths,
  publish try=1 everywhere). Census: baseline 17 -> 7 PASSED / 9 FAILED / 1 unfinished.
  Criterion (a) BUDGET FAILED for a NEW named mechanism = FINDING (20): every e2e
  single-file CUSTOMERS fixture wedges at status RUNNING in the STAGE phase -- genuine
  stage try-1 pods crash seconds after claiming (etl-monitor caught phase=Failed pods in
  the exact try-1 windows), the retry lands inside the 5-min lease -> SKIPPED_CONCURRENT
  -> task SUCCESS with zero rows staged (silent drop at task-status layer), every later
  genuine attempt crashes identically, tests burn 73.5min of wait-timeouts failing, and
  teardown deletes the fixture leaving the run orphaned RUNNING forever. PRE-EXISTING
  (identical signature in ROUND 13's dump: runs 461/515/623/666 RUNNING) -- NOT a fix-(18)
  regression. Candidate (19) did NOT fire (zero e2e QUARANTINED -- the wedge is upstream
  of publish). Honest as-is total ~3h03m-3h20m; green-suite projection ~158-188min incl.
  the never-started observability+capstone steps -- 150 does not hold even green.
  DECISION CHECKPOINT returned. See Current Focus ROUND 14 OUTCOME + ROUND 14 post-run
  Evidence + Resolution (20).)
updated_prior_round14_open: 2026-08-27 (ROUND 14 opened on user decision Option C for finding (18a): trim the
  mass-delete collateral (all three complementary shapes, B-ii production change explicitly
  approved) AND raise timeout-minutes to 150, plus the pytest-observability rider. See Current
  Focus ROUND 14.)
updated_prior_round14: 2026-08-27 (ROUND 13 POST-RUN ANALYSIS COMPLETE on run 33062702180 (headSha 4d3db56,
  conclusion CANCELLED by job timeout-minutes: 120 after exactly 2h00m49s -- partial log +
  always()-diagnostics fully recovered, 7280 lines): fix (17) LIVE-CONFIRMED IN FULL --
  cluster-up's layer-B unpause line fired 10:28:07 ("csv_ingest_orders unpaused"), FIRST-EVER
  orders execution on CI: all 12 orders corpus files SUCCEEDED (initial wave runs 25-34 +
  replay wave 49-60 incl. days 12/13; orders schema v1 CONTRACT -> v2 INFERRED evolution
  mirrored customers'), the sweep's orders-terminal wait DRAINED (87.6min in R12 -> minutes),
  and the suite progressed through FOUR further test modules to the LAST one
  (test_smoke_and_idempotency, run 666 RUNNING at cancel) -- deepest CI progress ever, by far.
  The 120min went to: 6.8min cluster-up/setup + ~75min of tests running correctly + ONE
  measured avoidable sink = ~40min MASS-DELETE BREAKER COLLATERAL (root-cause-(18a) shape:
  test_mass_delete_snapshot_trips_circuit_breaker's deliberately-truncated snapshot was ALSO
  ingested by the */1 cron run scheduled__11:09, whose publish retried a DETERMINISTIC
  QualityThresholdExceeded 7x over 42min and held max_active_runs=1 -- cron gap 11:09->11:53
  stalled every later cron-dependent wait) + a permanent orders co-scheduling tax
  (steady-state cron runs 96-104s -> 2.3-3.7min). Transient FailedScheduling burst
  10:42-10:47 (7 pods, Insufficient cpu, self-healed) = guard (d) strictly failed;
  scheduler peak 1881MiB = 91.9% of 2048Mi (new high-water). Projected honest total
  ~2h35m as-is / ~2h with the collateral eliminated -- the timeout-vs-trim decision is
  now LIVE with real arithmetic. DECISION CHECKPOINT returned. See Current Focus
  ROUND 13 OUTCOME + ROUND 13 post-run Evidence.)
updated_prior_round13_postrun: 2026-08-27 (ROUND 13 opened on user decision A+B+C for root cause (17): csv_ingest_orders
  registers paused on fresh clusters and silently drops asset events. Implementing all three
  layers: (A) csv_ingest_orders added to tests/e2e/slice/conftest.py::_unpause_slice_dags +
  docstring truth-up (auto-covers chaos via its conftest import); (B) Makefile cluster-up gains a
  retried csv_ingest_orders unpause (smoke-verify's 24x5s idiom -- NOT smoke-verify itself, which
  e2e-full.yml never runs; cluster-up is the only site that covers the rebuild-from-raw capstone);
  (C, explicitly user-approved production-semantics change) is_paused_upon_creation=False on the
  csv_ingest_orders @dag -- an asset-triggered downstream that silently drops events while paused
  violates the platform's no-silent-drops core value. DAG survey: orders is the ONLY
  asset-scheduled DAG (customers=cron */1, smoke/retention=@daily, chaos probes=None) -- C applies
  to orders alone. timeout-minutes stays 120, measured this round per budget arithmetic (33min
  honest floor + orders drain should fit). See Current Focus ROUND 13.)
updated_prior_round13: 2026-08-27 (ROUND 12 POST-RUN ANALYSIS COMPLETE on run 33051719850 (headSha 794db33,
  conclusion CANCELLED by job-level timeout-minutes: 120 after exactly 2h00m42s -- partial log +
  always()-diagnostics fully recovered): fix (16) LIVE-CONFIRMED IN FULL -- ZERO breaker trips
  (0 grep hits for QualityThresholdExceeded/mass_delete_circuit_breaker/vanished in 5939 lines),
  63/63 customers DagRuns state=success (62 complete + 1 running at cancel), ZERO +45:00
  dagrun_timeout deaths, 344 TIs = 226 success + 114 skipped (empty-window short-circuits) + 4
  running, ZERO failures, ZERO retries; fix-in-force probe exact-matched predictions
  (schema_versions v1 CONTRACT + v2 INFERRED; runs 25-34 carry replay_of_run_id; silver _run_id
  distribution 35->1/36->49 = deterministic newest-run winner per 16c; run 36's 49-key pass
  published clean = bronze-scoped 16a working). All regression guards green (FailedScheduling 0,
  Kyverno 0, restarts 0, scheduler peak 1415MiB<2048Mi). VERDICT: BRANCH (b) -- a NEW mechanism
  consumed the budget = ROOT CAUSE (17): csv_ingest_orders (Asset-scheduled off customers_asset)
  is NEVER unpaused on ephemeral CI clusters (Airflow default dags_are_paused_at_creation=true;
  _unpause_slice_dags unpauses only smoke+customers; repo-wide grep: no other unpause site) -- a
  paused DAG silently consumes no asset events, so the sweep's orders-terminal wait
  (timeout=5400s) sat 87.6min (08:27:38->09:55:12 cancel) with zero orders pods ever, while the
  job timeout fired ~2min before the test's own deadline. Differential clincher: local cluster's
  csv_ingest_orders is_paused=False (hand-state from an earlier session -- same class as root
  cause 12). Honest-work floor measured: everything through the full-sweep backfill = 33min.
  DECISION CHECKPOINT returned. See Current Focus ROUND 12 OUTCOME + ROUND 12 post-run Evidence
  + Resolution (16)/(17).)
updated_prior_round12_postrun: 2026-08-27 (ROUND 12 COMPLETE offline: root cause (16) CONFIRMED via local red/green
  reproduction -- the 54% vanished mass is silver dedup-tie lineage on a BYTE-IDENTICAL
  D-18 replay wave (schema_version_term ''->'2' re-eligibilizes all files after pass 1;
  replayed rows full-tie the silver ranking; the then-silver-scoped _VANISHED_SQL read
  every tie-loser as vanished; local repro 24/50=48% pre-fix, 0 post-fix). Fix (16):
  bronze-scoped staged snapshot (_VANISHED_SQL + _SNAPSHOT_MAX_EVENT_TS_SQL) +
  deterministic _run_id desc tie-break in both silver models + red/green regression test
  + e2e-full.yml run->file/schema-version diagnostics. Riders resolved as non-bugs
  (dagrun_timeout force-skip; try-7 backoff past +45:00). B-leg: corpus churn 2% max --
  corpus/threshold correctly sized, no change. Offline battery green, zero regressions
  (whole-directory A/B byte-identical to HEAD baseline). Awaiting live CI verification.
  See Current Focus ROUND 12 + the two ROUND 12 Evidence entries + Resolution (16).)
updated_prior_round12: 2026-08-25 (ROUND 11 POST-RUN ANALYSIS COMPLETE on run 32884691063 (headSha 377c068,
  conclusion CANCELLED by the job-level timeout-minutes: 120 after exactly 2h00m42s -- partial
  logs + always()-diagnostics fully recovered): fix (15) LIVE-CONFIRMED -- criteria (a)/(b)/(d)
  ALL MET: dbt_build state=success try=1 in ALL 7 DagRuns that reached it (first-ever CI dbt
  successes, 20-30s each), ZERO NOT-NULL/permission-denied anywhere, stage 60/60 success try=1
  (8-26s), FailedScheduling 0, Kyverno DENY 0, restarts 0 (scheduler peak 1288MiB<2048Mi).
  FIRST-EVER complete end-to-end DagRun on CI: backfill__10:24 in 6m28s incl. publish success
  try=1 (29s). Criterion (c) FAILED for a NEW mechanism = residual candidate (16): every
  subsequent DagRun's publish deterministically trips mass_delete_circuit_breaker
  (QualityThresholdExceeded, vanished 27/50=54% > threshold 10%), retries exhaust, DagRun
  wedges to exactly dagrun_timeout=45min; 4 wedged runs x 45min consumed the 120-min job
  budget (poison is self-sustaining -- raising timeout-minutes alone CANNOT go green).
  DECISION CHECKPOINT returned. See Current Focus ROUND 11 OUTCOME + ROUND 11 post-run
  Evidence + Resolution (15)/(16).)
updated_prior_round11: 2026-08-25 (ROUND 10 POST-RUN ANALYSIS COMPLETE on run 32873456327 (headSha d0d1ad6):
  ROOT CAUSE (14) LIVE-CONFIRMED, fix (14) WORKS at mechanism level -- ALL FIVE pre-registered
  primaries green: (0) publish.yml companion success; (1) fix verifiably in force
  (stage_cpu_request=200m registered AND effective, node platform requests 2780m->2580m);
  (2) FIRST-EVER stage successes on CI: 20/20 mapped stage TIs state=success try=1 at ~30-32s
  wall each (was: zero successes ever, all attempts 128-130s); (3) FailedScheduling census
  ZERO across the whole run -- pods schedule immediately post-fix; (4) Kyverno DENY 0;
  (5) control-plane restarts 0 (scheduler peak 1644MiB < 2048Mi, above the old 1536Mi ceiling
  again). Node-ID set is the SAME 17 for the 12th run (saturated, as pre-registered) but the
  census shifted decisively: pilot PENDING->STAGED, dbt-build pods now RUN and emit real dbt
  output failing deterministically with TWO concrete DB errors = NEW RESIDUAL CANDIDATE (15):
  (15a) silver_orders model: null dataset_id violates dedup_audit NOT NULL; (15b)
  reconciliation_customers test: permission denied for table datasets under least-privilege
  dbt_app. The dbt_build retry wedge holds DagRuns to dagrun_timeout=45min, pinning
  max_active_runs=1 -> the 7 discovery-window misses + 3 AlreadyRunningBackfill + data-state
  asserts are knock-ons. Two independent test-side artifacts also named: git_sha full-vs-short
  compare, 'meta' unexpected-schema expectation. DECISION CHECKPOINT returned. See Current
  Focus ROUND 10 outcome + ROUND 10 post-run Evidence + Resolution (14)/(15).)
updated_prior_round10_analysis: 2026-08-25 (ROUND 10 THROUGHPUT-CEILING ANALYSIS COMPLETE under the user-chosen Hybrid
  charter: the pre-registered three-branch expectation resolved to a FOURTH branch -- the
  per-file drain floor on CI is INFINITE. NEW ROOT CAUSE (14), arithmetic-decisive from
  round9-job.log + source: _STAGE_RESOURCES requests cpu=500m (stage AND publish pods) vs
  ~220m free on the 3000m-allocatable CI node (2780m/92% steady-state platform requests) --
  the pods are STRUCTURALLY UNSCHEDULABLE; KPO default startup_timeout_seconds=120 turns
  every attempt into a deterministic ~129s failure (all recorded stage attempts 128-130s,
  tries to 6, zero stage successes EVER on CI; pilot file PENDING at 1800s proves no stage
  container ever started; discover at 100m succeeds try=1 in 11-23s). Every residual failure
  template reduces to (14). Corpus size irrelevant (19 x ~3.3KiB); corpus count load-bearing
  only post-fix; option C proper (slot redesign) assessed AGAINST. DECISION CHECKPOINT
  returned with quantified fix options (A: request 500m->200m both DAGs + ~200m CI values
  trims; B: Variable-driven per-profile resources; C: CI-values-only trims) plus the
  in-round-regardless corpus-filler shrink 19->~12 and etl-namespace event instrumentation.
  See Current Focus ROUND 10 + the two ROUND 10 Evidence entries + root_cause (14).)
updated_prior_round10: 2026-08-25 (ROUND 9 POST-RUN ANALYSIS COMPLETE on run 32855002333: BOTH fixes
  (12)/(13) LIVE-CONFIRMED at mechanism level -- analytics_db_default provisioned ('created'
  13:50:28Z), ZERO dbt_build upstream_failed stamps anywhere (wedge GONE, was every DagRun of
  rounds 5-8), scheduler restarts 0 over 66min (was 9) with measured peak 1559MiB/24pids --
  ABOVE the old 1536Mi ceiling, BELOW the new 2048Mi, proving fix (13) load-bearing, and
  Kyverno DENY still 0 (fix 11 holding). pytest node-ID set is the same 17 for the 11th run
  (saturated instrument, as pre-registered) BUT the failure-template census shifted materially
  for the first time all session: dbt chain now starts (meta.run_stages DBT_BUILD row exists),
  backfill-create failures dropped 6->3 (all AlreadyRunningBackfill -- backfills overrun and
  collide, also present in ROUND 8), pilot file now REGISTERS but stays PENDING at 1800s, 7
  files still miss the 180s discovery window. Residual = the deferred option C throughput
  ceiling (max_active_tis_per_dag=1 global slots + ~3 CPU) plus downstream data-state asserts.
  Awaiting user decision checkpoint on next direction. See Current Focus ROUND 9 OUTCOME.)
updated_prior_round9_fix: 2026-08-25 (ROUND 9 investigation COMPLETE + fixes (12)/(13) implemented, offline-verified,
  awaiting live CI verification. Both ROUND 8 suspects for the dbt_build upstream_failed leg
  REFUTED (stamps recur under a healthy scheduler in ALL DagRuns of rounds 5-8): actual cause is
  root cause (12) -- Airflow Connection analytics_db_default was NEVER provisioned in code (only
  ad-hoc hand-written into the long-lived LOCAL Vault by a prior session; scripts/
  vault-bootstrap.py wrote only minio_default), so every ephemeral CI cluster lacks it and
  list_run_ids_pending_dbt_build (root task, retries=0) fails ~30s into EVERY CI DagRun,
  cascading upstream_failed through mark_dbt_build_running->dbt_build and launching publish
  prematurely into guaranteed-failing 2-min KPO retry pods (= deferred-items.md plan-11-09
  'defect 1' firing 100% deterministically on CI; dbt has NEVER run on CI). Scheduler OOM leg:
  root cause (13) -- burst-concurrency, not a leak (stable 360MiB baseline; abrupt
  1331-1533MiB/24-25-pid spikes one 17s sample before each of 8 OOMKills = ~8 simultaneous task
  processes x ~145MiB import cost vs the 1536Mi limit; crash-loop provably stops the moment the
  wedged runs die at dagrun_timeout 12:54:44). Fixes: (12) vault-bootstrap.py now provisions
  airflow/connections/analytics_db_default from etl/analytics-db#dsn (read-guarded, idempotent,
  local hand-written secret untouched); (13) CI scheduler memory limit 1536Mi->2048Mi
  (limit-only, zero request-budget cost, sized to the measured worst-case parallelism=8 burst).
  See Current Focus ROUND 9 + the four ROUND 9 Evidence entries + root_cause (12)/(13).)
updated_prior_round9: 2026-08-25 (ROUND 8 post-run analysis COMPLETE on run 32845181597: per the pre-registered
  decision tree, hypothesis (10)-as-signature-cause is INSUFFICIENT -- the exemption was
  VERIFIABLY applied (policy created at cluster-up from the ce73d9d committed file) and WORKED at
  the mechanism level (ZERO Kyverno denials anywhere in the 5434-line log vs 14-18 in rounds 6/7;
  ZERO Docker Hub 429s; discover tasks now reach state=success try=1 in 11-15s when scheduled --
  first discover successes EVER observed on CI this session), yet the pytest signature is the
  10TH consecutive byte-identical 17-test node-ID set (17 failed/21 passed/6 skipped in
  3509.10s). The run exposes the actual residual mechanism directly: scheduler OOM crash-loop
  (9 restarts at ~6min cadence, kubectl describe: OOMKilled/137, one container alive only 38s)
  wedged the first two DagRuns for 47min (dbt_build marked upstream_failed at 12:07:51/59 --
  seconds into the FIRST crash window -- with discover try=0/start=None) until dagrun_timeout
  failed them at 12:54:44; replacement runs then executed correctly (discover success) but were
  throttled by the global max_active_tis_per_dag slot (269 'task concurrency ... reached'
  scheduler-log messages: integrity_gate x149, stage x120) with the suite ending at 13:05 with
  runs still in flight. Awaiting user decision checkpoint. See Current Focus ROUND 8 OUTCOME.)
updated_prior_round8_fix: 2026-08-25 (ROUND 8 fix A implemented per user decision A+B: alpine:3.24.1 XCom-sidecar
  exemption added to kubernetes/kyverno-policy.yaml (both ref forms), LIVE-FALSIFIED before/after
  on the LOCAL cluster via server-side dry-run probes (denied -> admitted; negative control still
  denied), LOCAL reconciliation question ANSWERED (local does NOT pass -- same denial reproduced
  1:1, 'local passes' was stale), full offline battery green. Awaiting authoritative CI run.
  See Current Focus ROUND 8 block.)
updated_prior_round7: 2026-08-25 (ROUND 7 post-run analysis COMPLETE: aggregate-load hypothesis REFUTED per
  its own pre-registered falsification test (run 32834311083: byte-identical 17-test signature,
  9th consecutive, with parallelism=8 VERIFIED in force) -- BUT the run's DENY text surfaced a
  NEW, structural root-cause mechanism with direct evidence: Kyverno policy evaluation aborts on
  'GET https://index.docker.io/v2/library/alpine/manifests/3.24.1: 429 Too Many Requests' -- the
  KPO XCom sidecar image (provider 10.19.0 XCOM_SIDECAR_IMAGE='alpine:3.24.1', injected at
  runtime into every KPO pod by do_xcom_push=True in _common/kpo.py) is NOT on
  kubernetes/kyverno-policy.yaml's exception list (which pins stale 'alpine:3.17'), so it
  requires cosign verification it can structurally never pass, and on GH-hosted runners even the
  manifest fetch is Docker-Hub-rate-limited. See Current Focus ROUND 7 OUTCOME + Resolution
  root_cause (10). Awaiting user decision checkpoint on the ROUND 8 fix direction.)
updated_prior: 2026-08-25 (ROUND 7 opened on session resume -- user chose REDUCE CONCURRENT LOAD
  direction after ROUND 6's live falsification FAILED (14 Kyverno DENY messages persisted, same
  invariant 17-test signature, 8th consecutive identical run). See Current Focus ROUND 7 block.
  Prior ROUND 6 note: CONFIRMED via direct evidence: Kyverno's require-signed-images
  admission policy denies discover/publish (and by extrapolation stage/dbt_build) pod creation for
  csv_ingest_customers under CI-node CPU contention, refuting ROUND 5's sweep_corpus/integrity_gate
  backlog theory (8 independent later-failing tests' own files, spread across the whole run, all hit
  the identical failure, ruling out a one-time drainable backlog). Fix applied: Kyverno
  admissionController CPU limit 200m->500m (zero CI budget-gate cost) + short, capped retry_delay
  for the 4 KPO tasks (more attempts fit inside dagrun_timeout=45min). Awaiting live verification.)
updated_prior_2: 2026-08-25 (ROUND 5 opens -- ROUND 4's fix (8, DAG-pause-fixture removal) independently
  RE-CONFIRMED LIVE-VERIFIED INSUFFICIENT via direct `gh run view`/`gh api actions/jobs/.../logs`
  evidence gathered fresh this round against run 32779160265/job 97597115875 (headSha f0ebfe3,
  fix (8) itself, NOT a stale/different commit): pytest "17 failed, 21 passed, 6 skipped... in
  3659.44s" -- name-for-name IDENTICAL to the pre-fix(8) 17-test baseline (byte-for-byte same 17
  node-IDs, same error-message templates, only UUID/timestamp/customer_id values differ), a SIXTH
  consecutive occurrence. Scheduler restarts continued their monotonic decline (3->2 this run,
  dag-processor 0) -- ROUNDs 1-3's fixes keep working exactly as designed, on a mechanism now
  doubly-confirmed orthogonal to this signature. Also independently reconciled: an orchestrator
  deployment-staleness investigation (prior session, now recorded in Evidence below) proved this
  repo's hostPath DAG-mount convention means EVERY fix this session has made (Helm values, DAG
  files, test fixtures) reaches the live CI cluster/tests with zero staleness/lag -- "the fix isn't
  reaching the cluster" is RULED OUT as an explanation for all 4 rounds' lack of effect on this
  specific signature, not just some of them. ROUND 5 opens a genuine strategic reset: the actual
  mechanism behind the invariant 17-test signature remains unexplained after 4 substantive rounds.
  ROUNDs 1-4's fixes and the vault-0 fixes are UNCHANGED/NOT reverted, per scope_guardrails.
  UPDATE (same day): source-level investigation against the installed apache-airflow==3.3.0 found
  a NEW, not-yet-live-tested mechanism candidate (max_active_runs is independent per (dag_id,
  backfill_id) but max_active_tis_per_dag on stage/dbt_build/publish IS truly global -- see Current
  Focus/Evidence) and a real, previously-unmonitored component (triggerer, which wait_for_files'
  deferrable S3KeySensor depends on). Added new throwaway diagnostics to e2e-full.yml (DagRun/
  TaskInstance DB-state dump, scheduler/triggerer log greps for exact concurrency-constraint
  messages, MinIO listing, triggerer cgroup time series) -- offline-verified, zero production code
  touched, NOT YET pushed/live-verified. No fix proposed yet -- awaiting this instrumentation's own
  direct live evidence per this round's explicit charter.)
---

## Current Focus
<!-- OVERWRITE on each update - always reflects NOW -->

ROUND 19 (2026-08-28, DIAGNOSTICS-ONLY round opened on user decision confirming this session's
own diagnostics-first precedent -- NO production fix for the podkill stall or the sweep
assertion-10 DELETE-detection gap this round; closes ROUND 18's two named evidence gaps so
ROUND 20 can fix from confirmed root cause instead of reconstruction):
  charter: "(1) Add an orders-DagRun/TaskInstance dump to end-of-job diagnostics, mirroring the
      existing ROUND 5 customers dump exactly (same 6-task list: wait_for_files/discover/
      stage/integrity_gate/dbt_build/publish -- confirmed csv_ingest_orders.py declares the
      identical task_ids, including integrity_gate via .expand()). (2) Capture raw scheduler
      stdout/stderr, UNFILTERED by dag_id, and grep for the EXACT dagrun_timeout/scheduling-
      error log signatures -- confirmed by direct source read of the INSTALLED
      apache-airflow==3.3.0 scheduler_job_runner.py on this session's own LOCAL persistent
      cluster (same image version as CI): `self.log.info(\"Run %s of %s has timed-out\", ...)`
      (line ~2833, fires exactly when dagrun_timeout trips), `self.log.exception(\"Error
      scheduling DAG run %s of %s\", ...)` (line ~2785, a genuine scheduling-loop error), and
      `\"Exception when executing SchedulerJob._run_scheduler_loop\"` (scheduler-loop-fatal).
      All three carry dag_run_id/dag_id INSIDE the message text, so no dag_id pre-filter
      needed -- `--since=200m` (vs ROUND 5's 100m) because this diagnostics step runs after
      cluster-slice-verify completes, which can itself exceed 100 minutes. (3) Terminated-
      container reason capture for stage/dbt_build/publish pods BEFORE KPO's own
      `on_finish_action: \"delete_pod\"` (confirmed in airflow/dags/_common/kpo.py) deletes
      them -- a SEPARATE fast (2s) background poll loop, narrowly scoped to only these three
      pod-name substrings (not the whole etl namespace like the existing 15s cp-monitor loop),
      extracting `status.containerStatuses[].state.terminated` / `.lastState.terminated`
      (reason/exitCode/message/timestamps) via a `kubectl get pods -o json | python3` pipeline,
      logged to /tmp/etl-pod-terminations.log, deduped with `sort -u` at dump time. (4) Widened
      finding (26)'s error-bearing-runs query: added `OR (r.status IN ('PENDING', 'RUNNING',
      'STAGED') AND r.started_at < now() - interval '20 minutes')` -- 20min is deliberately far
      below dagrun_timeout=45min (unambiguously stale well before dagrun_timeout would even
      fire) and far above this suite's own ~1-2min/file processing pace (no false-positive risk
      against a normal in-flight run near job end); also surfaces `started_at`/
      `lease_expires_at`/`age_since_started` columns directly in the dump text. (5) Extended
      (24)'s exact forensics-rider pattern (same file, same helper-function shape) to sweep
      assertion (10): new `_delete_detection_forensics()` helper streams, on EITHER of
      assertion-10's two checks failing, (a) the final day's own publish pass's FULL
      staged_run_ids batch -- reconstructed via `finished_at` equality across
      meta.ingestion_runs (direct source read of pipeline/run.py's `publish_ingest` confirms
      `finished_at` is computed ONCE per invocation, outside the per-run finalize loop, and
      stamped IDENTICALLY onto every run_id that pass finalizes -- the durable, after-the-fact
      pass-membership key, since meta.ingestion_runs itself carries no persistent
      staged_run_ids/pass-id column), (b) whether the missing customer
      (customer_id=2100100032) appears in staging.customers (bronze) for ANY run_id in that
      batch, (c) the missing customer's own actual last-seen normalized.customers row. Both
      former plain `assert` statements converted to `if not X: pytest.fail(msg + forensics)`,
      matching (24)'s exact structural pattern. Converted find_vanished_customer_ids' own
      `staged_run_ids` sourcing (`list_staged_run_ids` = every currently-STAGED run_id at
      publish time, confirmed via direct source read of metadata/postgres.py) into the
      forensics narrative so ROUND 20 can adjudicate the batching hypothesis from the dump
      text alone, no further inference needed."
  pre_registered_success_criterion: "If podkill or the sweep assertion-10 DELETE-detection
      finding fails AGAIN in this round's live-verification run, the round's SUCCESS is judged
      independently of whether either underlying bug reproduces: success means the NEW
      diagnostics make the mechanism FORENSICALLY CLOSED with DIRECT evidence (exact 'has
      timed-out' log line + exact orders-side DagRun/TI state for podkill; exact staged_run_ids
      batch composition + exact bronze presence/absence for assertion 10) -- NOT 'confirmed via
      inference' the way ROUND 18's podkill reconstruction was. Failure of this round means the
      new diagnostics STILL leave a gap (e.g. the pod-termination watcher's 2s poll missed the
      window, or the raw scheduler log rotated out the timed-out event, or the finished_at
      grouping heuristic does not actually match how the batch was formed) -- in which case
      ROUND 20 must close THAT gap before attempting any fix, never fix blind against a
      still-reconstructed mechanism."
  scope_guardrails: "NO production code touched this round (dags/, packages/dataplat/src/ all
      unchanged) -- diagnostics/instrumentation only, in .github/workflows/e2e-full.yml and
      tests/e2e/slice/test_backfill_2year_sweep.py (the LATTER only adds a forensics-capture
      helper + converts 2 asserts to pytest.fail-with-forensics on the SAME two conditions --
      the assertions' own pass/fail semantics are UNCHANGED, matching (24)'s own precedent of
      the rider never repointing or weakening the assertion it instruments). Rounds 1-18 fixes
      stay unchanged. Runner migration/job-splitting retired. Offline 'CI' workflow failures
      out of scope. Carried follow-ups unchanged: sidecar mirror, stage-side
      RejectionRateCircuitBreaker classification, teardown-race flake class, v_run_recovery
      wording, ADR-0012's deferred silver disposition, merge.py delta-scoping before any
      dataset adopts strategy 'merge', scd_concurrent duration-variance watch."
  offline_battery: "make manifests (kubeconform -strict, 540 resources, 378 valid/0
      invalid/0 errors -- unrelated to this round's changes but confirms nothing broke); make
      test (564 passed); make test-dagtest (14 passed); make policy (157 passed, 2
      PRE-EXISTING failures: test_csv_ingest_customers_stays_under_150_lines and
      test_the_main_gate_does_not_lint_the_bad_samples -- BOTH confirmed pre-existing, matching
      this round's own pre-registered expectation, unrelated to this round's diffs); ruff check
      on the modified test file shows only the SAME pre-existing line-1258 E501/W505 (untouched
      by this round's edits); ruff format --check shows 4 pre-existing reformatting diffs, NONE
      inside this round's inserted code (verified by line-range inspection); `python3 -m
      pytest tests/e2e/slice/test_backfill_2year_sweep.py --collect-only` succeeds (7 tests
      collected, confirms the new helper/pytest.fail rewiring is syntactically and structurally
      sound); bash -n + python ast.parse on the extracted heredoc scripts (cp-monitor.sh's new
      pod-term-watch.sh block, including its embedded python3 -c snippet) all pass."
  push_ordering_correction: "The code commit (dfdacd5) and the docs commit (9ccaf2d, carrying
      the skip-ci marker) were pushed together in ONE `git push`. GitHub evaluates the skip-ci
      marker against the PUSH's head commit only, not per-commit -- since 9ccaf2d (carrying
      the marker) was HEAD at push time, the ENTIRE push's workflow triggering was suppressed,
      including e2e-full.yml/publish.yml against the code commit this round actually needs
      live-verified (confirmed: zero new check-runs/workflow-runs for either SHA, `gh api .../
      dfdacd5/check-runs` returned `total_count: 0`). SECOND TRAP live-discovered fixing the
      first: GitHub's skip-ci detection is a substring match against the WHOLE commit message,
      not a trailer -- a follow-up commit (68bc3e4) whose body merely DESCRIBED the marker in
      prose (quoting it to explain what happened) was ALSO skipped, because the literal
      bracketed text was still present as a substring anywhere in the message. Corrected by a
      SECOND follow-up commit that describes this incident WITHOUT ever reproducing the
      literal bracketed marker text itself (see this very field -- deliberately phrased as
      'the skip-ci marker' throughout, never spelled out) -- see live_verification_state below
      for the retrigger commit SHA and the resulting authoritative run IDs. Convention going
      forward, TWO rules: (1) when a round's code+docs commits are pushed in the SAME `git
      push` invocation, mark NEITHER -- the marker only affects a push whose OWN head commit
      carries it, so a docs-only commit should get it ONLY when pushed strictly on its own, in
      a SEPARATE `git push` from any code commit that still needs to trigger CI; (2) NEVER
      spell out the literal marker text inside a commit message that is not itself intended to
      be skipped, even in an explanatory/quoting context -- GitHub's detector does not
      distinguish a live directive from a description of one."
  next_action: "CHECKPOINT REACHED (human-action) with the authoritative e2e-full.yml run ID
      (+ companion publish.yml run ID as item 0) recorded in live_verification_state below --
      session manager runs the single 60s watcher. Do NOT self-watch."

ROUND 18 (2026-08-28, opened on user decision confirming the FINAL targeted round exactly as
recommended -- fix (24) + (26) diagnostics rider + three accepted-behavior/test-budget
dispositions; ceiling stays 190 -- SUPERSEDED BY ROUND 18 OUTCOME BELOW):
  charter: "(1) FIX (24) at the test layer: repoint sweep assert-4 to
      normalized.customers -- the test's own comment specifies the proof is
      verbatim backdated event_ts retention in normalized.customers (SCD2
      retains every version boundary; the corpus reuses the SAME 50
      customer_ids daily so the one-row-per-key silver slot deterministically
      goes to the newest business-ts -- R17-adjudicated platform-correct
      behavior). Keep the forensics rider attached to the new failure path.
      (2) DIAGNOSTICS RIDER (26): poll_ingestion_run selects + returns
      error_type/error_message (and streams them in its timeout failure
      message); the four slice-suite SUCCEEDED asserts that consumed R17's
      FAILED shapes (idempotent_reupload, podkill, dbtkill, u3, orphan)
      include the error fields in their assert messages; e2e-full.yml's
      end-of-job dump gains an error-bearing-rows query (FAILED/QUARANTINED +
      any error_message-bearing rows, with error_type + left(error_message,
      500)). (26) is the only genuinely unexplained residue -- this makes the
      two 120-row fixture failures adjudicable next run.
      (3) DISPOSITION podkill: _RETRY_TIMEOUT_SECONDS 600 -> 900 with the
      R17-measured arithmetic recorded in the comment (designed ~5.5min (20a)
      lease wait + measured ~5.6min restage+dbt+publish of 1M = ~11.1min
      total observed; it missed 600s by ~66s; 900 budgets the designed
      mechanism honestly).
      (4) DISPOSITION dbtkill/u3/orphan/idempotent_reupload discovery waits:
      bounded orders-DagRun-queue-idle drain helper
      (wait_for_orders_dagrun_queue_idle, slice conftest, <=600s, clear
      failure message naming the still-active DagRuns) called BEFORE each
      test's upload -- keeps the 180s discovery budgets honest instead of
      inflating them (R17 proof: the whole family's failures were queue-drain
      latency behind podkill's 10.5-min slot occupancy, zero FailedScheduling
      in the starved windows).
      (5) DISPOSITION rebuild: _wait_for_all_raw_files_settled converted from
      a fixed 1800s deadline to a monotonic-progress assertion -- fail only
      if the observed (filename -> status/rows_read) snapshot shows ZERO
      change across a 600s stall window; keep a 3600s hard cap so a genuine
      wedge still fails legibly (R17: 9/16 STAGED at 1800s with real forward
      progress throughout -- the budget, not the mechanism, was the failure)."
  pre_registered_criteria:
    - "(a) sweep assert-4 PASSES (normalized.customers holds the verbatim
      backdated event_ts as the late member's earliest version boundary)."
    - "(b) dbtkill/u3/orphan/idempotent_reupload clear via the drain helper
      (discovery inside 180s once the queue is idle at upload time)."
    - "(c) podkill passes within the 900s terminal wait."
    - "(d) rebuild's monotonic-progress assertion holds (settles with
      progress never stalling 600s, inside the 3600s hard cap)."
    - "(e) (26) either passes or fails WITH the streamed
      error_type/error_message making it adjudicable."
    - "(f) Zero NEW failures vs the R17 census."
    - "(g) Guards green: Kyverno 0, restarts 0, zero-failed-TIs class holds,
      fixes 16-23 + (25)-A/B hold, scheduler under 2560Mi."
    - "TARGET: fully green census, or green-except-(26)-with-adjudicable-
      evidence, inside the 190-min ceiling."
  reasoning_checkpoint:
    hypothesis: "R17's 7 residual failures decompose into (i) one
        adjudicated test-layer artifact ((24): assert-4 queries silver for
        rows that D-05 semantics correctly place in normalized), (ii) five
        test-budget shortfalls of a proven-sound mechanism (podkill missed
        600s by 66s with the full kill->lease->restage->dbt->publish cycle
        live-measured at ~11.1min; the dbtkill/u3/orphan family is pure
        queue-drain knock-on; rebuild showed continuous forward progress at
        1800s), and (iii) one genuinely unexplained application-level
        failure ((26): 120-row fixtures FAILED pre-schema with the error
        recorded only in the never-dumped meta.ingestion_runs error columns)
        -- so repointing (24), rebudgeting (ii) honestly, and instrumenting
        (iii) yields a fully green census or an adjudicable (26)."
    confirming_evidence:
      - "R17 forensics block (streamed live): bronze holds run 39's 50 rows,
        run SUCCEEDED, ledger claimed it, every late-file key in silver
        attributed to runs 47/48 (day-12/13 snapshots) -- assert-4's silver
        query is provably the wrong table per the test's own comment."
      - "R17 timing: podkill publish done ~07:52:40 vs deadline 07:51:34
        (66s over); zero FailedScheduling during the dbtkill/u3 starved
        windows; rebuild 9/16 STAGED at -33% requeue mass (R16: 2/16)."
      - "R17 (26): runs 62/438/439 all FAILED pre-schema (schema_version_id
        NULL), zero failed TIs, zero pod warnings => application-level error
        recorded only in meta.ingestion_runs error columns, which no
        diagnostic captures."
    falsification_test: "Live run: sweep assert-4 failing against
        normalized.customers refutes the (24) adjudication (would mean the
        backdated version boundary is genuinely absent -- a real platform
        bug); podkill missing 900s refutes 'mechanism sound, budget short';
        the dbtkill family still starving WITH the queue drained at upload
        refutes the knock-on model; rebuild stalling 600s with zero snapshot
        change refutes 'progress was continuous'; (26) recurring with an
        empty error_message refutes 'the error is recorded at application
        level'."
    fix_rationale: "(24) moves the assertion to the layer its own comment
        specifies -- no platform change. (3)/(4)/(5) encode ACCEPTED designed
        behavior into test budgets (lease wait is design, queue serialization
        is design, rebuild progress is design) instead of masking or
        inflating blindly -- the drain helper keeps budgets honest rather
        than raising them. (26)'s rider is instrumentation-only: no behavior
        change, purely making the next failure adjudicable."
    blind_spots: "(1) (26)'s mechanism stays unexplained by design this
        round -- the rider only makes it adjudicable. (2) The 600s stall
        window for rebuild tolerates the observed ~5min quiet gaps but a
        legitimate >10min quiet gap (e.g. an extreme queue-drain pause)
        would fail the stall assertion -- the hard cap and stall message
        make that legible if it fires. (3) The drain helper waits on
        queued/running DagRuns only -- a DagRun created AFTER the drain
        returns (cron asset event racing the upload) can still occupy the
        slot; bounded residual risk, same class R17 measured at ~50s.
        (4) scd_concurrent's 5.1->16.5min variance stays a watch item."
  offline_status: "COMPLETE 2026-08-28: all five charter items implemented;
      battery green at bare-HEAD parity (ruff/format: only pre-existing HEAD
      drift; unit+regression 564 passed incl. 4 NEW drain-helper unit tests;
      policy 2 pre-existing failures only; dagtest 14 passed; manifests +
      kubeconform valid; mypy clean; slice collection clean; workflow YAML
      valid). No production-code changes this round -- test + workflow layer
      only. See the ROUND 18 offline Evidence entry."
  live_verification_state: "RECORDED 2026-08-28T10:5xZ: ROUND 18 pushed as
      docs commit ce3b3a6 ([skip ci], buried) + code commit 4818867 (HEAD,
      clean message -- docs FIRST / code LAST, single push, workflows
      trigger at 4818867). AUTHORITATIVE ROUND 18 live-verification run:
      e2e-full.yml run 33164806655 (headSha 4818867, created 10:49:21Z).
      Companions same headSha: publish.yml 33164806681 (criterion 0: all 3
      images must push before cluster-up's pull window -- NOTE this round's
      changes ride the CHECKOUT only [tests + workflow], no image-borne
      change, so a publish/pull race cannot mask any ROUND 18 fix);
      CI 33164806656 (expected FAILURE -- the pre-existing offline
      Quality-gate+Integration pattern, bare-HEAD differentials confirmed
      again this round: 2 policy failures + 2 sweep lint errors + 2 format-
      drift files, all present on stashed HEAD); e2e-chaos 33164806660
      (observational). Analysis criteria = pre_registered_criteria above
      ((a) sweep assert-4 passes; (b) dbtkill/u3/orphan/idempotent clear
      via the drain helper; (c) podkill inside 900s; (d) rebuild's
      monotonic-progress assertion holds; (e) (26) passes or fails WITH
      streamed error_type/error_message; (f) zero new failures vs R17;
      (g) guards green). TARGET: fully green census, or green-except-(26)-
      with-adjudicable-evidence, inside the 190-min ceiling. Scratchpad
      convention: save the job log as round18-job.log."
  next_action: "SUPERSEDED -- see ROUND 18 OUTCOME below."

ROUND 18 OUTCOME (2026-08-28, post-run analysis of run 33164806655 -- CURRENT STATE):
  run: "e2e-full.yml 33164806655, headSha 4818867, conclusion FAILURE, job
      10:49:25Z->13:58:41Z = 3h09m16s = 189.3min (self-terminated, NOT cancelled -- 0.7min
      inside the 190-min ceiling, the tightest margin of the whole session). Suite
      10866.63s (3:01:06). Log saved as scratchpad round18-job.log (15,674 lines)."
  census: "7 failed / 31 passed / 6 skipped -- SAME COUNT as R17 but a DIFFERENT
      composition (not the same 7 test node-IDs recurring unchanged). Fix (24) CONFIRMED
      WORKING: sweep assert-4 now PASSES against normalized.customers. Fix (26)'s rider
      CONFIRMED WORKING but returned zero rows (no FAILED/QUARANTINED/error-bearing run
      existed this round -- see criteria_adjudication (e) below on why that is itself
      informative, not merely silent). Dispositions (3)/(4)/(5) (podkill 900s, drain
      helper, rebuild stall-assertion) all fired exactly as designed and produced clean,
      fast, honestly-labeled failures -- but did not clear the underlying podkill
      mechanism, which got WORSE (full 900s stall vs R17's 66s miss). Failed:
      test_full_2year_sweep_customers_and_orders (NEW failure point -- assertion (10),
      DELETE-detection, never reached before), test_pod_kill_mid_load_produces_no_duplicates
      (genuinely wedged the full 900s, STAGED, error_type/error_message both None),
      test_pod_kill_mid_dbt_build_produces_no_duplicates (drain helper: podkill's own
      DagRun still 'running' after 600s), test_u3_throughput_and_peak_rss_baseline (SAME
      drain-helper cascade, NEW to this specific census slot but same root cause),
      test_rebuild_from_raw_reconciles_and_reverts_quarantine_to_pending (NEW shape: the
      monotonic-stall assertion fired -- 14/14 orders files ZERO progress for the full
      600s stall window, not the old 1800s-budget-exhaustion shape),
      test_referential_orphan.py::test_orphan_order_quarantined_while_valid_rows_publish
      (poll_ingestion_run 180s timeout, STAGED, same cascade one test later),
      test_idempotent_reupload (drain helper: a DIFFERENT active DagRun set this time --
      podkill's own run had already left 'running' by this point in the suite, replaced
      by a fresh post-timeout backlog)."
  criteria_adjudication:
    - "(a) sweep assert-4: MET. Fix (24) confirmed live -- the test's forensics-adjacent
      assertion (4) no longer fires; the test now runs to a LATER assertion never
      reached in any prior round."
    - "(b) dbtkill/u3/orphan/idempotent_reupload clear via the drain helper: NOT MET, but
      the drain helper's OWN mechanism is confirmed sound and doing exactly its
      documented job -- all four failures carry direct, legible evidence naming
      podkill's own DagRun ('e2e-podkill-59f13a4ddbe9') or its aftermath as the blocker,
      not an inscrutable timeout. This is criterion (f)'s knock-on model CONFIRMED again,
      one layer downstream of R17's adjudication (see dbtkill/u3/orphan/idempotent
      cascade evidence below)."
    - "(c) podkill inside 900s: NOT MET, and WORSE than R17 -- R17 missed the 600s budget
      by 66s after completing the full kill->lease->restage->dbt->publish cycle; R18
      never reached a terminal ETL status at all inside 900s (STAGED, zero error). The
      900s budget was not the limiting factor this round; the DagRun itself deadlocked
      at the Airflow level."
    - "(d) rebuild's monotonic-progress assertion: NOT MET in the sense of passing, but
      MET in the sense of firing EXACTLY as designed -- a genuine, unambiguous, unbounded
      stall (0 files of 14 progressing for the full 600s window) is precisely the failure
      mode this assertion was built to catch cleanly, replacing R17's uninformative
      '9/16 STAGED, 1800s exhausted, no clear stall-vs-slow distinction' shape."
    - "(e) (26) passes or fails WITH adjudicable error evidence: PARTIALLY MET -- the
      rider correctly executed and correctly found ZERO error-bearing rows, which is
      itself the adjudicating signal: this round's failure has NO FAILED/QUARANTINED
      row and NO non-null error_message anywhere (confirmed by the end-of-job dump,
      '(0 rows)'), which is DIRECT evidence the podkill wedge is an Airflow-level
      DagRun/TaskInstance-layer event (dagrun_timeout + SKIPPED tasks), never surfaced to
      the application layer at all -- (26)'s rider was scoped to catch FAILED/
      QUARANTINED/error-message rows, and this failure shape structurally cannot produce
      one. NAMED GAP: (26)'s query needs widening to also catch stale non-terminal rows
      (e.g. status NOT IN terminal AND age beyond some bound) to close this specific
      blind spot."
    - "(f) zero NEW failures vs R17: NOT MET literally (composition changed: assert-4
      cleared, assertion-10 is new; rebuild's failure shape changed from budget-exhaustion
      to stall-assertion; u3 and orphan's specific failure text changed) -- but every
      change is EITHER a confirmed fix (24) working, OR the SAME root mechanism
      (podkill's DagRun) surfacing through a NEWLY HONEST diagnostic (drain helper,
      stall assertion) instead of the old inscrutable timeout shapes. No failure this
      round is unexplained by an already-understood mechanism except the two genuinely
      new items handled above: assertion (10) (strong hypothesis, forensics-first
      ROUND 19 action recommended) and podkill's qualitatively-worse wedge (root-caused
      below with strong, source-confirmed, not-yet-100%-closed evidence)."
    - "(g) guards green: MET. Kyverno 0 denials (only the deliberate PASSED unsigned-image
      test), restarts 0 all roles (empty restart-change timeline), scheduler peak
      1,891,291,136B = 1804MiB = 70.5% of 2560Mi (R17: 1776MiB -- stable, no growth
      trend), ZERO OOM/crash-loop this round -- directly ruling OUT the ROUND-3-era
      adopt_or_reset_orphaned_tasks livelock (M1) as this round's mechanism (that
      mechanism requires a scheduler restart to manifest; this scheduler never
      restarted)."
  podkill_root_cause_investigation: "Direct source read of the INSTALLED apache-
      airflow==3.3.0 on the LOCAL persistent cluster (kubectl exec into
      deploy/airflow-scheduler, same version as CI's ephemeral cluster) --
      scheduler_job_runner.py's _schedule_dag_run() (line ~2789) confirms the
      dagrun_timeout check itself (`dag_run.start_date and dag.dagrun_timeout and
      dag_run.start_date < utcnow() - dag.dagrun_timeout`) has NO run_type gate and no
      dependency on task progress -- on firing it unconditionally sets
      dag_run.state=FAILED and every unfinished TaskInstance (state in
      State.unfinished or None) to SKIPPED, then flushes. This is a hard, DB-state-
      driven ceiling, independent of task retries, confirmed sound and already
      live-verified working elsewhere in this SAME session (ROUND 3's fix; root cause 14's
      'backfill__05:31 ... dagrun_timeout killed it at 14:36:44'). Timeline
      reconstruction from the test's own poll budgets: kill at ~12:14:44 (900s before
      the 12:29:45 test failure), DagRun creation no later than that (likely
      ~12:07-12:14, consistent with etl-monitor's first podkill-window stage pod at
      12:10:59) -- so dagrun_timeout=45min should fire ~12:52-12:59. Direct etl-
      monitor.log pod-lifecycle evidence in that exact window: stage-lqu9he3u Running
      12:21:45-12:22:55 (~70s, consistent with fix 20a's ~5.5min lease-wait + restage),
      dbt-build-eleymmox Running 12:23:30-12:24:24 (~1min), then publish-n1l2gcsv
      Running 12:24:42-12:25:17 followed by publish-n1l2gcsv Failed at 12:25:35 -- a
      REAL publish pod failure, ~11min post-kill (matching R17's OWN successful timing
      almost exactly, except this time publish itself crashed instead of merely running
      long). publish has retries=3/retry_exponential_backoff=True with this project's
      stock 5min-base uncapped-multiplier retry_delay (confirmed via grep, no override) --
      a failed first attempt's exponential backoff plausibly never reaches a second
      attempt before the ~45min dagrun_timeout ceiling arrives, at which point
      dagrun_timeout fires, marks the DagRun FAILED, and SKIPS publish's still-
      up_for_retry TaskInstance -- but publish's own KPO pod never got far enough on its
      ONE real attempt to write anything to meta.ingestion_runs.error (a pod-level
      crash, not an application-level exception the dataplat code itself caught), so
      the run is orphaned forever at its pre-crash status (STAGED) with error_type/
      error_message both permanently NULL. Reconciling evidence for 'dagrun_timeout DID
      eventually fire': idempotent_reupload's OWN drain-helper failure (the LAST test in
      the run, ~13:5x) names a COMPLETELY DIFFERENT active DagRun set
      ('asset_triggered__...running', 'e2e-orphan-...queued') with NO mention of
      'e2e-podkill-59f13a4ddbe9' at all -- meaning the podkill DagRun genuinely left
      'running' state SOMETIME between orphan's failure (13:47:24) and idempotent's own
      check, freeing the global stage/dbt_build/publish slot for a fresh backlog to
      occupy instead (exactly what a ~45min-after-creation dagrun_timeout firing would
      produce). meta.ingestion_runs.status='STAGED' persisting in the FINAL end-of-job
      dump (13:58:36) is fully consistent with this: the ETL-layer status is
      independent of and never updated by the Airflow-layer DagRun/TaskInstance state
      change. HONEST GAP: this is NOT yet closed by direct, first-hand evidence -- no
      raw scheduler stdout/stderr log survives in this round's diagnostics (only a
      customers-filtered scheduler-log grep exists; there is no orders-DagRun/
      TaskInstance dump equivalent to the customers one already in the end-of-job
      dump), and no pod-level crash reason for publish-n1l2gcsv was captured before K8s
      garbage-collected it. The 'publish pod crashed, retries never got a second shot
      inside 45min' story is the single most evidence-consistent reconstruction
      available, not a directly-observed fact."
    dbtkill_u3_orphan_idempotent_cascade_confirmation: "Direct evidence, no inference
      needed: dbtkill's and u3's own failure text is
      wait_for_orders_dagrun_queue_idle's own assertion, naming
      'e2e-podkill-59f13a4ddbe9' (state 'running') as the still-active DagRun at
      12:39:47 and 12:49:49 respectively -- both squarely inside the pre-dagrun_timeout
      window this analysis reconstructs (created ~12:07-12:14, timeout ~12:52-12:59).
      orphan's later failure (13:47:24) is a plain poll_ingestion_run 180s timeout
      (STAGED) on ITS OWN upload -- meaning orphan's OWN drain-helper call passed (the
      queue WAS idle by orphan's own upload time, most likely just after
      dagrun_timeout finally released podkill's slot), but orphan's own file then
      queued behind whatever backlog remained and could not clear its own 180s
      discovery/stage budget -- same root mechanism (podkill's abnormally long, ~45min
      vs R17's ~11min, slot occupancy), one hop further downstream, not a
      separate bug. idempotent_reupload (last test) shows the slot handed to an
      entirely different DagRun set, confirming the queue kept churning through a
      backlog well past podkill's own release. CONFIRMED: dbtkill, u3, orphan, and
      idempotent_reupload are pure downstream noise from podkill's single root wedge;
      none needs an independent fix."
  sweep_assertion_10_delete_detection_finding: "STRONG HYPOTHESIS, same class as
      already-adjudicated (24), NOT forensically confirmed this round (no forensics
      rider was attached to this specific assertion -- (24)'s rider is scoped only to
      assert-4's own failure text, which no longer fires). Direct source read of
      packages/dataplat/src/dataplat/scd/delete_detection.py confirms
      find_vanished_customer_ids' own DELETE-detection is correctly scoped to
      staged_run_ids ('THIS pass's own staged run ids') read from staging.customers
      (bronze) -- the ROUND-12-fixed, currently-correct mechanism. The test's own
      module docstring confirms this is a BACKFILL test: all 13 corpus files are
      uploaded up front and processed via a multi-tick backfill window, with fix (21)'s
      own 'publish_ingest claims ALL currently-STAGED runs' batching semantics (the
      SAME mechanism (24) was ultimately adjudicated against) meaning a single publish
      pass's staged_run_ids can legitimately span MULTIPLE days' files if more than one
      became STAGED before any publish ran -- plausible under this same round's
      confirmed CPU-contention/queue-drain conditions. If the FINAL day's publish pass
      batched an EARLIER day's run_id (the one still carrying customer_id=2100100032)
      together with the final day's own run_id, find_vanished_customer_ids' bronze
      snapshot would correctly (by the platform's actual, batch-scoped semantics)
      include that customer as 'staged this pass' and correctly NOT invalidate it --
      the exact same 'test assumes one-file-per-publish-pass granularity; the platform
      actually operates at whatever-is-STAGED-when-publish-runs granularity' mismatch
      (24) was. Recommended ROUND 19 action: extend (24)'s exact forensics-rider
      pattern to this NEW assertion -- on failure, dump the FINAL day's own publish
      pass's staged_run_ids (via meta.dbt_processed_runs claims for customers), whether
      customer_id=2100100032 appears in staging.customers for any run_id in that batch,
      and which day/file each such run_id maps to. Do NOT repoint or weaken the
      assertion blind; confirm the batching mechanism with the SAME evidence standard
      (24) was held to before deciding whether this is a test-layer artifact (like (24))
      or a genuine DELETE-detection bug."
  duration_and_ceiling_risk: "189.3min self-terminated inside the 190-min ceiling by
      only 0.7min -- the tightest margin of the whole session (R17: 2:37:34 suite,
      well inside; R16: 2h20m16s). The drain helper's bounded 600s waits and the
      rebuild stall-assertion's 600s window both fail FAST relative to the old
      180s/1800s shapes they replaced, which is why this round finished at all despite
      podkill's DagRun consuming its FULL 900s test-budget PLUS an additional ~35min of
      real wall-clock dagrun_timeout wait baked into the suite's actual execution
      before the drain helper ever got a chance to observe it clear. NEW RISK: if
      ROUND 19 does not shorten podkill's own wedge duration, the ceiling margin has
      essentially zero further slack -- any additional dagrun_timeout-driven wait
      elsewhere would very plausibly push a future run over 190min."
  next_action: "DECISION CHECKPOINT returned to user: root cause of the podkill wedge
      is strong but not forensically closed (no raw scheduler log, no orders-DagRun/TI
      dump, no crash reason for the failed publish pod survive this round's
      diagnostics); assertion (10)'s DELETE-detection finding is a strong same-class
      hypothesis as (24), also not forensically confirmed. Recommending a
      diagnostics-first ROUND 19 (mirroring this session's own established (26)
      precedent: instrument before fixing blind) rather than another guess-and-verify
      cycle on the platform's single largest correctness-risk area (podkill). Carried
      follow-ups unchanged: sidecar mirror, stage-side RejectionRateCircuitBreaker
      classification, teardown-race flake class, v_run_recovery wording, ADR-0012's
      deferred silver disposition, merge.py delta-scoping before any dataset adopts
      strategy 'merge', scd_concurrent duration-variance watch."

ROUND 17 OUTCOME (2026-08-28, post-run analysis of run 33147620963 -- SUPERSEDED BY ROUND 18 ABOVE):
  run: "e2e-full.yml 33147620963, headSha 79dd299, conclusion FAILURE, job
      06:21:03Z->09:06:11Z = 2h45m08s (finished under its own steam; suite
      9454.05s = 2:37:34), well inside the 190-min job ceiling. Log saved as
      scratchpad round17-job.log (13,941 lines, forensics + all always()-
      diagnostics recovered)."
  census: "7 failed / 31 passed / 6 skipped -- BEST of the session (R16: 9/29/6,
      R15: 10/28/6). Newly green: test_backfill_reentry (0042's grant live: the
      previously-fatal _fetch_dagrun_identity read now passes and the test runs
      7.4min to full green) + test_no_extra_schemas_exist ('normalized'
      allowlisted). The 7 failures are an exact subset of R16's 9 -- ZERO new
      failing node-IDs. Failed: full_2year_sweep (assert-4, (24) -- now
      ADJUDICATED), podkill_mid_load (600s terminal wait, last STAGED),
      dbtkill + u3 + orphan (180s discovery starvation), rebuild (15/16
      unsettled at 1800s, 9 STAGED + 6 PENDING, dbtkill's file settled FAILED),
      idempotent_reupload (NEW SHAPE: first upload's run FAILED -- finding 26)."
  criteria_adjudication:
    - "(a) NOT MET -- all 6 (25)-owned failures persist, BUT the mechanism
      moved decisively: podkill's 1M DagRun now COMPLETES end-to-end in
      ~11.1min (kill ~07:41:35 -> publish done ~07:52:40) including the
      designed ~5.5min (20a) lease wait; it missed its 600s terminal deadline
      (07:51:34) by only ~66s. In R16 the same DagRun NEVER completed and
      starved the pipeline 70+ minutes until rebuild. The pre-registered
      podkill residual risk fired -- by a whisker."
    - "(b) MET AND ADJUDICATED: assert-4 fired ('silver.customers has no rows
      for _run_id=39') WITH the full forensics block streamed. VERDICT:
      finding (24) is a TEST-LAYER ARTIFACT; the platform is CORRECT. Proof
      from the forensics: [b1] bronze holds run 39's 50 rows; [b2] run 39
      SUCCEEDED (replay_of=7); [a] ledger claimed run 39 (txid 1291 @
      06:41:48); [c] every one of the late file's 50 business keys IS in
      silver, attributed to runs 47/48 (files 11/12 = day-12/13 snapshots,
      event_ts 2024-01-12/13) -- the corpus reuses the SAME 50 customer_ids
      every day, so the day-8 late file's backdated rows deterministically
      LOSE every one-row-per-key silver slot to newer business timestamps.
      That is exactly D-05's required semantics (business-ts ordering wins).
      Test defect 1: the docstring/comment states the proof belongs in
      normalized.customers (verbatim backdated event_ts retention) but the
      code queries silver.customers -- wrong table. Test defect 2
      (sequencing): the corpus files were already terminal from the pilot
      era, so both dataset waits drained instantly and assert-4 ran against
      pilot-era attempt history -- the sweep's own backfill-3 DagRuns started
      06:54:04, ELEVEN SECONDS AFTER the test failed at 06:53:53."
    - "(c) MET: reentry green (0042 applied at cluster-up 06:27:49, alembic
      line '0041 -> 0042'), no_extra_schemas green."
    - "(d) MET: zero new failures vs R16."
    - "(e) MET: Kyverno 0 denials (only the deliberate unsigned-image test,
      PASSED); restarts 0 all roles (restart-change timeline empty); ZERO
      failed TIs (1492 customers TIs dumped, 0 failed); 66/66 customers
      DagRuns success; scheduler peak 1,862,717,440B = 1776MiB = 69.4% of
      2560Mi (R16 1786MiB -- stable); stage_cpu_request 200m in force; all
      images at 79dd299 (publish pods pulled csv-processor:79dd299...);
      fixes 16-23 hold (stage>>dbt_build>>publish ordering visible in TI
      history, mass_delete PASSED with exclusions, zero QUARANTINED-not-
      SUCCEEDED, zero SKIPPED_CONCURRENT)."
    - "(f) MET AT MECHANISM LEVEL, NOT SUITE LEVEL: direct O(delta) live
      measurement -- podkill's post-restage dbt_build+publish on 1M rows took
      <=2min (publish pod publish-s4hc01fs running <=07:51:33 -> gone by
      07:52:46) vs R16 where the same segment did not finish in the residual
      600s and the DagRun ran 56+min more without completing. Rebuild-era
      stage throughput: 9/16 files STAGED inside the 1800s settle vs R16's 2
      STAGED at -33% re-queue mass. Suite TOTAL went UP 24.7min (2:37:34 vs
      2:12:50), fully decomposed per-test: scd_concurrent +11.4min (5.1 ->
      16.5, PASSED both rounds -- contention variance, watch item), reentry
      +5.1min (R16 died at 2.3min on the grant error; R17 runs the whole test
      to green = new coverage, not regression), rebuild +3.9min, live_run
      +2.0min, podkill itself -1.5min, everything else within +-1min."
    - "(g) NOT MET (not green) but: first complete census with every failure
      NAMEABLE, 2:37:34 << ceiling, failure mass 9 -> 7."
  dbtkill_slot_holder_adjudication: "User's live observation CONFIRMED in
      substance, with one refinement. During dbtkill's 180s discovery window
      (~07:51:50 -> 07:55:00) the orders max_active_runs=1 slot was held first
      by PODKILL'S OWN 1M DagRun -- direct evidence: podkill's run key
      64184a77... observed STAGED at 07:51:34 while publish pod
      publish-s4hc01fs ran (07:51:33 -> gone by 07:52:46), i.e. the DagRun
      released the slot ~07:52:40, ~50s into dbtkill's window. The REMAINDER
      of dbtkill's window and all of u3's (07:55 -> 07:58:21) were consumed by
      the serial drain of orders DagRuns QUEUED during podkill's 10.5-min
      occupancy: customers cron completed ~5 publishes 07:43:31-07:52:57, each
      firing an asset event -> ~5 queued orders DagRuns ahead of dbtkill's
      manual trigger (created ~07:51:50, FIFO behind them). No orders discover
      pod executed before 07:58:21; the etl namespace was pod-sparse/idle
      07:53-07:56 with ZERO FailedScheduling events in that window -- the gap
      is DagRun-queue drain latency, not CPU saturation. So yes: this is the
      flagged podkill residual-risk zone (600s window), and the entire
      dbtkill/u3 failure family is knock-on from podkill finishing ~66s late.
      orphan (08:58 -> 09:01:35) is the same starvation class in the
      post-rebuild backlog era (15 re-queued files churning; orphan's own
      cleanup then deleted its object at line 279)."
  new_finding_26: "Three orders ingestion runs terminally FAILED at the
      pre-schema phase (schema_version_id NULL): runs 62 AND 438 = dbtkill's
      120-row file (two different eras: rebuild re-drive wave + post-rebuild
      wave 437-453), run 439 = idempotent_reupload's fresh 120-row fixture
      (the test's new failure shape: poll returned terminal FAILED within
      ~3min of upload). Zero failed TIs, zero pod-level warnings (no OOM /
      eviction / BackOff / admission denial in the final-events window) =>
      the failure is APPLICATION-LEVEL, recorded only in
      meta.ingestion_runs.error -- which NO diagnostic dump captures. Pattern:
      both distinct FAILED files are exactly the 120-row fixtures
      (_SMALL_ORDERS_ROWS=120, _IDEMPOTENT_FIXTURE_ROWS=120) while 50-row
      dated, 250k and 1M files staged clean in the same eras; fixture
      builders were NOT touched by 79dd299 (conftest unchanged) and dbtkill's
      same file reached STAGED in R16. Mechanism UNADJUDICABLE this round.
      NAMED DIAGNOSTICS GAP: end-of-job ingestion_runs dump omits the error
      column; poll_ingestion_run's assertion prints only the status."
  recommendation_for_user_decision: "ROUND 18 as a FINAL targeted round,
      splitting one real fix, one diagnostics rider, and three explicit
      test-budget dispositions: (1) FIX (24) at the test layer -- repoint
      sweep assert-4 to normalized.customers (verbatim backdated event_ts,
      exactly what its own comment specifies), keep the forensics rider;
      optionally gate the waits on the sweep's OWN backfill runs.
      (2) DIAGNOSTICS RIDER for (26) -- stream meta.ingestion_runs.error in
      poll_ingestion_run's failure message + add the error column (FAILED
      rows) to the end-of-job dump; (26) is the ONLY genuinely unexplained
      residue and cannot be fixed blind. (3) DISPOSITION podkill as
      test-budget: raise the terminal wait 600 -> 900s (the designed 5.5-min
      lease wait leaves ~4.5min for a 1M restage+dbt+publish on ~3 CPU; it
      missed by 66s -- the mechanism is proven sound). (4) DISPOSITION
      dbtkill/u3/orphan discovery waits: preferred = a bounded
      orders-queue-idle drain helper before upload (keeps per-test budgets
      honest); alternative = raise 180 -> ~480s. (5) DISPOSITION rebuild: on
      the CI profile either raise settle 1800 -> 2700s OR convert to a
      monotonic-progress assertion (recommended: progress assertion --
      ceiling arithmetic is tight: green-path projection with raised budgets
      ~= 157min + up to ~30min of newly-waited completions ~= 185-190min vs
      the 190-min ceiling). Watch item, no action: scd_concurrent 5.1 ->
      16.5min variance."
  next_action: "DECISION CHECKPOINT returned to user: confirm the ROUND 18
      shape (fix (24) + (26) rider + dispositions 3/4/5) or redirect. Carried
      follow-ups unchanged: sidecar mirror, stage-side
      RejectionRateCircuitBreaker classification, teardown-race flake class,
      v_run_recovery wording, ADR-0012's deferred silver disposition,
      merge.py delta-scoping before any dataset adopts strategy 'merge'."

ROUND 17 (2026-08-28, opened on user decision (25) A+B + confirmed 22b/22c/24-rider -- SUPERSEDED BY ROUND 17 OUTCOME ABOVE):
  charter: "(1) (25)-A: delta-scope merge_orders' _PUBLISH_SQL -- merge only the
      pass's own staged_run_ids' silver rows instead of the whole silver table
      (production semantics done right; also closes the (20b) leak-vector-ii shape
      at the same layer). Red/green required, determinism preserved, quarantine
      exclusion kept. Survey other publishers, apply only where the argument holds.
      (2) (25)-B: right-size retained chaos fixtures -- shrink where not genuinely
      scale-bound, keep 1M only where it is; bound the rebuild re-queue mass and
      verify the arithmetic. (3) (22b) migration 0042 SELECT grant on
      meta.ingestion_runs for analytics_owner. (4) (22c) 'normalized' into the
      topology test's ALLOWED_SCHEMAS. (5) (24) rider: sweep assert-4 failure
      message streams the dbt_processed_runs ledger claims + late-file bronze
      census + silver attribution so a next-run failure is adjudicable despite
      rebuild destroying the DB state."
  adjudications_from_source_reads:
    - "(25)-A survey: THREE publishers. scd.py (customers) is ALREADY
      staged_run_ids-scoped everywhere except the deliberately-unscoped bronze
      history (its own F-1 design + 20b exclusion) -- no change. merge_orders.py
      is the live O(accumulated-silver) offender (staged_run_ids accepted,
      noqa'd unused) -- FIX HERE. merge.py (strategy 'merge') has the identical
      whole-table shape BUT no live dataset uses it (customers=scd since Phase
      10, orders=merge_orders; registry keeps 'merge' for future datasets) --
      LEAVE UNCHANGED, recorded as a documented follow-up if a dataset ever
      adopts 'merge'."
    - "(25)-A safety argument (why delta-scope is exact, not approximate): the
      whole-table read was compensation for INEXACT eligibility (pre-ledger,
      dbt's watermark batching could consolidate runs invisibly -- run.py's own
      docstring says 'reads silver.<dataset> unconditionally' for exactly this).
      ROUND 16's fix (21) made eligibility EXACT: publish_ingest claims ALL
      currently-STAGED runs; fix (23) orders stage>>dbt_build>>publish within a
      DagRun; the claim ledger guarantees every staged run's bronze is folded
      into silver before its own DagRun's publish; max_active_runs=1 serializes
      DagRuns. Therefore every silver row whose winner _run_id is in
      staged_run_ids is exactly 'this pass's delta', and every key whose winner
      is an older run is already published by that run's own pass. Silver is one
      row per business key (delete+insert, unique_key=order_id), so the delta
      predicate `_run_id = ANY(staged_run_ids)` selects precisely the keys whose
      state changed."
    - "(25)-B verification per test's own assertions: podkill_mid_load KEEPS 1M
      (kill-window genuinely scale-bound -- heartbeat-poll kill must land
      mid-COPY with margin; local U3 rate 41,946 rows/s => 250k stages in ~6s,
      too slim vs 0.5s poll + kubectl latency; 1M gives ~24s locally, minutes on
      CI; plus (20a)'s restage-at-scale proof distinct_keys=1M). u3 SHRINKS
      1M -> 250k (assertions are throughput>0 + peak>0 -- rate-based, NOT
      scale-bound; steady-state dominated at 250k under CI contention; the doc
      self-describes its fixture so the baseline stays honest). dbtkill: user
      premise corrected -- it is ALREADY 120 rows (_SMALL_ORDERS_ROWS; the kill
      target is the dbt_build pod, a big file buys nothing). concurrent_select
      already 250k (R16). Rebuild re-queue arithmetic: retained orders corpus
      before rebuild = 12 dated (~600) + concurrent 250k + podkill 1M + dbtkill
      120 + u3 250k ~= 1.50M rows, down from R16's ~2.25M (-33%); with (25)-A
      the rebuild-era publish work is sum-of-deltas (~1.5M total) instead of
      N-passes x O(accumulated) -- the multiplier, not the mass, was R16's
      dominant sink."
    - "(22b): reentry's _fetch_dagrun_identity reads meta.ingestion_runs via
      analytics_owner_connection (test line 707); 0040 already granted
      analytics_owner SELECT on meta.dbt_processed_runs, 0038 on
      meta.files/meta.datasets -- 0042 is the same one-liner for
      meta.ingestion_runs. Red/green via has_table_privilege in
      test_migrations.py (0638-precedent shape)."
    - "(22c): ALLOWED_SCHEMAS (tests/e2e/cluster/test_postgres_topology.py:45)
      lacks 'normalized' -- information_schema.schemata lists a schema only when
      the connecting role holds a privilege on it, and 0039's USAGE grant made
      normalized newly visible. One-line test-side fix."
    - "(24) rider: sweep failure line 1289 (late_silver_row is None) becomes a
      pytest.fail with a lazily-built forensics block: (a) meta.dbt_processed_runs
      claims for dataset customers, (b) bronze (staging.customers) run census for
      the late file's file_id + all meta.ingestion_runs rows for that file_id,
      (c) silver.customers attribution for the late file's business keys. Built
      ONLY at failure time; streams via the -v/traceback rider."
  pre_registered_criteria:
    - "(a) The 6 (25)-owned failures clear: podkill_mid_load, dbtkill
      (podkill_mid_dbt), u3, rebuild, orphan, idempotent_reupload all PASS."
    - "(b) sweep assert 4 passes OR fails WITH the adjudicable forensics block
      streamed."
    - "(c) (22b) reentry passes its _fetch_dagrun_identity read; (22c)
      no_extra_schemas passes with 'normalized' allowed."
    - "(d) Zero NEW failures vs the R16 census."
    - "(e) Guards green: Kyverno 0, restarts 0, zero-failed-TIs class holds,
      fixes 16-23 hold, scheduler under the 2560Mi limit."
    - "(f) Duration decomposition shows meaningful shortening from O(delta)
      publish (R16 suite 132.8min; expect the podkill-era and rebuild-era
      publish/dbt sinks to shrink)."
    - "(g) TARGET: first fully green census, or nameable-stragglers-only,
      inside the 190-min ceiling."
  reasoning_checkpoint:
    hypothesis: "R16 finding (25)'s throughput collapse is dominated by the
        O(accumulated-silver) whole-table merge in merge_orders' publish (every
        pass rescans+row-locks ~all accumulated orders keys, so each retained 1M
        fixture taxes every later publish) multiplied by the serialized
        max_active_runs=1 pipe and the rebuild's full-corpus re-queue; making
        publish O(delta) and right-sizing the retained mass clears the 6
        (25)-owned failures without touching production breaker semantics."
    confirming_evidence:
      - "R16 live: podkill's 1M restage COMPLETED (run STAGED) but dbt+publish
        did not finish in the residual 600s window; every subsequent orders test
        starved behind it; rebuild 16/16 unsettled at 1800s with FailedScheduling
        = 'Insufficient cpu' only -- zero logic wedges, zero failed TIs."
      - "Source: merge_orders._PUBLISH_SQL reads {staging_table} (= silver.orders
        since 08.1-10) with NO run scoping; staged_run_ids accepted and noqa'd
        unused; scd.py by contrast is already scoped and customers cron passes
        stayed fast all round."
      - "R15 vs R16 differential: podkill PASSED in R15 on customers (scd,
        run-scoped publish) and FAILED in R16 on orders (whole-table merge) at
        the same 1M scale and same 600s budget."
    falsification_test: "Live run: podkill/u3 still timing out with the delta
        publish live and no upstream backlog refutes 'publish multiplier
        dominates' (residual would be dbt-build or COPY cost); rebuild still
        16/16-unsettled at 1800s with sum-of-deltas ~1.5M refutes the re-queue
        mass model. Offline: the lock-based no-rescan test failing on the fixed
        SQL means the delta predicate does not actually stop old-key access."
    fix_rationale: "The fix is at the mechanism's own layer: publish cost should
        scale with what the pass finalizes (its staged runs' delta), not with
        platform lifetime -- the whole-table read existed to compensate for
        pre-ledger inexact eligibility, and fix (21) removed that inexactness.
        Determinism/auditability preserved: same DISTINCT ON + tie-break, same
        ON CONFLICT guard, same quarantine NOT-IN exclusion, RETURNING unchanged.
        Fixture right-sizing only shrinks where the tested property is
        rate/mechanism-bound, never where scale is load-bearing."
    blind_spots: "(1) The residual 1M cost inside podkill's 600s budget (lease
        wait ~330s + restage + dbt + delta publish of 1M) is estimated, not
        measured -- if it misses, it is a nameable straggler whose knob (budget,
        option C) the user explicitly held back this round. (2) The equivalence
        proof covers upsert semantics on one-row-per-key silver; a future
        multi-row-per-key silver shape would need re-derivation. (3) (24)'s
        mechanism stays UNRESOLVED by design this round -- the rider only makes
        the next failure adjudicable. (4) The 2 offline policy failures are
        pre-existing and out of scope."
  offline_status: "COMPLETE 2026-08-28: all four charter items implemented;
      red/green proven for (25)-A (LockNotAvailable red on stashed pre-fix
      code, green with the delta scope) and (22b) (grant test red with 0042
      parked); (25)-B per-assertion verification recorded (podkill keeps 1M,
      u3 250k, dbtkill already 120, rebuild mass 2.25M -> ~1.50M); battery
      green at bare-HEAD parity everywhere with 3 pre-existing
      test_publish_orders TypeErrors additionally FIXED and zero new
      failures. See the ROUND 17 offline Evidence entry."
  live_verification_state: "RECORDED 2026-08-28T06:2xZ: ROUND 17 pushed as
      docs commit d2a733f ([skip ci], buried) + code commit 79dd299 (HEAD,
      clean message -- the R16 skip-marker trap avoided by committing docs
      FIRST and code LAST, single push, workflows trigger at 79dd299).
      AUTHORITATIVE ROUND 17 live-verification run: e2e-full.yml run
      33147620963 (headSha 79dd299, started 06:20:59Z). Companions same
      headSha: publish.yml 33147621001,
      SUCCESS 06:23:55Z (criterion 0: images at 79dd299 pushed ~3min after
      trigger, before cluster-up's pull window -- migration 0042 rides the
      migrate step, the delta-scoped merge_orders rides the csv-processor
      image, test changes ride the checkout); CI 33147620927 FAILURE
      (expected -- the pre-existing Quality-gate+Integration offline
      pattern, exactly matching this round's bare-HEAD differentials: 14
      pre-existing integration failures + 3 lint errors/17 reformat files,
      all present on HEAD); e2e-chaos 33147621003 (observational).
      Analysis criteria = pre_registered_criteria above ((a) the 6
      (25)-owned failures clear; (b) sweep assert-4 passes OR fails with
      the forensics block streamed; (c) reentry passes the
      _fetch_dagrun_identity read + no_extra_schemas green; (d) zero new
      failures; (e) guards green incl. scheduler < 2560Mi; (f) duration
      decomposition shows the O(delta) shortening; (g) fully green census
      or nameable stragglers, <= 190min). Pre-registered residual risk:
      podkill's 600s window (lease ~330s + 1M restage + dbt + delta
      publish). Scratchpad convention: save the job log as
      round17-job.log."
  next_action: "CHECKPOINT (human-action) returned: session manager runs the
      single 60s-interval watcher on run 33147620963, then spawn post-run
      analysis per live_verification_state. Carried follow-ups: sidecar
      mirror, stage-side RejectionRateCircuitBreaker classification,
      teardown-race flake class, v_run_recovery wording, ADR-0012's deferred
      silver disposition, merge.py delta-scoping before any dataset adopts
      strategy 'merge'."

ROUND 16 OUTCOME (2026-08-28, post-run analysis of run 33126343052 -- SUPERSEDED BY ROUND 17 ABOVE):
  run: "e2e-full.yml 33126343052, headSha 0a69dec (tree identical to code commit
      55c8e41), conclusion FAILURE after 2h20m16s (23:27:32Z -> 01:47:45Z),
      self-terminated. Job 98705301890; suite step 132.8min of the 140.2min total;
      observability + rebuild-from-raw capstone steps SKIPPED (gated on suite green).
      Log: scratchpad round16-job.log (12,700 lines). Companion publish 33126343060
      SUCCESS (all 3 images pushed 23:28:19-23:29:34, before cluster-up pulled at
      23:31:15 -- criterion 0 met, no stale-image race)."
  census: "9 failed / 29 passed / 6 skipped in 7970.98s (2:12:50) -- best census of
      the session (R15: 10/28/6). Newly GREEN vs R15: concurrent_select (orders 250k
      repoint, full discover->stage->dbt->publish->atomicity proof), dbt_silver
      (snapshot-complete customers fixture, zero quarantine), and the R15
      QUARANTINED-not-SUCCEEDED signature is EXTINCT: zero QUARANTINED terminals in
      the entire run (the only breaker trip is mass_delete's designed one, which
      PASSED with the (20b) exclusion predicates in force). Newly RED vs R15:
      test_no_extra_schemas_exist (see 22c below -- mechanical consequence of 0039
      being live). Failed node-IDs: no_extra_schemas, sweep(assert 4), reentry,
      podkill_mid_load, podkill_mid_dbt(dbtkill), u3, rebuild, orphan,
      idempotent_reupload."
  fixes_in_force_proofs:
    - "(19)-A LIVE + its mechanism CLEARED: concurrent_select PASSED (orders 250k,
      SUCCEEDED, atomic publish observed); dbt_silver PASSED (snapshot-complete
      echo, no vanished trip); reentry's original bad-row run reached SUCCEEDED
      un-quarantined (its R15 death) and the test progressed to a line R15 never
      reached. 6 of the 8 (19)-owned tests are still red but ALL with DIFFERENT
      mechanisms (grant gap x1, throughput collapse x5) -- the QUARANTINED
      signature itself is gone 8/8."
    - "(22) 0039 LIVE, proven by its own side effect: test_no_extra_schemas_exist
      now FAILS with 'unexpected schema(s): [normalized]' on the analytics_owner
      connection -- information_schema.schemata lists a schema only when the role
      holds a privilege on it, so this failure IS the observation that USAGE landed."
    - "(23) LIVE: TI history shows every customers DagRun (cron + backfill) running
      ALL stage maps -> dbt_build -> publish strictly ordered (e.g. backfill 15:15:
      stages 23:36:29-23:40:35, dbt_build 23:40:41, publish 23:41:13). Criterion (d)
      itself unadjudicated (dbtkill never reached its poll -- starved upstream)."
    - "(21) ledger LIVE (model in-image, 0040 applied, builds succeeded) but
      criterion (b) NOT met -- see finding 24."
    - "(20b) LIVE: 0041 applied; mass_delete PASSED (breaker trips, gold unmutated)
      with the exclusion predicates in the publish path; zero quarantine-lineage
      assertions failed anywhere (criterion (e) met on available evidence)."
    - "Migrations 0038->0039->0040->0041 all applied at cluster-up (streamed alembic
      log 23:34:14)."
  new_findings:
    - "(24) SWEEP ASSERT 4 STILL RED POST-LEDGER (silver.customers has no rows for
      _run_id=31, test line 1289, 23:58:24 -- run 31 = the late file
      customers_20240108's newest replay run per the drain's max-run_id query;
      asserts 1-3 all PASSED, orders counts exact at 600). The pre-registered
      falsification ('sweep assert 4 still failing refutes the (21) mechanism')
      FIRED: the global-max watermark was real but was NOT the sole mechanism
      behind this assert. Model logic re-verified by source read: claim ledger
      (txid-scoped) + existing_silver_contenders + _run_id desc tie-break SHOULD
      attribute the newest replay copy; every stage was followed by a same-DagRun
      dbt_build (fix 23). MECHANISM UNRESOLVED -- and unresolvable from this run's
      artifacts: the rebuild test DROPS/RESETS meta.ingestion_runs (run numbering
      restarts at 1 in the end-of-job dumps) and the idempotent_rerun test re-runs
      the same backfill window (try=2 overwrites sweep-era TI timings), so ALL
      sweep-era forensic state (ledger claims, bronze census, silver attribution)
      is destroyed before the always()-diagnostics run. NAMED DIAGNOSTICS GAP +
      rider: the sweep test must dump meta.dbt_processed_runs rows, the late
      file's bronze run census, and silver attribution for the late keys INTO the
      streamed assertion message at failure time."
    - "(25) ORDERS SERIALIZED-PIPELINE THROUGHPUT COLLAPSE UNDER RETAINED 1M-ROW
      FIXTURES -- owns 6 failures. Chain: podkill (00:27 SIGKILL) -> (20a) lease
      wait + full 1M re-stage COMPLETED (run observed STAGED, the R15 wedge is
      NOT back) but dbt_build+publish on 1M rows did not finish inside the
      remaining 600s poll -> test failed while the DagRun kept running -> every
      subsequent orders test's manual trigger queued behind it (dbtkill/u3
      discovery timeouts at 00:40/00:44, 180s budgets) -> rebuild (00:44-01:40)
      reset ALL runs to PENDING and re-triggered re-ingestion of the ENTIRE
      retained raw corpus (16 orders files incl. 3x1M + 250k + 12 dated =
      ~3.25M+ rows, versus the charter's ~2.35M estimate) through the same
      serialized pipe -> 16/16 unsettled at 1800s, node CPU-saturated
      (FailedScheduling census: stage/discover/publish/dbt-build pods all
      'Insufficient cpu', one stage pod 38min old at job end) -> orphan +
      idempotent_reupload starved (01:43/01:47). The pre-registered (g) risk
      fired, WIDER than predicted: not just the rebuild settle windows but the
      pre-rebuild per-test budgets. Contributing structural cost: merge_orders
      _PUBLISH_SQL publishes the WHOLE silver table each pass (already flagged
      as (20b) leak vector ii), so orders publish cost scales with ACCUMULATED
      silver mass, not the run's delta -- each retained 1M fixture makes every
      later publish slower. Zero evidence of any logic wedge: zero failed TIs,
      zero SKIPPED_CONCURRENT, files eventually progressed (dbtkill's file
      reached STAGED by rebuild-era)."
    - "(22b) analytics_owner lacks SELECT on meta.ingestion_runs: reentry died at
      _fetch_dagrun_identity (test line 707/603) with InsufficientPrivilege --
      first time any test reached this read as analytics_owner (R15 died earlier
      at the quarantine). Same grant-history family as 0038/0039; fix is a
      one-line migration 0042 (analytics_owner already holds meta.files/
      meta.datasets/meta.rejected_records SELECTs by precedent)."
    - "(22c) tests/e2e/cluster/test_postgres_topology.py ALLOWED_SCHEMAS is stale:
      'normalized' must be added now that analytics_owner holds USAGE (0039). One
      line, test-side."
  guards: "Kyverno denials 0 (only the deliberate unsigned-image test, PASSED);
      restarts 0 all roles (timeline empty); ZERO failed TIs (best ever; R15 had
      the teardown-race one); scheduler peak 1,873,281,024B = 1786MiB = 69.8% of
      the new 2560Mi limit (18b vindicated -- same absolute mass would have been
      91.5% of the old 2048Mi; R15 peaked 1923MiB/93.9%); dag-processor peak
      784MiB, triggerer 401MiB; SKIPPED_CONCURRENT 0 (docstring mentions only);
      FailedScheduling class = 'Insufficient cpu' ONLY, but no longer transient --
      it is finding 25's mechanism during the rebuild era; publish idem-rerun
      try=2s are the designed D-18 replay shape; suite 140.2min < 190 ceiling
      (obs + capstone still unexercised, skipped on suite failure)."
  next_action: "DECISION CHECKPOINT returned to user: choose the ROUND 17 shape
      for finding (25) -- (A) delta-scope merge_orders' publish (fix the O(total)
      whole-silver merge, the platform-correct move), (B) shrink retained orders
      chaos fixtures (1M -> 250k where the property is not scale-bound), (C)
      raise the per-test budgets, (D) CI-profile scope reduction -- plus the two
      one-line stragglers (22b migration 0042, 22c allowlist) and the (24)
      forensics rider on the sweep test. Carried follow-ups: sidecar mirror,
      stage-side RejectionRateCircuitBreaker classification, teardown-race flake
      class, v_run_recovery wording, ADR-0012 silver disposition."

ROUND 16 (2026-08-28, opened on user decision (19)-A + FULL scope 19+21+22+23+20b -- SUPERSEDED BY OUTCOME ABOVE):
  charter: "(1) (19)-A delivery-shape-aware fixtures: repoint the lone-file e2e tests at
      a delivery shape their dataset's contract tolerates. Production breaker semantics
      untouched; sweep module keeps full-snapshot customers coverage. (2) (21) adjudicate
      sweep D-05 lineage locally with the dbt SQL; fix at the right layer. (3) (22)
      one-line grant-vs-etl_app fix per the platform role model. (4) (23) source-read the
      run_stages DBT_BUILD write path vs the dbtkill 300s poll; fix instrumentation or
      polling assumption. (5) (20b) conscious disposition design for silver/bronze rows
      retained from QUARANTINED runs; minimal correct piece + ADR for the rest. Watch 18b
      (scheduler 93.9% high-water): bump if changes plausibly increase load. Target: first
      fully green cluster-slice-verify (or census with only nameable stragglers), <=190min."
  adjudications_from_source_reads:
    - "(19) DESIGN: the 8 tests split by mechanics. (a) Tests whose mechanics are
      dataset-generic (concurrent_select, podkill, dbtkill, u3, idempotent_reupload)
      REPOINT TO ORDERS: orders' merge_orders publish has NO snapshot-vanished check
      (R15 run 765 SUCCEEDED on a partial delivery), csv_ingest_orders supports manual
      `airflow dags trigger` (test_referential_orphan's own proven idiom), has the same
      stage heartbeat env + dbt_build + run_stages wiring. Orders fixtures are generated
      in-test (order_id offset windows disjoint from orphan test's [1e9,1.499e9)) with
      customer_ids sampled from live normalized.customers (referential barrier requires
      real parents; orphan test's _existing_customer_ids precedent). (b) Tests whose
      mechanics REQUIRE the cron-scheduled customers DAG (reentry: `airflow backfill
      create` raises DagNonPeriodicScheduleException on asset-scheduled DAGs --
      rebuild-from-raw.py's own probe documents this; rebuild: the script triggers
      csv_ingest_customers backfills and snapshots customers SCD2 state) STAY ON CUSTOMERS
      with SNAPSHOT-COMPLETE fixtures: echo the current gold roster (queried live at
      fixture-build time) + the test's own new rows => vanished=0, breaker never trips --
      this IS the delivery shape customers.yaml's snapshot contract declares.
      test_dbt_silver_pipeline also stays customers (v_customers_lineage has no orders
      counterpart; roster stays small once all 1M tests move to orders) with the same
      snapshot-complete echo. (c) Raw-file retention: tests that PUBLISH keep their raw
      uploads (finally-deletes removed) -- section-63 raw immutability, and
      test_rebuild_from_raw's D-29 compare is only coherent if raw is complete. Orders
      sizes: podkill+u3 keep 1M (kill-window + U3 baseline are load-bearing),
      concurrent_select drops to 250k (property is publish atomicity, not scale) to bound
      the rebuild capstone's reprocessing mass. (d) test_rebuild_from_raw additionally
      gains dataset-wide terminal settle waits (sweep's _wait_for_dataset_files_terminal
      shape) before its post-rebuild snapshots -- orders rebuilds via the ASSET CASCADE
      (script source: _dry_run_supports_backfill skips orders), so orders_after taken
      right after the customers backfill completes races the cascade."
    - "(21) VERDICT: REAL correctness bug (silent-drop class), not a test artifact.
      Mechanism, confirmed by source read of dbt/models/silver/silver_customers.sql
      lines 39-45 + round15-job.log traceback (assert at test line 1289): the
      incremental filter `_run_id > max(_run_id) from {{ this }}` is a GLOBAL max
      watermark; any run whose rows are staged AFTER a higher _run_id already landed in
      silver is excluded FOREVER. Run 42 (late file's newest replay run, staged late in
      the replay wave while runs >42 had already been dbt-built) never entered the
      ranking; silver kept the initial wave's attribution => assert 4 failed with content
      present and correct (assertion 1 passed). Retro-explains R14's blind sweep failure
      (same assert). The same shape can drop GENUINELY NEW rows: a stage retry/lease
      reclaim (exactly the (20a) wait-and-reclaim path built in R15) that completes after
      a higher run's dbt pass is permanently invisible -- no-silent-drops violation.
      dedup_audit_post_hook + reconciliation_post_hook share the same max(max_run_id)
      floor => audit counts have the same gap. FIX at the model layer: new
      meta.dbt_processed_runs ledger claimed by a PRE-hook INSERT (invocation-scoped,
      statically-rendered SQL -- the macros' own documented deferred-render constraint
      forbids run_query-computed args); model filter becomes `_run_id IN (SELECT run_id
      FROM ledger WHERE dataset=X AND dbt_invocation_id='{{ invocation_id }}')` under
      is_incremental; both post-hook macros switch from the max-floor to the same
      invocation-scoped set. Pre-hook INSERT freezes the batch in the SAME transaction
      the model reads it => exact under READ COMMITTED (no committed-between-statements
      race); concurrent builds (both DAGs build both models) serialize on the ledger's
      PK, loser claims nothing and no-ops. dbt_app grants mirror meta.dedup_audit's own
      precedent (migration 0024)."
    - "(22) VERDICT: missing SCHEMA USAGE, exact 0038 precedent. R15's error is
      'permission denied for SCHEMA normalized' -- migration 0019 granted analytics_owner
      TABLE SELECT on normalized.customers/orders but schema `normalized` (owned by the
      migration superuser) never got USAGE for analytics_owner; 0038 fixed the identical
      gap for schema meta and its docstring even flags 0019 as 'apparently worked some
      other way (unconfirmed)'. Platform role model says analytics_owner IS supposed to
      read normalized (0019's own rationale) => fix is migration 0039 GRANT USAGE ON
      SCHEMA normalized TO analytics_owner, not a test-side etl_app switch."
    - "(23) VERDICT: instrumentation bug in wire_dbt_build_tracking
      (airflow/dags/_common/run_stage_recorder.py): list_run_ids_pending_dbt_build has NO
      upstream edge -- the scheduler runs it at DagRun start, BEFORE the same DagRun's
      stage completes, so a run staged by its own DagRun is never in the pending list and
      its DBT_BUILD row is deferred to the NEXT DagRun (which for run 668 landed after the
      test's 300s poll expired; contributing: podkill's 1M re-stage delayed 668's claim to
      the 19:46 pass). The function's own docstring already claims 'stage >> mark_running
      >> dbt_build' ordering -- the eligibility QUERY just isn't constrained by it. FIX:
      add `stage >> pending_run_ids` in wire_dbt_build_tracking so eligibility is computed
      post-stage; the staging DagRun then writes RUNNING before its own dbt_build pod
      launches (also fixes v_run_recovery's one-DagRun-behind DBT_BUILD visibility)."
    - "(20b) VERDICT: real leak paths confirmed at source, quarantine currently blocks
      only the PASS, not the DATA. (i) SCDPublisher Step C (_BRONZE_HISTORY_SQL) reads a
      key's ENTIRE bronze history unscoped -- a QUARANTINED run's bronze rows are folded
      into gold by ANY later pass touching the key. (ii) merge_orders/_PUBLISH_SQL
      publishes the WHOLE silver table -- quarantined orders runs' silver rows leak into
      gold on the next pass. (iii) silver retains 3M+ rows attributed to QUARANTINED runs
      (R15 census: corpus keys attributed to quarantined 397/450 via the byte-identical
      tie-break). MINIMAL CORRECT PIECE this round: exclusion predicate `_run_id NOT IN
      (SELECT run_id FROM meta.ingestion_runs WHERE status='QUARANTINED')` in (i) scd
      _BRONZE_HISTORY_SQL and (ii) merge.py + merge_orders.py _PUBLISH_SQL (NOT-IN shape
      so harness rows without metadata stay included), + identifiability view
      meta.v_quarantined_artifacts (bronze/silver row counts per quarantined run), +
      red/green integration test. ADR records the deferred silver disposition (dbt-side
      exclusion needs status visibility for dbt_app; retro-deletion must re-materialize
      displaced keys; bronze rows stay per raw-immutability/traceability, excluded from
      all consumers)."
  pre_registered_criteria:
    - "(a) The 8 (19)-owned failures clear: repointed orders tests reach SUCCEEDED
      (podkill/dbtkill/u3/concurrent/idempotent), snapshot-complete customers tests
      (reentry, dbt_silver, rebuild step 0) publish without QUARANTINE."
    - "(b) (21) sweep assert 4 passes (silver attribution follows the newest replay run
      once its bronze is claimable) -- sweep test goes green end-to-end."
    - "(c) (22) orphan test passes its normalized.orders read as analytics_owner."
    - "(d) (23) dbtkill observes DBT_BUILD RUNNING inside its poll window on the
      staging DagRun itself."
    - "(e) (20b) guards: no gold row carries a QUARANTINED _run_id lineage
      (spot-check via v_quarantined_artifacts); mass_delete test still passes."
    - "(f) Guards green: Kyverno 0, restarts 0, breaker collateral 0, fixes
      16/17/18/20/20a hold; run completes inside 190min."
    - "(g) TARGET: fully green census, or failures reduced to nameable stragglers with
      streamed tracebacks. Known pre-registered risk: the rebuild test now reprocesses
      the kept ~2.25M-row orders raw history via the asset cascade inside its 1800s
      settle -- if it blows the budget it is a nameable straggler with a clear knob."
  scheduler_memory_18b: "DONE: scheduler memory limit 2048Mi -> 2560Mi in
      helm/values/ci/airflow.yaml (requests untouched -- zero CI-budget-gate effect),
      justified in-file: R15 peak 93.9% new high-water AND this round makes 1M-row
      runs succeed + the rebuild capstone reprocesses the retained raw mass."
  reasoning_checkpoint:
    hypothesis: "The 10 remaining failures decompose into exactly five independent
        mechanisms, each with a source-pinned cause: (19) fixture delivery shape vs
        customers' snapshot contract (design, not bug); (21) global-max _run_id
        watermark drops out-of-order-staged runs (real silent-drop bug); (22) missing
        schema USAGE for analytics_owner on normalized (grant-history gap); (23)
        eligibility query unordered vs stage (instrumentation gap); (20b) quarantine
        blocks the pass, not the data (real gold-leak bug). Fixing all five yields a
        green (or nameable-stragglers-only) cluster-slice-verify."
    confirming_evidence:
      - "(21): red/green local repro -- the EXACT out-of-order shape fails on pre-fix
        models and passes with the claim ledger; run 42's traceback + end-of-session
        silver census match the mechanism 1:1."
      - "(22): the R15 error is schema-level ('permission denied for schema
        normalized'); 0019 granted only table SELECTs; 0038 closed the identical gap
        for schema meta and its own docstring flags 0019's unexplained survival."
      - "(23): red/green -- the new structure assertion fails on pre-fix wiring;
        list_run_ids_pending_dbt_build provably had no upstream edge; 668's timing
        (staged 19:49 by its own DagRun, row never written in-window) fits exactly."
      - "(20b): red/green -- pre-fix publishers deliver a QUARANTINED run's row to
        gold in both the SCD-recompute and whole-silver-merge paths under
        testcontainers; R15 census showed silver lineage attributed to quarantined
        397/450 live."
      - "(19): R15 pre-registered prediction fired exactly (all 8 lone-file runs
        QUARANTINED); orders' contract tolerance proven live by run 765 SUCCEEDED on
        a partial delivery; manual-trigger idiom proven live by the orphan test."
    falsification_test: "Live run: any repointed orders test wedging or quarantining
        refutes the (19) adjudication; sweep assert 4 still failing refutes the (21)
        mechanism; the orphan test still hitting InsufficientPrivilege refutes (22);
        dbtkill's 300s poll still expiring refutes (23); any gold row carrying a
        QUARANTINED _run_id refutes (20b)."
    fix_rationale: "Each fix is at its mechanism's own layer: (21) replaces the
        broken eligibility PREDICATE (not the tie-break, which was already correct);
        (22) completes the established grant model rather than rerouting the test;
        (23) orders the existing instrumentation rather than changing what it
        records; (20b) enforces quarantine's meaning at every gold-feeding read
        while retaining the artifacts for traceability; (19) changes TEST deliveries
        to honor production contracts rather than weakening any breaker."
    blind_spots: "(1) The rebuild test's live cost is unmeasured: it now reprocesses
        ~2.35M retained orders rows via the asset cascade inside two 1800s settle
        windows -- pre-registered as the round's nameable-straggler risk. (2)
        Snapshot-complete fixtures race concurrent roster changes between build and
        publish (accepted: during singles only this suite publishes customers). (3)
        dbt pre-hook/model same-transaction assumption is dbt-postgres default
        behavior; the integration harness exercises it for real, but a future dbt
        major could change it (the out_of_order test would catch that). (4) The 2
        offline policy failures and test_publish_orders TypeErrors are pre-existing
        and out of scope, unchanged on bare HEAD."
  offline_status: "COMPLETE 2026-08-28: all five items implemented; red/green proven
      for (21)/(23)/(20b); battery green (unit+regression 560, dagtest 14, policy
      157 + 2 known pre-existing, manifests+kubeconform 378/0/0, mypy strict 91
      files, integration 92 across all touched suites, slice collects 17). See the
      ROUND 16 offline Evidence entry."
  live_verification_state: "RECORDED 2026-08-28T~09:4xZ: ROUND 16 pushed as code
      commit 55c8e41 (base 11eaa1d; docs 8d14130; empty trigger commits 642c0fa +
      0a69dec -- the first trigger was itself skipped because its body quoted the
      docs commit's skip marker verbatim and GitHub matches skip instructions
      anywhere in the head commit message; 0a69dec is the clean trigger and the
      headSha all four workflows run at, tree identical to 55c8e41).
      AUTHORITATIVE ROUND 16 live-verification run: e2e-full.yml run 33126343052
      (headSha 0a69dec). Companions same headSha: publish.yml 33126343060
      (criterion 0 -- fixes must be in-image: migrations 0039-0041 run in the
      migrate step, dbt project changes ride the dbt image, test changes ride the
      checkout), CI 33126343071 (expect the pre-existing Quality-gate+Integration
      pattern only), e2e-chaos 33126343043 (observational). Analysis criteria =
      pre_registered_criteria above ((a) 8 (19)-owned failures clear; (b) sweep
      assert 4 passes via the claim ledger; (c) orphan test's owner read works;
      (d) dbtkill sees DBT_BUILD RUNNING in-window; (e) no QUARANTINED lineage in
      gold + mass_delete still passes; (f) guards green + inside 190min; (g) fully
      green census or nameable stragglers -- pre-registered risk: the rebuild
      test's asset-cascade reprocessing of the retained ~2.35M-row orders raw
      history inside its two 1800s settle windows). If the job dies without
      output, always()-diagnostics + the streamed-traceback rider carry the
      evidence; scratchpad convention: save the job log as round16-job.log."
  next_action: "CHECKPOINT (human-action) returned: session manager runs the single
      60s-interval watcher on run 33126343052, then spawn post-run analysis per
      live_verification_state. Carried follow-ups: sidecar mirror, stage-side
      RejectionRateCircuitBreaker classification, teardown-race flake class,
      v_run_recovery wording, ADR-0012's deferred silver disposition."

ROUND 15 OUTCOME (2026-08-27, post-run analysis of run 33103279876 -- SUPERSEDED BY ROUND 16 ABOVE):
  run: "e2e-full.yml 33103279876, headSha 25b6eb0, conclusion FAILURE after 1h44m12s
      (18:24:15Z -> 20:08:27Z) -- FIRST run of the entire session to finish under its
      own steam rather than hitting a job-timeout ceiling. Companions same headSha:
      publish.yml 33103279760 SUCCESS (criterion 0 MET -- fixes in-image), CI
      33103279751 = exactly the pre-existing Quality-gate+Integration pattern (no new
      job-level failures), e2e-chaos 33103279815 failure (observational, out of scope)."
  criteria_verdict:
    - "(0) publish SUCCESS: MET."
    - "(a)/(2) OLD (20) SIGNATURE ABSENT: MET DECISIVELY. run->file dump: EVERY e2e
      run terminal (zero RUNNING wedges, zero orphans); staging.customers: every
      5-col fixture STAGED with bronze rows (544:3, 584:1M, 612:120, 640:1M, 668:120,
      736:1M, 764:3, 809:120 -- distinct_keys == bronze_rows everywhere); zero
      'SKIPPED_CONCURRENT' occurrences in the whole 10,603-line log. Fix (20)
      schema-compat + pad LIVE-CONFIRMED. Fix (20a) LEG 2 LIVE-EXERCISED by the real
      podkill SIGKILL: scheduled__19:36 stage[0] try=2 success 19:39:31->19:45:00
      (5m29s = wait-out-live-lease then genuine re-stage), 1M rows staged exactly
      once, zero duplicates. The 9 (20)-owned failures' MECHANISM cleared."
    - "(3) PREDICTION (19) FIRED EXACTLY AS PRE-REGISTERED: all 8 lone-customers e2e
      runs terminal QUARANTINED (544 backfill-original, 584 concurrent-select, 612
      dbt-silver, 640 podkill, 668 dbtkill, 736 u3, 764 rebuild-original, 809
      idempotent-1); their tests failed FAST (1.5-10.5min) and LEGIBLY
      ('QUARANTINED', not SUCCEEDED with streamed traceback). e2e-orphan ORDERS run
      765 SUCCEEDED (orders has no customers-snapshot vanished check) -- exactly the
      customers-only shape the prediction named. NOT a (20) refutation: a design
      decision, returned to user."
    - "(b)/(4) BUDGET: MET -- 1h44m12s of 190 (45%, 86min headroom). Decomposition:
      setup 7.1min; cluster module 19s (21P/6S); slice 96.3min = sweep module 52.6min
      (dry 54s P, pilot 13m43s P, sweep 10m31s F, idem_rerun 9m05s P, live 7m44s P,
      mass_delete 1m51s P, scd 8m54s P [was 29m57s in R14]) + singles 43.7min
      (reentry 2m53s F, concurrent 5m06s F, dbt_silver 3m22s F, podkill 10m45s F,
      dbtkill 7m11s F, u3 5m27s F, rebuild 3m29s F, orphan 3m12s F, smoke 39s P,
      idem_reupload 1m29s F); diagnostics 30s. The R14 73.5min timeout burn is GONE
      (failures now cost ~43.5min and carry their WHY). Observability + capstone
      steps still SKIPPED (gated on suite green) -- green projection ~104min suite +
      obs/capstone comfortably inside 190."
    - "(c)/(5) GUARDS: Kyverno DENY 0; control-plane restarts 0 (timeline empty);
      breaker collateral 0 (mass_delete PASSED 1m51s designed-shape, run 397 the only
      corpus QUARANTINED); fix 16 holds (idem_rerun PASSED; replay wave runs 29-48
      clean; the three backfill_3 publish try=2s are the test's own designed re-runs,
      all success); fix 17 holds (orders 25-60 ALL SUCCEEDED, live asset cascade);
      fix 18 holds. 35 customers DagRuns wall-to-wall 18:32->20:05, zero gaps, zero
      +45:00 deaths; 679 TIs = 636 success / 36 skipped / 1 failed / 3
      upstream_failed / 2 removed / 1 running-at-end. FailedScheduling: one transient
      burst 18:45-18:50 (~10 pods, pilot/sweep co-scheduling, pendings to ~9min, aged
      out, ZERO test failures attributable -- pilot PASSED this time) + 1 pod 19:19.
      WATCH 18b TIGHTENS: scheduler peak 2016055296B = 1923MiB = 93.9% of 2048Mi,
      NEW session high-water (R13 91.9%, R14 86.3%) -- ~125MiB headroom."
    - "(d)/(6) CENSUS: FIRST COMPLETE census of the session -- 28 passed / 10 failed /
      6 skipped in 5795.05s (1:36:35); ALL 17 baseline node-IDs finished. 7 PASSED
      (same set as R14): no_extra_schemas, pilot, idem_rerun, live_run, mass_delete,
      scd, smoke_xcom. 10 FAILED, every one with a streamed traceback: 8x
      (19)-owned + sweep (21) + orphan (22). ZERO new failing node-IDs beyond the
      baseline."
    - "(e)/(7) SILENT-DROP GATE: MET. Zero SKIPPED_CONCURRENT shapes; the one real
      crash (podkill SIGKILL) re-staged genuinely via wait-and-reclaim; ONE failed TI
      all session = integrity_gate[15] of scheduled__19:51 failing 19:53:37->19:53:41
      -- the EXACT second the dbtkill test's teardown deleted its fixture: the R14
      integrity_gate flake class is ADJUDICATED as a teardown-deletes-file-mid-check
      race (the gate correctly failed a file that vanished under it; zero knock-on,
      next cron success)."
  residual_findings:
    - "(19) LIVE-CONFIRMED, now a DESIGN DECISION (owns 8 of 10 failures): each lone
      e2e customers file stages+builds fine, then its publish pass computes vanished
      ~= the whole gold roster (built by the sweep corpus + prior 1M-row e2e files)
      -> ratio ~100% >> 0.10 -> mass-delete breaker QUARANTINED. The breaker is doing
      EXACTLY its job for a snapshot-declared dataset: a customers delivery
      containing 120 keys where gold knows ~3M IS a mass-delete signal. The tension
      is e2e fixture design vs shared gold state, not a platform bug."
    - "(21) sweep D-05 late-row lineage (adjudicates R14's sweep failure, now fully
      legible): BOTH dataset waits drained (all customers+orders files terminal
      SUCCEEDED -- first time ever on CI), assertions 1-3 PASSED (scoped orders
      counts exact, gap-day semantics exact, pre/post schema versions differ), then
      assert 4 failed: silver.customers has no rows for _run_id=42 (the late file's
      newest replay run, SUCCEEDED, 50 bronze rows staged). End-of-session silver
      _run_id census shows NO sweep-corpus run at all (only e2e runs + 397/450) --
      lineage attribution moved off the corpus runs entirely. Data present and
      correct (assertion 1's row counts exact); this is an ATTRIBUTION/timing
      question in the silver dedup tie-break vs replay waves, needing local
      adjudication with the dbt model SQL. NOT silent drop."
    - "(22) orphan test InsufficientPrivilege (new legible signature, likely R14's
      2m35s failure too): test_referential_orphan line 255 SELECT FROM
      normalized.orders AS analytics_owner -> permission denied for schema
      normalized. Pipeline side was FINE (orders run 765 SUCCEEDED, orphan
      quarantine designed shape). Either a missing USAGE grant for analytics_owner
      on the dbt-owned normalized schema (migration/grants gap) or the test should
      use etl_app for that read. Small, isolated."
    - "(23) dbtkill kill-window instrumentation: meta.run_stages[668,'DBT_BUILD']
      never observed (last observed: None) within the test's 300s poll, yet
      scheduled__19:46's dbt_build TI ran 19:49:11->19:49:34 INSIDE that window and
      silver holds 120 rows attributed to 668. Either the run_stages DBT_BUILD row
      is written under different keying/timing than the test polls for, or the
      claim-batch cutoff excluded 668; needs a source read. Even fixed, the test's
      final assert would then hit (19)'s QUARANTINED -- so (19) gates it anyway.
      Contributing context: the podkill 1M re-stage stretched scheduled__19:36 to
      10m47s, delaying 668's claim to the 19:46 pass."
    - "(20b) NOW MATERIAL AT SCALE (carried): silver retains 3,000,000+ rows from
      QUARANTINED runs 584/640/736 (1M each) + 120-row sets -- terminal quarantine
      blocks the PASS, not the DATA. Unchanged from R14's finding, but the volume is
      now 3M rows."
  path_to_green: "10 failures decompose: 8 clear with the (19) design decision
      (however resolved); (21) sweep lineage needs local adjudication (may be
      test-side); (22) is a one-line grant or test-connection fix; (23) is
      instrumentation keyed behind (19). With those resolved the suite projects
      ~100-110min + observability/capstone -- comfortably inside 190. The suite is,
      for the first time, within concrete reach of green."
  next_action: "DECISION CHECKPOINT returned to user: choose the (19) resolution
      (e2e fixture/dataset design vs breaker scoping vs threshold semantics -- see
      checkpoint options), plus disposition of (21)/(22)/(23)/(20b). Carried:
      sidecar mirror, stage-side RejectionRateCircuitBreaker classification, 18b
      scheduler-headroom watch (93.9% new high-water), v_run_recovery wording."

ROUND 15 (2026-08-27, opened on user decision Option B + both riders -- SUPERSEDED BY OUTCOME ABOVE):
  charter: "(1) Root-cause finding (20)'s inner exception via LOCAL repro -- why does the
      stage try-1 pod crash seconds after claiming on the single-file CUSTOMERS fixtures?
      Direct evidence, then fix at the right layer. (2) Fix (20a): a crashed claim must
      release its lease / be marked FAILED so retries actually re-stage; SKIPPED_CONCURRENT
      must never convert a crash into task SUCCESS with nothing staged -- production
      semantics done right ((20b) addressed only if it falls out naturally, else carried).
      (3) timeout-minutes 150 -> 190 (arithmetic-backed: green projects 158-188).
      (4) Riders: pytest_runtest_logreport conftest hook streaming failure tracebacks
      immediately; add integrity_gate to the always()-diagnostics TI dump. Carried
      unchanged: sidecar mirror, (19) latent, 18b watch, stage-side breaker
      classification. Usual cycle: offline battery (red/green), commit, push, single 60s
      watcher run by the session manager, pre-registered criteria."
  finding_20_root_cause: "SOURCE-CONFIRMED (direct code read, every link; local repro red
      test to follow as falsification): the inner exception is
      IncompatibleSchemaError(diagnostic_code='schema-column-disappeared',
      column='signup_country'). Chain:
      (i) ALL wedging e2e fixtures are 5-column customers files (header
      customer_id,name,country,birth_date,event_ts): tests/fixtures/slice-corpus.yaml
      declares customers_small/large with that exact 5-col header (lines 59/219), and
      test_backfill_reentry/test_rebuild_from_raw's _build_customers_csv hand-writes the
      same 5-col header. The sweep corpus (tools/corpus/dated_series.py, plan 10-06)
      'already always emits all 6 columns' (test_stage_ingest.py docstring) -- which is
      why every corpus-shaped file staged fine minutes earlier.
      (ii) configs/datasets/customers.yaml gained a 6th column signup_country in Phase 10
      (plan 10-01/migration 0035) with required: false EXPLICITLY documented as 'files
      delivered before this column existed never carried it... not reject files missing
      it' (D-13).
      (iii) But dataplat.schema.evolution.classify_schema_change has NO concept of
      optional columns: ANY old_columns entry absent from the observed header raises
      IncompatibleSchemaError schema-column-disappeared -- and
      csv_processor.source.CsvSource._resolve_schema builds old_columns from ALL of
      ctx.config.columns (6 incl. signup_country), passing no required info.
      (iv) Timing matches exactly: stage_ingest claims (status=RUNNING + 5-min lease
      committed), then loader.load -> source.open() -> inspect() -> _resolve_schema ->
      raise. 'Catches nothing' contract -> CLI exit 1 -> pod phase=Failed seconds after
      claim, ZERO rows staged. Deterministic on every retry/re-offer. Customers-only
      (orders e2e-orphan fixture carries orders' full header -> SUCCEEDED). PRE-EXISTING
      since Phase 10 landed -- explains R13's identical signature.
      (v) round14-job.log mining confirms the etl-monitor captures carry only
      name+phase (no container status/exit code), so the CI log alone could never name
      this -- consistent with the ROUND 14 'inner exception unknown' assessment.
      DOC CONFLICT RECORDED: dataplat/config/model.py's ColumnContract docstring says
      'required: False with the column absent ... classified breaking' -- directly
      contradicting customers.yaml's D-13 comment AND the field's own first sentence
      ('whether the column must appear in the file's structure at all'). Under the
      docstring's reading required:false would be behaviorally identical to
      required:true, i.e. dead. Resolution: implement the D-13 semantics (absence of a
      required:false column is COMPATIBLE), fix the model docstring."
  fix_20_design: "RIGHT LAYER = schema classification + loader, honoring positional
      loading: a file's observed header is loadable iff it is a STRICT PREFIX of the
      contract's column order and every contract column beyond the prefix is
      required:false (D-13's append-only evolution case -- old files are prefixes).
      Changes: (a) classify_schema_change gains keyword-only optional_columns:
      frozenset[str]; the disappearance raise is skipped for names in it (hash inputs
      UNTOUCHED -- hash_schema hashes the whole mapping dicts, so old_columns/new_columns
      must not gain keys). (b) _resolve_schema passes {name for c in config.columns if
      not c.required}; on the findings (new-column INFERRED) path adds a
      contract-prefix-intact guard (new_names[:len(old)] == old_names else raise --
      also closes a PRE-EXISTING latent hole where a file with contract columns present
      but a NEW column at a non-trailing position would sync INFERRED and then be
      positionally corrupted by load()'s truncation); on the no-findings path, a
      narrower header that passes the prefix+optional rule resolves to the CONTRACT
      version via resolve_by_hash(hash_schema(old_columns)) (StorageError fallback:
      sync CONTRACT) -- deliberately NOT sync(INFERRED), which would flip the CURRENT
      version and re-key/re-eligibilize every file (R12's measured replay-wave
      mechanism) on every narrower-file arrival; anything else keeps raising with the
      reorder diagnostic extended to name the non-trailing-missing case. (c)
      StagingLoader.load pads a narrower row to len(target_columns) with None at the
      existing truncate site, BEFORE _record_hash -- a padded 5-col row hashes
      identically to a 6-col row with empty signup_country (None renders ''), which is
      semantically the same business record; no back-compat hash risk because no 5-col
      row has EVER staged successfully (they all crashed). Docstring truth-ups in
      staging.py (module + truncate-site comment) and model.py. KNOWN LIMIT (documented,
      not built): a streaming quality rule configured on an absent optional trailing
      column would IndexError -- no dataset declares one; noted at the pad site."
  fix_20a_design: "TWO complementary legs, stage-side only (the measured gap):
      LEG 1 (crash releases the claim): new guarded repository method
      fail_ingestion_run_claim(run_id, pod_name) -- UPDATE meta.ingestion_runs SET
      status='FAILED', lease_expires_at=now() WHERE run_id=%s AND status='RUNNING' AND
      k8s_pod_name=%s. Called from stage_ingest's existing outer finally when
      run_status=='failed' (the metrics finally already distinguishes exactly this) --
      best-effort try/except (a DB-down crash cause would otherwise REPLACE the true
      in-flight exception in the finally), logged; lease expiry stays the backstop for
      the SIGKILL class. Guarded by pod_name so a retry never stomps another live
      claimant. 'Catches nothing' preserved: no except around the body, pure finally
      side-effect.
      LEG 2 (retry honesty): stage_ingest's claim-refused path no longer returns
      SKIPPED_CONCURRENT immediately. New bounded wait-and-reclaim loop
      (concurrent_wait_seconds kwarg, default 420 = 5-min lease + margin; CLI reads
      DATAPLAT_STAGE_CONCURRENT_WAIT_SECONDS like the heartbeat precedent): poll
      status; STAGED/SUCCEEDED -> skipped receipt (the work verifiably exists);
      otherwise re-attempt the claim (succeeds the moment the lease expires or LEG 1
      wrote FAILED) -> stage genuinely; deadline expiry -> raise DataPlatformError
      (task FAILS, Airflow retries with backoff) -- NEVER task SUCCESS with nothing
      staged. A live heartbeating claimant keeps extending its lease -> we keep
      waiting until it terminates or our deadline fires; both outcomes honest.
      (20b) quarantine-silver leakage: does NOT fall out of stage-side work -- stays
      carried."
  timeline_expectations: "Post-fix on CI: e2e 5-col fixtures stage try-1 (schema fix);
      podkill's deliberate SIGKILL mid-load leaves RUNNING+live lease -> retry waits
      <=~5min for lease expiry -> reclaims -> re-stages (test budget
      _RETRY_TIMEOUT_SECONDS=600 covers it); exception-crash class releases
      instantly via LEG 1. The 9 (20)-owned failures should clear."
  reasoning_checkpoint:
    hypothesis: "Finding (20)'s inner exception is IncompatibleSchemaError
        schema-column-disappeared(signup_country): every wedging fixture is a 5-col
        customers file, the contract has 6 columns since Phase 10, and
        classify_schema_change treats ANY disappeared contract column as BREAKING
        regardless of required:false; the crash lands after claim_ingestion_run's
        commit, and the retry-inside-lease converts it to SKIPPED_CONCURRENT SUCCESS
        (20a). Fixing prefix-with-optional-tail compatibility + padding, plus
        crash-release + wait-and-reclaim, clears the 9 (20)-owned failures without
        touching any other passing path."
    confirming_evidence:
      - "slice-corpus.yaml lines 59/219: customers_small/large header is exactly the
        5-col list; _build_customers_csv hand-writes the same; dated_series corpus
        emits 6 -- matching 'corpus staged fine, e2e fixtures crash' 1:1."
      - "evolution.py classify_schema_change docstring+code: disappearance raise has no
        required-awareness; _resolve_schema passes all 6 contract columns as
        old_columns."
      - "test_stage_ingest.py's own module constant comment (10-07 Task 1) documents the
        SAME 5-vs-6 desync class being hit in the harness and 'fixed' by widening the
        TEST fixture, explicitly deferring the loader-side fix as out-of-scope then."
      - "stage() CLI (csv_processor/cli.py): both except branches write a FAILED
        receipt and re-raise but never touch meta.ingestion_runs -- the run stays
        RUNNING with a live 5-min lease; claim predicate (postgres.py 390-394) then
        refuses the retry; _skipped_receipt maps RUNNING -> SKIPPED_CONCURRENT ->
        exit 0. (20a) confirmed at source."
      - "R14 timing evidence fits: pods die seconds after claim (16:00:08, 16:15:34
        captures), retries land inside the lease 15-30s later, stage try=2
        state=success with zero rows."
    falsification_test: "RED test: stage_ingest over a 5-col customers file against the
        6-col contract in the testcontainers harness must fail TODAY with
        IncompatibleSchemaError schema-column-disappeared naming signup_country. If it
        stages instead, the hypothesis is wrong -> back to investigation. GREEN after
        fix: same file reaches STAGED with signup_country NULL. (20a) RED: a run
        claimed under a live lease by another pod_name must currently yield an
        immediate SKIPPED_CONCURRENT receipt from stage_ingest; post-fix it must wait
        and then raise (short wait override) -- and a crashed stage attempt must leave
        status=FAILED with a successful genuine re-stage on the next call."
    fix_rationale: "Root-cause layer, not symptom: the contract ALREADY declares the
        intended semantics (required: false, D-13 comment); the classifier simply never
        implemented it, and the loader's positional COPY needs the matching pad. The
        prefix rule is the narrowest possible loosening that keeps positional loading
        sound (reordered/middle-missing files still rejected loudly). (20a) fixes the
        task-status lie at its two sources (no release on crash; skip-as-success),
        preserving lease semantics for genuine concurrency."
    blind_spots: "(1) The sweep failure (14:48:31) is NOT explained by (20) -- separate
        adjudication next round if it recurs (the -v traceback rider will carry the
        WHY). (2) Integrity_gate flake class unexplained -- rider adds it to the TI
        dump. (3) A live-but-stuck heartbeating claimant makes LEG 2 wait its full
        budget then fail the task -- honest but slow; acceptable. (4) The 21 offline
        tests/integration failures on this machine are pre-existing/out of scope.
        (5) Schema-version interleaving: the narrower shape resolves to the CONTRACT
        version by hash -- if a dataset's contract hash was never recorded (only
        possible pre-bootstrap-fix histories), the fallback sync(CONTRACT) flips
        current once; edge case, documented."
  pre_registered_criteria:
    - "(a) The 9 (20)-owned failures clear: test_backfill_reentry(original),
      test_concurrent_select, test_dbt_silver_pipeline, test_pod_kill_retry (podkill +
      dbtkill + u3), test_rebuild_from_raw, test_smoke_and_idempotency(idempotent),
      test_idempotent_reupload -- their e2e runs reach terminal (STAGED->SUCCEEDED or
      legible QUARANTINED per candidate 19, adjudicated separately if it now fires)."
    - "(b) Run completes INSIDE timeout-minutes: 190 -- record the measured
      decomposition either way."
    - "(c) Guards green: Kyverno DENY 0, control-plane restarts 0, breaker collateral 0
      (fix 18 holds), fixes 16/17 hold (customers cron wall-to-wall, orders live)."
    - "(d) Census diffed vs the 17-test baseline: expect >=16/17 with the sweep failure
      adjudicated separately if it persists (its traceback now streams via the rider)."
    - "(e) No new silent-drop paths: crashed claims become FAILED/released, never
      SUCCESS-with-nothing-staged; grep the run for stage SKIPPED_CONCURRENT-with-
      zero-rows shapes."
  offline_status: "COMPLETE 2026-08-27: root cause (20) repro'd RED locally
      (IncompatibleSchemaError schema-column-disappeared(signup_country), exact
      predicted shape), both fixes implemented and GREEN (4 new stage_ingest tests,
      3 new schema-resolution tests, 4 new evolution unit tests, behavior-3
      rewritten), full battery green (unit+regression 560, dagtest 14, policy 157 +
      only the 2 known pre-existing failures, manifests+kubeconform valid, mypy
      strict 91 files clean, 74 integration tests across all touched suites).
      Riders + timeout 190 + QUARANTINED terminal-status truth-up in. See the two
      ROUND 15 Evidence entries."
  prediction_19_now_expected: "PRE-REGISTERED before the live run: with (20) fixed,
      candidate (19) is EXPECTED to fire -- each lone e2e customers fixture stages,
      then its publish pass sees vanished ~= the whole bronze-known gold roster
      (ratio ~100% >> 0.10) -> terminal QUARANTINED -> tests fail FAST and LEGIBLY
      (QUARANTINED is now poll-terminal; tracebacks stream via the rider). (20)'s
      OWN signature (stage wedge at RUNNING, zero bronze rows, SKIPPED_CONCURRENT
      try=2, orphaned runs, 73.5min timeout burn) must be ABSENT either way. e2e
      tests SUCCEEDING would mean union coverage; wedging RUNNING again would REFUTE
      the (20) fix. (19) stays the user's design decision -- this run supplies its
      definitive live evidence."
  live_verification_state: "RECORDED 2026-08-27T18:25Z: fixes (20)+(20a) pushed as
      commit 25b6eb0 (base 8c9dce1). AUTHORITATIVE ROUND 15 live-verification run:
      e2e-full.yml run 33103279876 (headSha 25b6eb0, created 2026-08-27T18:24:11Z,
      in_progress at recording; timeout-minutes now 190). Companions, same headSha:
      publish.yml 33103279760 = criterion 0 (images MUST rebuild from 25b6eb0 --
      csv-processor carries the schema fix + claim lifecycle IN-IMAGE; a stale image
      silently reverts BOTH fixes; verify success BEFORE judging the e2e run); CI
      33103279751 (expect the pre-existing Quality gate + Integration failure
      pattern -- anything NEW at job level is a ROUND 15 regression signal);
      e2e-chaos 33103279815 (observational; imports the slice conftest so the
      QUARANTINED truth-up + traceback rider apply there too).
      POST-RUN ANALYSIS STEPS (continuation agent, judged on the ROUND 15
      pre_registered_criteria):
      (1) CRITERION 0: publish.yml 33103279760 conclusion=success.
      (2) FIX-IN-FORCE probes: any e2e single-file customers run must show stage
      state=success try=1 (or a legible FAILED->re-stage via LEG 1/2) -- NEVER the
      old signature: run wedged RUNNING with zero bronze rows + stage try=2
      success-with-nothing-staged. Grep the TI dump + run->file dump for it.
      (3) PREDICTION (19): expect the customers e2e runs terminal QUARANTINED with
      their tests failing FAST on a legible not-SUCCEEDED assert carrying the
      streamed traceback (ratio ~100% in the publish log). QUARANTINED = (19)
      CONFIRMED LIVE -> return a design-decision item (delivery-shape contract vs
      breaker scoping), NOT a (20) refutation. SUCCEEDED = union-covered, (19)
      stays latent. RUNNING-wedge recurrence = (20) fix REFUTED -> back to
      investigation.
      (4) BUDGET (b): complete INSIDE 190min -- decomposition either way; the
      73.5min (20) burn should be gone; (19) failures cost minutes each.
      (5) GUARDS (c): Kyverno 0, restarts 0, scheduler peak vs 2048Mi (18b watch),
      FailedScheduling burst class noted; fixes 16/17/18 hold.
      (6) CENSUS (d): full -v diff vs the 17-test baseline + streamed tracebacks --
      FIRST run where every failure carries its WHY; adjudicate the sweep failure
      (14:48:31 in R14) from its now-visible traceback.
      (7) SILENT-DROP GATE (e): zero SKIPPED_CONCURRENT-with-nothing-staged shapes;
      crashed claims (if any) show FAILED + genuine re-stage; integrity_gate now
      visible in the TI dump for the R14 flake class."
  next_action: "Await the session manager's watcher on run 33103279876; then post-run
      analysis per live_verification_state. Carried follow-ups: sidecar mirror,
      stage-side RejectionRateCircuitBreaker classification, (19) adjudication (now
      expected live), (20b) quarantine-silver leakage, 18b scheduler-headroom watch,
      v_run_recovery wording."

ROUND 14 OUTCOME (2026-08-27, post-run analysis of run 33080823061 -- SUPERSEDED BY ROUND 15 ABOVE):
  run: "e2e-full.yml 33080823061, headSha a247b67, conclusion CANCELLED at the NEW
      timeout-minutes: 150 ceiling -- 14:11:06Z -> 16:42:07Z = 2h31m01s (cancel signal
      in-log 16:41:24). Companions, same headSha: publish.yml 33080823116 SUCCESS
      (criterion 0 MET -- quarantine semantics verifiably in-image); CI 33080823102
      failure = exactly the pre-existing Quality gate + Integration pattern, no new
      job-level regressions. Fix-in-force probes MET: 'registering publish_retries=3'
      + 'Variable publish_retries created' in the cluster-up log; the one breaker trip
      produced the QUARANTINED shape with the carrying DagRun SUCCESS try=1."
  round14_criteria_scorecard:
    - "(a) BUDGET: FAILED -- pytest alone ran 14:18:47->16:41:24 (2h22m37s) and never
      finished (1 test in flight at cancel); the post-pytest observability check and
      rebuild-from-raw capstone NEVER STARTED. But the failure is a NEW named mechanism
      (finding 20 below), NOT the (18a) collateral -- which is verifiably gone."
    - "(b) ZERO COLLATERAL CRON COST: MET. 53 csv_ingest_customers DagRuns wall-to-wall
      14:19->16:42, inter-run gap ~1s, ZERO wedges, ZERO +45:00 dagrun_timeout deaths,
      publish state=success try=1 in every run. The fixture (customers_20240114.csv,
      uploaded 15:06:28) was claimed by cron scheduled__15:04 and terminally QUARANTINED
      within ~2.5min; that cron SUCCEEDED at 15:09:06 (3m39s -- designed shape exactly).
      Two isolated cron failures (scheduled__15:42, scheduled__15:55) failed <2min each
      at the mapped integrity_gate layer (wait_for_files success try=1; discover/stage/
      dbt_build upstream_failed try=0) with zero knock-on gap -- NOT mass-delete-related;
      unexplained flake class, watch item (integrity_gate is NOT in the TI-dump task
      list -- diagnostics gap to fix)."
    - "(c) QUARANTINE PATH EXERCISED WHERE DESIGNED: MET. Run 421 = the truncated
      snapshot = the ONLY QUARANTINED row in meta.ingestion_runs; its 35 bronze rows
      staged then the pass tripped and quarantined terminally; gold-unchanged assertion
      passed; test_mass_delete_snapshot_trips_circuit_breaker PASSED in 2m12s (ROUND 13:
      ~40min collateral + 45min backfill death). The (18a) sink is ELIMINATED."
    - "(d) GUARDS: Kyverno DENY 0; control-plane restarts 0 (restart timeline empty);
      scheduler peak 1852981248B = 1767MiB... exact: 1852981248/2^20 = 1767MiB? NO --
      1852981248 bytes = 1767.1MiB = 86.3% of 2048Mi (below R13's 1881MiB/91.9%,
      headroom watch continues); FailedScheduling: ONE burst confined to 14:30-14:42
      (pilot/sweep co-scheduling, 11 unique pods, pendings up to ~9min, ALL reached
      Running/Succeeded -- pod-level self-heal), ZERO FailedScheduling 14:42->16:41.
      CAVEAT: the burst overlaps test_full_2year_sweep's window and that test FAILED at
      14:48:31, so '18b self-heals without test failures' cannot be fully credited this
      round -- 18b stays open pending the sweep-failure adjudication."
    - "(e) FULL NODE-ID CENSUS: MET -- first measurable census in 4 rounds (the -v rider
      worked; 28 newline-terminated result lines survived cancellation). See census in
      the ROUND 14 post-run Evidence entry."
    - "(f) WATCH ITEMS: recorded (scheduler headroom; 18b; integrity_gate flake;
      quarantine-silver leakage 20b below)."
  candidate_19_adjudication: "DID NOT FIRE as predicted. Predicted signature (e2e-* runs
      terminal QUARANTINED + tests failing fast on a legible quarantine) is ABSENT: zero
      e2e QUARANTINED runs. The e2e single-file runs never REACH publish -- they wedge in
      the STAGE phase (finding 20). (19)'s snapshot-delivery-shape tension remains latent
      and untested behind (20); keep carried."
  finding_20: "NEW DOMINANT RESIDUAL MECHANISM (named with evidence, PRE-EXISTING -- not a
      fix-(18) regression): every e2e single-file CUSTOMERS fixture run wedges at
      meta.ingestion_runs.status='RUNNING' in the STAGE phase with ZERO bronze rows.
      Run->file dump: runs 823 (e2e-backfill-original), 863 (e2e-concurrent-select),
      959 (e2e-dbt-silver), 1015 (e2e-podkill), 1071 (e2e-dbtkill), 1127 (e2e-u3),
      1211 (e2e-rebuild-original) ALL 'RUNNING'; staging.customers per-run dump has NO
      rows for any of them; e2e-orphan (ORDERS, run 1212) SUCCEEDED -- customers-only.
      MECHANISM (each leg evidence-backed):
      (i) A genuine stage try-1 attempt CRASHES seconds after claim_ingestion_run
      commits status=RUNNING + a 5-min lease: etl-monitor captured stage pods
      phase=Failed at 16:00:08 (stage-tabyhjge) and 16:15:34 (stage-1q9yvapa) --
      exactly the try-1 windows of the 15:57 and 16:12 crons' stage TIs.
      (ii) Airflow's retry lands ~15-30s later, INSIDE the still-live lease ->
      claim_ingestion_run refuses -> _skipped_receipt returns SKIPPED_CONCURRENT ->
      the retry TI reports SUCCESS having staged nothing. TI dump: every cron whose
      window held a live e2e file shows stage try=2 state=success (15:38, 15:44,
      15:51, 15:57, 16:05 crons) or try=1 success when a NEIGHBORING cron's
      crash-lease was still live (15:48, 16:02) -- and the DagRun then SUCCEEDS.
      (iii) Later crons re-offer the still-present file; every genuine attempt crashes
      identically (deterministic).
      (iv) The test's wait loop burns its full timeout (4m41s-13m24s per test,
      73.5min total across 9 FAILED tests) and fails; teardown deletes the fixture
      from raw/customers/ (end-of-run MinIO listing confirms only 2 e2e objects
      remain, both from the final minutes) -> discovery can never re-offer -> the run
      is orphaned at RUNNING permanently.
      PRE-EXISTING: ROUND 13's run->file dump (pre-quarantine image 4d3db56) shows the
      IDENTICAL signature -- runs 461 (e2e-dbtkill), 515 (e2e-u3), 623 (e2e-rebuild),
      666 (e2e-idempotent) all RUNNING, e2e-orphan orders SUCCEEDED. So the 17-test
      baseline's e2e members have been failing THIS way at least since R13; the old
      'discovery never registered it' template is gone (discovery now registers them --
      rounds 12-14 progress), and (20) is what the census unmasked underneath.
      INNER EXCEPTION UNKNOWN: pytest prints tracebacks only at session end (cancelled)
      and the pod/task logs died with the cluster. ELIMINATED as the inner cause by this
      run's own evidence: filename-mask rejection (customers declares NO filename mask
      -- config comment explicit), KPO startup-timeout/FailedScheduling (ZERO
      FailedScheduling events 14:42->16:41), Kyverno (0 denials), stage-side VOLUME
      barrier (customers.yaml declares no VOLUME rule). NOT eliminated: stage-side
      RejectionRateCircuitBreaker (rejection_rate_threshold 0.5 -- plausible only for
      the reentry fixture's deliberate bad row, NOT for the clean fixtures), schema/
      parse/loader errors specific to the customers_small-derived fixture shape
      (5-col v1 header + marker-in-name-field + offset customer_ids). Corpus-shaped
      files -- including v1-shaped 459/486 and the 35-row truncated snapshot 421 --
      all staged fine minutes earlier, so the trigger is something these fixtures
      uniquely carry. LOCAL repro is the ROUND 15 move: run the stage path against one
      generated e2e fixture and capture the exception.
      DESIGN GAPS surfaced regardless of the inner exception:
      (20a) claim-then-crash + retry-inside-lease = the retry reports task SUCCESS
      while the run stays unstaged -- a SILENT DROP at the task-status layer (core-value
      violation); stage_ingest's 'catches nothing' contract leaves no on-crash status
      release (claim predicate allows FAILED reclaim but nothing ever writes FAILED).
      (20b) QUARANTINE-SILVER LEAKAGE: silver.customers retains _run_id=421 (1 row) --
      dbt consolidates bronze regardless of quarantine, and publisher.publish upserts
      the ENTIRE silver table with no _run_id filter (documented in run.py itself), so
      a quarantined run's surviving silver rows are published to gold by the NEXT
      successful pass. Terminal quarantine currently blocks the PASS, not the DATA.
      Design follow-up for the quarantine semantics."
  sweep_failure_note: "test_full_2year_sweep_customers_and_orders FAILED at 14:48:31
      (11m51s in), the first slice failure. At failure time every customers/orders run
      was terminal EXCEPT the 4 PENDING zombies (runs 11/12/35/36 -- initial-wave rows
      superseded after schema v1->v2 evolution by replacement runs 47/48/71/72 under
      extended idempotency keys, all of which SUCCEEDED); no full-window backfill
      existed yet (backfill_3 was created seconds AFTER the failure and carried the
      NEXT test, test_idempotent_rerun, which PASSED). The 14:30-14:42 FailedScheduling
      burst overlaps its window. Exact assert unknown (no traceback -- see rider
      recommendation); needs adjudication in ROUND 15."
  budget_arithmetic_two_points: "R13 (120 ceiling): cancelled at 2h00m49s, projection
      ~2h35m as-is / ~1h55-2h10 trimmed. R14 (150 ceiling): cancelled at 2h31m01s
      TRUNCATED -- remaining work at cancel: >=1 test (idempotent_reupload, would fail
      via (20) at ~7min) + observability step + rebuild capstone (+25-40min est) ->
      honest as-is ~3h03m-3h20m. Decomposition: setup 7.7min; cluster module 20s
      (15 pass/6 skip); sweep module 79.5min (dry_run 49s P, pilot 16m44s P, sweep
      11m51s F, idem_rerun 10m00s P, live_run 7m56s P, mass_delete 2m12s P -- the trim
      delta, scd 29m57s P); post-sweep singles 62.3min = 8 FAILED via (20) + orphan
      2m35s F + smoke_xcom 37s P. Passing tests consumed ~68min, failing ~73.5min.
      GREEN-SUITE PROJECTION with (20) fixed: setup 8 + cluster 0.5 + sweep ~75-80
      (scd 30 and pilot 17 are honest throughput costs) + 10 singles at ~4-6min each
      ~45-60 + observability+capstone 25-40 = ~158-188min. VERDICT: 150 does NOT hold
      even green. Either raise to 180 (arithmetic-backed, covers the projection's
      upper half thinly -- 190-210 covers with margin) or the sweep module's 47min
      (scd+pilot) needs its own throughput round (job-splitting direction is retired
      per scope guardrails)."
  next_action: "DECISION CHECKPOINT returned to user: choose ROUND 15 direction --
      (A) root-cause finding (20)'s inner exception via LOCAL repro (generate one
      e2e-style marked customers_small fixture, drive the stage path against it,
      capture the exception), fix it PLUS the (20a) silent-drop gap; keep 150 and
      re-measure. (B) = A + raise timeout-minutes to 180-190 now (arithmetic-backed;
      avoids another ceiling-cancel while (20) is being fixed). (C) adjudicate the
      sweep failure first (smaller scope but (20) owns 73.5min of the budget).
      Riders recommended regardless: stream failure tracebacks immediately via a
      conftest pytest_runtest_logreport hook (print rep.longreprtext on failure --
      cancelled runs then carry the WHY, not just the WHICH); add integrity_gate to
      the always()-diagnostics TI-dump task list; carried follow-ups (sidecar mirror,
      stage-side breaker classification, (19) latent, 18b, scheduler headroom,
      v_run_recovery wording, NEW 20b quarantine-silver leakage)."

ROUND 14 (2026-08-27, opened on user decision Option C -- SUPERSEDED BY OUTCOME ABOVE):
  charter: "Fix finding (18a)'s ~40min mass-delete collateral via ALL THREE complementary trim
      shapes AND raise timeout-minutes 120 -> 150, plus the pytest-observability rider.
      (1) timeout-minutes: 150 in e2e-full.yml (fits the ~1h55-2h10 with-trims projection with
      margin; 120 cannot hold even the trimmed projection). (2) Trim i (test-scoped): the
      mass-delete breaker fixture must be invisible/short-lived to the */1 cron window.
      (3) Trim ii (production semantics, EXPLICITLY USER-APPROVED): deterministic
      breaker-class errors (QualityThresholdExceeded-style trips) fail fast / quarantine the
      batch per the section-51 quarantine concept instead of burning exponential-backoff
      retries; retries stay for the transient-infrastructure class. (4) Trim iii (CI config):
      reduce publish retry attempts in the CI profile. (5) Rider: replace/augment pytest -q so
      cancelled runs leave legible per-test progress. User's complementarity condition: all
      three trims land PROVIDED they are complementary; on a genuine conflict prefer the safer
      subset and record why. Standing rules: rounds 1-13 fixes stay; runner migration/job
      splitting retired; offline battery before push; single 60s watcher run by the session
      manager; judge by internals + census; commit docs per convention."
  complementarity_and_trim_i_letter_conflict: "RECORDED PER THE USER'S OWN RULE. Trims i/ii/iii
      are complementary (i test-side, ii production semantics, iii CI config) -- no
      contradiction BETWEEN them. But trim i's LETTER ('no cron run should ever see the
      truncated snapshot') is structurally unachievable inside the current platform topology,
      confirmed by direct source read of discovery.py: discover_files lists the ENTIRE
      config.source.bucket/path prefix on EVERY call (objects.list_objects, no time-window
      filter of any kind), re-offering every non-terminal run; the fixture MUST live in
      raw/customers/ for the customers pipeline to ingest it at all (one dataset = one source
      prefix = one DAG); an ingest takes >=2-3min through the 1-slot stage/publish queue; the
      cron fires every 60s. Some cron discover therefore ALWAYS lists the object -- ROUND 13's
      live evidence (cron scheduled__11:09 claimed it within ~60s of upload, BEFORE the test's
      own backfill_5 pipeline reached publish) is the direct proof. SAFER-SUBSET RESOLUTION:
      implement trim i's INTENT -- zero collateral COST (no cron wedge, no cron failure, no
      retry burn, no max_active_runs hold, no cron gap) -- which trim ii's terminal-quarantine
      semantics deliver structurally: the FIRST publish pass that trips quarantines the batch
      in seconds with exit 0 (the DagRun that performs it SUCCEEDS -- a quality-gate refusal
      is a data disposition, recorded in meta, not an infrastructure failure), and QUARANTINED
      runs are never re-offered by discovery, so the fixture is 'short-lived to the cron
      window': claimable for exactly ONE pipeline pass, permanently invisible after.
      Consequently the test's own backfill machinery (backfill_5) is REMOVED as redundant --
      the cron claims the fixture within 60s regardless (that WAS the collateral; post-ii it
      is the designed, zero-cost path), which also deletes backfill_5's own observed +45:00
      dagrun_timeout death from the budget."
  trim_ii_design_note: "CLASSIFICATION BOUNDARY (the user asked for this recorded): the
      platform's own error hierarchy ALREADY encodes it -- errors.py's PublicationError
      docstring: 'distinct from QualityThresholdExceeded, which is a deliberate business-rule
      rollback, not an infrastructure failure.' Trim ii operationalizes exactly that line:
      publish_ingest catches QualityThresholdExceeded ONLY (deterministic by construction: a
      pure function of staged bronze + gold state + configured threshold -- rerunning it is
      rerunning an identical computation, which is what burned 7 tries x 42min in ROUND 13);
      every other exception (PublicationError, psycopg OperationalError, ConfigurationError,
      ...) propagates unchanged -> KPO pod fails -> Airflow retries stay fully in force for
      the transient-infrastructure class. ON TRIP: the publish transaction has already rolled
      back (breaker is a pre-mutation barrier; gold untouched); publish_ingest then marks
      every run of the tripped pass status='QUARANTINED' (new app-level status value --
      meta.ingestion_runs.status is sa.Text with NO CHECK constraint, migration 0004, so no
      migration needed), logs the breaker context, and returns
      {'status': 'QUARANTINED', 'runs_quarantined': [...], ...} -> CLI writes it to XCom and
      exits 0. WHY TERMINAL (not left STAGED): a run left STAGED re-enters EVERY subsequent
      publish pass (list_staged_run_ids has no other filter), so one poisoned batch
      deterministically re-trips every later pass until a co-staged union happens to cover
      gold -- ROUND 11's self-sustaining-poison shape at lower cost. Terminal quarantine
      removes the poison from all future passes: subsequent innocents publish clean. WHY
      PASS-SCOPED (all staged runs of the tripped pass, not 'the guilty run'): the vanished
      mass is the ABSENCE of keys from the pass's union -- attribution to a single run is
      structurally impossible; an innocent run co-staged with a poisoned one is quarantined
      WITH it (bounded, loud, recorded in meta, operator-recoverable by re-opening the run;
      raw file untouched per section-63 immutability -- corrections arrive as new files). WHY
      EXIT 0: with KubernetesPodOperator, a nonzero exit is indistinguishable from a transient
      pod failure at the Airflow layer (no exit-code->no-retry mapping without brittle custom
      operator logic); section-51's quarantine concept says bad data is diverted + recorded
      and the pipeline CONTINUES -- the task evaluated the batch, refused it, and recorded the
      refusal = the task did its job. Loudness lives in meta (ingestion_runs.status
      QUARANTINED + structured log with ratio/threshold), the platform's system of record for
      business state (business metrics read the analytical DB by design). SCOPE BOUNDARY:
      publish-side only (the measured sink). The stage-side RejectionRateCircuitBreaker also
      raises QualityThresholdExceeded deterministically; its retry burn was NOT the measured
      collateral -- named as a documented follow-up, not changed this round. Change surface:
      publish_ingest's except branch + discovery's two skip sites + one new status value."
  trim_iii_design_note: "publish retries stay 6 by default (LOCAL unchanged); CI sets Airflow
      Variable publish_retries=3 in scripts/ci-set-workload-images.sh (the exact
      stage_cpu_request per-profile precedent). Post-trim-ii, publish retries exist ONLY for
      the transient class (KubernetesJobWatcher 30s-read-timeout race, Kyverno admission
      hiccups, co-scheduling CPU bursts): with retry_delay=30s exponential, retries=3 spans 4
      attempts over ~12min -- covering ROUND 13's measured 5-min self-healed FailedScheduling
      burst (18b) with margin, while halving the worst-case burn of any not-yet-classified
      deterministic failure. Applied to customers' publish (the measured burn site; orders'
      publish is already retries=3 and asset-triggered, left alone)."
  pre_registered_criteria:
    - "(a) BUDGET: run completes INSIDE timeout-minutes: 150 (projection ~1h55-2h10 with the
      collateral eliminated). Record the full decomposition either way."
    - "(b) ZERO COLLATERAL CRON COST: no cron run wedges/fails/stalls on the mass-delete
      fixture -- no cron gap, no 42min publish-retry burn, no max_active_runs hold. (The
      letter 'no cron run ever sees it' is structurally unachievable -- see
      complementarity_and_trim_i_letter_conflict; ONE cron pass processing-and-quarantining
      it in seconds at exit 0 is the designed shape.)"
    - "(c) QUARANTINE PATH EXERCISED WHERE DESIGNED: the deliberate breaker test passes, now
      fast -- terminal meta.ingestion_runs.status QUARANTINED for the truncated snapshot,
      gold byte-for-byte unchanged, no Airflow task failure, no retries burned."
    - "(d) GUARDS GREEN: Kyverno DENY 0, control-plane restarts 0; FailedScheduling -- the
      known transient co-scheduling burst class (18b) may recur; it self-healing without
      test failures keeps fixes (16)/(17) 'holding'; a repeat WITH failures upgrades (18b)."
    - "(e) FULL NODE-ID CENSUS finally measurable end-to-end (pytest -v rider): diff against
      the saturated 17-test baseline; per-test lines survive even a cancelled job."
    - "(f) WATCH ITEMS RECORDED: scheduler peak vs 2048Mi (18b was 91.9%); co-scheduling
      burst behavior; plus NEW pre-registered candidate (19) below."
  pre_registered_candidate_19: "(19) STRUCTURAL, named BEFORE the run, adjudicated BY the run:
      lone partial-file deliveries to the snapshot-semantics customers dataset may
      structurally trip the mass-delete breaker when their publish pass is not co-staged with
      roster-covering runs (vanished = gold-current minus the pass's few keys -> ratio near
      100%). The sweep module's OWN docstring already knows this ('a lone single-row file
      would make DELETE-detection wrongly treat every other roster member as vanished' -- its
      full-roster fixtures are sized accordingly), but the OLDER e2e single-file fixtures
      (test_smoke_and_idempotency e2e-idempotent-*, test_pod_kill_retry e2e-dbtkill-*/e2e-u3-*,
      test_rebuild_from_raw e2e-rebuild-*) upload small partial files and expect SUCCEEDED.
      Pre-ROUND-14 such passes wedge 45min and MAY self-heal when a later pass co-stages
      roster-covering leftovers (this is exactly how ROUND 13's run 409 -- the truncated
      snapshot itself -- ended SUCCEEDED at the 11:53 cron: union healing, live-observed);
      post-ROUND-14 they would QUARANTINE legibly (ratio ~90%+ in the log, status QUARANTINED,
      test fails fast with a readable message). PREDICTED SIGNATURE IF REAL: e2e-* single-file
      runs terminal QUARANTINED + their tests failing on 'not SUCCEEDED' within minutes, suite
      still completing inside budget. If it fires it is a PRE-EXISTING test-fixture/design
      tension (partial deliveries to a snapshot dataset) made visible -- a design decision for
      its own round (per-dataset delivery-shape contract vs breaker scoping), deliberately NOT
      expanded into ROUND 14's scope. NOT a (18a)-fix refutation."
  reasoning_checkpoint:
    hypothesis: "The ~40min/run mass-delete collateral (18a) is caused by Airflow-level
        exponential-backoff retries of a DETERMINISTIC QualityThresholdExceeded trip
        (publish retries=6/7 re-running an identical pure computation) plus the tripped
        pass's runs staying STAGED (re-entering every later publish pass) -- so classifying
        the trip as a terminal, non-retryable, non-reofferable quarantine disposition
        removes the entire collateral class, and the honest suite then fits inside a 150-min
        budget (~1h55-2h10 measured projection)."
    confirming_evidence:
      - "ROUND 13 live run 33062702180: cron scheduled__11:09 publish retried the identical
        trip 7x over 42min (11:11:51->11:54:00), holding max_active_runs=1 (cron gap
        11:09->11:53); backfill_5 burned its own 6 tries to the +45:00 dagrun_timeout --
        ~45min actual vs ~5min designed cost, ONE avoidable ~40min sink."
      - "Determinism of the trip: MassDeleteCircuitBreaker.apply is a pure function of
        (staged bronze keys, gold is_current keys, threshold) -- source-read; every retry
        re-evaluates identical inputs (rollback restores them); ROUND 11 observed the
        identical 54% ratio on every attempt of every wedged run."
      - "Re-offer mechanics: list_staged_run_ids selects ALL status='STAGED' runs per
        dataset (repository source-read) and discovery re-offers every non-SUCCEEDED run
        (discovery.py source-read) -- a tripped pass's runs poison every later pass until
        union healing; run 409's late SUCCEEDED at the 11:53 cron (co-staged
        roster-covering leftovers) live-confirms the union-healing mechanism AND its
        fragility."
      - "Budget arithmetic: ROUND 13 decomposition -- ~2h35-2h50 as-is projection vs
        ~1h55-2h10 with the collateral eliminated; 150min covers the trimmed projection
        with margin, 120 covers neither."
    falsification_test: "Live run with ROUND 14 in force: if the mass-delete fixture still
        produces >5min of cron-visible collateral (any cron DagRun failed/wedged on it, any
        publish retry of a breaker trip, any cron gap), trim ii's classification or the
        quarantine wiring is wrong. If the deliberate test does not reach terminal
        QUARANTINED (e.g. union-healed SUCCEEDED because the claim pool was not empty), the
        isolation assumption is wrong -- both return to investigation. Criterion (a) failing
        with zero collateral means the budget model itself is wrong -> re-measure."
    fix_rationale: "Root-cause layer, not symptom: the collateral's mechanism is
        retry-of-a-deterministic-computation + re-offer-of-a-poisoned-batch; trim ii removes
        both legs at the semantic source (the platform's own error hierarchy already
        declares this exception class 'a deliberate business-rule rollback, not an
        infrastructure failure'), rather than papering over it with a bigger timeout alone.
        The timeout raise to 150 covers the HONEST suite the fixes have now uncovered
        (orders live = 2x datasets everywhere), per measured arithmetic, not as a blind
        bump. Trim iii trims the remaining transient-retry worst case within the class where
        retries remain correct. The rider ends the 3-round census blindness."
    blind_spots: "(1) Candidate (19) above -- single-file e2e fixtures may quarantine
        legibly; pre-registered, out of scope. (2) An innocent run co-staged into a tripped
        pass is quarantined with it (pass-scoped attribution is the only honest option);
        bounded + recorded + operator-recoverable. (3) The quarantined-batch operator
        re-open path is manual (SQL status flip) -- no tooling this round. (4) Exit-0
        quarantine means Airflow-only observers see green; loudness lives in meta by
        design (business metrics read the analytical DB). (5) The observability step +
        rebuild-from-raw capstone remain unmeasured (+25-40min rough estimate inside the
        150 budget). (6) The 21 offline tests/integration failures on this machine are
        pre-existing and out of scope."
  live_verification_state: "RECORDED 2026-08-27T14:12Z: fix (18) pushed as commit a247b67
      (base 4b2c606). AUTHORITATIVE ROUND 14 live-verification run: e2e-full.yml run
      33080823061 (headSha a247b67, created 2026-08-27T14:11:02Z, in_progress at
      recording time; timeout-minutes now 150). Companion runs, same headSha:
      publish.yml 33080823116 = criterion 0 (images must rebuild from a247b67 --
      csv-processor carries the quarantine semantics IN-IMAGE, so a stale image would
      silently revert trim ii; verify its success BEFORE judging the e2e run; the DAG
      files also reach the cluster via the hostPath mount, zero staleness); CI
      33080823102 (expect the job-for-job pre-existing Quality gate + Integration
      failure pattern -- anything NEW at job level is a ROUND 14 regression signal);
      e2e-chaos 33080823098 out of scope for this signature (but note: the chaos
      suite imports the slice conftest and runs on its own cluster -- if any chaos
      test uploads partial customers files, candidate (19)'s signature may appear
      there too; observational only).
      POST-RUN ANALYSIS STEPS (for the continuation agent once the session manager's
      single 60s watcher reports terminal), judged on the ROUND 14 pre-registered
      criteria via the always()-diagnostics + the NEW -v per-test output:
      (1) CRITERION 0: publish.yml 33080823116 conclusion=success (in-image semantics).
      (2) FIX-IN-FORCE probes: job log shows 'publish_retries=3' registered
      (ci-set-workload-images step); any breaker trip in the run must produce a
      'publish_ingest.quarantined' shape (run status QUARANTINED in the ROUND 12
      run->file diagnostics dump) with the carrying DagRun SUCCESS and try=1.
      (3) BUDGET (a): job completes INSIDE 150min -- full decomposition either way.
      (4) COLLATERAL (b): zero cron wedges/failures/gaps attributable to the
      mass-delete fixture; the fixture's run terminal QUARANTINED within minutes of
      its upload; no publish retries of a deterministic trip anywhere.
      (5) QUARANTINE PATH (c): the mass-delete test PASSES (per-test -v line) --
      status QUARANTINED + gold unchanged; if instead SUCCEEDED, the claim pool was
      not empty (union healing) -- investigate which runs co-staged.
      (6) GUARDS (d): Kyverno 0, restarts 0, scheduler peak vs 2048Mi (18b watch:
      was 91.9%), FailedScheduling transient-burst class noted not failed.
      (7) CENSUS (e): the FULL per-test -v record -- diff against the saturated
      17-test baseline; this is the first measurable census in 4 rounds.
      (8) CANDIDATE (19): check e2e-idempotent/dbtkill/u3/rebuild runs' terminal
      statuses -- QUARANTINED there confirms (19) (a pre-existing design tension made
      legible, NOT a (18) refutation; return a decision item); SUCCEEDED means their
      passes were union-covered this run (19 stays latent, keep carried).
      (9) If the mass-delete fixture still burns >5min of collateral or the
      quarantine path never fires, trim ii's wiring is wrong -- return to
      investigation per the reasoning_checkpoint falsification test."
  next_action: "Await the session manager's watcher on run 33080823061; then post-run
      analysis per live_verification_state. Carried follow-ups: sidecar mirror
      (follow-up B); stage-side RejectionRateCircuitBreaker deterministic-trip
      classification (publish-side only this round); candidate (19) adjudication;
      sweep-assertion-(10) caveat (still unobserved); scheduler memory headroom watch
      (18b); v_run_recovery next_action wording for QUARANTINED (cosmetic)."

ROUND 13 OUTCOME (2026-08-27, post-run analysis of run 33062702180 -- SUPERSEDED BY ROUND 14 ABOVE):
  verdict: "Fix (17) LIVE-CONFIRMED IN FULL; refutation branch (7) NOT taken. The suite
      reached its FINAL test module for the first time ever on CI and was cancelled at
      the 120-min job timeout mid-way through test_smoke_and_idempotency. The budget was
      consumed by (i) the honest, now-longer suite (orders live = every phase does 2x
      datasets) and (ii) ONE measured avoidable sink: ~40min of mass-delete
      deterministic-breaker-trip collateral on the */1 cron DAG. The
      raise-timeout-vs-trim decision is now LIVE with measured arithmetic."
  round13_criteria_scorecard:
    - "(1) FIX-IN-FORCE: MET. Job log line 886 @10:28:07Z: '==> csv_ingest_orders
      unpaused (asset-triggered runs enabled)' (layer B retry loop succeeded at
      cluster-up; DAG file at 4d3db56 carries layer C's flag; layer A active for the
      pytest session)."
    - "(2) PRIMARY (a) orders pods/runs: MET -- FIRST EVER on CI. meta.ingestion_runs:
      initial orders wave runs 25-34 (files 37-46 = orders_20240101-11) ALL SUCCEEDED
      with schema evolution v1 CONTRACT (10:37:56) -> v2 INFERRED (10:39:29) for
      dataset_id 55, mirroring customers' predicted term-flip shape; replay wave runs
      49-60 ALL SUCCEEDED incl. days 12/13 (runs 59/60, no replay_of = first-pass);
      later, run 624 (s3://raw/orders/e2e-orphan-*.csv) SUCCEEDED = the
      referential-orphan test's orders pipeline ran end-to-end on demand."
    - "(3) PRIMARY (b) sweep drains: MET. The orders-terminal wait that consumed
      87.6min in ROUND 12 completed within the sweep phase (~10:45-11:03); the next
      test's backfill (id=4) started 11:03:27. Suite then executed
      idempotent_rerun (backfill_4, 3 runs, 11:03-11:10), mass-delete breaker
      (backfill_5), then the pod_kill (fixtures e2e-dbtkill run 461, e2e-u3 run 515),
      rebuild_from_raw (e2e-rebuild run 623), referential_orphan (e2e-orphan run 624
      SUCCEEDED), and smoke_and_idempotency (e2e-idempotent run 666 RUNNING) modules --
      1301 meta.files rows and 666+ ingestion runs by cancel vs 50/60 in ROUND 12."
    - "(4) BUDGET (c): FAILED at 120min -- full decomposition in
      round13_budget_decomposition below; this is the decision input."
    - "(5) REGRESSION GUARDS (d): MOSTLY GREEN, two flags. Kyverno DENY 0;
      control-plane restarts 0 (restart timeline EMPTY); dag-processor peak 771MiB;
      triggerer 419MiB; zero unwanted breaker trips in job log (fix (16) held: no
      vanished-mass false positives anywhere; the ONLY trips were the mass-delete
      test's DELIBERATE truncated-snapshot trip + its cron collateral, below).
      FLAG 1: FailedScheduling NOT zero -- transient burst 10:42:22-10:47:08, 7 pods
      (stage-4d21bnsk/ubwp6p9s/jgio5oqj/i53so4ae, publish-5sztukiv/5rha8oxg,
      discover-q8lhyk5t) all 'Insufficient cpu', first-ever customers+orders
      co-scheduling peak; self-healed via retries (backfill_3's first stage tries
      state=removed, re-ran try=2 successfully 10:54+; no test failed from it; ~9min
      recovery cost). FLAG 2: scheduler peak 1881MiB/26pids = 91.9% of the 2048Mi
      limit (was 1415MiB = 69% in R12) -- 167MiB headroom under orders-live load."
    - "(6) CENSUS (e): NOT MEASURABLE for the 3rd consecutive round -- pytest -q
      emitted zero flushed output before cancel (the carried observability rider).
      Sweep-assertion-(10) caveat: cannot be judged without the summary; carried.
      Indirect: the suite RAN PAST the sweep into 4 later modules, so the sweep test
      itself terminated (pass or fail unobservable)."
    - "(7) REFUTATION BRANCH: NOT taken -- orders triggered immediately once unpaused."
  round13_budget_decomposition: "Job 10:21:52 -> cancel 12:22:05 (2h00m13s of work):
      [1] 10:21:52-10:27:59 install+cluster-up = 6.1min.
      [2] 10:27:59-10:28:40 images Variable + migrations + vault = 0.7min.
      [3] 10:28:40 pytest starts (cluster tests + slice session setup; first cron
      DagRun 10:29:24).
      [4] ~10:30-10:45 pilot test: backfill_1 (2 runs) 10:30:04-10:43:02 = ~14min.
      [5] 10:42-10:47 FailedScheduling burst (first customers+orders co-scheduling);
      backfill_3's first tries removed -> ~9min recovery folded into [6].
      [6] ~10:45-11:03 full_2year_sweep: backfill_3 (3 runs) 10:54-11:02; customers
      wait + ORDERS WAIT BOTH DRAINED (orders replay 49-60 incl. days 12/13
      SUCCEEDED) = ~18min.
      [7] 11:03-11:10 idempotent_rerun: backfill_4 (3 runs) = 7min.
      [8] 11:10-11:55 MASS-DELETE WINDOW = ~45min, the avoidable sink:
      backfill__08:40 (backfill_5, = the offset_minutes=150 window) publish tripped
      the DESIGNED QualityThresholdExceeded, retried to skipped try=6
      (11:12->11:42:49), DagRun failed at +45:00 (11:55:54). COLLATERAL: the */1 cron
      run scheduled__11:09 ALSO discovered the truncated snapshot (fresh file in
      raw/customers/ enters the cron window within a minute), publish failed try=7
      over 42min (11:11:51->11:54:00), holding max_active_runs=1 -- cron gap
      11:09->11:53 stalled every subsequent cron-dependent test wait. Designed cost
      ~5min; actual cost ~45min; AVOIDABLE ~40min.
      [9] 11:55-12:22:05 four remaining modules: pod_kill (dbtkill 461, u3 515),
      rebuild_from_raw (623), referential_orphan (624 SUCCEEDED), and
      smoke_and_idempotency IN-FLIGHT at cancel (test_idempotent_reupload, run 666
      RUNNING) = 27min and nearly done.
      [10] Steady-state tax: cron runs 2.3-3.7min each (was 96-104s in R12) --
      orders co-scheduling doubles background load in perpetuity.
      PROJECTION: pytest alone needed ~10-15 more min => ~2h10m total from job start.
      The observability step (install trimmed monitoring + tests/e2e/observability +
      teardown) and the rebuild-from-raw capstone step NEVER STARTED -- unmeasured,
      rough estimate +25-40min combined. Projected honest job total AS-IS:
      ~2h35m-2h50m. With the ~40min collateral eliminated: ~1h55m-2h10m. 120min
      cannot hold either way without trimming AND margin."
  new_findings_named:
    - "(18a) DESIGN, measured price tag for the carried quarantine-vs-retry follow-up:
      retrying a DETERMINISTIC QualityThresholdExceeded breaker trip (publish
      retries=6 + capped backoff) burns ~40min of wall per poisoned DagRun on CI and
      holds the dag_id's max_active_runs=1 slot throughout; the mass-delete test
      makes this structural (its truncated snapshot is unavoidably also cron-visible).
      Fix directions (user decision): fail-fast/quarantine on breaker-class errors
      (production semantics -- needs approval), and/or test-scoped: make the
      mass-delete fixture invisible to the cron window (e.g., cleanup/finalize the
      poisoned file's PENDING run before the cron picks it up), and/or CI-profile
      retries reduction for publish."
    - "(18b) CAPACITY, watch-only: orders-live co-scheduling => transient
      Insufficient-cpu bursts (7 pods, 10:42-10:47, self-healed) and 2-3x slower
      steady-state runs; scheduler peak 91.9% of 2048Mi. No failures caused this
      round; flag for the next round's guards (a repeat WITH failures upgrades it)."
  next_action: "DECISION CHECKPOINT returned to user: choose ROUND 14 direction --
      (A) raise timeout-minutes to measured+margin (180min covers the as-is
      projection; 150min only if paired with trims); (B) trim the ~40min mass-delete
      collateral (test-scoped or production quarantine semantics -- the latter needs
      explicit approval) and keep/raise less; (C) combination A+B (e.g., collateral
      trim + 150min). Carried: quarantine-vs-retry design (now priced), sidecar
      mirror, pytest progress observability (3rd blind round -- consider bumping
      priority), sweep-wait legibility, sweep-assertion-(10) caveat (still
      unobserved), scheduler memory headroom watch (18b)."

ROUND 13 (2026-08-27, opened on user decision: A+B+C -- SUPERSEDED BY ROUND 13 OUTCOME ABOVE):
  charter: "Fix root cause (17) at all three layers, C explicitly user-approved as a
      production-semantics change: (A) test-scoped -- add csv_ingest_orders to
      tests/e2e/slice/conftest.py::_unpause_slice_dags and truth-up its docstring (the chaos
      suite auto-inherits via tests/e2e/chaos/conftest.py's import); (B) platform-scoped --
      unpause csv_ingest_orders at cluster-up in the Makefile so EVERY consumer of a
      freshly-booted ephemeral cluster (pytest suites, the rebuild-from-raw capstone, manual
      use) sees deterministic asset-trigger behavior; (C) DAG-definition --
      is_paused_upon_creation=False on the csv_ingest_orders @dag, recorded rationale: an
      asset-triggered downstream that silently drops events while paused violates the
      platform's no-silent-drops core value ('no data is ever silently dropped'). Survey
      requirement: apply C ONLY where the asset-triggered-downstream argument holds -- do not
      blanket-change schedule-based DAGs. timeout-minutes: 120 KEPT, measure the suite
      against it this round (budget arithmetic: 33min honest floor + post-fix orders drain
      plausibly fits). Standing rules: rounds 1-12 fixes stay; runner migration/job
      splitting retired; offline battery before push; single 60s watcher run by the session
      manager; judge by internals + census; commit docs per convention; carried follow-ups
      stay captured (quarantine-vs-retry design, sidecar mirror, pytest progress
      observability, sweep-wait legibility)."
  hypothesis: "H17-fix: with csv_ingest_orders unpaused end-to-end (born unpaused via C,
      belt-and-braces unpaused via A for pytest sessions and via B for every cluster-up),
      customers' ~60 asset events per run actually trigger orders DagRuns; orders pods
      appear on CI for the first time; _wait_for_dataset_files_terminal(dataset=orders,
      timeout=5400) drains instead of sitting 87.6min; the suite plausibly completes inside
      timeout-minutes: 120 at the measured post-(16) pace (steady-state runs 96-104s,
      backfills 2-8min)."
  dag_survey_for_C: "airflow/dags inventory (direct grep of every @dag schedule):
      csv_ingest_orders = schedule=[customers_asset] -- ASSET-SCHEDULED, C applies (a paused
      asset consumer drops events silently, unrecoverable except by manual unpause + event
      re-emission); csv_ingest_customers = cron '*/1 * * * *' -- pausing delays visibly (next
      tick runs on unpause), no silent drop, Airflow default KEPT; smoke_kubernetes_pod +
      platform_retention = '@daily' -- same cron reasoning, default KEPT; chaos_probe x3 =
      schedule=None -- manually-triggered only, pause state irrelevant, default KEPT.
      Conclusion: C applies to csv_ingest_orders ALONE."
  preregistered_criteria:
    - "(a) PRIMARY: orders pods appear on CI for the first time (etl-namespace rolling pod
      census shows csv_ingest_orders discover/stage/dbt_build/publish pods; meta.ingestion_runs
      gains dataset=orders rows)."
    - "(b) PRIMARY: test_full_2year_sweep_customers_and_orders drains -- the orders-terminal
      wait completes instead of consuming 87+ min; no 5400s wait exhaustion."
    - "(c) BUDGET: suite completes within timeout-minutes: 120 -- record the measured
      duration decomposition either way (this is the deferred timeout decision's input)."
    - "(d) REGRESSION GUARDS: breaker trips 0, FailedScheduling 0, Kyverno DENY 0,
      control-plane restarts 0, stage/dbt_build success try=1, scheduler peak < 2048Mi."
    - "(e) NODE-ID CENSUS vs the saturated 17-test baseline: expect substantial clearing.
      CARRIED CAVEAT: sweep assertion (10) (missing-customer invalidated on the final day's
      own pass) may legitimately still fail if days 11+12 consolidate into one publish pass
      (consolidation-semantics test-design issue, independent of (17) -- name it, do not
      treat it as a (17) refutation)."
  reasoning_checkpoint:
    hypothesis: "csv_ingest_orders registers is_paused=True on every fresh cluster (Airflow
        default dags_are_paused_at_creation=true, no repo override, no
        is_paused_upon_creation on the @dag) and NO repo code path unpauses it
        (_unpause_slice_dags covers only smoke+customers; Makefile unpauses only smoke,
        and only in smoke-verify which e2e-full.yml never runs) -- and a paused
        Asset-scheduled DAG silently consumes no asset events, so orders can never run on
        CI, which is exactly why zero orders pods appeared while ~60 customers_asset
        events were emitted in ROUND 12's run."
    confirming_evidence:
      - "STRUCTURAL (source-read): csv_ingest_orders.py schedule=[customers_asset], no
        is_paused_upon_creation; repo-wide grep confirms no dags_are_paused_at_creation
        override and no unpause site reaching csv_ingest_orders (slice conftest tuple =
        smoke+customers only; Makefile:440 = smoke only, inside smoke-verify)."
      - "BEHAVIORAL (run 33051719850): customers' publish (outlets=[customers_asset])
        succeeded ~60 times across the run, yet the full-run etl pod monitor shows ZERO
        orders pods ever; the sweep's orders-terminal wait sat 87.6min to job cancel."
      - "DIFFERENTIAL (live query 2026-08-27): the long-lived LOCAL cluster's
        csv_ingest_orders is_paused='False' -- hand-unpaused in an earlier session, state
        persisted; ephemeral CI starts fresh+paused. Same
        local-hand-state-vs-ephemeral-CI class as root cause (12), which was confirmed."
    falsification_test: "If, on a run with the fix verifiably in force (orders registered
        unpaused -- checkable via the always()-diagnostics DB dumps), orders DagRuns STILL
        never trigger despite customers publish emitting asset events, the paused-DAG
        attribution is wrong and something else drops the events (return to
        investigation). Pre-registered criteria (a)/(b) are the live falsification."
    fix_rationale: "Root-cause layer, not symptom: the paused flag IS the mechanism (a
        paused DAG's asset events are dropped by design in Airflow's scheduler), so
        unpausing at DAG definition (C) removes the failure mode for every fresh
        deployment -- production-shaped, matching the platform's no-silent-drops core
        value. A and B are defense-in-depth for the two consumer classes that previously
        relied on hand state (pytest sessions; cluster-up consumers incl. the capstone) and
        keep the guarantee even if C's flag is ever regressed or a cluster's DagModel row
        predates the flag. NOT touching schedule-based DAGs (survey above), NOT changing
        timeout-minutes (measure first), NOT reverting any prior round's fix."
    blind_spots: "(1) Sweep assertion (10) consolidation caveat carried from ROUND 12 --
        judged from the run, not blocking. (2) Suite duration vs 120min is a measurement,
        not a guarantee -- orders' own drain (backfill-equivalent volume through the
        global stage slot) has never been observed on CI; if it structurally cannot fit,
        that is a NEW budget finding, not a (17) refutation. (3) Whether further residuals
        surface beneath a running orders pipeline (whack-a-mole precedent: 6 layers so
        far). (4) The 21 offline tests/integration failures on this machine are
        pre-existing and out of scope."
  live_verification_state: "RECORDED 2026-08-27T10:25Z: fix (17) pushed as commit 4d3db56
      (base 4bc09b1). AUTHORITATIVE ROUND 13 live-verification run: e2e-full.yml run
      33062702180 (headSha 4d3db56, created 2026-08-27T10:21:48Z, in_progress at
      recording time). Companion runs, same headSha: publish.yml 33062702191 ALREADY
      conclusion=success (criterion 0 image-race check PRE-CLEARED -- csv-processor,
      dbt AND airflow images rebuilt from 4d3db56, which carries the
      is_paused_upon_creation flag in the DAG file, before the e2e cluster pulls; note
      the DAG itself also reaches the CI cluster via the hostPath mount, zero
      staleness); CI 33062702164 failure = job-for-job IDENTICAL conclusion pattern to
      the 794db33 baseline (Quality gate + Integration tests failing, all else green --
      the SAME pre-existing out-of-scope failures, zero ROUND 13 regressions at job
      level); e2e-chaos 33062702107 out of scope for this signature. The docs push
      recording this state uses [skip ci] (ROUND 7 lesson -- no supersession).
      POST-RUN ANALYSIS STEPS (for the continuation agent once the session manager's
      single 60s watcher reports terminal), judged on the ROUND 13 pre-registered
      criteria via the always()-diagnostics:
      (1) FIX-IN-FORCE probe: csv_ingest_orders must register UNPAUSED (cluster-up log
      shows the ROUND 13 unpause line succeeding AND/OR the DagModel/diagnostics show
      is_paused=false; the DAG file at 4d3db56 carries the flag);
      (2) PRIMARY (a): orders pods appear on CI for the first time -- rolling
      etl-namespace pod census shows orders discover/stage/dbt_build/publish pods;
      meta.ingestion_runs gains dataset=orders rows;
      (3) PRIMARY (b): test_full_2year_sweep_customers_and_orders' orders-terminal wait
      (timeout=5400) DRAINS instead of consuming 87+ min;
      (4) BUDGET (c): suite duration measured against timeout-minutes: 120 -- record the
      full decomposition either way (the deferred timeout decision's input);
      (5) REGRESSION GUARDS (d): breaker trips 0, FailedScheduling 0, Kyverno DENY 0,
      control-plane restarts 0, stage/dbt_build success try=1, scheduler peak < 2048Mi;
      (6) CENSUS (e): pytest node-ID census vs the saturated 17-set -- expect
      substantial clearing; CARRIED CAVEAT: sweep assertion (10) may legitimately still
      fail on days-11+12 publish-pass consolidation (consolidation-semantics test-design
      issue, independent of (17) -- name it, do not treat it as a (17) refutation);
      (7) If orders is verifiably unpaused AND customers publish emits asset events yet
      orders DagRuns still never trigger, (17)'s attribution is wrong -- return to
      investigation."
  next_action: "Await the session manager's watcher on run 33062702180; then post-run
      analysis per live_verification_state. Carried decision follow-ups for the user:
      quarantine/park-vs-retry on deterministic breaker trips; sidecar mirror (follow-up
      B); pytest progress observability (-q dots never flush in a cancelled job);
      sweep-wait legibility (module worst-case waits vs job budget)."

ROUND 12 OUTCOME (2026-08-27, post-run analysis of run 33051719850 -- SUPERSEDED BY ROUND 13
    ABOVE, retained as the root-cause-(17) evidence record):
  verdict: "BRANCH (b): fix (16) LIVE-CONFIRMED IN FULL; the 120 minutes were consumed by a NEW,
      previously-masked mechanism = ROOT CAUSE (17): csv_ingest_orders never unpaused on
      ephemeral CI clusters. This is NOT a DagRun wedge (zero wedges this run) -- it is a
      test-level 5400s wait polling for a dataset whose DAG can never run."
  round12_criteria_scorecard:
    - "(1) FIX-IN-FORCE probe: MET exactly. meta.schema_versions = v1 CONTRACT
      (08:05:08, valid_to 08:07:06) + v2 INFERRED (08:07:06, open) -- the predicted ''->'2'
      term flip. Run->file mapping shows the full predicted 3-wave shape: pass1 runs 1-12
      (10 SUCCEEDED + days-12/13 PENDING), pass2 runs 13-24 (same shape, replay wave),
      wave3 runs 25-34 with replay_of_run_id set (13,14,3,4,5-10), runs 35/36 = days 12/13
      SUCCEEDED. silver _run_id distribution: 35->1 row, 36->49 rows = deterministic
      newest-run winner (16c tie-break working; contrast ROUND 11/12's arbitrary 26/24 and
      23/27 splits). Run 36 delivered 49 bronze keys and published CLEAN (1/50=2% vanished
      under bronze-scoped 16a, below 10%)."
    - "(2) PRIMARY: MET. ZERO mass_delete_circuit_breaker / QualityThresholdExceeded /
      vanished hits anywhere in the 5939-line log. 63 customers DagRuns: 62 success + 1
      running at cancel -- first run in 13 CI runs where EVERY DagRun succeeded. Zero
      +45:00 deaths. publish state=success try=1 in every run that ran it. 344 TIs: 226
      success, 114 skipped (empty-window stage/dbt short-circuit, correct), 0 failed, 0
      retried."
    - "(3) REGRESSION GUARDS: ALL GREEN. FailedScheduling census EMPTY; Kyverno DENY 0;
      restart-count timeline EMPTY (0 restarts); scheduler peak 1415MiB/24pids < 2048Mi;
      dag-processor 872MiB; triggerer 426MiB. Stage 32/32 ingestion-run deliveries
      succeeded try=1 (~30s each); dbt_build success try=1 in every run that reached it."
    - "(4) BUDGET MEASUREMENT: honest-work floor = 33min (cluster-up 07:54:59->08:01:42 =
      6.7min; migrations+vault ~0.6min; pytest 08:02:15; corpus upload 08:03:07; cluster
      tests + pilot backfill (2 runs: 7m50s+7m53s) done 08:19:15; full-sweep backfill
      (3 runs: 3m41s+2m04s+1m59s) done 08:27:38; customers fully drained SUCCEEDED incl.
      files 12/13 by ~08:27). The REMAINING 87.6min (08:27:38->09:55:12) was ONE wait:
      _wait_for_dataset_files_terminal(dataset=orders, timeout=5400) -- job timeout fired
      ~2min before the test's own 90-min deadline would have failed it legibly. Post-fix
      DagRun pace is FAST: steady-state scheduled runs 96-104s each; backfill runs
      2-8min each."
    - "(5) NODE-ID CENSUS: NOT MEASURABLE -- pytest -q emitted zero flushed output before
      cancel (progress dots never complete a line; no summary block exists in a cancelled
      job). Sweep-assertion-(10) caveat: NEVER REACHED (test still inside wait 3); carry
      the caveat forward to ROUND 13."
  root_cause_17_evidence: "(i) STRUCTURAL: airflow/dags/csv_ingest_orders.py schedule=
      [customers_asset], no is_paused_upon_creation override; repo has no
      dags_are_paused_at_creation override anywhere -> Airflow default TRUE -> orders
      registers PAUSED on every fresh cluster. tests/e2e/slice/conftest.py::
      _unpause_slice_dags (session-autouse) unpauses ONLY (_SMOKE_DAG_ID,
      _CUSTOMERS_DAG_ID) -- its docstring says 'both this phase's DAGs' but orders is a
      phase DAG too and is absent; repo-wide grep: NO other unpause site for
      csv_ingest_orders (Makefile:440 unpauses only smoke). (ii) BEHAVIORAL: customers'
      publish (outlets=[customers_asset], customers DAG line 192) emitted asset events
      ~60 times across the run (2 pilot + 3 sweep backfill publishes + ~56 steady-state
      publishes, one KPO publish pod per ~96s scheduled run) yet ZERO orders pods appear
      in the full-run etl monitor (every observed pod maps to customers: 1 discover + 1
      publish per steady-state run, stage/dbt clusters only 08:04-08:25). A paused DAG
      consumes no asset events -- silence by design, exactly the failure shape
      _unpause_slice_dags's own docstring warns about for customers. (iii) DIFFERENTIAL:
      live query of the long-lived LOCAL cluster (kind-airflow-platform, 2026-08-27):
      csv_ingest_orders is_paused='False' -- hand-unpaused during some earlier phase's
      live session, state persisted; ephemeral CI starts fresh+paused. Same
      local-hand-state-vs-ephemeral-CI class as root cause (12) (analytics_db_default).
      Only unobservable link: the dead CI cluster's is_paused flag itself; every other
      link is direct."
  riders_and_notes: "(a) pytest silence is an OBSERVABILITY defect of the harness, not a
      bug: -q dot-progress never flushes a partial line, so a cancelled job shows zero
      test progress; consider -v or --timeout-style progress output for CI (decision
      item, cosmetic). (b) The customers */1 schedule + ~96s runtime means CI burns one
      no-op KPO publish pod per ~1.6min in perpetuity after drain -- not a bug (publish
      must run to emit the asset event that feeds orders), but it is permanent background
      load worth knowing about. (c) sweep test module's waits are 1800-5400s -- the
      trigger's '180s fixed timeouts' premise no longer describes tests/e2e/slice/
      test_backfill_2year_sweep.py; the job-level 120min budget and the module's summed
      worst-case waits (up to ~4.5h) are structurally inconsistent -- fixing (17) makes
      the happy path fast, but any future silent-DAG regression re-produces exactly this
      illegible cancel."
  next_action: "DECISION CHECKPOINT returned to user: choose ROUND 13 direction --
      (A) test-scoped fix: add csv_ingest_orders to _unpause_slice_dags (1-line + docstring
      truth-up; auto-covers chaos suite via its conftest import); (B) platform-scoped:
      unpause orders at cluster-up in Makefile next to smoke (covers the D-24
      rebuild-from-raw capstone step independent of pytest fixtures); (C) DAG-definition
      is_paused_upon_creation=False (production-semantics change -- requires explicit
      user approval per charter). Also carry: quarantine-vs-retry design follow-up
      (unchanged), sweep-assertion-(10) caveat (unobserved, carried), pytest progress
      observability (cosmetic)."

ROUND 12 (2026-08-27, opened on user decision: A+B -- mechanism AND design -- SUPERSEDED BY
    ROUND 12 OUTCOME ABOVE):
  charter: "A leg (mechanism): root-cause residual (16) -- the deterministic 54% vanished-key
      mass_delete_circuit_breaker trip that self-sustains the publish wedge. Close the
      evidence gap first (bronze _run_id->file mapping dump in diagnostics and/or local
      repro against a fresh warehouse); test the leading hypothesis (re-publication of an
      older day-window snapshot against gold evolved past it). Riders: why publish ends
      skipped try=6; why wedged runs sat to exactly +45:00 after publish resolved.
      B leg (design): compute per-window churn for seed v5's 12-file corpus analytically --
      does any legal re-publish ordering structurally exceed the 10% threshold? Assess
      whether a breaker trip should quarantine/park instead of retrying a deterministic
      quality-gate trip (retry-on-deterministic-trip is the wedge fuel); any
      production-semantics change requires a decision checkpoint BEFORE implementation
      (CI/test-config-scoped fixes may land directly). Timeout decision stays deferred to
      post-fix measurement. Standing rules: rounds 1-11 fixes stay; runner migration/job
      splitting retired; offline battery before push; one 60s watcher (session manager);
      judge by internals + census; commit docs per convention."
  hypothesis: "H16: publish's vanished-key computation identifies 'this pass's snapshot' as
      silver rows WHERE _run_id = ANY(staged_run_ids) (delete_detection._VANISHED_SQL).
      silver_customers dedups per key by (event_ts desc, _source_row_number desc, _file_id
      desc) and the winning row keeps ITS OWN _run_id -- so any publish pass whose staged
      rows LOSE the dedup to already-resident newer-day rows sees those keys as 'vanished'
      even though its own staged files contain them. Combined with (a) discovery's
      max_units_per_run=10 cap over a 12-file corpus (candidates sorted deterministically,
      cap trims the tail) and (b) tripped passes never finalizing their runs (files stay
      un-SUCCEEDED, re-discovered forever), the trip is deterministic and self-sustaining.
      EVIDENCE SO FAR (direct, from round11-job.log TI history): EVERY DagRun staged 10
      mapped files (map 0-9) -- including runs AFTER backfill__10:24's publish finalized 10
      runs SUCCEEDED at 18:50:51 (10:25's discover at 18:51:54 still emitted 10 units, which
      contradicts a simple '2 leftovers' model and is not yet explained -- the exact
      run->file mapping is the remaining gap). The 54%=27/50 arithmetic is not yet
      reproduced from first principles; needs the local replay."
  falsification_criteria_preregistered:
    - "(a) Local replay of the corpus in CI ordering (batches of 10, publish between
      batches) either reproduces a >10% vanished ratio on the second publish pass
      (mechanism confirmed, exact numbers explain 27/50) or it does not (H16 attribution
      wrong -- return to investigation)."
    - "(b) For the live verification run (if a CI/test-config fix lands): zero breaker
      trips OR breaker trips handled without wedging; multiple complete DagRuns; no
      +45:00 dagrun_timeout deaths."
    - "(c) Regression guards hold: stage/dbt successes try=1 persist, FailedScheduling 0,
      Kyverno DENY 0, control-plane restarts 0."
    - "(d) Suite duration measured against the 120-min budget (post-fix measurement for
      the deferred timeout decision)."
    - "(e) Node-ID census: expect substantial clearing of the saturated 17-set."
  reasoning_checkpoint:
    hypothesis: "H16 (refined): the vanished-key computation's silver-scoped staged
        snapshot misreads dedup-tie-loser keys as vanished on any byte-identical replay
        wave; the replay wave itself is deterministic on a fresh cluster (schema term
        '' -> '2' after pass 1); the trip aborts finalization, making the wave permanent."
    confirming_evidence:
      - "Local red reproduction: fresh PG18 + alembic + real dbt build + real
        SCDPublisher, byte-identical replay wave -> 24/50 = 48% vanished,
        QualityThresholdExceeded at threshold 0.10 (pre-fix); 26/24 arbitrary tie split
        measured directly in silver _run_id lineage."
      - "round11-job.log TI history: every DagRun's discover emitted 10 units including
        one minute after 10 runs were finalized SUCCEEDED -- only explicable by an
        idempotency-key term change (schema_version_term '' -> '2'), source-confirmed in
        discovery.py line 909 + csv_processor source.py _resolve_schema."
      - "Byte-identical 54% (27/50) trips across all observed tries and passes in the
        live run -- matches the frozen-silver arithmetic exactly (no new bronze after the
        replay wave's build -> ratio frozen)."
    falsification_test: "Pre-registered (a): if the local replay had shown vanished==0
        (ties all breaking toward the new runs), H16's attribution would be wrong. It
        showed 48% -- H16 confirmed. Live criteria (b)-(e) above remain the CI-level
        falsification for the fix."
    fix_rationale: "Root-cause layer, not symptom: the staged snapshot's DEFINITION was
        wrong (silver lineage proxy instead of the pass's delivered bronze keys) -- the
        fix restates it as bronze-scoped, which is exact by construction and matches
        Step B's existing _TOUCHED_KEYS_SQL scoping; the dbt _run_id tie-break separately
        removes the nondeterminism (section 67) rather than masking it. NOT raising the
        threshold, NOT shrinking the corpus (B-leg analysis: genuine churn is 2%max --
        the data was never the problem), NOT touching retries/timeouts."
    blind_spots: "(1) Sweep assertion (10) (missing-customer invalidated on the final
        day's own pass) requires day 12 to publish in a pass whose bronze union lacks
        member 32 -- if days 11+12 consolidate into one pass (likely under cap-10
        batching: pass 3 = exactly those 2 files together), the closure never fires and
        that node-ID stays red for a CONSOLIDATION-SEMANTICS reason independent of (16);
        judge from the run, name it if observed. (2) The replay wave adds one full extra
        pass (~3-7min) to every fresh-cluster suite -- expected D-18 behavior, budgeted.
        (3) The 21 full-directory tests/integration failures on the LOCAL machine are
        PRE-EXISTING (byte-identical failure set on clean HEAD, A/B-verified twice) --
        local-env/order condition, out of scope. (4) Whether further residuals surface
        beneath the unwedged publish (whack-a-mole precedent) -- post-run analysis."
  investigation_state: "COMPLETE -- root cause (16) CONFIRMED with a local red/green
      reproduction. Full causal chain (every link source-read + the core link reproduced
      empirically): (1) first discovery on a fresh cluster mints idempotency keys with
      schema_version_term='' (no meta.schema_versions row exists yet -- discovery.py line
      909); (2) staging registers v1 (CONTRACT baseline, csv_processor source.py
      _resolve_schema) and day-5+'s extra-column files record v2 (INFERRED; days 6-12
      no-op on the same hash; pass-2 restages of S1 days resolve_by_hash to historical v1
      -- NO flip-flop, version is stable at 2 after pass 1); (3) the NEXT discovery
      computes keys with term '2' != '' -> ALL 12 files re-eligible -> full replay wave
      with replay_of_run_id lineage (D-18, BY DESIGN -- explains the observed 10 units on
      every discover incl. 10:25's at 18:51:54, one minute AFTER 10 runs were finalized
      SUCCEEDED); (4) replayed bronze rows are BYTE-IDENTICAL to wave-1 rows: same
      event_ts, same _source_row_number, same _file_id (create_file idempotent by
      object_uri) -- only _run_id differs; (5) silver_customers.sql ranks (event_ts desc,
      _source_row_number desc, _file_id desc) -> FULL TIE between resident and replayed
      row per key -> ARBITRARY winner keeps its own _run_id (LOCAL REPRO: 26/24 split;
      CI: 23/27); (6) _VANISHED_SQL defined 'this pass's snapshot' as silver rows with
      _run_id ANY staged_run_ids -> every tie-loser key reads vanished -> 27/50=54%>10%
      trip (LOCAL REPRO: 24/50=48% -- the split is genuinely arbitrary, the >10% trip is
      structurally guaranteed at roster scale); (7) QualityThresholdExceeded rolls back
      the whole publish tx -> runs stay STAGED -> identical staged_run_ids + frozen
      silver next pass -> byte-identical trip forever (self-sustaining poison confirmed).
      BOTH RIDERS RESOLVED, no separate bugs: 'skipped try=6' = dagrun_timeout's
      force-skip of the up_for_retry TI (the TI dump shows try 6's own start/end with
      the overwritten state); 'sat to +45:00' = try 7's exponential-backoff delay
      (30s base doubling -> ~16min after try 6) reached past dagrun_timeout -- publish
      had NOT resolved at 19:22, it was waiting for try 7. B-LEG ANSWERS: corpus churn
      analysis -- roster is FIXED at 50, resent in full every non-gap day; the only
      genuine key churn is the missing-customer member (1/50 = 2% from day 12) -- NO
      legal re-publish ordering can exceed the 10% threshold under a CORRECT vanished
      computation; corpus and threshold are correctly sized, ROUND 10's shrink did NOT
      structurally break anything, no corpus/threshold change made. The deliberate
      mass-delete fixture (15/50 = 30%) still trips post-fix (its staged file genuinely
      lacks those keys)."
  fixes_implemented: "(16a) dataplat/scd/delete_detection.py _VANISHED_SQL: staged_snapshot
      CTE now reads staging.customers (bronze) scoped by staged_run_ids -- bronze IS the
      pass's delivered key set by construction, immune to silver dedup-tie lineage; +
      NULL-guard in the CTE (a NULL inside NOT IN would silently empty the vanished set).
      (16b) load/publish/scd.py _SNAPSHOT_MAX_EVENT_TS_SQL: same silver->staging rescope
      (the effective-dating timestamp had the same tie hazard: silver scoped to a
      tie-losing pass could return NULL/partial max). (16c) dbt silver_customers.sql +
      silver_orders.sql: ranking gains a final '_run_id desc' tie-break -- deterministic
      winner (newest run) under byte-identical replays; fixes the section-67 determinism
      violation independently (silver lineage was nondeterministic under replay).
      (16d) NEW regression test tests/integration/test_scd_replay_delete_detection.py
      (own fresh PG18 container + alembic + REAL dbt builds + real SCDPublisher at
      threshold 0.10): red/green-proven -- pre-fix it reproduces the trip (24/50=48%
      vanished on a byte-identical replay wave), post-fix vanished==0 and publish
      succeeds; also logs the tie split (post-16c: 50/0 deterministic).
      (16e) tests/integration/test_scd_delete_detection.py updated to bronze-scoped
      semantics (vanish = absent from this pass's staged BRONZE) incl. a dedicated (16)
      unit-level guard test_replayed_key_with_stale_silver_lineage_is_never_vanished.
      (16f) e2e-full.yml always()-diagnostics: ROUND 12 block dumps
      meta.ingestion_runs->meta.files mapping (+replay_of_run_id), meta.schema_versions
      history, silver _run_id distribution, and per-run bronze counts via psql in the
      CNPG primary -- closes ROUND 11's named evidence gap for all future runs.
      DELIBERATELY NOT implemented (production semantics -- decision checkpoint item):
      quarantine/park-on-deterministic-breaker-trip instead of retrying (retries=6 with
      exponential backoff is still wedge fuel if any future deterministic trip appears);
      with (16) fixed CI has no deterministic trips left, so this is a design follow-up,
      not a blocker. No timeout-minutes change (per charter: post-fix measurement
      first; arithmetic says ~3 passes x 3-7min + sweep backfills should fit 120min)."
  live_verification_state: "RECORDED 2026-08-27T07:56Z: fix (16) pushed as commit 794db33
      (base 89bacea). AUTHORITATIVE ROUND 12 live-verification run: e2e-full.yml run
      33051719850 (headSha 794db33, created 2026-08-27T07:54:53Z, in_progress at
      recording time). Companion runs, same headSha: publish.yml 33051719760 ALREADY
      conclusion=success (criterion 0 image-race check PRE-CLEARED -- both the
      csv-processor image (carries dataplat fixes 16a/16b) and the dbt image (carries
      16c via COPY dbt/) rebuilt from 794db33 before the e2e cluster pulls); CI
      33051719761 failure = the SAME 21 pre-existing integration failures as the local
      clean-HEAD baseline (byte-identical list; out of scope per charter, zero ROUND 12
      regressions); e2e-chaos 33051719758 out of scope for this signature. The docs
      push recording this state uses [skip ci] (ROUND 7 lesson -- no supersession).
      POST-RUN ANALYSIS STEPS (for the continuation agent once the session manager's
      single 60s watcher reports terminal), judged on the ROUND 12 pre-registered
      criteria via the always()-diagnostics:
      (1) FIX-IN-FORCE probe: the ROUND 12 diagnostics block must show
      meta.schema_versions history (expect v1 CONTRACT + v2 INFERRED) AND the
      run->file mapping with replay_of_run_id lineage; silver _run_id distribution
      should show the replay wave's runs winning deterministically (16c);
      (2) PRIMARY: zero mass_delete_circuit_breaker trips (grep vanished-key) OR any
      trip NOT wedging (no publish retries-exhausted); MULTIPLE complete DagRuns
      (state=success), publish success try=1 in each; NO DagRun failing at exactly
      start+45:00;
      (3) REGRESSION GUARDS: stage/dbt_build successes try=1 persist, FailedScheduling
      census 0, Kyverno DENY 0, control-plane restarts 0, scheduler peak < 2048Mi;
      (4) BUDGET MEASUREMENT (deferred timeout decision input): total pytest wall vs
      the 120-min job budget; expect ~1 extra full replay pass (~3-7min, D-18 by
      design) + the days-11/12 pass;
      (5) SECONDARY: pytest node-ID census vs the saturated 17-set -- expect
      substantial clearing; PRE-REGISTERED CAVEAT: sweep assertion (10)
      (missing-customer invalidated on the final day's own pass) may legitimately
      still fail if days 11+12 consolidate into one publish pass (bronze union then
      contains member 32) -- that is a consolidation-semantics test-design issue
      INDEPENDENT of (16); name it, do not treat it as a (16) refutation;
      (6) If dbt_build or publish fails with the 794db33 images verifiably pulled and
      vanished-key text present, (16)'s fix attribution is wrong -- return to
      investigation."
  next_action: "Await the session manager's watcher on run 33051719850; then post-run
      analysis per live_verification_state; decision checkpoint follow-up item for the
      user: quarantine/park-vs-retry on deterministic breaker trips (production
      semantics, proposed NOT implemented)."

ROUND 11 (2026-08-25, opened on user decision: SCOPE B -- SUPERSEDED BY ROUND 12 ABOVE):
  charter: "User-chosen scope B: (1) 15a silver_orders/dedup_audit null dataset_id -- fix the
      orders dataset_id resolution on a fresh cluster at the right layer (registration
      ordering vs seed vs the model's lookup), NOT a nullable-column workaround. (2) 15b
      reconciliation_customers permission denial -- make the compiled SQL use migration 0028's
      lookup function instead of reading meta.datasets directly; do NOT grant dbt_app SELECT.
      (3) Determine whether (15) is latent locally (orders pre-registered? more privileged
      role?) with direct evidence, record in Evidence -- if latent, state explicitly that this
      is a production-shaped defect CI caught. (4) Test artifacts: fix
      test_smoke_dag_xcom_contains_built_sha's full-vs-short sha comparison (shape bug, image
      is correct) and test_no_extra_schemas_exist's stale allowlist (add 'meta' after
      confirming migrations create it). DEFERRED to post-run analysis (user-accepted):
      measured-floor budget assessment for discovery windows/backfill collisions -- judge from
      this round's verification run data. Standing rules unchanged: rounds 1-10 fixes stay; no
      slot redesign; runner migration out of scope; follow-up B (sidecar mirror) captured;
      offline battery before push; one 60s watcher (run by session manager, NOT this agent);
      judge by internals + census, node-ID diff secondary (expect the saturated 17-set to
      START clearing if the knock-on theory is right -- note exactly which node-IDs go
      green); commit docs per convention."
  hypothesis: "Fixing (15a)+(15b) unblocks dbt_build (first-ever dbt_build success on CI) ->
      the 45-min dbt-retry dagrun_timeout wedge disappears -> max_active_runs=1 slot frees ->
      the knock-on templates (7x discovery-window, 3x AlreadyRunningBackfill, publish-never-
      ran data-state asserts, DBT_BUILD-never-RUNNING) collapse."
  falsification_criteria_preregistered:
    - "(a) dbt_build reaches state=success on CI (first ever)."
    - "(b) ZERO dedup_audit not-null violations and ZERO permission-denied errors in dbt
      pod logs."
    - "(c) DagRuns complete well under dagrun_timeout=45min (no wedge)."
    - "(d) Regression guards hold: stage successes persist (try=1, ~30s), FailedScheduling 0,
      Kyverno DENY 0, control-plane restarts 0."
    - "(e) Node-ID diff: expect the saturated 17-test signature to START clearing; record
      exactly which node-IDs go green + measured floors for discovery windows/backfill
      collisions (deferred budget assessment input)."
  investigation_state: "IN PROGRESS. Confirmed so far by direct source reads: (15a) mechanism
      -- dbt/macros/dedup_audit_post_hook.sql inserts ONE audit row UNCONDITIONALLY per
      invocation, resolving dataset_id via meta.dataset_id_for_name('{dataset}') (migration
      0028 SECURITY DEFINER fn returning NULL for unregistered names); meta.datasets rows are
      created ONLY by ingestion-side code (dataplat config/registry.py sync() upsert +
      metadata/postgres.py get_or_create_dataset -- no config-sync DAG exists yet, registry
      docstring says planned), so a fresh cluster where orders has never ingested has no
      'orders' row; dbt build (whole project, triggered from csv_ingest_customers) still runs
      silver_orders whose post-hook then inserts dataset_id=NULL vs NOT NULL FK (migration
      0024). Sibling macro reconciliation_post_hook.sql ALREADY documents+implements the
      no-phantom-row-on-empty-input behavior (bronze_files cross join). CRITICAL coupling
      found: reconciliation's prior_watermark EXCLUDES the current build's own dedup_audit
      row by identity (dedup_audit_id < max(dedup_audit_id)), which RELIES on dedup's
      unconditional insert -- so a naive skip-when-new_bronze-empty guard in dedup would
      lower recon's floor on every no-op build and re-write duplicate reconciliation rows.
      Right-layer fix: guard dedup's audit insert on DATASET RESOLVABILITY (where
      meta.dataset_id_for_name(...) is not null) -- unregistered implies staging empty
      implies pure no-op row, and recon's floor logic is provably unaffected in that branch
      (max(dedup_audit_id) over zero rows -> NULL -> empty prior set -> floor 0 -> bronze_files
      empty -> zero rows). (15b) mechanism -- dbt/tests/reconciliation_customers.sql (and
      reconciliation_orders.sql, same defect) JOIN meta.datasets directly; dbt_app has
      SELECT+INSERT on meta.reconciliation_results (migration 0032) and EXECUTE on the 0028
      fn, but zero grant on meta.datasets (D-08 boundary) -> rewrite the join as
      rr.dataset_id = meta.dataset_id_for_name('customers'/'orders')."
  reasoning_checkpoint:
    hypothesis: "(15a) dedup_audit_post_hook's unconditional audit INSERT resolves
        dataset_id via meta.dataset_id_for_name(), which returns NULL for a dataset never
        registered in meta.datasets -- and registration happens ONLY via that dataset's own
        ingestion path, so a whole-project dbt build on a fresh cluster fails silver_orders
        with a NOT NULL violation before orders ever ingests. (15b) the two singular
        reconciliation tests JOIN meta.datasets directly, which the least-privilege dbt_app
        role deliberately cannot SELECT (D-08/migrations 0021/0028). Together they make
        dbt_build fail deterministically every try -> 45min dagrun_timeout wedge -> the
        knock-on templates."
    confirming_evidence:
      - "Round10 dbt pod output: exact NOT NULL failing row (dataset 'orders',
        dataset_id null) + exact 'permission denied for table datasets' in
        reconciliation_customers -- read directly from the run log."
      - "Direct source reads: unconditional INSERT in dedup_audit_post_hook.sql; NOT NULL
        FK in migration 0024; only two meta.datasets writers repo-wide, both
        ingestion-side; both singular tests join meta.datasets; 0032 grants dbt_app
        reconciliation_results but nothing grants meta.datasets."
      - "Direct LOCAL reproduction: SET ROLE dbt_app -> the join errors 'permission denied
        for table datasets' on the live local warehouse; dataset_id_for_name works as
        dbt_app (1/76); local orders pre-registered (latent 15a); local dbt_build zero
        successes since 2026-08-20 (15b live locally, not latent)."
    falsification_test: "Pre-registered for the ROUND 11 live run (internals primary,
        node-IDs secondary): (a) dbt_build reaches state=success on CI (first ever);
        (b) ZERO dedup_audit not-null violations and ZERO permission-denied errors in dbt
        pod logs; (c) DagRuns complete well under dagrun_timeout=45min (no wedge);
        (d) regression guards hold (stage successes try=1 persist, FailedScheduling 0,
        Kyverno DENY 0, control-plane restarts 0); (e) census/node-ID diff -- the 17-set
        should START clearing; record which node-IDs go green + measured discovery-window/
        backfill floors for the deferred budget assessment. If dbt_build STILL fails with
        both fixes verifiably in the image, (15)'s attribution is wrong."
    fix_rationale: "Root-cause-level, not symptom-level: (15a)'s guard makes the audit
        write conditional on the dataset being REGISTERED -- the exact predicate whose
        violation produced the NULL -- rather than nulling the column, seeding data from
        migrations (structure/data boundary violation), or granting dbt registration
        rights; the unregistered=>empty-staging invariant means the skipped row is always
        a zero-information no-op row, and the recon-floor coupling analysis proves sibling
        behavior is unperturbed. (15b) switches the tests to the purpose-built 0028 lookup
        function -- the project's own documented least-privilege interface -- instead of
        widening dbt_app's grants. Artifacts: prefix-compare proves image-commit identity
        under BOTH bake formats; 'meta' allowlisting matches what migrations 0001+0038
        deliberately create/expose."
    blind_spots: "(1) dbt's partial-parse cache could in principle serve a stale compiled
        test -- mitigated: the dbt image is rebuilt from scratch per commit (COPY dbt/).
        (2) The recon-floor analysis for the unregistered branch is source-derived, not
        yet empirically run against a live dbt build on a fresh warehouse -- offline
        battery includes a fresh-schema simulation of both macro paths if feasible via
        testcontainers integration tests. (3) A dbt_build success on CI exposes publish
        and downstream hops to first-ever CI execution -- new residuals may surface
        beneath (per this session's whack-a-mole precedent). (4) The 180s windows may
        still miss at 12-file depth even unwedged -- that is the deferred measured-floor
        branch, NOT a refutation of (15)."
  fixes_implemented: "ALL FOUR IMPLEMENTED + offline-verified (see Resolution fix (15) and
      its verification entry, incl. the red/green falsification of the 15a guard against a
      fresh-database dbt build reproducing the exact CI NOT NULL error). Pushed as commit
      377c068 (base 50d80b6)."
  live_verification_state: "RECORDED 2026-08-25T18:36Z: fix (15) pushed as commit 377c068.
      AUTHORITATIVE ROUND 11 live-verification run: e2e-full.yml run 32884691063 (headSha
      377c068, created 2026-08-25T18:35:59Z, in_progress at recording time). Companion
      runs, same headSha: publish.yml 32884691018 (must complete build+sign BEFORE the e2e
      cluster pulls -- verify its conclusion as POST-RUN CHECK ITEM 0), CI 32884691069,
      e2e-chaos 32884690971 (both out of scope for this signature). The docs push
      recording this state uses [skip ci] per the ROUND 7 lesson -- no supersession risk.
      POST-RUN ANALYSIS STEPS (for the continuation agent, once the session manager's
      single 60s-interval watcher reports terminal), judged on INTERNAL diagnostics per
      the ROUND 11 pre-registered falsification criteria (node-ID diff SECONDARY):
      (0) publish.yml 32884691018 conclusion must be success (image race check; the dbt
      image MUST be rebuilt from 377c068 -- both fixes live inside the dbt image via
      COPY dbt/);
      (1) FIX-IN-FORCE probe: the dbt-build pods' logs must show NO 'null value in column
      dataset_id' and NO 'permission denied for table datasets' -- their presence with the
      377c068 dbt image verifiably pulled REFUTES the (15) fix;
      (2) PRIMARY -- first-ever dbt_build state=success on CI (TI dump), and DagRuns
      completing WELL under dagrun_timeout=45min (no wedge);
      (3) PRIMARY -- knock-on collapse: discovery-window misses and AlreadyRunningBackfill
      counts must drop vs ROUND 10's 7/3 as the max_active_runs slot frees; record the
      MEASURED floors (per-file drain, discovery latency, backfill window occupancy) for
      the deferred measured-floor budget assessment;
      (4) REGRESSION GUARDS: stage successes persist (try=1, ~30s), FailedScheduling 0,
      Kyverno DENY 0, scheduler/dag-processor/triggerer restarts 0, scheduler peak below
      2048Mi;
      (5) SECONDARY -- pytest failure-template census + node-ID diff vs the invariant
      17-set (12 consecutive runs): the saturated signature should START clearing; note
      exactly which node-IDs go green (expected direct beneficiaries:
      test_smoke_dag_xcom_contains_built_sha, test_no_extra_schemas_exist, the dbt/
      data-state asserts, plus whatever the unwedged slot drains in time); per the
      whack-a-mole precedent, NEW residuals may surface beneath (publish and downstream
      hops execute on CI for the first time) -- name them with direct evidence, return a
      decision checkpoint before any ROUND 12 fix."
  ROUND_11_OUTCOME (post-run analysis on run 32884691063, conclusion CANCELLED at the
      job-level `timeout-minutes: 120` after 2h00m42s -- orchestrator triage confirmed no
      concurrency supersession, no manual cancel; publish.yml 32884691018 success =
      criterion 0 met; partial job log 5577 lines fetched incl. ALL always()-diagnostics):
    criterion_a: "MET -- dbt_build state=success try=1 in ALL 7 DagRuns that reached it
        (backfills 10:24/10:25/12:55/12:56, scheduled 18:43/19:28/20:13; 20-30s wall each).
        FIRST-EVER dbt_build successes on CI."
    criterion_b: "MET -- zero 'null value in column dataset_id' and zero 'permission denied
        for table datasets' anywhere in the log (grep count 0 across all diagnostics)."
    criterion_c: "NOT MET -- but for a NEW mechanism, not (15): backfill__10:24 is the
        FIRST-EVER complete end-to-end CI DagRun (18:44:24->18:50:52 = 6m28s; publish
        success try=1, 29s). EVERY subsequent DagRun wedged at exactly start+45:00
        (dagrun_timeout): scheduled 18:43, backfill 10:25, scheduled 19:28, backfill
        12:55 all failed at +45:00; 12:56 (publish try=4 up_for_retry) + scheduled 20:13
        (publish try=5) in flight at cancel; 12:57 queued. Wedge cause = residual (16)."
    criterion_d: "MET -- stage 60/60 state=success try=1 (19-26s first runs, 8-12s warm);
        FailedScheduling census EMPTY; Kyverno DENY 0; control-plane restarts 0
        (restart-timeline empty; peaks: scheduler 1288MiB/21pids < 2048Mi, dag-processor
        699MiB, triggerer 422MiB)."
    criterion_e: "UNMEASURABLE for pytest node-IDs -- the cancelled step's streamed stdout
        was NOT archived (only the invocation line survived before ##[error] The operation
        was canceled); no PASS/FAIL lines exist. Cluster-side evidence places the suite
        still inside tests/e2e/slice (sweep module backfill_id=2 window in flight) at
        113m26s of pytest wall."
    new_residual_16: "Publish poison: dataplat publish fails QualityThresholdExceeded,
        rule_id=mass_delete_circuit_breaker, 'vanished-key ratio 54.00% exceeds configured
        mass-delete threshold 10.00%', current_count=50 vanished_count=27 -- BYTE-IDENTICAL
        across all observed tries (12:56 tries 1-4 directly in scheduler log; earlier
        wedged runs' publish tries rotated out of the captured window but share the try=6
        end-state 'skipped'). raw/customers/ listing at cancel: EXACTLY the 12 corpus
        files, all uploaded 18:43:54, NO separate mass-delete snapshot ever uploaded ->
        the 54% arises from re-publishing corpus snapshots against evolved gold state,
        not from the mass-delete test's own fixture. current_count=50 == _ROWS_PER_DAY
        (each file is a full 50-row daily snapshot; gold current after run 1 = one day's
        50-key roster). Leading hypothesis (parts INFERRED, needs ROUND 12 evidence):
        publish applies delete-detection per staged snapshot against currently-current
        gold; a later run staging an OLDER/different day-window fails to re-confirm 27 of
        gold's 50 current keys (cumulative roster churn over the re-published window >10%)
        -> trip; and because tripped runs never publish, their files are never marked
        ingested -> re-discovered by every subsequent run -> SELF-SUSTAINING poison loop.
        Evidence gap: WHICH files each run staged is not in any diagnostic (no
        assignment_uri dump); needs a bronze _run_id->file mapping dump or local repro.
        Open sub-questions: (i) why publish's final try ends state=skipped try=6 (no
        skip_on_exit_code wiring found in _common/kpo.py or the DAG); (ii) why wedged
        DagRuns still sat until +45:00 after publish resolved (10:25 publish skipped
        19:22:08, run failed 19:35:53 -- 13.7min later, something downstream never
        resolved)."
    timeline_120min: "job start 18:36:02; cluster-up 18:36:10->18:42:21 (6m11s); images/
        migrate/vault 37s; pytest 18:42:58->cancel 20:36:24 (113m26s). Inside pytest:
        cluster tests + 12-file corpus upload done by 18:43:54 (<1min); one healthy DagRun
        6m28s; then 4 x 45min wedges (overlapping scheduled/backfill streams, serialized
        per-stream by max_active_runs=1) consumed the rest. Workflow sets
        timeout-minutes: 120 (.github/workflows/e2e-full.yml line 41)."
    measured_floors: "wait_for_files 5-28s; discover 9-19s try=1; stage per file 19-26s
        cold / 8-12s warm; dbt_build 20-30s try=1; publish 29s (success). Healthy 10-file
        DagRun: 6m28s cold, ~2.5-3min warm (est. from warm stage cadence). Publish retry
        cadence (capped delays): tries at +0/+1m/+2.5m/+5.9m. KEY ARITHMETIC: the poison
        never clears, so raising timeout-minutes ALONE cannot go green; conversely, with
        (16) fixed, ~5 sweep DagRuns x 3-6.5min + scheduled stream suggests the suite
        plausibly fits the existing 120-min budget -- measure again post-fix before
        touching timeout-minutes."
  next_action: "DECISION CHECKPOINT returned to user: choose ROUND 12 direction for
      residual (16) (investigate re-staging/re-publish ordering + idempotency-ledger
      interaction; add per-run file-assignment diagnostics; local repro) vs corpus/test
      redesign vs accepting breaker behavior and isolating the sweep. No fix without a
      decision per the standing rule."

ROUND 10 (2026-08-25, opened on user decision: HYBRID charter -- SUPERSEDED BY ROUND 11 ABOVE):
  charter: "User-chosen Hybrid, amendment verbatim: 'reduce number of processed files if they
      are an issue and their size.' Priority order: (1) throughput-ceiling analysis FIRST --
      empirically quantify from round9-job.log whether a file can drain within 180s given
      max_active_tis_per_dag=1 global slots on stage/dbt_build/publish + measured CI
      task-start latencies; quantify the per-file floor and where each 180s window goes; also
      quantify corpus load (file counts/sizes actually processed, incl. sweep_corpus's 19-file
      corpus with ROUND 7's batching cap 10). (2) PREFERRED LEVER: shrink the CI test corpus
      (fewer/smaller synthetic files) if the analysis implicates count/size -- preserve
      coverage intent (each messy-CSV feature exercised >=1x), not raw volume; test-fixture
      change, not production code. (3) Concurrency-slot design change (option C proper) only
      where single-slot serialization remains binding even with a reduced corpus -- PROPOSE
      via decision checkpoint before implementing if production-code design change.
      (4) Budget adjustments only surgically: per-test, to a measured floor, justified in this
      file -- no across-the-board loosening. AlreadyRunningBackfill triaged as part of the
      drain-time problem (halved 6->3 in round 9). Standing rules: rounds 1-9 fixes stay;
      follow-up B non-blocking; offline gates before push; one watcher at 60s; judged on
      internal diagnostics + failure-template census (node-ID set is a saturated instrument)."
  hypothesis: "The residual 17-test signature is now dominated by a throughput ceiling: with
      admission (10), connection gap (12) and OOM bursts (13) verifiably gone, per-file
      end-to-end latency on CI (~3 CPU, KPO pod-start ~10-20s x 4-6 serialized tasks, plus
      stage/dbt_build/publish each holding ONE global max_active_tis_per_dag slot across ALL
      concurrent DagRuns) exceeds the 180s discovery/terminal-state windows whenever more than
      ~N files/DagRuns are in flight; the aggregate corpus volume (sweep_corpus 19 files +
      per-test single files + 1-min cron runs) keeps the queue permanently deeper than the
      windows allow. Falsifiable: the log's own timestamps must show per-file drain time and
      queue depth; if a single file drains well under 180s even at observed queue depth, the
      hypothesis is wrong and the residual is elsewhere (e.g. downstream data-state asserts)."
  test: "Mine round9-job.log (surviving scratchpad artifact, 5470 lines) for: TI state
      transitions per task (start->end wall time), task-concurrency-reached message census,
      DagRun creation->terminal latency, backfill overrun timing vs AlreadyRunningBackfill
      collisions, and the actual file count/size the suite pushes through. Cross-read test
      fixtures for corpus size/shape. Compute the per-file floor arithmetic."
  expecting: "Either (a) per-file floor > 180s even at queue depth 1 (=> surgical budget
      adjustment territory + corpus reduction lowers depth), or (b) floor < 180s but queue
      depth x floor > 180s (=> corpus reduction is the direct lever, matching the user's
      preference), or (c) slot serialization dominates regardless of corpus (=> option C
      design proposal checkpoint)."
  analysis_result: "COMPLETE -- outcome is a FOURTH branch none of the pre-registered three
      anticipated: the per-file floor on CI is INFINITE. NEW ROOT CAUSE (14): _STAGE_RESOURCES
      requests cpu=500m (stage AND publish pods) against a CI node with only ~220m free
      (3000m allocatable - 2780m steady-state platform requests, 92%); the pods can NEVER be
      scheduled; KPO default startup_timeout_seconds=120 converts every attempt into a
      deterministic ~129s failure (every recorded stage attempt: 128-130s, try counts to 6,
      up_for_retry observed, ZERO stage successes EVER on CI); the pilot file's
      PENDING-at-1800s DB state proves no stage container ever started. All residual failure
      templates (7x discovery-window, 3x AlreadyRunningBackfill, pilot PENDING, DBT never
      RUNNING, <2 rows, SCD2 preconditions, XCom git_sha) reduce to (14). Corpus SIZE
      irrelevant (19 x ~3.3KiB); corpus COUNT (10-file batch depth through the single global
      slot) becomes load-bearing only AFTER (14) is fixed. Option C proper (slot redesign)
      assessed AGAINST: slot=1 is matched to CI capacity even post-fix. Full numbers in the
      two ROUND 10 Evidence entries."
  user_decision: "A+B COMBINATION chosen (2026-08-25): B's leg for the DAG side -- per-profile
      stage/publish CPU REQUEST via an Airflow Variable following the csv_processor_image
      precedent, ci=200m / local=500m verbatim (limits untouched at 2CPU/4Gi in both); A's leg
      for the platform side -- trim ~200m of idle CI platform requests (analytics-db 200->100m,
      minio 100->50m, vault 100->50m), CI values only, local untouched. All bundled items
      approved: corpus shrink 19->12 preserving all six anomaly features, FailedScheduling/
      etl-namespace event capture, no slot redesign, backfill treated as coupled knock-on, no
      pre-committed budget raises. FALLBACK RULE (user's own words): if the combination proves
      impractical during implementation -- e.g. the Variable machinery turns out fragile,
      untestable offline, or creates an unacceptable divergence axis -- drop to plain A (200m
      request in both DAGs everywhere + the same platform trims) rather than stalling; note in
      this file which path was taken and why."
  fix_path_taken: "A+B COMBINATION (not the fallback). The Variable leg proved neither fragile
      nor untestable offline: Variable.get(key, default=...) resolves through the same layered
      secrets chain csv_processor_image already exercises at DAG-parse time (AIRFLOW_VAR_* env
      backend first), and a default of '500m' means LOCAL needs zero provisioning -- local
      behavior is byte-identical to today's hardcoded value without any new local bootstrap
      step, while CI sets stage_cpu_request=200m in scripts/ci-set-workload-images.sh (the
      exact same post-cluster-up `airflow variables set` site the csv_processor_image
      precedent uses, covering all three e2e workflows with one change). Divergence-axis
      check: D-06's permitted axes are replica counts / resource sizing / monitoring -- a
      per-profile CPU request is squarely the resource-sizing axis, same as every helm-values
      CI sizing divergence already in force."
  reasoning_checkpoint:
    hypothesis: "(14) stage/publish pods request cpu=500m against ~220m free on the 3000m-
        allocatable CI node (2780m steady-state platform requests), so kube-scheduler can
        never place them; KPO startup_timeout_seconds=120 converts every attempt into a
        deterministic ~129s failure; every residual failure template reduces to this."
    confirming_evidence:
      - "Arithmetic: 3000m allocatable - 2780m platform requests = ~220m free < 500m request;
        memory a non-issue (26% requested)."
      - "Run 32855002333 TI dump: EVERY recorded stage attempt lasted 128-130s (six distinct
        attempts measured), try counts to 6, up_for_retry observed, ZERO stage successes ever
        on CI -- while discover (100m request) succeeded try=1 in 11-23s in all 4 DagRuns."
      - "Pilot file meta.ingestion_runs status=PENDING at 1800s: a stage container that ever
        STARTED would have advanced it -- direct DB-state proof no stage container has ever
        run on CI."
      - "kpo.py's own on_finish_action comment (2026-08-16) already documents this exact
        startup-timeout failure mode as routine under tight node CPU."
    falsification_test: "Pre-registered on the next live run's INTERNAL diagnostics (node-ID
        set is a saturated instrument, secondary only): (a) FIRST-EVER stage success on CI
        (TI dump: stage state=success with try>=1 and a start/end under ~60s) -- if stage
        STILL fails every attempt at ~129s with the 200m request verifiably in force and
        >=400m free, root cause (14) is WRONG; (b) the NEW etl-namespace event census must
        show FailedScheduling events BEFORE this fix's first successful schedule (or none at
        all post-fix) -- present-with-Insufficient-cpu confirms (14)'s mechanism, absent-
        while-stage-still-fails refutes it; (c) Kyverno DENY count stays 0 (fix 11
        regression); (d) scheduler/dag-processor/triggerer restarts stay 0 (fixes 1-3/13
        regression); (e) failure-template census + node-ID diff as secondary -- any shrink is
        signal, unchanged-with-clean-internals means a further stacked cause remains."
    fix_rationale: "Addresses the root cause (request > free capacity) on both sides of the
        inequality: the request side drops to 200m (a REQUEST is a scheduling reservation,
        not a cap -- the untouched 2-CPU LIMIT still lets stage burst identically once
        placed, so local runtime behavior at 500m request is preserved verbatim via the
        Variable default), and the free-capacity side rises ~220m->~420m by trimming idle CI
        platform requests (analytics-db/minio/vault run near-idle in CI at steady state).
        200m + discover's 100m co-schedule inside ~420m with margin. NOT symptom-level: no
        timeout was loosened, no retry added, no test budget raised. The corpus shrink
        (19->12) is the user's preferred post-fix drain lever -- load-bearing only AFTER
        schedulability is fixed, per the ROUND 10 arithmetic."
    blind_spots: "(1) The 2780m platform-request census came from one run's node describe;
        if another platform pod's requests grew since, free capacity could still be <200m --
        the etl-namespace FailedScheduling census will show it directly. (2) A 200m-request
        stage pod on a contended node gets less guaranteed CPU: stage wall time may stretch
        beyond the ~15-40s post-fix floor estimate; the 180s windows may still miss at 12-file
        depth -- judged by the census, budget raises only per-test to a measured floor if so.
        (3) publish shares _STAGE_RESOURCES: a publish-specific failure mode beyond
        scheduling would surface only post-fix. (4) AlreadyRunningBackfill is treated as a
        coupled knock-on -- if it persists after stage drains, it is a NEW mechanism to
        triage. (5) The Variable read happens at DAG-parse time: a parse cycle BEFORE
        ci-set-workload-images.sh runs uses the 500m default -- same already-proven
        eventual-consistency shape as csv_processor_image (dag-processor re-parses
        continuously; the suite starts minutes after cluster-up)."
  live_verification_state: "RECORDED 2026-08-25T16:42Z: fix (14) pushed as commit d0d1ad6
      (base 55a740a). AUTHORITATIVE ROUND 10 live-verification run: e2e-full.yml run
      32873456327 (headSha d0d1ad6, created 2026-08-25T16:41:24Z, in_progress at recording
      time). Companion runs, same headSha: publish.yml 32873456458 (must complete
      build+sign BEFORE the e2e cluster pulls -- the recurring image-race blind spot;
      verify its conclusion as POST-RUN CHECK ITEM 0), CI 32873456453, e2e-chaos
      32873456426 (both out of scope for this signature). The docs push recording this
      state uses [skip ci] per the ROUND 7 lesson -- no supersession risk.
      POST-RUN ANALYSIS STEPS (for the continuation agent, once the single 60s-interval
      watcher reports terminal), judged on INTERNAL diagnostics per the ROUND 10
      pre-registered falsification test in reasoning_checkpoint above (node-ID diff
      SECONDARY, saturated instrument):
      (0) publish.yml 32873456458 conclusion must be success (image race check);
      (1) FIX-IN-FORCE probes: job log must show ci-set-workload-images output
      'registering stage_cpu_request=200m' AND the dump step's 'effective
      stage_cpu_request Airflow Variable' section must print 200m; node describe should
      show platform requests ~200m lower than ROUND 9's 2780m census;
      (2) PRIMARY -- first-ever stage success on CI: TI dump must show stage
      state=success (try>=1) with a start->end wall time well under ~60s; if stage STILL
      fails every attempt at ~129s with (1) verified, root cause (14) is WRONG;
      (3) PRIMARY -- FailedScheduling census (NEW etl-monitor.log): Insufficient-cpu
      FailedScheduling events BEFORE the first success (or none at all) CONFIRM (14)'s
      mechanism; a census showing stage pods scheduled-but-failing refutes the
      request-sizing attribution and names the real blocker directly;
      (4) Kyverno DENY grep: must REMAIN 0 (fix 11 regression check);
      (5) scheduler/dag-processor/triggerer restarts: must REMAIN 0 (fixes 1-3/13
      regression check; cp-monitor CSV + kubectl describe);
      (6) SECONDARY: pytest failure-template census + node-ID diff vs the invariant
      17-test baseline -- any shrink is signal; per blind_spots (2), if stage succeeds but
      180s windows still miss at 12-file depth, that is the surgical per-test-budget
      branch, NOT a refutation of (14)."
  outcome (POST-RUN ANALYSIS, run 32873456327, job 97885720167, log 5891 lines archived at
      scratchpad round10-job.log): "ROOT CAUSE (14) LIVE-CONFIRMED; fix (14) VERIFIED at
      mechanism level. Pre-registered item-by-item:
      (0) PASS -- companion publish.yml 32873456458 conclusion=success (no image race).
      (1) PASS -- fix in force: log line 979 'registering stage_cpu_request=200m', 987
      'Variable stage_cpu_request created', dump prints effective value '200m' (line 5381);
      node describe Allocated cpu Requests 2580m (86%) vs ROUND 9's 2780m census -- the full
      ~200m platform trim landed (free ~420m).
      (2) PASS (PRIMARY) -- FIRST-EVER stage successes on CI: ALL 20 mapped stage TIs across
      both backfill DagRuns state=success try=1, wall ~30-32s each (16:51:32->16:56:47 and
      17:36:34->17:41:56 drain the full 10-file batches back-to-back); publish for orders and
      discover unchanged-healthy (discover try=1 in 17-22s). Pre-fix: ZERO stage successes
      ever, every attempt 128-130s.
      (3) PASS (PRIMARY) -- FailedScheduling census: ZERO FailedScheduling/Insufficient lines
      captured in etl-monitor.log across the whole run; etl events show immediate
      Scheduled->Pulled->Started for every stage/dbt-build pod. Matches the
      'none-at-all-post-fix' confirming branch: pods schedule instantly once request <= free.
      (4) PASS -- Kyverno DENY 0; every kyverno.io/image-verification-outcomes annotation
      status=pass.
      (5) PASS -- restart-count timeline EMPTY; kubectl describe Restart Count 0 on
      scheduler/dag-processor (+triggerer); scheduler peak 1644MiB/23pids -- above the old
      1536Mi ceiling, below the 2048Mi limit, fix (13) load-bearing again.
      (6) SECONDARY -- pytest '17 failed, 21 passed, 6 skipped in 3746.00s'; node-ID diff vs
      baseline_sorted.txt: IDENTICAL set, 12th consecutive (saturated instrument, as
      pre-registered: blind_spot-2 branch, NOT a refutation of (14)). CENSUS SHIFTED
      DECISIVELY: pilot file customers_20240101.csv now 'still non-terminal: STAGED' (was
      PENDING all session -- stage hop verifiably executes, pilot now blocked at the dbt
      hop); dbt-build pods now SCHEDULE, START and RUN dbt to completion of its own error
      handling -- 6 dbt-build pods Failed with REAL dbt output: 'Completed with 2 errors':
      (15a) 'Database Error in model silver_orders ... null value in column dataset_id of
      relation dedup_audit violates not-null constraint, DETAIL: Failing row contains
      (37, null, 79de837c-..., orders, null, null, 0, 0, 0, 0, ...)' -- the dedup_audit
      insert resolves dataset_id NULL for dataset 'orders' on a fresh CI cluster;
      (15b) 'Database Error in test reconciliation_customers ... permission denied for table
      datasets' -- the compiled test SQL reads meta.datasets directly, which the
      least-privilege dbt_app role (migrations 0021/0028) cannot SELECT. dbt_build fails
      deterministically every try (~30s wall, tries to 6, then skipped when dagrun_timeout
      kills the run), holding the first scheduled DagRun + backfill run to the full 45min
      dagrun_timeout (16:49:34->17:34:34) -- max_active_runs=1 pins the scheduled slot for
      45min, so the 7 'discovery never registered within 180s' templates, the 3
      AlreadyRunningBackfill backfill-create failures, DBT_BUILD-never-RUNNING (300s),
      normalized.customers <2 rows (publish still never ran -- downstream of dbt_build) and
      both SCD2 precondition templates are ALL knock-ons of the dbt_build wedge. TWO
      independent test-side artifacts also nameable now: (i) test_smoke_dag_xcom_contains_
      built_sha compares the XCom's FULL 40-char sha ('d0d1ad6be1...') against `git rev-parse
      --short HEAD` ('d0d1ad6') -- the image IS built from the checked-out commit this run
      (same full-vs-short shape in ROUND 9 with ee87708...), a comparison/build-format
      artifact, not an image race; (ii) test_no_extra_schemas_exist flags schema 'meta' as
      unexpected -- identical in ROUND 9, likely a stale test allowlist vs the migrations
      that legitimately create meta.* (needs 1 confirmation read)."
  next_action: "DECISION CHECKPOINT returned to user: residual cause (15) (dbt-side: 15a
      dataset_id NULL in dedup_audit for orders + 15b dbt_app permission denied on
      meta.datasets) named with direct evidence; awaiting user direction before any ROUND 11
      fix work, per the standing no-new-round-without-checkpoint rule."

ROUND 9 OUTCOME (2026-08-25, post-run analysis of run 32855002333 -- SUPERSEDED BY ROUND 10 ABOVE):
  status: "ANALYSIS COMPLETE. Decision-tree branch: mechanisms (12)/(13) CONFIRMED FIXED and
      the remaining failures have nameable causes. Awaiting user decision checkpoint on the
      next direction (residual = deferred option C territory + AlreadyRunningBackfill
      collisions + downstream data-state asserts)."
  run_analyzed: "e2e-full.yml run 32855002333 (headSha ee87708, fixes 12+13), job 97824707600
      'Full local E2E suite + rebuild-from-raw capstone', conclusion failure. Companion
      publish.yml 32855002320 SUCCESS (image race clear). NOTE: the original analysis agent
      completed all log analysis then died to an API interruption before writing this file;
      this block was reconstructed by a resumed agent from the surviving scratchpad artifacts
      (round9-job.log, round9_failed.txt, baseline_sorted.txt) with every criterion re-run
      fresh against the raw log."
  criterion_results:
    - "(1) fix (12) in force: VERIFIED -- job log 13:50:28Z 'secret
      airflow/connections/analytics_db_default: created' (fresh CI Vault, 'created' as
      required)."
    - "(2) WEDGE CHECK: PASSED -- ZERO occurrences of dbt_build+upstream_failed anywhere in
      the log (rounds 5-8: every single DagRun had the t+~30s stamp). Root cause (12)
      attribution CONFIRMED live. Corroboration that the dbt chain now actually starts:
      a failing test's own assert references meta.run_stages[run_id=58,
      stage_name='DBT_BUILD'] -- that row EXISTING at all is a first for CI."
    - "(3) OOM CHECK: PASSED, fix (13) LOAD-BEARING -- scheduler restarts 0 over the full
      66min run (was 9); zero OOMKilled/137 anywhere. Decisive: per-role peak census shows
      scheduler peak_mem_bytes=1635192832 (~1559MiB) at peak_pids=24 -- ABOVE the old 1536Mi
      (1610612736) ceiling and BELOW 2048Mi. The parallelism=8 dispatch burst still happened
      exactly as modeled and would have OOMed the old limit; per the pre-registered protocol
      this is the 'spike above old ceiling, absorbed by new limit' branch, NOT the
      'unnecessary-but-harmless' branch."
    - "(4) Kyverno DENY grep: 0 -- fix (11) regression check passed."
    - "(5) publish behavior: not directly extracted (this run's log lacks a greppable TI-dump
      in the format prior rounds matched; honest gap) -- but zero premature-publish retry
      storms appear in the failure templates, and with (12)'s upstream chain no longer
      short-circuiting, the structural premature-launch path is closed by construction."
    - "(6) pytest: '17 failed, 21 passed, 6 skipped, 16 warnings in 3766.04s (1:02:46)'.
      Node-ID set IDENTICAL to baseline, 11th consecutive -- exactly as the saturated-
      instrument pre-registration anticipated. BUT the failure-TEMPLATE census shifted
      materially for the FIRST time all session: 7x 'meta.files has no row ... within 180s --
      discovery never registered it' (persisting); 3x 'airflow backfill create ... failed
      after 3 attempts (exit 1)' with the terminal exception now cleanly visible --
      airflow.models.backfill.AlreadyRunningBackfill: 'Another backfill is running for Dag
      csv_ingest_customers. There can be only one running backfill per Dag.' (12 occurrences
      in BOTH round-8 and round-9 logs -- persisting collision, backfills overrun their test
      windows and serialize; down from 6 such test failures in ROUND 8); 1x pilot file NOW
      REGISTERS in meta.ingestion_runs but stays status=PENDING at 1800s (progress: baseline
      never registered); 1x 'unexpected schema(s): [meta]' on the analytical cluster; 1x
      meta.run_stages DBT_BUILD never reached RUNNING; 2x SCD2 precondition asserts; 1x XCom
      git_sha '' mismatch; 1x 'normalized.customers has fewer than 2 rows'."
  interpretation: "The stacked-sufficient-causes model keeps paying out: with admission (10),
      the connection gap (12) and the OOM burst ceiling (13) all verifiably gone, the
      pipeline now RUNS on CI (dbt chain starts, files register) but cannot DRAIN fast
      enough for the per-test windows: a pilot file registered-but-PENDING at 1800s, 7 files
      missing the 180s discovery window, and backfills overrunning into
      AlreadyRunningBackfill serialization. That is the deferred option C throughput ceiling
      (global max_active_tis_per_dag=1 slots on stage/dbt_build/publish + ~3 allocatable
      CPUs) as the dominant residual, plus downstream data-state asserts (schema hygiene,
      SCD2 preconditions, XCom git_sha) that may simply be knock-ons of never having had a
      complete prior ingest -- untriageable until a run drains."
  next_action: "USER DECISION CHECKPOINT: option C (throughput-ceiling design analysis --
      re-examine max_active_tis_per_dag=1 global slots and the DAG concurrency shape, a
      production-code change with design implications, explicitly deferred until runs stop
      wedging -- they now have) vs. triaging the AlreadyRunningBackfill test-collision leg
      vs. ROUND 8's reserved directions (loosen timeouts / split jobs, local evaluation).
      Scope guardrails: rounds 1-9 fixes stay; follow-up B still captured, non-blocking."

ROUND 9 (2026-08-25, opened on user decision A+B combined -- fix round, SUPERSEDED BY ROUND 9 OUTCOME ABOVE):
  charter: "What happens inside the scheduler in its first 60 seconds under real DagRun load.
      Treat the near-simultaneous first scheduler OOM (12:08:06) and the wrongful
      upstream_failed stamps on dbt_build (12:07:51/59, seconds after wait_for_files succeeded,
      with discover at try=0/start=None) as ONE question. Both the 47-min wedge and the ~6min
      OOM cadence hang off that first-minute window. Option C (throughput-ceiling design
      analysis, max_active_tis_per_dag) explicitly DEFERRED until runs stop wedging."
  hypothesis: "PRELIMINARY (to be refined from direct log+source evidence): in the first
      scheduler crash window, the scheduler dies (OOM) mid-scheduling-loop and/or the restart
      recovery path (adopt_or_reset_orphaned_tasks / trigger-rule dep evaluation over
      partially-committed TI state) evaluates dbt_build's trigger rule against an upstream TI
      state that never legitimately existed, stamping dbt_build upstream_failed while discover
      is still try=0/start=None -- wedging the DagRun until dagrun_timeout (12:54:44). Two
      named suspects: (a) OOM mid-loop leaving partially-committed TI state that a fresh loop
      then misreads; (b) the orphan/stuck-in-queued reset path misclassifying."
  test: "Mine round8-job.log 12:07:00-12:09:00 for the two wedged DagRuns: exact scheduler
      lines for the upstream_failed stamps, executor events, first OOM kill boundary, adoption/
      orphan messages after restart; then source-read installed apache-airflow==3.3.0 for the
      exact code path that can write upstream_failed on dbt_build given the observed upstream
      states. Direct evidence over theory."
  expecting: "The log shows WHICH component wrote the upstream_failed stamps and what upstream
      state it saw; source shows the only code path(s) that produce that write, narrowing to a
      falsifiable mechanism with a targeted fix within scope guardrails."
  falsification_test_pre_registered: "Because the pytest 17-test node-ID signature is a
      SATURATED instrument (any >180s delay reproduces it identically), ROUND 9 success/failure
      is defined on INTERNAL diagnostics of the next live run: (1) scheduler restart count
      (ROUND 8: 9) -- must drop materially; (2) wedge presence -- ZERO wrongful upstream_failed
      stamps on dbt_build while discover try=0 (ROUND 8: 2 DagRuns wedged 47min); (3) Kyverno
      DENY count must REMAIN 0 (fix 11 regression check); (4) node-ID diff vs the invariant
      17-test baseline as a secondary check only. If the wedge recurs with the fix verifiably
      in force, the ROUND 9 mechanism attribution is wrong."
  scope_guardrails: "No timeout loosening, no job splitting, no runner migration, no reverting
      rounds 1-8 fixes. Fix (11) stays; follow-up B (sidecar mirror) captured, non-blocking.
      Option C deferred (noted as such per user decision)."
  investigation_result: "BOTH ROUND 8 suspects for the upstream_failed leg REFUTED by direct
      evidence (stamps recur identically under a healthy scheduler in the replacement runs);
      actual mechanisms found and confirmed -- see the four ROUND 9 Evidence entries. Two
      distinct root causes: (12) analytics_db_default never provisioned in code (ad-hoc local
      Vault fix never landed in scripts/vault-bootstrap.py; ephemeral CI Vault always missing
      it -> list_run_ids_pending_dbt_build fails ~30s into EVERY DagRun -> dbt_build
      upstream_failed cascade + premature failing publish churn, = deferred-items.md plan-11-09
      'defect 1' firing 100% deterministically on CI); (13) scheduler OOM is burst-concurrency
      (~360MiB baseline + 8 simultaneous task processes x ~145MiB > 1536Mi limit), spikes
      abrupt (one 17s sample), no leak, crash-loop self-sustaining via re-synchronized dispatch
      waves killing the wedged runs' own tasks ((3c) resonance), ends exactly when the wedged
      runs die at dagrun_timeout."
  reasoning_checkpoint:
    hypothesis: "(12) On CI the Airflow Connection analytics_db_default does not exist (only
        minio_default is provisioned by scripts/vault-bootstrap.py; the local repair was ad-hoc
        infra-only), so list_run_ids_pending_dbt_build (root task, retries=0) fails ~30s into
        every DagRun, cascading upstream_failed to mark_dbt_build_running/dbt_build and
        launching publish prematurely into guaranteed 2-min failing pods. (13) Independently,
        a dispatch burst that fills all 8 parallelism slots simultaneously spikes the scheduler
        cgroup ~1.5-1.7GiB transient (+7-8 task processes x ~145MiB over the ~360MiB baseline),
        exceeding the 1536Mi CI limit -> OOMKilled -> orphan-reset resync -> repeat, wedging
        the first two DagRuns until dagrun_timeout."
    confirming_evidence:
      - "(12) dbt_build upstream_failed try=0 ~30-80s after run start in 14+ DagRuns across 4
        instrumented runs INCLUDING healthy-scheduler replacement runs; prior session's direct
        root-token Vault listing (minio_default only) + 22/22 failure history + repo-wide grep
        zero provisioning sites; vault-bootstrap.py read directly (only minio_default under
        airflow/connections); e2e-full.yml runs make vault-bootstrap on ephemeral cluster-up."
      - "(13) cgroup series: stable 360MiB/17pids baseline, abrupt 1331-1533MiB/24-25pid spikes
        one sample before each of 8 OOMs, cadence ~6min, crash-loop stops permanently at
        12:54:44 when the wedged runs die; post-wedge phase peaks 1496MiB with pids<=22 and
        zero OOMs."
    falsification_test: "Pre-registered above (falsification_test_pre_registered): next live CI
        run with both fixes verifiably in force must show (1) scheduler restarts materially
        below 9 (target 0-1), (2) ZERO dbt_build-upstream_failed-with-discover-try=0 wedge
        stamps (dbt_build must reach a real state: running/success/failed-with-try>=1), (3)
        Kyverno DENY still 0, (4) node-ID diff secondary. If dbt_build still goes
        upstream_failed at t+30s with the connection verifiably provisioned, (12) is wrong; if
        OOMs persist at the same burst signature under a 2048Mi limit, (13)'s sizing model is
        wrong."
    fix_rationale: "(12) provisions the missing Connection in the SAME bootstrap code CI
        rebuilds Vault from, reusing the already-existing etl_app DSN (etl/analytics-db#dsn)
        exactly as the prior session's hand-fix did -- root cause (provisioning gap), not
        symptom (not retries/trigger-rule surgery; the wiring defect remains tracked as
        deferred-items defect 1). (13) sizes the CI scheduler memory LIMIT to the measured
        worst-case burst at the ROUND 7-chosen parallelism=8 (2048Mi > 360 + 8x150 + margin);
        request untouched -> zero CI request-budget cost; same CI-only limit-raise shape as
        ROUND 2's precedent, but now grounded in a direct per-process burst model instead of a
        trend extrapolation."
    blind_spots: "(1) The exact ~6min resync period is attributed (retry backoff + post-restart
        backlog) but not proven line-by-line; the fix does not depend on it. (2) publish may
        have an ADDITIONAL failure mode beyond running-prematurely-with-nothing-staged --
        post-fix runs will show it (publish try counts/states are in the diagnostics). (3)
        max_active_tis_per_dag=1 throughput ceiling (option C, deferred) still bounds
        end-to-end latency; some of the 17 tests may still time out at 180s even with zero
        OOMs and a working dbt chain -- expected residual, judged on internal diagnostics.
        (4) The Task-SDK worker path resolves connections via the supervisor API, not directly;
        the offline get_uri round-trip plus the hand-fixed local cluster's live success are the
        evidence it works -- CI run is the final proof."
  live_verification_state: "RECORDED 2026-08-25T13:43Z: fixes (12)+(13) pushed as commit
      ee87708 (base c72c4d0). AUTHORITATIVE ROUND 9 live-verification run: e2e-full.yml run
      32855002333 (headSha ee87708, created 2026-08-25T13:42:54Z, in_progress at recording
      time). Companion runs, same headSha: publish.yml 32855002320 (must complete
      build+sign BEFORE the e2e cluster pulls -- the recurring image-race blind spot; verify
      its conclusion in post-run analysis), CI 32855002326, e2e-chaos 32855002282 (both out
      of scope for this signature). This docs push uses [skip ci] per the ROUND 7 lesson --
      no supersession risk.
      POST-RUN ANALYSIS STEPS (for the continuation agent, once the single 60s-interval
      watcher reports terminal), judged on INTERNAL diagnostics per the pre-registered
      falsification test, node-ID diff SECONDARY:
      (1) verify fix (12) in force: grep the job log for vault-bootstrap output 'secret
      airflow/connections/analytics_db_default: created' (the bootstrap prints it; CI Vault
      is fresh so it must be 'created', never 'already present');
      (2) WEDGE CHECK (primary): in the ROUND 5 TI-history dump, dbt_build must NOT be
      upstream_failed-with-try=0 in any DagRun; it must reach a real state (running/success/
      failed-or-retry with try>=1). If ANY DagRun still shows the t+~30s upstream_failed
      stamp with the connection verifiably provisioned, root cause (12) attribution is WRONG;
      (3) OOM CHECK (primary): scheduler restart count from the cgroup CSV / kubectl describe
      -- target 0-1 (was 9). If OOMs persist with the same abrupt-burst signature (spike to
      >1900MiB against 2048Mi with pids ~24-25), fix (13)'s sizing model is wrong; if spikes
      cap BELOW the old 1536Mi ceiling, (13) was unnecessary-but-harmless and (12) removed
      the churn -- record which;
      (4) Kyverno DENY grep: must REMAIN 0 (fix 11 regression check);
      (5) publish behavior: with (12) in force publish must no longer launch before stage --
      check publish try-counts/timing vs stage in the TI dump; if publish STILL fails with
      try>=2 AFTER stage completes, that is blind-spot (2)'s additional failure mode -- a
      NEW, distinct mechanism to triage;
      (6) pytest summary + node-ID diff vs the invariant 17-test baseline (SECONDARY,
      saturated instrument): any shrink is signal; unchanged-set-with-clean-internals means
      residual is the deferred option C throughput ceiling."
  next_action: "(watcher handoff) Session manager runs the single 60s-interval watcher on run
      32855002333; continuation agent executes the post-run analysis steps above."

ROUND 8 OUTCOME (2026-08-25, post-run analysis of run 32845181597 -- SUPERSEDED BY ROUND 9 ABOVE):
  status: "ANALYSIS COMPLETE. Decision-tree branch: signature unchanged despite exemption
      verifiably applied -> hypothesis (10)-as-signature-cause INSUFFICIENT. Awaiting user
      decision checkpoint on ROUND 9 direction."
  run_analyzed: "e2e-full.yml run 32845181597 (headSha ce73d9df, fix commit ce73d9d), job
      97793152902 'Full local E2E suite + rebuild-from-raw capstone', conclusion failure.
      Log: 5434 lines via gh api .../jobs/97793152902/logs."
  exemption_in_force: "VERIFIED: git show ce73d9d:kubernetes/kyverno-policy.yaml contains both
      'alpine:3.24.1' and 'docker.io/library/alpine:3.24.1' in matchImageReferences; job log
      12:01:09-10Z shows 26-kyverno-policy.sh 'applying kubernetes/kyverno-policy.yaml' ->
      'imagevalidatingpolicy.policies.kyverno.io/require-signed-images created'."
  mechanism_level_result: "FIX (11) WORKED ON CI: ZERO 'denied the request' occurrences in the
      entire log (rounds 6/7 had 14-18); ZERO real Docker Hub 429s (all 23 grep hits are
      coincidental substrings in timestamps/container IDs/byte counts); discover reached
      state=success try=1 in 11-15s in BOTH post-wedge DagRuns (backfill__03:48 12:56:26->
      12:56:41; scheduled__12:54 12:57:22->12:57:33) -- the first discover successes ever
      observed on CI this session. Root cause (10)'s denial mechanism is ELIMINATED."
  signature_level_result: "FALSIFICATION TRIGGERED: pytest '17 failed, 21 passed, 6 skipped,
      16 warnings in 3509.10s (0:58:29)'; node-ID diff vs the invariant 17-test baseline:
      IDENTICAL, all 17 names match -- 10th consecutive occurrence. Failure templates unchanged
      ('meta.files has no row ... within 180s -- discovery never registered it' x7 distinct
      files; 'airflow backfill create ... failed after 3 attempts' x6)."
  residual_mechanism_directly_observed:
    - "Scheduler OOM crash-loop, WORSE than ROUND 3's live-verified 3 restarts: 9 restarts at
      ~6min cadence (12:08:06, 12:14:10, 12:19:56, 12:26:17, 12:33:31, 12:36:59, 12:42:29,
      12:48:31, 12:54:35), kubectl describe scheduler-0: Last State Terminated/OOMKilled/137,
      restart-8 container alive only 38s (12:48:31->12:49:09). First restart 40s after the
      first DagRuns started."
    - "First two DagRuns (scheduled__12:06, backfill__03:47) WEDGED 47min: wait_for_files
      SUCCESS at 12:07:45/12:07:53, then dbt_build marked upstream_failed at 12:07:51/12:07:59
      (inside the first scheduler crash window) while discover NEVER launched (try=0,
      start=None; final state 'skipped' -- consistent with fix (7)'s dagrun_timeout handler
      overwriting at 12:54:44 when both runs went failed). UNEXPLAINED: what marked dbt_build
      upstream_failed seconds after wait_for_files succeeded, when discover never even ran --
      prime ROUND 9 investigation target (suspect: scheduler crash mid-scheduling-loop leaving
      partial TI state, or the orphan-reset path (3c))."
    - "Replacement DagRuns (12:54:45) executed CORRECTLY (discover success) but stage/
      integrity_gate throttled by the global max_active_tis_per_dag slot: 269 'task concurrency
      for this task has been reached' scheduler-log messages (csv_ingest_customers.
      integrity_gate x149, .stage x120); at suite end (13:05) stage map_index TIs still
      state=scheduled try=1 start=None. ROUND 5's source-verified global-limit mechanism is
      now VISIBLE in live logs for the first time (previously masked by the admission denial)."
    - "dag-processor 0 restarts (peak ~559Mi/1Gi), triggerer 0 restarts -- rounds 1-2 fixes
      still holding on their own mechanisms."
  interpretation: "Root cause (10) was REAL and its fix (11) STAYS (a deterministic admission
      denial, live-verified removed on two independent clusters) -- but it was one of MULTIPLE
      sufficient causes, not the sole first domino. With it removed, the failure reverts to the
      scheduler-OOM class (3)/(3b)/(3c) that rounds 1-3 partially treated (restarts went 7->6->3
      across rounds, now BACK UP to 9 with KPO pods actually executing for the first time --
      the memory/work profile changed when admission started succeeding) plus max_active_tis
      throttling making even the healthy post-wedge pipeline too slow for 180s test windows.
      Blind_spot (1) of the ROUND 8 reasoning_checkpoint ('other mechanisms hidden behind the
      first-domino denial') is exactly what materialized -- except the signature did not even
      shrink, because the 180s test timeouts fire identically whether the pipeline is blocked
      by admission denial, scheduler crash-loop, or concurrency throttling."
  next_action: "Present decision checkpoint to user: ROUND 9 direction against the
      scheduler-OOM crash-loop + first-DagRun wedge (upstream_failed anomaly) + global
      max_active_tis_per_dag throughput ceiling, within scope guardrails (no timeout loosening,
      no job splitting, no runner migration)."

ROUND 8 FIX-IMPLEMENTATION RECORD (superseded by the OUTCOME above; kept for the reasoning
    chain -- the falsification_test below is what the OUTCOME's decision-tree branch executed):
  reasoning_checkpoint:
    hypothesis: "Adding the runtime-injected KPO XCom sidecar reference (alpine:3.24.1, the
        exact string provider 10.19.0 writes into every do_xcom_push pod spec; plus the
        registry-qualified docker.io/library/alpine:3.24.1 defensively) to
        kubernetes/kyverno-policy.yaml's matchImageReferences exception list removes the
        deterministic require-signed-images admission denial of all 4 KPO tasks' pods --
        root cause (10) -- because an exempted image is excluded from images.containers
        entirely: BOTH the structurally-impossible cosign verification AND the Docker Hub
        429 manifest fetch simply never happen for it."
    confirming_evidence:
      - "ROUND 7 DENY text names the mechanism verbatim (GET .../library/alpine/manifests/
        3.24.1: 429) on run 32834311083."
      - "Every chain link directly verified: kpo.py do_xcom_push=True (all 4 tasks);
        provider 10.19.0 XCOM_SIDECAR_IMAGE='alpine:3.24.1' (read from inside BOTH the .venv
        and the running LOCAL airflow image); exception list lacked 3.24.1 (both committed
        file and live local policy)."
      - "ROUND 8 LIVE BEFORE/AFTER ON LOCAL: dry-run KPO-shaped probe DENIED with the
        byte-identical CI DENY text (incl. 429) under the old policy, ADMITTED under the
        fixed policy ~2 min later on the same cluster/network; negative control
        (unexempted alpine:3.19) still DENIED -- enforcement intact."
      - "LOCAL Airflow DB shows 22 consecutive stage failures 08-24 08:38-08:48Z (30s-retry
        cadence) -- the same denial active locally, now repaired by the same fix."
    falsification_test: "If the authoritative CI e2e-full run for the pushed commit (with the
        exemption verifiably in the applied policy -- 26-kyverno-policy.sh applies this
        committed file on cluster-up, no staleness path exists) STILL shows the invariant
        17-test node-ID signature unchanged, or ANY Kyverno DENY naming the alpine sidecar,
        the exemption is insufficient and the hypothesis is wrong."
    fix_rationale: "Addresses the root cause (structural exception-list gap for a
        runtime-injected image that no static render can enumerate), not a symptom -- unlike
        rounds 1-7's resourcing/concurrency/retry fixes, which the 9-run invariance proved
        orthogonal. Follow-up B (mirror+sign+remove exemption) later restores the
        verify-everything posture; the exemption is the deliberate, documented interim."
    blind_spots: "(1) CI could fail on OTHER mechanisms hidden behind 9 runs of this
        first-domino denial (e.g. genuine capacity limits rounds 5-7 worried about) -- the
        17-test signature may shrink rather than vanish; partial improvement still confirms
        the hypothesis per the node-ID diff. (2) The LOCAL 08-23 14:18-17:31Z
        post-policy-creation success window is explained only plausibly (webhook-enforcement
        gap during Kyverno churn), not proven -- recorded honestly in Evidence, not
        load-bearing for the fix. (3) publish.yml must rebuild/sign images for the pushed
        sha before DAG pods can pull -- same race every prior round navigated."
  fix_applied: "kubernetes/kyverno-policy.yaml: +2 exception entries ('alpine:3.24.1',
      'docker.io/library/alpine:3.24.1') in sorted positions + header paragraph documenting
      the runtime-injection indirection, the NOT-dead-weight status of alpine:3.17 (it is the
      CNPG db-ping-test Job image, live in all 4 cnpg renders -- KEPT), the provider-bump
      coupling, and the follow-up-B removal plan. Also applied live to the LOCAL cluster
      (same kubectl-apply shape as 26-kyverno-policy.sh), which repaired local's own
      since-08-24 KPO denial as a side effect."
  offline_verification: "make manifests: kubeconform 540 resources 0 invalid 0 errors;
      tests/policy -m 'not manifests': 157 passed / 2 failed (SAME 2 pre-existing
      out-of-scope: test_dag_line_budget customers budget, test_gates_actually_fail);
      test_manifest_resources 5/5; test_values_profiles 6/6; dagtest 14/14. YAML parse clean."
  local_reconciliation: "ANSWERED (see Evidence 2026-08-25 ROUND 8 entries): LOCAL does NOT
      pass -- same provider, same sidecar constant, same policy gap, same DENY text
      live-reproduced via dry-run probe; 'local passes' was stale impression. Not
      ref-normalization, not a policy-version difference."
  live_verification_state: "RECORDED 2026-08-25T12:0xZ: fix pushed as commit ce73d9d (base
      30d4d96). AUTHORITATIVE ROUND 8 live-verification run: e2e-full.yml run 32845181597
      (headSha ce73d9df, created 2026-08-25T11:59:42Z, in_progress at recording time). No
      supersession risk this time: this run-ID-recording docs push uses [skip ci] per the
      ROUND 7 lesson. Companion runs, same headSha: publish.yml 32845181663 --
      already conclusion=SUCCESS (all 3 images built+signed for ce73d9d BEFORE
      the e2e cluster needs them -- the blind-spot (3) image race is already closed);
      e2e-chaos 32845181677 and CI 32845181713 (both out of scope for this signature).
      POST-RUN ANALYSIS STEPS (for the continuation agent, once the single 60s-interval
      watcher reports terminal): (1) verify exemption in force -- grep the job log for
      26-kyverno-policy.sh's 'applying kubernetes/kyverno-policy.yaml' + configured/created
      output (the committed file is applied on cluster-up; no staleness path exists);
      (2) pytest summary + node-ID diff vs the invariant 17-test baseline (Evidence line
      ~2302) -- expect the KPO-dependent tests to flip green / the signature to break for
      the first time in 10 runs; a PARTIAL shrink still confirms hypothesis per the diff,
      then remaining failures are a NEW, distinct signature to triage; (3) grep scheduler
      log for Kyverno DENY -- expect ZERO alpine-sidecar denials (csv-processor/airflow
      GHCR verifications may still appear and must PASS)."
  next_action_superseded: "(DONE -- watcher ran, run terminal, post-run analysis performed;
      see ROUND 8 OUTCOME above for the live next_action.)"
  scope_guardrails: "Rounds 1-7 fixes stay in place. Timeout loosening / job splitting /
      runner migration remain out of scope. Follow-up B is NOT blocking the green signal."
  follow_up_B_verbatim: "Mirror the sidecar image to GHCR, sign it in publish.yml, point
      KPO's sidecar_container_image in airflow/dags/_common/kpo.py at the signed mirror, then
      remove the step-1 exemption. Implement only if step-1 verification goes green and
      budget remains."

ROUND 7 OUTCOME (2026-08-25, post-run analysis of run 32834311083 -- SUPERSEDED BY ROUND 8
    ABOVE, kept for the mechanism chain):
  status: "RESOLVED: user chose direction A+B; ROUND 8 implements A."
  run_analyzed: "e2e-full.yml run 32834311083 (headSha 84e9c74, identical fix code to 32d9911),
      job 97760563853 'Full local E2E suite + rebuild-from-raw capstone', started 09:56:21Z,
      completed 11:03:56Z, conclusion failure. Terminal status confirmed via fallback poller
      after the original watcher died in a network outage (user checkpoint response); log
      fetched via gh api .../jobs/97760563853/logs (5402 lines)."
  falsification_result: "pytest '17 failed, 21 passed, 6 skipped in 3605.36s'. Node-ID diff vs
      the invariant 17-test baseline (Evidence line ~2302): IDENTICAL, all 17 names match --
      the 9TH consecutive byte-identical signature. Regime precondition: (a) parallelism=8
      POSITIVELY VERIFIED in force ('LocalExecutor(parallelism=8)' throughout scheduler log --
      the lever this round's own blind_spots declared load-bearing); (b) max_active_tasks=6 in
      DAG source but zero 'max_active_tasks limit of 6' throttle lines (only logs when binding;
      plausibly never bound under global cap 8 -- ambiguous, not falsified); (c)
      max_units_per_run=10 unobservable ('discovery.units_capped' absent because discover pods
      NEVER RAN -- denied at admission, see below). Kyverno DENY count 18 (was 14) -- secondary
      indicator went UP. Per the pre-registered test: aggregate-load hypothesis REFUTED (blind
      spot (2) 'network-side bottleneck' materialized exactly as anticipated)."
  new_root_cause_discovered: "The ROUND 7 DENY text is DIFFERENT from ROUND 6's and names the
      true mechanism: 'Policy require-signed-images error: failed to evaluate policy: GET
      https://index.docker.io/v2/library/alpine/manifests/3.24.1: unexpected status code 429
      Too Many Requests' -- a policy-evaluation ERROR (not a verification-false), raised while
      admitting discover/publish KPO pods. Chain (every link directly verified): (1)
      airflow/dags/_common/kpo.py line 136 sets do_xcom_push=True on ALL 4 KPO tasks; (2)
      provider 10.19.0 (constraints-pinned, verified in .venv dist-info) injects XCom sidecar
      XCOM_SIDECAR_IMAGE='alpine:3.24.1' (xcom_sidecar.py line 31) into every such pod AT
      RUNTIME; (3) kubernetes/kyverno-policy.yaml's matchImageReferences exception list pins
      'alpine:3.17' -- NOT 3.24.1 -- because the list was built from 'make manifests' renders +
      a live Vault pod query, and a runtime-injected KPO sidecar structurally CANNOT appear in
      any static render (follow-the-indirection bug); (4) so alpine:3.24.1 requires cosign
      keyless verification against publish.yml's OIDC identity -- structurally impossible for a
      Docker-library image; (5) on GH-hosted runners even the manifest fetch 429s (Docker Hub
      per-IP anonymous rate limits on shared runner egress IPs), so failurePolicy:Fail +
      validationActions:[Deny] rejects the pod either way; (6) discover/publish only ever
      reached up_for_retry try 1-5, never success, in the captured scheduler log. This explains
      the 9-run invariance (structural policy mismatch -- no CPU/concurrency/timeout knob
      touches it), ROUND 6's load-correlation observation (early single verifications squeeze
      under the rate limit; suite-time bursts exhaust it), and why the airflow control-plane
      image (no XCom sidecar) verified clean. ROUND 6's DENY was almost certainly the SAME
      sidecar failing the verification expression (its custom message does not name the failing
      image; round 6 attributed it to csv-processor by inference, not observation)."
  open_question: "Why does the LOCAL slice suite pass (if it currently does) with the same
      policy + same provider? Candidates: local cluster's Kyverno/policy version predates this
      ImageValidatingPolicy, local hasn't re-run the slice suite since the policy/provider
      state changed, or local Docker Hub fetches succeed AND alpine:3.24.1 somehow matches the
      exception via a normalization difference. MUST be answered during fix verification."
  next_action: "Present decision checkpoint to user: ROUND 8 direction. Evidence-backed fix
      candidates (NOT yet chosen): (A) add the exact runtime sidecar reference
      'alpine:3.24.1' (and/or 'docker.io/library/alpine:3.24.1', matching however Kyverno
      normalizes ref) to kyverno-policy.yaml's exception list -- one-line, follows the file's
      own pinned-upstream pattern; (B) override the sidecar image in _common/kpo.py to a
      registry not subject to Docker Hub rate limits (KPO exposes the sidecar image via
      PodDefaults/sidecar_container_image), e.g. a GHCR mirror this repo signs itself --
      stronger (also removes the 429 exposure and keeps the verify-everything posture); (C) the
      previously-reserved directions (loosen timeouts / split jobs / leave GH runners) -- now
      DISFAVORED: none address the structural policy mismatch, and leaving GH runners would
      only mask the 429 leg, not the verification-impossible leg."

ROUND 7 (OPENED 2026-08-25 on session resume -- USER-CHOSEN STRATEGIC DIRECTION, not another
    single-cause guess):
  round_6_outcome (recorded from the orchestrator's session handoff; the live watch completed after
      this file's last update): "ROUND 6's falsification test FAILED. The live run for commit
      87cddd4 (Kyverno admissionController CPU 200m->500m + 30s retry_delay on the 4 KPO tasks)
      still showed 14 Kyverno DENY messages in the scheduler log and the SAME invariant 17-test
      failure signature -- the 8th consecutive byte-identical run. Per ROUND 6's own pre-registered
      falsification_test, the Kyverno-CPU-throttling hypothesis is refuted or insufficient. The
      user then explicitly paused the automated round loop: 6 structurally different, individually
      well-evidenced, live-verified fixes across 8 runs produced zero net movement on the
      signature."
  strategic_decision (USER, 2026-08-25, blocking question answered at session resume): "ROUND 7
      implements REDUCE CONCURRENT LOAD: throttle sweep_corpus's 19-file simultaneous ingestion
      and cap active DagRuns so the 4 CPU/16GB GitHub-hosted runner is not oversubscribed. The
      goal is to lower peak platform-wide contention (the confirmed common theme behind scheduler/
      dag-processor OOM, Kyverno admission latency, and pod-start latency) rather than chase
      another single component. Explicitly OUT OF SCOPE this round: loosening per-file timeout
      budgets and splitting cluster-slice-verify into smaller CI jobs (both reserved for a
      possible ROUND 8, run locally); moving this tier off GitHub-hosted runners entirely is the
      final fallback if ROUND 7 fails."
  hypothesis: "The invariant 17-test signature is an emergent property of aggregate concurrent
      load: sweep_corpus uploading 19 files at once plus multiple simultaneous DagRuns
      oversubscribes the ~3 allocatable CPUs, keeping the node in the contention regime where pod
      admission (Kyverno), pod startup, and task execution all exceed the per-file budgets. No
      single component fix moved the signature because each fix only relocated the same fixed
      demand-vs-capacity deficit. Reducing peak concurrency (staggered/batched sweep_corpus
      uploads, max_active_runs / max_active_tis_per_dag caps tuned for the CI profile) attacks the
      deficit itself."
  live_verification_state (2026-08-25T09:52Z, CORRECTED 09:55Z): "Fix committed (32d9911 code)
      and pushed to main. SUPERSESSION NOTE: the first run-ID-recording docs push (84e9c74)
      itself triggered a new push event, which cancelled pending run 32834232783 (9291e98) via
      e2e-full.yml's `group: workflow-ref` concurrency -- the AUTHORITATIVE ROUND 7
      live-verification run is now 32834311083 (headSha 84e9c7457304adf51100ab1d5398416c8f4fb39e,
      which contains the identical ROUND 7 fix code; only docs commits differ from 32d9911).
      publish.yml run 32834311181 (same headSha) is building/signing the 84e9c74 images the DAGs
      will pull. This correction commit is pushed with '[skip ci]' precisely so it cannot
      supersede the queue again -- the lesson: every ROUND's post-fix docs push must either be
      bundled into the fix push or marked [skip ci]. NOTE ALSO: 32834311083 is queued behind
      in-progress run 32822780401 (headSha 3742be8, the PRE-ROUND-7 wip commit; that run's
      result is NOT informative for ROUND 7 and must not be confused with 32834311083's).
      On resume: a SINGLE `gh run watch 32834311083 --exit-status --interval 60` (never
      more than one watcher, never the 3s default), then `gh api repos/KonuTech/airflow-platform/
      actions/jobs/<job-id>/logs` and (1) FIRST verify the reduced-concurrency regime was in force
      (falsification-test precondition): grep for 'max_active_tasks limit of 6' scheduler lines,
      'discovery.units_capped' cap=10 in discover output, and read the DagRun/TI history dump's
      per-run TI census; (2) diff the failing-test NODE-ID list against the invariant 17-test
      baseline (names, not counts); (3) grep for 'denied the request' Kyverno DENY as the
      secondary indicator. Signature gone/materially reduced -> hypothesis confirmed, proceed to
      human-verify. Same 17 node-IDs under verified reduced concurrency -> hypothesis REFUTED per
      the pre-registered falsification test; ROUND 8's reserved directions apply."
  next_action_completed: "IMPLEMENTED (this session, investigation complete -- see
      reasoning_checkpoint ROUND 7 below): three-lever load reduction, all verified against the installed
      apache-airflow==3.3.0 source and this repo's own policy gates before writing a line:
      (a) config.core.parallelism 16->8 in BOTH helm/values/{ci,local}/airflow.yaml (D-06
      behavioral non-divergence rule, same precedent as ROUND 2's 32->16) -- the ONLY truly
      GLOBAL TI cap available (LocalExecutor.start() forks exactly `parallelism` workers up
      front; 8 halves that pool again AND caps platform-wide concurrent TIs at 8);
      (b) max_active_tasks=6 added to both ingest DAGs' @dag(...) -- VERIFIED per-DagRun (NOT
      global across runs) in 3.3.0's _executable_task_instances_to_queued (row_number
      partitioned by [dag_id, run_id], `task_per_dr_count < DM.max_active_tasks`) -- a per-run
      flood guard so no single DagRun's fan-out can monopolize the 8 global slots;
      (c) batching.max_units_per_run 100->10 in configs/datasets/{customers,orders}.yaml -- the
      platform's own metadata-driven batching engine throttles sweep_corpus's 19-file corpus
      into <=10-file claims per DagRun (stage fan-out per run 19->10; listing is sorted
      [discovery.py:881] so the pilot's customers_20240101.csv deterministically lands in batch
      1; corpus drains in 2 claims; configs are baked into the csv-processor image rebuilt by
      publish.yml on the same push, so this reaches CI with zero staleness).
      Then offline-verify (dag structure tests + dagtest, tests/policy incl. values-profiles +
      manifests via make manifests + kubeconform, unit batching tests), commit, push,
      live-verify with a SINGLE `gh run watch --exit-status --interval 60`, then diff the
      failing-test node-ID list against the invariant 17-test baseline, grep Kyverno DENY as
      secondary indicator, and grep the scheduler log for the new 'max_active_tasks limit of 6'
      / 'discovery.units_capped' lines as direct proof the reduced-concurrency regime was
      actually in force (falsification_test requires VERIFIABLY reduced concurrency)."
  falsification_test: "If a fresh live cluster-slice-verify run with materially reduced peak
      concurrency (verifiably fewer simultaneous DagRuns/TIs and staggered sweep_corpus uploads)
      STILL reproduces the same 17-test signature, the aggregate-load hypothesis is refuted and
      ROUND 8's reserved directions (loosen timeouts / split jobs, evaluated locally; or leave
      GH-hosted runners) become the path."

reasoning_checkpoint (ROUND 7 -- written BEFORE any fix edit, per protocol):
  hypothesis: "The invariant 17-test signature is emergent from a fixed demand-vs-capacity
    deficit: aggregate concurrent TI/DagRun/pod load on the ~3.2-allocatable-CPU single-node CI
    cluster keeps it permanently in the contention regime where Kyverno admission verification,
    pod startup, and task execution all exceed per-file budgets -- no single-component fix
    (ROUNDs 1-6) moved the signature because each only relocated the same deficit. Reducing peak
    concurrency itself (global TI cap 8, per-run TI cap 6, per-DagRun file-claim cap 10) lowers
    the deficit directly."
  confirming_evidence:
    - "CI CPU request total 3.180 of a 3.200-core budget (ROUND 6 direct computation) -- the node
      runs at essentially full REQUEST commitment before any ETL pod exists."
    - "Kyverno verification for the SAME commit's airflow image PASSED at 05:45:30 (light load,
      pre-suite) and the csv-processor image was DENIED repeatedly 06:02+ (heavy load, mid-suite)
      in the same run -- load-correlated, not structural (ROUND 6)."
    - "29 'task concurrency reached' scheduler throttle messages across 3 concurrent DagRuns in
      one run; integrity_gate fanning 19 mapped in-process TIs per customers DagRun (each a
      fork+full-import LocalExecutor process in the scheduler pod) (ROUND 5)."
    - "Six structurally different, individually well-evidenced, live-verified fixes across 8 runs
      produced zero net movement -- consistent with a shared upstream cause (aggregate load),
      inconsistent with six independent proximate causes."
  falsification_test: "Pre-registered above (ROUND 7 block): same 17-test node-ID signature under
    verifiably-reduced concurrency refutes the aggregate-load hypothesis."
  fix_rationale: "parallelism 16->8 attacks the load SOURCE (pre-forked LocalExecutor worker pool
    + global concurrent-TI ceiling, apache/airflow#56641's own mechanism, extending ROUND 2's
    measured-effective 32->16 trim); max_active_tasks=6 is a per-DagRun flood guard (verified
    per-run semantics in installed source -- documented honestly, NOT claimed global) so one
    run's fan-out cannot monopolize the 8 slots; max_units_per_run 100->10 throttles the
    19-file corpus at the platform's own batching layer -- the exact knob built for this. None
    of these loosen timeouts, split CI jobs, or revert rounds 1-6 (scope guardrails intact)."
  blind_spots: "(1) max_active_tasks is per-DagRun in 3.3.0 -- with 3 concurrent DagRuns the
    per-DAG bound is 18, so the global parallelism=8 is the load-bearing cap; (2) if the true
    Kyverno bottleneck is network-side (GHCR/Sigstore latency from runner IPs) load reduction
    may be insufficient -- that outcome is exactly what the falsification test detects; (3)
    parallelism=8 + max_units_per_run=10 modestly slow the LOCAL slice suite (more DagRuns to
    drain the corpus, fewer slots) -- accepted, local timeouts have wide margins (2700s); (4)
    config-hash change from (c) rotates idempotency keys, making previously-processed files
    re-eligible once -- by design (config_hash is part of the key), irrelevant in CI's fresh
    clusters."

reasoning_checkpoint (ROUND 6 -- REFUTED/INSUFFICIENT per round_7 block above; kept verbatim for
    continuity -- ROUND 6, written after direct evidence
    confirmed the root cause and BEFORE any fix was applied):
  hypothesis: "Kyverno's `require-signed-images` policy (kubernetes/kyverno-policy.yaml) denies
    KubernetesPodOperator pod creation for `csv_ingest_customers`'s `discover`/`stage`/`dbt_build`/
    `publish` tasks intermittently but pervasively throughout a `cluster-slice-verify` run, because
    the CI admission-controller's CPU limit (200m, the tightest of any Kyverno component in either
    profile) cannot reliably complete cosign's live, per-admission, uncached signature-verification
    network round-trip (GHCR + Sigstore Rekor/Fulcio) fast enough under this node's real, established
    CPU contention (the SAME contention theme ROUNDs 1-3 already proved for the scheduler/
    dag-processor) -- the verification failure/internal-timeout is indistinguishable, from the
    policy's own CEL boolean logic, from a genuinely-unsigned image, so it hard-denies a correctly-
    signed image. This blocks EVERY customers-file discover attempt for as long as the cluster stays
    under contention, independent of which DagRun or which file -- explaining the invariant 17-test
    signature far more completely than any of ROUNDs 1-5's own hypotheses (none of which touch pod
    ADMISSION at all)."
  confirming_evidence:
    - "Direct, unambiguous Kyverno DENY admission responses captured for 4 separate discover/publish
      pod-create attempts (try=2/try=3, two tasks), full exception traceback, clean policy-message
      text ('container image failed cosign keyless signature verification...'), NOT a webhook-timeout
      message (a different, already-eliminated-as-unrelated mechanism from earlier this session)."
    - "DagRun/TaskInstance history: discover NEVER reached `success` in any of the 4 DagRuns this
      whole ~62min session -- final states skipped/up_for_retry/upstream_failed only."
    - "publish.yml's own csv-processor build+sign job (run 32813826173) completed success at
      05:42:48Z -- 7-20+ minutes before the captured denied attempts -- refuting a simple
      'image not pushed yet' cross-workflow race."
    - "Independent `cosign verify` (same certificate-identity-regexp/oidc-issuer as the policy)
      against ghcr.io/konutech/csv-processor:1c111c033f638327b8ed26ee1bf5317715cfd5d4 succeeds
      cleanly right now -- the signature is genuinely, currently valid, not broken."
    - "The triggerer pod's own `kyverno.io/image-verification-outcomes` annotation shows a clean PASS
      for the SAME commit's airflow image at 05:45:30 (lighter load, early in the run) -- Kyverno/
      signing/registry connectivity all fundamentally work; failure correlates with WHEN (how loaded
      the cluster is) verification is attempted, not a structural defect."
    - "8 independent later-failing tests in 6 different modules, each uploading their OWN
      uniquely-named single file (none part of sweep_corpus's 19-file corpus), spread across the
      whole run including tests sorting well after test_backfill_2year_sweep.py alphabetically, ALL
      show the identical 'discovery never registered it' signature -- refutes a one-time startup
      backlog (which should drain, letting later tests succeed) in favor of a persistent, ongoing
      blocking mechanism."
    - "CI's admission-controller CPU limit (200m) is the tightest of any Kyverno component in either
      profile (LOCAL's same container: 500m), on a container doing live crypto+network verification
      with NO caching (confirmed via the policy file's own header comment), matching this repo's OWN
      pre-existing, pre-dating-this-debug-session documented history (kyverno-policy.yaml's Rule 3
      fix, plan 11-06) of this exact verification call taking 15-20s under LIGHTER load than
      cluster-slice-verify's own multi-DagRun, full-pytest-suite window generates."
  falsification_test: "If, after raising the admission-controller's CPU limit plus giving KPO tasks
    more, closer-spaced retry attempts within the existing 45min dagrun_timeout, a fresh live
    cluster-slice-verify run STILL shows Kyverno DENY messages for discover/stage/dbt_build/publish
    pod-create attempts (grep the scheduler log the same way this round did), the CPU-throttling
    hypothesis is refuted or insufficient -- would point to a deeper issue (e.g. GHCR/Sigstore-side
    rate limiting/latency unrelated to Kyverno's own CPU, or a genuine Kyverno/cosign version
    incompatibility) requiring further investigation, not a resourcing fix. If the 17-test signature
    turns materially green (most of the 8 independent 'discovery never registered it' failures
    resolve) and/or zero Kyverno DENY messages appear in a fresh run's scheduler log, this round's
    hypothesis and fix are confirmed."
  fix_rationale: "Targets the CONFIRMED root mechanism directly (verification-under-load reliability
    on the exact container performing it), not a workaround. The CPU limit raise (200m->500m,
    matching LOCAL's own already-proven value for the identical container) costs ZERO CI CPU-request
    budget (limits are never summed into `test_ci_profile_fits_runner`'s gate, confirmed via direct
    source read of `request_totals()`) -- pure burst headroom, the same 'raise LIMIT not REQUEST'
    pattern this session has used successfully before (ROUND 1 fix (1), ROUND 2/3 memory-limit
    raises). The complementary retry_delay tightening (stock Airflow 5min exponential -> a short,
    still-backed-off 30s base) is defense-in-depth: even with more Kyverno CPU headroom, some
    residual load-driven verification flakiness on this CPU-thin node is plausible, and the CURRENT
    retry timing only fits 2-3 attempts into the 45min dagrun_timeout window before force-skip -- a
    short base delay fits many more independent attempts into the SAME existing time budget, without
    weakening the fail-closed policy itself (still mandatory, still Deny -- just more chances to
    catch a moment when the node has spare CPU)."
  blind_spots: "Not yet live-verified -- the live push-and-wait is the real test, per this file's own
    established discipline every round. The EXACT underlying reason verification takes 15-20s+ in the
    first place (registry/Sigstore network latency vs Kyverno's own CPU-bound CEL/crypto work vs
    something else) is not fully isolated -- the CPU-limit raise is a well-evidenced, low-risk,
    zero-budget-cost lever against the most directly-supported contributing factor, but if the true
    bottleneck is instead network-side (GHCR/Rekor latency/rate-limiting from GitHub Actions runner
    IPs, unrelated to Kyverno's own CPU), this fix would have less effect than hoped, and the
    retry_delay tightening becomes the load-bearing mitigation instead. `stage`/`dbt_build`'s OWN
    Kyverno denials were never directly captured in this run's log (only discover/publish were swept
    in via the 'Backfill' substring coincidence) -- applying the SAME retry_delay fix to all 4 KPO
    tasks is a reasoned extrapolation (same policy, same image family, same node), not independently
    confirmed per-task. `smoke_kubernetes_pod`'s own non-terminal-state failure and the SEVENTH
    finding remain out of scope per the task's own scope guardrails, though both are plausibly
    explained by this same mechanism -- noted, not fixed, this round."

hypothesis (ROUND 6, CONFIRMED via direct evidence -- Kyverno pod-admission denial, NOT the
    sweep_corpus/integrity_gate backlog theory this round opened to test. Sits logically ABOVE every
    round below (all kept verbatim for continuity, NOT re-litigated or reverted -- see
    scope_guardrails). Directly answers this round's own charter: traced the sweep_corpus hypothesis
    against 8 independent later-failing tests' own uploaded files (test_backfill_reentry.py,
    test_concurrent_select.py, test_dbt_silver_pipeline.py, test_pod_kill_retry.py x3,
    test_rebuild_from_raw.py, test_smoke_and_idempotency.py -- none part of sweep_corpus's own 19
    files) and found it REFUTED as the proximate mechanism -- see Evidence 'ROUND 6' entries above
    for the full evidence chain. The actual, directly-observed mechanism: Kyverno's
    `require-signed-images` ImageValidatingPolicy denies `discover`/`publish` (and, by extrapolation,
    `stage`/`dbt_build`) pod CREATE requests for this session's own correctly-signed `csv-processor`/
    `dbt` GHCR images, under real CI-node CPU contention that leaves its admission-controller's 200m
    CPU limit unable to reliably complete a live, uncached cosign/registry verification round-trip in
    time.):
  next_action: "Fix committed (87cddd4) and pushed to main. Confirmed via `gh run list
    --workflow=e2e-full.yml`: run 32822697162, headSha 87cddd480424b932b023566d87f01c862d063666
    (matches this commit exactly), status in_progress at push+20s, no competing e2e-full.yml run
    ahead of it in the concurrency queue. Now waiting for terminal status via a SINGLE `gh run watch
    32822697162 --exit-status --interval 60` (per this file's own hard-learned ROUND 2/3 lesson:
    never more than one such watcher concurrently, never the 3s default). Expect ~70-90min total. On
    terminal: fetch the raw job log via `gh api repos/KonuTech/airflow-platform/actions/jobs/<id>/
    logs`, grep the scheduler-log section for 'denied the request'/Kyverno DENY text (should be
    ABSENT or much rarer if the fix worked), re-extract the pytest summary/failing-test list and diff
    against the invariant 17-test baseline (not just the count), per the falsification_test above.
    See reasoning_checkpoint above for the full statement/evidence/rationale. Fix applied: (a)
    helm/values/ci/kyverno.yaml admissionController.container.resources.limits.cpu 200m->500m
    (matches LOCAL's already-proven value, zero CI budget-gate impact); (b)
    airflow/dags/csv_ingest_customers.py discover/stage/dbt_build/publish get an explicit
    `retry_delay=pendulum.duration(seconds=30)` (was: stock 5min exponential default). Offline-verify
    (make manifests + kubeconform, tests/policy/ suite, dagtest), commit, push to main, trigger a
    live e2e-full.yml run via a SINGLE `gh run watch --exit-status --interval 60`, then on terminal
    status fetch the raw job log and grep the scheduler-log section for 'denied the request'/Kyverno
    text AND re-check the pytest failing-test list against the invariant 17-test baseline, per the
    falsification_test above."

hypothesis (ROUND 5, strategic reset -- NOT YET FORMED as a single falsifiable statement. Opens
    with the orchestrator's own reconciliation finding already independently RE-CONFIRMED (see
    Evidence "ROUND 5 -- reconciliation" below) and 3 suggested investigation angles, none yet
    tested. Sits logically ABOVE every round below (all kept verbatim for continuity, NOT
    re-litigated or reverted -- see scope_guardrails). Task guidance is explicit: a 5th
    plausible-single-cause guess without direct evidence is not an acceptable outcome this round --
    the bar is a DIRECT observation of the actual failure mechanism (task pod logs, MinIO/DB state,
    real pytest execution order), not another well-reasoned theory.):
  ruled_out_this_round_before_starting (via the orchestrator's own prior-session investigation,
      independently re-verified by this round's own reconciliation check -- see Evidence below):
    - "'The fix isn't reaching the cluster/tests' -- RULED OUT for all 4 rounds' worth of fixes.
      helm/values/ci/airflow.yaml sets dags.persistence.enabled:false and dags.gitSync.enabled:false
      -- DAGs are hostPath-mounted directly from the CI runner's own live `actions/checkout`,
      never baked into the published GHCR image (confirmed: `csv_ingest_customers.py` does not
      exist anywhere inside `ghcr.io/konutech/airflow:f0ebfe3`). Helm values changes, DAG file
      changes, and test-fixture changes (pytest imports the fresh checkout directly) are ALL
      structurally guaranteed live/current with zero staleness or sync lag."
    - "ROUND 4's own specific hypothesis (DAG-pause causing a scheduler livelock) -- DISCONFIRMED,
      not merely unconfirmed: its fix (8) is proven present/active (repo-wide grep on current HEAD
      confirms `_pause_customers_dag_for_backfill_only_tests` is genuinely gone, replaced by a
      session-scoped autouse `_unpause_slice_dags` keeping `csv_ingest_customers` permanently
      unpaused for the whole tests/e2e/slice/ session) yet produced a byte-for-byte-identical 6th
      consecutive failure run (independently re-confirmed this round via direct `gh api` log fetch,
      not just the orchestrator's summary -- see Evidence below)."
  suggested_angles_from_orchestrator (investigate empirically, do not assume which is correct):
    - "A1: pytest's REAL execution/collection order for `make cluster-slice-verify` (not
      alphabetical -- actual conftest fixture scoping, directory walk order, no explicit
      random-order plugin assumed). Find the FIRST test in that real order that needs `customers`
      ingested, trace its discover-task attempt log-line by log-line in a fresh live run. If the
      very FIRST customers-ingestion attempt in the whole suite run reliably fails/never happens,
      that single early event could explain most/all of the cascade."
    - "A2: a resource that is NOT actually ephemeral/fresh per run -- verify directly (not assume)
      that no external state (MinIO bucket/object-lock/versioning, a PV, GHCR layer cache, etc.)
      carries identical poisoned state across all 6 runs to date."
    - "A3: something upstream of Airflio -- er, Airflow -- entirely: did the very first customers
      CSV actually land in the raw MinIO bucket for a fresh run? What does discover_files' OWN task
      pod log show it saw? The pytest assertion only reports 'discovery never registered it' --
      the real failure could be 1-2 layers upstream of Airflow (upload rejection/delay, bucket
      policy, object-lock) and invisible in the test's own assertion text."
    - "Priority: DIRECT observation of the actual failure mechanism (live discover/stage task pod
      logs for the very first customers file of a run) over another plausible-sounding theory."
  mechanism_investigation_this_round (Phase 1/2 COMPLETE -- source-level, offline; narrows the 3
      suggested angles to concrete, testable candidates BEFORE spending a live run; full detail in
      Evidence below, summarized here for Current Focus continuity):
    - "A1 confirmed structurally: `make cluster-slice-verify` runs `pytest tests/e2e/cluster
      tests/e2e/slice -q` with NO random-order plugin, no xdist `-n`, no addopts affecting order
      (confirmed via pyproject.toml). pytest's real collection order is therefore file-alphabetical
      within each directory: tests/e2e/cluster/* (mostly clean/skipped) then tests/e2e/slice/*
      starting with test_backfill_2year_sweep.py (alphabetically first) -- matches ROUND 2's own
      deep-mined progress-string decoding exactly."
    - "NEW, load-bearing fact this round's source read surfaced that no prior round checked: Airflow
      3.3.0's `_start_queued_dagruns` (scheduler_job_runner.py) computes 'active runs' via
      `Counter` keyed by `(DagRun.dag_id, DagRun.backfill_id)` -- a regular schedule-created DagRun
      (backfill_id=None) and a backfill-created DagRun (backfill_id=<id>) of the SAME dag_id are
      counted and capped COMPLETELY INDEPENDENTLY (`dag_run.max_active_runs` vs
      `backfill.max_active_runs` respectively). This REFUTES a hypothesis this round formed and
      then disproved via direct source read before ever committing it to Current Focus as 'the'
      theory (recorded here, not in Eliminated, since it was refuted by reasoning before being
      tested live): 'the regular schedule's own DagRun exhausts the DAG's max_active_runs=1 slot,
      permanently blocking the pilot backfill's DagRun from ever leaving QUEUED, the same
      mechanism ROUND 4 found for is_paused.' That mechanism does NOT exist for max_active_runs
      specifically -- a regular DagRun and a backfill DagRun of csv_ingest_customers CAN be RUNNING
      simultaneously. ROUND 4's own is_paused finding is UNAFFECTED (that gate is DAG-wide, applied
      identically before either counter is ever consulted)."
    - "What IS confirmed still-shared/global (direct source read,
      `_executable_task_instances_to_queued`): `max_active_tis_per_dag` (set on stage/dbt_build/
      publish) is keyed by `(dag_id, task_id)` ONLY -- no backfill_id in the key -- so a regular
      DagRun's and a backfill DagRun's stage/dbt_build/publish TaskInstances DO compete for ONE
      truly global slot each, ordered `-priority_weight, DR.logical_date, TI.map_index` (earlier
      logical_date wins ties). The pilot backfill's artificially-offset window
      (`_window(offset_minutes=500,...)`, ~8.3h 'in the past' relative to `now()`) would generally
      have an EARLIER logical_date than a live regular-schedule tick, meaning simple FIFO-by-
      logical-date priority should favor the backfill's own stage TIs, not starve them -- but this
      is exactly the kind of interaction that needs DIRECT observation (which DagRun's TIs actually
      ran, in what order) rather than further reasoning from source alone."
    - "wait_for_files (S3KeySensor, deferrable=True) has NO explicit `timeout=` override anywhere in
      csv_ingest_customers.py -- confirmed directly against the installed Airflow config on the
      LOCAL cluster: `[sensors] default_timeout = 604800` (7 days) is what BaseSensorOperator's own
      `timeout=None` parameter falls back to. Practically bounded only by `dagrun_timeout=45min` --
      but ONLY for a DagRun already in RUNNING state (per ROUND 4's own already-confirmed
      `get_running_dag_runs_to_examine` scope); a DagRun stuck in QUEUED is not bounded by this at
      all, exactly as ROUND 4 found for the is_paused case."
    - "`sweep_corpus` (module-scoped fixture, test_backfill_2year_sweep.py) uploads the FULL 20-day
      corpus (19 real customers files, `customers_20240101.csv` through `customers_20240203.csv`-ish,
      one gap day, `_START_DATE=2024-01-01`) via direct `s3_client('app').put_object(...)` calls, ALL
      BEFORE the module's first test runs (even before the --dry-run test) -- confirmed via direct
      read of the fixture source. This means `raw/customers/` stops being empty within the first few
      minutes of `cluster-slice-verify` (once pytest reaches this module, shortly after
      tests/e2e/cluster's quick checks), not late in the run and not never -- weakens (does not
      eliminate) a pure 'sensor never finds anything, ever' framing, and instead raises a NEW,
      concrete, testable mechanism: the REGULAR schedule's own uncontrolled, test-blind DagRun(s)
      (created automatically every minute from the moment cluster-up finishes, well before any test
      controls it) may 'win the race' to discover this freshly-landed 19-file corpus via its OWN
      wait_for_files/discover cycle, then spend many minutes processing files nobody intended it to
      process via the shared max_active_tis_per_dag=1 stage/dbt_build/publish slot(s) -- a materially
      different mechanism than anything ROUNDs 1-4 tested, not yet confirmed or refuted by direct
      evidence."
    - "A2 (non-ephemeral carried-over state) structurally weakened: `.github/workflows/e2e-full.yml`'s
      own code comment confirms NO data-seeding step exists before `cluster-slice-verify` --
      `rebuild-from-raw`'s own comment explicitly says it reuses cluster-slice-verify's OWN
      already-populated history 'with no data-seeding step between them', meaning the raw bucket
      starts genuinely empty each run. Combined with the structural fact that CI's kind cluster +
      all Helm-installed workloads (including MinIO, a Deployment with its own PVC) are created
      fresh inside a brand-new ephemeral GitHub Actions runner and destroyed at job end (no volume,
      registry cache, or DB state persists across separate `gh run` invocations by construction) --
      A2 is not fully proven false by direct observation this round, but has no structural mechanism
      left to hide behind either; not pursued further as a leading candidate absent contrary evidence."
    - "Triggerer (wait_for_files' own deferred-poke executor) has been monitored by ZERO prior
      round's diagnostics -- confirmed via direct re-read of every diagnostic step in e2e-full.yml.
      Direct re-mining of run 32779160265's own already-fetched log shows `airflow-triggerer-0`
      healthy (2/2 Ready, 0 restarts) for the FULL visible tail of `cp-monitor-allpods.log`
      (~last 12-15min of the 61min run, all samples '0,0' restarts, cumulative count so this implies
      0 restarts for the pod's whole ~64min life) -- weakens but does not eliminate a triggerer
      resource-health mechanism, since K8s events have a ~1h TTL and the FIRST ~45min (the actual
      window dagrun_timeout would still be silent in) has zero surviving direct evidence either way."
    - "Genuine platform landmine caught and fixed BEFORE it could break this round's own new
      instrumentation (recorded so a future round does not rediscover it the hard way): CI's
      `airflow-scheduler` renders as `kind: StatefulSet` (confirmed directly in
      build/manifests/ci/airflow.yaml, `executor: LocalExecutor`) with pod name `airflow-scheduler-0`
      -- DIFFERENT from LOCAL's `kind: Deployment` (`executor: KubernetesExecutor`) that every prior
      round's own `kubectl exec deploy/airflow-scheduler ...` LOCAL-cluster source-reading commands
      correctly relied on. A first draft of this round's own new CI diagnostic copied that
      LOCAL-only `deploy/airflow-scheduler` shape and would have failed outright in CI with
      'deployments.apps \"airflow-scheduler\" not found' -- caught via direct rendered-manifest
      inspection before pushing, fixed to reference the pod directly (`airflow-scheduler-0`),
      matching the already-correct pattern this round's own log-grep commands used from the start."
  next_action: "Phase 0/1/2 COMPLETE (reconciliation + source-level mechanism investigation, see
    Evidence below). Phase 3 (instrumentation, COMPLETE, offline-verified): added NEW throwaway
    diagnostics to .github/workflows/e2e-full.yml's existing 'DEBUG: dump control-plane resource
    monitor + final diagnostics' (if: always()) step -- (1) full csv_ingest_customers DagRun history
    (run_id/type/state/backfill_id/start/end/logical_date, ALL DagRuns, queried directly from the
    metadata DB via a python heredoc against the live scheduler pod -- a single end-of-run query
    reconstructs the WHOLE session's DagRun timeline for free, no periodic polling needed, since DB
    rows persist across scheduler restarts unlike pod logs); (2) key TaskInstance history
    (wait_for_files/discover/stage/dbt_build/publish specifically, same query technique); (3)
    scheduler log (current+previous container) grepped for csv_ingest_customers lines mentioning
    contention/constraint/concurrency/backfill/pause (the EXACT log-message substrings this round's
    own source read of _start_queued_dagruns/_executable_task_instances_to_queued confirmed Airflow
    emits at the precise moment either concurrency gate blocks a DagRun or TaskInstance -- direct,
    unambiguous mechanism evidence if captured, not inference); (4) triggerer pod status/describe +
    log tail (never captured by any prior round); (5) MinIO raw/customers/ listing with per-object
    LastModified timestamps, via `mc` inside the MinIO pod itself (no external tooling/ingress
    dependency) -- answers definitively WHEN the corpus actually landed relative to the DagRun
    timeline. Also extended cp-monitor.sh's existing role loop to include `triggerer` (cgroup
    memory/pids/restart-count time series, closing the one genuine blind spot that DOES need a time
    series rather than an end-of-run snapshot). Offline-verified: YAML parses (`python3 -c
    'yaml.safe_load'`), extracted bash syntax clean (`bash -n`), both embedded python heredocs
    syntax-clean (`python3 -m py_compile`), all resource references (StatefulSet pod name for
    scheduler in CI specifically, Deployment for MinIO, StatefulSet for triggerer, secret name
    minio-root) cross-checked against the actual rendered CI manifest and confirmed correct -- NOT
    just copied from LOCAL-cluster-only prior-round commands. Full offline policy suite (`uv run
    pytest tests/policy/ -q -m \"not manifests\"`, 159 collectible): 157 passed, 2 failed -- the
    SAME 2 pre-existing, already-documented out-of-scope failures every prior round in this file has
    shown -- zero new regressions. Phase 4 (next): commit + push this instrumentation-only change
    (touches ONLY .github/workflows/e2e-full.yml -- zero production code, zero fix applied yet, per
    this round's own explicit mandate to get DIRECT evidence before proposing a fix), trigger a live
    e2e-full.yml run, wait for terminal status via a SINGLE `gh run watch <id> --exit-status
    --interval 60` (per this file's own hard-learned ROUND 2/3 lesson: never more than one watcher,
    never the 3s default), then fetch the raw job log and directly read the 5 new diagnostic
    sections to determine: (a) how many csv_ingest_customers DagRuns exist and what state each is in
    at run-end, (b) whether the corpus's own file-arrival timestamps (MinIO listing) precede or
    follow the FIRST successful wait_for_files completion, (c) whether stage/dbt_build/publish
    TaskInstances for EITHER the regular schedule's or the backfill's own DagRun ever actually ran
    (and how many, how long each took), (d) whether the scheduler's own log shows an explicit
    constraint/concurrency message naming csv_ingest_customers, (e) triggerer's own health/activity
    signal. Do NOT propose or apply a fix until this direct evidence is in hand and points to ONE
    specific, falsifiable mechanism -- per this round's own explicit charter, a 5th plausible guess
    without direct evidence is not an acceptable outcome."
    (pickup, 2026-08-25T05:41Z): instrumentation committed (1c111c0) and pushed to main. Confirmed
    via `gh run list --workflow=e2e-full.yml --json databaseId,status,conclusion,headSha,createdAt`:
    run 32813826344, headSha 1c111c033f638327b8ed26ee1bf5317715cfd5d4 (matches this commit exactly),
    status in_progress at push+20s, no competing e2e-full.yml run ahead of it in the concurrency
    queue (the immediately-prior run 32779160265 already reached `completed` before this push, per
    this file's own hard-learned ROUND 2 lesson about e2e-full.yml's serial-queue concurrency
    group) -- no queue-delay expected this time. Now waiting for terminal status via a SINGLE
    `gh run watch 32813826344 --exit-status --interval 60` (per this file's own hard-learned ROUND 3
    lesson: never more than one such watcher concurrently, never the 3s default). Expect ~70-90min
    total (cluster setup + ~61min cluster-slice-verify, matching every prior round's own timing).
    On terminal: fetch the raw job log via `gh api repos/KonuTech/airflow-platform/actions/jobs/
    <id>/logs`, read the 5 new diagnostic sections directly (DagRun history, key TaskInstance
    history, scheduler log grep, triggerer status+log, MinIO listing) plus the still-existing
    pytest summary/failing-test list and cp-monitor.csv (now including triggerer), and update
    Current Focus/Evidence/Resolution with whatever the direct evidence actually shows -- per this
    round's own charter, form the fix hypothesis from THIS data, not from pre-run speculation."

hypothesis (ROUND 4, test-suite DAG-pause bug -- CONFIRMED as the actual proximate cause of the
    session's persistent 17-test failure signature, independent of scheduler/dag-processor
    restart count entirely. Sits logically ABOVE every round below (all kept verbatim for
    continuity, NOT re-litigated or reverted -- see scope_guardrails) since it answers this
    session's own PRIMARY mandate for the first time with a mechanism that actually explains the
    invariant 17-test signature, rather than another restart-count-focused partial mitigation):
  statement: "`tests/e2e/slice/test_backfill_2year_sweep.py::
    _pause_customers_dag_for_backfill_only_tests` (module-scoped, autouse=True) pauses
    `csv_ingest_customers` before the module's first real backfill test runs, and Airflow 3.3.0
    treats a paused DAG's DagRuns -- backfill-created or schedule-created, no distinction -- as
    permanently frozen in `queued` state (never reaching `running`, never queuing a single
    TaskInstance) for as long as the DAG stays paused, not merely 'stops creating new
    schedule-created runs' as the fixture's own docstring assumed. This freezes
    test_pilot_window_drains_without_cpu_starvation's own backfill on every live CI run
    ('missing entirely', not 'stuck mid-pipeline'), which then blocks every later test in the
    same module via Airflow's own AlreadyRunningBackfill uniqueness constraint, and plausibly
    seeds downstream resource contention for later files once the DAG is finally unpaused again
    at module teardown."
  falsification_plan: "CONFIRMED via (1) direct source read of the installed
    apache-airflow==3.3.0 (SchedulerJobRunner._executable_task_instances_to_queued's
    `~DM.is_paused` filter; DagRun.get_queued_dag_runs_to_set_running's INNER JOIN on
    `DagModel.is_paused == false()`; DagRun.get_running_dag_runs_to_examine only returning
    RUNNING-state DagRuns, meaning dagrun_timeout structurally cannot reach a QUEUED-stuck
    DagRun) and (2) a live empirical reproduction on the LOCAL cluster (paused
    smoke_kubernetes_pod, created a real backfill, observed its DagRun stuck in `queued` and its
    TaskInstance stuck at `state=None` for 50+ continuous seconds with
    `last_scheduling_decision=None`, proving the scheduler never examined it even once while
    paused). See Evidence entries below for full detail.
  next_action: "Fix (8) committed (f0ebfe3) and pushed to main (ADDITIVE on top of commit 20d151f
    -- does not touch/revert any prior round's fix). Triggered run 32779160265 (headSha f0ebfe3,
    status queued at push+~30s, no competing e2e-full.yml run in progress so no concurrency-queue
    delay expected this time). Now waiting for terminal status (~70-90min: cluster setup +
    ~60min cluster-slice-verify) via a SINGLE `gh run watch 32779160265 --exit-status --interval
    60` (widened interval, per this file's own hard-learned ROUND 3 lesson: never run more than
    one such watcher concurrently, and never at the 3s default -- both caused a self-inflicted
    GitHub API rate-limit exhaustion once already this session). On terminal: fetch the raw job
    log via `gh api repos/KonuTech/airflow-platform/actions/jobs/<id>/logs`, extract the pytest
    summary line and the full failing-test list, diff against the invariant 17-test baseline
    recorded above (not just the count). Per the reasoning_checkpoint's own falsification_test
    below: CONFIRMED if the 6 test_backfill_2year_sweep.py failures (or most of them) turn green
    and/or a materially different failure signature appears in their place; REFUTED or
    insufficient if the exact
    same 17-test signature recurs unchanged."
    (continuation pickup, 2026-08-24T21:38:31Z): re-confirmed run 32779160265 directly (`gh run
    view --json status,conclusion,createdAt,updatedAt,headSha,workflowName`) -- status still
    `in_progress`, conclusion empty, headSha confirmed f0ebfe31bcb9db05895b67be5bcc4ce5bd79d7bc
    (matches fix (8) exactly), createdAt 21:22:40Z (~16min elapsed, consistent with the
    cluster-setup phase of the expected ~70-90min total). `gh api rate_limit` checked: 4927/5000
    remaining -- healthy, not at risk. `ps aux` revealed TWO pre-existing background trackers
    already alive from before this continuation started (not started by this continuation): (1)
    PID 406964, `gh run watch 32779160265 --exit-status --interval 60`, elapsed 15:17 -- matches
    this next_action's own documented plan exactly; (2) PID 412682, a hand-rolled until-loop
    polling `gh run view --json status -q .status` every 120s, elapsed 4:29. Combined polling
    load (~1.5 API calls/min) is trivial and not a rate-limit risk on its own -- NOT the cause of
    any prior exhaustion (that was 3 processes at the 3s DEFAULT interval). Per this file's own
    hard-learned ROUND 3 lesson (never run more than one watcher concurrently), did NOT start a
    third GitHub-API-polling process. Instead, verified `tail (GNU coreutils) 9.4` supports
    `--pid`, then started (run_in_background) a ZERO-API-COST local wait: `tail --pid=406964 -f
    /dev/null` -- blocks purely locally (kill-0 polling, no ptrace/parentage required) until the
    existing primary watcher process (406964, `gh run watch --exit-status`) exits, which happens
    exactly when the run reaches terminal status. This achieves the same 'wait for terminal
    status' goal as the existing next_action with zero additional GitHub API load, fully
    respecting the 'exactly ONE watcher' constraint while not wasting the two already-running
    trackers' progress. On notification that this local wait exits: proceed exactly as originally
    planned below (fetch raw job log via `gh api repos/KonuTech/airflow-platform/actions/jobs/
    <id>/logs`, extract the pytest summary line and full failing-test list, diff against the
    invariant 17-test baseline, update Current Focus/Evidence/Resolution per the
    reasoning_checkpoint's falsification_test)."
  reasoning_checkpoint (MANDATORY, fix_and_verify Phase 0 -- written before this round's fix was
      applied, after direct source-level investigation against the installed
      apache-airflow==3.3.0 on the live LOCAL scheduler pod plus a live empirical reproduction):
    hypothesis: "Pausing `csv_ingest_customers` via `_pause_customers_dag_for_backfill_only_tests`
      causes `test_pilot_window_drains_without_cpu_starvation`'s own backfill-created DagRun to
      freeze permanently in `queued` state (Airflow 3.3.0 ties `is_paused` to a total DagRun
      freeze, not merely 'no new scheduled runs'), and this single frozen DagRun -- via Airflow's
      own Backfill-uniqueness constraint (`_mark_backfills_complete()` requires no DagRuns in
      RUNNING/QUEUED) -- explains the AlreadyRunningBackfill cascade blocking the module's other 5
      tests, and plausibly the downstream resource contention affecting later files too, all
      independent of scheduler/dag-processor restart count."
    confirming_evidence:
      - "Direct source read, installed apache-airflow==3.3.0, live LOCAL scheduler pod:
        SchedulerJobRunner._executable_task_instances_to_queued (scheduler_job_runner.py:524)
        filters `.where(~DM.is_paused)` when selecting TIs to queue -- no backfill carve-out."
      - "Direct source read: DagRun.get_queued_dag_runs_to_set_running (models/dagrun.py) INNER
        JOINs on `DagModel.is_paused == false()` -- a paused DAG's queued DagRuns never match this
        query at all, so they never transition to RUNNING, backfill or scheduled alike."
      - "Direct source read: DagRun.get_running_dag_runs_to_examine (feeding _schedule_dag_run,
        which enforces dagrun_timeout per ROUND 3's own source read) only returns DagRuns ALREADY
        RUNNING -- a QUEUED-stuck DagRun is never even considered, so ROUND 3's fix (7) structurally
        cannot reach this failure mode, directly explaining its own zero effect on the 17-test set."
      - "Live empirical reproduction, LOCAL cluster: paused smoke_kubernetes_pod, created a real
        backfill (`airflow backfill create` succeeded immediately -- confirmed NOT gated by
        is_paused, matching a direct source read of airflow/models/backfill.py), then observed
        DagRun state stuck at `queued` and TaskInstance state stuck at `None` for 50+ continuous
        seconds with `last_scheduling_decision: None`, while the scheduler pod was independently
        confirmed actively looping (continuous log activity) -- direct, unambiguous, zero-inference
        evidence the scheduler never examined this DagRun even once while the DAG was paused."
      - "In-repo corroboration: tests/e2e/slice/conftest.py::_unpause_slice_dags's own docstring
        (predates plan 10-07's pause fixture) already documents the exact mechanism and the exact
        symptom text in plain language: 'A paused DAG's scheduler simply never starts a run for it
        -- there is no error, no timeout shortcut, just silence.' tests/e2e/slice/
        test_smoke_and_idempotency.py::test_smoke_dag_xcom_contains_built_sha independently
        confirms the same Airflow behavior from a different angle in its own docstring."
      - "Structural match: test_backfill_2year_sweep.py has exactly 7 tests; the FIRST
        (test_dry_run_sizing_reports_reasonable_dagrun_count, a --dry-run call unaffected by
        DagRun-level freezing) is the only one NOT in the 17-test failure list; the other 6 (the
        first of which, test_pilot_window_drains_without_cpu_starvation, is the first REAL
        backfill this module creates) are ALL in the failure list -- an exact structural match for
        the suite's own observed '1 pass, then a wall of straight failures' shape."
    falsification_test: "If, after this fix, a fresh live cluster-slice-verify run still shows the
      EXACT SAME 17-test failure set (or test_pilot_window_drains_without_cpu_starvation still
      shows 'missing entirely'/AlreadyRunningBackfill), the hypothesis is refuted or this fix is
      insufficient by itself -- would indicate either a second, still-unidentified DAG-pause-style
      landmine elsewhere (repo-wide grep this round found none), or the mechanism is real but not
      the dominant driver of the persistent signature after all."
    fix_rationale: "Removes the CONFIRMED root mechanism directly (the fixture that freezes
      DagRun progress) rather than compensating for its symptom. Does not attempt to preserve the
      fixture's own original anti-contention goal (preventing occasional stage-slot races between
      backfill and live-schedule DagRuns, plan 10-07 finding 4) via a different Airflow-native
      mechanism, because no such mechanism exists in Airflow 3.3.0 that decouples 'stop new
      scheduled DagRuns' from 'stop all DagRun progress' -- both are the SAME `is_paused` flag,
      confirmed via exhaustive source read. Trades finding 4's narrower, already-mitigated
      (1-3 tick windows, retries=6 with exponential backoff) risk for eliminating a deterministic,
      suite-wide, catastrophic cascade that reproduced on every single live run this session."
    blind_spots: "Not yet live-verified against real CI contention (no live cluster in this
      sandbox) -- the live push-and-wait is the real test, exactly per this file's own
      established discipline throughout every prior round. The downstream 'later files also fail'
      half of the explanation (large recovering backfill competing for CI CPU/task-slot capacity
      once unpaused again) is plausible and consistent with the orchestrator's own finding (b)
      but was NOT separately live-instrumented this round -- if later files' tests do NOT turn
      green even after test_backfill_2year_sweep.py's own 6 tests do, that would indicate a
      SEPARATE, still-uninvestigated mechanism for those specific failures, not a refutation of
      this round's core finding. test_no_extra_schemas_exist remains explicitly out of scope
      (unrelated mechanism, unchanged disposition from every prior round)."

hypothesis (ROUND 3, scheduler memory growth mechanism -- REOPENS root cause 3b a THIRD time since
    ROUND 2's fix (b1ef8e2: core.parallelism 32->16, CI scheduler memory limit 1Gi->1536Mi) is now
    LIVE-VERIFIED INSUFFICIENT (see Evidence entry "ROUND 3 -- ROUND 2 fix live-verification
    results" immediately above). Sits logically ABOVE the ROUND 2 block below (kept verbatim for
    continuity, NOT re-litigated except where this round's own new evidence directly bears on it)
    and does NOT re-litigate ROUND 1 (scheduler CPU/health-check thresholds), the dag-processor
    memory fix, or the vault-0 fixes -- all remain fully solid, live-verified, and out of scope.
    NOT YET FORMED as a single falsifiable statement -- this round opens with a live investigation
    phase (source-level + log-mining) before committing to a specific mechanism, per task guidance
    to "investigate empirically rather than assuming which mechanism" and avoid a 4th
    partial-mitigation cycle):
  candidate_mechanisms_to_distinguish:
    - "M1 (livelock/retry-accumulation, already source-confirmed as PLAUSIBLE in ROUND 2 but not
      yet confirmed as the DOMINANT term): the SAME small set of tasks keep getting orphan-reset
      (not failed) and re-attempted indefinitely because OOM-cycle period < task completion time,
      and `_mark_backfills_complete()` never clears the stuck backfill -- each re-attempt possibly
      leaves residual state (in the metadata DB, and/or rebuilds larger in-memory scheduler
      structures each loop as more TIs sit in a retry-eligible state) that compounds. Needs: (a)
      confirmation `adopt_or_reset_orphaned_tasks()` truly does not consume a `try_number`/retries
      slot (so the task can loop forever rather than eventually reaching a terminal FAILED state
      that would let `_mark_backfills_complete()` clear it), (b) confirmation the SAME test
      (test_pilot_window_drains_without_cpu_starvation, per REOPENED ROUND 2's deep-mining Evidence)
      is the trigger every round, not a coincidentally-equal count masking a different set."
    - "M2 (sustained per-task CoW/allocator growth INSIDE the persistent LocalExecutor worker
      processes themselves, per apache/airflow#56641's own SUSTAINED-not-just-startup variant --
      distinct from the already-confirmed-partial startup-import-overhead mechanism ROUND 2 fixed):
      would explain why peak_pids shrinking (48->33, tracking parallelism) did NOT proportionally
      shrink peak memory -- if the DOMINANT growth term is per-worker CoW accumulated across many
      sequential task executions in the SAME long-lived worker process (not a one-time import cost
      at fork time), fewer workers each doing MORE cumulative task-churn could net out to similar or
      even worse total growth, matching the observed data better than a pure pool-size-proportional
      model."
    - "M3 (scheduler MAIN process's own bookkeeping growth -- e.g. `executor.running`/
      `event_buffer`/callback-queue/zombie-detection state -- growing specifically BECAUSE of the
      livelock's stuck DagRuns/TaskInstances, i.e. M1 and M3 may be the SAME underlying mechanism
      viewed from two angles: the DB-state livelock (M1) is the TRIGGER, and M3 is HOW that trigger
      manifests as actual heap growth in the pod's cgroup)."
    - "M4 (something else entirely -- e.g. a genuine leak in KubernetesPodOperator's in-process
      watch/log-streaming loop under LocalExecutor accumulating faster with MORE distinct task
      executions over wall-clock time, independent of both pool size and the livelock)."
  falsification_plan: "Not a single falsification_test yet -- this round's Phase 1 (investigation
    techniques: source read of adopt_or_reset_orphaned_tasks/TaskInstance retry semantics inside
    the actual installed apache-airflow==3.3.0, plus a name-for-name diff of this round's failing
    test list against the already-recorded ROUND-1 list, plus re-reading test_backfill_2year_sweep.py
    and the production DAG files' own `retries`/`execution_timeout`/`retry_delay` config) is
    designed to distinguish M1/M3 (livelock-driven, DB-state-persistent) from M2/M4 (pure
    per-process/per-task resource accumulation, no DB-state dependency) BEFORE proposing a fix --
    per this session's own mandatory reasoning_checkpoint discipline, a fix will not be proposed
    until a specific, falsifiable mechanism is confirmed by direct evidence, not inference alone."
  next_action: "Phase 1: read airflow.jobs.scheduler_job_runner.SchedulerJobRunner.
    adopt_or_reset_orphaned_tasks() and the TaskInstance state-reset path it calls (does it consume
    a try_number/retries slot, or is it a free/unlimited reset?) directly from the installed
    apache-airflow==3.3.0 source -- via live LOCAL cluster `kubectl exec` if available in this
    sandbox (established technique, used successfully in ROUND 2), else via GitHub raw source for
    the pinned tag. Phase 2: read airflow/dags/csv_ingest_customers.py, csv_ingest_orders.py, and
    tests/e2e/slice/test_backfill_2year_sweep.py in full for `retries`/`execution_timeout`/
    `retry_delay`/`max_active_tis_per_dag` config on the specific tasks involved in the observed
    16-straight-failure wall (integrity_gate, stage, dbt_build, publish). Phase 3: if `gh` CLI is
    available, fetch the raw job log for run 32755940740/job 97523386546 to (a) extract the exact
    17 failing test names and diff against the already-recorded ROUND-1 failing-test list
    (REOPENED ROUND 2 deep-mining Evidence, this file) to confirm/refute 'same trigger every round',
    and (b) pull the full cp-monitor.csv (not just the orchestrator's summary) for a finer-grained
    restart/memory timeline correlated against pytest's own test-boundary timestamps if obtainable.
    Then form ONE specific, falsifiable hypothesis (per the mandatory reasoning_checkpoint before
    any fix) and design a fix that targets the confirmed mechanism -- not a ceiling raise."
  reasoning_checkpoint (MANDATORY, fix_and_verify Phase 0 -- written before any fix is applied,
      after Phase 1/2/3 source-level investigation directly against the installed
      apache-airflow==3.3.0 on the live LOCAL scheduler pod plus a name-for-name failing-test diff
      of the live ROUND 2 verification run):
    hypothesis: "The scheduler's OOM-crash-loop under sustained cluster-slice-verify load is
      driven primarily by an UNBOUNDED task-instance retry livelock, not (only) by eagerly-forked
      worker-pool size: when the scheduler pod's whole cgroup is OOM-killed mid-task, the ONLY
      recovery path that survives the restart (adopt_or_reset_orphaned_tasks, DB-state-driven,
      runs at every scheduler startup) resets the interrupted TaskInstance to schedulable state
      WITHOUT ever calling is_eligible_to_retry()/handle_failure() -- unlike the NORMAL failure
      path (_process_executor_events -> ti.handle_failure()), which DOES enforce
      try_number<=max_tries and respects retry_delay/exponential backoff, but depends on the
      LocalExecutor's own in-memory event queue, destroyed by the same whole-pod OOM kill that
      interrupted the task. Since this project's affected tasks legitimately take ~13-15min
      (integrity_gate+stage alone, per test_backfill_2year_sweep.py's own docstring) while the
      observed post-first-OOM cadence is ~6-7min, an interrupted task NEVER gets to either
      complete OR exhaust its retries=6 budget -- it is reset and immediately rescheduled (NO
      backoff delay, since UP_FOR_RETRY/retry_delay is bypassed entirely by the reset-to-None
      path) every scheduling loop, forever. Because both production DAGs have max_active_runs=1
      (shared across backfill AND regular schedule-created DagRuns of that dag_id) and
      stage/dbt_build/publish share a GLOBAL max_active_tis_per_dag=1 slot, ONE permanently-stuck
      DagRun blocks EVERY future DagRun of that dag_id for the rest of the run (matching
      AlreadyRunningBackfill and 'missing entirely'/discovery-never-registered failures observed
      identically across all 4 live runs to date) -- and since csv_ingest_customers is on a
      1-minute cron, a NEW DagRun row is created every minute that can never start, growing an
      ever-larger backlog the scheduler must re-examine every ~1s loop for the rest of the run.
      This explains why ROUND 2's parallelism trim (32->16, which correctly halved peak_pids
      48->33) did NOT proportionally reduce peak memory (which instead grew, 910MiB->1471MiB) --
      the dominant growth term is DB-state/backlog-driven and time-proportional, not
      eager-fork-pool-size-proportional."
    confirming_evidence:
      - "Direct source read of the INSTALLED apache-airflow==3.3.0 (live LOCAL scheduler pod, via
        kubectl exec): SchedulerJobRunner.adopt_or_reset_orphaned_tasks() unconditionally does
        `ti.prepare_db_for_next_try(session)` then `ti.state = None` for every TI in
        State.adoptable_states={RESTARTING,RUNNING,QUEUED} whose queuing Job is not RUNNING -- NO
        call to is_eligible_to_retry()/handle_failure() anywhere in this function."
      - "Direct source read confirms is_eligible_to_retry()/handle_failure() are ONLY invoked
        from TaskInstance.fetch_handle_failure_context (called by handle_failure) and
        SchedulerJobRunner._process_executor_events() (scheduler_job_runner.py:1579) -- the
        latter fires only when the executor's OWN in-memory result/event queue delivers a
        completion event, which cannot happen for a task whose entire hosting process (scheduler
        + all LocalExecutor workers, same cgroup) was just OOM-SIGKILLed simultaneously."
      - "Direct source read of DagRun.schedule_tis() (dagrun.py): a reset (state=None, non-
        UP_FOR_RESCHEDULE) TI's try_number IS incremented on next scheduling
        (`else_=TI.try_number + 1`) -- confirming this is a real, climbing retry loop, not a
        static no-op -- but max_tries is never recomputed by the reset path, and nothing checks
        try_number<=max_tries before this unconditional increment-and-reschedule."
      - "Direct source read of csv_ingest_customers.py/csv_ingest_orders.py: retries=6 (stage/
        dbt_build/publish/discover), retry_exponential_backoff=True, NO dagrun_timeout set on
        either @dag(); max_active_runs=1 (DAG-level, both files); stage/dbt_build (both files)
        and publish (customers only) max_active_tis_per_dag=1 GLOBAL. Confirmed via grep: no
        retry_delay/max_retry_delay override anywhere in this project (stock 5min default,
        uncapped exponential multiplier applies)."
      - "Direct source read of SchedulerJobRunner._schedule_dag_run(): dagrun_timeout
        enforcement IS purely DB-state-driven (dag_run.start_date vs dag.dagrun_timeout, checked
        fresh every scheduling loop for every active DagRun) -- unlike the in-memory
        executor-event path, this SURVIVES a scheduler restart. On timeout it force-sets
        dag_run.state=FAILED AND explicitly sets every unfinished TI (state in State.unfinished
        or None) to SKIPPED -- directly breaking the reset-and-reschedule loop for that DagRun's
        stuck TI(s), not merely flagging it."
      - "Live run 32755940740/job 97523386546 (ROUND 2 fix verification) name-for-name
        failing-test diff against the 3 already-recorded pre-ROUND-2 runs: IDENTICAL 17-test set,
        same test_pilot_window_drains_without_cpu_starvation 'missing entirely' signature, same
        AlreadyRunningBackfill text naming csv_ingest_customers specifically, across FOUR
        independent runs now regardless of the parallelism/memory-ceiling change -- consistent
        with a deterministic, DB-state-driven trigger (this specific test's own backfill getting
        caught by the livelock) rather than generic time-proportional resource exhaustion alone."
    falsification_test: "If, after adding dagrun_timeout to both production DAGs, a fresh live
      cluster-slice-verify run STILL shows scheduler restarts (any count > 0) OR the SAME
      AlreadyRunningBackfill/missing-entirely failure signature recurring with no
      SKIPPED/timed-out DagRun evidence in between, the livelock hypothesis is refuted or this
      fix is insufficient by itself -- would indicate either the growth has a genuinely separate,
      still-unidentified driver (M2/M4 from this round's candidate list) independent of the
      DB-backlog/livelock mechanism, or dagrun_timeout's own enforcement has a gap not caught by
      this round's source read."
    fix_rationale: "Targets the CONFIRMED root mechanism (an unbounded retry loop that bypasses
      Airflow's own retry-exhaustion/backoff enforcement specifically because the only
      restart-surviving recovery path never calls it) directly, using an existing, first-class,
      DB-state-driven Airflow safety net (dagrun_timeout) proven (by direct source read of the
      SAME installed version) to force-terminate a stuck DagRun and its unfinished
      TaskInstances even with zero in-memory state -- not another resource-ceiling raise
      (explicitly out of favor per this round's own task guidance, and ROUND 1->ROUND 2's own
      data already shows the peak chasing the ceiling upward rather than converging under it).
      Does not touch core.parallelism/scheduler memory limits at all -- a genuinely different,
      complementary fix axis to ROUND 2's, addressing the SPECIFIC gap ROUND 2's own live data
      exposed (memory growth getting WORSE as a % of ceiling despite the pool being halved)."
    blind_spots: "45 minutes (pendulum.duration(minutes=45), reusing this test suite's own
      already-established 'single-window backfill' 2700s precedent rather than inventing a new
      number) is a judgment call balancing 'long enough to not false-positive-kill a legitimately
      slow-but-progressing DagRun under real CI contention plus realistic
      KubernetesJobWatcher-race retry/backoff overhead' against 'short enough to meaningfully
      bound the livelock's duration within a single ~62min suite run' -- NOT validated against a
      live run's own precise internal timing (when exactly test_pilot_window's backfill starts
      relative to cluster-slice-verify's own ~62min budget is inferred, not directly measured),
      so this value may need retuning in a follow-up round based on live evidence, exactly as
      ROUND 1->ROUND 2's own ceiling value was iteratively refined. Does NOT address the
      SEPARATE, already-documented (Phase 9/10, pre-dating this debug session), out-of-scope
      structural throughput question the test's OWN docstring raises (the 1-minute cron creating
      DagRuns that compete with a backfill for the same global max_active_tis_per_dag=1 slot) --
      dagrun_timeout bounds worst-case stuck duration but does not make that pre-existing
      contention faster. Does not guarantee all 17 currently-failing tests turn green (some may
      need test-level timeout retuning as a separate follow-up, matching this session's own
      established scope discipline of separating crash-loop root causes from downstream
      test-tuning) -- this round's own success bar (per task guidance) is scheduler restarts
      dropping to 0, not 100% green tests. Not live-tested in this sandbox (no live cluster
      reproduces CI's LocalExecutor+real-contention topology here) -- the live push-and-wait is
      the real test, exactly per this file's own established discipline throughout every prior
      round."
  next_action: "Fix (7) committed (20d151f) and pushed to main -- triggered run 32765704491
    (headSha 20d151f, status pending/in_progress at push+10s). cp-monitor.sh instrumentation
    confirmed still present at current HEAD (.github/workflows/e2e-full.yml lines ~107-205).
    NOTE: an unrelated prior run (624cf4f, a pure docs commit predating this fix) is ALSO
    still in_progress concurrently -- do NOT mistake its results for this fix's own; only
    32765704491/headSha 20d151f carries fix (7). Now waiting for 32765704491 to reach terminal
    status (~70-90min: cluster setup + ~60min cluster-slice-verify) via `gh run watch 32765704491
    --exit-status` (a single opaque blocking command, not a sleep loop, per this file's own
    already-established environment-compatible waiting technique from the ROUND 2 continuation).
    On terminal: fetch the raw job log via `gh api repos/KonuTech/airflow-platform/actions/jobs/
    <id>/logs`, extract cp-monitor.csv + the final `kubectl describe pod -l component=scheduler`
    snapshot, and the failing-test list (diff against this round's own 17-test baseline to see
    whether test_pilot_window_drains_without_cpu_starvation's specific cascade breaks, and
    whether a SKIPPED/timed-out DagRun appears in place of the AlreadyRunningBackfill cascade --
    the falsification_test's own predicted signature). Update Current Focus with
    CONFIRMED/REFUTED/PARTIAL per the reasoning_checkpoint's falsification_test -- fix confirmed
    only if scheduler restarts drop to 0 (per task guidance's own explicit bar), not just
    fewer/later, before considering ROUND 3 resolved.
    (continuation pickup, 2026-08-24T19:06:43Z): re-verified run 32765704491 directly
    (`gh run view --json status,conclusion,createdAt,updatedAt,headSha,workflowName`) --
    status still `pending`, conclusion empty, headSha confirmed 20d151f, jobs array still empty
    (~5.5min since createdAt 19:01:01Z, consistent with cluster-setup-phase queuing, nothing
    alarming). Starting the live wait now via `gh run watch 32765704491 --exit-status` launched
    with run_in_background (survives the single-Bash-call 10-minute cap and this session's own
    repeatedly-documented background-poller-death pattern by using the harness's own
    notify-on-completion mechanism rather than a hand-rolled sleep loop) -- not abandoning early.
    UPDATE (~19:19Z, still empty jobs array after 17+min pending): diagnosed WHY -- confirmed via
    `grep -A5 '^concurrency:' .github/workflows/e2e-full.yml`: `concurrency.group:
    ${{ github.workflow }}-${{ github.ref }}`, `cancel-in-progress: ${{ github.event_name ==
    'pull_request' }}` (false for a push-to-main trigger, which this is) -- e2e-full.yml runs on
    the SAME branch queue strictly serially, never auto-cancel. `gh run list --workflow=e2e-full.yml`
    confirms run 32762092524 (headSha 624cf4f, the pure-docs commit already flagged in this
    next_action's own earlier note as unrelated/predating fix 7) is `in_progress`, job started
    18:28:41Z, currently mid-step-13 ('Run cluster + slice E2E suite') -- run 32765704491 is
    QUEUED BEHIND IT, not stuck/broken, and cannot even START (jobs array stays empty) until
    32762092524 finishes its ENTIRE job (steps 13-17: cluster-slice-verify ~60min +
    diagnostics-dump + observability-verify-ci + rebuild-from-raw capstone + issue-filing), not
    just its own cluster-slice-verify step. Revises the original ~70-90min total-wait estimate
    upward substantially (must add 32762092524's OWN remaining wall-clock, likely 60-90+ more
    minutes from ~19:19Z, before 32765704491 even begins its own ~70-90min). The existing
    background `gh run watch 32765704491 --exit-status` process remains the correct strategy
    (it will keep reporting pending/queued until the queue clears, then track 32765704491 itself
    through to terminal) -- not restarting it, just documenting the extended timeline so a future
    continuation is not confused by an apparently-stuck pending status.
    SELF-CORRECTING NOTE (~19:20-19:52Z): this continuation made a real process mistake here --
    started THREE concurrent `gh run watch`/polling background processes (bafc3ondo direct watch,
    bhfkfgiy4 chained blocker-then-target watch, bbperv5px a 30s-interval until-loop) plus several
    manual one-off `gh run view`/`gh api` checks, all against the same default 3s-refresh
    `gh run watch` internal polling cadence -- this collectively EXHAUSTED the GitHub REST API core
    rate limit (confirmed via `gh api rate_limit`: 0/5000 remaining, reset at 2026-08-24T20:10:04Z).
    Two of the three background processes (bafc3ondo, bbperv5px) died with `HTTP 403: API rate
    limit exceeded` -- NOT a real workflow-run failure signal, a self-inflicted artifact of this
    continuation's own over-polling. Lesson for any future continuation in this file: `gh run
    watch` defaults to a 3s refresh (`-i/--interval` flag exists to widen it); NEVER run more than
    ONE such watcher concurrently against the same or related runs.
    REAL DATA RECOVERED from the third process (bhfkfgiy4) before ITS OWN eventual rate-limit
    crash, still fully valid (read from the completed run's own terminal JOBS/ANNOTATIONS block,
    not from an error path): blocker run 32762092524 (headSha 624cf4f, unrelated pre-fix docs
    commit) reached a REAL terminal conclusion -- `Process completed with exit code 2`, failed at
    step 'Run cluster + slice E2E suite (observability deferred, staggered below)' after
    1h8m45s job time (job started 18:28:41Z -> finished ~19:37Z) -- consistent with every prior
    round's known pre-fix failure pattern (same commit lineage predates fix 7 entirely, expected
    to fail identically to the 17/21/6 baseline). This freed the `e2e-full.yml` concurrency queue:
    target run 32765704491 (fix 7, headSha 20d151f) began ACTUALLY RUNNING (job assigned, ID
    97565550961) at ~19:37Z -- confirmed progressing cleanly through ALL cluster-setup steps
    (checkout, setup-uv, install-cluster, cluster-up, control-plane monitor start, image config,
    DB migrations, Grafana webhook, Vault unseal/bootstrap -- every one showing a checkmark) and
    was captured mid-step 'Run cluster + slice E2E suite (observability deferred, staggered
    below)' (the ~60min step this round's own success bar depends on) at the last successful
    poll before the rate-limit crash -- i.e. the run itself is healthy and progressing normally,
    the crash was purely local tooling exhaustion, not a run-side problem.
    REVISED next_action: wait (via a LOCAL-time-only check loop -- no GitHub API calls at all,
    avoiding any further rate-limit risk) until the reset time (2026-08-24T20:10:04Z + a 30s
    safety buffer), then issue exactly ONE `gh api rate_limit` sanity check, then start exactly
    ONE `gh run watch 32765704491 --exit-status --interval 60` (widened interval, ~20x fewer
    calls than the 3s default) as the sole live tracker through to terminal status. On terminal:
    proceed as originally planned (fetch raw job log, extract cp-monitor.csv + final `kubectl
    describe pod -l component=scheduler` snapshot, check pytest summary, compare against the
    zero-restart success bar).
    Also confirmed via `ps aux` (local process inspection, no API cost): an EXTERNALLY-started
    poller (`round3verify[32765704491]=...`, 90s interval after an initial ~19min sleep, NOT
    started by this continuation) is already independently tracking this same run -- matches this
    file's own previously-documented pattern (ROUND 2 continuation's note: 'a second concurrent
    tracker of this same run... not started by this continuation'). Gentle cadence, not a
    rate-limit risk on its own, left untouched -- not the cause of the earlier exhaustion (this
    continuation's own 3-process/3s-interval mistake was)."

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
  next_action: "(environment note, mid-wait: this continuation's first background poller process
    was killed by the sandbox partway through the live wait -- confirmed this does NOT affect the
    actual GitHub Actions run, which executes independently of local polling; re-confirmed via a
    fresh direct `gh run view` immediately after the kill notification that 32755940740 was still
    `in_progress`, and a second background poller plus other already-running monitoring processes
    -- apparently a separate concurrent tracker of this same run, PIDs 312171/312746, not started
    by this continuation -- were still alive at that point. Still waiting; not abandoning.
    UPDATE: a second self-started poller (a `sleep 300` loop, the same shape as the first) was
    ALSO killed shortly after -- both of this continuation's own hand-rolled sleep-loop waits died,
    while the externally-started `gh run watch`/label-poller processes (not started by this
    continuation) stayed alive throughout, consistent with the environment specifically guarding
    against detectable manual sleep-loop patterns (matching this environment's own documented
    guidance: 'Do not chain shorter sleeps to work around the block'). Switched strategy to `gh run
    watch 32755940740 --exit-status` (a single opaque blocking command, not a sleep loop) instead
    -- exactly what the task instructions themselves suggested as an alternative. Still waiting; not
    abandoning.)
    Fix committed (b1ef8e2) and pushed to main -- no queue this time (the prior
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

- timestamp: 2026-08-24 (ROUND 3 -- ROUND 2 fix live-verification results, gathered by the human
    orchestrator directly from GitHub Actions against run 32755940740/job 97523386546 (commit
    68986ed, same lineage as fix commit b1ef8e2), recorded here verbatim per this file's own
    established discipline for evidence provided by another party; full round-over-round
    comparison table also supplied by the orchestrator)
  checked: >
    cp-monitor.csv (15s-interval poll, same instrumentation as ROUND 2's own confirmation run) for
    the SAME cluster-slice-verify suite, directly compared against ROUND 1's identical-instrumentation
    baseline (run 32743870344/job 97491592863, already recorded above): scheduler peak_mem_bytes,
    peak_mem-as-%-of-limit, peak_pids, restart count, time-to-first-restart, post-first restart
    cadence, dag-processor restarts/peak-mem (as an unaffected control), and a final `kubectl
    describe pod -l component=scheduler` snapshot at run end.
  found: >
    Pytest: "17 failed, 21 passed, 6 skipped" in 3718.44s (1:01:58) -- IDENTICAL failure count and
    near-identical duration to ALL THREE prior runs on this commit lineage (17/21/6, 3704-3718s
    range across four total runs now, extending the reproducibility-check evidence already
    recorded above to a FOURTH occurrence -- this time POST-fix, meaning the fix changed the
    resource-restart mechanics measurably but did not change the pytest-visible outcome at all).
    Failing test SET not yet re-diffed name-for-name against the prior round's list this round --
    flagged as an explicitly open check, investigated below.
    scheduler, ROUND 1 (1Gi limit, parallelism=32) vs ROUND 2 (1536Mi limit, parallelism=16):
      peak_mem_bytes:       954,281,984 (~910MiB)   ->  1,542,131,712 (~1471MiB)   [+62%]
      peak as % of limit:   89% of 1Gi               ->  95.8% of 1536Mi            [WORSE, not better]
      peak_pids:             48                       ->  33                        [-31%, tracks parallelism halving]
      restart count:          7                        ->  6                        [small reduction]
      time to FIRST restart: ~79s into cluster-slice-verify -> ~40min in            [major delay -- real]
      cadence after first:   irregular (one 31m52s gap, then 5-7min repeats) -> regular ~6-7min apart, all 5 remaining
    dag-processor (unaffected control): 0 restarts both rounds; peak ~763MiB/1Gi (ROUND 1) vs
    ~745MiB/1Gi (ROUND 2) -- statistically flat, confirms root cause (2)'s fix continues to hold
    cleanly under the heavier suite regardless of this round's changes; not reopened, not
    investigated further.
    Final `kubectl describe pod -l component=scheduler` (captured 18:28:32Z, run end): the
    immediately-prior container instance (Restart Count 6) shows `Last State: Terminated / Reason:
    OOMKilled / Exit Code: 137`, `Started: 18:25:02Z / Finished: 18:25:45Z` (43s alive) --
    UNAMBIGUOUS: still a genuine cgroup memory-ceiling breach against the NEW 1536Mi limit, not
    merely a stale artifact of the OLD 1Gi one.
  implication: >
    ROUND 2's fix (commit b1ef8e2) is CONFIRMED INSUFFICIENT -- a real, measurable, PARTIAL
    improvement (delayed onset, slightly fewer restarts, smaller eagerly-forked pool exactly as
    designed), but restarts do not drop anywhere near zero and OOMKilled recurs against the raised
    ceiling itself, 43s after the final restart's container even started. Critically, peak memory
    as a PERCENTAGE of the ceiling got WORSE (89%->95.8%) even though the ceiling grew 50% AND the
    pre-forked worker population was HALVED: if growth were purely proportional to worker-pool size
    (ROUND 2's own fix_rationale), trimming parallelism in half should have roughly halved the
    growth rate or peak, not left it proportionally WORSE against a bigger ceiling. This is fairly
    strong evidence that eagerly-forked-worker import overhead (apache/airflow#56641's documented
    mechanism, ROUND 2's own root-cause finding) is at most a PARTIAL contributor to the observed
    growth, and something else scales with elapsed wall-clock time or cumulative task-execution/
    retry count over the ~62min sustained suite -- consistent with, but not yet directly proven to
    be, the livelock mechanism already source-confirmed this session (`_mark_backfills_complete()`
    never clearing while a task keeps getting interrupted mid-execution via repeated
    OOM-SIGKILL-then-orphan-reset cycles, potentially causing the SAME task(s) to be retried/
    re-attempted indefinitely rather than a fixed one-time cost per restart). The delayed
    time-to-first-restart (79s->40min) is real signal that trimming the eagerly-forked pool helped
    delay ONSET -- it does not, by itself, explain or fix the ONGOING accumulation once sustained
    execution is underway; note also the cadence AFTER the first restart became MORE regular and
    stayed fast (~6-7min, not shrinking further but also not recovering), suggesting whatever
    resumes accumulating post-restart does so at a fairly constant rate once triggered, not a
    runaway acceleration. "Raise the ceiling again" (a third time) is explicitly not indicated by
    this data: ROUND 1->ROUND 2 shows the peak chasing the ceiling upward rather than converging
    under it. This round's investigation must target the accumulation MECHANISM directly.

- timestamp: 2026-08-24 (ROUND 3 -- post-push corroborating check, performed while waiting for
    live-verification run 32765704491 to reach terminal status)
  checked: >
    Whether fix (7)'s dagrun_timeout-driven forced SKIP of in-flight `stage`/`dbt_build`/`publish`
    TaskInstances could leave this project's OWN business-level tracking (`meta.run_stages`,
    separate from Airflow's own TaskInstance state) in a NEW kind of stuck state --
    airflow/dags/_common/run_stage_recorder.py (DBT_BUILD tracking) and
    packages/dataplat/src/dataplat/metadata/postgres.py's claim_run_stage/heartbeat_run_stage/
    complete_run_stage (STAGE_LOAD/PUBLISH tracking, D-14) read in full.
  found: >
    Both existing, pre-dating-this-debug-session mechanisms ALREADY self-heal an abandoned
    in-flight stage independent of Airflow's own task/DagRun state: (1)
    list_run_ids_pending_dbt_build's own docstring explicitly documents a `RUNNING`-status
    DBT_BUILD row as "a retry candidate" -- any FUTURE DagRun's own call to this function will
    find and re-attempt it (`db.status IN ('FAILED', 'RUNNING')`), regardless of why the
    ORIGINAL attempt's DagRun never finished. (2) claim_run_stage uses a `lease_expires_at`
    mechanism (5-minute lease, `postgres.py` line ~389) with an explicit reclaim predicate
    (`status = 'RUNNING' AND lease_expires_at < now()`, line ~500) -- an abandoned STAGE_LOAD/
    PUBLISH claim becomes reclaimable by ANY future attempt 5 minutes after its last heartbeat,
    entirely independent of Airflow's own TaskInstance/DagRun state machine.
  implication: >
    Fix (7) does not fight or bypass either of this project's OWN pre-existing recovery
    mechanisms -- it COMPLEMENTS them. Before this fix, a permanently-stuck DagRun blocked
    max_active_runs=1 forever, meaning no FUTURE DagRun could ever exist to exploit either
    self-healing mechanism (the lease/RUNNING-retry-candidate design was present in the code but
    structurally unable to activate, since nothing could ever create a subsequent DagRun to do
    the reclaiming). Fix (7) directly unblocks this: once a stuck DagRun is force-FAILED and its
    stage/dbt_build claims naturally go stale (heartbeats stop the moment the underlying pod's
    watch loop is interrupted, well before the 45min dagrun_timeout even fires), the VERY NEXT
    DagRun for that dag_id (regular-schedule or a fresh backfill) can both re-claim the expired
    STAGE_LOAD/PUBLISH leases AND re-attempt the RUNNING DBT_BUILD row -- exactly the recovery
    path this project's own D-14/LOAD-06 design already anticipated. No new stuck-state class is
    introduced by this fix.

- timestamp: 2026-08-24 (ROUND 4 -- orchestrator-supplied live verification of ROUND 3 fix (7),
    against run 32765704491/job 97565550961, commit 20d151f; recorded here verbatim per this
    file's own established discipline for evidence provided by another party, matching e.g. the
    "ROUND 2 continuation -- instrumented live run results" entry above. This CORRECTS the
    "NOT YET LIVE-VERIFIED" status this file's own Current Focus/Resolution carried when this
    evidence was supplied -- the orchestrator independently pulled and analyzed this run after
    that text was last written.)
  checked: >
    cp-monitor.csv (15s-interval poll, same instrumentation as ROUND 2/3's own confirmation
    runs) for the SAME cluster-slice-verify suite, PLUS a name-for-name diff of the failing-test
    SET (not just the count) across all 5 live runs to date (2 pre-ROUND-2, ROUND-2-verify,
    ROUND-3-verify, and the original run) -- performed by the orchestrator directly against
    GitHub Actions, supplied here as DATA per this session's security/evidence-recording
    discipline (never as instructions).
  found: >
    Pytest: "17 failed, 21 passed, 6 skipped" in 3728.76s (1:02:08) -- IDENTICAL failed/passed/
    skipped counts to every prior run on this commit lineage (a FIFTH occurrence of the same
    pattern). The exact same 17 test node-IDs failed, with the exact same error-message
    TEMPLATES (only random UUID suffixes differ), across EVERY one of the 5 live runs regardless
    of which fix was applied:
      tests/e2e/cluster/test_postgres_topology.py::test_no_extra_schemas_exist
      tests/e2e/slice/test_backfill_2year_sweep.py::test_pilot_window_drains_without_cpu_starvation
      tests/e2e/slice/test_backfill_2year_sweep.py::test_full_2year_sweep_customers_and_orders
      tests/e2e/slice/test_backfill_2year_sweep.py::test_idempotent_rerun_produces_zero_additional_rows
      tests/e2e/slice/test_backfill_2year_sweep.py::test_live_run_concurrent_with_backfill_same_dataset
      tests/e2e/slice/test_backfill_2year_sweep.py::test_mass_delete_snapshot_trips_circuit_breaker_without_mutating_gold_state
      tests/e2e/slice/test_backfill_2year_sweep.py::test_scd_concurrent_attribute_change_and_correction_same_key
      tests/e2e/slice/test_backfill_reentry.py::test_backfill_resolves_previously_rejected_row
      tests/e2e/slice/test_concurrent_select.py::test_concurrent_select_never_observes_partial_publish
      tests/e2e/slice/test_dbt_silver_pipeline.py::test_fresh_customers_file_flows_through_stage_dbt_build_publish
      tests/e2e/slice/test_pod_kill_retry.py::test_pod_kill_mid_load_produces_no_duplicates
      tests/e2e/slice/test_pod_kill_retry.py::test_pod_kill_mid_dbt_build_produces_no_duplicates
      tests/e2e/slice/test_pod_kill_retry.py::test_u3_throughput_and_peak_rss_baseline
      tests/e2e/slice/test_rebuild_from_raw.py::test_rebuild_from_raw_reconciles_and_reverts_quarantine_to_pending
      tests/e2e/slice/test_referential_orphan.py::test_orphan_order_quarantined_while_valid_rows_publish
      tests/e2e/slice/test_smoke_and_idempotency.py::test_smoke_dag_xcom_contains_built_sha
      tests/e2e/slice/test_smoke_and_idempotency.py::test_idempotent_reupload
    Meanwhile scheduler restart count dropped substantially and monotonically across the SAME 3
    fixes this session already applied: ROUND 1 baseline 7 -> ROUND 2 (parallelism trim + memory
    headroom) 6 -> ROUND 3 (dagrun_timeout, same memory/parallelism as ROUND 2) only 3 this run
    (20:18:08Z r=1 mem=349MiB, 20:23:48Z r=2 mem=123MiB, 20:30:01Z r=3 mem=274MiB,
    peak_mem_bytes=1,549,049,856 ~1477MiB, ~96% of the 1536Mi ceiling; dag-processor fully clean
    at peak ~762MiB/0 restarts throughout).
    Two distinguishable failure shapes within the 17: (a) 11 of 17 are "meta.files has no row...
    within 180s -- discovery never registered it" / "airflow backfill create ... failed after 3
    attempts" / cascading preconditions from those; (b)
    test_smoke_and_idempotency.py::test_smoke_dag_xcom_contains_built_sha differs in KIND, not
    just instance: "did not reach a terminal state within 180s (last observed state: 'running')"
    -- a DagRun that WAS created, WAS triggered, and had a task ACTUALLY running, just not
    finished in time; (c) test_no_extra_schemas_exist ("unexpected schema(s): ['meta']") remains
    flagged (per prior rounds) as likely-unrelated test-ordering/environment noise.
  implication: >
    Cutting scheduler restarts from 7->3 (more than half, across THREE genuinely different,
    well-targeted, independently-real mechanisms: CPU/thresholds, memory/parallelism, and a
    livelock-timeout) did not change which tests fail or how many, AT ALL, across FIVE
    independent live runs spanning every restart-count regime this session has produced. This is
    now the strongest possible evidence that the scheduler restart/OOM pattern, while real and
    independently worth fixing (and NOT reverted -- see scope_guardrails), is NOT the proximate
    cause of this specific, persistent 17-test failure signature. ROUND 3's fix (7) is hereby
    corrected from "NOT YET LIVE-VERIFIED" to LIVE-VERIFIED (it measurably reduced restarts
    exactly as its own falsification_test predicted) but is ALSO now confirmed INSUFFICIENT to
    explain or resolve the session's primary symptom -- opens ROUND 4 to find the actual
    proximate cause of the 17-test signature, independent of scheduler restart count entirely,
    per task guidance.

- timestamp: 2026-08-24 (ROUND 4 -- chronological/structural investigation, independent of
    scheduler restart count per task guidance)
  checked: >
    Chronological pytest collection order for cluster-slice-verify (`grep -n "^def test_"` in
    file-definition order), cross-referenced against ROUND 2's own deep-mining evidence decoding
    the suite's progress string: "tests/e2e/cluster ran almost entirely clean... THEN
    tests/e2e/slice opens with one more pass, then hits a wall and produces 16 STRAIGHT FAILURES
    ... with ZERO further passes". tests/e2e/slice/test_backfill_2year_sweep.py (alphabetically
    first in tests/e2e/slice/, and the file whose own tests account for 6 of the 17 failures) has
    EXACTLY 7 test functions, in this file-definition order: test_dry_run_sizing_reports_
    reasonable_dagrun_count (a `--dry-run` CLI call, NOT in the failing list), then
    test_pilot_window_drains_without_cpu_starvation (IN the failing list), then 5 more (all IN
    the failing list) -- an exact structural match for "1 pass, then 16 straight failures". Read
    the module's own fixtures (lines 394-639) in full to find what module-level state could
    explain a wall starting at the SECOND test specifically.
  found: >
    `_pause_customers_dag_for_backfill_only_tests` (module-scoped, `autouse=True`) pauses
    `csv_ingest_customers` via `airflow dags pause` BEFORE the module's first test runs, and only
    unpauses in a `finally` block firing after the module's LAST test completes (one exception:
    `test_live_run_concurrent_with_backfill_same_dataset` gets a dedicated
    `_live_concurrency_needs_dag_unpaused` fixture that unpauses for just that test then
    re-pauses). Its own docstring states the purpose: prevent this module's 5 backfill-only tests
    from losing a race for the shared, GLOBAL `max_active_tis_per_dag=1` slot on
    `stage`/`dbt_build`/`publish` against `csv_ingest_customers`' own live `*/1 * * * *` schedule
    (a real, previously-fixed bug from plan 10-07, per the module's own docstring finding 4) --
    implicitly assuming pausing the DAG only stops the scheduler from creating NEW
    regular-schedule DagRuns, while a backfill's OWN already-created DagRun keeps progressing.
    `tests/e2e/slice/conftest.py::_unpause_slice_dags` (session-scoped, autouse, written BEFORE
    plan 10-07's module-scoped pause fixture existed) already documents, in plain language, the
    opposite: "Discovered live: ... nothing anywhere in this suite unpaused
    `csv_ingest_customers`, which every test that uploads a file and polls for discovery...
    depends on actually running on its own schedule/sensor. A paused DAG's scheduler simply never
    starts a run for it -- there is no error, no timeout shortcut, just silence -- so every one
    of those tests would poll `meta.files` until its own deadline and fail with a misleading
    'discovery never registered it' message that looks like a pipeline bug." A verbatim, in-repo
    description of the EXACT symptom text dominating ROUND 4's own 17-test signature, sitting in
    the SAME directory as the fixture that (module-scoped, added later) reintroduces exactly what
    it warns against.
    `tests/e2e/slice/test_smoke_and_idempotency.py::test_smoke_dag_xcom_contains_built_sha` (one
    of the 17 failing tests) independently confirms the same mechanism from a different angle:
    "Unpausing first is required -- a paused DAG's triggered run stays `queued` forever (`airflow
    dags trigger --help`'s own documented behaviour)" -- and its code correctly unpauses
    `smoke_kubernetes_pod` before triggering it.
  implication: >
    A strong, specific, in-repo-corroborated hypothesis: IF Airflow 3.3.0 treats a paused DAG's
    backfill-created DagRuns identically to regular-schedule ones (not yet confirmed at this
    point -- requires direct source read, done next), `test_pilot_window_drains_without_cpu_
    starvation`'s own backfill -- created by `_invoke_backfill_create` WHILE `csv_ingest_
    customers` is already paused -- would never progress at all, explaining "missing entirely"
    (not "stuck mid-pipeline") precisely, and (via Airflow's own Backfill-uniqueness constraint,
    already source-confirmed this session in ROUND 2/3: a Backfill only completes once none of
    its DagRuns are RUNNING/QUEUED) would explain the AlreadyRunningBackfill cascade for the rest
    of this module's own tests too.

- timestamp: 2026-08-24 (ROUND 4 -- direct source read of the installed apache-airflow==3.3.0 on
    the live LOCAL scheduler pod, PLUS a live empirical reproduction, confirming the hypothesis
    above with direct evidence rather than inference alone, per this session's own established
    discipline)
  checked: >
    `kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- python -c "..."` against
    the LOCAL cluster to read the installed source of `SchedulerJobRunner.
    _executable_task_instances_to_queued` (selects which TaskInstances move SCHEDULED->QUEUED),
    `DagRun.get_queued_dag_runs_to_set_running` (selects which QUEUED DagRuns move to RUNNING,
    called by `_start_queued_dagruns`), and `DagRun.get_running_dag_runs_to_examine` (selects
    which DagRuns `_schedule_dag_run` -- the function enforcing `dagrun_timeout`, per ROUND 3's
    own source read -- actually considers). THEN a live empirical test on the LOCAL persistent
    cluster: NOT against `csv_ingest_customers` (which already carried unrelated stuck DagRuns
    from earlier work) but against the low-risk, single-task, test-only `smoke_kubernetes_pod`
    DAG (the same mechanism applies -- the filters found are dag-agnostic): paused it (confirmed
    via direct DB read: `is_paused=True`), created a real backfill for it (`airflow backfill
    create --dag-id smoke_kubernetes_pod --from-date <yesterday> --to-date <yesterday>
    --reprocess-behavior completed`), then polled the resulting DagRun/TaskInstance state
    directly against the metadata DB every ~10s for ~50s while the DAG remained paused, then
    unpaused and re-checked.
  found: >
    Source: `_executable_task_instances_to_queued` (scheduler_job_runner.py:524) includes
    `.where(~DM.is_paused)` when selecting TIs to queue -- no backfill-specific carve-out
    anywhere in the function. `get_queued_dag_runs_to_set_running` (models/dagrun.py) INNER JOINs
    on `DagModel.is_paused == false()` (and separately `is_stale == false()`) -- an inner join
    means a DagRun belonging to a paused DAG does not match this query AT ALL, so it can never
    transition QUEUED->RUNNING while paused, backfill- or schedule-created alike.
    `get_running_dag_runs_to_examine` (feeding `_schedule_dag_run`, which enforces
    `dagrun_timeout` per ROUND 3's own source read) only returns DagRuns ALREADY in RUNNING state
    -- a DagRun stuck in QUEUED is never even considered, meaning `dagrun_timeout` (ROUND 3's fix
    (7)) structurally CANNOT fire for a DagRun that never left QUEUED.
    Live empirical test: `airflow backfill create` for the paused `smoke_kubernetes_pod`
    SUCCEEDED immediately ("Created backfill Dag run", backfill_id=82) -- confirms backfill
    CREATION is not gated by `is_paused` (matches a direct source read of
    `airflow/models/backfill.py`: its only `is_paused` reference is an unrelated column on the
    `Backfill` model itself, never checked against `DagModel.is_paused` anywhere in
    `_create_backfill`). Immediately after creation: DagRun state `queued`, TaskInstance state
    `None`. ~50s later, DAG STILL paused: DagRun state STILL `queued`, TaskInstance state STILL
    `None`, `last_scheduling_decision: None` -- proving the scheduler NEVER examined this DagRun
    even once in that window, despite the scheduler pod itself confirmed actively looping the
    whole time (continuous Kubernetes-watch-cycle and orphan-reset activity in its own log).
    Unpausing did not immediately unstick it in this particular live test -- traced to a
    SEPARATE, LOCAL-cluster-specific staleness flag (`DagModel.is_stale=True` on all 3 checked
    DAGs, `last_parsed_time` ~12 hours stale, an artifact of this LOCAL dev cluster's own
    dag-processor not having freshly re-parsed recently -- unrelated to a fresh-every-run CI
    cluster where dag-processor is confirmed live-verified healthy with continuous clean parse
    cycles across ROUNDs 1-3) -- flagged as a separate, out-of-scope LOCAL hygiene observation,
    not chased further, and does not weaken the "stays stuck while paused" half of this test,
    which was directly, unambiguously observed with zero confounding factors.
  implication: >
    CONFIRMED, by direct source read AND live empirical reproduction: pausing a DAG in Airflow
    3.3.0 does not merely stop new regular-schedule DagRuns -- it freezes EVERY DagRun of that
    dag_id, backfill included, in `queued` state indefinitely, with zero TaskInstances ever
    reaching `scheduled`/`queued`/`running`. This mechanism is COMPLETELY INDEPENDENT of
    scheduler/dag-processor resource health (CPU, memory, restart count) -- explaining, with a
    specific confirmed mechanism rather than a coincidence, why THREE independently-real,
    correctly-targeted, live-verified restart-reducing fixes (ROUNDs 1-3) had ZERO effect on this
    specific 17-test signature: none touch DAG pause state, and `dagrun_timeout` structurally
    cannot reach a DagRun that never leaves QUEUED. `test_pilot_window_drains_without_cpu_
    starvation`'s own backfill -- created while `csv_ingest_customers` is ALREADY paused (the
    module-scoped autouse fixture runs before every test in the module, including the dry-run one
    before it) -- is frozen exactly this way on every live CI run: `discover_files` never gets a
    single TaskInstance queued, so `meta.files` never gets a row ("missing entirely", matching
    the orchestrator's own finding (a) precisely). Because a Backfill only completes once none of
    its DagRuns are RUNNING/QUEUED (`_mark_backfills_complete()`, already source-confirmed this
    session in ROUND 2/3), this single frozen DagRun then blocks every LATER test in the SAME
    module from creating its own backfill for `csv_ingest_customers` with `AlreadyRunningBackfill`
    -- explaining test_full_2year_sweep_customers_and_orders, test_idempotent_rerun_produces_
    zero_additional_rows, test_live_run_concurrent_with_backfill_same_dataset (its own backfill
    call also races the same stuck Backfill uniqueness constraint), test_mass_delete_snapshot_
    trips_circuit_breaker_without_mutating_gold_state, and test_scd_concurrent_attribute_change_
    and_correction_same_key -- all 5 of this module's remaining tests, all 5 in the failing list.
    Once this module's own fixture teardown finally unpauses `csv_ingest_customers` again (after
    its last test completes), the original frozen backfill (a 20-day, SCD-anomaly-laden window)
    finally starts draining for real, plausibly consuming the SAME shared
    `max_active_tis_per_dag=1` slot and real CI CPU/pod-scheduling capacity deep into the rest of
    the suite -- a plausible (not separately live-instrumented this round) explanation for why
    LATER FILES' own tests also fail, including the orchestrator's finding (b):
    test_smoke_dag_xcom_contains_built_sha's DagRun DOES reach 'running' (the DAG is genuinely
    unpaused again by the time this, the LAST slice file alphabetically, runs) but doesn't finish
    within 180s -- a materially different, downstream RESOURCE-CONTENTION signature consistent
    with a large recovering backfill still competing for constrained CI CPU/task-slot capacity,
    not a still-paused DAG. test_no_extra_schemas_exist (finding (c)) is unrelated to this
    mechanism entirely (a schema-existence assertion against `ALLOWED_SCHEMAS = {"pg_catalog",
    "information_schema", "public", "pg_toast"}`, which does not include `meta` -- `meta`
    legitimately exists by cluster-up time via Alembic migrations per CLAUDE.md's own
    architecture, an unrelated, pre-existing test/migration-ordering assumption mismatch already
    flagged out-of-scope by every prior round) -- kept out of scope, unchanged disposition.

- timestamp: 2026-08-24 (ROUND 4 -- fix implementation and offline verification)
  checked: >
    Repo-wide grep confirmed `_pause_customers_dag_for_backfill_only_tests`/
    `_live_concurrency_needs_dag_unpaused`/`_set_customers_dag_paused` (and the literal `airflow
    dags pause` invocation) are used NOWHERE else in the repository -- a narrow, single-file
    blast radius. Removed all three from `tests/e2e/slice/test_backfill_2year_sweep.py` (the
    fixtures, their 2 `usefixtures` call sites, and updated the 4 docstring/comment locations
    describing the now-removed behavior), replacing the removed code block with a documented
    explanation (source-read + live-empirical evidence, matching this session's own established
    in-code documentation density). `csv_ingest_customers` now simply stays unpaused for this
    module too, exactly as `conftest.py`'s own session-scoped `_unpause_slice_dags` fixture
    already guarantees for every other file in `tests/e2e/slice/`. No production DAG file
    touched.
    Verified offline: `python -m py_compile` clean; `ruff check` -- 0 new issues (2 pre-existing
    E501/W505 findings at line ~1038, confirmed identical via `git show HEAD:... | ruff check -`
    on the unmodified file, in code this fix never touched); `ruff format --check --diff` -- 86
    diff lines both before and after this fix (byte-identical, confirmed via `git show HEAD:...`
    on the unmodified file -- pre-existing project-wide formatting drift, none of it in code this
    fix touched); `mypy` -- 0 errors; `pytest --collect-only` -- all 7 tests in the module still
    collect cleanly in the identical order; repo-wide grep confirms zero remaining "pause"
    (non-"unpause") call sites anywhere in `tests/e2e/`; full offline policy suite (`pytest
    tests/policy/ -q -m "not manifests"`, 159 collectible) -- 157 passed, 2 failed, the SAME 2
    pre-existing, already-documented out-of-scope failures every prior round in this file has
    shown (test_dag_line_budget.py's 150-line budget, test_gates_actually_fail.py's lint
    meta-test) -- zero new regressions.
  found: >
    Clean offline verification across every gate this debug session has established as
    authoritative, with zero new regressions and a confirmed narrow, single-file blast radius.
  implication: >
    Offline-complete. This fix is purely test-suite-scoped (one file, zero production code, zero
    Helm/manifest changes) and purely ADDITIVE to ROUNDs 1-3's own fixes (does not touch
    scheduler/dag-processor resourcing, `core.parallelism`, `dagrun_timeout`, or any of the
    vault-0 fixes -- all remain in place per scope_guardrails). Only a live `cluster-slice-verify`
    run against real CI contention can confirm whether removing this DAG-pause mechanism actually
    resolves the 17-test failure signature (or the bulk of it) -- per this session's own
    established discipline, self-verification alone is not sufficient without direct live
    evidence.

- timestamp: 2026-08-24 (orchestrator, prior session, DATA supplied to ROUND 5 at charter time --
    recorded here verbatim per this file's own established discipline for evidence provided by
    another party, matching e.g. the "ROUND 2 continuation -- instrumented live run results" entry
    above; independently re-verified where checkable, see the next Evidence entry immediately below)
  checked: >
    Whether ROUNDs 1-4's fixes (Helm values changes, DAG file changes, test-fixture changes) were
    actually reaching the live CI cluster/tests, or whether a deployment-staleness gap could
    explain their lack of effect on the 17-test signature. Pulled the actual published image
    `ghcr.io/konutech/airflow:f0ebfe31bcb9db05895b67be5bcc4ce5bd79d7bc` (commit f0ebfe3, ROUND 4's
    HEAD) directly via `docker pull` + local inspection (`find / -name csv_ingest_customers.py`
    inside the image). Read `helm/values/ci/airflow.yaml`'s `dags.persistence`/`dags.gitSync`
    settings and their own code comments. Read `tests/e2e/slice/test_backfill_2year_sweep.py` and
    `tests/e2e/slice/conftest.py` on current repo HEAD directly via `grep`.
  found: >
    (1) `csv_ingest_customers.py` does NOT exist anywhere inside the published Airflow image --
    DAGs are NOT baked into the image at all. (2) `helm/values/ci/airflow.yaml` explicitly sets
    `dags.persistence.enabled: false` and `dags.gitSync.enabled: false`, with an existing code
    comment confirming this repo uses a hostPath DAG-delivery convention instead (both `ci` and
    `local` profiles) -- DAGs are mounted directly from the CI runner's own live git checkout into
    the kind cluster nodes. (3) Direct `grep` on current repo HEAD confirms
    `_pause_customers_dag_for_backfill_only_tests` is genuinely gone from
    `test_backfill_2year_sweep.py`, replaced by a session-scoped autouse `_unpause_slice_dags`
    fixture in `tests/e2e/slice/conftest.py` documented as keeping `csv_ingest_customers`
    "permanently unpaused for the WHOLE tests/e2e/slice/ session."
  implication: >
    "The fix isn't reaching the cluster/tests" is RULED OUT as an explanation for ALL FOUR rounds'
    lack of measurable effect on the 17-test signature, not just some of them -- DAG file changes
    (ROUND 3), Helm values changes (ROUNDs 1-2), and test-fixture changes (ROUND 4) are ALL
    structurally guaranteed live/current by this repo's hostPath convention, with no image, no
    sync lag, no possible staleness. ROUND 4's own specific hypothesis (DAG-pause causing a
    scheduler livelock) is therefore DISCONFIRMED, not merely unconfirmed -- its fix is proven
    present and active, yet produced a byte-for-byte-identical 6th consecutive failure run (see
    next entry). This is a genuine strategic reset for ROUND 5, not another partial-mitigation
    cycle in the same vein as ROUNDs 1-4.

- timestamp: 2026-08-25 (ROUND 5 -- Phase 0 reconciliation, independent direct re-verification of
    the orchestrator's summary above, per this round's own explicit charter instruction to verify
    rather than take the orchestrator's summary as a substitute for direct evidence)
  checked: >
    `ps aux` for any stale local `gh run watch`/background poller processes (none found -- consistent
    with the run already being terminal, though confirmed independently below rather than inferred
    from this absence alone). `gh run view 32779160265 --json status,conclusion,createdAt,
    updatedAt,headSha,workflowName` directly. `gh run view 32779160265 --json jobs` for the job ID.
    `gh api repos/KonuTech/airflow-platform/actions/jobs/97597115875/logs` (4337 lines, fetched
    fresh this round, not reused from any prior session's cache) for the pytest summary line, the
    full failing-test list (test node-IDs + assertion text, not just the count), and the
    cp-monitor.csv restart-count/peak-memory summary + final `kubectl describe pod` snapshot.
  found: >
    Run 32779160265 (job 97597115875, "Full local E2E suite + rebuild-from-raw capstone"):
    `status: completed`, `conclusion: failure`, `headSha:
    f0ebfe31bcb9db05895b67be5bcc4ce5bd79d7bc` (byte-identical to fix (8)'s own commit -- NOT a
    different/stale commit), `createdAt: 2026-08-24T21:22:40Z`, `updatedAt: 2026-08-24T22:31:08Z`
    (~68min total). Step 13 ("Run cluster + slice E2E suite") itself: `conclusion: failure`.
    Pytest's own summary line: "17 failed, 21 passed, 6 skipped, 16 warnings in 3659.44s (1:00:59)"
    -- IDENTICAL counts to every prior run on this session's commit lineage (a SIXTH consecutive
    occurrence). Extracted and compared the full failing-test list (18 FAILED lines, including
    their assertion text) against ROUND 4's own pre-fix(8) 17-test list (recorded verbatim in this
    file's Evidence "ROUND 4 -- orchestrator-supplied live verification of ROUND 3 fix (7)" entry)
    name-for-name: EXACT SAME 17 test node-IDs, in the same file/definition order, with the same
    error-message TEMPLATES (only UUID/timestamp/customer_id values differ, e.g.
    `e2e-backfill-a550159b8cbc-original.csv` vs a different-run UUID) -- zero tests turned green,
    zero new failures, zero different failure signature. cp-monitor.csv summary: scheduler
    `peak_mem_bytes=1608232960` (~1533MiB, ~99.7% of the 1536Mi limit), `peak_pids=32`, restart
    timeline shows exactly 2 restarts this run (`22:16:27Z restarts=1`, `22:22:21Z restarts=2`) --
    continuing ROUNDs 1-3's own monotonic restart decline (7->6->3->2) on a mechanism now doubly-
    confirmed orthogonal to this signature. Final `kubectl describe pod -l component=scheduler`:
    `Last State: Terminated / Reason: OOMKilled / Exit Code: 137`, `Restart Count: 2` -- the OOM
    mechanism itself remains real and unfixed as a residual (ROUND 2/3's own fixes reduced but did
    not eliminate it) but, per ROUND 4's own already-established evidence and this round's
    reconfirmation, is NOT the cause of the 17-test signature. dag-processor: `peak_mem_bytes=
    874315776` (~834MiB), 0 restarts throughout -- unaffected, consistent with every prior round.
  implication: >
    Direct, independently-gathered evidence (not inference from the orchestrator's summary) exactly
    matches what the orchestrator reported: ROUND 4's fix (8) is LIVE-VERIFIED INSUFFICIENT --
    proven present/active on the exact commit that produced this run, yet the persistent 17-test
    signature this session was chartered to resolve recurred completely unchanged for a sixth
    consecutive time. This closes ROUND 4's own falsification_test decisively in the REFUTED/
    insufficient direction (the reasoning_checkpoint's own falsification_test text: "If... a fresh
    live cluster-slice-verify run still shows the EXACT SAME 17-test failure set... the hypothesis
    is refuted or this fix is insufficient by itself" -- exactly what happened). ROUND 4's own
    mechanism (DAG-pause freezing backfill DagRuns) was REAL and worth fixing (a second,
    genuinely-existing landmine the repo-wide grep this session performed would have caught if it
    recurred), but it was never the dominant/proximate driver of THIS specific invariant signature.
    Opens ROUND 5 with a clean, confirmed slate: 4 substantively different, independently real,
    live-verified mechanisms (CPU/thresholds, memory/parallelism, livelock-timeout, DAG-pause) have
    now been tried and each shown NOT to explain the 17-test signature -- the actual root cause
    requires the strategic reset and direct-observation-first approach the orchestrator's own task
    guidance specifies for this round (see Current Focus, ROUND 5, above).

- timestamp: 2026-08-25 (ROUND 5 -- source-level mechanism investigation, direct read of the
    installed apache-airflow==3.3.0 on the LOCAL cluster's live scheduler pod, per this round's own
    established discipline of verifying mechanisms against the actual installed source rather than
    generic/remembered Airflow behavior)
  checked: >
    `kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- python -c "..."` (LOCAL,
    Deployment/KubernetesExecutor profile -- used ONLY for reading installed Airflow source, not for
    anything CI-specific) against `SchedulerJobRunner._start_queued_dagruns`,
    `SchedulerJobRunner._set_exceeds_max_active_runs`, `SchedulerJobRunner.
    _executable_task_instances_to_queued`, and `airflow.models.backfill._create_backfill`.
  found: >
    `_start_queued_dagruns` computes `active_runs_of_dags = Counter({(dag_id, backfill_id): num
    ...})` -- GROUPED BY `(DagRun.dag_id, DagRun.backfill_id)`, from a query `WHERE DagRun.state ==
    RUNNING GROUP BY DagRun.dag_id, DagRun.backfill_id`. For each candidate queued DagRun: if
    `backfill_id is not None`, checked against `backfill.max_active_runs` using ONLY that
    backfill_id's own running count; `elif dag_run.max_active_runs` (backfill_id is None), checked
    against the DAG's own `max_active_runs` using ONLY the backfill_id=None running count. These are
    two INDEPENDENT counters. `_create_backfill` confirms the Backfill row carries its OWN
    `max_active_runs` field (from the CLI's `--max-active-runs` flag), never compared against or
    combined with `DagModel.max_active_runs`.
    `_executable_task_instances_to_queued` confirms the OPPOSITE for `max_active_tis_per_dag`:
    `concurrency_map.task_concurrency_map[(task_instance.dag_id, task_instance.task_id)]` -- keyed
    WITHOUT backfill_id, i.e. genuinely global across every DagRun of that dag_id regardless of
    trigger type, exactly matching the test suite's own docstring claim for THIS specific
    constraint (not for max_active_runs, which the docstring conflated).
  implication: >
    A regular schedule-created DagRun and a backfill-created DagRun of `csv_ingest_customers` CAN be
    simultaneously RUNNING -- refutes (before it was ever tested live) a hypothesis this round
    formed by analogy to ROUND 4's is_paused finding ("the regular DagRun's own max_active_runs=1
    slot permanently blocks the backfill's DagRun from ever leaving QUEUED"). Not recorded in
    Eliminated since it was never promoted to Current Focus's committed hypothesis or live-tested --
    refuted by direct source read during this round's own investigation phase, exactly the kind of
    self-correction the hypothesis_testing discipline calls for. The REAL shared bottleneck is at
    the TASK level (`max_active_tis_per_dag` on stage/dbt_build/publish, confirmed global) once
    BOTH DagRun types are actively RUNNING and racing for it -- a materially different mechanism
    shape than "one DagRun blocks another from ever starting," requiring direct observation (this
    round's new instrumentation) rather than further source-level inference to resolve.

- timestamp: 2026-08-25 (ROUND 5 -- installed sensor-timeout config, LOCAL cluster)
  checked: >
    `kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- python -c "..."`: `airflow.
    configuration.conf.get('sensors', 'default_timeout')` and
    `inspect.signature(BaseSensorOperator.__init__).parameters['timeout'].default`. Direct read of
    `airflow/dags/csv_ingest_customers.py`'s own `wait_for_files = S3KeySensor(...)` construction
    for an explicit `timeout=` kwarg.
  found: >
    `sensors.default_timeout` = 604800 (7 days). `BaseSensorOperator.__init__`'s own `timeout`
    parameter defaults to `None`, which falls back to that config value. `wait_for_files` in
    csv_ingest_customers.py sets `poke_interval=30`, `retries=2`, `retry_exponential_backoff=True`
    but NO `timeout=` override.
  implication: >
    A deferred `wait_for_files` task that genuinely finds nothing would poke for up to 7 days before
    its own sensor-level timeout fires -- practically irrelevant here since the corpus lands within
    minutes (see next entry), but confirms this task has NO independent self-bounding mechanism of
    its own; whatever bounds it in practice is either finding a match, or `dagrun_timeout=45min`
    (ROUND 3's fix) -- which per ROUND 4's own already-confirmed `get_running_dag_runs_to_examine`
    scope only reaches DagRuns already in RUNNING state, not ones stuck in QUEUED.

- timestamp: 2026-08-25 (ROUND 5 -- sweep_corpus fixture direct read, tests/e2e/slice/
    test_backfill_2year_sweep.py)
  checked: >
    `sweep_corpus` (module-scoped fixture, lines 407-456) and the module's own corpus-size
    constants (`_NUM_DAYS`, `_START_DATE`, `_MASTER_SEED`, `_ROWS_PER_DAY`).
  found: >
    `sweep_corpus` calls `s3_client('app').put_object(Bucket='raw', Key=f'customers/{filename}',
    Body=body)` in a plain `for` loop over ALL generated files, unconditionally, as fixture SETUP --
    runs before the module's first test (even the --dry-run one, per that test's own
    `sweep_corpus: _Corpus # noqa: ARG001 -- ensures the corpus is uploaded before this dry-run`
    parameter comment). `_NUM_DAYS = 20` (19 real customers files after 1 gap day),
    `_START_DATE = date(2024, 1, 1)` -- confirms `customers_20240101.csv` (test_pilot_window's own
    polled filename, `sweep_corpus.customers_manifest.filenames[0]`) is literally the corpus's own
    first chronological file, uploaded as part of this SAME bulk setup call, not a separately-timed
    upload.
  implication: >
    The raw bucket stops being empty within the first few minutes of `cluster-slice-verify`
    (tests/e2e/cluster's own quick checks run first, then this module's setup fires immediately),
    not late and not never. This directly informs what the new instrumentation should distinguish:
    NOT "did a file ever arrive" (it does, early and in bulk) but "what happened to the FIRST
    DagRun(s) whose wait_for_files sensor was already deferred, polling on an empty bucket, at the
    moment this bulk upload lands" -- does it correctly detect the new files and proceed, and if so,
    does ITS OWN subsequent discover/stage/dbt_build/publish fan-out (against ALL 19 corpus files
    per the "discover re-lists the WHOLE prefix every tick" already-documented behavior) consume the
    shared max_active_tis_per_dag=1 slot(s) for long enough, in a way ordered unfavorably enough
    (`-priority_weight, DR.logical_date, TI.map_index`), to explain a full 30-minute "missing
    entirely" result for the pilot backfill's own target file specifically. This is a genuinely new,
    not-yet-tested-by-any-prior-round mechanism shape -- direct DagRun/TaskInstance timeline
    evidence (this round's new instrumentation) is needed to confirm or refute it, not further
    inference.

- timestamp: 2026-08-25 (ROUND 5 -- triggerer visibility gap check, re-mining run 32779160265's
    already-fetched log rather than waiting for a new live run)
  checked: >
    Re-grepped the already-downloaded raw log for run 32779160265/job 97597115875 (fetched during
    this round's own Phase 0 reconciliation) for every "triggerer" mention: cp-monitor-allpods.log's
    tail (last ~400 lines / ~12-15min of the 61min run), the kubectl get events dump, and the
    kubectl describe node "Non-terminated Pods" resource table.
  found: >
    `airflow-triggerer-0` shows `2/2 Ready, restarts=0,0, phase=Running` in EVERY sample of the
    visible allpods.log tail (consistent, no variation). `kubectl get events` shows zero events
    mentioning triggerer at all (only api-server and scheduler events appear). `describe node`'s
    resource table shows triggerer's declared requests/limits (120m/600m cpu, 320Mi/640Mi memory --
    main container 100m/500m/256Mi/512Mi + logGroomerSidecar 20m/100m/64Mi/~128Mi, matching
    helm/values/ci/airflow.yaml's `triggerer:` block exactly, NEVER touched by any fix this
    session) but not restart count (that table doesn't carry it). No prior round's diagnostic step
    has EVER queried `-l component=triggerer` specifically (confirmed via re-reading every
    diagnostic step in e2e-full.yml/e2e-chaos.yml this session has touched).
  implication: >
    Restart counts are cumulative for a pod's lifetime (never reset except by pod recreation, and
    triggerer's pod age ~64m matches the run's own overall length with no recreation evidence) --
    "0,0" holding steady across the entire visible tail is consistent with triggerer having had ZERO
    restarts for the pod's WHOLE life this run, not just its final 12-15 minutes. This weakens (does
    not eliminate) a triggerer crash/OOM hypothesis, but leaves a real gap: K8s events have a ~1h
    default TTL, and this run's diagnostic snapshot was taken at ~68min elapsed, meaning any event
    from the run's FIRST ~8 minutes could already have expired -- and more importantly, cp-monitor's
    own OWN per-role cgroup memory/pids time series (the thing that would show CPU-throttling-without
    -OOM, a softer degradation mode events wouldn't capture at all) has never included triggerer
    until this round's own fix to cp-monitor.sh's role loop (see next_action). No retroactive way to
    get this data from an already-completed run -- requires the new live run.

- timestamp: 2026-08-25 (ROUND 5 -- new instrumentation, offline verification)
  checked: >
    Edited .github/workflows/e2e-full.yml: (1) cp-monitor.sh's `for role in scheduler
    dag-processor` extended to `... dag-processor triggerer`; (2) 5 new diagnostic blocks appended
    to the existing "DEBUG: dump control-plane resource monitor + final diagnostics" (if: always())
    step (DagRun history, key-task TaskInstance history, scheduler log grep, triggerer status+log,
    MinIO listing via in-pod `mc`) -- full detail in Current Focus next_action above. Validated:
    `python3 -c "yaml.safe_load(open('.github/workflows/e2e-full.yml'))"` (parses cleanly);
    extracted both edited `run:` blocks verbatim and ran `bash -n` on each (clean); extracted both
    embedded python heredoc bodies and ran `python3 -m py_compile` on each (clean, confirming the
    YAML block-scalar indentation stripping left correct relative Python indentation intact); cross-
    checked every new resource reference against the ACTUAL rendered CI manifest
    (build/manifests/ci/airflow.yaml, build/manifests/ci/minio.yaml) rather than assuming LOCAL's
    shape applies -- caught and fixed a real mismatch (see Current Focus's own "genuine platform
    landmine" note: CI's scheduler is `kind: StatefulSet`, pod `airflow-scheduler-0`, NOT
    `deploy/airflow-scheduler` as LOCAL's KubernetesExecutor profile renders it); confirmed MinIO is
    `kind: Deployment` in CI too (`deploy/minio` correct) and `minio-root` secret name matches;
    confirmed triggerer is `kind: StatefulSet` in CI (matches LOCAL). Ran `tests/policy/
    test_no_manual_kubectl_surgery.py` (SCAN_DIRS is scripts/tools only, confirmed .github/workflows
    is out of its scope, 13/13 relevant policy tests pass) and the full `uv run pytest tests/policy/
    -q -m "not manifests"` (159 collectible): 157 passed, 2 failed -- the SAME 2 pre-existing,
    already-documented out-of-scope failures every prior round in this file has shown.
  found: >
    Clean offline verification across every gate available in this sandbox. The DagRun/TaskInstance
    query syntax was ADDITIONALLY validated by running the exact same heredoc-via-stdin commands
    live against the LOCAL cluster's own scheduler pod (not just syntax-checked) before writing them
    into the CI workflow -- both returned correct, well-formed output (3163 historical DagRuns and a
    sample of stage TaskInstance rows respectively) confirming the ORM query logic itself is correct
    against this exact installed Airflow version, not just that it parses.
  implication: >
    This round's new instrumentation is ready to commit and push with high confidence it will
    execute cleanly in CI and produce the intended direct evidence -- not merely "should work" but
    verified against the actual installed Airflow ORM and the actual rendered CI manifest shapes.
    Zero production code touched (workflow-only, throwaway diagnostic, matching this session's own
    established "never to be merged" precedent for this exact step). Ready for the live push-and-
    wait per Current Focus's own next_action.

- timestamp: 2026-08-25T06:49Z (ROUND 5 -- live run 32813826344/job 97698134909 completed, raw
    evidence extracted by the orchestrator directly from the job log, recorded verbatim here before
    ROUND 6's own analysis)
  checked: >
    `gh api repos/KonuTech/airflow-platform/actions/jobs/97698134909/logs` -- pytest summary,
    csv_ingest_customers DagRun/TaskInstance history dump, scheduler log grep, MinIO raw/customers/
    listing.
  found: >
    17 failed, 21 passed, 6 skipped (7th consecutive identical signature). Only 4 total DagRuns
    existed all session (1 backfill_id=1 backfill + 1 scheduled, plus 2 more later). The FIRST
    scheduled DagRun ran 05:49:08->06:34:08 (exactly 45:00, confirming ROUND 3's dagrun_timeout
    fired correctly and forcibly skipped discover/publish/stage/upstream_failed dbt_build). That
    same DagRun's wait_for_files (S3KeySensor) succeeded in 22s (05:49:08->05:49:30) -- file
    DETECTION is fast, not the bottleneck; the problem is downstream, in discover/integrity_gate/
    publish actually processing what was detected. MinIO `raw/customers/` listing showed 19 files
    (customers_20240101.csv..customers_20240120.csv, the `sweep_corpus` fixture's full historical
    corpus) all landing with the IDENTICAL LastModified timestamp `2026-08-25 05:49:11 UTC`, right
    as/before the first DagRun's wait_for_files completes. Scheduler log grepped for concurrency
    messages: 29 "Not executing <TaskInstance...> since the task concurrency for this task has been
    reached" lines across discover/integrity_gate/publish/resolve_window/
    list_run_ids_pending_dbt_build/wait_for_files, across all 3 concurrently-existing DagRuns --
    integrity_gate showing map_index up to 18 (19 mapped instances) throttled by its own
    max_active_tis_per_dag=3.
  implication: >
    This was the ROUND 5 hypothesis-forming evidence (sweep_corpus's 19-file bulk upload creating an
    integrity_gate/discover backlog every later test gets queued behind). NOT yet examined by the
    orchestrator at hand-off: the triggerer log section, the full scheduler log window around
    05:49:11-05:50:xx, and -- the load-bearing question -- whether a SPECIFIC later-failing test's
    OWN uploaded file genuinely gets stuck queued behind this backlog, or fails for an unrelated
    reason. ROUND 6 (below) answers this with direct evidence and REFUTES the backlog framing as the
    proximate mechanism.

- timestamp: 2026-08-25 (ROUND 6 -- direct re-examination of run 32813826344/job 97698134909's own
    raw log, re-fetched via `gh api repos/KonuTech/airflow-platform/actions/jobs/97698134909/logs`
    and read directly rather than relying on the orchestrator's paraphrase; DagRun/TaskInstance
    history section, lines ~4523-4551 of the fetched log)
  checked: >
    The full "csv_ingest_customers key TaskInstance history (wait_for_files/discover/stage/
    dbt_build/publish)" dump -- all 20 rows (5 key tasks x 4 DagRuns), states/tries/start/end.
  found: >
    In EVERY ONE of the 4 DagRuns (the original backfill, its immediate successor DagRun, and both
    scheduled runs), `discover` NEVER reached `state=success` -- final states were `skipped` (try=3,
    force-skipped by dagrun_timeout), `up_for_retry` (try=2), or `upstream_failed`. `stage` never
    even started in 3 of 4 DagRuns (`start=None,end=None`) because its own upstream (`discover`)
    never produced output to expand over. `dbt_build` was `upstream_failed` near-instantly in every
    DagRun (cascading from `stage`). `publish` also never reached `success` (skipped/up_for_retry) --
    it DOES get real attempts despite discover/stage never succeeding, because
    `wire_dbt_build_tracking`'s own `resolve_dbt_build_status`/`mark_dbt_build_done` bridge uses
    `trigger_rule="all_done"` (not `all_success`), so `publish`'s immediate upstream (`mark_done`)
    still reaches its own task-level `success` (a clean no-op write) even when everything further
    upstream failed -- a separate, minor wiring quirk, not the object of this round's fix.
  implication: >
    Zero evidence of a "successful discover, eventually" anywhere in this run. This directly informs
    the next check: WHY does discover never succeed -- stuck queued (confirms the sweep_corpus
    backlog hypothesis) or actively failing (refutes it)?

- timestamp: 2026-08-25 (ROUND 6 -- direct re-examination of the same log, "scheduler log grep"
    section, lines ~4551-5052; the grep pattern itself, read from .github/workflows/e2e-full.yml
    line 264, matches `csv_ingest_customers` AND any of
    `constraint|concurrency|max_active|AlreadyRunning|is_paused|starv|Backfill|wait_for_files|
    deferred|exceeds_max` -- meaning any log line whose structured fields include `run_id=backfill__
    ...` incidentally matches via the substring "Backfill", pulling in FULL task-execution
    transcripts for both backfill-type DagRuns, not just literal concurrency messages)
  checked: >
    Every line in this 502-line section; counted by task_id and searched for "denied the request".
  found: >
    SMOKING GUN: `discover` (try_number=2, at 2026-08-25 06:02:53) and `discover` (try_number=3, at
    ~06:20:1x), plus `publish` (try_number=2 at 06:04:15, try_number=3 at 06:21:46) -- ALL FOUR --
    show a clean, unambiguous Kubernetes API `ApiException: (400) Bad Request` when
    `KubernetesPodOperator` attempts `create_namespaced_pod`: `"admission webhook
    \"ivpol.validate.kyverno.svc-fail-finegrained-require-signed-images\" denied the request: Policy
    require-signed-images failed: container image failed cosign keyless signature verification
    against this repository's publish.yml OIDC identity, and is not on the pinned-upstream/local-dev
    exception list -- see kubernetes/kyverno-policy.yaml"`, `"code":400`. The denied image in every
    case: `ghcr.io/konutech/csv-processor:1c111c033f638327b8ed26ee1bf5317715cfd5d4` (this exact
    commit). This is a POLICY DENIAL (the CEL validation expression evaluated and returned false),
    NOT a webhook-call timeout (that would read "failed to call webhook: ... context deadline
    exceeded", a DIFFERENT, ALREADY-eliminated-as-unrelated mechanism from earlier this session --
    see the "Kyverno admission-webhook timeouts" Evidence entries above, all of which occurred during
    HELM INSTALL of Airflow control-plane Deployments/StatefulSets during CLUSTER BRING-UP, a
    completely different code path/timing window from a live KubernetesPodOperator pod-create call
    mid-DagRun). No prior round of this debug session ever grepped scheduler logs for actual
    KubernetesPodOperator pod-creation exceptions -- this is a genuinely new observation, surfaced
    only because ROUND 5's own grep pattern happened to sweep it in via the "Backfill" substring
    coincidence.
  implication: >
    DIRECT, unambiguous confirmation that `discover`/`publish` pod creation is being actively DENIED
    by Kyverno's `require-signed-images` policy for this session's own correctly-tagged,
    correctly-published ETL image -- not "queued behind a backlog" (a scheduling/concurrency
    explanation) but "attempted and rejected at the Kubernetes admission layer" (a policy-enforcement
    explanation). This is a different axis entirely from every one of ROUNDs 1-5's own hypotheses
    (scheduler/dag-processor CPU/memory, DAG-pause, dagrun_timeout, sweep_corpus/integrity_gate
    concurrency) -- none of which touch pod ADMISSION at all.

- timestamp: 2026-08-25 (ROUND 6 -- cross-check: was the image actually signed in time?)
  checked: >
    `gh run list --workflow=publish.yml --json databaseId,status,conclusion,headSha,createdAt
    --limit 10` and `gh run view 32813826173 --json jobs` (the publish.yml run for the SAME commit
    1c111c033f638327b8ed26ee1bf5317715cfd5d4, triggered by the identical push-to-main event as
    e2e-full.yml run 32813826344, per e2e-full.yml's own line 68-72 comment: "publish.yml (triggered
    by the same push-to-main event) tags every image with the full github.sha" -- confirmed via
    direct read of both workflow files' `on:` triggers that there is NO `needs:`/`workflow_run:`
    coupling between them at all; they are two fully independent, unsynchronized workflow runs).
  found: >
    publish.yml run 32813826173 (headSha 1c111c0..., createdAt 05:41:25Z) completed `success` at
    05:43:31Z. Its "Build, sign and scan csv-processor" matrix job individually: started 05:41:29Z,
    completed 05:42:48Z, `conclusion: success`, INCLUDING step 10 "cosign sign the published digest"
    (`conclusion: success`). This is ~7 minutes before discover's earliest plausible first attempt
    (wait_for_files succeeded 05:49:37-05:49:49 for this same backfill DagRun) and ~20 minutes before
    the CAPTURED try=2 denial at 06:02:53.
  implication: >
    REFUTES the naive "image not pushed/signed yet, simple cross-workflow race" explanation -- the
    image was fully built, pushed, AND cosign-signed several minutes before ANY discover attempt
    could plausibly have started, let alone the captured denied attempts 13-20+ minutes later. The
    denial mechanism must be something else: either the verification GENUINELY intermittently fails
    at admission time (not "never signed"), or something narrower. Directly informs the next check.

- timestamp: 2026-08-25 (ROUND 6 -- is the signature actually valid? Direct, independent
    re-verification, replicating Kyverno's exact check)
  checked: >
    Ran `cosign verify --certificate-identity-regexp
    '^https://github\.com/KonuTech/airflow-platform/\.github/workflows/publish\.yml@refs/(heads/main
    |pull/[0-9]+/merge)$' --certificate-oidc-issuer https://token.actions.githubusercontent.com
    ghcr.io/konutech/csv-processor:1c111c033f638327b8ed26ee1bf5317715cfd5d4` locally (unauthenticated
    against GHCR -- confirmed no ghcr.io credential configured in this environment, `~/.docker/
    config.json` only has a Docker Hub entry -- i.e. the SAME anonymous-access path Kyverno's own
    admission controller would use), using the EXACT `certificate-identity-regexp`/
    `certificate-oidc-issuer` pair read directly from `kubernetes/kyverno-policy.yaml`'s own
    `attestors.cosign.cosign.keyless.identities` block.
  found: >
    Verification SUCCEEDS cleanly: "The following checks were performed... cosign claims were
    validated... Existence of the claims in the transparency log was verified offline... code-signing
    certificate was verified using trusted CA certificates", returning a single valid signature
    payload bound to digest `sha256:254ec949f901784112c468e94fad452884a25c0512ec2ebfbc40339852e4e646`.
  implication: >
    The signature is GENUINELY, CURRENTLY valid -- this is not a broken/never-completed signing
    pipeline. Combined with the prior entry, this strongly points to a TRANSIENT verification
    failure at the specific moments Kyverno's admission controller attempted it inside the live CI
    cluster (06:02:53, ~06:20:1x), not a permanent defect in the image or its signature.

- timestamp: 2026-08-25 (ROUND 6 -- did Kyverno's verification mechanism work AT ALL in this same
    run, for this same commit? Cross-check against the triggerer pod's own admission record, already
    captured in this run's dump, per this round's own task charter to check this section before
    deciding whether a new live run is needed)
  checked: >
    "ROUND 5: triggerer pod status" section of the same log (`kubectl describe pod -l
    component=triggerer`) -- `airflow-triggerer-0`'s own pod annotations and container images.
  found: >
    `airflow-triggerer-0` (image `ghcr.io/konutech/airflow:1c111c033f638327b8ed26ee1bf5317715cfd5d4`
    -- the SAME commit, a DIFFERENT image built by the SAME publish.yml run) carries the annotation
    `kyverno.io/image-verification-outcomes:
    {"require-signed-images":{"name":"require-signed-images","ruleType":"ImageVerify","message":
    "success","status":"pass"}}`. Pod `Start Time: Tue, 25 Aug 2026 05:45:30 +0000` -- i.e. Kyverno
    successfully verified and ADMITTED this pod only ~3 minutes after the airflow image's own
    cosign-sign step completed (05:43:31Z per the publish.yml cross-check above), well BEFORE the
    later, heavier-load window (06:02:53+) when discover/publish's own attempts were denied. Every
    Airflow control-plane pod (api-server/scheduler/dag-processor/triggerer, all sharing this image)
    came up cleanly with 0 restarts this run, confirming their own admissions all passed too.
  implication: >
    Kyverno's verification mechanism, this cluster's network path to GHCR/Sigstore, and the signing
    pipeline are ALL demonstrably working correctly -- confirmed by a PASS for the same commit's
    other image, early in the run. The denials are therefore neither "Kyverno is broken" nor "this
    commit's images are unsigned" -- they correlate with WHEN the verification was attempted: early
    (05:45, lighter load, before the full test suite + multiple concurrent DagRuns are hammering the
    single-node cluster) succeeds; later (06:02+, heavier load, multiple concurrent DagRuns + the
    full pytest suite running) fails. This timing correlation points at load-dependent verification
    reliability, not a structural signature/pipeline defect.

- timestamp: 2026-08-25 (ROUND 6 -- the load-bearing check per this round's own charter: does a
    SPECIFIC later-failing test's OWN uploaded file get stuck queued behind the sweep_corpus backlog,
    or fail for an unrelated reason? Full pytest failure-list re-read, "short test summary info"
    section of the same log)
  checked: >
    All 17 FAILED lines and their full assertion text/error messages (not just the 6 already
    attributed to test_backfill_2year_sweep.py in ROUND 4's own analysis).
  found: >
    Of the 17: 1 is the already-out-of-scope `test_no_extra_schemas_exist`. 6 are
    `test_backfill_2year_sweep.py`'s own tests (1 "missing entirely" for `customers_20240101.csv`
    [a sweep_corpus file] + 3 `AlreadyRunningBackfill` cascades + 2 downstream-precondition cascades
    of those). 1 (`test_orphan_order_quarantined_while_valid_rows_publish`) is the already-documented
    SEVENTH finding (`normalized.customers` empty because nothing ever published all session). 1
    (`test_smoke_dag_xcom_contains_built_sha`) is a DIFFERENT DAG (`smoke_kubernetes_pod`) not yet
    reaching terminal state. The remaining 8 -- `test_backfill_reentry.py::
    test_backfill_resolves_previously_rejected_row` (own file `e2e-backfill-53fa71a131e1-
    original.csv`), `test_concurrent_select.py::test_concurrent_select_never_observes_partial_publish`
    (`e2e-concurrent-select-00f0f1a2262a.csv`), `test_dbt_silver_pipeline.py::
    test_fresh_customers_file_flows_through_stage_dbt_build_publish` (`e2e-dbt-silver-f70d23087f93.
    csv`), `test_pod_kill_retry.py::test_pod_kill_mid_load_produces_no_duplicates`
    (`e2e-podkill-b3a7cf4b54c1.csv`), `test_pod_kill_retry.py::
    test_pod_kill_mid_dbt_build_produces_no_duplicates` (`e2e-dbtkill-e1b388335395.csv`),
    `test_pod_kill_retry.py::test_u3_throughput_and_peak_rss_baseline` (`e2e-u3-b183b4cbe283.csv`),
    `test_rebuild_from_raw.py::test_rebuild_from_raw_reconciles_and_reverts_quarantine_to_pending`
    (`e2e-rebuild-bd158e51d186-original.csv`), `test_smoke_and_idempotency.py::
    test_idempotent_reupload` (`e2e-idempotent-dee298488191-1.csv`) -- are EIGHT INDEPENDENT tests,
    in SIX DIFFERENT test modules, each uploading its OWN uniquely-named single file (NONE are part
    of sweep_corpus's 19-file corpus), spread across what pytest's own real collection order (file-
    alphabetical, per ROUND 5's own already-confirmed A1 finding) places at DIFFERENT points across
    the ~62-minute run, including modules that sort well after test_backfill_2year_sweep.py
    alphabetically (test_pod_kill_retry.py, test_rebuild_from_raw.py, test_smoke_and_idempotency.py
    -- i.e. LATE in the run, long after any one-time startup backlog should have drained) -- and
    EVERY ONE of them hits the byte-for-byte identical `meta.files has no row for dataset='customers'
    object_uri=... within 180s -- discovery never registered it` signature.
  implication: >
    DIRECT REFUTATION of the sweep_corpus/integrity_gate-backlog hypothesis as the proximate
    mechanism for the FULL failure signature. A one-time startup backlog (19 files landing at once,
    fanning out to 19 integrity_gate instances at max_active_tis_per_dag=3) would be expected to
    drain within some bounded window, after which LATER, independent, single-file uploads should
    succeed normally -- but 8 independent files, spread across the entire run including tests that
    run late, ALL fail identically. This is far better explained by a PERSISTENT, ONGOING blocking
    mechanism active throughout the whole run -- exactly what the directly-observed Kyverno pod-
    admission denials are: `discover`'s pod creation can be denied EVERY time it is attempted for
    ANY DagRun processing ANY file, for as long as the cluster stays under the load conditions that
    make verification unreliable, independent of which file or which DagRun. This directly answers
    this round's own charter question: the traced later-failing tests' own files do NOT get stuck
    queued behind the sweep_corpus backlog (that would predict LATE-running tests recovering once the
    backlog drains) -- they fail for the SAME unrelated (to sweep_corpus), previously-undiscovered
    reason discover's very first attempts also failed: Kyverno admission denial. The sweep_corpus
    burst likely DOES also create some real integrity_gate/discover scheduling pressure (ROUND 5's
    own 29 concurrency-message count is real), but it is not the dominant, proximate blocker of this
    session's invariant 17-test signature -- Kyverno pod-admission reliability is.

- timestamp: 2026-08-25 (ROUND 6 -- root-cause-level check: why would verification be unreliable
    under load? Direct inspection of Kyverno's own CI resource allocation and the CI CPU budget's
    remaining headroom)
  checked: >
    `helm/values/ci/kyverno.yaml` (current `admissionController.container.resources`) vs
    `helm/values/local/kyverno.yaml` (same container, LOCAL profile) vs `kubernetes/
    kyverno-policy.yaml`'s own header comment (Rule 3 fix, plan 11-06: "re-verifying
    ghcr.io/konutech/airflow's cosign signature against the real GHCR registry (a live network
    round-trip Kyverno's own ... webhook makes on every admission, cache or not) consistently took
    15-20s under this session's ambient cluster load... confirmed via kyverno-admission-controller's
    own logs: repeated 'verifying cosign image signature' TRC lines followed by 'Get
    \"https://ghcr.io/v2/\": context canceled' and 'write: broken pipe' at exactly the 10s mark" --
    i.e. this EXACT component was ALREADY documented, in this repository's own commit history, as
    running close to its time budget under load, well before this debug session began). Also ran
    `make manifests` (fresh render, confirmed current/non-stale against `helm/values/ci/` which has
    zero uncommitted changes) then computed `request_totals()` (the same function
    `test_ci_profile_fits_runner` uses) directly against the rendered CI manifests.
  found: >
    CI's `admissionController.container.resources`: requests cpu=50m/memory=64Mi, LIMITS
    cpu=200m/memory=192Mi. LOCAL's same container: requests cpu=100m/memory=128Mi, LIMITS
    cpu=500m/memory=384Mi (2.5x CI's CPU limit, 2x CI's memory limit) -- LOCAL was already
    provisioned more generously for this exact container. Current CI CPU request total (freshly
    rendered, confirmed non-stale): 3.180 cores against a 3.200-core effective budget -- only 0.020
    cores (20m) of REQUEST headroom remain, confirmed via direct computation, not estimation.
    Memory has substantial headroom (6504Mi used of a 13107Mi budget, ~6.6GB free).
    `test_manifest_resources.py::request_totals()` sums `resources.requests` ONLY (confirmed via
    direct source read) -- LIMITS are never summed into the CI budget gate at all, and
    `unsized_containers()` only checks presence, never limit VALUES.
  implication: >
    The admission controller's own CPU LIMIT (200m, i.e. 1/5 of one core) is the tightest resource
    ceiling of any Kyverno component in either profile, on a container that must perform live
    cryptographic signature verification PLUS multiple external network round-trips (GHCR manifest
    fetch, Sigstore Rekor/Fulcio lookups) per pod admission, with NO caching (per the policy file's
    own header comment), while sharing this same CPU-thin node with everything else this whole debug
    session has already proven becomes CPU-contended under real sustained load (ROUNDs 1-3's own
    scheduler/dag-processor OOM/restart findings). A 200m CPU cgroup limit means this container gets
    THROTTLED the moment its actual usage exceeds 0.2 cores even briefly -- directly consistent with
    the already-documented 15-20s verification latency (measured under LIGHTER load than
    cluster-slice-verify's own multi-DagRun, full-pytest-suite window) risking the CEL verification
    call itself timing out/erroring internally (collapsing to "0 valid signatures", the SAME denial
    text as a genuinely-unsigned image) under HEAVIER load. Critically: the CPU REQUEST budget has
    only 20m of headroom left (cannot be raised without breaking `test_ci_profile_fits_runner`), but
    the LIMIT is not counted in that budget sum at all -- raising ONLY the limit (burst headroom, not
    the guaranteed reservation) is a zero-budget-risk lever precisely matching this debug session's
    own established pattern (ROUND 1 fix (1), ROUND 2/3 fix (3)/(6): raise LIMIT for burst capacity
    while leaving REQUEST, and therefore the budget gate, untouched).

- timestamp: 2026-08-25 (ROUND 7 -- fix design investigation, implementation, and offline
    verification)
  checked: >
    (1) sweep_corpus's actual upload fan-out (tests/e2e/slice/test_backfill_2year_sweep.py lines
    407-456): 19 customers + 19 orders files PUT in one tight loop at module setup -- the upload
    itself is trivial (38 small PUTs, sub-second, confirmed by ROUND 5's identical-LastModified
    listing); the LOAD is the platform's REACTION (per-DagRun integrity_gate fan-out over all 19
    keys as fork+full-import LocalExecutor task processes inside the scheduler pod, plus stage
    mapped-TI creation). Time-based upload staggering was REJECTED (violates this repo's own
    "sleep-in-e2e-tests" discipline and only delays an identical steady state -- discover is
    bucket-wide/date-agnostic, so all later DagRuns see all files regardless of upload timing);
    terminal-status-gated upload batching was REJECTED (deadlocks the fixture if processing is
    broken -- the exact bug under investigation). (2) The concurrency-knob inventory against the
    INSTALLED apache-airflow==3.3.0 source: `max_active_tasks` is enforced PER (dag_id, run_id)
    -- `_executable_task_instances_to_queued`'s critical-section query joins a
    `task_per_dr_count` subquery on (dag_id, run_id) and row_numbers partitioned by [dag_id,
    run_id]; `concurrency_map.dag_run_active_tasks_map[(dag_id, run_id)]` in the per-TI loop --
    so it CANNOT bound the scheduled+backfill aggregate; the only truly global TI cap is
    `core.parallelism` (executor slots; LocalExecutor pre-forks exactly that many workers).
    DEFERRED TIs hold no slot (EXECUTION_STATES only). (3) `discovery.py`: listing is sorted
    (line 881) before the `max_units_per_run` cap (lines 981-989), so batch membership is
    deterministic and `customers_20240101.csv` (the pilot's polled file) lands in batch 1.
    Configs are baked into the csv-processor image (docker/csv-processor/Dockerfile COPY
    configs/), rebuilt+signed by publish.yml on every push -- a configs/ change reaches CI with
    zero staleness. The idempotency key includes config_hash (discovery.py module docstring), so
    the config change rotates keys once -- by design, irrelevant on CI's fresh clusters.
    (4) Integration/unit tests construct their own BatchingConfig objects (never load the real
    YAML) -- no test pins max_units_per_run=100.
  found: >
    FIX IMPLEMENTED (three levers, none touching timeouts/CI-job-splitting/rounds-1-6 fixes):
    (a) helm/values/{ci,local}/airflow.yaml `config.core.parallelism` "16"->"8" (D-06 behavioral
    non-divergence, both profiles identical -- values-profiles policy gate confirmed passing);
    (b) airflow/dags/csv_ingest_{customers,orders}.py `@dag(..., max_active_tasks=6)` --
    documented in-code as PER-DAGRUN (honest semantics), a flood guard so one run's fan-out
    cannot monopolize the 8 global slots; (c) configs/datasets/{customers,orders}.yaml
    `batching.max_units_per_run` 100->10 -- the platform's own batching engine throttles the
    19-file corpus into <=10-file claims per DagRun (drains in 2 claims; orders' asset-cascade
    math still covers its 19 files across the >=2 customers publishes the drain now produces).
    OFFLINE VERIFICATION ALL GREEN: `make manifests` + kubeconform (540 resources, 378 valid, 0
    invalid); tests/unit 554 passed (incl. test_dag_structure's gate-cap and graph assertions);
    tests/dagtest 14 passed; tests/policy full suite -> ONLY the 2 pre-existing out-of-scope
    failures remain (test_dag_line_budget::customers -- already failing at HEAD, 201>155, now
    208>158 with the mirrored comment, still tracked separately; test_gates_actually_fail lint
    -- migration 0038's pre-existing D301, commit e0972e9, predates this session's rounds).
    test_dag_line_budget's orders budget bumped <=155 -> <=158 following that test's own
    documented exact-lines-needed precedent (orders sat at a zero-headroom 155; the 2-comment +
    1-kwarg addition needed exactly 3 lines). test_manifest_resources CI budget gate passes
    (zero request changes -- parallelism/max_active_tasks/max_units_per_run are all
    non-resource knobs). ruff clean on every touched file.
  implication: >
    ROUND 7's reduced-concurrency regime is implemented and offline-verified. Peak-load
    arithmetic post-fix: at most 8 concurrent TIs platform-wide (was 16), at most 6 per DagRun,
    stage fan-out per run 19->10 mapped TIs, LocalExecutor pre-fork pool halved again. Live
    verification (single `gh run watch`) is the real test per this file's own discipline;
    verifiable-reduction indicators to grep in the new run's log: 'max_active_tasks limit of 6'
    scheduler lines, 'discovery.units_capped' (cap=10) in discover pod logs, and the DagRun/TI
    history dump's per-run TI census -- required by the pre-registered falsification test
    before any signature comparison is meaningful.

- timestamp: 2026-08-25 (ROUND 7 -- post-run analysis, part 1: falsification-test execution
    against run 32834311083)
  checked: >
    `gh run view 32834311083 --json jobs,...` (conclusion failure; single job 97760563853 'Full
    local E2E suite + rebuild-from-raw capstone', 09:56:21Z-11:03:56Z) then `gh api
    repos/KonuTech/airflow-platform/actions/jobs/97760563853/logs` (5402 lines, saved to
    scratchpad). Executed the pre-registered checks in order: (1) regime-precondition greps
    ('max_active_tasks limit of 6', 'discovery.units_capped', parallelism evidence); (2) full
    failing-test node-ID extraction (grep 'FAILED tests/e2e/...' with a digit-safe character
    class -- an initial `[a-z_/]` class silently dropped all 6 test_backfill_2year_sweep.py
    entries because of the '2' in the filename; corrected and re-diffed) and exact diff against
    the invariant 17-test baseline recorded in this file; (3) 'denied the request' grep.
  found: >
    (1) REGIME: 'LocalExecutor(parallelism=8)' appears on every enqueue/finish scheduler line --
    lever (a), the self-declared load-bearing global cap, POSITIVELY in force. ZERO
    'max_active_tasks' lines of any kind -- lever (b) cannot be positively confirmed from logs
    (the message only appears when the cap BINDS; with a global ceiling of 8 across 2+ runs it
    plausibly never bound), but the kwarg is verifiably in the deployed DAG source (hostPath
    mount, zero staleness per the prior deployment-staleness investigation). ZERO
    'units_capped' lines -- lever (c) unobservable because NO discover pod ever ran (see (3));
    integrity_gate fan-out still reached map_index=19 (20 mapped TIs) but integrity_gate maps
    over the bucket listing, not the claimed batch, so this does not falsify lever (c) either.
    (2) SIGNATURE: pytest '17 failed, 21 passed, 6 skipped, 16 warnings in 3605.36s'; the 17
    node-IDs diff CLEAN against the baseline -- IDENTICAL name-for-name, the 9TH consecutive
    byte-identical signature. First-casualty assertion text also unchanged
    (test_pilot_window_drains_without_cpu_starvation: customers_20240101.csv 'missing
    entirely'). (3) KYVERNO: 18 'denied the request' lines (9 unique events, each mirrored in
    scheduler + task-runner views), ALL for discover/publish of
    backfill__2026-08-25T01:45:00+00:00, try_numbers 2-4; TaskInstance Finished lines show
    discover try 1-5 and publish try 2-5 ALL 'up_for_retry', NEVER success. DENY count moved UP
    (14 -> 18) despite verified halved parallelism. NOTE the captured scheduler-log section is
    the ROUND-5 grep slice (sweeps in backfill-run lines via the 'Backfill' substring), so both
    counts are sampling-biased lower bounds -- directionally, load reduction did not reduce
    denials.
  implication: >
    Per the pre-registered falsification test: the aggregate-load hypothesis is REFUTED. The
    load-bearing concurrency lever was verifiably in force, the signature is unchanged for a
    9th run, and the secondary indicator worsened. Blind spot (2) of the ROUND 7
    reasoning_checkpoint ('if the true Kyverno bottleneck is network-side... load reduction may
    be insufficient -- that outcome is exactly what the falsification test detects')
    materialized precisely.

- timestamp: 2026-08-25 (ROUND 7 -- post-run analysis, part 2: the DENY text itself names a NEW
    structural mechanism -- Docker Hub 429 on the KPO XCom sidecar image alpine:3.24.1)
  checked: >
    Full text of the 18 DENY lines (previous rounds only counted/attributed them); then
    followed the indirection: kubernetes/kyverno-policy.yaml lines 103-215 (matchImageReferences
    exception list, attestors, validations, failurePolicy), the installed provider's
    .venv/lib/python3.12/site-packages/airflow/providers/cncf/kubernetes/utils/xcom_sidecar.py,
    the provider dist-info version, and airflow/dags/_common/kpo.py.
  found: >
    Every DENY reads: 'Policy require-signed-images error: failed to evaluate policy: GET
    https://index.docker.io/v2/library/alpine/manifests/3.24.1: unexpected status code 429 Too
    Many Requests' -- an EVALUATION ERROR (Kyverno could not even fetch the manifest), distinct
    from ROUND 6's verification-failed message. This is HTTP 429 from Docker Hub's anonymous
    per-IP rate limit, notorious on GitHub-hosted runners' shared egress IPs. Why alpine at
    all: (a) _common/kpo.py line 136 sets "do_xcom_push": True for ALL 4 KPO tasks; (b) the
    constraints-pinned provider apache-airflow-providers-cncf-kubernetes==10.19.0 (verified
    dist-info) injects an XCom sidecar container with XCOM_SIDECAR_IMAGE = "alpine:3.24.1"
    (xcom_sidecar.py line 31) into every do_xcom_push KPO pod AT POD-CREATE TIME; (c) the
    Kyverno exception list pins 'alpine:3.17' -- NOT 3.24.1 -- and the policy file's own
    comment says the list was enumerated from 'make manifests' renders plus a live Vault pod
    query: a runtime-injected KPO sidecar image structurally CANNOT appear in any static
    manifest render, so it was never enumerated (follow-the-indirection: the list's producer
    and the admission-time consumer disagree); (d) an unexempted image requires cosign keyless
    verification against publish.yml's OIDC identity
    (subjectRegExp ^https://github.com/KonuTech/airflow-platform/...publish.yml@...$) --
    docker.io/library/alpine can NEVER satisfy this, so even without the 429 the pod is denied
    (this is very likely what ROUND 6 actually observed: its custom deny message does not name
    the failing image, and round 6 attributed csv-processor by inference); (e) failurePolicy:
    Fail + validationActions [Deny] make both failure modes (fetch-429 and verify-false) an
    identical hard pod-creation denial.
  implication: >
    STRUCTURAL root-cause candidate that explains every stubborn property of this signature:
    (1) 9-run invariance -- no scheduler/CPU/memory/concurrency/timeout/retry fix can make an
    unexempted, unsignable sidecar image admissible; (2) exactly the same 17 tests -- precisely
    those needing a KPO-bearing DagRun to reach SUCCEEDED; (3) ROUND 6's load-correlation
    (airflow image verified clean early, csv-processor pods denied mid-suite) -- the airflow
    control-plane pods carry NO XCom sidecar, and isolated early fetches can squeeze under the
    rate limit while suite-time admission bursts exhaust it; (4) retries can't help -- try 1-5
    all hit the same deny. Kyverno DENIALS are the proximate cause; the alpine:3.24.1
    sidecar-vs-exception-list mismatch is the root cause; Docker Hub 429 on GH runner IPs is an
    aggravating second leg that makes even a would-be-exempted fetch flaky in CI. OPEN
    QUESTION for fix verification: reconcile with LOCAL slice-suite behavior (if local
    currently passes with this same policy + provider, something differs -- policy version,
    stale local airflow image, or ref-normalization -- and the fix must be verified against
    whichever is true).

- timestamp: 2026-08-25 (ROUND 8 -- LOCAL reconciliation, ANSWERED with direct evidence)
  checked: >
    The ROUND 7 open question "why does the LOCAL slice suite pass (or appear to) with the same
    policy + provider?" -- investigated directly against the live LOCAL cluster
    (kind-airflow-platform, reachable) BEFORE touching the policy file: (a) live policy read via
    `kubectl get imagevalidatingpolicy require-signed-images -o jsonpath=...` (generation 3,
    created 2026-08-23T10:00:15Z); (b) provider + sidecar constant read from INSIDE the running
    LOCAL airflow image via `kubectl -n airflow exec deploy/airflow-scheduler -- python -c ...`;
    (c) Airflow metadata DB queried directly (`kubectl -n data exec airflow-db-1 -- psql`) for
    all discover/stage/dbt_build/publish TaskInstance states since policy creation; (d) pod ages
    via `kubectl get pods --sort-by=.metadata.creationTimestamp`; (e) a live SERVER-SIDE DRY-RUN
    admission probe (`kubectl apply --dry-run=server`) of a KPO-shaped mixed pod (base =
    localhost:5001/csv-processor:917e45c + sidecar alpine:3.24.1) against the etl namespace.
  found: >
    (1) The LOCAL airflow image (localhost:5001/airflow:cd0d2aa) carries the IDENTICAL provider
    (apache-airflow-providers-cncf-kubernetes 10.19.0) and IDENTICAL sidecar constant
    (alpine:3.24.1) as CI -- no provider/image-staleness difference exists. (2) The live LOCAL
    policy's exception list also lacks alpine:3.24.1 (it is stale only w.r.t. the two
    kindest/local-path-* entries commit caeeae4 added -- so 26-kyverno-policy.sh has not been
    re-run locally since 08-23 evening). (3) THE DECISIVE FINDING: the dry-run probe was DENIED
    with the BYTE-IDENTICAL CI DENY text INCLUDING the Docker Hub 429 leg ('Policy
    require-signed-images error: failed to evaluate policy: GET
    https://index.docker.io/v2/library/alpine/manifests/3.24.1: unexpected status code 429 Too
    Many Requests') -- the CI mechanism reproduces 1:1 on LOCAL, right now, on this host's own
    (also currently rate-limited) egress IP. (4) DB corroboration: LOCAL is NOT passing --
    csv_ingest_customers `stage` shows 22 consecutive `failed` TaskInstances on 2026-08-24
    08:38-08:48Z at the ~45s cadence the ROUND 6 retry_delay=30s fix produces under instant
    admission denial; nothing KPO-bearing has succeeded locally since 2026-08-23 17:31Z. (5) One
    residual anomaly, honestly recorded: 71 stage + 3 discover + 5 publish successes DID occur
    on LOCAL 2026-08-23 14:18-17:31Z, AFTER policy creation (10:00Z), with do_xcom_push=True
    already in kpo.py since 08-13/08-16 (git-verified) -- most plausibly a webhook-enforcement
    gap during that afternoon's Kyverno/control-plane churn (kyverno pods recreated ~12:00Z
    08-23, admission-controller has 6 restarts since; scheduler/api-server redeployed 16:21Z),
    but the exact window mechanism is NOT load-bearing for the fix and was not chased further.
  implication: >
    The open question's answer is: "LOCAL does NOT pass -- the impression it passes is stale."
    Neither ref-normalization nor a policy-version difference explains anything (bare
    'alpine:3.24.1' is what Kyverno matches, same as the pod spec, and the live local list has
    the same alpine gap as the committed file). The structural root cause (10) is now
    LIVE-REPRODUCED ON DEMAND on a second, independent cluster -- the strongest confirmation
    this session has produced for any hypothesis.

- timestamp: 2026-08-25 (ROUND 8 -- fix A applied + before/after LIVE falsification on LOCAL +
    offline verification battery)
  checked: >
    Implemented user-chosen direction A: added 'alpine:3.24.1' AND 'docker.io/library/
    alpine:3.24.1' (defensive dual form) to kubernetes/kyverno-policy.yaml's
    matchImageReferences exception list, plus a header paragraph documenting the
    runtime-injection indirection (so the next static-render re-enumeration cannot remove the
    entries), the provider-bump coupling, and the follow-up-B removal plan. 'alpine:3.17'
    KEPT -- verified NOT dead weight: it is the CNPG db-ping-test Job's container, present in
    all four build/manifests/{local,ci}/cnpg-*.yaml renders. Then: applied the updated policy
    to the LOCAL cluster (the exact `kubectl apply -f` shape 26-kyverno-policy.sh uses) and
    re-ran the SAME dry-run probe, plus a NEGATIVE control probe (unexempted alpine:3.19).
  found: >
    BEFORE fix: probe DENIED (429 text). AFTER fix (same cluster, same still-rate-limited
    network, ~2 minutes apart): probe ADMITTED ('pod/kyverno-probe-alpine-sidecar created
    (server dry run)') -- proving the exemption removes BOTH legs at once (an exempted image is
    excluded from images.containers entirely, so Kyverno never fetches its manifest at all --
    the 429 leg dies with the verification leg). NEGATIVE control: alpine:3.19 still DENIED --
    fail-closed enforcement intact, not weakened. Offline battery, all green: `make manifests`
    0 chart-lint failures, kubeconform -strict 540 resources / 0 invalid / 0 errors;
    `tests/policy -m "not manifests"`: 157 passed, 2 failed -- the SAME 2 pre-existing
    out-of-scope failures as every prior round (test_dag_line_budget.py customers budget,
    test_gates_actually_fail.py), zero new regressions; test_manifest_resources 5/5 (CPU budget
    untouched -- this fix changes no chart values); test_values_profiles 6/6; dagtest 14/14.
  implication: >
    Fix A is as verified as it can be without a CI run: live before/after falsification on an
    independent cluster exhibiting the identical denial, plus a negative control, plus the full
    offline battery. Incidental benefit: the LOCAL cluster's own since-08-24 KPO denial is now
    repaired (and local picked up the caeeae4 kindest entries it was missing). Remaining risk
    for the CI run is only environmental (anything ELSE broken on GH runners), not mechanistic.

- timestamp: 2026-08-25 (ROUND 8 -- post-run analysis of the authoritative live-verification
    run 32845181597, per the pre-registered decision tree)
  checked: >
    e2e-full.yml run 32845181597 (headSha ce73d9df, fix commit ce73d9d), job 97793152902 'Full
    local E2E suite + rebuild-from-raw capstone', conclusion failure; raw 5434-line log fetched
    via gh api .../jobs/97793152902/logs. Executed the 4 pre-registered steps: (1) exemption-in-
    force check; (2) pytest summary + name-for-name node-ID diff vs the invariant 17-test
    baseline; (3) Kyverno DENY grep; (4) plus the ROUND 5 diagnostics dumps (DagRun/TI DB state,
    scheduler-log concurrency greps, pod monitor CSV, kubectl describe) that this run captured.
  found: >
    (1) EXEMPTION IN FORCE: git show ce73d9d:kubernetes/kyverno-policy.yaml carries both
    'alpine:3.24.1' and 'docker.io/library/alpine:3.24.1'; job log 12:01:09-10Z shows
    26-kyverno-policy.sh applying the committed file -> 'imagevalidatingpolicy.../
    require-signed-images created'. (2) SIGNATURE: '17 failed, 21 passed, 6 skipped, 16
    warnings in 3509.10s (0:58:29)'; the 17 FAILED node-IDs are name-for-name IDENTICAL to the
    baseline -- 10TH consecutive byte-identical signature; failure templates unchanged
    ('meta.files has no row ... within 180s -- discovery never registered it' for 7 distinct
    per-test files; 'airflow backfill create ... failed after 3 attempts' x6). (3) DENY GREP:
    ZERO 'denied the request' occurrences in the ENTIRE log (rounds 6/7: 14-18); zero real 429s
    (all 23 grep hits are coincidental substrings in timestamps/container IDs/memory byte
    counts); the only 'signature' match is a workflow comment string. (4) DIAGNOSTICS -- the
    decisive new picture: (a) discover reached state=success try=1 for the FIRST TIME EVER on
    CI, twice, in 11-15s each (backfill__03:48: 12:56:26->12:56:41; scheduled__12:54:
    12:57:22->12:57:33) -- the admission-denial mechanism is definitively gone; (b) the FIRST
    two DagRuns (scheduled__12:06 start 12:07:24, backfill__03:47 start 12:07:47) WEDGED for
    47min and were failed at 12:54:44 (fix (7) dagrun_timeout doing its job): within them
    wait_for_files SUCCEEDED (12:07:45/12:07:53) but dbt_build was marked upstream_failed at
    12:07:51/12:07:59 -- SECONDS after wait_for_files success and inside the scheduler's first
    crash window -- while discover shows try=0/start=None (final state 'skipped', consistent
    with the dagrun_timeout handler's overwrite); (c) scheduler OOM crash-loop: 9 restarts at
    a strikingly regular ~6min cadence (first at 12:08:06 -- 40s after the DagRuns started;
    then 12:14, 12:19, 12:26, 12:33, 12:36, 12:42, 12:48, 12:54), kubectl describe: Last State
    Terminated/OOMKilled/Exit 137, restart-8's container alive only 38s (12:48:31->12:49:09);
    dag-processor 0 restarts (~559Mi peak), triggerer 0 restarts -- rounds 1-2 fixes still
    holding on their own mechanisms; (d) the replacement DagRuns (12:54:45) executed correctly
    but slowly: 269 'task concurrency for this task has been reached' scheduler-log messages
    (csv_ingest_customers.integrity_gate x149, .stage x120 -- ROUND 5's source-verified global
    max_active_tis_per_dag mechanism now VISIBLE live for the first time), and at suite end
    (13:05) stage map_index TIs still sat state=scheduled try=1 start=None.
  implication: >
    Decision-tree branch: signature unchanged despite exemption verifiably applied ->
    hypothesis (10)-as-signature-cause INSUFFICIENT. Root cause (10) was REAL and fix (11)
    STAYS (deterministic denial, now live-verified removed on BOTH clusters, first-ever CI
    discover successes prove it) -- but it was one of MULTIPLE sufficient causes stacked behind
    the same externally-identical 180s-timeout symptom, not the sole first domino. With
    admission repaired, the failure reverts to the scheduler-OOM class (3)/(3b)/(3c) that
    rounds 1-3 only partially treated: restarts had declined 7->6->3 across rounds but are now
    BACK UP to 9 -- plausibly because KPO pods now actually execute, changing the scheduler's
    work/memory profile (executor events, XCom sidecar handling, real task churn) versus 9
    prior runs where every KPO pod was denied at admission. Three concrete ROUND 9 targets, in
    causal order: (i) the UNEXPLAINED upstream_failed anomaly -- what marks dbt_build
    upstream_failed seconds after wait_for_files succeeds when discover never launched
    (suspects: scheduler crash mid-scheduling-loop leaving partially-committed TI state, or the
    (3c) orphan-reset path misclassifying); this wedge consumed 47 of 58 suite minutes and is
    when most of the 17 tests burned their 180s windows; (ii) the scheduler OOM cycle itself
    (~6min period against the 1536Mi ceiling); (iii) the global max_active_tis_per_dag=1
    throughput ceiling that makes even the healthy post-wedge pipeline unable to finish a file
    within a 180s test window under CI's task-start latencies. NOTE the signature did not even
    SHRINK because the 180s test timeouts fire identically regardless of WHICH upstream
    mechanism delays the pipeline -- the node-ID set is a saturated, low-resolution instrument
    and cannot distinguish these mechanisms; only the internal diagnostics can.

- timestamp: 2026-08-25 (ROUND 9, first-minute mining of round8-job.log + cross-round diff)
  checked: >
    The end-of-run DagRun/TI DB dump (round8-job.log lines 4536-4571) for ALL FOUR
    csv_ingest_customers DagRuns, cross-referenced against the SAME dump in rounds 5/6/7 logs
    (round5verify_97698134909.log:4531-4546, round6verify_97724031971.log:4525-4540,
    round7_job.log:4526-4531).
  found: >
    dbt_build reaches upstream_failed with try=0 in EVERY DagRun of EVERY diagnostic-instrumented
    run (rounds 5, 6, 7, 8 -- 14+ DagRuns total), always ~30-80s after that DagRun's own start --
    INCLUDING ROUND 8's two HEALTHY post-wedge replacement runs (backfill__03:48 stamped
    12:55:13.5, scheduled__12:54 stamped 12:55:19.1 -- ~28-34s after run start at 12:54:45, with
    a HEALTHY scheduler: restart 9 was 12:54:35 and NO further restarts through suite end 13:05)
    and BEFORE discover even ran in those runs (discover succeeded 12:56:26/12:57:22). Also:
    publish in both replacement runs STARTED and failed repeatedly (up_for_retry try=3, ~2min
    per attempt, 13:01-13:04) while stage map TIs were still scheduled/running.
  implication: >
    The upstream_failed stamps are NOT a scheduler-crash artifact -- both prime ROUND 8 suspects
    (OOM mid-scheduling-loop partial commit; orphan-reset misclassification) are WRONG for this
    leg: the stamp occurs identically under a healthy scheduler, deterministically, in every
    DagRun, load-independent. It must be a deterministic failure of dbt_build's upstream chain
    within each run's first ~30s.

- timestamp: 2026-08-25 (ROUND 9, source read: DAG wiring for dbt_build)
  checked: >
    airflow/dags/csv_ingest_customers.py + airflow/dags/_common/run_stage_recorder.py
    (wire_dbt_build_tracking) + _common/integrity_gate.py + _common/gap_recorder.py.
  found: >
    Graph: stage >> mark_dbt_build_running >> dbt_build >> resolve_dbt_build_status(all_done) >>
    mark_dbt_build_done(all_done) >> publish. list_run_ids_pending_dbt_build (feeding
    mark_dbt_build_running's run_ids XCom) is a ROOT task -- no upstream at all, Airflow default
    retries=0 -- that unconditionally resolves BaseHook.get_connection("analytics_db_default")
    at DagRun start. integrity_gate only resolves that conn in its REJECTION path (_reject_file);
    its happy path never touches it -- reconciling why gate/discover succeed while the dbt chain
    dies. gap_recorder no-ops unless an EMPTY backfill listing. publish's ONLY upstream is
    mark_dbt_build_done: when list_run_ids fails at t+~30s, all_success short-circuits
    mark_running and dbt_build to upstream_failed immediately, the two all_done tasks then
    complete (mark_done's run_ids XCom resolves None -> `if not run_ids: return` -> SUCCESS),
    and publish launches PREMATURELY -- before stage has even started -- fails (nothing staged),
    and burns its 6 retries as real 2-min KPO pods holding the GLOBAL max_active_tis_per_dag=1
    publish slot plus node CPU. This exact wiring defect is ALREADY DOCUMENTED as plan 11-09's
    'defect 1' (CRITICAL, Open) in .planning/phases/11-ci-cd-completion-operations/
    deferred-items.md -- there framed as firing only during injected DB/Vault fault windows.
  implication: >
    A single deterministic root-task failure explains the dbt_build stamps, the premature-publish
    churn, and (via never-running dbt) every 'normalized.customers has fewer than N rows'-class
    test precondition failure. Question reduces to: why does list_run_ids_pending_dbt_build fail
    on CI in EVERY run?

- timestamp: 2026-08-25 (ROUND 9, follow-the-indirection: analytics_db_default provisioning)
  checked: >
    tests/e2e/slice/test_backfill_2year_sweep.py module docstring (prior session's live
    diagnosis); scripts/vault-bootstrap.py (grep for connections provisioning);
    .github/workflows/e2e-full.yml (line 177: `make vault-bootstrap` on CI cluster-up);
    helm values (no AIRFLOW_CONN_* env), Makefile (only kubernetes_default is CLI-added);
    local cluster vault-0 state (0/1 sealed, 28h uptime -- not probed further).
  found: >
    A PRIOR session already root-caused this exact failure ON LOCAL and recorded it in the sweep
    test's own docstring: 'analytics_db_default was never provisioned in Vault' --
    list_run_ids_pending_dbt_build failed 22/22 historical attempts; root-token
    `vault kv list airflow/connections` showed minio_default as the ONLY entry; grep confirmed
    ZERO provisioning sites repo-wide. It was then 'Fixed live (Rule 3, infra-only, no repo file
    changed)' -- i.e. the secret was hand-written into the LOCAL cluster's Vault ONLY and the fix
    never became code. scripts/vault-bootstrap.py::_ensure_airflow_secrets provisions ONLY
    airflow/connections/minio_default (verified by direct read); CI's ephemeral cluster
    re-bootstraps Vault from this script on every run; no other seeding path exists (workflow/
    helm/Makefile all checked). Therefore on CI the Connection NEVER EXISTS, and the root task
    fails deterministically ~seconds into every DagRun, every run, forever.
  implication: >
    CONFIRMED ROOT CAUSE for the dbt_build/premature-publish leg: a follow-the-indirection gap --
    an ad-hoc live-cluster repair that never landed in the bootstrap code the ephemeral CI
    cluster is rebuilt from. Fix: provision airflow/connections/analytics_db_default in
    scripts/vault-bootstrap.py (read-guarded, from the same etl/analytics-db#dsn the bootstrap
    itself writes earlier in the same invocation -- same etl_app credential the prior session's
    hand-fix used). Offline round-trip VERIFIED against the installed stack:
    Connection(uri='postgresql://...').get_uri() -> 'postgres://...' -> psycopg
    conninfo_to_dict parses cleanly (conn_type normalized 'postgresql'->'postgres'; both schemes
    libpq-valid).

- timestamp: 2026-08-25 (ROUND 9, scheduler cgroup memory time-series extraction)
  checked: >
    Full scheduler memory/pids/restart-count series from ROUND 5's cgroup instrumentation in
    round8-job.log (17s sampling, 12:06-13:05), plotted per crash cycle.
  found: >
    Idle baseline is a STABLE ~360MiB with 17 pids (scheduler + 8 pre-forked LocalExecutor
    workers) for minutes at a time -- NO gradual leak. Every OOM is preceded by an ABRUPT spike
    within one or two 17s samples: 361->1331MiB (12:13:17->12:13:34), 361->1356 (12:19:20),
    411->1532 (12:25:25), 359->1442 (12:31:47), 752->1529 (12:37:17), 779->1511 (12:43:05),
    836->1533 (12:49:08) -- each spike coinciding with pids jumping 17 -> 24-25, i.e. ~7-8
    NEW task processes launched simultaneously (each imports the full Airflow tree, ~140-150MiB
    RSS). Spike cadence is ~5:45-6:20 apart. THE CRASH-LOOP ENDS PERMANENTLY the moment the two
    wedged DagRuns die at dagrun_timeout (12:54:44): from 12:55 to 13:05 memory oscillates
    726-1496MiB with pids 18-22 and ZERO further OOMs (concurrency never fills all 8 slots at
    once again).
  implication: >
    The scheduler OOM is a BURST-CONCURRENCY phenomenon, not a leak: peak ~= 360MiB baseline +
    N_simultaneous_task_processes x ~145MiB. At parallelism=8 the worst-case dispatch burst
    (~1.5-1.7GiB transient) exceeds the 1536Mi CI limit almost exactly -- the limit raised in
    ROUND 2 was sized against ROUND 2's observed peak under parallelism=16's DIFFERENT regime
    (denied KPO pods = short task lifetimes), not against 8 concurrent full task processes.
    First-minute connection: the initial burst at 12:07:47 (917MiB, pids 23) IS the first
    DagRuns' root-task fan-out (2 customers runs + orders runs each launching resolve_window/
    list_matched_keys/list_run_ids/record_gap/wait_for_files near-simultaneously). Subsequent
    ~6min-spaced bursts are re-synchronized dispatch waves (retry backoff + post-restart
    backlog); the wedged runs' tasks are repeatedly killed by the very bursts their dispatch
    participates in (self-resonant variant of ROUND 3's (3c) orphan-reset livelock), which is
    why the wedge only cleared when dagrun_timeout killed the runs and why the loop then
    stopped. Fix: raise the CI scheduler memory limit to cover the measured worst-case burst
    at parallelism=8 (limit-only change; requests untouched -> zero budget-gate cost),
    complementing (not replacing) the ROUND 7 parallelism=8 cap.

- timestamp: 2026-08-25 (ROUND 9 -- post-run analysis of live-verification run 32855002333,
    reconstructed by a resumed agent after the original analyst completed the analysis but was
    killed by an API interruption before persisting it; all findings below re-derived fresh
    from the surviving scratchpad log, not recalled)
  checked: "e2e-full.yml run 32855002333 (headSha ee87708) job 97824707600 raw log (fetched via
    gh api .../jobs/97824707600/logs, saved as scratchpad round9-job.log): (a) vault-bootstrap
    output; (b) grep census for dbt_build upstream_failed; (c) restart/OOM census (pod listings,
    kubectl describe Restart Count, OOMKilled/137 greps) + the per-role peak-memory summary from
    the ROUND 5 cgroup monitor CSV; (d) 'denied the request' grep; (e) failing-test node-ID diff
    (round9_failed.txt vs baseline_sorted.txt) + failure-template census; (f) the terminal
    exception inside the 'backfill create failed after 3 attempts' capture; (g) same-template
    grep against the retained ROUND 8 log for movement comparison; (h) companion publish.yml
    32855002320 conclusion via gh run view."
  found: "(a) 'secret airflow/connections/analytics_db_default: created' at 13:50:28Z -- fix (12)
    in force on a fresh CI Vault. (b) ZERO dbt_build upstream_failed stamps in the entire log --
    the wedge that appeared in EVERY DagRun of rounds 5-8 is GONE; a failing test's assert text
    proves a meta.run_stages DBT_BUILD row now exists (run_id=58), i.e. the dbt chain started on
    CI for the first time ever. (c) scheduler restarts 0 across the full 66min (was 9), zero
    OOMKilled/137; per-role peak census: scheduler peak_mem_bytes=1635192832 (~1559MiB) at
    peak_pids=24 -- ABOVE the old 1536Mi ceiling (1610612736 bytes), BELOW the new 2048Mi: the
    parallelism=8 burst recurred exactly as fix (13)'s model predicted and the new limit absorbed
    it (load-bearing, not unnecessary headroom). dag-processor peak 724MiB/7pids, triggerer
    404MiB/12pids, both 0 restarts. (d) Kyverno DENY count 0 -- fix (11) holding. (e) pytest '17
    failed, 21 passed, 6 skipped, 16 warnings in 3766.04s (1:02:46)'; node-ID set IDENTICAL to
    the baseline for the 11th consecutive run -- but the failure-TEMPLATE census shifted for the
    first time all session: 7x meta.files-no-row-within-180s (persisting), 3x backfill-create
    exit 1 (down from 6), 1x pilot file registered-but-PENDING at 1800s (baseline: never
    registered at all), 1x unexpected schema ['meta'], 1x DBT_BUILD run_stage never RUNNING, 2x
    SCD2 precondition asserts, 1x XCom git_sha '' mismatch, 1x normalized.customers <2 rows.
    (f) the backfill-create terminal exception is airflow.models.backfill.AlreadyRunningBackfill
    ('There can be only one running backfill per Dag') -- and (g) the SAME exception appears 12
    times in BOTH the round-8 and round-9 logs: a persisting backfill-overrun/serialization
    collision, not a new regression. (h) publish.yml 32855002320 SUCCESS -- image race clear."
  meaning: "Fixes (12) and (13) are LIVE-CONFIRMED per the pre-registered falsification test
    (all primary internal-diagnostic criteria passed); root causes (12)/(13) join (10) as
    verified-eliminated stacked causes. The residual blocker set is now nameable: the deferred
    option C throughput ceiling (global max_active_tis_per_dag=1 slots + ~3 allocatable CPUs --
    pipeline runs but cannot drain within per-test windows) as dominant, the
    AlreadyRunningBackfill overrun collision as a coupled second leg, and a tail of data-state
    asserts likely knock-ons of no complete prior ingest. Decision checkpoint returned to the
    user; option C's deferral condition ('until runs stop wedging') is now satisfied."

- timestamp: 2026-08-25 (ROUND 10 -- throughput-ceiling analysis, Hybrid charter priority 1)
  checked: >
    round9-job.log (surviving scratchpad artifact, run 32855002333) -- TI-history dump, DagRun
    history, node describe (allocated resources), MinIO corpus listing, scheduler concurrency
    census; airflow/dags/csv_ingest_customers.py (_STAGE_RESOURCES/_DISCOVER_RESOURCES, task
    graph); airflow/dags/_common/kpo.py (KPO defaults incl. its own documented
    startup_timeout_seconds=120 failure mode); kind/cluster-ci.yaml (allocatable derivation);
    tests/e2e/slice/test_backfill_2year_sweep.py (corpus generation parameters).
  found: >
    NEW ROOT CAUSE (14), arithmetic-decisive: THE STAGE (AND PUBLISH) POD IS STRUCTURALLY
    UNSCHEDULABLE ON THE CI NODE. Chain: (a) CI node allocatable CPU is exactly 3000m
    (cluster-ci.yaml's own derivation, matches node describe); steady-state requests with ZERO
    ETL task pods = 2780m (92%) across 23 platform pods (kube-system 950m, airflow 1080m
    incl. rounds-1/2's own deliberate scheduler/dag-processor raises, data 400m, kyverno 150m,
    vault 100m, ingress+cnpg 100m) -> FREE ~= 220m. (b) _STAGE_RESOURCES requests cpu=500m,
    memory=1Gi -- used by BOTH stage (TracingKPO .partial line 152-160) and publish (line
    185-202, deliberately reusing the heavy profile since plan 10-07's publish-OOM fix).
    500m > 220m: kube-scheduler can NEVER place these pods; memory is a non-issue (3618Mi/
    14.2Gi requested). (c) KubernetesPodOperator default startup_timeout_seconds=120 turns
    every attempt into a deterministic ~2min failure -- kpo.py's own on_finish_action comment
    (2026-08-16 debug session) already documents this exact mode as 'routine under this
    cluster's tight node CPU budget'. (d) TI-dump corroboration, run 32855002333: EVERY stage
    attempt with a recorded start/end lasted 128-130s (14:19:58->14:22:08=130s,
    14:22:09->14:24:17=128s, 14:30:44->14:32:53=128s, 14:32:53->14:35:02=129s,
    14:47:24->14:49:33=129s, 14:49:34->14:51:43=129s), try counts up to 6, at least one
    up_for_retry (proving FAILED attempts, not throttling), and ZERO stage successes -- while
    discover (100m request, fits in 220m free) succeeded try=1 in 11-23s in ALL FOUR DagRuns.
    (e) The pilot file customers_20240101.csv sat in meta.ingestion_runs status=PENDING for
    the full 1800s window: a stage container that ever STARTED would have moved it past
    PENDING -- direct DB-state proof that no stage container ever ran, ruling out the
    'pod runs then application fails' alternative. (f) The single global
    max_active_tis_per_dag=1 stage slot was 100%-occupied by these guaranteed-failing ~130s
    attempts (288 'task concurrency ... reached' messages, all stage): backfill__05:31 burned
    ~18-20 attempts x ~130s ~= 40min of slot time before dagrun_timeout killed it at 14:36:44,
    while scheduled__13:50's ten stage TIs starved completely (try=1, start=None at kill).
    HONEST GAP: no direct FailedScheduling/Pending event was captured (kubectl events had
    expired; the cp-monitor only watches the airflow namespace, never etl) -- the (d)+(e)
    convergence substitutes; next run's diagnostics should add an etl-namespace pod/event
    capture to close it.
  implication: >
    The ROUND 9 residual framed as 'throughput ceiling' is actually INFINITE per-file cost:
    no corpus size, batch cap, test budget, or concurrency-slot change can make CI drain while
    stage cannot schedule at all. Every remaining failure template reduces to (14):
    7x 180s-discovery-window misses (the DAG's max_active_runs=1 slot is pinned by
    45-min-dagrun_timeout runs that can never progress, so fresh files wait >180s for their
    next discover), 3x AlreadyRunningBackfill (backfills structurally cannot complete ->
    overrun test windows -> collide; 12 log occurrences), pilot PENDING-at-1800s, DBT_BUILD
    never RUNNING, normalized.customers <2 rows, SCD2 preconditions, XCom git_sha '' (its
    DagRun never completed). Fix (13)'s OOM headroom and fix (12)'s dbt chain remain
    necessary-but-insufficient; (14) is the next (and plausibly last structural) gate.

- timestamp: 2026-08-25 (ROUND 10 -- corpus-load quantification, Hybrid charter priorities 1-2)
  checked: >
    MinIO raw/customers listing from round9-job.log; sweep-corpus generation constants in
    tests/e2e/slice/test_backfill_2year_sweep.py; discover batching (map_index census in the
    TI dump); post-fix drain-floor arithmetic from measured pod-lifecycle latencies.
  found: >
    (a) CORPUS SIZE IS IRRELEVANT: the sweep corpus is 19 files x 3.1-3.5KiB (~62KiB TOTAL,
    50 rows/day); COPY cost is nil; pod lifecycle dominates any per-file cost. The user's
    'and their size' lever has nothing to cut. (b) CORPUS COUNT MATTERS via batch depth:
    discover's batching cap maps 10 stage TIs per DagRun (map_index 0-9 observed), all
    serialized through the ONE global stage slot. Corpus shape: _NUM_DAYS=20 with a gap day
    -> 19 files; feature-bearing days are ONLY indices 5(gap/absent), 7(schema change),
    10(late event), 12(attribute change), 16(late-correction arrival), 19(missing customer);
    the other ~13 files are plain filler whose only role is window realism. (c) POST-FIX
    FLOOR ESTIMATE (once stage schedules): measured pod-task wall time for the same image
    class is 11-23s (discover, incl. admission+start+xcom); stage adds trivial COPY ->
    ~15-40s per file; a 10-file DagRun's stage phase ~= 3-7min serialized, plus dbt_build +
    publish -> ~5-9min per DagRun. A fresh test file's discovery latency = remainder of the
    in-flight DagRun + next run's discover, i.e. WORST CASE still >180s at 10-file depth --
    so corpus/batch-depth reduction (the user's preferred lever) is genuinely load-bearing
    for the 180s windows AFTER schedulability is fixed, but is a NO-OP before it (the pilot
    test already proves a 1-file workload fails identically today). (d) Slot-redesign
    (option C proper) assessment: with CI free CPU realistically raisable to only
    ~350-450m, TWO concurrent 200m-class stage pods plus ambient discover/dbt pods do not
    fit anyway -- max_active_tis_per_dag=1 on stage/dbt_build/publish is MATCHED to CI
    capacity, and changing production concurrency would buy CI nothing. Recommend NOT
    exercising option C proper.
  implication: >
    Fix shape for ROUND 10 (pending user decision -- production-code implications): make
    stage/publish pods schedulable on CI (CPU-request sizing, options quantified in the
    checkpoint), THEN shrink the corpus filler days (user's preferred lever, preserving all
    six anomaly features) to bring per-DagRun drain under the 180s test windows; budget
    adjustments only if a measured floor still exceeds a specific test's window afterward.

- timestamp: 2026-08-25 (ROUND 10 -- fix implementation, A+B combination, offline verification)
  checked: >
    Implementation of the user-chosen A+B combination (fallback NOT needed -- see Current
    Focus fix_path_taken): airflow/dags/_common/kpo.py (new stage_pod_resources() reading
    Airflow Variable stage_cpu_request with default '500m'); both DAG files switched to it;
    scripts/ci-set-workload-images.sh (CI-profile Variable bootstrap site, sets
    stage_cpu_request=200m); helm/values/ci/{cnpg-analytics,minio,vault}.yaml CPU-request
    trims; tests/e2e/slice/test_backfill_2year_sweep.py corpus 20->13 days / seed v4->v5;
    .github/workflows/e2e-full.yml cp-monitor + dump step etl-namespace instrumentation.
  found: >
    (a) VARIABLE LEG PROVEN OFFLINE (the fallback trigger did not fire): direct DagBag
    round-trip -- with AIRFLOW_VAR_STAGE_CPU_REQUEST unset, both DAGs parse clean and
    stage/publish requests == {cpu:500m, memory:1Gi} (byte-identical to the pre-fix
    hardcoded values, so LOCAL needs zero provisioning); with the env var set to 200m, both
    DAGs' stage AND customers' publish resolve {cpu:200m, memory:1Gi}; limits {cpu:2,
    memory:4Gi} unchanged in every case; orders' publish stays on the light 100m profile
    (unchanged, as designed). (b) CI VARIABLE SITE: scripts/ci-set-workload-images.sh is
    invoked by ALL THREE e2e workflows post-cluster-up (grep-verified), so one change covers
    every CI cluster; PROFILE=ci values-file resolution for the vault trim verified via
    scripts/helm-install.sh's own values_file derivation. (c) PLATFORM TRIMS RENDERED:
    build/manifests/ci shows analytics-db 100m and minio 50m; vault is NOT in the rendered
    manifest set (installed by scripts/stages/80-vault.sh -> helm_install with
    helm/values/ci/vault.yaml), so its 50m trim lands live-only -- the offline budget-gate
    sum drops 150m (3.180 -> 3.030 of the 3.200 effective budget, test_ci_profile_fits_runner
    PASSES), while the NODE-level trim is the full 200m (~220m -> ~420m free; 200m stage +
    100m discover co-schedule with ~120m margin). (d) CORPUS: 12 customers + 12 orders files
    at the new shape (_NUM_DAYS=13, gap=3, schema=5, late_event=7, attr=8,
    correction_arrival=10/offset=7 landing exactly on the gap date, missing=12=last);
    direct byte-level verification of ALL SIX anomaly features (gap file absent; loyalty_tier
    header appears exactly from day 5; day-7 file contains the 90d-backdated 2023-10-10
    event_ts; member 30's name/country differ across the day-8 boundary; day-10 file carries
    member 31's EXTRA row backdated to the gap date 2024-01-04, strictly between real day-2
    and day-4 events; member 32 present on day 11, absent from final day 12); total corpus
    ~41KiB. _MASTER_SEED bumped v4->v5 per the established content-hash-idempotency
    precedent (same-named files change bytes under the new shape). (e) OFFLINE BATTERY ALL
    GREEN, zero new regressions: make manifests kubeconform -strict 540 resources/0 invalid/
    0 errors; test_manifest_resources 5/5; test_values_profiles 6/6 (per-profile CPU request
    is the permitted resource-sizing divergence axis); tests/policy -m 'not manifests' 157
    passed / 2 failed -- the SAME 2 pre-existing out-of-scope failures as every prior round;
    tests/unit 555/555; tests/dagtest 14/14; sweep file collects 7/7; ruff/py_compile clean
    on all touched python (2 ruff findings byte-identical on bare HEAD, pre-existing); mypy
    0 new errors (1 identical pre-existing import-untyped line A/B-verified via git stash);
    ruff format diffs on the sweep file byte-identical before/after (pre-existing drift,
    ROUND 4 precedent); bash -n clean on the edited script and BOTH edited workflow step
    bodies including the inner MONITOR_EOF heredoc.
  implication: >
    Root cause (14)'s fix is implemented on both sides of the scheduling inequality
    (request 500m->200m on CI via the Variable; free capacity ~220m->~420m via the trims)
    plus the user's preferred post-fix drain lever (12-file batch depth) and the
    FailedScheduling evidence-gap instrumentation. Ready for live falsification per the
    pre-registered criteria in the reasoning_checkpoint.

- timestamp: 2026-08-25 (ROUND 10 -- post-run analysis of live-verification run 32873456327)
  checked: >
    e2e-full.yml run 32873456327 (headSha d0d1ad6, job 97885720167, conclusion=failure at
    job level -- judged on internals per pre-registration; full 5891-line log archived at
    scratchpad round10-job.log), companion publish.yml 32873456458 conclusion, the six
    pre-registered post-run items: fix-in-force probes, TI dump, NEW etl-monitor.log
    FailedScheduling census, Kyverno DENY grep, cp-monitor restart timeline + describe,
    pytest failure-template census + node-ID diff vs baseline_sorted.txt.
  found: >
    (a) FIX IN FORCE: 'registering stage_cpu_request=200m' + 'Variable stage_cpu_request
    created' at cluster-up; effective-Variable dump prints 200m; node Allocated cpu Requests
    2580m/86% vs ROUND 9's 2780m/92% (full ~200m trim landed; ~420m free). (b) FIRST-EVER
    STAGE SUCCESSES ON CI: all 20 mapped stage TIs (2 backfill DagRuns x 10-file batch)
    state=success try=1 at ~30-32s wall, draining back-to-back through the single global
    slot in ~5.5min per batch; discover try=1 in 17-22s as always. Pre-fix: zero stage
    successes ever, every attempt 128-130s. (c) FailedScheduling census: ZERO lines captured
    across the entire run; etl events show immediate Scheduled->Started for every
    stage/dbt-build pod -- the confirming 'none at all post-fix' branch. (d) Kyverno DENY 0
    (all image-verification-outcomes status=pass). (e) Restarts 0 across
    scheduler/dag-processor/triggerer; scheduler peak 1644MiB/23pids (>1536Mi old ceiling,
    <2048Mi new limit -- fix (13) again proven load-bearing). (f) pytest 17/21/6 in 3746s;
    node-ID set IDENTICAL for the 12th run (saturated) BUT census shifted decisively:
    pilot PENDING->STAGED; dbt-build pods now RUN (6 pods Failed with real dbt output
    'Completed with 2 errors'): silver_orders model fails 'null value in column dataset_id
    of relation dedup_audit violates not-null constraint' (failing row: run 37, dataset_id
    null, dataset 'orders'), and test reconciliation_customers fails 'permission denied for
    table datasets' under the least-privilege dbt_app role. dbt_build fails every try
    (~30s), tries to 6, DagRuns held to dagrun_timeout=45min (16:49:34->17:34:34) ->
    max_active_runs=1 pins the scheduled slot 45min -> 7 discovery-window misses, 3
    AlreadyRunningBackfill, DBT_BUILD-never-RUNNING, <2-rows and SCD2 preconditions are all
    knock-ons. git_sha template: XCom carries the FULL sha OF THE CURRENT COMMIT
    (d0d1ad6be1...) vs `git rev-parse --short HEAD` (d0d1ad6) -- same full-vs-short shape as
    ROUND 9 (ee87708...) => comparison/build-format artifact, not a stale image.
    'meta' unexpected-schema template byte-identical to ROUND 9.
  implication: >
    ROOT CAUSE (14) is LIVE-CONFIRMED and its fix VERIFIED at mechanism level -- the
    structural unschedulability is gone and the ingestion stage hop works on CI for the
    first time in the session. The residual signature is now dominated by a NEW, nameable,
    deterministic cause candidate (15): the dbt project/role layer fails on a fresh CI
    cluster ((15a) dedup_audit dataset_id NULL for 'orders'; (15b) dbt_app lacks SELECT on
    meta.datasets in the reconciliation test's compiled SQL), and everything else in the
    17-test set reduces to its 45min-wedge knock-on plus two independent test-side
    artifacts (git_sha full-vs-short compare; 'meta' schema allowlist). Why (15) was never
    visible before: dbt had NEVER actually executed on CI until this run (blocked by (12)
    then (14)). Per the standing rule, no ROUND 11 fix without a decision checkpoint.

- timestamp: 2026-08-25 (ROUND 11 -- root-cause investigation of (15a)/(15b) + the two
    test artifacts, all by direct source reads + direct LOCAL-cluster queries)
  checked: >
    (a) dbt/macros/dedup_audit_post_hook.sql + dbt/macros/reconciliation_post_hook.sql
    full source; (b) migrations 0001/0021/0024/0028/0032 (schema/grant/constraint layer);
    (c) dataplat config/registry.py + metadata/postgres.py (the ONLY two meta.datasets
    writers repo-wide); (d) dbt/tests/reconciliation_{customers,orders}.sql; (e) LOCAL
    analytics-db live queries: meta.datasets contents, role_table_grants on meta.datasets,
    SET ROLE dbt_app probes (the recon-test join AND meta.dataset_id_for_name), schema
    owners + has_schema_privilege + information_schema.schemata AS analytics_owner;
    (f) LOCAL airflow-db task_instance history for task_id='dbt_build'; (g) Vault
    bootstrap _ensure (k) block (dbt-db secret -> user=dbt_app both profiles);
    (h) docker/dbt/Dockerfile (COPY dbt/ -> project baked into image) + local image tags
    vs git ancestry of the recon-test commit 60284e7; (i) Makefile GIT_SHA (short) vs
    publish.yml build-args GIT_SHA=${{ github.sha }} (full); (j)
    tests/e2e/cluster/test_postgres_topology.py ALLOWED_SCHEMAS + round10-job.log exact
    failure text.
  found: >
    (15a) MECHANISM CONFIRMED: dedup_audit_post_hook inserts ONE audit row UNCONDITIONALLY
    per invocation, resolving dataset_id via meta.dataset_id_for_name('{dataset}')
    (migration 0028 SECURITY DEFINER, returns NULL for unregistered names) into a NOT NULL
    FK column (migration 0024). meta.datasets rows are created ONLY by ingestion-side code
    (registry.sync upsert at cli config-sync; get_or_create_dataset at discovery) -- no
    config-sync DAG exists yet, no seed. A fresh cluster where orders never ingested has no
    'orders' row, but dbt build (WHOLE project, triggered from csv_ingest_customers) still
    runs silver_orders -> post-hook inserts dataset_id NULL -> deterministic failure.
    LATENT-LOCALLY CONFIRMED: local meta.datasets has orders=dataset_id 1 (registered
    before customers=76, from early local ingests) -- the resolution succeeds locally
    purely by historical accident of ingestion order. This is a PRODUCTION-SHAPED defect
    CI caught: ANY fresh deployment where the dbt project builds before every dataset's
    first ingest hits it. CRITICAL COUPLING (constrains the fix): reconciliation_post_hook
    derives its per-model floor by EXCLUDING the current build's own dedup_audit row via
    identity (dedup_audit_id < max(dedup_audit_id) for model_name) -- this RELIES on
    dedup's unconditional insert, so a skip-on-empty-new_bronze guard would lower recon's
    floor on every no-op build of a REGISTERED dataset and re-write duplicate
    reconciliation rows. A skip-on-UNRESOLVABLE-dataset guard has no such hazard:
    unregistered => staging.<ds> empty (rows only arrive via ingestion, which registers
    the dataset first) => recon's max(dedup_audit_id) over zero rows -> NULL -> floor 0 ->
    bronze_files empty -> recon writes zero rows (its own documented no-phantom-row
    cross-join) -- both hooks cleanly no-op.
    (15b) MECHANISM CONFIRMED + NOT LATENT LOCALLY -- LIVE ON BOTH CLUSTERS: the two
    singular tests JOIN meta.datasets directly; dbt runs as dbt_app in BOTH profiles
    (vault-bootstrap (k): etl/dbt-db user=dbt_app; resolve_secrets.py -> DBT_PG_USER);
    dbt_app has SELECT+INSERT on meta.reconciliation_results (0032) + EXECUTE on the 0028
    function but ZERO grant on meta.datasets (D-08 boundary). Direct local reproduction:
    SET ROLE dbt_app -> the recon-test join = 'ERROR: permission denied for table
    datasets'; meta.dataset_id_for_name('orders'/'customers') as dbt_app = 1/76 (works).
    Corroborating timeline: local dbt_build has ZERO successes since 2026-08-20 19:26
    (customers; orders' single success 2026-08-20 16:42) -- the recon tests landed
    2026-08-19 (60284e7), are contained in dbt image 46da94a (built 08-20, ancestry
    verified) and in the currently-pinned localhost:5001/dbt:d290f77 (built 08-23, project
    baked in via COPY dbt/) -- consistent with the permission denial having broken local
    dbt_build the moment an image containing the tests rolled out. NOT a fresh-cluster
    artifact.
    ARTIFACT (i) CONFIRMED: Makefile bakes GIT_SHA=$(git rev-parse --short HEAD) locally;
    publish.yml bakes GIT_SHA=${{ github.sha }} (full 40-char) on CI; the test compares
    XCom git_sha against `git rev-parse --short HEAD` -- passes locally, fails on CI with
    the image demonstrably CORRECT (round10 log: XCom d0d1ad6be1... IS the checked-out
    d0d1ad6). Pure comparison-shape bug.
    ARTIFACT (ii) CONFIRMED: ALLOWED_SCHEMAS={pg_catalog,information_schema,public,
    pg_toast} (test_postgres_topology.py line 37, Phase-3-era 'no schema exists yet'
    docstring). Migration 0001 creates schema meta; migrations 0012/0013/0038 grant
    analytics_owner (the analytics-db-app secret user the test connects as) USAGE/SELECT
    on meta objects -- so information_schema.schemata AS analytics_owner returns exactly
    {information_schema,meta,pg_catalog,public} (verified live locally; staging/silver/
    normalized are NOT visible to it -- no USAGE), matching CI's exact failure "unexpected
    schema(s): ['meta']". Stale allowlist; would fail locally identically. Add 'meta'.
  implication: >
    All four ROUND 11 scope-B items have confirmed mechanisms and right-layer fixes:
    (15a) guard dedup_audit_post_hook's audit INSERT on dataset resolvability (WHERE
    meta.dataset_id_for_name(...) IS NOT NULL) -- fixes the fresh-deployment NULL without
    perturbing the recon floor coupling, keeps the every-build-audit-row invariant for
    registered datasets, and fails LOUDLY (decisions-insert FK violation) if the
    unregistered-implies-empty-staging invariant is ever breached; (15b) rewrite both
    singular tests to rr.dataset_id = meta.dataset_id_for_name('<ds>') -- preserves
    least-privilege dbt_app exactly as migration 0028 designed (NO new grant); (i) compare
    XCom sha as a prefix of `git rev-parse HEAD` (min-length-guarded) so both the local
    short-sha and CI full-sha bake formats prove the same commit; (ii) add 'meta' to
    ALLOWED_SCHEMAS with a migration-0001 justification. Note: fixing 15b also unblocks
    LOCAL dbt_build (broken since 2026-08-20, previously unnoticed).

- timestamp: 2026-08-25 (ROUND 11 POST-RUN -- analysis of live-verification run 32884691063,
    headSha 377c068, conclusion CANCELLED by the job-level timeout)
  checked: >
    gh run view 32884691063 (single job 97922265576 'Full local E2E suite + rebuild-from-raw
    capstone', started 18:36:02Z, completed 20:36:44Z = 2h00m42s, conclusion cancelled);
    partial job log fetched via gh api actions/jobs/97922265576/logs (5577 lines, saved
    round11-job.log) -- the cancelled `make cluster-slice-verify` step's STREAMED stdout was
    NOT archived (log jumps from the pytest invocation line at 18:42:58 straight to
    '##[error]The operation was canceled.' at 20:36:24), but ALL if:always() diagnostic
    steps ran at cancel time and were captured in full: cp-monitor per-role peaks +
    restart timeline, FailedScheduling census, etl-monitor, final pods/events/node,
    DagRun history (8 runs), 5-key TaskInstance history (103 TIs), scheduler-log grep,
    triggerer status/log, MinIO raw/customers listing. Also: .github/workflows/e2e-full.yml
    line 41 (timeout-minutes: 120), git show d0d1ad6 (ROUND 10 corpus shrink 20->13 days,
    _ROWS_PER_DAY=50, anomaly indices re-derived, seed v4->v5),
    packages/dataplat/src/dataplat/scd/delete_detection.py (delete-detection scoping
    docstrings), airflow/dags/csv_ingest_customers.py (publish retries=6, dagrun_timeout=
    45min, max_active_runs=1, schedule */1).
  found: >
    (1) FIX (15) LIVE-CONFIRMED -- criteria (a)+(b) MET: dbt_build state=success try=1 in
    ALL 7 DagRuns that reached it (backfill 10:24 18:49:48-18:50:17; 10:25; 12:55; 12:56;
    scheduled 18:43; 19:28; 20:13 -- 20-30s wall each), the FIRST dbt_build successes ever
    observed on CI; zero occurrences of 'null value in column' and 'permission denied for
    table datasets' in the entire log.
    (2) FIRST-EVER COMPLETE END-TO-END DAGRUN ON CI: backfill__10:24 state=success,
    18:44:24->18:50:52 (6m28s): wait_for_files 8s, discover try=1 19s, stage map 0-9 ALL
    try=1 (19-26s each, serial), dbt_build 29s, publish try=1 SUCCESS 29s. The full
    discover->stage->dbt_build->publish pipeline is now PROVEN on GitHub free-tier CI.
    (3) Criterion (c) NOT MET -- NEW residual (16), publish poison: every subsequent
    DagRun's publish fails deterministically with QualityThresholdExceeded rule_id=
    mass_delete_circuit_breaker 'vanished-key ratio 54.00% exceeds configured mass-delete
    threshold 10.00%' current_count=50 vanished_count=27 (byte-identical across 12:56
    tries 1-4, the window the captured scheduler log covers; publish end-states elsewhere:
    skipped try=6 in 10:25/18:43/19:28/12:55, up_for_retry try=4/5 in the two in-flight
    runs). Four DagRuns failed at EXACTLY start+45:00 = dagrun_timeout (18:43:51->19:28:51;
    18:50:53->19:35:53; 19:28:52->20:13:53; 19:36:07->20:21:08). backfill__12:56 running +
    12:57 queued + scheduled 20:13 running at cancel.
    (4) MinIO listing at cancel: raw/customers/ contains EXACTLY the 12 corpus files, all
    LastModified 18:43:54, sizes 3.1-3.5KiB -- NO mass-delete snapshot object exists, so
    the 54% trip is generated by re-publishing ordinary corpus snapshots against evolved
    gold state, not by the mass-delete test's fixture. current_count=50 == _ROWS_PER_DAY
    (each corpus file is a full 50-row daily snapshot -> gold current == one day's roster).
    (5) Criterion (d) MET, all regression guards green: stage 60/60 success try=1 across
    all 6 staged runs (19-26s in the first two runs, 8-12s warm from 12:55 onward --
    fastest CI stage times ever); FailedScheduling census EMPTY; Kyverno DENY count 0;
    restart-count timeline EMPTY (0 restarts all roles); peaks scheduler 1288MiB/21pids
    (< 2048Mi), dag-processor 699MiB, triggerer 422MiB.
    (6) Criterion (e) pytest node-ID diff UNMEASURABLE: streamed stdout of the cancelled
    step is unrecoverable; no per-test PASS/FAIL exists. Cluster evidence places the suite
    still in tests/e2e/slice (sweep module; backfill_id=2 window 12:55-12:57 in flight) at
    113m26s of pytest wall.
    (7) Timeline of the 120min: cluster-up 6m11s (18:36:10-18:42:21), glue 37s, pytest
    113m26s of which: cluster tests + corpus upload <1min, one healthy DagRun 6m28s, then
    the serial 45-min wedge cadence ate everything else. Job timeout-minutes: 120.
    (8) Measured floors (deferred budget assessment input): wait_for_files 5-28s, discover
    9-19s, stage/file 8-12s warm, dbt_build 20-30s, publish 29s; healthy 10-file DagRun
    6m28s cold / ~2.5-3min warm est.; publish retry cadence +0/+1m/+2.5m/+5.9m.
  implication: >
    The ROUND 11 hypothesis is CONFIRMED at its core -- (15a)/(15b) were the dbt_build
    blockers, and unblocking dbt exposed the next layer exactly as blind-spot (3)
    predicted (publish executing repeatedly on CI for the first time). The orchestrator's
    proposed interpretation ('suite grew past 120min BECAUSE fixes work') is only HALF
    right: the fixes verifiably work (a/b/d green, first full pipeline proven), but the
    budget was consumed by the NEW deterministic publish wedge (16), not by honest
    long-running successes. Decisive arithmetic: the poison is self-sustaining (tripped
    runs never publish -> their files are never marked ingested -> re-discovered next run
    -> same trip), so each future DagRun costs 45min and fails -- raising timeout-minutes
    ALONE can never make this suite green; (16) must be root-caused first. Conversely the
    healthy-path floors suggest that WITH (16) fixed the suite plausibly fits the existing
    120-min budget (five-ish sweep DagRuns x 3-6.5min + scheduled stream + fixed 7min
    setup), so the timeout decision should WAIT for a post-fix measurement. Evidence gap
    for ROUND 12: which files each run staged (no assignment_uri/bronze-run mapping in any
    diagnostic) -- needed to decide between 'older-window re-publish vs evolved gold' and
    alternative orderings; also two open sub-questions (publish skipped-at-try-6 mechanism;
    why wedged runs sat to +45:00 after publish resolved). Bearing on reserved options:
    zero FailedScheduling + warm 8-12s stages + 0 restarts say the platform now FITS the
    free-tier runner -- current evidence argues AGAINST reviving runner migration/job
    splitting.

- timestamp: 2026-08-27 (ROUND 12 -- root-cause investigation of residual (16), source
    reads + round11 TI-history mining + LOCAL red/green reproduction)
  checked: >
    (a) dataplat/scd/delete_detection.py (_VANISHED_SQL silver scoping, breaker),
    load/publish/scd.py (publish steps A-D, _CURRENT_COUNT/_SNAPSHOT_MAX SQL),
    pipeline/run.py publish_ingest (claims ALL currently-STAGED runs),
    metadata/repository.py (run lifecycle PENDING->RUNNING->STAGED->SUCCEEDED; stage
    claim requires PENDING/FAILED/expired-lease; SKIPPED_DUPLICATE for SUCCEEDED),
    discovery.py (idempotency-key formula incl. schema_version_term resolved from
    schema.get_current at line 909; create_file/get_or_create_batch idempotent;
    deterministic sort + max_units_per_run cap; ALREADY_SUCCEEDED skip; replay_of_run_id),
    csv_processor source.py _resolve_schema (CONTRACT v1 bootstrap; INFERRED v2 on new
    column; resolve_by_hash for historical schemas -- no version flip-flop),
    schema/repository.py sync() (hash-compare, no-op on same hash),
    dbt silver_customers.sql (incremental floor = max(_run_id) in silver; ranking
    event_ts desc/_source_row_number desc/_file_id desc; winner keeps its own _run_id),
    tools/corpus/dated_series.py (fixed 50-member roster resent in full every non-gap
    day; include_extra = day_index >= 5 so days 5-12 all carry the extra column; missing
    member only from day 12), test_backfill_2year_sweep.py seed-v5 parameters.
    (b) round11-job.log TI history: stage map counts per DagRun, discover/publish
    timings, publish end-states. (c) NEW local reproduction
    tests/integration/test_scd_replay_delete_detection.py: fresh PG18 container +
    alembic + REAL dbt builds + real SCDPublisher at threshold 0.10, wave 1 (10
    day-files x 50-key roster) published, then a byte-identical replay wave under new
    run_ids.
  found: >
    ROOT CAUSE (16) CONFIRMED, full chain: fresh cluster -> first discovery mints keys
    with schema term '' -> staging registers v1 then v2 (day-5+ extra column) -> next
    discovery's term '2' re-eligibilizes ALL files (D-18 replay by design; explains
    every discover emitting 10 units, incl. 10:25's at 18:51:54 one minute AFTER 10
    runs were SUCCEEDED) -> replayed bronze rows are byte-identical (same
    event_ts/_source_row_number/_file_id, only _run_id differs) -> silver's ranking
    FULL-TIES and the winner's _run_id is ARBITRARY (live 23/27, local repro 26/24) ->
    _VANISHED_SQL's silver-scoped snapshot reads every tie-loser as vanished (live
    27/50=54%, local repro 24/50=48% -- red reproduction of the exact trip class) ->
    trip rolls back the tx -> runs stay STAGED -> identical staged_run_ids + frozen
    silver -> byte-identical trip forever. RIDERS RESOLVED (not separate bugs):
    'skipped try=6' is dagrun_timeout's force-skip of the up_for_retry TI; the +45:00
    sit is try 7's ~16min exponential backoff reaching past the timeout -- publish had
    not resolved at 19:22, it was up_for_retry. B-LEG: roster churn is structurally
    1/50=2% max (missing member, day 12 only) -- NO legal ordering exceeds the 10%
    threshold under a correct computation; corpus/threshold correctly sized, ROUND 10
    shrink blameless; the deliberate mass-delete fixture (15/50=30%) still trips
    post-fix.
  implication: >
    Production-shaped platform defect, not a test/corpus problem: ANY deployment doing
    a D-18 formula-driven replay (schema evolution, config change, processor bump) of
    snapshot-semantics data would trip the breaker on correct traffic -- or WORSE, at a
    permissive threshold, apply delete semantics to keys that are actually present
    (silent SCD closure of live keys). Fix at the definition layer: the pass's staged
    snapshot is its BRONZE (staging.customers scoped by staged_run_ids -- exact by
    construction, same scoping Step B always used), never silver's tie-lineage proxy;
    plus a deterministic _run_id desc final tie-break in both silver models (section-67
    determinism). Quarantine-vs-retry on deterministic breaker trips left as a design
    decision item (production semantics, checkpoint), not needed for CI to go green.

- timestamp: 2026-08-27 (ROUND 12 -- fix implementation + offline verification)
  checked: >
    Red/green falsification of fix (16): the new replay regression test run pre-fix
    (red) and post-fix (green); tie-split diagnostic pre/post 16c; full offline battery
    incl. an A/B of the whole tests/integration directory against clean HEAD.
  found: >
    RED (pre-fix): test_scd_replay_delete_detection fails exactly as live CI --
    'replay of identical content read 24/50 keys as vanished (48%)'; tie split 26
    new / 24 old (arbitrary). GREEN (post-fix): vanished==0, publish succeeds at
    threshold 0.10, tie split deterministic 50/0 (16c working). One implementation
    defect caught by the battery and fixed: the first silver_orders.sql edit used
    Jinja '{#- -#}' trim markers between 'partition by' and 'order by', gluing them
    into invalid SQL ('order_idorder by') -- switched both models to non-trimming
    '{# #}' comments; all dbt suites re-green. Battery: tests/unit 555/555;
    tests/dagtest 14/14; tests/policy -m 'not manifests' 157 pass / 2 fail (SAME 2
    pre-existing out-of-scope); make manifests kubeconform 540 resources / 0 invalid
    / 0 errors; test_manifest_resources 5/5; test_values_profiles 6/6; targeted
    integration: test_scd_delete_detection 14/14 (semantics updated + new (16) guard
    test), test_dbt_{silver_dedup,dedup_audit,silver_incremental,reconciliation} +
    replay test 11/11 + 1/1; FULL tests/integration A/B: failure set with fix is
    BYTE-IDENTICAL to clean-HEAD baseline (21 pre-existing local-env/order failures,
    diff empty) -- zero regressions; ruff clean; ruff format diffs byte-identical
    pre-existing drift; mypy errors identical-class pre-existing test idiom
    (type-ignore-with-suffix, test_publish_scd precedent); e2e-full.yml yaml parse OK.
  implication: >
    Fix (16) is offline-proven at the strongest level this session can produce (genuine
    red/green against a fresh-database reproduction of the live failure). Remaining
    verification is the live CI run (pre-registered criteria in Current Focus).

- timestamp: 2026-08-27 (ROUND 12 -- post-run analysis of live-verification run 33051719850)
  checked: >
    Run 33051719850 (headSha 794db33): conclusion=cancelled at job timeout-minutes:120,
    job 98448606061 ran 07:54:59Z->09:55:41Z = 2h00m42s. Partial log (5939 lines, 736KB)
    fetched via gh api jobs/<id>/logs to scratchpad round12-job.log; all always()-
    diagnostics present incl. the new ROUND 12 blocks. Cross-checked against source
    (tests/e2e/slice/conftest.py, tests/e2e/slice/test_backfill_2year_sweep.py,
    airflow/dags/csv_ingest_orders.py, airflow/dags/csv_ingest_customers.py, Makefile)
    and a live query of the LOCAL cluster's orders DAG paused flag.
  found: >
    (1) BREAKER: 0 grep hits for QualityThresholdExceeded / mass_delete_circuit_breaker /
    vanished in the entire log -- zero trips (was: byte-identical 27/50=54% trip in every
    post-first publish of ROUND 11).
    (2) DAGRUNS: 63 total csv_ingest_customers DagRuns -- 62 state=success + 1 running at
    cancel; ZERO failed; ZERO at +45:00. Pilot backfill_id=1: 2 runs (7m50s, 7m53s,
    08:03:31->08:19:15). Sweep backfill_id=2: 3 runs (3m41s, 2m04s, 1m59s,
    08:19:53->08:27:38). First scheduled run 18m24s (overlapped both backfills);
    thereafter ~56 steady-state scheduled runs at 96-104s each, back-to-back (*/1
    schedule + ~96s runtime = always exactly one active run).
    (3) TASKINSTANCES: 344 total across 5 key tasks: 226 success, 114 skipped, 4
    running/blank, 0 failed, 0 up_for_retry; every recorded try= 1 (skips try=0). The
    114 skipped = stage+dbt_build in the ~57 empty-window runs (correct short-circuit);
    publish RAN in every scheduled run (one KPO publish pod per ~96s -- it must, to emit
    the customers_asset event).
    (4) FIX-IN-FORCE DIAGNOSTICS (new ROUND 12 blocks, all present): meta.schema_versions
    = exactly 2 rows: v1 CONTRACT (valid 08:05:08->08:07:06) + v2 INFERRED (08:07:06,
    open) -- the predicted ''->'2' idempotency-term flip. meta.ingestion_runs->files: 36
    runs in the predicted 3-wave shape -- runs 1-12 pass 1 (10 SUCCEEDED, days-12/13
    PENDING), 13-24 pass 2, 25-34 replay wave with replay_of_run_id=(13,14,3,4,5,6,7,8,
    9,10), 35/36 = days 12/13 SUCCEEDED. staging.customers per-run: 32 delivered runs,
    50 keys each (49 for run 36 -- the day-13 missing-member file; 51 rows/50 keys for
    the in-file-dup runs 10/22/34). silver _run_id distribution: {35: 1, 36: 49} --
    fully deterministic newest-run lineage (16c tie-break live-confirmed; ROUND 11 had
    arbitrary 23/27). Run 36's 49-key pass published CLEAN: 1/50=2% vanished under
    bronze-scoped (16a) < 10% threshold -- exactly the B-leg churn arithmetic.
    (5) REGRESSION GUARDS: FailedScheduling census EMPTY; Kyverno denials 0; restart
    timeline EMPTY; peaks scheduler 1415MiB/24pids (<2048Mi), dag-processor 872MiB,
    triggerer 426MiB. All 32 stage deliveries ~30s try=1.
    (6) THE 120 MINUTES: cluster-up 6.7min (07:54:59->08:01:42); migrate+vault 0.6min;
    pytest starts 08:02:15; corpus upload 08:03:07 (12 customers files, MinIO listing);
    all test-driven pipeline work COMPLETE by 08:27:38 (last stage pod 08:25; customers
    12/12 files terminal SUCCEEDED incl. 35/36). Then 87.6min (08:27:38->09:55:12) with
    ZERO test-driven artifacts: no new backfills (only ids 1,2 exist), no uploads, no
    orders pods -- only the customers steady-state discover+publish churn. pytest emitted
    ZERO flushed output the whole step (-q dots never complete a line; no summary in a
    cancelled job) -- node-ID census NOT measurable this round.
    (7) THE WEDGE: test_full_2year_sweep_customers_and_orders (alphabetically the 2nd
    slice module test to run) passed wait 1 (backfill complete, 08:27:38) and wait 2
    (customers files terminal, ~immediately after) and entered wait 3:
    _wait_for_dataset_files_terminal(dataset=orders, timeout=5400) (line 1168). Orders
    drains ONLY via csv_ingest_orders (schedule=[customers_asset], Deviation 1: CLI
    backfill impossible -- DagNonPeriodicScheduleException). csv_ingest_orders is PAUSED
    on every fresh cluster (Airflow default dags_are_paused_at_creation=true, no repo
    override, no is_paused_upon_creation on the @dag) and NOTHING unpauses it:
    _unpause_slice_dags loops over (_SMOKE_DAG_ID, _CUSTOMERS_DAG_ID) only
    (conftest.py:155); Makefile:440 unpauses only smoke; repo-wide grep finds no other
    site. ~60 customers_asset events were emitted (publish outlets, line 192) -- a
    paused DAG consumes none, silently. The wait would have failed legibly at its own
    5400s deadline ~09:57; the job-level 120min timeout cancelled at 09:55:12, ~2min
    earlier, destroying the pytest summary. DIFFERENTIAL: local long-lived cluster
    queried live 2026-08-27: csv_ingest_orders is_paused='False' (hand-state from an
    earlier session) -- explains why this has never been seen locally. Same class as
    root cause (12).
  implication: >
    BRANCH (b) holds with direct evidence at every link but one (the dead CI cluster's
    own is_paused flag, unobservable post-mortem). Fix (16) is fully live-confirmed;
    root cause (16) is CLOSED. NEW ROOT CAUSE (17): csv_ingest_orders never unpaused on
    ephemeral CI. The fix is small and test/platform-scoped (three candidate shapes --
    conftest tuple, Makefile cluster-up, or is_paused_upon_creation=False; the last is a
    production-semantics change requiring explicit approval). Budget outlook after (17):
    measured honest floor 33min through the sweep backfill + an estimated 5-15min orders
    drain (13 files, ~30s stage each, integrity_gate capped at 3) + the module's
    remaining tests + 6 other slice modules -- plausibly fits 120min given post-fix
    DagRun pace (96s-8min), but unproven until the next run; timeout-minutes decision
    stays deferred per charter.

- timestamp: 2026-08-27 (ROUND 13 -- fix (17) implemented A+B+C, offline battery green)
  checked: >
    Implementation of the user-approved A+B+C fix shape for root cause (17), plus the
    C-scope survey and the full offline battery. SURVEY (which DAGs get
    is_paused_upon_creation=False): direct grep of every @dag schedule in airflow/dags/ --
    csv_ingest_orders schedule=[customers_asset] (ASSET-scheduled: paused => asset events
    silently dropped, unrecoverable without manual unpause + re-emission => the
    no-silent-drops argument HOLDS); csv_ingest_customers cron '*/1 * * * *',
    smoke_kubernetes_pod + platform_retention '@daily' (cron/interval: pausing delays runs
    VISIBLY -- next tick fires on unpause -- no silent drop => argument does NOT hold =>
    Airflow default kept); chaos_probe x3 schedule=None (manual-trigger only, pause state
    irrelevant => default kept). C applied to csv_ingest_orders ALONE. SITE CHECK for B:
    the only pre-existing Makefile unpause (line ~440, smoke) lives in smoke-verify, which
    e2e-full.yml NEVER runs (its steps: cluster-up -> migrate-analytics ->
    vault-unseal/bootstrap -> cluster-slice-verify -> observability-verify-ci ->
    rebuild-from-raw) -- so B landed in the cluster-up target itself (the only site that
    covers the rebuild-from-raw capstone), with a 24x5s retry loop copied from
    smoke-verify's own trigger idiom (right after the stages finish, the dag-processor may
    not have parsed the DAG yet; `airflow dags unpause` exits non-zero on DagNotFound),
    hard-failing after 120s (a silent skip would recreate exactly the bug being fixed).
    Also confirmed: tests/e2e/chaos/conftest.py imports _unpause_slice_dags from the slice
    conftest, so A auto-covers the chaos suite; NO dagtest/unit test asserts paused state
    anywhere (grep), so no test updates needed beyond the line-budget bump;
    tests/policy/test_no_manual_kubectl_surgery.py scans only *.sh files, so the Makefile
    kubectl-exec follows the established smoke-verify precedent cleanly.
  found: >
    FILES CHANGED: (A) tests/e2e/slice/conftest.py -- _ORDERS_DAG_ID added, unpause tuple
    now (smoke, customers, orders), docstring truth-up documents the ROUND 13 mechanism
    (the old text claimed 'both this phase's DAGs' while covering only two). (B) Makefile
    cluster-up target -- retried csv_ingest_orders unpause appended after
    scripts/cluster-up.sh, ROUND 13 comment block, hard fail if never registered within
    120s. (C) airflow/dags/csv_ingest_orders.py -- is_paused_upon_creation=False + 2-line
    comment (exactly 3 lines at the zero-headroom 158 ceiling);
    tests/policy/test_dag_line_budget.py budget <=158 -> <=161 with the precedent-style
    docstring paragraph recording the survey rationale (deliberately NOT mirrored to
    cron-scheduled customers). OFFLINE BATTERY ALL GREEN, zero new regressions: throwaway
    DagBag proof via the tests/unit conftest fixture -- csv_ingest_orders
    is_paused_upon_creation is False, every OTHER DAG stays None (default) -- 2/2 passed;
    tests/unit 555/555; tests/dagtest 14/14 (real dag.test() accepts the new kwarg);
    tests/policy -m 'not manifests' 157 passed / 2 failed -- the SAME 2 pre-existing
    out-of-scope failures as every prior round (test_dag_line_budget customers 208>158
    tracked separately; test_gates_actually_fail), and the ORDERS budget test passes at
    exactly 161/161; make manifests kubeconform -strict 540 resources / 0 invalid / 0
    errors (no helm changes this round); test_manifest_resources 5/5;
    test_values_profiles 6/6; make -n cluster-up parses; py_compile + ruff check + ruff
    format --check clean on all touched files; mypy A/B via git stash: 74 error lines
    both pre- and post-change (all pre-existing common_kpo_kwargs/XComArg idiom -- zero
    new). timeout-minutes: 120 deliberately unchanged (measure this round).
    NOT STAGED (not mine): .planning/HANDOFF.json deletion + .planning/STATE.md
    modification present in the working tree from the session manager.
  implication: >
    Fix (17) is implemented at all three layers with the C-scope survey recorded.
    Remaining: commit + push, record the authoritative e2e-full run ID + companion
    publish.yml run for the pushed sha, then hand the single 60s watcher to the session
    manager. Judged next run on the ROUND 13 pre-registered criteria (orders pods appear;
    sweep drains; duration vs 120min measured; regression guards; node-ID census vs the
    17-set with the carried sweep-assertion-(10) caveat).

- timestamp: 2026-08-27 (ROUND 13 -- post-run analysis of live-verification run 33062702180)
  checked: >
    Authoritative ROUND 13 run 33062702180 (headSha 4d3db56, created 10:21:48Z, job
    98485143753 'Full local E2E suite + rebuild-from-raw capstone' 10:21:52->12:22:41Z,
    conclusion CANCELLED at exactly the 2h job timeout, 3rd consecutive 120-min cancel).
    Partial job log FULLY recovered via gh api actions/jobs/98485143753/logs -- 7280
    lines (vs 5939 in R12) incl. the complete always()-diagnostics battery: cp-monitor
    time series + per-role peaks + restart timeline, etl-monitor rolling pod/event
    census (365 polls), FailedScheduling census, customers DagRun/TI DB dumps,
    scheduler/triggerer log greps, meta.ingestion_runs->meta.files mapping,
    schema_versions history, silver _run_id distribution, staging per-run counts.
    Saved to scratchpad round13-job.log. Greps run: csv_ingest_orders/unpause;
    QualityThresholdExceeded/mass_delete_circuit_breaker/vanished; FailedScheduling;
    Kyverno DENY; per-pod FS first/last-seen extraction; poll-by-poll pod census.
  found: >
    (i) FIX (17) IN FORCE: cluster-up printed '==> csv_ingest_orders unpaused
    (asset-triggered runs enabled)' at 10:28:07Z (layer B loop succeeded; ROUND 13
    Makefile comment block visible at lines 876-886).
    (ii) FIRST-EVER ORDERS EXECUTION ON CI: meta.ingestion_runs rows for dataset=orders:
    runs 25-34 (files 37-46, orders_20240101-11) ALL SUCCEEDED, schema_versions rows 3/4
    (dataset_id 55) = v1 CONTRACT 10:37:56 -> v2 INFERRED 10:39:29 (the same term-flip
    shape customers showed in R12); replay wave runs 49-60 ALL SUCCEEDED with
    replay_of_run_id 25-34, plus days 12/13 (runs 59/60, files 49/50) SUCCEEDED
    first-pass. Later run 624 (raw/orders/e2e-orphan-*.csv) SUCCEEDED -- orders pipeline
    re-ran on demand for the referential-orphan test. Criterion (a) MET; refutation
    branch (7) NOT taken.
    (iii) SWEEP DRAINED: the orders-terminal wait that ate 87.6min in R12 completed
    inside the sweep phase (~10:45-11:03); backfill_4 (next test) started 11:03:27.
    Criterion (b) MET.
    (iv) SUITE DEPTH RECORD: by cancel, meta.files reached id 1301 and ingestion_runs
    id 666 (R12: 50/36). Fixture trail proves module progression:
    e2e-dbtkill run 461 + e2e-u3 run 515 (test_pod_kill_retry), e2e-rebuild run 623
    (test_rebuild_from_raw), e2e-orphan run 624 SUCCEEDED (test_referential_orphan),
    e2e-idempotent-...-1 run 666 RUNNING (test_smoke_and_idempotency::
    test_idempotent_reupload -- the LAST test file in tests/e2e/slice). pytest was
    mid-final-module at cancel; the observability and rebuild-from-raw workflow steps
    never started.
    (v) THE 45-MIN SINK: customers DagRun dump (31 total, 29 success, 2 failed, 1
    running at cancel): backfill__2026-08-27T08:40 (backfill_id=5 = the mass-delete
    test's offset_minutes=150/span=0 window; test source confirms it EXPECTS a
    deliberate truncated-snapshot QualityThresholdExceeded trip and a hard FAILED run)
    ran 11:10:53->11:55:54 failed at +45:00 with publish state=skipped try=6
    (11:42:20->11:42:49); COLLATERAL: cron run scheduled__11:09 (started 11:09:42,
    discover/stage/dbt_build all success try=1 by 11:12:15) had publish FAIL try=7
    (last try 11:53:31->11:54:00.02), DagRun failed 11:54:00.7 -- the truncated
    snapshot entered the cron discovery window too. Cron gap 11:09->11:53 (next
    scheduled run 11:53 started 11:54:01) = max_active_runs=1 held 44min, stalling all
    later cron-dependent waits. Job-log greps for QualityThresholdExceeded/vanished =
    0 hits because task-pod logs never reach the job log; the mechanism is the test's
    own documented design + TI shape (publish-only failure, 6-7 deterministic tries,
    both runs in the same window). Zero OTHER failed TIs anywhere: 187 customers TIs =
    success/skipped/removed except these two publish chains.
    (vi) FAILEDSCHEDULING (guard flag): burst 10:42:22-10:47:08, 7 unique pods (4
    stage, 2 publish, 1 discover) 'Insufficient cpu... no preemption victims';
    first-ever customers+orders co-scheduling peak. Self-healed: backfill_3's first
    stage tries state=removed at 10:45-10:46, re-ran try=2 to success from 10:54;
    no test failed from it. Event lines aged out by 11:47 (1h TTL); no NEW FS pods
    after 10:47.
    (vii) OTHER GUARDS: Kyverno DENY 0; restart timeline EMPTY (0 restarts all
    roles); scheduler peak 1972506624 bytes = 1881MiB = 91.9% of the 2048Mi limit,
    26 pids (R12: 1415MiB = 69%) -- new high-water under orders-live load, 167MiB
    headroom; dag-processor 771MiB; triggerer 419MiB. Fix (16) held: zero unwanted
    breaker trips; silver _run_id distribution {409:1, 460:49} deterministic; run
    460 = 50 bronze rows / 49 distinct keys published clean.
    (viii) PACE: steady-state cron DagRuns 2.3-3.7min each (R12: 96-104s) = the
    permanent orders co-scheduling tax; 9 clean steady-state runs 11:54->12:18:49+
    after the poison cleared, all success.
    (ix) CENSUS: unmeasurable AGAIN (pytest -q, zero flushed output in a cancelled
    job -- 3rd blind round). Companion runs already pre-cleared at recording time:
    publish.yml 33062702191 success (images at 4d3db56); CI 33062702164 failure =
    same 2 pre-existing out-of-scope jobs as the 794db33 baseline.
  implication: >
    Root cause (17) is CLOSED: unpaused orders triggers, runs, evolves schema,
    drains, and re-runs on demand on ephemeral CI. No new hidden mechanism appeared
    beneath it -- the remaining gap is pure BUDGET ARITHMETIC plus one priced design
    defect: (18a) deterministic-breaker-trip retrying cost a measured ~40min of the
    120 (designed ~5min vs actual ~45min window), and the honest as-is job needs
    ~2h35m-2h50m (pytest ~2h10m + unmeasured observability/capstone steps ~25-40min);
    with the collateral trimmed, ~1h55m-2h10m. The deferred timeout-minutes decision
    is now LIVE: raise to 180 as-is, or trim the collateral (test-scoped fixture
    hygiene or production quarantine semantics -- needs explicit user approval) plus
    150, or both. Secondary watches: (18b) transient Insufficient-cpu bursts and
    scheduler at 91.9% of its memory limit under doubled load; pytest progress
    observability now blocking census measurement 3 rounds running.

- timestamp: 2026-08-27 (ROUND 14 -- Option C implemented, offline battery green)
  checked: "Implemented the user-chosen Option C (trims i+ii+iii + timeout 150 + -v rider)
    and ran the full offline battery. Key mechanism facts established by source read BEFORE
    implementing (all recorded in Current Focus ROUND 14):
    (i) discovery has NO time-window filter -- discover_files lists the whole
    config.source prefix every call and re-offers every non-SUCCEEDED run; trim i's letter
    ('no cron run ever sees the fixture') is therefore structurally unachievable; the
    safer-subset resolution (user's own rule) routes the fixture through ONE
    quarantining pass instead (complementarity_and_trim_i_letter_conflict block).
    (ii) publish_ingest claims ALL currently-STAGED runs per dataset
    (list_staged_run_ids has no other filter) -- a tripped pass left STAGED re-poisons
    every later pass; terminal quarantine is the only shape that removes the poison.
    (iii) errors.py ALREADY declares the classification line: PublicationError's docstring
    distinguishes 'deliberate business-rule rollback' (QualityThresholdExceeded) from
    infrastructure failure -- trim ii operationalizes the hierarchy's own boundary.
    (iv) meta.ingestion_runs.status is sa.Text with NO CHECK constraint (migration 0004)
    -- the new QUARANTINED value needs no migration.
    (v) ROUND-13-log archaeology resolving the run-409 puzzle: the truncated snapshot's
    run 409 ended SUCCEEDED because the 11:53 cron pass co-staged leftover
    replay-eligible corpus runs whose union covered the roster (vanished below
    threshold) -- live proof of the union-healing dynamic AND its fragility; terminal
    quarantine deliberately trades that away for determinism (recorded in the design
    note).
    (vi) The e2e single-partial-file fixtures (e2e-idempotent/dbtkill/u3/rebuild) violate
    the snapshot-delivery shape the sweep module's own docstring warns about ('a lone
    single-row file would make DELETE-detection wrongly treat every other roster member
    as vanished') -- pre-registered as candidate (19) with its predicted post-ROUND-14
    signature (legible QUARANTINED instead of 45-min wedges), deliberately NOT expanded
    into this round's scope."
  found: "CHANGES: (1) e2e-full.yml timeout-minutes 120->150 (measured-arithmetic comment).
    (2) Makefile cluster-slice-verify + observability-verify-ci pytest -q -> -v (CI-only
    targets; -q dots never complete a line so cancelled jobs showed zero output 3 rounds
    running; -v emits newline-terminated per-test lines that survive cancellation).
    (3) dataplat/pipeline/run.py: publish_ingest catches QualityThresholdExceeded ONLY --
    marks every run of the tripped pass QUARANTINED via update_ingestion_run_status,
    logs breaker context, returns {'status': 'QUARANTINED', runs_quarantined, reason}
    (CLI writes it to XCom and exits 0 -- no csv_processor change needed); success path
    moved to try/else (ruff TRY300); module + function docstrings document the carve-out.
    (4) discovery.py: new _TERMINAL_NON_REOFFERABLE_STATUSES = {SUCCEEDED, QUARANTINED}
    applied at both skip sites (ungrouped + multipart), decision label 'QUARANTINED'.
    (5) kpo.py publish_retries() (Variable publish_retries, default '6' = pre-fix
    literal); customers publish retries=publish_retries() (net-zero lines, file stays
    208); ci-set-workload-images.sh sets publish_retries=3 (transient-class sizing
    comment: 4 attempts over ~12min vs the measured ~5min burst).
    (6) test_backfill_2year_sweep.py: mass-delete test rewritten -- NO backfill (cron
    delivery is the designed path), expects terminal QUARANTINED, gold-unchanged
    assertion kept, module docstring REDESIGNED paragraph; _TERMINAL_RUN_STATUSES +=
    QUARANTINED.
    (7) NEW tests/integration/test_scd_replay_delete_detection.py::
    test_breaker_trip_quarantines_the_pass_while_transient_errors_still_raise -- fresh
    PG18 + real dbt: PHASE 1 trip (35/50 keys = 30% > 0.10 through publish_ingest's REAL
    pass-claiming path) -> QUARANTINED return + terminal statuses + list_staged empty +
    gold byte-identical + follow-up publish clean no-op; PHASE 2 PublicationError from a
    monkeypatched publisher -> propagates, runs stay STAGED (retry budget intact).
    (8) NEW tests/unit/test_discovery.py::test_discover_files_never_re_offers_a_
    quarantined_run (mirrors the SUCCEEDED-exclusion test; also asserts no
    replay_of_run_id ever points at a quarantined run).
    OFFLINE BATTERY ALL GREEN, zero new regressions: tests/unit 556/556 (was 555, +1 new);
    tests/policy -m 'not manifests' 157 passed / 2 failed -- the SAME 2 pre-existing
    out-of-scope failures (customers line budget 208>158 A/B-confirmed unchanged at 208
    both sides; test_gates_actually_fail); tests/dagtest 14/14; make manifests
    kubeconform -strict 540/0/0; test_manifest_resources 5/5; test_values_profiles 6/6;
    sweep module collect-only 7/7 same order; replay integration file 2/2 (one in-round
    defect caught by the battery itself and fixed: the gold-snapshot query referenced a
    nonexistent valid_from column -- normalized.customers' event_ts doubles as
    valid_from per migration 0035); publish-path integration A/B: failing set
    BYTE-IDENTICAL to clean HEAD (5 of the known 21 pre-existing local-env failures,
    diff empty); test_publish_scd 7/7; ruff clean on touched files (2 remaining findings
    = pre-existing untouched sweep line 1075, stash-A/B-confirmed; run.py format drift
    pre-existing byte-identical on HEAD); mypy dataplat clean, DAG files 71 errors both
    pre/post (identical pre-existing idiom), test files identical-class pre-existing
    type-ignore idiom; DagBag per-profile proof EXACT: no Variable -> customers publish
    retries=6 (byte-identical to pre-fix), AIRFLOW_VAR_PUBLISH_RETRIES=3 -> 3, orders
    publish 3 and stage 6 untouched in both. Accepted cosmetic residue:
    meta.v_run_recovery's next_action CASE says 'retry stage PUBLISH' for a QUARANTINED
    run (view predates the status; run_status column exposes QUARANTINED alongside, so
    the state is legible -- no migration this round)."
  implication: "ROUND 14 fix (18) is implemented and offline-verified end-to-end,
    including a genuine both-branches classification proof (trip -> quarantine;
    infrastructure error -> propagate + stay STAGED). Ready for live CI verification
    against the ROUND 14 pre-registered criteria (a)-(f) + candidate (19)."

- timestamp: 2026-08-27 (ROUND 14 post-run -- run 33080823061 analysis, first legible census)
  checked: "Full job log of e2e-full.yml run 33080823061 (headSha a247b67, conclusion
    CANCELLED at the NEW 150-min ceiling: 14:11:06Z -> 16:42:07Z = 2h31m01s, cancel
    signal in-log 16:41:24; companions same headSha: publish.yml 33080823116 SUCCESS,
    CI 33080823102 = the pre-existing Quality-gate+Integration pattern only). Analyzed:
    the NEW pytest -v per-test result lines (the rider worked -- newline-terminated
    lines survived cancellation), the always()-diagnostics (ROUND 5 DagRun/TI dumps,
    ROUND 10 FailedScheduling census + etl-monitor rolling capture, ROUND 12
    run->file + silver _run_id dumps, MinIO listing), and the cluster-up
    fix-in-force probes."
  found: >
    (A) FIX-IN-FORCE: 'registering publish_retries=3' 14:18:29 + 'Variable
    publish_retries created' 14:18:36 in cluster-up; publish.yml SUCCESS = quarantine
    semantics in-image (criterion 0 MET).
    (B) CENSUS (criterion e MET -- first measurable census in 4 rounds). Cluster module
    15 PASSED / 6 SKIPPED, 20s total, zero failures. Against the 17-test baseline:
    7 PASSED = test_no_extra_schemas_exist (14:19:07), test_pilot_window_drains_
    without_cpu_starvation (16m44s), test_idempotent_rerun_produces_zero_additional_
    rows (10m00s), test_live_run_concurrent_with_backfill_same_dataset (7m56s),
    test_mass_delete_snapshot_trips_circuit_breaker... (2m12s), test_scd_concurrent_
    attribute_change_and_correction_same_key (29m57s), test_smoke_dag_xcom_contains_
    built_sha (37s). 9 FAILED = test_full_2year_sweep (11m51s, 14:48:31, first slice
    failure), test_backfill_resolves_previously_rejected_row (4m41s),
    test_concurrent_select_never_observes_partial_publish (13m24s),
    test_fresh_customers_file_flows_through_stage_dbt_build_publish (7m50s),
    test_pod_kill_mid_load (7m58s), test_pod_kill_mid_dbt_build (7m26s),
    test_u3_throughput_and_peak_rss_baseline (12m41s), test_rebuild_from_raw (5m06s),
    test_orphan_order_quarantined_while_valid_rows_publish (2m35s). 1 NEVER FINISHED =
    test_idempotent_reupload (in flight at cancel). ZERO new failing node-IDs vs
    baseline -- the quarantine change introduced no regression at test level.
    (C) MASS-DELETE / QUARANTINE PATH (criteria b+c MET): run 421 is the ONLY
    QUARANTINED row in the run->file dump (customers_20240114.csv, uploaded 15:06:28,
    claimed by cron scheduled__15:04, quarantined terminally within ~2.5min; that cron
    SUCCEEDED 15:09:06 in 3m39s -- the designed shape exactly; ROUND 13's same fixture
    cost ~40min of collateral + a 45min backfill death). 53 csv_ingest_customers cron
    DagRuns wall-to-wall 14:19->16:42, inter-run gap ~1s, zero wedges, zero +45:00
    dagrun_timeout deaths, publish state=success try=1 in every run. Two isolated cron
    failures (scheduled__15:42, scheduled__15:55) died <2min each at mapped
    integrity_gate (wait_for_files success try=1; discover/stage/dbt_build
    upstream_failed try=0), zero knock-on gap -- NOT mass-delete-related; watch item
    (integrity_gate is absent from the TI-dump task list -- diagnostics gap).
    (D) GUARDS (criterion d): Kyverno DENY 0; restart timeline empty (0 control-plane
    restarts); scheduler peak 1852981248B = 1767MiB = 86.3% of 2048Mi (below R13's
    1881MiB/91.9%); FailedScheduling: ONE burst, 11 unique pods (stage/discover/
    dbt-build/publish of the pilot+sweep co-scheduling window 14:30-14:42), pendings
    up to ~9min, all reached Running/Succeeded; the rolling etl-monitor census shows
    the burst aging out (event ages 0s->60m across samples, pod-name set FIXED at 11)
    with ZERO fresh FailedScheduling 14:42->16:41. CAVEAT: the burst overlaps
    test_full_2year_sweep's window and that test FAILED -- 18b cannot be credited
    'self-heals without test failures' this round.
    (E) FINDING (20) EVIDENCE CHAIN: run->file dump shows EVERY e2e single-file
    CUSTOMERS run terminal-state RUNNING with zero bronze rows: runs 823
    (e2e-backfill-original), 863 (e2e-concurrent-select), 959 (e2e-dbt-silver),
    1015 (e2e-podkill), 1071 (e2e-dbtkill), 1127 (e2e-u3), 1211 (e2e-rebuild-original)
    -- while e2e-orphan (ORDERS, run 1212) SUCCEEDED try-count 3. staging.customers
    per-run dump: NO rows for any of the 7. etl-monitor captured stage pods
    phase=Failed at 16:00:08 (stage-tabyhjge) and 16:15:34 (stage-1q9yvapa) -- exactly
    the try-1 windows of the 15:57/16:12 crons' stage TIs; TI dump shows those crons'
    stage try=2 state=success (retry landed inside the crashed try's still-live 5-min
    lease -> SKIPPED_CONCURRENT receipt -> task SUCCESS with nothing staged) and the
    DagRuns SUCCEEDED. End-of-run MinIO listing: only 2 e2e objects remain (both from
    the final minutes) -- teardown deleted each failed test's fixture, orphaning the
    RUNNING rows permanently. IDENTICAL signature exists in ROUND 13's dump
    (pre-quarantine image 4d3db56: runs 461/515/623/666 RUNNING) -- PRE-EXISTING, not
    a fix-(18) regression. Inner exception NOT capturable this run (pytest prints
    tracebacks only at session end; pod logs died with the cluster).
    (F) TIMELINE DECOMPOSITION (criterion a FAILED): setup 7.7min (14:11->14:18:47);
    cluster module 20s; sweep module 79.5min (dry_run 49s P, pilot 16m44s P, sweep
    11m51s F, idem_rerun 10m00s P, live_run 7m56s P, mass_delete 2m12s P, scd
    29m57s P); post-sweep singles 62.3min (8 FAILED via (20) mechanism + orphan
    2m35s F + smoke_xcom 37s P); cancel with test_idempotent_reupload in flight;
    observability step + rebuild-from-raw capstone NEVER STARTED. Passing tests
    ~68min, failing ~73.5min. Honest as-is total ~3h03m-3h20m.
  implication: >
    Fix (18) is LIVE-CONFIRMED IN FULL and CLOSED -- the (18a) mass-delete sink is
    eliminated (2m12s designed-shape PASS vs ~40min collateral), zero cron collateral,
    quarantine path exercised exactly where designed, zero new failing node-IDs.
    Candidate (19) did NOT fire (zero e2e QUARANTINED -- the wedge is UPSTREAM of
    publish; stays latent/carried). The budget criterion failed for a NEW named
    mechanism, finding (20): stage-phase claim-then-crash + retry-inside-lease
    silently drops every e2e single-file customers fixture (7 tests, 73.5min of
    wait-timeout burn), pre-existing since at least R13 and only now unmasked by the
    census. Two design gaps stand regardless of (20)'s inner exception: (20a) a retry
    that lands inside a crashed try's live lease reports task SUCCESS with zero rows
    staged (silent drop -- core-value violation; nothing ever writes FAILED to release
    the claim); (20b) silver.customers retains _run_id=421 from the QUARANTINED run
    and publisher.publish upserts the whole silver table with no _run_id filter --
    terminal quarantine currently blocks the PASS, not the DATA. Even green, the
    suite projects ~158-188min: 150 does not hold. Decision checkpoint returned
    (LOCAL repro of (20) +/- timeout raise +/- sweep-failure adjudication)."

- timestamp: 2026-08-27 (ROUND 15, offline root-cause + local repro)
  checked: "Finding (20)'s inner exception via direct source reads + a wired local
    reproduction through the REAL CsvSource.inspect() chain (testcontainers
    PostgreSQL+MinIO, tests/integration/test_schema_resolution.py new D-13 section)."
  found: "ROOT CAUSE (20) CONFIRMED RED-FIRST: IncompatibleSchemaError: column
    'signup_country' present in the recorded schema disappeared from this file
    (diagnostic_code schema-column-disappeared), raised by
    dataplat.schema.evolution.classify_schema_change during
    CsvSource._resolve_schema inside inspect() -- which stage_ingest reaches only
    AFTER claim_ingestion_run commits RUNNING + the 5-min lease (source.open() calls
    inspect(); StagingLoader.load() calls source.open()). Chain: (i) ALL wedging e2e
    fixtures are 5-column customers files -- tests/fixtures/slice-corpus.yaml declares
    customers_small/large with header [customer_id,name,country,birth_date,event_ts]
    (lines 59/219) and test_backfill_reentry/test_rebuild_from_raw's
    _build_customers_csv hand-writes the same 5-col header; the sweep corpus
    (dated_series, plan 10-06) always emits all 6 columns -- exactly why corpus files
    staged fine minutes earlier in the same CI runs. (ii) customers.yaml gained
    signup_country (required: false, D-13, plan 10-01/migration 0035) with the comment
    'files delivered before this column existed never carried it ... not reject files
    missing it' -- but classify_schema_change has NO optional-column concept: any
    contract column absent from the observed header raises BREAKING. PRE-EXISTING
    since Phase 10 landed; customers-only (orders e2e-orphan carries orders' full
    header). (iii) test_stage_ingest.py's own 10-07 constant comment documents the
    SECOND leg: a 5-wide row against the 6-wide _TARGET_COLUMNS_BY_DATASET
    desynchronizes the positional COPY -- the harness widened its TEST fixture and
    deferred the loader fix. (iv) DOC CONFLICT: config/model.py's ColumnContract
    docstring said required:False absence is 'classified breaking' -- under that
    reading required would be behaviorally dead; customers.yaml's D-13 comment and the
    field's own definition contradict it. (v) (20a) confirmed at source:
    csv_processor.cli.stage's except branches write a FAILED receipt and re-raise but
    NEVER touch meta.ingestion_runs -- the run stays RUNNING under the live lease;
    claim predicate (postgres.py) refuses the retry; _skipped_receipt maps RUNNING ->
    SKIPPED_CONCURRENT -> exit 0 -> task SUCCESS with zero rows staged. RED tests
    confirmed both shapes exactly (IncompatibleSchemaError on the 5-col file;
    status RUNNING + silent-success on the crash/retry path)."
  implication: "The fix belongs at the schema-classification + loader layer (the
    contract ALREADY declares the intended semantics; the code never implemented it),
    plus the claim lifecycle (crash release + honest wait). Not a test-side fixture
    widen: 5-col files are D-13's real production case (files predating a column)."

- timestamp: 2026-08-27 (ROUND 15, offline fix + battery)
  checked: "Implemented fixes (20)+(20a), riders, timeout raise; full offline battery."
  found: "ALL GREEN, red->green proven. FIX (20): (a) classify_schema_change gains
    keyword-only optional_columns (hashed column mappings deliberately untouched --
    hash_schema hashes whole dicts); (b) CsvSource._resolve_schema passes
    {c.name for c in config.columns if not c.required}, adds a contract-prefix-intact
    guard on the INFERRED/new-column path (closes a latent positional-corruption hole:
    contract-present-but-new-column-non-trailing used to sync INFERRED then corrupt
    positionally), and resolves a narrower strict-prefix-with-optional-tail header to
    the CONTRACT version via resolve_by_hash (StorageError fallback sync CONTRACT) --
    NEVER sync(INFERRED), which would flip CURRENT and re-key every file (R12's
    replay-wave mechanism as a permanent oscillation); (c) StagingLoader gains
    _OptionalColumnPadStage right after RaggedRowGuard (ragged detection stays
    file-relative; normalizers indexing the absent trailing column -- live-caught
    RED: boolean_null.py IndexError -- read None) + defense-in-depth pad at the hash
    site (padded row hashes identically to full-width-with-empty-optional); (d)
    model.py ColumnContract.required docstring corrected. FIX (20a): LEG 1 --
    fail_ingestion_run_claim(run_id, pod_name) repository method (guarded UPDATE
    RUNNING+same-pod -> FAILED + lease expired), called best-effort from
    stage_ingest's finally when run_status=='failed' (never masks the in-flight
    exception; lease expiry stays the SIGKILL backstop); LEG 2 -- claim-refused path
    now waits (_await_concurrent_claim, concurrent_wait_seconds default 420s, CLI env
    DATAPLAT_STAGE_CONCURRENT_WAIT_SECONDS): STAGED/SUCCEEDED -> verified
    SKIPPED_DUPLICATE; claim recovers (lease expiry or LEG 1's FAILED) -> genuine
    re-stage; budget exhausted -> DataPlatformError (task FAILS; Airflow backoff
    re-enters later). SKIPPED_CONCURRENT is no longer a possible stage_ingest return.
    RIDERS: tests/e2e/conftest.py pytest_runtest_logreport streams every failure's
    longreprtext immediately (OBS-03 carve-out 3 added DELIBERATELY in pyproject +
    test_print_ban_scope.py allowlist -- the hook's output IS the streamed CI log);
    integrity_gate added to e2e-full.yml's TI dump; timeout-minutes 150 -> 190.
    TRUTH-UP (budget-critical): _TERMINAL_RUN_STATUSES in slice+observability
    conftests gains QUARANTINED (terminal since R14 trim ii; sweep module's local set
    already had it) -- without it, candidate (19) firing post-(20)-fix would burn full
    poll timeouts on already-decided QUARANTINED runs. BATTERY: unit+regression
    560 pass; dagtest 14 pass; policy 157 pass + exactly the 2 known pre-existing
    failures (dag_line_budget, gates_actually_fail); make manifests + kubeconform
    valid (378/0/0); mypy strict full 91 files clean; ruff check clean on every
    touched file (repo residual = 4 pre-existing in untouched files); integration:
    test_stage_ingest (7, incl. 4 new red->green), test_schema_resolution (19, incl.
    3 new), test_run_ingest (rewritten behavior-3), test_claim_lease_split,
    test_staging_loader/durability/quality_rules, test_publish_ingest,
    test_scd_replay_delete_detection, test_scd_delete_detection,
    test_dbt_silver_dedup -- ALL PASS (74 total). Pre-existing out-of-scope offline
    failures unchanged (test_staging_normalization target-columns mismatch, confirmed
    identical on bare HEAD via git stash)."
  implication: "PRE-REGISTERED PREDICTION for the live run: candidate (19) is now
    EXPECTED TO FIRE -- with (20) fixed, each lone e2e customers fixture stages and
    its publish pass computes vanished = every bronze-known gold-current key absent
    from the pass (find_vanished_customer_ids source-read: lone-file pass vs the
    sweep's roster -> ratio ~100% >> 0.10) -> terminal QUARANTINED. The customers e2e
    tests should therefore fail FAST and LEGIBLY (QUARANTINED surfaced by the
    now-terminal poll + streamed tracebacks) instead of burning 73.5min of
    wait-timeouts -- (20)'s mechanism (stage wedge, orphaned RUNNING, silent drop)
    must be ABSENT either way. If they instead SUCCEED, their passes were
    union-covered; if they wedge RUNNING again, (20)'s fix is refuted. (19) remains
    the user's design decision (delivery-shape contract vs breaker scoping) -- this
    run supplies its definitive live evidence."

- timestamp: 2026-08-27 (ROUND 15 post-run -- run 33103279876 analysis, first self-terminating run)
  checked: "Full job log of e2e-full.yml run 33103279876 (headSha 25b6eb0, conclusion
    FAILURE, 18:24:15Z -> 20:08:27Z = 1h44m12s, job 98626355323, 10,603 lines saved to
    scratchpad round15-job.log; companions same headSha: publish.yml 33103279760
    SUCCESS, CI 33103279751 pre-existing pattern only, chaos 33103279815 observational).
    Analyzed: the streamed per-failure tracebacks (ROUND 15 rider, first run where
    every failure carries its WHY inline), the -v census, the always()-diagnostics
    (DagRun/TI dumps now incl. integrity_gate, FailedScheduling census, etl-monitor,
    run->file + schema_versions + silver _run_id + staging per-run dumps),
    cp-monitor peaks."
  found: >
    (A) CENSUS (first COMPLETE one): 28 passed / 10 failed / 6 skipped in 5795.05s
    (1:36:35); every one of the 17 baseline node-IDs finished; zero new failing
    node-IDs. 7 PASSED unchanged from R14 (no_extra_schemas 19:07 window, pilot
    13m43s, idem_rerun 9m05s, live_run 7m44s, mass_delete 1m51s, scd 8m54s [R14:
    29m57s], smoke_xcom 39s). 10 FAILED with streamed assertions: sweep 10m31s
    ("silver.customers has no rows for _run_id=42"), reentry 2m53s / concurrent
    5m06s / dbt_silver 3m22s / podkill 10m45s / u3 5m27s / rebuild 3m29s /
    idem_reupload 1m29s (all "'QUARANTINED', not SUCCEEDED"), dbtkill 7m11s
    ("meta.run_stages[run_id=668,'DBT_BUILD'] never reached RUNNING within 300s
    (last observed: None)"), orphan 3m12s (psycopg.errors.InsufficientPrivilege:
    permission denied for schema normalized, as analytics_owner, test line 255).
    USER-OBSERVED "test_partial_batch_lifecycle" MATCHED: no such node-ID exists;
    the observed mid-run AssertionError was
    test_concurrent_select.py::test_concurrent_select_never_observes_partial_publish
    at 19:32:23 ("run finished 'QUARANTINED', not SUCCEEDED") -- the only baseline
    node-ID containing "partial".
    (B) FIX (20) LIVE-CONFIRMED: staging.customers shows every 5-col e2e fixture
    STAGED -- 544:3, 584:1,000,000, 612:120, 640:1,000,000, 668:120, 736:1,000,000,
    764:3, 809:120 rows, distinct_keys == bronze_rows in every case (R14: zero rows
    for all of them). run->file dump: ZERO runs in status RUNNING except the
    in-flight cron at dump time; zero orphans. grep 'SKIPPED_CONCURRENT': 0 hits.
    (C) FIX (20a) LEG 2 LIVE-EXERCISED: podkill's real SIGKILL of stage-ajbgjldq
    mid-1M-row-load left a live lease; the Airflow retry (scheduled__19:36 stage[0]
    try=2, 19:39:31->19:45:00 = 5m29s) waited out the lease via
    _await_concurrent_claim, reclaimed, and genuinely re-staged 1M rows exactly once
    (bronze distinct 1M, test's no-duplicates property held in data; the test failed
    only on (19)'s QUARANTINED terminal). The old signature (try=2 instant SUCCESS
    with zero rows) is gone.
    (D) PREDICTION (19) FIRED EXACTLY: 8/8 lone-customers e2e runs terminal
    QUARANTINED (544/584/612/640/668/736/764/809); e2e-orphan ORDERS run 765
    SUCCEEDED (no customers-snapshot vanished check on orders). Failures were fast
    (1.5-10.5min) and legible; total failure burn ~43.5min vs R14's 73.5min of
    timeouts. Design decision returned, not a (20) refutation.
    (E) SWEEP ADJUDICATED (R14's blind failure, now finding 21): FIRST-EVER full
    drain on CI -- both dataset waits completed (all customers+orders corpus files
    terminal SUCCEEDED incl. days 12/13 first-staged as runs 47/48), assertions 1-3
    PASSED (scoped orders counts exact at 600, gap-day absent everywhere +
    processing_gaps 0 as designed, pre/post schema_version_ids differ). Assert 4
    (D-05 late-row lineage) failed: run 42 (newest replay of customers_20240108,
    replay_of=7, SUCCEEDED, 50 bronze rows) has zero silver rows attributed at
    18:56:50. End-of-session silver _run_id census: {397:1, 450:49, 544:3,
    584:1M, 612:120, 640:1M, 668:120, 736:1M, 764:3, 809:120} -- NO run 1-60
    holds any silver attribution by session end. Data correct, attribution moved;
    needs local adjudication against the silver dedup tie-break (fix 16c) + replay
    timing. NOT a silent drop (counts exact).
    (F) GUARDS: Kyverno DENY 0 (the only admission denial is the deliberate
    unsigned-image test, PASSED); restart timeline EMPTY (0 restarts all roles);
    scheduler peak 2016055296B = 1923MiB = 93.9% of 2048Mi -- NEW HIGH-WATER (18b
    watch: ~125MiB headroom); dag-processor peak 854MiB; FailedScheduling ONE
    transient burst 18:45:31-18:49:54 (~10 unique pods, pilot/sweep co-scheduling,
    Insufficient cpu, pendings to ~9min, aged out with zero fresh events after +
    one pod 19:19:05) -- ZERO test failures attributable (pilot PASSED through its
    own burst this time, unlike R14's caveat). 35 customers DagRuns wall-to-wall
    18:32->20:05 zero gaps zero +45:00 deaths; 679 key TIs = 636 success / 36
    skipped / 1 failed / 3 upstream_failed / 2 removed / 1 running-at-dump. Fixes
    16/17/18 all hold: idem_rerun's backfill_3 re-publish try=2s all success
    (designed D-18 replay), orders 25-60 + replay 49-60 all SUCCEEDED via live
    asset cascade, mass_delete 1m51s designed-shape with run 397 the only corpus
    QUARANTINED.
    (G) INTEGRITY_GATE FLAKE CLASS ADJUDICATED (R14 watch item, rider delivered):
    the session's ONLY failed TI is scheduled__19:51 integrity_gate[15], failing
    19:53:37->19:53:41 -- the exact second dbtkill's teardown deleted its fixture
    (test failed 19:53:41). Teardown-deletes-file-mid-check race; the gate
    correctly failed a file that vanished under it; zero knock-on (next cron
    success 19:54). R14's two isolated <2min cron failures are the same class.
    (H) TIMELINE: setup 7.1min; pytest 96.6min; diagnostics 30s; total 1h44m12s =
    45% of the 190 ceiling. Observability + rebuild-from-raw capstone steps
    SKIPPED (gated on suite green). Green projection: ~100-110min suite +
    obs/capstone, comfortably inside 190.
  implication: >
    Fixes (20)+(20a) are LIVE-CONFIRMED and CLOSED -- the stage-wedge/silent-drop
    mechanism is eliminated at both layers, proven by the hardest case available (a
    real mid-load SIGKILL on a 1M-row file re-staging exactly once). The budget
    criterion is finally MET with a self-terminating run. The remaining distance to
    green is fully enumerated: (19) design decision owns 8 failures; (21) sweep
    lineage, (22) normalized-schema grant, (23) dbtkill instrumentation own one
    each; (20b) is now 3M rows of quarantined-run silver residue. Decision
    checkpoint returned on (19) + residuals.

- timestamp: 2026-08-28 (ROUND 16 offline -- full-scope 19-A+21+22+23+20b implemented, battery green)
  checked: "Source adjudication + implementation + offline battery for all five items.
    Adjudication reads: dbt/models/silver/*.sql + both post-hook macros (watermark
    mechanism), tests/e2e/slice/* (all 8 failing tests' mechanics),
    scripts/rebuild-from-raw.py (orders rebuilds via asset cascade, backfill probe
    skips asset DAGs), load/publish/{scd,merge,merge_orders}.py + scd/delete_detection.py
    (quarantine leak paths + vanished scoping), migrations 0019/0038 (grant history),
    airflow/dags/_common/run_stage_recorder.py (eligibility-query wiring),
    round15-job.log traceback lines 1300-1375 (sweep assert 4 exact shape)."
  found: >
    ALL FIVE ITEMS IMPLEMENTED, red->green where feasible. (21) REAL BUG CONFIRMED
    RED->GREEN: new tests/integration/test_dbt_silver_out_of_order.py reproduces the
    exact shape (run A id<B staged after B built; pre-fix silver never gets A's key --
    RED verified against stashed pre-fix models; GREEN with fix). FIX: migration 0040
    meta.dbt_processed_runs claim ledger + dbt/macros/claim_dbt_processed_runs.sql
    (pre-hook INSERT, same transaction) + both silver models filter on claimed_txid =
    txid_current() (NEVER a Jinja invocation_id -- reconciliation_post_hook's own
    documented partial-parse stale-literal hazard) + both post-hook macros scope
    new_bronze/bronze_files to the same claimed set (their old max(max_run_id) audit
    floor shared the bug); bronze _run_id btree indexes added (FK never auto-indexed).
    Rider adjudicated while regressing: test_dbt_reconciliation's `discrepancy == 0`
    only ever held because the old {{ this }}-scoped watermark DROPPED when the
    harness cleanup deleted max-holding silver rows (accidental re-materialization);
    rescoped to build-local balance with the mechanism documented in-test.
    (22) MECHANISM PINNED: 0019 granted analytics_owner TABLE SELECTs on
    normalized.* but schema USAGE was never granted (0038's exact meta-schema
    precedent; its docstring even flagged 0019 as 'covered some other way,
    unconfirmed' -- the other way was local hand-state). FIX: migration 0039 GRANT
    USAGE ON SCHEMA normalized TO analytics_owner.
    (23) INSTRUMENTATION BUG CONFIRMED RED->GREEN: list_run_ids_pending_dbt_build had
    NO upstream edge -> scheduler ran it at DagRun start -> a run staged by its own
    DagRun never got a DBT_BUILD row until the NEXT DagRun (668's row landed after the
    300s poll). FIX: `stage >> pending_run_ids` in wire_dbt_build_tracking + new
    test_dag_structure assertion (verified RED on stashed pre-fix wiring).
    (20b) LEAK PATHS CONFIRMED RED->GREEN: new
    tests/integration/test_publish_quarantine_exclusion.py proves (i) SCD Step C's
    unscoped bronze-history read folded a QUARANTINED run's row into the gold chain
    and (ii) merge_orders' whole-silver upsert published a quarantined run's silver
    row (both RED on stashed pre-fix publishers). FIX: `_run_id NOT IN (SELECT run_id
    FROM meta.ingestion_runs WHERE status='QUARANTINED')` in scd/_BRONZE_HISTORY_SQL +
    merge.py + merge_orders.py _PUBLISH_SQL (NOT-IN shape: metadata-less harness rows
    stay included; operator re-open re-includes automatically) + migration 0041
    meta.v_quarantined_artifacts (identifiability view, grants to
    etl_app/analytics_owner/grafana_reader) + ADR-0012 recording the deferred silver
    disposition (dbt-side status visibility + displaced-key re-materialization).
    (19)-A IMPLEMENTED AS ADJUDICATED: orders repoint for the dataset-agnostic five
    (concurrent_select 250k rows, podkill 1M, dbtkill 120, u3 1M, idempotent_reupload
    120 -- in-test generated fixtures via conftest build_orders_csv_bytes with
    live-sampled normalized.customers parents, manual `airflow dags trigger
    csv_ingest_orders` per the orphan test's proven idiom, disjoint random order_id
    windows); snapshot-complete customers fixtures for the three that structurally
    need the cron DAG (reentry + rebuild need `airflow backfill create` --
    DagNonPeriodicScheduleException on asset DAGs, rebuild script's own probe;
    dbt_silver keeps the v_customers_lineage assertions) via conftest
    snapshot_complete_customers_csv (echoes the breaker's exact denominator roster --
    _CURRENT_COUNT_SQL scoping -- so vanished==0 by construction; echoed rows carry
    current values at current event_ts -> no new SCD versions). Raw uploads of
    published data are no longer deleted (section 63/ADR-0011 alignment; rebuild's
    D-29 compare is only coherent with a complete raw history), and
    test_rebuild_from_raw gained _wait_for_all_raw_files_settled for BOTH datasets
    before its post-rebuild snapshots (orders rebuilds via the ASSET CASCADE, so the
    old snapshot-right-after-customers-backfill raced it; duplicates carve-out
    included). 18b: scheduler memory limit 2048Mi->2560Mi in helm/values/ci
    (requests untouched; justified in-file -- R15 peak 93.9% + this round makes 1M
    runs succeed + rebuild capstone reprocesses the retained raw mass).
    BATTERY: unit+regression 560 pass (incl. new stage>>pending structure assert);
    dagtest 14 pass; policy 157 pass + exactly the 2 known pre-existing failures;
    make manifests + kubeconform valid (378/0/0, incl. the bumped scheduler limit);
    mypy strict 91 files clean; ruff clean on all touched files (repo residual = 3
    pre-existing in untouched files); integration battery 92 pass across
    test_dbt_* (incl. new out_of_order), test_migrations (EXPECTED_TABLES +
    dbt_app/grafana grant surfaces truthed up for 0040/0041), test_publish_scd,
    test_publish_quarantine_exclusion, test_scd_*, test_stage_ingest,
    test_schema_resolution, test_run_recovery_view, test_claim_lease_split;
    slice suite collects 17 node-IDs clean. Pre-existing out-of-scope offline
    failures unchanged (test_publish_orders staged_run_ids TypeError x3, confirmed
    identical on bare HEAD via git stash).
  implication: >
    All five ROUND 16 items are implemented with the pre-registered criteria
    testable live: (a) the 8 (19)-owned failures should clear (5 via orders repoint,
    3 via snapshot-complete fixtures); (b) sweep assert 4 should pass (the claim
    ledger makes run 42's replay bronze claimable at its staging DagRun's own dbt
    pass; the _run_id desc tie-break then attributes silver to the newest run --
    also retro-explains R14's blind sweep failure, same assert); (c) orphan test's
    owner read works with schema USAGE; (d) dbtkill observes DBT_BUILD RUNNING in
    its own staging DagRun; (e) no gold row can carry QUARANTINED lineage. Known
    pre-registered risk: the rebuild test now reprocesses the retained ~2.35M-row
    orders raw history through the asset cascade inside its 1800s settle windows.

- timestamp: 2026-08-28 (ROUND 16 post-run -- run 33126343052 analysis, best census of the session)
  checked: "Full job log of e2e-full.yml run 33126343052 (headSha 0a69dec, conclusion
    FAILURE, 23:27:32Z -> 01:47:45Z = 2h20m16s self-terminated, job 98705301890,
    12,700 lines saved to scratchpad round16-job.log; companion publish 33126343060
    SUCCESS with all 3 images pushed 23:28:19-23:29:34 BEFORE cluster-up pulled at
    23:31:15 -- criterion 0 met, stale-image hypothesis for the sweep failure
    eliminated by timestamps). Analyzed: streamed tracebacks for all 9 failures,
    the -v census, alembic migration log, TI/DagRun history dumps, FailedScheduling
    census, etl-monitor, final etl-namespace pods, cp-monitor peaks, ROUND 12 DB
    dumps (found post-rebuild, see finding 24), plus source reads of
    silver_customers.sql and _wait_for_dataset_files_terminal for the sweep
    adjudication."
  found: >
    (A) CENSUS: 9 failed / 29 passed / 6 skipped in 7970.98s (2:12:50). Best of the
    session (R15: 10/28/6). Newly green: concurrent_select (orders 250k repoint,
    SUCCEEDED end-to-end -- (19)-A's core mechanic proven live), dbt_silver
    (snapshot-complete customers, zero quarantine). Newly red: no_extra_schemas
    (allowlist vs 0039, see D). The R15 QUARANTINED-not-SUCCEEDED signature is
    EXTINCT: zero QUARANTINED terminals in the whole run; the only breaker trip is
    mass_delete's designed one (test PASSED with the (20b) exclusion predicates
    live).
    (B) FIXES IN FORCE: migrations 0039/0040/0041 applied at cluster-up (alembic
    log 23:34:14); csv_processor/dbt/airflow images all at 0a69dec; fix (23)'s
    edge proven live in TI history (backfill 15:15: stages 23:36:29-23:40:35 ->
    dbt_build 23:40:41-23:41:08 -> publish 23:41:13-23:41:44; same shape on every
    DagRun); reentry's original bad-row run reached SUCCEEDED un-quarantined
    (snapshot-complete fixture worked; R15's death point cleared).
    (C) FINDING (25) -- orders serialized-pipeline throughput collapse owns 6
    failures: podkill mid_load killed its stage pod ~00:27:25; the (20a) lease
    reclaim + full 1M re-stage COMPLETED inside the poll (last observed status
    STAGED -- the R15 stage-wedge is NOT back) but dbt_build+publish on 1M rows
    did not finish in the remaining 600s. The still-running DagRun then starved
    every later orders test: dbtkill 00:40:43 + u3 00:44:01 (discovery never
    registered within 180s), rebuild 00:44->01:40 reset all runs to PENDING and
    re-triggered the ENTIRE retained raw corpus (16 orders files: podkill/dbtkill/
    u3 1M each + concurrent 250k + 12 dated = ~3.25M+ rows vs the charter's ~2.35M
    estimate) through the same serialized pipe -> 16/16 unsettled at 1800s
    ({concurrent:STAGED, dbtkill:STAGED, podkill:RUNNING, u3+12 dated:PENDING}),
    orphan 01:43:21 + idempotent_reupload 01:47:10 starved. End-state: etl
    namespace CPU-saturated -- FailedScheduling census all 'Insufficient cpu'
    (stage/discover/publish/dbt-build pods), one stage pod (stage-jxsiz8ms, 2/2)
    38min old and still Running at job end. Structural cost multiplier:
    merge_orders' _PUBLISH_SQL publishes the WHOLE silver table each pass ((20b)
    leak vector ii), so orders publish scales with accumulated silver mass --
    every retained 1M fixture makes all later publishes slower. Zero failed TIs,
    zero SKIPPED_CONCURRENT, files eventually progressed (dbtkill's file reached
    STAGED by rebuild-era) => contention, not a logic wedge. The pre-registered
    (g) risk fired WIDER than predicted (per-test budgets, not just the rebuild
    settle).
    (D) FINDING (22b)+(22c) -- grant-family stragglers: reentry failed at
    _fetch_dagrun_identity (line 707/603) with InsufficientPrivilege on
    meta.ingestion_runs as analytics_owner -- a read no test had reached before
    (R15 died earlier); precedent-consistent fix = migration 0042 GRANT SELECT.
    test_no_extra_schemas_exist failed with 'unexpected schema(s): [normalized]'
    on the analytics_owner connection -- information_schema.schemata only lists
    schemas the role holds a privilege on, so this failure is itself the live
    PROOF that 0039's USAGE landed; fix = add 'normalized' to ALLOWED_SCHEMAS.
    (E) FINDING (24) -- sweep assert 4 STILL red post-ledger: 'silver.customers
    has no rows for _run_id=31' at 23:58:24 (run 31 = late file
    customers_20240108's newest replay per the drain's max-run_id DISTINCT ON;
    asserts 1-3 PASSED, orders counts exact at 600). Pre-registered falsification
    fired: the global-max watermark was NOT the sole mechanism. Source re-read
    confirms the live model SHOULD attribute correctly (txid-scoped claim ledger,
    existing_silver_contenders, _run_id desc tie-break) and every stage had a
    same-DagRun follow-up build. Forensics unrecoverable from this run: the
    rebuild test drops/resets meta.ingestion_runs (end-of-job ROUND 12 dumps show
    run numbering restarted at 1, sweep-era rows gone) and idempotent_rerun
    re-executes the same backfill window (try=2 overwrites sweep-era TI timings).
    NAMED DIAGNOSTICS GAP: end-of-job DB dumps are post-rebuild; the sweep test
    needs a failure-time dump rider (ledger claims + late-file bronze census +
    silver attribution for the late keys, streamed in the assertion message).
    (F) GUARDS: Kyverno 0 (only the deliberate unsigned-image denial, PASSED);
    restarts 0 all roles; ZERO failed TIs (session first); scheduler peak
    1,873,281,024B = 1786MiB = 69.8% of the new 2560Mi (18b vindicated: same mass
    = 91.5% of the old 2048Mi; R15 peak 1923MiB was 93.9%); dag-processor 784MiB;
    triggerer 401MiB; SKIPPED_CONCURRENT 0; publish try=2s = designed D-18 replay
    (idempotent_rerun test re-creates the same backfill window, PASSED); suite
    140.2min < 190; observability + rebuild capstone steps SKIPPED (suite-green
    gated), still never exercised live.
  implication: >
    ROUND 16's five-mechanism hypothesis is 4/5 confirmed-or-cleared: (19)-A's
    quarantine mechanism is extinct, (22) landed (spawning two one-line
    grant-family stragglers), (23) is live, (20b) is live. The residual failure
    mass has COLLAPSED from five mechanisms to two: finding (25), a capacity/
    design problem (serialized orders pipeline whose publish cost scales with
    accumulated silver mass, colliding with retained 1M fixtures and per-test
    budgets on ~3 CPU -- the session's original trigger shape, now with the
    root causes stripped away), and finding (24), the one genuinely unexplained
    correctness question left (sweep late-row attribution), which needs a
    failure-time forensics rider because the rebuild test destroys the evidence.
    Decision checkpoint returned on the (25) knob (delta-scoped merge_orders
    publish vs fixture shrink vs budget raise vs scope cut) + the two one-liners
    + the (24) rider.

- timestamp: 2026-08-28 (ROUND 17 offline implementation + red/green battery)
  checked: >
    merge_orders.py/_PUBLISH_SQL + publish(); merge.py + scd.py (publisher
    survey); run.py publish_ingest + reconciliation comments; silver_orders.sql
    (one-row-per-key confirmation); test_pod_kill_retry.py fixture sizes vs
    each test's own assertions; test_rebuild_from_raw.py settle budget;
    migrations 0038/0039/0040 precedent; test_postgres_topology.py
    ALLOWED_SCHEMAS; test_backfill_2year_sweep.py assert-4 site; full offline
    battery incl. bare-HEAD differentials for integration + ruff.
  found: >
    (25)-A IMPLEMENTED: `_run_id = ANY(%(staged_run_ids)s)` added to
    merge_orders' _PUBLISH_SQL (quarantine NOT-IN kept; DISTINCT ON + tie-break
    + ON CONFLICT guard + RETURNING unchanged => determinism/auditability
    preserved). Publisher survey recorded: scd.py already run-scoped (no
    change); merge.py has the identical O(accumulated) shape but ZERO live
    datasets use strategy 'merge' -- left unchanged with a load-bearing
    docstring warning to delta-scope before any dataset adopts it. RED/GREEN
    PROVEN: new tests/integration/test_publish_orders_delta_scope.py -- (i)
    equivalence: delta-scoped pass-by-pass gold byte-identical to a legacy
    whole-table merge over the same final silver state (regression guard,
    green both sides by design); (ii) no-rescan lock proof: with a concurrent
    FOR UPDATE held on an already-published key and lock_timeout=2s, the
    PRE-FIX publish failed `psycopg.errors.LockNotAvailable: canceling
    statement due to lock timeout` (RED, observed on stashed pre-fix code)
    and the delta-scoped publish completes green -- direct, deterministic
    proof the accumulated keys are no longer read or row-locked.
    test_publish_quarantine_exclusion.py's orders test updated to
    publish_ingest's production shape (pass claims BOTH runs; exclusion still
    run-scoped). test_publish_orders.py's 3 call sites now pass
    staged_run_ids=[run_id] -- clearing the 3 PRE-EXISTING TypeError failures
    R16 had declared out of scope (in scope now: the seam itself changed).
    (25)-B IMPLEMENTED per-assertion verification: podkill KEEPS 1M (kill
    window scale-bound: at U3's measured 41,946 rows/s a 250k stage is ~6s
    local, too slim vs the 0.5s poll + kubectl latency; (20a) restage-at-scale
    proof also 1M-bound); u3 SHRUNK 1M -> 250k (assertions are rate-based
    throughput>0/peak>0, NOT scale-bound; doc self-describes its fixture);
    dbtkill: user premise corrected -- ALREADY 120 rows since R16 (kill target
    is the dbt pod). Rebuild re-queue arithmetic verified and recorded in the
    test: 12 dated (~600) + 250k + 1M + 120 + 250k ~= 1.50M rows, down from
    R16's ~2.25M (-33%); with (25)-A total rebuild-era publish work becomes
    sum-of-deltas, killing the N-passes x O(accumulated) multiplier that was
    R16's dominant sink. (22b) migration 0042 GRANT SELECT ON
    meta.ingestion_runs TO analytics_owner (0038/0039/0040 family): red/green
    via new has_table_privilege test (RED with 0042 parked, GREEN with it;
    read-only boundary asserted). (22c) 'normalized' added to ALLOWED_SCHEMAS
    with the visibility-mechanism comment. (24) RIDER: sweep assert-4 failure
    now pytest.fail's with _late_file_lineage_forensics -- ledger claims
    (dataset_name/run_id/claimed_txid/claimed_at), late-file bronze run census
    + all ingestion_runs attempts (incl. replay_of_run_id), and silver
    attribution for the late file's business keys -- built lazily at failure
    time, streamed via the -v traceback rider, adjudicable despite rebuild's
    later meta reset. BATTERY: unit+regression 560 passed; dagtest 14 passed;
    policy 157 + the 2 known pre-existing failures; manifests+kubeconform
    378/0/0; mypy strict 91 files clean; slice collects 17 / e2e collects 53.
    Integration FULL-SUITE DIFFERENTIAL vs bare HEAD: HEAD = 17 failed / 186
    passed; with ROUND 17 = 14 failed / 192 passed -- the 14 are the IDENTICAL
    pre-existing node-IDs (config_registry x2, dbt_docker_image, lineage_view
    x2, publish_ingest full-suite-order coupling, transaction_wiring x4,
    referential_integrity, staging_normalization, watermarks x2; all fail on
    bare HEAD in full-suite order, all pass in touched-suite runs), the 3
    fixed are the test_publish_orders TypeErrors. Zero new failures. Ruff
    lint/format restored to HEAD's exact pre-existing baseline (3 errors / 17
    would-reformat, all pre-existing offline Quality-gate class). New
    delta-scope tests drop their staging scratch tables + gold rows eagerly
    (removes the role_table_grants ordering coupling they'd otherwise add).
  implication: >
    All four charter items are implemented with the pre-registered red/green
    evidence. The live run adjudicates: (a) whether O(delta) publish + the
    1.5M re-queue bound clears the 6 (25)-owned failures inside unchanged
    budgets (the podkill 600s window -- lease ~330s + 1M restage + dbt +
    delta publish -- is the pre-registered residual risk), and (b) finding
    (24), which either passes or finally yields adjudicable forensics.

- timestamp: 2026-08-28 (ROUND 17 post-run analysis of run 33147620963)
  checked: >
    Full job log (13,941 lines, scratchpad round17-job.log): per-test result
    lines + short summary; sweep assert-4 traceback WITH the streamed
    _late_file_lineage_forensics block; podkill/dbtkill/u3/rebuild/orphan/
    idempotent tracebacks; end-of-job always()-diagnostics (cp-monitor peaks,
    restart timeline, FailedScheduling census, etl-monitor rolling pod
    timeline 07:40-08:05 + final events, customers DagRun history 66 rows,
    customers TI history 1492 rows, meta.ingestion_runs->files mapping 93
    rows, schema_versions, silver _run_id census); alembic migration log at
    cluster-up; R16 log per-test timestamps for the duration diff; test
    sources (test_pod_kill_retry.py trigger paths, test_smoke_and_idempotency
    fixture size, test_referential_orphan cleanup delete) and the 79dd299
    commit stat (conftest fixture builders untouched).
  found: >
    (A) CENSUS: 7 failed / 31 passed / 6 skipped in 9454.05s (2:37:34); job
    2h45m08s, self-completed. Best census of the session; the 7 failures are
    an exact subset of R16's 9 (newly green: reentry, no_extra_schemas).
    (B) FIXES IN FORCE: migration 0042 applied at cluster-up (06:27:49
    '0041 -> 0042'); all images at 79dd299; publish_retries=3 CI profile
    registered; fix (23) ordering visible throughout TI history.
    (C) (24) ADJUDICATED VIA THE RIDER -- TEST ARTIFACT, PLATFORM CORRECT:
    forensics show bronze holds run 39's 50 rows, run 39 SUCCEEDED
    (replay_of=7), ledger claimed it (txid 1291 @ 06:41:48), and all 50 late-
    file business keys ARE in silver attributed to runs 47/48 (day-12/13
    snapshot files, event_ts 2024-01-13/12, one key to run 47). The corpus
    reuses the same 50 customer_ids daily; backdated day-8 rows must lose
    every silver slot under business-ts-wins semantics. The assertion queries
    silver.customers although its own comment specifies normalized.customers.
    Sequencing defect: waits drained on pilot-era terminal state; backfill-3
    DagRuns (00:03/00:04/00:05) executed 06:54:04-07:01:25 -- AFTER the
    test's 06:53:53 failure.
    (D) PODKILL: kill ~07:41:35; (20a) lease wait + full 1M restage + dbt +
    DELTA publish completed ~07:52:40 (publish-s4hc01fs running <=07:51:33 ->
    gone by 07:52:46) = ~66s past the 600s deadline (last observed STAGED).
    The O(delta) publish is live-proven: post-restage dbt+publish <=2min on
    1M rows (R16: same segment >600s residual and the DagRun never finished).
    (E) DBTKILL/U3 STARVATION MECHANISM (user observation confirmed +
    refined): podkill's DagRun held the max_active_runs=1 slot through the
    first ~50s of dbtkill's window, then ~5 asset-triggered orders DagRuns
    queued during its 10.5-min occupancy (customers publishes 07:43:31-
    07:52:57) drained serially ahead of dbtkill's/u3's manual triggers; no
    orders discover pod before 07:58:21; etl namespace pod-sparse 07:53-07:56
    with ZERO FailedScheduling in that window -- queue latency, not CPU.
    (F) REBUILD: 15/16 unsettled at 1800s but 9/16 STAGED + 6 PENDING (R16:
    2 STAGED/1 RUNNING/13 PENDING at ~2.25M mass; R17 mass ~1.50M) --
    stage-side throughput through the requeue vastly improved; the staged
    tail's serialized dbt/publish still exceeds the settle budget. dbtkill's
    file 'settled' by going FAILED (run 62). CPU-saturation FailedScheduling
    census is rebuild-era only.
    (G) NEW FINDING (26): runs 62 + 438 (dbtkill's 120-row file, two eras)
    and run 439 (idempotent's fresh 120-row fixture) reached terminal FAILED
    at pre-schema phase (schema_version_id NULL) with zero failed TIs and
    zero pod-level warnings -- application-level failure whose error text no
    dump captures (ingestion_runs dump lacks the error column;
    poll_ingestion_run prints only status). idempotent_reupload's failure
    thereby MORPHED from R16 starvation to run-FAILED. Both FAILED files are
    exactly the two 120-row fixtures; 50-row/250k/1M files staged clean in
    the same eras; conftest fixture builders untouched by 79dd299; dbtkill's
    same file reached STAGED in R16. UNADJUDICABLE this round.
    (H) GUARDS: Kyverno 0; restarts 0; ZERO failed TIs; 66/66 customers
    DagRuns success; scheduler peak 1776MiB = 69.4% of 2560Mi; suite duration
    diff vs R16 fully decomposed (scd_concurrent +11.4min variance-watch,
    reentry +5.1min now-full-coverage, rebuild +3.9min, live_run +2.0min,
    podkill -1.5min).
  implication: >
    The (25) multiplier is dead: orders publish is O(delta) live, podkill's
    DagRun completes, and the remaining 6 non-sweep failures reduce to (i)
    ONE 66s budget miss (podkill 600s) with (ii) queue-drain discovery
    starvation and (iii) the rebuild settle budget as its knock-ons, plus
    (iv) finding (26), the only unexplained residue, blocked solely on a
    missing error-column dump. Finding (24) is closed as a test-layer
    artifact with the platform proven correct by its own forensics. Decision
    checkpoint returned recommending a final ROUND 18: fix (24)'s wrong-table
    assert, add the (26) error-forensics rider, and disposition
    podkill/discovery/rebuild budgets explicitly.

- timestamp: 2026-08-28 (ROUND 18 offline implementation + battery)
  checked: >
    tests/e2e/slice/test_backfill_2year_sweep.py (assert-4 + manifest
    late_event_row_index provenance), tools/corpus/dated_series.py
    (_render_customer_day_lines: late row = member rows_per_day//2 == 25,
    attributes IDENTICAL to baseline, event_ts backdated 90 days -- so the
    member's SCD2 chain collapses to versions whose EARLIEST boundary is the
    backdated ts, disjoint from every other anomaly member 30/31/32/33-47),
    migrations/versions/0004_meta_ingestion_runs.py (error columns are
    error_type/error_message/error_detail; rows_read exists for the
    heartbeat), tests/e2e/slice/conftest.py, test_pod_kill_retry.py,
    test_referential_orphan.py, test_smoke_and_idempotency.py,
    test_rebuild_from_raw.py, .github/workflows/e2e-full.yml.
  found: >
    All five ROUND 18 charter items implemented: (1) sweep assert-4
    repointed to normalized.customers -- late member identified from the
    corpus' OWN manifest (late_event_row_index -> _CUSTOMER_ID_BASE + 25,
    same idiom as the module's other anomaly asserts), assertion is
    min(event_ts) == the verbatim backdated ts (also era-insensitive,
    closing R17's secondary sequencing defect), forensics rider kept on the
    failure path. (2) poll_ingestion_run now selects/returns
    error_type/error_message, streams them in its timeout message; the five
    SUCCEEDED asserts that consumed R17's failures (podkill, dbtkill, u3,
    orphan, idempotent_reupload) print them; e2e-full.yml's always()-dump
    gained a dedicated error-bearing-runs query (FAILED/QUARANTINED or
    error_message NOT NULL, all datasets, error_detail included, own tail
    budget -- no truncation risk). (3) _RETRY_TIMEOUT_SECONDS 600 -> 900
    with the R17 arithmetic recorded (330s designed lease + ~340s measured
    restage/dbt/publish = ~11.1min observed cycle). (4) NEW
    wait_for_orders_dagrun_queue_idle helper (slice conftest, <=600s bound,
    fails naming still-active run_ids) called before dbtkill/u3/orphan and
    BOTH idempotent_reupload uploads (orphan's call sited AFTER its unpause
    -- a paused DAG's queue cannot drain). (5) rebuild settle converted to
    monotonic-progress: stall window 600s over the per-file
    (duplicate,status,rows_read) snapshot (rows_read = stage heartbeat, so a
    long single-file COPY counts as progress), hard cap 3600s, both failure
    messages distinct and legible. OFFLINE BATTERY at bare-HEAD parity:
    ruff check on all touched files -> only the 2 pre-existing sweep
    W505/E501 (verified identical on stashed HEAD); ruff format -> only the
    2 pre-existing drift files with hunks in UNTOUCHED regions (411/516/536);
    unit+regression 564 passed (incl. 4 NEW drain-helper unit tests: idle
    fast-path, poll-until-drain, legible never-drains failure naming
    dag_id/run_ids/states, dag_id parameterization); policy 2 pre-existing
    failures only (dag_line_budget + gates_actually_fail, HEAD lint-driven);
    dagtest 14 passed; make manifests + kubeconform 378 valid / 0 invalid;
    mypy strict clean (91 files); e2e slice collection clean (17 tests);
    e2e-full.yml parses as YAML. No integration surface touched (no
    packages/ change at all this round -- test + workflow layer only).
  implication: >
    ROUND 18 is implementation-complete offline: one test-layer fix, one
    instrumentation rider, three explicit budget/behavior dispositions, zero
    production-code changes. Ready to commit (docs [skip ci] first, code
    last, single push) and hand the live watcher the authoritative
    e2e-full.yml run ID against the pre-registered criteria.

- timestamp: 2026-08-28 (ROUND 18 post-run analysis, run 33164806655, headSha 4818867)
  checked: >
    Full job log (round18-job.log, 15,674 lines) via gh api job logs for the
    'Full local E2E suite + rebuild-from-raw capstone' job (98827564090); the
    pytest short-summary (7 failed/31 passed/6 skipped, 10866.63s); the full
    text of every failing test's assertion + traceback; the end-of-job
    diagnostics dump (cp-monitor.csv/etl-monitor.log/final ingestion_runs
    dumps/(26)'s error-bearing-rows query); direct source read of the
    INSTALLED apache-airflow==3.3.0 on the LOCAL persistent cluster (kubectl
    exec into deploy/airflow-scheduler -- scheduler_job_runner.py's
    _schedule_dag_run/_schedule_all_dag_runs, dagrun.py's
    get_running_dag_runs_to_examine); packages/dataplat/src/dataplat/scd/
    delete_detection.py and load/publish/scd.py in full; airflow/dags/
    csv_ingest_orders.py and _common/run_stage_recorder.py in full; the
    orders DAG's own dagrun_timeout/retries/retry_exponential_backoff
    config; tests/e2e/slice/conftest.py's poll_ingestion_run/
    wait_for_orders_dagrun_queue_idle in full.
  found: >
    (1) Fix (24) CONFIRMED WORKING live: sweep assert-4 no longer fires;
    the test proceeds to a LATER assertion (10, DELETE-detection on
    customer_id=2100100032) never reached in any prior round -- masked
    every prior round by assert-4's own earlier failure. (2) podkill's
    DagRun ('e2e-podkill-59f13a4ddbe9') genuinely never reached a terminal
    meta.ingestion_runs status this round (STAGED for the full 900s test
    budget, error_type/error_message both NULL throughout) -- qualitatively
    worse than R17's 66s-miss-then-complete. (3) Direct source read,
    installed 3.3.0, scheduler_job_runner.py:_schedule_dag_run (~line 2789):
    the dagrun_timeout check (`dag_run.start_date and dag.dagrun_timeout and
    dag_run.start_date < utcnow() - dag.dagrun_timeout`) has NO run_type
    gate, fires purely on wall-clock + DB state, and on firing sets
    dag_run.state=FAILED + every unfinished TaskInstance to SKIPPED --
    confirmed sound, and already independently live-verified working
    elsewhere this session (ROUND 3's fix; root cause 14's backfill
    dagrun_timeout kill at 14:36:44). (4) dagrun.py:get_running_dag_runs_to_examine
    uses `with_row_locks(..., skip_locked=True)` -- a theoretically possible
    (but NOT confirmed this round; single scheduler replica, no evidence of
    a lock holder) alternate mechanism for a DagRun to be silently excluded
    from every scheduling loop forever; ruled less likely than the
    dagrun_timeout-fired-but-not-propagated explanation given direct
    supporting pod-lifecycle evidence (below). (5) etl-monitor.log direct
    pod-lifecycle evidence in the reconstructed post-kill window: stage
    pod (stage-lqu9he3u) Running 12:21:45-12:22:55 (~70s, matches fix 20a's
    ~5.5min lease-wait + restage arithmetic), dbt_build pod
    (dbt-build-eleymmox) Running 12:23:30-12:24:24 (~1min), THEN publish
    pod (publish-n1l2gcsv) Running 12:24:42-12:25:17 followed by
    publish-n1l2gcsv reaching phase 'Failed' at 12:25:35 -- a REAL publish
    pod failure ~11min post-kill, timing-matched to R17's own SUCCESSFUL
    publish completion (~11.1min) almost exactly, except this round publish
    itself crashed. (6) publish has retries=3/retry_exponential_backoff=True
    with the project's stock (no override, confirmed via grep) ~5min-base
    uncapped-multiplier retry_delay -- a failed first attempt's own backoff
    plausibly does not reach a second attempt before dagrun_timeout=45min
    (measured from DagRun creation ~12:07-12:14) elapses (~12:52-12:59).
    (7) idempotent_reupload's OWN drain-helper failure (last test in the
    run, ~13:5x) names a COMPLETELY DIFFERENT active DagRun set
    ('asset_triggered__...running', 'e2e-orphan-...queued') with ZERO
    mention of 'e2e-podkill-59f13a4ddbe9' -- direct evidence the podkill
    DagRun DID eventually leave 'running' state sometime between orphan's
    failure (13:47:24) and idempotent's own check, freeing the global
    stage/dbt_build/publish slot for a fresh backlog -- exactly the
    signature a ~45min-after-creation dagrun_timeout firing would produce.
    (8) (26)'s error-bearing-rows dump printed literally '(0 rows)' at the
    very end of the job (13:58:36) even though a genuine wedge existed all
    along -- confirms (26)'s WHERE clause (FAILED/QUARANTINED/non-null
    error_message) structurally cannot see a run stuck at a non-terminal
    status with NULL errors; a real, now-named diagnostics gap. (9) dbtkill
    (12:39:47) and u3 (12:49:49) failures are direct, unambiguous
    wait_for_orders_dagrun_queue_idle assertions naming
    'e2e-podkill-59f13a4ddbe9' (state 'running') -- both inside the
    reconstructed pre-dagrun_timeout window. orphan (13:47:24) is a plain
    poll_ingestion_run 180s STAGED timeout on ITS OWN upload, one hop
    further downstream (its own drain-helper call passed -- the queue had
    JUST cleared -- but its own file then queued behind residual backlog).
    (10) rebuild's NEW monotonic-stall assertion fired cleanly: 14/14
    orders files, ZERO status/rows_read change for the full 600s stall
    window -- same root mechanism, observed via the NEW honest diagnostic
    instead of the old 1800s-budget-exhaustion shape. (11) Guards fully
    green: Kyverno 0 denials, restarts 0 all roles (empty restart-change
    timeline), scheduler peak 1804MiB (70.5% of 2560Mi, stable vs R17's
    1776MiB) -- ZERO OOM/crash-loop this round, directly ruling OUT the
    ROUND-3-era adopt_or_reset_orphaned_tasks livelock (M1) as this round's
    mechanism (that path requires a scheduler restart to manifest; none
    occurred). (12) find_vanished_customer_ids (delete_detection.py) is
    confirmed correctly scoped to `staged_run_ids` (bronze, ROUND-12-fixed)
    -- assertion (10)'s failure is NOT an unscoped-read regression; it is,
    by direct code read, consistent with the SAME 'test assumes
    one-file-per-publish-pass' mismatch (24) was, one assertion downstream,
    IF fix (21)'s claim-ALL-currently-STAGED-runs batching folded an
    earlier day's still-has-the-customer run_id into the SAME publish pass
    as the final day's run_id -- plausible under this round's own confirmed
    contention conditions, but NOT forensically confirmed (no forensics
    rider attached to this specific new assertion this round). (13) Job
    duration 189.3min self-terminated, 0.7min inside the 190-min ceiling --
    the tightest margin of the session; the drain helper's and stall
    assertion's own fast-fail budgets (600s each) are what kept the round
    inside budget despite podkill's DagRun consuming its full 900s test
    wait plus real wall-clock dagrun_timeout time baked into the suite's
    actual execution.
  implication: >
    Two genuinely new findings this round, both same-class as already-
    adjudicated (24)/(26) but NOT yet forensically closed: (i) podkill's
    DagRun wedge is now root-caused with strong, source-confirmed,
    multi-source-corroborated evidence to an Airflow-level dagrun_timeout
    firing with no propagation path back into meta.ingestion_runs -- but
    the proximate trigger (WHY publish's first attempt crashed this round,
    when the same mechanism succeeded in R17) is not directly observed
    (no raw scheduler log, no orders-DagRun/TI dump, no captured pod-crash
    reason survive this round's diagnostics); (ii) sweep assertion (10)'s
    DELETE-detection failure is a strong, code-consistent same-class
    hypothesis to (24) (batch-scoped publish spanning multiple days), also
    not forensically confirmed. Per this session's own established
    discipline (never fix blind; instrument first when a mechanism is
    plausible but unconfirmed, exactly as (26) was born from R17's
    idempotent_reupload finding), ROUND 19 should be diagnostics-first:
    (a) an orders-DagRun/TaskInstance dump mirroring the existing customers
    one, specifically surfacing dag_run.state/start_date/end_date and any
    SKIPPED-by-timeout TI signature; (b) raw scheduler pod log capture
    (or at minimum a grep for 'has timed-out'/'Error scheduling DAG run')
    across the WHOLE run, not just the customers-filtered grep that exists
    today; (c) a rolling capture of terminated-container reasons for
    stage/dbt_build/publish pods (their crash reason is currently lost the
    moment Kubernetes garbage-collects the pod); (d) widen (26)'s
    error-bearing dump to also catch stale non-terminal rows (age-bounded,
    not just FAILED/QUARANTINED/error-message); (e) extend (24)'s exact
    forensics-rider pattern to assertion (10)'s new failure point. Dagrun
    wedge duration is now also a ceiling-margin risk (0.7min slack this
    round) independent of whether it is ever fixed to complete faster.

## Eliminated
<!-- APPEND ONLY - never delete -->

- hypothesis: "ROUND 12 alternates for (16)'s 54%: (i) the mass-delete test fixture's own
    15/50 snapshot leaked into the sweep corpus; (ii) per-window roster churn in seed v5
    structurally exceeds 10%; (iii) re-publication of an older day-window against evolved
    gold with CORRECT snapshot computation."
  evidence: "(i) raw/customers listing at cancel: exactly the 12 corpus files, no fixture
    object (ROUND 11 evidence). (ii) dated_series.py roster model: fixed 50 members,
    full resend every non-gap day; only genuine churn = missing member 1/50 = 2% from
    day 12 -- structurally cannot exceed 10%. (iii) the older-window hypothesis was
    half-right (re-publication IS the trigger) but the vanished mass comes from
    dedup-tie lineage on BYTE-IDENTICAL replays, not from genuine window differences --
    proven by the local repro where every wave contains identical full-roster content
    yet 48% still 'vanished' pre-fix."
  timestamp: 2026-08-27 (ROUND 12)

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

- hypothesis: "ROUND 7: The invariant 17-test signature is an emergent property of aggregate
    concurrent load (sweep_corpus 19-file fan-out + multiple simultaneous DagRuns oversubscribing
    ~3 allocatable CPUs); reducing peak concurrency (parallelism 16->8, max_active_tasks=6,
    max_units_per_run 100->10) will move the signature."
  evidence: >
    REFUTED per its own pre-registered falsification test. Live run 32834311083 (headSha
    84e9c74, containing the full three-lever fix) reproduced the byte-identical 17-test
    node-ID signature (9th consecutive run) WITH the load-bearing lever positively verified in
    force ('LocalExecutor(parallelism=8)' on every scheduler enqueue line). Kyverno DENY count
    rose 14->18 rather than falling. The DENY text itself revealed the true mechanism is
    network/policy-structural (Docker Hub 429 + unexempted alpine:3.24.1 XCom-sidecar image --
    see Evidence ROUND 7 part 2), which no concurrency knob can influence -- exactly the
    outcome ROUND 7's own blind_spots (2) pre-declared as the refutation signature. NOTE: the
    three levers are real load-hygiene improvements and are NOT reverted (scope guardrails);
    only their claim to be the root-cause fix for THIS signature is eliminated.
  timestamp: 2026-08-25 (ROUND 7 post-run analysis)

- hypothesis: "ROUND 8: root cause (10) -- the Kyverno require-signed-images denial of the
    runtime-injected alpine:3.24.1 XCom sidecar -- is the (sole) proximate cause of the
    invariant 17-test signature; exempting the sidecar image will break the signature."
  evidence: >
    REFUTED per its own pre-registered falsification test, on its strongest possible terms.
    Live run 32845181597 (headSha ce73d9df, the exemption commit itself): exemption VERIFIABLY
    applied at cluster-up (policy created from the committed file, 12:01:10Z) and VERIFIABLY
    effective at the mechanism level -- ZERO Kyverno denials in the whole 5434-line log (vs
    14-18 in rounds 6/7), zero Docker Hub 429s, and discover tasks reached state=success try=1
    in 11-15s (the first CI discover successes of the entire session) -- yet the pytest
    signature was the 10th consecutive byte-identical 17-test node-ID set (17/21/6 in
    3509.10s), same failure templates. The run's own diagnostics expose the residual
    mechanisms directly: scheduler OOM crash-loop (9 restarts, ~6min cadence, OOMKilled/137)
    wedging the first two DagRuns for 47min behind an unexplained early upstream_failed
    cascade, then global max_active_tis_per_dag throttling (269 scheduler-log occurrences)
    slowing the healthy replacement runs past every 180s test window. NOTE: fix (11) is NOT
    reverted -- the denial mechanism was real, deterministic, and is now live-verified
    eliminated on both clusters; only its claim to be THE cause of the 17-test signature is
    eliminated. Key lesson recorded: the 17-test node-ID set is a SATURATED instrument -- any
    mechanism that delays the pipeline past 180s produces the identical set, so 'signature
    unchanged' can never distinguish which upstream mechanism is active; internal diagnostics
    (TI states, restart timelines, DENY greps) are the only discriminating measurements.
  timestamp: 2026-08-25 (ROUND 8 post-run analysis)

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
  (3c, ROUND 3, same root-cause CLASS as (3)/(3b) -- scheduler memory -- CONFIRMED as an
  UNBOUNDED RETRY LIVELOCK, not merely a growth-rate question): ROUND 2's fix (parallelism
  32->16, CI scheduler memory limit 1Gi->1536Mi) was LIVE-VERIFIED INSUFFICIENT: peak memory grew
  910MiB->1471MiB (89%->95.8% of ceiling, WORSE as a percentage) despite the eagerly-forked pool
  being HALVED (peak_pids 48->33) -- direct evidence the eager-fork-pool-size mechanism (3b) is
  at most a PARTIAL contributor, refuting "raise the ceiling / trim the pool again" as sufficient.
  Direct source-level investigation against the INSTALLED apache-airflow==3.3.0 (live LOCAL
  scheduler pod) identified the DOMINANT term: `SchedulerJobRunner.adopt_or_reset_orphaned_tasks()`
  -- the ONLY task-recovery path that survives a whole-pod OOM-SIGKILL, since it queries the DB
  fresh rather than relying on any in-memory executor state -- unconditionally resets an
  interrupted TaskInstance to schedulable (`ti.prepare_db_for_next_try()` + `ti.state = None`)
  WITHOUT ever calling `is_eligible_to_retry()`/`handle_failure()`. The NORMAL retry-exhaustion
  path (`_process_executor_events()` -> `ti.handle_failure()`, which DOES enforce
  `try_number<=max_tries` and `retry_delay`/exponential backoff) only fires when the LocalExecutor's
  own in-memory event queue delivers a completion event -- impossible for a task whose entire
  hosting process (scheduler + all LocalExecutor workers, same cgroup) was just OOM-killed.
  `DagRun.schedule_tis()` DOES increment `try_number` on each reset-and-reschedule cycle
  (confirmed via source read), but nothing ever checks it against `max_tries` for an
  orphan-reset TI -- so `retries=6` (configured on every KPO task in both production DAGs) never
  actually bounds this specific failure mode, and the reset-to-None path bypasses
  `UP_FOR_RETRY`/backoff entirely, meaning the stuck task is immediately re-attempted on the very
  next scheduling loop, with zero delay, forever. Because both DAGs have `max_active_runs=1`
  (shared across backfill AND regular-schedule DagRuns of a dag_id) and `stage`/`dbt_build`/
  `publish` share one GLOBAL `max_active_tis_per_dag=1` slot, ONE permanently-stuck DagRun blocks
  EVERY future DagRun of that dag_id for the rest of the run -- directly explaining the
  `AlreadyRunningBackfill` cascade and the "missing entirely"/discovery-never-registered failures,
  and (since `csv_ingest_customers` is on a 1-minute cron) an ever-growing backlog of
  can-never-start DagRun rows the scheduler must re-examine every ~1s loop, a genuinely
  TIME-PROPORTIONAL (not pool-size-proportional) accumulation matching the data ROUND 2 exposed.
  Empirically corroborated: a name-for-name diff of ROUND 2's own live-verification failing-test
  list against all 3 prior runs shows an IDENTICAL 17-test set across FOUR independent runs now,
  with `test_pilot_window_drains_without_cpu_starvation` (a deliberate concurrent-backfill stress
  test, per its own docstring) as the consistent, deterministic first casualty every time,
  regardless of the parallelism/ceiling change -- consistent with a DB-state-driven trigger, not
  generic time-based resource exhaustion.
  (8, ROUND 4, the ACTUAL proximate cause of the session's persistent 17-test failure signature --
  a DIFFERENT root-cause CLASS entirely from (1)/(2)/(3)/(3b)/(3c), unrelated to
  scheduler/dag-processor resourcing or restart count): ROUND 3's own fix (7) was LIVE-VERIFIED
  (restart count dropped 7->6->3, monotonically, across ROUNDs 1-3's three genuinely different
  mechanisms) but the exact same 17-test failure SET recurred unchanged across all 5 live runs to
  date regardless of restart count -- direct evidence the scheduler OOM/livelock mechanism (3)/
  (3b)/(3c), while real and independently worth fixing, was never the proximate cause of THIS
  symptom. The actual cause: `tests/e2e/slice/test_backfill_2year_sweep.py::
  _pause_customers_dag_for_backfill_only_tests` (module-scoped, autouse=True, added by an earlier
  plan 10-07 to stop this module's 5 backfill-only tests losing a race for the shared
  `max_active_tis_per_dag=1` slot against `csv_ingest_customers`' own live schedule) pauses that
  DAG before the module's first real backfill test runs. Confirmed via direct source read of the
  installed apache-airflow==3.3.0 AND a live empirical reproduction on the LOCAL cluster:
  `SchedulerJobRunner._executable_task_instances_to_queued` filters `~DagModel.is_paused` when
  selecting TaskInstances to queue, and `DagRun.get_queued_dag_runs_to_set_running` INNER JOINs
  on `DagModel.is_paused == false()` -- together meaning a paused DAG's DagRuns, backfill-created
  or schedule-created alike, NEVER transition past `queued` and NEVER get a single TaskInstance
  queued, for as long as the DAG stays paused (not merely "stops new scheduled runs", the
  fixture's own incorrect assumption). `DagRun.get_running_dag_runs_to_examine` (feeding
  `_schedule_dag_run`, which enforces `dagrun_timeout`) only returns DagRuns already RUNNING, so
  fix (7)'s `dagrun_timeout` structurally cannot reach a DagRun stuck in `queued` -- a clean,
  mechanistic explanation for fix (7)'s own zero effect on this signature. This freezes
  `test_pilot_window_drains_without_cpu_starvation`'s own backfill on every live CI run
  (`discover_files` never gets a single TaskInstance queued, so `meta.files` never gets a row --
  "missing entirely", matching the orchestrator's own ROUND 4 finding (a) precisely), which then
  blocks every later test in the SAME module via Airflow's own Backfill-uniqueness constraint
  (`_mark_backfills_complete()`, already source-confirmed in ROUND 2/3, only clears a Backfill
  once none of its DagRuns are RUNNING/QUEUED) with `AlreadyRunningBackfill` -- explaining all 5
  of this module's remaining tests. Once the module's own fixture teardown finally unpauses
  `csv_ingest_customers` again, the original frozen 20-day backfill finally starts draining for
  real, plausibly consuming the shared `max_active_tis_per_dag=1` slot and real CI CPU/pod-
  scheduling capacity deep into the rest of the suite -- a plausible (not separately
  live-instrumented) explanation for later files' own failures too, including the orchestrator's
  own finding (b) (test_smoke_dag_xcom_contains_built_sha's DagRun DOES reach 'running' -- the
  DAG is genuinely unpaused again by then -- but doesn't finish within 180s, a materially
  different, downstream resource-contention signature consistent with a large recovering backfill
  competing for capacity, not a still-paused DAG). `tests/e2e/slice/conftest.py::
  _unpause_slice_dags`'s own docstring (predates plan 10-07's pause fixture) already documented
  this exact mechanism and this exact symptom text in plain language before this round began:
  "A paused DAG's scheduler simply never starts a run for it -- there is no error, no timeout
  shortcut, just silence." `test_no_extra_schemas_exist` (finding (c)) remains unrelated to this
  mechanism (a schema-existence assertion, out of scope, unchanged disposition from every prior
  round).
  (9, ROUND 6, the actual proximate cause of the session's persistent 17-test failure signature --
  a DIFFERENT root-cause CLASS entirely from (1)-(8), unrelated to scheduler/dag-processor
  resourcing, DAG-pause, dagrun_timeout, or sweep_corpus/integrity_gate scheduling concurrency; also
  REFUTES ROUND 5's own leading sweep_corpus/integrity_gate-backlog hypothesis as the proximate
  mechanism): direct re-examination of ROUND 5's own live run (32813826344/job 97698134909) found
  clean, unambiguous Kubernetes admission-webhook DENY responses for `discover`/`publish` pod
  CREATE attempts in `csv_ingest_customers`: `admission webhook
  "ivpol.validate.kyverno.svc-fail-finegrained-require-signed-images" denied the request: Policy
  require-signed-images failed: container image failed cosign keyless signature verification
  against this repository's publish.yml OIDC identity...` for
  `ghcr.io/konutech/csv-processor:1c111c033f638327b8ed26ee1bf5317715cfd5d4` -- this session's own
  correctly-built, correctly-published image. This is a POLICY DENIAL (the CEL validation expression
  evaluated false), not a webhook-call timeout (a different, already-eliminated mechanism from
  earlier in this session, confined to cluster bring-up). Direct evidence chain (full detail in
  Evidence "ROUND 6" entries): (a) discover NEVER reached `success` in any of the 4 DagRuns this
  whole ~62min session; (b) `publish.yml`'s own csv-processor build+sign job completed successfully
  ~7-20+ minutes BEFORE the denied attempts, ruling out a simple "image not pushed yet" race; (c)
  independent `cosign verify` (replicating the policy's exact identity/issuer) succeeds cleanly
  right now -- the signature is genuinely valid, not broken; (d) the triggerer pod's own
  `kyverno.io/image-verification-outcomes` annotation shows a clean PASS for the SAME commit's
  airflow image early in the run (lighter load) -- Kyverno/signing/registry connectivity all
  fundamentally work, and failure correlates with WHEN (how loaded the cluster is) verification is
  attempted; (e) 8 INDEPENDENT later-failing tests across 6 different modules, each uploading their
  OWN uniquely-named single file (none part of sweep_corpus's 19-file corpus), spread across the
  whole run including tests sorting well after test_backfill_2year_sweep.py alphabetically, ALL hit
  the byte-identical "discovery never registered it" signature -- refuting a one-time,
  eventually-draining startup backlog in favor of a persistent, ongoing admission-denial mechanism
  active throughout the run, independent of which file or DagRun. Root mechanism: Kyverno's
  `require-signed-images` `ImageValidatingPolicy` (`kubernetes/kyverno-policy.yaml`) performs a
  LIVE, uncached cosign/registry round-trip (GHCR manifest fetch + Sigstore Rekor/Fulcio lookups) on
  EVERY pod admission cluster-wide, with NO caching (confirmed via that file's own header comment).
  This repository's own commit history (that same file's Rule 3 fix, plan 11-06) already documented
  this exact call taking 15-20s under LIGHTER ambient load than a full `cluster-slice-verify` run
  (multiple concurrent DagRuns + the whole pytest suite) generates. The CI admission-controller
  container's CPU LIMIT was only 200m (1/5 of one core) -- the tightest of any Kyverno component in
  either profile (LOCAL's identical container: 500m) -- on a container doing CPU-bound crypto
  verification plus external network I/O; under real sustained contention this throttles the
  verification call, and when it fails/errors/times out internally, the CEL boolean logic
  (`verifyImageSignatures(...).all(e, e > 0)`) cannot distinguish "verification inconclusive due to
  resource starvation" from "genuinely unsigned" -- both collapse to the SAME hard DENY, blocking a
  correctly-signed image. This directly explains the invariant 17-test signature across all 7
  consecutive runs this session (none of ROUNDs 1-5's fixes touch pod admission at all, so this
  mechanism persisted unchanged through every one of them).
  (10, ROUND 7, SUPERSEDES (9)'s proximate-mechanism attribution -- the CURRENT LEADING ROOT
  CAUSE for the invariant 17-test signature, direct-evidence-backed, NOT YET FIXED, awaiting
  user decision on fix direction): the Kyverno `require-signed-images` denial of
  discover/stage/dbt_build/publish KPO pods is caused by the KPO XCom SIDECAR image, not by
  csv-processor's own signature and not by Kyverno CPU throttling. Chain, every link directly
  verified in ROUND 7's post-run analysis (run 32834311083): (a) `airflow/dags/_common/kpo.py`
  line 136 sets `do_xcom_push: True` on all 4 KPO tasks; (b) the constraints-pinned provider
  `apache-airflow-providers-cncf-kubernetes==10.19.0` injects a sidecar container with
  `XCOM_SIDECAR_IMAGE = "alpine:3.24.1"` (`utils/xcom_sidecar.py` line 31) into every such pod
  at pod-create time; (c) `kubernetes/kyverno-policy.yaml`'s `matchImageReferences` exception
  list pins stale `'alpine:3.17'` -- the list was enumerated from static `make manifests`
  renders plus a live Vault query, and a runtime-injected sidecar image structurally cannot
  appear in any static render, so alpine:3.24.1 was never enumerated; (d) an unexempted image
  must pass cosign keyless verification against publish.yml's OIDC identity, which
  docker.io/library/alpine can never satisfy -- a STRUCTURAL, deterministic denial (explains
  9-run invariance and why every resourcing/concurrency/timeout/retry fix produced zero
  movement); (e) a second, CI-specific aggravating leg: ROUND 7's DENY text is 'failed to
  evaluate policy: GET https://index.docker.io/v2/library/alpine/manifests/3.24.1: unexpected
  status code 429 Too Many Requests' -- Docker Hub anonymous per-IP rate limiting on GitHub's
  shared runner egress IPs aborts even the manifest fetch, and failurePolicy: Fail collapses
  evaluation-error and verification-false into the same hard Deny. (9)'s
  CPU-throttling framing is superseded: its 200m->500m fix was live-refuted in ROUND 6's own
  falsification, and its 'denied image: csv-processor' attribution was inference (the custom
  deny message names no image), whereas (10)'s alpine attribution is read directly from the
  DENY text. ROUND 8 UPDATE: the LOCAL reconciliation is ANSWERED (local does NOT pass --
  identical provider/sidecar/policy-gap, DENY text live-reproduced 1:1 via server-side dry-run
  probe, 22 consecutive local stage failures since 08-24 08:38Z; 'local passes' was a stale
  impression), and root cause (10) is now the CONFIRMED root cause with a live before/after
  falsification on a second independent cluster -- fix (11) below applies user-chosen
  direction A. FOLLOW-UP B (recorded verbatim, deliberately NOT blocking the green signal,
  implement only if step-1 verification goes green and budget remains): mirror the sidecar
  image to GHCR, sign it in publish.yml, point KPO's sidecar_container_image in
  airflow/dags/_common/kpo.py at the signed mirror, then remove the step-1 exemption --
  restoring the verify-everything posture and removing the Docker Hub 429 exposure entirely.
  ROUND 8 POST-RUN CORRECTION (run 32845181597): (10)'s MECHANISM is CONFIRMED AND FIXED --
  zero Kyverno denials on CI, discover reached success try=1 for the first time all session --
  but its ATTRIBUTION as the sole proximate cause of the 17-test signature is REFUTED (10th
  consecutive identical node-ID set despite the mechanism being verifiably gone; see
  Eliminated ROUND 8 entry). (10) was one of multiple stacked sufficient causes behind the
  same saturated 180s-timeout symptom. The signature's REMAINING causes, directly observed in
  the same run: the scheduler-OOM class (3)/(3b)/(3c) back at 9 restarts (worse than ROUND 3's
  3, plausibly because KPO pods now actually execute), an UNEXPLAINED early upstream_failed
  cascade wedging the first two DagRuns for 47min (dbt_build upstream_failed seconds after
  wait_for_files success with discover never launched -- ROUND 9's prime target), and the
  global max_active_tis_per_dag=1 throughput ceiling (ROUND 5's mechanism, now live-visible:
  269 task-concurrency-reached messages). NOT YET RESOLVED -- ROUND 9 direction awaiting user
  decision.
  (12, ROUND 9, CONFIRMED -- the deterministic mechanism behind the dbt_build upstream_failed
  stamps and the premature-publish churn, REFUTING both ROUND 8 suspects for that leg): the
  Airflow Connection `analytics_db_default` is NEVER provisioned by any code path --
  scripts/vault-bootstrap.py's `_ensure_airflow_secrets` wrote ONLY
  `airflow/connections/minio_default` -- and the connection existed solely as an ad-hoc,
  hand-written Vault secret on the long-lived LOCAL cluster (a prior session's 'Rule 3,
  infra-only, no repo file changed' live repair, recorded in test_backfill_2year_sweep.py's
  module docstring finding 4 after 22/22 consecutive local failures and a direct root-token
  `vault kv list`). Every ephemeral CI cluster re-bootstraps Vault from the script, so on CI
  the connection NEVER exists: `list_run_ids_pending_dbt_build` (a ROOT task, no upstream,
  Airflow-default retries=0, resolving `BaseHook.get_connection("analytics_db_default")` at
  DagRun start) fails ~30s into EVERY DagRun; `all_success` short-circuits
  `mark_dbt_build_running` and `dbt_build` to upstream_failed immediately (the observed
  12:07:51/59 and 12:55:13/19 stamps -- present in ALL FOUR ROUND 8 DagRuns including the
  healthy-scheduler replacement runs, and in every DagRun of rounds 5/6/7, load-independent);
  the two `all_done` recorder tasks then complete (mark_done's failed-upstream XCom resolves
  None -> its own `if not run_ids: return` no-op), so `publish` -- whose ONLY upstream is
  mark_done -- launches PREMATURELY before stage has run, fails against nothing-staged, and
  burns its 6 retries as real ~2min KPO pods holding the GLOBAL max_active_tis_per_dag=1
  publish slot and node CPU. This is deferred-items.md plan 11-09 'defect 1' (CRITICAL, Open)
  firing 100% deterministically on CI, not just during injected fault windows -- and it means
  dbt_build has NEVER once run on CI, so silver/normalized are never (re)built and every
  'normalized.customers has fewer than N rows' test precondition is structurally
  unsatisfiable there. The DAG-wiring half of defect 1 (publish not gated on stage) remains
  tracked in deferred-items.md, deliberately NOT fixed here.
  (13, ROUND 9, CONFIRMED -- the scheduler-OOM mechanism, refining (3)/(3b)/(3c)): the CI
  scheduler OOM is BURST-CONCURRENCY, not a leak. Direct cgroup time-series evidence (ROUND
  5's instrumentation, run 32845181597, 17s cadence): stable ~360MiB/17-pid idle baseline
  (scheduler + 8 pre-forked LocalExecutor workers) for minutes at a time, then an ABRUPT
  one-to-two-sample spike to 1331-1533MiB with pids jumping to 24-25 immediately before every
  one of the 8 observed OOMKills -- a dispatch burst filling all core.parallelism=8 slots
  launches ~8 task processes simultaneously, each importing the full Airflow tree (~145MiB
  RSS), for a transient worst case of ~360 + 8x150 ~= 1.6-1.7GiB against the 1536Mi limit
  (which ROUND 2 sized against parallelism=16's different regime, when Kyverno-denied KPO
  pods kept task lifetimes short). The ~6min crash cadence is re-synchronized dispatch waves
  (retry backoff + post-restart backlog), and the first two DagRuns' 47min wedge is the (3c)
  orphan-reset livelock in self-resonant form: the wedged runs' tasks are dispatched IN the
  burst that OOMs the pod, so they die mid-run every cycle and are reset without retry
  accounting. Decisive corroboration: the crash-loop STOPS PERMANENTLY at 12:54:44 -- the
  exact moment dagrun_timeout kills the two wedged runs -- and the post-wedge phase peaks at
  1496MiB with pids<=22 and zero further OOMs.
  (14, ROUND 10, LIVE-CONFIRMED by run 32873456327: with the 200m request + platform trims
  verifiably in force, all 20 stage TIs succeeded try=1 at ~30-32s -- the first stage
  successes EVER on CI -- with ZERO FailedScheduling events; originally confirmed by
  convergent analysis -- the mechanism behind the ENTIRE post-(12)/(13) residual failure census): the
  stage and publish KPO pods are STRUCTURALLY UNSCHEDULABLE on the CI node.
  _STAGE_RESOURCES (airflow/dags/csv_ingest_customers.py, shared by stage AND publish)
  requests cpu=500m/memory=1Gi, but the CI node has only ~220m free CPU at steady state
  (3000m allocatable per kind/cluster-ci.yaml's own derivation, minus 2780m/92% of platform
  requests -- including rounds 1-2's own deliberate scheduler/dag-processor raises); memory
  is a non-issue (26% requested). kube-scheduler can never place a 500m-request pod, the KPO
  default startup_timeout_seconds=120 fires, and the attempt fails after ~129s -- a failure
  mode kpo.py's own comments already documented as 'routine under this cluster's tight node
  CPU budget'. Run-32855002333 evidence: every recorded stage attempt lasted 128-130s with
  try counts to 6 and zero successes ever (discover, 100m request, succeeded try=1 in 11-23s
  in all 4 DagRuns); the pilot file stayed meta.ingestion_runs status=PENDING through an
  1800s window (a started stage container would have advanced it -- no stage container has
  EVER run on CI); the single global max_active_tis_per_dag=1 stage slot was 100%-occupied
  by these guaranteed-failing attempts (288 throttle messages), starving sibling DagRuns'
  stage TIs entirely and holding each DagRun to its full 45min dagrun_timeout -- which in
  turn pins max_active_runs=1, makes fresh-file discovery miss every 180s test window, and
  makes backfills overrun and collide (AlreadyRunningBackfill x12). Publish shares the same
  500m profile, so even a staged run could not publish. Corpus size is irrelevant
  (19 x ~3.3KiB files); corpus count (10-file batch depth) only matters after (14) is fixed.
  (15, ROUND 10 post-run, CANDIDATE -- named with direct evidence, NOT yet root-caused to a
  specific code line; no fix proposed pending user decision checkpoint): with (14) fixed,
  dbt_build pods run for the first time ever on CI and fail DETERMINISTICALLY every try
  (~30s wall, 6 pods Failed, dbt 'Completed with 2 errors'): (15a) model silver_orders --
  'null value in column dataset_id of relation dedup_audit violates not-null constraint'
  (failing row: run_id 37, dataset 'orders', dataset_id NULL) -- the dedup-audit insert
  cannot resolve a dataset_id for 'orders' on a fresh CI cluster; (15b) test
  reconciliation_customers -- 'permission denied for table datasets': the compiled test SQL
  reads meta.datasets directly, which the least-privilege dbt_app role (migrations
  0021/0028; 0028's narrow lookup function exists precisely to avoid this) cannot SELECT.
  Never visible before because dbt had NEVER executed on CI (blocked by (12), then (14)).
  Knock-on chain (accounts for the rest of the 17-set): dbt_build retries hold each DagRun
  to dagrun_timeout=45min -> max_active_runs=1 pins the scheduled slot -> 7 discovery-window
  misses, 3 AlreadyRunningBackfill, publish never runs (<2 rows / SCD2 preconditions),
  DBT_BUILD never RUNNING. Two INDEPENDENT test-side artifacts, also nameable: (i)
  test_smoke_dag_xcom_contains_built_sha compares the XCom's full 40-char sha against
  `git rev-parse --short HEAD` -- the image IS from the checked-out commit (both ROUND 9 and
  ROUND 10 show the same full-vs-short shape); (ii) test_no_extra_schemas_exist flags
  schema 'meta' which the project's own migrations create (stale allowlist suspicion,
  1 confirmation read needed).
  ROUND 11 UPDATE: (15) is now CONFIRMED (upgraded from candidate) with full mechanism
  detail and a direct local reproduction -- see the ROUND 11 Evidence entry. (15a) root
  cause: dedup_audit_post_hook's UNCONDITIONAL audit INSERT resolves dataset_id via
  meta.dataset_id_for_name(), which returns NULL for any dataset not yet registered in
  meta.datasets; registration happens ONLY via that dataset's own ingestion path (no seed,
  no config-sync DAG exists), so a whole-project dbt build on a fresh cluster fails
  silver_orders before orders' first ingest. LATENT LOCALLY (orders pre-registered as
  dataset_id=1 by early local ingests) => a production-shaped fresh-deployment defect CI
  caught. (15b) root cause: both singular reconciliation tests JOIN meta.datasets directly,
  which dbt_app (the role dbt runs as in BOTH profiles, per vault-bootstrap (k)) deliberately
  cannot SELECT (D-08 boundary). NOT latent locally: reproduced 1:1 via SET ROLE dbt_app on
  the live local warehouse, and local dbt_build has had ZERO successes since 2026-08-20
  (when a dbt image containing the 08-19-committed reconciliation tests rolled out) --
  fixing 15b also repairs LOCAL dbt_build, broken since then and previously unnoticed.
  ROUND 11 POST-RUN: (15) LIVE-CONFIRMED on run 32884691063 -- dbt_build success try=1 in
  all 7 reaching DagRuns, zero NOT-NULL/permission errors; first-ever complete end-to-end
  CI DagRun (backfill__10:24, 6m28s). NEW RESIDUAL CANDIDATE (16) -- publish poison: after
  the first successful publish, EVERY subsequent DagRun's publish deterministically trips
  mass_delete_circuit_breaker (QualityThresholdExceeded, vanished 27 / current 50 = 54% >
  10% threshold; current_count=50 == _ROWS_PER_DAY, gold current == one day's roster; NO
  mass-delete snapshot object exists in raw/customers/ -- only the 12 corpus files),
  publish retries=6 exhaust, DagRun wedges to exactly dagrun_timeout=45min, and because
  tripped runs never publish their files are never marked ingested -> re-discovered next
  run -> SELF-SUSTAINING poison; 4x45min wedges blew the job's timeout-minutes: 120
  (conclusion cancelled at 2h00m42s).
  ROUND 12 UPDATE: (16) is now CONFIRMED (upgraded from candidate) with a local
  red/green reproduction and every chain link source-read. Actual mechanism (the
  ROUND 11 'older day-window' inference was only the trigger half): on a fresh cluster
  the first discovery mints idempotency keys with schema_version_term='' (no schema
  version exists yet); staging registers v1 then v2 (the corpus' day-5+ extra column,
  INFERRED); the next discovery's term '2' makes ALL files -- including SUCCEEDED ones
  -- eligible again (D-18 formula replay, BY DESIGN, replay_of_run_id set), producing a
  full replay wave of BYTE-IDENTICAL bronze rows (same event_ts/_source_row_number/
  _file_id via idempotent create_file -- only _run_id differs). silver_customers'
  dedup ranking (event_ts desc, _source_row_number desc, _file_id desc) FULL-TIES
  between each resident row and its replay, the arbitrary winner keeps its own
  _run_id (live: 23 new/27 old; local repro: 26/24), and _VANISHED_SQL's then
  silver-scoped 'staged snapshot' read every tie-loser as vanished: 27/50 = 54% (local
  repro: 24/50 = 48%) > 10% -> trip -> tx rollback -> runs stay STAGED -> identical
  trip forever. Riders resolved as non-bugs: 'skipped try=6' is dagrun_timeout
  force-skipping the up_for_retry TI; the +45:00 sit is try 7's ~16min exponential
  backoff reaching past dagrun_timeout (publish was up_for_retry, not resolved).
  Production-shaped: any D-18 replay (schema/config/processor change) of
  snapshot-semantics data would trip on correct traffic -- or, under a permissive
  threshold, silently apply delete semantics to keys that are present. B-leg: corpus
  churn is structurally 2% max; corpus/threshold correctly sized; no corpus change.
  ROUND 12 POST-RUN: (16) LIVE-CONFIRMED AND CLOSED on run 33051719850 (headSha 794db33)
  -- zero breaker trips in the whole run, 63/63 DagRuns success, zero +45:00 deaths,
  zero failed TIs (344 total), publish success try=1 everywhere; the new diagnostics
  match every prediction exactly (schema_versions ''->'2' flip as v1 CONTRACT + v2
  INFERRED; replay wave runs 25-34 with replay_of_run_id lineage; silver _run_id
  distribution {35:1, 36:49} = deterministic 16c winner; run 36's 49-key pass = 2%
  vanished, clean publish). NEW ROOT CAUSE (17) exposed beneath it (whack-a-mole
  continues, 6th layer): csv_ingest_orders -- Asset-scheduled off customers_asset
  (schedule=[customers_asset], the ONLY way orders can run; CLI backfill structurally
  impossible per DagNonPeriodicScheduleException) -- is PAUSED on every fresh/ephemeral
  cluster (Airflow default dags_are_paused_at_creation=true; no repo override; no
  is_paused_upon_creation on the @dag) and NOTHING in the repo unpauses it
  (_unpause_slice_dags covers only smoke+customers despite its 'both this phase's DAGs'
  docstring; Makefile cluster-up unpauses only smoke). A paused DAG silently consumes
  no asset events, so orders never ran (~60 emitted events, zero orders pods all run),
  meta.ingestion_runs never gained orders rows, and test_full_2year_sweep's
  _wait_for_dataset_files_terminal(dataset=orders, timeout=5400) consumed the final
  87.6min of the 120min budget (job cancel beat the test's own deadline by ~2min,
  destroying the pytest summary). Differential: the long-lived LOCAL cluster has
  is_paused=False from an earlier session's hand-unpause -- same
  local-hand-state-vs-ephemeral-CI class as root cause (12). ROUND 13 UPDATE: the user
  approved ALL THREE candidate shapes (A+B+C; C explicitly approved as a
  production-semantics change on the no-silent-drops rationale, applied ONLY to
  csv_ingest_orders per the recorded asset-vs-cron survey); fix (17) implemented and
  offline-verified, awaiting live CI verification against the ROUND 13 pre-registered
  criteria.
  ROUND 13 POST-RUN: (17) LIVE-CONFIRMED AND CLOSED on run 33062702180 (headSha
  4d3db56) -- cluster-up's unpause line fired 10:28:07; FIRST-EVER orders execution on
  CI (runs 25-34 + replay 49-60 + on-demand 624 ALL SUCCEEDED; orders schema v1->v2
  evolution; days 12/13 drained); the sweep's orders wait completed in minutes (was
  87.6min) and the suite reached its FINAL test module before the 120-min cancel.
  Remaining, NOT a root-cause failure but a priced decision: (18a) the mass-delete
  breaker test's deliberately-truncated snapshot is also cron-visible, and retrying a
  DETERMINISTIC QualityThresholdExceeded (publish retries=6/7 + backoff) held the
  cron dag's max_active_runs=1 slot for 44min (cron gap 11:09->11:53) -- ~40min of
  avoidable budget burn per run; and the honest as-is job projects to ~2h35m-2h50m
  (pytest ~2h10m + never-started observability/capstone steps) vs timeout-minutes
  120. (18b) watch-only: transient Insufficient-cpu bursts at first customers+orders
  co-scheduling (7 pods, self-healed) and scheduler peak 1881MiB = 91.9% of 2048Mi.
  ROUND 14 POST-RUN: (18) LIVE-CONFIRMED AND CLOSED on run 33080823061 (headSha
  a247b67) -- the mass-delete fixture was claimed by ONE cron pass and terminally
  QUARANTINED within ~2.5min (run 421, the only QUARANTINED row; carrying cron
  SUCCEEDED try=1 in 3m39s; test PASSED in 2m12s vs R13's ~40min collateral + 45min
  backfill death); 53 wall-to-wall cron DagRuns with zero wedges/gaps/+45:00 deaths
  and publish try=1 everywhere; zero new failing node-IDs. The first legible census
  (pytest -v rider) unmasked NEW FINDING (20) beneath it (whack-a-mole, 7th layer;
  PRE-EXISTING -- identical signature in ROUND 13's pre-quarantine dump, so NOT a
  fix-(18) regression): every e2e single-file CUSTOMERS fixture run wedges at
  meta.ingestion_runs status=RUNNING in the STAGE phase with zero bronze rows (runs
  823/863/959/1015/1071/1127/1211; ORDERS e2e-orphan run 1212 SUCCEEDED --
  customers-only). Mechanism, each leg evidence-backed: a genuine stage try-1 pod
  CRASHES seconds after claim_ingestion_run commits status=RUNNING + a 5-min lease
  (etl-monitor caught phase=Failed stage pods in the exact try-1 windows); Airflow's
  retry lands inside the still-live lease -> SKIPPED_CONCURRENT receipt -> the retry
  TI reports SUCCESS having staged nothing (SILENT DROP at the task-status layer);
  later crons re-offer the file and every genuine attempt crashes identically
  (deterministic); the test burns its wait-timeout (73.5min across 9 FAILED tests)
  and teardown deletes the fixture, orphaning the run at RUNNING forever. Inner
  exception UNKNOWN pending LOCAL repro (eliminated by this run's own evidence:
  filename-mask, FailedScheduling/KPO startup, Kyverno, VOLUME barrier; NOT
  eliminated: stage-side RejectionRateCircuitBreaker, schema/parse/loader errors
  specific to the customers_small-derived fixture shape). Design gaps standing
  regardless: (20a) claim-then-crash + retry-inside-lease = task SUCCESS with the
  run unstaged (nothing ever writes FAILED to release the claim -- core-value
  violation); (20b) quarantine-silver leakage: silver retains the quarantined run's
  rows and publisher.publish upserts the WHOLE silver table with no _run_id filter,
  so quarantine blocks the PASS, not the DATA. Candidate (19) did NOT fire (zero e2e
  QUARANTINED -- the wedge is upstream of publish; stays latent). Budget: cancelled
  at 2h31m01s of 150; honest as-is ~3h03m-3h20m; green-suite projection with (20)
  fixed ~158-188min -- 150 does not hold even green (raise to ~180-190 or trim the
  sweep module's 47min scd+pilot throughput cost). Sweep failure at 14:48:31 (exact
  assert unknown, no traceback) needs ROUND 15 adjudication.
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
  (7, ROUND 3): `airflow/dags/csv_ingest_customers.py` and `airflow/dags/csv_ingest_orders.py`:
  added `dagrun_timeout=pendulum.duration(minutes=45)` to both `@dag()` decorators (pendulum
  already imported in both, no new import needed) plus a short justification comment on each.
  Targets root_cause (3c) directly: Airflow's `dagrun_timeout` enforcement
  (`SchedulerJobRunner._schedule_dag_run`, confirmed via direct source read of the installed
  apache-airflow==3.3.0) is purely DB-state-driven (checks `dag_run.start_date` vs
  `dag.dagrun_timeout` fresh every scheduling loop for every active DagRun) and therefore SURVIVES
  a scheduler restart, unlike the in-memory-executor-event-driven retry-exhaustion path that the
  OOM-livelock bypasses. On timeout it force-sets `dag_run.state=FAILED` AND explicitly sets every
  unfinished TaskInstance (state in `State.unfinished` or `None`) to `SKIPPED` -- directly breaking
  the reset-and-reschedule loop for that DagRun's stuck TI(s) rather than merely flagging it. This
  frees the DAG's `max_active_runs=1` slot and the shared `max_active_tis_per_dag=1` slot for
  subsequent DagRuns, and lets `_mark_backfills_complete()` finally observe no RUNNING/QUEUED
  DagRuns for a stuck backfill, unblocking the `AlreadyRunningBackfill` cascade. 45 minutes reuses
  this test suite's own already-established "single-window backfill" 2700s precedent
  (test_backfill_2year_sweep.py) rather than inventing a new number, chosen to comfortably exceed
  the documented ~13-15min normal-case duration (integrity_gate+stage alone) plus realistic
  KubernetesJobWatcher-race retry/backoff overhead (retry_delay is Airflow's stock 5min default,
  uncapped exponential, confirmed via grep -- no project override exists), while still being a
  REAL, finite bound versus the current fully-unbounded (runs for the rest of the suite) state.
  Does NOT touch `core.parallelism` or scheduler memory limits at all -- a genuinely different,
  complementary fix axis to ROUND 2's own, addressing the specific gap ROUND 2's live data
  exposed. Incidentally fixed alongside (same precedent as this session's other incidentally-found
  fixes): `tests/policy/test_dag_line_budget.py`'s shared 152-line budget bumped to 155 (exact
  minimal amount needed -- `csv_ingest_orders.py` needed 3 new lines and had zero headroom left at
  152; `csv_ingest_customers.py` was already over budget before this fix, unaffected/unchanged
  status, tracked separately as out of scope), following this exact test file's own established,
  twice-precedented "bump by the exact minimal lines needed, with a written justification"
  convention; and one pre-existing (confirmed via `git show HEAD` + bare-HEAD `ruff check`,
  unrelated to this fix) E501 line-length violation in `csv_ingest_customers.py`'s `dbt_build`
  comment, trivially shortened to fit under 100 chars with zero new lines.
  (8, ROUND 4): `tests/e2e/slice/test_backfill_2year_sweep.py` -- removed
  `_set_customers_dag_paused`/`_pause_customers_dag_for_backfill_only_tests`/
  `_live_concurrency_needs_dag_unpaused` entirely (the module-scoped autouse fixture that paused
  `csv_ingest_customers`, its sibling per-test unpause/re-pause fixture, and their shared helper),
  removed the 2 `@pytest.mark.usefixtures("_live_concurrency_needs_dag_unpaused")` call sites on
  `test_live_run_concurrent_with_backfill_same_dataset` and
  `test_scd_concurrent_attribute_change_and_correction_same_key`, and updated the 4 affected
  docstring/comment locations (module docstring's "Plan 10-07" section finding 4; the removed
  code's own former location, now a documented explanation of why; both tests' own docstrings/
  comments referencing the removed fixtures) so nothing in the file still describes behavior that
  no longer exists. `csv_ingest_customers` now simply stays unpaused for this module too, exactly
  as `tests/e2e/slice/conftest.py`'s own session-scoped `_unpause_slice_dags` fixture already
  guarantees for every other file in `tests/e2e/slice/`. Test-file-only change -- zero production
  DAG code, zero Helm/manifest changes, purely ADDITIVE on top of ROUNDs 1-3's own fixes (does
  not touch `core.parallelism`, scheduler/dagProcessor resource sizing, `dagrun_timeout`, or any
  of the vault-0 fixes -- all remain in place per scope_guardrails).
  (9, ROUND 6): `helm/values/ci/kyverno.yaml`: `admissionController.container.resources.limits.cpu`
  200m -> 500m (matches LOCAL's own already-proven value for the identical container,
  `helm/values/local/kyverno.yaml`, unchanged). LIMIT only -- `requests.cpu` stays 50m, so this
  costs ZERO CI CPU-request budget (`test_manifest_resources.py::request_totals()` sums requests
  only, confirmed via direct source read; the CI profile currently has only 20m of request headroom
  left, confirmed via a fresh `make manifests` + direct computation, so a request-side change was
  not viable). Targets the CONFIRMED mechanism directly: gives the admission-controller's CPU-bound
  cosign verification + external network I/O more burst headroom to complete reliably under real
  cluster-slice-verify contention, reducing the odds of the verification call itself
  timing-out/erroring internally (which the policy's CEL logic cannot distinguish from a genuinely
  unsigned image). `airflow/dags/csv_ingest_customers.py`: added a new shared constant
  `_KYVERNO_RETRY_DELAY = pendulum.duration(seconds=30)` (pendulum already imported, no new import)
  and `retry_delay=_KYVERNO_RETRY_DELAY` to all 4 KubernetesPodOperator tasks whose pods are subject
  to Kyverno's require-signed-images check (`discover`, `stage`, `dbt_build`, `publish`) --
  `retries=6`/`retry_exponential_backoff=True` both left unchanged (already deliberately tuned for
  the unrelated KubernetesJobWatcher race, per each task's own existing comment; the DEFAULT
  `retry_delay` this replaces is Airflow's stock 5-minute base, which combined with exponential
  backoff only fits 2-3 attempts inside `dagrun_timeout=45min`, matching exactly what this round's
  live evidence showed -- discover reached only try=2/try=3 before the DagRun's own 45min ceiling
  force-skipped it). A short, still-backed-off 30s base fits many more independent attempts into the
  SAME existing 45-minute budget, giving transient Kyverno-verification failures far more chances to
  clear without weakening the fail-closed policy itself. Defense-in-depth, complementary to the
  kyverno.yaml CPU-limit fix -- targets "how many chances does a transient failure get" rather than
  "how likely is each individual attempt to fail." `csv_ingest_orders.py` intentionally NOT touched
  this round (same mechanism plausibly applies, but none of the 17 in-scope failing tests are in its
  own pipeline -- noted as a documented follow-up, not fixed here, matching this session's own
  established precedent for related-but-out-of-scope findings).
  (11, ROUND 8): `kubernetes/kyverno-policy.yaml`: added `'alpine:3.24.1'` and
  `'docker.io/library/alpine:3.24.1'` (defensive dual ref form; the bare form is what provider
  10.19.0 writes into the pod spec and what Kyverno's `ref` matches, per the existing
  `alpine:3.17` precedent) to the `matchImageReferences` exception list, in sorted positions,
  plus a header paragraph documenting: the runtime-injection indirection (a KPO XCom sidecar
  structurally cannot appear in any `make manifests` render, so static re-enumeration must not
  remove these entries), that `alpine:3.17` is NOT dead weight (it is the CNPG db-ping-test
  Job's container, live in all four `build/manifests/{local,ci}/cnpg-*.yaml` renders --
  verified, KEPT), the provider-bump coupling (re-read XCOM_SIDECAR_IMAGE on any
  cncf-kubernetes provider bump), and the follow-up-B removal plan. Exemption removes BOTH
  denial legs at once: an exempted image never enters `images.containers`, so neither the
  structurally-impossible cosign verification nor the Docker-Hub-rate-limited manifest fetch
  ever happens for it. Also applied live to the LOCAL cluster (identical `kubectl apply -f`
  shape to `scripts/stages/26-kyverno-policy.sh`), incidentally repairing LOCAL's own
  since-08-24 KPO denial and its missing caeeae4 kindest entries. No chart values, DAG code,
  test code, or CI workflow touched -- one committed file.
  (12, ROUND 9): `scripts/vault-bootstrap.py`: `_ensure_airflow_secrets` now also provisions
  `airflow/connections/analytics_db_default` (same read-then-skip-or-write InvalidPath guard
  as every other `_ensure_*`), with `conn_uri` copied VERBATIM from the `etl/analytics-db`
  `dsn` field `_ensure_etl_secrets` (h) guarantees exists earlier in the same `bootstrap()`
  invocation -- the SAME `etl_app` credential the prior session's ad-hoc local repair used,
  no new role/privilege, and the recovery read is deliberately un-caught (an InvalidPath
  there would be a genuine bootstrap-ordering bug that must surface). Module docstring gains
  step (i2) documenting the whole indirection. The raw `postgresql://` DSN as `conn_uri` was
  offline-verified against the installed stack: Connection(uri=...) normalizes conn_type to
  `postgres`, get_uri() emits `postgres://...`, psycopg `conninfo_to_dict` parses it cleanly
  (both schemes libpq-valid). Idempotent on LOCAL (guard skips the existing hand-written
  secret, read-only); e2e-full.yml already runs `make vault-bootstrap` on every CI
  cluster-up, so CI picks this up with zero workflow changes. `tests/unit/
  test_vault_bootstrap.py`: existing conn-uri test extended to model the new read pattern
  (per-path side_effect; asserts BOTH writes, analytics conn_uri == the etl dsn verbatim)
  plus a NEW guard test (already-present connection is never overwritten and no recovery
  read happens -- protecting the local hand-written secret).
  (13, ROUND 9): `helm/values/ci/airflow.yaml`: scheduler.resources.limits.memory
  1536Mi -> 2048Mi (request untouched at 512Mi -- zero CI requests-budget cost, node memory
  requests ~26% of allocatable so limit overcommit is safe), with the values comment
  documenting the measured burst model (stable 360MiB baseline + ~8 simultaneous task
  processes x ~145MiB import cost ~= 1.6-1.7GiB transient > old 1536Mi ceiling; 2048Mi
  covers the full-burst worst case at the ROUND 7-chosen parallelism=8 with ~20% margin).
  Same CI-only limit-raise shape as ROUND 2's precedent but grounded in the direct
  per-process burst evidence, not trend extrapolation. parallelism=8 (the burst bound
  itself) deliberately unchanged.
  (14, ROUND 10, user-chosen A+B combination -- fallback to plain A NOT needed): B leg --
  `airflow/dags/_common/kpo.py` gains `stage_pod_resources()`: builds the heavy stage/publish
  V1ResourceRequirements with the CPU REQUEST read from Airflow Variable `stage_cpu_request`
  (default '500m' via `Variable.get(key, default=...)`; memory request 1Gi and limits
  2CPU/4Gi hardcoded, identical everywhere). Both DAGs
  (`csv_ingest_customers.py`/`csv_ingest_orders.py`) replace their duplicated hardcoded
  `_STAGE_RESOURCES` literal with `stage_pod_resources()` -- customers' publish deliberately
  keeps sharing the heavy profile (plan 10-07 precedent), orders' publish stays on the light
  100m profile. LOCAL never sets the Variable and gets 500m verbatim (zero new local
  bootstrap); CI sets `stage_cpu_request=200m` in `scripts/ci-set-workload-images.sh` -- the
  exact csv_processor_image-precedent `airflow variables set` site, invoked by all three e2e
  workflows post-cluster-up. A REQUEST is a scheduling reservation, not a cap: the untouched
  2-CPU limit means placed pods burst identically in both profiles. A leg --
  `helm/values/ci/{cnpg-analytics,minio,vault}.yaml` CPU requests trimmed 200->100m /
  100->50m / 100->50m respectively (limits untouched; local values untouched): frees ~200m
  of idle platform reservation at the node level (~220m -> ~420m free), so a 200m stage pod
  plus a 100m discover pod co-schedule with margin. Bundled (same round, user-approved):
  sweep corpus filler shrink `_NUM_DAYS` 20->13 (19->12 real files, `_MASTER_SEED` v4->v5
  per the content-hash idempotency precedent, all six anomaly features preserved at
  re-derived indices gap=3/schema=5/late_event=7/attr=8/correction=10(offset 7 -> gap
  date)/missing=12) -- the user's preferred post-fix drain lever through the single global
  stage slot; and e2e-full.yml's cp-monitor + dump step gain rolling etl-namespace
  pod/FailedScheduling-event capture (events expire ~1h, so only rolling capture yields a
  trustworthy census -- closes root cause (14)'s recorded HONEST GAP) plus an end-of-run
  `airflow variables get stage_cpu_request` proof-of-fix-in-force probe.
  (15, ROUND 11, user-chosen scope B): (15a) `dbt/macros/dedup_audit_post_hook.sql`: the
  audit INSERT gains a registration guard -- `where meta.dataset_id_for_name('{{
  dataset_name }}') is not null` -- plus docstring point 5 documenting the fresh-deployment
  rationale, the unregistered=>empty-staging invariant (a skipped row is always a
  zero-information no-op: counts 0, NULL run-id range), the loud-failure property if that
  invariant were ever breached (dedup_decisions' NOT NULL dedup_audit_id), and WHY it is
  deliberately NOT a skip-when-new_bronze-empty guard (reconciliation_post_hook's floor
  excludes the current build's own audit row BY IDENTITY and relies on the
  every-invocation-writes-a-row behavior for REGISTERED datasets; in the unregistered
  branch the sibling macro provably no-ops on its own: max(dedup_audit_id) over zero rows
  -> NULL -> floor 0 -> bronze_files empty -> zero rows). NOT a nullable-column workaround,
  no migration-time data seeding (structure/data boundary), no new dbt_app privileges.
  (15b) `dbt/tests/reconciliation_customers.sql` + `dbt/tests/reconciliation_orders.sql`:
  the `join meta.datasets` is replaced with `rr.dataset_id =
  meta.dataset_id_for_name('<dataset>')` (migration 0028's purpose-built least-privilege
  lookup; dbt_app keeps zero grant on meta.datasets, headers document the D-08 boundary
  and the observed denial). NEW REGRESSION TEST: `tests/integration/test_dbt_dedup_audit.py::
  test_whole_project_build_on_a_fresh_unregistered_database_writes_no_audit_rows` -- spins
  its OWN throwaway PG18 container (the shared migrated_dsn gets 'customers'/'orders'
  registered by sibling files, and env.py's wrong-database guard demands the name
  'analytics', so a second container is the minimal honest fresh-cluster simulation), runs
  alembic head + a WHOLE-project dbt build with ZERO datasets registered, asserts build
  green + zero dedup_audit rows + zero reconciliation_results rows. Artifact (i):
  `tests/e2e/slice/test_smoke_and_idempotency.py::test_smoke_dag_xcom_contains_built_sha`
  now compares the XCom sha as a min-length-guarded (>=7) PREFIX of `git rev-parse HEAD` --
  Makefile bakes the short sha locally, publish.yml bakes the full `${{ github.sha }}` on
  CI, both name the same commit; equality-vs-one-shape was the bug (image provably correct
  in rounds 9/10); U1 spike-doc template text updated to match. Artifact (ii):
  `tests/e2e/cluster/test_postgres_topology.py` ALLOWED_SCHEMAS += 'meta' with a comment
  citing migration 0001 (creates meta) and 0012/0013/0038 (grant analytics_owner -- the
  analytics-db-app user the test connects as -- USAGE/SELECT on meta objects, which is
  exactly why information_schema.schemata reports it); staging/silver/normalized
  deliberately NOT allowlisted (no analytics_owner USAGE -- if one ever appears the test
  should flag it); stale Phase-3 'no schema exists yet' docstring corrected.
  (16, ROUND 12, user-chosen A+B charter): (16a)
  packages/dataplat/src/dataplat/scd/delete_detection.py: _VANISHED_SQL's
  staged_snapshot CTE reads staging.customers (bronze) scoped by staged_run_ids --
  bronze IS the pass's delivered key set by construction, immune to silver dedup-tie
  lineage -- plus a customer_id IS NOT NULL guard (one NULL inside NOT IN silently
  empties the vanished set); module + function docstrings document the replay-tie
  mechanism. (16b) packages/dataplat/src/dataplat/load/publish/scd.py:
  _SNAPSHOT_MAX_EVENT_TS_SQL same silver->staging rescope (SCD-06 effective dating from
  the pass's own delivered rows; the silver-scoped read could return NULL/partial under
  a tie-losing pass). (16c) dbt/models/silver/silver_customers.sql +
  silver_orders.sql: ranking gains a final '_run_id desc' tie-break (deterministic
  newest-run winner under byte-identical D-18 replays; fixes the section-67 determinism
  violation in silver lineage). (16d) NEW
  tests/integration/test_scd_replay_delete_detection.py: fresh-PG18 red/green replay
  regression (wave 1 published, byte-identical replay wave, asserts vanished==0 AND a
  clean publish at customers.yaml's real 0.10 threshold; logs the tie split). (16e)
  tests/integration/test_scd_delete_detection.py updated to bronze-scoped semantics +
  new unit-level (16) guard test_replayed_key_with_stale_silver_lineage_is_never_
  vanished. (16f) .github/workflows/e2e-full.yml: ROUND 12 always()-diagnostics block
  (run->file mapping incl. replay_of_run_id, schema_versions history, silver _run_id
  distribution, per-run bronze counts via psql in the CNPG primary) -- closes ROUND
  11's evidence gap permanently. Deliberately NOT changed: retries/quarantine behavior
  on deterministic breaker trips (production semantics -- proposed as a decision item),
  timeout-minutes (deferred to post-fix measurement per charter), corpus/threshold
  (B-leg analysis proved them correctly sized).
  (17, ROUND 13, user-chosen A+B+C -- C explicitly approved as production semantics):
  (17A) tests/e2e/slice/conftest.py: _ORDERS_DAG_ID added and the _unpause_slice_dags
  session-autouse tuple extended to (smoke, customers, orders); docstring truthed-up
  (previously claimed 'both this phase's DAGs' while covering only two) and now
  documents the paused-asset-consumer silent-drop mechanism; tests/e2e/chaos/conftest.py
  inherits automatically via its existing import. (17B) Makefile cluster-up target:
  retried csv_ingest_orders unpause appended after scripts/cluster-up.sh (24x5s
  DagNotFound retry loop, smoke-verify's own idiom; hard fail after 120s -- a silent
  skip would recreate the bug), sited at cluster-up because e2e-full.yml never runs
  smoke-verify and only cluster-up covers ALL fresh-cluster consumers including the
  rebuild-from-raw capstone. (17C) airflow/dags/csv_ingest_orders.py:
  is_paused_upon_creation=False on the @dag (2-line comment + kwarg; recorded
  rationale: a paused Asset-scheduled DAG silently drops its upstream's asset events,
  violating the platform's no-silent-drops core value on every fresh deployment);
  applied to csv_ingest_orders ALONE per the survey (customers/smoke/retention are
  cron/interval -- pausing delays runs visibly, no silent drop; chaos probes
  schedule=None, pause state irrelevant); tests/policy/test_dag_line_budget.py orders
  budget <=158 -> <=161 (exact 3 lines, precedent-style justification). Deliberately
  NOT changed: timeout-minutes stays 120 (measured this round per charter); no
  schedule-based DAG touched; carried follow-ups remain captured (quarantine-vs-retry,
  sidecar mirror, pytest progress observability, sweep-wait legibility).
  (18, ROUND 14, user-chosen Option C -- trim ii explicitly approved as production
  semantics): (18-ii, PRODUCTION) packages/dataplat/src/dataplat/pipeline/run.py:
  publish_ingest catches QualityThresholdExceeded ONLY (the error hierarchy's own
  'deliberate business-rule rollback, not an infrastructure failure' line,
  errors.PublicationError docstring) -- quarantines the tripped pass terminally
  (every claimed run -> status='QUARANTINED' via update_ingestion_run_status; no
  migration needed, meta.ingestion_runs.status is unconstrained Text) and returns
  {'status': 'QUARANTINED', runs_quarantined, reason} so the CLI exits 0: a
  quality-gate refusal is a recorded data DISPOSITION (section-51 quarantine), not a
  task failure -- no Airflow retries, no max_active_runs hold, no re-poisoned later
  passes; ALL other exceptions keep propagating (transient class retains its full
  retry budget). packages/dataplat/src/dataplat/discovery.py: new
  _TERMINAL_NON_REOFFERABLE_STATUSES = {SUCCEEDED, QUARANTINED} at both re-offer skip
  sites -- a quarantined batch is permanently invisible to discovery (recovery =
  explicit operator re-open, or a corrected file as a NEW raw object per section-63).
  (18-i, TEST-SCOPED) tests/e2e/slice/test_backfill_2year_sweep.py: mass-delete test
  rewritten -- fixture delivered via the live */1 cron (backfill machinery removed;
  the letter 'no cron run ever sees it' is structurally unachievable given
  whole-prefix discovery, recorded conflict resolution per the user's safer-subset
  rule), expects terminal QUARANTINED, gold-unchanged assertion kept;
  _TERMINAL_RUN_STATUSES += QUARANTINED. (18-iii, CI CONFIG)
  airflow/dags/_common/kpo.py publish_retries() (Variable, default '6' = the pre-fix
  literal; LOCAL unchanged) + csv_ingest_customers.py publish
  retries=publish_retries() (net-zero lines) + scripts/ci-set-workload-images.sh sets
  publish_retries=3 on CI (transient-class sizing: 4 attempts over ~12min covers the
  measured ~5min burst). (18-timeout) .github/workflows/e2e-full.yml timeout-minutes
  120 -> 150 (measured arithmetic: ~1h55-2h10 trimmed projection + margin).
  (18-rider) Makefile cluster-slice-verify + observability-verify-ci pytest -q -> -v
  (CI-only targets; newline-terminated per-test lines survive job cancellation --
  ends the 3-round census blindness). NEW COVERAGE: integration
  test_breaker_trip_quarantines_the_pass_while_transient_errors_still_raise (both
  classification branches against fresh PG18 + real dbt) + unit
  test_discover_files_never_re_offers_a_quarantined_run. PRE-REGISTERED alongside:
  candidate (19) -- lone partial-file e2e fixtures may now quarantine legibly
  (pre-existing snapshot-delivery-shape tension, NOT expanded into this round).
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
  LIVE-VERIFIED INSUFFICIENT (ROUND 3 finding, see Evidence "ROUND 3 -- ROUND 2 fix
  live-verification results" and root_cause (3c) above): run 32755940740/job 97523386546
  (commit b1ef8e2) showed a real, measurable, PARTIAL improvement (time-to-first-restart delayed
  79s->40min, restart count 7->6, peak_pids 48->33 tracking the pool trim exactly as designed)
  but scheduler restarts did NOT drop to 0 -- 6 restarts remained, still ending in a direct
  `Reason: OOMKilled` against the raised 1536Mi ceiling, and peak memory as a percentage of the
  ceiling got WORSE (89%->95.8%) despite the pool being halved. This directly motivated ROUND 3's
  fix (7) above -- a mechanism-level fix, not a further ceiling raise.
  Fix (7, ROUND 3): offline-verified -- `python -m py_compile` clean on all 3 touched files;
  `ruff check` 0 issues (including one incidentally-found, confirmed-pre-existing E501 fixed
  alongside, verified via `git show HEAD` + bare-HEAD `ruff check` reproducing the identical
  error before this fix touched anything); `ruff format --check --diff` clean; `mypy` -- 0 NEW
  errors (132 error-output lines both before and after, confirmed via `git stash`/`git stash pop`
  A/B comparison -- all pre-existing, about `common_kpo_kwargs`/`XComArg` typing this fix never
  touched); `uv run pytest tests/unit/test_dag_structure.py tests/policy/test_dag_line_budget.py
  -q` -- 15/16 pass, the 1 failure is the SAME pre-existing `csv_ingest_customers.py` over-budget
  test (now 191 vs the bumped 155 budget -- same failure status as before this fix, not a new
  regression); `uv run pytest tests/policy/ -q -m "not manifests"` (167 collectible) -- 157
  passed, 2 failed, the SAME 2 pre-existing, already-documented out-of-scope failures every prior
  round in this file has shown (confirmed identical count and identical failing tests); `uv run
  pytest tests/unit/ -q` -- 554/554 pass, zero regressions; `uv run --frozen --group cluster
  pytest tests/dagtest -q` (real `dag.test()` behavioral suite against a live testcontainers
  Airflow metadata DB) -- 14/14 pass, confirming Airflow actually ACCEPTS and correctly applies
  `dagrun_timeout=pendulum.duration(minutes=45)` at real DAG-execution time, not just at
  DagBag-import time.
  LIVE-VERIFIED (CORRECTED, ROUND 4 -- see Evidence "ROUND 4 -- orchestrator-supplied live
  verification of ROUND 3 fix (7)"): run 32765704491/job 97565550961 (commit 20d151f) showed
  scheduler restarts drop 6->3, a real, measurable, monotonic improvement across ROUNDs 1-3's
  three genuinely different mechanisms -- BUT the exact same 17-test failure SET recurred
  unchanged, proving fix (7) (and ROUNDs 1-3 collectively) was never sufficient to resolve this
  debug session's own primary symptom, because that symptom's actual root cause (8, below) is a
  test-suite bug fully independent of scheduler restart/OOM behavior. See root_cause (8) for the
  mechanistic explanation of WHY dagrun_timeout structurally cannot reach this specific failure
  mode (the affected DagRuns never leave `queued`, so `dagrun_timeout` enforcement -- which only
  examines already-RUNNING DagRuns -- never even considers them).
  Fix (8, ROUND 4): offline-verified -- `python -m py_compile` clean; `ruff check` 0 new issues
  (2 pre-existing E501/W505 findings at an untouched line, confirmed identical via `git show
  HEAD:... | ruff check -` on the unmodified file); `ruff format --check --diff` -- 86 diff lines
  both before and after this fix (byte-identical, confirmed via `git show HEAD:...` -- pre-
  existing project-wide formatting drift, none of it in code this fix touched); `mypy` -- 0
  errors; `pytest --collect-only` -- all 7 tests in the module still collect cleanly in the
  identical order; repo-wide grep confirms zero remaining "pause" (non-"unpause") call sites
  anywhere in `tests/e2e/`, and zero remaining references to the removed fixture names outside
  this one file; `uv run pytest tests/policy/ -q -m "not manifests"` (159 collectible) -- 157
  passed, 2 failed, the SAME 2 pre-existing, already-documented out-of-scope failures every prior
  round in this file has shown -- zero new regressions.
  LIVE-VERIFIED INSUFFICIENT (ROUND 5 reconciliation, direct evidence -- see Evidence "ROUND 5 --
  Phase 0 reconciliation" above): run 32779160265/job 97597115875 (headSha f0ebfe3, fix (8) itself,
  confirmed via direct `gh run view`/`gh api actions/jobs/.../logs`, not inference from a summary)
  shows "17 failed, 21 passed, 6 skipped... in 3659.44s" -- a name-for-name IDENTICAL failing-test
  set to every prior run this session (6th consecutive occurrence), zero tests turned green. Fix
  (8) is proven present/active (matches the orchestrator's own independent deployment-staleness
  finding: this repo's hostPath DAG-mount convention guarantees zero staleness for any fix this
  session has made) yet had zero measurable effect on the 17-test signature -- exactly the
  falsification_test's own predicted "refuted or insufficient" outcome. ROUND 4's own mechanism
  (DAG-pause freezing backfill DagRuns) was real, is not reverted (an independently-valuable fix,
  per scope_guardrails), but is now DISCONFIRMED as the proximate cause of this session's primary
  symptom. Opens ROUND 5 -- see Current Focus above for the strategic reset and suggested
  investigation angles (pytest execution order / non-ephemeral state / upstream-of-Airflow
  discovery failure), none yet tested as of this reconciliation update.
  Archiving this debug session is now blocked on: the still-unanswered human checkpoint for the
  REOPENED ROUND (4b, vault-0 Python-side wait race, already live-verified, awaiting confirmation
  only) AND ROUND 5's own not-yet-started investigation into the actual root cause of the 17-test
  signature.
  Fix (9, ROUND 6): offline-verified -- `python -m py_compile` clean on `csv_ingest_customers.py`;
  `python3 -c "yaml.safe_load(...)"` clean on `kyverno.yaml`; `ruff check` 0 issues on the DAG file;
  `ruff format --check` clean; `mypy` -- 0 NEW errors (74 error-output lines both before and after
  this fix, confirmed via `git stash`/`git stash pop` A/B comparison -- all pre-existing, about
  `common_kpo_kwargs`/`XComArg` typing this fix never touched, same precedent as ROUND 3's own
  verification); `uv run pytest tests/unit/test_dag_structure.py tests/policy/test_dag_line_budget.py
  -q` -- 15/16 pass, the 1 failure is the SAME pre-existing `csv_ingest_customers.py` over-budget
  test (now 201 vs the 155 budget -- already failing pre-fix at 191, not a new regression); `make
  manifests` -- 0 chart lint failures across all 9 charts both profiles, kubeconform -strict 0
  invalid/0 errors across 540 resources; direct render inspection confirms the CI admission-
  controller Deployment's `kyverno` container now carries `limits: {cpu: 500m, memory: 192Mi}` while
  `requests` stays `{cpu: 50m, memory: 64Mi}`; `uv run pytest tests/policy/test_manifest_resources.py
  -q -m manifests` -- 5/5 pass, CPU request total UNCHANGED at 3.180/3.200 cores (confirmed via
  direct `request_totals()` computation before and after -- the limit-only change has zero effect on
  the budget sum, exactly as designed); `uv run pytest tests/policy/ -q -m "not manifests"` (169
  collectible) -- 157 passed, 2 failed, the SAME 2 pre-existing, already-documented out-of-scope
  failures every prior round in this file has shown (`test_dag_line_budget.py`,
  `test_gates_actually_fail.py`) -- zero new regressions.
  LIVE VERIFICATION: pending -- see Current Focus's own next_action for the push-and-wait plan.
  Fix (11, ROUND 8): STRONGEST pre-CI verification of any round this session -- a genuine live
  before/after falsification on an independent cluster exhibiting the identical failure:
  (a) BEFORE: server-side dry-run of a KPO-shaped mixed pod (base localhost:5001/csv-processor
  + sidecar alpine:3.24.1) against the LOCAL cluster's etl namespace DENIED with the
  byte-identical CI DENY text including the Docker Hub 429; (b) applied the updated committed
  file to the same cluster; (c) AFTER (~2 min later, same still-rate-limited network): same
  probe ADMITTED; (d) NEGATIVE CONTROL: unexempted alpine:3.19 probe still DENIED --
  fail-closed enforcement intact. Offline battery all green: YAML parse clean; `make
  manifests` kubeconform -strict 540 resources / 0 invalid / 0 errors; `tests/policy -m "not
  manifests"` 157 passed / 2 failed -- the SAME 2 pre-existing out-of-scope failures as every
  prior round (test_dag_line_budget.py customers budget, test_gates_actually_fail.py), zero
  new regressions; test_manifest_resources 5/5 (no chart values touched, CPU budget
  unaffected); test_values_profiles 6/6; dagtest 14/14. LIVE CI VERIFICATION (run
  32845181597, ROUND 8 post-run analysis): MECHANISM-LEVEL SUCCESS -- exemption verifiably
  applied at cluster-up, ZERO Kyverno denials in the entire run (vs 14-18 in rounds 6/7),
  zero Docker Hub 429s, and discover reached state=success try=1 in 11-15s twice (the first
  CI discover successes of the session). SIGNATURE-LEVEL INSUFFICIENT -- the invariant
  17-test node-ID set recurred identically (10th consecutive) because further stacked causes
  (scheduler OOM crash-loop at 9 restarts, first-DagRun upstream_failed wedge,
  max_active_tis_per_dag throttling) independently exceed the tests' 180s windows. Fix (11)
  STAYS (real mechanism, really fixed); see Eliminated ROUND 8 entry and root_cause (10)'s
  ROUND 8 POST-RUN CORRECTION.
  Fixes (12)+(13), ROUND 9: offline battery ALL GREEN, zero new regressions --
  `python -m py_compile` + `ruff check` + `ruff format --check` + `mypy` clean on
  scripts/vault-bootstrap.py AND tests/unit/test_vault_bootstrap.py; YAML parse clean on
  helm/values/ci/airflow.yaml; `make manifests` kubeconform -strict 540 resources /
  0 invalid / 0 errors, rendered build/manifests/ci/airflow.yaml carries `memory: 2048Mi`
  (2 occurrences -- scheduler pod's paired containers, same render shape as the prior 1536Mi,
  0 remaining 1536Mi); `tests/policy/test_manifest_resources.py -m manifests` 5/5 (CPU
  request budget UNCHANGED -- limit-only change); `test_values_profiles.py` 6/6;
  `tests/policy -m "not manifests"` 157 passed / 2 failed -- the SAME 2 pre-existing
  out-of-scope failures as every prior round; `tests/dagtest` 14/14; `tests/unit` 555/555
  (incl. the updated + 1 new vault-bootstrap tests; suite was 554 before, +1 = the new guard
  test). Additional targeted offline proof for (12): live .venv round-trip
  `Connection(uri='postgresql://etl_app:...@analytics-db-rw.data:5432/analytics')` ->
  conn_type 'postgres' -> get_uri() 'postgres://...' -> psycopg.conninfo.conninfo_to_dict
  parses to the correct host/port/user/dbname. LIVE CI VERIFICATION: pending -- judged on
  INTERNAL diagnostics per the ROUND 9 pre-registered falsification test (scheduler restart
  count, zero dbt_build-upstream_failed-with-discover-try=0 stamps, dbt_build reaching a
  real state, Kyverno DENY still 0), with the node-ID diff secondary (saturated instrument).
  Fixes (12)+(13), ROUND 9 LIVE VERIFICATION (run 32855002333, job 97824707600, headSha
  ee87708): ALL primary internal-diagnostic criteria of the pre-registered falsification test
  PASSED -- (1) analytics_db_default 'created' in the bootstrap log; (2) ZERO dbt_build
  upstream_failed stamps (wedge gone; dbt chain starts, meta.run_stages DBT_BUILD row exists
  for the first time on CI); (3) scheduler restarts 0 over 66min (was 9), measured peak
  1559MiB/24pids -- above the old 1536Mi ceiling, below 2048Mi, proving the limit raise
  load-bearing under the recurring parallelism=8 burst; (4) Kyverno DENY 0 (fix 11
  regression-clean). Node-ID set unchanged (11th run, saturated instrument as pre-registered)
  but the failure-template census moved for the first time all session; residual mechanisms
  recorded in the ROUND 9 OUTCOME block and Evidence -- NOT a failure of fixes (12)/(13).
  Fix (14), ROUND 10: offline battery ALL GREEN, zero new regressions -- full detail in the
  ROUND 10 fix-implementation Evidence entry: DagBag proof of BOTH profiles' Variable
  resolution (absent->500m byte-identical to pre-fix, 200m->200m on stage AND customers'
  publish, limits unchanged, orders publish unchanged); corpus byte-level verification of
  all six anomaly features at the new 13-day shape; make manifests kubeconform 540/0/0;
  test_manifest_resources 5/5 with the CI CPU request total dropping 3.180 -> 3.030 of the
  3.200 effective budget (the vault trim additionally lands live-only, outside the rendered
  set); test_values_profiles 6/6; policy not-manifests 157/2 (same 2 pre-existing);
  tests/unit 555/555; tests/dagtest 14/14; ruff/mypy/format/bash -n all clean or
  byte-identical-pre-existing. LIVE CI VERIFICATION (run 32873456327, headSha d0d1ad6):
  ALL FIVE pre-registered primaries PASS -- fix in force (Variable 200m effective, node
  requests 2780m->2580m), FIRST-EVER stage successes on CI (20/20 TIs try=1 ~30-32s, zero
  128-130s startup-timeout failures), FailedScheduling census ZERO, Kyverno DENY 0,
  control-plane restarts 0 (scheduler peak 1644MiB < 2048Mi). Root cause (14) CONFIRMED and
  its fix verified at mechanism level. Suite NOT yet green: residual candidate (15) (dbt
  layer -- see root_cause (15) and the ROUND 10 post-run Evidence entry) now dominates the
  unchanged 17-node-ID set via its dagrun_timeout wedge knock-ons.
  Fix (15) + artifacts, ROUND 11: offline battery ALL GREEN, zero new regressions, WITH a
  genuine red/green falsification of the (15a) guard: the NEW fresh-database integration
  test reproduces the EXACT CI error with the guard reverted (`git stash` the macro only ->
  dbt build fails 'null value in column "dataset_id" of relation "dedup_audit" violates
  not-null constraint' -- byte-matching run 32873456327's failure) and passes with the
  guard in place (build green, 0 dedup_audit rows, 0 reconciliation_results rows on a
  never-registered database). Full battery: tests/integration/test_dbt_dedup_audit.py +
  test_dbt_reconciliation.py 5/5 (these execute REAL `dbt build`s through the modified
  macro AND the rewritten singular tests -- compiled-SQL correctness proven);
  test_dbt_silver_dedup.py 4/4; tests/unit 555/555; tests/dagtest 14/14; tests/policy
  -m "not manifests" 157 passed / 2 failed -- the SAME 2 pre-existing out-of-scope
  failures as every prior round (test_dag_line_budget, test_gates_actually_fail);
  test_manifest_resources 5/5 + test_values_profiles 6/6 (no helm changes this round);
  make manifests kubeconform -strict 540 resources / 0 invalid / 0 errors; ruff check +
  ruff format --check + mypy clean on all three touched Python test files; collect-only
  clean on both modified e2e files. The (15b) rewrite is additionally grounded in the
  direct local reproduction: the OLD join fails as dbt_app ('permission denied for table
  datasets') while meta.dataset_id_for_name() succeeds as dbt_app (returns 1/76) on the
  live local warehouse. LIVE CI VERIFICATION (run 32884691063, headSha 377c068, cancelled
  at the 120-min job timeout -- verdict from always()-diagnostics): criteria (a)/(b)/(d)
  ALL MET -- dbt_build success try=1 in all 7 reaching DagRuns (first ever on CI), zero
  NOT-NULL/permission-denied, stage 60/60 try=1, FailedScheduling 0, Kyverno DENY 0,
  restarts 0 (scheduler peak 1288MiB < 2048Mi); first-ever complete end-to-end CI DagRun
  (6m28s incl. publish success). Fix (15) CONFIRMED WORKING. Criterion (c) failed for the
  NEW residual (16) (publish mass-delete-breaker poison, 45-min wedges); criterion (e)
  node-ID diff unmeasurable (cancelled step's streamed stdout not archived). Suite NOT
  yet green: (16) dominates.
  Fix (16), ROUND 12: offline battery ALL GREEN with a genuine red/green falsification
  and a whole-directory A/B -- RED: the NEW fresh-database replay regression reproduces
  the exact live trip class pre-fix ('replay of identical content read 24/50 keys as
  vanished (48%)', QualityThresholdExceeded at threshold 0.10; measured arbitrary tie
  split 26/24). GREEN post-fix: vanished==0, publish succeeds, tie split deterministic
  50/0 (16c load-bearing). Battery: tests/unit 555/555; tests/dagtest 14/14;
  tests/policy -m 'not manifests' 157 passed / 2 failed -- the SAME 2 pre-existing
  out-of-scope failures as every prior round; make manifests kubeconform -strict 540
  resources / 0 invalid / 0 errors; test_manifest_resources 5/5 (no chart values
  touched); test_values_profiles 6/6; targeted integration
  test_scd_delete_detection 14/14, dbt suites (silver_dedup/dedup_audit/
  silver_incremental/reconciliation) + replay test 12/12; FULL tests/integration
  directory A/B against clean HEAD: failure set BYTE-IDENTICAL (21 pre-existing
  local-env/order-dependent failures, empty diff -- zero regressions; these 21 fail on
  unmodified HEAD on this machine and are out of this session's scope); ruff check
  clean; ruff format diffs byte-identical pre-existing drift; mypy errors
  identical-class pre-existing test idiom (test_publish_scd precedent); e2e-full.yml
  YAML parse clean. Implementation defect caught by the battery itself and fixed
  in-round: Jinja '{#- -#}' trim markers in the first silver_orders.sql edit glued
  'partition by order_id' to 'order by' -- switched to non-trimming '{# #}', all dbt
  suites re-green. LIVE CI VERIFICATION: COMPLETE on run 33051719850 (2026-08-27) --
  criteria (b) zero breaker trips + zero wedges + zero +45:00 deaths: MET; (c)
  regression guards all green (FailedScheduling 0, Kyverno 0, restarts 0, scheduler
  1415MiB<2048Mi): MET; (16f) diagnostics matched every prediction (schema-term flip,
  replay lineage, deterministic silver winner, 2% churn clean publish): MET; (d) budget:
  honest floor 33min, remainder consumed by NEW root cause (17) (orders DAG paused on
  ephemeral CI -- see root_cause); (e) node-ID census: not measurable (cancelled job has
  no pytest summary; -q dots never flush). (16) CLOSED.
  Fix (17), ROUND 13: offline battery ALL GREEN, zero new regressions -- throwaway
  DagBag proof via the tests/unit conftest fixture: csv_ingest_orders
  is_paused_upon_creation is False AND every other DAG stays None (default), 2/2
  passed; tests/unit 555/555; tests/dagtest 14/14 (real dag.test() accepts the new
  kwarg); tests/policy -m 'not manifests' 157 passed / 2 failed -- the SAME 2
  pre-existing out-of-scope failures as every prior round (test_dag_line_budget
  customers 208>158 tracked separately; test_gates_actually_fail), with the ORDERS
  budget test passing at exactly 161/161; make manifests kubeconform -strict 540
  resources / 0 invalid / 0 errors (no helm changes); test_manifest_resources 5/5;
  test_values_profiles 6/6; make -n cluster-up parses cleanly; py_compile + ruff
  check + ruff format --check clean on all touched files; mypy A/B via git stash --
  74 error lines both pre- and post-change (all pre-existing
  common_kpo_kwargs/XComArg idiom, zero new). Heavy tests/integration suites
  deliberately NOT re-run: no dataplat/dbt/csv_processor code touched this round
  (DAG kwarg + test fixture + Makefile only). LIVE CI VERIFICATION: COMPLETE on run
  33062702180 -- criteria (1)/(a)/(b) MET (unpause line 10:28:07; orders runs 25-34 +
  49-60 + 624 SUCCEEDED; sweep drained, suite reached the final module); criterion (c)
  budget FAILED at 120min with full decomposition recorded (honest projection
  ~2h35m-2h50m as-is, ~1h55m-2h10m with the (18a) collateral trimmed); guards: Kyverno
  0, restarts 0, fix (16) zero unwanted trips, but FailedScheduling burst 10:42-10:47
  (7 pods, transient, self-healed) and scheduler peak 91.9% of limit; census
  unmeasurable (3rd blind round, pytest -q). (17) CLOSED; decision checkpoint on
  timeout-vs-trim returned.
  Fix (18), ROUND 14: offline battery ALL GREEN, zero new regressions, with a genuine
  BOTH-BRANCHES classification proof: the new fresh-PG18 integration test drives the
  REAL publish_ingest pass-claiming path -- a 35/50-key truncated pass at the real
  0.10 threshold returns {'status': 'QUARANTINED', 'runs_quarantined': [run]}, the
  run's status is terminally QUARANTINED, list_staged_run_ids comes back empty (no
  re-poisoned later pass), gold is byte-for-byte unchanged (pre-mutation barrier),
  and a follow-up publish_ingest is a clean SUCCEEDED no-op; a monkeypatched
  publisher raising PublicationError (the infrastructure class) propagates OUT with
  the pass still STAGED (retry budget intact). Full battery: tests/unit 556/556 (+1
  new discovery non-reoffer test); tests/dagtest 14/14; tests/policy -m 'not
  manifests' 157/2 -- the SAME 2 pre-existing out-of-scope failures (customers
  line-budget A/B-confirmed 208 lines both sides = net-zero DAG change);
  make manifests kubeconform -strict 540/0/0; test_manifest_resources 5/5 (no chart
  values touched); test_values_profiles 6/6; publish-path integration failing set
  BYTE-IDENTICAL to clean HEAD (diff empty; 5 of the known 21 pre-existing local-env
  failures); test_publish_scd 7/7; sweep module collect-only 7/7 same order; ruff/
  format/mypy clean or byte-identical-pre-existing (stash A/B); DagBag per-profile
  proof EXACT (no Variable -> publish retries 6 byte-identical pre-fix;
  AIRFLOW_VAR_PUBLISH_RETRIES=3 -> 3; orders publish 3 / stage 6 untouched both
  profiles); e2e-full.yml YAML parse + make -n clean. One in-round defect caught by
  the battery itself and fixed (gold-snapshot query referenced nonexistent
  valid_from; event_ts doubles as valid_from per migration 0035). LIVE CI
  VERIFICATION: COMPLETE on run 33080823061 -- criteria (b)/(c)/(e) MET (run 421
  terminally QUARANTINED via one cron pass in ~2.5min, test PASSED 2m12s, 53
  wall-to-wall cron runs with zero collateral, first legible per-test census: 7 of
  the 17 baseline PASSED / 9 FAILED / 1 unfinished, zero NEW failing node-IDs);
  criterion (d) guards clean except the 18b caveat (one self-healed FailedScheduling
  burst overlapping the failed sweep test; scheduler peak 86.3%); criterion (a)
  budget FAILED at the new 150 ceiling (cancelled 2h31m01s) for a NEW pre-existing
  mechanism -- finding (20), stage-phase claim-then-crash + retry-inside-lease
  silent drop (see root_cause ROUND 14 POST-RUN). Candidate (19) did not fire.
  (18) CLOSED; decision checkpoint on ROUND 15 direction returned.
  ROUND 15 (fix 20 + 20a, offline-verified red->green, live verification pending):
  root cause (20) = IncompatibleSchemaError schema-column-disappeared(signup_country):
  every wedging e2e fixture is a 5-col customers file vs the 6-col contract whose
  6th column is required:false (D-13), but classify_schema_change had no
  optional-column concept -- the raise lands AFTER claim commits RUNNING+lease, the
  pod exits nonzero, and (20a)'s two gaps (no crash release; SKIPPED_CONCURRENT on
  refused claim) convert the retry into silent SUCCESS. FIX (20): optional_columns
  parameter on classify_schema_change (hash inputs untouched);
  _resolve_schema resolves a strict-prefix-with-optional-tail header to the CONTRACT
  version by hash (never an INFERRED current-flip) and gains a contract-prefix guard
  on the new-column path (closes a latent positional-corruption hole);
  _OptionalColumnPadStage pads absent trailing optional columns with None right
  after RaggedRowGuard (+ defense-in-depth pad at the hash site); ColumnContract
  docstring corrected. FIX (20a): LEG 1 fail_ingestion_run_claim (guarded
  RUNNING+same-pod -> FAILED) called best-effort from stage_ingest's finally; LEG 2
  _await_concurrent_claim wait-and-reclaim (default 420s,
  DATAPLAT_STAGE_CONCURRENT_WAIT_SECONDS) -- verified duplicate, genuine re-stage,
  or loud DataPlatformError; stage_ingest can no longer return SKIPPED_CONCURRENT.
  Riders: e2e failure-traceback streaming hook (OBS-03 carve-out 3, deliberate);
  integrity_gate in the TI dump; timeout-minutes 190; QUARANTINED added to the
  slice/observability terminal-status sets (fail-fast for the now-expected (19)
  signature). (20b) carried unchanged.
  LIVE CI VERIFICATION, ROUND 15: COMPLETE on run 33103279876 (headSha 25b6eb0,
  conclusion FAILURE at 1h44m12s -- first self-terminating run of the session, 45%
  of the 190 ceiling). Fixes (20)+(20a) LIVE-CONFIRMED IN FULL and CLOSED: every
  5-col e2e fixture staged with bronze rows (zero RUNNING wedges, zero
  SKIPPED_CONCURRENT anywhere), and the podkill SIGKILL exercised LEG 2 live (stage
  try=2 waited out the lease 5m29s then re-staged 1M rows exactly once, zero
  duplicates). Pre-registered prediction (19) FIRED exactly: 8/8 lone-customers e2e
  runs terminal QUARANTINED, failing fast+legibly with streamed tracebacks -- (19)
  is now a live design decision (delivery-shape/e2e-fixture design vs breaker
  scoping), returned via decision checkpoint. First complete census: 28P/10F/6S;
  failure taxonomy fully enumerated: 8x (19), 1x (21) sweep D-05 late-row lineage
  (both dataset waits drained first time ever on CI; data counts exact; attribution
  question), 1x (22) orphan-test InsufficientPrivilege (analytics_owner on schema
  normalized). Sub-finding (23): dbtkill's run_stages[668,DBT_BUILD] never observed
  during its 300s kill-window poll despite the dbt_build TI running inside it.
  Integrity_gate flake class adjudicated: teardown-deletes-fixture race (the one
  failed TI all session fired the exact second dbtkill's teardown ran). Guards
  green; scheduler peak 93.9% of 2048Mi = new high-water (18b watch). (20b) now
  material at scale: 3M+ silver rows retained from QUARANTINED runs.
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
  - airflow/dags/csv_ingest_customers.py
  - airflow/dags/csv_ingest_orders.py
  - tests/policy/test_dag_line_budget.py
  - tests/e2e/slice/test_backfill_2year_sweep.py
  - helm/values/ci/kyverno.yaml
  - kubernetes/kyverno-policy.yaml
  - scripts/vault-bootstrap.py
  - tests/unit/test_vault_bootstrap.py
  - airflow/dags/_common/kpo.py
  - scripts/ci-set-workload-images.sh
  - helm/values/ci/cnpg-analytics.yaml
  - helm/values/ci/minio.yaml
  - helm/values/ci/vault.yaml
  - configs/datasets/customers.yaml
  - .github/workflows/e2e-full.yml
  - dbt/macros/dedup_audit_post_hook.sql
  - dbt/tests/reconciliation_customers.sql
  - dbt/tests/reconciliation_orders.sql
  - tests/integration/test_dbt_dedup_audit.py
  - tests/e2e/slice/test_smoke_and_idempotency.py
  - tests/e2e/cluster/test_postgres_topology.py
  - packages/dataplat/src/dataplat/scd/delete_detection.py
  - packages/dataplat/src/dataplat/load/publish/scd.py
  - dbt/models/silver/silver_customers.sql
  - dbt/models/silver/silver_orders.sql
  - tests/integration/test_scd_replay_delete_detection.py
  - tests/integration/test_scd_delete_detection.py
  - tests/e2e/slice/conftest.py
  - Makefile
  - packages/dataplat/src/dataplat/pipeline/run.py
  - packages/dataplat/src/dataplat/discovery.py
  - tests/unit/test_discovery.py
  - packages/dataplat/src/dataplat/schema/evolution.py
  - packages/csv-processor/src/csv_processor/source.py
  - packages/dataplat/src/dataplat/load/staging.py
  - packages/dataplat/src/dataplat/config/model.py
  - packages/dataplat/src/dataplat/metadata/repository.py
  - packages/dataplat/src/dataplat/metadata/postgres.py
  - packages/csv-processor/src/csv_processor/cli.py
  - tests/e2e/conftest.py
  - tests/e2e/observability/conftest.py
  - tests/policy/test_print_ban_scope.py
  - pyproject.toml
  - tests/unit/schema/test_evolution.py
  - tests/unit/test_csv_processor_cli.py
  - tests/integration/test_stage_ingest.py
  - tests/integration/test_schema_resolution.py
  - tests/integration/test_run_ingest.py
