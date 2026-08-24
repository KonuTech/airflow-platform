---
status: verifying
trigger: "CI pipeline ingestion timeout/contention: real Airflow pipeline runs (discover -> ingest -> publish) never complete within their fixed 180s test timeouts when running on GitHub Actions' single-node ephemeral CI cluster (kind/cluster-ci.yaml, ~3 allocatable CPU), even though the cluster itself comes up healthy. As a result, no test that requires a full DAG run to reach SUCCEEDED has ever been observed passing on GitHub's free-tier runners, blocking Phase 11's CICD-09 requirement from being provable end-to-end."
created: 2026-08-24
updated: 2026-08-24 (continuation session, after orchestrator handoff)
---

## Current Focus
<!-- OVERWRITE on each update - always reflects NOW -->

reasoning_checkpoint:
  hypothesis: "airflow-scheduler and airflow-dag-processor pods in the CI profile genuinely crash-loop (not merely 'run slowly') under real single-node CPU contention, because (a) their CPU requests/limits (200m/500m each) are far too small for what LocalExecutor requires them to do (scheduler forks+runs ALL task-instance code in-process; dag-processor parses heavyweight DAG files importing kubernetes/hvac/boto3-adjacent libs), and (b) the prior 'fix' (raising K8s livenessProbe/startupProbe timeoutSeconds 20s->60s, commit 5abe533/99197cf) only raised how long kubelet waits for the PROBE COMMAND to execute, but did nothing for Airflow's own internal scheduler_health_check_threshold (default 30s) that `airflow jobs check` uses to decide the SchedulerJob's DB heartbeat is stale -- so a genuinely CPU-starved scheduler that cannot heartbeat within 30s still gets reported unhealthy and killed, regardless of the K8s-level timeout. This produces exactly the observed symptom cluster: DagRuns stuck in 'queued' forever (scheduler dies/restarts before progressing them), and DAGs flapping between registered and DagNotFound (dag-processor dies mid-parse-cycle repeatedly)."
  confirming_evidence:
    - "Live kubectl snapshot captured by e2e-smoke.yml's own 'DEBUG live scheduler/pod/node state' step (run 32675592471, job 97283007457, commit 99197cf -- which ALREADY included the timeoutSeconds:60 probe fix): airflow-dag-processor showed 5 restarts in 8m45s of pod age, most recent 104s before the snapshot; airflow-scheduler-0 showed 1+ restarts and was 1/2 Ready (not fully healthy) at snapshot time."
    - "Same log's kubectl events: 'Warning Unhealthy pod/airflow-scheduler-0 Startup probe failed: No alive jobs found.' and 'Warning Unhealthy pod/airflow-dag-processor... Liveness probe failed:' followed immediately by 'Warning BackOff ... Back-off restarting failed container dag-processor' -- 'No alive jobs found' is `airflow jobs check`'s own DB-heartbeat-staleness message, not a probe-subprocess-spawn-latency symptom, meaning the scheduler was genuinely failing to heartbeat within its internal scheduler_health_check_threshold (30s default), a DIFFERENT mechanism than the K8s probe timeoutSeconds the prior fix touched."
    - "Same node snapshot: 'Allocated resources: cpu 2480m (82%)' of the node's own reported allocatable -- consistent with kind/cluster-ci.yaml's documented ~3000m allocatable math -- committed to steady-state platform REQUESTS alone, before ANY KubernetesPodOperator task pod (discover/stage/dbt_build/publish) is scheduled, leaving very little request headroom (~520m) for the dynamic ETL workload the DAGs launch every 1 minute."
    - "helm/values/ci/kyverno.yaml's own header comment independently documents 'measured this session: 2.650 cores / 5176Mi against a 3.2-core / 13107Mi effective budget' -- corroborating the node-level 82% figure via the project's OWN separate manifest-resources accounting."
    - "Real CI run 97356158949 (e2e-full.yml, run 32699260549) pytest output: `dag_run[dag_id='smoke_kubernetes_pod', ...] did not reach a terminal state within 180s (last observed state: 'queued')` -- a DagRun-level 'queued' stall (scheduler never dispatched it), and separately `airflow.exceptions.DagNotFound: Could not find Dag csv_ingest_customers` from a live `airflow backfill create` CLI call ~9 minutes into the run, on a DAG that IS committed/mounted from cluster boot -- both match crash-loop-induced state loss, not merely slow-but-working parsing."
    - "Web research confirms Airflow's `[scheduler] scheduler_health_check_threshold` (default 30s) is the exact mechanism `airflow jobs check --job-type SchedulerJob --local` (the chart's own startup/liveness probe command) uses to decide DB-heartbeat staleness, and that `[dag_processor] dag_file_processor_timeout` (default 50s) kills an individual DAG file's parse subprocess if it runs long -- both independently corroborated by a live, still-open upstream Airflow issue (apache/airflow#44652, 'Standalone DAG Processor Causes DAGs to Appear and Disappear Frequently') describing this exact appear/disappear symptom under real-world resource pressure."
  falsification_test: "If, after raising scheduler/dagProcessor CPU requests+limits AND raising scheduler_health_check_threshold/dag_file_processor_timeout, a fresh e2e-full.yml or e2e-smoke.yml run still shows RESTARTS>0 on airflow-scheduler-0/airflow-dag-processor pods (via the same DEBUG diagnostic step) or the same DagNotFound/'queued'-stall failure signatures, this hypothesis is refuted or incomplete -- would point to a genuinely different bottleneck (e.g. real per-task-pod scheduling contention once task pods are added, or a code-level Airflow 3 dag-processor bug independent of resourcing)."
  fix_rationale: "The fix targets the CONFIRMED, measured mechanism (repeated scheduler/dag-processor container restarts from failed internal health checks under genuine CPU starvation) rather than the timeout literals in the test suite (180s/120s), which are a downstream SYMPTOM of the control plane being unavailable, not the cause. Raising CPU allocation increases these two pods' cgroup CPU shares (real effect under contention, not just a scheduling-accounting change) and removes their own self-imposed CFS quota ceiling (raised limits); raising the two Airflow-internal thresholds stops Kubernetes from killing a process that is merely running slow (but still making progress) under real contention, mirroring the exact mitigation independently converged on by the upstream Airflow community issue for this same standalone-dag-processor instability pattern."
  blind_spots: "Cannot verify live against a real CI cluster in this session (the CI cluster from every referenced run is already torn down) -- self-verification is limited to (1) re-reading the edited YAML for correctness, (2) manually re-summing CPU requests against the project's own EFFECTIVE_CI_CPU_BUDGET=3.2-core policy ceiling (helm/kubeconform tooling is not installed in this sandbox, so `make manifests` cannot be run to get an exact rendered total). Does not address the SEPARATE, still-likely-real concern that once the control plane stays alive, dynamic KubernetesPodOperator task pods (up to ~1.2 CPU of simultaneous request for one DAG's discover/stage/dbt_build/publish flow) may still contend for the node's remaining ~250-500m of real request headroom -- that is a distinct, not-yet-confirmed follow-on hypothesis this fix does not attempt to resolve, and the human verification step should watch for it specifically (e.g. Pending/Unschedulable task pods) if it reappears after this fix lands."

hypothesis: "airflow-scheduler + airflow-dag-processor crash-loop under real CI CPU contention because their CPU sizing and Airflow-internal health-check thresholds were never adequate for a LocalExecutor CI profile, not merely that the fixed 180s/120s test timeouts are too short"
test: "increase scheduler/dagProcessor CPU requests+limits in helm/values/ci/airflow.yaml, and raise [scheduler] scheduler_health_check_threshold + [dag_processor] dag_file_processor_timeout (identically in both local and ci profiles, since these are behavioral config, not a permitted resource-sizing divergence axis) -- then request a human-verified real CI run"
expecting: "a fresh e2e-full.yml/e2e-smoke.yml run shows zero restarts on airflow-scheduler-0/airflow-dag-processor, and DagRuns/DAG registration no longer flap between healthy and DagNotFound/'queued'-stuck"
next_action: "RE-ANALYZED, STILL NEEDS ONE MORE LIVE DATA POINT -- deeper log analysis (full raw logs, not summaries, across all 7 pre-fix + 1 post-fix e2e-chaos.yml runs and 6 pre-fix + 1 post-fix e2e-full.yml runs from today) resolved 2 of the orchestrator's 3 open questions: the vault-0 restart-timeout failure is a PRE-EXISTING test-code race (kubectl delete-then-immediate-wait races the StatefulSet controller's recreate latency; kubectl wait on a named resource fails fast with NotFound instead of polling for creation) confirmed recurring in pre-fix run 32693178072 (05:33, hours before the fix) -- NOT caused by the fix's CPU increase, moved to Eliminated. The Kyverno webhook timeout in e2e-full.yml is also pre-existing infra flakiness (seen pre-fix in run 32692744455, 05:13) -- unrelated to this fix's scope. HOWEVER the core falsification_test (scheduler/dag-processor RESTARTS count) remains genuinely UNTESTED: e2e-chaos.yml has no diagnostic step at all (structurally cannot produce this evidence, confirmed by reading the workflow file), and the chaos suite's own discovery-timeout test (test_dag_still_resolves_its_connection_and_runs) is not a usable proxy either -- it shows the byte-identical failure signature pre-fix and post-fix (DAG unpause succeeds both times, only the S3KeySensor's 180s poll budget is exceeded both times), meaning this specific test was likely never primarily gated on control-plane crash-looping to begin with. RECOMMENDATION (requires user decision, not to be actioned by this agent): trigger a fresh e2e-smoke.yml run via a throwaway PR (this project's established pattern) to get the one piece of evidence that actually tests the hypothesis -- e2e-smoke.yml's dedicated 'DEBUG live scheduler/pod/node state' step, which directly reports RESTARTS counts on airflow-scheduler-0/airflow-dag-processor. This is the only path to a clean confirm/refute of the falsification_test."

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
  On CI's single-node kind cluster (~3000m real allocatable CPU, LocalExecutor), the
  airflow-scheduler and airflow-dag-processor pods are CPU-undersized (200m request/500m limit
  each) for what LocalExecutor requires of them -- the scheduler runs ALL task-instance code
  in-process (unlike KubernetesExecutor's scheduler, which only dispatches), and the
  dag-processor parses DAG files with heavy imports (kubernetes.client, hvac) -- while the
  static platform's steady-state CPU requests alone already consume ~82% of the node's real
  allocatable capacity before any ETL task pod exists. Under this genuine, measured contention,
  both pods intermittently fail their OWN internal Airflow health checks
  (`scheduler_health_check_threshold`, `dag_file_processor_timeout`, both far tighter than the
  K8s-level probe timeoutSeconds a prior fix this session already raised) and get killed/
  restarted by Kubernetes, losing scheduling/parsing state. This directly produces the observed
  symptoms: DagRuns stuck in 'queued' forever (scheduler dies before dispatching them) and DAGs
  flapping between registered and DagNotFound (dag-processor dies mid-parse-cycle), which in
  turn cascade into every fixed-180s/120s E2E timeout this debug session was opened to explain --
  discovery/ingestion "never completing in time" is a downstream symptom of the control plane
  itself being intermittently unavailable, not merely "everything running slowly but working."
fix: >
  helm/values/ci/airflow.yaml: raised scheduler.resources (request 200m->400m cpu, limit
  500m->1500m cpu) and dagProcessor.resources (request 200m->300m cpu, limit 500m->1200m cpu) --
  stays within the project's own EFFECTIVE_CI_CPU_BUDGET=3.2-core policy ceiling (documented
  prior total 2.650 cores + net +300m = ~2.95 cores). helm/values/{local,ci}/airflow.yaml
  (identically, since this is behavioral config, not a permitted resource-sizing divergence
  axis under tests/policy/test_values_profiles.py's D-06 axis table): added
  config.scheduler.scheduler_health_check_threshold: "90" and
  config.dag_processor.dag_file_processor_timeout: "120", raising Airflow's own internal
  DB-heartbeat-staleness and per-file-parse-timeout thresholds (defaults 30s/50s) so genuine
  but bounded slowness under contention is tolerated rather than treated as process death.
verification: >
  Self-verified (no live CI cluster reachable from this session -- every referenced run's
  ephemeral cluster is already torn down): (1) both edited YAML files parse cleanly and the
  new config keys land at the expected paths (python yaml.safe_load spot-check); (2)
  `uv run pytest tests/policy/test_values_profiles.py -q` -- all 6 tests pass, confirming the
  new config.scheduler/config.dag_processor keys are identical across local/ci (not a stray
  divergence the D-06/D-08 axis policy would reject); (3)
  `uv run pytest tests/policy/test_manifest_resources.py -q -m "not manifests"` -- all 7
  offline-collectible tests pass (helm is not installed in this sandbox, so the 5
  `manifests`-marked tests that need `make manifests`-rendered output, including
  `test_ci_profile_fits_runner` itself, could not run here); (4) manual re-derivation of the CI
  CPU request budget: helm/values/ci/kyverno.yaml's own header comment documents a previously
  *measured* (by the manifest-resources policy test, an earlier phase) total of 2.650 cores
  against the project's EFFECTIVE_CI_CPU_BUDGET=3.2-core ceiling; this fix's net requests delta
  is +300m (scheduler +200m, dagProcessor +100m; apiServer/triggerer/statsd/CNPG/MinIO/Vault/
  Kyverno/ingress-nginx all untouched), landing at an estimated ~2.950 cores -- still under
  budget with ~0.25-core margin, though NOT machine-verified in this session (see blind_spots
  in Current Focus). REQUIRES a human-verified real CI run (push or PR) to confirm: (a)
  `test_ci_profile_fits_runner` still passes with the actual rendered totals, (b) a fresh
  e2e-smoke.yml or e2e-full.yml run shows airflow-scheduler-0/airflow-dag-processor with ZERO
  restarts via the same `kubectl get pods -o wide` diagnostic this debug session used as
  evidence, and (c) the previously-failing fixed-timeout E2E assertions (DagNotFound,
  'queued'-stuck DagRuns, meta.files/meta.ingestion_runs polling timeouts) clear.
files_changed:
  - helm/values/ci/airflow.yaml
  - helm/values/local/airflow.yaml
