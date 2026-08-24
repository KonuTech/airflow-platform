---
status: verifying
trigger: "CI pipeline ingestion timeout/contention: real Airflow pipeline runs (discover -> ingest -> publish) never complete within their fixed 180s test timeouts when running on GitHub Actions' single-node ephemeral CI cluster (kind/cluster-ci.yaml, ~3 allocatable CPU), even though the cluster itself comes up healthy. As a result, no test that requires a full DAG run to reach SUCCEEDED has ever been observed passing on GitHub's free-tier runners, blocking Phase 11's CICD-09 requirement from being provable end-to-end."
created: 2026-08-24
updated: 2026-08-24 (continuation session 2 -- dagProcessor memory fix applied + offline-verified, live verification next)
---

## Current Focus
<!-- OVERWRITE on each update - always reflects NOW -->

reasoning_checkpoint:
  hypothesis: "airflow-dag-processor's crash-loop is caused by hitting its container MEMORY limit (512Mi, never touched by the prior CPU-only fix) during fork()-based concurrent DAG-file parsing ('up to 2 processes at a time' across 11 real DAG files, several importing kubernetes.client/hvac/boto3-adjacent libraries), triggering an OOM kill -- a mechanism entirely distinct from the CPU/scheduler_health_check_threshold/dag_file_processor_timeout mechanism already fixed (and confirmed working) for the scheduler. This explains why the prior CPU/threshold fix had statistically zero effect on dag-processor's own restart rate while genuinely helping the scheduler: they have different bottlenecks."
  confirming_evidence:
    - "The CI run's --previous container log for dag-processor (job 97405917287, PR #13, fetched fresh this session) shows the container starting at 11:02:09.084Z, completing a normal startup sequence (found 11 files, dispatched 2 parser subprocesses for smoke_kubernetes_pod.py/csv_ingest_orders.py, both 0 errors), printing ONE 'DAG File Processing Stats' table at 11:02:14.546Z, then the log STOPS -- no shutdown message, no exception, no traceback. An abrupt, silent death within ~5-15s of container start."
    - "This timing is mathematically incompatible with a liveness-probe-driven kill: the pulled chart 1.22.0's own default dagProcessor.livenessProbe (values.yaml lines 3052-3057) is initialDelaySeconds:10/failureThreshold:5/periodSeconds:60 (this session's prior fix only overrode timeoutSeconds to 60s, not these three). 5 CONSECUTIVE failures at periodSeconds:60 requires a minimum of ~250s before a kill; the pod's own restart cadence (5 restarts in ~8m38s, ~100s average) and this specific captured death (~5-15s into container life, BEFORE initialDelaySeconds:10 even fires the first check) do not fit that arithmetic."
    - "Direct live cgroup measurement on the LOCAL cluster (same image, same 11 DAG files; pod airflow-dag-processor-5786f6f97-pldmx, deliberately cold-started via `kubectl delete pod` to mirror CI's exact cold-start scenario, then polled at 0.5s resolution): memory.current holds steady at ~237MiB, then during an actual parse-cycle burst climbs through 271.8 -> 321.3 -> 349.5 -> 371.9 MiB before dropping back to baseline one sample later -- a real, measured +135MiB swing from a single observed cycle (true peak likely higher, unsampled between 0.5s polls)."
    - "CI's dagProcessor memory limit (512Mi) leaves only ~140MiB of margin above this directly-measured LOCAL burst peak, while LOCAL's own dagProcessor memory limit is 1Gi (1024Mi) -- exactly double CI's -- and LOCAL's memory REQUEST alone (512Mi) equals CI's entire LIMIT. LOCAL has never exhibited this crash-loop symptom (per Symptoms.expected: 'This is how the same pipeline reliably behaves against the local persistent 3-node kind cluster'); its own 2 restarts over 19h correlate with a known unrelated host-level quirk (WSL/kind DAG-mount freeze on host resume), not OOM (memory.events on the live pod shows oom_kill: 0)."
    - "Node-level memory in the CI diagnostic snapshot was abundant (22% requests / 44% limits of ~14GB allocatable) -- ruling out node-wide memory pressure and pointing specifically at dag-processor's OWN per-container cgroup limit (512Mi) as the binding constraint, exactly the signature of an individual container hitting its own OOM ceiling regardless of the node's overall health."
    - "Independent web research confirms Airflow 3.x's dag-processor fork()-based multiprocessing is a documented OOM-prone pattern (apache/airflow#50708, #50097, #58509 -- dag-processor memory leaks/growth in 3.x; discussion #53662 on fork() failing with 'Cannot allocate memory' under container memory pressure), corroborating that this failure class is real and known in exactly the component/mechanism this hypothesis names."
  falsification_test: "If, after raising dagProcessor's memory request/limit to match LOCAL's proven-stable sizing (512Mi/1Gi), a fresh live CI run's diagnostic step (this time including `kubectl describe pod` for dag-processor, closing the observability gap that let this mechanism go unconfirmed for a full round) still shows dag-processor restarting at a similar cadence AND the container's Last State reason is NOT OOMKilled, this hypothesis is refuted -- would point to a genuinely different, still-unidentified mechanism (e.g. a startup-time exception not yet captured, or a probe/threshold interaction specific to DagProcessorJob's own heartbeat path distinct from what was fixed for SchedulerJob)."
  fix_rationale: "Targets memory, the ONE resource axis dag-processor's crash-loop investigation had never touched (the prior fix raised CPU and two Airflow-internal timing thresholds only). Raising it to match LOCAL's already-proven-stable dagProcessor sizing (512Mi request / 1Gi limit, running 19+ hours with zero OOM kills on the identical codebase/DAG set) is the smallest, most defensible correction with real precedent, rather than an arbitrary new number. Does not touch CPU (already raised last round with a measured ZERO effect on dag-processor specifically -- further CPU spend is not evidence-justified) or the two Airflow-internal thresholds (already raised; this session's own log analysis shows individual file parses complete in ~0.09-0.10s, nowhere near even the original 50s dag_file_processor_timeout default, so it was never the binding constraint)."
  blind_spots: "Cannot obtain a direct 'OOMKilled' reason/exit-code from the ACTUAL failed CI run analyzed here, because its diagnostic step never ran `kubectl describe pod` for dag-processor (only `describe node` and `logs --previous`) and that CI cluster is already torn down -- this hypothesis is inferred from timing-incompatibility-with-liveness-probe-math, live cgroup measurement on a DIFFERENT (LOCAL) cluster under LOWER CPU contention, and cross-profile sizing comparison, not a direct same-instance measurement. If CI's real contention extends forked children's lifetime enough to cause MORE overlapping simultaneous memory use than observed locally, matching LOCAL's 1Gi limit exactly might still be insufficient and could require a further increase in a follow-up round -- this fix deliberately does not pad beyond LOCAL's proven number (smallest defensible change), so the next live-verify step must watch specifically for continued dag-processor restarts even after this lands, not assume success from CPU/memory arithmetic alone. The diagnostic step is being upgraded this round specifically to close the observability gap for next time regardless of outcome."

hypothesis: "airflow-dag-processor crash-loops from hitting its 512Mi memory limit during fork()-based concurrent DAG parsing -- a mechanism the prior CPU-only fix never addressed, unlike scheduler whose bottleneck genuinely was CPU/health-check-threshold and which the prior fix measurably improved"
test: "raise dagProcessor.resources.requests/limits.memory in helm/values/ci/airflow.yaml to match LOCAL's proven-stable sizing (512Mi/1Gi), verify offline (test_manifest_resources.py, make helm-lint against real rendered manifests via the now-located tools/bin/helm), then use a throwaway PR with an upgraded diagnostic step (adds `kubectl describe pod` for dag-processor to directly capture Last State/Reason/Exit Code) to get a direct OOMKilled confirm/refute"
expecting: "a fresh live CI run shows airflow-dag-processor with zero (or dramatically fewer) restarts, and if any restart still occurs, `kubectl describe pod`'s Last State reason is no longer OOMKilled -- confirming memory was the actual binding constraint"
next_action: "Fix applied and offline-verified (make manifests, test_manifest_resources.py -m manifests, test_values_profiles.py, full offline policy suite -- all pass, real CPU margin ~3.08/3.2 cores). Also fixed a separate, incidentally-discovered, pre-existing CI-CPU-budget regression (tempo/otel-collector/monitoring CPU trims) so the policy gate is green again. NEXT: add the upgraded throwaway diagnostic step (kubectl describe pod for dag-processor, to directly capture Last State/Reason/Exit Code) to e2e-smoke.yml on a fresh throwaway branch/PR, trigger a live CI run, fetch its diagnostic step output directly, and compare dag-processor's restart count + Last State reason against this round's falsification_test."

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
  TWO independent root causes, discovered in sequence:
  (1) SCHEDULER + DAG-PROCESSOR (fixed and live-verified for scheduler; dag-processor's own
  restart cadence was UNCHANGED by this part -- see (2) below): CI's single-node kind cluster
  under-sized scheduler/dagProcessor CPU (200m/500m each) for what LocalExecutor requires, and
  Airflow's own internal health-check thresholds (`scheduler_health_check_threshold`,
  `dag_file_processor_timeout`) were tighter than the K8s probe timeoutSeconds a prior fix had
  already raised. Fixed in commit a73282e; scheduler genuinely improved (live-verified).
  (2) DAG-PROCESSOR MEMORY (this continuation session's finding, the piece that actually
  explains why dag-processor kept crash-looping identically after (1) landed): dag-processor's
  memory limit (256Mi request/512Mi limit) was never touched by fix (1) and is the actual
  binding constraint. Its --previous container log (job 97405917287) shows an abrupt, silent
  death ~5-15s into container life, right as it dispatches forked parser subprocesses for its
  11-file DAG bundle -- mathematically too fast to be a liveness-probe kill (chart default
  failureThreshold:5/periodSeconds:60 requires >=250s), consistent instead with an OOM kill.
  Live cgroup measurement on the LOCAL cluster (never exhibits this crash-loop, and provisions
  DOUBLE CI's dagProcessor memory) captured a real parse-cycle memory burst reaching >=372MiB
  against CI's old 512Mi limit -- only ~140MiB of margin. dag-processor's fork()-based
  concurrent parsing of DAG files with heavy imports (kubernetes.client, hvac) under CI's
  tighter memory ceiling is a documented OOM-prone pattern in Airflow 3.x (apache/airflow#50708,
  #50097, #58509, #53662). This is why fix (1) (CPU + threshold only) measurably helped
  scheduler but had ZERO effect on dag-processor's own restart rate: they had genuinely
  different bottlenecks. dag-processor's continued crash-loop is what actually explains every
  fixed-timeout E2E failure this debug session was opened to investigate, since dag-processor
  must stay alive to register any DAG in DagModel at all.
  SEPARATELY (unrelated third finding, fixed alongside for CI hygiene, not part of either root
  cause above): the real Helm-rendered CI-profile CPU total had independently drifted to 3.400
  cores against the 3.200-core EFFECTIVE_CI_CPU_BUDGET (confirmed pre-existing on bare `main`
  via `git stash`, unaffected by either fix above) -- traced to the same-day monitoring-stack
  quick task (260824-ayw) never re-running this specific CI-gated budget check
  (`.github/workflows/ci.yml`'s `check` job runs it via `make manifest-policy`).
fix: >
  (1) helm/values/ci/airflow.yaml: scheduler.resources (request 200m->400m cpu, limit
  500m->1500m cpu) and dagProcessor.resources (request 200m->300m cpu, limit 500m->1200m cpu).
  helm/values/{local,ci}/airflow.yaml identically: config.scheduler.scheduler_health_check_
  threshold: "90", config.dag_processor.dag_file_processor_timeout: "120" (behavioral config,
  not a permitted resource-sizing divergence axis per D-06).
  (2) helm/values/ci/airflow.yaml: dagProcessor.resources.requests/limits.memory 256Mi/512Mi ->
  512Mi/1Gi -- matches LOCAL's already-proven-stable dagProcessor sizing exactly (smallest
  defensible change with real precedent, not an arbitrary new number). CPU and the two
  Airflow-internal thresholds were NOT touched further in this round (already raised in (1) with
  a measured zero effect on dag-processor specifically, so not evidence-justified to spend more
  budget there).
  (3, separate CI-hygiene fix, not part of the dag-processor root cause): trimmed CPU requests
  on helm/values/ci/{tempo,otel-collector,monitoring}.yaml -- tempo/otel-collector 100m->10m
  each (confirmed never deployed live in CI, zero behavioral risk); monitoring.yaml's smallest
  housekeeping/one-shot containers only (grafana initChownData/downloadDashboards/sidecar sync
  10m->5m each, prometheusOperator 20m->10m, its admission-webhook patch Job 10m->5m) --
  grafana/prometheus's own serving containers, Kyverno, and all Airflow components deliberately
  left untouched.
verification: >
  Offline (this continuation session -- tools/bin/helm and tools/bin/kubeconform ARE available
  here): (1) `make manifests` -- 0 chart lint failures across all 9 charts both profiles,
  kubeconform -strict reports 0 invalid/0 errors across 540 resources; (2) `uv run pytest
  tests/policy/test_manifest_resources.py -q -m manifests` -- all 5 tests pass INCLUDING
  `test_ci_profile_fits_runner` against the REAL rendered manifests (not manual arithmetic),
  landing at ~3.08/3.2 cores (~120m real margin) after fix (3)'s trims; (3) `uv run pytest
  tests/policy/test_values_profiles.py -q` -- 6/6 pass, confirming no D-06/D-08 divergence-axis
  violation (the memory change makes CI/local IDENTICAL on dagProcessor memory, and the CPU
  trims stay within the already-permitted resource-sizing axis); (4) `uv run pytest
  tests/policy/ -q -m "not manifests"` (159 collectible) -- 157 pass, 2 fail, both the SAME
  pre-existing, already-documented-out-of-scope failures from earlier in this same debug session
  (test_dag_line_budget.py, test_gates_actually_fail.py) -- nothing new broken by any of these
  changes.
  Live: fix (1) was live-verified via throwaway PR #13 (job 97405917287) -- scheduler genuinely
  improved (1 restart, stays 2/2 Ready) but dag-processor was UNCHANGED (5 restarts in ~9min,
  statistically identical to pre-fix), which is what led to this session's memory investigation.
  Fix (2) (the memory fix) and fix (3) (CPU-budget trim) are NOT yet live-verified -- REQUIRES a
  fresh throwaway-PR run with an upgraded diagnostic step (adding `kubectl describe pod` for
  dag-processor, to directly capture Last State/Reason/Exit Code and close the observability gap
  that left OOM only inferable, not directly confirmed, this round) before this debug session can
  be marked resolved.
files_changed:
  - helm/values/ci/airflow.yaml
  - helm/values/local/airflow.yaml
  - helm/values/ci/tempo.yaml
  - helm/values/ci/otel-collector.yaml
  - helm/values/ci/monitoring.yaml
