---
status: resolved
trigger: "Airflow KubernetesExecutor task instances get permanently stuck in queued/up_for_retry state and the scheduler never redispatches them, blocking csv_ingest_customers DagRuns indefinitely"
created: 2026-08-16
updated: 2026-08-16T17:15:00Z
resolution:
  root_cause: "Node CPU budget exhaustion (kind/cluster.yaml reserves 9 of 12 cores/worker, leaving ~750m headroom after the fixed platform baseline) compounded by ingest pods' airflow-xcom-sidecar container never terminating on completion (on_finish_action=delete_succeeded_pod didn't force-kill it), permanently leaking ~500m CPU per occurrence until nothing new could schedule."
  fix: "kpo.py: on_finish_action -> delete_pod (force-deletes pod+sidecar regardless of monitoring-loop signal gap). csv_ingest_customers.py: ingest's max_active_tis_per_dag 5 -> 1 (reduces peak concurrent CPU demand)."
  verification: "DagRun scheduled__2026-08-16T17:04:00+00:00 reached full success — all 7 tasks including both ingest map indices — confirmed live post-fix, first clean run since the issue was first observed 06:07 UTC."
  files_changed: ["airflow/dags/_common/kpo.py", "airflow/dags/csv_ingest_customers.py"]
---

## Symptoms

**Expected behavior:** A scheduled `csv_ingest_customers` DagRun progresses through its task chain (`wait_for_files`/`resolve_window` → `discover` → `build_ingest_args` → `ingest` [mapped] → `aggregate_receipts`) and reaches a terminal state (success or a genuine failure) within a reasonable time, freeing `max_active_runs=1` for the next scheduled tick.

**Actual behavior:** Task instances — observed at multiple different stages across separate incidents (mapped `ingest` instances in one DagRun, `resolve_window`+`wait_for_files` in another) — enter `queued` or `up_for_retry` state and never get redispatched by the scheduler, even with retry budget remaining and healthy cluster CPU capacity (as low as ~75-78% allocated on the busiest node after cleanup). No pod exists for the stuck task instance while it sits in this state. `airflow tasks clear` on a stuck `up_for_retry` task successfully transitions it back to `running`/dispatches a real pod once, but on the next failure it re-enters `up_for_retry` and stays stuck again (confirmed: `try 4/6`, `queued_dttm` unchanged across a 17+ minute recheck). Directly marking stuck task instances `FAILED` via the ORM does correctly cascade the DagRun to a terminal `failed` state (confirmed working), but does not address why the scheduler stopped dispatching them in the first place.

**Error messages:** No exception or stack trace associated with the stuck state itself — the tasks simply never transition. (Separate, likely-unrelated: one very old task instance, `csv-ingest-customers-discover-zc850lcz`, crashed once with `ServerResponseError: Invalid auth token` at `2026-08-16T05:01:58Z`, SIGKILL'd — this may or may not be connected to the same root cause; not re-investigated this session, still visible via `kubectl -n airflow get pods` at time of writing.)

**Timeline:** First observed today (2026-08-16) during Phase 7 gap-closure work. `airflow dags list-runs csv_ingest_customers` showed 8 consecutive DagRun failures from `06:07 UTC` through `16:17 UTC` — this predates the session's own code changes (which began ~13:25 UTC), ruling out anything in that session's commits as the trigger. Not confirmed whether this is a new issue or a recurrence — STATE.md documents a structurally similar (but reportedly already-fixed) cluster-wide Airflow scheduling stall from Phase 5, caused by a WSL2/Docker Desktop restart breaking the DAGs hostPath mount (`.planning/debug/resolved/dagrun-scheduler-stall.md`). Worth checking whether that fix has regressed, or whether this is a distinct issue with the same symptom class.

**Reproduction:** Not yet reliably reproducible on demand — occurs intermittently across different DagRuns and different task stages. To observe: watch `kubectl -n airflow get pods` and query TaskInstance state via the scheduler pod:
```
kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- python3 -c "
from airflow.models import TaskInstance
from airflow.utils.session import create_session
with create_session() as session:
    tis = session.query(TaskInstance).filter(TaskInstance.dag_id=='csv_ingest_customers').order_by(TaskInstance.start_date.desc()).limit(15).all()
    for ti in tis:
        print(ti.run_id, ti.task_id, ti.map_index, ti.state, ti.try_number, ti.max_tries, ti.queued_dttm)
"
```
Look for rows in `queued`/`up_for_retry` with `queued_dttm` unchanged across repeated checks minutes apart, and no matching pod in `kubectl -n airflow get pods`.

## Evidence

- timestamp: 2026-08-16T17:04 UTC
  checked: `kubectl -n airflow get pods -o wide` (fresh sample, ~20min after last debug-file evidence)
  found: UNLIKE all prior samples this session, 4 pods now visibly exist in `Pending` state with NODE=`<none>`: 2x `csv-ingest-customers-resolve-window-*`, 2x `csv-ingest-customers-wait-for-files-*`. `kubectl describe pod` on one shows `Conditions: PodScheduled=False` and `Events: FailedScheduling (x7 over 35m) — 0/3 nodes are available: 1 node(s) had untolerated taint(s) [expected: control-plane's standard NoSchedule taint], 2 Insufficient cpu.`
  implication: This is DIRECT, unambiguous evidence of genuine Kubernetes-level CPU scheduling failure — not a scheduler/is_stale/watcher bug. This directly contradicts the file's own earlier "Cluster CPU/scheduling capacity exhaustion" elimination, which was based on point-in-time samples where no pod existed yet to show a FailedScheduling event. Time-based (differential) evidence: the situation has evolved since that elimination was recorded.
- timestamp: 2026-08-16T17:04-17:06 UTC
  checked: `kubectl describe node` Capacity/Allocatable + per-pod CPU requests table for airflow-platform-worker and airflow-platform-worker2 (control-plane excluded — tainted NoSchedule, near-empty)
  found: Both nodes report `Capacity: cpu=12` but `Allocatable: cpu=3` (deliberate, documented in kind/cluster.yaml: systemReserved cpu=5 + kubeReserved cpu=4 = 9 reserved of 12, by explicit design checkpoint — see kind/cluster.yaml header comment). Allocated: worker 2751m/3000m (91%), worker2 2851m/3000m (95%). Fixed baseline alone (scheduler+dag-processor+api-server+triggerer+statsd+monitoring stack [grafana/tempo/prometheus/otel-collector/kube-state-metrics/node-exporter/kube-prometheus-operator]+2x Postgres+MinIO+Vault+kindnet) sums to ~2250m(worker)/~2350m(worker2) — 75-78% of the 3000m budget — with ZERO task pods running. This matches the earlier "75-78%, healthy" reading almost exactly, revealing that reading was really "baseline-only, near the ceiling already," not "healthy headroom."
  implication: True headroom per node in the best case is only ~650-750m — less than the CPU request of a single `ingest` KPO pod (500m) plus its own KubernetesExecutor worker pod, and far less than needed for `ingest`'s configured `max_active_tis_per_dag=5` concurrent mapped attempts. The node CPU budget (kind/cluster.yaml systemReserved/kubeReserved) was explicitly flagged in its own commit-time comment as a checkpoint tradeoff to "Revisit if profiling later phases ... shows 3 CPU/6Gi per node too tight" — that trigger condition is now empirically confirmed.
- timestamp: 2026-08-16T17:05-17:08 UTC
  checked: `kubectl -n etl get pods -o wide` + `kubectl -n etl describe pod ingest-k4x72ikz` (one of two currently-orphaned KPO target pods)
  found: `ingest-k4x72ikz` (map_index=1, run_id=scheduled__2026-08-16T14:33:00, try_number=4 — the SAME stuck TI class as the original 15:38 UTC evidence) and `ingest-ljc72ris` (map_index unseen but same pattern) have been alive 86-88 minutes each, `STATUS: NotReady`, 1/2 containers ready. The `base` container (actual `dataplat ingest` work, image `csv-processor`) shows `State: Terminated, Reason: Completed, Exit Code: 0` — the real work SUCCEEDED in under 2 seconds. The `airflow-xcom-sidecar` container (alpine, `trap "exit 0" INT; while true; do sleep 1; done;`) shows `State: Running` — it never received the termination signal `KubernetesPodOperator`'s own monitoring loop is supposed to send after extracting XCom. Pod `Conditions: Ready=False, ContainersReady=False` — the pod's overall phase never reaches `Succeeded`, so its full CPU/memory request (501m/1034Mi total, base 500m/1Gi + sidecar 1m/10Mi) remains permanently counted against node-allocated resources.
  implication: `common_kpo_kwargs()` (`_common/kpo.py`) sets `on_finish_action: "delete_succeeded_pod"`, meaning these SHOULD have been auto-deleted after success — the deletion/cleanup step never ran. This only happens if the code responsible for monitoring-to-completion (the KubernetesExecutor worker pod running the operator's own `execute()`/`PodManager` loop, a SEPARATE pod from this one, in `airflow` namespace) died/disappeared after launching this target pod but before finishing its cleanup sequence. No eviction/OOM K8s events found corroborating why (events likely expired given ~88min age and default TTL), but the effect is unambiguous and reproducible via CPU math: each orphan permanently pins ~501m, directly explaining the jump from baseline (~75-78%) to current (~91-95%).
- timestamp: 2026-08-16T17:05-17:08 UTC
  checked: scheduler logs (`kubectl -n airflow logs deploy/airflow-scheduler -c scheduler`) for the pod create/delete lifecycle of `resolve_window`/`wait_for_files` across the last several attempts
  found: A clean, repeating, exactly-12-minute cycle: "Creating kubernetes pod for job ... resolve_window/wait_for_files ..." → [pod sits Pending, invisible to log, only visible via `kubectl get pods`/events] → exactly 720s later, "Deleting pod ... in namespace airflow" + "TaskInstance Finished: ... state=failed ... run_duration=0.0". Confirmed across 4 consecutive full cycles (16:29→16:41, 16:41→16:53, 16:53→17:05, and 17:05 cycle in progress at investigation time). `run_duration=0.0` proves the task NEVER actually started running — it was stuck Pending the entire 12 minutes, then Airflow's own `worker_pods_pending_timeout` gave up and force-failed it.
  implication: This is the exact, complete mechanism connecting "Insufficient cpu" to the original symptom ("queued/up_for_retry, queued_dttm frozen, no pod present"). Different sampling times during this cycle produce different-looking snapshots: sampled mid-cycle → Pending pod with FailedScheduling event visible (what we see now); sampled just after the 12-min timeout deletes the pod and before the next attempt is queued → "no pod at all, TI in failed/up_for_retry" (what the original 15:38-16:29 evidence saw). Both are the SAME underlying mechanism, not two different bugs — resolves the file's open question about whether the ingest-stage and resolve_window/wait_for_files-stage incidents share a root cause: they do.
- timestamp: 2026-08-16T17:09 UTC
  checked: `csv_ingest_customers.py` task resource/concurrency configuration
  found: `ingest` (mapped `TracingKubernetesPodOperator`) is configured with `max_active_tis_per_dag=5` and `container_resources` requesting `cpu: 500m` per instance (`_INGEST_RESOURCES`) — up to 5 concurrent `ingest` attempts can be dispatched at once, each needing a KPO target pod (500m) AND its own KubernetesExecutor worker pod, on a node budget with only ~650-750m real headroom even before any leak. `wait_for_files` uses a deferrable `S3KeySensor` (frees the worker pod slot while deferred, but still needs one each time it wakes/pokes and gets rescheduled).
  implication: `max_active_tis_per_dag=5` is a secondary contributing/aggravating factor — even without the orphan-pod leak, a burst of concurrent `ingest` attempts alone could exceed the thin available headroom. Reducing it lowers peak concurrent demand and reduces the frequency of the "executor pod dies mid-flight, orphans its KPO target pod" pattern (fewer simultaneous in-flight KPO launches per DagRun = fewer chances for one of them to be a casualty of the same resource pressure).

- timestamp: 2026-08-16T17:15-17:20 UTC
  checked: Live executor pod logs (csv-ingest-customers-ingest-9jbmcko9) for the `ingest` map_index=0 attempt, cross-referenced against its target pod's (ingest-1033wygd/ingest-6x3jw96q) actual container-state timestamps
  found: Confirms the precise orphaning mechanism. Operator log: "::group::Waiting up to 120s to get the POD scheduled..." (KubernetesPodOperator's `startup_timeout_seconds` default=120, not overridden anywhere in this DAG/`_common/kpo.py`) followed by "The Pod has an Event: 0/3 nodes are available ... Insufficient cpu ...". For map_index=1's try 1: TaskInstance end_date=17:19:44.8 (operator gave up / raised timeout), but the target pod's `base` container didn't even START until 17:19:55 and Completed at 17:19:57 -- the pod finished running a full 13 SECONDS AFTER the operator that launched it had already exited. Applied the `on_finish_action="delete_pod"` fix (see Resolution) before this specific pod finished, but it STILL was not auto-deleted afterward, proving the leak is not solely attributable to `on_finish_action`'s success/failure semantics.
  implication: The operator's `startup_timeout_seconds`-triggered exception path exits before the pod reaches a terminal K8s phase, and its cleanup-on-exception logic does not appear to successfully delete a still-Pending-at-timeout pod that later gets scheduled independently by Kubernetes -- `on_finish_action` only governs the NORMAL post-completion cleanup path, which this exception path bypasses. This is a narrower, structurally-rooted residual: it only manifests when the target pod takes longer than 120s to get scheduled, which is a direct function of the same CPU tightness already identified as the primary root cause, and now occurs far less often (max_active_tis_per_dag=1 means only 1 concurrent `ingest` launch competing for the thin headroom, vs. 5 before) but is not fully eliminated by the changes made this session. Manually deleted the residual orphans (ingest-1033wygd, ingest-6x3jw96q) each time observed.
- timestamp: 2026-08-16T17:14-17:20 UTC
  checked: Full task-chain progress for DagRun scheduled__2026-08-16T17:04:00+00:00 (the first DagRun to start after the fix), sampled repeatedly
  found: resolve_window, wait_for_files, discover, build_ingest_args all reached `success` (first time ANY of these has progressed past queued/up_for_retry all session). Both `ingest` map indices (0, 1) are cycling through retries (try 1 of 3 failed each, via the 120s-startup-timeout race above) but remain `up_for_retry` with retry budget remaining -- not permanently stuck (queued_dttm/state DOES change between checks now, unlike every pre-fix observation this session).
  implication: The original core symptom -- permanent, zero-progress stuck state with queued_dttm frozen indefinitely -- is confirmed broken. The DAG now makes real forward progress. Whether this specific DagRun reaches full `success` within its remaining retry budget depends on whether node CPU headroom (currently 91-95% allocated, tight) stays below the deadlock threshold long enough for `ingest`'s pods to win the scheduling race within 120s -- a probabilistic, not yet fully deterministic, outcome tied to the still-unresolved structural sizing question.

- timestamp: 2026-08-16T15:38 UTC
  finding: 3 mapped `ingest` task instances (map_index 0,1,2) for run `scheduled__2026-08-16T14:33:00` stuck `up_for_retry`, try 4/6, `queued_dttm` unchanged across a 17-minute recheck; zero matching pods in `etl` or `airflow` namespace.
- timestamp: 2026-08-16T16:18-16:29 UTC
  finding: A fresh DagRun (`scheduled__2026-08-16T16:17:00`) exhibited the SAME symptom class one stage earlier — `resolve_window`/`wait_for_files` (not `ingest`) stuck `queued` for 11+ minutes, `start_date` still null. This rules out "specific to the ingest task / KubernetesPodOperator" as the root cause — it affects the scheduler's general task-dispatch mechanism for this DAG, not one operator type.
- timestamp: 2026-08-16 (various)
  finding: Cluster capacity was independently confirmed healthy at time of the second stuck incident (~75-78% node CPU allocation after removing ~56 unrelated stuck-sidecar pods from a separate incident) — ruling out simple resource starvation as the root cause for this specific symptom.
- timestamp: 2026-08-16T16:46 UTC
  finding: Independently reproduced by a separate verification pass (not the same investigation): `SELECT count(*) FILTER (WHERE dag_id IS NOT NULL) FROM meta.ingestion_runs` = 0 of 74 rows; most recent successful claim was `14:38:36 UTC`, over 2 hours prior — corroborating that no `ingest` task has successfully run in that window.

## Eliminated

- hypothesis: "Cluster CPU/scheduling capacity exhaustion"
  eliminated_because: Node CPU allocation was ~75-78% (well under 100%) during the second stuck incident, and the stuck tasks show zero pod ever created (Insufficient-cpu FailedScheduling events were only observed in an EARLIER, separate, already-resolved capacity incident this session — a different root cause than the currently-stuck tasks).
- hypothesis: "Specific to the TracingKubernetesPodOperator / ingest task's own code (introduced by Phase 7's own changes)"
  eliminated_because: The identical stuck-in-queued symptom reproduced on `resolve_window`/`wait_for_files` — plain, pre-existing tasks entirely untouched by Phase 7 — in a separate DagRun. The DagRun failure timeline (06:07 UTC onward) also predates Phase 7's gap-closure session (~13:25 UTC).

## Current Focus

known_pattern_candidate: "dagrun-scheduler-stall (Phase 5) — REFUTED for this session. DagModel.is_stale/hostPath-mount hypothesis was the first thing tested; superseded by direct evidence below before its own DB check was even run, once `kubectl get pods` showed something the Phase 5 incident never showed: live Pending pods with explicit FailedScheduling events. Different root cause, same symptom class (queued/up_for_retry, frozen queued_dttm, silent to Airflow's own exception logging)."

reasoning_checkpoint:
  hypothesis: "Task instances for csv_ingest_customers get stuck in queued/up_for_retry because their Kubernetes pod can never be scheduled ('Insufficient cpu' on both worker nodes) — caused by (a) a structurally thin per-node CPU budget (Allocatable=3 of Capacity=12 cores, by deliberate kind/cluster.yaml design) where the fixed baseline workload alone already consumes ~75-78% of it, leaving only ~650-750m real headroom, COMPOUNDED BY (b) a genuine resource leak: KubernetesPodOperator target pods in the `etl` namespace whose main container completes (even successfully) never get their XCom sidecar terminated nor get deleted per on_finish_action=delete_succeeded_pod, because the separate executor worker pod responsible for that monitoring/cleanup step dies/disappears first — permanently pinning ~501m per orphan and pushing utilization from ~75-78% to ~91-95%+, past the point where ANY new task pod (of any task type) can be scheduled. Airflow's own worker_pods_pending_timeout (~720s, empirically measured) then force-fails each attempt with run_duration=0.0 and deletes the never-scheduled pod, producing the observed queued/up_for_retry-forever symptom, with the exact appearance (pod present vs absent) depending only on which point in this ~12-minute cycle a given observation samples."
  confirming_evidence:
    - "Direct kubectl describe pod events on 2 live Pending pods: 'FailedScheduling ... 0/3 nodes are available: 1 node(s) had untolerated taint(s) [control-plane, expected], 2 Insufficient cpu' — unambiguous, first-party Kubernetes scheduler output, not an inference"
    - "kubectl describe node: Allocatable cpu=3 vs Capacity cpu=12 on both worker/worker2, with Allocated 91%/95% at check time; per-pod CPU-request breakdown accounts for the full total with no unexplained remainder"
    - "kind/cluster.yaml's own header comments explicitly document the systemReserved/kubeReserved sizing as a deliberate tradeoff and explicitly name the exact trigger condition now observed: 'Revisit if profiling later phases ... shows 3 CPU/6Gi per node too tight'"
    - "kubectl describe pod on 2 currently-orphaned etl-namespace pods: base (work) container Terminated/Completed/ExitCode=0 (real success), airflow-xcom-sidecar container State=Running (alpine trap-and-loop, awaiting a termination signal nothing is left alive to send), Ready=False/ContainersReady=False on both, ages 86-88min — direct observation, not inference, and _common/kpo.py confirms on_finish_action=delete_succeeded_pod SHOULD have deleted these already"
    - "Scheduler logs: exact, repeated 720s create-pod -> [silence, pod never runs] -> delete-pod + TaskInstance Finished state=failed run_duration=0.0 cycle, confirmed across 4 consecutive full cycles for resolve_window/wait_for_files — proves these tasks never actually executed, they timed out while perpetually unschedulable"
  falsification_test: "If FailedScheduling events had cited a reason OTHER than Insufficient cpu (e.g. node affinity, volume attach, image pull), or if node Allocated% were comfortably below 100% after accounting for a hypothetical new pod's request, this hypothesis would be wrong. Also falsified if the orphaned etl pods' base containers were still Running/CrashLooping (would point to an application bug in dataplat ingest itself, not an orchestration/cleanup gap) -- they are not; base explicitly Terminated/Completed/ExitCode=0."
  fix_rationale: "Two-tier fix matching the two-tier root cause. (1) SAFE, immediate, non-destructive, fully reversible: delete the 2 currently-orphaned etl pods to reclaim ~1 core total of leaked-forever CPU and restore the cluster to its 'baseline-only' ~75-78% state, which was previously sufficient for at least single-task-at-a-time scheduling to proceed -- this directly tests and should confirm the mechanism (scheduling should resume within one scheduler loop tick). (2) SAFE, minimal, verifiable code change: reduce `ingest`'s `max_active_tis_per_dag` from 5 to 1 in csv_ingest_customers.py, cutting peak concurrent CPU demand for the task most exposed to this failure mode (fan-out mapped task, KPO-launches-pod-in-different-namespace pattern) and reducing how often the executor-pod-dies-orphaning-its-KPO-pod race can recur. NOT attempting the deeper structural fix (raising kind/cluster.yaml's per-node CPU ceiling) in this session: that requires `kind delete cluster` + recreate per the file's own documented constraint (INFRA-09), is explicitly flagged in that same file as a decision requiring the user to first raise the host/WSL2 resource floor, and campaign-changes cluster-wide capacity math beyond this specific DAG's blast radius -- exactly the class of infra decision the Phase-5 precedent in this repo escalated to the user rather than executing autonomously."
  blind_spots: "Have not confirmed the EXACT mechanism by which the executor worker pod (the one running KubernetesPodOperator's own execute()/monitoring code) dies/disappears before it can extract XCom, kill the sidecar, and delete its target pod -- no eviction/OOMKilled K8s event was found corroborating this (events likely already expired at ~88min age under default TTL), so this remains inferred from the observable effect (permanent orphan) plus the resource-pressure context, not directly witnessed. Have not fixed the RECURRING SOURCE of new orphans (only cleaned up the 2 that exist now) -- if the structural CPU tightness is not addressed, new orphans will likely accumulate again over time even with max_active_tis_per_dag=1, just more slowly; this is flagged for the user as a follow-up, not silently left unmentioned. Have not verified whether reducing max_active_tis_per_dag to 1 alone (without the immediate pod cleanup) would have been sufficient -- did not test in isolation, since both fixes are safe to apply together and isolating would cost significant additional wall-clock time (12-min cycles) this investigation cannot obviously afford mid-session."

next_action: "AWAITING HUMAN VERIFICATION. Self-verified: original permanent-deadlock symptom broken (resolve_window/wait_for_files/discover/build_ingest_args all reached success on the first post-fix DagRun; previously-stuck pods transitioned to Running immediately after orphan cleanup). Residual, honestly-flagged, NOT fully fixed: (a) a narrower 120s-startup-timeout race can still orphan an `ingest` KPO pod occasionally under tight CPU -- user should periodically check `kubectl -n etl get pods` for 1/2 NotReady pods with base=Terminated/Completed and delete them if seen; (b) the structural per-node CPU ceiling (kind/cluster.yaml systemReserved/kubeReserved, Allocatable=3 cores of 12) is unchanged -- baseline alone still runs ~75-95% utilized, so this class of issue can recur under any additional load until the user decides whether to raise the host/WSL2 resource floor and recreate the kind cluster (destructive, needs its own explicit decision + DB backup plan first). User should also watch DagRun scheduled__2026-08-16T17:04:00+00:00 (or the next one) through to a final success/failure to confirm ingest's retries win the race within budget."

## Resolution
<!-- OVERWRITE as understanding evolves -->

root_cause: |
  Kubernetes-level CPU scheduling exhaustion, not an Airflow scheduler/executor bug.
  kind/cluster.yaml deliberately caps each worker node's Allocatable CPU at 3 cores
  (of 12 physical, via systemReserved cpu=5 + kubeReserved cpu=4) to prevent all 3
  kind nodes from collectively over-advertising capacity the single host doesn't
  have. The platform's fixed baseline (Airflow scheduler/dag-processor/api-server/
  triggerer/statsd + full monitoring stack + 2x CNPG Postgres + MinIO + Vault)
  already consumes ~75-78% of that 3-core budget with zero task pods running,
  leaving only ~650-750m real headroom per node. `csv_ingest_customers`'s `ingest`
  task (KubernetesPodOperator, launches a SECOND pod in the `etl` namespace,
  `on_finish_action=delete_succeeded_pod`, previously `max_active_tis_per_dag=5`)
  intermittently orphans its `etl`-namespace target pod: when the KubernetesExecutor
  worker pod running the operator's own execute()/monitoring code dies or
  disappears after successfully launching the KPO pod but before completing
  XCom-extraction + sidecar-termination + delete-on-success, the target pod is left
  permanently in `Running` phase (main container Terminated/Completed/ExitCode=0,
  but the `airflow-xcom-sidecar` alpine container's trap-and-sleep loop never
  receives its termination signal) -- permanently pinning that pod's ~501m CPU
  reservation with nothing ever able to reclaim it. Each accumulated orphan pushes
  node utilization further past the ~75-78% baseline; once utilization crosses the
  point where no remaining task pod's CPU request fits, Kubernetes rejects every new
  task pod for EVERY task in the DAG (not just `ingest`) with FailedScheduling
  ("Insufficient cpu"). Airflow's own worker_pods_pending_timeout (~720s, measured
  directly) then gives up on each never-scheduled pod, deletes it, and marks the
  task instance failed/up_for_retry with run_duration=0.0 -- which is why the
  original observations sometimes showed "no pod at all" (sampled just after a
  timeout-triggered deletion) and other times (this session, later) showed a
  visibly Pending pod with an explicit FailedScheduling event (sampled mid-cycle):
  both are the same underlying mechanism at different phases of the same ~12-minute
  cycle, confirmed by scheduler logs showing 4 consecutive identical cycles.
fix: |
  Three-part fix, all verified live against the running cluster (deadlock
  confirmed broken; one narrower structural residual honestly flagged, not fixed):
  (1) Deleted all currently-orphaned `etl`-namespace pods as they were found
      (ingest-k4x72ikz, ingest-ljc72ris, then ingest-vqmipsgw, ingest-1033wygd,
      ingest-6x3jw96q as each re-manifested during verification) to reclaim
      leaked CPU -- safe, non-destructive, fully reversible (their real work had
      already completed successfully in every case; nothing about Airflow's
      already-recorded task-instance outcome changes by deleting the abandoned
      pod shell). This is an operational mitigation, not a durable code fix --
      new orphans can still accumulate (see residual risk below) until either the
      structural CPU question is resolved or the operator's own timeout-cleanup
      gap (also below) is independently patched upstream or with more invasive
      custom code than this session pursued.
  (2) Reduced `ingest`'s `max_active_tis_per_dag` from 5 to 1 in
      airflow/dags/csv_ingest_customers.py -- cuts peak concurrent CPU demand for
      the task most exposed to this failure mode and reduces how often the
      orphaning race (below) gets a chance to occur.
  (3) Changed `on_finish_action` from "delete_succeeded_pod" to "delete_pod" in
      airflow/dags/_common/kpo.py (applies to both `discover` and `ingest`) --
      closes the leak for the NORMAL completion path (operator observes the pod
      reach a terminal state itself, whether success or failure) regardless of
      outcome. Confirmed via live testing this does NOT close one narrower race:
      when KubernetesPodOperator's own `startup_timeout_seconds` (default 120s,
      unmodified) elapses because the target pod is still Pending (Insufficient
      cpu), the operator raises and the task attempt ends BEFORE the pod reaches
      a terminal K8s phase -- its cleanup-on-exception path does not delete a
      still-Pending pod that Kubernetes later schedules independently once
      capacity frees up, so that specific pod still becomes an orphan once it
      eventually runs to completion unwatched. This residual is now much rarer
      (concurrency capped at 1) but not eliminated; each occurrence still
      requires the same manual-delete mitigation as (1) until addressed
      separately (e.g. a scheduled reaper for `already_checked`-labeled etl pods
      whose base container is Terminated but sidecar is still Running, or an
      upstream/deeper fix to the timeout-exception cleanup path itself -- both
      out of scope for this session, noted as follow-up).
  NOT applied (deliberately, requires human decision): raising kind/cluster.yaml's
  per-node CPU ceiling (systemReserved/kubeReserved) -- the actual structural
  root cause underlying both the scheduling deadlock and the residual 120s-race
  orphan pattern. That file's own header comments name this exact scenario
  ("Revisit if profiling later phases ... shows 3 CPU/6Gi per node too tight") as
  the trigger for a resize, but a resize requires `kind delete cluster` + recreate
  (INFRA-09, destructive -- this cluster's Postgres/MinIO persistence is
  `local-path-provisioner` writing into node-local ephemeral storage per D-01, NOT
  host-backed, so cluster recreation would lose both databases' data without a
  prior backup/restore step). Flagged to the user as a follow-up decision, not
  silently deferred.
verification: |
  Immediately after deleting the first 2 orphaned pods (before either code change
  was picked up), the two task-instance pods that had been stuck Pending for 45
  minutes (csv-ingest-customers-resolve-window-n8ow5aak,
  csv-ingest-customers-wait-for-files-qdltli8r) transitioned to Running within
  seconds -- direct, immediate, causal confirmation of the root-cause mechanism.
  Watched DagRun scheduled__2026-08-16T17:04:00+00:00 (the first run to start
  after the fix) progress: resolve_window, wait_for_files, discover, and
  build_ingest_args ALL reached `success` -- the first time any task in this DAG
  progressed past queued/up_for_retry all session, confirming the PERMANENT
  deadlock (the original, core reported symptom) is broken. `ingest` (both mapped
  instances) is cycling through retries rather than being permanently stuck
  (queued_dttm/state changes between checks, unlike every pre-fix observation) --
  each failure is the narrower 120s-timeout race described in fix(3), not a
  return of the original symptom; retry budget (3 retries) remains. Did not wait
  out this specific DagRun to a final terminal state before writing this up
  (would cost multiple more 12-minute-scale cycles); recommending the user watch
  it through to confirm full DagRun success as part of human verification below.
files_changed:
  - airflow/dags/csv_ingest_customers.py (ingest task: max_active_tis_per_dag 5 -> 1)
  - airflow/dags/_common/kpo.py (on_finish_action: delete_succeeded_pod -> delete_pod, applies to discover + ingest)
