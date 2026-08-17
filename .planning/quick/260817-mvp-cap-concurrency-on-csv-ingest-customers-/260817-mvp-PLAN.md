---
phase: quick-260817-cap-integrity-gate-concurrency
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - airflow/dags/csv_ingest_customers.py
  - airflow/dags/csv_ingest_orders.py
  - tests/unit/test_dag_structure.py
autonomous: true
requirements:
  - "STATE.md blocker (2026-08-17): integrity_gate dynamic-mapping fan-out starves other DAGs' pod scheduling on kind's 3-allocatable-CPU-core worker nodes (root cause confirmed live this session; superseded the earlier 'discover task reliability' misdiagnosis)"

must_haves:
  truths:
    - "csv_ingest_customers's integrity_gate mapped task never has more than 3 pods concurrently Pending/ContainerCreating/Running for a single DagRun"
    - "csv_ingest_orders's integrity_gate mapped task has the identical cap applied (same code shape, same rationale)"
    - "Other DAGs'/tasks' pods (e.g. csv_ingest_orders's wait_for_files, platform components) are no longer starved of node CPU by an integrity_gate fan-out during a live backlog"
    - "The cap is proven both structurally (offline DagBag test) and live (real kind cluster pod-count observation)"
  artifacts:
    - path: "airflow/dags/csv_ingest_customers.py"
      provides: "integrity_gate.partial(..., max_active_tis_per_dag=3, ...) — bounded dynamic-mapping concurrency"
      contains: "max_active_tis_per_dag=3"
    - path: "airflow/dags/csv_ingest_orders.py"
      provides: "identical bounded dynamic-mapping concurrency for orders' own integrity_gate"
      contains: "max_active_tis_per_dag=3"
    - path: "tests/unit/test_dag_structure.py"
      provides: "offline structural proof the cap is set on integrity_gate in both DAGs"
      contains: "max_active_tis_per_dag"
  key_links:
    - from: "airflow/dags/csv_ingest_customers.py: gate = integrity_gate.partial(...)"
      to: "BaseOperator.__init__'s max_active_tis_per_dag parameter (airflow/sdk/bases/operator.py)"
      via: "TaskFlow .partial() forwarding (airflow/sdk/bases/decorator.py's _expand(), partial_keys derived from BaseOperator's own signature)"
      pattern: "max_active_tis_per_dag=3"
---

<objective>
Cap concurrency of the `integrity_gate` dynamically-mapped TaskFlow task in
both `csv_ingest_customers` and `csv_ingest_orders` at 3 concurrent pods per
DagRun, so a backlog of matched files can no longer fan out to 8-19+
simultaneous ~250m-CPU-request pods and exhaust kind worker nodes' real
~700-800m headroom — starving OTHER DAGs' (and other tasks') pod scheduling
cluster-wide, which was previously misdiagnosed as a "discover intermittently
registers zero rows" application bug.

Purpose: `integrity_gate` is a plain `@task` (no `container_resources`
override), so every mapped instance inherits the Helm chart's
`workers.kubernetes.resources.requests.cpu: 250m` default
(`helm/values/local/airflow.yaml`). With kind worker nodes' ~3-CPU
allocatable budget (`kind/cluster.yaml`) and the fixed Airflow platform
baseline (api-server/scheduler/dag-processor/triggerer/statsd) already
consuming ~2200-2400m, real headroom is only ~700-800m/node. An unbounded
`.expand(key=matched_keys)` over a backlog (confirmed LIVE this session: 19
`csv-ingest-customers-integrity-gate-*` pods, 5 Running/11 Pending/3 Error,
both worker nodes at 97-98% CPU allocation) starves everything else. This
mirrors the EXACT root cause and fix shape already applied to this file's own
`ingest` task (`max_active_tis_per_dag=1`, debug session
`airflow-scheduler-stuck-tasks`, commit `6ea4129`) — same mechanism, applied
to the second mapped task that was missed.

Output: Both DAG files updated with `max_active_tis_per_dag=3` on their
`integrity_gate.partial(...)` calls; a new offline structural test proving
it; a live-cluster observation proving the cap actually holds under the
real, currently-present backlog.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/STATE.md
@airflow/dags/csv_ingest_customers.py
@airflow/dags/csv_ingest_orders.py
@airflow/dags/_common/integrity_gate.py
@tests/unit/test_dag_structure.py
@tests/unit/conftest.py
@helm/values/local/airflow.yaml
@kind/cluster.yaml

<mechanism_confirmed_live_against_installed_venv>
Installed versions (`.venv`, matching CLAUDE.md's pin): `apache-airflow
3.3.0`, `apache-airflow-task-sdk 1.3.0`.

`max_active_tis_per_dag` is a genuine `BaseOperator.__init__` parameter
(`airflow/sdk/bases/operator.py`, `__init__` signature line ~1069,
`self.max_active_tis_per_dag: int | None = max_active_tis_per_dag` at line
~1199). It is exposed through TaskFlow's own `.partial()` (used by `@task`
functions, not only classic operators): `airflow/sdk/bases/decorator.py`'s
`_expand()` builds `partial_keys` as
`set(inspect.signature(BaseOperator).parameters) - ignore`, and
`max_active_tis_per_dag` is NOT in that `ignore` set (only `task_concurrency`,
its deprecated predecessor, is excluded). This is the IDENTICAL mechanism
`csv_ingest_customers.py`'s own `ingest` task already uses today
(`TracingKubernetesPodOperator.partial(..., max_active_tis_per_dag=1, ...)`,
line ~141) — proof by existing precedent in this exact codebase, not a new
pattern.

`integrity_gate` is a plain `@task` (TaskFlow-decorated function, NOT a
`KubernetesPodOperator`), so the fix is `integrity_gate.partial(bucket=...,
dataset_name=..., max_active_tis_per_dag=3).expand(key=matched_keys)` — same
kwarg, same `.partial()` call site, just added alongside the two existing
`bucket`/`dataset_name` partial kwargs.
</mechanism_confirmed_live_against_installed_venv>

<live_backlog_confirmed_this_session>
`kubectl --context kind-airflow-platform get pods -n airflow --no-headers |
grep integrity-gate` currently shows 19 `csv-ingest-customers-integrity-gate-*`
pods (5 Running, 11 Pending, 3 stale Error). Both worker nodes report
`cpu 2910m-2950m (97-98%)` requested (`kubectl describe nodes`). Because
`raw/` is append-only (§63) and `list_matched_keys` lists ALL currently-
matching `customers/*.csv` keys on EVERY DagRun (not just new arrivals), this
backlog reproduces on essentially every scheduled run given this project's 5
days of accumulated historical fixture uploads — no artificial backlog needs
to be manufactured for Task 2's live verification; it is already live.
</live_backlog_confirmed_this_session>
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Add the concurrency cap to both integrity_gate mappings + offline structural proof</name>
  <files>airflow/dags/csv_ingest_customers.py, airflow/dags/csv_ingest_orders.py, tests/unit/test_dag_structure.py</files>
  <behavior>
    - Test: `test_integrity_gate_concurrency_capped(dagbag)` — for both
      `csv_ingest_customers` and `csv_ingest_orders`, look up
      `dag.task_dict["integrity_gate"]` and assert
      `task.max_active_tis_per_dag == 3` (the mapped task exposes this as a
      `partial_kwargs`-backed property, same access pattern
      `test_max_active_runs_is_one` already uses for `dag.max_active_runs`
      one test up). Follows this file's existing loop-over-both-dag_ids
      style (`_BOTH_DAG_IDS` minus the unrelated `smoke_kubernetes_pod`
      entry, which has no `integrity_gate` task).
    - Run it first — MUST fail (`AttributeError`/`None != 3`) against the
      unmodified DAG files, proving the test exercises real code.
  </behavior>
  <action>
    Add the RED test to `tests/unit/test_dag_structure.py` per the
    `<behavior>` block, run it, confirm it fails. Then make it pass: in
    `airflow/dags/csv_ingest_customers.py`, change the `gate =
    integrity_gate.partial(bucket="raw", dataset_name="customers").expand(
    key=matched_keys)` line (currently line 119) to also pass
    `max_active_tis_per_dag=3` inside `.partial(...)`, per the
    `<mechanism_confirmed_live_against_installed_venv>` context block above.
    Add a short comment directly above the `gate = ...` line, matching this
    file's own per-decision documentation convention (see the `ingest`
    task's own comment a few lines below for the precedent style): explain
    that `integrity_gate` inherits the Helm chart's default worker-pod CPU
    request (`workers.kubernetes.resources.requests.cpu: 250m`,
    `helm/values/local/airflow.yaml`, since it sets no
    `container_resources` of its own), that kind worker nodes have only
    ~700-800m real headroom after the fixed platform baseline
    (`kind/cluster.yaml`), and that capping at 3 (750m) keeps the mapped
    fan-out under that headroom instead of starving other DAGs'/tasks' pod
    scheduling — the same root cause and mechanism already fixed for
    `ingest` via `max_active_tis_per_dag=1` (debug session
    `airflow-scheduler-stuck-tasks`, commit `6ea4129`). Apply the identical
    one-line change plus a comment (substituting "orders" for "customers",
    matching `csv_ingest_orders.py`'s own existing convention of citing
    `csv_ingest_customers.py` for shared design rationale) to the mirrored
    `gate = integrity_gate.partial(bucket="raw",
    dataset_name="orders").expand(key=matched_keys)` line in
    `csv_ingest_orders.py` (currently line 117). Do not touch
    `discover`/`ingest`/`wait_for_files`/`list_matched_keys` or any resource
    request/limit values — this task changes exactly one kwarg per DAG file.
  </action>
  <verify>
    <automated>cd /home/konutec/projects/airflow-platform && AIRFLOW_VAR_CSV_PROCESSOR_IMAGE=test .venv/bin/pytest tests/unit/test_dag_structure.py -q</automated>
  </verify>
  <done>Both DAG files set `max_active_tis_per_dag=3` on their `integrity_gate.partial(...)` call; the new `test_integrity_gate_concurrency_capped` test (and the full existing `test_dag_structure.py` suite, unaffected) passes offline, no live cluster needed.</done>
</task>

<task type="auto">
  <name>Task 2: Deploy and prove the cap holds against the real, currently-live backlog</name>
  <files>airflow/dags/csv_ingest_customers.py, airflow/dags/csv_ingest_orders.py</files>
  <action>
    Deploy: these two files are already on the kind cluster's hostPath-
    mounted `airflow/dags/` volume (`kind/cluster.yaml`) — Task 1's on-disk
    edit is already visible inside every Airflow pod; no image build, no
    `kubectl cp`, no Helm upgrade. Force an IMMEDIATE re-parse instead of
    waiting for the default 300s `dag_processor.refresh_interval`: run
    `kubectl --context kind-airflow-platform rollout restart deployment/airflow-dag-processor -n airflow`
    then `kubectl --context kind-airflow-platform rollout status deployment/airflow-dag-processor -n airflow --timeout=120s`.
    Record the restart's completion timestamp — every pod verification step
    below counts ONLY pods created at or after this timestamp, so any
    pre-fix backlog pods already in flight are correctly excluded from the
    cap check (`max_active_tis_per_dag` governs newly-scheduled instances
    going forward; it does not retroactively delete already-created pods).

    Verify live, for `csv_ingest_customers` (a real backlog is already
    present this session — no synthetic backlog needs to be manufactured,
    per the `<live_backlog_confirmed_this_session>` context block): poll
    `kubectl --context kind-airflow-platform get pods -n airflow -o json`
    every ~10-15s for up to 5 minutes, filtering to pods whose name matches
    `csv-ingest-customers-integrity-gate-*`, `creationTimestamp` is at/after
    the restart timestamp, and phase is `Pending` or `Running` (exclude
    `Succeeded`/`Failed`/`Error` — terminal states hold no node CPU
    reservation). Track the maximum concurrent count observed across all
    samples. Also capture `kubectl --context kind-airflow-platform describe
    nodes | grep -A5 "Allocated resources"` once mid-window as corroborating
    evidence of restored headroom, and check
    `kubectl --context kind-airflow-platform get events -n airflow --sort-by=.lastTimestamp`
    for any NEW (post-restart-timestamp) `FailedScheduling`/`Insufficient
    cpu` event — there should be none once the cap is live.

    Best-effort, same method, for `csv_ingest_orders`: check whether
    `raw/orders/*.csv` currently has a matched-key backlog large enough to
    trigger a >3 fan-out (list via the same `S3Hook`/boto3 pattern
    `scripts/ingest-demo.py` uses — MinIO endpoint `http://minio.localtest.me`,
    credentials from `scripts/minio-credentials.sh show`) AND an orders
    DagRun is actually active (recall: `orders` is asset-scheduled off
    `csv_ingest_customers`'s own `ingest` publish, `max_active_runs=1`, so a
    fresh orders run only starts once customers next publishes). If a live
    orders fan-out is observed during the window, apply the identical
    poll-and-assert. If not (no live orders backlog materializes within the
    verification window), do not block on it — Task 1's offline
    `test_integrity_gate_concurrency_capped` already structurally proves
    `orders.py` carries the identical `max_active_tis_per_dag=3` setting via
    the exact same code path just proven live for customers; note this
    explicitly as the accepted verification scope for orders.
  </action>
  <verify>
    <automated>kubectl --context kind-airflow-platform get pods -n airflow --no-headers | grep integrity-gate | grep -v -E 'Succeeded|Error|Completed' | wc -l</automated>
  </verify>
  <done>Post-restart, the maximum concurrent count of `csv-ingest-customers-integrity-gate-*` pods in Pending/Running state, sampled across a >=5-minute window against the live, already-present backlog, never exceeds 3. No new `Insufficient cpu`/`FailedScheduling` events appear for any pod after the dag-processor restart timestamp. `csv_ingest_orders.py` carries the identical, structurally-proven cap (live-proven if a live orders fan-out occurred during the window; otherwise accepted via Task 1's offline proof plus the identical code path).</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|--------------|
| DagRun fan-out -> Kubernetes scheduler | `integrity_gate.expand(key=matched_keys)` creates one worker pod request per matched S3 key, competing for the SAME shared node CPU pool as the Airflow platform's own control-plane pods and every other DAG's task pods. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|------------------|
| T-quick-01 | Denial of Service | `integrity_gate.partial(...).expand(key=matched_keys)` (both DAGs) | mitigate | `max_active_tis_per_dag=3` bounds concurrent mapped-task pod requests to 750m, under the ~700-800m real per-node headroom (`kind/cluster.yaml` + `helm/values/local/airflow.yaml`'s 250m default worker request), so a large matched-key backlog can no longer starve other DAGs'/tasks' pod scheduling — the exact mechanism already applied to `ingest` (`max_active_tis_per_dag=1`) for the same root cause. |
| T-quick-02 | Denial of Service (accepted tradeoff) | Same mapped task, throughput dimension | accept | Capping concurrency at 3 slows large-backlog gate throughput versus the previous unbounded fan-out. Accepted: correctness (not starving the whole cluster) outranks gate throughput for this platform's core value (traceable, replayable ingestion); the structural node-CPU-budget question (`kind/cluster.yaml` reservations) remains the separately-deferred, out-of-scope fix for raising real headroom. |
</threat_model>

<verification>
1. `pytest tests/unit/test_dag_structure.py -q` passes offline (Task 1), including the new `test_integrity_gate_concurrency_capped` test for both `csv_ingest_customers` and `csv_ingest_orders`.
2. Live, against the real kind cluster (Task 2): the maximum concurrent count of post-restart `csv-ingest-customers-integrity-gate-*` pods in Pending/Running state never exceeds 3 across a sampling window run against the currently-live backlog.
3. `grep -c "max_active_tis_per_dag=3" airflow/dags/csv_ingest_customers.py airflow/dags/csv_ingest_orders.py` reports exactly 1 match in each file.
</verification>

<success_criteria>
- Both `airflow/dags/csv_ingest_customers.py` and `airflow/dags/csv_ingest_orders.py` cap `integrity_gate`'s dynamic mapping at `max_active_tis_per_dag=3`.
- `tests/unit/test_dag_structure.py` structurally proves the cap offline, no live cluster required.
- A live kind-cluster observation proves the cap holds under the real, currently-present `csv_ingest_customers` backlog: never more than 3 concurrent `integrity_gate` pods Pending/Running at once post-fix.
- No new CPU-starvation (`FailedScheduling`/`Insufficient cpu`) events occur for any other pod during the live verification window.
- The out-of-scope structural fix (`kind/cluster.yaml` node CPU budget) is untouched, per the task's explicit instruction.
</success_criteria>

<output>
Create `.planning/quick/260817-mvp-cap-concurrency-on-csv-ingest-customers-/260817-mvp-SUMMARY.md` when done.
</output>
