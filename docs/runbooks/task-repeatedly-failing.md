# Task repeatedly failing — retry budget exhausting without progress

**Sourcing choice, stated explicitly per this runbook set's own convention:** this scenario pivots
to ORCH-04's documented retry/failure semantics (`airflow/dags/csv_ingest_customers.py`,
`airflow/dags/_common/kpo.py`) as its primary framing, rather than a third reuse of
[`.planning/debug/resolved/airflow-scheduler-stuck-tasks.md`](../../.planning/debug/resolved/airflow-scheduler-stuck-tasks.md)
(already the source for `kubernetes-pod-stuck.md`). That incident is still the right first
cross-reference when the underlying failure reason turns out to be resource starvation — see
Diagnosis below.

## Symptoms

A specific task's `try_number` climbs on every check (`1/2`, `2/2`, ...) without ever reaching
`success`. Once `try_number` reaches the task's own `retries` limit, it lands in `FAILED` and
cascades its `DagRun` to `failed`. Unlike `kubernetes-pod-stuck.md`, a pod usually *does* launch and
run each attempt — the failure is inside the task's own execution, not a scheduling failure to get
a pod at all.

## Diagnosis

```bash
airflow tasks states-for-dag-run <dag_id> <logical_date>
```

Read `try_number`/`max_tries` per task, then read the real per-attempt failure reason from the task
log (`kubectl logs`, if the pod hasn't been deleted yet) — do not guess from the retry count alone.

Every task in this platform declares an explicit `retries` + `retry_exponential_backoff=True`
(ORCH-04) — most tasks use `retries=2`, but three specific tasks deliberately use `retries=6`,
because `apache-airflow-providers-cncf-kubernetes`'s `KubernetesJobWatcher` has a known
request-timeout race that needs more attempts to statistically clear (see the comment directly
above each `retries=6` in `airflow/dags/csv_ingest_customers.py`). A task exhausting a **lower**
retry budget than its neighbors may simply be under-tuned for a flaky dependency, not evidence of a
new bug.

Cross-reference the two most common root causes already documented on this platform before
assuming something new:

- Vault sealed after a restart → every attempt fails with "connection not found" —
  see [`vault-unavailable.md`](vault-unavailable.md).
- Node CPU exhaustion / an orphaned KPO sidecar → every attempt times out waiting for a pod —
  see [`kubernetes-pod-stuck.md`](kubernetes-pod-stuck.md).

## Recovery

Fix the underlying cause first — clearing a task without addressing why it failed just re-runs the
same failure. Once the root cause is resolved, reset the task's retry budget:

```bash
airflow tasks clear <dag_id> -s <logical_date> -e <logical_date> -t <task_id> -d -y
```

`-d` includes downstream tasks, matching how this platform's own diagnostic sessions have used this
command; omitting `-d` leaves downstream tasks frozen at whatever pre-clear state they were in
(observed live as a real, separate incident — always pass `-d` when clearing a task inside an
active DAG run).

## Reprocessing

Because every stage of this pipeline is idempotent under retry (LOAD-01/LOAD-02: an Airflow retry
mid-load creates no duplicate rows, proven live at 10M-row scale), clearing and retrying a
repeatedly-failing task is always safe — even if an earlier attempt partially wrote to staging
before failing. Staging is `UNLOGGED`/`ON COMMIT DROP` and transactional, so a failed attempt never
leaves partial data visible to a retry.

## Verification

1. The cleared task reaches `success`, and its `try_number` did not need to exhaust `max_tries`
   again for the same reason.
2. The `DagRun` reaches `success`.
3. Re-running the same logical date afterward (an idempotency check, QUAL-09) produces zero
   additional rows in the target table — confirming no partial or duplicate work survived the
   retry cycle.
