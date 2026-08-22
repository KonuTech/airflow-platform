# Kubernetes pod stuck — node CPU exhaustion and orphaned sidecar leak

Sourced from [`.planning/debug/resolved/airflow-scheduler-stuck-tasks.md`](../../.planning/debug/resolved/airflow-scheduler-stuck-tasks.md)
(2026-08-16). Every claim below is that incident's own `resolution.root_cause`/`fix`/`verification`,
restated for an operator audience.

## Symptoms

Task instances sit in `queued`/`up_for_retry` and never get redispatched, with retry budget still
remaining. What you actually observe depends on exactly when you look, because both are the same
~12-minute cycle sampled at a different point:

- **No pod at all** — `kubectl -n airflow get pods` / `kubectl -n etl get pods` show nothing for the
  stuck task; Airflow's own `worker_pods_pending_timeout` (~720s) already gave up and deleted a pod
  that never got scheduled, then marked the task instance `failed`/`up_for_retry` with
  `run_duration=0.0`.
- **A live `Pending` pod** — `kubectl describe pod <name>` shows `Events: FailedScheduling ...
  Insufficient cpu`.
- **An orphaned pod in the `etl` namespace** — `kubectl -n etl get pods` shows a pod `1/2` ready:
  the `base` container (the real work) is `Terminated / Completed / Exit Code: 0` (it succeeded),
  but the `airflow-xcom-sidecar` container is still `Running`, so the pod's full CPU/memory request
  stays permanently counted against the node.

## Diagnosis

This platform deliberately caps each kind worker node's *allocatable* CPU well below its physical
capacity (`kind/cluster.yaml`'s `systemReserved`/`kubeReserved`), and the platform's own fixed
baseline (scheduler, dag-processor, api-server, triggerer, the monitoring stack, both PostgreSQL
instances, MinIO, Vault) already consumes the large majority of that budget with **zero task pods
running**. Real headroom per node is thin by design.

```bash
kubectl describe node <node-name>   # Capacity vs Allocatable vs Allocated
kubectl -n etl get pods -o wide     # look for 1/2-ready pods
kubectl -n etl describe pod <name>  # confirm base=Terminated/Completed, sidecar=Running
```

The orphaned-sidecar pattern is the aggravating factor: when the `KubernetesExecutor` worker pod
that launched a `KubernetesPodOperator` target pod dies or disappears before it finishes extracting
XCom and terminating the sidecar, the target pod's `airflow-xcom-sidecar` container never receives
its termination signal and the pod never reaches a terminal K8s phase — so its CPU reservation leaks
forever, one orphan at a time, until nothing new can schedule anywhere in the DAG (not just the task
that orphaned).

## Recovery

**Immediate, safe, reversible:** delete each orphaned pod to reclaim its leaked CPU. The real work
already completed successfully (`base` container exit 0) — deleting the shell pod changes nothing
about an already-recorded outcome.

```bash
kubectl -n etl delete pod <orphaned-pod-name> --force --grace-period=0
```

**Structural, already applied in this codebase — verify it hasn't regressed:**
`airflow/dags/_common/kpo.py`'s `common_kpo_kwargs()` sets `on_finish_action: "delete_pod"` (not
`"delete_succeeded_pod"`) so the pod is force-deleted regardless of the monitoring loop's own
signal timing, and the affected DAGs cap concurrent mapped-task fan-out
(`max_active_tis_per_dag`) to reduce how often the orphaning race gets a chance to occur at all. If
this incident recurs frequently, check whether either setting has drifted.

**Not a same-session fix:** raising the node CPU ceiling itself requires `kind delete cluster` +
recreate (destructive — this cluster's PostgreSQL/MinIO persistence is node-local, not host-backed,
so it needs a backup/restore plan first). Treat this as a deliberate, separately-scheduled decision,
not something to reach for mid-incident.

## Reprocessing

None needed for the reclaimed orphan itself — its real work already succeeded. For a task that
timed out waiting to be scheduled (the "no pod at all" symptom), the scheduler dispatches a fresh
attempt automatically once capacity frees and the task's own retry budget allows it; this platform's
load path is idempotent under retry (LOAD-01/LOAD-02), so no duplicate rows result even if an
earlier attempt partially staged data before being orphaned.

## Verification

1. `kubectl -n etl get pods` shows no `1/2`-ready pods with a `Terminated`/`Completed` base
   container.
2. `kubectl describe node` shows CPU allocation back under the level that was producing
   `FailedScheduling` events.
3. The previously-stuck task instance transitions to `Running` within seconds of the cleanup, not
   minutes.
4. The affected `DagRun` reaches `success` with all mapped task instances (if any) accounted for.
