# Airflow unavailable — scheduler stops dispatching, cluster-wide

Sourced from [`.planning/debug/resolved/dagrun-scheduler-stall.md`](../../.planning/debug/resolved/dagrun-scheduler-stall.md)
(2026-08-14). Every claim below is that incident's own `root_cause`/`fix`/`verification`, restated
for an operator audience — see the source file for the full evidence chain (`docker exec`/`mount`
output, `DagModel.is_stale` queries, scheduler source reads).

## Symptoms

No DAG in the cluster gets a new `DagRun`, and every already-`running` `DagRun`'s tasks stop
progressing — not scoped to one DAG. `dag_run.last_scheduling_decision` is frozen at the same
timestamp across the entire metastore, for every `dag_id`. The `airflow-dag-processor` pod's logs
show an empty "DAG File Processing Stats" table and a repeating "Not time to refresh bundle
dags-folder" loop, as if zero DAG files exist. No exception, traceback, or error-level log line
appears anywhere — this is a silent freeze, not a crash.

## Diagnosis

This platform mounts `airflow/dags/` into each kind node via a `hostPath` bind mount
(`kind/cluster.yaml`'s `extraMounts`), not a git-sync sidecar. A Docker Desktop/WSL2-level host
event (restart, suspend/resume) can restart every container on every kind node simultaneously and
leave that bind mount **not** reattached — Docker silently falls back to an empty, read-only
`tmpfs` at the mount point instead of re-establishing the real host directory.

Confirm directly, per node:

```bash
docker exec <kind-node-name> mount | grep /mnt/dags
```

A healthy mount reports a real block device and filesystem, e.g. `/dev/sde on /mnt/dags type ext4
(ro,relatime,...)`. A broken one reports `none on /mnt/dags type tmpfs (ro,relatime)` — an empty,
read-only stand-in. With `/opt/airflow/dags/` empty inside the dag-processor pod, it discovers zero
files, never re-parses any DAG, and never clears `DagModel.is_stale`. The scheduler's own
`get_running_dag_runs_to_examine()` query filters `WHERE DagModel.is_stale == false()` — a silent,
exception-free SQL exclusion that removes every `DagRun`, for every `dag_id`, from ever being
scheduled again.

Cross-check `DagModel.is_stale` for more than one DAG (including one unrelated to whatever you were
actually working on) — if it is `True` cluster-wide, this is the scheduler-stall pattern, not a
problem specific to one pipeline.

## Recovery

Docker cannot live-remount a running container's broken bind mount, from inside or outside the
container — a container restart is required on every node showing the broken `tmpfs`:

```bash
docker restart <kind-node-name>
```

Restarting the node hosting `airflow-dag-processor` (and `airflow-api-server`) is sufficient to
unblock scheduling cluster-wide: the scheduler consumes `DagModel.is_stale` from the database, not
a local filesystem read, so it resumes once the DAG processor re-parses successfully — regardless
of which node the scheduler itself runs on. Restarting the remaining nodes afterward closes a
narrower residual risk: a `@task`-decorated TaskFlow function must re-import its own source file
when its task pod lands on a still-broken node, whereas a predefined operator/sensor class
(reconstructed from serialized DAG metadata via the API server) does not need the local file and
can complete correctly even on a still-broken node. Restart every affected node before declaring
this resolved, not just the first one that unblocks scheduling.

## Reprocessing

This is a scheduling freeze, not a data-correctness problem — no file was misread and no row was
mis-loaded while the mount was broken, because no task pod could make forward progress at all.
Once the mount reattaches and `is_stale` clears, the scheduler autonomously resumes and drains any
backlog that accumulated during the freeze; no manual re-trigger, backfill, or `airflow dags
trigger` is needed. Confirmed live: the scheduler advanced through dozens of queued `DagRun`s with
zero manual intervention once unblocked.

## Verification

1. `docker exec <node> mount | grep /mnt/dags` reports the real filesystem (e.g. `ext4`), not
   `tmpfs`, on every node.
2. `DagModel.is_stale` is `False` for the DAG(s) you check, with a fresh `last_parsed_time`.
3. The previously-stuck `DagRun` reaches `state=success` with every task individually `success`.
4. The scheduler's `last_scheduling_decision` timestamp is advancing (seconds-fresh on repeated
   checks), and it autonomously picks up the next queued `DagRun` with no manual action.
5. Zero new task-instance failures in the minutes following the fix.
