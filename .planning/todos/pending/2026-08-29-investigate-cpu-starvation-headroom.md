---
created: 2026-08-29T00:00:00Z
title: Investigate CI node CPU-starvation (~420m headroom after baseline platform pods)
area: platform
files:
  - kind/cluster.yaml
  - helm/values/ci/
  - airflow/dags/_common/kpo.py
---

## Problem

The CI kind cluster's single node is chronically CPU-starved:
baseline platform pods (Airflow control plane, MinIO, Vault, CNPG, Kyverno,
monitoring, etc.) alone already consume **~86% (2580m/3000m) of allocatable
CPU** before a single ETL pod (`discover`/`stage`/`dbt_build`/`publish`/
`integrity_gate`) ever launches, leaving only **~420m of headroom** for all
of both DAGs' concurrent ETL work combined.

Confirmed live and repeatedly during `debug/ci-pipeline-ingestion-timeout`
(`.planning/debug/resolved/ci-pipeline-ingestion-timeout.md` once archived):

- ROUND 19: 3483 `Insufficient cpu` `FailedScheduling` events in one
  `e2e-full.yml` run.
- ROUND 20: 3657 `Insufficient cpu` `FailedScheduling` events in the
  following run — **not newly introduced by ROUND 20's own fixes**, a
  pre-existing condition confirmed present in at least the two most recent
  runs at the time of writing.
- ROUND 20's own root-cause analysis for the `dbtkill` test failure named
  CPU-starvation as ONE of two contributing factors (the other being an
  internally-inconsistent timeout-budget hierarchy, fixed separately in
  ROUND 21): a killed/retried stage task needing multiple retry cycles to
  actually get a pod scheduled under this contention level can consume its
  entire `dagrun_timeout=45min` budget on scheduling delay alone, even with
  a well-margined per-attempt `execution_timeout`.

This is a genuine, structural resource-sizing problem, distinct from (and
upstream of) any DAG-level retry/timeout tuning. ROUND 21's own timeout-
budget rebalance (execution_timeout 10min->6min, retries trimmed, a shared
30s retry_delay applied everywhere) makes the DAGs' worst-case retry
arithmetic fit safely under `dagrun_timeout` even under today's contention
level, but it does not address WHY the contention exists in the first
place — a sufficiently pathological CI run could still exhaust the
rebalanced budget if the ~420m headroom shrinks further (e.g. another
platform component's requests grow) or a mapped fan-out concentrates more
concurrent ETL demand than 420m can serve.

## Solution

TBD. Start a **new, separate `/gsd:debug` or `/gsd:quick` investigation**
scoped specifically to CI node CPU capacity once `debug/ci-pipeline-
ingestion-timeout` closes out — do not fold this into that session; it is a
platform resource-sizing question, not a DAG-orchestration bug.

Candidate directions (not mutually exclusive):

1. **Right-size platform-pod CPU requests.** Audit every non-ETL pod's
   `resources.requests.cpu` on the CI profile (`helm/values/ci/*.yaml` --
   `airflow.yaml`, `minio.yaml`, `vault.yaml`, `cnpg-*.yaml`, `kyverno.yaml`,
   `monitoring.yaml`, `otel-collector.yaml`, `tempo.yaml`,
   `ingress-nginx.yaml`) against actual measured usage — many requests were
   likely set from local-profile defaults or upstream chart defaults never
   revisited for the CI node's tighter 4 CPU / 16 GB ceiling (README's own
   CI-runner-sizing constraint).
2. **Reduce concurrent ETL pod resource requests and/or concurrency caps**
   specifically for the CI profile — e.g. a smaller `stage_cpu_request`
   Airflow Variable than today's 200m (ROUND 10 precedent), or a lower
   `max_active_tis_per_dag`/`max_active_tasks` ceiling so fewer ETL pods
   compete for the same ~420m at once.
3. **Disable or trim non-essential platform components on the CI profile**
   if any are running but not actually exercised by the e2e suites (the
   monitoring stack is already profile-parameterized per README's own
   CI-runner-sizing constraint — confirm it is actually minimized, not just
   nominally "CI profile").
4. Re-run `kubectl top nodes` / `kubectl describe node` against a live CI
   run (or the ephemeral kind cluster reproduced locally with the CI
   profile) to get a precise, current per-pod CPU-request breakdown before
   choosing where to cut — the 86%/420m figures above are aggregate
   `FailedScheduling`-event counts and the ROUND-20-era headroom estimate,
   not a fresh per-component audit.

Re-check the live `FailedScheduling` event count for the current failure
signature before starting, since more pushes (and ROUND 21's own timeout
rebalance) will have landed by the time this is picked up.
