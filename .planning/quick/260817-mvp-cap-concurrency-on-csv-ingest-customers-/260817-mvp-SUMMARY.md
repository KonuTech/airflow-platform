---
phase: quick-260817-cap-integrity-gate-concurrency
plan: 01
subsystem: airflow-dags
tags: [concurrency, kubernetes-executor, resource-starvation, taskflow]
dependency-graph:
  requires: []
  provides:
    - "integrity_gate.max_active_tis_per_dag=3 in csv_ingest_customers"
    - "integrity_gate.max_active_tis_per_dag=3 in csv_ingest_orders"
  affects:
    - "airflow/dags/csv_ingest_customers.py"
    - "airflow/dags/csv_ingest_orders.py"
tech-stack:
  added: []
  patterns:
    - "TaskFlow @task concurrency cap via .override(max_active_tis_per_dag=N).partial(...).expand(...) -- NOT .partial(..., max_active_tis_per_dag=N)"
key-files:
  created: []
  modified:
    - airflow/dags/csv_ingest_customers.py
    - airflow/dags/csv_ingest_orders.py
    - tests/unit/test_dag_structure.py
decisions:
  - "TaskFlow-decorated tasks (@task) must use .override(...) to set BaseOperator-level fields on a mapped call, not .partial(...) -- .partial() on a TaskDecorator validates kwargs against the decorated FUNCTION's own signature and routes everything through to op_kwargs, unlike a classic operator's .partial() (e.g. KubernetesPodOperator.partial())."
metrics:
  duration: "~35min"
  completed: 2026-08-17
---

# Quick Task 260817-mvp: Cap integrity_gate concurrency on csv_ingest_customers/orders Summary

Capped the `integrity_gate` dynamically-mapped TaskFlow task's concurrency
at 3 pods per DagRun in both `csv_ingest_customers` and `csv_ingest_orders`,
preventing an unbounded `.expand(key=matched_keys)` fan-out from exhausting
kind worker nodes' ~700-800m real CPU headroom and starving other DAGs'
pod scheduling cluster-wide.

## What Was Built

**Task 1 (complete, offline, TDD RED/GREEN):**

- Added `test_integrity_gate_concurrency_capped` to
  `tests/unit/test_dag_structure.py`, asserting
  `dag.task_dict["integrity_gate"].max_active_tis_per_dag == 3` for both
  `csv_ingest_customers` and `csv_ingest_orders`.
  - RED confirmed: `AssertionError: assert None == 3` against the
    unmodified DAG files (commit `faa43f3`).
- Fixed both `airflow/dags/csv_ingest_customers.py` and
  `airflow/dags/csv_ingest_orders.py`: changed
  `integrity_gate.partial(bucket=..., dataset_name=...).expand(key=matched_keys)`
  to
  `integrity_gate.override(max_active_tis_per_dag=3).partial(bucket=..., dataset_name=...).expand(key=matched_keys)`
  (commit `ea5a38e`).
  - GREEN confirmed: the new test passes for both DAGs; full
    `tests/unit/test_dag_structure.py` suite (11 tests) and the full
    `tests/unit` suite (484 tests) pass unaffected.
  - `ruff check` and `ruff format --check` both pass on all three modified
    files.
  - `grep -c "max_active_tis_per_dag=3"` reports exactly 1 match in each
    DAG file, matching the plan's own verification criterion #3.

**Task 2 (completed post-merge-back by the orchestrator from the main tree — see below).**

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Plan's assumed `.partial()` mechanism does not work for TaskFlow-decorated tasks**

- **Found during:** Task 1, while implementing the GREEN step.
- **Issue:** The plan's `<mechanism_confirmed_live_against_installed_venv>`
  context block asserted that
  `integrity_gate.partial(bucket=..., dataset_name=..., max_active_tis_per_dag=3)`
  would work, citing `TracingKubernetesPodOperator.partial(..., max_active_tis_per_dag=1, ...)`
  as precedent. That precedent is a **classic operator's** `.partial()`
  (`MappedOperator`/`BaseOperator`'s own classmethod), which validates
  kwargs against `BaseOperator`'s signature. `integrity_gate` is a
  **TaskFlow-decorated** `@task` function; its `.partial()` is a different
  method (`_TaskDecorator.partial()` in
  `airflow/sdk/bases/decorator.py`), which validates kwargs against the
  *decorated function's own signature* (`bucket`/`key`/`dataset_name`) and
  folds everything through to `op_kwargs`. Passing `max_active_tis_per_dag`
  there raised, live: `TypeError: partial() got an unexpected keyword
  argument 'max_active_tis_per_dag'` — confirmed by running the offline
  DagBag import (`test_no_import_errors`) against the first attempted fix.
- **Fix:** Used `integrity_gate.override(max_active_tis_per_dag=3).partial(bucket=..., dataset_name=...)`
  instead. `TaskDecorator.override(**kwargs)` merges kwargs into
  `self.kwargs`, which IS filtered against `BaseOperator`'s signature
  inside `_expand()` — the correct, documented way to set a
  BaseOperator-level field (like `max_active_tis_per_dag`) on a
  TaskFlow-decorated task ahead of `.partial()`/`.expand()`.
- **Files modified:** `airflow/dags/csv_ingest_customers.py`,
  `airflow/dags/csv_ingest_orders.py`.
- **Commit:** `ea5a38e`.

**2. [Rule 1 - Correction] Comment text duplicated the literal `max_active_tis_per_dag=3` string, breaking the plan's own grep-count verification**

- **Found during:** Task 1, running the plan's declared verification
  command #3 (`grep -c "max_active_tis_per_dag=3" ...` expects exactly 1
  match per file) after the GREEN fix and `ruff format` pass.
- **Issue:** My first-draft explanatory comments repeated the literal
  string `max_active_tis_per_dag=3` (once in a leading label, once
  quoting the failed `.partial()` call) in addition to the real code line,
  making `grep -c` report 3 matches in `csv_ingest_customers.py` and 2 in
  `csv_ingest_orders.py` instead of the plan's expected 1.
- **Fix:** Rewrote the comments to describe the mechanism without
  repeating the exact literal kwarg=value string, leaving exactly one
  match per file (the real code line).
- **Files modified:** Same two DAG files.
- **Commit:** `ea5a38e` (folded into the same commit, pre-push).

None of the above required user input — both are Rule 1 (bug/correctness)
fixes discovered and resolved inline during Task 1's own execution.

## Task 2: Live Verification (completed post-merge-back by the orchestrator)

Executed from the main tree after the worktree branch (`worktree-agent-add67f73d3d5b3274`,
commits `faa43f3`, `ea5a38e`) was merged to `main` (merge commit follows
in git log) and the worktree removed.

**Deploy:** `kubectl rollout restart deployment/airflow-dag-processor -n airflow`
at `2026-08-17T14:45:22Z`. The rollout itself hit the exact bug this plan
fixes: the NEW dag-processor pod sat `Pending` for ~4 minutes with
`FailedScheduling: 2 Insufficient cpu`, competing against the still-live
**pre-fix** backlog (16 non-terminal `integrity_gate` pods, both worker
nodes at 97-100% CPU). This is expected and non-blocking -- `Pending` pods
hold no CPU reservation, so it cost only wall-clock time, not cluster
health; the OLD dag-processor pod kept serving normally throughout. The new
pod (carrying the fix) reached `2/2 Running` at `2026-08-17T14:45:30Z`,
which is the restart timestamp used for all filtering below.

**Structural cap verification (authoritative — Airflow's own task-instance
states, not pod-phase snapshots):** queried
`airflow tasks states-for-dag-run csv_ingest_customers scheduled__2026-08-17T14:40:00+00:00 -o json`
mid-window and inspected `integrity_gate`'s mapped-index `start_date`/
`end_date` timeline directly. Overlapping `running` windows never exceeded
3 concurrent map indices at any point observed (e.g. indices 42/43/44 ran
concurrently for an ~11s window, indices 45/46 for a separate window with
47 correctly held at `queued` and 48-53 at `scheduled` rather than being
launched) -- this is the ground truth for what Airflow's scheduler actually
launched, and it never exceeded the cap.

**Pod-count polling (5-minute window, 12s interval, `Pending`/`Running`
pods with `creationTimestamp >= 2026-08-17T14:45:30Z`):**

```
17/24 samples = 3
 4/24 samples = 4
 3/24 samples = 5
 0/24 samples < 3 or > 5
```

The 7/24 samples reading 4-5 are **not** scheduler cap violations --
cross-referenced against the task-instance timeline above, they land
exactly at the moment a `success`-state pod's container has exited but
Kubernetes/KubernetesExecutor has not yet deleted the pod object, so a
finished pod briefly still reads `Running` phase in a raw `kubectl get
pods` snapshot while a fresh one is already starting underneath the same
cap. This is K8s pod-lifecycle cleanup lag, not evidence of more than 3
task instances genuinely executing concurrently -- confirmed by the
task-instance-level data being the authoritative source. Worth noting as
an honest measurement nuance rather than glossing over it: a naive
pod-phase-only check (as the plan originally specified) would have to
tolerate this ±1-2 transient overshoot rather than asserting a hard `<= 3`
on every single sample.

**Corroborating evidence the actual goal was achieved (starvation of OTHER
tasks stopped):** `kubectl get events -n airflow --field-selector
reason=FailedScheduling`, filtered to exclude `integrity-gate` pod names,
shows the most recent such event for any OTHER task
(`wait_for_files`/`discover`/`resolve_window`/`list_matched_keys`/
`build_ingest_args`, all `csv_ingest_customers` AND `csv_ingest_orders`)
was **28+ minutes before** the fix's restart timestamp. Zero new
`FailedScheduling` events for anything other than `integrity_gate` (and
the dag-processor's own transitional restart pod) occurred in the ~9
minutes after the fix went live -- directly confirming the starvation this
plan set out to fix has stopped, not just that the `integrity_gate` count
itself looks better in isolation.

**`csv_ingest_orders` live fan-out:** did not occur during the
verification window (asset-triggered off customers' `ingest` publish; no
publish happened in this window, so no fresh orders backlog materialized).
Per the plan's own accepted scope for this case, Task 1's offline
`test_integrity_gate_concurrency_capped` (passing for both DAGs, exact
same code path just proven live for customers) is the accepted
verification for orders.

## Verification Status

| Plan verification item | Status |
|---|---|
| `pytest tests/unit/test_dag_structure.py -q` passes offline | PASS (11/11, plus 484/484 full unit suite) |
| Live: max concurrent `integrity_gate` pods never exceeds 3 | PASS at the Airflow task-instance level (authoritative); pod-phase snapshots show occasional +1-2 transient overshoot from K8s pod-cleanup lag, explained and cross-verified above, not a real cap violation |
| No new `FailedScheduling`/`Insufficient cpu` events for other tasks post-fix | PASS -- zero in ~9min post-fix window vs. 12 such events in the preceding ~1hr |
| `grep -c "max_active_tis_per_dag=3"` == 1 per file | PASS (1 in each of `csv_ingest_customers.py`, `csv_ingest_orders.py`) |
| `csv_ingest_orders` live fan-out cap | Not exercised live (no active orders backlog this window); accepted via Task 1's offline structural proof per plan's own scope |

## Self-Check: PASSED

- FOUND: airflow/dags/csv_ingest_customers.py
- FOUND: airflow/dags/csv_ingest_orders.py
- FOUND: tests/unit/test_dag_structure.py
- FOUND commit: faa43f3 (test, RED)
- FOUND commit: ea5a38e (fix, GREEN)
