---
phase: quick-260817-trim-monitoring-stack-helm-values-cpu
plan: 01
subsystem: infra
tags: [helm, kubernetes, prometheus, grafana, tempo, otel-collector, resource-sizing, kind]

requires: []
provides:
  - "Trimmed CPU requests.cpu for grafana, grafana.sidecar, kube-state-metrics, prometheus-node-exporter, prometheusOperator, and prometheus.prometheusSpec in helm/values/local/monitoring.yaml, matching helm/values/ci/monitoring.yaml exactly"
  - "Trimmed CPU requests.cpu for tempo and otel-collector in their respective local values files, matching the ci profile"
  - "~305m freed on airflow-platform-worker, ~330m freed on airflow-platform-worker2 (real, schedulable, not yet deployed)"
affects: [kind-cluster-capacity, monitoring-stack, csv-ingest-dags]

tech-stack:
  added: []
  patterns: ["Reuse the already-vetted ci values profile as the sizing precedent for a local-profile trim, instead of deriving new numbers from a point-in-time usage snapshot alone"]

key-files:
  created: []
  modified:
    - helm/values/local/monitoring.yaml
    - helm/values/local/tempo.yaml
    - helm/values/local/otel-collector.yaml

key-decisions:
  - "Target values = helm/values/ci/*.yaml's already-committed numbers for the same components, not new numbers derived solely from a live usage snapshot -- reuses an existing, already-reviewed precedent and still leaves several-times headroom over measured actual usage (e.g. ci's prometheus 100m request vs ~23.6m measured, tempo/otel-collector's 100m vs ~3-4m measured)."
  - "Only resources.requests.cpu touched. memory, limits, initChownData/downloadDashboards init-container blocks, admissionWebhooks.patch, retention, and storageSpec/persistence sizing were explicitly left unchanged."

patterns-established: []

requirements-completed: []

duration: ~40min (executor stalled mid-Task-2, orchestrator diagnosed and finished)
completed: 2026-08-17
---

# Quick Task 260817-rvq: Trim Monitoring Stack Helm Values CPU Requests Summary

**Trimmed 8 CPU `requests` values across 3 local Helm values files (monitoring.yaml, tempo.yaml, otel-collector.yaml) to exactly match the already-vetted `ci` profile, freeing ~305-330m of real schedulable CPU per kind worker node.**

## Performance

- **Duration:** ~40 min (executor agent stalled 600s into Task 2's verification; orchestrator diagnosed the stall as a false positive and finished manually)
- **Tasks:** 2 (both complete)
- **Files modified:** 3

## Accomplishments

- `helm/values/local/monitoring.yaml`: `grafana` (100m→50m), `grafana.sidecar` (25m→10m), `kube-state-metrics` (50m→20m), `prometheus-node-exporter` (50m→20m), `prometheusOperator` (50m→20m), `prometheus.prometheusSpec` (250m→100m) — all now byte-identical to `helm/values/ci/monitoring.yaml`'s own values for the same keys.
- `helm/values/local/tempo.yaml`: `resources.requests.cpu` 250m→100m, matching `ci/tempo.yaml`.
- `helm/values/local/otel-collector.yaml`: `resources.requests.cpu` 250m→100m, matching `ci/otel-collector.yaml`.
- `prometheusOperator.admissionWebhooks.patch`'s CPU request (20m, a one-shot Job) confirmed untouched, as scoped.
- No `memory` or `limits` value touched anywhere — confirmed via `git diff` spot-check (only comment text mentioning "Memory untouched" appears, no actual value changes).

## Task Commits

1. **Task 1: Trim CPU requests in all three local monitoring-stack values files** — `0404941` (fix)
2. **Task 2: Static render/lint validation and D-06 policy regression check** — no separate commit; verification re-run by the orchestrator after Task 1 (see Issues Encountered)

**Worktree merge:** `19ea3f8` (chore: merge quick task worktree)

## Files Created/Modified

- `helm/values/local/monitoring.yaml` — 6 CPU requests trimmed to ci-profile values, rationale comments updated
- `helm/values/local/tempo.yaml` — 1 CPU request trimmed to ci-profile value
- `helm/values/local/otel-collector.yaml` — 1 CPU request trimmed to ci-profile value

## Decisions Made

- Reused `helm/values/ci/*.yaml`'s already-committed values as the trim target rather than inventing new numbers purely from the live Prometheus usage snapshot — a stronger, already-reviewed precedent that still carries several-times headroom over measured actual usage.

## Deviations from Plan

None in the actual file edits — Task 1 executed exactly as planned and its own automated YAML key-path comparison (re-run by the orchestrator post-merge) confirms all 8 values are byte-identical to their `ci` counterparts, with `admissionWebhooks.patch` untouched.

## Issues Encountered

**Executor agent stalled 600s into Task 2's verification (`make helm-lint && make manifests && make policy`), reported `failed`.** Task 1's file edits and commit (`0404941`) were already complete and clean at the time of the stall — no work was lost. The orchestrator:

1. Inspected the stalled agent's worktree: Task 1's commit was present and clean, no uncommitted changes.
2. Merged the worktree's commit into `main` (`19ea3f8`) and removed the worktree/branch.
3. Re-ran `make helm-lint` (15s, 0 charts failed), `make manifests` (44s, kubeconform -strict: 299 valid, 0 invalid), and `make policy` directly.
4. `make policy`'s first run showed **1 failure**: `tests/policy/test_print_ban_scope.py::test_no_inline_suppression_relaxes_the_ban_off_the_agreed_paths`, flagging a `noqa: T201` match inside a file path under `.claude/worktrees/agent-af406b4633cb79355/tests/policy/test_print_ban_scope.py`. This was a **false positive** caused by the not-yet-cleaned-up stalled worktree still sitting inside the repo tree — the policy test's own file-scanning walker doesn't exclude `.claude/worktrees/`, so it picked up a stale nested copy of its own test file (whose docstring contains a `noqa: T201` example as literal text). Not a regression from this task's changes.
5. After removing the worktree (step 2, already done), re-ran `make policy`: **124 passed, 10 deselected**, clean.

No code-level issue with the values-file changes themselves; the failure was entirely an artifact of the stalled worktree's presence during the first verification attempt.

## Next Phase Readiness

- **Deployed live, same session, on user request:** `bash scripts/stages/85-monitoring.sh` (idempotent `helm upgrade --install`, `PROFILE=local` default) applied all three releases — `otel-collector` (rev 3), `tempo` (rev 3), `monitoring`/kube-prometheus-stack (rev 5). All pods rolled out cleanly to `Running`/`Ready`; new pods independently confirmed via `kubectl get pod -o jsonpath` to carry the exact trimmed `requests.cpu` values (grafana 50m, sidecar 10m, kube-state-metrics 20m, prometheusOperator 20m, otel-collector 100m, prometheus 100m, tempo 100m).
- **Measured live improvement** (`kubectl describe nodes`, before → after):
  - `airflow-platform-worker`: 2750-3000m (91-100%) → **2390m (79%)** — ~360-610m freed.
  - `airflow-platform-worker2`: 2610m (87%) → **2320m (77%)** — ~290m freed.
- Does not by itself resolve the CPU-budget blocker tracked in `STATE.md`'s Blockers (that's the physical 12-core host ceiling, unaffected by this trim) — but both worker nodes now sit meaningfully below the ~700-800m real-headroom starvation threshold documented there, for the first time this session without needing the destructive `kind delete cluster` recreation path.

---
*Quick task: 260817-rvq*
*Completed: 2026-08-17*
