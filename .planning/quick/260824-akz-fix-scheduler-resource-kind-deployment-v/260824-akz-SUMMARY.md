---
phase: quick-260824-akz-fix-scheduler-resource-kind-deployment-v
plan: 01
subsystem: testing
tags: [pytest, kubectl, kubernetes, chaos-testing, airflow, ci-cd]

# Dependency graph
requires:
  - phase: 11-ci-cd-completion-operations
    provides: "scripts/stages/70-airflow.sh's PROFILE-branched wait_for_deploy_available/wait_for_statefulset_ready fix for airflow-scheduler's dual object-kind rendering (CI LocalExecutor -> StatefulSet, local KubernetesExecutor -> Deployment)"
provides:
  - "Live-detected (not PROFILE-env-var-based) scheduler kubectl exec target resolution in both e2e chaos test files"
  - "_scheduler_resource_ref helper, duplicated per-file per this repo's established small-helper convention"
affects: [e2e-chaos-testing, ci-cd-completion-operations]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Live-probe kubectl get deployment/statefulset (in that order) rather than reading PROFILE env var, since PROFILE is never exported into the pytest process by e2e-chaos.yml/Makefile chaos-verify"

key-files:
  created: []
  modified:
    - tests/e2e/chaos/test_vault_unavailable.py
    - tests/e2e/chaos/test_minio_unavailable.py

key-decisions:
  - "Duplicated _scheduler_resource_ref verbatim in both files rather than adding a shared conftest.py helper, matching this repo's own already-documented convention (test_vault_unavailable.py's _poll_task_instance_state docstring cites the same precedent)"
  - "Live cluster probe (kubectl get deployment/statefulset airflow-scheduler) chosen over a PROFILE environment-variable branch, because PROFILE is never exported into the pytest process by e2e-chaos.yml or Makefile's chaos-verify target -- an env-var read would silently default to the wrong branch under a different mechanism"

patterns-established:
  - "_scheduler_resource_ref(kubectl_fn) -> str: probes Deployment then StatefulSet, returns the correct kubectl exec target string, raises AssertionError if neither exists"

requirements-completed:
  - "deferred-items.md 'Plan 11-05' background: e2e-chaos.yml's first live merge-triggered run failed 4 tests with `deployments.apps \"airflow-scheduler\" not found`"

duration: 15min
completed: 2026-08-24
---

# Quick Task 260824-akz: Fix scheduler resource-kind hardcoding in e2e chaos tests Summary

**Both e2e chaos test files now live-probe whether `airflow-scheduler` is a Deployment or a StatefulSet via a duplicated `_scheduler_resource_ref` helper, instead of hardcoding `deploy/airflow-scheduler` — closing the last known gap blocking Phase 11's CICD-09 requirement.**

## Performance

- **Duration:** 15 min
- **Completed:** 2026-08-24T05:44:50Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- `tests/e2e/chaos/test_vault_unavailable.py`'s `_poll_task_instance_state` now resolves the scheduler's real object kind via a live `kubectl get` probe instead of a hardcoded `deploy/airflow-scheduler` literal
- `tests/e2e/chaos/test_minio_unavailable.py`'s `_poll_task_instance_state` and `_poll_dagrun_state` (both affected helpers in that file) do the same
- Neither file reads a `PROFILE` environment variable — the live probe is authoritative regardless of what the calling process's environment carries, matching the reasoning already documented in `scripts/stages/70-airflow.sh`

## Task Commits

Each task was committed atomically:

1. **Task 1: Make test_vault_unavailable.py's scheduler kubectl exec target live-detected** - `05bcd84` (fix)
2. **Task 2: Make test_minio_unavailable.py's scheduler kubectl exec targets (both helpers) live-detected** - `9e82be1` (fix)

**Plan metadata:** (added by orchestrator after this summary)

## Files Created/Modified
- `tests/e2e/chaos/test_vault_unavailable.py` - Added `_scheduler_resource_ref`; `_poll_task_instance_state` uses it instead of a hardcoded `deploy/airflow-scheduler` literal
- `tests/e2e/chaos/test_minio_unavailable.py` - Added `_scheduler_resource_ref`; both `_poll_task_instance_state` and `_poll_dagrun_state` use it

## Decisions Made
- Duplicated `_scheduler_resource_ref` byte-for-byte in both files rather than sharing it via `conftest.py`, following the repository's own explicit, already-documented convention for small per-file polling helpers.
- Chose live cluster detection (`kubectl get deployment`/`statefulset`) over a `PROFILE` environment-variable read, since `PROFILE` never reaches the pytest process that runs these tests (`e2e-chaos.yml` sets it only inline on a separate `make cluster-up` step; `chaos-verify`'s own pytest invocation never threads it through).

## Deviations from Plan

None - plan executed exactly as written. Only a minor line-length fix (ruff E501, two lines split across multiple lines for `kubectl_fn` call arguments) was needed during Task 1, applied inline as part of that task's own implementation before committing (not treated as a separate deviation since it was pure formatting of the plan's own specified code, not new logic).

## Issues Encountered
None.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Both files now pass `ruff check`, `mypy`, and `pytest --collect-only` cleanly.
- No `kubectl exec ... deploy/airflow-scheduler` literal remains hardcoded in either file (grep-confirmed: remaining occurrences are only inside `_scheduler_resource_ref`'s own docstring/return-value, which is the live-detection helper itself, not a call site).
- Live re-verification against a genuine CI-profile (StatefulSet) cluster is explicitly deferred to a separate follow-up, per this plan's own scope — no live cluster/CI run was required to close this plan.
- Follow-up-2 of 2 (the `cluster-verify` CI-scoping decision) remains out of scope and still open, tracked separately in Phase 11's deferred-items.md.

---
*Phase: quick-260824-akz-fix-scheduler-resource-kind-deployment-v*
*Completed: 2026-08-24*

## Self-Check: PASSED

- FOUND: tests/e2e/chaos/test_vault_unavailable.py
- FOUND: tests/e2e/chaos/test_minio_unavailable.py
- FOUND: 05bcd84
- FOUND: 9e82be1
