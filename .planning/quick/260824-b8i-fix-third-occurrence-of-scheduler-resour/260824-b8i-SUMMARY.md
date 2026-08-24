---
phase: quick-260824-b8i-fix-third-occurrence-of-scheduler-resour
plan: 01
subsystem: testing
tags: [pytest, kubectl, kubernetes, airflow, e2e, chaos-testing, vault]

# Dependency graph
requires:
  - phase: quick-260824-akz
    provides: "Established the live-detection pattern (_scheduler_resource_ref) for the same bug class in tests/e2e/chaos/test_vault_unavailable.py and test_minio_unavailable.py"
provides:
  - "tests/e2e/vault/test_airflow_backend.py's SEC-05 check (test_airflow_conn_minio_default_is_absent_from_every_component) now live-detects airflow-scheduler's real object kind instead of assuming it is always a Deployment"
affects: [tests/e2e/vault, tests/e2e/chaos, e2e-chaos-ci-workflow]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Live kubectl probe (deployment then statefulset, --ignore-not-found) to resolve a component's actual object kind on the connected cluster, rather than a PROFILE env-var branch that doesn't propagate through make chaos-verify"
    - "Single module-level name constant referenced by every probe/call site touching a dual-kind component, so the literal object name is written exactly once"

key-files:
  created: []
  modified:
    - tests/e2e/vault/test_airflow_backend.py

key-decisions:
  - "Introduced a bare-kind-string helper (_scheduler_kind returning 'deployment'/'statefulset') rather than reusing test_vault_unavailable.py's ref-string helper (_scheduler_resource_ref returning 'deploy/name'), because this module's own loop already keys on kind and name as two separate positional kubectl_json arguments, not a combined ref"
  - "Added a _SCHEDULER_NAME module constant so the literal string 'airflow-scheduler' appears exactly once in the file, referenced by both live-probes and the follow-up kubectl_json call — satisfies the plan's own machine-checkable grep verification criterion while keeping identical runtime behavior to the sibling file's inline-literal style"

requirements-completed: []

# Metrics
duration: 15min
completed: 2026-08-24
---

# Quick Task 260824-b8i: Fix Third Occurrence of Scheduler Resource-Kind Hardcoding Summary

**Live-detects airflow-scheduler's object kind (Deployment vs StatefulSet) in tests/e2e/vault/test_airflow_backend.py's SEC-05 check via a new `_scheduler_kind` kubectl-probe helper, closing the third and final known occurrence of the scheduler resource-kind hardcoding bug class.**

## Performance

- **Duration:** ~15 min
- **Completed:** 2026-08-24T06:11:40Z
- **Tasks:** 1
- **Files modified:** 1

## Accomplishments

- Removed `"airflow-scheduler"` from the `_DEPLOYMENTS` tuple in `tests/e2e/vault/test_airflow_backend.py` — it is no longer assumed to always be a Deployment.
- Added `_scheduler_kind(kubectl_fn)`, a module-level helper that live-probes the connected cluster (`kubectl get deployment airflow-scheduler --ignore-not-found`, then `kubectl get statefulset airflow-scheduler --ignore-not-found`) and returns a bare kind string (`"deployment"` or `"statefulset"`), raising `AssertionError` if neither object exists.
- Wired the helper into `test_airflow_conn_minio_default_is_absent_from_every_component`: the test now takes both `kubectl` and `kubectl_json`, computes `scheduler_kind` once, and checks the resolved object for the retired `AIRFLOW_CONN_MINIO_DEFAULT` env var using the same field-access pattern as the existing `_DEPLOYMENTS`/`_STATEFULSETS` loop.
- `airflow-api-server`, `airflow-dag-processor`, and `airflow-triggerer` checks are unchanged in behavior — only `airflow-scheduler` moved from hardcoded to live-detected.
- `ruff check`, `mypy` (on the target file, isolated from an unrelated pre-existing error in a different transitively-imported file), and `pytest --collect-only` all pass cleanly.

## Task Commits

1. **Task 1: Live-detect airflow-scheduler's object kind in test_airflow_backend.py** - `9815c7d` (fix)

**Plan metadata:** committed separately by the orchestrator (docs commit, not part of this agent's task commits).

## Files Created/Modified

- `tests/e2e/vault/test_airflow_backend.py` - Removed `airflow-scheduler` from `_DEPLOYMENTS`; added `_SCHEDULER_NAME` constant, `_scheduler_kind` live-detection helper, and a scheduler-specific SEC-05 check in `test_airflow_conn_minio_default_is_absent_from_every_component`.

## Decisions Made

- `_scheduler_kind` returns a bare kind string, not a `kind/name` ref, matching this file's own two-positional-argument `kubectl_json` call shape — deliberately different from the sibling `_scheduler_resource_ref` helper in `tests/e2e/chaos/test_vault_unavailable.py`, which returns a combined ref string for its own `kubectl exec <ref>` call shape.
- Introduced a `_SCHEDULER_NAME = "airflow-scheduler"` module constant, referenced by both live-probes inside `_scheduler_kind` and by the test's own follow-up `kubectl_json` call, so the literal appears exactly once in the file. This was not explicitly specified in the plan's task action text (which described two separate inline `"airflow-scheduler"` literals, mirroring the sibling file's style) but is required to satisfy the plan's own `<verification>` section, item 4 (`grep -c '"airflow-scheduler"'` must report `1`). Runtime behavior is identical either way; this is a cosmetic/DRY refactor to satisfy an explicit machine-checkable success criterion in the same plan.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Reconciled a literal-count mismatch between the plan's task action text and its own verification criterion**
- **Found during:** Task 1 verification (`grep -c '"airflow-scheduler"'`)
- **Issue:** The plan's task action text described `_scheduler_kind` using two separate inline `"airflow-scheduler"` string literals (one per probe), plus a third at the test's own follow-up `kubectl_json` call site — 3 literal occurrences total. But the plan's own `<verification>` section (item 4) states `grep -c '"airflow-scheduler"' tests/e2e/vault/test_airflow_backend.py` must report `1`. Implementing the action text literally as described would fail the plan's own stated verification.
- **Fix:** Introduced a `_SCHEDULER_NAME` module constant holding the one literal string, and referenced it from both probes in `_scheduler_kind` and from the test's own `kubectl_json` call, reducing the quoted-literal count to exactly 1 while preserving identical runtime behavior.
- **Files modified:** tests/e2e/vault/test_airflow_backend.py
- **Verification:** `grep -c '"airflow-scheduler"' tests/e2e/vault/test_airflow_backend.py` now reports `1`; `grep -c '_scheduler_kind'` reports `4` (>=2 required); `ruff check`, `mypy`, and `pytest --collect-only` all pass.
- **Committed in:** `9815c7d` (part of Task 1 commit — not a separate commit, since this was resolved during initial implementation, before the task commit was made)

---

**Total deviations:** 1 auto-fixed (Rule 1 — internal plan inconsistency between action text and verification criterion)
**Impact on plan:** No scope creep; resolves an internal contradiction in the plan document itself in favor of the machine-checkable verification criterion, while satisfying every other stated `must_haves` truth and the task's `<done>` description verbatim.

## Issues Encountered

- `uv run mypy tests/e2e/vault/test_airflow_backend.py` (the exact command in the plan's `<verify>` block) reports one pre-existing error at `tests/e2e/cluster/test_airflow_workloads.py:244` (`cur.fetchone()[0]` — `Value of type "Any | None" is not indexable`), surfaced only because that file is transitively imported (`from tests.e2e.cluster.test_airflow_workloads import metadata_connection`). This file was not touched by this plan and the error predates this task's changes. Confirmed the target file itself is clean via `uv run mypy --follow-imports=silent tests/e2e/vault/test_airflow_backend.py` → `Success: no issues found in 1 source file`. Out of scope per the scope boundary rule (pre-existing issue in an unrelated file); not fixed here.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- This closes the third and, per the plan's own stated scope, final known occurrence of the scheduler resource-kind (Deployment vs StatefulSet) hardcoding bug class across `tests/e2e/chaos/test_vault_unavailable.py`, `tests/e2e/chaos/test_minio_unavailable.py` (both fixed in quick task `260824-akz`), and now `tests/e2e/vault/test_airflow_backend.py`.
- No live cluster/CI run was required to close this plan — static verification (`ruff`, `mypy`, `pytest --collect-only`) is sufficient per the plan's own success criteria. Live re-verification against a real `e2e-chaos.yml` CI run (or `make chaos-verify` against a live CI-profile cluster) is the natural follow-up to confirm this fix holds under both profiles, but is explicitly deferred, matching this plan's own stated scope.
- The pre-existing `tests/e2e/cluster/test_airflow_workloads.py:244` mypy error (unrelated to this task) remains open and un-actioned; worth a small standalone quick task if `mypy tests/e2e/vault/test_airflow_backend.py` (without `--follow-imports=silent`) is ever used as a CI gate as-is, since it will fail on that transitively-imported pre-existing issue.

---
*Phase: quick-260824-b8i-fix-third-occurrence-of-scheduler-resour*
*Completed: 2026-08-24*

## Self-Check: PASSED

- FOUND: tests/e2e/vault/test_airflow_backend.py
- FOUND: 9815c7d (Task 1 commit)
