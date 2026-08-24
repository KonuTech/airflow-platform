---
phase: quick-260824-akz-fix-scheduler-resource-kind-deployment-v
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - tests/e2e/chaos/test_vault_unavailable.py
  - tests/e2e/chaos/test_minio_unavailable.py
autonomous: true
requirements:
  - "deferred-items.md 'Plan 11-05' background: e2e-chaos.yml's first live merge-triggered run failed 4 tests with `deployments.apps \"airflow-scheduler\" not found` — CI's LocalExecutor profile renders `<release>-scheduler` as a StatefulSet (per plan 11-04's own scripts/stages/70-airflow.sh fix), but tests/e2e/chaos/test_vault_unavailable.py's `_poll_task_instance_state` and tests/e2e/chaos/test_minio_unavailable.py's `_poll_task_instance_state`/`_poll_dagrun_state` all still hardcode `kubectl exec deploy/airflow-scheduler`, never updated when 70-airflow.sh was fixed"

must_haves:
  truths:
    - "No `kubectl exec ... deploy/airflow-scheduler ...` literal remains hardcoded anywhere in tests/e2e/chaos/test_vault_unavailable.py or tests/e2e/chaos/test_minio_unavailable.py"
    - "Both files determine the scheduler's actual kubectl exec target (deploy/airflow-scheduler vs statefulset/airflow-scheduler) by live-querying which object kind the chart actually rendered on the connected cluster, not by reading a PROFILE-style environment variable (make chaos-verify / e2e-chaos.yml never export PROFILE into the pytest process's own environment, verified live via .github/workflows/e2e-chaos.yml and Makefile's chaos-verify target — an env-var read would silently default wrong and reproduce this exact bug class under a different name)"
    - "All three affected polling helpers (test_vault_unavailable.py's _poll_task_instance_state; test_minio_unavailable.py's _poll_task_instance_state and _poll_dagrun_state) use the live-detected resource reference"
    - "ruff check and mypy both pass cleanly on both changed files"
  artifacts:
    - path: "tests/e2e/chaos/test_vault_unavailable.py"
      provides: "profile-aware (live-detected) scheduler kubectl exec target in _poll_task_instance_state"
      contains: "_scheduler_resource_ref"
    - path: "tests/e2e/chaos/test_minio_unavailable.py"
      provides: "profile-aware (live-detected) scheduler kubectl exec target in _poll_task_instance_state and _poll_dagrun_state"
      contains: "_scheduler_resource_ref"
  key_links:
    - from: "_poll_task_instance_state (both files) / _poll_dagrun_state (test_minio_unavailable.py)"
      to: "_scheduler_resource_ref's live kubectl get probe"
      via: "scheduler_ref = _scheduler_resource_ref(kubectl_fn) computed once before the poll loop, then used in place of the hardcoded deploy/airflow-scheduler literal"
      pattern: "_scheduler_resource_ref\\(kubectl_fn\\)"
---

<objective>
Fix the scheduler resource-kind hardcoding bug in `tests/e2e/chaos/test_vault_unavailable.py` and `tests/e2e/chaos/test_minio_unavailable.py`: both files' `kubectl exec` helpers unconditionally target `deploy/airflow-scheduler`, but the official Airflow chart renders `<release>-scheduler` as a **StatefulSet** under CI's LocalExecutor profile and as a **Deployment** under local's KubernetesExecutor profile (already fixed once, in `scripts/stages/70-airflow.sh`, by plan 11-04 — see that script's own "Post-merge fix (CICD-09 follow-up...)" comment, lines ~78-96). This exact mismatch was never propagated to these two test files, and caused 4 real test failures (`deployments.apps "airflow-scheduler" not found`) in `e2e-chaos.yml`'s first live merge-triggered run.

This is follow-up-1 of 2 scoped follow-ups blocking Phase 11's CICD-09 requirement from closing. Phase 11 itself is functionally complete (14/14 plans merged). Follow-up-2 (a separate `cluster-verify` CI-scoping decision) is explicitly out of scope for this plan.

Purpose: make the three affected `kubectl exec` call sites resolve the correct object kind for `airflow-scheduler` regardless of which profile produced the live cluster, closing the last known gap before CICD-09 can be marked complete.

Output: both files updated so no call site hardcodes `deploy/airflow-scheduler`; each instead computes the live resource reference once per poll-helper invocation via a small, per-file `_scheduler_resource_ref` helper.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/STATE.md
@scripts/stages/70-airflow.sh
@tests/e2e/chaos/conftest.py

<why_live_detection_not_profile_env_var>
The obvious mirror of `70-airflow.sh`'s fix would be `if os.environ.get("PROFILE", "local") == "ci": ... else: ...`. This is WRONG here and must not be used: `.github/workflows/e2e-chaos.yml` sets `PROFILE=ci` only as an inline prefix on the single `PROFILE=ci make cluster-up` step (line 74) — it is never written to `$GITHUB_ENV`, so it does not persist into the later `make chaos-verify` step (line 100) that actually runs these pytest files. `Makefile`'s `chaos-verify` target (`$(RUN_CLUSTER) pytest tests/e2e/chaos tests/e2e/vault -q -m cluster`) also never threads `PROFILE` through. A `PROFILE`-env-var read inside these test files would therefore silently evaluate to `"local"` even when running against a genuinely CI-profile cluster, defaulting straight back to the Deployment branch — reproducing this exact bug under a different mechanism. Live-querying which object kind the chart actually rendered (`kubectl -n airflow get deployment/statefulset airflow-scheduler`) is authoritative regardless of what the calling process's environment does or doesn't carry, and is the fix these two files need.
</why_live_detection_not_profile_env_var>

<why_per_file_duplication_not_a_shared_conftest_helper>
`tests/e2e/chaos/conftest.py` exists and is the natural place to hang a shared helper, but this repository has an explicit, already-documented convention against doing so for small polling helpers of exactly this kind: `test_vault_unavailable.py`'s own `_poll_task_instance_state` docstring states verbatim "Duplicated from `test_minio_unavailable.py`'s own helper of the same name (this repository's established convention: small helpers are copied per test tier/file, not shared through a library module)." `conftest.py`'s own re-exports are reserved for genuinely shared pytest fixtures (`kubectl`, `s3_client`, `analytics_connection`, ...), not standalone helper functions like this. This plan follows that established, explicitly-cited precedent: `_scheduler_resource_ref` is duplicated verbatim in both files, matching how `_poll_task_instance_state` itself is already duplicated. Do not add anything to `conftest.py`.
</why_per_file_duplication_not_a_shared_conftest_helper>

<current_shapes_to_modify>
Both files already have `from __future__ import annotations` and a `TYPE_CHECKING` block importing `subprocess`/`Callable`/`Iterator` as needed — reuse those, do not add new imports.

`tests/e2e/chaos/test_vault_unavailable.py`:
- `_poll_task_instance_state` (currently lines ~121-186): the `kubectl_fn(...)` call inside the `while` loop passes the flat positional args `"-n", "airflow", "exec", "deploy/airflow-scheduler", "-c", "scheduler", "--", "python3", "-c", script, timeout=30`. The literal `"deploy/airflow-scheduler"` is the 4th positional arg.

`tests/e2e/chaos/test_minio_unavailable.py`:
- `_poll_task_instance_state` (currently lines ~133-201): identical shape to the one above — same `"deploy/airflow-scheduler"` literal as the 4th positional arg inside its `kubectl_fn(...)` call.
- `_poll_dagrun_state` (currently lines ~204-247): same shape, same `"deploy/airflow-scheduler"` literal as the 4th positional arg inside its own `kubectl_fn(...)` call.
</current_shapes_to_modify>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Make test_vault_unavailable.py's scheduler kubectl exec target live-detected</name>
  <files>tests/e2e/chaos/test_vault_unavailable.py</files>
  <action>
    Add a new module-level helper function `_scheduler_resource_ref` placed immediately before `_poll_task_instance_state` (after `_build_orders_csv`). Signature: `def _scheduler_resource_ref(kubectl_fn: Callable[..., subprocess.CompletedProcess[str]]) -> str:`. Implementation: run `kubectl_fn("-n", "airflow", "get", "deployment", "airflow-scheduler", "-o", "name", "--ignore-not-found")`; if `proc.returncode == 0` and `proc.stdout.strip()` is non-empty, return the literal `"deploy/airflow-scheduler"`. Otherwise run the equivalent probe for `"statefulset"` in place of `"deployment"`; if that one succeeds with non-empty output, return `"statefulset/airflow-scheduler"`. If neither probe finds the object, raise `AssertionError` with a message stating that `airflow-scheduler` exists as neither a Deployment nor a StatefulSet in the `airflow` namespace. Write a Google-style docstring (Args/Returns/Raises) matching the existing docstring style in this file, and explain in 2-3 sentences WHY this queries the live cluster instead of reading a `PROFILE` environment variable — reference `scripts/stages/70-airflow.sh`'s own branch-on-`PROFILE` fix as the origin of the underlying distinction, and state inline that `PROFILE` is not exported into this pytest process's environment (this plan's `<why_live_detection_not_profile_env_var>` context block has the full reasoning — restate it concisely so the file is self-contained, don't just cite the plan).

    Then, inside `_poll_task_instance_state`: immediately before the `deadline = time.monotonic() + timeout` line, add `scheduler_ref = _scheduler_resource_ref(kubectl_fn)` (computed once, not per poll iteration — the resource kind cannot change mid-test). Replace the hardcoded `"deploy/airflow-scheduler"` positional argument inside the loop's `kubectl_fn(...)` call with `scheduler_ref`.

    Do not touch anything else in the file: `_existing_customer_ids`, `_build_orders_csv`, `_run_vault_unseal_script`, and the test function body itself are all unchanged, including every other `kubectl_fn(...)`/`kubectl(...)` call in the test body that already correctly targets `deploy/airflow-api-server` (unaffected by this bug — the API server is always a Deployment under both profiles) or `vault-0`/`vault` (unrelated resource entirely).
  </action>
  <verify>
    <automated>cd /home/konutec/projects/airflow-platform && uv run ruff check tests/e2e/chaos/test_vault_unavailable.py && uv run mypy tests/e2e/chaos/test_vault_unavailable.py && uv run pytest tests/e2e/chaos/test_vault_unavailable.py --collect-only -q</automated>
  </verify>
  <done>`_scheduler_resource_ref` exists in the file, live-probes Deployment then StatefulSet, and raises a clear `AssertionError` if neither exists. `_poll_task_instance_state` computes `scheduler_ref` once before its poll loop and uses it (not a hardcoded literal) in its `kubectl_fn` call. No literal `"deploy/airflow-scheduler"` string remains anywhere in the file. `ruff check`, `mypy`, and `pytest --collect-only` all pass with zero errors on this file.</done>
</task>

<task type="auto">
  <name>Task 2: Make test_minio_unavailable.py's scheduler kubectl exec targets (both helpers) live-detected</name>
  <files>tests/e2e/chaos/test_minio_unavailable.py</files>
  <action>
    Add the identical `_scheduler_resource_ref` helper described in Task 1 (byte-for-byte the same function body and docstring reasoning — this repository's established convention duplicates small helpers per file rather than sharing them; see this plan's `<why_per_file_duplication_not_a_shared_conftest_helper>` context block), placed immediately before `_poll_task_instance_state` (after `_build_orders_csv`).

    Wire it into BOTH affected helpers in this file:
    1. `_poll_task_instance_state`: same change as Task 1 — add `scheduler_ref = _scheduler_resource_ref(kubectl_fn)` immediately before `deadline = time.monotonic() + timeout`, and replace the hardcoded `"deploy/airflow-scheduler"` positional argument inside its loop's `kubectl_fn(...)` call with `scheduler_ref`.
    2. `_poll_dagrun_state`: identical pattern — add `scheduler_ref = _scheduler_resource_ref(kubectl_fn)` immediately before its own `deadline = time.monotonic() + timeout` line, and replace its hardcoded `"deploy/airflow-scheduler"` positional argument with `scheduler_ref`.

    Do not touch anything else in the file: `_existing_customer_ids`, `_build_orders_csv`, and the test function body are unchanged, including every `kubectl(...)` call already correctly targeting `deploy/airflow-api-server` (Deployment under both profiles, unaffected) or `deployment/minio` (a wholly separate, already-correct fix documented in this file's own module docstring — do not touch the MinIO scale/wait calls).
  </action>
  <verify>
    <automated>cd /home/konutec/projects/airflow-platform && uv run ruff check tests/e2e/chaos/test_minio_unavailable.py && uv run mypy tests/e2e/chaos/test_minio_unavailable.py && uv run pytest tests/e2e/chaos/test_minio_unavailable.py --collect-only -q</automated>
  </verify>
  <done>`_scheduler_resource_ref` exists in the file, identical in shape to Task 1's. Both `_poll_task_instance_state` and `_poll_dagrun_state` compute `scheduler_ref` once before their respective poll loops and use it (not a hardcoded literal) in their `kubectl_fn` calls. No literal `"deploy/airflow-scheduler"` string remains anywhere in the file. `ruff check`, `mypy`, and `pytest --collect-only` all pass with zero errors on this file.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|--------------|
| Test helper -> live Kubernetes API server | `_scheduler_resource_ref` issues read-only `kubectl get` probes against the connected cluster's `airflow` namespace to determine which object kind exists; this plan only adds read-only discovery calls, no new write path or trust boundary. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|------------------|
| T-quick-01 | Denial of Service (test-suite self-inflicted) | `_scheduler_resource_ref`'s live probe | accept | Adds one or two extra read-only `kubectl get` calls per poll-helper invocation (a handful per test run); negligible cost against an already-live cluster inside an already-cluster-gated (`pytest.mark.cluster`), non-CI-blocking-outside-chaos-suite E2E test. |
| T-quick-02 | Tampering (accidental logic regression) | `_scheduler_resource_ref`'s neither-found branch and both wiring call sites | mitigate | `AssertionError` with a clear message if neither Deployment nor StatefulSet is found, rather than silently falling through to a wrong literal; static verification (`ruff`, `mypy`) and `pytest --collect-only` catch syntax/type/import errors before any live run is attempted. Live re-run against a real cluster is deferred to a separate follow-up task per this plan's own scope (no live cluster contact required to close this plan). |
</threat_model>

<verification>
1. `ruff check tests/e2e/chaos/test_vault_unavailable.py tests/e2e/chaos/test_minio_unavailable.py` reports zero issues.
2. `mypy tests/e2e/chaos/test_vault_unavailable.py tests/e2e/chaos/test_minio_unavailable.py` reports zero errors.
3. `pytest tests/e2e/chaos/test_vault_unavailable.py tests/e2e/chaos/test_minio_unavailable.py --collect-only -q` collects both modules with zero collection errors.
4. `grep -c 'deploy/airflow-scheduler"' tests/e2e/chaos/test_vault_unavailable.py tests/e2e/chaos/test_minio_unavailable.py` reports `0` for both files (the hardcoded literal is gone).
5. `grep -c '_scheduler_resource_ref' tests/e2e/chaos/test_vault_unavailable.py` reports `>=2` (definition + 1 call site); the same grep against `test_minio_unavailable.py` reports `>=3` (definition + 2 call sites).
</verification>

<success_criteria>
- No `kubectl exec ... deploy/airflow-scheduler ...` literal remains hardcoded in either file.
- All three affected polling helpers (`test_vault_unavailable.py`'s `_poll_task_instance_state`; `test_minio_unavailable.py`'s `_poll_task_instance_state` and `_poll_dagrun_state`) resolve the scheduler's real object kind via a live cluster probe (`_scheduler_resource_ref`), computed once per helper invocation, not via a `PROFILE` environment-variable read.
- `_scheduler_resource_ref` is duplicated per-file (matching this repository's established convention for small polling helpers), not added to `tests/e2e/chaos/conftest.py`.
- `ruff check` and `mypy` both pass cleanly on both changed files; `pytest --collect-only` proves both modules remain importable.
- No live cluster/CI run is required to close this plan — that live re-verification is explicitly deferred to a separate follow-up task.
</success_criteria>

<output>
Create `.planning/quick/260824-akz-fix-scheduler-resource-kind-deployment-v/260824-akz-SUMMARY.md` when done.
</output>
