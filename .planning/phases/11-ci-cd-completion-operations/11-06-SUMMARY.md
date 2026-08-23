---
phase: 11-ci-cd-completion-operations
plan: 06
subsystem: infra
tags: [ci, github-actions, coverage, kyverno, helm, rollback, dagtest, airflow]

# Dependency graph
requires:
  - phase: 11-ci-cd-completion-operations
    provides: "plan 11-02's matrix publish.yml -- a real merge SHA (d5a3ec0) with all three images (csv-processor/dbt/airflow) published together, the exact fixture this plan's Task 3 needed"
provides:
  - "ci.yml's check job: coverage report/html steps writing to $GITHUB_STEP_SUMMARY plus an actions/upload-artifact upload of htmlcov/, no fail_under gate"
  - "Makefile test-dagtest target + ci.yml dagtest job -- tests/dagtest wired into continuous CI for the first time since Phase 8"
  - "Makefile rollback SHA=<sha> target (D-12) -- redeploys all three workloads at a prior, already-published GHCR SHA, live-proven against the real cluster"
  - "A real, previously-latent tests/dagtest cross-file collection-order bug, fixed in conftest.py's airflow_env fixture"
  - "A real Kyverno ImageValidatingPolicy webhook-timeout bug (10s default too short for live cosign verification), fixed by raising to the CRD's own 30s maximum"
affects: [11-final-verification]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "rollback's recipe calls helm upgrade --install directly (not the helm_install() wrapper), since the wrapper has no ad hoc --set forwarding as of this plan -- mirrors the exact --wait=hookOnly + scripts/wait-for.sh sequence 70-airflow.sh already uses for the same chart"
    - "airflow_env's fixture now defensively re-binds airflow.settings (configure_vars/dispose_orm/configure_orm) after setting env vars, rather than trusting whichever DSN happened to be in effect the first time airflow.settings imported in-process -- needed because sibling test modules' own module-level _common imports can trigger settings.initialize() before any fixture runs"

key-files:
  created:
    - tests/policy/test_ci_workflow_coverage.py
  modified:
    - .github/workflows/ci.yml
    - Makefile
    - tests/dagtest/conftest.py
    - kubernetes/kyverno-policy.yaml

key-decisions:
  - "[Rule 1/3] tests/dagtest/conftest.py's airflow_env fixture forces airflow.settings to (re)bind against the freshly-set metadata DSN after setting env vars -- test_gap_recorder.py/test_run_stage_recorder.py's own module-level _common imports transitively trigger airflow.settings.initialize() at pytest COLLECTION time, before any fixture runs, permanently binding Session to the sqlite default. Reproduced live only when the full tests/dagtest suite ran together -- the first time this Makefile target (or any command) ever did that."
  - "[Rule 3] Makefile's rollback recipe added `set -e` after sourcing helm/versions.env -- without it, a failed helm upgrade did not fail the target, and the subsequent wait_for_* calls trivially 'succeeded' against the OLD, unchanged Deployment, silently reporting success without ever changing the deployed image. Found live during Task 3's own proof."
  - "[Rule 3] kubernetes/kyverno-policy.yaml's require-signed-images ImageValidatingPolicy webhookConfiguration.timeoutSeconds raised from the CRD's 10s default to 30s (its own documented maximum) -- cosign verification against the real ghcr.io registry for the Airflow image consistently took 15-20s under live cluster load, exceeding the deadline and failing the Deployment/StatefulSet UPDATE with 'context canceled'. Not a security weakening: verification is still mandatory (validationActions stays [Deny], failurePolicy stays Fail), only enough wall-clock time for a legitimate, correctly-signed verification to complete."
  - "[Rule 2] tests/policy/test_ci_workflow_coverage.py added even though no task's action text named it -- the plan's own frontmatter files_modified list declared it, but the gap between that declaration and the task list was never closed. Mirrors test_ci_calls_make_ci.py's established mutation-based non-vacuity shape."
  - "Live cluster restored to its pre-rollback baseline (localhost:5001 local-registry images) immediately after Task 3's live proof completed, since the cluster is shared with other concurrent phase-11 work."

requirements-completed: [CICD-05, CICD-06]

# Metrics
duration: ~55min
completed: 2026-08-23
---

# Phase 11 Plan 06: CI Coverage Reporting, dagtest Wiring & Live-Proven Rollback Summary

**Coverage reported via $GITHUB_STEP_SUMMARY + an htmlcov/ artifact (no fail_under gate), tests/dagtest wired into a new CI job for the first time since Phase 8, and a `make rollback SHA=<sha>` target proven live against the real cluster -- which surfaced and fixed two genuine platform bugs (a cross-file pytest collection-order defect in tests/dagtest, and a too-short Kyverno webhook timeout) along the way.**

## Performance

- **Duration:** ~55 min active execution, including live debugging of two real platform bugs surfaced by Task 3's own live proof
- **Tasks:** 3/3 complete, plus one Rule 2 addition (a policy test file the plan's own frontmatter declared but no task wrote)
- **Files modified:** 5 (1 created, 4 modified)

## Accomplishments

- `ci.yml`'s `check` job gained a coverage-report step (`coverage html` + `coverage report --format=markdown >> $GITHUB_STEP_SUMMARY`) and an `actions/upload-artifact@v7.0.1` step for `htmlcov/`, with zero `fail_under`/exit-code gate anywhere (D-23).
- `Makefile` gained `test-dagtest` (`$(RUN_CLUSTER)`, since `tests/dagtest/test_run_stage_recorder.py`/`test_gap_recorder.py` both `import psycopg` directly); `ci.yml` gained a `dagtest` job — `tests/dagtest` has existed since Phase 8 but was never exercised continuously until now.
- **Found and fixed a real, previously-latent bug** while making `make test-dagtest` pass for the first time as a full suite: `test_gap_recorder.py`/`test_run_stage_recorder.py`'s own module-level `from _common import ...` imports transitively trigger `airflow.settings.initialize()` at pytest collection time — before `conftest.py`'s `airflow_env` fixture ever runs — permanently binding `airflow.settings.Session` to the default sqlite DSN regardless of any env var set afterward. `test_backfill_dagrun.py`/`test_platform_retention_dagrun.py` failed with `sqlite3.OperationalError: no such table: task_instance` only when the full suite ran together (never when either failing file ran alone). Fixed by forcing `configure_vars()`/`dispose_orm()`/`configure_orm()` in `airflow_env`'s own fixture body, after setting env vars. All 14 dagtest tests pass together now.
- `Makefile` gained `rollback SHA=<sha>` (D-12): required `SHA`, guarded behind the same live-cluster reachability probe `image-csv-processor` uses, resolves the GHCR owner from `git remote get-url origin`, points `csv_processor_image`/`dbt_image` Airflow Variables and the Airflow chart's own `defaultAirflowRepository`/`defaultAirflowTag` at `ghcr.io/<owner>/{csv-processor,dbt,airflow}:<SHA>`, calls `helm upgrade --install` directly with `--wait=hookOnly` (never `watcher`, the documented Phase-2 deadlock), then waits on the same four workloads `70-airflow.sh` waits on.
- **Task 3's live proof against real, already-published images** (`d5a3ec08f575438f91d98abca4f16a853e82a2f5`, the exact SHA plan 11-02's own SUMMARY documents all three images being published together at) surfaced and fixed two genuine blockers on the first attempt:
  1. `rollback`'s recipe lacked `set -e` — a failed `helm upgrade` did not fail the target, and the wait calls trivially "succeeded" against the unchanged old Deployment.
  2. `kubernetes/kyverno-policy.yaml`'s `require-signed-images` policy used the CRD's 10s default webhook timeout; live cosign verification against `ghcr.io` for the Airflow image took 15-20s under this session's cluster load, exceeding the deadline (`context canceled`, `write: broken pipe` in Kyverno's own admission-controller logs). Raised to 30s, the CRD's own documented maximum.
  With both fixed, the live proof succeeded end-to-end and was independently confirmed via fresh `kubectl`/`airflow variables get` calls (not the target's own echoed output) — see Live Proof Evidence below.

## Task Commits

1. **Task 1: Coverage job-summary + artifact in ci.yml, and a dagtest CI job** — `0b5fe4e` (feat) — includes the Rule 1/3 `tests/dagtest/conftest.py` fix, discovered while making `make test-dagtest` pass for the first time
2. **Task 2: rollback Make target** — `a778934` (feat)
3. **Task 3: Live rollback proof against real, already-published images** — `4bb0778` (fix) — the `set -e` and Kyverno webhook-timeout fixes this proof surfaced
4. **Rule 2 addition: proof-over-prose test for this plan's own CI claims** — `37fffb2` (test)

**Plan metadata:** this SUMMARY's own commit (see below).

## Files Created/Modified

- `.github/workflows/ci.yml` — coverage-report/artifact steps in `check`; new `dagtest` job
- `Makefile` — `test-dagtest`, `rollback SHA=<sha>` targets
- `tests/dagtest/conftest.py` — `airflow_env` fixture forces `airflow.settings` rebind after setting env vars
- `kubernetes/kyverno-policy.yaml` — `webhookConfiguration.timeoutSeconds: 30` on `require-signed-images`
- `tests/policy/test_ci_workflow_coverage.py` — new, proof-over-prose coverage for this plan's own CI wiring claims

## Decisions Made

See `key-decisions` in frontmatter. Summary: two real platform bugs (tests/dagtest collection-order, Kyverno webhook timeout) were found live and fixed under Rule 1/3 rather than deferred, because they directly blocked this plan's own required verification steps (`make test-dagtest` passing; Task 3's live rollback proof). Both fixes are narrow, targeted, and do not weaken any existing guarantee — the dagtest fix makes an existing fixture correct rather than accidentally-working; the Kyverno fix raises a timeout to its own schema-documented maximum without touching `validationActions`/`failurePolicy`.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1/3 — Bug/Blocking] tests/dagtest cross-file collection-order bug**
- **Found during:** Task 1's own verification (`make test-dagtest`)
- **Issue:** `test_gap_recorder.py`/`test_run_stage_recorder.py` import `_common.gap_recorder`/`_common.run_stage_recorder` at module level, which import `airflow.sdk.bases.hook.BaseHook`, which transitively triggers `airflow.settings.initialize()` at pytest collection time — before `conftest.py`'s `airflow_env` fixture (which sets `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`) ever runs. `airflow.settings.Session` binds to the default sqlite DSN permanently; a later env-var change does not rebind it.
- **Fix:** `airflow_env` fixture now calls `airflow.settings.configure_vars()` / `dispose_orm()` / `configure_orm()` explicitly after setting env vars, forcing a correct rebind regardless of any earlier premature import. Also disposes the ORM pool in the fixture's own teardown, before the testcontainers Postgres container tears down (eliminating a separate, benign `atexit` teardown warning observed live).
- **Files modified:** `tests/dagtest/conftest.py`
- **Verification:** `uv run --group cluster pytest tests/dagtest -q` — 14 passed (was 4 failed/10 passed before the fix, reproduced deterministically across repeated runs and multiple file-pair combinations)
- **Committed in:** `0b5fe4e`

**2. [Rule 3 — Blocking] rollback recipe silently "succeeded" on a failed helm upgrade**
- **Found during:** Task 3's own live proof
- **Issue:** No `set -e` in the recipe's shell body; a failed `helm upgrade --install` (see item 3) did not stop the recipe, and the subsequent `wait_for_deploy_available` calls trivially passed against the OLD, unchanged Deployment (already `Available` from before), making `rollback` exit 0 without the image ever having changed.
- **Fix:** Added `set -e;` immediately after sourcing `helm/versions.env`.
- **Files modified:** `Makefile`
- **Verification:** Re-ran the guard-clause and `--wait=hookOnly`/no-`watcher` automated checks; full live proof re-run succeeded genuinely (helm upgrade exit 0, image confirmed changed)
- **Committed in:** `4bb0778`

**3. [Rule 3 — Blocking] Kyverno's require-signed-images webhook timeout too short for live cosign verification**
- **Found during:** Task 3's own live proof (first attempt)
- **Issue:** `ImageValidatingPolicy/require-signed-images`'s generated mutating webhook (`ivpol.mutate.kyverno.svc-fail`) used the CRD's own 10s default `timeoutSeconds`. Live cosign signature verification against `ghcr.io` for `ghcr.io/konutech/airflow:d5a3ec0...` consistently took 15-20s under this session's cluster load (confirmed via `kyverno-admission-controller`'s own logs: repeated "verifying cosign image signature" TRC lines followed by `Get "https://ghcr.io/v2/": context canceled` and `write: broken pipe` at exactly the 10s mark). The `helm upgrade` failed outright for the Deployment/StatefulSet UPDATE (Kyverno's own pod-policy autogen extends the pod-scoped rule to controller resources).
- **Fix:** Raised `spec.webhookConfiguration.timeoutSeconds` to `30` — the CRD schema's own documented maximum ("the value must be between 1 and 30 seconds"). `validationActions: [Deny]` and `failurePolicy: Fail` are unchanged; verification remains mandatory.
- **Files modified:** `kubernetes/kyverno-policy.yaml`
- **Verification:** `kubectl apply -f kubernetes/kyverno-policy.yaml`; confirmed the live `mutatingwebhookconfiguration`'s own `timeoutSeconds` picked up `30`; re-ran the live rollback and it succeeded with zero Kyverno-related denials/timeouts in the admission-controller log for the entire retry window
- **Committed in:** `4bb0778`

**4. [Rule 2 — Missing critical functionality] tests/policy/test_ci_workflow_coverage.py**
- **Found during:** Final review before writing this SUMMARY
- **Issue:** This plan's own frontmatter `files_modified` list declares `tests/policy/test_ci_workflow_coverage.py`, but no task's action text describes writing it — the plan's own must_haves (coverage-summary steps present, dagtest job present and wired) were otherwise asserted only in prose (the plan's `<verification>` section, this SUMMARY), never proven by a committed, CI-collected test.
- **Fix:** Added the file, mirroring `test_ci_calls_make_ci.py`'s established mutation-based non-vacuity shape: parses the real `ci.yml`/`pyproject.toml`, asserts the real claim, then feeds a mutated copy through the same predicate to prove the check is sensitive to regression.
- **Files modified:** `tests/policy/test_ci_workflow_coverage.py` (new)
- **Verification:** `uv run pytest tests/policy/test_ci_workflow_coverage.py -v` — 6 passed; full `tests/policy` suite re-run — 157 passed (up from 151), same 2 pre-existing unrelated failures (see below)
- **Committed in:** `37fffb2`

---

**Total deviations:** 4 auto-fixed (2 Rule 1/3 blocking bugs found via this plan's own live verification, 1 Rule 3 blocking config fix, 1 Rule 2 missing test coverage)
**Impact on plan:** All four were necessary either to make this plan's own declared verification steps pass (`make test-dagtest`; Task 3's live proof) or to close a genuine gap between the plan's frontmatter and its task list. No scope creep beyond what each blocking issue required.

## Pre-existing, unrelated test failures (not touched)

`tests/policy/test_dag_line_budget.py::test_csv_ingest_customers_stays_under_150_lines` and `tests/policy/test_gates_actually_fail.py::test_the_main_gate_does_not_lint_the_bad_samples` fail on every run in this repository state, confirmed pre-existing on the base commit and already documented in `.planning/phases/11-ci-cd-completion-operations/deferred-items.md`'s "Plan 11-01" section (traces to Phase 9/10 DAG/test-corpus work, entirely outside this plan's file scope). Re-confirmed unaffected by this plan's own changes via `git stash -u` + re-run on the unmodified tree.

## Live Proof Evidence (Task 3)

Live cluster: `kind-airflow-platform` (3 nodes, confirmed `Ready`). Target SHA: `d5a3ec08f575438f91d98abca4f16a853e82a2f5` (the real merge commit plan 11-02's own SUMMARY documents all three images — csv-processor, dbt, airflow — being published together at, owner `konutech`).

Pre-rollback baseline (captured before the proof):
- `csv_processor_image` = `localhost:5001/csv-processor:917e45c`
- `dbt_image` = `localhost:5001/dbt:46da94a`
- `airflow-scheduler` image = `localhost:5001/airflow:9fa4531`

`make rollback SHA=d5a3ec08f575438f91d98abca4f16a853e82a2f5` — exit 0. Independent readback (fresh `kubectl`/`airflow variables get` calls, not the target's own echoed output):

```
$ kubectl --context kind-airflow-platform exec -n airflow deploy/airflow-api-server -- airflow variables get csv_processor_image
ghcr.io/konutech/csv-processor:d5a3ec08f575438f91d98abca4f16a853e82a2f5

$ kubectl --context kind-airflow-platform exec -n airflow deploy/airflow-api-server -- airflow variables get dbt_image
ghcr.io/konutech/dbt:d5a3ec08f575438f91d98abca4f16a853e82a2f5

$ kubectl --context kind-airflow-platform get deployment -n airflow airflow-scheduler -o jsonpath='{.spec.template.spec.containers[0].image}'
ghcr.io/konutech/airflow:d5a3ec08f575438f91d98abca4f16a853e82a2f5

$ kubectl --context kind-airflow-platform -n airflow get pods -l component=scheduler -o jsonpath='{.items[0].spec.containers[0].image}'
ghcr.io/konutech/airflow:d5a3ec08f575438f91d98abca4f16a853e82a2f5   # the live running pod, not just the Deployment spec
```

All three exactly match `ghcr.io/konutech/{csv-processor,dbt,airflow}:d5a3ec08f575438f91d98abca4f16a853e82a2f5`, and the scheduler pod (2/2 Ready) is genuinely running the rolled-back image, not merely specced to.

**Cluster restored afterward** (this is a shared cluster used by other concurrent phase-11 work): re-ran the same Variable-set + `helm upgrade` sequence pointed back at the pre-rollback `localhost:5001` images; independently re-confirmed all three readbacks match the original baseline exactly.

## Issues Encountered

Both fully resolved — see Deviations above. No unresolved issues.

## User Setup Required

None — no external service configuration required.

## Next Phase Readiness

- CICD-05 (coverage reporting) and CICD-06 (this plan's share: `tests/dagtest` continuous coverage + the D-12 rollback mechanism) are both complete and live-proven.
- `make rollback SHA=<sha>` is ready for real operational use — the exact runbook procedure is documented inline in the Makefile recipe itself (Claude's Discretion, per CONTEXT.md's own grant: no separate `docs/runbooks/` file).
- The Kyverno webhook-timeout fix (10s → 30s) is a durable platform improvement beyond this plan's own narrow need — any future `helm upgrade`/`kubectl apply` touching a GHCR-published, cosign-verified image under similar cluster load benefits from it, not just `rollback`.
- No blockers for the phase's remaining plans.

---
*Phase: 11-ci-cd-completion-operations*
*Completed: 2026-08-23*

## Self-Check: PASSED

All files confirmed present on disk; all 4 task/deviation commit hashes confirmed in git log.
