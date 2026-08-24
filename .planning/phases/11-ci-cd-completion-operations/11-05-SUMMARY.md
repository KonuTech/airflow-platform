---
phase: 11-ci-cd-completion-operations
plan: 05
subsystem: cicd
tags: [github-actions, e2e, chaos-testing, kind, ephemeral-cluster, gh-issue, cicd-09, qual-15]

# Dependency graph
requires:
  - phase: 11-04
    provides: "kind/cluster-ci.yaml CI-portable single-node kind topology, AIRFLOW_IMAGE_OVERRIDE_*/ci-set-workload-images.sh cluster-up override mechanism, scripts/doctor.sh CI overrides"
  - phase: 11-09
    provides: "tests/e2e/chaos scaffolding + test_pod_crash.py/test_database_unavailable.py/test_minio_unavailable.py/test_vault_unavailable.py"
  - phase: 11-10
    provides: "tests/e2e/chaos/test_malformed_csv.py/test_invalid_encoding.py/test_duplicate_batch.py/test_oom.py/test_task_timeout.py"
  - phase: 11-12
    provides: "scripts/rebuild-from-raw.py + make rebuild-from-raw target, live-proven port-forward/role-idempotency fixes"
provides:
  - ".github/workflows/e2e-full.yml — merge-triggered full local E2E suite + rebuild-from-raw capstone, written and policy-clean, NOT live-verified this session"
  - ".github/workflows/e2e-chaos.yml — merge-triggered, dedicated-cluster QUAL-15 chaos suite, written and policy-clean, NOT live-verified this session"
  - "make chaos-verify — new Makefile target routing e2e-chaos.yml through make (CICD-02), covering all 11 QUAL-15 scenarios"
  - "Two documented, structural blockers to Task 3's own live-proof: worktree-isolated executors cannot push to main, and tests/e2e/observability conflicts with the CI profile's monitoring-disabled design"
affects: ["the orchestrator's post-merge flow (must watch the first real push-to-main run of both new workflows)", "a future follow-up session that resolves the observability/CI-monitoring conflict and the still-open kind/cluster-ci.yaml capacity ceiling"]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "A merge-triggered workflow's own D-22 failure-notification step needs its own (workflow, job) entry in tests/policy/test_workflow_secrets.py's ALLOWED_PERMISSION_WIDENING allowlist — issues: write is a real widening even though it is scoped to an if: failure() step"
    - "CICD-02 (no workflow invokes pytest directly) applies even to a suite invoked only through a bootstrap-heavy CI workflow — route through a new, purpose-scoped make target (mirroring cluster-verify/vault-verify/smoke-verify's own established pattern) rather than inlining the pytest call"

key-files:
  created:
    - .github/workflows/e2e-full.yml
    - .github/workflows/e2e-chaos.yml
  modified:
    - Makefile
    - tests/policy/test_offline_gate_stays_offline.py
    - tests/policy/test_workflow_secrets.py
    - .planning/phases/11-ci-cd-completion-operations/deferred-items.md

key-decisions:
  - "e2e-full.yml calls `make cluster-verify` by name, unmodified, per this plan's own interfaces contract (\"do not re-list its component directories inline\") -- even though this session found, while writing the workflow, that tests/e2e/observability's own live monitoring-stack dependency conflicts with the CI profile's unconditional monitoring-disabled skip (plan 11-04's own CPU-necessitated fix). Documented as a known, live-untested risk in deferred-items.md rather than silently narrowing the invocation to dodge it -- narrowing cluster-verify's own definition is a Rule-4 architectural decision outside this plan's authority."
  - "Both workflows tag AIRFLOW_IMAGE_OVERRIDE_TAG/ci-set-workload-images.sh at ${{ github.sha }} (full SHA), not a pr-<number> tag -- confirmed by reading publish.yml's own tag-selection step, which tags every image with the full github.sha on a push-to-main trigger (the exact event these two new workflows also trigger on)."
  - "Added a new `make chaos-verify` target (Rule 3 fix, not pre-planned) so e2e-chaos.yml could satisfy CICD-02 (test_ci_invokes_make_only.py forbids a workflow calling pytest directly) -- mirrors cluster-verify/vault-verify/smoke-verify's own established make-delegation convention exactly; registered in test_offline_gate_stays_offline.py's ARGUED_TESTS_E2E_TARGETS with its own written justification."
  - "Did NOT attempt Task 3's own literal instruction (push to main from this session) -- this plan was dispatched as a worktree-isolated wave executor whose branch the orchestrator merges into main afterward; pushing to main directly from inside the worktree would race that merge and is outside a worktree-isolated executor's authority. Documented as a structural gap in deferred-items.md with a recommended next step for whoever watches the first post-merge push."

requirements-completed: []

# Metrics
duration: ~90min
completed: 2026-08-24
---

# Phase 11 Plan 05: Merge-Triggered Full E2E Suite + Dedicated Chaos Cluster Summary

**Wrote and policy-verified `.github/workflows/e2e-full.yml` (full local suite + rebuild-from-raw capstone) and `.github/workflows/e2e-chaos.yml` (all 11 QUAL-15 chaos scenarios on a dedicated parallel cluster), both merge-triggered with an idempotent D-22 failure-issue step — but could not perform Task 3's own live-verification-on-a-real-merge-to-main step, since that requires pushing to `main`, which is outside a worktree-isolated wave executor's authority (the orchestrator's own post-merge flow owns that). Also found and documented a genuine, pre-existing structural conflict: `tests/e2e/observability` needs a live monitoring stack the CI profile unconditionally disables.**

## Performance

- **Duration:** ~90 min
- **Started:** 2026-08-24
- **Completed:** 2026-08-24
- **Tasks:** 3 planned; Task 1 and Task 2 complete and committed; Task 3 could not be executed this session (see Deviations/Issues)
- **Files modified:** 6 (2 created, 4 modified)

## Accomplishments

- `.github/workflows/e2e-full.yml`: merge-triggered (`push: branches: [main]`), single job `e2e-full`, `timeout-minutes: 120`, job-level `permissions: {contents: read, issues: write}`. Mirrors `e2e-smoke.yml`'s CI-portable bootstrap sequence (checkout, setup-uv, `make install-cluster`, optional Docker Hub login, `AIRFLOW_IMAGE_OVERRIDE_*` pinned to `github.sha`, `PROFILE=ci make cluster-up` with `doctor.sh`'s CI overrides, `scripts/ci-set-workload-images.sh ${{ github.sha }}`, `make migrate-analytics` before Vault bootstrap, a placeholder Grafana webhook, `make vault-unseal`/`make vault-bootstrap`), then runs the unchanged full local suite (`make cluster-verify`) strictly before `make rebuild-from-raw` on the same populated cluster (D-24/D-30 — no data-seeding step between them, structurally proven by the plan's own automated step-order check), then a D-22 idempotent `gh issue create`/`gh issue comment` failure step titled "E2E full suite failure on main".
- `.github/workflows/e2e-chaos.yml`: identical merge trigger, its own wholly independent job `chaos` with its own from-scratch `PROFILE=ci make cluster-up` (no `needs:` coupling to `e2e-full.yml` — D-25/D-27, GitHub Actions runs the two workflow files' jobs concurrently on separate runners by default), `timeout-minutes: 90`, then `make chaos-verify` (new target, see below) covering all 11 QUAL-15 scenarios (`tests/e2e/chaos`'s 9 files + `tests/e2e/vault`'s 2 already-proven scenarios, both `pytest.mark.cluster`), then its own D-22 failure step titled "Chaos suite failure on main" (a distinct title prefix from `e2e-full.yml`'s own, so a full-suite failure and a chaos failure never collide into one tracked issue).
- New `make chaos-verify` Makefile target: found live (via the pre-existing, already-enforced `tests/policy/test_ci_invokes_make_only.py`) that a workflow may never invoke `pytest` directly (CICD-02) — added this target mirroring `cluster-verify`/`vault-verify`/`smoke-verify`'s own established make-delegation convention, and registered it in `test_offline_gate_stays_offline.py`'s `ARGUED_TESTS_E2E_TARGETS` with its own written justification.
- Registered both new (workflow, job) pins — `("e2e-full.yml", "e2e-full")` and `("e2e-chaos.yml", "chaos")` — in `test_workflow_secrets.py`'s `ALLOWED_PERMISSION_WIDENING`, each with the exact `{contents: read, issues: write}` permission set D-22's failure step needs.
- Both workflow files independently structure-verified: YAML parses, `make cluster-verify` appears strictly before `make rebuild-from-raw` in `e2e-full.yml`'s own step list, `e2e-chaos.yml`'s `chaos` job carries no `needs:` key and its `make chaos-verify` invocation is backed by a Makefile target that literally names both `tests/e2e/chaos` and `tests/e2e/vault`.
- Full `tests/policy -q -m "not manifests"` suite re-run after all changes: 157 passed, only the 2 pre-existing, already-documented (Plan 11-01, base-commit) failures remain (`test_dag_line_budget.py`, `test_gates_actually_fail.py`) — zero new regressions from this plan's own changes.
- Documented two genuine, structural findings in `deferred-items.md`'s new "Plan 11-05" section: (1) Task 3's own live-verification step cannot be performed by a worktree-isolated wave executor — only the orchestrator's post-merge push to `main` can trigger either workflow for the first time; (2) `tests/e2e/observability` (part of the unchanged `make cluster-verify` this plan's own interfaces contract requires calling by name) needs a live Prometheus/Grafana/Tempo stack the CI profile unconditionally disables (plan 11-04's own CPU-necessitated fix, no override mechanism) — `e2e-full.yml`'s first real run is very likely to fail on this specific sub-suite for a reason entirely outside this plan's own file scope.

## Task Commits

1. **Task 1: e2e-full.yml** — `a0de3f3` (feat)
2. **Task 2: e2e-chaos.yml** — `5765842` (feat)
3. **Rule 1/3 fix: route chaos suite through make + register policy allowlists** — `2c136f4` (fix)
4. **Documentation: Task 3 gap + observability/CI-monitoring conflict** — `66d1b69` (docs)

**Plan metadata:** (this commit, immediately following)

## Files Created/Modified

- `.github/workflows/e2e-full.yml` (new) — merge-triggered full E2E suite + rebuild-from-raw capstone
- `.github/workflows/e2e-chaos.yml` (new) — merge-triggered, dedicated-cluster QUAL-15 chaos suite
- `Makefile` — new `chaos-verify` target (Rule 3, CICD-02 compliance)
- `tests/policy/test_offline_gate_stays_offline.py` — registered `chaos-verify` in `ARGUED_TESTS_E2E_TARGETS`
- `tests/policy/test_workflow_secrets.py` — registered both new (workflow, job) `issues: write` pins in `ALLOWED_PERMISSION_WIDENING`
- `.planning/phases/11-ci-cd-completion-operations/deferred-items.md` — new "Plan 11-05" section (Task 3 gap, observability/CI-monitoring conflict, both cross-referenced against plan 11-04's own still-open CI-portability finding)

## Decisions Made

See `key-decisions` in the frontmatter above for the four decisions with rationale: calling `cluster-verify` unmodified despite the known observability conflict, tagging both workflows' image overrides at `github.sha` (not `pr-<number>`, confirmed by reading `publish.yml`'s own tag-selection logic), adding `make chaos-verify` to satisfy CICD-02, and not attempting to push to `main` from inside this worktree.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] `e2e-chaos.yml` could not call `pytest` directly — CICD-02's `test_ci_invokes_make_only.py` forbids it**
- **Found during:** Running `tests/policy` against the two new workflow files after Task 2
- **Issue:** The plan's own action text specified `pytest tests/e2e/chaos tests/e2e/vault -q -m cluster` as a `run:` step, but this repository's own pre-existing, already-enforced policy (`DIRECT_TOOLS` regex matching `pytest`) forbids any workflow step from invoking a gate tool directly — every other live-cluster suite (`cluster-verify`, `vault-verify`, `smoke-verify`) already goes through `make`.
- **Fix:** Added a new `make chaos-verify` target with the identical pytest invocation, and pointed `e2e-chaos.yml` at `make chaos-verify` instead.
- **Files modified:** `Makefile`, `.github/workflows/e2e-chaos.yml`
- **Verification:** `tests/policy -q -k "workflow or ci_invokes or offline_gate"` — 55 passed (was 2 failed before the fix).
- **Committed in:** `2c136f4`

**2. [Rule 3 - Blocking] Both new workflows' `issues: write` permission needed an `ALLOWED_PERMISSION_WIDENING` entry**
- **Found during:** Same policy run as above
- **Issue:** `test_the_workflow_token_stays_read_only` asserts every job's permissions are least-privilege unless explicitly, individually allowlisted with its EXACT permission set — D-22's failure-notification step's `issues: write` (job-level, since Actions has no narrower per-step permissions grant) is a real widening this pre-existing test correctly caught.
- **Fix:** Added `("e2e-full.yml", "e2e-full")` and `("e2e-chaos.yml", "chaos")` entries, each with `{contents: read, issues: write}`.
- **Files modified:** `tests/policy/test_workflow_secrets.py`
- **Verification:** Same policy run as fix #1, plus a full `tests/policy -q -m "not manifests"` re-run (157 passed, only the 2 pre-existing Plan-11-01 failures remain).
- **Committed in:** `2c136f4`

**3. [Rule 3 - Blocking, adjacent] `make chaos-verify` needed registering in `ARGUED_TESTS_E2E_TARGETS`**
- **Found during:** Same policy run
- **Issue:** `test_offline_gate_stays_offline.py`'s own non-vacuity check requires every live-cluster-verify Makefile target to carry a written justification for existing as a separate target — an unargued new target is exactly what that test exists to catch.
- **Fix:** Added the `chaos-verify` entry with a written justification mirroring `cluster-verify`/`vault-verify`/`smoke-verify`'s own existing entries.
- **Files modified:** `tests/policy/test_offline_gate_stays_offline.py`
- **Verification:** Same policy run as fix #1.
- **Committed in:** `2c136f4`

---

**Total deviations:** 3 auto-fixed (all Rule 3, all pre-existing project-wide policy gates this plan's own new files had to satisfy — no scope creep, no architectural change). **Not auto-fixed, deliberately deferred (Rule 4 territory, documented not attempted):** the observability/CI-monitoring conflict and Task 3's own structural gap — see "Issues Encountered" below.

## Issues Encountered

**Task 3 (live-verify on a real merge to main) could not be executed this session.** Both new
workflows trigger `on: push: branches: [main]` only — there is no `pull_request` path to exercise
them (unlike `e2e-smoke.yml`'s throwaway-PR proof pattern), so genuinely observing either run
requires a real push to `main`. This session was dispatched as a worktree-isolated wave executor;
the orchestrator merges this worktree's branch into `main` after this wave completes, and pushing
to `main` directly from inside the worktree would race that merge — explicitly outside a
worktree-isolated executor's authority. Full reasoning and a recommended next step (watch the
first real post-merge push) are in `deferred-items.md`'s new "Plan 11-05" section.

**A genuine, structural conflict was found while designing `e2e-full.yml`, not fixed:**
`tests/e2e/observability` (part of `make cluster-verify`, which this plan's own interfaces
contract requires calling unmodified) needs a live Prometheus/Grafana/Tempo stack. The CI profile
(`scripts/stages/85-monitoring.sh`) unconditionally skips monitoring under `PROFILE=ci` — a real,
deliberate, already-committed fix from plan 11-04's own CI-portability follow-up, needed to keep
the single CI node under its own CPU ceiling, with no override mechanism. `e2e-full.yml`'s first
real run is very likely to fail on this specific sub-suite the first time it runs in CI, for a
reason entirely outside anything this plan's own files control. Two Rule-4 remediation options are
recorded in `deferred-items.md` for a future session — not attempted here, since resolving it means
editing `scripts/stages/85-monitoring.sh`/`kind/cluster-ci.yaml`/`helm/values/ci/*.yaml`, all
outside this plan's declared `files_modified`.

**Carried forward from plan 11-04 (not re-diagnosed, per this session's own dispatch instructions):**
the last observed CI-portability blocker — `airflow dags trigger` timing out at its own 120s
ceiling under real GitHub Actions runner contention — remains open. `e2e-full.yml` does not reuse
that exact hardcoded loop (it calls `cluster-verify`/`rebuild-from-raw`, not `smoke-verify`), but
runs a materially heavier suite on the identical single-node CI topology, so the same underlying
capacity ceiling is plausible in some form. Flagged as a known risk, not re-investigated, per this
session's own explicit instruction not to repeat already-exhausted diagnostic approaches.

## Known Stubs

None — no data-flow stubs introduced. Both new files are CI workflow definitions, not application
code.

## User Setup Required

None new. Both workflows reuse `e2e-smoke.yml`'s already-documented optional `DOCKERHUB_USERNAME`/
`DOCKERHUB_TOKEN` repository secrets (D-21's graceful-degradation path); no new secret or manual
step is introduced.

## Next Phase Readiness

- `.github/workflows/e2e-full.yml` and `.github/workflows/e2e-chaos.yml` are both written, locally
  structure-verified, and policy-clean — ready to run the moment this worktree's branch is merged
  and the orchestrator (or a subsequent push) triggers them on `main`.
- **CICD-09 and QUAL-15 are NOT marked complete this session** (`requirements-completed` is
  deliberately empty in this SUMMARY's frontmatter) — Task 3's own live-proof requirement has not
  been observed. The orchestrator (or whoever performs the next push to `main`) should watch the
  first real run of both workflows and treat that as this plan's own outstanding acceptance
  criterion, budgeting for the two known risks documented above (the CI-portability capacity
  ceiling and the observability/CI-monitoring conflict) rather than expecting a clean green run on
  the first attempt.
- If `e2e-full.yml`'s first run fails specifically on `tests/e2e/observability`, that is the
  documented, anticipated conflict above — the fix belongs to a dedicated follow-up session
  choosing between this SUMMARY's two Rule-4 remediation options, not a re-diagnosis from scratch.

---
*Phase: 11-ci-cd-completion-operations*
*Completed: 2026-08-24 (Tasks 1-2 fully written and policy-verified; Task 3 not executable from a worktree-isolated wave — deferred to the orchestrator's post-merge flow)*

## Self-Check: PASSED

- FOUND: `.github/workflows/e2e-full.yml`
- FOUND: `.github/workflows/e2e-chaos.yml`
- FOUND: `Makefile` (chaos-verify target present)
- FOUND: `tests/policy/test_offline_gate_stays_offline.py` (chaos-verify entry present)
- FOUND: `tests/policy/test_workflow_secrets.py` (both new allowlist entries present)
- FOUND: `.planning/phases/11-ci-cd-completion-operations/deferred-items.md` ("## Plan 11-05" section)
- FOUND commit: `a0de3f3` (Task 1)
- FOUND commit: `5765842` (Task 2)
- FOUND commit: `2c136f4` (Rule 3 policy fixes)
- FOUND commit: `66d1b69` (docs)

No missing items.
