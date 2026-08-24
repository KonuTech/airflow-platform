---
phase: quick-260824-ayw-build-a-trimmed-ci-only-monitoring-stack
plan: 01
subsystem: infra
tags: [helm, kube-prometheus-stack, otel-collector, tempo, ci-cd, github-actions, makefile]

# Dependency graph
requires:
  - phase: 11-ci-cd-completion-operations
    provides: helm/values/ci/{monitoring,tempo,otel-collector}.yaml (already-trimmed CI values from quick task 260817-rvq), .github/workflows/e2e-full.yml (D-19/D-24/D-30 merge-triggered E2E job), Makefile's cluster-verify/rollback/migrate-analytics precedent shapes
provides:
  - scripts/monitoring-install.sh (extracted, shared helm_install/wait_for_* monitoring install logic)
  - scripts/monitoring-teardown.sh (fail-closed helm uninstall of the three monitoring releases)
  - Makefile cluster-slice-verify and observability-verify-ci targets
  - helm/values/ci/monitoring.yaml with kubeStateMetrics/nodeExporter disabled
  - .github/workflows/e2e-full.yml staggered monitoring install around the tests/e2e/observability window
affects: [ci-cd-completion-operations, observability]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Shared install/teardown scripts sourced by both a local persistent-cluster stage script and a CI-only staggered Makefile target, so the two paths cannot silently diverge in helm_install/wait_for_* call shape"
    - "set -e as its own explicit recipe line (mirroring Makefile's rollback target) for a multi-command backslash-continued Makefile recipe, so an intermediate command's failure is not silently swallowed by the final command's own exit status"

key-files:
  created:
    - scripts/monitoring-install.sh
    - scripts/monitoring-teardown.sh
  modified:
    - scripts/stages/85-monitoring.sh
    - Makefile
    - helm/values/ci/monitoring.yaml
    - tests/policy/test_values_profiles.py
    - .github/workflows/e2e-full.yml

key-decisions:
  - "Stagger the CI monitoring stack's live window to only tests/e2e/observability, not the whole ~120-minute e2e-full.yml job (CONTEXT.md-locked decision), by splitting the old single `make cluster-verify` step into cluster-slice-verify + observability-verify-ci."
  - "Disable kubeStateMetrics/nodeExporter in the CI monitoring values (verified this session: no observability test references either) for a free CPU/pod-count saving with zero test-coverage loss."
  - "observability-verify-ci's recipe uses an explicit `set -e;` line (mirroring rollback's shape) so a failing pytest run genuinely fails the make target instead of being masked by monitoring-teardown.sh's own exit status."

patterns-established:
  - "A CI-workflow-specific narrower Makefile target (cluster-slice-verify) sits alongside the shared cluster-verify target without modifying it, matching smoke-verify's precedent for disclosed, CI-scoped narrowing."

requirements-completed: []

# Metrics
duration: ~35min
completed: 2026-08-24
---

# Quick Task 260824-ayw: Build a trimmed CI-only monitoring stack Summary

**Staggered CI-only monitoring install (otel-collector/tempo/kube-prometheus-stack, kubeStateMetrics+nodeExporter disabled) around `tests/e2e/observability`'s own window in `e2e-full.yml`, via new `scripts/monitoring-install.sh`/`monitoring-teardown.sh` and Makefile targets `cluster-slice-verify`/`observability-verify-ci`.**

## Performance

- **Duration:** ~35 min
- **Completed:** 2026-08-24
- **Tasks:** 3/3 completed
- **Files modified:** 6 (2 created, 4 modified)

## Accomplishments

- Extracted the monitoring stack's `helm_install`/`wait_for_*` install sequence out of `scripts/stages/85-monitoring.sh` into a new, shared `scripts/monitoring-install.sh`, called by both the local cluster-up path and the new CI staggered path — one place the install shape can be edited, so the two paths cannot silently drift apart.
- Added a fail-closed `scripts/monitoring-teardown.sh` (single `helm uninstall` of all three monitoring releases, no `|| true`) used only by the new CI path.
- Added two new Makefile targets: `cluster-slice-verify` (cluster+slice only) and `observability-verify-ci` (install trimmed monitoring, run `tests/e2e/observability` alone, tear down — as one `set -e`-guarded shell invocation so a failing pytest run genuinely fails the target). `cluster-verify` itself is untouched.
- Disabled `kubeStateMetrics`/`nodeExporter` in `helm/values/ci/monitoring.yaml` and widened `tests/policy/test_values_profiles.py`'s `_is_monitoring_enablement` predicate to keep the D-06 divergence-axis policy gate passing.
- Staggered `.github/workflows/e2e-full.yml`: the old single `make cluster-verify` step is now `make cluster-slice-verify` followed by `make observability-verify-ci`, positioned in the exact same place, immediately before the unmodified `make rebuild-from-raw` capstone step.

## Task Commits

Each task was committed atomically:

1. **Task 1: Extract monitoring install logic and add a teardown script** - `8493f1d` (feat)
2. **Task 2: Add cluster-slice-verify and observability-verify-ci Makefile targets, trim CI monitoring values, keep the values-profile policy gate passing** - `06eb89e` (feat)
3. **Task 3: Stagger e2e-full.yml's monitoring install around the observability window** - `a279cc9` (feat)

**Plan metadata:** committed separately by the orchestrator (docs commit, per constraints — this executor does not commit SUMMARY.md/STATE.md itself)

## Files Created/Modified

- `scripts/monitoring-install.sh` - New. Shared otel-collector/tempo/kube-prometheus-stack `helm_install`/`wait_for_*` install sequence, extracted verbatim from `85-monitoring.sh`.
- `scripts/monitoring-teardown.sh` - New. Fail-closed `helm uninstall otel-collector tempo monitoring`, used only by the CI staggered path.
- `scripts/stages/85-monitoring.sh` - `PROFILE=ci` skip guard and header comment unchanged; local install path now delegates to `scripts/monitoring-install.sh`.
- `Makefile` - Added `cluster-slice-verify` and `observability-verify-ci` targets (and their `.PHONY` entries); `cluster-verify`'s own recipe body untouched.
- `helm/values/ci/monitoring.yaml` - `kubeStateMetrics.enabled: false` / `nodeExporter.enabled: false` replace the now-dead per-subchart resource overrides; top-of-file header comment updated to state the chart is now installed live in CI for the staggered window.
- `tests/policy/test_values_profiles.py` - `_is_monitoring_enablement` widened with a new branch recognizing `kubeStateMetrics.enabled`/`nodeExporter.enabled` as the existing monitoring-enablement axis; `PERMITTED_AXES` count (6) unchanged.
- `.github/workflows/e2e-full.yml` - The old single `make cluster-verify` step replaced by `make cluster-slice-verify` then `make observability-verify-ci`, in the same position, before `make rebuild-from-raw`. Comments referencing the old single-step shape updated (including the top-of-file header) so no literal `make cluster-verify` string remains anywhere in the file.

## Decisions Made

- Followed CONTEXT.md's locked staggering strategy and further-trimming decisions exactly as specified — no new implementation decisions beyond what the plan already resolved (Claude's Discretion items in CONTEXT.md were already settled by the plan itself: `observability-verify-ci` as a new Makefile target, a full `helm uninstall` teardown, no further resource-number trimming beyond kubeStateMetrics/nodeExporter).
- During Task 3, discovered the plan's own automated verify (`! grep -q 'make cluster-verify' .github/workflows/e2e-full.yml`) required zero occurrences of that literal string anywhere in the file, including pre-existing comments (the top-of-file header comment and my own new step comments both referenced it). Rewrote those comments to describe the staggering without using the literal `make cluster-verify` substring, preserving their original meaning. Not a deviation from the plan's intent — the plan's own action text said "update the comment block that previously explained the single combined step," and the automated verify simply required this more literally than the action text alone implied.

## Deviations from Plan

None requiring the Rule 1-4 framework — plan executed as written. One minor plan-arithmetic note, not a fix: the plan's overall `<verification>` item 6 states `scripts/monitoring-install.sh` should contain "5" `helm_install`/`wait_for_*` lines ("3 helm_install + 2 wait_for_*"), but the original `85-monitoring.sh` content the plan's own Task 1 action text instructs to copy "verbatim" actually has 3 `wait_for_*` calls (two `wait_for_deploy_available` + one `wait_for_statefulset_ready`), for a true total of 6. `scripts/monitoring-install.sh` was written to match the actual original script content verbatim (as Task 1 explicitly required), not the plan's own miscounted summary figure. Task 1's own task-level `<verify>`/`<done>` criteria only assert the `helm_install` count (3), which is satisfied and was independently confirmed.

## Issues Encountered

None beyond the comment-string fix documented above under Decisions Made.

## User Setup Required

None - no external service configuration required. This plan explicitly excludes a live CI proof run (deferred to a separate follow-up-3 task per CONTEXT.md); all verification performed here was offline/local (`bash -n`, `helm template`, `make helm-lint`, `make manifests`, and the two policy pytest suites).

## Verification Performed

All items from the plan's `<verification>` block were run and passed:

1. `bash -n` passed for `scripts/monitoring-install.sh`, `scripts/monitoring-teardown.sh`, `scripts/stages/85-monitoring.sh`.
2. `tools/bin/helm template monitoring prometheus-community/kube-prometheus-stack --version 88.2.0 -f helm/values/ci/monitoring.yaml -n monitoring` renders zero `app.kubernetes.io/name: kube-state-metrics`/`app.kubernetes.io/name: prometheus-node-exporter` resources (network access was available in this environment, so this ran live rather than being skipped).
3. `make helm-lint` — exit 0, 0 charts failed (10 charts × 2 profiles). `make manifests` — exit 0, kubeconform summary: 540 resources found in 20 files, Invalid: 0, Errors: 0.
4. `uv run pytest tests/policy/test_ci_invokes_make_only.py -q` — 4 passed.
5. `uv run pytest tests/policy/test_values_profiles.py -q` — 6 passed.
6. `grep -c 'helm_install\|wait_for_' scripts/stages/85-monitoring.sh` (comments filtered) = 0; same in `scripts/monitoring-install.sh` = 6 (see Deviations note above on the plan's own miscounted "5").
7. Manual read of `.github/workflows/e2e-full.yml` confirmed: the two new steps (`Run cluster + slice E2E suite`, `Install trimmed monitoring, run tests/e2e/observability, tear down`) sit exactly where the old single step was, immediately followed by the unmodified `Run rebuild-from-raw (D-24 capstone)` step.

## Known Stubs

None. No hardcoded empty values, placeholder text, or unwired data sources were introduced.

## Next Phase Readiness

- `tests/e2e/observability` can now genuinely run in `.github/workflows/e2e-full.yml` via `make observability-verify-ci`, instead of being permanently unreachable under `PROFILE=ci` — closing the last unproven piece of CICD-09/D-19.
- Explicitly NOT proven by this plan (per its own scope boundary and CONTEXT.md): whether the staggered, trimmed stack actually fits the CI node's ~3000m CPU budget under real, concurrent load in an actual GitHub Actions run. RESEARCH.md's own estimate (~2770m/3000m, ~8% margin) is a projection, not a fresh live measurement of this exact staggered sequence. A live CI run on a real push to `main` is the only way to confirm this margin holds — explicitly deferred to a separate follow-up-3 task, per CONTEXT.md's own stated scope boundary. No blockers for that follow-up; this plan's own three tasks are fully complete and independently verified offline.

---
*Phase: quick-260824-ayw-build-a-trimmed-ci-only-monitoring-stack*
*Completed: 2026-08-24*

## Self-Check: PASSED

All 8 claimed files confirmed present on disk (`scripts/monitoring-install.sh`, `scripts/monitoring-teardown.sh`, `scripts/stages/85-monitoring.sh`, `Makefile`, `helm/values/ci/monitoring.yaml`, `tests/policy/test_values_profiles.py`, `.github/workflows/e2e-full.yml`, this SUMMARY.md). All 3 task commit hashes (`8493f1d`, `06eb89e`, `a279cc9`) confirmed present in `git log --oneline --all`.
