---
phase: quick-260824-ayw-build-a-trimmed-ci-only-monitoring-stack
verified: 2026-08-24T06:40:28Z
status: passed
score: 8/8 must-haves verified
overrides_applied: 0
---

# Quick Task 260824-ayw: Build a trimmed CI-only monitoring stack Verification Report

**Task Goal:** Build a trimmed CI-only monitoring stack (single-pod Prometheus/Grafana/Tempo profile) so tests/e2e/observability can run in CI
**Verified:** 2026-08-24T06:40:28Z
**Status:** passed
**Re-verification:** No — initial verification

**Scope note (per task instructions):** This plan's own success criteria explicitly defer a live CI proof run to a separate follow-up-3 task. That is NOT verified here (it cannot be — there is no live GitHub Actions run to observe) and is NOT counted as a gap, per the plan's own stated scope boundary. This report verifies only what the plan's must_haves/success_criteria actually claim: the extraction is real, the recipe is correct (including the `set -e` fix from checker round 2), the policy test passes, the workflow YAML is staged correctly around the observability window, and `make cluster-verify` is unchanged.

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | `make observability-verify-ci` installs the trimmed CI monitoring stack, runs `tests/e2e/observability` alone, then tears down via `helm uninstall` | VERIFIED | `Makefile:389-395` — recipe is `set -a; . helm/versions.env; set +a; \` / `set -e; \` / `ctx="kind-$$CLUSTER_NAME"; \` / `PROFILE=ci KUBECTL_CONTEXT="$$ctx" scripts/monitoring-install.sh; \` / `$(RUN_CLUSTER) pytest tests/e2e/observability -q; \` / `KUBECTL_CONTEXT="$$ctx" scripts/monitoring-teardown.sh`. `bash -n` clean; `scripts/monitoring-install.sh` and `scripts/monitoring-teardown.sh` confirmed executable and correct (see artifacts below). |
| 2 | `make cluster-slice-verify` runs only `tests/e2e/cluster` and `tests/e2e/slice` — no observability, no monitoring install | VERIFIED | `Makefile:357-358`: `cluster-slice-verify:` recipe body is exactly `$(RUN_CLUSTER) pytest tests/e2e/cluster tests/e2e/slice -q`. No monitoring-install/teardown reference in this target. |
| 3 | `make cluster-verify` (unmodified byte-for-byte) still runs cluster+slice+observability together in one pytest invocation | VERIFIED | `diff` of `cluster-verify:`'s recipe block between commit `eb97595` (pre-plan HEAD) and current `Makefile` produced zero output — confirmed byte-identical. Recipe still reads `$(RUN_CLUSTER) pytest tests/e2e/cluster tests/e2e/slice tests/e2e/observability -q`. |
| 4 | `scripts/stages/85-monitoring.sh`'s local path and the CI staggered path both install via the SAME extracted `scripts/monitoring-install.sh` — no duplicated call shape | VERIFIED | `85-monitoring.sh:91` calls `"${repo_root}/scripts/monitoring-install.sh"` after the unchanged `PROFILE=ci` skip guard; zero `helm_install`/`wait_for_` calls remain in `85-monitoring.sh` (`grep -v '^#' \| grep -c` = 0). `Makefile:393` (`observability-verify-ci`) calls the identical script path. Both paths converge on one file. |
| 5 | `.github/workflows/e2e-full.yml` installs monitoring only for the observability window via two `make`-only steps, positioned exactly where the old single step was, before `make rebuild-from-raw` | VERIFIED | `e2e-full.yml:125-129`: `Run cluster + slice E2E suite` (`make cluster-slice-verify`) immediately followed by `Install trimmed monitoring, run tests/e2e/observability, tear down` (`make observability-verify-ci`), immediately followed (line 135-136) by the unmodified `Run rebuild-from-raw (D-24 capstone)` step. Literal string `make cluster-verify` no longer appears anywhere in the file (`grep -c` = 0). |
| 6 | `helm/values/ci/monitoring.yaml` sets `kubeStateMetrics.enabled: false`/`nodeExporter.enabled: false`, and a `helm template` render produces zero kube-state-metrics/node-exporter resources | VERIFIED | File lines 688-692 set both keys. Live `helm template monitoring prometheus-community/kube-prometheus-stack --version 88.2.0 -f helm/values/ci/monitoring.yaml -n monitoring \| grep -c 'app.kubernetes.io/name: kube-state-metrics\|app.kubernetes.io/name: prometheus-node-exporter'` executed by this verifier — result `0`. `make helm-lint` also run live: `1 chart(s) linted, 0 chart(s) failed` for both local and ci monitoring profiles (pre-existing unrelated servicemonitor naming warning only). |
| 7 | `tests/policy/test_ci_invokes_make_only.py` still passes | VERIFIED | Ran `uv run pytest tests/policy/test_ci_invokes_make_only.py tests/policy/test_values_profiles.py -q` — `10 passed in 0.28s`. |
| 8 | `tests/policy/test_values_profiles.py::test_profiles_diverge_only_on_permitted_axes` still passes with the widened predicate | VERIFIED | `_is_monitoring_enablement` (test_values_profiles.py:97-133) has a new branch `segments[0] in {"kubeStateMetrics", "nodeExporter"} and path.endswith("enabled")`. Test passes (see above); `PERMITTED_AXES` count assertion (`== 6`) unchanged and still passes. |

**Score:** 8/8 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `scripts/monitoring-install.sh` | extracted install logic, single source for both call sites | VERIFIED | Exists, executable (`-rwxr-xr-x`), `bash -n` clean, contains exactly 3 `helm_install` calls (otel-collector, tempo, monitoring) + 2 `wait_for_*` calls (matches plan's Task-1-level `<done>` criteria — the plan's separate `<verification>` item 6 "5 total" figure was a documented arithmetic slip in the plan itself, correctly called out in SUMMARY.md's Deviations section; actual content is 6 helm_install/wait_for_ lines total, verbatim-copied from the pre-existing 85-monitoring.sh content). |
| `scripts/monitoring-teardown.sh` | helm uninstall of the three releases, CI-only | VERIFIED | Exists, executable, `bash -n` clean, single `helm uninstall otel-collector tempo monitoring --namespace monitoring --wait --timeout ... ` call, no `\|\| true`, header comment matches the corrected (round-1-fixed) fail-closed rationale. |
| `scripts/stages/85-monitoring.sh` | PROFILE=ci guard unchanged, delegates to monitoring-install.sh | VERIFIED | Header comment (lines 1-53) and PROFILE=ci skip guard byte-identical in shape/content to pre-plan version; zero `helm_install`/`wait_for_` calls remain; delegates via line 91. |
| `Makefile` | two new targets, cluster-verify unchanged | VERIFIED | `cluster-slice-verify`/`observability-verify-ci` present, both in `.PHONY` (line 61); `cluster-verify` recipe byte-diff-confirmed unchanged. |
| `helm/values/ci/monitoring.yaml` | kubeStateMetrics/nodeExporter disabled, header updated | VERIFIED | Lines 688-692 set both to `enabled: false`; header comment (lines 9-14) updated to state the chart is now installed live in CI for the staggered window. |
| `.github/workflows/e2e-full.yml` | split step in place before rebuild-from-raw | VERIFIED | Confirmed via direct read, lines 125-136. |
| `tests/policy/test_values_profiles.py` | widened predicate | VERIFIED | Lines 121-133; test suite passes live. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|----|--------|---------|
| `Makefile`'s `observability-verify-ci` | `scripts/monitoring-install.sh` / `monitoring-teardown.sh` | direct script invocation, `PROFILE=ci`/`KUBECTL_CONTEXT` inline | WIRED | `Makefile:393,395` |
| `scripts/stages/85-monitoring.sh` | `scripts/monitoring-install.sh` | direct invocation after PROFILE guard | WIRED | `85-monitoring.sh:91` |
| `.github/workflows/e2e-full.yml` | `Makefile`'s `cluster-slice-verify`/`observability-verify-ci` | `run: make <target>` steps | WIRED | `e2e-full.yml:126,129` |
| `helm/values/ci/monitoring.yaml`'s `kubeStateMetrics.enabled`/`nodeExporter.enabled` | `tests/policy/test_values_profiles.py`'s `_is_monitoring_enablement` | widened predicate branch | WIRED | `test_values_profiles.py:133`; live test run passes |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| `bash -n` syntax check on all 3 shell scripts | `bash -n scripts/monitoring-install.sh scripts/monitoring-teardown.sh scripts/stages/85-monitoring.sh` | all pass (OK1/OK2/OK3) | PASS |
| Executable bits set | `ls -la scripts/monitoring-install.sh scripts/monitoring-teardown.sh` | `-rwxr-xr-x` both | PASS |
| `helm template` renders zero kube-state-metrics/node-exporter resources | `helm template monitoring ... -f helm/values/ci/monitoring.yaml \| grep -c ...` | `0` | PASS |
| `make helm-lint` (both profiles, includes kube-prometheus-stack chart) | `make helm-lint` | `1 chart(s) linted, 0 chart(s) failed` (both local and ci monitoring profiles) | PASS |
| Policy test suite (both relevant files) | `uv run pytest tests/policy/test_values_profiles.py tests/policy/test_ci_invokes_make_only.py -q` | `10 passed` | PASS |
| `cluster-verify` recipe byte-diff against pre-plan commit | `diff` of extracted recipe blocks (eb97595 vs current) | empty diff | PASS |
| Literal `make cluster-verify` string absent from workflow | `grep -c 'make cluster-verify' .github/workflows/e2e-full.yml` | `0` (grep exit 1) | PASS |

### Probe Execution

Not applicable — this is a quick task with no `scripts/*/tests/probe-*.sh` convention invoked by its plan or success criteria. Verification relied on `bash -n`, live `helm template`/`helm-lint`, and live `pytest` runs against the actual policy tests, all executed directly by this verifier (not sourced from SUMMARY.md claims).

### Requirements Coverage

No formal `requirements:` IDs are declared in this plan's frontmatter beyond narrative CICD-09 follow-up-2 references (this is a quick task, not a numbered roadmap phase). The plan's five bullet-point requirements (staggering mechanism, cluster-verify stability, kubeStateMetrics/nodeExporter drop, no bigger runner) are all covered by the observable truths above.

### Anti-Patterns Found

None blocking. `grep -E "TBD|FIXME|XXX|TODO|HACK|PLACEHOLDER"` (case-insensitive) across all 7 modified/created files found only one incidental hit in `.github/workflows/e2e-full.yml` (lines 102-106) — a pre-existing "placeholder Grafana alert webhook" step unrelated to this task's monitoring-install/teardown/staggering work (confirmed by content: it's the CI synthetic webhook-URL fixture, not a stub in this plan's own deliverables).

Minor documentation staleness (informational, not a gap): `helm/values/ci/monitoring.yaml`'s `additionalServiceMonitors` block comment (lines 734-741) still reads "D-16/monitoring-enablement is about whether this chart is deployed LIVE in CI (it never is)" — this sentence is now stale given the top-of-file header (lines 9-14) was correctly updated to say the chart IS installed live for the staggered window. This is a leftover comment in a section the plan's Task 2 action text did not explicitly instruct to touch (only the top-of-file header and the kubeStateMetrics/nodeExporter block were in scope). Does not affect functional correctness of any must-have; noted for hygiene only.

### Human Verification Required

None. All must-haves in this plan are structurally/statically verifiable (file content, `bash -n`, `helm template`, `pytest`) and were independently re-executed by this verifier rather than trusted from SUMMARY.md. The one item genuinely requiring a live environment — whether the staggered stack fits the CI node's ~3000m CPU budget under real load — is explicitly out of scope for this plan (deferred to follow-up-3) and is not a gap per the task's own stated success criteria.

### Gaps Summary

No gaps. All 8 must-have truths verified against the actual codebase (not SUMMARY.md claims): the two new scripts exist, are executable, and contain the exact extracted logic; the two new Makefile targets exist with the correct `set -e`-guarded recipe shape (confirming the checker round-2 fix survived into the merged code); `cluster-verify` is byte-identical to its pre-plan state; the workflow YAML stages the two new steps in the exact original position; the CI monitoring values file disables kubeStateMetrics/nodeExporter with a live-confirmed zero-resource `helm template` render; and both affected policy tests pass live.

---

_Verified: 2026-08-24T06:40:28Z_
_Verifier: Claude (gsd-verifier)_
