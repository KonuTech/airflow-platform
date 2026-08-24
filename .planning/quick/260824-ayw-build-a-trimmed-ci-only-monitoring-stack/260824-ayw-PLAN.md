---
phase: quick-260824-ayw-build-a-trimmed-ci-only-monitoring-stack
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - scripts/monitoring-install.sh
  - scripts/monitoring-teardown.sh
  - scripts/stages/85-monitoring.sh
  - Makefile
  - helm/values/ci/monitoring.yaml
  - .github/workflows/e2e-full.yml
  - tests/policy/test_values_profiles.py
autonomous: true
requirements:
  - "Phase 11 CICD-09 follow-up-2 (part 2 of 2, deferred-items.md 'Plan 11-05'): tests/e2e/observability cannot pass on the CI profile because scripts/stages/85-monitoring.sh unconditionally skips the whole monitoring stack under PROFILE=ci (the fix for the live CrashLoopBackOff this session diagnosed at ~93% CPU allocation). User chose to build a trimmed, staggered CI-only monitoring stack over the cheaper alternative (marker-skipping tests/e2e/observability in CI)."
  - "CONTEXT.md locked decision: stagger the monitoring stack so it is live only during tests/e2e/observability's own test window, not the whole 120-minute e2e-full.yml job, by splitting the single `make cluster-verify` step in e2e-full.yml into (1) cluster+slice, then (2) install monitoring / run observability / tear down, before the existing `make rebuild-from-raw` capstone step."
  - "CONTEXT.md locked decision: `make cluster-verify` itself (the shared target local devs also use) must NOT change behavior — it keeps running cluster+slice+observability together for the local persistent 3-node profile."
  - "CONTEXT.md locked decision: drop kubeStateMetrics and nodeExporter from helm/values/ci/monitoring.yaml (kubeStateMetrics.enabled: false / nodeExporter.enabled: false) — verified zero test-coverage loss."
  - "CONTEXT.md locked decision: do not propose a bigger GitHub runner class."

must_haves:
  truths:
    - "`make observability-verify-ci` installs the trimmed CI monitoring stack (otel-collector, tempo, kube-prometheus-stack with kubeStateMetrics/nodeExporter disabled), runs tests/e2e/observability alone as its own pytest process, then tears the stack down via helm uninstall"
    - "`make cluster-slice-verify` runs only tests/e2e/cluster and tests/e2e/slice — no observability, no monitoring install"
    - "`make cluster-verify` (unmodified byte-for-byte) still runs tests/e2e/cluster, tests/e2e/slice and tests/e2e/observability together in one pytest invocation, exactly as before this plan"
    - "scripts/stages/85-monitoring.sh's local cluster-up path (PROFILE=local) and the new CI staggered path (observability-verify-ci) both install monitoring via the SAME extracted scripts/monitoring-install.sh — no duplicated helm_install/wait_for_* call shape"
    - ".github/workflows/e2e-full.yml installs monitoring only for the tests/e2e/observability window (via `make observability-verify-ci`), not for the whole job — the prior single `make cluster-verify` step is replaced by two `make`-only steps, positioned exactly where the old step was, before the existing `make rebuild-from-raw` step"
    - "helm/values/ci/monitoring.yaml explicitly sets kubeStateMetrics.enabled: false and nodeExporter.enabled: false, and a `helm template` render of the chart against this file produces zero kube-state-metrics/node-exporter resources"
    - "tests/policy/test_ci_invokes_make_only.py still passes — e2e-full.yml's new steps call `make <target>` only, never a bare pytest/helm invocation in the workflow YAML"
    - "tests/policy/test_values_profiles.py::test_profiles_diverge_only_on_permitted_axes still passes — the widened `_is_monitoring_enablement` predicate classifies kubeStateMetrics.enabled/nodeExporter.enabled as the existing 'monitoring enablement' axis, not an unclassified divergence"
  artifacts:
    - path: "scripts/monitoring-install.sh"
      provides: "extracted helm_install/wait_for_* install logic for otel-collector, tempo, kube-prometheus-stack — the single source both 85-monitoring.sh and observability-verify-ci call"
      contains: "helm_install monitoring prometheus-community/kube-prometheus-stack"
    - path: "scripts/monitoring-teardown.sh"
      provides: "helm uninstall of the three monitoring releases, used only by the CI staggered path"
      contains: "helm uninstall"
    - path: "scripts/stages/85-monitoring.sh"
      provides: "PROFILE=ci skip guard unchanged; local install path now delegates to scripts/monitoring-install.sh instead of inlining the helm_install/wait_for_* calls"
      contains: "scripts/monitoring-install.sh"
    - path: "Makefile"
      provides: "two new targets: cluster-slice-verify (cluster+slice only) and observability-verify-ci (install/run/teardown observability); cluster-verify target body unchanged"
      contains: "observability-verify-ci"
    - path: "helm/values/ci/monitoring.yaml"
      provides: "kubeStateMetrics and nodeExporter explicitly disabled for the CI profile; header comment updated to state the chart is now installed live in CI for the staggered tests/e2e/observability window"
      contains: "kubeStateMetrics"
    - path: ".github/workflows/e2e-full.yml"
      provides: "the former single cluster-verify step split into cluster-slice-verify + observability-verify-ci steps, in place, before rebuild-from-raw"
      contains: "make observability-verify-ci"
    - path: "tests/policy/test_values_profiles.py"
      provides: "widened `_is_monitoring_enablement` predicate recognizing kubeStateMetrics.enabled/nodeExporter.enabled as the existing monitoring-enablement axis"
      contains: "kubeStateMetrics"
  key_links:
    - from: "Makefile's observability-verify-ci target"
      to: "scripts/monitoring-install.sh / scripts/monitoring-teardown.sh"
      via: "direct script invocation with PROFILE=ci and KUBECTL_CONTEXT set inline in the recipe"
      pattern: "scripts/monitoring-install\\.sh"
    - from: "scripts/stages/85-monitoring.sh"
      to: "scripts/monitoring-install.sh"
      via: "direct script invocation after the PROFILE=ci skip guard, on the local cluster-up path"
      pattern: "repo_root.*/scripts/monitoring-install\\.sh"
    - from: ".github/workflows/e2e-full.yml"
      to: "Makefile's cluster-slice-verify and observability-verify-ci targets"
      via: "run: make <target> steps, replacing the old run: make cluster-verify step"
      pattern: "make (cluster-slice-verify|observability-verify-ci)"
    - from: "helm/values/ci/monitoring.yaml's kubeStateMetrics.enabled/nodeExporter.enabled"
      to: "tests/policy/test_values_profiles.py's _is_monitoring_enablement"
      via: "widened predicate branch matching these two literal top-level keys' .enabled leaf"
      pattern: "kubeStateMetrics.*nodeExporter"
---

<objective>
Build a trimmed, staggered CI-only monitoring stack so `tests/e2e/observability` can run in `.github/workflows/e2e-full.yml` instead of being permanently unreachable under `PROFILE=ci` (Phase 11's CICD-09 follow-up-2, part 2 of 2). The monitoring stack (otel-collector, tempo, kube-prometheus-stack with kubeStateMetrics/nodeExporter disabled) is installed only for the duration of the `tests/e2e/observability` sub-suite — not the whole ~120-minute job — by splitting `e2e-full.yml`'s single `make cluster-verify` step into a narrower `cluster-slice-verify` step followed by a new `observability-verify-ci` step that installs monitoring, runs the suite, and tears it down before `make rebuild-from-raw`. `make cluster-verify` itself, and `scripts/stages/85-monitoring.sh`'s behavior for local developers, are unchanged.

Purpose: close the last unproven piece of CICD-09/D-19 — every other E2E sub-suite already runs in CI; `tests/e2e/observability` is the one still permanently skipped there.

Output: `scripts/monitoring-install.sh` (extracted, shared install logic), `scripts/monitoring-teardown.sh` (new), two new Makefile targets (`cluster-slice-verify`, `observability-verify-ci`), a trimmed `helm/values/ci/monitoring.yaml` with an updated header comment, a widened `tests/policy/test_values_profiles.py` predicate, and a staggered `e2e-full.yml`.

This plan does NOT include a live CI proof run — that is explicitly deferred to a separate follow-up-3 task per CONTEXT.md. All verification here is offline: syntax/lint checks, `helm template` rendering, policy tests, and reading the modified files for correctness. The ~8% sizing margin RESEARCH.md computed is an estimate, not a guarantee, and is called out again in this plan's own success criteria.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/STATE.md
@.planning/quick/260824-ayw-build-a-trimmed-ci-only-monitoring-stack/260824-ayw-CONTEXT.md
@.planning/quick/260824-ayw-build-a-trimmed-ci-only-monitoring-stack/260824-ayw-RESEARCH.md

<policy_question_resolved>
RESEARCH.md flagged an open question: does `tests/policy/test_ci_invokes_make_only.py` scope its ban on bare `pytest`/`helm` invocations to workflow YAML `run:` blocks only, or also to Makefile recipe bodies? Read directly this session: `WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"`, and `_run_steps()` walks only `(workflow.get("jobs") or {}).items()` parsed from files under that directory. The `DIRECT_TOOLS` regex (`\b(ruff|mypy|pytest|lint-imports)\b|tools/bin/gitleaks`) is applied only to lines inside those workflow YAML `run:` blocks — Makefile recipe bodies are never scanned at all (proven by `cluster-verify`'s, `smoke-verify`'s and `chaos-verify`'s own existing bare `pytest ...` recipe lines, which already pass this test today). Conclusion: a bare `pytest tests/e2e/cluster tests/e2e/slice -q` line INSIDE a new Makefile target is fine; the same line inline in `e2e-full.yml`'s `run:` block would be flagged and fail this test. This confirms RESEARCH.md's recommended design (a new `cluster-slice-verify` Makefile target, not an inline pytest call in the workflow) is required, not merely safer.
</policy_question_resolved>

<revision_note>
This plan was revised after checker review (260824-ayw). Fixes applied: (1) BLOCKER — `helm/values/ci/monitoring.yaml`'s new `kubeStateMetrics.enabled`/`nodeExporter.enabled` keys are an unclassified divergence under `tests/policy/test_values_profiles.py::test_profiles_diverge_only_on_permitted_axes`; Task 2 now also widens that file's `_is_monitoring_enablement` predicate and the plan's own verification runs that test. (2) `observability-verify-ci`'s recipe description now shows the explicit backslash-continued single-shell body, matching `migrate-analytics`/`rollback`'s actual shape. (3) The teardown fail-closed rationale no longer claims it protects `make rebuild-from-raw` (that step has no `if: always()` and never runs in this failure branch on a single-use ephemeral runner) — the real rationale is honest CI failure signal. (4) `helm/values/ci/monitoring.yaml`'s stale top-of-file header comment ("never deployed live in CI") is now updated as part of Task 2.
</revision_note>

<interfaces>
From `scripts/helm-install.sh` (sourced, provides `helm_install`):
```
helm_install <release> <chart-ref> <namespace> <version-var-name> <values-basename> [<wait-strategy>]
```
Resolves the chart version from the named `helm/versions.env` variable, the values file from `helm/values/${PROFILE:-local}/${values-basename}.yaml`, and `--kube-context` from `$KUBECTL_CONTEXT` if set. Default wait strategy is `watcher`; pass `hookOnly` as the 6th arg to skip waiting on the chart's own resources before running hooks (required for kube-prometheus-stack — see 85-monitoring.sh's existing header comment, unchanged).

From `scripts/wait-for.sh` (sourced, provides the wait helpers used here):
```
wait_for_deploy_available <namespace> <deployment>
wait_for_statefulset_ready <namespace> <statefulset> [<replicas>]
```
Both use `$KUBECTL_CONTEXT` if set (via an internal `_kubectl_wait` helper), otherwise plain `kubectl`.

From `helm/versions.env` (sourced): `OTEL_COLLECTOR_CHART_VERSION`, `TEMPO_CHART_VERSION`, `KUBE_PROMETHEUS_STACK_CHART_VERSION`, `CLUSTER_NAME`.

From `Makefile` (existing conventions this plan's new targets must match):
- `RUN_CLUSTER := $(RUN) --group cluster` — every target touching boto3/psycopg/pytest against a live cluster uses this, never bare `$(RUN)`.
- `cluster-verify:` (unchanged by this plan): `$(RUN_CLUSTER) pytest tests/e2e/cluster tests/e2e/slice tests/e2e/observability -q`
- The `migrate-analytics`/`rollback` targets' own shape for resolving `ctx="kind-$$CLUSTER_NAME"` via `@set -a; . helm/versions.env; set +a;` at the top of a recipe, with every subsequent line ending in a literal trailing `\` up through the final command — this is what keeps the whole recipe body ONE shell invocation so `ctx` (and any exported env) stays in scope across every line.

From `tests/policy/test_values_profiles.py` (existing, being widened by Task 2):
```
def _is_monitoring_enablement(path: str) -> bool:
    segments = path.split(".")
    if "metrics" in segments or "monitoring" in segments:
        return True
    return path == "env" or path.startswith("config.traces")
```
`PERMITTED_AXES` is a `tuple[PermittedAxis, ...]` of `(name, predicate, argument)`; `test_every_permitted_axis_carries_an_argument` asserts `len(PERMITTED_AXES) == 6` — widening an existing predicate's `return` logic does not change this count and must not.
</interfaces>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Extract monitoring install logic and add a teardown script</name>
  <files>scripts/monitoring-install.sh, scripts/monitoring-teardown.sh, scripts/stages/85-monitoring.sh</files>
  <action>
    Create `scripts/monitoring-install.sh` (new, executable, `set -euo pipefail`). It resolves `repo_root` the same way `scripts/stages/85-monitoring.sh` currently does, sources `helm/versions.env`, `scripts/helm-install.sh` and `scripts/wait-for.sh`, then runs verbatim the same sequence `scripts/stages/85-monitoring.sh` currently inlines at lines 95-115: the three `helm repo add` calls (open-telemetry, grafana-community, prometheus-community) plus `helm repo update`, then `helm_install otel-collector open-telemetry/opentelemetry-collector monitoring OTEL_COLLECTOR_CHART_VERSION otel-collector`, `helm_install tempo grafana-community/tempo monitoring TEMPO_CHART_VERSION tempo`, `wait_for_deploy_available monitoring otel-collector-opentelemetry-collector`, `wait_for_statefulset_ready monitoring tempo`, then `helm_install monitoring prometheus-community/kube-prometheus-stack monitoring KUBE_PROMETHEUS_STACK_CHART_VERSION monitoring hookOnly`, `wait_for_deploy_available monitoring monitoring-kube-prometheus-operator`. Keep the `hookOnly` 6th argument on the kube-prometheus-stack call exactly as-is — do not "simplify" it to the default `watcher` strategy (this would deadlock on Grafana's Secret dependency on a genuinely first-ever local cluster-up; see the header comment staying in 85-monitoring.sh). After writing the file, run `chmod +x scripts/monitoring-install.sh` (directly-executed scripts in this repo, e.g. `scripts/cluster-up.sh`/`scripts/doctor.sh`, are committed executable — sourced-only helpers like `scripts/helm-install.sh` are +x too by existing convention, so match it here). End with the same two echo lines 85-monitoring.sh currently prints (the "Monitoring stage installed and running" summary and the "NOTE: on a first-ever cluster-up..." Grafana caveat) — this script is now the single owner of both the install work and its own completion messaging, since it is called from two places.

    Create `scripts/monitoring-teardown.sh` (new, executable, `set -euo pipefail`). Resolve `repo_root` and `helm_bin` the same way. Build `context_args=(--kube-context "${KUBECTL_CONTEXT}")` when `$KUBECTL_CONTEXT` is set, matching `helm-install.sh`'s own convention. After writing the file, run `chmod +x scripts/monitoring-teardown.sh` to match this repo's convention for directly-executed scripts. Run one `helm uninstall otel-collector tempo monitoring --namespace monitoring --wait --timeout "${HELM_UNINSTALL_TIMEOUT:-3m}" "${context_args[@]}"` call (helm supports multiple release names in one uninstall invocation). Do NOT append `|| true` — swallowing a failed uninstall would report a false success with no honest signal for whoever has to debug a hung/failed CI run; fail loudly instead, matching this repo's existing fail-closed Makefile targets (`rollback`, `migrate-analytics`). Get the rationale precisely right, because the header comment below must state it correctly: `.github/workflows/e2e-full.yml`'s subsequent `make rebuild-from-raw` step carries no `if: always()`/`continue-on-error`, so on a single-use ephemeral GitHub Actions runner a failure here stops the job outright — `rebuild-from-raw` never actually runs in that failure branch, so fail-closed here is NOT protecting that later step from a leftover install; it exists purely to surface the failure honestly rather than hide it. Add a short header comment explaining: (a) this is used only by the new CI staggered path, never by the local persistent-cluster path; (b) `helm uninstall` does not delete the PVCs or CRDs these charts created, which is fine here because the whole ephemeral kind cluster is torn down at the end of the CI job regardless; (c) errors are not swallowed because this is a fail-closed target giving a human debugging the CI failure honest signal — not because it protects `make rebuild-from-raw`, which never runs in that failure branch anyway (no `if: always()` on that step).

    Modify `scripts/stages/85-monitoring.sh`: keep the existing header comment block (lines 1-53) and the `PROFILE=ci` skip guard (the `if [ "${PROFILE:-local}" = "ci" ]; then ... exit 0; fi` block) completely unchanged. Replace everything after the skip guard (the `source` lines, the `helm repo add`/`update` calls, the three `helm_install` calls, the `wait_for_*` calls, and the two closing echo lines) with a single call: `"${repo_root}/scripts/monitoring-install.sh"`. Add a short comment immediately above that call explaining the extraction: this stage script's job is now only to decide WHETHER to install (the PROFILE guard above it), never HOW — the actual `helm_install`/`wait_for_*` logic now lives in `scripts/monitoring-install.sh`, shared with the new CI staggered path (Makefile's `observability-verify-ci`, added in Task 2) so the two paths can never drift into two different install shapes.
  </action>
  <verify>
    <automated>cd /home/konutec/projects/airflow-platform && bash -n scripts/monitoring-install.sh && bash -n scripts/monitoring-teardown.sh && bash -n scripts/stages/85-monitoring.sh && test -x scripts/monitoring-install.sh && test -x scripts/monitoring-teardown.sh && [ "$(grep -v '^#' scripts/stages/85-monitoring.sh | grep -c 'helm_install\|wait_for_')" = "0" ] && [ "$(grep -v '^#' scripts/monitoring-install.sh | grep -c 'helm_install')" = "3" ]</automated>
  </verify>
  <done>`scripts/monitoring-install.sh` exists, is executable, passes `bash -n`, and contains exactly the 3 `helm_install` calls (otel-collector, tempo, monitoring) plus the 2 `wait_for_*` calls previously inlined in `85-monitoring.sh`. `scripts/monitoring-teardown.sh` exists, is executable, passes `bash -n`, and runs a single `helm uninstall` covering all three release names with no `|| true`, with a header comment whose fail-closed rationale is honest CI failure signal — not protection of `make rebuild-from-raw`, which never runs after a failure in this step. `scripts/stages/85-monitoring.sh` retains its unchanged header comment and `PROFILE=ci` skip guard, contains zero `helm_install`/`wait_for_*` calls of its own, and instead calls `scripts/monitoring-install.sh`.</done>
</task>

<task type="auto">
  <name>Task 2: Add cluster-slice-verify and observability-verify-ci Makefile targets, trim CI monitoring values, keep the values-profile policy gate passing</name>
  <files>Makefile, helm/values/ci/monitoring.yaml, tests/policy/test_values_profiles.py</files>
  <action>
    In `Makefile`, add `cluster-slice-verify` and `observability-verify-ci` to the `.PHONY` list (alongside the existing `cluster-verify` entry). Add a new target `cluster-slice-verify` immediately after `cluster-verify`'s recipe (before `smoke-verify`): recipe body `$(RUN_CLUSTER) pytest tests/e2e/cluster tests/e2e/slice -q`. Give it a header comment explaining it exists ONLY because `e2e-full.yml`'s CONTEXT.md-locked staggering strategy needs `tests/e2e/observability` to run in its own separate window (installed monitoring only for that window, see `observability-verify-ci` below) — `cluster-verify` itself stays the single target a local developer runs for the full cluster+slice+observability suite against the persistent 3-node profile; this is a disclosed, CI-workflow-specific narrowing, not a replacement, mirroring `smoke-verify`'s own "deliberately narrower" precedent.

    Add a second new target `observability-verify-ci` immediately after `cluster-slice-verify`. Its recipe body MUST be one continuous shell invocation, matching `migrate-analytics`/`rollback`'s own established backslash-continuation shape exactly (read both before writing this) — every physical recipe line except the last ends in a literal trailing `\` continuation character, because a bare newline with no trailing `\` starts a brand-new subshell under Make (absent `.ONESHELL:`), and `ctx` set on an earlier line would not exist in a later one. Write it as five physical lines: line 1 `@set -a; . helm/versions.env; set +a; \`; line 2 `ctx="kind-$$CLUSTER_NAME"; \`; line 3 `PROFILE=ci KUBECTL_CONTEXT="$$ctx" scripts/monitoring-install.sh; \`; line 4 `$(RUN_CLUSTER) pytest tests/e2e/observability -q; \`; line 5, with NO trailing `\` since it is the final command in the chain, `KUBECTL_CONTEXT="$$ctx" scripts/monitoring-teardown.sh`. `PROFILE=ci` is a literal on line 3 (not `$(PROFILE)`) since this target only ever makes sense against the single-node CI cluster shape. Header comment above the target: explains the staggering rationale (CONTEXT.md's locked CPU-contention decision — monitoring live only for this window, not the whole job); that teardown runs unconditionally after a successful pytest run but the recipe still aborts (leaving monitoring installed) if either the install or the pytest step itself fails, since a failing line in a `\`-continued recipe still stops the whole chain before the next command runs — an accepted trade-off given the whole ephemeral kind cluster is destroyed at job end regardless, not a bug to fix here; and that this is about honest CI failure signal, not about protecting `make rebuild-from-raw` from a leftover install (that later step has no `if: always()` and never runs in this failure branch anyway, per Task 1's `monitoring-teardown.sh` header comment).

    In `helm/values/ci/monitoring.yaml`, remove the existing `kube-state-metrics:` (subchart resources block, `resources.requests.cpu: 20m` etc.) and `prometheus-node-exporter:` (subchart resources block) top-level keys — once the subcharts are disabled below, these per-subchart resource overrides become dead configuration. In their place, add two top-level keys with a comment explaining the change: `kubeStateMetrics:` with `enabled: false`, and `nodeExporter:` with `enabled: false` (these are the kube-prometheus-stack chart's own top-level enable/disable toggles, distinct from the per-subchart values keys just removed). The comment must state: verified this session that no file under `tests/e2e/observability/` references kube-state-metrics or node-exporter metrics/targets (only `analytics-postgres`, `prometheus`, `tempo` datasources are exercised) — disabling both is a free CPU/pod-count saving with zero test-coverage loss, now that this stack is genuinely installed live in CI for the staggered `tests/e2e/observability` window (reference `observability-verify-ci` / `scripts/monitoring-install.sh`).

    Also update this file's own top-of-file header comment (currently lines 1-9). It currently states the chart "is never deployed live in CI (D-16: `make manifests` renders and lints this file; nothing in `scripts/stages/85-monitoring.sh`'s CI path actually installs it)" — that sentence becomes false after this plan. Rewrite it to say the chart IS now installed live in CI, but only for the staggered `tests/e2e/observability` window via `make observability-verify-ci` (which calls `scripts/monitoring-install.sh` then `scripts/monitoring-teardown.sh`), not for the whole CI job; `make manifests` still separately renders/lints this same file offline as before, unrelated to the live install path. Keep the rest of the header comment (the "resource sizing" axis argument referencing `test_values_profiles.py`) unchanged.

    In `tests/policy/test_values_profiles.py`, fix the blocker this plan would otherwise introduce: `kubeStateMetrics.enabled`/`nodeExporter.enabled` are new leaf paths that diverge between `helm/values/local/monitoring.yaml` (absent) and `helm/values/ci/monitoring.yaml` (`false`), and neither path matches the existing `_is_monitoring_enablement` predicate's literal-segment check (`"metrics"`/`"monitoring"` as a bare path segment, or `env`/`config.traces.*`) — `test_profiles_diverge_only_on_permitted_axes` fails without this fix. Widen `_is_monitoring_enablement` — do NOT add a seventh entry to `PERMITTED_AXES`; this is the same "monitoring enablement" axis applied to a fourth chart, the same incomplete-implementation-gap pattern the function's own existing comment already documents for the `config.traces`/`env` branch, not a new axis — by adding one more `return` branch: `segments[0] in {"kubeStateMetrics", "nodeExporter"} and path.endswith("enabled")` (restricted to the `.enabled` leaf specifically, so a hypothetical future unrelated key nested under `kubeStateMetrics`/`nodeExporter` would still be caught as an unclassified divergence). Add an inline comment above this branch, matching the style of the existing `config.traces`/`env` comment block, stating: this is kube-prometheus-stack's own top-level subchart enable/disable toggle, camelCase-spelled by the chart itself rather than a bare `metrics`/`monitoring` segment; verified this session that no file under `tests/e2e/observability/` references kube-state-metrics or node-exporter; and disabling both in CI only is now meaningful because the chart is genuinely installed live in CI for the staggered `tests/e2e/observability` window (cross-reference `helm/values/ci/monitoring.yaml`'s own updated header comment). Do NOT change `test_every_permitted_axis_carries_an_argument`'s `len(PERMITTED_AXES) == 6` assertion or its docstring's axis count — widening an existing predicate's matching logic does not add a table entry.
  </action>
  <verify>
    <automated>cd /home/konutec/projects/airflow-platform && grep -c 'observability-verify-ci:' Makefile | grep -qx 1 && grep -c 'cluster-slice-verify:' Makefile | grep -qx 1 && grep -A1 '^kubeStateMetrics:' helm/values/ci/monitoring.yaml | grep -q 'enabled: false' && grep -A1 '^nodeExporter:' helm/values/ci/monitoring.yaml | grep -q 'enabled: false' && tools/k8s/install_helm.sh >/dev/null && set -a && . helm/versions.env && set +a && tools/bin/helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null 2>&1 || true && tools/bin/helm repo update >/dev/null && [ "$(tools/bin/helm template monitoring prometheus-community/kube-prometheus-stack --version "$KUBE_PROMETHEUS_STACK_CHART_VERSION" -f helm/values/ci/monitoring.yaml -n monitoring | grep -c 'app.kubernetes.io/name: kube-state-metrics\|app.kubernetes.io/name: prometheus-node-exporter')" = "0" && uv run pytest tests/policy/test_values_profiles.py -q</automated>
  </verify>
  <done>`Makefile` defines `cluster-slice-verify` (running only `tests/e2e/cluster tests/e2e/slice`) and `observability-verify-ci` (install trimmed monitoring via `scripts/monitoring-install.sh`, run `tests/e2e/observability` alone, tear down via `scripts/monitoring-teardown.sh`, recipe written as a single backslash-continued shell invocation), both added to `.PHONY`; `cluster-verify`'s own recipe body is untouched. `helm/values/ci/monitoring.yaml` sets `kubeStateMetrics.enabled: false` and `nodeExporter.enabled: false`, with the dead per-subchart resource blocks removed and its top-of-file header comment updated to reflect the chart now being installed live in CI for the staggered window; a `helm template` render of the chart against this file produces zero kube-state-metrics/node-exporter resources. `tests/policy/test_values_profiles.py::test_profiles_diverge_only_on_permitted_axes` passes — the widened `_is_monitoring_enablement` predicate classifies both new leaf paths as the existing "monitoring enablement" axis rather than an unclassified divergence, and `test_every_permitted_axis_carries_an_argument` still asserts exactly 6 axes.</done>
</task>

<task type="auto">
  <name>Task 3: Stagger e2e-full.yml's monitoring install around the observability window</name>
  <files>.github/workflows/e2e-full.yml</files>
  <action>
    Replace the existing "Run the full local E2E suite (make cluster-verify)" step (the comment block above it plus its single `run: make cluster-verify` line) with two steps, in the exact same position in the job (immediately after the "Unseal and bootstrap Vault" step, immediately before the existing "Run rebuild-from-raw (D-24 capstone)" step, which stays completely unmodified): first a step named "Run cluster + slice E2E suite (observability deferred, staggered below)" with `run: make cluster-slice-verify`; second, immediately after it, a step named "Install trimmed monitoring, run tests/e2e/observability, tear down" with `run: make observability-verify-ci`. Update the comment block that previously explained the single combined step to instead reference this plan's staggering: `tests/e2e/cluster`/`tests/e2e/slice` run first (unchanged D-19 local suite, minus observability), then monitoring is installed only for the `tests/e2e/observability` window and torn down again before `rebuild-from-raw` runs — per this quick task's CONTEXT.md-locked decision, to keep the trimmed monitoring stack's own CPU footprint off the node for the rest of the ~120-minute job. Do not modify any other step in this workflow file (Vault bootstrap, rebuild-from-raw, the failure-issue step, env/permissions blocks all stay exactly as they are).
  </action>
  <verify>
    <automated>cd /home/konutec/projects/airflow-platform && grep -c 'make cluster-slice-verify' .github/workflows/e2e-full.yml | grep -qx 1 && grep -c 'make observability-verify-ci' .github/workflows/e2e-full.yml | grep -qx 1 && ! grep -q 'make cluster-verify' .github/workflows/e2e-full.yml && uv run pytest tests/policy/test_ci_invokes_make_only.py -q</automated>
  </verify>
  <done>`.github/workflows/e2e-full.yml` runs `make cluster-slice-verify` then `make observability-verify-ci` in place of the old single `make cluster-verify` step, positioned before the unmodified `make rebuild-from-raw` step. `tests/policy/test_ci_invokes_make_only.py` passes — both new steps call `make` only, no bare `pytest`/`helm` in the workflow YAML.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|--------------|
| GitHub Actions runner -> ephemeral kind cluster | `observability-verify-ci`'s `helm upgrade --install`/`helm uninstall` calls run with the same cluster-admin-equivalent kubeconfig every other CI stage already uses; this plan adds no new credential or RBAC surface, only new call sites of already-existing `helm_install`/`wait_for_*` helpers. |
| CI job timeline -> live monitoring stack | The trimmed stack is now genuinely installed live in CI (previously it never was) for one bounded window; a failed or hung `tests/e2e/observability` run leaves it installed until the job's own timeout/cleanup, not indefinitely on a persistent cluster. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|------------------|
| T-quick-01 | Tampering (wrong/partial teardown) | `scripts/monitoring-teardown.sh` | mitigate | Uninstalls exactly the three known release names (`otel-collector`, `tempo`, `monitoring`) in the `monitoring` namespace only — no wildcard, no dynamic discovery of "whatever is installed". Errors are not swallowed (`set -euo pipefail`, no `\|\| true`), so a partial/failed teardown fails the CI step loudly instead of reporting a false success — honest signal for whoever debugs it, not protection of `make rebuild-from-raw` (that step has no `if: always()` and never runs in this failure branch on a single-use ephemeral runner anyway). |
| T-quick-02 | Denial of Service (CPU starvation reproducing the original CrashLoopBackOff) | The staggered CI window itself, per RESEARCH.md's sizing math | accept | RESEARCH.md's own live-measured-baseline projection shows only an ~8% margin (~2770m/3000m) for the staggered window — genuinely thin, not a proven-safe number. Accepted per CONTEXT.md (no bigger runner class permitted) and explicitly deferred to a separate follow-up-3 task for live CI confirmation; this plan's own success criteria state the estimate is not a guarantee. |
| T-quick-03 | Repudiation / silent scope drift (local vs CI install logic diverging over time) | `scripts/stages/85-monitoring.sh` vs the new CI path | mitigate | Both paths call the same `scripts/monitoring-install.sh` — there is structurally only one place the `helm_install`/`wait_for_*` call shape can be edited, so the two paths cannot silently drift into different behavior the way two independent copies could. |
</threat_model>

<verification>
1. `bash -n` passes for `scripts/monitoring-install.sh`, `scripts/monitoring-teardown.sh`, and `scripts/stages/85-monitoring.sh`.
2. `helm template monitoring prometheus-community/kube-prometheus-stack --version "$KUBE_PROMETHEUS_STACK_CHART_VERSION" -f helm/values/ci/monitoring.yaml -n monitoring` renders zero `kube-state-metrics`/`prometheus-node-exporter` resources.
3. `make helm-lint` and `make manifests` both pass (full offline Helm template + kubeconform validation across both profiles, all nine charts) — run once at the end of this plan, not per-task, since each is a multi-minute network-fetching sweep.
4. `uv run pytest tests/policy/test_ci_invokes_make_only.py -q` passes — the modified `e2e-full.yml` still calls `make` only.
5. `uv run pytest tests/policy/test_values_profiles.py -q` (equivalently `make policy`) passes — the widened `_is_monitoring_enablement` predicate classifies `kubeStateMetrics.enabled`/`nodeExporter.enabled` as the existing "monitoring enablement" divergence axis instead of failing `test_profiles_diverge_only_on_permitted_axes` as an unclassified difference.
6. `grep -c 'helm_install\|wait_for_' scripts/stages/85-monitoring.sh` (comments filtered) is `0`; the same count in `scripts/monitoring-install.sh` is `5` (3 `helm_install` + 2 `wait_for_*`).
7. Manual read of `.github/workflows/e2e-full.yml`: the two new steps sit exactly where the old single step was, `make rebuild-from-raw` is untouched immediately after them.
</verification>

<success_criteria>
- `scripts/monitoring-install.sh` and `scripts/monitoring-teardown.sh` exist and are the single, shared source of the monitoring stack's install/uninstall logic for both the local (`85-monitoring.sh`) and CI (`observability-verify-ci`) paths.
- `make cluster-verify` is byte-identical in behavior to before this plan; `make cluster-slice-verify` and `make observability-verify-ci` are new, additive targets.
- `helm/values/ci/monitoring.yaml` disables `kubeStateMetrics`/`nodeExporter` with a `helm template` render proving zero coverage-losing resources are rendered, and `tests/policy/test_values_profiles.py::test_profiles_diverge_only_on_permitted_axes` passes against the widened `_is_monitoring_enablement` predicate.
- `.github/workflows/e2e-full.yml` stages the monitoring install/teardown strictly around the `tests/e2e/observability` window, per CONTEXT.md's locked decision, without touching `make rebuild-from-raw` or any other step.
- `tests/policy/test_ci_invokes_make_only.py` still passes.
- Explicitly NOT proven by this plan: whether the staggered, trimmed stack actually fits the CI node's ~3000m CPU budget under real, concurrent load. RESEARCH.md's own estimate (~2770m/3000m, ~8% margin) is a projection built from this session's prior live measurements, not a fresh live measurement of this exact staggered sequence. A live CI run (`e2e-full.yml` on a real push to `main`) is the only way to confirm this margin holds, and is explicitly deferred to a separate follow-up-3 task, per CONTEXT.md's own stated scope boundary.
</success_criteria>

<output>
Create `.planning/quick/260824-ayw-build-a-trimmed-ci-only-monitoring-stack/260824-ayw-SUMMARY.md` when done.
</output>
</content>
