# Trimmed CI-only monitoring stack, staggered for tests/e2e/observability - Research

**Researched:** 2026-08-24
**Domain:** CI orchestration mechanics + Helm/K8s sizing on an already-tight single-node kind CI cluster
**Confidence:** MEDIUM (mechanics HIGH; sizing math MEDIUM — grounded in this session's own live measurements, but no live re-run of the staggered sequence has happened yet)

## Summary

The staggering strategy is locked (CONTEXT.md). This research answers the remaining "how exactly"
questions using this repo's own conventions and its own prior live-measured numbers.

**Primary recommendation:** Add a new CI-only Makefile target `observability-verify-ci` that wraps
(1) `pytest tests/e2e/observability -q`, sandwiched between (2) a monitoring install and (3) a
monitoring teardown — reusing `scripts/stages/85-monitoring.sh`'s exact `helm_install`/`wait_for_*`
call shape by extracting its three `helm_install` calls into a small `scripts/monitoring-install.sh`
that both `85-monitoring.sh` (local) and this new CI path call, rather than duplicating the calls
inline in YAML. `e2e-full.yml` changes its single `make cluster-verify` step into three steps:
`pytest tests/e2e/cluster tests/e2e/slice -q` (a narrowed `cluster-verify`-shaped inline call),
`make observability-verify-ci`, then the existing `make rebuild-from-raw`. Grafana's
first-install secret race (85-monitoring.sh's own header comment) is confirmed NOT a risk here:
`e2e-full.yml` already runs `make vault-bootstrap` (which creates `grafana-alert-webhook`) several
steps before `cluster-verify` even starts. Teardown should be `helm uninstall` for all three
releases (not scale-to-zero) — simpler, matches this project's declarative-state convention, and
`rebuild-from-raw` needs the freed CPU, not a scaled-to-zero-but-still-PVC-and-Job-bearing release.
Sizing math (below) suggests the trimmed, staggered stack plausibly fits, but the margin is genuinely
thin and this is NOT a proven-safe conclusion — flag it for the planner as a real, disclosed risk
requiring one live CI run to confirm.

## User Constraints (from CONTEXT.md)

### Locked Decisions
- Stagger the monitoring stack: live only for the observability test window, not the whole
  120-minute `e2e-full.yml` job.
- Mechanism: split `e2e-full.yml`'s single `make cluster-verify` pytest invocation into two steps
  — (1) `tests/e2e/cluster tests/e2e/slice`, then (2) install monitoring, run
  `tests/e2e/observability` alone, then tear monitoring down before `make rebuild-from-raw`.
- `make cluster-verify` itself must NOT change behavior — still runs cluster+slice+observability
  together for local's persistent 3-node profile. Staggering is CI-workflow-specific orchestration
  only.
- Drop `kubeStateMetrics.enabled: false` / `nodeExporter.enabled: false` in
  `helm/values/ci/monitoring.yaml` — verified zero test-coverage loss (see Sources below).
- Do NOT propose a bigger GitHub runner class.

### Claude's Discretion
- Exact CI-workflow mechanics (new Makefile target vs. inline `run:` steps vs. wrapper script).
- Whether `85-monitoring.sh`'s unconditional `PROFILE=ci` skip changes shape.
- Full `helm uninstall` vs. scale-to-zero for teardown.
- Exact resource numbers for any further trimming beyond dropping kube-state-metrics/node-exporter.

### Deferred Ideas (OUT OF SCOPE)
- Re-litigating staggering-vs-whole-job or the kube-state-metrics/node-exporter drop.
- A live CI proof run (explicitly a separate, later follow-up-3 task).

## Phase Requirements

No formal REQ IDs were supplied for this quick task; the four numbered focus questions in the task
brief function as the requirement set and are addressed by the sections below in the same order.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Stagger monitoring install/teardown around one pytest sub-suite | CI workflow (`.github/workflows/e2e-full.yml`) | Makefile (new target) | Orchestration timing is a CI-only concern per CONTEXT.md; the shared `cluster-verify` target must stay untouched. |
| Install/teardown mechanics (helm calls, wait conditions) | Shell script (`scripts/`) | — | Must reuse `scripts/helm-install.sh`/`scripts/wait-for.sh` conventions, not reinvent them inline in YAML. |
| Resource sizing (drop 2 subcharts, verify CPU budget) | Helm values (`helm/values/ci/monitoring.yaml`) | kind cluster config (`kind/cluster-ci.yaml`) | Values file is the sizing lever; cluster-ci.yaml is the fixed ceiling it must fit under. |
| Test-surface scoping (which datasources/targets matter) | pytest fixtures (`tests/e2e/observability/conftest.py`) | — | Already-locked scope; research only confirms no untested surface gets trimmed. |

## 1. CI workflow/Makefile mechanics for staggering

### Recommended concrete design

**A. New script: `scripts/monitoring-install.sh` (extracted, not duplicated)**

`scripts/stages/85-monitoring.sh` (lines 95-115) currently inlines three `helm_install` calls plus
two `wait_for_*` calls directly in the stage script, guarded by `PROFILE=ci` skip at the top
(lines 81-84). Extract lines 95-115 verbatim into a new sourceable/standalone script
`scripts/monitoring-install.sh` that both:
- `scripts/stages/85-monitoring.sh` calls (for local's persistent-cluster path, unconditionally
  for `PROFILE=local`), and
- a new `scripts/monitoring-teardown.sh` companion + the new Makefile target call directly (for
  CI's staggered path).

This keeps the actual `helm_install`/`wait_for_*` call shape byte-identical between local and CI —
zero duplicated logic, and any future chart-version or wait-strategy change (e.g. a `hookOnly` fix)
only has to be made once. `85-monitoring.sh` keeps its `PROFILE=ci` skip exactly as-is (this
satisfies "Claude's Discretion" bullet 2 in the simplest available way: the stage script's shape
does NOT need to change — it stays the single "unconditionally skip for local's `cluster-up` when
PROFILE=ci" guard it already is, and the CI path calls the extracted script directly, never through
`85-monitoring.sh`).

**B. New Makefile target: `observability-verify-ci`**

```makefile
observability-verify-ci:        ## CICD-09 follow-up-2: install monitoring, run tests/e2e/observability alone, tear down [quick 260824-ayw]
	# CI-only counterpart to cluster-verify's own tests/e2e/observability slice
	# (D-19 unchanged for local — see cluster-verify's own comment). Staggered
	# so the trimmed monitoring stack is live only for this window, not the
	# whole e2e-full.yml job (this session's own CPU-contention findings).
	PROFILE=ci scripts/monitoring-install.sh
	$(RUN_CLUSTER) pytest tests/e2e/observability -q
	scripts/monitoring-teardown.sh
```

Rationale for a Makefile target over inline `run:` steps in the YAML: this project's own
`test_ci_invokes_make_only.py` (referenced by `chaos-verify`'s comment, CICD-02) establishes the
convention that CI workflows call `make <target>`, never raw `pytest`/`helm` commands directly —
an inline 3-line `run:` block in `e2e-full.yml` would violate that same policy gate. A Makefile
target is also directly reusable by a developer debugging this locally against a CI-profile cluster
(`PROFILE=ci make cluster-up && make observability-verify-ci`), matching `smoke-verify`'s own
"developer and workflow invoke the identical command" rationale.

**C. `e2e-full.yml` changes**

Replace the single "Run the full local E2E suite (make cluster-verify)" step (lines 118-119) with:

```yaml
      - name: Run cluster + slice E2E suite (observability deferred, staggered below)
        run: $(RUN_CLUSTER equivalent) pytest tests/e2e/cluster tests/e2e/slice -q
```

This is an inline pytest call, not a `make` call — a genuine, disclosed narrowing of the
CICD-02/`test_ci_invokes_make_only.py` "call by name" convention `cluster-verify`'s own comment
established, needed specifically because `cluster-verify` itself must not change (CONTEXT.md
locked decision) and no separate `cluster-slice-verify` Make target currently exists. **Two
options for the planner to pick between, both acceptable, neither pre-selected here:**
1. Add a second Makefile target (e.g. `cluster-slice-verify`) that runs
   `pytest tests/e2e/cluster tests/e2e/slice -q`, keeping `e2e-full.yml` a pure `make` caller —
   consistent with CICD-02, at the cost of a 3rd near-duplicate target alongside `cluster-verify`.
2. Call pytest inline in the workflow, accepting the CICD-02 narrowing as a documented exception
   (mirroring how `smoke-verify` already does its own inline pytest calls at lines 421/423 for the
   Vault/Kyverno sub-checks — this project already has precedent for pytest calls inside a
   Makefile target, just not inside the workflow YAML itself directly).

Given `test_ci_invokes_make_only.py` almost certainly greps for `run:` blocks containing `pytest`
inside `.github/workflows/*.yml` (not inside `Makefile`), **option 1 (a new
`cluster-slice-verify` Makefile target) is the safer, policy-clean choice** and is what this
research recommends. It costs one more three-line target, consistent with `smoke-verify`'s own
"deliberately narrower" precedent (Makefile line 348 comment).

Then add, immediately after it:
```yaml
      - name: Install trimmed monitoring, run tests/e2e/observability, tear down
        run: make observability-verify-ci
```
...positioned exactly where the old single step was, before the existing `make rebuild-from-raw`
step (lines 121-126, untouched).

### D. Teardown: `scripts/monitoring-teardown.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
helm_bin="${repo_root}/tools/bin/helm"
"${helm_bin}" uninstall monitoring tempo otel-collector -n monitoring --wait --timeout 3m || true
```

`helm uninstall --wait` blocks until all uninstalled resources (Pods, PVCs are NOT auto-deleted by
`helm uninstall` unless a chart's own `helm.sh/resource-policy` differs — the kube-prometheus-stack
PVCs are not chart-hook-owned, they are plain PVCs and survive `helm uninstall`) are removed. See
Pitfalls section below for the PVC/CRD leftover caveat and why it does not block `rebuild-from-raw`.

## 2. Grafana bootstrap dependency — CONFIRMED NOT a risk here

`85-monitoring.sh`'s own header comment (lines 36-53) documents the first-ever-`cluster-up` race:
`watcher` wait strategy would deadlock because Grafana's pod needs the `grafana-alert-webhook`
Secret, which does not exist until `make vault-bootstrap` runs. That is why `85-monitoring.sh`
uses `hookOnly` for the kube-prometheus-stack release and does not wait on Grafana at all.

**Confirmed via `e2e-full.yml` (lines 90-112):** `make vault-unseal` and `make vault-bootstrap`
already run as their own dedicated step, well before `make cluster-verify` is invoked. By the time
the new staggered `observability-verify-ci` target runs `monitoring-install.sh` (which happens even
later, after `cluster-slice-verify`), the `grafana-alert-webhook` Secret has existed for the entire
duration of the job already. **The first-install race this comment describes cannot occur in the
staggered CI path** — this is a materially different bootstrap order than a genuinely first-ever
local `cluster-up` (where monitoring installs before `vault-bootstrap` has ever run). No mitigation
needed; the existing `hookOnly` strategy for the kube-prometheus-stack release is sufficient as-is,
and `wait_for_deploy_available monitoring monitoring-kube-prometheus-operator` will also observe
Grafana come up cleanly on the first reconcile (not just eventually, via retry) since its Secret
dependency is already satisfied.

## 3. Sizing/budget math (honest estimate, not a live-tested guarantee)

**Fixed ceiling** (`kind/cluster-ci.yaml` lines 29-30, its own header-derived numbers):
`allocatable_target_cpu = 3000m` on the single CI node (4 CPU runner minus 1 CPU
systemReserved+kubeReserved).

**Live-measured baseline WITH the pre-trim monitoring stack running for the whole job**
(`scripts/stages/85-monitoring.sh` lines 68-70, this session's own throwaway-PR diagnosis):
`2810m/3000m (93%)` allocated **before any burst** — i.e. steady-state, with Airflow/CNPG×2/MinIO/
Vault/Kyverno/ingress-nginx/otel-collector/tempo/kube-prometheus-stack (incl. kube-state-metrics
+ node-exporter) all co-resident and idle.

**Trimmed monitoring stack's own request total** (summing `helm/values/ci/{monitoring,tempo,
otel-collector}.yaml`'s current `resources.requests.cpu`, with kube-state-metrics and node-exporter
now disabled):

| Component | requests.cpu |
|---|---|
| grafana (main container) | 50m |
| grafana sidecar | 10m |
| prometheusOperator | 20m |
| prometheus (prometheusSpec) | 100m |
| otel-collector | 100m |
| tempo | 100m |
| **Total (running Pods)** | **~380m** |

(`grafana.initChownData`/`downloadDashboards` at 10m each are init containers — sequential, not
concurrent with the above; `admissionWebhooks.patch` at 10m is a one-shot Job that completes and
exits before steady state.) Dropping `kubeStateMetrics` (20m) and `nodeExporter` (20m) removes 40m
versus the pre-trim stack that produced the 2810m/3000m figure — the pre-trim total was
approximately `380m + 40m = 420m`, so the un-staggered pre-trim baseline-minus-monitoring was
roughly `2810m − 420m ≈ 2390m` (all non-monitoring CI workloads, steady state, before any DAG-task
burst).

**Estimate for the staggered window:** `2390m (persistent workloads) + 380m (trimmed monitoring)
≈ 2770m / 3000m (~92%)`, evaluated at the point in the job where `tests/e2e/slice`'s heavy 2-year
DAG-trigger sweep has already finished (per CONTEXT.md's own stated pytest arg ordering) and no
KubernetesExecutor worker/KPO pods should be actively churning. This is **directionally safer**
than the 93% figure that produced the CrashLoopBackOff (that 93% already included the FULL,
untrimmed monitoring stack for the *entire* job duration, concurrent with active DAG bursts later
in the run) — but the margin over the 3000m ceiling is still only ~230m (~8%), and:

- This estimate ignores real-world variance in the "persistent workloads" 2390m figure across CI
  runs (GitHub-hosted runner performance is not perfectly reproducible run-to-run).
- It does not account for transient spikes during the `helm upgrade --install` of the three charts
  themselves (image pulls, CNPG/webhook reconciliation activity, `admission-create`/`admission-patch`
  Job execution — brief but real CPU consumption on top of steady-state requests).
- Requests, not actual usage, drive scheduling admission — but the original CrashLoopBackOff was
  diagnosed as a *real* CPU-starvation cascade (probe timeouts under actual contention), not merely
  a scheduling-admission failure, so the requests-based estimate above is a proxy, not a guarantee.
- `webhook_receiver` fixture (`tests/e2e/observability/conftest.py` lines 296-337) launches one more
  throwaway Pod (10m request) during `test_alert_webhook_delivery.py` — negligible, but real.

**Explicit risk statement for the planner:** this sizing estimate suggests the staggered, trimmed
approach *plausibly* fits without reproducing the CrashLoopBackOff, but an ~8% margin against a
budget that was previously measured at 93%+7% utilization under similar conditions is not a wide
safety margin. Treat this as MEDIUM confidence, not a proof. The planner should not gate phase
completion on a live CI green run within this quick task's own scope (that is explicitly deferred,
per CONTEXT.md, to a separate follow-up-3 task) — but should flag this margin explicitly in the
plan's own risk/assumptions, and the follow-up-3 live-verification task should capture a
`kubectl describe node`/`kubectl top pod` snapshot during the observability window as its first
diagnostic step if it fails.

## 4. Common pitfalls

### Pitfall 1: `hookOnly` wait strategy already handles the admission-webhook Jobs correctly
**What goes wrong (if changed):** Switching kube-prometheus-stack to the default `watcher` strategy
would deadlock exactly as `85-monitoring.sh`'s header comment (lines 31-53) already documents for
the local first-install case — `watcher` waits for the chart's own Deployments (incl. Grafana) to
report Ready *before* running the `admission-create`/`admission-patch` post-install hooks.
**Prevention:** Keep the extracted `monitoring-install.sh` byte-identical to `85-monitoring.sh`'s
existing `helm_install monitoring prometheus-community/kube-prometheus-stack monitoring
KUBE_PROMETHEUS_STACK_CHART_VERSION monitoring hookOnly` call (line 110-111) — do not "simplify" it
to the default strategy during extraction.

### Pitfall 2: PVCs and CRDs survive `helm uninstall`
**What goes wrong:** `helm uninstall` does not delete PersistentVolumeClaims or CRDs the chart
created (kube-prometheus-stack installs CRDs like `prometheuses.monitoring.coreos.com`,
`alertmanagers.monitoring.coreos.com`, etc., and Prometheus/Grafana/Tempo each have their own PVC
via `storageSpec`/`persistence.enabled: true` in the CI values files). On a genuinely ephemeral
`kind` cluster (torn down at job end regardless), this is harmless — nothing persists past the
runner's lifetime — but it means the freed CPU from `helm uninstall` is real (Pods are gone,
requests are released) even though the freed *disk* is not, and a `helm upgrade --install` of the
SAME release name later in the same job (there isn't one here — this only installs once per job)
would need `--reuse-values` or a clean PVC to avoid stale-data surprises. Not a blocker for this
task; worth one sentence in the plan so a future author doesn't assume `helm uninstall` is a full
clean-slate reset.
**Prevention:** No action needed for this task's scope — `rebuild-from-raw` only needs CPU headroom
back, which `helm uninstall --wait` (Pod termination is what frees `Allocated resources` on the
node) genuinely provides.

### Pitfall 3: Port-forward fixtures are session-scoped but process-scoped to the observability pytest run
**What goes wrong (if NOT handled):** If a plan were to run `tests/e2e/observability` in the SAME
pytest process/invocation as `tests/e2e/cluster`/`tests/e2e/slice` (i.e., accidentally reverting to
`cluster-verify`'s combined invocation shape inside the staggered target), the `grafana_addr`/
`vault_addr` session-scoped port-forward fixtures (`tests/e2e/observability/conftest.py` lines
162-176, 230-248) would only start being requested once the first observability test actually runs
— by which point monitoring might not be installed yet if install/teardown timing were reordered
incorrectly.
**Prevention:** The recommended design already avoids this — `observability-verify-ci` installs
monitoring, THEN runs `pytest tests/e2e/observability -q` as its own separate process (matching the
locked CONTEXT.md decision), THEN tears down. Because it's a genuinely separate `pytest` invocation,
all fixtures (including the session-scoped port-forwards) are guaranteed torn down (their
`try/finally` `proc.terminate()`/`proc.wait()` blocks run at that process's own session teardown)
before `monitoring-teardown.sh` runs. No explicit synchronization needed beyond "the pytest command
exits before the next Makefile line runs" (which `set -e`/sequential `RUN:` recipe lines already
guarantee).

### Pitfall 4: don't let the new `cluster-slice-verify` (or equivalent) target silently diverge from `cluster-verify`'s own comment contract
`cluster-verify`'s own header comment (Makefile lines 328-346) explicitly frames "tests/e2e/slice
joins... tests/e2e/observability joins the same way... one target so this collects the whole
suite" as an intentional design statement. Adding a second, narrower target that peels off
`tests/e2e/cluster tests/e2e/slice` needs its own comment cross-referencing `cluster-verify`
(mirroring how `smoke-verify`'s own comment already explains its relationship to `cluster-verify`)
so a future reader does not conclude the two targets have silently drifted apart by accident.

## Sources

### Primary (HIGH confidence — read directly this session)
- `scripts/stages/85-monitoring.sh` (full file) — `helm_install`/`wait_for_*` call shape, `hookOnly`
  rationale, Grafana first-install race, the live 2810m/3000m CrashLoopBackOff diagnosis.
- `scripts/helm-install.sh`, `scripts/wait-for.sh` — helper function contracts.
- `Makefile` lines 145-146 (`install-cluster`), 257-267 (`rebuild-from-raw`), 328-436
  (`cluster-verify`, `smoke-verify`, `chaos-verify`) — existing target conventions.
- `.github/workflows/e2e-full.yml` (full file) — exact current step ordering, confirming
  `vault-bootstrap` runs before `cluster-verify`.
- `kind/cluster-ci.yaml` — `allocatable_target_cpu = 3000m` derivation, single-node topology.
- `helm/values/ci/{monitoring,tempo,otel-collector}.yaml` — current CPU requests per component.
- `helm/versions.env` — pinned chart versions (`OTEL_COLLECTOR_CHART_VERSION=0.169.0`,
  `TEMPO_CHART_VERSION=2.2.4`, `KUBE_PROMETHEUS_STACK_CHART_VERSION=88.2.0`).
- `.planning/phases/11-ci-cd-completion-operations/deferred-items.md` — "PARTIALLY RESOLVED
  (2026-08-24)" (8 fixed CI-portability bugs) and "Plan 11-05" (original observability-cannot-pass
  finding, two previously-weighed remediation options).
- `.planning/quick/260817-rvq-.../260817-rvq-SUMMARY.md` — live-measured actual usage (prometheus
  ~23.6m, tempo/otel-collector ~3-4m vs 100m requests) and the local-cluster before/after CPU
  measurements from the same trim applied to the `local` profile.
- `tests/e2e/observability/conftest.py`, `test_grafana_provisioning.py` — confirmed exact
  datasource/surface usage (`analytics-postgres`, `prometheus`, `tempo` only; zero kube-state-metrics
  or node-exporter references anywhere in the package).

### Tertiary (LOW confidence — inferred, not directly verified this session)
- The assumption that `test_ci_invokes_make_only.py` scans `.github/workflows/*.yml` `run:` blocks
  specifically (not `Makefile` recipe lines) for bare `pytest`/`helm` calls — inferred from
  `chaos-verify`'s own comment ("invoked through make... not a direct pytest call in the workflow")
  but the test file itself was not read this session. **The planner should read
  `tests/policy/test_ci_invokes_make_only.py` directly before finalizing whether option 1 or 2 in
  section 1.C is required**, rather than trusting this inference.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `test_ci_invokes_make_only.py` blocks bare `pytest`/`helm` calls in workflow YAML `run:` blocks specifically, not in Makefile recipes | 1.C | If wrong, inline `run:` pytest calls in `e2e-full.yml` would be simpler and the extra `cluster-slice-verify` target unnecessary — low cost either way, but worth a 30-second file read before the plan locks the approach. |
| A2 | The ~2390m "persistent workloads" baseline (derived by subtracting the pre-trim monitoring stack's ~420m from the observed 2810m) is stable enough to project onto the staggered window | 3 | If GitHub Actions runner variance or a slice-sweep DAG-task residual is larger than assumed, the ~92% projected utilization could still tip into CrashLoopBackOff territory — this is why the research explicitly flags MEDIUM confidence and recommends a live follow-up run rather than declaring success. |

## Open Questions

1. **Does `test_ci_invokes_make_only.py` actually scan Makefile recipes, or only workflow YAML?**
   - What we know: `chaos-verify`'s own comment implies the policy is scoped to workflow YAML.
   - What's unclear: whether a bare `pytest tests/e2e/cluster tests/e2e/slice -q` line INSIDE a new
     Makefile target itself would also trip a stricter version of the same policy.
   - Recommendation: planner reads the policy test file directly as a first planning step before
     committing to the `cluster-slice-verify` Makefile-target design in section 1.C.

2. **Real transient CPU spikes during the 3 `helm upgrade --install` calls themselves.**
   - What we know: steady-state requests total ~380m for the trimmed stack.
   - What's unclear: the peak transient CPU during CRD registration, admission-webhook Job
     execution, and image pull/unpack for 3 charts happening back-to-back on an already-92%-loaded
     node.
   - Recommendation: not resolvable without a live run — explicitly the scope of the deferred
     follow-up-3 task.

## Metadata

**Confidence breakdown:**
- Mechanics (Makefile/workflow/script structure): HIGH — grounded directly in this repo's existing,
  read conventions (`helm-install.sh`, `wait-for.sh`, `85-monitoring.sh`, `cluster-verify`/
  `smoke-verify`/`chaos-verify` comments).
- Grafana bootstrap-race question: HIGH — directly confirmed by reading `e2e-full.yml`'s actual
  step ordering.
- Sizing/budget math: MEDIUM — built from this session's own live-measured numbers, but is an
  arithmetic projection, not a fresh live measurement of the staggered scenario itself.

**Research date:** 2026-08-24
**Valid until:** Effectively tied to the next live CI run against this cluster topology — treat as
stale the moment `kind/cluster-ci.yaml`, `helm/values/ci/*.yaml` resource requests, or the CI
runner's own advertised CPU/memory change.
