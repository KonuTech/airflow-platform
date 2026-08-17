---
phase: quick-260817-trim-monitoring-stack-helm-values-cpu
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - helm/values/local/monitoring.yaml
  - helm/values/local/tempo.yaml
  - helm/values/local/otel-collector.yaml
autonomous: true
requirements:
  - "STATE.md-documented CPU over-provisioning: kind worker nodes' ~700-800m real headroom threshold (root-caused live in prior quick tasks 260817-mvp/260817-oqy) makes monitoring-stack CPU requests a live scheduling-starvation risk; this task frees real schedulable CPU by trimming local-profile monitoring CPU requests down to helm/values/ci/*.yaml's already-vetted, already-committed values, backed by live-measured Prometheus usage data (all components 1-10% of their current requested CPU)."

must_haves:
  truths:
    - "helm/values/local/monitoring.yaml's grafana, grafana.sidecar, kube-state-metrics, prometheus-node-exporter, prometheusOperator, and prometheus.prometheusSpec CPU requests exactly match helm/values/ci/monitoring.yaml's own values for the same keys"
    - "helm/values/local/tempo.yaml's and helm/values/local/otel-collector.yaml's resources.requests.cpu exactly match their ci counterparts"
    - "No memory value, no limits value, no initChownData/downloadDashboards/admissionWebhooks.patch value, and no file outside these three is touched"
    - "Both the local and ci Helm values profiles still render successfully (helm lint + helm template + kubeconform -strict) after the trim"
    - "The repository's own D-06 divergence-axis policy test (tests/policy/test_values_profiles.py) still passes -- these are resource-sizing values, an explicitly permitted axis, and local/ci becoming equal on this axis is not a violation"
  artifacts:
    - path: "helm/values/local/monitoring.yaml"
      provides: "Trimmed CPU requests for grafana (50m), grafana.sidecar (10m), kube-state-metrics (20m), prometheus-node-exporter (20m), prometheusOperator (20m), prometheus.prometheusSpec (100m) -- all matching ci/monitoring.yaml"
      contains: "cpu: 50m"
    - path: "helm/values/local/tempo.yaml"
      provides: "Trimmed CPU request (100m) matching ci/tempo.yaml"
      contains: "cpu: 100m"
    - path: "helm/values/local/otel-collector.yaml"
      provides: "Trimmed CPU request (100m) matching ci/otel-collector.yaml"
      contains: "cpu: 100m"
  key_links:
    - from: "helm/values/local/monitoring.yaml"
      to: "helm/values/ci/monitoring.yaml"
      via: "identical CPU request values per component, reused as an already-vetted precedent rather than an arbitrary new number"
      pattern: "cpu: (50m|10m|20m|100m)"
    - from: "helm/values/local/{tempo,otel-collector}.yaml"
      to: "helm/values/ci/{tempo,otel-collector}.yaml"
      via: "identical resources.requests.cpu: 100m value"
      pattern: "cpu: 100m"
---

<objective>
Trim six CPU `requests` values in `helm/values/local/monitoring.yaml` and one
each in `helm/values/local/tempo.yaml` / `helm/values/local/otel-collector.yaml`
down to `helm/values/ci/*.yaml`'s already-committed, already-reviewed values
for the exact same components, to relieve real CPU over-provisioning on kind
cluster worker nodes.

Purpose: the orchestrator's live Prometheus query on this cluster measured
every monitoring-namespace container's actual 15m-average CPU usage at 1-10%
of its currently-requested CPU (e.g. tempo ~3.1m actual vs 250m requested).
`helm/values/ci/*.yaml` already defines a smaller-but-still-generous CPU
request profile for these identical components (CI runners are 4 CPU total,
per CLAUDE.md), already committed and exercised in CI. Reusing those exact
numbers for the local profile is not a new, unreviewed sizing decision -- it
reuses an existing in-repo precedent, and every reused value still leaves
several-times headroom over the measured actual usage. This frees ~305m on
`airflow-platform-worker` and ~330m on `airflow-platform-worker2`, meaningful
against STATE.md's documented ~700-800m real-headroom threshold that has
twice caused live `FailedScheduling` cascades this session (`260817-mvp`).

Output: `helm/values/local/monitoring.yaml`, `helm/values/local/tempo.yaml`,
and `helm/values/local/otel-collector.yaml` updated with trimmed CPU
`requests` and updated rationale comments; no `helm upgrade`/live cluster
change (this is a values-file-only change, applied on a future deploy).
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/STATE.md
@helm/values/local/monitoring.yaml
@helm/values/local/tempo.yaml
@helm/values/local/otel-collector.yaml
@helm/values/ci/monitoring.yaml
@helm/values/ci/tempo.yaml
@helm/values/ci/otel-collector.yaml
@tests/policy/test_values_profiles.py

<pre_investigated_measurements>
Orchestrator-measured live 15m-average CPU usage vs current LOCAL profile
requests (Prometheus query against this cluster, this session):
- tempo: ~3.1m actual vs 250m requested (1.2%)
- otel-collector: ~3.6m actual vs 250m requested (1.4%)
- prometheus: ~23.6m actual vs 250m requested (9.4%)
- grafana (3 containers combined, main+2 sidecars): ~12.2m actual vs 150m
  requested (100m main + 25m sidecar)
- kube-prometheus-operator: ~2.6m actual vs 50m requested
- kube-state-metrics: ~2.2m actual vs 50m requested
- node-exporters (x3 daemonset instances): ~2.7m combined actual vs 50m/instance

`helm/values/ci/monitoring.yaml`, `helm/values/ci/tempo.yaml`,
`helm/values/ci/otel-collector.yaml` already define the exact target values
below for these same components -- these are the numbers to copy, not
independently derived.

`tests/policy/test_values_profiles.py`'s `_is_resource_sizing()` names every
`resources` key as a permitted local/ci divergence axis (D-06). Local and ci
becoming EQUAL on this axis (rather than merely differing) still passes --
the test only flags UNEXPECTED differences, never flags sameness.
</pre_investigated_measurements>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Trim CPU requests in all three local monitoring-stack values files</name>
  <files>helm/values/local/monitoring.yaml, helm/values/local/tempo.yaml, helm/values/local/otel-collector.yaml</files>
  <action>
    Edit ONLY the `requests.cpu` values named below, in each of the three
    files. Do not touch any `memory` value, any `limits` value, the
    `initChownData`/`downloadDashboards` init-container blocks, the
    `admissionWebhooks.patch` block, `retention`, `storageSpec`/`persistence`
    sizing, or any other key. For each edited block, replace the existing
    rationale comment (where one exists) with a short comment explaining the
    new value is sized to match `helm/values/ci/*.yaml`'s own already-vetted
    number for that same key, backed by the live-measured actual usage from
    the `<pre_investigated_measurements>` context block above -- do not
    delete unrelated historical context (e.g. the "holds no persistent
    state" note in otel-collector.yaml, or the memory-sizing rationale) that
    still explains something the edit does not change; only the CPU-request
    rationale itself should read as stale after this edit.

    In `helm/values/local/monitoring.yaml`:
    1. `grafana.resources.requests.cpu` (currently `100m`, line ~133) -> `50m`.
    2. `grafana.sidecar.resources.requests.cpu` (currently `25m`, line ~150) -> `10m`.
    3. `kube-state-metrics.resources.requests.cpu` (currently `50m`, line ~692) -> `20m`.
    4. `prometheus-node-exporter.resources.requests.cpu` (currently `50m`, line ~701) -> `20m`.
    5. `prometheusOperator.resources.requests.cpu` (currently `50m`, line ~710) -> `20m` -- leave the sibling `admissionWebhooks.patch.resources.requests.cpu` (`20m`, line ~724) untouched; add a one-line note that the patch Job is intentionally excluded.
    6. `prometheus.prometheusSpec.resources.requests.cpu` (currently `250m`, line ~753) -> `100m`.

    In `helm/values/local/tempo.yaml`:
    7. `resources.requests.cpu` (currently `250m`, line ~32) -> `100m`.

    In `helm/values/local/otel-collector.yaml`:
    8. `resources.requests.cpu` (currently `250m`, line ~81) -> `100m`.

    Every new value must be byte-identical to the corresponding key in
    `helm/values/ci/monitoring.yaml`, `helm/values/ci/tempo.yaml`, and
    `helm/values/ci/otel-collector.yaml` respectively -- confirm this with
    the automated verify command below (a YAML-level key-path comparison
    against the ci files), not just a visual read of the numbers in this
    plan.
  </action>
  <verify>
    <automated>cd /home/konutec/projects/airflow-platform && python3 -c "
import yaml

def get(d, path):
    for k in path.split('.'):
        d = d[k]
    return d

checks = [
    ('helm/values/local/monitoring.yaml', 'helm/values/ci/monitoring.yaml', 'grafana.resources.requests.cpu'),
    ('helm/values/local/monitoring.yaml', 'helm/values/ci/monitoring.yaml', 'grafana.sidecar.resources.requests.cpu'),
    ('helm/values/local/monitoring.yaml', 'helm/values/ci/monitoring.yaml', 'kube-state-metrics.resources.requests.cpu'),
    ('helm/values/local/monitoring.yaml', 'helm/values/ci/monitoring.yaml', 'prometheus-node-exporter.resources.requests.cpu'),
    ('helm/values/local/monitoring.yaml', 'helm/values/ci/monitoring.yaml', 'prometheusOperator.resources.requests.cpu'),
    ('helm/values/local/monitoring.yaml', 'helm/values/ci/monitoring.yaml', 'prometheus.prometheusSpec.resources.requests.cpu'),
    ('helm/values/local/tempo.yaml', 'helm/values/ci/tempo.yaml', 'tempo.resources.requests.cpu'),
    ('helm/values/local/otel-collector.yaml', 'helm/values/ci/otel-collector.yaml', 'resources.requests.cpu'),
]
cache = {}
def load(p):
    if p not in cache:
        cache[p] = yaml.safe_load(open(p))
    return cache[p]

failures = []
for local_path, ci_path, key in checks:
    lv = get(load(local_path), key)
    cv = get(load(ci_path), key)
    status = 'OK' if lv == cv else 'MISMATCH'
    if lv != cv:
        failures.append(f'{local_path}::{key} = {lv!r} != {ci_path}::{key} = {cv!r}')
    print(f'{status}: {local_path}::{key} = {lv!r}')

# admissionWebhooks.patch must be untouched (still 20m, unrelated to this trim)
patch_cpu = get(load('helm/values/local/monitoring.yaml'), 'prometheusOperator.admissionWebhooks.patch.resources.requests.cpu')
print(f'untouched admissionWebhooks.patch cpu: {patch_cpu!r} (expected 20m)')
if patch_cpu != '20m':
    failures.append(f'admissionWebhooks.patch.resources.requests.cpu unexpectedly changed to {patch_cpu!r}')

if failures:
    print('FAILURES:')
    for f in failures:
        print(' -', f)
    raise SystemExit(1)
print('all local CPU requests match their ci precedent; admissionWebhooks.patch untouched')
"
    </automated>
  </verify>
  <done>All eight trimmed CPU `requests` values in the three local values files are byte-identical to their `helm/values/ci/*.yaml` counterparts; `prometheusOperator.admissionWebhooks.patch`'s CPU request is unchanged at `20m`; no memory or limits value differs from before the edit.</done>
</task>

<task type="auto">
  <name>Task 2: Static render/lint validation and D-06 policy regression check (no live cluster)</name>
  <files>helm/values/local/monitoring.yaml, helm/values/local/tempo.yaml, helm/values/local/otel-collector.yaml</files>
  <action>
    Prove the edited values files are still structurally valid Helm input,
    without deploying anything or touching the live kind cluster. Run `make
    helm-lint` (pulls the pinned chart versions into a throwaway directory
    and runs `helm lint` against both the `local` and `ci` values profile for
    all eight pinned charts, including `monitoring`, `tempo`, and
    `otel-collector`) and `make manifests` (renders every chart with `helm
    template` against both profiles and validates the output with
    `kubeconform -strict` -- still no cluster contact, pure client-side
    templating). Both targets exit non-zero on any lint/render/schema
    failure. Then run `make policy` (offline `tests/policy/*` suite,
    including `tests/policy/test_values_profiles.py`'s D-06 divergence-axis
    check) to confirm the trim did not introduce an unclassified local/ci
    divergence -- since `resources` is an explicitly permitted axis and this
    change makes local and ci EQUAL rather than merely differing on that
    axis, this must pass with no changes needed to the test file itself. Do
    NOT run `helm upgrade`, `kubectl apply`, or anything else that mutates
    the live cluster -- this task is static validation only, per this quick
    task's explicit scope boundary.
  </action>
  <verify>
    <automated>cd /home/konutec/projects/airflow-platform && make helm-lint && make manifests && make policy</automated>
  </verify>
  <done>`make helm-lint` and `make manifests` both exit 0 for all eight pinned charts against both the `local` and `ci` values profiles (helm lint clean, `helm template` renders, kubeconform -strict passes) with the trimmed CPU values in place; `make policy` passes, including `tests/policy/test_values_profiles.py`'s D-06 divergence-axis check, with no test-file changes required. No `kubectl`/`helm upgrade` command touching the live cluster was run.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|--------------|
| Helm values file -> Kubernetes scheduler request | `resources.requests.cpu` in these three values files becomes the exact CPU quantity the Kubernetes scheduler reserves for each monitoring-stack pod on the next `helm upgrade`; under-sizing it risks CPU throttling for an observability workload, over-sizing it (the pre-existing state) starves scheduling capacity for every OTHER pod on the same node, including the ETL data-path pods this platform exists to run. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|------------------|
| T-quick-01 | Denial of Service (the problem this task fixes) | Every monitoring-namespace pod's CPU `requests` in `helm/values/local/{monitoring,tempo,otel-collector}.yaml` | mitigate | Trim CPU requests to `helm/values/ci/*.yaml`'s already-committed, already-reviewed values -- not an arbitrary new number -- freeing ~305-330m of real schedulable CPU per worker node, directly relieving the `FailedScheduling`/`Insufficient cpu` cascade pattern STATE.md already documents recurring against ETL task pods (`260817-mvp`). |
| T-quick-02 | Denial of Service (residual, accepted tradeoff) | Same trimmed CPU requests, throughput dimension for the monitoring stack itself | accept | Every trimmed value still carries several-times headroom over this session's own live-measured actual usage (e.g. prometheus: 100m request vs ~23.6m measured, a ~4x margin; tempo/otel-collector: 100m vs ~3-4m measured, a >25x margin). Residual risk of CPU throttling under an unusually large observability burst is judged low and non-critical-path: this is the observability stack, not the ETL data path this platform's core value depends on, and the exact same request/limit ratio (limits unchanged) still allows CPU bursting above the request when the node has spare capacity. |
| T-quick-03 | Tampering (accepted, no code path affected) | These three YAML files are Helm values, not executable code or a package-manager install target | accept | Pure declarative resource-sizing edit with no new dependency, no new package install, no new external service integration -- the npm/pip/cargo package-legitimacy gate does not apply to this change. |
</threat_model>

<verification>
1. Task 1's automated YAML key-path comparison confirms all eight trimmed local CPU requests are byte-identical to their `helm/values/ci/*.yaml` counterparts, and `admissionWebhooks.patch`'s CPU request is untouched.
2. Task 2's `make helm-lint && make manifests && make policy` all exit 0 -- both values profiles still render and lint cleanly (client-side only, no live cluster contact), and the repository's own D-06 divergence-axis policy test still passes.
3. `git diff --stat helm/values/local/monitoring.yaml helm/values/local/tempo.yaml helm/values/local/otel-collector.yaml` shows changes confined to these three files, with no unrelated hunk (spot-check: no `memory:` or `limits:` line appears in the diff's added/removed lines).
</verification>

<success_criteria>
- `helm/values/local/monitoring.yaml`'s six named CPU requests, `helm/values/local/tempo.yaml`'s one, and `helm/values/local/otel-collector.yaml`'s one are all trimmed to exactly match `helm/values/ci/*.yaml`'s already-vetted values for the same keys.
- No memory value, no limits value, no init-container/admission-webhook value, and no file outside these three is touched.
- Both Helm values profiles still render and lint cleanly with no live-cluster interaction (`make helm-lint`, `make manifests`).
- `tests/policy/test_values_profiles.py`'s D-06 divergence-axis check still passes unmodified.
- Real schedulable CPU headroom on `airflow-platform-worker`/`airflow-platform-worker2` increases by roughly 305m/330m respectively once this values change is next deployed (deployment itself is out of this quick task's scope, per the task description's explicit boundary).
</success_criteria>

<output>
Create `.planning/quick/260817-rvq-trim-monitoring-stack-helm-values-cpu-re/260817-rvq-SUMMARY.md` when done
</output>
