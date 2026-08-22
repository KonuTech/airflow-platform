# Phase 11: CI/CD Completion & Operations - Pattern Map

**Mapped:** 2026-08-22
**Files analyzed:** 27 new/modified files (grouped into 21 pattern-assignment units)
**Analogs found:** 26 / 27 (1 file — `.trivyignore` — has no analog by design, D-07)

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `.github/workflows/publish.yml` | config (workflow) | event-driven | `.github/workflows/ci.yml` | role-match |
| `.github/workflows/e2e-smoke.yml` (or job) | test | event-driven | `.github/workflows/ci.yml` | role-match |
| `.github/workflows/e2e-full.yml` (or job) | test | event-driven | `.github/workflows/ci.yml` | role-match |
| `.github/workflows/e2e-chaos.yml` | test | event-driven | `.github/workflows/ci.yml` | role-match |
| `.github/workflows/ghcr-cleanup.yml` | utility | event-driven | `.github/workflows/ci.yml` | role-match |
| `helm/values/local/kyverno.yaml` | config | batch | `helm/values/local/cnpg-operator.yaml` | exact |
| `helm/values/ci/kyverno.yaml` | config | batch | `helm/values/ci/cnpg-operator.yaml`, `helm/values/ci/minio.yaml` | exact |
| `kubernetes/kyverno-policy.yaml` | config | batch | `kubernetes/rbac-etl.yaml`, `kubernetes/namespaces.yaml` | exact |
| `scripts/stages/25-kyverno.sh` | utility (stage script) | batch | `scripts/stages/40-cnpg-operator.sh` | exact |
| `scripts/stages/26-kyverno-policy.sh` | utility (stage script) | batch | `scripts/stages/75-etl.sh` | exact |
| `helm/versions.env` (extend) | config | batch | self | exact |
| `docs/runbooks/*.md` (18 files) | doc | N/A | `.planning/debug/resolved/*.md` (content), `docs/ci-branch-protection.md` (form) | role-match |
| `docs/adr/0011-raw-immutability-iam-not-worm.md` (optional) | doc | N/A | `docs/adr/0010-dbt-silver-layer-boundary.md`, `docs/adr/0000-template.md` | exact |
| `airflow/dags/platform_retention.py` | controller (DAG) | batch | `airflow/dags/smoke_kubernetes_pod.py`, `airflow/dags/csv_ingest_customers.py` | role-match |
| `configs/datasets/customers.yaml` / `orders.yaml` (extend, `retention:` block) | config | CRUD | self (`scd:`/`freshness:` blocks in same file) | exact |
| `packages/dataplat/src/dataplat/config/model.py` (extend, `RetentionConfig`) | model | CRUD | self (`FreshnessConfig`/`ScdConfig` classes) | exact |
| `Makefile` (extend: `rebuild-from-raw`, rollback target) | utility | batch | self (`cluster-rebuild`, `migrate-analytics`, `image-csv-processor`) | exact |
| `packages/dataplat/src/dataplat/pipeline/run.py` (extend `_table_checksum`) | service | transform | self (existing function, same file) | exact |
| New retention service module (e.g. `packages/dataplat/src/dataplat/retention/policy.py`) | service | batch | `packages/dataplat/src/dataplat/scd/delete_detection.py` (`MassDeleteCircuitBreaker`) | role-match |
| New rebuild-reconciliation module (e.g. `packages/dataplat/src/dataplat/pipeline/rebuild_reconciliation.py`) | service | transform | `pipeline/run.py::_compute_silver_gold_reconciliation` + `metadata/repository.py::record_reconciliation` | role-match |
| `tests/e2e/chaos/__init__.py`, `conftest.py`, `test_*.py` (11 scenario files) | test | event-driven | `tests/e2e/slice/test_pod_kill_retry.py`, `tests/e2e/vault/test_unseal_survives_restart.py` | role-match |
| `tests/e2e/cluster/test_kyverno_admission.py` | test | request-response | `tests/e2e/cluster/test_minio_buckets.py`, `tests/e2e/vault/test_positive_auth.py` + `test_negative_auth.py` | exact |
| `tests/e2e/slice/test_rebuild_from_raw.py` | test | batch | `tests/e2e/slice/test_smoke_and_idempotency.py`, `tests/e2e/slice/test_backfill_2year_sweep.py` | role-match |
| `tests/unit/test_retention_*.py` | test | batch | `tests/unit/validate/test_circuit_breaker.py` | exact |
| `tests/dagtest/test_platform_retention_dagrun.py` | test | batch | `tests/dagtest/test_backfill_dagrun.py` | exact |
| `tests/policy/test_supply_chain_guards.py` (extend: Kyverno image-tag agreement, trivy invocation check) | test | transform | self (`test_every_image_tag_agrees_with_versions_env`) | exact |
| `.trivyignore` (conditional, D-07) | config | N/A | none | no analog |

## Pattern Assignments

### `.github/workflows/publish.yml`, `e2e-smoke.yml`, `e2e-full.yml`, `e2e-chaos.yml`, `ghcr-cleanup.yml` (config/test, event-driven)

**Analog:** `.github/workflows/ci.yml` (the ONLY workflow file in this repository today — 161 lines, read in full)

This is the single most important analog in the whole phase: every convention a new workflow file must follow is already established here, and at least two existing tests (`tests/policy/test_supply_chain_guards.py`, `tests/policy/test_workflow_secrets.py`) already grep workflow YAML for these exact shapes.

**Naming + trigger pattern** (lines 1-13):
```yaml
# The workflow name is load-bearing, not cosmetic: plan 01-09 selects this
# workflow by the exact string "CI" when it reads a run conclusion for the
# branch-protection rule.
name: CI

# `pull_request`, never `pull_request_target`: the target variant runs in the
# context of the base repository and would expose repository secrets to a fork.
on:
  pull_request:
  push:
    branches: [main]
```
`publish.yml` needs a third trigger the existing file has no example of — `release: types: [created]` (D-03) and `pull_request: types: [closed]` for `ghcr-cleanup.yml` (D-11). Follow the same "name the workflow, comment WHY the trigger shape is what it is" discipline.

**Least-privilege permissions pattern** (lines 14-16):
```yaml
# Least privilege at the workflow level; no job widens it.
permissions:
  contents: read
```
`publish.yml` is "the first workflow to interpolate a secret" per `ci.yml`'s own line 113 comment — it needs `packages: write` and `id-token: write` (cosign keyless), but per the Security Domain findings these must be scoped to the **job**, not widened at the workflow level. Mirror `ci.yml`'s workflow-level `contents: read` floor, then add narrower per-job blocks (see Code Example below, from RESEARCH.md Pattern 2).

**Concurrency pattern** (lines 18-20):
```yaml
concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: ${{ github.event_name == 'pull_request' }}
```
Reuse verbatim in every new workflow — cancels a superseded PR run, never cancels a merge-triggered run.

**Pinned-action-by-SHA pattern** (lines 34-42, repeated per job):
```yaml
      # Actions are pinned by commit SHA, not tag: a tag is mutable and can be
      # repointed at malicious code, a SHA cannot.
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
      - uses: astral-sh/setup-uv@c771a70e6277c0a99b617c7a806ffedaca235ff9 # v9.0.0
        with:
          version: ${{ env.UV_VERSION }}
          enable-cache: true
          cache-dependency-glob: "uv.lock"
```
Every new action reference (`docker/setup-buildx-action`, `docker/login-action`, `docker/build-push-action`, `sigstore/cosign-installer`, `actions/delete-package-versions`, `actions/upload-artifact`) must follow this `@<full-sha> # vX.Y.Z` comment convention — `tests/policy/test_supply_chain_guards.py` and/or a sibling policy test will very likely check for pinned SHAs the same way it already checks tool-version agreement.

**Job-per-concern pattern** (whole file structure): `check` / `manifests` / `integration` / `secrets` are four independent jobs, each single-purpose, each delegating its substantive work to a `make <target>` call rather than inlining logic:
```yaml
      - run: make install
      - run: make check
```
D-06 explicitly wants the new publish/scan job to run "independently/in parallel," matching this exact shape — one job per concern, `runs-on: ubuntu-latest`, its own `timeout-minutes`, delegating to `make` wherever a Make target already exists (there is no existing Make target for build+push+sign, so this is the one place a workflow does more than call `make`, per RESEARCH.md's Pattern 2).

**Fetch-depth-0 pattern for anything needing full history** (lines 122-131):
```yaml
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
        with:
          # Load-bearing. actions/checkout defaults to depth 1, and
          # `gitleaks git --log-opts="--all"` over a depth-1 checkout examines a
          # single commit and reports "no leaks found" — a green build that
          # proves nothing.
          fetch-depth: 0
```
Not directly needed by the new workflows, but the comment style ("load-bearing, not cosmetic") is the house style to match in every new workflow's non-obvious steps — e.g. why `e2e-full`/`e2e-chaos` need `timeout-minutes: 90-120` instead of the existing jobs' `15`.

**Timeout discipline** (every job): `timeout-minutes: 15` (check/manifests/integration), `10` (secrets). Per RESEARCH.md Pitfall 9, do **not** copy `15` for `e2e-full`/`e2e-chaos` — comment the real reasoning the way `secrets`' own comment does ("Measured at ~150ms... generous ceiling is for runner variance, not the scan").

---

### `helm/values/local/kyverno.yaml` / `helm/values/ci/kyverno.yaml` (config, batch)

**Analog:** `helm/values/local/cnpg-operator.yaml` + `helm/values/ci/minio.yaml` (both read in full)

Kyverno is architecturally an **operator/controller chart with CRDs**, exactly like CNPG's operator — not a workload chart like MinIO/Vault. `cnpg-operator.yaml` is the closer structural analog; `minio.yaml`'s local/ci pair shows the exact three-axis divergence discipline that governs how the two new files must differ from each other.

**Full local/ci divergence discipline** (`helm/values/local/cnpg-operator.yaml` lines 1-14):
```yaml
# D-06: the local and CI profiles diverge on EXACTLY three axes — replica
# counts, resource sizing, and monitoring. Any fourth divergence axis needs an
# argument in review. This file and helm/values/ci/cnpg-operator.yaml must
# otherwise be identical in shape.
```
`tests/policy/test_values_profiles.py`'s `PERMITTED_AXES` (line 132) and `test_profiles_diverge_only_on_permitted_axes` (line 236) enforce this **automatically** for any new `helm/values/local/<X>.yaml` + `helm/values/ci/<X>.yaml` pair — no policy-test change is needed for Kyverno as long as the two new files only diverge on the same three permitted axes. `test_both_profiles_exist_for_every_component` (line 211) will also fail the build if `helm/values/local/kyverno.yaml` is added without a `helm/values/ci/kyverno.yaml` counterpart (or vice versa) — this is how D-17 ("Kyverno in both profiles") gets mechanically enforced for free.

**CRD + resource-sizing pattern** (`cnpg-operator.yaml`, full file, 38 lines):
```yaml
crds:
  create: true

replicaCount: 1

# The chart's `resources: {}` default is empty — see 02-RESEARCH.md Pitfall 5
# ("the CNPG operator's `resources` key defaults to empty"). An unrequested
# container is QoS BestEffort and is evicted first, exactly when its data is
# needed to explain the incident.
resources:
  requests:
    cpu: 100m
    memory: 128Mi
  limits:
    cpu: 500m
    memory: 256Mi

# Phase 7 owns metrics ... — the third D-06 axis, disabled in both profiles for now.
monitoring:
  podMonitorEnabled: false
```
Per RESEARCH.md Pitfall 3, Kyverno's chart installs **four** Deployments by default (`admissionController`, `backgroundController`, `cleanupController`, `reportsController`), each defaulting to its own `resources.requests` — do not assume a single `resources:` key covers the whole chart the way it does for CNPG. Explicitly set `cleanupController.enabled: false` (this project never uses `CleanupPolicy`) and size `backgroundController`/`reportsController` down for the `ci` profile specifically, mirroring `minio.yaml`'s CI-vs-local resource split below.

**CI-profile sizing-for-the-runner-budget pattern** (`helm/values/ci/minio.yaml` lines 8-10, 32-39):
```yaml
# D-12: CI's rendered manifests are summed by the manifest-resources policy
# test against the GitHub-hosted runner's 4 CPU / 16 GB budget, so the sizes
# below are load-bearing, not decorative.
...
# Smaller than local: this profile shares a 4 CPU / 16 GB runner with every
# other pod the CI job renders.
resources:
  requests:
    cpu: 100m
    memory: 256Mi
  limits:
    cpu: 500m
    memory: 512Mi
```
`tests/policy/test_manifest_resources.py`'s `EFFECTIVE_CI_CPU_BUDGET`/`EFFECTIVE_CI_MEMORY_BUDGET` (lines 349-360) and `test_ci_profile_fits_runner` (line 382) will automatically sum every rendered CI-profile manifest, Kyverno's four Deployments included — run `make manifests && REQUIRE_RENDERED_MANIFESTS=1 uv run pytest tests/policy/test_manifest_resources.py -k ci_profile_fits_runner -q` early to measure real headroom before finalizing numbers (RESEARCH.md's own Wave-0 recommendation).

**Image-tag agreement pattern** (`tests/policy/test_supply_chain_guards.py` lines 341-397, see below) will need a **new** reader function (mirroring `minio_image_readings()`/`airflow_image_readings()`) for Kyverno's chart version once `helm/versions.env` gets a `KYVERNO_CHART_VERSION` entry — see that file's Pattern Assignment.

---

### `kubernetes/kyverno-policy.yaml` (config, batch) + `scripts/stages/25-kyverno.sh` / `26-kyverno-policy.sh` (utility, batch)

**Analog for the manifest's location:** `kubernetes/namespaces.yaml` + `kubernetes/rbac-etl.yaml` (both already-committed raw K8s manifests, read in full)

This resolves RESEARCH.md's Open Question 3 ("where does the `ImageValidatingPolicy` manifest get committed?") directly against an existing, enforced repository convention that RESEARCH.md's own author had not yet found: `tests/policy/test_no_manual_kubectl_surgery.py` hard-codes the exact rule.

**The exact permitted-`kubectl apply` rule** (`tests/policy/test_no_manual_kubectl_surgery.py` lines 146-149, 212-217):
```python
# A `kubectl apply -f <target>` argument is permitted when it is `-` (stdin
# — the D-14 credential-materialisation pattern, see module docstring) or
# names a path with a `kubernetes/` path segment (a committed manifest).
_COMMITTED_KUBERNETES_PATH = re.compile(r"(^|/)kubernetes/[^/]")

def _is_permitted_apply(apply_target: str | None) -> bool:
    if apply_target is None:
        return False
    if apply_target == "-":
        return True
    return bool(_COMMITTED_KUBERNETES_PATH.search(apply_target))
```
Any `kubectl apply -f <path>` in any script is a policy violation **unless** `<path>` contains a `kubernetes/` path segment. This means the Kyverno policy manifest MUST live at `kubernetes/kyverno-policy.yaml` (or similar), not under a new `helm/kyverno-policies/` directory as RESEARCH.md's Open Question 3 speculated — `helm/schemas/cnpg/` (the only existing "adjunct to a vendored chart" directory) is confirmed to hold only JSON CRD *schemas* for kubeconform, not applied manifests, so it is not a precedent to follow here.

**`75-etl.sh`'s own comment already names this as the established, repeatable shape** (whole file, 30 lines):
```bash
#!/usr/bin/env bash
#
# The etl namespace's RBAC (plan 04-02) — last in the LC_ALL=C stage order.
# ...
# One step: apply the committed kubernetes/rbac-etl.yaml (the same
# `kubectl apply -f <committed kubernetes/ path>` shape
# scripts/stages/20-namespaces.sh's own header comment documents — a second,
# equally narrow instance, not a new exception; ...)

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "==> applying kubernetes/rbac-etl.yaml"
kubectl --context "${KUBECTL_CONTEXT:-kind-${CLUSTER_NAME}}" apply -f "${repo_root}/kubernetes/rbac-etl.yaml"
```
`scripts/stages/26-kyverno-policy.sh` should copy this file's shape almost verbatim — it becomes "a **third**, equally narrow instance, not a new exception" of the same permitted pattern, with its own header comment saying so explicitly (matching this project's house style of narrating *why* a rule is not being bent).

**Analog for the chart-install stage itself:** `scripts/stages/40-cnpg-operator.sh` (whole file, 40 lines) — Kyverno is a controller/CRD chart exactly like CNPG's operator, including the same "CRD established, then Deployment available" two-stage wait:
```bash
helm_install cnpg cnpg/cloudnative-pg cnpg-system \
  CNPG_OPERATOR_CHART_VERSION cnpg-operator

wait_for_crd_established clusters.postgresql.cnpg.io
wait_for_deploy_available cnpg-system cnpg-cloudnative-pg
```
`25-kyverno.sh` should install the Kyverno chart, then wait for its `ImageValidatingPolicy`/`ClusterPolicy` CRD(s) to reach `Established` and for `admissionController` to reach `Available`, using the exact same `scripts/wait-for.sh` helpers (`wait_for_crd_established`, `wait_for_deploy_available`) already sourced in `40-cnpg-operator.sh` line 29.

**Stage numbering is load-bearing for Pitfall 4** (RESEARCH.md): number `25-kyverno.sh` right after `20-namespaces.sh` and before `30-ingress-nginx.sh` (NOT appended after `85-monitoring.sh`), so every subsequent stage's pods are genuinely admission-checked on every real `cluster-up`, not just in the synthetic D-18 test. `20-namespaces.sh`'s own header comment ("The ONE permitted `kubectl apply`... namespaces are owned by `kubernetes/namespaces.yaml` alone") is the numbering precedent to cite.

---

### `helm/versions.env` (config, extend)

**Analog:** self — the file's own existing structure (65 lines, read in full)

**Pattern to follow exactly** (existing `MINIO_CHART_VERSION`/`MINIO_IMAGE_TAG` pair):
```
MINIO_CHART_VERSION=5.4.0
MINIO_IMAGE_TAG=RELEASE.2026-08-04T00-00-00Z
```
Add `KYVERNO_CHART_VERSION=3.8.2` as a new bare `KEY=value` line (per the file's own header: "`KEY=value` lines only, no quoting, no inline comments"). If a specific comment is warranted (e.g. explaining the deliberate 3.8.2-over-3.9.0 pin, per RESEARCH.md's Package Legitimacy Audit), follow the file's existing per-entry comment style seen above `AIRFLOW_IMAGE_TAG` and `TEMPO_CHART_VERSION` (a short paragraph citing the plan/verification method, not just the number). `tests/policy/test_pinned_tool_versions_agree.py` is the shared enforcement mechanism — no separate test is needed if the new chart follows the same `helm-lint` Makefile-target wiring described below.

---

### `docs/runbooks/*.md` (doc, N/A) — 18 files, one per verified README §89 scenario

**Analog for content (5 files with a real matching incident):** `.planning/debug/resolved/dagrun-scheduler-stall.md`, `.planning/debug/resolved/wait-for-files-stuck-task.md`, `.planning/debug/resolved/airflow-scheduler-stuck-tasks.md`, `.planning/debug/resolved/prometheus-runs-started-scrape.md`, `.planning/debug/resolved/backfill-does-not-redrive-rejected-row.md`

**Analog for doc tone/structure (procedural, not incident-log):** `docs/ci-branch-protection.md` (247 lines) — an existing "how an operator does X" doc, not an ADR and not an incident log.

There is **no existing runbook-shaped file** in this repository (`docs/runbooks/` does not exist yet) — this is the one doc category in this phase with no direct structural analog, only content-source analogs. D-41 specifies runbook shape explicitly (symptoms/diagnosis/recovery/reprocessing/verification per scenario), so the *content* discipline should come from the debug logs, not an invented structure.

**What to extract from a resolved debug log, concretely** — `dagrun-scheduler-stall.md`'s `## Resolution` section (lines 169-177) is written exactly the way a runbook's own "diagnosis → recovery → verification" sections should read:
```
root_cause: A Docker Desktop/WSL2-level infrastructure event ... The DAGs
  hostPath bind mount ... failed to reattach on all 3 nodes after that
  restart; Docker silently fell back to an empty, read-only tmpfs ...
fix: APPLIED in two stages, both explicitly user-approved. Stage 1 (scoped):
  `docker restart airflow-platform-worker` ONLY ...
verification: (1) `docker exec airflow-platform-worker` confirmed `/mnt/dags`
  changed from `none on /mnt/dags type tmpfs` [empty] to `/dev/sde on
  /mnt/dags type ext4` with real files ... (2) Confirmed identically from
  inside the dag-processor pod's own mounted view ... (3) `DagModel.is_stale`
  flipped to `False` ...
```
A runbook entry for "Airflow unavailable" (§89 item 1) should read as the operator-facing distillation of exactly this: **symptoms** = the `## Symptoms` section's `expected`/`actual`/`errors` fields, **diagnosis** = the numbered evidence chain condensed to the load-bearing checks only (not every dead-end), **recovery** = the `fix` field's exact commands, **verification** = the `verification` field's exact checks.

**Direct per-scenario source mapping** (from RESEARCH.md's own verified table — use this, not README's raw §89 list, since the counts in CONTEXT.md were off-by-one):

| Scenario | Write from |
|---|---|
| Airflow unavailable | `dagrun-scheduler-stall.md` |
| Vault unavailable | `wait-for-files-stuck-task.md` |
| Kubernetes pod stuck | `airflow-scheduler-stuck-tasks.md` |
| Failed backfill | `backfill-does-not-redrive-rejected-row.md` + INCR-06 |
| Task repeatedly failing | `airflow-scheduler-stuck-tasks.md` (again) or ORCH-04 |
| Partial database load | `meta.v_run_recovery` (Phase 9 plan 09-06, `tests/integration/test_run_recovery_view.py`) — an existing **feature** to document, not an incident |
| MinIO / PostgreSQL unavailable, Secret unavailable | D-25 chaos test suite's own observed behavior — write these 3 **after** `tests/e2e/chaos/` exists, never speculatively ahead of it (D-41's own explicit ordering) |
| CDC failure | No real subject exists (CDC dropped from v1) — write a short "not applicable, here is the seam" stub per RESEARCH.md Open Question 1, do not omit the file |
| CSV malformed / Schema changed / Duplicate batch / Late-arriving data / SCD correction / Corrupted file / Secret rotation / Unauthorized access | Existing, already-built features — document current behavior as a how-to, not an incident reconstruction (VALID-01, SCHEMA-04/05, LOAD-08, INCR-03/04, SCD-07, LOAD-10, SEC-09, SEC-12) |

**Naming convention to establish** (none exists yet — Claude's discretion per CONTEXT.md): one file per scenario under `docs/runbooks/`, e.g. `docs/runbooks/airflow-unavailable.md`, matching the kebab-case-from-scenario-name convention already used for `.planning/debug/resolved/*.md` filenames (`dagrun-scheduler-stall.md`, `wait-for-files-stuck-task.md`).

---

### `docs/adr/0011-raw-immutability-iam-not-worm.md` (doc, optional)

**Analog:** `docs/adr/0000-template.md` (35 lines, read in full) + `docs/adr/0010-dbt-silver-layer-boundary.md` (122 lines, the most recent real ADR)

**Template structure to fill** (`0000-template.md`, whole file):
```markdown
---
status: {proposed | accepted | rejected | deprecated | superseded by ADR-00NN}
date: YYYY-MM-DD
---

# ADR-00NN: {short title, a decision phrased as a claim}

## Context and Problem Statement
{What forces are in play? Which README section or research finding does this touch?}

## Considered Options
* Option A
* Option B

## Decision Outcome
Chosen option: "{A}", because {justification}.

### Consequences
* Good, because …
* Bad, because …
* Neutral, because …

## Migration trigger
{What observable event would make us revisit this? "None — this is permanent" is a
valid answer and must be written explicitly rather than left blank.}

## References
* README §NN
* .planning/research/{FILE}.md §{section}
```
For ADR-0011, "Considered Options" is trivially "IAM deny-delete policy" vs. "Object-lock/WORM" — already argued at length in `helm/values/local/minio.yaml`'s own D-08 comment (Phase 2) and D-40's CONTEXT.md text; "Migration trigger" should name the same condition D-08 already flagged ("if a compliance requirement demands tamper-proof retention lock, revisit WORM"). Next ADR number after `0010-dbt-silver-layer-boundary.md` is `0011` — confirmed no ADR 0011 exists yet.

---

### `airflow/dags/platform_retention.py` (controller/DAG, batch)

**Analog (primary, structural simplicity):** `airflow/dags/smoke_kubernetes_pod.py` (whole file, 26 lines) — the only existing DAG that is NOT part of the ingestion task graph, exactly D-35's requirement.

**Analog (secondary, task/KPO conventions):** `airflow/dags/csv_ingest_customers.py` (whole file, 182 lines) + `airflow/dags/_common/kpo.py` (whole file, 163 lines)

**Minimal-maintenance-DAG shape** (`smoke_kubernetes_pod.py`, whole file):
```python
"""U1 (ORCH-01/06): permanent smoke-test fixture -- can KPO run a pod here?"""

import pendulum
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import dag
from kubernetes.client import models as k8s

from _common.kpo import common_kpo_kwargs


@dag(schedule="@daily", start_date=pendulum.datetime(2026, 1, 1, tz="UTC"), catchup=False)
def smoke_kubernetes_pod() -> None:
    """Run one pod; write the built image's git SHA to XCom -- U1's pass criteria."""
    resources = k8s.V1ResourceRequirements(
        requests={"cpu": "100m", "memory": "128Mi"}, limits={"cpu": "500m", "memory": "256Mi"}
    )
    KubernetesPodOperator(
        task_id="print_version_to_xcom",
        cmds=["sh", "-c"],
        arguments=['printf \'{"git_sha": "%s"}\' "$GIT_SHA" > /airflow/xcom/return.json'],
        retries=1,
        **common_kpo_kwargs(resources=resources),
    )


smoke_kubernetes_pod()
```
`platform_retention` should follow this exact "@dag decorator, module-level resources, one-or-few KPO tasks via `common_kpo_kwargs`, call the function at module bottom" shape — D-35's "deliberately NOT part of any ingestion DAG's task graph" is structurally satisfied simply by being its own top-level `@dag`-decorated function in its own file, same as `smoke_kubernetes_pod` already is relative to `csv_ingest_customers`.

**Reusable KPO-kwargs builder** (`_common/kpo.py::common_kpo_kwargs`, signature at line 50):
```python
def common_kpo_kwargs(
    *,
    resources: k8s.V1ResourceRequirements,
    extra_env_vars: list[k8s.V1EnvVar] | None = None,
    service_account_name: str = "csv-processor",
    image_variable: str = "csv_processor_image",
    vault_k8s_role: str = _VAULT_K8S_ROLE,
    include_dataplat_credentials: bool = True,
) -> dict[str, object]:
```
The retention DAG's task(s) should call `common_kpo_kwargs(...)` the exact way `dbt_build` in `csv_ingest_customers.py` overrides `service_account_name`/`vault_k8s_role`/`include_dataplat_credentials` (lines 143-155) if retention needs its own least-privilege identity, or use the defaults if it runs as `csv-processor`. **Do not hand-roll a new KPO-kwargs builder** — every field this function returns (`namespace`, `on_finish_action: "delete_pod"`, `get_logs`, `env_vars`) encodes a hard-won live-cluster finding (see `kpo.py` lines 137-153's own comment on the `on_finish_action` CPU-leak incident) that a new, separate builder would have to rediscover.

**Schedule + `max_active_runs` pattern** (`csv_ingest_customers.py` lines 76-84):
```python
# */1 * * * *: short interval keeps sensing prompt; max_active_runs=1 (D-03) caps concurrency.
@dag(
    dag_id="csv_ingest_customers",
    schedule="*/1 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["vertical-slice", "customers"],
)
```
`platform_retention`'s own schedule (daily vs weekly, per CONTEXT.md's own "Claude's Discretion") should be declared the same explicit way, with a one-line comment justifying the cadence, and `tags=["maintenance", "retention"]` (or similar) to distinguish it from `["vertical-slice", ...]` DAGs at a glance in the Airflow UI.

**Cross-cutting policy tests that automatically apply to the new file:**
- `tests/policy/test_dag_thinness.py` (lines 1-60+ read) scans **every** `airflow/dags/*.py` file for a `csv`/`psycopg`/`boto3`/`pydantic` import and for raw SQL-string literals — `platform_retention.py` is in scope automatically, with no by-name exemption, unless it legitimately needs a narrow exemption the way `_common/integrity_gate.py`/`_common/run_stage_recorder.py` do (documented precedent for "this DAG file needs a direct DB touchpoint for a narrow, named reason").
- `tests/policy/test_dag_line_budget.py` (whole file, 43 lines) enforces per-file line ceilings **by literal file path**, one `def test_<name>_stays_under_N_lines()` function per DAG — it does **NOT** automatically cover a new file. If a budget is wanted for `platform_retention.py`, a new test function must be added here, following the exact pattern:
```python
def test_smoke_kubernetes_pod_stays_under_30_lines() -> None:
    path = REPO_ROOT / "airflow" / "dags" / "smoke_kubernetes_pod.py"
    line_count = len(path.read_text(encoding="utf-8").splitlines())
    msg = f"ORCH-06: smoke_kubernetes_pod.py is {line_count} lines, budget is <30"
    assert line_count < 30, msg
```

---

### `configs/datasets/customers.yaml` / `orders.yaml` (config, CRUD) — extend with a `retention:` block

**Analog:** self — the file's own existing `scd:`/`freshness:`/`quality:` opt-in blocks (`configs/datasets/customers.yaml`, whole file, 163 lines, read in full)

**The exact opt-in-block shape to copy** (lines 103-111, the most recent/closest precedent — Phase 10's own SCD block):
```yaml
# scd: is D-05/D-06's SCD2 DELETE-semantics and mass-delete circuit-breaker
# declaration (Phase 10, 10-CONTEXT.md). delete_semantics: invalidate means
# a customer_id present historically but absent from this pass's snapshot
# has its current version closed out, with no new version opened.
# mass_delete_threshold: 0.10 reuses Phase 8's 10% starting reference value
# as a first-pass, tunable circuit-breaker threshold.
scd:
  delete_semantics: invalidate
  mass_delete_threshold: 0.10
```
and the freshness block (lines 37-45):
```yaml
# freshness: is D-08's opt-in data-freshness expectation (OBS-01/OBS-09),
# flowing through ConfigRegistry.sync() into meta.datasets' three new
# nullable interval columns (migration 0010).
freshness:
  expected_frequency: "1 day"
  warn_after: "2 hours"
  fail_after: "6 hours"
```
A new `retention:` block should follow this identical shape: a short comment naming the CONTEXT.md decision ID (D-37/D-38/D-39), then a flat mapping of tiered windows plus the dry-run opt-in flag, e.g. (illustrative, not prescriptive on exact key names — that is plan-time/Claude's-discretion territory per CONTEXT.md):
```yaml
retention:
  raw_days: null            # D-36: indefinite by default, structurally supported but unset
  processed_days: 60        # D-37: tiered default, Claude's discretion within 30-90
  quarantine_days: 180
  validation_reports_days: 730
  enforce: false             # D-38: dry-run by default; true is the explicit opt-in to hard-delete
```
Every existing opt-in block in this file is `None` by default at the Pydantic model layer and entirely absent from a dataset's YAML when not applicable (`ScdConfig`/`FreshnessConfig`/`QualityConfig`/`ReconciliationConfig` are all `| None = None` on `DatasetConfig`) — `RetentionConfig` should follow the same optionality convention, and D-38's `enforce: false` default must be enforced in the **Pydantic model's own field default**, not merely as a YAML convention, so a dataset config that omits `retention:` entirely still can never accidentally hard-delete.

---

### `packages/dataplat/src/dataplat/config/model.py` (model, CRUD) — extend with `RetentionConfig`

**Analog:** self — `FreshnessConfig` (lines 404-444) and `ScdConfig` (lines 513-541), both in the same file, both the immediately-preceding opt-in-block precedents.

**Class shape to copy** (`ScdConfig`, lines 513-541, in full — the closest analog since it also carries a threshold + closed-vocabulary semantics field):
```python
class ScdConfig(BaseModel):
    """A dataset's opt-in SCD2 DELETE-semantics and mass-delete circuit-breaker declaration.

    Absent entirely when a dataset is not SCD-tracked -- mirrors
    ``ReconciliationConfig``/``FreshnessConfig``/``QualityConfig``'s own
    opt-in precedent. ...

    Attributes:
        delete_semantics: ... one of ``"ignore"`` (do nothing), ``"invalidate"``
            (close out the current version, no new version opened), ``"new_record"``
            ...
        mass_delete_threshold: The fraction of previously-current business
            keys that may vanish from a single pass before the circuit
            breaker trips and fails the run (D-06), e.g. ``0.10`` for 10% --
            same shape as ``QualityConfig.rejection_rate_threshold``.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    delete_semantics: Literal["ignore", "invalidate", "new_record"]
    mass_delete_threshold: float
```
and the registration on `DatasetConfig` itself (lines 602-617):
```python
class DatasetConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    dataset: str
    config_schema_version: int
    source: SourceConfig
    ...
    freshness: FreshnessConfig | None = None
    quality: QualityConfig | None = None
    reconciliation: ReconciliationConfig | None = None
    scd: ScdConfig | None = None
    csv: CsvParsingConfig = Field(default_factory=CsvParsingConfig)
    ...
```
A new `RetentionConfig(BaseModel)` class, with `model_config = ConfigDict(extra="forbid", frozen=True)` (every single sibling class uses this identically — never deviate), plus a new `retention: RetentionConfig | None = None` field added to `DatasetConfig`, and a new docstring entry in `DatasetConfig`'s own Attributes list (matching the existing per-field docstring convention at lines 575-589) is the complete, in-convention change. `ConfigRegistry.sync()` (see next section) does **not** need to change — it persists `config.model_dump(mode="json")` as an opaque `Jsonb` blob (line 184), so a new field is captured automatically the moment `DatasetConfig` gains it.

---

### `Makefile` (utility, batch) — extend with `rebuild-from-raw` and a rollback target

**Analog:** self — `cluster-rebuild` (line 172), `migrate-analytics` (lines 202-255), `image-csv-processor` (lines 302-340), all in the same file (499 lines, read in full).

**Pattern for a target that wraps multiple existing operations in sequence** (`migrate-analytics`, lines 202-255, abbreviated to the structural skeleton):
```makefile
migrate-analytics:               ## 08.1-13: alembic upgrade head against the LIVE analytical PostgreSQL, via a port-forward [plan 08.1-13]
	# Mirrors scripts/vault-bootstrap.py's own _port_forwarded_vault shape
	# (port-forward svc/<X> to a free local port, poll until it accepts a
	# connection, run the real work, always tear the tunnel down) as a plain
	# shell recipe...
	@set -a; . helm/versions.env; set +a; \
	ctx="kind-$$CLUSTER_NAME"; \
	...
	trap 'kill $$pf_pid >/dev/null 2>&1 || true' EXIT; \
	...
	$(RUN) alembic -c migrations/alembic.ini upgrade head
```
`rebuild-from-raw` (D-32) needs the identical "source `helm/versions.env`, resolve the kubectl context, do real work, always clean up via `trap ... EXIT`" shape, sequencing: drop ETL-owned schemas (new SQL, run the same way `migrate-analytics` reaches the live DB) → `alembic upgrade head` (literally reuse `migrate-analytics`'s own recipe body, or make `rebuild-from-raw` depend on it as a prerequisite target) → trigger Airflow backfill DagRuns (likely a new small Python script under `scripts/`, invoked the same way `vault-bootstrap`/`vault-unseal` call `scripts/vault-*.py` via `$(RUN_CLUSTER) python scripts/<name>.py`, per lines 183-187).

**Pattern for the `$(RUN_CLUSTER)` vs `$(RUN)` distinction** (lines 14-20, 189-190):
```makefile
RUN_CLUSTER := $(RUN) --group cluster
...
vault-verify:                    ## INFRA-06: run tests/e2e/vault against the live cluster [plan 05-01]
	$(RUN_CLUSTER) pytest tests/e2e/vault -q
```
`rebuild-from-raw` needs a live cluster and live boto3/psycopg — it MUST use `$(RUN_CLUSTER)`, and MUST be listed in the `.PHONY` declaration (line 56-61) and given a `##` help comment (the `help:` target's own `grep -E '^[a-z%-]+:.*##'` convention, line 65-66) — an undocumented target is invisible to `make help`.

**Pattern for immutable-git-SHA image references** (`image-csv-processor`, lines 300-340) is the precedent for a rollback target: `GIT_SHA := $(shell git rev-parse --short HEAD)` (line 300) is computed once per invocation; a rollback target instead needs to accept an **explicit** prior SHA as a parameter (mirroring `FILE ?=` at line 54's "no default, fail loudly if unset" convention for `ingest-demo`):
```makefile
FILE ?=
...
ingest-demo:                    ## D-14: upload FILE and wait for the real sensor-driven pipeline [plan 04-09]
	@if [ -z "$(FILE)" ]; then echo "ERROR: FILE is required, e.g. make ingest-demo FILE=..." >&2; exit 1; fi
	$(RUN_CLUSTER) python scripts/ingest-demo.py --file $(FILE)
```
A `rollback` target should take a `SHA ?=` variable the same way, fail loudly if unset, and re-run the exact `helm upgrade`/Variable-set steps `image-csv-processor`/`image-airflow` already perform, pointed at the caller-supplied SHA's already-published image tag instead of a freshly-built one.

**`ci`/`check` composition is the Shared Pattern governing where any new target may be wired in** — see Shared Patterns below.

---

### `packages/dataplat/src/dataplat/pipeline/run.py` (service, transform) — extend `_table_checksum`

**Analog:** self, same file, same function (lines 301-421, read in full)

**Current implementation** (lines 301-315):
```python
def _table_checksum(conn: Connection[Any], table: str) -> str | None:
    """Compute an order-independent aggregate hash over every row of ``table`` (D-21).

    ``bit_xor`` is commutative -- the result does not depend on row order,
    so two tables holding the SAME rows in a DIFFERENT physical order
    produce the SAME checksum. `table` is a config-resolved identifier
    (T-09-03), interpolated as an identifier only.
    """
    # `table` is a config-resolved identifier (T-09-03), never row content.
    query = (
        f"SELECT to_hex(bit_xor(('x' || substr(md5(t::text), 1, 16))::bit(64)::bigint)) "  # noqa: S608
        f"FROM {table} t"
    )
    result = _scalar(conn, query)
    return None if result is None else str(result)
```
**Its one existing caller** (`_compute_silver_gold_reconciliation`, lines 318-421) calls it unchanged, twice, at lines 413-414:
```python
        checksum_input=_table_checksum(conn, source_table),
        checksum_output=_table_checksum(conn, target_table),
```
Per RESEARCH.md Pitfall 7, D-29's rebuild-comparison checksum **must not** hash `_run_id`/`_file_id`/`_batch_id`/`_source_row_number`/`_ingested_at`/`_dbt_loaded_at` (a rebuild deliberately re-mints these) — a literal `SELECT * FROM {table} t` will always mismatch on a byte-perfect rebuild. Add a new optional `columns: Sequence[str] | None = None` keyword parameter (exact code already composed in RESEARCH.md's own Code Examples section) that, when given, hashes only the named column list via a subquery; when `None` (the default), behavior is **byte-identical** to today, so `_compute_silver_gold_reconciliation`'s two existing call sites need zero changes. This is a strict additive extension of the existing function, not a new sibling function — follow the same "extend the signature, preserve old callers unchanged" discipline `common_kpo_kwargs` (DAG section above) already demonstrates via its many optional keyword-only parameters.

---

### New retention service module (service, batch) — e.g. `packages/dataplat/src/dataplat/retention/policy.py`

**Analog:** `packages/dataplat/src/dataplat/scd/delete_detection.py::MassDeleteCircuitBreaker` (lines 167-286, read in full) + `packages/dataplat/src/dataplat/validate/circuit_breaker.py::RejectionRateCircuitBreaker` (whole file, 147 lines, read in full)

Both existing circuit breakers share one exact shape: **constructor-parameterized totals, `apply()` never re-derives counts, a trivial-PASS empty-input guard, structured `ValidationResult` findings.** D-38's retention dry-run is a close cousin — "count what would be deleted, structure a report" — but differs in one load-bearing way: the circuit breakers **raise** on breach (fail the run); the retention module must **report and only conditionally act**, never raise merely because something is old enough to delete.

**Constructor + trivial-guard shape to copy** (`RejectionRateCircuitBreaker`, lines 41-76, 96-116):
```python
class RejectionRateCircuitBreaker(BarrierStage):
    name = "rejection_rate_circuit_breaker"

    def __init__(
        self,
        *,
        threshold: float,
        total_rows_read: int,
        total_rows_rejected: int,
        rule_id: str = "rejection_rate_circuit_breaker",
    ) -> None:
        self._threshold = threshold
        self._total_rows_read = total_rows_read
        self._total_rows_rejected = total_rows_rejected
        self._rule_id = rule_id

    def apply(self, ctx: PipelineContext) -> StageResult:
        del ctx  # unused -- totals come from the constructor, see module docstring
        placeholder_chunk = RecordChunk(rows=(), first_ordinal=0, expected_field_count=0)

        if self._total_rows_read == 0:
            return StageResult(..., outcome="PASS", ...)
```
A retention "policy evaluator" should take the same shape: construct with the already-queried candidate counts (per-layer: raw/processed/quarantine/validation-reports/logs), never re-query inside `apply`/`evaluate`, and return a structured report object (count, size, oldest/newest — D-38's own exact wording) rather than raising, with `enforce: bool` (from the new `RetentionConfig`) gating whether a second method actually issues the deletes.

**The "count vs. threshold, structured outcome" arithmetic** (`MassDeleteCircuitBreaker.apply`, lines 211-267) is the template for how to structure the report even though the retention case never raises:
```python
        ratio = self._vanished_count / self._current_count
        if ratio > self._threshold:
            msg = (...)
            raise QualityThresholdExceeded(msg, context={...})

        return StageResult(
            chunk=placeholder_chunk,
            rejected=[],
            findings=[
                ValidationResult(
                    rule_id=self._rule_id,
                    rule_type="QUALITY",
                    severity="ERROR",
                    outcome="PASS",
                    evaluated_count=self._current_count,
                    failed_count=self._vanished_count,
                    message="mass-delete ratio within threshold",
                    threshold={"mass_delete_threshold": self._threshold},
                    observed={"ratio": ratio},
                )
            ],
        )
```
Reuse `dataplat.models.report.ValidationResult`'s `threshold`/`observed` dict-shape convention for the retention dry-run report too — it is already this codebase's established way to make a "config value vs. observed value" comparison machine-readable, and downstream consumers (a retention "job summary" log line, or a future Grafana panel) can parse it the same way.

**T-08-14/T-10-08's "misconfiguration is a deliberate choice, not a platform defect" framing** (both files' docstrings) is directly reusable for D-38: an `enforce: true` with an aggressive window is a deliberate operator choice the dry-run report exists to surface loudly, not a condition the code should second-guess.

---

### New rebuild-reconciliation comparison module (service, transform) — e.g. `packages/dataplat/src/dataplat/pipeline/rebuild_reconciliation.py`

**Analog:** `pipeline/run.py::_compute_silver_gold_reconciliation` (lines 318-421) + `metadata/repository.py::record_reconciliation` (lines 990-1074+)

Per RESEARCH.md Pitfall 8, `record_reconciliation`'s own grain (one row per `(file_id, hop)`, inside a single processing pass) is naturally re-exercised during the rebuild's own backfill DagRuns — it answers "did this file's publish lose rows," not "does table T today equal table T before the drop." D-29's whole-table pre/post comparison needs a small, genuinely new routine, built from parts that already exist:

**The aggregate-query pattern to copy** (`_compute_silver_gold_reconciliation`, lines 361-420, abbreviated):
```python
    input_count = int(_scalar(conn, f"SELECT count(*) FROM {source_table}"))  # noqa: S608
    output_count = int(_scalar(conn, f"SELECT count(*) FROM {target_table}"))  # noqa: S608
    ...
    return _ReconciliationAggregates(
        input_count=input_count,
        output_count=output_count,
        ...
        checksum_input=_table_checksum(conn, source_table),
        checksum_output=_table_checksum(conn, target_table),
        ...
    )
```
A new function should compute the SAME shape of aggregates (`count(*)`, the corrected `_table_checksum(conn, table, columns=business_columns)` from the pipeline/run.py extension above, SCD2 version-count + `is_current` state per business key) **once before the drop** (persisted somewhere durable enough to survive the drop — e.g. written to a file/XCom/a table outside the schemas being dropped) and **once after the rebuild**, then diffs the two structures.

**`record_reconciliation`'s signature is the shape for what "durable, queryable proof" looks like in this codebase** (lines 990-1014, signature only):
```python
    def record_reconciliation(
        self,
        *,
        conn: Connection[Any],
        dataset_id: int,
        file_id: int | None,
        hop: str,
        input_count: int,
        output_count: int,
        ...
        checksum_input: str | None = None,
        checksum_output: str | None = None,
        ...
    ) -> int:
```
D-29 point 4 requires the comparison to **reuse** `meta.reconciliation_results`/`record_reconciliation` rather than a bespoke mechanism — the honest reading (RESEARCH.md's own framing) is: keep relying on this exact method for what it already proves per-file during the rebuild's own backfill (zero new code), and add the new whole-table pre/post routine as a thin, separate caller that itself may optionally also write a `hop="rebuild_snapshot"`-style row through this same method for a durable audit trail, rather than inventing a new table.

---

### `tests/e2e/chaos/` (test, event-driven) — 11 files + `__init__.py` + `conftest.py`

**Analog:** `tests/e2e/slice/test_pod_kill_retry.py` (existing, chaos-shaped: kill a pod, observe recovery) + `tests/e2e/vault/test_unseal_survives_restart.py` (existing, "break a component, verify recovery" shape) + `tests/e2e/cluster/conftest.py` (fixture conventions, whole file previously read in this repo's own prior phases)

**`pytestmark` + marker convention** (every existing e2e file, e.g. `test_minio_buckets.py` line 28, `test_positive_auth.py` line 40):
```python
pytestmark = pytest.mark.cluster
```
The 11 new chaos test files need a **new**, additionally-registered marker (D-25 calls for "its own pytest marker, mirroring `cluster`") — register `chaos` in `pyproject.toml`'s marker list the same way `cluster`/`dagtest`/`dbt`/`slow`/`regression` are already registered (Validation Architecture table in RESEARCH.md), and set `pytestmark = pytest.mark.cluster` **plus** a chaos-specific marker on every new file (a chaos test still needs a live cluster, it does not replace that marker).

**`__init__.py` module-docstring convention** (`tests/e2e/vault/__init__.py`, 365 bytes; `tests/e2e/cluster/__init__.py`, 358 bytes — both tiny docstring-only files) — the new `tests/e2e/chaos/__init__.py` should be the same: a short module docstring naming what this test package proves (QUAL-15's 11 named scenarios) and nothing else.

**Directory-per-suite-with-its-own-`conftest.py`** is the established structure (`cluster/conftest.py`, `vault/conftest.py`, `slice/conftest.py` all exist independently) — `tests/e2e/chaos/conftest.py` should follow the same "fixtures specific to this suite's own needs" convention rather than reaching into a sibling suite's conftest.

**File-per-scenario, not one giant file** — matches `tests/e2e/vault/test_positive_auth.py` / `test_negative_auth.py`'s own split (positive and negative concerns get separate files even though they test the same subsystem) and QUAL-15's own explicit 11-item enumeration; one file per scenario (`test_pod_crash.py`, `test_database_unavailable.py`, `test_minio_unavailable.py`, `test_vault_unavailable.py`, `test_malformed_csv.py`, `test_invalid_encoding.py`, `test_oom.py`, `test_task_timeout.py`, `test_duplicate_batch.py`, `test_secret_rotation.py`, `test_unauthorized_secret_access.py`) keeps each scenario's own setup/teardown independently runnable and independently skippable, matching D-25's own "a chaos test that leaves a component deliberately broken can't contaminate the happy-path suite" reasoning taken to its logical per-file conclusion.

---

### `tests/e2e/cluster/test_kyverno_admission.py` (test, request-response)

**Analog:** `tests/e2e/cluster/test_minio_buckets.py` (whole file, 128 lines, read in full) — this is an **exact** match; RESEARCH.md's own Pattern 1 already names this file as "the exact template to mirror for D-18."

**The exact positive+negative shape to copy** (lines 84-128, in full):
```python
def test_raw_delete_is_denied_for_app_credential(s3_client: Callable[[str], Any]) -> None:
    """The negative case that carries §63: the pipeline's own credential cannot delete from raw."""
    app = s3_client("app")
    admin = s3_client("admin")

    bucket, key = "raw", "e2e/minio-buckets/deny-delete.txt"
    payload = b"must survive an app-credential delete attempt"

    try:
        app.put_object(Bucket=bucket, Key=key, Body=payload)

        with pytest.raises(ClientError) as exc_info:
            app.delete_object(Bucket=bucket, Key=key)
        error_code = exc_info.value.response.get("Error", {}).get("Code")
        assert error_code == "AccessDenied", (...)

        # An error code that left the object deleted anyway would pass a naive
        # assertion — prove it is still there, byte for byte.
        still_there = app.get_object(Bucket=bucket, Key=key)["Body"].read()
        assert still_there == payload, (...)
    finally:
        admin.delete_object(Bucket=bucket, Key=key)


def test_raw_delete_is_permitted_for_admin_credential(s3_client: Callable[[str], Any]) -> None:
    """The positive control: a policy denying everyone is as wrong as denying nobody."""
    admin = s3_client("admin")
    bucket, key = "raw", "e2e/minio-buckets/admin-delete.txt"
    admin.put_object(Bucket=bucket, Key=key, Body=b"admin retains delete on raw")
    admin.delete_object(Bucket=bucket, Key=key)
    with pytest.raises(ClientError) as exc_info:
        admin.get_object(Bucket=bucket, Key=key)
    error_code = exc_info.value.response.get("Error", {}).get("Code")
    assert error_code in {"NoSuchKey", "404"}, (...)
```
For D-18, translate this shape 1:1 (RESEARCH.md's own words): deploy a Pod referencing a cosign-signed project image via `kubectl` (expect: created, mirrors `test_raw_delete_is_permitted_for_admin_credential`'s positive control) and deploy a Pod referencing a deliberately unsigned/tampered image not on the D-16 exception list (expect: the `kubectl`/API call raises, admission-webhook denial, **the Pod object itself never gets created** — mirrors `test_raw_delete_is_denied_for_app_credential`'s "prove it is still there, byte for byte" discipline: prove the Pod does NOT exist afterward, not merely that a client-side error surfaced).

**File-split convention as an alternative/complementary shape** — `tests/e2e/vault/test_positive_auth.py` (122 lines) and `test_negative_auth.py` (143 lines) show this codebase is equally comfortable splitting positive and negative into **two files** when there are several of each (SEC-12's multi-identity matrix). If D-18's admission test grows beyond a simple pair, mirror the vault split instead of overloading one file — either is in-convention; `test_minio_buckets.py`'s single-file version is the minimum viable shape.

**Cleanup-in-`finally`/teardown discipline** — every test above wraps its own mutation in `try`/`finally` so a failed assertion never leaves cluster state behind; the Kyverno test's negative case (a Pod that WAS denied) needs no cleanup, but the positive case (a Pod that WAS created) does — mirror this discipline exactly.

---

### `tests/e2e/slice/test_rebuild_from_raw.py` (test, batch)

**Analog:** `tests/e2e/slice/test_smoke_and_idempotency.py` (346 lines, this suite's existing "run twice, compare state" shape) + `tests/e2e/slice/test_backfill_2year_sweep.py` (119,342 bytes — the existing large-scale, multi-dataset, historical-config-replay proof D-31 explicitly wants reused) + `tests/e2e/slice/conftest.py` (34,040 bytes, this suite's own shared live-cluster fixtures)

D-30 requires this test to run **last**, reusing whatever state the suite's own earlier tests (including the 2-year sweep) already populated — this is a structural dependency on **test ordering within the same suite**, not merely "a new independent test file." Follow `tests/e2e/slice/`'s existing convention of one file per scenario, but this file specifically must not seed its own fixture data (D-30's "no separate data-seeding" instruction) — it should read `conftest.py`'s already-established live-cluster fixtures the same way every sibling file in this directory does, and its own body should be structured as: (1) snapshot pre-drop state via the new rebuild-reconciliation module above, (2) invoke `make rebuild-from-raw` (or the underlying script directly), (3) assert the four D-29 proofs (row counts, corrected checksum, SCD2 state, `record_reconciliation`-mechanism reuse) against the post-rebuild state.

---

### `tests/unit/test_retention_*.py` (test, batch)

**Analog:** `tests/unit/validate/test_circuit_breaker.py` (whole file, 73 lines, read in full) — an exact structural match for testing a constructor-parameterized, threshold-driven evaluator in isolation, before it is wired into a live DAG.

**Exact test shape to copy** (whole file):
```python
"""Unit tests for ``dataplat.validate.circuit_breaker.RejectionRateCircuitBreaker``.

Proves D-10's threshold arithmetic in isolation, before plan 08-11 wires it
into a live publication transaction.
"""

def test_a_breach_raises_quality_threshold_exceeded_with_ratio_and_threshold_in_context() -> None:
    breaker = RejectionRateCircuitBreaker(threshold=0.10, total_rows_read=100, total_rows_rejected=15)
    with pytest.raises(QualityThresholdExceeded) as exc_info:
        breaker.apply(_make_context())
    assert exc_info.value.context["observed_ratio"] == pytest.approx(0.15)
    assert exc_info.value.context["threshold"] == 0.10


def test_an_under_threshold_run_does_not_raise_and_returns_a_pass_finding() -> None:
    ...


def test_a_ratio_exactly_at_threshold_does_not_raise() -> None:
    ...


def test_zero_rows_read_never_raises_a_division_by_zero_or_any_other_error() -> None:
    breaker = RejectionRateCircuitBreaker(threshold=0.10, total_rows_read=0, total_rows_rejected=0)
    result = breaker.apply(_make_context())
    assert result.findings[0].outcome == "PASS"
```
Translate directly for the new retention module: test names in this exact "state the property in the function name" style (`test_a_dry_run_never_deletes_anything`, `test_enforce_true_deletes_only_records_past_the_configured_window`, `test_a_window_of_none_never_selects_any_candidate` for D-36's raw-indefinite default, `test_zero_candidates_never_raises`). The `_make_context()` placeholder-fixture pattern (lines 19-28) is reusable verbatim if the retention evaluator also implements `BarrierStage`; if it does not (more likely, since it never raises), a simpler fixture suffices, but the "one behavior per test function, boundary-value tests for the threshold's exact edge" discipline should be copied regardless.

---

### `tests/dagtest/test_platform_retention_dagrun.py` (test, batch)

**Analog:** `tests/dagtest/test_backfill_dagrun.py` (whole file, 135 lines, read in full) + `tests/dagtest/conftest.py` (this suite's existing `load_dag`/`mock_kpo_execute`/`mock_s3_infrastructure` fixtures)

**`dag.test()` shape to copy** (lines 48-75, abbreviated):
```python
def test_backfill_dagrun_customers_succeeds_and_is_structurally_stable(
    load_dag: Callable[[str], Any],
    mock_kpo_execute: list[dict[str, Any]],
    mock_s3_infrastructure: None,  # noqa: ARG001
    mock_run_stage_recorder_db: None,  # noqa: ARG001
) -> None:
    dag = load_dag("csv_ingest_customers")

    dag_run_1 = dag.test(logical_date=_LOGICAL_DATE_1)
    assert dag_run_1.state == "success", (
        f"DagRun did not reach success (state={dag_run_1.state!r}); task states: "
        f"{[(ti.task_id, ti.map_index, ti.state) for ti in dag_run_1.get_task_instances()]}"
    )
    assert _all_task_instances_succeeded(dag_run_1)
```
The retention DAG's own `dag.test()`-based test should load `"platform_retention"` the same way, mock whatever KPO tasks it launches (if any), and assert the DagRun reaches `success` — plus one retention-specific assertion the ingestion DAG tests have no analog for: prove the DAG's dry-run default actually ran in dry-run mode (e.g. assert on an XCom value or a mocked deletion-call count of zero) unless the test explicitly configures `enforce: true`. `pytestmark = pytest.mark.dagtest` (line 32) applies unchanged.

---

### `tests/policy/test_supply_chain_guards.py` (test, transform) — extend

**Analog:** self — `test_every_image_tag_agrees_with_versions_env` and its supporting readers (lines 314-397, read in full)

**Reader-function + agreement-check pattern to copy** (lines 341-397, in full):
```python
VERSIONS_ENV = REPO_ROOT / "helm" / "versions.env"
VALUES_LOCAL_DIR = REPO_ROOT / "helm" / "values" / "local"
VALUES_CI_DIR = REPO_ROOT / "helm" / "values" / "ci"

def minio_image_readings() -> dict[str, str]:
    return {
        "helm/versions.env": _versions_env_variable("MINIO_IMAGE_TAG"),
        "helm/values/local/minio.yaml": _values_field(VALUES_LOCAL_DIR / "minio.yaml", "image", "tag"),
        "helm/values/ci/minio.yaml": _values_field(VALUES_CI_DIR / "minio.yaml", "image", "tag"),
    }

IMAGE_TAG_READINGS: dict[str, Any] = {
    "minio": minio_image_readings,
    "airflow": airflow_image_readings,
}

def test_every_image_tag_agrees_with_versions_env() -> None:
    problems: list[str] = []
    for image, reader in IMAGE_TAG_READINGS.items():
        problems += image_tag_disagreements(image, reader())
    assert not problems, (...)
```
For Kyverno, this needs a **chart version**, not an image tag — the existing `helm-lint` Makefile target (lines 412-454) is the actual place `CNPG_OPERATOR_CHART_VERSION`/`MINIO_CHART_VERSION`/etc. are consumed (`lint_chart cnpg-operator cnpg/cloudnative-pg "$${CNPG_OPERATOR_CHART_VERSION}" cnpg-operator`), so the equivalent new coverage is: (a) add a `lint_chart kyverno kyverno/kyverno "$${KYVERNO_CHART_VERSION}" kyverno` line to the `helm-lint` target (with a `helm repo add kyverno https://kyverno.github.io/kyverno/` line alongside the other `repo add` calls), and (b) optionally add a Kyverno-specific reader here ONLY if Kyverno's values files also select an image tag independently of the chart version (unlike CNPG/MinIO, most Kyverno images float with the chart's own `appVersion` unless explicitly overridden — verify against the rendered chart before deciding this is needed).

**No existing analog for a trivy-invocation check** — no test in this repository currently greps for `trivy` anywhere (verified: zero matches repo-wide). The closest **structural** analog for "assert a workflow file invokes tool X the required way" is this same file's own `test_the_secret_scan_job_checks_out_full_history`/`test_a_scanning_job_exists_at_all` functions (lines 127-145) which grep `ci.yml` for the `gitleaks`/`fetch-depth: 0` invocations — a new `test_publish_workflow_scans_images_with_trivy` should grep `publish.yml` for `trivy image --severity HIGH,CRITICAL --exit-code 1` the same way, following this file's own established "read the workflow YAML as text, assert a required substring/pattern is present" idiom (also demonstrated in `test_removing_full_depth_is_reported`, line 158, which proves the check is non-vacuous by mutating the text and expecting a failure — CICD-08's D-10 "same gate for `pr-<number>` images" should get the identical non-vacuity treatment).

---

## Shared Patterns

### Actions pinned by commit SHA, never a tag
**Source:** `.github/workflows/ci.yml` lines 34-42 (every `uses:` step)
**Apply to:** All five new/extended workflow files
```yaml
- uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
```
Every new action (`docker/setup-buildx-action`, `docker/login-action`, `docker/build-push-action`, `sigstore/cosign-installer`, `actions/delete-package-versions`, `actions/upload-artifact`) needs its own `@<full-40-char-sha> # vX.Y.Z` pin, resolved from that action's own GitHub Releases page — never a floating tag (`@v4`, `@main`).

### `helm/versions.env` is the single declared version source
**Source:** `helm/versions.env` (whole file) + `tests/policy/test_pinned_tool_versions_agree.py` + `tests/policy/test_supply_chain_guards.py::test_every_image_tag_agrees_with_versions_env`
**Apply to:** `helm/values/{local,ci}/kyverno.yaml`, `Makefile`'s `helm-lint` target
No chart or tool version literal may appear anywhere except `helm/versions.env` — every other file (`Makefile`, values files, installer scripts) reads it via `source helm/versions.env` (shell) or the equivalent Python/YAML lookup. `KYVERNO_CHART_VERSION=3.8.2` is the one new line this phase adds here.

### Local/CI profile divergence — exactly three permitted axes, mechanically enforced
**Source:** `tests/policy/test_values_profiles.py` (`PERMITTED_AXES`, line 132; `test_both_profiles_exist_for_every_component`, line 211; `test_profiles_diverge_only_on_permitted_axes`, line 236) + every existing `helm/values/local/<X>.yaml`'s own header comment (e.g. `minio.yaml` lines 1-6, `cnpg-operator.yaml` lines 1-4)
**Apply to:** `helm/values/local/kyverno.yaml` / `helm/values/ci/kyverno.yaml`
```yaml
# D-06: the local and CI profiles diverge on EXACTLY three axes — replica
# counts, resource sizing, and monitoring. Any fourth divergence axis needs an
# argument in review.
```
This is enforced automatically by an existing test, not something the new values files need to re-implement — just don't invent a fourth divergence axis without arguing it in a comment first.

### Numbered stage script + shared `wait-for.sh`/`helm-install.sh` helpers
**Source:** `scripts/stages/40-cnpg-operator.sh`, `scripts/stages/80-vault.sh`, `scripts/stages/20-namespaces.sh`, `scripts/stages/75-etl.sh` (all read in full)
**Apply to:** `scripts/stages/25-kyverno.sh`, `scripts/stages/26-kyverno-policy.sh`
```bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=/dev/null
source "${repo_root}/helm/versions.env"
# shellcheck source=/dev/null
source "${repo_root}/scripts/helm-install.sh"
# shellcheck source=/dev/null
source "${repo_root}/scripts/wait-for.sh"
```
Every component stage sources these same three things and never re-implements `helm repo add`/wait-loop logic locally.

### The ONE permitted `kubectl apply` shape (committed `kubernetes/` path, or stdin)
**Source:** `tests/policy/test_no_manual_kubectl_surgery.py` lines 144-238 (full logic read); `scripts/stages/20-namespaces.sh`, `scripts/stages/75-etl.sh` (both existing instances)
**Apply to:** `scripts/stages/26-kyverno-policy.sh`, `kubernetes/kyverno-policy.yaml`
A `kubectl apply -f <path>` is permitted only when `<path>` is `-` (stdin) or contains a `kubernetes/` path segment. This is a hard policy-test gate — do not create a new manifest directory outside `kubernetes/` for anything that needs `kubectl apply`.

### Positive + negative live-proof (access-control claims)
**Source:** `tests/e2e/cluster/test_minio_buckets.py` (D-40, already merged), `tests/e2e/vault/test_positive_auth.py` + `test_negative_auth.py` (SEC-06/07/12, already merged)
**Apply to:** `tests/e2e/cluster/test_kyverno_admission.py` (D-18), the Vault check inside the D-20 PR smoke subset
Every access-control claim in this codebase is proven both ways: the legitimate path succeeds, AND the illegitimate path is denied with an assertion that the denial didn't silently no-op. Never ship only the positive or only the negative half.

### Opt-in, `None`-by-default per-dataset config block
**Source:** `packages/dataplat/src/dataplat/config/model.py` — `FreshnessConfig`/`QualityConfig`/`ReconciliationConfig`/`ScdConfig`, every one `X | None = None` on `DatasetConfig` (lines 611-617)
**Apply to:** The new `RetentionConfig` on `DatasetConfig`, and `configs/datasets/*.yaml`'s new `retention:` block
```python
model_config = ConfigDict(extra="forbid", frozen=True)
```
appears on literally every one of these classes — never omit it on the new class. A dataset with no `retention:` block in its YAML must behave identically to today (no retention enforcement), never silently inherit a platform-wide default.

### Constructor-parameterized threshold evaluator, never re-derives from context
**Source:** `packages/dataplat/src/dataplat/validate/circuit_breaker.py::RejectionRateCircuitBreaker`, `packages/dataplat/src/dataplat/scd/delete_detection.py::MassDeleteCircuitBreaker` (both read in full)
**Apply to:** The new retention dry-run evaluator
Construct once per run with already-known totals; `apply`/`evaluate` never re-queries; a `total == 0` trivial-pass/no-op guard is always the first branch.

### `Makefile` is the single quality-gate definition; CI calls `make <target>` only
**Source:** `Makefile` lines 1-2, 495-496; `.github/workflows/ci.yml` line 48 (`- run: make check`)
**Apply to:** Every new CI job in `publish.yml`/`e2e-*.yml` — delegate substantive work to a Make target wherever one exists (`make cluster-up`, `make cluster-verify`, `make rebuild-from-raw`), never inline the equivalent shell/pytest invocation directly in workflow YAML. `tests/policy/test_ci_calls_make_ci.py` and `test_ci_invokes_make_only.py` already enforce this for the existing jobs and will very likely need sibling assertions (or a generalized version) for the new ones.

### Immutable git-SHA image tags, never `:latest`
**Source:** `Makefile` lines 291-340 (`GIT_SHA := $(shell git rev-parse --short HEAD)`, `image-csv-processor`); `tests/policy/test_no_latest_image_tag.py`
**Apply to:** `publish.yml`'s image tags (git-SHA on merge, `pr-<number>` on PR per D-09, plus a semver tag on release per D-03 — never `:latest` for any of them)

## No Analog Found

| File | Role | Data Flow | Reason |
|---|---|---|---|
| `.trivyignore` | config | N/A | Does not exist yet by design (D-07: created live only when a real, dated, justified HIGH/CRITICAL finding occurs — do not pre-seed it). No repository convention for its format exists yet either; follow trivy's own upstream `.trivyignore` format (`CVE-ID`/`# comment` pairs) when the need arises. |
| `docs/runbooks/*.md` structural shape | doc | N/A | No existing file in this repository has this exact "symptoms/diagnosis/recovery/reprocessing/verification per scenario" shape — only content-source analogs exist (`.planning/debug/resolved/*.md` for 5 scenarios) and one tone-analog (`docs/ci-branch-protection.md`, a procedural how-to). Treat D-41's own listed sections as the structural spec directly; there is nothing closer to copy from. |
| Trivy-invocation policy check | test | transform | No test in this repository currently references `trivy` (verified: zero repo-wide matches). `test_supply_chain_guards.py`'s own "grep the workflow YAML for a required substring, prove non-vacuity by mutating it" idiom (lines 127-181) is the closest *structural* pattern, but there is no trivy-specific precedent to extend — this is genuinely new ground within an established idiom, not a gap. |

## Metadata

**Analog search scope:** `.github/workflows/`, `helm/values/{local,ci}/`, `helm/versions.env`, `kubernetes/`, `scripts/stages/`, `docs/adr/`, `docs/` (top-level), `.planning/debug/resolved/`, `airflow/dags/` (incl. `_common/`), `configs/datasets/`, `packages/dataplat/src/dataplat/{config,pipeline,metadata,validate,scd,cli.py}`, `Makefile`, `tests/{e2e,unit,dagtest,policy}/`

**Files scanned (read in full or via targeted offset/limit):** 40 (`ci.yml`, `Makefile`, `customers.yaml`, `config/registry.py`, `config/model.py` [targeted], `validate/circuit_breaker.py`, `scd/delete_detection.py`, `csv_ingest_customers.py`, `smoke_kubernetes_pod.py`, `_common/kpo.py`, `helm/values/{local,ci}/minio.yaml`, `helm/values/local/vault.yaml`, `helm/values/local/cnpg-operator.yaml`, `tests/e2e/cluster/test_minio_buckets.py`, `tests/e2e/vault/test_{positive,negative}_auth.py`, `scripts/stages/{20-namespaces,40-cnpg-operator,75-etl,80-vault}.sh`, `docs/adr/{0000-template,0010-dbt-silver-layer-boundary}.md`, `.planning/debug/resolved/{dagrun-scheduler-stall,wait-for-files-stuck-task}.md`, `docs/README.md`, `dataplat/cli.py`, `pipeline/run.py` [targeted], `metadata/repository.py` [targeted], `tests/dagtest/test_backfill_dagrun.py`, `tests/policy/test_no_manual_kubectl_surgery.py` [targeted], `tests/policy/test_supply_chain_guards.py` [targeted], `tests/unit/validate/test_circuit_breaker.py`, `helm/versions.env`, plus directory listings/grep passes over `tests/e2e/*`, `tests/policy/*`, `tests/unit/*`, `packages/dataplat/src/dataplat/*`)

**Pattern extraction date:** 2026-08-22
