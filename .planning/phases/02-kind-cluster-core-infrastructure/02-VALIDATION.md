---
phase: 2
slug: kind-cluster-core-infrastructure
# status lifecycle: draft (seeded by plan-phase) → validated (set by validate-phase §6)
# audit-milestone §5.5 distinguishes NOT-VALIDATED (draft) from PARTIAL (validated + nyquist_compliant: false) (#2117)
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-08-12
---

# Phase 2 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from `02-RESEARCH.md` § Validation Architecture (verified against a live kind cluster).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 9.1.x (`minversion = "9.0"`, `addopts = "-ra --strict-markers --strict-config"`) |
| **Config file** | `pyproject.toml` (`[tool.pytest.ini_options]`, `testpaths = ["tests"]`) |
| **Quick run command** | `make policy` → `uv run --frozen pytest tests/policy -q -m "not manifests"` — the deselected tests need rendered output and the network-installed helm/kubeconform binaries, and run under `make manifest-policy` |
| **Full suite command** | `make check` (offline, cluster-free); `make ci` adds `manifest-policy` and the secret scan |
| **Render-dependent suite** | `make manifest-policy` (declares `manifests` as a prerequisite) → `REQUIRE_RENDERED_MANIFESTS=1 uv run --frozen pytest tests/policy -q -m manifests` — in `ci`, never in `check` |
| **Live-cluster suite** | `make cluster-verify` → `uv run --frozen --group cluster pytest tests/e2e/cluster -q` — **deliberately wired into neither `check` nor `ci`**. The `--group cluster` is load-bearing: `boto3` and `psycopg` live in a non-default dependency group so the offline gate cannot import them |
| **Estimated runtime** | `make policy` sub-second · `make check` seconds · `make cluster-verify` requires a live cluster |

**New markers required:** `cluster: requires a live kind cluster` and `manifests: requires rendered
manifests under build/ and the pinned helm and kubeconform binaries` must both be added to `markers`
in `[tool.pytest.ini_options]`, or `--strict-markers` rejects them. Both are registered in plan
02-01 so the registry lands in one commit.

**WINDOWS #8 applies:** `make check` names test paths explicitly, so a new test directory is
silently uncollected until named. `tests/policy/` is already collected; `tests/e2e/cluster/` must
get its own target and must **not** join `make check`, which is contractually offline.

---

## Sampling Rate

- **After every task commit:** `make policy` — static, sub-second, needs no cluster and no network
- **After every plan wave:** `make check` — the full offline gate. **`make manifests` does NOT join `check`**: it fetches pinned charts over the network, and `check` must stay runnable on a fresh clone with nothing running (Phase 1 success criterion 4). `manifests` joins `ci`, following the `gitleaks` precedent exactly — and must be ordered *ahead of* `policy` in the `ci` chain, or the rendered-manifest tests skip and the sizing gate measures nothing. The plans realize that ordering as a prerequisite edge rather than as list position (`manifest-policy: manifests`), because position guarantees nothing under `make -j`; and the render-dependent tests fail rather than skip when `REQUIRE_RENDERED_MANIFESTS` is set, so "green" cannot mean "measured nothing".
- **Phase gate:** `make cluster-up && make cluster-verify` green, then `make cluster-rebuild && make cluster-verify` green a **second** time. One pass proves it works; two passes prove it is reproducible, which is what INFRA-01 actually claims.
- **Before `/gsd-verify-work`:** full suite green
- **Max feedback latency:** < 5 s for the per-commit gate

---

## Per-Task Verification Map

Task IDs are assigned by the planner. The requirement → test mapping below is fixed by research;
the planner must attach each row to a task and `validate-phase` completes the ID column.

| Requirement | Behavior | Test Type | Automated Command | File Exists | Status |
|-------------|----------|-----------|-------------------|-------------|--------|
| INFRA-01 | `cluster.yaml` declares 3 nodes, both `extraMounts`, both `extraPortMappings`, `containerdConfigPatches`, and a `KubeletConfiguration` patch on every node | policy (static) | `pytest tests/policy/test_kind_cluster_config.py -x` | ❌ W0 | ⬜ pending |
| INFRA-01 | destroy + recreate produces a working cluster | e2e (live) | `make cluster-rebuild && make cluster-verify` | ❌ W0 | ⬜ pending |
| INFRA-02 | four Airflow workloads Ready as separate objects — 3 Deployments + 1 **StatefulSet** (triggerer) | e2e | `pytest tests/e2e/cluster/test_airflow_workloads.py -x` | ❌ W0 | ⬜ pending |
| INFRA-02 | the Airflow UI answers through the ingress | e2e | `pytest tests/e2e/cluster/test_ingress.py -x` (HTTP 200 from `http://airflow.localtest.me/`) | ❌ W0 | ⬜ pending |
| INFRA-03 | metadata cluster reports PostgreSQL 17 | e2e | `pytest tests/e2e/cluster/test_postgres_topology.py::test_metadata_is_pg17 -x` | ❌ W0 | ⬜ pending |
| INFRA-04 | analytical cluster reports PostgreSQL 18; two distinct `Cluster` resources; disjoint PVCs and nodes | e2e | `…::test_two_distinct_clusters_no_shared_storage -x` | ❌ W0 | ⬜ pending |
| INFRA-05 | five buckets reachable as `s3://bucket/key` **via boto3** | e2e | `pytest tests/e2e/cluster/test_minio_buckets.py -x` | ❌ W0 | ⬜ pending |
| INFRA-05 / §63 | the application credential is **refused** on `DeleteObject` against `raw`; the admin credential is not | e2e (negative) | `…::test_raw_delete_is_denied_for_app_credential -x` | ❌ W0 | ⬜ pending |
| INFRA-07 | no `kubectl create/edit/patch/apply` outside committed manifests in any script | policy | `pytest tests/policy/test_no_manual_kubectl_surgery.py -x` | ❌ W0 | ⬜ pending |
| INFRA-09 | reservations + `maxPods` present in `cluster.yaml`; live node allocatable below a declared ceiling | policy + e2e | `test_kind_cluster_config.py` and `tests/e2e/cluster/test_node_capacity.py` | ❌ W0 | ⬜ pending |
| INFRA-10 | both profiles render; they differ on **only** replicas, resources and monitoring | policy | `pytest tests/policy/test_values_profiles.py -x` | ❌ W0 | ⬜ pending |
| CICD-07 | `kubeconform -strict` passes on both rendered profiles and **fails** on a deliberately broken manifest | policy (non-vacuity, `manifests` marker — ci-only, needs the downloaded binary) | `make manifest-policy` | ❌ W0 | ⬜ pending |
| CICD-07 / D-12 #1 | summed container requests over the CI profile ≤ 4 CPU / 16 GB, **including** CNPG `Cluster` CRs | policy (`manifests` marker — runs after the render, fails rather than skips without it) | `make manifest-policy` | ❌ W0 | ⬜ pending |
| CICD-07 / D-12 #2 | every container in both profiles has CPU + memory requests and limits | policy | `…::test_every_container_is_sized -x` | ❌ W0 | ⬜ pending |
| D-10 | `doctor` exits non-zero on each failure class it claims to block | policy (fault injection) | `pytest tests/policy/test_doctor_fails_closed.py -x` | ❌ W0 | ⬜ pending |
| D-14 | no credential literal in any values file, manifest or script | policy | widen `tests/policy/test_workflow_secrets.py` to `helm/`, `kubernetes/`, `kind/`, `scripts/` | ⚠️ exists, needs widening | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/policy/test_kind_cluster_config.py` — INFRA-01, INFRA-09
- [ ] `tests/policy/test_values_profiles.py` — INFRA-10 / D-06 three-axis rule
- [ ] `tests/policy/test_manifest_resources.py` — D-12 tests 1 and 2 (with the CNPG `Cluster` CR special case)
- [ ] `tests/policy/test_manifest_validation_fails_closed.py` — CICD-07 non-vacuity
- [ ] `tests/policy/test_doctor_fails_closed.py` — D-10 non-vacuity
- [ ] `tests/policy/test_no_manual_kubectl_surgery.py` — INFRA-07
- [ ] `tests/e2e/cluster/conftest.py` — shared fixtures: kube client / `kubectl` shell helper, boto3 client built from `make minio-creds`, the `cluster` marker, skip-if-no-cluster
- [ ] `tests/e2e/cluster/{test_airflow_workloads,test_postgres_topology,test_minio_buckets,test_ingress,test_node_capacity}.py`
- [ ] Makefile targets: `doctor`, `cluster-up`, `cluster-down`, `cluster-rebuild`, `cluster-verify`, `minio-creds`, `manifests`, `helm-lint`; wire `manifests` into `check`, and `cluster-verify` into **nothing**
- [ ] Root dependency group `cluster = ["boto3", "psycopg[binary]"]` + `uv lock` — **cross-phase coordination point, see RESEARCH Open Question 1**
- [ ] `markers` entry for `cluster` in `[tool.pytest.ini_options]` (required by `--strict-markers`)

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| `.wslconfig` applied on the Windows host | D-11 | Lives outside WSL and requires `wsl --shutdown`; `doctor` can assert the resulting floor but cannot apply the file | Copy `docs/wsl/wslconfig.example` to `C:\Users\<you>\.wslconfig`, run `wsl --shutdown`, reopen, then `make doctor` |
| Unmaintained-upstream acknowledgement (`pgsty/minio`, ingress-nginx 1.15.1, `mc`) | ADR-0006 | A supply-chain risk acceptance is a human decision, not a test assertion | Read ADR-0006 and confirm each named artifact is one you accept |
| Rebuild wall-clock against the ~15 min budget | D-04 | Deliberately warn-only — measures the network, not the repository | Read the per-stage breakdown printed by `make cluster-rebuild` |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 5 s for the per-commit gate
- [ ] Both non-vacuity tests present (`doctor` and `kubeconform` each observed failing)
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
