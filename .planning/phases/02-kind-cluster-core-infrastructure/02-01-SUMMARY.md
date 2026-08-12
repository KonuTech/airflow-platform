---
phase: 02-kind-cluster-core-infrastructure
plan: 01
subsystem: infra
tags: [kind, helm, kubeadm, kubelet, ingress-nginx, docker-registry, uv, cluster-bootstrap]

# Dependency graph
requires: []
provides:
  - "helm/versions.env — single source of every Phase 2 chart/tool/image pin"
  - "tools/k8s/install_{kind,helm,kubeconform}.sh — pinned, digest-verified binary installers"
  - "root `cluster` uv dependency group (boto3, psycopg[binary]), isolated from the offline gate"
  - "kind/cluster.yaml — the whole creation-time-only cluster surface (3 nodes, fair-share kubelet reservations, extraPortMappings, extraMounts, containerd registry config)"
  - "kubernetes/namespaces.yaml — the five D-13 namespaces"
  - "scripts/{helm-install,wait-for,cluster-up,cluster-down}.sh and scripts/stages/{10-registry,20-namespaces,30-ingress-nginx}.sh — the D-09 bootstrap spine"
  - "helm/values/{local,ci}/ingress-nginx.yaml — first component on both profiles"
  - "Makefile targets: install-cluster, cluster-up, cluster-down, stage-%"
affects: [02-02, 02-03, 02-04, 02-05, 02-06, 02-07, 02-08]

# Tech tracking
tech-stack:
  added: [kind 0.32.0, helm 4.2.3, kubeconform 0.8.0, ingress-nginx chart 4.15.1, boto3, psycopg[binary]]
  patterns:
    - "Pinned-binary installer pattern (tools/security/install_gitleaks.sh) extended to tools/k8s/install_{kind,helm,kubeconform}.sh"
    - "helm/versions.env as the single source of chart/tool/image version pins, enforced by tests/policy/test_pinned_tool_versions_agree.py"
    - "Sourceable helm_install()/wait_for_*() shell helpers instead of inline helm/kubectl calls in every stage script"
    - "scripts/stages/*.sh run in LC_ALL=C lexical order by scripts/cluster-up.sh; stage-% gives each one an individually invocable (but non-bootstrapping) Make target"

key-files:
  created:
    - helm/versions.env
    - tools/k8s/install_kind.sh
    - tools/k8s/install_helm.sh
    - tools/k8s/install_kubeconform.sh
    - kind/cluster.yaml
    - kubernetes/namespaces.yaml
    - scripts/helm-install.sh
    - scripts/wait-for.sh
    - scripts/cluster-up.sh
    - scripts/cluster-down.sh
    - scripts/stages/10-registry.sh
    - scripts/stages/20-namespaces.sh
    - scripts/stages/30-ingress-nginx.sh
    - helm/values/local/ingress-nginx.yaml
    - helm/values/ci/ingress-nginx.yaml
  modified:
    - .gitignore
    - pyproject.toml
    - uv.lock
    - tests/policy/test_pinned_tool_versions_agree.py
    - Makefile

key-decisions:
  - "Checkpoint (human-confirmed): fair-share kubelet reservations — systemReserved {cpu: 11, memory: 8Gi}, kubeReserved {cpu: 10, memory: 7Gi}, evictionHard {memory.available: 500Mi, nodefs.available: 10%, nodefs.inodesFree: 5%}, maxPods: 60 — over the PITFALLS-proposed 500m/1Gi, which 02-RESEARCH.md measured as removing only 3% of a node's allocatable"
  - "maxPods and both extraMounts host paths confirmed as proposed: airflow/dags -> /mnt/dags (ro, D-02), $HOME/.local/share/airflow-platform/pv -> /mnt/persist (declared-but-unbound, D-01)"
  - "Rule 2 addition: tools/k8s/install_kubeconform.sh, not named in the plan's Task 1 file list, added because the plan's own acceptance criteria require a working kubeconform_readings() case in test_pinned_tool_versions_agree.py now, which needs a second live source beyond helm/versions.env"
  - "Rule 3 addition: installed uv 0.12.3 via the official installer (was entirely absent on this machine) before running uv lock, per the Makefile's own uv-guard remediation text"

requirements-completed: [INFRA-01, INFRA-07, INFRA-09]

# Metrics
duration: ~2h10m active work (Task 1 ~55min, human checkpoint decision, Task 2 ~75min); excludes idle time waiting on the checkpoint
completed: 2026-08-12
---

# Phase 2 Plan 1: kind Cluster Bootstrap Spine Summary

**Committed the entire Phase 2 creation-time cluster surface, pinned-tool installers, and the D-09 stage-runner bootstrap path (kind + local registry + namespaces + ingress-nginx); live `make cluster-up` verification could not complete on this execution host because its kubelet's cgroup v1 configuration is refused outright by the kindest/node image, confirmed independent of any file in this plan.**

## Performance

- **Duration:** ~2h10m of active execution across two sessions, separated by the human checkpoint decision on kubelet sizing
- **Tasks:** 2/2 addressed (Task 1 fully verified; Task 2 file-complete and statically verified, live cluster verification blocked — see Known Issues)
- **Files modified:** 21 (8 in Task 1, 13 in Task 2, including 4 `.gitkeep` deletions)

## Accomplishments

- Closed WINDOWS #7 (`.gsd/` now gitignored) and landed the cross-phase `cluster` uv dependency group (`boto3`, `psycopg[binary]`), isolated from `dev` so the offline gate's environment never imports `boto3` — verified with a paired positive/negative control
- Registered `cluster` and `manifests` pytest markers for `--strict-markers`
- Added `helm/versions.env` as the single source of every Phase 2 chart/tool/image pin, and three pinned, digest-verified installers (`kind`, `helm`, `kubeconform`) following the Phase 1 `install_gitleaks.sh` six-stage pattern — all digests measured by downloading each artifact in this session and cross-checked against upstream's own checksum files
- Extended `tests/policy/test_pinned_tool_versions_agree.py` with `kind_readings()`, `helm_readings()`, `kubeconform_readings()` and a `_versions_env_variable()` reader
- Wrote the entire creation-time-only cluster surface in `kind/cluster.yaml` (3 nodes, node labels, the human-confirmed fair-share kubelet reservations, `extraPortMappings` 80/443, both `extraMounts`, containerd registry config) and nowhere else in the repository
- Wrote the D-09 bootstrap spine: `scripts/cluster-up.sh` (idempotent, `LC_ALL=C` ordered stage runner), `scripts/cluster-down.sh` (safe no-op against a missing cluster), `scripts/helm-install.sh` and `scripts/wait-for.sh` (sourceable helpers), and the three stage scripts (local registry, the one permitted `kubectl apply`, ingress-nginx)
- Wrote both `helm/values/{local,ci}/ingress-nginx.yaml` profiles — hostPort 80/443, ClusterIP, `ingress-ready` nodeSelector + control-plane toleration, `watchIngressWithoutClass`, default `IngressClass nginx`, explicit CPU/memory requests+limits on every container including both admission-webhook Jobs — diverging on exactly the three D-06 axes
- Added `install-cluster`, `cluster-up`, `cluster-down` and the `stage-%` pattern rule to the Makefile, with the D-09 substitution recorded inline and `help`'s grep widened so `stage-%` is discoverable

## Task Commits

1. **Task 1: Land the cross-phase dependency, close WINDOWS #7, install pinned kind/helm binaries** - `420cc51` (feat)
2. **Task 2: End-to-end tracer — cluster-up path** - `eecfed0` (feat) — files complete and statically verified; live `make cluster-up` verification blocked, see Known Issues

_No plan-metadata commit yet — see "Next Phase Readiness" for why this SUMMARY documents an incomplete verification state rather than closing the plan._

## Files Created/Modified

- `helm/versions.env` - single source of kind/helm/kubeconform/chart/image version pins
- `tools/k8s/install_kind.sh`, `tools/k8s/install_helm.sh`, `tools/k8s/install_kubeconform.sh` - pinned, digest-verified binary installers
- `pyproject.toml`, `uv.lock` - `cluster` dependency group, `cluster`/`manifests` pytest markers
- `.gitignore` - `.gsd/` ignored (WINDOWS #7)
- `tests/policy/test_pinned_tool_versions_agree.py` - kind/helm/kubeconform version-agreement readings
- `kind/cluster.yaml` - the entire creation-time-only cluster surface
- `kubernetes/namespaces.yaml` - the five D-13 namespaces
- `scripts/helm-install.sh`, `scripts/wait-for.sh` - sourceable D-09 helpers
- `scripts/cluster-up.sh`, `scripts/cluster-down.sh` - the only bootstrap/teardown entry points
- `scripts/stages/10-registry.sh`, `20-namespaces.sh`, `30-ingress-nginx.sh` - the ordered component stages
- `helm/values/local/ingress-nginx.yaml`, `helm/values/ci/ingress-nginx.yaml` - first component values profiles
- `Makefile` - `PROFILE`, `RUN_CLUSTER`, `install-cluster`, `cluster-up`, `cluster-down`, `stage-%`

## Decisions Made

- Checkpoint decision (human-confirmed, relayed by the coordinator): fair-share kubelet reservations, `maxPods: 60`, and both `extraMounts` host paths as proposed in the plan — see frontmatter `key-decisions` for the exact values and rationale.
- Kept `tools/k8s/install_kubeconform.sh` in Task 1's scope (Rule 2) despite it not appearing in the plan's own Task 1 file list, because the plan's acceptance criteria require a working `kubeconform_readings()` case immediately.
- Registry stage (`scripts/stages/10-registry.sh`) deliberately does **not** write the `local-registry-hosting` ConfigMap documented at kind.sigs.k8s.io/docs/user/local-registry/, to preserve the plan's "one permitted `kubectl apply`" invariant (the namespaces stage). This is a narrower implementation than the upstream convention; revisit if a later plan needs that ConfigMap for tooling discovery.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Installed uv, which was entirely absent from the host**
- **Found during:** Task 1, before `uv lock`
- **Issue:** `uv` was not on `PATH` anywhere on this machine (contradicting PROJECT.md's "uv 0.8.11 installed" note), so `uv lock`/`uv sync` could not run at all.
- **Fix:** Installed the pinned `uv 0.12.3` via the official installer script (`curl -LsSf https://astral.sh/uv/0.12.3/install.sh | sh`) — the exact remediation the Makefile's own `uv-guard` target prints. Not a package-manager install of a third-party dependency; this is the project-mandated build tool itself.
- **Files modified:** none (host tooling only)
- **Verification:** `uv --version` → `0.12.3`; `make check`-equivalent policy suite green afterward
- **Committed in:** n/a (host-level, no repo file changed)

**2. [Rule 2 - Missing critical functionality] Added `tools/k8s/install_kubeconform.sh`**
- **Found during:** Task 1, while extending `test_pinned_tool_versions_agree.py`
- **Issue:** The plan's Task 1 `<files>` list only names `install_kind.sh`/`install_helm.sh`, but its action text and acceptance criteria require `kubeconform_readings()` to exist and the whole test module to exit 0 with a working kubeconform case now. Without a second live source (an installer's `PINNED_VERSION`), `test_every_source_is_load_bearing` would fail immediately.
- **Fix:** Added `tools/k8s/install_kubeconform.sh` following the identical six-stage pattern used for kind/helm, with digests measured and cross-checked against upstream (`yannh/kubeconform` release `CHECKSUMS`) in this session.
- **Files modified:** `tools/k8s/install_kubeconform.sh`, `tests/policy/test_pinned_tool_versions_agree.py`
- **Verification:** installer runs, is idempotent on a second run, `kubeconform -v` reports `v0.8.0`; `pytest tests/policy/test_pinned_tool_versions_agree.py -q` passes
- **Committed in:** `420cc51`

**3. [Rule 1 - Bug/consistency] Removed stale `.gitkeep` from now-populated directories**
- **Found during:** Task 2, before committing
- **Issue:** `kubernetes/`, `scripts/`, `helm/values/{local,ci}/` each carried a placeholder `.gitkeep` that no longer serves any purpose once real files exist there — inconsistent with every other populated directory in the repo.
- **Fix:** Deleted the four `.gitkeep` files.
- **Files modified:** `kubernetes/.gitkeep`, `scripts/.gitkeep`, `helm/values/local/.gitkeep`, `helm/values/ci/.gitkeep` (all deleted)
- **Verification:** `git status` clean; directories still populated
- **Committed in:** `eecfed0`

---

**Total deviations:** 3 (1 Rule 3, 1 Rule 2, 1 Rule 1)
**Impact on plan:** All three were necessary for correctness/completeness of what the plan itself asks for; no scope creep beyond the plan's own stated acceptance criteria.

## Issues Encountered

**Live `make cluster-up` could not be verified on this execution host.** Two independent attempts (the plan's actual `kind/cluster.yaml`, and a fully vanilla `kind create cluster` with zero custom configuration) both failed identically at the `kubeadm init` step with `connection refused` to the API server on `:6443`. Diagnosis (via a `--retain`ed control-plane container's `journalctl -u kubelet`):

```
kubelet[...]: E... "command failed" err="failed to validate kubelet configuration, error: kubelet is
configured to not run on a host using cgroup v1. cgroup v1 support is unsupported and will be removed
in a future release, ..."
```

`docker info` on this host confirms `Cgroup Version: 1` (`Cgroup Driver: cgroupfs`), and `free -h` / `nproc` report **12 CPUs / 15 GiB RAM total** — sharply smaller than PROJECT.md's documented "32 CPUs, 47 GB RAM" environment that 02-RESEARCH.md's live verification ran against, and on cgroup v1 rather than the cgroup v2 02-RESEARCH.md assumed. The `kindest/node` kubelet build refuses to start on cgroup v1 **unconditionally**, before any of this plan's `KubeletConfiguration` patch (reservations, `maxPods`, `evictionHard`) is ever evaluated — the vanilla-config probe proves this is not caused by anything committed in this plan.

This is exactly the class of problem CONTEXT.md's D-11 anticipated: `docs/wsl/wslconfig.example` and the WSL2-side fix are explicitly scoped as "a deliberate human act — it needs `wsl --shutdown` and lives on the Windows side," which is outside what any agent running inside this WSL2 distro can perform. No file change in this repository can fix a cgroup v1 kubelet host.

**Resolution status:** unresolved, requires human action. Cleaned up all probe clusters/containers (`kind delete cluster` ×3, confirmed `kind get clusters` reports none, confirmed no leftover `kind-registry` container). The `kind` Docker network created by kind's own bootstrap remains (harmless, kind's normal behavior across cluster create/delete cycles).

## User Setup Required

**To unblock live verification of this plan, the host's WSL2 distro needs cgroup v2.** This is the same remediation CONTEXT.md's D-11 already names:
1. On the Windows side, edit (or create) `%UserProfile%\.wslconfig` — Phase 2's own `docs/wsl/wslconfig.example` (not yet written; a later plan's deliverable per D-11) is the intended reference, but the load-bearing setting here specifically is enabling **systemd** support (WSL2 defaults to cgroup v2 once systemd is enabled in `/etc/wsl.conf`'s `[boot] systemd=true`, or via a newer WSL2 kernel with cgroup v2 as default) — verify against current Microsoft WSL2 documentation, since the exact mechanism has moved between WSL versions.
2. Run `wsl --shutdown` from PowerShell/cmd (not from inside the distro).
3. Re-open the WSL2 terminal and confirm with `docker info | grep -i cgroup` → expect `Cgroup Version: 2`.
4. Re-run `make cluster-down && make cluster-up && make cluster-up` (the plan's tracer `<verify>` block) to complete this plan's live verification.

## Known Stubs

None. Every file this plan commits is the real, intended implementation — nothing is a placeholder pending later wiring.

## Next Phase Readiness

**File-level implementation is complete and statically verified**: `kind/cluster.yaml` parses correctly with exactly 3 nodes each carrying a `KubeletConfiguration` patch; both ingress-nginx values profiles render cleanly under `helm template`; `make install-cluster` installs `boto3`/`psycopg` under `--group cluster` and they import successfully; the full `pytest tests/policy -q` suite (58 tests) passes; `make -n install-cluster` and the `RUN_CLUSTER` Makefile variable both name `--group cluster`; the `stage-%` D-09-substitution comment is in place.

**Blocked**: the plan's `<done>` criterion ("A single `make cluster-up` on a machine with only Docker produces a 3-node cluster...") and the tracer task's `<verify>` automated block (which asserts `kubectl get nodes` reports 3 `Ready` nodes, the registry hosts.toml is wired, and the ingress returns HTTP 404) could not be exercised end-to-end in this session — not because of anything wrong in the committed files, but because this execution host's Docker/WSL2 runs cgroup v1, which every `kindest/node` kubelet build refuses outright.

**Recommendation:** either (a) re-run this plan's live verification (`make cluster-down && make cluster-up && make cluster-up`, then the curl/kubectl assertions from the tracer task's `<verify>` block) on a cgroup v2 host before treating plan 02-01 as fully done, or (b) have a human apply the WSL2 fix above and re-run. Plans 02-02 through 02-08 all build on the assumption that `make cluster-up` produces a real, reachable cluster — none of them should be attempted until this is confirmed working on whatever host will run them.

---
*Phase: 02-kind-cluster-core-infrastructure*
*Completed: 2026-08-12 (file-level; live verification pending — see Next Phase Readiness)*

## Self-Check: PASSED

- All 15 created files verified present on disk (`helm/versions.env`, `tools/k8s/install_{kind,helm,kubeconform}.sh`, `kind/cluster.yaml`, `kubernetes/namespaces.yaml`, `scripts/{helm-install,wait-for,cluster-up,cluster-down}.sh`, `scripts/stages/{10-registry,20-namespaces,30-ingress-nginx}.sh`, `helm/values/{local,ci}/ingress-nginx.yaml`)
- Both task commits verified present in `git log`: `420cc51`, `eecfed0`
