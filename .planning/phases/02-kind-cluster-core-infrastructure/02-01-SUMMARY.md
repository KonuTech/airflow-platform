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
  - "Checkpoint (human-confirmed): fair-share kubelet reservations, originally systemReserved {cpu: 11, memory: 8Gi}, kubeReserved {cpu: 10, memory: 7Gi} — over the PITFALLS-proposed 500m/1Gi, which 02-RESEARCH.md measured as removing only 3% of a node's allocatable"
  - "Post-checkpoint rescale (same session, human-confirmed): the original fair-share numbers assumed a 32-CPU/47-GiB host and made kubelet refuse to start on this developer's actual 12-CPU/32-GiB laptop (capacity < reservation). Rescaled to systemReserved {cpu: 2, memory: 3Gi}, kubeReserved {cpu: 2, memory: 2Gi}, same evictionHard/maxPods: 60, plus a WSL2 memory cap bump (.wslconfig [wsl2] memory=24GB) to give the VM headroom above the default ~50%-of-host allocation"
  - "maxPods and both extraMounts host paths confirmed as proposed: airflow/dags -> /mnt/dags (ro, D-02), $HOME/.local/share/airflow-platform/pv -> /mnt/persist (declared-but-unbound, D-01)"
  - "Rule 2 addition: tools/k8s/install_kubeconform.sh, not named in the plan's Task 1 file list, added because the plan's own acceptance criteria require a working kubeconform_readings() case in test_pinned_tool_versions_agree.py now, which needs a second live source beyond helm/versions.env"
  - "Rule 3 addition: installed uv 0.12.3 via the official installer (was entirely absent on this machine) before running uv lock, per the Makefile's own uv-guard remediation text"

requirements-completed: [INFRA-01, INFRA-07, INFRA-09]

# Metrics
duration: ~2h10m active work (Task 1 ~55min, human checkpoint decision, Task 2 ~75min) plus a follow-up session resolving two host-environment blockers (cgroup v1, kubelet reservation sizing) and completing live verification
completed: 2026-08-12
---

# Phase 2 Plan 1: kind Cluster Bootstrap Spine Summary

**Committed the entire Phase 2 creation-time cluster surface, pinned-tool installers, and the D-09 stage-runner bootstrap path (kind + local registry + namespaces + ingress-nginx). Live `make cluster-up` verification initially could not complete on this execution host — first blocked by Docker Desktop running cgroup v1, then by kubelet reservations sized for a 32-CPU/47-GiB host that exceeded this 12-CPU/32-GiB laptop's real capacity. Both were host-environment issues, not plan defects; both are now resolved and the plan's full automated `<verify>` block passes.**

## Performance

- **Duration:** ~2h10m of active execution across two sessions, separated by the human checkpoint decision on kubelet sizing, plus a follow-up session to resolve host-environment blockers and complete live verification
- **Tasks:** 2/2 complete and fully verified, including the live cluster tracer
- **Files modified:** 22 (8 in Task 1, 13 in Task 2 including 4 `.gitkeep` deletions, plus `kind/cluster.yaml` rescaled post-checkpoint)

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
2. **Task 2: End-to-end tracer — cluster-up path** - `eecfed0` (feat)
3. **Fix: rescale kubelet reservations for real host capacity** - `9cdf4e5`+ (fix) — see Deviations/Issues below

Live verification complete: `make cluster-down && make cluster-up && make cluster-up` all succeed, 3 nodes `Ready`, all 5 namespaces `Active`, ingress-nginx controller `Available`, `tracer.localtest.me` returns `404` — the exact command sequence and expected output from the plan's Task 2 `<verify><automated>` block.

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

**4. [Rule 1 - Bug/consistency] Rescaled kubelet reservations to fit the actual execution host**
- **Found during:** live verification, after the host's cgroup v1 issue (below) was resolved
- **Issue:** The checkpoint-approved fair-share numbers (`systemReserved` cpu=11/mem=8Gi + `kubeReserved` cpu=10/mem=7Gi = 21 cores/15Gi reserved) were sized for the 32-CPU/47-GiB host assumed throughout STACK.md/02-RESEARCH.md. This developer's actual laptop has 12 CPU/32GiB total (WSL2 defaulting to ~15.44GiB of that). kubelet refused to start on **every** node: `"invalid Node Allocatable configuration... capacity of 12 but reservation of 21... Expected capacity >= reservation"`.
- **Fix:** confirmed with the human (same session): raised the WSL2 memory cap to 24GiB via `%UserProfile%\.wslconfig` (`[wsl2] memory=24GB`), and rescaled `kind/cluster.yaml`'s three `KubeletConfiguration` patches to `systemReserved: {cpu: "2", memory: "3Gi"}`, `kubeReserved: {cpu: "2", memory: "2Gi"}` — same `evictionHard`/`maxPods: 60` as approved. ~8 CPU/~19GiB allocatable per node; conservative enough to also work unmodified on the user's other dev machine (a 64GiB PC).
- **Files modified:** `kind/cluster.yaml`
- **Verification:** full plan `<verify><automated>` block passes (see Issues Encountered)
- **Committed in:** fix commit following `9cdf4e5` (the earlier worktree merge)

---

**Total deviations:** 4 (1 Rule 3, 1 Rule 2, 2 Rule 1)
**Impact on plan:** All four were necessary for correctness/completeness of what the plan itself asks for; no scope creep beyond the plan's own stated acceptance criteria.

## Issues Encountered

**Two host-environment blockers, both now resolved, in sequence:**

**1. cgroup v1 (resolved).** Two independent attempts (the plan's actual `kind/cluster.yaml`, and a fully vanilla `kind create cluster` with zero custom configuration) both failed identically at `kubeadm init` because Docker Desktop's engine ran cgroup v1 (`kubelet: "cgroup v1 support is unsupported"`) — confirmed via a `--retain`ed control-plane container's `journalctl -u kubelet`. Root cause traced past the obvious guess (WSL distro systemd — already correctly enabled) to Docker Desktop's *separate* internal VM, which mounts a hybrid legacy-cgroup layout at boot regardless of any individual distro's systemd state. **Fix:** `%UserProfile%\.wslconfig` → `[wsl2] kernelCommandLine = cgroup_no_v1=all`, then `wsl --shutdown` from Windows. Verified: `docker info` now reports `Cgroup Version: 2` and `/sys/fs/cgroup/cgroup.controllers` is the unified hierarchy.

**2. Kubelet reservation sizing (resolved).** See Deviation 4 above — the approved fair-share numbers exceeded this host's real capacity outright.

Both fixes were applied and confirmed working in the same session; see Deviations and the frontmatter `key-decisions` for exact values. A memory note was recorded (outside this repo) documenting the user's two dev machines (12-CPU/32GiB laptop, 64GiB PC) so future work sizes config against real hardware rather than STACK.md's assumed 32-CPU/47-GiB box.

## User Setup Required

None remaining — both blockers above were resolved with the user's confirmation in this session. `%UserProfile%\.wslconfig` now reads:
```ini
[wsl2]
kernelCommandLine = cgroup_no_v1=all
memory = 24GB
```

## Known Stubs

None. Every file this plan commits is the real, intended implementation — nothing is a placeholder pending later wiring.

## Next Phase Readiness

**Fully verified, file-level and live.** `kind/cluster.yaml` parses correctly with exactly 3 nodes each carrying a `KubeletConfiguration` patch sized to this host's real capacity; both ingress-nginx values profiles render cleanly under `helm template`; `make install-cluster` installs `boto3`/`psycopg` under `--group cluster` and they import successfully; the full `pytest tests/policy -q` suite (58 tests) passes; the plan's Task 2 `<verify><automated>` block passes in full — `make cluster-down && make cluster-up && make cluster-up` (idempotence proven on the second `cluster-up`), all 5 namespaces `Active`, all 3 nodes `Ready`, ingress-nginx controller `Available`, and `tracer.localtest.me` returns the expected `404`.

Plans 02-02 through 02-08 can proceed — the cluster this phase builds on is confirmed reachable and correctly sized on the actual execution host.

---
*Phase: 02-kind-cluster-core-infrastructure*
*Completed: 2026-08-12*

## Self-Check: PASSED

- All 15 created files verified present on disk (`helm/versions.env`, `tools/k8s/install_{kind,helm,kubeconform}.sh`, `kind/cluster.yaml`, `kubernetes/namespaces.yaml`, `scripts/{helm-install,wait-for,cluster-up,cluster-down}.sh`, `scripts/stages/{10-registry,20-namespaces,30-ingress-nginx}.sh`, `helm/values/{local,ci}/ingress-nginx.yaml`)
- Both task commits verified present in `git log`: `420cc51`, `eecfed0`
