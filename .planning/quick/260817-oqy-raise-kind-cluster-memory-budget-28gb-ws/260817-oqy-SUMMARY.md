---
phase: quick-260817-raise-kind-cluster-memory-budget-28gb
plan: 01
subsystem: infra-kind-cluster
tags: [kind, kubelet, resource-budget, wsl2, capacity-planning]
dependency-graph:
  requires:
    - "260817-mvp: integrity_gate concurrency cap (demand-side fix)"
  provides:
    - "kind cluster memory allocatable raised from ~6.3Gi to ~7.5Gi/node (supply-side fix)"
  affects:
    - "kind/cluster.yaml"
    - "/mnt/c/Users/admin/.wslconfig (host-side, outside repo)"
tech-stack:
  added: []
  patterns:
    - "Fair-share kubelet reservation formula (allocatable_target = (C/N) - headroom; reserved_total = capacity - allocatable_target), reapplied verbatim at a new host memory ceiling per kind/cluster.yaml's own documented convention"
key-files:
  created: []
  modified:
    - kind/cluster.yaml
    - /mnt/c/Users/admin/.wslconfig (non-git, host-side)
decisions:
  - "CPU reservation values left untouched (systemReserved.cpu=5, kubeReserved.cpu=4, 3 CPU/node allocatable) -- the host's 12-core ceiling is physical and did not move; only memory could be raised via the WSL2 cap"
metrics:
  duration: "~10min"
  completed: 2026-08-17
---

# Quick Task 260817-oqy: Raise kind cluster memory budget Summary

Raised the WSL2 host memory ceiling from 24GB to 28GB and recomputed
`kind/cluster.yaml`'s three `KubeletConfiguration` memory reservations at
the new ceiling, using the exact fair-share formula the file already
documents. This is the "raise the host floor" fix that file's own
comments explicitly prescribed once live CPU/memory starvation was
confirmed this session (STATE.md blocker, quick task `260817-mvp`).

## What Was Built

**Task 1 (complete):** `/mnt/c/Users/admin/.wslconfig`'s `memory = 24GB`
line changed to `memory = 28GB`. `kernelCommandLine = cgroup_no_v1=all`
left untouched. Verified: `grep -qx "memory = 28GB"` succeeds, no
`memory = 24GB` remains, `kernelCommandLine` line unchanged.

**Task 2 (complete, commit `811438b`):** All three `KubeletConfiguration`
blocks (control-plane, worker, worker2) in `kind/cluster.yaml` updated:
`systemReserved.memory` `9Gi` → `11Gi`, `kubeReserved.memory` `8Gi` →
`9.5Gi`. CPU values (`systemReserved.cpu=5`, `kubeReserved.cpu=4`) left
unchanged in all three blocks — the host's 12-core ceiling is physical
and this change cannot move it. A new dated 2026-08-17 comment block was
added documenting the recomputation arithmetic, citing the triggering
STATE.md blocker and quick task `260817-mvp`, noting the manual
`wsl --shutdown` + Docker Desktop restart precondition, and explicitly
flagging that the CPU ceiling (3 CPU/node × 3 nodes = 9 CPU cluster-wide)
did not move and remains a possible future bottleneck.

Verified: `kind/cluster.yaml` parses as valid YAML; exactly 3×
`memory: "11Gi"`, 3× `memory: "9.5Gi"`, 0× stale `memory: "9Gi"`/`"8Gi"`;
`cpu: "5"`/`cpu: "4"` unchanged at 3× each.

New per-node memory allocatable: ~7.5Gi (was ~6.3Gi), a ~19% increase.
CPU allocatable stays at 3 CPU/node — unchanged.

## Deviations from Plan

None. Worktree base was corrected once at start via
`git reset --hard 620edfa005b34dc1a3866a679e3b365aa6f25ed6` (a known
#2015-class worktree-branch-drift issue, not a plan deviation).

## Post-Session Follow-Up: Activation (recorded here for continuity)

The Windows-side steps out of this plan's scope were completed in a
follow-up conversation, with an outcome different from what was planned:

1. User ran a **full laptop restart** (not just `wsl --shutdown`), which
   cleanly applied the new `.wslconfig` cap — `docker info` confirmed
   `Total Memory: 27.41GiB` (up from 23.47GiB).
2. Docker Desktop **restarted the existing kind node containers** rather
   than the cluster being recreated. Kubelet re-read the new, bigger
   cgroup capacity live, but its `KubeletConfiguration` reservation
   values were still the OLD baked-in ones (`systemReserved.memory=9Gi`,
   `kubeReserved.memory=8Gi`) — this file's `11Gi`/`9.5Gi` recomputation
   requires a genuine `kind create cluster` to take effect, not a
   container restart.
3. Net effect: allocatable memory/node rose from ~6.3Gi to **~9.92Gi**
   (`kubectl get nodes` confirmed `mem=10405032Ki` on all 3 nodes) — MORE
   than this plan's own deliberately-conservative ~7.5Gi target, because
   the old (smaller) absolute reservation is now applied against the new
   (bigger) capacity.
4. Given that, and given CPU (not memory) was the session's dominant
   contention factor and is completely unaffected either way, the user
   chose to **skip the destructive `kind delete cluster` recreation**
   rather than trade the current ~9.92Gi/node for the deliberately-lower
   ~7.5Gi/node this plan's committed values would produce. See
   `.planning/STATE.md`'s Blockers/Concerns for the full record — the
   committed `kind/cluster.yaml` values (`11Gi`/`9.5Gi`) are correct and
   intentional, they just aren't live yet, and won't be until the next
   genuine cluster recreation (at which point allocatable memory/node
   will *drop* to ~7.5Gi from the current accidental ~9.92Gi — expected,
   not a regression).
5. Post-restart cleanup performed: `make vault-unseal` (expected per D-02,
   no auto-unseal), 15 zombie `Unknown`/`Error` pods force-deleted, DAGs
   hostPath mount confirmed intact on all 3 nodes (no recurrence of the
   `dagrun-scheduler-stall` class of issue this time).

## Verification Status

| Plan verification item | Status |
|---|---|
| `.wslconfig` memory line = `28GB`, `kernelCommandLine` untouched | PASS |
| `kind/cluster.yaml` parses as valid YAML | PASS |
| 3× `memory: "11Gi"`, 3× `memory: "9.5Gi"`, 0× stale `9Gi`/`8Gi` | PASS |
| `cpu: "5"`/`cpu: "4"` unchanged at 3× each | PASS |
| New dated comment block present, cites triggering evidence | PASS |
| No cluster-recreation/`wsl --shutdown` commands run | PASS (correctly out of scope) |

## Self-Check: PASSED

- FOUND: kind/cluster.yaml
- FOUND: /mnt/c/Users/admin/.wslconfig (non-git, verified directly on disk)
- FOUND commit: 811438b (kind/cluster.yaml)
