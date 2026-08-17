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

## Out of Scope (by design, not deferred as a gap)

No cluster-recreation, `wsl --shutdown`, or Docker Desktop restart was
run — these require a Windows-side action outside this WSL session and
are a manually-coordinated follow-up step for the user:

1. Run `wsl --shutdown` from Windows PowerShell/cmd (not from inside WSL).
2. Restart Docker Desktop.
3. Confirm the new memory ceiling took effect (`free -h` inside WSL should
   show ~28Gi total).
4. `kind delete cluster` + recreate (destroys current Postgres/MinIO/Vault
   data — already accepted by the user this session) to pick up the new
   `KubeletConfiguration` reservations (kubelet reservations are
   creation-time-only, per this file's own top-of-file comment).
5. Full stack redeploy: Helm charts, Vault bootstrap/unseal, migrations,
   image builds/pushes.

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
