---
phase: quick-260817-raise-kind-cluster-memory-budget-28gb
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - /mnt/c/Users/admin/.wslconfig
  - kind/cluster.yaml
autonomous: true
requirements:
  - "STATE.md blocker (2026-08-17, quick task 260817-mvp, .planning/quick/260817-mvp-cap-concurrency-on-csv-ingest-customers-/260817-mvp-SUMMARY.md): live CPU/memory starvation confirmed this session; the structural node-CPU-budget question (kind/cluster.yaml node allocatable) was explicitly left open/deferred pending a raised host memory ceiling — this plan raises that ceiling and recomputes the documented fair-share reservations at the new value."
  - "kind/cluster.yaml's own top-of-file comment ('[Rule 1 fix, plan 02-02]'): 'Revisit if profiling later phases ... shows 3 CPU/6Gi per node too tight — the fix then is raising the host floor ... not quietly reintroducing this over-commit bug.' This plan is that revisit."

must_haves:
  truths:
    - "/mnt/c/Users/admin/.wslconfig's memory line reads exactly 'memory = 28GB'; the kernelCommandLine line is byte-identical to before"
    - "All three KubeletConfiguration blocks in kind/cluster.yaml (control-plane, worker, worker2) set systemReserved.memory=\"11Gi\" and kubeReserved.memory=\"9.5Gi\""
    - "CPU reservation values (systemReserved.cpu=\"5\", kubeReserved.cpu=\"4\") are unchanged in all three blocks"
    - "kind/cluster.yaml carries a new dated 2026-08-17 comment block, in the file's own established style, documenting the fair-share recomputation arithmetic at the new M=28Gi ceiling, citing the STATE.md blocker and quick task 260817-mvp, noting the .wslconfig change requires a manual wsl --shutdown + Docker Desktop restart to take effect, and explicitly flagging that CPU allocatable (3 CPU/node, 9 CPU cluster-wide) did NOT change and remains a possible future ceiling"
    - "kind/cluster.yaml still parses as valid top-level YAML after the edits"
    - "No out-of-scope fields (maxPods, evictionHard, extraPortMappings, extraMounts, containerdConfigPatches, node images, node-labels) were touched, and no cluster-recreation or WSL-shutdown commands were run"
  artifacts:
    - path: "/mnt/c/Users/admin/.wslconfig"
      provides: "Raised WSL2 memory cap from 24GB to 28GB, the precondition the recomputed kubelet reservations depend on"
      contains: "memory = 28GB"
    - path: "kind/cluster.yaml"
      provides: "Recomputed fair-share KubeletConfiguration memory reservations (systemReserved/kubeReserved) at the new 28Gi host ceiling, applied identically to all three nodes, plus a dated decision comment"
      contains: "memory: \"11Gi\""
  key_links:
    - from: "kind/cluster.yaml's new 2026-08-17 comment block"
      to: "kind/cluster.yaml's existing '[Rule 1 fix, plan 02-02]' comment block (same formula: allocatable_target = (C/N) - headroom; reserved_total = capacity - allocatable_target)"
      via: "identical fair-share arithmetic reapplied at M=28Gi instead of the prior ~23.5Gi, producing systemReserved.memory=11Gi + kubeReserved.memory=9.5Gi (sum 20.5Gi) instead of 9Gi+8Gi (sum 17Gi)"
      pattern: "systemReserved.*memory.*11Gi"
---

<objective>
Raise the host-side WSL2 memory ceiling from 24GB to 28GB (`.wslconfig`) and
re-derive `kind/cluster.yaml`'s three `KubeletConfiguration` memory
reservations at that new ceiling, using the exact fair-share formula the file
already documents and instructs future maintainers to reuse. CPU inputs are
unchanged (host is still 12 cores), so only the memory reservation numbers
move.

Purpose: live CPU/memory starvation was confirmed this session (STATE.md
Blockers/Concerns, 2026-08-17 entry; quick task `260817-mvp`) and traced back
to `kind/cluster.yaml`'s node memory/CPU budget, which that same file's own
comments already flagged as the deferred fix — "the fix then is raising the
host floor ... not quietly reintroducing this over-commit bug." This plan
performs that documented, pre-authorized revisit. It does NOT recreate the
cluster or restart WSL — those are separate, manually-coordinated follow-up
steps outside this plan's scope.

Output: `.wslconfig`'s `memory` line raised to `28GB`; `kind/cluster.yaml`'s
three `KubeletConfiguration` blocks updated to
`systemReserved.memory: "11Gi"` / `kubeReserved.memory: "9.5Gi"` (CPU
untouched); a new dated comment block in `kind/cluster.yaml` recording the
arithmetic and the residual CPU-ceiling caveat.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/STATE.md
@kind/cluster.yaml
@.planning/quick/260817-mvp-cap-concurrency-on-csv-ingest-customers-/260817-mvp-SUMMARY.md

<formula_already_documented_in_kind_cluster_yaml>
The file's own `[Rule 1 fix, plan 02-02]` comment block (lines ~25-45) is the
formula to reuse verbatim, just with new inputs:

  allocatable_target = (C/N) - headroom
  reserved_total      = capacity - allocatable_target

Old inputs: C=12 CPU, host memory capacity ~23.5Gi, N=3, headroom_cpu=1,
headroom_mem=~1.8Gi -> allocatable_target_mem ~6Gi/node -> reserved_total_mem
= 23.5Gi - 6Gi = 17Gi, split ~9:8 (systemReserved:kubeReserved) -> 9Gi/8Gi.

New inputs (this plan): M=28Gi (matching the new `.wslconfig` ceiling used
directly as the capacity input, per the task's explicit instruction),
C=12 CPU (unchanged), N=3, headroom_cpu=1, headroom_mem=1.8Gi:

  allocatable_target_mem = (28Gi/3) - 1.8Gi = 9.33Gi - 1.8Gi ~= 7.5Gi/node
  reserved_total_mem     = 28Gi - 7.5Gi = 20.5Gi
  split ~9:8 (same proportion as the existing 9Gi:8Gi split)
    -> systemReserved.memory = 20.5Gi * 9/17 ~= 11Gi
    -> kubeReserved.memory   = 20.5Gi * 8/17 ~= 9.5Gi

  allocatable_target_cpu = (12/3) - 1 = 3/node   (UNCHANGED — C did not move)
  reserved_total_cpu     = 12 - 3 = 9  -> systemReserved.cpu=5, kubeReserved.cpu=4 (UNCHANGED)

Resulting per-node allocatable: memory ~7.5Gi (was ~6Gi), CPU still ~3.
Cluster-wide CPU ceiling is unchanged: 3 CPU/node x 3 nodes = 9 CPU total.
</formula_already_documented_in_kind_cluster_yaml>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Raise the WSL2 memory cap in .wslconfig</name>
  <files>/mnt/c/Users/admin/.wslconfig</files>
  <action>
    Edit the single `memory = 24GB` line in `/mnt/c/Users/admin/.wslconfig`
    (reached via the `/mnt/c` passthrough — this is a one-time Windows-side
    config file edit outside the git repo, not a live DAG mount, so it is
    exempt from CLAUDE.md's general `/mnt/c` mounting guidance) to
    `memory = 28GB`. Do not touch the `[wsl2]` header line or the
    `kernelCommandLine = cgroup_no_v1=all` line — both must remain byte-for-
    byte identical. Do not add, remove, or reorder any other lines. Do not
    run `wsl --shutdown` or restart Docker Desktop — this change requires
    that manual, Windows-side step to take effect, and it happens outside
    this plan.
  </action>
  <verify>
    <automated>grep -qx "memory = 28GB" "/mnt/c/Users/admin/.wslconfig" && grep -qx "kernelCommandLine = cgroup_no_v1=all" "/mnt/c/Users/admin/.wslconfig" && ! grep -q "memory = 24GB" "/mnt/c/Users/admin/.wslconfig" && echo OK</automated>
  </verify>
  <done>`/mnt/c/Users/admin/.wslconfig` contains exactly `memory = 28GB`, the `kernelCommandLine` line is untouched, and no `memory = 24GB` line remains.</done>
</task>

<task type="auto">
  <name>Task 2: Recompute kind/cluster.yaml's kubelet memory reservations at the new 28Gi ceiling</name>
  <files>kind/cluster.yaml</files>
  <action>
    In all THREE `KubeletConfiguration` `kubeadmConfigPatches` blocks
    (control-plane, worker, worker2 — currently identical), change
    `systemReserved.memory: "9Gi"` to `"11Gi"` and `kubeReserved.memory:
    "8Gi"` to `"9.5Gi"`. Leave `systemReserved.cpu: "5"` and
    `kubeReserved.cpu: "4"` untouched in all three blocks — the host's
    12-core ceiling did not change. Do not touch `evictionHard`, `maxPods`,
    `extraPortMappings`, `extraMounts`, `containerdConfigPatches`, node
    images, or `node-labels` anywhere in the file.

    Add ONE new dated comment block near the top of the file, immediately
    after the existing `[Rule 1 fix, plan 02-02]` comment block and before
    the `kind: Cluster` document start, following this file's own
    established convention (dated, cites the triggering evidence, shows the
    arithmetic, states the resulting values, flags what did NOT change).
    Cover, in prose matching the file's existing tone:
      - Date 2026-08-17.
      - Live CPU/memory starvation was confirmed this session — cite
        STATE.md's Blockers/Concerns 2026-08-17 entry and quick task
        `260817-mvp` (`.planning/quick/260817-mvp-cap-concurrency-on-csv-ingest-customers-/260817-mvp-SUMMARY.md`),
        which capped `integrity_gate` concurrency as a demand-side fix and
        explicitly deferred the supply-side fix (raising the host memory
        floor) to this file.
      - The host's WSL2 memory cap was raised from 24GB to 28GB via
        `.wslconfig` — note that this requires `wsl --shutdown` (run from
        Windows, not from inside WSL) plus a Docker Desktop restart to take
        effect, and that nothing in this repo or its tooling applies that
        automatically; the cluster must be recreated separately once the
        host-level restart has happened, as a manually-coordinated follow-up
        step outside this file's own scope.
      - The SAME fair-share formula this file already documents
        (`allocatable_target = (C/N) - headroom`, `reserved_total =
        capacity - allocatable_target`) was reapplied at the new M=28Gi:
        `(28Gi/3) - 1.8Gi ~= 7.5Gi/node` allocatable target, `reserved_total
        = 28Gi - 7.5Gi = 20.5Gi`, split ~9:8 (matching the existing
        9Gi:8Gi proportion) into `systemReserved.memory=11Gi` +
        `kubeReserved.memory=9.5Gi`. New memory allocatable per node is
        ~7.5Gi (was ~6.3Gi).
      - CPU stays fixed at `systemReserved.cpu=5` / `kubeReserved.cpu=4`
        (3 CPU/node allocatable) since the host's 12-core ceiling did not
        move — `(12/3) - 1 = 3`, unchanged from before.
      - Explicitly flag this as a memory-only improvement: CPU-bound
        scheduling contention can still recur under heavy concurrent load,
        since 3 CPU/node x 3 nodes = 9 CPU cluster-wide remains the real
        ceiling and did not move. A future CPU-ceiling revisit would need
        either more host cores or further demand-side capping (as
        `260817-mvp` already did for `integrity_gate`).
  </action>
  <verify>
    <automated>cd /home/konutec/projects/airflow-platform && python3 -c "import yaml; yaml.safe_load(open('kind/cluster.yaml'))" && test "$(grep -c 'memory: \"11Gi\"' kind/cluster.yaml)" -eq 3 && test "$(grep -c 'memory: \"9.5Gi\"' kind/cluster.yaml)" -eq 3 && test "$(grep -c 'memory: \"9Gi\"' kind/cluster.yaml)" -eq 0 && test "$(grep -c 'memory: \"8Gi\"' kind/cluster.yaml)" -eq 0 && test "$(grep -c 'cpu: \"5\"' kind/cluster.yaml)" -eq 3 && test "$(grep -c 'cpu: \"4\"' kind/cluster.yaml)" -eq 3 && echo OK</automated>
  </verify>
  <done>`kind/cluster.yaml` parses as valid top-level YAML; all three `KubeletConfiguration` blocks show `systemReserved.memory="11Gi"` and `kubeReserved.memory="9.5Gi"` with `cpu` values unchanged at `"5"`/`"4"`; a new dated 2026-08-17 comment block documents the recomputation, cites the triggering STATE.md/quick-task evidence, and flags the unmoved CPU ceiling.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|--------------|
| Host WSL2 config (`.wslconfig`) -> kind node capacity | Any process with write access to the Windows filesystem under `/mnt/c/Users/admin/` can change the memory ceiling every kind node reports as `status.capacity`; this plan is a deliberate, human-directed instance of that same edit path. |
| `kind/cluster.yaml` -> kubelet boot-time admission | `KubeletConfiguration`'s `systemReserved`/`kubeReserved` values are only validated at `kind create cluster` time (`capacity >= reservation`, per the file's own `[Rule 1 fix, plan 02-02]` comment); an arithmetic mistake here would only surface as a boot failure or a silent over/under-reservation on the NEXT cluster recreation, not immediately. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|------------------|
| T-quick-01 | Tampering | `.wslconfig` (host-side, outside repo/git history) | accept | Single-user local dev machine; the file is plain text with no secrets, editable only by someone with existing Windows filesystem access to the user's own profile directory. No git history, no CI exposure. |
| T-quick-02 | Denial of Service (arithmetic error) | `kind/cluster.yaml` `KubeletConfiguration` memory values | mitigate | Task 2's `<verify>` step greps for the exact expected values (3x `"11Gi"`, 3x `"9.5Gi"`, 0x stale `"9Gi"`/`"8Gi"`) across all three nodes before the plan is considered done, and confirms `cpu` values are unchanged, catching a transcription mistake before it ever reaches a live `kind create cluster` boot attempt. |
| T-quick-03 | Denial of Service (accepted, unresolved) | Cluster-wide CPU ceiling (9 CPU total, unchanged) | accept | Explicitly out of scope for this plan (host has only 12 physical cores; C did not change). The new dated comment block records this residual risk so it is not silently forgotten — same demand-side mitigation (`max_active_tis_per_dag` capping) as `260817-mvp` remains the available lever if CPU contention recurs. |
</threat_model>

<verification>
1. `grep -qx "memory = 28GB" "/mnt/c/Users/admin/.wslconfig"` succeeds and no `memory = 24GB` remains.
2. `python3 -c "import yaml; yaml.safe_load(open('kind/cluster.yaml'))"` succeeds (outer document still parses).
3. `grep -c 'memory: "11Gi"' kind/cluster.yaml` and `grep -c 'memory: "9.5Gi"' kind/cluster.yaml` both report exactly 3; `grep -c 'memory: "9Gi"' kind/cluster.yaml` and `grep -c 'memory: "8Gi"' kind/cluster.yaml` both report 0.
4. `grep -c 'cpu: "5"' kind/cluster.yaml` and `grep -c 'cpu: "4"' kind/cluster.yaml` both still report exactly 3 (unchanged).
5. `git diff kind/cluster.yaml` shows only the two memory-value substitutions (x3 each) plus the one new comment block — no other lines changed.
</verification>

<success_criteria>
- `.wslconfig`'s WSL2 memory ceiling is raised from 24GB to 28GB, with nothing else in the file changed.
- `kind/cluster.yaml`'s three `KubeletConfiguration` blocks carry the recomputed fair-share memory reservations (`systemReserved.memory="11Gi"`, `kubeReserved.memory="9.5Gi"`) using the file's own documented formula, with CPU values left untouched.
- A new dated comment block in `kind/cluster.yaml` documents the arithmetic, cites the triggering evidence (STATE.md blocker + quick task `260817-mvp`), notes the manual `wsl --shutdown` + Docker Desktop restart precondition, and flags that the CPU ceiling is unchanged and unresolved.
- `kind/cluster.yaml` still parses as valid YAML.
- No cluster-recreation, `wsl --shutdown`, or other destructive/manual-coordination commands were run as part of this plan.
</success_criteria>

<output>
Create `.planning/quick/260817-oqy-raise-kind-cluster-memory-budget-28gb-ws/260817-oqy-SUMMARY.md` when done.
</output>
