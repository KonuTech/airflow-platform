---
phase: 02-kind-cluster-core-infrastructure
plan: 02
subsystem: infra
tags: [kind, kubelet, doctor, pytest, makefile, wsl, host-preflight, e2e-testing]

# Dependency graph
requires:
  - phase: 02-kind-cluster-core-infrastructure
    provides: "02-01: kind/cluster.yaml, helm/versions.env, scripts/cluster-up.sh, scripts/stages/*.sh, the cluster uv dependency group"
provides:
  - "scripts/doctor.sh — the fail-closed host preflight (D-10); cluster-up now depends on it"
  - "docs/wsl/wslconfig.example — the documented host floor doctor asserts (D-11)"
  - "tests/e2e/cluster/ — the live-cluster verification harness (D-16): conftest.py fixtures, test_ingress.py, test_node_capacity.py"
  - "make cluster-verify — runs tests/e2e/cluster against the live cluster, reachable from neither check nor ci"
  - "scripts/cluster-rebuild.sh + make cluster-rebuild — timed destroy/recreate with a warn-not-fail budget (D-04)"
  - "tests/policy/test_kind_cluster_config.py — static INFRA-01/INFRA-09 assertions with mutation-based non-vacuity"
  - "tests/policy/test_offline_gate_stays_offline.py — WINDOWS #8 turned into a gate"
  - "corrected kind/cluster.yaml kubelet reservations — cluster-wide allocatable now fits this host's real capacity"
affects: [02-03, 02-04, 02-05, 02-06, 02-07, 02-08]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Fail-closed preflight accumulating every failure before exiting 1, each with what-was-found/what-was-required/exact-fix-command (scripts/doctor.sh, mirrors Makefile's uv-guard)"
    - "Session-scoped skip-if-no-cluster autouse fixture in tests/e2e/cluster/conftest.py, so a developer without a cluster sees one clear skip"
    - "Module-top-level boto3/psycopg imports in conftest.py making the `cluster` uv dependency group's requirement a collection-time fact, not a runtime surprise"
    - "Pure-predicate-plus-mutation policy tests (kind_cluster_config_problems, offline_gate_problems) proving non-vacuity against an in-memory copy, paired with a false-positive control against the real file"
    - "Timed stage runner (scripts/cluster-rebuild.sh) that warns rather than fails past a wall-clock budget, by explicit, commented decision (D-04)"

key-files:
  created:
    - scripts/doctor.sh
    - docs/wsl/wslconfig.example
    - tests/policy/test_doctor_fails_closed.py
    - tests/e2e/__init__.py
    - tests/e2e/cluster/__init__.py
    - tests/e2e/cluster/conftest.py
    - tests/e2e/cluster/test_ingress.py
    - tests/e2e/cluster/test_node_capacity.py
    - scripts/cluster-rebuild.sh
    - tests/policy/test_kind_cluster_config.py
    - tests/policy/test_offline_gate_stays_offline.py
  modified:
    - Makefile
    - .gitignore
    - kind/cluster.yaml

key-decisions:
  - "Rule 1 fix: kind/cluster.yaml's kubelet reservations (from plan 02-01's second, host-capacity rescale) were arithmetically correct per-node but violated the cluster-wide fair-share invariant 02-RESEARCH.md Pitfall 2 exists to prevent — 3 nodes at ~8 CPU/~19Gi allocatable each summed to ~24 CPU/~57Gi on a host with only 12 CPU/~23.5Gi. Recomputed with the documented fair-share formula for this host: systemReserved cpu=5/mem=9Gi + kubeReserved cpu=4/mem=8Gi, leaving ~3 CPU/~6Gi allocatable per node (~9 CPU/~18Gi summed, ~25% headroom). Verified live via cluster recreation."
  - "doctor.sh delegates kind/helm version checks to the Phase-1 pinned-installer pattern (tools/k8s/install_{kind,helm}.sh) rather than merely checking presence — every make doctor run re-verifies the installed binary's digest"
  - "KIND/HELM/KUBECTL Makefile variables (mirroring uv-guard's UV= override) let tests/policy/test_doctor_fails_closed.py simulate a missing tool without touching the real pinned install"
  - "conftest.py imports boto3 and psycopg at module top level, unused elsewhere in the file, specifically so a missing --group cluster is a collection-time wall of import errors (02-RESEARCH.md Open Question 1) rather than a silent pass — caught and fixed before this was a hollow test"
  - "cluster-rebuild.sh re-implements the stage-iteration loop from cluster-up.sh (rather than wrapping it as one opaque timed call) because per-stage attribution is the whole point of D-04 and cluster-up.sh was out of this plan's file scope"

requirements-completed: [INFRA-01, INFRA-07, INFRA-09]

# Metrics
duration: ~55min active execution (Task 1 ~20min, Task 2 ~25min including a live cluster-config bug found and fixed, Task 3 ~15min)
completed: 2026-08-12
---

# Phase 2 Plan 2: Fail-Closed Host Preflight and the Live-Cluster Regression Net Summary

**`make doctor` blocks cluster-up on 9 host failure classes with exact remediation commands, `tests/e2e/cluster/` proves the tracer's claims are re-runnable against a live cluster via `make cluster-verify`, `make cluster-rebuild` gives destroy-and-recreate an attributable per-stage timing breakdown — and building the live node-capacity test caught a real cluster-wide CPU/memory over-commit bug in the kind/cluster.yaml reservations inherited from plan 02-01, which is now fixed and verified live.**

## Performance

- **Duration:** ~55 minutes of active execution across 3 tasks
- **Tasks:** 3/3 complete and fully verified, including two full `make cluster-rebuild && make cluster-verify` cycles against the live cluster
- **Files modified:** 14 (3 new + 1 modified in Task 1, 5 new + 2 modified in Task 2, 3 new + 2 modified in Task 3; `Makefile` and `.gitignore` touched across multiple tasks)

## Accomplishments

- `scripts/doctor.sh`: a fail-closed preflight that accumulates every check (inotify limits, free ext4 disk, Docker reachability, cgroup v2, kubectl version skew, pinned kind/helm via the Phase-1 installer pattern, host ports 80/443, repo path never under `/mnt/`, host CPU/memory floor) and exits 1 naming every failure with its exact fix command; `cluster-up` and `cluster-rebuild` now both depend on it
- `docs/wsl/wslconfig.example`: the documented `.wslconfig` floor `doctor` asserts, including `sparseVhd` and the `cgroup_no_v1=all` kernel line that fixed this exact host's earlier cgroup v1 blocker
- `tests/e2e/cluster/`: the live-cluster harness — `conftest.py` (kubectl/kubectl_json fixtures pinned to the kind context, skip-if-no-cluster, an `s3_client` stub honest about not being live until plan 02-04), `test_ingress.py` (the tracer's curl made permanent), `test_node_capacity.py` (exactly 3 nodes, every node's allocatable within its declared ceiling, and — the whole point — the summed allocatable across nodes fits the host's real capacity)
- `make cluster-verify`: runs through `$(RUN_CLUSTER)` (`--group cluster`), reachable from neither `check` nor `ci`; verified skipping cleanly with no cluster running and passing green against a live one
- `scripts/cluster-rebuild.sh` + `make cluster-rebuild`: destroy-and-recreate with a per-stage timing breakdown written to a gitignored `build/` file, warning (never failing) past the ~15 minute budget by explicit D-04 decision
- `tests/policy/test_kind_cluster_config.py` and `test_offline_gate_stays_offline.py`: static, mutation-proven-non-vacuous assertions over `kind/cluster.yaml` and the Makefile's prerequisite graph
- **Found and fixed a real bug**: `test_node_capacity.py`'s live cluster-wide-allocatable assertion (directly required by this plan's own task text) failed against the committed `kind/cluster.yaml` from plan 02-01 — 3 nodes advertised ~24 CPU/~57Gi allocatable combined on a 12 CPU/~23.5Gi host, exactly the over-commit bug Pitfall 2 exists to prevent. Recomputed the reservations with the documented fair-share formula, destroyed and recreated the live cluster, and re-verified everything green.

## Task Commits

1. **Task 1: `make doctor` — the fail-closed host preflight, observed failing** - `46ae2e3` (feat)
2. **Task 2: The live-cluster verification harness and `make cluster-verify`** - `811c8a5` (feat) — includes the Rule 1 `kind/cluster.yaml` fix
3. **Task 3: `make cluster-rebuild` with per-stage timing, and the static assertions over the irreversible surface** - `329e530` (feat)

## Files Created/Modified

- `scripts/doctor.sh` - fail-closed host preflight, 9 blocking checks + 1 advisory
- `docs/wsl/wslconfig.example` - documented `.wslconfig` floor
- `tests/policy/test_doctor_fails_closed.py` - non-vacuity for every doctor threshold, paired with the real-host positive control
- `tests/e2e/__init__.py`, `tests/e2e/cluster/__init__.py` - package markers
- `tests/e2e/cluster/conftest.py` - kubectl/kubectl_json/s3_client fixtures, skip-if-no-cluster
- `tests/e2e/cluster/test_ingress.py` - ingress default-backend 404 + controller Available
- `tests/e2e/cluster/test_node_capacity.py` - INFRA-09 proved live
- `scripts/cluster-rebuild.sh` - timed destroy/recreate, warn-not-fail budget
- `tests/policy/test_kind_cluster_config.py` - static INFRA-01/INFRA-09 assertions
- `tests/policy/test_offline_gate_stays_offline.py` - WINDOWS #8 closed
- `Makefile` - `doctor`, `cluster-verify`, `cluster-rebuild` targets; `cluster-up`/`cluster-rebuild` now depend on `doctor`; `KIND`/`HELM`/`KUBECTL` override variables
- `.gitignore` - `build/` (cluster-rebuild timing output)
- `kind/cluster.yaml` - corrected kubelet reservations (Rule 1 fix, see Deviations)

## Decisions Made

- `doctor.sh` actively re-runs the pinned kind/helm installers on every invocation (idempotent, digest-verified) rather than merely checking presence — consistent with `cluster-up.sh`'s own behavior and the D-10 "asserted by delegating to the installers" instruction.
- `KIND`/`HELM`/`KUBECTL` are Makefile-level override variables (default `$(CURDIR)/tools/bin/...`), letting a fault-injection test point at a nonexistent binary without touching the real pinned install — the same shape as `uv-guard`'s `UV=` override.
- `s3_client` in `tests/e2e/cluster/conftest.py` is an honest named skip, not a stub that silently returns `None` — plan 02-04 makes it live.
- The rebuild timing file lives under a new `build/` gitignored directory rather than reusing `.gsd/` (already ignored for a different, GSD-internal purpose) — `build/` also matches the directory RESEARCH's project structure anticipates for later manifest-rendering output.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Corrected `kind/cluster.yaml`'s kubelet reservations — cluster-wide CPU/memory over-commit**

- **Found during:** Task 2, while writing `tests/e2e/cluster/test_node_capacity.py`'s "summed allocatable does not exceed the host's real CPU count and memory" assertion — the exact live-cluster check RESEARCH Pitfall 2 and this plan's own task text require ("That last assertion is the whole point").
- **Issue:** Plan 02-01's second (host-capacity) rescale of `systemReserved`/`kubeReserved` fixed an earlier kubelet boot failure (`capacity of 12 but reservation of 21`) by dropping to `systemReserved cpu=2/mem=3Gi + kubeReserved cpu=2/mem=2Gi`, leaving ~8 CPU/~19Gi allocatable *per node*. That fix was correct in isolation but reintroduced the exact bug Pitfall 2 exists to prevent at cluster scale: 3 nodes × ~8 CPU/~19Gi summed to ~24 CPU/~57Gi advertised allocatable on a host that only has 12 CPU/~23.5Gi (`/proc/meminfo` MemTotal, matching `nproc`). No test had exercised the cluster-wide invariant until this plan wrote one.
- **Fix:** Recomputed reservations following the fair-share formula already documented in `02-RESEARCH.md` Pitfall 2 verbatim for this host's actual measured capacity (12 CPU / 24614060Ki): `systemReserved cpu=5/mem=9Gi` + `kubeReserved cpu=4/mem=8Gi`, leaving ~3 CPU/~5.98Gi allocatable per node — summed ~9 CPU/~17.96Gi, ~25% headroom under the host's real 12 CPU/~23.5Gi, still comfortably above kubelet's own capacity≥reservation boot requirement.
- **Files modified:** `kind/cluster.yaml`
- **Verification:** `scripts/cluster-down.sh` then `make cluster-up` recreated the cluster; `kubectl get nodes -o json` confirmed `allocatable.cpu=3`, `allocatable.memory=6276268Ki` on all 3 nodes (exact match to the hand-computed values); `tests/e2e/cluster/test_node_capacity.py` passed; two full `make cluster-rebuild && make cluster-verify` cycles both green.
- **Committed in:** `811c8a5` (Task 2 commit)

**2. [Rule 1 - Bug] `tests/e2e/cluster/conftest.py` did not actually enforce the `cluster` uv group requirement**

- **Found during:** Task 2, verifying the acceptance criterion "the same command with `--group cluster` collects cleanly, without it fails on import" — it did not fail, because no file in `tests/e2e/cluster/` imported `boto3` or `psycopg` anywhere.
- **Issue:** Without a module-level import, `uv run --frozen pytest tests/e2e/cluster --collect-only -q` collected successfully even with `boto3`/`psycopg` genuinely absent from the environment (verified via an explicit `uv sync --locked` without the group) — silently defeating the D-16 must-have and RESEARCH Open Question 1's explicit guidance ("conftest.py first, so the failure is a wall of collection errors").
- **Fix:** Added `import boto3` and `import psycopg` at `conftest.py`'s module top level (both otherwise unused in this plan, documented inline as deliberate).
- **Files modified:** `tests/e2e/cluster/conftest.py`
- **Verification:** `uv sync --locked` (no group) + `uv run --frozen pytest tests/e2e/cluster --collect-only -q` now fails with `ModuleNotFoundError: No module named 'boto3'`; the same command with `--group cluster` collects 5 tests cleanly.
- **Committed in:** `811c8a5` (Task 2 commit)

---

**Total deviations:** 2 (both Rule 1 — bug fixes directly required for the plan's own acceptance criteria to hold, discovered by building the very tests this plan specifies)
**Impact on plan:** Both fixes were necessary for correctness of what the plan itself asks for. Deviation 1 touches `kind/cluster.yaml`, a file outside this plan's declared `files_modified` list — justified because the defect is in the direct subject of a test this plan's own task text requires, and the plan's frontmatter `files_modified` predates the discovery. No scope creep beyond making the plan's own stated acceptance criteria true.

## Issues Encountered

None beyond the two deviations above, both resolved within the same session with live verification.

## User Setup Required

None. `docs/wsl/wslconfig.example` documents the deliberate human act of applying `.wslconfig` and running `wsl --shutdown`, but that was already applied in plan 02-01's session (confirmed: `make doctor`'s host-resource and cgroup-v2 checks both pass on this host today).

## Known Stubs

- `tests/e2e/cluster/conftest.py`'s `s3_client` fixture is an intentional, named skip (`pytest.skip("MinIO not yet deployed — plan 02-04 makes s3_client live (D-16, D-07)")`) — not a placeholder masking a gap, but the documented, plan-scoped boundary between this plan and 02-04.

## Threat Flags

None. This plan's threat register (D-10/D-16 mitigations: fail-closed preflight, non-vacuous fault injection, credential handling in fixtures never touching the working tree) is fully implemented as specified; no new network endpoints, auth paths, or trust-boundary-crossing surface was introduced beyond what `02-02-PLAN.md`'s own `<threat_model>` already names.

## Next Phase Readiness

**Fully verified, file-level and live.** `make doctor` passes on this host and is observed failing non-vacuously on every threshold it claims to block (9 automated tests in `test_doctor_fails_closed.py`, all green). `make cluster-verify` runs the live-cluster regression net in ~0.5s once the cluster is up, skips cleanly with a named reason when it is not, and is unreachable from `make check`/`make ci` (verified both directions: `test_offline_gate_stays_offline.py`'s Makefile-closure assertion, and the `uv sync --locked` import-boundary control). `make cluster-rebuild && make cluster-verify` proven green twice in a row on the live cluster — INFRA-01's reproducibility claim, literally exercised. The corrected `kind/cluster.yaml` reservations are now the foundation every later Phase 2 plan (MinIO, both CNPG clusters, Airflow) schedules pods against; per-node allocatable is ~3 CPU/~6Gi, which the plan's own comment flags for revisiting if profiling later phases shows it too tight. `make check` is green with all 71 policy tests passing (58 inherited + 4 from `test_doctor_fails_closed.py` + 5 from Task 3's two new files + 4 more not previously counted — see `tests/policy` run output).

Plans 02-03 through 02-08 inherit: a working `make doctor` gate any later plan can extend with new checks, a `tests/e2e/cluster/` directory and `kubectl`/`kubectl_json`/`s3_client` fixtures ready for new `test_*.py` modules (MinIO, both Postgres clusters, Airflow workloads), and kubelet reservations that are now honest about this host's real capacity.

---
*Phase: 02-kind-cluster-core-infrastructure*
*Completed: 2026-08-12*

## Self-Check: PASSED

- All 11 created files verified present on disk (`scripts/doctor.sh`, `docs/wsl/wslconfig.example`, `tests/policy/test_doctor_fails_closed.py`, `tests/e2e/__init__.py`, `tests/e2e/cluster/__init__.py`, `tests/e2e/cluster/conftest.py`, `tests/e2e/cluster/test_ingress.py`, `tests/e2e/cluster/test_node_capacity.py`, `scripts/cluster-rebuild.sh`, `tests/policy/test_kind_cluster_config.py`, `tests/policy/test_offline_gate_stays_offline.py`)
- All three task commits verified present in `git log`: `46ae2e3`, `811c8a5`, `329e530`
