---
phase: 11-ci-cd-completion-operations
plan: 04
subsystem: cicd
tags: [github-actions, kind, ephemeral-cluster, kyverno, vault, smoke-test, ci-cd]

# Dependency graph
requires:
  - phase: 11-02
    provides: 3-image matrix publish.yml with pr-<number> tag parity — the GHCR image reference shape this plan's cluster-up override mechanism points at
  - phase: 11-03
    provides: Kyverno admission controller + tests/e2e/cluster/test_kyverno_admission.py — one of smoke-verify's own 4 D-20 checks
  - phase: 11-06
    provides: rollback Makefile target's owner-resolution snippet, reused verbatim in scripts/ci-set-workload-images.sh
provides:
  - "helm_install (scripts/helm-install.sh) extra-args passthrough, backward-compatible with every existing 5/6-arg call site"
  - "scripts/stages/70-airflow.sh AIRFLOW_IMAGE_OVERRIDE_REPO/TAG branch — installs Airflow pointed at any GHCR repo/tag in one pass"
  - "scripts/ci-set-workload-images.sh <tag> — points csv_processor_image/dbt_image at ghcr.io/<owner>/{csv-processor,dbt}:<tag>"
  - "make smoke-verify — the D-20 4-point fast PR-gating subset, live-verified against the local cluster"
  - ".github/workflows/e2e-smoke.yml — PR-triggered ephemeral-kind workflow, live-verified up through kind create cluster / kubeadm init on a real GitHub Actions runner"
affects: [11-05]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "GitHub Actions workflow gotcha (live-discovered, not documented anywhere in this repo before): referencing secrets.* directly inside a step's own if: conditional makes the workflow parser reject the WHOLE FILE at parse time (zero jobs, generic 'workflow file issue' failure, never registers for its own trigger) — mirror the secret into a job-level env: var first, then gate on env.* instead; secrets.* stays fine inside with: blocks"
    - "scripts/doctor.sh's DOCTOR_MIN_CPUS/DOCTOR_MIN_MEM_GB are a sanctioned CI override point (documented in the script's own header) — set them as step-level env: vars in a CI workflow rather than editing the script's local-dev defaults"

key-files:
  created:
    - scripts/ci-set-workload-images.sh
    - .github/workflows/e2e-smoke.yml
  modified:
    - scripts/helm-install.sh
    - scripts/stages/70-airflow.sh
    - Makefile
    - tests/policy/test_offline_gate_stays_offline.py
    - tests/policy/test_workflow_secrets.py

key-decisions:
  - "make smoke-verify's D-20 check 1 (core Helm releases/Deployments/StatefulSets Ready) is a new inline shell assertion in the Makefile, not a new pytest file — no broad 'everything healthy' test already existed in tests/e2e/cluster/ (each existing test is scoped to one component), and this matches the Makefile's own established inline-shell idiom (rollback/migrate-analytics) rather than adding pytest collection overhead for two kubectl/helm queries"
  - "D-20 check 2 (one real DAG run reaching SUCCEEDED) uses the airflow dags trigger/dags state CLI directly, never a DB port-forward — deliberately NOT tests/e2e/slice/test_smoke_and_idempotency.py::test_smoke_dag_xcom_contains_built_sha, which also asserts the XCom git_sha matches this checkout's own --short HEAD; in e2e-smoke.yml's own ephemeral-kind context the pod runs the PR's GHCR image (built with the FULL git SHA), which can never equal a local --short HEAD value"
  - "helm_install's extra-args passthrough activates only when more than 6 positional args are given (never when exactly 5 or 6), so every existing call site is unaffected by construction, not just by convention"
  - "Live-verified GitHub Actions gotcha: secrets.* in a step's if: breaks the parser outright; fixed via the job-level env: mirror pattern (env.* in if:, secrets.* still fine in with:)"
  - "Live-verified: scripts/doctor.sh's local-WSL2 CPU/mem floors (8 CPU/20GiB) needed an explicit CI override (DOCTOR_MIN_CPUS=4/DOCTOR_MIN_MEM_GB=14) to match this project's own already-documented CI runner sizing (CLAUDE.md: 4 CPU/16GB) — set as env: vars in e2e-smoke.yml's own cluster-up step, using doctor.sh's own sanctioned override mechanism, not by editing doctor.sh"

requirements-completed: []

# Metrics
duration: ~50min
completed: 2026-08-23
---

# Phase 11 Plan 04: PR-Gating Fast Smoke Subset (Ephemeral Kind in CI) Summary

**Image-override mechanism for CI's cluster-up (`helm_install` extra-args passthrough + `AIRFLOW_IMAGE_OVERRIDE_*` + `ci-set-workload-images.sh`), `make smoke-verify`'s D-20 4-point fast subset, and `.github/workflows/e2e-smoke.yml` — all four live-verified against the real cluster and a real throwaway PR (#9), which also found and fixed two genuine GitHub Actions bugs, but surfaced a pre-existing, deep infrastructure gap (`kind/cluster.yaml` was never actually built to be CI-portable) that blocks the workflow's own full green run.**

## Performance

- **Duration:** ~50 min
- **Started:** 2026-08-23T18:30Z
- **Completed:** 2026-08-23T19:16Z
- **Tasks:** 3/3 attempted; Task 1 and Task 2 fully complete and live-verified; Task 3 partially complete (see below)
- **Files modified:** 8 (2 created, 6 modified)

## Accomplishments

- `scripts/helm-install.sh`'s `helm_install` now forwards any arguments beyond the 6 named positional ones as literal extra `helm upgrade --install` flags — backward-compatible by construction (only activates when `$# > 6`), verified against every existing call site in `scripts/stages/*.sh` (all pass exactly 5 or 6 args).
- `scripts/stages/70-airflow.sh` installs Airflow pointed at `AIRFLOW_IMAGE_OVERRIDE_REPO`/`AIRFLOW_IMAGE_OVERRIDE_TAG` when both are set, unchanged otherwise — live-verified via `scripts/ci-set-workload-images.sh pr-999` against the live cluster (confirmed via `airflow variables get csv_processor_image`), then restored to local dev images via `make image-csv-processor`/`make image-dbt`.
- `make smoke-verify` composes D-20's 4 checks as one target: (1) new inline shell assertion for Helm-releases-deployed + Deployments/StatefulSets-Ready, (2) suite-local `airflow dags trigger`/`dags state` CLI trigger+poll for `smoke_kubernetes_pod`, (3) `pytest tests/e2e/vault -q -m cluster`, (4) `pytest tests/e2e/cluster/test_kyverno_admission.py -q -m cluster`. All 4 checks live-verified working against the local cluster (checks 1/2/4 clean; check 3 required copying this worktree's own gitignored `.secrets/vault-init.json` from the main tree — a known, documented worktree-local gap, not a code issue — after which 22/23 vault-cluster tests pass, the 1 remaining failure being a pre-existing, out-of-scope local-cluster DB-grant issue documented in `deferred-items.md`).
- `.github/workflows/e2e-smoke.yml` created and, via a real throwaway PR (#9), found and fixed two genuine bugs neither `python3 -c "import yaml..."` nor local review caught: a GitHub Actions parser-breaking `secrets.*`-in-`if:` construct (isolated via 10 bisected pushes) and a local-dev-calibrated `doctor.sh` CPU/mem floor mismatch for the real 4-CPU/16GiB runner. Both fixed and live-confirmed: the workflow now correctly registers as `pull_request`-triggered, its Docker Hub login step correctly skips (D-21's graceful degradation, live-confirmed), and `make cluster-up` genuinely reaches `kind create cluster`/`kubeadm init` on the real runner.
- Found (not fixed — out of scope) a deep, pre-existing, multi-file infrastructure gap: `kind/cluster.yaml`'s kubelet reservations and DAG hostPath mount are hardcoded for one specific 12-CPU/28GiB local dev host, and 3 CI Helm values files carry hard `nodeSelector`s assuming the same 3-node topology — none of it built to be CI-portable despite CLAUDE.md's own stated "trimmed single-node CI profile" intent. Fully documented in `deferred-items.md` with root cause, evidence, and a recommended follow-up scope.

## Task Commits

Each task was committed atomically:

1. **Task 1: Image-override mechanism for CI's ephemeral cluster-up** - `db72ce6` (feat)
2. **Task 2: make smoke-verify + e2e-smoke.yml** - `c61cbad` (feat)
3. **Task 3, fix 1: e2e-smoke.yml's secrets-in-if: parser bug** - `e99d813` (fix)
4. **Task 3, fix 2: doctor.sh CPU/mem floor override for CI** - `24ad7f9` (fix)
5. **Task 3, documentation: live-PR findings** - `df6a4dc` (docs)

**Plan metadata:** (this SUMMARY's own commit)

## Files Created/Modified

- `scripts/helm-install.sh` - extra-args passthrough on `helm_install`
- `scripts/stages/70-airflow.sh` - `AIRFLOW_IMAGE_OVERRIDE_REPO`/`TAG` branch
- `scripts/ci-set-workload-images.sh` (new) - points `csv_processor_image`/`dbt_image` Variables at a GHCR tag
- `Makefile` - `smoke-verify` target (D-20's 4-point subset)
- `.github/workflows/e2e-smoke.yml` (new) - PR-triggered ephemeral-kind smoke workflow
- `tests/policy/test_offline_gate_stays_offline.py` - added `smoke-verify` to `ARGUED_TESTS_E2E_TARGETS`
- `tests/policy/test_workflow_secrets.py` - widened `ALLOWED_SECRETS` for `DOCKERHUB_USERNAME`/`DOCKERHUB_TOKEN` (D-21 re-audit)
- `.planning/phases/11-ci-cd-completion-operations/deferred-items.md` - documented Task 3's live-PR findings

## Decisions Made

See `key-decisions` in the frontmatter above. In short: `smoke-verify`'s check 1 is a new inline shell assertion (no broad test existed, matches Makefile's own idiom); check 2 is a suite-local CLI trigger+poll, deliberately not reusing the existing XCom-comparing test (would fail in CI for an unrelated reason); `helm_install`'s extra-args passthrough is backward-compatible by construction; two live-discovered GitHub Actions/doctor.sh gotchas were fixed via sanctioned mechanisms (env-mediated `if:`, documented `DOCTOR_MIN_*` overrides).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `e2e-smoke.yml`'s Docker Hub `if:` condition broke GitHub's own workflow parser**
- **Found during:** Task 3's live-PR proof (PR #9), via 10 bisected throwaway pushes
- **Issue:** `if: ${{ secrets.DOCKERHUB_USERNAME != '' }}` on a step makes GitHub reject the entire workflow file at parse time — zero jobs, generic "workflow file issue" failure, the workflow never registers for its own `pull_request` trigger at all. Confirmed via isolated minimal reproductions (even a trivial `run: echo` step with this exact `if:` construct, even using the built-in `secrets.GITHUB_TOKEN`) — not specific to Docker Hub or to a custom secret name.
- **Fix:** Mirror the secret into a job-level `env:` var (`DOCKERHUB_USERNAME: ${{ secrets.DOCKERHUB_USERNAME }}`), gate the step on `env.DOCKERHUB_USERNAME != ''` instead. `secrets.*` stays fine referenced directly inside a step's `with:` block (confirmed in the same bisection).
- **Files modified:** `.github/workflows/e2e-smoke.yml`
- **Verification:** Re-pushed to PR #9; the workflow correctly registered as `pull_request`-triggered and ran; the Docker Hub login step correctly skipped (env var empty, secret unconfigured).
- **Committed in:** `e99d813`

**2. [Rule 3 - Blocking] `scripts/doctor.sh`'s local-WSL2 CPU/mem floors blocked `make cluster-up` on the real CI runner**
- **Found during:** Task 3's live-PR proof, after fix #1 let the workflow actually run
- **Issue:** `doctor.sh`'s `DOCTOR_MIN_CPUS=8`/`DOCTOR_MIN_MEM_GB=20` defaults (calibrated for `docs/wsl/wslconfig.example`, local dev) failed outright against the real runner (measured: 4 CPUs, ~15GiB) — exactly matching CLAUDE.md's own documented "GitHub-hosted runners are 4 CPU / 16 GB" constraint, which `doctor.sh` itself was never made aware of.
- **Fix:** Set `DOCTOR_MIN_CPUS=4`/`DOCTOR_MIN_MEM_GB=14` as step-level `env:` vars on `e2e-smoke.yml`'s own `make cluster-up` step — using `doctor.sh`'s own documented, sanctioned override mechanism, not editing the script's local-dev defaults.
- **Files modified:** `.github/workflows/e2e-smoke.yml`
- **Verification:** Re-pushed to PR #9; `doctor` passed cleanly and `make cluster-up` proceeded to real `kind create cluster`/`kubeadm init` execution.
- **Committed in:** `24ad7f9`

---

**Total deviations:** 2 auto-fixed (1 bug fix, 1 blocking-issue fix via a sanctioned override), both found live against a real GitHub Actions run and both squarely within `.github/workflows/e2e-smoke.yml`, the exact file this plan already declared.
**Impact on plan:** Both were necessary for the workflow to even attempt its own real work; neither is scope creep. A third, much deeper finding (see below) was investigated but deliberately NOT fixed — genuinely outside this plan's authority.

## Issues Encountered

**CRITICAL, unresolved: `kind/cluster.yaml` was never actually built to be CI-portable.** After both fixes above, `make cluster-up` genuinely reached `kind create cluster`/`kubeadm init` on the real GitHub Actions runner and failed there: `kubeadm init`'s control-plane bootstrap timed out (`unable to create ClusterRoleBinding: client rate limiter Wait returned an error: context deadline exceeded`). Root cause, confirmed by direct inspection (read-only — this file is outside this plan's scope): `kind/cluster.yaml`'s three nodes each set `systemReserved.cpu: "5"` + `kubeReserved.cpu: "4"` = 9 CPU reserved, but the real CI runner reports only 4 CPU capacity per node (kind nodes report the host's own full capacity, not a per-node share) — the same "reservation exceeds capacity" failure class this file's own header comments already document occurring once before on an under-provisioned local host. The DAG hostPath mount (`/home/konutec/projects/airflow-platform/airflow/dags`, all three nodes) is also a local-machine-specific absolute path that does not exist on a GH runner. And `helm/values/ci/{minio,ingress-nginx,cnpg-airflow,cnpg-analytics}.yaml` all carry hard `nodeSelector`s against this same 3-node topology's own role labels, meaning even a naive single-node collapse would leave those components permanently `Pending`. This is a real, deep, multi-file infrastructure gap that predates this plan (unchanged since Phase 2) and requires a dedicated architectural follow-up — not a same-plan auto-fix, since `kind/cluster.yaml` has no sanctioned override mechanism the way `doctor.sh` does. Fully documented with evidence and a recommended follow-up scope in `deferred-items.md`'s "Plan 11-04" section.

## User Setup Required

None for this session's own work. The plan's own `user_setup` note (optional `DOCKERHUB_USERNAME`/`DOCKERHUB_TOKEN` repository secrets for D-21) remains a legitimate future step — not configuring them is the D-21-intended graceful-degradation path, live-confirmed working (the login step correctly skipped in PR #9's run).

## Next Phase Readiness

- Plan 11-05 (the merge-gated full E2E suite, `e2e-full.yml`) will face the SAME `kind/cluster.yaml`/CI-Helm-values gap this plan found — its own live proof cannot succeed either until the deferred follow-up above lands. Worth flagging explicitly before that plan's own execution begins.
- Everything this plan's own file scope owns (`helm_install` extra-args, `AIRFLOW_IMAGE_OVERRIDE_*`, `ci-set-workload-images.sh`, `smoke-verify`, and `e2e-smoke.yml`'s own step wiring) is proven correct, live-verified up to the exact point the pre-existing `kind/cluster.yaml` gap blocks it — no further work needed on any of those once the cluster-topology follow-up lands.
- `CICD-09`'s live-proof requirement (D-19/D-20's "a pull request spins up an ephemeral kind cluster... and runs E2E") is NOT met this session — `requirements-completed` is deliberately left empty in this SUMMARY's frontmatter; the orchestrator should not mark CICD-09 complete until the deferred `kind/cluster.yaml` follow-up closes and a genuinely green `e2e-smoke.yml` run is observed.

---
*Phase: 11-ci-cd-completion-operations*
*Completed: 2026-08-23 (Tasks 1-2 fully verified; Task 3 partially complete — 2 real bugs found and fixed, 1 deep pre-existing infrastructure gap found and deferred)*

## Self-Check: PASSED

- FOUND: `scripts/helm-install.sh`
- FOUND: `scripts/stages/70-airflow.sh`
- FOUND: `scripts/ci-set-workload-images.sh`
- FOUND: `Makefile`
- FOUND: `.github/workflows/e2e-smoke.yml`
- FOUND: `tests/policy/test_offline_gate_stays_offline.py`
- FOUND: `tests/policy/test_workflow_secrets.py`
- FOUND: `.planning/phases/11-ci-cd-completion-operations/deferred-items.md`
- FOUND: `.planning/phases/11-ci-cd-completion-operations/11-04-SUMMARY.md`
- FOUND: commit `db72ce6` (Task 1)
- FOUND: commit `c61cbad` (Task 2)
- FOUND: commit `e99d813` (Task 3 fix 1)
- FOUND: commit `24ad7f9` (Task 3 fix 2)
- FOUND: commit `df6a4dc` (Task 3 documentation)
- FOUND: commit `29d64d8` (SUMMARY.md)

No missing items.
