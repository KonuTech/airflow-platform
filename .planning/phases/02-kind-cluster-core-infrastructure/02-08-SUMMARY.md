---
phase: 02-kind-cluster-core-infrastructure
plan: 08
subsystem: testing
tags: [policy-tests, kubernetes, helm, kubeconform, resource-sizing, secrets-scanning, supply-chain, pytest]

# Dependency graph
requires:
  - phase: 02-kind-cluster-core-infrastructure
    provides: "02-07: make manifests / build/manifests/{local,ci}/ render output, the manifests pytest marker, manifest-policy target"
  - phase: 02-kind-cluster-core-infrastructure
    provides: "02-01 through 02-06: all ten helm/values/{local,ci}/*.yaml files, helm/versions.env, scripts/ and tools/k8s/ trees"
provides:
  - "tests/policy/test_manifest_resources.py — D-12: the CI profile's summed container requests (including both CNPG Cluster CRs) fit a 4 CPU / 16 GB runner with 20% headroom, and every container in both profiles carries a CPU/memory request and limit"
  - "tests/policy/test_values_profiles.py — D-06: the two values profiles diverge only on four named, argued axes (replica counts, resource sizing, monitoring enablement, executor)"
  - "tests/policy/test_no_manual_kubectl_surgery.py — INFRA-07 in its decidable form: no script mutates cluster state except get/wait/apply-to-committed-path/apply-to-stdin"
  - "tests/policy/test_workflow_secrets.py — widened for D-14 to helm/, kubernetes/, kind/, scripts/ (Phase 1's workflow-scoped claim untouched)"
  - "tests/policy/test_supply_chain_guards.py — extended with image-tag-pin agreement against helm/versions.env and a no-mutable-tag scan"
affects: []

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "A Kubernetes quantity parser built from a suffix table + a dynamically-constructed regex (longest-suffix-first alternation) rather than a hand-rolled string strip — covers plain/milli/decimal-SI/binary/exponent forms"
    - "Two-tier text scanning for shell scripts: mask quoted spans before searching for a bare command word (defeats false positives from prose like `fail \"kubectl not found\"`), then a lightweight local token scanner (not full shlex) for the subcommand/flag walk after the match — shlex collapses a `\"$(...)\"` command substitution into one opaque token and cannot see inside it"
    - "Exact-key-name matching (via a flattened-path walk) instead of substring/regex matching for forbidden credential keys, so `fernetKeySecretName` (permitted reference) is never mistaken for `fernetKey` (forbidden literal) merely because one contains the other"

key-files:
  created:
    - tests/policy/test_manifest_resources.py
    - tests/policy/test_values_profiles.py
    - tests/policy/test_no_manual_kubectl_surgery.py
  modified:
    - tests/policy/test_workflow_secrets.py
    - tests/policy/test_supply_chain_guards.py

key-decisions:
  - "[Rule 1 — deviation from the plan's literal wording, documented in the module itself] test_no_manual_kubectl_surgery.py permits `kubectl apply -f -` (stdin) in addition to the plan's literal 'committed path under kubernetes/' wording. scripts/minio-credentials.sh and scripts/airflow-metadata-secret.sh (both cited in the plan's own read_first list, Pattern 4/D-14) build a Secret manifest in-memory and pipe it to `kubectl apply -f -` on stdin — by construction this content can never be a committed path, since D-14 requires it generated at cluster-up and to live only in the cluster. Implementing the plan's wording literally would fail this test against the exact scripts the plan cites as load-bearing."
  - "Kubectl-invocation detection recognizes bare `kubectl` and this repository's own `_kubectl`/`_kubectl_<suffix>` wrapper-function naming convention (scripts/wait-for.sh, scripts/minio-credentials.sh, scripts/airflow-metadata-secret.sh), but NOT the exported `${KUBECTL}` override variable used directly — recorded as a known, narrow gap (today it appears exactly once, read-only, in scripts/doctor.sh's own version-compatibility check) rather than silently assumed away."
  - "Resource-sizing budget: 4 CPU / 16 GB GitHub-hosted-runner nominal, with a flat 20% headroom fraction (effective 3.2 CPU / 12.8 GiB) — real CI totals measured at ~2.16 cores / ~3.9 GiB, comfortably inside the effective budget with room for the OS, kube-proxy/CNI and the Actions agent, none of which this repository's charts render."
  - "D-06's divergence classification uses structural predicates over dotted leaf paths (any path segment equal to 'resources', or ending in 'storage.size', for the resource-sizing axis; 'metrics'/'monitoring' segments for the monitoring axis) rather than an enumerated list of exact key names — verified against the real diff of all six components before being written, so the classification matches what the values files actually do today, not an assumption about what they might do."

requirements-completed: [CICD-07, INFRA-10, INFRA-07]

# Metrics
duration: ~4h (session included a mid-execution stall recovery after a long blocking verification call; no work was lost — subsequent verification was re-run in smaller, bounded pieces)
completed: 2026-08-12
---

# Phase 2 Plan 8: Repository-Invariant Tests — Sizing, Divergence, IaC, Secrets, Supply Chain Summary

**Five static pytest modules close the sizing half of ROADMAP success criterion 5 and the remaining INFRA-07/D-14 claims: the CI profile's summed container requests (including both CNPG `Cluster` CRs) provably fit a 4 CPU / 16 GB runner, every container in both values profiles is sized, the two profiles diverge only on four named axes, no script performs manual `kubectl` surgery, and no credential literal or mutable image tag has leaked into `helm/`, `kubernetes/`, `kind/` or `scripts/`.**

## Performance

- **Duration:** ~4h, including a mid-session stall (a single long blocking verification call tripped the no-progress watchdog) recovered without losing any work — every file was already on disk and every subsequent verification pass was re-run in smaller, bounded pieces (individual pytest files, then `make manifest-policy`, `make gitleaks`, `make gitleaks-selftest` standalone, then a fully-backgrounded `make ci` polled with short bounded checks rather than one giant blocking call).
- **Tasks:** 3/3 complete and fully verified, including two clean end-to-end `make check` runs and two clean end-to-end `make ci` runs (the first of each pass was used to design/debug against the real tree; the final pass on the committed code was clean on the first try).
- **Files modified:** 5 (3 created, 2 modified)

## Accomplishments

- Wrote `tests/policy/test_manifest_resources.py`: a Kubernetes quantity parser (plain/milli/decimal-SI/binary/exponent forms, built from a suffix table and a dynamically-constructed longest-suffix-first regex), a container walker dispatching on Pod-template kinds with an explicit `NO_CONTAINER_KINDS` allow-list and a fail-closed `ValueError` naming any other kind, and a `Cluster`-CR special case (`spec.resources.requests × spec.instances`) — the exact fix for 02-RESEARCH.md Pitfall 6, where the prototype's naive walker summed zero for both PostgreSQL clusters
- `test_ci_profile_fits_runner`: measured the real rendered CI profile at **~2.16 CPU-cores / ~3.9 GiB**, against an effective budget of 3.2 CPU / 12.8 GiB (4 CPU/16 GB less a stated 20% headroom) — both CNPG `Cluster` CRs confirmed contributing non-zero requests
- `test_every_container_is_sized`: confirmed zero unsized containers across both rendered profiles (every container in every one of the six components carries a CPU/memory request and limit)
- `test_this_module_runs_after_the_render` imports `parse_prerequisites`/`chain` from `test_ci_calls_make_ci.py` (per the plan's explicit instruction) rather than reimplementing a Makefile parser, and asserts the `manifest-policy: manifests` prerequisite edge and `policy`'s marker deselection
- Wrote `tests/policy/test_values_profiles.py`: flattened every one of the ten values files to dotted leaf paths and diffed all six components against the real tree — confirmed the only differing paths across the whole tree are `resources.*`, `storage.size`, `controller.metrics.enabled`, and `executor`, which is exactly what the four-entry `PERMITTED_AXES` table (each carrying a written argument) classifies
- Wrote `tests/policy/test_no_manual_kubectl_surgery.py`: INFRA-07 stated as a decidable structural claim. Built and empirically validated (against the real `scripts/`/`tools/` tree, not just by inspection) a scanner that masks quoted spans before searching for a kubectl command word — defeating false positives from `scripts/doctor.sh`'s own prose (`fail "kubectl not found..."`) — and recognizes this repository's `_kubectl`/`_kubectl_<suffix>` wrapper-function convention, correctly treating a wrapper's own `"$@"`-forwarding definition as "nothing to check" while still catching its call sites' literal subcommand
- Widened `tests/policy/test_workflow_secrets.py` additively for D-14: a second scanned surface over `helm/`, `kubernetes/`, `kind/`, `scripts/`, reporting an exact-match forbidden literal-holding key (`rootPassword`, `fernetKey`, `webserverSecretKey`) or a committed `kind: Secret` `data:`/`stringData:` block — the Phase 1 workflow-scoped `ALLOWED_SECRETS` claim is provably unchanged (`test_the_allowed_secrets_set_is_unchanged_by_d14`)
- Extended `tests/policy/test_supply_chain_guards.py` with image-tag-pin agreement (MinIO and Airflow tags against `helm/versions.env`, mirroring `test_pinned_tool_versions_agree.py`'s load-bearing-source model exactly) and a no-mutable-tag scan across every values file
- Two full, clean `make check` runs and two full, clean `make ci` runs (`105 passed, 10 deselected` offline; `10 passed, 0 skipped` under `manifest-policy`, up from 5 at the end of plan 02-07; `gitleaks`/`gitleaks-selftest` both clean)

## Task Commits

1. **Task 1: The two D-12 tests — the CI profile fits its runner, and every container is sized** - `c341234` (test)
2. **Task 2: The divergence-axis rule — three permitted axes plus one argued fourth, and nothing else** - `5f6c51a` (test)
3. **Task 3: Infrastructure as code, no credential literals, no mutable image tags** - `992742d` (test)

No separate plan-metadata commit — this summary's own commit (worktree mode) is the final commit for this plan.

## Files Created/Modified

- `tests/policy/test_manifest_resources.py` - the D-12 sizing tests, the quantity parser, the container walker, the Cluster-CR special case, the render-ordering assertion
- `tests/policy/test_values_profiles.py` - the D-06 divergence-axis rule and its permitted-axis table
- `tests/policy/test_no_manual_kubectl_surgery.py` - the INFRA-07 kubectl-surgery scanner
- `tests/policy/test_workflow_secrets.py` - widened (additively) with the D-14 whole-infrastructure-tree credential scan
- `tests/policy/test_supply_chain_guards.py` - extended with image-tag-pin agreement and the mutable-tag scan

## Decisions Made

See frontmatter `key-decisions` for the four load-bearing calls: the deliberate `kubectl apply -f -` (stdin) widening beyond the plan's literal wording (documented in the test module itself, not silently applied), the recognized-vs-unrecognized kubectl-invocation boundary, the 20% CI-budget headroom fraction, and the structural (not enumerated) divergence-axis predicates.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug/gap in the plan's literal wording] `test_no_manual_kubectl_surgery.py`'s permitted-apply predicate widened to include `kubectl apply -f -` (stdin)**
- **Found during:** Task 3, while designing the scanner against the real `scripts/` tree before writing any assertions
- **Issue:** The plan's action text states "Permit `kubectl wait`, `kubectl get`, and `kubectl apply` only when its argument is a committed path under `kubernetes/`." Implemented literally, this fails `test_no_script_performs_manual_kubectl_surgery` against `scripts/minio-credentials.sh` and `scripts/airflow-metadata-secret.sh` — both of which the plan's own read_first list cites as the D-14/Pattern-4 credential-materialisation scripts, and both of which build a Secret manifest in-memory and pipe it to `kubectl apply -f -` on stdin. By D-14's own design (credentials generated at `cluster-up`, living only in the cluster), this content can never be a committed path — the plan's literal wording and the plan's own cited example scripts are in tension.
- **Fix:** Widened the permitted-apply predicate to accept `-f -` (stdin) as a second, explicitly-documented case, alongside `-f <committed kubernetes/ path>`. Recorded prominently in the module's own docstring (a labeled "DELIBERATE WIDENING" section) rather than applied silently, with the reasoning spelled out and a dedicated test (`test_stdin_apply_is_not_reported`) proving it takes effect.
- **Files modified:** `tests/policy/test_no_manual_kubectl_surgery.py`
- **Verification:** `test_the_real_scripts_produce_no_messages` passes against the real tree; `test_an_imperative_mutation_is_reported` confirms `-f /tmp/not-committed.yaml` (a non-stdin, non-kubernetes/ target) is still reported, so the widening is narrowly scoped to exactly the stdin case and does not broaden the rule generally.
- **Committed in:** `992742d`

---

**Total deviations:** 1 (Rule 1 — a genuine tension between the plan's literal wording and the plan's own cited example scripts, resolved in the direction that keeps the test true against the real, already-reviewed tree, and documented rather than silently applied)
**Impact on plan:** Necessary for the plan's own acceptance criterion ("`uv run --frozen pytest tests/policy/test_no_manual_kubectl_surgery.py -q` exits 0") to hold against the real repository. No scope creep — every other `-f` target still must be a committed `kubernetes/` path.

## Issues Encountered

**Session stall mid-verification.** A single long blocking `tail -f` call (used to wait for a backgrounded `make ci` run) tripped a 10-minute no-progress watchdog. No work was lost — all five test files were already committed-ready on disk, confirmed by `git status` immediately on resume. Recovery: re-verified each piece independently and with bounded timeouts (individual pytest files, `make manifest-policy` alone, `make gitleaks`/`make gitleaks-selftest` alone), then ran a final `make ci` fully detached in the background and polled it with short, bounded `tail`/`ps` checks rather than one long blocking call. The final `make ci` run against the exact committed code was clean end to end.

**Two real dry-run/mutation false positives found and fixed during design, before any test was committed** (not deviations — these were caught while empirically validating the detectors against the real tree, which is exactly what the plan's own non-vacuity instructions ask for):
- `test_no_manual_kubectl_surgery.py`'s first draft (regex-search without quote-masking) flagged `scripts/doctor.sh`'s own error-message prose (`fail "kubectl not found ..."`, `"install kubectl matching Kubernetes ..."`) as invocations. Fixed by masking quoted spans before searching for the command word.
- The same first draft also mis-parsed shell function *definitions* (`_kubectl_wait() {`) as invocations with a bogus "()" subcommand. Fixed with an explicit function-definition line exclusion, verified via a dedicated false-positive-control test.

Neither reached a committed test in a broken state — both were found and fixed during the empirical-validation step the plan's own read_first instructions call for ("Prove non-vacuity... pair it with the false-positive control that the real scripts produce no messages"), before the first `make check` run.

## User Setup Required

None — no external service configuration required. All five modules are pure static analysis over committed files and (for two tests in `test_manifest_resources.py`) the gitignored `build/manifests/` render output that `make manifests` produces automatically.

## Known Stubs

None. Every file this plan commits is the real, intended implementation, and every claim is backed by a test that has been observed both passing against the real tree and failing against an injected defect.

## Threat Flags

None. This plan's own `<threat_model>` names every file it modifies (T-02-34 through T-02-37, T-02-SC), and no file outside that list was touched.

## Next Phase Readiness

**Phase 2 is now fully closed.** This was the final plan in the phase (wave 6 of 6). The sizing half of ROADMAP success criterion 5 is mechanically true (`test_ci_profile_fits_runner`), joining the rendering half plan 02-07 already closed. Every claim this phase has been making since its first commit — the two-database topology, the five MinIO buckets with deny-delete, the four Airflow workloads, the offline manifest gate, the CI-profile sizing budget, the four-axis values-profile divergence rule, the no-manual-kubectl-surgery claim, and the whole-infrastructure-tree credential scan — is now held by a test that has been watched failing (via mutation) and passing (against the real tree). No test in this plan can report green by skipping: the two rendered-output tests fail rather than skip when `REQUIRE_RENDERED_MANIFESTS=1` and their input is absent, and the ordering that supplies that input (`manifest-policy: manifests`) is itself asserted by a test.

No blockers for Phase 3 (the parallel `csv_processor`/`dataplat` track) or subsequent phases that build on this cluster.

---
*Phase: 02-kind-cluster-core-infrastructure*
*Completed: 2026-08-12*

## Self-Check: PASSED

- All 5 files created/modified by this plan verified present on disk (`tests/policy/test_manifest_resources.py`, `tests/policy/test_values_profiles.py`, `tests/policy/test_no_manual_kubectl_surgery.py`, `tests/policy/test_workflow_secrets.py`, `tests/policy/test_supply_chain_guards.py`)
- All three task commits verified present in `git log`: `c341234`, `5f6c51a`, `992742d`
- Two full, clean `make check` runs and two full, clean `make ci` runs confirmed (final pass against the exact committed code: `EXIT:0`, `manifest-policy` reporting `10 passed, 0 skipped`)
