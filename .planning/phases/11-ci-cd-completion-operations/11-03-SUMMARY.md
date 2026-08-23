---
phase: 11-ci-cd-completion-operations
plan: 03
subsystem: infra
tags: [kyverno, admission-control, cel, cosign, image-validating-policy, supply-chain, kubernetes]

# Dependency graph
requires:
  - phase: 11-01
    provides: publish.yml's cosign keyless OIDC signing shape and GHCR lowercase-owner convention; the real, live-signed merge-tagged csv-processor image this plan's positive-case test resolves live
  - phase: 11-02
    provides: 3-image matrix publish.yml + PR-tag parity, confirming the exact subjectRegExp shape (heads/main and pull/<N>/merge) this plan's cosign attestor matches against
provides:
  - "Kyverno admission controller (kyverno chart 3.8.2) deployed in both Helm profiles, positioned at stage 25-26 so every later component's pods pass through it on a normal cluster-up"
  - "kubernetes/kyverno-policy.yaml — cluster-wide policies.kyverno.io/v1 ImageValidatingPolicy requiring cosign keyless verification against publish.yml's OIDC identity, with a live-verified D-16 pinned-upstream/local-dev exception list"
  - "Live positive+negative admission proof (D-18), matching SEC-12's precedent, with an empirical spot-check proving the reason-text assertions are load-bearing"
affects: [11-04, 11-05]

# Tech tracking
tech-stack:
  added: [kyverno chart 3.8.2 (appVersion v1.18.2)]
  patterns:
    - "Live-verify third-party CRD schemas and CEL variable bindings against the actually-pinned chart version by pulling it and probing a real cluster with throwaway policies, rather than trusting a plan's or a docs page's assumed field names — this session found and corrected THREE such assumptions (validationActions enum, absence of spec.skipImageReferences, and a real live-discovered admission bypass in the first exemption-mechanism draft)"
    - "Per-image CEL exemption (matchImageReferences[].expression using the `ref` variable) instead of whole-object matchConditions when a policy must judge each container independently — a whole-object exemption silently admits any unexempted sibling container the moment ONE container in the same pod matches an allowlist"

key-files:
  created:
    - helm/values/local/kyverno.yaml
    - helm/values/ci/kyverno.yaml
    - kubernetes/kyverno-policy.yaml
    - scripts/stages/25-kyverno.sh
    - scripts/stages/26-kyverno-policy.sh
    - tests/e2e/cluster/test_kyverno_admission.py
  modified:
    - helm/versions.env
    - scripts/render-manifests.sh
    - Makefile
    - tests/policy/test_supply_chain_guards.py
    - kubernetes/namespaces.yaml

key-decisions:
  - "KYVERNO_CHART_VERSION=3.8.2 (appVersion v1.18.2), one release back from the 2-day-old 3.9.0 at verification time — matches this project's own established CNPG-over-Zalando 'avoid brand-new releases' pattern"
  - "Kyverno self-manages its own webhook TLS certs (certManager.enabled: false, createSelfSignedCert: false are both chart defaults, confirmed by pulling and reading the chart directly) — no cert-manager dependency added"
  - "validationActions: [Deny], not [Enforce] — the plan's assumed value does not exist in this CRD's real enum (Deny/Audit/Warn), discovered by reading the pulled chart's schema directly"
  - "The D-16 exception list is implemented via a per-image spec.matchImageReferences[].expression (the ref CEL variable), NOT spec.skipImageReferences (which does not exist anywhere in this CRD) and NOT the whole-pod spec.matchConditions mechanism an earlier draft used — that earlier draft was live-tested and found to have a real bypass (a mixed pod with one exempted + one unexempted container was wrongly admitted in full), fixed by moving the exemption to the per-image level and re-verified live"
  - "The D-16 exception list also covers this project's own local-dev-registry images (localhost:5001/*) by design, not just genuine third-party upstream images — the real, currently-deployed Airflow chart and the real, currently-running KubernetesPodOperator task pods both resolve to unsigned localhost:5001 images today (confirmed live), and exempting that registry is what keeps D-15's 'core components admitted on a normal cluster build' truth actually true rather than breaking every real DAG run the moment this policy goes live; documented as a genuine, deliberate scope limitation, not hidden"
  - "The positive-case test resolves the signed image reference LIVE from GHCR (newest 40-hex-git-SHA-tagged csv-processor version) instead of hardcoding a specific SHA/digest, since a frozen reference would eventually go stale and this is exactly what a merge-triggered CI run of the same file would naturally exercise"

requirements-completed: [CICD-09]

# Metrics
duration: ~45min
completed: 2026-08-23
---

# Phase 11 Plan 03: Kyverno Admission-Time Cosign Signature Enforcement Summary

**A cluster-wide `ImageValidatingPolicy` (Kyverno 3.8.2) now denies admission to any pod whose image isn't cosign-signed by this repository's own `publish.yml` OIDC identity or on a live-verified pinned exception list — proven live, positive and negative, against the real cluster, with a real admission bypass found and fixed mid-plan rather than shipped.**

## Performance

- **Duration:** ~45 min
- **Started:** 2026-08-23 (this session, continuing from plan 11-02's completed HEAD)
- **Completed:** 2026-08-23
- **Tasks:** 3/3
- **Files modified:** 11 (6 created, 5 modified)

## Accomplishments

- Kyverno's admission controller, background controller and reports controller are all Running in both Helm profiles, positioned at stage 25-26 (right after namespaces, before ingress-nginx) so every other component's pods on a normal `make cluster-up` actually pass through the webhook.
- `kubernetes/kyverno-policy.yaml`'s `ImageValidatingPolicy` compiled cleanly on the first real apply (`ready: true`, `WebhookConfigured`/`RBACPermissionsGranted` both `True`) and was exercised live through four distinct scenarios before being finalized: an exempted third-party image (admit), an unsigned non-exempt public image (deny), a real cosign-signed merge-tagged csv-processor image (admit), and a mixed pod with one exempted + one unexempted container (correctly denies, after a design revision — see Deviations).
- CI-profile resource sum with Kyverno's three controllers now included: 2.840/3.2 effective cores, 5528/13107Mi effective memory — comfortably under the GitHub-hosted runner budget.
- `tests/e2e/cluster/test_kyverno_admission.py` passes live (2/2) with a genuinely non-vacuous negative case: the reason-text assertions were spot-checked live (not just reasoned about) by reproducing an unrelated denial (nonexistent namespace) and confirming it produces the identical "non-zero exit + pod does not exist" signature the structural-only assertions alone would have accepted as a false green.

## Task Commits

Each task was committed atomically:

1. **Task 1: Kyverno chart, both profiles, versions.env, render/lint wiring, CI-budget check** - `c709b7c` (feat)
2. **Task 2: Cluster-wide ImageValidatingPolicy + early-sequence stage scripts + live deploy** - `c721c23` (feat)
3. **Task 3: Live positive+negative admission proof (D-18)** - `34d9c29` (test)

**Plan metadata:** (this commit)

## Files Created/Modified

- `helm/versions.env` - `KYVERNO_CHART_VERSION=3.8.2`, with a dated comment explaining the deliberate one-release-back choice
- `helm/values/local/kyverno.yaml`, `helm/values/ci/kyverno.yaml` - `crds.install: true`, `cleanupController.enabled: false`, explicit resources for all three active controllers, `serviceMonitor.enabled: false` in both (D-06's three-axis discipline)
- `scripts/render-manifests.sh`, `Makefile` - kyverno wired into the render loop and `helm-lint` target (ninth/tenth chart)
- `tests/policy/test_supply_chain_guards.py` - non-vacuity test confirming the chart version isn't re-declared as a second source in either values file
- `kubernetes/namespaces.yaml` - added the `kyverno` namespace (this file's own sole-owner convention)
- `kubernetes/kyverno-policy.yaml` - the cluster-wide `ImageValidatingPolicy`, with an extensive header comment recording every schema/CEL correction made this session and why
- `scripts/stages/25-kyverno.sh`, `scripts/stages/26-kyverno-policy.sh` - deploy the controller and apply the policy, stage-numbered to run immediately after namespaces
- `tests/e2e/cluster/test_kyverno_admission.py` - live positive+negative proof (D-18)

## Decisions Made

See `key-decisions` in the frontmatter above for the six decisions with full rationale. In short: chart version pinned one release back; no cert-manager dependency (confirmed by inspection); `validationActions: [Deny]` not the plan's assumed `[Enforce]`; the D-16 exception list is a per-image `matchImageReferences` expression, not the nonexistent `skipImageReferences` field and not a whole-pod `matchConditions` (which had a real, live-caught bypass); the exception list also covers this project's own local-dev registry by design; the positive-case test resolves its image reference live rather than hardcoding one.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Plan's assumed `crds: create: true` key does not exist in this chart**
- **Found during:** Task 1, while pulling and inspecting the pinned chart before writing any values
- **Issue:** The real key is `crds.install` (already `true` by chart default); `crds.create` is not a field this chart's `values.yaml` defines at all — writing it would have silently no-op'd
- **Fix:** Used `crds: install: true` in both values files, documented in a header comment
- **Files modified:** `helm/values/local/kyverno.yaml`, `helm/values/ci/kyverno.yaml`
- **Verification:** `helm template kyverno kyverno/kyverno --version 3.8.2 -f helm/values/local/kyverno.yaml` renders all 22 CRDs including `imagevalidatingpolicies.policies.kyverno.io`; confirmed live post-install via `kubectl get crd`
- **Committed in:** `c709b7c` (Task 1 commit)

**2. [Rule 1 - Bug] Plan's assumed `spec.validationActions: [Enforce]` does not exist in this API version**
- **Found during:** Task 2, while reading the pulled chart's bundled `ImageValidatingPolicy` CRD schema directly
- **Issue:** The real enum is exactly `[Deny, Audit, Warn]` — `Enforce` is not a valid value for this policy type/version and would have been rejected at apply time (or silently ignored, depending on webhook validation strictness)
- **Fix:** Used `validationActions: [Deny]`, the fail-closed equivalent the plan's own threat register (T-11-11) actually calls for
- **Files modified:** `kubernetes/kyverno-policy.yaml`
- **Verification:** `kubectl apply --dry-run=server` accepted the manifest; live apply reports `ready: true`
- **Committed in:** `c721c23` (Task 2 commit)

**3. [Rule 1 - Bug] Plan's assumed `spec.skipImageReferences` field does not exist anywhere in this CRD**
- **Found during:** Task 2, while grepping the full CRD schema (both `v1` and `v1alpha1`) for the field the plan's own action text named
- **Issue:** There is no such field in either API version. CONTEXT.md's own "Claude's Discretion" note on exact policy shape explicitly permits this correction
- **Fix:** First draft used `spec.matchConditions` (the mechanism Kyverno's own `test/cli/sample-policy-exclusion/ivpol.yaml` reference example uses for whole-object exemption) — see deviation #4 below for why this was then revised again
- **Files modified:** `kubernetes/kyverno-policy.yaml`
- **Verification:** Confirmed via GitHub code search across `kyverno/kyverno`'s own source and CRD schema, cross-checked against a live `kubectl apply --dry-run=server`
- **Committed in:** `c721c23` (Task 2 commit)

**4. [Rule 1 - Bug, found via live testing] `matchConditions`-based exemption had a real admission bypass**
- **Found during:** Task 2, while live-testing the deployed policy with a deliberately adversarial fourth scenario (a mixed pod: one exempted container + one unexempted, unsigned container) — not part of the plan's own two required test scenarios, added because the whole-pod exemption mechanism's blast radius was unclear from documentation alone
- **Issue:** `spec.matchConditions` evaluates once per admission request against the whole Pod object. `!(containers + initContainers).exists(c, c.image in EXEMPT_LIST)` returns `false` (skip the entire policy) the moment ANY single container in the pod matches the exemption list — live-confirmed: a pod with an exempted `hashicorp/vault:2.0.3` main container and an unexempted `hello-world:latest` init container was WRONGLY admitted in full, silently bypassing the init container's own verification requirement
- **Fix:** Investigated Kyverno's Go source (`pkg/cel/compiler/env.go`, `pkg/cel/matching/image_test.go`) and confirmed a per-image CEL variable (`ref`) is bound and compiles correctly in `matchImageReferences[].expression` for this exact pinned chart version — verified empirically via three throwaway probe `ImageValidatingPolicy` resources applied to the live cluster (one confirming `ref` binds and filters per-image, one confirming `images.initContainers` is a real, separately-populated field). Rewrote the exemption as `matchImageReferences: [{expression: "!(ref.startsWith('localhost:5001/') || ref in [...])"}]`, and extended `validations` to check both `images.containers` and `images.initContainers` independently
- **Files modified:** `kubernetes/kyverno-policy.yaml`
- **Verification:** Re-ran all four scenarios live after the fix — the previously-bypassed mixed pod now correctly denies with an init-container-specific message; the original three scenarios (exempt-alone admit, unsigned-alone deny, signed-GHCR-image admit) all still pass
- **Committed in:** `c721c23` (Task 2 commit)

---

**Total deviations:** 4 auto-fixed (all Rule 1 — corrections to the plan's own assumed CRD field names/values, discovered by pulling and reading the pinned chart directly rather than trusting the plan text, plus one live-caught security bypass in the first design iteration).
**Impact on plan:** All four were necessary for the policy to compile and enforce correctly at all — three are naming/schema corrections with no behavioral ambiguity, and the fourth (the matchConditions bypass) is a genuine correctness fix that would otherwise have shipped a policy provably weaker than its own threat-model claim (T-11-10: "mitigate... any pod image not on the exception list"). No scope creep — all four stayed within `kubernetes/kyverno-policy.yaml`, the exact file the plan already declared for this task.

## Issues Encountered

None beyond the deviations above — the live cluster, Vault, and `gh` authentication were all already confirmed healthy at dispatch time, matching the sequential-execution briefing.

## User Setup Required

None — no external service configuration required. `gh` was already authenticated in this environment from prior phase work.

## Next Phase Readiness

- Kyverno is live, enforcing, and stage-numbered correctly for both the local and CI Helm profiles — plan 11-04 (PR-smoke workflow, D-20) can rely on `tests/e2e/cluster/test_kyverno_admission.py` as one of its four required smoke-subset checks, and on the CI-profile Kyverno deployment fitting the runner budget (already measured this session: 2.840/3.2 cores, 5528/13107Mi).
- `kubernetes/kyverno-policy.yaml`'s exception list is a snapshot of every infrastructure image resolved in this session's own `make manifests` render plus a live Vault query — a future new chart addition (or a routine chart version bump changing an existing image's tag) will surface as that component's pods being denied on the next cluster-up, per T-11-12's own documented "fails loud" design. Whoever adds a new chart to this platform needs to extend this list.
- The local-dev-registry exemption (`localhost:5001/*`) is a documented, deliberate scope limitation, not a gap to silently close later: pointing local dev/CI images at the signed GHCR copies instead is real future work, out of this plan's declared file scope.
- `uv run pytest tests/policy -q` — 156 passed, 4 pre-existing failures (logged in `deferred-items.md` from plan 11-01, unrelated to this plan's changes) unchanged.

---
*Phase: 11-ci-cd-completion-operations*
*Completed: 2026-08-23*

## Self-Check: PASSED

- FOUND: `helm/values/local/kyverno.yaml`
- FOUND: `helm/values/ci/kyverno.yaml`
- FOUND: `kubernetes/kyverno-policy.yaml`
- FOUND: `scripts/stages/25-kyverno.sh`
- FOUND: `scripts/stages/26-kyverno-policy.sh`
- FOUND: `tests/e2e/cluster/test_kyverno_admission.py`
- FOUND: `.planning/phases/11-ci-cd-completion-operations/11-03-SUMMARY.md`
- FOUND: commit `c709b7c` (Task 1)
- FOUND: commit `c721c23` (Task 2)
- FOUND: commit `34d9c29` (Task 3)
- FOUND: commit `5cb0b25` (SUMMARY.md)

No missing items.
