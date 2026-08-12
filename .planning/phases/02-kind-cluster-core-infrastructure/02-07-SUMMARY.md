---
phase: 02-kind-cluster-core-infrastructure
plan: 07
subsystem: infra
tags: [kubeconform, helm, kustomize-free, cnpg, kubernetes, ci, manifest-validation, gitleaks]

# Dependency graph
requires:
  - phase: 02-kind-cluster-core-infrastructure
    provides: "kind/helm/kubeconform pinned installers (02-01), all ten helm/values/{local,ci}/*.yaml files (02-01/02-03/02-04/02-06), helm/versions.env as the single chart-version source"
provides:
  - "tools/k8s/crd_to_jsonschema.py — small, typed, in-repo CRD-to-JSON-Schema converter, passing mypy --strict and ruff select=ALL unmodified"
  - "scripts/vendor-crd-schemas.sh + helm/schemas/cnpg/ — 11 vendored CNPG CRD schemas, regenerable deterministically from the pinned operator chart"
  - "scripts/render-manifests.sh + make manifests — renders both values profiles for all five pinned charts, validates every document with kubeconform -strict"
  - "make manifest-policy — the manifests marker, non-vacuity-proven, ordered behind the render via a Make prerequisite"
  - "tests/policy/test_manifest_validation_fails_closed.py + tests/policy/badmanifests/ — the gate observed rejecting a real invalid CNPG Cluster CR and accepting its valid twin"
  - "CI job 'manifests' in .github/workflows/ci.yml, offline of any cluster"
affects: [02-08]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "kubeconform -schema-location lowercases {{.ResourceKind}} — vendored schema filenames must be {kind_lowercase}_{version}.json, confirmed by reading kubeconform 0.8.0's pkg/registry/registry.go and by running the pipeline live"
    - "-skip CustomResourceDefinition is required alongside a vendored CRD-instance schema location: kubeconform's own default schema catalogue (yannh/kubernetes-json-schema) has never carried a schema for the CustomResourceDefinition kind itself, at any Kubernetes version — a narrow, upstream-catalogue gap, distinct from validating instances of CNPG-defined kinds"
    - "path-scoped .gitleaks.toml allowlist for a gitignored, chart-rendered build directory, mirroring the existing tests/fixtures/ pattern but path-only (no content regex) because the directory is architecturally incapable of holding a real credential (D-08/D-14/D-15)"

key-files:
  created:
    - tools/k8s/__init__.py
    - tools/k8s/crd_to_jsonschema.py
    - scripts/vendor-crd-schemas.sh
    - helm/schemas/cnpg/README.md
    - helm/schemas/cnpg/{backup,cluster,clusterimagecatalog,database,databaserole,failoverquorum,imagecatalog,pooler,publication,scheduledbackup,subscription}_v1.json
    - scripts/render-manifests.sh
    - tests/policy/badmanifests/cluster_null_postgresql.yaml
    - tests/policy/badmanifests/good_cluster_null_postgresql.yaml
    - tests/policy/test_manifest_validation_fails_closed.py
  modified:
    - Makefile
    - .github/workflows/ci.yml
    - .gitleaks.toml

key-decisions:
  - "Hand-written tools/k8s/crd_to_jsonschema.py rather than vendoring kubeconform's own openapi2jsonschema.py, per the plan's own reasoning: a vendored third-party script would need a permanent mypy/ruff exclusion, and the lockstep property that matters comes from regenerating from the pinned chart, not from the converter's identity"
  - "Vendored schema filenames use lowercase kind (cluster_v1.json), confirmed against kubeconform 0.8.0 source rather than assumed from the plan's illustrative {{.ResourceKind}}_{{.ResourceAPIVersion}} template text"
  - "-skip CustomResourceDefinition added to the kubeconform invocation (Rule 1 — bug found and fixed this session, not anticipated by 02-RESEARCH.md): the cloudnative-pg chart's own 11 CustomResourceDefinition documents have no schema in kubeconform's default catalogue at any Kubernetes version; every *instance* of a CNPG-defined kind (a Cluster CR) is still validated in full via the vendored schema location"
  - "Rule 1 — .gitleaks.toml gained a path-scoped allowlist for build/manifests/: composing manifests into ci alongside the existing gitleaks target surfaced 42 false positives (Helm's own checksum/*-secret annotations and chart-default placeholders) in the freshly-rendered, gitignored tree; fixed narrowly rather than weakening the scan"

requirements-completed: [CICD-07, INFRA-10]

# Metrics
duration: ~3h (majority spent on repeated full make check/make ci verification cycles, each 6-11 min wall time, run to ground truth rather than assumed)
completed: 2026-08-12
---

# Phase 2 Plan 7: Offline Manifest Validation Gate Summary

**`make manifest-policy` renders both Helm values profiles for all five pinned charts, validates every document with `kubeconform -strict` against a vendored CNPG CRD schema set, and has been observed live rejecting a real invalid `Cluster` CR and accepting its valid twin — all offline of any cluster, wired into CI as its own job, and kept out of `make check`'s network-free contract.**

## Performance

- **Duration:** ~3h. The implementation itself (three tasks) was the smaller share; most wall time went to running the repository's full `make check` / `make ci` gates to ground truth after each task, each cycle taking 6–11 minutes (policy suite ~6 min, corpus fixture verification, full-history gitleaks scan).
- **Tasks:** 3/3 complete and fully verified, including a final clean `make ci` run (exit 0) after all three commits landed.
- **Files modified:** 18 (12 created under `tools/k8s/`, `scripts/`, `helm/schemas/cnpg/`, `tests/policy/`; 3 modified: `Makefile`, `.github/workflows/ci.yml`, `.gitleaks.toml`)

## Accomplishments

- Wrote `tools/k8s/crd_to_jsonschema.py` — a ~200-line, fully typed, in-repo CRD→JSON-Schema converter that passes this repository's `mypy --strict` and `ruff select=["ALL"]` gates without any new exclusion, as the plan required
- Discovered and documented, by reading kubeconform 0.8.0's own Go source (`pkg/registry/registry.go`), that its `-schema-location` template variable `{{.ResourceKind}}` is lowercased — vendored schema filenames are `cluster_v1.json`, not `Cluster_v1.json`; re-verified live end-to-end (missing-schema error → `Valid: 1, Invalid: 0` on the exact same sample)
- Vendored all 11 CloudNativePG CRD schemas into `helm/schemas/cnpg/` via `scripts/vendor-crd-schemas.sh`, proven idempotent and byte-identical across two consecutive regenerations
- Built `scripts/render-manifests.sh` / `make manifests`: renders both `local` and `ci` values profiles for all five pinned charts (six `helm template` calls per profile — the `cluster` chart renders twice, airflow metadata + analytical) into a gitignored `build/manifests/<profile>/`, then validates everything with `kubeconform -strict -kubernetes-version 1.35.5`
- Discovered and fixed a genuine kubeconform-catalogue gap (Rule 1): kubeconform's own default schema catalogue has never carried a schema for the `CustomResourceDefinition` kind itself (verified absent at v1.35.5, v1.30.0 and `master`) — added a narrowly-scoped `-skip CustomResourceDefinition`, which does not weaken validation of any CNPG-defined *instance* kind (a `Cluster` CR), since that still goes through the vendored schema location
- Added `helm-lint` (helm pull + helm lint per chart per values profile, no version literal) as `manifests`' prerequisite
- Wrote `tests/policy/test_manifest_validation_fails_closed.py` — five tests, `pytest.mark.manifests`: the negative case (a real, previously-documented `spec.postgresql: null` failure) and its positive control, a CRD-schema load-bearing proof (missing schema → fails; supplied → passes), the real rendered CNPG Cluster manifests validating (the natural home for the `REQUIRE_RENDERED_MANIFESTS` anti-vacuity switch), and a guard proving `tests/policy/badmanifests/` cannot poison `make manifests`
- Wired `Makefile`: `policy` deselects the `manifests` marker; new `manifest-policy: manifests` target runs it with `REQUIRE_RENDERED_MANIFESTS=1`; `ci` now reaches `manifest-policy` (not `manifests` directly), so the render is ordered ahead of the tests that read it via a declared prerequisite, not list position
- Discovered and fixed a second, unrelated Rule 1 bug: composing `manifests` into `ci` alongside the pre-existing `gitleaks` target surfaced 42 false-positive `gitleaks dir` findings in the freshly-rendered `build/manifests/` tree (Helm's own `checksum/*-secret` annotations and chart-default placeholder strings); fixed with a path-scoped `.gitleaks.toml` allowlist entry rather than weakening the scan
- Final clean `make ci` run: **exit 0** — `check` (lint/format/typecheck/imports/policy/test/fixtures-verify), `manifest-policy` (5 passed, 0 skipped), `gitleaks` (0 leaks, both git-history and working-tree scans), `gitleaks-selftest` (scanner proven live)

## Task Commits

1. **Task 1: Pinned kubeconform and vendored CloudNativePG CRD schemas** - `706f71f` (feat)
2. **Task 2: `make manifests` — render both profiles for all five charts and validate them** - `c81cbd9` (feat)
3. **Task 3: Prove the validator discriminates — a broken manifest and its valid twin, behind a ci-only target** - `35d8e4e` (test)

No separate plan-metadata commit — this summary's own commit (worktree mode) is the final commit for this plan.

## Files Created/Modified

- `tools/k8s/__init__.py` - package docstring, matches `tools/security/__init__.py`'s convention
- `tools/k8s/crd_to_jsonschema.py` - the CRD→JSON-Schema converter
- `scripts/vendor-crd-schemas.sh` - regenerates `helm/schemas/cnpg/` from the pinned CNPG operator chart
- `helm/schemas/cnpg/README.md` + 11 `*.json` files - vendored, generated, committed CRD schemas
- `scripts/render-manifests.sh` - the render-and-validate pipeline behind `make manifests`
- `tests/policy/badmanifests/cluster_null_postgresql.yaml` + `good_cluster_null_postgresql.yaml` - the paired invalid/valid `Cluster` CR
- `tests/policy/test_manifest_validation_fails_closed.py` - the five non-vacuity tests
- `Makefile` - `helm-lint`, `manifests`, `manifest-policy` targets; `policy` deselects `manifests`; `ci` composition updated
- `.github/workflows/ci.yml` - new `manifests` job (`make install` then `make manifest-policy`)
- `.gitleaks.toml` - path-scoped allowlist for `build/manifests/`

## Decisions Made

- See frontmatter `key-decisions` for the four load-bearing calls: the hand-written converter, the lowercase-kind filename convention (confirmed against kubeconform source, not assumed), the `-skip CustomResourceDefinition` fix, and the `.gitleaks.toml` allowlist fix.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] kubeconform's default schema catalogue has no schema for the `CustomResourceDefinition` kind itself, at any Kubernetes version**
- **Found during:** Task 2, first live run of `scripts/render-manifests.sh`
- **Issue:** The `cnpg/cloudnative-pg` chart itself renders 11 `CustomResourceDefinition` documents (the meta-resources that *define* `Cluster`/`Backup`/etc., not instances of them). kubeconform's own default schema catalogue (`yannh/kubernetes-json-schema`) has never published a schema file for the `CustomResourceDefinition` kind itself — confirmed absent at `v1.35.5-standalone-strict`, `v1.30.0-standalone-strict`, and `master-standalone-strict` alike via the catalogue's own GitHub API contents listing. Every one of the 22 resulting "errors" was this same missing-schema class, not a real validation failure.
- **Fix:** Added `-skip CustomResourceDefinition` to the `kubeconform` invocation in `scripts/render-manifests.sh`, scoped to exactly one kind, with an inline comment recording the verification. Every *instance* of a CNPG-defined kind (a `Cluster` CR) is still validated in full via the vendored `helm/schemas/cnpg/` location — this is what Pitfall 3 and the CRD-schema non-vacuity test in Task 3 actually exercise.
- **Files modified:** `scripts/render-manifests.sh`
- **Verification:** `kubeconform -strict` reports `Valid: 135, Invalid: 0, Errors: 0, Skipped: 22` (22 = 11 CRDs × 2 profiles) instead of `Errors: 22`
- **Committed in:** `c81cbd9`

**2. [Rule 1 - Bug] Composing `manifests` into `ci` produced 42 false-positive `gitleaks dir` findings**
- **Found during:** Task 2, first full `make ci` verification run
- **Issue:** `gitleaks dir .` scans the raw filesystem (not git history, and does not respect `.gitignore`). Once `manifests` populated the gitignored `build/manifests/` tree with full Helm chart renders, `gitleaks` flagged 42 "leaks" — every one either a Helm `checksum/*-secret` annotation (a sha256 digest of a chart's default Secret content, matched purely because 64 hex characters look key-shaped) or a chart-default placeholder string neither this repository nor its committed values files ever supplied.
- **Fix:** Added a path-scoped `.gitleaks.toml` allowlist entry for `^build/manifests/`, following the existing allowlist convention but path-only (no content regex) rather than path+pattern: unlike the corpus allowlist (where a real secret dropped into `tests/fixtures/` must still be caught), `build/manifests/` is architecturally incapable of holding a real credential — D-08/D-14/D-15 already establish every values file references credentials by Secret *name* only, and `tests/policy/test_workflow_secrets.py` polices those files independently.
- **Files modified:** `.gitleaks.toml`
- **Verification:** `gitleaks dir --redact --no-banner --exit-code 1 .` → `no leaks found` (was 42); full `make ci` gitleaks step green
- **Committed in:** `c81cbd9`

---

**Total deviations:** 2 (both Rule 1 — bugs discovered and fixed during live execution, not anticipated by 02-RESEARCH.md)
**Impact on plan:** Both were necessary for the plan's own success criteria ("kubeconform -strict reports zero invalid documents"; "make check/make ci both green") to hold true under real execution. No scope creep beyond fixing what broke.

## Issues Encountered

**kubeconform's `{{.ResourceKind}}` template variable case.** 02-RESEARCH.md's verified pipeline example did not state whether the vendored schema filename should use the kind as written in the manifest (`Cluster`) or lowercased (`cluster`). Resolved by reading kubeconform 0.8.0's own `pkg/registry/registry.go` (`ResourceKind: strings.ToLower(resourceKind)`) and confirming live: a `Cluster_v1.json` file is silently never found; `cluster_v1.json` is found and validates. Documented at length in `tools/k8s/crd_to_jsonschema.py`'s module docstring and `helm/schemas/cnpg/README.md` so a future contributor does not have to re-derive this.

**Background-task wall time.** This session's own `make check`/`make ci` verification runs each took 6–11 minutes of real wall time (the policy suite alone runs ~350–390s; `fixtures-verify` regenerates a ~293 MB corpus). Several runs were started, monitored via background-task polling, and one run's output was inspected mid-flight while files were still being edited — that run reported two failures (`RUF100` unused-noqa and a `test_ci_calls_make_ci.py` failure) that were artifacts of editing the tree concurrently with a live test run, not real regressions; both were re-confirmed clean on a subsequent uninterrupted run. The final `make ci` run, executed after all three task commits landed with no concurrent edits, passed cleanly end-to-end (`EXIT:0`).

## User Setup Required

None — no external service configuration required. `tools/bin/kubeconform`, `tools/bin/helm` and `tools/bin/gitleaks` are all pinned, digest-verified, gitignored, and installed automatically by the `manifests`/`gitleaks` Make targets on first use.

## Known Stubs

None. Every file this plan commits is the real, intended implementation.

## Threat Flags

| Flag | File | Description |
|------|------|-------------|
| threat_flag: gate-scope-narrowing | `.gitleaks.toml` | New path-scoped allowlist entry (`^build/manifests/`) added to an existing Phase 1 security control (SEC-02/SEC-11's `gitleaks dir` scan) that this plan's own `<threat_model>` does not name. Narrowly scoped and justified above (Deviation 2) — the directory is gitignored, chart-rendered, and architecturally incapable of holding a real credential per D-08/D-14/D-15 — but it is a modification to a control this plan does not own, so it is flagged for reviewer visibility rather than silently folded in. |

## Next Phase Readiness

**Fully verified, file-level and by live execution.** The rendering half of ROADMAP success criterion 5 is mechanically true: both values profiles render for all five pinned charts, every document validates under `kubeconform -strict` against Kubernetes 1.35.5, the manifest gate is defined only in the Makefile and invoked from CI by one `make` call, and it has been observed rejecting a broken manifest and accepting its twin, and failing without the vendored CRD schemas. `make check` stays green offline with `tools/bin/` absent and no network. A final, clean `make ci` run (no concurrent tree edits) passed end-to-end.

Plan 02-08 (the sizing half of success criterion 5 — CI-profile resource requests fitting the 4 CPU / 16 GB runner budget) can proceed; it is bounded statically and does not depend on anything this plan changed beyond `build/manifests/` already existing as a render target.

---
*Phase: 02-kind-cluster-core-infrastructure*
*Completed: 2026-08-12*

## Self-Check: PASSED

- All 9 representative created files verified present on disk (`tools/k8s/__init__.py`, `tools/k8s/crd_to_jsonschema.py`, `scripts/vendor-crd-schemas.sh`, `helm/schemas/cnpg/README.md`, `helm/schemas/cnpg/cluster_v1.json`, `scripts/render-manifests.sh`, `tests/policy/badmanifests/{cluster_null_postgresql,good_cluster_null_postgresql}.yaml`, `tests/policy/test_manifest_validation_fails_closed.py`)
- All three task commits verified present in `git log`: `706f71f`, `c81cbd9`, `35d8e4e`
- Final clean `make ci` run (no concurrent tree edits) confirmed `EXIT:0`
