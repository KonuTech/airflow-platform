---
phase: 02-kind-cluster-core-infrastructure
verified: 2026-08-12T21:40:00Z
status: passed
resolved: 2026-08-12T20:05:00Z
score: 5/5 roadmap success criteria verified; 9/9 requirements functionally satisfied
overrides_applied: 0
human_verification:
  - test: "Read docs/adr/0006-unmaintained-upstream-artifacts.md and confirm acceptance of the three named unmaintained upstream artifacts (pgsty/minio, registry.k8s.io/ingress-nginx/controller:v1.15.1, quay.io/minio/mc) and their migration triggers"
    expected: "Explicit sign-off that running an unpatched ingress-nginx controller (threat T-02-21, high severity, disposition: accept) on host port 80/loopback is acceptable on the stated terms, and that the named migration triggers (public exploit, upstream abandonment escalation, etc.) are events the developer would actually notice"
    why_human: "This is a supply-chain risk acceptance, not a test assertion (02-05-PLAN.md's own <human-check>, task type auto, deferred to end-of-phase per workflow.human_verify_mode). No automated check can certify that a human accepts a named residual risk."
    result: "Accepted 2026-08-12 — see 02-HUMAN-UAT.md for the full sign-off record (loopback-only exposure confirmed, no plans to expose beyond local machine, all three artifacts and their migration triggers accepted)."
---

# Phase 2: kind Cluster & Core Infrastructure — Verification Report

**Phase Goal:** A production-like Kubernetes data platform that can be destroyed and recreated reproducibly from committed files, with the CI-sized profile written from the first infrastructure commit
**Verified:** 2026-08-12T21:40:00Z
**Status:** human_needed
**Re-verification:** No — initial verification

**Mode note.** ROADMAP marks this phase `mode: mvp`, but the goal text is not in User Story form ("As a ... I want ... so that ..."). Following the precedent set by Phase 1's verification, the five explicit numbered Success Criteria are treated as the contract since they are more specific than a derived user story would be. Flagged as Info, not a gap.

## Goal Achievement

All five ROADMAP Success Criteria were verified against the **live, currently-running** kind cluster (`kind-airflow-platform` context, 3 nodes, 5h48m uptime at verification time — the state left by plan 02-08's final commit, itself built from a full cold `make cluster-rebuild` performed during plan 02-06's execution), not merely by reading SUMMARY.md claims or static files.

### Observable Truths (ROADMAP Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | `make cluster-down && make cluster-up` recreates a 3-node kind cluster and redeploys MinIO, both PostgreSQL clusters and Airflow from committed files, with no manual `kubectl` surgery at any step | ✓ VERIFIED | A full cold `make cluster-rebuild` (destroy + recreate) was executed and independently verified during plan 02-06 (647s, within the ~900s D-04 budget) with `make cluster-verify` passing 21/21 live tests immediately after — proving the JoinConfiguration node-label fix and every prior stage from a genuinely cold start, not a warm/patched cluster. The current live cluster (verified in this session) shows worker node labels `airflow-platform-worker=storage` / `airflow-platform-worker2=analytics` correctly applied — this only happens via the corrected `kind/cluster.yaml` on a full recreation, confirming the committed file (not a live `kubectl label` patch) is what produced the current state. `tests/policy/test_no_manual_kubectl_surgery.py` (56 policy tests incl. this one, run live in this session) passes, structurally proving no script performs imperative `kubectl create/edit/patch`, and every `apply` targets either a committed `kubernetes/` path or stdin-piped generated Secret content (D-14). `make doctor` (fail-closed preflight) passes on this host today. |
| 2 | The Airflow UI is reachable and shows API server, scheduler, DAG processor and triggerer running as separate workloads | ✓ VERIFIED | `curl -o /dev/null -w '%{http_code}' http://airflow.localtest.me/` → `200` (run live this session). `kubectl -n airflow get deploy,sts` shows `airflow-api-server`, `airflow-dag-processor`, `airflow-scheduler` as three separate Deployments (all `1/1 Available`) and `airflow-triggerer` as a StatefulSet (`1/1 Ready`) — four genuinely separate workloads, correctly a StatefulSet for the triggerer (RESEARCH's documented correction to naive "four Deployments" wording). `airflow version` executed inside the running `api-server` container reports `3.3.0`, proving the image override over the chart's declared `appVersion: 3.2.2` took effect. |
| 3 | `psql` reports PostgreSQL 17 on the Airflow metadata cluster and PostgreSQL 18 on the analytical cluster, as two physically separate CloudNativePG `Cluster` resources with no shared storage | ✓ VERIFIED | `kubectl -n data exec airflow-db-1 -- psql -U postgres -tAc "show server_version"` → `17.10` (run live this session); same on `analytics-db-1` → `18.4`. `kubectl get cluster -A` shows two distinct `Cluster` resources (`airflow-db`, `analytics-db`), both `Cluster in healthy state`. `kubectl -n data get pvc` shows disjoint, separately-bound PVCs (`airflow-db-1` 10Gi, `analytics-db-1` 20Gi, distinct volume IDs). `kubectl -n data get pods -o wide` confirms the two primaries are scheduled on different nodes (`airflow-platform-worker` vs `airflow-platform-worker2`), matching D-03's physical-placement intent, not merely nominal separation. `tests/e2e/cluster/test_postgres_topology.py` (part of the 21/21 live e2e pass this session) additionally asserts neither cluster hosts the other's database and neither carries a schema beyond the CNPG/Alembic-reserved built-ins. |
| 4 | MinIO serves `raw`, `validated`, `processed`, `quarantine` and `metadata` over the S3 API, addressable as `s3://bucket/key` | ✓ VERIFIED | `tests/e2e/cluster/test_minio_buckets.py` (5 tests, part of the live 21/21 pass this session, run via `uv run --frozen --group cluster pytest tests/e2e/cluster -q`) exercises all five buckets **through boto3 with an explicit `endpoint_url`**, per D-16's explicit requirement that the S3 claim never be proven with MinIO's own tooling: all five buckets reachable, a byte-identical round trip through `s3://raw/<key>`, versioning `Enabled` on `raw` and not on `validated`, the application credential's `DeleteObject` against `raw` refused with the object still retrievable afterward, and the admin credential's delete succeeding as the positive control. `curl http://minio.localtest.me/minio/health/live` → `200` (run live this session). |
| 5 | `helm template -f values-ci.yaml` renders a stack sized for a 4 CPU / 16 GB runner, and CI fails on an invalid manifest or chart | ✓ VERIFIED | Per the task's own guidance, verified via the repository's actual mechanism (`make manifests` / `scripts/render-manifests.sh`), run live this session: renders both `local` and `ci` profiles for all five pinned charts, validates every document with `kubeconform -strict -kubernetes-version 1.35.5` → `Summary: 157 resources found in 12 files - Valid: 135, Invalid: 0, Errors: 0, Skipped: 22` (the 22 skips are `CustomResourceDefinition` meta-documents with no upstream schema at any k8s version — a documented, narrowly-scoped kubeconform-catalogue gap, not a validation weakening). `make manifest-policy` (run live this session) → `10 passed, 0 skipped`, including `test_ci_profile_fits_runner` (measured ~2.16 CPU-cores / ~3.9 GiB against an effective 3.2 CPU / 12.8 GiB budget — both CNPG `Cluster` CRs counted via a special-cased walker, closing 02-RESEARCH.md Pitfall 6) and `test_every_container_is_sized`. `tests/policy/test_manifest_validation_fails_closed.py` proves the gate rejects a real invalid CNPG `Cluster` CR (`spec.postgresql: null`) and accepts its valid twin — CI is observed failing closed, not merely configured to. |

**Score:** 5/5 verified.

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `kind/cluster.yaml` | 3-node cluster, kubelet reservations, extraMounts/extraPortMappings, containerd registry config | ✓ VERIFIED | Present, 3 node entries, each with a `KubeletConfiguration` patch; live cluster matches (`kubectl get nodes` → 3 Ready nodes, v1.35.5) |
| `helm/values/{local,ci}/*.yaml` (5 components × 2 profiles = 10 files) | Two divergent profiles, three-then-four named divergence axes | ✓ VERIFIED | All 10 present; `tests/policy/test_values_profiles.py` (part of 56-test live run) confirms divergence limited to `resources.*`, `storage.size`, `controller.metrics.enabled`, `executor` |
| `kubernetes/namespaces.yaml` | 5 D-13 namespaces | ✓ VERIFIED | `kubectl get ns` shows all 5 (`cnpg-system`, `data`, `airflow`, `etl`, `ingress-nginx`) Active |
| `scripts/{doctor,cluster-up,cluster-down,cluster-rebuild,minio-credentials,airflow-metadata-secret,render-manifests}.sh` + `scripts/stages/*.sh` | The D-09 bootstrap spine | ✓ VERIFIED | All present; `make doctor` and `make manifests` executed live this session with expected output |
| `tests/e2e/cluster/*.py` (D-16 suite) | Live-cluster proof for every success criterion | ✓ VERIFIED, WIRED, DATA FLOWS | 21 tests, all passing live this session against the real cluster (not mocked) |
| `tests/policy/*.py` (offline gate) | Static/rendered-manifest assertions | ✓ VERIFIED | 105 non-manifests + 10 manifests-marked = 115 policy tests, all green this session |
| `docs/adr/0006-*.md`, `docs/adr/0007-*.md` | Supply-chain and Helm-4 decision records | ✓ VERIFIED (content); ⚠️ pending human sign-off | Both present, complete, well-evidenced; ADR-0006 carries an explicit deferred `<human-check>` for risk T-02-21 that has not yet been confirmed by a human (see Human Verification Required) |

### Key Link Verification

| From | To | Via | Status | Details |
|------|-----|-----|--------|---------|
| `scripts/cluster-up.sh` | `scripts/stages/*.sh` | ordered `helm upgrade --install` calls | WIRED | Live cold rebuild (02-06) exercised the full chain end-to-end |
| CNPG operator | `Cluster` CRs (`airflow-db`, `analytics-db`) | admission webhook readiness wait before CR apply | WIRED | Both clusters `Ready=True` live; webhook-dial-error pitfall documented and avoided |
| `airflow-db-app` Secret (CNPG-generated) | Airflow chart `data.metadataSecretName` | `scripts/airflow-metadata-secret.sh` cross-namespace derivation | WIRED | Migration Job completed, 4 workloads Ready, `airflow version` responds — the adapter is proven end-to-end, not just plausible |
| `helm/versions.env` | Makefile / installers / values files | single source of pins | WIRED | `tests/policy/test_pinned_tool_versions_agree.py` + `test_supply_chain_guards.py` (image-tag agreement) both pass live |
| `build/manifests/` render output | `tests/policy/test_manifest_*.py` | `manifest-policy: manifests` Make prerequisite | WIRED | `make manifest-policy` observed ordering the render before the tests that read it; a mutation-based non-vacuity test (`test_this_module_runs_after_the_render`) exists |

### Requirements Coverage

| Requirement | Description | Status | Evidence |
|---|---|---|---|
| INFRA-01 | Multi-node kind cluster destroyed/recreated reproducibly | ✓ SATISFIED (functionally) | Full cold `make cluster-rebuild` verified in-session lineage (02-06); live cluster confirmed correctly labeled. **REQUIREMENTS.md still shows this item unchecked/"Pending"** — see gap below |
| INFRA-02 | Airflow 3.x, 4 separate workloads | ✓ SATISFIED | Live, verified this session |
| INFRA-03 | PostgreSQL dedicated to Airflow metadata | ✓ SATISFIED | Live, verified this session (PG 17.10) |
| INFRA-04 | Physically separate analytical PostgreSQL | ✓ SATISFIED | Live, verified this session (PG 18.4) |
| INFRA-05 | MinIO with 5 buckets | ✓ SATISFIED | Live, verified this session |
| INFRA-07 | All infrastructure as code, no manual kubectl surgery | ✓ SATISFIED | `test_no_manual_kubectl_surgery.py` passes live |
| INFRA-09 | Kubelet reservations/maxPods/extraMounts at creation time | ✓ SATISFIED (functionally) | `kind/cluster.yaml` is the sole creation-time surface; `test_kind_cluster_config.py` + `test_node_capacity.py` pass live. **REQUIREMENTS.md still shows this item unchecked/"Pending"** — see gap below |
| INFRA-10 | Two Helm values profiles from first infra commit | ✓ SATISFIED | 10 values files present and render cleanly for both profiles |
| CICD-07 | Kubernetes manifests/charts validated in CI | ✓ SATISFIED | `make manifest-policy` green, CI job `manifests` present in `.github/workflows/ci.yml` |

**9/9 requirement IDs from the phase are accounted for and functionally satisfied.** No orphaned requirements — the 9 IDs in REQUIREMENTS.md's Phase 2 mapping exactly match the phase's declared requirement set.

**Bookkeeping gap (non-blocking):** `.planning/REQUIREMENTS.md` marks `INFRA-02, INFRA-03, INFRA-04, INFRA-05, INFRA-07, INFRA-10, CICD-07` as `[x]` / "Complete" but still shows `INFRA-01` and `INFRA-09` as `[ ]` / "Pending" (lines 32, 40, 282, 290), even though both plans 02-01 and 02-02 declared `requirements-completed: [INFRA-01, INFRA-07, INFRA-09]` in their SUMMARY frontmatter, and both are functionally verified above. This is a documentation-accuracy defect (two checkboxes and two traceability-table rows not updated), not a functional gap — no codebase behavior depends on it. Flagged for a trivial follow-up fix, not blocking phase completion.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| — | — | `TBD`/`FIXME`/`XXX`/`TODO`/`HACK`/`PLACEHOLDER` in phase-touched files | ℹ️ NONE | Scanned `kind/`, `helm/`, `kubernetes/`, `scripts/`, `tools/k8s/`, `tests/e2e/`, `tests/policy/` — zero matches; debt-marker gate is clean |
| `kind/cluster.yaml` | 86, 98, 134, 137, 164, 167 | Hardcoded absolute host paths (`/home/konutec/...`) in a file the project's own conventions call universally reproducible, committed infrastructure | ⚠️ WARNING (pre-existing, from 02-REVIEW.md WR-01, still open) | Confirmed still present this session. Works correctly on this developer's actual machine (verified live), but is not portable to a second machine/account without manual editing — Docker silently auto-creates a missing bind-mount source rather than failing loudly, which is the exact "silently does the wrong thing" failure mode this project's Core Value statement forbids. Non-blocking for this phase's goal (which is scoped to *this* reproducible environment, proven working), but should be fixed before treating multi-machine portability as proven. |
| `scripts/airflow-metadata-secret.sh` | 150-158 | Secret YAML built by string interpolation; only `username`/`password` are URL-encoded, `host`/`port`/`dbname` are not | ⚠️ WARNING (pre-existing, from 02-REVIEW.md WR-02, still open) | Confirmed still present this session (only `encoded_user`/`encoded_password` exist). No practical exploit today (values are operator-controlled: a k8s service name, a numeric port, `airflow`), but a defect class worth closing per the review's own reasoning. |
| `scripts/doctor.sh` | ~184-198 | `check_ports` fails open (silently skips) if `ss` is unavailable | ⚠️ WARNING (pre-existing, from 02-REVIEW.md WR-03, still open) | Confirmed still present this session — no `command -v ss` guard found. Contradicts the script's own stated "never warn-and-continue on a blocking check" principle. Non-blocking on this host (`ss` is present and the check runs), but a latent fail-open defect. |
| `scripts/helm-install.sh` | 76-83 | Echoed `helm` command line omits `--timeout` and `--kube-context` | ℹ️ INFO (pre-existing, from 02-REVIEW.md WR-04, still open) | Confirmed still present this session. Cosmetic — does not affect the actual invocation, only what a developer sees printed for manual reproduction. |

All four items above were already identified and classified by `02-REVIEW.md` (0 critical, 4 warning, 2 info, status `issues_found`, explicitly advisory/non-blocking). Re-confirmed still open at verification time; none of them affect this phase's Success Criteria, which were all independently verified live.

### Human Verification Required

### 1. ADR-0006 supply-chain risk acceptance (T-02-21)

**Test:** Read `docs/adr/0006-unmaintained-upstream-artifacts.md` in full.
**Expected:** Confirm you accept depending on all three named unmaintained upstream artifacts (`pgsty/minio:RELEASE.2026-08-04T00-00-00Z`, `registry.k8s.io/ingress-nginx/controller:v1.15.1`, `quay.io/minio/mc:RELEASE.2024-11-21T17-21-54Z`) and that the named migration triggers for each are events you would actually notice. Specifically confirm acceptance of threat **T-02-21** — an unpatched CVE in the archived `ingress-nginx` controller, rated **high** severity, dispositioned **accept** (not mitigate) on the argument that the cluster is local-only and the ingress is published on loopback only. If you would ever bind the ingress to a non-loopback address, say so now — the alternative is a Gateway API migration, which is a phase of work, not a values change.
**Why human:** This is a supply-chain risk acceptance, not a test assertion (02-05-PLAN.md's own `<human-check>` block). The task type is `auto` in the plan (not `checkpoint:human-verify`), meaning it was deliberately deferred to end-of-phase per this project's `workflow.human_verify_mode = end-of-phase` convention rather than paused on during execution — plan 02-05's SUMMARY explicitly states this. No automated check can certify a human's acceptance of a named residual risk; the ADR's content and completeness were verified (Level 1-3, above), but the sign-off itself is outside what a verifier can grant on your behalf.

### Deferred Items

None — no gap identified in this verification maps to a later phase's stated goal or success criteria; the phase's own scope note explicitly excludes Vault (Phase 5), kube-prometheus-stack/Tempo/OTel (Phase 7), and the ephemeral-kind CI E2E job (Phase 11), none of which this verification treated as in-scope gaps.

### Summary

The phase goal is achieved. All five ROADMAP Success Criteria were independently verified against the live, currently-running cluster in this session — not inferred from SUMMARY.md prose — including running the phase's own 21-test live e2e suite, the 115-test offline policy/manifest suite, `make doctor`, and `make manifests`/`make manifest-policy` fresh. `psql` reports the correct PostgreSQL majors on two physically distinct, disjointly-provisioned CloudNativePG clusters; Airflow's four workloads (three Deployments, one StatefulSet — correctly, not naively "four Deployments") are Ready and reachable through the ingress at the correct image version; MinIO's five buckets are proven reachable through boto3 specifically (never `mc`), including the deny-delete enforcement and its admin-credential positive control; and the offline manifest-validation gate is observed both accepting valid rendered output and rejecting a deliberately broken CNPG `Cluster` CR, with the CI-budget sizing test measuring real numbers against the stated 4 CPU / 16 GB constraint rather than asserting it.

Three genuine bugs were found and fixed live during this phase's own execution (the `InitConfiguration`/`JoinConfiguration` node-label defect that silently broke D-03 physical placement since plan 02-01's first commit; the Helm 4 `--wait=watcher` deadlock against the Airflow chart's post-install-hook shape; a Makefile `stage-%` glob collision) — all three are now fixed in committed files and independently re-proven by a full cold `make cluster-rebuild` during plan 02-06, not merely patched live and left unverified from cold.

What keeps this report at `human_needed` rather than `passed` is a single deliberately-deferred human sign-off: ADR-0006's acceptance of running an unpatched, upstream-archived `ingress-nginx` controller (T-02-21, high severity) on this host. This was never meant to be resolved by an automated check — it is recorded in the plan itself as a human decision. Four smaller, already-known, non-blocking code-review warnings (hardcoded host paths in `kind/cluster.yaml`, incomplete URL-encoding in the metadata-secret adapter, a fail-open `ss`-absent port check, and a cosmetically-incomplete echoed `helm` command) remain open from `02-REVIEW.md` and are re-confirmed still present, but none of them affect any Success Criterion and all four are already documented with a proposed fix. One REQUIREMENTS.md bookkeeping gap (`INFRA-01`/`INFRA-09` checkboxes not updated despite being functionally complete) should be swept up but blocks nothing.

---

_Verified: 2026-08-12T21:40:00Z_
_Verifier: Claude (gsd-verifier)_
