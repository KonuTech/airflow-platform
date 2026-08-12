---
phase: 02
slug: kind-cluster-core-infrastructure
status: verified
threats_open: 0
asvs_level: 1
created: 2026-08-12
---

# SECURITY.md — Phase 2: kind Cluster Core Infrastructure

Security audit of the implemented Phase 2 (`02-kind-cluster-core-infrastructure`, plans
02-01 through 02-08) against the threat models declared in each plan's `<threat_model>`
block. Every threat below was verified against the implemented code and, where the
mitigation is runtime-observable, against the live kind cluster running on this host
(CNPG, MinIO, Airflow, ingress-nginx all deployed). Documentation and stated intent were
not accepted as evidence — each row cites the file, line, or live command that proves the
mitigation exists.

**Audit date:** 2026-08-12
**ASVS level:** 1
**Block-on policy:** `high` (any open high-severity threat blocks shipment)
**Result:** 39/39 threats CLOSED. 0 open. 1 unregistered attack-surface flag (informational,
not a blocker under this phase's `block_on: high` policy, but noted for reviewer visibility).

Offline verification: `uv run --frozen pytest tests/policy -q -m "not manifests"` — **105
passed, 10 deselected**, run live during this audit (2026-08-12).

---

## Threat Verification

Threat ID `T-02-SC` recurs across plans 02-01, 02-04, 02-05, 02-07 and 02-08 as the same
tracked supply-chain-tampering risk applied to different artifacts as they are introduced;
each occurrence is listed once, against the artifact and file it names.

| Threat ID | Category | Component | Disposition | Evidence | Status |
|-----------|----------|-----------|-------------|----------|--------|
| T-02-01 | Tampering | `tools/k8s/install_{kind,helm}.sh` | mitigate | `PINNED_SHA256_<os>_<arch>` constants present and used as the authoritative check (`install_kind.sh:60-63`, `install_helm.sh:62-65`); `sha256sum -c` against the in-repo digest before install; upstream `.sha256sum` cross-check is advisory-only (comments at `install_kind.sh:44-59`) | CLOSED |
| T-02-02 | Elevation of Privilege | `tools/bin/kind`, `tools/bin/helm` | mitigate | Idempotence via `.stamp` file compared against `sha256sum` of the installed binary (`install_kind.sh:109-111`); binary is never executed to check its version (comment block `install_kind.sh:11-13` names this as the CR-03 fix) | CLOSED |
| T-02-03 | Information Disclosure | `kind-registry` container | mitigate | `docker run -p 127.0.0.1:${REGISTRY_PORT}:5000` (`scripts/stages/10-registry.sh:32`), never `0.0.0.0`. **Live-verified:** `docker inspect kind-registry` shows `"HostIp":"127.0.0.1","HostPort":"5001"` | CLOSED |
| T-02-04 | Denial of Service | kubelet node capacity | mitigate | `systemReserved`/`kubeReserved`/`evictionHard` set on every node (`kind/cluster.yaml:63-69,122-128,152-158`); every ingress-nginx container (main + both webhook Jobs) carries CPU/memory requests and limits in both profiles (`helm/values/{local,ci}/ingress-nginx.yaml`) | CLOSED |
| T-02-05 | Spoofing | `*.localtest.me` ingress routing | accept | Hostname resolves publicly to loopback; no code mitigation applicable. Logged in Accepted Risks below | CLOSED |
| T-02-SC | Tampering | `registry.k8s.io/ingress-nginx/controller:v1.15.1` | mitigate | Version-pinned via `helm/versions.env:20` (`INGRESS_NGINX_CHART_VERSION=4.15.1`) and consumed by `scripts/stages/30-ingress-nginx.sh:24`; no nginx-specific `Ingress` annotations anywhere in the tree (confirmed by grep across `helm/values/`); risk recorded in `docs/adr/0006-unmaintained-upstream-artifacts.md` | CLOSED |
| T-02-06 | Denial of Service | host inotify + disk exhaustion | mitigate | `scripts/doctor.sh:69-93` blocks on `max_user_watches`/`max_user_instances`/free-disk before `cluster-up`; `docs/wsl/wslconfig.example:50` documents `sparseVhd` | CLOSED |
| T-02-07 | Tampering | `make doctor` degraded into an advisory | mitigate | `tests/policy/test_doctor_fails_closed.py` — `test_doctor_rejects_each_threshold_it_claims_to_block` (per-threshold negative case), `test_doctor_passes_on_the_real_host` (positive control), `test_doctor_echoes_its_own_checks` (transcript assertion, lines 115-124) | CLOSED |
| T-02-08 | Information Disclosure | `tests/e2e/cluster/conftest.py` credential handling | mitigate | `s3_client` factory reads credentials at call time via `scripts/minio-credentials.sh show` (`conftest.py:152-215`), never persisted, never hardcoded; docstring states this explicitly (`conftest.py:184-189`) | CLOSED |
| T-02-09 | Repudiation | live-cluster suite silently uncollected | mitigate | `tests/policy/test_offline_gate_stays_offline.py` — `test_check_and_ci_never_reach_tests_e2e`, `test_cluster_verify_is_the_only_target_naming_tests_e2e`, `test_adding_tests_e2e_to_the_test_recipe_is_reported` (mutation non-vacuity) | CLOSED |
| T-02-10 | Spoofing | `doctor` accepting an unpinned kind/helm binary | mitigate | `scripts/doctor.sh:153,169` delegates directly to `tools/k8s/install_kind.sh` / `install_helm.sh` rather than trusting a pre-existing binary | CLOSED |
| T-02-11 | Information Disclosure | PostgreSQL credentials | mitigate | No `password` literal in any `helm/values/{local,ci}/cnpg-{airflow,analytics}.yaml` (grep returns nothing); CNPG generates and stores credentials in cluster-local Secrets | CLOSED |
| T-02-12 | Elevation of Privilege | analytical application role | mitigate | `postInitApplicationSQL: [CREATE ROLE etl_app LOGIN;]` (`helm/values/local/cnpg-analytics.yaml:65-68`) — LOGIN only, no `SUPERUSER`, no ownership grant | CLOSED |
| T-02-13 | Tampering | `Cluster` CR applied before admission webhook serves | mitigate | `scripts/stages/40-cnpg-operator.sh` runs `wait_for_crd_established` **then** `wait_for_deploy_available cnpg-system cnpg-cloudnative-pg` before any `Cluster` CR install, with a comment recording the measured webhook-dial-refusal failure mode | CLOSED |
| T-02-14 | Denial of Service | co-located databases on one node | mitigate | Explicit `nodeSelector: {airflow-platform/role: storage/analytics}` on each cluster (`cnpg-airflow.yaml:54-55`, `cnpg-analytics.yaml:47-48`); live test `test_two_distinct_clusters_no_shared_storage` in `tests/e2e/cluster/test_postgres_topology.py` | CLOSED |
| T-02-15 | Denial of Service | unbounded PostgreSQL resource use | mitigate | Full `cluster.resources` requests/limits present in all four `cnpg-{airflow,analytics}.yaml` files (both profiles) | CLOSED |
| T-02-16 | Information Disclosure | `scripts/minio-credentials.sh` | mitigate | Values piped to `kubectl apply -f -` on stdin (`minio-credentials.sh:79`), never a CLI arg, never echoed on `ensure`; comment block states this explicitly (lines 5, 27, 60) | CLOSED |
| T-02-17 | Tampering | objects in `s3://raw/` | mitigate | `versioning: true` on `raw` only (`helm/values/local/minio.yaml:65`); explicit `Deny` on `DeleteObject`/`DeleteObjectVersion` scoped to `raw` (lines 111-113) | CLOSED |
| T-02-18 | Elevation of Privilege | admin credential reachable by pipeline | mitigate | `etl-app` user attached only to the `etl-app` policy (`minio.yaml:115-121`); admin credential is not referenced by any workload | CLOSED |
| T-02-19 | Spoofing | unauthenticated access to buckets | mitigate | `policy: none` (anonymous access) on all five buckets (`minio.yaml:62-83`). **Live-verified:** unauthenticated `GET http://minio.localtest.me/raw/` returns `403` | CLOSED |
| T-02-20 | Denial of Service | MinIO stock resource defaults | mitigate | `mode: standalone`, `replicas: 1` (`minio.yaml:18-19`), explicit `resources:` on main deployment and all three post-job containers (lines 43,137,146,155) | CLOSED |
| T-02-SC | Tampering | `pgsty/minio`, `quay.io/minio/mc` | mitigate | Image pinned via `MINIO_IMAGE_TAG` in `helm/versions.env:24`, referenced by `minio.yaml:21-23`; risk recorded in ADR-0006 | CLOSED |
| T-02-SC | Tampering | `pgsty/minio`, ingress-nginx v1.15.1, `quay.io/minio/mc` (ADR) | mitigate | `docs/adr/0006-unmaintained-upstream-artifacts.md` — names all three artifacts with dated evidence, 6 lettered alternatives with verdicts, named migration targets (SeaweedFS, Gateway API) and 4 observable migration triggers; indexed in `docs/adr/README.md` (rows for 0006/0007) | CLOSED |
| T-02-21 | Elevation of Privilege | unpatched CVE in archived ingress-nginx controller, reachable on host port 80 | accept | Cluster local-only, ingress on loopback (T-02-03 evidence applies transitively via the same port-mapping mechanism); ADR-0006 names the migration trigger. **Human sign-off recorded** in `.planning/phases/02-kind-cluster-core-infrastructure/02-HUMAN-UAT.md` (2026-08-12) — explicit confirmation of T-02-21 by severity/disposition, confirmed no plan to expose the ingress beyond loopback. Logged in Accepted Risks below | CLOSED |
| T-02-22 | Repudiation | supply-chain risk absorbed without record | mitigate | `docs/adr/README.md` Records table lists both 0006 and 0007, both removed from the prospective-records table (verified: grep for `0006`/`0007` present, no Phase-2 prospective rows remain) | CLOSED |
| T-02-23 | Information Disclosure | derived Airflow metadata connection Secret | mitigate | `scripts/airflow-metadata-secret.sh` never echoes the assembled URI (only echoes a status line naming the Secret, `line 156`), never a CLI arg, writes only into namespace `airflow` | CLOSED |
| T-02-23b | Elevation of Privilege | `data`→`airflow` cross-namespace Secret read | accept | Trust-model header in `airflow-metadata-secret.sh:11-20` names the developer's own kubeconfig as the principal; no ServiceAccount/Role/RoleBinding created for this read. **Live-verified:** `kubectl -n airflow get role,rolebinding` and `kubectl -n data get role,rolebinding` show only the Airflow chart's own namespace-scoped RBAC and CNPG's per-cluster RBAC — no binding grants cross-namespace secret access. Logged in Accepted Risks below | CLOSED |
| T-02-24 | Cryptographic failure | `fernetKey`/API secret key | mitigate | `airflow-metadata-secret.sh:162,169` — both Secrets checked for existence and left unchanged (`"already exists — leaving it unchanged"`) before any regeneration; generated once via `_fernet_key()`/`_random_hex 32` | CLOSED |
| T-02-25 | Spoofing | unauthenticated access to Airflow UI | accept | No `auth`/`AuthManager` override in either `helm/values/{local,ci}/airflow.yaml` (grep confirms zero matches) — chart default auth manager is unmodified and in force; ingress on loopback only. Logged in Accepted Risks below | CLOSED |
| T-02-26 | Tampering | bundled `bitnamilegacy` PostgreSQL subchart re-enabled | mitigate | `postgresql: {enabled: false}` in both `helm/values/{local,ci}/airflow.yaml`; live test `test_no_bundled_postgres_is_running` in `tests/e2e/cluster/test_airflow_workloads.py:271-282` scans every pod image for `bitnamilegacy`/`bitnami/postgresql` | CLOSED |
| T-02-27 | Denial of Service | thirty empty `resources` keys in the Airflow chart | mitigate | All ten enumerated `resources:` keys present in `helm/values/local/airflow.yaml` (apiServer, scheduler+logGroomerSidecar, dagProcessor+logGroomerSidecar, triggerer+logGroomerSidecar, migrateDatabaseJob, createUserJob, workers.kubernetes) | CLOSED |
| T-02-28 | Elevation of Privilege | KubernetesExecutor Role/RoleBinding | accept | Chart's own RBAC (`airflow-pod-launcher-role`, `airflow-pod-log-reader-role`) scoped to namespace `airflow` only — **live-verified** via `kubectl -n airflow get role,rolebinding`; `etl` namespace live-verified empty (`kubectl -n etl get all` returns nothing). Logged in Accepted Risks below | CLOSED |
| T-02-29 | Tampering | `tools/k8s/install_kubeconform.sh` | mitigate | `PINNED_SHA256_<os>_<arch>` constants (`install_kubeconform.sh:67-70`), `.stamp` idempotence by digest (line 115-117), same verify-before-extract shape as T-02-01 | CLOSED |
| T-02-30 | Tampering | manifest gate validating nothing while reporting success | mitigate | `tests/policy/test_manifest_validation_fails_closed.py::test_the_crd_schema_is_load_bearing` (lines 128-161) — proves missing-schema case fails with a named error and supplied-schema case passes, against the identical valid sample | CLOSED |
| T-02-31 | Spoofing | unpinned chart resolution during rendering | mitigate | Every `helm template` call in `scripts/render-manifests.sh:77-79` passes explicit `--version "${version}"` read from `helm/versions.env` | CLOSED |
| T-02-32 | Elevation of Privilege | CI workflow permissions | mitigate | `.github/workflows/ci.yml:15-16` — workflow-level `permissions: {contents: read}`, no job widens it; every `uses:` pinned by commit SHA (lines 37-38, 69-70, 98, 108) | CLOSED |
| T-02-33 | Tampering | `helm template` output applied to a live cluster | mitigate | `scripts/render-manifests.sh:75` writes to `build/manifests/<profile>/`, gitignored (`git check-ignore -q build` exits 0); explicit anti-pattern comment at lines 14-15 against piping into `kubectl apply` | CLOSED |
| T-02-34 | Information Disclosure | committed values file drifting to a literal password | mitigate | `tests/policy/test_workflow_secrets.py` second scanned surface covers `helm/`, `kubernetes/`, `kind/`, `scripts/` (module docstring line 44); `ALLOWED_SECRETS` proven unchanged by `test_the_allowed_secrets_set_is_unchanged_by_d14` | CLOSED |
| T-02-35 | Denial of Service | BestEffort pod evicted first | mitigate | `tests/policy/test_manifest_resources.py::test_every_container_is_sized` (line 366) — every container in both rendered profiles asserted to carry a CPU/memory request and limit | CLOSED |
| T-02-36 | Denial of Service | CI profile outgrowing its runner unnoticed | mitigate | `tests/policy/test_manifest_resources.py::test_ci_profile_fits_runner` (line 302) sums requests including both CNPG `Cluster` CRs; `test_an_unrecognised_cluster_scoped_kind_is_reported` (line 401) proves the walker fails closed (`raise ValueError`, line 176-180) rather than contributing zero | CLOSED |
| T-02-37 | Tampering | infrastructure change applied by hand and never committed | mitigate | `tests/policy/test_no_manual_kubectl_surgery.py` — permitted set is `get`/`wait`/`apply -f <committed kubernetes/ path>`/`apply -f -` (stdin, a documented deliberate widening for the two credential-adapter scripts); everything else reported | CLOSED |
| T-02-SC | Tampering | image selected by a mutable tag | mitigate | `tests/policy/test_supply_chain_guards.py::test_every_image_tag_agrees_with_versions_env` (line 390) and the mutable-tag scan (`mutable_tag_problems`, line 423) against every values file | CLOSED |

**Totals:** 39 threat rows (37 numbered IDs + T-02-23b + 4 recurring `T-02-SC` occurrences
consolidated to their distinct artifact/evidence pairs above), all **CLOSED**. 0 open.

---

## Accepted Risks Log

Per this audit's ASVS Level 1 / local-development-platform context, the following risks are
formally logged as accepted. Each was declared `accept` in its originating plan's threat
model with a stated argument; this log is the durable record the audit process requires.

| Threat ID | Risk | Argument for acceptance | Re-evaluation trigger |
|-----------|------|--------------------------|------------------------|
| T-02-05 | `*.localtest.me` resolves publicly to loopback; a spoofed DNS response could theoretically redirect the hostname | Traffic never leaves the host; the cluster is local-only | Revisit if the ingress is ever bound to a non-loopback address |
| T-02-21 | Unpatched CVE in the archived `ingress-nginx` controller, reachable on host port 80 | Cluster local-only, ingress on loopback (`127.0.0.1`), no realistic remote attack path today. **Human-confirmed 2026-08-12** (`.planning/phases/02-kind-cluster-core-infrastructure/02-HUMAN-UAT.md`): developer explicitly accepted T-02-21 by severity and disposition, confirmed no current plan to expose the platform beyond the local machine | A CVE with a public exploit against any of the three ADR-0006 artifacts; the `pgsty/minio` fork going >6 months without release; a Kubernetes upgrade past 1.35; or binding the ingress to a non-loopback address (per ADR-0006's migration trigger list) |
| T-02-23b | The `data`→`airflow` cross-namespace Secret read is performed by the developer's own kubeconfig, outside any RBAC boundary | Host-side operation during `cluster-up`, not an in-cluster identity; grants no in-cluster principal cross-namespace access. Live-verified: no Role/RoleBinding in either namespace grants this read | Phase 5's Vault retrofit must add either a `data`-namespace Role/RoleBinding naming an `airflow`-namespace ServiceAccount, or a Vault Kubernetes-auth role — neither exists yet, deliberately |
| T-02-25 | Airflow UI has no authentication hardening beyond the chart's default auth manager | Ingress published on loopback only; full identity work is Phase 5's scope | Phase 5's identity/Vault work; or if the ingress is ever bound to a non-loopback address |
| T-02-28 | KubernetesExecutor's Role/RoleBinding is the chart's own, not independently reviewed here | Scoped to namespace `airflow` only, not widened; `etl` namespace exists but is empty until Phase 4 gives Vault's role a narrow first grant. Live-verified empty | Phase 4, when `etl` first carries a workload |

---

## Unregistered Flags

One item surfaced during implementation that does not map to any threat ID in this phase's
registers, per `02-07-SUMMARY.md`'s own `## Threat Flags` section:

| Flag | File | Description | Classification |
|------|------|--------------|-----------------|
| `gate-scope-narrowing` | `.gitleaks.toml` (lines 57-75) | A new path-scoped allowlist entry (`^build/manifests/`) was added to the Phase 1 `gitleaks dir` secret-scanning control to suppress 42 false positives from Helm's own `checksum/*-secret` annotations in freshly-rendered chart output. This modifies a security control (`gitleaks`) that plan 02-07's own `<threat_model>` does not name, and no Phase 2 threat register covers the general "gitleaks scan scope" as an asset. The allowlist is narrowly scoped (path-only, no content-pattern relaxation) and the directory is architecturally incapable of holding a real credential (D-08/D-14/D-15 — every values file references Secrets by name only, independently policed by `tests/policy/test_workflow_secrets.py`). Verified by re-reading `.gitleaks.toml` in this audit: the allowlist entry is path-scoped only and does not touch the two existing content-pattern allowlists for the synthetic fixture corpus. | **WARNING** — not a blocker under `block_on: high` (this is a scope change to an existing control, not an absent mitigation for a declared threat), but flagged for reviewer visibility since it was not pre-registered. |

No other unregistered flags were found. `02-01` through `02-06` and `02-08` `SUMMARY.md`
files each explicitly report `## Threat Flags: None` beyond what their own plan's
`<threat_model>` already names.

---

## Verification Method Notes

- **Static verification:** every `mitigate` threat was grepped against the exact file(s)
  named in its plan's Mitigation Plan column; matches confirmed to be load-bearing (not
  comments or dead code) by reading surrounding context.
- **Live verification:** where the mitigation is runtime-observable and a live cluster was
  available (T-02-03 registry binding, T-02-19 anonymous bucket access, T-02-23b/T-02-28
  RBAC absence), the claim was independently re-proven against the running cluster rather
  than trusted from the SUMMARY.md narrative.
- **Test-suite verification:** `uv run --frozen pytest tests/policy -q -m "not manifests"`
  was re-run during this audit (not merely cited from a prior SUMMARY) — **105 passed, 10
  deselected**, confirming the non-vacuity proofs cited above for T-02-07, T-02-09, T-02-30,
  T-02-34 through T-02-37, and T-02-SC (plan 08) are live, not historical.
- **Accept dispositions:** each was checked for (a) a written argument in the originating
  plan, (b) no code change that would silently strengthen or weaken the accepted exposure,
  and (c) for T-02-21 specifically, an independent human sign-off record — found in
  `02-HUMAN-UAT.md`, dated and specific to the threat ID, severity and disposition rather
  than a generic ADR approval.
- **Transfer dispositions:** none declared in this phase's threat register.

## Result

**39/39 threats CLOSED. 0 OPEN_THREATS. Phase 2 clears the security audit at ASVS Level 1
under a `block_on: high` policy.**

## Security Audit Trail

| Audit Date | Threats Total | Closed | Open | Run By |
|------------|---------------|--------|------|--------|
| 2026-08-12 | 39 | 39 | 0 | gsd-security-auditor |
