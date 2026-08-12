# Phase 2: kind Cluster & Core Infrastructure - Research

**Researched:** 2026-08-12
**Domain:** Local multi-node Kubernetes (kind) platform bootstrap — Helm 4, CloudNativePG, MinIO, Airflow 3.3, ingress, offline manifest validation
**Confidence:** HIGH

> **How this document was produced.** Every chart claim below was read from the **pinned chart tarball**, and every runtime claim was **executed against a live kind v0.32.0 cluster on `kindest/node:v1.35.5`** created for this research and destroyed afterwards. Where a claim is reasoning rather than measurement it is tagged `[ASSUMED]` and listed in the Assumptions Log. The three questions the ROADMAP flagged (Helm 4 vs. Helm-3 charts; chart values read off pinned values not docs; whether `run-airflow-migrations` succeeds at image 3.3.0 on chart 1.22.0) are all **answered by execution, not by argument**.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **D-01:** Cluster state is **disposable**. `make cluster-rebuild` is a first-class, scripted, timed operation (recreate cluster → charts → seed → ready), not an emergency measure. README §90 demands rebuildability anyway, so this converts PITFALLS A4 from a risk into a deliverable. `extraMounts` are still **declared in `kind/cluster.yaml` at creation** but left unbound, so adopting persistence later is a values change rather than a cluster rebuild. — **Reversibility:** one-way — reversing the *mount declaration* (not the policy) requires `kind delete cluster`, which destroys exactly the state persistence was meant to protect.
- **D-02:** The `airflow/dags/` hostPath `extraMount` (WSL ext4, **never `/mnt/c`**) is **declared on every node now, wired in Phase 4**. Both values profiles stay on the chart default in this phase. Rationale: the expensive half (cluster recreation) is paid now; the cheap half (a values change) waits until there is a DAG to mount. `values-ci.yaml` stays on the baked-image path permanently, so CI proves the production mechanism. — **Reversibility:** one-way — same reason as D-01; the mount must exist at creation time.
- **D-03:** Stateful placement **splits the two databases across the workers**: worker-1 = Airflow metadata PG 17 + MinIO; worker-2 = analytical PG 18, alone. Explicit `nodeSelector` on each — never left to the scheduler, because local-path PVs are node-bound and a rescheduled stateful pod sits `Pending` with `volume node affinity conflict`, which reads as a scheduler bug. This makes §4's "separation stays visible even inside one cluster" physical rather than nominal, and keeps the heaviest workload (COPY into PG 18) off the node serving object storage. — **Reversibility:** costly — changing placement after data exists means re-provisioning PVCs; node labels themselves are cheap.
- **D-04:** `make cluster-rebuild` **times each stage** (cluster create → CNPG operator → Cluster CRs → MinIO → ingress → Airflow → ready), prints the breakdown, and writes the last run to a gitignored file. Documented budget ~15 minutes, **warn past it, do not fail**: wall-clock on a cold image cache measures the network rather than the repository, and a flaky gate is one people learn to ignore. Per-stage numbers make a regression attributable.
- **D-05:** **ingress-nginx behind `extraPortMappings`.** Host `:80`/`:443` map to a node labelled `ingress-ready`; routing is by hostname under `*.localtest.me` (resolves to 127.0.0.1 with no `/etc/hosts` edit on WSL *or* Windows). Stable URLs that survive every rebuild, one mechanism for every service added through Phase 7. — **Reversibility:** one-way — `extraPortMappings` is a creation-time-only field in `kind/cluster.yaml`; adding a port later costs a cluster rebuild.
- **D-06:** **`values-ci.yaml` keeps the same ingress.** Single-node kind labels its one node `ingress-ready`, so the same manifests apply. The two profiles diverge on **exactly three axes: replica counts, resource sizing, monitoring disabled.** Every additional divergence axis is a bug class that appears only in CI, nine phases downstream of where it was introduced.
- **D-07:** MinIO is addressed through **one `S3_ENDPOINT_URL` injected per context** — in-cluster pods get the ClusterIP service DNS, host-side tools and tests get the ingress host. No address is hardcoded in Python, DAGs, manifests or values. Unsetting the variable resolves real AWS, which is what makes §5's swap-out claim true. Rejected: CoreDNS rewriting so one URL works everywhere (a kind-specific customization that exists nowhere in production).
- **D-08:** `s3://raw/` gets **versioning at bucket creation plus an IAM deny-delete policy** scoped to the application credential, with the admin credential retaining delete. §63 immutability moves from convention to the storage layer while `cluster-rebuild` and fixture reseeding stay ordinary operations. Object-lock WORM retention was considered and rejected: retained test objects cannot be cleaned up in place. The two-credential split is deliberately the same shape Phase 5's Vault policies will need. — **Reversibility:** one-way for the *option* — object lock can only be enabled at bucket creation, so choosing versioning-only now forecloses WORM without recreating the bucket. The policy itself is reversible.
- **D-09:** **Make + shell driving the `helm` CLI.** One target per component, ordered by Make prerequisites, each `helm upgrade --install --wait` against a committed values file. Preserves Phase 1's standing fact — `make` is the only gate definition and CI calls `make` and nothing else — for infrastructure too. Rejected: Helmfile (a sixth pinned tool and a second place gates are defined) and Argo CD (bootstrap chicken-and-egg against a local-only repo). Hand-written readiness waits are expected where `--wait` is insufficient: CRD establishment before applying `Cluster` CRs, and CNPG primary election before Airflow's migration Job.
- **D-10:** **`make doctor` is fail-closed and `cluster-up` depends on it.** Blocks on: inotify `max_user_watches`/`max_user_instances` below target, free ext4 disk under budget, Docker not running, and kind/helm/kubectl off their pinned versions — each printing the exact remediation command. Advisory only where it genuinely cannot verify. Rationale: Phase 1's discovery was that four gates passed on broken input; a check that reports without blocking is a check people scroll past during a 10-minute build.
- **D-11:** **`docs/wsl/wslconfig.example` is committed** (memory / processors / swap, plus `sparseVhd=true` — WSL2's `ext4.vhdx` never returns deleted space to Windows, and disk is the binding constraint, not RAM). `doctor` asserts a **floor**, not exact equality, so a larger machine is never punished. Applying it stays a deliberate human act — it needs `wsl --shutdown` and lives on the Windows side.
- **D-12:** Phase 2's CI job is **offline: `helm template` both profiles → `kubeconform -strict` against Kubernetes 1.35.5 (with `-schema-location` entries for the CNPG and ingress CRDs) → two policy tests.** Test 1 sums container requests across the rendered CI manifests and fails if the total exceeds the 4 CPU / 16 GB runner budget, making success criterion 5's "sized for" claim mechanically true. Test 2 fails any container missing requests/limits — an unrequested pod is QoS `BestEffort` and is evicted first, i.e. precisely when its data is needed to explain the incident. No cluster in CI until Phase 11.
- **D-13:** **Per-component namespaces:** `cnpg-system` (operator), `data` (both CNPG clusters + MinIO), `airflow` (chart components), `etl` (task pods and KPO pods), `ingress-nginx`. Chosen for the Phase 5 identity seam: Vault policies bind to `system:serviceaccount:<namespace>:<name>`, and PITFALLS #13 warns that the usual fix for a mismatch is to widen the Vault role, silently voiding least privilege. `etl` existing now, as the only place ETL runs, lets that role be written narrowly the first time. Also makes NetworkPolicy a real boundary later rather than a relabelling exercise. — **Reversibility:** costly — namespaces are cheap to create but moving a workload later invalidates every RBAC binding, service DNS name and (from Phase 5) Vault role that names it.
- **D-14:** MinIO's root and application credentials are **generated during `cluster-up` and live only in the cluster**. Values files reference secret *names* only; nothing is written to the working tree. Host tooling uses `make minio-creds` to read them back, so `mc` and tests never hold a stale copy. Credentials changing on every rebuild is a feature — nothing can quietly depend on a specific value. This is deliberately the exact shape Phase 5 replaces: same secret names, different source. CNPG generates and stores its own Postgres credentials, so both databases need no handling here. Rejected: a gitignored `.secrets/` file (a real credential at rest in the working tree, protected only by an ignore rule that a `git add -f` or a Docker build context can defeat, with gitleaks live since Phase 1).
- **D-15:** The CNPG `Cluster` CRs create **databases, an owner role and a least-privileged application role with grants — and nothing else.** Every schema and every object is Alembic's (Phase 3), keeping Phase 3's "`alembic upgrade head` against a throwaway PostgreSQL" criterion literally true and giving DDL exactly one home. A schema that exists in the cluster but in no migration is how the CI environment and the local one begin to disagree.
- **D-16:** The phase is proven by **`make cluster-verify`** — a `tests/e2e/cluster/` pytest suite run against the live cluster asserting each success criterion: both server versions and that they are two distinct CNPG `Cluster` resources with no shared storage; all five buckets reachable as `s3://bucket/key` **through boto3** (not `mc` — exercising the client §5 actually mandates); the four Airflow workloads Ready as separate deployments; and the deny-delete policy refusing a delete against `raw`. Re-runnable after any rebuild, and it becomes the regression net for every later phase that edits a values file.

### Claude's Discretion

The user made no "you decide" calls. Everything below is planner/researcher latitude within the decisions above:

- Exact kubelet reservation numbers and `maxPods`. PITFALLS proposes `systemReserved`/`kubeReserved` of 500m/1Gi each, `evictionHard` at `memory.available: 500Mi` / `nodefs.available: 10%`, `maxPods: 60` — and flags these as **proposals, not measurements**. Size them against the documented `.wslconfig` floor from D-11.
- Per-component resource requests/limits, subject to D-12's two policy tests.
- Local registry container shape and lifecycle (it is a plain Docker container and therefore survives `kind delete cluster` on its own — the image cache does not need protecting by D-01).
- Ingress hostname scheme beyond the `*.localtest.me` convention.
- Whether `cluster-down` also prunes node containerd image stores (PITFALLS A3 notes that pruning the host daemon does not touch images already loaded into nodes, and this is the step everyone forgets).
- Plan decomposition. The roadmap's internal parallelism is 2a MinIO ‖ 2b analytical PG ‖ 2c Airflow PG → 2d Airflow (needs 2c only).

### Deferred Ideas (OUT OF SCOPE)

- **Persisting cluster state via bound `extraMounts`** — the mounts are declared (D-01) but unbound. If rebuild time or accumulated state ever justifies it, adopting persistence is a values change. Revisit if `cluster-rebuild` drifts past its budget.
- **Object-lock WORM retention on `raw`** — rejected for Phase 2 (D-08) because retained test objects cannot be cleaned up in place. Genuinely worth revisiting at Phase 11, where INCR-07's rebuild-from-raw makes raw's integrity the capstone proof and the environment is ephemeral anyway.
- **NetworkPolicy between namespaces** — D-13 makes the namespace boundary real enough to enforce later; no policies are written in this phase. Natural companion to Phase 5's identity work.
- **CoreDNS rewrite so one S3 URL resolves identically inside and outside the cluster** — rejected in D-07 as a kind-specific customization with no production counterpart. Reconsider only if context-dependent endpoints actually cause a debugging problem.
- **Server-side `kubectl apply --dry-run=server` validation** — deferred to Phase 11 with the ephemeral-kind job (D-12). It catches CRD and admission-webhook errors no offline tool can.
- **`make clean-images` pruning each node's containerd store** — PITFALLS A3 flags this as the cleanup step everyone forgets and the reason disk keeps climbing after a "cleanup". Listed under Claude's discretion for this phase; if not built here it should become a ledger entry.

</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| INFRA-01 | Multi-node kind cluster (control-plane + 2 workers) destroyed and recreated reproducibly from a committed `kind/cluster.yaml` | §A verifies the exact `cluster.yaml` shape end-to-end on kind v0.32.0 / node v1.35.5: `kubeadmConfigPatches` (both `InitConfiguration` and `KubeletConfiguration`), `extraPortMappings`, `extraMounts`, `containerdConfigPatches`. Measured create-to-Ready: 17 s with a warm image cache |
| INFRA-02 | Airflow 3.x with API server, scheduler, DAG processor and triggerer as separate workloads | §B: chart 1.22.0 + image 3.3.0 renders and **runs** four separate workloads — three Deployments plus a **StatefulSet** for the triggerer. `airflow version` in the running api-server reports `3.3.0` |
| INFRA-03 | PostgreSQL dedicated exclusively to Airflow metadata | §C: CNPG `cluster` chart 0.8.1 with `version.postgresql: "17"`; verified `show server_version` → `17.10`, 71 Airflow tables, alembic head `d2f4e1b3c5a7` |
| INFRA-04 | Physically separate PostgreSQL for analytical workloads | §C: second `Cluster` CR, `version.postgresql: "18"` → verified `PostgreSQL 18.4`, separate namespace/PVC/node via `cluster.affinity.nodeSelector` |
| INFRA-05 | MinIO S3-compatible storage with `raw`, `validated`, `processed`, `quarantine`, `metadata` | §D: chart 5.4.0 `buckets:` list creates all five; `versioning: true` on `raw`; `policies[].statements[].effect: Deny` renders a real IAM deny statement; `users[].existingSecret` keeps credentials out of values |
| INFRA-07 | All infrastructure as code, no manual `kubectl` surgery | §E: `make` targets over `helm upgrade --install`, with the two readiness waits that Helm cannot supply (webhook readiness, `Cluster` Ready condition) named precisely |
| INFRA-09 | Kubelet reservations, `maxPods` and `extraMounts` set at cluster-creation time | §A.3 — measured allocatable arithmetic proves the PITFALLS-proposed numbers are an order of magnitude too small for their stated purpose; §A.3 gives the corrected sizing method |
| INFRA-10 | Two Helm values profiles exist from the first infrastructure commit | §F: the three divergence axes (D-06) mapped to concrete chart keys per component; measured footprint of the whole stack gives the CI budget a real basis |
| CICD-07 | Kubernetes manifests and Helm charts validated in CI | §G: `kubeconform 0.8.0` fails closed on the CNPG `Cluster` CR without a CRD schema; offline vendored schema pipeline verified working; the two D-12 policy tests prototyped against real rendered output |

</phase_requirements>

## Summary

Every version pin in `STACK.md` survives contact with the actual artifacts, and the one MEDIUM-confidence call — Helm 4.2.3 driving Helm-3-era charts — is now HIGH: **Helm 4.2.3 rendered and installed all five charts cleanly** (Airflow 1.22.0, MinIO 5.4.0, CloudNativePG operator 0.29.0, CloudNativePG `cluster` 0.8.1, ingress-nginx 4.15.1) on a live kind cluster. The fallback to Helm 3.21.3 has **no trigger left in this phase** and should be recorded as "not needed, re-evaluate only if a future chart fails to render". What Helm 4 *does* change is the CLI contract: `--atomic` is **gone**, `--wait` is now a *strategy* whose default when omitted is `hookOnly` (it does **not** wait for workloads), server-side apply is on by default, and `--force` became `--force-replace`. D-09's "`helm upgrade --install --wait`" must therefore be written as `--wait=watcher` (or `legacy`) deliberately, not inherited from Helm-3 muscle memory.

The three highest-risk unknowns resolved in the platform's favour. `run-airflow-migrations` **succeeds** with image tag `3.3.0` against chart `appVersion: 3.2.2` — the full 71-table schema migrated onto a CNPG PostgreSQL 17.10 cluster and all four Airflow 3 workloads reached Ready in 48 seconds. The MinIO chart's frozen 5.4.0 values express D-08 exactly: `buckets[].versioning`, and a `policies[]` entry whose statement carries `effect: Deny`, attached to an application user whose secret key never appears in a values file. And the CNPG `cluster` chart's PG-16 default is a one-key override (`version.postgresql`) that flows straight into `imageName: ghcr.io/cloudnative-pg/postgresql:<major>`.

Three findings actively contradict the inputs and should reshape the plan. **First**, `kind load docker-image` does not merely lose to a local registry on speed — on this host it *fails outright* (`ctr: content digest … not found`) because Docker 29's containerd image store holds a single platform of a multi-arch manifest while kind imports with `--all-platforms`; the local registry pushed the same image in 2.6 s. **Second**, PITFALLS A2's proposed kubelet reservations (500m + 500m CPU, 1Gi + 1Gi memory) reduce a kind node's advertised allocatable from 32 CPU / 47 GiB to **31 CPU / 44.5 GiB** — measured. Across three nodes the scheduler still believes it has 93 CPUs and 133 GiB on a 32-CPU/47-GiB host. The reservations must be sized as *"host total minus this node's fair share"*, which is roughly 21 CPU and 31 GiB per node, not 1 CPU and 2 GiB. **Third**, `ingress-nginx` was archived read-only on 2026-03-24 and its intended successor InGate was also retired; chart 4.15.1 / controller 1.15.1 is the final release and it does support Kubernetes 1.31–1.35, so D-05 works today — but this is the same shape of risk as MinIO and deserves the same treatment: an ADR, and Ingress objects written with no nginx-specific annotations so the migration is an `ingressClassName` change.

**Primary recommendation:** write `kind/cluster.yaml` with three nodes, `KubeletConfiguration` reservations sized by the *fair-share* arithmetic in §A.3, `extraPortMappings` for 80/443, `extraMounts` for both the DAG directory and a *non-default* persistence path (so D-01's "declared but unbound" is mechanically real), and the containerd `config_path` patch; then drive five `helm upgrade --install --wait=watcher` targets from `make`, with `kubectl wait --for=condition=Available deploy/cnpg-cloudnative-pg` before any `Cluster` CR and `kubectl wait --for=condition=Ready cluster/<name>` before Airflow.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Node capacity honesty (reservations, `maxPods`) | Node / kubelet (`kind/cluster.yaml`) | — | Creation-time only; nothing above the kubelet can correct a lying node |
| Host↔cluster ingress (`:80`/`:443`) | Node (`extraPortMappings`) → ingress controller | DNS (`*.localtest.me`) | Port publishing is creation-time; hostname routing is a runtime Ingress object |
| Image distribution | Local OCI registry (host Docker container) | Node containerd (`certs.d/hosts.toml`) | Registry semantics mirror GHCR; `kind load` has no production counterpart and is broken here |
| Persistence | Node filesystem via local-path provisioner | `extraMounts` (optional binding) | local-path PVs are node-bound, so placement is a scheduling concern, not a storage one |
| Database lifecycle, credentials, failover | CNPG operator (`cnpg-system`) | `Cluster` CR in `data` / `airflow` | Operator owns generation and rotation; the CR is declarative intent only |
| Object storage IAM (deny-delete on `raw`) | MinIO server policy engine | Chart post-install hook Job (`mc`) | Enforcement must live below the application, per §63 |
| Airflow metadata connection | Kubernetes Secret keyed `connection` | Derived at `cluster-up` from the CNPG `-app` Secret | The chart's contract and CNPG's output do not match; the adapter is a scripted derivation, not a values entry |
| Manifest correctness | Offline `kubeconform` + vendored CRD JSON schemas | Repository policy tests over rendered YAML | Static; must not need a cluster (D-12) |
| Cluster-truth verification | `tests/e2e/cluster/` pytest against a live cluster | `make cluster-verify` | Only a live cluster can prove criteria 1–4 |

## Project Constraints (from CLAUDE.md)

The following directives from `.claude/CLAUDE.md` are binding on every plan in this phase. They carry the same authority as the locked decisions above.

| Directive | Consequence for this phase |
|---|---|
| Local multi-node kind cluster mandated; Docker Compose forbidden as the workload platform | The local registry is a plain Docker container (tooling, not workload) — permitted. No Compose file may appear |
| Two physically separate PostgreSQL deployments; Airflow metadata never hosts analytical data | Two `Cluster` CRs, two namespaces, two node placements, two PVCs. Verified separately addressable |
| Applications address data as `s3://bucket/path`, never local paths | `tests/e2e/cluster/` must assert through **boto3** with `endpoint_url`, never `mc` (D-16) |
| Raw layer append-only | `buckets[].versioning: true` on `raw` plus the `Deny` statement — verified expressible in chart 5.4.0 |
| No credential in Git, Python source, Dockerfiles, Kubernetes manifests, Airflow Variables or CI workflow files | MinIO `existingSecret` + `users[].existingSecret`; Airflow `data.metadataSecretName`; CNPG self-generated. **No `rootPassword:`, `fernetKey:` or `webserverSecretKey:` literal may appear in a committed values file** |
| Upstream Helm charts, pinned, with committed values files | Five pinned charts; zero hand-written chart logic. Hand-written manifests limited to namespaces, the derived Secret, the PV/PVC pair (Phase 4) and RBAC |
| CI runner is 4 CPU / 16 GB; values must be profile-parameterized from the start | `values-ci.yaml` written now, with the sizing test of D-12 making the claim mechanical |
| Repo stays on WSL ext4; never hostPath-mount `dags/` from `/mnt/c` | `extraMounts.hostPath` must be under `/home/...`; `make doctor` should assert the repo path is not under `/mnt/` |
| "What NOT to Use" table | Bitnami charts/images, `minio/minio:latest`, the MinIO Operator, the `minio` Python SDK, CeleryExecutor, SequentialExecutor, `kubeval`, hand-rolled infra manifests are all **forbidden**. Nothing in this document proposes any of them |
| `make` is the only gate definition; CI calls `make` and nothing else | Every new gate is a Make target. `tests/policy/test_ci_invokes_make_only.py` enforces it |

## Standard Stack

### Core

| Component | Version | Purpose | Why Standard |
|---|---|---|---|
| kind | `v0.32.0` | Cluster runtime | Only mandated option. Verified: binary runs, creates a cluster with the full patch set `[VERIFIED: kind version → v0.32.0 go1.26.3 linux/amd64]` |
| kind node image | `kindest/node:v1.35.5@sha256:ce977ae6d65918d0b58a5f8b5e940429c2ce42fa3a5619ec2bbc60b949c0ac95` | Kubernetes 1.35.5 | Digest matches the v0.32.0 release notes exactly. **Do not take the v0.32.0 default `v1.36.1`** — outside Airflow 3.3.0's supported 1.30–1.35 `[VERIFIED: GitHub Releases API, kind v0.32.0 body]` |
| Helm | `4.2.3` | Chart installation | Renders and installs all five charts. `version.BuildInfo{Version:"v4.2.3", … KubeClientVersion:"v1.36"}` `[VERIFIED: helm version, executed]` |
| kubectl | `1.36.1` (installed) | Cluster CLI | Verified present: `Client Version: v1.36.1`. Within ±1 minor of server 1.35 |
| CloudNativePG operator chart | `cloudnative-pg` **`0.29.0`** (appVersion `1.30.0`, `kubeVersion: '>=1.29.0-0'`) | PostgreSQL operator | Read from the pinned `Chart.yaml`. Ships **11 CRDs from `templates/crds/`**, gated by `crds.create: true` `[VERIFIED: cloudnative-pg/Chart.yaml + rendered output]` |
| CloudNativePG cluster chart | `cluster` **`0.8.1`** (`kubeVersion: '>=1.29.0-0'`) | `Cluster` CR wrapper | `[VERIFIED: cluster/Chart.yaml]` |
| PostgreSQL images | `ghcr.io/cloudnative-pg/postgresql:17` / `:18` | Metadata / analytical | Derived automatically from `version.postgresql`. Verified running: `17.10 (Debian 17.10-1.pgdg11+1)` and `PostgreSQL 18.4 (Debian 18.4-1.pgdg11+1)` `[VERIFIED: psql -tAc "show server_version" on live pods]` |
| Airflow chart | `airflow` **`1.22.0`** (`appVersion: 3.2.2`) | Airflow deployment | `[VERIFIED: airflow/Chart.yaml lines 143,165 — "appVersion: 3.2.2", "version: 1.22.0"]` |
| Airflow image | `apache/airflow:3.3.0` | Airflow runtime | Override verified end-to-end; `airflow version` → `3.3.0` in the running api-server `[VERIFIED: kubectl exec]` |
| MinIO chart | `minio` **`5.4.0`** (`appVersion: RELEASE.2024-12-18T13-15-44Z`) | Object storage | `[VERIFIED: minio/Chart.yaml]` |
| MinIO image | `pgsty/minio:RELEASE.2026-08-04T00-00-00Z` | Maintained CE fork | Tag present on Docker Hub, pushed 2026-08-04, with `-amd64`/`-arm64` variants `[VERIFIED: hub.docker.com/v2/repositories/pgsty/minio/tags]` |
| ingress-nginx chart | **`4.15.1`** (controller `1.15.1`, `kubeVersion: >=1.21.0-0`) | HTTP ingress | Final release. Supports Kubernetes **1.31–1.35** `[CITED: github.com/kubernetes/ingress-nginx README support table]` |
| kubeconform | `0.8.0` | Offline manifest validation | `[VERIFIED: kubeconform -v → v0.8.0]` |
| Local registry image | `registry:3` | Local OCI registry | Pulled and run successfully; `docker push localhost:5001/…` succeeded in 2.6 s `[VERIFIED: executed]` |

### Supporting

| Component | Version | Purpose | When to Use |
|---|---|---|---|
| `mc` image (chart's bucket/policy bootstrap) | `quay.io/minio/mc:RELEASE.2024-11-21T17-21-54Z` (chart default) | Runs the post-install hook Job | Default. Tag confirmed still present on quay.io `[VERIFIED: quay.io API specificTag query]` |
| `pgsty/mc` | `RELEASE.2026-08-06T00-00-00Z` (18 tags published) | Maintained `mc` fork | Optional override for `mcImage`. **Requires verifying the chart's shell scripts still work** — they call `mc admin policy create/attach`, `mc anonymous set`, `mc version enable` against a 2024-era CLI surface `[ASSUMED: compatibility unverified]` |
| `openapi2jsonschema.py` (from yannh/kubeconform) | `master` | Convert CNPG CRDs → JSON Schema | Vendoring step for offline `-schema-location`. Verified working end-to-end |
| `PyYAML` | `>=6` (already in the root dev group) | Parse rendered multi-document YAML in policy tests | Already available; no new dependency for D-12 `[VERIFIED: pyproject.toml dependency-groups.dev]` |
| `boto3` | `1.43.x` | D-16 S3 assertions | **NOT currently in `uv.lock`** — see Open Question 1 |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|---|---|---|
| Helm 4.2.3 | Helm 3.21.3 | No trigger survives: all five charts render and install under 4.2.3. Falling back would cost the Feb-2027 EOL migration for nothing |
| Local registry | `kind load docker-image` | **Not a tradeoff on this host — `kind load` fails.** See Pitfall 1 |
| ingress-nginx 4.15.1 | Gateway API implementation (Envoy Gateway, Traefik, Cilium) | D-05 locks ingress-nginx. Gateway API is the ecosystem's answer but would add a CRD-heavy component and a second routing vocabulary in a phase that already carries five charts. Record as the ADR migration target |
| CRDs-catalog over the network for kubeconform | Vendored JSON schemas in-repo | The network location works but is an unpinned, unavailable-offline dependency; vendoring is verified and keeps the gate hermetic |
| `data.metadataConnection` (user/pass in values) | `data.metadataSecretName` + derived Secret | The former puts a password in a committed file — forbidden by §81 |

**Installation (host tooling — follow the `install_gitleaks.sh` pattern: commit the per-platform SHA-256, verify before executing):**

```bash
# kind v0.32.0
curl -Lo kind https://kind.sigs.k8s.io/dl/v0.32.0/kind-linux-amd64      # verify SHA-256, then install
# helm 4.2.3
curl -Lo helm.tgz https://get.helm.sh/helm-v4.2.3-linux-amd64.tar.gz    # verify, extract linux-amd64/helm
# kubeconform 0.8.0
curl -Lo kubeconform.tgz https://github.com/yannh/kubeconform/releases/download/v0.8.0/kubeconform-linux-amd64.tar.gz
```

All three URLs were fetched successfully in this session. Upstream publishes `*_checksums.txt` alongside each; the trust-anchor pattern from `tools/security/install_gitleaks.sh` (in-repo digest is authoritative, released checksums file only advisory) applies unchanged.

## Package Legitimacy Audit

This phase installs **no npm, PyPI or crates packages**. Its external artifacts are Helm charts and OCI images, for which the equivalent provenance audit is registry origin, publisher and pin immutability. `gsd-tools query package-legitimacy check` has no applicable ecosystem here; the table below is the honest substitute.

| Artifact | Registry / origin | Age | Publisher | Verdict | Disposition |
|---|---|---|---|---|---|
| `kindest/node@sha256:ce977ae6…` | Docker Hub, digest-pinned | 2026-06-02 | kubernetes-sigs (SIG Testing) | OK | Approved — digest, not tag |
| `apache/airflow:3.3.0` | Docker Hub | 2026-07-06 | Apache Software Foundation | OK | Approved |
| `ghcr.io/cloudnative-pg/postgresql:17` / `:18` | GHCR | current | CNCF CloudNativePG | OK | Approved |
| `ghcr.io/cloudnative-pg/cloudnative-pg:1.30.0` | GHCR | 2026-06-29 | CNCF CloudNativePG | OK | Approved |
| `registry.k8s.io/ingress-nginx/controller:v1.15.1` | registry.k8s.io | 2026-03-19 | Kubernetes (archived project) | **SUS** | Keep — upstream is archived read-only since 2026-03-24; no further CVE patches. Requires ADR + explicit acknowledgement |
| `pgsty/minio:RELEASE.2026-08-04T00-00-00Z` | Docker Hub | 2026-08-04 | Pigsty (single maintainer, community fork) | **SUS** | Keep — already the subject of the SeaweedFS ADR this phase owes. Single-maintainer supply chain |
| `quay.io/minio/mc:RELEASE.2024-11-21T17-21-54Z` | quay.io | 2024-12-15 | MinIO Inc. (pre-archival community artifact) | **SUS** | Keep — ~20 months stale but a genuine community build predating archival. Alternative `pgsty/mc` untested against the chart's scripts |
| `registry:3` | Docker Hub Official Images | current | Docker/CNCF distribution | OK | Approved — local tooling only, never in-cluster |
| `quay.io/prometheus/statsd-exporter:v0.30.0` | quay.io | current | Prometheus | OK | Disabled in this phase (`statsd.enabled: false`); Phase 7 owns it |

**Packages removed due to a `[SLOP]` verdict:** none.
**Artifacts flagged `[SUS]`:** `ingress-nginx/controller`, `pgsty/minio`, `quay.io/minio/mc`. All three are *knowingly* adopted upstream-unmaintained artifacts. The planner should add **one** `checkpoint:human-verify` covering the acceptance of these three, and this phase's ADR-0006 should name all three rather than only MinIO — the ingress finding is new since `STACK.md` was written.

## Architecture Patterns

### System Architecture Diagram

```
  Developer host (WSL2 / ext4)
  ────────────────────────────────────────────────────────────────────────────
   browser / curl / pytest(boto3)          make doctor ──► fail-closed preflight
        │  http://<svc>.localtest.me                       (inotify, disk, docker,
        │  → getaddrinfo: ::1 then 127.0.0.1                pinned tool versions)
        ▼
   host :80 / :443  ◄── docker publish 0.0.0.0 only ── kind extraPortMappings
        │                                                        ▲
        │                                              kind/cluster.yaml
        │                                        (creation-time-only surface)
        ▼
  ┌─ kind cluster "airflow-platform" ──────────────────────────────────────────┐
  │                                                                            │
  │  control-plane [ingress-ready=true]        worker-1 [role=storage]         │
  │   ├─ ingress-nginx controller (hostPort)    ├─ MinIO (Deployment,          │
  │   │        │                                │     standalone, PVC)         │
  │   │        ├─► airflow.localtest.me ──┐     └─ airflow-db (CNPG, PG 17)    │
  │   │        └─► minio.localtest.me ──┐ │                                    │
  │   └─ CNPG operator (cnpg-system)    │ │    worker-2 [role=analytics]       │
  │        │ mutating+validating webhook│ │     └─ analytics-db (CNPG, PG 18)  │
  │        │  ◄── must be Available     │ │                                    │
  │        │      BEFORE any Cluster CR │ │                                    │
  │        ▼                            │ ▼                                    │
  │   ns airflow: api-server(D) scheduler(D) dag-processor(D) triggerer(STS)   │
  │        ▲            │                                                      │
  │        │            └── Secret "airflow-metadata" key=connection           │
  │        │                    ▲ derived at cluster-up from                   │
  │        │                    └── Secret "airflow-db-app" (CNPG-generated)   │
  │   ns etl: (empty — Phase 4 task pods land here)                            │
  │   ns data: MinIO + analytics-db  |  ns ingress-nginx                       │
  │                                                                            │
  │   node containerd ──► /etc/containerd/certs.d/localhost:5001/hosts.toml    │
  └────────────────────────────────────┬───────────────────────────────────────┘
                                       │ image pull
                          kind-registry (docker container, joined to `kind` net)
                                       ▲ docker push localhost:5001/...
                                       │
  CI (GitHub Actions, no cluster until Phase 11)
   make manifests ─► helm template -f values-ci.yaml (×5 charts)
        ├─► kubeconform -strict -kubernetes-version 1.35.5
        │      -schema-location default
        │      -schema-location <vendored CNPG schemas>/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json
        └─► pytest tests/policy  ├─ sum(requests) ≤ 4 CPU / 16 GB
                                 └─ every container has requests+limits
```

### Recommended Project Structure

```
kind/
  cluster.yaml                  # LOCAL: 3 nodes, all creation-time fields
  cluster-ci.yaml               # CI: 1 node, same labels, same port maps
helm/
  versions.env                  # single source of chart + image pins
  values/local/{airflow,minio,cnpg-airflow,cnpg-analytics,ingress-nginx}.yaml
  values/ci/{...same five...}.yaml
  schemas/cnpg/                 # vendored CRD JSON schemas for kubeconform
kubernetes/
  namespaces.yaml               # cnpg-system, data, airflow, etl, ingress-nginx
scripts/
  doctor.sh                     # D-10 fail-closed preflight
  cluster-up.sh cluster-down.sh cluster-rebuild.sh
  minio-credentials.sh          # D-14 generate-into-cluster
  airflow-metadata-secret.sh    # derive `connection` from the CNPG -app Secret
  wait-for.sh                   # the readiness waits helm cannot do
  render-manifests.sh           # helm template both profiles → build/manifests/
  vendor-crd-schemas.sh         # regenerate helm/schemas/cnpg
tools/k8s/install_{kind,helm,kubeconform}.sh   # pinned-binary pattern from Phase 1
tests/policy/test_manifest_resources.py        # D-12 tests 1 and 2
tests/e2e/cluster/                             # D-16, NOT in `make check`
docs/adr/0006-*.md  docs/wsl/wslconfig.example
```

### Pattern 1: creation-time-only surface, isolated in one file

Everything that cannot be changed without `kind delete cluster` lives in `kind/cluster.yaml` and nowhere else: node count and roles, node labels, `extraPortMappings`, `extraMounts`, `kubeadmConfigPatches`, `containerdConfigPatches`. Everything else is a values file or a manifest. This is what makes INFRA-09 auditable — a reviewer can see the whole irreversible surface on one screen.

**Verified working configuration** (executed this session; node came Ready in 17 s):

```yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: airflow-platform
nodes:
  - role: control-plane
    image: kindest/node:v1.35.5@sha256:ce977ae6d65918d0b58a5f8b5e940429c2ce42fa3a5619ec2bbc60b949c0ac95
    kubeadmConfigPatches:
      # v1beta3 MAP form — correct for Kubernetes 1.23–1.35. kind translates
      # map→list automatically if the target is ever v1beta4 (1.36+).
      - |
        kind: InitConfiguration
        nodeRegistration:
          kubeletExtraArgs:
            node-labels: "ingress-ready=true"
      - |
        kind: KubeletConfiguration
        systemReserved: { cpu: "...", memory: "..." }   # see §A.3 for sizing
        kubeReserved:   { cpu: "...", memory: "..." }
        evictionHard:
          memory.available: "500Mi"
          nodefs.available: "10%"
        maxPods: 60
    extraPortMappings:
      - { containerPort: 80,  hostPort: 80,  protocol: TCP }
      - { containerPort: 443, hostPort: 443, protocol: TCP }
    extraMounts:
      - { hostPath: /home/<user>/projects/airflow-platform/airflow/dags, containerPath: /mnt/dags, readOnly: true }
      - { hostPath: /home/<user>/.local/share/airflow-platform/pv,       containerPath: /mnt/persist }
containerdConfigPatches:
  - |-
    [plugins."io.containerd.grpc.v1.cri".registry]
      config_path = "/etc/containerd/certs.d"
```

`[VERIFIED: all of the above applied on a live cluster — node labels present, /mnt/dags and /mnt/persist present in the node container, config_path written into /etc/containerd/config.toml, kubelet config.yaml carrying maxPods: 60 and both reservations]`

### Pattern 2: "declared but unbound" persistence, made mechanically real

`kind` has no notion of an inactive mount, so D-01's wording needs a mechanism. The one that works:

- The provisioner's path is **`/var/local-path-provisioner`**, defined in ConfigMap `local-path-config` (namespace `local-path-storage`), key `config.json`:
  `{"nodePathMap":[{"node":"DEFAULT_PATH_FOR_NON_LISTED_NODES","paths":["/var/local-path-provisioner"]}]}` `[VERIFIED: kubectl -n local-path-storage get cm local-path-config -o jsonpath — verbatim above]`
- Therefore: mount the host directory at a **different** container path (`/mnt/persist`). The mount exists at creation, so no rebuild is ever needed — but nothing uses it.
- Adopting persistence later = patching that ConfigMap's `paths` to `["/mnt/persist"]` and restarting the provisioner. A manifest change, not a cluster rebuild. Exactly D-01's stated intent.

Mounting directly at `/var/local-path-provisioner` would make persistence *active immediately*, which is the opposite of the decision.

### Pattern 3: readiness waits that Helm cannot supply

Three gaps, all measured:

1. **Helm 4 does not wait for workloads unless told.** `--wait` is a `WaitStrategy`; help text: *"Default when flag is omitted: 'hookOnly'"*. `helm upgrade --install cnpg …` without `--wait` returned in **1.0 s** with the operator not yet serving.
2. **The CNPG admission webhook is the real gate, not CRD establishment.** Applying a `Cluster` CR immediately after the operator install failed with:
   `Internal error occurred: failed calling webhook "mcluster.cnpg.io": … dial tcp 10.96.9.157:443: connect: connection refused` `[VERIFIED: executed]`
   `kubectl wait --for=condition=established crd/clusters.postgresql.cnpg.io` passed *before* this failure — so waiting on the CRD alone is insufficient.
3. **A `Cluster` CR is invisible to `--wait`.** But it exposes a usable condition: `Initialized=True ConsistentSystemID=True Ready=True ContinuousArchiving=True`, and `kubectl wait --for=condition=Ready cluster/<name>` returns *"condition met"* `[VERIFIED: executed]`.

Ordering that works:

```bash
helm upgrade --install cnpg cnpg/cloudnative-pg -n cnpg-system --create-namespace \
  --version 0.29.0 --wait --timeout 5m
kubectl wait --for=condition=established --timeout=120s crd/clusters.postgresql.cnpg.io
kubectl -n cnpg-system wait --for=condition=Available --timeout=180s deploy/cnpg-cloudnative-pg
# only now:
helm upgrade --install airflow-db cnpg/cluster -n airflow --version 0.8.1 -f helm/values/<p>/cnpg-airflow.yaml
kubectl -n airflow wait --for=condition=Ready --timeout=300s cluster/airflow-db
scripts/airflow-metadata-secret.sh          # derive the `connection` Secret
helm upgrade --install af apache-airflow/airflow -n airflow --version 1.22.0 -f ... --wait --timeout 15m
```

Prefer `kubectl wait --for=condition=Available deploy/…` over `rollout status`: `rollout status` returned *"successfully rolled out"* in 0.107 s when queried after the fact, and races when the Deployment's generation has not yet been observed.

### Pattern 4: the Airflow metadata connection adapter

The two contracts do not meet, and this is the single hand-written glue piece of the phase.

- Airflow chart wants a Secret named by `data.metadataSecretName` with key **`connection`**:
  `templates/_helpers.yaml:463-465` — `{{- define "airflow_metadata_secret" -}} {{- default (printf "%s-metadata" (include "airflow.fullname" .)) .Values.data.metadataSecretName }}`, consumed at `_helpers.yaml:77-78` as `key: connection` `[VERIFIED: airflow/templates/_helpers.yaml:77-78,463-465]`
- CNPG produces Secret `<cluster>-app`, type `kubernetes.io/basic-auth`, keys:
  `['dbname', 'fqdn-jdbc-uri', 'fqdn-uri', 'host', 'jdbc-uri', 'password', 'pgpass', 'port', 'uri', 'user', 'username']` `[VERIFIED: kubectl get secret airflow-db-app -o jsonpath='{.data}' — verbatim key list]`
  with `uri` = `postgresql://<user>:<pass>@<cluster>-rw.<ns>:5432/<db>` `[VERIFIED, redacted]`

So `scripts/airflow-metadata-secret.sh` reads `username/password/host/port/dbname`, URL-encodes user and password, and writes `connection`. **URL-encode**: CNPG generates random passwords that can contain characters that break a URI. Verified working end-to-end — the migration job succeeded against a Secret built exactly this way.

### Pattern 5: local registry instead of `kind load`

```bash
docker run -d --restart=always -p 127.0.0.1:5001:5000 --name kind-registry registry:3
docker network connect kind kind-registry
# per node, at cluster-up:
docker exec <node> mkdir -p /etc/containerd/certs.d/localhost:5001
printf '[host."http://kind-registry:5000"]\n' | docker exec -i <node> cp /dev/stdin \
  /etc/containerd/certs.d/localhost:5001/hosts.toml
```

`[VERIFIED: executed — hosts.toml written, docker push localhost:5001/airflow:3.3.0 completed in 2.6 s, and the cluster pulled and ran that image]`

The registry container is *not* covered by D-01's disposability: it survives `kind delete cluster` on its own, so `cluster-down` should leave it running and only `make clean-images` should touch it.

### Anti-Patterns to Avoid

- **`helm template … | kubectl apply -f -`.** `helm template` emits `helm.sh/hook: test` resources. Doing this with the CNPG `cluster` chart applied a stray `alpine:3.17` ping-test Job into the namespace `[VERIFIED: observed]`. Use `helm upgrade --install`; if templating is unavoidable, filter test hooks explicitly (`--no-hooks` is too blunt — it also drops MinIO's bucket-creation post-install Job, verified).
- **Relying on `--atomic`.** It does not exist in Helm 4.2.3 (`helm upgrade --help | grep -c -- --atomic` → `0`).
- **Assuming the triggerer is a Deployment.** It renders as a **StatefulSet** (`af-triggerer`). A `cluster-verify` assertion written as "four Deployments" fails on a correct cluster.
- **Leaving `cluster.postgresql` empty in the CNPG cluster chart.** It renders `spec.postgresql: null`, which fails schema validation. See Pitfall 3.
- **Trusting `mode: distributed` / `replicas: 16` / `resources.requests.memory: 16Gi`.** Those are the MinIO chart's stock defaults `[VERIFIED: minio/values.yaml:32,123,291-293]` and will make a kind node unschedulable.
- **Setting a literal `fernetKey` / `webserverSecretKey` / `rootPassword` in a committed values file.** Use `fernetKeySecretName`, `webserverSecretKeySecretName`, `existingSecret`. PITFALLS B8 warns these regenerate on `helm upgrade` if left unmanaged.

## Don't Hand-Roll

| Problem | Don't build | Use instead | Why |
|---|---|---|---|
| PostgreSQL provisioning, credentials, failover, PVC lifecycle | StatefulSet + initContainer + secret generator | CNPG `Cluster` CR | The operator already emits an 11-key credential Secret, a `-rw` service, PDB, and a `Ready` condition you can `kubectl wait` on |
| Bucket creation, versioning, IAM policy, user creation | A bespoke `mc` Job | MinIO chart `buckets:` / `policies:` / `users:` | Verified: the chart's post-install hook Job already runs policy → bucket → user with a 30×2 s connect-retry loop against the service |
| CRD JSON schemas for kubeconform | Hand-written schema files | `openapi2jsonschema.py` over the chart's own rendered CRDs | Regenerating from the pinned chart keeps schema and CRD in lockstep by construction |
| Kubernetes quantity parsing in the policy tests | `int(s.rstrip("m"))` | A ~25-line parser covering `m`, `k/M/G/T/P/E`, `Ki…Ei`, and exponent form | `"2"`, `"500m"`, `"90Mi"`, `"1e3"` all appear in real chart defaults; a naive parser silently under-counts |
| Waiting for readiness | `sleep 60` | `kubectl wait --for=condition=…` with explicit timeouts | Measured stage times vary 17 s–90 s with a warm cache and far more with a cold one; a fixed sleep is both slow and flaky |
| Ingress TLS/hostname plumbing for local dev | `/etc/hosts` edits, self-signed CA scripts | `*.localtest.me` + plain HTTP | Public DNS resolves it to loopback with zero host configuration — verified |
| Pinned-binary installation | New download scripts | The `tools/security/install_gitleaks.sh` shape | Already hardened after CR-03: in-repo digest as trust anchor, verify **before** executing, refuse on version mismatch |

**Key insight:** in this phase nearly every "small script" you might write is already a values key in a chart you have pinned. The hand-written surface that genuinely does not exist upstream is exactly four things: the `doctor` preflight, the Airflow metadata-connection adapter, the readiness waits, and the two policy tests.

## Common Pitfalls

### Pitfall 1: `kind load docker-image` fails outright on this host

**What goes wrong:**
```
ERROR: failed to load image: command "docker exec --privileged -i probe-control-plane ctr
--namespace=k8s.io images import --all-platforms --digests --snapshotter=overlayfs -" failed
Command Output: ctr: content digest sha256:1803ed9d010ddcdc…: not found
```
`[VERIFIED: executed against apache/airflow:3.3.0]`

**Why:** Docker 29.7.2 on this host uses the containerd image store (`docker info` → `storage=overlayfs`). A multi-arch image is present as a manifest list with only the host platform's content pulled; kind's import demands `--all-platforms` and aborts on the missing digest.

**How to avoid:** the local registry, which handles it gracefully — `docker push` reported *"Not all multiplatform-content is present and only the available single-platform image was pushed"* and succeeded `[VERIFIED]`. This upgrades the ROADMAP's "use a local registry" from an optimisation to a correctness requirement, and it applies to the **CI single-node cluster too** (Phase 11), where `STACK.md` had suggested keeping `kind load`.

**Warning signs:** any `ctr: content digest … not found`; `kind load` succeeding for a single-arch image and failing for a multi-arch one.

### Pitfall 2: kubelet reservations that do not actually shrink allocatable

**What goes wrong:** PITFALLS A2's proposal is applied, everyone believes the node no longer lies, and the scheduler still over-packs.

**Measured, on a 32-CPU / 47.04 GiB host, single node, with `systemReserved`/`kubeReserved` = 500m + 1Gi each and `evictionHard.memory.available: 500Mi`:**

| | cpu | memory |
|---|---|---|
| `status.capacity` | `32` | `49325752Ki` (≈47.04 GiB) |
| `status.allocatable` | `31` | `46716600Ki` (≈44.55 GiB) |

`[VERIFIED: kubectl get node -o jsonpath='{.status.capacity}{.status.allocatable}' — values verbatim]`

The arithmetic is exact (`49325752 − 2×1048576 − 512000 = 46716600`), and it removes **3 %** of the node. With three nodes the scheduler sees ~93 CPU and ~133 GiB on a 32-CPU/47-GiB machine.

**How to avoid:** size reservations from the *fair share*, not from daemon overhead. For `N` nodes on a host with `C` CPUs and `M` GiB:

```
allocatable_target ≈ (C / N) − small_headroom      # e.g. 32/3 ≈ 10 CPU
reserved_total     = C − allocatable_target        # ≈ 22 CPU, split across systemReserved+kubeReserved
```
For a 3-node cluster on the documented `.wslconfig` floor (D-11), a defensible starting point is `systemReserved: {cpu: "11", memory: "8Gi"}` and `kubeReserved: {cpu: "10", memory: "7Gi"}`, giving ≈10 CPU / ≈15 GiB allocatable per node and ≈30 CPU / ≈45 GiB cluster-wide — which matches the host. **Document that these are derived from `.wslconfig`, and have `doctor` fail if the actual host is smaller than the floor the numbers assume.** `[ASSUMED: the specific split between systemReserved and kubeReserved is judgement; only the arithmetic and the measurement are verified]`

**Note on `evictionHard`:** the patch **merges** with kind's defaults rather than replacing them — the resulting `/var/lib/kubelet/config.yaml` contained `memory.available: 500Mi`, `nodefs.available: 10%` *and* the inherited `nodefs.inodesFree: 0%` `[VERIFIED]`. Set every key you care about explicitly.

### Pitfall 3: the CNPG cluster chart emits `spec.postgresql: null`, and kubeconform rejects it

**What goes wrong (D-12's CI job goes red on a correct-looking values file):**
```
Cluster analytics-db-cluster is invalid: … at '/spec/postgresql': got null, want object
```
`[VERIFIED: kubeconform 0.8.0 against the CRDs-catalog schema]`

**Why:** `templates/cluster.yaml` renders the literal key `postgresql:` unconditionally; when `cluster.postgresql` contributes nothing the key is present with a null value.

**How to avoid:** always set at least one parameter. `cluster.postgresql.parameters.max_wal_size: "2GB"` (analytical) / `max_connections: "100"` (metadata) makes it a real object — re-validated `Valid: 3, Invalid: 0` `[VERIFIED]`. This is worth an inline comment in the values file, because the fix looks cosmetic and someone will delete it.

### Pitfall 4: the ingress hostname resolves to IPv6 first, and kind publishes IPv4 only

**Measured:**
- `socket.getaddrinfo("airflow.localtest.me", 8080)` returns `('::1', 8080, 0, 0)` **first**, then `('127.0.0.1', 8080)` `[VERIFIED]`
- `docker inspect <node> .NetworkSettings.Ports` → `{"80/tcp":[{"HostIp":"0.0.0.0","HostPort":"8080"}], …}` — **no `::` listener** `[VERIFIED]`
- `curl -v http://airflow.localtest.me:8080/` → `Trying [::1]:8080… Connection refused` then `Trying 127.0.0.1:8080… Connected` `[VERIFIED]`

**Consequence:** it works, because curl, `socket.create_connection`, urllib3 and therefore `requests`/`boto3` all iterate the address list. But every connection pays a failed IPv6 attempt, and any client that pins `AF_INET6` or does not fall back will simply fail. Expect it to be misdiagnosed as "the ingress is down".

**How to avoid:** state it in the runbook; consider setting `listenAddress` explicitly on the `extraPortMappings` entries. `[ASSUMED: whether `listenAddress: "::"` makes kind publish a dual-stack binding is untested]`

### Pitfall 5: the Airflow chart ships thirty `resources: {}` and a hidden fifth container class

Every one of the chart's 30 `resources` keys defaults to `{}` `[VERIFIED: enumerated from airflow/values.yaml]`. Rendering with a realistic values file left **17 containers across four charts with no CPU request, no memory request or no memory limit** `[VERIFIED: policy-test prototype run against rendered output]`:

`af-api-server:{wait-for-airflow-migrations,api-server}`, `af-dag-processor:{wait-for-airflow-migrations,dag-processor,dag-processor-log-groomer}`, `af-scheduler:{…same three…}`, `af-statsd:statsd`, `af-triggerer:{wait-for-airflow-migrations,triggerer,triggerer-log-groomer}`, `af-create-user:create-user`, `af-run-airflow-migrations:run-airflow-migrations`, `mio-minio-post-job:{minio-make-policy,minio-make-bucket,minio-make-user}`.

Two non-obvious mappings:
- The `wait-for-airflow-migrations` initContainer has **no key of its own** — it inherits the parent component's: `templates/scheduler/scheduler-deployment.yaml:152` reads `resources: {{- toYaml .Values.scheduler.resources | nindent 12 }}` `[VERIFIED]`. Setting `scheduler.resources` covers both.
- The log-groomer sidecars have separate keys: `<component>.logGroomerSidecar.resources`.

**Minimum key set** for KubernetesExecutor with statsd/celery/flower/pgbouncer/redis off:
`apiServer.resources`, `scheduler.resources`, `scheduler.logGroomerSidecar.resources`, `dagProcessor.resources`, `dagProcessor.logGroomerSidecar.resources`, `triggerer.resources`, `triggerer.logGroomerSidecar.resources`, `migrateDatabaseJob.resources`, `createUserJob.resources`, `workers.kubernetes.resources`.
MinIO: `resources`, `makeBucketJob.resources`, `makePolicyJob.resources`, `makeUserJob.resources` (each currently `requests.memory: 128Mi` only — no CPU request, no limits).
CNPG operator: `resources` (`{}`). ingress-nginx: `controller.resources` (the only chart that ships a non-empty default: `{'requests': {'cpu': '100m', 'memory': '90Mi'}}`) `[VERIFIED: enumerated from each pinned values.yaml]`.

### Pitfall 6: the D-12 sizing test cannot see the databases

The CNPG `Cluster` CR carries `spec.resources`, but it is not a Pod template — a policy test that walks `Deployment/StatefulSet/Job/DaemonSet/CronJob/Pod` sums **zero** for both PostgreSQL clusters. The prototype confirmed this: total across all four charts came to `cpu=0.350 cores mem=1.000 GiB`, all of it from MinIO and ingress-nginx, with the databases invisible `[VERIFIED: prototype output]`.

**How to avoid:** the test must special-case `postgresql.cnpg.io/v1 Cluster` → `spec.resources.requests × spec.instances`, and should **fail on any unrecognised CR kind** rather than silently contributing zero. A budget test that quietly ignores the two heaviest workloads is worse than no test.

### Pitfall 7: Helm 4's CLI is not Helm 3's

`[VERIFIED: helm 4.2.3 --help output, verbatim]`

| Helm 3 | Helm 4.2.3 | Note |
|---|---|---|
| `--atomic` | **removed** → `--rollback-on-failure` | *"The --wait flag will be defaulted to 'watcher' if --rollback-on-failure is set"* |
| `--force` | `--force-replace` | plus new `--force-conflicts` for server-side apply |
| `--wait` (boolean) | `--wait WaitStrategy[=watcher]` | *"Use '--wait' alone for 'watcher' strategy, or specify one of: 'watcher', 'hookOnly', 'legacy'. **Default when flag is omitted: 'hookOnly'**"* |
| client-side apply | `--server-side` **default `true`** on install, `"auto"` on upgrade | |
| `--dry-run` (boolean) | `--dry-run string` — `none` / `client` / `server` | |
| `helm status --show-resources` | **unknown flag** | scripts that parse it break |
| — | `--take-ownership` | new |

### Pitfall 8: the MinIO post-job runs bucket and user creation *in parallel*

`minio-make-policy` is an **initContainer**; `minio-make-bucket` and `minio-make-user` are ordinary **containers** in the same pod `[VERIFIED: rendered post-job.yaml]`. So policies exist before users (good — `users[].policy` can name a custom policy), but a bucket is not guaranteed to exist when the user is created. Harmless for D-08 because IAM statements name ARNs, not objects — but do **not** add a `customCommands` entry that assumes a bucket exists.

Also: `users[].policy` is a **single** policy name (`mc admin policy attach myminio $POLICY --user=$USER`) `[VERIFIED: _helper_create_user.txt:77]`. D-08's allow-plus-deny must therefore be **one** policy document containing both statements, not two attached policies.

And note the naming trap: `buckets[].policy` is the **anonymous** access policy (`mc anonymous set none|download|upload|public`) `[VERIFIED: _helper_create_bucket.txt]` — completely unrelated to `policies[]`, which are IAM. Set `policy: none` on all five buckets.

### Pitfall 9: `helm template` emits test hooks; `--no-hooks` removes too much

`helm template` includes both `helm.sh/hook: test` resources and `post-install` hooks. `--no-hooks` drops both `analytics-db-cluster-ping-test` **and** `mio-minio-post-job` `[VERIFIED: grep -c on both renderings → 0]`. The bucket-bootstrap Job is a genuine part of the deployed system and must be validated; the `alpine:3.17` ping-test is not. Filter on the annotation in the policy tests rather than at the `helm template` call.

### Pitfall 10: the Airflow image is 3.15 GB, not ~2 GB

`docker images` reports `apache/airflow:3.3.0 → 3.15GB`; in the node's containerd store it lands as **662 MB** `[VERIFIED: crictl images]`. Both numbers matter: the 3.15 GB is the host Docker/WSL VHDX cost (PITFALLS A3), the 662 MB × N nodes is the per-node containerd cost. The full probe stack's node image set totalled ~1.6 GB across 20 image entries.

## Code Examples

### Deny-delete IAM policy on `raw` (MinIO chart 5.4.0)

```yaml
# helm/values/<profile>/minio.yaml — verified to render valid IAM JSON
existingSecret: minio-root            # created by scripts/minio-credentials.sh (D-14)
mode: standalone
replicas: 1
image: { repository: pgsty/minio, tag: "RELEASE.2026-08-04T00-00-00Z" }
buckets:
  - { name: raw,        policy: none, purge: false, versioning: true,  objectlocking: false }
  - { name: validated,  policy: none, purge: false, versioning: false, objectlocking: false }
  - { name: processed,  policy: none, purge: false, versioning: false, objectlocking: false }
  - { name: quarantine, policy: none, purge: false, versioning: false, objectlocking: false }
  - { name: metadata,   policy: none, purge: false, versioning: false, objectlocking: false }
policies:
  - name: etl-app
    statements:
      - effect: Allow
        resources: ['arn:aws:s3:::raw/*', 'arn:aws:s3:::validated/*']
        actions:   ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
      - effect: Deny                      # §63 immutability, enforced by the server
        resources: ['arn:aws:s3:::raw/*']
        actions:   ["s3:DeleteObject", "s3:DeleteObjectVersion"]
users:
  - accessKey: etl-app
    existingSecret: minio-app            # secret NAME only — no value in git (D-14)
    existingSecretKey: secretKey
    policy: etl-app
```

Renders to `policy_0.json` in the chart's ConfigMap with `"Effect": "Deny"` present verbatim `[VERIFIED: helm template output]`. The default `effect` is `Allow` — `templates/_helper_policy.tpl` reads `"Effect": "{{ $statement.effect | default "Allow" }}"` `[VERIFIED]`.

### CNPG cluster values — the four overrides that matter

```yaml
# helm/values/<profile>/cnpg-analytics.yaml
type: postgresql
mode: standalone
fullnameOverride: analytics-db          # otherwise the release name gets "-cluster" appended
version:
  postgresql: "18"                      # chart default is "16" — override or you silently get PG 16
cluster:
  instances: 1                          # chart default is 3
  enablePDB: false                      # "mainly useful for … single-instance clusters" (values.yaml comment)
  storage: { size: 20Gi }
  resources:                            # chart default is {} → D-12 test 2 fails
    requests: { cpu: 500m, memory: 1Gi }
    limits:   { cpu: "2",  memory: 2Gi }
  affinity:
    nodeSelector: { airflow-platform/role: analytics }   # D-03
    topologyKey: kubernetes.io/hostname                  # kind nodes carry no zone label
  postgresql:
    parameters: { max_wal_size: "2GB", work_mem: "32MB" }  # NOT optional — see Pitfall 3
  initdb:
    database: analytics
    owner: analytics_owner
    postInitApplicationSQL:
      - CREATE ROLE etl_app LOGIN;                        # D-15: role only, no schemas
  monitoring: { enabled: false }
backups: { enabled: false }
```

`version.postgresql` flows through `templates/_helpers.tpl` — `{{- printf "ghcr.io/cloudnative-pg/postgresql:%s" .Values.version.postgresql -}}` `[VERIFIED: cluster/templates/_helpers.tpl, define "cluster.imageName"]` — and produced `imageName: ghcr.io/cloudnative-pg/postgresql:18` and a live `PostgreSQL 18.4` server. `cluster.affinity` is a raw `toYaml` passthrough (`templates/cluster.yaml`), and the `nodeSelector` was verified reaching the pod's `spec.nodeSelector` `[VERIFIED]`. `postInitApplicationSQL` produced the `etl_app` role `[VERIFIED: select rolname from pg_roles]`.

### Airflow values — the minimum honest set

```yaml
airflowVersion: "3.3.0"
defaultAirflowRepository: localhost:5001/airflow   # local profile; ghcr.io/... in CI
defaultAirflowTag: "3.3.0"
executor: KubernetesExecutor                       # LocalExecutor in values-ci.yaml
postgresql: { enabled: false }                     # bitnamilegacy subchart — always off
data: { metadataSecretName: airflow-metadata }     # key `connection`, derived at cluster-up
statsd: { enabled: false }                         # Phase 7 owns metrics
triggerer: { enabled: true }
fernetKeySecretName: airflow-fernet-key            # never a literal in git
webserverSecretKeySecretName: airflow-api-secret-key
ingress:
  apiServer:
    enabled: true
    ingressClassName: nginx
    hosts: [{ name: airflow.localtest.me }]
# ...plus every resources key from Pitfall 5
```

`[VERIFIED end-to-end: this shape (with literal keys substituted for the probe) installed in 48 s; migration job completed; `alembic_version` = `d2f4e1b3c5a7`; 71 tables in `public`; api-server, scheduler, dag-processor Deployments and the triggerer StatefulSet all Ready]`

### Offline CRD schema vendoring for kubeconform

```bash
# scripts/vendor-crd-schemas.sh  (regenerate whenever the CNPG chart pin moves)
helm template cnpg cnpg/cloudnative-pg --version 0.29.0 -n cnpg-system \
  | yq 'select(.kind == "CustomResourceDefinition")' > build/cnpg-crds.yaml
FILENAME_FORMAT='{kind}_{version}' python3 tools/k8s/openapi2jsonschema.py build/cnpg-crds.yaml
# → cluster_v1.json, database_v1.json, pooler_v1.json, … (11 files) into helm/schemas/cnpg/
```
```bash
# make manifests
kubeconform -strict -summary -kubernetes-version 1.35.5 \
  -schema-location default \
  -schema-location 'helm/schemas/cnpg/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json' \
  build/manifests/*.yaml
```
`[VERIFIED: this exact pipeline turned "Cluster … failed validation: could not find schema for Cluster" (exit 1) into "Valid: 3, Invalid: 0, Errors: 0" (exit 0)]`
**ingress-nginx 4.15.1 ships no CRDs at all** — no `crds/` directory and no `CustomResourceDefinition` template `[VERIFIED]` — so the CONTEXT/ROADMAP expectation of "`-schema-location` entries for the CNPG **and ingress-nginx** CRDs" needs only the CNPG half.

### D-12 policy test skeleton (prototyped and run)

```python
SUFFIX = {"": 1, "m": 0.001, "k": 1e3, "M": 1e6, "G": 1e9, "T": 1e12, "P": 1e15, "E": 1e18,
          "Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40, "Pi": 2**50, "Ei": 2**60}
QTY = re.compile(r"^(?P<num>[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)(?P<suf>m|[kKMGTPE]i?|)$")

def containers(doc):
    kind = doc.get("kind")
    if kind in ("Deployment", "StatefulSet", "DaemonSet", "Job", "ReplicaSet"):
        spec = doc["spec"]["template"]["spec"]
    elif kind == "CronJob":
        spec = doc["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    elif kind == "Pod":
        spec = doc["spec"]
    else:
        return []                       # Cluster CRs handled separately — see Pitfall 6
    return list(spec.get("initContainers", [])) + list(spec.get("containers", []))

def is_test_hook(doc):
    ann = (doc.get("metadata", {}).get("annotations") or {})
    return "test" in ann.get("helm.sh/hook", "")

for doc in yaml.safe_load_all(path.read_text()):
    if not doc or is_test_hook(doc):    # `if not doc` is load-bearing: `---` + comments → None
        continue
```

Follow the existing `tests/policy/` convention of proving non-vacuity: mutate a copy of the rendered bundle (drop one container's `resources`) and assert the predicate reports it — the shape `test_ci_invokes_make_only.py::test_a_direct_tool_invocation_is_reported` already uses.

## State of the Art

| Old approach | Current approach | When changed | Impact on this phase |
|---|---|---|---|
| `helm upgrade --install --atomic --wait` | `--rollback-on-failure --wait=watcher` | Helm 4.0 | D-09's command line must be rewritten, not copied |
| Helm client-side apply | Server-side apply by default | Helm 4.0 | Field-manager conflicts become possible on hand-edited resources; `--force-conflicts` is the escape hatch |
| kubeadm `v1beta3` patches | `v1beta4` for Kubernetes ≥ 1.36 | kind v0.32.0 | We pin 1.35.5, so **map-form** `kubeletExtraArgs` is correct today; kind auto-translates map→list if the target is v1beta4 |
| `minio/minio` community images | archived 2026-04-25; `pgsty/minio` fork | 2026 | Already absorbed by STACK.md |
| Bitnami free catalog | `bitnamilegacy`, frozen | 2025-08-28 | `postgresql.enabled: false` |
| **ingress-nginx** as the default local ingress | **archived read-only 2026-03-24**; InGate successor also retired; Gateway API is the ecosystem answer | 2026 | **New since STACK.md.** D-05 still works (1.15.1 supports k8s 1.31–1.35) but the artifact is unmaintained |
| `kind load docker-image` | local OCI registry | Docker 29 containerd image store | Now a correctness requirement, not a speed optimisation |

**Deprecated / outdated in the inputs:**
- `STACK.md`'s `kind/cluster.yaml` example uses the **list form** `kubeletExtraArgs: [- name: node-labels, value: …]`. That is the v1beta4 shape; for the pinned Kubernetes 1.35.5 the v1beta3 **map** form is what was verified working.
- `STACK.md`'s `extraPortMappings` example maps NodePorts 30080/30900/30300/30820. D-05 supersedes this with 80/443 + hostname routing.
- `STACK.md`'s CNPG example places `airflow-db` in namespace `airflow` and `analytics-db` in `data` — D-13 is the authority; note it puts *both* CNPG clusters' namespaces differently (`data` holds analytics + MinIO).
- `STACK.md`'s CNPG example uses `postInitApplicationSQL` to create `staging`/`warehouse`/`analytics`/`metadata` schemas. **D-15 forbids this** — schemas are Alembic's, Phase 3.

## Assumptions Log

| # | Claim | Section | Risk if wrong |
|---|---|---|---|
| A1 | The specific split of the fair-share reservation between `systemReserved` and `kubeReserved` (11/10 CPU, 8/7 GiB) is judgement; only the arithmetic and the 3-node inflation are measured | Pitfall 2 | Mis-sized nodes; correctable by a cluster rebuild, which is exactly what INFRA-09 exists to avoid — so validate on the real 3-node cluster in plan 2a |
| A2 | `pgsty/mc` works with the MinIO chart 5.4.0 bootstrap scripts | Standard Stack | Bucket/policy/user creation fails at install. Mitigation: keep the chart default `quay.io/minio/mc` unless a reason appears |
| A3 | Setting `listenAddress: "::"` on `extraPortMappings` would give a dual-stack host binding | Pitfall 4 | Only affects a nice-to-have; the IPv4 fallback already works |
| A4 | 3-node stage timings scale from the measured single-node numbers | Validation Architecture | D-04's ~15 min budget may be wrong; it is a warn-not-fail gate by design |
| A5 | ~10 CPU / ~15 GiB allocatable per node is the right target for the D-11 `.wslconfig` floor | Pitfall 2 | Under-provisioned local cluster; a values change, not a rebuild |
| A6 | The CI profile's rendered request total fits 4 CPU / 16 GB once every container is given requests | §F / D-12 | The sizing test fails on first run and requests must be trimmed — which is the test doing its job |
| A7 | ingress-nginx 1.15.1 will keep working on Kubernetes 1.35.5 for the project's lifetime despite receiving no patches | Package Legitimacy Audit | Security exposure on a local-only cluster is low; a production port would need the Gateway API migration named in ADR-0006 |

## Open Questions

1. **`tests/e2e/cluster/` needs `boto3`, and `uv.lock` is shared with Phase 3.**
   - What we know: `boto3`, `botocore`, `psycopg` and the Kubernetes Python client are **absent from `uv.lock`** `[VERIFIED: grep -cE '^name = "(boto3|psycopg|kubernetes)"' uv.lock → 0]`. The root dev group is `['dataplat', 'csv-processor', 'pytest', 'pytest-cov', 'pytest-xdist', 'hypothesis', 'ruff==0.16.2', 'mypy==2.3.0', 'import-linter', 'pre-commit', 'PyYAML>=6']` `[VERIFIED: pyproject.toml]`.
   - What's unclear: CONTEXT.md asserts Phase 2 and Phase 3 "share no files", but D-16's boto3 requirement forces a root `pyproject.toml` + `uv.lock` change, and Phase 3 will change `packages/*/pyproject.toml` + the same `uv.lock`. `make install` uses `uv sync --locked`, which **fails** rather than resolving, so a stale lock is a hard CI failure for whichever phase lands second.
   - Recommendation: land the dependency addition as the **first task of Phase 2** in its own commit (a `cluster` dependency-group containing `boto3` and `psycopg[binary]`), and record it as a known coordination point with Phase 3. Do not assume the phases are file-disjoint.

2. **Whether to reuse Phase 1's installer pattern for three binaries or generalise it.**
   - What we know: `tools/security/install_gitleaks.sh` is a 148-line, single-purpose, hardened script whose exact ordering (`verify → extract → install`, never execute-to-check-version) was the fix for CR-03.
   - What's unclear: three near-copies invite drift; one parameterised script risks weakening the property that made CR-03's fix reviewable.
   - Recommendation: three scripts sharing one sourced `verify_pinned_download()` helper, each keeping its own `PINNED_SHA256_*` block. `tests/policy/test_pinned_tool_versions_agree.py` already exists and should be extended to cover kind/helm/kubeconform across the Makefile, the installers and the workflow.

3. **Where the pinned chart/image versions live so the Makefile and both values profiles cannot drift.**
   - What we know: Phase 1's standing convention is "pinned versions live in exactly one place and are asserted at runtime" (`UV_REQUIRED_VERSION`).
   - What's unclear: Helm values files cannot read a `versions.env`; `--set image.tag=$(MINIO_TAG)` on the command line contradicts "committed values files".
   - Recommendation: `helm/versions.env` as the single source for **chart** versions (consumed by `--version $(…)` in the Makefile) and image tags duplicated into the values files, with a policy test asserting the two agree. This is the same shape as the existing gitleaks-version test.

4. **`.gsd/` is still untracked and unignored (WINDOWS #7).**
   - Recommendation: fix `.gitignore` in the first commit of this phase, before any infrastructure file is added. It is a one-line change and the ledger says it will otherwise be swept into a Phase-2 commit.

5. **Whether `make cluster-verify` should be an alias into `make ci`.**
   - What we know: WINDOWS #8 records that `make check` names test paths explicitly and a new directory is silently uncollected. `make check` is contractually offline and cluster-free.
   - Recommendation: `cluster-verify` is its own target, must **not** join `check` or `ci`, and `tests/policy/` should gain an assertion that no `check`/`ci` prerequisite chain reaches `tests/e2e/` — turning WINDOWS #8 from a note into a gate.

## Environment Availability

| Dependency | Required by | Available | Version | Fallback |
|---|---|---|---|---|
| Docker daemon | everything | ✓ | `29.7.2`, storage driver `overlayfs` (containerd image store) | none — `doctor` blocks |
| kubectl | all cluster ops | ✓ | `v1.36.1` | none |
| uv | Python gates | ✓ | `0.12.3` (matches `UV_REQUIRED_VERSION`) | none |
| Python | policy + e2e tests | ✓ | `3.12.3` | none |
| **kind** | cluster creation | ✗ | — | `tools/k8s/install_kind.sh` (pinned + digest-verified). Download verified reachable |
| **helm** | all chart installs | ✗ | — | `tools/k8s/install_helm.sh`. Download verified reachable |
| **kubeconform** | CICD-07 | ✗ | — | `tools/k8s/install_kubeconform.sh`. Download verified reachable |
| `jq` / `yq` | manifest filtering in scripts | ✗ | — | Use `python3 -c` with PyYAML (already a dev dep) — avoids two new pinned binaries |
| `mc` (host) | — | ✗ | — | Not needed: D-16 mandates boto3, and bucket bootstrap runs in-cluster |
| inotify limits | PITFALLS A1 | ✓ | `max_user_watches=1048576`, `max_user_instances=8192`, `fs.file-max` unbounded | Already above target on this machine — `doctor` must still assert, since a fresh WSL distro defaults to 8192/128 |
| cgroup v2 | kind on WSL2 | ✓ | `cgroup2fs` | kind v0.32.0 now warns on cgroup v1 |
| Free ext4 disk | image + PV storage | ✓ | 812 GB free of 1007 GB | — |
| Host CPU / RAM | 3-node cluster | ✓ | 32 CPU / 47 GiB | — |
| Host ports 80 / 443 | D-05 | ✓ free | — | **`doctor` must check this**: this host runs ten unrelated containers, and a port conflict surfaces as a confusing `kind create` failure |
| Network to Docker Hub / GHCR / quay.io / charts.min.io / kubernetes.github.io | image + chart pulls | ✓ | all five reachable | — |

**Missing dependencies with no fallback:** none.
**Missing dependencies with fallback:** kind, helm, kubeconform — all three install via the Phase-1 pinned-binary pattern; their download URLs were exercised successfully in this session.

## Validation Architecture

### Test Framework

| Property | Value |
|---|---|
| Framework | `pytest 9.1.x` (`minversion = "9.0"`, `addopts = "-ra --strict-markers --strict-config"`) `[VERIFIED: pyproject.toml tool.pytest.ini_options]` |
| Config file | `pyproject.toml` (`[tool.pytest.ini_options]`, `testpaths = ["tests"]`) |
| Quick run command | `make policy` → `uv run --frozen pytest tests/policy -q` |
| Full suite command | `make check` (offline, cluster-free) and `make ci` (adds gitleaks) |
| Live-cluster suite | `make cluster-verify` → `uv run --frozen pytest tests/e2e/cluster -q` — **deliberately not in `check` or `ci`** |
| New markers needed | `cluster: requires a live kind cluster` (add to `markers` so `--strict-markers` does not reject it) |

### Phase Requirements → Test Map

| Req | Behavior | Test type | Automated command | File exists? |
|---|---|---|---|---|
| INFRA-01 | `cluster.yaml` declares 3 nodes, both `extraMounts`, both `extraPortMappings`, `containerdConfigPatches` and a `KubeletConfiguration` patch on every node | policy (static) | `pytest tests/policy/test_kind_cluster_config.py -x` | ❌ Wave 0 |
| INFRA-01 | destroy+recreate produces a working cluster | e2e (live) | `make cluster-rebuild && make cluster-verify` | ❌ Wave 0 |
| INFRA-02 | four Airflow workloads Ready as separate objects (3 Deployments + 1 **StatefulSet**) | e2e | `pytest tests/e2e/cluster/test_airflow_workloads.py -x` | ❌ Wave 0 |
| INFRA-02 | the Airflow UI answers through the ingress | e2e | `pytest tests/e2e/cluster/test_ingress.py -x` (HTTP 200 from `http://airflow.localtest.me/`) | ❌ Wave 0 |
| INFRA-03 | metadata cluster reports PostgreSQL 17 | e2e | `pytest tests/e2e/cluster/test_postgres_topology.py::test_metadata_is_pg17 -x` | ❌ Wave 0 |
| INFRA-04 | analytical cluster reports PostgreSQL 18; two distinct `Cluster` resources; disjoint PVCs and nodes | e2e | `…::test_two_distinct_clusters_no_shared_storage -x` | ❌ Wave 0 |
| INFRA-05 | five buckets reachable as `s3://bucket/key` **via boto3** | e2e | `pytest tests/e2e/cluster/test_minio_buckets.py -x` | ❌ Wave 0 |
| INFRA-05 / §63 | the application credential is **refused** on `DeleteObject` against `raw`, and the admin credential is not | e2e (negative) | `…::test_raw_delete_is_denied_for_app_credential -x` | ❌ Wave 0 |
| INFRA-07 | no `kubectl create/edit/patch/apply` outside committed manifests in any script | policy | `pytest tests/policy/test_no_manual_kubectl_surgery.py -x` | ❌ Wave 0 |
| INFRA-09 | reservations + `maxPods` present in `cluster.yaml`; live node allocatable is below a declared ceiling | policy + e2e | `…test_kind_cluster_config.py` and `…/test_node_capacity.py` | ❌ Wave 0 |
| INFRA-10 | both profiles render; they differ on **only** replicas, resources and monitoring | policy | `pytest tests/policy/test_values_profiles.py -x` | ❌ Wave 0 |
| CICD-07 | `kubeconform -strict` passes on both rendered profiles and **fails** on a deliberately broken manifest | policy (non-vacuity) | `make manifests` + `pytest tests/policy/test_manifest_validation_fails_closed.py -x` | ❌ Wave 0 |
| CICD-07 / D-12 #1 | summed container requests over the CI profile ≤ 4 CPU / 16 GB, **including** CNPG `Cluster` CRs | policy | `pytest tests/policy/test_manifest_resources.py::test_ci_profile_fits_runner -x` | ❌ Wave 0 |
| CICD-07 / D-12 #2 | every container in both profiles has CPU+memory requests and limits | policy | `…::test_every_container_is_sized -x` | ❌ Wave 0 |
| D-10 | `doctor` exits non-zero on each failure class it claims to block | policy (fault injection) | `pytest tests/policy/test_doctor_fails_closed.py -x` | ❌ Wave 0 |
| D-14 | no credential literal in any values file, manifest or script | policy | extend `tests/policy/test_workflow_secrets.py` scope to `helm/`, `kubernetes/`, `kind/`, `scripts/` | ⚠️ exists, needs widening |

### Sampling Rate

- **Per task commit:** `make policy` (static, sub-second; needs no cluster and no network).
- **Per wave merge:** `make check` — the full offline gate, including `make manifests` once it is wired into `check`.
- **Phase gate:** `make cluster-up && make cluster-verify` green on a real 3-node cluster, then `make cluster-rebuild && make cluster-verify` green a second time. One pass proves it works; two passes prove it is reproducible, which is what INFRA-01 actually claims.

### Wave 0 Gaps

- [ ] `tests/policy/test_kind_cluster_config.py` — INFRA-01, INFRA-09
- [ ] `tests/policy/test_values_profiles.py` — INFRA-10 / D-06 three-axis rule
- [ ] `tests/policy/test_manifest_resources.py` — D-12 tests 1 and 2 (with the CNPG `Cluster` special case)
- [ ] `tests/policy/test_manifest_validation_fails_closed.py` — CICD-07 non-vacuity
- [ ] `tests/policy/test_doctor_fails_closed.py` — D-10 non-vacuity
- [ ] `tests/policy/test_no_manual_kubectl_surgery.py` — INFRA-07
- [ ] `tests/e2e/cluster/conftest.py` — shared fixtures: kube client / `kubectl` shell helper, boto3 client built from `make minio-creds`, `cluster` marker, skip-if-no-cluster
- [ ] `tests/e2e/cluster/{test_airflow_workloads,test_postgres_topology,test_minio_buckets,test_ingress,test_node_capacity}.py`
- [ ] Makefile: `doctor`, `cluster-up`, `cluster-down`, `cluster-rebuild`, `cluster-verify`, `minio-creds`, `manifests`, `helm-lint`; wire `manifests` into `check`, and `cluster-verify` into **nothing**
- [ ] Root dependency group `cluster = ["boto3", "psycopg[binary]"]` + `uv lock` (see Open Question 1)
- [ ] `markers` entry for `cluster` in `[tool.pytest.ini_options]` (required by `--strict-markers`)

## Security Domain

`security_enforcement: true`, `security_asvs_level: 1`. This phase has no application code; its attack surface is credential handling, supply chain and workload isolation.

### Applicable ASVS Categories

| ASVS category | Applies | Standard control for this phase |
|---|---|---|
| V2 Authentication | yes | MinIO root + application credentials generated in-cluster (D-14); Airflow's own auth deferred to the FabAuthManager the chart configures; CNPG-generated Postgres credentials |
| V3 Session Management | no | No session-bearing code is written here |
| V4 Access Control | yes | MinIO IAM allow/deny split (D-08); per-namespace ServiceAccounts (D-13); the Airflow chart's Role/RoleBinding for KubernetesExecutor |
| V5 Input Validation | partial | `kubeconform -strict` over rendered manifests is the phase's input validation; `-strict` rejects unknown fields |
| V6 Cryptography | yes | `fernetKey` and `webserverSecretKey` must be generated (never literal, never regenerated on upgrade — PITFALLS B8) and stored as Secrets referenced by name |
| V14 Configuration | yes | Non-root containers, resource limits on every container, no `:latest` tag, pinned chart versions, digest-pinned node image |

### Known Threat Patterns

| Pattern | STRIDE | Standard mitigation |
|---|---|---|
| Credential written to the working tree by a bootstrap script and later committed | Information Disclosure | D-14: generate directly into a Kubernetes Secret; `make minio-creds` reads back on demand; gitleaks (live since Phase 1) scans full history |
| Tampered `kind`/`helm`/`kubeconform` download | Tampering | In-repo SHA-256 trust anchor, verify **before** extract or execute (`install_gitleaks.sh` pattern, hardened by CR-03) |
| Unmaintained upstream artifact ships an unpatched CVE (`pgsty/minio`, ingress-nginx 1.15.1, `mc` 2024) | Elevation of Privilege | Explicit ADR-0006 acknowledgement + `checkpoint:human-verify`; the S3 client abstraction keeps the escape hatch a values change |
| `BestEffort` pod evicted first, destroying the evidence needed to explain the incident | Denial of Service | D-12 test 2 — every container carries requests and limits |
| Host OOM killer arbitrating instead of Kubernetes eviction | Denial of Service | Fair-share kubelet reservations (Pitfall 2) + `evictionHard` |
| Local registry reachable from outside the host | Information Disclosure / Tampering | Publish on `127.0.0.1:5001` only (never `0.0.0.0`), as verified in the recipe above |
| Airflow metadata credential exposed through the derived Secret | Information Disclosure | The derivation script must not echo, and the Secret must live only in `airflow`; the CNPG `-app` Secret already lives in that namespace |
| A committed values file drifting to a literal password | Information Disclosure | Widen `tests/policy/test_workflow_secrets.py` to `helm/`, `kubernetes/`, `kind/` and `scripts/` |

## Sources

### Primary (HIGH confidence — executed or read from the pinned artifact this session)

- Live kind cluster `probe` on `kindest/node:v1.35.5@sha256:ce977ae6…`, created and destroyed 2026-08-12 — node capacity/allocatable, kubelet `config.yaml`, node labels, `extraMounts`, containerd `config.toml`, `local-path-config` ConfigMap, CNPG install ordering and webhook failure, `Cluster` conditions, CNPG `-app` Secret key list, `show server_version` on both majors, Airflow install + migration + four workloads, `airflow version`, `alembic_version`, ingress end-to-end HTTP 200, `crictl images`
- `helm v4.2.3` binary — `helm version`, `helm install/upgrade --help` (flag surface), five `helm template` renderings, three `helm upgrade --install` runs
- Pinned chart tarballs: `airflow-1.22.0.tgz` (`Chart.yaml`, `values.yaml`, `templates/_helpers.yaml`, `templates/scheduler/scheduler-deployment.yaml`), `minio-5.4.0.tgz` (`values.yaml`, `templates/post-job.yaml`, `_helper_policy.tpl`, `_helper_create_bucket.txt`, `_helper_create_user.txt`, `_helper_create_policy.txt`, `secrets.yaml`), `cluster-0.8.1.tgz` (`Chart.yaml`, `values.yaml`, `templates/cluster.yaml`, `templates/_helpers.tpl`, `templates/_bootstrap.tpl`), `cloudnative-pg-0.29.0.tgz`, `ingress-nginx-4.15.1.tgz`
- `kubeconform v0.8.0` binary — four validation runs, before and after the CRD-schema fix
- `kind v0.32.0` binary; GitHub Releases API `kubernetes-sigs/kind` tag `v0.32.0` (full body — node-image digests, kubeadm v1beta3/v1beta4 boundary, containerd config version-aware patching, Envoy LB, cgroup v1 warning)
- Docker Hub API — `pgsty/minio` tags (newest `RELEASE.2026-08-04T00-00-00Z`, 2026-08-04), `pgsty/mc` tags (18 tags, newest `RELEASE.2026-08-06T00-00-00Z`); quay.io API — `minio/mc` `RELEASE.2024-11-21T17-21-54Z` present
- `https://kubernetes.github.io/ingress-nginx/index.yaml` — chart 4.15.1 / appVersion 1.15.1, created 2026-03-19, `kubeVersion: >=1.21.0-0`
- Repository files read directly: `Makefile`, `.github/workflows/ci.yml`, `tools/security/install_gitleaks.sh`, `pyproject.toml`, `uv.lock`, `.gitignore`, `tests/policy/*`, `docs/adr/0000-template.md`, `.planning/{PROJECT,STATE,ROADMAP,REQUIREMENTS,WINDOWS}.md`, `.planning/research/{STACK,SUMMARY,PITFALLS}.md`, `.planning/phases/02-*/02-CONTEXT.md`

### Secondary (MEDIUM confidence)

- `github.com/kubernetes/ingress-nginx` README — archival date 2026-03-24 and the controller↔Kubernetes support table (fetched, not executed)
- WebSearch on the ingress-nginx retirement and the InGate outcome — corroborated by the archived README

### Tertiary (LOW confidence)

- WebSearch on kubelet reservation practice — generic, superseded by the direct measurement in Pitfall 2

## Metadata

**Confidence breakdown:**

- Standard stack: **HIGH** — every chart, image and binary was fetched and exercised; every version string quoted is from the artifact
- Chart values keys: **HIGH** — read from the pinned tarballs and confirmed by rendering; nothing inferred from documentation
- Helm 4 compatibility: **HIGH** — was the MEDIUM call; five charts rendered and three installed successfully
- Airflow 3.3.0 on chart 1.22.0: **HIGH** — migration and all four workloads verified running
- Cluster-config semantics (reservations, mounts, port maps, containerd): **HIGH** — applied and read back from a live node
- Resource sizing numbers: **MEDIUM** — the arithmetic and the over-count are measured; the chosen split is judgement (A1, A5)
- CI budget fit for the rendered CI profile: **MEDIUM** — the test mechanism is verified, the outcome is not yet known (A6)
- Timing budget for a 3-node cold-cache rebuild: **LOW-MEDIUM** — extrapolated from single-node measurements (A4)

**Measured stage times (single node, warm image cache, this host):** `kind create` 17 s · CNPG operator install (no wait) 1.0 s · CNPG cluster PG 18 Ready 75 s · CNPG cluster PG 17 Ready ~90 s · Airflow chart install incl. migration 48 s · ingress-nginx install 25 s · `docker push` 3.15 GB Airflow image to the local registry 2.6 s. Full stack footprint: **17 pods / 26 containers / 2.95 GiB RSS**.

**Research date:** 2026-08-12
**Valid until:** 2026-09-11 (30 days). Re-verify sooner if the Airflow chart 2.0.0 / appVersion 3.3.0 release lands — it removes the appVersion override entirely.
