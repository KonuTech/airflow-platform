# Phase 2: kind Cluster & Core Infrastructure - Context

**Gathered:** 2026-08-11
**Status:** Ready for planning

<domain>
## Phase Boundary

A production-like Kubernetes data platform that can be destroyed and recreated reproducibly
from committed files: a 3-node kind cluster (control-plane + 2 workers) running MinIO, two
physically separate CloudNativePG clusters (PG 17 for Airflow metadata, PG 18 analytical) and
Airflow 3.3 — with `values-local.yaml` and `values-ci.yaml` written from the first
infrastructure commit, and CI failing on an invalid manifest or chart.

**Out of scope — belongs to other phases:**

- **Vault** — Phase 5 (deviation D3). This phase must not deploy it, and must not design
  around its absence in a way that makes the Phase 5 retrofit more than a configuration change.
- **kube-prometheus-stack / Tempo / OTel collector** — Phase 7.
- **The `csv-processor` image and any Python** — Phase 3 (runs fully in parallel; the two
  phases share no files).
- **Any real DAG, `KubernetesPodOperator` usage, XCom** — Phase 4.
- **The ephemeral-kind E2E job in GitHub Actions** — Phase 11. This phase writes the
  `values-ci.yaml` that job consumes, and nothing more.
- **Alembic migrations and the `meta` schema** — Phase 3.

</domain>

<decisions>
## Implementation Decisions

### Cluster state and host mounts

- **D-01:** Cluster state is **disposable**. `make cluster-rebuild` is a first-class, scripted,
  timed operation (recreate cluster → charts → seed → ready), not an emergency measure.
  README §90 demands rebuildability anyway, so this converts PITFALLS A4 from a risk into a
  deliverable. `extraMounts` are still **declared in `kind/cluster.yaml` at creation** but left
  unbound, so adopting persistence later is a values change rather than a cluster rebuild.
  — **Reversibility:** one-way — reversing the *mount declaration* (not the policy) requires
  `kind delete cluster`, which destroys exactly the state persistence was meant to protect.

- **D-02:** The `airflow/dags/` hostPath `extraMount` (WSL ext4, **never `/mnt/c`**) is
  **declared on every node now, wired in Phase 4**. Both values profiles stay on the chart
  default in this phase. Rationale: the expensive half (cluster recreation) is paid now; the
  cheap half (a values change) waits until there is a DAG to mount. `values-ci.yaml` stays on
  the baked-image path permanently, so CI proves the production mechanism.
  — **Reversibility:** one-way — same reason as D-01; the mount must exist at creation time.

- **D-03:** Stateful placement **splits the two databases across the workers**:
  worker-1 = Airflow metadata PG 17 + MinIO; worker-2 = analytical PG 18, alone. Explicit
  `nodeSelector` on each — never left to the scheduler, because local-path PVs are node-bound
  and a rescheduled stateful pod sits `Pending` with `volume node affinity conflict`, which
  reads as a scheduler bug. This makes §4's "separation stays visible even inside one cluster"
  physical rather than nominal, and keeps the heaviest workload (COPY into PG 18) off the node
  serving object storage.
  — **Reversibility:** costly — changing placement after data exists means re-provisioning
  PVCs; node labels themselves are cheap.

- **D-04:** `make cluster-rebuild` **times each stage** (cluster create → CNPG operator →
  Cluster CRs → MinIO → ingress → Airflow → ready), prints the breakdown, and writes the last
  run to a gitignored file. Documented budget ~15 minutes, **warn past it, do not fail**:
  wall-clock on a cold image cache measures the network rather than the repository, and a flaky
  gate is one people learn to ignore. Per-stage numbers make a regression attributable.

### Access surface

- **D-05:** **ingress-nginx behind `extraPortMappings`.** Host `:80`/`:443` map to a node
  labelled `ingress-ready`; routing is by hostname under `*.localtest.me` (resolves to
  127.0.0.1 with no `/etc/hosts` edit on WSL *or* Windows). Stable URLs that survive every
  rebuild, one mechanism for every service added through Phase 7.
  — **Reversibility:** one-way — `extraPortMappings` is a creation-time-only field in
  `kind/cluster.yaml`; adding a port later costs a cluster rebuild.

- **D-06:** **`values-ci.yaml` keeps the same ingress.** Single-node kind labels its one node
  `ingress-ready`, so the same manifests apply. The two profiles diverge on **exactly three
  axes: replica counts, resource sizing, monitoring disabled.** Every additional divergence axis
  is a bug class that appears only in CI, nine phases downstream of where it was introduced.

- **D-07:** MinIO is addressed through **one `S3_ENDPOINT_URL` injected per context** —
  in-cluster pods get the ClusterIP service DNS, host-side tools and tests get the ingress host.
  No address is hardcoded in Python, DAGs, manifests or values. Unsetting the variable resolves
  real AWS, which is what makes §5's swap-out claim true. Rejected: CoreDNS rewriting so one URL
  works everywhere (a kind-specific customization that exists nowhere in production).

- **D-08:** `s3://raw/` gets **versioning at bucket creation plus an IAM deny-delete policy**
  scoped to the application credential, with the admin credential retaining delete. §63
  immutability moves from convention to the storage layer while `cluster-rebuild` and fixture
  reseeding stay ordinary operations. Object-lock WORM retention was considered and rejected:
  retained test objects cannot be cleaned up in place. The two-credential split is deliberately
  the same shape Phase 5's Vault policies will need.
  — **Reversibility:** one-way for the *option* — object lock can only be enabled at bucket
  creation, so choosing versioning-only now forecloses WORM without recreating the bucket. The
  policy itself is reversible.

### Bootstrap and teardown

- **D-09:** **Make + shell driving the `helm` CLI.** One target per component, ordered by Make
  prerequisites, each `helm upgrade --install --wait` against a committed values file.
  Preserves Phase 1's standing fact — `make` is the only gate definition and CI calls `make`
  and nothing else — for infrastructure too. Rejected: Helmfile (a sixth pinned tool and a
  second place gates are defined) and Argo CD (bootstrap chicken-and-egg against a local-only
  repo). Hand-written readiness waits are expected where `--wait` is insufficient: CRD
  establishment before applying `Cluster` CRs, and CNPG primary election before Airflow's
  migration Job.

- **D-10:** **`make doctor` is fail-closed and `cluster-up` depends on it.** Blocks on: inotify
  `max_user_watches`/`max_user_instances` below target, free ext4 disk under budget, Docker not
  running, and kind/helm/kubectl off their pinned versions — each printing the exact remediation
  command. Advisory only where it genuinely cannot verify. Rationale: Phase 1's discovery was
  that four gates passed on broken input; a check that reports without blocking is a check
  people scroll past during a 10-minute build.

- **D-11:** **`docs/wsl/wslconfig.example` is committed** (memory / processors / swap, plus
  `sparseVhd=true` — WSL2's `ext4.vhdx` never returns deleted space to Windows, and disk is the
  binding constraint, not RAM). `doctor` asserts a **floor**, not exact equality, so a larger
  machine is never punished. Applying it stays a deliberate human act — it needs
  `wsl --shutdown` and lives on the Windows side.

- **D-12:** Phase 2's CI job is **offline: `helm template` both profiles → `kubeconform -strict`
  against Kubernetes 1.35.5 (with `-schema-location` entries for the CNPG and ingress CRDs) →
  two policy tests.** Test 1 sums container requests across the rendered CI manifests and fails
  if the total exceeds the 4 CPU / 16 GB runner budget, making success criterion 5's "sized for"
  claim mechanically true. Test 2 fails any container missing requests/limits — an unrequested
  pod is QoS `BestEffort` and is evicted first, i.e. precisely when its data is needed to
  explain the incident. No cluster in CI until Phase 11.

### Namespaces and credentials

- **D-13:** **Per-component namespaces:** `cnpg-system` (operator), `data` (both CNPG clusters
  + MinIO), `airflow` (chart components), `etl` (task pods and KPO pods), `ingress-nginx`.
  Chosen for the Phase 5 identity seam: Vault policies bind to
  `system:serviceaccount:<namespace>:<name>`, and PITFALLS #13 warns that the usual fix for a
  mismatch is to widen the Vault role, silently voiding least privilege. `etl` existing now, as
  the only place ETL runs, lets that role be written narrowly the first time. Also makes
  NetworkPolicy a real boundary later rather than a relabelling exercise.
  — **Reversibility:** costly — namespaces are cheap to create but moving a workload later
  invalidates every RBAC binding, service DNS name and (from Phase 5) Vault role that names it.

- **D-14:** MinIO's root and application credentials are **generated during `cluster-up` and
  live only in the cluster**. Values files reference secret *names* only; nothing is written to
  the working tree. Host tooling uses `make minio-creds` to read them back, so `mc` and tests
  never hold a stale copy. Credentials changing on every rebuild is a feature — nothing can
  quietly depend on a specific value. This is deliberately the exact shape Phase 5 replaces:
  same secret names, different source. CNPG generates and stores its own Postgres credentials,
  so both databases need no handling here. Rejected: a gitignored `.secrets/` file (a real
  credential at rest in the working tree, protected only by an ignore rule that a `git add -f`
  or a Docker build context can defeat, with gitleaks live since Phase 1).

- **D-15:** The CNPG `Cluster` CRs create **databases, an owner role and a least-privileged
  application role with grants — and nothing else.** Every schema and every object is Alembic's
  (Phase 3), keeping Phase 3's "`alembic upgrade head` against a throwaway PostgreSQL" criterion
  literally true and giving DDL exactly one home. A schema that exists in the cluster but in no
  migration is how the CI environment and the local one begin to disagree.

- **D-16:** The phase is proven by **`make cluster-verify`** — a `tests/e2e/cluster/` pytest
  suite run against the live cluster asserting each success criterion: both server versions and
  that they are two distinct CNPG `Cluster` resources with no shared storage; all five buckets
  reachable as `s3://bucket/key` **through boto3** (not `mc` — exercising the client §5 actually
  mandates); the four Airflow workloads Ready as separate deployments; and the deny-delete
  policy refusing a delete against `raw`. Re-runnable after any rebuild, and it becomes the
  regression net for every later phase that edits a values file.

### Claude's Discretion

The user made no "you decide" calls. Everything below is planner/researcher latitude within the
decisions above:

- Exact kubelet reservation numbers and `maxPods`. PITFALLS proposes
  `systemReserved`/`kubeReserved` of 500m/1Gi each, `evictionHard` at `memory.available: 500Mi`
  / `nodefs.available: 10%`, `maxPods: 60` — and flags these as **proposals, not measurements**.
  Size them against the documented `.wslconfig` floor from D-11.
- Per-component resource requests/limits, subject to D-12's two policy tests.
- Local registry container shape and lifecycle (it is a plain Docker container and therefore
  survives `kind delete cluster` on its own — the image cache does not need protecting by D-01).
- Ingress hostname scheme beyond the `*.localtest.me` convention.
- Whether `cluster-down` also prunes node containerd image stores (PITFALLS A3 notes that
  pruning the host daemon does not touch images already loaded into nodes, and this is the step
  everyone forgets).
- Plan decomposition. The roadmap's internal parallelism is 2a MinIO ‖ 2b analytical PG ‖
  2c Airflow PG → 2d Airflow (needs 2c only).

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Adjudicated research — the authority for this phase
- `.planning/research/SUMMARY.md` — stages **S1** (kind cluster + local registry) and **S2**
  (MinIO ‖ analytical PG ‖ Airflow PG → Airflow). Records that Vault is removed from this stage
  (deviation D3) and that both Helm values profiles are written now. Also the parallelization
  map (wave A: Phase 2 ‖ Phase 3) and the confidence caveats at lines 278 and 305.
- `.planning/research/PITFALLS.md` — **A1** (inotify exhaustion → the `doctor` preflight),
  **A2** (kind nodes lie about capacity; kubelet reservations; requests on every chart),
  **A3** (`ext4.vhdx` never shrinks; `sparseVhd`; per-node containerd stores), **A4** (what
  survives `kind delete cluster`; local-path PVs are node-bound), **A5** (mutable tags +
  `IfNotPresent`), **B10** (DAG distribution: git-sync rejected locally, hostPath recommended,
  `extraMounts` at creation time). Summary table rows **#10** and **#11**.
- `.planning/research/STACK.md` — every pinned version and the rejected alternatives with
  reasons: kind `v0.32.0` / `kindest/node:v1.35.5` (digest-pinned; **do not** take kind's
  default 1.36.1, which is outside Airflow 3.3.0's supported 1.30–1.35), Helm `4.2.3` with
  `3.21.3` documented as fallback, CNPG operator `1.30.0` / chart `0.29.0` / `cluster` chart
  `0.8.1` (**defaults to PG 16 — override explicitly**), MinIO chart `5.4.0` +
  `pgsty/minio:RELEASE.2026-08-04T00-00-00Z`, Airflow chart `1.22.0` with image override to
  `3.3.0` and `postgresql.enabled: false`, `kubeconform 0.8.0`.
- `.planning/research/ARCHITECTURE.md` — cluster topology and the component boundaries this
  phase instantiates.
- `.planning/research/FEATURES.md` — capability decomposition behind the INFRA requirements.

### Phase scope and requirements
- `.planning/ROADMAP.md` § "Phase 2: kind Cluster & Core Infrastructure" — goal, the five
  success criteria, and the plan guidance (internal parallelism, Vault exclusion,
  `kindest/node` pin, `postgresql.enabled: false`, local registry over `kind load`, sysctl
  bootstrap + `make doctor`, the SeaweedFS ADR, INFRA-10 gating Phase 11).
- `.planning/ROADMAP.md` § "Cheap-Now / Unrecoverable-Later Decisions by Phase" — rows **#10**
  and **#11** are decided in this phase.
- `.planning/REQUIREMENTS.md` — INFRA-01, INFRA-02, INFRA-03, INFRA-04, INFRA-05, INFRA-07,
  INFRA-09, INFRA-10, CICD-07 (lines 32–42, 207).
- `.planning/PROJECT.md` § Constraints — the kind mandate, two-Postgres topology, `s3://`
  addressing, raw immutability, CI runner sizing, and the ext4/`/mnt/c` filesystem rule.

### Master specification
- `README.md` §3.1 (kind mandated, Compose forbidden as workload platform), §4 (two physically
  separate PostgreSQL instances), §5 (`s3://bucket/path` addressing, MinIO→S3 swappable), §63
  (raw layer append-only), §77/§85 (non-root containers, per-workload requests and limits),
  §81 (no credential in Git, source, images, manifests or CI files), §90 (environment
  recreatable from the repository), §94 DoD items 1–5, 7, 109.

### Repository conventions established in Phase 1
- `Makefile` — the only place a gate is defined; CI calls `make` and nothing else. New
  infrastructure targets extend this file rather than adding a parallel mechanism.
- `docs/adr/0001-record-architecture-decisions.md` and `docs/adr/0000-template.md` — the MADR
  format the **SeaweedFS-as-MinIO-migration-target ADR** owed by this phase must follow.
  Next free number is **0006**.
- `tools/security/install_gitleaks.sh` — the established pattern for installing a pinned
  third-party binary (commit the per-platform SHA-256 as trust anchor; verify **before**
  executing). `kind`, `helm` and `kubeconform` installers should follow it, not re-invent it.
- `.planning/WINDOWS.md` — the broken-windows ledger. **#7 is live for this phase**: `.gsd/` is
  untracked and unignored and will be swept into a commit here unless `.gitignore` is fixed
  first. **#8** notes that `make check` names test paths explicitly, so `tests/e2e/` is silently
  uncollected until named — directly relevant to D-16.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets

- **`Makefile`** — `uv-guard` is the working template for D-10's version assertions: it compares
  an installed tool's version against a pinned constant and fails with the exact install command.
  `kind`, `helm` and `kubectl` checks in `doctor` should mirror its shape, not invent a new one.
- **`tools/security/install_gitleaks.sh`** — pinned-binary installer with a committed SHA-256
  trust anchor and verify-before-execute ordering (hardened after CR-03). Reuse for `kind`,
  `helm` and `kubeconform`.
- **`tests/policy/`** — the existing home for repository-invariant tests. D-12's requests-sizing
  and requests-present tests belong here; they are static assertions over rendered YAML and need
  no cluster.
- **`docs/adr/`** — MADR structure, `README.md` index, and `0000-template.md`.
- **`.github/workflows/ci.yml`** — the single workflow; it invokes `make` only. D-12's job must
  preserve that property (see `tests/policy/test_ci_invokes_make_only.py`).

### Established Patterns

- **`make` is the sole gate definition.** Local and CI gates cannot drift because CI calls
  `make install` and `make check`/`make ci` and nothing else. Infrastructure targets must join
  this, not sit beside it.
- **Gates enforce rather than advise, and are observed failing.** Phase 1 wrote tests that prove
  each gate rejects bad input (`gitleaks-selftest`, `tests/policy/`). D-10, D-12 and D-16 inherit
  that expectation — a check nobody has watched fail is not yet a gate.
- **Pinned versions live in exactly one place** and are asserted at runtime (`UV_REQUIRED_VERSION`).
  Chart and image versions should follow: one `versions.env`-style source, referenced by the
  Makefile and the values files rather than duplicated across them.
- **`make check` names test paths explicitly** — a new test directory is silently uncollected
  until named. This is the exact defect that made QUAL-07 partial (WINDOWS #8). `tests/e2e/cluster/`
  must be wired into a target deliberately, and must **not** be added to `make check`, which is
  contractually offline and cluster-free.

### Integration Points

Every infrastructure directory exists but is empty (`.gitkeep` only) — this phase fills them:

- `kind/` — **does not exist yet**; `kind/cluster.yaml` is the creation-time artifact carrying
  D-01, D-02, D-03, D-05 and the kubelet reservations.
- `helm/values/local/` and `helm/values/ci/` — the two profiles from D-06. The directory split
  already anticipates INFRA-10.
- `kubernetes/` — CNPG `Cluster` CRs (D-15), namespaces (D-13), the MinIO bucket/policy
  bootstrap Job (D-08), RBAC.
- `airflow/dags/` — the D-02 mount target. Stays empty until Phase 4.
- `airflow/config/` — Airflow configuration that is not chart values.
- `scripts/` — `doctor`, `cluster-up`/`down`/`rebuild` internals, credential generation (D-14).
- `docs/adr/` — ADR-0006 (SeaweedFS migration target) and any ADR recording D-01's disposability
  choice.
- `docs/wsl/` — **does not exist yet**; `wslconfig.example` from D-11.
- `tests/e2e/cluster/` — **does not exist yet**; D-16's suite.
- `Makefile` — `doctor`, `cluster-up`, `cluster-down`, `cluster-rebuild`, `cluster-verify`,
  `minio-creds`, `helm-lint`/`manifests`.

**No Python package changes.** `packages/dataplat` and `packages/csv-processor` are Phase 3's
territory and this phase must not touch them — the two phases run in parallel and share no files.

</code_context>

<specifics>
## Specific Ideas

- **`*.localtest.me` specifically**, because it resolves to 127.0.0.1 publicly and therefore
  needs no `/etc/hosts` edit on either the WSL or the Windows side — the setup path stays
  "clone and run".
- **boto3, not `mc`, in the verification suite** (D-16) — the point is to exercise the client
  §5 actually mandates, so the test fails if MinIO is reachable only through MinIO's own tooling.
- **Credentials rotating on every rebuild is desirable, not tolerated** (D-14) — it makes it
  impossible for anything to quietly depend on a specific value before Vault arrives.
- **The §4 database separation should be physical, not nominal** (D-03) — the two Postgres
  clusters cannot share a node, let alone storage.
- **Divergence between the two Helm profiles is capped at three named axes** (D-06) — replicas,
  resources, monitoring. Any fourth axis needs an argument.
- **Warn, don't fail, on rebuild wall-clock** (D-04) — deliberately *unlike* every other gate in
  this repo, because the measurement depends on network conditions the repository does not
  control, and a flaky gate teaches people to ignore gates.

</specifics>

<deferred>
## Deferred Ideas

- **Persisting cluster state via bound `extraMounts`** — the mounts are declared (D-01) but
  unbound. If rebuild time or accumulated state ever justifies it, adopting persistence is a
  values change. Revisit if `cluster-rebuild` drifts past its budget.
- **Object-lock WORM retention on `raw`** — rejected for Phase 2 (D-08) because retained test
  objects cannot be cleaned up in place. Genuinely worth revisiting at Phase 11, where
  INCR-07's rebuild-from-raw makes raw's integrity the capstone proof and the environment is
  ephemeral anyway.
- **NetworkPolicy between namespaces** — D-13 makes the namespace boundary real enough to
  enforce later; no policies are written in this phase. Natural companion to Phase 5's identity
  work.
- **CoreDNS rewrite so one S3 URL resolves identically inside and outside the cluster** —
  rejected in D-07 as a kind-specific customization with no production counterpart. Reconsider
  only if context-dependent endpoints actually cause a debugging problem.
- **Server-side `kubectl apply --dry-run=server` validation** — deferred to Phase 11 with the
  ephemeral-kind job (D-12). It catches CRD and admission-webhook errors no offline tool can.
- **`make clean-images` pruning each node's containerd store** — PITFALLS A3 flags this as the
  cleanup step everyone forgets and the reason disk keeps climbing after a "cleanup". Listed
  under Claude's discretion for this phase; if not built here it should become a ledger entry.

</deferred>

---

*Phase: 2-kind-cluster-core-infrastructure*
*Context gathered: 2026-08-11*
