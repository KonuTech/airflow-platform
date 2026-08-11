# Phase 2: kind Cluster & Core Infrastructure - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-08-11
**Phase:** 2-kind-cluster-core-infrastructure
**Areas discussed:** Host mounts & cluster state, Cluster access surface, Bootstrap & teardown shape, Namespaces & interim credentials

Four gray areas were offered; the user selected all four.

---

## Host mounts & cluster state

### Q1 — `kind delete cluster` destroys every PVC at once (PITFALLS A4)

| Option | Description | Selected |
|--------|-------------|----------|
| Disposable + timed rebuild | State declared throwaway; `make cluster-rebuild` a first-class timed operation; extraMounts declared but unbound so persistence stays a values change | ✓ |
| Persist Postgres + MinIO | extraMounts bind a WSL ext4 host dir at the local-path provisioner path; databases and buckets survive `kind delete` | |
| Persist only `raw` | Split along the immutability seam — `raw` persisted (§63, INCR-07), everything else rebuilt from it | |

**User's choice:** Disposable + timed rebuild
**Notes:** README §90 already demands rebuildability, so this converts PITFALLS A4 from a risk into a deliverable rather than adding work. Research's stated failure mode — "past ~15 minutes you will avoid doing it, and avoiding it is how environments rot" — is what Q4 addresses.

### Q2 — DAG delivery (PITFALLS B10); git-sync rejected locally

| Option | Description | Selected |
|--------|-------------|----------|
| Mount now, wire in Phase 4 | extraMount declared on every node at creation; both values profiles stay on the chart default until a real DAG exists | ✓ |
| Mount and wire it now | Also add the PV/PVC and `dags.persistence` wiring to values-local.yaml this phase; needs a placeholder DAG to prove it | |
| Baked image in both profiles | DAGs ship in the image everywhere; rebuild → push → restart for every edit | |

**User's choice:** Mount now, wire in Phase 4
**Notes:** Splits the decision along its cost line — the expensive half (cluster recreation) is paid now, the cheap half (a values change) waits for something to mount. `values-ci.yaml` stays on the baked-image path permanently so CI proves the production mechanism.

### Q3 — local-path PVs are node-bound; placement of the two CNPG clusters and MinIO

| Option | Description | Selected |
|--------|-------------|----------|
| Split the two databases | worker-1 = Airflow PG 17 + MinIO; worker-2 = analytical PG 18 alone; explicit nodeSelector on each | ✓ |
| One labelled data node | All three stateful workloads on worker-1 (`role=data`); worker-2 clear for Airflow and task pods | |
| Let the scheduler place them | No nodeSelector; accept `Pending` / `volume node affinity conflict` after the first eviction | |

**User's choice:** Split the two databases
**Notes:** Makes §4's "separation stays visible even inside one cluster" physical rather than nominal, and keeps the heaviest workload (COPY into PG 18) off the node also serving object storage.

### Q4 — What keeps `make cluster-rebuild` honest

| Option | Description | Selected |
|--------|-------------|----------|
| Timed, recorded, warns loudly | Per-stage timing, last run to a gitignored file, documented ~15 min budget, warn past it | ✓ |
| Hard-fail over budget | Non-zero exit past budget, consistent with every other gate in the repo | |
| Working target, no timing | `cluster-down && cluster-up` simply works; measurement deferred to Phase 11 | |

**User's choice:** Timed, recorded, warns loudly
**Notes:** Deliberately unlike every other gate in this repo. Wall-clock on a cold image cache measures the network rather than the repository, and a flaky gate teaches people to ignore gates. Per-stage numbers keep a regression attributable.

---

## Cluster access surface

### Q1 — How traffic reaches the cluster (`extraPortMappings` is creation-time only)

| Option | Description | Selected |
|--------|-------------|----------|
| ingress-nginx + extraPortMappings | Host :80/:443 → `ingress-ready` node; hostname routing under `*.localtest.me`; stable URLs across rebuilds | ✓ |
| extraPortMappings + NodePort | Fixed host ports straight to NodePorts; no extra chart, but every new service needs another creation-time mapping | |
| kubectl port-forward only | No creation-time commitment; forwards die on pod restarts and the s3:// endpoint story stays split | |

**User's choice:** ingress-nginx + extraPortMappings
**Notes:** `*.localtest.me` resolves to 127.0.0.1 publicly, so no `/etc/hosts` edit is needed on either WSL or Windows — the setup path stays "clone and run". One mechanism serves every service added through Phase 7.

### Q2 — How much of the access surface `values-ci.yaml` carries

| Option | Description | Selected |
|--------|-------------|----------|
| Same ingress, differ only on size | CI keeps ingress-nginx; profiles diverge on exactly three axes — replicas, resources, monitoring off | ✓ |
| No ingress in CI | Phase 11's E2E runs in-cluster over ClusterIP DNS; saves a pod, adds a divergence axis | |
| No ingress in CI, port-forward instead | E2E reaches services via port-forward from the runner; same extra divergence axis plus flakiness | |

**User's choice:** Same ingress, differ only on size
**Notes:** Each additional divergence axis is a bug class that appears only in CI, nine phases downstream of where it was introduced. The controller is ~100m/90Mi, cheap next to Airflow and two Postgres clusters.

### Q3 — Reconciling MinIO's two addresses with §5 swappability

| Option | Description | Selected |
|--------|-------------|----------|
| Two addresses, one config key | One `S3_ENDPOINT_URL` injected per context; never hardcoded; unsetting it resolves AWS | ✓ |
| One address everywhere via CoreDNS | Patch CoreDNS so the ingress hostname resolves in-cluster too; identical URL both sides | |
| In-cluster address only | Everything touching S3 runs inside the cluster; single address, container round-trip for ad-hoc inspection | |

**User's choice:** Two addresses, one config key
**Notes:** The address being context-dependent is a fact about the network; hiding it behind a kind-specific DNS customization with no production counterpart costs more than naming it.

### Q4 — Protecting `s3://raw/` (§63 append-only; object lock is creation-time only)

| Option | Description | Selected |
|--------|-------------|----------|
| Versioning + deny-delete policy | Versioning at creation plus an IAM policy denying delete/overwrite to the app credential; admin retains it | ✓ |
| Versioning + object lock retention | WORM retention so not even admin can remove a retained object before its window expires | |
| Convention plus a test | Plain versioned bucket; immutability is a code rule a test asserts | |

**User's choice:** Versioning + deny-delete policy
**Notes:** Object lock was rejected on a concrete cost — retained test objects cannot be cleaned up in place, so resetting `raw` would mean rebuilding the cluster. The two-credential split is deliberately the shape Phase 5's Vault policies will need. Deferred to Phase 11, where the environment is ephemeral anyway.

---

## Bootstrap & teardown shape

### Q1 — What drives the ordered install of five components

| Option | Description | Selected |
|--------|-------------|----------|
| Make + shell driving helm CLI | One target per component ordered by Make prerequisites; `helm upgrade --install --wait` per committed values file | ✓ |
| Helmfile | One declarative file with `needs:` ordering; idempotent and diffable; a sixth pinned tool | |
| Argo CD / GitOps | Controller reconciles from the repo; drift detection for free; bootstrap chicken-and-egg on a local-only repo | |

**User's choice:** Make + shell driving helm CLI
**Notes:** Preserves Phase 1's standing fact that `make` is the only gate definition and CI calls make and nothing else. Accepted cost: hand-written readiness waits where `--wait` is insufficient (CRD establishment, CNPG primary election).

### Q2 — `make doctor`'s enforcement stance

| Option | Description | Selected |
|--------|-------------|----------|
| Fail-closed, cluster-up depends on it | Blocks on inotify sysctls, free disk, Docker down, tool versions; prints exact remediation | ✓ |
| Advisory, cluster-up proceeds | Standalone diagnostic; never falsely blocks; warning scrolls past in a 10-minute build | |
| Split: hard blockers fail, tunables warn | Fail on impossible, warn on capacity; but inotify is exactly the tunable producing the multi-day mystery | |

**User's choice:** Fail-closed, cluster-up depends on it
**Notes:** Directly informed by Phase 1's discovery that four gates had passed on broken input. Research's framing: a preflight assertion turns inotify exhaustion from a multi-day mystery into a one-second error message.

### Q3 — Pinning the WSL VM via `.wslconfig`

| Option | Description | Selected |
|--------|-------------|----------|
| Commit the example, check a floor | `docs/wsl/wslconfig.example` with `sparseVhd=true`; doctor asserts a floor, not equality | ✓ |
| Pin exactly and assert it | Committed values become the contract; identical hardware for every measurement including Phase 4's U3 baseline | |
| Leave the VM alone | No guidance; capacity becomes a function of what Windows is doing | |

**User's choice:** Commit the example, check a floor
**Notes:** A floor means a larger machine is never punished and the repo documents the target without dictating the host. Applying it stays a deliberate human act — it lives on the Windows side and needs `wsl --shutdown`. `sparseVhd=true` matters independently: WSL2's `ext4.vhdx` never returns deleted space to Windows, and disk is the binding constraint.

### Q4 — What Phase 2's CI job validates

| Option | Description | Selected |
|--------|-------------|----------|
| Offline render + schema + a sizing test | helm template both profiles, kubeconform -strict against 1.35.5 with CRD schema locations, plus requests-sizing and requests-present policy tests | ✓ |
| Render + schema validation only | Literally what CICD-07 asks; sizing stays a review-time judgement | |
| Add ephemeral kind now | Server-side dry-run against a live API server; pulls Phase 11's job forward and taxes every PR | |

**User's choice:** Offline render + schema + a sizing test
**Notes:** Makes success criterion 5's "sized for a 4 CPU / 16 GB runner" mechanically true rather than asserted. The requests-present test exists because an unrequested pod is QoS `BestEffort` and evicted first — precisely when its data is needed to explain the incident.

---

## Namespaces & interim credentials

### Q1 — Namespace layout

| Option | Description | Selected |
|--------|-------------|----------|
| Per-component namespaces | `cnpg-system` / `data` / `airflow` / `etl` / `ingress-nginx` | ✓ |
| One `platform` namespace | Everything but the operator together; simplest, but a namespace-scoped Vault policy grants everything | |
| Split only Airflow from data | `airflow` + `data` + `cnpg-system`; loses the boundary Phase 5's negative test must prove | |

**User's choice:** Per-component namespaces
**Notes:** Chosen for a Phase 5 consequence, not a Phase 2 convenience: Vault policies bind to `system:serviceaccount:<namespace>:<name>`, and PITFALLS #13 warns the usual fix for a mismatch is to widen the Vault role, silently voiding least privilege. `etl` existing now as the only place ETL runs lets that role be written narrowly the first time.

### Q2 — Where MinIO credentials come from before Vault

| Option | Description | Selected |
|--------|-------------|----------|
| Generated at cluster-up, live only in the cluster | Random creds generated in-memory into K8s Secrets; values reference names only; `make minio-creds` reads back | ✓ |
| Generated once into a gitignored file | `.secrets/local.env` reused across rebuilds; friendlier loop; a real credential at rest in the working tree | |
| Developer-supplied `.env.local` | Human creates it from a committed example; doctor fails if missing; manual step in "clone and run" | |

**User's choice:** Generated at cluster-up, live only in the cluster
**Notes:** Credentials rotating on every rebuild is treated as a feature — nothing can quietly depend on a specific value. Same secret *names* as Phase 5 will use, different source, so the Vault retrofit is a configuration change. CNPG generates and stores its own Postgres credentials, so neither database needs handling here.

### Q3 — Where the CNPG CR stops and Alembic starts

| Option | Description | Selected |
|--------|-------------|----------|
| CR creates database + roles only | Databases, owner role, least-privileged app role with grants; every schema and object is Alembic's | ✓ |
| CR also creates the schemas | `postInitSQL` creates staging/warehouse/analytics/meta; visible immediately, but two owners of DDL | |
| CR creates the database only | Roles and grants deferred to migrations; least privilege slips into the phase meant to remove dev credentials | |

**User's choice:** CR creates database + roles only
**Notes:** Keeps Phase 3's first success criterion — `alembic upgrade head` against a throwaway PostgreSQL — literally true. A schema that exists in the cluster but in no migration is how the CI and local environments begin to disagree.

### Q4 — How the phase is proven done

| Option | Description | Selected |
|--------|-------------|----------|
| pytest suite under `make cluster-verify` | `tests/e2e/cluster/` asserts each criterion against the live cluster, including boto3 s3:// access and policy denial | ✓ |
| Shell assertions inside cluster-up | kubectl/psql/mc checks at the end of bootstrap; cannot re-verify without rebuilding | |
| Manual UAT against the criteria | Documented checklist; nothing catches the regression when Phase 5 or 7 edits a values file | |

**User's choice:** pytest suite under `make cluster-verify`
**Notes:** boto3 rather than `mc` is deliberate — it exercises the client §5 actually mandates, so the suite fails if MinIO is reachable only through MinIO's own tooling. Note WINDOWS #8: `make check` names test paths explicitly and is contractually offline, so `tests/e2e/cluster/` must be wired into its own target and must not join `make check`.

---

## Claude's Discretion

The user made no "you decide" calls; every question was answered with a concrete option. The
following were explicitly left to the planner/researcher within the decisions above:

- Exact kubelet reservation numbers and `maxPods` (PITFALLS flags its proposals as unmeasured)
- Per-component resource requests and limits, subject to the two CI policy tests
- Local registry container shape and lifecycle
- Ingress hostname scheme beyond the `*.localtest.me` convention
- Whether `cluster-down` prunes per-node containerd image stores
- Plan decomposition within the roadmap's 2a ‖ 2b ‖ 2c → 2d structure

## Deferred Ideas

- Binding the declared `extraMounts` to persist cluster state — revisit if rebuild time drifts
- Object-lock WORM retention on `raw` — genuinely worth revisiting at Phase 11 (INCR-07)
- NetworkPolicy between the namespaces created here — natural companion to Phase 5
- CoreDNS rewrite for a single S3 URL — only if context-dependent endpoints cause real pain
- Server-side `kubectl apply --dry-run=server` — Phase 11, with the ephemeral-kind job
- `make clean-images` pruning node containerd stores — build here or record in the ledger

No scope creep arose during discussion; every area stayed inside the phase boundary.
