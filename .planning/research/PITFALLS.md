# Pitfalls Research

**Domain:** Production-like local Kubernetes (kind) Apache Airflow ETL platform; first workload a metadata-driven universal CSV ingestion engine loading into PostgreSQL
**Researched:** 2026-08-11
**Confidence:** MEDIUM-HIGH (per-entry confidence tags below)

## How to read this document

This is the fourth research document. It deliberately does **not** repeat `STACK.md` (version
pins, rejected alternatives), `ARCHITECTURE.md` (component design, build order, the
`Source → RecordChunk → Publisher` seam, the Airflow↔pod contract) or `FEATURES.md` (REQ-ID
taxonomy, anti-features). Where those documents *name* a trap, this document supplies the
**mechanics of avoiding it**.

Every entry carries:

- **Severity** — `DATA CORRUPTION` > `REWORK` > `ANNOYANCE`
- **Retrofit cost** — cost of fixing it *after* the fact versus designing for it now
- **Phase** — which build phase must own the prevention (§92 numbering)
- **Confidence** — HIGH (official docs / reproduced upstream issue), MEDIUM (strong secondary
  evidence, version-checked), LOW (reasoned inference, flagged as such)

Sections are ordered by retrofit cost, highest first. Section A (environment) leads not because
it is the most dangerous but because everything else runs on top of it: a wrong foundation
invalidates every measurement taken above it.


---

## The fifteen decisions that are cheap today and expensive later

If the roadmap absorbs nothing else from this document, absorb this table. Each row is a decision
that costs minutes now and days-to-unrecoverable later. The "decide by" column is the **latest**
phase at which the decision can still be made cheaply.

| # | Decision | If deferred, the cost is | Severity | Decide by |
|---|---|---|---|---|
| 1 | Store a `hash_version` alongside every change hash (**C6**) | Changing the hash recipe invalidates all stored hashes; every dimension appears to change at once; history may be unrecoverable | DATA CORRUPTION | The first migration that stores a hash |
| 2 | Create SCD2 dimensions with a `btree_gist` **exclusion constraint** on `(business_key, validity range)` (**C7**) | Once overlapping intervals exist, the constraint cannot be added and every as-of query is silently wrong | DATA CORRUPTION | The dimension's creating migration |
| 3 | Run-scoped identity (`run_id`, `attempt`) on every staged and loaded row (**C5**) | Retrofitting means rewriting the loader and back-filling identity onto existing rows; duplicates cannot be attributed or removed | DATA CORRUPTION | Phase 5 (the vertical slice) |
| 4 | Dynamic Task Mapping expands over a **frozen manifest**, never a live listing (**B6**) | Reruns and backfills silently produce different work; §62/§67 claims become false | DATA CORRUPTION | Phase 5 (manifest) — before mapping exists |
| 5 | Stream and chunk in **records**, via one `csv.reader` over a `newline=""` text wrapper (**E1**) | Every record containing an embedded newline is corrupted; the checkpoint model (§38) must be redesigned | DATA CORRUPTION | Phase 5 |
| 6 | SCD corrections **recompute** a key's history from an ordered event log (**C8**) | In-place interval surgery is not idempotent; combined with at-least-once CDC this drifts permanently | DATA CORRUPTION | Phase 8 (design) / 9 (build) |
| 7 | Advance the watermark only from observed committed cursor values, **lagged** (**C10**) | Rows committed out of timestamp order are never seen again; only control totals can detect it | DATA CORRUPTION | Phase 8 |
| 8 | Business date comes from the **data**, never the clock or `logical_date` (**B7**) | Backfilled rows carry today's effective date, corrupting SCD2 history invisibly | DATA CORRUPTION | Phase 5 (the rule) |
| 9 | The processor is the **only** CSV parser — never `COPY … FORMAT csv` (**C3**) | The warehouse contains rows your validation never saw; all §19–§27 guarantees become decorative | DATA CORRUPTION | Phase 5 (the first load) |
| 10 | Decide PV persistence and put `extraMounts` in `kind/cluster.yaml` (**A4**, **B10**) | Adding them later requires recreating the cluster, destroying the state you wanted to keep | REWORK | Phase 1 |
| 11 | Kubelet reservations and `maxPods` in the kind config; requests/limits on every chart (**A2**) | The scheduler over-packs and the host OOM killer arbitrates; changing kubelet config means cluster recreation | REWORK | Phase 1 / 2 |
| 12 | Metric labels are bounded; unbounded identity lives in the metadata DB (**F2**) | Prometheus OOMs; dashboards, alerts and recording rules must be rewritten | REWORK | Phase 5 (the rule) |
| 13 | Explicit `namespace` + `service_account_name` on task pods, matched to the Vault role (**B5**, **D3**) | The usual "fix" is to widen the Vault role, silently voiding §81 least privilege | REWORK + security | Phase 3 / 4 |
| 14 | Single-writer publication via advisory lock, `ON CONFLICT` on the natural key (**C1**) | `MERGE` fails or duplicates under the concurrency §86 explicitly requires | DATA CORRUPTION | Phase 5 (shape) / 8 (hardening) |
| 15 | Fixtures are **generated from a seed**, not committed en masse (**G3**, **E6**) | Build contexts bloat, the secret scanner gets globally disabled, and the oversized-file memory test is impossible | REWORK | Phase 6 |

**Reading the pattern:** eleven of these fifteen are *"make the bad state unrepresentable"* rather
than *"remember to handle the bad case"*. That is the through-line of this whole document, and H4
argues it should be the roadmap's explicit design bias.


---

## ARCHITECTURE.md's three unvalidated assumptions, as pitfalls

ARCHITECTURE.md closes by naming three assumptions it could not validate, in risk order. Each is
converted here into something the roadmap can act on: a concrete failure mode, a detection signal,
a prevention, and a timeboxed experiment that settles it.

---

### U1. "A locally-built image can be pulled and run by `KubernetesPodOperator` on kind without registry friction"

**Verdict after this research: MEDIUM risk — the mechanism works, but three specific frictions are
near-certain and all are cheap to pre-empt.**

| Friction | Detection | Prevention |
|---|---|---|
| Image present on one node, absent on another (`kind load` is per-node) | `ImagePullBackOff` on some runs and not others; the failing pod is always on the same node | Local registry (STACK.md); or `kind load --nodes` to all nodes and verify with `crictl images` on each |
| Stale image served because the tag already exists (**A5**) | Fixed bug reappears; pod `.spec.containers[0].image` ≠ current git SHA | Immutable `:<git-sha>` tags; `imagePullPolicy: Always` against the local registry |
| Pull time exceeding `startup_timeout_seconds` (**B3**) | "Pod took too long to start" on the first task after a rebuild, passing on retry | Pre-pull in `make cluster-up`; raise the timeout to 300 s |

**Settle it in Phase 3, in under an hour:** build a trivial `csv-processor:<sha>` that prints its
own version, push to the local registry, run it via `KubernetesPodOperator` with
`do_xcom_push=True` writing `/airflow/xcom/return.json`, and assert the XCom contains the SHA you
built. That single test exercises the registry, the pull policy, the tag scheme, the sidecar (B2)
and the receipt contract at once, and it becomes the permanent smoke test for the platform.

---

### U2. "Vault Kubernetes auth works on kind without manual JWT-issuer overrides"

**Verdict after this research: LOW risk — largely resolved.** Vault has defaulted
`disable_iss_validation` to true since 1.9 (STACK.md pins major 2.x), because the Kubernetes
TokenReview API performs the same validation. The widely-cited `claim "iss" is invalid` error is a
Kubernetes-1.21-era artefact.

**What remains, in likelihood order (all in D3):** the token reviewer lacking
`system:auth-delegator`; an audience mismatch between the projected token and the role;
`kubernetes_host` copied from a Vault-outside-Kubernetes tutorial; and — most likely of all — the
role binding a ServiceAccount that `KubernetesPodOperator` does not actually use (B5).

**Settle it in Phase 4, in under half a day:** `make vault-bootstrap` creates the auth method,
policy, role and Kubernetes RBAC from one variable set; then run **both** tests — the `csv-processor`
SA in `etl` reads its own path (positive), and the `default` SA is denied (negative). If the
negative test is awkward to write, the identity model is not real yet, which is itself the finding.

**Residual risk to note:** A7's clock drift produces `permission denied` on valid tokens and will be
misdiagnosed as this assumption failing. Check `date` first.

---

### U3. "Streaming CSV parsing with per-chunk `COPY` hits acceptable throughput inside a kind pod's resource limits"

**Verdict: genuinely unvalidated — this is the one that needs an experiment, not an argument.**

Fully specified as **E7**, with pre-declared responses for each outcome. Two points worth repeating
because they determine whether the experiment measures the right thing:

- **Do not run it with `executemany`.** A row-at-a-time loader is 10–100× slower than `COPY`
  (C11), and measuring it would produce a false negative that changes the architecture for no
  reason.
- **Measure peak RSS as well as throughput.** The failure this assumption is really about is
  E6 — an implementation that is "streaming" in shape but accumulates somewhere — and that shows up
  in memory before it shows up in speed.

**Settle it in Phase 5**, record the baseline number in the repository, and treat a later 5×
regression as a bug rather than a mystery.


---

## A. kind / Docker / WSL2 — the foundation that silently invalidates everything above it

*These are cheap in Phase 1 and expensive later, because a cluster reshape means destroying and
recreating the cluster — trivial when it holds nothing, painful in Phase 8 when it holds a
seeded warehouse and a week of run history.*

---

### A1. inotify and file-descriptor exhaustion — the failure that appears in Phase 2, not Phase 1

**Severity:** REWORK (looks like random infrastructure flakiness; burns days)
**Retrofit cost:** CHEAP to prevent (two sysctls), EXPENSIVE to diagnose
**Phase:** 1 (cluster bootstrap), re-verified in 2
**Confidence:** HIGH — documented on kind's own known-issues page

**What goes wrong:** A 3-node kind cluster comes up fine with a handful of pods. You then add
Airflow (API server + scheduler + DAG processor + triggerer + N task pods), two CloudNativePG
clusters, MinIO, Vault, Prometheus, Grafana and an OTel collector — and pods start hanging in
`ContainerCreating`, kubelet logs `failed to create fsnotify watcher: too many open files`,
containerd restarts, and the DAG processor stops noticing file changes.

**Why it happens:** inotify limits are **per host uid, enforced by the host kernel** — they are
*not* namespaced per container. All three kind "nodes" are Docker containers sharing the one
WSL2 kernel, so the whole cluster draws on a single pool. Kubelet, containerd, every
ConfigMap/Secret projected volume, Prometheus's file-SD, Grafana's provisioning watcher and
Airflow's DAG-directory watching each consume instances/watches. Defaults are
`max_user_watches=8192` and `max_user_instances=128`; the latter is what actually runs out.

**Prevention — exact values, and the WSL-specific placement that people get wrong:**

```conf
# /etc/sysctl.d/99-kind.conf   (inside the WSL distro, not on Windows)
fs.inotify.max_user_watches   = 524288
fs.inotify.max_user_instances = 512
fs.file-max                   = 2097152   # prudent for this pod count; not mandated by kind
```

The WSL trap: **WSL2 does not reliably apply `/etc/sysctl.conf` or `/etc/sysctl.d/` at boot
unless systemd is enabled** (microsoft/WSL#4232). Two supported ways to make it stick:

```ini
# /etc/wsl.conf — option 1 (preferred): let systemd-sysctl apply it
[boot]
systemd=true

# /etc/wsl.conf — option 2: no systemd (requires WSL >= 0.67.6)
[boot]
command = "/sbin/sysctl -p /etc/sysctl.d/99-kind.conf"
```

Then `wsl --shutdown` from PowerShell — restarting the distro's shell is *not* enough — and
**verify**, do not assume:

```bash
sysctl fs.inotify.max_user_instances fs.inotify.max_user_watches fs.file-max
```

**Warning signs (escalating):** `kubectl get events -A | grep -i "too many open files"`; kubelet
or containerd restart counts climbing inside a node container; an edited DAG not being picked
up; Grafana/Prometheus in `CrashLoopBackOff` while everything else looks healthy.

**Do this:** add a preflight assertion to the cluster-bootstrap script that *fails loudly* if
the sysctls are below target. It converts a multi-day mystery into a one-second error message.

---

### A2. kind nodes lie about their capacity — the scheduler over-packs and the *host* OOM killer arbitrates

**Severity:** REWORK, occasionally DATA CORRUPTION (a Postgres backend killed mid-`COPY`)
**Retrofit cost:** MEDIUM — kubelet config lives in `kind/cluster.yaml`, so changing it means
recreating the cluster
**Phase:** 1 (kubelet reservations) + 2 (requests/limits on every chart)
**Confidence:** MEDIUM-HIGH — kind node containers are unconstrained Docker containers reading
the host's `/proc/meminfo`; the specific reservation numbers below are proposals, not measurements

**What goes wrong:** each kind node reports the *entire* WSL VM's CPU and memory as node
capacity. With 3 nodes on a 47 GB VM, Kubernetes believes it has ~141 GB allocatable and ~96
CPUs. It therefore schedules far more than physically fits. When the VM runs out, the **Linux
OOM killer inside WSL** — not Kubernetes eviction — decides who dies, by `oom_score`, which
favours large-RSS processes: your PostgreSQL or your CSV task pod, not the idle Grafana. You get
exit code 137 with no useful Kubernetes event, and because Kubernetes never saw pressure it
happily reschedules the pod into the same doomed situation.

**Why it is missed:** a 3-pod smoke test never approaches the limit. The lie is only exposed
under the full stack — i.e. exactly when you also added five other things, so attribution is hard.

**Prevention:**

1. **Pin the VM's size** so the environment is reproducible rather than a function of what
   Windows is doing, in `C:\Users\<you>\.wslconfig`:
   ```ini
   [wsl2]
   memory=32GB
   processors=16
   swap=8GB
   ```
   Commit this as `docs/wsl/wslconfig.example` — it is part of "recreate the environment from
   the repository" (§90), and it is currently invisible to the repo.
2. **Cap allocatable per node** so the scheduler stops triple-counting, via
   `kubeadmConfigPatches` in `kind/cluster.yaml`:
   ```yaml
   kubeadmConfigPatches:
     - |
       kind: KubeletConfiguration
       systemReserved: { cpu: "500m", memory: "1Gi" }
       kubeReserved:   { cpu: "500m", memory: "1Gi" }
       evictionHard:   { "memory.available": "500Mi", "nodefs.available": "10%" }
       maxPods: 60
   ```
   `KubeletConfiguration` patches are *not* affected by the kubeadm `v1beta3`/`v1beta4` churn
   STACK.md flags for `InitConfiguration` — that caveat applies to the kubeadm API group only.
3. **Requests on everything, from the first chart.** README §85 asks this of ETL workloads; the
   pitfall is that the *infrastructure* charts get installed without requests. A pod with no
   request is QoS `BestEffort` and is evicted first — meaning Prometheus and the OTel collector
   die at precisely the moment you need their data to explain the incident.

**Warning signs:** restart counts on components you never touched; exit code 137 with an empty
`lastState` message; `dmesg -T | grep -i oom` inside WSL showing kills with no matching
Kubernetes event; `kubectl describe node | grep -A6 "Allocated resources"` summing to far more
than the machine has.

---

### A3. WSL2's `ext4.vhdx` never shrinks — disk is the real ceiling, not RAM

**Severity:** REWORK (hard stop: pods evicted, builds fail, Windows `C:` fills)
**Retrofit cost:** CHEAP now (one `.wslconfig` flag plus hygiene scripts), painful mid-project
**Phase:** 1
**Confidence:** MEDIUM-HIGH — dynamic-VHD growth without automatic reclaim is long-standing and
documented; `sparseVhd` availability is WSL-version dependent, so verify on your build

**What goes wrong:** every image build, every `kind load`, every containerd layer on every node
writes into the distro's virtual disk. The VHDX grows and **never returns space to Windows when
you delete things inside Linux**. Six weeks of iterating on a ~2 GB Airflow image plus a
`csv-processor` image plus fixture corpora leaves tens of GB of dead space. Kubernetes then
evicts pods for `DiskPressure` and Docker builds fail with `no space left on device` — while
`docker system df` claims plenty is free.

**Why it is missed:** the failure is *asymmetric*. Cleaning up inside Linux fixes the Kubernetes
symptom but not the Windows symptom, so the two get diagnosed separately and neither
investigation finds the cause.

**Prevention:**

- Enable sparse VHD so deletes are punched through (WSL 2.0+/recent Windows 11):
  ```ini
  [experimental]
  sparseVhd=true
  ```
  or per-distro `wsl --manage <distro> --set-sparse true` (distro must be stopped).
- Use the **local registry** STACK.md already chose. Beyond speed there is a disk argument:
  `kind load` materialises a full copy of the image in *each* node's containerd store — a 2 GB
  image costs 6 GB on a 3-node cluster, per tag, forever.
- Add `make clean-images` that prunes the host daemon **and each node's containerd**
  (`docker exec <node> crictl rmi --prune`). Pruning the host daemon does not touch images
  already loaded into nodes; this is the step everyone forgets and the reason disk keeps
  climbing after a "cleanup".
- Budget explicitly. STACK.md flags disk as the binding CI constraint; it is also the binding
  *local* constraint.

**Warning signs:** `df -h /` in WSL diverging from `docker system df`; `DiskPressure=True` or a
`node.kubernetes.io/disk-pressure` taint; DaemonSet pods evicted first; the Windows
`ext4.vhdx` file far exceeding `du -sh /` inside the distro.

---

### A4. What survives `kind delete cluster`: nothing — and local-path PVs are node-bound

**Severity:** DATA CORRUPTION (of test data and run history — still a real loss)
**Retrofit cost:** CHEAP in Phase 1 (one `extraMounts` block), EXPENSIVE in Phase 8 (adding it
requires recreating the cluster, destroying exactly what you were trying to protect)
**Phase:** 1 — a decision that must be made **before Phase 2 deploys anything stateful**
**Confidence:** HIGH — kind ships Rancher local-path-provisioner as the default `standard`
StorageClass, backed by host paths inside the node container

**Two distinct traps:**

1. **Everything is ephemeral.** PVC data lives at `/var/local-path-provisioner/...` *inside the
   node container*. `kind delete cluster` removes the containers, so both PostgreSQL instances,
   MinIO's buckets, Vault's storage and Prometheus's TSDB vanish together. Everyone eventually
   runs `kind delete && kind create` to fix an unrelated problem — and does it at 11pm in Phase 9.
2. **PVs are pinned to one node.** local-path uses `volumeBindingMode: WaitForFirstConsumer` plus
   node affinity. If a pod is rescheduled to a different worker (node restart, drain, eviction),
   the PVC cannot follow and the pod sits `Pending` with
   `node(s) had volume node affinity conflict`. It looks like a scheduler bug; it is not.

**Prevention — choose one deliberately and write the choice down:**

- **Option A (recommended here): declare cluster state disposable.** Make `make cluster-rebuild`
  a first-class, *timed*, scripted operation: recreate cluster → install charts → restore Vault →
  reseed MinIO from committed fixtures → run migrations → smoke DAG. README §90 asks for
  rebuildability anyway, so this converts a risk into a DoD item. Measure it: if rebuild takes
  more than ~15 minutes you will avoid doing it, and avoiding it is how environments rot.
- **Option B: persist selectively** via kind `extraMounts` binding a host directory **on WSL
  ext4** (never `/mnt/c` — PROJECT.md's measured 50–60× small-file penalty applies to Postgres
  and MinIO far worse than to source files) into each node at the provisioner path. Cost: the
  "reproducible from the repository" story weakens and stale state starts masking bugs.
- Either way, **pin stateful pods to a labelled node** with `nodeSelector` so trap 2 cannot fire.
  Give the two CNPG clusters and MinIO explicit placement rather than letting the scheduler choose.

**Warning signs:** `Pending` with `volume node affinity conflict`; a CNPG cluster that recovers
on one machine but not another; a "works after I recreated the cluster" report nobody can
reproduce.

---

### A5. Mutable image tags + `IfNotPresent` = you are testing yesterday's code

**Severity:** REWORK (hours chasing a bug you already fixed)
**Retrofit cost:** CHEAP now; later the tag scheme touches Helm values, the
`KubernetesPodOperator` call, CI and the registry simultaneously
**Phase:** 1–3
**Confidence:** HIGH

**What goes wrong:** you build `csv-processor:dev`, load it, run the DAG, and see the *old*
behaviour. The tag already exists on the node, so `imagePullPolicy: IfNotPresent` (the default
for any tag other than `latest`) skips the pull. Worse, with `kind load` on a 3-node cluster one
node may have the new image and two the old — so the bug is **intermittent by node**, which is
the most confusing failure mode in local Kubernetes development.

**Prevention:** immutable `:<git-sha>` (or digest) tags for both images, produced by the build
script and injected into Helm values *and* the `KubernetesPodOperator` `image` argument. Never
reference `:latest`/`:dev` from a manifest. With the local registry, set `imagePullPolicy:
Always` on the task-pod image — a digest pull from `localhost:5001` is essentially free and
eliminates the class entirely. Have the DAG **log the image tag it launched**, and add
`make image-sha`.

**Warning signs:** behaviour differing between reruns of the same task; the finished pod's
`.spec.containers[0].image` not matching `git rev-parse --short HEAD`.

---

### A6. Docker Hub anonymous pull limits — a CI failure that looks like a network flake

**Severity:** ANNOYANCE locally; REWORK in CI (flaky red builds destroy trust in the suite)
**Retrofit cost:** CHEAP
**Phase:** 1 locally, 10 in CI
**Confidence:** MEDIUM — Docker has tightened unauthenticated limits repeatedly; treat the
*existence* of a low anonymous limit as certain and re-check the current figure before quoting it

**What goes wrong:** `apache/airflow` lives on Docker Hub and GitHub-hosted runners share
outbound NAT addresses with an enormous number of other jobs, so anonymous pulls hit
`429 toomanyrequests` unpredictably. Locally, `kind create` (pulling `kindest/node`) plus a full
chart install can also trip it.

**Prevention:** authenticate in CI (`docker/login-action` with a Docker Hub token — even the
free tier's authenticated limit is far higher) **and** mirror the base images you depend on into
GHCR once, pinned by digest. Locally, extend the registry container from STACK.md's pattern into
a **pull-through mirror for `docker.io`** via `containerdConfigPatches`, so the 2 GB Airflow base
is fetched once per machine rather than once per cluster rebuild — which directly reduces the
cost of A4 Option A.

**Warning signs:** `ImagePullBackOff` whose event text contains `toomanyrequests`; a step that
fails then passes on retry, at roughly the same point in the job.

---

### A7. WSL2 clock drift after the host sleeps — x509 and token failures that "fix themselves"

**Severity:** ANNOYANCE-to-REWORK (routinely misdiagnosed as a Vault or RBAC bug)
**Retrofit cost:** CHEAP
**Phase:** 1, but it *manifests* in Phase 4 (Vault)
**Confidence:** MEDIUM — much improved in recent WSL builds but still reported; verify
empirically rather than assuming immunity

**What goes wrong:** you close the laptop lid; on resume the WSL2 VM's clock is behind wall time.
Every short-lived credential in this stack is time-sensitive: kubelet client certificates,
**bound ServiceAccount tokens** (whose `iat`/`exp`/`nbf` are exactly what Vault's Kubernetes auth
validates), Vault leases, TLS handshakes. Symptoms: `x509: certificate has expired or is not yet
valid`; Vault returning `permission denied` for an obviously valid token; an API server that
refuses connections until you do something unrelated and it "starts working again".

**Prevention:** check `date` in WSL against Windows *before* blaming your code. `sudo hwclock -s`
resyncs immediately. Durably: rely on WSL's resume-time sync on a current build, or run a time
daemon. Then put a **`make doctor`** target in the repo asserting, in one shot: clock skew < 5 s,
the A1 sysctls, free disk (A3), the expected kube context (A8) and registry reachability. Every
"the platform is broken this morning" should start there; this target repays itself within a week.

---

### A8. Stale kubeconfig contexts — and scripts that act on `current-context`

**Severity:** ANNOYANCE, with a tail risk of acting on the wrong cluster
**Retrofit cost:** CHEAP now; retrofitting `--context` into 40 scripts later is tedious
**Phase:** 1
**Confidence:** HIGH

**What goes wrong:** `kind delete` + `kind create` with the same name produces a **new CA and a
new random host port**, but the old `kind-airflow-platform` entry can linger in `~/.kube/config`
(or in a merged `KUBECONFIG` list) pointing at the dead port with the dead CA. `kubectl` reports
`connection refused` or `x509: certificate signed by unknown authority`, and the natural reaction
— recreating the cluster again — does not help. Separately, any script that omits `--context`
operates on whatever `current-context` happens to be, which is how a teardown script eventually
runs somewhere it should not.

**Prevention:** run `kind export kubeconfig --name airflow-platform` after every create; have the
teardown script explicitly delete the context, cluster and user entries. **Every** script and
Makefile target passes `--context kind-airflow-platform`, defined once as a variable, and refuses
to run if `kubectl config get-contexts` does not list it. In CI, set a job-scoped
`KUBECONFIG=$RUNNER_TEMP/kubeconfig`.

---

### A9. cgroup v2 and kernel-module assumptions on WSL2

**Severity:** REWORK if it bites (cluster will not start at all)
**Retrofit cost:** CHEAP — it fails immediately and loudly, unlike A1/A2
**Phase:** 1
**Confidence:** MEDIUM — kind documents this class of WSL2 failure; on kernel 6.18 with a current
Docker the default path is usually fine

**What goes wrong:** kind's canonical WSL2 failure is `unable to start container process: error
adding pid to cgroups`, from inadequate cgroup delegation. A second class is a **custom WSL
kernel** missing modules kube-proxy needs (`ip_tables`/`nf_tables`, `br_netfilter`, conntrack) —
Microsoft's stock kernel carries them; hand-rolled ones often do not.

**Prevention:** stay on the stock Microsoft WSL kernel; enable systemd in `/etc/wsl.conf` (which
also fixes A1's persistence); if the cgroup error appears, follow kind's documented WSL cgroup v2
workaround rather than improvising. Record the exact kernel and Docker versions in the repo
(kernel 6.18.33.2-microsoft-standard-WSL2, Docker 29.7.2) so a future "it broke after a Windows
update" has a reference point.

**Warning signs:** `kind create` failing during `kubeadm init`; kube-proxy `CrashLoopBackOff`
with iptables errors; Services that resolve but never connect.


---

## B. Airflow 3.x on Kubernetes — chart, pod operator, and mapping traps

*Second-highest retrofit cost. The Helm values file, the DAG-distribution mechanism and the
expansion-input contract are all things that get "decided" implicitly in Phase 3 and are painful
to change in Phase 8 once dozens of DAG runs exist.*

---

### B1. Pod amplification — KubernetesExecutor **plus** KubernetesPodOperator is two pods per task

**Severity:** REWORK (capacity planning is wrong by 2×, and by 2×N under mapping)
**Retrofit cost:** MEDIUM — changes pool sizing, resource requests and possibly the executor choice
**Phase:** 3, sized properly before 8 (dynamic mapping)
**Confidence:** HIGH — this is inherent to the two mechanisms, not a bug

**What goes wrong:** with `KubernetesExecutor`, every task instance runs in its own **worker
pod**. That worker pod then runs `KubernetesPodOperator`, which creates a **second pod** to do
the actual work, and the worker pod sits idle-but-resident streaming logs until it finishes. So
each unit of ETL costs two pods, two images pulled, two sets of resource requests. Fan out over
50 files with dynamic task mapping and you have asked kind for 100 concurrent pods, against
`maxPods` (default 110/node) and against the very real memory ceiling from A2.

**Prevention:**

- Size pools against **pods**, not tasks. Set an Airflow **pool** (e.g. `csv_ingest`, 8 slots)
  and use it on the mapped task; this is the only reliable throttle, since `max_active_tis_per_dag`
  does not know about the second pod.
- Give the *worker* pod a deliberately tiny request (it does nothing but poll and stream logs) —
  the chart's default worker resources are far too generous for this pattern. Put the real
  requests on the KPO pod via `container_resources`.
- Set `AIRFLOW__CORE__PARALLELISM` and the mapped task's `max_active_tis_per_dag` explicitly and
  low (start at 4) on a local cluster. Raise only after measuring.
- Reconsider per-environment: STACK.md's `LocalExecutor` in CI avoids the amplification entirely,
  which is a second reason that choice is right.

**Warning signs:** `kubectl get pods -n airflow --watch` showing pairs; tasks queued for minutes
with an empty scheduler log; nodes at `maxPods`; a mapped task where wall time scales worse than
linearly with map size.

---

### B2. The XCom sidecar has four separate ways to fail — and it is on your critical path

**Severity:** REWORK (blocks the Airflow↔pod receipt contract ARCHITECTURE.md depends on)
**Retrofit cost:** CHEAP if designed for; EXPENSIVE if the receipt contract is built on assumptions
**Phase:** 3 (prove it), 5 (depend on it)
**Confidence:** HIGH for mechanics (provider docs), MEDIUM for the sidecar image/override names —
verify against the exact `apache-airflow-providers-cncf-kubernetes` version you pin

**Mechanics:** with `do_xcom_push=True`, KPO injects an `emptyDir` at `/airflow/xcom` and an
extra container named `airflow-xcom-sidecar` whose only job is to stay alive after the base
container exits so the operator can read `/airflow/xcom/return.json`. Four failure modes:

1. **The file must exist and must be valid JSON.** If the base container succeeds but writes
   nothing, the task **fails** — a green ETL run reported as red. Make writing the receipt the
   final, unconditional statement of the container entrypoint (write it in a `finally`), and
   have it be valid JSON even in the failure path.
2. **The sidecar pulls its own image** (historically `alpine`, from Docker Hub). That is a second
   registry dependency on the critical path of every task — see A6. Newer provider versions
   expose an override (`xcom_sidecar_container_image`); set it to an image in your local
   registry, or accept an occasional `ImagePullBackOff` on the *sidecar* while the real container
   is fine, which is a maximally confusing symptom.
3. **Pod Security Standards / non-root.** README requires non-root containers. The default
   sidecar spec may not satisfy a `restricted` namespace policy; the provider exposes
   `xcom_sidecar_container_security_context` for exactly this. If you apply PSS labels to the
   task namespace (you should), test the sidecar under them in Phase 3, not Phase 8.
4. **`do_xcom_push` defaults differ by class.** `BaseOperator` (Task SDK) defaults it to `True`;
   `KubernetesPodOperator` has historically defaulted to `False`. Do not rely on the default —
   pass it explicitly, and assert in a test that the operator you construct has the value you think.

Combine with the constraint ARCHITECTURE.md already verified — **XComs are pushed only for tasks
reaching `SUCCESS`** — and the design rule follows: the receipt is a *confirmation*, never the
only record. The pod must have already written its outcome to the metadata database (or MinIO)
before it exits; XCom carries an identifier, not state.

**Warning signs:** tasks failing with `IOError`/`FileNotFoundError` referencing
`/airflow/xcom/return.json`; a pod whose base container is `Completed` while the pod stays
`Running`; `ImagePullBackOff` on a container named `airflow-xcom-sidecar`.

---

### B3. `startup_timeout_seconds` is 120 by default — a 2 GB image pull on kind can exceed it

**Severity:** ANNOYANCE that masquerades as a flaky test
**Retrofit cost:** CHEAP
**Phase:** 3
**Confidence:** MEDIUM-HIGH — the default has been 120 s across recent provider versions; confirm
in the pinned version

**What goes wrong:** the first task after a cluster rebuild (or after an image tag change) fails
with *"Pod took too long to start"* while the pod is, in fact, healthily pulling. Retries then
succeed because the image is now cached — producing a test suite that fails only on the first run
of the day and passes on rerun. This is precisely the flakiness that erodes confidence in E2E.

**Prevention:** raise `startup_timeout_seconds` to 300–600 for tasks that may pull; **and** remove
the cause by pre-pulling. On kind, the local registry (STACK.md) plus a warm-up step in
`make cluster-up` that pulls both images onto every node makes the timeout academic. In CI, load
images before the DAG ever runs. Also distinguish `startup_timeout_seconds` (waiting for
`Running`) from `execution_timeout` (the task's own budget) — conflating them produces either a
hair-trigger or an infinite hang.

---

### B4. Pod cleanup destroys your evidence — decide the policy before you need it

**Severity:** REWORK (a failure you cannot post-mortem is a failure you will reproduce)
**Retrofit cost:** CHEAP
**Phase:** 3, revisited in the runbook work (§89)
**Confidence:** HIGH

**What goes wrong:** KPO's `on_finish_action` (which replaced the older `is_delete_operator_pod`)
defaults to deleting the pod. A task fails, you open `kubectl describe pod` — and there is no
pod. Meanwhile `keep_pod` fills a small cluster with `Completed` pods, each still holding its
emptyDir, and interacts badly with A3.

**Prevention:** set `on_finish_action="delete_succeeded_pod"` — successes are cleaned up,
failures are preserved for inspection. Keep `get_logs=True` (default) so pod stdout is copied
into the Airflow task log *before* deletion; that copy is the only durable record once the pod is
gone. Add a scheduled cleanup for pods older than N hours so preserved failures do not accumulate
forever. Crucially, this reinforces the ARCHITECTURE rule: **the container's own structured
outcome must land in PostgreSQL/MinIO, not only in stdout** — because logs are best-effort and
pods are not.

**A related trap:** log streaming from the Kubernetes API can break on long-running silent
containers, after which the task appears hung even though the pod is progressing. Emit a
heartbeat log line per chunk from the processor (you want per-chunk progress logging anyway for
§38 checkpointing); silence is indistinguishable from death.

---

### B5. Namespace, ServiceAccount and RBAC do not propagate the way people assume — and Vault identity depends on it

**Severity:** REWORK, and a **security** issue if solved by over-permissioning
**Retrofit cost:** MEDIUM — it entangles the Helm values, the RBAC manifests, the Vault role
binding and the DAG code at once
**Phase:** 3 (mechanics) and 4 (identity)
**Confidence:** MEDIUM-HIGH — RBAC semantics are certain; the exact defaults changed recently
(the `@task.kubernetes` decorator's default namespace became `None`, resolving to the cluster
namespace when `in_cluster=True`), so pin and verify

**What goes wrong:** the KPO pod does **not** inherit the worker's ServiceAccount. It gets
`default` in whatever namespace it lands in, unless you set `service_account_name`. Since Vault's
Kubernetes auth binds a role to `(namespace, serviceaccount)`, the pod then presents the wrong
identity and Vault returns `permission denied` — which reads like a Vault misconfiguration and
gets "fixed" by widening the Vault role to `bound_service_account_names: ["*"]`, destroying the
least-privilege property README §81 requires and §81.12 tests for.

Second half of the trap: if you launch task pods into a *different* namespace than Airflow (you
should — it makes NetworkPolicy and quota boundaries meaningful), the chart's RBAC only covers
the Airflow namespace, so pod creation fails with a 403 from the worker's SA.

**Prevention:** make all four explicit and colocated in one place:

| Thing | Set it to | Where |
|---|---|---|
| `namespace` | `etl` (a dedicated namespace) | KPO default args |
| `service_account_name` | `csv-processor` | KPO default args |
| Role/RoleBinding for `pods`, `pods/log`, `pods/exec` in `etl` | granted to the Airflow worker SA | your own manifest, not the chart |
| Vault role `bound_service_account_names` / `_namespaces` | `csv-processor` / `etl` | Vault config (Phase 4) |

Write a negative test early (§81.12): a pod running as `default` in `etl` must be **denied** by
Vault. If that test is hard to write, the identity model is not real yet.

---

### B6. Dynamic Task Mapping over a *live* listing breaks determinism, replay and backfill

**Severity:** DATA CORRUPTION risk / REWORK — this violates §67 determinism and §62 replayability
**Retrofit cost:** **EXPENSIVE.** Changing where the expansion input comes from changes the DAG
shape, the metadata schema and every historical run's reproducibility
**Phase:** decide in 5, implement no later than 8
**Confidence:** HIGH on the mechanism; MEDIUM on Airflow 3's exact re-expansion state handling

**What goes wrong:** the natural first implementation is

```python
files = list_new_files_in_minio()      # a task that lists the bucket right now
process.expand(assignment=files)
```

Now clear and rerun the task a week later, or backfill: the listing returns a *different* set,
the map length changes, previously-existing mapped task instances are marked `REMOVED`, and new
indices appear. The run is no longer a reproduction of the original — it is a new run wearing the
old run's ID. Every downstream claim about replayability (§62), backfill safety (§34) and
deterministic processing (§67) is now false, and nothing anywhere reports an error.

**Prevention — the fix is architectural and cheap only if done early:**

1. The **file manifest (§41) is the expansion input.** A discovery task writes the manifest rows
   to the metadata DB inside a transaction, keyed by `(dag_id, run_id)`, and *returns only
   identifiers*. The mapped task expands over `manifest_entry_id`s read back from that frozen
   manifest.
2. Re-running the mapped task alone therefore re-expands **identically**. Re-running discovery is
   an explicit, separate, logged act.
3. This also solves XCom pressure: the expansion input is a list of integers or short keys, not a
   list of file-metadata dicts. ARCHITECTURE's AP5 ("no data through XCom") applies with double
   force to expansion inputs, because Airflow stores the expansion input *and* one XCom row per
   map index.
4. Cap the map length deliberately. `max_map_length` defaults to 1024
   (`AIRFLOW__CORE__MAX_MAP_LENGTH`); exceeding it fails the *expansion*, i.e. the whole run, not
   a single file. Batch the manifest into chunks of N files per mapped task so a 5,000-file day
   is 50 tasks of 100, not an instant failure. Doing this from the start also fixes ARCHITECTURE's
   AP6 ("one mapped task per file, uncapped").

**Warning signs:** mapped task instances in `REMOVED` state; a cleared task producing a different
number of map indices; run duration varying wildly for "the same" run; `Map length … exceeds
maximum` in the scheduler log.

---

### B7. Airflow 3 backfills interact badly with anything that reads the clock or the bucket

**Severity:** DATA CORRUPTION (data written to the wrong partition/effective date)
**Retrofit cost:** EXPENSIVE — effective-dating decisions propagate into SCD2 history
**Phase:** 8 (backfill) but **constrained in 5**
**Confidence:** MEDIUM — mechanics verified in ARCHITECTURE.md against Airflow 3.1.x; re-verify
against 3.3.0 before relying on specific field names

ARCHITECTURE.md establishes the sharp edge: asset- and REST-triggered Airflow 3 runs have
`logical_date = None` and no data interval, so touching `logical_date`/`data_interval_*` raises
`KeyError`. The *pitfall* that follows, and that the roadmap must plan for, is the temptation to
"fix" it by falling back to `datetime.now()`. That single line silently makes every backfilled
row carry today's effective date, corrupting SCD2 validity intervals (§57, §61) in a way that is
invisible until someone asks a historical question months later.

**Prevention:** the business date comes from **the data**, in priority order — filename mask
(§8) → control file (§43) → an explicit DAG-run `conf` parameter → declared dataset default. Never
from the clock, and never from `logical_date` in an asset-triggered DAG. Encode this as one
function, `resolve_business_date(assignment) -> date`, that **raises** rather than defaulting, and
unit-test the raise. Then: backfill and normal runs use the identical code path (§33 forbids a
bypass), and the only difference between them is the manifest they were handed.

**Warning sign to build in:** a data-quality assertion that no batch's business date is more than
N days from its file's parsed date, run as part of the pipeline, not as an afterthought.

---

### B8. Fernet key and API/webserver secret key regenerated on every `helm upgrade`

**Severity:** ANNOYANCE-to-REWORK; DATA LOSS of stored credentials in the worst case
**Retrofit cost:** CHEAP now, painful after you have encrypted Variables you cannot decrypt
**Phase:** 2
**Confidence:** MEDIUM-HIGH — the mechanism is well documented; ARCHITECTURE.md already names
this as AP12, so this entry is the *mechanics*

**Mechanics of doing it right:** do not let Helm generate these. Create the Secrets out of band,
once, and reference them by name so the chart never templates a fresh value:

```bash
kubectl -n airflow create secret generic airflow-fernet-key \
  --from-literal=fernet-key="$(python -c 'from cryptography.fernet import Fernet;print(Fernet.generate_key().decode())')"
kubectl -n airflow create secret generic airflow-api-secret-key \
  --from-literal=secret-key="$(openssl rand -hex 32)"
```

then set the corresponding `*SecretName` values (`fernetKeySecretName`, and the equivalent for the
API/webserver secret key — **the value name changed in the Airflow 3 line as `[webserver]
secret_key` moved toward `[api] secret_key`; check the chart 1.22.0 `values.yaml` rather than
copying a 2.x blog post**). Store both in Vault as the source of truth and document their
regeneration in the runbook.

**The nuance worth knowing:** because this project resolves Connections through the **Vault
secrets backend**, the Fernet key protects much less than usual — Connections are not in the
Airflow DB at all. What it still protects is encrypted Variables and any legacy connection rows.
The *secret key* is more disruptive: rotating it invalidates sessions and produces logout/CSRF
loops that look like an auth bug. Knowing which one causes which symptom saves an afternoon.

**Warning signs:** `cryptography.fernet.InvalidToken` in scheduler/API logs after an upgrade;
being logged out on every page navigation; a `helm upgrade` diff showing the secret's `data`
changing when you changed something unrelated.

---

### B9. Migration job races and version skew between chart components

**Severity:** REWORK (a half-migrated metadata DB is genuinely unpleasant to recover)
**Retrofit cost:** CHEAP
**Phase:** 2
**Confidence:** MEDIUM-HIGH — behaviours confirmed in apache/airflow chart issues (#27561,
#40573); exact defaults must be read from chart 1.22.0's `values.yaml`

Three distinct traps:

1. **The migration Job is immutable.** With `migrateDatabaseJob.useHelmHooks: false` (which you
   are told to set for ArgoCD/Flux/Terraform), `helm upgrade` tries to patch an existing Job and
   fails, because Job specs are immutable. Locally, keep `useHelmHooks: true` — you are running
   plain `helm upgrade`, not GitOps — and if you ever switch, delete the Job before upgrading.
2. **Do not disable `waitForMigrations`.** The scheduler and API server ship init containers that
   block until the DB schema is current. People disable them to "speed up" installs and get a
   scheduler `CrashLoopBackOff` complaining about the alembic revision — then blame the database.
3. **Version skew is the one that silently corrupts.** STACK.md establishes that chart 1.22.0
   ships `appVersion` 3.2.2 while you want 3.3.0, so a tag override is required. Override it at
   `defaultAirflowTag` (global) rather than per-component `images.*.tag`, otherwise the migration
   Job can run a *different* Airflow version than the scheduler — migrating the schema to one
   version while another version's code reads it. Add a CI/`make doctor` assertion that every
   Airflow-image container in the namespace reports the same tag:
   ```bash
   kubectl -n airflow get pods -o jsonpath='{range .items[*]}{.spec.containers[*].image}{"\n"}{end}' | sort -u
   ```

---

### B10. DAG distribution on kind: git-sync is wrong here, baked images are slow, and the third option must be chosen at cluster-create time

**Severity:** ANNOYANCE that compounds into REWORK (a 3-minute DAG edit loop changes how you work)
**Retrofit cost:** MEDIUM — the good option requires `extraMounts`, i.e. **cluster recreation**,
which makes this a Phase 1 decision disguised as a Phase 3 decision
**Phase:** 1 (mount) / 3 (chart wiring)
**Confidence:** MEDIUM-HIGH — mechanisms are documented; the recommendation is reasoned

| Option | Local kind reality |
|---|---|
| **git-sync** (chart default path for many) | Needs a reachable remote. Your repo is local; you would be pushing to GitHub to test a one-line DAG edit. Also adds a container per Airflow pod, more inotify pressure (A1), and a poll interval of latency. **Reject locally**; it is the right answer in a real cluster, so keep the values file able to switch to it. |
| **Baked into the image** | Correct for CI (immutable, matches production) and correct for the *processor* image. Locally it means rebuild → push → restart DAG processor for every DAG edit. **Use in CI only.** |
| **hostPath via kind `extraMounts` → PV/PVC** | Instant edits, zero rebuild. Requires `extraMounts` on **every node that can host the DAG processor** at cluster-create time, and the source directory must be on **WSL ext4** — PROJECT.md's constraint explicitly forbids `/mnt/c` here, because the DAG processor's periodic re-stat loop over 9p is pathological. **Recommended locally.** |

**Prevention:** put `extraMounts` in `kind/cluster.yaml` in Phase 1 even if you do not wire it up
until Phase 3 — adding it later costs a cluster rebuild plus whatever A4 has stored by then. Pin
the DAG processor to the mounted node with a `nodeSelector` so it cannot be scheduled somewhere
without the mount. And keep `values-ci.yaml` on the baked-image path, so CI proves the production
mechanism while local dev keeps the fast loop.

---

### B11. Top-level DAG code is worse here than in a normal Airflow project

**Severity:** ANNOYANCE → REWORK (scheduler responsiveness collapses; the fix touches every DAG)
**Retrofit cost:** CHEAP if the rule is set on day one; tedious later
**Phase:** 3, enforced by lint from 5 onward
**Confidence:** HIGH

**Why it is worse here specifically:** the DAG files import from `csv_processor`, which pulls in
`boto3`, `psycopg`, `pydantic` and the config loader. The DAG processor re-parses every file
every `min_file_process_interval` (default 30 s) **forever**, so a 2-second import tax is paid
continuously, on a machine that is also running two Postgres instances and Prometheus. Worse, the
metadata-driven design invites the truly fatal version: reading dataset configs *at parse time*
(a database query or a MinIO listing at module scope), which turns every parse into I/O against
services that may be down — and a DAG that fails to parse does not appear in the UI at all,
producing "my DAG disappeared" rather than an error.

**Prevention:**

- Hard rule: **module scope contains only imports of `airflow`, `datetime`, and your own
  lightweight declarative config; everything else is inside `@task` functions.** Heavy imports go
  inside the callable.
- Dataset configs are read from **files bundled with the DAGs** (YAML) at parse time if they must
  be read at all — never from PostgreSQL, MinIO or Vault at module scope. ARCHITECTURE's AP11
  ("config only in a ConfigMap") and this rule are the same coin.
- Enforce mechanically: a `ruff` rule set banning heavy imports in `dags/`, plus a test that
  asserts `DagBag` import time per file stays under a budget (e.g. 1 s) — this is one of the few
  genuinely valuable tests to write early, because it fails the moment someone regresses it.
- Set `min_file_process_interval` to 60–120 s locally; you do not need 30 s responsiveness when
  the hostPath mount gives you instant file visibility anyway.

**Warning signs:** `airflow dags list-import-errors` non-empty; DAG processor CPU steadily at
100% of its request; the "Last parsed" timestamp in the UI drifting minutes behind; a DAG that
vanishes from the UI when Vault or MinIO is down (a dead giveaway of parse-time I/O).


---

## C. PostgreSQL and data correctness — where silent corruption actually comes from

*Highest severity in the document. Several of these are effectively impossible to retrofit,
because by the time you notice, the corrupt history is the history.*

---

### C1. `MERGE` is **not** concurrency-safe; `INSERT … ON CONFLICT` is

**Severity:** DATA CORRUPTION / run failure under concurrency
**Retrofit cost:** MEDIUM — swapping the publication statement is contained, but discovering you
needed to is usually preceded by a period of trusting bad data
**Phase:** 5 (single-writer), hardened in 8 (§86/§87 concurrency)
**Confidence:** HIGH — this is a documented semantic difference, repeatedly confirmed on the
pgsql-hackers/bugs lists

**What goes wrong:** SQL-standard `MERGE` (PostgreSQL 15+) evaluates its match condition against
the snapshot it started with. If two transactions `MERGE` a business key that does not yet exist,
**both** take the `WHEN NOT MATCHED` branch and both attempt an `INSERT`; the loser raises a
unique-constraint violation. `INSERT … ON CONFLICT` was designed specifically to be safe here;
`MERGE` was not. The trap is that `MERGE` reads better, appears in every modern tutorial, and
works perfectly in single-threaded tests — so it survives all the way to the first day two
batches of the same dataset run concurrently (README §86 explicitly requires that scenario).

**A second, sharper edge:** `ON CONFLICT` uses exactly **one arbiter index** per statement. If the
target table has both a primary key and another unique constraint (e.g. surrogate PK *and*
`UNIQUE(business_key, valid_from)`), specifying `ON CONFLICT (id)` does nothing to prevent a
violation of the *other* index — so you get duplicate-key errors and occasional deadlocks under
concurrency despite "using upsert". Arbitrate on the **natural** key that actually defines
uniqueness, not the surrogate.

**A third:** `MERGE` raises `cardinality_violation` — *"MERGE command cannot affect row a second
time"* — and `ON CONFLICT DO UPDATE` raises *"cannot affect row a second time"* whenever the
**source** contains duplicate keys. A CSV batch containing the same business key twice is not an
edge case here; it is the normal case that §26 deduplication exists to handle. If dedup runs
*after* the load, this error is your first notification.

**Prevention:**

1. Deduplicate **in the staging query**, always, even when you believe the source is clean:
   `SELECT DISTINCT ON (business_key) … ORDER BY business_key, <deterministic tiebreak>`.
   The tiebreak must be total and deterministic (§67) — e.g.
   `event_ts DESC, source_sequence DESC, source_file, source_row_number`.
2. Use `INSERT … ON CONFLICT (natural_key) DO UPDATE` for row-level upserts, arbitrating on the
   real uniqueness constraint. Keep `MERGE` for the cases where you *hold a lock* (below) and
   want its multi-action expressiveness.
3. Make the publication a **single writer per dataset** with
   `SELECT pg_advisory_xact_lock(hashtextextended('publish:' || :dataset, 0))` as the first
   statement of the publication transaction. This is a few lines, removes the entire race class
   (README §87's "Task A / Task B → same PostgreSQL target"), and is far more robust than
   reasoning about isolation levels. The lock is released automatically at commit or rollback —
   including when the pod is killed, once C2's timeouts are set.

**Warning signs:** intermittent `duplicate key value violates unique constraint` on a code path
that "cannot" produce duplicates; `deadlock detected` in the Postgres log; `cardinality_violation`.
Write the concurrency test (two overlapping batches of the same dataset) in Phase 5, before the
logic is complex enough to hide the bug.

---

### C2. The long publication transaction — bloat, blocked autovacuum, and pods that die holding locks

**Severity:** REWORK, with DATA CORRUPTION potential if a kill lands mid-write
**Retrofit cost:** MEDIUM (splitting the transaction changes the recovery model)
**Phase:** 5 (shape) / 8 (tuning)
**Confidence:** HIGH on Postgres mechanics; MEDIUM on the specific timeout values

**Three compounding problems:**

1. **Transaction duration is contagious.** An open transaction holds back the cluster-wide `xmin`
   horizon, so autovacuum cannot reclaim dead tuples **in any table**, not just yours. A 40-minute
   "atomic load" of a large file therefore bloats the *entire* analytical database, including
   tables it never touched. The symptom appears weeks later as inexplicably slow queries.
2. **A killed pod does not release its locks promptly.** When Kubernetes kills a task pod (OOM,
   eviction, node pressure — see A2), the TCP connection may not be reset cleanly. PostgreSQL
   keeps the backend `idle in transaction`, holding every lock and the `xmin` horizon, until TCP
   keepalives eventually time out — which can be **hours** with defaults. The next run then blocks
   forever on a lock held by a pod that no longer exists. This is a Kubernetes-specific failure
   that does not occur on a laptop, and it will be blamed on your locking design.
3. **`ACCESS EXCLUSIVE` at the wrong moment.** `TRUNCATE`, `ALTER TABLE … ATTACH PARTITION` and
   index rebuilds take exclusive locks; taken inside a long transaction, they queue every reader
   behind them (and, because Postgres lock requests queue, a single blocked DDL blocks all
   subsequent readers too).

**Prevention:**

- **Structure:** the expensive work (`COPY` into an unlogged/plain staging table, index build on
  staging, validation queries) happens **outside** the publication transaction and is fully
  restartable. The publication transaction is short — a handful of statements: advisory lock,
  dedupe-merge or `ATTACH PARTITION`, metadata rows, watermark advance, commit. ARCHITECTURE.md
  already makes the publication transaction the metadata commit point; the pitfall is letting the
  `COPY` creep inside it "for atomicity".
- **Set these on the analytical database (CNPG `postgresql.parameters`) from Phase 2:**
  ```
  idle_in_transaction_session_timeout = '5min'   # kills orphaned pod transactions
  statement_timeout                   = '30min'  # per-session override for the loader
  lock_timeout                        = '30s'    # fail fast instead of queueing forever
  tcp_keepalives_idle                 = 60
  tcp_keepalives_interval             = 10
  tcp_keepalives_count                = 6
  ```
  Set `lock_timeout` deliberately low for the publication transaction: a load that cannot get the
  lock should **fail and retry**, not queue behind a zombie.
- **Per-table autovacuum tuning** on the SCD2 dimension tables, which churn heavily
  (`autovacuum_vacuum_scale_factor = 0.02`).

**Warning signs:** `SELECT * FROM pg_stat_activity WHERE state = 'idle in transaction'` returning
rows older than a few minutes; `pg_locks` entries owned by no live pod; `n_dead_tup` climbing in
`pg_stat_user_tables`; table size growing while row count does not.

---

### C3. Letting PostgreSQL parse the CSV voids the entire product

**Severity:** DATA CORRUPTION (silently different results from the validated path)
**Retrofit cost:** CHEAP to prevent, EXPENSIVE to detect (nothing errors)
**Phase:** 5 — the very first load
**Confidence:** HIGH

**What goes wrong:** `COPY target FROM '/data/file.csv' WITH (FORMAT csv, HEADER)` is fast,
one line, and works. It also means PostgreSQL's CSV parser — not your engine — decides what the
data is. Its dialect handling, quoting rules, NULL representation and encoding conversion differ
from the Python `csv` module you validated with. The rows that land in the warehouse are then
**not the rows your validation, deduplication and hashing saw**. Every guarantee in §19–§27
becomes decorative. Because both parsers succeed, there is no error to notice.

Two specific divergences to expect: PostgreSQL's `COPY … CSV` treats an unquoted empty field as
an empty string but a `NULL AS` match as NULL, which will not agree with your §17 NULL policy; and
`COPY` aborts the entire statement on the first malformed row, which is incompatible with §51's
requirement that bad records be retained with a reason rather than discarded.

**Prevention:** the processor is the **only** CSV parser. Feed `COPY … FROM STDIN` from
already-parsed, already-normalized Python values via `psycopg`'s copy interface, in `text` format
with explicit escaping (or binary). The database receives *fields*, never a file. Add an explicit
architectural rule and a lint/grep in CI: no occurrence of `FORMAT csv` in any SQL in the repo.
Note that this also removes the "the file must be visible to the database server" problem, which
is unsolvable anyway once MinIO is the source of truth (§5).

**Related trap — NUL bytes.** FEATURES.md flags that NUL bytes hard-fail the stdlib `csv` module.
They *also* hard-fail PostgreSQL: `\x00` cannot be stored in a `text` column, and `COPY` aborts
with `invalid byte sequence`/`unsupported Unicode escape sequence`. So a NUL surviving the parser
kills the load at the last possible moment, after all the work. Strip or reject NULs during
normalization, count them, and record the count in the validation report.

---

### C4. Partition attach validation, and the DDL that cannot run in your transaction

**Severity:** REWORK (a "instant" publication that takes minutes and locks the table)
**Retrofit cost:** CHEAP if the constraint is added at staging-table creation
**Phase:** 8 (atomic publication at scale)
**Confidence:** MEDIUM-HIGH — PostgreSQL skips attach-time validation when a matching `CHECK`
constraint proves the partition bound; verify against the PG major you pin

**What goes wrong:** ARCHITECTURE.md's atomic publication via `ATTACH PARTITION` is right, but
`ALTER TABLE … ATTACH PARTITION` takes `ACCESS EXCLUSIVE` on the parent **and scans the whole
staging table** to prove every row satisfies the partition bound — unless an existing `CHECK`
constraint already implies it. On a large batch that scan turns a millisecond publication into a
multi-minute exclusive lock, i.e. exactly the outage the design was meant to avoid.

Second half: `CREATE INDEX CONCURRENTLY` and `ALTER TABLE … DETACH PARTITION CONCURRENTLY`
**cannot run inside a transaction block**. Plans that say "build indexes concurrently, then
publish atomically" are self-contradictory.

**Prevention:**

- When creating the staging table, add a `CHECK` constraint that exactly matches the partition
  bound (`CHECK (business_date >= DATE '2026-08-01' AND business_date < DATE '2026-09-01')`), and
  validate it *while the table is private and unattached* — the scan then costs nothing extra
  because you are already writing the table.
- Build indexes on the staging table **non-concurrently** while it is private. Concurrency
  protection is unnecessary for a table nobody else can see; `CONCURRENTLY` there is pure cost.
- Keep the `ATTACH` itself as the last statement before commit, with `lock_timeout` set.

---

### C5. Idempotency: the four things that make a "safe retry" unsafe

**Severity:** DATA CORRUPTION (duplicates that look like real data)
**Retrofit cost:** **EXPENSIVE** — retrofitting run-scoped identity means rewriting the loader and
back-filling identity columns onto existing rows
**Phase:** 5 (the vertical slice must already be idempotent), enforced in 8
**Confidence:** HIGH

ARCHITECTURE.md establishes that idempotency lives *inside* the vertical slice and defines the
four identity layers. These are the specific mechanisms that break it:

1. **More than one commit per attempt.** The moment a run commits twice, a crash between the
   commits leaves a state neither "done" nor "not started". A retry that assumes "not started"
   double-loads. **Prevention:** every attempt begins with an unconditional, idempotent *undo* of
   its own prior partial work, keyed by attempt: `DELETE FROM stg_x WHERE run_id = :run_id` (or
   drop/recreate the run-scoped staging table). Never `TRUNCATE` a shared staging table — it
   deletes another concurrent run's work and takes `ACCESS EXCLUSIVE`.
2. **Non-transactional side effects.** MinIO writes, metric emissions, Vault leases, lineage
   events and notifications are not covered by the database transaction. A retry repeats them.
   **Prevention:** every object this pipeline writes gets a **deterministic key** derived from
   `(dataset, run_id, chunk_index)`, so a rewrite overwrites rather than accumulates. Anything
   that genuinely cannot be made idempotent (an email, an external API call) happens *after*
   commit, exactly once, and is recorded as having happened.
3. **Sequences are non-transactional.** A rolled-back load still consumes surrogate key values.
   Gaps are harmless and expected — but any logic that assumes contiguity, or that maps staging
   rows to keys via `currval` across a retry boundary, is broken. **Prevention:** never derive
   meaning from surrogate key values; FEATURES.md already forbids deriving them from a hash, and
   the converse holds too — do not derive anything *from* them.
4. **Identity from the wrong thing.** FEATURES.md covers why filename identity fails. Add: **do
   not use the S3/MinIO `ETag` as content identity.** For multipart uploads the ETag is not an MD5
   of the content; it is a hash of part hashes plus a part count, so the same bytes uploaded with
   a different part size produce a different ETag, and it is unusable as a dedup key. Compute a
   `sha256` of the bytes yourself as they stream through (§42 file integrity), once, and store it.
   Keep *arrival* identity (`bucket`, `key`, `version_id`, `size`, `last_modified`) separate from
   *content* identity (`sha256`) — the legitimate case "the same file was genuinely re-sent" needs
   both to be distinguishable from "we processed this twice".

**Warning signs:** row counts in the warehouse exceeding control totals (§46) by exactly the size
of one chunk; a `run_id` appearing in the fact table with two different `attempt` values; MinIO
objects with UUID-ish names accumulating without bound.

---

### C6. Change-hash design decisions you can only make once

**Severity:** DATA CORRUPTION (phantom versions, or missed changes)
**Retrofit cost:** **EXPENSIVE** — a changed hash recipe invalidates every stored hash, so every
existing SCD2 row must be recomputed, and history may be unrecoverable
**Phase:** 5 (decide and version it) / 9 (SCD)
**Confidence:** HIGH

FEATURES.md establishes that normalization must precede hashing (NFC/NFD, whitespace). These are
the *other* decisions, all of which are cheap to make correctly now and very expensive to change:

- **Store a `hash_version` column next to every stored hash, from the first row.** This is the
  single cheapest insurance policy in the project. Without it, the day you improve normalization
  you cannot tell old hashes from new ones, and every business key appears to have changed —
  generating a phantom SCD2 version for the entire dimension.
- **Field-boundary ambiguity.** Concatenating fields before hashing makes `("ab","c")` and
  `("a","bc")` identical, and choosing `|` as a separator merely moves the collision to values
  containing `|`. **Prevention:** length-prefix each field (`f"{len(v)}:{v}"`) or hash a canonical
  JSON encoding with sorted keys. Do not rely on "our data never contains the separator".
- **Column set and order must be explicit and versioned**, taken from the data contract (§22), not
  from `df.columns` or `row.keys()`. A schema evolution that adds a column must not change the
  hash of unrelated rows — which means the hash input is the *contract's* attribute list, not
  whatever the file happened to contain.
- **NULL must not equal empty string.** Encode NULL as a distinct, unambiguous marker (and note
  that FEATURES.md's Excel-damaged-value cases mean empty-vs-NULL genuinely differs in meaning).
- **Never hash a float.** `0.1 + 0.2` and repr differences across platforms make float hashing
  non-deterministic in practice. Hash the normalized `Decimal` string produced by the §15 numeric
  normalizer.
- **Compute the hash in exactly one place — Python.** Postgres `md5()`/`digest()` will disagree
  with `hashlib` about encoding and normalization, and having two implementations guarantees they
  drift. Never recompute a hash in SQL "to check".
- **Exclude CDC/transport metadata from the change hash.** Including `event_ts`, offset, ingestion
  time or source filename means an at-least-once redelivery of an identical event produces a new
  hash and therefore a **new SCD2 version** — which is exactly the failure §60 warns about. The
  change hash covers business attributes only.

---

### C7. SCD2 overlapping validity intervals — make them impossible, not merely unlikely

**Severity:** DATA CORRUPTION (as-of queries return two rows; every historical answer is wrong)
**Retrofit cost:** **EXPENSIVE** — once overlaps exist, adding the constraint fails and repairing
history requires reconstructing it
**Phase:** table creation for the dimension, i.e. 9 at the latest — but the migration should be
written in 8
**Confidence:** MEDIUM-HIGH — `btree_gist` + range `EXCLUDE` is a standard, documented PostgreSQL
pattern; the exact DDL should be validated against the PG major pinned in STACK.md

**What goes wrong:** SCD2 correctness depends on the invariant "for a given business key, validity
intervals do not overlap and there is at most one current row". Application code maintains this by
closing the old row and inserting the new one. Every bug — a retry, a late correction, a race
between two batches, a mis-ordered CDC stream — breaks it silently. Nothing complains; the table
just quietly starts returning two rows for an as-of query, and downstream aggregates double-count.

**Prevention — let the database enforce the invariant:**

```sql
CREATE EXTENSION IF NOT EXISTS btree_gist;

ALTER TABLE dim_customer ADD CONSTRAINT dim_customer_no_overlap
  EXCLUDE USING gist (
    business_key WITH =,
    tstzrange(valid_from, valid_to, '[)') WITH &&
  );
-- plus: at most one current row per key
CREATE UNIQUE INDEX dim_customer_one_current
  ON dim_customer (business_key) WHERE is_current;
```

Supporting decisions that must be made at the same time:

- **Half-open intervals `[valid_from, valid_to)`.** Closed intervals make adjacent versions
  overlap at the boundary instant, which the constraint will (correctly) reject — and which is a
  real bug even without the constraint.
- **Use `'infinity'::timestamptz` for the open end, not `NULL` and not `9999-12-31`.** `NULL`
  makes range semantics and `is_current` derivation ambiguous; a magic date is a Y10K joke that
  turns into an off-by-one in `<` comparisons.
- **`timestamptz`, UTC, everywhere.** Naive `timestamp` columns with a source in another timezone
  produce validity boundaries that are off by hours — invisible until a DST transition puts two
  versions in the wrong order.
- **The constraint will fail your tests before it fails your data.** That is the point. Expect to
  spend a day fixing genuine ordering bugs it exposes; that day is much cheaper than the alternative.

---

### C8. Late-arriving SCD corrections: mutate-in-place is not idempotent; recompute-per-key is

**Severity:** DATA CORRUPTION
**Retrofit cost:** EXPENSIVE (it is a design choice, not a bug fix)
**Phase:** 9, decided in 8
**Confidence:** MEDIUM — reasoned design, corroborated by standard practice; the throughput
tradeoff is not measured

**What goes wrong:** §58 requires that a late correction repairs historical validity intervals.
Implemented as in-place surgery ("find the version covering this date, split it, adjust the
neighbours"), the operation is order-dependent, hard to test, and **not idempotent** — applying
the same correction twice produces a different result than applying it once. Combined with §61
(backfill-safe SCD) and at-least-once CDC redelivery, this is a guaranteed source of drift.

**Prevention:** keep an **append-only, ordered event table** per dimension (which CDC gives you
for free, and which §63's immutable-raw principle argues for anyway). Deriving SCD2 becomes:
*for each affected business key, delete its current version rows and rebuild them by folding the
ordered event log*. Properties: naturally idempotent, naturally order-independent at the API level
(ordering is applied inside the fold), trivially testable as a pure function
`events -> versions`, and it makes §61 backfill safety fall out rather than being engineered.

Cost: rebuilding a key's whole history on every change. At this project's scale (local kind,
synthetic corpus) that is irrelevant; document the tradeoff and the scale at which it would stop
being acceptable, so the choice is visible rather than accidental. Scope the delete-and-rebuild to
the affected keys inside the same transaction as the advisory lock from C1.

**Warning sign:** a test that applies the same correction file twice and gets a different row count
the second time. Make that test exist in Phase 8, before the SCD code does.

---

### C9. CDC ordering: ties, tombstones and resurrection

**Severity:** DATA CORRUPTION
**Retrofit cost:** MEDIUM-EXPENSIVE
**Phase:** 9
**Confidence:** MEDIUM-HIGH

Four specific traps beyond §30's ordering requirement:

1. **Ordering by `event_ts` alone is non-deterministic.** Source systems routinely stamp many rows
   with the same millisecond. Sorting by a non-total key means the applied order varies between
   runs — violating §67 determinism and producing a different SCD2 history on replay. **Prevention:**
   the sort key must be *total*: `(event_ts, source_sequence, source_file, source_row_number)`.
   Where no sequence exists, say so explicitly in the dataset contract and accept the documented
   weaker guarantee (§30 already forbids unearned claims).
2. **Delete-then-reinsert ("resurrection") is indistinguishable from update** if you physically
   delete. **Prevention:** model DELETE as closing the current interval and setting `is_deleted`,
   never as a physical delete. A later INSERT for the same key then correctly opens a *new*
   interval rather than looking like an amendment to the old one.
3. **Tombstones with no payload.** A delete event often carries only the key. Code that builds the
   change hash from the payload will hash `NULL`s and conclude "changed" or crash. **Prevention:**
   handle op-type before hashing; a DELETE never computes a change hash.
4. **Missing/gapped sequence numbers.** A gap can mean loss or can mean the source skips values.
   **Prevention:** detect and *report* gaps (§45 reconciliation) rather than assuming either
   interpretation; block publication on a gap only if the dataset contract says the sequence is
   dense. Record the decision per dataset — this is exactly the kind of thing that must be
   configuration (§65), not a hard-coded belief.

---

### C10. Watermarks: the commit-order trap that silently loses rows forever

**Severity:** DATA CORRUPTION (permanent silent loss — the worst category)
**Retrofit cost:** MEDIUM (a lag parameter is cheap; the lost data is not recoverable without a
full reload)
**Phase:** 8
**Confidence:** HIGH — this is the classic incremental-extraction failure

FEATURES.md covers the boundary-tie problem (`>` loses ties; use `>=` plus an idempotent merge).
The deeper trap is **commit order versus timestamp order**: a source transaction can set
`updated_at = 10:00:00` and not commit until `10:00:05`. An extraction that runs at `10:00:02`
sees nothing for that row, advances the watermark to `10:00:02`, and the row — whose timestamp is
now *behind* the watermark — is **never selected again**. No error, no gap detectable in the target.

**Prevention:**

- **Lag the watermark** by a safety interval larger than the source's plausible maximum transaction
  duration (start with minutes, make it per-dataset configuration), accepting that the same rows
  will be re-read and relying on the idempotent merge to absorb them. This is the standard
  trade: re-read cheaply rather than lose silently.
- Prefer a **commit-ordered cursor** (LSN, transaction ID, monotonic sequence) over a wall-clock
  column whenever the source offers one.
- **Watermarks are per `(dataset, partition)`**, never global — a global watermark means a late
  file for one partition drags the cursor for all of them.
- **Advance the watermark only inside the publication transaction** (ARCHITECTURE's AP4) *and*
  only from the maximum cursor value actually observed in committed rows — not from `now()`, and
  not from the query's upper bound.
- Detect it anyway: §46 control totals and §45 reconciliation are the only mechanisms that can
  catch this class after the fact. That is the argument for building them in Phase 8 rather than
  treating them as polish.

---

### C11. Bulk-load and index management traps

**Severity:** ANNOYANCE → REWORK at scale
**Retrofit cost:** CHEAP
**Phase:** 5 (basic) / 8 (tuning)
**Confidence:** MEDIUM-HIGH

- **`COPY` beats multi-row `INSERT` by roughly an order of magnitude**, and `executemany` of
  single-row `INSERT`s by two. If the first implementation uses `executemany`, the "streaming is
  too slow" conclusion in ARCHITECTURE's unvalidated assumption #3 will be an artefact of the
  loader, not of streaming. Measure `COPY` before concluding anything about throughput.
- **Do not drop indexes on the live warehouse table to speed a load.** The rebuild takes
  `ACCESS EXCLUSIVE` and, if it fails, you are left with an unindexed production table. Load into
  a private staging table instead (C4).
- **Unlogged staging tables** are materially faster and perfectly safe *for staging*, because
  staging is reconstructible by definition. They are lost on crash — which is the correct
  behaviour for a restartable stage. Do not make warehouse tables unlogged.
- **`is_current` update churn defeats HOT updates**, because `is_current` is indexed. SCD2
  dimensions therefore bloat faster than their row count suggests. Use a **partial** index
  (`WHERE is_current`) to keep it small, and tune autovacuum per table (C2).
- **Batch size is a tuning knob, not a constant.** Per-chunk `COPY` with a chunk of 10k–50k rows
  is a reasonable starting point; a chunk of 100 rows makes the round-trip dominate, and a chunk
  of 1M defeats the bounded-memory requirement (§39).

---

### C12. Two databases, one cluster: connection budgets and the wrong-database accident

**Severity:** ANNOYANCE, with one REWORK-grade variant
**Retrofit cost:** CHEAP
**Phase:** 2
**Confidence:** MEDIUM

- PostgreSQL's `max_connections` default (100) is a real ceiling once the Airflow components, the
  KubernetesExecutor's activity and your task pods all connect. Airflow 3 helps — task code talks
  to the **API server**, not the metadata DB — but that shifts the bottleneck to the API server's
  own pool rather than removing it. Set `max_connections` deliberately on both CNPG clusters and
  give the analytical database a small connection pool per task pod (1–2 connections), not a
  default-sized one.
- **The wrong-database accident:** the single most damaging typo in this project is pointing the
  analytical loader at the Airflow metadata database (or vice versa) — README §4 makes the
  separation a hard architectural rule precisely because it is easy to violate. **Prevention:**
  different database *names*, different roles, and a startup assertion in the processor that the
  connected database has a marker table (`SELECT 1 FROM platform_marker WHERE kind = 'analytical'`)
  and refuses to run otherwise. Ten lines; eliminates the class permanently.
- Give the loader role only what it needs: `INSERT`/`SELECT` on staging and warehouse schemas, no
  `SUPERUSER`, no rights on the Airflow database at all — and prove it with a negative test, which
  also satisfies §88.


---

## D. Vault + Kubernetes — identity, persistence and the availability tension

*Medium retrofit cost, high daily-friction cost. The identity model (D3) is expensive to change
because policies, roles, ServiceAccounts and tests all reference it.*

---

### D1. Vault dev mode loses everything on restart — and on WSL2 that means "every morning"

**Severity:** ANNOYANCE that becomes REWORK (people stop using Vault properly to avoid the pain)
**Retrofit cost:** CHEAP if the bootstrap is scripted from day one; expensive as a habit to unlearn
**Phase:** 2 (deploy) / 4 (integrate)
**Confidence:** HIGH — dev mode is explicitly in-memory, auto-unsealed, root token `root`

**What goes wrong:** `server.dev.enabled: true` is the obvious local choice: no unseal, no
storage, instant. Then you configure the Kubernetes auth method, three policies, four roles and a
dozen secrets **by hand with the CLI**. The pod restarts — because you ran `wsl --shutdown`, or
the node container restarted, or A2's OOM killer fired — and all of it is gone. The second time
this happens, the natural reaction is to weaken the design (one shared token, secrets in env vars)
to avoid re-doing the work. That is how README §81's least-privilege requirement quietly dies.

**Prevention:**

- **Nothing about Vault's configuration is ever done interactively.** `make vault-bootstrap` is an
  idempotent script that enables the KV mount, the Kubernetes auth method, every policy, every
  role, an audit device, and seeds development secrets. Re-running it must be safe. Write it in
  Phase 2, before you need it, and treat any manual `vault write` as a bug.
- **Run sealed mode (file/raft storage) locally at least once, deliberately**, so the unseal path
  exists and is documented in the runbook (§89) — §90 disaster recovery is not credible if nobody
  has ever unsealed this Vault. Keep dev mode for CI (STACK.md already does).
- **Enable an audit device explicitly.** §81.8 requires auditable secret access; Vault has **no**
  audit device enabled by default, and in dev mode the audit log dies with the pod. `vault audit
  enable file file_path=/vault/audit/audit.log` belongs in the bootstrap script, with a
  persistent volume if you want it to survive.

---

### D2. Sealed Vault after a restart is a *manual* step unless you plan for it

**Severity:** ANNOYANCE; REWORK if it blocks CI
**Retrofit cost:** CHEAP
**Phase:** 2
**Confidence:** HIGH

With file or Raft storage, Vault starts **sealed** after every pod restart and every cluster
rebuild, and Vault Community Edition's only unattended options are cloud KMS or Transit auto-unseal
(i.e. another Vault). On a local kind cluster you therefore need three unseal keys at hand every
time a pod is rescheduled.

**Prevention:** a `make vault-unseal` that reads keys from a **gitignored** local file
(`.secrets/vault-init.json`, mode 0600) written by `vault operator init`. Add a readiness gate to
every other bootstrap step so "Vault is sealed" produces a clear message rather than a cascade of
confusing auth failures. Add the file's exact path to `.gitignore` *and* verify the secret-scanner
still flags it if it ever gets committed (do not simply allowlist the pattern — see G's note on
scanner allowlists). Never let unseal keys reach a CI log; CI uses dev mode precisely so it never
holds them.

---

### D3. Kubernetes auth: the `iss` scare is mostly obsolete — the real failures are TokenReview, audience, and the SA you forgot

**Severity:** REWORK; **security** issue when "fixed" by widening the role
**Retrofit cost:** MEDIUM — the identity model is referenced by policies, roles, manifests and tests
**Phase:** 4
**Confidence:** MEDIUM-HIGH — Vault has defaulted `disable_iss_validation` to true since 1.9, so
this **resolves ARCHITECTURE.md's unvalidated assumption #2 as low-risk**; the remaining failure
modes below are the ones that actually bite on a fresh cluster

**The obsolete part:** the widely-cited `claim "iss" is invalid` error dates from Kubernetes 1.21
enabling `BoundServiceAccountTokenVolume`, where the token issuer became cluster-specific. Vault
1.9+ disables issuer validation by default (the Kubernetes TokenReview API performs the same check),
so on Vault major 2.x with a modern kind cluster you should **not** need to discover or configure
the issuer. If you do hit it, `disable_iss_validation=true` or setting `issuer` to match
kube-apiserver's `--service-account-issuer` are both documented fixes. Budget an hour, not a day.

**What actually goes wrong on kind:**

1. **The token reviewer lacks `system:auth-delegator`.** Vault validates the client's token by
   calling the Kubernetes `TokenReview` API. Its own ServiceAccount must be bound to the
   `system:auth-delegator` ClusterRole or every login fails with a 403 that reads like a Vault
   problem. The Vault Helm chart creates this binding for its own SA; if you run Vault with a
   custom SA, or use an explicit `token_reviewer_jwt`, you must create it yourself.
2. **Audience mismatch.** Projected ServiceAccount tokens are minted with an audience. If the pod
   requests a token with `audience: vault` but the Vault role has no `audience` configured (or a
   different one), login fails with an unhelpful permission error. Decide once whether you use the
   default SA token (audience = the API server) or a projected token with a dedicated audience, and
   make every role consistent.
3. **`kubernetes_host` must be reachable from Vault's network position** — `https://kubernetes.
   default.svc:443` when Vault runs in-cluster. Copying a config from a Vault-outside-Kubernetes
   tutorial gives you an unreachable host and a timeout.
4. **The role binds a ServiceAccount that your pods do not actually use.** This is B5's trap
   viewed from the Vault side and is by far the most common cause. `bound_service_account_names`
   and `bound_service_account_namespaces` must match what `KubernetesPodOperator` actually sets.

**Prevention:** one script creates the role and the Kubernetes RBAC together, from one set of
variables, so they cannot drift. Then write both tests §81.12 asks for **in Phase 4**:
the positive (the `csv-processor` SA in `etl` can read its own path) and, more importantly, the
negative (the `default` SA cannot; the `csv-processor` SA cannot read another dataset's path). A
least-privilege claim with no negative test is an aspiration.

---

### D4. The Airflow Vault secrets backend fails *open*, and it fails at parse time

**Severity:** REWORK; **security**-relevant (you believe Vault is in the path when it is not)
**Retrofit cost:** CHEAP to test for, expensive to discover late
**Phase:** 4
**Confidence:** MEDIUM-HIGH — Airflow's secrets-backend chain semantics are stable across 2.x/3.x;
verify the caching config names against Airflow 3.3.0

**Three distinct traps:**

1. **Silent fallback.** Airflow resolves a Connection by trying the configured secrets backend
   first, then the metadata database, then environment variables. If your Vault path is wrong, the
   lookup falls through and finds the connection you created earlier in the UI — and everything
   works. You will believe Vault is integrated. It is not. **Prevention:** the acceptance test for
   Phase 4 must *delete* the metadata-DB connection and unset any `AIRFLOW_CONN_*` variable, then
   prove the DAG still runs. Do this before building on top of it.
2. **KV v2 path shape.** With KV version 2, the HTTP API path contains `/data/`, but the Airflow
   backend inserts that itself. Writing `mount_point`/`connections_path` with `data` in them
   produces `secret/data/data/connections/...` and a lookup that silently misses (see trap 1).
   The correct shape is `mount_point="secret"`, `connections_path="connections"`,
   `variables_path="variables"`, and the secret written at `secret/connections/<conn_id>` with a
   `conn_uri` (or discrete fields including `conn_type`, which is mandatory and the most commonly
   omitted one).
3. **Lookups happen during DAG parsing** if any module-scope code touches `Variable.get()` or a
   Connection. Combined with B11: when Vault is sealed or down, the DAG **disappears from the UI**
   rather than failing visibly, and the DAG processor burns CPU retrying against a dead Vault every
   parse interval. **Prevention:** the module-scope rule from B11 is a security and availability
   rule as much as a performance one. Secrets are fetched inside task callables, never at import.

**Warning signs:** a connection that resolves when Vault is stopped; `airflow dags
list-import-errors` mentioning Vault; DAG-processor CPU correlating with Vault latency.

---

### D5. Caching versus rotation — you must choose, and then document the lag

**Severity:** ANNOYANCE; a real **security** finding if rotation silently never takes effect
**Retrofit cost:** CHEAP
**Phase:** 4, verified in the §81.7 rotation work
**Confidence:** MEDIUM — the secrets-cache settings exist in modern Airflow; confirm exact names
and defaults in 3.3.0 before relying on numbers

**The tension, stated plainly:** every Connection/Variable lookup is a network call to Vault.
Without caching, Vault is on the hot path of every task start and a Vault blip fails runs
(§84 wants a defined behaviour here). With caching, a rotated secret keeps working with the **old**
value until the TTL expires — so your §81.7 rotation test passes only because the test happened to
run after the TTL, or fails mysteriously because it ran before.

**Prevention:** set the cache TTL **explicitly** rather than accepting a default, write the
resulting rotation lag into the runbook ("a rotated credential takes effect within N seconds for
Airflow, and at next pod start for task pods"), and make the rotation test wait for it deliberately
rather than sleeping arbitrarily.

**The bigger rotation trap that §81.7 hides:** rotating a *static* KV secret changes what Vault
returns; it does **not** change the password in PostgreSQL or MinIO. A complete rotation is a
coordinated two-sided operation, and any long-lived connection pool keeps using the old credential
until it reconnects. Two honest options:

- **Dynamic database credentials** via Vault's database secrets engine — Vault creates a
  short-lived role per lease, so rotation is intrinsic and the two sides can never disagree. This
  is the *correct* answer for the analytical database and is genuinely achievable here.
- **Documented two-phase static rotation** — add the new credential, deploy, verify, revoke the
  old. Slower and manual, but honest.

Whichever you pick, note that **environment-injected secrets never rotate in a running pod**. Task
pods are short-lived, so they pick up new values naturally; long-lived Airflow components do not.
That asymmetry belongs in the runbook.

---

### D6. Define "Vault is down" behaviour before you need it (§84)

**Severity:** REWORK
**Retrofit cost:** CHEAP
**Phase:** 4, tested in 10
**Confidence:** HIGH (design guidance)

The failure-scenario list demands defined behaviour for "Vault unavailable" and "Secret
unavailable". Decide and encode:

- **Task pod, Vault down:** fail fast with a distinct, greppable exception type
  (`SecretUnavailableError`), not a generic connection error, and **do not** consume retries at
  short intervals — an infrastructure outage should back off, and the task should be
  distinguishable in metrics from a data error. This is what §71's exception hierarchy is *for*.
- **Task pod, secret missing (as opposed to Vault down):** a different exception. A missing secret
  is a configuration bug and should never be retried at all.
- **Airflow component, Vault down:** must not take DAGs out of the UI (D4 trap 3) and must not
  wedge the scheduler.
- **Never** fall back to a default or embedded credential on failure. Fail closed.

---

### D7. Secret zero, and the root token in your shell history

**Severity:** **Security**
**Retrofit cost:** CHEAP
**Phase:** 4
**Confidence:** HIGH

ARCHITECTURE.md already names the chicken-and-egg exception (§81.5). The operational pitfalls:

- Dev mode's root token is literally `root`, and `export VAULT_TOKEN=root` ends up in shell
  history, in Makefile output, in CI logs and — eventually — in a screenshot in a README. Use
  `VAULT_TOKEN` from an env file that is gitignored even in dev mode, so the habit is right.
- The bootstrap script needs a privileged token; get it from `vault operator init` output or dev
  mode, never from a committed file, and have the script **unset** it on exit.
- Any Kubernetes Secret used to bootstrap Vault is readable by anyone with `get secrets` in that
  namespace. Restrict it with RBAC and note it as an accepted, documented exception (§81.5), rather
  than pretending it does not exist.


---

## E. CSV parsing and streaming — the corruption bugs that never raise an exception

*Retrofit cost is mixed: E1 and E6 are architectural (expensive later), E2–E5 are contained but
produce the kind of corruption that is only discovered by a business user months on.*

---

### E1. Chunking a CSV by lines destroys every record containing an embedded newline

**Severity:** **DATA CORRUPTION**
**Retrofit cost:** EXPENSIVE — it changes the streaming API, the chunk boundary definition and the
checkpoint model (§38) all at once
**Phase:** 5 — this is the first thing the vertical slice's reader must get right
**Confidence:** HIGH — Python documents the `newline=''` requirement; the S3 line-splitting trap is
inherent to `iter_lines()`

**What goes wrong — three variants, all of which produce plausible-looking wrong data:**

1. **Splitting the byte stream on `\n` yourself** (or `for line in body.iter_lines()`, or
   `body.read().splitlines()`) cuts through the middle of quoted fields that legitimately contain
   newlines. §7/§10 require supporting multiline fields, so this is a stated requirement being
   silently violated. Result: one record becomes two, both malformed, and if the field count
   happens to work out, they load without complaint.
2. **Opening the text stream without `newline=''`.** Python's universal-newline translation
   rewrites `\r\n` inside quoted fields before `csv` ever sees them, corrupting field *contents*
   rather than record boundaries. The Python `csv` documentation states the requirement
   explicitly, and it is the single most-skipped line in every CSV tutorial.
3. **Reading a chunk of N bytes and parsing it independently.** A quoted field spanning the
   boundary makes the next chunk start inside a quote, inverting the quoting state for the rest of
   the chunk — every subsequent field in it is wrong.

**Prevention — the only safe shape:**

```python
# ONE reader over the whole object; chunking happens in RECORDS, downstream of the parser.
with s3_object_as_binary_stream(bucket, key) as raw:          # see the adapter note below
    text = io.TextIOWrapper(raw, encoding=encoding, newline="", errors="strict")
    reader = csv.reader(text, dialect=dialect)
    for chunk in batched(reader, chunk_size):                  # itertools.batched / islice loop
        yield RecordChunk(rows=chunk, first_ordinal=...)
```

Three consequences the roadmap must absorb:

- **`csv.reader` needs a real file-like text object.** boto3's `StreamingBody` is not a
  `BufferedIOBase`, so `io.TextIOWrapper` will not accept it directly. Write a tiny
  `io.RawIOBase` adapter implementing `readinto()` over `body.read(n)` (about 15 lines) and wrap it
  in `io.BufferedReader`. Reaching into `body._raw_stream` works today and is a private attribute
  that will break; the adapter is the seam ARCHITECTURE.md already wants around boto3.
- **Checkpoints (§38) must be record ordinals, not byte offsets.** You cannot resume CSV parsing
  from a byte offset in the middle of a quoted field. Resume means re-streaming from the start and
  skipping to ordinal N (cheap — it is CPU over a stream you were going to read anyway), or storing
  per-chunk boundaries alongside the chunk's committed state. Decide this in Phase 5, because §38
  in Phase 8 will assume whatever Phase 5 built.
- **`csv.field_size_limit` defaults to 131,072 bytes.** A legitimate large free-text field raises
  `_csv.Error: field larger than field limit (131072)`. Raise it deliberately to a documented
  bound (not `sys.maxsize` — an unbounded field limit turns a malformed quote into an
  out-of-memory kill, which is E6's failure mode wearing a disguise).

**Warning signs / tests to write in Phase 5:** a fixture with a quoted field containing `\n` and
one containing `\r\n`, both crossing a chunk boundary at a chunk size of 2; assert the record count
and the exact field contents. FEATURES.md's fixture corpus should carry this as a *boundary* case,
parameterised over chunk sizes 1, 2, 3, so an off-by-one in chunking cannot survive.

---

### E2. Encoding detection is confidently wrong on exactly the files you care about

**Severity:** DATA CORRUPTION (mojibake in a minority of rows)
**Retrofit cost:** CHEAP to prevent; expensive to detect after loading
**Phase:** 6, but the *policy* belongs in 5
**Confidence:** HIGH

**What goes wrong:** the single-byte encodings this project must support — Windows-1250,
Windows-1252, ISO-8859-1/2 — are statistically almost indistinguishable, especially in files that
are 99% ASCII. A detector will return a high confidence for the wrong one, and the damage appears
only in the rows containing `ą ć ę ł ń ó ś ź ż` or `ä ö ü ß` — often deep in the file, and often in
name/address fields where nobody checks. Detecting on a sampled prefix makes this worse: the first
64 KB may be pure ASCII, so the detector has nothing to go on and guesses.

**Prevention:**

- **Detection is a *hint*, never a decision.** The dataset contract (§22) declares the expected
  encoding. Detection runs anyway and its job is to **disagree loudly**: a mismatch between
  declared and detected encoding is a validation finding, not a silent override.
- **Decode with `errors="strict"`, always.** `errors="replace"` converts a hard failure into
  invisible U+FFFD corruption spread through your warehouse; `errors="ignore"` deletes characters
  outright. A `UnicodeDecodeError` at row 900,000 is *the desired behaviour* — it is a file-level
  failure with a precise byte offset. This is the real safety net, and it is why sampling-based
  detection is survivable.
- **BOM handling is a separate decision from encoding.** `utf-8` leaves a BOM as `U+FEFF` glued to
  the first header name, producing a column literally named U+FEFF followed by `id`, which will not match the
  contract — and whose error message looks identical to `id`. Use `utf-8-sig` when a UTF-8 BOM is
  present. Detect the BOM by reading the first bytes yourself; do not rely on the detector.
- **UTF-16 needs special handling twice.** Read as bytes it is full of NUL bytes, which hard-fail
  both the stdlib `csv` module and PostgreSQL (C3). Decoded correctly via `TextIOWrapper` it is
  fine. So UTF-16 must be resolved *before* the parser sees anything — which the E1 shape gives
  you for free, since decoding happens in the wrapper.
- **Mixed encodings in one file** (concatenated exports) are real and are caught by strict
  decoding — provided you never fall back on error.

**Warning sign to build:** a DQ rule counting U+FFFD occurrences per batch, alerting on any. If
your pipeline is correct this is always zero, which makes it a perfect canary.

---

### E3. Dialect sniffing fails in the ways your data actually fails

**Severity:** DATA CORRUPTION (a wrong delimiter shifts every column)
**Retrofit cost:** CHEAP
**Phase:** 6
**Confidence:** HIGH

**What goes wrong:** `csv.Sniffer` raises `_csv.Error: Could not determine delimiter` on
single-column files and on short files, and — worse — *succeeds with the wrong answer* on the two
cases this project will definitely meet:

- **European semicolon-delimited files with comma decimal separators.** Sniffed as comma-delimited,
  `1,50` becomes two fields and every subsequent column shifts by one. Arity checks catch it only
  if the shift changes the field count, which it does not when several numerics are present.
- **Files with a metadata preamble** (§11). Sniffing the first lines samples the preamble, not the
  data, and returns whatever punctuation the preamble happened to contain.

`Sniffer.has_header()` is a heuristic on top of a heuristic and is unreliable on all-string data.

**Prevention:**

- Sniff **after** preamble detection, over a window of complete records from the data region.
- Restrict candidates explicitly (`delimiters=",;\t|"`); an unconstrained sniffer will happily
  choose `.` or a space.
- Do not trust the sniffer's verdict alone — **score candidates by field-count consistency**: parse
  the first ~200 records with each candidate delimiter and pick the one whose field count is most
  consistent (and, as a tiebreak, largest). This is ~20 lines, is deterministic, and is far more
  robust than `Sniffer`. Record the score in the metadata so a low-margin decision is visible.
- The contract always wins if it declares a dialect; detection then only reports disagreement.
- Emit the detected dialect into the run metadata. When a load looks wrong, "which delimiter did we
  use?" must be answerable by SQL, not by re-running the detector.

---

### E4. Header and footer heuristics fail *silently* by producing plausible rows

**Severity:** DATA CORRUPTION
**Retrofit cost:** CHEAP
**Phase:** 6 / 7
**Confidence:** MEDIUM-HIGH — reasoned from the failure shapes in §11 and §51

Two specific misfires:

- **A totals footer with the same arity as data** (`"TOTAL";;;"123456,78"`) passes structural
  validation and loads as a record with a nonsense business key. It then participates in
  deduplication and — if the dataset is a dimension — creates a permanent phantom SCD2 member.
- **A repeated header mid-file** (from concatenated exports) loads as a data row whose every field
  is a column name. Type conversion may even succeed if the staging is all-TEXT (which
  ARCHITECTURE mandates for good reasons), so nothing complains.

**Prevention:** footer/preamble boundaries are **contract-declared** (`skip_leading: N`,
`skip_trailing: N`, or a regex on the first field), with detection used only to flag disagreement —
the same policy as E2/E3. Add two cheap universal DQ rules that catch the general case:
(a) any data row whose field values equal the header values is quarantined, not dropped;
(b) the business key must match a declared pattern (§20 patterns), so `TOTAL` fails validation
rather than becoming a customer. §51 requires rejected records be retained with a reason — that is
exactly what makes these two rules safe to apply aggressively.

---

### E5. Type inference: the failures are all in the identifiers

**Severity:** DATA CORRUPTION (irreversible)
**Retrofit cost:** EXPENSIVE if it reaches the warehouse (the original value is gone)
**Phase:** 6 (inference) / 7 (validation)
**Confidence:** HIGH

FEATURES.md already covers Excel-damaged IDs (`1.23457E+14`) as unrecoverable. The prevention
principles that follow, and the traps they cover:

- **Identifiers are always TEXT. Always.** Postcodes (`01234` → `1234`), account numbers, phone
  numbers with `+`, national IDs with leading zeros, and any integer above 2^53 that touches a
  float. Inference must never be allowed to type a column as numeric merely because every observed
  value parses as a number; the contract declares identifiers as strings and inference is not
  consulted for them.
- **Never lock a type from a sample.** "Infer from the first 1000 rows" is standard and wrong: row
  1001 is where the `N/A` lives. Since you are streaming the whole file anyway, infer over the
  whole file — and even then, treat the result as a *proposal for a human to put in a contract*,
  never as something applied automatically. Inference output belongs in the validation report, not
  in the load path.
- **Ambiguous dates are undecidable, not hard.** `01/02/2026` is January 2nd or February 1st and no
  amount of sampling settles it unless a value exceeding 12 appears. STACK.md already mandates
  explicit formats; the pitfall is the "helpful" fallback to `dateutil`, which will parse both and
  give you a silently wrong answer. Make the date parser **raise** on an undeclared format.
- **Locale decimals collide with delimiters.** `1.234,56` (Polish/German) versus `1,234.56` (US)
  cannot be distinguished per-value; it is a per-dataset contract property. Attempting per-value
  heuristics produces a column where some rows are 1000× off — the worst kind of numeric error
  because totals still look roughly plausible.
- **Booleans and NULLs are locale- and source-specific.** `TAK/NIE`, `Y/N`, `1/0`, `T/F`,
  `true/FALSE`; and `NULL`, `N/A`, `NA`, `-`, `` , `NIL`, `\N`. Both sets are **configuration**
  (§16, §17), per dataset, and both must distinguish "this token means null" from "this is a
  literal value" — a `-` is a genuine value in some columns and a null marker in others. Never
  hard-code the list.
- **Every conversion failure preserves the raw value.** The bad-record path (§51) stores the
  original string, the target type, the rule that failed and the row ordinal. Without the raw value
  the record is unreprocessable and §62 replayability is a fiction.

---

### E6. "Streaming" implementations that still use O(file) memory

**Severity:** REWORK (OOM kills at the worst possible moment — mid-load, per A2)
**Retrofit cost:** MEDIUM
**Phase:** 5 (shape) / 8 (dedup at scale)
**Confidence:** HIGH

The requirement is bounded memory (§39). The things that quietly break it:

| Accumulator | Why it appears | Bounded alternative |
|---|---|---|
| A Python `set` of row hashes for in-file dedup | The obvious way to do §26 exact-row dedup | Load all rows to staging, dedupe with SQL `DISTINCT ON` (C1). The database is the right place for a large set; 10M hex digests in a Python set is >1 GB. |
| A list of bad records / validation errors | You want them all in the report | Stream bad records to a quarantine writer as they occur; keep only a **capped sample** (e.g. first 100 per rule) plus counters in memory |
| A per-column value-frequency map for profiling/anomaly detection (§53) | Statistics need a pass over the data | Bounded sketches or a fixed top-N; and profile from the *staging table* in SQL after load, not in the streaming loop |
| `list(reader)`, `rows = [...]`, or a pandas DataFrame anywhere in the path | Convenience | Generators throughout; STACK.md's choice of stdlib `csv` already points this way |
| The chunk itself | `chunk_size` chosen without arithmetic | 50k rows × 40 columns × ~50 bytes ≈ 100 MB **before** any per-row objects. Chunk size must be derived from the pod's memory limit, not picked round. |
| Log/metric records accumulated per row | Observability retrofitted naively | Aggregate counters; log per chunk, never per row (this also saves the DAG log from becoming unreadable) |

**Prevention that actually holds:** write the test. Generate a synthetic file well beyond the pod's
memory limit (say 5 GB / 20M rows) with the fixture generator you need anyway, run the processor in
a pod with the real limit, and assert peak RSS stays under it. Put this in the *nightly* suite, not
the PR suite (G's CI budget). Without this test, "streaming" is an adjective.

---

### E7. De-risking ARCHITECTURE.md's assumption #3 — streaming throughput under pod limits

**Severity:** REWORK if it fails late (the chunking/loading design would need to change)
**Retrofit cost:** CHEAP to test now, EXPENSIVE to discover in Phase 8
**Phase:** 5 — a timeboxed spike, before the engine is elaborated
**Confidence:** N/A (this is the experiment, not a claim)

ARCHITECTURE.md lists "streaming CSV parsing with per-chunk `COPY` hits acceptable throughput
inside a kind pod's resource limits" as its third unvalidated assumption. Make it a concrete,
half-day spike with pre-declared pass criteria rather than an ambient worry:

**Experiment:** generate a 2 GB / ~8M-row synthetic CSV in MinIO; run the E1 reader shape →
normalize → `COPY` to an unlogged all-TEXT staging table, in a pod with the intended
`container_resources` (start at 1 CPU / 1 Gi). Measure rows/second, peak RSS, and the split between
parse time and `COPY` time.

**Pre-declared responses:**

| Observation | Response |
|---|---|
| Peak RSS near the limit | Reduce chunk size first (E6 arithmetic), then raise the limit |
| `COPY` dominates | Use binary `COPY` or `psycopg` pipeline mode; check you are not doing per-row round trips (C11) |
| Parsing dominates | Confirm you are not decoding twice or building per-row dicts; stdlib `csv` at ~1M rows/s of simple rows is the realistic ceiling |
| Throughput acceptable | Record the number in the repo as the baseline; a later 5× regression is then detectable |

The point is not the number. It is that the number exists **before** Phase 8 depends on it, and
that the pass/fail criteria were written before the result was known.


---

## F. Observability — short-lived pods, cardinality, and trace context

*Low-to-medium retrofit cost, except the cardinality decision (F2), which is expensive to unwind
once dashboards and alerts are written against the wrong label set.*

---

### F1. Prometheus cannot scrape a pod that lives for 90 seconds — and Pushgateway is the wrong fix

**Severity:** REWORK (you build the metrics, then discover they never arrive)
**Retrofit cost:** MEDIUM — changes where metrics are produced
**Phase:** 8 (observability), designed in 5
**Confidence:** MEDIUM-HIGH — the scrape/lifetime mismatch is inherent; the recommendation is
reasoned from this project's specific assets

**What goes wrong:** the pull model needs the target alive at scrape time. With a 30 s scrape
interval and task pods that exist for seconds to minutes, you get zero, one, or a partial sample —
non-deterministically. Worse, the *final* values (rows loaded, rows rejected) are produced
immediately before exit and are the ones you always miss. Teams then reach for Pushgateway, which
STACK.md already rejected, and correctly: Pushgateway has no staleness semantics, so metrics from a
dead job persist forever, `up` becomes meaningless, and it silently becomes a second, conflicting
system of record for numbers that must be authoritative.

**Prevention — two tiers, matching the two kinds of metric this project has:**

| Metric kind | Examples | Where it comes from |
|---|---|---|
| **Business / data metrics** (must be exact, must survive, must be queryable) | rows read / loaded / rejected / deduplicated, batch counts, control totals, freshness lag | The **metadata database**, written inside the publication transaction (ARCHITECTURE's commit point). A small long-lived **exporter** runs SQL against it and exposes gauges. Prometheus scrapes the exporter, which is always alive. |
| **Technical / runtime metrics** (best-effort, high-frequency) | parse rate, peak RSS, chunk durations, retry counts | **Pushed via OTLP** from the task pod to the long-lived OTel collector you already deploy, which exports to Prometheus. No new component. |

This is a strictly better arrangement than Pushgateway for three reasons that are worth stating in
the roadmap: the authoritative numbers have exactly one source (the transaction that made them
true); a metrics outage cannot lose data, only visibility; and §83 lineage questions
("where did this row come from?") are answered by SQL against the same tables, so the metric and
the lineage answer can never disagree.

**Consequence:** the metadata schema must carry the counters *before* the observability phase — one
more reason ARCHITECTURE's AP8 ("deferring the metadata schema") is right.

**Warning signs:** metrics that appear only for long-running tasks; `rate()` over a counter that
resets to zero every pod; a Grafana panel whose number disagrees with `SELECT count(*)`.

---

### F2. Cardinality explosion from per-file and per-run labels

**Severity:** REWORK (Prometheus OOMs on a cluster that has no memory to spare — see A2)
**Retrofit cost:** EXPENSIVE-ish — dashboards, alerts and recording rules all reference the labels
**Phase:** 8, but the **rule** must be written down in 5
**Confidence:** HIGH — this is the best-documented Prometheus failure mode there is

**What goes wrong:** `csv_rows_loaded_total{dataset="x", file="customers_20260811_001.csv"}` looks
obviously useful. Each distinct file name creates a new time series that Prometheus keeps in memory
and on disk **forever** (subject to retention), even though the file will never be seen again.
Ten thousand files means ten thousand dead series per metric. On a kind cluster where Prometheus
has a 1–2 Gi limit, this is an OOM within weeks — and per A2, the OOM may take something else with it.

**Prevention — a single, enforceable rule:**

> **Metric labels may only contain values from a small, closed, enumerable set. Everything with
> unbounded cardinality lives in the metadata database, not in a label.**

| Allowed as labels (bounded) | Forbidden as labels (unbounded) |
|---|---|
| `dataset`, `source_system`, `country` | `file_name`, `object_key`, `run_id`, `batch_id` |
| `status` (`ok`/`warn`/`fail`/`quarantine`) | `error_message`, `exception_text` |
| `stage` (`parse`/`validate`/`dedup`/`load`) | `business_key`, `row_hash`, `schema_hash` |
| `schema_version` (small integer) | `pod_name` (churns with every task) |

Note `pod_name` specifically: Kubernetes service discovery adds pod labels automatically, so a
per-task-pod scrape target *is itself* a cardinality source. That is a second, independent reason
F1's exporter-based design is right — one stable target instead of thousands of ephemeral ones.

**Detection:** add a Grafana panel or a periodic check on
`topk(10, count by (__name__)({__name__=~".+"}))` and on `prometheus_tsdb_head_series`. If head
series grows monotonically with the number of files processed, you have already made the mistake.
Set a hard `sample_limit` on the scrape config so a bad exporter is rejected rather than absorbed.

---

### F3. Trace context does not cross the KubernetesPodOperator boundary by itself

**Severity:** ANNOYANCE (traces exist but are disconnected — which is worse than no traces, because
it looks like it works)
**Retrofit cost:** CHEAP if the assignment document carries the field from day one
**Phase:** 8; **add the field in 5**
**Confidence:** MEDIUM — the W3C mechanism is standard and certain; how much of Airflow 3.3's own
task span is available inside the operator is the uncertain part, so design to not depend on it

STACK.md establishes that OTel trace context does **not** propagate into KPO pods. The mechanics of
fixing it:

1. **Carry `traceparent` in the assignment document** that ARCHITECTURE.md already writes to MinIO,
   and *also* as an environment variable on the pod. The document is the durable copy (so a replay
   can be correlated to the original run); the env var is the convenient copy.
   ```python
   from opentelemetry.propagate import inject
   carrier: dict[str, str] = {}
   inject(carrier)                       # produces {"traceparent": "00-<32hex>-<16hex>-01"}
   assignment["trace"] = carrier
   ```
2. **Extract in the pod before creating any span:**
   ```python
   from opentelemetry.propagate import extract
   ctx = extract(assignment["trace"])
   with tracer.start_as_current_span("csv.process", context=ctx):
       ...
   ```
3. **Do not depend on Airflow's own span being current** inside the operator. If it is, `inject()`
   picks it up and you get one connected trace for free. If it is not, you still get a coherent
   trace rooted at a span you created explicitly in the task. Design for the second case; enjoy the
   first if it happens. This removes the risk from an area STACK.md flagged as "aspirational".
4. **Put the `trace_id` in the metadata database run row and in every structured log record.** This
   is the cheapest, highest-value part of the whole tracing effort: it lets an operator go from a
   SQL row → a trace → the logs, and it keeps working even if the tracing backend is down or
   sampled away. If the tracing work has to be cut for time, keep this.

**Span-volume trap:** one span per row is catastrophic (millions of spans, collector OOM, and the
collector is competing for the same memory as everything else per A2). Emit spans per **stage** and
per **chunk**, with row counts as attributes. Set an explicit sampler; never leave it at
always-on for a 20M-row file.

---

### F4. Airflow's own metrics have a single-backend constraint you must design around

**Severity:** ANNOYANCE
**Retrofit cost:** CHEAP
**Phase:** 8
**Confidence:** MEDIUM-HIGH — STACK.md establishes that Airflow emits metrics to only one backend

Because Airflow emits to one backend, the *platform* metrics (scheduler heartbeat, task duration,
queue depth) and your *data* metrics may end up in different places if you are careless. Decide
once: send Airflow's metrics to the OTel collector (which also receives your pod pushes) and let
the collector be the single fan-out point to Prometheus. Then one Grafana data source shows both,
and the §82 dashboard is buildable. Choosing StatsD for Airflow and OTLP for everything else means
maintaining two pipelines and correlating by hand.

**Also:** build **five** dashboard panels first — pipeline success rate, rows loaded per dataset per
day, freshness lag per dataset, quarantine rate, and task duration p95 — and prove they answer real
questions before building out the full §82 set. A 30-panel dashboard nobody reads is a common and
expensive way to feel productive (see H2).


---

## G. CI/CD — ephemeral kind, scanners, and build context

*Mostly cheap to fix, with one expensive exception: a flaky E2E suite that people learn to ignore
is very hard to recover trust in.*

---

### G1. Verify the disk claim on the runner rather than trusting any number

**Severity:** REWORK (CI fails at the last step, after 15 minutes, every time)
**Retrofit cost:** CHEAP
**Phase:** 10
**Confidence:** MEDIUM — STACK.md's claim that **disk**, not CPU, is the binding constraint matches
the shape of the problem (a ~2 GB Airflow image plus two Postgres images plus MinIO, Vault and the
kind node image on a runner whose free space is a fraction of its total); the exact free-space
figure varies by runner image revision and should be measured, not quoted

**What to do instead of estimating:** make the first two steps of the E2E job print the truth, and
keep them forever — they cost two seconds and turn every future capacity failure into a data point:

```yaml
- run: df -h / /mnt || true
- run: docker system df || true
```

Then apply the reclaim step **before** creating the cluster (STACK.md lists the directories). Fail
the job early with a clear message if free space is below a threshold you set, rather than letting
it die inside `kind load` with `no space left on device`.

**Second-order effect worth planning for:** because disk is binding, the CI profile cannot simply
be "the local stack with fewer replicas". Every image you add to the CI path costs more than its
runtime footprint suggests. This is an argument for the CI profile omitting the monitoring stack
entirely (as STACK.md already specifies) and for the E2E asserting *data* outcomes, not
observability outcomes.

---

### G2. The GitHub Actions cache will not save you on the first build of a PR

**Severity:** ANNOYANCE
**Retrofit cost:** CHEAP
**Phase:** 10
**Confidence:** MEDIUM-HIGH — GitHub's cache scoping (branch caches read from the base branch but
not from sibling branches) and a repository-wide cache size limit are documented behaviours; the
exact limit should be re-checked

**What goes wrong:** `cache-from/to: type=gha` makes the *second* build fast. The first build on a
new PR branch can only read the base branch's cache, and a repo that pushes many large image layers
will evict its own cache. So the honest expectation is: warm builds ~1–2 minutes, cold builds the
full build time, and cold happens more often than people assume.

**Prevention:** keep the images genuinely thin — the extended Airflow image should be a *thin layer*
over `apache/airflow:<pinned>` (STACK.md), and `csv-processor` should be a slim Python base plus a
wheel, not a build environment. Order Dockerfile layers so dependency installation (rarely changes)
precedes source copy (changes every commit); with `uv`, copy `pyproject.toml`/`uv.lock` and install
before copying `src/`. This single ordering decision matters more than any cache configuration.

---

### G3. The synthetic fixture corpus will fight both the secret scanner and the Docker build context

**Severity:** ANNOYANCE that becomes REWORK (a scanner that cries wolf gets globally disabled, and
then a real secret gets through)
**Retrofit cost:** CHEAP now; expensive once thousands of fixture files exist
**Phase:** 10, but the **corpus layout** decision belongs in 6–7
**Confidence:** MEDIUM-HIGH

**Two problems from the same cause — a large corpus of realistic-looking synthetic data:**

1. **Secret-scanner false positives.** gitleaks' generic high-entropy and card/IBAN-shaped rules
   will fire on synthetic account numbers, UUIDs, base64-ish blobs and long identifiers. The
   tempting fix — a global allowlist regex or `--no-verify` — disables the control you built the
   requirement for (§81.11). **Prevention:**
   - **Path-scope every allowlist** to `tests/fixtures/**` in `.gitleaks.toml`. Never allowlist a
     *pattern* globally.
   - **Make synthetic secrets recognisable by construction**: generate them from a seeded PRNG with
     a fixed, documented prefix (e.g. every synthetic token starts `SYNTH_`), and allowlist that
     prefix, path-scoped. Now the allowlist is precise and auditable.
   - STACK.md notes `gitleaks-action` v3 requires a paid licence for organisation repositories —
     invoke the gitleaks **binary** in a plain step instead; same engine, no licence question.
   - Add a deliberate **negative test**: commit a canary secret in a branch and assert CI fails.
     A scanner nobody has ever seen fail is not known to work.
2. **Docker build context bloat.** If fixtures live anywhere under the build context, every
   `docker build` uploads them and any change to any fixture invalidates the cache.
   **Prevention:** a `.dockerignore` covering `tests/`, `.planning/`, `.git/`, `docs/` — and,
   better, **do not commit large fixtures at all**. §73's corpus should be a *generator* plus a
   small set of hand-crafted golden files for the pathological cases (BOM, embedded newlines,
   ragged rows, NUL bytes, mixed encodings). Generated-on-demand fixtures are reproducible from a
   seed, cost nothing in the repository, and — importantly — can be generated at *any size*, which
   is what E6's memory test needs.

---

### G4. E2E flakiness has five specific causes here, and all five are fixable

**Severity:** REWORK — a suite people rerun until it passes provides negative value
**Retrofit cost:** CHEAP per cause, expensive as accumulated habit
**Phase:** 10
**Confidence:** MEDIUM-HIGH — causes are derived from the specific mechanics elsewhere in this
document

| Cause | Fix |
|---|---|
| Image pull exceeding `startup_timeout_seconds` on the first task (B3) | Load images before the cluster runs anything; raise the timeout; assert images are present on the node before triggering |
| Triggering a DAG before the DAG processor has parsed it | Poll until the DAG appears **and** is unpaused, with a timeout — never `sleep 30` |
| `kubectl wait` racing resource creation | `kubectl wait` fails if the object does not exist yet; wait for existence, then for condition |
| Asserting on log text | Assert on **database state** — row counts, batch status, control totals. This project has a metadata control plane; use it as the test oracle. Log assertions break on every message tweak |
| Cluster resource starvation on a 4 CPU runner | The CI values profile (STACK.md) plus explicit requests; if the scheduler cannot place a pod, the failure looks like a timeout |

Two structural rules: **hard timeouts on every wait** so a hung E2E fails in ten minutes rather than
six hours, and **always upload diagnostics on failure** (`kubectl get events -A`, `describe pods`,
all pod logs, plus a dump of the metadata tables). The diagnostics bundle is what makes a flaky
failure diagnosable after the fact instead of requiring reproduction.

**Budget rule:** PR CI runs lint, type check, unit, integration and manifest validation. E2E on an
ephemeral cluster runs on merge to `main` and nightly. A 20-minute PR gate will be routed around;
a 4-minute one will not.

---

### G5. Two images, two dependency sets, one chance to get the split right

**Severity:** ANNOYANCE → REWORK
**Retrofit cost:** MEDIUM
**Phase:** 5 (when the second image first exists)
**Confidence:** HIGH — follows directly from STACK.md's finding that Airflow's constraints pin
`pandas==2.1.4`, which argues for separate images

The pitfall is *drift*, not the split itself: the `csv_processor` package ends up installed in both
images at different versions, so a DAG-side import and a pod-side import behave differently, and a
bug reproduces in one and not the other. **Prevention:** the DAG image contains only what DAG files
import (which, per B11, is almost nothing — the DAG should not import the processor's heavy
modules at all). If the DAG needs shared constants or the assignment-document schema, factor those
into a **third, tiny, dependency-free package** that both images pin to the same version. Assert
version equality in CI. This is a small amount of packaging work in Phase 5 that prevents a
recurring class of "works in the pod, fails in the DAG".


---

## H. Project-level — where builds of this ambition actually stall

*Confidence: MEDIUM throughout. These are judgements about effort and value, informed by the
README's own §93 warning and by the shape of the DoD, not by external measurement. They are stated
opinionatedly because a hedged version would be useless.*

---

### H1. The five stall patterns

**Severity:** REWORK, at project scale
**Retrofit cost:** N/A — this is about sequencing
**Phase:** roadmap construction itself
**Confidence:** MEDIUM

1. **Infrastructure yak-shaving before any row has been loaded.** README §92's phases 1–4 are all
   platform; §93 then says, emphatically, build the vertical slice first. The two are in tension,
   and ARCHITECTURE.md already resolves it by deviating from §92. The stall pattern is spending
   three weeks perfecting Vault policies, Prometheus dashboards and network policies with **zero
   rows in PostgreSQL** — at which point motivation is spent and every subsequent decision has been
   made without feedback from real data. **Countermeasure:** make "one real CSV lands in the
   warehouse via a pod" the *earliest possible* milestone, with Vault stubbed behind the seam it
   will later occupy, and treat the date it happens as the project's key metric.
2. **Treating the 114-item DoD as a burn-down list.** Horizontal completion of a layer feels like
   progress and produces nothing usable. The DoD is an *acceptance* checklist, not a work
   breakdown. **Countermeasure:** every phase must end with something that runs end to end; DoD
   items get ticked as side effects, never as goals.
3. **The CSV engine becoming a CSV parser project.** FEATURES.md already lists this as an
   anti-feature. The gravitational pull is real: dialect detection and encoding heuristics are
   *fun*, unbounded, and produce no business value past the point where the contract can override
   them. **Countermeasure:** the contract-overrides-detection policy (E2–E4) is not only a
   correctness rule, it is a scope fence. Detection exists to *disagree*, and its job is done when
   it can do that.
4. **Observability before there is anything to observe.** Dashboards built against imagined metrics
   get rebuilt. Worse, per A2, an unconstrained monitoring stack destabilises the cluster you are
   trying to develop on. **Countermeasure:** structured logs with a correlation ID and the metadata
   tables from day one (they cost nothing extra — see F1); Prometheus/Grafana/OTel *after* the
   pipeline works.
5. **A hand-crafted fixture corpus.** §73's 29 fixtures plus FEATURES.md's additions are
   maintainable by hand; the "grown as cases are found" requirement is not, and neither is E6's
   multi-GB memory test. **Countermeasure:** a seeded generator from the start (G3), with
   hand-crafted files reserved for pathological byte-level cases that a generator cannot express.

---

### H2. DoD items with the worst value-to-effort ratio

**Severity:** ANNOYANCE (time spent, not damage done) — but time is the scarcest resource here
**Retrofit cost:** N/A
**Phase:** roadmap prioritisation
**Confidence:** MEDIUM — these are opinions, offered as a starting point for negotiation, not as
instructions to cut

| Item | Why it is expensive | Cheaper thing that captures most of the value |
|---|---|---|
| **End-to-end OTel tracing across Airflow → pod → processor → PostgreSQL** (§82) | STACK.md already flags that context does not propagate; every hop is bespoke; the collector competes for scarce cluster memory | F3's step 4 alone: `trace_id` in the metadata row and in every log line. Delivers the "explain this run" capability at ~5% of the effort. Add real spans later if the correlation ID proves insufficient |
| **The full §82 metric set as dashboards** | 30 panels is days of work and most are never opened | Five panels (F4). Add panels when a question is asked twice |
| **Anomaly detection (§53)** | Even "simple statistical thresholds" need history, tuning and a false-positive story | Volume/row-count thresholds per dataset from the contract, evaluated as an ordinary DQ rule. Defer distribution-based detection |
| **Network policies (§88 "where practical")** | On kind, with a CNI that must support them, this is fiddly and proves little locally | One policy: task pods may reach PostgreSQL, MinIO and Vault, nothing else. Demonstrates the capability; skip the matrix |
| **Property-based tests everywhere (§72)** | Hypothesis strategies for complex domain objects are slow to write and slow to run | Concentrate them where they genuinely find bugs: the CSV round-trip (write→parse→compare), the normalizer, the change-hash (equal inputs ⇒ equal hash; different ⇒ different), and SCD2 fold (interval non-overlap invariant). Four strategies, high yield |
| **Data retention (§64, §91)** | Two sections, real design work, zero local benefit — nothing will age out during this milestone | A documented policy and a `retention_days` column. Implement the deletion job only if a dataset actually grows |
| **SCD Type 0 / Type 1 as separately pluggable strategies** | The abstraction costs more than the code it abstracts | Type 2 done properly; Types 0 and 1 fall out as degenerate cases of the same fold (C8) |
| **A CDC "framework" with no CDC source** | Everything arrives as files; a general CDC abstraction is speculative generality | CDC as a *file format with op-type and sequence columns* (which is what it will actually be), feeding the same SCD fold. §29's requirement is satisfied; the framework is not built until a second source exists |

**The inverse list — items whose value is understated by their one-line DoD entry:** the metadata
control plane, run-scoped idempotency (C5), the change-hash version column (C6), the SCD2 exclusion
constraint (C7), control totals and reconciliation (§45/§46, the only thing that catches C10), and
the fixture generator. These are where the project's actual differentiation lives.

---

### H3. Ordering mistakes that cause expensive rework

**Severity:** REWORK
**Retrofit cost:** the whole point of this entry
**Phase:** roadmap construction
**Confidence:** MEDIUM-HIGH — each is derived from a specific dependency established elsewhere in
this document

| If you do this… | …you will pay for it here | Do this instead |
|---|---|---|
| Defer `kind/cluster.yaml` decisions (mounts, kubelet reservations, registry) past Phase 1 | A4, A2, B10 — each fix requires cluster recreation, destroying accumulated state | Make **every** cluster-shape decision in Phase 1, even for things not wired until Phase 3 |
| Build validation (Phase 7) before idempotency (Phase 8) | Validation output is metadata that must be run-scoped and re-emittable; retrofitting run identity into it means rewriting the reports and their tables | Establish run identity in the Phase 5 slice; validation then writes into a model that already exists |
| Add Dynamic Task Mapping before the file manifest exists | B6 — expansion over a live listing, then a painful migration to manifest-driven expansion after runs exist | Manifest first (§41), mapping second. They are one phase apart at most |
| Write the SCD2 loader before deciding change-hash versioning | C6 — every stored hash is invalidated by the first recipe change | `hash_version` column exists from the first hash ever stored |
| Create the SCD2 dimension table without the exclusion constraint | C7 — the constraint cannot be added once overlaps exist | Constraint in the creating migration |
| Turn on Prometheus/Grafana/OTel before resource requests are set on everything | A2 — the monitoring stack is `BestEffort` and gets evicted precisely when needed | Requests and limits everywhere first; monitoring second |
| Build the fixture corpus before the parser API stabilises | Every fixture's test harness gets rewritten | A generator plus a handful of golden files; the harness is one function |
| Write runbooks (§89) at the end | You will have forgotten the failures; the runbook becomes fiction | One runbook entry per failure you actually hit, written the day it happens. This is the cheapest possible discipline and produces the most credible artefact in the repository |

---

### H4. The honest risk that no checklist captures

**Confidence:** MEDIUM (judgement)

This platform's genuine difficulty is not any single item — it is that **correctness properties
compose multiplicatively**. Idempotency × concurrency × late-arriving data × schema evolution ×
CDC ordering × backfill is not six features; it is one very large state space, and each pair
interacts. §74's edge-case list and §84's failure list are, read carefully, a request to test that
product.

Two practical consequences for the roadmap:

1. **Make the interactions explicit as test scenarios, not as features.** "Backfill a range that
   overlaps an already-loaded batch, while a late correction for the same business key arrives, with
   a schema change in between" is one test. Three or four such compound scenarios are worth more
   than fifty single-property tests, and they are the ones that will actually fail.
2. **Prefer designs that make interactions impossible over designs that handle them.** The
   recurring pattern in this document is exactly that: the database constraint that makes overlaps
   impossible (C7); the advisory lock that makes the concurrency race impossible (C1); the frozen
   manifest that makes non-deterministic expansion impossible (B6); the recompute-from-events fold
   that makes non-idempotent correction impossible (C8). Each replaces a class of test with a
   guarantee. When a choice arises between "handle it carefully" and "make it unrepresentable",
   take the second — it is the only approach that scales to a state space this size.


---

## Technical Debt Patterns

Shortcuts that seem reasonable and are sometimes right. The column that matters is the last one.

| Shortcut | Immediate benefit | Long-term cost | When acceptable |
|---|---|---|---|
| Single-node kind locally | Faster start, no `kind load` fan-out, less memory | Never exercises scheduling, node affinity, PV node-binding (A4) or pod distribution — all of which the README's "production-like" claim rests on | Only for CI. STACK.md already draws this line |
| `kind load docker-image` instead of a local registry | One less container to manage | A5's stale-image class, 3× disk (A3), slow inner loop | Only for the CI single-node cluster |
| Vault dev mode locally | No unseal, instant | Loses configuration on every restart (D1); never exercises the sealed path §90 needs | Acceptable **if** `make vault-bootstrap` is idempotent and sealed mode is proven once |
| `errors="replace"` when decoding | Files stop failing | Silent U+FFFD corruption in the warehouse (E2) | **Never** |
| `COPY … FORMAT csv` directly from the file | One line, very fast | Voids validation, dedup and hashing guarantees (C3) | **Never** |
| Deduplicating with a Python `set` | Simple, obvious | Unbounded memory (E6) | Only for files with a declared small row bound, and only with an assertion enforcing it |
| Mutable `:dev` image tag | No tag plumbing | A5 | Only inside a single uncommitted debugging session |
| Skipping `hash_version` | One less column | C6 — invalidates all history when the recipe changes | **Never**. It is one column |
| SCD2 without the exclusion constraint | Fewer test failures early | C7 — silent double-counting, unrepairable history | **Never** for a dimension you intend to trust |
| Metrics labelled by file name | Trivially useful dashboards | F2 cardinality explosion | **Never**; put it in the metadata DB |
| Global allowlist in the secret scanner | Green CI | G3 — the control stops working | **Never**; path-scope it |
| Business logic in DAG files | Fast prototyping | Slow parsing (B11), untestable, breaks the §6.4 rule | Only in a throwaway spike, never merged |
| `sleep` in an E2E test | Makes it pass today | G4 — permanent flakiness | **Never**; poll with a timeout |
| Deferring the metadata schema | Get a pipeline running sooner | ARCHITECTURE AP8; and F1's metrics have nowhere to live | **Never** — the schema *is* the vertical slice |

---

## Integration Gotchas

| Integration | Common mistake | Correct approach |
|---|---|---|
| **MinIO / S3** | Using the `ETag` as content identity | ETag is not an MD5 for multipart uploads. Compute `sha256` while streaming (C5) |
| **MinIO / S3** | `iter_lines()` or `read().splitlines()` to stream a CSV | Byte stream → `BufferedReader` adapter → `TextIOWrapper(newline="")` → `csv.reader` (E1) |
| **MinIO / S3** | Assuming list-then-process is atomic | New objects appear between listing and processing; freeze the manifest (B6) |
| **PostgreSQL** | `MERGE` for concurrent upsert | `INSERT … ON CONFLICT` on the natural key, under an advisory lock (C1) |
| **PostgreSQL** | `ON CONFLICT` arbitrating on the surrogate PK | Arbitrate on the constraint that actually defines uniqueness (C1) |
| **PostgreSQL** | Long transaction wrapping the whole load | Slow work outside; short publication transaction (C2) |
| **PostgreSQL** | Letting a killed pod hold locks indefinitely | `idle_in_transaction_session_timeout`, `lock_timeout`, TCP keepalives (C2) |
| **Vault** | Chasing the `iss` claim error | Vault ≥1.9 disables issuer validation by default; look at TokenReview RBAC, audience, and the bound SA instead (D3) |
| **Vault** | Trusting that the Airflow backend is in the path | It falls through to the metadata DB and env vars; test with those removed (D4) |
| **Vault** | Putting `data` in the KV v2 path | The backend inserts it; use `mount_point`/`connections_path` without it (D4) |
| **Vault** | Assuming rotation propagates | Cache TTL delays it; env-injected secrets never update in a running pod; the other side of a static credential does not change at all (D5) |
| **Airflow / KPO** | Assuming the task pod inherits the worker's ServiceAccount | Set `service_account_name` and `namespace` explicitly; grant cross-namespace RBAC (B5) |
| **Airflow / KPO** | Relying on the XCom sidecar without writing `return.json` | Write it unconditionally in a `finally`; treat XCom as a receipt, not as state (B2) |
| **Airflow / KPO** | Default `startup_timeout_seconds` with a large image | Raise it and pre-pull (B3) |
| **Airflow / KPO** | Default pod deletion | `on_finish_action="delete_succeeded_pod"` (B4) |
| **Airflow** | Secrets or config reads at module scope | Everything heavy inside `@task` callables (B11, D4) |
| **Airflow** | `.expand()` over a live listing | Expand over a frozen manifest (B6) |
| **Airflow Helm** | Letting the chart template the Fernet / secret key | Pre-create the Secrets and reference by name (B8) |
| **Airflow Helm** | Per-component image tags | Override `defaultAirflowTag`; assert uniformity (B9) |
| **Prometheus** | Scraping task pods | Long-lived exporter over the metadata DB, plus OTLP push (F1) |
| **OpenTelemetry** | Assuming trace context crosses into the pod | Carry `traceparent` in the assignment document and extract it (F3) |

---

## Performance Traps

| Trap | Symptoms | Prevention | When it breaks |
|---|---|---|---|
| Node capacity over-commitment on kind | Exit 137 with no Kubernetes event; unrelated pods restarting | Kubelet reservations + requests on everything (A2) | As soon as the full stack runs concurrently |
| Pod amplification (executor pod + KPO pod) | Queued tasks, nodes at `maxPods` | Pools sized in pods; tiny worker requests (B1) | Any fan-out beyond ~10 concurrent files |
| Top-level DAG imports | DAG processor CPU pinned; "last parsed" drifting | Heavy imports inside callables; parse-time budget test (B11) | Immediately, and worsens with every DAG added |
| Unbounded map length | `Map length exceeds maximum`; scheduler slowdown | Batch the manifest; cap explicitly (B6) | ~1024 files, or far earlier on this hardware |
| Long publication transaction | Cluster-wide bloat; queries degrading over weeks | Short publication transaction (C2) | Files large enough to make the load exceed ~1 minute |
| `executemany` instead of `COPY` | "Streaming is too slow" | Per-chunk `COPY` (C11) | Any file over ~100k rows |
| `ATTACH PARTITION` validation scan | A "instant" publish taking minutes with an exclusive lock | Matching `CHECK` constraint before attach (C4) | Partitions over a few hundred thousand rows |
| SCD2 `is_current` update churn | Table size growing faster than row count | Partial index; per-table autovacuum tuning (C11) | After a few full-dimension refreshes |
| In-memory dedup set | RSS climbing linearly with rows; OOM kill | Dedupe in SQL (E6) | ~1–5M rows depending on the limit |
| Metric cardinality from file names | Prometheus RSS climbing monotonically | Bounded labels only (F2) | A few thousand distinct files |
| One span per row | Collector OOM; traces unusable | Spans per stage/chunk with counts as attributes (F3) | Immediately on the first large file |
| Docker build context including fixtures | Every build slow; cache never hits | `.dockerignore` + generated fixtures (G3) | As soon as the corpus exceeds a few hundred MB |

---

## Security Mistakes

Domain-specific; general container/web hygiene is assumed.

| Mistake | Risk | Prevention |
|---|---|---|
| Widening the Vault role to `bound_service_account_names: ["*"]` to make auth work | Every workload can read every secret; §81 least-privilege becomes decorative | Fix the actual cause (B5/D3); keep a **negative** test that an unauthorised SA is denied (§81.12) |
| Relying on the Airflow secrets-backend chain without proving Vault is in it | You believe secrets are managed; they are actually in the metadata DB (D4) | Delete the DB connection and env vars in the acceptance test |
| Global secret-scanner allowlist to silence fixture noise | A real credential passes the scanner | Path-scoped allowlists plus a recognisable synthetic prefix; a canary test that CI fails on a real-looking secret (G3) |
| Vault dev-mode root token in shell history, Makefile echo, or CI logs | Trivial privilege escalation in any shared environment; a bad habit that survives into production patterns | Token from a gitignored env file even in dev; `set +x` around anything that touches it (D7) |
| Unseal keys committed or logged | Full compromise of the secret store | Gitignored `.secrets/`; CI uses dev mode and never holds keys (D2) |
| No audit device enabled in Vault | §81.8 unmet; no answer to "who read this secret?" | `vault audit enable file` in the bootstrap script (D1) |
| Loader database role with excessive rights | A bug in the ETL can drop the warehouse, or reach the Airflow metadata DB (§4 violation) | Least-privilege role, marker-table assertion, negative test (C12) |
| Logging record contents on validation failure | PII/secrets in logs; §70 forbids it | Log the **rule**, the column and the row ordinal; the value goes to the quarantine store, not the log |
| Real data used "just once" for testing | PII in a repository that is otherwise safe to share | The corpus is synthetic by construction (PROJECT.md); enforce with a generator, not a policy |

---

## Operator-Experience Pitfalls

There is no end-user UI — Airflow's own is the interface. The equivalent failures are about the
person operating and debugging the platform.

| Pitfall | Impact | Better approach |
|---|---|---|
| A failure that can only be diagnosed by reading logs | §37 explicitly requires partial-failure recovery to be determinable **without** reading logs | Every stage writes its outcome to the metadata tables; the runbook's first step is a SQL query, not a `kubectl logs` |
| Pods deleted on failure | Nothing to inspect (B4) | `delete_succeeded_pod` |
| Silent quarantine | Records disappear with no visible signal | Quarantine counts surfaced as a metric and as a run status; a run with quarantined rows is `PASS_WITH_WARNING`, never `PASS` |
| Generic exceptions | Every failure looks the same; retry policy cannot distinguish infrastructure from data | §71's exception hierarchy, used to drive retry decisions (D6) |
| "It broke this morning" with no starting point | Hours lost to environment drift | `make doctor` (A7) asserting clock, sysctls, disk, context, registry |
| Reprocessing requires bespoke SQL | Recovery is risky and unrepeatable | A documented, parameterised reprocess command that uses the same pipeline (§33 forbids a bypass) |

---

## "Looks Done But Isn't" Checklist

- [ ] **Streaming:** verified with a file larger than the pod's memory limit, measuring peak RSS — not merely written as a generator (E6)
- [ ] **Idempotency:** the same file processed twice produces identical warehouse state *and* identical metadata, including after a kill between the staging load and the publication commit (C5)
- [ ] **Multiline CSV fields:** tested across a chunk boundary at chunk sizes 1, 2 and 3 (E1)
- [ ] **Encoding:** a Windows-1250 file with Polish diacritics round-trips exactly; a truncated multi-byte sequence raises rather than replacing (E2)
- [ ] **Vault integration:** works with the metadata-DB connection deleted and `AIRFLOW_CONN_*` unset (D4)
- [ ] **Least privilege:** a negative test proves an unauthorised ServiceAccount is denied (D3)
- [ ] **Concurrency:** two overlapping batches of the same dataset run simultaneously without duplicates or deadlock (C1)
- [ ] **SCD2:** the exclusion constraint exists and a deliberate overlap attempt is rejected by the database (C7)
- [ ] **Late correction:** applying the same correction twice yields the same result as applying it once (C8)
- [ ] **Watermark:** a row committed out of timestamp order is still picked up (C10)
- [ ] **Backfill:** a backfilled batch carries the business date from the data, never `now()` (B7)
- [ ] **Dynamic mapping:** clearing and rerunning a mapped task produces the same map length (B6)
- [ ] **Metrics:** the exporter's numbers equal `SELECT count(*)`, and head-series count does not grow with file count (F1, F2)
- [ ] **Cluster rebuild:** `make cluster-rebuild` from scratch, timed, with a smoke DAG passing at the end (A4)
- [ ] **Secret scanning:** a canary secret in a branch makes CI fail (G3)
- [ ] **Runbook:** every failure scenario in §84 has an entry with symptom, diagnosis, recovery and verification

---

## Recovery Strategies

| Pitfall | Recovery cost | Recovery steps |
|---|---|---|
| inotify exhaustion (A1) | LOW | Raise sysctls, `wsl --shutdown`, recreate cluster |
| Disk exhaustion in the VHDX (A3) | LOW–MEDIUM | Prune host daemon **and** node containerd; enable sparse VHD; compact the VHDX from Windows with the distro stopped |
| Cluster state lost on delete (A4) | LOW if `make cluster-rebuild` exists; HIGH if not | Rebuild + reseed; if it was not scripted, this is the moment to script it |
| Stale image served (A5) | LOW | Move to immutable tags; `crictl rmi` the stale tag on every node |
| Fernet key regenerated (B8) | MEDIUM | Restore the key from Vault if stored; otherwise delete and recreate affected Variables. Vault-backed Connections are unaffected |
| Schema half-migrated (B9) | MEDIUM–HIGH | Restore the Airflow metadata DB from a CNPG backup; re-run migrations with a single, correct image tag |
| Duplicates loaded from a non-idempotent retry (C5) | MEDIUM | Delete by `run_id`/`attempt` (which is why those columns exist), re-run. If they are absent, this is HIGH and manual |
| Change-hash recipe changed without `hash_version` (C6) | **HIGH** | Recompute hashes for all rows from the immutable raw layer; where raw is unavailable, the affected history is not recoverable |
| Overlapping SCD2 intervals (C7) | **HIGH** | Rebuild the affected keys' history from the event log (C8's fold makes this routine; without it, manual) |
| Rows silently missed by the watermark (C10) | MEDIUM–HIGH | Full reload of the affected window from raw; detectable only via control totals (§46) |
| Mojibake loaded (E2) | MEDIUM | Reprocess from the immutable raw layer with the correct encoding; this is precisely the case §62 replayability exists for |
| Prometheus OOM from cardinality (F2) | LOW–MEDIUM | Delete the TSDB, fix the labels; historical series are not worth saving |

Note how many recovery paths reduce to "reprocess from the immutable raw layer". That is the
strongest argument for §63 being built early rather than treated as a nicety.

---

## Pitfall-to-Phase Mapping

| Pitfall | Prevention phase | Verification |
|---|---|---|
| A1 inotify | 1 | Bootstrap script asserts sysctl values |
| A2 capacity lies | 1 + 2 | `kubectl describe node` allocatable is bounded; no BestEffort pods |
| A3 disk | 1 | `make doctor` checks free space; `clean-images` prunes node containerd |
| A4 PV persistence | 1 | Timed `make cluster-rebuild` in the runbook |
| A5 image tags | 1–3 | Running pod image equals current git SHA |
| A7 clock drift | 1 | `make doctor` |
| A8 kubeconfig | 1 | Every script pins `--context`; teardown removes entries |
| B1 pod amplification | 3 | Pool limits observed under a fan-out test |
| B2 XCom sidecar | 3 | Receipt written in `finally`; sidecar image local; PSS-compatible |
| B3 startup timeout | 3 | First-task-after-rebuild succeeds |
| B4 pod cleanup | 3 | A failed task leaves an inspectable pod |
| B5 SA / namespace | 3 + 4 | Negative Vault test |
| B6 mapping determinism | 5 (manifest) / 8 (mapping) | Clear-and-rerun yields identical map length |
| B7 backfill dates | 5 (rule) / 8 (backfill) | Business date never derived from the clock |
| B8 keys | 2 | Two consecutive `helm upgrade`s leave the Secret unchanged |
| B9 migrations / tags | 2 | Uniform image tag assertion in CI |
| B10 DAG distribution | 1 (mount) / 3 (wiring) | DAG edit visible without a rebuild locally; baked image in CI |
| B11 parse time | 3, enforced from 5 | Per-file DagBag import time budget test |
| C1 concurrency | 5 / 8 | Two overlapping batches test |
| C2 transaction shape | 5 / 8 | No transaction over N seconds; timeouts configured |
| C3 no SQL CSV parsing | 5 | CI grep for `FORMAT csv` |
| C4 partition attach | 8 | Publication duration bounded |
| C5 idempotency | 5 | Double-run and kill-and-retry tests |
| C6 hash versioning | 5 | `hash_version` present in the first migration |
| C7 SCD2 overlap | 8 (migration) / 9 | Constraint exists; deliberate overlap rejected |
| C8 late corrections | 9 | Correction applied twice is idempotent |
| C9 CDC ordering | 9 | Shuffled input yields identical history |
| C10 watermark | 8 | Out-of-order-commit test; control totals reconcile |
| C12 wrong database | 2 | Marker-table assertion at processor startup |
| E1 chunking | 5 | Multiline fields across chunk boundaries |
| E2 encoding | 5 (policy) / 6 | Strict decoding; U+FFFD canary rule |
| E3 dialect | 6 | Consistency-scored detection; contract override |
| E4 header/footer | 6 / 7 | Footer row quarantined, not loaded |
| E5 type inference | 6 / 7 | Leading zeros preserved; ambiguous dates raise |
| E6 memory | 5 / 8 | Peak-RSS test on an oversized file (nightly) |
| E7 throughput spike | 5 | Baseline number recorded in the repo |
| F1 metric collection | 5 (schema) / 8 | Exporter numbers equal SQL counts |
| F2 cardinality | 5 (rule) / 8 | Head-series count flat as files accumulate |
| F3 trace context | 5 (field) / 8 | `trace_id` in metadata rows and logs |
| G1 CI disk | 10 | `df -h` printed; threshold gate |
| G3 scanner / context | 6–7 (layout) / 10 | Canary test; `.dockerignore` |
| G4 E2E flakiness | 10 | No `sleep`; DB-state assertions; diagnostics uploaded |
| G5 image drift | 5 | Shared-package version equality assertion |

---

## Sources

Verified 2026-08-11 unless noted. Confidence tags on individual entries reflect these.

**Official documentation**

- kind — Known Issues (inotify limits `fs.inotify.max_user_watches`/`max_user_instances`, WSL2
  cgroup v2, disk/memory pressure, `kind load --name`): https://kind.sigs.k8s.io/docs/user/known-issues/
- Apache Airflow — `KubernetesPodOperator` operator guide (XCom sidecar reading
  `/airflow/xcom/return.json`, `xcom_sidecar_container_security_context`, "XComs are only pushed
  for tasks that reach a success state", `dry_run`): providers/cncf/kubernetes docs, via Context7
- Apache Airflow — Task SDK `BaseOperator` (`do_xcom_push` default `True` at the base class), via Context7
- Apache Airflow — cncf/kubernetes provider changelog (`@task.kubernetes` default namespace now
  `None`, resolving to the cluster namespace when `in_cluster=True`), via Context7
- Apache Airflow — Helm chart documentation and `chart/values.yaml`
  (`defaultAirflowTag`, `migrateDatabaseJob.useHelmHooks`, `applyCustomEnv`, `fernetKey`,
  `webserverSecretKey`, `waitForMigrations`): https://airflow.apache.org/docs/helm-chart/stable/
- HashiCorp Vault — Kubernetes auth method and HTTP API (`disable_iss_validation` defaulting to
  true since Vault 1.9, `token_reviewer_jwt`, `kubernetes_host`, bound service accounts):
  https://developer.hashicorp.com/vault/docs/auth/kubernetes
- HashiCorp support — "Issuer is invalid after Kubernetes upgrade to 1.21":
  https://support.hashicorp.com/hc/en-us/articles/4412703197075
- PostgreSQL — UPSERT wiki page and `MERGE`/`INSERT … ON CONFLICT` documentation:
  https://wiki.postgresql.org/wiki/UPSERT
- Python — `csv` module documentation (the `newline=''` requirement, `field_size_limit`,
  `Sniffer` limitations)

**Issues, mailing lists and post-mortems**

- microsoft/WSL#4232 — `/etc/sysctl.conf` values not applied on WSL2 start
- microsoft/WSL#4293 — increasing inotify watches on WSL2
- apache/airflow#27561 — Helm chart tries to patch immutable Job resources on `helm upgrade`
- apache/airflow#28637, #27992 — `run-airflow-migrations` job behaviour on upgrade
- apache/airflow#40573 — Helm chart DB migration failures
- apache/airflow#52267 — `api` `secret_key` capabilities replacing `webserver` for 3.0+
- hashicorp/vault-helm#562, cert-manager#4144 / #6150, external-secrets#721 — the Kubernetes 1.21
  bound-token `iss`/audience class of failures and their resolution
- PostgreSQL BUG #18279 and the pgsql-hackers thread — duplicate key violations and deadlocks with
  `ON CONFLICT DO UPDATE` when multiple unique indexes exist (single arbiter index)

**Project documents (not duplicated here)**

- `.planning/research/STACK.md`, `ARCHITECTURE.md`, `FEATURES.md`
- `README.md` §§4, 5, 8–17, 19–27, 29–39, 41–46, 51, 53, 55–63, 65, 67, 70–75, 81–90, 92–94
- `.planning/PROJECT.md` (measured environment, WSL relocation, CI runner sizing constraint)

**Reasoned, not externally sourced** (flagged MEDIUM/LOW in the entries themselves): the kubelet
reservation values in A2, the two-tier metrics design in F1, the recompute-from-events SCD design
in C8, the DoD value judgements in H2, and the ordering table in H3.


---

*Pitfalls research for: local Kubernetes Airflow CSV ingestion platform*
*Researched: 2026-08-11*
