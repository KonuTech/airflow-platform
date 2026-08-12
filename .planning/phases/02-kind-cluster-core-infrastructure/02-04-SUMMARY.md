---
phase: 02-kind-cluster-core-infrastructure
plan: 04
subsystem: infra
tags: [minio, s3, boto3, helm, iam-policy, kubernetes-secrets, kind]

# Dependency graph
requires:
  - phase: 02-kind-cluster-core-infrastructure
    provides: "02-01: kind/cluster.yaml, helm/versions.env, scripts/cluster-up.sh, scripts/stages/*.sh, scripts/helm-install.sh, scripts/wait-for.sh, the cluster uv dependency group"
  - phase: 02-kind-cluster-core-infrastructure
    provides: "02-02: tests/e2e/cluster/ harness (conftest.py skip-if-no-cluster, kubectl/kubectl_json fixtures, make cluster-verify), make doctor"
provides:
  - "scripts/minio-credentials.sh — ensure/show subcommands (D-14): credentials generated directly into Kubernetes Secrets, never written to the working tree, read back on demand"
  - "make minio-creds — shell-sourceable live credential printer"
  - "helm/values/{local,ci}/minio.yaml — five buckets, versioning on raw, single etl-app IAM policy (allow+explicit deny), existingSecret references only"
  - "scripts/stages/60-minio.sh — credentials-then-chart-then-readiness stage, reachable as make stage-minio"
  - "tests/e2e/cluster/conftest.py — s3_client fixture is now a LIVE factory (admin|app), no address/credential hardcoded"
  - "tests/e2e/cluster/test_minio_buckets.py — INFRA-05 and §63 proved live: five buckets, round trip, versioning, deny-delete negative + positive control"
  - "kind/cluster.yaml JoinConfiguration fix — D-03 worker node-role labels now actually apply on cluster creation (were previously silently dropped)"
affects: [02-06, 02-07, 02-08]

# Tech tracking
tech-stack:
  added: [minio chart 5.4.0, pgsty/minio image, boto3 s3 client (live)]
  patterns:
    - "Credential Secret generated with values piped to `kubectl apply -f -` on stdin (stringData YAML built in-process) — never a CLI argument, never a file in the working tree"
    - "s3_client(credential) factory fixture: reads live Secrets via a subprocess call to the same shell script host tooling uses, so tests and humans read credentials through one path"
    - "Single IAM policy document with an explicit Allow (bucket-level ListBucket + object-level Get/Put) and an explicit Deny (DeleteObject/DeleteObjectVersion scoped to one bucket) — the chart's default effect is Allow, so the deny is always spelled out"

key-files:
  created:
    - scripts/minio-credentials.sh
    - helm/values/local/minio.yaml
    - helm/values/ci/minio.yaml
    - scripts/stages/60-minio.sh
    - tests/e2e/cluster/test_minio_buckets.py
  modified:
    - Makefile
    - kind/cluster.yaml
    - tests/e2e/cluster/conftest.py

key-decisions:
  - "Split the etl-app policy's Allow into two statements instead of the one shown in 02-RESEARCH.md's code example: a bucket-level ListBucket (needed for HeadBucket/ListObjectsV2 to actually succeed) plus an object-level Get/Put — the research example only had object-level ARNs, which is valid JSON but under-permissions ListBucket in AWS-style IAM semantics. Verified live: HeadBucket against raw/validated succeeds with the app credential."
  - "etl-app's Allow list is deliberately raw+validated only (not all five buckets) — processed/quarantine/metadata are proven reachable with the admin credential in tests/e2e/cluster/test_minio_buckets.py, matching how later phases will write to those layers and giving the negative-vs-admin test pair something real to distinguish."
  - "s3_client is a factory fixture (`s3_client(\"admin\")` / `s3_client(\"app\")`), not two separate fixtures — one signature for both credentials, matching the plan's 'takes a credential selector' wording."

requirements-completed: [INFRA-05]

# Metrics
duration: ~55min active execution across two sessions (a transport/API interruption occurred after Task 2's commit; resumed cleanly with zero rework — see Deviations)
completed: 2026-08-12
---

# Phase 2 Plan 4: MinIO Object Storage — Five Buckets, Versioning, Deny-Delete Summary

**MinIO (pgsty/minio fork, chart 5.4.0) live on the storage worker with `raw`/`validated`/`processed`/`quarantine`/`metadata`, versioning enabled on `raw` alone, and a single IAM policy that lets the pipeline's own credential read/write its working buckets while a server-enforced Deny refuses it any delete against `raw` — proved through boto3 against the live cluster, including the negative delete case and its admin-credential positive control.**

## Performance

- **Duration:** ~55 min active execution across two sessions (transport interruption after Task 2's commit; verified nothing was lost and resumed at Task 3)
- **Tasks:** 3/3 complete and fully verified live against the shared kind cluster
- **Files modified:** 8 (5 created, 3 modified — one of the three, `kind/cluster.yaml`, outside this plan's declared scope; see Deviations)

## Accomplishments

- `scripts/minio-credentials.sh`: `ensure` creates `minio-root` (`rootUser`/`rootPassword`) and `minio-app` (`secretKey`) Secrets in namespace `data` if and only if they do not already exist — verified idempotent (unchanged `resourceVersion` across three consecutive runs) — with every value piped to `kubectl apply -f -` on stdin so nothing ever lands in a process listing or the working tree; `show` reads them back for `make minio-creds` and the e2e fixture
- `helm/values/{local,ci}/minio.yaml`: standalone MinIO pinned to `pgsty/minio:RELEASE.2026-08-04T00-00-00Z`, five buckets with `raw` alone versioned, a single `etl-app` policy combining an explicit Allow (bucket-level `ListBucket` + object-level `Get`/`Put` on `raw`/`validated`) with an explicit Deny (`DeleteObject`/`DeleteObjectVersion` scoped to `raw`), `existingSecret` references only, ingress on `minio.localtest.me`, and requests+limits on all four containers (main deployment plus the three post-job containers, whose chart defaults carry a memory-only request)
- `scripts/stages/60-minio.sh`: credentials ensured, then `helm upgrade --install`, then a readiness wait — `make stage-minio` verified idempotent against the live cluster and the health endpoint returns `200` through the ingress
- `tests/e2e/cluster/conftest.py`: `s3_client` is now a live factory built from `scripts/minio-credentials.sh show`, never hardcoding an address or credential — `S3_ENDPOINT_URL` resolved per D-07, defaulting to the ingress host
- `tests/e2e/cluster/test_minio_buckets.py`: five tests, all passing live — all five buckets exist and are reachable (app credential where its policy allows, admin otherwise), a byte-identical round trip through `s3://raw/<key>`, versioning `Enabled` on `raw` and not on `validated`, the app credential's delete denied with the object still retrievable afterward, and the admin credential's delete succeeding as the positive control
- **Found and fixed a real cluster-wide bug** (outside this plan's declared file scope, Rule 1): worker nodes' `kind/cluster.yaml` kubeadmConfigPatches used `InitConfiguration` for `node-labels`, which is a no-op for nodes that join via `kubeadm join` rather than `kubeadm init` — D-03's storage/analytics placement had silently never taken effect on any cluster since plan 02-01. Fixed the file for future rebuilds and applied the labels at runtime (non-destructively) to unblock this plan's live verification without recreating the cluster shared with plan 02-03's concurrent work.

## Task Commits

1. **Task 1: Generate MinIO's credentials into the cluster and nowhere else** - `9c68cea` (feat)
2. **Task 2: MinIO with five buckets, versioning on `raw`, and a server-enforced deny on deleting from `raw`** - `c600905` (feat) — includes the Rule 1 `kind/cluster.yaml` fix
3. **Task 3: Prove the object store through boto3, including the refusal** - `a28feb0` (feat)

## Files Created/Modified

- `scripts/minio-credentials.sh` - `ensure`/`show` subcommands; Secrets created via stdin, never a CLI arg or a file
- `helm/values/local/minio.yaml`, `helm/values/ci/minio.yaml` - five buckets, versioning on `raw`, the `etl-app` allow+deny policy, ingress, sized containers
- `scripts/stages/60-minio.sh` - credentials → chart install → readiness wait
- `tests/e2e/cluster/conftest.py` - `s3_client` factory fixture made live
- `tests/e2e/cluster/test_minio_buckets.py` - the five required live assertions
- `Makefile` - `minio-creds` target (`.PHONY`, joins neither `check` nor `ci`)
- `kind/cluster.yaml` - Rule 1 fix: worker `InitConfiguration` → `JoinConfiguration` for `node-labels`

## Decisions Made

- Split the canonical policy example's single Allow statement into a bucket-level `ListBucket` statement plus an object-level `Get`/`Put` statement (see frontmatter `key-decisions`) — verified live that `HeadBucket` needs the bucket-level ARN to succeed for the app credential.
- Kept the `etl-app` Allow scoped to `raw`+`validated` only, matching the research's canonical example; `processed`/`quarantine`/`metadata` are proven reachable with the admin credential in the e2e suite.
- `s3_client` is one factory fixture parameterized by a credential-selector string argument, not two separate fixtures, per the plan's own wording ("takes a credential selector").

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `kind/cluster.yaml` worker node-labels never applied — wrong kubeadm patch kind**

- **Found during:** Task 2, `make stage-minio` — the MinIO pod stayed `Pending` with `FailedScheduling: 2 node(s) didn't match Pod's node affinity/selector` against `nodeSelector: {airflow-platform/role: storage}`.
- **Issue:** Both worker nodes' `kubeadmConfigPatches` declared `node-labels` under `kind: InitConfiguration`. `InitConfiguration` only applies to the node that runs `kubeadm init` — the control-plane. Workers join via `kubeadm join`, which reads `JoinConfiguration` instead; `InitConfiguration` is silently ignored for them. Verified live: `docker exec <worker> cat /var/lib/kubelet/kubeadm-flags.env` showed `--node-labels=` empty on both workers, while the control-plane's own `ingress-ready=true` (correctly under `InitConfiguration` there) was present. This means D-03's worker-role placement (storage/analytics) has never actually taken effect on any cluster built from this repository since plan 02-01 first committed `kind/cluster.yaml` — the live cluster this plan and plan 02-03 both depend on had been scheduling everything on default node selectors the whole time.
- **Fix:** Changed both worker node blocks' `kind: InitConfiguration` to `kind: JoinConfiguration` in `kind/cluster.yaml` for future `cluster-up`/`cluster-rebuild` correctness. Since labels are a creation-time-only field (D-01/D-02's own stated invariant) and the cluster was live and shared with plan 02-03's concurrent CNPG work, did **not** delete/recreate the cluster — instead applied the same two labels at runtime via `kubectl label node ... --overwrite` (non-destructive, immediately effective) to unblock this plan's live verification. Both worker pods reachable via the intended nodeSelector rescheduled and became `Running`/`Bound` within seconds, including plan 02-03's own pending `airflow-db-1` pod as a side effect.
- **Files modified:** `kind/cluster.yaml` (root-cause fix); runtime label applied directly via `kubectl label` (no file)
- **Verification:** `tests/policy/test_kind_cluster_config.py` still 6/6 green after the edit (it only asserts the `ingress-ready` label, which stayed on `InitConfiguration` and is unaffected); `kubectl get node -o json` showed both `airflow-platform/role` labels present; the MinIO pod and PVC both became `Running`/`Bound`
- **Committed in:** `c600905` (Task 2 commit)

---

**Total deviations:** 1 (Rule 1 — a bug found while making this plan's own required live verification pass, in a file outside this plan's declared scope but directly blocking it, matching the precedent set by plan 02-02's own `kind/cluster.yaml` fix)
**Impact on plan:** Necessary for correctness of D-03's node placement, which this plan's `nodeSelector: {airflow-platform/role: storage}` on MinIO directly depends on. No scope creep beyond making this plan's own stated acceptance criteria hold; the fix also unblocked plan 02-03's concurrent, unrelated work as an incidental benefit.

## Issues Encountered

A transport/API error interrupted the agent mid-response while editing `tests/e2e/cluster/conftest.py`'s `s3_client` fixture (after Task 2's commit `c600905` had already landed cleanly). On resume: `git status --porcelain` was clean (the in-progress, uncommitted edit was lost, not silently corrupted), both prior task commits were intact, and the live MinIO deployment was still healthy. Re-did the `s3_client` fixture edit and Task 3 from scratch with no need to redo Tasks 1 or 2.

## User Setup Required

None. Both host-environment blockers from earlier plans (cgroup v1, kubelet reservation sizing) were already resolved; this plan's own blocker (the `InitConfiguration`/`JoinConfiguration` bug) was fixed without requiring any manual host action — see Deviations.

## Known Stubs

None. Every file this plan commits is the real, intended implementation.

## Threat Flags

None beyond what this plan's own `<threat_model>` already names (T-02-16 through T-02-20, T-02-SC) — no new network endpoints, auth paths, or trust-boundary-crossing surface was introduced.

## Next Phase Readiness

**Fully verified, file-level and live.** `scripts/minio-credentials.sh ensure` is idempotent (three consecutive runs, unchanged Secret `resourceVersion`) and leaves a clean `git status --porcelain`; `make minio-creds` prints a shell-sourceable assignment for all four values; `./tools/bin/gitleaks dir --redact --no-banner --exit-code 1 .` exits 0. `make stage-minio` is idempotent against the live cluster; the MinIO Deployment reports `1/1` `Available`; `curl -H 'Host: minio.localtest.me' http://127.0.0.1/minio/health/live` returns `200`; `helm template` of both profiles renders exactly five buckets with `versioning: true` on `raw` only, an explicit `"Effect": "Deny"` scoped to `raw`, and every one of the four containers (main + three post-job) carrying both CPU and memory requests and limits. `uv run --frozen --group cluster pytest tests/e2e/cluster -q` passes 10/10 (5 inherited + 5 new); `make cluster-verify` green; `make check` green with `tests/e2e/` still uncollected there.

The `kind/cluster.yaml` `JoinConfiguration` fix (Deviation 1) means every later plan in this phase that relies on `airflow-platform/role` node placement (both CNPG clusters via D-03, and any future Airflow component pinning) will get correct scheduling from the next `cluster-up`/`cluster-rebuild` — not just from the live-patched runtime labels this session applied. Plans 02-06 through 02-08 inherit: a live MinIO instance with the exact Secret names (`minio-root`, `minio-app`) Phase 5's Vault retrofit is designed to replace without a redesign, and a `tests/e2e/cluster/` suite with a proven `s3_client` factory pattern any later plan's own S3-touching tests can reuse.

---
*Phase: 02-kind-cluster-core-infrastructure*
*Completed: 2026-08-12*

## Self-Check: PASSED

- All 8 claimed files verified present on disk (`scripts/minio-credentials.sh`, `helm/values/{local,ci}/minio.yaml`, `scripts/stages/60-minio.sh`, `tests/e2e/cluster/test_minio_buckets.py`, `Makefile`, `kind/cluster.yaml`, `tests/e2e/cluster/conftest.py`)
- All three task commits verified present in `git log`: `9c68cea`, `c600905`, `a28feb0`
