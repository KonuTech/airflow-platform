---
phase: 02-kind-cluster-core-infrastructure
plan: 06
subsystem: infra
tags: [airflow, helm, cloudnativepg, kubernetes-secrets, kind, helm-hooks]

# Dependency graph
requires:
  - phase: 02-kind-cluster-core-infrastructure
    provides: "02-01: kind/cluster.yaml, helm/versions.env, scripts/{helm-install,wait-for,cluster-up}.sh, scripts/stages/{10,20,30}-*.sh, the cluster uv dependency group"
  - phase: 02-kind-cluster-core-infrastructure
    provides: "02-03: the CloudNativePG operator, Cluster/airflow-db (PostgreSQL 17, namespace data), the airflow-db-app Secret shape"
provides:
  - "scripts/airflow-metadata-secret.sh — the one hand-written adapter of the phase: derives the chart's `connection` Secret from the CNPG-generated `-app` Secret, generates the Fernet and API secret keys once"
  - "helm/values/{local,ci}/airflow.yaml — Airflow 3.3.0 chart values, bundled postgresql subchart off, executor recorded as the argued fourth D-06 divergence axis"
  - "scripts/stages/70-airflow.sh — the last stage in the D-09 bootstrap spine, reachable as make stage-airflow"
  - "[Rule 3 fix] scripts/helm-install.sh helm_install() gained an optional 6th wait-strategy argument (default watcher, unchanged for the other seven stages) — the Airflow chart deadlocks under watcher and needs hookOnly"
  - "[Rule 3 fix] scripts/wait-for.sh gained wait_for_statefulset_ready — the JSONPath-condition wait the triggerer needs, since StatefulSets carry no Available-style condition the way Deployments do"
  - "[Rule 3 fix] Makefile's stage-% pattern rule glob tightened from *-$**.sh to *-$*.sh — the looser form matched 50-airflow-db.sh against `make stage-airflow` as well as 70-airflow.sh"
  - "tests/e2e/cluster/test_airflow_workloads.py — INFRA-02 proved live: four workloads with the D-16-corrected kinds, running version, migrated metadata schema, ingress reachability, no bundled Postgres"
affects: [02-07, 02-08]

# Tech tracking
tech-stack:
  added: [apache-airflow helm chart 1.22.0, apache/airflow image 3.3.0, KubernetesExecutor (local) / LocalExecutor (CI)]
  patterns:
    - "helm_install()'s wait-strategy is now a caller-selectable 6th argument rather than a hardcoded constant — the shape a future chart with the same post-install-hook-behind-an-initContainer pattern would reuse"
    - "Cross-namespace Secret derivation performed host-side by the developer's own kubeconfig, never by an in-cluster identity — no ServiceAccount/Role/RoleBinding created, explicitly deferred to Phase 5's Vault retrofit"
    - "Fernet key generated with openssl rand -base64 32 | tr '+/' '-_' — no python/cryptography dependency needed for a value this constrained"

key-files:
  created:
    - scripts/airflow-metadata-secret.sh
    - helm/values/local/airflow.yaml
    - helm/values/ci/airflow.yaml
    - scripts/stages/70-airflow.sh
    - tests/e2e/cluster/test_airflow_workloads.py
  modified:
    - scripts/helm-install.sh
    - scripts/wait-for.sh
    - Makefile
    - .planning/REQUIREMENTS.md

key-decisions:
  - "[Rule 1 — deviation from the plan's literal wording] Used `apiSecretKeySecretName` rather than the plan's stated `webserverSecretKeySecretName` for the API secret key. Read from the pinned chart's own templates/_helpers.yaml (not from documentation, per the plan's own read_first instruction): `webserverSecretKeySecretName` only takes effect when `semverCompare \"<3.0.0\" .Values.airflowVersion` is true — for airflowVersion 3.3.0 it is silently inert. `apiSecretKeySecretName` is the chart's Airflow-3+ contract (`AIRFLOW__API__SECRET_KEY`, Secret key `api-secret-key`). The Secret NAME (`airflow-api-secret-key`) the plan's acceptance criteria actually check is unaffected either way."
  - "[Rule 3 — blocking, discovered live] helm_install()'s hardcoded `--wait=watcher` deadlocks the Airflow chart outright: watcher waits for the chart's own Deployments/StatefulSet to reach Ready BEFORE Helm ever runs post-install hooks, but every one of those workloads carries a wait-for-airflow-migrations initContainer gated on the migration Job, itself a post-install hook. Watcher can never reach the hook that would unblock what it is waiting for. Observed live: the first `make stage-airflow` attempt timed out at the full 15m with every workload `Progress deadline exceeded` and zero Job ever created. Fixed by giving `helm_install()` an optional wait-strategy override (default `watcher`, unchanged for every other stage) and passing `hookOnly` for Airflow specifically; the stage script then proves the four workloads itself via `scripts/wait-for.sh`."
  - "[Rule 3 — blocking, discovered live] The Makefile's `stage-%` pattern rule used `find ... -name '*-$**.sh'`, which for `make stage-airflow` matched BOTH `70-airflow.sh` and `50-airflow-db.sh` (the middle wildcard let `-airflow-db.sh` satisfy `-airflow*.sh`). `$script` held two filenames and the rule tried to execute the concatenation. Tightened to `*-$*.sh` (no middle wildcard, name must immediately precede `.sh`) — verified unambiguous for all eight existing stage names."

requirements-completed: [INFRA-02]

# Metrics
duration: ~2h45m active execution, including two live cluster-wide verification passes (in-place and a full cold `make cluster-rebuild`)
completed: 2026-08-12
---

# Phase 2 Plan 6: Airflow 3.3.0 — Last Component of the Phase Summary

**Airflow 3.3.0 (chart 1.22.0, image override over the chart's declared appVersion 3.2.2) runs against the CloudNativePG PostgreSQL 17 cluster as four separately schedulable workloads — three Deployments and a StatefulSet for the triggerer — reachable through the ingress; getting there required discovering and fixing a real Helm 4 `--wait=watcher` deadlock against this exact chart's post-install-hook-behind-an-initContainer shape, proved by both an in-place stage re-run and a full cold `make cluster-rebuild`.**

## Performance

- **Duration:** ~2h45m of active execution, including live debugging of the wait-strategy deadlock and two full end-to-end verification passes
- **Tasks:** 3/3 complete and fully verified against the live cluster, including a cold cluster-rebuild
- **Files modified:** 9 (5 created, 4 modified — 3 of the 4 modifications outside this plan's declared `files_modified` list; see Deviations)

## Accomplishments

- `scripts/airflow-metadata-secret.sh ensure`: derives Secret `airflow-metadata` (key `connection`) from the CNPG-generated `airflow-db-app` Secret in namespace `data`, URL-encoding the username/password (verified against a reserved-character password locally) and namespace-qualifying the host (`airflow-db-rw.data`) for cross-namespace DNS resolution from pods in namespace `airflow`. Generates `airflow-fernet-key` and `airflow-api-secret-key` once, left alone on every later `ensure`. Verified live: idempotent across two consecutive runs (all three Secrets' `resourceVersion` unchanged), and the assembled connection string round-trips through `psql` from inside the cluster.
- `helm/values/{local,ci}/airflow.yaml`: `airflowVersion`/`defaultAirflowTag` overridden to `3.3.0`, `postgresql.enabled: false`, all three Secrets referenced by name only, `statsd.enabled: false`, `triggerer.enabled: true`, ingress at `airflow.localtest.me`, and every container in the enumerated Pitfall-5 minimum key set given explicit CPU/memory requests and limits in both profiles (verified: zero unsized containers in either rendered profile).
- `scripts/stages/70-airflow.sh`: waits for `Cluster/airflow-db` Ready, runs the metadata-secret adapter, installs the chart, then proves the four workloads itself.
- **Found and fixed a genuine Helm 4 deadlock** (Rule 3, see Deviations): `helm_install()`'s hardcoded `--wait=watcher` can never succeed on this chart's first install. Root-caused live by watching the stuck `helm upgrade --install` process, confirming zero Jobs were ever created, and reading Helm's own hook-lifecycle ordering (main resources loaded and waited-on BEFORE post-install hooks run). Fixed with a caller-selectable wait-strategy argument.
- **Found and fixed a Makefile pattern-matching bug** (Rule 3, see Deviations) that this plan's own new `70-airflow.sh` filename collided with `50-airflow-db.sh` under the pre-existing `stage-%` glob.
- `tests/e2e/cluster/test_airflow_workloads.py`: six tests proving INFRA-02 live — the D-16-corrected workload kinds, four genuinely separate pod sets, `airflow version` reporting `3.3.0` from inside the running api-server, a migrated metadata schema (alembic_version present, `public` schema non-trivial), the UI answering through the ingress, and no Bitnami legacy image anywhere in the namespace.

## Task Commits

1. **Task 1: The metadata-connection adapter** - `bd13f1b` (feat)
2. **Task 2: Airflow 3.3.0 from the pinned chart, bundled subchart off** - `4c8e65e` (feat) — includes the two Rule 3 fixes (helm-install.sh/wait-for.sh, Makefile)
3. **Task 3: Prove the four workloads** - `8b0e799` (test)

Live verification complete on two separate cluster states: the cluster left running by prior plans (in-place `make stage-airflow`, twice, idempotent), and a full `make cluster-rebuild` from a destroyed cluster (647s total, within the ~900s budget, cold image pull included) followed by `make cluster-verify` — 21/21 tests green both times.

## Files Created/Modified

- `scripts/airflow-metadata-secret.sh` - the one hand-written adapter (Secret derivation + Fernet/API key generation)
- `helm/values/local/airflow.yaml`, `helm/values/ci/airflow.yaml` - Airflow 3.3.0 chart values, both profiles
- `scripts/stages/70-airflow.sh` - the Airflow component stage
- `tests/e2e/cluster/test_airflow_workloads.py` - INFRA-02 proved live (6 tests)
- `scripts/helm-install.sh` - **[Rule 3 fix]** optional wait-strategy override argument
- `scripts/wait-for.sh` - **[Rule 3 fix]** added `wait_for_statefulset_ready`
- `Makefile` - **[Rule 3 fix]** tightened the `stage-%` glob
- `.planning/REQUIREMENTS.md` - INFRA-02 marked complete

## Decisions Made

- Used `apiSecretKeySecretName` instead of the plan's literal `webserverSecretKeySecretName` wording — see frontmatter `key-decisions` for the full chart-helper citation. The Secret name the acceptance criteria check is identical either way.
- `helm_install()`'s wait-strategy is now a 6th, optional, caller-selectable argument rather than a hardcoded constant, defaulting to `watcher` so the other seven stages are byte-for-byte unaffected.
- Chose `apache/airflow` (Docker Hub, not `localhost:5001/airflow`) as `defaultAirflowRepository` in both profiles, per the plan's explicit instruction — this phase does not build a custom Airflow image, so the vanilla upstream image needs no local-registry round trip (the registry's containerd mirror config still transparently caches the pull).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `webserverSecretKeySecretName` → `apiSecretKeySecretName`**

- **Found during:** Task 2, while reading the pinned chart's `templates/_helpers.yaml` per the plan's own read_first instruction ("Take the key names the chart expects from the pinned chart's own helpers rather than from documentation").
- **Issue:** The plan's action text says to set `webserverSecretKeySecretName: airflow-api-secret-key`. Reading `_helpers.yaml` directly: `AIRFLOW__WEBSERVER__SECRET_KEY` (and therefore `webserverSecretKeySecretName`) is only wired in when `semverCompare "<3.0.0" .Values.airflowVersion` is true. For `airflowVersion: "3.3.0"` this branch never fires — the chart instead wires `AIRFLOW__API__SECRET_KEY` from `apiSecretKeySecretName`, with the Secret required to carry a key named `api-secret-key` (not `webserver-secret-key`).
- **Fix:** Used `apiSecretKeySecretName: airflow-api-secret-key` in both values files, and had `scripts/airflow-metadata-secret.sh` write the Secret with key `api-secret-key`.
- **Files modified:** `helm/values/local/airflow.yaml`, `helm/values/ci/airflow.yaml`, `scripts/airflow-metadata-secret.sh`
- **Verification:** `kubectl -n airflow get secret airflow-api-secret-key` exists with exactly key `api-secret-key`; the api-server pod started and served traffic (a wrong/missing key here would surface as a Flask secret-key error at startup)
- **Committed in:** `bd13f1b` (script), `4c8e65e` (values)

**2. [Rule 3 - Blocking] `helm_install()`'s hardcoded `--wait=watcher` deadlocks the Airflow chart**

- **Found during:** Task 2, first `make stage-airflow` attempt — timed out at the full 15m with `Error: resource Deployment/airflow/airflow-api-server not ready. status: Failed, message: Progress deadline exceeded` for all three Deployments and `InProgress` for the StatefulSet, and `kubectl -n airflow get jobs` showed zero Jobs ever created.
- **Issue:** Helm's documented hook lifecycle loads non-hook resources (the chart's Deployments/StatefulSet) and, if `--wait` requires it, waits for them to become ready — **before** running `post-install` hooks. The Airflow chart's `airflow-run-airflow-migrations` and `airflow-create-user` Jobs are both `post-install` hooks, and every one of the four workloads carries a `wait-for-airflow-migrations` initContainer that polls the migration Job's completion and never returns until it exists and finishes. `--wait=watcher` (this script's hardcoded value) waits for the workloads first, so it can never reach the hook that would let them proceed — a structural deadlock for this exact chart shape, not a timing issue.
- **Fix:** Added an optional 6th positional argument to `helm_install()` (`wait_strategy`, default `watcher`) and passed `hookOnly` from `scripts/stages/70-airflow.sh` — `hookOnly` runs and awaits the post-install hooks without first waiting on the chart's own resources, breaking the cycle. Added `wait_for_statefulset_ready` to `scripts/wait-for.sh` (StatefulSets carry no `Available`-style condition, so this uses a JSONPath wait on `.status.readyReplicas`, matching the existing convention of never using `rollout status`) and the stage script now proves all four workloads itself after the chart install returns.
- **Files modified:** `scripts/helm-install.sh`, `scripts/wait-for.sh`, `scripts/stages/70-airflow.sh`
- **Verification:** `make stage-airflow` succeeded from a `failed` prior release (264s); re-run twice more, idempotent both times; a full `make cluster-rebuild` from a destroyed cluster completed 70-airflow.sh in 264s with all four workloads Ready
- **Committed in:** `4c8e65e`

**3. [Rule 3 - Blocking] Makefile `stage-%` pattern rule matched two files for one target**

- **Found during:** Task 2, the very first `make stage-airflow` invocation, before the chart install even started — `/bin/bash: line 7: scripts/stages/50-airflow-db.sh\nscripts/stages/70-airflow.sh: No such file or directory`.
- **Issue:** `find scripts/stages -maxdepth 1 -type f -name '*-$**.sh'` for stage name `airflow` expands to `*-airflow*.sh` — a pattern with no anchor between the stage name and `.sh`. `50-airflow-db.sh` (`"50" + "-airflow" + "-db" + ".sh"`) satisfies this pattern just as much as `70-airflow.sh` does, so `$script` held two newline-separated filenames and the rule tried to execute their concatenation as one command. This bug was latent since plan 02-01's first commit and was only exposed once two stage files shared the substring `airflow`.
- **Fix:** Tightened the glob to `*-$*.sh` (dropped the middle wildcard) — the stage name must now immediately precede `.sh`. Verified against all eight existing stage files: each of `registry`, `namespaces`, `ingress-nginx`, `cnpg-operator`, `airflow-db`, `analytics-db`, `minio` and `airflow` now resolves to exactly one file.
- **Files modified:** `Makefile`
- **Verification:** `find scripts/stages -maxdepth 1 -type f -name '*-<name>.sh'` run for all eight stage names, each returning exactly one match; `make stage-airflow` and `make stage-airflow-db` both individually invocable afterward
- **Committed in:** `4c8e65e`

---

**Total deviations:** 3 (1 Rule 1, 2 Rule 3)
**Impact on plan:** All three were necessary for the plan's own stated acceptance criteria to be achievable at all — the two Rule 3 fixes are genuine blocking bugs discovered by attempting to make `make stage-airflow` succeed, matching the precedent set by plans 02-02/02-03/02-04's own `kind/cluster.yaml` fixes. No scope creep beyond what was required to make this plan's stated acceptance criteria hold.

## Issues Encountered

None beyond the three deviations above, all resolved and verified live within this session.

## User Setup Required

None.

## Known Stubs

None. Every file this plan commits is the real, intended implementation — nothing is a placeholder pending later wiring.

## Threat Flags

None beyond what this plan's own `<threat_model>` already names (T-02-23 through T-02-28) — no new network endpoints, auth paths, or trust-boundary-crossing surface was introduced. T-02-23's mitigation (never echo, never a CLI argument) is implemented in `scripts/airflow-metadata-secret.sh` exactly as specified, including the URL-encoding step that guards against CNPG-generated passwords containing URI-reserved characters.

## Next Phase Readiness

**Fully verified, file-level and live, twice.** `make stage-airflow` is idempotent (three consecutive successful runs against the live cluster, no error, workloads unaffected in shape though pod-template hashes cycle on each `helm upgrade` due to the chart's own unpinned `jwtSecret` — a pre-existing chart behavior, not something this plan changed). A full `make cluster-rebuild && make cluster-verify` cycle from a destroyed cluster passed end to end (647s total, within the ~900s D-04 budget), proving the plan's fixes hold from a cold start and not just against an already-warmed cluster. `airflow version` inside the running api-server reports `3.3.0`; `kubectl -n airflow get pods -o jsonpath='{..image}'` contains no `bitnamilegacy` reference; `helm template` of both profiles renders every container with CPU and memory requests and limits; neither values file contains a Fernet key, API secret key or connection-string literal. `uv run --frozen --group cluster pytest tests/e2e/cluster -q` passes 21/21 (15 inherited + 6 new); `make check` is green (71 policy tests, 60 unit/regression tests, fixtures verified) and still collects nothing under `tests/e2e/`.

Plans 02-07 and 02-08 inherit: a complete, live four-component Airflow deployment with the exact Secret names Phase 5's Vault retrofit is designed to replace without a redesign; the `hookOnly`-wait-strategy pattern and `wait_for_statefulset_ready` helper, reusable by any future chart with the same post-install-hook-behind-an-initContainer shape; and a corrected `stage-%` Makefile pattern that will not silently misfire again as later plans add more stage scripts whose names share substrings.

---
*Phase: 02-kind-cluster-core-infrastructure*
*Completed: 2026-08-12*

## Self-Check: PASSED

- All 5 created files verified present on disk (`scripts/airflow-metadata-secret.sh`, `helm/values/{local,ci}/airflow.yaml`, `scripts/stages/70-airflow.sh`, `tests/e2e/cluster/test_airflow_workloads.py`)
- All three task commits verified present in `git log`: `bd13f1b`, `4c8e65e`, `8b0e799`
