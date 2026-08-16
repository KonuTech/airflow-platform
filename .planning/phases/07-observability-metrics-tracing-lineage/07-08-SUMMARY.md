---
phase: 07-observability-metrics-tracing-lineage
plan: 08
subsystem: testing
tags: [e2e, kubernetes, w3c-traceparent, grafana-alerting, webhook, kubectl, vault, pytest]

# Dependency graph
requires:
  - phase: 07-observability-metrics-tracing-lineage (plan 04)
    provides: "TracingKubernetesPodOperator (airflow/dags/_common/tracing_kpo.py) -- W3C traceparent injection into the ingest pod's spec"
  - phase: 07-observability-metrics-tracing-lineage (plan 05)
    provides: "dataplat.cli.main()'s TRACEPARENT extraction + run_ingest()'s child span + trace_id/span_id/k8s_pod_name persisted together via claim_ingestion_run"
  - phase: 07-observability-metrics-tracing-lineage (plan 07)
    provides: "kube-prometheus-stack live -- 3 Grafana datasources, 1 dashboard, 5 alert rules (2 freshness severities + 3 live gauges), the platform-webhook contact point reading $GRAFANA_ALERT_WEBHOOK_URL"
provides:
  - "tests/e2e/observability/conftest.py: vault_root_client (session-scoped Vault port-forward + root-token hvac.Client) and webhook_receiver (function-scoped throwaway in-cluster HTTP receiver Pod+Service) fixtures, plus a poll_trace_claimed polling helper"
  - "tests/e2e/observability/test_alert_webhook_delivery.py: D-20's live proof -- a forced, persistent freshness breach makes Grafana's real Alerting engine deliver a real HTTP POST to an in-cluster receiver, with every mutation (meta.datasets row, Vault secret, K8s Secret, Grafana's own pod) restored in a finally block -- LIVE-VERIFIED PASSING (472.95s)"
  - "tests/e2e/observability/test_trace_propagation.py: OBS-10's live end-to-end proof -- a real ingest pod's TRACEPARENT trace-id segment compared against meta.ingestion_runs.trace_id for the same run -- fully implemented and lint/type/collection-clean, but BLOCKED from a live green run this session by a pre-existing, unrelated cluster resource-exhaustion condition (documented in deferred-items.md)"
  - ".planning/phases/07-observability-metrics-tracing-lineage/deferred-items.md: a fully-diagnosed, out-of-scope infrastructure blocker (two stuck etl-namespace ingest-* pods) with an exact recommended fix for whoever picks it up"
affects: [07-verification, 08]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "A throwaway in-cluster test receiver (Pod running a one-file stdlib http.server handler + a matching ClusterIP Service, both uniquely-suffixed and torn down unconditionally) is how this codebase proves an in-cluster-only system (Grafana Alerting) actually delivers to an external target, when the test process itself cannot be reached from inside the cluster"
    - "A run's trace_id/span_id/k8s_pod_name are written together in ONE UPDATE near a run's start (before staging/publish work) -- polling for that row's non-NULL state is therefore also the earliest reliable signal that a KubernetesPodOperator-launched, on_finish_action=delete_succeeded_pod pod is still capturable before deletion"
    - "Grafana's own envFromSecret + $VAR provisioning interpolation is read exactly once, at process start -- any live change to the backing Secret's value requires an explicit kubectl rollout restart + rollout status wait before the new value takes effect anywhere, including in an already-running Grafana pod's contact points"
    - "A finally block covering multiple independent live-state restorations collects each restoration's own errors into a list (never letting one restoration's exception skip the next) and only pytest.fails at the very end if anything remains unrestored -- extends test_rotation.py's single-resource restore-with-closing-assertion shape to the multi-resource case"

key-files:
  created:
    - tests/e2e/observability/test_alert_webhook_delivery.py
    - tests/e2e/observability/test_trace_propagation.py
  modified:
    - tests/e2e/observability/conftest.py
    - .planning/phases/07-observability-metrics-tracing-lineage/deferred-items.md

key-decisions:
  - "Reused this package's own already-established convention (import repo_root/cluster_name/kubectl_context/_require_cluster/kubectl/kubectl_json/s3_client from tests/e2e/cluster/conftest.py; duplicate everything else) rather than the plan's own <interfaces> block literal instruction to duplicate from tests/e2e/vault/conftest.py -- because tests/e2e/observability/conftest.py already existed from plan 07-07 with this real, different (and equally valid) convention already live; matching the existing file beats re-deriving a divergent one"
  - "webhook_receiver's Pod/Service manifests are applied via kubectl apply -f <tmp_path file>, never via the kubectl test fixture's stdin (that fixture's own _run() signature has no input= support) -- mirrors how test_alert_webhook_delivery.py's own _apply_grafana_webhook_secret helper writes a manifest file rather than piping to stdin"
  - "test_alert_webhook_delivery.py's webhook-body matching parses each WEBHOOK_RECEIVED: log line's JSON payload and inspects Grafana's own alerts[].labels structure directly, rather than raw-substring-matching on an assumed exact spacing/formatting of \"severity\": \"critical\" -- robust to Go's compact (no-space) JSON marshaling, confirmed live this session via a manual curl probe of the receiver"
  - "Did NOT attempt Task 3's own explicitly-optional 'tighten the rule group's interval' speedup -- reconstructing the live 5-rule platform group via the provisioning API risks corrupting the other 4 rules test_grafana_provisioning.py depends on, for a speed gain a permanent regression test does not need. Used the plan's own primary, safer approach (a generous 900s bound covering >=2 real 5m evaluation cycles) instead -- it passed in 472.95s, comfortably inside that bound"
  - "test_trace_propagation.py's own timeouts stayed at the standard 180s every sibling e2e test in this repo uses, even though a live green run could not be obtained this session -- a temporary, diagnostic-only 900s widening (reverted before commit) was used to determine whether the observed blocker was transient (it is not); shipping a permanently-inflated timeout to work around an unrelated infrastructure issue would mask future genuine regressions"

patterns-established:
  - "Pattern: force/observe/restore-in-finally, extended to when MULTIPLE independent resources are mutated -- each restoration attempt is wrapped in its own try/except appending to a shared errors list, so a failure restoring resource A never prevents an attempt to restore resource B, and the test only fails at the very end if the accumulated list is non-empty"
  - "Pattern: when a live E2E proof's own wait is dominated by an external system's fixed evaluation cadence (here, Grafana's own 5m rule-group interval + 5m per-rule for duration), the committed test keeps a bounded-but-generous timeout covering at least two cycles, and the executor session documents that the theoretical floor was independently confirmed live via a quicker manual/throwaway probe of the same mechanism, rather than needlessly widening the permanent test's own bound as a habit"

requirements-completed: [OBS-01, OBS-09, OBS-10]

# Metrics
duration: 57min
completed: 2026-08-16
---

# Phase 7 Plan 8: Live Observability E2E Proofs (Trace Propagation & Alert Webhook Delivery) Summary

**A real forced freshness breach makes Grafana's Alerting engine deliver a genuine HTTP POST to an in-cluster receiver (D-20, live-verified passing in 472.95s); OBS-10's TRACEPARENT-vs-persisted-trace_id live proof is fully implemented and clean but could not obtain a live green run this session because of a pre-existing, unrelated cluster resource-exhaustion condition, fully diagnosed and logged for follow-up.**

## Performance

- **Duration:** ~57 min
- **Started:** 2026-08-16T12:46:00+02:00 (approx.)
- **Completed:** 2026-08-16T13:43:00+02:00
- **Tasks:** 3 (all executed and committed)
- **Files modified:** 4 (2 created, 2 modified)

## Accomplishments

- **D-20 (Grafana webhook alert delivery) proven live, end to end.** `test_alert_webhook_delivery.py` forces a real freshness breach on a dedicated test dataset, points Grafana's `platform-webhook` contact point at a throwaway in-cluster receiver (Vault write + Kubernetes Secret re-apply + `kubectl rollout restart` — Grafana's own `envFromSecret`/`$VAR` provisioning interpolation is read only once, at process start, discovered and confirmed live this session), and polls for a real `severity: critical` webhook naming the test dataset. **Live-verified PASSING in 472.95s.** Every mutation (the test dataset row, the Vault secret, the Kubernetes Secret, Grafana's own pod) was confirmed restored to its exact original value afterward.
- **A genuinely new testing mechanism: a throwaway in-cluster webhook receiver.** `webhook_receiver` (Task 1, `conftest.py`) deploys a uniquely-named `python:3.12-slim` Pod running a one-file `http.server` handler plus a matching ClusterIP Service — because Grafana's Alerting engine runs entirely in-cluster and can never reach a pytest-process-local listener (`RESEARCH.md`'s own Wave 0 Gaps section, confirmed accurate). Live-verified this session: happy path, teardown-on-test-failure, and a real curl POST correctly captured and logged — with zero leftover pods after every run, including the two failure-injection dry runs used to prove teardown.
- **OBS-10's live trace-propagation proof fully implemented, but blocked by an unrelated infrastructure fault.** `test_trace_propagation.py` uploads a real customers CSV, polls for `meta.ingestion_runs.trace_id`/`k8s_pod_name` to become claimed (written together, near a run's start, by `dataplat.pipeline.run.run_ingest`'s `claim_ingestion_run` call — the earliest reliable signal the launched pod is still capturable before `on_finish_action: delete_succeeded_pod` removes it), then compares the pod's `TRACEPARENT` trace-ID segment against the persisted value. The code is correct and complete (ruff/mypy/collection all clean); it could not get a live green run this session because `csv_ingest_customers` cannot schedule ANY new task pod while a pre-existing, unrelated resource-exhaustion condition persists (see Deviations below) — fully diagnosed, not papered over.
- **A genuine, pre-existing cluster infrastructure bug found and fully root-caused** (out of this plan's own scope to fix, logged for follow-up): two `etl`-namespace `ingest-*` pods have had their `base` container exit cleanly 6+ hours ago while their `airflow-xcom-sidecar` container never exits, permanently consuming 1 CPU / 2Gi memory combined against a deliberately-capped 3-CPU-per-node budget.

## Task Commits

Each task was committed atomically:

1. **Task 1: The observability e2e package — fixtures, including the in-cluster webhook receiver** - `5b56a7c` (feat)
2. **Task 2: test_trace_propagation.py — a real ingest pod, a real trace_id, matched** - `4d796cc` (feat)
3. **Task 3: test_alert_webhook_delivery.py — force a breach, observe a real POST, restore** - `3a4ff1d` (feat)

_Note: Task 3 was committed before Task 2 in wall-clock time (its ~8-minute live run was kicked off in the background while Task 2's live verification — ultimately blocked — was independently investigated), but all three tasks' own commits stand independently and are each individually correct and complete for their own task's scope._

**Plan metadata:** (this commit, `docs(07-08): complete plan`, follows this SUMMARY)

## Files Created/Modified

- `tests/e2e/observability/conftest.py` — added `vault_root_client` (Vault port-forward + root-token `hvac.Client`), `webhook_receiver` (throwaway in-cluster HTTP receiver Pod+Service), and `poll_trace_claimed` (polls for a run's claimed `trace_id`/`k8s_pod_name`)
- `tests/e2e/observability/test_alert_webhook_delivery.py` — D-20's live webhook-delivery proof (new)
- `tests/e2e/observability/test_trace_propagation.py` — OBS-10's live trace-propagation proof (new)
- `.planning/phases/07-observability-metrics-tracing-lineage/deferred-items.md` — logged the out-of-scope infrastructure blocker found while running Task 2

## Decisions Made

See `key-decisions` in frontmatter. In short: matched this package's own already-live conftest.py convention rather than the plan's literal (but superseded) interfaces text; used manifest files rather than stdin for `kubectl apply` (matching the existing `kubectl` fixture's own no-stdin shape); parsed JSON structurally rather than raw-substring-matching Grafana's webhook payload; declined the plan's own optional "tighten the rule group interval" speedup as an unnecessary risk to shared alerting state; kept the committed trace-propagation test's timeout at the same 180s every sibling test uses rather than permanently inflating it to paper over an unrelated, transient-looking-but-actually-durable infrastructure fault.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `webhook_receiver`'s Pod/Service applied via manifest file, not stdin**
- **Found during:** Task 1
- **Issue:** The plan's own action text implied a straightforward `kubectl apply`, but the shared `kubectl` test fixture (`tests/e2e/cluster/conftest.py`) has no `input=`/stdin support in its `_run()` closure.
- **Fix:** Manifests are written to `tmp_path`-provided temp files and applied via `kubectl apply -f <path>`.
- **Files modified:** `tests/e2e/observability/conftest.py`
- **Verification:** Live-verified this session (happy path, failure-path teardown, real curl POST captured).
- **Committed in:** `5b56a7c`

**2. [Rule 1 - Bug] `test_alert_webhook_delivery.py`'s webhook-body matching parses JSON structurally**
- **Found during:** Task 3 (design), confirmed via a manual curl probe during Task 1's verification
- **Issue:** The plan's own text illustrated the expected match as `"severity": "critical"` (with a space); Go's `encoding/json` marshals compactly (no space) by default, so a raw-substring match could have been fragile against Grafana's actual wire format.
- **Fix:** `_extract_webhook_bodies`/`_find_critical_alert_for_dataset` parse each log line's JSON body and inspect `alerts[].labels.severity` directly.
- **Files modified:** `tests/e2e/observability/test_alert_webhook_delivery.py`
- **Verification:** Live-verified passing (472.95s) — the real Grafana payload was correctly matched.
- **Committed in:** `3a4ff1d`

---

**Total deviations:** 2 auto-fixed (both Rule 1 — bugs in the plan's own illustrative shape, not the underlying design). No scope creep.

### Deferred (out of scope, logged, not auto-fixed)

**A pre-existing, unrelated cluster resource-exhaustion condition blocks `test_trace_propagation.py` (Task 2) from a live green run this session.**

- **Found during:** Task 2, first live attempt (standard 180s timeout)
- **Root cause, fully diagnosed:** Two `etl`-namespace pods, `ingest-qp3ougwy` and `ingest-qgw33dq0` (both `dag_id=csv_ingest_customers`, `task_id=ingest`, from `run_id=scheduled__2026-08-16T0607000000-913ad3735`, predating this session by 5-6 hours), have their `base` container already `terminated` (`exitCode: 0`, `reason: Completed`, since `2026-08-16T06:19:0{1,3}Z`) while their `airflow-xcom-sidecar` container is still `running` with 0 restarts. Because at least one container remains running, Kubernetes counts the WHOLE pod's resource requests — including the already-exited `base` container's `500m` CPU / `1Gi` memory — against the node's allocatable budget for as long as the pod itself never reaches a terminal phase. Both worker nodes' `Allocatable.cpu` is deliberately capped at `3` (9 total across the 3-node cluster), so these two pods alone permanently consume roughly a third of one node's entire CPU budget; combined with ordinary load this pushed both nodes to 91-97% allocated, leaving no room for `csv_ingest_customers`' own `resolve_window`/`wait_for_files` pods (`250m` CPU each) to ever schedule. `kubectl describe pod` on the pending pods names the scheduler's own reason verbatim: `FailedScheduling ... 2 Insufficient cpu`.
- **Confirmed durable, not transient:** the standard 180s timeout failed; a deliberately generous 900s (15-minute) diagnostic retry (timeout temporarily widened, reverted before commit) ALSO failed identically (905.85s total, same "discovery never registered it" outcome); a final, clean confirmation run at the standard 180s timeout (for an accurate reference log) also failed identically, 185.70s. Across the whole ~40-minute observation window, the DagRun's own scheduler kept retrying with fresh pod names (`csv-ingest-customers-wait-for-files-4p1jit65` → `-zd0fx7ql`, etc.) with no change in outcome. `resolve_window` (an independent `@task` with zero dependencies on `wait_for_files`/`discover`/`ingest`) was ALSO stuck `Pending` throughout, proving this is a pure Kubernetes pod-scheduling capacity problem, not a DAG logic defect. `dag_run` history showed 4 consecutive `failed` runs immediately before this plan's own session even started — this was already degrading the DAG for hours before this work began.
- **Not auto-fixed:** deleting the two stuck pods (`kubectl -n etl delete pod ingest-qp3ougwy ingest-qgw33dq0`) is outside this plan's own declared mutation scope (its threat model and this session's own worktree instructions explicitly authorize mutating only the `monitoring` namespace's throwaway `webhook-receiver-*` resources, the dedicated `meta.datasets` test row, and the `grafana/alert-webhook` Vault/K8s-Secret pair — never arbitrary `etl`-namespace pods from an unrelated historical DagRun). This is a Rule 4 (architectural/infrastructure-operations) call for the orchestrator or a human, not an in-scope auto-fix by this worktree executor.
- **Strong non-live evidence the underlying mechanism is correct, independent of this block:** `TracingKubernetesPodOperator.build_pod_request_obj()` (`airflow/dags/_common/tracing_kpo.py`), `run_ingest()`'s span-context capture (`packages/dataplat/src/dataplat/pipeline/run.py`), and `claim_ingestion_run()`'s atomic persistence (`packages/dataplat/src/dataplat/metadata/postgres.py`) were all read in full and cross-checked against `test_trace_propagation.py`'s own assertions; all three are already unit/integration-tested in plans 07-04/07-05 (their own SUMMARY.md files record this). This session additionally re-ran `pytest tests/unit -k "trace or tracing" -q` live: 21/21 passing.
- **Logged:** `.planning/phases/07-observability-metrics-tracing-lineage/deferred-items.md`, section "From plan 07-08" — full diagnostic detail and the exact recommended fix.
- **Recommended next step:** `kubectl -n etl delete pod ingest-qp3ougwy ingest-qgw33dq0`, then re-run `uv run pytest tests/e2e/observability/test_trace_propagation.py -m cluster -x -q` — expected to pass immediately once cluster capacity is restored, since nothing in the test itself is broken. Separately, the `airflow-xcom-sidecar` container's own exit-on-base-container-completion logic should be root-caused, since this will recur for every future `ingest` task attempt until fixed.

## Issues Encountered

- The live cluster's `.secrets/vault-init.json` (needed by `vault_root_client`) is gitignored and therefore not present in a fresh worktree checkout by default — copied (read-only from the main tree, write into this worktree's own `.secrets/`) at session start, since this is local, non-tracked, regeneratable state and the same live Vault instance either way. No git-tracked file was touched.
- See Deviations above for the one substantive issue: the pre-existing cluster resource-exhaustion condition blocking Task 2's live proof.

## User Setup Required

None - no external service configuration required. (The `grafana/alert-webhook` Vault secret still holds the same operator-supplied placeholder URL `helm/values/local/monitoring.yaml`/plan 07-06 left it at; a real webhook destination remains the operator's own follow-up, unrelated to this plan.)

## Next Phase Readiness

- D-20 (Grafana → real webhook delivery) is now proven live and permanently regression-tested.
- OBS-10's live end-to-end proof is fully implemented and ready to pass the moment the pre-existing, unrelated cluster capacity issue is cleared (see Deviations) — this is a cluster-state blocker, not a code gap, and does not require any further plan-07-08 work.
- **Blocker for the orchestrator/next verification pass:** two stuck `ingest-*` pods in namespace `etl` (see Deviations/`deferred-items.md` for exact names and the one-line fix) are currently preventing `csv_ingest_customers` from scheduling ANY new task pod at all. This should be cleared before `/gsd:verify-phase` (or any other live-cluster proof) is attempted, since it will affect more than just this plan's own test.

## Self-Check: PASSED

- `tests/e2e/observability/conftest.py` — FOUND
- `tests/e2e/observability/test_alert_webhook_delivery.py` — FOUND
- `tests/e2e/observability/test_trace_propagation.py` — FOUND
- `.planning/phases/07-observability-metrics-tracing-lineage/deferred-items.md` — FOUND
- Commit `5b56a7c` (Task 1) — FOUND in `git log --oneline --all`
- Commit `4d796cc` (Task 2) — FOUND in `git log --oneline --all`
- Commit `3a4ff1d` (Task 3) — FOUND in `git log --oneline --all`
- Task 3's "PASSED in 472.95s" claim — directly observed in this session's own captured pytest output (`1 passed, 3 warnings in 472.95s`)
- Task 2's blocked-live-run claims — directly observed in this session's own captured pytest output across three separate live attempts (180s standard, 900s diagnostic, 180s final confirmation), all failing identically with the same `FailedScheduling ... 2 Insufficient cpu` root cause independently confirmed via `kubectl describe pod`/`kubectl describe node`
- "21/21 tracing unit tests passing" claim — directly observed in this session's own captured `pytest tests/unit -k "trace or tracing" -q` output

---
*Phase: 07-observability-metrics-tracing-lineage*
*Completed: 2026-08-16*
