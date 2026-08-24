# Quick Task 260824-ayw: Build a trimmed CI-only monitoring stack (single-pod Prometheus/Grafana/Tempo profile) so tests/e2e/observability can run in CI - Context

**Gathered:** 2026-08-24
**Status:** Ready for planning

<domain>
## Task Boundary

Build a trimmed CI-only monitoring stack (single-pod Prometheus/Grafana/Tempo profile) so tests/e2e/observability can run in CI instead of being unconditionally skipped. This is Phase 11's CICD-09 follow-up-2 (part 2 of 2) — the user chose this over the cheaper alternative (marker-skipping the observability suite in CI as a disclosed narrowing of D-19).

</domain>

<decisions>
## Implementation Decisions

### Deploy timing
- **Stagger the monitoring stack: live only for the observability test window, not the whole 120-minute e2e-full.yml job.**
- Mechanism: split `e2e-full.yml`'s single `make cluster-verify` pytest invocation (currently `pytest tests/e2e/cluster tests/e2e/slice tests/e2e/observability -q`) into two steps — (1) `tests/e2e/cluster tests/e2e/slice`, then (2) install monitoring, run `tests/e2e/observability` alone, then tear monitoring down before the `make rebuild-from-raw` capstone step.
- Rationale (established this session, not to be re-litigated): the CI node has ~3 CPU allocatable total shared by Airflow, 2x Postgres, MinIO, Vault, and Kyverno for the whole job. The monitoring stack's own CPU footprint is modest (~550-600m requests even before further trimming below), but the CrashLoopBackOff that got monitoring disabled entirely happened with these same already-small values running concurrently with everything else for the full job duration. Narrowing monitoring's live window to just the observability sub-suite (which runs last in `cluster-verify`'s current pytest arg order, after the heaviest DAG-trigger load from `tests/e2e/slice`'s 2-year sweep has already finished) avoids that contention without needing draconian trimming.
- `make cluster-verify` (the target itself, used identically by local devs) must NOT change its own behavior — it still runs cluster+slice+observability together in one invocation for local's persistent 3-node profile, which has no CPU contention problem. The staggering is CI-workflow-specific orchestration (in `.github/workflows/e2e-full.yml` and/or a new CI-only Makefile target), not a change to the shared `cluster-verify` semantics.

### Further trimming
- **Drop `kubeStateMetrics` and `nodeExporter` from the CI monitoring values** (`helm/values/ci/monitoring.yaml`) — explicitly disable both via `kubeStateMetrics.enabled: false` / `nodeExporter.enabled: false` (chart defaults are `true` for both; neither is currently overridden in the CI values file).
- Verified during discussion: none of the 3 observability test files (`test_grafana_provisioning.py`, `test_alert_webhook_delivery.py`, `test_trace_propagation.py`) or `tests/e2e/observability/conftest.py` reference `kube-state-metrics` or `node-exporter` metrics/targets — only `analytics-postgres`, `prometheus` (fed by the OTel Collector's own `runs_started_total` etc.), and `tempo` datasources are exercised. Disabling both is a free CPU saving with zero test-coverage loss.
- A bigger (paid) GitHub runner class was raised as a possible lever but explicitly NOT chosen — stay within the existing free 4 CPU / 16 GB runner CLAUDE.md's CI-runner-sizing constraint already commits to. Do not propose a runner-class change as part of this plan.

### Claude's Discretion
- The exact CI-workflow mechanics of "install monitoring, run observability, tear down" (e.g., whether this becomes a new Makefile target like `observability-verify-ci`, inline shell steps in `e2e-full.yml`, or a wrapper script) — pick whatever fits this project's existing `scripts/stages/*.sh` + `Makefile` conventions most cleanly.
- Whether `85-monitoring.sh`'s unconditional `PROFILE=ci` skip needs to change shape (e.g., become a separate script/target invoked explicitly by `e2e-full.yml` at the right moment) vs. staying as-is with a new explicit installer step added elsewhere — a real architectural call within the agreed staggering strategy, left to the planner/executor's judgment given the existing `helm_install`/`wait_for_*` helper conventions in `scripts/helm-install.sh` / `scripts/wait-for.sh`.
- Whether the teardown before `make rebuild-from-raw` needs to be a full `helm uninstall` of all 3 charts or can be scaled-to-zero — pick whichever is simpler and reliably frees the CPU headroom `rebuild-from-raw`'s own full historical replay needs.
- Exact resource numbers for any further trimming beyond dropping kube-state-metrics/node-exporter (e.g., whether Prometheus/Grafana/Tempo/otel-collector's already-set CPU requests in `helm/values/ci/{monitoring,tempo,otel-collector}.yaml` need further reduction) — informed by research into actual live measurements from this session's prior CPU-starvation debugging (`.planning/phases/11-ci-cd-completion-operations/deferred-items.md`'s "PARTIALLY RESOLVED" and quick task `260817-rvq`'s own live-measured numbers), not guessed.

</decisions>

<specifics>
## Specific Ideas

No additional specific implementation ideas beyond the decisions above — open to standard approaches for the CI-workflow/Makefile mechanics.

</specifics>

<canonical_refs>
## Canonical References

- `.planning/phases/11-ci-cd-completion-operations/deferred-items.md` — "PARTIALLY RESOLVED (2026-08-24)" section (the 8 real CI-portability bugs found/fixed, including the monitoring-disabled fix and its live-measured CPU numbers) and "Plan 11-05" section ("New finding: tests/e2e/observability cannot pass on the CI profile..." — the original finding and its two previously-weighed remediation options).
- `scripts/stages/85-monitoring.sh` — current unconditional `PROFILE=ci` skip and its own header-comment history of why monitoring was disabled (the live CrashLoopBackOff diagnosis).
- `helm/values/ci/{monitoring,tempo,otel-collector}.yaml` — the already-trimmed CI values from quick task `260817-rvq` (starting point for any further trimming).
- `Makefile`'s `cluster-verify:` target (currently `pytest tests/e2e/cluster tests/e2e/slice tests/e2e/observability -q`) and `smoke-verify:` target (an example of an existing narrower, CI-specific target following this Makefile's own established idiom).
- `.github/workflows/e2e-full.yml` — the workflow this task's staggering changes will modify.
- Sibling quick tasks `260824-akz` (scheduler resource-kind live-detection) and `260824-ay9` (multi_node CI skip marker) — both already found and worked around the same underlying lesson (PROFILE does not propagate from `cluster-up` into later `make cluster-verify`/`make chaos-verify` steps in CI workflows); this task should build on that established finding rather than re-discovering it.

</canonical_refs>
