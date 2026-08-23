#!/usr/bin/env bash
#
# The Observability tier's ingress stage (plan 07-03). Numbered after
# 80-vault.sh: nothing in this stage depends on Vault's own contents, but 85
# keeps it last among the currently-numbered component stages, since this is
# the newest addition to this stage runner.
#
# Unlike 80-vault.sh, neither the OTel Collector nor Tempo chart carries a
# Vault-style sealed/migration-gated readiness deadlock (verified this
# session via `helm template`: neither chart renders any Helm hook resource
# at all, so Helm 4's default `--wait=watcher` strategy -- which blocks
# `helm upgrade --install` until the chart's own non-hook Deployment/
# StatefulSet is Ready -- cannot deadlock the way 70-airflow.sh's/
# 80-vault.sh's own hookOnly override exists to route around). `helm_install`
# is therefore called with no 6th argument for both charts, taking its
# default `watcher` strategy.
#
# The follow-up wait below is `wait_for_deploy_available`/
# `wait_for_statefulset_ready`, not `wait_for_pod_running`:
# `wait_for_pod_running` (scripts/wait-for.sh) takes an exact `pod/<name>`,
# which is deterministic for Vault's StatefulSet (always `vault-0`) but not
# for the OTel Collector's Deployment (a ReplicaSet-hash + random suffix,
# unknown ahead of time). Both `wait_for_deploy_available` and
# `wait_for_statefulset_ready` need only the owning resource's own name, and
# -- since neither chart has Vault's sealed-state reason to settle for a
# lesser "Running, not Ready" bar -- asserting the stricter Ready condition
# is both the more meaningful signal and, by the time `helm_install` (default
# `watcher`) has already returned, effectively a fast confirmation of a
# condition Helm itself already waited for.
#
# kube-prometheus-stack (plan 07-07) is DIFFERENT from both charts above and
# needs `hookOnly`, for the exact same reason 70-airflow.sh/80-vault.sh
# already do: `helm template` this session confirms this chart DOES render
# Helm hook resources (the `admission-create`/`admission-patch` Jobs,
# `pre-install`/`post-install`), and `watcher` waits for the chart's own
# non-hook Deployments (including `monitoring-grafana`) to become Ready
# BEFORE running those hooks. On a FIRST-EVER `make cluster-up` (before
# `make vault-bootstrap` has ever run), Grafana's own pod will fail to start
# with a missing-Secret error (`grafana-alert-webhook` does not exist yet,
# per plan 07-06's `_ensure_grafana_secrets`) -- `watcher` would therefore
# deadlock exactly like the Airflow-migration-Job case `helm-install.sh`'s
# own header comment documents. This is an accepted, documented
# bootstrapping-order requirement identical in shape to Vault's own
# cluster-up-then-unseal-then-bootstrap sequence, not a bug: run
# `make vault-bootstrap` once, then `helm upgrade` this release again (or
# just wait for Kubernetes' own Deployment controller to retry the pod once
# the Secret exists -- no re-install needed). The follow-up wait below
# therefore only targets `monitoring-kube-prometheus-operator` (a Deployment
# with no Secret dependency, reconciled immediately) as a real "did the
# release apply cleanly" signal -- it deliberately does NOT wait on Grafana
# or the operator-managed Prometheus StatefulSet (created asynchronously by
# the Operator once it is itself ready, not by this `helm upgrade` call at
# all).

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Post-merge fix (CICD-09 follow-up): CLAUDE.md's own CI-runner-sizing
# constraint states this explicitly -- "a trimmed single-node CI profile
# (monitoring disabled, minimal replicas) for ephemeral-kind E2E" -- and
# tests/policy/test_values_profiles.py's own "monitoring enablement" axis
# docstring already says "kube-prometheus-stack itself is Phase 7 and is
# not deployed by this phase's CI job". This stage never actually honoured
# either: it ran unconditionally regardless of PROFILE, deploying the full
# kube-prometheus-stack + otel-collector + tempo stack onto the single CI
# node too. Live-diagnosed (this session, CICD-09's own throwaway-PR proof):
# with monitoring installed, the CI node's own Allocated-resources showed
# cpu requests at 2810m/3000m (93%) BEFORE any burst, and airflow-scheduler-0
# itself was CrashLoopBackOff with its own startup probe (`airflow jobs
# check --job-type SchedulerJob --local`) timing out at 20s under that
# contention -- the scheduler never stayed alive long enough to dispatch a
# single queued task, so smoke-verify's [2/4] DagRun sat in `queued`
# forever. Skipping this stage entirely on PROFILE=ci is not a new
# architectural decision -- it is finally implementing what this project's
# own design already specified. helm/values/ci/{otel-collector,tempo,
# monitoring}.yaml stay committed and are still rendered/linted by `make
# manifests`/`helm-lint` (an offline concern, unaffected by skipping the
# LIVE install here) -- only this stage's live `helm upgrade --install`
# calls are skipped.
if [ "${PROFILE:-local}" = "ci" ]; then
  echo "==> skipping monitoring stage (PROFILE=ci -- CLAUDE.md's own CI profile disables monitoring)"
  exit 0
fi

# shellcheck source=/dev/null
source "${repo_root}/helm/versions.env"
# shellcheck source=/dev/null
source "${repo_root}/scripts/helm-install.sh"
# shellcheck source=/dev/null
source "${repo_root}/scripts/wait-for.sh"

helm_bin="${repo_root}/tools/bin/helm"

"${helm_bin}" repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts >/dev/null 2>&1 || true
"${helm_bin}" repo add grafana-community https://grafana-community.github.io/helm-charts >/dev/null 2>&1 || true
"${helm_bin}" repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null 2>&1 || true
"${helm_bin}" repo update >/dev/null

helm_install otel-collector open-telemetry/opentelemetry-collector monitoring OTEL_COLLECTOR_CHART_VERSION otel-collector
helm_install tempo grafana-community/tempo monitoring TEMPO_CHART_VERSION tempo

wait_for_deploy_available monitoring otel-collector-opentelemetry-collector
wait_for_statefulset_ready monitoring tempo

# plan 07-07: AFTER the OTel Collector/Tempo installs above -- Task 1's own
# additionalServiceMonitors entry targets the OTel Collector's already-live
# Service, and this ordering matches this plan's own wave dependency
# (depends_on 07-03). `hookOnly` -- see this file's own header comment.
helm_install monitoring prometheus-community/kube-prometheus-stack monitoring \
  KUBE_PROMETHEUS_STACK_CHART_VERSION monitoring hookOnly

wait_for_deploy_available monitoring monitoring-kube-prometheus-operator

echo "==> Monitoring stage installed and running: otel-collector, tempo, kube-prometheus-stack (prometheus-operator ready)"
echo "      NOTE: on a first-ever cluster-up, Grafana's own pod stays in"
echo "      CreateContainerConfigError until \`make vault-bootstrap\` creates"
echo "      the grafana-alert-webhook Secret -- this is expected, not a bug."
