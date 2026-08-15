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

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# shellcheck source=/dev/null
source "${repo_root}/helm/versions.env"
# shellcheck source=/dev/null
source "${repo_root}/scripts/helm-install.sh"
# shellcheck source=/dev/null
source "${repo_root}/scripts/wait-for.sh"

helm_bin="${repo_root}/tools/bin/helm"

"${helm_bin}" repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts >/dev/null 2>&1 || true
"${helm_bin}" repo add grafana-community https://grafana-community.github.io/helm-charts >/dev/null 2>&1 || true
"${helm_bin}" repo update >/dev/null

helm_install otel-collector open-telemetry/opentelemetry-collector monitoring OTEL_COLLECTOR_CHART_VERSION otel-collector
helm_install tempo grafana-community/tempo monitoring TEMPO_CHART_VERSION tempo

wait_for_deploy_available monitoring otel-collector-opentelemetry-collector
wait_for_statefulset_ready monitoring tempo

echo "==> Monitoring stage installed and running: otel-collector, tempo"
