#!/usr/bin/env bash
#
# Quick task 260824-ayw: the extracted monitoring install logic --
# previously inlined in scripts/stages/85-monitoring.sh, now the single
# shared source both that stage script's local-cluster-up path AND the new
# CI staggered path (Makefile's observability-verify-ci) call. Having one
# place the helm_install/wait_for_* call shape can be edited means the two
# paths cannot silently drift into two different install shapes.
#
# See scripts/stages/85-monitoring.sh's own header comment for the full
# argument behind each wait strategy/ordering choice below (--wait=watcher
# default for otel-collector/tempo, hookOnly for kube-prometheus-stack, the
# Grafana first-ever-cluster-up caveat) -- that reasoning is unchanged by
# this extraction and is not repeated here.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

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
# (depends_on 07-03). `hookOnly` -- see scripts/stages/85-monitoring.sh's
# own header comment.
helm_install monitoring prometheus-community/kube-prometheus-stack monitoring \
  KUBE_PROMETHEUS_STACK_CHART_VERSION monitoring hookOnly

wait_for_deploy_available monitoring monitoring-kube-prometheus-operator

echo "==> Monitoring stage installed and running: otel-collector, tempo, kube-prometheus-stack (prometheus-operator ready)"
echo "      NOTE: on a first-ever cluster-up, Grafana's own pod stays in"
echo "      CreateContainerConfigError until \`make vault-bootstrap\` creates"
echo "      the grafana-alert-webhook Secret -- this is expected, not a bug."
