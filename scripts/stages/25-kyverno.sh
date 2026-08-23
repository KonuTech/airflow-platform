#!/usr/bin/env bash
#
# D-14/D-17: the cluster-wide admission controller (kyverno chart), with the
# two readiness waits Helm cannot supply (mirrors
# scripts/stages/40-cnpg-operator.sh's own shape exactly):
#
#   1. wait_for_crd_established imagevalidatingpolicies.policies.kyverno.io
#   2. wait_for_deploy_available kyverno kyverno-admission-controller
#
# Both names confirmed by directly rendering the pinned chart this session
# (`helm template kyverno kyverno/kyverno --version 3.8.2 --namespace
# kyverno --set cleanupController.enabled=false`), not assumed.
#
# CRITICAL — stage numbering: this file MUST stay named `25-*`, running
# immediately after `20-namespaces.sh` and before `30-ingress-nginx.sh`. This
# is the only ordering under which every other component's pods (ingress-
# nginx, CNPG, MinIO, Airflow, Vault, monitoring) actually pass through
# Kyverno's admission webhook on a normal `make cluster-up`/`cluster-
# rebuild` — admission control never retroactively scans already-running
# pods.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# shellcheck source=/dev/null
source "${repo_root}/helm/versions.env"
# shellcheck source=/dev/null
source "${repo_root}/scripts/helm-install.sh"
# shellcheck source=/dev/null
source "${repo_root}/scripts/wait-for.sh"

helm_bin="${repo_root}/tools/bin/helm"

"${helm_bin}" repo add kyverno https://kyverno.github.io/kyverno/ >/dev/null 2>&1 || true
"${helm_bin}" repo update kyverno >/dev/null

helm_install kyverno kyverno/kyverno kyverno \
  KYVERNO_CHART_VERSION kyverno

wait_for_crd_established imagevalidatingpolicies.policies.kyverno.io
wait_for_deploy_available kyverno kyverno-admission-controller
