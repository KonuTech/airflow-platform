#!/usr/bin/env bash
#
# The first component on the cluster-up path (D-05). Every later component
# this phase adds (both PostgreSQL clusters, MinIO, Airflow) is an additional
# scripts/stages/*.sh file — no edit to scripts/cluster-up.sh.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# shellcheck source=/dev/null
source "${repo_root}/helm/versions.env"
# shellcheck source=/dev/null
source "${repo_root}/scripts/helm-install.sh"
# shellcheck source=/dev/null
source "${repo_root}/scripts/wait-for.sh"

helm_bin="${repo_root}/tools/bin/helm"

"${helm_bin}" repo add ingress-nginx https://kubernetes.github.io/ingress-nginx >/dev/null 2>&1 || true
"${helm_bin}" repo update ingress-nginx >/dev/null

helm_install ingress-nginx ingress-nginx/ingress-nginx ingress-nginx \
  INGRESS_NGINX_CHART_VERSION ingress-nginx

wait_for_deploy_available ingress-nginx ingress-nginx-controller
