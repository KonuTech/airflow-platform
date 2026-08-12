#!/usr/bin/env bash
#
# The analytical Cluster CR — PostgreSQL 18, namespace `data`, worker-2
# alone (D-03: kept off the node serving object storage). The `55-` prefix
# keeps this after the metadata cluster (`50-`) in the stage runner's
# lexical order while leaving numbering room between stages.
#
# See scripts/stages/50-airflow-db.sh for why a Cluster CR needs its own
# `kubectl wait` rather than relying on Helm's `--wait` strategy.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# shellcheck source=/dev/null
source "${repo_root}/helm/versions.env"
# shellcheck source=/dev/null
source "${repo_root}/scripts/helm-install.sh"
# shellcheck source=/dev/null
source "${repo_root}/scripts/wait-for.sh"

helm_bin="${repo_root}/tools/bin/helm"

"${helm_bin}" repo add cnpg https://cloudnative-pg.github.io/charts >/dev/null 2>&1 || true
"${helm_bin}" repo update cnpg >/dev/null

helm_install analytics-db cnpg/cluster data \
  CNPG_CLUSTER_CHART_VERSION cnpg-analytics

wait_for_cnpg_cluster_ready data analytics-db
