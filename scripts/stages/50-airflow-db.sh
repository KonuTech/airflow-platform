#!/usr/bin/env bash
#
# The Airflow metadata Cluster CR — PostgreSQL 17, namespace `data` (D-13
# correction, see helm/values/local/cnpg-airflow.yaml), worker-1 (D-03).
#
# A Cluster CR is invisible to Helm's `--wait` strategy (it is not a
# Deployment/StatefulSet/Job Helm knows how to watch) but exposes a usable
# `Ready` condition once the operator finishes reconciling it — verified
# returning "condition met" in 02-RESEARCH.md Pattern 3. Runs after
# 40-cnpg-operator.sh in the LC_ALL=C stage order, so the operator's
# admission webhook is already serving by the time this Cluster CR is
# applied.

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

helm_install airflow-db cnpg/cluster data \
  CNPG_CLUSTER_CHART_VERSION cnpg-airflow

wait_for_cnpg_cluster_ready data airflow-db
