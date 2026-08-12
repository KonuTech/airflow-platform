#!/usr/bin/env bash
#
# The CloudNativePG operator (D-09), with the two readiness waits Helm cannot
# supply (02-RESEARCH.md Pattern 3, measured this session):
#
#   1. wait_for_crd_established clusters.postgresql.cnpg.io
#   2. wait_for_deploy_available cnpg-system cnpg-cloudnative-pg
#
# The CRD wait alone is NOT enough: applying a Cluster CR immediately after
# the CRD reports "established" still failed with
#   `failed calling webhook "mcluster.cnpg.io": ... connect: connection refused`
# The admission webhook, served by this Deployment, is the real gate — do not
# remove the second wait as "redundant" with the first.
#
# `helm_install`'s `--wait=watcher` (never bare `--wait`, whose default when
# omitted is `hookOnly` in Helm 4.2.3) already waits for hook completion, but
# neither wait strategy inspects a CRD's Established condition or this
# specific Deployment's Available condition — hence both explicit waits below.

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

helm_install cnpg cnpg/cloudnative-pg cnpg-system \
  CNPG_OPERATOR_CHART_VERSION cnpg-operator

wait_for_crd_established clusters.postgresql.cnpg.io
wait_for_deploy_available cnpg-system cnpg-cloudnative-pg
