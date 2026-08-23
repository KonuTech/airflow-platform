#!/usr/bin/env bash
#
# scripts/ci-set-workload-images.sh <tag>
#
# Plan 11-04 (D-19/D-20): points the csv_processor_image and dbt_image
# Airflow Variables at ghcr.io/<owner>/{csv-processor,dbt}:<tag> — the SAME
# `kubectl ... exec ... airflow variables set` shape `make image-csv-
# processor`/`image-dbt` already use for a local-registry image, applied
# here to a GHCR reference instead. No local-registry re-push happens: GHCR
# packages are public, so pods pull them directly.
#
# Owner resolution reuses the EXACT `git remote get-url origin` parsing
# already established in the Makefile's `rollback` target (plan 11-06) —
# not a second, differently-worded variant.
#
# Usage:
#   scripts/ci-set-workload-images.sh <tag>
#   e.g. scripts/ci-set-workload-images.sh pr-42

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

tag="${1:-}"
if [ -z "${tag}" ]; then
  echo "ERROR: scripts/ci-set-workload-images.sh: a <tag> argument is required, e.g." >&2
  echo "  scripts/ci-set-workload-images.sh pr-42" >&2
  exit 1
fi

# shellcheck source=/dev/null
set -a
. "${repo_root}/helm/versions.env"
set +a

kubectl_bin="${KUBECTL:-kubectl}"
ctx="${KUBECTL_CONTEXT:-kind-${CLUSTER_NAME}}"

owner="$(git remote get-url origin | sed -E 's#.*[:/]([^/]+)/[^/]+(\.git)?$#\1#' | tr '[:upper:]' '[:lower:]')"
if [ -z "${owner}" ]; then
  echo "ERROR: could not resolve the GitHub owner from 'git remote get-url origin'" >&2
  exit 1
fi

echo "==> resolved owner=${owner}, tag=${tag}"

echo "==> registering csv_processor_image=ghcr.io/${owner}/csv-processor:${tag}"
"${kubectl_bin}" --context "${ctx}" exec -n airflow deploy/airflow-api-server -- \
  airflow variables set csv_processor_image "ghcr.io/${owner}/csv-processor:${tag}"

echo "==> registering dbt_image=ghcr.io/${owner}/dbt:${tag}"
"${kubectl_bin}" --context "${ctx}" exec -n airflow deploy/airflow-api-server -- \
  airflow variables set dbt_image "ghcr.io/${owner}/dbt:${tag}"

echo "==> ci-set-workload-images complete: csv_processor_image/dbt_image now point at tag=${tag}"
