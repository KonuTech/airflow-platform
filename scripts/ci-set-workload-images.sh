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

# debug/ci-pipeline-ingestion-timeout ROUND 10 (root cause 14): the CI
# profile's stage/publish pods must request 200m CPU, not the 500m default
# both DAGs' stage_pod_resources() falls back to on local -- 500m can never
# be scheduled on CI's single ~3-CPU node (~220m free at steady state
# pre-fix), which made every stage attempt a deterministic ~129s
# startup-timeout failure. This script is the CI-profile Airflow-Variable
# bootstrap site (the csv_processor_image precedent above), invoked by all
# three e2e workflows post-cluster-up, so the per-profile request lives here
# rather than in a fourth mechanism. Local clusters never run this script
# and keep the 500m default verbatim.
echo "==> registering stage_cpu_request=200m (CI profile; local default is 500m)"
"${kubectl_bin}" --context "${ctx}" exec -n airflow deploy/airflow-api-server -- \
  airflow variables set stage_cpu_request "200m"

# debug/ci-pipeline-ingestion-timeout ROUND 14 (finding 18a, trim iii): CI
# trims customers' publish retries 6 -> 3. Post-ROUND-14, deterministic
# quality-gate trips (mass-delete breaker) quarantine + exit 0 and never
# consume a retry, so publish retries serve ONLY the transient class
# (KubernetesJobWatcher read-timeout race, Kyverno hiccups, co-scheduling
# CPU bursts). With retry_delay=30s exponential backoff, retries=3 spans 4
# attempts over ~12min -- covering ROUND 13's measured ~5min self-healed
# FailedScheduling burst with margin. Local never runs this script and
# keeps the DAG's own default of 6 (airflow/dags/_common/kpo.py).
echo "==> registering publish_retries=3 (CI profile; local default is 6)"
"${kubectl_bin}" --context "${ctx}" exec -n airflow deploy/airflow-api-server -- \
  airflow variables set publish_retries "3"

echo "==> ci-set-workload-images complete: csv_processor_image/dbt_image now point at tag=${tag}; stage_cpu_request=200m; publish_retries=3"
