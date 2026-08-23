#!/usr/bin/env bash
#
# The Airflow component stage — the last of the phase (D-09, D-13). The
# metadata-connection adapter MUST run before the chart installs, because the
# chart references all three of its Secrets (airflow-metadata,
# airflow-fernet-key, airflow-api-secret-key) by name. `Cluster/airflow-db`
# in namespace `data` MUST also already report Ready before either — the
# adapter reads its generated `-app` Secret, and the chart's migration job
# connects immediately on install; a not-yet-elected primary produces a
# confusing failure in both places. Runs after 50-airflow-db.sh in the
# LC_ALL=C stage order, so both preconditions already hold by the time this
# script starts.
#
# A cold pull of the multi-gigabyte apache/airflow image is the slowest
# single step in the whole bootstrap (02-RESEARCH.md: 48s with a warm image
# cache) — HELM_INSTALL_TIMEOUT is raised well past scripts/helm-install.sh's
# 5m default for this stage alone.
#
# WAIT STRATEGY (verified live, this plan): `helm_install`'s default
# `--wait=watcher` DEADLOCKS on this chart. `watcher` waits for the chart's
# own Deployments/StatefulSet to become Ready before Helm ever runs its
# `post-install` hooks — but every one of those workloads carries a
# `wait-for-airflow-migrations` initContainer that blocks on
# `airflow-run-airflow-migrations`, itself a `post-install` hook. `watcher`
# therefore can never reach the hook that would unblock the very thing it is
# waiting for, and times out with every workload `Progress deadline
# exceeded`. `hookOnly` (the 6th `helm_install` argument below) runs the
# migration and create-user hooks without first waiting on the chart's own
# resources, which lets them complete and unblocks the initContainers — this
# script then proves the four workloads itself via scripts/wait-for.sh,
# unaffected by which WaitStrategy Helm used internally.
#
# IMAGE OVERRIDE (plan 11-04, D-19/D-20): when BOTH AIRFLOW_IMAGE_OVERRIDE_REPO
# and AIRFLOW_IMAGE_OVERRIDE_TAG are set, this install is pointed at that
# GHCR repo/tag via two extra `--set` flags forwarded through
# scripts/helm-install.sh's own generic extra-args passthrough — this is
# what lets CI's ephemeral cluster-up install Airflow ONCE, correctly,
# pointed at the PR's own just-published image, rather than installing at
# the chart's default image and then re-upgrading afterward. A normal
# `make cluster-up` (no override vars set) leaves neither variable set, so
# this falls through to the exact same call as before this plan.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# shellcheck source=/dev/null
source "${repo_root}/helm/versions.env"
# shellcheck source=/dev/null
source "${repo_root}/scripts/helm-install.sh"
# shellcheck source=/dev/null
source "${repo_root}/scripts/wait-for.sh"

helm_bin="${repo_root}/tools/bin/helm"

echo "==> waiting for Cluster/airflow-db (namespace data) to report Ready"
wait_for_cnpg_cluster_ready data airflow-db

echo "==> deriving the Airflow metadata connection and generating the Fernet/API secret keys"
"${repo_root}/scripts/airflow-metadata-secret.sh" ensure

"${helm_bin}" repo add apache-airflow https://airflow.apache.org >/dev/null 2>&1 || true
"${helm_bin}" repo update apache-airflow >/dev/null

if [ -n "${AIRFLOW_IMAGE_OVERRIDE_REPO:-}" ] && [ -n "${AIRFLOW_IMAGE_OVERRIDE_TAG:-}" ]; then
  echo "==> AIRFLOW_IMAGE_OVERRIDE_REPO/TAG set — installing Airflow at ${AIRFLOW_IMAGE_OVERRIDE_REPO}:${AIRFLOW_IMAGE_OVERRIDE_TAG}"
  HELM_INSTALL_TIMEOUT="${HELM_INSTALL_TIMEOUT:-15m}" \
    helm_install airflow apache-airflow/airflow airflow AIRFLOW_CHART_VERSION airflow hookOnly \
    --set "defaultAirflowRepository=${AIRFLOW_IMAGE_OVERRIDE_REPO}" \
    --set "defaultAirflowTag=${AIRFLOW_IMAGE_OVERRIDE_TAG}"
else
  HELM_INSTALL_TIMEOUT="${HELM_INSTALL_TIMEOUT:-15m}" \
    helm_install airflow apache-airflow/airflow airflow AIRFLOW_CHART_VERSION airflow hookOnly
fi

echo "==> waiting for the three Airflow Deployments and the triggerer StatefulSet"
wait_for_deploy_available airflow airflow-api-server
wait_for_deploy_available airflow airflow-scheduler
wait_for_deploy_available airflow airflow-dag-processor
wait_for_statefulset_ready airflow airflow-triggerer
