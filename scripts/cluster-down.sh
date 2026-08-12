#!/usr/bin/env bash
#
# Deletes the kind cluster if it exists; otherwise exits 0 and reports that
# there was nothing to delete (safe against a host with no
# `airflow-platform` cluster — probe: INFRA-07 empty).
#
# Deliberately leaves the local registry container (`kind-registry`) running:
# it is an ordinary Docker container, survives `kind delete cluster` on its
# own, and is not covered by D-01's cluster disposability. Only
# `make clean-images` (not this script) touches it.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# shellcheck source=/dev/null
source "${repo_root}/helm/versions.env"

kind_bin="${repo_root}/tools/bin/kind"

if [ ! -x "${kind_bin}" ]; then
  echo "kind is not installed at ${kind_bin} — nothing to delete."
  exit 0
fi

if "${kind_bin}" get clusters | grep -qx "${CLUSTER_NAME}"; then
  echo "==> deleting cluster '${CLUSTER_NAME}'"
  "${kind_bin}" delete cluster --name "${CLUSTER_NAME}"
else
  echo "No cluster named '${CLUSTER_NAME}' — nothing to delete."
fi
