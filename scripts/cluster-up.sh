#!/usr/bin/env bash
#
# The ONLY bootstrap entry point (D-09). `make cluster-up` calls this and
# nothing else in this repository runs `kind create cluster` — INFRA-07's "no
# manual kubectl surgery" extends to "no manual kind surgery" the same way.
#
# Idempotent by construction: a re-run against an already-live cluster skips
# `kind create cluster` (so an interrupted bootstrap is resumed by re-running
# this script, never by destroying the cluster first) and every
# scripts/stages/*.sh script is itself required to be re-runnable
# (`helm upgrade --install` is idempotent; kubectl apply is idempotent).
#
# Stage ordering is `LC_ALL=C` ascending lexical order over
# scripts/stages/*.sh, so it never depends on locale or filesystem
# enumeration order — the numeric filename prefixes ARE the order and are
# readable in one `ls`.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# shellcheck source=/dev/null
source "${repo_root}/helm/versions.env"

export PROFILE="${PROFILE:-local}"
export CLUSTER_NAME
export KUBECTL_CONTEXT="kind-${CLUSTER_NAME}"

kind_bin="${repo_root}/tools/bin/kind"

echo "==> ensuring pinned kind and helm binaries"
"${repo_root}/tools/k8s/install_kind.sh"
"${repo_root}/tools/k8s/install_helm.sh"

if "${kind_bin}" get clusters | grep -qx "${CLUSTER_NAME}"; then
  echo "==> cluster '${CLUSTER_NAME}' already exists — skipping kind create cluster"
else
  echo "==> creating cluster '${CLUSTER_NAME}' from kind/cluster.yaml"
  "${kind_bin}" create cluster --name "${CLUSTER_NAME}" --config "${repo_root}/kind/cluster.yaml"
fi

while IFS= read -r stage; do
  echo "==> running stage: $(basename "${stage}")"
  "${stage}"
done < <(LC_ALL=C find "${repo_root}/scripts/stages" -maxdepth 1 -type f -name '*.sh' | LC_ALL=C sort)

echo "==> cluster-up complete (profile=${PROFILE}, context=${KUBECTL_CONTEXT})"
