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

# D-06/PROFILE: `local` selects kind/cluster.yaml (3-node, full sizing,
# untouched by the CI-portability fix below); `ci` selects kind/cluster-ci.yaml
# (single-node, trimmed — see that file's own header for the fair-share
# reservation math). kind/cluster.yaml is NEVER edited or substituted into —
# it stays exactly as local dev has always run it.
if [ "${PROFILE}" = "ci" ]; then
  cluster_config="${repo_root}/kind/cluster-ci.yaml"
else
  cluster_config="${repo_root}/kind/cluster.yaml"
fi

if "${kind_bin}" get clusters | grep -qx "${CLUSTER_NAME}"; then
  echo "==> cluster '${CLUSTER_NAME}' already exists — skipping kind create cluster"
else
  if [ "${PROFILE}" = "ci" ]; then
    # kind/cluster-ci.yaml's own DAG hostPath mount carries a literal
    # __CI_REPO_ROOT__ placeholder (never a real path — see that file's own
    # comment) because a GitHub Actions checkout path varies per run and
    # cannot be baked into a static, committed YAML file. Render a throwaway
    # substituted copy here, at invocation time, and pass THAT to `kind
    # create cluster --config` — kind/cluster-ci.yaml itself is never
    # modified on disk.
    rendered_config="$(mktemp "${TMPDIR:-/tmp}/cluster-ci.XXXXXX.yaml")"
    trap 'rm -f "${rendered_config}"' EXIT
    sed "s#__CI_REPO_ROOT__#${repo_root}#g" "${cluster_config}" > "${rendered_config}"
    cluster_config="${rendered_config}"
  fi
  echo "==> creating cluster '${CLUSTER_NAME}' from ${cluster_config} (profile=${PROFILE})"
  "${kind_bin}" create cluster --name "${CLUSTER_NAME}" --config "${cluster_config}"
fi

while IFS= read -r stage; do
  echo "==> running stage: $(basename "${stage}")"
  "${stage}"
done < <(LC_ALL=C find "${repo_root}/scripts/stages" -maxdepth 1 -type f -name '*.sh' | LC_ALL=C sort)

echo "==> cluster-up complete (profile=${PROFILE}, context=${KUBECTL_CONTEXT})"
