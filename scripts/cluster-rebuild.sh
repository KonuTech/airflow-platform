#!/usr/bin/env bash
#
# D-04: destroy and recreate the whole environment from committed files,
# with an ATTRIBUTABLE per-stage timing breakdown. Deliberately not
# `cluster-down.sh && cluster-up.sh` timed as one blob — a regression is only
# attributable if you know WHICH stage got slower, so this script times
# `kind create cluster` and every scripts/stages/*.sh individually, in the
# same LC_ALL=C lexical order scripts/cluster-up.sh uses.
#
# The total is written to a gitignored file under the repository and
# compared against a documented ~15 minute budget (REBUILD_BUDGET_SECONDS).
# Deliberately WARNS, never fails, past it — unlike every other gate in this
# repository. Wall-clock on a cold image cache measures the network, not the
# repository, and a flaky gate is one people learn to ignore (D-04). Do not
# "fix" this into a hard failure — that is the one deliberate advisory this
# phase grants, and hardening it silently defeats the point.

set -uo pipefail   # NOT -e: every stage must still run so the breakdown is complete.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# shellcheck source=/dev/null
source "${repo_root}/helm/versions.env"

export PROFILE="${PROFILE:-local}"
export CLUSTER_NAME
export KUBECTL_CONTEXT="kind-${CLUSTER_NAME}"

kind_bin="${repo_root}/tools/bin/kind"

# Gitignored (build/) — the last run's breakdown, not a build artifact
# anyone commits. Overwritten on every run; git history is not the record of
# past timings, this file is.
timing_dir="${repo_root}/build"
timing_file="${timing_dir}/cluster-rebuild-timing.txt"
mkdir -p "${timing_dir}"

REBUILD_BUDGET_SECONDS="${REBUILD_BUDGET_SECONDS:-900}"  # ~15 minutes (D-04)

stage_names=()
stage_seconds=()

record_stage() {
  stage_names+=("$1")
  stage_seconds+=("$2")
}

run_timed() {
  local label="$1"
  shift
  local start end elapsed
  start=$(date +%s)
  if ! "$@"; then
    echo "ERROR: stage '${label}' failed — aborting cluster-rebuild" >&2
    exit 1
  fi
  end=$(date +%s)
  elapsed=$((end - start))
  record_stage "${label}" "${elapsed}"
  echo "==> ${label}: ${elapsed}s"
}

echo "==> make cluster-rebuild: destroying the current cluster (if any)"
"${repo_root}/scripts/cluster-down.sh"

echo "==> ensuring pinned kind and helm binaries"
"${repo_root}/tools/k8s/install_kind.sh"
"${repo_root}/tools/k8s/install_helm.sh"

run_timed "kind create cluster" \
  "${kind_bin}" create cluster --name "${CLUSTER_NAME}" --config "${repo_root}/kind/cluster.yaml"

while IFS= read -r stage; do
  run_timed "$(basename "${stage}")" "${stage}"
done < <(LC_ALL=C find "${repo_root}/scripts/stages" -maxdepth 1 -type f -name '*.sh' | LC_ALL=C sort)

total=0
for elapsed in "${stage_seconds[@]}"; do
  total=$((total + elapsed))
done

{
  echo "cluster-rebuild timing — $(date -u +%Y-%m-%dT%H:%M:%SZ) (profile=${PROFILE})"
  for i in "${!stage_names[@]}"; do
    printf '%-30s %6ss\n' "${stage_names[$i]}" "${stage_seconds[$i]}"
  done
  printf '%-30s %6ss\n' "TOTAL" "${total}"
} | tee "${timing_file}"

echo ""
if [ "${total}" -gt "${REBUILD_BUDGET_SECONDS}" ]; then
  # WARN, never fail — see the header comment.
  echo "WARNING: cluster-rebuild took ${total}s, over the documented ~${REBUILD_BUDGET_SECONDS}s budget (REBUILD_BUDGET_SECONDS)." >&2
else
  echo "cluster-rebuild: ${total}s, within the ~${REBUILD_BUDGET_SECONDS}s budget."
fi

echo "cluster-rebuild complete (profile=${PROFILE}, context=${KUBECTL_CONTEXT})"
