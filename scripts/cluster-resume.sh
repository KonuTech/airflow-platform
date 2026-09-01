#!/usr/bin/env bash
#
# Starts a cluster previously stopped by `scripts/cluster-pause.sh` back up:
# `docker start` on the registry + every kind node container (control-plane
# first), waits for every node to report Ready, then runs `doctor-live.sh`
# unconditionally — a `docker stop`/`start` cycle is the same class of event
# doctor-live.sh was written to detect (see its own header:
# .planning/debug/docker-desktop-wsl2-vm-restart.md), so resuming a paused
# cluster is exactly the moment to self-heal the DAGs tmpfs-fallback mount
# before anything schedules against it.
#
# Not a substitute for `cluster-up`: if the cluster containers do not exist
# at all (only `cluster-pause` stops them, `cluster-down` deletes them),
# this exits with guidance to run `make cluster-up` instead.

set -uo pipefail   # NOT -e: every container must be attempted even if one start fails.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# shellcheck source=/dev/null
source "${repo_root}/helm/versions.env"

DOCKER="${DOCKER:-docker}"
KUBECTL="${KUBECTL:-kubectl}"
KUBECTL_CONTEXT="kind-${CLUSTER_NAME}"

mapfile -t all_nodes < <(${DOCKER} ps -aq --filter "label=io.x-k8s.kind.cluster=${CLUSTER_NAME}")
mapfile -t control_plane < <(${DOCKER} ps -aq --filter "label=io.x-k8s.kind.cluster=${CLUSTER_NAME}" --filter "label=io.x-k8s.kind.role=control-plane")
mapfile -t registry < <(${DOCKER} ps -aq --filter "name=^kind-registry\$")

if [ "${#all_nodes[@]}" -eq 0 ]; then
  echo "No '${CLUSTER_NAME}' cluster containers found (paused or otherwise) — nothing to resume. Run 'make cluster-up' to create one." >&2
  exit 1
fi

start_container() {
  local id="$1"
  local name
  name="$(${DOCKER} inspect "${id}" --format '{{.Name}}' 2>/dev/null | sed 's#^/##')"
  local state
  state="$(${DOCKER} inspect "${id}" --format '{{.State.Running}}' 2>/dev/null)"
  if [ "${state}" = "true" ]; then
    echo "  ${name:-$id} already running — skipped"
    return 0
  fi
  echo "  starting ${name:-$id} ..."
  ${DOCKER} start "${id}" >/dev/null
}

echo "==> resuming cluster '${CLUSTER_NAME}'"

for id in "${registry[@]}"; do
  start_container "${id}"
done

# Control plane first so the API server has a head start on the workers'
# kubelets reconnecting.
for id in "${control_plane[@]}"; do
  start_container "${id}"
done
for id in "${all_nodes[@]}"; do
  is_cp=false
  for cp_id in "${control_plane[@]}"; do
    [ "${id}" = "${cp_id}" ] && is_cp=true && break
  done
  [ "${is_cp}" = true ] && continue
  start_container "${id}"
done

echo "==> waiting for all nodes to report Ready (context ${KUBECTL_CONTEXT})"
# `kubectl wait` does not retry a hard API error (verified live: freshly
# restarted node containers serve `Forbidden: unknown` from the apiserver
# for the first few seconds while its own caches/RBAC warm up, and a single
# `wait` invocation exits immediately on that instead of polling through
# it) — so poll by re-invoking `wait` with a short per-attempt timeout
# across the overall budget, rather than trusting one long-timeout call.
deadline=$(( $(date +%s) + ${CLUSTER_RESUME_NODE_TIMEOUT:-120} ))
nodes_ready=false
while [ "$(date +%s)" -lt "${deadline}" ]; do
  if ${KUBECTL} --context "${KUBECTL_CONTEXT}" wait --for=condition=Ready nodes --all \
      --timeout=5s >/dev/null 2>&1; then
    nodes_ready=true
    break
  fi
  sleep 2
done

if [ "${nodes_ready}" != true ]; then
  echo "cluster-resume: nodes did not report Ready in time — inspect with 'kubectl --context ${KUBECTL_CONTEXT} get nodes'." >&2
  exit 1
fi

echo "==> self-healing the DAGs mount (a stop/start cycle is the exact trigger doctor-live.sh guards against)"
if ! DOCKER="${DOCKER}" "${repo_root}/scripts/doctor-live.sh"; then
  echo "cluster-resume: nodes are Ready but the DAGs mount repair did not fully clear — see doctor-live output above." >&2
  exit 1
fi

echo ""
echo "cluster-resume: cluster '${CLUSTER_NAME}' is back up."
