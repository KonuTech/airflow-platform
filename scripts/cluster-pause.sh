#!/usr/bin/env bash
#
# Stops the kind node containers and the local registry container WITHOUT
# deleting them — frees host CPU/RAM for other work while preserving every
# container's filesystem (etcd data, PVs, images), unlike `cluster-down`
# (which deletes the cluster outright and makes `cluster-up` re-provision
# everything from scratch). Pairs with `scripts/cluster-resume.sh`.
#
# Idempotent: containers that are already stopped are silently skipped, and
# a host with no `airflow-platform` cluster is a no-op, same as
# cluster-down.sh.

set -uo pipefail   # NOT -e: every container must be attempted even if one stop fails.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# shellcheck source=/dev/null
source "${repo_root}/helm/versions.env"

DOCKER="${DOCKER:-docker}"

# kind labels every node container with io.x-k8s.kind.cluster=<name> at
# creation time (verified live: `docker inspect <node> --format
# '{{json .Config.Labels}}'`) — the same reliable selector doctor-live.sh's
# node-name convention targets by name instead.
mapfile -t running_nodes < <(${DOCKER} ps -q --filter "label=io.x-k8s.kind.cluster=${CLUSTER_NAME}")

# The local registry carries no kind label (it is an ordinary standalone
# container, per cluster-down.sh's own comment) — matched by its well-known
# fixed name instead.
mapfile -t running_registry < <(${DOCKER} ps -q --filter "name=^kind-registry\$")

targets=("${running_nodes[@]}" "${running_registry[@]}")

if [ "${#targets[@]}" -eq 0 ]; then
  echo "No running '${CLUSTER_NAME}' cluster or registry containers — nothing to pause."
  exit 0
fi

echo "==> pausing ${#targets[@]} container(s) for cluster '${CLUSTER_NAME}'"
failures=0
for id in "${targets[@]}"; do
  name="$(${DOCKER} inspect "${id}" --format '{{.Name}}' 2>/dev/null | sed 's#^/##')"
  echo "  stopping ${name:-$id} ..."
  if ! ${DOCKER} stop "${id}" >/dev/null; then
    echo "  FAILED to stop ${name:-$id}" >&2
    failures=$((failures + 1))
  fi
done

if [ "${failures}" -gt 0 ]; then
  echo "cluster-pause: ${failures} container(s) failed to stop — see output above." >&2
  exit 1
fi

echo "cluster-pause: ${#targets[@]} container(s) stopped. Resume with 'make cluster-resume'."
