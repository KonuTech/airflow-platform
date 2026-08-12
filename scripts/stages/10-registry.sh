#!/usr/bin/env bash
#
# Local OCI registry instead of `kind load docker-image`. Not an optimisation:
# 02-RESEARCH.md Pitfall 1 measured `kind load` failing outright on Docker 29's
# containerd image store (`ctr: content digest ... not found`) for multi-arch
# images, while the local registry pushed the same image in 2.6s.
#
# Published on 127.0.0.1 ONLY, never 0.0.0.0 (T-02-03, threat register:
# reachable from the cluster over the `kind` Docker network, never the host's
# LAN). Re-runnable: starting an already-running registry, or reconnecting an
# already-connected network, is a no-op.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# shellcheck source=/dev/null
source "${repo_root}/helm/versions.env"

registry_name="kind-registry"

if docker inspect "${registry_name}" >/dev/null 2>&1; then
  if [ "$(docker inspect -f '{{.State.Running}}' "${registry_name}")" != "true" ]; then
    echo "==> starting existing (stopped) registry container '${registry_name}'"
    docker start "${registry_name}" >/dev/null
  else
    echo "==> local registry container '${registry_name}' already running"
  fi
else
  echo "==> starting local registry container '${registry_name}' on 127.0.0.1:${REGISTRY_PORT}"
  docker run -d --restart=always \
    -p "127.0.0.1:${REGISTRY_PORT}:5000" \
    --name "${registry_name}" \
    "${REGISTRY_IMAGE}" >/dev/null
fi

if ! docker network inspect kind --format '{{range .Containers}}{{.Name}} {{end}}' 2>/dev/null \
     | grep -qw "${registry_name}"; then
  echo "==> connecting '${registry_name}' to the 'kind' Docker network"
  docker network connect kind "${registry_name}"
fi

kind_bin="${repo_root}/tools/bin/kind"
nodes="$("${kind_bin}" get nodes --name "${CLUSTER_NAME}")"

for node in ${nodes}; do
  echo "==> wiring registry mirror config into node ${node}"
  docker exec "${node}" mkdir -p "/etc/containerd/certs.d/localhost:${REGISTRY_PORT}"
  printf '[host."http://%s:5000"]\n' "${registry_name}" \
    | docker exec -i "${node}" cp /dev/stdin "/etc/containerd/certs.d/localhost:${REGISTRY_PORT}/hosts.toml"
done

echo "==> registry stage complete (push images to localhost:${REGISTRY_PORT}/<name>)"
