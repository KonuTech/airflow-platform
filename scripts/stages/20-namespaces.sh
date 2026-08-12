#!/usr/bin/env bash
#
# The ONE permitted `kubectl apply` in this repository, and it names a
# committed file. Namespaces are owned by kubernetes/namespaces.yaml alone —
# no Helm release in this repository may pass --create-namespace, so no two
# releases ever manage the same object.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

kubectl --context "${KUBECTL_CONTEXT:-kind-${CLUSTER_NAME}}" apply -f "${repo_root}/kubernetes/namespaces.yaml"
