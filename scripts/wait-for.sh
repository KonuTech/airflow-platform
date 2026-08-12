#!/usr/bin/env bash
#
# Sourceable helper: the hand-written readiness waits Helm 4 cannot supply
# (D-09, 02-RESEARCH.md Pattern 3). No `sleep` anywhere — every wait is a
# `kubectl wait --for=condition=...` with an explicit timeout.
#
#   wait_for_crd_established <crd>
#   wait_for_deploy_available <namespace> <deployment>
#   wait_for_cnpg_cluster_ready <namespace> <cluster>
#   wait_for_statefulset_ready <namespace> <statefulset> [<replicas>]
#
# `wait_for_deploy_available` deliberately uses
# `--for=condition=Available`, never `rollout status`: 02-RESEARCH.md
# measured `rollout status` returning "successfully rolled out" in 0.107s
# when queried before the Deployment's generation had been observed by the
# controller — a false positive precisely when it matters.
#
# `wait_for_statefulset_ready` mirrors that same choice: a StatefulSet
# carries no `Available`-style `.status.conditions` entry the way a
# Deployment does, so this waits on a JSONPath condition over
# `.status.readyReplicas` instead of `rollout status`, for the same reason.

_kubectl_wait() {
  if [ -n "${KUBECTL_CONTEXT:-}" ]; then
    kubectl --context "${KUBECTL_CONTEXT}" "$@"
  else
    kubectl "$@"
  fi
}

wait_for_crd_established() {
  local crd="$1"
  _kubectl_wait wait --for=condition=established --timeout="${WAIT_CRD_TIMEOUT:-120s}" "crd/${crd}"
}

wait_for_deploy_available() {
  local namespace="$1"
  local deploy="$2"
  _kubectl_wait -n "${namespace}" wait --for=condition=Available \
    --timeout="${WAIT_DEPLOY_TIMEOUT:-180s}" "deploy/${deploy}"
}

wait_for_cnpg_cluster_ready() {
  local namespace="$1"
  local cluster="$2"
  _kubectl_wait -n "${namespace}" wait --for=condition=Ready \
    --timeout="${WAIT_CNPG_TIMEOUT:-300s}" "cluster/${cluster}"
}

wait_for_statefulset_ready() {
  local namespace="$1"
  local statefulset="$2"
  local replicas="${3:-1}"
  _kubectl_wait -n "${namespace}" wait \
    --for="jsonpath={.status.readyReplicas}=${replicas}" \
    --timeout="${WAIT_STATEFULSET_TIMEOUT:-300s}" "statefulset/${statefulset}"
}
