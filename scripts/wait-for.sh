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
#   wait_for_pod_running <namespace> <pod>
#
# `wait_for_pod_running` (plan 05-01) waits on `.status.phase` alone, never
# readiness: Vault's own readinessProbe hits `/v1/sys/health`, which fails
# while the server is sealed, so `wait_for_statefulset_ready`'s
# `readyReplicas` condition would hang forever on a freshly-installed,
# still-sealed vault-0. Waiting for Running (the kubelet has started the
# container) is all `scripts/stages/80-vault.sh` needs before handing off to
# `make vault-unseal`.
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

wait_for_pod_running() {
  local namespace="$1"
  local pod="$2"
  # Debug session ci-pipeline-ingestion-timeout, continuation session 2
  # (2026-08-24): a NAMED (non-label-selector) `kubectl wait` fails FAST
  # with NotFound if the object does not exist yet at call time -- it does
  # NOT poll for the object's creation, only for its condition once it
  # exists. This is a documented kubectl behavior (kubernetes/kubectl#1516),
  # not a bug in this script, but it produced a real, recurring cluster-up
  # flake here: `scripts/stages/80-vault.sh` calls `helm upgrade --install
  # vault` then immediately calls this function, and the StatefulSet
  # controller can take a moment after Helm reports "STATUS: deployed" to
  # actually create the vault-0 Pod object -- hit twice in direct
  # succession live this session (`Error from server (NotFound): pods
  # "vault-0" not found` -> `cluster-up` exits 2). The EXACT SAME race class
  # was already independently diagnosed this same debug session for
  # tests/e2e/vault/test_unseal_survives_restart.py's own raw `kubectl
  # wait` call (see that debug session's Eliminated section) -- this is the
  # first fix for it, scoped to this shared helper's only production caller.
  # `--for=create` (kubectl 1.23+, this project pins 1.36.1) is the
  # kubectl-native solution: it succeeds immediately if the object already
  # exists (the common, fast-Helm case) and polls for its creation
  # otherwise -- verified against a NAMED resource (not a label selector,
  # where this flag has documented limitations per kubernetes/kubectl#1675,
  # not applicable here).
  _kubectl_wait -n "${namespace}" wait --for=create \
    --timeout="${WAIT_POD_CREATE_TIMEOUT:-30s}" "pod/${pod}"
  _kubectl_wait -n "${namespace}" wait --for=jsonpath='{.status.phase}'=Running \
    --timeout="${WAIT_POD_TIMEOUT:-180s}" "pod/${pod}"
}
