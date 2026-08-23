#!/usr/bin/env bash
#
# Applies kubernetes/kyverno-policy.yaml (D-14/D-15/D-16/D-18) — the
# cluster-wide ImageValidatingPolicy. THIRD instance of the ONE permitted
# `kubectl apply -f <committed kubernetes/ path>` shape (scripts/stages/
# 20-namespaces.sh and scripts/stages/75-etl.sh's own precedent comments),
# not a new exception.
#
# CRITICAL — stage numbering: this file MUST stay named `26-*`, running
# immediately after `25-kyverno.sh` (the admission controller must already
# be Available before this policy is applied) and before `30-ingress-
# nginx.sh` — see that file's own header comment for why this ordering is
# the only one under which every other component's pods actually pass
# through the webhook on a normal cluster-up.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "==> applying kubernetes/kyverno-policy.yaml"
kubectl --context "${KUBECTL_CONTEXT:-kind-${CLUSTER_NAME}}" apply -f "${repo_root}/kubernetes/kyverno-policy.yaml"
