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
#
# Post-merge fix (CICD-09 follow-up): `25-kyverno.sh`'s own `wait_for_
# deploy_available` (condition=Available) can report true a few hundred
# milliseconds before kube-proxy has actually programmed the
# `kyverno-svc.kyverno.svc` ClusterIP's iptables/ipvs rules on this node --
# live-diagnosed (this session, a genuinely fresh ephemeral CI cluster)
# failing with `dial tcp <svc-ip>:443: connect: connection refused` on the
# very next line. The EXACT same class of Deployment-Available-but-Service-
# not-yet-routable race 02-RESEARCH.md Pattern 3 already documents for the
# CNPG admission webhook (this file's own module docstring, via tests/
# policy/test_no_manual_kubectl_surgery.py) -- never caught here before
# because a persistent local cluster's kube-proxy state is always already
# settled between runs. A bounded retry on the apply itself (never a sleep
# before it) keeps this the same single committed-path `kubectl apply -f`
# INFRA-07 already permits, just retried.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "==> applying kubernetes/kyverno-policy.yaml"
applied=0
for attempt in $(seq 1 10); do
  if kubectl --context "${KUBECTL_CONTEXT:-kind-${CLUSTER_NAME}}" apply -f "${repo_root}/kubernetes/kyverno-policy.yaml"; then
    applied=1
    break
  fi
  echo "    kyverno-svc webhook not yet routable (attempt ${attempt}/10) -- retrying"
  sleep 3
done
if [ "${applied}" != "1" ]; then
  echo "ERROR: kubectl apply -f kubernetes/kyverno-policy.yaml never succeeded after 10 attempts" >&2
  exit 1
fi
