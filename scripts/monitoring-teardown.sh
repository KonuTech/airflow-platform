#!/usr/bin/env bash
#
# Quick task 260824-ayw: tears down the three monitoring releases
# (otel-collector, tempo, monitoring) this project's monitoring stack
# installs. Used ONLY by the new CI staggered path (Makefile's
# observability-verify-ci) -- never by scripts/stages/85-monitoring.sh's
# local persistent-cluster path, which has no reason to ever uninstall what
# it just installed.
#
# `helm uninstall` does not delete the PVCs or CRDs these charts created --
# that is fine here because the whole ephemeral kind cluster this runs
# against is torn down at the end of the CI job regardless, so nothing is
# left orphaned beyond the job's own lifetime.
#
# Errors are NOT swallowed (set -euo pipefail, no `|| true`): this is a
# fail-closed target, matching this repo's existing fail-closed Makefile
# targets (`rollback`, `migrate-analytics`). The rationale is honest CI
# failure signal, not protection of `make rebuild-from-raw` -- that later
# step in .github/workflows/e2e-full.yml carries no `if: always()`/
# `continue-on-error`, so on a single-use ephemeral GitHub Actions runner a
# failure here stops the job outright and `rebuild-from-raw` never actually
# runs in that failure branch anyway. Failing loudly here exists purely so
# whoever debugs a hung/failed CI run gets a true signal instead of a false
# "teardown succeeded" report.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
helm_bin="${repo_root}/tools/bin/helm"

context_args=()
if [ -n "${KUBECTL_CONTEXT:-}" ]; then
  context_args=(--kube-context "${KUBECTL_CONTEXT}")
fi

echo "==> helm uninstall otel-collector tempo monitoring --namespace monitoring"
"${helm_bin}" uninstall otel-collector tempo monitoring \
  --namespace monitoring \
  --wait \
  --timeout "${HELM_UNINSTALL_TIMEOUT:-3m}" \
  "${context_args[@]}"

echo "==> monitoring stack torn down (otel-collector, tempo, monitoring)"
