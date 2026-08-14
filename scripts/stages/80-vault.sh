#!/usr/bin/env bash
#
# The Vault component stage (plan 05-01). Numbered after 75-etl.sh: nothing
# in this stage depends on the etl namespace's own contents, but 80 keeps
# Vault last among the D-08/D-14-era component stages, consistent with it
# being the newest addition to this stage runner.
#
# Unlike every earlier chart in this runner, `helm_install`'s default
# `watcher` wait strategy would DEADLOCK here: Vault's own StatefulSet will
# NEVER report Ready while sealed (its readinessProbe hits
# `/v1/sys/health`, which fails while sealed) -- this is the exact same
# sealed/migration-gated deadlock 70-airflow.sh already avoids for the
# Airflow chart's own analogous readiness dependency, documented in
# scripts/helm-install.sh's own header comment. `hookOnly` is passed
# explicitly for the same reason.
#
# This stage installs and waits for the pod to be RUNNING (not ready) --
# it deliberately does NOT unseal or bootstrap Vault. Those are two
# separate, scripted steps (D-02, plan 05-01 Task 2) a developer runs by
# hand after `make cluster-up` completes: `make vault-unseal` then
# `make vault-bootstrap`.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# shellcheck source=/dev/null
source "${repo_root}/helm/versions.env"
# shellcheck source=/dev/null
source "${repo_root}/scripts/helm-install.sh"
# shellcheck source=/dev/null
source "${repo_root}/scripts/wait-for.sh"

helm_bin="${repo_root}/tools/bin/helm"

"${helm_bin}" repo add hashicorp https://helm.releases.hashicorp.com >/dev/null 2>&1 || true
"${helm_bin}" repo update hashicorp >/dev/null

helm_install vault hashicorp/vault vault VAULT_CHART_VERSION vault hookOnly

wait_for_pod_running vault vault-0

echo "==> Vault installed and running (sealed). Next steps:"
echo "      make vault-unseal      # D-02: init-or-unseal against .secrets/vault-init.json"
echo "      make vault-bootstrap   # mounts, auth method, roles/policies, audit device"
