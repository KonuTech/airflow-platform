#!/usr/bin/env bash
#
# Sourceable helper: helm_install <release> <chart-ref> <namespace>
#                                  <version-var-name> <values-basename>
#                                  [<wait-strategy>]
#
# Wraps `helm upgrade --install` with the D-09 conventions this phase needs:
#   - the chart version comes from the named `helm/versions.env` variable,
#     never a literal in the caller (tests/policy/test_pinned_tool_versions_agree.py
#     and the Makefile-scanner-target policy establish the "one source" rule;
#     this is the same rule for chart versions)
#   - the values file is resolved from the caller's PROFILE (defaulting to
#     `local`), and a missing file is a hard failure, not a fall-through to
#     chart defaults
#   - `--wait=watcher` is the DEFAULT strategy (6th arg omitted). In Helm
#     4.2.3 `--wait` is a WaitStrategy whose default *when the flag is
#     omitted entirely* is `hookOnly` — 02-RESEARCH.md Pitfall/Pattern 3
#     measured `helm upgrade --install` without an explicit wait returning
#     in 1.0s with the workload not yet serving. `--atomic` does not exist
#     in Helm 4.
#   - the 6th argument overrides the strategy for charts where `watcher`
#     deadlocks: `watcher` waits for a chart's own (non-hook) Deployments/
#     StatefulSets to become Ready BEFORE Helm ever runs its `post-install`
#     hooks (verified live, plan 02-06 — the Airflow chart's
#     `wait-for-airflow-migrations` initContainer blocks every workload's
#     readiness on `airflow-run-airflow-migrations`, itself a
#     `post-install` hook, so `watcher` never gets far enough to run it and
#     times out at `--timeout` with every workload `Progress deadline
#     exceeded`). `hookOnly` runs hooks without first waiting on the
#     chart's own resources, breaking that cycle; the caller is then
#     responsible for its own `wait_for_*` calls afterward
#     (scripts/wait-for.sh) — see scripts/stages/70-airflow.sh.
#   - `--create-namespace` is NEVER passed. Namespaces are owned by
#     kubernetes/namespaces.yaml alone (D-13); a second owner of the same
#     object is exactly the failure this convention exists to prevent.
#   - plan 11-04: any argument BEYOND the first 5 (or 6, when a wait
#     strategy is given) is forwarded verbatim as extra literal flags on the
#     final `helm upgrade --install` invocation — this is what lets CI's
#     ephemeral cluster-up install Airflow ONCE, correctly, pointed at a
#     PR's own GHCR image (`--set defaultAirflowRepository=... --set
#     defaultAirflowTag=...`), instead of installing then re-upgrading.
#     Every EXISTING call site in scripts/stages/*.sh passes exactly 5 or 6
#     arguments, so this is a no-op for all of them.
#
# Usage (from another script, after `source scripts/helm-install.sh`):
#   helm_install ingress-nginx ingress-nginx/ingress-nginx ingress-nginx \
#                INGRESS_NGINX_CHART_VERSION ingress-nginx
#   helm_install airflow apache-airflow/airflow airflow \
#                AIRFLOW_CHART_VERSION airflow hookOnly
#   helm_install airflow apache-airflow/airflow airflow \
#                AIRFLOW_CHART_VERSION airflow hookOnly \
#                --set defaultAirflowRepository=ghcr.io/owner/airflow \
#                --set defaultAirflowTag=pr-42

helm_install() {
  local release="$1"
  local chart_ref="$2"
  local namespace="$3"
  local version_var="$4"
  local values_basename="$5"
  local wait_strategy="${6:-watcher}"

  # Consume exactly the positional arguments named above (always 6 — a
  # caller that omitted the 6th, wait_strategy, argument has $# <= 5 here
  # and there can be no extras to shift past), so anything left in "$@" is
  # genuinely extra — never accidentally re-including values_basename/
  # wait_strategy themselves.
  local -a extra_args=()
  if [ "$#" -gt 6 ]; then
    shift 6
    extra_args=("$@")
  fi

  local repo_root
  repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

  # shellcheck source=/dev/null
  source "${repo_root}/helm/versions.env"

  local version="${!version_var:-}"
  if [ -z "${version}" ]; then
    echo "ERROR: helm_install: ${version_var} is not set in helm/versions.env" >&2
    return 1
  fi

  local profile="${PROFILE:-local}"
  local values_file="${repo_root}/helm/values/${profile}/${values_basename}.yaml"
  if [ ! -f "${values_file}" ]; then
    echo "ERROR: helm_install: no values file at ${values_file} (PROFILE=${profile})" >&2
    return 1
  fi

  local helm_bin="${repo_root}/tools/bin/helm"
  local -a context_args=()
  if [ -n "${KUBECTL_CONTEXT:-}" ]; then
    context_args=(--kube-context "${KUBECTL_CONTEXT}")
  fi

  echo "helm upgrade --install ${release} ${chart_ref} --version ${version} -n ${namespace} -f ${values_file} --wait=${wait_strategy}${extra_args[*]:+ ${extra_args[*]}}"
  "${helm_bin}" upgrade --install "${release}" "${chart_ref}" \
    --version "${version}" \
    --namespace "${namespace}" \
    -f "${values_file}" \
    --wait="${wait_strategy}" \
    --timeout "${HELM_INSTALL_TIMEOUT:-5m}" \
    "${context_args[@]}" \
    "${extra_args[@]}"
}
