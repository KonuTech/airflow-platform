#!/usr/bin/env bash
#
# Sourceable helper: helm_install <release> <chart-ref> <namespace>
#                                  <version-var-name> <values-basename>
#
# Wraps `helm upgrade --install` with the D-09 conventions this phase needs:
#   - the chart version comes from the named `helm/versions.env` variable,
#     never a literal in the caller (tests/policy/test_pinned_tool_versions_agree.py
#     and the Makefile-scanner-target policy establish the "one source" rule;
#     this is the same rule for chart versions)
#   - the values file is resolved from the caller's PROFILE (defaulting to
#     `local`), and a missing file is a hard failure, not a fall-through to
#     chart defaults
#   - `--wait=watcher` is passed EXPLICITLY. In Helm 4.2.3 `--wait` is a
#     WaitStrategy whose default *when the flag is omitted entirely* is
#     `hookOnly` — 02-RESEARCH.md Pitfall/Pattern 3 measured
#     `helm upgrade --install` without an explicit wait returning in 1.0s with
#     the workload not yet serving. `--atomic` does not exist in Helm 4.
#   - `--create-namespace` is NEVER passed. Namespaces are owned by
#     kubernetes/namespaces.yaml alone (D-13); a second owner of the same
#     object is exactly the failure this convention exists to prevent.
#
# Usage (from another script, after `source scripts/helm-install.sh`):
#   helm_install ingress-nginx ingress-nginx/ingress-nginx ingress-nginx \
#                INGRESS_NGINX_CHART_VERSION ingress-nginx

helm_install() {
  local release="$1"
  local chart_ref="$2"
  local namespace="$3"
  local version_var="$4"
  local values_basename="$5"

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

  echo "helm upgrade --install ${release} ${chart_ref} --version ${version} -n ${namespace} -f ${values_file}"
  "${helm_bin}" upgrade --install "${release}" "${chart_ref}" \
    --version "${version}" \
    --namespace "${namespace}" \
    -f "${values_file}" \
    --wait=watcher \
    --timeout "${HELM_INSTALL_TIMEOUT:-5m}" \
    "${context_args[@]}"
}
