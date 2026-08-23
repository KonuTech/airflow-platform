#!/usr/bin/env bash
#
# Render both Helm values profiles for all nine pinned charts into a
# gitignored build directory, then validate every rendered document with
# `kubeconform -strict` against the pinned Kubernetes version (CICD-07).
#
# Ten `helm template` calls per profile — the `cluster` chart renders TWICE
# (airflow metadata + analytical), so "all nine pinned charts" (ingress-nginx,
# cloudnative-pg, cluster, minio, airflow, otel-collector, tempo — plan 07-03
# added these two — monitoring/kube-prometheus-stack, plan 07-07's own
# addition, and kyverno, plan 11-03's addition) produces ten output files.
# Exactly mirrors
# scripts/stages/*.sh's chart-ref/namespace/values pairing (D-09) — this is
# the offline analogue of the live cluster-up path, not a second definition
# of it. (Vault is deployed live by scripts/stages/80-vault.sh but was never
# added to this render loop — a pre-existing gap, out of this plan's scope.)
#
# NEVER pipe `helm template` output into `kubectl apply` (02-RESEARCH.md
# Anti-Patterns): rendering writes to a gitignored build directory only.
#
# NEVER pass `--no-hooks` (Pitfall 9): it drops both the
# `analytics-db-cluster-ping-test` test-hook Job (fine to drop) AND MinIO's
# `mio-minio-post-job` bucket-bootstrap Job (NOT fine — it is a genuine part
# of the deployed system and must be validated). `helm.sh/hook: test`
# filtering happens in the policy tests, on the annotation, not here.
#
# `-schema-location` entries are added only for CNPG: ingress-nginx 4.15.1
# ships no CRDs at all (verified: no crds/ directory, no
# CustomResourceDefinition template).
#
# `-skip CustomResourceDefinition` (discovered this session, not in
# 02-RESEARCH.md): the cloudnative-pg chart itself EMITS eleven
# CustomResourceDefinition documents (apiVersion apiextensions.k8s.io/v1) —
# these are the meta-resources that DEFINE Cluster/Backup/Pooler/etc, not
# instances of them, and kubeconform's own default schema catalog
# (yannh/kubernetes-json-schema) has never carried a schema for the
# CustomResourceDefinition kind itself, at any Kubernetes version — verified
# absent from v1.35.5-standalone-strict, v1.30.0-standalone-strict and
# master-standalone-strict alike. This is a narrow, well-documented gap in an
# upstream catalogue, not a softening of the gate for content it could
# otherwise validate: every *instance* of a CNPG-defined kind (a `Cluster` CR)
# is still validated in full via the vendored helm/schemas/cnpg/ location
# above, which is what Pitfall 3 and the CRD-schema non-vacuity test actually
# exercise. Scoped to exactly one kind so no other kind's validation is
# weakened.
#
# `-skip PrometheusRule,ServiceMonitor,Prometheus,Alertmanager` (plan 07-07,
# same narrow, well-documented gap class as CustomResourceDefinition above,
# discovered this session via a live `kubeconform` run): kube-prometheus-
# stack's own CRD-INSTANCE kinds (not the CustomResourceDefinition
# meta-resources — those are unconditionally skipped by the entry above and
# this chart does not even emit any) have no schema in kubeconform's default
# catalog either. Unlike CNPG's `Cluster`, this project does not vendor a
# schema for these four kinds: `PrometheusRule`/`ServiceMonitor` carry no
# container/resource content to validate deeply (pure config the Operator
# reconciles), and `Prometheus`/`Alertmanager`'s one property this project
# actually cares about getting right — real `spec.resources`/`spec.replicas`
# counting toward the CI budget, not silently zero — is already covered by a
# stronger, more targeted check than a generic JSON-schema validation would
# be: tests/policy/test_manifest_resources.py's own `custom_resource_
# requests()` (the same Pitfall-6-avoiding treatment `cluster_requests()`
# already gives CNPG's `Cluster` kind).
#
# Usage: scripts/render-manifests.sh
# Exit status: non-zero if any chart fails to render, or if kubeconform
# reports any invalid document.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# shellcheck source=/dev/null
source "${repo_root}/helm/versions.env"

helm_bin="${repo_root}/tools/bin/helm"
kubeconform_bin="${repo_root}/tools/bin/kubeconform"

for bin_path in "${helm_bin}" "${kubeconform_bin}"; do
  if [ ! -x "${bin_path}" ]; then
    echo "ERROR: ${bin_path} not found. \`make manifests\` installs it as its first step." >&2
    exit 1
  fi
done

"${helm_bin}" repo add ingress-nginx https://kubernetes.github.io/ingress-nginx >/dev/null 2>&1 || true
"${helm_bin}" repo add cnpg https://cloudnative-pg.github.io/charts >/dev/null 2>&1 || true
"${helm_bin}" repo add minio https://charts.min.io >/dev/null 2>&1 || true
"${helm_bin}" repo add apache-airflow https://airflow.apache.org >/dev/null 2>&1 || true
"${helm_bin}" repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts >/dev/null 2>&1 || true
"${helm_bin}" repo add grafana-community https://grafana-community.github.io/helm-charts >/dev/null 2>&1 || true
"${helm_bin}" repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null 2>&1 || true
"${helm_bin}" repo add kyverno https://kyverno.github.io/kyverno/ >/dev/null 2>&1 || true
"${helm_bin}" repo update >/dev/null

# render_one <profile> <release> <chart-ref> <namespace> <version-var-name> <values-basename>
render_one() {
  local profile="$1" release="$2" chart_ref="$3" namespace="$4" version_var="$5" values_basename="$6"
  local version="${!version_var}"
  local values_file="${repo_root}/helm/values/${profile}/${values_basename}.yaml"
  local out_dir="${repo_root}/build/manifests/${profile}"
  mkdir -p "${out_dir}"
  echo "==> [${profile}] helm template ${release} ${chart_ref} --version ${version}"
  "${helm_bin}" template "${release}" "${chart_ref}" \
    --version "${version}" \
    --namespace "${namespace}" \
    -f "${values_file}" \
    > "${out_dir}/${values_basename}.yaml"
}

for profile in local ci; do
  render_one "${profile}" ingress-nginx ingress-nginx/ingress-nginx ingress-nginx \
    INGRESS_NGINX_CHART_VERSION ingress-nginx
  render_one "${profile}" cnpg-operator cnpg/cloudnative-pg cnpg-system \
    CNPG_OPERATOR_CHART_VERSION cnpg-operator
  render_one "${profile}" airflow-db cnpg/cluster data \
    CNPG_CLUSTER_CHART_VERSION cnpg-airflow
  render_one "${profile}" analytics-db cnpg/cluster data \
    CNPG_CLUSTER_CHART_VERSION cnpg-analytics
  render_one "${profile}" minio minio/minio data \
    MINIO_CHART_VERSION minio
  render_one "${profile}" airflow apache-airflow/airflow airflow \
    AIRFLOW_CHART_VERSION airflow
  render_one "${profile}" otel-collector open-telemetry/opentelemetry-collector monitoring \
    OTEL_COLLECTOR_CHART_VERSION otel-collector
  render_one "${profile}" tempo grafana-community/tempo monitoring \
    TEMPO_CHART_VERSION tempo
  render_one "${profile}" monitoring prometheus-community/kube-prometheus-stack monitoring \
    KUBE_PROMETHEUS_STACK_CHART_VERSION monitoring
  render_one "${profile}" kyverno kyverno/kyverno kyverno \
    KYVERNO_CHART_VERSION kyverno
done

echo "==> kubeconform -strict against Kubernetes ${KUBERNETES_VERSION}"
"${kubeconform_bin}" -strict -summary \
  -kubernetes-version "${KUBERNETES_VERSION}" \
  -schema-location default \
  -schema-location "${repo_root}/helm/schemas/cnpg/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json" \
  -skip CustomResourceDefinition,PrometheusRule,ServiceMonitor,Prometheus,Alertmanager \
  "${repo_root}"/build/manifests/local/*.yaml \
  "${repo_root}"/build/manifests/ci/*.yaml
