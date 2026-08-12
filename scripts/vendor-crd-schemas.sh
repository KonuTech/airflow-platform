#!/usr/bin/env bash
#
# Regenerate helm/schemas/cnpg/ from the pinned CloudNativePG operator chart
# (D-12 / CICD-07). Run whenever CNPG_OPERATOR_CHART_VERSION in
# helm/versions.env moves — the generated schemas and the CRDs they describe
# must stay in lockstep, and the only way to guarantee that is regenerating
# from the pinned chart rather than hand-maintaining the JSON.
#
# Pipeline (02-RESEARCH.md § Code Examples "Offline CRD schema vendoring for
# kubeconform", this exact shape verified turning kubeconform's
# "could not find schema for Cluster" (exit 1) into "Valid: 3, Invalid: 0,
# Errors: 0" (exit 0)):
#   1. `helm template` the pinned cloudnative-pg chart — this renders ALL of
#      its resources, CRDs included, without installing anything.
#   2. tools/k8s/crd_to_jsonschema.py filters that stream down to
#      CustomResourceDefinition documents itself (python3 + the already-
#      available PyYAML — no `yq` added as a sixth pinned binary) and
#      converts each version's schema.openAPIV3Schema into a JSON Schema
#      file named for kubeconform's -schema-location template.
#
# ingress-nginx 4.15.1 ships no CRDs at all (verified: no crds/ directory, no
# CustomResourceDefinition template) — CNPG is the only chart this phase
# vendors schemas for.
#
# Idempotent and deterministic: re-running with no chart-pin change produces
# byte-identical output (tests/policy/test_manifest_validation_fails_closed.py
# and 02-07-PLAN.md Task 1's <verify> block both check
# `git status --porcelain helm/schemas` is empty afterwards).
#
# Usage: scripts/vendor-crd-schemas.sh

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# shellcheck source=/dev/null
source "${repo_root}/helm/versions.env"

helm_bin="${repo_root}/tools/bin/helm"
if [ ! -x "${helm_bin}" ]; then
  echo "ERROR: ${helm_bin} not found. Run tools/k8s/install_helm.sh first." >&2
  exit 1
fi

workdir="$(mktemp -d)"
cleanup() { rm -rf "${workdir}"; }
trap cleanup EXIT

"${helm_bin}" repo add cnpg https://cloudnative-pg.github.io/charts >/dev/null 2>&1 || true
"${helm_bin}" repo update cnpg >/dev/null

echo "==> rendering cloudnative-pg ${CNPG_OPERATOR_CHART_VERSION} to extract its CRDs"
"${helm_bin}" template cnpg cnpg/cloudnative-pg \
  --version "${CNPG_OPERATOR_CHART_VERSION}" \
  --namespace cnpg-system \
  > "${workdir}/rendered.yaml"

schema_dir="${repo_root}/helm/schemas/cnpg"
mkdir -p "${schema_dir}"

# Remove every previously-generated schema before regenerating: a CRD kind
# renamed or removed upstream must not leave a stale schema file behind that
# still looks vendored-and-current but no longer corresponds to anything in
# the pinned chart.
find "${schema_dir}" -maxdepth 1 -name '*.json' -delete

(
  cd "${repo_root}"
  uv run --frozen python3 tools/k8s/crd_to_jsonschema.py \
    "${workdir}/rendered.yaml" "${schema_dir}"
)
