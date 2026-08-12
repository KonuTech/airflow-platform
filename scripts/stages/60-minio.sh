#!/usr/bin/env bash
#
# The MinIO component stage (D-08, D-14). Credentials MUST exist before the
# chart installs, because the chart references them by Secret name
# (`existingSecret`, `users[].existingSecret`) — running the chart first
# would leave the post-install Job's `minio-make-user` container waiting on
# a Secret nobody created.
#
# Pitfall 8: the post-job's bucket and user containers run in PARALLEL, so
# this stage adds no `customCommands` step that assumes a bucket already
# exists — none is needed here.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# shellcheck source=/dev/null
source "${repo_root}/helm/versions.env"
# shellcheck source=/dev/null
source "${repo_root}/scripts/helm-install.sh"
# shellcheck source=/dev/null
source "${repo_root}/scripts/wait-for.sh"

helm_bin="${repo_root}/tools/bin/helm"

echo "==> ensuring MinIO credentials (Secrets minio-root, minio-app in namespace data)"
"${repo_root}/scripts/minio-credentials.sh" ensure

"${helm_bin}" repo add minio https://charts.min.io >/dev/null 2>&1 || true
"${helm_bin}" repo update minio >/dev/null

helm_install minio minio/minio data MINIO_CHART_VERSION minio

wait_for_deploy_available data minio
