#!/usr/bin/env bash
#
# The etl/airflow namespaces' dev-only credential Secret(s) not yet migrated
# to Vault. Originally three Secrets (plan 04-02). Plan 05-02 retired
# csv-processor-db and csv-processor-s3 once resolve_secret()'s vault://
# scheme (packages/dataplat/src/dataplat/secrets/resolver.py) and the KPO
# pod's env vars (airflow/dags/_common/kpo.py) proved themselves live
# against the cluster (tests/e2e/vault/test_positive_auth.py,
# test_negative_auth.py) — both credentials are now Vault-served, sourced
# from etl/analytics-db and etl/minio. Only airflow-minio-connection remains
# here, Kubernetes-Secret-served, pending plan 05-03's Airflow VaultBackend
# wiring.
#
# The one remaining Secret is idempotent: `ensure` re-run against a live
# cluster leaves an already-existing Secret completely unchanged (no
# regenerated password, no moved resourceVersion) — the same "a credential
# must not rotate mid-lifetime" doctrine scripts/minio-credentials.sh already
# establishes for MinIO's own root/app credentials.
#
# TRUST MODEL (read before touching this file): identical to
# scripts/airflow-metadata-secret.sh's own. The principal performing every
# read/write below is the DEVELOPER'S OWN KUBECONFIG CONTEXT — this script
# runs host-side, as the human who created the cluster, using the
# credentials `kind` wrote to their kubeconfig. It therefore needs no
# ServiceAccount, no Role and no RoleBinding of its own, and creates none:
# nothing INSIDE the cluster is granted the cross-namespace Secret reads or
# the `kubectl exec` this script performs; no in-cluster workload can do what
# this script does. Plan 05-03 retires the remainder of this script.
#
# No value generated or read by this script is ever written to the working
# tree, passed as a command-line argument (T-02-23/T-04-09 — a credential in
# argv is visible in `ps`/`/proc/<pid>/cmdline` to any local user on a shared
# host), or echoed. Every secret payload moves only through a pipe:
# `kubectl apply -f -` on stdin, `kubectl exec -i ... psql` on stdin, or
# `printf | python3` for URL-encoding — `printf` here is bash's own builtin,
# not an external command, so its argument list never appears in `ps` either
# (the same reasoning scripts/airflow-metadata-secret.sh's `_apply_secret`
# already relies on).
#
# One subcommand:
#   scripts/etl-secrets.sh ensure

set -euo pipefail

DATA_NAMESPACE="data"
AIRFLOW_NAMESPACE="airflow"

MINIO_APP_SECRET="minio-app"
AIRFLOW_MINIO_SECRET="airflow-minio-connection"

# The fixed, non-secret MinIO application access-key literal this repo
# already establishes (scripts/minio-credentials.sh's own `cmd_show`
# hardcodes the identical value) — reused here, never invented anew.
MINIO_APP_ACCESS_KEY="etl-app"

kubectl_bin="${KUBECTL:-kubectl}"

_kubectl() {
  if [ -n "${KUBECTL_CONTEXT:-}" ]; then
    "${kubectl_bin}" --context "${KUBECTL_CONTEXT}" "$@"
  else
    "${kubectl_bin}" "$@"
  fi
}

_secret_exists() {
  local namespace="$1" name="$2"
  _kubectl get secret -n "${namespace}" "${name}" >/dev/null 2>&1
}

# _apply_secret <namespace> <name> <key=value> [<key=value> ...]
# Builds a Secret manifest with `stringData` in memory and pipes it to
# `kubectl apply -f -` on stdin — values never appear in argv, mirroring
# scripts/airflow-metadata-secret.sh's `_apply_secret` exactly (including its
# `${kv%%=*}` / `${kv#*=}` split, which correctly keeps any `=` characters
# that are part of the VALUE itself, e.g. this file's own query-string URI).
_apply_secret() {
  local namespace="$1" name="$2"
  shift 2
  {
    printf 'apiVersion: v1\n'
    printf 'kind: Secret\n'
    printf 'metadata:\n'
    printf '  name: %s\n' "${name}"
    printf '  namespace: %s\n' "${namespace}"
    printf 'type: Opaque\n'
    printf 'stringData:\n'
    local kv key value
    for kv in "$@"; do
      key="${kv%%=*}"
      value="${kv#*=}"
      printf '  %s: %s\n' "${key}" "${value}"
    done
  } | _kubectl apply -f - >/dev/null
}

# _urlencode: reads a raw value on stdin, writes the URL-encoded form on
# stdout. Never a CLI argument. Copied verbatim in shape from
# scripts/airflow-metadata-secret.sh.
_urlencode() {
  python3 -c 'import sys, urllib.parse; sys.stdout.write(urllib.parse.quote(sys.stdin.read(), safe=""))'
}

# _read_minio_app_secret_key: the one source every application MinIO
# credential in this script derives from — scripts/minio-credentials.sh's
# own generated `minio-app` Secret (namespace data, key secretKey).
_read_minio_app_secret_key() {
  _kubectl get secret -n "${DATA_NAMESPACE}" "${MINIO_APP_SECRET}" \
    -o jsonpath='{.data.secretKey}' | base64 -d
}

# airflow-minio-connection Secret (namespace airflow, key
# AIRFLOW_CONN_MINIO_DEFAULT) — Airflow's own S3KeySensor connection, in
# Airflow's URI-form connection encoding (empty host between @ and /, extras
# as query params). The last of the three original Secrets still
# Kubernetes-Secret-served; pending plan 05-03's Airflow VaultBackend wiring.
_ensure_airflow_minio_connection_secret() {
  if _secret_exists "${AIRFLOW_NAMESPACE}" "${AIRFLOW_MINIO_SECRET}"; then
    echo "==> Secret ${AIRFLOW_NAMESPACE}/${AIRFLOW_MINIO_SECRET} already exists — leaving it unchanged"
    return
  fi

  local secret_key encoded_secret_key uri
  secret_key="$(_read_minio_app_secret_key)"
  encoded_secret_key="$(printf '%s' "${secret_key}" | _urlencode)"
  uri="aws://${MINIO_APP_ACCESS_KEY}:${encoded_secret_key}@/?endpoint_url=http%3A%2F%2Fminio.data.svc.cluster.local%3A9000&region_name=us-east-1"

  echo "==> creating Secret ${AIRFLOW_NAMESPACE}/${AIRFLOW_MINIO_SECRET}"
  _apply_secret "${AIRFLOW_NAMESPACE}" "${AIRFLOW_MINIO_SECRET}" \
    "AIRFLOW_CONN_MINIO_DEFAULT=${uri}"
  unset secret_key encoded_secret_key uri
}

cmd_ensure() {
  _ensure_airflow_minio_connection_secret
}

usage() {
  echo "usage: $(basename "$0") {ensure}" >&2
  exit 2
}

main() {
  local sub="${1:-}"
  case "${sub}" in
    ensure) cmd_ensure ;;
    *) usage ;;
  esac
}

main "$@"
