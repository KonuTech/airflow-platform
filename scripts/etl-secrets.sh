#!/usr/bin/env bash
#
# The etl namespace's three dev-only credential Secrets (plan 04-02). Every
# one of the three is independently idempotent: `ensure` re-run against a
# live cluster leaves an already-existing Secret completely unchanged (no
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
# this script does. Phase 5 replaces this whole script with Vault.
#
# (a)'s `ALTER ROLE etl_app WITH PASSWORD ...` runs via `kubectl exec` into
# the CNPG primary pod itself, authenticating as `postgres` under
# PostgreSQL's peer/local trust (the pod's own local socket, not a network
# connection) — so it needs no new inbound-connection RBAC either, on top of
# needing no in-cluster RBAC to invoke `kubectl exec` in the first place
# (that permission comes from the operator's own cluster-admin kubeconfig,
# per the trust model above).
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

ETL_NAMESPACE="etl"
DATA_NAMESPACE="data"
AIRFLOW_NAMESPACE="airflow"

ANALYTICS_CLUSTER="analytics-db"
DB_SECRET="csv-processor-db"
S3_SECRET="csv-processor-s3"
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

_random_hex() {
  local bytes="${1:-32}"
  openssl rand -hex "${bytes}"
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

# (a) etl_app's Postgres password + the csv-processor-db Secret (namespace
# etl, key dsn). Skipped entirely — including the ALTER ROLE itself — if the
# Secret already exists, so a password already handed to a caller is never
# silently rotated out from under it.
_ensure_csv_processor_db_secret() {
  if _secret_exists "${ETL_NAMESPACE}" "${DB_SECRET}"; then
    echo "==> Secret ${ETL_NAMESPACE}/${DB_SECRET} already exists — leaving it unchanged"
    return
  fi

  echo "==> resolving Cluster/${ANALYTICS_CLUSTER} (namespace ${DATA_NAMESPACE})'s current primary pod"
  local primary_pod
  primary_pod="$(_kubectl get cluster -n "${DATA_NAMESPACE}" "${ANALYTICS_CLUSTER}" \
    -o jsonpath='{.status.currentPrimary}')"
  if [ -z "${primary_pod}" ]; then
    echo "ERROR: Cluster/${ANALYTICS_CLUSTER} (namespace ${DATA_NAMESPACE}) has no currentPrimary yet" >&2
    exit 1
  fi

  # openssl rand -hex is pure [0-9a-f] — a character set that cannot break
  # out of the single-quoted SQL string literal it is embedded in below (no
  # quote character is possible in hex), the same reasoning
  # scripts/minio-credentials.sh already relies on for the YAML it emits.
  local password
  password="$(_random_hex 32)"

  echo "==> setting etl_app's password via kubectl exec (peer/local trust, not a network connection)"
  printf "ALTER ROLE etl_app WITH PASSWORD '%s';\n" "${password}" \
    | _kubectl exec -i -n "${DATA_NAMESPACE}" "${primary_pod}" -- \
        psql -v ON_ERROR_STOP=1 -U postgres -d analytics >/dev/null

  local encoded_password dsn
  encoded_password="$(printf '%s' "${password}" | _urlencode)"
  # Host qualified as <svc>.<namespace> — a pod in etl resolves
  # analytics-db-rw.data via cluster DNS but not the bare service name,
  # exactly like scripts/airflow-metadata-secret.sh's own connection string.
  dsn="postgresql://etl_app:${encoded_password}@analytics-db-rw.${DATA_NAMESPACE}:5432/analytics"

  echo "==> creating Secret ${ETL_NAMESPACE}/${DB_SECRET}"
  _apply_secret "${ETL_NAMESPACE}" "${DB_SECRET}" "dsn=${dsn}"
  unset password encoded_password dsn
}

# (b) csv-processor-s3 Secret (namespace etl, keys access_key/secret_key) —
# the KPO pod's own MinIO application credential.
_ensure_csv_processor_s3_secret() {
  if _secret_exists "${ETL_NAMESPACE}" "${S3_SECRET}"; then
    echo "==> Secret ${ETL_NAMESPACE}/${S3_SECRET} already exists — leaving it unchanged"
    return
  fi

  local secret_key
  secret_key="$(_read_minio_app_secret_key)"

  echo "==> creating Secret ${ETL_NAMESPACE}/${S3_SECRET}"
  _apply_secret "${ETL_NAMESPACE}" "${S3_SECRET}" \
    "access_key=${MINIO_APP_ACCESS_KEY}" \
    "secret_key=${secret_key}"
  unset secret_key
}

# (c) airflow-minio-connection Secret (namespace airflow, key
# AIRFLOW_CONN_MINIO_DEFAULT) — Airflow's own S3KeySensor connection, in
# Airflow's URI-form connection encoding (empty host between @ and /, extras
# as query params).
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
  _ensure_csv_processor_db_secret
  _ensure_csv_processor_s3_secret
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
