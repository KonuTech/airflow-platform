#!/usr/bin/env bash
#
# D-14: MinIO's root and application credentials are generated directly into
# Kubernetes Secrets in namespace `data` and live ONLY there — never written
# to the working tree, never echoed on `ensure`, never passed as a command-
# line argument that would land in a process listing (T-02-16). This is
# deliberately the exact shape Phase 5's Vault retrofit replaces: the same
# Secret names (`minio-root`, `minio-app`), sourced from Vault instead of
# from this script, so adopting Vault is a configuration change, not a
# redesign.
#
# Two subcommands:
#   scripts/minio-credentials.sh ensure   create both Secrets IF AND ONLY IF
#                                          they do not already exist. Re-running
#                                          against a live cluster is a safe
#                                          no-op — a credential must not rotate
#                                          mid-lifetime. A fresh cluster always
#                                          gets fresh values (D-14: nothing may
#                                          quietly depend on one).
#   scripts/minio-credentials.sh show     read the two Secrets back and print
#                                          shell-sourceable `export` lines, so
#                                          host tooling and the e2e suite never
#                                          hold a stale copy (`make minio-creds`).
#
# Values are generated with `openssl rand -hex`, which is both a
# cryptographically secure source and a character set (0-9a-f only) that
# cannot break the YAML this script pipes to `kubectl apply -f -` on stdin —
# the value never appears as a CLI argument.
#
# Secret shapes (chart 5.4.0's own expectations, read from its
# templates/secrets.yaml and _helper_create_user.txt — not from documentation):
#   minio-root:  data.rootUser, data.rootPassword   (existingSecret)
#   minio-app:   data.secretKey                     (users[].existingSecretKey)

set -euo pipefail

NAMESPACE="data"
ROOT_SECRET="minio-root"
APP_SECRET="minio-app"

kubectl_bin="${KUBECTL:-kubectl}"

_kubectl() {
  if [ -n "${KUBECTL_CONTEXT:-}" ]; then
    "${kubectl_bin}" --context "${KUBECTL_CONTEXT}" "$@"
  else
    "${kubectl_bin}" "$@"
  fi
}

_random_hex() {
  # Pure lowercase hex (0-9a-f) — safe as an unquoted YAML scalar and safe on
  # any shell command line, though it never appears on one here regardless.
  local bytes="${1:-32}"
  openssl rand -hex "${bytes}"
}

# _create_secret <name> <key=value> [<key=value> ...]
# Builds a Secret manifest with `stringData` in memory and pipes it to
# `kubectl apply -f -` on stdin. The values never appear in argv, so they
# never land in `ps`/`/proc/<pid>/cmdline` — T-02-16's exact requirement.
_create_secret() {
  local name="$1"
  shift
  {
    printf 'apiVersion: v1\n'
    printf 'kind: Secret\n'
    printf 'metadata:\n'
    printf '  name: %s\n' "${name}"
    printf '  namespace: %s\n' "${NAMESPACE}"
    printf 'type: Opaque\n'
    printf 'stringData:\n'
    local kv key value
    for kv in "$@"; do
      key="${kv%%=*}"
      value="${kv#*=}"
      # Single-quote the scalar so YAML's core schema cannot ever resolve it
      # to a non-string type. _random_hex output is pure lowercase hex
      # (0-9a-f), so an unquoted value that happens to be all-digits (e.g.
      # "0123456789012345") is a syntactically valid YAML integer -- kubectl
      # then submits a JSON payload with a numeric stringData field, which
      # the API server rejects: "cannot unmarshal number into Go struct
      # field Secret.stringData of type string". Safe here because the only
      # values ever passed through this helper (openssl rand -hex output)
      # can never contain a single quote to escape.
      printf "  %s: '%s'\n" "${key}" "${value}"
    done
  } | _kubectl apply -f - >/dev/null
}

_secret_exists() {
  local name="$1"
  _kubectl get secret -n "${NAMESPACE}" "${name}" >/dev/null 2>&1
}

cmd_ensure() {
  if _secret_exists "${ROOT_SECRET}"; then
    echo "==> Secret ${NAMESPACE}/${ROOT_SECRET} already exists — leaving it unchanged"
  else
    echo "==> creating Secret ${NAMESPACE}/${ROOT_SECRET}"
    _create_secret "${ROOT_SECRET}" \
      "rootUser=$(_random_hex 8)" \
      "rootPassword=$(_random_hex 32)"
  fi

  if _secret_exists "${APP_SECRET}"; then
    echo "==> Secret ${NAMESPACE}/${APP_SECRET} already exists — leaving it unchanged"
  else
    echo "==> creating Secret ${NAMESPACE}/${APP_SECRET}"
    _create_secret "${APP_SECRET}" \
      "secretKey=$(_random_hex 32)"
  fi
}

cmd_show() {
  local root_user root_password app_secret_key
  root_user="$(_kubectl get secret -n "${NAMESPACE}" "${ROOT_SECRET}" -o jsonpath='{.data.rootUser}' | base64 -d)"
  root_password="$(_kubectl get secret -n "${NAMESPACE}" "${ROOT_SECRET}" -o jsonpath='{.data.rootPassword}' | base64 -d)"
  app_secret_key="$(_kubectl get secret -n "${NAMESPACE}" "${APP_SECRET}" -o jsonpath='{.data.secretKey}' | base64 -d)"

  echo "# scripts/minio-credentials.sh show: output contains LIVE credentials — do not log or commit it" >&2

  printf 'export MINIO_ROOT_USER=%q\n' "${root_user}"
  printf 'export MINIO_ROOT_PASSWORD=%q\n' "${root_password}"
  printf 'export MINIO_APP_ACCESS_KEY=%q\n' "etl-app"
  printf 'export MINIO_APP_SECRET_KEY=%q\n' "${app_secret_key}"
}

usage() {
  echo "usage: $(basename "$0") {ensure|show}" >&2
  exit 2
}

main() {
  local sub="${1:-}"
  case "${sub}" in
    ensure) cmd_ensure ;;
    show) cmd_show ;;
    *) usage ;;
  esac
}

main "$@"
