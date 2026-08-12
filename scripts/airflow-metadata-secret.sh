#!/usr/bin/env bash
#
# The one hand-written adapter of the phase (02-RESEARCH.md Pattern 4). The
# Airflow chart wants a Secret named by `data.metadataSecretName` with
# exactly one key, `connection` (airflow/templates/_helpers.yaml:77-78,
# 463-465, chart 1.22.0 — read from the pinned chart, not from
# documentation). CloudNativePG's own `<cluster>-app` Secret carries eleven
# different keys instead (dbname, fqdn-jdbc-uri, fqdn-uri, host, jdbc-uri,
# password, pgpass, port, uri, user, username). This script joins the two.
#
# TRUST MODEL (read before touching this file): the principal performing the
# cross-namespace read below is the DEVELOPER'S OWN KUBECONFIG CONTEXT. This
# script runs host-side, during `make cluster-up`, as the human who created
# the cluster — using the credentials `kind` wrote to their kubeconfig. It
# therefore needs no ServiceAccount, no Role and no RoleBinding, and this
# script creates none: nothing INSIDE the cluster is granted cross-namespace
# Secret access, and no in-cluster workload can perform this read. Phase 5
# replaces this whole script with Vault; whatever in-cluster mechanism
# eventually replaces THIS SPECIFIC READ (a Job, an operator, a Vault sync)
# will need either a Role+RoleBinding in `data` naming a ServiceAccount in
# `airflow`, or a Vault Kubernetes-auth role bound to
# `system:serviceaccount:airflow:<name>` plus a policy scoped to this one
# Secret. Neither exists yet, deliberately.
#
# D-13 places both CNPG `Cluster` CRs in namespace `data`, while the Airflow
# chart requires its metadata Secret in the chart's own namespace (`airflow`)
# — this read crosses that boundary on purpose, and supersedes the
# architecture diagram in 02-RESEARCH.md, which drew `airflow-db` inside the
# `airflow` namespace.
#
# CROSS-NAMESPACE DNS: the CNPG-generated Secret's own bare `host` key
# (`airflow-db-rw`) resolves ONLY from within namespace `data` — a pod's
# default DNS search list covers its own namespace, not an arbitrary other
# one. Airflow's pods run in namespace `airflow`, so the connection string
# this script assembles qualifies the host as `<host>.<source-namespace>`
# (`airflow-db-rw.data`) — the same short, namespace-qualified form CNPG's
# own `uri`/`jdbc-uri` keys already use (verified against the live Secret
# this session) — never the bare short name and never the FQDN.
#
# Two more Secrets, generated once and left alone on every later `ensure`:
#   airflow-fernet-key      key `fernet-key`      (_helpers.yaml:70-71)
#   airflow-api-secret-key  key `api-secret-key`  (_helpers.yaml:113-115 —
#     Airflow 3+'s `apiSecretKeySecretName` contract; NOT the deprecated
#     `webserverSecretKeySecretName`, which only takes effect for
#     `airflowVersion < 3.0.0` and would be silently inert on this cluster)
# Leaving either unmanaged makes the chart regenerate it on every
# `helm upgrade`, orphaning everything previously encrypted (fernet) or
# signed (API) with the old value (02-RESEARCH.md PITFALLS B8).
#
# No value generated or read by this script is ever written to the working
# tree, passed as a command-line argument (T-02-23 — a credential in argv is
# visible in `ps`/`/proc/<pid>/cmdline` to any local user on a shared host),
# or echoed. Every secret payload moves only through a pipe:
# `kubectl apply -f -` on stdin, `printf | python3` for URL-encoding.
#
# One subcommand:
#   scripts/airflow-metadata-secret.sh ensure

set -euo pipefail

SOURCE_NAMESPACE="data"
SOURCE_SECRET="airflow-db-app"

TARGET_NAMESPACE="airflow"
METADATA_SECRET="airflow-metadata"
FERNET_SECRET="airflow-fernet-key"
API_SECRET="airflow-api-secret-key"

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
# `kubectl apply -f -` on stdin — values never appear in argv (T-02-23),
# mirroring scripts/minio-credentials.sh's `_create_secret`.
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

_read_source_key() {
  local key="$1"
  _kubectl get secret -n "${SOURCE_NAMESPACE}" "${SOURCE_SECRET}" \
    -o jsonpath="{.data.${key}}" | base64 -d
}

_random_hex() {
  local bytes="${1:-32}"
  openssl rand -hex "${bytes}"
}

# A Fernet key is urlsafe-base64(32 random bytes) — `openssl rand -base64 32`
# already produces exactly that shape (44 chars incl. one '=' pad); the only
# gap is standard-vs-urlsafe alphabet (`+/` vs `-_`), so `tr` closes it. No
# python/cryptography dependency needed for a value this constrained.
_fernet_key() {
  openssl rand -base64 32 | tr -d '\n' | tr '+/' '-_'
}

# _urlencode: reads a raw value on stdin, writes the URL-encoded form on
# stdout. Never a CLI argument (see the header note on T-02-23). CNPG's own
# generated passwords can contain characters (`@`, `:`, `/`, `%`, ...) that
# break a URI's own grammar if left raw.
_urlencode() {
  python3 -c 'import sys, urllib.parse; sys.stdout.write(urllib.parse.quote(sys.stdin.read(), safe=""))'
}

cmd_ensure() {
  # -- the derived metadata connection --------------------------------
  # Recomputed every run: while the source Secret is unchanged (CNPG never
  # rotates it after initial creation) the assembled connection string is
  # byte-identical, so `kubectl apply` performs a no-op PATCH and the
  # Secret's resourceVersion does not move — verified by running `ensure`
  # twice.
  local username password host port dbname
  username="$(_read_source_key username)"
  password="$(_read_source_key password)"
  host="$(_read_source_key host)"
  port="$(_read_source_key port)"
  dbname="$(_read_source_key dbname)"

  local encoded_user encoded_password
  encoded_user="$(printf '%s' "${username}" | _urlencode)"
  encoded_password="$(printf '%s' "${password}" | _urlencode)"

  local connection
  connection="postgresql://${encoded_user}:${encoded_password}@${host}.${SOURCE_NAMESPACE}:${port}/${dbname}"

  echo "==> deriving Secret ${TARGET_NAMESPACE}/${METADATA_SECRET} (key: connection) from ${SOURCE_NAMESPACE}/${SOURCE_SECRET}"
  _apply_secret "${TARGET_NAMESPACE}" "${METADATA_SECRET}" "connection=${connection}"
  unset connection encoded_user encoded_password username password

  # -- Fernet + API secret keys: created once, left alone thereafter -----
  if _secret_exists "${TARGET_NAMESPACE}" "${FERNET_SECRET}"; then
    echo "==> Secret ${TARGET_NAMESPACE}/${FERNET_SECRET} already exists — leaving it unchanged"
  else
    echo "==> creating Secret ${TARGET_NAMESPACE}/${FERNET_SECRET}"
    _apply_secret "${TARGET_NAMESPACE}" "${FERNET_SECRET}" "fernet-key=$(_fernet_key)"
  fi

  if _secret_exists "${TARGET_NAMESPACE}" "${API_SECRET}"; then
    echo "==> Secret ${TARGET_NAMESPACE}/${API_SECRET} already exists — leaving it unchanged"
  else
    echo "==> creating Secret ${TARGET_NAMESPACE}/${API_SECRET}"
    _apply_secret "${TARGET_NAMESPACE}" "${API_SECRET}" "api-secret-key=$(_random_hex 32)"
  fi
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
