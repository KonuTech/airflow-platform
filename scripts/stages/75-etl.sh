#!/usr/bin/env bash
#
# The etl namespace's RBAC (plan 04-02) — last in the LC_ALL=C stage order.
# Numbered after 70-airflow.sh, not before: kubernetes/rbac-etl.yaml's
# RoleBinding names the ServiceAccounts `airflow-worker` and
# `airflow-scheduler` as subjects, and both must already exist — they are
# created by the Airflow chart itself, installed in 70-airflow.sh. Nothing
# later in this stage runner depends on this one.
#
# One step: apply the committed kubernetes/rbac-etl.yaml (the same
# `kubectl apply -f <committed kubernetes/ path>` shape
# scripts/stages/20-namespaces.sh's own header comment documents — a second,
# equally narrow instance, not a new exception; no `-n` flag, matching
# 20-namespaces.sh's own convention, since every document in the file already
# carries its own `metadata.namespace`).
#
# Originally TWO steps: this stage also handed off to scripts/etl-secrets.sh
# for the etl namespace's three dev-only credential Secrets (plan 04-02).
# Plan 05-03 deleted that script and this call: all three credentials
# (csv-processor-db, csv-processor-s3 — plan 05-02; airflow-minio-connection
# — this plan) are now Vault-served, sourced once (by scripts/vault-
# bootstrap.py) FROM those Secrets at migration time, never created by this
# stage again.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "==> applying kubernetes/rbac-etl.yaml"
kubectl --context "${KUBECTL_CONTEXT:-kind-${CLUSTER_NAME}}" apply -f "${repo_root}/kubernetes/rbac-etl.yaml"
