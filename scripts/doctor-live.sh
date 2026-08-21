#!/usr/bin/env bash
#
# Live-cluster self-heal for the DAGs hostPath bind mount, distinct from
# `scripts/doctor.sh`.
#
# `make doctor` is a PRE-flight: it runs before `cluster-up` and has nothing
# to say about a cluster that is already running. Three documented incidents
# on this host (.planning/debug/resolved/dagrun-scheduler-stall.md,
# STATE.md's 2026-08-16 entry, and
# .planning/debug/docker-desktop-wsl2-vm-restart.md) share one downstream
# symptom: a Docker Desktop/WSL2-level VM restart (most likely triggered by
# Windows entering sleep/modern-standby during a keyboard/mouse-idle-but-
# background-compute-active session — see the debug file's Resolution for
# the full research trail) breaks every kind node's `/mnt/dags` bind mount.
# Docker falls back to an empty, read-only tmpfs instead of re-attaching the
# real host directory. The DAG processor then discovers zero DAG files,
# `DagModel.is_stale` never clears, and the scheduler silently excludes every
# DagRun from consideration cluster-wide — with no exception logged anywhere.
#
# This script detects that exact tmpfs-fallback state on an ALREADY-RUNNING
# cluster's kind node containers (bypassing Kubernetes entirely, the same way
# the two prior incidents diagnosed it) and self-heals by `docker restart`ing
# only the affected node container(s) — the one fix Docker/Linux actually
# supports for a bind mount that failed to reattach (a running container's
# bind mount cannot be live-remounted from inside or outside it).
#
# DOCKER/CLUSTER_NAME are overridable, mirroring doctor.sh's KIND=/HELM=
# pattern, so a test can point this at a fake `docker` without touching the
# real cluster (see tests/policy/test_doctor_live_detects_mount_state.py).
# DOCTOR_LIVE_REPAIR=false runs detection only, no `docker restart` — the
# report-only mode `make doctor-live-check` uses.

set -uo pipefail   # NOT -e: every node must be checked even if an earlier one fails.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# shellcheck source=/dev/null
source "${repo_root}/helm/versions.env"

DOCKER="${DOCKER:-docker}"
DOCTOR_LIVE_REPAIR="${DOCTOR_LIVE_REPAIR:-true}"

# Same three roles kind/cluster.yaml declares, same node-name convention kind
# itself uses (`${CLUSTER_NAME}-${role}`, second/third worker gets no suffix
# vs "-worker2" per kind's own naming — verified against the live cluster).
NODES=("${CLUSTER_NAME}-control-plane" "${CLUSTER_NAME}-worker" "${CLUSTER_NAME}-worker2")

failures=0
repaired=0

echo "==> make doctor-live: checking /mnt/dags on every running kind node"

# classify_mount_output <mount-output-text>
# Echoes one of: healthy | broken | unknown
# Isolated as its own function (not inlined) so a test can source this file
# and call it directly with canned text — the same reason doctor.sh's
# check_* functions are named and self-contained rather than one long script.
classify_mount_output() {
  local mount_text="$1"
  local dags_line
  dags_line="$(printf '%s\n' "${mount_text}" | grep -E ' on /mnt/dags type ' || true)"
  if [ -z "${dags_line}" ]; then
    echo "unknown"
    return
  fi
  if printf '%s' "${dags_line}" | grep -q 'type tmpfs'; then
    echo "broken"
  elif printf '%s' "${dags_line}" | grep -q 'type ext4'; then
    echo "healthy"
  else
    echo "unknown"
  fi
}

check_node() {
  local container="$1"
  local running mount_out state

  running="$(${DOCKER} ps --filter "name=^${container}\$" --format '{{.Names}}' 2>/dev/null || true)"
  if [ "${running}" != "${container}" ]; then
    echo "DOCTOR-LIVE ADVISORY: ${container} is not a running container — skipped (this script only heals an already-running cluster; use 'make cluster-up' if the cluster itself is down)." >&2
    return
  fi

  mount_out="$(${DOCKER} exec "${container}" mount 2>/dev/null || true)"
  state="$(classify_mount_output "${mount_out}")"

  case "${state}" in
    healthy)
      echo "${container}: healthy (/mnt/dags is a real ext4 bind mount)"
      ;;
    unknown)
      echo "DOCTOR-LIVE ADVISORY: ${container}: could not read /mnt/dags mount state (docker exec failed or unexpected 'mount' output) — inspect manually with 'docker exec ${container} mount | grep dags'." >&2
      ;;
    broken)
      echo "DOCTOR-LIVE FAIL: ${container}: /mnt/dags has fallen back to an empty read-only tmpfs (the DAGs hostPath bind mount did not reattach)." >&2
      failures=$((failures + 1))
      if [ "${DOCTOR_LIVE_REPAIR}" = "true" ]; then
        echo "  repairing: docker restart ${container} ..." >&2
        if ${DOCKER} restart "${container}" >/dev/null 2>&1; then
          # Docker's own bind-mount reattachment happens at container start;
          # give it a moment before re-checking rather than racing it.
          sleep 3
          mount_out="$(${DOCKER} exec "${container}" mount 2>/dev/null || true)"
          state="$(classify_mount_output "${mount_out}")"
          if [ "${state}" = "healthy" ]; then
            echo "  repaired: ${container}'s /mnt/dags is now a real ext4 bind mount." >&2
            failures=$((failures - 1))
            repaired=$((repaired + 1))
          else
            echo "  repair did not clear the condition (post-restart state: ${state}) — a deeper Docker Desktop/WSL2 restart may be required (see the incident's fix_rationale)." >&2
          fi
        else
          echo "  'docker restart ${container}' failed — see output above." >&2
        fi
      else
        echo "  DOCTOR_LIVE_REPAIR=false — detection only, no restart attempted." >&2
      fi
      ;;
  esac
}

for node in "${NODES[@]}"; do
  check_node "${node}"
done

echo ""
if [ "${failures}" -gt 0 ]; then
  echo "doctor-live: ${failures} node(s) still show a broken DAGs mount after this run. If a repair was attempted and did not clear it, the fix from prior incidents is a full 'wsl --shutdown' + Docker Desktop restart from Windows (cannot be done from inside WSL)." >&2
  exit 1
fi

echo "doctor-live: ${repaired} node(s) repaired; all checked nodes now report a healthy /mnt/dags mount (or were skipped as not running)."
