#!/usr/bin/env bash
#
# D-10: the fail-closed host preflight. `make cluster-up` depends on this
# target and cannot skip it — Phase 1's discovery was that a check which
# reports without blocking is a check people scroll past during a ten-minute
# build (02-CONTEXT.md D-10).
#
# Runs EVERY check, accumulates every failure, and prints each one with what
# was found, what was required, and the literal command that fixes it —
# Shared Pattern 1 in 02-PATTERNS.md, the `uv-guard` shape generalised to the
# whole host. Never warn-and-continue on a blocking check; exits 1 if any
# check failed, after every check has run.
#
# Every blocking threshold is read from an environment variable with a
# default, so a fault-injection test can move a floor without editing this
# file. Defaults are derived from the documented `docs/wsl/wslconfig.example`
# floor (D-11) — a FLOOR, never exact equality, so a larger machine is never
# punished:
#   DOCTOR_MIN_INOTIFY_WATCHES   default 524288  (fresh WSL distro ships 8192)
#   DOCTOR_MIN_INOTIFY_INSTANCES default 512     (fresh WSL distro ships 128)
#   DOCTOR_MIN_FREE_GB           default 50      (ext4 free space budget)
#   DOCTOR_MIN_CPUS              default 8
#   DOCTOR_MIN_MEM_GB            default 20
#
# kind/helm are checked by delegating to tools/k8s/install_{kind,helm}.sh
# (idempotent — network I/O only on first install or a digest mismatch) and
# then reading the installed binary's own version. KIND/HELM/KUBECTL name the
# path doctor reads that version from and default to the installers' own
# fixed destination; overriding one (mirroring uv-guard's `UV=` override) lets
# a fault-injection test simulate a missing tool without touching the real
# install.
#
# No version literal beyond what this script reads from helm/versions.env.

set -uo pipefail   # NOT -e: every check below must run even if an earlier one failed.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# shellcheck source=/dev/null
source "${repo_root}/helm/versions.env"

DOCTOR_MIN_INOTIFY_WATCHES="${DOCTOR_MIN_INOTIFY_WATCHES:-524288}"
DOCTOR_MIN_INOTIFY_INSTANCES="${DOCTOR_MIN_INOTIFY_INSTANCES:-512}"
DOCTOR_MIN_FREE_GB="${DOCTOR_MIN_FREE_GB:-50}"
DOCTOR_MIN_CPUS="${DOCTOR_MIN_CPUS:-8}"
DOCTOR_MIN_MEM_GB="${DOCTOR_MIN_MEM_GB:-20}"

KIND="${KIND:-${repo_root}/tools/bin/kind}"
HELM="${HELM:-${repo_root}/tools/bin/helm}"
KUBECTL="${KUBECTL:-kubectl}"

failures=0

fail() {
  # fail <what-was-found> <what-was-required> <the-exact-remediation-command>
  echo "DOCTOR FAIL: ${1}" >&2
  echo "  required: ${2}" >&2
  echo "  fix:      ${3}" >&2
  failures=$((failures + 1))
}

advise() {
  echo "DOCTOR ADVISORY: ${1}" >&2
}

echo "==> make doctor: running host preflight"

# -- inotify limits (PITFALLS A1) -------------------------------------------
check_inotify() {
  local watches instances
  watches="$(cat /proc/sys/fs/inotify/max_user_watches 2>/dev/null || echo 0)"
  instances="$(cat /proc/sys/fs/inotify/max_user_instances 2>/dev/null || echo 0)"
  if [ "${watches}" -lt "${DOCTOR_MIN_INOTIFY_WATCHES}" ]; then
    fail "fs.inotify.max_user_watches=${watches}" \
         "at least ${DOCTOR_MIN_INOTIFY_WATCHES} (a fresh WSL distro defaults to 8192)" \
         "echo 'fs.inotify.max_user_watches=1048576' | sudo tee -a /etc/sysctl.d/99-kind.conf && sudo sysctl --system (and confirm [boot] systemd=true in /etc/wsl.conf, then 'wsl --shutdown' from Windows)"
  fi
  if [ "${instances}" -lt "${DOCTOR_MIN_INOTIFY_INSTANCES}" ]; then
    fail "fs.inotify.max_user_instances=${instances}" \
         "at least ${DOCTOR_MIN_INOTIFY_INSTANCES} (a fresh WSL distro defaults to 128)" \
         "echo 'fs.inotify.max_user_instances=8192' | sudo tee -a /etc/sysctl.d/99-kind.conf && sudo sysctl --system (and confirm [boot] systemd=true in /etc/wsl.conf, then 'wsl --shutdown' from Windows)"
  fi
}

# -- free disk on the filesystem holding the repository (PITFALLS A3) ------
check_free_disk() {
  local avail_kb avail_gb
  avail_kb="$(df -Pk "${repo_root}" 2>/dev/null | awk 'NR==2{print $4}')"
  avail_gb=$(( ${avail_kb:-0} / 1024 / 1024 ))
  if [ "${avail_gb}" -lt "${DOCTOR_MIN_FREE_GB}" ]; then
    fail "free disk on the filesystem holding the repository: ${avail_gb}GiB" \
         "at least ${DOCTOR_MIN_FREE_GB}GiB" \
         "reclaim space — WSL2's ext4.vhdx never returns deleted space to Windows on its own; see docs/wsl/wslconfig.example's 'sparseVhd' setting, then 'wsl --shutdown' and compact the VHDX from Windows"
  fi
}

# -- Docker daemon reachable -------------------------------------------------
check_docker() {
  if ! docker info >/dev/null 2>&1; then
    fail "Docker daemon not reachable" \
         "a running Docker daemon" \
         "start Docker Desktop (or 'sudo systemctl start docker' on native Linux) and retry"
  fi
}

# -- cgroup v2 (kind v0.32.0 warns/fails on cgroup v1) -----------------------
check_cgroup_v2() {
  if ! docker info >/dev/null 2>&1; then
    return  # already reported by check_docker; a duplicate message here would mislead
  fi
  local cgroup_version
  cgroup_version="$(docker info --format '{{.CgroupVersion}}' 2>/dev/null || true)"
  if [ "${cgroup_version}" != "2" ]; then
    fail "Docker engine cgroup version: '${cgroup_version:-unknown}' (docker info)" \
         "cgroup v2 — kind v0.32.0's kubelet is unsupported on a v1 engine" \
         "add 'kernelCommandLine = cgroup_no_v1=all' under [wsl2] in %UserProfile%\\.wslconfig on the Windows side, then run 'wsl --shutdown' and restart Docker Desktop"
  fi
}

# -- kubectl within one minor of KUBERNETES_VERSION --------------------------
check_kubectl() {
  if ! command -v "${KUBECTL}" >/dev/null 2>&1; then
    fail "kubectl not found (KUBECTL=${KUBECTL})" \
         "kubectl on PATH, within one minor of Kubernetes ${KUBERNETES_VERSION}" \
         "install kubectl matching Kubernetes ${KUBERNETES_VERSION} — https://kubernetes.io/docs/tasks/tools/"
    return
  fi
  local client_minor required_minor diff
  client_minor="$("${KUBECTL}" version --client -o json 2>/dev/null \
    | python3 -c 'import json,sys
try:
    print(json.load(sys.stdin)["clientVersion"]["minor"])
except Exception:
    print("")' 2>/dev/null | tr -dc '0-9' || true)"
  required_minor="$(echo "${KUBERNETES_VERSION}" | cut -d. -f2)"
  if [ -z "${client_minor}" ]; then
    fail "kubectl client version unreadable (KUBECTL=${KUBECTL})" \
         "'${KUBECTL} version --client -o json' to report a minor version" \
         "reinstall kubectl matching Kubernetes ${KUBERNETES_VERSION} — https://kubernetes.io/docs/tasks/tools/"
    return
  fi
  diff=$(( client_minor - required_minor ))
  diff=${diff#-}
  if [ "${diff}" -gt 1 ]; then
    fail "kubectl client minor '${client_minor}' vs cluster Kubernetes ${KUBERNETES_VERSION} (minor ${required_minor})" \
         "kubectl within one minor of Kubernetes ${KUBERNETES_VERSION} (supported version skew)" \
         "install a kubectl release within one minor of ${KUBERNETES_VERSION} — https://kubernetes.io/releases/version-skew-policy/"
  fi
}

# -- kind / helm at their pinned versions ------------------------------------
check_kind() {
  if ! "${repo_root}/tools/k8s/install_kind.sh"; then
    fail "kind installer failed — see output above" \
         "a successfully installed, pinned kind ${KIND_VERSION}" \
         "tools/k8s/install_kind.sh"
    return
  fi
  local have
  have="$("${KIND}" version 2>/dev/null | awk '{print $2}' | sed 's/^v//' || true)"
  if [ "${have}" != "${KIND_VERSION}" ]; then
    fail "kind version '${have:-none}' (KIND=${KIND})" \
         "kind ${KIND_VERSION}, pinned in helm/versions.env" \
         "tools/k8s/install_kind.sh"
  fi
}

check_helm() {
  if ! "${repo_root}/tools/k8s/install_helm.sh"; then
    fail "helm installer failed — see output above" \
         "a successfully installed, pinned helm ${HELM_VERSION}" \
         "tools/k8s/install_helm.sh"
    return
  fi
  local have
  have="$("${HELM}" version --template '{{.Version}}' 2>/dev/null | sed 's/^v//' || true)"
  if [ "${have}" != "${HELM_VERSION}" ]; then
    fail "helm version '${have:-none}' (HELM=${HELM})" \
         "helm ${HELM_VERSION}, pinned in helm/versions.env" \
         "tools/k8s/install_helm.sh"
  fi
}

# -- host ports 80/443 free, unless held by this project's own cluster ------
check_ports() {
  local port holder
  for port in 80 443; do
    if ss -ltn 2>/dev/null | awk '{print $4}' | grep -qE "[:.]${port}\$"; then
      holder="$(docker ps --format '{{.Names}}\t{{.Ports}}' 2>/dev/null \
        | grep -E "0\.0\.0\.0:${port}->|:::${port}->" | awk '{print $1}' | head -n1 || true)"
      if [ "${holder}" != "${CLUSTER_NAME}-control-plane" ]; then
        fail "host port ${port} already in use${holder:+ (by container '${holder}')}" \
             "port ${port} free, or held by this project's own cluster (${CLUSTER_NAME}-control-plane)" \
             "stop whatever is bound to port ${port} ('docker ps' to find it), or 'make cluster-down' if it is a stale cluster under a different name"
      fi
    fi
  done
}

# -- repository not under /mnt/ (9p penalty; CLAUDE.md forbids it) ----------
check_repo_path() {
  case "${repo_root}" in
    /mnt/*)
      fail "repository path is under /mnt/ (${repo_root})" \
           "the repository on WSL ext4, never a /mnt/* 9p mount" \
           "move the repository to a native WSL path (e.g. /home/<you>/projects/airflow-platform) and re-open it from there"
      ;;
  esac
}

# -- host CPU / memory floor (docs/wsl/wslconfig.example) -------------------
check_host_resources() {
  local cpus mem_kb mem_gb
  cpus="$(nproc 2>/dev/null || echo 0)"
  mem_kb="$(awk '/MemTotal/{print $2}' /proc/meminfo 2>/dev/null || echo 0)"
  mem_gb=$(( ${mem_kb:-0} / 1024 / 1024 ))
  if [ "${cpus}" -lt "${DOCTOR_MIN_CPUS}" ]; then
    fail "host CPU count: ${cpus}" \
         "at least ${DOCTOR_MIN_CPUS} (the documented docs/wsl/wslconfig.example floor)" \
         "use a host with at least ${DOCTOR_MIN_CPUS} CPUs, or lower DOCTOR_MIN_CPUS only if you accept a smaller cluster"
  fi
  if [ "${mem_gb}" -lt "${DOCTOR_MIN_MEM_GB}" ]; then
    fail "host memory: ${mem_gb}GiB" \
         "at least ${DOCTOR_MIN_MEM_GB}GiB (the documented docs/wsl/wslconfig.example floor)" \
         "raise [wsl2] memory in %UserProfile%\\.wslconfig to at least ${DOCTOR_MIN_MEM_GB}GB, then 'wsl --shutdown'"
  fi
}

check_inotify
check_free_disk
check_docker
check_cgroup_v2
check_kubectl
check_kind
check_helm
check_ports
check_repo_path
check_host_resources

# Advisory only: doctor runs INSIDE the WSL VM and cannot see whether
# %UserProfile%\.wslconfig on the Windows side has actually been applied —
# that requires 'wsl --shutdown', which is a deliberate human act (D-11).
advise "docs/wsl/wslconfig.example must be applied and 'wsl --shutdown' run from Windows for any change to it to take effect — this check cannot verify that from inside WSL, only the floors above once it is."

echo ""
if [ "${failures}" -gt 0 ]; then
  echo "doctor: ${failures} check(s) failed. Fix the above before 'make cluster-up'." >&2
  exit 1
fi

echo "doctor: all checks passed."
