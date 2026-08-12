#!/usr/bin/env bash
#
# Fetch the pinned helm binary into tools/bin/, verifying its published
# SHA-256 checksum BEFORE extraction.
#
# Threat T-02-01 (Tampering, tools/k8s/install_{kind,helm}.sh): helm installs
# every chart in this platform with the developer's Kubernetes credentials. A
# tampered binary would run silently forever. Verification therefore happens
# before the archive is opened, never after.
#
# T-02-02 (Elevation of Privilege): idempotence is a digest comparison against
# a `.stamp` file, never `"${dest}" version` — a substituted binary in the
# gitignored tools/bin/ is re-verified by digest on every run, not merely
# trusted because something is already there.
#
# The binary is gitignored (tools/bin/ in .gitignore) and never committed.
# `make cluster-up` invokes this installer on every run, so idempotence matters
# more here than for `install_gitleaks.sh`: a re-run against an already-correct
# install must do no network I/O.
#
# Usage:  tools/k8s/install_helm.sh
# Pin:    HELM_VERSION env var, defaulting to the version below. This script
#         also reads HELM_VERSION from helm/versions.env (the single source of
#         truth, per Phase 2 convention) and refuses if the two disagree.
#         tests/policy/test_pinned_tool_versions_agree.py enforces that
#         helm/versions.env and this script's PINNED_VERSION never drift apart.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
versions_env="${repo_root}/helm/versions.env"

# The single source of truth for the required version (Phase 2 convention,
# analogous to Phase 1's UV_REQUIRED_VERSION). Read before the env var default
# so a `helm/versions.env` bump is what makes the installer request a new
# version, not the other way around.
versions_env_helm_version="$(grep -E '^HELM_VERSION=' "${versions_env}" | cut -d= -f2-)"

HELM_VERSION="${HELM_VERSION:-${versions_env_helm_version}}"

# The trust anchor, committed to this repository (CR-03 pattern, from
# tools/security/install_gitleaks.sh).
#
# helm's own per-file `.tar.gz.sha256sum` files are fetched from the SAME URL
# prefix as the tarball they describe, so whoever can alter one can alter the
# other. Checking a download against a checksum the download's own origin
# supplied detects corruption in transit but not substitution at the source.
# These digests live in git instead, so a substituted artifact fails the build
# even when its accompanying `.sha256sum` file agrees with it.
#
# Honest limit: these values were captured from the published checksums at pin
# time (measured directly against
# https://get.helm.sh/helm-v4.2.3-<os>-<arch>.tar.gz and cross-checked against
# each artifact's own .sha256sum file, both in this session), so this is
# trust-on-first-use. It cannot prove the bytes pinned here were authentic; it
# makes any LATER change detectable.
#
# Updating the version: bump HELM_VERSION in helm/versions.env, then replace
# every line below from the new release's helm-v<version>-<os>-<arch>.tar.gz.sha256sum
# files. A version whose digest is missing is refused rather than installed
# unverified.
PINNED_SHA256_linux_amd64="e9b88b4ee95b18c706839c28d3a0220e5bc470e9cd9262410c90793c45ff8b7c"
PINNED_SHA256_linux_arm64="21abd9354d39b2cd79a8d76be6912cd137a983cbf997193503fb8a6a6e2f2785"
PINNED_SHA256_darwin_amd64="ff3ac86755a45f3422473bc1200776aac0fe04c5766abe6ca66699f7b564b23b"
PINNED_SHA256_darwin_arm64="048ecf5ad3160f83d918f9fe945238d2132b079640f7b106175331c25f242c64"
PINNED_VERSION="4.2.3"

dest_dir="${repo_root}/tools/bin"
dest="${dest_dir}/helm"
stamp="${dest_dir}/.helm.stamp"

case "$(uname -s)" in
  Linux)  os="linux" ;;
  Darwin) os="darwin" ;;
  *)      echo "ERROR: unsupported OS '$(uname -s)' for a pinned helm build." >&2; exit 1 ;;
esac

case "$(uname -m)" in
  x86_64|amd64)  arch="amd64" ;;
  aarch64|arm64) arch="arm64" ;;
  *)             echo "ERROR: unsupported architecture '$(uname -m)'." >&2; exit 1 ;;
esac

tarball="helm-v${HELM_VERSION}-${os}-${arch}.tar.gz"
checksum_file="${tarball}.sha256sum"
base_url="https://get.helm.sh"

# Resolve the in-repo digest for this platform. Refuse rather than fall back to
# the downloaded checksum file: a missing pin means an unpinned version was
# requested, and installing it unverified is the failure this block prevents.
pinned_var="PINNED_SHA256_${os}_${arch}"
pinned="${!pinned_var:-}"
if [ "${HELM_VERSION}" != "${PINNED_VERSION}" ]; then
  echo "ERROR: HELM_VERSION is ${HELM_VERSION} (from helm/versions.env or the" >&2
  echo "environment) but the pinned digests in this script are for" >&2
  echo "${PINNED_VERSION}. Update the PINNED_SHA256_* values from" >&2
  echo "helm-v${HELM_VERSION}-<os>-<arch>.tar.gz.sha256sum at ${base_url}/, then" >&2
  echo "re-run." >&2
  exit 1
fi
if [ -z "${pinned}" ]; then
  echo "ERROR: no pinned SHA-256 for ${os}/${arch}. Refusing to install an" >&2
  echo "unverified helm binary." >&2
  exit 1
fi

# Idempotent: `make cluster-up` calls this on every run. The check is a digest
# comparison against the in-repo pin, NOT `"${dest}" version` — that would
# execute whatever binary happened to sit in the gitignored tools/bin/, so a
# once-planted binary would be trusted forever and never re-verified. Nothing
# here runs the binary before it is verified.
if [ -f "${dest}" ] && [ -f "${stamp}" ]; then
  installed_digest="$(sha256sum "${dest}" | cut -d' ' -f1)"
  if [ "$(cat "${stamp}")" = "${pinned}:${installed_digest}" ]; then
    echo "helm ${HELM_VERSION} already installed and verified at ${dest}"
    exit 0
  fi
  echo "Reinstalling: ${dest} does not match its recorded verification." >&2
fi

workdir="$(mktemp -d)"
cleanup() { rm -rf "${workdir}"; }
trap cleanup EXIT

echo "Downloading helm ${HELM_VERSION} (${os}/${arch})..."
curl -sSLf --retry 3 -o "${workdir}/${tarball}"        "${base_url}/${tarball}"
curl -sSLf --retry 3 -o "${workdir}/${checksum_file}"  "${base_url}/${checksum_file}"

cd "${workdir}"

# The authoritative check: the bytes received must match the digest committed
# to THIS repository. Written as a `sha256sum -c` file built from the in-repo
# pin, not from the downloaded checksum file, so the origin cannot vouch for
# itself.
echo "${pinned}  ${tarball}" > expected.sha256

# helm ships a tarball — verify BEFORE extract, never after.
if ! sha256sum -c expected.sha256; then
  echo "ERROR: SHA-256 mismatch for ${tarball}." >&2
  echo "Expected (pinned in tools/k8s/install_helm.sh): ${pinned}" >&2
  echo "Refusing to extract or install." >&2
  exit 1
fi

# Secondary, advisory only: does the release's own .sha256sum file agree with
# the pin? Disagreement means the upstream artifact changed under a published
# version — worth shouting about even though the pin already refused to
# install it. This runs AFTER the authoritative check and can never substitute
# for it.
if ! grep -qE "^${pinned}[[:space:]]+\*?${tarball}\$" "${checksum_file}"; then
  echo "WARNING: the release's ${checksum_file} does not list the pinned" >&2
  echo "digest for ${tarball}. The pin held, so nothing unverified was" >&2
  echo "installed, but the upstream artifact may have been republished." >&2
  echo "Investigate before bumping." >&2
fi

tar -xzf "${tarball}" "${os}-${arch}/helm"
mkdir -p "${dest_dir}"
install -m 0755 "${os}-${arch}/helm" "${dest}"

# Record what was verified so the idempotent path above can re-check the
# binary by digest instead of executing it. The stamp is a cache key, NOT a
# trust anchor: it lives in gitignored tools/bin/, so anyone able to write
# there could write a matching stamp. Out of scope for T-02-01/02, which is
# about the DOWNLOAD — an attacker with local write access to the repo can
# edit the Makefile just as easily. Stated rather than implied.
printf '%s:%s' "${pinned}" "$(sha256sum "${dest}" | cut -d' ' -f1)" > "${stamp}"

echo "Installed helm ${HELM_VERSION} (digest verified against in-repo pin) -> ${dest}"
