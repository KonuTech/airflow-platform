#!/usr/bin/env bash
#
# Fetch the pinned kubeconform binary into tools/bin/, verifying its published
# SHA-256 checksum BEFORE extraction.
#
# Threat T-02-01 (Tampering, tools/k8s/install_*.sh): kubeconform is the
# offline manifest-validation gate (CICD-07). A tampered binary could report
# clean forever. Verification therefore happens before the archive is opened,
# never after.
#
# Deviation note (plan 02-01, Rule 2 — missing critical functionality): the
# plan's action text for Task 1 only names `install_kind.sh` and
# `install_helm.sh` in the files list, but explicitly requires
# `kubeconform_readings()` to compare `helm/versions.env` against "the
# corresponding installer's PINNED_VERSION" and the acceptance criteria
# require `test_pinned_tool_versions_agree.py` to collect and pass a
# kubeconform case now. Without this installer, `kubeconform_readings()` would
# have only one real source (`helm/versions.env`) and
# `test_every_source_is_load_bearing` would fail outright. This script is
# added now so that requirement is genuinely satisfied rather than only
# nominally registered; plans 02-07/02-08 are its first functional consumers
# (`make manifests`), per 02-RESEARCH.md and the `manifests` pytest marker.
#
# The binary is gitignored (tools/bin/ in .gitignore) and never committed.
#
# Usage:  tools/k8s/install_kubeconform.sh
# Pin:    KUBECONFORM_VERSION env var, defaulting to the version below. This
#         script also reads KUBECONFORM_VERSION from helm/versions.env (the
#         single source of truth, per Phase 2 convention) and refuses if the
#         two disagree. tests/policy/test_pinned_tool_versions_agree.py
#         enforces that helm/versions.env and this script's PINNED_VERSION
#         never drift apart.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
versions_env="${repo_root}/helm/versions.env"

# The single source of truth for the required version (Phase 2 convention,
# analogous to Phase 1's UV_REQUIRED_VERSION). Read before the env var default
# so a `helm/versions.env` bump is what makes the installer request a new
# version, not the other way around.
versions_env_kubeconform_version="$(grep -E '^KUBECONFORM_VERSION=' "${versions_env}" | cut -d= -f2-)"

KUBECONFORM_VERSION="${KUBECONFORM_VERSION:-${versions_env_kubeconform_version}}"

# The trust anchor, committed to this repository (CR-03 pattern, from
# tools/security/install_gitleaks.sh).
#
# kubeconform publishes a single CHECKSUMS file per release, fetched from the
# SAME URL prefix as the tarball it describes, so whoever can alter one can
# alter the other. Checking a download against a checksum the download's own
# origin supplied detects corruption in transit but not substitution at the
# source. These digests live in git instead, so a substituted artifact fails
# the build even when its accompanying CHECKSUMS file agrees with it.
#
# Honest limit: these values were captured from the published CHECKSUMS at pin
# time (measured directly against
# https://github.com/yannh/kubeconform/releases/download/v0.8.0/kubeconform-<os>-<arch>.tar.gz
# and cross-checked against the release's own CHECKSUMS file, both in this
# session), so this is trust-on-first-use. It cannot prove the bytes pinned
# here were authentic; it makes any LATER change detectable.
#
# Updating the version: bump KUBECONFORM_VERSION in helm/versions.env, then
# replace every line below from the new release's CHECKSUMS file. A version
# whose digest is missing is refused rather than installed unverified.
PINNED_SHA256_linux_amd64="9bc2bffbf71f261128533edaf912153948b7ff238f9a531ae6d34466ec287883"
PINNED_SHA256_linux_arm64="1f53fc8e81258197a35e8603054162a5af1de8c5af13746c71ab680d9534ed87"
PINNED_SHA256_darwin_amd64="71dbc87ac9f24099a62b93570e65aa06312ba6ac8aea63b7f86e9d999edf5a92"
PINNED_SHA256_darwin_arm64="f84f4dfbebf4a6b0b230385fa065a39ea35e02608c2b50d025dcf64775a69d67"
PINNED_VERSION="0.8.0"

dest_dir="${repo_root}/tools/bin"
dest="${dest_dir}/kubeconform"
stamp="${dest_dir}/.kubeconform.stamp"

case "$(uname -s)" in
  Linux)  os="linux" ;;
  Darwin) os="darwin" ;;
  *)      echo "ERROR: unsupported OS '$(uname -s)' for a pinned kubeconform build." >&2; exit 1 ;;
esac

case "$(uname -m)" in
  x86_64|amd64)  arch="amd64" ;;
  aarch64|arm64) arch="arm64" ;;
  *)             echo "ERROR: unsupported architecture '$(uname -m)'." >&2; exit 1 ;;
esac

tarball="kubeconform-${os}-${arch}.tar.gz"
checksums="CHECKSUMS"
base_url="https://github.com/yannh/kubeconform/releases/download/v${KUBECONFORM_VERSION}"

# Resolve the in-repo digest for this platform. Refuse rather than fall back to
# the downloaded checksums file: a missing pin means an unpinned version was
# requested, and installing it unverified is the failure this block prevents.
pinned_var="PINNED_SHA256_${os}_${arch}"
pinned="${!pinned_var:-}"
if [ "${KUBECONFORM_VERSION}" != "${PINNED_VERSION}" ]; then
  echo "ERROR: KUBECONFORM_VERSION is ${KUBECONFORM_VERSION} (from" >&2
  echo "helm/versions.env or the environment) but the pinned digests in this" >&2
  echo "script are for ${PINNED_VERSION}. Update the PINNED_SHA256_* values" >&2
  echo "from CHECKSUMS at ${base_url}/, then re-run." >&2
  exit 1
fi
if [ -z "${pinned}" ]; then
  echo "ERROR: no pinned SHA-256 for ${os}/${arch}. Refusing to install an" >&2
  echo "unverified kubeconform binary." >&2
  exit 1
fi

# Idempotent: the check is a digest comparison against the in-repo pin, NOT
# `"${dest}" -v` — that would execute whatever binary happened to sit in the
# gitignored tools/bin/, so a once-planted binary would be trusted forever and
# never re-verified. Nothing here runs the binary before it is verified.
if [ -f "${dest}" ] && [ -f "${stamp}" ]; then
  installed_digest="$(sha256sum "${dest}" | cut -d' ' -f1)"
  if [ "$(cat "${stamp}")" = "${pinned}:${installed_digest}" ]; then
    echo "kubeconform ${KUBECONFORM_VERSION} already installed and verified at ${dest}"
    exit 0
  fi
  echo "Reinstalling: ${dest} does not match its recorded verification." >&2
fi

workdir="$(mktemp -d)"
cleanup() { rm -rf "${workdir}"; }
trap cleanup EXIT

echo "Downloading kubeconform ${KUBECONFORM_VERSION} (${os}/${arch})..."
curl -sSLf --retry 3 -o "${workdir}/${tarball}"    "${base_url}/${tarball}"
curl -sSLf --retry 3 -o "${workdir}/${checksums}"  "${base_url}/${checksums}"

cd "${workdir}"

# The authoritative check: the bytes received must match the digest committed
# to THIS repository. Written as a `sha256sum -c` file built from the in-repo
# pin, not from the downloaded checksums file, so the origin cannot vouch for
# itself.
echo "${pinned}  ${tarball}" > expected.sha256

# kubeconform ships a tarball — verify BEFORE extract, never after.
if ! sha256sum -c expected.sha256; then
  echo "ERROR: SHA-256 mismatch for ${tarball}." >&2
  echo "Expected (pinned in tools/k8s/install_kubeconform.sh): ${pinned}" >&2
  echo "Refusing to extract or install." >&2
  exit 1
fi

# Secondary, advisory only: does the release's own CHECKSUMS file agree with
# the pin? Disagreement means the upstream artifact changed under a published
# version — worth shouting about even though the pin already refused to
# install it. This runs AFTER the authoritative check and can never substitute
# for it.
if ! grep -qE "^${pinned}[[:space:]]+\*?${tarball}\$" "${checksums}"; then
  echo "WARNING: the release's ${checksums} does not list the pinned digest" >&2
  echo "for ${tarball}. The pin held, so nothing unverified was installed," >&2
  echo "but the upstream artifact may have been republished. Investigate" >&2
  echo "before bumping." >&2
fi

tar -xzf "${tarball}" kubeconform
mkdir -p "${dest_dir}"
install -m 0755 kubeconform "${dest}"

# Record what was verified so the idempotent path above can re-check the
# binary by digest instead of executing it. The stamp is a cache key, NOT a
# trust anchor: it lives in gitignored tools/bin/, so anyone able to write
# there could write a matching stamp. Out of scope for T-02-01, which is about
# the DOWNLOAD — an attacker with local write access to the repo can edit the
# Makefile just as easily. Stated rather than implied.
printf '%s:%s' "${pinned}" "$(sha256sum "${dest}" | cut -d' ' -f1)" > "${stamp}"

echo "Installed kubeconform ${KUBECONFORM_VERSION} (digest verified against in-repo pin) -> ${dest}"
