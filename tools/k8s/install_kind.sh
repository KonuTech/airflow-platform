#!/usr/bin/env bash
#
# Fetch the pinned kind binary into tools/bin/, verifying its published
# SHA-256 checksum BEFORE install.
#
# Threat T-02-01 (Tampering, tools/k8s/install_{kind,helm}.sh): kind creates
# and drives the whole local cluster with the developer's Docker privileges. A
# tampered binary would run silently forever. Verification therefore happens
# before the binary reaches tools/bin/, never after.
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
# Usage:  tools/k8s/install_kind.sh
# Pin:    KIND_VERSION env var, defaulting to the version below. This script
#         also reads KIND_VERSION from helm/versions.env (the single source of
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
versions_env_kind_version="$(grep -E '^KIND_VERSION=' "${versions_env}" | cut -d= -f2-)"

KIND_VERSION="${KIND_VERSION:-${versions_env_kind_version}}"

# The trust anchor, committed to this repository (CR-03 pattern, from
# tools/security/install_gitleaks.sh).
#
# kind's own per-file `.sha256sum` files are fetched from the SAME URL prefix
# as the binary they describe, so whoever can alter one can alter the other.
# Checking a download against a checksum the download's own origin supplied
# detects corruption in transit but not substitution at the source. These
# digests live in git instead, so a substituted artifact fails the build even
# when its accompanying `.sha256sum` file agrees with it.
#
# Honest limit: these values were captured from the published checksums at pin
# time (measured directly against https://kind.sigs.k8s.io/dl/v0.32.0/kind-*
# and cross-checked against each artifact's own .sha256sum file, both in this
# session), so this is trust-on-first-use. It cannot prove the bytes pinned
# here were authentic; it makes any LATER change detectable.
#
# Updating the version: bump KIND_VERSION in helm/versions.env, then replace
# every line below from the new release's kind-<os>-<arch>.sha256sum files. A
# version whose digest is missing is refused rather than installed unverified.
PINNED_SHA256_linux_amd64="50030de23cf40a18505f20426f6a8506bedf13c6e509244bd1fa9463721b0f54"
PINNED_SHA256_linux_arm64="b92cd615e97585de8ddade28ed5cd7feb4248d717c233eea5b03c37298900f5d"
PINNED_SHA256_darwin_amd64="295ac6d0d634c9819c9907df45e3017d1f13166bd13c3404c45e79f7faa47498"
PINNED_SHA256_darwin_arm64="dca67911095a110c2b5c36e26df6cac860c602033e456c0db47be498cdef1ebb"
PINNED_VERSION="0.32.0"

dest_dir="${repo_root}/tools/bin"
dest="${dest_dir}/kind"
stamp="${dest_dir}/.kind.stamp"

case "$(uname -s)" in
  Linux)  os="linux" ;;
  Darwin) os="darwin" ;;
  *)      echo "ERROR: unsupported OS '$(uname -s)' for a pinned kind build." >&2; exit 1 ;;
esac

case "$(uname -m)" in
  x86_64|amd64)  arch="amd64" ;;
  aarch64|arm64) arch="arm64" ;;
  *)             echo "ERROR: unsupported architecture '$(uname -m)'." >&2; exit 1 ;;
esac

binary="kind-${os}-${arch}"
checksum_file="${binary}.sha256sum"
base_url="https://kind.sigs.k8s.io/dl/v${KIND_VERSION}"

# Resolve the in-repo digest for this platform. Refuse rather than fall back to
# the downloaded checksum file: a missing pin means an unpinned version was
# requested, and installing it unverified is the failure this block prevents.
pinned_var="PINNED_SHA256_${os}_${arch}"
pinned="${!pinned_var:-}"
if [ "${KIND_VERSION}" != "${PINNED_VERSION}" ]; then
  echo "ERROR: KIND_VERSION is ${KIND_VERSION} (from helm/versions.env or the" >&2
  echo "environment) but the pinned digests in this script are for" >&2
  echo "${PINNED_VERSION}. Update the PINNED_SHA256_* values from" >&2
  echo "kind-<os>-<arch>.sha256sum at ${base_url}/, then re-run." >&2
  exit 1
fi
if [ -z "${pinned}" ]; then
  echo "ERROR: no pinned SHA-256 for ${os}/${arch}. Refusing to install an" >&2
  echo "unverified kind binary." >&2
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
    echo "kind ${KIND_VERSION} already installed and verified at ${dest}"
    exit 0
  fi
  echo "Reinstalling: ${dest} does not match its recorded verification." >&2
fi

workdir="$(mktemp -d)"
cleanup() { rm -rf "${workdir}"; }
trap cleanup EXIT

echo "Downloading kind ${KIND_VERSION} (${os}/${arch})..."
curl -sSLf --retry 3 -o "${workdir}/${binary}"        "${base_url}/${binary}"
curl -sSLf --retry 3 -o "${workdir}/${checksum_file}" "${base_url}/${checksum_file}"

cd "${workdir}"

# The authoritative check: the bytes received must match the digest committed
# to THIS repository. Written as a `sha256sum -c` file built from the in-repo
# pin, not from the downloaded checksum file, so the origin cannot vouch for
# itself.
echo "${pinned}  ${binary}" > expected.sha256

# kind ships a bare binary — there is no extract stage. Verify before install,
# not before extract.
if ! sha256sum -c expected.sha256; then
  echo "ERROR: SHA-256 mismatch for ${binary}." >&2
  echo "Expected (pinned in tools/k8s/install_kind.sh): ${pinned}" >&2
  echo "Refusing to install." >&2
  exit 1
fi

# Secondary, advisory only: does the release's own .sha256sum file agree with
# the pin? Disagreement means the upstream artifact changed under a published
# version — worth shouting about even though the pin already refused to
# install it. This runs AFTER the authoritative check and can never substitute
# for it.
if ! grep -qE "^${pinned}[[:space:]]+\*?${binary}\$" "${checksum_file}"; then
  echo "WARNING: the release's ${checksum_file} does not list the pinned" >&2
  echo "digest for ${binary}. The pin held, so nothing unverified was" >&2
  echo "installed, but the upstream artifact may have been republished." >&2
  echo "Investigate before bumping." >&2
fi

mkdir -p "${dest_dir}"
install -m 0755 "${binary}" "${dest}"

# Record what was verified so the idempotent path above can re-check the
# binary by digest instead of executing it. The stamp is a cache key, NOT a
# trust anchor: it lives in gitignored tools/bin/, so anyone able to write
# there could write a matching stamp. Out of scope for T-02-01/02, which is
# about the DOWNLOAD — an attacker with local write access to the repo can
# edit the Makefile just as easily. Stated rather than implied.
printf '%s:%s' "${pinned}" "$(sha256sum "${dest}" | cut -d' ' -f1)" > "${stamp}"

echo "Installed kind ${KIND_VERSION} (digest verified against in-repo pin) -> ${dest}"
