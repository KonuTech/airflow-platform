#!/usr/bin/env bash
#
# Fetch the pinned gitleaks binary into tools/bin/, verifying its published
# SHA-256 checksum BEFORE extraction.
#
# Threat T-01-09 (Tampering, gitleaks binary download): the scanner is the
# control that keeps credentials out of a public repository, and it is fetched
# over the network and then executed. A tampered binary would report clean
# forever and nobody would notice. Verification therefore happens before the
# archive is opened, never after, and a mismatch aborts without writing
# anything into tools/bin/.
#
# The binary is gitignored (tools/bin/ in .gitignore) and never committed.
# `make check` must run with no network (ROADMAP criterion 4), which is why
# this download lives in `make ci` / the CI secrets job and not in `check`.
#
# Usage:  tools/security/install_gitleaks.sh
# Pin:    GITLEAKS_VERSION env var, defaulting to the version below. The CI
#         workflow exports the same value from its top-level `env:` block, so
#         the workflow and a developer machine install the identical binary.

set -euo pipefail

GITLEAKS_VERSION="${GITLEAKS_VERSION:-8.30.1}"

# The trust anchor, committed to this repository (CR-03).
#
# The release's own checksums.txt is fetched from the SAME URL prefix as the
# tarball it describes, so whoever can alter one can alter the other. Checking a
# download against a checksum the download's own origin supplied detects
# corruption in transit but not substitution at the source — which is the
# threat T-01-09 actually names. These digests live in git instead, so a
# substituted artifact fails the build even when its accompanying checksums file
# agrees with it.
#
# Honest limit: these values were captured from the published checksums at pin
# time, so this is trust-on-first-use. It cannot prove the bytes pinned here
# were authentic; it makes any LATER change detectable, which is the property
# that matters for a control that must keep working unattended.
#
# Updating the version: bump GITLEAKS_VERSION, then replace every line below
# from `gitleaks_<version>_checksums.txt`. A version whose digest is missing is
# refused rather than installed unverified.
PINNED_SHA256_linux_x64="551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb"
PINNED_SHA256_linux_arm64="e4a487ee7ccd7d3a7f7ec08657610aa3606637dab924210b3aee62570fb4b080"
PINNED_SHA256_darwin_x64="dfe101a4db2255fc85120ac7f3d25e4342c3c20cf749f2c20a18081af1952709"
PINNED_SHA256_darwin_arm64="b40ab0ae55c505963e365f271a8d3846efbc170aa17f2607f13df610a9aeb6a5"
PINNED_VERSION="8.30.1"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
dest_dir="${repo_root}/tools/bin"
dest="${dest_dir}/gitleaks"
stamp="${dest_dir}/.gitleaks.stamp"

case "$(uname -s)" in
  Linux)  os="linux" ;;
  Darwin) os="darwin" ;;
  *)      echo "ERROR: unsupported OS '$(uname -s)' for a pinned gitleaks build." >&2; exit 1 ;;
esac

case "$(uname -m)" in
  x86_64|amd64)  arch="x64" ;;
  aarch64|arm64) arch="arm64" ;;
  *)             echo "ERROR: unsupported architecture '$(uname -m)'." >&2; exit 1 ;;
esac

tarball="gitleaks_${GITLEAKS_VERSION}_${os}_${arch}.tar.gz"
checksums="gitleaks_${GITLEAKS_VERSION}_checksums.txt"
base_url="https://github.com/gitleaks/gitleaks/releases/download/v${GITLEAKS_VERSION}"

# Resolve the in-repo digest for this platform. Refuse rather than fall back to
# the downloaded checksums file: a missing pin means an unpinned version was
# requested, and installing it unverified is the failure this whole block exists
# to prevent.
pinned_var="PINNED_SHA256_${os}_${arch}"
pinned="${!pinned_var:-}"
if [ "${GITLEAKS_VERSION}" != "${PINNED_VERSION}" ]; then
  echo "ERROR: GITLEAKS_VERSION is ${GITLEAKS_VERSION} but the pinned digests in this" >&2
  echo "script are for ${PINNED_VERSION}. Update the PINNED_SHA256_* values from" >&2
  echo "gitleaks_${GITLEAKS_VERSION}_checksums.txt, then re-run." >&2
  exit 1
fi
if [ -z "${pinned}" ]; then
  echo "ERROR: no pinned SHA-256 for ${os}/${arch}. Refusing to install an" >&2
  echo "unverified scanner binary." >&2
  exit 1
fi

# Idempotent: `make gitleaks` may call this on every run. The check is a digest
# comparison against the in-repo pin, NOT `"${dest}" version` — that executed
# whatever binary happened to sit in the gitignored tools/bin/, so a once-planted
# binary was trusted forever and never re-verified. Nothing here runs the binary.
if [ -f "${dest}" ] && [ -f "${stamp}" ]; then
  installed_digest="$(sha256sum "${dest}" | cut -d' ' -f1)"
  if [ "$(cat "${stamp}")" = "${pinned}:${installed_digest}" ]; then
    echo "gitleaks ${GITLEAKS_VERSION} already installed and verified at ${dest}"
    exit 0
  fi
  echo "Reinstalling: ${dest} does not match its recorded verification." >&2
fi

workdir="$(mktemp -d)"
cleanup() { rm -rf "${workdir}"; }
trap cleanup EXIT

echo "Downloading gitleaks ${GITLEAKS_VERSION} (${os}/${arch})..."
curl -sSLf --retry 3 -o "${workdir}/${tarball}"   "${base_url}/${tarball}"
curl -sSLf --retry 3 -o "${workdir}/${checksums}" "${base_url}/${checksums}"

cd "${workdir}"

# The authoritative check: the bytes received must match the digest committed to
# THIS repository. Written as a `sha256sum -c` file built from the in-repo pin,
# not from the downloaded checksums, so the origin cannot vouch for itself.
echo "${pinned}  ${tarball}" > expected.sha256

# Fail closed: nothing is extracted and nothing reaches tools/bin/ unless the
# pinned digest matches the bytes actually received.
if ! sha256sum -c expected.sha256; then
  echo "ERROR: SHA-256 mismatch for ${tarball}." >&2
  echo "Expected (pinned in tools/security/install_gitleaks.sh): ${pinned}" >&2
  echo "Refusing to extract or install." >&2
  exit 1
fi

# Secondary, advisory only: does the release's own checksums file agree with the
# pin? Disagreement means the upstream artifact changed under a published
# version — worth shouting about even though the pin already refused to install
# it. This runs AFTER the authoritative check and can never substitute for it.
if ! grep -qE "^${pinned}[[:space:]]+\*?${tarball}\$" "${checksums}"; then
  echo "WARNING: the release's checksums.txt does not list the pinned digest for" >&2
  echo "${tarball}. The pin held, so nothing unverified was installed, but the" >&2
  echo "upstream artifact may have been republished. Investigate before bumping." >&2
fi

tar -xzf "${tarball}" gitleaks
mkdir -p "${dest_dir}"
install -m 0755 gitleaks "${dest}"

# Record what was verified so the idempotent path above can re-check the binary
# by digest instead of executing it. The stamp is a cache key, NOT a trust
# anchor: it lives in gitignored tools/bin/, so anyone able to write there could
# write a matching stamp. That is out of scope for T-01-09, which is about the
# DOWNLOAD — an attacker with local write access to the repo can edit the
# Makefile just as easily. Stated rather than implied.
printf '%s:%s' "${pinned}" "$(sha256sum "${dest}" | cut -d' ' -f1)" > "${stamp}"

echo "Installed gitleaks ${GITLEAKS_VERSION} (digest verified against in-repo pin) -> ${dest}"
