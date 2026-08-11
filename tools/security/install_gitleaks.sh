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

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
dest_dir="${repo_root}/tools/bin"
dest="${dest_dir}/gitleaks"

# Idempotent: `make gitleaks` may call this on every run. If the pinned version
# is already in place there is nothing to download and nothing to verify.
if [ -x "${dest}" ] && "${dest}" version 2>/dev/null | grep -qF "${GITLEAKS_VERSION}"; then
  echo "gitleaks ${GITLEAKS_VERSION} already installed at ${dest}"
  exit 0
fi

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

workdir="$(mktemp -d)"
cleanup() { rm -rf "${workdir}"; }
trap cleanup EXIT

echo "Downloading gitleaks ${GITLEAKS_VERSION} (${os}/${arch})..."
curl -sSLf --retry 3 -o "${workdir}/${tarball}"   "${base_url}/${tarball}"
curl -sSLf --retry 3 -o "${workdir}/${checksums}" "${base_url}/${checksums}"

# Select exactly the one line naming our artifact. A checksums file that does
# not mention it (wrong version, renamed asset, truncated download) must fail
# here rather than silently verifying nothing: `sha256sum -c` over an empty
# file exits 1, and the count check below makes the reason readable.
cd "${workdir}"
grep -E "[[:space:]]\*?${tarball}\$" "${checksums}" > expected.sha256 || true
line_count="$(wc -l < expected.sha256)"
if [ "${line_count}" -ne 1 ]; then
  echo "ERROR: expected exactly one checksum line for ${tarball}, found ${line_count}." >&2
  echo "Refusing to extract an artifact whose checksum is not published." >&2
  exit 1
fi

# Fail closed: nothing is extracted and nothing reaches tools/bin/ unless the
# published digest matches the bytes actually received.
if ! sha256sum -c expected.sha256; then
  echo "ERROR: SHA-256 mismatch for ${tarball}. Refusing to extract or install." >&2
  exit 1
fi

tar -xzf "${tarball}" gitleaks
mkdir -p "${dest_dir}"
install -m 0755 gitleaks "${dest}"

echo "Installed $("${dest}" version) -> ${dest}"
