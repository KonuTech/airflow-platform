---
status: testing
phase: 01-repository-toolchain-ci-skeleton
source: [01-VERIFICATION.md]
started: 2026-08-11T21:10:00Z
updated: 2026-08-11T21:10:00Z
---

## Current Test

number: 1
name: Decide whether the gitleaks installer's trust anchor is acceptable (CR-03)
expected: |
  Either pin the expected SHA-256 in the repository so the digest is independent
  of the download origin, or accept the risk in writing.
awaiting: user response

## Tests

### 1. gitleaks installer trust anchor (CR-03)

expected: A digest that is not fetched from the same origin as the artifact it validates — or a written risk acceptance.
result: [pending]

**What was observed.** `tools/security/install_gitleaks.sh` downloads
`gitleaks_<version>_linux_x64.tar.gz` and `gitleaks_<version>_checksums.txt` from
the same `base_url`. An adversary able to alter one can alter the other, so the
digest defends against corruption in transit but not against tampering at the
source — which is the threat the file's own header (T-01-09) names. Separately,
line 32 *executes* whatever binary already sits in the gitignored `tools/bin/`
in order to read its version, before anything is verified, so a once-planted
binary is never re-checked.

**Why this is a decision, not a defect.** The arrangement is common and
defensible, and it does not undermine SEC-02, SEC-10 or SEC-11 — all three were
proven by executing the scanner, not by trusting its provenance. Fixing it means
committing the per-platform SHA-256 as the trust anchor and verifying before
executing an existing binary. That touches the download path every CI run
depends on, so it is a deliberate change rather than a drive-by.

Recorded in `.planning/WINDOWS.md`.

### 2. Filename of `tests/policy/test_secret_scan_depth.py` (optional, blocks nothing)

expected: Either split the file, or rename it to something like `test_supply_chain_guards.py`.
result: [pending]

The file now holds three unrelated guards: the original full-history scan-depth
assertions, the installer verify-before-extract ordering guard (CR-02), and the
Makefile lockfile guard (CR-01). Both regression-marked tests live here, so the
filename actively misleads about where the project's regression inventory sits.

Cosmetic, with no correctness impact. Raised only because this phase is the one
that sets the repository's conventions, and a misleading filename is cheapest to
fix before ten more phases accrete around it.

## Summary

total: 2
passed: 0
issues: 0
pending: 2
skipped: 0
blocked: 0

## Gaps

None. The phase goal is achieved: 5/5 roadmap success criteria verified and
12/12 requirements satisfied. Neither item above blocks phases 2–11 — one is a
risk-acceptance decision, the other is cosmetic.
