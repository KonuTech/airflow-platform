---
status: partial
phase: 02-kind-cluster-core-infrastructure
source: [02-VERIFICATION.md]
started: 2026-08-12T19:09:10Z
updated: 2026-08-12T19:09:10Z
---

## Current Test

[awaiting human testing]

## Tests

### 1. ADR-0006 supply-chain risk acceptance (T-02-21)
expected: Read `docs/adr/0006-unmaintained-upstream-artifacts.md` in full. Confirm you accept depending on all three named unmaintained upstream artifacts (`pgsty/minio:RELEASE.2026-08-04T00-00-00Z`, `registry.k8s.io/ingress-nginx/controller:v1.15.1`, `quay.io/minio/mc:RELEASE.2024-11-21T17-21-54Z`) and that the named migration triggers for each are events you would actually notice. Specifically confirm acceptance of threat **T-02-21** — an unpatched CVE in the archived `ingress-nginx` controller, rated **high** severity, dispositioned **accept** (not mitigate) on the argument that the cluster is local-only and the ingress is published on loopback only. If you would ever bind the ingress to a non-loopback address, say so now — the alternative is a Gateway API migration, which is a phase of work, not a values change.
result: [pending]

## Summary

total: 1
passed: 0
issues: 0
pending: 1
skipped: 0
blocked: 0

## Gaps
