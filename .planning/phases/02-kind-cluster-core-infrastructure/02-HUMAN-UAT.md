---
status: resolved
phase: 02-kind-cluster-core-infrastructure
source: [02-VERIFICATION.md]
started: 2026-08-12T19:09:10Z
updated: 2026-08-12T20:05:00Z
---

## Current Test

[all tests complete]

## Tests

### 1. ADR-0006 supply-chain risk acceptance (T-02-21)
expected: Read `docs/adr/0006-unmaintained-upstream-artifacts.md` in full. Confirm you accept depending on all three named unmaintained upstream artifacts (`pgsty/minio:RELEASE.2026-08-04T00-00-00Z`, `registry.k8s.io/ingress-nginx/controller:v1.15.1`, `quay.io/minio/mc:RELEASE.2024-11-21T17-21-54Z`) and that the named migration triggers for each are events you would actually notice. Specifically confirm acceptance of threat **T-02-21** — an unpatched CVE in the archived `ingress-nginx` controller, rated **high** severity, dispositioned **accept** (not mitigate) on the argument that the cluster is local-only and the ingress is published on loopback only. If you would ever bind the ingress to a non-loopback address, say so now — the alternative is a Gateway API migration, which is a phase of work, not a values change.
result: Accepted, 2026-08-12. Human confirmed via interactive Q&A: (1) accepts T-02-21 as loopback-only exposure with no realistic attack path today; (2) does not currently plan to expose the platform beyond the local machine — if that changes, the ADR's Gateway API migration trigger applies; (3) accepts the other two named artifacts (`pgsty/minio`, `quay.io/minio/mc`) and their documented migration triggers. Confirmed understanding that accepting this does not compromise the platform's production-like architecture goal — the risk is about container-image provenance, not the Kubernetes/Helm/S3 patterns being built.

## Summary

total: 1
passed: 1
issues: 0
pending: 0
skipped: 0
blocked: 0

## Gaps
