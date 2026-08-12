---
status: accepted
date: 2026-08-12
---

# ADR-0006: This platform knowingly runs three unmaintained upstream artifacts, each with a named migration target and a dated trigger

## Context and Problem Statement

README §5 names MinIO as the data lake and README §3.1 mandates kind plus an ingress path
into it. Phase 2's Package Legitimacy Audit (`02-RESEARCH.md` § Package Legitimacy Audit)
found that satisfying those two requirements today means depending on **three** artifacts
whose upstreams no longer publish patches — not one, which is what `docs/adr/README.md`'s
prospective-records table originally scoped this record to.

**`pgsty/minio:RELEASE.2026-08-04T00-00-00Z`.** The `minio/minio` GitHub repository was
marked "no longer maintained" on 2026-02-12 and archived on 2026-04-25. The last community
image on Docker Hub is `RELEASE.2025-09-07T16-13-09Z`, pushed 2025-09-07; `minio/minio:latest`
has not moved since. The CE web console was removed in May 2025. `pgsty/minio` is a
single-maintainer community fork (Pigsty project, created 2025-10-25) that rebuilds the last
known-good MinIO source, restores the admin console and applies CVE patches, including
CVE-2025-62506. It still works today. It will not receive an official CVE patch again.

**`registry.k8s.io/ingress-nginx/controller:v1.15.1`.** The `kubernetes/ingress-nginx`
project was archived read-only on 2026-03-24. Its intended successor, InGate, was also
retired. Chart `4.15.1` / controller `1.15.1` is the final release; it supports Kubernetes
1.31–1.35, which covers this platform's pinned 1.35.5 today. This finding is new since
`.planning/research/STACK.md` was written — STACK.md never flagged ingress-nginx as an
unmaintained dependency, and D-05's ingress design was chosen before the archival date. It
still works today. It will not receive an official CVE patch again.

**`quay.io/minio/mc:RELEASE.2024-11-21T17-21-54Z`.** The MinIO chart's default bucket/policy
bootstrap image, published by MinIO Inc. before the archival above — a genuine community
build, not a commercial hotfix, but roughly twenty months stale at the time of this record.
It still works today. It will not receive an official CVE patch again.

All three are demonstrably functional as of this writing (`02-RESEARCH.md` § Summary: all
five charts, including MinIO and ingress-nginx, rendered and installed cleanly on a live
kind cluster in this phase). The question this record answers is not whether to use them —
there is no maintained alternative that satisfies README §5 and §3.1 today without a larger
architectural change — but what happens when one of them stops working.

## Considered Options

* **A — Keep official `minio/minio:RELEASE.2025-09-07T16-13-09Z` instead of the `pgsty`
  fork.** Safest provenance (published by MinIO Inc. before archival), but no console and no
  CVE patches after September 2025 — strictly worse security posture than the fork for no
  provenance gain, since the fork rebuilds from the same last-known-good source.
* **B — Adopt SeaweedFS now, instead of MinIO.** Apache-2.0, actively developed, integrated
  S3 API — the strongest genuinely-maintained alternative. Rejected for this phase because
  README §5 names MinIO explicitly and a mid-phase storage swap is exactly the kind of
  architectural change Rule 4 reserves for a human decision, not a default.
* **C — Adopt Garage now, instead of MinIO.** Lightweight and well-suited to a small cluster,
  but its multipart and versioning surface is weaker — risky for §63's object-lock/versioning
  immutability requirement (D-08).
* **D — Adopt Ceph RGW now, instead of MinIO.** The most production-credible S3
  implementation available, but its operational weight is wildly disproportionate for a kind
  cluster whose entire point is local reproducibility on a laptop.
* **E — Adopt a Gateway API implementation now, instead of ingress-nginx.** Envoy Gateway,
  Traefik or Cilium's Gateway API mode is the ecosystem's actual answer to ingress-nginx's
  archival. Rejected for this phase because it would add a CRD-heavy component and a second
  routing vocabulary to a phase that already carries five charts, for a controller that still
  works and is still inside Kubernetes's supported version range.
* **F — Swap `quay.io/minio/mc` for the maintained `pgsty/mc` fork now.** `pgsty/mc` has 18
  published tags including one from 2026-08-06, so it is actively maintained — but it is
  **untested against this chart's 2024-era bootstrap scripts**, which call
  `mc admin policy create/attach`, `mc anonymous set` and `mc version enable`
  (`02-RESEARCH.md` § Supporting). Swapping an untested CLI into a working bootstrap path
  trades a dated-but-proven artifact for an unverified one, for no immediate gain.

## Decision Outcome

Chosen option: **pin all three artifacts as they stand today, and keep the client seams that
already exist hard.** No swap in this phase.

This decision is cheap to hold and cheap to reverse *because* two engineering commitments
already exist independently of this record, and this record's job is to name them so they
are not accidentally weakened later:

* **The S3 client is `boto3` with a context-injected endpoint (§5, D-07).** No Python,
  DAG, manifest or values file hardcodes a MinIO-specific address or SDK. Replacing MinIO
  with SeaweedFS — or with real AWS S3 — is a values-file image swap, not a code change.
  `tests/e2e/cluster/` already asserts through boto3, never through `mc`, precisely so this
  claim stays true (D-16).
* **Ingress objects carry no nginx-specific annotation.** Every `Ingress` this platform
  writes uses only the portable `ingressClassName` field. Replacing the controller with a
  Gateway API implementation is an `ingressClassName`-and-manifest-shape change, not a
  rewrite of routing logic.

### Consequences

* Good, because the platform ships today on artifacts that are verified working, rather than
  blocking Phase 2 on an unplanned storage or ingress migration.
* Good, because both migration paths this record commits to (boto3 endpoint injection,
  annotation-free Ingress objects) are also good design independent of this risk — they are
  the same abstraction §5 already demands for the AWS-swap case.
* Bad, because none of the three artifacts will receive an upstream CVE patch again, and two
  of the three (`pgsty/minio`, `pgsty/mc` if ever adopted) depend on a single maintainer's
  continued attention.
* Bad, because `registry.k8s.io/ingress-nginx/controller:v1.15.1` is reachable on host port
  80 (T-02-21) — the disposition and sign-off for that specific risk is recorded separately
  in the threat register below and in this task's `<human-check>`, precisely because it is
  the one high-severity item in this phase not dispositioned `mitigate`.
* Neutral, because `quay.io/minio/mc` is a low-risk artifact regardless of its age: it never
  runs against untrusted input, only against a bootstrap Job that creates buckets and
  policies at `cluster-up` time.

## Migration trigger

Not "none" — each of the following, for any of the three artifacts, is a mid-phase or
mid-milestone reason to open the migration this record already named:

* **A CVE with a public exploit** against `pgsty/minio`, `registry.k8s.io/ingress-nginx/controller:v1.15.1`, or `quay.io/minio/mc`, published after this record's date.
* **The `pgsty/minio` fork goes more than six months without a release.** Its release cadence
  today (2026-04-17, 2026-06-18, 2026-08-04) is roughly bimonthly; a six-month gap is a
  material change in the single maintainer's attention.
* **A Kubernetes upgrade past 1.35** takes the cluster outside `ingress-nginx` 4.15.1's
  documented support table (1.31–1.35). This platform pins `kindest/node:v1.35.5`
  deliberately (`.claude/CLAUDE.md` § A), so this trigger fires only on a future, deliberate
  version bump — not silently.
* **MinIO's S3 API surface diverges** from what this platform's `boto3` usage needs — for
  example, a multipart or object-lock behavior the application depends on regresses or is
  removed in a future `pgsty/minio` release.

On any of these: MinIO's migration target is **SeaweedFS**; ingress-nginx's migration target
is a **Gateway API implementation**. Both migrations are values-file and manifest-shape
changes under the seams this record commits to, not application rewrites.

## References

* README §5 — `s3://bucket/path` addressing, MinIO→S3 swappable
* README §63 — raw layer append-only (D-08's versioning + deny-delete policy)
* `.planning/research/STACK.md` § D — MinIO archival evidence, the alternatives-considered
  table (SeaweedFS, Garage, Ceph RGW, LocalStack) and their verdicts
* `.planning/phases/02-kind-cluster-core-infrastructure/02-RESEARCH.md` § Package Legitimacy
  Audit — the three `[SUS]` rows with registry, date and publisher; § Standard Stack — the
  ingress-nginx row and its Kubernetes 1.31–1.35 support table; § State of the Art — the
  ingress-nginx archival row, new since STACK.md; § Assumptions Log A7
* `helm/values/local/minio.yaml`, `helm/values/local/ingress-nginx.yaml` — the values files
  that pin the exact artifacts this record accepts
