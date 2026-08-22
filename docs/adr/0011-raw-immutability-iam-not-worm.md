---
status: accepted
date: 2026-08-22
---

# ADR-0011: Raw immutability is enforced via an IAM deny-delete policy, not object-lock/WORM

## Context and Problem Statement

README §63 requires the raw layer to be append-only: corrections arrive as new files or new
versions, never overwrites, and §90/§91 require the platform to be able to prove that
immutability rather than merely assert it by convention. Phase 2 built the `raw` MinIO bucket
with `versioning: true` (so a same-key upload becomes a new version rather than an overwrite) and
considered enabling MinIO's own object-lock/WORM feature on top of that, to make deletion
provably impossible at the storage engine level rather than merely policy-denied.

Phase 2 explicitly considered and rejected WORM at that time (`helm/values/local/minio.yaml`'s
own D-08 comment, commit `c600905`): a WORM retention lock is, by design, permanent for its
configured duration -- no principal, including an administrator, can shorten or remove it early.
On a **persistent local development cluster** that is rebuilt and reseeded with synthetic test
fixtures repeatedly throughout this project's life (not a production write-once archive), that
permanence is a genuine operational liability: objects written under a WORM-locked bucket policy
would either need to expire on their own schedule or make the whole bucket unusable for iterative
development and testing. Phase 2's D-08 comment named Phase 11 (rebuild-from-raw, this phase's
own capstone) as the place this question would be revisited, once the platform had a concrete
rebuild-from-raw story to weigh the trade-off against.

This record formalizes that revisit's conclusion: the IAM deny-delete policy Phase 2 already
shipped alongside `versioning: true` (`helm/values/local/minio.yaml` and
`helm/values/ci/minio.yaml`, the `etl-app` policy's `Deny` statement on `s3:DeleteObject`/
`s3:DeleteObjectVersion` against `arn:aws:s3:::raw/*`) is kept as the permanent mechanism, not
superseded by WORM. `.planning/phases/11-ci-cd-completion-operations/11-CONTEXT.md`'s D-40
records the same conclusion at the phase-planning level; this ADR is its permanent, standalone
record, and the reason no new IAM statement or new test appears alongside it -- the resolution
was already fully implemented and live-proven, and this phase's own job was to verify that still
holds, not to quietly rebuild it as new work.

## Considered Options

* **IAM deny-delete policy (chosen; already implemented since Phase 2).** Every workload identity
  attached to the `etl-app` MinIO policy is structurally denied `s3:DeleteObject`/
  `s3:DeleteObjectVersion` against `raw/*`, enforced by the MinIO server itself, not by
  application-code convention. A separate, no-workload-attached admin credential retains delete,
  for genuine break-glass/administrative use (`cluster-rebuild` tooling, fixture reseeding).
* **Object-lock / WORM.** MinIO's native write-once-read-many retention lock on the `raw` bucket,
  configured with a retention period and mode (`GOVERNANCE` or `COMPLIANCE`). Deletion becomes
  impossible for every principal, including an administrator, for the configured retention window
  (`COMPLIANCE` mode), or requires a special bypass permission even under `GOVERNANCE` mode.

## Decision Outcome

Chosen option: **IAM deny-delete policy**, because it gives every workload identity the same
practical inability to delete `raw` objects that WORM would, while remaining revocable for a
genuine break-glass administrative scenario -- WORM's retention lock is permanent for its
configured duration, which directly conflicts with this being a persistent, repeatedly rebuilt
local development cluster rather than a production write-once archive. The IAM policy was also
already live and proven (Phase 2, positive+negative tested in
`tests/e2e/cluster/test_minio_buckets.py::test_raw_delete_is_denied_for_app_credential` /
`::test_raw_delete_is_permitted_for_admin_credential`, both re-verified live in this phase --
`uv run --group cluster pytest tests/e2e/cluster/test_minio_buckets.py -q -m cluster -k delete`,
2 passed). Choosing WORM instead would have meant tearing out a working, tested control to
replace it with one this project's own D-08 finding had already identified as operationally
worse for this specific environment.

### Consequences

* Good, because the `raw` bucket stays cleanable in development -- a `cluster-rebuild` or fixture
  reseed can remove and recreate test objects without waiting out or bypassing a retention lock,
  which matters for a cluster this project expects to tear down and rebuild repeatedly (INCR-07's
  own rebuild-from-raw capstone is a direct beneficiary: the raw objects it replays from must
  themselves stay manageable by the same tooling that manages everything else).
* Bad, because a compromised or misused **admin** credential could still delete `raw` objects --
  unlike true WORM, this mechanism's guarantee is scoped to "every workload identity," not "every
  principal, full stop." The admin credential is used by no live workload (only manual/
  `cluster-rebuild` tooling), which bounds but does not eliminate this exposure (T-11-18, this
  phase's own threat register, records the same trade-off as an accepted risk).
* Neutral, because §63's actual requirement -- corrections arrive as new versions, never
  overwrites -- is independently satisfied by `versioning: true` on `raw` regardless of which
  deletion-prevention mechanism sits alongside it; this decision is specifically about who can
  delete, not about whether an overwrite silently loses data.

## Migration trigger

Not "none" -- a genuine compliance or regulatory requirement for **tamper-proof, no-exceptions**
retention (a requirement that even a compromised or misused administrator credential must be
unable to circumvent) is a concrete, observable reason to revisit this decision toward
object-lock/WORM, accepting the operational cost of a bucket that can no longer be freely cleaned
up in development. Absent such a requirement, the IAM deny-delete policy remains the right fit
for a persistent, repeatedly-rebuilt local development cluster.

## References

* README §63 (raw immutability), §90 (Disaster Recovery / Rebuildability), §91 (Data Retention,
  duplicate of §64)
* `.planning/phases/11-ci-cd-completion-operations/11-CONTEXT.md` D-40
* `helm/values/local/minio.yaml` -- the `objectlocking: false` bucket declarations and the
  `etl-app` policy's `Deny` statement on `raw/*`
* `helm/values/ci/minio.yaml` -- the CI profile's parallel bucket/policy configuration
* `tests/e2e/cluster/test_minio_buckets.py` -- the live positive+negative proof
  (`test_raw_delete_is_denied_for_app_credential` /
  `test_raw_delete_is_permitted_for_admin_credential`)
* Phase 2 commit `c600905` -- the original D-08 decision and IAM policy implementation
