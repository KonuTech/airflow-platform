---
status: accepted
date: 2026-08-14
---

# ADR-0009: Vault's BUSL-1.1 licence is accepted for this milestone; OpenBao is the named migration target

## Context and Problem Statement

`.claude/CLAUDE.md` §E and `.planning/research/STACK.md` §E both record, at HIGH
confidence, that HashiCorp Vault's server went major to **2.x on 2026-04-14** and is
licensed **BUSL-1.1** (Business Source License), not an OSI-approved open-source
licence — the Helm chart's own `Chart.yaml` copyright header now reads "IBM Corp.
2018, 2026", confirming the 2024 HashiCorp/IBM acquisition carried through to this
artifact. Phase 5 is the phase `docs/adr/README.md`'s own "Deliberately deferred
records" table already named for this exact record: "Nothing is deployed against
Vault until Phase 5. The licence assessment is real but has no consequence to record
yet." That phase has now run — Vault is deployed in-cluster (`helm/values/{local,ci}/
vault.yaml`, plan 05-01), it is the sole runtime source of credentials for both the
`csv-processor` and `airflow` workload identities (plans 05-02/05-03), and its audit
log and rotation behavior are live-proven (plan 05-04). The licence assessment now has
a real consequence to record.

BUSL-1.1 is source-available, not open-source: it permits free use, modification and
self-hosting for any purpose that does not compete with HashiCorp's own commercial
Vault offering, and converts to the Mozilla Public License 2.0 on a per-release
"Change Date" (typically four years after each version's release). This platform's use
— a local, non-commercial, non-competing kind-cluster deployment — sits squarely
inside BUSL-1.1's permitted field of use today. The question this record answers is
not whether that use is currently permitted (it is), but what happens if the licence
terms, the deployment context, or the artifact's own maintenance posture changes.

## Considered Options

* **A — Accept Vault as-is for this milestone.** BUSL-1.1 permits non-competing
  internal/self-hosted use, and this project is a local ETL platform, not a competing
  secrets-management product. No engineering cost; the risk is a licence-term risk,
  not a functional one, and this project's own S3 (ADR-0006) and CSV-parsing seams
  already demonstrate the pattern of accepting a real risk today behind a documented,
  named escape hatch rather than pre-emptively re-architecting around a risk that has
  not materialized.
* **B — Adopt OpenBao now, instead of Vault.** OpenBao is a Linux Foundation fork of
  Vault's last MPL-2.0-licensed release (forked in response to the same BUSL-1.1
  relicensing this record is about), API-compatible with Vault's Kubernetes auth
  method, KV v2 engine, and audit-device model. Rejected for THIS milestone: it would
  mean re-verifying every empirically-confirmed fact this phase's five plans already
  established live against Vault 2.0.3 specifically — the `disable_iss_validation`
  default (05-RESEARCH.md Question 1), the exact `bound_service_account_names` set
  each Airflow component needs (05-03's empirical audit-log correction), and the audit
  log's field-hashing behavior (05-04's `test_audit_log.py`) — against a different
  binary with no guarantee every behavior is byte-identical, for a licence risk that
  is not yet operative for this project's field of use.
* **C — Adopt a cloud-native secrets manager instead (AWS Secrets Manager / GCP
  Secret Manager / Azure Key Vault).** Rejected outright, not just deferred: README
  §3.1 mandates a local, production-like kind cluster, and PROJECT.md's own Out of
  Scope section excludes cloud deployment for this milestone. A cloud secrets manager
  would also reintroduce exactly the swap-out coupling `SecretsResolver`'s opaque
  `vault://` scheme (SEC-15, D3) was built to avoid — trading one named risk (a
  licence) for a structural one (a network dependency this local platform cannot
  satisfy offline).

## Decision Outcome

Chosen option: **A — accept Vault 2.0.3/BUSL-1.1 for this milestone, and name OpenBao
as the documented, API-compatible migration target**, because the licence risk is real
but not yet operative for this project's local, non-competing, non-commercial field of
use, and re-verifying this phase's entire empirical evidence base against a different
binary (Option B) is a cost this milestone should not pay for a risk that has not
materialized. This mirrors ADR-0006's own precedent exactly: name the artifact, name
the risk, name the alternative, and record the observable event that would change the
answer — rather than silently accepting the risk or pre-emptively re-architecting
around it.

This decision is cheap to hold and cheap to reverse *because* two engineering
commitments already exist independently of this record:

* **Every credential reference in this codebase is opaque** (`vault://mount/path#field`,
  `packages/dataplat/src/dataplat/secrets/resolver.py`). No call site in `dataplat`,
  `csv_processor`, or any DAG names `hvac`, Vault, or any Vault-specific concept — the
  scheme dispatch inside `resolve_secret()` is the only place that would need to change
  for an OpenBao swap, exactly as SEC-15/D3 already require for the Kubernetes-Secret-
  to-Vault swap this same phase performed.
* **Vault's own bootstrap/unseal tooling is a thin `hvac` wrapper**
  (`scripts/vault-bootstrap.py`, `scripts/vault-unseal.py`), not a hand-rolled
  reimplementation of any Vault-specific protocol. `hvac`'s HTTP client targets any
  server implementing Vault's API surface — including OpenBao, which deliberately
  preserves that surface.

### Consequences

* Good, because this milestone ships on an artifact already deployed, empirically
  verified, and live-proven across five plans, rather than blocking Phase 5 on an
  unplanned secrets-backend migration.
* Good, because the migration path this record commits to (an opaque `vault://`
  scheme, a thin `hvac`-based bootstrap layer) is also good design independent of this
  risk — it is the same abstraction SEC-15/D3 already demand for the Kubernetes-
  Secrets-to-Vault swap this phase performed, applied one layer further out.
* Bad, because this platform now depends on a BUSL-1.1-licensed artifact whose licence
  terms are set unilaterally by a single vendor (HashiCorp/IBM), not a foundation or
  community process — a materially different governance posture than the CNCF-governed
  CloudNativePG this project chose for PostgreSQL (`.claude/CLAUDE.md` §C).
* Bad, because `tls_disable = 1` (T-05-05, accepted separately in plan 05-01) means the
  ServiceAccount JWT and every credential value this phase moves through Vault already
  travel as plaintext HTTP inside the cluster network — a second, independent
  local-dev-only risk acceptance layered on top of this one, both scoped to the same
  non-production deployment.
* Neutral, because BUSL-1.1's eventual conversion to MPL-2.0 (four years after each
  version's release, per HashiCorp's own published Change Date schedule) means the
  specific Vault 2.0.3 binary this platform pins today becomes unambiguously
  open-source on its own, without any migration, on a fixed future date — the risk
  this record accepts has a built-in expiry independent of any action this project
  takes.

## Migration trigger

Not "none" — each of the following is a mid-milestone or post-milestone reason to
execute the OpenBao migration this record already names:

* **A BUSL-1.1 licence-term change adverse to this project's use** — for example, a
  narrowing of the "non-competing" field-of-use definition, or a change that makes
  self-hosted internal use require a paid licence regardless of competitive posture.
* **A security-relevant CVE against the pinned Vault version** (`2.0.3`, chart
  `0.34.0`) published and left unpatched past a stated window (aligned with this
  project's own `.trivyignore` discipline, `.claude/CLAUDE.md` § I): 90 days for a
  HIGH-severity finding, 30 days for CRITICAL.
* **A decision to deploy this pattern outside local dev** — a shared, multi-user, or
  production-adjacent environment, where BUSL-1.1's field-of-use restriction becomes
  operative in a way it structurally is not for a single-developer, laptop-only kind
  cluster (README §3.1's own local-only mandate is what keeps this trigger from firing
  today).
* **The `pgsty/minio`-style single-maintainer risk, inverted** — if HashiCorp/IBM
  materially reduces Vault's own release or patch cadence (the inverse of ADR-0006's
  "six months without a release" trigger for a community fork), the calculus in Option
  B's rejection (avoiding re-verification cost for an actively-maintained artifact)
  no longer holds, and the re-verification cost becomes worth paying.

On any of these: the migration target is **OpenBao**, via the Helm chart and image
swap `helm/values/{local,ci}/vault.yaml` already isolates (mirroring ADR-0006's MinIO
precedent — a values-file and image-tag change, not an application rewrite), with
`scripts/vault-bootstrap.py`/`scripts/vault-unseal.py` re-verified against OpenBao's
own `hvac`-compatible API surface before cutover.

## References

* `.claude/CLAUDE.md` §E — Vault chart `0.34.0`/server `2.0.3` version pins, the IBM
  copyright header finding, the secret-delivery mechanism comparison table
* `.planning/research/STACK.md` §E — the BUSL-1.1/major-2.x finding this record acts
  on, and the two-tier direct-SA-token-login pattern this record's "cheap to reverse"
  argument depends on
* `.planning/phases/05-vault-secrets-workload-identity/05-RESEARCH.md` — live
  verification that `apache-airflow-providers-hashicorp`/`hvac` are already installed
  in the stock Airflow image, and the empirical resolution of which ServiceAccounts
  authenticate to Vault (both independent of the Vault-vs-OpenBao choice)
  `.planning/phases/05-vault-secrets-workload-identity/05-01-SUMMARY.md` through
  `05-04-SUMMARY.md` — the five live proofs (deployment/restart-survival, workload
  identity, Airflow backend, rotation/audit) this record's "already empirically
  verified against Vault 2.0.3 specifically" argument (Option B's rejection) is based
  on
* `docs/adr/0006-unmaintained-upstream-artifacts.md` — the precedent this record
  follows: name the artifact, name the risk, name the alternative, name the trigger
* `docs/secrets-architecture.md` — production-substitution documentation (SEC-14)
  that cross-references this record
* `packages/dataplat/src/dataplat/secrets/resolver.py` — the opaque `vault://` scheme
  that makes this record's migration path a configuration change, not a rewrite
