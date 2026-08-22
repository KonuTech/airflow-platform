# Phase 11: CI/CD Completion & Operations - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-08-22
**Phase:** 11-CI/CD Completion & Operations
**Areas discussed:** Image publish & scanning scope, Ephemeral kind E2E scope in CI, Rebuild-from-raw semantics (INCR-07), Retention mechanism & defaults (INFRA-11)

---

## Image publish & scanning scope

| # | Question | Options presented | Selected |
|---|----------|--------------------|----------|
| 1 | Which images get published/scanned? | All three / csv-processor only / csv-processor+dbt | **All three** (D-01) |
| 2 | Publish trigger | Build-verify on PR, push on merge / push every PR / merge-only | **Build-verify on PR, push on merge** (D-02) |
| 3 | Semver tagging now? | Add now / defer | **Add now** (D-03) |
| 4 | Semver release trigger | Git tag push / GitHub Release creation | **GitHub Release creation** (D-03) |
| 5 | Package visibility | Match repo (public) / explicitly private | **Match repo — public** (D-04) |
| 6 | Architecture | amd64-only / multi-arch | **amd64-only** (D-05) |
| 7 | Image job vs Quality gate dependency | Independent/parallel / depend on check passing | **Independent/parallel** (D-06) |
| 8 | Pre-existing unfixed CVE handling | Handle live / pre-audit & pre-seed .trivyignore | **Handle live** (D-07) |
| 9 | Workflow file structure | New publish.yml / add jobs to ci.yml | **New publish.yml** (D-08) |
| 10 | How does E2E get PR's own image | Build locally + kind load / push PR builds tagged pr-N | **Push PR builds tagged pr-N** (D-09) |
| 11 | Scan pr-N images too? | Yes / merge-tagged only | **Yes** (D-10) |
| 12 | Cleanup pr-N tags | Defer / auto-cleanup on PR close | **Auto-cleanup on PR close** (D-11) |
| 13 | Rollback tooling | No special tooling / dedicated script | **Dedicated rollback Make target/script** (D-12) |
| 14 | Supply-chain attestation | Neither / SBOM only / SBOM+cosign signing | **SBOM + cosign signing** (D-13) |
| 15 | Admission signature enforcement | Sign only / add admission enforcement | **Add admission-time enforcement (Kyverno)** — flagged as scope expansion beyond Phase 11's REQ-IDs; user explicitly kept it in (D-14) |
| 16 | Kyverno policy scope | Project images only / cluster-wide | **Cluster-wide** (D-15) |
| 17 | Third-party image handling | Exception list / re-sign upstream images | **Exception list of pinned upstream images** (D-16) |
| 18 | Kyverno in which profile(s) | Both local+CI / local-only | **Both** (D-17) |
| 19 | Live proof for Kyverno | Positive+negative (SEC-12 pattern) / positive only | **Positive+negative** (D-18) |

**Notes:** Question 15 was a genuine scope-creep checkpoint — Claude explicitly stated Kyverno
admission enforcement is not covered by CICD-05/06/08/09, QUAL-15, OBS-06, INFRA-11, or INCR-07,
and asked the user to confirm before proceeding. User confirmed keeping it in-phase.

---

## Ephemeral kind E2E scope in CI

| # | Question | Options presented | Selected |
|---|----------|--------------------|----------|
| 1 | What runs in the PR-gating job vs full suite | Full/every PR / fast subset PR, full nightly / full merge-only, smoke on PR | **Full suite merge-only, smoke subset on PR** (D-19) |
| 2 | Smoke subset contents | Cluster health / one DAG run / Vault pos+neg / Kyverno pos+neg (multiSelect) | **All four** (D-20) |
| 3 | Docker Hub rate-limit mitigation | Authenticated login / retry-only | **Authenticated Docker Hub login** (D-21) |
| 4 | Failure handling for merge-triggered full suite | Red status only / auto-file GitHub issue | **Auto-file GitHub issue** (D-22) |
| 5 | Coverage reporting location | GH Actions summary+artifact / Codecov | **GH Actions summary+artifact** (D-23) |
| 6 | Where does rebuild-from-raw run | Part of full E2E suite / local-only | **Part of full E2E suite** (D-24) |
| 7 | Where do chaos/failure-scenario tests run | Same job after normal E2E / separate dedicated job | **Separate dedicated job/cluster** (D-25) |
| 8 | Chaos job trigger | Merge (same as full E2E) / scheduled | **Merge to main** (D-26) |
| 9 | Full-E2E vs chaos job scheduling | Parallel / sequential | **Parallel** (D-27) |

---

## Rebuild-from-raw semantics (INCR-07)

| # | Question | Options presented | Selected |
|---|----------|--------------------|----------|
| 1 | What gets dropped | All ETL-owned schemas incl. meta / data schemas only / whole PostgreSQL instance | **All ETL-owned schemas incl. meta** (D-28) |
| 2 | What proves reconciliation | Row counts / content hash / SCD2 version+is_current / reuse reconciliation_results (multiSelect) | **All four** (D-29) |
| 3 | Sequencing/data source | Runs last, reuses suite's own state / independent seeded dataset | **Runs last, reuses suite's state** (D-30) |
| 4 | Config version during replay | Historical config resolution / current config only | **Historical config resolution** (D-31) |
| 5 | Rebuild invocation | New Make target wrapping drop+backfill / CI-only test code | **New Make target** (D-32) |
| 6 | MinIO layer scope | PostgreSQL only / wipe+regenerate MinIO layers too | **Wipe+regenerate MinIO layers too** (D-33) |
| 7 | Quarantine-resolution history loss | Acceptable/document / reconciliation must account for it | **Acceptable/document, don't special-case** (D-34) |

---

## Retention mechanism & defaults (INFRA-11)

| # | Question | Options presented | Selected |
|---|----------|--------------------|----------|
| 1 | Enforcement mechanism | Airflow maintenance DAG / standalone script+cron / K8s CronJob | **Dedicated Airflow maintenance DAG** (D-35) |
| 2 | Raw layer default | Effectively indefinite / long-but-finite (2yr) | **Effectively indefinite** (D-36) |
| 3 | Other layers' defaults | Tiered by purpose / one uniform default | **Tiered by purpose** (D-37) |
| 4 | Delete mode | Dry-run default, opt-in hard-delete / hard-delete directly | **Dry-run default, explicit opt-in** (D-38) |
| 5 | Config location | Extend dataset YAML / separate retention.yaml | **Extend dataset YAML** (D-39) |
| 6 | Raw storage-layer enforcement | Add MinIO object-lock+deny-delete / rely on retention default alone | **Add storage-layer enforcement** — refined below |
| 6b | Object-lock vs IAM policy (Phase 2 D-08 conflict surfaced) | IAM deny-delete policy / object-lock CI-only+IAM local / reverse D-08 fully | **IAM deny-delete policy (not object-lock/WORM)** (D-40) |
| 7 | Runbook structure | One doc per scenario, seeded from real incidents / single consolidated file | **One doc per scenario, seeded from real incidents** (D-41) |

**Notes:** Question 6/6b surfaced a direct conflict with a prior locked decision: Phase 2's
`helm/values/local/minio.yaml` explicitly rejected MinIO object-lock/WORM (D-08) because it makes
local dev test-object cleanup impossible, and named Phase 11 as the place to revisit it. Claude
surfaced this conflict before proceeding; the resolution (IAM policy instead of object-lock)
achieves the same practical goal without reopening D-08's objection.

---

## Claude's Discretion

- Exact numeric retention-window defaults within D-37's tiers.
- Exact Kyverno `ClusterPolicy` resource shape/naming; exact cosign OIDC issuer/identity config.
- Retention DAG's schedule cadence (daily vs weekly).
- Exact rollback Make target name/mechanics beyond "redeploy at a prior git SHA."
- `.trivyignore` file format specifics — applied only if/when a real finding occurs.
- Exact GitHub Actions workflow YAML structure (job names, step ordering) beyond what's locked.
- Whether the rollback target gets its own `docs/runbooks/` entry or stays Makefile-only.

## Deferred Ideas

None — discussion stayed within Phase 11's scope, including the one flagged-and-kept scope
expansion (Kyverno admission enforcement, D-14).
