# Phase 11: CI/CD Completion & Operations - Research

**Researched:** 2026-08-22
**Domain:** GitHub Actions CI/CD (ephemeral-kind E2E, supply-chain signing/admission enforcement), operational runbooks, rebuild-from-raw disaster recovery, retention
**Confidence:** MEDIUM-HIGH (codebase findings HIGH via direct read; Kyverno/cosign ecosystem findings HIGH-MEDIUM via direct API/doc fetch; a few items flagged LOW where sources disagreed)

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**Image Publishing & Registry**
- D-01: All three images get GHCR-published + trivy-scanned: `csv-processor`, `dbt` (Phase 08.1), and the custom Airflow[otel] image (Phase 07) — full parity with the local `image-csv-processor`/`image-dbt`/`image-airflow` Make targets.
- D-02: Publish trigger is build-verify on every PR (no push), push + git-SHA tag on merge to main (refined by D-09).
- D-03: Semver tagging added now, triggered by GitHub Release creation (not a bare git-tag push).
- D-04: GHCR package visibility matches the repo (public repo → public packages). No extra config beyond `GITHUB_TOKEN` with `packages: write`.
- D-05: Images are amd64-only. No buildx multi-platform/QEMU emulation.
- D-06: The image build/publish/scan job runs independently/in parallel with `check`/`manifests`/`integration`/`secrets` in `ci.yml`.
- D-07: If trivy's first real run finds a pre-existing unfixed HIGH/CRITICAL CVE in a base image, handle it live — add a dated, justified `.trivyignore` entry at that time, not pre-emptively.
- D-08: Image build/publish/scan/sign jobs live in a new separate workflow file (e.g. `.github/workflows/publish.yml`), not added to `ci.yml`.
- D-09: PR builds ARE pushed to GHCR too, tagged `pr-<number>`, so the E2E job can pull the PR's actual code.
- D-10: `pr-<number>` images get the same trivy HIGH/CRITICAL scan gate as merge-tagged images.
- D-11: `pr-<number>` tags are automatically cleaned up from GHCR when the PR closes (merged or not).
- D-12: A dedicated rollback Make target/script is added (redeploy the image at a prior git SHA), documented as a runbook entry too.

**Image Scanning & Supply Chain**
- D-13: Every published image gets SBOM (buildx `--provenance`/`--sbom`) + cosign keyless (OIDC-based) signing.
- D-14 (LOCKED — deliberate scope expansion, flagged by Claude and kept by the user anyway): Admission-time signature enforcement is added via Kyverno, a new cluster admission controller — genuinely new platform capability beyond any Phase 11 REQ-ID/success-criterion.
- D-15: Kyverno's policy applies cluster-wide (every pod, every namespace).
- D-16: A Kyverno policy exception list of the exact pinned upstream image references already committed in `helm/values/*.yaml` is exempted from verification. Anything NOT on that list must verify.
- D-17: Kyverno is deployed in both the local and CI (`values-ci.yaml`) Helm values profiles. Footprint assumed ~100-200m CPU, one controller pod (see Common Pitfalls — this estimate needs correcting).
- D-18: Kyverno enforcement gets the same positive + negative live-proof pattern as SEC-12: a test deploys a signed image (admits) and a deliberately unsigned/tampered image (denied).

**Ephemeral kind E2E Scope in CI (CICD-09)**
- D-19: The full local e2e suite runs only on merge to main. PRs get a fast smoke subset.
- D-20: The PR-gating smoke subset covers, at minimum: (1) cluster boots + core services healthy (Airflow, both PostgreSQL, MinIO, Vault, Kyverno all Helm-release healthy), (2) one real DAG run reaching SUCCEEDED end-to-end, (3) a Vault positive+negative auth test (SEC-12 pattern), (4) a Kyverno positive+negative admission test (D-18).
- D-21: CI authenticates to Docker Hub (`docker/login-action` + a repo secret) before pulling chart images, to raise the pull-rate limit from anonymous (100/6hr/IP) to authenticated (200/6hr).
- D-22: When the merge-triggered full E2E suite fails, a workflow step auto-files/updates a GitHub issue tagging the failing commit, in addition to the red CI status check.
- D-23: Coverage (CICD-05) is reported via the GitHub Actions job summary + an uploaded HTML/artifact — no third-party service.
- D-24: The rebuild-from-raw capstone test (INCR-07) runs as part of the merge-triggered full E2E suite, not local/manual-only.
- D-25: QUAL-15's failure-scenario/chaos tests run in a separate dedicated job/ephemeral-kind cluster, not interleaved with the happy-path suite.
- D-26: The chaos job triggers on merge to main, same trigger as the full E2E suite (not scheduled).
- D-27: The full-E2E job and the chaos job run in parallel (two separate GitHub-hosted runners, each with its own ephemeral kind cluster).

**Rebuild-from-Raw Semantics (INCR-07)**
- D-28: "The analytical warehouse is dropped and rebuilt" means all ETL-owned schemas: staging, silver (dbt), gold/normalized (incl. SCD2 customers/orders), AND meta — not just data schemas, not the whole PostgreSQL instance.
- D-29: A rebuild re-derives its own run/batch identity from scratch. "Reconciles to its pre-drop state" is proven by all four: (1) row counts per table match, (2) a content hash/checksum over each table's business data — excluding surrogate run/batch identity columns — matches, (3) SCD2 version count + valid_from/valid_to/is_current state per customer_id matches, and (4) the comparison reuses the existing `meta.reconciliation_results`/`record_reconciliation` mechanism rather than a bespoke pre/post query.
- D-30: Rebuild-from-raw runs last, within the merge-triggered full E2E suite, reusing whatever state the suite's own tests already populated as the pre-drop baseline. No separate data-seeding.
- D-31: The rebuild uses the existing historical config-resolution path (ConfigRegistry, the same mechanism INCR-06 proves for backfills) — each raw file reprocessed under the config version actually in effect for it.
- D-32: Rebuild is invoked via a new reusable Make target (e.g. `make rebuild-from-raw`) wrapping: drop ETL-owned schemas → `alembic upgrade head` → trigger Airflow backfill DagRuns across the full historical range discovered from raw. CI calls the same target a real operator would use.
- D-33: The rebuild also wipes and regenerates MinIO's `validated`/`processed`/`quarantine` object-storage layers, not just PostgreSQL schemas.
- D-34: Quarantine-resolution history (VALID-08) is lost when `meta` is dropped — a previously-resolved rejected record comes back as freshly-rejected/PENDING after rebuild. This is acceptable/expected, documented explicitly, and must NOT be special-cased out of the D-29 reconciliation comparison.

**Retention (INFRA-11)**
- D-35: Retention is enforced by a dedicated Airflow maintenance DAG (e.g. `platform_retention`), deliberately not part of any ingestion DAG's task graph.
- D-36: Raw's default retention is effectively indefinite (no automatic pruning) — the DAG still supports a configurable window for raw, but the shipped default is disabled/None.
- D-37: Other layers get tiered defaults by operational purpose: `processed` short (30-90 days), `quarantine` longer (180 days), validation reports + ingestion metadata long (1-2 years), logs short (30 days). Exact numeric defaults within ranges are Claude's discretion.
- D-38: The retention DAG dry-runs by default — every run reports what WOULD be deleted regardless of configuration. Actual hard-deletion requires an explicit opt-in flag (e.g. `retention.enforce: true`, defaulting false).
- D-39: Retention configuration lives per-dataset, extending the existing dataset YAML with a new `retention:` block validated by the same Pydantic contract model. Not a separate platform-wide `retention.yaml`.
- D-40 (LOCKED — resolves Phase 2's own D-08 revisit note): Enforce raw immutability via an IAM bucket policy denying `s3:DeleteObject`/`s3:DeleteObjectVersion` on `raw` to every role except a break-glass admin identity — NOT object-lock/WORM. **See Common Pitfalls — this is already fully implemented since Phase 2; verify, do not rebuild.**

**Runbooks (OBS-06)**
- D-41: Runbooks are one doc per §89 scenario in `docs/runbooks/` (symptoms, diagnosis, recovery, reprocessing, verification). Scenarios with a matching real incident in `.planning/debug/resolved/*.md` are written from that incident's actual diagnosis/fix. Scenarios never actually hit are written from the D-25 chaos test suite's own observed behavior once that test exists — runbooks trail the chaos tests.

### Claude's Discretion
- Exact numeric retention-window defaults within the D-37 tiers.
- Exact Kyverno policy resource shape/naming, and exact cosign OIDC issuer/identity configuration details.
- Retention DAG's schedule cadence (daily vs weekly).
- Exact rollback Make target name/mechanics beyond "redeploy at a prior git SHA."
- `.trivyignore` file format specifics — apply when a real finding occurs.
- Exact GitHub Actions workflow YAML structure (job names, step ordering) beyond what D-01..D-27 lock.
- Whether the rollback Make target also gets its own runbook entry under D-41, or lives only as a Makefile target with inline comments.

### Deferred Ideas (OUT OF SCOPE)
None raised that belong to a different phase. D-14 (Kyverno) was explicitly flagged by Claude as a scope expansion beyond any Phase 11 REQ-ID and confirmed kept by the user — do not quietly descope it during planning.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| CICD-05 | Unit, integration and E2E tests run automatically with coverage reporting | `coverage report --format=markdown` → `$GITHUB_STEP_SUMMARY` + `actions/upload-artifact` HTML report (see Code Examples). No new dependency — `coverage` 7.15.4 already pinned. Project's own `pyproject.toml` deliberately has no `fail_under` — do not introduce a numeric coverage gate now. |
| CICD-06 | Container images build automatically and are tagged by git SHA | `docker/build-push-action` v7.3.0 replaces the Makefile's plain `docker build` for CI (buildx is required for SBOM/provenance) — see Architecture Patterns, "Two build paths." |
| CICD-08 | Image vulnerability and dependency scanning run in CI | trivy 0.73.0 already pinned in CLAUDE.md/CI conventions; extend the existing per-image gate to `publish.yml` (D-06/D-08/D-10). |
| CICD-09 | Ephemeral kind cluster in CI deploys the stack and runs E2E | D-19/D-20 smoke subset + D-24/D-27 full/chaos jobs. See Architecture Patterns for job topology and realistic timeout sizing. |
| QUAL-15 | Failure/recovery scenarios from §84 tested (11-item DoD-89 subset) | See "README §84 vs §89" finding below for the exact, verified 11-item list and file layout recommendation (`tests/e2e/chaos/`). |
| OBS-06 | Operational runbooks for each §89 scenario | See "README §84 vs §89" finding for the verified 18-item list (not 17) and per-scenario source mapping (real incident vs. chaos-test-derived vs. existing-feature-documentation). |
| INFRA-11 | Configurable retention policies, enforced separately from processing logic | `platform_retention` DAG (D-35), dry-run-by-default (D-38), per-dataset config (D-39). See Architecture Patterns. |
| INCR-07 | Analytical data rebuilt from immutable raw layer | D-28..D-34. See "D-29 reconciliation reuse" finding — `_table_checksum()` exists but needs a column-exclusion variant; `record_reconciliation`'s grain is per-file-per-hop, not a snapshot-diff mechanism. |
</phase_requirements>

## Summary

This phase's 41 locked decisions are unusually well-specified — most of the "what" is already answered. The highest-value research findings are not about filling gaps in CONTEXT.md but about correcting or sharpening specific technical claims it makes, and about verifying which of its "new work" items already exist in the codebase.

Three findings should change how the plan is written, not just add detail to it. First, **D-40's entire resolution (the IAM deny-delete policy on `raw`) is already implemented and already has a live positive+negative proof test**, committed in Phase 2 (`c600905`). Second, **Kyverno's `ClusterPolicy.verifyImages` is deprecated** (removal targeted for Kyverno v1.20, ~October 2026) — the current, stable mechanism is the CEL-based `ImageValidatingPolicy` (stable since chart 3.8.0 / app v1.18.0), and D-17's assumed "~100-200m CPU, one controller pod" understates the real footprint by roughly 2-4x because the modern chart deploys four separate controllers by default. Third, **combining D-09 (PR images pushed to GHCR) with D-17 (Kyverno enforced in the CI profile) with D-20 (PR smoke test must reach a real DAG SUCCEEDED)** creates a hard requirement that CONTEXT.md never states explicitly: PR-tagged (`pr-<number>`) images must ALSO be cosign-signed, or the PR's own ephemeral-kind cluster will refuse to admit the PR's own image once Kyverno is wired in, and every PR's smoke test fails for an unrelated-looking reason.

The codebase's `_table_checksum()` helper (`packages/dataplat/src/dataplat/pipeline/run.py:301`) already implements the "order-independent content hash" D-29 asks for, but it hashes every column via `t::text`, including the six embedded lineage columns (`_run_id`, `_file_id`, `_batch_id`, `_ingested_at`, `_dbt_loaded_at`) that a rebuild deliberately re-mints with new values. A literal reuse of this function will report a false mismatch on every rebuild. `record_reconciliation`'s own grain (one row per `(file_id, hop)`, written inside a single processing pass) is naturally exercised again during the rebuild's own backfill DagRuns — but it does not, by itself, give you a "does table T today equal table T before the drop" comparison; that half of D-29 still needs new (small) code, just not a bespoke reconciliation subsystem.

**Primary recommendation:** Treat this phase as "verify, correct, and wire in" rather than "build from scratch" wherever CONTEXT.md's decisions overlap with Phase 2-10 work (D-40 above all), spend the real new-build effort on Kyverno/cosign using the *current* (not the CONTEXT.md-assumed) API surface, and explicitly test the PR-image-signing / fork-PR-token / Kyverno-stage-ordering interactions called out below before they surface as confusing CI failures.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Image build/tag/push/scan/sign | CI/CD (GitHub Actions) | Registry (GHCR) | Build-time supply-chain integrity; no in-cluster component owns this |
| Admission-time signature verification | Kubernetes / Cluster (Kyverno admission webhook) | — | Runtime gate at the API server, cluster-wide (D-15); cannot live in CI, must be enforced at every `kubectl create`/kubelet-adjacent path |
| Ephemeral E2E test execution | CI/CD (GitHub Actions job) | Kubernetes (ephemeral kind) | The job orchestrates; the ephemeral cluster is disposable infrastructure the job owns for its lifetime only |
| Rebuild-from-raw orchestration | Orchestrator (Airflow, via `make rebuild-from-raw`) | Database / Storage (schema drop, MinIO layer wipe) | Airflow drives backfill DagRuns; the actual state mutation (DROP SCHEMA, `mc rm`) happens in Database/Storage tier |
| Reconciliation comparison (D-29) | Database / Storage (SQL aggregates) | Orchestrator (test/CLI driving the comparison) | Row counts, checksums and SCD2 state are computed in PostgreSQL; the comparison harness is a thin caller |
| Retention enforcement | Orchestrator (dedicated `platform_retention` DAG) | Storage (MinIO) + Database (meta tables) | D-35 requires it structurally separate from ingestion DAGs, but the DAG still issues deletes against Storage/Database tiers |
| Runbook documentation | Docs (repo) | — | Not runtime; purely an operational artifact, but its accuracy depends on Chaos-test tier (D-25) behavior |
| GHCR package lifecycle (PR tag cleanup) | CI/CD (GitHub Actions) | Registry (GHCR API) | Triggered by `pull_request: closed`, executes against the registry's own API |

## Package Legitimacy Audit

**Not applicable in the traditional sense.** This phase introduces no new PyPI, npm, or cargo dependencies — no `pip install`/`uv add`/`npm install` of anything new. It introduces new **infrastructure tooling** (a Helm chart and a signing CLI) and new **GitHub Actions**, which the standard slopcheck/registry gate does not cover. Instead, each new tool/action was verified directly against its GitHub repository (org identity, release cadence, archival status) — see the table below.

| Tool/Action | Maintaining org (verified) | Repo archived? | Latest release (verified via GitHub API) | Disposition |
|---|---|---|---|---|
| Kyverno (Helm chart) | `kyverno` (CNCF project) | No | chart 3.9.0 / app v1.19.0, 2026-08-20 (2 days old at research time) | Approved, but **pin chart 3.8.2 / app v1.18.2** (2026-07-10, ~6 weeks old) instead of the 2-day-old latest — see Common Pitfalls |
| cosign (CLI) | `sigstore` | No | v3.1.3, 2026-08-06 | Approved — **pin >= v3.1.3**, fixes GHSA-fx35-mq7g-6g98 (signature-verification bypass in legacy bundles) |
| `sigstore/cosign-installer` (Action) | `sigstore` | No | v4.1.2, 2026-05-07 | Approved |
| `docker/login-action` | `docker` | No | v4.6.0, 2026-07-29 | Approved — new to this repo, needed for both GHCR and Docker Hub auth |
| `docker/build-push-action` | `docker` | No | v7.3.0, 2026-07-01 | Approved — already pinned in CLAUDE.md, confirmed still current |
| `docker/setup-buildx-action` | `docker` | No | v4.3.0, 2026-08-19 | Approved — CLAUDE.md pins v4.2.0, one minor behind; bump when touching this file |
| `actions/delete-package-versions` | `actions` (official GitHub org) | No (pushed 2025-06-06) | v5.0.0, 2024-01-16 (no new tag in ~2.5 years, but org is first-party) | Approved with caveat — stable/narrow-scope tool from a trusted org, not recently re-tagged; verify behavior against the current GHCR API in a scratch run before relying on it in D-11's cleanup job |
| `actions/upload-artifact` | `actions` | No | v7.0.1, 2026-04-10 | Approved |

**Packages removed due to slopcheck verdict:** none (no PyPI/npm packages introduced).
**Flagged as suspicious:** none — every tool above is maintained by a well-known, verifiable org (`kyverno`, `sigstore`, `docker`, `actions`) with an active (non-archived) repository.

## Standard Stack

### Core (new to this phase)
| Tool | Version | Purpose | Why this version |
|---|---|---|---|
| Kyverno | Helm chart `3.8.2` (appVersion `v1.18.2`) | Cluster-wide admission-time cosign signature verification (D-14/D-15) | `ImageValidatingPolicy` reached Stable status at app v1.18 [CITED: kyverno.io/docs/policy-types/overview/]; `v1.18.2` has ~6 weeks of field exposure vs. `v1.19.0`'s 2 days. `ClusterPolicy` (the API CONTEXT.md's decisions assume) is deprecated with removal targeted for v1.20 (~Oct 2026) [CITED: kyverno.io/blog, multiple 1.17/1.18 release-note sources — see Sources]. |
| cosign | CLI `>= 3.1.3` via `sigstore/cosign-installer@<pinned-sha>` (action v4.1.2) | Keyless (OIDC) image signing in `publish.yml`, verified at admission by Kyverno | v3.1.3 (2026-08-06) fixes GHSA-fx35-mq7g-6g98, a verification-bypass CVE in legacy bundle handling [VERIFIED: GitHub Releases API, `sigstore/cosign`]. `COSIGN_EXPERIMENTAL=1` is **not** needed — removed as a requirement since cosign v2; many tutorials still show it (Common Pitfalls). |
| `docker/build-push-action` | `v7.3.0` (pinned by commit SHA per this repo's existing convention) | Buildx-based build+push+SBOM+provenance for GHCR publish | Confirmed current via GitHub Releases API; matches CLAUDE.md's existing pin exactly. |
| `docker/login-action` | `v4.6.0` | Authenticate to `ghcr.io` (GITHUB_TOKEN) and `docker.io` (D-21 rate-limit relief) | New to this repo — this phase's `publish.yml` is the first place either registry login is needed. |
| `docker/setup-buildx-action` | `v4.3.0` | Buildx driver, prerequisite for SBOM/provenance attestations | CLAUDE.md currently pins `v4.2.0` — bump when this file is touched (one minor behind, non-urgent). |

### Supporting
| Tool | Version | Purpose | When to use |
|---|---|---|---|
| `actions/delete-package-versions` | `v5.0.0` | D-11: delete `pr-<number>` GHCR package version on PR close | `pull_request: types: [closed]` trigger; needs `package-type: container`, exact version-id lookup by tag |
| `actions/upload-artifact` | `v7.0.1` | D-23: publish `htmlcov/` as a downloadable CI artifact | After `coverage html` runs |
| `coverage` (Python, already pinned `7.15.4`) | 7.15.4 | `coverage report --format=markdown` for the job-summary table | No new dependency — already in `uv.lock`; `--format=markdown` has been stable since well before this pin |

### Alternatives Considered
| Instead of | Could use | Tradeoff |
|---|---|---|
| `ImageValidatingPolicy` (CEL, `policies.kyverno.io/v1`) | `ClusterPolicy.spec.rules[].verifyImages` (legacy) | Legacy API still functions today but is deprecated NOW and scheduled for removal in ~2 months (v1.20). CONTEXT.md's "Claude's Discretion" explicitly grants latitude on exact policy shape — using the modern API is within that discretion, not a decision reversal. |
| Kyverno as admission enforcer | OPA/Gatekeeper with `ratify` for signature verification | Gatekeeper+ratify is a viable alternative stack, but CONTEXT.md's D-14 names Kyverno specifically and the user confirmed it — not re-litigated here. |
| `actions/delete-package-versions` for D-11 cleanup | Hand-rolled `gh api` pagination script | The GitHub Packages API for listing+matching container versions by tag is non-trivial to paginate correctly by hand; the official action is a reasonable pragmatic choice despite its 2.5-year-old last tag. |
| `gh issue create`/`gh issue comment` scripted in a `run:` step (recommended, D-22) | Third-party marketplace "create-issue" actions | This repo has an established pattern of preferring a direct CLI/binary invocation in a `run:` step over a third-party action wherever one suffices (see `ci.yml`'s gitleaks handling: "Sidestep it entirely — download and run the gitleaks binary"). `gh` is pre-authenticated on every GitHub-hosted runner; no new action/trust surface needed. |

**Installation:** No new Python packages. New GitHub Actions are referenced by pinned commit SHA in workflow YAML (matching this repo's existing `# vX.Y.Z` commented-SHA convention in `ci.yml`). New Helm chart added to `helm/versions.env` as `KYVERNO_CHART_VERSION=3.8.2` (matching every other chart's entry).

**Version verification performed:** every version above was checked against the GitHub Releases API directly (`api.github.com/repos/<org>/<repo>/releases/latest`) at research time (2026-08-22), not inferred from training data.

## Architecture Patterns

### System Architecture Diagram

```
                            ┌─────────────────────────────┐
                            │   GitHub Actions triggers    │
                            └──────────────┬───────────────┘
                                           │
              ┌────────────────────────────┼────────────────────────────┐
              │                            │                            │
       pull_request                  push (main)                GitHub Release
              │                            │                     created
              ▼                            ▼                            ▼
     ┌─────────────────┐        ┌──────────────────┐          ┌──────────────────┐
     │ ci.yml (existing)│        │ publish.yml (D-08)│          │ publish.yml       │
     │ check/manifests/ │        │ build+scan+sign   │          │ semver-tag job     │
     │ integration/     │        │ push :sha AND     │          │ (D-03) — adds      │
     │ secrets          │        │ :pr-<N> (D-09)    │          │ release semver tag │
     └─────────────────┘        └─────────┬─────────┘          │ on top of :sha      │
              │                            │                    └──────────────────┘
              │                            ├──────────────┐
              ▼                            ▼              ▼
     ┌─────────────────┐        ┌──────────────────┐  ┌──────────────────┐
     │ e2e-smoke (D-20) │        │ e2e-full (D-19/24)│  │ e2e-chaos (D-25)  │
     │ trigger: PR      │        │ trigger: merge     │  │ trigger: merge    │
     │ ephemeral kind    │        │ ephemeral kind      │  │ ephemeral kind     │
     │ pulls :pr-<N>     │        │ pulls :sha          │  │ pulls :sha          │
     │ 4 checks (D-20)   │        │ full suite + 2yr    │  │ 11 QUAL-15 failure  │
     │                   │        │ sweep + rebuild-    │  │ scenarios, own      │
     │                   │        │ from-raw LAST (D-30)│  │ cluster (D-25)      │
     └─────────────────┘        └──────────┬─────────┘  └──────────┬─────────┘
                                            │  (parallel, D-27)      │
                                            └───────────┬───────────┘
                                                        ▼
                                            ┌──────────────────────┐
                                            │ on failure (D-22):    │
                                            │ gh issue create/      │
                                            │ comment (idempotent)  │
                                            └──────────────────────┘

Inside EVERY ephemeral-kind cluster (D-17):
  kind cluster up → Kyverno FIRST (see Pitfall: stage order) → CNPG/MinIO/Airflow/
  Vault/monitoring → every pod creation from here on is admission-checked by
  Kyverno's ImageValidatingPolicy (D-15) against the D-16 exception list
```

### Recommended Project Structure
```
.github/workflows/
├── ci.yml                    # existing — untouched (D-06: publish stays independent)
├── publish.yml               # NEW (D-08): build/scan/sign/push, PR + merge + release triggers
├── e2e-smoke.yml or a job     # NEW (D-19/D-20): PR-triggered fast subset
│   inside publish.yml
├── e2e-full.yml or a job      # NEW (D-19/D-24/D-27/D-30): merge-triggered, incl. rebuild-from-raw LAST
├── e2e-chaos.yml              # NEW (D-25/D-26/D-27): QUAL-15, separate ephemeral-kind, parallel to e2e-full
└── ghcr-cleanup.yml           # NEW (D-11): pull_request closed → delete pr-<N> package version

helm/values/{local,ci}/kyverno.yaml   # NEW — chart values (D-17)
<tbd>/kyverno-policies/               # NEW — the actual ImageValidatingPolicy + skipImageReferences
                                       # manifest is NOT part of the Helm chart's own values; needs its
                                       # own committed manifest, applied via a numbered stage script
scripts/stages/25-kyverno.sh          # NEW — deploy EARLY (see Pitfall: stage order), not appended at 90+

docs/runbooks/                        # NEW (D-41) — 18 files, one per verified §89 scenario
docs/adr/0011-raw-immutability-iam-not-worm.md   # OPTIONAL — closes out Phase 2's D-08 revisit note

dags/platform_retention.py            # NEW (D-35)
configs/datasets/*.yaml               # EXTEND — add `retention:` block (D-39) to each existing file

Makefile                              # EXTEND — `rebuild-from-raw` (D-32) target, rollback target (D-12)
helm/versions.env                     # EXTEND — KYVERNO_CHART_VERSION=3.8.2

tests/e2e/chaos/                      # NEW — QUAL-15's 11 scenarios, own pytest marker (mirrors `cluster`)
tests/e2e/cluster/test_kyverno_admission.py   # NEW — D-18 positive+negative, mirrors test_minio_buckets.py
tests/e2e/slice/test_rebuild_from_raw.py      # NEW or added to existing 2yr-sweep module — D-29 proof
tests/unit/test_retention_*.py                # NEW — D-38 dry-run-by-default logic
```

### Pattern 1: Positive+negative live-proof (already established, reuse the shape)
**What:** Every access-control-shaped claim in this codebase is proven with a test that (a) shows the legitimate path succeeds and (b) shows the illegitimate path is denied, with an explicit assertion the denied attempt did NOT silently succeed.
**When to use:** D-18 (Kyverno signed/unsigned), D-20's Vault check (already exists), and by extension D-40's MinIO deny-delete (already exists).
**Example — the exact template to mirror for D-18, already in the repo:**
```python
# Source: tests/e2e/cluster/test_minio_buckets.py (this repo, already merged)
def test_raw_delete_is_denied_for_app_credential(s3_client):
    """The negative case: the pipeline's own credential cannot delete from raw."""
    app = s3_client("app"); admin = s3_client("admin")
    bucket, key = "raw", "e2e/minio-buckets/deny-delete.txt"
    try:
        app.put_object(Bucket=bucket, Key=key, Body=b"must survive a delete attempt")
        with pytest.raises(ClientError) as exc_info:
            app.delete_object(Bucket=bucket, Key=key)
        assert exc_info.value.response["Error"]["Code"] == "AccessDenied"
        # Prove it wasn't deleted anyway, not just that an error was raised.
        assert app.get_object(Bucket=bucket, Key=key)["Body"].read() == b"must survive a delete attempt"
    finally:
        admin.delete_object(Bucket=bucket, Key=key)

def test_raw_delete_is_permitted_for_admin_credential(s3_client):
    """The positive control: a policy denying everyone is as wrong as denying nobody."""
    ...
```
For D-18, the same shape becomes: deploy a Pod referencing a cosign-signed project image (expect: created), then deploy a Pod referencing an unsigned public image not on the D-16 exception list (expect: `kubectl` / API call raises, admission webhook denial, Pod never created) — mirror `tests/e2e/vault/test_positive_auth.py` / `test_negative_auth.py`'s file-split convention too.

### Pattern 2: Two build paths for images (new in this phase)
**What:** The Makefile's existing `image-csv-processor`/`image-dbt`/`image-airflow` targets use plain `docker build` against the local registry (`localhost:5001`) — verified by reading `docker/csv-processor/Dockerfile` and the Makefile. Plain `docker build` cannot emit `--sbom`/`--provenance` attestations.
**When to use:** `publish.yml` needs its own `docker/build-push-action` step against the SAME Dockerfiles, pointed at GHCR — it is a parallel path, not a refactor of the Make targets (which continue to serve local/dev iteration against the local registry).
**Example:**
```yaml
# Source: composed from docker/build-push-action's documented provenance/sbom
# inputs [CITED: github.com/docker/build-push-action] + this repo's own
# Dockerfile build-arg convention (docker/csv-processor/Dockerfile).
permissions:
  contents: read
  packages: write
  id-token: write   # required for cosign keyless signing later in the job

steps:
  - uses: docker/setup-buildx-action@<pin-by-sha>   # v4.3.0
  - uses: docker/login-action@<pin-by-sha>          # v4.6.0
    with:
      registry: ghcr.io
      username: ${{ github.actor }}
      password: ${{ secrets.GITHUB_TOKEN }}
  - id: build
    uses: docker/build-push-action@<pin-by-sha>     # v7.3.0
    with:
      context: .
      file: docker/csv-processor/Dockerfile
      push: true
      tags: ghcr.io/${{ github.repository_owner }}/csv-processor:${{ github.sha }}
      provenance: true    # shorthand for --attest=type=provenance
      sbom: true          # shorthand for --attest=type=sbom
      build-args: GIT_SHA=${{ github.sha }}
```

### Pattern 3: Kyverno ImageValidatingPolicy with an upstream exception list (D-14/D-16)
**What:** The current (non-deprecated) way to express "verify everything except this pinned list."
**When to use:** The single cluster-wide policy D-15 asks for.
**Example:**
```yaml
# Source: composed from kyverno.io/docs/policy-types/image-validating-policy/
# [CITED] + kyverno.io/docs/policy-types/cluster-policy/verify-images/sigstore/
# [CITED] for the keyless identity shape, + docs.sigstore.dev / GitHub Actions
# OIDC token documentation for the issuer/subject values [CITED].
apiVersion: policies.kyverno.io/v1
kind: ImageValidatingPolicy
metadata:
  name: verify-signed-images
spec:
  validationActions: [Enforce]
  webhookConfiguration:
    failurePolicy: Fail
    timeoutSeconds: 15
  matchConstraints:
    resourceRules:
      - apiGroups: ['']
        apiVersions: ['v1']
        resources: ['pods']
        operations: ['CREATE', 'UPDATE']
  matchImageReferences:
    - glob: '*'
  skipImageReferences:
    # D-16: exact pinned upstream references already committed in
    # helm/values/*.yaml — one glob per pinned third-party image. Keep this
    # list and helm/versions.env in sync (a policy test should assert this,
    # mirroring test_supply_chain_guards.py's existing image-tag-agreement
    # checks).
    - glob: 'apache/airflow:3.3.0-python3.12'
    - glob: 'pgsty/minio:RELEASE.2026-08-04T00-00-00Z*'
    - glob: 'hashicorp/vault:2.0.3'
    - glob: 'ghcr.io/cloudnative-pg/cloudnative-pg:*'
    - glob: 'quay.io/prometheus*'
    # ... one entry per pinned image actually present in helm/values/*.yaml
  attestors:
    - name: cosign
      cosign:
        keyless:
          identities:
            # NOTE: must match BOTH the merge-triggered and the PR-triggered
            # OIDC subject shapes, or PR-tagged images (D-09) are denied by
            # the very policy meant to admit them (see Common Pitfalls).
            - issuer: 'https://token.actions.githubusercontent.com'
              subjectRegExp: '^https://github\.com/<owner>/<repo>/\.github/workflows/publish\.yml@refs/(heads/main|pull/[0-9]+/merge)$'
        ctlog:
          url: 'https://rekor.sigstore.dev'
  validations:
    - expression: >-
        images.containers.map(image,
        verifyImageSignatures(image, [attestors.cosign])).all(e, e > 0)
      message: 'Image is not signed by this repository''s publish workflow, and is not on the upstream exception list.'
```

### Anti-Patterns to Avoid
- **Writing the D-14 policy as `ClusterPolicy.spec.rules[].verifyImages`:** functions today but is deprecated and scheduled for removal in Kyverno v1.20 (~Oct 2026), shortly after this phase would ship. Use `ImageValidatingPolicy` instead (Claude's Discretion in CONTEXT.md already permits this).
- **Assuming Kyverno is "one controller pod, ~100-200m CPU":** the modern chart installs four Deployments (`admissionController`, `backgroundController`, `cleanupController`, `reportsController`) at `100m` CPU request each by default — see Common Pitfalls for the exact numbers and a trimming recommendation.
- **Deploying Kyverno last in the stage sequence:** if Kyverno comes online after Airflow/MinIO/Vault/CNPG/monitoring pods already exist, D-16's exception list is never actually exercised on a normal cluster build (admission control only fires on new Pod CREATE/UPDATE events) — see Common Pitfalls.
- **Signing only merge-tagged images, not `pr-<number>` images:** breaks D-20's PR smoke test once Kyverno enforcement (D-17) is live in the CI profile — see Common Pitfalls.
- **Re-implementing the MinIO raw-immutability IAM policy:** it already exists (`helm/values/{local,ci}/minio.yaml`, `etl-app` policy's `Deny` statement) and already has a live positive+negative test (`tests/e2e/cluster/test_minio_buckets.py`). Verify, do not rebuild.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Image signature verification at admission time | A custom admission webhook | Kyverno `ImageValidatingPolicy` + cosign | Webhook TLS cert lifecycle, CEL evaluation, and Rekor transparency-log lookups are exactly the kind of "deceptively complex" plumbing this project's own `.claude/CLAUDE.md` philosophy (§88 Security) already warns against reinventing |
| SBOM generation | A custom dependency-walker script | `docker/build-push-action`'s `sbom: true` (buildx native `--attest=type=sbom`, backed by Syft) | Already integrated into the existing build tool; hand-rolling would duplicate a solved, standardized (SPDX/CycloneDX) problem |
| Keyless signing key management | Any key-management scheme (KMS, GPG, long-lived secrets) | cosign keyless + GitHub OIDC (Fulcio/Rekor) | The entire point of D-13 choosing keyless is to have ZERO keys to manage/rotate; hand-rolling defeats the decision |
| GHCR package-version pagination/deletion | A bespoke `gh api` loop | `actions/delete-package-versions` (official `actions` org) | Listing + filtering + deleting versions correctly (pagination, matching by tag, avoiding deleting the wrong version) is non-trivial to get right by hand |
| Table-level content hashing for reconciliation | A new hashing scheme for D-29 | The existing `_table_checksum()` in `packages/dataplat/src/dataplat/pipeline/run.py` (order-independent `bit_xor(md5(...))`), extended with an explicit column list | The commutative-hash design is already correct and already used for the exact same purpose (D-21, Phase 9/10); only the column selection needs to change, not the algorithm |

**Key insight:** every "don't hand-roll" item in this phase is either an already-solved problem in the Kyverno/cosign/Sigstore ecosystem (signing, verification, SBOM), or an already-solved problem *in this specific codebase* (content hashing, positive/negative live-proof tests, per-environment Helm profiles). The research risk here is not "will I build something worse than a library" — it's "will I fail to notice the codebase already solved this in Phase 2/9/10 and duplicate it."

## Common Pitfalls

### Pitfall 1: D-40 is already fully implemented — do not rebuild it
**What goes wrong:** A plan writes tasks to "add an IAM policy denying delete on raw" as new Phase 11 work, duplicating or conflicting with existing configuration.
**Why it happens:** CONTEXT.md's D-40 framing ("Resolution: enforce raw immutability via an IAM bucket policy...") reads like a forward-looking decision, and the `helm/values/local/minio.yaml` comment literally says "Phase 11 ... is the named place to revisit it."
**How to avoid:** `helm/values/{local,ci}/minio.yaml`'s `etl-app` policy already contains the exact statement:
```yaml
- effect: Deny
  resources: ["arn:aws:s3:::raw/*"]
  actions: ["s3:DeleteObject", "s3:DeleteObjectVersion"]
```
committed in Phase 2 (`c600905 feat(02-04): MinIO with five buckets, versioning on raw, deny-delete IAM policy`). A live positive+negative proof test already exists: `tests/e2e/cluster/test_minio_buckets.py::test_raw_delete_is_denied_for_app_credential` and `::test_raw_delete_is_permitted_for_admin_credential`. Phase 11's real remaining work for D-40 is at most: (a) update the stale "Phase 11 is the named place to revisit it" comment in `minio.yaml` to note it was resolved in Phase 2, and (b) optionally write `docs/adr/0011-...md` closing out the decision for the record (next ADR number after `0010-dbt-silver-layer-boundary.md`).
**Warning signs:** Any task description that says "create the IAM policy" rather than "verify the IAM policy" for D-40.

### Pitfall 2: Kyverno's ClusterPolicy is deprecated NOW, not "eventually"
**What goes wrong:** Building D-14 on `ClusterPolicy.spec.rules[].verifyImages` because that's the shape CONTEXT.md's decisions describe and most existing tutorials still show.
**Why it happens:** `ClusterPolicy` was Kyverno's only policy type for years; most blog content and even some current search results default to it.
**How to avoid:** Kyverno marked `ClusterPolicy`/`CleanupPolicy` Deprecated and is targeting removal in v1.20 (~October 2026) [CITED: kyverno.io/blog/2026/04/24/announcing-kyverno-release-1.18, corroborated by multiple 1.17/1.18 release-note summaries — see Sources]. The replacement, `ImageValidatingPolicy` (`policies.kyverno.io/v1`), reached Stable status by chart 3.8.0/app v1.18.0 [CITED: kyverno.io/docs/policy-types/overview/]. Use `ImageValidatingPolicy`; CONTEXT.md's "Claude's Discretion" note on "exact ClusterPolicy resource shape/naming" already grants latitude to do this.
**Warning signs:** Any manifest with `apiVersion: kyverno.io/v1, kind: ClusterPolicy` and a `verifyImages:` rule.

### Pitfall 3: Kyverno's real resource footprint is ~2-4x what D-17 assumed
**What goes wrong:** Values files sized for "~100-200m CPU, one controller pod" (D-17's own estimate) silently push the CI profile over its 3.2-effective-core budget (`tests/policy/test_manifest_resources.py`'s `EFFECTIVE_CI_CPU_BUDGET`, 4 cores less 20% headroom), and the existing dynamic-summing test (`test_ci_profile_fits_runner`) fails the build.
**Why it happens:** The modern Kyverno chart (3.x) installs FOUR separate Deployments by default, each with its own `resources.requests`: `admissionController` (100m CPU / 128Mi mem, the one CONTEXT.md was likely thinking of), `backgroundController` (100m / 64Mi), `cleanupController` (100m / 64Mi), `reportsController` (100m / 64Mi) [VERIFIED: raw `values.yaml` for chart 3.8.2, `raw.githubusercontent.com/kyverno/kyverno`]. Minimum steady-state total at replicas=1 each: ~400m CPU / ~320Mi memory — before any replica-count-for-HA multiplier.
**How to avoid:** This project has no use for `cleanupController` (no `CleanupPolicy`/`ClusterCleanupPolicy` resources are planned) — set `cleanupController.enabled: false` in both profiles. Consider also disabling or minimally sizing `backgroundController`/`reportsController` in the CI profile specifically (this project's use case is pure admission-time enforcement, not background policy-report generation), mirroring the already-established `260817-rvq` precedent of trimming the monitoring stack's CPU requests down to measured actual usage. Run `make manifests && REQUIRE_RENDERED_MANIFESTS=1 uv run pytest tests/policy/test_manifest_resources.py -k ci_profile_fits_runner -q` early (Wave 0) to measure real headroom before finalizing Kyverno's CI values.
**Warning signs:** `test_ci_profile_fits_runner` failing after adding `helm/values/ci/kyverno.yaml`.

### Pitfall 4: Kyverno deployed late in the stage sequence never actually exercises D-16's exception list
**What goes wrong:** Adding Kyverno as `scripts/stages/90-kyverno.sh` (after the existing `85-monitoring.sh`) means every other component's pods (Airflow, MinIO, Vault, CNPG, monitoring) are already running BEFORE the admission webhook exists on a freshly-built cluster.
**Why it happens:** Admission control only fires on Pod CREATE/UPDATE events; it does not retroactively scan already-running pods. The numbered-stage pattern (`10-registry.sh` ... `85-monitoring.sh`) naturally invites appending a new stage at the end.
**How to avoid:** Deploy Kyverno EARLY — e.g. `scripts/stages/25-kyverno.sh`, right after `20-namespaces.sh` and before `30-ingress-nginx.sh` — so that on every fresh `cluster-up` (local) or ephemeral-kind build (CI), every subsequent component's pods genuinely pass through the admission webhook, and D-16's exception list is exercised for real on every build, not just in a synthetic D-18 test. This also gives D-17's "the ephemeral kind cluster... needs to prove the complete stack, including the admission policy, actually boots" success criterion its strongest possible form.
**Warning signs:** D-18's live-proof test passing while a normal `make cluster-up` shows zero `AdmissionReport`/policy-evaluation activity for the platform's own core components.

### Pitfall 5: PR-tagged images must be signed too, or the PR smoke test becomes self-defeating
**What goes wrong:** D-13 says "every published image" gets SBOM+cosign signing; D-09 separately says PR builds ARE published (pushed to GHCR as `pr-<number>`). If the signing step is only wired for the merge-triggered path, `pr-<number>` images are unsigned. Once D-17 puts Kyverno in the CI profile and D-20 requires the PR smoke subset to reach a real DAG `SUCCEEDED`, the PR's own `csv-processor`/`dbt`/airflow image — unsigned — is DENIED by the very cluster it needs to run in, and every PR's smoke job fails with what looks like an unrelated pod-scheduling problem.
**Why it happens:** CONTEXT.md's decisions were made independently (image publishing, Kyverno enforcement, PR smoke scope) and their interaction was never traced end-to-end.
**How to avoid:** Sign (and SBOM) the `pr-<number>` tag exactly like the merge-tagged image, in the same `publish.yml` job, before the PR smoke test runs. Additionally, the Kyverno policy's identity matcher must accept BOTH the merge-triggered OIDC subject (`.../publish.yml@refs/heads/main`) and the PR-triggered subject (`.../publish.yml@refs/pull/<N>/merge`) — use `subjectRegExp`, not an exact `subject` string (see Code Examples, Pattern 3).
**Warning signs:** PR smoke tests failing with pod `ImagePullBackOff`-adjacent or generic scheduling-looking errors that are actually admission webhook denials — check `kubectl get events` for `Warning ... admission webhook "..." denied the request` before assuming it's a registry/pull issue.

### Pitfall 6: `pull_request` from a fork cannot get `packages: write`, no matter what the workflow YAML requests
**What goes wrong:** D-09's `pr-<number>` GHCR push silently fails (or the whole job fails on the push step) for any PR that originates from a fork, not a branch in this same repository.
**Why it happens:** GitHub deliberately restricts `GITHUB_TOKEN` to read-only on `pull_request`-triggered runs when the PR's head repository differs from the base repository — this is a hard platform security boundary, not something the workflow's `permissions:` block can override [CITED: GitHub Community discussions, corroborated across multiple independent threads — see Sources]. `pull_request_target` (which does get write access) is explicitly the WRONG choice here and is already correctly avoided by this repo's existing `ci.yml` comment ("never `pull_request_target`... would expose repository secrets to a fork").
**How to avoid:** Given this repository currently has a single contributor and direct-push workflow (per project memory / git log), this is a low-probability, low-impact gap today. Document it explicitly rather than silently ignoring it: the `publish.yml` PR-image-push step should either check `github.event.pull_request.head.repo.full_name == github.repository` and skip/report cleanly when false, or the plan should note this as a known limitation revisited if/when external contributions are accepted.
**Warning signs:** A PR from a fork producing a confusing 403 on the GHCR push step.

### Pitfall 7: `_table_checksum()` hashes the whole row, including columns a rebuild deliberately changes
**What goes wrong:** D-29's "reuse the existing mechanism" is read as "call `_table_checksum()` unchanged before the drop and after the rebuild, compare the two hashes" — which will ALWAYS report a mismatch, even for a byte-perfect rebuild, because the hash includes `_run_id`, `_file_id`, `_batch_id` (new identity values by design, per D-29's own text) and `_ingested_at`/`_dbt_loaded_at` (fresh wall-clock timestamps).
**Why it happens:** `_table_checksum(conn, table)` (`packages/dataplat/src/dataplat/pipeline/run.py:301`) computes `md5(t::text)` over `SELECT * FROM {table}` — every column, unconditionally — because its one existing caller (`_compute_silver_gold_reconciliation`, for the D-20/D-21/D-22 per-run reconciliation) never needed to exclude anything: it compares silver vs. gold within the SAME run, where identity columns are expected to differ anyway (silver's own `_run_id` doesn't need to equal gold's).
**How to avoid:** Add a column-list-aware variant (or an optional `columns: Sequence[str] | None` parameter) that hashes only named business columns, e.g. `SELECT to_hex(bit_xor(...md5((col_a, col_b, ...)::text)...)) FROM (SELECT col_a, col_b, ... FROM {table}) t` instead of `SELECT * FROM {table} t`. The six embedded lineage columns to exclude are documented verbatim in `.planning/research/ARCHITECTURE.md` §2.3: `_run_id`, `_file_id`, `_batch_id`, `_source_row_number`, `_ingested_at` (plus `_dbt_loaded_at` on silver tables). Note `_record_hash`/`_record_hash_version` do NOT need exclusion — they are deterministic functions of business data (this is what QUAL-16's determinism property test already asserts) and SHOULD match across a rebuild; including them is actually a useful extra determinism check, not noise.
**Warning signs:** D-29's checksum comparison failing on every single rebuild run, including ones where row counts and SCD2 state match perfectly.

### Pitfall 8: `record_reconciliation`'s grain doesn't give you a pre/post snapshot comparison "for free"
**What goes wrong:** Reading D-29 point 4 ("the comparison reuses the existing `meta.reconciliation_results`/`record_reconciliation` mechanism") as "no new code is needed at all."
**Why it happens:** `record_reconciliation` (`packages/dataplat/src/dataplat/metadata/repository.py:990`) writes ONE row per `(file_id, hop)` INSIDE a single processing pass's own transaction — it answers "did this file's silver→gold publish lose rows," not "does table T today equal table T as it stood before the drop." It IS naturally re-exercised during the rebuild (every reprocessed file gets its own fresh reconciliation row, exactly like any real ingestion run), which legitimately satisfies "don't build a bespoke per-file accounting path" — but it does not by itself answer D-29's points 1-3 (whole-table row counts, whole-table content hash, SCD2 version/state comparison across the drop).
**How to avoid:** Plan for a small, explicitly-new comparison routine/test (using `_table_checksum`'s corrected variant from Pitfall 7, plus `SELECT count(*)`/SCD2-state queries) that runs the pre-drop snapshot and post-rebuild comparison — while still leaning on `record_reconciliation` for what it actually does well (per-file accounting during the rebuild's own backfill, which requires zero new code since it's the same code path every ingestion run already takes).
**Warning signs:** A plan task that says "no new SQL needed for D-29" without identifying where the pre-drop snapshot values get stored/passed to the post-rebuild comparison.

### Pitfall 9: Copying the existing 15-minute CI job timeout for the new E2E/chaos jobs
**What goes wrong:** Every existing `ci.yml` job uses `timeout-minutes: 15` (they're all genuinely fast: lint/typecheck, offline manifest render, testcontainers integration, a ~150ms gitleaks scan). The new full-E2E job (kind cluster boot + N Helm chart installs + the full local e2e suite + the 2-year backfill sweep + rebuild-from-raw running a SECOND full historical reprocessing pass, D-30) and the chaos job are categorically different in scale.
**Why it happens:** Copy-paste from the existing, well-tested job template is the path of least resistance.
**How to avoid:** A single already-measured data point: `tests/e2e/observability/` alone (7 tests, on an ALREADY-RUNNING, ALREADY-WARM cluster) took 491 seconds (~8 minutes) [per `.planning/STATE.md`'s own resolved-incident log]. The full local suite (`tests/e2e/cluster` + `tests/e2e/slice` incl. the 2-year sweep + `tests/e2e/observability`) plus kind cluster creation plus 6+ Helm chart installs plus a SECOND historical reprocessing pass for rebuild-from-raw is a much larger number — plausibly 45-90+ minutes. Set `timeout-minutes` generously (e.g. 90-120) for `e2e-full`/`e2e-chaos` as a starting point, and tune down based on the first few real CI runs' observed wall-clock time — do not guess a tight number up front.
**Warning signs:** The new E2E job getting killed by GitHub Actions mid-suite with no application-level error.

### Pitfall 10: `COSIGN_EXPERIMENTAL=1` is a stale-tutorial trap
**What goes wrong:** Copying an older cosign keyless-signing tutorial that sets `COSIGN_EXPERIMENTAL=1`.
**Why it happens:** This env var was required for keyless signing in cosign v1.x; it has not been required since cosign v2.0 (keyless is the default, GA behavior) [CITED: sigstore/docs issue #440, "Remove obsolete COSIGN_EXPERIMENTAL=1... keyless is default since cosign v2"]. Many still-indexed tutorials predate this.
**How to avoid:** Do not set this variable in `publish.yml`. Its presence in a new workflow is itself a signal the pattern was copied from stale material.

### Pitfall 11: README §84/§89 item counts in CONTEXT.md are off by one each — use the counts below
**What goes wrong:** Planning exactly 20 chaos-test cases or exactly 17 runbook docs because CONTEXT.md's canonical_refs describes §84 as "20-item" and §89 as "17-item."
**Why it happens:** A miscount somewhere upstream of CONTEXT.md's own drafting — verified directly against the live `README.md` file at research time.
**How to avoid:** §84 (Failure Scenarios) literally lists **21** items; §89 (Operational Runbook) literally lists **18** items — see "README §84 vs §89" below for the full verified lists and the exact 11-item QUAL-15 subset (which DOES match CONTEXT.md's "11 of them" claim exactly — only the parent list totals are off).
**Warning signs:** A runbook directory with exactly 17 files, silently missing one §89 scenario.

## README §84 vs §89 — Verified Lists

Both lists were re-read directly from `README.md` at research time (`sed -n` over the exact line ranges) to eliminate transcription risk.

### §84 Failure Scenarios — 21 items (not 20)
Pod crashes · Database unavailable · MinIO unavailable · Vault unavailable · Malformed CSV · Invalid encoding · **Invalid schema** · **Invalid row** · **Network failure** · Task timeout · Out-of-memory · **Airflow retry** · **Duplicate file** · Duplicate batch · **Schema evolution** · **CDC ordering issue** · **Late-arriving data** · **Partial database load** · **Secret unavailable** · Unauthorized secret access · Secret rotation

**QUAL-15's exact subset is 11 items** (verified against the literal REQUIREMENTS.md text, matches CONTEXT.md's "11 of them" claim precisely): pod crash, database unavailable, MinIO unavailable, Vault unavailable, malformed CSV, invalid encoding, OOM, task timeout, duplicate batch, secret rotation, unauthorized secret access.

The 10 bolded-above items are in §84 but NOT in QUAL-15's tested subset — most are already covered by requirements from earlier, completed phases (invalid schema/row → SCHEMA-04/05, VALID-01/02, Phase 6/8; network failure → not separately named elsewhere, likely folds into "database/MinIO/Vault unavailable" transport-level handling; Airflow retry → LOAD-02, Phase 4; duplicate file → LOAD-03, Phase 4; schema evolution → QUAL-12, Phase 6; CDC ordering issue → **out of scope entirely**, CDC was dropped from v1 2026-08-21; late-arriving data → INCR-03/04, Phase 08.1; partial database load → LOAD-06, Phase 9 — see below; secret unavailable → distinct from "Vault unavailable," not separately named as a QUAL-15 target). Recommendation: `tests/e2e/chaos/` should contain exactly the 11 QUAL-15 scenarios; do not scope-creep into testing all 21 (that is a different, much larger phase's worth of work already partially discharged elsewhere).

### §89 Operational Runbook — 18 items (not 17)
Airflow unavailable · MinIO unavailable · PostgreSQL unavailable · Vault unavailable · Kubernetes pod stuck · CSV malformed · Schema changed · Duplicate batch · Failed backfill · Late-arriving data · CDC failure · SCD correction · Corrupted file · Task repeatedly failing · Partial database load · Secret unavailable · Secret rotation · Unauthorized access

**Source mapping for D-41** (which of the three D-41 provenance modes — real incident, chaos-test-derived, or existing-feature documentation — applies to each):

| # | Scenario | Source to write from |
|---|----------|----------------------|
| 1 | Airflow unavailable | `.planning/debug/resolved/dagrun-scheduler-stall.md` (real incident: DAGs hostPath mount break, cluster-wide freeze) |
| 2 | MinIO unavailable | Chaos test (D-25) — never actually hit as a real incident |
| 3 | PostgreSQL unavailable | Chaos test (D-25) |
| 4 | Vault unavailable | `.planning/debug/resolved/wait-for-files-stuck-task.md` (real incident: Vault reseal after host restart) |
| 5 | Kubernetes pod stuck | `.planning/debug/resolved/airflow-scheduler-stuck-tasks.md` (real incident: CPU exhaustion + xcom-sidecar leak) |
| 6 | CSV malformed | Existing feature (VALID-01, Phase 8) — document current behavior, no incident/chaos test needed |
| 7 | Schema changed | Existing feature (SCHEMA-04/05, Phase 6) |
| 8 | Duplicate batch | Existing feature (LOAD-08 batch ledger, Phase 4) or chaos test overlap with QUAL-15 |
| 9 | Failed backfill | `.planning/debug/resolved/backfill-does-not-redrive-rejected-row.md` (real incident) + existing INCR-06 (Phase 9) retry mechanics |
| 10 | Late-arriving data | Existing feature (INCR-03/04, Phase 08.1) |
| 11 | **CDC failure** | **No real subject exists** — CDC was dropped from v1 entirely (REQUIREMENTS.md "Out of Scope": DoD 44/45/46/87, dropped 2026-08-21). See Open Questions. |
| 12 | SCD correction | Existing feature (SCD-07 late-arriving correction, Phase 10) — not a failure mode, an operational how-to |
| 13 | Corrupted file | Existing feature (LOAD-10 integrity verification, Phase 8) |
| 14 | Task repeatedly failing | `.planning/debug/resolved/airflow-scheduler-stuck-tasks.md` again, or ORCH-04 retry/failure semantics |
| 15 | Partial database load | **`meta.v_run_recovery`** — a real, already-built view answering "what succeeded/remains, retry stage X" (Phase 9, plan 09-06; see `tests/integration/test_run_recovery_view.py`). REQUIREMENTS.md's traceability table shows LOAD-06 as "Pending," but this is stale documentation (last mechanically updated 2026-08-11, before Phase 9 executed) — verified directly against `09-VALIDATION.md` that the view and its tests exist. |
| 16 | Secret unavailable | Chaos test (D-25) — distinct scenario from "Vault unavailable" (a specific path missing vs. Vault itself being down) |
| 17 | Secret rotation | Existing feature (SEC-09, documented rotation procedure, Phase 5) + chaos test |
| 18 | Unauthorized access | Existing feature (SEC-12 positive/negative test pattern, Phase 5) |

Not all 18 come from "a chaos test" — several (6, 7, 8, 10, 12, 13, 15, 17, 18) are best written as "how to operate an already-built feature," not "what we observed when we broke something." D-41's own text anticipates this ("Scenarios with a matching real incident... are written from that incident"; only "scenarios never actually hit" need to trail the chaos suite) — this table just makes the split concrete per scenario.

## Runtime State Inventory

Not applicable — this phase is additive (new CI workflows, new Kyverno admission controller, new runbooks, new retention DAG) rather than a rename/refactor/migration. The one state-mutating new capability (`rebuild-from-raw`, D-32) is itself the subject of D-28..D-34's own detailed semantics above, not a hidden state-migration concern.

## Code Examples

### Coverage reporting via job summary (D-23)
```yaml
# Source: composed from coverage.py's documented `report --format=markdown`
# [CITED: coverage.readthedocs.io/en/7.15.4/commands/cmd_report.html] +
# GitHub's own $GITHUB_STEP_SUMMARY mechanism [CITED: docs.github.com].
# No new Python dependency — coverage 7.15.4 is already in uv.lock.
- run: uv run pytest tests/unit tests/regression -q --cov --cov-report=html --cov-report=xml
- name: Coverage summary
  run: |
    echo "## Coverage Report" >> "$GITHUB_STEP_SUMMARY"
    uv run coverage report --format=markdown >> "$GITHUB_STEP_SUMMARY"
- uses: actions/upload-artifact@<pin-by-sha>   # v7.0.1
  with:
    name: coverage-html
    path: htmlcov/
```
Note: this repo's `pyproject.toml` deliberately has no `fail_under` ("a threshold over a tree that is mostly configuration is a number people game," and explicitly anticipates CICD-05 landing in Phase 11) — do not introduce a numeric coverage gate as part of this work; CICD-05 asks for reporting, not enforcement.

### Idempotent GitHub issue on CI failure (D-22)
```yaml
# Source: composed from GitHub CLI's documented `gh issue` subcommands
# [CITED: docs.github.com/actions/security-guides/automatic-token-authentication],
# following this repo's own established preference for a direct CLI
# invocation over a third-party marketplace action (see ci.yml's gitleaks
# handling for the precedent).
permissions:
  issues: write
steps:
  - name: File or update failure issue
    if: failure()
    env:
      GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
    run: |
      set -euo pipefail
      TITLE_PREFIX="CI failure on main"
      EXISTING=$(gh issue list --repo "${{ github.repository }}" \
        --search "in:title \"$TITLE_PREFIX\"" --state open \
        --json number --jq '.[0].number // empty')
      BODY="Workflow run: ${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}
      Commit: ${{ github.sha }}"
      if [ -n "$EXISTING" ]; then
        gh issue comment "$EXISTING" --repo "${{ github.repository }}" --body "$BODY"
      else
        gh issue create --repo "${{ github.repository }}" \
          --title "$TITLE_PREFIX: ${{ github.sha }}" --body "$BODY"
      fi
```

### `_table_checksum` extended for D-29 (illustrative — exact implementation is a plan-time task)
```python
# Source: extends the existing function at
# packages/dataplat/src/dataplat/pipeline/run.py:301 (this repo, already
# merged). The existing signature/behavior for the D-20/D-21/D-22 caller
# must NOT change — add a new optional parameter or a new sibling function,
# never silently change what _compute_silver_gold_reconciliation gets.
def _table_checksum(
    conn: Connection[Any],
    table: str,
    *,
    columns: Sequence[str] | None = None,
) -> str | None:
    """Order-independent aggregate hash over `table` (D-21), optionally
    scoped to `columns` only — D-29's rebuild comparison passes the
    business-column list, excluding _run_id/_file_id/_batch_id/
    _ingested_at/_dbt_loaded_at, which a rebuild deliberately re-mints.
    """
    if columns is not None:
        column_list = ", ".join(columns)  # config-resolved identifiers only
        source = f"(SELECT {column_list} FROM {table}) t"
    else:
        source = f"{table} t"
    query = (
        f"SELECT to_hex(bit_xor(('x' || substr(md5(t::text), 1, 16))::bit(64)::bigint)) "
        f"FROM {source}"
    )
    result = _scalar(conn, query)
    return None if result is None else str(result)
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|---|---|---|---|
| Kyverno `ClusterPolicy.spec.rules[].verifyImages` | Kyverno `ImageValidatingPolicy` (`policies.kyverno.io/v1`, CEL-based) | Introduced v1.14 (Apr 2025), Stable since v1.18 (Apr 2026); `ClusterPolicy` deprecated ~v1.17, removal targeted v1.20 (~Oct 2026) | Any Phase 11 policy written against `ClusterPolicy` has a ~2-month shelf life before the API is removed outright |
| `COSIGN_EXPERIMENTAL=1` for keyless signing | No env var needed — keyless is default/GA | cosign v2.0 | Stale tutorials still show the old var; harmless if set, but a signal of copied stale material |
| cosign `< 3.1.3` / `< 2.6.5` | cosign `>= 3.1.3` (or `>= 2.6.5` on the 2.x line) | 2026-08-06 | GHSA-fx35-mq7g-6g98 (verification bypass via unexpected public keys in legacy bundles) fixed |

**Deprecated/outdated:**
- Kyverno `ClusterPolicy`/`CleanupPolicy`: deprecated, targeted for removal in v1.20.
- `COSIGN_EXPERIMENTAL=1`: vestigial, ignorable if present but should not be newly added.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | The exact Kyverno release timeline (1.17 marking ClusterPolicy deprecated vs. a slightly different minor) varies slightly between two independently fetched sources; the removal target (v1.20, ~Oct 2026) and ImageValidatingPolicy's Stable-since-v1.18 status were corroborated across multiple sources | Common Pitfalls #2, Standard Stack | Low — the actionable conclusion (use ImageValidatingPolicy, avoid ClusterPolicy) holds regardless of which exact minor first marked it deprecated |
| A2 | The recommended chart pin (3.8.2 / app v1.18.2) follows this project's own established "avoid brand-new releases" pattern (e.g. CNPG over a 3-week-old Zalando major) applied by analogy to Kyverno's 2-day-old v1.19.0 — this is a judgment call, not a hard requirement from any source | Standard Stack, Package Legitimacy Audit | Low-Medium — if the planner prefers the newest release regardless, v1.19.0/chart 3.9.0 also has ImageValidatingPolicy at Stable status; the only loss is field-exposure time |
| A3 | The recommendation to disable `cleanupController` and trim `backgroundController`/`reportsController` for the CI profile is inferred from this project's demonstrated resource-trimming pattern (`260817-rvq`), not from any Kyverno-specific guidance found | Common Pitfalls #3 | Low — worst case, the existing dynamic CI-budget test simply fails loud and forces the same trimming decision at implementation time |
| A4 | Kyverno self-manages its own admission-webhook TLS certificates without requiring `cert-manager` as an additional dependency | Don't Hand-Roll, Architecture Patterns | Medium if wrong — would add an undiscovered new chart dependency; verify against the chart's `values.yaml` `certificates:`/`admissionController.certificates` keys before finalizing the values file (not fully verified in this research pass — only inferred from general Kyverno architecture knowledge and the absence of a cert-manager reference in the values.yaml sections actually inspected) |
| A5 | `actions/delete-package-versions` v5.0.0 (tagged 2024-01-16) still functions correctly against the current GitHub Packages API | Package Legitimacy Audit, Standard Stack | Medium — recommend a scratch/manual verification run before relying on it in the D-11 cleanup job, since no functional test of this specific action was performed in this research pass |

**If this table is empty:** N/A — see entries above.

## Open Questions

1. **What does the "CDC failure" runbook (§89 item 11) actually document, given CDC is entirely out of scope for v1?**
   - What we know: REQUIREMENTS.md's "Out of Scope" section confirms CDC (DoD 44/45/46/87) was dropped from v1 on 2026-08-21; no CDC `Source` is implemented; the `Source`/`Publisher` seam (Phase 3 ADR) "still admits a CDC Source later without redesign."
   - What's unclear: Whether D-41 expects a full runbook here at all, or a short "not applicable, here's why, here's the seam that would carry it" stub.
   - Recommendation: Write the short stub. Silently omitting the file is worse for the Core Value's own "traceable, explained" standard than an honest one-paragraph "not applicable" entry with a pointer to the architectural seam. Treat this as a documentation decision the planner can make directly rather than something requiring a round-trip to the user.

2. **Does the Kyverno chart require `cert-manager` or fully self-manage webhook certs?**
   - What we know: The chart's `values.yaml` structure (admissionController container/initContainer/resources) was inspected in detail; no `cert-manager` dependency was observed in the sections read, and Kyverno has historically self-managed its webhook certs via an internal cert-controller.
   - What's unclear: This was not exhaustively confirmed against the full `values.yaml` (2511 lines; only ~800 were inspected in detail) or the chart's `templates/`.
   - Recommendation: Confirm during Wave 0 by rendering the chart (`helm template`) and checking for a `cert-manager.io/*` CRD reference or an internal `kyverno-svc.kyverno.svc` self-signed cert Job/hook before finalizing the values files.

3. **Where exactly does the `ImageValidatingPolicy` manifest itself get committed and applied, given it's not a Helm `values.yaml` key?**
   - What we know: The Kyverno Helm chart installs the CONTROLLER and its CRDs; it does not appear to accept initial policies as a `values.yaml` input (no such key was found in the sections of `values.yaml` inspected).
   - What's unclear: Whether the project should ship the policy as a raw manifest applied by a new numbered stage script (matching the `scripts/stages/NN-*.sh` pattern) or via a small wrapper Helm chart of its own (matching how `helm/kyverno-policies/` might mirror `helm/schemas/cnpg/`'s precedent of a project-owned adjunct to a vendored chart).
   - Recommendation: A new `scripts/stages/26-kyverno-policy.sh` (immediately after the Kyverno chart's own stage, per Pitfall 4's "deploy early" finding) applying a committed manifest via `kubectl apply -f` is the simplest option consistent with existing patterns and INFRA-07's "no manual kubectl surgery" (the apply is scripted and idempotent, not manual). This is squarely within CONTEXT.md's "Claude's Discretion: exact Kyverno ClusterPolicy resource shape/naming."

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| `kind`, `helm`, `kubectl` | Local cluster-up, CI ephemeral-kind | Per `.planning/STATE.md`: "kind and helm are not installed on this machine" as of the last recorded check | — | `helm/kind-action@v1.14.0` installs both in CI automatically; local dev needs `tools/k8s/install_*.sh` (already established from Phase 2) |
| `slopcheck` | Package Legitimacy Gate (Python packages) | N/A this phase | — | Not applicable — no new PyPI packages introduced |
| GitHub-hosted `ubuntu-latest` runner | All new CI jobs | Assumed available (GitHub Actions is already in use) | 4 vCPU / 16 GB per this repo's own `tests/policy/test_manifest_resources.py` constants | None needed — this is the target environment, not a dependency to substitute |
| Docker Hub account/PAT for `docker/login-action` (D-21) | Raising the CI pull-rate limit | Not yet provisioned (new secret) | — | Without it, CI falls back to anonymous 100/6hr/IP; acceptable but flagged by ROADMAP as a source of flaky-looking network failures under load |

**Missing dependencies with no fallback:** none identified — every new dependency has either a documented fallback or is itself the CI environment being targeted.
**Missing dependencies with fallback:** Docker Hub authentication (falls back to anonymous rate limits, degraded but functional).

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest 9.1.1 (already pinned), with markers: `cluster`, `manifests`, `integration`, `dagtest`, `dbt`, `slow`, `regression` (`pyproject.toml`) |
| Config file | `pyproject.toml` `[tool.pytest.ini_options]` / `[tool.coverage.*]` |
| Quick run command | `uv run pytest tests/unit tests/regression -q --cov --cov-report=term-missing` (existing `make test`) |
| Full suite command | `make ci` (existing: `check manifest-policy gitleaks gitleaks-selftest`) — this phase adds `make cluster-verify`-equivalent jobs to the CI workflow itself, not to `make ci` (cluster-dependent suites are deliberately excluded from the offline `check`/`ci` targets, per existing D-16/WINDOWS-#8 convention) |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| CICD-05 | Coverage reported via job summary + artifact | CI-workflow-level (not pytest) | Observe `publish.yml`/`ci.yml` job summary + artifact after a run | ❌ Wave 0 |
| CICD-06 | Images built and tagged by git SHA | CI-workflow-level | `gh api /users/<owner>/packages/container/csv-processor/versions` shows a `sha`-tagged version after merge | ❌ Wave 0 |
| CICD-08 | Trivy scan fails build on HIGH/CRITICAL | CI-workflow-level, with a deliberate-failure smoke test | A policy test asserting `publish.yml` invokes `trivy image --exit-code 1 --severity HIGH,CRITICAL` (mirrors `test_supply_chain_guards.py`'s existing pattern of grepping workflow YAML for required invocations) | ❌ Wave 0 |
| CICD-09 | Ephemeral kind cluster deploys stack, runs E2E | cluster (live) | `pytest tests/e2e/cluster tests/e2e/slice tests/e2e/observability -q -m cluster` inside the CI job (existing `cluster-verify` target, invoked from a new workflow) | ✅ (target exists; new workflow wiring is Wave 0) |
| QUAL-15 | 11 named failure/recovery scenarios pass | cluster (live), new `chaos` marker | `pytest tests/e2e/chaos -q -m cluster` | ❌ Wave 0 — new directory, 11 test modules |
| OBS-06 | Runbooks documented per §89 scenario | Manual/doc review (not pytest-automatable) | A policy test can assert file existence: `docs/runbooks/*.md` count == 18, each containing the 5 required headings (symptoms/diagnosis/recovery/reprocessing/verification) | ❌ Wave 0 — both the docs and a structural existence/shape test |
| INFRA-11 | Retention DAG dry-runs by default, tiered per-dataset config | unit + dagtest | `pytest tests/unit/test_retention_*.py -q` (dry-run logic) + `pytest tests/dagtest -q -k retention` (DAG structure) | ❌ Wave 0 |
| INCR-07 | Rebuild-from-raw reconciles to pre-drop state (4-part proof) | cluster (live), part of full E2E | `pytest tests/e2e/slice/test_rebuild_from_raw.py -q -m cluster` (new) or folded into the existing 2-year-sweep module | ❌ Wave 0 |

### Sampling Rate
- **Per task commit:** `make test` (existing quick unit/regression + coverage, no cluster)
- **Per wave merge:** `make ci` (existing offline gate) + the relevant new cluster-dependent suite run manually against a local `cluster-up` before trusting CI to catch it first
- **Phase gate:** All of: `make ci` green, PR smoke subset green, merge-triggered full E2E green (incl. rebuild-from-raw), chaos suite green, before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `tests/e2e/chaos/` — new directory + `chaos` pytest marker registration in `pyproject.toml`, covering QUAL-15's 11 scenarios
- [ ] `tests/e2e/cluster/test_kyverno_admission.py` — D-18 positive+negative
- [ ] `tests/e2e/slice/test_rebuild_from_raw.py` (or extension of the 2-year-sweep module) — INCR-07/D-29
- [ ] `tests/unit/test_retention_*.py` + `tests/dagtest/test_platform_retention*.py` — INFRA-11
- [ ] `helm/values/{local,ci}/kyverno.yaml`, `helm/versions.env` entry, `scripts/stages/25-kyverno.sh`, a Kyverno-policy manifest + its own apply stage — none exist yet
- [ ] `.github/workflows/publish.yml`, and the new full-E2E/smoke/chaos workflow(s) or jobs — none exist yet (only `ci.yml` exists today)
- [ ] `docs/runbooks/` directory — does not exist yet (18 files, per the verified §89 list above)

## Security Domain

### Applicable ASVS Categories
(ASVS category numbers below follow the commonly-used v4.x scheme; exact numbering has shifted between ASVS major versions — treat the category NAMES as the reliable part of this mapping, not the numbers, at `security_asvs_level: 1` per `.planning/config.json`.)

| ASVS Category | Applies | Standard Control |
|---------------|---------|-------------------|
| V4 Access Control | Yes | Kyverno `ImageValidatingPolicy`, cluster-wide admission-time enforcement (D-14/D-15) — this IS the access-control mechanism for "what may run in this cluster" |
| V10 Malicious Code / Supply Chain Integrity | Yes | cosign keyless signing + SBOM (D-13) + trivy scanning (existing, extended to `publish.yml`) — verifies provenance and known-vulnerability status of every image before it can run |
| V14 Configuration | Yes | Least-privilege `permissions:` blocks per workflow file (existing `ci.yml` convention: `contents: read` at workflow level, no job widens it); `publish.yml` is the first workflow needing `packages: write`/`id-token: write` — scope these to the specific job, not the whole workflow |
| V6 Cryptography | Indirect | Signing/verification cryptography is entirely delegated to cosign/Sigstore (Fulcio/Rekor) — correctly NOT hand-rolled (see Don't Hand-Roll); no new cryptographic code is written by this project |
| V2/V3 Authentication/Session Management | No new surface | Docker Hub / GHCR auth in CI uses standard token-based `docker/login-action` patterns; no new user-facing auth surface is introduced |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|----------------------|
| Unsigned/tampered image admitted into the cluster | Tampering | Kyverno `ImageValidatingPolicy` denies any image not matching a valid cosign signature or the explicit D-16 exception list |
| Fork PR exfiltrating `GITHUB_TOKEN`-scoped registry write access | Elevation of Privilege | Already correctly mitigated — this repo's `ci.yml` explicitly avoids `pull_request_target`; GitHub's own platform behavior additionally forces read-only tokens on fork-originated `pull_request` runs (Pitfall 6) |
| Long-lived Docker Hub credentials leaking via CI logs | Information Disclosure | `docker/login-action` never echoes the password; store as a repository secret, never a plaintext value in workflow YAML (matches existing SEC-10 convention) |
| A stale/unfixed CVE silently accumulating in `.trivyignore` | Tampering / Repudiation | D-07's own policy: every `.trivyignore` entry must be dated and justified at the time it's added, not pre-seeded |
| GHCR package left publicly listing a vulnerable `pr-<number>` image indefinitely | Information Disclosure | D-11's automatic cleanup on PR close bounds the exposure window |

## Sources

### Primary (HIGH confidence)
- Direct file reads of this repository: `README.md` (§84, §89 verified line-by-line), `.planning/REQUIREMENTS.md`, `.planning/STATE.md`, `helm/values/{local,ci}/minio.yaml`, `tests/e2e/cluster/test_minio_buckets.py`, `packages/dataplat/src/dataplat/pipeline/run.py`, `packages/dataplat/src/dataplat/metadata/repository.py`, `migrations/versions/0005_normalized_customers.py`, `migrations/versions/0023_silver_customers_orders_tables.py`, `migrations/versions/0032_meta_reconciliation_results.py`, `.planning/research/ARCHITECTURE.md` §2.3, `Makefile`, `.github/workflows/ci.yml`, `scripts/render-manifests.sh`, `scripts/stages/*.sh`, `tests/policy/test_manifest_resources.py`, `.planning/phases/09-.../09-VALIDATION.md`, `docker/csv-processor/Dockerfile`, `git log -- helm/values/local/minio.yaml`
- GitHub Releases API (`api.github.com/repos/<org>/<repo>/releases/latest`), queried directly at research time for: `sigstore/cosign-installer`, `sigstore/cosign`, `docker/build-push-action`, `docker/setup-buildx-action`, `docker/login-action`, `aquasecurity/trivy-action`, `gitleaks/gitleaks`, `helm/kind-action`, `kyverno/kyverno`, `actions/delete-package-versions`, `actions/upload-artifact`
- Raw Kyverno Helm chart `index.yaml` and `values.yaml` for chart 3.8.2 (`raw.githubusercontent.com/kyverno/kyverno`), fetched and parsed directly

### Secondary (MEDIUM confidence)
- kyverno.io/docs/policy-types/overview/, kyverno.io/docs/policy-types/image-validating-policy/, kyverno.io/docs/policy-types/cluster-policy/verify-images/sigstore/ — Kyverno policy-type maturity/deprecation status and YAML shapes (WebFetch-summarized; cross-checked against two independent fetches with minor date-detail disagreement, noted in Assumptions Log A1)
- kyverno.io/blog/2026/04/24/announcing-kyverno-release-1.18 and related 1.17/1.18 release-note summaries (byteiota, dedico, heise) — ClusterPolicy deprecation timeline
- GitHub Community discussions on `pull_request`-from-fork `GITHUB_TOKEN` restrictions (multiple independent threads corroborating the same restriction)
- coverage.py official docs (coverage.readthedocs.io) — `report --format=markdown` option
- sigstore/docs GitHub issue #440 — `COSIGN_EXPERIMENTAL` obsolescence
- docs.docker.com/docker-hub/usage.md and corroborating search results — Docker Hub rate limits (100/6hr anon, 200/6hr authenticated), confirmed still current

### Tertiary (LOW confidence)
- Exact Kyverno webhook-certificate self-management claim (Assumption A4) — based on general Kyverno architecture knowledge, not directly verified against this specific chart version's templates in this research pass
- `actions/delete-package-versions`'s current functional correctness against the live GitHub Packages API (Assumption A5) — verified only via release metadata (last tag 2024-01-16, repo still pushed to in 2025), not via an actual invocation

## Metadata

**Confidence breakdown:**
- Standard stack (Kyverno/cosign/GH Actions versions): HIGH — every version claim verified directly via GitHub Releases API, not training-data recall
- Architecture (stage ordering, CI job topology, PR-image-signing interaction): HIGH — derived from direct reads of this repo's own established patterns, cross-referenced against verified Kyverno/cosign behavior
- Kyverno policy-type deprecation timeline: MEDIUM-HIGH — core conclusion (ClusterPolicy deprecated, ImageValidatingPolicy is current) corroborated across 3+ independent sources; exact minor-version-of-first-deprecation-notice has small cross-source disagreement (immaterial to the recommendation)
- D-29/D-40 codebase-reuse findings: HIGH — verified by direct code/migration/test reads, not inference
- README §84/§89 counts: HIGH — verified by direct `sed`/line-count of the live file
- Pitfalls: HIGH — each traces to either a direct code read or a verified external source, not speculation

**Research date:** 2026-08-22
**Valid until:** ~30 days for codebase-specific findings (stable until the codebase itself changes); ~14 days for the Kyverno version/deprecation-timeline findings specifically, since Kyverno is on a ~2-month minor release cadence and v1.20 (the ClusterPolicy removal release) is expected within that window — re-verify the exact chart/app version pin at implementation time if planning is delayed.
