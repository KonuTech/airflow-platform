# Phase 11: CI/CD Completion & Operations - Context

**Gathered:** 2026-08-22
**Status:** Ready for planning

<domain>
## Phase Boundary

The whole environment provably rebuilds from the repository, every catalogued failure mode has
a passing test, and someone who did not build the platform can operate it. Two independent
tracks (ROADMAP Wave H): **S13 CI/CD** (ephemeral-kind E2E in GitHub Actions, image
build/publish/tag/scan/sign, admission enforcement) and **S14 Operations** (failure-scenario
tests, rebuild-from-raw capstone, retention, runbooks). Research stage explicitly recommends
**skipping `--research-phase`** — "the CI patterns are standard and `values-ci.yaml` already
exists from Phase 2."

</domain>

<decisions>
## Implementation Decisions

### Image Publishing & Registry

- **D-01:** All three images get GHCR-published + trivy-scanned: `csv-processor`, `dbt`
  (Phase 08.1), and the custom Airflow[otel] image (Phase 07) — full parity with what's already
  built locally via the `image-csv-processor`/`image-dbt`/`image-airflow` Make targets.
- **D-02:** Publish trigger is **build-verify on every PR (no push), push + git-SHA tag on merge
  to main** — matches README §78's "merge → build → tag → push" flow and avoids leaking
  untested images. (Refined by D-09 below for the E2E case.)
- **D-03 (LOCKED — user chose to add this now, not defer):** Semver tagging is added in this
  phase, triggered by **GitHub Release creation** (not a bare git-tag push) — a Release-creation
  workflow additionally tags all three images with the release's semver, on top of the git-SHA
  tag every merge already gets.
- **D-04:** GHCR package visibility matches the repo — repo is public, so packages are public.
  No extra visibility configuration beyond `GITHUB_TOKEN` with `packages: write`.
- **D-05:** Images are **amd64-only** — matches the dev environment (WSL2/x86_64) and
  GitHub-hosted runners. No buildx multi-platform/QEMU emulation.
- **D-06:** The image build/publish/scan job runs **independently/in parallel** with the existing
  `check`/`manifests`/`integration`/`secrets` jobs in `ci.yml` — not gated on `make check`
  passing first.
- **D-07:** If trivy's first real run finds a pre-existing unfixed HIGH/CRITICAL CVE in a base
  image (e.g. `apache/airflow:3.3.0-python3.12`), **handle it live** — add a dated, justified
  `.trivyignore` entry for that specific finding at that time. Do not pre-emptively audit and
  seed `.trivyignore` for hypothetical findings before the gate exists.
- **D-08:** Image build/publish/scan/sign jobs live in a **new separate workflow file**
  (e.g. `.github/workflows/publish.yml`), not added as jobs inside the existing `ci.yml` —
  keeps `ci.yml` free of the `packages: write` permission and secret references its own comments
  currently claim it doesn't need (see Canonical References below).
- **D-09 (refines D-02 for the E2E case):** Since PR builds don't push under the general policy,
  the ephemeral-kind E2E job (CICD-09) needs the PR's own code as a runnable image. Resolution:
  **PR builds ARE pushed to GHCR too, tagged `pr-<number>`**, specifically so the E2E job can pull
  them and test the PR's actual code, not main's last published image.
- **D-10:** `pr-<number>` images get the **same trivy HIGH/CRITICAL scan gate** as merge-tagged
  images — a vulnerable dependency is caught before merge, not after.
- **D-11:** `pr-<number>` tags are **automatically cleaned up** from GHCR when the PR closes
  (merged or not) — a workflow step deletes the package version, keeping the tag list to
  meaningful (main + release) tags.
- **D-12:** A **dedicated rollback Make target/script** is added (redeploy the image at a prior
  git SHA) rather than relying on manually editing the Helm values `image.tag` — documents the
  procedure as a runbook entry too (ties into Operations below).

### Image Scanning & Supply Chain

- **D-13:** Every published image gets **SBOM (buildx `--provenance`/`--sbom`) + cosign keyless
  (OIDC-based) signing** — no key management needed (OIDC), SBOM is a `docker/build-push-action`
  config flag.
- **D-14 (LOCKED — deliberate scope expansion; Claude flagged this as outside any Phase 11
  REQ-ID/success-criterion, user explicitly chose to keep it in-phase anyway):** **Admission-time
  signature enforcement is added via Kyverno**, a new cluster admission controller, rather than
  signing being the deliverable on its own. This is genuinely new platform capability beyond
  what CICD-05/06/08/09, QUAL-15, OBS-06, INFRA-11, or INCR-07 ask for — recorded here as an
  informed, explicit choice, not something to quietly soften back to "no enforcement" during
  planning.
- **D-15:** Kyverno's policy applies **cluster-wide** (every pod, every namespace) — not scoped
  to just this project's own images.
- **D-16:** Cluster-wide enforcement needs an answer for third-party images (Airflow, MinIO,
  Vault, CNPG, kube-prometheus-stack, etc.) that were never cosign-signed by this pipeline:
  a Kyverno policy **exception list of the exact pinned upstream image references** already
  committed in `helm/values/*.yaml` is exempted from verification. Anything NOT on that list must
  verify.
- **D-17:** Kyverno is deployed in **both the local and CI (`values-ci.yaml`) Helm values
  profiles** — CI is exactly where "a PR spins up an ephemeral kind cluster and deploys the
  stack" (success criterion 1) needs to prove the complete stack, including the admission policy,
  actually boots. Kyverno's footprint (~100-200m CPU, one controller pod) fits the CI-profile
  trimming budget.
- **D-18:** Kyverno enforcement gets the same **positive + negative live-proof pattern** this
  project established for SEC-12 (Phase 5): a test deploys a signed image (admits) and a
  deliberately unsigned/tampered image (denied) — "if the negative test is awkward to write, the
  control isn't real."

### Ephemeral kind E2E Scope in CI (CICD-09)

- **D-19:** The **full local e2e suite** (tests/e2e/cluster, slice incl. the 2-year backfill
  sweep, observability, vault) runs only **on merge to main**, not on every PR. PRs get a fast
  **smoke subset** instead.
- **D-20:** The PR-gating smoke subset covers, at minimum, all four: (1) cluster boots + core
  services healthy (Airflow, both PostgreSQL instances, MinIO, Vault, Kyverno all Helm-release
  healthy), (2) one real DAG run reaching `SUCCEEDED` end-to-end, (3) a Vault positive+negative
  auth test (SEC-12 pattern), (4) a Kyverno positive+negative admission test (D-18).
- **D-21:** CI authenticates to Docker Hub (`docker/login-action` + a repo secret) before pulling
  chart images, to raise the pull-rate limit from anonymous (100/6hr/IP) to authenticated
  (200/6hr) — directly addresses ROADMAP's own flagged risk ("Docker Hub anonymous pull limits —
  they look like network flakes").
- **D-22:** When the merge-triggered full E2E suite fails (main already has the commit by
  definition), a workflow step **auto-files/updates a GitHub issue** tagging the failing commit,
  in addition to the red CI status check.
- **D-23:** Coverage (CICD-05) is reported via the **GitHub Actions job summary + an uploaded
  HTML/artifact** — no third-party service (Codecov etc.) for this phase.
- **D-24:** The rebuild-from-raw capstone test (INCR-07) runs as **part of the merge-triggered
  full E2E suite**, not local/manual-only.
- **D-25:** QUAL-15's failure-scenario/chaos tests (pod crash, DB/MinIO/Vault unavailable, OOM,
  task timeout, secret rotation, unauthorized access) run in a **separate dedicated
  job/ephemeral-kind cluster**, not interleaved with the normal full-E2E pass — a chaos test that
  leaves a component deliberately broken can't contaminate/flake the happy-path suite's results.
- **D-26:** The chaos job triggers on **merge to main**, same trigger as the full E2E suite (not
  scheduled).
- **D-27:** The full-E2E job and the chaos job run **in parallel** (two separate GitHub-hosted
  runners, each with its own ephemeral kind cluster) to minimize total wall-clock time to merge
  feedback.

### Rebuild-from-Raw Semantics (INCR-07)

- **D-28:** "The analytical warehouse is dropped and rebuilt" (success criterion 4) means **all
  ETL-owned schemas**: staging, silver (dbt), gold/normalized (including SCD2 `customers`,
  `orders`), AND `meta` (control-plane: batches, runs, watermarks, validation results, dedup
  audit, lineage) — not just data schemas, and not the whole PostgreSQL instance (Alembic
  migration correctness is a separate concern from rebuild-from-raw).
- **D-29:** Since `meta` is dropped too, a rebuild re-derives its own run/batch identity from
  scratch (new run_ids, new batch_ids). "Reconciles to its pre-drop state" is proven by **all
  four**: (1) row counts per table match, (2) a content hash/checksum over each table's business
  data — excluding surrogate run/batch identity columns — matches, (3) SCD2 version count +
  `valid_from`/`valid_to`/`is_current` state per `customer_id` matches, and (4) the comparison
  reuses the **existing** `meta.reconciliation_results` / `record_reconciliation` mechanism
  (Phase 9/10) rather than a bespoke pre/post query.
- **D-30:** Sequencing: rebuild-from-raw runs **last**, within the merge-triggered full E2E suite,
  reusing whatever state the suite's own tests (incl. the 2-year sweep) already populated as the
  pre-drop baseline. No separate data-seeding for this test — it operates on realistic
  multi-dataset, multi-schema-version history.
- **D-31:** The rebuild uses the **existing historical config-resolution path** (ConfigRegistry,
  the same mechanism INCR-06 already proves for backfills) — each raw file is reprocessed under
  the config version actually in effect for it, not just whatever `configs/datasets/*.yaml` says
  today. This is the honest reading of "rebuilt from raw + versioned configuration."
- **D-32:** Rebuild is invoked via a **new reusable Make target** (e.g. `make rebuild-from-raw`)
  that wraps: drop the ETL-owned schemas → `alembic upgrade head` → trigger Airflow backfill
  DagRuns across the full historical date range discovered from raw. CI calls the same target a
  real operator would reach for after an actual disaster — one implementation, two callers.
- **D-33:** The rebuild also **wipes and regenerates MinIO's `validated`/`processed`/`quarantine`
  object-storage layers**, not just the PostgreSQL schemas — a more complete proof that nothing
  outside `raw` is a required source of truth.
- **D-34:** Quarantine-resolution history (VALID-08, Phase 8) is lost when `meta` is dropped —
  a record originally rejected-then-resolved via a corrected-file backfill re-drive will come
  back as freshly-rejected/PENDING after rebuild, since the resolution action itself isn't raw
  data. This is **acceptable and expected**, not a reconciliation failure — document it
  explicitly (runbook/ADR) as a deliberate rebuild property. Do NOT special-case the D-29
  reconciliation comparison to exclude these rows.

### Retention (INFRA-11)

- **D-35:** Retention is enforced by a **dedicated Airflow maintenance DAG** (e.g.
  `platform_retention`), deliberately **not** part of any ingestion DAG's task graph — satisfies
  README §64's "retention must remain separate from processing logic" structurally, and gets the
  same observability/logging/retry machinery as every other DAG.
- **D-36:** Given rebuild-from-raw (D-28..D-34) needs the FULL raw history to work, raw's
  **default retention is effectively indefinite** (no automatic pruning). The retention DAG still
  supports a configurable window for raw (proving INFRA-11's "configurable" requirement
  structurally), but the shipped default is disabled/None — a real raw-pruning decision (e.g. a
  compliance-driven window) is a deliberate future operator choice, not this phase's default.
- **D-37:** Other layers get **tiered defaults by operational purpose**: `processed` (MinIO
  artifacts, cheaply re-derivable from raw) short, e.g. 30–90 days; `quarantine` (represents
  unresolved work needing human attention) longer, e.g. 180 days; validation reports + ingestion
  metadata (the audit/lineage trail the Core Value statement promises) long, e.g. 1–2 years;
  logs short, e.g. 30 days (standard operational hygiene). Exact numeric defaults within these
  ranges are Claude's discretion at planning time.
- **D-38:** The retention DAG **dry-runs by default** — every run reports what WOULD be deleted
  (count, size, oldest/newest) regardless of configuration. Actual hard-deletion requires an
  explicit opt-in config flag (e.g. `retention.enforce: true`, defaulting `false`) — matches the
  Core Value's "no data is ever silently dropped": a misconfigured window fails loud in a report,
  not by silently vaporizing data on first run.
- **D-39:** Retention configuration lives **per-dataset**, extending the existing dataset YAML
  (e.g. `configs/datasets/customers.yaml`) with a new `retention:` block validated by the same
  Pydantic contract model — matches the established "config-not-code, Pydantic-validated"
  convention (Phase 10 D-05/D-06 set this same precedent for DELETE semantics and the
  circuit-breaker threshold). Not a separate platform-wide `retention.yaml`.
- **D-40 (LOCKED — resolves Phase 2's D-08 raw-immutability revisit point, which explicitly named
  Phase 11):** `helm/values/local/minio.yaml` (lines 52-66) shows Phase 2 considered and
  **rejected** MinIO object-lock/WORM on every bucket, specifically because retained test objects
  become impossible to clean up on the persistent local cluster — and the comment names Phase 11
  as "the named place to revisit it." Resolution: enforce raw immutability (README §63) via an
  **IAM bucket policy denying `s3:DeleteObject`/`s3:DeleteObjectVersion`** on `raw` to every role
  except a break-glass admin identity — NOT object-lock/WORM. This gets the same practical
  protection (every workload identity is structurally unable to delete raw) without reopening
  D-08's exact objection (WORM's retention lock is permanent; an IAM policy is revocable).

### Runbooks (OBS-06)

- **D-41:** Runbooks are **one doc per §89 scenario** in `docs/runbooks/` (symptoms, diagnosis,
  recovery, reprocessing, verification per scenario — not a single consolidated file). Scenarios
  with a matching **real** incident in `.planning/debug/resolved/*.md` (scheduler stall, Vault
  reseal, CPU starvation, prometheus scrape naming, backfill re-drive) are written FROM that
  incident's actual diagnosis and fix, not hypothetically. Scenarios never actually hit
  (deliberate chaos scenarios like "Vault unavailable", "MinIO unavailable") are written from the
  D-25 chaos test suite's own observed behavior once that test exists — runbooks trail the chaos
  tests, they are not written speculatively ahead of them.

### Claude's Discretion

- Exact numeric retention-window defaults within the D-37 tiers (e.g. 30 vs 90 days for
  `processed`) — ranges are locked, precise values are not.
- Exact Kyverno `ClusterPolicy` resource shape/naming, and exact cosign OIDC issuer/identity
  configuration details.
- Retention DAG's schedule cadence (daily vs weekly) — not discussed.
- Exact rollback Make target name/mechanics (D-12) beyond "redeploy at a prior git SHA."
- `.trivyignore` file format specifics (D-07) — apply when a real finding occurs.
- Exact GitHub Actions workflow YAML structure (job names, step ordering) beyond what D-01..D-27
  lock.
- Whether the rollback Make target (D-12) also gets its own runbook entry under D-41's
  `docs/runbooks/` structure, or lives only as a Makefile target with inline comments — plan-time
  call.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Roadmap & Requirements
- `.planning/ROADMAP.md` Phase 11 section — goal, success criteria, plan guidance (Wave H
  concurrent S13/S14 tracks, skip `--research-phase`, Docker Hub pull-limit risk, "runbooks trail
  everything by design," INCR-07 as capstone, retention-vs-rebuild tension)
- `.planning/REQUIREMENTS.md` CICD-05, CICD-06, CICD-08, CICD-09, QUAL-15, OBS-06, INFRA-11,
  INCR-07 — full requirement text; traceability table

### README — the exact spec language behind each requirement
- README.md §64 Data Retention, §76 Pull Request CI, §77 Container Engineering,
  §78 Continuous Deployment, §79 Git Practices, §81.9 CI/CD Secrets, §84 Failure Scenarios
  (20-item list; QUAL-15's DoD 89 subset is 11 of them), §89 Operational Runbook (17-item
  scenario list, distinct from §84's list), §90 Disaster Recovery / Rebuildability, §91 Data
  Retention (duplicate of §64)

### Existing CI/CD — what this phase extends
- `.github/workflows/ci.yml` — existing `check`/`manifests`/`integration`/`secrets` jobs; the
  comment on the `secrets` job explicitly states "The first job to interpolate a secret (Phase
  11, publishing images) owes a re-audit" of the current "this workflow references no repository
  secret at all" claim — D-08's new `publish.yml` is where that first secret reference belongs,
  keeping `ci.yml`'s claim intact
- `Makefile` targets `image-csv-processor`, `image-dbt`, `image-airflow` — existing
  local-registry build/tag/push pattern this phase extends to GHCR (D-01, D-02); `helm-lint`,
  `manifests`, `cluster-verify`, `test-integration`, `check`, `ci` — existing gate composition
  pattern to follow for any new Make targets (D-12 rollback, D-32 rebuild-from-raw)
- `.github/dependabot.yml` — already configured (Phase 1); no new dependency-scanning setup
  needed beyond trivy

### MinIO / Raw Immutability — the D-08/D-40 thread
- `helm/values/local/minio.yaml` lines 51-66 — `buckets:` block, `objectlocking: false` on every
  bucket with the D-08 comment explicitly naming Phase 11 as the revisit point; `versioning: true`
  already set on `raw` alone (§63 correction-as-new-version, already done)
- `helm/values/ci/minio.yaml` lines 40-65 — CI profile's parallel bucket config

### Prior-phase precedent (do not re-litigate)
- `.planning/phases/10-slowly-changing-dimensions/10-CONTEXT.md` — D-06 mass-delete circuit
  breaker (pattern precedent for "fail loud on implausible bulk change," relevant to D-38's
  dry-run-by-default retention posture); D-09 "codebase-convention-over-aspirational-doc"
  (relevant to D-39's config-not-code precedent)
- `.planning/phases/08-validation-quarantine-metadata-control-plane-completion/08-CONTEXT.md` —
  VALID-08 quarantine-resolution mechanism (D-34's subject), D-10 rejection-rate circuit breaker
  (direct template referenced for D-06 in Phase 10, same "count vs threshold, fail loud" shape
  relevant here)
- `.planning/phases/05-vault-secrets-workload-identity/` — SEC-12 positive+negative test pattern
  (direct template for D-18's Kyverno test and D-20's Vault smoke test)

### Real incidents — runbook (D-41) source material
- `.planning/debug/resolved/airflow-scheduler-stuck-tasks.md` — node CPU exhaustion +
  xcom-sidecar leak
- `.planning/debug/resolved/dagrun-scheduler-stall.md` — Docker Desktop/WSL2 restart breaking the
  DAGs hostPath mount, cluster-wide scheduler freeze
- `.planning/debug/resolved/wait-for-files-stuck-task.md` — Vault reseal after host restart
- `.planning/debug/resolved/prometheus-runs-started-scrape.md` — OTel Collector counter naming
- `.planning/debug/resolved/backfill-does-not-redrive-rejected-row.md` — quarantine re-drive bug
- `.planning/STATE.md` "Blockers/Concerns" and "Quick Tasks Completed" sections — CPU/memory
  starvation incidents (260817-mvp, 260817-oqy, 260817-rvq) with full root-cause narratives,
  additional runbook candidates

### Memory
- `host_hardware_context` (project memory) — WSL2/kind CPU/memory constraints directly relevant
  to running full-E2E + chaos-test + rebuild-from-raw as three concurrent-ish heavy CI jobs
  (D-19-D-27, D-30)

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `meta.reconciliation_results` / `record_reconciliation` (Phase 9/10,
  `packages/dataplat/src/dataplat/pipeline/run.py`, `metadata/repository.py`) — direct reuse
  target for D-29's rebuild reconciliation proof.
- `ConfigRegistry` / historical schema-version resolution (Phase 3, proven for backfills via
  INCR-06) — direct reuse target for D-31's historical config replay.
- `resolve_rejected_records_for_business_keys` (Phase 8, migration 0020) — the mechanism whose
  history is deliberately NOT preserved across rebuild per D-34.
- SEC-12's positive+negative Vault auth test (Phase 5) — direct template for D-18 (Kyverno) and
  D-20 (Vault smoke test).
- `RejectionRateCircuitBreaker` (Phase 8) / mass-delete circuit breaker (Phase 10 D-06) —
  "count vs configurable threshold, fail loud" shape, relevant precedent for D-38's dry-run
  posture on retention.

### Established Patterns
- Config-not-code, Pydantic-validated per-dataset YAML (every phase since 3) — governs D-39
  (retention config location).
- Two Helm values profiles (`values-local.yaml`/`values-ci.yaml`) parameterized from Phase 2 —
  governs D-17 (Kyverno in both), D-19-D-27 (CI job design against the CI profile's resource
  budget).
- Immutable git-SHA image tagging, never `:latest` (established since Phase 3/4) — governs D-01,
  D-02, D-09, D-12.

### Integration Points
- New `.github/workflows/publish.yml` (D-08) — image build/scan/sign/Kyverno-policy-adjacent
  work; first workflow file to hold `packages: write` and any secret reference in this repo.
- New Airflow DAG `platform_retention` (D-35) — new file alongside existing DAGs, deliberately
  outside any ingestion DAG's task graph.
- New Make targets: `rebuild-from-raw` (D-32), a rollback target (D-12).
- `helm/values/local/minio.yaml` and `helm/values/ci/minio.yaml` — new IAM deny-delete policy for
  `raw` (D-40); Kyverno chart addition to both files (D-17).
- New `docs/runbooks/` directory (D-41).

</code_context>

<specifics>
## Specific Ideas

- The user consistently chose the more rigorous/thorough option over the cheaper recommendation
  throughout this discussion (semver tagging now, GitHub-Release trigger over plain git-tag,
  SBOM+cosign signing, cluster-wide Kyverno enforcement, dedicated rollback tooling, wipe+
  regenerate MinIO layers on rebuild, tiered retention with dry-run safety) — directly consistent
  with the pattern already noted in Phase 9's and Phase 10's own CONTEXT.md ("user chose the more
  ambitious/rigorous option"). Treat these as deliberate, informed choices, not something to
  quietly scale back during planning.
- One deliberate scope expansion was explicitly flagged by Claude mid-discussion and confirmed by
  the user anyway: D-14 (Kyverno admission enforcement) is not covered by any Phase 11 REQ-ID or
  success criterion. The user's own words: "Keep it in Phase 11 anyway." This should NOT be
  quietly dropped or descoped during planning without going back to the user.
- D-40 directly resolves a forward-reference planted in Phase 2: `helm/values/local/minio.yaml`'s
  own comment names "Phase 11 (rebuild-from-raw, ephemeral environment)" as where MinIO
  object-lock should be revisited. The resolution chosen (IAM deny-delete policy, not
  object-lock/WORM) respects the original D-08 objection rather than reopening it.

</specifics>

<deferred>
## Deferred Ideas

None raised that belong to a different phase — this discussion stayed within Phase 11's CI/CD +
Operations boundary (including the one flagged-but-kept scope expansion, D-14).

### Reviewed Todos (not folded)
`draft-adr-dbt-silver-layer-boundary.md` (score 0.6) matched Phase 11 but was already folded into
Phase 08.1's scope during that phase's own discussion — confirmed still not relevant here.

</deferred>

---

*Phase: 11-CI/CD Completion & Operations*
*Context gathered: 2026-08-22*
