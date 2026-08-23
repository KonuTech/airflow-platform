---
phase: 11-ci-cd-completion-operations
plan: 02
subsystem: cicd
tags: [github-actions, ghcr, cosign, trivy, docker-buildx, publish-workflow, dbt, apache-airflow]

# Dependency graph
requires:
  - phase: 11-01
    provides: single-image (csv-processor) publish.yml proof — 7-step build/sign/scan shape, cosign keyless OIDC signing, GHCR lowercase-owner fix
provides:
  - "3-image matrix publish.yml (csv-processor, dbt, airflow), each signed by digest and trivy-scanned identically on push-to-main and same-repo pull_request"
  - "pr-<number> tag parity: PR-triggered builds are signed/scanned exactly like merge builds, never gated on github.event_name == 'push' alone"
  - "release: created retags an already-published SHA image with semver via docker buildx imagetools create (no rebuild, signature stays valid — digest-bound)"
  - "ghcr-cleanup.yml: closing a PR (merged or not) deletes that PR's pr-<number> GHCR package versions for all three images"
  - "All three images pass trivy's HIGH/CRITICAL gate cleanly (0 findings) — dbt fixed at the version-pin level, airflow's inherited base-image findings recorded in a dated .trivyignore"
affects: [11-03, 11-04]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "D-07 base-image-finding triage: before writing a .trivyignore suppression, check whether a newer patch release of the pinning package relaxes the constraint that's blocking the fix (dbt-core 1.12.2→1.12.3 relaxed sqlparse<0.6.0→<0.7.0, making a real fix possible instead of a suppression)"
    - ".trivyignore entries are grouped by ecosystem/package with a shared dated justification comment, not one comment per CVE line, when the root cause is a single inherited base image"

key-files:
  created:
    - .github/workflows/ghcr-cleanup.yml
    - .trivyignore
  modified:
    - .github/workflows/publish.yml
    - tests/policy/test_publish_workflow_guards.py
    - docker/dbt/Dockerfile

key-decisions:
  - "dbt-core bumped 1.12.2 -> 1.12.3 (not .trivyignore'd) because a real upstream fix path existed: 1.12.3 relaxes its own sqlparse pin from <0.6.0 to <0.7.0, making the already-fixed sqlparse 0.6.0 installable"
  - "airflow image's 53 HIGH/CRITICAL findings (Java jackson-databind/httpcore5 inside ray_dist.jar, Python GitPython/aiohttp/cryptography/litellm/pyasn1/sqlparse, Rust quinn-proto inside uv/uvx, Go stdlib inside the bundled docker CLI) are 100% inherited from the untouched apache/airflow:3.3.0-python3.12 base image (verified via a direct scan of the bare upstream image showing byte-identical finding counts) — recorded as a dated, justified .trivyignore entry per D-07 rather than deviating from Airflow's own officially pinned constraints file"
  - "Live throwaway-PR end-to-end proof executed successfully once gh CLI authentication was available: PR #8 (branch throwaway/11-02-live-pr-proof, a comment-only edit to docs/ci-branch-protection.md) proved the full pr-tag + sign + scan + cleanup chain live, end to end — see 'Live PR Proof — RESOLVED' below for the complete evidence trail"

requirements-completed: [CICD-06, CICD-08]

# Metrics
duration: ~55min (prior session's residual-verification work) + ~15min (this session's live-PR proof)
completed: 2026-08-23
---

# Phase 11 Plan 02: 3-Image Matrix Publish Pipeline (dbt/airflow Trivy Fixes) Summary — FULLY VERIFIED

**Matrixed `publish.yml` across csv-processor/dbt/airflow with PR-tag parity and a `release`-triggered semver retag, plus `ghcr-cleanup.yml` for PR-close teardown — all three images pass the trivy HIGH/CRITICAL gate cleanly, and the plan's own required live-PR end-to-end proof (PR #8) confirmed the entire pr-tag + sign + scan + cleanup chain works in production, including verified teardown.**

## Performance

- **Duration:** ~55 min (trivy fix session) + ~15 min (this session: live-PR proof)
- **Started:** 2026-08-23 (continuation session)
- **Completed:** 2026-08-23 (fully verified — see "Live PR Proof — RESOLVED")
- **Tasks:** 3/3 code tasks committed by prior sessions; trivy investigation + fix done in the intermediate session; live PR proof completed this session — plan fully done
- **Files modified:** 2 (`docker/dbt/Dockerfile`, `.trivyignore`, prior session) + 0 repo-content files this session (the throwaway PR's commit was discarded by design; only planning docs updated)

## Accomplishments

- Confirmed (by reading `git show` on each of the three prior commits `e885618`/`82d0893`/`a30740e`) that Task 1 (3-image matrix + PR-tag parity + fork guard), Task 2 (release-semver retag job + `ghcr-cleanup.yml`), and Task 3's test suite (`tests/policy/test_publish_workflow_guards.py`, 20 tests) all match the plan's declared acceptance criteria.
- `uv run pytest tests/policy/test_publish_workflow_guards.py -q` — **20 passed**, confirmed green this session.
- Built and trivy-scanned all three images fresh, from scratch, at HEAD (`a30740e`):
  - `csv-processor:a30740e` — 0 HIGH/CRITICAL findings, clean.
  - `dbt:a30740e` (pre-fix) — 3 HIGH findings, all `sqlparse 0.5.5` (CVE-2026-54284, CVE-2026-59893, CVE-2026-71491), fixed upstream in sqlparse 0.6.0. Root cause: `dbt-core==1.12.2` pins `sqlparse<0.6.0,>=0.5.5`. Verified `dbt-core` 1.12.3 (a same-day patch release) relaxes that pin to `<0.7.0,>=0.5.5`. Bumped the Dockerfile pin; rebuilt; `sqlparse` now resolves to 0.6.0; re-scanned clean (exit 0).
  - `airflow:a30740e` — 53 HIGH/CRITICAL findings across 4 ecosystems (Java jar, Python, Rust binary, Go binary). Verified via a direct scan of the **untouched** `apache/airflow:3.3.0-python3.12` base image that every finding (same CVE/GHSA IDs, same counts: 4 Java / 36 Python / 1+1 rustbinary / 8 gobinary) is already present there — nothing introduced by this repo's own two `RUN pip install` lines (`apache-airflow[otel]==3.3.0` under Airflow's own official pinned constraints file, and `psycopg[binary]==3.3.4`). Added a dated (`2026-08-23`), justified `.trivyignore` entry per D-07's own documented process; re-scanned clean (exit 0, "Some vulnerabilities have been ignored/suppressed").
- All three images now pass `trivy image --severity HIGH,CRITICAL --exit-code 1 --ignore-unfixed` with `.trivyignore` present — unblocks `publish.yml`'s trivy-scan step for every matrix leg.

## Task Commits

Prior sessions (already on `main` before this session began):

1. **Task 1: Matrix publish.yml across 3 images, PR-tag parity, fork guard** — `e885618` (feat)
2. **Task 2: release-semver retag job + GHCR PR-tag cleanup workflow** — `82d0893` (feat)
3. **Task 3: policy guard tests for 3-image matrix/PR-tag parity/cleanup** — `a30740e` (test)
4. **Trivy investigation + fix (dbt version bump, airflow .trivyignore)** — `d5a3ec0` (fix)
5. **Blocker documentation (gh auth gate)** — `cf5f306`, `e601f99` (docs)

This session (continuation, `gh` now authenticated):

6. **Live PR proof** — no repo-content commit; the throwaway commit `68b626b` (branch `throwaway/11-02-live-pr-proof`, PR #8) was pushed, exercised, and permanently discarded along with its branch when the PR closed — it never landed on `main` and is not part of this repository's real history, by design (mirrors `docs/ci-branch-protection.md`'s own PR #1 precedent).
7. **This SUMMARY.md update + STATE.md/ROADMAP.md/REQUIREMENTS.md finalization** — see commit below.

**Plan metadata:** this SUMMARY's own commit (see below).

## Files Created/Modified

- `.github/workflows/publish.yml` (prior session) — 3-image matrix, PR-tag parity, fork guard, release retag job
- `.github/workflows/ghcr-cleanup.yml` (prior session) — PR-close cleanup
- `tests/policy/test_publish_workflow_guards.py` (prior session) — 20 tests
- `docker/dbt/Dockerfile` (this session) — `dbt-core` 1.12.2 → 1.12.3, dated comment recording the sqlparse CVE chain
- `.trivyignore` (this session, new) — dated, justified allowlist for the airflow image's inherited base-image findings

## Decisions Made

- **dbt-core version bump over suppression:** confirmed a real fix path existed (1.12.3 relaxes the sqlparse pin) before touching `.trivyignore` at all — per D-07's own instruction not to suppress when a genuine fix is available.
- **airflow `.trivyignore` scope:** verified against a direct scan of the bare upstream base image (not just inferred) before writing any suppression, to avoid masking a finding this repo's own Dockerfile might have introduced. Confirmed byte-identical counts, so 100% of the airflow findings are pre-existing/upstream.
- **Did not attempt to patch/pin around GitPython, aiohttp, cryptography, litellm, pyasn1, the Java jars inside `ray_dist.jar`, quinn-proto, or the Go stdlib inside the bundled `/usr/bin/docker` CLI** — none are installed by this repo's own Dockerfile; all come from Airflow's own official image build, pinned by Airflow's own officially published constraints file. Overriding any of them would mean deviating from Airflow's tested/supported dependency set, which the Dockerfile's own header comment already establishes as a deliberate constraint.
- **Throwaway PR content chosen as a comment-only edit, not a functional test file:** used a `<!-- -->` HTML comment appended to `docs/ci-branch-protection.md` (a file that already documents the same throwaway-PR-proof pattern for PR #1) rather than a change to any production code path, Dockerfile, or workflow file — guarantees the PR's build content is genuinely inert regardless of outcome, and ties this session's proof to the same precedent the docs already establish.
- **`requirements-completed: [CICD-06, CICD-08]` now set** and `state advance-plan` / `requirements mark-complete CICD-06 CICD-08` / `roadmap update-plan-progress 11` run this session — the plan's own `<verification>` block's second half (the live throwaway-PR proof) is now satisfied per the "Live PR Proof — RESOLVED" section above, alongside the pytest suite already green from the prior session.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] dbt image shipped with 3 HIGH sqlparse CVEs unfixable at its pinned dbt-core version**
- **Found during:** live trivy re-verification of the dbt image (Task 3's own live-verify requirement)
- **Issue:** `dbt-core==1.12.2` pins `sqlparse<0.6.0,>=0.5.5`; sqlparse's own fix for CVE-2026-54284/-59893/-71491 shipped in 0.6.0, which that pin makes uninstallable
- **Fix:** bumped `docker/dbt/Dockerfile`'s pin to `dbt-core==1.12.3` (verified via `pip index versions dbt-core` that this is a real, already-published release that relaxes the sqlparse constraint to `<0.7.0,>=0.5.5`); rebuilt and re-scanned clean
- **Files modified:** `docker/dbt/Dockerfile`
- **Verification:** `docker run --rm --entrypoint dbt dbt:testfix --version` reports `Core: 1.12.3 - Up to date!`, `Plugins: postgres: 1.11.0 - Up to date!`; trivy re-scan exit code 0
- **Committed in:** `d5a3ec0`

**2. [Rule 2 - Missing critical functionality / D-07] airflow image's trivy scan would fail on real content this repo doesn't control**
- **Found during:** live trivy re-verification of the airflow image
- **Issue:** the untouched `apache/airflow:3.3.0-python3.12` base image ships 53 pre-existing HIGH/CRITICAL findings (GitPython, aiohttp, cryptography, litellm, pyasn1, sqlparse, Java jars inside `ray_dist.jar`, quinn-proto inside `uv`/`uvx`, Go stdlib inside a bundled `/usr/bin/docker` CLI) — without a `.trivyignore` entry, `publish.yml`'s trivy-scan step fails unconditionally for the airflow matrix leg on every future push/PR, blocking D-10's own gate from ever passing for this image
- **Fix:** added a dated (2026-08-23), justified `.trivyignore` entry per D-07's documented process, grouped by ecosystem, each block explaining why the finding is unfixable from this repo's own Dockerfile without deviating from Airflow's own officially pinned constraints
- **Files modified:** `.trivyignore` (new)
- **Verification:** re-scanned `airflow:a30740e` with `--ignorefile .trivyignore` — 0 findings across every target (Java/Python/rustbinary/gobinary), exit code 0
- **Committed in:** `d5a3ec0`

---

**Total deviations:** 2 auto-fixed (1 bug fix via version bump, 1 D-07-mandated documented suppression)
**Impact on plan:** Both were necessary for D-10 (PR images scanned identically to merge images) to actually be satisfiable in CI; neither touches this plan's own declared `files_modified` scope beyond what Rule 1/Rule 2 explicitly permit for blocking issues found during the plan's own verification step.

## Live PR Proof — RESOLVED

**This session's own scope.** The prior session's blocker (`gh` CLI unauthenticated) was resolved externally before this session began — `gh auth status` now reports a logged-in `github.com` account (`KonuTech`, scopes `repo`, `workflow`, `read:packages`, `read:org`, `gist`). This session executed the plan's own required live-PR proof in full, end to end, exactly per the resume steps the prior session's SUMMARY recorded.

### What was done

1. **Branch + PR:** Created throwaway branch `throwaway/11-02-live-pr-proof` off `main` (base commit `e601f99`), with one comment-only, no-op commit `68b626b` (a `<!-- -->` HTML comment appended to `docs/ci-branch-protection.md`, mirroring that same file's own documented PR #1 precedent — no production logic touched). Pushed and opened via `gh pr create`: **PR #8** (`https://github.com/KonuTech/airflow-platform/pull/8`), head SHA `68b626b1276807068c3969ddb840c1d79318a18a`.

2. **`publish.yml` watched to completion — run `32630879549`** (triggered by `pull_request`, same-repo, not a fork): overall `status: completed`, `conclusion: success`. All three matrix legs individually confirmed `success`:
   - `Build, sign and scan csv-processor` — 1m23s, every step (checkout, buildx, login, build-push, cosign-installer, cosign sign, trivy scan) green.
   - `Build, sign and scan dbt` — 1m31s, same shape, green.
   - `Build, sign and scan airflow` — 2m5s, same shape, green.
   - `Retag ... with release semver (no rebuild)` job correctly **skipped** (this was a `pull_request` event, not `release` — the job's own `if: github.event_name == 'release'` guard behaved exactly as designed).

3. **GHCR `pr-8` package versions confirmed to exist** for all three images via `gh api /users/konutech/packages/container/<image>/versions`:
   - `csv-processor` — version id `1162158342`, digest `sha256:a4a9b35640e25e6769686b7479f83fb10c5b91403705d16dc8dc43948a694b75`, tags `["pr-8"]`, created `2026-08-23T09:24:34Z`.
   - `dbt` — version id `1162158490`, digest `sha256:203290e6bafd69f2858c1cc7eed202ce3766f02797055ea6767ec90843624fb0`, tags `["pr-8"]`, created `2026-08-23T09:24:39Z`.
   - `airflow` — version id `1162158926`, digest `sha256:520d42b385898d3f5d9f8388e74884e346e7b311de6c5f9d655b9bf154a43e32`, tags `["pr-8"]`, created `2026-08-23T09:25:04Z`.

   Confirmed this is a **user-owned** repository (`gh repo view --json owner`: `KonuTech`, not an org), so `/users/konutech/packages/container/...` is the correct endpoint shape (not `/orgs/...`) — matches what `ghcr-cleanup.yml`'s own `Resolve pr-<number> version id(s)` step already assumes.

4. **cosign verify confirmed live** against the csv-processor `pr-8` digest, using the PR-triggered OIDC identity regex the plan's own acceptance criteria specify:
   ```
   cosign verify \
     --certificate-identity-regexp "https://github.com/KonuTech/airflow-platform/\.github/workflows/publish\.yml@refs/pull/8/merge" \
     --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
     ghcr.io/konutech/csv-processor@sha256:a4a9b35640e25e6769686b7479f83fb10c5b91403705d16dc8dc43948a694b75
   ```
   Result: **verified** — "The cosign claims were validated", "Existence of the claims in the transparency log was verified offline", "The code-signing certificate was verified using trusted certificate authority certificates". This proves D-10 live: a `pr-<number>` image is signed exactly like a merge-tagged image, under the correct PR-scoped OIDC subject (`.../publish.yml@refs/pull/8/merge`), not skipped or weakened for PR builds.

5. **PR closed:** `gh pr close 8 --delete-branch` — closed (not merged; `merged: false`, `closed_at: 2026-08-23T09:26:43Z`), throwaway branch and its one-off comment-only commit deleted. The comment-only edit to `docs/ci-branch-protection.md` never landed on `main`.

6. **`ghcr-cleanup.yml` watched to completion — run `32631014608`** (triggered by `pull_request: types: [closed]`): overall `status: completed`, `conclusion: success`, all three matrix legs (`Delete pr-<number> package versions (csv-processor|dbt|airflow)`) individually `success`. Verified via the csv-processor job's own log (`gh api .../jobs/97173733417/logs`) that the resolve-then-delete mechanism actually did work, not vacuously: `IDS=` resolved a real version ID (not empty — the `::notice::No GHCR version tagged ... found` fallback line did NOT fire), `actions/delete-package-versions` ran with `package-version-ids` populated, and its own log line reads `Total versions deleted till now: 1`.

7. **GHCR `pr-8` package versions re-queried and confirmed GONE** for all three images, post-cleanup:
   - `csv-processor` — no version tagged `pr-8` found. CONFIRMED GONE.
   - `dbt` — no version tagged `pr-8` found. CONFIRMED GONE.
   - `airflow` — no version tagged `pr-8` found. CONFIRMED GONE.

### What this proves

The full D-01/D-09/D-10/D-11 chain works live, end to end, exactly as `publish.yml`/`ghcr-cleanup.yml` were designed:
- A same-repo `pull_request` publishes a `pr-<number>`-tagged image for all three matrix legs (D-09).
- Each is signed by cosign keyless/OIDC and trivy-scanned identically to a merge-tagged image — never conditioned on `push` alone (D-10), confirmed both by reading the run's own step list (cosign-sign and trivy-scan steps present and green on the PR-triggered run) and by an independent `cosign verify` against the live digest.
- Closing the PR (without merging) triggers `ghcr-cleanup.yml`, which correctly resolves and deletes the specific `pr-<number>` GHCR package version for all three images (D-11), verified by both the workflow's own "Total versions deleted till now: 1" log line and an independent post-hoc GHCR API re-query showing the tag is actually gone, not just reported deleted.

This closes the plan's own `<verification>` requirement ("A real throwaway PR proves the pr-tag+sign+scan+cleanup chain live, end to end, including teardown") in full. **CICD-06 and CICD-08 are marked complete.**

## GHCR Image Reference Shape (for plans 11-03 and 11-04)

Recorded here per this plan's own `<output>` requirement. This shape is read directly from `publish.yml`'s own tag-computation and build-tags logic AND is now live-verified end to end by PR #8 above (both the `pr-<N>` publish path and the cleanup teardown path were exercised against real GHCR state, not just inferred from source):

- **Owner:** `github.repository_owner` lowercased (`KonuTech` → `konutech`) — every image reference MUST use the lowercased form; GHCR/OCI repository names reject mixed case (`invalid tag ... repository name must be lowercase`, live-confirmed in plan 11-01).
- **Merge-tagged (push to `main`):** `ghcr.io/konutech/{csv-processor,dbt,airflow}:${GIT_SHA}` — full 40-character git SHA of the triggering commit, e.g. `ghcr.io/konutech/csv-processor:d5a3ec0...` (full SHA, not the 7-char short form `make image-*` targets use locally).
- **PR-tagged (same-repo `pull_request`, any event type — opened/synchronize/reopened):** `ghcr.io/konutech/{csv-processor,dbt,airflow}:pr-<N>` where `<N>` is `github.event.pull_request.number`, e.g. `ghcr.io/konutech/dbt:pr-42`. Signed by digest (cosign keyless/OIDC) and trivy-scanned identically to a merge-tagged image — never conditioned on `push` alone.
- **Release-tagged (`release: created`, retag only, no rebuild):** `ghcr.io/konutech/{csv-processor,dbt,airflow}:${release.tag_name}` — a `docker buildx imagetools create` manifest copy of the same-SHA image already published by the `push` trigger; the cosign signature (digest-bound) remains valid without re-signing.
- **Fork-PR guard:** if `github.event.pull_request.head.repo.full_name != github.repository`, no image is published for any tag shape above — the job logs a `::notice::` and exits cleanly (not a failure).
- **Cleanup:** `pull_request: types: [closed]` (merged or not) removes the `pr-<N>` package **version** (not the whole package) for all three images via `actions/delete-package-versions`, keyed by resolving the specific version ID whose `metadata.container.tags` contains `pr-<N>` (never by `ignore-versions` regex, which does not match container-package tags — see `ghcr-cleanup.yml`'s own extensive comment on this).

## User Setup Required

None — `gh` authentication (a one-time environment setup step, not a per-plan requirement) is already in place and was used to complete this plan's live-PR proof.

## Next Phase Readiness

**Fully unblocked.** This plan is complete:
- Plans 11-03 (Kyverno policy) and 11-04 (PR-smoke workflow) can rely on the GHCR image reference shape documented above — it is now both source-verified and live-verified (PR #8's real publish + cleanup cycle).
- CICD-06 and CICD-08 marked complete in REQUIREMENTS.md; STATE.md and ROADMAP.md updated to reflect plan 11-02 as fully done.

---
*Phase: 11-ci-cd-completion-operations*
*Completed: 2026-08-23 (fully verified — trivy gate fixed, live PR #8 proved the pr-tag+sign+scan+cleanup chain end to end, including confirmed teardown)*
