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
  - "Live throwaway-PR end-to-end proof (plan's own Task 3 <verify> requirement) could NOT be executed this session: the environment's `gh` CLI has no authenticated GitHub host and no GH_TOKEN/GITHUB_TOKEN is set, so PR creation, Actions-run watching, and GHCR package-version querying/cleanup confirmation are all unavailable. SSH git push/fetch works (already used to push this session's own commits) but that alone cannot open/close a PR or query the REST API. This is a genuine authentication gate, not a design decision — see 'Blocked: Live PR Proof' below."

requirements-completed: []  # CICD-06/CICD-08 remain incomplete — see Blocked section. Not marked complete pending the live PR proof.

# Metrics
duration: ~55min (this session's residual-verification work only — Tasks 1-3's own implementation was already committed by prior sessions)
completed: 2026-08-23
---

# Phase 11 Plan 02: 3-Image Matrix Publish Pipeline (dbt/airflow Trivy Fixes) Summary — PARTIALLY VERIFIED, BLOCKED ON LIVE PR PROOF

**Matrixed `publish.yml` across csv-processor/dbt/airflow with PR-tag parity and a `release`-triggered semver retag, plus `ghcr-cleanup.yml` for PR-close teardown — all three images now pass the trivy HIGH/CRITICAL gate cleanly, but the plan's own required live-PR end-to-end proof is blocked by missing `gh` CLI authentication in this environment.**

## Performance

- **Duration:** ~55 min (this session; Tasks 1-3's code was already committed by two prior, host-restart-interrupted sessions)
- **Started:** 2026-08-23 (continuation session)
- **Completed:** 2026-08-23 (partial — see Blocked section)
- **Tasks:** 3/3 code tasks already committed pre-session; this session's own residual work: trivy investigation + fix (done), live PR proof (blocked)
- **Files modified:** 2 (this session: `docker/dbt/Dockerfile`, `.trivyignore` created)

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

This session:

4. **Trivy investigation + fix (dbt version bump, airflow .trivyignore)** — `d5a3ec0` (fix) — pushed to `origin/main`.

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
- **Left `requirements-completed` empty** and did **not** run `state advance-plan` / `requirements mark-complete` / `roadmap update-plan-progress` for this plan — the plan's own `<verification>` block requires both the pytest suite AND a real, live throwaway-PR proof to be green; only the former is currently satisfied. Marking the plan complete without the latter would misrepresent verification status for plans 11-03/11-04, which depend on this plan's GHCR image-reference shape actually working live.

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

## Issues Encountered

**Blocked: Live PR Proof (Task 3's own `<verify>`/`<action>` requirement, plan's own `<verification>` block)**

The plan's `<verification>` block requires: *"A real throwaway PR proves the pr-tag+sign+scan+cleanup chain live, end to end, including teardown."* This session investigated whether that step could be executed autonomously and found it cannot, in this environment, right now:

- `gh auth status` reports "You are not logged into any GitHub hosts."
- No `GH_TOKEN` or `GITHUB_TOKEN` environment variable is set in this shell.
- `git` itself works fine over SSH (`git@github.com`) — this session's own `docker/dbt/Dockerfile`/`.trivyignore` commit was pushed to `origin/main` successfully via SSH — but opening a PR, watching an Actions run to completion, and querying/confirming GHCR package-version deletion all require the GitHub REST/GraphQL API, which needs an authenticated `gh` (or an equivalent bearer token for `curl`/`gh api`). SSH access alone cannot perform any of those three things.

This was not silently skipped or faked: per the authentication-gate protocol, this is a genuine "STOP and report" situation, not a design decision or a scope judgment call. **What's needed to unblock:** either (a) run `gh auth login` interactively in this environment once (device-flow: visit a URL, enter a code — a single human action, not a recurring one), or (b) set `GH_TOKEN`/`GITHUB_TOKEN` to a PAT with `repo` + `workflow` + `packages:read`/`delete:packages`-equivalent scope for this session's shell. Once either is done, a continuation agent (or this same session, resumed) can:

1. Push a trivial throwaway branch + open a PR via `gh pr create`.
2. `gh run watch --exit-status` the triggered `publish.yml` run to `success` for all three matrix legs.
3. `gh api /users/<owner>/packages/container/<image>/versions` for each of the three images, confirming a `pr-<N>` tag exists (and optionally `cosign verify` against the csv-processor `pr-<N>` digest, per the plan's own acceptance criteria).
4. `gh pr close <N> --delete-branch`, then watch `ghcr-cleanup.yml` to completion, then re-query the same three package-version endpoints confirming the `pr-<N>` versions are gone.
5. Update this SUMMARY.md with the PR number, run URLs, and confirmed teardown, and only then run `state advance-plan` / `requirements mark-complete CICD-06 CICD-08` / `roadmap update-plan-progress 11`.

**None of the code-level work is in question** — `publish.yml`'s tag-computation, fork guard, cosign-sign/trivy-scan non-conditioning on `push` alone, and `ghcr-cleanup.yml`'s tag-to-version-ID resolution logic were all read in full this session and match the plan's acceptance criteria exactly (matrix `include` lists exactly the three images with existing Dockerfile paths; `steps.tag.outputs.value` branches on `github.event_name == 'pull_request'` and produces a `pr-`-prefixed value; cosign-sign/trivy-scan steps use the same fork-guard `if:` as the build step, never `github.event_name == 'push'` alone; `docker buildx imagetools create` references both `github.event.release.tag_name` and `github.sha`). What remains unverified is purely the **live, real-GitHub-activity proof** the plan's own `<verification>` block demands — a proof this session's available credentials cannot produce.

## GHCR Image Reference Shape (for plans 11-03 and 11-04)

Recorded here per this plan's own `<output>` requirement, independent of the live-PR-proof blocker above (this shape is read directly from `publish.yml`'s own tag-computation and build-tags logic, not from a live-verified artifact):

- **Owner:** `github.repository_owner` lowercased (`KonuTech` → `konutech`) — every image reference MUST use the lowercased form; GHCR/OCI repository names reject mixed case (`invalid tag ... repository name must be lowercase`, live-confirmed in plan 11-01).
- **Merge-tagged (push to `main`):** `ghcr.io/konutech/{csv-processor,dbt,airflow}:${GIT_SHA}` — full 40-character git SHA of the triggering commit, e.g. `ghcr.io/konutech/csv-processor:d5a3ec0...` (full SHA, not the 7-char short form `make image-*` targets use locally).
- **PR-tagged (same-repo `pull_request`, any event type — opened/synchronize/reopened):** `ghcr.io/konutech/{csv-processor,dbt,airflow}:pr-<N>` where `<N>` is `github.event.pull_request.number`, e.g. `ghcr.io/konutech/dbt:pr-42`. Signed by digest (cosign keyless/OIDC) and trivy-scanned identically to a merge-tagged image — never conditioned on `push` alone.
- **Release-tagged (`release: created`, retag only, no rebuild):** `ghcr.io/konutech/{csv-processor,dbt,airflow}:${release.tag_name}` — a `docker buildx imagetools create` manifest copy of the same-SHA image already published by the `push` trigger; the cosign signature (digest-bound) remains valid without re-signing.
- **Fork-PR guard:** if `github.event.pull_request.head.repo.full_name != github.repository`, no image is published for any tag shape above — the job logs a `::notice::` and exits cleanly (not a failure).
- **Cleanup:** `pull_request: types: [closed]` (merged or not) removes the `pr-<N>` package **version** (not the whole package) for all three images via `actions/delete-package-versions`, keyed by resolving the specific version ID whose `metadata.container.tags` contains `pr-<N>` (never by `ignore-versions` regex, which does not match container-package tags — see `ghcr-cleanup.yml`'s own extensive comment on this).

## User Setup Required

None - no external service configuration required beyond the `gh` authentication documented in "Blocked: Live PR Proof" above, which is a one-time environment setup step, not a per-plan requirement.

## Next Phase Readiness

**Blocked on this plan's own live-PR proof**, not on anything downstream. Once `gh` is authenticated in this (or a successor) session:
- Plans 11-03 (Kyverno policy) and 11-04 (PR-smoke workflow) can already be planned/written against the GHCR image reference shape documented above — that shape does not depend on the live proof, only on `publish.yml`'s already-committed, already-read source.
- This plan itself should not be marked complete in STATE.md/ROADMAP.md/REQUIREMENTS.md until the live proof runs and this SUMMARY.md is updated with its results, per the plan's own `<verification>` block.

---
*Phase: 11-ci-cd-completion-operations*
*Completed: 2026-08-23 (partial — trivy gate fixed and pushed; live PR proof blocked on `gh` auth)*
