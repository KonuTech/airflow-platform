---
phase: 11-ci-cd-completion-operations
plan: 01
subsystem: infra
tags: [github-actions, docker, buildx, ghcr, cosign, sigstore, trivy, sbom, provenance, supply-chain]

# Dependency graph
requires:
  - phase: 01-repository-toolchain-ci-skeleton
    provides: ci.yml's SHA-pin/least-privilege/concurrency conventions, tests/policy/test_supply_chain_guards.py's problems()-returning checker idiom, tests/policy/test_workflow_secrets.py's SEC-10 secret/permission claim (both mirrored/extended here)
provides:
  - .github/workflows/publish.yml — merge-triggered build+push+SBOM+provenance+cosign-sign+trivy-scan pipeline for csv-processor, tagged by full git SHA
  - tests/policy/test_publish_workflow_guards.py — 9 non-vacuous, mutation-tested structural proofs of publish.yml's supply-chain claims
  - tests/policy/test_workflow_secrets.py's SEC-10 re-audit — GITHUB_TOKEN + ALLOWED_PERMISSION_WIDENING allowlists, documented and re-audited
  - A resolved, confirmed repository owner value (KonuTech) for plan 11-03's Kyverno work to reuse
affects: [11-02 (generalizes this to 3 images + PR-tag variant, D-09), 11-03 (Kyverno admission policy needs a real signed image as its positive-case test — NOT YET PRODUCED, see Next Phase Readiness)]

# Tech tracking
tech-stack:
  added: [docker/setup-buildx-action@v4.3.0, docker/login-action@v4.6.0, docker/build-push-action@v7.3.0, sigstore/cosign-installer@v4.1.2 (cosign CLI v3.1.3), aquasecurity/trivy-action@v0.36.0]
  patterns: ["publish.yml separate from ci.yml with its own push trigger, never gated on make check (D-06)", "cosign sign by digest (steps.build.outputs.digest), never by tag", "job-scoped permissions widening with an explicit, exact-match test allowlist (ALLOWED_PERMISSION_WIDENING) rather than a blanket exemption"]

key-files:
  created: [.github/workflows/publish.yml, tests/policy/test_publish_workflow_guards.py, .planning/phases/11-ci-cd-completion-operations/deferred-items.md]
  modified: [tests/policy/test_workflow_secrets.py]

key-decisions:
  - "Every action SHA was resolved live via `git ls-remote --tags <repo> refs/tags/<version>` at execution time, never hand-written from memory (5 network resolutions, all confirmed 40-hex-char)."
  - "Task 3's `git push origin main` + live-run-watch + docker pull + cosign verify steps were NOT executed by this worktree-isolated parallel executor — deferred to post-merge (see Next Phase Readiness). This is the plan's single most consequential deviation."
  - "tests/policy/test_workflow_secrets.py's SEC-10 claim was extended (GITHUB_TOKEN + ALLOWED_PERMISSION_WIDENING) rather than weakened — each addition is an exact-match allowlist, not a blanket exemption, and both are proven non-vacuous by new mutation tests."
  - "4 pre-existing, unrelated tests/policy failures (DAG line budget, DAG thinness x2, bad-samples lint gate — all Phase 9/10 artifacts) were found during the mandated regression check and deliberately left unfixed, logged to deferred-items.md."

patterns-established:
  - "Pattern: a workflow file's own `permissions:` widening is proven safe via a companion exact-match allowlist test (ALLOWED_PERMISSION_WIDENING), not by relaxing the underlying least-privilege assertion. Future workflows widening permissions should extend this same dict, one entry per (workflow, job), pinning the exact scope set."

requirements-completed: [CICD-06, CICD-08]

# Metrics
duration: ~55min (approximate — exact start timestamp not captured; first action performed was the mandatory worktree HEAD-safety/base-commit-reset check)
completed: 2026-08-22
---

# Phase 11 Plan 01: Publish csv-processor to GHCR (build+SBOM+cosign+trivy) Summary

**`.github/workflows/publish.yml` builds, SBOM/provenance-attests, cosign-signs and trivy-scans csv-processor on every push to main, tagged by full git SHA — proven structurally by 9 new mutation-tested policy checks, but NOT YET proven live: the plan's own Task 3 (push to main, watch the run, `docker pull` + `cosign verify` the real published image) was deliberately not executed by this parallel worktree agent and remains open work.**

## Performance

- **Duration:** ~55 min (approximate)
- **Completed:** 2026-08-22T18:01:54Z
- **Tasks:** 2 of 3 fully executed and committed; Task 3 partially executed (local regression check only — see Deviations)
- **Files modified:** 4 (3 created, 1 modified)

## Accomplishments
- `.github/workflows/publish.yml` exists, is valid YAML, and every one of Task 1's 9 acceptance criteria passes by direct inspection (verified via `python3 -c "import yaml..."`, `grep -c`, and a parsed-YAML regex check against every `uses:` line).
- 9 new tests in `tests/policy/test_publish_workflow_guards.py` prove the workflow's supply-chain claims non-vacuously — including 2 mutation tests whose checkers were spot-checked by hand (temporarily disabling one assertion, confirming the corresponding test then fails, then reverting) per the task's own `<done>` requirement.
- Found and fixed a real regression Task 1's own change caused: `tests/policy/test_workflow_secrets.py`'s SEC-10 claim ("no workflow references a repository secret," "no job widens permissions") broke the moment `publish.yml` existed — exactly as that module's own docstring anticipated ("expected to grow in Phase 11"). Fixed with a documented, re-audited, exact-match allowlist extension (not a blanket relaxation), itself proven non-vacuous by a new test.
- Ran the full `uv run pytest tests/policy -q` regression check (145 tests) as Task 3 instructs, triaged every failure, fixed the 2 in-scope ones, and logged the 4 confirmed-pre-existing/unrelated ones to `deferred-items.md` with `git show <base-commit>` evidence that they predate this plan.

## Task Commits

Each task was committed atomically:

1. **Task 1: Create publish.yml — csv-processor build, SBOM, cosign sign, trivy scan** - `5f78d69` (feat)
2. **Task 2: Mutation-tested policy guard for publish.yml** - `d47d426` (test) — bundled with the directly-caused `tests/policy/test_workflow_secrets.py` fix and `deferred-items.md` (see Deviations; task_commit_protocol's per-task atomicity was preserved, this is Task 2's one commit, not a separate deviation commit, because the fix was discovered and resolved while completing Task 2's own regression-check work)
3. **Task 3: Push, watch the live run, verify the published artifact** - NOT EXECUTED (deliberately deferred — see Deviations and Next Phase Readiness)

**Plan metadata:** (this commit, `docs(11-01): complete publish.yml plan`, made by the orchestrator after merge per worktree-mode convention — not created by this agent)

## Files Created/Modified
- `.github/workflows/publish.yml` - New workflow: build+push+SBOM+provenance (docker/build-push-action) + cosign keyless sign-by-digest + trivy HIGH/CRITICAL gate, triggered only on push to main (D-02), permissions scoped to the job (D-13/T-11-03)
- `tests/policy/test_publish_workflow_guards.py` - 9 tests: SHA-pin, permission-scope, git-sha tagging, SBOM/provenance, cosign-by-digest, no-COSIGN_EXPERIMENTAL, trivy-gate, plus 2 mutation/non-vacuity tests
- `tests/policy/test_workflow_secrets.py` - Added `GITHUB_TOKEN` to `ALLOWED_SECRETS`, added `ALLOWED_PERMISSION_WIDENING` (exact-match allowlist, not blanket), rewrote the docstring's re-audit section, fixed a stale hardcoded-empty-set assertion (`test_the_allowed_secrets_set_is_unchanged_by_d14`), added `test_widening_the_allowlisted_job_beyond_its_pinned_scopes_is_reported`
- `.planning/phases/11-ci-cd-completion-operations/deferred-items.md` - Logs 4 pre-existing, unrelated `tests/policy` failures found during the mandated regression check (DAG line budget, DAG thinness x2, bad-samples lint gate), all traced via `git show` to Phase 9/10 files already broken at this plan's base commit

## Decisions Made
- Reused `actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1` verbatim from `ci.yml` (confirmed via `git ls-remote` to be the same commit the tag still points to) rather than re-resolving it independently, per the plan's own instruction.
- Kept the `COSIGN_EXPERIMENTAL`-explaining comment worded to avoid the literal string `COSIGN_EXPERIMENTAL` itself, after discovering the acceptance criterion's `grep -c 'COSIGN_EXPERIMENTAL' ... -eq 0` check is a blind text match with no comment-awareness — the first draft's own explanatory comment briefly broke this criterion.
- `ALLOWED_PERMISSION_WIDENING` is keyed by exact `(workflow_name, job_id)` and stores the EXACT permission dict allowed, not a boolean "this job may widen" flag — a later edit adding a third scope to the same already-exempted job is still caught (proven by a dedicated new test), matching this codebase's existing precedent of exact-match rather than presence-only checks (`FORBIDDEN_LITERAL_KEYS`, image-tag-agreement checks in `test_supply_chain_guards.py`).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `COSIGN_EXPERIMENTAL` acceptance-criterion false failure caused by my own explanatory comment**
- **Found during:** Task 1, immediately after writing `publish.yml`
- **Issue:** The plan's own verify script does `grep -c 'COSIGN_EXPERIMENTAL' ... -eq 0`. My first draft's comment explaining that `COSIGN_EXPERIMENTAL` is deliberately never set contained the literal string `COSIGN_EXPERIMENTAL`, making the grep count 1, not 0.
- **Fix:** Reworded the comment to describe "the legacy experimental-keyless env var" without spelling out its exact name.
- **Files modified:** `.github/workflows/publish.yml`
- **Verification:** `grep -c 'COSIGN_EXPERIMENTAL' .github/workflows/publish.yml` now returns 0; re-ran all other Task 1 acceptance checks, all still pass.
- **Committed in:** `5f78d69` (Task 1 commit)

**2. [Rule 1 - Bug] `tests/policy/test_publish_workflow_guards.py` first draft failed ruff (PERF401 x2, E501 x2) and ruff format**
- **Found during:** Task 2, immediately after writing the test file
- **Issue:** Two for-loops appending to a list instead of `list.extend`, and two assert-message lines over the 100-char limit.
- **Fix:** Rewrote both loops as generator-expression `.extend()` calls; wrapped the two long assert messages onto their own lines. Re-ran `ruff check` and `ruff format --check` — both clean.
- **Files modified:** `tests/policy/test_publish_workflow_guards.py`
- **Verification:** `uv run ruff check` and `uv run ruff format --check` both pass; `uv run pytest tests/policy/test_publish_workflow_guards.py -q` still 9/9 passing after the rewrite.
- **Committed in:** `d47d426` (Task 2 commit)

**3. [Rule 1/3 - Bug caused by this plan's own change, blocking `make check`] `tests/policy/test_workflow_secrets.py`'s SEC-10 claim broken by `publish.yml`'s existence**
- **Found during:** Task 2/3 boundary, while running `uv run pytest tests/policy -q` (Task 3's instructed regression check)
- **Issue:** `publish.yml` is the first workflow to reference `secrets.GITHUB_TOKEN` (for `docker/login-action`) and to widen job permissions beyond `contents: read` (for cosign OIDC + GHCR push). `test_workflow_secrets.py`'s `ALLOWED_SECRETS` was hardcoded empty and `permission_problems()` required every job's permissions to equal exactly `{"contents": "read"}` — both are collected by `make check`'s `policy` target (not deselected), so `make check` would go red on `main` the moment this plan merges, undoing this whole plan's purpose.
- **Fix:** Added `GITHUB_TOKEN` to `ALLOWED_SECRETS` (module docstring rewritten with the re-audit it explicitly requires — confirmed `docker/login-action` never echoes the credential, no `run:` step in `publish.yml` references it, `env_dump_problems` independently covers the whole workflow for leak patterns). Added `ALLOWED_PERMISSION_WIDENING`, an exact-match `(workflow, job) -> permission dict` allowlist, and updated `permission_problems()`/its two call sites to consult it. Fixed a third, previously-hardcoded-empty assertion (`test_the_allowed_secrets_set_is_unchanged_by_d14`) to check equality against the new Phase-11 baseline instead of `frozenset()`. Added a new non-vacuity test (`test_widening_the_allowlisted_job_beyond_its_pinned_scopes_is_reported`) proving the allowlist is exact-match, not a blanket exemption.
- **Files modified:** `tests/policy/test_workflow_secrets.py`
- **Verification:** `uv run pytest tests/policy/test_workflow_secrets.py -q` — 18/18 passing (was 16/18 before the fix, with the 2 failures being exactly the ones this fix targets). `uv run ruff check`/`ruff format --check` both clean (one auto-fixed Yoda-condition finding, SIM300).
- **Committed in:** `d47d426` (Task 2 commit)

---

**Total deviations:** 3 auto-fixed (2 Rule 1 bugs in my own new code, 1 Rule 1/3 bug this plan's own change caused in a pre-existing file) — all narrowly scoped, all re-verified.
**Impact on plan:** All three were necessary for correctness (the workflow's own acceptance criteria) or for the repository's build to stay green (`make check`'s `policy` target). No scope creep — none touched files outside what Task 1/2's own actions directly implicated.

### NOT auto-fixed — deliberately deferred (see below, this is the significant one)

**Task 3's `git push origin main` + live-run-watch + `docker pull` + `cosign verify` steps were not executed.**

This is not a Rule 1-3 auto-fix and not a Rule 4 architectural question needing a mid-plan decision — it is a structural consequence of the execution context this plan ran in, and no alternative action within that context would have been safe:

- This agent runs as ONE of 5 PARALLEL worktree executors in Wave 1 of this phase, each on its own isolated `worktree-agent-*` branch. The orchestrator merges all wave branches to `main` AFTER every agent returns — that ordering is the whole point of the wave/worktree model.
- `git push origin main` from inside an unmerged worktree branch would push ONLY this plan's 2 commits directly onto the shared `main` ref, bypassing the orchestrator's merge step entirely and racing against whichever of the other 4 wave-1 agents pushes next.
- Worse, a successful push would immediately trigger `publish.yml` for real against a `main` that has NOT yet received the rest of Wave 1's work — publishing a real, signed, GHCR-hosted image built from an incomplete, non-final tree, which is exactly the kind of premature real-world side effect (a signed artifact, a Sigstore/Rekor transparency-log entry — neither of which can be un-published) the orchestrator-owns-shared-state model exists to prevent.
- The plan's own Task 3 text ("this repository's established direct-push workflow") was written assuming a single, serial, main-branch-resident executor session (matching the project's Phase 1-10 convention per project memory: "direct pushes to main are intentional through Phase 10") — a precondition that does not hold for a parallel worktree agent in Phase 11.

**What was done instead:**
- Ran `uv run pytest tests/policy -q` (Task 3's first instructed sub-step) — see Accomplishments/deviations above for the full triage.
- Resolved the repository owner value WITHOUT pushing: `git remote get-url origin` → `git@github.com:KonuTech/airflow-platform.git` → owner is **`KonuTech`**, independently cross-confirmed against `docker/csv-processor/Dockerfile`'s already-committed `org.opencontainers.image.source="https://github.com/KonuTech/airflow-platform"` OCI label. This is safe to reuse for plan 11-03 — it will not change.
- Confirmed `gh` is NOT currently on `PATH` in this environment (Task 3's own contingency branch — "If `gh` is not on PATH, install it..." — will need to be exercised by whoever runs this step).

**What still needs to happen, by the orchestrator or a human, after this wave merges to `main`:**
1. Merge Wave 1 (including this plan's 2 commits) to `main`.
2. `git push origin main` (or simply observe the orchestrator's own merge-and-push, if that's how the merge lands) — this will trigger `publish.yml` for real for the first time.
3. Follow Task 3's action text verbatim from that point: resolve the pushed commit's full SHA (`git rev-parse HEAD` on `main`), poll `gh run list --workflow=publish.yml --branch=main --limit=1 --json databaseId --jq '.[0].databaseId'` then `gh run watch <id> --exit-status`, then `docker pull ghcr.io/KonuTech/csv-processor:<full-sha>` (anonymous, no login — D-04), then install cosign and run `cosign verify --certificate-identity-regexp "^https://github.com/KonuTech/airflow-platform/\.github/workflows/publish\.yml@refs/heads/main$" --certificate-oidc-issuer https://token.actions.githubusercontent.com ghcr.io/KonuTech/csv-processor:<full-sha>`.
4. Record the resulting verified `ghcr.io/KonuTech/csv-processor:<full-sha>` reference somewhere plan 11-03 can find it (this SUMMARY cannot carry it, since it does not exist yet) — updating this file's frontmatter/body, or a fresh note, is the natural place.
5. Confirm the plan's own success criteria that depend on this: "A real merge to main produced a real signed+scanned image in GHCR, confirmed by `docker pull` and `cosign verify` run independently of the workflow" is **NOT YET met** by this plan's execution alone.

## Issues Encountered
- The worktree's initial HEAD was on a stale commit (`13417ba0...`, NOT an ancestor of the expected base `0bcc4652a5c74609dc16dbf2df574bc043ed4860`) at session start. Resolved per the mandated `<worktree_branch_check>` protocol: confirmed HEAD was on a safe `worktree-agent-*` branch (not a protected ref) before running `git reset --hard 0bcc4652a5c74609dc16dbf2df574bc043ed4860`, then re-verified `git rev-parse HEAD` matched. No data loss — this worktree had no prior commits of its own.
- The sandboxed Bash tool rejected several multi-line/chained commands (including ones with no git operations at all) as "too complex to verify... stays inside the worktree." Worked around by splitting every command into single, plain, independent invocations throughout the session.
- A transient classifier timeout ("claude-sonnet-5 is temporarily unavailable") interrupted one `ruff format --check` call; retried successfully on the next attempt with no other effect.

## User Setup Required
None - no external service configuration required. (Task 3's deferred live-verification step needs `gh` installed and a `docker`/`cosign` CLI available in whatever environment performs it — see Deviations above — but that is automation for a future session, not manual user configuration.)

## Next Phase Readiness
- **Blocking concern for plan 11-03:** its Kyverno live-proof task needs "a real signed image as its positive-case test" (per this plan's frontmatter `key_links`), reusing the image reference this plan's Task 3 was supposed to record. That reference does not exist yet — plan 11-03 (or whoever merges this wave) must complete this plan's deferred Task 3 first, or plan 11-03 needs its own equivalent live-publish step.
- `.github/workflows/publish.yml` itself is complete, valid, and structurally proven (9 passing non-vacuous policy tests) — ready to run for real the moment it reaches `main`.
- `tests/policy` is green except the 4 pre-existing, unrelated, logged-and-deferred failures in `deferred-items.md` (all Phase 9/10 DAG-file issues) — `make check` will still be red on `main` post-merge for THOSE reasons, independent of anything this plan did. Worth flagging to whoever owns Phase 9/10 follow-up.
- Repository owner (`KonuTech`) is confirmed and stable for plan 11-02/11-03 to reuse without re-resolving it.

## Self-Check: PASSED

- FOUND: `.github/workflows/publish.yml`
- FOUND: `tests/policy/test_publish_workflow_guards.py`
- FOUND: `tests/policy/test_workflow_secrets.py`
- FOUND: `.planning/phases/11-ci-cd-completion-operations/deferred-items.md`
- FOUND: commit `5f78d69` (`git log --oneline --all | grep -q 5f78d69`)
- FOUND: commit `d47d426` (`git log --oneline --all | grep -q d47d426`)

No missing items.

---
*Phase: 11-ci-cd-completion-operations*
*Completed: 2026-08-22*
