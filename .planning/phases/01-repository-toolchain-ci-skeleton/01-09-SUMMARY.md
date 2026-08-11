---
phase: 01-repository-toolchain-ci-skeleton
plan: 09
subsystem: ci-enforcement
tags: [branch-protection, required-checks, validation-map, blocked, human-owned]
status: blocked

requires:
  - phase: 01-05
    provides: the gate meta-verification, and CICD-02/SEC-02/SEC-10 deliberately left open for this plan
  - phase: 01-08
    provides: the closed 69-declaration corpus, without which `make ci` cannot reach the end of its chain
provides:
  - docs/ci-branch-protection.md — the required-checks rule specified field by field, with the exact applying and reversing commands and the two rot paths
  - a per-task verification map with 26 of 30 rows observed green by running the real commands
  - a behavioural verification of the regression provenance hook, which no policy test covered
  - an explicit, committed record of what this phase did NOT establish
affects:
  - the repository owner, who must apply the branch rule and publish the first CI run
  - CICD-01, CICD-02, SEC-02, SEC-10 — all four remain OPEN and are NOT marked complete
  - Phase 2, which inherits an unprotected default branch until the owner acts

actuals:
  tokens: 7000
  tasks: 2
  commits: 2

tech-stack:
  added: []
  patterns:
    - A required check name is read from a reported run, never guessed — the API accepts a context matching nothing and produces a rule that blocks nothing
    - When an external action cannot be performed, specify it in a committed document and mark the verification human-owned rather than manufacturing a green row
    - A claim with no automated test is verified behaviourally before being marked green, or it is not marked green

key-files:
  created:
    - docs/ci-branch-protection.md
  modified:
    - .planning/phases/01-repository-toolchain-ci-skeleton/01-VALIDATION.md
    - .planning/WINDOWS.md

key-decisions:
  - "The branch rule was NOT applied. The plan prohibits guessing a required check name, and names must be read from a reported run. The repository has no run history — origin/main predates .github/workflows/ci.yml — and the environment denied both `git push` and `gh run list`. Applying the rule would have meant violating the plan's own prohibition in order to produce a green row."
  - "Task ordering was inverted relative to the plan and then blocked. Task 1 needs check names from a run; Task 2 produces that run. Push-before-protect is forced by the plan's own text, not a workaround — but the push was denied, so neither half completed."
  - "26 of 30 map rows were observed by running the real commands rather than by trusting predecessor SUMMARYs. `make ci` was executed to completion in this worktree; the four rows it does not cover were run individually."
  - "01-04/T3 was verified behaviourally rather than assumed. tests/regression/ is empty and no policy test asserts the collection hook, so the claim was untested. A scratch provenance-less module was observed making pytest exit 2, then deleted."
  - "CICD-01, CICD-02, SEC-02 and SEC-10 are NOT marked complete. 01-05 held the last three open specifically for this plan; this plan did not earn them."
  - "01-VALIDATION.md frontmatter set to status: validated + nyquist_compliant: false — the documented PARTIAL state — rather than draft, because the strategy was executed and 26 rows are green, but four gaps remain."

requirements-completed: []

coverage:
  - id: D1
    description: "The required-checks rule is specified reproducibly and reversibly from a committed document"
    requirement: "CICD-02"
    verification:
      - kind: unit
        ref: "test -f docs/ci-branch-protection.md && grep -q 'gh api' && grep -q 'enforce_admins' -> RULE_DOCUMENTED"
        status: pass
    human_judgment: false
  - id: D2
    description: "The rule is actually APPLIED to the default branch and reads back with both checks required"
    requirement: "CICD-02, SEC-02"
    verification:
      - kind: integration
        ref: "gh api repos/KonuTech/airflow-platform/branches/main/protection -> 404 Branch not protected"
        status: fail
    human_judgment: true
    rationale: "NOT DONE. The rule was specified, not applied. Requires the repository owner to publish the first CI run and then run the documented PUT. Recorded in 01-VALIDATION.md § Outstanding and in .planning/WINDOWS.md."
  - id: D3
    description: "A real workflow run on the default branch completed successfully with both jobs"
    requirement: "CICD-01, SEC-10"
    verification:
      - kind: integration
        ref: "gh run list --branch main --workflow CI — command denied by the environment; no run has ever happened on this repository"
        status: fail
    human_judgment: true
    rationale: "NOT OBSERVED, and explicitly not claimed. origin/main is 49 commits behind local main and predates the workflow file. `git push origin main:main` was denied by the environment's permission classifier."
  - id: D4
    description: "The local gate is green end to end, running the identical targets CI runs"
    requirement: "CICD-02, CICD-03, CICD-04, QUAL-01, QUAL-02, QUAL-08, OBS-03, SEC-02, SEC-11"
    verification:
      - kind: integration
        ref: "`make ci` exit 0 — lock-check, ruff check, ruff format, mypy (10 files), lint-imports (1 contract kept), 56 policy tests, 60 unit tests at 100% coverage, 70 fixtures matching the oracle, 55 commits scanned with no leaks, SEC-11 self-test live"
        status: pass
    human_judgment: false
  - id: D5
    description: "The per-task verification map records observed statuses for all 30 rows across plans 01-01 to 01-09"
    requirement: "all 12 phase requirements"
    verification:
      - kind: unit
        ref: "grep -c '⬜ pending' 01-VALIDATION.md -> 1, and that one occurrence is the status legend on line 101, not a row"
        status: pass
    human_judgment: false
  - id: D6
    description: "The regression provenance hook rejects a module with no BUG line"
    requirement: "QUAL-07"
    verification:
      - kind: integration
        ref: "scratch module written to tests/regression/ -> `pytest tests/regression` exit 2, 'has no bug-provenance line'; module deleted, `git status --short` empty"
        status: pass
    human_judgment: false
  - id: D7
    description: "A failing required check blocks the merge, not merely the build"
    requirement: "CICD-02, ROADMAP success criterion 1"
    verification: []
    human_judgment: true
    rationale: "The one claim in this phase that cannot be made from the working tree. Requires the rule to exist first, then a human to open a deliberately-failing pull request and confirm the merge button is blocked. Procedure recorded in docs/ci-branch-protection.md and 01-VALIDATION.md § Manual-Only Verifications."

duration: 13 min
completed: 2026-08-11
---

# Phase 1 Plan 09: Enforcing the Gate Summary

**The branch rule was specified, documented and made reversible — but it was NOT applied, and no CI run was observed, because the execution environment denied both `git push` and `gh run list`. This plan therefore closes the verification map honestly at 26 of 30 rows and leaves CICD-01, CICD-02, SEC-02 and SEC-10 open rather than manufacturing the green rows it could not earn.**

## Performance

- **Duration:** 13 min
- **Tasks:** 2 attempted, 0 fully complete (both partial)
- **Files created:** 1
- **Files modified:** 2

## What was established

- **The precondition held, and was checked before anything else.** `gh auth status` reports an authenticated account with `ADMIN` permission on the repository; `gh repo view` reports `visibility: PUBLIC` (so RESEARCH assumption A4 is genuinely resolved); and `make gitleaks` exited 0 over the full history — 54 commits scanned, no leaks, on both the `git --log-opts=--all` and `dir` passes. Publication is the irreversible step in this area and the green scan is its gate; that gate is satisfied.

- **`docs/ci-branch-protection.md` is a complete, reproducible specification.** All six top-level payload fields with the reasoning for each, the exact `gh api --method PUT` that applies the rule, the exact `gh api --method DELETE` that reverses it, and six separate read-back assertions — including the note that `enforce_admins`, `allow_force_pushes` and `allow_deletions` are *written* as booleans but *read back* as objects with an `.enabled` member, which is the kind of asymmetry that turns a read-back into a false green.

- **Both rot paths are documented, which is the part that decays quietest.** First, `enforce_admins: true` looks like an obvious hardening in review and is in fact a self-lockout: GSD runs this repository with `branching_strategy: "none"`, so phases 2 through 11 commit straight to the default branch and admin enforcement would refuse them. The reversal command is given inline so the reader who flips it can undo it. Second, the required check names are coupled to the workflow's job display names — renaming `Quality gate` or `Secret scan (full history)` silently un-requires that check, because the API validates nothing at configuration time.

- **The local gate is green end to end.** `make ci` exit 0: `uv lock --check`, `ruff check`, `ruff format --check`, `mypy` over 10 source files, `lint-imports` (contract kept), 56 policy tests, 60 unit tests at 100% coverage, 70 fixtures byte-matching the committed oracle, 55 commits scanned with no leaks, and the SEC-11 self-test confirming the scanner exits 1 on a disposable repository with path-scoped allowlists.

- **26 of 30 map rows were observed, not inherited.** Statuses were obtained by running the commands in this worktree rather than by reading what plans 01-01 through 01-08 said about themselves. The four rows `make ci` does not cover were run individually: `git ls-files tests/fixtures/csv` (0 entries), the pull-request template grep, the ADR template grep, and `grep -L 'Migration trigger'` across all five taken ADRs (no output — all five carry a reversal trigger).

- **One row was upgraded from assumed to observed.** 01-04/T3 claims that a provenance-less module under `tests/regression/` makes the suite exit non-zero. `tests/regression/` is empty and a grep showed no policy test references it, so nothing in the committed suite asserted that behaviour — the row would have been marked green on the strength of a predecessor's prose. A scratch module without a `# BUG:` line was written there instead; `pytest tests/regression` exited **2** with the hook's "has no bug-provenance line" error; the module was deleted and `git status --short` confirmed clean.

## What was NOT established

Stated plainly, because the entire point of this plan's threat T-01-47 is that a verification map must not claim more than was observed.

- **The branch rule is not applied.** `gh api "repos/KonuTech/airflow-platform/branches/main/protection"` returns `404 Branch not protected`, and did so before and after this plan.
- **No workflow run has ever happened on this repository.** `origin/main` sits 49 commits behind local `main` at the last planning commit, which predates `.github/workflows/ci.yml`.
- **Merge-blocking is unconfirmed.** It cannot be confirmed while the rule does not exist.
- **CICD-01, CICD-02, SEC-02 and SEC-10 are not marked complete.** 01-05 held the last three open specifically because this plan also declares them; this plan did not earn them, so they stay open.

## Why — and why nothing was faked

Two environment denials, both hard:

```
git push origin main:main   -> Permission denied by the Claude Code auto mode classifier
gh run list --repo ...      -> Permission denied by the Claude Code auto mode classifier
```

These are not incidental. The plan's Task 1 instructs that check names be read **from the repository's own run history rather than off the workflow file**, and its prohibitions say a required check name "must not be guessed — a name that does not match a reported check silently never becomes required, and the rule then blocks nothing". Task 2 produces the run that Task 1 needs to read, so the real order is push → read names → apply → read back.

With the push denied there is no run; with no run there are no reported names; and with no reported names the only way to have applied a rule was to guess `Quality gate` and `Secret scan (full history)` from the workflow file. Those two strings are almost certainly correct — but "almost certainly" is exactly the failure mode T-01-46 describes, and a rule built on an unverified name is a rule that blocks nothing while looking like it blocks everything. Guessing would have produced four green rows and a security control that might not exist. It was left blocked.

The names are recorded in `docs/ci-branch-protection.md` as **expected, not observed**, with the `gh run view --json jobs` command that converts one into the other.

## Task Commits

1. **Task 1 (partial): the required-checks rule, specified** — `6326af7` (docs)
2. **Task 2 (partial): the verification map, with observed statuses** — `0fb5de4` (docs)

## Deviations from Plan

**1. [Rule 3 — Blocker, escalated] Task 1 could not be executed as written**

- **Found during:** Task 1, at the first action step.
- **Issue:** the action requires reading check names from run history; no run history exists, and creating one requires a push that the environment denies.
- **Action taken:** the reversible, in-repository half of Task 1 was completed in full (the specification document, which the plan lists as its only `<files>` entry and which satisfies its `RULE_DOCUMENTED` verify). The irreversible, external half was not performed and is not claimed. Escalated as a checkpoint rather than worked around.
- **Commit:** `6326af7`

**2. [Rule 2 — Missing critical] a map row would have been marked green on untested behaviour**

- **Found during:** Task 2, while filling 01-04/T3.
- **Issue:** the row's mechanism — a provenance-less regression module failing collection — is asserted by no committed test, and `tests/regression/` is empty, so `pytest tests/regression` exits 5 (no tests) in the normal case. Marking it green would have been the same category of error the plan's T-01-47 forbids.
- **Fix:** verified behaviourally with a scratch module, observed exit 2, deleted it, confirmed the tree clean.
- **Commit:** `0fb5de4`

**3. [Scope] `01-VALIDATION.md`'s branch-protection note contradicted reality**

The seeded note read "Plan 01-09 configures it through the API". After this plan that sentence would have been false in a document whose purpose is to be true. Rewritten to say the rule was specified but not applied, with a pointer to the new § Outstanding section.

---

**Total deviations:** 1 blocker escalated, 1 missing-critical auto-fixed, 1 scope correction.
**Impact:** Task 1 half-delivered, Task 2 delivered except for its CI-run dependency. No requirement was marked complete on unearned evidence.

## Known Stubs

None in code — this plan added no code. The four unmet claims are tracked as blocked rows in `01-VALIDATION.md` § Outstanding and as ledger entry 2 in `.planning/WINDOWS.md`, not as stubs.

Ledger entry 1 (the gitleaks installer's corrupted-download behaviour, handed over by 01-05) is **left open and untouched** — this plan's scope did not admit a test module for it.

## Threat Flags

None introduced. This plan added one markdown document and edited two planning files.

Threat dispositions from this plan's register, honestly reported:

| Threat | Disposition | Actual state |
|---|---|---|
| T-01-43 (merging past a failing gate) | mitigate | **NOT mitigated** — rule unapplied |
| T-01-43b (rule locking out the sole maintainer) | mitigate | Mitigation *designed* and documented (`enforce_admins: false`, `strict: false`, count 0) but not yet in force |
| T-01-44 (publishing a history containing a credential) | mitigate | **Satisfied** — full-history scan green, 54 commits, before any push was attempted |
| T-01-45 (force push erasing gated history) | mitigate | **NOT mitigated** — rule unapplied |
| T-01-46 (a required check name matching nothing) | mitigate | **Avoided by not applying the rule.** The coupling is documented as the rot path and the read-from-a-real-run command is recorded |
| T-01-47 (a map claiming more than was observed) | mitigate | **Satisfied** — this is the threat that drove every decision above; the two documented partial verifications stay partial and four rows stay blocked |

## Issues Encountered

The two permission denials above. No authentication gate: `gh` is authenticated and `gh api` reads succeeded (repo visibility, branch-protection 404) — it is the write path and the run-history read that are denied, not the credential.

## Next Phase Readiness

**Blocked on the repository owner.** Four commands, in order, from `docs/ci-branch-protection.md`:

1. `git push origin main` — publishes 49 commits and triggers the first CI run. The safety gate for this (a green full-history secret scan) was verified during this plan and held.
2. `gh run view --json jobs,conclusion` — confirm both jobs ran and the run went green, and read the two check names verbatim.
3. The documented `gh api --method PUT` with those names, then the six read-back assertions.
4. By hand: a throwaway branch with a failing commit, a pull request, and confirmation that the merge button is blocked by the failing required check rather than merely showing a red mark.

Only after step 4 should CICD-01, CICD-02, SEC-02 and SEC-10 be marked complete and
`01-VALIDATION.md` be flipped to `nyquist_compliant: true`.

## Self-Check: PASSED

`docs/ci-branch-protection.md` present on disk and tracked. Both commits (`6326af7`, `0fb5de4`) reachable in `git log`. No file deletions in either commit. Working tree clean after the scratch-module probe was removed. `make ci` green at `0fb5de4`. `grep -c '⬜ pending'` over the map returns 1, and that occurrence is the status legend rather than a row.

The self-check deliberately does **not** assert that the plan's own `<verify>` blocks passed — five of the nine could not be run at all. That is recorded above rather than papered over.

---
*Phase: 01-repository-toolchain-ci-skeleton*
*Completed: 2026-08-11 — PARTIAL, blocked on repository owner action*
