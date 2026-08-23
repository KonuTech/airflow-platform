# Branch protection on the default branch

**Status: APPLIED and OBSERVED ENFORCING (2026-08-11).** The rule specified below exists on `main`
and has been verified to refuse a merge, not merely to report one red.

Both check names were read verbatim from a real reported run (run `31531101283`, both jobs green)
rather than guessed — see [rot path 2](#rot-path-2-the-check-names-are-coupled-to-job-display-names)
for why that distinction is load-bearing. All six [read-back assertions](#reading-the-rule-back)
were then run and matched.

Enforcement was proved by a deliberate negative test: a throwaway branch carrying a `print()` in
`packages/dataplat/src/dataplat/version.py` (a T201 / OBS-03 violation) was pushed and opened as
PR #1. The result was:

| Field | Value | What it proves |
|---|---|---|
| `Quality gate` | `failure` | The gate caught the violation on a clean runner |
| `mergeable` | `true` | The branch has no merge *conflict* |
| `mergeable_state` | `blocked` | GitHub **refused the merge** |

`mergeable: true` alongside `mergeable_state: blocked` is the pair that matters: the refusal comes
from the failing required check, not from a conflict or an unrelated obstruction. A rule built on
guessed context names would have reported `clean` here and merged through a red build. PR #1 was
closed and its branch deleted; the probe commit is not in `main`'s history.

The rule exists so that ROADMAP success criterion 1 is fully true. Without it, a failing check makes
the build red but does not stop the merge — the gate is advisory. Required status checks are what
convert "CI noticed" into "GitHub refused".

---

## The configuration

Six top-level fields, each one a decision rather than a default.

| Field | Value | Why |
|---|---|---|
| `required_status_checks.contexts` | `["Quality gate", "Secret scan (full history)"]` | The two job display names from `.github/workflows/ci.yml`. See [the coupling note](#rot-path-2-the-check-names-are-coupled-to-job-display-names) — these must be **read from a real reported run**, never guessed. |
| `required_status_checks.strict` | `false` | Strict mode requires a branch to be up to date with the base before merging. On a repository with one committer and no concurrent branches that is a rebase treadmill, and it blocks a direct push outright. |
| `enforce_admins` | `false` | **The field that decides whether the rest of this project can be built.** See [the rationale](#rot-path-1-tightening-enforce_admins-stops-the-project). |
| `required_pull_request_reviews.required_approving_review_count` | `0` | An author cannot approve their own pull request. On a single-maintainer repository any positive count makes every pull request permanently unmergeable — the rule would not be stricter, it would be broken. |
| `restrictions` | `null` | Push restrictions are an organisation-and-team feature. On a personal repository the field must be explicitly `null` rather than omitted; the API rejects the payload if it is missing. |
| `allow_force_pushes` / `allow_deletions` | `false` / `false` | These two are the protections that survive `enforce_admins: false`. The owner may push *forward* to `main` but may not rewrite or delete its history. An accidental `push --force` is the failure mode a solo maintainer actually has. |

### What this rule knowingly does not buy

With `enforce_admins: false`, the owner can still push past a red gate. That residual is accepted
deliberately and recorded as threat **T-01-43b**: on a single-maintainer repository this is a
discipline problem, not an access-control one. The alternative — admin enforcement — is not a
stricter version of this rule, it is a self-lockout (see below).

---

## Applying the rule

```bash
gh api \
  --method PUT \
  -H "Accept: application/vnd.github+json" \
  "repos/KonuTech/airflow-platform/branches/main/protection" \
  --input - <<'JSON'
{
  "required_status_checks": {
    "strict": false,
    "contexts": ["Quality gate", "Secret scan (full history)"]
  },
  "enforce_admins": false,
  "required_pull_request_reviews": {
    "required_approving_review_count": 0,
    "dismiss_stale_reviews": false,
    "require_code_owner_reviews": false
  },
  "restrictions": null,
  "allow_force_pushes": false,
  "allow_deletions": false
}
JSON
```

**Before running it,** confirm the two check names against a run that actually happened — the rule
must never be built on guessed names:

```bash
gh run list --branch main --workflow CI --limit 1
gh run view --json jobs --jq '[.jobs[].name]'
```

Expected: `["Quality gate","Secret scan (full history)"]`. If the output differs, use the reported
strings verbatim and update the table above.

Do **not** configure this through the web interface instead. A setting that exists nowhere in the
repository is not reproducible — the same standard this project applies to its cluster.

## Removing the rule

The exact inverse of the command above — same endpoint, `DELETE` instead of `PUT`:

```bash
gh api \
  --method DELETE \
  -H "Accept: application/vnd.github+json" \
  "repos/KonuTech/airflow-platform/branches/main/protection"
```

There is no migration and no dependent system. Applying and removing are both single API calls, so
this configuration is reversible at any time.

## Reading the rule back

Existence of a rule is not the assertion; the contents are. Assert each field separately:

```bash
P="repos/KonuTech/airflow-platform/branches/main/protection"

gh api "$P" --jq '.required_status_checks.contexts'   # ["Quality gate","Secret scan (full history)"]
gh api "$P" --jq '.required_status_checks.strict'     # false
gh api "$P" --jq '.enforce_admins.enabled'            # false  <- see rot path 1
gh api "$P" --jq '.required_pull_request_reviews.required_approving_review_count'  # 0
gh api "$P" --jq '.allow_force_pushes.enabled'        # false
gh api "$P" --jq '.allow_deletions.enabled'           # false
```

Note the read-back shapes differ from the write shapes: `enforce_admins`, `allow_force_pushes` and
`allow_deletions` are written as booleans but read back as objects with an `.enabled` member.

---

## How this configuration rots

Two ways, one by being tightened and one by drifting. Both are why the read-back above asserts
contents rather than existence.

### Rot path 1: tightening `enforce_admins` stops the project

`enforce_admins: true` looks like an obvious improvement in a security review. It is not, here.

GSD runs this repository with `branching_strategy: "none"`, so Phases 2 through 11 commit straight
to the default branch. With `enforce_admins: true` the repository owner becomes subject to the
pull-request requirement as well: direct pushes to `main` are refused and every remaining phase
stops. The gate stays enforcing for the pull-request path either way — a failing check still blocks
that merge — while the sole maintainer keeps the bypass that the project's own workflow depends on.

If you flip it to `true` and the project stops committing, that is the cause. Reverse it with:

```bash
gh api --method DELETE \
  "repos/KonuTech/airflow-platform/branches/main/protection/enforce_admins"
```

The `DELETE` on the `enforce_admins` sub-resource disables admin enforcement while leaving the rest
of the rule intact. Re-running the full `PUT` payload above also restores it, since that payload
sets `enforce_admins: false` explicitly.

The read-back of `.enforce_admins.enabled` exists precisely so this tightening is caught by a
failing check rather than discovered when the next phase cannot commit.

### Rot path 2: the check names are coupled to job display names

A required check name is matched against the names GitHub *reports*. There is no validation at
configuration time: a context that matches nothing is accepted silently, and the rule then requires
a check that never arrives — which either blocks every merge forever or, on the pull-request path,
blocks nothing meaningful.

The two names come from the `name:` of each job in `.github/workflows/ci.yml`:

| Workflow job key | `name:` | Required check context |
|---|---|---|
| `check` | `Quality gate` | `Quality gate` |
| `secrets` | `Secret scan (full history)` | `Secret scan (full history)` |

**Renaming a job silently un-requires its check until this rule is updated.** If you change either
`name:`, re-run the `PUT` above with the new strings in the same commit.

The workflow's own top-level `name: CI` is load-bearing for a different reason: `gh run list
--workflow CI` addresses the workflow by that string. An unnamed workflow is addressed by its file
path instead, and that query fails for a reason unrelated to the gate.

---

## How this came to be applied

Plan 01-09 executed in an environment whose permission layer denied both `git push` and
`gh run list`. That left the repository with **no workflow run history**, and therefore no check
names to read. Because the plan prohibits guessing a required check name — a name matching nothing
produces a rule that blocks nothing — the plan halted and specified the rule rather than applying
it. That halt was correct, and it is why the names in the table above are observed rather than
inferred.

The blockage was cleared by the repository owner publishing the phase, after which the sequence
below was executed and each step observed:

| # | Step | Observed result |
|---|---|---|
| 1 | Owner pushed `main`, triggering the first CI run | run `31531101283` |
| 2 | Read job names from that run | `Quality gate`, `Secret scan (full history)` — both `success` |
| 3 | Applied the `PUT` with those verbatim names | rule created |
| 4 | Ran all six read-back assertions | all matched |
| 5 | Negative test via PR #1 | `mergeable_state: blocked` (see the status header) |
| 6 | Direct push to `main` after the rule was live | succeeded — `enforce_admins: false` holds |

Step 6 is not a formality. GSD runs this repository with `branching_strategy: "none"`, so phases 2
through 11 commit straight to `main`. Had the rule blocked the owner's own push, it would have
stopped the project rather than protected it — which is precisely the failure
[rot path 1](#rot-path-1-tightening-enforce_admins-stops-the-project) describes.

### What the repository owner needs to do

```bash
# 1. Publish the phase's commits. This triggers the first CI run (push: branches: [main]).
git push origin main

# 2. Wait for it, and confirm both jobs ran and the run went green.
gh run watch
gh run view --json jobs,conclusion --jq '{conclusion, jobs: [.jobs[].name]}'

# 3. Apply the rule using the observed names (see "Applying the rule" above).

# 4. Read it back (see "Reading the rule back" above).

# 5. Confirm the owner's direct-push path still works — this is what phases 2-11 depend on.
git commit --allow-empty -m "chore: verify direct push still permitted"
git push origin main
```

The precondition for step 1 was verified during plan 01-09 and held: `make gitleaks` exited 0 over
the full history (54 commits scanned, no leaks). Publication is the irreversible step in this area
and the green full-history scan is its gate — a leaked credential in a public history cannot be
recalled, only rotated.

### The claim that could only be settled against the live repository

That a failing required check blocks the **merge** and not merely the build cannot be established
from the working tree, and is not established by the read-back either. It was settled by
observation, using exactly the procedure this section previously prescribed:

1. Branched off `main` with a bare `print()` in `packages/dataplat/src/dataplat/version.py`
   (ruff `T201`), confirmed failing locally first.
2. Pushed the branch and opened PR #1.
3. Read the decision from the API rather than the rendered page, because a red mark beside an
   enabled button and a genuinely disabled button look similar and report differently:
   `mergeable: true`, `mergeable_state: blocked`, `Quality gate: failure`.
4. Closed the pull request and deleted the branch; the probe commit is not in `main`'s history.

**Re-verify this after any change to either job's `name:`**, since that is the one edit that can
silently un-require a check while leaving this document looking correct.

Recorded in `01-VALIDATION.md` § Manual-Only Verifications as `01-09/T2`.

<!-- Throwaway comment-only edit, plan 11-02's own live-PR proof (same pattern as PR #1 above):
     opened to prove publish.yml's pr-<N> tag + sign + scan + ghcr-cleanup.yml chain live,
     end to end. Closed without merging; not part of main's real history once closed. -->
