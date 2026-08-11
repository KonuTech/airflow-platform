# Branch protection on the default branch

**Status: NOT YET APPLIED.** This document specifies the rule; it does not assert that the rule
exists. At the time of writing, `gh api "repos/KonuTech/airflow-platform/branches/main/protection"`
returns `404 Branch not protected`. See [Applying the rule](#applying-the-rule) for the one command
that changes that, and [Why this is not applied yet](#why-this-is-not-applied-yet) for what blocked
it.

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

## Why this is not applied yet

Plan 01-09 executed in an environment whose permission layer denied both `git push` to the remote
and `gh run list`. Two consequences, in order:

1. The repository has **no workflow run history** — `origin/main` is still at the last planning
   commit, which predates `.github/workflows/ci.yml`. The workflow has therefore never reported a
   check, and there are no names to read.
2. The plan explicitly prohibits guessing a required check name, because a name that matches
   nothing produces a rule that blocks nothing. With no run to read from, applying the rule would
   have meant violating that prohibition.

So the rule was specified rather than applied. The names in the table above are derived from the
workflow file and are the *expected* values — they are not observed values, and the
`gh run view --json jobs` step above is what turns one into the other.

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

### The one claim that stays human-owned regardless

That a failing required check blocks the **merge** and not merely the build cannot be established
from the working tree, and is not established by the read-back either. Confirm it once, by hand:

1. Branch off `main`, add a commit that fails the gate — for example a bare `print()` in
   `packages/dataplat/src/dataplat/version.py`, which trips ruff `T201`.
2. Push the branch and open a pull request.
3. On the pull request page, confirm the merge button is **blocked by the failing required check**,
   rather than the check merely showing a red mark beside an enabled button.
4. Close the pull request and delete the branch.

Recorded in `01-VALIDATION.md` § Manual-Only Verifications as `01-09/T2 (manual)`.
