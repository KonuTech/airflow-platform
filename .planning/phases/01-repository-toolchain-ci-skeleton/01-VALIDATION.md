---
phase: 1
slug: repository-toolchain-ci-skeleton
# status lifecycle: draft (seeded by plan-phase) → validated (set by validate-phase §6)
# audit-milestone §5.5 distinguishes NOT-VALIDATED (draft) from PARTIAL (validated + nyquist_compliant: false) (#2117)
#
# PARTIAL, deliberately. 26 of the 30 rows below were observed green on
# 2026-08-11 by running the real commands. The remaining 4 are all plan 01-09's
# own rows and all depend on a repository setting or a published run, neither of
# which the execution environment could reach: `git push` to the remote and
# `gh run list` were both denied. nyquist_compliant stays FALSE until the branch
# rule is applied and a real run is observed. See § Outstanding at the end.
status: validated
nyquist_compliant: false
wave_0_complete: true
created: 2026-08-11
statuses_observed: 2026-08-11
---

# Phase 1 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Source: `01-RESEARCH.md` § Validation Architecture (line 1180), which maps all 12 requirements
> to observable signal, mechanism and cadence.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest `9.1.1` (+ pytest-cov `7.1.0`, hypothesis `6.165.3`) — **Wave 0 installs** |
| **Config file** | `pyproject.toml` `[tool.pytest.ini_options]` — none exists yet, Wave 0 creates |
| **Quick run command** | `uv run pytest -q` |
| **Full suite command** | `make check` (ruff → mypy → import-linter → pytest → policy greps) |
| **Estimated runtime** | ~15–30 s (no cluster, no containers, no network this phase) |

**Parity requirement:** CI and local MUST invoke the same entry point, so they cannot drift.
`make check` is the single definition; the GitHub Actions job calls it rather than re-listing steps.

---

## Sampling Rate

- **After every task commit:** `uv run pytest -q`
- **After every plan wave:** `make check`
- **Before `/gsd-verify-work`:** full suite green on a clean checkout (`uv sync && make check`)
- **Max feedback latency:** 30 seconds

---

## Per-Task Verification Map

Seeded by the planner; statuses filled in by plan 01-09 task 2 on **2026-08-11**. All 12 phase
requirements appear below. `File Exists` was seeded as the state *before* the phase ran (every row
Wave 0, because the repository then contained no source, test or configuration file) and now records
the state *after* it.

**How these statuses were obtained.** Every ✅ below was observed, not inferred from a predecessor
SUMMARY. `make ci` was run to completion in the plan-09 worktree and exited 0 — `uv lock --check`,
`ruff check`, `ruff format --check`, `mypy` (10 source files), `lint-imports` (1 contract kept),
`pytest tests/policy` (56 passed), `pytest tests/unit` (60 passed, 100% coverage),
`fixtures-verify` (70 fixtures match the oracle), `gitleaks git --log-opts=--all` and
`gitleaks dir` (55 commits, no leaks), and `gitleaks-selftest` (scanner exits 1 on the disposable
repository; allowlist proven path-scoped). The four rows whose command `make ci` does not run were
executed individually — see the per-row notes.

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 01-01/T1 | 01-01 | 1 | QUAL-01, QUAL-02, OBS-03, CICD-03, CICD-04 | T-01-03, T-01-04 | Exact-pinned gating tools from one lockfile; gate defined once | meta + unit | `uv sync && make check` | ✅ | ✅ green |
| 01-01/T1 (LOAD-12) | 01-01 | 1 | — (LOAD-12, enforced for Phase 4) | T-01-04 | Architecture policy live before the code it governs | policy | `uv run --frozen pytest tests/policy/test_no_postgres_csv_parsing.py -q` | ✅ | ✅ green |
| 01-01/T2 | 01-01 | 1 | — (structure) | — | Generated artifacts un-committable by construction | structural | `git ls-files tests/fixtures/csv` is empty; `make check` | ✅ | ✅ green |
| 01-01/T3 | 01-01 | 1 | CICD-01, CICD-02, SEC-10 | T-01-01, T-01-02, T-01-05 | SHA-pinned actions; least-privilege permissions; zero secret references; workflow named `CI` | config | YAML parse assertion (`name == CI`, job display name `Quality gate`) + `grep -Ec 'secrets\.' .github/workflows/ci.yml` | ✅ | ✅ green |
| 01-01/T3 (PR template) | 01-01 | 1 | QUAL-07 | — | QUAL-07's review-time half: the regression-test checkbox no linter can replace | structural | `grep -q 'tests/regression/' .github/pull_request_template.md` | ✅ | ✅ green |
| 01-02/T1 | 01-02 | 2 | SEC-02 | T-01-08, T-01-09 | Checksum-verified scanner; path-and-prefix scoped allowlists | integration | `make gitleaks` | ✅ | ✅ green |
| 01-02/T2 | 01-02 | 2 | SEC-11 | T-01-11, T-01-08 | Scanner observed failing on non-vendor canaries; allowlist proven non-global | integration | `make gitleaks-selftest` | ✅ | ✅ green |
| 01-02/T3 | 01-02 | 2 | SEC-02, SEC-10 | T-01-10, T-01-12 | Full-history checkout; redaction on every invocation | config | YAML assertion on the `secrets` job + `make ci` | ✅ | ✅ green |
| 01-03/T1 | 01-03 | 2 | QUAL-08 | T-01-13 | Safe deserialisation into a closed, frozen model | unit | `uv run --frozen pytest tests/unit/test_corpus_manifest.py -q` | ✅ | ✅ green |
| 01-03/T2 | 01-03 | 2 | QUAL-08 | T-01-14, T-01-17 | Committed oracle; regeneration never rewrites it | integration | `make fixtures && make fixtures-verify` | ✅ | ✅ green |
| 01-03/T3 | 01-03 | 2 | QUAL-08 | T-01-15, T-01-18 | Determinism rules enforced by source inspection and by consequence | policy | `uv run --frozen pytest tests/policy -q` | ✅ | ✅ green |
| 01-04/T1 | 01-04 | 2 | QUAL-07 | T-01-19 | Decision format and numbering fixed before the first record | structural | `grep -q 'Migration trigger' docs/adr/0000-template.md` | ✅ | ✅ green |
| 01-04/T2 | 01-04 | 2 | QUAL-07 | T-01-19, T-01-21 | Every taken decision recorded with alternatives and a reversal trigger | structural | trigger-presence loop over `docs/adr/000[1-5]-*.md` | ✅ | ✅ green |
| 01-04/T3 | 01-04 | 2 | QUAL-07 | T-01-20, T-01-22 | Bug provenance enforced at collection time | policy (negative) | provenance-less module under `tests/regression/` makes `pytest tests/regression` exit non-zero | ✅ | ✅ green |
| 01-05/T1 | 01-05 | 3 | QUAL-01, QUAL-02, OBS-03, CICD-03, CICD-04 | T-01-23, T-01-28 | Every gate observed rejecting a bad sample and accepting a good one | meta | `uv run --frozen pytest tests/policy/test_gates_actually_fail.py -q` | ✅ | ✅ green |
| 01-05/T2 | 01-05 | 3 | CICD-02, CICD-03, CICD-04 | T-01-24, T-01-25 | CI delegates to make; pins agree; print-ban scope fixed | policy | `uv run --frozen pytest tests/policy/test_ci_invokes_make_only.py tests/policy/test_ci_calls_make_ci.py tests/policy/test_pinned_tool_versions_agree.py tests/policy/test_print_ban_scope.py -q` | ✅ | ✅ green |
| 01-05/T3 | 01-05 | 3 | SEC-02, SEC-10 | T-01-26, T-01-27 | Full-depth scan; mandatory redaction; empty secret set | policy | `uv run --frozen pytest tests/policy/test_secret_scan_depth.py tests/policy/test_workflow_secrets.py -q` | ✅ | ✅ green |
| 01-06/T1 | 01-06 | 4 | QUAL-08 | T-01-29 | New capability disturbs no existing byte | integration | `make fixtures-verify && git diff --exit-code tests/fixtures/CORPUS.sha256` | ✅ | ✅ green |
| 01-06/T2 | 01-06 | 4 | QUAL-08 | T-01-30, T-01-31 | Encoding and mark semantics asserted, not assumed | unit | `uv run --frozen pytest tests/unit/test_corpus_byte_level_fixtures.py -q` | ✅ | ✅ green |
| 01-06/T3 | 01-06 | 4 | QUAL-08 | T-01-32, T-01-33 | Deterministic archive and multipart output | integration + unit | `make fixtures && make fixtures-verify` + the byte-level module | ✅ | ✅ green |
| 01-07/T1 | 01-07 | 5 | QUAL-08 | T-01-35, T-01-36 | No ad-hoc generator special cases; detection declines rather than guesses | integration | `make fixtures-verify` + `git diff --stat tools/corpus/` empty | ✅ | ✅ green |
| 01-07/T2 | 01-07 | 5 | QUAL-08 | T-01-34 | Header layout declared, degenerate files distinguishable | integration | manifest count assertion + `make fixtures-verify` | ✅ | ✅ green |
| 01-07/T3 | 01-07 | 5 | QUAL-08 | T-01-34, T-01-37 | Row-shape anomalies asserted structurally | unit | `uv run --frozen pytest tests/unit/test_corpus_structural_fixtures.py -q` | ✅ | ✅ green |
| 01-08/T1 | 01-08 | 6 | QUAL-08 | T-01-38, T-01-40 | Unrecoverable damage declared as rejection; exact decimal expectations | integration | manifest count assertion + `make fixtures-verify` | ✅ | ✅ green |
| 01-08/T2 | 01-08 | 6 | QUAL-08 | T-01-39 | Explicit formats only; real zone transitions | unit | zone-resolution assertion + `uv run --frozen pytest tests/unit/test_corpus_semantic_fixtures.py -q` | ✅ | ✅ green |
| 01-08/T3 | 01-08 | 6 | QUAL-08 | T-01-41, T-01-42 | Corpus complete at 69 names, no gaps or inventions | unit | completeness assertion in `tests/unit/test_corpus_semantic_fixtures.py` | ✅ | ✅ green |
| 01-09/T1 | 01-09 | 7 | CICD-02, SEC-02 | T-01-43, T-01-44, T-01-45, T-01-46 | Required checks enforce the gate; no force push; scan green before publication | config read-back | `gh api "repos/{owner}/{repo}/branches/main/protection" --jq '.required_status_checks.contexts'` | ✅ doc only | 🔒 blocked |
| 01-09/T1 (admin bypass) | 01-09 | 7 | CICD-02 | T-01-43b | Rule enforces without locking the sole maintainer out — `enforce_admins` stays false so phases 2–11 can commit | config read-back | `gh api "repos/{owner}/{repo}/branches/main/protection" --jq '.enforce_admins.enabled' \| grep -qx false` | ✅ doc only | 🔒 blocked |
| 01-09/T2 | 01-09 | 7 | CICD-01, SEC-10 | T-01-47 | A real run observed green with both jobs present | end-to-end | `gh run list --branch main --workflow CI --limit 1 --json conclusion` | — | 🔒 blocked |
| 01-09/T2 (manual) | 01-09 | 7 | CICD-02 | T-01-43 | A failing required check blocks the merge, not only the build | human-check | none — deliberately manual; see Manual-Only Verifications | — | 👤 human-owned |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky · 🔒 blocked (external action required) ·
👤 human-owned (cannot be established mechanically)*

**Per-row notes for the four rows `make ci` does not cover:**

- **01-01/T2** — `git ls-files tests/fixtures/csv` returns 0 entries: the generated corpus is
  un-committable by construction, not by discipline.
- **01-01/T3 (PR template)** — `grep -q 'tests/regression/' .github/pull_request_template.md`
  matched.
- **01-04/T1, 01-04/T2** — `grep -q 'Migration trigger' docs/adr/0000-template.md` matched, and
  `grep -L 'Migration trigger'` over `docs/adr/0001-*` … `0005-*` returned nothing: all five taken
  decisions carry a reversal trigger.
- **01-04/T3** — verified behaviourally rather than assumed. `tests/regression/` is empty, and no
  policy test asserts the collection hook, so a scratch module without a `# BUG:` line was written
  into `tests/regression/`, `pytest tests/regression` was observed exiting **2** with the hook's
  "has no bug-provenance line" error, and the scratch module was deleted (`git status --short`
  empty afterwards). This closes what would otherwise have been an unverified claim.

**The four blocked rows are all plan 01-09's own.** Nothing in plans 01-01 through 01-08 is
outstanding. See § Outstanding.

**Requirement coverage:** QUAL-01 (01-01/T1, 01-05/T1) · QUAL-02 (01-01/T1, 01-05/T1) ·
QUAL-07 (01-01/T3 pull-request checkbox — the review-time half; 01-04/T1-T3 — the mechanical half) ·
QUAL-08 (01-03/T1-T3, 01-06, 01-07, 01-08) ·
CICD-01 (01-01/T3, 01-09/T2) · CICD-02 (01-01/T3, 01-05/T2, 01-09/T1) · CICD-03 (01-01/T1, 01-05/T1-T2) ·
CICD-04 (01-01/T1, 01-05/T1-T2) · SEC-02 (01-02/T1, 01-02/T3, 01-05/T3, 01-09/T1) ·
SEC-10 (01-01/T3, 01-02/T3, 01-05/T3, 01-09/T2) · SEC-11 (01-02/T2) · OBS-03 (01-01/T1, 01-05/T1).

---

## Requirement → Mechanism Summary

Condensed from `01-RESEARCH.md` § Validation Architecture. Full detail lives there.

| Requirement | Mechanism | Cadence |
|-------------|-----------|---------|
| QUAL-01 type hints | mypy strict (flags enumerated individually — `strict=false` in `[[tool.mypy.overrides]]` is silently ignored by mypy 2.3.0) | every PR |
| QUAL-02 docstrings | ruff `D` pydocstyle, scoped to public API | every PR — **partial**, presence not quality |
| QUAL-07 regression tests | `tests/regression/` + the collection hook that rejects a provenance-less test (01-04); the pull-request checkbox for the judgement half (01-01/T3). The LOAD-12 policy test is **not** a QUAL-07 mechanism — it is an architecture ban landed early for Phase 4 | phase acceptance |
| QUAL-08 fixture corpus | `make fixtures` + committed SHA-256 digest manifest | every PR |
| CICD-01 GitHub Actions | workflow file present and running | every PR |
| CICD-02 PR quality gate | required checks on PR | every PR |
| CICD-03 ruff | `ruff check` non-zero on violation | every PR |
| CICD-04 mypy | `mypy` non-zero on violation | every PR |
| SEC-02 no secrets in history | `gitleaks detect` over full history | every PR + phase acceptance |
| SEC-10 no secret echoed in CI | Phase-1 form ("workflow references no secret") is checkable; general form is undecidable | phase acceptance — **partial** |
| SEC-11 secret scanning in CI | negative test: a planted synthetic credential fails the build | every PR |
| OBS-03 no `print()` | ruff `T20`, scoped to library packages | every PR |

**Honest limitations carried forward from research — do not paper over:**
- **SEC-10** is partly undecidable in general (a future `curl -v` carrying a bearer token matches no grep). Its Phase-1 form is fully checkable and that is what this phase asserts.
- **QUAL-02** verifies a docstring *exists*, not that it is meaningful.
- The **gitleaks negative test must not use `AKIAIOSFODNN7EXAMPLE`** — gitleaks 8.30.1 stopword-lists it and does not flag it, so a test built on that string asserts the opposite of what it claims.

---

## Wave 0 Requirements

All five complete — observed 2026-08-11.

- [x] `pyproject.toml` — pytest config, ruff config, mypy config for both workspace members
- [x] `tests/conftest.py` — shared fixtures
- [x] `tests/unit/`, `tests/regression/` — tree established (QUAL-07 policy; `tests/regression/`
      is still empty of tests, which is correct — its collection hook is what is load-bearing)
- [x] pytest + pytest-cov + hypothesis installed via `uv sync` (36 packages, lockfile frozen)
- [x] `make check` target defined as the single CI/local entry point, and `make ci` is a strict
      superset of it computed from the Makefile prerequisite graph (asserted by
      `tests/policy/test_ci_calls_make_ci.py`)

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| A failing required check blocks the **merge**, not only the build | CICD-02, success criterion 1 | The rule's *contents* are now read back automatically (plan 01-09 task 1), but only a human can confirm the merge button is actually blocked on a real pull request | Push a throwaway branch whose commit fails the gate, open a pull request, confirm the merge is blocked by the failing required check rather than merely marked red; close the PR and delete the branch |
| CI log contains no secret values | SEC-10 | General form undecidable; needs human read of a real run's logs | Open the most recent Actions run, confirm no job echoes a credential. This phase's structural claim — the workflow references no secret at all — is automated by `tests/policy/test_workflow_secrets.py`; the general form is re-audited when the first secret is introduced (Phase 11) |
| Docstring *content* quality | QUAL-02 | Presence is mechanical (`D` rules + `D417`); purpose, assumptions, exceptions and side effects are not | Review-time, via the pull-request template checkbox. Do not mark this green on the strength of a passing lint run |
| Whether a given bug warranted a regression test | QUAL-07 | "Important" is a judgement no linter can make; the directory convention and the provenance marker are the mechanical half | Review-time, via the pull-request template checkbox |

**Note on branch protection:** RESEARCH.md assumption A4 is **resolved** — the developer made the
repository public during planning, which unlocks the setting that previously returned a permission
error on a private personal-account repository. Plan 01-09 **specified** the rule and recorded the
applying and removing commands in `docs/ci-branch-protection.md`, so it is reproducible from the
repository rather than from memory — but it did **not** apply it, because its environment could not
publish a commit and therefore could not read the reported check names from a real run. The rule is
one `gh api --method PUT` away; see § Outstanding.

**Note on admin bypass — a deliberate, locked shape, not an oversight.** The rule sets
`enforce_admins: false`, `strict: false` and `required_approving_review_count: 0`. This project
commits straight to the default branch (`branching_strategy: "none"`), so admin enforcement would
refuse the owner's pushes and stop phases 2 through 11; and on a single-maintainer repository a
positive approval count makes any pull request unmergeable, because an author cannot approve their
own. What the rule still buys: a failing check blocks a pull-request merge, and force pushes and
branch deletions are refused outright. What it knowingly does not buy: it cannot stop the owner
pushing past a red gate — that residual is accepted and recorded as T-01-43b. The
`enforce_admins` read-back is asserted so a future "obvious" tightening fails a check instead of
silently halting the project.

---

## Outstanding

Four rows are not green, and all four belong to plan 01-09. None of them is a defect in the work
plans 01-01 through 01-08 produced; all four need an action outside the working tree.

| Row | What is missing | Who can do it |
|---|---|---|
| 01-09/T1 | The branch rule is **specified but not applied** — `gh api .../branches/main/protection` returns `404 Branch not protected`. The full payload, the applying `PUT`, the reversing `DELETE` and the read-back assertions are committed in `docs/ci-branch-protection.md`. | Repository owner |
| 01-09/T1 (admin bypass) | `enforce_admins` cannot be read back from a rule that does not exist. | Repository owner, after the rule is applied |
| 01-09/T2 | **No workflow run has ever happened on this repository.** `origin/main` is still at the last planning commit, which predates `.github/workflows/ci.yml`. | Repository owner: `git push origin main` |
| 01-09/T2 (manual) | A failing required check blocking the **merge** — the one claim in this phase that cannot be made from the working tree. | Repository owner, by hand |

**Why plan 01-09 could not close these.** Its execution environment denied `git push` to the remote
and denied `gh run list`. Both tasks depend on those. The plan explicitly prohibits guessing a
required check name — a context that matches nothing is accepted silently by the API and produces a
rule that blocks nothing — so with no run history to read the reported names from, applying the rule
would have meant violating that prohibition to manufacture a green row. It was left blocked instead.

**What was NOT done, stated plainly:** no CI run was observed; the required-checks rule was not
applied; and CICD-01, CICD-02, SEC-02 and SEC-10 are **not** marked complete in `REQUIREMENTS.md`.

`nyquist_compliant` flips to `true` when the four rows above are green.

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references — all five Wave 0 items complete
- [x] No watch-mode flags
- [x] Feedback latency < 30s — `pytest tests/unit -q` completes in 1.9 s; the full `make check`
      chain is dominated by `fixtures-verify`, which regenerates the whole corpus
- [ ] `nyquist_compliant: true` set in frontmatter — **blocked**, see § Outstanding

**Approval:** PARTIAL — 26 of 30 rows observed green on 2026-08-11; 4 rows blocked on repository
owner action. Not approved as complete.
