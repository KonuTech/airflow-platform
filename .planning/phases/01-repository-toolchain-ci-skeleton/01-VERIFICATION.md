---
phase: 01-repository-toolchain-ci-skeleton
verified: 2026-08-11T20:35:59Z
reverified: 2026-08-11T21:05:00Z
status: human_needed
score: 5/5 roadmap success criteria verified; 12/12 requirements satisfied
behavior_unverified: 0
overrides_applied: 0
re_verification:
  rounds: 3
  previous_status: human_needed
  previous_score: "5/5 SCs; 11.5/12 requirements"
  gaps_closed:
    - "QUAL-07 wiring (e92ac5d) — tests/regression/ is collected by the `test:` target; the provenance rule fires in the gate and a regression test placed there actually runs and can fail the build"
    - "QUAL-07 bookkeeping (a1d0348) — the stale README paragraph is corrected, and both of this phase's Critical bugs now carry `@pytest.mark.regression`, making `pytest -m regression` a real inventory (2 selected, was 0)"
  gaps_remaining: []
  regressions: []
  notes: >-
    QUAL-07 moves to Complete. Not because a fix was applied, but because the
    specific evidence I named as missing now exists and was re-derived
    independently: the CR-01 guard was mutation-tested and FAILS when `--locked`
    is removed from the recipe, while `--locked` remained present in the comment
    directly above it — proving the comment-stripping is load-bearing and that
    this guard does not repeat the vacuity that made CR-02's guard useless.
human_verification:
  - test: "Decide whether the gitleaks installer's trust anchor is acceptable (CR-03, already in WINDOWS.md)"
    expected: "Either pin the expected SHA-256 in the repository so the digest is independent of the download origin, or accept the risk in writing"
    why_human: >-
      Supply-chain risk acceptance is a judgement call, and the current arrangement is
      a common and defensible one. It does not undermine SEC-02/SEC-10/SEC-11, all
      three of which were proven here by executing the scanner.
  - test: >-
      Optional tidy, blocks nothing: both regression-marked tests live in
      tests/policy/test_secret_scan_depth.py, whose filename no longer describes its
      contents — it now also holds an installer-ordering guard and a Makefile
      lockfile guard.
    expected: "Either split the file or rename it to something like test_supply_chain_guards.py"
    why_human: >-
      Cosmetic organisation with no correctness impact. Raised only because this phase
      is the one that sets the repository's conventions, and a misleading filename is
      cheapest to fix before ten more phases accrete around it.
---

# Phase 1: Repository, Toolchain & CI Skeleton — Verification Report

**Phase Goal:** Every future commit is gated by lint, type checking, unit tests and secret scanning, and the CSV fixture corpus that specifies the engine is reproducible from a seed
**Verified:** 2026-08-11T20:35:59Z · **Re-verified:** e92ac5d, then a1d0348
**Status:** human_needed — the phase goal is achieved and every requirement is satisfied; two non-blocking items await a human decision
**Re-verification:** Yes — round 3, targeted re-assessment of QUAL-07 only. The five Success Criteria were not re-derived.

**Mode note.** ROADMAP marks this phase `mode: mvp`, but its goal is not in User Story form. This report treats the five explicit numbered Success Criteria as the contract, since they are more specific than a user story would be. Flagged as Info, not a gap.

## Verdict

**The phase is complete. Nothing blocks marking it so, and nothing blocks phases 2 through 11.**

All five Success Criteria were verified by execution. All twelve requirements are now satisfied, including QUAL-07, which I held at Partial through two rounds and am now upgrading on evidence. The two remaining human items are a documented risk-acceptance decision (CR-03) and a cosmetic filename — neither is a defect and neither gates anything.

## Goal Achievement

### Observable Truths (ROADMAP Success Criteria)

| # | Truth | Status | Evidence |
|---|---|---|---|
| 1 | Opening a PR runs ruff, mypy, unit tests and gitleaks automatically, and a commit containing a fake credential fails the build | ✓ VERIFIED | Both halves. `.github/workflows/ci.yml` triggers on `pull_request`; jobs `Quality gate` (→ `make check`) and `Secret scan (full history)` (→ `make gitleaks`, `make gitleaks-selftest`). Run 31531101283 green; branch protection requires both contexts; negative PR #1 reported `mergeable_state: blocked`. Credential half proven by executing `make gitleaks-selftest`: scanner exits 1, reporting `aws-access-token, generic-api-key, github-pat, slack-bot-token` |
| 2 | `make fixtures` regenerates the entire corpus byte-identically from a recorded seed on a clean checkout — no corpus files committed en masse | ✓ VERIFIED | Executed on a fresh `git clone`. `git ls-files tests/fixtures/csv` → 0; the clean checkout had no such directory at all. `make fixtures` generated 70 files and rewrote `CORPUS.sha256`; `git status --porcelain` was **0** afterwards, so the regenerated oracle is byte-identical to the committed one. `sha256sum -c` → 70/70 OK |
| 3 | Adding a `print()` to library code, an untyped public function, or an undocumented public API fails CI | ✓ VERIFIED | Three live mutations of `version.py` in the clean clone, each followed by `make check`: `print()` → exit 2, `T201`; untyped def → exit 2, `ANN201`/`ANN001`; no docstring → exit 2, `D103` |
| 4 | A developer clones the repo and runs `uv sync && make check` successfully with no cluster, no credentials and no network services | ✓ VERIFIED | Executed literally on the fresh clone: bare `uv sync` (no flags) exit 0, left `uv.lock` **unmodified**; `make check` exit 0. No services running |
| 5 | A scan of full git history reports zero secrets, and no CI job echoes a secret value into its log | ✓ VERIFIED | `make gitleaks` → `60 commits scanned`, `no leaks found`; working-tree scan clean. `grep -c 'secrets\.' ci.yml` → **0**; no env-dumping construct. `--redact` lives inside the Makefile target |

**Score:** 5/5 verified (0 present, behavior-unverified). Every one confirmed by running a command.

### QUAL-07 closure — round 3 (commit a1d0348)

I re-derived both claims rather than accepting the report.

**1. The stale README paragraph is corrected.** The sentence asserting `make check` "does not name this directory" is gone. Line 105 now reads: `make check` names this directory: its `test` target runs `pytest tests/unit tests/regression`. It also keeps the history — that this was *not* true when the file was written, that the omission was a real defect, and that `e92ac5d` fixed it. Recording the defect rather than quietly overwriting it is the right call, and it is the behaviour this project's core value asks for.

**2. The marker inventory is real.** `pytest -m regression` → **2 selected, 115 deselected** (was 0 selected, 116 deselected).

| Marked test | Bug | Verified |
|---|---|---|
| `test_the_installer_verifies_before_it_extracts` | CR-02 (vacuous ordering guard) | Comment-stripping read in source; established mutation-tested in the prior fix |
| `test_make_install_refuses_a_stale_lockfile` | CR-01 (bare `uv sync` defeated `lock-check`) | **Mutation-tested here — see below** |

**3. The CR-01 guard is live, and is not a repeat of CR-02's vacuity.** This was the claim that mattered, so I tested it directly. My first attempt at the mutation silently failed to apply — the recipe is `$(UV) sync --locked`, not a literal `uv sync --locked` — and the test passed. Rather than accept that as a result I inspected the target with `cat -A`, found my sed had matched nothing, and redid it against the real text. That distinction is the whole point of a mutation test, and it is worth recording that the naive attempt produced a false green.

With the mutation correctly applied to line 45 only:

```
line 40 (comment):  # --locked, never bare `uv sync`: a bare sync REWRITES uv.lock when it is
line 45 (recipe):   $(UV) sync            <- --locked removed
```

```
FAILED tests/policy/test_secret_scan_depth.py::test_make_install_refuses_a_stale_lockfile
E  assert '--locked' in 'install: uv-guard  ## Create the venv from the lockfile\n\t$(UV) sync\n'
```

Two things are proven at once. The guard **fails** when the fix is reverted — so it is a working regression test, not decoration. And the string it searched contains none of the five comment lines, while `--locked` was still sitting in the comment immediately above the recipe. The comment-stripping is genuinely load-bearing: prose cannot satisfy this assertion. That is precisely the trap that made CR-02's original guard vacuous, and it has been avoided deliberately rather than by luck.

Restoring the Makefile returned `2 passed, 115 deselected`, working tree clean.

**4. The README documents the hardened-in-place route.** Line 121 onward: *"The marker is the route for hardened-in-place tests. Not every fixed bug earns a new file here… harden it where it lives and tag it `@pytest.mark.regression`… A hardened assertion that carries no marker is invisible to that inventory, which is the same failure mode as an uncollected directory."* That closes the discoverability gap I named in round 2, and does so by generalising from the defect rather than patching the instance.

**Policy suite: 57 passed** (was 56), consistent with exactly one new test.

### Why QUAL-07 is now Complete

I held this at Partial through two rounds, and the coordinator twice invited me to keep it there. I am upgrading it because the specific evidence I said was missing now exists, not because effort was expended.

My stated reason for Partial in round 2 was: *"the phase's own two Critical bugs are the counter-example — neither is filed as a regression test by either sanctioned route."* That statement is now false. Both are filed, both carry the marker, both are discoverable through `pytest -m regression`, and the route used for them is documented as sanctioned rather than improvised.

More substantively, CR-01 went from **no regression protection whatsoever** — reverting `--locked` was completely silent — to a guard I have personally watched fail on the exact revert. That is a real increase in the property QUAL-07 asks for, not a bookkeeping gesture.

**On the flag-versus-behaviour caveat, which the coordinator flagged honestly and asked me to rule on:** the test asserts the presence of `--locked` rather than reproducing the stale-lock rewrite end to end. I accept this, for three reasons. The fix *is* one token, so the token's absence is the regression — there is no gap between what is asserted and what would break. Reproducing the behaviour requires a network resolve and a mutated `pyproject.toml`, which ROADMAP criterion 4 forbids from the offline `make check` path; the alternative is not a better test but an untestable one, or a second CI job whose cost is real. And the limitation is stated in the test's own docstring rather than implied, matching the pattern the sibling CR-02 test already uses for corrupted downloads. A regression test that is honest about its boundary is worth more than one that overclaims.

The residual risk is narrow and worth naming: this is a structural assertion over Makefile text, so restructuring the install target (moving the sync into a script, or hiding the flag behind a variable) could make it pass or fail for the wrong reason. That is inherent to guards of this class and is not a reason to withhold Complete.

### Plan-level truths beyond the roadmap contract

| Truth (plan) | Status | Evidence |
|---|---|---|
| `make install` is equivalent to bare `uv sync` (01-01) | ⚠️ SUPERSEDED, correctly | CR-01 changed `install:` to `uv sync --locked`, deliberately breaking the literal equivalence so a stale lock cannot be silently refreshed. The user-facing criterion still holds: bare `uv sync` on a clean clone leaves `uv.lock` untouched and `make check` passes. Now protected by a marked regression test |
| Every configured gate observed rejecting a bad input (01-05) | ✓ VERIFIED | `test_gates_actually_fail.py` → 12 passed; ten paired good/bad samples |
| Workflow invokes no tool directly; CI/local parity; pin agreement; scan depth (01-05) | ✓ VERIFIED | 4 policy modules → 22 passed |
| Corpus digests stable across hash seed, timezone and locale (01-03) | ✓ VERIFIED | `PYTHONHASHSEED=1 TZ=Asia/Tokyo LC_ALL=C make fixtures-verify` → exit 0 |
| `fixtures-verify` fails when a byte differs (01-03) | ✓ VERIFIED | Corrupted one digest → exit 2 with the R1 coupling diagnostic |
| Allowlist is AND-scoped by path *and* prefix (01-02) | ✓ VERIFIED | Byte-identical value reported outside `tests/fixtures/`, silenced inside |
| No canary value appears in scanner output (01-02) | ✓ VERIFIED | Self-test asserts it |
| All 69 fixtures declared, no gaps or duplicates (01-08) | ✓ VERIFIED | 69 declarations, 70 oracle lines, 70 files |
| A regression module without a bug reference fails collection (01-04) | ✓ VERIFIED | Fires through `make test`; re-verified in both directions |
| An empty `tests/regression/` must not make the run exit 5 (01-04) | ✓ VERIFIED | Collected alongside `tests/unit`'s 60 tests; exit 0 |
| ADRs exist for decisions actually taken (01-04) | ✓ VERIFIED | `0001`–`0005` plus template and index |

### Required Artifacts

| Artifact | Status | Details |
|---|---|---|
| `pyproject.toml` | ✓ VERIFIED | `testpaths = ["tests"]`, `--strict-markers --strict-config`; `slow` and `regression` markers registered — and the latter is now used |
| `Makefile` | ✓ VERIFIED | `test` collects `tests/unit tests/regression`; `check` = uv-guard, lock-check, lint, format, typecheck, imports, policy, test, fixtures-verify; `ci` = check + gitleaks + selftest |
| `setup.cfg` | ✓ VERIFIED | Import contract observed KEPT |
| `.github/workflows/ci.yml` | ✓ VERIFIED | `name: CI`; two jobs; `fetch-depth: 0`; actions pinned by SHA; `permissions: contents: read` |
| `.github/pull_request_template.md` | ✓ VERIFIED | Regression checkbox with explicit N/A escape |
| `.gitleaks.toml` | ✓ VERIFIED | AND-scoping proven behaviourally |
| `tools/security/gitleaks_selftest.py` | ✓ VERIFIED | Asserts exit code, rule identity, scoping and redaction |
| `tools/corpus/*` | ✓ VERIFIED | Drives both `make fixtures` and `make fixtures-verify` |
| `tests/fixtures/corpus.yaml` / `CORPUS.sha256` | ✓ VERIFIED | 69 fixtures; 70-line oracle reproduced byte-identically |
| `tests/policy/badsamples/` | ✓ VERIFIED | 5 bad + 5 matching good controls |
| `tests/regression/conftest.py` | ✓ VERIFIED | Reachable from `make check`; fires in the gate |
| `tests/regression/README.md` | ✓ VERIFIED (was ⚠️ STALE) | Accurate; records the defect and its fix; documents the marker route |
| `tests/policy/test_secret_scan_depth.py` | ✓ VERIFIED | Holds both marked regression guards. Filename now under-describes its contents — see Human Verification #2 |
| `docs/adr/*`, `docs/ci-branch-protection.md`, `01-VALIDATION.md` | ✓ VERIFIED | Present; branch rule read back live |

### Key Link Verification

| From | To | Via | Status |
|---|---|---|---|
| `ci.yml` | `Makefile` | `make install` / `make check`; secrets job runs `make gitleaks` + selftest | ✓ WIRED |
| `Makefile` | `pyproject.toml` | every gate runs `uv run --frozen` | ✓ WIRED |
| `setup.cfg` | `packages/*/src` | `root_packages` resolve to workspace members | ✓ WIRED |
| `Makefile` | `tools/corpus/__main__.py` | `python -m tools.corpus generate\|verify` | ✓ WIRED |
| `generators.py` | `corpus.yaml` | every byte from manifest + master seed | ✓ WIRED |
| `gitleaks_selftest.py` | `.gitleaks.toml` | self-test runs the project's own config | ✓ WIRED |
| `Makefile` | `tests/regression/` | `test:` collects it; `test` is a prerequisite of `check` | ✓ WIRED |
| `conftest.py` | `pull_request_template.md` | mechanical ↔ review half | ✓ WIRED |
| `regression` marker | the two guards | `pytest -m regression` selects 2 | ✓ **WIRED** (was ⚠️ ORPHANED) |
| `docs/ci-branch-protection.md` | `ci.yml` job names | required checks match exactly | ✓ WIRED |

### Behavioural Spot-Checks

| Behaviour | Result | Status |
|---|---|---|
| Clean checkout reaches a green gate (`uv sync && make check`) | exit 0; lock unmodified | ✓ PASS |
| Corpus reproduces byte-identically on a clean checkout | 70 files; 0 tree changes | ✓ PASS |
| Oracle is sensitive to a single byte | exit 2 | ✓ PASS |
| Determinism across hash seed / TZ / locale | exit 0 | ✓ PASS |
| print() / untyped def / missing docstring fail the gate | exit 2 — `T201`, `ANN201`, `D103` | ✓ PASS |
| Scanner is live, not merely configured | exits 1 on 4 rule classes | ✓ PASS |
| Full history scanned, not depth-1 | `60 commits scanned` | ✓ PASS |
| Gates observed failing on bad samples | 12 passed | ✓ PASS |
| Provenance rule fires in the gate | exit 2 | ✓ PASS |
| A compliant regression test runs in the gate | 60 → 61 passed | ✓ PASS |
| A failing regression test turns the gate red | exit 2 | ✓ PASS |
| **`pytest -m regression` is a real inventory** | **2 selected, 115 deselected** | ✓ **PASS** (was 0 selected) |
| **CR-01 guard fails when `--locked` is removed** | **FAILED as required** | ✓ **PASS** (new) |
| **CR-01 guard is not satisfied by `--locked` in a comment** | comment lines stripped from the searched text | ✓ **PASS** (new) |
| Policy suite after the new test | 57 passed (was 56) | ✓ PASS |

### Requirements Coverage

| Requirement | Status | Evidence |
|---|---|---|
| QUAL-01 (type hints, mypy in CI) | ✓ SATISFIED | Live mutation → `ANN201`/`ANN001`; mypy strict inside `check` |
| QUAL-02 (docstrings on public API) | ✓ SATISFIED | Live mutation → `D103`; `missing_param_doc` bad sample gated |
| QUAL-07 (every important bug gains a permanent regression test) | ✓ **SATISFIED** | Mechanism live and gated; policy documented accurately; inventory real — 2/2 of this phase's Critical bugs carry the marker, and the CR-01 guard was mutation-proven to fail on the actual revert while resisting the comment trap |
| QUAL-08 (seed-generated corpus, not committed en masse) | ✓ SATISFIED | 69 fixtures, 0 committed, byte-identical regeneration, oracle sensitivity proven |
| CICD-01 (GitHub Actions provides CI/CD) | ✓ SATISFIED | Workflow `CI`, run 31531101283 green |
| CICD-02 (PRs run the full quality gate) | ✓ SATISFIED | `on: pull_request`; step is `make check`; `ci` is a superset; parity tests prevent drift; both contexts required; negative PR #1 blocked |
| CICD-03 (ruff runs automatically) | ✓ SATISFIED | `lint`/`format` in `check`; live failures observed |
| CICD-04 (mypy runs automatically) | ✓ SATISFIED | `typecheck` in `check`; strict, flags enumerated rather than the silently-ignored toggle |
| SEC-02 (no secret in history or working tree) | ✓ SATISFIED | 60 commits scanned, no leaks; working tree clean |
| SEC-10 (no unnecessary long-lived CI credentials; secrets never printed) | ✓ SATISFIED **as scoped** | Workflow references no repository secret at all; `--redact` inside the target. The undecidability of the general form is recorded in the workflow, with Phase 11 owing a re-audit |
| SEC-11 (secret scanning fails the build on a credential) | ✓ SATISFIED | Scanner observed exiting 1 on four rule classes, allowlists confirmed path-scoped by a byte-identical control |
| OBS-03 (no `print()` for operational logging) | ✓ SATISFIED | Live mutation → `T201`; carve-out scope guarded |

**12/12 satisfied. No orphaned requirements.**

**REQUIREMENTS.md updates:** promote **QUAL-07, QUAL-08, CICD-01, CICD-02, SEC-02, SEC-10, SEC-11** from Pending to **Complete**. QUAL-01, QUAL-02, CICD-03, CICD-04, OBS-03 confirmed **Complete**.

### Anti-Patterns Found

| File | Pattern | Severity | Impact |
|---|---|---|---|
| — | `TBD` / `FIXME` / `XXX` / `TODO` / `HACK` in phase-touched source | ℹ️ NONE | Debt-marker gate passes: zero matches |
| `Makefile` | Gate target enumerated only `tests/unit` | ✓ RESOLVED | Fixed in e92ac5d, re-verified |
| `tests/regression/README.md` | Documented pre-fix behaviour | ✓ RESOLVED | Fixed in a1d0348, re-verified; history preserved |
| `regression` marker | Registered but unused | ✓ RESOLVED | Fixed in a1d0348; selects 2 |
| `tests/policy/test_secret_scan_depth.py` | Filename no longer describes contents | ℹ️ INFO | Now also holds an installer-ordering guard and a Makefile lockfile guard. Cosmetic; see Human Verification #2 |
| `tools/corpus/generators.py` `_decimal_renderer` | Negative bounds render incorrectly (WINDOWS.md, OPEN) | ⚠️ **latent** | Every `kind: decimal` uses positive bounds; all negative corpus values are escaped literals. Does not corrupt the corpus or undermine QUAL-08 |
| policy test `\bmake\s+gitleaks\b` | Regex also matches `make gitleaks-selftest` (OPEN) | ℹ️ INFO | The asserted property is true; only the proof is loose |
| `tools/security/install_gitleaks.sh` | Tarball and checksums share a base URL (CR-03, OPEN) | ⚠️ WARNING | Routed to human decision; does not undermine SEC-02/10/11 |
| ROADMAP line 52 | `mode: mvp` on a non-User-Story goal | ℹ️ INFO | Verified against the five Success Criteria instead |

### Deferred Items

| # | Item | Addressed In | Evidence |
|---|---|---|---|
| 1 | `tests/property`, `tests/integration`, `tests/e2e` are not collected by any gate target | Phase 3 | Recorded in WINDOWS.md as a Phase 3 obligation and in the `Makefile` `test:` comment in place. Empty today and needing testcontainers or a live cluster; wiring them into `make check` now would create a target that cannot run. Phase 3 ("tested with testcontainers and no cluster") is where they become runnable |

### Human Verification Required

**1. gitleaks installer trust anchor (CR-03).**
- **Test:** Pin the expected SHA-256 in-repo, or accept the risk in writing.
- **Expected:** A digest not fetched from the same origin as the artifact it validates.
- **Why human:** Risk acceptance is a judgement call; the current arrangement is common and defensible. Blocks nothing.

**2. Filename of `tests/policy/test_secret_scan_depth.py` (optional).**
- **Test:** The file now holds the secret-scan depth guard, the installer-ordering guard and the Makefile lockfile guard.
- **Expected:** Split it, or rename to something like `test_supply_chain_guards.py`.
- **Why human:** Purely cosmetic. Raised only because this phase sets the repository's conventions and it is cheapest to fix before ten phases accrete around it.

### Summary

The phase goal is achieved and every one of the twelve requirements is satisfied. The evidence is unusually strong throughout: all five Success Criteria were re-derived by running commands, including a full clean-clone reproduction of criterion 4 in a scratch directory, three live source mutations for criterion 3, and an executed self-test for criteria 1 and 5. The corpus result remains the most convincing — `make fixtures` on a checkout containing no `tests/fixtures/csv` directory produced 70 files and rewrote the oracle to a byte-identical state, leaving `git status` empty.

QUAL-07 took three rounds and is now genuinely closed. The progression is worth recording, because each round fixed something real rather than restating a claim. Round 1 found that the regression tree was collected by no gate target, so a regression test placed there would never have run in CI — the mechanism existed but was inert. Round 2 confirmed the wiring fix and tested the direction that mattered most, watching a compliant regression test raise the gate's count from 60 to 61 and a failing one turn the gate red; what remained was that the policy document misdescribed its own enforcement, and that the phase's two Critical bugs were filed by neither sanctioned route, leaving `pytest -m regression` empty. Round 3 closes both. The README is accurate and, better, preserves the history of the defect instead of quietly overwriting it. The inventory holds two real entries.

The CR-01 guard deserves specific mention because it is the strongest single piece of evidence in this phase. That bug previously had no regression protection at all — reverting `--locked` was completely silent. I mutation-tested the new guard and watched it fail on exactly that revert, with `--locked` still present in the comment directly above the recipe and correctly ignored. The assertion message shows the comment lines stripped from the searched text. That is the same trap that made CR-02's original guard vacuous, avoided deliberately and demonstrably. Worth noting for the record: my own first mutation attempt silently failed to apply because the recipe uses `$(UV)` rather than a literal `uv`, and produced a false green. Inspecting rather than accepting that result is what turned it into evidence.

I accept the flag-versus-behaviour limitation. The fix is one token, so the token's absence is the regression; reproducing the stale-lock rewrite end to end needs a network resolve that ROADMAP criterion 4 excludes from `make check`; and the boundary is stated in the test's own docstring rather than implied. A regression test honest about its limits is worth more than one that overclaims.

Two items remain, and neither is a defect: a documented supply-chain risk-acceptance decision, and a filename that has drifted from its contents. **Nothing blocks marking this phase complete.**

---

_Verified: 2026-08-11T20:35:59Z · Re-verified through commits e92ac5d and a1d0348_
_Verifier: Claude (gsd-verifier)_
