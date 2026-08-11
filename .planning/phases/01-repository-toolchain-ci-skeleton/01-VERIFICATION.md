---
phase: 01-repository-toolchain-ci-skeleton
verified: 2026-08-11T20:35:59Z
reverified: 2026-08-11T21:18:00Z
status: passed
score: 5/5 roadmap success criteria verified; 12/12 requirements satisfied
behavior_unverified: 0
overrides_applied: 0
re_verification:
  rounds: 4
  previous_status: human_needed
  previous_score: "5/5 SCs; 12/12 requirements"
  gaps_closed:
    - "QUAL-07 wiring (e92ac5d) — tests/regression/ is collected by the `test:` target; the provenance rule fires in the gate and a regression test placed there runs and can fail the build"
    - "QUAL-07 bookkeeping (a1d0348) — stale README paragraph corrected; both Critical bugs marked, making `pytest -m regression` a real inventory"
    - "CR-03 (0a032c1) — the gitleaks trust anchor is now an in-repo pinned digest; the `sha256sum -c` input is BUILT from the pin, and the idempotent fast path no longer executes the binary to decide whether to trust it"
    - "Filename drift (25b7bbe) — test_secret_scan_depth.py renamed to test_supply_chain_guards.py via git mv"
  gaps_remaining: []
  regressions: []
  notes: >-
    All human-verification items are resolved. CR-03 was fixed rather than
    risk-accepted, and I mutation-proved all three assertions of its new guard.
    One inaccuracy in the handoff was found and is recorded below as a
    non-blocking Info item: the claim that nothing in code or config referenced
    the old filename is false — .github/workflows/ci.yml line 78 still does.
    The guard it points to survives under the new filename, so the protection is
    intact and only the pointer is stale.
---

# Phase 1: Repository, Toolchain & CI Skeleton — Verification Report

**Phase Goal:** Every future commit is gated by lint, type checking, unit tests and secret scanning, and the CSV fixture corpus that specifies the engine is reproducible from a seed
**Verified:** 2026-08-11T20:35:59Z · **Re-verified through:** e92ac5d, a1d0348, 0a032c1, 25b7bbe
**Status:** **passed**
**Re-verification:** Yes — round 4, assessing only whether `human_needed` can move to `passed`. The Success Criteria and requirement accounting were not re-derived; they stand.

**Mode note.** ROADMAP marks this phase `mode: mvp`, but its goal is not in User Story form. This report treats the five explicit numbered Success Criteria as the contract, since they are more specific than a user story would be. Flagged as Info, not a gap.

## Verdict

**The phase passes.** The goal is achieved, all five Success Criteria were verified by execution, all twelve requirements are satisfied, and no item requires a human decision. Phases 2 through 11 are unblocked.

One loose thread is recorded below as Info. It is a stale filename inside a code comment, it blocks nothing, and it is not a reason to hold the phase open.

## Round 4: closing the two human-verification items

### Item 1 — CR-03 gitleaks trust anchor: **fixed, and I agree with fixing rather than accepting**

The reasoning for pinning is sound and I would have argued for it: the project already pins by digest (`kindest/node:v1.35.5@sha256:…`), and the failure mode is genuinely unrecoverable — a tampered scanner reports "no leaks found" forever, and a credential published to a public repository can only be rotated, never recalled. One constant per version bump is a trivial price for removing a silent-forever failure.

Verified in `tools/security/install_gitleaks.sh`:

| Claim | Verified |
|---|---|
| Digests for all four platforms committed in-repo | Lines 44–47, `PINNED_SHA256_{linux,darwin}_{x64,arm64}` |
| The `sha256sum -c` input is **built from the pin**, not grepped from the download | Line 115: `echo "${pinned}  ${tarball}" > expected.sha256`, with the comment stating the origin cannot vouch for itself |
| Fail-closed before extraction | Lines 117–124: mismatch exits 1 with a message naming the pinned value; nothing is extracted |
| The release's `checksums.txt` is advisory only, and runs **after** the authoritative check | Authoritative check at 119; advisory cross-check at 126, downgraded to a `WARNING` |
| The idempotent fast path no longer executes the binary | Lines 93–99: compares `sha256sum "${dest}"` against the recorded pin. The comment states plainly that `"${dest}" version` "executed whatever binary happened to sit in the gitignored tools/bin/, so a once-planted binary was trusted forever" |

**The new guard is mutation-proven.** `test_the_installer_trusts_only_an_in_repo_digest` makes three assertions, and I broke each one independently:

| Mutation | Result |
|---|---|
| M1 — remove the in-repo pin constants | **FAILED** as required |
| M2 — build the verification input from the download (`grep … "${checksums}" > expected.sha256`) — i.e. restore the exact origin-vouches-for-itself construction | **FAILED** as required |
| M3 — reintroduce `"${dest}" version` as an executable line | **FAILED** as required |
| M4 — the same string present **only inside a comment** | **PASSED** — no false positive |

M4 is the one that matters most. This module's history is that a guard was vacuous *because it matched prose*; the new guard resists that trap in the same way its two siblings now do. As in the previous round, one of my mutation attempts silently failed to apply (a sed delimiter clash) and produced a false green; I checked whether the mutation had landed rather than accepting the pass, and redid it. Worth stating because it is the second time a naive mutation has produced a misleading result in this file.

`pytest -m regression` → **3 passed, 115 deselected**.

**On the two limits you asked me to judge — neither should keep the phase open.**

*(a) Trust-on-first-use.* Correct, and correctly stated. Pinning cannot establish that the originally captured bytes were authentic; it can only make any later change detectable. Bootstrapping authenticity needs an independent channel — signature verification against a key you obtained elsewhere — which is a different and much larger piece of work. The achievable property is exactly what was implemented, it matches the precedent this project already set with the kind node image, and the limitation is written in the script rather than implied. This is the standard practice, not a shortfall.

*(b) The stamp file is a cache key, not a trust anchor.* Also correct, and your reasoning is the right one. An attacker who can write to `tools/bin/` can write a matching stamp — but the same attacker can edit the Makefile, the installer, or the tests themselves. T-01-09 concerns the download path, and a local-filesystem attacker is out of scope for it by definition; treating them as in-scope would make every file in the repository a trust boundary and the threat model useless. Scoping this out explicitly, in writing, is better than a defence that pretends to cover it.

Both limits are documented in the script rather than hidden, which is the pattern this repository has used consistently and one of the reasons its claims have held up under adversarial checking.

### Item 2 — filename: **renamed, verified**

`git mv tests/policy/test_secret_scan_depth.py tests/policy/test_supply_chain_guards.py`. The file now holds seven tests — four secret-scan-depth guards and three marked regression guards — defending one claim, with a module docstring saying so. Renaming rather than splitting is the right call; the guards share a subject.

**One correction to the handoff.** The claim that "nothing in code, config or the Makefile referenced the old name" is **not accurate**. `.github/workflows/ci.yml` line 78 still reads:

```
# future regression here is visible in the job log, and plan 01-05's
# test_secret_scan_depth.py fails the build if this line disappears.
```

I checked what this costs. The protection itself is intact — `test_the_secret_scan_job_checks_out_full_history` and `test_removing_full_depth_is_reported` both survive under the new filename, so the *assertion the comment makes* is still true. Only the pointer is stale: a reader following it to `test_secret_scan_depth.py` finds nothing.

**This does not block `passed`,** and I want to be explicit about why, because I held the phase open for a documentation defect in round 2 and consistency matters. The round-2 case was a policy document asserting a *false fact about its own enforcement* — the README claimed `make check` did not collect a directory that it needed to collect, which misled a reader about whether the policy was live. This is a filename in a comment whose substantive claim remains true. It is a one-word fix, it affects no gate, no requirement and no success criterion, and holding a phase open for it would be miscalibrated in the opposite direction. Recorded as Info and worth sweeping up opportunistically.

### Final suite state (independently confirmed)

| Measure | Claimed | Observed |
|---|---|---|
| Policy tests | 58 | **58 passed** |
| Unit + regression | 60 | **60 passed** |
| `pytest -m regression` | 3 | **3 passed, 115 deselected** |

## Goal Achievement

### Observable Truths (ROADMAP Success Criteria)

Established in round 1 by execution; not re-derived.

| # | Truth | Status | Evidence |
|---|---|---|---|
| 1 | Opening a PR runs ruff, mypy, unit tests and gitleaks automatically, and a commit containing a fake credential fails the build | ✓ VERIFIED | `on: pull_request`; jobs `Quality gate` (→ `make check`) and `Secret scan (full history)`. Run 31531101283 green; branch protection requires both contexts; negative PR #1 reported `mergeable_state: blocked`. Credential half proven by executing `make gitleaks-selftest`: scanner exits 1 on `aws-access-token, generic-api-key, github-pat, slack-bot-token` |
| 2 | `make fixtures` regenerates the corpus byte-identically from a recorded seed on a clean checkout — no corpus files committed en masse | ✓ VERIFIED | Fresh `git clone`: no `tests/fixtures/csv` directory at all; `make fixtures` produced 70 files and rewrote `CORPUS.sha256` to a byte-identical state (`git status` empty). `sha256sum -c` → 70/70 |
| 3 | Adding a `print()`, an untyped public function, or an undocumented public API fails CI | ✓ VERIFIED | Three live mutations, each → exit 2: `T201`, `ANN201`/`ANN001`, `D103` |
| 4 | A developer clones and runs `uv sync && make check` with no cluster, credentials or network services | ✓ VERIFIED | Executed literally: bare `uv sync` exit 0, `uv.lock` unmodified; `make check` exit 0 |
| 5 | Full git history reports zero secrets, and no CI job echoes a secret | ✓ VERIFIED | `60 commits scanned`, no leaks; working tree clean; `grep -c 'secrets\.' ci.yml` → 0; `--redact` inside the Makefile target |

**Score:** 5/5 verified (0 present, behavior-unverified).

### Requirements Coverage

Established in round 3; not re-derived. **12/12 satisfied, no orphans.**

| Requirement | Status |
|---|---|
| QUAL-01 (type hints, mypy in CI) | ✓ SATISFIED |
| QUAL-02 (docstrings on public API) | ✓ SATISFIED |
| QUAL-07 (every important bug gains a permanent regression test) | ✓ SATISFIED |
| QUAL-08 (seed-generated corpus, not committed en masse) | ✓ SATISFIED |
| CICD-01 (GitHub Actions provides CI/CD) | ✓ SATISFIED |
| CICD-02 (PRs run the full quality gate) | ✓ SATISFIED |
| CICD-03 (ruff runs automatically) | ✓ SATISFIED |
| CICD-04 (mypy runs automatically) | ✓ SATISFIED |
| SEC-02 (no secret in history or working tree) | ✓ SATISFIED |
| SEC-10 (no unnecessary long-lived CI credentials; secrets never printed) | ✓ SATISFIED as scoped — Phase 11 owes a re-audit when it first interpolates a secret |
| SEC-11 (secret scanning fails the build on a credential) | ✓ SATISFIED |
| OBS-03 (no `print()` for operational logging) | ✓ SATISFIED |

**REQUIREMENTS.md updates:** promote **QUAL-07, QUAL-08, CICD-01, CICD-02, SEC-02, SEC-10, SEC-11** from Pending to **Complete**. QUAL-01, QUAL-02, CICD-03, CICD-04, OBS-03 confirmed **Complete**.

### Behavioural Spot-Checks (cumulative)

| Behaviour | Result | Status |
|---|---|---|
| Clean checkout reaches a green gate | exit 0; lock unmodified | ✓ PASS |
| Corpus reproduces byte-identically on a clean checkout | 70 files; 0 tree changes | ✓ PASS |
| Oracle sensitive to a single byte | exit 2 | ✓ PASS |
| Determinism across hash seed / TZ / locale | exit 0 | ✓ PASS |
| print() / untyped def / missing docstring fail the gate | exit 2 — `T201`, `ANN201`, `D103` | ✓ PASS |
| Scanner live, not merely configured | exits 1 on 4 rule classes | ✓ PASS |
| Full history scanned, not depth-1 | `60 commits scanned` | ✓ PASS |
| Provenance rule fires in the gate | exit 2 | ✓ PASS |
| A compliant regression test runs; a failing one turns the gate red | 60 → 61; exit 2 | ✓ PASS |
| CR-01 guard fails on the real revert; resists the comment trap | FAILED as required; comments stripped | ✓ PASS |
| **CR-03 guard: all three assertions mutation-proven** | **M1, M2, M3 FAILED as required** | ✓ **PASS** |
| **CR-03 guard: no false positive from a comment-only match** | **M4 PASSED** | ✓ **PASS** |
| `pytest -m regression` is a real inventory | 3 passed, 115 deselected | ✓ PASS |
| Policy / unit+regression suites | 58 / 60 passed | ✓ PASS |

### Anti-Patterns Found

| File | Pattern | Severity | Impact |
|---|---|---|---|
| — | `TBD` / `FIXME` / `XXX` / `TODO` / `HACK` in phase-touched source | ℹ️ NONE | Debt-marker gate passes |
| `Makefile` | Gate target enumerated only `tests/unit` | ✓ RESOLVED | e92ac5d, re-verified |
| `tests/regression/README.md` | Documented pre-fix behaviour | ✓ RESOLVED | a1d0348, re-verified; history preserved |
| `regression` marker | Registered but unused | ✓ RESOLVED | a1d0348; now selects 3 |
| `install_gitleaks.sh` | Digest not independent of download origin; binary executed before verification | ✓ RESOLVED | 0a032c1, mutation-proven |
| `test_secret_scan_depth.py` | Filename no longer described contents | ✓ RESOLVED | 25b7bbe, renamed via `git mv` |
| `.github/workflows/ci.yml` line 78 | Comment points at `test_secret_scan_depth.py`, which no longer exists | ℹ️ **INFO — open** | The guard survives under the new filename, so the comment's substantive claim is still true; only the pointer is stale. One-word fix. Blocks nothing |
| `tools/corpus/generators.py` `_decimal_renderer` | Negative bounds render incorrectly (WINDOWS.md, OPEN) | ⚠️ **latent** | Every `kind: decimal` uses positive bounds; all negative corpus values are escaped literals. Does not corrupt the corpus or undermine QUAL-08. Will bite the first fixture declaring a negative bound |
| policy test `\bmake\s+gitleaks\b` | Regex also matches `make gitleaks-selftest` (OPEN) | ℹ️ INFO | The asserted property is true; only the proof is loose |
| ROADMAP line 52 | `mode: mvp` on a non-User-Story goal | ℹ️ INFO | Verified against the five Success Criteria instead |

### Deferred Items

| # | Item | Addressed In | Evidence |
|---|---|---|---|
| 1 | `tests/property`, `tests/integration`, `tests/e2e` are not collected by any gate target | Phase 3 | Recorded in WINDOWS.md as a Phase 3 obligation and in the `Makefile` `test:` comment in place. Empty today and needing testcontainers or a live cluster; wiring them into `make check` now would create a target that cannot run |
| 2 | SEC-10's general form ("no CI job ever echoes a secret") is undecidable once a job interpolates one | Phase 11 | Recorded in `ci.yml`. This phase claims the stronger structural fact that the workflow references no repository secret at all; the first job to interpolate one owes a re-audit |

### Human Verification Required

**None.** Both prior items are resolved and independently verified.

### Summary

The phase is complete. All five Success Criteria were verified by running commands rather than reading claims, all twelve requirements are satisfied, and nothing awaits a human decision.

Four rounds of verification each closed something real, which is the outcome that makes the process worth its cost. Round 1 found that the regression tree was collected by no gate target — the mechanism existed but was inert, so a regression test placed there would never have run in CI. Round 2 confirmed the wiring and tested the direction that mattered most, watching a compliant regression test raise the gate's count and a failing one turn it red; what remained was a policy document that misdescribed its own enforcement, and an empty marker inventory. Round 3 closed both and, in doing so, gave CR-01 its first regression protection — reverting `--locked` had been completely silent until then. Round 4 replaced the last risk-accepted item with an actual fix.

The CR-03 work is the strongest of these. The old installer had two independent defects that both ended in the same place: a scanner nobody could trust, reporting "no leaks found" forever. The digest came from the same origin as the artifact it validated, and an already-present binary was executed to read its version, so a once-planted shim was trusted permanently. Both are now closed, the fix is guarded by three assertions I broke individually, and the guard does not repeat this module's own history of matching prose instead of code. The two residual limits — trust-on-first-use, and a stamp file that a local-filesystem attacker could forge — are correctly scoped and honestly written into the script. Neither is a shortfall; the first is what pinning can achieve, and the second is outside the threat T-01-09 names.

One loose thread, recorded as Info: `.github/workflows/ci.yml` line 78 still points at `test_secret_scan_depth.py`. The handoff stated nothing in code or config referenced the old name; that was inaccurate. The guard it describes survives under the new filename, so the protection is intact and the comment's substantive claim is still true — only the pointer is stale. It is a one-word fix, it touches no gate, requirement or success criterion, and it is not a reason to hold the phase open.

Two items are carried forward deliberately and are recorded where the next person will meet them: the uncollected `tests/property`, `tests/integration` and `tests/e2e` trees belong to Phase 3, and SEC-10's undecidable general form belongs to Phase 11. The latent `_decimal_renderer` defect remains open in WINDOWS.md and does not affect the current corpus, since every generated decimal column declares positive bounds.

**Nothing blocks marking this phase complete.**

---

_Verified: 2026-08-11T20:35:59Z · Re-verified through commits e92ac5d, a1d0348, 0a032c1, 25b7bbe_
_Verifier: Claude (gsd-verifier)_
