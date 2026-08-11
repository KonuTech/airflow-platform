---
phase: 01-repository-toolchain-ci-skeleton
plan: 05
subsystem: quality-gates
tags: [meta-testing, policy-tests, ruff, mypy, import-linter, gitleaks, ci-parity, sec-10]
status: complete

requires:
  - phase: 01-01
    provides: the Makefile gate chain, ruff/mypy/import-linter configuration, the badsamples lint and type exclusions, the CI workflow
  - phase: 01-02
    provides: the secrets job on a fetch-depth 0 checkout, the checksum-verified installer, the GITLEAKS_VERSION triplication this plan freezes
  - phase: 01-03
    provides: tools/corpus and tests/fixtures/corpus.yaml, without which `make check` cannot reach the end of its chain
provides:
  - meta-verification that all five configured gates are OBSERVED rejecting a bad input and accepting a good one
  - tests/policy/badsamples/ — five deliberately-broken modules and five correct counterparts
  - CI/local parity as a structural property computed from the Makefile's prerequisite graph
  - drift detection across every file naming a gate tool's version
  - SEC-10 restated in its decidable form, with the undecidable residue documented rather than claimed
  - guards for the two verified silent-pass modes of the secret scan (shallow checkout, unredacted output)
affects:
  - 01-06 through 01-09 (any new workflow job, make target or tool pin must now satisfy these seven modules)
  - 01-09 (CICD-02/SEC-02/SEC-10 stay open until the branch-protection plan lands; it shares all three IDs)
  - Phase 11 (the first repository secret invalidates this phase's structural SEC-10 claim and owes a re-audit)

actuals:
  tokens: 17200
  tasks: 3
  commits: 3

tech-stack:
  added: []
  patterns:
    - A gate is proven by watching it fail on a bad input AND stay silent on a good one; either alone is uninformative
    - A policy test asserts on a tool's exit status and reported rule identity, never on the presence of a configuration key
    - Sensitivity is committed, not observed once: the predicate is pure and sibling tests feed it mutated copies of the real file
    - A bad sample is copied to a library-shaped path before a gate runs, because the path it lives at suppresses the rules under test
    - A structural claim is computed from the source of truth (the Makefile graph) rather than matched against a fixed string

key-files:
  created:
    - tests/policy/badsamples/print_in_library.py
    - tests/policy/badsamples/good_print_in_library.py
    - tests/policy/badsamples/missing_docstring.py
    - tests/policy/badsamples/good_missing_docstring.py
    - tests/policy/badsamples/missing_param_doc.py
    - tests/policy/badsamples/good_missing_param_doc.py
    - tests/policy/badsamples/untyped_public_def.py
    - tests/policy/badsamples/good_untyped_public_def.py
    - tests/policy/badsamples/forbidden_import.py
    - tests/policy/badsamples/good_forbidden_import.py
    - tests/policy/test_gates_actually_fail.py
    - tests/policy/test_ci_invokes_make_only.py
    - tests/policy/test_ci_calls_make_ci.py
    - tests/policy/test_pinned_tool_versions_agree.py
    - tests/policy/test_print_ban_scope.py
    - tests/policy/test_secret_scan_depth.py
    - tests/policy/test_workflow_secrets.py
  modified:
    - .planning/REQUIREMENTS.md

key-decisions:
  - "Samples are copied into a library-shaped scratch package before any gate runs. They live under tests/, where pyproject.toml relaxes D and ANN, so linting them in place would have proved nothing about the docstring rules — the path itself suppresses them. This was not a precaution; it is why the D103/D417 cases work at all."
  - "Each policy module carries its own non-vacuity proof as committed tests rather than a one-off manual observation. The predicates are pure, and sibling tests feed them mutated copies of the real file. A hand-run experiment proves sensitivity on the day it is run; a committed one proves it on every commit."
  - "test_ci_calls_make_ci.py treats an aggregate target as covered when all its prerequisites are covered. The workflow deliberately splits the gate across two jobs so the secret scan gets its own fetch-depth-0 checkout; demanding a literal `make ci` step would forbid that split and would be exactly the hard-coded matching the plan prohibits."
  - "The print-ban carve-out check uses subset semantics: extra relaxed paths fail, a removed carve-out does not. Removing one makes the ban stricter, and the gate itself surfaces that immediately."
  - "test_workflow_secrets.py asserts a fourth property beyond SEC-10's three parts — that the workflow token stays read-only. Without it the empty-secret-set claim overstates itself, because GITHUB_TOKEN is injected whether or not anything references secrets.*."
  - "The README.md initially written into badsamples/ was removed: the acceptance criterion says EVERY file there carries a rule-and-consumer header, and a markdown file cannot. Its content moved into the test module docstring and the directory is now asserted to hold nothing but samples."

patterns-established:
  - "Derive a fixture's consuming test name from its file name and assert both the declaration and the test's existence, so a sample cannot drift away from its test while still looking documented."
  - "Read a shared constant out of its source of truth rather than restating it — the meta-test parses the Makefile's `RUN :=` line so it cannot exercise a different resolution of the toolchain than the gate does."
  - "When a scan must not match its own source, assemble the pattern from fragments (the repo's existing LOAD-12 convention) and exclude the scanning module from its own walk."
  - "Pair every forbidden-construct pattern with a false-positive guard, so the pattern cannot later be 'fixed' by weakening it until it stops catching the real thing."

requirements-completed: [QUAL-01, QUAL-02, OBS-03, CICD-02, CICD-03, CICD-04, SEC-02, SEC-10]

coverage:
  - id: D1
    description: "Every configured gate observed rejecting a deliberately-bad input and accepting its correct counterpart"
    requirement: "QUAL-01, QUAL-02, OBS-03, CICD-03, CICD-04"
    verification:
      - kind: unit
        ref: "tests/policy/test_gates_actually_fail.py — 12 tests: T201, D103, D417, mypy no-untyped-def, import contract, each with a positive control"
        status: pass
    human_judgment: false
  - id: D2
    description: "The bad samples cannot poison the gate they exist to prove (T-01-28)"
    requirement: "CICD-03"
    verification:
      - kind: integration
        ref: "tests/policy/test_gates_actually_fail.py::test_the_main_gate_does_not_lint_the_bad_samples — runs `make lint` and `make typecheck`, asserting exit 0 and that the tool actually appeared in the transcript"
        status: pass
      - kind: manual_procedural
        ref: "badsamples exclusion deleted from pyproject.toml → `make lint` exit 2, 11 findings, this suite failed; reverted (git diff empty)"
        status: pass
    human_judgment: false
  - id: D3
    description: "No workflow step invokes a gate tool directly; the installer remains correctly exempt"
    requirement: "CICD-02"
    verification:
      - kind: unit
        ref: "tests/policy/test_ci_invokes_make_only.py — real workflow clean; mutated copies with `uv run ruff check .`, `pytest`, a direct scanner call each reported; installer + `make gitleaks` not misreported"
        status: pass
      - kind: manual_procedural
        ref: "`- run: uv run ruff check .` added to the real ci.yml → test failed naming the step; reverted"
        status: pass
    human_judgment: false
  - id: D4
    description: "A pull request runs the full gate — the workflow's make targets cover the whole CI chain, and the CI target strictly contains the local one"
    requirement: "CICD-02"
    verification:
      - kind: unit
        ref: "tests/policy/test_ci_calls_make_ci.py — chain parsed from the Makefile prerequisite graph; a workflow running only `make install` and a `ci` that skips `check` are both reported"
        status: pass
    human_judgment: false
  - id: D5
    description: "Pinned tool versions agree across every file that names them, and the two gate tools are pinned exactly"
    requirement: "CICD-03, CICD-04"
    verification:
      - kind: unit
        ref: "tests/policy/test_pinned_tool_versions_agree.py — ruff/mypy/gitleaks/uv across pyproject, uv.lock, ci.yml, Makefile, install_gitleaks.sh, .pre-commit-config.yaml; every source perturbed in turn and required to report"
        status: pass
      - kind: manual_procedural
        ref: "ruff pin → 0.16.1 in pyproject alone → failed; GITLEAKS_VERSION → 8.30.0 in ci.yml alone → failed; both reverted"
        status: pass
    human_judgment: false
  - id: D6
    description: "The console-write ban stays repository-wide with exactly the two agreed carve-outs"
    requirement: "OBS-03"
    verification:
      - kind: unit
        ref: "tests/policy/test_print_ban_scope.py — widened carve-out, blanket ignore in four prefix forms (T/T2/T20/T201), deselected family, ignored PGH004, and inline noqa off the agreed paths"
        status: pass
      - kind: manual_procedural
        ref: "`packages/** = [T20]` added to the real pyproject → test failed naming the unapproved path; reverted"
        status: pass
    human_judgment: false
  - id: D7
    description: "The secret scan cannot be narrowed to a single commit"
    requirement: "SEC-02"
    verification:
      - kind: unit
        ref: "tests/policy/test_secret_scan_depth.py — parsed workflow; dropped `with:` and a depth-1 checkout both reported; Makefile side asserts the all-refs form"
        status: pass
    human_judgment: false
  - id: D8
    description: "The workflow references no repository secret, redacts every scanner finding, dumps no environment, and holds a read-only token"
    requirement: "SEC-10"
    verification:
      - kind: unit
        ref: "tests/policy/test_workflow_secrets.py — 9 tests including an injected secrets.* reference, six env-dump constructs, a removed --redact, a widened permission, and a false-positive guard"
        status: pass
    human_judgment: true
    rationale: "The decidable form is fully asserted and green. The general form of SEC-10 — no job ever echoes a secret — is undecidable and is documented as such in the module docstring rather than claimed. That residue is a review-time rule and becomes live the moment Phase 11 introduces the first credential."
  - id: D9
    description: "The whole gate is green with the bad samples present"
    verification:
      - kind: integration
        ref: "`make check` green; `make ci` green end to end (36 commits scanned, no leaks, SEC-11 self-test passed); 56 policy tests"
        status: pass
    human_judgment: false

duration: 29 min
completed: 2026-08-11
---

# Phase 1 Plan 05: Gate Meta-Verification Summary

**Seven policy modules that convert "the gate is configured" into "the gate is observed to work" — every linter, type checker and import contract in this phase has now been watched rejecting a deliberately-bad input and accepting its correct twin, and each module carries a committed proof of its own sensitivity.**

## Performance

- **Duration:** 29 min
- **Started:** 2026-08-11T17:18:00Z
- **Completed:** 2026-08-11T17:47:00Z
- **Tasks:** 3
- **Files created:** 17

## Accomplishments

- **Six requirements stopped resting on "a linter is configured".** `test_gates_actually_fail.py` runs each real tool through the same locked runner the Makefile uses and asserts both a non-zero exit *and* the reported rule identity: `T201` (OBS-03), `D103` and `D417` (QUAL-02), mypy `no-untyped-def` (QUAL-01/CICD-04), and the import contract. Every one is paired with a positive control, so "fails on everything" is distinguishable from "fails on the right thing".

- **The docstring cases only work because the samples are moved first.** The samples live under `tests/`, where `pyproject.toml` deliberately relaxes `D` and `ANN` — test code is not public API. Linting a bad sample in place reports nothing for D103 or D417. Each case therefore copies its sample into a library-shaped package under `tmp_path` and runs the tool there against the repository's real config, so the rules are evaluated exactly as they are for `packages/*/src`. This was discovered by probing, not assumed: the in-place run is visibly silent on the docstring rules, which is precisely the false green the plan warned about.

- **CI/local parity is now structural in both directions.** `test_ci_invokes_make_only.py` forbids duplicating a gate command into the workflow; `test_ci_calls_make_ci.py` computes the `ci` target's transitive chain from the Makefile's prerequisite graph and requires the workflow's make targets to cover all of it. Adding a target to `check` automatically tightens the test — nobody has to remember to update a list.

- **A whole class of silent weakening is now a failing test.** `test_pinned_tool_versions_agree.py` freezes the `GITLEAKS_VERSION` triplication 01-02 explicitly handed over, and extends it to ruff, mypy and uv across six files including `uv.lock` — which is what CI actually installs.

- **Both verified silent-pass modes of the secret scan are guarded.** A shallow checkout (`1 commits scanned`, green, proving nothing) and an unredacted finding are each a failing test now, asserted over the parsed workflow so a restructured step still gets checked.

- **SEC-10 is stated in the form that is true.** The module docstring records that the general form is undecidable, names the concrete leak it cannot catch, and asserts instead the stronger structural claim that the workflow references no secret at all — with an explicit note that the first entry added to `ALLOWED_SECRETS` invalidates that claim and obliges a re-audit.

## Task Commits

1. **Task 1: Meta-verification — each gate rejects a bad sample** — `edf4756` (test)
2. **Task 2: Parity and gate-strength** — `c4ad0ac` (test)
3. **Task 3: Secret-scan integrity** — `bda3026` (test)

## Verification Performed

| Claim | Command / experiment | Result |
|---|---|---|
| Precondition: the tree was green before starting | `make check`, then `make ci` | both exit 0 |
| Ten gate observations pass | `pytest tests/policy/test_gates_actually_fail.py` | 12 passed (10 gate cases + 2 structural) |
| Bad samples do not break the main gate | `make lint`, `make typecheck` | exit 0 with samples present |
| …and that assertion is not vacuous | badsamples exclusion deleted from `pyproject.toml` | `make lint` exit 2, 11 findings; suite failed; reverted |
| Direct tool invocation is caught | `- run: uv run ruff check .` added to real `ci.yml` | test failed naming the step; reverted |
| Version drift is caught, either side alone | ruff pin → `0.16.1`; `GITLEAKS_VERSION` → `8.30.0` | each failed the suite alone; both reverted |
| Carve-out widening is caught | `packages/** = ["T20"]` added to real `pyproject.toml` | test failed naming the unapproved path; reverted |
| Every scratch mutation was reverted | `git diff --stat` on tracked files | empty after each |
| Whole policy suite | `pytest tests/policy -q` | 56 passed |
| Local gate | `make check` | exit 0 |
| CI gate end to end | `make ci` | exit 0 — 36 commits scanned, no leaks, SEC-11 self-test passed |
| Files and commits exist | self-check script | 17/17 files, 3/3 commits |

## Decisions Made

Beyond the frontmatter list, two are worth reading in full:

**Sensitivity is committed rather than demonstrated once.** The plan asks for each test to be observed failing against a scratch mutation. Doing that by hand proves the test worked on the afternoon it was written; it says nothing about the day someone refactors the predicate. Every module here is built as a pure predicate over parsed inputs plus sibling tests that feed it mutated deep copies of the real file — a third carve-out, a global `T20` ignore, an injected `secrets.*`, a depth-1 checkout, a removed `--redact`, every version source perturbed in turn. The hand-run experiments were *also* performed (table above) because they exercise the file readers end to end, which an in-memory mutation does not. The two are complementary: the manual run proves the reader opens the right file, the committed one proves the predicate stays sensitive.

**An aggregate make target counts as covered when its prerequisites are.** The first draft of `test_ci_calls_make_ci.py` failed against the real workflow, reporting `ci` as uncovered. The workflow is right and the test was wrong: the gate is deliberately split across two jobs so the secret scan can have its own `fetch-depth: 0` checkout, and `make ci` is a grouping node with no recipe. Requiring a literal `make ci` step would have forbidden a correct design *and* been the hard-coded string match the plan explicitly prohibits. The fix computes coverage recursively over the prerequisite graph, which is what "the CI target is a superset of the local one" actually means.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 1 — Bug] the first bad sample carried a comment that would have disabled the rule it tests**

- **Found during:** Task 1, while writing `print_in_library.py`.
- **Issue:** the `print(value)` line ended with an explanatory comment containing the bare token `noqa`. Ruff parses a bare `# noqa` as a blanket suppression, so the sample that exists to trip `T201` would have silenced it — and the meta-test would have failed for a reason with nothing to do with the gate.
- **Fix:** removed the inline comment. The rationale lives in the file header instead, where it cannot be parsed as a directive.
- **Verification:** `ruff check` on the scratch copy reports `T201`.
- **Committed in:** `edf4756`

**2. [Rule 1 — Bug] the good samples failed their own positive control**

- **Found during:** Task 1, first probe run — a genuine RED.
- **Issue:** the `# Consumed by: …` header lines ran past 100 characters, so every good sample reported `E501`/`W505` and exited 1. A positive control that cannot be silent is not a control.
- **Fix:** shortened the consumer reference and made the test name derivable from the file name (`good_X.py` → `test_good_X_is_accepted`), which also let the header check assert the *derived* name rather than free text.
- **Verification:** all five good samples now exit 0 with zero findings.
- **Committed in:** `edf4756`

**3. [Rule 2 — Missing critical] the `make` assertion could have passed vacuously**

- **Found during:** Task 1, reviewing the 0.12 s runtime of the exclusion check.
- **Issue:** the timing was legitimate (ruff 0.029 s, mypy 0.091 s on this tree — confirmed by timing both targets directly), but a `make` target that had been gutted would also exit 0 and pass.
- **Fix:** the test now also requires the tool name to appear in make's echoed recipe.
- **Committed in:** `edf4756`

**4. [Rule 2 — Missing critical] the empty-secret-set claim overstated itself**

- **Found during:** Task 3.
- **Issue:** "this workflow holds no credential" is not implied by "it references no `secrets.*`" — `GITHUB_TOKEN` is injected into every workflow regardless. A job with `permissions: write-all` would hold a very capable credential while passing all three of SEC-10's stated parts.
- **Fix:** added a fourth assertion that the workflow token stays `contents: read` and that no job widens it, with a mutation test. This is one assertion beyond the plan's stated three, and is called out as such in the module docstring.
- **Committed in:** `bda3026`

### Scope decision (not auto-fixed — surfaced deliberately)

**5. [Scope] the installer's fail-closed path has partial, not full, coverage**

01-02 flagged that `tools/security/install_gitleaks.sh` verifies its checksum before extraction but that the behaviour has no committed automated test, and handed a policy test to this plan. This plan's task list does not admit a new module for it (`files_modified` names twelve files, none of them an installer test), so the gap is covered *partially* and the residue recorded rather than dropped:

- **Covered:** `test_secret_scan_depth.py::test_the_installer_verifies_before_it_extracts` asserts `sha256sum -c` precedes `tar -xzf` in the script. That is exactly the regression 01-02 named — a future edit that verifies *after* extraction — and it is a plausible, innocent-looking change.
- **Still open:** the behavioural test (a corrupted download producing exit 1 with nothing written) needs a PATH-shimmed `curl` and a scratch install root, which is a new module outside this plan's declared scope. Recorded in `.planning/WINDOWS.md` as an `unrun-verify` entry against `tools/security/install_gitleaks.sh`.

The docstring on that test states the limit in place, so nobody reads the ordering check as behavioural proof.

**6. [Scope] `badsamples/README.md` was written and then removed**

A directory README explaining the pairing convention was the obvious thing to add, but acceptance criterion 5 requires *every* file under `tests/policy/badsamples/` to carry a header naming the rule it trips and the test that consumes it — which a markdown file cannot. Following 01-02's precedent that a mechanical criterion is not satisfied in spirit only, the README was deleted, its content moved into the test module's docstring, and a new assertion now requires the directory to contain nothing but `.py` samples.

---

**Total deviations:** 4 auto-fixed (2 bugs, 2 missing-critical) + 2 scope decisions surfaced.
**Impact:** none on the deliverables. All acceptance criteria are met; one predecessor gap is partially closed and its residue is recorded.

## Note on the TDD marking

Task 1 carries `tdd="true"`, and as in 01-02 the deliverable *is* the test, so a RED-then-GREEN pair of commits would mean committing a knowingly-red tree — which in this phase means committing a red `make check`, the exact broken window these tests exist to prevent. The cycle was executed at runtime instead, and both RED states were real and reproducible:

- **RED (1):** the first probe run showed all five good samples exiting 1 on `E501`/`W505` — the positive controls failing. Fixed by shortening the headers, then green.
- **RED (2):** with the badsamples exclusion removed from `pyproject.toml`, `make lint` exited 2 and `test_the_main_gate_does_not_lint_the_bad_samples` failed. Reverted, then green.

The commit log therefore shows one `test(...)` commit for Task 1 rather than a `test` → `feat` pair. Recording that plainly rather than manufacturing a compliant-looking sequence.

## Issues Encountered

None beyond the deviations above. No authentication gates: this plan touches no authenticated service. The one network operation (`tools/security/install_gitleaks.sh`) is unauthenticated and succeeded.

## Known Stubs

None. All seven modules run real tools against real inputs; nothing is placeholdered or skipped, and no test is marked `xfail` or `skip`.

Two limits are documented rather than stubbed, because both are properties of the world rather than unfinished work:

- **D417 fires only when an `Args:` section exists and omits a parameter.** A docstring with no `Args:` section at all is not flagged by any rule, so QUAL-02's "documents every parameter" stays partly a review-time rule. The case is kept and the limit is stated in the consuming test rather than deleted, per the plan's instruction.
- **SEC-10's general form is undecidable.** Stated in full in `test_workflow_secrets.py`'s docstring, along with the decidable claim this phase does assert.

## Threat Flags

None. This plan introduces no network endpoint, auth path or schema at a trust boundary. It adds only tests and inert sample files, and the sample files are excluded from the main runs by configuration that is itself now asserted.

Every mitigation this plan's threat register assigned was applied and observed:

| Threat | Mitigation | Evidence |
|---|---|---|
| T-01-23 (a configured-but-dead gate) | each real tool run against a bad sample, asserting exit status and rule identity, with a good-sample control | `test_gates_actually_fail.py`, 12 passing |
| T-01-24 (gate weakened by a config edit) | carve-out scope and version agreement both fail on a narrowing or a one-sided change | real-file probes: `packages/** = [T20]`, ruff `0.16.1`, gitleaks `8.30.0` — each failed, each reverted |
| T-01-25 (CI running a different gate) | direct-invocation ban plus graph-computed chain coverage | `test_ci_invokes_make_only.py`, `test_ci_calls_make_ci.py` |
| T-01-26 (secret reaching a CI log) | empty secret set, mandatory `--redact`, no env-dump construct, read-only token; undecidable residue documented | `test_workflow_secrets.py`, 9 passing |
| T-01-27 (scan narrowed to one commit) | `fetch-depth: 0` asserted on the parsed workflow, plus the all-refs form on the Makefile side | `test_secret_scan_depth.py` |
| T-01-28 (bad samples breaking the main gate) | exclusion asserted by running the real targets; failure observed when it is removed | `make lint`/`make typecheck` exit 0 with samples present; exit 2 without the exclusion |

## Requirements

The plan declares eight requirement IDs. Five were marked complete in `REQUIREMENTS.md`; three are held open by the shared-ID gate:

- **Marked complete:** QUAL-01, QUAL-02, OBS-03, CICD-03, CICD-04 — the only other plan declaring these is 01-01, which is already summarised.
- **Held open:** CICD-02, SEC-02, SEC-10 — plan 01-09 (branch protection) also declares all three and has not run. Marking them now would flip them green while a plan that must still prove them is outstanding. They become ready when 01-09 finishes.

## Next Phase Readiness

- **Ready for 01-06 onwards:** any new workflow job, make target or pinned tool version must now satisfy these seven modules. In particular, a new job that runs a gate directly, or a make target added to `check` but not reached by CI, will fail the build rather than drift quietly.
- **Owed by 01-09:** CICD-02, SEC-02 and SEC-10 close there. `test_ci_calls_make_ci.py` already asserts the workflow covers the full chain, so the branch-protection plan inherits a checked precondition rather than an assumption.
- **Owed by Phase 11:** the first repository secret invalidates this phase's structural SEC-10 claim. `ALLOWED_SECRETS` is empty and its docstring says plainly that adding an entry obliges a re-audit of the general form.
- **Still open (recorded, not dropped):** a behavioural test for the installer's fail-closed download path. Logged in `.planning/WINDOWS.md`.

## Self-Check: PASSED

All 17 created files verified present on disk and tracked by git (10 samples, 7 policy modules). All three commits verified reachable in `git log` (`edf4756`, `c4ad0ac`, `bda3026`). No file deletions in any commit. Every scratch mutation reverted — `git diff` over tracked files is empty apart from the intended `.planning/REQUIREMENTS.md` update. `make check` and `make ci` both green at `bda3026`; 56 policy tests pass.

---
*Phase: 01-repository-toolchain-ci-skeleton*
*Completed: 2026-08-11*
