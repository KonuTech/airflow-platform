---
phase: 01-repository-toolchain-ci-skeleton
plan: 02
subsystem: security
tags: [gitleaks, secret-scanning, pre-commit, github-actions, supply-chain, sha256]
status: complete

requires:
  - phase: 01-01
    provides: the Makefile `gitleaks` / `gitleaks-selftest` targets, the `CI` workflow and its env block, the uv workspace the self-test runs inside
provides:
  - .gitleaks.toml with two path-AND-prefix allowlists that cannot silence anything outside tests/fixtures/
  - a checksum-verified, idempotent gitleaks installer that fails closed on tamper
  - tools/security/gitleaks_selftest.py — SEC-11's negative proof, observed failing a build
  - executed evidence for 01-RESEARCH.md assumption A1 (condition = "AND" is a real conjunction)
  - the `Secret scan (full history)` CI job on a fetch-depth 0 checkout
affects:
  - 01-03 (the corpus generator must emit the SYNTH_ prefix — the allowlist keys on it)
  - 01-05 (policy tests: fetch-depth, no secrets.*, GITLEAKS_VERSION agreement)
  - 01-09 (branch protection; publishing the repository depends on this scan being green)
  - every later phase that adds a workflow job or a credential-handling path

actuals:
  tokens: 7400
  tasks: 3
  commits: 3

tech-stack:
  added:
    - gitleaks 8.30.1 (binary in a run: step, never gitleaks-action — no licence, no rate limit)
    - pre-commit hook set — ruff 0.16.2, gitleaks 8.30.1, pre-commit-hooks 6.0.0
  patterns:
    - Allowlists are scoped by path AND by value prefix; neither alone silences anything
    - Downloaded tooling is checksum-verified before extraction and fails closed
    - Credential-shaped test values are derived at runtime, never written as literals
    - Controls are proven by observing them fail, not by observing them configured

key-files:
  created:
    - .gitleaks.toml
    - .pre-commit-config.yaml
    - tools/security/__init__.py
    - tools/security/gitleaks_selftest.py
    - tools/security/install_gitleaks.sh
  modified:
    - .github/workflows/ci.yml

key-decisions:
  - "Canary values are derived at runtime from a namespaced SHA-256 rather than written as literals: a credential-shaped literal in tools/security/ would be found by this repository's own `make gitleaks`, so the file proving the control works would trip it."
  - "The self-test asserts a specific set of rule identifiers, not merely a non-zero exit, so an upstream ruleset that stops matching a credential shape fails loudly instead of silently."
  - "The Slack canary carries the rule's structural shape (xoxb- + two numeric groups) because a flat alphanumeric tail matches nothing — discovered by the self-test's first run, not assumed."
  - "The Makefile was NOT modified to auto-invoke the installer: the Makefile belongs to a sibling plan in this wave. The installer is idempotent and invoked explicitly by CI."
  - "pre-commit is documented in-file as a courtesy that avoids a history rewrite, never a gate, so nobody later mistakes a bypassable local hook for the control."

patterns-established:
  - "Prove a negative control by building a disposable repository in a temp dir — never by committing a canary to real history, which would permanently trip every future full-history scan and force the global allowlist the config exists to avoid."
  - "When a research assumption is tagged HIGH risk, the plan that depends on it converts it to evidence with a committed assertion, and the assertion is itself checked for vacuity by breaking the config and watching it fail."

requirements-completed: [SEC-02, SEC-10, SEC-11]

coverage:
  - id: D1
    description: "Scanner configuration whose allowlists are scoped by path AND by the SYNTH_ prefix, so the prefix cannot silence anything outside the fixture tree"
    requirement: "SEC-02"
    verification:
      - kind: integration
        ref: "uv run python -c 'tomllib … assert len(allowlists)==2 and all(condition==AND, paths, regexes)'"
        status: pass
      - kind: integration
        ref: "make gitleaks (25 commits scanned, working tree scanned, exit 0)"
        status: pass
    human_judgment: false
  - id: D2
    description: "SEC-11 negative proof: the scanner is observed exiting 1 on credential-shaped canaries and naming the expected rules"
    requirement: "SEC-11"
    verification:
      - kind: integration
        ref: "make gitleaks-selftest — reports aws-access-token, generic-api-key, github-pat, slack-bot-token"
        status: pass
    human_judgment: false
  - id: D3
    description: "01-RESEARCH.md assumption A1 converted to evidence: a byte-identical line is reported under packages/ and silenced under tests/fixtures/, so condition = \"AND\" is a genuine conjunction"
    requirement: "SEC-02"
    verification:
      - kind: integration
        ref: "tools/security/gitleaks_selftest.py::assert_allowlist_is_conjunction_scoped"
        status: pass
      - kind: manual_procedural
        ref: "vacuity probe — condition flipped to OR, self-test failed with the repository-wide-allowlist message, config reverted"
        status: pass
    human_judgment: false
  - id: D4
    description: "The CI job scans the complete history on every pull request and references no repository secret"
    requirement: "SEC-10"
    verification:
      - kind: integration
        ref: "uv run python -c 'yaml … assert fetch-depth==0, make gitleaks, make gitleaks-selftest' → SECRETS_JOB_OK"
        status: pass
      - kind: integration
        ref: "grep -Ec 'secrets\\.' .github/workflows/ci.yml → 0; permissions == {contents: read}; no job overrides"
        status: pass
      - kind: integration
        ref: "make ci (the identical targets the job runs) → green end to end"
        status: pass
    human_judgment: true
    rationale: "The job's shape is asserted mechanically, but it has never executed on a GitHub runner — no pull request exists yet. A real green run is phase acceptance (01-09), and only that proves the download step works on a runner rather than on this machine."
  - id: D5
    description: "The gitleaks binary is fetched with its published SHA-256 verified before extraction, failing closed on tamper"
    verification:
      - kind: manual_procedural
        ref: "PATH-shimmed curl corrupted the tarball in transit → exit 1, 'Refusing to extract or install', no tools/bin/ created"
        status: pass
    human_judgment: true
    rationale: "The fail-closed path was observed by hand and is not covered by a committed automated test, so nothing would catch a future edit that verifies after extraction instead of before. A policy test belongs in plan 01-05."
  - id: D6
    description: "Local pre-commit hooks (ruff, ruff-format, gitleaks, large-file, private-key) as a courtesy that avoids a history rewrite"
    verification:
      - kind: other
        ref: "uv run pre-commit validate-config .pre-commit-config.yaml → exit 0"
        status: pass
    human_judgment: true
    rationale: "Schema validity is proven; hook behaviour is not. The hooks were never installed or executed here (the gitleaks hook builds from source via the golang language runtime), and by design nothing in the project may depend on them — CI is the gate."

duration: 16 min
completed: 2026-08-11
---

# Phase 1 Plan 02: Secret Scanning Summary

**gitleaks 8.30.1 wired into CI over the full history and the working tree, with allowlists proven conjunction-scoped by a self-test that watches the scanner exit 1 on a disposable repository.**

## Performance

- **Duration:** 16 min
- **Started:** 2026-08-11T16:37:00Z
- **Completed:** 2026-08-11T16:53:00Z
- **Tasks:** 3
- **Files created/modified:** 6

## Accomplishments

- **The scanner is live, and that is now a fact rather than a configuration.** `make gitleaks-selftest` builds a disposable git repository, commits credential-shaped canaries into it, runs the project's own `.gitleaks.toml`, and asserts exit 1 plus the rule identifiers `aws-access-token`, `generic-api-key`, `github-pat`, `slack-bot-token`.
- **Assumption A1 is discharged.** 01-RESEARCH.md flagged `condition = "AND"` as HIGH risk — read from the gitleaks schema, never exercised. The self-test writes a *byte-identical* line to `tests/fixtures/csv/70_synthetic_credentials.csv` and `packages/dataplat/src/dataplat/leak_probe.py`; the second is reported, the first is silenced. Path is the only variable, so the conjunction is real.
- **That assertion was itself checked for vacuity.** With `condition` flipped to `OR`, the self-test failed exactly as designed: the `SYNTH_` prefix silenced the value repository-wide and `generic-api-key` vanished from the reported rules. The config was reverted immediately. This is the experiment the research asked for, and it confirms one keyword is the difference between a precise control and a disabled one.
- **The scanner binary cannot be tampered with silently.** `tools/security/install_gitleaks.sh` verifies the published SHA-256 before extraction. Observed failing closed against a corrupted download: exit 1, nothing extracted, `tools/bin/` never created.
- **Every pull request will scan the complete history**, on a `fetch-depth: 0` checkout, through `make` targets only, with no `secrets.*` reference anywhere in the workflow.

## Task Commits

1. **Task 1: Scanner configuration — allowlists that stay honest** — `371ac7a` (feat)
2. **Task 2: The negative proof — a self-test that watches the scanner fail** — `8460c24` (test)
3. **Task 3: The secrets job — full-depth history scanning in CI** — `927d9d5` (ci)

## Files Created/Modified

- `.gitleaks.toml` — extends the default ruleset; two allowlists, each carrying `paths` + `regexes` + `condition = "AND"`, with the rationale written into each `description` so a future reader cannot widen one by accident.
- `tools/security/install_gitleaks.sh` — arch-aware download, one-line checksum selection, `sha256sum -c` before `tar`, idempotent short-circuit when the pinned version is already present.
- `tools/security/gitleaks_selftest.py` — the negative proof; 403 lines including the reasoning that makes each assertion non-obvious.
- `tools/security/__init__.py` — makes the package linted and type-checked as library code.
- `.pre-commit-config.yaml` — ruff, ruff-format, gitleaks, `check-added-large-files`, `detect-private-key`; documented as a courtesy, never the gate.
- `.github/workflows/ci.yml` — the `secrets` job, plus a comment recording who owes an SEC-10 re-audit when a secret is first introduced.

## Verification Performed

| Claim | Command | Result |
|---|---|---|
| Full history + working tree are clean | `make gitleaks` | `25 commits scanned`, `no leaks found`, exit 0 |
| Allowlists are AND-scoped | `tomllib` assertion over `.gitleaks.toml` | `ALLOWLISTS_SCOPED` |
| No deprecated subcommands | `grep -Ec 'gitleaks (detect\|protect)'` over Makefile, workflow, tools | 0, 0, 0 |
| The binary is the pinned one | `./tools/bin/gitleaks version` | `8.30.1` |
| Download fails closed | PATH-shimmed `curl` corrupting the tarball | exit 1, nothing extracted, no `tools/bin/` |
| Installer is idempotent | second `install_gitleaks.sh` run | "already installed", exit 0, no download |
| Binary is never committed | `git check-ignore tools/bin/gitleaks` | ignored |
| The scanner fails a build | `make gitleaks-selftest` | scanner exit 1, four expected rules |
| The allowlist is not repository-wide | same, third assertion | `packages/` reported, `tests/fixtures/` silenced |
| …and that assertion is not vacuous | `condition` set to `OR`, re-run, reverted | self-test FAILED with the repository-wide message |
| No vendor example key is used | `grep -c 'AKIAIOSFODNN7EXAMPLE' tools/security/gitleaks_selftest.py` | 0 |
| The real repository is untouched | `git status --porcelain`, `git log --oneline \| wc -l` | empty; 21 before and after a self-test run |
| Secrets job shape | `yaml.safe_load` assertions | `SECRETS_JOB_OK` |
| No repository secret referenced | `grep -Ec 'secrets\.' .github/workflows/ci.yml` | 0 |
| Permissions not widened | `yaml` assertion | top level `contents: read`, no job override |
| Job runs only make + the helper | `yaml` assertion | `['make install', 'tools/security/install_gitleaks.sh', 'make gitleaks', 'make gitleaks-selftest']` |
| The whole chain is green | `make ci` | exit 0 end to end |

## Decisions Made

Beyond the frontmatter list, two are worth reading in full:

**Canaries are derived, not written.** The obvious implementation puts three realistic credential literals in `gitleaks_selftest.py`. That file is committed, and `make gitleaks` scans the repository — so the control would fire on the file that proves the control works, and the only escapes are a `.gitleaksignore` or a third allowlist, both of which the plan prohibits. Deriving each canary from `sha256(namespace|tag|counter)` at runtime leaves only inert prefixes (`AKIA`, `ghp_`, `xoxb-`) in the source, which match no rule on their own. It is also deterministic, so README §67 holds.

**Assert rule identifiers, not just the exit code.** An exit-code-only assertion stays green if the upstream ruleset stops recognising, say, GitHub PATs — the scanner would still fail on *something* and the regression would be invisible. Asserting `EXPECTED_RULE_IDS` as a subset means new upstream detections are fine and lost ones fail loudly. This paid for itself on the first run: the Slack canary was silently not matching because the rule requires two numeric groups, and the assertion is what surfaced it.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 3 — Blocking] `make typecheck` failed the moment `tools/` existed**

- **Found during:** Task 1, before the first commit.
- **Issue:** plan 01-01's `TYPECHECK_PATHS := … $(wildcard tools)` starts including `tools` as soon as the directory exists. Task 1 creates only a shell script there, and `mypy tools` over a directory with no `.py` files exits 2 with "There are no .py[i] files in directory 'tools'". Task 1's commit would have left the tree red until Task 2 landed.
- **Fix:** created `tools/security/__init__.py` — a file this plan owns and Task 2 needs anyway — in Task 1's commit instead of Task 2's, so every commit in this plan passes the gate.
- **Verification:** `make typecheck` green at `371ac7a`.
- **Committed in:** `371ac7a`

**2. [Rule 1 — Bug] the Slack canary matched no rule**

- **Found during:** Task 2, first self-test run (the RED observation).
- **Issue:** the canary was `xoxb-` plus a flat 40-character alphanumeric tail. The gitleaks 8.30.1 rule is `xoxb-[0-9]{10,13}-[0-9]{10,13}[a-zA-Z0-9-]*` with an entropy floor, so it matched nothing. The self-test correctly reported that `slack-bot-token` was missing from the reported rules — the assertion doing its job on its first outing.
- **Fix:** gave the canary the rule's structural shape (two 12-digit groups plus a 24-character tail) after reading the rule source at tag `v8.30.1`. Recorded the reason in a comment so nobody "simplifies" it back.
- **Verification:** `make gitleaks-selftest` now reports all four expected rules.
- **Committed in:** `8460c24`

**3. [Rule 1 — Bug] the acceptance grep failed on my own prose**

- **Found during:** Task 2 acceptance checks.
- **Issue:** the acceptance criterion is `grep -c 'AKIAIOSFODNN7EXAMPLE' tools/security/gitleaks_selftest.py` = 0. My module docstring quoted that key while explaining *why* it must not be used, so the count was 1. The criterion is mechanical and plan 01-05 may well assert it, so satisfying it in spirit only is not satisfying it.
- **Fix:** the docstring now describes the key without spelling it, and says so explicitly.
- **Verification:** grep count 0.
- **Committed in:** `8460c24`

### Scope deviation (not auto-fixed — surfaced deliberately)

**4. [Scope] the Makefile's `gitleaks` target does not auto-install the binary**

The plan's Task 1 asks for the download helper to be called by the Makefile's `gitleaks` target "when the binary is absent, so the target is idempotent". The `Makefile` is owned by a sibling plan executing in this same wave, and editing it here would collide at merge. The helper is idempotent and safe to call unconditionally, so the change is a one-line prerequisite on the target when whoever owns the Makefile next touches it:

```make
gitleaks: ; @tools/security/install_gitleaks.sh
```

**Consequence today:** on a fresh clone `make ci` fails at the `gitleaks` target with "No such file or directory" until `tools/security/install_gitleaks.sh` is run once. CI is unaffected — the workflow invokes the helper explicitly as its own step. The plan's `key_links` requirement (Makefile → `tools/bin/gitleaks`) was already satisfied by plan 01-01's target text.

---

**Total deviations:** 3 auto-fixed (2 bugs, 1 blocking) + 1 scope deviation surfaced for the orchestrator.
**Impact on plan:** none on the deliverables. Every success criterion is met; the scope deviation costs one manual command on a fresh clone and is a one-line follow-up.

## Note on the TDD marking

Task 2 carries `tdd="true"`, but its deliverable *is* the test — a negative proof, not a feature with a test beside it. A separate pytest file asserting it would have to live under `tests/`, outside this plan's declared file scope. The cycle was therefore executed at runtime rather than across commits, and both RED states were real and are reproducible:

- **RED (1):** the first self-test run failed, naming `slack-bot-token` as missing. Fixed, then green.
- **RED (2):** with `condition = "OR"` in `.gitleaks.toml`, the self-test failed with the repository-wide-allowlist message. Reverted, then green.

There is no `test(...)` → `feat(...)` gate pair in the log for this plan: the configuration landed in Task 1's `feat` commit and the test in Task 2's `test` commit, which is GREEN-then-RED-then-GREEN in commit order. Recording it plainly rather than manufacturing a compliant-looking sequence.

## Issues Encountered

None beyond the deviations above. No authentication gates: this plan touches no authenticated service. The gitleaks release download is unauthenticated.

## Known Stubs

None. Every target this plan was asked to implement (`gitleaks`, `gitleaks-selftest`) now runs for real, and `make ci` reaches the end of its chain — the condition plan 01-01's summary listed as outstanding.

Two items are recorded as `human_judgment: true` in the coverage block rather than as stubs, because they are shipped and working but not covered by a committed automated test: the installer's fail-closed path (observed by hand) and the CI job's behaviour on a real runner (no pull request exists yet).

## Threat Flags

None. This plan introduces no network endpoint, auth path or schema at a trust boundary. Its own downloads are the surface it mitigates.

Every mitigation the plan's threat register assigned here was applied and observed:

| Threat | Mitigation | Evidence |
|---|---|---|
| T-01-07 (history disclosure) | full-history + working-tree scan in CI on a full-depth checkout | `make gitleaks`, 25 commits scanned, clean |
| T-01-08 (allowlist repudiation) | path AND prefix; the self-test asserts a prefixed value outside the fixture tree is reported | self-test assertion 3, plus the OR vacuity probe |
| T-01-09 (binary tampering) | SHA-256 verified before extraction, fail closed | corrupted-download probe: exit 1, nothing extracted |
| T-01-10 (log disclosure) | `--redact` on every invocation; zero `secrets.*` references | self-test asserts no canary value in output; grep = 0 |
| T-01-11 (a scanner that never fires) | disposable-repo canaries, exit 1 + rule identifiers asserted | `make gitleaks-selftest` |
| T-01-12 (shallow checkout) | `fetch-depth: 0` on the secrets job | yaml assertion; gitleaks prints the commit count every run |

## Next Phase Readiness

- **Ready for 01-03:** the corpus generator must emit the `SYNTH_` prefix on every credential-shaped synthetic value. The allowlist keys on that prefix and on nothing else, so a corpus value without it will be reported — which is the intended behaviour, not a bug to allowlist around.
- **Ready for 01-05:** three assertions this plan established are worth freezing as policy tests — `fetch-depth: 0` on the secrets job, `grep -Ec 'secrets\.'` = 0, and `GITLEAKS_VERSION` agreement between `ci.yml`, `install_gitleaks.sh` and `.pre-commit-config.yaml` (currently 8.30.1 in all three, kept in step by hand). A fourth is new: the installer's fail-closed path has no automated coverage.
- **Ready for 01-09:** SEC-02 is green over the whole history, which is the precondition for making the repository public.
- **One follow-up owed:** the Makefile's `gitleaks` target should gain the installer prerequisite (see deviation 4).

## Self-Check: PASSED

All six files verified present on disk. All three commits verified in `git log` (`371ac7a`, `8460c24`, `927d9d5`). Working tree clean after the final task commit; no file deletions in any commit (`git diff --stat` shows 662 insertions, 1 deletion — the replaced `GITLEAKS_VERSION` comment line). `make ci` green end to end at `927d9d5`.

---
*Phase: 01-repository-toolchain-ci-skeleton*
*Completed: 2026-08-11*
