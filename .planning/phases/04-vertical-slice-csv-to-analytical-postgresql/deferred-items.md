# Deferred Items — Phase 04

Issues discovered during execution that are out of scope for the plan that
found them (pre-existing, in files the plan does not touch). Logged per the
executor's scope-boundary rule rather than fixed inline.

## From Plan 04-01

### `tests/policy/test_gates_actually_fail.py` — 2 pre-existing failures, unrelated to 04-01

- **Found during:** Task 3 full-gate verification (`make check` / `uv run --frozen pytest tests/policy -q -m "not manifests"`).
- **Symptom:** `test_forbidden_import_is_rejected` and `test_good_forbidden_import_is_accepted`
  both fail with `AssertionError: the checker failed/passed without
  naming/evaluating the contract`. Both assert a plain substring
  (`f"{CONTRACT_NAME} BROKEN"` / `f"{CONTRACT_NAME} KEPT"`) is present in
  `lint-imports`' captured stdout.
- **Root cause:** The pinned `import-linter==2.13` (`import-linter>=2.13,<3`
  in `pyproject.toml`) now renders its per-contract result line with an
  inline ANSI color escape sequence between the contract name and the
  KEPT/BROKEN word (e.g. `...the plugin \x1b[31mBROKEN\x1b[0m` instead of a
  plain `...the plugin BROKEN`). The plain-substring assertion written
  against an earlier `import-linter` rendering no longer matches, even
  though the tool's actual pass/fail behavior is correct (verified
  independently: `uv run --frozen lint-imports` against the real
  `dataplat`/`csv_processor` contract in `setup.cfg` reports `1 kept, 0
  broken`, exactly as expected, both before and after 04-01's changes).
- **Not caused by 04-01:** `tests/policy/test_gates_actually_fail.py` was
  last modified by Phase 1 commit `edf4756` (`test(01-05): observe every
  gate reject a bad sample and accept a good one`). 04-01 never touches
  this file, `setup.cfg`, or any `lint-imports`/lint-invocation code —
  its own `setup.cfg` Contract 1 (`dataplat core must not depend on the CSV
  plugin`) is independently confirmed `KEPT` throughout 04-01's execution.
- **Verified reproducible on `main` before 04-01's first commit** in spirit
  (the test's own fixture/assertion logic and the installed `import-linter`
  version are both untouched by this plan; the failure is deterministic on
  every invocation, not a flake).
- **Status:** Not fixed. Out of scope for 04-01 (SCOPE BOUNDARY: only
  auto-fix issues directly caused by the current task's changes).
- **Suggested resolution for whoever picks this up:** Either strip ANSI
  codes from `proc.stdout` before the substring assertion (e.g.
  `re.sub(r"\x1b\[[0-9;]*m", "", proc.stdout)`), or invoke `lint-imports`
  with a `--no-color`/`NO_COLOR=1` environment in `_import_contract()`.

## From Plan 04-02

### Pre-existing, unrelated test failures in `tests/policy/test_gates_actually_fail.py`

- **Found during:** Task 1/2 verification (full `tests/policy` run)
- **Tests:** `test_forbidden_import_is_rejected`, `test_good_forbidden_import_is_accepted`
- **Symptom:** Both fail on an `AssertionError` comparing captured `lint-imports`
  CLI output against an expected substring. The actual `lint-imports` output now
  includes a Rich-rendered ANSI-colored banner/progress display (box-drawing
  characters, animated "Checking contracts" progress bar) that the test's plain
  substring assertion does not account for — looks like upstream `import-linter`
  (or its `grimp`/`rich` dependency) started emitting a fancier terminal UI since
  this test was last touched.
- **Confirmed pre-existing and unrelated to 04-02:** `git log -1 -- tests/policy/test_gates_actually_fail.py`
  shows it was last committed in `edf4756` (phase 01, plan 01-05), and this
  plan made zero changes to that file, `pyproject.toml`, or any import-linter
  contract. Reproduces identically on an unmodified tree.
- **Status:** Deferred — not fixed by 04-02 (out of scope: import-linter
  self-test tooling, unrelated to RBAC/secrets/Helm/image-build work).
- **Suggested owner:** whichever future plan next touches CI/lint tooling, or
  a dedicated chore plan. Likely fix: strip ANSI/box-drawing output before
  the substring match (mirroring how other tests in this same phase's own
  `tests/policy/test_no_manual_kubectl_surgery.py` mask quoted spans before
  matching), or pin `import-linter`'s output mode.

**Both plans independently confirm the same underlying issue** (import-linter
output-format drift breaking a plain-substring assertion in a Phase 1 policy
test) — two independent characterizations of the same drift, not two bugs.
