# Deferred Items — Phase 04

Out-of-scope discoveries logged during plan execution. Not fixed as part of
the plan that found them (scope boundary: only auto-fix issues directly
caused by the current task's changes).

## From 04-02

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
