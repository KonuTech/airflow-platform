# Deferred Items — Phase 5

Out-of-scope discoveries logged during plan execution, per the executor's
scope-boundary rule (only auto-fix issues directly caused by the current
task's changes).

## Plan 05-01

### Pre-existing failure: `tests/policy/test_gates_actually_fail.py`

**Found during:** Task 1/2 verification (`pytest tests/policy -q -m "not manifests"`).

**Symptom:** `test_forbidden_import_is_rejected` and
`test_good_forbidden_import_is_accepted` both fail with an `AssertionError`
asserting a plain-text substring (e.g. `"gatecheck core must not depend on
the plugin KEPT"`) is present in `lint-imports`' captured stdout — the
actual stdout contains the same text wrapped in ANSI colour escape codes,
so the plain substring match fails.

**Root cause (not investigated further — out of scope):** appears to be an
environment-specific terminal/colour-detection difference in how
`import-linter`'s `lint-imports` CLI decides to emit ANSI colour codes
(e.g. a TTY-detection difference in this sandboxed execution environment),
not a regression in the contract-checking logic itself — both tests report
the checker DID run and DID produce the expected verdict (`KEPT`/`0
dependencies`), just wrapped in colour codes the assertion does not strip.

**Why out of scope for plan 05-01:** confirmed via `git diff uv.lock` that
this plan's `hvac` addition is the ONLY dependency change in the lockfile —
`import-linter`'s resolved version is unchanged. Neither this plan's
`pyproject.toml` edit nor any other file it touches (helm values,
`kubernetes/namespaces.yaml`, `scripts/wait-for.sh`,
`tests/policy/test_values_profiles.py`'s `_is_resource_sizing` fix) has any
relationship to import-linter, `dataplat`/`csv_processor` import structure,
or terminal output rendering.

**Action:** Not fixed. Logged here per the scope-boundary rule. A future
plan/session should investigate whether `import-linter`'s CLI needs an
explicit `--no-color` equivalent flag or `NO_COLOR`/`FORCE_COLOR` env
handling in this test's subprocess invocation.
