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

## Plan 05-02

### Architectural gap: `tests/e2e/slice/` depends on the now-deleted `csv-processor-db` Secret

**Found during:** Task 3 completion (the Secret-retirement step), while
verifying no test suite silently depended on `csv-processor-db`/
`csv-processor-s3` beyond `tests/e2e/vault/test_positive_auth.py` (already
fixed in this plan) and `scripts/vault-bootstrap.py` (already
guard-skipped, safe as-is).

**What was found:** `tests/e2e/slice/conftest.py` (Phase 4's own
04-08-PLAN.md e2e harness) reads the `csv-processor-db` Secret directly
(`_etl_app_credentials()` -> `_read_secret_data(kubectl_json_fn, "etl",
"csv-processor-db")`) to build its `analytics_connection` fixture --
documented in that file's own docstring as "the DEFAULT connection this
suite's tests use: `etl_app` is the exact role the real pipeline pods
authenticate as." 27 references to `analytics_connection` across
`test_pod_kill_retry.py` (11), `test_smoke_and_idempotency.py` (7) and
`test_concurrent_select.py` (9) -- the large majority of Phase 4's e2e
slice tier. `make cluster-verify`'s committed recipe is `pytest
tests/e2e/cluster tests/e2e/slice -q`, and 05-VALIDATION.md names `make
check && make test-integration && make cluster-verify && make vault-verify`
as the standing "Full suite command" this and every later Phase 5 wave is
expected to re-run.

**Why this blocks nothing in THIS plan:** confirmed via direct `kubectl get
secret -n etl csv-processor-db csv-processor-s3` immediately after deletion
-- both report `NotFound` (exit 1), which is the exact failure
`tests/e2e/slice/conftest.py`'s own `kubectl_json` helper surfaces as a
plain `AssertionError` at fixture setup (`assert proc.returncode == 0, ...`
-- the same shape `test_positive_auth.py`'s own now-removed
`_kubectl_get_secret_field` used). None of plan 05-02's own acceptance
criteria or `<verification>` block names `tests/e2e/slice/` or
`cluster-verify` -- this plan's own gate is satisfied without it.

**Why not auto-fixed here:** this is a DIFFERENT, Phase-4-owned test tier,
not listed in Task 3's `<files>` (`tests/e2e/vault/test_positive_auth.py`,
`tests/e2e/vault/test_negative_auth.py`, `scripts/etl-secrets.sh`). A
correct fix is architecturally non-trivial, not a same-file assertion
removal like `test_positive_auth.py`'s own fix: `tests/e2e/slice/` runs
host-side (no projected ServiceAccount token for Vault's Kubernetes-auth
login path `resolve_secret()` itself uses), so it would need a NEW
root-token-authenticated Vault read, mirroring `tests/e2e/vault/conftest.py`'s
`vault_root_client` fixture pattern rather than the pod-side `_vault_client()`
pattern -- a real design decision (which fixture shape, which file owns it,
whether `tests/e2e/slice/` should depend on `tests/e2e/vault/`'s fixtures at
all) rather than a mechanical bug fix. Per the executor's Rule 4 (Rule
Priority: architectural changes require a decision, not a silent auto-fix).

**Action:** Not fixed. `make cluster-verify` will fail on `analytics_connection`
fixture setup (in most of `tests/e2e/slice/`'s tests) starting immediately
after this plan's commit. A future plan needs to either (a) migrate
`tests/e2e/slice/conftest.py`'s `_etl_app_credentials()` to read
`etl/analytics-db#dsn` from Vault (via a root-token client, same shape as
`tests/e2e/vault/conftest.py`'s `vault_root_client`), or (b) make an
explicit decision that this tier is historical/frozen and exclude it from
`cluster-verify` going forward. Surfaced prominently in
`05-02-SUMMARY.md`'s Deviations and Next Phase Readiness sections.

### Pre-existing, unrelated: deployed csv-processor image is stale

**Found during:** An exploratory run of `tests/e2e/slice/test_smoke_and_idempotency.py
-m cluster -x` performed to empirically confirm the finding above.

**Symptom:** `test_smoke_dag_xcom_contains_built_sha` failed before reaching
any `analytics_connection` fixture: `XCom git_sha '2247d2c' does not match
this checkout's git rev-parse --short HEAD ('96069a8')` -- the
`csv-processor` image currently running in the live cluster was built from
an earlier commit than the one checked out now.

**Root cause (not investigated further -- out of scope):** the deployed
image was never rebuilt (`make image-csv-processor`) after the commits that
have landed since it was last built -- unrelated to Vault, `SecretsResolver`,
or anything this plan (05-02) touched. Matches the exact class of gap
04-REVIEW.md's own standing fact already names: "a rebuild step inside one
plan's worktree can only ever bake in that worktree's own history."

**Action:** Not fixed. Logged here per the scope-boundary rule. A future
plan (plausibly 05-03, which also needs a live DAG trigger against a
current image per its own Task 2) should run `make image-csv-processor`
before relying on any live-pod-based test evidence.
