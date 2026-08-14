---
phase: 05-vault-secrets-workload-identity
plan: 04
subsystem: secrets
tags: [vault, audit-log, rotation, hvac, reproducibility, developer-tooling]

# Dependency graph
requires:
  - phase: 05-vault-secrets-workload-identity (plan 05-03)
    provides: "Airflow's VaultBackend serving minio_default live per-lookup (AIRFLOW__SECRETS__USE_CACHE=False), the empirically-corrected four-SA airflow role, and a live-confirmed KPO discover/ingest path -- all directly exercised by this plan's rotation and audit proofs"
provides:
  - "tests/e2e/vault/test_rotation.py: D-03 live proof -- rotating airflow/connections/minio_default's conn_uri in Vault is observed by the SAME already-running airflow-api-server pod's next CLI read, no restart, restored in a finally block and re-verified"
  - "scripts/vault-audit-tail.py + make vault-audit-tail (D-04): a human-readable renderer for Vault's persistent audit log, matching the make ingest-demo developer-experience bar, that never touches request/response bodies at all (so it can never mis-render an HMAC-hashed value)"
  - "tests/e2e/vault/test_audit_log.py: SEC-08 live proof -- a known successful login and a known denied login both appear in the tailed audit log, and the CURRENT plaintext DSN/MinIO access key/MinIO secret key/Airflow connection URI are all absent from the raw log text, verified non-vacuous during authoring"
  - "tests/e2e/vault/test_dev_secrets_reproducible.py: SEC-13 live proof -- re-running scripts/vault-bootstrap.py and scripts/vault-unseal.py against an already-configured Vault changes nothing (structural snapshot + KV version numbers identical before/after); .secrets/vault-init.json is gitignored/untracked; a missing init file fails vault-unseal.py closed rather than silently re-initializing"
affects: [05-05-secrets-documentation]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Rotation proof via a harmless, ignorable query-parameter append: verified live against the actually-installed apache-airflow-providers-amazon AwsConnectionWrapper (_get_credentials(**kwargs) absorbs unknown extra keys without raising) before choosing this shape, so the rotated value stays fully functional throughout the test -- zero risk to concurrently-running pipeline traffic, including the phase's own background DagRun backlog."
    - "Secret-safe assertions: every comparison/failure message that touches an `airflow connections get -o json` row goes through a `_sanitized()` copy (password/get_uri redacted) first -- a pytest assertion failure pretty-prints its operands, so an unredacted comparison would be a live channel for a credential to leak into test output, matching test_positive_auth.py's own established convention."
    - "Audit-log rendering that never reads request/response bodies at all (only time/request.path/auth.metadata/error) is a stronger SEC-08/T-05-11 mitigation than pattern-matching for Vault's hmac-sha256: prefix and hoping every case is covered -- the safest way to never mis-render a hashed value is to never look at the field that could carry one."
    - "Non-destructive non-vacuity testing for irreplaceable local state: scripts/vault-unseal.py's fail-closed behavior on a missing .secrets/vault-init.json is proven by an atomic Path.replace() rename (never a delete), with an in-memory byte backup as a second line of defence and a final byte-equality assertion in the finally block -- given this exact file's own documented history of a prior, real loss earlier in this phase (STATE.md), a literal delete-based non-vacuity test would have reintroduced the same class of risk it exists to guard against."
    - "Vault's audit log entry shape, confirmed live (not just from docs): top-level `error` (string, present only on a denied/failed operation), `auth.metadata.service_account_name`/`service_account_namespace` (present only on a Kubernetes-auth-derived identity -- a root-token request has `auth.display_name: \"root\"` but no `metadata` key at all), `request.path`, `time`. Sensitive string values inside `request`/`response` bodies are HMAC-SHA256-hashed by default, confirmed live against real DSN/access-key/secret-key/conn_uri values."

key-files:
  created:
    - tests/e2e/vault/test_rotation.py
    - scripts/vault-audit-tail.py
    - tests/e2e/vault/test_audit_log.py
    - tests/e2e/vault/test_dev_secrets_reproducible.py
  modified:
    - Makefile

key-decisions:
  - "vault-audit-tail's Makefile target has no `set -a; . helm/versions.env; set +a` env-sourcing prefix, unlike the plan's own literal wording (which cited minio-creds's shape) -- the script duplicates _kubectl_context() internally (as the SAME task's action text separately instructs) and reads helm/versions.env directly, matching vault-unseal.py/vault-bootstrap.py/ingest-demo.py's own established Python-script Makefile shape, not the shell-script minio-credentials.sh's shape. Sourcing an env var the script never reads would be dead Makefile configuration."
  - "test_rotation.py's rotation payload is a harmless appended query parameter, not a change to login/password/endpoint/region -- confirmed live via the installed provider's AwsConnectionWrapper.__post_init__/_get_credentials(**kwargs) that an unrecognised extra key is silently absorbed, never raised on, keeping the connection fully functional (including for concurrent pipeline traffic) for the whole test."
  - "test_dev_secrets_reproducible.py's non-vacuity test renames (os.replace/Path.replace) .secrets/vault-init.json aside instead of deleting it, given this exact file's own recent, real loss earlier in this phase -- an atomic rename can never produce a state where the data does not exist somewhere on disk, unlike a delete-then-rewrite sequence."
  - "test_dev_secrets_reproducible.py's bootstrap-reproducibility snapshot captures only structure and KV VERSION NUMBERS, never a secret VALUE -- safe to print in any assertion failure message, and sufficient to detect a silent rewrite (Vault's own create_role/create_or_update_secret always produce a new version on any write, so an unchanged version number is proof of no write, not just a proxy for one)."

patterns-established:
  - "A rotation/reproducibility test that mutates live Vault state always restores it in a `finally` block AND re-reads to confirm the restore succeeded -- never trusts that a `finally` block's own write call succeeding is sufficient proof."
  - "When a plan cites two Makefile precedents for the same new script (one shell-script shape, one Python-script shape) and the script's own action text separately instructs a self-contained design, follow the design instruction and its own established sibling shape, not the cited Makefile precedent that would contradict it."

requirements-completed: [SEC-08, SEC-09, SEC-13]

# Metrics
duration: ~40min
completed: 2026-08-14
---

# Phase 5 Plan 4: Vault Rotation, Audit Visibility & Dev-Secret Reproducibility Summary

**Three live proofs against the fully-migrated Vault identity set (D-03 credential rotation with no restart, SEC-08 audit content with zero leaked secret values, SEC-13 bootstrap/unseal re-run idempotency) plus `make vault-audit-tail`, a human-readable audit log renderer matching the `make ingest-demo` developer-experience bar.**

## Performance

- **Duration:** ~40 min (approximate; exact start not captured via an explicit timestamp call, estimated from session context and git commit history)
- **Started:** ~2026-08-14T14:00:00Z (estimate, following plan 05-03's 14:05:55Z completion)
- **Completed:** 2026-08-14T14:41:00Z
- **Tasks:** 3/3 plan tasks complete
- **Files modified:** 5 (4 created, 1 modified) -- matches the plan's own `files_modified` frontmatter exactly

## Accomplishments

- `tests/e2e/vault/test_rotation.py` proves D-03 live: rotates `airflow/connections/minio_default`'s `conn_uri` to a new Vault KV version, reads it via `kubectl exec ... airflow connections get minio_default` against the SAME already-running `airflow-api-server` pod before and after, asserts the second read reflects the change, and restores the original value in a `finally` block -- re-verified by re-reading after restoration. Confirmed live, twice.
- `scripts/vault-audit-tail.py` + `make vault-audit-tail` (D-04) tails Vault's persistent audit log via the already-permitted `kubectl exec -i` pattern (never `kubectl logs`) and renders one compact, human-readable line per entry (timestamp, request path, calling identity, outcome) -- confirmed live against the real cluster, producing clean output with zero raw JSON dumped.
- `tests/e2e/vault/test_audit_log.py` proves SEC-08 live: a deliberately-triggered `csv-processor` login succeeds and is recorded; a deliberately-triggered `default`-ServiceAccount login is denied and recorded on the same path; the CURRENT plaintext value of all four credentials this phase migrated (analytics DSN, MinIO access key, MinIO secret key, Airflow connection URI) are read fresh and asserted absent from the raw tailed log text. Non-vacuity was proven two ways: an in-test assertion that a known-present string IS found, and (during authoring, reverted before commit) a scratch mutation confirming the SAME absence-assertion mechanism correctly FAILS when pointed at a string that genuinely is present.
- `tests/e2e/vault/test_dev_secrets_reproducible.py` proves SEC-13 live: re-running `scripts/vault-bootstrap.py` against the live, already-bootstrapped Vault leaves every auth method, secrets-engine mount, audit device, tracked role definition, and tracked KV secret's version number byte-for-byte identical; re-running `scripts/vault-unseal.py` against the already-unsealed Vault prints `"already unsealed"` and never touches `.secrets/vault-init.json`'s mtime; `.secrets/vault-init.json` is confirmed gitignored and untracked; and -- the non-vacuity proof -- with that file temporarily renamed aside (never deleted), `scripts/vault-unseal.py` fails closed with a named error instead of silently re-initializing over live data, with the original file byte-for-byte restored and verified regardless of outcome.
- Full `tests/e2e/vault` suite (16 tests, including the pre-existing `test_unseal_survives_restart.py`, which restarts `vault-0`) re-confirmed green after all three new files landed -- no regression anywhere in the phase's standing `make vault-verify` gate.
- `make policy` re-confirmed the SAME two pre-existing, already-logged (plan 05-01) ANSI-colour-code test failures in `tests/policy/test_gates_actually_fail.py`, unchanged (118 passed / 2 failed, identical counts to plan 05-03's own self-check) -- confirmed unrelated to any file this plan touched.

## Task Commits

Each task was committed atomically:

1. **Task 1: D-03 -- live rotation proof, no restart required** - `4a93899` (feat)
2. **Task 2: make vault-audit-tail (D-04) and the SEC-08 audit-content proof** - `683b9ce` (feat)
3. **Task 3: SEC-13 -- dev-secret reproducibility and isolation proof** - `451ec3c` (feat)

**Plan metadata:** (this commit, following this SUMMARY)

## Files Created/Modified

- `tests/e2e/vault/test_rotation.py` - D-03 live rotation proof (new)
- `scripts/vault-audit-tail.py` - D-04 human-readable Vault audit log renderer (new)
- `tests/e2e/vault/test_audit_log.py` - SEC-08 live audit-content proof (new)
- `tests/e2e/vault/test_dev_secrets_reproducible.py` - SEC-13 live reproducibility/isolation proof (new)
- `Makefile` - `vault-audit-tail` target + `.PHONY` entry

## Decisions Made

See `key-decisions` in the frontmatter for the full record. Summary: (1) the `vault-audit-tail` Makefile target is self-contained, matching `vault-unseal`/`vault-bootstrap`/`ingest-demo`'s own Python-script shape rather than the plan's literal (and internally inconsistent) citation of `minio-creds`'s shell-sourcing shape; (2) the rotation test's payload is a harmless appended query parameter, verified live against the installed AWS provider to never break a concurrently-running connection; (3) the SEC-13 non-vacuity test uses an atomic rename with an in-memory byte backup rather than deleting the real `.secrets/vault-init.json`, given this exact file's documented prior loss earlier in this phase.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] The plan's own Makefile-shape citation for `vault-audit-tail` conflicted with its own preceding instruction**

- **Found during:** Task 2, adding the `Makefile` target.
- **Issue:** The plan's action text instructs, in the same sentence, to (a) duplicate `_kubectl_context()` as a self-contained helper inside `scripts/vault-audit-tail.py` (so the script reads `helm/versions.env` directly and needs no externally-set `KUBECTL_CONTEXT`), and (b) shape the Makefile target to match `minio-creds`'s `set -a; . helm/versions.env; set +a; KUBECTL_CONTEXT=...` env-sourcing pattern "since this script also needs `KUBECTL_CONTEXT`". These two instructions are mutually inconsistent: `minio-credentials.sh` is a *shell* script with no other way to learn `helm/versions.env`'s values, which is why its Makefile target sources them as an env var; a *Python* script that computes its own context internally (as instruction (a), and the sibling scripts `vault-unseal.py`/`vault-bootstrap.py`/`ingest-demo.py`, already do) has no use for that env var at all.
- **Fix:** Implemented `_kubectl_context()`/`_versions_env_variable()`/`_require_kubectl()` as self-contained helpers inside `scripts/vault-audit-tail.py` (per instruction (a) and the established Python-script convention), and gave the Makefile target the SAME simple shape as `vault-unseal`/`vault-bootstrap` (`$(RUN_CLUSTER) python scripts/vault-audit-tail.py`, no env-sourcing prefix), with a comment explaining the deliberate divergence from the plan's literal `minio-creds` citation.
- **Files modified:** `Makefile`
- **Verification:** `make vault-audit-tail` run live against the cluster produces correct, human-readable output with no `KUBECTL_CONTEXT` env var set in the Makefile recipe at all.
- **Committed in:** `683b9ce` (Task 2 commit)

**2. [Rule 2 - Missing Critical] Secret-safe assertions added to test_rotation.py**

- **Found during:** Task 1, while designing the "before read != after read" assertion.
- **Issue:** The plan did not explicitly flag this, but `airflow connections get -o json`'s row includes `password` (the real MinIO secret key) and `get_uri` (which embeds it) -- a naive `assert after_read != before_read` would print both full rows, including the plaintext secret, in a pytest assertion-failure message. This directly conflicts with this codebase's own established convention (`tests/e2e/vault/test_positive_auth.py`'s own docstring) and the project's standing OBS-05 requirement that a credential resolved through the secrets layer never appears in a captured log line.
- **Fix:** Added a `_sanitized()` helper that redacts `password`/`get_uri` before any comparison or failure-message construction; every assertion in the test operates on the sanitized copy.
- **Files modified:** `tests/e2e/vault/test_rotation.py`
- **Verification:** Manually confirmed the redaction fires (temporarily forced a comparison failure during authoring and confirmed the printed dicts showed `"<redacted>"`, not the real password); reverted before the final version.
- **Committed in:** `4a93899` (Task 1 commit)

**3. [Rule 2 - Missing Critical / safety] Non-destructive non-vacuity mechanism in test_dev_secrets_reproducible.py**

- **Found during:** Task 3, implementing the plan's acceptance criterion: "Deliberately deleting `.secrets/vault-init.json` between the snapshot and the re-run... causes `scripts/vault-unseal.py`'s re-run to fail with a clear, named error."
- **Issue:** A literal delete-then-restore implementation risks permanently losing the ONLY copy of the live, already-bootstrapped Vault's unseal key/root token if the test process is interrupted between the delete and the restore-write -- exactly the failure mode this project's own STATE.md documents already happened once, earlier in this same phase (plan 05-02, session 1), requiring a destructive PVC deletion and full Vault re-bootstrap to recover.
- **Fix:** Implemented the same acceptance criterion (a missing init file must make `vault-unseal.py` fail closed) using `Path.replace()` -- an atomic filesystem rename that can never produce a state where the data does not exist anywhere on disk -- plus an in-memory byte backup as a second line of defence, and a final byte-for-byte equality assertion after restoration. The plan's own acceptance criteria explicitly permits this: "a non-vacuity check authored into the test, OR documented as verified during authoring" -- this fulfils the "authored into the test" branch with a safer mechanism achieving the identical proof.
- **Files modified:** `tests/e2e/vault/test_dev_secrets_reproducible.py`
- **Verification:** Test passed live; `.secrets/vault-init.json` confirmed present, mode `0600`, and Vault confirmed still unsealed and reachable with the SAME root token immediately afterward. No `.test-backup` file left behind. Re-ran the full test module a second time immediately after to confirm full idempotency.
- **Committed in:** `451ec3c` (Task 3 commit)

---

**Total deviations:** 3 auto-fixed (1 Rule 1 bug in the plan's own text, 2 Rule 2 safety/correctness additions). No scope creep -- all three were necessary for the plan's own acceptance criteria to be satisfiable correctly and safely.

## Issues Encountered

None. Unlike plan 05-03, this plan's three tasks all worked correctly against the already-corrected identity set and already-fixed `_build_common()` resolution path on the first live attempt -- no new bugs were found in `scripts/vault-bootstrap.py`, `scripts/vault-unseal.py`, `VaultBackend`, or `SecretsResolver`.

The phase's known background condition (a self-draining `csv_ingest_customers` DagRun backlog, documented in STATE.md and this plan's own prompt context) did not affect this plan's execution: none of the three new test files interact with `csv_ingest_customers` or any DagRun at all -- Task 1 targets Airflow's own CLI connection resolution, Task 2 targets Vault's audit log and login attempts, and Task 3 targets Vault's own bootstrap/unseal scripts. All three ran quickly and deterministically (8 tests combined in ~11s; the full 16-test `tests/e2e/vault` suite, including the pod-restarting `test_unseal_survives_restart.py`, in ~84s).

## User Setup Required

None -- no external service configuration required.

## Next Phase Readiness

- **Ready:** D-03, D-04, SEC-08 and SEC-13 are all proven live, on the cluster, by automated tests -- not merely documented, per this plan's own success criteria.
- **Ready:** `make vault-audit-tail` exists and works, matching the `make ingest-demo` developer-experience bar D-04 asked for.
- **Ready:** The full `tests/e2e/vault` suite (16 tests) is green, confirming no regression from this plan's additions to the phase's standing `make vault-verify` gate.
- **Ready for plan 05-05:** SEC-14's documentation task can cite this plan's live-proven rotation (`test_rotation.py`) and audit (`test_audit_log.py`) behavior directly, and its own docstring note in `test_dev_secrets_reproducible.py` already points to `05-VALIDATION.md`'s "Manual-Only Verifications" table (SEC-13's other half: a full `kind delete cluster` + recreate cycle) as the source 05-05's documentation should surface for a future operator.
- **Carried forward, unrelated to this plan:** the same two pre-existing `tests/policy/test_gates_actually_fail.py` ANSI-colour-code failures first logged in plan 05-01's `deferred-items.md`, re-confirmed unchanged (118 passed / 2 failed). `tests/e2e/slice/conftest.py`'s `analytics_connection` fixture dependency on the deleted `csv-processor-db` Secret (plan 05-02's `deferred-items.md`) is also unrelated to and untouched by this plan.

---
*Phase: 05-vault-secrets-workload-identity*
*Completed: 2026-08-14*

## Self-Check: PASSED

**Files verified to exist:**
- FOUND: `tests/e2e/vault/test_rotation.py`
- FOUND: `scripts/vault-audit-tail.py`
- FOUND: `tests/e2e/vault/test_audit_log.py`
- FOUND: `tests/e2e/vault/test_dev_secrets_reproducible.py`
- FOUND: `Makefile`

**Commits verified to exist in `git log --oneline --all`:**
- FOUND: `4a93899` (Task 1)
- FOUND: `683b9ce` (Task 2)
- FOUND: `451ec3c` (Task 3)

**Acceptance criteria re-verified live in this session:**
- `pytest tests/e2e/vault/test_rotation.py tests/e2e/vault/test_audit_log.py tests/e2e/vault/test_dev_secrets_reproducible.py -q -m cluster` -- 8 passed
- `pytest tests/e2e/vault -q -m cluster` (full suite, 16 tests including `test_unseal_survives_restart.py`) -- 16 passed
- `make vault-audit-tail` -- human-readable output, no raw JSON blob dumped, confirmed live twice
- `.secrets/vault-init.json` confirmed present, mode `0600`, byte-identical to its pre-test-3 state, and Vault confirmed still unsealed/reachable with the same root token afterward
- `ruff check .` / `ruff format --check .` -- all checks passed, repo-wide
- `mypy` on all 4 new/modified Python files -- no issues
- `make typecheck` (dataplat/csv-processor/tools) -- no issues, unaffected by this plan
- `make policy` -- 118 passed, 2 failed (the same pre-existing, already-logged-in-05-01 ANSI-colour-code failures in `tests/policy/test_gates_actually_fail.py`, unchanged count, unrelated to this plan)

No missing items. This plan's own deliverables are complete.
