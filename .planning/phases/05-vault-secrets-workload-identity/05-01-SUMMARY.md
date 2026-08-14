---
phase: 05-vault-secrets-workload-identity
plan: 01
subsystem: infra
tags: [vault, hvac, kubernetes-auth, secrets-management, helm, kv-v2, unseal, statefulset]

# Dependency graph
requires:
  - phase: 02-kind-cluster-core-infrastructure
    provides: kubernetes/namespaces.yaml ownership convention, scripts/helm-install.sh's helm_install wrapper, scripts/wait-for.sh's wait_for_* family, scripts/stages/*.sh runner, the etl namespace itself (created as "the Phase 5 identity seam")
  - phase: 04-vertical-slice-csv-to-analytical-postgresql
    provides: kubernetes/rbac-etl.yaml's csv-processor ServiceAccount in namespace etl -- the FINAL, already-fixed identity this plan's csv-processor Vault role binds to
provides:
  - A persistent, non-dev-mode Vault StatefulSet deployed in both Helm values profiles (local and ci), with PVC-backed file storage and a PVC-backed audit-log volume
  - scripts/vault-unseal.py -- D-02's scripted single-command init-or-unseal against .secrets/vault-init.json (gitignored, mode 600)
  - scripts/vault-bootstrap.py -- idempotent kv-v2 mounts (etl, airflow), kubernetes auth method + config, csv-processor policy/role (final binding), airflow policy/role (documented best guess), and a persistent file audit device
  - make vault-unseal / make vault-bootstrap / make vault-verify Makefile targets
  - Live, empirically-proven INFRA-06 restart-survival: a vault-0 pod delete-and-recreate reseals Vault without losing data; only the documented unseal procedure restores service
affects: [05-02-secretsresolver-vault-scheme, 05-03-airflow-vault-backend, 05-04, 05-05-secrets-documentation]

# Tech tracking
tech-stack:
  added: [hvac 2.4.0, "HashiCorp Vault Helm chart 0.34.0 (server 2.0.3)"]
  patterns:
    - "hvac idempotency checks read every system-backend list_* response via its ['data'] key (list_mounted_secrets_engines/list_auth_methods/list_enabled_audit_devices/list_policies all return BOTH a flat top-level map AND the same content nested under 'data' -- verified empirically, not assumed) -- compare membership, write only if absent, mirroring etl-secrets.sh's _secret_exists-before-_apply_secret shape"
    - "Fresh-port-forward-per-restart-boundary: any HTTP client whose target pod may have been deleted and recreated opens its OWN new kubectl port-forward afterward -- a tunnel is bound to the pod IP it first connected to and does not follow a Service to a freshly-recreated backing pod"

key-files:
  created:
    - helm/values/local/vault.yaml
    - helm/values/ci/vault.yaml
    - scripts/stages/80-vault.sh
    - scripts/vault-unseal.py
    - scripts/vault-bootstrap.py
    - tests/e2e/vault/__init__.py
    - tests/e2e/vault/conftest.py
    - tests/e2e/vault/test_unseal_survives_restart.py
  modified:
    - kubernetes/namespaces.yaml
    - helm/versions.env
    - scripts/wait-for.sh
    - pyproject.toml
    - uv.lock
    - Makefile
    - .gitignore
    - tests/policy/test_values_profiles.py
    - tests/policy/test_offline_gate_stays_offline.py

key-decisions:
  - "Both Helm values profiles set server.dev.enabled: false, deliberately reversing STACK.md's original CI-dev-mode guidance -- CI needs the same real seal/unseal/persistence semantics local does, because this phase's own tests (scripted unseal, idempotent bootstrap, restart-survival proof) assert behavior a dev-mode Vault structurally cannot exhibit."
  - "hvac 2.4.0's actual create_role() parameter names are policies/ttl/max_ttl, not token_policies/token_ttl/token_max_ttl (the Vault HTTP API's own field names, used in 05-RESEARCH.md's illustrative `vault write` CLI example and carried into the plan's action text) -- verified against the installed library's source before writing scripts/vault-bootstrap.py, so the correct names were used from the first commit."
  - "tls_disable = 1 (the chart's own standalone listener default) is left unoverridden -- an explicit, argued acceptance (T-05-05) for this LOCAL-DEV, ClusterIP-only, no-ingress Vault, not a silent inheritance."

patterns-established:
  - "Vault bootstrap idempotency: list existing state, check membership, create only if absent -- applied uniformly across kv-v2 mounts, the kubernetes auth method, policies, roles, and the audit device."
  - "E2E tests that themselves restart a StatefulSet pod must never reuse a session-scoped fixture's port-forward after the restart -- open a fresh tunnel scoped to the post-restart assertions."

requirements-completed: [INFRA-06, SEC-13]

# Metrics
duration: 55min
completed: 2026-08-14
---

# Phase 5 Plan 1: Vault Deployment, Scripted Unseal & Idempotent Bootstrap Summary

**Persistent, non-dev Vault StatefulSet in both Helm profiles, D-02's scripted single-command unseal, an idempotent hvac-based admin bootstrap (kv-v2 mounts, kubernetes auth method, both workload roles/policies, a persistent audit device), and INFRA-06's restart-survival claim live-proven against the kind cluster by actually deleting the vault-0 pod.**

## Performance

- **Duration:** 55 min
- **Started:** 2026-08-14T10:05:00Z (approx.)
- **Completed:** 2026-08-14T11:00:00Z (approx.)
- **Tasks:** 3 completed (+ 1 follow-up fix commit, see Deviations)
- **Files modified:** 18 (9 created, 9 modified)

## Accomplishments

- Vault deployed in-cluster as a persistent, non-dev-mode StatefulSet (PVC-backed file storage + PVC-backed audit storage) in both `helm/values/local/vault.yaml` and `helm/values/ci/vault.yaml`, matched per D-06 with resource sizing as the only divergence axis
- `scripts/vault-unseal.py`: a single-share/threshold-1 init-or-unseal script, live-verified idempotent (`already unsealed` on a second run, `.secrets/vault-init.json` mtime unchanged) and live-verified to correctly take the unseal-only path (not re-initialize) against a resealed, already-initialized Vault
- `scripts/vault-bootstrap.py`: idempotent creation of both KV v2 mounts, the `kubernetes` auth method + config, the `csv-processor` policy/role (final binding to the ServiceAccount `kubernetes/rbac-etl.yaml` already fixed), the `airflow` policy/role (documented best guess for plan 05-03 to empirically correct), and a persistent `file` audit device — live-verified to perform **zero writes** on a second run
- `tests/e2e/vault/test_unseal_survives_restart.py`: INFRA-06's restart-survival claim proven live — a real `kubectl delete pod vault-0`, a bounded wait for the StatefulSet-recreated pod, an assertion that the previously-written KV data is unreadable while resealed, then `scripts/vault-unseal.py` run as a subprocess restoring service and the data reading back byte-identical
- Non-vacuity proof performed live: substituting a no-op stub for `scripts/vault-unseal.py` fails the test with a clear `AssertionError` naming `is_sealed()`, not a bare crash — then the real script was restored and the suite re-confirmed green

## Task Commits

Each task was committed atomically:

1. **Task 1: Vault namespace, Helm values (both profiles), versions.env, and the stage script** - `02b6a71` (feat)
2. **Task 2: hvac dependency, scripted unseal (D-02), and the idempotent bootstrap script** - `8977d6f` (feat)
3. **Task 3: Live proof — Vault survives a pod restart, unseal restores service (INFRA-06)** - `afbb144` (test)
4. **Follow-up: reconcile `vault-verify` with the offline-gate policy suite** - `6a45532` (fix — see Deviations #3)

**Plan metadata:** (this commit, following this SUMMARY)

_Note: Task 3 carries `tdd="true"` but has no separate `<implementation>` block — the capability under test (`scripts/vault-unseal.py`) was built in Task 2, one task prior, in the same plan. A genuine RED→GREEN cycle occurred during authoring (see Issues Encountered) but was resolved before any commit, so there is a single `test(05-01):` commit rather than a separate test→feat pair. See "TDD Task 3 Interpretation" below._

## Files Created/Modified

- `helm/values/local/vault.yaml` / `helm/values/ci/vault.yaml` - Vault StatefulSet: standalone, non-dev, file storage + audit storage, matched pair per D-06
- `kubernetes/namespaces.yaml` - sixth namespace `vault` added
- `helm/versions.env` - `VAULT_CHART_VERSION=0.34.0` pinned
- `scripts/wait-for.sh` - `wait_for_pod_running` added (Running, not Ready — Vault's readinessProbe fails while sealed)
- `scripts/stages/80-vault.sh` - installs the chart with `hookOnly` wait, waits for `vault-0` Running
- `pyproject.toml` / `uv.lock` - `hvac>=2.4,<3` added to the `cluster` dependency group; resolved to `2.4.0`
- `scripts/vault-unseal.py` - D-02 scripted init-or-unseal
- `scripts/vault-bootstrap.py` - idempotent admin bootstrap (mounts, auth method, policies, roles, audit device)
- `Makefile` - `vault-unseal`, `vault-bootstrap`, `vault-verify` targets
- `.gitignore` - `.secrets/` entry (first credential-shaped value this repo stores on disk)
- `tests/e2e/vault/__init__.py`, `conftest.py`, `test_unseal_survives_restart.py` - the live restart-survival proof
- `tests/policy/test_values_profiles.py` - `_is_resource_sizing` fixed to match `storage.size` case-insensitively (see Deviations)
- `tests/policy/test_offline_gate_stays_offline.py` - broadened to an argued allowlist so `vault-verify` coexists with `cluster-verify` as a second, documented `tests/e2e` target (see Deviations)
- `.planning/phases/05-vault-secrets-workload-identity/deferred-items.md` - logs one pre-existing, unrelated test failure found during verification

## Decisions Made

- **`server.dev.enabled: false` in both profiles** (deliberate reversal of STACK.md's original CI-dev-mode recommendation): argued in both values files' header comments and in the STRIDE threat register (T-05-02) — CI needs real seal/unseal/persistence semantics because this phase's own tests assert behavior dev mode cannot exhibit.
- **`tls_disable = 1` left as the chart default** (T-05-05, accepted): plaintext HTTP is explicitly acceptable for this local-only, no-ingress, ClusterIP Vault; production TLS is deferred to plan 05-05's documentation.
- **hvac `create_role()`'s real parameter names** (`policies`/`ttl`/`max_ttl`) were used instead of the plan's illustrative `token_policies`/`token_ttl`/`token_max_ttl` — verified against the installed hvac 2.4.0 source before writing any code (see Deviations).
- **Idempotency checks read every `list_*` response via `['data']`** — verified empirically against the live Vault that `list_mounted_secrets_engines()`/`list_auth_methods()`/`list_enabled_audit_devices()`/`list_policies()` all return a dual-format envelope (flat top-level keys AND a `'data'` key with the same content); `list_roles()` is the one exception (hvac already unwraps it, and raises `InvalidPath` rather than returning an empty list when no roles exist yet).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `test_values_profiles.py`'s `_is_resource_sizing` predicate missed camelCase `*Storage.size` keys**
- **Found during:** Task 1 verification (`pytest tests/policy/test_values_profiles.py`)
- **Issue:** `_is_resource_sizing` matched the `storage.size` suffix case-sensitively. CNPG's own key is lowercase (`storage.size`), but the Vault chart's PVC knobs are `server.dataStorage.size`/`server.auditStorage.size` — camelCase, so the existing predicate reported them as an unclassified (forbidden) divergence axis between the local and CI profiles, even though they are exactly the class of value the axis's own written argument already permits ("every PVC `storage.size` may differ in magnitude").
- **Fix:** Changed the suffix check to `path.lower().endswith("storage.size")`.
- **Files modified:** `tests/policy/test_values_profiles.py`
- **Verification:** `pytest tests/policy/test_values_profiles.py -q` — 15/15 passed (previously 1 failed).
- **Committed in:** `02b6a71` (Task 1 commit)

**2. [Rule 1 - Bug, caught before any commit] Plan's illustrative `create_role()` call used the wrong hvac parameter names**
- **Found during:** Task 2, before writing `scripts/vault-bootstrap.py` (pre-implementation API verification)
- **Issue:** `05-01-PLAN.md`'s action text and `05-RESEARCH.md`'s code example both show `client.auth.kubernetes.create_role(..., token_policies=[...], token_ttl="20m", token_max_ttl="1h")` — these are the raw Vault HTTP API's field names (as used by the `vault write auth/kubernetes/role/...` CLI form), not hvac 2.4.0's actual Python method signature, which is `create_role(..., policies=[...], ttl=..., max_ttl=...)`. Calling it with `token_policies=` would have raised `TypeError: unexpected keyword argument` at runtime.
- **Fix:** Inspected `hvac.api.auth_methods.Kubernetes.create_role`'s source directly (installed package, version 2.4.0) before writing any code, and used the verified-correct parameter names from the start.
- **Files modified:** `scripts/vault-bootstrap.py` (never contained the incorrect names — this is a plan/research correction, not a code fix)
- **Verification:** Live `make vault-bootstrap` run twice against the kind cluster — first run creates the `csv-processor` and `airflow` roles successfully, second run reports both `already present`.
- **Committed in:** `8977d6f` (Task 2 commit)

**3. [Rule 1 - Bug, blocking] `test_offline_gate_stays_offline.py` hardcoded a single-target assumption `vault-verify` breaks**
- **Found during:** post-Task-3 full-suite verification (`pytest tests/policy -q -m "not manifests"`) — this command is Task 2's OWN stated `<verify>` block, which the narrower acceptance-criteria subset run before Task 2's own commit did not happen to exercise
- **Issue:** `test_cluster_verify_is_the_only_target_naming_tests_e2e` asserted `names == ["cluster-verify"]` — a hardcoded single-item list. Task 2's new `vault-verify` target (`$(RUN_CLUSTER) pytest tests/e2e/vault -q`) also names a `tests/e2e` path in its recipe, exactly as the plan's own action text deliberately specifies ("targeting the WHOLE directory so no later plan needs to edit this recipe again" — i.e. a separate, independent target, not folded into `cluster-verify`), so the test's stale single-name assumption failed.
- **Fix:** Replaced the hardcoded name with `ARGUED_TESTS_E2E_TARGETS`, an allowlist mirroring `test_values_profiles.py`'s `PERMITTED_AXES` discipline — every target carries a written argument for why it is separate, and a new `test_a_third_target_is_reported` non-vacuity test proves an unargued third target is still caught. The core D-16/WINDOWS #8 invariant (`check`/`ci` must never reach `tests/e2e/`) is untouched.
- **Files modified:** `tests/policy/test_offline_gate_stays_offline.py`
- **Verification:** `pytest tests/policy/test_offline_gate_stays_offline.py -v` — 5/5 passed (previously 1 failed); full `pytest tests/policy -q -m "not manifests"` re-run confirms only the two pre-existing, unrelated `test_gates_actually_fail.py` failures remain.
- **Committed in:** `6a45532` (standalone follow-up commit, since Tasks 1-3 were already committed when this was found)

---

**Total deviations:** 3 auto-fixed (3 Rule 1 bugs — one in an existing test found during Task 1, one in the plan's own illustrative code caught pre-implementation during Task 2, one in a different existing test found during post-Task-3 full verification)
**Impact on plan:** All three were necessary for correctness. No scope creep — every fix was either required to satisfy this plan's own stated acceptance criteria/verify blocks, or (for the `create_role` parameter names) prevented a runtime `TypeError` that would have blocked Task 2 entirely.

## Issues Encountered

- **`tests/e2e/vault/test_unseal_survives_restart.py`'s first draft had a genuine bug in its `finally` cleanup**, caught by running the test live (a real RED): the cleanup reused the pre-restart `vault_root_client` fixture's session-scoped port-forward, which is bound to the pod IP that no longer exists after `vault-0` is deleted and recreated — cleanup failed with `requests.exceptions.ConnectionError`. Every assertion BEFORE cleanup had already passed. Fixed by opening a fresh, freshly-authenticated port-forward inside `finally`, matching every other post-restart check in the same test. Re-run confirmed GREEN, and the fix was never committed in its broken form (iterated locally before the single `test(05-01):` commit).
- **A pre-existing, unrelated test failure** (`tests/policy/test_gates_actually_fail.py::test_forbidden_import_is_rejected` / `test_good_forbidden_import_is_accepted`) was discovered while running the full offline policy suite for extra confidence beyond Task 1's own narrower `<verify>` scope. Confirmed unrelated via `git diff uv.lock` (this plan's only lockfile change is `hvac`'s addition; `import-linter`'s resolved version is unchanged) and via file-scope analysis (none of this plan's changed files relate to import-linter, `dataplat`/`csv_processor` import structure, or terminal colour rendering). Logged to `deferred-items.md` per the scope-boundary rule rather than fixed.

## TDD Task 3 Interpretation

Task 3 carries `tdd="true"` and a `<behavior>` block, but the plan format for this task has a single `<action>` rather than a separate `<implementation>` — because the capability under test, `scripts/vault-unseal.py`'s unseal-restores-service behavior, was already built in Task 2, one task earlier in this same plan. Running the test after writing it therefore exercises already-built infrastructure against a live failure scenario (a real pod restart) it had never been exercised against before, rather than driving new production code into existence. This is a legitimate variant of the pattern (an E2E regression proof for tooling just built), not the anti-pattern the fail-fast rule guards against (a test that passes vacuously because nothing was implemented). Two pieces of evidence support this reading rather than a formality violation:

1. **A genuine RED did occur** (see Issues Encountered) — the first version of the test failed for a real reason (a bug in the test's own cleanup logic), was fixed, and then passed.
2. **The acceptance criteria's own fault-injection check was performed live**: substituting a no-op stub for `scripts/vault-unseal.py` made the test fail with a clear, named `AssertionError` (`vault-0 still reports sealed after scripts/vault-unseal.py exited 0 ...`, with pytest's assertion introspection showing `is_sealed()` explicitly) — proving the test has real teeth and is not a vacuous pass, satisfying the acceptance criteria's own literal wording without needing an artificial separate `feat(...)` commit.

Since this plan's frontmatter is `type: execute` (not `type: tdd`), the plan-level TDD gate enforcement (mandatory `test(...)` → `feat(...)` commit sequence) does not formally apply; only the per-task guidance does, which explicitly allows investigation and iteration before committing.

## User Setup Required

None — no external service configuration required. Everything in this plan runs against the already-live `kind-airflow-platform` cluster using existing tooling (`kubectl`, the pinned `helm` binary, `uv`).

## Next Phase Readiness

- Vault is live, unsealed, and bootstrapped on the cluster at the time this plan completed (`vault-0` `1/1 Running`, `.secrets/vault-init.json` present at mode 600).
- Plan 05-02 (`SecretsResolver`'s `vault://` scheme) can now authenticate against the `csv-processor` Vault role and read from the `etl` KV v2 mount this plan created.
- Plan 05-03 (Airflow's native `VaultBackend`) can now authenticate against the `airflow` Vault role and read from the `airflow` KV v2 mount — but must empirically verify (via this plan's own audit device) which Airflow ServiceAccount actually performs the login, since `bound_service_account_names=["airflow-api-server"]` is a documented best guess, not a confirmed binding (05-RESEARCH.md Pitfall 1).
- No credential from this plan's own work reaches git, stdout, or a Kubernetes Secret — verified by direct git-history inspection and by every script's own status-line-only printing discipline.
- Known, deliberately-not-yet-fixed finding carried forward: `tests/policy/test_gates_actually_fail.py`'s two import-linter colour-code assertion failures (pre-existing, unrelated to this plan — see `deferred-items.md`).

---
*Phase: 05-vault-secrets-workload-identity*
*Completed: 2026-08-14*

## Self-Check: PASSED

**Files verified to exist:**
- FOUND: `helm/values/local/vault.yaml`
- FOUND: `helm/values/ci/vault.yaml`
- FOUND: `scripts/stages/80-vault.sh`
- FOUND: `scripts/vault-unseal.py`
- FOUND: `scripts/vault-bootstrap.py`
- FOUND: `tests/e2e/vault/__init__.py`
- FOUND: `tests/e2e/vault/conftest.py`
- FOUND: `tests/e2e/vault/test_unseal_survives_restart.py`
- FOUND: `.planning/phases/05-vault-secrets-workload-identity/deferred-items.md`

**Commits verified to exist in `git log --oneline --all`:**
- FOUND: `02b6a71` (Task 1)
- FOUND: `8977d6f` (Task 2)
- FOUND: `afbb144` (Task 3)
- FOUND: `6a45532` (follow-up fix)

No missing items.
