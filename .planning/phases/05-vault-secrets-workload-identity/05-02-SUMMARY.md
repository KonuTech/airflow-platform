---
phase: 05-vault-secrets-workload-identity
plan: 02
subsystem: secrets
tags: [vault, hvac, kubernetes-auth, secrets-management, kv-v2, workload-identity, kubernetes-pod-operator]

# Dependency graph
requires:
  - phase: 05-vault-secrets-workload-identity (plan 05-01)
    provides: "the etl KV v2 mount, kubernetes auth method, csv-processor policy/role (final binding to ServiceAccount csv-processor in namespace etl), and the persistent file audit device"
provides:
  - "resolve_secret() vault:// scheme: scheme://mount/path#field, module-level cached authenticated hvac.Client, fail-closed on malformed refs/hvac errors/missing fields -- unit-tested against a mocked client, no live cluster needed"
  - "airflow/dags/_common/kpo.py: common_kpo_kwargs()'s DATAPLAT_DB_DSN/DATAPLAT_S3_ACCESS_KEY/DATAPLAT_S3_SECRET_KEY are vault:// literals (not secretKeyRef); VAULT_ADDR/VAULT_K8S_ROLE added as plain configuration"
  - "scripts/vault-bootstrap.py's idempotent _ensure_etl_secrets() step, RUN LIVE against the cluster: etl/analytics-db and etl/minio are populated in Vault, sourced from the (now-retired) csv-processor-db/csv-processor-s3 Kubernetes Secrets"
  - "tests/e2e/vault/test_negative_auth.py: SEC-12 proven live -- default (and, as a stretch case, airflow-scheduler in a different namespace) are both denied a login against the csv-processor role"
  - "tests/e2e/vault/test_positive_auth.py: SEC-06/SEC-07 proven live in full -- csv-processor authenticates via its own role and reads exactly its own two KV paths, each a well-formed, non-empty value"
  - "csv-processor-db and csv-processor-s3 Kubernetes Secrets deleted from the live cluster; scripts/etl-secrets.sh no longer creates them -- two of the three D-01 credential migrations are complete"
affects: [05-03-airflow-vault-backend, 05-04, 05-05-secrets-documentation]

# Tech tracking
tech-stack:
  added: ["hvac>=2.4,<3 as a real packages/dataplat runtime dependency (previously only in the root pyproject.toml's cluster group, plan 05-01)"]
  patterns:
    - "vault:// URI shape scheme://mount/path#field via urlsplit(), mirroring env://file://'s existing minimalism -- .netloc is the mount, .path.lstrip('/') is the KV path, .fragment is the field"
    - "Module-level cached hvac.Client (lazy singleton, global _client), authenticated once per process via Kubernetes auth -- avoids a fresh auth/kubernetes/login round trip on every resolve_secret() call in the same pod"
    - "Kubernetes-Secret-to-Vault migration sourcing: scripts/vault-bootstrap.py reads the OLD Secret's value via kubectl get secret -o jsonpath + base64 decode (subprocess.run, never printed) and writes the SAME value into Vault, so the credential itself never changes during the swap -- only its delivery mechanism does"
    - "A migration-proof test that compares a NEW backend's value against an OLD backend's value must not outlive the OLD backend: once the old source is retired (by the same plan's own later task), any lingering comparison against it makes the test permanently unrunnable. The comparison is a one-time proof performed live at migration time, not a standing invariant -- the test should assert the new value is well-formed on its own after the old source is gone (see test_positive_auth.py's own fix, this plan)"
    - "A credential retirement's blast radius is not limited to the file the plan names -- grep the literal Secret/credential name across the WHOLE tests/ and scripts/ tree before deleting it live, not just the files the current task's <files> list names. A different test tier can depend on the same Secret for an unrelated reason (this plan found tests/e2e/slice/conftest.py's default DB connection fixture reads csv-processor-db) without either plan author having connected the two at write time"

key-files:
  created:
    - tests/e2e/vault/test_positive_auth.py
    - tests/e2e/vault/test_negative_auth.py
  modified:
    - packages/dataplat/pyproject.toml
    - packages/dataplat/src/dataplat/errors.py
    - packages/dataplat/src/dataplat/secrets/resolver.py
    - tests/unit/test_secrets_resolver.py
    - uv.lock
    - airflow/dags/_common/kpo.py
    - scripts/vault-bootstrap.py
    - scripts/etl-secrets.sh
    - .planning/phases/05-vault-secrets-workload-identity/deferred-items.md

key-decisions:
  - "hvac's KV v2 field value is wrapped in str(...) before being returned from resolve_secret()'s vault:// branch -- hvac has no type stubs (ignore_missing_imports=true), so the raw dict-index expression is Any-typed; str(...) gives mypy strict a statically str-typed return without changing runtime behavior for the string values Vault actually stores."
  - "The Vault root-token/unseal-key loss that blocked this plan's first execution attempt was resolved OUTSIDE a plan-executor session: the orchestrator, with explicit user approval, deleted vault-0's StatefulSet/pod/PVCs, redeployed Vault fresh, and re-ran make vault-unseal (creating .secrets/vault-init.json on the MAIN tree this time, not an ephemeral worktree that would delete it on merge) and make vault-bootstrap. This plan's own already-committed Task 2 code (_ensure_etl_secrets()) ran successfully as part of that recovery, confirmed by its own output naming both secrets created. See Deviations for the full account."
  - "test_positive_auth.py's original value-equality assertion (Vault's copy byte-identical to the csv-processor-db/csv-processor-s3 Secrets it migrated from) was REMOVED, not kept, once this plan's own Task 3 mandated deleting those Secrets -- keeping it would make the test permanently fail on every future run, including make vault-verify's standing per-wave gate. The equality proof was performed live once, immediately before deletion; going forward the test asserts the values are well-formed on their own."
  - "tests/e2e/slice/conftest.py's analytics_connection fixture (used by the large majority of Phase 4's e2e slice tier) reads csv-processor-db directly and was found to depend on a Secret this plan retires. This was deliberately NOT auto-fixed here (Rule 4 -- architectural: a correct fix needs a NEW root-token-authenticated Vault read in a host-side test harness with no projected ServiceAccount token, not a same-file assertion tweak) -- flagged in deferred-items.md and this Summary instead of silently expanding this plan's file scope."
  - "All six of this plan's own requirements (SEC-01, SEC-03, SEC-04, SEC-06, SEC-07, SEC-12) are marked complete in this session's final commit, correcting the prior attempt's SUMMARY claim that SEC-03/SEC-04/SEC-12 were 'already marked complete' -- REQUIREMENTS.md showed all six still Pending going into this session, so that prior claim was never actually executed against the file. All six are now genuinely proven and marked together, since the whole plan is complete."

patterns-established:
  - "A credential migration's live-bootstrap script sources its NEW backend's value from the OLD backend directly (kubectl get secret, not a human-supplied value) -- the credential's actual VALUE never changes across a delivery-mechanism swap, only the SOURCE code that installed the old Secret."
  - "Before deleting a live credential a plan retires, grep its literal name across the whole tests/ and scripts/ tree (not just the current task's declared files) -- a sibling test tier can depend on it for a reason unconnected to the plan performing the retirement."

requirements-completed: [SEC-01, SEC-03, SEC-04, SEC-06, SEC-07, SEC-12]

# Metrics
duration: 45min
completed: 2026-08-14
---

# Phase 5 Plan 2: SecretsResolver vault:// Scheme & KPO Workload Identity Summary

**`vault://` scheme live in `resolve_secret()` with a cached Kubernetes-auth client, the KPO pod wired to `vault://` literals, and both SEC-06/SEC-07 (positive) and SEC-12 (negative) proven live end-to-end -- `csv-processor-db`/`csv-processor-s3` Kubernetes Secrets are deleted from the live cluster and `scripts/etl-secrets.sh` no longer creates them.**

## Performance

- **Duration:** 45 min total across two executor sessions (25 min original + ~20 min continuation), excluding an intervening orchestrator-performed Vault recovery that ran outside a timed plan-executor session (see Deviations)
- **Session 1 -- Started:** 2026-08-14T11:15:41Z
- **Session 1 -- Completed:** 2026-08-14T11:38:17Z (Tasks 1-2 complete, Task 3 partially complete: tests written and SEC-12 proven live, but blocked on a lost Vault root token before the retirement step)
- **Session 2 (this continuation) -- Started:** ~2026-08-14T11:58Z (estimate -- not captured precisely at session start; the orchestrator's own recovery work, external to this session, finished around 11:57:19Z per `.secrets/vault-init.json`'s mtime)
- **Session 2 -- Completed:** 2026-08-14T12:17:56Z
- **Tasks:** 3/3 fully complete
- **Files modified:** 11 total across the plan (2 created, 9 modified: 7 from session 1, plus `scripts/etl-secrets.sh` and `deferred-items.md` from session 2; `tests/e2e/vault/test_positive_auth.py` was created in session 1 and edited again in session 2)

## Accomplishments

- `resolve_secret("vault://etl/analytics-db#dsn")` and friends work today, proven by 6 unit tests against a mocked `hvac.Client` -- no live cluster needed, all offline checks (ruff, mypy strict, `uv lock --check`) green
- `common_kpo_kwargs()`'s three credential env vars are `vault://` literals instead of `secretKeyRef`, with `VAULT_ADDR`/`VAULT_K8S_ROLE` added as plain configuration -- passes `test_dag_thinness.py` and every offline check
- `scripts/vault-bootstrap.py`'s `_ensure_etl_secrets()` step **ran live** against the recovered cluster: `etl/analytics-db` and `etl/minio` are populated in Vault, confirmed by the bootstrap run's own output (`secret etl/analytics-db: created`, `secret etl/minio: created`)
- **SEC-12 proven live**: `tests/e2e/vault/test_negative_auth.py` -- both the required `default`-ServiceAccount case and the optional stretch case (`airflow-scheduler`, a different namespace) pass, 2/2, re-confirmed in this session
- **SEC-06/SEC-07 proven live IN FULL**: `tests/e2e/vault/test_positive_auth.py`'s `client.auth.kubernetes.login(role="csv-processor", jwt=<csv-processor's own token>)` succeeds, and the subsequent `read_secret_version` calls against `etl/analytics-db` and `etl/minio` return well-formed, non-empty values -- re-confirmed independently in this session both before and after this session's own edit to the file
- **`csv-processor-db` and `csv-processor-s3` Kubernetes Secrets deleted from the live cluster** (`kubectl --context kind-airflow-platform delete secret csv-processor-db csv-processor-s3 -n etl`), confirmed `NotFound` afterward
- **`scripts/etl-secrets.sh` no longer creates either Secret**: `_ensure_csv_processor_db_secret`/`_ensure_csv_processor_s3_secret` and their `cmd_ensure` calls are deleted, along with their now-unused constants and helper (`ETL_NAMESPACE`, `ANALYTICS_CLUSTER`, `DB_SECRET`, `S3_SECRET`, `_random_hex`); `grep -v '^\s*#' scripts/etl-secrets.sh | grep -c csv-processor-db` and the `-s3` equivalent both report `0`
- Fixed a self-inflicted breakage this plan's own retirement step would otherwise have caused: `test_positive_auth.py`'s original comparison against the now-deleted Secrets is gone, replaced with structural assertions, so `make vault-verify` (the phase's standing per-wave gate) stays green going forward
- Discovered and clearly flagged (not silently fixed) a real architectural gap this retirement exposes in a DIFFERENT, Phase-4-owned test tier -- see Deviations and `deferred-items.md`

## Task Commits

Each task was committed atomically (spanning two sessions):

1. **Task 1: The vault:// contract in resolve_secret()** - `ad2750b` (feat) -- fully complete, all offline checks green
2. **Task 2: Populate Vault's etl KV secrets and wire the KPO pod's env vars** - `63555fa` (feat) -- code complete in session 1; the live `_ensure_etl_secrets()` run itself happened during the orchestrator's recovery between sessions (see Deviations), no additional commit needed for that
3. **Task 3, part A: e2e proof tests** - `aac2329` (test) -- `test_positive_auth.py`/`test_negative_auth.py` written, SEC-12 proven live; SEC-06/SEC-07's full proof and the Secret-retirement step were blocked at this commit
4. **Task 3, part B: retirement completion (this continuation session)** - `d3144e4` (feat) -- `scripts/etl-secrets.sh`'s two functions deleted, `csv-processor-db`/`csv-processor-s3` deleted from the live cluster, `test_positive_auth.py`'s doomed comparison replaced with structural assertions, `deferred-items.md` updated with two new findings

**Superseded:** `725bc94` (docs commit for the original, partially-complete SUMMARY.md) is superseded by this rewritten SUMMARY.md's own docs commit (following this file).

**Plan metadata:** (this commit, following this SUMMARY)

## Files Created/Modified

- `packages/dataplat/src/dataplat/secrets/resolver.py` - `vault://` scheme dispatch, module-level cached `_vault_client()`
- `packages/dataplat/src/dataplat/errors.py` - `SecretResolutionError` docstring corrected (`vault://` is real, not an example of an unrecognized scheme)
- `packages/dataplat/pyproject.toml` / `uv.lock` - `hvac>=2.4,<3` added as a real `dataplat` runtime dependency
- `tests/unit/test_secrets_resolver.py` - 6 tests against a mocked `hvac.Client`, replacing the old "vault:// fails closed" test
- `airflow/dags/_common/kpo.py` - three credential env vars are `vault://` literals; `VAULT_ADDR`/`VAULT_K8S_ROLE` added; unused `_DB_DSN_SECRET_NAME`/`_S3_SECRET_NAME` constants removed
- `scripts/vault-bootstrap.py` - `_kubectl_get_secret_field()` and `_ensure_etl_secrets()` (bootstrap step (h)), wired into `bootstrap()`, now confirmed run live
- `tests/e2e/vault/test_negative_auth.py` - SEC-12 live proof
- `tests/e2e/vault/test_positive_auth.py` - SEC-06/SEC-07 live proof; edited again this session to drop its comparison against the now-deleted Secrets
- `scripts/etl-secrets.sh` - `_ensure_csv_processor_db_secret`/`_ensure_csv_processor_s3_secret` and their `cmd_ensure` calls deleted; now-unused constants/helper removed; header comment updated to record the migration state
- `.planning/phases/05-vault-secrets-workload-identity/deferred-items.md` - two new findings logged (the `tests/e2e/slice/` architectural gap; an unrelated pre-existing deployed-image staleness failure)

## Decisions Made

- `str(...)`-wrapped the KV field-value return from `resolve_secret()`'s `vault://` branch for mypy-strict correctness against an untyped (`ignore_missing_imports`) `hvac` import.
- The Vault root-token loss that blocked session 1 was resolved outside a plan-executor session (orchestrator action, explicit user approval): fresh Vault deploy, `make vault-unseal` (writing `.secrets/vault-init.json` to the main tree, not a worktree this time), `make vault-bootstrap`. See Deviations for the full account.
- `test_positive_auth.py`'s value-equality comparison against `csv-processor-db`/`csv-processor-s3` was removed rather than kept once this plan's own Task 3 requires deleting those Secrets -- see Deviations and the `key-decisions` frontmatter.
- `tests/e2e/slice/conftest.py`'s dependency on `csv-processor-db` was found and deliberately NOT auto-fixed (Rule 4 -- cross-tier architectural change, out of Task 3's declared file scope) -- flagged instead of silently expanded into.
- All six of this plan's requirements (SEC-01, SEC-03, SEC-04, SEC-06, SEC-07, SEC-12) are marked complete together in this session, correcting a gap where the prior attempt's SUMMARY claimed three of them were already marked but REQUIREMENTS.md showed otherwise.

## Deviations from Plan

### Historical: Vault root-token loss and recovery (preserved briefly for the record)

**What happened (session 1):** The live cluster's `vault-0` pod was `Initialized: true, Sealed: false`, but `.secrets/vault-init.json` (the gitignored root-token/unseal-key file `scripts/vault-unseal.py` writes at initialization) did not exist anywhere in that session's environment -- it had been written inside plan 05-01's own now-deleted isolated git worktree, and gitignored files never travel between worktrees or back to the main tree. `scripts/vault-unseal.py`'s own documented failure mode names this scenario unrecoverable without a fresh Vault. Session 1 attempted the documented Rule-3 auto-fix (delete `vault-0`'s pod and PVCs, reinit), but the destructive cluster mutation was denied by the permission classifier, so session 1 escalated rather than working around the denial, completing Task 1 in full and Tasks 2-3's code (offline-verified) while SEC-12 was still provable live and SEC-06/SEC-07's auth half succeeded before failing on the missing KV data.

**Resolution (between sessions, not a timed plan-executor session):** The orchestrator investigated, confirmed recovery was impossible in place (single-share Shamir seal, root token only ever existed in the lost file, by design per D-02), obtained explicit user approval, then personally: deleted `vault-0`'s StatefulSet, pod, and both PVCs (`data-vault-0`, `audit-vault-0` -- local kind-cluster dev state, explicitly regenerable per D-14, not a production secret); redeployed Vault via `scripts/stages/80-vault.sh`; ran `make vault-unseal`, which created a NEW `.secrets/vault-init.json` **on the main tree** (not an ephemeral worktree), so it persists for this and all remaining Phase 5 waves; ran `make vault-bootstrap`, which idempotently recreated plan 05-01's mount/auth/role/policy/audit-device steps AND ran this plan's own already-committed `_ensure_etl_secrets()` step, confirmed by its own output naming both secrets created; and ran `tests/e2e/vault -m cluster`, confirming all 4 tests passed, including the previously-blocked full KV-value match.

**Why this matters for this Summary:** it explains why session 2 could proceed directly to Task 3's retirement step without repeating any Vault-side recovery work, and why `helm/values/*/vault.yaml` and every role/policy from plan 05-01 needed no changes -- only the ADMIN (root-token) capability was ever lost, never the authorization boundary itself, which is why SEC-12 (an authorization-boundary proof) was provable live throughout, even mid-blocker.

### New in this continuation session

**1. [Rule 1 - Bug] `test_positive_auth.py`'s Secret-equality comparison would have broken every future run once this plan's own retirement step completed**

- **Found during:** Task 3 completion, immediately before performing the mandated Secret deletion.
- **Issue:** The test read `csv-processor-db`/`csv-processor-s3` directly (via a local `_kubectl_get_secret_field` helper) to assert Vault's copy was byte-identical to the value it migrated from. `make vault-verify` runs the whole `tests/e2e/vault/` directory unconditionally and is the phase's own standing "after every plan wave" gate (05-VALIDATION.md). Once this plan's own Task 3 deletes both Secrets (mandated, non-negotiable), every subsequent invocation of this test -- including every later wave's `make vault-verify` -- would fail at `kubectl get secret` with `NotFound`, a self-inflicted permanent regression caused by faithfully executing Task 3 as written.
- **Fix:** Removed the two `_kubectl_get_secret_field` comparisons and the now-unused helper/import; kept and slightly strengthened the existing structural assertions (`dsn.startswith("postgresql://")`, non-empty `secret_key`). The one-time value-equality proof already happened live (both during the orchestrator's recovery run and my own re-run before this fix), so no proof is lost -- only a comparison against a source this same plan intentionally deletes.
- **Files modified:** `tests/e2e/vault/test_positive_auth.py`
- **Verification:** `pytest tests/e2e/vault/test_positive_auth.py tests/e2e/vault/test_negative_auth.py -q -m cluster` passed (3/3) both before this fix (with the Secrets still live) and after it (still with the Secrets live, pre-deletion); ruff check/format and mypy both pass on the file.
- **Committed in:** `d3144e4`

**2. [Rule 4 - Architectural, flagged not fixed] `tests/e2e/slice/conftest.py`'s default DB fixture depends on the now-deleted `csv-processor-db` Secret**

- **Found during:** Task 3 completion, grepping the whole `tests/`/`scripts/` tree for the literal Secret names before deleting them live (a check beyond what Task 3's own `<files>` list required, done because a credential retirement's blast radius is not bounded by one plan's declared files).
- **Issue:** `tests/e2e/slice/conftest.py` (Phase 4's own 04-08-PLAN.md e2e harness) reads `csv-processor-db` directly to build its `analytics_connection` fixture, documented in that file's own docstring as "the DEFAULT connection this suite's tests use." 27 references across `test_pod_kill_retry.py` (11), `test_smoke_and_idempotency.py` (7) and `test_concurrent_select.py` (9) -- the large majority of Phase 4's e2e slice tier. `make cluster-verify`'s recipe (`pytest tests/e2e/cluster tests/e2e/slice -q`) is also part of 05-VALIDATION.md's standing "Full suite command," so this will fail starting immediately after this plan's own commit.
- **Why not auto-fixed:** a correct fix needs a NEW root-token-authenticated Vault read in a host-side harness with no projected ServiceAccount token (mirroring `tests/e2e/vault/conftest.py`'s `vault_root_client` pattern, not the pod-side `_vault_client()` pattern `resolve_secret()` uses) -- a real design decision about which fixture shape and which file owns it, not a same-file assertion removal like `test_positive_auth.py`'s own fix above. It is also a DIFFERENT, Phase-4-owned test tier, not listed in Task 3's `<files>`.
- **Action:** Logged in full detail to `deferred-items.md` (including the exact failure mechanism, confirmed via a direct `kubectl get secret` `NotFound` check) and flagged here. NOT fixed. A future plan needs to either migrate `tests/e2e/slice/conftest.py` to a Vault-backed credential source, or make an explicit decision to exclude this tier from `cluster-verify` going forward.
- **Incidental finding while investigating:** an exploratory run of `tests/e2e/slice/test_smoke_and_idempotency.py -m cluster -x` (performed to empirically confirm the above) surfaced an UNRELATED, pre-existing failure first: the deployed `csv-processor` image's built git SHA (`2247d2c`) does not match the current checkout's HEAD (`96069a8`) -- the image was never rebuilt after later commits landed. Logged to `deferred-items.md` as its own out-of-scope entry; not fixed (matches 04-REVIEW.md's own standing fact about per-worktree rebuilds not carrying sibling commits).

---

**Total deviations:** 1 historical (Vault root-token loss, resolved before this session by the orchestrator, outside a timed plan-executor session) + 1 auto-fixed in this session (Rule 1) + 1 flagged-not-fixed in this session (Rule 4, with one incidental unrelated finding also logged)
**Impact on plan:** All three tasks are now fully complete, live-verified, and committed. The Rule 1 fix was necessary to prevent this plan's own retirement step from silently breaking the phase's own standing verification gate. The Rule 4 finding does not block this plan's own acceptance criteria (none of which name `tests/e2e/slice/` or `cluster-verify`), but is a real, quantified, immediate-impact gap a future plan must address.

## Issues Encountered

- Re-ran the full offline `pytest tests/policy -q -m "not manifests"` suite in this session (118 passed, 2 failed, 147.11s) -- reconfirms only the two pre-existing, unrelated `test_gates_actually_fail.py` ANSI-colour-code failures already logged in `deferred-items.md` by plan 05-01. No new regressions from this session's edits.

## User Setup Required

None. The blocker that previously required external/operator action (see the Historical deviation above) was already resolved before this session began; no further external action is needed to consider this plan complete.

## Next Phase Readiness

- **Ready:** `resolve_secret()`'s `vault://` scheme (Task 1) is complete, tested, and requires no further work.
- **Ready:** `kpo.py`'s `vault://`-literal env vars and `vault-bootstrap.py`'s `_ensure_etl_secrets()` step are both correct, committed, AND now confirmed run live -- any future plan can rely on `etl/analytics-db` and `etl/minio` genuinely holding the pipeline's credentials in Vault.
- **Done:** `csv-processor-db`/`csv-processor-s3` Kubernetes Secrets no longer exist; `scripts/etl-secrets.sh` no longer creates them. Two of the three D-01 credential migrations are complete -- only `airflow-minio-connection` (plan 05-03's own credential) remains Kubernetes-Secret-served.
- **New flag for a near-term future plan (plausibly folded into 05-03, or its own small fix):** `tests/e2e/slice/conftest.py`'s `analytics_connection` fixture needs to migrate off `csv-processor-db` to a Vault-backed credential source, or `cluster-verify` needs an explicit, argued exclusion of this tier -- see `deferred-items.md` for the full account and a suggested fix shape. Until this is addressed, `make cluster-verify` will fail on fixture setup for the large majority of Phase 4's e2e slice tests.
- **Also flagged (unrelated, pre-existing):** the deployed `csv-processor` image is stale relative to the current checkout's HEAD -- a future plan relying on live-pod evidence (05-03's own Task 2 live DAG trigger is the most likely candidate) should run `make image-csv-processor` first.
- Known, deliberately-not-yet-fixed finding carried forward from plan 05-01, unrelated to this plan: `tests/policy/test_gates_actually_fail.py`'s two ANSI-colour-code assertion failures.

---
*Phase: 05-vault-secrets-workload-identity*
*Completed: 2026-08-14*

## Self-Check: PASSED

**Files verified to exist:**
- FOUND: `tests/e2e/vault/test_positive_auth.py`
- FOUND: `tests/e2e/vault/test_negative_auth.py`
- FOUND: `packages/dataplat/pyproject.toml`
- FOUND: `packages/dataplat/src/dataplat/errors.py`
- FOUND: `packages/dataplat/src/dataplat/secrets/resolver.py`
- FOUND: `tests/unit/test_secrets_resolver.py`
- FOUND: `uv.lock`
- FOUND: `airflow/dags/_common/kpo.py`
- FOUND: `scripts/vault-bootstrap.py`
- FOUND: `scripts/etl-secrets.sh`
- FOUND: `.planning/phases/05-vault-secrets-workload-identity/deferred-items.md`

**Commits verified to exist in `git log --oneline --all`:**
- FOUND: `ad2750b` (Task 1)
- FOUND: `63555fa` (Task 2)
- FOUND: `aac2329` (Task 3, part A)
- FOUND: `d3144e4` (Task 3, part B -- this session's retirement completion)

**Acceptance criteria re-verified live in this session:**
- `pytest tests/e2e/vault/test_positive_auth.py tests/e2e/vault/test_negative_auth.py -q -m cluster` -- 3 passed (re-confirmed twice: once before editing `test_positive_auth.py`, once after)
- `kubectl --context kind-airflow-platform get secret -n etl csv-processor-db csv-processor-s3` -- both `NotFound` (exit 1)
- `grep -v '^\s*#' scripts/etl-secrets.sh | grep -c csv-processor-db` -- `0`
- `grep -v '^\s*#' scripts/etl-secrets.sh | grep -c csv-processor-s3` -- `0`
- `bash -n scripts/etl-secrets.sh` -- syntax OK
- `ruff check` / `ruff format --check` / `mypy` on `tests/e2e/vault/test_positive_auth.py` -- all pass
- `pytest tests/policy/test_no_manual_kubectl_surgery.py -q` -- 9 passed
- `pytest tests/policy -q -m "not manifests"` -- 118 passed, 2 failed (both pre-existing, unrelated, already logged in `deferred-items.md` by plan 05-01)

No missing items. This plan is now fully complete.
