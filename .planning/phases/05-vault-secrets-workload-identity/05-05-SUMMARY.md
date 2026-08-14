---
phase: 05-vault-secrets-workload-identity
plan: 05
subsystem: secrets
tags: [vault, secrets-management, policy-test, adr, documentation, openbao, regression-guard]

# Dependency graph
requires:
  - phase: 05-vault-secrets-workload-identity (plan 05-03)
    provides: "airflow-minio-connection deleted and scripts/etl-secrets.sh deleted outright -- completes all three D-01 migrations this plan's guard protects"
  - phase: 05-vault-secrets-workload-identity (plan 05-04)
    provides: "test_rotation.py/test_audit_log.py/test_dev_secrets_reproducible.py's live-proven D-03/SEC-08/SEC-13 behavior -- cited directly in docs/secrets-architecture.md's Rotation and Audit sections"
provides:
  - "tests/policy/test_no_stale_secrets.py -- SEC-01's permanent, automatically-enforced regression guard: csv-processor-db, csv-processor-s3, airflow-minio-connection may never again appear as a Secret-creation target under scripts/**/*.sh or helm/values/{local,ci}/**/*.yaml"
  - "docs/secrets-architecture.md -- SEC-14's single, cited, end-to-end secrets document: injection mechanism, trust boundaries (consolidated from all five phase PLAN.md threat models), what-is-where, rotation, audit, production substitution"
  - "docs/adr/0009-openbao-licence-escape-hatch.md -- the pre-announced Phase 5 ADR, fulfilling docs/adr/README.md's own reserved table row"
  - "docs/adr/README.md's own bookkeeping brought current: 0009 added to the Records table, the fulfilled Phase-5 row removed from Deliberately-deferred-records"
affects: []

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "A regression guard scanning parsed YAML for a re-introduced field must recurse into BOTH dicts and lists, not dicts alone -- a Kubernetes/Helm env: block is always a list of dicts, and secretKeyRef.name lives two levels inside each list element. test_workflow_secrets.py's own _flatten_keys is dict-only; this plan's _iter_leaves adds list recursion (env[0], env[1], ...) specifically because that is the EXACT shape (git show 851e7e5) the three real secretKeyRef blocks this guard protects against used -- a dict-only walk would have silently never caught a re-introduced one."

key-files:
  created:
    - tests/policy/test_no_stale_secrets.py
    - docs/secrets-architecture.md
    - docs/adr/0009-openbao-licence-escape-hatch.md
  modified:
    - docs/adr/README.md

key-decisions:
  - "Wrote a custom _iter_leaves walker (dict+list recursion) instead of importing or duplicating test_workflow_secrets.py's own _flatten_keys verbatim (the plan's own suggested approach) -- a dict-only walk cannot see a secretKeyRef.name re-introduced inside a Kubernetes env: list, which is the EXACT real historical shape all three of this phase's now-deleted secretKeyRef blocks used. Verified live: a dict-only flatten reports zero problems against the non-vacuity mutation used in this plan's own second test, which is precisely the false negative this guard exists to prevent."
  - "scripts/etl-secrets.sh -- the plan's own originally-cited file for the script-side non-vacuity test -- no longer exists (deleted outright in plan 05-03 once all three D-01 migrations completed). scripts/stages/75-etl.sh is used instead: a real, currently-committed script under the identical scanned scripts/**/*.sh surface, whose own header comment already documents in prose (never as a live creation target) that it used to hand off to the now-deleted script."
  - "docs/adr/README.md's 'Deliberately deferred records' table keeps its header row and explanatory prose in place after removing the fulfilled Phase-5 row, rather than deleting the whole table -- the plan's own instruction says remove the ROW, and the table's own stated purpose (tracking records not yet written) remains valid for any future phase that reserves one."

patterns-established:
  - "A stale-value / stale-name regression guard's YAML walker must be verified against the credential's REAL historical shape (checked via git show on the removal commit), not assumed from a sibling policy test's existing walker -- a shape mismatch (dict-only vs. dict+list) produces a guard that passes today and silently never fires against the one regression it was written for."

requirements-completed: [SEC-01, SEC-14]

# Metrics
duration: ~20min
completed: 2026-08-14
---

# Phase 5 Plan 5: SEC-01 Permanent Regression Guard & End-to-End Secrets Documentation Summary

**A permanent, list-aware pytest guard against the three D-01 migrated Secrets ever reappearing as a creation target, plus `docs/secrets-architecture.md` and the pre-announced `docs/adr/0009-openbao-licence-escape-hatch.md`, closing SEC-01 and SEC-14 and completing all twelve of Phase 5's requirements.**

## Performance

- **Duration:** ~20 min (approximate; exact start not captured via an explicit timestamp call, estimated from session context following plan 05-04's 2026-08-14T14:47:39Z hand-off recorded in STATE.md)
- **Started:** ~2026-08-14T14:50:00Z (estimate)
- **Completed:** 2026-08-14T15:09:29Z
- **Tasks:** 2/2 complete
- **Files modified:** 4 (3 created, 1 modified) — matches the plan's own `files_modified` frontmatter exactly

## Accomplishments

- `tests/policy/test_no_stale_secrets.py` created: `csv-processor-db`, `csv-processor-s3` and `airflow-minio-connection` can never again appear as a Secret-creation target — neither in a `scripts/**/*.sh` script (a comment-stripped substring scan) nor as a `secretKeyRef.name`/`existingSecret` value under `helm/values/local/` or `helm/values/ci/` (a recursive dict-**and-list** YAML walk). Confirmed passing against the current tree (0 problems — all three migrations are genuinely complete), with two non-vacuity tests and a false-positive control, 4/4 green.
- **Found and fixed a real design gap before it shipped** (see Deviations): a naive port of `test_workflow_secrets.py`'s own `_flatten_keys` (dict-only recursion) would have silently never caught a `secretKeyRef` re-introduced inside a Kubernetes `env:` list — the *exact* shape all three real, now-deleted `secretKeyRef` blocks this guard protects against used. Wrote `_iter_leaves` (dict + list recursion) instead, and the plan's own list-nested non-vacuity test proves the difference matters.
- Full `pytest tests/policy -q -m "not manifests"` re-confirmed green with the new module included: 122 passed, 2 failed (the same two pre-existing, already-logged `test_gates_actually_fail.py` ANSI-colour-code failures first flagged in plan 05-01's `deferred-items.md`, unchanged in identity — 118 → 122 is exactly this plan's 4 new tests, zero new failures).
- `docs/secrets-architecture.md` created: six cited sections — injection mechanism (the two-tier direct-SA-token-login pattern, the `vault://mount/path#field` shape, `resolve_secret()`'s dispatch, `VaultBackend`'s `backend_kwargs`, and the double-`resolve_secret()` indirection bug plan 05-03 found and fixed), trust boundaries (a table consolidated from all five phase `PLAN.md` `<threat_model>` blocks, explicitly citing T-05-05's plaintext-listener acceptance), what-is-where (every migrated credential and which identity reads it), rotation (SEC-09, backed by `test_rotation.py`'s live proof and the once-per-process-vs-live-per-lookup distinction), audit (SEC-08, backed by `test_audit_log.py` and `make vault-audit-tail`), and production substitution (SEC-14's explicit ask: auto-unseal, a genuine Shamir ceremony, OpenBao cross-referencing ADR-0009, VSO, and TLS as a hard non-local requirement).
- `docs/adr/0009-openbao-licence-escape-hatch.md` created: accepts Vault `2.0.3`/BUSL-1.1 for this milestone, names OpenBao as the migration target, with four concrete, observable migration triggers (a licence-term change, an unpatched CVE past a stated window, deployment outside local dev, or a material drop in Vault's own release cadence) — `grep -c '^## '` reports 5, matching the acceptance criterion.
- `docs/adr/README.md` brought current: the `0009` row added to the Records table (with its own sentence in the explanatory paragraph, matching the file's own established convention for describing each new record), and the now-fulfilled Phase 5 / Vault-BUSL row removed from the "Deliberately deferred records" table — the table's header and explanatory prose are left in place for any future phase that reserves a new row.

## Task Commits

Each task was committed atomically:

1. **Task 1: The permanent SEC-01 structural guard** - `f720fef` (test)
2. **Task 2: SEC-14 documentation and ADR-0009** - `7e6a532` (docs)

**Plan metadata:** (this commit, following this SUMMARY)

## Files Created/Modified

- `tests/policy/test_no_stale_secrets.py` - `STALE_SECRET_NAMES`, `_iter_leaves` (dict+list recursive YAML walker), `_script_stale_secret_problems`, `_values_stale_secret_problems`, the permanent guard test, two non-vacuity tests, one false-positive control
- `docs/secrets-architecture.md` - the six-section, fully-cited end-to-end secrets document (SEC-14)
- `docs/adr/0009-openbao-licence-escape-hatch.md` - the pre-announced Vault-licence ADR
- `docs/adr/README.md` - Records table `0009` row added; Deliberately-deferred-records table's fulfilled Phase 5 row removed

## Decisions Made

See `key-decisions` in the frontmatter for the full record. Summary: (1) wrote a custom dict+list-recursive YAML walker rather than reusing the plan's suggested dict-only `_flatten_keys`, because the real historical `secretKeyRef` shape this guard protects against lives inside a Kubernetes `env:` list; (2) used `scripts/stages/75-etl.sh` instead of the plan's originally-cited (now-deleted) `scripts/etl-secrets.sh` for the script-side non-vacuity test; (3) left the "Deliberately deferred records" table's header and prose in place after removing its one fulfilled row, since the plan's instruction was to remove the row, not the table.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] A dict-only YAML leaf walk would have silently defeated this guard's own purpose**

- **Found during:** Task 1, designing the `helm/values/*` walk.
- **Issue:** The plan's action text suggests importing `test_workflow_secrets.py`'s own `_flatten_keys` (or duplicating it) for the parsed-YAML walk. That function recurses into `dict` values only — a `list` value is treated as one opaque leaf. All three real `secretKeyRef` blocks this guard protects against (`git show 851e7e5`, plan 05-03's retirement commit) lived inside a Kubernetes `env:` list — e.g. `triggerer.env[0].valueFrom.secretKeyRef.name` — which a dict-only walk would never descend into. A guard built on `_flatten_keys` verbatim would pass every test written against a bare top-level `secretKeyRef` dict, yet silently never fire against the one shape a real regression would actually take.
- **Fix:** Wrote `_iter_leaves(value, prefix)`, a generator that recurses into both `dict` (as `_flatten_keys` already does) and `list` (indexing each element as `prefix[N]`) values. `_is_secret_creation_target_key()` still matches on the last one or two dot-separated path segments (`...secretKeyRef.name` / `...existingSecret`), which works correctly regardless of any `[N]` index segments in between.
- **Files modified:** `tests/policy/test_no_stale_secrets.py`
- **Verification:** `test_a_stale_secret_name_in_helm_values_is_reported` deliberately injects its mutation *inside* an `env:` list (mirroring the exact real historical shape), not as a bare top-level key — confirmed failing against a dict-only implementation during authoring, confirmed passing against `_iter_leaves`. Full module: 4/4 passed.
- **Committed in:** `f720fef` (Task 1 commit)

**2. [Rule 3 - Blocking] The plan's originally-cited file for the script-side non-vacuity test no longer exists**

- **Found during:** Task 1, `<read_first>`/`<behavior>` review, before writing any code.
- **Issue:** The plan cites `scripts/etl-secrets.sh` in its `<read_first>` list and describes the script-side non-vacuity test as "injecting one of the three names back into an in-memory copy of `scripts/etl-secrets.sh`'s text." That file was deleted outright in plan 05-03 (`851e7e5`) once all three D-01 migrations completed — confirmed via direct filesystem lookup (`No such file or directory`) before writing any code. The plan was written/last touched before accounting for that deletion.
- **Fix:** Used `scripts/stages/75-etl.sh` instead — a real, currently-committed script under the identical scanned `scripts/**/*.sh` surface, and (fittingly) the one file whose own header comment already documents, in prose only, that it used to hand off to the now-deleted script. The non-vacuity mechanics (mutate an in-memory copy, assert the mutation is reported, never touch disk) are unchanged from the plan's own intent.
- **Files modified:** `tests/policy/test_no_stale_secrets.py` (no other file needed to change; Task 1's own `<files>` list was unaffected)
- **Verification:** `pytest tests/policy/test_no_stale_secrets.py -q` — 4/4 passed; the non-vacuity property was independently reconfirmed by temporarily disabling the injected-mutation line and observing the expected `AssertionError`, then restoring it and re-confirming green.
- **Committed in:** `f720fef` (Task 1 commit)

---

**Total deviations:** 2 auto-fixed (1 Rule 1 bug — a design gap that would have made the new guard vacuous against its own primary threat shape; 1 Rule 3 blocking issue — a plan-cited file no longer existing). No scope creep: both fixes were required for Task 1's own acceptance criteria (a REAL, non-vacuous regression guard) to be satisfiable at all.

## Issues Encountered

- While manually verifying non-vacuity for the script-side test (per the plan's own acceptance criteria: "fails if its own injected mutation is removed"), a `git checkout --` on the not-yet-committed (untracked) test file predictably failed (`pathspec did not match any file(s)`) since the file had no prior committed version to check out. Recovered immediately via a direct `Edit` restoring the temporarily-disabled mutation line, then re-confirmed all 4 tests green before proceeding. No lasting impact; noted here only for a transparent record of the verification session.
- The full `pytest tests/policy -q -m "not manifests"` run took noticeably longer than a typical run (~225s) — consistent with `tests/policy/test_manifest_resources.py`/`test_supply_chain_guards.py`'s known heavier I/O profile in this suite, not a symptom of this plan's own new module (which itself runs in well under a second, confirmed both standalone and inside the full run).

## User Setup Required

None — no external service configuration required.

## Next Phase Readiness

- **Phase 5 is now complete.** All twelve of its requirements — INFRA-06, SEC-01, SEC-03, SEC-04, SEC-05, SEC-06, SEC-07, SEC-08, SEC-09, SEC-12, SEC-13, SEC-14 — are proven live and/or documented, matching `.planning/REQUIREMENTS.md`'s own traceability table and `.planning/ROADMAP.md`'s five stated success criteria (updated as part of this plan's state-sync step).
- `docs/secrets-architecture.md` is now the one place a future phase or operator reads to understand the whole secrets story end-to-end. Any future phase that adds a new Vault-stored credential should extend its "What is where" table (§3) — the document explicitly invites this rather than needing rediscovery.
- `tests/policy/test_no_stale_secrets.py` runs automatically as part of `make policy`/`make check` going forward; no separate invocation is needed, and no Makefile change was required.
- **Known, deliberately-not-yet-fixed findings carried forward, unrelated to this plan's own scope:**
  - `tests/policy/test_gates_actually_fail.py`'s two ANSI-colour-code assertion failures (plan 05-01, `deferred-items.md`) — re-confirmed unchanged this session.
  - `tests/e2e/slice/conftest.py`'s `analytics_connection` fixture still depends on the deleted `csv-processor-db` Secret (plan 05-02, `deferred-items.md`) — `make cluster-verify` will continue to fail on that fixture's setup until a future plan migrates it to a Vault-backed credential source; this plan's own `docs/secrets-architecture.md` does not paper over this, since SEC-01's own claim (verified by this plan's new guard) is scoped to Secret-*creation*, not to every test fixture that happens to reference a since-deleted Secret's name.
  - The self-draining Airflow scheduling backlog from plan 05-03 (documented as expected background noise in this session's own prompt context) — untouched by this plan's work, which is entirely docs and a policy test.

---
*Phase: 05-vault-secrets-workload-identity*
*Completed: 2026-08-14*

## Self-Check: PASSED

**Files verified to exist:**
- FOUND: `tests/policy/test_no_stale_secrets.py`
- FOUND: `docs/secrets-architecture.md`
- FOUND: `docs/adr/0009-openbao-licence-escape-hatch.md`
- FOUND: `docs/adr/README.md`
- FOUND: `.planning/phases/05-vault-secrets-workload-identity/05-05-SUMMARY.md`

**Commits verified to exist in `git log --oneline --all`:**
- FOUND: `f720fef` (Task 1)
- FOUND: `7e6a532` (Task 2)

**Acceptance criteria re-verified:**
- `pytest tests/policy/test_no_stale_secrets.py -q` — 4 passed
- `pytest tests/policy -q -m "not manifests"` — 122 passed, 2 failed (both pre-existing, unrelated, already logged in `deferred-items.md` by plan 05-01)
- `test -f docs/secrets-architecture.md && test -f docs/adr/0009-openbao-licence-escape-hatch.md && grep -q "0009-openbao-licence-escape-hatch" docs/adr/README.md` — all pass
- `grep -c '^## ' docs/adr/0009-openbao-licence-escape-hatch.md` — `5`
- `grep -n "^## " docs/secrets-architecture.md` — 6 named sections present

No missing items. This plan's own deliverables are complete.
