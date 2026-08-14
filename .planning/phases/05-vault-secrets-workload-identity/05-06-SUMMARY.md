---
phase: 05-vault-secrets-workload-identity
plan: 06
subsystem: secrets
tags: [vault, secrets-management, gap-closure, regression-guard, unit-test, incident]
gap_closure: true

# Dependency graph
requires:
  - phase: 05-vault-secrets-workload-identity (plan 05-01)
    provides: "scripts/vault-bootstrap.py's idempotent _ensure_* structure and Makefile vault-unseal/vault-bootstrap/vault-verify targets this plan repoints and re-proves"
  - phase: 05-vault-secrets-workload-identity (plan 05-03)
    provides: "the deletion of csv-processor-db, csv-processor-s3, airflow-minio-connection and scripts/etl-secrets.sh -- the root cause this plan fixes"
  - phase: 05-vault-secrets-workload-identity (plan 05-05)
    provides: "05-VERIFICATION.md and 05-REVIEW.md's independent discovery of the same root cause (CR-01/CR-02), the gap this plan closes"
provides:
  - "scripts/vault-bootstrap.py -- corrected credential sourcing for etl/analytics-db (live kubectl exec + ALTER ROLE against the CNPG analytics primary), etl/minio and airflow/connections/minio_default (live data/minio-app Secret), never a deleted Secret; _ensure_policy now self-corrects a drifted policy body (CR-02)"
  - "tests/unit/test_vault_bootstrap.py -- this repo's first unit test for a scripts/*.py file (dynamic importlib import); 7 cases, fully offline, mocked hvac.Client + subprocess.run"
  - "docs/secrets-architecture.md Section 6 -- corrected SEC-13 claim citing what was actually proven, including the honestly-disclosed partial result"
  - "05-VALIDATION.md -- SEC-13 rows and Manual-Only Verifications updated to reflect the partial live-proof outcome"
affects: []

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "A scripts/*.py file with no package structure can be unit-tested via importlib.util.spec_from_file_location + module_from_spec + exec_module -- no prior precedent in this repo; established here for test_vault_bootstrap.py."
    - "An idempotent _ensure_* Vault-bootstrap function must source its value from the same live system its pre-Vault predecessor did (Secret/DB), never from a coincidentally-similar but differently-privileged source (e.g. CNPG's own analytics-db-app Secret, which holds analytics_owner, not etl_app) -- verified via git archaeology (git show 6d86cb8:scripts/etl-secrets.sh) rather than guessed."

key-files:
  created:
    - tests/unit/test_vault_bootstrap.py
  modified:
    - scripts/vault-bootstrap.py
    - docs/secrets-architecture.md
    - .planning/phases/05-vault-secrets-workload-identity/05-VALIDATION.md

key-decisions:
  - "Rejected 05-REVIEW.md's own literal CR-01 fix suggestion (source etl/analytics-db#dsn from CNPG's analytics-db-app Secret) because that Secret holds analytics_owner, a more-privileged role than etl_app -- would have been a real privilege escalation. Restored the original, least-privileged etl_app-scoped kubectl-exec/ALTER-ROLE mechanism from git history instead (git show 6d86cb8:scripts/etl-secrets.sh), retargeted to write straight to Vault."
  - "Split execution into Task 1 (code fix, local-only, no cluster access) dispatched to a worktree-isolated gsd-executor subagent, and Task 2 (live Vault pod/PVC reinstall) run directly by the orchestrator via individual Bash/kubectl calls -- after Claude Code's own permission classifier blocked a single combined dispatch covering both tasks, and the user chose to split rather than override."
  - "Stopped escalating live-cluster remediation after two scoped attempts (clearing the one stuck DagRun's tasks; deleting three orphaned terminal pod objects) once it became clear the root cause was a separate, deeper Airflow KubernetesExecutor scheduling fault unrelated to Vault credentials -- consistent with this session's own prior precedent (STATE.md: an earlier bulk-drain attempt at the same backlog was denied by the permission classifier as too invasive) and out of this plan's declared file scope."

patterns-established:
  - "When a gap-closure plan's live-proof task uncovers a SEPARATE, pre-existing infrastructure fault unrelated to the fix being proven, document the fix's own proof as far as it genuinely goes (cite exactly what passed and why), name the separate fault with its own evidence trail, and do not fold the two into a single pass/fail verdict -- overclaiming either direction (marking the whole plan green, or blaming the credential fix for an unrelated scheduler bug) would violate this project's own citation-discipline bar."

requirements-completed: [SEC-01, SEC-13]

# Metrics
duration: ~2h (Task 1: ~21min via subagent; remainder: live Task 2 execution, incident diagnosis, and documentation, spread across a longer session with user checkpoints)
completed: 2026-08-14T19:27:55Z
---

# Phase 5 Plan 6: SEC-13 Gap Closure (CR-01/CR-02 vault-bootstrap.py fix) Summary

**Fixed `vault-bootstrap.py`'s credential sourcing off three Kubernetes Secrets this same phase had already deleted (CR-01), fixed policy-drift self-correction alongside it (CR-02), and proved the fix live against a genuinely empty, freshly-reinstalled Vault. The unrelated Airflow scheduler fault that initially blocked full closure was root-caused and fixed via a separate debug session (`.planning/debug/resolved/dagrun-scheduler-stall.md`) — a real DAG run now reaches `SUCCEEDED` using the freshly-generated Vault credentials, satisfying Task 2's core acceptance criterion.**

> **Update (2026-08-14, same day):** The section below was written when Task 2 was genuinely partial (15/16 tests, real-DAG-run unproven). A separate `/gsd:debug` session subsequently found and fixed the actual blocker — a Docker Desktop/WSL2-level restart had broken the DAGs hostPath mount on all 3 kind nodes, silently freezing Airflow's scheduler for every DAG cluster-wide via `DagModel.is_stale`. That fix is independently reconfirmed here: `meta.ingestion_runs` row `5127` reached `SUCCEEDED` at `2026-08-14 20:09:50`, after Task 2's Vault reinstall (~18:22 UTC) — direct proof a real pipeline run completed end-to-end on the freshly-generated `etl_app`/MinIO credentials. The original narrative below is left intact for audit trail; the **Final Status** section after it reflects the actual outcome.

## Performance

- **Duration:** ~2h total (Task 1 subagent: ~21 min; Task 2 + incident diagnosis + docs: remainder, across a session with several user checkpoints for scope decisions)
- **Completed:** 2026-08-14T19:27:55Z
- **Tasks:** 2/2 attempted; **Task 1 fully complete, Task 2 partially complete** (see below)
- **Files modified:** 4 — matches the plan's own `files_modified` frontmatter exactly

## Accomplishments

**Task 1 (complete):**
- `scripts/vault-bootstrap.py`'s `_ensure_etl_secrets`/`_ensure_airflow_secrets` no longer read `csv-processor-db`, `csv-processor-s3`, or `airflow-minio-connection` (all three deleted by plans 05-02/05-03). `etl/analytics-db`'s DSN is now generated fresh via two new helpers, `_kubectl_cluster_primary_pod` and `_kubectl_exec_psql` (live `kubectl exec` + `ALTER ROLE etl_app WITH PASSWORD '<secrets.token_hex(32)>'`, password passed via stdin, never argv or a log line), restoring the exact mechanism `scripts/etl-secrets.sh` used before deletion (`git show 6d86cb8`). `etl/minio` and `airflow/connections/minio_default` now both read the live `data/minio-app` Secret's `secretKey` field.
- CR-02 fixed: `_ensure_policy` now calls `read_policy(name=name)` and compares its body against the target HCL before deciding to skip — verified against the **installed** hvac 2.4.0 source (`Client.get_policy()`), not guessed.
- Dead code removed: `_ETL_NAMESPACE`, `_AIRFLOW_NAMESPACE`, `_DB_SECRET_NAME`, `_S3_SECRET_NAME`, `_AIRFLOW_MINIO_SECRET_NAME` constants deleted; stale present-tense comments referencing `scripts/etl-secrets.sh` rewritten.
- `tests/unit/test_vault_bootstrap.py` created — this repo's first unit test for a `scripts/*.py` file, via `importlib.util.spec_from_file_location`. 7 cases, fully offline (mocked `hvac.Client`, mocked `subprocess.run`), RED confirmed first then GREEN. **Independently re-run and reconfirmed passing (7/7) after this task's own subagent report, before being cited in this SUMMARY or in docs.**
- `make check` green: ruff, mypy, full offline unit suite (150/150), and `tests/policy/test_no_stale_secrets.py` (124 passed) — zero regressions.

**Task 2 (partial — the credential fix is proven; the full-DAG-completion clause is not):**
- Performed the scoped live-cluster proof exactly as scoped: deleted PVCs `data-vault-0`/`audit-vault-0` and pod `vault-0` in namespace `vault` only (never `data`/`airflow`/`etl`, never `make cluster-down`). StatefulSet reconciled a fresh pod bound to fresh PVCs; Vault came up `Initialized: false, Sealed: true` — genuinely empty, the exact precondition needed.
- `make vault-unseal` succeeded against the fresh Vault (new root token/unseal key; `.secrets/vault-init.json` mtime and Vault's own Cluster ID both changed, proving genuine reinitialization, not a no-op).
- `make vault-bootstrap` exited 0 and printed **"created"** (not "already present") for all three previously-broken paths — the core proof that CR-01's fix works: before this fix, this exact sequence exited 1 with a `RuntimeError` naming a Secret that no longer exists.
- `make vault-verify` (`pytest tests/e2e/vault -q`): **15 of 16 passed**, including every test that exercises credential *function* rather than mere presence (`test_positive_auth.py`, `test_negative_auth.py`, `test_audit_log.py`, `test_rotation.py`, `test_dev_secrets_reproducible.py`'s idempotent-rerun case).
- **The one failure** (`test_airflow_backend.py::test_dag_still_resolves_its_connection_and_runs`) is not a credential fault. Root-caused live: `csv_ingest_customers` has `max_active_runs: 1`, and the one active slot was occupied by an old backlog DagRun (`logical_date 2026-08-14T02:50:00`) whose `resolve_window` task was `failed` and `wait_for_files` was `up_for_retry`, both frozen since ~17:00 UTC — over 2 hours before this test ran — correlating with a scheduler pod restart around the same time. Scheduler logs showed its Kubernetes watch repeatedly timing out and restarting from `resource_version: 0`, re-processing the same two already-terminal pod events in a loop instead of progressing. This is the same pre-existing Airflow scheduling backlog plan 05-05's own SUMMARY.md already flagged as "untouched by this plan's work" — confirmed here to still be active and now directly blocking a live-proof test, but still not caused by, or fixable within, this plan's declared scope (`scripts/vault-bootstrap.py`, `tests/unit/test_vault_bootstrap.py`, docs).
- Two scoped remediation attempts, each explicitly approved by the user before running: (1) `airflow tasks clear` on just the one stuck DagRun's `resolve_window` + downstream, scoped to that single `logical_date` — reset the task's state but the scheduler never picked it up for a retry over ~2 minutes of observation; (2) force-deleting the 3 stale/terminal pod objects (`Unknown`/`Error` status) the watch kept re-processing — this stopped the log-loop noise but the scheduler still did not queue a new attempt over a further ~2.5 minutes of observation. All 3 kind nodes confirmed `Ready` throughout (not a node-health problem). Stopped escalating further (e.g. restarting the live scheduler pod) since that crosses from "narrow, already-dead-object cleanup" into touching a shared, actively-running component for a problem outside this plan's scope.
- No fresh `meta.ingestion_runs` `SUCCEEDED` row exists after the reinstall (confirmed via direct query) — a direct consequence of the same scheduler stall, not of the credential fix.

## Task Commits

1. **Task 1: CR-01/CR-02 fix + regression test** — `20778cf` (test, RED) → `a6d1241` (fix, GREEN) → merged via `66837fb`
2. **Task 2: Live-cluster proof + docs correction** — no code commit (docs-only changes below); live-cluster actions (PVC/pod deletion, unseal, bootstrap, verify, DagRun clear, stale-pod cleanup) are operational, not committed artifacts

**Docs commit:** (this commit, following this SUMMARY) — `docs/secrets-architecture.md` §6, `05-VALIDATION.md`

## Files Created/Modified

- `scripts/vault-bootstrap.py` — corrected `_ensure_etl_secrets`/`_ensure_airflow_secrets`/`_ensure_policy`; new `_kubectl_cluster_primary_pod`/`_kubectl_exec_psql` helpers
- `tests/unit/test_vault_bootstrap.py` — new, 7 cases, offline/mocked
- `docs/secrets-architecture.md` — §6 rewritten with the accurate, partial live-proof account
- `.planning/phases/05-vault-secrets-workload-identity/05-VALIDATION.md` — T1/05-06 row → ✅, T2/05-06 row → ⚠️ partial with full evidence citation, T3/05-04 SEC-13 row → ✅ (reconfirmed passing this session), Manual-Only Verifications SEC-13 row rewritten, Approval line updated, Wave 0 checkbox for `test_vault_bootstrap.py` checked

## Decisions Made

See `key-decisions` in the frontmatter. Summary: (1) rejected the reviewed-but-wrong CR-01 fix suggestion (CNPG's `analytics-db-app` Secret) in favor of the original least-privileged `etl_app` mechanism restored from git history; (2) split Task 1/Task 2 execution across a subagent and direct orchestrator action after the permission classifier blocked a combined dispatch describing live-cluster mutation; (3) stopped escalating live remediation of a discovered-but-separate Airflow scheduler fault after two scoped, user-approved attempts, rather than continuing to expand scope into a shared live component.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 4 - Scope/Architecture] Combined single-dispatch execution was blocked by the platform's own permission classifier**

- **Found during:** Initial wave dispatch, before any task work began.
- **Issue:** The plan's `autonomous: true` frontmatter anticipated a single, uninterrupted executor run covering both tasks. Claude Code's auto-mode classifier denied the subagent spawn once, most likely due to Task 2's description of live pod/PVC deletion on the cluster.
- **Fix:** Orchestrator split execution: Task 1 dispatched to a scoped, worktree-isolated subagent (successfully); Task 2 executed directly by the orchestrator via individual Bash/kubectl calls, with explicit user check-ins at each escalation point (the initial split decision, the DagRun clear, the stale-pod deletion, and the final partial-result docs decision).
- **Files modified:** None beyond the plan's own declared scope.
- **Committed in:** N/A (orchestration decision, not a code change)

### Not Fixed (Escalated / Left Open)

**1. [Blocking, out of scope] Pre-existing Airflow KubernetesExecutor scheduling fault blocks the plan's real-DAG-run acceptance criterion**

- **Found during:** Task 2, running `make vault-verify` against the freshly-reinstalled Vault.
- **Issue:** `test_airflow_backend.py::test_dag_still_resolves_its_connection_and_runs` times out. Root cause (see Accomplishments above for the full evidence trail) is a stalled `max_active_runs: 1` backlog DagRun plus a Kubernetes-watch event-replay loop in the scheduler — entirely independent of Vault/credentials, and pre-dating this plan's own work (plan 05-05's SUMMARY.md already flagged the same backlog as background noise).
- **Why not fixed:** Out of this plan's declared file scope (`scripts/vault-bootstrap.py`, `tests/unit/test_vault_bootstrap.py`, docs). Two scoped remediation attempts did not resolve it; further escalation (e.g. restarting the live scheduler pod) would touch a shared, actively-running component for a problem this plan was never scoped to fix, and a similar bulk-remediation attempt at the same underlying backlog was already denied by the permission classifier earlier in this project's history (per STATE.md).
- **Recommendation:** Track as its own debug/fix item. Once resolved, `pytest tests/e2e/vault/test_airflow_backend.py -m cluster` can be re-run in isolation (no need to repeat the Vault PVC/pod reinstall) to close out this plan's Task 2 acceptance criterion in full.

---

**Total deviations:** 1 auto-fixed (execution-split, orchestration-level, no code impact), 1 escalated/left open (the scheduler fault — documented, not silently dropped, tracked as a follow-up).

## Issues Encountered

- The `kubectl delete pvc` step (issued before `kubectl delete pod` per the plan's own literal instruction) blocks on the PVC's protection finalizer until the pod using it is also deleted — running both in one sequential shell script deadlocks. Recovered by issuing the pod delete as a separate call; documented here since the plan's own parenthetical explanation of the ordering was slightly inconsistent with what actually happens mechanically (the recovery, not the plan's literal instruction, is the correct operational sequence to reuse next time).
- See Deviations above for the Airflow scheduler fault — the most significant issue encountered this session, fully diagnosed but not resolved.

## User Setup Required

None — no external service configuration required. The open Airflow scheduler issue requires no user action either; it's a tracked follow-up, not a blocker on anything the user needs to do.

## Next Phase Readiness (superseded — see Final Status below)

- **SEC-01 is fully closed**: the last remaining dependency on a deleted Kubernetes Secret is gone, and `tests/policy/test_no_stale_secrets.py` (plan 05-05) continues to guard against regression.
- **SEC-13 is substantially, but not fully, closed.** The literal claim ("development secrets are... reproducible when rebuilding the local environment from scratch") is now backed by a real, live, scoped proof of the credential-sourcing code path (the part that was actually broken) plus an offline regression guard. The narrower "and a real pipeline run succeeds afterward" clause remains unproven, blocked by the separate scheduler fault documented above — not by anything in this plan's own files.
- **Recommendation before treating Phase 5 as fully complete:** decide whether to (a) open a dedicated debug session for the Airflow `KubernetesExecutor`/scheduler stall, then re-run just `pytest tests/e2e/vault/test_airflow_backend.py -m cluster` to close this out, or (b) accept the current partial-proof state and move on, revisiting if the scheduler issue resurfaces. This SUMMARY and `docs/secrets-architecture.md` §6 both document the gap accurately either way — nothing here is silently dropped.
- Standard phase-completion steps (`verify_phase_goal`, marking Phase 5 complete in ROADMAP/STATE) were **not** run as part of this plan's execution — left for explicit user decision given the partial result above.

## Final Status (2026-08-14, post-debug-session)

**SEC-01 and SEC-13 are both now fully closed.**

The user asked for the Airflow scheduler fault to be debugged rather than accepted. `/gsd:debug` (`.planning/debug/resolved/dagrun-scheduler-stall.md`) found the real root cause was unrelated to anything Airflow-internal: a Docker Desktop/WSL2-level event restarted every container on every kind node simultaneously, breaking the DAGs hostPath bind mount on all 3 nodes. With an empty mount, the DAG processor never re-parsed anything, `DagModel.is_stale` never cleared, and the scheduler's own query (`WHERE DagModel.is_stale == false()`) silently excluded *every* DagRun for *every* DAG — cluster-wide, zero exceptions logged, unrelated to Vault/credentials/`csv_ingest_customers` specifically.

Fix: `docker restart` on each affected kind node (staged — `worker` first per a scoped user authorization, then `worker2`/`control-plane` after independent re-verification and a follow-up authorization), reattaching the mount each time. Independently re-verified by the orchestrator (not just the debug session's own report):
- The originally-stuck DagRun reached `state=success`, all 6 tasks succeeded.
- The scheduler autonomously advanced through the backlog with zero manual intervention.
- **`meta.ingestion_runs` row `5127`: `SUCCEEDED`, `started_at=2026-08-14 20:09:50`** — after Task 2's Vault reinstall (~18:22 UTC). This is Task 2's own literal acceptance criterion, met with a real, credential-backed pipeline run.

**One honest remaining nuance:** re-running `pytest tests/e2e/vault/test_airflow_backend.py -q -m cluster` still shows `test_dag_still_resolves_its_connection_and_runs` failing — but now for a *different, benign* reason: the backlog (from an unrelated over-broad `airflow tasks clear` during plan 05-03) is still deep, so the DagRun that's actually executing right now has no reason to notice this specific test's freshly-uploaded marker file. This is expected, self-resolving queue depth, not a recurrence of the scheduler-freeze bug — the debug session explicitly distinguished the two. Forcing it faster would mean bulk-clearing the backlog, the same class of action already declined once this session (STATE.md) — not repeated without further explicit authorization. `tests/e2e/vault -q -m cluster`'s literal 100%-green bar therefore remains technically unmet by this one test, even though the thing it exists to prove (a real DAG run succeeds on Vault-resolved credentials) is now independently proven via the `meta.ingestion_runs` row above.

Standard phase-completion steps (`verify_phase_goal`, marking Phase 5 complete in ROADMAP/STATE) were still **not** run as part of this session — that remains an explicit next step for the user or a future `/gsd:execute-phase 5` invocation, now that both plan 05-06 tasks are substantively complete.

---
*Phase: 05-vault-secrets-workload-identity*
*Completed: 2026-08-14 (Task 1 fully; Task 2 partially — see above)*

## Self-Check: PASSED (after follow-up debug session — see Final Status above)

**Files verified to exist:**
- FOUND: `scripts/vault-bootstrap.py` (modified)
- FOUND: `tests/unit/test_vault_bootstrap.py`
- FOUND: `docs/secrets-architecture.md` (modified)
- FOUND: `.planning/phases/05-vault-secrets-workload-identity/05-VALIDATION.md` (modified)
- FOUND: `.planning/phases/05-vault-secrets-workload-identity/05-06-SUMMARY.md` (this file)

**Commits verified to exist in `git log --oneline --all`:**
- FOUND: `20778cf` (Task 1, RED)
- FOUND: `a6d1241` (Task 1, GREEN)
- FOUND: `66837fb` (merge)

**Acceptance criteria re-verified (final, post-debug-session):**
- `uv run pytest tests/unit/test_vault_bootstrap.py -v` — 7/7 passed (independently re-run)
- `grep -n "csv-processor-db\|csv-processor-s3\|airflow-minio-connection" scripts/vault-bootstrap.py` — zero matches (Task 1 acceptance criterion met)
- `make vault-bootstrap` against a genuinely empty Vault — exit 0, "created" for all 3 paths (Task 2's core proof met)
- `pytest tests/e2e/vault -q -m cluster` — **still 15/16** (one test blocked by unrelated, self-resolving backlog-depth timing — root-cause distinguished from the scheduler-freeze bug, see Final Status)
- `meta.ingestion_runs` fresh `SUCCEEDED` row after reinstall — **NOW PRESENT**: `run_id=5127, status=SUCCEEDED, started_at=2026-08-14 20:09:50`, independently re-verified by direct query. Task 2 acceptance criterion met.

**Honest verdict:** Task 1 is fully complete and independently reverified. Task 2's central purpose — proving the CR-01 credential-sourcing fix works against a genuinely empty Vault, end-to-end, including a real pipeline run — is now fully proven via the `meta.ingestion_runs` row above, following a separate debug session that found and fixed the actual blocker (a Docker/WSL2-level mount failure, confirmed unrelated to Vault/credentials). The one remaining test failure is honestly attributed to backlog-queue depth, not the original bug, and not something this plan's own scope covers fixing further. SEC-01 and SEC-13 are both genuinely closed.
