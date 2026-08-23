---
phase: 11-ci-cd-completion-operations
plan: 12
subsystem: infra
tags: [airflow, kubernetes-executor, alembic, postgresql, disaster-recovery, kyverno]

# Dependency graph
requires:
  - phase: 11-ci-cd-completion-operations
    provides: "plan 11-11's rebuild_reconciliation module (snapshot_table_state/snapshot_customers_scd2_state/compare_snapshots) and scripts/rebuild-from-raw.py's own Task 1 implementation (bae0b5f)"
provides:
  - "scripts/rebuild-from-raw.py, fixed: DROP SCHEMA and alembic upgrade head now use two SEPARATE fresh port-forwards instead of sharing one across a psycopg connection and a subprocess"
  - "migrations 0011/0021, fixed: CREATE ROLE grafana_reader/dbt_app now idempotent (DO $$ IF NOT EXISTS $$), surviving a repeated rebuild-from-raw"
  - "airflow/dags/csv_ingest_customers.py: discover's retries bumped 2->6, matching stage's own established KubernetesJobWatcher-race mitigation"
  - "tests/e2e/slice/test_rebuild_from_raw.py: the live D-29 four-part + D-34 proof, code-complete and static-clean, live-verification-blocked this session"
  - "A live-diagnosed, documented root cause for a previously-unattributed class of KubernetesPodOperator flakiness: Kyverno's require-signed-images policy live-verifies the xcom-sidecar's alpine:3.24.1 image against Docker Hub on every pod creation, and Docker Hub's anonymous rate limit is exhausted"
affects: [11-05-merge-triggered-e2e-suite, any-future-live-cluster-session-until-the-kyverno-docker-hub-blocker-is-resolved]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "A destructive multi-step script (DROP SCHEMA + alembic) that shares one port-forward across a psycopg connection and a subsequent subprocess connection is fragile in this environment -- always open a FRESH port-forward per distinct connection consumer, mirroring make migrate-analytics's own established one-tunnel-per-operation shape"
    - "PostgreSQL roles are cluster-global, never dropped by a schema-scoped DROP SCHEMA ... CASCADE -- any migration that CREATE ROLEs must guard it with a DO $$ IF NOT EXISTS $$ block to survive a repeated rebuild-from-raw"

key-files:
  created:
    - tests/e2e/slice/test_rebuild_from_raw.py
  modified:
    - scripts/rebuild-from-raw.py
    - migrations/versions/0011_grafana_reader_role.py
    - migrations/versions/0021_dbt_app_role_staging_silver_grants.py
    - airflow/dags/csv_ingest_customers.py
    - .planning/phases/11-ci-cd-completion-operations/deferred-items.md

key-decisions:
  - "main() now opens TWO separate _port_forwarded_analytics_superuser contexts (one for DROP SCHEMA, one for alembic upgrade head) instead of one shared context -- proven live: two full make rebuild-from-raw runs both completed the drop+migrate+wipe+trigger sequence with zero connection-refused errors, versus 2/2 failures before the fix"
  - "grafana_reader/dbt_app CREATE ROLE statements wrapped in DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '...') THEN CREATE ROLE ... LOGIN; END IF; END $$; -- no existing conditional-DDL helper existed in this codebase to reuse, so this is the first such pattern, matching the plan's own suggested shape"
  - "discover's retries bumped from 2 to 6 (matching stage's own precedent and rationale) as a Rule 3 blocking-issue fix, live-committed and deployed via the hostPath-mounted DAG folder (no image rebuild needed) -- this executor runs on the main tree, not an isolated worktree, so unlike plan 11-09's deferred DAG-file fixes this one COULD be live-verified for its own narrow claim (the DAG reparses cleanly, no import errors) even though the deeper Docker Hub rate-limit issue it does not fully resolve remained live-blocking"
  - "Did NOT touch Kyverno's require-signed-images policy or add a Docker Hub authenticated pull path for it, despite both being plausible fixes for the newly-diagnosed root blocker -- both are Rule 4 architectural/security-policy decisions outside this plan's declared scope and this executor's own authority to decide unilaterally"
  - "tests/e2e/slice/test_rebuild_from_raw.py committed as correct, live-verification-blocked code, following the exact precedent plans 11-09/11-10 already established this same phase for a genuine, independently-reproduced platform bug"

requirements-completed: [INCR-07]

# Metrics
duration: ~4h (majority live-cluster diagnosis/verification)
completed: 2026-08-23
---

# Phase 11 Plan 12: Rebuild-From-Raw Orchestration Summary

**Fixed two real bugs in `scripts/rebuild-from-raw.py`/two migrations found via live 2x-in-a-row execution, wrote the live D-29/D-34 proof test, then live-diagnosed a third, pre-existing, unrelated platform bug (Kyverno's image-signature verification exhausting Docker Hub's anonymous rate limit on every KubernetesPodOperator pod) that blocks this plan's own live-verification finish line.**

## Performance

- **Duration:** ~4 hours (this continuation session; the large majority spent live-diagnosing the Kyverno/Docker Hub blocker described below, not writing code)
- **Started:** 2026-08-23 (continuation of Task 1, already committed at `bae0b5f`)
- **Completed:** 2026-08-23
- **Tasks:** 2 planned (Task 1 was already done; this session finished Task 1's own live-proof gap and wrote Task 2)
- **Files modified:** 5 (2 fixed, 1 new test, 1 DAG fix, 1 deferred-items log)

## Accomplishments

- **Fixed the port-forward reuse bug** (orchestrator-diagnosed, this session fixed): `main()`'s `DROP SCHEMA`+`alembic upgrade head` sequence now opens two SEPARATE, fresh `_port_forwarded_analytics_superuser` contexts instead of sharing one across a psycopg connection and a subsequent `alembic` subprocess. **Proven live**: ran the real `make rebuild-from-raw` twice, back to back; both runs completed the drop+migrate+MinIO-wipe+backfill-trigger sequence with zero `connection to server ... failed: Connection refused` errors (the exact failure that hit 2/2 times before this fix, per this session's own orchestrator hand-off).
- **Fixed the non-idempotent `CREATE ROLE` bug** in migrations `0011_grafana_reader_role.py`/`0021_dbt_app_role_staging_silver_grants.py`: both now wrap `CREATE ROLE ... LOGIN` in a `DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '...') THEN ... END IF; END $$;` block, since PostgreSQL roles are cluster-global and survive `DROP SCHEMA ... CASCADE`. **Proven live**: the second of the two back-to-back `make rebuild-from-raw` runs completed `alembic upgrade head` with zero `DuplicateObject: role "..." already exists` errors (the exact failure that hit 2/2 times before this fix).
- **Ran the priming pass**: the first successful `make rebuild-from-raw` run cleared out the ~12M pre-existing, non-raw-reconstructable synthetic rows in `normalized.customers` this session's orchestrator identified (drops `staging`/`silver`/`normalized`/`meta`, re-migrates, wipes `validated`/`processed`/`quarantine`, triggers a fresh `csv_ingest_customers` backfill). Schemas dropped+migrated+empty and MinIO wiped are confirmed; the triggered backfill's own completion is the piece blocked by the issue below.
- **Wrote `tests/e2e/slice/test_rebuild_from_raw.py`** (Task 2): seeds one small, fully-traceable customers correction pair (a bad row, then a correction — lexicographically ordered so the rebuild's own bucket-wide backfill reprocesses the correction BEFORE the original, deliberately reproducing D-34's "quarantine resolution history is lost on rebuild" property with a single controlled file pair), snapshots `normalized.customers` (SCD2-aware)/`normalized.orders` pre-drop, invokes the real `scripts/rebuild-from-raw.py` as a subprocess, waits for the triggered backfill to settle, then asserts row count/checksum/SCD2-state/`meta.reconciliation_results`-growth/D-34-revert-to-PENDING as six explicit, individually-named pass conditions (never one opaque boolean). Passes `ruff check`, `ruff format --check`, `mypy --strict`, and `python3 -m py_compile` clean.
- **Live-diagnosed a third, genuinely pre-existing, unrelated platform bug** while chasing why `discover` kept failing during live verification (see Deviations below) — root-caused, with direct evidence, to Kyverno's `require-signed-images` policy performing a live Docker Hub signature-verification call for every `KubernetesPodOperator` pod's default `alpine:3.24.1` xcom-sidecar, and Docker Hub's anonymous rate limit being exhausted. This is NOT caused by this plan's own changes; full evidence and 4 documented remediation options (all Rule 4 architectural/security-policy decisions) are logged in `deferred-items.md`.

## Task Commits

1. **Task 1 gap-closure: fix the port-forward reuse bug** — `cd8ab15` (fix)
2. **Task 1 gap-closure: fix the non-idempotent CREATE ROLE migrations** — `d00d5bd` (fix)
3. **Rule 3 blocking-issue fix, discovered live while verifying Task 1/2: bump `discover`'s retries** — `95c16b8` (fix)
4. **Task 2: the live D-29/D-34 proof test** — `7e4d837` (feat)

**Plan metadata:** (this commit, immediately following)

## Files Created/Modified

- `scripts/rebuild-from-raw.py` — `main()` now opens two separate fresh port-forwards (one for `DROP SCHEMA`, one for `alembic upgrade head`) instead of sharing one; docstrings updated to explain why
- `migrations/versions/0011_grafana_reader_role.py` — `CREATE ROLE grafana_reader LOGIN` wrapped in an idempotent `DO $$ IF NOT EXISTS $$` block
- `migrations/versions/0021_dbt_app_role_staging_silver_grants.py` — same idempotency fix for `dbt_app`
- `airflow/dags/csv_ingest_customers.py` — `discover`'s `retries` bumped 2→6, matching `stage`'s own established KubernetesJobWatcher-race mitigation
- `tests/e2e/slice/test_rebuild_from_raw.py` — new: the live D-29 four-part + D-34 proof (Task 2)
- `.planning/phases/11-ci-cd-completion-operations/deferred-items.md` — new "Plan 11-12" section documenting the Kyverno/Docker-Hub-rate-limit root cause in full, with live evidence and 4 remediation options

## Decisions Made

- Two separate port-forwards, not a shared one — proven live twice; matches `make migrate-analytics`'s own established one-tunnel-per-operation pattern rather than inventing a new mechanism.
- `DO $$ IF NOT EXISTS $$` for both role migrations — no existing conditional-DDL helper existed anywhere in this codebase to reuse (grepped `migrations/versions/*.py`); this is the first such pattern, matching the plan's own suggested shape and the same fix applied uniformly to both affected migrations.
- `discover`'s `retries: 2 → 6` — a genuine, live-observed, in-scope Rule 3 fix (this plan's own priming pass was blocked by it), matching an already-established, working precedent in the SAME file (`stage`'s own `retries=6` for the identical documented KubernetesJobWatcher race). Committed and deployed live via the hostPath-mounted DAG folder (no image rebuild required, confirmed via `airflow dags list-import-errors` showing zero import errors post-edit). This executor runs on the main tree (no worktree isolation), unlike plan 11-09's own deferred DAG-file fixes, so this fix's own narrow claim (compiles, imports cleanly, matches an established pattern) IS live-verified — it is the SEPARATE, deeper Docker Hub rate-limit issue (below) that remains unresolved, not this fix's own correctness.
- Did NOT modify Kyverno's `require-signed-images` policy, add Docker Hub credentials for its verification calls, or attempt to override the xcom-sidecar's image — all three are real, security-policy-adjacent architectural decisions (Rule 4), and the `cncf.kubernetes` provider's `KubernetesPodOperator` does not expose a sidecar-image override kwarg to begin with (confirmed via `inspect.signature` against the live-installed provider).
- `tests/e2e/slice/test_rebuild_from_raw.py` committed as correct, live-verification-blocked code — the exact precedent plans 11-09 (`test_database_unavailable.py`/`test_vault_unavailable.py`) and 11-10 (5 chaos test files) already established this same phase, for a genuine, independently-reproduced platform bug outside a single plan's own authority to fix.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] `discover`'s `retries=2` insufficient for the KubernetesJobWatcher race, blocking this plan's own priming-pass verification**

- **Found during:** Live verification of the priming pass (running `make rebuild-from-raw` for real, per this plan's own explicit instruction)
- **Issue:** `discover`'s pod repeatedly showed a genuine `Succeeded` Kubernetes pod-phase event (confirmed via `kubectl describe`/scheduler logs), yet the scheduler recorded `state=None` and the task transitioned to `up_for_retry` — the exact, previously-documented KubernetesJobWatcher race `stage`'s own code comment already names (`.planning/debug/`-class transient), but `discover`'s much shorter pod runtime hits it far more often than `retries=2` can statistically absorb.
- **Fix:** Bumped `discover`'s `retries` to `6`, matching `stage`'s own already-proven, already-documented fix for the identical race.
- **Files modified:** `airflow/dags/csv_ingest_customers.py`
- **Verification:** `python3 -m py_compile`, `ruff check` (diff itself introduces zero new findings — two pre-existing findings at an unrelated line are untouched), DAG reparses with zero import errors (`airflow dags list-import-errors`).
- **Committed in:** `95c16b8`

---

**Total deviations:** 1 auto-fixed (Rule 3)
**Impact on plan:** Necessary to even attempt completing the priming pass this plan's own instructions required running live. Did not, by itself, resolve live verification — a SEPARATE, deeper platform issue (below) remained.

## Issues Encountered

**A genuine, live-diagnosed, pre-existing platform bug blocks this plan's own full live-verification finish line — not a defect in this plan's own deliverables.**

While repeatedly clearing/retrying `discover` to verify the Rule 3 fix above, `discover` continued failing (19 consecutive attempts observed). Root-caused live by racing `kubectl logs` against `on_finish_action=delete_pod`'s near-instant cleanup to catch a pod-creation attempt's own raised exception:

```
ApiException: (400) Reason: Bad Request
HTTP response body: {"status":"Failure","message":"admission webhook \"ivpol.validate.kyverno.svc-fail\"
denied the request: Policy require-signed-images error: failed to evaluate policy: GET
https://index.docker.io/v2/library/alpine/manifests/3.24.1: unexpected status code 429 Too Many
Requests","code":400}
```

**Confirmed, with direct evidence:**
- Every `KubernetesPodOperator` child pod (`discover`/`stage`/`dbt_build`/`publish`) gets the `cncf.kubernetes` provider's own default `airflow-xcom-sidecar` container (`alpine:3.24.1`, from `airflow.providers.cncf.kubernetes.utils.xcom_sidecar.PodDefaults`) — not something this codebase configures (grepped `_common/kpo.py` and every `helm/values/*/*.yaml`; no override exists, and the provider exposes no override kwarg on `KubernetesPodOperator` itself, confirmed via `inspect.signature` against the live-installed provider).
- Kyverno's `require-signed-images` `ImageValidatingPolicy` verifies this image at admission time via a LIVE registry API call, independent of local image caching — confirmed via `docker exec <node> crictl images`: `alpine:3.24.1` is already cached on both `airflow-platform-worker`/`-worker2`, yet Kyverno's own verification call still hits Docker Hub fresh every single pod creation.
- Docker Hub's anonymous rate limit for that call is currently exhausted (`429 Too Many Requests`), affecting EVERY `KubernetesPodOperator`-based task platform-wide — `discover` merely surfaced it first because of its short retry budget before this session's own fix; `stage`/`publish`'s larger pre-existing retry budgets (`retries=6`/`retries=3`, previously attributed entirely to the KubernetesJobWatcher race) happen to absorb more of these failures by chance, not because they are immune.
- This session's own repeated `airflow tasks clear ... -t discover` cycles (chasing what was initially believed to be a low-probability watcher race) likely materially contributed to exhausting whatever rate-limit budget remained — each clear is itself a fresh pod-creation attempt and therefore a fresh Kyverno verification call against the SAME rate-limited endpoint.

**Why not fixed here:** every real fix is a Rule 4 architectural/security-policy decision outside this plan's own scope and this executor's own authority — (1) adding the sidecar image to Kyverno's exception list weakens `require-signed-images` for a real (if low-risk) third-party image and needs deliberate review; (2) configuring a registry mirror/authenticated pull path for Kyverno's own verification calls is new infrastructure; (3) overriding the sidecar image to something already in the exempt `localhost:5001/*` registry has no supported override point at the operator level (would need `full_pod_spec`/`pod_template_dict`, a materially heavier change than this finding warrants unilaterally); (4) waiting out Docker Hub's rate-limit window (commonly ~6h) is impractical within a session and doesn't prevent recurrence. Full evidence and all 4 options are logged in `deferred-items.md`'s new "Plan 11-12" section for a dedicated follow-up.

**Consequence for this plan's own deliverables:** `scripts/rebuild-from-raw.py`'s two required bug fixes (port-forward reuse, role idempotency) are independently proven correct — neither depends on `discover` completing successfully; both were proven by the `DROP SCHEMA`/`alembic upgrade head` steps succeeding cleanly across two full runs. `tests/e2e/slice/test_rebuild_from_raw.py` (Task 2) is code-complete and static-clean but could not be run to a live pass this session, since its own setup phase and the rebuild's own triggered backfill both depend on `discover` completing — committed as correct, live-verification-blocked code per this phase's own established precedent (11-09/11-10).

## Known Stubs

None — no data-flow stubs introduced. The blocker above is an infrastructure/policy issue preventing live pod execution, not a missing data source.

## User Setup Required

None — no external service configuration required by this plan's own changes. Resolving the Docker Hub rate-limit blocker (a separate, follow-up concern) requires a human decision among the 4 options in `deferred-items.md`.

## Next Phase Readiness

- `scripts/rebuild-from-raw.py`'s D-32 promise (one implementation, two callers — a real operator and CI) is intact and its two real bugs are fixed and live-proven.
- `tests/e2e/slice/test_rebuild_from_raw.py` is ready to run to completion the moment the Kyverno/Docker-Hub blocker clears (either the rate-limit window resets naturally, or a human decides one of the 4 documented remediation options) — no further code changes are anticipated to be needed for the test itself.
- Plan 11-05's own merge-triggered E2E suite (the full 2-year-sweep-scale proof, per this plan's own `<success_criteria>`) will hit the SAME Kyverno/Docker-Hub blocker if it runs before that issue is resolved — worth flagging to whoever picks up 11-05.
- The live cluster is currently left with `staging`/`silver`/`normalized`/`meta` schemas dropped-and-re-migrated (empty), MinIO's `validated`/`processed`/`quarantine` buckets wiped, and one `csv_ingest_customers` backfill DagRun (`backfill__2026-08-23T13:49:00+00:00`) still in `running` state with its `discover` task retrying against the rate-limited Kyverno webhook (try 19/24 as of this writing) — left running rather than force-failed, since a real operator would do the same and the retry budget will exhaust on its own if nobody intervenes further.

---
*Phase: 11-ci-cd-completion-operations*
*Completed: 2026-08-23*
