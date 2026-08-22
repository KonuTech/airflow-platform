---
phase: 11-ci-cd-completion-operations
plan: 13
subsystem: docs
tags: [runbooks, operations, obs-06, vault, airflow, backfill, scd, incident-response]

# Dependency graph
requires:
  - phase: 05-vault-secrets-workload-identity
    provides: tests/e2e/vault/test_rotation.py, tests/e2e/vault/test_negative_auth.py, docs/secrets-architecture.md
  - phase: 08-validation-quarantine-metadata-control-plane-completion
    provides: resolve_rejected_records_for_business_keys, meta.rejected_records.business_key (migration 0020)
  - phase: 09-etl-correctness-dedup-incremental-backfill-recovery
    provides: meta.v_run_recovery (migration 0033), SchemaRepository.resolve_by_hash
  - phase: 10-slowly-changing-dimensions
    provides: SCDPublisher, recompute_version_chain
provides:
  - 15 of 18 README §89 operational runbooks under docs/runbooks/
  - tests/policy/test_runbooks_structure.py structural policy guard
affects: [11-14 (remaining 3 chaos-trailing runbooks + the 18-file count assertion)]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Runbooks are one markdown file per README §89 scenario under docs/runbooks/, never a single consolidated file, each with exactly 5 `##` headings in fixed order (Symptoms/Diagnosis/Recovery/Reprocessing/Verification)"
    - "Real-incident runbooks cite their exact .planning/debug/resolved/*.md source by path and are written from that incident's own root_cause/fix/verification fields, never from memory"
    - "Existing-feature runbooks cross-reference established docs (docs/secrets-architecture.md) and exact test files rather than duplicating mechanism descriptions, avoiding a second driftable copy"

key-files:
  created:
    - docs/runbooks/airflow-unavailable.md
    - docs/runbooks/vault-unavailable.md
    - docs/runbooks/kubernetes-pod-stuck.md
    - docs/runbooks/failed-backfill.md
    - docs/runbooks/task-repeatedly-failing.md
    - docs/runbooks/csv-malformed.md
    - docs/runbooks/schema-changed.md
    - docs/runbooks/duplicate-batch.md
    - docs/runbooks/late-arriving-data.md
    - docs/runbooks/cdc-failure.md
    - docs/runbooks/scd-correction.md
    - docs/runbooks/corrupted-file.md
    - docs/runbooks/partial-database-load.md
    - docs/runbooks/secret-rotation.md
    - docs/runbooks/unauthorized-access.md
    - tests/policy/test_runbooks_structure.py
  modified: []

key-decisions:
  - "task-repeatedly-failing.md pivots to ORCH-04's documented retry/failure semantics as its primary framing (stated explicitly in the file) rather than a third reuse of airflow-scheduler-stuck-tasks.md, which already sources kubernetes-pod-stuck.md"
  - "partial-database-load.md documents meta.v_run_recovery as already-built and notes REQUIREMENTS.md's LOAD-06 'Pending' status is stale documentation predating Phase 9's actual delivery, without editing REQUIREMENTS.md (out of scope per the plan)"
  - "cdc-failure.md is an honest out-of-v1-scope stub citing REQUIREMENTS.md's Out of Scope table and naming the Source/Publisher seam (ADR-0008), with every section stating 'Not applicable in v1' rather than fabricating a CDC failure mode"
  - "tests/policy/test_runbooks_structure.py deliberately does not assert docs/runbooks/*.md count == 18 -- that assertion is deferred to plan 11-14, which completes the set after the chaos tests it trails"

requirements-completed: [OBS-06]

# Metrics
duration: ~30min
completed: 2026-08-22
---

# Phase 11 Plan 13: Operational Runbooks (15 of 18) Summary

**15 of README §89's 18 operational runbooks, each with symptoms/diagnosis/recovery/reprocessing/verification sections — 5 written from this project's own real, already-diagnosed incidents, 9 documenting already-built features as operator how-tos, and 1 honest CDC out-of-scope stub — plus a structural policy test proving every file's shape.**

## Performance

- **Duration:** ~30 min
- **Completed:** 2026-08-22T17:41:35Z
- **Tasks:** 3
- **Files modified:** 16 (15 new runbooks + 1 new test file)

## Accomplishments

- 5 real-incident runbooks, each citing its exact `.planning/debug/resolved/*.md` source and
  written from that incident's own `root_cause`/`fix`/`verification` fields: `airflow-unavailable.md`
  (DAGs hostPath mount / `DagModel.is_stale`), `vault-unavailable.md` (Vault reseal, no auto-unseal),
  `kubernetes-pod-stuck.md` (node CPU exhaustion + orphaned xcom sidecar), `failed-backfill.md`
  (Airflow's own row-lock race + VALID-08's business-key redrive mechanism, also citing INCR-06's
  historical schema resolution), and `task-repeatedly-failing.md` (pivots to ORCH-04's documented
  retry semantics, with the pivot choice stated explicitly in the file).
- 10 existing-feature runbooks documenting already-built platform capability as operator how-tos
  (not incident reconstructions): CSV structural validation, schema drift classification, the
  batch ledger's content-addressed idempotency, late-arriving-data routing by event time, SCD2
  correction via full-history recompute, pre-pod-launch file integrity, `meta.v_run_recovery`,
  and Vault secret rotation/unauthorized-access — the last two deliberately cross-reference
  `docs/secrets-architecture.md` and their exact Phase-5 test files rather than duplicating the
  mechanism.
- 1 honest CDC stub (`cdc-failure.md`) that states plainly, in every section, that CDC is out of
  v1 scope, citing `REQUIREMENTS.md`'s Out of Scope table and naming the `Source`/`Publisher` seam
  (ADR-0008) that would carry it later — never omitted, never fabricated.
- `tests/policy/test_runbooks_structure.py` — a reusable `missing_headings(text) -> list[str]`
  checker (mirroring `test_supply_chain_guards.py`'s style) proving all 15 files carry exactly the
  5 required `##` headings in order, plus a non-vacuity test proving the checker itself catches a
  missing heading. Built via a genuine RED/GREEN TDD cycle: the checker was undefined first
  (confirmed `NameError` on both tests), then implemented (confirmed both pass, clean under ruff
  and mypy).

## Task Commits

Each task was committed atomically:

1. **Task 1: 5 real-incident runbooks** - `5d53ca2` (docs)
2. **Task 2: 10 existing-feature + stub runbooks** - `effc70c` (docs)
3. **Task 3: Structural policy test (TDD)** - `f12f002` (test, RED) → `808c174` (feat, GREEN)

**Plan metadata:** commit to follow (docs: complete plan)

## Files Created/Modified

- `docs/runbooks/airflow-unavailable.md` - DAGs hostPath mount / cluster-wide scheduler stall
- `docs/runbooks/vault-unavailable.md` - Vault reseal after restart, no auto-unseal by design
- `docs/runbooks/kubernetes-pod-stuck.md` - node CPU exhaustion + orphaned xcom-sidecar pods
- `docs/runbooks/failed-backfill.md` - Airflow row-lock race + business-key redrive scoping
- `docs/runbooks/task-repeatedly-failing.md` - ORCH-04 retry semantics, cross-referencing the two runbooks above
- `docs/runbooks/csv-malformed.md` - structural validation, `meta.rejected_records`, VALID-08 redrive
- `docs/runbooks/schema-changed.md` - `classify_schema_change`, `meta.schema_versions`
- `docs/runbooks/duplicate-batch.md` - `meta.batches` content-addressed `batch_key`
- `docs/runbooks/late-arriving-data.md` - dbt silver `existing_silver_contenders` event-time routing
- `docs/runbooks/cdc-failure.md` - honest out-of-v1-scope stub, `Source`/`Publisher` seam pointer
- `docs/runbooks/scd-correction.md` - `SCDPublisher` full-history recompute from `staging.customers`
- `docs/runbooks/corrupted-file.md` - `integrity_gate`, pre-pod-launch LOAD-10/11 checks
- `docs/runbooks/partial-database-load.md` - `meta.v_run_recovery`, retry-only recovery
- `docs/runbooks/secret-rotation.md` - cross-references `docs/secrets-architecture.md` §4, `test_rotation.py`
- `docs/runbooks/unauthorized-access.md` - cross-references `docs/secrets-architecture.md` §2, `test_negative_auth.py`
- `tests/policy/test_runbooks_structure.py` - structural proof for all 15 files' heading shape

## Decisions Made

- **`task-repeatedly-failing.md`'s source choice:** pivoted to ORCH-04's documented retry/failure
  semantics rather than a third reuse of `airflow-scheduler-stuck-tasks.md` (already used once for
  `kubernetes-pod-stuck.md`). Stated explicitly in the file itself, per the plan's own requirement
  to record which option was chosen.
- **`partial-database-load.md`'s stale-status note:** `REQUIREMENTS.md`'s traceability table still
  marks `LOAD-06` "Pending" even though `meta.v_run_recovery` (migration 0033, plan 09-06) already
  implements it — documented as stale documentation predating actual delivery, per the plan's
  explicit instruction not to edit `REQUIREMENTS.md` in this plan.
- **`cdc-failure.md`'s stub shape:** every one of the 5 required headings states "Not applicable in
  v1" plus a pointer, rather than omitting the file or fabricating CDC-specific content — matches
  the plan's explicit instruction and D-41's "never silently omitted" requirement.
- **Task 3's TDD split:** since this task has no separate `<implementation>` block and its only
  file is the test module itself, RED and GREEN were split as two literal commits around the same
  file: RED committed the two test functions calling an undefined `missing_headings` (confirmed
  failing with `NameError`), GREEN then added the real implementation (confirmed both tests pass).

## Deviations from Plan

None - plan executed exactly as written. All file paths, heading structure, and citation
requirements from the plan's `<tasks>` and `<interfaces>` sections were followed as specified.

## Issues Encountered

One ruff line-length violation (E501, 101 > 100 chars) in the GREEN-phase test assertion message,
found and fixed before committing GREEN — reformatted the f-string across two lines. Re-verified
clean under both `ruff check` and `mypy` before the GREEN commit.

## Known Stubs

- **`docs/runbooks/cdc-failure.md`** — every section (`## Symptoms` through `## Verification`)
  states "Not applicable in v1." This is the plan's own deliberate, explicit instruction (Task 2's
  `<action>`: "write it as a short, honest stub... rather than fabricating content for a feature
  that does not exist"), not an unintentional gap. It is fully resolved by design — CDC is
  out of v1 scope per `REQUIREMENTS.md`'s Out of Scope table (DoD 44/45/46/87), and the file names
  the exact seam (`docs/adr/0008-pipeline-composition-seam.md`) that would carry a CDC `Source`
  later without redesign. No future plan is expected to "fix" this file's content unless CDC itself
  re-enters scope.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- 15 of 18 README §89 operational runbooks are complete and structurally proven. The remaining 3
  (MinIO unavailable, PostgreSQL unavailable, Secret unavailable) are deliberately deferred to plan
  11-14, which trails plan 11-09's chaos tests by design (D-41) — `tests/policy/test_runbooks_
  structure.py`'s own module docstring documents this so a future reader isn't confused by an
  apparently incomplete count.
- Plan 11-14 should extend `THIS_PLAN_RUNBOOKS` (or replace it with a full `docs/runbooks/*.md`
  glob) once it adds the final 3 files, and is the correct place to add the `len(...) == 18`
  completeness assertion this plan deliberately withheld.
- No blockers for any other Phase 11 plan — this plan's files have zero cross-file coupling with
  any other in-flight Wave 1 plan (each runbook is a standalone file; the one shared touchpoint,
  `docs/secrets-architecture.md`, was only read, never modified).

---
*Phase: 11-ci-cd-completion-operations*
*Completed: 2026-08-22*

## Self-Check: PASSED

- All 17 claimed files (`docs/runbooks/*.md` x15, `tests/policy/test_runbooks_structure.py`,
  this `SUMMARY.md`) confirmed present via `ls`.
- All 5 commit hashes (`5d53ca2`, `effc70c`, `f12f002`, `808c174`, `3afb20e`) confirmed present
  via `git log --oneline --all`.
