---
phase: 09-etl-correctness-dedup-incremental-backfill-recovery
verified: 2026-08-21T08:43:45Z
status: gaps_found
score: 5/5 roadmap success criteria substantively verified; 2 repo-health gaps found in non-cluster test suite
overrides_applied: 0
gaps:
  - truth: "The non-cluster verification suite (unit + integration) is green, confirming the codebase is in a genuinely working state right now"
    status: failed
    reason: "tests/integration/test_migrations.py has two deterministic, reproducible failures: EXPECTED_TABLES and the grafana_reader expected-grants set were never updated for migrations 0033 (meta.v_run_recovery) and 0034 (meta.processing_gaps), added by plans 09-06 and 09-10. The migrations themselves are correct (the live/actual grant and table sets are a superset containing the new objects) — only the test's hardcoded expectation is stale."
    artifacts:
      - path: "tests/integration/test_migrations.py"
        issue: "EXPECTED_TABLES set (line ~107) missing ('meta','processing_gaps'); expected_objects set in test_grafana_reader_role_exists_and_is_select_only (line ~369) missing ('meta','processing_gaps') and ('meta','v_run_recovery')"
    missing:
      - "Add meta.processing_gaps to EXPECTED_TABLES"
      - "Add meta.processing_gaps and meta.v_run_recovery to the grafana_reader expected_objects set"
  - truth: "The non-cluster verification suite (unit + integration) is green, confirming the codebase is in a genuinely working state right now"
    status: failed
    reason: "tests/integration/test_staging_durability.py has two deterministic, reproducible failures: both tests construct a RunContext with metadata=None and a stale comment '# type: ignore[arg-type] -- unused by StagingLoader'. Plan 09-07 (commit efa6001) added a ctx.metadata.get_or_create_dataset(...)/record_reconciliation(...) call inside promote_to_durable_bronze, making metadata a hard dependency. The test file was never updated to supply a real metadata repository, so both tests now crash with AttributeError: 'NoneType' object has no attribute 'get_or_create_dataset'."
    artifacts:
      - path: "tests/integration/test_staging_durability.py"
        issue: "_make_context (or equivalent) passes metadata=None; comment claiming it's unused by StagingLoader is now false as of plan 09-07's staging.py change"
    missing:
      - "Wire a real PostgresMetadataRepository (or equivalent testcontainers-backed fixture) into test_staging_durability.py's context builder so promote_to_durable_bronze's raw_bronze reconciliation write has a real metadata sink, matching the pattern test_batch_complete_control_totals.py and test_reconciliation.py already use"
deferred: []
human_verification: []
---

# Phase 9: ETL Correctness — Dedup, Incremental, Backfill & Recovery Verification Report

**Phase Goal:** The platform processes only what is new, never loses late data, recovers from partial failure without reading logs, and can prove target matches source
**Verified:** 2026-08-21T08:43:45Z
**Status:** gaps_found (repo-hygiene gaps in pre-existing tests; the 5 ROADMAP success criteria themselves are substantively verified in code, and independently proven live per 09-11-SUMMARY.md, whose live-cluster account this verification trusts per its explicit instructions)
**Re-verification:** No — initial verification

**Note on phase Mode:** ROADMAP.md tags this phase `Mode: mvp`, but its goal text ("The platform processes only what is new...") is a capability statement, not a `As a X, I want Y, so that Z.` user story (confirmed via `gsd-sdk query user-story.validate` → `valid: false`). Standard goal-backward verification against the ROADMAP's 5 explicit Success Criteria was used instead of MVP-mode user-flow-coverage verification, since there is no user-story shape to narrow against. This is a pre-existing goal-authoring inconsistency, not something introduced by this phase's plans — flagged for awareness, not scored as a gap.

## Goal Achievement

### Observable Truths (ROADMAP Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Duplicates (within file/across files/across batches) collapse to one row per business key, with `meta.dedup_audit` explaining every removal | ✓ VERIFIED | Dedup mechanism itself is Phase 08.1-owned (DEDUP-01..04 explicitly remapped there per ROADMAP's Requirements line); Phase 9 adds the bronze→silver reconciliation hop that proves fidelity around it: `dbt/macros/reconciliation_post_hook.sql` (167 lines) wired into both `silver_customers.sql`/`silver_orders.sql` alongside the pre-existing `dedup_audit_post_hook.sql`. `tests/integration/test_dbt_dedup_audit.py` passes in isolation. |
| 2 | Watermark advances only after publish commits, from committed cursor values, never regresses; a mid-flight kill leaves it unchanged | ✓ VERIFIED (with a real bug found and fixed) | `record_watermark` in `packages/dataplat/src/dataplat/metadata/postgres.py:784-842` — `MAX(watermark_column) FROM source_table WHERE _run_id = ANY(%(run_ids)s)`, `GREATEST()` on conflict, unconditional `meta.watermark_history` append. The `run_ids` scoping was a genuine correctness bug found and fixed live during 09-11 (previously an unscoped `MAX()` over the whole cumulative table could be poisoned by any stray row from any run) — confirmed by direct code read, not just SUMMARY narrative. Call site in `pipeline/run.py:1084-1092` passes the full `staged_run_ids` list. `tests/integration/test_watermarks.py` (3/3) passes in isolation. |
| 3 | A 2-year backfill runs the same discover→validate→normalize→dedupe→load→lineage path, no bypass, resolves historical schema versions, handles a missing file explicitly, is idempotent on re-run | ✓ VERIFIED | `tests/e2e/slice/test_backfill_2year_sweep.py` (1135 lines, `pytest.mark.cluster`) is a real, non-stub test: `test_full_2year_sweep_customers_and_orders` asserts distinct `schema_version_id` before/after the corpus's schema-change boundary, asserts the gap file produces zero rows without corrupting the rest of the window, and `test_idempotent_rerun_produces_zero_additional_rows` asserts zero new rows on `--reprocess-behavior completed`. Uses the native `airflow backfill create` CLI per D-11 (no bypass). Trusted per task instructions as live-passed (09-11-SUMMARY.md's detailed, specific account — including two independently found/fixed bugs — reads as genuine engineering narrative, not a pass-marker claim). |
| 4 | A 3-months-late record lands in its correct historical partition; out-of-order records produce correct final state | ✓ VERIFIED | Test asserts the late row's own `event_ts` (backdated) is preserved in the gold table, never silently corrected to the file's nominal day, and asserts `meta.watermarks.cursor_value` equals the corpus's true max event_ts — never the late event's earlier value (`test_backfill_2year_sweep.py` lines ~147-200). |
| 5 | One query reports success/remaining/retry-vs-rollback after an interrupted load; reconciliation reports counts/sums/checksums/min-max/key-counts, flagging a corrupted control total | ✓ VERIFIED | `meta.v_run_recovery` (migration `0033_meta_v_run_recovery.py`) is a real 3-way LEFT JOIN over `STAGE_LOAD`/`DBT_BUILD`/`PUBLISH`, `next_action` column literally never emits "rollback" (`'retry stage <NAME>'` / `'complete'` / `'in progress'` only). Grafana alert `alert-run-recovery-exhausted` in both `helm/values/local/monitoring.yaml` and `helm/values/ci/monitoring.yaml` queries this view. Control-total discrepancy flagging is genuinely tested: `tests/integration/test_batch_complete_control_totals.py::test_marker_wrong_row_count_records_discrepancy_and_run_still_reaches_staged` deliberately sets a wrong `expected_row_count` and asserts `control_total_discrepancy == wrong_delta`, run still reaches STAGED (flag-don't-block). 9/9 tests in this file + `test_run_recovery_view.py` pass live in this verification session. |

**Score:** 5/5 ROADMAP success criteria substantively verified against the codebase.

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `packages/dataplat/src/dataplat/metadata/repository.py::record_watermark` | Protocol with `run_ids: Sequence[int]` param | ✓ VERIFIED | Confirmed present, lines 895-905 |
| `packages/dataplat/src/dataplat/metadata/postgres.py::record_watermark` | `MAX()` scoped by `WHERE _run_id = ANY(%(run_ids)s)`, `GREATEST()` merge, unconditional history append | ✓ VERIFIED | Confirmed present, lines 784-842 |
| `packages/dataplat/src/dataplat/pipeline/run.py` call site | Passes full `staged_run_ids`, not just max | ✓ VERIFIED | Line 1092: `run_ids=staged_run_ids` |
| `tests/e2e/slice/test_backfill_2year_sweep.py` | Live capstone: sizing, pilot, full sweep, idempotency, concurrency | ✓ VERIFIED | 1135 lines, 6 test functions, `pytest.mark.cluster`, substantive assertions confirmed (not a stub) |
| Migrations 0025-0034 | watermarks, reconciliation_results, v_run_recovery, processing_gaps, dedup_audit (0024), run_stages (0025) | ✓ VERIFIED | All 10 files present in `migrations/versions/`, each contains real `op.create_table`/`CREATE VIEW` DDL |
| `dbt/macros/reconciliation_post_hook.sql` | bronze→silver reconciliation hop | ✓ VERIFIED | 167 lines, wired into both `silver_customers.sql` and `silver_orders.sql` |
| `airflow/dags/_common/run_stage_recorder.py` | DBT_BUILD stage tracking, ADR-0004 exception | ✓ VERIFIED | `wire_dbt_build_tracking()` builds `stage >> mark_running >> dbt_build >> status >> mark_done >> publish`; called from both DAGs |
| `airflow/dags/_common/gap_recorder.py` | `record_processing_gap_if_empty`, D-06 | ✓ VERIFIED | Wired into both `csv_ingest_customers.py`/`csv_ingest_orders.py` |
| Grafana alert `alert-run-recovery-exhausted` | Queries `meta.v_run_recovery` | ✓ VERIFIED | Present in both `helm/values/local/monitoring.yaml` and `helm/values/ci/monitoring.yaml` |

### Key Link Verification

| From | To | Via | Status |
|------|-----|-----|--------|
| `pipeline/run.py::publish_ingest` | `metadata/postgres.py::record_watermark` | `ctx.metadata.record_watermark(..., run_ids=staged_run_ids)` | ✓ WIRED |
| `load/staging.py::promote_to_durable_bronze` | `metadata/postgres.py::record_reconciliation` | raw_bronze hop, same transaction | ✓ WIRED (confirmed live via `test_batch_complete_control_totals.py`, but see Gap 2 — a *different* legacy test that calls the same method directly is now broken by this dependency) |
| `silver_customers.sql`/`silver_orders.sql` | `reconciliation_post_hook.sql` | post_hook_sql captured block | ✓ WIRED |
| `csv_ingest_customers.py`/`csv_ingest_orders.py` | `run_stage_recorder.py::wire_dbt_build_tracking` | task graph insertion around `dbt_build` KPO | ✓ WIRED |
| `helm/values/*/monitoring.yaml` alert rule | `meta.v_run_recovery` | SQL query in alert rule definition | ✓ WIRED |

### Requirements Coverage

Phase 9's active requirement set (per ROADMAP.md's Requirements line, DEDUP-01..04/INCR-03/INCR-04/QUAL-10 explicitly remapped to Phase 08.1): INCR-01, INCR-02, INCR-05, INCR-06, LOAD-06, VALID-05, VALID-06, QUAL-11.

| Requirement | Claimed by | Status | Evidence |
|-------------|-----------|--------|----------|
| INCR-01 | 09-02 | ✓ SATISFIED | Watermark advance mechanism, `meta.watermarks` |
| INCR-02 | 09-02 | ✓ SATISFIED | `GREATEST()`, never bare `>`, committed-cursor-only |
| INCR-05 | 09-11 | ✓ SATISFIED | Native `airflow backfill create`, same pipeline, no bypass |
| INCR-06 | 09-05, 09-10, 09-11 | ✓ SATISFIED | 2-year corpus generator, `meta.processing_gaps`, live sweep |
| LOAD-06 | 09-04, 09-06, 09-09 | ✓ SATISFIED | `run_stage_recorder.py`, `meta.v_run_recovery`, DAG wiring, Grafana alert |
| VALID-05 | 09-01, 09-02, 09-07, 09-08 | ✓ SATISFIED | 3-hop reconciliation (raw_bronze, bronze_silver, silver_gold), `meta.reconciliation_results` |
| VALID-06 | 09-03, 09-07 | ✓ SATISFIED | `_BATCH_COMPLETE` manifest parsing, control-total comparison and discrepancy recording |
| QUAL-11 | 09-05, 09-11 | ✓ SATISFIED | Idempotent-rerun test, historical schema resolution test |

**Note:** `.planning/REQUIREMENTS.md`'s own checkbox list and Traceability table still show VALID-06, LOAD-06, INCR-01, INCR-02, INCR-05, INCR-06 and QUAL-11 as `[ ] Pending` / `Phase 9 | Pending`, in contrast to Phase 8's items which are all marked `[x] Complete`. This is a documentation-sync gap (REQUIREMENTS.md was not updated when the phase closed) — not a code gap, since the implementation evidence above is independently confirmed in the codebase. Flagged for the phase-closure step to fix; not counted as a `gaps:` item since it does not reflect an unbuilt capability.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `tests/integration/test_migrations.py` | ~107, ~369 | Stale hardcoded expected-schema/expected-grants sets not updated for migrations 0033/0034 | ⚠️ Warning | 2 reproducible test failures; migrations themselves are correct, only the test assertions are stale |
| `tests/integration/test_staging_durability.py` | `metadata=None` construction + stale "unused by StagingLoader" comment | Legacy test fixture not updated after plan 09-07 made `ctx.metadata` a hard dependency of `promote_to_durable_bronze` | ⚠️ Warning | 2 reproducible `AttributeError` crashes; production code path is correct (real DAGs always construct a real metadata repository), this is test-only breakage |

No debt markers (`TBD`/`FIXME`/`XXX`) found in the phase's modified files. No placeholder/stub implementations found in any of the artifacts inspected.

### Non-Cluster Verification Run (this session, independently)

- `uv run --group dev pytest tests/unit -q` → **511 passed** (matches SUMMARY's claim, independently reproduced)
- `uv run --group dev pytest tests/integration -q` (full suite, one session) → **163 passed, 9 failed**
- Isolating each failing file individually: `test_config_registry.py` (6/6 pass alone), `test_dbt_dedup_audit.py` (1/1 pass alone), `test_reconciliation.py` (6/6 pass alone), `test_watermarks.py` (3/3 pass alone) — these 5 failures do **not** reproduce in isolation, indicating cross-file test-order/shared-session-DB pollution in the full-suite run (the `tests/integration/conftest.py` fixtures are explicitly session-scoped), not a code defect.
- `test_migrations.py` (2/2 fail, every time, in isolation) and `test_staging_durability.py` (2/2 fail, every time, in isolation) — **genuinely reproducible, order-independent failures**, root-caused above and listed in `gaps:`.
- `test_batch_complete_control_totals.py` + `test_run_recovery_view.py` → 9/9 pass
- Live-cluster (`-m cluster`) tests were **not** re-run per task instructions; 09-11-SUMMARY.md's detailed, specific account (including two independently-found-and-fixed bugs, with commit hashes and exact SQL diffs) is trusted for the live-cluster claims.

### Human Verification Required

None. All findings above are independently verifiable via code and non-cluster test runs; the live-cluster claims were explicitly out of scope for re-execution per task instructions and are accepted on the strength of 09-11-SUMMARY.md's detailed, falsifiable account.

### Gaps Summary

The phase's actual engineering content — the watermark scoping fix, the 3-hop reconciliation, `meta.v_run_recovery`, the 2-year backfill capstone test, gap recording, and DAG wiring — is real, substantive, and correctly wired, not a documentation-only completion. All 5 ROADMAP success criteria are backed by genuine code artifacts and (per the trusted 09-11 account) a live-cluster proof that itself surfaced and fixed two previously-unknown bugs — a strong positive signal that the phase's own verification was adversarial rather than confirmatory.

However, running the full non-cluster test suite in this verification session (as the task explicitly requested, to confirm "genuinely working state right now") surfaced two sets of reproducible, order-independent failures that none of the 11 SUMMARY.md files mention:

1. **`test_migrations.py`** — two assertions using hardcoded expected-table/expected-grant sets that were never updated across two later plans (09-06's `v_run_recovery`, 09-10's `processing_gaps`). Low-risk, test-only fix.
2. **`test_staging_durability.py`** — two tests crash with `AttributeError` because plan 09-07 made `ctx.metadata` a hard dependency of `promote_to_durable_bronze` but this pre-existing test file (not in 09-07's declared `files_modified`) still constructs the context with `metadata=None`. Low-risk (production paths always supply real metadata), but the tests are currently non-functional, meaning this file provides zero regression coverage for `promote_to_durable_bronze` right now.

Neither gap falsifies any of the phase's 5 ROADMAP success criteria — the underlying capabilities are proven correct by other tests (`test_batch_complete_control_totals.py`, `test_reconciliation.py`) and by the live capstone. But a claim of "the codebase is in a genuinely working state" is not fully true today: `pytest tests/integration` does not exit green. This is squarely a "task completion ≠ goal achievement" style gap — every individual plan's own narrow verification passed, but nobody ran the accumulated full suite together after all 11 plans landed, so these two staleness/regression pairs were never caught.

**Recommendation:** A small follow-up (not a new plan-worthy scope, likely a `/gsd-quick` fix) updating the two stale assertion sets in `test_migrations.py` and wiring a real metadata repository into `test_staging_durability.py`'s context builder, then re-running `pytest tests/unit tests/integration -q` to confirm a fully green non-cluster suite before the milestone moves on to Phase 10.

---

*Verified: 2026-08-21T08:43:45Z*
*Verifier: Claude (gsd-verifier)*
