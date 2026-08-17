---
phase: 08-validation-quarantine-metadata-control-plane-completion
verified: 2026-08-17T11:35:01Z
status: human_needed
score: 5/5 roadmap success criteria mechanistically verified in code+tests; 2 items require live-cluster human verification before full closure
overrides_applied: 0
gaps: []
human_verification:
  - test: "Deploy this phase's artifacts to the live kind cluster (apply migrations 0014-0017 to analytics-db, rebuild/redeploy the csv-processor image with configs/datasets/orders.yaml + customers.yaml's quality block baked in, sync the Airflow DAG bundle so csv_ingest_orders.py is picked up, and unseal Vault), then run `pytest tests/e2e/slice/test_referential_orphan.py tests/e2e/slice/test_backfill_reentry.py -m cluster`."
    expected: "test_referential_orphan.py passes, proving VALID-07 (referential-orphan quarantine + non-orphan publish) live end-to-end. test_backfill_reentry.py's outcome is the key open question — see the next item."
    why_human: "Requires actual cluster deployment actions (migrations, image rebuild, DAG bundle sync, Vault unseal) this verifier cannot perform; confirmed empirically that both tests currently ERROR (not pass) — analytics-db is at migration 0013 (4 behind), csv_ingest_orders is absent from the deployed DAG bundle, and Vault is sealed (hvac.exceptions.VaultDown on kubernetes-auth login)."
  - test: "Investigate whether a content-differing 'corrected' file re-upload actually flips its predecessor's meta.rejected_records row from PENDING to REDRIVEN via the documented D-01 backfill re-entry path, per the architecture finding in deferred-items.md and this test's own docstring."
    expected: "Either the assertion in test_backfill_resolves_previously_rejected_row holds (in which case VALID-08's documented re-drive path is genuinely proven end-to-end), or it fails because meta.batches.batch_key is a pure function of content_sha256 (dataplat/discovery.py) while resolve_rejected_records_for_batch resolves PENDING rows strictly by batch_id — a corrected (content-different) file discovers under a brand-new batch_id, so the resolve call scoped to the new batch never touches the original PENDING row's batch. Code-level tracing by this verifier confirms the batch_key formula is exactly as described, so this is a real, not hypothetical, structural risk."
    why_human: "This is a Rule-4-territory architecture question (the phase's own 08-14-SUMMARY.md explicitly declines to fix it as a one-line change), and can only be empirically settled once the live cluster is deployed and the e2e test actually executes. The published-data half of VALID-08's success criterion (corrected data lands in normalized.customers/orders via ON CONFLICT upsert, independent of batch_id) is already proven by other integration tests and is not in question — only the resolution_type/audit-trail bookkeeping for a genuinely content-different correction is uncertain."
---

# Phase 8: Validation, Quarantine & Metadata Control-Plane Completion Verification Report

**Phase Goal:** No data is ever silently dropped — every rejected record is retained with a reason, reportable, and has a documented path back into the pipeline
**Verified:** 2026-08-17T11:35:01Z
**Status:** human_needed
**Re-verification:** No — initial verification

**Mode note:** ROADMAP.md marks this phase `mode: mvp`, but the phase goal text ("No data is ever silently dropped...") does not match the `As a <role>, I want <capability>, so that <outcome>.` User Story shape (`gsd-sdk query user-story.validate` returns `valid: false`). The phase does, however, carry five detailed, testable roadmap Success Criteria — the richer contract this report verifies against. This is flagged as an inconsistency for the roadmap/process owner to resolve (either the goal text should be rewritten as a User Story, or this phase's `mode` should not be `mvp`), not treated as a blocker to verification.

## Goal Achievement

### Observable Truths (Roadmap Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | A file with malformed rows loads the good rows and writes quarantine records naming source file, row number, column, error type, run and timestamp — nothing silently discarded | VERIFIED | `RaggedRowGuard`/`CompletenessRule`/`PatternRule`/`ValidityRangeRule`/`UniquenessRule` each convert a row-level violation into a `RejectedRecord` (never raise, QUAL-03). `meta.rejected_records` (migration `0015`) carries `run_id`→`meta.ingestion_runs` (timestamp), `file_id`→`meta.files` (source file), `source_row_number`, `error_column`, `error_type`. `test_backfill_run_never_resolves_its_own_fresh_rejects` and 5 other tests in `tests/integration/test_publish_transaction_wiring.py` prove the full write path against real PostgreSQL. All 116 integration tests pass. |
| 2 | A validation report exists as PostgreSQL rows and a MinIO artifact; a threshold-breaching dataset reports FAIL/QUARANTINE, an under-threshold one reports PASS_WITH_WARNING | VERIFIED | `pipeline/run.py:_apply_post_publish_barriers_and_persist` calls `ctx.metadata.record_validation_results` (Postgres) and `ctx.objects.put_object(bucket="validated", key=f"{dataset}/{run_id}/report.json", ...)` (MinIO) from the SAME `all_findings`/`all_rejected` objects — no second, independently-computed view. `RejectionRateCircuitBreaker` (D-10) raises `QualityThresholdExceeded` on a real numeric ratio-vs-threshold breach; `StrategyDispatchStage` maps `WARN_AND_CONTINUE`→`PASS_WITH_WARNING` finding, `FAIL_FILE`/`QUARANTINE_FILE`→run-fatal exception. `test_report_artifact_matches_persisted_postgres_rows` (passing) proves the MinIO/Postgres pair matches. |
| 3 | Corrected quarantined records re-enter the pipeline through the documented re-drive path and land in the warehouse | UNCERTAIN (see human_verification) | The "land in the warehouse" half is proven: `OrdersMergePublisher`/`MergePublisher` use `INSERT ... ON CONFLICT DO UPDATE`, so any valid corrected row publishes on its next ingest regardless of batch scoping. The "documented re-drive path" (D-01: Airflow backfill) half has a code-traced, documented, NOT-yet-empirically-confirmed structural risk: `meta.batches.batch_key` is a pure function of `content_sha256` (`discovery.py`), while `resolve_rejected_records_for_batch` resolves strictly by `batch_id` — a content-differing correction discovers under a new `batch_id` and may never flip its predecessor's `PENDING` row to `REDRIVEN`. `tests/e2e/slice/test_backfill_reentry.py` is written to prove/disprove this but currently ERRORs (Vault sealed; DAG/migrations not deployed) rather than running to a real result. Deferred-items.md documents this as an open, Rule-4-territory architecture question. |
| 4 | A truncated/still-uploading file (checksum mismatch, size mismatch, wrong extension, empty, missing `_BATCH_COMPLETE`) is refused before any parsing occurs | VERIFIED | `airflow/dags/_common/integrity_gate.py` (`integrity_gate` `@task`) runs extension/empty/5-second-stability/readability checks purely in the Airflow scheduler process, BEFORE any `KubernetesPodOperator` pod (which is where parsing happens) launches; wired `wait_for_files >> matched_keys >> gate >> discover` in both `csv_ingest_customers.py` and `csv_ingest_orders.py`, proven by `test_integrity_gate_upstream_of_discover` (passing). `_BATCH_COMPLETE` gate lives in `discovery.py`'s `_apply_batch_complete_marker_gate`, opt-in via `config.source.batch_complete_marker`, proven by `tests/unit/validate/test_batch_complete_marker.py` (passing). `tests/unit/test_integrity_gate.py` covers wrong-extension/empty/unstable/unreadable rejection paths (all passing), each writing its own `meta.files` row (D-20) so the rejection is itself durable, never silent. |
| 5 | A file at 10× historical baseline row count is flagged as a volume anomaly; an orphan foreign key produces the dataset's configured fail/quarantine/warn outcome | VERIFIED | `VolumeAnomalyBarrier` compares `current_row_count > avg(historical row_count) * multiplier` (default `10.0`, dataset-configurable) against real persisted `meta.validation_results`/`meta.ingestion_runs` rows — self-referential history, cold-start-safe (`_MIN_PRIOR_RUNS_FOR_COMPARISON`). `ReferentialIntegrityBarrier` (`validate/referential.py`) anti-joins staging against `normalized.customers`, dispatches `fail`/`quarantine`/`warn` per D-16/D-07 strategy. Both proven by `tests/integration/test_volume_anomaly.py` and `tests/integration/test_referential_integrity.py` (all passing against real testcontainers PostgreSQL). |

**Score:** 4/5 fully VERIFIED, 1/5 UNCERTAIN (structural risk documented and code-traced, not yet empirically confirmed either way).

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `migrations/versions/0014_meta_validation_results.py` | `meta.validation_results` DDL | VERIFIED | Applied in test DB (all integration tests pass against it); confirmed absent from the LIVE cluster's analytics-db (`alembic_version` = `0013`) — a deployment gap, not a code gap. |
| `migrations/versions/0015_meta_rejected_records.py` | `meta.rejected_records` DDL incl. `resolution_type`/`resolved_by_run_id`/`batch_id` | VERIFIED | Same as above. |
| `migrations/versions/0016_normalized_orders.py`, `0017_normalized_orders_business_key_unique.py` | `normalized.orders` DDL | VERIFIED | Same as above. |
| `packages/dataplat/src/dataplat/validate/{completeness,pattern,validity_range,uniqueness,circuit_breaker,referential,volume_anomaly}.py` | VALID-02/03/07/09 rule classes | VERIFIED | All exist, registered in `VALIDATION_RULE_REGISTRY`, each with passing unit + property tests (`tests/property/test_quality_rules_never_raise.py` proves QUAL-03 for adversarial input). |
| `packages/dataplat/src/dataplat/validate/strategy_dispatch.py` | D-07 5-strategy outcome wrapper | VERIFIED | Fails fast on unknown strategy; `REJECT_RECORD`/`QUARANTINE_RECORD` passthrough, `WARN_AND_CONTINUE`→`PASS_WITH_WARNING`, `FAIL_FILE`/`QUARANTINE_FILE`→run-fatal. |
| `packages/dataplat/src/dataplat/metadata/postgres.py` | `record_validation_results`/`record_rejected_records`/`resolve_rejected_records_for_batch` | VERIFIED | All three exist (lines 474, 513, 557); ordering bug (CR-01) fixed — resolve now runs BEFORE this run's own inserts, verified in source and by `test_backfill_run_never_resolves_its_own_fresh_rejects`. |
| `airflow/dags/_common/integrity_gate.py` | LOAD-10 pre-pod-launch gate | VERIFIED | Wired upstream of `discover` in both DAGs; own `meta.files` rejection-row write (D-20). |
| `airflow/dags/csv_ingest_orders.py` | Second live dataset DAG | VERIFIED | 145 lines, mirrors `csv_ingest_customers.py`'s shape, Asset-scheduled off `customers_asset`, integrity-gate-wired. NOT yet deployed to the live cluster (`airflow dags list` shows only `csv_ingest_customers`). |
| `packages/dataplat/src/dataplat/load/publish/merge_orders.py` | `OrdersMergePublisher` | VERIFIED | NULL-safe `order_date` comparison (WR-04 fix confirmed present: `normalized.orders.order_date IS NULL OR EXCLUDED.order_date >= ...`). |
| `configs/datasets/orders.yaml`, `configs/datasets/customers.yaml` | Real `quality:` blocks | VERIFIED | Referenced by passing integration tests exercising REFERENTIAL/UNIQUENESS/COMPLETENESS rules against real data. |
| `tests/dagtest/test_backfill_dagrun.py` | Backfill DagRun mechanics proof | VERIFIED | 2/2 tests pass (testcontainers Airflow metadata DB, `dag.test()`), for both `csv_ingest_customers` and `csv_ingest_orders`. |
| `tests/e2e/slice/test_referential_orphan.py`, `tests/e2e/slice/test_backfill_reentry.py` | Live-cluster closing proofs | ORPHANED (correctly written, not yet runnable) | `pytest --collect-only`/`ruff`/`mypy` all clean; executing with `-m cluster` against the actual live cluster ERRORs on `hvac.exceptions.VaultDown: Vault is sealed` before even reaching the DB/DAG gaps also confirmed live (migrations behind, DAG bundle missing `csv_ingest_orders`). Not a code defect — an infrastructure/deployment state issue, independently confirmed by this verifier via direct `kubectl exec`. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `wait_for_files` (S3KeySensor) | `discover` (KPO) | `matched_keys >> gate >> discover` | WIRED | Confirmed in both DAG source files and `test_integrity_gate_upstream_of_discover` (passing). |
| `pipeline/run.py` barrier stages | `meta.validation_results`/`meta.rejected_records` | `ctx.metadata.record_validation_results`/`record_rejected_records` inside the publish transaction | WIRED | Same transaction, same `all_findings`/`all_rejected` objects also used for the MinIO report — confirmed by `test_report_artifact_matches_persisted_postgres_rows`. |
| `pipeline/run.py` | MinIO `validated` bucket | `ctx.objects.put_object` | WIRED | Confirmed at `run.py` (report_key construction + put_object call). |
| `StagingLoader._build_stages` | `ctx.config.quality` rule declarations | `resolve_validation_rule` + `StrategyDispatchStage` wrapping | WIRED | Confirmed via `tests/integration/test_staging_quality_rules.py` (passing). |
| `RejectionRateCircuitBreaker`/`StrategyDispatchStage` raise | Publish-transaction rollback (D-11) | `QualityThresholdExceeded` propagation, "this function catches nothing" | WIRED | `test_circuit_breaker_trip_leaves_zero_rows_for_this_run` (passing). |
| Airflow `airflow backfill create` | `meta.rejected_records.resolution_type` flip | `resolve_rejected_records_for_batch` scoped by `batch_id` | UNCERTAIN for content-differing corrections | See Truth #3 above — proven for same-`batch_id` re-runs (integration test), NOT yet proven end-to-end for a genuinely corrected (different-content) file discovered under a new `batch_id`. |

### Requirements Coverage

| Requirement | Source Plan(s) | Description | Status | Evidence |
|-------------|----------------|--------------|--------|----------|
| VALID-01 | 08-01, 08-04, 08-10 | Structural validation (column count, malformed rows, unclosed quotes, missing delimiters) | SATISFIED | `RaggedRowGuard` (pre-existing Phase 3/6 detection, D-08) registered as `"STRUCTURAL"` in `VALIDATION_RULE_REGISTRY` this phase, wired through `StrategyDispatchStage`. REQUIREMENTS.md still shows `[ ]`/"Pending" for this ID — a stale tracking-doc lag, not a code gap (see note below). |
| VALID-02 | 08-01, 08-04, 08-07, 08-10, 08-11 | Completeness/uniqueness/validity-range/pattern/referential with configurable thresholds | SATISFIED | All 5 rule classes exist, registered, strategy-dispatched, tested (unit + property + integration). |
| VALID-03 | 08-01, 08-04, 08-07, 08-10 | Quarantine per configurable strategy, retaining source file/row/error/run/timestamp | SATISFIED | `meta.rejected_records` schema + `record_rejected_records` + strategy dispatch, proven live against real PostgreSQL. |
| VALID-04 | 08-01, 08-03, 08-11 | Machine-readable reports in Postgres rows + MinIO artifacts | SATISFIED | Confirmed above (Truth #2). REQUIREMENTS.md shows `[ ]`/"Pending" — stale doc, not a code gap. |
| VALID-07 | 08-05, 08-08, 08-12, 08-14 | Referential integrity, configurable fail/quarantine/warn | SATISFIED at the mechanism level (integration-tested); live-cluster closing proof (08-14) blocked on deployment, not code | `ReferentialIntegrityBarrier` + `tests/integration/test_referential_integrity.py` (passing). Live e2e (`test_referential_orphan.py`) written, clean, currently ERRORs due to Vault-sealed/undeployed-migrations state — see human_verification. |
| VALID-08 | 08-03, 08-12, 08-13, 08-14 | Documented re-drive path after correction | UNCERTAIN — see Truth #3 and human_verification | `resolve_rejected_records_for_batch` mechanism proven for same-`batch_id` scope; content-differing-correction scenario has a code-traced, documented, unconfirmed structural risk (content-hash `batch_key`). |
| VALID-09 | 08-01, 08-09, 08-11 | Volume/quality anomalies against persisted statistical baselines, no ML | SATISFIED | `VolumeAnomalyBarrier`, plain SQL `avg * multiplier` comparison, `tests/integration/test_volume_anomaly.py` (passing). |
| LOAD-10 | 08-02, 08-12 | File integrity verified before processing | SATISFIED | `integrity_gate.py`, wired upstream of `discover`, own rejection-row write (D-20). REQUIREMENTS.md shows `[ ]`/"Pending" — stale doc, not a code gap. |
| LOAD-11 | 08-01, 08-06 | Optional `_BATCH_COMPLETE` manifest support | SATISFIED | `_apply_batch_complete_marker_gate` in `discovery.py`, opt-in, tested. REQUIREMENTS.md shows `[ ]`/"Pending" — stale doc, not a code gap. |

**Note on REQUIREMENTS.md staleness:** 5 of the 9 phase-08 requirement IDs (VALID-01, VALID-04, VALID-08, LOAD-10, LOAD-11) are still shown as `[ ]`/"Pending" in `.planning/REQUIREMENTS.md`, while the other 4 (VALID-02, VALID-03, VALID-07, VALID-09) are marked `[x]`/"Complete". Direct code/test verification in this report shows all 9 have real, substantive implementations and passing tests (with VALID-08's live-cluster closing proof still open, as documented above). This is very likely a documentation-update lag (REQUIREMENTS.md was not fully re-synced after phase 08 closed) rather than a reflection of actual incompleteness, but it should be corrected — a future reader of REQUIREMENTS.md alone would draw the wrong conclusion.

**Orphaned requirements:** None. REQUIREMENTS.md's Phase 8 row list (VALID-01/02/03/04/07/08/09, LOAD-10/11) matches the roadmap's declared requirement set and every plan's own `requirements:` frontmatter exactly.

### Anti-Patterns Found

No `TBD`/`FIXME`/`XXX`/`TODO`/`HACK`/`PLACEHOLDER` markers found in any of the phase's key modified files (`integrity_gate.py`, both DAG files, `pipeline/run.py`, `merge_orders.py`, `metadata/postgres.py`). No empty-implementation or hardcoded-empty-return stubs found in the validation rule classes or barrier stages.

The phase's own code review (`08-REVIEW.md`) found 1 critical + 4 warnings:
- **CR-01** (a run's own fresh rejects immediately marked REDRIVEN by the same run) — **FIXED**, confirmed present in `pipeline/run.py` (commit `9731192`) and covered by a new regression test (`test_backfill_run_never_resolves_its_own_fresh_rejects`, passing).
- **WR-04** (NULL `order_date` permanently blocks re-publication) — **FIXED**, confirmed present in `merge_orders.py` (commit `e7bf450`).
- **WR-01** (`rows_deduplicated` metric conflates referential-orphan deletion with genuine dedup for `orders`) — deferred, documented, metrics-accuracy issue only, no data-loss/silent-drop impact.
- **WR-02** (`VolumeAnomalyBarrier` has no `REJECT_RECORD` strategy mapping, falls back to `"FAIL"`) — deferred, documented, dormant (no live dataset declares a `VOLUME` rule with this strategy).
- **WR-03** (`::int` cast against a `type: string`-declared business key column) — deferred, documented, pre-existing pattern from an earlier phase, dormant under current live data (columns are genuinely integer-typed today).

None of WR-01/02/03 are blockers to this phase's success criteria; all are correctly scoped as future architecture/config-policy decisions per the phase's own deferred-items.md.

### Behavioral Spot-Checks / Test Suite Execution

This verifier independently ran the full local test suite (not trusting SUMMARY.md's claimed counts):

| Suite | Command | Result | Status |
|-------|---------|--------|--------|
| unit + property + policy | `pytest tests/unit tests/property tests/policy -q -m "not integration and not dagtest and not cluster and not manifests"` | 613 passed, 12 deselected | PASS |
| integration (testcontainers Postgres/MinIO) | `pytest tests/integration -q` | 116 passed | PASS |
| dagtest (testcontainers Airflow metadata DB) | `pytest tests/dagtest -q` | 2 passed | PASS |
| mypy (in-scope: `packages/dataplat/src`, `packages/csv-processor/src`) | `mypy packages/dataplat/src packages/csv-processor/src` | Success: no issues found in 72 source files | PASS |
| ruff | `ruff check .` | All checks passed! | PASS |
| e2e cluster (`test_referential_orphan.py`, `test_backfill_reentry.py`) | `pytest tests/e2e/slice/test_referential_orphan.py tests/e2e/slice/test_backfill_reentry.py -m cluster` | 2 errors — `hvac.exceptions.VaultDown: Vault is sealed` | ERROR (expected, documented, infra-gated — see human_verification) |

Note: `mypy` was NOT run over `airflow/dags` because `Makefile`'s own `TYPECHECK_PATHS` never includes it (pre-existing scope decision, not a phase-08 regression) — running it manually surfaces 71 pre-existing `KubernetesPodOperator`/XComArg stub-typing errors unrelated to phase 08's changes, consistent with this being an out-of-scope, long-standing gap in the Airflow-provider type stubs rather than a phase-08 defect.

### Live-Cluster State (independently confirmed by this verifier)

```
$ kubectl -n data exec analytics-db-1 -- psql -U postgres -d analytics -c "SELECT version_num FROM meta.alembic_version"
 version_num
-------------
 0013
```
Four migrations behind (`0014`-`0017` not applied).

```
$ kubectl -n airflow exec deploy/airflow-api-server -- airflow dags list
csv_ingest_customers | ...
```
`csv_ingest_orders` absent from the deployed DAG bundle.

```
$ pytest tests/e2e/slice/test_referential_orphan.py tests/e2e/slice/test_backfill_reentry.py -m cluster
...
E  hvac.exceptions.VaultDown: Vault is sealed, on post http://127.0.0.1:45023/v1/auth/kubernetes/login
2 errors in 8.51s
```

All three findings match `deferred-items.md`'s own documented state exactly — this is a genuine, expected, well-documented deployment gap, not a discrepancy between SUMMARY claims and reality.

### Human Verification Required

See YAML frontmatter `human_verification` for full detail. Summary:

1. **Deploy phase 08's artifacts to the live cluster and re-run the two `-m cluster` e2e tests.** Requires: apply migrations `0014`-`0017`, rebuild/redeploy `csv-processor` with the updated dataset configs, sync the Airflow DAG bundle, unseal Vault.
2. **Once deployed, observe whether `test_backfill_reentry.py`'s resolution assertion passes or fails**, and treat a failure as confirmation of the documented `batch_key`/content-hash architecture finding (VALID-08's redrive-audit-trail gap for genuinely content-different corrections) rather than a new, unexplained regression — the root cause is already identified and documented in `deferred-items.md` and the test's own module docstring.

### Gaps Summary

No code-level BLOCKER gaps were found. All 9 phase-08 requirements have substantive, tested implementations; the phase's own code-review CRITICAL finding (CR-01) and one of its NULL-safety WARNING findings (WR-04) were fixed and independently re-verified present in this report. Three lower-severity WARNING findings (WR-01/02/03) are correctly deferred, documented, and non-blocking (metric-accuracy, dormant-strategy-gap, dormant-type-cast issues — none causes silent data loss today).

The phase is functionally complete in code and locally-testable ways, but two things remain open and require a human/operator action before the phase can be called fully closed:
1. A standard post-wave deployment step (migrations, image rebuild, DAG bundle sync, Vault unseal) that this isolated verification cannot and should not perform against the shared live cluster.
2. A specific, well-documented architectural uncertainty about whether VALID-08's re-drive path correctly resolves the audit trail for a genuinely content-different corrected file (as opposed to a same-content re-run) — this can only be settled empirically once the live e2e test actually runs to completion.

REQUIREMENTS.md's checkbox/table tracking for 5 of the 9 requirement IDs is stale and should be updated to reflect the actual (substantiated) completion state documented in this report.

---

_Verified: 2026-08-17T11:35:01Z_
_Verifier: Claude (gsd-verifier)_
