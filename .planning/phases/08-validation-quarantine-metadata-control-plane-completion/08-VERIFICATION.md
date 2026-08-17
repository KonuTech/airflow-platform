---
phase: 08-validation-quarantine-metadata-control-plane-completion
verified: 2026-08-18T02:00:00Z
status: passed
score: 5/5 roadmap success criteria VERIFIED (VALID-08 / Truth #3, the one confirmed-FAILING gap from the prior verification round, is now independently confirmed closed)
overrides_applied: 0
re_verification:
  previous_status: gaps_found
  previous_score: "4/5 fully VERIFIED, 1/5 (VALID-08) confirmed FAILING live"
  gaps_closed:
    - "VALID-08 / roadmap success criterion 3 ('Corrected quarantined records re-enter the pipeline through the documented re-drive path and land in the warehouse'): resolution scoping moved from strict batch_id matching (which the prior verification proved live can never resolve a content-differing correction's PENDING row) to (dataset_id, business_key) matching, per locked decisions D-23/D-24/D-25. Independently re-verified: migration 0020 applied (schema), business_key extraction wired into every RejectedRecord-creation site (CompletenessRule/PatternRule/ValidityRangeRule/UniquenessRule/ReferentialIntegrityBarrier), and run_ingest's resolution call wired to the real derivation. Live-cluster query this session confirms meta.rejected_records shows PENDING:8/REDRIVEN:2 (was PENDING:5/REDRIVEN:0 at the prior verification) — the mechanism has now genuinely fired twice against the real deployed platform."
    - "CR-01 (08-REVIEW.md, this gap-closure round's own code review): the original wiring resolved rejects by reading SELECT DISTINCT over the STAGING table (what merely staged), not what the publish statement's ON CONFLICT ... WHERE conflict-guard actually wrote/updated -- a real false-positive-resolution path. Fixed: both MergePublisher/OrdersMergePublisher now RETURNING their business-key column from the INSERT ... ON CONFLICT statement, threaded through PublishResult.published_business_keys, and run.py resolves using that instead of a staging-table read. Independently reproduced this session: checked out the pre-fix source into an isolated worktree, ran the new regression test (test_staged_but_conflict_guard_blocked_business_key_stays_pending) against it -- confirmed it FAILS with exactly the claimed symptom (AssertionError: assert 'REDRIVEN' == 'PENDING'), then confirmed it PASSES against the current (fixed) code. Not a tautology."
    - "WR-01 (same review): business-key column resolution silently picked the first business_key:true column with no cardinality guard. Fixed via a DatasetConfig model_validator rejecting >1 business_key:true column; independently confirmed present in config/model.py and covered by a dedicated unit test."
  gaps_remaining: []
  regressions: []
---

# Phase 8: Validation, Quarantine & Metadata Control-Plane Completion Verification Report

**Phase Goal:** No data is ever silently dropped — every rejected record is retained with a reason, reportable, and has a documented path back into the pipeline
**Verified:** 2026-08-18T02:00:00Z
**Status:** passed
**Re-verification:** Yes — after gap-closure round (plans 08-16, 08-17, 08-18, spawned via `/gsd:plan-phase 8 --gaps`), targeting the single confirmed-FAILING gap (VALID-08) from the prior 08-VERIFICATION.md.

**Mode note (carried forward, unchanged from prior verification):** ROADMAP.md marks this phase `mode: mvp`, but `gsd-sdk query user-story.validate` confirms the phase goal text does not match the User Story shape (`valid: false`). The phase carries five detailed, testable roadmap Success Criteria, which this report verifies against as the richer contract — a process inconsistency, not a verification blocker, unchanged from the prior report's judgment.

## Summary of This Verification's Independent Findings

This is a re-verification following a targeted gap-closure round. Rather than trust 08-16/17/18-SUMMARY.md, 08-REVIEW.md, or 08-REVIEW-FIX.md's own narration, this verifier independently re-derived the current state:

- **Read** every gap-closure plan (08-16/17/18) and summary, the locked D-23/D-24/D-25 decisions in 08-CONTEXT.md, and 08-REVIEW.md/08-REVIEW-FIX.md's own critical/warning findings and fixes.
- **Read the actual source** of every claimed change: `migrations/versions/0020_meta_rejected_records_business_key.py`, `models/record.py`, `metadata/repository.py`, `metadata/postgres.py`, `pipeline/run.py`, `load/publish/{protocol,merge,merge_orders}.py`, `config/model.py`. Confirmed the code matches what the plans/summaries claim, not merely that files were touched.
- **Grepped the full source+test tree** for `resolve_rejected_records_for_batch` (0 references anywhere) and for every remaining `SELECT DISTINCT ... FROM {staging_table}`-shaped read (none feed the resolution call any more — the only `SELECT DISTINCT` left is the publishers' own pre-existing `DISTINCT ON (customer_id|order_id)` deduplication clause inside their `INSERT` statement, unrelated to reject-resolution).
- **Independently reproduced the CR-01 regression test's claim** that it fails on pre-fix code and passes on the fix: checked out the pre-CR-01-fix commit into an isolated `git worktree`, copied in the current (post-fix) test file, ran `test_staged_but_conflict_guard_blocked_business_key_stays_pending` against the OLD source via `PYTHONPATH` override — it failed with exactly the claimed symptom (`AssertionError: assert 'REDRIVEN' == 'PENDING'`). Re-ran the same test against the current tree — passes. This is not a tautological test.
- **Independently re-ran** ruff (`packages/dataplat/src tests` — clean), mypy (`packages/dataplat/src packages/csv-processor/src` — 72 files, no issues), the full unit+property+policy suite (622 passed), the full integration suite (117 passed, including the two previously-skipped Test C/C2 now un-skipped and passing, plus the new CR-01 regression test), the combined `pytest tests/unit tests/integration -m "not cluster"` (609 passed, matching the SUMMARY's claim exactly), `pytest tests/policy` (134 passed, including the DAG line-budget test), and `make check` end to end (492 tests + 71 corpus fixtures, green).
- **Queried the live kind cluster directly** this session: `meta.alembic_version` = `0020`; `meta.rejected_records.business_key` column and its `(business_key, resolution_type)` index both present; `meta.rejected_records` resolution-type counts are `PENDING: 8, REDRIVEN: 2` (was `PENDING: 5, REDRIVEN: 0` at the prior verification — the mechanism has now fired twice, matching the SUMMARY's claim of two live test runs); both `csv_ingest_customers`/`csv_ingest_orders` DAGs deployed; the live `csv_processor_image` Airflow Variable points at `localhost:5001/csv-processor:4a2ded0` — the WR-01-fix commit, confirming the currently-deployed image includes both this round's CR-01 and WR-01 fixes, not a stale pre-fix build; Vault unsealed.
- **Did not re-run** `pytest tests/e2e/slice/test_backfill_reentry.py -x -m cluster` myself this session (a ~4-5 minute live-cluster test against a shared demo cluster that has already twice completed genuinely per this round's own SUMMARY evidence, corroborated by the live DB's `REDRIVEN: 2` count and the deployed image tag matching the WR-01 fix). The live DB state is offered as strong independent corroboration that is consistent with, not merely repeating, the SUMMARY's narrative.

## Goal Achievement

### Observable Truths (Roadmap Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | A file with malformed rows loads the good rows and writes quarantine records naming source file, row number, column, error type, run and timestamp — nothing silently discarded | VERIFIED | Unchanged since prior verification (this round did not touch these code paths). Re-confirmed passing this session via the full integration suite. |
| 2 | A validation report exists as PostgreSQL rows and a MinIO artifact; a threshold-breaching dataset reports FAIL/QUARANTINE, an under-threshold one reports PASS_WITH_WARNING | VERIFIED | Unchanged; `test_report_artifact_matches_persisted_postgres_rows` re-confirmed passing this session. |
| 3 | Corrected quarantined records re-enter the pipeline through the documented re-drive path and land in the warehouse | **VERIFIED (gap closed)** | Was the single FAILED truth in the prior verification, live-confirmed broken (`resolve_rejected_records_for_batch`'s strict `batch_id` scoping could never resolve a content-differing correction's PENDING row). Now: (a) schema — `meta.rejected_records.business_key` column + index, migration 0020, live-confirmed applied; (b) capture — every `RejectedRecord`-creation site in the platform's real rule set populates `business_key` when reliably extractable, `RaggedRowGuard` provably untouched (`git diff --stat` empty for that file); (c) resolution — `resolve_rejected_records_for_business_keys` matches `(dataset_id, business_key)` across ANY `batch_id`, proven in isolation (`test_backfill_resolution.py`, full D-23/D-24/D-25 scoping matrix) and through real `run_ingest` execution (`test_publish_transaction_wiring.py` Test C/C2, cross-batch + CR-01 self-protection); (d) correctness hardening — CR-01 (staged-but-conflict-guard-blocked business keys must never resolve) independently reproduced as a genuine, non-tautological regression test; (e) live proof — `meta.rejected_records` on the real cluster shows `REDRIVEN: 2` (was `0`), the deployed image tag matches the WR-01-fix commit, both DAGs and migration 0020 are live. |
| 4 | A truncated/still-uploading file (checksum mismatch, size mismatch, wrong extension, empty, missing `_BATCH_COMPLETE`) is refused before any parsing occurs | VERIFIED | Unchanged; `test_integrity_gate.py`/`test_batch_complete_marker.py` re-confirmed passing this session. |
| 5 | A file at 10× historical baseline row count is flagged as a volume anomaly; an orphan foreign key produces the dataset's configured fail/quarantine/warn outcome | VERIFIED | Unchanged; both halves re-confirmed passing this session (`test_referential_integrity.py`, extended this round with a `business_key == order_id` assertion, also re-confirmed passing). |

**Score:** 5/5 fully VERIFIED — the previously-confirmed-failing truth is now closed at the schema, extraction, resolution-matching, and live-cluster levels, plus a genuine correctness bug (CR-01) this round's own code review found and fixed before it could cause a real false-positive resolution.

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `migrations/versions/0020_meta_rejected_records_business_key.py` | `meta.rejected_records.business_key` column + index | VERIFIED | Read directly: `revision="0020"`, `down_revision="0019"`, nullable `business_key` column + `(business_key, resolution_type)` index, no GRANT (table-level grant from 0015 covers it). Live-confirmed applied: `alembic_version = 0020`. |
| `packages/dataplat/src/dataplat/models/record.py` | `RejectedRecord.business_key: str \| None = None` | VERIFIED | Field present, last position, non-breaking (every construction call site uses kwargs). |
| `packages/dataplat/src/dataplat/metadata/repository.py` | `resolve_rejected_records_for_business_keys` Protocol method | VERIFIED | Present; `resolve_rejected_records_for_batch` fully removed (0 references anywhere in `packages/dataplat/src` or `tests`). |
| `packages/dataplat/src/dataplat/metadata/postgres.py` | `PostgresMetadataRepository.resolve_rejected_records_for_business_keys` | VERIFIED | `UPDATE meta.rejected_records ... FROM meta.batches WHERE meta.batches.batch_id = meta.rejected_records.batch_id AND meta.batches.dataset_id = %s AND meta.rejected_records.business_key = ANY(%s) AND meta.rejected_records.resolution_type = 'PENDING'` — matches D-23 exactly, `business_keys=[]` short-circuits to a documented no-op, docstring states this is the sole write path to `resolution_type`. |
| `packages/dataplat/src/dataplat/validate/{completeness,pattern,validity_range,uniqueness,referential}.py` | `business_key`/`business_key_index` extraction at every `RejectedRecord` site | VERIFIED | Read directly: all four streaming rules take `business_key_index`, extract via a per-file `_extract_business_key` helper, thread `business_key=` into every `RejectedRecord(...)` call (both of `ValidityRangeRule`'s two sites). `ReferentialIntegrityBarrier` captures `business_key=str(row["order_id"])` — the orphan row's own identity, not the FK it failed against. `pipeline/engine.py` (`RaggedRowGuard`) provably untouched. |
| `packages/dataplat/src/dataplat/load/staging.py` | `business_key_index` computed once, threaded into every quality-rule construction | VERIFIED | `_build_quality_stages` resolves the single `business_key: true` column via the same `self._target_columns.index(...)` idiom `_build_one_quality_stage` already uses, threaded unconditionally into `rule_kwargs`. |
| `packages/dataplat/src/dataplat/pipeline/run.py` | `_apply_post_publish_barriers_and_persist` wired to real, correctness-safe business-key derivation | VERIFIED | `dataset_id` hoisted once; resolution call uses `published_business_keys` — a caller PARAMETER sourced from `publisher.publish()`'s own `PublishResult.published_business_keys`, NOT a `SELECT DISTINCT` over the staging table (this was the CR-01 fix, verified below). CR-01-ordering (resolve strictly BEFORE `record_rejected_records`) preserved. |
| `packages/dataplat/src/dataplat/load/publish/{protocol,merge,merge_orders}.py` | `PublishResult.published_business_keys` populated from `RETURNING` | VERIFIED | `protocol.py` adds the field (default `()`), documents why. Both `MergePublisher`/`OrdersMergePublisher`'s `_PUBLISH_SQL` now end in `RETURNING customer_id`/`RETURNING order_id`; both `publish()` methods populate `published_business_keys` from the cursor's `fetchall()` — the exact rows the conflict-guarded `ON CONFLICT DO UPDATE` actually affected, never a blind staging-table read. |
| `packages/dataplat/src/dataplat/config/model.py` | Cardinality guard on `business_key: true` columns (WR-01 fix) | VERIFIED | `DatasetConfig._check_at_most_one_business_key_column` model_validator present; rejects >1 `business_key: true` column; unit test (`test_dataset_config_rejects_more_than_one_business_key_column`) confirmed passing. |
| `tests/integration/test_backfill_resolution.py` | D-23/D-24/D-25 scoping matrix proof (isolated repository call) | VERIFIED | 2/2 tests pass: `test_resolution_scoped_to_business_key_across_batches_and_idempotent_on_replay` (cross-batch resolution, business-key isolation, dataset isolation, NULL-never-resolves, idempotent replay all in one scenario) and `test_resolve_rejected_records_for_business_keys_is_the_only_write_path_to_resolution_type`. |
| `tests/integration/test_publish_transaction_wiring.py` | Real `run_ingest`-driven cross-batch proof (Test C/C2) + CR-01 regression test | VERIFIED | All 6 tests pass, 0 skipped (the two tests 08-16 had to skip as structurally NULL-incompatible are now un-skipped and rewritten). Independently reproduced that `test_staged_but_conflict_guard_blocked_business_key_stays_pending` fails on pre-fix code and passes on the fix — not a tautology. |
| `tests/e2e/slice/test_backfill_reentry.py` | Live-cluster proof, docstring updated | VERIFIED | Docstring/failure-message no longer references the superseded `resolve_rejected_records_for_batch`/D-05 batch-scoping caveat; describes the current `resolve_rejected_records_for_business_keys`/D-23 mechanism. Not re-run live this session (see Summary above); live DB state (`REDRIVEN: 2`) is independent corroboration that it has run to completion twice. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `run_ingest`'s `publisher.publish(...)` call | `_apply_post_publish_barriers_and_persist`'s resolution call | `published_business_keys=result.published_business_keys` (a real function parameter, not a re-derivation) | WIRED | Read directly at `pipeline/run.py` lines ~702-732: `result = publisher.publish(...)`, then `_apply_post_publish_barriers_and_persist(..., published_business_keys=result.published_business_keys, ...)`. |
| `MergePublisher`/`OrdersMergePublisher`'s `INSERT ... ON CONFLICT ... RETURNING` | `PublishResult.published_business_keys` | `cursor.fetchall()` after `conn.execute(_PUBLISH_SQL...)` | WIRED | Read directly in both files: `published_business_keys = tuple(str(row[0]) for row in cursor.fetchall())`. |
| `_apply_post_publish_barriers_and_persist` | `ctx.metadata.resolve_rejected_records_for_business_keys` | Direct call, `dataset_id`+`published_business_keys` params, strictly BEFORE `record_rejected_records` | WIRED | Read directly; ordering comment explicitly ties this to CR-01's guarantee holding under the new predicate. |
| `resolve_rejected_records_for_business_keys` | `meta.rejected_records.resolution_type` | `UPDATE ... FROM meta.batches WHERE ... business_key = ANY(%s) ... resolution_type='PENDING'` | WIRED, live-proven | Live query this session: `PENDING: 8, REDRIVEN: 2` — was `PENDING: 5, REDRIVEN: 0` at the prior verification; the link has now fired twice against real content-differing-correction data. |
| `DatasetConfig` validation | `staging.py`/`run.py`'s business-key column resolution | `_check_at_most_one_business_key_column` model_validator | WIRED | Confirmed present and unit-tested; both real dataset configs (`customers.yaml`/`orders.yaml`) still validate cleanly (single-column business keys). |

### Data-Flow Trace (Level 4) — the VALID-08 chain, re-traced

1. **Extraction:** A quality-rule rejection (e.g. `PatternRule`) calls `_extract_business_key(row, self._business_key_index)`, populating `RejectedRecord.business_key` from the dataset's configured business-key column — confirmed by reading the code and by the unit tests added this round (`test_quality_rules.py`, `test_uniqueness.py`) asserting the extracted value on both the populated and `None`-index paths.
2. **Persistence:** `record_rejected_records` inserts `record.business_key` into `meta.rejected_records.business_key` — confirmed by reading `postgres.py`'s `INSERT` column/parameter list.
3. **Publish:** `MergePublisher`/`OrdersMergePublisher`'s `RETURNING`-augmented `INSERT ... ON CONFLICT` returns exactly the business-key values the statement actually affected (excluding conflict-guard-blocked "locked but unchanged" rows) — confirmed by direct SQL reading and by the independently-reproduced CR-01 regression test.
4. **Resolution:** `run_ingest` passes those actually-published keys into `resolve_rejected_records_for_business_keys(dataset_id=..., business_keys=...)`, which flips every `PENDING` row sharing `(dataset_id, business_key)` — regardless of `batch_id` — to `REDRIVEN`.
5. **Live confirmation:** `meta.rejected_records` on the real cluster shows `REDRIVEN: 2`, up from `0` at the prior verification, with the currently-deployed image tag (`4a2ded0`) matching the commit that includes both the CR-01 and WR-01 fixes — i.e. the chain traced above is not merely unit-tested, it is the exact code presently running against the live platform.

**Conclusion:** DISCONNECTED (prior verification) → FLOWING (this verification). The chain that previously "never executed on this cluster" now demonstrably has, twice, with the corrected code deployed.

### Behavioral Spot-Checks / Test Suite Execution

Independently re-run this session (not trusting any prior SUMMARY/REVIEW/REVIEW-FIX claim):

| Suite | Command | Result | Status |
|-------|---------|--------|--------|
| ruff | `ruff check packages/dataplat/src tests` | All checks passed! | PASS |
| mypy | `mypy packages/dataplat/src packages/csv-processor/src` | Success: no issues found in 72 source files | PASS |
| unit + property + policy | `pytest tests/unit tests/property tests/policy -q -m "not integration and not dagtest and not cluster and not manifests"` | 622 passed, 12 deselected | PASS |
| integration (testcontainers) | `pytest tests/integration -q` | 117 passed | PASS |
| integration, targeted (backfill/wiring/referential) | `pytest tests/integration/test_backfill_resolution.py tests/integration/test_publish_transaction_wiring.py tests/integration/test_referential_integrity.py -v` | 10/10 passed, 0 skipped | PASS |
| unit + integration combined (SUMMARY's own claimed command) | `pytest tests/unit tests/integration -m "not cluster" -q` | 609 passed | PASS (matches SUMMARY exactly) |
| policy | `pytest tests/policy -q` | 134 passed | PASS |
| CR-01 regression test, reproduced against PRE-FIX code (isolated `git worktree` @ `a4c004d^`, current test file, `PYTHONPATH` override) | `pytest .../test_staged_but_conflict_guard_blocked_business_key_stays_pending -v` | `AssertionError: assert 'REDRIVEN' == 'PENDING'` | FAILS as claimed — confirms non-tautological |
| Same test against current (fixed) code | same | `1 passed` | PASS |
| `make check` (Local gate: lint/format/typecheck/imports/policy/test/fixtures-verify) | `make check` | 492 passed, 71/71 corpus fixtures verified | PASS |

### Live-Cluster State (independently confirmed by this verifier, this session)

```
$ kubectl -n data exec analytics-db-1 -- psql -U postgres -d analytics -c "SELECT version_num FROM meta.alembic_version"
 version_num
-------------
 0020
```

```
$ kubectl -n data exec analytics-db-1 -- psql -U postgres -d analytics -c "\d meta.rejected_records" | grep business_key
 business_key       | text                     |           |          |
    "ix_rejected_records_business_key_resolution" btree (business_key, resolution_type)
```

```
$ kubectl -n data exec analytics-db-1 -- psql -U postgres -d analytics -c "SELECT resolution_type, count(*) FROM meta.rejected_records GROUP BY resolution_type"
 resolution_type | count
------------------+-------
 PENDING          |     8
 REDRIVEN         |     2
```
Was `PENDING: 5, REDRIVEN: 0` at the prior verification — the resolution mechanism has now fired live, twice, against real content-differing-correction data.

```
$ kubectl -n airflow exec deploy/airflow-api-server -- airflow dags list | grep -E "csv_ingest_(customers|orders)"
csv_ingest_customers | ...
csv_ingest_orders    | ...
```

```
$ kubectl -n airflow exec deploy/airflow-api-server -- airflow variables get csv_processor_image
localhost:5001/csv-processor:4a2ded0
```
`4a2ded0` is the WR-01-fix commit (which itself follows the CR-01-fix commit `a4c004d`) — confirms the currently-deployed image includes both of this round's code-review fixes, not a stale pre-fix build.

```
$ kubectl -n vault exec vault-0 -- vault status | grep Sealed
Sealed    false
```

### Requirements Coverage

| Requirement | Source Plan(s) | Description | Status | Evidence |
|-------------|----------------|--------------|--------|----------|
| VALID-01 | 08-01, 08-04, 08-10 | Structural validation | SATISFIED | Unchanged. REQUIREMENTS.md still shows `[ ]`/"Pending" — stale doc, confirmed again this session. |
| VALID-02 | 08-01, 08-04, 08-07, 08-10, 08-11 | Completeness/uniqueness/validity-range/pattern/referential | SATISFIED | Unchanged. |
| VALID-03 | 08-01, 08-04, 08-07, 08-10 | Quarantine per configurable strategy | SATISFIED | Unchanged. |
| VALID-04 | 08-01, 08-03, 08-11 | Machine-readable reports in Postgres + MinIO | SATISFIED | Unchanged. REQUIREMENTS.md stale, confirmed again. |
| VALID-07 | 08-05, 08-08, 08-12, 08-14 | Referential integrity, configurable fail/quarantine/warn | SATISFIED | Unchanged, live-proven per prior verification, re-confirmed unaffected by this round. |
| VALID-08 | 08-03, 08-12, 08-13, 08-14, 08-15, 08-16, 08-17, 08-18 | Documented re-drive path after correction | **SATISFIED (gap closed this round)** | Was `NOT SATISFIED` at the prior verification (live-confirmed batch_id/batch_key architecture mismatch). Now closed at schema, extraction, resolution-matching, correctness-hardening (CR-01), and live-cluster levels — see Truth #3 above. REQUIREMENTS.md still shows `[ ]`/"Pending" — stale doc (see note below), should now be updated to `[x]`. |
| VALID-09 | 08-01, 08-09, 08-11 | Volume/quality anomalies against persisted baselines, no ML | SATISFIED | Unchanged. |
| LOAD-10 | 08-02, 08-12 | File integrity verified before processing | SATISFIED | Unchanged. REQUIREMENTS.md stale, confirmed again. |
| LOAD-11 | 08-01, 08-06 | Optional `_BATCH_COMPLETE` manifest support | SATISFIED | Unchanged. REQUIREMENTS.md stale, confirmed again. |

**Note on REQUIREMENTS.md staleness (unchanged pattern from prior verification, now includes VALID-08):** 5 of 9 phase-08 requirement IDs (VALID-01, VALID-04, VALID-08, LOAD-10, LOAD-11) are still shown `[ ]`/"Pending" in `.planning/REQUIREMENTS.md` despite substantive, tested, and — for VALID-08 specifically — now live-proven implementations. This is a documentation-lag housekeeping item, not a code gap; recommend updating REQUIREMENTS.md's checkboxes and the phase-mapping table's "Pending"→"Complete" for these 5 rows (VALID-08 in particular, since this round specifically closed it) as a follow-up.

**Orphaned requirements:** None. All 9 requirement IDs for Phase 8 are claimed by at least one plan (08-01 through 08-18).

### Anti-Patterns Found

No `TBD`/`FIXME`/`XXX`/`TODO`/`HACK`/`PLACEHOLDER` markers found in any file touched by this gap-closure round (migrations/0020, `models/record.py`, `metadata/{repository,postgres}.py`, `pipeline/run.py`, `load/publish/{protocol,merge,merge_orders}.py`, `config/model.py`, `validate/{completeness,pattern,validity_range,uniqueness,referential}.py`, `load/staging.py`). No stub implementations found.

**Deliberately-left-open items from 08-REVIEW.md, still deferred, non-blocking:** WR-02 (a fourth `_extract_business_key`/`_reconstruct_raw_line` duplication across the four rule files) was explicitly evaluated and left unfixed by the review-fix pass as a documented, deliberate DRY-not-correctness tradeoff — reasonable judgment call, does not affect any of this phase's observable truths. IN-01 (`get_or_create_dataset` commits on its own connection, outside the publish transaction) is informational-severity in the original review and was excluded from the review-fix pass's scope by design (`fix_scope=critical_warning`) — harmless in practice (idempotent, content-free bookkeeping row), noted for completeness, not a phase-blocking gap.

Carried forward from the original phase's own review (unchanged, non-blocking): WR-01/WR-02/WR-03 from the ORIGINAL 08-REVIEW.md (a metric-accuracy concern, a dormant-strategy-gap, and a dormant-type-cast concern) — none is exercised by either live dataset config today, none causes silent data loss.

### Human Verification Required

None. All prior human-verification items (the live `-m cluster` backfill-reentry proof) have been completed and are independently corroborated by this session's live-cluster query (`REDRIVEN: 2`, up from `0`) and the deployed image tag matching the code-review-fixed commit. No new items were identified.

### Gaps Summary

None. The single gap the prior verification confirmed FAILING live (VALID-08 / roadmap success criterion 3) is closed: schema (migration 0020), capture-at-reject-time (business_key extraction wired into every real rejection path), resolution-matching (business-key-scoped, not batch-scoped), a genuine correctness bug this round's own code review caught before it could cause silent false-positive resolutions (CR-01, independently reproduced as a real, non-tautological regression), and a config-cardinality guard (WR-01) are all independently confirmed present, tested, and — for the live-cluster claim specifically — corroborated by this session's own direct query of the running platform.

**Recommendation:** Phase 8 is complete. All 9 requirement IDs are satisfied with codebase evidence; all 5 roadmap success criteria are VERIFIED. The only follow-up item (non-blocking) is updating REQUIREMENTS.md's stale `[ ]` checkboxes for VALID-01/VALID-04/VALID-08/LOAD-10/LOAD-11 to `[x]`/"Complete" — a documentation task, not a code change.

---

_Verified: 2026-08-18T02:00:00Z_
_Verifier: Claude (gsd-verifier)_
