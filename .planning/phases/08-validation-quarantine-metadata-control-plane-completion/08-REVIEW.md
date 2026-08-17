---
phase: 08-validation-quarantine-metadata-control-plane-completion
reviewed: 2026-08-17T11:12:58Z
depth: standard
files_reviewed: 63
files_reviewed_list:
  - airflow/dags/_common/integrity_gate.py
  - airflow/dags/csv_ingest_customers.py
  - airflow/dags/csv_ingest_orders.py
  - configs/datasets/customers.yaml
  - configs/datasets/orders.yaml
  - migrations/versions/0014_meta_validation_results.py
  - migrations/versions/0015_meta_rejected_records.py
  - migrations/versions/0016_normalized_orders.py
  - migrations/versions/0017_normalized_orders_business_key_unique.py
  - packages/csv-processor/src/csv_processor/cli.py
  - packages/dataplat/src/dataplat/config/model.py
  - packages/dataplat/src/dataplat/discovery.py
  - packages/dataplat/src/dataplat/errors.py
  - packages/dataplat/src/dataplat/load/publish/merge_orders.py
  - packages/dataplat/src/dataplat/load/publish/registry.py
  - packages/dataplat/src/dataplat/load/staging.py
  - packages/dataplat/src/dataplat/metadata/postgres.py
  - packages/dataplat/src/dataplat/metadata/repository.py
  - packages/dataplat/src/dataplat/models/receipt.py
  - packages/dataplat/src/dataplat/models/report.py
  - packages/dataplat/src/dataplat/pipeline/run.py
  - packages/dataplat/src/dataplat/validate/__init__.py
  - packages/dataplat/src/dataplat/validate/circuit_breaker.py
  - packages/dataplat/src/dataplat/validate/completeness.py
  - packages/dataplat/src/dataplat/validate/pattern.py
  - packages/dataplat/src/dataplat/validate/referential.py
  - packages/dataplat/src/dataplat/validate/registry.py
  - packages/dataplat/src/dataplat/validate/strategy_dispatch.py
  - packages/dataplat/src/dataplat/validate/uniqueness.py
  - packages/dataplat/src/dataplat/validate/validity_range.py
  - packages/dataplat/src/dataplat/validate/volume_anomaly.py
  - tests/dagtest/__init__.py
  - tests/dagtest/conftest.py
  - tests/dagtest/test_backfill_dagrun.py
  - tests/e2e/slice/test_backfill_reentry.py
  - tests/e2e/slice/test_referential_orphan.py
  - tests/integration/conftest.py
  - tests/integration/test_backfill_resolution.py
  - tests/integration/test_lineage_view.py
  - tests/integration/test_migrations.py
  - tests/integration/test_publish_orders.py
  - tests/integration/test_publish_transaction_wiring.py
  - tests/integration/test_referential_integrity.py
  - tests/integration/test_run_ingest.py
  - tests/integration/test_staging_quality_rules.py
  - tests/integration/test_validation_persistence.py
  - tests/integration/test_volume_anomaly.py
  - tests/policy/test_dag_thinness.py
  - tests/property/test_quality_rules_never_raise.py
  - tests/unit/test_assignment_document.py
  - tests/unit/test_csv_processor_cli.py
  - tests/unit/test_dag_structure.py
  - tests/unit/test_integrity_gate.py
  - tests/unit/test_publisher_registry.py
  - tests/unit/test_quality_config.py
  - tests/unit/test_run_ingest_trace.py
  - tests/unit/validate/__init__.py
  - tests/unit/validate/test_batch_complete_marker.py
  - tests/unit/validate/test_circuit_breaker.py
  - tests/unit/validate/test_quality_rules.py
  - tests/unit/validate/test_strategy_dispatch.py
  - tests/unit/validate/test_structural_rules.py
  - tests/unit/validate/test_uniqueness.py
  - tests/unit/validate/test_volume_anomaly.py
findings:
  critical: 1
  warning: 4
  info: 0
  total: 5
status: issues_found
addendum:
  reviewed: 2026-08-17T00:00:00Z
  scope: gap-closure (plan 08-15, single file)
  findings:
    critical: 0
    warning: 3
    info: 2
    total: 5
  all_resolved: true
  resolved_commit: cb56e15
---

# Phase 08: Code Review Report

**Reviewed:** 2026-08-17T11:12:58Z
**Depth:** standard
**Files Reviewed:** 62
**Status:** issues_found

## Summary

This phase wires four new `BarrierStage`/`StreamingStage` validation rules
(circuit breaker, uniqueness, referential integrity, volume anomaly) into a
generic `StrategyDispatchStage` wrapper, adds a real `MetadataRepository`
persistence path for `meta.validation_results`/`meta.rejected_records`, brings
up `orders` as a second live dataset through the same pipeline, and adds an
Airflow-side pre-pod-launch file-integrity gate plus corresponding DAG
wiring. The individual rule classes (`CompletenessRule`, `PatternRule`,
`ValidityRangeRule`, `UniquenessRule`, `RejectionRateCircuitBreaker`,
`ReferentialIntegrityBarrier`) are each well-isolated, `QUAL-03`-compliant
(no row-level exception ever escapes `apply()`), and reasonably tested in
isolation, including a property-test pass over adversarial inputs.

The most serious problem is in the orchestration layer that ties these rules
together in `pipeline/run.py`: the very same transaction that inserts a run's
own `meta.rejected_records` rows immediately re-resolves them as `REDRIVEN`
(i.e. "fixed by a later backfill run") in the identical `batch_id` scope,
because `resolve_rejected_records_for_batch` is called unconditionally right
after `record_rejected_records` with no filter excluding the rows just
written by the current run. This directly undermines the phase's own
D-04/D-05 PENDING/REDRIVEN lifecycle contract and the platform's stated core
value around traceable, trustable quarantine — a query for `resolution_type
= 'PENDING'` will never surface a record that was rejected by the very run
that is currently succeeding. No existing test (unit, integration, or e2e)
asserts on `resolution_type` for a run's own freshly-rejected records, so
this regressed silently.

Three further, lower-severity issues were found: a metric-conflation bug in
`rows_deduplicated` once a referential barrier deletes orphan rows from
staging before publish; an incomplete strategy-to-outcome mapping in
`VolumeAnomalyBarrier`; and a `::int` cast on a business key declared
`type: string` in the dataset config, extended into new SQL this phase.

**Resolution status (updated 2026-08-17, see Addendum below):** CR-01 and
WR-04 were fixed directly during phase close (commits `9731192`, `e7bf450`
— see `deferred-items.md`). WR-01/WR-02/WR-03 remain deliberately deferred
(each requires a design decision not exercised by any live config today —
see `deferred-items.md`).

## Critical Issues

### CR-01: A run's own newly-rejected records are immediately marked "REDRIVEN" by the same run

**File:** `packages/dataplat/src/dataplat/pipeline/run.py:398-417`

**Issue:**

`_apply_post_publish_barriers_and_persist` does, in order, inside the SAME
open transaction (`conn`) and the SAME `batch_id`:

```python
ctx.metadata.record_validation_results(conn=conn, run_id=run_id, results=all_findings)
if all_rejected:
    ctx.metadata.record_rejected_records(
        conn=conn,
        run_id=run_id,
        file_id=file_id,
        batch_id=batch_id,
        rejected=all_rejected,
    )
# D-05: unconditional, never gated behind `all_rejected`/an `if` -- a
# batch's PENDING rows may belong to a PRIOR run, not this run's own
# rejections. ...
ctx.metadata.resolve_rejected_records_for_batch(
    conn=conn,
    batch_id=batch_id,
    resolved_by_run_id=run_id,
    resolution_type="REDRIVEN",
)
```

`record_rejected_records` inserts every row this run itself just rejected
(`REJECT_RECORD`/`QUARANTINE_RECORD` streaming violations, plus
`REFERENTIAL_ORPHAN` rows from `_apply_referential_barrier`) with
`resolution_type` defaulted to `'PENDING'` (migration 0015's column
default; `PostgresMetadataRepository.record_rejected_records` never sets
this column, per its own docstring). `resolve_rejected_records_for_batch`'s
own SQL is:

```sql
UPDATE meta.rejected_records
   SET resolution_type = %s, resolved_by_run_id = %s
 WHERE batch_id = %s AND resolution_type = 'PENDING'
```

Because this runs against the SAME `batch_id` inside the SAME transaction
that just inserted this run's own rows as `PENDING`, those brand-new rows
match the `WHERE resolution_type = 'PENDING'` predicate and get flipped to
`resolution_type='REDRIVEN', resolved_by_run_id=<this run's own run_id>` in
the same statement — i.e. a record is recorded as having been "resolved via
a backfill run completing" (per `MetadataRepository.
resolve_rejected_records_for_batch`'s own docstring) by the exact run that
rejected it, before any backfill has ever happened.

This defeats the entire PENDING/REDRIVEN/DISCARDED lifecycle this phase
built (D-04/D-05): an operator or dashboard filtering
`meta.rejected_records WHERE resolution_type = 'PENDING'` to find rows that
still need attention will never see a record rejected by a currently
(first-time) SUCCEEDED run — it looks pre-resolved. It also corrupts
`resolved_by_run_id`'s stated lineage meaning ("was this row ever fixed, and
by which run" — `repository.py:604-620`): it now sometimes points at the
same run that created the row, which is not a fix.

The docstring comment above the call ("a batch's PENDING rows may belong to
a PRIOR run") only reasons about resolving a *prior* run's leftover PENDING
rows; it does not account for the fact that this SAME call also sweeps up
rows this run inserted a few lines above it, in the same transaction and
batch scope.

**Why this was not caught by the test suite:** `tests/integration/
test_publish_transaction_wiring.py::test_quarantine_under_threshold_succeeds_and_persists_both`
exercises exactly this path (a run that both rejects rows of its own AND
succeeds) but its `_fetch_rejected_records` helper only selects
`source_row_number, error_type, source_row_number` — it never reads
`resolution_type`, so the bug is invisible to that assertion.
`test_backfill_run_resolves_the_batch_pending_rejects` (same file) and
`tests/integration/test_backfill_resolution.py` only ever seed PENDING rows
via a *separate, prior* run and then run a *second, all-good* run as the
"backfill" — they never assert on the resolution state of a run's own
rejects in the same transaction.

**Fix:**

Exclude the current run's own just-inserted rows from the resolve call —
either by resolving strictly-older PENDING rows only, or by reordering so
the resolve happens *before* `record_rejected_records` inserts this run's
own rows (still inside the same transaction so a rollback still undoes
both consistently):

```python
# Resolve any PRE-EXISTING PENDING rows for this batch (from a prior,
# failed run) BEFORE inserting this run's own rejects, so this run's own
# rows can never be swept up by the same predicate.
ctx.metadata.resolve_rejected_records_for_batch(
    conn=conn,
    batch_id=batch_id,
    resolved_by_run_id=run_id,
    resolution_type="REDRIVEN",
)
ctx.metadata.record_validation_results(conn=conn, run_id=run_id, results=all_findings)
if all_rejected:
    ctx.metadata.record_rejected_records(
        conn=conn,
        run_id=run_id,
        file_id=file_id,
        batch_id=batch_id,
        rejected=all_rejected,
    )
```

Add an integration test asserting that a run which BOTH rejects rows of its
own AND succeeds leaves those rows' `resolution_type = 'PENDING'` (not
`REDRIVEN`), while a genuinely later backfill run over the same batch still
correctly flips prior-run PENDING rows to `REDRIVEN`.

## Warnings

### WR-01: `rows_deduplicated` conflates referential-orphan quarantine with genuine deduplication

**File:** `packages/dataplat/src/dataplat/pipeline/run.py:734`

**Issue:** `rows_deduplicated = max(staging_result.rows_parsed - result.rows_affected, 0)`.
For `orders`, `_apply_referential_barrier` (called at `run.py:655-661`, before
`publisher.publish()`) `DELETE`s every orphan row from the staging table
before publish runs. `staging_result.rows_parsed` was computed earlier by
`StagingLoader.load()` and still counts those now-deleted orphan rows, so
`result.rows_affected` (from the orders publisher, which reads the
post-deletion staging table) is now smaller for TWO different reasons that
get collapsed into one number: genuine `ON CONFLICT`-collapsed duplicates,
and rows removed entirely because they were quarantined as referential
orphans (which are already correctly counted separately via
`receipt.rows_quarantined`/`meta.rejected_records`). The existing in-code
comment ("This phase does not separately track collapsed by DISTINCT ON ...
from suppressed as a no-op write by the WHERE guard") only anticipated the
`customers`-only case and does not account for the referential-barrier
deletion this same phase adds for `orders`.

**Fix:** Compute `rows_deduplicated` from a value that excludes rows already
accounted for via `all_rejected`, e.g. subtract `len(referential_rejected)`
from the parsed count before diffing, or track "rows submitted to publish"
(post-barrier-deletion) separately from `rows_parsed` so the arithmetic only
reflects genuine merge-time collapsing:

```python
rows_submitted_to_publish = staging_result.rows_parsed - len(referential_rejected)
rows_deduplicated = max(rows_submitted_to_publish - result.rows_affected, 0)
```

### WR-02: `VolumeAnomalyBarrier` has no mapping for the `REJECT_RECORD` strategy

**File:** `packages/dataplat/src/dataplat/validate/volume_anomaly.py:56-61, 176`

**Issue:** `_STRATEGY_TO_OUTCOME` maps only `QUARANTINE_FILE`, `QUARANTINE_RECORD`,
`FAIL_FILE`, `WARN_AND_CONTINUE`. D-07's strategy set has a fifth value,
`REJECT_RECORD`, which is a legal `QualityRuleConfig.strategy` value
(`config/model.py:406-409` documents all five as valid for any rule type).
If a future dataset config declares a `VOLUME` rule with
`strategy: REJECT_RECORD`, `_STRATEGY_TO_OUTCOME.get(self._strategy,
"FAIL")` silently falls back to `"FAIL"` — the most severe outcome — even
though `REJECT_RECORD` is normally the least severe, row-scoped strategy
elsewhere in this codebase (`strategy_dispatch.py`'s own
`_PASSTHROUGH_STRATEGIES`). This is untested (`tests/unit/validate/
test_volume_anomaly.py` only covers `QUARANTINE_FILE`/`FAIL_FILE`/
`WARN_AND_CONTINUE`) and unused by both live dataset configs today, so it is
currently dormant, but it is a real correctness trap for the next dataset
that adds a `VOLUME` rule.

**Fix:** Either add an explicit `"REJECT_RECORD": "QUARANTINE"` (or similar)
entry to `_STRATEGY_TO_OUTCOME`, or raise `ConfigurationError` at
construction time (mirroring `StrategyDispatchStage.__init__`'s own
fail-fast validation) for a `VolumeAnomalyBarrier` strategy outside a known,
deliberately-supported set, rather than silently defaulting to `"FAIL"`.

### WR-03: Referential anti-join hardcodes `customer_id::int` against a column declared `type: string`

**File:** `packages/dataplat/src/dataplat/validate/referential.py:49-55` (new this phase); also `packages/dataplat/src/dataplat/load/publish/merge_orders.py:58-59` (pre-existing pattern extended)

**Issue:** `orders.yaml`/`customers.yaml` both declare `customer_id`/
`order_id` as `type: string` (`configs/datasets/orders.yaml:86-89`,
`configs/datasets/customers.yaml:99-104`) — the platform's own stated
principle (STACK.md §F, referenced throughout this codebase's docstrings) is
that a string identifier column must never be silently treated as numeric
(the canonical "`001234` must not become `1234`" example). `_ANTI_JOIN_SQL`
casts the staging column with `s.{staging_column}::int` unconditionally.
Although `normalized.customers.customer_id`/`normalized.orders.order_id`/
`customer_id` happen to be `sa.Integer()` columns today (migrations 0005,
0016), the dataset config's own declared `type: string` says this column's
values are not guaranteed to be integer-parseable, and a value with leading
zeros or non-digit characters would either raise a runtime error inside the
barrier's anti-join query (`invalid input syntax for type integer`,
aborting the whole run rather than the intended row-level
`REFERENTIAL_ORPHAN` classification) or, for a numeric-but-zero-padded ID,
silently compare unequal/equal in ways inconsistent with true string
equality. This phase's own `ReferentialIntegrityBarrier` is new code
extending a fragile, pre-existing assumption into a second SQL statement.

**Fix:** Either declare `customer_id`/`order_id` as `type: integer` in the
dataset configs to make the contract match the actual DB column type and the
SQL that assumes it, or make the anti-join/publish SQL compare as text
(`s.{staging_column} = t.{target_column}::text`) so a genuinely
non-numeric business-key value degrades to an expected
`REFERENTIAL_ORPHAN`/publish-time rejection instead of a raw, run-aborting
database error.

### WR-04: `OrdersMergePublisher`'s `WHERE` guard silently blocks re-publication of a row with `NULL order_date`

**File:** `packages/dataplat/src/dataplat/load/publish/merge_orders.py:52-73`

**Issue:** `order_date` is declared `nullable: true` in
`configs/datasets/orders.yaml:94-98`. `_PUBLISH_SQL`'s `ON CONFLICT DO
UPDATE ... WHERE ... AND EXCLUDED.order_date >= normalized.orders.order_date`
uses a direct `>=` comparison against `order_date` with no `NULL`-safe
handling. In SQL, `NULL >= x` (and `x >= NULL`) evaluates to `NULL`, which
is treated as false in a `WHERE` clause. Consequently, once a row with
`order_date IS NULL` exists in `normalized.orders` (or a later revision
arrives with `order_date IS NULL`), any subsequent re-publish of the same
`order_id` — including a genuine correction with a real, non-NULL
`order_date`, or vice versa — is silently skipped ("locked but left
unchanged", per this class's own docstring) rather than applied, because
the `WHERE` guard never evaluates true when either side is `NULL`. This
contradicts the project's stated core value that no correction is ever
silently dropped.

**Fix:** Make the ordering comparison NULL-safe, e.g.
`EXCLUDED.order_date IS NOT DISTINCT FROM normalized.orders.order_date OR
EXCLUDED.order_date > normalized.orders.order_date OR
normalized.orders.order_date IS NULL`, or treat a `NULL order_date` as
"always apply" (since there is no ordering information to lose by
overwriting). Add a test seeding a `NULL order_date` row and asserting a
subsequent correction is actually applied.

---

_Reviewed: 2026-08-17T11:12:58Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_

## Addendum: Gap-Closure Review (Plan 08-15, 2026-08-17)

**Scope:** `tests/e2e/slice/test_backfill_reentry.py` only — the single file
modified by plan 08-15 to close `08-HUMAN-UAT.md` gap 2 (a bounded retry
around `airflow backfill create`, keyed on `backfill_dag_run.exception_reason`,
working around a confirmed, live-verified Airflow 3.3.0 row-lock race — see
`.planning/debug/backfill-does-not-redrive-rejected-row.md`).

**Findings:** 0 critical, 3 warning, 2 info — no correctness BLOCKER found
after tracing the fix against the debug log's live-verified evidence (stale
reads across retries, collision with Airflow's other `IN_FLIGHT` code path,
and a `logical_date` round-trip precision mismatch were all ruled out).

**Status: all 5 findings RESOLVED, commit `cb56e15`.**

### WR-05 (was WR-01): Settle loop could not distinguish "row not written yet" from "row written, succeeded"

`_fetch_latest_backfill_exception_reason` returned `None` for both "no
`backfill_dag_run` row yet" and "row exists, `exception_reason IS NULL`
(success)" — a delayed write would have silently been treated as success
with no diagnostic trail.

**Fix applied:** Replaced with `_fetch_latest_backfill_dag_run_row` →
`(row_found, exception_reason)`, extracted into a new
`_wait_for_backfill_dag_run_row` helper. The settle loop now breaks on
`row_found`, and raises a distinct, diagnosable `AssertionError` if no row
appears within the settle window instead of falling through silently.

### WR-06 (was WR-02): Retry-detection depended on exact `logical_date` equality with no diagnostic if that precondition silently broke

Same root cause and same fix as WR-05 — `_wait_for_backfill_dag_run_row`'s
explicit `row_found` check now surfaces a loud failure instead of a silent
no-op if the `logical_date` match ever stops finding a row.

### WR-07 (was WR-03): Retry only covered the DB-observed "in flight" signal, not kubectl/CLI-level transient failures

`_invoke_backfill_create_once` was called once per attempt but a failure
there was not itself retried, aborting the whole test on attempt 1
regardless of remaining budget.

**Fix applied:** The CLI invocation is now wrapped in the same
bounded-attempt retry loop (`try`/`except AssertionError`, same
`_BACKFILL_CREATE_MAX_ATTEMPTS`/`_BACKFILL_CREATE_RETRY_BACKOFF_SECONDS`
budget) as the DB-lock race.

### IN-01/IN-02: Bare asserts and unexplained settle-loop timing

**Fix applied:** Added descriptive messages to the two previously-bare
`assert` statements; the settle loop's defensive (not required) nature is
now documented inline in `_wait_for_backfill_dag_run_row`'s docstring.

**Verification:** `ruff check`, `mypy`, and `pytest --collect-only` all
clean on the modified file; full `make test` (484 tests) passes with no
regressions.

_Addendum reviewed: 2026-08-17T00:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
