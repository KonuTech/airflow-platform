---
phase: 08-validation-quarantine-metadata-control-plane-completion
reviewed: 2026-08-18T00:00:00Z
depth: standard
files_reviewed: 18
files_reviewed_list:
  - migrations/versions/0020_meta_rejected_records_business_key.py
  - packages/dataplat/src/dataplat/load/staging.py
  - packages/dataplat/src/dataplat/metadata/postgres.py
  - packages/dataplat/src/dataplat/metadata/repository.py
  - packages/dataplat/src/dataplat/models/record.py
  - packages/dataplat/src/dataplat/pipeline/run.py
  - packages/dataplat/src/dataplat/validate/completeness.py
  - packages/dataplat/src/dataplat/validate/pattern.py
  - packages/dataplat/src/dataplat/validate/referential.py
  - packages/dataplat/src/dataplat/validate/uniqueness.py
  - packages/dataplat/src/dataplat/validate/validity_range.py
  - tests/e2e/slice/test_backfill_reentry.py
  - tests/integration/test_backfill_resolution.py
  - tests/integration/test_publish_transaction_wiring.py
  - tests/integration/test_referential_integrity.py
  - tests/unit/test_run_ingest_trace.py
  - tests/unit/validate/test_quality_rules.py
  - tests/unit/validate/test_uniqueness.py
findings:
  critical: 1
  warning: 2
  info: 1
  total: 4
status: issues_found
---

# Phase 08: Code Review Report (Gap-Closure Round: plans 08-16/08-17/08-18)

**Reviewed:** 2026-08-18T00:00:00Z
**Depth:** standard
**Files Reviewed:** 18
**Status:** issues_found

## Summary

This round replaced `resolve_rejected_records_for_batch` with
`resolve_rejected_records_for_business_keys`, threaded a new
`business_key`/`business_key_index` value through `RejectedRecord`,
`StagingLoader`, all four row-scoped quality rules, `ReferentialIntegrityBarrier`,
and `run_ingest`'s post-publish resolution call, and added migration 0020
(`meta.rejected_records.business_key`). The live E2E proof
(`test_backfill_reentry.py -m cluster`) genuinely exercises the intended
happy-path scenario (a completeness-violation reject, corrected under a new
`content_sha256`/`batch_id`, resolving via the new business-key predicate),
and the integration-test matrix for `resolve_rejected_records_for_business_keys`
itself (dataset scoping, NULL-never-matches, idempotency) is thorough and
convincing.

The one significant gap this review found is a genuine correctness mismatch
between what the resolution call *claims* to do (resolve rejects "by the
business key THIS run actually published", per `run.py`'s own comment) and
what it *actually* does (resolve rejects by the business key THIS run
*staged*, regardless of whether the publish statement's own conflict-guard
actually wrote/updated that row). This is untested by the new test suite and
is a real, reachable false-positive-resolution path. Two smaller design gaps
(silent single-column business-key selection, and a further round of
duplicated per-file helpers) are also noted.

## Critical Issues

### CR-01: A staged-but-not-actually-published business key still resolves its PENDING reject

**File:** `packages/dataplat/src/dataplat/pipeline/run.py:430-447`
**Also relevant (unchanged, but load-bearing context):**
`packages/dataplat/src/dataplat/load/publish/merge.py:50-71` and
`packages/dataplat/src/dataplat/load/publish/merge_orders.py:63-85`

**Issue:** `_apply_post_publish_barriers_and_persist` computes
`published_business_keys` by running `SELECT DISTINCT <business_key_column>
FROM <staging_table> WHERE <business_key_column> IS NOT NULL` — i.e. it reads
whatever survived streaming validation and landed in the **staging** table.
It then calls `resolve_rejected_records_for_business_keys` with that list,
which resolves every matching `PENDING` reject to `REDRIVEN`. The code's own
comment (lines 403-405) claims this resolves rejects "by the business key
THIS run actually **published**" — but nothing here checks whether the
business key was actually inserted/updated in the target table.

Both concrete `Publisher` implementations in this codebase have a
conflict-guard `WHERE` clause on their `ON CONFLICT DO UPDATE` that can
silently leave a conflicting row "locked but unchanged" — excluded from
`cursor.rowcount` — whenever the incoming staged row does not actually
improve on what's already published:

```sql
-- merge.py (customers)
 WHERE normalized.customers._record_hash IS DISTINCT FROM EXCLUDED._record_hash
   AND EXCLUDED.event_ts >= normalized.customers.event_ts

-- merge_orders.py (orders)
 WHERE normalized.orders._record_hash IS DISTINCT FROM EXCLUDED._record_hash
   AND (normalized.orders.order_date IS NULL
        OR EXCLUDED.order_date >= normalized.orders.order_date)
```

Concrete reproduction: a row for `customer_id=100` is rejected in batch B1
for an unrelated reason (e.g. a `PATTERN_VIOLATION`), with `event_ts=T2`,
`business_key="100"` recorded `PENDING`. Independently, a *different*,
unrelated, legitimately-newer row for `customer_id=100` (`event_ts=T3 > T2`)
is later published from a different batch. An operator now uploads a
"corrected" file (new content, new `content_sha256`, new `batch_id`) that
fixes the pattern violation for the *original* `event_ts=T2` row. This row
now survives streaming validation and is staged — `business_key="100"`
appears in `published_business_keys`. At publish time, `MergePublisher`'s
`WHERE ... AND EXCLUDED.event_ts >= normalized.customers.event_ts` evaluates
`false` (`T2 < T3`), so the row is "locked but unchanged": **nothing is
written to `normalized.customers`**. `resolve_rejected_records_for_business_keys`
is nonetheless called with `"100"` in `business_keys`, and the original
`PENDING` reject flips to `REDRIVEN`, `resolved_by_run_id` pointing at a run
whose correction was silently no-op'd.

This directly contradicts this project's core value ("no data is ever
silently dropped, duplicated or corrupted" / "can be traced, explained,
reprocessed and trusted"): an operator inspecting `meta.rejected_records`
sees `REDRIVEN` and reasonably concludes the row is now correctly published,
when in fact the target table still holds the old (or no) data for that
business key. No test in this round's suite (`test_backfill_resolution.py`,
`test_publish_transaction_wiring.py`, `test_backfill_reentry.py`) exercises
this path — every seeded scenario relies on the target row not existing yet
(a plain `INSERT`, which is unconditional and always affects a row), so the
conflict-guard's interaction with resolution is entirely unexercised.

**Fix:** Scope `published_business_keys` to rows the publish statement
*actually* affected, not merely staged. One option: have each `Publisher`
return the set of business-key values it actually inserted/updated (e.g. add
a `RETURNING <business_key_column>` clause to `_PUBLISH_SQL` in both
`merge.py`/`merge_orders.py` and thread that result back through
`PublishResult`), and read `published_business_keys` from that instead of a
blind `SELECT DISTINCT` over the staging table:

```python
# merge.py
_PUBLISH_SQL = """
INSERT INTO normalized.customers (...)
SELECT DISTINCT ON (customer_id) ...
FROM   {staging_table}
ORDER  BY customer_id, event_ts DESC, _source_row_number DESC
ON CONFLICT (customer_id) DO UPDATE
   SET ...
 WHERE normalized.customers._record_hash IS DISTINCT FROM EXCLUDED._record_hash
   AND EXCLUDED.event_ts >= normalized.customers.event_ts
RETURNING customer_id
"""
```
and in `run.py`, use the returned keys (deduplicated) instead of
`staging_result.staging_table`'s own contents.

## Warnings

### WR-01: Business-key column resolution silently picks the first match, with no cardinality guard

**File:** `packages/dataplat/src/dataplat/load/staging.py:424-427` and
`packages/dataplat/src/dataplat/pipeline/run.py:430-433`

**Issue:** Both call sites resolve the dataset's business-key column via:

```python
business_key_column = next(
    (column for column in ctx.config.columns if column.business_key),
    None,
)
```

`ColumnContract.business_key` is a plain `bool` with no model-level validator
anywhere in `dataplat.config.model` enforcing "at most one `business_key:
true` column per dataset" (the only related validator,
`_check_deduplication_keys_are_business_key_columns`, checks the reverse
direction — that every `deduplication.keys` entry is itself marked
`business_key: true` — and says nothing about cardinality). A dataset config
that declares a composite business key (multiple columns marked
`business_key: true`, consistent with a multi-column
`deduplication.keys: [a, b]`) would pass config validation cleanly, but both
`_build_quality_stages` and `_apply_post_publish_barriers_and_persist` would
silently use only the *first* such column (in `ctx.config.columns` order)
for `business_key_index`/resolution scoping — the second key column is
silently dropped from both the reject's `RejectedRecord.business_key` and
the resolution predicate, with no error raised anywhere. This is exactly the
kind of "config typo becomes a silent, hard-to-diagnose outage" case
`CLAUDE.md`'s own Pydantic guidance (`extra="forbid"` reasoning) explicitly
tries to prevent elsewhere.

Today's two real dataset configs (`customers.yaml`, `orders.yaml`) both use
a single-column business key, so this does not currently manifest — but
nothing stops a future dataset config from silently mis-scoping resolution.

**Fix:** Add a `model_validator` on `DatasetConfig` rejecting more than one
`columns[].business_key: true` entry (or, if composite business keys are a
genuine future requirement, change `business_key_column`/`business_key_index`
to a tuple and compose the resolution predicate over all of them), e.g.:

```python
@model_validator(mode="after")
def _check_at_most_one_business_key_column(self) -> DatasetConfig:
    business_key_columns = [c.name for c in self.columns if c.business_key]
    if len(business_key_columns) > 1:
        msg = (
            f"columns: declares multiple business_key: true entries "
            f"{business_key_columns!r}; only a single-column business key is "
            "currently supported by resolve_rejected_records_for_business_keys"
        )
        raise ValueError(msg)
    return self
```

### WR-02: `_extract_business_key`/`_reconstruct_raw_line` duplicated a fourth time

**File:** `packages/dataplat/src/dataplat/validate/completeness.py:153-171`,
`packages/dataplat/src/dataplat/validate/pattern.py:154-172`,
`packages/dataplat/src/dataplat/validate/uniqueness.py:162-180`,
`packages/dataplat/src/dataplat/validate/validity_range.py:179-197`

**Issue:** This round adds a fourth (`_extract_business_key`) and, combined
with the pre-existing `_reconstruct_raw_line`, an eighth near-identical
private helper copy across these four files. The module docstrings
explicitly justify this as "mirroring this codebase's own established
convention," so this is a deliberate, documented pattern rather than an
oversight — but each copy is a place a future bug fix (e.g. a change to how
a non-`str`/non-numeric business-key value should be stringified) must be
applied four times, and this round increased that count rather than
consolidating it. Worth reconsidering before a fifth streaming rule adds a
ninth copy.

**Fix:** Consider hoisting `_extract_business_key`/`_reconstruct_raw_line`
into a shared module (e.g. `dataplat.validate._helpers`) now that four
call sites exist, while still respecting the codebase's existing convention
if there is a documented reason (e.g. avoiding cross-module coupling in this
package) not to.

## Info

### IN-01: `get_or_create_dataset` writes on its own connection, outside the publish transaction, unconditionally

**File:** `packages/dataplat/src/dataplat/pipeline/run.py:379`

**Issue:** `dataset_id = ctx.metadata.get_or_create_dataset(ctx.config.dataset)`
is now called unconditionally at the top of `_apply_post_publish_barriers_and_persist`
(previously only when a `VOLUME` quality rule was configured). `get_or_create_dataset`
opens and commits its own connection from the pool (`PostgresMetadataRepository.get_or_create_dataset`),
independent of the publish transaction's own `conn`. If a later step in the
same function raises (e.g. `RejectionRateCircuitBreaker` tripping via
`QualityThresholdExceeded`, `test_circuit_breaker_trip_leaves_zero_rows_for_this_run`'s
own scenario), the publish transaction rolls back everything else, but this
call has already committed independently. In practice this is harmless
(`meta.datasets` rows are idempotent, content-free bookkeeping, and the
dataset row already exists by this point in every real call path), so this
is informational rather than a correctness bug — noted for awareness since
it slightly widens the "everything commits atomically or nothing does"
claim made elsewhere in this module's own docstring.

---

_Reviewed: 2026-08-18T00:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
