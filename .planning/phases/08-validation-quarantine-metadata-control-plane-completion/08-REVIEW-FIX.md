---
phase: 08-validation-quarantine-metadata-control-plane-completion
fixed_at: 2026-08-17T23:01:43Z
review_path: .planning/phases/08-validation-quarantine-metadata-control-plane-completion/08-REVIEW.md
iteration: 1
findings_in_scope: 2
fixed: 2
skipped: 1
status: partial
---

# Phase 08: Code Review Fix Report

**Fixed at:** 2026-08-17T23:01:43Z
**Source review:** .planning/phases/08-validation-quarantine-metadata-control-plane-completion/08-REVIEW.md
**Iteration:** 1

**Summary:**
- Findings in scope (fix_scope=critical_warning -- CR-*/BL-*/WR-* only, IN-01 excluded): 2 counted findings (CR-01, WR-01) fixed; WR-02 evaluated and intentionally left unfixed (see Skipped Issues).
- Fixed: 2 (CR-01, WR-01)
- Skipped: 1 (WR-02, by explicit judgment call per this run's instructions, not a failed-fix rollback)

## Fixed Issues

### CR-01: A staged-but-not-actually-published business key still resolves its PENDING reject

**Files modified:** `packages/dataplat/src/dataplat/load/publish/protocol.py`, `packages/dataplat/src/dataplat/load/publish/merge.py`, `packages/dataplat/src/dataplat/load/publish/merge_orders.py`, `packages/dataplat/src/dataplat/pipeline/run.py`, `tests/unit/test_run_ingest_trace.py`, `tests/integration/test_publish_transaction_wiring.py`
**Commit:** `a4c004d`
**Applied fix:** Followed the review's suggested direction, verified against current code rather than the review's cited line numbers (which had drifted slightly):

- Added `PublishResult.published_business_keys: tuple[str, ...] = ()` to the `Publisher` protocol (`protocol.py`), documenting why it exists (a business key that merely staged is not the same as one a publish statement actually wrote).
- Added `RETURNING customer_id` / `RETURNING order_id` to `MergePublisher`/`OrdersMergePublisher`'s `_PUBLISH_SQL`, and both `publish()` methods now populate `published_business_keys` from the `RETURNING` rows the `INSERT ... ON CONFLICT` statement actually surfaced (verified via a throwaway Postgres container that `RETURNING` correctly excludes a "locked but unchanged" conflict-guard-blocked row).
- `run.py`'s `_apply_post_publish_barriers_and_persist` now takes `published_business_keys` as a parameter (sourced from `publisher.publish()`'s own result at the `run_ingest` call site) instead of re-deriving it via a blind `SELECT DISTINCT` over the staging table. The now-dead SELECT and `business_key_column` lookup were removed from `run.py`.
- Updated `tests/unit/test_run_ingest_trace.py`'s `_FakePublishResult` to expose the new field (default `()`), since `run.py` now reads `result.published_business_keys` unconditionally.
- Added a new integration test, `test_staged_but_conflict_guard_blocked_business_key_stays_pending`, to `tests/integration/test_publish_transaction_wiring.py` (the file's own established "Test C/C2" convention), reproducing the review's exact scenario: a PENDING reject for a business key that later becomes "locked but unchanged" by `MergePublisher`'s conflict-guard `WHERE` clause during a real `run_ingest` publish. **Confirmed the test fails against the pre-fix code** (`git stash` of the fix commit's files, rerun -> `AssertionError: assert 'REDRIVEN' == 'PENDING'`) and passes with the fix restored -- a genuine regression test, not a tautology.
- Verification: `ruff check` clean, `mypy` clean (0 errors across the touched files and the full `packages/dataplat/src` / `packages/csv-processor/src` tree), and the full `pytest tests/unit tests/integration -m "not cluster"` suite (609 tests) passes, including all pre-existing publish/backfill-resolution/referential-integrity integration tests.

This is a correctness fix to code that resolves quarantine/reject state -- not a pure logic-error classification in the reviewer's sense (it was a data-flow/source-of-truth bug: reading the wrong table for "what did we actually publish"), and it is now covered by a test that fails on the old behavior and passes on the new. No further human logic verification flag is applied, but reviewing the new test's scenario construction (documented at length in its own docstring, including why the "already published, newer" row is seeded directly via SQL rather than through a second `run_ingest` call) is still worthwhile before considering this fully closed.

### WR-01: Business-key column resolution silently picks the first match, with no cardinality guard

**Files modified:** `packages/dataplat/src/dataplat/config/model.py`, `tests/unit/test_dataset_config_columns.py`
**Commit:** `4a2ded0`
**Applied fix:** Added `DatasetConfig._check_at_most_one_business_key_column`, a `model_validator(mode="after")` rejecting more than one `columns[].business_key: true` entry, following the review's suggested shape closely (adapted the error message slightly and added a fuller docstring explaining which two call sites -- `staging.py`'s `_build_quality_stages` and `run.py`'s `_apply_post_publish_barriers_and_persist` -- the guard protects). Added `test_dataset_config_rejects_more_than_one_business_key_column` to `tests/unit/test_dataset_config_columns.py`, matching that file's existing per-validator test convention (one `pytest.raises` test per failure mode). Verified the two real dataset configs (`customers.yaml`, `orders.yaml`, both single-column business keys) still pass validation via the existing full config-test run. `ruff`/`mypy` clean; the file's own 8-test suite (7 pre-existing + 1 new) passes.

## Skipped Issues

### WR-02: `_extract_business_key`/`_reconstruct_raw_line` duplicated a fourth time

**File:** `packages/dataplat/src/dataplat/validate/completeness.py:153-171`, `packages/dataplat/src/dataplat/validate/pattern.py:154-172`, `packages/dataplat/src/dataplat/validate/uniqueness.py:162-180`, `packages/dataplat/src/dataplat/validate/validity_range.py:179-197`
**Reason:** Judgment call per this run's explicit instructions ("use your own judgment... apply it if low-risk, or leave it with a note if the existing convention is intentional enough"). Left unfixed for two reasons: (1) the review's own finding text states this duplication is "a deliberate, documented pattern rather than an oversight" -- each of the four module docstrings explicitly justifies it as "mirroring this codebase's own established convention" (per the review), so consolidating now would run against a pattern the codebase has repeatedly, deliberately chosen across multiple prior phases, not something introduced carelessly this round. (2) This is a maintainability/DRY concern with zero behavioral or correctness impact today -- unlike CR-01/WR-01, there is no reachable bug here, so the risk/benefit of touching four validated, tested modules (and inventing a new shared module, with attendant import-linter/dependency-direction considerations this codebase is otherwise careful about -- see `pyproject.toml`'s explicit workspace-membership comments) to fix a non-bug did not clear the bar for this fix pass. Recommend addressing as a dedicated refactor (with its own plan/task, so `import-linter` boundaries and the four call sites' test coverage can be checked deliberately) rather than folding it into a review-fix pass.
**Original issue:** See REVIEW.md's WR-02 section for full text -- in summary, `_extract_business_key`/`_reconstruct_raw_line` now exist as 8 near-identical private-helper copies across 4 validation-rule modules; a future bug fix to either helper must be applied 4 times.

---

_Fixed: 2026-08-17T23:01:43Z_
_Fixer: Claude (gsd-code-fixer)_
_Iteration: 1_
