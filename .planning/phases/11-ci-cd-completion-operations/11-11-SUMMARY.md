---
phase: 11-ci-cd-completion-operations
plan: 11
subsystem: database
tags: [postgresql, psycopg, reconciliation, scd2, checksum, dataclasses]

# Dependency graph
requires:
  - phase: 10-slowly-changing-dimensions
    provides: normalized.customers' SCD2 shape (migration 0035 -- valid_to/is_current/validity/excl_customers_business_key_validity)
  - phase: 09-source-to-target-reconciliation
    provides: "_table_checksum/_compute_silver_gold_reconciliation (packages/dataplat/src/dataplat/pipeline/run.py) and meta.reconciliation_results/record_reconciliation"
provides:
  - "_table_checksum's additive columns= keyword-only parameter (byte-for-byte backward compatible)"
  - "dataplat.pipeline.rebuild_reconciliation: TableSnapshot, ScdKeySnapshot, CustomersScd2Snapshot, RebuildComparisonResult dataclasses"
  - "snapshot_table_state(), snapshot_customers_scd2_state(), compare_snapshots() -- D-29 points 1-3 reconciliation arithmetic, proven correct in isolation"
affects: [11-12-rebuild-from-raw-orchestration]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Additive keyword-only parameter with a None default preserves an existing function's exact prior behavior while adding new caller-opt-in capability (columns: Sequence[str] | None = None)"
    - "Pure comparison function (compare_snapshots) accepting two same-shaped frozen dataclasses, returning a named-mismatch result rather than a bare boolean"

key-files:
  created:
    - packages/dataplat/src/dataplat/pipeline/rebuild_reconciliation.py
    - tests/integration/test_rebuild_reconciliation.py
    - .planning/phases/11-ci-cd-completion-operations/deferred-items.md
  modified:
    - packages/dataplat/src/dataplat/pipeline/run.py

key-decisions:
  - "TableSnapshot carries an optional 4th field (key_count: int | None = None) beyond the plan's literal 3-field description (table/row_count/checksum) -- mirrors _compute_silver_gold_reconciliation's own key_count_input/key_count_output pattern when a caller passes business_key_column, stays fully backward compatible since it defaults to None"
  - "compare_snapshots() dispatches on isinstance(before/after, CustomersScd2Snapshot) vs TableSnapshot rather than folding SCD2 comparison into TableSnapshot itself -- matches the plan's explicitly offered alternative ('or fold into a richer TableSnapshot variant')"
  - "Kept test_compare_snapshots_* (pure, no DB) in the same file as the testcontainers-based snapshot tests, under the same pytest.mark.integration mark -- Task 2's acceptance criteria explicitly sanctions this, and tests/integration/conftest.py's autouse _require_docker fixture plus its unconditional testcontainers import mean the whole directory already requires Docker regardless of any single test's own marker"

requirements-completed: [INCR-07]

# Metrics
duration: 30min
completed: 2026-08-22
---

# Phase 11 Plan 11: Rebuild Reconciliation Arithmetic Summary

**Additive `_table_checksum(columns=...)` scoping plus a new `rebuild_reconciliation` module (`TableSnapshot`/`CustomersScd2Snapshot`/`compare_snapshots`) proving D-29's pre-drop/post-rebuild comparison arithmetic correct in isolation, via 8 new testcontainers-backed tests.**

## Performance

- **Duration:** ~30 min
- **Started:** 2026-08-22T17:35:00Z (approx.)
- **Completed:** 2026-08-22T18:11:49Z
- **Tasks:** 2 (both `tdd="true"`, each RED then GREEN)
- **Files modified:** 4 (1 modified, 3 created)

## Accomplishments

- Extended `_table_checksum` (`packages/dataplat/src/dataplat/pipeline/run.py`) with an additive, keyword-only `columns: Sequence[str] | None = None` parameter. `None` (the default) is byte-for-byte identical to the pre-existing behavior — proven by independently recomputing the literal pre-change SQL and asserting equality, not by trusting the function's own output circularly. The one existing caller (`_compute_silver_gold_reconciliation`) needed zero changes.
- Proved the column-scoped path genuinely excludes named columns: two single-row temp tables holding identical business data but a different `_run_id` produce the SAME scoped checksum and a DIFFERENT unscoped checksum. Also proved the scoped path stays order-independent (mirrors the pre-existing `bit_xor` commutativity property).
- Built `packages/dataplat/src/dataplat/pipeline/rebuild_reconciliation.py`: `snapshot_table_state()` (row count + column-scoped checksum + optional key count), `snapshot_customers_scd2_state()` (adds per-`customer_id` SCD2 version-count + current-row `valid_from`/`valid_to`/`is_current` state, reading migration 0035's exact column shape), and `compare_snapshots()` (a pure function returning named per-field mismatches, never a bare boolean).
- Confirmed the module performs no mutating SQL anywhere (`DROP TABLE`/`DROP SCHEMA`/`DELETE FROM`/`TRUNCATE`) via a static source-inspection test.
- Confirmed `import-linter` contracts, `ruff check` (full `select = ["ALL"]` ruleset), `ruff format`, and `mypy --strict` all pass clean on every touched file.

## Task Commits

Each task followed RED → GREEN (both `tdd="true"`):

1. **Task 1: Extend `_table_checksum` with an additive `columns=` parameter**
   - `bed35f4` (test) — 3 failing tests added; confirmed RED for the right reason (`TypeError: _table_checksum() got an unexpected keyword argument 'columns'`)
   - `0154241` (feat) — implementation; all 3 tests pass, existing caller's test still passes unchanged
2. **Task 2: `rebuild_reconciliation.py` — snapshot + compare**
   - `94cbb5c` (test) — 5 more failing tests added (2 snapshot tests, 2 compare tests, 1 static no-mutating-SQL guard); confirmed RED for the right reason (`ModuleNotFoundError: No module named 'dataplat.pipeline.rebuild_reconciliation'`)
   - `d6f2c47` (feat) — implementation; all 8 tests in the file pass

**Plan metadata:** (this commit, immediately following)

## Files Created/Modified

- `packages/dataplat/src/dataplat/pipeline/run.py` — `_table_checksum` gains `columns: Sequence[str] | None = None`; docstring names all six excluded-by-caller-choice lineage columns (`_run_id`, `_file_id`, `_batch_id`, `_source_row_number`, `_ingested_at`, `_dbt_loaded_at`) and explains why `_record_hash`/`_record_hash_version` are deliberately not excluded
- `packages/dataplat/src/dataplat/pipeline/rebuild_reconciliation.py` — new module: `TableSnapshot`, `ScdKeySnapshot`, `CustomersScd2Snapshot`, `RebuildComparisonResult` (all frozen `@dataclasses.dataclass(slots=True, frozen=True)`), `snapshot_table_state()`, `snapshot_customers_scd2_state()`, `compare_snapshots()`
- `tests/integration/test_rebuild_reconciliation.py` — 8 tests across both tasks, mirroring `test_reconciliation.py`'s established fixture/helper conventions (`migrated_dsn`, direct `psycopg.connect(...)`, per-file-duplicated seeding helpers)
- `.planning/phases/11-ci-cd-completion-operations/deferred-items.md` — new: logs one pre-existing, out-of-scope test failure discovered during verification (see Issues Encountered)

## Exact Signatures for Plan 11-12

Per this plan's own `<output>` instruction, the exact shapes plan 11-12's orchestration should consume directly:

```python
# packages/dataplat/src/dataplat/pipeline/run.py
def _table_checksum(
    conn: Connection[Any],
    table: str,
    *,
    columns: Sequence[str] | None = None,
) -> str | None: ...

# packages/dataplat/src/dataplat/pipeline/rebuild_reconciliation.py
@dataclasses.dataclass(slots=True, frozen=True)
class TableSnapshot:
    table: str
    row_count: int
    checksum: str | None
    key_count: int | None = None

@dataclasses.dataclass(slots=True, frozen=True)
class ScdKeySnapshot:
    business_key: str
    version_count: int
    current_valid_from: datetime | None
    current_valid_to: datetime | None
    current_is_current: bool

@dataclasses.dataclass(slots=True, frozen=True)
class CustomersScd2Snapshot:
    table_snapshot: TableSnapshot
    keys: tuple[ScdKeySnapshot, ...]

@dataclasses.dataclass(slots=True, frozen=True)
class RebuildComparisonResult:
    matches: bool
    mismatches: tuple[str, ...]

def snapshot_table_state(
    conn: Connection[Any],
    table: str,
    *,
    business_columns: Sequence[str],
    business_key_column: str | None = None,
) -> TableSnapshot: ...

def snapshot_customers_scd2_state(
    conn: Connection[Any],
    *,
    business_columns: Sequence[str],
    table: str = "normalized.customers",
) -> CustomersScd2Snapshot: ...

def compare_snapshots(
    before: TableSnapshot | CustomersScd2Snapshot,
    after: TableSnapshot | CustomersScd2Snapshot,
) -> RebuildComparisonResult: ...
```

`mismatches` entries are named strings: `"row_count"`, `"checksum"`, `"key_count"` for a plain `TableSnapshot` comparison; the same three plus `"scd2_key:<key>"` (key present on only one side) or `"scd2_key:<key>.version_count"` / `.current_valid_from` / `.current_valid_to` / `.current_is_current` for a `CustomersScd2Snapshot` comparison.

**Note for plan 11-12:** these snapshot dataclasses are produced in-memory only — WHERE they persist across the schema drop (T-11-30, accepted risk, flagged for continuity in this plan's own threat model) is entirely plan 11-12's decision to make.

## Decisions Made

- **`TableSnapshot` carries an optional 4th field `key_count`** beyond the plan's literal 3-field description, defaulting to `None` so every 3-arg construction the plan describes still works unchanged. Added because the plan's action text explicitly asked `snapshot_table_state` to accept `business_key_column`, and `_compute_silver_gold_reconciliation` (the function Task 2 was told to mirror) already computes exactly this figure (`key_count_input`/`key_count_output`) when a business key column is available.
- **`compare_snapshots` dispatches via `isinstance` on `CustomersScd2Snapshot` vs. `TableSnapshot`** rather than folding SCD2 state into `TableSnapshot` itself — the plan explicitly offered both shapes as acceptable ("or fold into a richer `TableSnapshot` variant for SCD2 tables"); the separate-structure approach keeps `TableSnapshot` usable standalone for non-SCD2 tables (e.g. `silver.customers`, `normalized.orders`) without carrying an always-empty `keys` tuple.
- **Pure `compare_snapshots` tests stay in `tests/integration/test_rebuild_reconciliation.py`** rather than a separate `tests/unit/` file, per Task 2's own acceptance criteria wording — verified this doesn't create a real offline-gate regression, since `tests/integration/conftest.py` imports `testcontainers` unconditionally at module scope and its `_require_docker` fixture is `autouse=True` for the whole directory regardless of any individual test's marker; `make check`'s `test`/`check` targets already exclude `tests/integration/` by path, not by marker.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug, caught before it landed] Test fixture customer_id values used underscore-separated digit strings**

- **Found during:** Task 1/2, while drafting seed data for `silver.customers` rows
- **Issue:** Initial draft used Python-literal-style strings like `"9_700_001"` as `customer_id` values. `tests/integration/conftest.py`'s own `_clean_up_non_numeric_silver_business_keys` docstring documents that every `silver.customers`/`silver.orders` row's business key MUST cast to `normalized.customers`/`normalized.orders`' `integer` column, because those staging tables are session-scoped and shared across the whole `tests/integration/` collection — a non-numeric key left behind would abort every later real `MergePublisher` publish for the rest of the test session.
- **Fix:** Replaced all such values with pure-digit strings (`"9702001"`, `"970301{i}"`) in a dedicated, collision-checked numeric range (grepped the whole `tests/integration/` tree first to confirm no other file already used the `9702xxx`/`9703xxx`/`9704xxx` ranges).
- **Files modified:** `tests/integration/test_rebuild_reconciliation.py` (caught during drafting, before the RED commit — not a separate fix commit)
- **Verification:** All 8 tests pass; re-ran `test_reconciliation.py` afterward to confirm no poisoned rows carried over.

**2. [Rule 1 - Bug] Static no-mutating-SQL guard test had a false-positive substring match**

- **Found during:** Task 2 GREEN verification
- **Issue:** `test_rebuild_reconciliation_module_performs_no_mutating_sql`'s naive `"DROP "` substring check matched the module's own docstring prose ("reconciles to its PRE-DROP STATE" contains "DROP STATE", which contains "DROP ").
- **Fix:** Narrowed the forbidden-substring list to actual SQL-statement-shaped keywords (`"DROP TABLE"`, `"DROP SCHEMA"`, `"DELETE FROM"`, `"TRUNCATE"`) instead of a bare `"DROP "`.
- **Files modified:** `tests/integration/test_rebuild_reconciliation.py`
- **Verification:** Test passes; still correctly guards against the module ever gaining a real mutating statement.
- **Committed in:** `d6f2c47` (Task 2 GREEN commit)

---

**Total deviations:** 2 auto-fixed (both Rule 1, both caught and fixed before/during the relevant task's own GREEN commit — neither reached a committed RED state as a bug)
**Impact on plan:** Both fixes were necessary for correctness (test data integrity / test assertion precision). No scope creep — nothing outside Task 1/2's own declared files was touched.

## Issues Encountered

- **Pre-existing, out-of-scope test failures found during acceptance-criteria verification.** Running `uv run --group cluster pytest tests/integration/test_reconciliation.py -q` (the plan's own acceptance criteria for Task 1, and part of the plan's overall `<verification>` command) surfaced 4 failing tests (`test_clean_staging_pass_writes_one_raw_bronze_row_with_zero_discrepancy` and 3 siblings), all failing with `psycopg.errors.InvalidTextRepresentation: invalid input syntax for type bigint` on `_source_row_number` during a `COPY` into `staging.customers__r<N>` — the value being written looks like a `_record_hash` hex string, suggesting a column-count/ordering bug in `StagingLoader`'s `COPY` column list (`packages/dataplat/src/dataplat/load/staging.py`). Confirmed via code-path analysis that this is unrelated to plan 11-11's change: `_table_checksum`/`_compute_silver_gold_reconciliation` (the only functions this plan touches) are called exclusively from `publish_ingest`, never from `stage_ingest` (the function these 4 failing tests exercise) — my diff makes no change reachable from that code path. Logged in `.planning/phases/11-ci-cd-completion-operations/deferred-items.md` per the scope-boundary rule (only auto-fix issues directly caused by the current task's own changes); not fixed. The 1 test in that file that DOES exercise `_table_checksum`/`_compute_silver_gold_reconciliation` directly (`test_customers_scd2_multi_version_output_count_exceeds_key_count_output`) passes unchanged, which is the actual proof this plan's acceptance criteria needed.

## User Setup Required

None — no external service configuration required.

## Next Phase Readiness

- Plan 11-12 (rebuild-from-raw orchestration) can now call `snapshot_table_state()`/`snapshot_customers_scd2_state()` before the drop and after the rebuild, and `compare_snapshots()` to get a named-mismatch verdict — see "Exact Signatures for Plan 11-12" above.
- Plan 11-12 still owns: WHERE the pre-drop snapshot persists across the schema drop (T-11-30, this plan's threat model flags it for continuity, deliberately unresolved here), the actual `DROP SCHEMA`/`alembic upgrade head`/backfill-trigger orchestration (D-32), and wiring `record_reconciliation`'s existing per-file mechanism (D-29 point 4, needs no new code per Pitfall 8).
- The 4 pre-existing `raw_bronze` test failures (staging COPY column-ordering bug, `deferred-items.md`) are NOT blocking for plan 11-12 — they live in `stage_ingest`'s code path, not `publish_ingest`'s, and are unrelated to the reconciliation arithmetic this plan built. Worth a dedicated `/gsd:debug` session at some point, but out of this plan's scope.

---
*Phase: 11-ci-cd-completion-operations*
*Completed: 2026-08-22*
