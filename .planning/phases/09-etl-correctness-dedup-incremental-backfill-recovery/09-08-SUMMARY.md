---
phase: 09-etl-correctness-dedup-incremental-backfill-recovery
plan: 08
subsystem: data-quality
tags: [dbt, postgres, reconciliation, post-hook, jinja, source-to-target]

# Dependency graph
requires:
  - phase: 09 (plan 09-02)
    provides: "meta.reconciliation_results table (migration 0032), dbt_app/etl_app/grafana_reader grants, D-20..D-24 grain/accounting design"
provides:
  - "reconciliation_post_hook.sql: durable, per-file, per-hop bronze->silver reconciliation write in the model's own dbt post-hook transaction"
  - "dbt/tests/reconciliation_{customers,orders}.sql: severity:warn native dbt test, the second of D-26's two required signals"
affects: [09-09, 09-10, 09-11, phase-verification]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "dbt singular test severity config via {{ config(severity='warn') }} in the test file itself, plus an explicit `-- depends_on: {{ ref(...) }}` comment to wire it into --select without a literal ref() call in the query body"
    - "Cross-statement post-hook watermark isolation: when two macro-generated INSERT statements share one post_hook_sql string and the second reads state the first just wrote, exclude the first statement's own row by identity column (dedup_audit_id), never by a Jinja-rendered value like invocation_id which dbt's partial-parsing cache can freeze stale across separate builds"

key-files:
  created:
    - dbt/macros/reconciliation_post_hook.sql
    - dbt/tests/reconciliation_customers.sql
    - dbt/tests/reconciliation_orders.sql
    - tests/integration/test_dbt_reconciliation.py
  modified:
    - dbt/models/silver/silver_customers.sql
    - dbt/models/silver/silver_orders.sql
    - dbt/models/silver/silver_customers.yml
    - dbt/models/silver/silver_orders.yml

key-decisions:
  - "reconciliation_post_hook's own watermark floor excludes the current build's just-inserted dedup_audit row by dedup_audit_id (identity-column subquery), not by dbt_invocation_id — dbt's partial-parsing cache can silently reuse a stale invocation_id across separate `dbt build` processes when it detects no source-file changes, which would otherwise duplicate reconciliation rows on every idempotent rerun"
  - "The two post_hook macro calls concatenated in one post_hook_sql string require an explicit semicolon between them — Postgres parses the whole string as one multi-statement batch, and a missing semicolon produces a genuine syntax error at the second statement's opening keyword"
  - "severity:warn is set via {{ config(severity='warn') }} inside the singular test .sql file itself, not via a model-level tests:/data_tests: YAML entry — that YAML key only applies to generic, parametrized tests referenced by macro name, which does not fit a standalone singular test file"

requirements-completed: [VALID-05]

# Metrics
duration: 23min
completed: 2026-08-19
---

# Phase 9 Plan 08: Bronze->Silver Reconciliation Post-Hook Summary

**Bronze->silver reconciliation now writes one durable, per-file `meta.reconciliation_results` row every `dbt build`, proven idempotent live, plus a non-blocking `severity: warn` dbt test — D-26's explicit "both" requirement, with two real bugs found and fixed via live multi-invocation `dbt build` testing rather than static review alone.**

## Performance

- **Duration:** 23 min (base commit to final task commit)
- **Started:** 2026-08-19T15:04:04Z
- **Completed:** 2026-08-19T15:27:02Z
- **Tasks:** 2/2 completed
- **Files modified:** 8 (4 created, 4 modified)

## Accomplishments

- `reconciliation_post_hook.sql` writes one `bronze_silver` row per contributing `_file_id`
  (D-24's per-file, per-hop grain) in the SAME transaction as the silver model's own write and
  `dedup_audit_post_hook`'s write, using D-22's exact accounting formula
  (`discrepancy = input_count - (output_count + dedup_count)`)
- Proved live against a real, migrated (Alembic `head`) testcontainers-equivalent Postgres: first
  build writes correct per-file rows with zero discrepancy; an idempotent rerun with no new data
  writes zero additional rows; a third build with genuinely new data writes exactly one new row for
  the new file only
- Added `severity: warn` singular dbt tests for both silver models — proven live to surface as a
  `warn` (never `error`) outcome in `dbt build`'s own JSON run-results output on a deliberately
  corrupted fixture, with the build itself still exiting 0
- New `tests/integration/test_dbt_reconciliation.py` (3 tests) passes both standalone and combined
  with the phase's 3 pre-existing `test_dbt_*.py` files in the same pytest session (9/9 passed)

## Task Commits

Each task was committed atomically:

1. **Task 1: reconciliation_post_hook.sql macro + wire into both silver models** - `4d6a99a` (feat)
2. **Task 2: severity:warn dbt test + integration test** - `60284e7` (test)

_Note: no separate plan-metadata commit in worktree mode — SUMMARY.md is committed as part of this
executor's final metadata commit per the orchestrator's worktree protocol._

## Files Created/Modified

- `dbt/macros/reconciliation_post_hook.sql` - the durable per-file bronze->silver reconciliation write, templated on `dedup_audit_post_hook.sql`
- `dbt/models/silver/silver_customers.sql` - `post_hook_sql` capture extended with a second, semicolon-separated `reconciliation_post_hook(...)` call
- `dbt/models/silver/silver_orders.sql` - same extension, dataset-substituted
- `dbt/models/silver/silver_customers.yml` - documentation comment pointing at `dbt/tests/reconciliation_customers.sql` (not a `tests:` block entry — see Decisions)
- `dbt/models/silver/silver_orders.yml` - same documentation comment, dataset-substituted
- `dbt/tests/reconciliation_customers.sql` - singular, `severity: warn` dbt test for the customers dataset
- `dbt/tests/reconciliation_orders.sql` - same test, dataset-substituted
- `tests/integration/test_dbt_reconciliation.py` - 3 integration tests: discrepancy-formula correctness, D-24 per-file grain (3 files -> 3 rows, idempotent rerun), and severity:warn never blocking the build

## Decisions Made

- **Watermark floor keyed on `dedup_audit_id`, not `dbt_invocation_id`.** Found live: dbt's
  partial-parsing cache ("Nothing changed, skipping partial parsing") can reuse a PREVIOUS
  invocation's already-rendered `post_hook_sql` string — including a frozen, stale
  `{{ invocation_id }}` literal — across separate `dbt build` processes when no project source file
  changed between runs. A `dbt_invocation_id !=` filter keyed on that frozen literal duplicated
  reconciliation rows on every idempotent rerun in a live test. Switched to excluding the current
  build's own row by `dedup_audit_id < (select max(dedup_audit_id) from meta.dedup_audit where
  model_name = ...)` — this depends only on real, transaction-local database state (the current
  build's own row via `dedup_audit_post_hook`'s prior insert is always the highest id for that
  `model_name` at the moment this query runs), immune to any Jinja-rendering-order or caching
  hazard.
- **Explicit semicolon between the two concatenated post-hook macro calls.** Both macros produce a
  single SQL statement with no trailing semicolon (by design, since previously only one call
  existed per model). Concatenating two such statements into one `post_hook_sql` string without a
  separator produced a genuine Postgres syntax error (`syntax error at or near "with"`) the moment
  Task 1 wired the second call in.
- **`severity: warn` set via the singular test file's own `{{ config(...) }}`, not a model YAML
  `tests:` entry.** dbt's model-level `tests:`/`data_tests:` keys apply only to generic,
  parametrized tests referenced by macro name — a standalone singular `.sql` test file configures
  its own severity directly. An explicit `-- depends_on: {{ ref(...) }}` comment (dbt's documented
  mechanism for a test whose SQL body never literally calls `ref()`) wires the test into
  `dbt build --select silver_customers`'s own selection graph.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Missing semicolon between concatenated post-hook statements**
- **Found during:** Task 1, first live `dbt build` attempt
- **Issue:** `silver_customers.sql`/`silver_orders.sql`'s `post_hook_sql` capture concatenated
  `dedup_audit_post_hook(...)` and the new `reconciliation_post_hook(...)` call with only a
  newline between them. Postgres parsed the combined string as one continued statement, failing
  with `syntax error at or near "with"` at the second macro's opening keyword.
- **Fix:** Added a semicolon immediately after the `dedup_audit_post_hook(...)` call in both model
  files.
- **Files modified:** `dbt/models/silver/silver_customers.sql`, `dbt/models/silver/silver_orders.sql`
- **Verification:** `dbt build --select silver_customers` (and `silver_orders`) against a real,
  migrated Postgres now completes with `Completed successfully`.
- **Committed in:** `4d6a99a` (part of Task 1 commit)

**2. [Rule 1 - Bug] Watermark floor excluded nothing on every build, including the first**
- **Found during:** Task 1, second live `dbt build` verification pass (checking `meta.
  reconciliation_results` row counts, not just build success)
- **Issue:** The macro's `prior_watermark` CTE originally reused `dedup_audit_post_hook`'s own
  `coalesce(max(max_run_id), 0) where model_name = ...` pattern verbatim. Because
  `dedup_audit_post_hook`'s INSERT runs first in the SAME transaction and Postgres read-own-writes
  semantics apply across separate statements in one transaction, `reconciliation_post_hook`'s own
  floor query already saw that just-inserted row — making the floor equal to the CURRENT build's
  own max `_run_id` and excluding every row the build itself just processed. Result: zero
  `meta.reconciliation_results` rows written on every build, including the very first.
- **Fix (first attempt, itself found broken):** Excluded the current row via
  `dbt_invocation_id != '{{ invocation_id }}'`. This introduced a SECOND bug (see below).
- **Fix (final):** Excluded the current row by identity column instead:
  `dedup_audit_id < (select max(dedup_audit_id) from meta.dedup_audit where model_name = ...)`.
- **Files modified:** `dbt/macros/reconciliation_post_hook.sql`
- **Verification:** Live-proved across three consecutive `dbt build` invocations against the same
  Postgres: build 1 (2 seeded files) wrote exactly 2 rows with `discrepancy = 0`; build 2 (no new
  data) wrote zero additional rows; build 3 (1 new file) wrote exactly 1 new row for the new file
  only, leaving the prior 2 rows untouched.
- **Committed in:** `4d6a99a` (part of Task 1 commit)

**3. [Rule 1 - Bug] `dbt_invocation_id`-based watermark exclusion duplicated rows on partial-parse-cached reruns**
- **Found during:** Task 1, third live verification pass (a second, immediate `dbt build` rerun
  with no code changes, to prove idempotency)
- **Issue:** dbt's partial-parsing cache ("Nothing changed, skipping partial parsing") reused the
  PREVIOUS invocation's already-compiled `post_hook_sql` string — including a stale, frozen
  `{{ invocation_id }}` literal from the earlier build — across the two separate `dbt build`
  subprocess invocations. The `dbt_invocation_id != '<frozen-id>'` filter then failed to exclude
  the correct row, and the second build wrote 2 duplicate reconciliation rows instead of zero.
- **Fix:** Replaced the `dbt_invocation_id` filter with the `dedup_audit_id`-based identity-column
  exclusion described in bug #2's final fix — immune to Jinja/caching timing since it depends only
  on real database state, never a value baked into compiled SQL.
- **Files modified:** `dbt/macros/reconciliation_post_hook.sql`
- **Verification:** Two consecutive `dbt build` invocations against the same unmodified `dbt/`
  project directory (reproducing the exact partial-parse-cache condition) now write 2 rows on the
  first and zero additional rows on the second.
- **Committed in:** `4d6a99a` (part of Task 1 commit)

**4. [Rule 1 - Test bug, own new test] `severity:warn` integration test asserted a "clean pass" baseline that the shared test session doesn't guarantee**
- **Found during:** Task 2, running the new test file combined with the phase's 3 pre-existing
  `test_dbt_*.py` files in one pytest session (not caught running the new file in isolation)
- **Issue:** `test_severity_warn_test_surfaces_as_warn_never_error_on_a_violated_fixture` asserted
  the FIRST `dbt build`'s own outcome was `pass` before deliberately corrupting a row. In the
  shared, session-scoped `migrated_dsn` this codebase's `test_dbt_*.py` files all use, an existing
  autouse fixture (`_clean_up_non_numeric_silver_business_keys` in `tests/integration/conftest.py`)
  deletes non-numeric-business-key rows from `silver.customers`/`silver.orders` after every
  `dbt`-marked test, but never touches the cumulative, append-only `staging.customers` bronze
  table. When an earlier test in the same session (this file's own
  `test_reconciliation_post_hook_writes_one_row_per_file_grain`, using non-numeric `"pg0"`/`"pg1"`/
  `"pg2"` keys) has its silver rows cleaned up post-test, those bronze rows become genuinely
  orphaned — correctly producing a non-zero discrepancy the NEXT test's own "first build" then
  observed as `warn`, not `pass`.
- **Fix:** Removed the baseline `== "pass"` assertion (the macro's own correctness for a clean case
  is already proven by the separate `test_reconciliation_post_hook_writes_a_row_with_the_correct_
  discrepancy_formula` test, using deliberately all-numeric keys unaffected by the cleanup
  fixture). Also switched this test's own seeded `customer_id` from `"w1"` to `"5"` (all-numeric)
  so it doesn't itself become a pollution source for whichever test runs next. The post-corruption
  `warn` assertion remains valid regardless of the pre-existing baseline, since decrementing
  `output_count` by 1 can only make an existing discrepancy more non-zero, never mask it to 0.
- **Files modified:** `tests/integration/test_dbt_reconciliation.py`
- **Verification:** Full combined run (`test_dbt_silver_dedup.py test_dbt_dedup_audit.py
  test_dbt_silver_incremental.py test_dbt_reconciliation.py -m dbt`) now passes 9/9; standalone run
  of just this file passes 3/3.
- **Committed in:** `60284e7` (part of Task 2 commit)

## Known Stubs

None.

## Threat Flags

None — this plan's only new surface is `reconciliation_post_hook`'s `source_schema`/
`source_identifier`/`target_schema`/`target_identifier` arguments, already covered by the plan's
own threat model (T-09-14, mitigated: always literal, dbt-config-controlled strings at the model's
own call site, never row-derived).

## Verification

- `dbt compile --select silver_customers silver_orders` — zero Jinja errors
- `grep -c "bronze_files.file_id" dbt/macros/reconciliation_post_hook.sql` — 1 (present)
- `dbt build --select silver_customers silver_orders` against a real, Alembic-migrated Postgres —
  `Completed successfully`, exit 0, including when the `severity: warn` test's condition is
  deliberately violated
- `pytest tests/integration/test_dbt_reconciliation.py -q -m dbt` — 3 passed
- Combined with the phase's 3 pre-existing `test_dbt_*.py` files in one session — 9 passed

## Self-Check: PASSED

- FOUND: `dbt/macros/reconciliation_post_hook.sql`
- FOUND: `dbt/tests/reconciliation_customers.sql`
- FOUND: `dbt/tests/reconciliation_orders.sql`
- FOUND: `tests/integration/test_dbt_reconciliation.py`
- FOUND commit `4d6a99a` in `git log --oneline --all`
- FOUND commit `60284e7` in `git log --oneline --all`
