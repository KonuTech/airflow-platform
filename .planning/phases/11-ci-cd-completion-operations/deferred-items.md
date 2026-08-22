# Deferred Items — Phase 11

Out-of-scope discoveries found while executing Phase 11 plans, logged per the
executor's scope-boundary rule (only auto-fix issues directly caused by the
current task's own changes). None of these were fixed here. Entries are
grouped by the plan that found them.

## Plan 11-01

### Pre-existing `make check` / `tests/policy` failures, unrelated to CI/CD image publishing

Found while running `uv run pytest tests/policy -q` as part of plan 11-01
Task 3's regression check. Confirmed via `git show <base-commit>:<path>` that
each root cause predates plan 11-01 entirely (traces to Phase 9/10
slowly-changing-dimensions work, not to `.github/workflows/publish.yml` or
`tests/policy/test_publish_workflow_guards.py`, the only two files this plan
touches). All 4 are collected by `make check`'s `policy` target (not
deselected by `-m "not manifests"`), so `make check` is red on the base
commit `0bcc4652a5c74609dc16dbf2df574bc043ed4860` independent of this plan.

| Item | Status | Deferred At |
|------|--------|-------------|
| `tests/policy/test_dag_line_budget.py::test_csv_ingest_customers_stays_under_150_lines` — `airflow/dags/csv_ingest_customers.py` is 182 lines, budget (ORCH-06) is <=152. Confirmed 182 lines already at base commit via `git show`. | Deferred | 2026-08-22, plan 11-01 |
| `tests/policy/test_dag_thinness.py::test_no_business_logic_imports` — `airflow/dags/_common/gap_recorder.py:25` imports `psycopg` directly (ORCH-02/06 requires DAGs delegate DB access to the csv-processor image via `KubernetesPodOperator`, never import a DB driver). Traces to commit `d4a0a22` "feat(09-10): meta.processing_gaps migration + gap-recorder wiring (D-06)". | Deferred | 2026-08-22, plan 11-01 |
| `tests/policy/test_dag_thinness.py::test_no_raw_sql_strings` — same file, `gap_recorder.py:58-59` contains a raw `INSERT INTO meta.processing_gaps ... SELECT ...` string literal. Same root cause/commit as above. | Deferred | 2026-08-22, plan 11-01 |
| `tests/policy/test_gates_actually_fail.py::test_the_main_gate_does_not_lint_the_bad_samples` — `make lint` itself is red with 5 ruff findings (E501/W505 line-too-long) in `airflow/dags/csv_ingest_customers.py:141`, `tests/e2e/slice/test_backfill_2year_sweep.py:1072`, and `tests/integration/test_migrations.py:681`. Comments at the offending lines reference "plan 10-08" and are dated 2026-08-22 — Phase 10 work in progress at this plan's base commit. This test's own purpose (proving `make lint`/`make typecheck` fail closed against a *known-bad sample corpus*) is masked by the *real* tree already failing `make lint` for an unrelated reason. | Deferred | 2026-08-22, plan 11-01 |

**Why deferred rather than fixed:** all four sit entirely outside plan 11-01's
`files_modified` (`.github/workflows/publish.yml`,
`tests/policy/test_publish_workflow_guards.py`, `.claude/CLAUDE.md`) and
outside Phase 11's CI/CD-completion scope. Fixing them would mean editing
Phase 9/10 DAG and test files this plan never read, has no context budget to
review for correctness, and was not asked to touch. Whoever picks these up
should re-verify `make check` (or at minimum `make lint` +
`tests/policy/test_dag_line_budget.py` + `tests/policy/test_dag_thinness.py`)
passes clean before closing this entry.

**In scope and fixed in this plan (not deferred, listed here only for
completeness):** `tests/policy/test_workflow_secrets.py::test_no_workflow_
references_a_repository_secret` and `::test_the_workflow_token_stays_read_
only` also failed on first run, but were caused directly by plan 11-01's own
`publish.yml` (the first workflow to reference `secrets.GITHUB_TOKEN` and to
widen job permissions) and were anticipated by that test module's own
docstring ("expected to grow in Phase 11"). Fixed in the same commit as
`publish.yml` — see plan 11-01's SUMMARY.md for detail.

## Plan 11-11

| Category | Item | Status | Deferred At |
|----------|------|--------|-------------|
| Pre-existing bug | `tests/integration/test_reconciliation.py`'s four `raw_bronze` tests (`test_clean_staging_pass_writes_one_raw_bronze_row_with_zero_discrepancy` and 3 siblings) fail with `psycopg.errors.InvalidTextRepresentation: invalid input syntax for type bigint` on the `_source_row_number` column during `COPY INTO staging.customers__r<N>` — the value being written looks like a `_record_hash` hex string, suggesting a column-count/ordering mismatch in `StagingLoader`'s `COPY` column list vs. its value tuples (`packages/dataplat/src/dataplat/load/staging.py`), unrelated to `stage_ingest`'s reconciliation-writing step. Confirmed pre-existing and out of scope for plan 11-11: `_table_checksum`/`_compute_silver_gold_reconciliation` (the only functions plan 11-11 touches) are called exclusively from `publish_ingest`, never from `stage_ingest` (the function these 4 failing tests exercise) — this plan's diff makes no change reachable from that code path. Reproducer: `uv run --group cluster pytest tests/integration/test_reconciliation.py -q`. | Open | 2026-08-22 (plan 11-11) |
