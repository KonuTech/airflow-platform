# Phase 8 — Deferred Items

Out-of-scope discoveries found during plan execution, logged (not fixed)
per the executor's scope-boundary rule (SCOPE BOUNDARY: only fix issues
directly caused by the current task's own changes).

## From plans 08-05 and 08-06 (duplicate finding)

- **`PostgresMetadataRepository` cannot be instantiated — pre-existing, not caused by 08-05 or 08-06.**
  `make typecheck` (and `tests/policy/test_gates_actually_fail.py::
  test_the_main_gate_does_not_lint_the_bad_samples`) fails with:
  ```
  packages/csv-processor/src/csv_processor/cli.py:109: error: Cannot
  instantiate abstract class "PostgresMetadataRepository" with abstract
  attributes "record_rejected_records", "record_validation_results" and
  "resolve_rejected_records_for_batch"  [abstract]
  ```
  Root cause: plan 08-01 widened the `MetadataRepository` Protocol
  (`packages/dataplat/src/dataplat/metadata/repository.py`) with these three
  new methods but did not add concrete implementations to
  `PostgresMetadataRepository` (`packages/dataplat/src/dataplat/metadata/
  postgres.py`) — confirmed pre-existing by reproducing the identical mypy
  error against the pre-08-05 commit (`5031e73`, before any of this plan's
  changes). Neither plan 08-05 nor 08-06 touches `metadata/postgres.py`.
  Plan 08-03 implements these three methods on `PostgresMetadataRepository`
  and merged into this same wave — verify this self-resolves once the
  Wave-2 merge completes; if it does not, it needs its own gap-closure plan.

- **`csv_ingest_customers.py` exceeds ORCH-06's 150-line budget — pre-existing, not caused by 08-05.**
  `tests/policy/test_dag_line_budget.py::
  test_csv_ingest_customers_stays_under_150_lines` fails: the file is 162
  lines. Root cause: plan 08-02 added the `integrity_gate` task (LOAD-10)
  to `airflow/dags/csv_ingest_customers.py` without trimming the file back
  under budget. `airflow/dags/csv_ingest_customers.py` is not in plan
  08-05's `files_modified` scope. Whichever plan owns DAG-thinness cleanup
  for this phase should either shrink the file or revise the budget.

## From plan 08-11

- **`tests/integration/test_publish_orders.py:263` is 103 chars, over ruff's
  100-char limit — pre-existing, not caused by 08-11.**
  `tests/policy/test_gates_actually_fail.py::
  test_the_main_gate_does_not_lint_the_bad_samples` fails because `make lint`
  itself is red: `ruff check .` reports `E501 Line too long (103 > 100)` on
  the `_seed_run(repository, migrated_dsn, key_suffix="orders_noop_republish")`
  line. Root cause: commit `8490926` (plan 08-05's own gap-closure fix,
  "namespace orders publish-test idempotency keys to avoid cross-file
  collision") lengthened this line past 100 chars without wrapping it.
  `tests/integration/test_publish_orders.py` is not in plan 08-11's
  `files_modified` scope, and this session made no other change to that
  file. Confirmed via `git log`/`git status` that the file has zero diff
  from this session. A future plan/verification pass should wrap this line
  to restore a green `make lint`.
