# Phase 8 — Deferred Items

Out-of-scope discoveries found during plan execution, logged (not fixed)
per the executor's scope-boundary rule.

## From plan 08-05

- **`PostgresMetadataRepository` cannot be instantiated — pre-existing, not caused by 08-05.**
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
  changes), with `git stash`/`stash pop` isolating exactly this plan's diff.
  Not in plan 08-05's `files_modified` scope (`metadata/postgres.py` is not
  listed), and fixing three unrelated Protocol-method implementations is a
  different plan's job (per 08-CONTEXT.md's own file-ownership split between
  the validation-engine and metadata-completion work streams). Whichever
  later plan in this phase implements `record_rejected_records`/
  `record_validation_results`/`resolve_rejected_records_for_batch` on
  `PostgresMetadataRepository` closes this.

- **`csv_ingest_customers.py` exceeds ORCH-06's 150-line budget — pre-existing, not caused by 08-05.**
  `tests/policy/test_dag_line_budget.py::
  test_csv_ingest_customers_stays_under_150_lines` fails: the file is 162
  lines. Root cause: plan 08-02 added the `integrity_gate` task (LOAD-10)
  to `airflow/dags/csv_ingest_customers.py` without trimming the file back
  under budget. `airflow/dags/csv_ingest_customers.py` is not in plan
  08-05's `files_modified` scope. Whichever plan owns DAG-thinness cleanup
  for this phase should either shrink the file or revise the budget.
