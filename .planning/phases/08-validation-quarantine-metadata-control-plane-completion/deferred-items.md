# Phase 8 — Deferred Items

Out-of-scope discoveries found during plan execution, logged (not fixed)
per the executor's scope-boundary rule (SCOPE BOUNDARY: only fix issues
directly caused by the current task's own changes).

## From plans 08-05 and 08-06 (duplicate finding)

- **RESOLVED (Wave 2 orchestrator merge).** `PostgresMetadataRepository` cannot be instantiated.
  Root cause: plan 08-01 widened the `MetadataRepository` Protocol with three
  new methods but only 08-03 (parallel sibling, same wave) added the concrete
  implementations. Confirmed resolved: `make typecheck` passes clean
  immediately after the Wave 2 worktree merge (`Success: no issues found in
  80 source files`).

- **RESOLVED (plan 08-12).** `csv_ingest_customers.py` exceeded ORCH-06's
  150-line budget (162 lines, from plan 08-02's `integrity_gate` task
  addition without a compensating trim). Plan 08-12 already modified this
  file (wiring `list_matched_keys`/`integrity_gate`/`outlets` in) so the
  gap was closed inline rather than deferred further: module docstring and
  inline comments condensed, no functional lines removed. File is now 149
  lines; `tests/policy/test_dag_line_budget.py::
  test_csv_ingest_customers_stays_under_150_lines` passes.

## From plan 08-11

- **RESOLVED (Wave 5 orchestrator post-merge gate).** `tests/integration/test_publish_orders.py:263`
  was 103 chars, over ruff's 100-char limit, from commit `8490926` (plan
  08-05's own gap-closure fix). Wrapped the offending call across three
  lines in commit `274385c`; `ruff check .` is clean.

- **`normalized.customers`/`normalized.orders` real quality rules exposed a genuine cross-plan
  test conflict (Wave 5 orchestrator post-merge gate, resolved in commit `271b6b7`).**
  08-11 wired a real `QUALITY_UNIQUENESS` rule on `customer_id` into
  `configs/datasets/customers.yaml`. `tests/integration/test_staging_normalization.py`
  (phase 6) intentionally staged two rows sharing `customer_id="5"` to prove
  `_record_hash` is NFC-invariant across differently-encoded source text —
  the second row is now correctly `REJECT_RECORD`-ed as a uniqueness
  violation before ever reaching hash computation, which is the new
  intended behavior. Fixed by giving the two rows distinct `customer_id`s
  and proving NFC-invariant hashing per-row instead of via hash-set-equality
  across a shared business key — same regression coverage, compatible with
  the now-enforced constraint.

## From plan 08-14

- **Live cluster is not yet current with this phase's own deployment artifacts** (found
  during Task 1/2 live-verification attempts; not fixed — a deployment
  operation, not a code defect, and well outside this plan's `files_modified`
  scope). Empirically confirmed via `kubectl -n data exec analytics-db-1 --
  psql ... "SELECT version_num FROM alembic_version"` (relation does not
  exist — the DB is at least 4 migrations behind `0015`/`0016`/`0017`) and
  `kubectl -n airflow exec deploy/airflow-api-server -- airflow dags list`
  (only `csv_ingest_customers`/`smoke_kubernetes_pod` are known; `csv_ingest_orders`
  is absent). Both `tests/e2e/slice/test_referential_orphan.py` and
  `test_backfill_reentry.py` are written and ready, but a live `-m cluster`
  run will error (not skip -- the cluster itself IS reachable) until:
  migrations `0014`-`0017` are applied to `analytics-db`, `csv-processor` is
  rebuilt/redeployed with `configs/datasets/orders.yaml` and the updated
  `customers.yaml` quality block baked in, and the Airflow DAG bundle picks
  up `airflow/dags/csv_ingest_orders.py`. This is a standard post-wave
  deployment step (matching Phase 4's own "the rebuild belongs after the
  merge, not inside either plan" precedent, `04-11-SUMMARY.md`), not
  something this isolated worktree plan should perform against the shared
  live cluster mid-wave.

- **A real architecture finding, not a code defect in this plan's own files**:
  `dataplat.discovery.discover_files`'s `batch_key` is a pure function of a
  file's `content_sha256` (`f"{dataset_name}:{content_sha256_hex[:16]}"`).
  `resolve_rejected_records_for_batch` (D-05) is scoped strictly by
  `batch_id`. A "corrected" re-upload of a previously-rejected row
  necessarily changes the file's bytes, so it discovers under a brand-new
  `file_id`/`batch_id` — distinct from the original reject's batch. Combined
  with `discover_files`'s own `if status == "SUCCEEDED": return None` skip
  (unchanged content is never re-processed once its run has succeeded, even
  via a genuine `airflow backfill create` invocation, since Airflow-level
  backfill re-executes `discover`/`ingest` against whatever the bucket
  currently holds — 08-13-PLAN.md's own docstring, "against the CURRENT
  state" — not a content-addressed replay), there appears to be no code
  path today where a real, content-differing correction resolves the SAME
  batch's `PENDING` `meta.rejected_records` row through D-05's own
  production mechanism. `08-11`'s own `test_backfill_run_resolves_the_batch_
  pending_rejects` (in `tests/integration/test_publish_transaction_wiring.py`)
  proves the mechanism correct in isolation by constructing the SAME
  `batch_id` directly via the repository API, not by discovering two
  genuinely different files. `test_backfill_reentry.py`'s own
  `test_backfill_resolves_previously_rejected_row` writes the assertion
  exactly as the plan's locked D-05 intent describes (a previously-PENDING
  row should flip to `REDRIVEN`); if it fails once the live cluster is fully
  deployed, this is the most likely reason, and the fix belongs to a future
  plan with a full architecture review (Rule 4 territory: content-addressed
  batching vs. row-level correction is a design decision, not a one-line
  bug fix) — not a change this closing plan silently made to
  `discovery.py`.
