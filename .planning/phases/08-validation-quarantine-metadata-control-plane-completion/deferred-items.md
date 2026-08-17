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
