---
phase: 09-etl-correctness-dedup-incremental-backfill-recovery
plan: 04
subsystem: metadata-control-plane
tags: [airflow, adr-0004-exception, run-stages, dbt-build, recovery-visibility]

requires: []
provides:
  - "airflow/dags/_common/run_stage_recorder.py: list_run_ids_pending_dbt_build / record_dbt_build_stage"
affects:
  - "09-06 (meta.v_run_recovery view, reads DBT_BUILD rows this module writes)"
  - "09-09 (DAG wiring: calling these tasks from csv_ingest_customers/csv_ingest_orders)"

tech-stack:
  added: []
  patterns:
    - "Third sanctioned ADR-0004 exception (alongside integrity_gate.py and kpo.py/tracing_kpo.py): plain psycopg, BaseHook.get_connection(...).get_uri() DSN resolution, no dataplat import"
    - "Defensive ON CONFLICT upsert (not a claim/lease) for a single-task-processes-many-runs write pattern"

key-files:
  created:
    - airflow/dags/_common/run_stage_recorder.py
    - tests/dagtest/test_run_stage_recorder.py
  modified:
    - tests/policy/test_dag_thinness.py

decisions:
  - "record_dbt_build_stage's INSERT branch must independently set finished_at (a CASE expression mirroring the ON CONFLICT DO UPDATE branch's own), not rely on the UPDATE branch alone -- found and fixed via a genuinely failing test (Rule 1), see Deviations."
  - "test_run_stage_recorder.py defines its own session-scoped testcontainers-Postgres-plus-migrations fixture locally, duplicating tests/integration/conftest.py's postgres_dsn/run_migrations/migrated_dsn pattern rather than cross-importing across test-tier conftest.py files, matching this codebase's established convention (tests/dagtest/conftest.py's own module docstring) -- the file lives under tests/dagtest/ per the plan's file list, but the concern under test is the analytical database, not Airflow's metadata database tests/dagtest/conftest.py's own fixtures stand up."

metrics:
  duration: 25min
  completed: 2026-08-19
---

# Phase 9 Plan 04: DBT_BUILD run_stages recorder Summary

A third, narrowly-scoped ADR-0004 exception: `airflow/dags/_common/run_stage_recorder.py` lets the DAG folder record a `DBT_BUILD` `meta.run_stages` row directly via plain `psycopg`, closing LOAD-06's whole-pipeline recovery-visibility gap without ever importing `dataplat` or coupling dbt's own idempotency to the Python claim/lease/heartbeat mechanism (D-14).

## What Was Built

- **`list_run_ids_pending_dbt_build(dataset_name)`** — an `@task` that returns every `run_id` whose `STAGE_LOAD` stage is `SUCCEEDED` and either has no `DBT_BUILD` row yet or has one that's `FAILED`/`RUNNING` (a retry candidate). A run whose `DBT_BUILD` row is already `SUCCEEDED` is never returned.
- **`record_dbt_build_stage(run_ids, status)`** — an `@task` that upserts a `DBT_BUILD` `meta.run_stages` row per `run_id`. An empty `run_ids` list is a safe no-op (no connection opened). `pod_name` is deliberately `NULL` (this task runs before the `dbt_build` KPO pod exists). `finished_at` is set whenever the incoming status is terminal (anything but `RUNNING`), on both the fresh-INSERT path and the `ON CONFLICT` upsert path.
- Both tasks resolve their DSN through the same `analytics_db_default` Airflow Connection `integrity_gate.py` already uses — no new credential, no new grant beyond what migration 0025 already gave `etl_app`.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `record_dbt_build_stage`'s fresh-INSERT path never set `finished_at`**
- **Found during:** Task 1, running Test 4 (`record_dbt_build_stage(status="SUCCEEDED")` without a prior `RUNNING` row)
- **Issue:** The original `INSERT ... ON CONFLICT DO UPDATE` statement only computed `finished_at` inside the `DO UPDATE SET` branch. A genuinely fresh row (no prior `DBT_BUILD` row for that `run_id`) took the plain `INSERT` path, which never populated `finished_at` at all — a terminal-status write for a never-before-seen run_id left `finished_at` NULL, silently violating the plan's own Test 4 behavior ("transitions ... to SUCCEEDED with finished_at set, without requiring a prior RUNNING row to exist").
- **Fix:** Added a matching `CASE WHEN %(status)s != 'RUNNING' THEN now() ELSE NULL END` to the `VALUES` clause itself, so both the INSERT and the ON CONFLICT UPDATE branches independently compute `finished_at` from the incoming status.
- **Files modified:** `airflow/dags/_common/run_stage_recorder.py`
- **Commit:** `2ee0c1c` (folded into Task 1's single commit — caught before commit, during test-driven verification)

### Also Modified (acceptance-criteria-driven, not in `files_modified`)

**`tests/policy/test_dag_thinness.py`** — the plan's own acceptance criteria required either an existing exemption or adding one. `run_stage_recorder.py` legitimately imports `psycopg` and contains raw SQL literals (the ADR-0004 exception itself), so it was added to both `_EXEMPT_FROM_IMPORT_CHECK` and `_EXEMPT_FROM_SQL_CHECK`, alongside `integrity_gate.py`, each documented with the same by-name, narrowly-scoped reasoning already established there.

## Verification

`pytest tests/dagtest/test_run_stage_recorder.py tests/policy/test_dag_thinness.py -q` — 8 passed (5 new behavior tests + 3 existing policy tests, run together as the plan's own `<verification>` block specifies).

`grep -c "import dataplat\|from dataplat" airflow/dags/_common/run_stage_recorder.py` — 0.

`ruff check` / `mypy` on the new module — clean. `pytest tests/unit tests/regression -q` (497 passed) and `pytest tests/policy -q -m "not manifests"` (125 passed) confirm no regression to the surrounding offline gate.

## Self-Check: PASSED

- FOUND: `airflow/dags/_common/run_stage_recorder.py`
- FOUND: `tests/dagtest/test_run_stage_recorder.py`
- FOUND commit `2ee0c1c` in `git log --oneline --all`
