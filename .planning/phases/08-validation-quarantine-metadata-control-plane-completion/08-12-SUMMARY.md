---
phase: 08-validation-quarantine-metadata-control-plane-completion
plan: 12
subsystem: orchestration
tags: [airflow, dagbag, asset, kubernetespodoperator, integrity-gate, s3keysensor]

# Dependency graph
requires:
  - phase: 08-02
    provides: "list_matched_keys/integrity_gate @task functions (LOAD-10 pre-pod-launch file-integrity gate) in airflow/dags/_common/integrity_gate.py, unwired into any real DAG"
  - phase: 08-11
    provides: "barrier/transaction wiring (ReferentialIntegrityBarrier, merge_orders publisher) that csv_ingest_orders's real pod runs now exercise"
provides:
  - "csv_ingest_customers.py wired wait_for_files -> list_matched_keys -> integrity_gate -> discover (D-18), ingest declares outlets=[customers_asset] (D-15)"
  - "csv_ingest_orders.py -- the second real ingestion DAG (D-14), Asset-triggered off customers' own ingest/publish step (D-15), same D-18 gate wiring"
  - "test_dag_structure.py extended: integrity-gate chain and Asset coupling proven structurally via DagBag, no live cluster"
affects: ["08-13", "08-14"]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Airflow Asset cross-DAG coupling by URI, not Python object identity: DagBag gives each DAG file a unique module name, so a cross-file `from other_dag import asset_object` re-executes and re-registers the whole imported module (including its own @dag()-decorated call), producing a duplicate dag_id. Each DAG file that needs the same Asset independently constructs Asset(same_uri) instead."

key-files:
  created:
    - airflow/dags/csv_ingest_orders.py
  modified:
    - airflow/dags/csv_ingest_customers.py
    - tests/unit/test_dag_structure.py
    - .planning/phases/08-validation-quarantine-metadata-control-plane-completion/deferred-items.md

key-decisions:
  - "csv_ingest_orders.py declares its own Asset(\"s3://normalized/customers\") object instead of importing customers_asset from csv_ingest_customers.py -- a live DagBag run proved the cross-file import causes AirflowDagDuplicatedIdException (DagBag assigns each DAG file its own unique module name, so importing another DAG file by module name re-executes and re-registers its @dag() call under a second name). Airflow matches Asset scheduling by URI, so two independently-constructed Asset objects sharing a URI schedule identically without that re-execution."
  - "Closed the pre-existing, deferred ORCH-06 line-budget gap on csv_ingest_customers.py (162 lines, tracked since plan 08-02) inline rather than leaving it deferred again: this plan already modifies the file, so condensing its docstring/comments (no functional-line removal) to 149 lines was in-scope, not scope creep."

requirements-completed: [VALID-07, VALID-08, LOAD-10]

# Metrics
duration: ~20min
completed: 2026-08-17
---

# Phase 8 Plan 12: Integrity-Gate DAG Wiring + csv_ingest_orders Summary

**`list_matched_keys -> integrity_gate` wired ahead of `discover` in both DAGs, and `csv_ingest_orders` stood up as a real, Asset-triggered second ingestion pipeline coupled to `csv_ingest_customers`'s own publish step.**

## Performance

- **Duration:** ~20 min
- **Completed:** 2026-08-17T10:29:26Z
- **Tasks:** 2
- **Files modified:** 3 (1 created, 2 modified) + 1 deferred-items doc update

## Accomplishments
- `csv_ingest_customers.py`: `wait_for_files >> list_matched_keys >> integrity_gate >> discover` replaces the prior direct `wait_for_files >> discover` edge; `discover` structurally cannot run for a file the LOAD-10 gate rejects.
- `csv_ingest_customers.py`'s `ingest` task now declares `outlets=[customers_asset]`, making it a real Airflow Asset producer.
- `csv_ingest_orders.py` created: the second real ingestion DAG, mirroring `csv_ingest_customers`'s full task shape (`resolve_window`, gate chain, `discover`, mapped `ingest`, `aggregate_receipts`), `dataset="orders"` substituted throughout, `schedule=[customers_asset]` instead of a cron string.
- `tests/unit/test_dag_structure.py` extended with three new tests proving the gate chain and Asset coupling structurally (no live cluster): `test_integrity_gate_upstream_of_discover`, `test_orders_dag_present_and_asset_scheduled`, `test_customers_ingest_declares_outlets`. All prior tests (namespace/service-account/resources/retries loops) picked up `csv_ingest_orders` automatically via the extended `_BOTH_DAG_IDS` constant with no per-test changes needed.
- Incidentally closed the pre-existing ORCH-06 line-budget deferred item on `csv_ingest_customers.py` (162 -> 149 lines) since this plan already modified that file.

## Task Commits

Each task was committed atomically:

1. **Task 1: list_matched_keys -> integrity_gate wiring + outlets on csv_ingest_customers; new csv_ingest_orders DAG** - `4937ef6` (feat)
2. **Task 2: test_dag_structure.py — both DAGs present, gate wiring, Asset coupling** - `f866cf5` (test)

**Plan metadata:** (this commit) `docs(08-12): complete integrity-gate DAG wiring plan`

## Files Created/Modified
- `airflow/dags/csv_ingest_orders.py` - New DAG (D-14): same trigger/gate/task shape as `csv_ingest_customers.py`, `dataset="orders"`, `schedule=[customers_asset]`, no `outlets=` of its own (D-16)
- `airflow/dags/csv_ingest_customers.py` - Added `list_matched_keys`/`integrity_gate` wiring ahead of `discover` (D-18), `customers_asset` module-level Asset declaration, `outlets=[customers_asset]` on `ingest` (D-15); condensed docstring/comments to close the pre-existing ORCH-06 line-budget gap
- `tests/unit/test_dag_structure.py` - `_BOTH_DAG_IDS` now includes `csv_ingest_orders`; three new structural tests for the gate chain and Asset coupling
- `.planning/phases/08-validation-quarantine-metadata-control-plane-completion/deferred-items.md` - Marked the `csv_ingest_customers.py` line-budget item RESOLVED

## Decisions Made
- **Asset coupling by URI, not cross-file Python import.** The plan's literal text suggested `from csv_ingest_customers import customers_asset`. A live `DagBag(dag_folder="airflow/dags")` run during Task 1 proved this causes `AirflowDagDuplicatedIdException`: Airflow's `DagBag` gives every DAG file its own unique module name when parsing, so a plain `import csv_ingest_customers` from inside `csv_ingest_orders.py` does a *fresh*, second import of that file under the plain module name, re-executing its top-level `csv_ingest_customers()` call and re-registering the `csv_ingest_customers` dag_id a second time. Fixed by having `csv_ingest_orders.py` independently construct its own `Asset("s3://normalized/customers")` object — Airflow's Asset-scheduling/outlet matching keys on URI, not object identity, so this schedules identically without the collision. Documented at length in both DAG files' module docstrings so a future editor doesn't "fix" this back into a cross-file import.
- **Closed the ORCH-06 line-budget gap inline.** `csv_ingest_customers.py` was already 162 lines (over the 150-line budget) before this plan touched it, tracked in `deferred-items.md` since plan 08-02. Because this plan's own Task 1 already modifies that file, condensing its docstring and inline comments (removing zero functional lines) to bring it to 149 lines was genuinely in-scope work, not new scope — `deferred-items.md` updated to mark it resolved.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Asset coupling via cross-file import caused a duplicate dag_id**
- **Found during:** Task 1 (wiring `csv_ingest_orders.py`'s `schedule=[customers_asset]`)
- **Issue:** The plan's literal text specified `from csv_ingest_customers import customers_asset`. Running `DagBag(dag_folder="airflow/dags").import_errors` showed `AirflowDagDuplicatedIdException: Ignoring DAG csv_ingest_customers from .../csv_ingest_customers.py - also found in .../csv_ingest_orders.py` — the plan's own acceptance criterion (`import_errors == {}` for both files) failed.
- **Fix:** `csv_ingest_orders.py` declares its own `Asset("s3://normalized/customers")` object (same URI, independently constructed) instead of importing the one declared in `csv_ingest_customers.py`. Verified live: `dagbag.dags["csv_ingest_orders"].timetable` is `AssetTriggeredTimetable` with `asset_condition.objects` containing an `Asset` whose `.uri == "s3://normalized/customers"`, and `import_errors == {}`.
- **Files modified:** `airflow/dags/csv_ingest_orders.py`
- **Verification:** `pytest tests/unit/test_dag_structure.py -x` green (10/10), including the new `test_orders_dag_present_and_asset_scheduled`
- **Committed in:** `4937ef6` (Task 1 commit)

**2. [Rule 2 - Missing Critical] Closed the pre-existing ORCH-06 line-budget violation on a file this plan already modifies**
- **Found during:** Task 1 (editing `csv_ingest_customers.py`)
- **Issue:** `deferred-items.md` already tracked `csv_ingest_customers.py` at 162 lines against ORCH-06's <150-line budget (`tests/policy/test_dag_line_budget.py` failing), left unresolved since plan 08-02. This plan's own edits would have pushed the file further over budget.
- **Fix:** Condensed the module docstring and inline comments (no functional-line removal) while adding the new gate-wiring/outlets code; file is now 149 lines.
- **Files modified:** `airflow/dags/csv_ingest_customers.py`, `.planning/phases/08-validation-quarantine-metadata-control-plane-completion/deferred-items.md`
- **Verification:** `pytest tests/policy/test_dag_line_budget.py -q` passes (was failing before this plan)
- **Committed in:** `4937ef6` (Task 1 commit); deferred-items.md update in this plan's final commit

---

**Total deviations:** 2 auto-fixed (1 bug fix, 1 missing-critical closure of a flagged pre-existing gap)
**Impact on plan:** Both necessary for the plan's own stated acceptance criteria (`import_errors == {}`, structural gate proof) and for closing a gap the plan's own briefing explicitly asked this session to consider. No scope creep beyond the file this plan already owned.

## Issues Encountered
None beyond the deviation above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Both DAGs (`csv_ingest_customers`, `csv_ingest_orders`) import cleanly, are gated by LOAD-10's integrity check ahead of `discover`, and are coupled via a real Airflow Asset -- ready for plan 08-13's `dag.test()` proof and plan 08-14's live cluster proof.
- Full repo test suite run during verification: 608 passed, 3 pre-existing failures unrelated to this plan (`tests/policy/test_manifest_validation_fails_closed.py` -- missing local `tools/bin/kubeconform` binary, requires `make manifests`/`install_kubeconform.sh`, out of this plan's scope).
- `ruff check` clean on all files this plan touched. `mypy` run against `airflow/dags/*.py` shows pre-existing untyped-kwargs/XComArg noise identical in shape to the unmodified parts of `csv_ingest_customers.py` -- `airflow/dags` is not part of `make typecheck`'s `TYPECHECK_PATHS` (packages/dataplat/src, packages/csv-processor/src, tools only), so this is expected, not a regression.

---
*Phase: 08-validation-quarantine-metadata-control-plane-completion*
*Completed: 2026-08-17*

## Self-Check: PASSED

- FOUND: airflow/dags/csv_ingest_orders.py
- FOUND: airflow/dags/csv_ingest_customers.py
- FOUND: tests/unit/test_dag_structure.py
- FOUND: commit 4937ef6
- FOUND: commit f866cf5
