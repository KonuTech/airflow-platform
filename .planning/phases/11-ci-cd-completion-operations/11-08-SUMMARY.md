---
phase: 11-ci-cd-completion-operations
plan: 08
subsystem: infra
tags: [airflow, retention, dag, adr-0004, dagtest, minio, postgres]

# Dependency graph
requires:
  - phase: 11-ci-cd-completion-operations
    provides: "plan 11-07's RetentionConfig/evaluate_retention (dataplat.retention.policy) -- a pure, zero-I/O, dry-run-by-default evaluator this plan wires into a real DAG"
provides:
  - "airflow/dags/platform_retention.py: a dedicated, @daily maintenance DAG, structurally separate from every ingestion DAG's task graph (D-35)"
  - "airflow/dags/_common/retention_query.py: the fifth ADR-0004 exception -- queries all six retention layers (MinIO for raw/processed/quarantine, SQL age queries for validation_reports/ingestion_metadata, an honest no-op for logs), feeds evaluate_retention, logs the structured report, and performs an actual delete only when a dataset's enforce: true"
  - "tests/dagtest/test_platform_retention_dagrun.py: dag.test()-based proof that the DagRun reaches success and that the default (dry-run) configuration issues zero delete-shaped calls, genuinely inspected via a fake cursor's own call log"
  - "A pre-existing, previously-undetected gap_recorder.py policy-test exemption gap fixed as a same-mechanism adjacent bug (Rule 1)"
affects: [11-06-dagtest-ci-job]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Dataset configuration resolved from meta.config_versions (the CURRENT, valid_to IS NULL row), never from configs/datasets/*.yaml on disk -- mirrors ConfigRegistry.get_by_id's identical reasoning: only the dags hostPath is mounted into any Airflow pod, not configs/"
    - "A DAG's business logic lives in its own _common/*.py submodule (never inlined in the top-level DAG file) specifically so tests/dagtest/ fixtures can patch it BEFORE load_dag() triggers Airflow's DagBag to freshly re-exec the top-level DAG file -- a submodule import is cached normally and survives that re-exec, matching mock_run_stage_recorder_db's own proven pattern"
    - "A dedicated _connect(dsn) seam, never psycopg.connect called inline, so a test can replace the DB connection without patching the global psycopg.connect Airflow's own metadata-DB dialect also depends on"

key-files:
  created:
    - airflow/dags/platform_retention.py
    - airflow/dags/_common/retention_query.py
    - tests/dagtest/test_platform_retention_dagrun.py
  modified:
    - tests/policy/test_dag_thinness.py
    - tests/policy/test_dag_line_budget.py

key-decisions:
  - "Split the DAG into a thin platform_retention.py (@dag wrapper only) plus a _common/retention_query.py submodule holding all query/evaluate/delete logic -- not the single-file shape the plan's own action text first suggested. Needed for two independent reasons: (1) genuine testability, since Airflow's DagBag freshly re-execs a top-level DAG file on every load_dag() call, so patches applied to functions defined directly IN that file would not survive into a dag.test() run, while a _common/ submodule's functions are cached normally and DO survive (mock_run_stage_recorder_db's own proven precedent); (2) it exactly matches this codebase's own established convention (csv_ingest_customers.py imports and wires _common/integrity_gate.py's tasks rather than defining them inline)."
  - "Dataset configuration is read from meta.config_versions (Postgres), never configs/datasets/*.yaml on disk -- confirmed via ConfigRegistry.get_by_id's own documented reasoning and via helm/values/*/airflow.yaml (only the dags hostPath is mounted into any Airflow pod; configs/ is not)."
  - "raw/processed/quarantine query MinIO directly; validation_reports/ingestion_metadata query SQL (meta.validation_results/meta.files/meta.ingestion_runs); logs is an honest structural no-op -- exactly the plan's own instructed layer-to-source mapping."
  - "ingestion_metadata candidate identifiers carry a file:/run: prefix so a later conditional delete never sends one table's numeric id to the other table's DELETE statement (a real correctness bug caught and fixed before it was ever executed)."
  - "Left meta.files/meta.validation_results/meta.ingestion_runs's existing etl_app grants (SELECT/INSERT/UPDATE only, no DELETE) unchanged rather than adding a migration to grant DELETE -- documented as a deliberate, in-place defense-in-depth finding (T-11-21) rather than fixed, since granting DELETE preemptively while enforce stays dry-run-by-default everywhere today would be an unforced privilege-widening."

patterns-established:
  - "Pattern: a maintenance DAG's own I/O logic lives in a _common/*.py submodule (never the top-level DAG file itself) specifically for dag.test() testability, not merely for line-budget thinness."

requirements-completed: [INFRA-11]

# Metrics
duration: ~35min
completed: 2026-08-23
---

# Phase 11 Plan 08: Retention Maintenance DAG (D-35) Summary

**A dedicated `platform_retention` DAG wires plan 11-07's pure evaluator into real MinIO/PostgreSQL queries across all six retention layers, proven dry-run-safe by default via `dag.test()`, with all query/delete logic isolated in a `_common/` submodule for genuine testability and structural separation from the ingest pipeline.**

## Performance

- **Duration:** ~35 min (exploration + implementation + verification, commit-to-commit span ~30 min)
- **Started:** 2026-08-23T12:2x (research/read phase)
- **Completed:** 2026-08-23T12:55:29+02:00 (last commit)
- **Tasks:** 2/2 completed
- **Files modified:** 5 (3 created, 2 modified)

## Accomplishments

- `airflow/dags/platform_retention.py`: a thin `@daily` `@dag` wrapper (46 lines), tagged `["maintenance", "retention"]`, structurally separate from every ingestion DAG's task graph (D-35) -- never imported by, never imports from, `csv_ingest_customers.py`/`csv_ingest_orders.py`
- `airflow/dags/_common/retention_query.py`: the actual per-run logic. Resolves every active dataset's CURRENT config from `meta.config_versions` (never `configs/datasets/*.yaml` on disk -- that path is not mounted into any Airflow pod), queries all six retention layers per D-38's mapping (MinIO `list_objects_v2` for raw/processed/quarantine; SQL age queries against `meta.validation_results`/`meta.files`/`meta.ingestion_runs` for validation_reports/ingestion_metadata; an honest structural no-op for logs), feeds candidates to `evaluate_retention`, logs the full structured report, and performs an actual delete ONLY when a dataset's `enforce: true` (never true in any currently-committed dataset config)
- `tests/dagtest/test_platform_retention_dagrun.py`: `dag.test()` proves (1) the DagRun reaches `success` with `run_retention` succeeding, and (2) with a real over-window MinIO candidate present, `enforce=False` genuinely issues zero delete-shaped calls -- verified via a fake cursor's own recorded SQL call log, not merely the absence of an exception
- Found and fixed a genuine, pre-existing gap in `tests/policy/test_dag_thinness.py`: `_common/gap_recorder.py` (plan 09-10) was never added to either by-name exemption list, so both policy tests were already failing on `main` before this plan touched anything -- fixed as a same-mechanism, directly-in-scope adjacent bug (Rule 1)

## Task Commits

Each task was committed atomically:

1. **Task 1: platform_retention DAG** - `f5356f2` (feat)
2. **Task 2: DAG-structure proof + line-budget policy test** - `2f55515` (test)

## Files Created/Modified

- `airflow/dags/platform_retention.py` - Thin `@dag` wrapper; imports and wires `_common.retention_query.run_retention`
- `airflow/dags/_common/retention_query.py` - The fifth ADR-0004 exception: all query/evaluate/delete logic for D-35/D-38
- `tests/dagtest/test_platform_retention_dagrun.py` - `dag.test()`-based DAG-mechanics + dry-run-safety proof
- `tests/policy/test_dag_thinness.py` - Adds `_common/retention_query.py` (and the pre-existing `_common/gap_recorder.py` gap) to both by-name exemption lists
- `tests/policy/test_dag_line_budget.py` - Adds `test_platform_retention_stays_under_60_lines`

## Decisions Made

- **Split into `platform_retention.py` + `_common/retention_query.py`, not a single file.** The plan's own action text first suggested either an in-DAG `@task` or a KubernetesPodOperator invoking a new CLI subcommand; neither fit `files_modified`'s declared scope (no new CLI file) cleanly, and a single-file DAG would not have survived `dag.test()`-based patching (Airflow's `DagBag` freshly re-execs a top-level DAG file on every `load_dag()` call, so functions defined directly in that file cannot be reliably patched before a `dag.test()` run). Moving the logic into a `_common/` submodule — exactly matching `csv_ingest_customers.py`'s own established "DAG file imports and wires a `_common/`-defined task" convention — solved both problems at once.
- **Dataset config resolved from `meta.config_versions`, never `configs/*.yaml` on disk.** Verified directly (not assumed) that only the `dags` hostPath is mounted into any Airflow pod (`helm/values/*/airflow.yaml`); `dataplat.config.registry.ConfigRegistry.get_by_id`'s own docstring independently confirms and explains the identical constraint and resolution.
- **`ingestion_metadata` candidate identifiers carry a `file:`/`run:` prefix.** Caught before writing any delete logic: combining `meta.files.file_id` and `meta.ingestion_runs.run_id` into one layer's candidate list without a namespaced identifier would let a numerically-colliding id from one table incorrectly trigger a `DELETE` against the other table.
- **Left `etl_app`'s existing DELETE-less grants on `meta.files`/`meta.validation_results`/`meta.ingestion_runs` unchanged.** Documented as a deliberate defense-in-depth finding (this DAG's own DB credential cannot actually delete those rows even with `enforce: true`, backstopping T-11-21 one layer further than asked), not fixed with a new migration -- granting DELETE preemptively while `enforce` stays `False` everywhere today would be an unforced privilege-widening.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed a pre-existing `tests/policy/test_dag_thinness.py` exemption-list gap for `_common/gap_recorder.py`**
- **Found during:** Task 1, while verifying `test_dag_thinness.py` against the new DAG
- **Issue:** `_common/gap_recorder.py` (plan 09-10, D-06) legitimately imports `psycopg` and contains raw SQL, and its own module docstring self-identifies as "A FOURTH, narrowly-scoped exception" -- but it was never actually added to either `_EXEMPT_FROM_IMPORT_CHECK`/`_EXEMPT_FROM_SQL_CHECK` frozenset. Confirmed via `git stash -u` against a clean `main` that `test_no_business_logic_imports`/`test_no_raw_sql_strings` were ALREADY failing before this plan touched anything.
- **Fix:** Added `airflow/dags/_common/gap_recorder.py` to both exemption lists, with the same documented justification style as the other entries.
- **Files modified:** tests/policy/test_dag_thinness.py
- **Verification:** `uv run pytest tests/policy/test_dag_thinness.py -q` passes (3/3).
- **Committed in:** f5356f2 (Task 1 commit)

**2. [Design refinement, not a plan deviation per se] Split single-file DAG into `platform_retention.py` + `_common/retention_query.py`**
- **Found during:** Task 2, while designing the `dag.test()`-based test
- **Issue:** A single-file design (all query/evaluate/delete logic inline in `platform_retention.py`, the plan's `files_modified` list's literal scope) could not be reliably patched for `dag.test()` testing, since Airflow's `DagBag` freshly re-execs the top-level DAG file on every `load_dag()` call.
- **Fix:** Moved all logic into a new `airflow/dags/_common/retention_query.py` submodule (not in the plan's declared `files_modified`, but directly necessitated by Task 2's own explicit `dag.test()` requirement and precedented by 4 existing `_common/` files following the identical shape).
- **Files modified:** airflow/dags/platform_retention.py, airflow/dags/_common/retention_query.py (new)
- **Verification:** `dag.test()` proof passes cleanly; DagBag import check clean; `test_dag_thinness.py`/`test_dag_line_budget.py` pass.
- **Committed in:** f5356f2 (Task 1 commit)

---

**Total deviations:** 2 (1 Rule 1 pre-existing-bug fix, 1 architectural refinement necessitated by Task 2's own testability requirement)
**Impact on plan:** Both are minor in scope and directly serve the plan's own stated goals (a genuinely dag.test()-provable DAG; a clean policy-test baseline). No unrelated scope creep.

## Issues Encountered

- `csv_ingest_customers.py` is already 182 lines against its own committed `<=152` line-budget test, and `make lint` is separately red on 3 pre-existing findings unrelated to this plan -- both confirmed via `git stash -u` to predate this plan entirely, and both already logged in `.planning/phases/11-ci-cd-completion-operations/deferred-items.md` (plan 11-01's entry). No new entry needed; left untouched per the executor's scope-boundary rule.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `platform_retention` is ready for plan 11-06's `dagtest` CI job to exercise continuously.
- D-35 is structurally proven: the retention DAG shares no task-graph edge with either ingestion DAG.
- D-38's dry-run-by-default safety is proven live, in-process, via `dag.test()` -- no real cluster run has been attempted in this plan (out of scope; `enforce` stays `False` in every currently-committed dataset config, so a live run would also be a pure dry-run).
- Known, deliberate finding for a future phase to revisit if it ever needs `enforce: true` to actually delete metadata rows: `etl_app` has no `DELETE` grant on `meta.files`/`meta.validation_results`/`meta.ingestion_runs` today (see `_common/retention_query.py`'s own module docstring).

---
*Phase: 11-ci-cd-completion-operations*
*Completed: 2026-08-23*

## Self-Check: PASSED

All 3 claimed created files verified present on disk (`airflow/dags/platform_retention.py`,
`airflow/dags/_common/retention_query.py`, `tests/dagtest/test_platform_retention_dagrun.py`).
Both claimed commit hashes verified present in git history via `git cat-file -e`
(`f5356f2`, `2f55515`).
