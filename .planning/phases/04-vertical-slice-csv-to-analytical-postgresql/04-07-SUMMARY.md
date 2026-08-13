---
phase: 04-vertical-slice-csv-to-analytical-postgresql
plan: 07
subsystem: orchestration
tags: [airflow, taskflow, kubernetespodoperator, s3keysensor, dagbag, import-linter, dynamic-task-mapping]

# Dependency graph
requires:
  - phase: 04-02
    provides: "etl namespace RBAC (kubernetes/rbac-etl.yaml), dev-only credential Secrets, Helm DAG-mount wiring, csv_processor_image Airflow Variable set by `make image-csv-processor`"
  - phase: 04-03
    provides: "AssignmentDocument/Receipt contracts and discover_files's frozen-manifest units (assignment_uri, idempotency_key, run_id)"
  - phase: 04-05
    provides: "dataplat discover/ingest CLI subcommands (entry-point plugin wiring), run_ingest orchestration, Receipt XCom shape"
provides:
  - "airflow/dags/smoke_kubernetes_pod.py -- the permanent U1 platform smoke test (26 lines)"
  - "airflow/dags/csv_ingest_customers.py -- the vertical-slice DAG: S3KeySensor -> discover -> build_ingest_args -> ingest (expanded) -> aggregate_receipts (148 lines)"
  - "airflow/dags/_common/kpo.py -- common_kpo_kwargs, the one shared non-business-logic KPO-construction helper"
  - "setup.cfg import-linter Contract 2 -- nothing may import the DAG folder (verified both directions live)"
  - "tests/unit/conftest.py -- the shared DagBag-loading fixture (sys.path + AIRFLOW_VAR_CSV_PROCESSOR_IMAGE) every DAG-structure test reuses"
  - "tests/unit/test_dag_structure.py, tests/unit/test_resolve_window.py, tests/policy/test_dag_thinness.py, tests/policy/test_dag_line_budget.py -- 15 fast, offline, no-live-cluster tests proving ORCH-01..09"
  - "pyproject.toml dev group gains apache-airflow==3.3.0 + cncf-kubernetes/amazon providers (dev/test tooling only, never a runtime image)"
affects: [04-08-e2e-suite, 04-09-make-ingest-demo]

# Tech tracking
tech-stack:
  added:
    - "apache-airflow==3.3.0 (dev dependency group -- DAG authoring/testing only, never installed into any runtime image)"
    - "apache-airflow-providers-cncf-kubernetes==10.19.0"
    - "apache-airflow-providers-amazon (unpinned, resolver-selected compatible version)"
  patterns:
    - "TaskFlow API DAG authoring: @dag/@task from airflow.sdk exclusively, never airflow.decorators/airflow.models (STACK.md gotcha 1)"
    - "airflow.sdk.Variable over airflow.models.Variable -- the latter is deprecated in 3.3.0 and internally delegates to the former"
    - "_common/kpo.py: one shared, explicitly-non-business-logic KPO-kwargs builder, exempted BY NAME (not by omission) from the DAG-thinness import scan"
    - "Offline DagBag structural testing: session-scoped conftest fixture inserts airflow/dags onto sys.path and sets AIRFLOW_VAR_<KEY> env vars before construction -- no live metadata DB or API server needed"
    - "Dynamic Task Mapping: KubernetesPodOperator.partial(...).expand(arguments=build_ingest_args(discover.output)), fan-out bounded upstream by discover_files's batching.max_units_per_run, never by anything in the DAG file"

key-files:
  created:
    - airflow/dags/smoke_kubernetes_pod.py
    - airflow/dags/csv_ingest_customers.py
    - airflow/dags/_common/__init__.py
    - airflow/dags/_common/kpo.py
    - tests/unit/conftest.py
    - tests/unit/test_dag_structure.py
    - tests/unit/test_resolve_window.py
    - tests/policy/test_dag_thinness.py
    - tests/policy/test_dag_line_budget.py
  modified:
    - setup.cfg
    - docker/csv-processor/Dockerfile
    - pyproject.toml
    - uv.lock

key-decisions:
  - "Added apache-airflow + cncf-kubernetes/amazon providers to pyproject.toml's `dev` group (not a new opt-in group) so the plan's own bare `uv run --frozen` verify commands work, and so DagBag-based structural tests actually run (not skip) under `make check`'s environment -- resolved with zero version conflicts against dataplat/csv-processor's existing pins."
  - "Used airflow.sdk.Variable instead of the plan's literal airflow.models.Variable, resolving the plan's own explicit uncertainty note in favor of the empirically-verified non-deprecated path."
  - "Moved the DagBag fixture into tests/unit/conftest.py rather than importing it across test modules (the plan's literal suggestion), avoiding a ruff F811 false-positive against pytest's cross-module fixture-injection pattern -- still exactly one shared loading mechanism."
  - "Dropped tags=[...] from smoke_kubernetes_pod.py's @dag(...) call to stay under the 30-line hard budget once ruff-format's line-wrapping was accounted for; dag_id also relies on @dag's function-name default (verified) rather than being spelled out, for the same reason."

patterns-established:
  - "Pattern: DAG-authoring dependencies (apache-airflow + providers) live in the same workspace `dev` group as ETL-library dev tooling, but are NEVER installed into either runtime image (csv-processor's Dockerfile passes --no-dev; Airflow's own image is the unmodified upstream image) -- ADR-0004's two-images-two-dependency-sets guarantee holds structurally, not by omission from dev tooling."
  - "Pattern: any future DAG-folder policy/structural test reuses tests/unit/conftest.py's `dagbag` fixture rather than re-deriving the sys.path/env-var setup."

requirements-completed: [ORCH-01, ORCH-02, ORCH-03, ORCH-04, ORCH-05, ORCH-06, ORCH-07, ORCH-08, ORCH-09]

# Metrics
duration: ~65min
completed: 2026-08-13
---

# Phase 04 Plan 07: DAG Files, KPO Helper & Import-Linter Contract 2 Summary

**Two thin TaskFlow DAGs (26-line U1 smoke test, 148-line customers vertical slice with deferrable S3KeySensor -> KPO discover -> Dynamic-Task-Mapped KPO ingest -> receipt aggregation) plus import-linter Contract 2, proven by 15 offline DagBag-based tests -- zero business logic in the DAG folder, zero live cluster needed to verify it.**

## Performance

- **Duration:** ~65 min
- **Completed:** 2026-08-13
- **Tasks:** 2/2 completed
- **Files modified:** 13 (9 created, 4 modified)

## Accomplishments

- `airflow/dags/smoke_kubernetes_pod.py` (26 lines) and `airflow/dags/csv_ingest_customers.py` (148 lines) both exist, parse cleanly under a real `DagBag`, and stay within their ORCH-06 line budgets.
- `csv_ingest_customers` implements the full D-01..D-04 trigger design and ORCH-01..09 task graph: deferrable `S3KeySensor` (30s poke) → independent `resolve_window` (ORCH-05 proof) → `discover` (one `KubernetesPodOperator`, folding `resolve_config`+`discover_files`) → `build_ingest_args` → `ingest` (Dynamic-Task-Mapped `KubernetesPodOperator`, bounded upstream by `discover_files`'s own cap) → `aggregate_receipts`. `max_active_runs=1`.
- `airflow/dags/_common/kpo.py` centralizes every KPO kwarg both tasks share (namespace, service account, image Variable, XCom/finish-action settings, the four Secret-backed env vars) -- proven to contain no business logic both by construction and by a named exemption in the policy scan.
- `setup.cfg` now enforces import-linter Contract 2 ("nothing may import the DAG folder") -- verified live in both directions: a fake `import dags` injected into `dataplat/errors.py` was correctly caught (including transitively through `csv_processor.cli`), then reverted.
- 15 new tests (`tests/unit/test_dag_structure.py`, `tests/unit/test_resolve_window.py`, `tests/policy/test_dag_thinness.py`, `tests/policy/test_dag_line_budget.py`) prove every ORCH requirement offline, via a shared `tests/unit/conftest.py` `DagBag` fixture -- no live cluster, no Docker daemon, no `cluster` dependency group installed.

## Task Commits

Each task was committed atomically:

1. **Task 1: The two DAG files and the shared KPO-kwargs helper** - `2bedbae` (feat)
2. **Task 2: Structural, policy and unit tests proving every ORCH requirement** - `4e1e752` (test)

**Plan metadata:** commit pending (this SUMMARY + deferred-items.md update)

## Files Created/Modified

- `airflow/dags/smoke_kubernetes_pod.py` - U1 permanent smoke test: one KPO task writing the built image's `$GIT_SHA` to the XCom sidecar
- `airflow/dags/csv_ingest_customers.py` - the vertical-slice DAG (ORCH-01..09, D-01..D-04)
- `airflow/dags/_common/__init__.py` - empty package marker
- `airflow/dags/_common/kpo.py` - `common_kpo_kwargs`, the one shared KPO-construction helper
- `setup.cfg` - import-linter Contract 2 + `include_external_packages = True`
- `docker/csv-processor/Dockerfile` - added `ENV GIT_SHA=${GIT_SHA}` to the runtime stage (U1's smoke-test consumer)
- `pyproject.toml` - `dev` group gains `apache-airflow==3.3.0` + cncf-kubernetes/amazon providers
- `uv.lock` - regenerated (206 packages resolved, zero conflicts with existing pins)
- `tests/unit/conftest.py` - shared `dagbag` session-scoped fixture
- `tests/unit/test_dag_structure.py` - 7 structural tests (import errors, both dags present, retries, KPO resources, sensor config, `max_active_runs`, namespace/SA)
- `tests/unit/test_resolve_window.py` - 3 tests proving ORCH-05 (`logical_date=None` never raises)
- `tests/policy/test_dag_thinness.py` - 3 tests (no business-logic imports, no raw SQL strings, anti-vacuity)
- `tests/policy/test_dag_line_budget.py` - 2 tests mechanically enforcing the line budgets

## Decisions Made

- **`apache-airflow` joins the `dev` dependency group, not a new opt-in group.** The plan's own `<verify>` block invokes a bare `uv run --frozen` with no `--group` flag, and Task 2's acceptance criteria require these tests to *pass* (not skip) under exactly that invocation. Verified this introduces zero conflicts with `dataplat`/`csv-processor`'s own pins (`boto3`, `pydantic`, `psycopg`, `click`, `PyYAML`, `structlog` all resolved unchanged) and never reaches either runtime image (csv-processor's Dockerfile passes `--no-dev`; Airflow's own image is the unmodified upstream image per STACK.md) -- ADR-0004's "two images, two dependency sets" holds structurally.
- **`airflow.sdk.Variable` over `airflow.models.Variable`.** The plan flagged this as an open question ("check which the pinned provider version expects"). Reading the installed `airflow.models.Variable.get()` source shows it is explicitly deprecated in 3.3.0 in favor of `airflow.sdk.Variable.get()`, and internally delegates to it whenever running inside any real execution context anyway -- confirms 04-RESEARCH.md's own "never `airflow.models`" gotcha extends to `Variable`, not just `dag`/`task`.
- **Shared `DagBag` fixture lives in `tests/unit/conftest.py`, not inside `test_dag_structure.py` with a cross-module import.** The plan suggested the latter; it produces a ruff `F811` false-positive (ruff has no built-in model of pytest's same-named-parameter fixture injection). `conftest.py` is the standard pytest mechanism for exactly this sharing need and keeps it to one mechanism, not two.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Added `apache-airflow` + providers to `pyproject.toml`'s `dev` group**
- **Found during:** Task 1 (writing the DAG files) / Task 2 (writing the DagBag-based tests)
- **Issue:** `apache-airflow` was not installed anywhere in this workspace (`uv.lock` had no entry, no `.venv` existed). Neither the DAG files' own imports nor Task 2's `DagBag`-based tests can work without it, and the plan's own `<verify>` block assumes a bare `uv run --frozen` already has it.
- **Fix:** Added `apache-airflow==3.3.0`, `apache-airflow-providers-cncf-kubernetes==10.19.0`, `apache-airflow-providers-amazon` (unpinned) to the `dev` dependency group, matching STACK.md's pinned versions; ran `uv lock` then `uv sync --locked`.
- **Files modified:** `pyproject.toml`, `uv.lock`
- **Verification:** `uv lock --check` reports the lockfile fresh; all of `dataplat`/`csv-processor`'s own dependency pins resolved unchanged; full `ruff check .`, `ruff format --check .`, `mypy` (existing `TYPECHECK_PATHS`), `lint-imports`, `tests/unit`+`tests/regression` (136 passed), and `tests/policy` (116 passed, 2 pre-existing unrelated failures -- see Issues Encountered) all green afterward.
- **Committed in:** `2bedbae` (Task 1 commit)

**2. [Rule 3 - Blocking] Added `ENV GIT_SHA=${GIT_SHA}` to `docker/csv-processor/Dockerfile`'s runtime stage**
- **Found during:** Task 1, per the plan's own explicit instruction to check for and add this if missing
- **Issue:** The runtime stage declared `ARG GIT_SHA` (used only in the `LABEL` instruction) but never promoted it to an `ENV`, so `$GIT_SHA` would be unset inside the running container -- silently defeating U1's own pass criteria ("the XCom contains the SHA that was built").
- **Fix:** Added `ENV GIT_SHA=${GIT_SHA}` immediately after the `ARG` redeclaration in the runtime stage, with a comment citing the smoke DAG as the consumer.
- **Files modified:** `docker/csv-processor/Dockerfile`
- **Verification:** Read-through only (no image build available in this offline execution); the fix is a standard, well-understood Docker `ARG`→`ENV` promotion pattern.
- **Committed in:** `2bedbae` (Task 1 commit)

**3. [Rule 1 - Bug] Added `retries`/`retry_exponential_backoff` to `wait_for_files` (`S3KeySensor`)**
- **Found during:** Task 1, while cross-checking against Task 2's own test spec before writing it
- **Issue:** The Interfaces section's `S3KeySensor` snippet omitted `retries`, but Task 2's own `test_retries_set` spec explicitly requires every `S3KeySensor`/`KubernetesPodOperator` instance in the DAG to carry a positive `retries` value.
- **Fix:** Added `retries=2, retry_exponential_backoff=True` to `wait_for_files`.
- **Files modified:** `airflow/dags/csv_ingest_customers.py`
- **Verification:** `tests/unit/test_dag_structure.py::test_retries_set` passes.
- **Committed in:** `2bedbae` (Task 1 commit)

**4. [Rule 1 - Bug] Added `include_external_packages = True` to `setup.cfg`'s `[importlinter]` section**
- **Found during:** Task 1, running `lint-imports` after adding Contract 2
- **Issue:** import-linter refused to evaluate Contract 2 at all ("The top level configuration must have `include_external_packages=True` when there are external forbidden modules") because `dags` (Contract 2's `forbidden_modules`) is deliberately not a `root_packages` entry -- undocumented in 01-RESEARCH.md's own verified `setup.cfg` config, discovered only by running the real tool.
- **Fix:** Added the flag at `[importlinter]` top level. `dags` was NOT added to `root_packages`, per the plan's explicit instruction -- this is a separate, compatible mechanism.
- **Files modified:** `setup.cfg`
- **Verification:** `lint-imports` now reports `2 kept, 0 broken`; a live negative-case injection (`import dags` added to `dataplat/errors.py`) was correctly caught and reported, then reverted.
- **Committed in:** `2bedbae` (Task 1 commit)

---

**Total deviations:** 4 auto-fixed (2 Rule 3 blocking-issue, 2 Rule 1 bug fixes)
**Impact on plan:** All four were necessary for the plan's own stated verification commands and Task 2's own test spec to be satisfiable at all. No scope creep -- every fix stays inside this plan's stated files or the one pre-existing file (`pyproject.toml`/`uv.lock`) required to make DAG-authoring/testing possible in this repository for the first time.

## Issues Encountered

- **Two genuine Airflow 3.3.0 API-signature discoveries, not deviations** (no plan text asserted the opposite; the plan's own verify text and CLAUDE.md both show an API shape that does not exist in the pinned version): `DagBag.__init__` has no `include_examples` keyword argument in `apache-airflow==3.3.0` (passing it raises `TypeError`) -- worked around by omitting it, since an explicit `dag_folder` already scopes the scan correctly. And `airflow/dags` is not automatically on `sys.path` outside a real Airflow process (whose own startup adds the configured `dags_folder`) -- worked around with an explicit `sys.path.insert` in the shared test fixture.
- **`tests/policy/test_gates_actually_fail.py::test_forbidden_import_is_rejected` / `test_good_forbidden_import_is_accepted` fail, pre-existing and unrelated.** Root cause: this sandboxed execution environment sets `FORCE_COLOR=3`, which the test's `_run()` helper inherits into the `lint-imports` subprocess it spawns, producing ANSI-colored output that breaks a plain-substring assertion. Confirmed pre-existing (file last touched by Phase 1 commit `edf4756`; already independently reproduced and logged by plans 04-01, 04-02 and 04-03 in this same phase) and confirmed unrelated to this plan's diff (this plan's own real `setup.cfg` contracts are independently verified `KEPT`/working via a live negative-case injection test, separate from this pre-existing test's own scratch-package harness). Logged to `deferred-items.md` per the scope-boundary rule; not fixed.
- **`airflow dags list` against a live/locally-configured Airflow (Task 1's second acceptance-criteria line) was not run** -- no live cluster or Airflow deployment is available in this offline plan-execution environment. The equivalent, stronger offline proof (`DagBag(dag_folder=...).import_errors == {}`, run directly against the real pinned Airflow package, plus 15 passing structural tests) was performed instead; live verification is naturally in scope for 04-08's E2E suite, which does have a live cluster.

## User Setup Required

None - no external service configuration required. (The `apache-airflow` dependency addition is a local `uv sync` effect only; no manual step needed beyond what `make install` already does.)

## Next Phase Readiness

- Both DAG files are ready for 04-08's live-cluster E2E suite (pod-kill/retry, concurrent-SELECT, idempotent reupload, U1/U3 spike results) to exercise them for real against a running kind cluster.
- `csv_ingest_customers.py` is ready for 04-09's `make ingest-demo` target to trigger via the real `S3KeySensor` path (no CLI-trigger shortcut, per D-15).
- No blockers. The one open item (`airflow dags list` live verification) is naturally covered by 04-08's live-cluster context, not a gap this plan could have closed without one.

## Self-Check: PASSED

- All 9 created files confirmed tracked via `git ls-files` (4 DAG/helper files, 5 test files).
- All 4 referenced commit hashes (`2bedbae`, `4e1e752`, `7df6a41`, `6f17673`) confirmed present via `git log --oneline --all`.
- No missing items.

---
*Phase: 04-vertical-slice-csv-to-analytical-postgresql*
*Completed: 2026-08-13*
