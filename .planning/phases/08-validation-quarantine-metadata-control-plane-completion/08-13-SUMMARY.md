---
phase: 08-validation-quarantine-metadata-control-plane-completion
plan: 13
subsystem: testing
tags: [airflow, dag-test, testcontainers, kubernetespodoperator, s3hook, pytest]

# Dependency graph
requires:
  - phase: 08-validation-quarantine-metadata-control-plane-completion
    provides: "csv_ingest_customers/csv_ingest_orders DAGs (plan 08-12) — the D-18 integrity_gate/list_matched_keys wiring and TracingKubernetesPodOperator's ingest task"
provides:
  - "tests/dagtest/ — this codebase's first dag.test()-based behavioral DAG test tier"
  - "A session-scoped testcontainers PostgreSQL 17 fixture for the Airflow metadata DB, fully independent of tests/integration/'s analytical-DB fixture"
  - "mock_kpo_execute / mock_s3_infrastructure fixtures proving backfill-DagRun mechanics without a real pod or a real MinIO"
affects: [08-14, "any future plan adding a third DAG to airflow/dags/"]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "dag.test() against a real testcontainers Airflow metadata DB, KubernetesPodOperator.execute mocked at the parent-class level so subclasses (TracingKubernetesPodOperator) are covered by one patch"
    - "Deferred (function-body-local) airflow imports throughout conftest.py — import airflow triggers settings.initialize()->configure_orm() at that exact moment, so every airflow-touching import must happen strictly after the env vars pointing at the real metadata DSN are set"
    - "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_ASYNC=\"\" disables Airflow 3.3.0's unconditional asyncpg-only async-engine derivation, keeping this project on psycopg3 with no new dependency"

key-files:
  created:
    - tests/dagtest/__init__.py
    - tests/dagtest/conftest.py
    - tests/dagtest/test_backfill_dagrun.py
  modified:
    - pyproject.toml

key-decisions:
  - "Used postgresql+psycopg:// (v3, already installed) instead of the plan's literal postgresql+psycopg2:// for AIRFLOW__DATABASE__SQL_ALCHEMY_CONN — psycopg2 was never installed in this project (CLAUDE.md's own stack table pins psycopg3 exclusively) and installing it would be a new package-manager dependency, excluded from auto-fix per the executor's Rule 3. SQLAlchemy 2.0's postgresql+psycopg dialect resolves to the same postgresql dialect.name Airflow's own code branches on, verified empirically both via a standalone create_engine() check and a full live airflow db migrate run."
  - "Set AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_ASYNC=\"\" — Airflow 3.3.0's configure_orm() unconditionally derives an async engine URL by mapping the postgresql scheme to asyncpg (airflow/settings.py::_get_async_conn_uri_from_sync), regardless of which SYNC driver was requested, which would otherwise require installing asyncpg — a driver CLAUDE.md explicitly rejects. An empty string makes _configure_async_session() skip async-engine creation entirely (verified: no asyncpg import occurs, airflow db migrate and dag.test() both succeed)."
  - "Added a second fixture, mock_s3_infrastructure (patching S3Hook.list_keys/get_conn and S3KeySensor.execute), beyond the plan's explicitly-named mock_kpo_execute — Rule 2 (missing critical functionality). Without it dag.test() still completes without raising (it swallows per-task exceptions internally), but wait_for_files/list_matched_keys/gate would all fail against a nonexistent minio_default connection, discover/ingest would never run, and the must_haves truth \"the sensor/gate/discover/ingest chain all 'ran,' per the mock\" would be silently unmet. This tier's own threat model (T-08-22) already commits to standing up no MinIO container, so doubling every S3 touchpoint is the only way to satisfy that truth."
  - "AIRFLOW__CORE__DAGS_FOLDER is set to airflow/dags explicitly — dag.test() internally re-syncs/re-serializes whichever DAG bundle owns the target DAG into the metadata DB via core.dags_folder before creating a DagRun (verified directly against the installed airflow.sdk.definitions.dag.DAG.test source), independent of whatever path a caller's own DagBag(...) construction used. Omitting this produced a real, reproduced failure: \"Cannot create DagRun ... because the dag is not serialized.\""

requirements-completed: [VALID-08]

# Metrics
duration: ~55min
completed: 2026-08-17
---

# Phase 08 Plan 13: dag.test() Backfill-DagRun Proof Summary

**Stood up `tests/dagtest/`, this codebase's first `dag.test()`-based behavioral DAG test tier, proving both ingestion DAGs' backfill-DagRun mechanics against a real testcontainers Airflow metadata database with `KubernetesPodOperator.execute` and every S3/`boto3` touchpoint mocked — zero real pod launches, zero real MinIO calls.**

## Performance

- **Duration:** ~55 min (including live empirical validation of several genuinely new API surfaces before writing final code)
- **Tasks:** 2 completed
- **Files modified:** 4 (1 modified, 3 created)

## Accomplishments

- `tests/dagtest/conftest.py` stands up a session-scoped, fully independent testcontainers PostgreSQL 17 container for the Airflow *metadata* database (never shared with `tests/integration/`'s analytical-DB fixture — CLAUDE.md Sec. 4's separation stays visible in test infrastructure, not just production)
- `airflow_env` fixture wires `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`/`AIRFLOW__CORE__EXECUTOR=LocalExecutor`/`AIRFLOW__CORE__DAGS_FOLDER` and runs `airflow db migrate` once per session, with careful import-order discipline (every airflow-touching import deferred inside a fixture body, never at module top level) so `pytest tests/dagtest/ --collect-only -q` succeeds with zero Docker
- `mock_kpo_execute` patches `KubernetesPodOperator.execute` at the parent-class level (CLAUDE.md's own documented pattern), covering both `discover` (plain KPO) and `ingest` (`TracingKubernetesPodOperator`, which only overrides `build_pod_request_obj()`) with one patch
- `mock_s3_infrastructure` doubles `S3Hook.list_keys`/`get_conn` and `S3KeySensor.execute` so `wait_for_files` → `list_matched_keys` → `integrity_gate` → `discover` → `build_ingest_args` → `ingest` → `aggregate_receipts` all genuinely run and reach `success`, never launching a real pod or touching real S3
- `test_backfill_dagrun.py` proves: `dag.test()` completes with `DagRunState.SUCCESS` and every task instance `success` for both DAGs; two different `logical_date`s produce different `dag_run_id`s but the identical task-graph shape (task_id set); `resolve_window`'s `logical_date` XCom output is non-`None` for a backfill-triggered run specifically (ORCH-05's own new angle, distinct from the existing `logical_date=None` asset-triggered proof); `discover`/`ingest` both genuinely exercised the KPO mock (asserted via the recorded-calls list), not skipped by an upstream failure

## Task Commits

1. **Task 1: tests/dagtest/ conftest.py — testcontainers Airflow metadata DB + KPO mock fixture** - `5131513` (test)
2. **Task 2: test_backfill_dagrun.py — dag.test() proves backfill DagRun mechanics** - `563e9e4` (test)

## Files Created/Modified

- `pyproject.toml` - Added the `dagtest` pytest marker (excluded from the offline gate, mirroring `integration`'s exact phrasing)
- `tests/dagtest/__init__.py` - Empty package marker (ruff `INP001` precedent, matches `tests/unit/normalize/__init__.py`)
- `tests/dagtest/conftest.py` - `_require_docker`, `airflow_metadata_dsn`, `airflow_env`, `load_dag`, `mock_kpo_execute`, `mock_s3_infrastructure` fixtures
- `tests/dagtest/test_backfill_dagrun.py` - `test_backfill_dagrun_customers_succeeds_and_is_structurally_stable`, `test_backfill_dagrun_orders_succeeds`

## Decisions Made

- **`postgresql+psycopg://` (v3) instead of the plan's literal `postgresql+psycopg2://`.** psycopg2 is not installed anywhere in this project (deliberately — CLAUDE.md's stack table pins psycopg3 exclusively across the whole codebase) and installing it would be a new package-manager dependency, which the executor's Rule 3 explicitly excludes from auto-fix (requires a human-verify checkpoint for slopsquatting protection — inapplicable here since the correct fix needed no new package at all). Verified empirically: `create_engine("postgresql+psycopg://...")` resolves `dialect.name == "postgresql"`, the same value Airflow's own `dialect.name == "postgresql"` branches check, and a full live `airflow db migrate` against a real testcontainers Postgres 17 succeeded end-to-end.
- **`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_ASYNC=""`.** Airflow 3.3.0's `configure_orm()` unconditionally derives an async engine URL by mapping the `postgresql` scheme to `asyncpg` regardless of which sync driver was requested (`airflow/settings.py::_get_async_conn_uri_from_sync`) — without this override, `airflow db migrate` fails immediately with `ModuleNotFoundError: No module named 'asyncpg'` (reproduced live during this plan's own validation). `asyncpg` is explicitly rejected by CLAUDE.md's stack ("Async-only, weaker Decimal/COPY ergonomics... Use `psycopg[binary,pool]` v3"), so disabling the derivation — not installing the rejected package — is the correct fix. `_configure_async_session()` treats a falsy `SQL_ALCHEMY_CONN_ASYNC` as "no async engine"; verified no `asyncpg` import ever occurs and `dag.test()` still succeeds fully.
- **Added `mock_s3_infrastructure`, beyond the plan's explicitly-named `mock_kpo_execute`.** Rule 2 (missing critical functionality): `dag.test()` swallows per-task exceptions internally and would technically "complete without raising" even if every S3-touching task failed — but the plan's own must_haves truth requires "the sensor/gate/discover/ingest chain all 'ran,' per the mock", which is unreachable without doubling every real S3/`boto3` call this tier's own threat model (T-08-22) already commits to never making for real (no MinIO container stood up in this tier).
- **`AIRFLOW__CORE__DAGS_FOLDER` set explicitly to `airflow/dags`.** `dag.test()` internally re-syncs/re-serializes the owning DAG bundle into the metadata DB via `core.dags_folder` before creating a DagRun — a real failure ("Cannot create DagRun ... because the dag is not serialized") was reproduced and fixed during this plan's own live validation before writing the final fixture code.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `postgresql+psycopg2://` (as the plan's action text literally specifies) does not work in this project — psycopg2 is not installed**
- **Found during:** Task 1, live validation before writing final `conftest.py`
- **Issue:** The plan's own action text says `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` needs "the `postgresql://` -> `postgresql+psycopg2://` SQLAlchemy dialect prefix" — but this project deliberately never installs psycopg2 (CLAUDE.md pins psycopg3 exclusively). Following the plan literally would either fail at `airflow db migrate` time or require adding a new, CLAUDE.md-contradicting dependency.
- **Fix:** Used `postgresql+psycopg://` (v3, already installed via `dataplat`'s own dependency) instead — verified to resolve the identical `dialect.name` Airflow's own code branches on.
- **Files modified:** `tests/dagtest/conftest.py`
- **Verification:** Live `airflow db migrate` against a real testcontainers Postgres 17 succeeded; `dag.test()` for both DAGs reached `DagRunState.SUCCESS`.
- **Committed in:** `5131513` (Task 1 commit)

**2. [Rule 3 - Blocking] `configure_orm()`'s unconditional asyncpg derivation blocked `airflow db migrate` entirely**
- **Found during:** Task 1, live validation
- **Issue:** `airflow db migrate` failed with `ModuleNotFoundError: No module named 'asyncpg'` on the very first attempt — Airflow 3.3.0 always tries to build an async SQLAlchemy engine for the metadata DB, deriving its URL by swapping in `asyncpg` regardless of the sync driver.
- **Fix:** Set `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_ASYNC=""`, which `_configure_async_session()` treats as "skip async engine entirely" — no new dependency, no code change to Airflow's own behavior needed.
- **Files modified:** `tests/dagtest/conftest.py`
- **Verification:** `airflow db migrate` succeeds; no `asyncpg` import occurs anywhere in the process.
- **Committed in:** `5131513` (Task 1 commit)

**3. [Rule 2 - Missing Critical] Mocking only `KubernetesPodOperator.execute` (as the plan's artifacts list names) leaves `wait_for_files`/`list_matched_keys`/`integrity_gate` making real, doomed S3 calls**
- **Found during:** Task 1/2, live validation
- **Issue:** This tier's own threat model deliberately stands up no MinIO container. Without doubling `S3Hook`/`S3KeySensor`, `dag.test()` would still return without raising (it catches per-task exceptions internally — verified directly against the installed `DAG.test` source) but `discover`/`ingest` would never actually run, silently failing the must_haves truth that the whole chain runs "per the mock."
- **Fix:** Added `mock_s3_infrastructure`, patching `S3Hook.list_keys`/`get_conn` (return-value mocks) and `S3KeySensor.execute` (a plain function, matching `mock_kpo_execute`'s own self-binding reasoning).
- **Files modified:** `tests/dagtest/conftest.py`
- **Verification:** Both DAGs' full 8-task chain reaches `success`; `mock_kpo_execute`'s recorded calls confirm `discover` and `ingest` both genuinely ran.
- **Committed in:** `5131513` (Task 1 commit)

---

**Total deviations:** 3 auto-fixed (1 Rule 1 bug fix, 1 Rule 3 blocking fix, 1 Rule 2 missing-critical addition)
**Impact on plan:** All three were required for the plan's own stated must_haves to actually hold true; none change the plan's declared file scope (`tests/dagtest/__init__.py`, `conftest.py`, `test_backfill_dagrun.py`, `pyproject.toml` — exactly as declared in frontmatter). No scope creep.

## Issues Encountered

None beyond the three deviations above, all resolved during live validation before the final fixture/test code was written (not discovered after the fact).

## User Setup Required

None - no external service configuration required. This tier needs only a local Docker daemon (already required by `tests/integration/`).

## Next Phase Readiness

- `tests/dagtest/` is fully independent of `tests/integration/`'s own testcontainers fixtures (separate PostgreSQL container, separate DSN shape, separate CI marker) — safe to run as its own CI stage/job per T-08-22's mitigation, never sharing a pytest invocation with `tests/integration/`.
- Verified live: `pytest tests/dagtest/ -x -m dagtest` — 2 passed in ~35s; `pytest tests/dagtest/ --collect-only -q` — 2 collected, zero Docker needed.
- Plan 08-14 (the next wave's genuine live-cluster proof) can build on this tier's own established pattern (deferred airflow imports, `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_ASYNC=""`) if it ever needs an in-process `dag.test()` proof of its own — though 08-14's own scope is a full live-cluster proof, a different tier entirely (Pitfall 3's third rung).
- No blockers.

---
*Phase: 08-validation-quarantine-metadata-control-plane-completion*
*Completed: 2026-08-17*

## Self-Check: PASSED

- FOUND: tests/dagtest/__init__.py
- FOUND: tests/dagtest/conftest.py
- FOUND: tests/dagtest/test_backfill_dagrun.py
- FOUND: commit 5131513 (Task 1)
- FOUND: commit 563e9e4 (Task 2)
