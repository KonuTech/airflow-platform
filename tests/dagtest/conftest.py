"""Shared fixtures for tests/dagtest/ -- `dag.test()` against a real Airflow metadata DB (VALID-08).

This is a genuinely NEW test tier for this codebase (08-13-PLAN.md), the
middle rung of RESEARCH.md Pitfall 3's explicit three-tier split: this tier
proves DAG-level backfill *mechanics* (correct `logical_date`, correct task
graph, correct `run_id`/`try_number` wiring) via Airflow's own `dag.test()`
API against a real (testcontainers) metadata database -- never the real
resolution-state-transition logic inside a launched pod (plan 08-03's job)
and never a full live-cluster proof (plan 08-14's job).

Deliberately independent of every other test tier's fixtures:

* `tests/integration/conftest.py`'s `postgres_dsn`/`migrated_dsn` stand up
  the *analytical* PostgreSQL (CLAUDE.md Sec. 4's other physically-separate
  database) -- this module's own `airflow_metadata_dsn` is a SEPARATE
  container for the Airflow *metadata* database. Sharing one container
  between the two would blur exactly the boundary Sec. 4 exists to keep
  visible, in test infrastructure as much as in production (T-08-22).
* `tests/unit/conftest.py`'s `dagbag` fixture parses `airflow/dags/` with NO
  metadata database at all (pure structural/import tests) -- this tier's own
  `load_dag` fixture below replicates that same `sys.path`/env-var bootstrap
  (rather than importing across test-tier conftest.py files, matching this
  codebase's existing "reuse the SAME loading mechanism, do not invent a
  second one" precedent, 04-07-PLAN.md Task 2) because it additionally needs
  a live metadata database wired up FIRST, which `tests/unit/`'s tier
  intentionally never needs.

Import-order discipline (load-bearing, not stylistic): `import airflow`
triggers `airflow.settings.initialize()` -> `configure_orm()` at THAT EXACT
MOMENT, against whatever `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` happens to be
set to right then (empirically verified against the pinned
`apache-airflow==3.3.0`, this plan's own first real use of `dag.test()`).
Every airflow-touching import in this module is therefore INSIDE a fixture
body, never at module top level, so pytest's own collection-time import of
this conftest.py (and of `test_backfill_dagrun.py`) can never race the
`airflow_env` fixture that sets the real DSN first. This is also why
`pytest tests/dagtest/ --collect-only -q` succeeds with zero Docker: nothing
importable at collection time touches Docker or Airflow settings.

Async-engine note: Airflow 3.3.0's `configure_orm()` unconditionally derives
an async engine URL from the sync DSN (`postgresql` -> `+asyncpg`,
`airflow/settings.py::_get_async_conn_uri_from_sync`), regardless of which
sync driver was requested -- so it would require `asyncpg` even though this
project's own driver of record is `psycopg[binary,pool]` v3 (CLAUDE.md
explicitly rejects `asyncpg`). `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_ASYNC=""`
below disables that derivation entirely (`_configure_async_session` treats a
falsy value as "no async engine"), so no new dependency is needed and no
`asyncpg` import ever happens -- verified empirically, not merely inferred.
"""

from __future__ import annotations

import io
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest
from testcontainers.community.postgres import PostgresContainer

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

REPO_ROOT = Path(__file__).resolve().parents[2]
DAGS_FOLDER = REPO_ROOT / "airflow" / "dags"

# The same fixture values tests/unit/conftest.py's own `dagbag` fixture uses
# for `Variable.get(image_variable)` at DAG-parse time (common_kpo_kwargs())
# -- this tier never launches a real pod, so neither value is ever resolved
# against a real registry. 08.1-12 adds the second: `dbt_build`'s own
# `image_variable="dbt_image"` override.
_TEST_FIXTURE_IMAGE = "localhost:5001/csv-processor:test-fixture"
_TEST_FIXTURE_DBT_IMAGE = "localhost:5001/dbt:test-fixture"

# The env vars `airflow_env` owns for the duration of the test session --
# restored to their pre-fixture values on teardown so a later, unrelated
# pytest invocation in the same interpreter (unlikely but not impossible
# under `-p no:cacheprovider` reuse) never inherits this tier's DSN.
_AIRFLOW_ENV_KEYS = (
    "AIRFLOW_HOME",
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_ASYNC",
    "AIRFLOW__CORE__EXECUTOR",
    "AIRFLOW__CORE__LOAD_EXAMPLES",
    "AIRFLOW__CORE__DAGS_FOLDER",
    "AIRFLOW_VAR_CSV_PROCESSOR_IMAGE",
    "AIRFLOW_VAR_DBT_IMAGE",
)


@pytest.fixture(scope="session", autouse=True)
def _require_docker() -> None:
    """Skip the whole suite, with a named reason, when no Docker daemon answers.

    Structurally identical to `tests/integration/conftest.py`'s own fixture
    of the same name (same skip-with-reason shape, same 30s `docker info`
    ceiling -- measured in that tier to avoid false-negative skips on a
    WSL2/Docker Desktop backend) -- this tier's own reason text names
    `tests/dagtest/` specifically so a developer sees which tier is skipping.
    """
    docker_bin = shutil.which("docker")
    if docker_bin is None:
        pytest.skip("docker not found on PATH — tests/dagtest/ needs a local Docker daemon")
    proc = subprocess.run(  # noqa: S603
        [docker_bin, "info"],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    if proc.returncode != 0:
        pytest.skip(
            f"docker daemon not reachable (exit {proc.returncode}) — "
            f"tests/dagtest/ needs a local Docker daemon:\n{proc.stderr}",
        )


@pytest.fixture(scope="session")
def airflow_metadata_dsn() -> Iterator[str]:
    """A throwaway PostgreSQL 17 container for the Airflow *metadata* DB, unmigrated.

    PostgreSQL 17 -- Airflow 3.3.0's own supported-major ceiling (CLAUDE.md:
    "PG 18 is NOT supported for the metadata DB"), and deliberately the OTHER
    major from `tests/integration/conftest.py`'s PG 18 analytical fixture:
    this tier's whole reason to exist is proving DAG mechanics against the
    metadata database specifically, so it must never accidentally share a
    container -- or a major version -- with the analytical one.

    A SEPARATE container from `tests/integration/conftest.py`'s `postgres_dsn`
    (never shared across the two conftest.py files, T-08-22): three
    concurrent testcontainers PostgreSQL instances (this tier's own plus
    `tests/integration/`'s two) would exceed the 4 CPU/16GB CI runner budget
    if ever invoked in the same pytest process/CI step -- this tier's own
    CI job/stage stays separate for exactly that reason.

    Yields:
        `PostgresContainer.get_connection_url()`'s own `postgresql+psycopg://`
        DSN, already the SQLAlchemy-ready shape `AIRFLOW__DATABASE__
        SQL_ALCHEMY_CONN` needs -- unlike `tests/integration/`'s analytical
        fixture, which strips the dialect suffix for a plain `psycopg`
        connection, this tier hands the DSN to Airflow's OWN SQLAlchemy
        engine, so the `+psycopg` suffix must stay.
    """
    with PostgresContainer("postgres:17-bookworm", driver="psycopg", dbname="airflow") as pg:
        yield pg.get_connection_url()


@pytest.fixture(scope="session")
def airflow_env(airflow_metadata_dsn: str) -> Iterator[None]:
    """Wire `airflow_metadata_dsn` into the process, migrate it once, and hold it for the session.

    Session-scoped, not function-scoped: `airflow db migrate`'s own cost
    (alembic stamping the full metadata schema) is real and not worth paying
    per-test, and every `dag.test()` call in this tier is read-mostly against
    the same schema -- no test here mutates schema, only DagRun/TaskInstance
    rows, which `dag.test()` itself creates fresh (a new `run_id`) on every
    call regardless of scope. If a future test in this tier needs true
    per-test DB isolation, that is the tradeoff to revisit then, not now.

    Sets `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` (this fixture's own DSN,
    already `+psycopg`-suffixed) and `AIRFLOW__CORE__EXECUTOR=LocalExecutor`
    (`dag.test()`'s own non-`use_executor` code path runs each task locally
    regardless, but a real `LocalExecutor` value keeps this tier honest about
    which executor's task-run semantics it is exercising, matching STACK.md's
    "CI default"). `AIRFLOW__CORE__DAGS_FOLDER` points at the SAME
    `airflow/dags` directory `load_dag` parses -- `dag.test()` internally
    re-syncs/re-serializes whichever bundle owns the DAG into the metadata DB
    before creating a DagRun (verified directly against the installed
    `airflow.sdk.definitions.dag.DAG.test` source), and that re-sync reads
    `core.dags_folder`, not whatever path a caller's own `DagBag(...)` used.
    """
    airflow_home = tempfile.mkdtemp(prefix="dagtest-airflow-home-")
    previous_env = {key: os.environ.get(key) for key in _AIRFLOW_ENV_KEYS}
    os.environ["AIRFLOW_HOME"] = airflow_home
    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = airflow_metadata_dsn
    # See module docstring "Async-engine note" -- disables configure_orm()'s
    # asyncpg-only async-engine derivation entirely; no asyncpg import ever
    # happens.
    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_ASYNC"] = ""
    os.environ["AIRFLOW__CORE__EXECUTOR"] = "LocalExecutor"
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = str(DAGS_FOLDER)
    # common_kpo_kwargs()'s Variable.get(image_variable) at DAG-parse time --
    # same mechanism, same values, as tests/unit/conftest.py's own `dagbag`
    # fixture. Both `discover`/`stage`/`publish`'s "csv_processor_image" AND
    # `dbt_build`'s "dbt_image" (08.1-12) must resolve for DagBag to parse.
    os.environ["AIRFLOW_VAR_CSV_PROCESSOR_IMAGE"] = _TEST_FIXTURE_IMAGE
    os.environ["AIRFLOW_VAR_DBT_IMAGE"] = _TEST_FIXTURE_DBT_IMAGE
    if str(DAGS_FOLDER) not in sys.path:
        sys.path.insert(0, str(DAGS_FOLDER))

    # A subprocess, deliberately: this is a SEPARATE Python process, so its
    # own first `import airflow` reads these just-set env vars fresh --
    # never racing, and never entangled with, whichever import ordering THIS
    # process's own fixtures/tests later rely on.
    migrate = subprocess.run(
        [sys.executable, "-m", "airflow", "db", "migrate"],
        cwd=str(REPO_ROOT),
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    if migrate.returncode != 0:
        pytest.fail(
            "airflow db migrate failed against the tests/dagtest/ metadata "
            f"container (exit {migrate.returncode}):\nSTDOUT:\n{migrate.stdout}\n"
            f"STDERR:\n{migrate.stderr}",
        )

    try:
        yield
    finally:
        for key, value in previous_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        shutil.rmtree(airflow_home, ignore_errors=True)


@pytest.fixture
def load_dag(airflow_env: None) -> Callable[[str], Any]:  # noqa: ARG001 -- ordering dependency: airflow_env must wire the metadata DB before any airflow import
    """Return a `load_dag(dag_id)` callable: `DagBag(dag_folder="airflow/dags").dags[dag_id]`.

    The same loading mechanism `tests/unit/conftest.py`'s own `dagbag`
    fixture uses (`DagBag(dag_folder=...)`, no `include_examples` kwarg --
    verified against the pinned `apache-airflow==3.3.0`, passing it raises
    `TypeError`), replicated here rather than imported across test-tier
    `conftest.py` files (module docstring) because this tier's own call must
    happen AFTER `airflow_env` has wired up the metadata DB, never before.
    """

    def _load(dag_id: str) -> Any:
        from airflow.models import DagBag  # noqa: PLC0415 -- deferred import, see module docstring

        bag = DagBag(dag_folder=str(DAGS_FOLDER))
        assert bag.import_errors == {}, bag.import_errors
        return bag.dags[dag_id]

    return _load


@pytest.fixture
def mock_kpo_execute(airflow_env: None) -> Iterator[list[dict[str, Any]]]:  # noqa: ARG001 -- ordering dependency, see load_dag above
    """Patch `KubernetesPodOperator.execute` to return a canned success value -- no real pod, ever.

    CLAUDE.md's own documented pattern: "Mock the KPO in unit tests: patch
    `KubernetesPodOperator.execute` and assert on the constructed pod spec."
    Patching the PARENT class (`KubernetesPodOperator`, not
    `TracingKubernetesPodOperator`) covers `discover`/`dbt_build`/`publish`
    (plain KPOs) and `stage` (`TracingKubernetesPodOperator`, which only
    overrides `build_pod_request_obj()`, never `execute()` -- verified
    directly against `airflow/dags/_common/tracing_kpo.py`) with the ONE
    patch, matching this module's own "one shared mechanism, not two"
    discipline (08.1-12: `stage`/`dbt_build`/`publish` replace the old
    single `ingest` task).

    A plain function, not a `unittest.mock.MagicMock`: only a real function
    object is a descriptor, so `patch.object(KubernetesPodOperator, "execute",
    new=_fake_execute)` makes `some_kpo_instance.execute(context)` correctly
    auto-bind `self` -- required here because the canned return value differs
    by `self.task_id` (`discover` needs an XCom shape `build_stage_args` can
    consume; every other task gets a receipt shape `aggregate_receipts` can
    sum, harmless for `dbt_build`/`publish` since nothing downstream reads
    their XCom). A bare `MagicMock` is not a descriptor and would silently
    drop `self`, losing that distinction (verified empirically while
    building this fixture).

    Yields:
        A list of `{"task_id": ...}` records, one per real invocation --
        lets a test assert the mock was genuinely exercised for
        `discover`/`stage`/`dbt_build`/`publish`, not skipped/short-
        circuited by an upstream failure.
    """
    from airflow.providers.cncf.kubernetes.operators.pod import (  # noqa: PLC0415 -- deferred, see load_dag above
        KubernetesPodOperator,
    )

    calls: list[dict[str, Any]] = []

    def _fake_execute(self: KubernetesPodOperator, context: dict[str, Any]) -> dict[str, Any]:
        del context  # unused -- this fixture only needs `self.task_id`
        calls.append({"task_id": self.task_id})
        if self.task_id == "discover":
            return {
                "units": [
                    {"assignment_uri": "s3://raw/customers/dagtest-fixture.csv#assignment=1"},
                ],
            }
        return {"rows_loaded": 1, "batch_id": "dagtest-fixture-batch"}

    with patch.object(KubernetesPodOperator, "execute", new=_fake_execute):
        yield calls


@pytest.fixture
def mock_run_stage_recorder_db(airflow_env: None) -> Iterator[None]:  # noqa: ARG001 -- ordering dependency, see load_dag above
    """Double `run_stage_recorder.py`'s own DB-touching tasks (plan 09-09's DAG wiring).

    This tier stands up only the Airflow *metadata* PostgreSQL (module
    docstring's T-08-22 precedent, mirroring `mock_s3_infrastructure`'s own
    "no second container" discipline) -- never a second analytical PostgreSQL
    container `list_run_ids_pending_dbt_build`/`record_dbt_build_stage`
    (`airflow/dags/_common/run_stage_recorder.py`) would otherwise need a real
    `analytics_db_default` Airflow Connection and a live `meta.run_stages`
    table for.

    Deliberately does NOT patch `psycopg.connect` (an earlier version of this
    fixture did, and it broke both tests non-deterministically): `psycopg` is
    a single process-wide module object, and Airflow's OWN `postgresql+psycopg`
    SQLAlchemy dialect (`airflow_env`'s `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`)
    calls the SAME `psycopg.connect` under the hood to open ITS OWN metadata-DB
    connections -- patching it here would silently intercept the metadata DB's
    real connections too, not just `run_stage_recorder.py`'s, producing the
    exact "IndexError: tuple index out of range" from a mocked
    `pg_catalog.version()` this fixture was rewritten to stop causing.

    Instead, `list_run_ids_pending_dbt_build`/`record_dbt_build_stage`
    themselves are replaced with `@task`-decorated no-DB-touching fakes on
    the `_common.run_stage_recorder` module object, BEFORE `load_dag()` (a
    later fixture/call) re-imports `airflow/dags/csv_ingest_customers.py`/
    `csv_ingest_orders.py` -- both DAG files bind these names via `from
    _common.run_stage_recorder import ...` at DAG-parse time, so they pick up
    these fakes instead of the real DB-touching originals. The fakes are
    themselves real `@task` objects (not plain functions) so `.override(...)`
    -- used by both DAG files for `mark_dbt_build_running`/`mark_dbt_build_done`
    -- still works identically. The fake list always returns `[]` (no run
    currently eligible for `dbt_build`), so `record_dbt_build_stage`'s own
    no-op empty-`run_ids` short-circuit is exercised for real by the fake too
    -- `dbt_build` itself still runs (mocked via `mock_kpo_execute`), and
    `resolve_dbt_build_status` reports its real terminal state via
    `ti.get_task_states`, a genuine Task-SDK API this fixture does not touch.
    """
    from airflow.sdk import task  # noqa: PLC0415 -- deferred import, see module docstring
    from _common import run_stage_recorder  # noqa: PLC0415 -- deferred import, see module docstring

    @task
    def list_run_ids_pending_dbt_build(dataset_name: str) -> list[int]:
        del dataset_name
        return []

    @task
    def record_dbt_build_stage(run_ids: list[int], status: str) -> None:
        del run_ids, status

    with (
        patch.object(
            run_stage_recorder, "list_run_ids_pending_dbt_build", list_run_ids_pending_dbt_build
        ),
        patch.object(run_stage_recorder, "record_dbt_build_stage", record_dbt_build_stage),
    ):
        yield


class _FakeS3Client:
    """A minimal stand-in for `boto3`'s S3 client, covering `integrity_gate`'s exact call shape.

    `head_object` is called twice per key (the D-21 stability check) and must
    return a NON-empty, STABLE `(ContentLength, ETag)` pair both times so
    `integrity_gate` takes its success path, not either rejection path --
    this tier proves the discover/ingest chain genuinely runs, not the gate's
    own rejection bookkeeping (`_reject_file`'s `psycopg` INSERT would need
    yet another live connection this tier deliberately does not stand up).
    """

    def head_object(self, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803 -- boto3's own param casing
        del Bucket, Key
        return {"ContentLength": 4, "ETag": '"dagtest-fixture-etag"'}

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
        del Bucket, Key
        return {"Body": io.BytesIO(b"data")}


@pytest.fixture
def mock_s3_infrastructure(airflow_env: None) -> Iterator[None]:  # noqa: ARG001 -- ordering dependency, see load_dag above
    """Patch every S3/`boto3` touchpoint `wait_for_files`/`list_matched_keys`/`integrity_gate` use.

    This tier's own threat model (T-08-22) deliberately stands up NO MinIO
    container -- only `airflow_metadata_dsn`'s PostgreSQL -- so every real S3
    call `airflow/dags/_common/integrity_gate.py` and the DAGs' own
    `S3KeySensor` would otherwise make must be doubled here instead. Rule 2
    (auto-add missing critical functionality): without this fixture,
    `dag.test()` would still complete without raising (it swallows per-task
    exceptions, verified directly against the installed `DAG.test` source),
    but `wait_for_files`/`list_matched_keys`/`gate` would all fail against a
    nonexistent `minio_default` connection, `discover`/`stage`/`dbt_build`/
    `publish` would never run, and `mock_kpo_execute`'s own mock would sit
    unexercised -- silently defeating this plan's own must_haves truth ("the
    sensor/gate/discover/stage/dbt_build/publish chain all 'ran,' per the
    mock").

    `S3Hook.list_keys`/`S3Hook.get_conn` are patched with `return_value=`
    (a `MagicMock`, not a plain function): neither needs `self` -- both are
    called with zero or keyword-only arguments a `MagicMock` accepts
    regardless of binding, unlike `mock_kpo_execute`'s own `self.task_id`
    branch above. `S3KeySensor.execute` IS patched with a plain function
    (matching `mock_kpo_execute`'s own reasoning), even though this fixture
    never needs `self` from it either, for stylistic consistency with the
    one case in this module that does.
    """
    # Deferred imports (module docstring): must happen AFTER airflow_env.
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # noqa: PLC0415
    from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor  # noqa: PLC0415

    def _fake_sensor_execute(self: S3KeySensor, context: dict[str, Any]) -> None:
        del self, context  # the DAG never reads wait_for_files's own XCom

    with (
        patch.object(S3Hook, "list_keys", return_value=["customers/dagtest-fixture.csv"]),
        patch.object(S3Hook, "get_conn", return_value=_FakeS3Client()),
        patch.object(S3KeySensor, "execute", new=_fake_sensor_execute),
    ):
        yield
