"""Proof-over-prose for run_stage_recorder.py's DBT_BUILD writer (09-04-PLAN.md Task 1).

Both `@task`-decorated functions are exercised against a REAL (testcontainers)
analytical PostgreSQL, migrated to `head` (which includes migration 0025's
`meta.run_stages`) -- not mocked, since this module's whole job is a set of
SQL statements whose exact semantics (the `LEFT JOIN`/`ON CONFLICT` shapes)
are the thing under test, not merely "was psycopg.connect called".

This module lives under `tests/dagtest/` per 09-04-PLAN.md's own file list,
but the concern under test is the ANALYTICAL database's `meta.run_stages`
table, not Airflow's metadata database `tests/dagtest/conftest.py`'s own
`airflow_metadata_dsn`/`airflow_env` fixtures stand up -- so this module
defines its OWN, independent testcontainers-PostgreSQL-plus-migrations
fixture below, duplicating `tests/integration/conftest.py`'s
`postgres_dsn`/`run_migrations`/`migrated_dsn` pattern locally rather than
importing across test-tier `conftest.py` files (this codebase's own
established convention -- see `tests/dagtest/conftest.py`'s module
docstring, "reuse the SAME loading mechanism, do not invent a second one"
applied to test infrastructure, not DAG-loading). `tests/dagtest/conftest.py`'s
own directory-wide, autouse `_require_docker` fixture still applies here
(inherited from the parent conftest.py), so this module does not duplicate
that one.

Calling convention: `@task`-decorated functions are invoked via `.function(...)`
(the documented way to call the raw, undecorated Python callable outside a
DAG context), matching `tests/unit/test_integrity_gate.py`'s own convention.
"""

from __future__ import annotations

import itertools
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from testcontainers.community.postgres import PostgresContainer

if TYPE_CHECKING:
    from collections.abc import Iterator

REPO_ROOT = Path(__file__).resolve().parents[2]
ALEMBIC_INI = REPO_ROOT / "migrations" / "alembic.ini"
DAGS_FOLDER = REPO_ROOT / "airflow" / "dags"

# Module-level, matching tests/unit/conftest.py's own established bootstrap
# (its module docstring: "pytest imports every ancestor conftest.py before
# collecting a directory's test modules... a fixture body only runs at test
# EXECUTION time, well after collection-time imports have already been
# attempted"). tests/dagtest/conftest.py deliberately defers its OWN
# sys.path insertion into the `airflow_env` fixture for `dag.test()`/DagBag
# import-order reasons this module never triggers (no DagBag, no
# `dag.test()` anywhere below) -- this module's own top-level `_common`
# import needs the DAGS_FOLDER bootstrap unconditionally at collection time,
# so it is duplicated here rather than relying on tests/dagtest/conftest.py's
# deferred one.
if str(DAGS_FOLDER) not in sys.path:
    sys.path.insert(0, str(DAGS_FOLDER))

from _common import run_stage_recorder as recorder  # noqa: E402 -- see sys.path bootstrap above

# Needs a local Docker daemon (testcontainers PostgreSQL) -- excluded from
# the offline gate, matching tests/integration/'s own marker for the
# identical reason (a real, migrated analytical PostgreSQL, not mocked).
pytestmark = pytest.mark.integration

_dataset_name_counter = itertools.count()


@pytest.fixture(scope="session")
def migrated_analytics_dsn() -> Iterator[str]:
    """A throwaway PostgreSQL 18 container, migrated to `head` (includes 0025's `meta.run_stages`).

    PG 18 -- the analytical database's pinned major (CLAUDE.md), matching
    `tests/integration/conftest.py`'s own `postgres_dsn` fixture exactly
    (container image, role bootstrap, migration mechanism all duplicated
    from there per this module's own docstring).
    """
    with PostgresContainer("postgres:18-bookworm", driver="psycopg", dbname="analytics") as pg:
        dsn = pg.get_connection_url().replace("postgresql+psycopg://", "postgresql://")
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute("CREATE ROLE etl_app LOGIN")
            cur.execute("CREATE ROLE analytics_owner LOGIN")

        previous = os.environ.get("ALEMBIC_DSN")
        os.environ["ALEMBIC_DSN"] = dsn
        try:
            command.upgrade(Config(str(ALEMBIC_INI)), "head")
        finally:
            if previous is None:
                os.environ.pop("ALEMBIC_DSN", None)
            else:
                os.environ["ALEMBIC_DSN"] = previous

        yield dsn


@pytest.fixture
def patched_connection(monkeypatch: pytest.MonkeyPatch, migrated_analytics_dsn: str) -> None:
    """Make `BaseHook.get_connection(_ANALYTICS_DB_CONN_ID)` resolve to the REAL migrated DSN.

    Unlike `tests/unit/test_integrity_gate.py`'s own `fake_cursor` fixture
    (which fakes `psycopg.connect` itself, never touching a database at
    all), this module's tests need genuine SQL execution against the real
    schema -- only the Airflow Connection lookup is faked, `psycopg.connect`
    itself stays real.
    """
    fake_connection = MagicMock()
    fake_connection.get_uri.return_value = migrated_analytics_dsn
    monkeypatch.setattr(recorder.BaseHook, "get_connection", lambda _conn_id: fake_connection)


@pytest.fixture
def dataset_name() -> str:
    """A fresh, unique dataset name per test -- the shared session-scoped DB is never reset."""
    return f"test_run_stage_recorder_{next(_dataset_name_counter)}"


def _insert_dataset(conn: psycopg.Connection[Any], name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO meta.datasets (dataset_name) VALUES (%s) RETURNING dataset_id",
            (name,),
        )
        row = cur.fetchone()
        assert row is not None
        return int(row[0])


def _insert_config_version(conn: psycopg.Connection[Any], *, dataset_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO meta.config_versions (
                dataset_id, version, config_hash, config_document,
                config_schema_version, valid_from
            ) VALUES (%s, 1, 'test-hash', '{}'::jsonb, 1, now())
            RETURNING config_version_id
            """,
            (dataset_id,),
        )
        row = cur.fetchone()
        assert row is not None
        return int(row[0])


def _insert_ingestion_run(
    conn: psycopg.Connection[Any],
    *,
    dataset_id: int,
    config_version_id: int,
    idempotency_key: str,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO meta.ingestion_runs (
                idempotency_key, dataset_id, config_version_id,
                processor_version, processor_image_digest, status
            ) VALUES (%s, %s, %s, 'test', 'sha256:test', 'STAGED')
            RETURNING run_id
            """,
            (idempotency_key, dataset_id, config_version_id),
        )
        row = cur.fetchone()
        assert row is not None
        return int(row[0])


def _insert_run_stage(
    conn: psycopg.Connection[Any], *, run_id: int, stage_name: str, status: str
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO meta.run_stages (run_id, stage_name, status) VALUES (%s, %s, %s)",
            (run_id, stage_name, status),
        )


def _make_run(
    conn: psycopg.Connection[Any],
    *,
    dataset_id: int,
    config_version_id: int,
    idempotency_key: str,
    stage_load_status: str,
) -> int:
    """A run with a STAGE_LOAD row set to `stage_load_status` -- no DBT_BUILD row."""
    run_id = _insert_ingestion_run(
        conn,
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        idempotency_key=idempotency_key,
    )
    _insert_run_stage(conn, run_id=run_id, stage_name="STAGE_LOAD", status=stage_load_status)
    return run_id


def _get_run_stage(
    conn: psycopg.Connection[Any], *, run_id: int, stage_name: str
) -> tuple[str, Any, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, started_at, finished_at FROM meta.run_stages"
            " WHERE run_id = %s AND stage_name = %s",
            (run_id, stage_name),
        )
        row = cur.fetchone()
        return None if row is None else (str(row[0]), row[1], row[2])


# --- Test 1: STAGE_LOAD SUCCEEDED, no DBT_BUILD row -> eligible --------------


def test_pending_dbt_build_includes_stage_load_succeeded_with_no_dbt_build_row(
    migrated_analytics_dsn: str,
    patched_connection: None,  # noqa: ARG001 -- fixture used for its patching side effect only
    dataset_name: str,
) -> None:
    with psycopg.connect(migrated_analytics_dsn, autocommit=True) as conn:
        dataset_id = _insert_dataset(conn, dataset_name)
        config_version_id = _insert_config_version(conn, dataset_id=dataset_id)
        run_id = _make_run(
            conn,
            dataset_id=dataset_id,
            config_version_id=config_version_id,
            idempotency_key=f"{dataset_name}-run-1",
            stage_load_status="SUCCEEDED",
        )

    result = recorder.list_run_ids_pending_dbt_build.function(dataset_name=dataset_name)

    assert result == [run_id]


# --- Test 2: FAILED DBT_BUILD is a retry candidate; SUCCEEDED is excluded ----


def test_pending_dbt_build_includes_failed_retry_but_excludes_succeeded(
    migrated_analytics_dsn: str,
    patched_connection: None,  # noqa: ARG001 -- fixture used for its patching side effect only
    dataset_name: str,
) -> None:
    with psycopg.connect(migrated_analytics_dsn, autocommit=True) as conn:
        dataset_id = _insert_dataset(conn, dataset_name)
        config_version_id = _insert_config_version(conn, dataset_id=dataset_id)

        retry_run_id = _make_run(
            conn,
            dataset_id=dataset_id,
            config_version_id=config_version_id,
            idempotency_key=f"{dataset_name}-retry",
            stage_load_status="SUCCEEDED",
        )
        _insert_run_stage(conn, run_id=retry_run_id, stage_name="DBT_BUILD", status="FAILED")

        done_run_id = _make_run(
            conn,
            dataset_id=dataset_id,
            config_version_id=config_version_id,
            idempotency_key=f"{dataset_name}-done",
            stage_load_status="SUCCEEDED",
        )
        _insert_run_stage(conn, run_id=done_run_id, stage_name="DBT_BUILD", status="SUCCEEDED")

    result = recorder.list_run_ids_pending_dbt_build.function(dataset_name=dataset_name)

    assert retry_run_id in result
    assert done_run_id not in result


# --- Test 3: record RUNNING -- insert or upsert, started_at set --------------


def test_record_dbt_build_stage_running_sets_started_at(
    migrated_analytics_dsn: str,
    patched_connection: None,  # noqa: ARG001 -- fixture used for its patching side effect only
    dataset_name: str,
) -> None:
    with psycopg.connect(migrated_analytics_dsn, autocommit=True) as conn:
        dataset_id = _insert_dataset(conn, dataset_name)
        config_version_id = _insert_config_version(conn, dataset_id=dataset_id)
        run_id_1 = _make_run(
            conn,
            dataset_id=dataset_id,
            config_version_id=config_version_id,
            idempotency_key=f"{dataset_name}-run-1",
            stage_load_status="SUCCEEDED",
        )
        run_id_2 = _make_run(
            conn,
            dataset_id=dataset_id,
            config_version_id=config_version_id,
            idempotency_key=f"{dataset_name}-run-2",
            stage_load_status="SUCCEEDED",
        )

    recorder.record_dbt_build_stage.function(run_ids=[run_id_1, run_id_2], status="RUNNING")

    with psycopg.connect(migrated_analytics_dsn, autocommit=True) as conn:
        for run_id in (run_id_1, run_id_2):
            row = _get_run_stage(conn, run_id=run_id, stage_name="DBT_BUILD")
            assert row is not None
            status, started_at, finished_at = row
            assert status == "RUNNING"
            assert started_at is not None
            assert finished_at is None


# --- Test 4: record a terminal status -- transitions without a prior RUNNING row --


def test_record_dbt_build_stage_succeeded_sets_finished_at_without_prior_running(
    migrated_analytics_dsn: str,
    patched_connection: None,  # noqa: ARG001 -- fixture used for its patching side effect only
    dataset_name: str,
) -> None:
    with psycopg.connect(migrated_analytics_dsn, autocommit=True) as conn:
        dataset_id = _insert_dataset(conn, dataset_name)
        config_version_id = _insert_config_version(conn, dataset_id=dataset_id)
        run_id_1 = _make_run(
            conn,
            dataset_id=dataset_id,
            config_version_id=config_version_id,
            idempotency_key=f"{dataset_name}-run-1",
            stage_load_status="SUCCEEDED",
        )
        run_id_2 = _make_run(
            conn,
            dataset_id=dataset_id,
            config_version_id=config_version_id,
            idempotency_key=f"{dataset_name}-run-2",
            stage_load_status="SUCCEEDED",
        )

    # Deliberately no prior RUNNING write -- a defensive upsert, not a
    # strict state-machine claim (D-14, dbt_build has no lease to steal).
    recorder.record_dbt_build_stage.function(run_ids=[run_id_1, run_id_2], status="SUCCEEDED")

    with psycopg.connect(migrated_analytics_dsn, autocommit=True) as conn:
        for run_id in (run_id_1, run_id_2):
            row = _get_run_stage(conn, run_id=run_id, stage_name="DBT_BUILD")
            assert row is not None
            status, _started_at, finished_at = row
            assert status == "SUCCEEDED"
            assert finished_at is not None


# --- Test 5: empty run_ids is a safe no-op -----------------------------------


def test_record_dbt_build_stage_empty_run_ids_is_a_safe_noop(
    migrated_analytics_dsn: str,
    patched_connection: None,  # noqa: ARG001 -- fixture used for its patching side effect only
) -> None:
    with psycopg.connect(migrated_analytics_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM meta.run_stages WHERE stage_name = 'DBT_BUILD'")
        row = cur.fetchone()
        assert row is not None
        before_count = int(row[0])

    recorder.record_dbt_build_stage.function(run_ids=[], status="RUNNING")

    with psycopg.connect(migrated_analytics_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM meta.run_stages WHERE stage_name = 'DBT_BUILD'")
        row = cur.fetchone()
        assert row is not None
        after_count = int(row[0])

    assert after_count == before_count
