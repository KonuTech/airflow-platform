"""Proof-over-prose for gap_recorder.py's D-06 "no file found" writer (09-10-PLAN.md Task 1).

`record_processing_gap_if_empty` is exercised against a REAL (testcontainers) analytical
PostgreSQL, migrated to `head` (which includes migration 0034's `meta.processing_gaps`) -- not
mocked, since the load-bearing behavior under test is the `ON CONFLICT (dataset_id, dag_run_id)
DO NOTHING` upsert semantics, not merely "was psycopg.connect called". Mirrors
`test_run_stage_recorder.py`'s exact fixture shape (own session-scoped testcontainers-PostgreSQL-
plus-migrations fixture, own `patched_connection` faking only the Airflow Connection lookup),
duplicated locally rather than imported across test modules per this codebase's established
convention.

Three scenarios prove the plan's own acceptance criteria: an empty `matched_keys` list with
`dag_run.backfill_id` set writes one row; an empty list with `backfill_id is None` (a live run)
writes nothing; a non-empty list writes nothing regardless of `backfill_id`.

Calling convention: `record_processing_gap_if_empty` is invoked via `.function(...)` (the
documented way to call the raw, undecorated Python callable outside a DAG context), matching
`tests/unit/test_integrity_gate.py`/`test_run_stage_recorder.py`'s own convention.
"""

from __future__ import annotations

import itertools
import os
import sys
from pathlib import Path
from types import SimpleNamespace
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

# Module-level, matching test_run_stage_recorder.py's own established bootstrap -- this module's
# own top-level `_common` import needs the DAGS_FOLDER bootstrap unconditionally at collection
# time, so it is duplicated here rather than relying on tests/dagtest/conftest.py's deferred one.
if str(DAGS_FOLDER) not in sys.path:
    sys.path.insert(0, str(DAGS_FOLDER))

from _common import gap_recorder  # noqa: E402 -- see sys.path bootstrap above

# Needs a local Docker daemon (testcontainers PostgreSQL) -- excluded from the offline gate,
# matching tests/integration/'s and test_run_stage_recorder.py's own marker for the identical
# reason (a real, migrated analytical PostgreSQL, not mocked).
pytestmark = pytest.mark.integration

_dataset_name_counter = itertools.count()


@pytest.fixture(scope="session")
def migrated_analytics_dsn() -> Iterator[str]:
    """A throwaway PostgreSQL 18 container, migrated to `head` (includes 0034's `processing_gaps`).

    Duplicates `test_run_stage_recorder.py`'s own fixture of the same name exactly (container
    image, role bootstrap, migration mechanism) -- a SEPARATE container, never shared across test
    modules (T-08-22's isolation precedent, applied here to test infrastructure).
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
    """Make `BaseHook.get_connection(_ANALYTICS_DB_CONN_ID)` resolve to the REAL migrated DSN."""
    fake_connection = MagicMock()
    fake_connection.get_uri.return_value = migrated_analytics_dsn
    monkeypatch.setattr(gap_recorder.BaseHook, "get_connection", lambda _conn_id: fake_connection)


@pytest.fixture
def dataset_name() -> str:
    """A fresh, unique dataset name per test -- the shared session-scoped DB is never reset."""
    return f"test_gap_recorder_{next(_dataset_name_counter)}"


def _insert_dataset(conn: psycopg.Connection[Any], name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO meta.datasets (dataset_name) VALUES (%s) RETURNING dataset_id",
            (name,),
        )
        row = cur.fetchone()
        assert row is not None
        return int(row[0])


def _count_gaps(conn: psycopg.Connection[Any], *, dataset_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM meta.processing_gaps WHERE dataset_id = %s", (dataset_id,)
        )
        row = cur.fetchone()
        assert row is not None
        return int(row[0])


def _make_dag_run(*, dag_id: str, run_id: str, backfill_id: int | None) -> SimpleNamespace:
    return SimpleNamespace(dag_id=dag_id, run_id=run_id, backfill_id=backfill_id)


# --- Test 1: empty matched_keys + backfill_id set -> writes exactly one row --


def test_empty_matched_keys_with_backfill_id_writes_one_gap_row(
    migrated_analytics_dsn: str,
    patched_connection: None,  # noqa: ARG001 -- fixture used for its patching side effect only
    dataset_name: str,
) -> None:
    with psycopg.connect(migrated_analytics_dsn, autocommit=True) as conn:
        dataset_id = _insert_dataset(conn, dataset_name)
        assert _count_gaps(conn, dataset_id=dataset_id) == 0

    dag_run = _make_dag_run(
        dag_id="csv_ingest_customers", run_id="backfill__2024-01-01", backfill_id=7
    )

    gap_recorder.record_processing_gap_if_empty.function(
        matched_keys=[], dataset_name=dataset_name, dag_run=dag_run
    )

    with psycopg.connect(migrated_analytics_dsn, autocommit=True) as conn:
        assert _count_gaps(conn, dataset_id=dataset_id) == 1


# --- Test 2: empty matched_keys + backfill_id None (live run) -> writes nothing --


def test_empty_matched_keys_without_backfill_id_writes_nothing(
    migrated_analytics_dsn: str,
    patched_connection: None,  # noqa: ARG001 -- fixture used for its patching side effect only
    dataset_name: str,
) -> None:
    with psycopg.connect(migrated_analytics_dsn, autocommit=True) as conn:
        dataset_id = _insert_dataset(conn, dataset_name)

    dag_run = _make_dag_run(
        dag_id="csv_ingest_customers", run_id="scheduled__2024-01-01", backfill_id=None
    )

    gap_recorder.record_processing_gap_if_empty.function(
        matched_keys=[], dataset_name=dataset_name, dag_run=dag_run
    )

    with psycopg.connect(migrated_analytics_dsn, autocommit=True) as conn:
        assert _count_gaps(conn, dataset_id=dataset_id) == 0


# --- Test 3: non-empty matched_keys -> writes nothing regardless of backfill_id --


def test_non_empty_matched_keys_writes_nothing_even_with_backfill_id(
    migrated_analytics_dsn: str,
    patched_connection: None,  # noqa: ARG001 -- fixture used for its patching side effect only
    dataset_name: str,
) -> None:
    with psycopg.connect(migrated_analytics_dsn, autocommit=True) as conn:
        dataset_id = _insert_dataset(conn, dataset_name)

    dag_run = _make_dag_run(
        dag_id="csv_ingest_customers", run_id="backfill__2024-01-01", backfill_id=7
    )

    gap_recorder.record_processing_gap_if_empty.function(
        matched_keys=["customers/2024-01-01.csv"], dataset_name=dataset_name, dag_run=dag_run
    )

    with psycopg.connect(migrated_analytics_dsn, autocommit=True) as conn:
        assert _count_gaps(conn, dataset_id=dataset_id) == 0


# --- Test 4: retried, still-empty backfill DagRun idempotently upserts, never duplicates -----


def test_retried_still_empty_backfill_run_does_not_duplicate_gap_row(
    migrated_analytics_dsn: str,
    patched_connection: None,  # noqa: ARG001 -- fixture used for its patching side effect only
    dataset_name: str,
) -> None:
    with psycopg.connect(migrated_analytics_dsn, autocommit=True) as conn:
        dataset_id = _insert_dataset(conn, dataset_name)

    dag_run = _make_dag_run(
        dag_id="csv_ingest_customers", run_id="backfill__2024-01-01", backfill_id=7
    )

    gap_recorder.record_processing_gap_if_empty.function(
        matched_keys=[], dataset_name=dataset_name, dag_run=dag_run
    )
    gap_recorder.record_processing_gap_if_empty.function(
        matched_keys=[], dataset_name=dataset_name, dag_run=dag_run
    )

    with psycopg.connect(migrated_analytics_dsn, autocommit=True) as conn:
        assert _count_gaps(conn, dataset_id=dataset_id) == 1


# --- Test 5: dag_run is None -> writes nothing, no connection even opened --------------------


def test_dag_run_none_writes_nothing(
    monkeypatch: pytest.MonkeyPatch,
    dataset_name: str,
) -> None:
    def _fail_if_called(_conn_id: str) -> None:
        msg = "BaseHook.get_connection must not be called when dag_run is None"
        raise AssertionError(msg)

    monkeypatch.setattr(gap_recorder.BaseHook, "get_connection", _fail_if_called)

    gap_recorder.record_processing_gap_if_empty.function(
        matched_keys=[], dataset_name=dataset_name, dag_run=None
    )
