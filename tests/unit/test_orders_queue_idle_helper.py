"""`wait_for_orders_dagrun_queue_idle`'s loop logic, proven offline with a fake connection.

Added by debug/ci-pipeline-ingestion-timeout ROUND 18 alongside the helper
itself (tests/e2e/slice/conftest.py): the helper is the accepted-behavior
disposition for the R17 dbtkill/u3/orphan discovery-starvation family, so its
own return/poll/fail mechanics deserve a proof that does not need a live
cluster -- the e2e suite only ever exercises its happy path. The fake
connection scripts `fetchall` results per poll (idle immediately, drains
after N polls, never drains), mirroring how `dag_run` rows would evolve;
no psycopg wire behavior is simulated because the helper only uses the
`cursor() -> execute -> fetchall` surface.
"""

from __future__ import annotations

from typing import Any, Self

import pytest

from tests.e2e.slice.conftest import wait_for_orders_dagrun_queue_idle


class _FakeCursor:
    """One scripted cursor: records the executed query, serves one fetchall result."""

    def __init__(self, owner: _FakeConn) -> None:
        self._owner = owner

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
        self._owner.executed.append((sql, params))

    def fetchall(self) -> list[tuple[str, str]]:
        return self._owner.next_result()


class _FakeConn:
    """Serves each scripted per-poll result in order, repeating the last one forever."""

    def __init__(self, script: list[list[tuple[str, str]]]) -> None:
        self._script = script
        self._index = 0
        self.executed: list[tuple[str, Any]] = []

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def next_result(self) -> list[tuple[str, str]]:
        result = self._script[min(self._index, len(self._script) - 1)]
        self._index += 1
        return result


def test_returns_immediately_when_queue_is_idle() -> None:
    conn = _FakeConn(script=[[]])
    wait_for_orders_dagrun_queue_idle(conn, timeout=5)  # type: ignore[arg-type]
    assert len(conn.executed) == 1, "an idle queue must be a single-query fast path"


def test_polls_until_the_queue_drains() -> None:
    conn = _FakeConn(
        script=[
            [("backlog-1", "running"), ("backlog-2", "queued")],
            [("backlog-2", "running")],
            [],
        ]
    )
    wait_for_orders_dagrun_queue_idle(conn, timeout=10)  # type: ignore[arg-type]
    assert len(conn.executed) == 3, "must re-poll until the scripted drain completes"


def test_fails_legibly_when_the_queue_never_drains() -> None:
    conn = _FakeConn(script=[[("wedged-run", "queued"), ("cron-run", "running")]])
    with pytest.raises(AssertionError) as excinfo:
        # One initial poll plus at most one retry inside a sub-second budget --
        # the deadline loop's real interval (0.5s) makes this deterministic
        # and fast without patching time.
        wait_for_orders_dagrun_queue_idle(conn, timeout=0.7)  # type: ignore[arg-type]
    message = str(excinfo.value)
    assert "csv_ingest_orders" in message, "failure must name the DAG whose queue is stuck"
    assert "wedged-run" in message, "failure must name each still-active run_id"
    assert "cron-run" in message
    assert "queued" in message, "failure must name each run's state"
    assert "running" in message, "failure must name each run's state"


def test_dag_id_parameterizes_the_query() -> None:
    conn = _FakeConn(script=[[]])
    wait_for_orders_dagrun_queue_idle(conn, dag_id="csv_ingest_customers", timeout=5)  # type: ignore[arg-type]
    _sql, params = conn.executed[0]
    assert params == ("csv_ingest_customers",)
