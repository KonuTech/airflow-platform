"""`dag.test()` proves `platform_retention`'s DAG-level mechanics and dry-run-by-default safety.

Scope, per RESEARCH.md Pitfall 3's own three-tier split (mirroring
`test_backfill_dagrun.py`'s identical framing): this tier proves DAG-level
mechanics -- the `run_retention` task genuinely reaches `success` inside a
real `DagRun`, and no delete-shaped call happens under the default (dry-run)
configuration -- not the real SQL/MinIO semantics of `_common/
retention_query.py`'s own queries (a lower, `pytest.mark.integration` tier's
job, matching `test_run_stage_recorder.py`'s/`test_gap_recorder.py`'s own
real-testcontainers-database precedent for THAT concern) and not a full
live-cluster proof.

Why a genuinely new fixture (not `mock_s3_infrastructure`) is unavoidable
here: `conftest.py`'s own `_FakeS3Client` implements exactly
`integrity_gate.py`'s two call shapes (`head_object`/`get_object`) --
`platform_retention`'s own MinIO touchpoints (`list_objects_v2`/
`delete_objects`, via `_common/retention_query.py`) are a different shape
entirely, and `platform_retention` has no `KubernetesPodOperator` task at
all, so `mock_kpo_execute` does not apply either. Rather than mock
`boto3`/`psycopg` at the transport level, this module replaces
`_common.retention_query`'s own query/connect functions directly --
`tests/dagtest/conftest.py`'s own `mock_run_stage_recorder_db` fixture
already establishes and documents why this is the SAFE pattern in this
tier: those functions are bound into a `_common/` submodule that Airflow's
`DagBag` does NOT re-execute on every `load_dag(...)` call (only the
top-level DAG file itself is freshly re-parsed), so patches applied before
`load_dag("platform_retention")` runs survive into the DagRun's real
in-process execution of `run_retention`. `_connect` (not the global
`psycopg.connect`) is the one seam replaced for the DB side, for the exact
reason `mock_run_stage_recorder_db`'s own docstring gives: Airflow's own
`postgresql+psycopg` metadata-DB dialect calls the SAME module-level
`psycopg.connect`, so patching it globally would silently intercept the
metadata DB's real connections too.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Self
from unittest.mock import patch

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

pytestmark = pytest.mark.dagtest


class _FakeCursor:
    """Records every SQL statement executed -- a real call-log, not just "no exception"."""

    def __init__(self) -> None:
        self.executed: list[str] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False

    def execute(self, query: str, params: object = None) -> _FakeCursor:
        del params
        self.executed.append(query)
        return self

    def fetchall(self) -> list[Any]:
        return []


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False

    def cursor(self) -> _FakeCursor:
        return self._cursor


@pytest.fixture
def patch_retention_queries(airflow_env: None) -> Iterator[dict[str, Any]]:  # noqa: ARG001 -- ordering dependency, see load_dag above
    """Replace every I/O touchpoint `_common/retention_query.py` owns with in-memory fakes.

    Configures exactly one dataset ("customers") with a `processed_days=1`
    retention window and one MinIO `processed`-layer candidate aged 5 days
    -- genuinely over-window, so `evaluate_retention`'s own
    `would_delete_count` is nonzero for at least one layer, the precondition
    `test_dry_run_default_performs_zero_deletion_calls` needs. `enforce`
    stays `False` (D-38's own default), so no delete-shaped call should ever
    happen.

    Yields a dict exposing the fake cursor (`"cursor"`, whose `.executed`
    list proves whether any SQL `DELETE` ran) and per-function call counts
    (`"calls"`), so a test can assert BOTH that the real query paths were
    genuinely exercised (not skipped by an upstream failure) AND that no
    delete-shaped call happened -- the acceptance criteria's own "a call
    count/log, not merely the absence of an exception" bar.
    """
    from _common import retention_query  # noqa: PLC0415 -- deferred, see load_dag above
    from dataplat.config.model import RetentionConfig  # noqa: PLC0415
    from dataplat.retention.policy import RetentionCandidate  # noqa: PLC0415

    cursor = _FakeCursor()
    connection = _FakeConnection(cursor)

    dataset_config = SimpleNamespace(
        retention=RetentionConfig(processed_days=1, enforce=False),
        source=SimpleNamespace(path="customers/"),
    )
    over_window_candidate = RetentionCandidate(
        layer="processed",
        identifier="customers/stale-fixture.csv",
        age_days=5,
        size_bytes=1024,
    )

    calls: dict[str, list[Any]] = {"minio": [], "sql_age": []}

    def _fake_minio_candidates(layer: str, prefix: str) -> list[Any]:
        calls["minio"].append((layer, prefix))
        return [over_window_candidate] if layer == "processed" else []

    def _fake_sql_age_candidates(
        cur: object, layer: str, query: str, dataset_id: int, id_prefix: str = ""
    ) -> list[Any]:
        del cur, query
        calls["sql_age"].append((layer, dataset_id, id_prefix))
        return []

    def _fake_get_connection(_conn_id: str) -> SimpleNamespace:
        return SimpleNamespace(get_uri=lambda: "postgresql://unused-fake-dsn/db")

    with (
        patch.object(retention_query, "_connect", lambda _dsn: connection),
        patch.object(retention_query.BaseHook, "get_connection", _fake_get_connection),
        patch.object(
            retention_query,
            "_current_dataset_configs",
            lambda _cur: [(1, "customers", dataset_config)],
        ),
        patch.object(retention_query, "_minio_candidates", side_effect=_fake_minio_candidates),
        patch.object(retention_query, "_sql_age_candidates", side_effect=_fake_sql_age_candidates),
    ):
        yield {"cursor": cursor, "calls": calls}


def test_platform_retention_dagrun_reaches_success_in_dry_run_mode(
    load_dag: Callable[[str], Any],
    patch_retention_queries: dict[str, Any],  # noqa: ARG001 -- fixture used for its patching side effect only
) -> None:
    """`dag.test()` against `platform_retention` proves the DAG's own mechanics reach `success`."""
    dag = load_dag("platform_retention")

    dag_run = dag.test()

    assert dag_run.state == "success", (
        f"DagRun did not reach success (state={dag_run.state!r}); task states: "
        f"{[(ti.task_id, ti.state) for ti in dag_run.get_task_instances()]}"
    )
    task_instances = list(dag_run.get_task_instances())
    assert task_instances, "no task instances ran -- the DagRun did nothing"
    assert all(ti.state == "success" for ti in task_instances)


def test_dry_run_default_performs_zero_deletion_calls(
    load_dag: Callable[[str], Any],
    patch_retention_queries: dict[str, Any],
) -> None:
    """With a real over-window candidate present, `enforce=False` still issues zero deletes.

    Genuinely inspects the mock's own call surface (per the acceptance
    criteria), not merely the absence of an exception: an evaluator that
    silently no-ops for the WRONG reason (e.g. never queried anything at
    all) must still fail this test, so this asserts BOTH that the read/list
    query paths were exercised (`calls["minio"]`/`calls["sql_age"]`
    non-empty) AND that no `DELETE` statement was ever executed against the
    fake cursor.
    """
    dag = load_dag("platform_retention")

    dag_run = dag.test()

    assert dag_run.state == "success"

    calls = patch_retention_queries["calls"]
    assert calls["minio"], "no MinIO candidate query ran -- the read path was never exercised"
    assert calls["sql_age"], "no SQL age-based candidate query ran -- read path never exercised"

    cursor = patch_retention_queries["cursor"]
    delete_statements = [stmt for stmt in cursor.executed if "DELETE" in stmt.upper()]
    assert not delete_statements, (
        f"enforce=False must issue zero deletes, but found: {delete_statements}"
    )
