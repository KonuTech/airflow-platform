"""tests/e2e/slice/test_smoke_and_idempotency.py — U1's proof, and LOAD-03/QUAL-09's reupload proof.

Honest limit: both tests below prove their claim for ONE triggered run /
ONE uploaded pair, against whatever `csv_processor_image` Variable and
`orders` `DatasetConfig` are live on the cluster at test time — they do
not re-verify the image was built from a clean tree, nor that no other
process is concurrently mutating `raw/orders/` (04-09-PLAN.md runs
E2E-adjacent work against the SAME live cluster in this wave; see that
plan's own note about shared-infrastructure interference).

The reupload proof was repointed from customers to ORDERS by
debug/ci-pipeline-ingestion-timeout ROUND 16 (finding 19-A): content-hash
duplicate detection is dataset-agnostic, and a lone-file delivery honors
orders' contract, where customers' full-snapshot contract makes the same
lone file a -- correct -- mass-delete breaker trip.
"""

from __future__ import annotations

import datetime
import subprocess
import time
import uuid
from typing import TYPE_CHECKING, Any

import pytest

from tests.e2e.slice.conftest import (
    build_orders_csv_bytes,
    existing_customer_ids,
    poll_file_discovered,
    poll_ingestion_run,
    poll_run_for_file,
    trigger_orders_dagrun,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    import psycopg

pytestmark = pytest.mark.cluster

_SMOKE_DAG_ID = "smoke_kubernetes_pod"
_SMOKE_TASK_ID = "print_version_to_xcom"
_ORDERS_DATASET = "orders"
_DAG_RUN_TIMEOUT_SECONDS = 180
_INGEST_TIMEOUT_SECONDS = 180

# `order_id` is `sa.Integer()` (migration 0016) -- this file's own random
# window base range, disjoint from test_referential_orphan.py's
# [1_000_000_000, 1_499_000_000) order_id band; shares the [2_000_000,
# 1_000_000_000) convention the other repointed orders tests use (collision
# odds across a handful of <=1M-row windows in a ~1e9 space are negligible,
# the same accepted odds the customers offsets always carried).
_ORDER_ID_LOW = 2_000_000
_ORDER_ID_HIGH = 1_000_000_000
_IDEMPOTENT_FIXTURE_ROWS = 120


def _run_id_state(
    airflow_metadata_connection: psycopg.Connection[Any],
    dag_id: str,
    run_id: str,
) -> str | None:
    with airflow_metadata_connection.cursor() as cur:
        cur.execute(
            "SELECT state FROM dag_run WHERE dag_id = %s AND run_id = %s",
            (dag_id, run_id),
        )
        row = cur.fetchone()
    return None if row is None else str(row[0])


def _wait_for_dag_run_terminal(
    airflow_metadata_connection: psycopg.Connection[Any],
    dag_id: str,
    run_id: str,
    *,
    timeout: float,
) -> str:
    """Poll `dag_run.state` for `run_id` until it reaches `success`/`failed`.

    A `time.monotonic()` deadline loop, matching the rest of this suite's
    established idiom -- never a blind `sleep(N)` for the whole wait.
    """
    terminal = {"success", "failed"}
    deadline = time.monotonic() + timeout
    last_state: str | None = None
    while time.monotonic() < deadline:
        last_state = _run_id_state(airflow_metadata_connection, dag_id, run_id)
        if last_state in terminal:
            return last_state
        time.sleep(0.5)
    msg = (
        f"dag_run[dag_id={dag_id!r}, run_id={run_id!r}] did not reach a terminal state "
        f"within {timeout}s (last observed state: {last_state!r})"
    )
    raise AssertionError(msg)


def _xcom_return_value(
    airflow_metadata_connection: psycopg.Connection[Any],
    *,
    dag_id: str,
    run_id: str,
    task_id: str,
) -> Any:
    with airflow_metadata_connection.cursor() as cur:
        cur.execute(
            "SELECT value FROM xcom "
            "WHERE dag_id = %s AND run_id = %s AND task_id = %s AND key = 'return_value'",
            (dag_id, run_id, task_id),
        )
        row = cur.fetchone()
    assert row is not None, (
        f"no xcom row for dag_id={dag_id!r} run_id={run_id!r} task_id={task_id!r} "
        f"key='return_value' -- the task may not have pushed one"
    )
    return row[0]


def test_smoke_dag_xcom_contains_built_sha(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    airflow_metadata_connection: psycopg.Connection[Any],
    repo_root: Path,
) -> None:
    """U1's pass criterion, verbatim: the smoke DAG's XCom contains the SHA that was built.

    `smoke_kubernetes_pod` is `@daily`-scheduled (ROADMAP's own "becomes the
    permanent platform smoke test"), so it is triggered explicitly here
    rather than waited for on its own schedule. Unpausing first is required
    -- a paused DAG's triggered run stays `queued` forever
    (`airflow dags trigger --help`'s own documented behaviour).
    """
    run_id = f"e2e-u1-{uuid.uuid4().hex[:12]}"

    unpause = kubectl(
        "-n",
        "airflow",
        "exec",
        "deploy/airflow-api-server",
        "--",
        "airflow",
        "dags",
        "unpause",
        _SMOKE_DAG_ID,
    )
    assert unpause.returncode == 0, f"airflow dags unpause failed:\n{unpause.stderr}"

    trigger = kubectl(
        "-n",
        "airflow",
        "exec",
        "deploy/airflow-api-server",
        "--",
        "airflow",
        "dags",
        "trigger",
        _SMOKE_DAG_ID,
        "--run-id",
        run_id,
    )
    assert trigger.returncode == 0, f"airflow dags trigger failed:\n{trigger.stderr}"

    state = _wait_for_dag_run_terminal(
        airflow_metadata_connection,
        _SMOKE_DAG_ID,
        run_id,
        timeout=_DAG_RUN_TIMEOUT_SECONDS,
    )
    assert state == "success", (
        f"DagRun {run_id!r} of {_SMOKE_DAG_ID!r} finished with state {state!r}, not 'success' -- "
        f"check `kubectl logs` for the KubernetesPodOperator pod"
    )

    xcom_value = _xcom_return_value(
        airflow_metadata_connection,
        dag_id=_SMOKE_DAG_ID,
        run_id=run_id,
        task_id=_SMOKE_TASK_ID,
    )
    assert isinstance(xcom_value, dict), (
        f"expected the XCom return_value to be a JSON object, got {xcom_value!r}"
    )
    assert "git_sha" in xcom_value, f"XCom return_value has no 'git_sha' key: {xcom_value!r}"

    # The two build paths bake DIFFERENT sha shapes into `ENV GIT_SHA`:
    # `make image-csv-processor` uses `git rev-parse --short HEAD` locally,
    # while publish.yml passes the full 40-char `${{ github.sha }}` on CI.
    # Both name the same commit, so the pass criterion is prefix identity
    # against the full sha (min-length-guarded so a degenerate value cannot
    # trivially match), never string equality against one fixed shape --
    # equality against the short form failed every CI run with a provably
    # correct image (debug session ci-pipeline-ingestion-timeout, ROUND 10).
    head_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"],  # noqa: S607 -- fixed argv, no user input
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    ).stdout.strip()

    xcom_sha = str(xcom_value["git_sha"])
    min_abbrev_len = 7  # git's default minimum abbreviated-sha length
    assert len(xcom_sha) >= min_abbrev_len, (
        f"XCom git_sha {xcom_sha!r} is shorter than git's minimum abbreviated-sha "
        f"length ({min_abbrev_len}) -- too short to identify any commit"
    )
    assert head_sha.startswith(xcom_sha), (
        f"XCom git_sha {xcom_sha!r} does not identify this checkout's HEAD "
        f"({head_sha!r}) -- the image running in the cluster was not built from "
        f"the commit currently checked out"
    )

    _write_u1_spike_doc(repo_root, dag_run_id=run_id, git_sha=xcom_value["git_sha"])


def _write_u1_spike_doc(repo_root: Path, *, dag_run_id: str, git_sha: str) -> None:
    """Regenerate `docs/spikes/U1-smoke-xcom.md` from this test's own live result.

    Automated from the test itself (04-08-PLAN.md's own instruction: "so the
    artifact is regenerated, not hand-maintained") -- never hand-edited.
    """
    path = repo_root / "docs" / "spikes" / "U1-smoke-xcom.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    proven_at = datetime.datetime.now(tz=datetime.UTC).isoformat()
    content = f"""# U1 — locally-built image pulls and runs via KubernetesPodOperator on kind

**Regenerated automatically by `tests/e2e/slice/test_smoke_and_idempotency.py::\
test_smoke_dag_xcom_contains_built_sha` — do not hand-edit.**

- Proven at: {proven_at}
- DAG: `{_SMOKE_DAG_ID}`, task `{_SMOKE_TASK_ID}`
- DagRun: `{dag_run_id}`
- Git SHA baked into the running image (`ENV GIT_SHA`,
  `docker/csv-processor/Dockerfile`): `{git_sha}`

## Pass criterion (ROADMAP.md, Spikes table)

> The XCom contains the SHA that was built.

## Assertion proved

`git rev-parse HEAD` of the checkout this test ran from starts with
`xcom.value["git_sha"]` (prefix identity, minimum 7 chars -- the local build bakes
the short sha, CI's publish.yml bakes the full sha; both name the same commit),
with the XCom read directly from the Airflow
metadata database's `xcom` table (`dag_id`, `run_id`, `task_id='{_SMOKE_TASK_ID}'`,
`key='return_value'`) after the triggered DagRun reached `state='success'`.

This is the permanent platform smoke test (ROADMAP.md's own words): `smoke_kubernetes_pod`
proves a locally-built `csv-processor:<git-sha>` image pulls from the local registry and
runs as the `csv-processor` service account via `KubernetesPodOperator`, and that its
`do_xcom_push=True` sidecar delivers `/airflow/xcom/return.json` back to Airflow correctly.
"""
    path.write_text(content, encoding="utf-8")


def test_idempotent_reupload(
    s3_client: Callable[[str], Any],
    analytics_connection: psycopg.Connection[Any],
    analytics_owner_connection: psycopg.Connection[Any],
    kubectl: Callable[..., Any],
) -> None:
    """D-07/LOAD-03/QUAL-09: re-uploading identical content under a new key adds zero rows.

    Repointed at ORDERS (debug/ci-pipeline-ingestion-timeout ROUND 16,
    finding 19-A): content-hash duplicate detection is dataset-agnostic, and
    a lone-file delivery honors orders' contract (no snapshot mass-delete
    breaker), where the same lone file against customers' full-snapshot
    contract is -- correctly -- a mass-delete signal. Builds a fresh
    orders payload (a random `order_id` window makes every run's content
    genuinely new -- the reason this test is safe to re-run against a live
    cluster carrying prior runs' data), uploads it, triggers
    `csv_ingest_orders` (asset-scheduled -- `trigger_orders_dagrun`'s own
    docstring), waits for SUCCEEDED, records `normalized.orders`'s row
    count, uploads the SAME bytes under a second key, triggers again, and
    asserts `meta.files` marks the second arrival a duplicate of the first,
    no second `meta.ingestion_runs` row is created, and `normalized.orders`'s
    row count is unchanged.

    The two raw uploads are deliberately NOT deleted afterwards: raw is
    append-only (section 63/ADR-0011), and `make rebuild-from-raw`'s
    reconstruct-from-raw premise is only coherent if published data's raw
    files persist.
    """
    app = s3_client("app")

    parent_ids = existing_customer_ids(analytics_connection, count=3)
    assert parent_ids, (
        "normalized.customers is empty on this live cluster -- orders fixtures need real "
        "parent customer_ids (the sweep corpus on CI, or any earlier customers ingest "
        "locally, populates it)"
    )

    rng = uuid.uuid4().int
    order_id_start = _ORDER_ID_LOW + (rng % (_ORDER_ID_HIGH - _ORDER_ID_LOW))
    payload = build_orders_csv_bytes(
        order_id_start=order_id_start,
        row_count=_IDEMPOTENT_FIXTURE_ROWS,
        customer_ids=parent_ids,
    )

    marker = uuid.uuid4().hex[:12]
    key_1 = f"orders/e2e-idempotent-{marker}-1.csv"
    key_2 = f"orders/e2e-idempotent-{marker}-2.csv"
    object_uri_1 = f"s3://raw/{key_1}"
    object_uri_2 = f"s3://raw/{key_2}"

    app.put_object(Bucket="raw", Key=key_1, Body=payload)
    trigger_orders_dagrun(kubectl, run_id=f"e2e-idempotent-{marker}-1")

    file_1 = poll_file_discovered(
        analytics_connection,
        dataset=_ORDERS_DATASET,
        object_uri=object_uri_1,
        timeout=_DAG_RUN_TIMEOUT_SECONDS,
    )
    assert file_1["duplicate_of_file_id"] is None, (
        f"the FIRST upload of a fresh, uniquely-windowed payload was already marked a "
        f"duplicate (of file_id={file_1['duplicate_of_file_id']!r}) -- the random order_id "
        f"window did not make this content genuinely new"
    )

    run_1 = poll_run_for_file(analytics_connection, file_id=file_1["file_id"], timeout=30)
    outcome_1 = poll_ingestion_run(
        analytics_connection,
        run_1["idempotency_key"],
        timeout=_INGEST_TIMEOUT_SECONDS,
    )
    assert outcome_1["status"] == "SUCCEEDED", (
        f"first upload's ingestion run finished {outcome_1['status']!r}, not SUCCEEDED"
    )

    row_count_before = _orders_row_count(analytics_connection)

    app.put_object(Bucket="raw", Key=key_2, Body=payload)
    trigger_orders_dagrun(kubectl, run_id=f"e2e-idempotent-{marker}-2")

    file_2 = poll_file_discovered(
        analytics_connection,
        dataset=_ORDERS_DATASET,
        object_uri=object_uri_2,
        timeout=_DAG_RUN_TIMEOUT_SECONDS,
    )
    assert file_2["duplicate_of_file_id"] == file_1["file_id"], (
        f"second upload's meta.files row has duplicate_of_file_id="
        f"{file_2['duplicate_of_file_id']!r}, expected {file_1['file_id']!r} "
        f"(the first upload's file_id)"
    )

    with analytics_owner_connection.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM meta.ingestion_runs WHERE file_id = %s",
            (file_2["file_id"],),
        )
        count_row = cur.fetchone()
        assert count_row is not None
        second_file_run_count = count_row[0]
    assert second_file_run_count == 0, (
        f"expected NO meta.ingestion_runs row for the duplicate file "
        f"(file_id={file_2['file_id']!r}), found {second_file_run_count}"
    )

    row_count_after = _orders_row_count(analytics_connection)
    assert row_count_after == row_count_before, (
        f"normalized.orders row count changed after the duplicate reupload: "
        f"{row_count_before} -> {row_count_after}"
    )


def _orders_row_count(conn: psycopg.Connection[Any]) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM normalized.orders")
        row = cur.fetchone()
        assert row is not None
        return int(row[0])
