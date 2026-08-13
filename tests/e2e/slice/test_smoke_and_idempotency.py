"""tests/e2e/slice/test_smoke_and_idempotency.py — U1's proof, and LOAD-03/QUAL-09's reupload proof.

Honest limit: both tests below prove their claim for ONE triggered run /
ONE uploaded pair, against whatever `csv_processor_image` Variable and
`customers` `DatasetConfig` are live on the cluster at test time — they do
not re-verify the image was built from a clean tree, nor that no other
process is concurrently mutating `raw/customers/` (04-09-PLAN.md runs
E2E-adjacent work against the SAME live cluster in this wave; see that
plan's own note about shared-infrastructure interference).
"""

from __future__ import annotations

import contextlib
import datetime
import subprocess
import time
import uuid
from typing import TYPE_CHECKING, Any

import pytest

from tests.e2e.slice.conftest import poll_file_discovered, poll_ingestion_run, poll_run_for_file

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    import psycopg

pytestmark = pytest.mark.cluster

_SMOKE_DAG_ID = "smoke_kubernetes_pod"
_SMOKE_TASK_ID = "print_version_to_xcom"
_CUSTOMERS_DATASET = "customers"
_DAG_RUN_TIMEOUT_SECONDS = 180
_INGEST_TIMEOUT_SECONDS = 180


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

    built_sha = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],  # noqa: S607 -- fixed argv, no user input
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    ).stdout.strip()

    assert xcom_value["git_sha"] == built_sha, (
        f"XCom git_sha {xcom_value['git_sha']!r} does not match this checkout's "
        f"`git rev-parse --short HEAD` ({built_sha!r}) -- the image running in the cluster "
        f"was not built from the commit currently checked out"
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

`xcom.value["git_sha"] == subprocess.run(["git", "rev-parse", "--short", "HEAD"]).stdout.strip()`
evaluated against the checkout this test ran from, read directly from the Airflow
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
    slice_fixtures_dir: Path,
) -> None:
    """D-07/LOAD-03/QUAL-09: re-uploading identical content under a new key adds zero rows.

    Uploads `customers_small.csv`'s bytes (with a unique per-test-run marker
    embedded in the first data row's `name` field, so this test is safe to
    re-run against a live cluster that already carries a prior run's data --
    a stale content-hash match from an EARLIER test run would otherwise make
    the "first" upload look like a duplicate too) to `raw/customers/`, waits
    for it to succeed, records `normalized.customers`'s row count, uploads
    the SAME bytes under a second key, and asserts `meta.files` marks the
    second arrival a duplicate of the first, no second `meta.ingestion_runs`
    row is created, and `normalized.customers`'s row count is unchanged.
    """
    app = s3_client("app")
    admin = s3_client("admin")

    base_bytes = (slice_fixtures_dir / "customers_small.csv").read_bytes()
    payload = _unique_small_csv_bytes(base_bytes)

    marker = uuid.uuid4().hex[:12]
    key_1 = f"customers/e2e-idempotent-{marker}-1.csv"
    key_2 = f"customers/e2e-idempotent-{marker}-2.csv"
    object_uri_1 = f"s3://raw/{key_1}"
    object_uri_2 = f"s3://raw/{key_2}"

    try:
        app.put_object(Bucket="raw", Key=key_1, Body=payload)

        file_1 = poll_file_discovered(
            analytics_connection,
            dataset=_CUSTOMERS_DATASET,
            object_uri=object_uri_1,
            timeout=_DAG_RUN_TIMEOUT_SECONDS,
        )
        assert file_1["duplicate_of_file_id"] is None, (
            f"the FIRST upload of a fresh, uniquely-marked payload was already marked a "
            f"duplicate (of file_id={file_1['duplicate_of_file_id']!r}) -- the uniqueness "
            f"marker did not make this content genuinely new"
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

        row_count_before = _customers_row_count(analytics_connection)

        app.put_object(Bucket="raw", Key=key_2, Body=payload)

        file_2 = poll_file_discovered(
            analytics_connection,
            dataset=_CUSTOMERS_DATASET,
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

        row_count_after = _customers_row_count(analytics_connection)
        assert row_count_after == row_count_before, (
            f"normalized.customers row count changed after the duplicate reupload: "
            f"{row_count_before} -> {row_count_after}"
        )
    finally:
        # Best-effort cleanup: S3 DELETE is idempotent (a missing key is not
        # an error), so this only needs to tolerate an infra-level failure
        # (network, credential) without masking a real assertion failure
        # above with a cleanup exception.
        for key in (key_1, key_2):
            with contextlib.suppress(Exception):
                admin.delete_object(Bucket="raw", Key=key)


def _unique_small_csv_bytes(base_bytes: bytes) -> bytes:
    """Return `base_bytes` with a fresh, unique marker in the first data row's `name` field.

    Keeps the file's shape (121 lines, 5 columns) byte-identical to the
    generated fixture except for this one field, so `content_sha256` is
    guaranteed different from any PRIOR test run's upload of the same base
    fixture -- the reason `test_idempotent_reupload` is safe to re-run
    against a live cluster that already carries earlier runs' data.
    """
    lines = base_bytes.decode("utf-8").splitlines(keepends=True)
    header, first_row, *rest = lines
    fields = first_row.rstrip("\r\n").split(",")
    terminator = first_row[len(first_row.rstrip("\r\n")) :]
    fields[1] = f"E2E-{uuid.uuid4().hex[:16]}"
    lines = [header, ",".join(fields) + terminator, *rest]
    return "".join(lines).encode("utf-8")


def _customers_row_count(conn: psycopg.Connection[Any]) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM normalized.customers")
        row = cur.fetchone()
        assert row is not None
        return int(row[0])
