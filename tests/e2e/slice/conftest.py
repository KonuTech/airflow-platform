"""Shared fixtures for tests/e2e/slice/ — the vertical-slice E2E harness (04-08-PLAN.md).

`tests/e2e/slice/` is a SIBLING of `tests/e2e/cluster/`, not a child of it —
pytest's conftest inheritance only flows down a directory tree, so this
module cannot inherit `tests/e2e/cluster/conftest.py`'s fixtures for free.
`tests/e2e/` is a real Python package (every level under `tests/` carries an
`__init__.py`), so the fixtures this suite genuinely shares with
`tests/e2e/cluster/` — `_require_cluster`'s skip-with-reason behaviour, the
`kubectl`/`kubectl_json` helpers, and the live `s3_client` factory — are
imported directly from that module rather than re-derived (this file's own
docstring commitment: never invent a second, differently-worded skip
message, and never build a third divergent MinIO-client construction
alongside `tests/e2e/cluster/conftest.py`'s and `scripts/ingest-demo.py`'s).

What IS re-derived here, deliberately: `tests/e2e/cluster/test_postgres_
topology.py`'s `_cluster_connection`/`_port_forwarded_postgres`/
`_read_app_secret` helpers are private (leading underscore) and colocated
with that module's own test bodies, so this file copies their SHAPE (same
free-port-then-port-forward-then-connect pattern, same unconditional
teardown) rather than importing private names — the same choice
04-08-PLAN.md's own Interfaces section calls out explicitly. Two roles are
supported, not the CNPG-generated owner alone: `role="owner"` is the exact
`_cluster_connection("analytics-db")` shape (reads the `analytics-db-app`
Secret CNPG itself generates); `role="etl_app"` is new here, reading the
`csv-processor-db` Secret plan 04-02 created (namespace `etl`, a single
`dsn` key) so this suite's default connection authenticates as the SAME
role the real pipeline pods use (`DATAPLAT_DB_DSN`,
`airflow/dags/_common/kpo.py`) — matching what the pipeline itself sees,
per 04-08-PLAN.md's Interfaces section. `analytics_owner_connection` is for
the narrower set of needs that genuinely require broader access than the
pipeline's own role has (this suite does not currently need one, but the
fixture pair is provided as 04-08-PLAN.md's Interfaces section specifies).
"""

from __future__ import annotations

import base64
import contextlib
import shutil
import socket
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote, urlsplit

import psycopg
import pytest
from tools.corpus.generators import generate_corpus
from tools.corpus.manifest import load_manifest

from tests.e2e.cluster.conftest import (  # noqa: F401 -- re-exported as pytest fixtures below
    _require_cluster,
    cluster_name,
    kubectl,
    kubectl_context,
    kubectl_json,
    repo_root,
    s3_client,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

SLICE_MANIFEST = Path(__file__).resolve().parents[2] / "fixtures" / "slice-corpus.yaml"

# tests/fixtures/slice-corpus.yaml's own `customers_large.csv` declaration --
# kept as a named constant here (not re-read from the manifest at collection
# time) so a manifest edit that changes `rows:` is a visible two-file diff,
# not a silent behavior change to every test that reads this constant.
LARGE_FIXTURE_ROWS = 1_000_000

_DATA_NAMESPACE = "data"
_ETL_NAMESPACE = "etl"
_ANALYTICS_CLUSTER = "analytics-db"
_ANALYTICS_DB_SECRET = "csv-processor-db"  # noqa: S105 -- a K8s Secret's metadata.name, not a credential

# The Airflow metadata cluster's own CNPG-generated owner Secret. Lives in
# `data` alongside `analytics-db-app` -- CNPG's own Secrets are namespaced by
# where the `Cluster` CR lives, not by which application (`airflow` vs.
# `etl`) reads them (matching `tests/e2e/cluster/test_airflow_workloads.py`'s
# own `DATA_NAMESPACE`/`METADATA_CLUSTER` constants exactly).
_AIRFLOW_DB_CLUSTER = "airflow-db"
_AIRFLOW_DB_SECRET = "airflow-db-app"  # noqa: S105 -- a K8s Secret's metadata.name, not a credential

# Terminal statuses `dataplat.pipeline.run.run_ingest`/`csv_processor.cli.ingest`
# ever write to `meta.ingestion_runs.status` (packages/dataplat/src/dataplat/
# pipeline/run.py's own docstring: SUCCEEDED, and the two `_skipped_receipt`
# outcomes; csv_processor.cli.ingest's `except DataPlatformError` branch adds
# FAILED). Any other value means the run is still in flight.
_TERMINAL_RUN_STATUSES = frozenset(
    {"SUCCEEDED", "FAILED", "SKIPPED_DUPLICATE", "SKIPPED_CONCURRENT"},
)

# Short inter-poll delay used by every deadline loop below — never the WAIT
# itself (PITFALLS' "sleep in E2E tests is a permanent-flakiness trap",
# ~line 2168; this codebase's own established `_port_forwarded_postgres`
# idiom).
_POLL_INTERVAL_SECONDS = 0.5


@pytest.fixture(scope="session")
def slice_fixtures_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Generate both `slice-corpus.yaml` fixtures once per session, return the directory.

    Calls `tools.corpus.generators.generate_corpus` directly in Python — no
    `make` subprocess, no pre-generated files committed to git (QUAL-08's
    "generated from a seed, not committed en masse" policy, extended to this
    corpus too). Generates the FULL manifest (never `fast=True`): this
    suite's own tests need `customers_large.csv`, not just the fast-loop
    small fixture.

    Args:
        tmp_path_factory: pytest's session-scoped temporary-directory
            factory.

    Returns:
        The directory both `customers_small.csv` and `customers_large.csv`
        were written into.
    """
    out_dir = tmp_path_factory.mktemp("slice-corpus")
    manifest = load_manifest(SLICE_MANIFEST)
    generate_corpus(manifest, out_dir, fast=False)
    return out_dir


def large_csv_with_offset_customer_ids(base_bytes: bytes, *, offset: int) -> bytes:
    """Return `base_bytes` with every row's `customer_id` shifted by `offset`.

    Shared by every test in this directory that uploads `customers_large.
    csv`: `test_pod_kill_retry.py`'s two tests and `test_concurrent_select.
    py`'s one. Every run gets its own randomly-chosen `offset` (never the
    fixture's literal `1..LARGE_FIXTURE_ROWS` range), so repeat runs of this
    suite, `test_smoke_and_idempotency.py`'s small-fixture test (customer_id
    `1..120`), and 04-09-PLAN.md's own concurrent demo activity never
    contend for the same `normalized.customers` keys -- a collision would
    make an exact-row-count assertion, a throughput figure derived from
    `rows_loaded`, or a concurrent-SELECT observation window all
    meaningless, since `ON CONFLICT ... WHERE _record_hash IS DISTINCT ...`
    correctly suppresses a no-op republish of already-identical rows.

    `customer_id` is always the text before the first comma on a data line
    (never a fixed-width slice: the fixture's `zero_padded_int(width=6)`
    renders row 1,000,000 as 7 digits, "1000000", not 6 -- see
    `tools/corpus/generators.py`'s own `_zero_padded_renderer`, a MINIMUM
    width). This changes only that leading integer, keeping every other
    field (`name`/`country`/`birth_date`/`event_ts`) exactly as generated.

    Args:
        base_bytes: The generated `customers_large.csv` bytes, unmodified.
        offset: Added to every row's `customer_id`.

    Returns:
        The rewritten CSV bytes, same header, same row count, same line
        terminator (`\\n`, matching `tests/fixtures/slice-corpus.yaml`'s own
        declaration).
    """
    lines = base_bytes.decode("utf-8").split("\n")
    out = [lines[0]]
    for line in lines[1:]:
        if not line:
            out.append(line)
            continue
        first_comma = line.index(",")
        new_id = int(line[:first_comma]) + offset
        out.append(f"{new_id:06d}{line[first_comma:]}")
    return "\n".join(out).encode("utf-8")


def _free_local_port() -> int:
    """Ask the OS for an unused TCP port, then release it immediately."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _read_secret_data(
    kubectl_json_fn: Callable[..., Any],
    namespace: str,
    name: str,
) -> dict[str, str]:
    """Read and base64-decode any Kubernetes Secret's `data` block.

    Never caches beyond one call, never writes a decoded value to disk or a
    log — the same discipline `tests/e2e/cluster/conftest.py`'s
    `_read_minio_credentials` and `test_postgres_topology.py`'s
    `_read_app_secret` already establish, generalised here to an arbitrary
    namespace/name pair (needed for both the CNPG-generated
    `analytics-db-app` Secret and plan 04-02's own `csv-processor-db`
    Secret, which live in different namespaces with different key shapes).

    Args:
        kubectl_json_fn: The `kubectl_json` fixture callable.
        namespace: The Secret's namespace.
        name: The Secret's name.

    Returns:
        Every key in the Secret's `data` block, base64-decoded.
    """
    secret = kubectl_json_fn("-n", namespace, "get", "secret", name)
    return {key: base64.b64decode(value).decode("utf-8") for key, value in secret["data"].items()}


@contextlib.contextmanager
def _port_forwarded_analytics(kubectl_context_value: str) -> Iterator[int]:
    """Port-forward `analytics-db-rw` to a free local port for this test process only.

    Copied in shape from `tests/e2e/cluster/test_postgres_topology.py`'s
    `_port_forwarded_postgres` (that name is private, so this is a copy, not
    an import — see module docstring). Torn down unconditionally in the
    `finally` block.

    Args:
        kubectl_context_value: The `kubectl_context` fixture's value.

    Yields:
        The local port `analytics-db-rw:5432` was forwarded to.
    """
    kubectl_bin = shutil.which("kubectl")
    assert kubectl_bin, "kubectl not found on PATH"

    local_port = _free_local_port()
    proc = subprocess.Popen(  # noqa: S603
        [
            kubectl_bin,
            "--context",
            kubectl_context_value,
            "-n",
            _DATA_NAMESPACE,
            "port-forward",
            f"svc/{_ANALYTICS_CLUSTER}-rw",
            f"{local_port}:5432",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        deadline = time.monotonic() + 30
        connected = False
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                output = proc.stdout.read() if proc.stdout else ""
                msg = f"kubectl port-forward for {_ANALYTICS_CLUSTER}-rw exited early:\n{output}"
                raise AssertionError(msg)
            with (
                contextlib.suppress(OSError),
                socket.create_connection(("127.0.0.1", local_port), timeout=1),
            ):
                connected = True
                break
            time.sleep(_POLL_INTERVAL_SECONDS)
        if not connected:
            msg = (
                f"kubectl port-forward for {_ANALYTICS_CLUSTER}-rw never accepted a "
                f"connection within 30s"
            )
            raise AssertionError(msg)
        yield local_port
    finally:
        proc.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=10)


@contextlib.contextmanager
def _port_forwarded_airflow_db(kubectl_context_value: str) -> Iterator[int]:
    """Port-forward `airflow-db-rw` to a free local port for this test process only.

    Same shape as `_port_forwarded_analytics`, targeting the Airflow
    metadata cluster instead (needed only by `test_smoke_and_idempotency.
    py`'s U1 test, to read `dag_run`/`xcom` directly).

    Args:
        kubectl_context_value: The `kubectl_context` fixture's value.

    Yields:
        The local port `airflow-db-rw:5432` was forwarded to.
    """
    kubectl_bin = shutil.which("kubectl")
    assert kubectl_bin, "kubectl not found on PATH"

    local_port = _free_local_port()
    proc = subprocess.Popen(  # noqa: S603
        [
            kubectl_bin,
            "--context",
            kubectl_context_value,
            "-n",
            _DATA_NAMESPACE,
            "port-forward",
            f"svc/{_AIRFLOW_DB_CLUSTER}-rw",
            f"{local_port}:5432",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        deadline = time.monotonic() + 30
        connected = False
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                output = proc.stdout.read() if proc.stdout else ""
                msg = f"kubectl port-forward for {_AIRFLOW_DB_CLUSTER}-rw exited early:\n{output}"
                raise AssertionError(msg)
            with (
                contextlib.suppress(OSError),
                socket.create_connection(("127.0.0.1", local_port), timeout=1),
            ):
                connected = True
                break
            time.sleep(_POLL_INTERVAL_SECONDS)
        if not connected:
            msg = (
                f"kubectl port-forward for {_AIRFLOW_DB_CLUSTER}-rw never accepted a "
                f"connection within 30s"
            )
            raise AssertionError(msg)
        yield local_port
    finally:
        proc.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=10)


def _etl_app_credentials(kubectl_json_fn: Callable[..., Any]) -> dict[str, str]:
    """Parse `etl_app`'s user/password/dbname out of the `csv-processor-db` Secret's DSN.

    `scripts/etl-secrets.sh`'s own `_ensure_csv_processor_db_secret` writes a
    single `dsn` key: `postgresql://etl_app:<url-encoded-password>@
    analytics-db-rw.data:5432/analytics` — the exact DSN
    `DATAPLAT_DB_DSN` resolves to inside a real KPO pod. The host in that
    DSN is cluster-internal (`analytics-db-rw.data`), unreachable from this
    suite's host process, so only the user/password/dbname are used here;
    the connection itself goes through `_port_forwarded_analytics`.

    Args:
        kubectl_json_fn: The `kubectl_json` fixture callable.

    Returns:
        `{"user": ..., "password": ..., "dbname": ...}`.
    """
    secret = _read_secret_data(kubectl_json_fn, _ETL_NAMESPACE, _ANALYTICS_DB_SECRET)
    parsed = urlsplit(secret["dsn"])
    assert parsed.username is not None, f"csv-processor-db Secret's dsn has no username: {parsed}"
    assert parsed.password is not None, f"csv-processor-db Secret's dsn has no password: {parsed}"
    return {
        "user": unquote(parsed.username),
        "password": unquote(parsed.password),
        "dbname": parsed.path.lstrip("/"),
    }


def _analytics_owner_credentials(kubectl_json_fn: Callable[..., Any]) -> dict[str, str]:
    """Read the CNPG-generated `analytics-db-app` Secret's user/password/dbname.

    Identical in shape to `tests/e2e/cluster/test_postgres_topology.py`'s
    `_read_app_secret` (copied, not imported — see module docstring): this
    is the `analytics_owner` role CNPG's own `bootstrap.initdb` created as
    the `analytics` database's owner, with unrestricted DDL/DML on it.

    Args:
        kubectl_json_fn: The `kubectl_json` fixture callable.

    Returns:
        `{"user": ..., "password": ..., "dbname": ...}`.
    """
    secret = _read_secret_data(kubectl_json_fn, _DATA_NAMESPACE, f"{_ANALYTICS_CLUSTER}-app")
    return {"user": secret["user"], "password": secret["password"], "dbname": secret["dbname"]}


@contextlib.contextmanager
def open_analytics_connection(
    kubectl_context_value: str,
    kubectl_json_fn: Callable[..., Any],
    *,
    role: str = "etl_app",
) -> Iterator[psycopg.Connection[Any]]:
    """Open an independent connection to the analytical cluster, torn down on exit.

    Exposed as a plain function (not only a fixture) so a test needing a
    SECOND, independent connection — `test_concurrent_select.py`'s own
    "observer" connection plus the main thread's own polling connection,
    which cannot safely share one psycopg `Connection` across threads — can
    open one without fighting pytest's per-test fixture caching (requesting
    the same fixture twice in one test returns the SAME cached instance).

    Args:
        kubectl_context_value: The `kubectl_context` fixture's value.
        kubectl_json_fn: The `kubectl_json` fixture callable.
        role: `"etl_app"` (default — matches what the real pipeline pods
            authenticate as) or `"owner"` (the CNPG-generated
            `analytics_owner` role, for test-harness needs broader than the
            pipeline's own access).

    Yields:
        An open `psycopg.Connection` to the `analytics` database.

    Raises:
        ValueError: `role` is neither `"etl_app"` nor `"owner"`.
    """
    if role == "etl_app":
        creds = _etl_app_credentials(kubectl_json_fn)
    elif role == "owner":
        creds = _analytics_owner_credentials(kubectl_json_fn)
    else:
        msg = f"unknown role {role!r} -- use 'etl_app' or 'owner'"
        raise ValueError(msg)

    with _port_forwarded_analytics(kubectl_context_value) as local_port:
        conn = psycopg.connect(
            host="127.0.0.1",
            port=local_port,
            dbname=creds["dbname"],
            user=creds["user"],
            password=creds["password"],
            connect_timeout=10,
        )
        try:
            yield conn
        finally:
            conn.close()


@pytest.fixture
def analytics_connection(
    kubectl_context: str,  # noqa: F811 -- pytest fixture-injection param name, not a real redefinition
    kubectl_json: Callable[..., Any],  # noqa: F811 -- same reasoning as kubectl_context above
) -> Iterator[psycopg.Connection[Any]]:
    """A live `etl_app`-authenticated connection to the analytical cluster.

    The DEFAULT connection this suite's tests use: `etl_app` is the exact
    role the real pipeline pods authenticate as (`DATAPLAT_DB_DSN`), so
    assertions made through this connection observe exactly what the
    pipeline itself is entitled to see.
    """
    with open_analytics_connection(kubectl_context, kubectl_json, role="etl_app") as conn:
        yield conn


@pytest.fixture
def analytics_owner_connection(
    kubectl_context: str,  # noqa: F811 -- pytest fixture-injection param name, not a real redefinition
    kubectl_json: Callable[..., Any],  # noqa: F811 -- same reasoning as kubectl_context above
) -> Iterator[psycopg.Connection[Any]]:
    """A live `analytics_owner`-authenticated connection to the analytical cluster.

    For test-harness needs broader than the pipeline's own `etl_app` role
    grants (04-08-PLAN.md's Interfaces section: "as the owner where broader
    test-harness access is needed"). None of this suite's own tests
    currently require it; provided because the plan names this fixture
    explicitly as a pair with `analytics_connection`.
    """
    with open_analytics_connection(kubectl_context, kubectl_json, role="owner") as conn:
        yield conn


@pytest.fixture
def airflow_metadata_connection(
    kubectl_context: str,  # noqa: F811 -- pytest fixture-injection param name, not a real redefinition
    kubectl_json: Callable[..., Any],  # noqa: F811 -- same reasoning as kubectl_context above
) -> Iterator[psycopg.Connection[Any]]:
    """A live connection to the Airflow metadata cluster (`airflow-db`), owner-equivalent.

    Only `test_smoke_and_idempotency.py`'s U1 test uses this — reading
    `dag_run`/`xcom` directly is the most direct, version-stable way to
    observe a triggered DAG run's outcome and its task's XCom payload
    without depending on the Airflow REST API's auth configuration (this
    cluster's `core.auth_manager` is `FabAuthManager`, which needs a login
    flow this suite has no credential for) or the exact CLI subcommand
    surface, which is not guaranteed stable across Airflow versions the way
    the `xcom`/`dag_run` table shapes are (both live, directly-verified via
    `psql \\d` against this cluster's actual migrated schema).
    """
    secret = _read_secret_data(kubectl_json, _DATA_NAMESPACE, _AIRFLOW_DB_SECRET)
    with _port_forwarded_airflow_db(kubectl_context) as local_port:
        conn = psycopg.connect(
            host="127.0.0.1",
            port=local_port,
            dbname=secret["dbname"],
            user=secret["user"],
            password=secret["password"],
            connect_timeout=10,
        )
        try:
            yield conn
        finally:
            conn.close()


def poll_ingestion_run(
    conn: psycopg.Connection[Any],
    idempotency_key: str,
    *,
    timeout: float = 120,
) -> dict[str, Any]:
    """Poll `meta.ingestion_runs` for `idempotency_key` until it reaches a terminal status.

    The ONE polling helper every test in this directory reuses for "wait
    for this run to finish" (04-08-PLAN.md's own Interfaces section). A
    `time.monotonic()` deadline loop — never `sleep(N)` for the whole wait,
    only a short, bounded interval between polls (this codebase's own
    established `_port_forwarded_postgres` idiom, generalised here to
    application-level run status instead of a TCP connect probe).

    Args:
        conn: An open connection to the analytical database (any role with
            `SELECT` on `meta.ingestion_runs` — both `etl_app` and
            `analytics_owner` have it).
        idempotency_key: The run's idempotency key.
        timeout: Maximum seconds to wait. Defaults to 120.

    Returns:
        `{"status": ..., "rows_loaded": ..., "lease_expires_at": ...}` once
        `status` is one of `SUCCEEDED`/`FAILED`/`SKIPPED_DUPLICATE`/
        `SKIPPED_CONCURRENT`.

    Raises:
        AssertionError: `timeout` elapses first — names the timeout and the
            last-observed status (or "no row yet" if the run was never even
            created).
    """
    deadline = time.monotonic() + timeout
    last_status: str | None = None
    while time.monotonic() < deadline:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, rows_loaded, lease_expires_at "
                "FROM meta.ingestion_runs WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            row = cur.fetchone()
        if row is not None:
            last_status = row[0]
            if last_status in _TERMINAL_RUN_STATUSES:
                return {"status": row[0], "rows_loaded": row[1], "lease_expires_at": row[2]}
        time.sleep(_POLL_INTERVAL_SECONDS)
    msg = (
        f"meta.ingestion_runs[idempotency_key={idempotency_key!r}] did not reach a terminal "
        f"status within {timeout}s (last observed status: {last_status!r})"
    )
    raise AssertionError(msg)


def poll_file_discovered(
    conn: psycopg.Connection[Any],
    *,
    dataset: str,
    object_uri: str,
    timeout: float = 120,
) -> dict[str, Any]:
    """Poll `meta.files` for `object_uri` until discovery has registered it.

    The bridge from "I uploaded a file" to "I know its idempotency_key":
    `idempotency_key` cannot be predicted client-side without replicating
    `dataplat.config.hashing.hash_config`'s canonical-JSON hash, so this
    suite polls by `object_uri` (a value the test itself chose) instead —
    04-08-PLAN.md's own Interfaces section names this as the documented
    alternative to a client-computed key.

    Args:
        conn: An open connection to the analytical database.
        dataset: The dataset name (`meta.datasets.dataset_name`).
        object_uri: The exact `s3://bucket/key` URI the test uploaded to.
        timeout: Maximum seconds to wait. Defaults to 120.

    Returns:
        `{"file_id": ..., "duplicate_of_file_id": ..., "content_sha256": ...}`.

    Raises:
        AssertionError: `timeout` elapses with no matching `meta.files` row.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT f.file_id, f.duplicate_of_file_id, f.content_sha256 "
                "FROM meta.files f "
                "JOIN meta.datasets d ON d.dataset_id = f.dataset_id "
                "WHERE d.dataset_name = %s AND f.object_uri = %s",
                (dataset, object_uri),
            )
            row = cur.fetchone()
        if row is not None:
            return {"file_id": row[0], "duplicate_of_file_id": row[1], "content_sha256": row[2]}
        time.sleep(_POLL_INTERVAL_SECONDS)
    msg = (
        f"meta.files has no row for dataset={dataset!r} object_uri={object_uri!r} "
        f"within {timeout}s -- discovery never registered it"
    )
    raise AssertionError(msg)


def poll_run_for_file(
    conn: psycopg.Connection[Any],
    *,
    file_id: int,
    timeout: float = 120,
) -> dict[str, Any]:
    """Poll `meta.ingestion_runs` for the run discovery created for `file_id`.

    The second half of the `poll_file_discovered` bridge: once a file's
    `meta.files` row exists (and is not a duplicate), discovery also
    pre-allocates its `meta.ingestion_runs` row in the SAME call
    (`dataplat.discovery.discover_files`) -- this polls for that linkage so
    the caller can hand the discovered `idempotency_key` to
    `poll_ingestion_run` for the terminal-status wait.

    Args:
        conn: An open connection to the analytical database.
        file_id: The `meta.files.file_id` to find a run for.
        timeout: Maximum seconds to wait. Defaults to 120.

    Returns:
        `{"run_id": ..., "idempotency_key": ..., "status": ...}`.

    Raises:
        AssertionError: `timeout` elapses with no matching
            `meta.ingestion_runs` row.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT run_id, idempotency_key, status "
                "FROM meta.ingestion_runs WHERE file_id = %s",
                (file_id,),
            )
            row = cur.fetchone()
        if row is not None:
            return {"run_id": row[0], "idempotency_key": row[1], "status": row[2]}
        time.sleep(_POLL_INTERVAL_SECONDS)
    msg = f"meta.ingestion_runs has no row for file_id={file_id} within {timeout}s"
    raise AssertionError(msg)
