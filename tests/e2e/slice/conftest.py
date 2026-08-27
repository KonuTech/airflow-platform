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
import csv
import io
import shutil
import socket
import subprocess
import time
from datetime import UTC
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote, urlsplit

import hvac
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
from tests.e2e.vault.conftest import (
    vault_addr,  # noqa: F401 -- re-exported as a pytest fixture below
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

# The Airflow metadata cluster's own CNPG-generated owner Secret. Lives in
# `data` alongside `analytics-db-app` -- CNPG's own Secrets are namespaced by
# where the `Cluster` CR lives, not by which application (`airflow` vs.
# `etl`) reads them (matching `tests/e2e/cluster/test_airflow_workloads.py`'s
# own `DATA_NAMESPACE`/`METADATA_CLUSTER` constants exactly).
_AIRFLOW_DB_CLUSTER = "airflow-db"
_AIRFLOW_DB_SECRET = "airflow-db-app"  # noqa: S105 -- a K8s Secret's metadata.name, not a credential

# Terminal statuses the pipeline ever writes to `meta.ingestion_runs.status`
# (packages/dataplat/src/dataplat/pipeline/run.py: SUCCEEDED, the
# `_skipped_receipt` outcomes, FAILED via the CLI's exception branches and
# ROUND 15's crash-release). QUARANTINED added by debug/ci-pipeline-
# ingestion-timeout ROUND 15 (it became a terminal status in ROUND 14's
# trim-ii quarantine semantics, and this set was never truthed up --
# test_backfill_2year_sweep.py's own local set already carries it): a poll
# waiting on a QUARANTINED run would otherwise burn its full timeout on an
# already-decided outcome, exactly candidate (19)'s predicted fail-fast
# signature turned slow again. Any other value means the run is still in
# flight.
_TERMINAL_RUN_STATUSES = frozenset(
    {"SUCCEEDED", "FAILED", "SKIPPED_DUPLICATE", "SKIPPED_CONCURRENT", "QUARANTINED"},
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


_SMOKE_DAG_ID = "smoke_kubernetes_pod"
_CUSTOMERS_DAG_ID = "csv_ingest_customers"
_ORDERS_DAG_ID = "csv_ingest_orders"


@pytest.fixture(scope="session", autouse=True)
def _unpause_slice_dags(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],  # noqa: F811 -- fixture-injection param name
) -> None:
    """Unpause all three of this phase's DAGs once per session — a paused DAG never runs.

    Discovered live: `test_smoke_dag_xcom_contains_built_sha` already
    unpauses `smoke_kubernetes_pod` itself before triggering it explicitly
    (idempotent, so this overlaps harmlessly, not a conflict) — but nothing
    anywhere in this suite unpaused `csv_ingest_customers`, which every test
    that uploads a file and polls for discovery
    (`test_idempotent_reupload`, both `test_pod_kill_retry` tests,
    `test_concurrent_select_never_observes_partial_publish`) depends on
    actually running on its own schedule/sensor. A paused DAG's scheduler
    simply never starts a run for it — there is no error, no timeout
    shortcut, just silence — so every one of those tests would poll
    `meta.files` until its own deadline and fail with a misleading
    "discovery never registered it" message that looks like a pipeline bug.
    Session-scoped and autouse so every test in this suite gets a running
    DAG regardless of which file pytest happens to collect first.

    `csv_ingest_orders` was added by debug/ci-pipeline-ingestion-timeout
    ROUND 13 (root cause 17): this docstring used to say "both this phase's
    DAGs" while the tuple below covered only smoke + customers — but orders
    is a phase DAG too, and as an ASSET-scheduled DAG
    (`schedule=[customers_asset]`) the failure shape while paused is even
    quieter than the cron case described above: a paused DAG silently
    consumes no asset events at all, so on every fresh/ephemeral cluster
    (Airflow default `dags_are_paused_at_creation=true`) orders never ran
    once, and the sweep test's orders-terminal wait burned 87 minutes of CI
    budget polling for a dataset whose DAG could never run. Belt-and-braces
    with the DAG's own `is_paused_upon_creation=False` (same round): this
    fixture also repairs clusters whose DagModel row predates that flag.
    """
    for dag_id in (_SMOKE_DAG_ID, _CUSTOMERS_DAG_ID, _ORDERS_DAG_ID):
        result = kubectl(
            "-n",
            "airflow",
            "exec",
            "deploy/airflow-api-server",
            "--",
            "airflow",
            "dags",
            "unpause",
            dag_id,
        )
        assert result.returncode == 0, f"airflow dags unpause {dag_id} failed:\n{result.stderr}"


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


def existing_customer_ids(conn: psycopg.Connection[Any], *, count: int) -> list[int]:
    """Return up to `count` genuinely-present `normalized.customers.customer_id` values.

    Shared by every ORDERS-repointed e2e test in this directory
    (debug/ci-pipeline-ingestion-timeout ROUND 16, finding 19-A):
    `orders.yaml`'s REFERENTIAL quality rule quarantines any orders row
    whose `customer_id` has no `normalized.customers` parent, so orders
    fixtures must reference real, live-queried parents -- the exact idiom
    `test_referential_orphan.py`'s own `_existing_customer_ids` established
    (plain `LIMIT`, never `ORDER BY random()` -- see that helper's comment).
    An orders fixture may CYCLE a small parent pool across many rows (a
    customer with many orders is the realistic shape), so callers assert
    only `len(...) >= 1` with a message pointing at prior customers
    ingestion (the sweep corpus on CI; any earlier ingest locally).
    """
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id FROM normalized.customers LIMIT %s", (count,))
        rows = cur.fetchall()
    return [int(row[0]) for row in rows]


def build_orders_csv_bytes(
    *,
    order_id_start: int,
    row_count: int,
    customer_ids: list[int],
) -> bytes:
    """Build an `orders.yaml`-shaped CSV: `row_count` rows, sequential order_ids, cycled parents.

    Column order (`order_id,customer_id,order_date,amount`) matches
    `configs/datasets/orders.yaml`'s `columns:` block verbatim (positional
    correspondence only -- `test_referential_orphan.py::_build_orders_csv`'s
    own documented precedent). `order_id` runs `order_id_start ..
    order_id_start + row_count - 1` so a caller's window assertions
    (`BETWEEN order_id_start AND order_id_start + row_count - 1`) are exact;
    `customer_id` cycles the live-sampled parent pool; `amount` varies per
    row so two different windows never share full row content by accident.
    """
    assert customer_ids, "build_orders_csv_bytes needs at least one valid parent customer_id"
    pool_size = len(customer_ids)
    lines = ["order_id,customer_id,order_date,amount"]
    lines.extend(
        f"{order_id_start + i},{customer_ids[i % pool_size]},"
        f"2026-01-15,{10 + (i % 90)}.{i % 100:02d}"
        for i in range(row_count)
    )
    return ("\n".join(lines) + "\n").encode("utf-8")


def trigger_orders_dagrun(
    kubectl_fn: Callable[..., subprocess.CompletedProcess[str]],
    *,
    run_id: str,
) -> None:
    """Manually trigger `csv_ingest_orders` -- the asset-scheduled DAG's test-side drive shape.

    `csv_ingest_orders` is Asset-scheduled (`schedule=[customers_asset]`),
    so an orders upload is only DISCOVERED when an orders DagRun actually
    runs -- and during the singles phase nothing may be publishing customers
    (asset events only fire on a real customers publish). A plain
    `airflow dags trigger` is the established, live-proven idiom
    (`test_referential_orphan.py`'s own docstring: Airflow accepts a manual
    trigger for an asset-scheduled DAG exactly like any other DAG). The
    unpause is belt-and-braces with `_unpause_slice_dags` (idempotent).
    """
    unpause = kubectl_fn(
        "-n",
        "airflow",
        "exec",
        "deploy/airflow-api-server",
        "--",
        "airflow",
        "dags",
        "unpause",
        _ORDERS_DAG_ID,
    )
    assert unpause.returncode == 0, f"airflow dags unpause failed:\n{unpause.stderr}"
    trigger = kubectl_fn(
        "-n",
        "airflow",
        "exec",
        "deploy/airflow-api-server",
        "--",
        "airflow",
        "dags",
        "trigger",
        _ORDERS_DAG_ID,
        "--run-id",
        run_id,
    )
    assert trigger.returncode == 0, f"airflow dags trigger failed:\n{trigger.stderr}"


# Mirrors `dataplat.load.publish.scd._CURRENT_COUNT_SQL`'s scoping exactly
# (gold `is_current` rows whose key has EVER appeared in bronze) -- the
# precise roster `MassDeleteCircuitBreaker`'s denominator counts, which is
# exactly the set a snapshot-complete fixture must echo for vanished == 0.
_SNAPSHOT_ROSTER_SQL = """
SELECT customer_id, name, country, birth_date, event_ts
  FROM normalized.customers
 WHERE is_current
   AND customer_id::text IN (SELECT DISTINCT customer_id FROM staging.customers)
 ORDER BY customer_id
"""


def snapshot_complete_customers_csv(
    conn: psycopg.Connection[Any],
    *,
    extra_rows: list[tuple[Any, ...]],
) -> bytes:
    """Build a customers CSV that honors the dataset's FULL-SNAPSHOT delivery contract.

    debug/ci-pipeline-ingestion-timeout ROUND 16, finding (19)-A:
    `customers.yaml` declares `change_semantics: snapshot` + an
    `scd.mass_delete_threshold` breaker -- a "lone" customers file carrying
    only a test's own handful of keys IS a mass-delete signal by that
    contract's own definition (every roster key it omits reads as deleted),
    and ROUND 15 proved the breaker fires on exactly that shape, correctly.
    A delivery-shape-aware customers fixture therefore ECHOES the current
    gold roster (queried live at build time, scoped exactly like the
    breaker's own denominator -- `_SNAPSHOT_ROSTER_SQL`) and appends the
    test's own new rows: vanished == 0 by construction, production breaker
    semantics untouched.

    Echoed rows re-deliver each key's CURRENT attribute values with its
    CURRENT `event_ts`: byte-stable attributes at an already-seen business
    timestamp fold into the existing version chain as duplicate
    observations -- no new SCD versions, no gold changes for echoed keys.
    Rendered via `csv.writer` (values are echoed from the live database, so
    quoting must be handled properly, never by string concatenation). The
    header is the 5-column pre-Phase-10 shape every existing slice fixture
    uses -- `signup_country` is `required: false` (D-13) and ROUND 15's
    schema-compat fix makes the 5-column prefix loadable by contract.

    Args:
        conn: An open analytics connection (any role with SELECT on
            `normalized.customers` and `staging.customers` -- `etl_app`
            has both).
        extra_rows: The test's own `(customer_id, name, country,
            birth_date, event_ts)` tuples, appended verbatim after the
            roster echo.

    Returns:
        The CSV bytes (header + roster echo + extra rows, `\\n` line
        terminator to match every other fixture in this suite).
    """
    with conn.cursor() as cur:
        cur.execute(_SNAPSHOT_ROSTER_SQL)
        roster = cur.fetchall()

    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(["customer_id", "name", "country", "birth_date", "event_ts"])
    for customer_id, name, country, birth_date, event_ts in roster:
        writer.writerow(
            [
                customer_id,
                name,
                country,
                "" if birth_date is None else birth_date.isoformat(),
                event_ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            ]
        )
    for row in extra_rows:
        writer.writerow(list(row))
    return buffer.getvalue().encode("utf-8")


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


def _kubectl_create_token(
    kubectl_fn: Callable[..., Any],
    *,
    service_account: str,
    namespace: str,
) -> str:
    """Obtain a fresh projected token for `service_account` in `namespace`.

    Copied from `tests/e2e/vault/test_positive_auth.py`'s own helper of the
    same name (this repository's established convention: small helpers are
    copied per test tier, not shared through a library module — see this
    module's own docstring).
    """
    proc = kubectl_fn("create", "token", service_account, "-n", namespace)
    assert proc.returncode == 0, (
        f"kubectl create token {service_account} -n {namespace} failed "
        f"(exit {proc.returncode}):\n{proc.stderr}"
    )
    return proc.stdout.strip()


def _etl_app_credentials(
    kubectl_fn: Callable[..., Any],
    vault_addr_value: str,
) -> dict[str, str]:
    """Parse `etl_app`'s user/password/dbname out of Vault's `etl/analytics-db#dsn`.

    Plan 05-02 migrated this credential off the `csv-processor-db`
    Kubernetes Secret (deleted from the live cluster once that migration's
    own live proof passed) into Vault, authenticated the SAME way the real
    pipeline pods do: the `csv-processor` ServiceAccount's own Kubernetes-auth
    role. `scripts/vault-bootstrap.py`'s `_ensure_etl_secrets` writes the
    identical single-`dsn`-key shape the old Secret held —
    `postgresql://etl_app:<url-encoded-password>@analytics-db-rw.data:5432/
    analytics` — so only the read mechanism changed, not the DSN shape. The
    host in that DSN is cluster-internal (`analytics-db-rw.data`),
    unreachable from this suite's host process, so only the
    user/password/dbname are used here; the connection itself goes through
    `_port_forwarded_analytics`.

    Args:
        kubectl_fn: The `kubectl` fixture callable (raw, not JSON-parsing).
        vault_addr_value: The `vault_addr` fixture's value.

    Returns:
        `{"user": ..., "password": ..., "dbname": ...}`.
    """
    csv_processor_jwt = _kubectl_create_token(
        kubectl_fn,
        service_account="csv-processor",
        namespace=_ETL_NAMESPACE,
    )
    client = hvac.Client(url=vault_addr_value)
    client.auth.kubernetes.login(role="csv-processor", jwt=csv_processor_jwt)
    secret = client.secrets.kv.v2.read_secret_version(mount_point="etl", path="analytics-db")
    dsn = secret["data"]["data"]["dsn"]
    parsed = urlsplit(dsn)
    assert parsed.username is not None, f"etl/analytics-db#dsn has no username: {parsed}"
    assert parsed.password is not None, f"etl/analytics-db#dsn has no password: {parsed}"
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
    kubectl_fn: Callable[..., Any],
    vault_addr_value: str,
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
        kubectl_json_fn: The `kubectl_json` fixture callable -- only used
            for `role="owner"` (reads the CNPG-generated Secret directly).
        kubectl_fn: The `kubectl` fixture callable -- only used for
            `role="etl_app"` (obtains a projected token for Vault auth).
        vault_addr_value: The `vault_addr` fixture's value -- only used for
            `role="etl_app"`.
        role: `"etl_app"` (default — matches what the real pipeline pods
            authenticate as, credential sourced from Vault since plan
            05-02) or `"owner"` (the CNPG-generated `analytics_owner` role,
            for test-harness needs broader than the pipeline's own access).

    Yields:
        An open `psycopg.Connection` to the `analytics` database.

    Raises:
        ValueError: `role` is neither `"etl_app"` nor `"owner"`.
    """
    if role == "etl_app":
        creds = _etl_app_credentials(kubectl_fn, vault_addr_value)
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
    kubectl: Callable[..., Any],  # noqa: F811 -- same reasoning as kubectl_context above
    vault_addr: str,  # noqa: F811 -- same reasoning as kubectl_context above
) -> Iterator[psycopg.Connection[Any]]:
    """A live `etl_app`-authenticated connection to the analytical cluster.

    The DEFAULT connection this suite's tests use: `etl_app` is the exact
    role the real pipeline pods authenticate as (`DATAPLAT_DB_DSN`), so
    assertions made through this connection observe exactly what the
    pipeline itself is entitled to see.
    """
    with open_analytics_connection(
        kubectl_context, kubectl_json, kubectl, vault_addr, role="etl_app"
    ) as conn:
        yield conn


@pytest.fixture
def open_etl_app_connection(
    kubectl_context: str,  # noqa: F811 -- pytest fixture-injection param name, not a real redefinition
    kubectl_json: Callable[..., Any],  # noqa: F811 -- same reasoning as kubectl_context above
    kubectl: Callable[..., Any],  # noqa: F811 -- same reasoning as kubectl_context above
    vault_addr: str,  # noqa: F811 -- same reasoning as kubectl_context above
) -> Callable[[], contextlib.AbstractContextManager[psycopg.Connection[Any]]]:
    """A zero-arg factory for a SECOND, independent `etl_app` connection.

    `open_analytics_connection` itself needs four fixture values
    (`kubectl_context`, `kubectl_json`, `kubectl`, `vault_addr`) to resolve
    Vault-backed `etl_app` credentials (plan 05-02) -- passing all four
    through a TEST function's own signature just to open one extra
    connection pushes it over this repository's `PLR0913`/`PLR0917` (max 5
    args) budget. This fixture binds them once via closure so a test needs
    only ONE parameter (this fixture) plus a `with ...() as conn:` call,
    exactly like `test_concurrent_select.py`'s own "observer" vs. "main"
    two-connection need (see `open_analytics_connection`'s own docstring).
    """

    def _factory() -> contextlib.AbstractContextManager[psycopg.Connection[Any]]:
        return open_analytics_connection(
            kubectl_context, kubectl_json, kubectl, vault_addr, role="etl_app"
        )

    return _factory


@pytest.fixture
def analytics_owner_connection(
    kubectl_context: str,  # noqa: F811 -- pytest fixture-injection param name, not a real redefinition
    kubectl_json: Callable[..., Any],  # noqa: F811 -- same reasoning as kubectl_context above
    kubectl: Callable[..., Any],  # noqa: F811 -- same reasoning as kubectl_context above
    vault_addr: str,  # noqa: F811 -- same reasoning as kubectl_context above
) -> Iterator[psycopg.Connection[Any]]:
    """A live `analytics_owner`-authenticated connection to the analytical cluster.

    For test-harness needs broader than the pipeline's own `etl_app` role
    grants (04-08-PLAN.md's Interfaces section: "as the owner where broader
    test-harness access is needed"). None of this suite's own tests
    currently require it; provided because the plan names this fixture
    explicitly as a pair with `analytics_connection`. `role="owner"` never
    touches `kubectl`/`vault_addr` internally, but `open_analytics_connection`
    requires them positionally for the `role="etl_app"` path its other
    caller (`analytics_connection`) uses.
    """
    with open_analytics_connection(
        kubectl_context, kubectl_json, kubectl, vault_addr, role="owner"
    ) as conn:
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
        `status` is one of `_TERMINAL_RUN_STATUSES` (`SUCCEEDED`/`FAILED`/
        `SKIPPED_DUPLICATE`/`SKIPPED_CONCURRENT`/`QUARANTINED`).

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


def _poll_dbt_build_running_signal(
    conn: psycopg.Connection[Any],
    run_id: int,
    *,
    timeout: float,
) -> str:
    """Poll `meta.run_stages` for D-18's `dbt_build` mid-flight signal: `stage_name='DBT_BUILD'`
    reaching `status='RUNNING'`.

    Same `deadline = time.monotonic() + timeout` / `while ... time.sleep(0.5)` loop shape as
    `test_pod_kill_retry.py`'s own `_poll_mid_load_signal`. Unlike `STAGE_LOAD`'s own
    `rows_read`-style heartbeat, `dbt_build` has no progress signal of its own — this is plan
    09-04's `mark_dbt_build_running` write (`_common/run_stage_recorder.py`), which lands
    BEFORE the `dbt_build` KPO pod itself is even launched (`stage >> mark_running >>
    dbt_build`), so a caller polling for the real pod afterward must still tolerate the pod not
    existing yet.

    Args:
        conn: An open connection to the analytical database.
        run_id: The `meta.ingestion_runs.run_id` to watch.
        timeout: Maximum seconds to wait.

    Returns:
        The last-observed status once it equals `"RUNNING"`.

    Raises:
        AssertionError: `timeout` elapses first — names the last-observed status (or "no row
            yet" if `DBT_BUILD` was never even written for this `run_id`).
    """
    deadline = time.monotonic() + timeout
    last_status: str | None = None
    while time.monotonic() < deadline:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM meta.run_stages WHERE run_id = %s AND stage_name = 'DBT_BUILD'",
                (run_id,),
            )
            row = cur.fetchone()
        if row is not None:
            last_status = row[0]
            if last_status == "RUNNING":
                return last_status
        time.sleep(_POLL_INTERVAL_SECONDS)
    msg = (
        f"meta.run_stages[run_id={run_id!r}, stage_name='DBT_BUILD'] never reached "
        f"status='RUNNING' within {timeout}s (last observed: {last_status!r})"
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
