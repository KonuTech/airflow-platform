"""tests/e2e/chaos/test_database_unavailable.py — QUAL-15 scenario 2: DB unreachable, then restored.

Uses `conftest.py`'s `cnpg_hibernation_fault` fixture — NOT a NetworkPolicy, despite
11-09-PLAN.md's original wording. See that fixture's own docstring and this repository's live
finding (`conftest.py`'s `_NETWORK_POLICY_ENFORCEMENT_FINDING`): this cluster's CNI does not
enforce NetworkPolicy at all, confirmed twice against the real cluster, including with a blanket
deny-all-egress policy. Hibernating the analytical CNPG `Cluster` (`cnpg.io/hibernation: "on"`)
achieves the same "database unavailable" effect — the analytical cluster's Service genuinely has
zero backing endpoints while hibernated, so a connection attempt gets a real, immediate
`ECONNREFUSED` (kube-proxy's own empty-endpoints handling) rather than a silent timeout.

`discover` (`dataplat.discovery.discover_files`) is the FIRST task in either ingestion DAG's
graph to touch the analytical database — `wait_for_files`/`matched_keys`/`gate` touch only
MinIO/S3 — so this fault is expected to surface as a `discover` task failure specifically, not
upstream of it.

Live-discovered bug (this session) this module's own design had to work around: the
`analytics_connection` fixture opens ITS OWN long-lived `psycopg` connection to the very same
analytical CNPG cluster this test hibernates. CNPG's hibernation forcibly terminates every open
server-side connection when it scales instances to zero, so `analytics_connection`'s own
connection object dies with a real `psycopg.errors.AdminShutdown` the moment the fault engages —
observed live, not hypothesized. A connection object psycopg has already marked broken cannot be
revived after the server comes back; this test therefore never touches `analytics_connection`
once `cnpg_hibernation_fault`'s `with` block is entered, and opens a genuinely FRESH connection
(`tests.e2e.slice.conftest.open_analytics_connection`, exposed there as a plain function
specifically for a test needing a second, independent connection) for everything that happens
after the fault clears.

Targets `csv_ingest_orders`/`orders`, NOT `csv_ingest_customers`/`customers` — see
`test_pod_crash.py`'s own module docstring for the full live-cluster reasoning (a pre-existing,
unrelated `csv_ingest_customers` scheduling backlog at test-authoring time, `csv_ingest_orders`
genuinely idle at the same moment, manually triggered exactly as
`tests/e2e/slice/test_referential_orphan.py` already does for this same DAG). This test manually
triggers `csv_ingest_orders` itself (rather than relying on its own `schedule=[customers_asset]`
coupling) for the identical reason that file already established: a manual trigger is accepted
for an asset-scheduled DAG exactly like any other DAG, and this test's own timing must not depend
on a different DAG's publish landing first.
"""

from __future__ import annotations

import contextlib
import random
import shutil
import subprocess
import time
import uuid
from typing import TYPE_CHECKING, Any

import pytest

from tests.e2e.slice.conftest import (
    open_analytics_connection,
    poll_file_discovered,
    poll_ingestion_run,
    poll_run_for_file,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    import psycopg

pytestmark = [pytest.mark.cluster, pytest.mark.chaos]

_ORDERS_DAG_ID = "csv_ingest_orders"
_ORDERS_DATASET = "orders"
_ANALYTICS_NAMESPACE = "data"
_ANALYTICS_CLUSTER = "analytics-db"

# `discover` only launches after `gate` (`integrity_gate.expand(key=matched_keys)`) has
# processed EVERY key `list_matched_keys` finds under `raw/orders/*.csv` -- live-observed this
# session (`test_pod_crash.py`'s own comment has the full measurement, TWICE: ~100s once,
# ~350s+ once, on the SAME ~20-object backlog -- this cluster's own tight, shared CPU budget
# makes per-batch latency genuinely variable): the shared cluster's `raw` bucket carries dozens
# of accumulated `orders/*.csv` objects from earlier E2E sessions, and `gate` re-checks EVERY
# currently-matching key's raw object properties on EVERY trigger (LOAD-10's checks are
# object-level, not discovery-status-aware, so this cost does not shrink even once `discover`
# itself later skips an already-`SUCCEEDED` file) -- processed 3-at-a-time
# (`integrity_gate.override(max_active_tis_per_dag=3)`). 480s budgets well above the slower
# observed run.
_DISCOVER_LABEL_SELECTOR = "dag_id=csv_ingest_orders,task_id=discover"
_POD_APPEAR_TIMEOUT_SECONDS = 480
_POD_APPEAR_POLL_INTERVAL_SECONDS = 0.2

# Disjoint from test_pod_crash.py's own [2_000_000_000, 2_100_000_000) window and from
# test_referential_orphan.py's [1_000_000_000, 1_999_000_000) window -- see either module's own
# comment for why this matters on a shared, concurrently-active cluster.
_ORDER_ID_LOW = 2_200_000_000
_ORDER_ID_HIGH = 2_300_000_000
_ROW_COUNT = 20

# The hibernated database refuses a connection near-instantly (kube-proxy's own
# empty-endpoints handling — live-measured ~1ms, see conftest.py's `cnpg_hibernation_fault`
# docstring), so the pod's own container should exit (and, per `kpo.py`'s unconditional
# `on_finish_action=delete_pod`, be deleted) within well under a minute of appearing. Generous
# margin for pod scheduling + image-pull-check overhead on this cluster's tight CPU budget.
_POD_LOG_CAPTURE_TIMEOUT_SECONDS = 90

# `discover`'s retry_delay is 5 minutes with retry_exponential_backoff=2.0 (live-read from this
# cluster's own SerializedDagModel, 2026-08-22) — Airflow will not attempt its own automatic
# retry until that backoff elapses. Live-observed this session (unrelated to this test's own
# fault): `dbt_build`'s own KubernetesJobWatcher request-timeout race (the SAME documented,
# accepted flakiness `csv_ingest_customers.py`'s own code comments describe) hit BOTH of its own
# allowed retries in a single run (three real attempts, each gap a 5-10min `retry_delay` wait,
# ~20-25min total), pushing a 1800s budget to time out by mere moments while `publish` was
# already running — `mark_dbt_build_done`'s `trigger_rule="all_done"` always lets `publish`
# proceed once dbt_build reaches ANY terminal state, so this is purely a wall-clock budget
# question (see `test_pod_crash.py`'s own `_RUN_TERMINAL_TIMEOUT_SECONDS` comment for the full
# live evidence). 3600s gives the full exhaustion sequence generous headroom.
_RECOVERY_TIMEOUT_SECONDS = 3600

# A handful of case-insensitive substrings that cover both a raw psycopg/libpq
# ECONNREFUSED-derived message and a dataplat-wrapped DataPlatformError — deliberately an
# "any of these" check rather than one exact string, since the exact wording is not
# contract-guaranteed by either psycopg or this codebase's own exception formatting.
_CONNECTION_FAILURE_SIGNATURES = (
    "could not connect",
    "connection refused",
    "connect() failed",
    "operationalerror",
    "connection failed",
)


def _existing_customer_ids(conn: psycopg.Connection[Any], *, count: int) -> list[int]:
    """Return `count` genuinely-present `normalized.customers.customer_id` values.

    Identical shape to `test_referential_orphan.py`'s own helper of the same name.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id FROM normalized.customers LIMIT %s", (count,))
        rows = cur.fetchall()
    return [int(row[0]) for row in rows]


def _build_orders_csv(*, order_ids: list[int], customer_ids: list[int]) -> bytes:
    """Build a minimal `orders.yaml`-shaped CSV: header + one valid row per (order_id, customer_id).

    Identical shape to `test_pod_crash.py`'s own helper of the same name.
    """
    lines = ["order_id,customer_id,order_date,amount"]
    lines.extend(
        f"{order_id},{customer_id},2026-01-15,199.99"
        for order_id, customer_id in zip(order_ids, customer_ids, strict=True)
    )
    return ("\n".join(lines) + "\n").encode("utf-8")


def _poll_discover_pod_name(
    kubectl_fn: Callable[..., subprocess.CompletedProcess[str]],
    *,
    timeout: float,
) -> str:
    """Poll for the real `discover` pod's name via Airflow's own dag_id/task_id pod labels.

    Same shape as `test_pod_crash.py`'s own helper of the same name.

    Args:
        kubectl_fn: The `kubectl` fixture callable.
        timeout: Maximum seconds to wait.

    Returns:
        The real pod's name.

    Raises:
        AssertionError: `timeout` elapses with no matching pod found.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        proc = kubectl_fn(
            "-n",
            "etl",
            "get",
            "pods",
            "-l",
            _DISCOVER_LABEL_SELECTOR,
            "-o",
            "jsonpath={.items[0].metadata.name}",
        )
        if proc.returncode == 0 and proc.stdout.strip():
            return proc.stdout.strip()
        time.sleep(_POD_APPEAR_POLL_INTERVAL_SECONDS)
    msg = (
        f"no pod matching -l {_DISCOVER_LABEL_SELECTOR} appeared in namespace etl within {timeout}s"
    )
    raise AssertionError(msg)


def _capture_pod_log_until_exit(
    kubectl_context: str,
    *,
    namespace: str,
    pod_name: str,
    timeout: float,
) -> str:
    """Stream a pod's logs via `kubectl logs -f` until the stream closes or `timeout` elapses.

    Started as soon as the pod is known to exist so the connection-failure error the failing
    container writes to stderr just before exiting is captured before `kpo.py`'s unconditional
    `on_finish_action=delete_pod` removes the pod object entirely — there is no "read logs after
    completion" path on this cluster (no remote logging is configured; a deleted pod's logs are
    gone, matching `.planning/debug/resolved/wait-for-files-stuck-task.md`'s own finding that
    task-level stdout is unrecoverable after pod deletion for this executor config).

    Args:
        kubectl_context: The kubectl context to run against.
        namespace: The pod's namespace.
        pod_name: The pod's name.
        timeout: Maximum seconds to wait for the stream to close on its own before killing it.

    Returns:
        Everything written to the pod's stdout/stderr while the stream was open (possibly
        empty, if the pod was deleted before this function's own `kubectl logs -f` could attach
        — the caller must treat that as informative, not fatal, and fall back to a structural
        DB-state check).
    """
    kubectl_bin = shutil.which("kubectl")
    assert kubectl_bin, "kubectl not found on PATH"
    proc = subprocess.Popen(  # noqa: S603
        [kubectl_bin, "--context", kubectl_context, "-n", namespace, "logs", "-f", pod_name],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        stdout, _ = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        stdout, _ = proc.communicate()
    return stdout or ""


def test_database_unavailable_fails_discover_clearly_then_recovers(  # noqa: PLR0913, PLR0917 -- one keyword per genuinely distinct fixture dependency; see test_pod_kill_retry.py's own _write_u3_spike_doc precedent for the same carve-out reasoning
    s3_client: Callable[[str], Any],
    analytics_connection: psycopg.Connection[Any],
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    kubectl_context: str,
    kubectl_json: Callable[..., Any],
    vault_addr: str,
    cnpg_hibernation_fault: Callable[..., contextlib.AbstractContextManager[None]],
) -> None:
    """Hibernating the analytical CNPG cluster fails `discover` clearly; un-hibernating recovers it.

    Confirms, in order: (1) with the database hibernated, a fresh file's `discover` pod fails
    with a connection-failure signature captured live from its own logs (not a silent hang —
    the pod reaches a terminal state and is cleaned up well within
    `_POD_LOG_CAPTURE_TIMEOUT_SECONDS`); (2) once the database is un-hibernated
    (`cnpg_hibernation_fault`'s own guaranteed-restoration exit), a FRESH connection (this
    module's own docstring explains why `analytics_connection` cannot be reused past this point)
    confirms no `meta.files` row was ever created for the file during the outage, then the SAME
    file's run recovers via Airflow's own automatic retry (no second upload, no manual
    intervention) and reaches `SUCCEEDED`; (3) `normalized.orders` ends up with exactly this
    run's own row count — no duplicate, no missing row.
    """
    app = s3_client("app")
    admin = s3_client("admin")

    customer_ids = _existing_customer_ids(analytics_connection, count=_ROW_COUNT)
    assert len(customer_ids) == _ROW_COUNT, (
        f"normalized.customers has fewer than {_ROW_COUNT} rows on this live cluster -- this "
        f"test needs prior customers ingestion to have already happened"
    )
    order_id_base = random.SystemRandom().randint(_ORDER_ID_LOW, _ORDER_ID_HIGH)
    order_ids = [order_id_base + i for i in range(_ROW_COUNT)]
    payload = _build_orders_csv(order_ids=order_ids, customer_ids=customer_ids)

    marker = uuid.uuid4().hex[:12]
    key = f"orders/e2e-chaos-dbunavailable-{marker}.csv"
    object_uri = f"s3://raw/{key}"
    run_id_marker = f"e2e-chaos-dbunavailable-{marker}"

    try:
        unpause = kubectl(
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

        with cnpg_hibernation_fault(namespace=_ANALYTICS_NAMESPACE, cluster=_ANALYTICS_CLUSTER):
            app.put_object(Bucket="raw", Key=key, Body=payload)

            trigger = kubectl(
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
                run_id_marker,
            )
            assert trigger.returncode == 0, f"airflow dags trigger failed:\n{trigger.stderr}"

            pod_name = _poll_discover_pod_name(kubectl, timeout=_POD_APPEAR_TIMEOUT_SECONDS)
            captured_log = _capture_pod_log_until_exit(
                kubectl_context,
                namespace="etl",
                pod_name=pod_name,
                timeout=_POD_LOG_CAPTURE_TIMEOUT_SECONDS,
            )
            lowered = captured_log.lower()
            assert any(signature in lowered for signature in _CONNECTION_FAILURE_SIGNATURES), (
                f"discover pod {pod_name!r}'s captured log did not contain any of "
                f"{_CONNECTION_FAILURE_SIGNATURES!r} while the analytical database was "
                f"hibernated -- expected a clear, attributable connection-failure signature, "
                f"not a silent/ambiguous failure. Captured log:\n{captured_log}"
            )
            # NO analytics_connection use beyond this point in the `with` block: hibernation's
            # own instance termination kills that connection's server side with a real
            # `psycopg.errors.AdminShutdown` (live-observed this session) -- see module docstring.
        # `cnpg_hibernation_fault`'s own `finally` has now removed the annotation and blocked
        # until the analytical cluster's pod is Ready again — the database is genuinely back.

        with open_analytics_connection(
            kubectl_context, kubectl_json, kubectl, vault_addr, role="etl_app"
        ) as fresh_conn:
            with fresh_conn.cursor() as cur:
                cur.execute(
                    "SELECT f.file_id FROM meta.files f "
                    "JOIN meta.datasets d ON d.dataset_id = f.dataset_id "
                    "WHERE d.dataset_name = %s AND f.object_uri = %s",
                    (_ORDERS_DATASET, object_uri),
                )
                premature_row = cur.fetchone()
            assert premature_row is None, (
                f"meta.files already has a row for {object_uri!r} even though discover never "
                f"succeeded while the database was hibernated"
            )

            file_row = poll_file_discovered(
                fresh_conn,
                dataset=_ORDERS_DATASET,
                object_uri=object_uri,
                timeout=_RECOVERY_TIMEOUT_SECONDS,
            )
            assert file_row["duplicate_of_file_id"] is None, (
                f"the freshly-marked fixture was already flagged a duplicate of file_id="
                f"{file_row['duplicate_of_file_id']!r} -- the uuid marker did not make this "
                f"content genuinely new"
            )

            run_row = poll_run_for_file(fresh_conn, file_id=file_row["file_id"], timeout=60)

            outcome = poll_ingestion_run(
                fresh_conn,
                run_row["idempotency_key"],
                timeout=_RECOVERY_TIMEOUT_SECONDS,
            )
            assert outcome["status"] == "SUCCEEDED", (
                f"after the analytical database recovered from hibernation, run "
                f"{run_row['idempotency_key']!r} finished {outcome['status']!r}, not SUCCEEDED"
            )

            with fresh_conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM normalized.orders WHERE order_id BETWEEN %s AND %s",
                    (order_ids[0], order_ids[-1]),
                )
                row = cur.fetchone()
                assert row is not None
                total = row[0]
            assert total == _ROW_COUNT, (
                f"expected exactly {_ROW_COUNT} rows in this run's own order_id window "
                f"[{order_ids[0]}, {order_ids[-1]}], found {total} -- a value below means rows "
                f"went missing after the database-unavailable recovery; a value above means "
                f"duplication"
            )
    finally:
        with contextlib.suppress(Exception):
            admin.delete_object(Bucket="raw", Key=key)
