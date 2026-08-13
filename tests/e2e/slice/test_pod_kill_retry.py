"""tests/e2e/slice/test_pod_kill_retry.py — D-09/D-10/D-11's permanent proof, and the U3 baseline.

Honest limit: `docs/spikes/U3-throughput-baseline.md`'s peak-RSS figure is a
sampled MAXIMUM of `/sys/fs/cgroup/memory.current` polled every few seconds
during the run, not a true kernel-tracked peak. This cluster's containers
were verified (live, via `kubectl exec ... cat /sys/fs/cgroup/memory.peak`)
to NOT expose the cgroup v2 `memory.peak` file the plan's own text
preferred, and `kubectl top pod` was verified (live) to fail outright --
this cluster has no metrics-server installed. Both are documented as
genuine, live-verified environment facts, not assumptions; the sampled-max
fallback is 04-08-PLAN.md's own explicitly anticipated alternative ("OR
reads /sys/fs/cgroup/memory.peak ... taking the MAX observed value" --
adapted here to `memory.current`, the file that actually exists).

Both tests give the ~1,000,000-row fixture a FRESH, randomly-offset
`customer_id` range on every invocation (`large_csv_with_offset_customer_ids`)
-- never the fixture's literal 1..1,000,000 range. This keeps repeat runs
of this suite, runs of `test_smoke_and_idempotency.py`'s small-fixture test
(customer_id 1..120), and 04-09-PLAN.md's own concurrent demo activity from
ever contending for the same `normalized.customers` keys, which would
otherwise make both this test's exact-row-count assertion and U3's
`rows_loaded`-based throughput figure meaningless on any run after the
first (`ON CONFLICT ... WHERE _record_hash IS DISTINCT ...` correctly
suppresses a no-op republish of already-identical rows, so a reused
customer_id range would make `rows_loaded` collapse to near-zero instead of
reflecting genuine per-row publish throughput).
"""

from __future__ import annotations

import contextlib
import datetime
import random
import threading
import time
import uuid
from typing import TYPE_CHECKING, Any

import pytest

from tests.e2e.slice.conftest import (
    LARGE_FIXTURE_ROWS,
    large_csv_with_offset_customer_ids,
    poll_file_discovered,
    poll_ingestion_run,
    poll_run_for_file,
)

if TYPE_CHECKING:
    import subprocess
    from collections.abc import Callable
    from pathlib import Path

    import psycopg

pytestmark = pytest.mark.cluster

_CUSTOMERS_DATASET = "customers"

# The KPO ingest pod's configured Kubernetes memory LIMIT
# (airflow/dags/csv_ingest_customers.py's `_INGEST_RESOURCES`, read directly
# from that file at the time this test was written -- verify there if this
# ever looks stale) -- recorded in the U3 doc for context, not enforced by
# this test itself.
_INGEST_POD_MEMORY_LIMIT = "4Gi"

# Airflow's own `retries=3` on the `ingest` task (csv_ingest_customers.py)
# means a genuinely NEW pod launches after a kill: Airflow must notice the
# failed try and requeue (up to ~1 scheduler loop), a fresh pod must be
# scheduled and its image pulled (~10-60s on this cluster's local registry),
# and the retry re-stages the WHOLE file from scratch -- StagingLoader's own
# `DROP TABLE IF EXISTS` first, never a resume -- meaning a 1,000,000-row
# COPY effectively runs to completion an EXTRA time beyond the killed
# attempt's partial, discarded work. 600s budgets generously for all of
# that on a resource-constrained kind node.
_RETRY_TIMEOUT_SECONDS = 600

# Sampling interval for the U3 peak-memory poller -- frequent enough to
# catch a multi-second-to-multi-minute run's actual peak, cheap enough
# (~1 `kubectl exec` per tick) not to itself perturb the pod's own resource
# usage materially.
_MEMORY_SAMPLE_INTERVAL_SECONDS = 3.0


def _poll_mid_load_signal(
    conn: psycopg.Connection[Any],
    idempotency_key: str,
    *,
    timeout: float,
) -> dict[str, Any]:
    """Poll `meta.ingestion_runs` for D-11's mid-load signal: `rows_read > 0 AND status='RUNNING'`.

    Populated live by `run_ingest`'s heartbeat thread (`packages/dataplat/
    src/dataplat/pipeline/run.py`), sourced from `StagingLoader.load()`'s
    own `on_progress` callback after each COPY chunk -- see 04-08-PLAN.md's
    Interfaces section for the full provenance chain. `k8s_pod_name` is
    guaranteed non-NULL by the time this condition is true: the SAME
    `claim_ingestion_run` UPDATE that first sets `status='RUNNING'` also
    sets `k8s_pod_name` in that one statement (`packages/dataplat/src/
    dataplat/metadata/postgres.py`).

    Args:
        conn: An open connection to the analytical database.
        idempotency_key: The run's idempotency key.
        timeout: Maximum seconds to wait.

    Returns:
        `{"status": "RUNNING", "rows_read": ..., "k8s_pod_name": ...}`.

    Raises:
        AssertionError: `timeout` elapses first.
    """
    deadline = time.monotonic() + timeout
    last_status: str | None = None
    last_rows_read: int | None = None
    while time.monotonic() < deadline:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, rows_read, k8s_pod_name "
                "FROM meta.ingestion_runs WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            row = cur.fetchone()
        if row is not None:
            last_status, last_rows_read, pod_name = row
            if last_status == "RUNNING" and last_rows_read and last_rows_read > 0 and pod_name:
                return {
                    "status": last_status,
                    "rows_read": last_rows_read,
                    "k8s_pod_name": pod_name,
                }
        time.sleep(0.5)
    msg = (
        f"never observed rows_read > 0 AND status = 'RUNNING' for idempotency_key="
        f"{idempotency_key!r} within {timeout}s (last observed: status={last_status!r}, "
        f"rows_read={last_rows_read!r})"
    )
    raise AssertionError(msg)


def _poll_pod_name(
    conn: psycopg.Connection[Any],
    idempotency_key: str,
    *,
    timeout: float,
) -> str:
    """Poll `meta.ingestion_runs.k8s_pod_name` until the claiming pod is known.

    Unlike `_poll_mid_load_signal`, this does not wait for `rows_read > 0`
    -- U3's memory sampler wants to start observing as EARLY as possible,
    including the pod's own startup/connection-pool-warmup memory, not only
    once the first heartbeat has fired.

    Args:
        conn: An open connection to the analytical database.
        idempotency_key: The run's idempotency key.
        timeout: Maximum seconds to wait.

    Returns:
        The claiming pod's name.

    Raises:
        AssertionError: `timeout` elapses with no pod name recorded yet.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT k8s_pod_name FROM meta.ingestion_runs WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            row = cur.fetchone()
        if row is not None and row[0]:
            return str(row[0])
        time.sleep(0.5)
    msg = f"k8s_pod_name never appeared for idempotency_key={idempotency_key!r} within {timeout}s"
    raise AssertionError(msg)


def test_pod_kill_mid_load_produces_no_duplicates(
    s3_client: Callable[[str], Any],
    analytics_connection: psycopg.Connection[Any],
    analytics_owner_connection: psycopg.Connection[Any],
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    slice_fixtures_dir: Path,
) -> None:
    """D-09/D-10/D-11: a REAL `kubectl delete pod` mid-load leaves no duplicate or missing rows.

    The platform's single largest correctness risk (silent duplication
    after a genuine crash), proven against the real deployed pipeline: the
    killed pod's partial staging work is discarded (never resumed --
    `StagingLoader`'s own `DROP TABLE IF EXISTS`-first retry semantics), a
    fresh pod re-stages and re-publishes the whole file, and the final row
    count for this run's own customer_id window matches the fixture's row
    count EXACTLY -- migration 0006's `UNIQUE(customer_id)` constraint
    already makes a literal SQL-level duplicate impossible, so the
    meaningful assertion is that nothing is missing OR doubled.
    """
    app = s3_client("app")
    admin = s3_client("admin")

    offset = random.SystemRandom().randint(2_000_000, 1_000_000_000)
    payload = large_csv_with_offset_customer_ids(
        (slice_fixtures_dir / "customers_large.csv").read_bytes(),
        offset=offset,
    )
    marker = uuid.uuid4().hex[:12]
    key = f"customers/e2e-podkill-{marker}.csv"
    object_uri = f"s3://raw/{key}"

    try:
        app.put_object(Bucket="raw", Key=key, Body=payload)

        file_row = poll_file_discovered(
            analytics_connection,
            dataset=_CUSTOMERS_DATASET,
            object_uri=object_uri,
            timeout=180,
        )
        assert file_row["duplicate_of_file_id"] is None, (
            f"the freshly-offset large fixture was marked a duplicate of file_id="
            f"{file_row['duplicate_of_file_id']!r} -- the customer_id offset did not make "
            f"this content genuinely new"
        )

        run_row = poll_run_for_file(analytics_connection, file_id=file_row["file_id"], timeout=60)
        idempotency_key = run_row["idempotency_key"]

        mid_load = _poll_mid_load_signal(analytics_connection, idempotency_key, timeout=300)
        pod_name = mid_load["k8s_pod_name"]

        delete = kubectl("-n", "etl", "delete", "pod", pod_name, "--wait=false")
        assert delete.returncode == 0, (
            f"kubectl delete pod {pod_name!r} -n etl failed (exit {delete.returncode}):\n"
            f"{delete.stderr}"
        )

        outcome = poll_ingestion_run(
            analytics_connection,
            idempotency_key,
            timeout=_RETRY_TIMEOUT_SECONDS,
        )
        assert outcome["status"] == "SUCCEEDED", (
            f"after killing pod {pod_name!r} mid-load, the run finished "
            f"{outcome['status']!r}, not SUCCEEDED -- check `kubectl logs` for the retry pod"
        )

        with analytics_owner_connection.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM normalized.customers WHERE customer_id BETWEEN %s AND %s",
                (offset + 1, offset + LARGE_FIXTURE_ROWS),
            )
            row = cur.fetchone()
            assert row is not None
            total = row[0]
        assert total == LARGE_FIXTURE_ROWS, (
            f"expected exactly {LARGE_FIXTURE_ROWS} rows in this run's own customer_id "
            f"window [{offset + 1}, {offset + LARGE_FIXTURE_ROWS}], found {total} -- "
            f"a value below the fixture's row count means rows went missing after the kill; "
            f"a value above is impossible under migration 0006's UNIQUE(customer_id) but "
            f"would mean the constraint itself regressed"
        )
    finally:
        with contextlib.suppress(Exception):
            admin.delete_object(Bucket="raw", Key=key)


def _read_run_metrics(conn: psycopg.Connection[Any], idempotency_key: str) -> tuple[int, int]:
    """Read and validate a completed run's `rows_loaded`/`duration_ms`.

    Args:
        conn: An open connection to the analytical database.
        idempotency_key: The completed run's idempotency key.

    Returns:
        `(rows_loaded, duration_ms)`, both confirmed non-NULL and positive.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT rows_loaded, duration_ms FROM meta.ingestion_runs WHERE idempotency_key = %s",
            (idempotency_key,),
        )
        metrics_row = cur.fetchone()
    assert metrics_row is not None
    rows_loaded, duration_ms = metrics_row
    assert rows_loaded is not None, "expected a non-NULL rows_loaded for the U3 baseline run"
    assert rows_loaded > 0, f"expected a nonzero rows_loaded, got {rows_loaded!r}"
    assert duration_ms is not None, "expected a non-NULL duration_ms for the U3 baseline run"
    assert duration_ms > 0, f"expected a nonzero duration_ms, got {duration_ms!r}"
    return int(rows_loaded), int(duration_ms)


def test_u3_throughput_and_peak_rss_baseline(
    s3_client: Callable[[str], Any],
    analytics_connection: psycopg.Connection[Any],
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    slice_fixtures_dir: Path,
    repo_root: Path,
) -> None:
    """U3: a measured streaming-throughput and peak-RSS baseline, committed for future comparison.

    A SEPARATE, non-killed full run of the same ~1,000,000-row fixture
    shape (its own fresh customer_id offset -- module docstring) -- killing
    mid-load is not the right run to measure steady-state throughput from.
    Peak memory is the running MAXIMUM of `/sys/fs/cgroup/memory.current`
    sampled every `_MEMORY_SAMPLE_INTERVAL_SECONDS` while the run is in
    flight, from a background thread stopped (and joined) before this
    function reads its result -- module docstring documents why this is a
    sampled approximation, not `memory.peak`, on this specific cluster.
    """
    app = s3_client("app")
    admin = s3_client("admin")

    offset = random.SystemRandom().randint(2_000_000, 1_000_000_000)
    payload = large_csv_with_offset_customer_ids(
        (slice_fixtures_dir / "customers_large.csv").read_bytes(),
        offset=offset,
    )
    marker = uuid.uuid4().hex[:12]
    key = f"customers/e2e-u3-{marker}.csv"
    object_uri = f"s3://raw/{key}"

    try:
        app.put_object(Bucket="raw", Key=key, Body=payload)

        file_row = poll_file_discovered(
            analytics_connection,
            dataset=_CUSTOMERS_DATASET,
            object_uri=object_uri,
            timeout=180,
        )
        assert file_row["duplicate_of_file_id"] is None

        run_row = poll_run_for_file(analytics_connection, file_id=file_row["file_id"], timeout=60)
        idempotency_key = run_row["idempotency_key"]

        pod_name = _poll_pod_name(analytics_connection, idempotency_key, timeout=120)

        samples: list[int] = []
        stop_sampling = threading.Event()

        def _sample_loop() -> None:
            while not stop_sampling.is_set():
                proc = kubectl(
                    "-n",
                    "etl",
                    "exec",
                    pod_name,
                    "--",
                    "cat",
                    "/sys/fs/cgroup/memory.current",
                )
                if proc.returncode == 0:
                    with contextlib.suppress(ValueError):
                        samples.append(int(proc.stdout.strip()))
                stop_sampling.wait(_MEMORY_SAMPLE_INTERVAL_SECONDS)

        sampler = threading.Thread(target=_sample_loop, name="u3-memory-sampler", daemon=True)
        sampler.start()
        try:
            outcome = poll_ingestion_run(
                analytics_connection,
                idempotency_key,
                timeout=_RETRY_TIMEOUT_SECONDS,
            )
        finally:
            stop_sampling.set()
            sampler.join(timeout=10)

        assert outcome["status"] == "SUCCEEDED", (
            f"U3 baseline run finished {outcome['status']!r}, not SUCCEEDED"
        )

        rows_loaded, duration_ms = _read_run_metrics(analytics_connection, idempotency_key)
        throughput_rows_per_sec = rows_loaded / (duration_ms / 1000)
        peak_bytes = max(samples) if samples else 0

        assert throughput_rows_per_sec > 0
        assert peak_bytes > 0, (
            "no memory.current sample was ever captured -- the pod may have completed and "
            "been deleted before the first sample landed (on_finish_action=delete_succeeded_pod)"
        )

        _write_u3_spike_doc(
            repo_root,
            rows_loaded=rows_loaded,
            duration_ms=duration_ms,
            throughput_rows_per_sec=throughput_rows_per_sec,
            peak_bytes=peak_bytes,
            sample_count=len(samples),
        )
    finally:
        with contextlib.suppress(Exception):
            admin.delete_object(Bucket="raw", Key=key)


def _write_u3_spike_doc(  # noqa: PLR0913 -- six independently-named metrics; a dataclass for one call site adds nothing
    repo_root: Path,
    *,
    rows_loaded: int,
    duration_ms: int,
    throughput_rows_per_sec: float,
    peak_bytes: int,
    sample_count: int,
) -> None:
    """Regenerate `docs/spikes/U3-throughput-baseline.md` from this test's own live measurement.

    Automated from the test itself, mirroring `test_smoke_and_idempotency.
    py`'s `_write_u1_spike_doc` -- never hand-edited.
    """
    path = repo_root / "docs" / "spikes" / "U3-throughput-baseline.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    measured_at = datetime.datetime.now(tz=datetime.UTC).isoformat()
    peak_mib = peak_bytes / (1024 * 1024)
    content = f"""# U3 — streaming CSV throughput and peak-RSS baseline

**Regenerated automatically by
`tests/e2e/slice/test_pod_kill_retry.py::test_u3_throughput_and_peak_rss_baseline`
— do not hand-edit.**

- Measured at: {measured_at}
- Fixture: `tests/fixtures/slice-corpus.yaml`'s `customers_large.csv`
  ({LARGE_FIXTURE_ROWS:,} rows, ~55 MB — see that manifest's own
  `expect.approx_bytes`)
- Ingest pod configured memory limit:
  `{_INGEST_POD_MEMORY_LIMIT}` (`airflow/dags/csv_ingest_customers.py`'s
  `_INGEST_RESOURCES`)
- `rows_loaded`: {rows_loaded:,}
- `duration_ms`: {duration_ms:,}
- **Throughput: {throughput_rows_per_sec:,.0f} rows/sec**
- **Peak RSS (sampled): {peak_mib:,.1f} MiB** ({sample_count} sample(s) of
  `/sys/fs/cgroup/memory.current`, every {_MEMORY_SAMPLE_INTERVAL_SECONDS:.0f}s)

## Measurement method, and its honest limits

Throughput is `rows_loaded / (duration_ms / 1000)` read directly from the
completed run's own `meta.ingestion_runs` row — the same wall-clock the
pipeline itself reports, not a value derived independently in this test.

Peak memory is the running MAXIMUM of `/sys/fs/cgroup/memory.current`,
polled by `kubectl exec`-ing into the ingest pod on a fixed interval while
the run is in flight. This cluster's containers were verified live to NOT
expose the cgroup v2 `memory.peak` file (`cat
/sys/fs/cgroup/memory.peak` → `No such file or directory`), and `kubectl
top pod` was verified live to fail (`error: Metrics API not available` —
no metrics-server is installed on this cluster). The sampled maximum is
therefore a LOWER BOUND on the true peak, not an exact figure: a spike
between two samples (up to
{_MEMORY_SAMPLE_INTERVAL_SECONDS:.0f}s apart) would not be observed, and a
spike in the pod's final moments before it is deleted
(`on_finish_action=delete_succeeded_pod`) could be missed entirely if it
happens after the last successful sample.

## Regression policy

A future run of this same test producing a throughput figure more than 5x
worse than the number above should be treated as a bug, not a mystery
(ROADMAP.md's own words) — investigate before assuming the hardware or
cluster is merely "slower today."
"""
    path.write_text(content, encoding="utf-8")
