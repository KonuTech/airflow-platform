#!/usr/bin/env python3
r"""scripts/rebuild-from-raw.py -- D-28..D-34: the whole-warehouse disaster-recovery rebuild.

`make rebuild-from-raw` runs this. D-32's single reusable implementation both a real operator
(after an actual disaster) and CI (this phase's own capstone E2E test,
`tests/e2e/slice/test_rebuild_from_raw.py`) invoke identically -- one command, two callers.

Sequencing, in this literal order:

  1. `DROP SCHEMA ... CASCADE` against the analytical PostgreSQL for exactly the four ETL-owned
     schemas (D-28) -- `staging`, `silver`, `normalized`, `meta` -- via the SAME discovered
     superuser credential `make migrate-analytics`'s own shell recipe uses (T-11-31). Never any
     other database, never the whole instance: the Airflow metadata database lives on a
     physically separate CNPG cluster (INFRA-04) this script never even resolves a connection
     string for.
  2. `alembic upgrade head` against the now-empty database, over its OWN fresh port-forward --
     mirroring `make migrate-analytics`'s own shell recipe, which always opens a fresh tunnel for
     its one `alembic upgrade head` call rather than reusing one a prior psycopg connection held
     open. Reusing a single port-forward across the step-1 psycopg connection and this step's
     separate `alembic` subprocess was tried and failed reliably (connection refused immediately
     after `DROP SCHEMA`, 2/2 live runs) -- a fresh tunnel per operation is the proven-reliable
     shape in this environment, not merely a style preference. This still reuses
     `migrations/env.py`'s own `EXPECTED_DATABASE` fail-closed guard for free.
  3. Empty MinIO's `validated`/`processed`/`quarantine` buckets via boto3, using the ADMIN
     credential (T-11-32) -- the `etl-app` IAM policy (`helm/values/*/minio.yaml`) grants this
     workload identity no delete/list access to `processed`/`quarantine` at all, and no delete
     access to `validated` either, so only admin can perform this step. NEVER `raw` (immutable,
     admin-credential-delete-only by design) and NEVER `metadata` (survives every rebuild by
     construction -- the correct place for an operator's own pre-drop snapshot, T-11-33).
  4. For each dataset declared under `configs/datasets/*.yaml` (D-31: the dataset list itself
     comes from versioned configuration, not a live S3 listing) with at least one file under
     `raw/<dataset>/`, trigger a real `airflow backfill create` for that dataset's DAG
     (`csv_ingest_<dataset>`). A dataset whose DAG has a non-periodic schedule (Asset-triggered,
     e.g. `csv_ingest_orders` off `customers_asset` -- `test_backfill_2year_sweep.py`'s own
     live-discovered deviation #1) is DELIBERATELY never targeted directly here: `airflow
     backfill create` raises `DagNonPeriodicScheduleException` for it, by design. This script
     detects that case via a `--dry-run` probe first and skips the direct trigger, relying on the
     dataset's own Asset-cascade off whichever dataset DOES drive it -- no bypass, no new
     tooling, matching D-11's "no DAG restructuring" precedent. A dataset with ZERO raw files at
     all is a genuine, unexpected state (should never happen for anything declared in
     `configs/datasets/`) and fails loudly rather than silently no-op-ing.

This step DOES NOT wait for the triggered backfill DagRuns to reach a terminal state --
`airflow backfill create` itself only registers the backfill request; the scheduler drains it
asynchronously. Waiting (and asserting SUCCESS) is the live proof's own job
(`tests/e2e/slice/test_rebuild_from_raw.py`, mirroring `test_backfill_2year_sweep.py`'s own
`_wait_for_new_backfill_completed`/`_wait_for_new_dag_run_terminal` polling), not this script's --
an operator who runs this by hand watches Airflow's own UI/logs afterward, the same way they
would for any other backfill they triggered manually today.

Every step here is a real, one-shot infrastructure mutation -- unlike `vault-bootstrap.py`'s
idempotent convergence steps, this script is meant to run once per disaster/rebuild, and it
fails loudly and specifically at its first checkable precondition (Task 1's own acceptance
criteria) rather than silently no-op-ing.

T-11-34 (accepted, not mitigated): no interactive confirmation gate exists here, matching this
project's own `vault-bootstrap`/`cluster-rebuild` precedent of trusting the operator who invokes
a Make target by its own explicit, self-documenting name.
"""

from __future__ import annotations

import base64
import contextlib
import math
import os
import re
import shlex
import shutil
import socket
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import boto3
import psycopg
from botocore.config import Config

from dataplat.config.loader import load_config

if TYPE_CHECKING:
    from collections.abc import Iterator

    from dataplat.config.model import DatasetConfig

_REPO_ROOT = Path(__file__).resolve().parent.parent
_VERSIONS_ENV = _REPO_ROOT / "helm" / "versions.env"
_ALEMBIC_INI = _REPO_ROOT / "migrations" / "alembic.ini"
_MINIO_CREDENTIALS_SCRIPT = _REPO_ROOT / "scripts" / "minio-credentials.sh"
_CONFIGS_DEFAULTS = _REPO_ROOT / "configs" / "defaults.yaml"
_CONFIGS_DATASETS_DIR = _REPO_ROOT / "configs" / "datasets"

_DATA_NAMESPACE = "data"
_ANALYTICS_CLUSTER = "analytics-db"
_ANALYTICS_DATABASE = "analytics"
_ANALYTICS_SUPERUSER_SECRET = "analytics-db-superuser"  # noqa: S105 -- a K8s Secret's metadata.name

# D-28: the ETL-owned schemas, and ONLY these -- never the whole instance, never the Airflow
# metadata database. Every one of these four is `CREATE SCHEMA`'d exactly once across this
# repo's whole migration history (grep-verified: migrations 0001/0005/0007/0021 respectively;
# no others exist).
_ETL_SCHEMAS: tuple[str, ...] = ("staging", "silver", "normalized", "meta")

# D-33: the MinIO layers this rebuild wipes -- and ONLY these. NEVER "raw" (immutable, T-11-32)
# and NEVER "metadata" (survives every rebuild by construction, T-11-33).
_WIPE_BUCKETS: tuple[str, ...] = ("validated", "processed", "quarantine")
_RAW_BUCKET = "raw"

_AIRFLOW_NAMESPACE = "airflow"
_AIRFLOW_API_SERVER_DEPLOYMENT = "deploy/airflow-api-server"

# Bounded so an unexpectedly large raw prefix can never enumerate an unbounded number of ticks
# (this project's own documented Pitfall: "more DagRuns does not mean more throughput here",
# test_backfill_2year_sweep.py's module docstring) -- any file this window does not drain gets
# picked up by the dataset's own live, already-unpaused per-minute schedule immediately
# afterward, since this script never pauses it.
_BACKFILL_MAX_TICKS = 20

_BACKFILL_CREATE_MAX_ATTEMPTS = 3
_BACKFILL_CREATE_RETRY_BACKOFF_SECONDS = 5.0


def _versions_env_variable(name: str) -> str:
    """Read a `KEY=value` line from `helm/versions.env` (the single source, plan 02-01)."""
    text = _VERSIONS_ENV.read_text(encoding="utf-8")
    for line in text.splitlines():
        if line.startswith(f"{name}="):
            return line.split("=", 1)[1].strip()
    msg = f"helm/versions.env does not define {name}"
    raise RuntimeError(msg)


def _kubectl_context() -> str:
    """Return the kubectl context kind registers for this cluster: `kind-<name>`."""
    return f"kind-{_versions_env_variable('CLUSTER_NAME')}"


def _require_kubectl() -> str:
    """Resolve the absolute path to the `kubectl` binary on `PATH`.

    Returns:
        The absolute path to `kubectl`.

    Raises:
        RuntimeError: `kubectl` is not found on `PATH`.
    """
    kubectl_bin = shutil.which("kubectl")
    if kubectl_bin is None:
        msg = "kubectl not found on PATH"
        raise RuntimeError(msg)
    return kubectl_bin


def _probe_live_cluster(kubectl_context: str) -> None:
    """Fail loudly, with a specific message, when no live cluster answers `kubectl get nodes`.

    Same reachability-probe shape `image-csv-processor`'s own Makefile recipe uses -- but this
    caller FAILS rather than warns-and-continues: the single most destructive operation in this
    whole platform must never proceed against an ambiguous or unreachable cluster.

    Args:
        kubectl_context: The kubectl context to probe.

    Raises:
        RuntimeError: `kubectl` is missing, or `kubectl get nodes` does not succeed within 5s.
    """
    kubectl_bin = _require_kubectl()
    proc = subprocess.run(  # noqa: S603
        [
            kubectl_bin,
            "--context",
            kubectl_context,
            "--request-timeout=5s",
            "get",
            "nodes",
            "-o",
            "name",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    if proc.returncode != 0:
        msg = (
            f"no live cluster reachable at context {kubectl_context!r} "
            f"(kubectl exited {proc.returncode}) -- run `make cluster-up` first:\n{proc.stderr}"
        )
        raise RuntimeError(msg)


def _free_local_port() -> int:
    """Ask the OS for an unused TCP port, then release it immediately."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _kubectl_get_secret_field(kubectl_context: str, *, namespace: str, name: str, key: str) -> str:
    """Read one base64-decoded field from a live Kubernetes Secret.

    Same mechanism `scripts/vault-bootstrap.py`'s own `_kubectl_get_secret_field` uses --
    reimplemented here (this project's own "small helpers are copied per script/test tier, not
    shared through a library module" convention) rather than imported. Never prints the decoded
    value.

    Args:
        kubectl_context: The kubectl context to read through.
        namespace: The Secret's namespace.
        name: The Secret's name.
        key: The Secret's `.data` key to read.

    Returns:
        The decoded field value.
    """
    kubectl_bin = _require_kubectl()
    proc = subprocess.run(  # noqa: S603
        [
            kubectl_bin,
            "--context",
            kubectl_context,
            "-n",
            namespace,
            "get",
            "secret",
            name,
            "-o",
            f"jsonpath={{.data.{key}}}",
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=15,
    )
    return base64.b64decode(proc.stdout).decode("utf-8")


@contextlib.contextmanager
def _port_forwarded_analytics_superuser(kubectl_context: str) -> Iterator[int]:
    """Port-forward `analytics-db-rw` to a free local port, torn down on exit.

    Same shape as `tests/e2e/slice/conftest.py`'s `_port_forwarded_analytics` and
    `make migrate-analytics`'s own shell recipe -- copied, not imported (this project's
    established per-tier-copy convention for small connection helpers).

    Args:
        kubectl_context: The kubectl context to port-forward through.

    Yields:
        The local port `analytics-db-rw:5432` was forwarded to.

    Raises:
        RuntimeError: The port-forward process exits before accepting a connection, or never
            accepts one within 30s.
    """
    kubectl_bin = _require_kubectl()
    local_port = _free_local_port()
    proc = subprocess.Popen(  # noqa: S603
        [
            kubectl_bin,
            "--context",
            kubectl_context,
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
                raise RuntimeError(msg)
            with (
                contextlib.suppress(OSError),
                socket.create_connection(("127.0.0.1", local_port), timeout=1),
            ):
                connected = True
                break
            time.sleep(0.5)
        if not connected:
            msg = (
                f"kubectl port-forward for {_ANALYTICS_CLUSTER}-rw never accepted a "
                f"connection within 30s"
            )
            raise RuntimeError(msg)
        yield local_port
    finally:
        proc.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=10)


def _superuser_dsn(kubectl_context: str, *, local_port: int) -> str:
    """Build the analytics superuser's `postgresql://` DSN over an already-open port-forward.

    Args:
        kubectl_context: The kubectl context to read the superuser Secret through.
        local_port: The local port `_port_forwarded_analytics_superuser` forwarded.

    Returns:
        A `postgresql://user:password@127.0.0.1:<local_port>/analytics` DSN. Never printed or
        logged by any caller.
    """
    user = _kubectl_get_secret_field(
        kubectl_context, namespace=_DATA_NAMESPACE, name=_ANALYTICS_SUPERUSER_SECRET, key="username"
    )
    password = _kubectl_get_secret_field(
        kubectl_context, namespace=_DATA_NAMESPACE, name=_ANALYTICS_SUPERUSER_SECRET, key="password"
    )
    encoded_password = quote(password, safe="")
    return f"postgresql://{user}:{encoded_password}@127.0.0.1:{local_port}/{_ANALYTICS_DATABASE}"


def _drop_etl_schemas(dsn: str) -> None:
    """D-28/T-11-31: `DROP SCHEMA ... CASCADE` for exactly `_ETL_SCHEMAS`, nothing else.

    Args:
        dsn: The analytics superuser DSN, over an already-open port-forward.
    """
    schema_list = ", ".join(_ETL_SCHEMAS)
    print(f"==> DROP SCHEMA IF EXISTS {schema_list} CASCADE")
    with psycopg.connect(dsn, autocommit=True) as conn:
        # module-level literal tuple, never row content or caller input (T-09-03 discipline).
        conn.execute(f"DROP SCHEMA IF EXISTS {schema_list} CASCADE")


def _run_alembic_upgrade(dsn: str) -> None:
    """Run `alembic upgrade head` against `dsn`'s now-empty database.

    Shells out to the SAME venv's `alembic` (`sys.executable -m alembic`, so this always
    resolves the exact interpreter `uv run` selected) -- reuses `migrations/env.py`'s own
    `EXPECTED_DATABASE` fail-closed guard for free, matching `make migrate-analytics`'s own
    `ALEMBIC_DSN` environment-variable contract exactly.

    Args:
        dsn: The analytics superuser DSN, over a FRESH port-forward opened by this call's own
            caller specifically for this step (never the same tunnel `_drop_etl_schemas` used --
            see `main()`'s own comment for why reusing one tunnel across a psycopg connection and
            this subprocess failed reliably in this environment).

    Raises:
        RuntimeError: The `alembic upgrade head` subprocess exits non-zero.
    """
    print("==> alembic upgrade head")
    proc = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "alembic", "-c", str(_ALEMBIC_INI), "upgrade", "head"],
        cwd=_REPO_ROOT,
        env={**os.environ, "ALEMBIC_DSN": dsn},
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if proc.returncode != 0:
        msg = f"alembic upgrade head failed (exit {proc.returncode}):\n{proc.stdout}\n{proc.stderr}"
        raise RuntimeError(msg)
    print(proc.stdout)


def _read_minio_admin_credentials(kubectl_context: str) -> dict[str, str]:
    """Run `scripts/minio-credentials.sh show` and parse its `MINIO_ROOT_*` export lines.

    Admin credentials are required for the MinIO wipe step (T-11-32): the `etl-app` IAM policy
    (`helm/values/*/minio.yaml`) grants no delete access to `validated` and no access at all to
    `processed`/`quarantine`.

    Args:
        kubectl_context: The kubectl context `minio-credentials.sh` resolves Secrets through.

    Returns:
        `{"MINIO_ROOT_USER": ..., "MINIO_ROOT_PASSWORD": ..., ...}`.
    """
    proc = subprocess.run(  # noqa: S603
        [str(_MINIO_CREDENTIALS_SCRIPT), "show"],
        env={**os.environ, "KUBECTL_CONTEXT": kubectl_context},
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    values: dict[str, str] = {}
    for line in proc.stdout.splitlines():
        match = re.match(r"^export ([A-Z_]+)=(.*)$", line)
        if not match:
            continue
        key, raw_value = match.groups()
        parts = shlex.split(raw_value)
        values[key] = parts[0] if parts else ""
    return values


def _minio_admin_client(kubectl_context: str) -> Any:
    """Build a live boto3 S3 client authenticated as MinIO's ADMIN (root) credential.

    Args:
        kubectl_context: The kubectl context to read the root Secret through.

    Returns:
        A configured `boto3` S3 client.
    """
    creds = _read_minio_admin_credentials(kubectl_context)
    endpoint_url = os.environ.get("S3_ENDPOINT_URL", "http://minio.localtest.me")
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=creds["MINIO_ROOT_USER"],
        aws_secret_access_key=creds["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def _wipe_bucket(client: Any, bucket: str) -> int:
    """D-33: delete every object in `bucket`, paginated, in batches of up to 1000.

    Args:
        client: An admin-credentialed boto3 S3 client (`_minio_admin_client`).
        bucket: The bucket to empty -- always one of `_WIPE_BUCKETS`, never `raw`/`metadata`
            (Task 1's own acceptance criteria greps this call site for exactly that).

    Returns:
        The number of objects deleted.
    """
    paginator = client.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=bucket):
        keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if not keys:
            continue
        client.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
        deleted += len(keys)
    return deleted


def _wipe_minio_layers(kubectl_context: str) -> None:
    """D-33/T-11-32: empty `validated`/`processed`/`quarantine` -- and ONLY these -- via admin."""
    client = _minio_admin_client(kubectl_context)
    for bucket in _WIPE_BUCKETS:
        count = _wipe_bucket(client, bucket)
        print(f"==> wiped {count} object(s) from s3://{bucket}/")


def _load_dataset_configs() -> list[DatasetConfig]:
    """D-31: the dataset list itself is versioned configuration -- `configs/datasets/*.yaml`.

    Never a live S3 prefix listing: D-28's "rebuilt from raw plus versioned configuration alone"
    means the set of datasets this rebuild reprocesses is exactly what this repo's own committed
    config declares, sorted for determinism.

    Returns:
        Every `configs/datasets/*.yaml` dataset, validated, sorted by filename.
    """
    return [
        load_config(path, defaults_path=_CONFIGS_DEFAULTS)
        for path in sorted(_CONFIGS_DATASETS_DIR.glob("*.yaml"))
    ]


def _count_raw_objects(client: Any, dataset: str) -> int:
    """Count objects under `raw/<dataset>/`, paginated.

    Args:
        client: A boto3 S3 client (admin credential -- `raw` also needs `s3:ListBucket`, which
            the `etl-app` policy already grants, but reusing the one admin client this script
            already built avoids a second, differently-scoped client for no benefit).
        dataset: The dataset name.

    Returns:
        The number of objects under `raw/<dataset>/`.
    """
    paginator = client.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=_RAW_BUCKET, Prefix=f"{dataset}/"):
        count += len(page.get("Contents", []))
    return count


def _kubectl_airflow(kubectl_context: str, *args: str) -> subprocess.CompletedProcess[str]:
    """Run `kubectl exec ... airflow <args>` against the live Airflow API-server pod.

    Same shape as `tests/e2e/slice/test_backfill_2year_sweep.py`'s own `_kubectl_airflow`.

    Args:
        kubectl_context: The kubectl context to exec through.
        *args: The `airflow` CLI subcommand and its own arguments.

    Returns:
        The raw `subprocess.CompletedProcess` (never `check=True` -- a caller decides whether a
        non-zero exit is itself the assertion under test).
    """
    kubectl_bin = _require_kubectl()
    return subprocess.run(  # noqa: S603
        [
            kubectl_bin,
            "--context",
            kubectl_context,
            "-n",
            _AIRFLOW_NAMESPACE,
            "exec",
            _AIRFLOW_API_SERVER_DEPLOYMENT,
            "--",
            "airflow",
            *args,
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )


def _minute_aligned_utc_now() -> datetime:
    """Return `datetime.now(UTC)`, truncated to the current minute."""
    return datetime.now(UTC).replace(second=0, microsecond=0)


def _resolve_backfill_window(*, file_count: int, max_units_per_run: int) -> tuple[str, str]:
    """Resolve a minute-aligned `[from_date, to_date]` backfill window sized to `file_count`.

    D-31's interface note asks this script to resolve business dates via a dataset's own
    filename-mask config when one is declared (`csv_processor.detect.filename.parse_filename`,
    never a second date-extraction path). Neither live dataset (`customers`/`orders`,
    `configs/datasets/*.yaml`) declares one today (`filename:` is absent from both files) -- this
    function's real, live-exercised path is therefore the fallback below, sized to file COUNT,
    not to a business-date span that does not exist for either dataset yet.

    `discover_files` (`dataplat.discovery`) is bucket-wide and date-agnostic (09-RESEARCH.md
    Pitfall 1, `test_backfill_2year_sweep.py`'s own module docstring) -- one DagRun's `discover`
    call already finds every raw file regardless of which tick triggered it, so this window only
    needs enough TICKS for enough `discover`/`stage` cycles to drain a raw prefix larger than one
    config's own `max_units_per_run` cap, never one tick per historical day. Bounded at
    `_BACKFILL_MAX_TICKS`: any file this window does not drain gets picked up by the dataset's
    own live, already-unpaused per-minute schedule immediately afterward -- this script never
    pauses it.

    Args:
        file_count: How many objects currently exist under this dataset's `raw/` prefix.
        max_units_per_run: This dataset's own `batching.max_units_per_run` (`DatasetConfig`) --
            how many units one `discover_files` call claims at most.

    Returns:
        `(from_iso, to_iso)`, both `datetime.isoformat()`-shaped, minute-aligned.
    """
    ticks_needed = max(1, math.ceil(file_count / max_units_per_run) + 1)
    ticks = min(_BACKFILL_MAX_TICKS, ticks_needed)
    end = _minute_aligned_utc_now()
    start = end - timedelta(minutes=ticks)
    return start.isoformat(), end.isoformat()


def _dry_run_supports_backfill(
    kubectl_context: str, *, dag_id: str, from_iso: str, to_iso: str
) -> bool:
    """Probe whether `dag_id` supports `airflow backfill create` at all (a periodic schedule).

    An Asset-triggered DAG (e.g. `csv_ingest_orders`, `schedule=[customers_asset]`) raises
    `DagNonPeriodicScheduleException` for ANY `airflow backfill create` invocation, dry-run or
    not -- `test_backfill_2year_sweep.py`'s own live-discovered deviation #1. This probe lets the
    real trigger step (`_trigger_backfills`) skip that dataset's DAG cleanly, relying on its own
    Asset-cascade instead, rather than treating a structurally-expected CLI failure as a bug.

    Args:
        kubectl_context: The kubectl context to exec through.
        dag_id: The DAG to probe.
        from_iso: The window's start (minute-aligned ISO datetime).
        to_iso: The window's end (minute-aligned ISO datetime).

    Returns:
        `True` if the dry-run succeeded (a periodic-schedule DAG, safe to backfill for real);
        `False` if it failed with the specific non-periodic-schedule signature.

    Raises:
        RuntimeError: The dry-run failed for any OTHER reason -- a genuine, unexpected CLI
            failure must never be silently treated as "this DAG just can't be backfilled".
    """
    result = _kubectl_airflow(
        kubectl_context,
        "backfill",
        "create",
        "--dag-id",
        dag_id,
        "--from-date",
        from_iso,
        "--to-date",
        to_iso,
        "--dry-run",
    )
    if result.returncode == 0:
        return True
    combined = f"{result.stdout}\n{result.stderr}"
    if "non-periodic schedule" in combined or "DagNonPeriodicScheduleException" in combined:
        print(f"==> {dag_id}: non-periodic (Asset-triggered) schedule -- skipping direct trigger")
        return False
    msg = (
        f"airflow backfill create --dry-run --dag-id {dag_id} failed for an unexpected reason "
        f"(exit {result.returncode}):\n{result.stdout}\n{result.stderr}"
    )
    raise RuntimeError(msg)


def _invoke_backfill_create(
    kubectl_context: str, *, dag_id: str, from_iso: str, to_iso: str
) -> None:
    """Invoke a real `airflow backfill create`, retrying a bounded number of times on CLI failure.

    Same retry shape as `test_backfill_2year_sweep.py`'s own `_invoke_backfill_create`: this
    project's own documented, recurring live-cluster transient (scheduler contention, a lost
    `SELECT ... FOR UPDATE SKIP LOCKED` race, `.planning/debug/backfill-does-not-redrive-
    rejected-row.md`) is retried rather than failing the whole rebuild on one transient blip.
    `--max-active-runs 1`: this project's own empirically-derived default (CPU starvation
    observed at higher values on this cluster's own node budget, `sweep_state`'s own hardcoded
    default in `test_backfill_2year_sweep.py`).

    Args:
        kubectl_context: The kubectl context to exec through.
        dag_id: The DAG to backfill.
        from_iso: The window's start (minute-aligned ISO datetime).
        to_iso: The window's end (minute-aligned ISO datetime).

    Raises:
        RuntimeError: Every attempt failed.
    """
    last: subprocess.CompletedProcess[str] | None = None
    for attempt in range(1, _BACKFILL_CREATE_MAX_ATTEMPTS + 1):
        last = _kubectl_airflow(
            kubectl_context,
            "backfill",
            "create",
            "--dag-id",
            dag_id,
            "--from-date",
            from_iso,
            "--to-date",
            to_iso,
            "--max-active-runs",
            "1",
            "--reprocess-behavior",
            "completed",
        )
        if last.returncode == 0:
            print(f"==> triggered backfill for {dag_id}: {from_iso} .. {to_iso}")
            return
        if attempt < _BACKFILL_CREATE_MAX_ATTEMPTS:
            time.sleep(_BACKFILL_CREATE_RETRY_BACKOFF_SECONDS)
    assert last is not None, "unreachable: the loop above always assigns `last` at least once"  # noqa: S101
    msg = (
        f"airflow backfill create --dag-id {dag_id} --from-date {from_iso} --to-date {to_iso} "
        f"failed after {_BACKFILL_CREATE_MAX_ATTEMPTS} attempts (exit {last.returncode}):\n"
        f"{last.stdout}\n{last.stderr}"
    )
    raise RuntimeError(msg)


def _trigger_backfills(kubectl_context: str) -> None:
    """D-31/D-32, step 4: trigger a real backfill for every configured dataset with raw files.

    Args:
        kubectl_context: The kubectl context to exec/list through.

    Raises:
        RuntimeError: A configured dataset has ZERO files under `raw/<dataset>/` -- an
            unexpected state this project's own "fail loud, never silently no-op" bias treats as
            a hard error, not a skip.
    """
    admin_client = _minio_admin_client(kubectl_context)
    for config in _load_dataset_configs():
        dataset = config.dataset
        file_count = _count_raw_objects(admin_client, dataset)
        if file_count == 0:
            msg = (
                f"dataset {dataset!r} (configs/datasets/{dataset}.yaml) has ZERO files under "
                f"raw/{dataset}/ -- refusing to silently skip it. If this dataset genuinely has "
                f"no history yet, remove it from configs/datasets/ or seed it before rebuilding."
            )
            raise RuntimeError(msg)

        from_iso, to_iso = _resolve_backfill_window(
            file_count=file_count, max_units_per_run=config.batching.max_units_per_run
        )
        dag_id = f"csv_ingest_{dataset}"
        supports_backfill = _dry_run_supports_backfill(
            kubectl_context, dag_id=dag_id, from_iso=from_iso, to_iso=to_iso
        )
        if not supports_backfill:
            continue
        _invoke_backfill_create(kubectl_context, dag_id=dag_id, from_iso=from_iso, to_iso=to_iso)


def main() -> int:
    """Run the whole D-28..D-33 rebuild-from-raw sequence against the live cluster.

    Returns:
        `0` on success. Never returns non-zero -- every failure mode raises instead, so the
        process exits via an uncaught exception and a non-zero exit code with a full traceback.
    """
    kubectl_context = _kubectl_context()
    _probe_live_cluster(kubectl_context)

    # Two SEPARATE, freshly-opened port-forwards -- one for the DROP SCHEMA psycopg
    # connection, one for the alembic subprocess -- not one tunnel shared across both.
    # Reusing a single tunnel across a live psycopg connection and a subsequent, separate
    # `alembic` subprocess connection failed reliably in this environment (connection refused
    # immediately after `DROP SCHEMA`, 2/2 live attempts) even though the tunnel process itself
    # was still running; `make migrate-analytics`'s own shell recipe already establishes the
    # working pattern of one fresh port-forward per operation, so this mirrors it exactly rather
    # than inventing a different fix.
    with _port_forwarded_analytics_superuser(kubectl_context) as local_port:
        dsn = _superuser_dsn(kubectl_context, local_port=local_port)
        _drop_etl_schemas(dsn)

    with _port_forwarded_analytics_superuser(kubectl_context) as local_port:
        dsn = _superuser_dsn(kubectl_context, local_port=local_port)
        _run_alembic_upgrade(dsn)

    _wipe_minio_layers(kubectl_context)
    _trigger_backfills(kubectl_context)

    print(
        "==> rebuild-from-raw: schemas dropped + migrated, MinIO layers wiped, backfills "
        "triggered. Watch Airflow's own UI/logs (or poll `backfill`/`dag_run`) for completion."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
