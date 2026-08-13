#!/usr/bin/env python3
r"""scripts/ingest-demo.py -- D-14/D-15/D-16 developer demo: upload, wait, receipt.

`make ingest-demo FILE=<path>` runs this: upload `FILE` to `s3://raw/`, then
wait for the REAL, unattended `csv_ingest_customers` pipeline (its deferred
`S3KeySensor`, 30s poke interval -> `discover` -> `ingest`) to notice and
process it, polling `meta.ingestion_runs` on the live analytical cluster
until a terminal status appears, then print a human-readable receipt.

PROHIBITION (D-15, explicit user instruction): this script must NEVER start
a DAG run through Airflow's command line -- neither its classic manual-run
subcommand nor a backfill invocation -- anywhere in this file. The whole
point of this demo is to prove the real, sensor-driven pipeline end to end
with no shortcut around the sensor -- a future edit must not "helpfully" add
one. A repository policy check enforces this mechanically by scanning this
file's own source for that manual-run subcommand's name and failing closed
if it is ever found -- so this docstring deliberately never spells that
subcommand out verbatim either.

Run location (open design point, 04-CONTEXT.md): this script is meant to run
from a developer's own host machine -- the SAME place `tests/e2e/cluster/`
runs from -- not from inside the cluster. It therefore reaches the
analytical PostgreSQL cluster the same way `tests/e2e/cluster/
test_postgres_topology.py`'s `_cluster_connection` does: a torn-down-on-exit
`kubectl port-forward` to `analytics-db-rw`, never a direct in-cluster DSN.

Identity is content-addressed (matching `dataplat.discovery`'s own D-13
duplicate-detection design): this script polls `meta.ingestion_runs` joined
through `meta.files` on the uploaded file's own sha256 content hash, not its
object key. A byte-identical re-upload under a fresh key (this script always
mints one, see `--key`'s default) is recognized by discovery as a content
duplicate of whatever file first carried that content, and no second run is
ever created for it -- so running this demo twice with the same fixture
resolves to the SAME governing run's receipt, immediately. That is not a
bug: it is `make ingest-demo` proving, to a developer's own satisfaction,
this phase's own definition of done -- "a re-run produces zero additional
rows" (ROADMAP).
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import dataclasses
import hashlib
import json
import os
import re
import shlex
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

import boto3
import psycopg
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

if TYPE_CHECKING:
    from collections.abc import Iterator

    from mypy_boto3_s3 import S3Client

_REPO_ROOT = Path(__file__).resolve().parent.parent
_VERSIONS_ENV = _REPO_ROOT / "helm" / "versions.env"
_MINIO_CREDENTIALS_SCRIPT = _REPO_ROOT / "scripts" / "minio-credentials.sh"

# This phase is deliberately single-dataset (04-CONTEXT.md: "one dataset
# (customers)"), matching `dataplat.pipeline.run`'s own hardcoded
# `_CUSTOMERS_TARGET_COLUMNS` and `configs/datasets/customers.yaml`'s
# `source.bucket: raw` / `source.path: customers/`. A future multi-dataset
# extension of this script adds a `--dataset` flag here; it does not change
# these two constants' meaning.
_RAW_BUCKET = "raw"
_DATASET_NAME = "customers"

# The CNPG namespace/cluster name every tests/e2e/cluster/ module also uses
# (test_postgres_topology.py) -- never a literal connection string anywhere
# in this file (D-14).
_NAMESPACE = "data"
_ANALYTICS_CLUSTER = "analytics-db"

# Bytes read per streaming-hash chunk, matching dataplat.discovery's own
# `_HASH_CHUNK_BYTES` -- the local file is never loaded whole into memory
# just to hash it.
_HASH_CHUNK_BYTES = 1_048_576

_POLL_INTERVAL_SECONDS = 5.0
_DEFAULT_TIMEOUT_SECONDS = 300.0

# meta.ingestion_runs.status values actually written by
# dataplat.metadata.postgres.PostgresMetadataRepository: 'PENDING' (insert),
# 'RUNNING' (claim/heartbeat), 'SUCCEEDED' (finalize_publication). 'FAILED'
# is a legitimate persisted value too (claim_ingestion_run's own WHERE
# clause reclaims from it) even though no call site in this phase's code
# currently sets it -- included here so a future plan that adds that write
# path needs no change on this side. `Receipt`'s own "SKIPPED_DUPLICATE"/
# "SKIPPED_CONCURRENT" strings are deliberately EXCLUDED: those are
# XCom/Receipt-only presentation labels `dataplat.pipeline.run._skipped_
# receipt` constructs in memory -- the underlying DB row's status is left
# untouched on that path (still 'SUCCEEDED' or 'RUNNING' respectively), so
# this script -- which reads the DB directly, never the XCom -- can never
# actually observe those two strings in `meta.ingestion_runs.status`.
_TERMINAL_STATUSES = frozenset({"SUCCEEDED", "FAILED"})

# `r.duration_ms` is never actually populated by `finalize_publication`
# (dataplat/metadata/postgres.py only sets status/finished_at/rows_loaded/
# report_uri) -- COALESCE to a value computed from started_at/finished_at
# (both of which ARE persisted) so the receipt still shows a real number.
# This is a read-only presentational fallback local to this script; it does
# not touch or paper over dataplat's own persistence gap, which is logged
# separately (see this plan's deferred-items.md entry).
_RUN_QUERY = """
    SELECT r.run_id,
           r.status,
           r.rows_loaded,
           COALESCE(
               r.duration_ms,
               (EXTRACT(EPOCH FROM (r.finished_at - r.started_at)) * 1000)::bigint
           ) AS duration_ms,
           r.report_uri
      FROM meta.ingestion_runs r
      JOIN meta.files f ON f.file_id = r.file_id
      JOIN meta.datasets d ON d.dataset_id = f.dataset_id
     WHERE d.dataset_name = %s
       AND f.content_sha256 = %s
     ORDER BY r.run_id DESC
     LIMIT 1
"""

_FILE_QUERY = """
    SELECT f.file_id, f.status, f.duplicate_of_file_id
      FROM meta.files f
      JOIN meta.datasets d ON d.dataset_id = f.dataset_id
     WHERE d.dataset_name = %s
       AND f.content_sha256 = %s
     ORDER BY f.file_id DESC
     LIMIT 1
"""


@dataclasses.dataclass(frozen=True, slots=True)
class _PollOutcome:
    """The result of polling `meta.ingestion_runs` until a terminal status, or timeout.

    Attributes:
        run_id: The terminal run's `meta.ingestion_runs.run_id`. `None` when
            polling timed out before any terminal status was observed.
        status: The terminal run's status (`"SUCCEEDED"` or `"FAILED"`).
            `None` on timeout -- callers should treat `status is None` as the
            "timed out" case.
        rows_loaded: Rows published by the run, when known.
        duration_ms: The run's wall-clock duration in milliseconds, when
            known.
        report_uri: Object-store URI of a fuller validation report, when one
            was written. `None` when no report exists (this phase never
            generates one).
        last_file_status: The most recently observed `meta.files.status` for
            this content hash, for timeout diagnostics. `None` if the file
            was never discovered at all within the timeout window.
        last_run_status: The most recently observed (non-terminal)
            `meta.ingestion_runs.status` for this content hash, for timeout
            diagnostics. `None` if no run was ever observed.
    """

    run_id: int | None
    status: str | None
    rows_loaded: int | None
    duration_ms: int | None
    report_uri: str | None
    last_file_status: str | None
    last_run_status: str | None

    @property
    def timed_out(self) -> bool:
        """Whether polling ended without ever observing a terminal status."""
        return self.status is None


def _versions_env_variable(name: str) -> str:
    """Read a `KEY=value` line from `helm/versions.env` (the single source, plan 02-01).

    Args:
        name: The variable name to look up.

    Returns:
        The variable's value, with surrounding whitespace stripped.

    Raises:
        RuntimeError: `name` is not defined in `helm/versions.env`.
    """
    text = _VERSIONS_ENV.read_text(encoding="utf-8")
    for line in text.splitlines():
        if line.startswith(f"{name}="):
            return line.split("=", 1)[1].strip()
    msg = f"helm/versions.env does not define {name}"
    raise RuntimeError(msg)


def _kubectl_context() -> str:
    """Return the kubectl context kind registers for this cluster: `kind-<name>`.

    Never the ambient current-context (which a developer's shell could have
    pointed anywhere) -- always derived from `helm/versions.env`, the same
    convention `tests/e2e/cluster/conftest.py`'s `kubectl_context` fixture
    uses.

    Returns:
        The `kind-<CLUSTER_NAME>` context string.
    """
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


def _sha256_of_file(path: Path) -> bytes:
    """Compute the raw (non-hex) sha256 digest of `path`'s bytes, in bounded chunks.

    Matches `dataplat.discovery.discover_files`'s own chunked-hash discipline
    -- the file is never loaded whole into memory just to hash it -- and the
    raw `bytes` digest this returns is exactly what `meta.files.
    content_sha256` (a `bytea` column) stores, so it can be compared directly
    against a query parameter with no hex-encoding round trip.

    Args:
        path: The local file to hash.

    Returns:
        The 32-byte sha256 digest.
    """
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(_HASH_CHUNK_BYTES)
            if not chunk:
                break
            digest.update(chunk)
    return digest.digest()


def _read_minio_credentials(kubectl_context: str) -> dict[str, str]:
    """Run `scripts/minio-credentials.sh show` and parse its export lines.

    Mirrors `tests/e2e/cluster/conftest.py`'s `_read_minio_credentials`
    exactly -- same regex/shlex parsing shape, per this plan's own
    Interfaces section. Credentials are read fresh from the in-cluster
    Secrets on every call, never cached beyond this function's return value,
    and never printed: `show` writes its own warning to stderr, which this
    function captures and discards along with the rest of stderr.

    Args:
        kubectl_context: The kubectl context to read the Secrets through.

    Returns:
        A mapping of the four `MINIO_*` environment variable names `show`
        exports to their live values.
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
        # printf '%q' shell-quotes only when a value needs it; shlex.split
        # unescapes either form (quoted or bare) uniformly.
        parts = shlex.split(raw_value)
        values[key] = parts[0] if parts else ""
    return values


def _build_s3_client(kubectl_context: str) -> S3Client:
    """Build a live boto3 S3 client from in-cluster MinIO 'app' credentials.

    Same construction shape as `tests/e2e/cluster/conftest.py`'s
    `s3_client("app")` factory and `dataplat.storage.objectstore.
    S3ObjectStore.__init__` (endpoint/region/path-style-addressing) -- never
    reimplemented against a different S3 library (04-RESEARCH.md's
    Don't-Hand-Roll table forbids `mc`).

    Args:
        kubectl_context: The kubectl context to read live credentials
            through.

    Returns:
        A configured boto3 S3 client.
    """
    creds = _read_minio_credentials(kubectl_context)
    endpoint_url = os.environ.get("S3_ENDPOINT_URL", "http://minio.localtest.me")
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=creds["MINIO_APP_ACCESS_KEY"],
        aws_secret_access_key=creds["MINIO_APP_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def _read_analytics_credentials(kubectl_context: str) -> dict[str, str]:
    """Read and base64-decode the CNPG-generated `analytics-db-app` Secret.

    Same shape as `tests/e2e/cluster/test_postgres_topology.py`'s
    `_read_app_secret` -- never writes a decoded value to disk or a log.

    Args:
        kubectl_context: The kubectl context to read the Secret through.

    Returns:
        The Secret's decoded key/value pairs (`dbname`, `user`, `password`,
        `host`, `port`, ...).
    """
    kubectl_bin = _require_kubectl()
    proc = subprocess.run(  # noqa: S603
        [
            kubectl_bin,
            "--context",
            kubectl_context,
            "-n",
            _NAMESPACE,
            "get",
            "secret",
            f"{_ANALYTICS_CLUSTER}-app",
            "-o",
            "json",
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    secret = json.loads(proc.stdout)
    return {key: base64.b64decode(value).decode("utf-8") for key, value in secret["data"].items()}


def _free_local_port() -> int:
    """Ask the OS for an unused TCP port, then release it immediately.

    Returns:
        A locally free TCP port number.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextlib.contextmanager
def _port_forwarded_analytics(kubectl_context: str) -> Iterator[int]:
    """Port-forward `analytics-db-rw` to a free local port, torn down on exit.

    Same shape as `tests/e2e/cluster/test_postgres_topology.py`'s
    `_port_forwarded_postgres` -- there is no ingress for raw PostgreSQL in
    this phase, so a `kubectl port-forward` tunnel is the only way this
    script (running on the developer's own host, outside the cluster) can
    reach the analytical cluster.

    Args:
        kubectl_context: The kubectl context to port-forward through.

    Yields:
        The local port the tunnel is listening on.

    Raises:
        RuntimeError: The port-forward process exits before ever accepting a
            connection, or never accepts one within 30s.
    """
    kubectl_bin = _require_kubectl()
    local_port = _free_local_port()
    proc = subprocess.Popen(  # noqa: S603
        [
            kubectl_bin,
            "--context",
            kubectl_context,
            "-n",
            _NAMESPACE,
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
                f"kubectl port-forward for {_ANALYTICS_CLUSTER}-rw "
                "never accepted a connection within 30s"
            )
            raise RuntimeError(msg)
        yield local_port
    finally:
        proc.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=10)


@dataclasses.dataclass
class _PollState:
    """Mutable last-observed diagnostics, threaded through `_poll_for_receipt`'s poll loop.

    A plain mutable holder (unlike `_PollOutcome`, frozen) so successive
    calls to `_poll_once` -- each over its own fresh tunnel and connection,
    see `_poll_for_receipt`'s docstring -- can accumulate the latest
    observation across iterations, for the timeout diagnostic message.

    Attributes:
        last_file_status: The most recently observed `meta.files.status`
            description for this content hash. `None` until first observed.
        last_run_status: The most recently observed (non-terminal)
            `meta.ingestion_runs.status` for this content hash. `None` until
            first observed.
    """

    last_file_status: str | None = None
    last_run_status: str | None = None


def _poll_once(
    *,
    local_port: int,
    creds: dict[str, str],
    content_sha256: bytes,
    state: _PollState,
) -> _PollOutcome | None:
    """Run one `meta.files`/`meta.ingestion_runs` check over a fresh psycopg connection.

    Args:
        local_port: The local end of the live `kubectl port-forward` tunnel
            to `analytics-db-rw`.
        creds: The decoded `analytics-db-app` Secret (`dbname`/`user`/
            `password`).
        content_sha256: The uploaded file's raw sha256 digest -- the join
            key.
        state: Last-observed diagnostics, updated in place with whatever
            this check newly observes.

    Returns:
        A terminal `_PollOutcome` when `meta.ingestion_runs.status` has
        reached `_TERMINAL_STATUSES`; otherwise `None` (poll again).

    Raises:
        psycopg.OperationalError: The tunnel has died or the connection
            otherwise fails -- the caller decides whether to re-establish
            the tunnel or give up.
    """
    with (
        psycopg.connect(
            host="127.0.0.1",
            port=local_port,
            dbname=creds["dbname"],
            user=creds["user"],
            password=creds["password"],
            connect_timeout=10,
        ) as conn,
        conn.cursor() as cur,
    ):
        cur.execute(_FILE_QUERY, (_DATASET_NAME, content_sha256))
        file_row = cur.fetchone()
        cur.execute(_RUN_QUERY, (_DATASET_NAME, content_sha256))
        run_row = cur.fetchone()

    if file_row is not None:
        _file_id, file_status, duplicate_of_file_id = file_row
        state.last_file_status = (
            f"{file_status} (content-duplicate of file_id={duplicate_of_file_id})"
            if duplicate_of_file_id is not None
            else str(file_status)
        )

    if run_row is not None:
        run_id, status, rows_loaded, duration_ms, report_uri = run_row
        state.last_run_status = str(status)
        if status in _TERMINAL_STATUSES:
            return _PollOutcome(
                run_id=run_id,
                status=status,
                rows_loaded=rows_loaded,
                duration_ms=duration_ms,
                report_uri=report_uri,
                last_file_status=state.last_file_status,
                last_run_status=state.last_run_status,
            )
    return None


def _poll_for_receipt(
    *,
    kubectl_context: str,
    content_sha256: bytes,
    timeout_seconds: float,
) -> _PollOutcome:
    """Poll `meta.ingestion_runs` (joined through `meta.files` by content hash) until terminal.

    Opens a FRESH `kubectl port-forward` tunnel for every single check, torn
    down again immediately after -- never one long-lived tunnel reused
    across checks. This is a deliberate, evidence-based choice, not the
    more obvious "open once, poll many times" shape: verified live against
    this project's own kind cluster (WSL2), a `kubectl port-forward` tunnel
    to `analytics-db-rw` here reliably serves exactly one real
    (data-carrying) connection before the pod-side end resets it
    ("read: connection reset by peer" in `kubectl`'s own stderr) -- a second
    `psycopg.connect()` reusing the same tunnel fails with "connection
    refused" every time, reproduced five times in a row with a 2s gap. One
    tunnel per connection is exactly the shape `tests/e2e/cluster/
    test_postgres_topology.py`'s own `_cluster_connection` already uses
    (never more than one connection per `_port_forwarded_postgres` call) --
    this function follows the same proven convention rather than
    introducing a new, this-environment-specific retry protocol.

    Args:
        kubectl_context: The kubectl context to reach the analytical cluster
            through.
        content_sha256: The uploaded file's raw sha256 digest -- the join
            key, per this module's own docstring on content-addressed
            identity.
        timeout_seconds: Seconds to keep polling before giving up.

    Returns:
        A `_PollOutcome`. `outcome.timed_out` is `True` when no terminal
        status was observed within `timeout_seconds`.
    """
    creds = _read_analytics_credentials(kubectl_context)
    deadline = time.monotonic() + timeout_seconds
    state = _PollState()

    while time.monotonic() < deadline:
        try:
            with _port_forwarded_analytics(kubectl_context) as local_port:
                outcome = _poll_once(
                    local_port=local_port,
                    creds=creds,
                    content_sha256=content_sha256,
                    state=state,
                )
        except (RuntimeError, psycopg.OperationalError) as exc:
            print(f"WARNING: transient poll error, retrying: {exc}", file=sys.stderr)
            outcome = None

        if outcome is not None:
            return outcome

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(_POLL_INTERVAL_SECONDS, remaining))

    return _PollOutcome(
        run_id=None,
        status=None,
        rows_loaded=None,
        duration_ms=None,
        report_uri=None,
        last_file_status=state.last_file_status,
        last_run_status=state.last_run_status,
    )


def _print_receipt(outcome: _PollOutcome) -> None:
    """Print a terminal `_PollOutcome` as human-readable key-value lines.

    Deliberately not raw JSON: this is human-facing demo output (this
    plan's Task 1 action), read by a developer's eyes, not parsed by
    another program.

    Args:
        outcome: A `_PollOutcome` with `outcome.timed_out` `False`.
    """
    print("--- Ingestion receipt ---")
    print(f"run_id:       {outcome.run_id}")
    print(f"status:       {outcome.status}")
    print(f"rows_loaded:  {outcome.rows_loaded if outcome.rows_loaded is not None else 'N/A'}")
    print(f"duration_ms:  {outcome.duration_ms if outcome.duration_ms is not None else 'N/A'}")
    print(f"report_uri:   {outcome.report_uri or '(none)'}")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this script's command-line arguments.

    Args:
        argv: Argument list to parse. `None` (the default) parses
            `sys.argv[1:]`.

    Returns:
        The parsed namespace, with `file: Path`, `key: str | None` and
        `timeout: float` attributes.
    """
    parser = argparse.ArgumentParser(
        description=(
            "D-14/D-15/D-16 developer demo: upload FILE to s3://raw/ and wait for the "
            "real, sensor-driven csv_ingest_customers pipeline to process it -- never "
            "triggers the DAG directly."
        ),
    )
    parser.add_argument(
        "--file",
        required=True,
        type=Path,
        help="Local CSV file to upload.",
    )
    parser.add_argument(
        "--key",
        default=None,
        help=(
            "Object key to upload to, under s3://raw/. Defaults to "
            "customers/<basename>-<unix-timestamp>.csv so repeated demo runs never "
            "collide on object key."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=_DEFAULT_TIMEOUT_SECONDS,
        help=f"Seconds to wait for a terminal run status (default: {_DEFAULT_TIMEOUT_SECONDS:g}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Upload `--file`, wait for the real pipeline, print a receipt.

    Args:
        argv: Argument list to parse. `None` (the default) parses
            `sys.argv[1:]`.

    Returns:
        `0` when a `"SUCCEEDED"` receipt was printed; `1` on a `"FAILED"`
        receipt or a timeout; `2` when `--file` does not exist or `--timeout`
        is not positive (neither ever attempts an upload).
    """
    args = _parse_args(argv)
    file_path: Path = args.file
    timeout_seconds: float = args.timeout

    if not file_path.is_file():
        print(f"ERROR: --file {file_path} does not exist", file=sys.stderr)
        return 2
    if timeout_seconds <= 0:
        print(f"ERROR: --timeout must be positive, got {timeout_seconds!r}", file=sys.stderr)
        return 2

    kubectl_context = _kubectl_context()
    content_sha256 = _sha256_of_file(file_path)
    key = args.key or f"customers/{file_path.name}-{int(time.time())}.csv"

    print("Resolving live MinIO credentials...")
    s3 = _build_s3_client(kubectl_context)

    try:
        with file_path.open("rb") as fh:
            s3.put_object(Bucket=_RAW_BUCKET, Key=key, Body=fh)
    except (ClientError, BotoCoreError) as exc:
        print(f"ERROR: failed to upload to s3://{_RAW_BUCKET}/{key}: {exc}", file=sys.stderr)
        return 1

    print(
        f"Uploaded to s3://{_RAW_BUCKET}/{key} — waiting for the sensor to notice "
        "(poke interval 30s, deferred)...",
    )

    outcome = _poll_for_receipt(
        kubectl_context=kubectl_context,
        content_sha256=content_sha256,
        timeout_seconds=timeout_seconds,
    )
    if outcome.timed_out:
        print(
            f"TIMEOUT: no terminal meta.ingestion_runs status within {timeout_seconds:g}s.\n"
            f"  last-observed meta.files status:          "
            f"{outcome.last_file_status or 'not yet discovered'}\n"
            f"  last-observed meta.ingestion_runs status: "
            f"{outcome.last_run_status or 'no run yet'}\n"
            "For further diagnosis:\n"
            "  kubectl logs -n airflow deploy/airflow-scheduler\n"
            "  kubectl logs -n airflow deploy/airflow-dag-processor\n"
            "  the Airflow UI: http://airflow.localtest.me",
            file=sys.stderr,
        )
        return 1

    _print_receipt(outcome)
    return 0 if outcome.status == "SUCCEEDED" else 1


if __name__ == "__main__":
    sys.exit(main())
