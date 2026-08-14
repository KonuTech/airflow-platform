#!/usr/bin/env python3
r"""scripts/repair-duplicate-file-lineage.py -- CR-02 backfill: fix orphaned `meta.files` rows.

`04-REVIEW.md`'s CR-02 finding: `find_file_by_content_hash`'s `SELECT ...
LIMIT 1` carried no `ORDER BY`, so PostgreSQL's own documentation treats
which row it returned as unspecified once 2+ rows shared a
`(dataset_id, content_sha256)`. `discovery.py`'s rediscovery-correction
logic depends on that call returning the SAME row across repeated calls for
the same content -- when it didn't, a genuine duplicate could end up with
`duplicate_of_file_id IS NULL` instead of pointing at its true original.
`04-VERIFICATION.md` observed this LIVE on this project's own cluster:
`meta.files.file_id=10` was orphaned -- `duplicate_of_file_id IS NULL`,
`status='DISCOVERED'`, zero `meta.ingestion_runs` rows referencing it --
even though its content already matched an earlier, already-`SUCCEEDED`
file. Task 2 of this plan (`04-10-PLAN.md`) fixed the root cause (an
`ORDER BY file_id ASC` added to `find_file_by_content_hash`); this script is
the one-off, safe-to-rerun repair for rows the OLD, non-deterministic
behavior already left orphaned before that fix was deployed.

This is a one-off, historical-defect repair tool, not a recurring
operational workflow -- it does not need, and deliberately does not get, a
permanent `Makefile` target. Run it once after the fixed image (Task 2) is
already live, to correct whatever the old code left behind; a healthy
cluster (nothing left to repair) is the expected steady state afterward, and
re-running this script against a healthy cluster is a safe, fast no-op (see
`--dry-run` and the idempotency proof in this module's own acceptance
criteria).

The core query is generic and dataset-agnostic -- it never hardcodes a
specific `file_id` -- so it repairs every orphaned duplicate-content group
it finds, on any dataset, not merely the specific row `04-VERIFICATION.md`
happened to observe.

Run location (mirrors `scripts/ingest-demo.py`'s own docstring): this script
is meant to run from a developer's own host machine, reaching the live
analytical PostgreSQL cluster the same way `tests/e2e/cluster/
test_postgres_topology.py`'s `_cluster_connection` and `ingest-demo.py`'s
`_poll_for_receipt` do -- a torn-down-on-exit `kubectl port-forward` to
`analytics-db-rw`, never a direct in-cluster DSN, and never more than ONE
`psycopg.connect()` over that tunnel: `ingest-demo.py`'s own
`_poll_for_receipt` docstring documents, from a live reproduction on this
project's own cluster, that a reused tunnel's second connection attempt
reliably fails ("connection refused"). This script opens exactly one tunnel
and one connection for its entire run and issues every query -- the initial
diagnostic, the repair, and the re-verification -- through that same
connection.

`_kubectl_context`/`_require_kubectl`/`_read_analytics_credentials`/
`_port_forwarded_analytics` below are duplicated from `scripts/
ingest-demo.py` rather than imported -- `ingest-demo.py`'s own docstring
already cites `tests/e2e/cluster/test_postgres_topology.py` as its analog
for the same reason, establishing per-script duplication as this
repository's norm for this exact helper set.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import json
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import psycopg

if TYPE_CHECKING:
    from collections.abc import Iterator

_REPO_ROOT = Path(__file__).resolve().parent.parent
_VERSIONS_ENV = _REPO_ROOT / "helm" / "versions.env"

# The CNPG namespace/cluster name every tests/e2e/cluster/ module and
# scripts/ingest-demo.py also use -- never a literal connection string
# anywhere in this file (D-14).
_NAMESPACE = "data"
_ANALYTICS_CLUSTER = "analytics-db"

# One generic, dataset-agnostic CTE, reused by both the diagnostic SELECT
# and the repair UPDATE below: per (dataset_id, content_sha256) group with
# more than one row, the group's true original is its lowest file_id (the
# same ordering Task 2's ORDER BY file_id ASC fix now applies at lookup
# time). An "orphan" is any non-original row in such a group whose
# duplicate_of_file_id is still NULL.
_CONTENT_GROUPS_CTE = """
    WITH content_groups AS (
        SELECT dataset_id,
               content_sha256,
               MIN(file_id) AS original_file_id,
               COUNT(*) AS group_size
          FROM meta.files
         GROUP BY dataset_id, content_sha256
        HAVING COUNT(*) > 1
    )
"""

# Suppression rationale (S608): both queries below are built by
# concatenating two module-level, fully-static string literals -- there is
# no caller-supplied value, no f-string interpolation and no runtime
# formatting anywhere in either constant; every value that ever crosses
# into these statements is a bound %s placeholder passed to conn.execute()
# at the call site, never string-interpolated here. Splitting the shared
# CTE out as its own constant (reused by both the diagnostic SELECT and the
# repair UPDATE) is what triggers this bandit-derived check on plain string
# concatenation, not any actual injection risk.
_DIAGNOSTIC_SQL = (
    _CONTENT_GROUPS_CTE  # noqa: S608
    + """
    SELECT f.file_id, f.object_uri, f.status, g.original_file_id
      FROM meta.files f
      JOIN content_groups g
        ON f.dataset_id = g.dataset_id AND f.content_sha256 = g.content_sha256
     WHERE f.file_id <> g.original_file_id
       AND f.duplicate_of_file_id IS NULL
     ORDER BY f.file_id
    """
)

_REPAIR_SQL = (
    _CONTENT_GROUPS_CTE  # noqa: S608
    + """
    UPDATE meta.files f
       SET duplicate_of_file_id = g.original_file_id
      FROM content_groups g
     WHERE f.dataset_id = g.dataset_id
       AND f.content_sha256 = g.content_sha256
       AND f.file_id <> g.original_file_id
       AND f.duplicate_of_file_id IS NULL
    RETURNING f.file_id, f.object_uri, g.original_file_id
    """
)


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


def _read_analytics_credentials(kubectl_context: str) -> dict[str, str]:
    """Read and base64-decode the CNPG-generated `analytics-db-app` Secret.

    Same shape as `tests/e2e/cluster/test_postgres_topology.py`'s
    `_read_app_secret` and `scripts/ingest-demo.py`'s helper of the same
    name -- never writes a decoded value to disk or a log.

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
    `_port_forwarded_postgres` and `scripts/ingest-demo.py`'s helper of the
    same name -- there is no ingress for raw PostgreSQL in this phase, so a
    `kubectl port-forward` tunnel is the only way this script (running on
    the developer's own host, outside the cluster) can reach the analytical
    cluster.

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


def _find_orphans(conn: psycopg.Connection[Any]) -> list[tuple[int, str, str, int]]:
    """Run the diagnostic query: every currently-orphaned duplicate-content row.

    Args:
        conn: An open connection to the analytical cluster.

    Returns:
        `(file_id, object_uri, status, original_file_id)` tuples, ordered by
        `file_id`. Empty when the cluster has nothing to repair.
    """
    rows = conn.execute(_DIAGNOSTIC_SQL).fetchall()
    conn.commit()  # release the read-only snapshot cleanly
    return [(int(r[0]), str(r[1]), str(r[2]), int(r[3])) for r in rows]


def _repair_orphans(conn: psycopg.Connection[Any]) -> list[tuple[int, str, int]]:
    """Run the repair UPDATE, inside one explicit transaction, and return what changed.

    Only `meta.files.duplicate_of_file_id` is ever written -- `status`,
    `meta.batch_files` and `meta.ingestion_runs` are deliberately left
    untouched (a duplicate file's `status` correctly stays `DISCOVERED`
    under this platform's existing design, matching the already-healthy
    duplicate-content groups this same query never touches).

    Args:
        conn: An open connection to the analytical cluster.

    Returns:
        `(file_id, object_uri, original_file_id)` tuples for every row this
        call corrected.
    """
    with conn.transaction():
        rows = conn.execute(_REPAIR_SQL).fetchall()
    return [(int(r[0]), str(r[1]), int(r[2])) for r in rows]


def _print_orphans(heading: str, orphans: list[tuple[int, str, str, int]]) -> None:
    """Print every orphaned row found, one line per row.

    Args:
        heading: A one-line label printed before the rows.
        orphans: `(file_id, object_uri, status, original_file_id)` tuples.
    """
    print(heading)
    for file_id, object_uri, status, original_file_id in orphans:
        print(
            f"  file_id={file_id} object_uri={object_uri!r} status={status} "
            f"-> true original file_id={original_file_id}",
        )


def _print_repaired(repaired: list[tuple[int, str, int]]) -> None:
    """Print every row the repair UPDATE actually changed, one line per row.

    Args:
        repaired: `(file_id, object_uri, original_file_id)` tuples.
    """
    print("Repaired:")
    for file_id, object_uri, original_file_id in repaired:
        print(
            f"  file_id={file_id} object_uri={object_uri!r} "
            f"duplicate_of_file_id -> {original_file_id}",
        )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this script's command-line arguments.

    Args:
        argv: Argument list to parse. `None` (the default) parses
            `sys.argv[1:]`.

    Returns:
        The parsed namespace, with a `dry_run: bool` attribute.
    """
    parser = argparse.ArgumentParser(
        description=(
            "CR-02 backfill: repair meta.files rows left orphaned "
            "(duplicate_of_file_id IS NULL despite sharing content with an "
            "earlier file) by find_file_by_content_hash's old, "
            "non-deterministic LIMIT 1 with no ORDER BY. Generic and "
            "dataset-agnostic; safe to re-run."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only run the diagnostic query and print what would change; write nothing.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Diagnose, and unless `--dry-run`, repair every orphaned duplicate-content file row.

    Args:
        argv: Argument list to parse. `None` (the default) parses
            `sys.argv[1:]`.

    Returns:
        `0` on success -- including the "nothing to repair" case, which is
        the expected outcome on a fresh or already-repaired cluster. `1`
        when a repair was attempted but the post-repair re-verification
        still finds orphaned rows (an incomplete repair must be loud, never
        silent).
    """
    args = _parse_args(argv)
    dry_run: bool = args.dry_run

    kubectl_context = _kubectl_context()
    creds = _read_analytics_credentials(kubectl_context)

    # Exactly ONE _port_forwarded_analytics tunnel and ONE psycopg.connect()
    # for this script's entire run -- every query below (diagnostic,
    # repair, re-verify) reuses this same connection; see the module
    # docstring for why a second connect() over this same tunnel is not
    # safe to attempt.
    with (
        _port_forwarded_analytics(kubectl_context) as local_port,
        psycopg.connect(
            host="127.0.0.1",
            port=local_port,
            dbname=creds["dbname"],
            user=creds["user"],
            password=creds["password"],
            connect_timeout=10,
        ) as conn,
    ):
        orphans = _find_orphans(conn)

        if not orphans:
            print("no orphaned duplicate files found -- nothing to repair")
            return 0

        _print_orphans("Found orphaned duplicate file(s):", orphans)

        if dry_run:
            print(f"--dry-run: would repair {len(orphans)} row(s); nothing written.")
            return 0

        repaired = _repair_orphans(conn)
        _print_repaired(repaired)

        remaining = _find_orphans(conn)
        if remaining:
            _print_orphans("ERROR: still orphaned after repair:", remaining)
            print(
                f"ERROR: repair incomplete -- {len(remaining)} orphaned row(s) still "
                "remain after the repair UPDATE",
                file=sys.stderr,
            )
            return 1

        print("Re-verified: zero orphaned duplicate files remain.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
