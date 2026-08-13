"""Shared fixtures for tests/integration/ — testcontainers PostgreSQL + MinIO (D-04).

Every fixture here is session-scoped: one throwaway PostgreSQL 18 container
and one throwaway MinIO container serve the whole `tests/integration/`
collection, mirroring `tests/e2e/cluster/conftest.py`'s session-scope
convention for expensive, shared dependencies.

`postgres_dsn` reproduces `helm/values/local/cnpg-analytics.yaml`'s
`postInitApplicationSQL` (`CREATE ROLE etl_app LOGIN;`) immediately after the
container starts, so every migration's `GRANT ... TO etl_app` statement
behaves identically here and against the live cluster — this fixture is the
one place that reproduction happens; no test in this directory should issue
its own `CREATE ROLE`.

`run_migrations`/`migrated_dsn` apply `migrations/` in-process via Alembic's
own `Config`/`command.upgrade` API against `migrations/alembic.ini`, with
`ALEMBIC_DSN` set to the fixture's DSN for the duration of the call — the
same environment variable `migrations/env.py` reads at runtime, so this
fixture exercises the exact code path `make test-integration` and a real
deployment both use, never a parallel one.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Any

import boto3
import psycopg
import pytest
from alembic import command
from alembic.config import Config
from botocore.config import Config as BotoConfig

# `testcontainers.postgres`/`testcontainers.minio` (no `community.` segment)
# are deprecated as of testcontainers 4.15.0 in favor of these modules —
# same classes, same API, no import-time DeprecationWarning.
from testcontainers.community.minio import MinioContainer
from testcontainers.community.postgres import PostgresContainer

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

REPO_ROOT = Path(__file__).resolve().parents[2]
ALEMBIC_INI = REPO_ROOT / "migrations" / "alembic.ini"


@pytest.fixture(scope="session", autouse=True)
def _require_docker() -> None:
    """Skip the whole suite, with a named reason, when no Docker daemon answers.

    A developer without Docker running should see one clear skip message,
    not a testcontainers stack trace from whichever test happens to start a
    container first — the same reasoning as
    `tests/e2e/cluster/conftest.py`'s `_require_cluster`.
    """
    docker_bin = shutil.which("docker")
    if docker_bin is None:
        pytest.skip("docker not found on PATH — tests/integration/ needs a local Docker daemon")
    # 30s, not 10s: measured this session, `docker info` against a WSL2/
    # Docker Desktop backend routinely takes ~10s on its own — a 10s ceiling
    # produced false-negative skips (TimeoutExpired, not a real unreachable
    # daemon) rather than a clean pass.
    proc = subprocess.run(  # noqa: S603
        [docker_bin, "info"],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    if proc.returncode != 0:
        pytest.skip(
            f"docker daemon not reachable (exit {proc.returncode}) — "
            f"tests/integration/ needs a local Docker daemon:\n{proc.stderr}",
        )


@pytest.fixture(scope="session")
def postgres_dsn() -> Iterator[str]:
    """A throwaway PostgreSQL 18 container, with `etl_app` created, unmigrated.

    PG 18 — the analytical database's pinned major (CLAUDE.md); the Airflow
    metadata major is capped at 17, and this fixture must never accidentally
    prove migrations only work against the wrong major. `dbname="analytics"`
    matches `helm/values/*/cnpg-analytics.yaml` exactly — testcontainers'
    own default (`test`) would trip `migrations/env.py`'s wrong-database
    guard (Pattern 2), which is doing its job correctly if it does: this
    fixture must present the *right* name, not a name the guard has to
    special-case.

    Yields:
        A plain `postgresql://` DSN (psycopg-compatible, no SQLAlchemy
        dialect suffix) pointed at the running container.
    """
    with PostgresContainer("postgres:18-bookworm", driver="psycopg", dbname="analytics") as pg:
        dsn = pg.get_connection_url().replace("postgresql+psycopg://", "postgresql://")
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            # Reproduces cnpg-analytics.yaml's postInitApplicationSQL exactly
            # (03-RESEARCH.md finding 2) — no schema, no password, no grants
            # beyond LOGIN. Every grant this phase's migrations issue is
            # proven against this same starting state.
            cur.execute("CREATE ROLE etl_app LOGIN")
        yield dsn


@pytest.fixture(scope="session")
def run_migrations() -> Callable[[str], None]:
    """A session-scoped `run_migrations(dsn)` callable: `alembic upgrade head`, in-process.

    Returns:
        A callable that runs every revision under `migrations/versions/`
        against whatever DSN it is given, via `ALEMBIC_DSN` — the exact
        mechanism `migrations/env.py` expects.
    """
    alembic_config = Config(str(ALEMBIC_INI))

    def _run(dsn: str) -> None:
        previous = os.environ.get("ALEMBIC_DSN")
        os.environ["ALEMBIC_DSN"] = dsn
        try:
            command.upgrade(alembic_config, "head")
        finally:
            if previous is None:
                os.environ.pop("ALEMBIC_DSN", None)
            else:
                os.environ["ALEMBIC_DSN"] = previous

    return _run


@pytest.fixture(scope="session")
def migrated_dsn(run_migrations: Callable[[str], None], postgres_dsn: str) -> str:
    """`postgres_dsn`, with `alembic upgrade head` already applied once, session-scoped.

    Most tests in this directory only need a fully-migrated database and
    never re-run migrations themselves — this is the one fixture they
    depend on. `test_upgrade_head_is_idempotent` is the deliberate exception:
    it depends on `run_migrations` directly, so it can invoke the same
    callable a second time against this fixture's own DSN.
    """
    run_migrations(postgres_dsn)
    return postgres_dsn


@pytest.fixture(scope="session")
def minio_config() -> Iterator[dict[str, str]]:
    """A throwaway MinIO container's connection details.

    Yields `get_config()` — never `get_client()`, which returns the
    forbidden `minio` SDK client (STACK.md rejects it; §5 requires the S3
    abstraction stay swappable). `s3_client` below builds a real `boto3`
    client from these values instead, exactly as `dataplat`'s own
    `ObjectStore` implementation does against real MinIO.
    """
    with MinioContainer() as minio:
        yield minio.get_config()


@pytest.fixture(scope="session")
def s3_client(minio_config: dict[str, str]) -> Any:
    """A `boto3` S3 client built from `minio_config`, path-style addressing forced."""
    return boto3.client(
        "s3",
        endpoint_url=f"http://{minio_config['endpoint']}",
        aws_access_key_id=minio_config["access_key"],
        aws_secret_access_key=minio_config["secret_key"],
        config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
