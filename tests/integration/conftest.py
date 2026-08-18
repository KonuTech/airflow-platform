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
from urllib.parse import urlsplit

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
    """A throwaway PostgreSQL 18 container, with `etl_app`/`analytics_owner` created, unmigrated.

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
            # `cnpg-analytics.yaml`'s `initdb.owner: analytics_owner` makes
            # CloudNativePG auto-create this role as part of its own
            # bootstrap, ahead of `postInitApplicationSQL` — a real cluster
            # never migrates without it already existing. Migration 0013
            # (`GRANT SELECT ON meta.v_customers_lineage TO analytics_owner`)
            # started depending on that role existing here too, but this
            # fixture was never updated to create it (found via a genuine
            # `alembic upgrade head` failure, `UndefinedObject: role
            # "analytics_owner" does not exist" — not caused by this plan's
            # own new migrations, but blocking verification of every
            # migration from 0013 onward, including this plan's 0014-0016).
            cur.execute("CREATE ROLE analytics_owner LOGIN")
        yield dsn


@pytest.fixture(autouse=True)
def _clean_up_non_numeric_silver_business_keys(
    request: pytest.FixtureRequest,
) -> Iterator[None]:
    """Defend the shared `silver.customers`/`silver.orders` tables' one invariant every real
    `MergePublisher`/`OrdersMergePublisher` call depends on: every row's business key must
    cast to the gold table's own `integer` business-key column
    (`normalized.customers.customer_id`/`normalized.orders.order_id`, migrations 0005/0016).

    `silver.customers`/`silver.orders` are single, SESSION-scoped tables shared across the
    WHOLE `tests/integration/` collection (`migrated_dsn`'s own docstring) with no per-test
    isolation and no dataset_id scoping -- and `merge.py`'s/`merge_orders.py`'s own
    `_PUBLISH_SQL` reads each table in FULL, unconditionally, with no `WHERE` clause at all
    (`merge.py`'s own module docstring: "deliberately single-dataset for this phase"). A
    handful of `tests/integration/test_dbt_*.py`'s own `dbt`-marked tests deliberately seed
    non-numeric business keys (e.g. `"A1"`, `"D1"`, `"I1"`) via a REAL `dbt build`, to
    exercise dedup/incremental logic in isolation from gold-layer concerns -- entirely
    legitimate for their own purposes, but such a row, left behind, would otherwise poison
    EVERY subsequent real publish for the rest of the session: a single non-castable
    `customer_id`/`order_id` aborts the whole `INSERT ... SELECT` statement, not merely the
    row that caused it.

    Only actually touches the database for `dbt`-marked tests: cheap for every other test in
    this directory (a `get_closest_marker` check, nothing else), and `test_docker_image.py`/
    `test_objectstore.py`/`test_metrics_otlp.py`/`test_dbt_docker_image.py` -- none of which
    request `migrated_dsn` at all -- never pay the cost of forcing that fixture's own
    container-start-plus-migrate chain into existence just for this cleanup.
    """
    # Resolved eagerly, before `yield`, when it will be needed -- pytest
    # deprecates `getfixturevalue` calls made during teardown (after
    # `yield`), so the marker check gates WHETHER this ever touches the
    # database, but the resolution itself always happens at setup time.
    migrated_dsn: str | None = None
    if request.node.get_closest_marker("dbt") is not None:
        migrated_dsn = request.getfixturevalue("migrated_dsn")
    yield
    if migrated_dsn is None:
        return
    with psycopg.connect(migrated_dsn, autocommit=True) as conn:
        conn.execute(r"DELETE FROM silver.customers WHERE customer_id !~ '^[0-9]+$'")
        conn.execute(r"DELETE FROM silver.orders WHERE order_id !~ '^[0-9]+$'")


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


# plan 08.1-08: shared `dbt build` invocation helper for tests/integration/
# test_dbt_*.py (`-m dbt`, registered in pyproject.toml). Needs BOTH a local
# Docker daemon (already covered by `_require_docker` above, autouse for the
# whole directory) AND a local `dbt` binary on PATH — checked here, narrowly,
# only when a `dbt`-marked test actually requests `run_dbt_build`, rather
# than as a second directory-wide autouse fixture every non-dbt integration
# test would otherwise pay the `shutil.which` cost for.
DBT_PROJECT_DIR = REPO_ROOT / "dbt"


def _dsn_to_dbt_env_vars(dsn: str) -> dict[str, str]:
    """Parse a `postgresql://user:pass@host:port/dbname` DSN into `DBT_PG_*` env vars.

    Matches `dbt/profiles.yml`'s five `env_var('DBT_PG_*')` calls exactly —
    the same env vars `docker/dbt/resolve_secrets.py` populates from Vault
    in a real deployment, populated here directly from the testcontainers
    DSN instead.
    """
    parsed = urlsplit(dsn)
    return {
        "DBT_PG_HOST": parsed.hostname or "",
        "DBT_PG_PORT": str(parsed.port or 5432),
        "DBT_PG_USER": parsed.username or "",
        "DBT_PG_PASSWORD": parsed.password or "",
        "DBT_PG_DBNAME": (parsed.path or "/").lstrip("/"),
    }


@pytest.fixture(scope="session")
def run_dbt_build() -> Callable[..., subprocess.CompletedProcess[str]]:
    """A session-scoped `run_dbt_build(dsn, *, select=None)` callable: a real `dbt build`.

    Skips the whole `dbt`-marked suite, with a named reason, when no `dbt`
    binary is on `PATH` — the same reasoning as `_require_docker` above.
    Asserts `returncode == 0` itself (surfacing `stdout`/`stderr` in the
    assertion message) rather than leaving each call site to repeat that
    check — a failed `dbt build` with no visible output is undebuggable.

    Returns:
        A callable that runs `dbt build --project-dir dbt --profiles-dir
        dbt` (optionally `--select <select>`) against whatever DSN it is
        given, and returns the completed process on success.
    """
    dbt_bin = shutil.which("dbt")
    if dbt_bin is None:
        pytest.skip(
            "dbt not found on PATH — tests/integration/test_dbt_*.py needs a local dbt binary"
        )

    def _run(dsn: str, *, select: str | None = None) -> subprocess.CompletedProcess[str]:
        args = [
            dbt_bin,
            "build",
            "--project-dir",
            str(DBT_PROJECT_DIR),
            "--profiles-dir",
            str(DBT_PROJECT_DIR),
        ]
        if select is not None:
            args += ["--select", select]
        proc = subprocess.run(  # noqa: S603 -- fixed argv, no shell, no user input
            args,
            env={**os.environ, **_dsn_to_dbt_env_vars(dsn)},
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )
        assert proc.returncode == 0, (
            f"dbt build failed (exit {proc.returncode}):\n"
            f"--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}"
        )
        return proc

    return _run
