"""Alembic runtime environment for the analytical database's `meta` schema.

This file is the one place a mistake here would be catastrophic: pointing
these migrations at the Airflow metadata database would violate README §4 /
INFRA-04 (the two PostgreSQL instances must stay physically and logically
separate) by creating application schema inside Airflow's own control-plane
database. `run_migrations_online()` therefore asserts `current_database()`
against `EXPECTED_DATABASE` before configuring the migration context, and
raises before any DDL can run if the two disagree.

Imports only `sqlalchemy`/`alembic` — never `dataplat.storage.db`. Alembic
needs a SQLAlchemy engine (`engine_from_config`); the application's own
`psycopg_pool.ConnectionPool` factory in `dataplat.storage.db` is a
completely separate connection path for a completely separate purpose
(03-RESEARCH.md Pitfall 4). Mixing the two here would make a migration-time
failure look like an application-runtime bug, or vice versa.
"""

from __future__ import annotations

import os
from logging.config import fileConfig

import sqlalchemy as sa
from alembic import context
from sqlalchemy import engine_from_config, pool

# The analytical database's name, per `helm/values/*/cnpg-analytics.yaml`
# `cluster.initdb.database`. Airflow's own metadata database has a different
# name entirely — this guard exists so that connecting to the wrong instance
# fails immediately and loudly rather than silently applying DDL there.
EXPECTED_DATABASE = "analytics"

# This is the Alembic Config object, which provides access to the values
# within the .ini file in use.
config = context.config

# Interpret the config file for Python logging. This line sets up loggers
# basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# No declarative ORM models exist in this project for `meta`/`normalized` —
# STACK.md is explicit that every revision is hand-written, and
# `alembic revision --autogenerate` is used only to produce a throwaway
# draft, never committed output. `target_metadata = None` is therefore
# correct here, not a placeholder waiting to be filled in.
target_metadata = None


def _sqlalchemy_url() -> str:
    """Resolve the migration target DSN from the `ALEMBIC_DSN` environment variable.

    Never a literal in a committed file (SEC-15 / README §81). Accepts a
    plain `postgresql://` DSN (what testcontainers' `get_connection_url()`
    yields once its `+psycopg` suffix is stripped for raw-psycopg callers,
    per `tests/integration/conftest.py`) and rewrites it to
    `postgresql+psycopg://` — SQLAlchemy 2.0's bare `postgresql://` dialect
    resolves to psycopg2, which this project does not install; psycopg3 is
    the supported driver and must be named explicitly.

    Returns:
        A `postgresql+psycopg://`-scheme SQLAlchemy URL.

    Raises:
        RuntimeError: `ALEMBIC_DSN` is unset. A migration must never fall
            back to a default connection target.
    """
    dsn = os.environ.get("ALEMBIC_DSN")
    if not dsn:
        msg = (
            "ALEMBIC_DSN is not set. Refusing to guess a migration target — "
            "export ALEMBIC_DSN to a postgresql:// or postgresql+psycopg:// DSN."
        )
        raise RuntimeError(msg)
    if dsn.startswith("postgresql://"):
        return dsn.replace("postgresql://", "postgresql+psycopg://", 1)
    return dsn


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (emit SQL, no live connection).

    Offline mode never opens a connection, so the wrong-database guard in
    `run_migrations_online()` does not apply here — there is no
    `current_database()` to read. Kept for parity with Alembic's own default
    environment shape; this project's tests and Make targets exercise the
    online path exclusively.
    """
    context.configure(
        url=_sqlalchemy_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        version_table_schema="meta",
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode, guarded against the wrong database.

    Connects once, confirms `SELECT current_database()` matches
    `EXPECTED_DATABASE`, and only then configures the migration context and
    runs migrations inside a transaction. The guard executes and raises
    before `context.configure()` — no DDL can run ahead of it.

    Raises:
        RuntimeError: the connected database's name is not
            `EXPECTED_DATABASE`.
    """
    config.set_main_option("sqlalchemy.url", _sqlalchemy_url())
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        actual_database = connection.execute(sa.text("SELECT current_database()")).scalar()
        if actual_database != EXPECTED_DATABASE:
            msg = (
                f"Refusing to run analytical migrations against database "
                f"{actual_database!r} (expected {EXPECTED_DATABASE!r}). This guard exists "
                f"specifically to prevent migrating the Airflow metadata database "
                f"(INFRA-04 / README §4)."
            )
            raise RuntimeError(msg)

        # Alembic creates its own bookkeeping table (`alembic_version`, in
        # version_table_schema below) BEFORE running any revision's
        # upgrade() — including 0001, which is otherwise the one place
        # `CREATE SCHEMA meta` lives. Against a brand-new database that
        # ordering makes `alembic_version`'s own CREATE TABLE fail with
        # `InvalidSchemaName`, since PostgreSQL never auto-creates a schema
        # for a table statement. Ensuring the schema here, committed ahead
        # of Alembic's own migration transaction, is what makes
        # version_table_schema="meta" usable at all against an empty
        # database. Revision 0001 still issues its own (idempotent)
        # `CREATE SCHEMA IF NOT EXISTS meta` — this is defense in depth,
        # not a duplicate no-op to remove.
        connection.execute(sa.text("CREATE SCHEMA IF NOT EXISTS meta"))
        connection.commit()

        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            # Keeps `public` empty — every object this project creates is
            # namespaced under `meta`/`normalized`/etc., including Alembic's
            # own bookkeeping table.
            version_table_schema="meta",
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
