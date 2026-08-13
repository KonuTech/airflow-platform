"""The one psycopg connection-pool factory dataplat's runtime path uses.

``create_pool()`` is the ONLY place a ``psycopg_pool.ConnectionPool`` is
constructed anywhere in the ``dataplat`` runtime path. ``dataplat.config.
registry`` and ``dataplat.metadata.postgres`` (both later plans) call
``create_pool()`` rather than constructing their own pool, so pool sizing and
construction-failure handling stay in one place.

This module talks to PostgreSQL through raw ``psycopg``/``psycopg_pool``
only, never SQLAlchemy. Alembic's ``migrations/env.py`` (a different plan)
needs a SQLAlchemy engine instead — the two connection factories must never
import each other (03-RESEARCH.md Pitfall 4).
"""

from __future__ import annotations

from urllib.parse import urlsplit

import psycopg
from psycopg_pool import ConnectionPool

from dataplat.errors import StorageError


def create_pool(dsn: str, *, min_size: int = 1, max_size: int = 2) -> ConnectionPool:
    """Construct an unopened connection pool sized for a short-lived pod.

    The pool never attempts a connection until the caller explicitly opens
    it — via ``pool.open(wait=True)``, or by using the pool as a context
    manager — so an invocation that never touches the database (e.g.
    ``dataplat --version``) never pays a connection cost.

    Args:
        dsn: A PostgreSQL connection string.
        min_size: Minimum number of connections the pool keeps open.
        max_size: Maximum number of connections the pool may open.

    Returns:
        An unopened ``psycopg_pool.ConnectionPool``.

    Raises:
        StorageError: If the pool cannot be constructed. The error's
            ``context`` carries only the DSN's scheme — never its
            credential component, since a ``StorageError`` may end up
            logged.
    """
    try:
        return ConnectionPool(dsn, min_size=min_size, max_size=max_size, open=False)
    except psycopg.OperationalError as exc:
        msg = "failed to construct the PostgreSQL connection pool"
        raise StorageError(msg, context={"dsn_scheme": urlsplit(dsn).scheme}) from exc
