"""The one psycopg connection-pool factory dataplat's runtime path uses.

``create_pool()`` is the ONLY place a ``psycopg_pool.ConnectionPool`` is
constructed anywhere in the ``dataplat`` runtime path. ``dataplat.config.
registry`` and ``dataplat.metadata.postgres`` (both later plans) call
``create_pool()`` rather than constructing their own pool, so pool sizing
stays in one place.

This module talks to PostgreSQL through raw ``psycopg``/``psycopg_pool``
only, never SQLAlchemy. Alembic's ``migrations/env.py`` (a different plan)
needs a SQLAlchemy engine instead — the two connection factories must never
import each other (03-RESEARCH.md Pitfall 4).
"""

from __future__ import annotations

from psycopg_pool import ConnectionPool


def create_pool(dsn: str, *, min_size: int = 1, max_size: int = 2) -> ConnectionPool:
    """Construct an unopened connection pool sized for a short-lived pod.

    ``ConnectionPool(..., open=False)`` neither parses/validates ``dsn`` nor
    attempts a connection at construction time (WR-02: verified directly
    against the pinned ``psycopg_pool`` -- even a malformed DSN such as
    ``""``, ``"not a dsn at all"``, or ``"postgresql://"`` constructs a pool
    object here without raising). The pool only attempts a connection once
    the caller explicitly opens it — via ``pool.open(wait=True)``, or by
    using the pool as a context manager — so an invocation that never
    touches the database (e.g. ``dataplat --version``) never pays a
    connection cost. Any DSN/connectivity failure, including a malformed
    DSN, therefore surfaces at that later ``.open()``/first-use call site,
    not here; this function has no failure mode of its own left to
    document or catch.

    Args:
        dsn: A PostgreSQL connection string.
        min_size: Minimum number of connections the pool keeps open.
        max_size: Maximum number of connections the pool may open.

    Returns:
        An unopened ``psycopg_pool.ConnectionPool``.
    """
    return ConnectionPool(dsn, min_size=min_size, max_size=max_size, open=False)
