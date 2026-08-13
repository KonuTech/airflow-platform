"""Config-not-code (SCHEMA-07): the model, loader, hasher and Postgres-backed registry.

Callers import from the submodule directly, e.g.
``from dataplat.config.model import DatasetConfig`` — this package marker
re-exports nothing, matching ``dataplat/models/__init__.py``'s and
``dataplat/storage/__init__.py``'s shallow re-export convention.
"""

from __future__ import annotations
