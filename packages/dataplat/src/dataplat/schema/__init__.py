"""Schema versioning, evolution and the `meta.schema_versions` repository: SCHEMA-03/04/05/06.

Callers import from the submodule directly, e.g.
``from dataplat.schema.repository import SchemaRegistry`` — this package
marker re-exports nothing, matching ``dataplat/config/__init__.py``'s and
``dataplat/sources/__init__.py``'s shallow re-export convention.
"""

from __future__ import annotations
