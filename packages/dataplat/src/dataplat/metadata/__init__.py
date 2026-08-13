"""The metadata control plane: typed CRUD over the `meta` schema's five slice tables.

Callers import from the submodule directly, e.g.
``from dataplat.metadata.repository import MetadataRepository`` — this
package marker re-exports nothing, matching ``dataplat/__init__.py``'s own
shallow re-export convention.
"""

from __future__ import annotations
