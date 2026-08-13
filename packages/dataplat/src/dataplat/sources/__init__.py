"""Source-agnostic ingestion contracts: the ``Source``/``RecordStream`` protocols.

Callers import from the submodule directly, e.g.
``from dataplat.sources.protocol import Source`` — this package marker
re-exports nothing, matching ``dataplat/config/__init__.py``'s shallow
re-export convention. Deliberately minimal in this phase: no
``DatasetSchema``/``SourceProfile`` attributes and no ``inspect()`` method —
see ``sources/protocol.py``'s module docstring.
"""

from __future__ import annotations
