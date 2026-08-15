"""Source-agnostic ingestion contracts: the ``Source``/``RecordStream`` protocols.

Callers import from the submodule directly, e.g.
``from dataplat.sources.protocol import Source`` — this package marker
re-exports nothing, matching ``dataplat/config/__init__.py``'s shallow
re-export convention. ``Source.inspect()`` and ``dataplat.models.profile.
CsvProfile`` landed in Phase 6 (plan 06-14) — see ``sources/protocol.py``'s
module docstring.
"""

from __future__ import annotations
