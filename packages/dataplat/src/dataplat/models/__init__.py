"""Frozen value objects shared across dataplat's pipeline stages.

Callers import from the submodule directly, e.g.
``from dataplat.models.record import RecordChunk`` — this package marker
re-exports nothing, matching ``dataplat/__init__.py``'s own shallow
re-export convention (it re-exports only from ``version.py`` and nothing
deeper).
"""

from __future__ import annotations
