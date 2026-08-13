"""Cross-cutting observability seams: structured logging, metrics, tracing.

Callers import from the submodule directly, e.g.
``from dataplat.observability import logging`` — this package marker
re-exports nothing, matching ``dataplat/__init__.py``'s own shallow
re-export convention.
"""

from __future__ import annotations
