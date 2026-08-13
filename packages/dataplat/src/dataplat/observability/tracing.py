"""No-op tracing call site — a real context manager now, a real backend in Phase 7.

``start_span()`` returns ``contextlib.nullcontext()``: a genuine no-op context
manager, not a bare pass-through function, so ``with tracing.start_span("stage"):
...`` reads identically to how a real span will read once Phase 7 wires an
OTel-backed implementation into this module's internals (CONTEXT.md D-03).
"""

from __future__ import annotations

import contextlib
from contextlib import AbstractContextManager


def start_span(name: str) -> AbstractContextManager[None]:  # noqa: ARG001 -- no-op today
    """Start a no-op span. No-op until Phase 7 wires a real OTel backend.

    Args:
        name: Span name, e.g. ``"stage"``.

    Returns:
        A no-op context manager (``contextlib.nullcontext()``).
    """
    return contextlib.nullcontext()
