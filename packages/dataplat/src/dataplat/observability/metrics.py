"""No-op metrics call site — a real signature now, a real backend in Phase 7.

``increment()`` does nothing today. Its signature is real and stable so
pipeline code that calls it now (e.g. ``metrics.increment("rows_loaded", n)``)
requires no caller changes when Phase 7 wires a StatsD-exporter-backed
implementation into this module's internals (CONTEXT.md D-03).
"""

from __future__ import annotations


def increment(name: str, value: int = 1, **labels: str) -> None:
    """Record a counter increment. No-op until Phase 7 wires a real backend.

    Args:
        name: Metric name, e.g. ``"rows_loaded"``.
        value: Amount to increment by. Defaults to ``1``.
        labels: Metric labels/dimensions, e.g. ``dataset="customers"``.
    """
