"""Real OTel-backed tracing -- a genuine no-op until ``configure()`` sees an endpoint.

``configure()`` builds one process-owned ``TracerProvider`` (CONTEXT.md D-11's
Tempo-bound OTLP/HTTP exporter, wired downstream of this seam by a later
plan): a real, ``BatchSpanProcessor``-backed provider when given
``otlp_endpoint``, or ``opentelemetry.trace``'s own ``NoOpTracerProvider``
when not -- genuinely non-recording (``is_recording() is False``), not
merely "configured to drop." ``start_span()`` keeps its Phase-3 signature and
call-site shape exactly (``with tracing.start_span("stage"): ...``, callers
never bind ``as x``); no caller anywhere needs to change.

``configure()`` deliberately never calls ``opentelemetry.trace.
set_tracer_provider()``: that function is documented "This can only be done
once, a warning will be logged if any further attempt is made"
(verified against the installed ``opentelemetry-api==1.44.0`` source).
Calling it twice in one process -- which this very module's own test suite
does, exercising both the unconfigured and configured cases as separate
test functions in one pytest session -- would silently strand every later
``configure()`` call, leaving ``start_span()`` bound to whichever endpoint
happened to configure first. This module owns its provider directly instead,
mirroring ``logging.py``'s ``structlog.configure()`` precedent: freely
re-callable, immediate effect, no hidden set-once state (same reasoning as
``secrets/resolver.py``'s module-level ``_client`` singleton). ``start_as_
current_span()``'s "current span" propagation is ``contextvars``-based and
entirely independent of which ``TracerProvider`` instance produced the
``Tracer``, so a downstream ``opentelemetry.trace.get_current_span()`` read
(this project's KPO-pod entrypoint, a later plan) still sees this module's
span correctly regardless.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

if TYPE_CHECKING:
    from collections.abc import Iterator

# The one process-owned provider this module manages -- see module docstring
# for why this is never registered via `opentelemetry.trace.
# set_tracer_provider()`. Starts as a genuine no-op so a process that never
# calls `configure()` at all (e.g. a unit test importing this module in
# isolation) behaves identically to one that explicitly calls
# `configure(otlp_endpoint=None)`.
_provider: trace.TracerProvider = trace.NoOpTracerProvider()


def configure(*, otlp_endpoint: str | None) -> None:
    """Build this process's one ``TracerProvider`` -- real when given an endpoint.

    Safely re-callable: each call replaces the module-owned provider
    outright (see module docstring for why this never touches
    ``opentelemetry.trace``'s own set-once global registry).

    Args:
        otlp_endpoint: The OTLP/HTTP collector's base URL (e.g.
            ``"http://otel-collector.observability:4318"``), or ``None``/
            empty to stay a genuine no-op. ``/v1/traces`` is appended for
            the actual export path.
    """
    global _provider  # noqa: PLW0603 -- the documented module-owned-singleton pattern (see module docstring)
    if not otlp_endpoint:
        _provider = trace.NoOpTracerProvider()
        return
    provider = TracerProvider()
    exporter = OTLPSpanExporter(endpoint=f"{otlp_endpoint}/v1/traces")
    provider.add_span_processor(BatchSpanProcessor(exporter))
    _provider = provider


@contextlib.contextmanager
def start_span(name: str) -> Iterator[None]:
    """Start a span nested under whatever context is currently active.

    No-op (non-recording, ``contextvars``-transparent) until ``configure()``
    is called with a real ``otlp_endpoint``.

    Args:
        name: Span name, e.g. ``"pipeline.run_streaming.chunk"``.

    Yields:
        Nothing -- callers never bind ``as x``. The created span becomes
        the active/current span for the duration of the ``with`` block (via
        ``start_as_current_span()``), so a nested call site sees it through
        ``opentelemetry.trace.get_current_span()`` without this function
        returning anything itself.
    """
    with _provider.get_tracer(__name__).start_as_current_span(name):
        yield None


def flush(timeout_millis: int = 5000) -> None:
    """Force-flush any buffered spans. No-op-safe when unconfigured.

    ``opentelemetry.trace.TracerProvider`` (the API base class) declares no
    ``force_flush`` -- only the real SDK provider does -- so this reaches for
    it defensively rather than assuming it exists, matching the module's own
    no-op-when-unconfigured posture.

    Args:
        timeout_millis: Milliseconds to wait for the flush to complete.
            Defaults to ``5000``.
    """
    force_flush = getattr(_provider, "force_flush", None)
    if callable(force_flush):
        force_flush(timeout_millis)
