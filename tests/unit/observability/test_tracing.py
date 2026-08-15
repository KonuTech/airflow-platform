"""Unit tests for ``dataplat.observability.tracing`` -- Task 1's real-backend proof.

Covers both of ``configure()``'s postures: a genuine non-recording no-op
when ``otlp_endpoint`` is ``None`` (``is_recording() is False``, an invalid
span context -- not merely "configured to drop") and a real, valid,
recording span when given one. Every "configured" test here monkeypatches
``OTLPSpanExporter.export()`` itself, so no real network socket is ever
opened (fast, offline, ``tests/unit``-tier).

Every test that configures a real endpoint explicitly shuts the provider
down again, inside the test body, before returning -- see
``test_metrics.py``'s module docstring for why (``TracerProvider`` has the
identical ``atexit``/``shutdown_on_exit=True`` behavior as ``MeterProvider``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace.export import SpanExportResult

from dataplat.observability import tracing

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from opentelemetry.sdk.trace import ReadableSpan


@pytest.fixture(autouse=True)
def _reset_after_test() -> Iterator[None]:
    """Reset ``tracing`` back to a genuine no-op after every test.

    ``configure()``'s module-owned singleton (``tracing.py``'s module
    docstring) has no built-in per-test isolation -- a test that configures
    a real provider must not leak that state into a sibling test in this
    file, in ``test_metrics.py``, or in
    ``tests/unit/test_logging_config.py``'s own no-op assertions.
    """
    yield
    tracing.configure(otlp_endpoint=None)


def _patch_span_exporter(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace ``OTLPSpanExporter.export`` with a no-network stand-in for this test."""

    def _fake_export(self: object, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        del self, spans
        return SpanExportResult.SUCCESS

    monkeypatch.setattr(tracing.OTLPSpanExporter, "export", _fake_export)


def test_start_span_is_a_genuine_noop_when_unconfigured() -> None:
    tracing.configure(otlp_endpoint=None)

    with tracing.start_span("x"):
        span = trace.get_current_span()
        assert span.is_recording() is False
        assert span.get_span_context().is_valid is False


def test_start_span_is_real_when_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_span_exporter(monkeypatch)

    tracing.configure(otlp_endpoint="http://127.0.0.1:1")

    with tracing.start_span("x"):
        span = trace.get_current_span()
        assert span.is_recording() is True
        assert span.get_span_context().is_valid is True

    tracing._provider.shutdown()  # noqa: SLF001 -- unregister the atexit hook before monkeypatch reverts


def test_start_span_yields_none_so_callers_never_bind_a_value() -> None:
    tracing.configure(otlp_endpoint=None)

    with tracing.start_span("x") as value:
        assert value is None


def test_configure_is_safely_re_callable_within_one_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression guard for this module's own module-docstring rationale.

    ``opentelemetry.trace.set_tracer_provider()`` can only succeed once per
    process -- this proves ``configure()`` never routes through it (this
    module owns its provider directly instead) by configuring twice, in
    both directions, within one test and observing each takes effect.
    """
    _patch_span_exporter(monkeypatch)

    tracing.configure(otlp_endpoint="http://127.0.0.1:1")
    with tracing.start_span("x"):
        assert trace.get_current_span().is_recording() is True
    tracing._provider.shutdown()  # noqa: SLF001 -- unregister the atexit hook before monkeypatch reverts

    tracing.configure(otlp_endpoint=None)
    with tracing.start_span("y"):
        assert trace.get_current_span().is_recording() is False


def test_flush_is_safe_when_unconfigured() -> None:
    tracing.configure(otlp_endpoint=None)
    tracing.flush()  # must not raise
