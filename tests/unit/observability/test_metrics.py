"""Unit tests for ``dataplat.observability.metrics`` -- Task 1's real-backend proof.

Covers both of ``configure()``'s postures: a genuine no-op when
``otlp_endpoint`` is ``None`` (the exporter class is never even instantiated)
and a real, wired ``MeterProvider`` when given one (an exporter's
``export()`` is genuinely invoked, carrying exactly what ``increment()`` was
called with). Wire-level OTLP/HTTP delivery -- decoding the actual protobuf
bytes a real receiver gets -- is ``tests/integration/test_metrics_otlp.py``'s
job, not this file's: every "configured" test here monkeypatches
``OTLPMetricExporter.export()`` itself, so no real network socket is ever
opened (fast, offline, ``tests/unit``-tier).

Every test that configures a real endpoint explicitly shuts the provider
down again, inside the test body, before returning -- ``MeterProvider``
registers an ``atexit`` hook (``shutdown_on_exit=True`` is the SDK default)
that would otherwise fire *after* ``monkeypatch`` has already reverted
``OTLPMetricExporter.export()`` back to the real implementation, attempting
a genuine (slow, doomed) network call against ``127.0.0.1:1`` at interpreter
exit. ``MeterProvider.shutdown()`` unregisters that hook.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from opentelemetry.sdk.metrics.export import MetricExportResult

from dataplat.observability import metrics

if TYPE_CHECKING:
    from collections.abc import Iterator

    from opentelemetry.sdk.metrics.export import MetricsData


@pytest.fixture(autouse=True)
def _reset_after_test() -> Iterator[None]:
    """Reset ``metrics`` back to a genuine no-op after every test.

    ``configure()``'s module-owned singleton (``metrics.py``'s module
    docstring) has no built-in per-test isolation -- a test that configures
    a real provider must not leak that state into a sibling test in this
    file, in ``test_tracing.py``, or in
    ``tests/unit/test_logging_config.py``'s own no-op assertions.
    """
    yield
    metrics.configure(otlp_endpoint=None)


def test_increment_is_a_genuine_noop_when_unconfigured(monkeypatch: pytest.MonkeyPatch) -> None:
    exporter_touched = False

    class _SpyExporter:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            nonlocal exporter_touched
            exporter_touched = True

    monkeypatch.setattr(metrics, "OTLPMetricExporter", _SpyExporter)

    metrics.configure(otlp_endpoint=None)
    result = metrics.increment("x", 1)

    assert result is None
    assert exporter_touched is False, "configure(None) must never construct OTLPMetricExporter"


def test_increment_reaches_the_configured_exporter(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[MetricsData] = []

    def _fake_export(
        self: object,
        metrics_data: MetricsData,
        timeout_millis: float | None = None,
        **_kwargs: object,
    ) -> MetricExportResult:
        del self, timeout_millis
        calls.append(metrics_data)
        return MetricExportResult.SUCCESS

    monkeypatch.setattr(metrics.OTLPMetricExporter, "export", _fake_export)

    metrics.configure(otlp_endpoint="http://127.0.0.1:1")
    metrics.increment(
        "rows_rejected",
        3,
        dataset="customers",
        stage="ragged_row_guard",
        status="rejected",
    )
    metrics.flush()

    assert len(calls) >= 1
    data_points = [
        dp
        for rm in calls[-1].resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name == "rows_rejected"
        for dp in m.data.data_points
    ]
    assert len(data_points) == 1
    assert data_points[0].value == 3
    assert dict(data_points[0].attributes) == {
        "dataset": "customers",
        "stage": "ragged_row_guard",
        "status": "rejected",
    }

    metrics._provider.shutdown()  # noqa: SLF001 -- unregister the atexit hook before monkeypatch reverts


def test_increment_caches_a_counter_by_name_across_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    """A second ``increment()`` call for the same name reuses one ``Counter`` (Task 1's action).

    Spies on the current provider *instance*'s ``get_meter`` (not the
    class) -- ``increment()`` only reaches ``get_meter().create_counter()``
    on a cache miss, so counting ``get_meter()`` calls proves the cache
    actually skips it on the second and third call for the same name.
    """
    metrics.configure(otlp_endpoint=None)
    real_get_meter = metrics._provider.get_meter  # noqa: SLF001 -- white-box test of this module's own cache
    calls = 0

    def _spy_get_meter(*args: object, **kwargs: object) -> object:
        nonlocal calls
        calls += 1
        return real_get_meter(*args, **kwargs)

    monkeypatch.setattr(metrics._provider, "get_meter", _spy_get_meter)  # noqa: SLF001

    metrics.increment("rows_kept", 1)
    metrics.increment("rows_kept", 2)
    metrics.increment("rows_kept", 3)

    assert calls == 1, "get_meter()/create_counter() must run once, not once per increment() call"


def test_flush_is_safe_when_unconfigured() -> None:
    metrics.configure(otlp_endpoint=None)
    metrics.flush()  # must not raise
