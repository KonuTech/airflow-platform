"""Real OTel-backed metrics -- a genuine no-op until ``configure()`` sees an endpoint.

``configure()`` builds one process-owned ``MeterProvider`` (CONTEXT.md D-01:
``dataplat`` metrics push via OTLP, not StatsD -- Airflow's own internal
metrics stay on the separate StatsD path, D-02): a real,
``PeriodicExportingMetricReader``-backed provider when given
``otlp_endpoint``, or ``opentelemetry.metrics``'s own ``NoOpMeterProvider``
when not -- genuinely inert (its ``Counter.add()`` does nothing: no timer
thread, no network attempt), not merely "configured to drop." ``increment()``
keeps its Phase-3 signature exactly (``metrics.increment(name, value,
**labels)``); no caller anywhere needs to change.

D-04's bounded label set (``dataset``/``stage``/``status``) is enforced
entirely by WHAT CALLERS PASS as ``**labels`` -- this module never filters or
rejects a label key itself (Task 2 is where the two real call sites in
``pipeline/engine.py`` are narrowed to exactly those three; T-07-05's threat
mitigation lives at the call site, by design, not here).

``configure()`` deliberately never calls ``opentelemetry.metrics.
set_meter_provider()`` -- see ``tracing.py``'s module docstring for the
identical set-once rationale (verified against the installed
``opentelemetry-api==1.44.0`` source: ``set_meter_provider`` "can only be
done once, a warning will be logged if any further attempt is made").
"""

from __future__ import annotations

from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

# The one process-owned provider this module manages -- see module docstring
# for why this is never registered via `opentelemetry.metrics.
# set_meter_provider()`. Starts as a genuine no-op so a process that never
# calls `configure()` at all behaves identically to one that explicitly
# calls `configure(otlp_endpoint=None)`.
_provider: metrics.MeterProvider = metrics.NoOpMeterProvider()

# Counters are cached by name, not recreated per `increment()` call -- but a
# cached `Counter` is bound to whichever `_provider` created it, so this
# cache is cleared on every `configure()` call (below) to avoid silently
# keeping a stale counter bound to a superseded provider/endpoint.
_counters: dict[str, metrics.Counter] = {}


def configure(*, otlp_endpoint: str | None) -> None:
    """Build this process's one ``MeterProvider`` -- real when given an endpoint.

    Safely re-callable: each call replaces the module-owned provider and
    clears the per-name counter cache (see module-level ``_counters``
    comment for why the cache must not survive reconfiguration).

    Args:
        otlp_endpoint: The OTLP/HTTP collector's base URL (e.g.
            ``"http://otel-collector.observability:4318"``), or ``None``/
            empty to stay a genuine no-op. ``/v1/metrics`` is appended for
            the actual export path.
    """
    global _provider, _counters  # noqa: PLW0603 -- the documented module-owned-singleton pattern (see module docstring)
    if not otlp_endpoint:
        _provider = metrics.NoOpMeterProvider()
    else:
        exporter = OTLPMetricExporter(endpoint=f"{otlp_endpoint}/v1/metrics")
        reader = PeriodicExportingMetricReader(exporter)
        _provider = MeterProvider(metric_readers=[reader])
    _counters = {}


def increment(name: str, value: int = 1, **labels: str) -> None:
    """Record a counter increment. No-op-safe until ``configure()`` is called.

    Args:
        name: Metric name, e.g. ``"rows_rejected"``.
        value: Amount to increment by. Defaults to ``1``.
        labels: Metric labels/dimensions, e.g. ``dataset="customers"``.
            Passed straight through as the counter's attributes -- this
            module never filters or bounds the label set itself (module
            docstring).
    """
    counter = _counters.get(name)
    if counter is None:
        counter = _provider.get_meter(__name__).create_counter(name)
        _counters[name] = counter
    counter.add(value, attributes=labels)


def flush(timeout_millis: int = 5000) -> None:
    """Force-flush any buffered metrics. No-op-safe when unconfigured.

    ``opentelemetry.metrics.MeterProvider`` (the API base class) declares no
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
