"""Integration test: ``metrics.increment()`` genuinely reaches an OTLP/HTTP receiver -- OBS-08.

No ``dataplat`` mocking anywhere in this file -- this is the genuine
wire-delivery proof 07-RESEARCH.md's Test Map names. A minimal fake OTLP/HTTP
receiver (``http.server.ThreadingHTTPServer``, OS-assigned loopback port)
decodes the exact bytes ``OTLPMetricExporter`` sends over the wire via the
real ``opentelemetry.proto.collector.metrics.v1.metrics_service_pb2``
protobuf message -- the same module ``opentelemetry-exporter-otlp-proto-http``
itself uses to serialize, already installed as one of its own transitive
dependencies (07-02-PLAN.md Task 1).

Marked ``@pytest.mark.integration`` (matching ``tests/property/
test_determinism.py``'s established convention, per 06-17-PLAN.md) so it
runs under ``pytest -m integration``, excluded from the fast offline gate --
even though this particular test needs no Docker daemon (a loopback HTTP
socket, not a container), it belongs in the same heavier tier as the rest of
``tests/integration/``, not ``tests/unit/``.
"""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING

import pytest
from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2

from dataplat.observability import metrics

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration


class _CapturingOtlpHandler(BaseHTTPRequestHandler):
    """Minimal fake OTLP/HTTP receiver: decodes each ``/v1/metrics`` POST body as real protobuf."""

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        if self.path == "/v1/metrics":
            request = metrics_service_pb2.ExportMetricsServiceRequest.FromString(body)
            server: _CapturingServer = self.server  # type: ignore[assignment]
            with server.lock:
                server.captured.append(request)
        self.send_response(200)
        self.send_header("Content-Type", "application/x-protobuf")
        self.end_headers()

    def log_message(self, log_format: str, *args: object) -> None:
        """Silence ``BaseHTTPRequestHandler``'s default per-request stderr logging."""
        del log_format, args


class _CapturingServer(ThreadingHTTPServer):
    """A ``ThreadingHTTPServer`` carrying the thread-safe capture list its handler appends to."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]
        self.captured: list[metrics_service_pb2.ExportMetricsServiceRequest] = []
        self.lock = threading.Lock()


@pytest.fixture(autouse=True)
def _reset_metrics_after_test() -> Iterator[None]:
    """Reset ``metrics`` back to a genuine no-op after this test.

    Restores a clean baseline for whatever else runs later in the same
    ``pytest tests/integration -q`` session (``make test-integration``
    collects this whole directory together, not this file in isolation).
    """
    yield
    metrics.configure(otlp_endpoint=None)


@pytest.fixture
def otlp_receiver() -> Iterator[tuple[str, list[metrics_service_pb2.ExportMetricsServiceRequest]]]:
    """Start a real, loopback-bound fake OTLP/HTTP receiver for the duration of one test.

    Yields:
        ``(endpoint, captured)`` -- ``endpoint`` is this receiver's base URL
        (``metrics.configure(otlp_endpoint=...)``'s own argument shape, e.g.
        ``"http://127.0.0.1:54321"``); ``captured`` is the thread-safe list
        every decoded ``ExportMetricsServiceRequest`` this receiver has seen
        is appended to, live, for the test to inspect after ``flush()``.
    """
    server = _CapturingServer(("127.0.0.1", 0), _CapturingOtlpHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_address[1]
    try:
        yield f"http://127.0.0.1:{port}", server.captured
    finally:
        server.shutdown()
        thread.join(timeout=5)


def test_increment_reaches_a_real_otlp_http_receiver_with_the_d04_label_set(
    otlp_receiver: tuple[str, list[metrics_service_pb2.ExportMetricsServiceRequest]],
) -> None:
    """OBS-08: a real, labeled ``increment()`` call is observed on the wire, nothing wider."""
    endpoint, captured = otlp_receiver

    metrics.configure(otlp_endpoint=endpoint)
    metrics.increment(
        "rows_rejected",
        3,
        dataset="customers",
        stage="ragged_row_guard",
        status="rejected",
    )
    metrics.flush()
    # Unregister the atexit shutdown hook while the fake receiver is still
    # alive (server.shutdown() happens in otlp_receiver's own teardown,
    # after this test function returns) -- otherwise a second, unwanted
    # export attempt fires later against an already-dead socket.
    # `shutdown()` itself performs one more internal flush, so the receiver
    # legitimately sees "exactly one (or at least one)" identical export --
    # the plan's own acceptance criteria wording for this exact scenario.
    metrics._provider.shutdown()  # noqa: SLF001 -- see module docstring / test_metrics.py's identical pattern

    assert len(captured) >= 1

    data_points = [
        data_point
        for request in captured
        for resource_metrics in request.resource_metrics
        for scope_metrics in resource_metrics.scope_metrics
        for metric in scope_metrics.metrics
        if metric.name == "rows_rejected"
        for data_point in metric.sum.data_points
    ]
    assert len(data_points) >= 1

    # Every captured data point must agree -- not just the first -- so a
    # stray divergent duplicate (e.g. a second increment() leaking in from
    # another test) would still fail this test.
    for data_point in data_points:
        assert data_point.as_int == 3

        attributes = {kv.key: kv.value.string_value for kv in data_point.attributes}
        assert attributes == {
            "dataset": "customers",
            "stage": "ragged_row_guard",
            "status": "rejected",
        }
        # The bounded label set is exactly what reached the wire -- no
        # fourth key, never an unbounded identity like
        # run_id/file_id/batch_id (D-04, T-07-05's permanent regression
        # guard against a future call site silently widening the set).
        assert set(attributes) == {"dataset", "stage", "status"}
