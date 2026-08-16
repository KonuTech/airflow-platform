"""Unit tests for ``dataplat.cli``'s TRACEPARENT extraction (plan 07-05 Task 1).

Covers ``main()``'s new "once, near the top, before dispatching" setup: an
incoming W3C ``TRACEPARENT`` env var is extracted into the active OTel
context before ``entry_points(group="dataplat.plugins")`` ever loads, and
both the ``tracing``/``metrics`` observability backends are configured at
the same point. All three tests call ``main()`` directly (not
``CliRunner.invoke()``), matching ``test_cli_error_handling.py``'s own
precedent -- ``main()``'s setup code runs before ``cli.main(...)`` dispatch
regardless of which subcommand follows, so ``main(["--version"])`` is enough
to exercise it while still returning a clean, non-raising exit.

Every test resets the process-global OTel context after itself:
``_extract_incoming_trace_context()`` deliberately never detaches its own
``opentelemetry.context.attach()`` call (by design -- it runs once per
process, for the process's whole remaining lifetime), so this file's own
fixture attaches a pristine sentinel context before each test and detaches
it after. This restores the pristine pre-test state even though ``main()``'s
own attach is never detached in between: ``contextvars.Token.reset()``
resets the raw variable value, not a call-order stack pointer (verified
directly against the installed ``contextvars``/``opentelemetry`` behavior
during this test file's own development).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from opentelemetry import context as otel_context
from opentelemetry import trace as otel_trace

from dataplat.cli import main
from dataplat.observability import metrics, tracing

if TYPE_CHECKING:
    from collections.abc import Iterator

_WELL_FORMED_TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
_INJECTED_TRACE_ID_HEX = "0af7651916cd43dd8448eb211c80319c"


@pytest.fixture(autouse=True)
def _isolate_otel_state(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Isolate every test in this file from ``main()``'s own never-detached attach.

    Also keeps ``OTEL_EXPORTER_OTLP_ENDPOINT`` unset for every test here, so
    ``main()``'s new ``tracing.configure()``/``metrics.configure()`` calls
    stay genuine no-ops -- this file only proves TRACEPARENT extraction, not
    the OTLP backend wiring plan 07-02 already covers.
    """
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    token = otel_context.attach(otel_context.Context())
    yield
    otel_context.detach(token)
    tracing.configure(otlp_endpoint=None)
    metrics.configure(otlp_endpoint=None)


def test_main_leaves_context_unchanged_when_traceparent_is_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("TRACEPARENT", raising=False)

    exit_code = main(["--version"])

    assert exit_code == 0
    assert otel_trace.get_current_span().get_span_context().is_valid is False


def test_main_extracts_a_well_formed_traceparent_into_the_active_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRACEPARENT", _WELL_FORMED_TRACEPARENT)

    exit_code = main(["--version"])

    assert exit_code == 0
    span_context = otel_trace.get_current_span().get_span_context()
    assert span_context.is_valid is True
    assert otel_trace.format_trace_id(span_context.trace_id) == _INJECTED_TRACE_ID_HEX


def test_main_does_not_raise_on_a_malformed_traceparent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRACEPARENT", "not-a-traceparent")

    exit_code = main(["--version"])  # must not raise

    assert exit_code == 0
    assert otel_trace.get_current_span().get_span_context().is_valid is False
