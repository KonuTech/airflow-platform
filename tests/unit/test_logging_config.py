"""Unit tests for ``dataplat.observability`` — OBS-02/04 and the D-03 no-op seams.

Covers: the dual JSON/console renderer (OBS-02), contextvars propagation across
independent log calls (OBS-04), and the no-op ``metrics``/``tracing`` call
sites (D-03). Secret redaction (OBS-05) and its end-to-end pairing with
``resolve_secret()`` (SEC-15) are proven separately in
``test_logging_redaction.py`` — not repeated here.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
import structlog

from dataplat.observability import logging as dataplat_logging
from dataplat.observability import metrics, tracing

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture(autouse=True)
def _clear_bound_context() -> Iterator[None]:
    """Reset bound contextvars after every test.

    contextvars are process-global within a thread, so a bind left over from
    one test would otherwise leak into the next.
    """
    yield
    structlog.contextvars.clear_contextvars()


def test_configure_in_cluster_emits_valid_json_with_event_key(
    capsys: pytest.CaptureFixture[str],
) -> None:
    dataplat_logging.configure(in_cluster=True)
    log = dataplat_logging.get_logger()

    log.info("something happened", widget="foo")

    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["event"] == "something happened"
    assert payload["widget"] == "foo"


def test_configure_local_emits_non_json_console_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    dataplat_logging.configure(in_cluster=False)
    log = dataplat_logging.get_logger()

    log.info("something happened", widget="foo")

    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert len(lines) == 1
    with pytest.raises(json.JSONDecodeError):
        json.loads(lines[0])
    assert "something happened" in lines[0]


def test_bound_contextvars_appear_on_every_subsequent_log_call(
    capsys: pytest.CaptureFixture[str],
) -> None:
    dataplat_logging.configure(in_cluster=True)
    dataplat_logging.bind_contextvars(dataset="customers", run_id=42)
    log = dataplat_logging.get_logger()

    log.info("first event")
    log.info("second event")

    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert len(lines) == 2
    for line in lines:
        payload = json.loads(line)
        assert payload["dataset"] == "customers"
        assert payload["run_id"] == 42
        # Neither call above passed these keys itself -- they came from context.
        assert "event" in payload


def test_clear_contextvars_removes_previously_bound_fields(
    capsys: pytest.CaptureFixture[str],
) -> None:
    dataplat_logging.configure(in_cluster=True)
    dataplat_logging.bind_contextvars(dataset="customers")

    dataplat_logging.clear_contextvars()
    log = dataplat_logging.get_logger()
    log.info("after clear")

    payload = json.loads(capsys.readouterr().out.strip())
    assert "dataset" not in payload


def test_metrics_increment_and_tracing_start_span_are_real_no_ops() -> None:
    assert metrics.increment("rows_loaded", 5) is None
    assert metrics.increment("rows_loaded") is None  # default value=1
    assert metrics.increment("rows_loaded", 5, dataset="customers") is None  # **labels

    with tracing.start_span("stage") as span:
        assert span is None
