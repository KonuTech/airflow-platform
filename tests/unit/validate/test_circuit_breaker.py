"""Unit tests for ``dataplat.validate.circuit_breaker.RejectionRateCircuitBreaker``.

Proves D-10's threshold arithmetic in isolation, before plan 08-11 wires it
into a live publication transaction.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dataplat.errors import QualityThresholdExceeded
from dataplat.models.identity import RunContext
from dataplat.pipeline.protocol import PipelineContext
from dataplat.validate.circuit_breaker import RejectionRateCircuitBreaker


def _make_context() -> PipelineContext:
    """Build a placeholder ``PipelineContext`` -- unused by this stage's ``apply()``."""
    return PipelineContext(
        run=RunContext(run_id=1, idempotency_key="test-run"),
        config=SimpleNamespace(dataset="test_dataset"),  # type: ignore[arg-type]
        metadata=None,  # type: ignore[arg-type]
        objects=None,  # type: ignore[arg-type]
        db=None,  # type: ignore[arg-type]
        log=None,  # type: ignore[arg-type]
    )


def test_a_breach_raises_quality_threshold_exceeded_with_ratio_and_threshold_in_context() -> None:
    breaker = RejectionRateCircuitBreaker(
        threshold=0.10, total_rows_read=100, total_rows_rejected=15
    )

    with pytest.raises(QualityThresholdExceeded) as exc_info:
        breaker.apply(_make_context())

    assert exc_info.value.context["observed_ratio"] == pytest.approx(0.15)
    assert exc_info.value.context["threshold"] == 0.10


def test_an_under_threshold_run_does_not_raise_and_returns_a_pass_finding() -> None:
    breaker = RejectionRateCircuitBreaker(
        threshold=0.10, total_rows_read=100, total_rows_rejected=5
    )

    result = breaker.apply(_make_context())

    assert len(result.findings) == 1
    finding = result.findings[0]
    assert finding.rule_type == "QUALITY"
    assert finding.outcome == "PASS"
    assert finding.evaluated_count == 100
    assert finding.failed_count == 5


def test_a_ratio_exactly_at_threshold_does_not_raise() -> None:
    breaker = RejectionRateCircuitBreaker(
        threshold=0.10, total_rows_read=100, total_rows_rejected=10
    )

    result = breaker.apply(_make_context())

    assert result.findings[0].outcome == "PASS"


def test_zero_rows_read_never_raises_a_division_by_zero_or_any_other_error() -> None:
    breaker = RejectionRateCircuitBreaker(threshold=0.10, total_rows_read=0, total_rows_rejected=0)

    result = breaker.apply(_make_context())

    assert result.findings[0].outcome == "PASS"
