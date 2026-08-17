"""Unit tests for ``dataplat.validate.strategy_dispatch.StrategyDispatchStage``.

This is VALID-03's own named proof (``08-VALIDATION.md``'s Per-Task
Verification Map): every one of D-07's 5 strategy values
(``REJECT_RECORD``/``QUARANTINE_RECORD``/``WARN_AND_CONTINUE``/``FAIL_FILE``/
``QUARANTINE_FILE``) appears here with an explicit, distinguishing
assertion, proving ``StrategyDispatchStage`` genuinely changes a rule's
outcome, not just its label.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dataplat.errors import ConfigurationError, QualityThresholdExceeded
from dataplat.models.identity import RunContext
from dataplat.models.record import RecordChunk
from dataplat.pipeline.protocol import PipelineContext
from dataplat.validate.completeness import CompletenessRule
from dataplat.validate.strategy_dispatch import StrategyDispatchStage

_RULE_ID = "name_not_empty"
_RULE_TYPE = "QUALITY_COMPLETENESS"


def _make_context() -> PipelineContext:
    """Build a placeholder ``PipelineContext``.

    Mirrors ``test_circuit_breaker.py``'s own convention.
    """
    return PipelineContext(
        run=RunContext(run_id=1, idempotency_key="test-run"),
        config=SimpleNamespace(dataset="test_dataset"),  # type: ignore[arg-type]
        metadata=None,  # type: ignore[arg-type]
        objects=None,  # type: ignore[arg-type]
        db=None,  # type: ignore[arg-type]
        log=None,  # type: ignore[arg-type]
    )


def _make_chunk() -> RecordChunk:
    """A fixed 3-row chunk, column 1 (``name``), with exactly 1 known violation (row 2, empty)."""
    return RecordChunk(
        rows=(
            ("1", "Alice"),
            ("2", ""),  # the one known violation -- empty required `name`
            ("3", "Carol"),
        ),
        first_ordinal=0,
        expected_field_count=2,
    )


def _make_inner_rule() -> CompletenessRule:
    return CompletenessRule(
        column_index=1, column_name="name", strategy="REJECT_RECORD", rule_id=_RULE_ID
    )


def test_reject_record_is_a_byte_identical_passthrough_of_the_inner_result() -> None:
    ctx = _make_context()
    chunk = _make_chunk()
    inner = _make_inner_rule()
    expected = inner.apply(ctx, _make_chunk())

    stage = StrategyDispatchStage(
        inner=_make_inner_rule(), strategy="REJECT_RECORD", rule_id=_RULE_ID, rule_type=_RULE_TYPE
    )
    result = stage.apply(ctx, chunk)

    assert result.chunk.rows == expected.chunk.rows
    assert len(result.rejected) == 1
    assert result.rejected[0].error_type == expected.rejected[0].error_type
    assert result.findings == expected.findings == []


def test_quarantine_record_is_identical_to_reject_record_passthrough() -> None:
    ctx = _make_context()
    chunk = _make_chunk()

    reject_stage = StrategyDispatchStage(
        inner=_make_inner_rule(), strategy="REJECT_RECORD", rule_id=_RULE_ID, rule_type=_RULE_TYPE
    )
    quarantine_stage = StrategyDispatchStage(
        inner=_make_inner_rule(),
        strategy="QUARANTINE_RECORD",
        rule_id=_RULE_ID,
        rule_type=_RULE_TYPE,
    )

    reject_result = reject_stage.apply(ctx, _make_chunk())
    quarantine_result = quarantine_stage.apply(ctx, chunk)

    assert quarantine_result.chunk.rows == reject_result.chunk.rows
    assert len(quarantine_result.rejected) == len(reject_result.rejected) == 1


def test_warn_and_continue_keeps_every_row_and_emits_one_warning_finding() -> None:
    ctx = _make_context()
    original_chunk = _make_chunk()

    stage = StrategyDispatchStage(
        inner=_make_inner_rule(),
        strategy="WARN_AND_CONTINUE",
        rule_id=_RULE_ID,
        rule_type=_RULE_TYPE,
    )
    result = stage.apply(ctx, original_chunk)

    # ALL 3 original rows kept -- the inner rule's own rejection is overridden.
    assert len(result.chunk.rows) == len(original_chunk.rows) == 3
    assert result.chunk.rows == original_chunk.rows
    assert result.rejected == []
    assert len(result.findings) == 1
    finding = result.findings[0]
    assert finding.outcome == "PASS_WITH_WARNING"
    assert finding.severity == "WARNING"
    assert finding.failed_count == 1
    assert finding.evaluated_count == 3
    assert finding.rule_id == _RULE_ID
    assert finding.rule_type == _RULE_TYPE


@pytest.mark.parametrize("strategy", ["FAIL_FILE", "QUARANTINE_FILE"])
def test_fail_file_and_quarantine_file_raise_quality_threshold_exceeded_with_context(
    strategy: str,
) -> None:
    ctx = _make_context()
    chunk = _make_chunk()

    stage = StrategyDispatchStage(
        inner=_make_inner_rule(), strategy=strategy, rule_id=_RULE_ID, rule_type=_RULE_TYPE
    )

    with pytest.raises(QualityThresholdExceeded) as exc_info:
        stage.apply(ctx, chunk)

    assert exc_info.value.context["rule_id"] == _RULE_ID
    assert exc_info.value.context["strategy"] == strategy
    assert exc_info.value.context["violation_count"] == 1


@pytest.mark.parametrize(
    "strategy",
    ["REJECT_RECORD", "QUARANTINE_RECORD", "WARN_AND_CONTINUE", "FAIL_FILE", "QUARANTINE_FILE"],
)
def test_zero_violations_never_raises_and_returns_inner_passthrough_for_every_strategy(
    strategy: str,
) -> None:
    ctx = _make_context()
    clean_chunk = RecordChunk(
        rows=(("1", "Alice"), ("2", "Bob"), ("3", "Carol")),
        first_ordinal=0,
        expected_field_count=2,
    )

    stage = StrategyDispatchStage(
        inner=_make_inner_rule(), strategy=strategy, rule_id=_RULE_ID, rule_type=_RULE_TYPE
    )
    result = stage.apply(ctx, clean_chunk)

    assert result.rejected == []
    assert len(result.chunk.rows) == 3
    assert result.findings == []


def test_an_unrecognized_strategy_raises_configuration_error_at_construction_time() -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        StrategyDispatchStage(
            inner=_make_inner_rule(),
            strategy="NOT_A_REAL_STRATEGY",
            rule_id=_RULE_ID,
            rule_type=_RULE_TYPE,
        )

    assert exc_info.value.context["strategy"] == "NOT_A_REAL_STRATEGY"
    assert exc_info.value.context["rule_id"] == _RULE_ID
