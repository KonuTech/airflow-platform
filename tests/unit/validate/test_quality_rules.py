"""Unit tests for ``dataplat.validate``'s row-scoped quality rules.

Covers ``CompletenessRule`` (empty-value rejection on a required column),
``ValidityRangeRule`` (numeric-bounds rejection, with unparseable values
distinguished from out-of-range ones) and ``PatternRule`` (regex-match
rejection) -- VALID-02's DoD-mandated minimum rule set, minus uniqueness and
referential integrity (later plans).

Every ``ctx: PipelineContext`` passed below is a placeholder built by
``_make_context()``, mirroring ``tests/unit/test_pipeline_errors.py``'s own
helper: only ``config.dataset`` (D-04's metric label) is populated with a
real value.
"""

from __future__ import annotations

from types import SimpleNamespace

from dataplat.models.identity import RunContext
from dataplat.models.record import RecordChunk
from dataplat.pipeline.protocol import PipelineContext
from dataplat.validate.completeness import CompletenessRule
from dataplat.validate.pattern import PatternRule
from dataplat.validate.validity_range import ValidityRangeRule


def _make_context() -> PipelineContext:
    """Build a placeholder ``PipelineContext`` -- only ``run``/``config.dataset`` are real."""
    return PipelineContext(
        run=RunContext(run_id=1, idempotency_key="test-run"),
        config=SimpleNamespace(dataset="test_dataset"),  # type: ignore[arg-type] -- only .dataset is read
        metadata=None,  # type: ignore[arg-type] -- unused by the code under test
        objects=None,  # type: ignore[arg-type] -- unused by the code under test
        db=None,  # type: ignore[arg-type] -- unused by the code under test
        log=None,  # type: ignore[arg-type] -- unused by the code under test
    )


def _chunk(
    rows: list[tuple[str | bool | None, ...]],
    *,
    first_ordinal: int = 0,
    expected_field_count: int = 2,
) -> RecordChunk:
    return RecordChunk(
        rows=tuple(rows),
        first_ordinal=first_ordinal,
        expected_field_count=expected_field_count,
    )


# --- CompletenessRule --------------------------------------------------------


def test_completeness_rule_rejects_an_empty_string_value() -> None:
    chunk = _chunk([("1", "")], first_ordinal=10)
    rule = CompletenessRule(
        column_index=1, column_name="name", strategy="REJECT_RECORD", rule_id="r1"
    )

    result = rule.apply(_make_context(), chunk)

    assert result.chunk.rows == ()
    assert len(result.rejected) == 1
    rejected = result.rejected[0]
    assert rejected.error_type == "COMPLETENESS_VIOLATION"
    assert rejected.error_column == "name"
    assert rejected.source_row_number == 10


def test_completeness_rule_rejects_a_none_value() -> None:
    chunk = _chunk([("1", None)])
    rule = CompletenessRule(
        column_index=1, column_name="name", strategy="REJECT_RECORD", rule_id="r1"
    )

    result = rule.apply(_make_context(), chunk)

    assert result.chunk.rows == ()
    assert result.rejected[0].error_type == "COMPLETENESS_VIOLATION"


def test_completeness_rule_keeps_a_non_empty_value_unchanged() -> None:
    chunk = _chunk([("1", "Alice")])
    rule = CompletenessRule(
        column_index=1, column_name="name", strategy="REJECT_RECORD", rule_id="r1"
    )

    result = rule.apply(_make_context(), chunk)

    assert result.chunk.rows == (("1", "Alice"),)
    assert result.rejected == []


def test_completeness_rule_never_raises_and_accounts_for_every_row() -> None:
    chunk = _chunk([("1", "Alice"), ("2", ""), ("3", None), ("4", "Bob")])
    rule = CompletenessRule(
        column_index=1, column_name="name", strategy="REJECT_RECORD", rule_id="r1"
    )

    result = rule.apply(_make_context(), chunk)

    assert len(result.chunk.rows) + len(result.rejected) == len(chunk.rows)


# --- ValidityRangeRule -------------------------------------------------------


def test_validity_range_rule_rejects_a_value_below_minimum() -> None:
    chunk = _chunk([("1", "-5")])
    rule = ValidityRangeRule(
        column_index=1,
        column_name="amount",
        strategy="REJECT_RECORD",
        rule_id="r2",
        minimum=0,
        maximum=1000,
    )

    result = rule.apply(_make_context(), chunk)

    assert result.chunk.rows == ()
    assert result.rejected[0].error_type == "VALIDITY_RANGE_VIOLATION"


def test_validity_range_rule_rejects_a_value_above_maximum() -> None:
    chunk = _chunk([("1", "1001")])
    rule = ValidityRangeRule(
        column_index=1,
        column_name="amount",
        strategy="REJECT_RECORD",
        rule_id="r2",
        minimum=0,
        maximum=1000,
    )

    result = rule.apply(_make_context(), chunk)

    assert result.chunk.rows == ()
    assert result.rejected[0].error_type == "VALIDITY_RANGE_VIOLATION"


def test_validity_range_rule_keeps_a_value_inside_both_bound_edges() -> None:
    chunk = _chunk([("1", "0"), ("2", "1000"), ("3", "500")])
    rule = ValidityRangeRule(
        column_index=1,
        column_name="amount",
        strategy="REJECT_RECORD",
        rule_id="r2",
        minimum=0,
        maximum=1000,
    )

    result = rule.apply(_make_context(), chunk)

    assert result.chunk.rows == chunk.rows
    assert result.rejected == []


def test_validity_range_rule_rejects_an_unparseable_value_distinctly_from_out_of_range() -> None:
    chunk = _chunk([("1", "not-a-number")])
    rule = ValidityRangeRule(
        column_index=1,
        column_name="amount",
        strategy="REJECT_RECORD",
        rule_id="r2",
        minimum=0,
        maximum=1000,
    )

    result = rule.apply(_make_context(), chunk)

    assert result.chunk.rows == ()
    assert result.rejected[0].error_type == "VALIDITY_RANGE_UNPARSEABLE"


def test_validity_range_rule_rejects_a_none_value_as_unparseable() -> None:
    chunk = _chunk([("1", None)])
    rule = ValidityRangeRule(
        column_index=1,
        column_name="amount",
        strategy="REJECT_RECORD",
        rule_id="r2",
        minimum=0,
        maximum=1000,
    )

    result = rule.apply(_make_context(), chunk)

    assert result.chunk.rows == ()
    assert result.rejected[0].error_type == "VALIDITY_RANGE_UNPARSEABLE"


def test_validity_range_rule_never_raises_and_accounts_for_every_row() -> None:
    chunk = _chunk([("1", "500"), ("2", "not-a-number"), ("3", "-1"), ("4", "1001")])
    rule = ValidityRangeRule(
        column_index=1,
        column_name="amount",
        strategy="REJECT_RECORD",
        rule_id="r2",
        minimum=0,
        maximum=1000,
    )

    result = rule.apply(_make_context(), chunk)

    assert len(result.chunk.rows) + len(result.rejected) == len(chunk.rows)


# --- PatternRule --------------------------------------------------------------


def test_pattern_rule_keeps_a_value_matching_the_full_pattern() -> None:
    chunk = _chunk([("1", "AB")])
    rule = PatternRule(
        column_index=1,
        column_name="code",
        strategy="REJECT_RECORD",
        rule_id="r3",
        pattern=r"^[A-Z]{2}$",
    )

    result = rule.apply(_make_context(), chunk)

    assert result.chunk.rows == (("1", "AB"),)
    assert result.rejected == []


def test_pattern_rule_rejects_a_non_matching_value() -> None:
    chunk = _chunk([("1", "abc")], first_ordinal=3)
    rule = PatternRule(
        column_index=1,
        column_name="code",
        strategy="REJECT_RECORD",
        rule_id="r3",
        pattern=r"^[A-Z]{2}$",
    )

    result = rule.apply(_make_context(), chunk)

    assert result.chunk.rows == ()
    assert len(result.rejected) == 1
    rejected = result.rejected[0]
    assert rejected.error_type == "PATTERN_VIOLATION"
    assert rejected.error_column == "code"
    assert rejected.source_row_number == 3


def test_pattern_rule_rejects_a_partial_match_since_fullmatch_is_required() -> None:
    # "ABC" contains a leading "AB" match but fullmatch requires the ENTIRE
    # value to match -- proves re.fullmatch, not re.match/re.search, is used.
    chunk = _chunk([("1", "ABC")])
    rule = PatternRule(
        column_index=1,
        column_name="code",
        strategy="REJECT_RECORD",
        rule_id="r3",
        pattern=r"^[A-Z]{2}$",
    )

    result = rule.apply(_make_context(), chunk)

    assert result.chunk.rows == ()
    assert result.rejected[0].error_type == "PATTERN_VIOLATION"


def test_pattern_rule_never_raises_and_accounts_for_every_row() -> None:
    chunk = _chunk([("1", "AB"), ("2", "abc"), ("3", None)])
    rule = PatternRule(
        column_index=1,
        column_name="code",
        strategy="REJECT_RECORD",
        rule_id="r3",
        pattern=r"^[A-Z]{2}$",
    )

    result = rule.apply(_make_context(), chunk)

    assert len(result.chunk.rows) + len(result.rejected) == len(chunk.rows)
