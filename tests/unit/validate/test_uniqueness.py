"""Unit tests for ``dataplat.validate.uniqueness.UniquenessRule``.

Proves the within-chunk business-key uniqueness detection logic in
isolation, ready for plan 08-10 to wire into ``StagingLoader`` as pure
plumbing.

Every ``ctx: PipelineContext`` passed below is a placeholder built by
``_make_context()``, mirroring ``tests/unit/validate/test_quality_rules.py``'s
own helper: only ``config.dataset`` (D-04's metric label) is populated with a
real value.
"""

from __future__ import annotations

from types import SimpleNamespace

from dataplat.models.identity import RunContext
from dataplat.models.record import RecordChunk
from dataplat.pipeline.protocol import PipelineContext
from dataplat.validate.uniqueness import UniquenessRule


def _make_context() -> PipelineContext:
    """Build a placeholder ``PipelineContext`` -- only ``run``/``config.dataset`` are real."""
    return PipelineContext(
        run=RunContext(run_id=1, idempotency_key="test-run"),
        config=SimpleNamespace(dataset="test_dataset"),  # type: ignore[arg-type]
        metadata=None,  # type: ignore[arg-type]
        objects=None,  # type: ignore[arg-type]
        db=None,  # type: ignore[arg-type]
        log=None,  # type: ignore[arg-type]
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


def test_the_first_occurrence_is_kept_and_the_second_is_rejected() -> None:
    chunk = _chunk([("cust-1", "Alice"), ("cust-1", "Alice Duplicate")])
    rule = UniquenessRule(
        column_index=0, column_name="customer_id", strategy="REJECT_RECORD", rule_id="r4"
    )

    result = rule.apply(_make_context(), chunk)

    assert result.chunk.rows == (("cust-1", "Alice"),)
    assert len(result.rejected) == 1
    rejected = result.rejected[0]
    assert rejected.error_type == "UNIQUENESS_VIOLATION"
    assert rejected.error_column == "customer_id"
    assert rejected.source_row_number == 1


def test_all_distinct_values_keeps_every_row_and_rejects_none() -> None:
    chunk = _chunk([("cust-1", "Alice"), ("cust-2", "Bob"), ("cust-3", "Carol")])
    rule = UniquenessRule(
        column_index=0, column_name="customer_id", strategy="REJECT_RECORD", rule_id="r4"
    )

    result = rule.apply(_make_context(), chunk)

    assert result.chunk.rows == chunk.rows
    assert result.rejected == []


def test_a_third_occurrence_of_the_same_value_is_also_rejected() -> None:
    chunk = _chunk([("cust-1", "A"), ("cust-1", "B"), ("cust-1", "C")])
    rule = UniquenessRule(
        column_index=0, column_name="customer_id", strategy="REJECT_RECORD", rule_id="r4"
    )

    result = rule.apply(_make_context(), chunk)

    assert result.chunk.rows == (("cust-1", "A"),)
    assert len(result.rejected) == 2


def test_uniqueness_is_scoped_to_one_chunk_only_never_cross_chunk() -> None:
    # Proves the documented within-chunk-only scope: a value repeated ACROSS
    # two separate apply() calls (simulating two different chunks) is NOT
    # rejected, because the "seen" set is rebuilt fresh on every call.
    rule = UniquenessRule(
        column_index=0, column_name="customer_id", strategy="REJECT_RECORD", rule_id="r4"
    )
    first_chunk = _chunk([("cust-1", "Alice")])
    second_chunk = _chunk([("cust-1", "Alice Again")], first_ordinal=1)

    first_result = rule.apply(_make_context(), first_chunk)
    second_result = rule.apply(_make_context(), second_chunk)

    assert first_result.rejected == []
    assert second_result.rejected == []
    assert second_result.chunk.rows == (("cust-1", "Alice Again"),)


def test_never_raises_and_accounts_for_every_row() -> None:
    chunk = _chunk([("cust-1", "A"), ("cust-2", "B"), ("cust-1", "C"), ("cust-3", "D")])
    rule = UniquenessRule(
        column_index=0, column_name="customer_id", strategy="REJECT_RECORD", rule_id="r4"
    )

    result = rule.apply(_make_context(), chunk)

    assert len(result.chunk.rows) + len(result.rejected) == len(chunk.rows)
