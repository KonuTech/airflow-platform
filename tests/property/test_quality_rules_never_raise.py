"""Property test for ``dataplat.validate``'s row-scoped quality rules -- QUAL-03's general proof.

``tests/unit/validate/test_quality_rules.py`` proves specific behaviors at
fixed configs. This file generalizes the "never raises, every row
accounted for exactly once" guarantee across *arbitrary* row values --
including ``None``, empty strings, unicode, very long strings, and strings
containing the configured pattern's own special regex characters -- for
``CompletenessRule``, ``ValidityRangeRule`` and ``PatternRule`` together.
"""

from __future__ import annotations

from types import SimpleNamespace

from hypothesis import given, settings
from hypothesis import strategies as st

from dataplat.models.identity import RunContext
from dataplat.models.record import RecordChunk
from dataplat.pipeline.protocol import PipelineContext
from dataplat.validate.completeness import CompletenessRule
from dataplat.validate.pattern import PatternRule
from dataplat.validate.validity_range import ValidityRangeRule

# Adversarial regex-special characters mixed into the generated text so
# PatternRule's compiled pattern is exercised against input that could
# confuse a naive (non-compiled, or substring-based) matcher -- not because
# the pattern source is untrusted (T-08-09: it is developer-authored config).
_ADVERSARIAL_CHARS = ".*+?^$()[]{}|\\"
_FIELD_VALUE = st.one_of(
    st.none(),
    st.just(""),
    st.text(min_size=0, max_size=200),
    st.text(alphabet=_ADVERSARIAL_CHARS, min_size=0, max_size=20),
)


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


def _chunk(rows: list[str | None]) -> RecordChunk:
    """Wrap each generated value as a single-column row."""
    return RecordChunk(
        rows=tuple((value,) for value in rows),
        first_ordinal=0,
        expected_field_count=1,
    )


# max_examples=100: each example is a handful of short in-memory strings with
# no I/O, well inside this repo's property-test convention (see
# tests/property/test_chunking_properties.py's own budget note).
@settings(max_examples=100)
@given(values=st.lists(_FIELD_VALUE, max_size=25))
def test_completeness_rule_never_raises_and_accounts_for_every_row(
    values: list[str | None],
) -> None:
    chunk = _chunk(values)
    rule = CompletenessRule(
        column_index=0, column_name="col", strategy="REJECT_RECORD", rule_id="r1"
    )

    result = rule.apply(_make_context(), chunk)

    assert len(result.chunk.rows) + len(result.rejected) == len(chunk.rows)


@settings(max_examples=100)
@given(values=st.lists(_FIELD_VALUE, max_size=25))
def test_validity_range_rule_never_raises_and_accounts_for_every_row(
    values: list[str | None],
) -> None:
    chunk = _chunk(values)
    rule = ValidityRangeRule(
        column_index=0,
        column_name="col",
        strategy="REJECT_RECORD",
        rule_id="r2",
        minimum=0,
        maximum=1000,
    )

    result = rule.apply(_make_context(), chunk)

    assert len(result.chunk.rows) + len(result.rejected) == len(chunk.rows)


@settings(max_examples=100)
@given(values=st.lists(_FIELD_VALUE, max_size=25))
def test_pattern_rule_never_raises_and_accounts_for_every_row(values: list[str | None]) -> None:
    chunk = _chunk(values)
    rule = PatternRule(
        column_index=0,
        column_name="col",
        strategy="REJECT_RECORD",
        rule_id="r3",
        pattern=r"^[A-Z]{2}$",
    )

    result = rule.apply(_make_context(), chunk)

    assert len(result.chunk.rows) + len(result.rejected) == len(chunk.rows)
