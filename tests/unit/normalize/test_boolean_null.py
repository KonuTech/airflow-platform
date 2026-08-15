"""Unit tests for ``dataplat.normalize.boolean_null``.

Covers ``NullTokenNormalizer`` (exact-match NULL-token replacement, corpus
fixture ``24_null_values.csv``) and ``BooleanNormalizer`` (locale-specific
true/false token mapping with unmapped-value rejection, corpus fixture
``60_boolean_localized.csv``).

Every ``ctx: PipelineContext`` passed below is a placeholder built by
``_make_context()``, mirroring ``tests/unit/test_pipeline_errors.py``'s own
helper: neither normalizer under test dereferences any of
``config``/``metadata``/``objects``/``db``/``log`` -- only ``chunk`` matters.
"""

from __future__ import annotations

from dataplat.models.identity import RunContext
from dataplat.models.record import RecordChunk
from dataplat.normalize.boolean_null import BooleanNormalizer, NullTokenNormalizer
from dataplat.pipeline.protocol import PipelineContext


def _make_context() -> PipelineContext:
    """Build a placeholder ``PipelineContext`` -- only ``run`` is real."""
    return PipelineContext(
        run=RunContext(run_id=1, idempotency_key="test-run"),
        config=None,  # type: ignore[arg-type] -- unused by the code under test
        metadata=None,  # type: ignore[arg-type] -- unused by the code under test
        objects=None,  # type: ignore[arg-type] -- unused by the code under test
        db=None,  # type: ignore[arg-type] -- unused by the code under test
        log=None,  # type: ignore[arg-type] -- unused by the code under test
    )


def _chunk(
    rows: list[tuple[str | bool | None, ...]],
    *,
    first_ordinal: int = 0,
    expected_field_count: int = 4,
) -> RecordChunk:
    return RecordChunk(
        rows=tuple(rows),
        first_ordinal=first_ordinal,
        expected_field_count=expected_field_count,
    )


# --- NullTokenNormalizer: corpus fixture 24_null_values.csv -----------------
# id,name,amount,note -- `name` is column_index=1.
_NULL_TOKENS = ("", "NULL", "null", "N/A", "NA", "-", "None")


def test_null_token_normalizer_replaces_every_declared_token_with_none() -> None:
    # Rows 1-7 of fixture 24: each `name` value is a declared NULL token.
    rows: list[tuple[str | bool | None, ...]] = [
        ("1", "", "100.00", "the empty string"),
        ("2", "NULL", "200.00", "the four letter token"),
        ("3", "N/A", "300.00", "not applicable"),
        ("4", "NA", "400.00", "the two letter token"),
        ("5", "null", "500.00", "lower case"),
        ("6", "-", "600.00", "a dash"),
        ("7", "None", "700.00", "a Python repr that leaked into an export"),
    ]
    chunk = _chunk(rows)
    normalizer = NullTokenNormalizer(column_index=1, column_name="name", null_tokens=_NULL_TOKENS)

    result = normalizer.apply(_make_context(), chunk)

    assert result.rejected == []
    assert [row[1] for row in result.chunk.rows] == [None] * len(rows)
    # Every other field is untouched.
    for original, normalized in zip(rows, result.chunk.rows, strict=True):
        assert normalized[0] == original[0]
        assert normalized[2] == original[2]
        assert normalized[3] == original[3]


def test_null_token_normalizer_never_substring_matches_null_industries() -> None:
    # Fixture 24 row 8: "NULL Industries" CONTAINS the token "NULL" but is
    # not an exact match -- it must survive as real data, never absent.
    chunk = _chunk(
        [("8", "NULL Industries", "800.00", "a company name that contains the token")],
    )
    normalizer = NullTokenNormalizer(column_index=1, column_name="name", null_tokens=_NULL_TOKENS)

    result = normalizer.apply(_make_context(), chunk)

    assert result.rejected == []
    assert result.chunk.rows == (
        ("8", "NULL Industries", "800.00", "a company name that contains the token"),
    )


def test_null_token_normalizer_leaves_an_already_none_field_untouched() -> None:
    # Defensive: a field that is already None (e.g. this stage re-running,
    # or a hypothetical upstream stage) must never crash the `in` check.
    chunk = _chunk([("9", None, "900.00", "already absent")])
    normalizer = NullTokenNormalizer(column_index=1, column_name="name", null_tokens=_NULL_TOKENS)

    result = normalizer.apply(_make_context(), chunk)

    assert result.rejected == []
    assert result.chunk.rows[0][1] is None


# --- BooleanNormalizer: corpus fixture 60_boolean_localized.csv -------------
# id,flag,language,note -- `flag` is column_index=1.
_TRUE_TOKENS = ("Tak", "Ja", "O", "Y")
_FALSE_TOKENS = ("Nie", "Nein", "N")


def test_boolean_normalizer_maps_every_declared_true_token_to_true() -> None:
    rows: list[tuple[str | bool | None, ...]] = [
        ("1", "Tak", "pl", "polish yes"),
        ("3", "Ja", "de", "german yes"),
        ("5", "O", "fr", "french single letter yes"),  # the O-means-Oui trap
        ("7", "Y", "en", "english single letter yes"),
    ]
    chunk = _chunk(rows)
    normalizer = BooleanNormalizer(
        column_index=1,
        column_name="flag",
        true_tokens=_TRUE_TOKENS,
        false_tokens=_FALSE_TOKENS,
    )

    result = normalizer.apply(_make_context(), chunk)

    assert result.rejected == []
    assert [row[1] for row in result.chunk.rows] == [True, True, True, True]


def test_boolean_normalizer_maps_every_declared_false_token_to_false() -> None:
    rows: list[tuple[str | bool | None, ...]] = [
        ("2", "Nie", "pl", "polish no"),
        ("4", "Nein", "de", "german no"),
        ("6", "N", "fr", "french single letter no"),
    ]
    chunk = _chunk(rows)
    normalizer = BooleanNormalizer(
        column_index=1,
        column_name="flag",
        true_tokens=_TRUE_TOKENS,
        false_tokens=_FALSE_TOKENS,
    )

    result = normalizer.apply(_make_context(), chunk)

    assert result.rejected == []
    assert [row[1] for row in result.chunk.rows] == [False, False, False]


def test_boolean_normalizer_rejects_an_unmapped_token_never_defaults_to_false() -> None:
    # Fixture 60 row 8: "Maybe" is declared in neither token list.
    chunk = _chunk([("8", "Maybe", "en", "not in the declared mapping")], first_ordinal=7)
    normalizer = BooleanNormalizer(
        column_index=1,
        column_name="flag",
        true_tokens=_TRUE_TOKENS,
        false_tokens=_FALSE_TOKENS,
    )

    result = normalizer.apply(_make_context(), chunk)

    assert result.chunk.rows == ()
    assert len(result.rejected) == 1
    rejected = result.rejected[0]
    assert rejected.error_type == "unmapped-boolean-token"
    assert rejected.source_row_number == 7
    assert rejected.error_column == "flag"


def test_boolean_normalizer_rejects_bare_0_and_1_when_typing_is_enforced() -> None:
    """Proves "left untouched" means "rejected as unmapped", never "silently
    coerced" -- BooleanNormalizer IS applied to this column (typing is being
    enforced) and neither "0" nor "1" is declared in either token list, so
    both are exact-match-rejected exactly like any other unrecognised
    string. CSV-10's named risk is a reader independently deciding 0/1 mean
    False/True; this test proves this class never does that. (The other
    legitimate half of this behavior -- a column BooleanNormalizer is never
    applied to at all -- is a pipeline-wiring guarantee, not something this
    class-level test can exercise.)
    """
    chunk = _chunk([("1", "0", "en", "bare zero"), ("2", "1", "en", "bare one")])
    normalizer = BooleanNormalizer(
        column_index=1,
        column_name="flag",
        true_tokens=_TRUE_TOKENS,
        false_tokens=_FALSE_TOKENS,
    )

    result = normalizer.apply(_make_context(), chunk)

    assert result.chunk.rows == ()
    assert len(result.rejected) == 2
    assert all(r.error_type == "unmapped-boolean-token" for r in result.rejected)


def test_boolean_normalizer_passes_a_none_field_through_unchanged() -> None:
    # Already normalized to absent upstream by NullTokenNormalizer for this
    # same nullable column -- never rejected, never coerced to False.
    chunk = _chunk([("9", None, "en", "already absent")])
    normalizer = BooleanNormalizer(
        column_index=1,
        column_name="flag",
        true_tokens=_TRUE_TOKENS,
        false_tokens=_FALSE_TOKENS,
    )

    result = normalizer.apply(_make_context(), chunk)

    assert result.rejected == []
    assert result.chunk.rows == (("9", None, "en", "already absent"),)
