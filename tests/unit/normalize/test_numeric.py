"""Unit tests for ``dataplat.normalize.numeric.NumericNormalizer`` -- CSV-10.

Covers every CSV-10-numeric-tagged corpus fixture directly against its own
``tests/fixtures/corpus.yaml`` ``expect:`` block: 20/21 (the comma/point
locale pair -- proven to denote the SAME quantities), 50/51 (the two
unrecoverable-damage rejections), 57 (accounting negative conventions, with
an explicit naive-implementation contrast), 58 (currency/percent), and 59
(numeric null sentinels, exact-match only, plus the platform-wide ``None``
passthrough convention).

Every ``ctx: PipelineContext`` passed below is a placeholder built by
``_make_context()``, mirroring ``tests/unit/test_pipeline_errors.py``:
``NumericNormalizer.apply()`` never dereferences ``config``/``metadata``/
``objects``/``db``/``log`` -- only ``chunk`` and the stage's own constructor
configuration matter to the code under test here.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from dataplat.models.identity import RunContext
from dataplat.models.record import RecordChunk
from dataplat.normalize.numeric import NumericNormalizer
from dataplat.pipeline.protocol import PipelineContext

if TYPE_CHECKING:
    from collections.abc import Sequence


def _make_context() -> PipelineContext:
    """Build a placeholder ``PipelineContext`` for stage tests.

    Only ``run`` is populated with a real value; the remaining fields are
    untouched by any code exercised in this file.
    """
    return PipelineContext(
        run=RunContext(run_id=1, idempotency_key="test-run"),
        config=None,  # type: ignore[arg-type] -- unused by the code under test
        metadata=None,  # type: ignore[arg-type] -- unused by the code under test
        objects=None,  # type: ignore[arg-type] -- unused by the code under test
        db=None,  # type: ignore[arg-type] -- unused by the code under test
        log=None,  # type: ignore[arg-type] -- unused by the code under test
    )


def _chunk(rows: Sequence[tuple[str, ...]], *, first_ordinal: int = 0) -> RecordChunk:
    """Build a ``RecordChunk`` whose ``expected_field_count`` matches ``rows``.

    ``NumericNormalizer`` never reads ``expected_field_count`` itself (only
    ``RaggedRowGuard`` does) -- it is set from the first row purely so the
    constructed ``RecordChunk`` is internally consistent.

    ``rows`` is typed ``Sequence`` (covariant), not ``list`` (invariant): a
    local variable holding a same-arity list of tuples, e.g.
    ``rows: list[tuple[str, str, str]]``, is not assignable to a
    ``list[tuple[str, ...]]`` parameter, but IS assignable to a
    ``Sequence[tuple[str, ...]]`` one -- avoids an
    ``# type: ignore``/restructure at every call site in this file.
    """
    return RecordChunk(
        rows=tuple(rows),
        first_ordinal=first_ordinal,
        expected_field_count=len(rows[0]) if rows else 0,
    )


# --- Task 1: locale-aware Decimal parsing and negative-style handling ------


# Fixture 20 (`20_decimal_comma.csv`): decimal_separator=",", no thousands
# separator, delimiter ";" (not exercised here -- rows are pre-split).
def test_decimal_comma_locale_normalizes_to_exact_decimal() -> None:
    rows = [
        ("1", "1234,56", "wartosc standardowa"),
        ("2", "-99,99", "wartosc ujemna"),
        ("3", "0,00", "zero"),
        ("4", "1234567,89", "bez separatora tysiecy"),
    ]
    normalizer = NumericNormalizer(
        column_index=1,
        column_name="kwota",
        decimal_separator=",",
        thousands_separator=None,
        currency_symbols=(),
        percent_as_fraction=True,
        negative_style="leading-minus",
    )

    result = normalizer.apply(_make_context(), _chunk(rows))

    assert result.rejected == []
    normalized = [row[1] for row in result.chunk.rows]
    assert normalized == ["1234.56", "-99.99", "0.00", "1234567.89"]
    assert [Decimal(v) for v in normalized] == [
        Decimal("1234.56"),
        Decimal("-99.99"),
        Decimal("0.00"),
        Decimal("1234567.89"),
    ]


# Fixture 21 (`21_decimal_point.csv`): the SAME four quantities as fixture
# 20, under the opposite (point) decimal-separator convention.
def test_decimal_point_locale_normalizes_to_exact_decimal() -> None:
    rows = [
        ("1", "1234.56", "standard value"),
        ("2", "-99.99", "negative value"),
        ("3", "0.00", "zero"),
        ("4", "1234567.89", "no grouping separator"),
    ]
    normalizer = NumericNormalizer(
        column_index=1,
        column_name="amount",
        decimal_separator=".",
        thousands_separator=None,
        currency_symbols=(),
        percent_as_fraction=True,
        negative_style="leading-minus",
    )

    result = normalizer.apply(_make_context(), _chunk(rows))

    assert result.rejected == []
    normalized = [row[1] for row in result.chunk.rows]
    assert normalized == ["1234.56", "-99.99", "0.00", "1234567.89"]


# corpus.yaml line 1586: `denotes_the_same_quantities_as: "20_decimal_comma.csv"`
# -- fixtures 20 and 21 must produce the SAME Decimal values, not merely
# individually-plausible ones.
def test_decimal_comma_and_decimal_point_locales_denote_the_same_quantities() -> None:
    comma_rows = [
        ("1", "1234,56", ""),
        ("2", "-99,99", ""),
        ("3", "0,00", ""),
        ("4", "1234567,89", ""),
    ]
    point_rows = [
        ("1", "1234.56", ""),
        ("2", "-99.99", ""),
        ("3", "0.00", ""),
        ("4", "1234567.89", ""),
    ]
    comma_normalizer = NumericNormalizer(
        column_index=1,
        column_name="kwota",
        decimal_separator=",",
        thousands_separator=None,
        currency_symbols=(),
        percent_as_fraction=True,
        negative_style="leading-minus",
    )
    point_normalizer = NumericNormalizer(
        column_index=1,
        column_name="amount",
        decimal_separator=".",
        thousands_separator=None,
        currency_symbols=(),
        percent_as_fraction=True,
        negative_style="leading-minus",
    )

    comma_result = comma_normalizer.apply(_make_context(), _chunk(comma_rows))
    point_result = point_normalizer.apply(_make_context(), _chunk(point_rows))

    comma_values = [Decimal(row[1]) for row in comma_result.chunk.rows]
    point_values = [Decimal(row[1]) for row in point_result.chunk.rows]
    assert (
        comma_values
        == point_values
        == [
            Decimal("1234.56"),
            Decimal("-99.99"),
            Decimal("0.00"),
            Decimal("1234567.89"),
        ]
    )


# Fixture 57 (`57_negative_parentheses_and_trailing_minus.csv`) row 1:
# "(123.45)" under negative_style="parentheses".
def test_negative_style_parentheses_normalizes_to_negative_decimal() -> None:
    normalizer = NumericNormalizer(
        column_index=1,
        column_name="amount",
        decimal_separator=".",
        thousands_separator=None,
        currency_symbols=(),
        percent_as_fraction=False,
        negative_style="parentheses",
    )

    result = normalizer.apply(
        _make_context(), _chunk([("1", "(123.45)", "accounting parentheses")])
    )

    assert result.rejected == []
    normalized = Decimal(result.chunk.rows[0][1])
    assert normalized == Decimal("-123.45")
    # corpus.yaml `values_stripped_naively_would_be`: a naive strip-every-
    # non-numeric-character implementation would (wrongly) produce a SIGN
    # FLIP, "123.45" -- this implementation must not do that.
    naively_stripped = Decimal("123.45")
    assert normalized != naively_stripped


# Fixture 57 row 2: "123.45-" under negative_style="trailing-minus".
def test_negative_style_trailing_minus_normalizes_to_negative_decimal() -> None:
    normalizer = NumericNormalizer(
        column_index=1,
        column_name="amount",
        decimal_separator=".",
        thousands_separator=None,
        currency_symbols=(),
        percent_as_fraction=False,
        negative_style="trailing-minus",
    )

    result = normalizer.apply(_make_context(), _chunk([("2", "123.45-", "trailing minus")]))

    assert result.rejected == []
    normalized = Decimal(result.chunk.rows[0][1])
    assert normalized == Decimal("-123.45")
    naively_stripped = Decimal("123.45")
    assert normalized != naively_stripped


# Fixture 57 row 3: "-123.45" under negative_style="leading-minus".
def test_negative_style_leading_minus_normalizes_to_negative_decimal() -> None:
    normalizer = NumericNormalizer(
        column_index=1,
        column_name="amount",
        decimal_separator=".",
        thousands_separator=None,
        currency_symbols=(),
        percent_as_fraction=False,
        negative_style="leading-minus",
    )

    result = normalizer.apply(_make_context(), _chunk([("3", "-123.45", "leading minus")]))

    assert result.rejected == []
    assert Decimal(result.chunk.rows[0][1]) == Decimal("-123.45")


# Fixture 57 row 4 (positive control): "123.45" stays positive under EVERY
# declared negative_style -- none of the three markers is present in it.
def test_negative_style_positive_control_unaffected_by_any_negative_style() -> None:
    for style in ("parentheses", "trailing-minus", "leading-minus"):
        normalizer = NumericNormalizer(
            column_index=1,
            column_name="amount",
            decimal_separator=".",
            thousands_separator=None,
            currency_symbols=(),
            percent_as_fraction=False,
            negative_style=style,
        )

        result = normalizer.apply(_make_context(), _chunk([("4", "123.45", "positive control")]))

        assert result.rejected == []
        assert Decimal(result.chunk.rows[0][1]) == Decimal("123.45")


# Fixture 58 (`58_currency_and_percent.csv`): decimal_separator=",",
# thousands_separator=".", currency_symbols=("EUR-sign", "zl") -- prefixed
# and suffixed currency symbols, both denoting the SAME quantity as the
# bare value.
def test_currency_symbols_prefixed_and_suffixed_normalize_to_the_same_quantity() -> None:
    normalizer = NumericNormalizer(
        column_index=1,
        column_name="kwota",
        decimal_separator=",",
        thousands_separator=".",
        currency_symbols=("€", "zł"),  # "EUR-sign" (prefix), "zl" (suffix)
        percent_as_fraction=True,
        negative_style="leading-minus",
    )
    rows = [
        ("1", "€1.234,56", "symbol z przodu"),
        ("2", "1.234,56 zł", "symbol z tylu"),
        ("3", "1234,56", "bez symbolu i bez grupowania"),
    ]

    result = normalizer.apply(_make_context(), _chunk(rows))

    assert result.rejected == []
    normalized = [Decimal(row[1]) for row in result.chunk.rows]
    assert normalized == [Decimal("1234.56")] * 3
    # corpus.yaml `misreading_the_grouping_dot_as_a_decimal_point_yields:
    # "1.234"` -- the grouping "." is NOT a decimal point; a reader that
    # misreads it produces a value wrong by a factor of 1000, and this
    # implementation must not do that.
    misreading = Decimal("1.234")
    assert all(value != misreading for value in normalized)


# Fixture 58's percent column: "12,5 %" -> 0.125, "7,25 %" -> 0.0725,
# "100 %" -> 1 -- percent is separated from the number by a space, and
# percent_as_fraction=True stores it as a fraction of 1.
def test_percent_as_fraction_true_divides_by_one_hundred() -> None:
    normalizer = NumericNormalizer(
        column_index=2,
        column_name="udzial",
        decimal_separator=",",
        thousands_separator=".",
        currency_symbols=(),
        percent_as_fraction=True,
        negative_style="leading-minus",
    )
    rows = [
        ("1", "€1.234,56", "12,5 %"),
        ("2", "1.234,56 zł", "7,25 %"),
        ("3", "1234,56", "100 %"),
    ]

    result = normalizer.apply(_make_context(), _chunk(rows))

    assert result.rejected == []
    normalized = [Decimal(row[2]) for row in result.chunk.rows]
    assert normalized == [Decimal("0.125"), Decimal("0.0725"), Decimal(1)]


# percent_as_fraction=False: the "%" is still stripped, but the literal
# number before the sign is kept, never divided.
def test_percent_as_fraction_false_keeps_the_literal_number() -> None:
    normalizer = NumericNormalizer(
        column_index=0,
        column_name="udzial",
        decimal_separator=".",
        thousands_separator=None,
        currency_symbols=(),
        percent_as_fraction=False,
        negative_style="leading-minus",
    )

    result = normalizer.apply(_make_context(), _chunk([("12.5 %",)]))

    assert result.rejected == []
    assert Decimal(result.chunk.rows[0][0]) == Decimal("12.5")


# A value this stage cannot parse under its declared locale profile is
# rejected (`invalid-numeric-value`), never allowed to let the raw
# `decimal.InvalidOperation` escape `apply()`.
def test_unparseable_value_is_rejected_not_raised() -> None:
    normalizer = NumericNormalizer(
        column_index=0,
        column_name="amount",
        decimal_separator=".",
        thousands_separator=None,
        currency_symbols=(),
        percent_as_fraction=False,
        negative_style="leading-minus",
    )

    result = normalizer.apply(_make_context(), _chunk([("not-a-number",)]))

    assert result.chunk.rows == ()
    assert len(result.rejected) == 1
    assert result.rejected[0].error_type == "invalid-numeric-value"


# --- Task 2: unrecoverable-damage rejection, null sentinels, None passthrough


# Fixture 50 (`50_excel_scientific_notation_ids.csv`), reject_scientific_
# notation=True: rows 1/2 are damaged beyond repair and rejected; row 3
# (clean control, formatted as text) is accepted unchanged.
def test_scientific_notation_identifier_rejected_as_unrecoverable() -> None:
    normalizer = NumericNormalizer(
        column_index=1,
        column_name="customer_id",
        decimal_separator=".",
        thousands_separator=None,
        currency_symbols=(),
        percent_as_fraction=False,
        negative_style="leading-minus",
        reject_scientific_notation=True,
    )
    rows = [
        ("1", "1.23457E+14", "damaged by the spreadsheet"),
        ("2", "9.87654E+14", "damaged by the spreadsheet"),
        ("3", "123456789012345", "intact - the column was formatted as text"),
    ]

    result = normalizer.apply(_make_context(), _chunk(rows))

    assert len(result.rejected) == 2
    assert all(
        r.error_type == "scientific-notation-identifier-unrecoverable" for r in result.rejected
    )
    # The wrongly-expanded value must never appear anywhere in the
    # rejection, as if it had been silently accepted.
    for r in result.rejected:
        assert "123457000000000" not in r.error_message
        assert "123457000000000" not in r.raw_line
        assert "987654000000000" not in r.error_message
        assert "987654000000000" not in r.raw_line
    assert result.chunk.rows == (
        ("3", "123456789012345", "intact - the column was formatted as text"),
    )


# Fixture 51 (`51_excel_leading_zero_stripped.csv`), fixed_width=5: rows
# 1/2 lost their leading zero(s) and are rejected; row 3 (clean control,
# already at the declared width) is accepted unchanged.
def test_fixed_width_identifier_rejected_as_unrecoverable() -> None:
    normalizer = NumericNormalizer(
        column_index=1,
        column_name="postcode",
        decimal_separator=".",
        thousands_separator=None,
        currency_symbols=(),
        percent_as_fraction=False,
        negative_style="leading-minus",
        fixed_width=5,
    )
    rows = [
        ("1", "1234", "Warszawa", "one leading zero stripped"),
        ("2", "234", "Krakow", "two leading zeros stripped"),
        ("3", "12345", "Gdansk", "intact - nothing to lose"),
    ]

    result = normalizer.apply(_make_context(), _chunk(rows))

    assert len(result.rejected) == 2
    assert all(
        r.error_type == "fixed-width-identifier-below-declared-width" for r in result.rejected
    )
    # Never silently left-padded: "1234"/"234" must never appear rewritten
    # to "01234"/"00234" anywhere.
    for r in result.rejected:
        assert "01234" not in r.error_message
        assert "00234" not in r.error_message
    assert result.chunk.rows == (("3", "12345", "Gdansk", "intact - nothing to lose"),)


# Fixture 59 (`59_numeric_null_sentinels.csv`): "-1" is a declared sentinel
# for `amount`/`quantity`, meaning absent -- but ONLY an EXACT match. "-1.50"
# is a genuine negative amount (not absent); "0" is a genuine value (not
# implicitly a sentinel unless declared).
def test_numeric_null_sentinels_match_exactly_never_by_substring() -> None:
    amount_normalizer = NumericNormalizer(
        column_index=1,
        column_name="amount",
        decimal_separator=".",
        thousands_separator=None,
        currency_symbols=(),
        percent_as_fraction=False,
        negative_style="leading-minus",
        null_sentinels=("-1",),
    )
    quantity_normalizer = NumericNormalizer(
        column_index=3,
        column_name="quantity",
        decimal_separator=".",
        thousands_separator=None,
        currency_symbols=(),
        percent_as_fraction=False,
        negative_style="leading-minus",
        null_sentinels=("-1",),
    )
    rows = [
        ("1", "-1", "9999-12-31", "-1", "sentinels in every column"),
        ("2", "-1.50", "2026-06-30", "3", "a genuine negative amount and a real date"),
        ("3", "0", "0000-00-00", "0", "zero is a value and the date is a sentinel"),
    ]

    amount_result = amount_normalizer.apply(_make_context(), _chunk(rows))
    quantity_result = quantity_normalizer.apply(_make_context(), _chunk(rows))

    assert amount_result.rejected == []
    assert quantity_result.rejected == []

    # Row 1: the exact sentinel "-1" normalizes to absent in both columns.
    # `RecordChunk.rows`' element type is nominally `str` in this plan's own
    # file scope (see `dataplat.normalize.numeric._replace_field`'s
    # docstring) -- reading through `object` keeps this `is None` check
    # meaningful, and `# type: ignore`-free, under both the current and the
    # post-06-11-merge element type.
    amount_cell: object = amount_result.chunk.rows[0][1]
    quantity_cell: object = quantity_result.chunk.rows[0][3]
    assert amount_cell is None
    assert quantity_cell is None

    # Row 2: "-1.50" is NOT the declared sentinel "-1" -- a real negative
    # value, never treated as absent by a substring/prefix match.
    assert amount_result.chunk.rows[1][1] == "-1.50"
    assert Decimal(amount_result.chunk.rows[1][1]) == Decimal("-1.50")

    # Row 3: "0" is a real value, not implicitly a sentinel.
    assert quantity_result.chunk.rows[2][3] == "0"
    assert Decimal(quantity_result.chunk.rows[2][3]) == Decimal(0)


# A field already `None` (normalized to absent upstream by a
# `NullTokenNormalizer` for this same nullable column, per plan 06-11 Task
# 1's platform-wide `None` convention) passes through unchanged: never
# crashing, never coerced to "0"/Decimal("0"), never re-rejected.
def test_none_field_passes_through_unchanged() -> None:
    normalizer = NumericNormalizer(
        column_index=1,
        column_name="amount",
        decimal_separator=".",
        thousands_separator=None,
        currency_symbols=(),
        percent_as_fraction=False,
        negative_style="leading-minus",
        null_sentinels=("-1",),
    )
    # `RecordChunk.rows` is typed `tuple[tuple[str, ...], ...]` within this
    # plan's own file scope, but plan 06-11 Task 1 widens that element type
    # to `tuple[str | None, ...]` platform-wide -- this row is constructed
    # exactly as a real upstream `NullTokenNormalizer` would leave it.
    row_with_none: tuple[str, ...] = ("1", None, "already normalized to absent")  # type: ignore[assignment]
    chunk = RecordChunk(rows=(row_with_none,), first_ordinal=0, expected_field_count=3)

    result = normalizer.apply(_make_context(), chunk)

    assert result.rejected == []
    assert len(result.chunk.rows) == 1
    surviving_cell: object = result.chunk.rows[0][1]
    assert surviving_cell is None
