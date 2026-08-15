"""Unit tests for ``dataplat.normalize.dates`` — CSV-09's explicit-format proof.

Every case below is drawn directly from ``tests/fixtures/corpus.yaml``'s own
``expect:`` block for the fixture it exercises (fixtures 22, 23, 52, 53, 54,
59 in this file; 55/56's DST-aware cases live in the same module once Task 2
lands). No format is ever guessed: every parse uses ``datetime.strptime``
against a format the test declares explicitly, mirroring what a real
``columns:`` contract (``ColumnContract.format``, ``config/model.py``) would
supply at runtime.

Every ``ctx: PipelineContext`` below is the same placeholder-context pattern
``tests/unit/test_pipeline_errors.py`` uses: only ``chunk`` matters to the
code under test here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from dataplat.models.identity import RunContext
from dataplat.models.record import RecordChunk
from dataplat.normalize.dates import DateNormalizer
from dataplat.pipeline.protocol import PipelineContext

if TYPE_CHECKING:
    from dataplat.models.record import RejectedRecord


def _make_context() -> PipelineContext:
    """Build a placeholder ``PipelineContext`` for ``DateNormalizer`` tests.

    Only ``run`` is populated with a real value; the remaining fields are
    untouched by any code exercised in this file (matches
    ``tests/unit/test_pipeline_errors.py``'s ``_make_context()``).
    """
    return PipelineContext(
        run=RunContext(run_id=1, idempotency_key="test-run"),
        config=None,  # type: ignore[arg-type]  # unused by the code under test
        metadata=None,  # type: ignore[arg-type]  # unused by the code under test
        objects=None,  # type: ignore[arg-type]  # unused by the code under test
        db=None,  # type: ignore[arg-type]  # unused by the code under test
        log=None,  # type: ignore[arg-type]  # unused by the code under test
    )


def _chunk(rows: list[tuple[str | None, ...]], *, first_ordinal: int = 0) -> RecordChunk:
    """Build a single-column-relevant ``RecordChunk`` from raw row tuples.

    A field may be ``None`` here — simulating a nullable date/timestamp
    column an upstream ``NullTokenNormalizer`` has already normalized to
    absent (plan 06-11 Task 1's platform-wide convention) — even though
    ``RecordChunk.rows`` is declared ``tuple[str, ...]``. ``DateNormalizer``
    is the code under test for handling that case; this helper's own typing
    stays honest about it via the ``type: ignore`` below rather than lying
    with a narrower annotation.
    """
    width = len(rows[0]) if rows else 0
    return RecordChunk(
        rows=tuple(rows),  # type: ignore[arg-type]  # see docstring: a field may be None
        first_ordinal=first_ordinal,
        expected_field_count=width,
    )


def _single_rejected(result_rejected: list[RejectedRecord]) -> RejectedRecord:
    assert len(result_rejected) == 1
    return result_rejected[0]


def _field(rows: tuple[tuple[str, ...], ...], row_idx: int, col_idx: int) -> str | None:
    """Read one field from ``RecordChunk.rows`` honestly typed as ``str | None``.

    ``RecordChunk.rows`` is declared ``tuple[str, ...]``, but a
    None-passthrough row genuinely carries the real Python ``None`` at
    runtime (see ``_chunk()``'s docstring) -- this cast makes that explicit
    for mypy instead of a bare index producing a statically-impossible
    (and therefore ``unreachable``-flagged) ``is None`` comparison.
    """
    return cast("str | None", rows[row_idx][col_idx])


# --- fixtures 22/23: format decides, the rendering carries no evidence -----


def test_eu_dates_parse_under_day_first_format() -> None:
    # corpus fixture 22_eu_dates.csv
    raw_values = ["31/12/2026", "01/02/2026", "15/08/2026"]
    iso_values = ["2026-12-31", "2026-02-01", "2026-08-15"]
    normalizer = DateNormalizer(column_index=1, column_name="event_date", format="%d/%m/%Y")
    chunk = _chunk([(str(i), v, "note") for i, v in enumerate(raw_values)])

    result = normalizer.apply(_make_context(), chunk)

    assert result.rejected == []
    assert [row[1] for row in result.chunk.rows] == iso_values


def test_us_dates_parse_under_month_first_format() -> None:
    # corpus fixture 23_us_dates.csv -- same underlying days as fixture 22,
    # different declared format, proving the format decides.
    raw_values = ["12/31/2026", "02/01/2026", "08/15/2026"]
    iso_values = ["2026-12-31", "2026-02-01", "2026-08-15"]
    normalizer = DateNormalizer(column_index=1, column_name="event_date", format="%m/%d/%Y")
    chunk = _chunk([(str(i), v, "note") for i, v in enumerate(raw_values)])

    result = normalizer.apply(_make_context(), chunk)

    assert result.rejected == []
    assert [row[1] for row in result.chunk.rows] == iso_values


# --- invalid dates: explicit rejection, never coercion ----------------------


def test_invalid_calendar_date_is_rejected_not_coerced() -> None:
    normalizer = DateNormalizer(column_index=0, column_name="d", format="%Y-%m-%d")
    chunk = _chunk([("2026-02-30",)])

    result = normalizer.apply(_make_context(), chunk)

    assert result.chunk.rows == ()
    rejected = _single_rejected(result.rejected)
    assert rejected.error_type == "invalid-calendar-date"
    assert rejected.source_row_number == 0


def test_31_02_2026_is_rejected_never_coerced_to_a_nearby_date() -> None:
    normalizer = DateNormalizer(column_index=0, column_name="d", format="%d/%m/%Y")
    chunk = _chunk([("31/02/2026",)])

    result = normalizer.apply(_make_context(), chunk)

    assert result.chunk.rows == ()
    assert _single_rejected(result.rejected).error_type == "invalid-calendar-date"


def test_not_a_date_string_is_rejected_and_apply_never_raises() -> None:
    normalizer = DateNormalizer(column_index=0, column_name="d", format="%Y-%m-%d")
    chunk = _chunk([("not-a-date",)])

    result = normalizer.apply(_make_context(), chunk)  # must not raise

    assert result.chunk.rows == ()
    assert _single_rejected(result.rejected).error_type == "invalid-calendar-date"


# --- fixture 52: genuinely ambiguous -- the format decides, never a guess --


def test_ambiguous_dm_vs_md_dates_are_decided_by_the_declared_format_not_guessed() -> None:
    # corpus fixture 52_date_ambiguous_dm_vs_md.csv: every component is <= 12,
    # so no row is self-evidencing under either reading -- both readings are
    # valid dates. DateNormalizer never guesses; it parses under whatever
    # format its contract declares, and the SAME raw values produce two
    # different, individually-correct answers depending on which format was
    # declared -- proving the rendering alone carries no evidence.
    raw_values = ["03/04/2026", "01/02/2026", "12/11/2026", "05/06/2026"]
    under_day_first = ["2026-04-03", "2026-02-01", "2026-11-12", "2026-06-05"]
    under_month_first = ["2026-03-04", "2026-01-02", "2026-12-11", "2026-05-06"]

    day_first = DateNormalizer(column_index=1, column_name="event_date", format="%d/%m/%Y")
    month_first = DateNormalizer(column_index=1, column_name="event_date", format="%m/%d/%Y")
    chunk = _chunk([(str(i), v) for i, v in enumerate(raw_values)])

    day_first_result = day_first.apply(_make_context(), chunk)
    month_first_result = month_first.apply(_make_context(), chunk)

    assert day_first_result.rejected == []
    assert [row[1] for row in day_first_result.chunk.rows] == under_day_first
    assert month_first_result.rejected == []
    assert [row[1] for row in month_first_result.chunk.rows] == under_month_first


# --- fixture 53: two-digit years need a declared pivot, never an inherited one --


def test_two_digit_year_pivot_missing_raises_before_any_row_is_processed() -> None:
    # A missing pivot is a contract-authoring mistake, not a row-level
    # problem -- it must fail loudly at construction time.
    with pytest.raises(ValueError, match="two_digit_year_pivot"):
        DateNormalizer(column_index=0, column_name="d", format="%d/%m/%y")


def test_two_digit_year_pivot_declared_reproduces_fixture_53_iso_values() -> None:
    # corpus fixture 53_two_digit_year.csv: pivot=68 matches CPython's own
    # inherited default, chosen so this test has a concrete, checkable value
    # -- but the pivot is OUR declared parameter, not a trust of strptime's
    # own guess (see dataplat.normalize.dates._apply_two_digit_year_pivot).
    raw_values = ["11/08/26", "11/08/68", "11/08/69", "11/08/99"]
    iso_values = ["2026-08-11", "2068-08-11", "1969-08-11", "1999-08-11"]
    normalizer = DateNormalizer(
        column_index=1,
        column_name="event_date",
        format="%d/%m/%y",
        two_digit_year_pivot=68,
    )
    chunk = _chunk([(str(i), v, "note") for i, v in enumerate(raw_values)])

    result = normalizer.apply(_make_context(), chunk)

    assert result.rejected == []
    assert [row[1] for row in result.chunk.rows] == iso_values


def test_a_different_declared_pivot_produces_different_years_than_the_inherited_one() -> None:
    # Proves the pivot logic is deliberate, not accidental: a pivot other
    # than 68 must NOT reproduce fixture 53's inherited-pivot answer for the
    # boundary year "68".
    normalizer = DateNormalizer(
        column_index=0,
        column_name="event_date",
        format="%d/%m/%y",
        two_digit_year_pivot=0,  # every two-digit year resolves to the 1900s
    )
    chunk = _chunk([("11/08/68",)])

    result = normalizer.apply(_make_context(), chunk)

    assert result.rejected == []
    assert result.chunk.rows[0][0] == "1968-08-11"


# --- fixture 54: spreadsheet serials need a declared epoch, never assumed --


def test_excel_serial_dates_reproduce_fixture_54_expected_values_under_1900_epoch() -> None:
    normalizer = DateNormalizer(
        column_index=0,
        column_name="serial_date",
        spreadsheet_epoch="1900",
    )
    chunk = _chunk([("45880",), ("1",)])

    result = normalizer.apply(_make_context(), chunk)

    assert result.rejected == []
    assert result.chunk.rows[0][0] == "2025-08-11"  # serial_45880_under_the_1900_system
    assert result.chunk.rows[1][0] == "1900-01-01"  # excel_shows_for_serial_1


def test_excel_serial_60_is_rejected_as_the_phantom_1900_leap_day() -> None:
    # 1900 is not a leap year; serial 60 denotes a day that never existed,
    # inherited from the Lotus 1-2-3 bug (corpus fixture 54).
    normalizer = DateNormalizer(
        column_index=0,
        column_name="serial_date",
        spreadsheet_epoch="1900",
    )
    chunk = _chunk([("60",)])

    result = normalizer.apply(_make_context(), chunk)

    assert result.chunk.rows == ()
    assert _single_rejected(result.rejected).error_type == "spreadsheet-serial-date-does-not-exist"


def test_excel_serial_45880_under_1904_epoch_differs_from_1900_epoch() -> None:
    # Proves the EPOCH decides the date, not the serial number: the exact
    # same serial produces two different dates, four years apart.
    normalizer = DateNormalizer(
        column_index=0,
        column_name="serial_date",
        spreadsheet_epoch="1904",
    )
    chunk = _chunk([("45880",)])

    result = normalizer.apply(_make_context(), chunk)

    assert result.rejected == []
    assert result.chunk.rows[0][0] == "2029-08-12"  # serial_45880_under_the_1904_system


# --- constructor validation: format/spreadsheet_epoch are mutually exclusive ---


def test_format_and_spreadsheet_epoch_both_supplied_raises() -> None:
    with pytest.raises(ValueError, match=r"format.*spreadsheet_epoch"):
        DateNormalizer(
            column_index=0,
            column_name="d",
            format="%Y-%m-%d",
            spreadsheet_epoch="1900",
        )


def test_neither_format_nor_spreadsheet_epoch_supplied_raises() -> None:
    with pytest.raises(ValueError, match=r"format.*spreadsheet_epoch"):
        DateNormalizer(column_index=0, column_name="d")


def test_unsupported_spreadsheet_epoch_raises() -> None:
    with pytest.raises(ValueError, match="1899"):
        DateNormalizer(column_index=0, column_name="d", spreadsheet_epoch="1899")


# --- acceptance criteria's literal smoke-test invocation --------------------


def test_smoke_construction_matches_the_plan_acceptance_criteria() -> None:
    normalizer = DateNormalizer(
        column_index=0,
        column_name="d",
        format="%Y-%m-%d",
        two_digit_year_pivot=None,
        spreadsheet_epoch=None,
    )
    assert normalizer.name == "date_normalizer"


# --- None passthrough: an already-null-normalized field is never touched ---


def test_none_field_passes_through_unchanged_in_plain_format_branch() -> None:
    normalizer = DateNormalizer(column_index=0, column_name="d", format="%Y-%m-%d")
    chunk = _chunk([(None,)])

    result = normalizer.apply(_make_context(), chunk)

    assert result.rejected == []
    assert len(result.chunk.rows) == 1
    assert _field(result.chunk.rows, 0, 0) is None


def test_none_field_passes_through_unchanged_in_spreadsheet_epoch_branch() -> None:
    normalizer = DateNormalizer(column_index=0, column_name="d", spreadsheet_epoch="1900")
    chunk = _chunk([(None,)])

    result = normalizer.apply(_make_context(), chunk)

    assert result.rejected == []
    assert _field(result.chunk.rows, 0, 0) is None


# --- fixture 59: a sentinel is a contract decision, not a universal rule ---


def test_fixture_59_sentinel_rows_pass_through_once_pre_normalized_to_none() -> None:
    # corpus fixture 59_numeric_null_sentinels.csv's valid_to column: rows 1
    # and 3 are declared sentinels ("9999-12-31", "0000-00-00") that an
    # upstream sentinel-aware normalizer has already converted to None
    # (sentinel_meaning: absent) by the time DateNormalizer runs; row 2 is a
    # genuine date and must still parse normally.
    normalizer = DateNormalizer(column_index=0, column_name="valid_to", format="%Y-%m-%d")
    chunk = _chunk([(None,), ("2026-06-30",), (None,)])

    result = normalizer.apply(_make_context(), chunk)

    assert result.rejected == []
    assert _field(result.chunk.rows, 0, 0) is None
    assert result.chunk.rows[1][0] == "2026-06-30"
    assert _field(result.chunk.rows, 2, 0) is None


def test_fixture_59_zero_date_sentinel_is_refused_by_strptime_if_not_pre_normalized() -> None:
    # zero_date_must_never_be_parsed_as_a_date: even if "0000-00-00" reached
    # DateNormalizer directly (sentinel conversion did not happen upstream),
    # strptime itself refuses year 0 -- it must surface as an explicit
    # rejection, never a silently-accepted date eight thousand years off.
    normalizer = DateNormalizer(column_index=0, column_name="valid_to", format="%Y-%m-%d")
    chunk = _chunk([("0000-00-00",)])

    result = normalizer.apply(_make_context(), chunk)

    assert result.chunk.rows == ()
    assert _single_rejected(result.rejected).error_type == "invalid-calendar-date"
