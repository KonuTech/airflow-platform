"""Unit tests for ``csv_processor.detect.filename`` — CSV-01's D-07/D-08/D-09/D-11 coverage.

Covers ``compile_mask``/``match_filename`` (Task 1) and ``parse_filename``
(Task 2) directly against plain literal filename strings — no corpus
fixture dependency. No filename-mask fixtures exist in
``tests/fixtures/corpus.yaml`` (D-10: ``customers``, the platform's only
real dataset, deliberately does not declare a mask), so this capability's
test oracle is this file alone, per 06-CONTEXT.md's ``canonical_refs``.
"""

from __future__ import annotations

from datetime import date

import pytest

from csv_processor.detect.filename import CompiledMask, compile_mask, match_filename, parse_filename
from dataplat.config.model import FilenameMaskConfig
from dataplat.diagnostics import DIAGNOSTIC_CODES
from dataplat.errors import FilenameParsingError


def test_compile_mask_extracts_every_declared_facet_as_a_typed_value() -> None:
    compiled = compile_mask("{dataset}_{country}_{business_date:%Y%m%d}.csv")

    result = match_filename(compiled, "customers_PL_20260815.csv")

    assert result == {
        "dataset": "customers",
        "country": "PL",
        "business_date": date(2026, 8, 15),
    }


def test_optional_segment_absent_omits_the_facet_entirely() -> None:
    compiled = compile_mask("{dataset}[_{seq:03d}].csv")

    result = match_filename(compiled, "customers.csv")

    assert result == {"dataset": "customers"}
    assert "seq" not in result


def test_optional_segment_present_extracts_it_as_an_int() -> None:
    compiled = compile_mask("{dataset}[_{seq:03d}].csv")

    result = match_filename(compiled, "customers_007.csv")

    assert result == {"dataset": "customers", "seq": 7}
    assert isinstance(result["seq"], int)


def test_no_match_at_all_returns_none_not_a_partial_dict() -> None:
    compiled = compile_mask("{dataset}_{country}.csv")

    result = match_filename(compiled, "totally-different-shape.txt")

    assert result is None


def test_whole_string_anchor_rejects_a_filename_matching_only_a_prefix() -> None:
    compiled = compile_mask("{dataset}_{country}_{business_date:%Y%m%d}.csv")

    result = match_filename(compiled, "customers_PL_20260815_extra_junk.csv")

    assert result is None


def test_whole_string_anchor_rejects_prefix_match_on_optional_seq_mask() -> None:
    """The plan's acceptance-criteria filename: prefix rejection with an optional facet present."""
    compiled = compile_mask("{dataset}_{country}_{business_date:%Y%m%d}[_{seq:03d}].csv")

    result = match_filename(compiled, "customers_PL_20260815_extra.csv")

    assert result is None


def test_malformed_mask_unclosed_bracket_raises_at_compile_time() -> None:
    with pytest.raises(ValueError, match="unclosed"):
        compile_mask("{dataset}[_{seq:03d}.csv")


def test_malformed_mask_unmatched_closing_bracket_raises_at_compile_time() -> None:
    with pytest.raises(ValueError, match="unmatched"):
        compile_mask("{dataset}]_{seq:03d}[.csv")


def test_malformed_mask_nested_brackets_raise_at_compile_time() -> None:
    with pytest.raises(ValueError, match="nested"):
        compile_mask("{dataset}[[_{seq:03d}]].csv")


def test_malformed_mask_unrecognized_format_spec_raises_at_compile_time() -> None:
    with pytest.raises(ValueError, match="unrecognized format spec"):
        compile_mask("{dataset}_{x:bogus}.csv")


def test_mask_using_every_supported_strptime_directive() -> None:
    compiled = compile_mask("{dataset}_{ts:%Y%m%d%H%M%S}.csv")

    result = match_filename(compiled, "customers_20260815143022.csv")

    assert result == {"dataset": "customers", "ts": date(2026, 8, 15)}


def test_mask_with_two_independent_optional_bracket_segments() -> None:
    """Fixed-width facets of different lengths disambiguate the two segments.

    An 8-digit date facet and a 3-digit zero-padded-int facet, each in its
    own bracket, can never be confused with each other regardless of which
    is present, absent, or both — unlike two adjacent free-form optional
    facets, where the leftmost (tried first by the regex engine) would
    greedily claim characters meant for a later one.
    """
    compiled = compile_mask("{dataset}[_{as_of:%Y%m%d}][_{seq:03d}].csv")

    assert match_filename(compiled, "customers_20260815_007.csv") == {
        "dataset": "customers",
        "as_of": date(2026, 8, 15),
        "seq": 7,
    }
    assert match_filename(compiled, "customers_20260815.csv") == {
        "dataset": "customers",
        "as_of": date(2026, 8, 15),
    }
    assert match_filename(compiled, "customers_007.csv") == {
        "dataset": "customers",
        "seq": 7,
    }
    assert match_filename(compiled, "customers.csv") == {"dataset": "customers"}


def test_literal_mask_characters_are_escaped_not_treated_as_regex_metacharacters() -> None:
    """A literal "." in a mask (e.g. before "csv") must match only a literal dot.

    An un-escaped mask compiler would emit a bare "." into the regex, which
    matches ANY character — silently accepting a filename that does not
    actually contain that literal character.
    """
    compiled = compile_mask("{dataset}.csv")

    assert match_filename(compiled, "customersXcsv") is None
    assert match_filename(compiled, "customers.csv") == {"dataset": "customers"}


def test_compile_mask_returns_a_compiled_mask_instance() -> None:
    compiled = compile_mask("{dataset}.csv")

    assert isinstance(compiled, CompiledMask)


# --- Task 2: parse_filename (D-09 reject-on-no-match, D-11 business_date facet) ---


def test_parse_filename_raises_filename_parsing_error_with_diagnostic_code_on_no_match() -> None:
    mask_config = FilenameMaskConfig(mask="{dataset}.csv")

    with pytest.raises(FilenameParsingError) as exc_info:
        parse_filename(mask_config, "nope.txt")

    assert exc_info.value.context["diagnostic_code"] == "filename-does-not-match-mask"
    assert exc_info.value.context["filename"] == "nope.txt"
    assert exc_info.value.context["mask"] == "{dataset}.csv"


def test_filename_does_not_match_mask_diagnostic_code_is_in_the_shared_catalog() -> None:
    """D-24's drift guard, applied to this module's one raise site.

    Keeps ``parse_filename``'s literal in sync with
    ``dataplat.diagnostics.DIAGNOSTIC_CODES`` — a rename on either side
    without the other becomes a failing test here, not a silent mismatch.
    """
    assert "filename-does-not-match-mask" in DIAGNOSTIC_CODES


def test_parse_filename_returns_business_date_as_a_date_when_mask_declares_one() -> None:
    mask_config = FilenameMaskConfig(mask="{dataset}_{business_date:%Y%m%d}.csv")

    result = parse_filename(mask_config, "customers_20260815.csv")

    assert isinstance(result["business_date"], date)
    assert result["business_date"] == date(2026, 8, 15)


def test_parse_filename_matches_and_returns_every_facet() -> None:
    mask_config = FilenameMaskConfig(
        mask="{dataset}_{country}_{business_date:%Y%m%d}[_{seq:03d}].csv",
    )

    result = parse_filename(mask_config, "customers_PL_20260815_007.csv")

    assert result == {
        "dataset": "customers",
        "country": "PL",
        "business_date": date(2026, 8, 15),
        "seq": 7,
    }
