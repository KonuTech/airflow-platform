"""The semantic claims the digest oracle cannot make (QUAL-08).

``CORPUS.sha256`` proves the bytes did not drift. It cannot prove the bytes still
*mean* what their declaration says. A scientific-notation fixture whose rendered
identifier happens to carry all fifteen digits, an "ambiguous" date fixture with
a day of 31 in it, or a daylight-saving fixture pointing at a zone whose
transitions moved would all hash perfectly stably — and every Phase 6 normaliser
written against them would inherit the mistake with nothing in the repository
noticing.

These fixtures are the ones where that matters most, because they do not merely
describe a shape: they pin a DECISION. 50 and 51 declare damage as unrecoverable,
52 declares that a format cannot be inferred, 55 declares two local times as
nonexistent and ambiguous, and 70 declares that present-and-empty is not absent.
A fixture that quietly stopped carrying its damage would turn each of those
declarations into a test that passes for the wrong reason.

Every assertion is compared against the fixture's own ``expect:`` block wherever
the declaration states a value, following
``test_corpus_structural_fixtures.py``: a test that restates the expectation in
Python has two copies of the truth and no way to notice when they disagree. The
daylight-saving test is the one deliberate exception — it re-derives the
classifications from ``zoneinfo`` rather than trusting the manifest, because the
manifest is exactly what it exists to check.

The corpus is generated into a temporary directory rather than read from
``tests/fixtures/csv/``, so these tests pass on a clean checkout.
"""

from __future__ import annotations

import csv
import datetime
import zoneinfo
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from tools.corpus.generators import generate_corpus
from tools.corpus.manifest import load_manifest

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST = REPO_ROOT / "tests" / "fixtures" / "corpus.yaml"

UTC = datetime.UTC

# The largest value a day-or-month component can take while leaving the
# day-first/month-first question genuinely undecidable.
AMBIGUOUS_COMPONENT_CEILING = 12

# The only expectation keys allowed to hold a binary float. A detector's
# confidence is a probability rather than a value that must round-trip; every
# other number in an `expect:` block describes data and is a quoted decimal.
CONFIDENCE_KEYS = ("encoding_confidence_min", "encoding_confidence_max")

# The seventy fixture names the corpus is complete against, transcribed from
# the two sources rather than derived from the manifest. Deriving them from the
# manifest would make this test a tautology: it would assert that the manifest
# contains what the manifest contains, and would pass with a fixture missing.
#
# README §73 names 01-29. `.planning/research/FEATURES.md` §3.4 adds 30-70.
# Note that 69 is not a fixture — the second list runs 68 then 70, then Phase 6
# plan 01 appends 71_zipped.csv.zip as the corpus's 70th fixture (README §73's
# own "grow the corpus as edge cases are discovered" policy).
README_SEVENTY_THREE = (
    "01_simple.csv",
    "02_semicolon.csv",
    "03_pipe.csv",
    "04_tab.csv",
    "05_utf8_bom.csv",
    "06_windows1250.csv",
    "07_utf16.csv",
    "08_quoted_fields.csv",
    "09_embedded_commas.csv",
    "10_embedded_newlines.csv",
    "11_no_header.csv",
    "12_metadata_before_header.csv",
    "13_footer.csv",
    "14_duplicate_columns.csv",
    "15_missing_columns.csv",
    "16_extra_columns.csv",
    "17_malformed_rows.csv",
    "18_empty.csv",
    "19_only_header.csv",
    "20_decimal_comma.csv",
    "21_decimal_point.csv",
    "22_eu_dates.csv",
    "23_us_dates.csv",
    "24_null_values.csv",
    "25_duplicate_rows.csv",
    "26_unicode.csv",
    "27_polish_characters.csv",
    "28_large_fields.csv",
    "29_large_file.csv",
)

FEATURES_THREE_FOUR = (
    "30_crlf_lf_mixed.csv",
    "31_cr_only.csv",
    "32_nul_bytes.csv",
    "33_ragged_rows.csv",
    "34_unclosed_quote_eof.csv",
    "35_quote_in_unquoted_field.csv",
    "36_doubled_vs_backslash_escape.csv",
    "37_delimiter_frequency_differs_header_vs_body.csv",
    "38_single_column_no_delimiter.csv",
    "39_utf8_invalid_sequences.csv",
    "40_utf16_no_bom.csv",
    "41_bom_mid_file.csv",
    "42_zero_width_and_bidi.csv",
    "43_nbsp_thousands_separator.csv",
    "44_unicode_nfc_vs_nfd.csv",
    "45_trailing_delimiter_every_row.csv",
    "46_no_trailing_newline.csv",
    "47_blank_lines_interspersed.csv",
    "48_duplicate_header_names_case_variant.csv",
    "49_header_with_leading_trailing_spaces.csv",
    "50_excel_scientific_notation_ids.csv",
    "51_excel_leading_zero_stripped.csv",
    "52_date_ambiguous_dm_vs_md.csv",
    "53_two_digit_year.csv",
    "54_excel_serial_dates.csv",
    "55_dst_gap_and_overlap.csv",
    "56_mixed_timezone_offsets.csv",
    "57_negative_parentheses_and_trailing_minus.csv",
    "58_currency_and_percent.csv",
    "59_numeric_null_sentinels.csv",
    "60_boolean_localized.csv",
    "61_gzipped.csv.gz",
    "62_multipart_split",
    "63_repeated_header_mid_file.csv",
    "64_footer_totals_with_different_column_count.csv",
    "65_very_wide.csv",
    "66_triple_nasty.csv",
    "67_row_exceeding_field_size_limit.csv",
    "68_utf8_bom_semicolon_pl_excel.csv",
    "70_empty_last_field_vs_null.csv",
    "71_zipped.csv.zip",
)

EXPECTED_FIXTURES = README_SEVENTY_THREE + FEATURES_THREE_FOUR


@pytest.fixture(scope="module")
def corpus(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Generate the corpus once, skipping the ~293 MB large-profile fixture."""
    manifest = load_manifest(MANIFEST)
    out_dir = tmp_path_factory.mktemp("corpus")
    generate_corpus(manifest, out_dir, fast=True)
    return out_dir


@pytest.fixture(scope="module")
def declared() -> Mapping[str, Mapping[str, Any]]:
    """Return each fixture's declared meaning, keyed by fixture name."""
    return {fixture.name: fixture.expect for fixture in load_manifest(MANIFEST).fixtures}


def _records(path: Path, delimiter: str = ",") -> list[list[str]]:
    """Parse a generated fixture into records, newline handling left to the parser."""
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.reader(handle, delimiter=delimiter))


def _significant_digits(value: str) -> int:
    """Count significant digits in a decimal or exponent-form literal."""
    mantissa = value.split("E", maxsplit=1)[0].split("e", maxsplit=1)[0]
    return len(mantissa.replace(".", "").replace("-", "").lstrip("0"))


def _classify_local_time(naive: datetime.datetime, zone: zoneinfo.ZoneInfo) -> str:
    """Classify a naive local time against a real zone (PEP 495).

    A local time is *nonexistent* when it does not survive a round trip through
    UTC — the spring-forward gap skipped it, so converting back yields a
    different wall clock. It is *ambiguous* when it does survive but ``fold=0``
    and ``fold=1`` resolve to different instants, which is the autumn overlap.

    Comparing UTC offsets across the two folds is the obvious implementation and
    it is wrong: it is true for both cases and therefore distinguishes neither.
    """
    first = naive.replace(tzinfo=zone, fold=0)
    second = naive.replace(tzinfo=zone, fold=1)
    if first.astimezone(UTC).astimezone(zone).replace(tzinfo=None) != naive:
        return "nonexistent"
    if first.astimezone(UTC) != second.astimezone(UTC):
        return "ambiguous"
    return "unambiguous"


def test_the_scientific_notation_fixture_really_lost_digits(
    corpus: Path, declared: Mapping[str, Mapping[str, Any]]
) -> None:
    # The decision this fixture pins is "unrecoverable". If the rendered value
    # ever carried all fifteen digits, the damage would be reversible and the
    # declared rejection would be indefensible — while the digest stayed stable.
    expect = declared["50_excel_scientific_notation_ids.csv"]
    records = _records(corpus / "50_excel_scientific_notation_ids.csv")
    rendered = [record[1] for record in records[1:]]

    assert rendered[0] == expect["rendered_value_row_1"]
    assert rendered[1] == expect["rendered_value_row_2"]

    original = expect["original_value_row_1"]
    assert _significant_digits(original) == expect["significant_digits_original"]
    assert _significant_digits(rendered[0]) == expect["significant_digits_rendered"]
    assert _significant_digits(rendered[0]) < _significant_digits(original), (
        "the rendered identifier still carries every digit; nothing was lost"
    )
    assert (
        expect["significant_digits_original"] - expect["significant_digits_rendered"]
        == expect["digits_lost"]
    )

    # Expanding the exponent form yields a DIFFERENT identifier — which is the
    # whole argument against "helpfully" coercing it back to a number.
    expanded = Decimal(rendered[0])
    assert expanded == Decimal(expect["expanding_the_rendered_form_yields_row_1"])
    assert expanded != Decimal(original)

    # The clean control survived intact, so "detects the damage" and "rejects
    # every long number" are distinguishable.
    assert records[3][1] == expect["clean_control_value"]
    assert _significant_digits(records[3][1]) == expect["significant_digits_original"]


def test_the_leading_zero_fixture_differs_by_the_leading_zeros_alone(
    corpus: Path, declared: Mapping[str, Mapping[str, Any]]
) -> None:
    # The claim is precise: the damage is the loss of leading zeros and nothing
    # else. If the two values differed in any other position the fixture would
    # be describing a different kind of corruption than it declares.
    expect = declared["51_excel_leading_zero_stripped.csv"]
    records = _records(corpus / "51_excel_leading_zero_stripped.csv")
    postcodes = [record[1] for record in records[1:]]

    assert postcodes[0] == expect["damaged_value_row_1"]
    assert postcodes[1] == expect["damaged_value_row_2"]

    for original_key, damaged in (
        ("original_value_row_1", postcodes[0]),
        ("original_value_row_2", postcodes[1]),
    ):
        original = expect[original_key]
        assert original.lstrip("0") == damaged, (
            f"{original!r} differs from {damaged!r} by more than leading zeros"
        )
        assert original != damaged
        assert len(original) == expect["declared_width"]
        assert len(damaged) < expect["declared_width"]

    assert len(postcodes[0]) == expect["rendered_width_row_1"]
    assert len(postcodes[1]) == expect["rendered_width_row_2"]

    # The control is already at the declared width, so it loses nothing.
    assert postcodes[2] == expect["clean_control_value"]
    assert len(postcodes[2]) == expect["declared_width"]


def test_every_component_of_the_ambiguous_date_fixture_is_twelve_or_below(
    corpus: Path, declared: Mapping[str, Mapping[str, Any]]
) -> None:
    # A single value with a day of 31 would make the file self-evidencing and
    # silently destroy the only property it exists to have: that the DATA cannot
    # decide the format.
    expect = declared["52_date_ambiguous_dm_vs_md.csv"]
    records = _records(corpus / "52_date_ambiguous_dm_vs_md.csv")
    values = [record[1] for record in records[1:]]

    assert values == list(expect["raw_values"])

    components = [(int(value[:2]), int(value[3:5])) for value in values]
    for first, second in components:
        assert first <= AMBIGUOUS_COMPONENT_CEILING, f"{first} decides the format on its own"
        assert second <= AMBIGUOUS_COMPONENT_CEILING, f"{second} decides the format on its own"

    assert max(first for first, _ in components) == expect["max_first_component"]
    assert max(second for _, second in components) == expect["max_second_component"]

    # Both readings are real dates, so neither raises — which is precisely why a
    # guess produces a plausible wrong date instead of an error.
    day_first = [datetime.datetime.strptime(v, "%d/%m/%Y").date().isoformat() for v in values]  # noqa: DTZ007
    month_first = [datetime.datetime.strptime(v, "%m/%d/%Y").date().isoformat() for v in values]  # noqa: DTZ007
    assert day_first == list(expect["under_day_first"])
    assert month_first == list(expect["under_month_first"])
    assert day_first != month_first


def test_the_daylight_saving_times_are_really_nonexistent_and_ambiguous(
    corpus: Path, declared: Mapping[str, Mapping[str, Any]]
) -> None:
    # This is the one test that does NOT trust the manifest: it re-derives each
    # classification from zoneinfo. The declaration names a real zone and a real
    # year, and if a future tzdata update moved Poland's transitions, the
    # fixture would silently stop testing the gap and the overlap.
    expect = declared["55_dst_gap_and_overlap.csv"]
    zone = zoneinfo.ZoneInfo(expect["timezone"])
    records = _records(corpus / "55_dst_gap_and_overlap.csv")
    values = [record[1] for record in records[1:]]

    assert values == list(expect["raw_values"])

    classifications = [
        _classify_local_time(datetime.datetime.strptime(value, expect["timestamp_format"]), zone)  # noqa: DTZ007
        for value in values
    ]
    assert classifications[0] == expect["row_1_classification"] == "nonexistent"
    assert classifications[1] == expect["row_2_classification"] == "ambiguous"
    assert classifications[2] == expect["row_3_classification"] == "unambiguous"

    # The two damaged rows share a wall-clock time and differ only in their date,
    # which is what makes a single "parse the timestamp" code path insufficient.
    gap, overlap = (
        datetime.datetime.strptime(v, expect["timestamp_format"])  # noqa: DTZ007
        for v in values[:2]
    )
    shared_wall_clock = expect["the_gap_and_the_overlap_share_the_same_wall_clock_time"]
    assert gap.time().isoformat() == overlap.time().isoformat()
    assert gap.time().isoformat() == shared_wall_clock

    # The declared instants are facts about the zone, not transcription.
    assert (
        gap.replace(tzinfo=zone, fold=0).astimezone(UTC).isoformat()
        == expect["row_1_utc_under_fold_0"]
    )
    assert (
        overlap.replace(tzinfo=zone, fold=1).astimezone(UTC).isoformat()
        == expect["row_2_utc_under_fold_1"]
    )
    round_tripped = gap.replace(tzinfo=zone, fold=0).astimezone(UTC).astimezone(zone)
    assert (
        round_tripped.strftime(expect["timestamp_format"])
        == expect["row_1_round_trips_through_utc_to"]
    )


def test_the_final_field_is_present_and_empty_not_absent(
    corpus: Path, declared: Mapping[str, Mapping[str, Any]]
) -> None:
    # The subtlest declaration in the corpus, and the easiest to lose: if row 1
    # ever stopped ending with the delimiter, both rows would be short and the
    # distinction the fixture exists to draw would vanish silently.
    expect = declared["70_empty_last_field_vs_null.csv"]
    path = corpus / "70_empty_last_field_vs_null.csv"
    records = _records(path)
    lines = path.read_text(encoding="utf-8").splitlines()

    assert dict(enumerate(len(record) for record in records)) == expect["field_count_by_row_index"]

    header, present_and_empty, absent, populated = records
    assert len(present_and_empty) == len(header), "the empty field is not present"
    assert present_and_empty[-1] == expect["row_1_comment_value"]
    assert len(absent) < len(header), "the absent field is present after all"
    assert populated[-1] == expect["row_3_comment_value"]

    # The byte-level evidence: exactly one of the two rows ends with the
    # delimiter, and that is the only thing distinguishing them.
    assert lines[1].endswith(",")
    assert not lines[2].endswith(",")


def test_the_null_and_boolean_tokens_are_matched_exactly_not_by_substring(
    corpus: Path, declared: Mapping[str, Mapping[str, Any]]
) -> None:
    # 24's row 8 is a company name containing the token, and 60's row 8 is a
    # value outside the mapping. Both exist so that a reader which matches
    # loosely, or defaults quietly, produces a different answer than one which
    # does not.
    nulls = declared["24_null_values.csv"]
    null_records = _records(corpus / "24_null_values.csv")
    tokens = frozenset(nulls["declared_null_tokens"])

    literal_index = nulls["literal_string_row_indices"][0]
    literal_value = null_records[literal_index][1]
    assert literal_value == nulls["row_8_value"]
    assert literal_value not in tokens, "the company name is being treated as a null token"
    assert any(token in literal_value for token in tokens if token), (
        "the company name no longer contains a null token, so it cannot catch substring matching"
    )
    for index in nulls["absent_row_indices"]:
        assert null_records[index][1] in tokens

    booleans = declared["60_boolean_localized.csv"]
    boolean_records = _records(corpus / "60_boolean_localized.csv")
    true_tokens = frozenset(booleans["declared_true_tokens"])
    false_tokens = frozenset(booleans["declared_false_tokens"])

    assert not (true_tokens & false_tokens), "a token maps to both true and false"
    for index in booleans["true_row_indices"]:
        assert boolean_records[index][1] in true_tokens
    for index in booleans["false_row_indices"]:
        assert boolean_records[index][1] in false_tokens
    for index in booleans["unmapped_row_indices"]:
        value = boolean_records[index][1]
        assert value == booleans["unmapped_value"]
        assert value not in true_tokens | false_tokens


def test_no_data_valued_expectation_is_a_binary_float(
    declared: Mapping[str, Mapping[str, Any]],
) -> None:
    # A corpus that exists to pin numeric fidelity must not state its own
    # expectations in the representation that loses it. 0.1 + 0.2 is not 0.3,
    # and the specification is the one place that must never be true. Every
    # amount, identifier and normalised value is therefore a quoted decimal.
    #
    # The exemption is narrow and deliberate: a detector's CONFIDENCE is a
    # probability, not a value that has to round-trip. `encoding_confidence_min:
    # 1.0` is a threshold a float compares against exactly as well as a decimal
    # would, and writing it as a string would imply a precision claim nobody is
    # making. The exemption is a fixed list rather than a pattern so a new float
    # anywhere else still fails, and each exempted key is asserted to exist so
    # the list cannot quietly outlive the declarations it covers.
    def _floats(value: object, path: str) -> Iterator[tuple[str, float]]:
        if isinstance(value, float):
            yield path, value
        elif isinstance(value, dict):
            for key, item in value.items():
                yield from _floats(item, f"{path}.{key}")
        elif isinstance(value, list):
            for index, item in enumerate(value):
                yield from _floats(item, f"{path}[{index}]")

    offenders = [
        (name, path, value)
        for name, expect in declared.items()
        for path, value in _floats(dict(expect), name)
        if path.rsplit(".", 1)[-1] not in CONFIDENCE_KEYS
    ]
    assert not offenders, f"data-valued expectations written as binary floats: {offenders}"

    exempted_in_use = {
        key for expect in declared.values() for key in expect if key in CONFIDENCE_KEYS
    }
    assert exempted_in_use == set(CONFIDENCE_KEYS), (
        f"the float exemption lists keys no fixture declares: "
        f"{set(CONFIDENCE_KEYS) - exempted_in_use}"
    )


def test_the_corpus_is_complete_against_both_sources() -> None:
    # The claim "the corpus is complete" is the one a reader is least able to
    # check by eye and most likely to believe. Seventy names, no gaps, no
    # duplicates, no inventions — asserted against a list transcribed from the
    # two sources rather than derived from the manifest.
    manifest = load_manifest(MANIFEST)
    declared_names = [fixture.name for fixture in manifest.fixtures]

    duplicates = sorted({name for name in declared_names if declared_names.count(name) > 1})
    assert not duplicates, f"declared more than once: {duplicates}"

    missing = sorted(set(EXPECTED_FIXTURES) - set(declared_names))
    invented = sorted(set(declared_names) - set(EXPECTED_FIXTURES))
    assert not missing, f"named in README §73 or FEATURES.md §3.4 but not declared: {missing}"
    assert not invented, f"declared but named in neither source: {invented}"
    assert len(declared_names) == len(EXPECTED_FIXTURES)
