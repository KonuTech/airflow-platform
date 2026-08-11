"""The structural claims the digest oracle cannot make (QUAL-08).

``CORPUS.sha256`` proves the bytes did not drift. It cannot prove the bytes have
the *shape* their declaration claims. A ragged fixture whose rows are all the
same width, a "very wide" fixture with nine columns, or an unclosed-quote
fixture whose quote happens to close all have perfectly stable digests, and
every Phase 6 detector test written against them inherits the mistake with
nothing in the repository noticing.

So each test below parses a generated fixture and asserts the one structural
property its declaration exists to assert — the field-count distribution around
the modal count, the gap between record count and line count, the phantom
trailing column, the missing final terminator, the column width, and the
distinct-row count.

Every assertion is compared against the fixture's own ``expect:`` block wherever
the declaration states a number. That is deliberate: a test that restates the
expectation in Python has two copies of the truth and no way to notice when they
disagree.

The corpus is generated into a temporary directory rather than read from
``tests/fixtures/csv/``, following ``test_corpus_byte_level_fixtures.py``: these
tests then pass on a clean checkout, and a difference between a committed file
and a freshly generated one is ``fixtures-verify``'s failure to report, not
this module's.
"""

from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from tools.corpus.generators import generate_corpus, output_names
from tools.corpus.manifest import load_manifest

if TYPE_CHECKING:
    from collections.abc import Mapping

REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST = REPO_ROOT / "tests" / "fixtures" / "corpus.yaml"

# Everything plan 01-07 declared. Named explicitly rather than derived from the
# manifest, so deleting a declaration fails a test instead of shrinking a loop.
STRUCTURAL_FIXTURES = (
    "02_semicolon.csv",
    "03_pipe.csv",
    "04_tab.csv",
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
    "25_duplicate_rows.csv",
    "33_ragged_rows.csv",
    "34_unclosed_quote_eof.csv",
    "35_quote_in_unquoted_field.csv",
    "36_doubled_vs_backslash_escape.csv",
    "37_delimiter_frequency_differs_header_vs_body.csv",
    "38_single_column_no_delimiter.csv",
    "45_trailing_delimiter_every_row.csv",
    "46_no_trailing_newline.csv",
    "47_blank_lines_interspersed.csv",
    "48_duplicate_header_names_case_variant.csv",
    "49_header_with_leading_trailing_spaces.csv",
    "63_repeated_header_mid_file.csv",
    "64_footer_totals_with_different_column_count.csv",
    "65_very_wide.csv",
    "66_triple_nasty.csv",
)

ONE_THOUSAND = 1000


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


def _field_counts(path: Path, delimiter: str = ",") -> list[int]:
    """Return the field count of every parsed record, header first."""
    return [len(record) for record in _records(path, delimiter)]


def test_the_ragged_fixture_is_ragged_in_both_directions(
    corpus: Path, declared: Mapping[str, Mapping[str, Any]]
) -> None:
    # The highest-severity case, and the one a half-fixture would let through: a
    # reader that pads short rows passes a file that is only long, and a reader
    # that truncates long rows passes a file that is only short. Both failures
    # are silent, so the file has to contain both shapes at once.
    expect = declared["33_ragged_rows.csv"]
    counts = _field_counts(corpus / "33_ragged_rows.csv")
    data = counts[1:]

    modal = Counter(data).most_common(1)[0][0]
    below = sorted({count for count in data if count < modal})
    above = sorted({count for count in data if count > modal})

    assert modal == expect["modal_field_count"]
    assert len(below) >= 2, f"only {below!r} below the modal count of {modal}"
    assert len(above) >= 2, f"only {above!r} above the modal count of {modal}"
    assert sorted(set(data) | {counts[0]}) == expect["distinct_field_counts"]
    assert dict(enumerate(counts)) == expect["field_count_by_row_index"]


def test_the_unclosed_quote_swallows_the_rest_of_the_file(
    corpus: Path, declared: Mapping[str, Mapping[str, Any]]
) -> None:
    # The gap between line count and record count IS the finding. If the quote
    # ever closed, this file would parse to six tidy records and would silently
    # stop being the fixture it claims to be — with an unchanged digest.
    expect = declared["34_unclosed_quote_eof.csv"]
    path = corpus / "34_unclosed_quote_eof.csv"
    payload = path.read_bytes()
    records = _records(path)

    assert payload.count(b"\n") == expect["physical_line_count"]
    assert len(records) == expect["record_count_including_header"]
    assert len(records) < payload.count(b"\n"), "the quote closed; nothing was swallowed"
    # The swallowed remainder really is inside one field, terminators and all.
    assert "\n" in records[-1][-1]
    assert records[-1][-1].count("\n") == expect["rows_swallowed_by_the_open_quote"]


def test_every_row_of_the_trailing_delimiter_fixture_ends_with_it(
    corpus: Path, declared: Mapping[str, Mapping[str, Any]]
) -> None:
    # "Every row" is the claim, not "the first row": a reader that special-cases
    # the header would pass a fixture where only the header is affected.
    expect = declared["45_trailing_delimiter_every_row.csv"]
    path = corpus / "45_trailing_delimiter_every_row.csv"
    lines = path.read_text(encoding="utf-8").splitlines()
    records = _records(path)

    assert all(line.endswith(",") for line in lines), lines
    assert dict(enumerate(len(record) for record in records)) == expect["field_count_by_row_index"]
    phantom = expect["phantom_final_column_index"]
    assert all(record[phantom] == "" for record in records)
    assert len(records[0]) == expect["declared_columns"] + 1


def test_the_final_record_is_not_terminated(
    corpus: Path, declared: Mapping[str, Mapping[str, Any]]
) -> None:
    # A file that ends with a terminator cannot prove the last record survives.
    expect = declared["46_no_trailing_newline.csv"]
    path = corpus / "46_no_trailing_newline.csv"
    payload = path.read_bytes()
    records = _records(path)

    assert payload[-1:] not in (b"\n", b"\r"), f"final byte is {payload[-1:]!r}"
    assert expect["final_byte_is_a_line_terminator"] is False
    assert payload.count(b"\n") == expect["terminator_count"]
    assert len(records) == expect["record_count_including_header"]
    assert records[-1] == expect["last_row_values"]


def test_the_very_wide_fixture_exceeds_a_thousand_columns(
    corpus: Path, declared: Mapping[str, Mapping[str, Any]]
) -> None:
    # Width is the entire fixture. A generation slip that emitted nine columns
    # would still hash stably and would still be named "very wide".
    expect = declared["65_very_wide.csv"]
    records = _records(corpus / "65_very_wide.csv")
    header, rows = records[0], records[1:]

    assert len(header) > ONE_THOUSAND, f"header has only {len(header)} fields"
    assert len(header) == expect["column_count"]
    assert header[0] == expect["first_column_name"]
    assert header[-1] == expect["last_column_name"]
    assert {len(row) for row in rows} == {len(header)}
    assert len(rows) == expect["data_rows"]
    assert rows[0][0] == expect["row_1_first_value"]
    assert rows[0][-1] == expect["row_1_last_value"]
    assert sum(field == "" for field in rows[0]) == expect["empty_fields_per_row"]


def test_the_duplicate_rows_fixture_has_the_declared_distinct_count(
    corpus: Path, declared: Mapping[str, Mapping[str, Any]]
) -> None:
    # Phase 9 measures deduplication against this difference, so both numbers
    # have to be true of the bytes and not only of the prose.
    expect = declared["25_duplicate_rows.csv"]
    records = _records(corpus / "25_duplicate_rows.csv")
    data = [tuple(record) for record in records[1:]]

    assert len(data) == expect["data_rows"]
    assert len(set(data)) == expect["distinct_rows"]
    assert len(data) - len(set(data)) == expect["duplicate_rows_removed"]
    assert Counter(row[0] for row in data) == expect["occurrences_by_id"]


def test_the_empty_and_header_only_fixtures_are_distinguishable(
    corpus: Path, declared: Mapping[str, Mapping[str, Any]]
) -> None:
    # Not merely different sizes: different declared meanings. An operator told
    # "0 rows" needs to know whether the schema arrived.
    empty = corpus / "18_empty.csv"
    header_only = corpus / "19_only_header.csv"

    assert empty.read_bytes() == b""
    assert header_only.read_bytes() != b""
    assert _records(empty) == []
    assert _records(header_only) == [list(declared["19_only_header.csv"]["header"])]

    assert declared["18_empty.csv"]["has_header"] is False
    assert declared["19_only_header.csv"]["has_header"] is True
    assert declared["18_empty.csv"]["outcome"] != declared["19_only_header.csv"]["outcome"]


def test_the_single_column_fixture_gives_a_detector_nothing_to_work_with(
    corpus: Path, declared: Mapping[str, Mapping[str, Any]]
) -> None:
    # If any candidate delimiter crept into the data, a detector could return a
    # plausible answer and the fixture would stop testing the decline path.
    expect = declared["38_single_column_no_delimiter.csv"]
    path = corpus / "38_single_column_no_delimiter.csv"
    text = path.read_text(encoding="utf-8")

    for candidate in expect["candidate_delimiters_absent"]:
        assert candidate not in text, f"{candidate!r} appears in a single-column fixture"
    assert {len(record) for record in _records(path)} == {expect["column_count"]}

    # And the stdlib really does raise rather than decline, which is why the
    # declaration says so instead of trusting the sniffer.
    with pytest.raises(csv.Error, match=expect["stdlib_sniffer_error"]):
        csv.Sniffer().sniff(text)


def test_the_triple_nasty_field_carries_all_three_hazards_at_once(
    corpus: Path, declared: Mapping[str, Mapping[str, Any]]
) -> None:
    # Declared field values, so a reader that handles each hazard alone fails
    # visibly rather than approximately.
    expect = declared["66_triple_nasty.csv"]
    path = corpus / "66_triple_nasty.csv"
    records = _records(path)

    assert path.read_bytes().count(b"\n") == expect["physical_line_count"]
    assert len(records) == expect["record_count_including_header"]
    assert records[1] == list(expect["value_row_1"])

    payload = records[1][1]
    assert "," in payload
    assert "\n" in payload
    assert '"' in payload


def test_the_delimiter_decision_must_come_from_the_body(
    corpus: Path, declared: Mapping[str, Mapping[str, Any]]
) -> None:
    # The trap only exists while the header and the body genuinely disagree.
    expect = declared["37_delimiter_frequency_differs_header_vs_body.csv"]
    path = corpus / "37_delimiter_frequency_differs_header_vs_body.csv"
    header_line, *body_lines = path.read_text(encoding="utf-8").splitlines()

    for character, count in expect["header_delimiter_counts"].items():
        assert header_line.count(character) == count
    for line in body_lines:
        for character, count in expect["body_delimiter_counts_per_row"].items():
            assert line.count(character) == count

    correct = _field_counts(path, delimiter=expect["correct_delimiter"])
    assert set(correct) == {expect["field_count_with_the_correct_delimiter"]}
    wrong = _field_counts(path, delimiter=expect["first_line_frequency_heuristic_picks"])
    assert wrong[0] == expect["header_field_count_if_comma_is_chosen"]
    assert set(wrong[1:]) == {expect["body_field_count_if_comma_is_chosen"]}


def test_every_structural_declaration_produced_a_file(corpus: Path) -> None:
    # The quiet failure this module is about: a declaration whose fixture no
    # longer exists still reads convincingly in the manifest.
    manifest = load_manifest(MANIFEST)
    by_name = {fixture.name: fixture for fixture in manifest.fixtures}

    missing = [name for name in STRUCTURAL_FIXTURES if name not in by_name]
    assert not missing, f"declared by plan 01-07 but absent from the manifest: {missing}"

    ungenerated = [
        name
        for name in STRUCTURAL_FIXTURES
        for relative in output_names(by_name[name])
        if not (corpus / relative).exists()
    ]
    assert not ungenerated, f"declared but not generated: {ungenerated}"
