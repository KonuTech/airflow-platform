"""Corpus-parametrized tests for ``csv_processor.detect.header.detect_header`` (CSV-07/CSV-08).

Every assertion is checked against a fixture's own ``expect:`` block, never
restated independently -- ``tests/fixtures/corpus.yaml``'s own framing:
"Phase 6's detector tests are a parametrised loop over these declarations."
Only fixtures 01, 11, 12, 13, 14, 18, 19, 48, 49, 63 and 64 -- the ones whose
``covers:`` list names CSV-07 or CSV-08 -- are exercised here (06-06-PLAN.md's
own ``<objective>``).

``48_duplicate_header_names_case_variant.csv`` is deliberately excluded from
the generic "matches corpus declaration" loop below: once duplicate-name
rejection exists, calling ``detect_header`` on it raises rather than
returning -- it is tested separately, alongside 14, in
``test_detect_header_rejects_duplicate_header_names``.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from tools.corpus.generators import generate_corpus
from tools.corpus.manifest import load_manifest

from csv_processor.detect.header import detect_header
from dataplat.errors import FileInspectionError

if TYPE_CHECKING:
    from collections.abc import Mapping

REPO_ROOT = Path(__file__).resolve().parents[3]
MANIFEST = REPO_ROOT / "tests" / "fixtures" / "corpus.yaml"

# Every corpus fixture whose `covers:` list includes CSV-07 or CSV-08 and
# whose declared outcome is a normal (non-raising) `detect_header` return.
# `48_duplicate_header_names_case_variant.csv` is covered separately below.
HEADER_FIXTURE_NAMES = (
    "01_simple.csv",
    "11_no_header.csv",
    "12_metadata_before_header.csv",
    "13_footer.csv",
    "18_empty.csv",
    "19_only_header.csv",
    "49_header_with_leading_trailing_spaces.csv",
    "63_repeated_header_mid_file.csv",
    "64_footer_totals_with_different_column_count.csv",
)

# The two footer fixtures, checked for footer_row_indices/footer_row_count.
FOOTER_FIXTURE_NAMES = (
    "13_footer.csv",
    "64_footer_totals_with_different_column_count.csv",
)


@pytest.fixture(scope="module")
def corpus(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Generate the corpus once for this module, skipping the large profile."""
    manifest = load_manifest(MANIFEST)
    out_dir = tmp_path_factory.mktemp("header-corpus")
    generate_corpus(manifest, out_dir, fast=True)
    return out_dir


@pytest.fixture(scope="module")
def declared() -> Mapping[str, Mapping[str, Any]]:
    """Return each fixture's declared meaning, keyed by fixture name."""
    return {fixture.name: fixture.expect for fixture in load_manifest(MANIFEST).fixtures}


def _rows_for(path: Path) -> list[tuple[str, ...]]:
    """Parse a generated fixture into rows. Every fixture here declares "," as its delimiter."""
    with path.open(encoding="utf-8", newline="") as handle:
        return [tuple(row) for row in csv.reader(handle, delimiter=",")]


@pytest.mark.parametrize("fixture_name", HEADER_FIXTURE_NAMES)
def test_detect_header_matches_corpus_declaration(
    fixture_name: str, corpus: Path, declared: Mapping[str, Mapping[str, Any]]
) -> None:
    expect = declared[fixture_name]
    rows = _rows_for(corpus / fixture_name)

    result = detect_header(rows, header_trim=True)

    if "header_row_index" in expect:
        assert result.header_row_index == expect["header_row_index"], fixture_name
    if "has_header" in expect:
        assert result.has_header == expect["has_header"], fixture_name
    if "preamble_row_count" in expect:
        assert result.preamble_row_count == expect["preamble_row_count"], fixture_name
    if "raw_header" in expect:
        assert list(result.raw_header) == expect["raw_header"], fixture_name
    if "trimmed_header" in expect:
        assert list(result.trimmed_header) == expect["trimmed_header"], fixture_name


def test_detect_header_returns_none_for_a_header_shaped_like_data() -> None:
    """The acceptance-criteria one-liner as a real test (11_no_header.csv's shape)."""
    result = detect_header([("000001", "Kowalski", "1234.56")] * 4)
    assert result.header_row_index is None
    assert result.has_header is False


def test_detect_header_returns_none_for_a_genuinely_empty_input() -> None:
    """18_empty.csv's case, isolated from corpus generation: zero rows in, zero header out."""
    result = detect_header([])
    assert result.header_row_index is None
    assert result.has_header is False
    assert result.raw_header == ()


def test_detect_header_honors_a_contract_header_row_override() -> None:
    """csv.header_row skips scoring entirely and trusts the contract, per CsvParsingConfig."""
    rows = [("meta",), ("id", "name"), ("1", "Kowalski")]
    result = detect_header(rows, contract_header_row=1)
    assert result.header_row_index == 1
    assert result.raw_header == ("id", "name")
    assert result.preamble_row_count == 1
    assert result.has_header is True


def test_detect_header_trimmed_header_equals_raw_header_by_default() -> None:
    """header_trim defaults to False -- trimming is a declared normalisation, never a default."""
    rows = [(" id ", " name "), ("1", "Kowalski")]
    result = detect_header(rows)
    assert result.trimmed_header == result.raw_header
    assert result.raw_header == (" id ", " name ")


@pytest.mark.parametrize("fixture_name", FOOTER_FIXTURE_NAMES)
def test_detect_header_identifies_footer_rows(
    fixture_name: str, corpus: Path, declared: Mapping[str, Mapping[str, Any]]
) -> None:
    """Footer rows are found by their differing field count, and excluded from the data count."""
    expect = declared[fixture_name]
    rows = _rows_for(corpus / fixture_name)

    result = detect_header(rows)

    assert list(result.footer_row_indices) == expect["footer_row_indices"]
    assert len(result.footer_row_indices) == expect["footer_row_count"]
    kept_data_rows = (
        len(rows)
        - 1  # the header row itself
        - len(result.footer_row_indices)
        - len(result.repeated_header_row_indices)
    )
    assert kept_data_rows == expect["data_rows"]


def test_detect_header_identifies_a_repeated_header_mid_file(
    corpus: Path, declared: Mapping[str, Mapping[str, Any]]
) -> None:
    """A concatenated export's re-embedded header line is found and excluded, not loaded."""
    expect = declared["63_repeated_header_mid_file.csv"]
    rows = _rows_for(corpus / "63_repeated_header_mid_file.csv")

    result = detect_header(rows)

    assert list(result.repeated_header_row_indices) == expect["repeated_header_row_indices"]
    kept_data_rows = (
        len(rows) - 1 - len(result.footer_row_indices) - len(result.repeated_header_row_indices)
    )
    assert kept_data_rows == expect["data_rows"]


@pytest.mark.parametrize(
    ("fixture_name", "check_colliding_names"),
    [
        ("14_duplicate_columns.csv", False),
        ("48_duplicate_header_names_case_variant.csv", True),
    ],
)
def test_detect_header_rejects_duplicate_header_names(
    fixture_name: str,
    *,
    check_colliding_names: bool,
    corpus: Path,
    declared: Mapping[str, Mapping[str, Any]],
) -> None:
    """Exact (14) and case-variant (48) duplicate names both raise, never rename or last-wins."""
    expect = declared[fixture_name]
    rows = _rows_for(corpus / fixture_name)

    with pytest.raises(FileInspectionError) as exc_info:
        detect_header(rows)

    assert exc_info.value.context["diagnostic_code"] == "duplicate-header-names"
    colliding = exc_info.value.context["colliding_names"]
    assert isinstance(colliding, list)
    if check_colliding_names:
        assert set(colliding) == set(expect["colliding_names"])
    else:
        assert expect["duplicated_name"] in colliding


def test_detect_header_skip_footer_rows_override_skips_without_scoring() -> None:
    """csv.skip_footer_rows takes precedence over heuristic footer scoring when non-zero."""
    rows = [
        ("id", "name", "amount"),
        ("1", "Kowalski", "10.00"),
        ("2", "Nowak", "20.00"),
        ("not", "scored", "at-all"),  # same field count as the header -- shape alone misses it
    ]
    result = detect_header(rows, skip_footer_rows=1)
    assert result.footer_row_indices == (3,)


def test_detect_header_footer_patterns_override_matches_a_configured_regex() -> None:
    """csv.footer_patterns classifies a same-shaped trailing row as footer via regex match."""
    rows = [
        ("id", "name", "amount"),
        ("1", "Kowalski", "10.00"),
        ("2", "Nowak", "20.00"),
        ("TOTAL", "", "30.00"),  # same field count as the header -- shape alone misses it
    ]
    result = detect_header(rows, footer_patterns=[r"^TOTAL$"])
    assert result.footer_row_indices == (3,)
