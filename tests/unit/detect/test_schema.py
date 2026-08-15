"""Unit tests for ``csv_processor.detect.schema`` -- SCHEMA-01's conservative type inference.

Every case in 06-07-PLAN.md Task 1's ``<behavior>`` block, plus the corpus-
grounded assertions Task 1's ``<action>`` names against ``01_simple.csv``,
``50_excel_scientific_notation_ids.csv`` and ``60_boolean_localized.csv``,
followed by Task 2's ``infer_schema``/``suggest_column_contracts`` coverage.

The corpus is generated into a temporary directory rather than read from
``tests/fixtures/csv/`` (gitignored, generated on demand), mirroring
``test_corpus_semantic_fixtures.py``'s own ``corpus`` fixture -- these tests
pass on a clean checkout with no ``make fixtures`` step required first.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest
from tools.corpus.generators import generate_corpus
from tools.corpus.manifest import load_manifest

from csv_processor.detect.schema import (
    infer_column_type,
    infer_schema,
    suggest_column_contracts,
)
from dataplat.config.model import ColumnContract

REPO_ROOT = Path(__file__).resolve().parents[3]
MANIFEST = REPO_ROOT / "tests" / "fixtures" / "corpus.yaml"


@pytest.fixture(scope="module")
def corpus(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Generate the corpus once per test module, skipping the large profile."""
    manifest = load_manifest(MANIFEST)
    out_dir = tmp_path_factory.mktemp("corpus")
    generate_corpus(manifest, out_dir, fast=True)
    return out_dir


def _records(path: Path) -> list[list[str]]:
    """Parse a generated fixture into records."""
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.reader(handle))


# --- <behavior> block: named-red-flag cases ---------------------------------


def test_leading_zero_identifier_infers_string_with_red_flag() -> None:
    # SCHEMA-01's own canonical example: 001234 must not become 1234.
    result = infer_column_type(["000001", "000002", "000003"])

    assert result.suggested_type == "string"
    assert "leading-zero" in result.red_flags


def test_a_single_bare_zero_is_not_flagged_as_a_leading_zero() -> None:
    result = infer_column_type(["0"])

    assert result.suggested_type == "integer"
    assert result.red_flags == ()


def test_clean_decimal_sample_infers_decimal() -> None:
    result = infer_column_type(["1234.56", "-99.99", "0.00"])

    assert result.suggested_type == "decimal"
    assert result.red_flags == ()


def test_date_sample_infers_date_against_a_given_candidate_format() -> None:
    # Inference only ever CONFIRMS a given candidate format -- it is never
    # asked to decide between day-first and month-first on its own.
    result = infer_column_type(["31/12/2026", "01/02/2026"], candidate_date_formats=["%d/%m/%Y"])

    assert result.suggested_type == "date"
    assert result.red_flags == ()


def test_a_format_with_a_time_directive_infers_timestamp_not_date() -> None:
    result = infer_column_type(
        ["2026-08-15T10:00:00", "2026-08-16T11:30:00"],
        candidate_date_formats=["%Y-%m-%dT%H:%M:%S"],
    )

    assert result.suggested_type == "timestamp"


def test_no_candidate_format_is_ever_invented_when_none_fits() -> None:
    # A candidate that does not fit every value is declined outright, never
    # substituted for a guessed format -- inference falls through to the
    # remaining checks and, here, lands on string.
    result = infer_column_type(["31/12/2026", "not-a-date"], candidate_date_formats=["%d/%m/%Y"])

    assert result.suggested_type == "string"


def test_scientific_notation_infers_string_never_decimal_or_integer() -> None:
    result = infer_column_type(["1.23457E+14"])

    assert result.suggested_type == "string"
    assert "scientific-notation" in result.red_flags


def test_scientific_notation_veto_applies_even_with_a_clean_value_present() -> None:
    # A single damaged value is enough to distrust the whole sampled column
    # -- the clean control value does not "outvote" it.
    result = infer_column_type(["1.23457E+14", "123456789012345"])

    assert result.suggested_type == "string"
    assert "scientific-notation" in result.red_flags


def test_a_word_containing_an_embedded_e_digit_is_not_mistaken_for_scientific_notation() -> None:
    # The pattern must match the WHOLE value, not merely contain "e5" --
    # otherwise ordinary text would false-positive on this red flag.
    result = infer_column_type(["Room2E5", "Room3E6"])

    assert "scientific-notation" not in result.red_flags


def test_mixed_parseable_sample_infers_string_and_drops_nothing_silently() -> None:
    result = infer_column_type(["12.5", "not-a-number"])

    assert result.suggested_type == "string"
    assert "mixed-parseable" in result.red_flags


def test_empty_sample_infers_string() -> None:
    result = infer_column_type([])

    assert result.suggested_type == "string"
    assert "empty-sample" in result.red_flags


# --- <behavior> block: boolean (D-14/CSV-10) --------------------------------


def test_zero_and_one_alone_never_infer_boolean() -> None:
    # D-14/CSV-10: "1/0 must never become boolean absent evidence" -- but a
    # bare 0/1 sample is still a perfectly good integer column.
    result = infer_column_type(["0", "1", "0", "1"])

    assert result.suggested_type == "integer"


def test_true_false_with_a_distinctive_token_infers_boolean() -> None:
    result = infer_column_type(["true", "false", "true"])

    assert result.suggested_type == "boolean"


def test_yes_no_mixed_with_zero_one_infers_boolean_via_the_distinctive_token() -> None:
    result = infer_column_type(["yes", "no", "1", "0"])

    assert result.suggested_type == "boolean"


def test_an_unrecognized_token_declines_boolean_and_falls_through_to_string() -> None:
    result = infer_column_type(["true", "false", "maybe"])

    assert result.suggested_type == "string"


# --- corpus-grounded assertions ---------------------------------------------


def test_fixture_01_simple_id_column_infers_string(corpus: Path) -> None:
    records = _records(corpus / "01_simple.csv")
    id_values = [record[0] for record in records[1:]]

    result = infer_column_type(id_values)

    assert result.suggested_type == "string"
    assert "leading-zero" in result.red_flags


def test_fixture_01_simple_amount_column_infers_decimal(corpus: Path) -> None:
    records = _records(corpus / "01_simple.csv")
    amount_values = [record[2] for record in records[1:]]

    result = infer_column_type(amount_values)

    assert result.suggested_type == "decimal"


def test_fixture_50_scientific_notation_customer_id_infers_string(corpus: Path) -> None:
    records = _records(corpus / "50_excel_scientific_notation_ids.csv")
    customer_id_values = [record[1] for record in records[1:]]

    result = infer_column_type(customer_id_values)

    assert result.suggested_type == "string"
    assert "scientific-notation" in result.red_flags


def test_fixture_60_boolean_localized_clean_english_letters_do_not_infer_boolean(
    corpus: Path,
) -> None:
    # 60's declared true/false tokens (Tak/Nie/Ja/Nein/O/Y/N) are locale
    # specific and only meaningful behind a per-dataset CSV-10 declaration
    # this function never has. Row 6 is French "N" (no) and row 7 is English
    # "Y" (yes) -- pulled without the fixture's other-language rows, this
    # sample must still not trigger a boolean suggestion: the conservative
    # default set this module documents deliberately excludes single-letter
    # tokens, because they collide across languages (this same fixture's own
    # "O" vs zero/off warning).
    records = _records(corpus / "60_boolean_localized.csv")
    clean_english_letters = [records[6][1], records[7][1]]
    assert clean_english_letters == ["N", "Y"]

    result = infer_column_type(clean_english_letters)

    assert result.suggested_type == "string"
    assert result.suggested_type != "boolean"


# --- Task 2: infer_schema / suggest_column_contracts ------------------------


def test_infer_schema_returns_one_inference_per_header_column() -> None:
    header = ["id", "amount"]
    sample_rows = [("000001", "12.50"), ("000002", "9.99")]

    inferences = infer_schema(header, sample_rows)

    assert len(inferences) == 2
    assert inferences[0].suggested_type == "string"
    assert inferences[1].suggested_type == "decimal"


def test_infer_schema_tolerates_a_ragged_sample_row_without_raising() -> None:
    header = ["id", "amount"]
    sample_rows = [("000001", "12.50"), ("000002",)]  # second row is missing amount

    inferences = infer_schema(header, sample_rows)

    assert len(inferences) == 2
    assert inferences[0].suggested_type == "string"


def test_suggest_column_contracts_shape_matches_column_contract_field_names() -> None:
    suggestions = suggest_column_contracts(["id", "amount"], [("000001", "12.50")])

    assert suggestions[0] == {"name": "id", "type": "string", "nullable": True, "required": True}
    assert suggestions[1]["name"] == "amount"
    assert suggestions[1]["type"] == "decimal"
    assert suggestions[1]["nullable"] is True
    assert suggestions[1]["required"] is True


def test_suggest_column_contracts_round_trips_into_a_real_column_contract() -> None:
    # Task 2's own acceptance criterion: constructing ColumnContract(**s[0])
    # from the suggestion dict must not raise.
    suggestions = suggest_column_contracts(["id", "amount"], [("000001", "12.50")])

    for suggestion in suggestions:
        contract = ColumnContract(**suggestion)
        assert contract.name in {"id", "amount"}
