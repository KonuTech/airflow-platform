"""Corpus-parametrized tests for ``csv_processor.detect.dialect`` (CSV-04/05/06).

Same ``load_manifest``/``generate_corpus`` pattern as
``tests/unit/test_corpus_semantic_fixtures.py`` and ``test_diagnostics.py``:
the corpus is materialised once into a temporary directory, and every
assertion is compared against a fixture's own ``expect:`` block rather than
restating an expected value in Python.

Two fixture-key shapes exist for "what delimiter should be detected", both
read from each fixture's own declaration rather than assumed uniformly (the
plan's own action text warns against exactly that assumption for fixture
37): most fixtures declare ``detected_delimiter`` (including ``None`` for a
declined detection -- fixture 38); ``37_delimiter_frequency_differs_header_vs_body.csv``
declares ``correct_delimiter`` instead, because the fixture's whole point is
that a naive heuristic gets the wrong answer, so its key names the *correct*
answer rather than claiming to be "detected" by anything naive.
``40_utf16_no_bom.csv`` is encoding-focused (CSV-02/03) and declares no
delimiter expectation at all; its check falls back to the fixture's own
generation-time ``delimiter`` attribute -- the ground truth the file was
actually written with.

**A genuine, live-verified gap in the plan's own fixture list, found while
writing this suite:** ``36_doubled_vs_backslash_escape.csv`` declares
``detected_delimiter: ","`` in its own ``expect:`` block, but real
``clevercsv==0.8.5`` cannot detect ANY dialect for this file's content --
``Detector().detect()`` returns ``None``, not a delimiter -- because the
file is deliberately, structurally inconsistent in its quoting (see
``dialect.py``'s module docstring). This is not a bug in
``csv_processor.detect.dialect``; it is verified, reproducible behaviour of
the underlying library against this exact fixture. Editing
``tests/fixtures/corpus.yaml``'s ``expect:`` block to "fix" this would be
out of this plan's declared file scope (a shared fixture file every Wave-2
detector plan reads from) and would misrepresent what actually happens.
Fixture 36 is therefore tested on its own
(``test_fixture_36_dialect_detection_declines_live``), proving and
documenting the discovery directly, rather than folded into the generic
"matches its own detected_delimiter" parametrization below. Its CSV-06
round-trip proof (Task 2) uses the documented, non-exceptional "declined,
but the caller has a contract fallback" path with ``contract_delimiter=","``
-- exactly the escape hatch ``dialect.py``'s own interface provides for
this situation.
"""

from __future__ import annotations

import csv
import io
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

import pytest
from tools.corpus.generators import generate_corpus
from tools.corpus.manifest import load_manifest

from csv_processor.detect.dialect import DialectDetection, detect_dialect, to_stdlib_dialect
from dataplat.errors import CsvDialectDetectionError

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from tools.corpus.manifest import Fixture

REPO_ROOT = Path(__file__).resolve().parents[3]
MANIFEST = REPO_ROOT / "tests" / "fixtures" / "corpus.yaml"

# Every fixture whose delimiter this suite checks against its own
# declaration, EXCLUDING 36 (tested separately -- see module docstring).
# 01 is included because the plan's own <behavior> text names it explicitly
# ("Fixtures 02_semicolon.csv, 03_pipe.csv, 04_tab.csv, 01_simple.csv
# (comma): each detects its own declared delimiter exactly") even though the
# <action> text's parenthetical enumeration starts at 02 -- <behavior> is
# the authoritative "what must be true" section.
DELIMITER_FIXTURES = (
    "01_simple.csv",
    "02_semicolon.csv",
    "03_pipe.csv",
    "04_tab.csv",
    "08_quoted_fields.csv",
    "09_embedded_commas.csv",
    "10_embedded_newlines.csv",
    "35_quote_in_unquoted_field.csv",
    "37_delimiter_frequency_differs_header_vs_body.csv",
    "38_single_column_no_delimiter.csv",
    "40_utf16_no_bom.csv",
    "66_triple_nasty.csv",
    "68_utf8_bom_semicolon_pl_excel.csv",
    "20_decimal_comma.csv",
    "21_decimal_point.csv",
    "52_date_ambiguous_dm_vs_md.csv",
)


@pytest.fixture(scope="module")
def corpus(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Generate the corpus once, skipping the large-profile fixture."""
    manifest = load_manifest(MANIFEST)
    out_dir = tmp_path_factory.mktemp("corpus")
    generate_corpus(manifest, out_dir, fast=True)
    return out_dir


@pytest.fixture(scope="module")
def declared() -> Mapping[str, Mapping[str, Any]]:
    """Return each fixture's declared meaning, keyed by fixture name."""
    return {fixture.name: fixture.expect for fixture in load_manifest(MANIFEST).fixtures}


@pytest.fixture(scope="module")
def fixtures_by_name() -> Mapping[str, Fixture]:
    """Return each fixture's full declaration, keyed by fixture name.

    Unlike ``declared`` (which exposes only ``expect:``), this exposes the
    whole ``Fixture`` -- needed for ``40_utf16_no_bom.csv``'s
    generation-time ``delimiter``/``encoding``/``bom`` attributes, which no
    ``expect:`` block carries.
    """
    return {fixture.name: fixture for fixture in load_manifest(MANIFEST).fixtures}


def _decode_sample(path: Path, fixture: Fixture) -> str:
    r"""Decode one corpus fixture's raw bytes into the text ``detect_dialect`` receives.

    Mirrors the real pipeline's ordering (encoding detection happens first;
    dialect detection receives already-decoded text, per the architecture
    diagram in 06-RESEARCH.md) using the fixture's own declared
    encoding/BOM rather than re-running encoding detection here.
    ``bytes.decode()`` never performs newline translation -- only text-mode
    file I/O does -- so an embedded ``\r\n`` inside a quoted field survives
    exactly as declared without needing an explicit ``newline=""`` reader.
    """
    raw = path.read_bytes()
    codec = fixture.encoding
    if fixture.bom:
        normalised = codec.lower().replace("_", "-")
        if normalised == "utf-8":
            codec = "utf-8-sig"
        elif normalised in ("utf-16", "utf-16-le", "utf-16-be"):
            codec = "utf-16"
    return raw.decode(codec)


def _expected_delimiter(fixture: Fixture, expect: Mapping[str, Any]) -> str | None:
    """Resolve the delimiter a fixture's own declaration says should be detected.

    See the module docstring for why three different sources exist.
    """
    if "detected_delimiter" in expect:
        value = expect["detected_delimiter"]
        return None if value is None else str(value)
    if "correct_delimiter" in expect:
        return str(expect["correct_delimiter"])
    return fixture.delimiter


@pytest.mark.parametrize("fixture_name", DELIMITER_FIXTURES)
def test_detects_the_fixtures_own_delimiter(
    fixture_name: str,
    corpus: Path,
    declared: Mapping[str, Mapping[str, Any]],
    fixtures_by_name: Mapping[str, Fixture],
) -> None:
    """``detect_dialect`` matches each fixture's own declared delimiter exactly.

    Covers CSV-04's five dialects (01 comma, 02 semicolon, 03 pipe, 04 tab --
    68 covers colon-adjacent semicolon-with-comma-decimal), CSV-05's
    contract-override-free detection including the declined case (38), and
    the two adversarial shapes (37's header/body frequency mismatch, 40's
    encoding-only expectation).
    """
    fixture = fixtures_by_name[fixture_name]
    expect = declared[fixture_name]
    sample = _decode_sample(corpus / fixture_name, fixture)

    result = detect_dialect(sample)

    expected = _expected_delimiter(fixture, expect)
    assert result.delimiter == expected
    assert result.declined == (expected is None)


def test_fixture_36_dialect_detection_declines_live(
    corpus: Path, fixtures_by_name: Mapping[str, Fixture]
) -> None:
    """clevercsv genuinely cannot detect fixture 36's dialect -- see module docstring.

    This is the live discovery this suite documents: the fixture's own
    ``expect.detected_delimiter`` claims ``","``, but real clevercsv 0.8.5
    returns ``None`` for this exact content, which ``detect_dialect`` must
    treat as a clean decline (never a crash -- the whole point of guarding
    against ``Detector().detect()``'s ``None`` return, not just Pitfall 1's
    degenerate empty-delimiter case).
    """
    fixture = fixtures_by_name["36_doubled_vs_backslash_escape.csv"]
    sample = _decode_sample(corpus / fixture.name, fixture)

    result = detect_dialect(sample)

    assert result.declined is True
    assert result.delimiter is None


def test_single_column_sample_declines_without_raising() -> None:
    """The exact minimal reproduction from this plan's own acceptance criteria."""
    result = detect_dialect("customer_reference\nCUST-000001\n")

    assert result.declined
    assert result.delimiter is None


def test_detects_a_colon_delimiter() -> None:
    """CSV-04 names five dialects -- comma, semicolon, pipe, tab AND colon.

    No corpus fixture uses a colon as its actual field delimiter: the only
    colon in the whole 70-fixture corpus is
    ``12_metadata_before_header.csv``'s ``preamble_contains_a_colon_which_is_
    itself_a_candidate_delimiter`` -- a DECOY inside a metadata preamble that
    must be correctly ignored (that fixture's own real delimiter is a
    comma), not a genuine colon-delimited file. Adding a corpus fixture
    would touch ``tests/fixtures/corpus.yaml``, a shared file outside this
    plan's declared scope and every other Wave-2 detector plan's own digest
    oracle. This hand-constructed sample proves ``detect_dialect`` itself
    handles colon correctly (verified live: ``clevercsv.Detector().detect()``
    returns ``SimpleDialect(':', '', '')`` for this exact shape), closing
    the gap between this plan's own ``must_haves.truths`` claim and what the
    committed corpus can prove.
    """
    sample = "id:name:amount\n000001:Kowalski:100.00\n000002:Nowak:200.00\n"

    result = detect_dialect(sample)

    assert result.delimiter == ":"
    assert result.declined is False


@pytest.mark.parametrize("fixture_name", DELIMITER_FIXTURES)
def test_detect_dialect_never_raises_csv_error(
    fixture_name: str, corpus: Path, fixtures_by_name: Mapping[str, Fixture]
) -> None:
    """No corpus fixture's content ever reaches ``detect_dialect`` as an uncaught ``_csv.Error``.

    Pitfall 1's exact failure mode (06-RESEARCH.md): calling
    ``SimpleDialect('', '', '').to_csv_dialect()`` unconditionally raises
    ``_csv.Error: "delimiter" must be a 1-character string``. This test
    exists in addition to the assertion-based tests above so a regression
    of the guard itself produces an explicit, clearly-labelled failure
    rather than an incidental pytest error report.
    """
    fixture = fixtures_by_name[fixture_name]
    sample = _decode_sample(corpus / fixture_name, fixture)

    try:
        detect_dialect(sample)
    except csv.Error as exc:  # pragma: no cover - regression guard, not expected to fire
        pytest.fail(f"{fixture_name}: detect_dialect raised csv.Error: {exc}")


def test_contract_delimiter_overrides_detection_unconditionally() -> None:
    """A contract-declared delimiter always wins, regardless of the sample's content.

    Uses ``02_semicolon.csv``-shaped content (a real semicolon-delimited
    sample) but supplies a contract delimiter of ``"|"`` -- if detection ran
    at all, it would find ``";"``, not ``"|"``, so a passing assertion here
    proves clevercsv was never even invoked (CSV-05).
    """
    sample = "id;name;amount\n000001;Kowalski;100.00\n"

    result = detect_dialect(sample, contract_delimiter="|")

    assert result == DialectDetection(delimiter="|", quotechar='"', declined=False)


# --- CSV-06: to_stdlib_dialect round-trips a real parser, never string splitting ---


def _assert_08(rows: list[list[str]], expect: Mapping[str, Any]) -> None:
    assert rows[1] == expect["value_row_1"]


def _assert_09(rows: list[list[str]], expect: Mapping[str, Any]) -> None:
    assert rows[1][1] == expect["value_row_1_name"]


def _assert_10(rows: list[list[str]], expect: Mapping[str, Any]) -> None:
    assert rows[1][1] == expect["value_row_1_note"]


def _assert_35(rows: list[list[str]], expect: Mapping[str, Any]) -> None:
    assert rows[1][1] == expect["value_row_1_code"]
    assert rows[2][1] == expect["value_row_2_code"]


def _assert_36(rows: list[list[str]], expect: Mapping[str, Any]) -> None:
    assert rows[1][1] == expect["value_row_1"]
    assert rows[2][1] == expect["value_row_2_under_the_doubled_convention"]
    assert rows[3][1] == expect["value_row_3"]


def _assert_66(rows: list[list[str]], expect: Mapping[str, Any]) -> None:
    assert rows[1] == expect["value_row_1"]
    assert rows[1][1] == expect["value_row_1_payload"]
    # The plan's own acceptance criteria names this exact literal string.
    assert rows[1][1] == 'a,b\nc"d'


class _RoundTripCase(NamedTuple):
    """One CSV-06 fixture's round-trip proof: which fixture, how to assert, what contract to use.

    ``contract_delimiter`` is not ``None`` only for 36, whose live detection
    declines (see module docstring) -- this is the documented "declined,
    but the caller has a contract fallback" path, not a workaround.
    """

    fixture_name: str
    assert_rows: Callable[[list[list[str]], Mapping[str, Any]], None]
    contract_delimiter: str | None


CSV06_ROUNDTRIP_CASES = (
    _RoundTripCase("08_quoted_fields.csv", _assert_08, None),
    _RoundTripCase("09_embedded_commas.csv", _assert_09, None),
    _RoundTripCase("10_embedded_newlines.csv", _assert_10, None),
    _RoundTripCase("35_quote_in_unquoted_field.csv", _assert_35, None),
    _RoundTripCase("36_doubled_vs_backslash_escape.csv", _assert_36, ","),
    _RoundTripCase("66_triple_nasty.csv", _assert_66, None),
)


@pytest.mark.parametrize("case", CSV06_ROUNDTRIP_CASES, ids=lambda c: c.fixture_name)
def test_to_stdlib_dialect_round_trips_csv06_hazards(
    case: _RoundTripCase,
    corpus: Path,
    declared: Mapping[str, Mapping[str, Any]],
    fixtures_by_name: Mapping[str, Fixture],
) -> None:
    """A real ``csv.reader`` built from ``to_stdlib_dialect`` parses each CSV-06 hazard correctly.

    Proves CSV-06's requirement text literally: quoted delimiters (09),
    multiline fields (10), a bare quote inside an unquoted field (35), the
    doubled-quote escape convention (36), and all three hazards combined in
    one field (66) are handled by a real parser -- never string splitting.
    """
    fixture = fixtures_by_name[case.fixture_name]
    expect = declared[case.fixture_name]
    sample = _decode_sample(corpus / case.fixture_name, fixture)

    detection = detect_dialect(sample, contract_delimiter=case.contract_delimiter)
    dialect = to_stdlib_dialect(detection)
    rows = list(csv.reader(io.StringIO(sample), dialect=dialect))

    case.assert_rows(rows, expect)


def test_to_stdlib_dialect_raises_on_a_declined_detection_with_no_contract_fallback() -> None:
    """The exact reproduction from this plan's own acceptance criteria.

    This is the boundary ``to_stdlib_dialect`` enforces: "declined, but the
    caller has a contract fallback" (``contract_delimiter`` was supplied to
    ``detect_dialect``, so ``declined`` is always ``False``) is never an
    error; "declined, with no fallback at all" always is.
    """
    declined = DialectDetection(delimiter=None, quotechar='"', declined=True)

    with pytest.raises(CsvDialectDetectionError) as exc_info:
        to_stdlib_dialect(declined)

    assert exc_info.value.context["diagnostic_code"] == "dialect-detection-declined"
