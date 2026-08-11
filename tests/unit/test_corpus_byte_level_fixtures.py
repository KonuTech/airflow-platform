"""The assertions the digest oracle structurally cannot make (QUAL-08).

``CORPUS.sha256`` proves the bytes have not changed. It cannot prove the bytes
*mean* what the manifest says they mean — a fixture whose declaration and
content silently disagree has a perfectly stable digest, and every Phase 6 test
built on it inherits the mistake. Nothing else in the repository would notice.

So each test below reads a generated fixture **in binary** and asserts the one
property its declaration exists to assert: the mark's byte offset, a strict
decoder refusing the invalid-sequence file, the terminator mix, the absence of
a line feed, a field either side of the parser's default limit, and the header
appearing in the first part of a split and nowhere else.

The corpus is generated into a temporary directory rather than read from
``tests/fixtures/csv/``. That is deliberate on two counts: the tests then pass
on a clean checkout where nobody has run ``make fixtures`` yet, and if a
generated file ever differed from a freshly generated one, that is
``fixtures-verify``'s failure to report, not this module's.
"""

from __future__ import annotations

import codecs
import csv
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from tools.corpus.generators import generate_corpus
from tools.corpus.manifest import load_manifest

if TYPE_CHECKING:
    from collections.abc import Mapping

REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST = REPO_ROOT / "tests" / "fixtures" / "corpus.yaml"

# The stdlib parser's documented default, and the number 28 and 67 are declared
# either side of. Read from the module rather than restated, so a future
# interpreter that changes it fails these tests loudly instead of quietly
# invalidating what the two fixtures are for.
STDLIB_FIELD_LIMIT = csv.field_size_limit()


@pytest.fixture(scope="module")
def corpus(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Generate the corpus once, skipping the ~293 MB large-profile fixture."""
    manifest = load_manifest(MANIFEST)
    out_dir = tmp_path_factory.mktemp("corpus")
    generate_corpus(manifest, out_dir, fast=True)
    return out_dir


@pytest.fixture(scope="module")
def declared() -> Mapping[str, object]:
    """Return each fixture's declared meaning, keyed by fixture name."""
    return {fixture.name: fixture.expect for fixture in load_manifest(MANIFEST).fixtures}


def test_the_mid_file_mark_is_at_a_non_zero_offset(corpus: Path) -> None:
    # A mark at offset zero is 05_utf8_bom.csv, a fixture that already exists.
    # If this one drifted to offset zero it would silently become a duplicate
    # of that fixture while still passing its digest check.
    payload = (corpus / "41_bom_mid_file.csv").read_bytes()
    offset = payload.find(codecs.BOM_UTF8)

    assert offset > 0, f"the mark must not be at offset zero; found at {offset}"
    assert not payload.startswith(codecs.BOM_UTF8)
    # Declared as after record 2 — the header plus the first data record — so
    # there is real content on both sides of it.
    assert payload.count(b"\n", 0, offset) == 2
    assert payload[offset + len(codecs.BOM_UTF8) :], "nothing follows the mark"


def test_the_two_byte_encoding_without_a_mark_carries_none(corpus: Path) -> None:
    # The ambiguity is the fixture. A mark here would make detection certain
    # and quietly delete the confidence-and-override path it exists to exercise.
    payload = (corpus / "40_utf16_no_bom.csv").read_bytes()

    assert not payload.startswith(codecs.BOM_UTF16_LE)
    assert not payload.startswith(codecs.BOM_UTF16_BE)
    assert codecs.BOM_UTF16_LE not in payload
    # Still genuinely UTF-16-LE: ASCII characters carry a trailing zero byte.
    assert payload.decode("utf-16-le").startswith("id,name")

    with_mark = (corpus / "07_utf16.csv").read_bytes()
    assert with_mark.startswith(codecs.BOM_UTF16_LE), "the counterpart must have one"


def test_a_strict_decoder_rejects_the_invalid_sequence_fixture(corpus: Path) -> None:
    # The whole point is bytes no encoder would emit. If a lenient decode had
    # repaired them at generation time this file would be ordinary UTF-8 and
    # the §9 error-policy decision would never be forced.
    payload = (corpus / "39_utf8_invalid_sequences.csv").read_bytes()

    with pytest.raises(UnicodeDecodeError):
        payload.decode("utf-8")

    # Both declared classes are present: a truncated two-byte lead and a lone
    # continuation byte.
    assert b"\xc3\x28" in payload
    assert b"\x80" in payload
    # Lenient decoding recovers a parseable file, which is what makes
    # `errors=` a real contract decision rather than a formality.
    assert payload.decode("utf-8", "replace").count("\n") == 4


def test_the_mixed_terminator_fixture_genuinely_mixes_them(corpus: Path) -> None:
    payload = (corpus / "30_crlf_lf_mixed.csv").read_bytes()

    assert b"\r\n" in payload
    bare_line_feeds = [
        index
        for index, byte in enumerate(payload)
        if byte == 0x0A and (index == 0 or payload[index - 1] != 0x0D)
    ]
    assert bare_line_feeds, "every line feed is part of a CRLF; nothing is mixed"
    assert payload.count(b"\r\n") >= 1
    # Six records, alternating: three CRLF and three bare LF would be a
    # perfectly balanced mix; assert only that both are really present and that
    # no \r is left stranded without a following \n.
    assert payload.count(b"\r") == payload.count(b"\r\n")


def test_the_bare_carriage_return_fixture_contains_no_line_feed(corpus: Path) -> None:
    # Read as bytes and split on \n, this file is a single row. That is exactly
    # the failure mode the fixture exists to expose.
    payload = (corpus / "31_cr_only.csv").read_bytes()

    assert b"\n" not in payload
    assert payload.count(b"\r") == 6  # header + five data records
    assert len(payload.split(b"\n")) == 1


def test_the_oversized_field_exceeds_the_parser_default_and_its_twin_does_not(
    corpus: Path,
) -> None:
    # A pair, because one fixture alone cannot distinguish "the limit works"
    # from "wide fields are broken".
    oversized = _longest_field(corpus / "67_row_exceeding_field_size_limit.csv")
    within = _longest_field(corpus / "28_large_fields.csv")

    assert oversized > STDLIB_FIELD_LIMIT, (
        f"the field is {oversized} bytes, which does not exceed the parser's "
        f"default limit of {STDLIB_FIELD_LIMIT}; the fixture is merely large"
    )
    assert within < STDLIB_FIELD_LIMIT, (
        f"28_large_fields.csv is {within} bytes, at or over the {STDLIB_FIELD_LIMIT} "
        f"limit; it is supposed to be the fixture that stays inside it"
    )

    # And the stdlib parser really does refuse the oversized one at its default,
    # which is why the declared outcome is a rejected record rather than a
    # successful parse.
    with (
        (corpus / "67_row_exceeding_field_size_limit.csv").open(
            encoding="utf-8", newline=""
        ) as handle,
        pytest.raises(csv.Error, match="field larger than field limit"),
    ):
        list(csv.reader(handle))


def test_the_part_set_carries_its_header_in_the_first_part_only(corpus: Path) -> None:
    # A reader that treats each object as its own file consumes part-00001's
    # first row as a header: one record silently vanishes and every column is
    # renamed to a customer's name.
    directory = corpus / "62_multipart_split"
    parts = sorted(directory.iterdir())

    assert [path.name for path in parts] == ["part-00000", "part-00001"]
    assert all(path.is_file() for path in parts)

    header = (corpus / "01_simple.csv").read_bytes().split(b"\n", 1)[0]
    first, second = (path.read_bytes() for path in parts)

    assert first.startswith(header + b"\n")
    assert header not in second, "the header must appear in the first part only"
    assert second.split(b"\n", 1)[0].startswith(b"000011,"), (
        "the second part must begin with a data record"
    )
    # The parts reconstruct the source exactly: no record was dropped or
    # duplicated by the split.
    assert first + second == (corpus / "01_simple.csv").read_bytes()


def test_every_declared_expectation_belongs_to_a_generated_fixture(
    corpus: Path, declared: Mapping[str, object]
) -> None:
    # Cheap guard against the quiet failure this whole module is about: a
    # declaration whose fixture no longer exists still reads convincingly.
    # The large-profile fixture is the one this module skips (see the `corpus`
    # fixture), so it is the one name allowed to be absent.
    missing = [
        name for name in declared if not (corpus / name).exists() and name != "29_large_file.csv"
    ]
    assert not missing, f"declared but not generated: {missing}"


def _longest_field(path: Path) -> int:
    """Return the longest field in a file, in bytes, ignoring the parser limit."""
    payload = path.read_bytes()
    return max(len(field) for line in payload.split(b"\n") for field in line.split(b","))
