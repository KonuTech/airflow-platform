"""The manifest model is the gate on corpus correctness (QUAL-08).

A corrupt manifest must fail loudly at load time. The alternative — silently
ignoring a misspelled field — generates a corpus that silently *means* something
else, and a corpus that means something else is no longer a specification.

Every rejection test asserts on the error *message*, not merely on the exception
type: the message is the part a future author actually reads.
"""

from __future__ import annotations

import textwrap
from typing import TYPE_CHECKING

import pytest
from tools.corpus.generators import GeneratorError, generate_corpus
from tools.corpus.manifest import ManifestError, load_manifest

if TYPE_CHECKING:
    from pathlib import Path

# A minimal well-formed manifest. Every rejection test below is this document
# with exactly one thing wrong, so the test names the defect and nothing else.
VALID = r"""
version: 1
master_seed: "test/corpus/v1"
fixtures:
  - name: "01_simple.csv"
    covers: [CSV-04]
    generator: tabular
    encoding: utf-8
    bom: false
    delimiter: ","
    quotechar: '"'
    line_terminator: "\n"
    header: [id, amount]
    rows: 3
    row_spec:
      id:     { kind: zero_padded_int, width: 6, start: 1 }
      amount: { kind: decimal, min: "1.00", max: "9.99", scale: 2 }
    expect:
      data_rows: 3
"""


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "corpus.yaml"
    path.write_text(textwrap.dedent(text), encoding="utf-8")
    return path


def test_well_formed_manifest_loads_frozen_and_in_declared_order(tmp_path: Path) -> None:
    manifest = load_manifest(
        _write(
            tmp_path,
            VALID
            + r"""
  - name: "02_second.csv"
    generator: literal
    encoding: utf-8
    content: "id\n1\n"
  - name: "03_third.csv"
    generator: literal
    encoding: utf-8
    content: "id\n2\n"
""",
        )
    )

    assert manifest.version == 1
    assert manifest.master_seed == "test/corpus/v1"
    # Declared order, not sorted order: R1's one-line-diff property and R7's
    # ban on hash-salted iteration both depend on this being the document order.
    assert [f.name for f in manifest.fixtures] == [
        "01_simple.csv",
        "02_second.csv",
        "03_third.csv",
    ]

    # Frozen: a generator run must not be able to mutate the specification it
    # is generating from (R6/R7 determinism, STACK.md §F `frozen=True`).
    with pytest.raises((AttributeError, TypeError)):
        manifest.fixtures[0].rows = 99  # type: ignore[misc]


def test_unknown_top_level_fixture_key_names_both_the_key_and_the_fixture(
    tmp_path: Path,
) -> None:
    path = _write(tmp_path, VALID + '    line_termnator: "\\n"\n')

    with pytest.raises(ManifestError) as excinfo:
        load_manifest(path)

    message = str(excinfo.value)
    assert "line_termnator" in message
    assert "01_simple.csv" in message


def test_unknown_manifest_root_key_is_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, "master_sed: x\n" + VALID)

    with pytest.raises(ManifestError, match="master_sed"):
        load_manifest(path)


def test_unknown_key_inside_expect_is_accepted_and_preserved(tmp_path: Path) -> None:
    # Open Question 3, adopted resolution: `expect:` is the one permissive
    # sub-schema, so vocabulary fixed in Phase 6 (encoding confidence) and
    # Phase 8 (quarantine reasons) is writable today without a model migration.
    manifest = load_manifest(
        _write(
            tmp_path,
            VALID
            + r"""      encoding_confidence_min: 1.0
      quarantine_reason: "nul-byte-in-text-field"
      a_phase_8_concept_not_yet_invented: 42
""",
        )
    )

    expect = manifest.fixtures[0].expect
    assert expect["data_rows"] == 3
    assert expect["encoding_confidence_min"] == 1.0
    assert expect["quarantine_reason"] == "nul-byte-in-text-field"
    assert expect["a_phase_8_concept_not_yet_invented"] == 42


def test_delimiter_equal_to_a_declared_decimal_separator_is_rejected(tmp_path: Path) -> None:
    # Dialect detection runs before numeric normalisation (STACK.md §F), so a
    # contract whose delimiter *is* the decimal separator is unsatisfiable by
    # construction. Reject it here rather than generating bytes nothing can read.
    plain = 'amount: { kind: decimal, min: "1.00", max: "9.99", scale: 2 }'
    comma = plain[:-2] + ', decimal_separator: "," }'
    path = _write(tmp_path, VALID.replace(plain, comma))

    with pytest.raises(ManifestError) as excinfo:
        load_manifest(path)

    message = str(excinfo.value)
    assert "01_simple.csv" in message
    assert "amount" in message
    assert "delimiter" in message


def test_large_profile_size_must_exceed_twice_the_address_space_limit(tmp_path: Path) -> None:
    # The bounded-memory assertion is vacuous unless the file is comfortably
    # larger than the limit it is streamed under.
    template = r"""
version: 1
master_seed: "test/corpus/v1"
fixtures:
  - name: "29_large_file.csv"
    generator: tabular
    encoding: utf-8
    delimiter: ","
    line_terminator: "\n"
    header: [id]
    rows: 10
    profile: large
    row_spec:
      id: {{ kind: zero_padded_int, width: 6, start: 1 }}
    expect:
      approx_bytes: {approx}
      rlimit_as_bytes: 100
"""

    with pytest.raises(ManifestError) as excinfo:
        load_manifest(_write(tmp_path, template.format(approx=200)))
    assert "rlimit_as_bytes" in str(excinfo.value)

    manifest = load_manifest(_write(tmp_path, template.format(approx=201)))
    assert manifest.fixtures[0].profile == "large"


def test_duplicate_fixture_name_is_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        VALID
        + r"""
  - name: "01_simple.csv"
    generator: literal
    encoding: utf-8
    content: "id\n1\n"
""",
    )

    with pytest.raises(ManifestError, match=r"01_simple\.csv"):
        load_manifest(path)


def test_wrapper_target_must_be_declared_earlier(tmp_path: Path) -> None:
    # Declared *earlier*, not merely present: generation is a single ordered
    # pass, so a forward reference has no bytes to wrap yet.
    forward_reference = r"""
version: 1
master_seed: "test/corpus/v1"
fixtures:
  - name: "61_gzipped.csv.gz"
    generator: wrapper
    wraps: "01_simple.csv"
    compression: gzip
""" + VALID.split("fixtures:", 1)[1]

    with pytest.raises(ManifestError) as excinfo:
        load_manifest(_write(tmp_path, forward_reference))
    assert "61_gzipped.csv.gz" in str(excinfo.value)
    assert "01_simple.csv" in str(excinfo.value)

    missing_target = r"""
version: 1
master_seed: "test/corpus/v1"
fixtures:
  - name: "61_gzipped.csv.gz"
    generator: wrapper
    wraps: "does_not_exist.csv"
    compression: gzip
"""
    with pytest.raises(ManifestError, match=r"does_not_exist\.csv"):
        load_manifest(_write(tmp_path, missing_target))


def test_unknown_generator_kind_is_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, VALID.replace("generator: tabular", "generator: magic"))

    with pytest.raises(ManifestError, match="magic"):
        load_manifest(path)


# --------------------------------------------------------------------------
# Byte-level construction (plan 01-06). Each capability below adds a way for a
# declaration to be *coherently wrong* — a mark positioned past the end of the
# file, a splice landing inside a character, a "split" into one part. Every one
# of them produces bytes that still look plausible, so each has to be refused at
# load time rather than discovered by someone reading a hex dump in Phase 6.
# --------------------------------------------------------------------------

# A literal fixture with a two-byte character, so splice offsets have a real
# character boundary to be right or wrong about. Encoded: i d \n <c3 a4> \n,
# so the legal offsets are 0, 1, 2, 3, 5, 6 — and 4 is inside the character.
MULTIBYTE = """
version: 1
master_seed: "test/corpus/v1"
fixtures:
  - name: "39_invalid.csv"
    generator: literal
    encoding: utf-8
    content: "id\\n\\u00e4\\n"
    splices:
      - {{ bytes: "{payload}", {addressing} }}
"""


def _multibyte(tmp_path: Path, addressing: str, payload: str = "\\\\xc3\\\\x28") -> Path:
    return _write(tmp_path, MULTIBYTE.format(addressing=addressing, payload=payload))


def test_a_mark_position_past_the_last_record_is_rejected(tmp_path: Path) -> None:
    # VALID declares three data rows, so it emits four records. A mark after
    # record 4 would sit at end of file with nothing after it to mis-detect.
    with pytest.raises(ManifestError) as excinfo:
        load_manifest(_write(tmp_path, VALID + "    bom: true\n    bom_after_record: 4\n"))

    message = str(excinfo.value)
    assert "bom_after_record" in message
    assert "01_simple.csv" in message

    inside = load_manifest(_write(tmp_path, VALID + "    bom: true\n    bom_after_record: 3\n"))
    assert inside.fixtures[0].bom_after_record == 3


def test_a_mark_position_without_a_mark_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ManifestError, match="bom is"):
        load_manifest(_write(tmp_path, VALID + "    bom_after_record: 2\n"))


def test_a_mark_position_on_a_kind_without_a_record_count_is_rejected(tmp_path: Path) -> None:
    literal = r"""
version: 1
master_seed: "test/corpus/v1"
fixtures:
  - name: "05_bom.csv"
    generator: literal
    encoding: utf-8
    bom: true
    bom_after_record: 1
    content: "id\n1\n"
"""
    with pytest.raises(ManifestError, match="bom_after_record"):
        load_manifest(_write(tmp_path, literal))


def test_a_splice_offset_inside_a_multibyte_character_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ManifestError) as excinfo:
        load_manifest(_multibyte(tmp_path, "offset: 4"))
    assert "multi-byte" in str(excinfo.value)

    # 3 and 5 straddle the same character and are both legal.
    for offset in (3, 5):
        manifest = load_manifest(_multibyte(tmp_path, f"offset: {offset}"))
        assert manifest.fixtures[0].splices[0].raw == b"\xc3\x28"


def test_a_splice_offset_past_the_content_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ManifestError, match="past the"):
        load_manifest(_multibyte(tmp_path, "offset: 99"))


def test_a_splice_must_declare_exactly_one_addressing_mode(tmp_path: Path) -> None:
    with pytest.raises(ManifestError, match="exactly one"):
        load_manifest(_multibyte(tmp_path, "offset: 0, after_record: 1"))

    unaddressed = """
version: 1
master_seed: "test/corpus/v1"
fixtures:
  - name: "39_invalid.csv"
    generator: literal
    encoding: utf-8
    content: "id\\n1\\n"
    splices:
      - { bytes: "\\\\x80" }
"""
    with pytest.raises(ManifestError, match="exactly one"):
        load_manifest(_write(tmp_path, unaddressed))


def test_a_splice_addressed_by_record_resolves_past_that_terminator(tmp_path: Path) -> None:
    manifest = load_manifest(_multibyte(tmp_path, "after_record: 1"))
    assert manifest.fixtures[0].splices[0].after_record == 1


def test_splices_out_of_order_are_rejected(tmp_path: Path) -> None:
    out_of_order = """
version: 1
master_seed: "test/corpus/v1"
fixtures:
  - name: "39_invalid.csv"
    generator: literal
    encoding: utf-8
    content: "id\\n1\\n"
    splices:
      - { bytes: "\\\\x80", offset: 4 }
      - { bytes: "\\\\x80", offset: 2 }
"""
    with pytest.raises(ManifestError, match="not after the previous"):
        load_manifest(_write(tmp_path, out_of_order))


def test_splices_on_a_generated_kind_are_rejected(tmp_path: Path) -> None:
    spliced_tabular = VALID + '    splices:\n      - { bytes: "\\\\x80", offset: 0 }\n'

    with pytest.raises(ManifestError, match="splices need a literal"):
        load_manifest(_write(tmp_path, spliced_tabular))


def test_a_mark_and_a_splice_in_one_fixture_are_rejected(tmp_path: Path) -> None:
    # Both insert bytes at declared positions. "Is this offset measured before
    # or after the mark?" is precisely the question a fixture must not raise.
    both = """
version: 1
master_seed: "test/corpus/v1"
fixtures:
  - name: "39_invalid.csv"
    generator: literal
    encoding: utf-8
    bom: true
    content: "id\\n1\\n"
    splices:
      - { bytes: "\\\\x80", offset: 2 }
"""
    with pytest.raises(ManifestError, match="both bom and splices"):
        load_manifest(_write(tmp_path, both))


def test_a_terminator_cycle_of_one_or_alongside_a_single_value_is_rejected(
    tmp_path: Path,
) -> None:
    single_entry = VALID.replace('    line_terminator: "\\n"\n', '    line_terminators: ["\\n"]\n')
    with pytest.raises(ManifestError, match="at least two"):
        load_manifest(_write(tmp_path, single_entry))

    with pytest.raises(ManifestError, match="both line_terminator and line_terminators"):
        load_manifest(_write(tmp_path, VALID + '    line_terminators: ["\\r\\n", "\\n"]\n'))

    mixed = VALID.replace(
        '    line_terminator: "\\n"\n', '    line_terminators: ["\\r\\n", "\\n"]\n'
    )
    manifest = load_manifest(_write(tmp_path, mixed))
    fixture = manifest.fixtures[0]
    assert (fixture.terminator_for(0), fixture.terminator_for(1)) == ("\r\n", "\n")


def test_an_unknown_terminator_in_the_cycle_is_rejected(tmp_path: Path) -> None:
    bad = VALID.replace('    line_terminator: "\\n"\n', '    line_terminators: ["\\n", "\\v"]\n')
    with pytest.raises(ManifestError, match="line_terminators entry"):
        load_manifest(_write(tmp_path, bad))


def test_a_repeat_column_needs_one_character_and_a_positive_length(tmp_path: Path) -> None:
    spec = 'amount: { kind: decimal, min: "1.00", max: "9.99", scale: 2 }'

    with pytest.raises(ManifestError, match="exactly one character"):
        load_manifest(
            _write(tmp_path, VALID.replace(spec, "amount: { kind: repeat, char: xy, length: 3 }"))
        )

    with pytest.raises(ManifestError, match="length must be at least 1"):
        load_manifest(
            _write(tmp_path, VALID.replace(spec, "amount: { kind: repeat, char: x, length: 0 }"))
        )

    manifest = load_manifest(
        _write(tmp_path, VALID.replace(spec, "amount: { kind: repeat, char: x, length: 9 }"))
    )
    assert manifest.fixtures[0].row_spec["amount"].length == 9  # type: ignore[union-attr]


def _multipart(overrides: str) -> str:
    return (
        VALID
        + """
  - name: "62_multipart_split"
    generator: multipart
    wraps: "01_simple.csv"
    encoding: utf-8
"""
        + overrides
    )


def test_a_split_below_two_parts_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ManifestError, match="at least 2"):
        load_manifest(_write(tmp_path, _multipart('    line_terminator: "\\n"\n    parts: 1\n')))

    manifest = load_manifest(
        _write(tmp_path, _multipart('    line_terminator: "\\n"\n    parts: 2\n'))
    )
    assert manifest.fixtures[1].parts == 2


def test_a_split_into_more_parts_than_records_is_rejected(tmp_path: Path) -> None:
    # VALID declares three data rows; a fourth part would be empty.
    with pytest.raises(ManifestError, match="only 3 data record"):
        load_manifest(_write(tmp_path, _multipart('    line_terminator: "\\n"\n    parts: 4\n')))


def test_a_part_set_disagreeing_with_its_target_is_rejected(tmp_path: Path) -> None:
    # The parts are byte ranges of the target, so a different terminator would
    # split at boundaries the target does not have.
    with pytest.raises(ManifestError, match="must agree"):
        load_manifest(_write(tmp_path, _multipart('    line_terminator: "\\r\\n"\n    parts: 2\n')))


def test_a_split_count_on_a_single_file_kind_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ManifestError, match="Only a multipart fixture splits"):
        load_manifest(_write(tmp_path, VALID + "    parts: 2\n"))


def test_generation_refuses_a_character_the_declared_encoding_cannot_represent(
    tmp_path: Path,
) -> None:
    # The strict error policy, observed. A lenient encoder would write "?" and
    # leave a fixture whose bytes no longer mean what its declaration says —
    # silently, and for the rest of the project's life.
    unrepresentable = """
version: 1
master_seed: "test/corpus/v1"
fixtures:
  - name: "06_windows1250.csv"
    generator: literal
    encoding: cp1250
    content: "id,name\\n1,\\u65e5\\n"
"""
    manifest = load_manifest(_write(tmp_path, unrepresentable))

    with pytest.raises(GeneratorError) as excinfo:
        generate_corpus(manifest, tmp_path / "out")

    message = str(excinfo.value)
    assert "06_windows1250.csv" in message
    assert "cp1250" in message


def test_yaml_object_construction_is_refused(tmp_path: Path) -> None:
    # V5 Input Validation: the manifest is the only untrusted input this phase
    # deserialises. A loader that can construct arbitrary objects is a
    # straight deserialisation vulnerability (T-01-13).
    path = _write(
        tmp_path,
        "version: 1\n"
        'master_seed: "x"\n'
        "fixtures: !!python/object/apply:os.system ['echo pwned']\n",
    )

    with pytest.raises(ManifestError):
        load_manifest(path)
