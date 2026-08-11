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
