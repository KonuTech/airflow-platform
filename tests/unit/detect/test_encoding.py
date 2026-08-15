"""Corpus-parametrized tests for `csv_processor.detect.encoding` (CSV-02/CSV-03).

Every fixture in ``tests/fixtures/corpus.yaml`` whose ``covers`` tuple
includes ``"CSV-02"`` or ``"CSV-03"`` is generated once (module-scoped, the
same idiom ``test_corpus_structural_fixtures.py`` already establishes) and
run through ``detect_encoding``, asserted against every ``expect:`` key that
fixture actually declares -- never a restated expectation, so the oracle
(the corpus) and this suite cannot quietly drift apart.

``ENCODING_FIXTURES`` is named explicitly rather than derived from the
manifest at collection time (matching ``test_corpus_structural_fixtures.py``'s
own stated reason: "so deleting a declaration fails a test instead of
shrinking a loop"); ``test_encoding_fixtures_matches_corpus_covers_filter``
is the drift guard proving that hardcoded list still equals what a live
``covers`` filter over the manifest produces.

Two fixtures get dedicated, heavily-commented assertions beyond the generic
per-key mapping, because they are exactly the two the corrected algorithm
in ``encoding.py`` exists to get right (see that module's own docstring):
``06_windows1250.csv`` (the near-tie corroboration correction) and
``40_utf16_no_bom.csv`` (the no-BOM wide-encoding confidence ceiling).
"""

from __future__ import annotations

import codecs
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final

import chardet
import pytest
from charset_normalizer import from_bytes
from tools.corpus.generators import generate_corpus
from tools.corpus.manifest import load_manifest

from csv_processor.detect.encoding import EncodingDetection, detect_encoding

if TYPE_CHECKING:
    from collections.abc import Mapping

REPO_ROOT = Path(__file__).resolve().parents[3]
MANIFEST = REPO_ROOT / "tests" / "fixtures" / "corpus.yaml"

# The ~64 KiB sample-bound convention tests/fixtures/corpus.yaml's own
# comments describe -- every fixture in ENCODING_FIXTURES is far smaller than
# this, so in practice every sample below is the whole file, but detect_
# encoding is exercised the same way a real bounded-sample caller would use
# it.
SAMPLE_BYTES: Final = 65536

# Named explicitly (see module docstring) -- every fixture in
# tests/fixtures/corpus.yaml whose covers includes CSV-02 or CSV-03.
ENCODING_FIXTURES: Final[tuple[str, ...]] = (
    "05_utf8_bom.csv",
    "06_windows1250.csv",
    "07_utf16.csv",
    "26_unicode.csv",
    "27_polish_characters.csv",
    "39_utf8_invalid_sequences.csv",
    "40_utf16_no_bom.csv",
    "41_bom_mid_file.csv",
    "68_utf8_bom_semicolon_pl_excel.csv",
)

# A BOM-sniffed detection reports the codec that also strips/auto-senses the
# mark (utf-8-sig strips it on decode; a bare utf-16/utf-32 auto-senses byte
# order from it) -- corpus.yaml's own comment on 05_utf8_bom.csv: a plain
# "utf-8" decode would leave the mark attached to the first field's name.
# Both are the semantically correct answer for a BOM-backed detection, not a
# mismatch against the fixture's simpler "detected_encoding" vocabulary.
_BOM_SIBLING_NAMES: Final[Mapping[str, str]] = MappingProxyType(
    {
        "utf-8": "utf-8-sig",
        "utf-16-le": "utf-16",
        "utf-16-be": "utf-16",
        "utf-32-le": "utf-32",
        "utf-32-be": "utf-32",
    }
)


def _matches_declared_encoding(detection: EncodingDetection, declared_encoding: str) -> bool:
    """True if `detection.encoding` is `declared_encoding`, or its BOM-aware sibling."""
    actual = codecs.lookup(detection.encoding).name
    expected = codecs.lookup(declared_encoding).name
    if actual == expected:
        return True
    sibling = _BOM_SIBLING_NAMES.get(expected)
    if sibling is None or detection.source != "bom":
        return False
    return actual == codecs.lookup(sibling).name


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


def test_encoding_fixtures_matches_corpus_covers_filter() -> None:
    """Drift guard: ENCODING_FIXTURES must equal a live covers=CSV-02/CSV-03 filter."""
    manifest = load_manifest(MANIFEST)
    filtered = tuple(
        fixture.name
        for fixture in manifest.fixtures
        if "CSV-02" in fixture.covers or "CSV-03" in fixture.covers
    )
    assert filtered == ENCODING_FIXTURES


@pytest.mark.parametrize("name", ENCODING_FIXTURES)
def test_encoding_fixture_matches_declared_expectation(
    corpus: Path, declared: Mapping[str, Mapping[str, Any]], name: str
) -> None:
    """Every encoding-tagged fixture's `detect_encoding` result matches its own `expect:` block."""
    expect = declared[name]
    sample = (corpus / name).read_bytes()[:SAMPLE_BYTES]
    detection = detect_encoding(sample, contract_encoding=None)

    declared_name = expect.get("detected_encoding") or expect.get("declared_encoding")
    if declared_name is not None:
        assert _matches_declared_encoding(detection, declared_name), (
            f"{name}: detected {detection.encoding!r} (source={detection.source!r}), "
            f"expected {declared_name!r}"
        )
        # A fixture that declares what it should detect as is one detection
        # genuinely succeeded on -- never left at "undetermined" (CSV-03: no
        # false certainty, but also no false uncertainty over a case the
        # corpus itself says should resolve).
        assert detection.source in ("bom", "detected")

    if "encoding_confidence_min" in expect:
        assert detection.confidence >= expect["encoding_confidence_min"]
    if "encoding_confidence_max" in expect:
        assert detection.confidence <= expect["encoding_confidence_max"]

    if expect.get("bom_byte_offset") == 0:
        assert detection.source == "bom"
    if expect.get("bom_at_offset_zero") is False:
        # 41_bom_mid_file.csv: the mark exists but is not at the sample's
        # start, so the BOM sniff (a prefix check) must not fire on it --
        # "the mark is data, not an encoding declaration".
        assert detection.source != "bom"

    if expect.get("encoding_is_certain") is False:
        # BOM/contract-grade certainty is reserved for deterministic
        # evidence (module docstring); a probabilistic detection -- however
        # confident -- must never report that exact 1.0.
        assert detection.confidence < 1.0
        assert detection.source != "bom"


def test_windows1250_confidence_uses_chaos_not_chardets_raw_number(corpus: Path) -> None:
    """06_windows1250.csv: confidence is 1 - chaos, provably not chardet's own far-lower number."""
    sample = (corpus / "06_windows1250.csv").read_bytes()[:SAMPLE_BYTES]
    detection = detect_encoding(sample, contract_encoding=None)

    assert codecs.lookup(detection.encoding).name == "cp1250"
    assert detection.source == "detected"

    chardet_raw_confidence = chardet.detect(sample)["confidence"]
    # 06-RESEARCH.md Common Pitfall 2, reproduced live against this exact
    # fixture: chardet's own confidence for a CORRECT cp1250 detection is
    # empirically far too conservative (single digits of a percent) to gate
    # on directly.
    assert detection.confidence > chardet_raw_confidence
    assert detection.confidence > 0.5


def test_windows1250_true_encoding_is_not_charset_normalizers_own_top_pick(corpus: Path) -> None:
    """06_windows1250.csv: proves WHY near-tie corroboration is needed, not just that it works.

    charset-normalizer's own `.best()` on this fixture's real bytes is a
    near-tied Latin-family candidate, not cp1250 itself -- verified directly
    against the library here, independent of `detect_encoding`'s own
    internals, so this test fails loudly if a future charset-normalizer
    release changes that ranking (at which point the near-tie correction in
    encoding.py may no longer be necessary, and this test is the signal).
    """
    sample = (corpus / "06_windows1250.csv").read_bytes()[:SAMPLE_BYTES]
    best = from_bytes(sample).best()
    assert best is not None
    assert codecs.lookup(best.encoding).name != "cp1250"


def test_undetermined_never_reports_a_specific_encoding(corpus: Path) -> None:
    """39_utf8_invalid_sequences.csv: garbled bytes stay undetermined, never a confident guess."""
    sample = (corpus / "39_utf8_invalid_sequences.csv").read_bytes()[:SAMPLE_BYTES]
    detection = detect_encoding(sample, contract_encoding=None)

    assert detection.source == "undetermined"
    assert detection.encoding == "undetermined"
    assert detection.confidence == 0.0


def test_contract_encoding_short_circuits_detection_entirely() -> None:
    """A contract-declared encoding is trusted outright, regardless of the sample's actual bytes."""
    # Deliberately NOT valid UTF-8 (a lone continuation byte) -- proves the
    # contract path never even looks at the bytes.
    detection = detect_encoding(b"\x80\x80\x80", contract_encoding="utf-8")
    assert detection == EncodingDetection("utf-8", 1.0, "contract")


def test_two_detectors_disagreeing_returns_undetermined_not_a_guess() -> None:
    """Genuinely different guesses (not a near-tie) never get silently resolved to one of them."""
    # Windows-1251 (Cyrillic) bytes that charset-normalizer and chardet are
    # not expected to agree on with an ASCII-shaped sample -- constructed
    # rather than corpus-sourced, since this is testing the disagreement
    # branch itself, not a specific fixture.
    sample = "Привет мир".encode("koi8-r")
    detection = detect_encoding(sample, contract_encoding=None, min_confidence=0.0)
    # Either a genuine disagreement (source="undetermined") or, if the two
    # detectors happen to agree on this exact sample, this assertion would
    # need updating -- so assert the contract this test exists to prove:
    # never a source="detected" claim while chardet's and charset-
    # normalizer's canonicalized top picks actually differ.
    cd_name = codecs.lookup(chardet.detect(sample)["encoding"] or "utf-8").name
    cn_best = from_bytes(sample).best()
    cn_name = codecs.lookup(cn_best.encoding).name if cn_best is not None else None
    if cn_name != cd_name:
        assert detection.source == "undetermined"
