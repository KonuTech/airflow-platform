"""Unit tests for ``dataplat.normalize.unicode``.

Covers ``UnicodeNormalizer``'s unconditional NFC pass (CSV-12, D-15) against
corpus fixtures ``44_unicode_nfc_vs_nfd.csv`` (NFC/NFD collapse to the same
bytes) and ``42_zero_width_and_bidi.csv`` (invisible characters NFC does
NOT strip).

Every ``ctx: PipelineContext`` passed below is a placeholder built by
``_make_context()``, mirroring ``tests/unit/test_pipeline_errors.py``'s own
helper: ``UnicodeNormalizer.apply()`` dereferences only ``chunk``.
"""

from __future__ import annotations

import unicodedata

from dataplat.models.identity import RunContext
from dataplat.models.record import RecordChunk
from dataplat.normalize.unicode import UnicodeNormalizer
from dataplat.pipeline.protocol import PipelineContext


def _make_context() -> PipelineContext:
    """Build a placeholder ``PipelineContext`` -- only ``run`` is real."""
    return PipelineContext(
        run=RunContext(run_id=1, idempotency_key="test-run"),
        config=None,  # type: ignore[arg-type] -- unused by the code under test
        metadata=None,  # type: ignore[arg-type] -- unused by the code under test
        objects=None,  # type: ignore[arg-type] -- unused by the code under test
        db=None,  # type: ignore[arg-type] -- unused by the code under test
        log=None,  # type: ignore[arg-type] -- unused by the code under test
    )


def _chunk(
    rows: list[tuple[str | bool | None, ...]],
    *,
    first_ordinal: int = 0,
    expected_field_count: int = 2,
) -> RecordChunk:
    return RecordChunk(
        rows=tuple(rows),
        first_ordinal=first_ordinal,
        expected_field_count=expected_field_count,
    )


# --- Fixture 44: NFC vs NFD collapse to the same, byte-identical string -----
def test_unicode_normalizer_collapses_nfc_and_nfd_forms_to_the_same_bytes() -> None:
    # corpus.yaml's own R9 comment: the generator applies unicodedata.normalize
    # explicitly, rather than pasting two visually-identical literals, which
    # editors/git filters/terminals can silently collapse. Reproduced the
    # same way here.
    nfc_form = unicodedata.normalize("NFC", "Wiśniewski")
    nfd_form = unicodedata.normalize("NFD", nfc_form)
    assert nfc_form != nfd_form  # sanity: the fixture's whole premise

    chunk = _chunk([("1", nfc_form), ("1", nfd_form)])
    normalizer = UnicodeNormalizer()

    result = normalizer.apply(_make_context(), chunk)

    assert result.rejected == []
    row0, row1 = result.chunk.rows
    normalized_0, normalized_1 = row0[1], row1[1]
    assert normalized_0 == normalized_1
    assert isinstance(normalized_0, str)
    assert isinstance(normalized_1, str)
    assert unicodedata.is_normalized("NFC", normalized_0)
    assert unicodedata.is_normalized("NFC", normalized_1)


# --- Fixture 42: invisible characters survive NFC untouched -----------------
def test_unicode_normalizer_does_not_strip_zero_width_or_bidi_marks() -> None:
    zero_width = "AB\u200bC12"  # U+200B ZERO WIDTH SPACE
    ltr_mark = "AB\u200eC12"  # U+200E LEFT-TO-RIGHT MARK
    clean = "ABC12"
    chunk = _chunk([("1", zero_width), ("2", ltr_mark), ("3", clean)])
    normalizer = UnicodeNormalizer()

    result = normalizer.apply(_make_context(), chunk)

    assert result.rejected == []
    values = [row[1] for row in result.chunk.rows]
    # NFC alone does not remove format/invisible characters -- all three
    # stay distinct (D-15 is NFC only, never invisible-character stripping).
    assert values == [zero_width, ltr_mark, clean]
    assert len({v for v in values if isinstance(v, str)}) == 3


def test_unicode_normalizer_never_rejects_a_row() -> None:
    chunk = _chunk([("1", "plain"), ("2", "also plain")])
    normalizer = UnicodeNormalizer()

    result = normalizer.apply(_make_context(), chunk)

    assert result.rejected == []
    assert result.findings == []
    assert len(result.chunk.rows) == 2


def test_unicode_normalizer_passes_a_none_field_through_unchanged_never_raises() -> None:
    nfd_value = unicodedata.normalize("NFD", "Wiśniewski")
    chunk = _chunk([("1", None), ("2", nfd_value)])
    normalizer = UnicodeNormalizer()

    result = normalizer.apply(_make_context(), chunk)

    assert result.rejected == []
    rows = result.chunk.rows
    assert rows[0][1] is None
    assert rows[1][1] == unicodedata.normalize("NFC", nfd_value)


def test_unicode_normalizer_passes_a_bool_field_through_unchanged_never_raises() -> None:
    # Defensive: BooleanNormalizer (plan 06-11 Task 1) already writes real
    # Python bool values into a row by the time UnicodeNormalizer runs last
    # over every column (plan 06-16's wiring) -- a bool is non-str exactly
    # like None, and must never reach unicodedata.normalize() either.
    chunk = _chunk([("1", True), ("2", False)])
    normalizer = UnicodeNormalizer()

    result = normalizer.apply(_make_context(), chunk)

    assert result.rejected == []
    rows = result.chunk.rows
    assert rows[0][1] is True
    assert rows[1][1] is False


def test_unicode_normalizer_never_raises_on_a_mixed_row() -> None:
    # A single row carrying a str, a None and a bool field simultaneously --
    # the realistic shape once earlier normalizers in a wired pipeline
    # (plan 06-16) have already touched some columns but not others.
    nfd_value = unicodedata.normalize("NFD", "Wiśniewski")
    chunk = _chunk(
        [("1", nfd_value, None, True)],
        expected_field_count=4,
    )
    normalizer = UnicodeNormalizer()

    result = normalizer.apply(_make_context(), chunk)

    assert result.rejected == []
    row = result.chunk.rows[0]
    assert row[1] == unicodedata.normalize("NFC", nfd_value)
    assert row[2] is None
    assert row[3] is True
