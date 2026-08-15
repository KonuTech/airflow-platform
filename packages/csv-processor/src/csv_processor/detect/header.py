"""``detect_header`` -- CSV-07/CSV-08's header-row and metadata-preamble detector.

For each candidate row index, score it on (a) all fields non-empty, (b) all
fields non-numeric, (c) field count equal to the modal field count of the
following rows -- STACK.md's own ``detector/header.py`` design (see
06-06-PLAN.md's ``<objective>``). The first row clearing every applicable
gate is the header; everything before it is a metadata preamble
(``12_metadata_before_header.csv``). A row shaped exactly like the data
below it (``11_no_header.csv``) clears no row at all, so ``has_header`` is
``False`` and column names must come from the contract instead.

Value uniqueness -- STACK.md §11's fourth signal -- is deliberately not a
gate here: fixtures ``14_duplicate_columns.csv``/
``48_duplicate_header_names_case_variant.csv`` need a row *with* duplicate
values to still be recognised as the header, so a later plan's
duplicate-name rejection has something to reject. A row that failed
detection outright would never reach that check. (Wave 2's sibling plan
adds that rejection directly on this module; see its own extension.)

Input contract (T-06-27, this plan's own threat register): ``rows`` is a
bounded prefix of the file's physical rows, already split by the dialect
detector's delimiter -- enough rows to see the modal field count reliably
(a caller passing, say, the first ~20 rows is enough for every corpus
fixture this module is tested against), never the full streamed file.
Bounding it is the caller's responsibility, not this function's.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass(frozen=True, slots=True)
class HeaderDetection:
    """One file's detected header row and metadata preamble.

    Attributes:
        header_row_index: The 0-based row index the header was found at, or
            ``None`` when no row cleared the detection threshold (fixture
            ``11_no_header.csv``) or ``rows`` was empty (``18_empty.csv``).
        raw_header: The header row's field values exactly as read, before
            any trimming. Empty when ``has_header`` is ``False``.
        trimmed_header: ``raw_header`` with each field's surrounding
            whitespace stripped, when ``header_trim=True`` was requested.
            Equal to ``raw_header`` otherwise -- trimming is a declared
            normalisation, never a parser default (CSV-07,
            ``49_header_with_leading_trailing_spaces.csv``).
        preamble_row_count: The number of rows before the header -- a
            metadata preamble (fixture ``12_metadata_before_header.csv``),
            or ``0`` when the header is at row 0 or none was found.
        has_header: Whether a header row was found at all. ``False`` for
            both the "no header, all rows are data" case
            (``11_no_header.csv``) and the "zero-byte file" case
            (``18_empty.csv``) -- callers distinguish the two by whether
            ``rows`` itself was empty.
    """

    header_row_index: int | None
    raw_header: tuple[str, ...]
    trimmed_header: tuple[str, ...]
    preamble_row_count: int
    has_header: bool


def _looks_numeric(value: str) -> bool:
    """Return whether ``value`` parses as a number (the "000001" trap, STACK.md §12).

    A header name is never a bare number; a data row's identifier or amount
    column often is. This is one of the header-shape signals below.

    Args:
        value: A single field's raw text.

    Returns:
        ``True`` when ``float(value)`` succeeds.
    """
    try:
        float(value)
    except ValueError:
        return False
    return True


def _modal_field_count(rows: Sequence[tuple[str, ...]]) -> int | None:
    """Return the most common field count across ``rows``, or ``None`` when empty.

    Ties break by first occurrence (``Counter.most_common`` is stable over
    insertion order) -- deterministic, per PROJECT.md's determinism
    constraint.

    Args:
        rows: The rows to count field-lengths across.

    Returns:
        The modal field count, or ``None`` when ``rows`` is empty.
    """
    if not rows:
        return None
    counts = Counter(len(row) for row in rows)
    return counts.most_common(1)[0][0]


def _row_is_header_shaped(row: tuple[str, ...], following_rows: Sequence[tuple[str, ...]]) -> bool:
    """Score one candidate row against the header-shape hard gates.

    Value uniqueness (STACK.md §11's fourth signal) is deliberately not a
    gate here -- see the module docstring's explanation of why fixtures
    14/48 must still be detected as a header despite duplicate values.

    Args:
        row: The candidate row.
        following_rows: Every row after the candidate, used to derive the
            expected field count. When empty (the candidate is the last row
            in ``rows`` -- fixture ``19_only_header.csv``), the field-count
            gate is skipped: there is nothing to compare against.

    Returns:
        Whether ``row`` clears every applicable hard gate.
    """
    if not row:
        return False
    if not all(field_value != "" for field_value in row):
        return False
    if not all(not _looks_numeric(field_value) for field_value in row):
        return False
    modal = _modal_field_count(following_rows)
    return modal is None or len(row) == modal


def detect_header(
    rows: Sequence[tuple[str, ...]],
    *,
    contract_header_row: int | None = None,
    header_trim: bool = False,
) -> HeaderDetection:
    """Detect a CSV file's header row and metadata preamble.

    See the module docstring for the full detection contract and the
    scoring signals.

    Args:
        rows: A bounded prefix of the file's physical rows, already split
            by the dialect detector's delimiter (T-06-27: never the whole
            streamed file -- the caller bounds this).
        contract_header_row: ``CsvParsingConfig.header_row`` -- when given,
            scoring is skipped entirely and this index is trusted directly.
        header_trim: ``CsvParsingConfig.header_trim`` -- whether
            ``trimmed_header`` strips each field's surrounding whitespace.

    Returns:
        The detection result. See ``HeaderDetection``'s own docstring for
        field meanings.
    """
    if contract_header_row is not None:
        raw_header = rows[contract_header_row]
        trimmed_header = tuple(value.strip() for value in raw_header) if header_trim else raw_header
        return HeaderDetection(
            header_row_index=contract_header_row,
            raw_header=raw_header,
            trimmed_header=trimmed_header,
            preamble_row_count=contract_header_row,
            has_header=True,
        )

    if not rows:
        return HeaderDetection(
            header_row_index=None,
            raw_header=(),
            trimmed_header=(),
            preamble_row_count=0,
            has_header=False,
        )

    for index, row in enumerate(rows):
        if _row_is_header_shaped(row, rows[index + 1 :]):
            trimmed_header = tuple(value.strip() for value in row) if header_trim else row
            return HeaderDetection(
                header_row_index=index,
                raw_header=row,
                trimmed_header=trimmed_header,
                preamble_row_count=index,
                has_header=True,
            )

    return HeaderDetection(
        header_row_index=None,
        raw_header=(),
        trimmed_header=(),
        preamble_row_count=0,
        has_header=False,
    )
