"""``detect_header`` -- CSV-07/CSV-08's header/metadata-preamble/footer detector.

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
values to still be recognised as the header, so the duplicate-name
rejection below has something to reject. A row that failed detection
outright would never reach that check.

Once a header is known, two more things are detected against the rows that
follow it: trailing footer rows (a differing field count, or a match
against a contract-supplied regex -- fixtures ``13_footer.csv``,
``64_footer_totals_with_different_column_count.csv``), and an interior row
that exactly repeats the header's own values, e.g. a concatenated export
(fixture ``63_repeated_header_mid_file.csv``). Both are recorded on
``HeaderDetection`` rather than silently dropped from ``rows`` --
excluding them from the loaded record set is a later wiring plan's job
(the streaming reader named in this plan's own ``<objective>``); this
module only detects.

Exact and case-variant duplicate header names both raise
``dataplat.errors.FileInspectionError`` unconditionally. Corpus fixture
48's own declared outcome is ``rejected-file`` regardless of
``CsvParsingConfig.header_case_sensitive``'s value, because PostgreSQL
folds unquoted identifiers to lower case: a case-insensitive collision is
real structural damage no matter how header-to-contract name matching is
configured elsewhere. ``header_case_sensitive`` therefore governs a
different, later concern (header-to-``columns:``-contract matching) and is
deliberately not a parameter of this function.

Input contract (T-06-27, this plan's own threat register): ``rows`` is a
bounded prefix of the file's physical rows, already split by the dialect
detector's delimiter -- enough rows to see the modal field count reliably
(a caller passing, say, the first ~20 rows is enough for every corpus
fixture this module is tested against), never the full streamed file.
Bounding it is the caller's responsibility, not this function's.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING

from dataplat.errors import FileInspectionError

if TYPE_CHECKING:
    from collections.abc import Sequence

# D-25's kebab-case diagnostic-code convention (dataplat.diagnostics
# DIAGNOSTIC_CODES already declares this exact string).
_DIAGNOSTIC_DUPLICATE_HEADER_NAMES = "duplicate-header-names"


@dataclass(frozen=True, slots=True)
class HeaderDetection:
    """One file's detected header, preamble, footer and repeated-header rows.

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
        footer_row_indices: Absolute row indices (into the original
            ``rows`` argument) of trailing rows classified as a footer --
            never loaded as records. Empty when no footer was detected or
            no header was found.
        repeated_header_row_indices: Absolute row indices of interior rows
            whose values exactly equal ``raw_header`` -- a concatenated
            export's repeated header line (fixture
            ``63_repeated_header_mid_file.csv``). Never loaded as records.
    """

    header_row_index: int | None
    raw_header: tuple[str, ...]
    trimmed_header: tuple[str, ...]
    preamble_row_count: int
    has_header: bool
    footer_row_indices: tuple[int, ...]
    repeated_header_row_indices: tuple[int, ...]


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


def _reject_duplicate_header_names(raw_header: tuple[str, ...]) -> None:
    """Raise if ``raw_header`` has an exact or case-variant duplicate name.

    A single case-folded grouping catches both: two identical strings are
    trivially also case-insensitive-equal, so no separate exact-match pass
    is needed -- fixture ``14_duplicate_columns.csv``'s exact
    ``"amount"``/``"amount"`` collision is caught the same way as
    ``48_duplicate_header_names_case_variant.csv``'s ``"Name"``/``"name"``
    one.

    Args:
        raw_header: The header's field values, untrimmed.

    Raises:
        FileInspectionError: A name collides with another after
            ``str.casefold()``. ``context["colliding_names"]`` names every
            colliding column, sorted for a deterministic message
            (PROJECT.md's determinism constraint) -- never a silent rename
            or last-wins resolution.
    """
    folded: dict[str, list[str]] = {}
    for name in raw_header:
        folded.setdefault(name.casefold(), []).append(name)
    colliding = sorted(name for names in folded.values() if len(names) > 1 for name in names)
    if colliding:
        msg = f"duplicate header names after case-folding: {colliding}"
        raise FileInspectionError(
            msg,
            context={
                "diagnostic_code": _DIAGNOSTIC_DUPLICATE_HEADER_NAMES,
                "colliding_names": colliding,
            },
        )


def _detect_footer_rows(
    data_rows: Sequence[tuple[str, ...]],
    *,
    data_start_index: int,
    header_field_count: int,
    skip_footer_rows: int,
    footer_patterns: Sequence[str],
) -> tuple[int, ...]:
    """Identify trailing footer rows among ``data_rows``.

    ``skip_footer_rows`` (a non-zero ``CsvParsingConfig`` override) takes
    precedence and skips exactly that many trailing rows without scoring
    them at all. Otherwise, walking backward from the end, a row is footer
    when its field count differs from ``header_field_count`` or its first
    field matches one of ``footer_patterns`` -- the walk stops at the
    first (from the end) row that matches neither, so a footer can never
    be "found" in the middle of real data (fixtures ``13_footer.csv``,
    ``64_footer_totals_with_different_column_count.csv``).

    Args:
        data_rows: Every row after the header.
        data_start_index: ``data_rows[0]``'s absolute index in the
            original ``rows`` sequence, used to translate offsets back to
            absolute indices.
        header_field_count: The header's own field count -- the expected
            shape a real data row matches.
        skip_footer_rows: A contract override that skips this many
            trailing rows unconditionally when non-zero.
        footer_patterns: Regexes matched against a row's first field.

    Returns:
        Absolute row indices classified as footer, in ascending order.
    """
    row_count = len(data_rows)
    if skip_footer_rows:
        skip_count = min(skip_footer_rows, row_count)
        start = data_start_index + row_count - skip_count
        return tuple(range(start, data_start_index + row_count))

    compiled_patterns = [re.compile(pattern) for pattern in footer_patterns]
    footer_offsets: list[int] = []
    for offset in range(row_count - 1, -1, -1):
        row = data_rows[offset]
        field_count_differs = len(row) != header_field_count
        pattern_matches = bool(row) and any(pattern.match(row[0]) for pattern in compiled_patterns)
        if not (field_count_differs or pattern_matches):
            break
        footer_offsets.append(offset)
    return tuple(data_start_index + offset for offset in reversed(footer_offsets))


def _detect_repeated_header_rows(
    data_rows: Sequence[tuple[str, ...]],
    *,
    data_start_index: int,
    raw_header: tuple[str, ...],
) -> tuple[int, ...]:
    """Identify interior rows whose values exactly equal ``raw_header``.

    A concatenated export re-embeds the header line mid-file (fixture
    ``63_repeated_header_mid_file.csv``); loaded as a record it becomes a
    row whose ``amount`` field is the literal string ``"amount"``.

    Args:
        data_rows: Every row after the header.
        data_start_index: ``data_rows[0]``'s absolute index in the
            original ``rows`` sequence.
        raw_header: The header's own field values, to compare each row
            against.

    Returns:
        Absolute row indices whose values exactly equal ``raw_header``, in
        ascending order.
    """
    return tuple(
        data_start_index + offset for offset, row in enumerate(data_rows) if row == raw_header
    )


def _find_header_row(
    rows: Sequence[tuple[str, ...]],
) -> tuple[int | None, tuple[str, ...]]:
    """Score every candidate row, returning the first that clears the header-shape gates.

    Args:
        rows: Every candidate row to score, in order.

    Returns:
        ``(index, row)`` for the first row that clears
        ``_row_is_header_shaped``'s gates, or ``(None, ())`` when no row
        does.
    """
    for index, row in enumerate(rows):
        if _row_is_header_shaped(row, rows[index + 1 :]):
            return index, row
    return None, ()


def _not_found() -> HeaderDetection:
    """Return the shared "no header could be established" result.

    Used both for a genuinely empty ``rows`` (``18_empty.csv``) and for
    "every candidate row failed the header-shape gates"
    (``11_no_header.csv``) -- the two cases a caller distinguishes by
    whether ``rows`` itself was empty, per ``HeaderDetection.has_header``'s
    own docstring.

    Returns:
        A ``HeaderDetection`` with ``has_header=False`` and every other
        field at its empty/zero value.
    """
    return HeaderDetection(
        header_row_index=None,
        raw_header=(),
        trimmed_header=(),
        preamble_row_count=0,
        has_header=False,
        footer_row_indices=(),
        repeated_header_row_indices=(),
    )


def detect_header(
    rows: Sequence[tuple[str, ...]],
    *,
    contract_header_row: int | None = None,
    header_trim: bool = False,
    skip_footer_rows: int = 0,
    footer_patterns: Sequence[str] = (),
) -> HeaderDetection:
    """Detect a CSV file's header row, metadata preamble, footer and repeats.

    See the module docstring for the full detection contract, the scoring
    signals, and why duplicate-name rejection is unconditional.

    Args:
        rows: A bounded prefix of the file's physical rows, already split
            by the dialect detector's delimiter (T-06-27: never the whole
            streamed file -- the caller bounds this).
        contract_header_row: ``CsvParsingConfig.header_row`` -- when given,
            scoring is skipped entirely and this index is trusted directly.
        header_trim: ``CsvParsingConfig.header_trim`` -- whether
            ``trimmed_header`` strips each field's surrounding whitespace.
        skip_footer_rows: ``CsvParsingConfig.skip_footer_rows`` -- when
            non-zero, skips exactly this many trailing rows as footer
            without scoring them.
        footer_patterns: Contract-configured regexes matched against a
            trailing row's first field to classify it as footer.

    Returns:
        The detection result. See ``HeaderDetection``'s own docstring for
        field meanings.

    Raises:
        FileInspectionError: The detected (or contract-given) header row
            has an exact or case-variant duplicate name.
    """
    header_row_index: int | None
    raw_header: tuple[str, ...]

    if contract_header_row is not None:
        header_row_index = contract_header_row
        raw_header = rows[contract_header_row]
    elif not rows:
        return _not_found()
    else:
        header_row_index, raw_header = _find_header_row(rows)

    if header_row_index is None:
        return _not_found()

    _reject_duplicate_header_names(raw_header)

    trimmed_header = tuple(value.strip() for value in raw_header) if header_trim else raw_header
    data_start_index = header_row_index + 1
    data_rows = rows[data_start_index:]
    footer_row_indices = _detect_footer_rows(
        data_rows,
        data_start_index=data_start_index,
        header_field_count=len(raw_header),
        skip_footer_rows=skip_footer_rows,
        footer_patterns=footer_patterns,
    )
    repeated_header_row_indices = _detect_repeated_header_rows(
        data_rows, data_start_index=data_start_index, raw_header=raw_header
    )

    return HeaderDetection(
        header_row_index=header_row_index,
        raw_header=raw_header,
        trimmed_header=trimmed_header,
        preamble_row_count=header_row_index,
        has_header=True,
        footer_row_indices=footer_row_indices,
        repeated_header_row_indices=repeated_header_row_indices,
    )
