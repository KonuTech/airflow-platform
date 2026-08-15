"""``DateNormalizer`` — CSV-09's explicit-format date/timestamp parser.

Mirrors ``dataplat.pipeline.engine.RaggedRowGuard`` exactly: a ``name`` class
attribute, an ``apply(ctx, chunk) -> StageResult`` that never raises for a
row-level problem, and every runtime parameter threaded in via constructor
injection rather than read from a global. Every parse uses
``datetime.strptime`` against a contract-declared format -- never
``dateutil.parser.parse``, never a guessed format (STACK.md §F).

Built against nine corpus fixtures (``tests/fixtures/corpus.yaml``), each
pinning one named failure mode a naive date parser gets wrong:

* 22/23 (``_eu_dates``/``_us_dates``) -- the SAME three dates, opposite
  conventions; the declared format decides, the rendering carries no
  evidence of its own convention.
* 52 (``_date_ambiguous_dm_vs_md``) -- genuinely undecidable from the data
  alone; both readings are valid dates, so neither raises -- only the
  contract-declared format resolves it.
* 53 (``_two_digit_year``) -- a two-digit year cannot be completed without a
  declared pivot; CPython's own inherited default is an accident, not a
  decision (:func:`_apply_two_digit_year_pivot`).
* 54 (``_excel_serial_dates``) -- a spreadsheet serial is not a rendered
  date; the EPOCH plays the role a ``strptime`` format plays elsewhere, and
  the 1900 epoch's phantom 1900-02-29 (inherited from the Lotus 1-2-3 bug)
  is rejected, never silently offset past.
* 55/56 (``_dst_gap_and_overlap``/``_mixed_timezone_offsets``) -- DST-aware
  naive-local-time classification (QUAL-17), added by this module's Task 2.
* 59 (``_numeric_null_sentinels``) -- an already-null-normalized field (a
  contract-declared sentinel like ``"9999-12-31"``/``"0000-00-00"``, turned
  into ``None`` upstream by ``NullTokenNormalizer``, plan 06-11 Task 1) must
  pass through unchanged -- never parsed, never rejected.
"""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, cast

from dataplat.models.record import RejectedRecord, StageResult
from dataplat.observability import metrics
from dataplat.pipeline.protocol import StreamingStage

if TYPE_CHECKING:
    from dataplat.models.record import RecordChunk
    from dataplat.pipeline.protocol import PipelineContext

type _RowOutcome = tuple[str, ...] | RejectedRecord

_SPREADSHEET_EPOCHS: frozenset[str] = frozenset({"1900", "1904"})
_TIME_COMPONENT_DIRECTIVES: tuple[str, ...] = ("%H", "%M", "%S")

# Excel/Lotus 1-2-3's 1900 date-system bug: 1900 is treated as a leap year
# even though it is not (not divisible by 400), inserting a phantom
# 1900-02-29 at serial 60 -- kept by Excel for backward compatibility ever
# since. Two anchor dates are needed, not one: every serial from 61 onward
# sits one day further from 1899-12-31 than serials 1-59 do, because the
# phantom day itself never consumed a day of real elapsed time (corpus
# fixture 54, verified against Excel's own displayed values).
_EXCEL_1900_PHANTOM_SERIAL: int = 60
_EXCEL_1900_BASE_BELOW_PHANTOM: dt.date = dt.date(1899, 12, 31)  # serial < 60
_EXCEL_1900_BASE_ABOVE_PHANTOM: dt.date = dt.date(1899, 12, 30)  # serial > 60
_EXCEL_1904_BASE: dt.date = dt.date(1904, 1, 1)  # serial 0; no leap-year bug


def _apply_two_digit_year_pivot(parsed: dt.datetime, pivot: int) -> dt.datetime:
    """Re-derive ``parsed``'s year from a DECLARED two-digit-year pivot.

    ``strptime`` already assigned *some* century to a ``%y`` year using
    CPython's own inherited default (fixture 53:
    ``python_strptime_inherited_pivot``) -- this function discards that
    guess and recomputes the century from the contract's own declared
    pivot instead ("the pivot is a contract decision rather than something
    a reader may invent"). ``parsed.year % 100`` recovers the original
    two-digit value losslessly, regardless of which century ``strptime``
    picked, so this re-derivation is exact.

    Args:
        parsed: The datetime ``strptime`` already produced.
        pivot: The last two-digit year (``0``-``99``) that resolves to the
            2000s; every value above it resolves to the 1900s.

    Returns:
        ``parsed`` with its year replaced per the declared pivot.
    """
    two_digit_year = parsed.year % 100
    century = 2000 if two_digit_year <= pivot else 1900
    return parsed.replace(year=century + two_digit_year)


def _resolve_excel_1900_serial(serial: int) -> dt.date | None:
    """Convert an Excel 1900-system serial to a date.

    Args:
        serial: The raw spreadsheet serial integer.

    Returns:
        The resolved date, or ``None`` for serial 60 -- the phantom
        1900-02-29 leap day that never existed (1900 is not a leap year).
    """
    if serial == _EXCEL_1900_PHANTOM_SERIAL:
        return None
    base = (
        _EXCEL_1900_BASE_BELOW_PHANTOM
        if serial < _EXCEL_1900_PHANTOM_SERIAL
        else _EXCEL_1900_BASE_ABOVE_PHANTOM
    )
    return base + dt.timedelta(days=serial)


def _replace_field(row: tuple[str, ...], index: int, value: str) -> tuple[str, ...]:
    """Return a copy of ``row`` with the field at ``index`` replaced by ``value``."""
    return (*row[:index], value, *row[index + 1 :])


class DateNormalizer(StreamingStage):
    """Parses a declared-format date/timestamp column, rejecting invalid values.

    A column is configured as exactly one of two mutually exclusive shapes:
    a rendered date/timestamp under a ``strptime`` format (``format``), or a
    bare spreadsheet serial integer under a declared epoch
    (``spreadsheet_epoch``) -- never both, never neither. See the module
    docstring for the corpus fixtures this class is built against.
    """

    name = "date_normalizer"

    def __init__(
        self,
        *,
        column_index: int,
        column_name: str,
        format: str | None = None,  # noqa: A002 -- matches ColumnContract.format (config/model.py), this phase's established contract vocabulary
        two_digit_year_pivot: int | None = None,
        spreadsheet_epoch: str | None = None,
    ) -> None:
        """Configure the column this stage parses.

        Args:
            column_index: The 0-based position of the date/timestamp column
                within each row tuple.
            column_name: The column's name, for diagnostics
                (``RejectedRecord.error_column``).
            format: The ``strptime`` format string for a rendered
                date/timestamp column. Mutually exclusive with
                ``spreadsheet_epoch`` -- exactly one of the two must be
                supplied.
            two_digit_year_pivot: The last two-digit year (``0``-``99``)
                that resolves to the 2000s; required when ``format``
                contains ``%y`` (fixture 53: a missing pivot is a
                contract-authoring mistake, not a row-level problem).
            spreadsheet_epoch: ``"1900"`` or ``"1904"`` -- the spreadsheet
                epoch system a bare serial-integer column is declared under
                (fixture 54). Mutually exclusive with ``format``.

        Raises:
            ValueError: ``format``/``spreadsheet_epoch`` are both supplied
                or neither is; ``spreadsheet_epoch`` names an unsupported
                epoch; or ``format`` contains ``%y`` with no declared
                ``two_digit_year_pivot``.
        """
        if (format is None) == (spreadsheet_epoch is None):
            msg = (
                f"{type(self).__name__} requires exactly one of `format` or "
                "`spreadsheet_epoch` -- a column is either a rendered date "
                "under a strptime format or a spreadsheet serial integer, "
                "never both, never neither"
            )
            raise ValueError(msg)
        if spreadsheet_epoch is not None and spreadsheet_epoch not in _SPREADSHEET_EPOCHS:
            msg = (
                f"unsupported spreadsheet_epoch {spreadsheet_epoch!r}; "
                f"supported: {sorted(_SPREADSHEET_EPOCHS)}"
            )
            raise ValueError(msg)
        if format is not None and "%y" in format and two_digit_year_pivot is None:
            msg = (
                f"{type(self).__name__}: format {format!r} contains a "
                "two-digit year (%y); `two_digit_year_pivot` must be "
                "declared explicitly -- the pivot is a contract decision, "
                "never something a reader may invent (corpus fixture 53)"
            )
            raise ValueError(msg)

        self.column_index = column_index
        self.column_name = column_name
        self.format = format
        self.two_digit_year_pivot = two_digit_year_pivot
        self.spreadsheet_epoch = spreadsheet_epoch
        self._has_two_digit_year = format is not None and "%y" in format
        self._has_time_component = format is not None and any(
            directive in format for directive in _TIME_COMPONENT_DIRECTIVES
        )

    def apply(self, ctx: PipelineContext, chunk: RecordChunk) -> StageResult:  # noqa: ARG002
        """Parse or reject this stage's declared column for every row in ``chunk``.

        Never raises, regardless of how malformed a value is: an invalid
        date/timestamp becomes a ``RejectedRecord`` instead. A field value
        that is already ``None`` (normalized to absent by an upstream
        ``NullTokenNormalizer`` for this nullable column) passes through
        unchanged -- never parsed, never rejected.

        Args:
            ctx: The current pipeline context. Unused: this stage's
                decisions depend only on ``chunk``.
            chunk: The chunk to process.

        Returns:
            A ``StageResult`` whose ``chunk`` holds every row (kept rows
            with this stage's column replaced by a canonical ISO-format
            string, unparseable rows entirely) minus the rows this stage
            rejected, plus one ``RejectedRecord`` per rejected row.
        """
        kept: list[tuple[str, ...]] = []
        rejected: list[RejectedRecord] = []
        for i, row in enumerate(chunk.rows):
            row_number = chunk.first_ordinal + i
            # RecordChunk.rows is declared tuple[str, ...], but a nullable
            # date/timestamp column's field may already be the real Python
            # None here -- normalized to absent by an upstream
            # NullTokenNormalizer (plan 06-11 Task 1's platform-wide
            # convention) before this stage ever runs. cast() makes that
            # narrower-than-declared runtime reality explicit for mypy
            # rather than silently widening RecordChunk.rows' own type,
            # which is out of this plan's file scope (models/record.py).
            raw_value = cast("str | None", row[self.column_index])
            if raw_value is None:
                kept.append(row)
                continue

            outcome = (
                self._parse_spreadsheet_serial(raw_value, row_number, row)
                if self.spreadsheet_epoch is not None
                else self._parse_plain_format(raw_value, row_number, row)
            )
            if isinstance(outcome, RejectedRecord):
                rejected.append(outcome)
            else:
                kept.append(outcome)

        metrics.increment("rows_rejected", len(rejected))
        metrics.increment("rows_kept", len(kept))
        return StageResult(chunk=chunk.replace(rows=tuple(kept)), rejected=rejected, findings=[])

    def _parse_plain_format(
        self, raw_value: str, row_number: int, row: tuple[str, ...]
    ) -> _RowOutcome:
        """Parse ``raw_value`` under ``self.format``, returning the updated row or a rejection."""
        try:
            # naive on purpose here: this branch is the non-timezone-aware
            # plain-format path (see the timezone-aware _parse_naive_local
            # added by Task 2 for the zone-resolving counterpart).
            parsed = dt.datetime.strptime(raw_value, cast("str", self.format))  # noqa: DTZ007
        except ValueError as exc:
            return RejectedRecord(
                source_row_number=row_number,
                error_type="invalid-calendar-date",
                error_message=f"{raw_value!r} does not match format {self.format!r}: {exc}",
                raw_line=raw_value,
                error_column=self.column_name,
            )
        if self._has_two_digit_year:
            parsed = _apply_two_digit_year_pivot(parsed, cast("int", self.two_digit_year_pivot))

        iso_value = parsed.isoformat() if self._has_time_component else parsed.date().isoformat()
        return _replace_field(row, self.column_index, iso_value)

    def _parse_spreadsheet_serial(
        self, raw_value: str, row_number: int, row: tuple[str, ...]
    ) -> _RowOutcome:
        """Convert ``raw_value`` from a spreadsheet serial integer.

        Returns the updated row on success, or a ``RejectedRecord``.
        """
        try:
            serial = int(raw_value)
        except ValueError as exc:
            return RejectedRecord(
                source_row_number=row_number,
                error_type="invalid-calendar-date",
                error_message=f"{raw_value!r} is not an integer spreadsheet serial: {exc}",
                raw_line=raw_value,
                error_column=self.column_name,
            )

        if self.spreadsheet_epoch == "1900":
            resolved = _resolve_excel_1900_serial(serial)
            if resolved is None:
                return RejectedRecord(
                    source_row_number=row_number,
                    error_type="spreadsheet-serial-date-does-not-exist",
                    error_message=(
                        f"serial {serial} denotes the phantom 1900-02-29 leap day "
                        "(1900 is not a leap year; inherited from the Lotus 1-2-3 bug)"
                    ),
                    raw_line=raw_value,
                    error_column=self.column_name,
                )
        else:  # "1904", validated at construction time
            resolved = _EXCEL_1904_BASE + dt.timedelta(days=serial)

        return _replace_field(row, self.column_index, resolved.isoformat())
