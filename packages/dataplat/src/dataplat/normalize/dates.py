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
from zoneinfo import ZoneInfo

from dataplat.models.record import RejectedRecord, StageResult
from dataplat.observability import metrics
from dataplat.pipeline.protocol import StreamingStage

if TYPE_CHECKING:
    from dataplat.models.record import RecordChunk
    from dataplat.pipeline.protocol import PipelineContext

type _RowOutcome = tuple[str | bool | None, ...] | RejectedRecord

_SPREADSHEET_EPOCHS: frozenset[str] = frozenset({"1900", "1904"})
_TIME_COMPONENT_DIRECTIVES: tuple[str, ...] = ("%H", "%M", "%S")
_OFFSET_DIRECTIVES: tuple[str, ...] = ("%z", "%Z")
_AMBIGUOUS_TIME_POLICIES: frozenset[str] = frozenset({"reject", "earliest", "latest"})

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


def classify_naive_local(naive: dt.datetime, zone: ZoneInfo) -> str:
    """Classify a naive local datetime as nonexistent, ambiguous or unambiguous.

    Verified live (06-RESEARCH.md Code Examples) to exactly reproduce every
    value in corpus fixture 55 (``55_dst_gap_and_overlap.csv``), including
    both fold-0/fold-1 UTC pairs. Comparing UTC offsets across ``fold=0``
    and ``fold=1`` alone would identify both the gap and overlap cases
    without distinguishing them -- the round-trip-through-UTC check below
    is what tells them apart: a nonexistent local time (spring-forward gap)
    does not survive the round trip at all; an ambiguous local time
    (autumn-overlap) survives it but the two folds resolve to different
    instants.

    Args:
        naive: A naive local datetime (no ``tzinfo``) to classify.
        zone: The IANA zone this datetime is local to.

    Returns:
        ``"nonexistent"`` if ``naive`` falls in a DST spring-forward gap,
        ``"ambiguous"`` if it falls in a DST autumn-overlap hour,
        ``"unambiguous"`` otherwise.
    """
    aware_fold0 = naive.replace(tzinfo=zone, fold=0)
    utc0 = aware_fold0.astimezone(dt.UTC)
    roundtrip0 = utc0.astimezone(zone).replace(tzinfo=None)
    if roundtrip0 != naive:
        return "nonexistent"
    utc1 = naive.replace(tzinfo=zone, fold=1).astimezone(dt.UTC)
    return "ambiguous" if utc0 != utc1 else "unambiguous"


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


def _replace_field(
    row: tuple[str | bool | None, ...], index: int, value: str
) -> tuple[str | bool | None, ...]:
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

    def __init__(  # noqa: PLR0913 -- one keyword per genuinely distinct contract axis; see class docstring
        self,
        *,
        column_index: int,
        column_name: str,
        format: str | None = None,  # noqa: A002 -- matches ColumnContract.format (config/model.py), this phase's established contract vocabulary
        two_digit_year_pivot: int | None = None,
        spreadsheet_epoch: str | None = None,
        timezone: str | None = None,
        ambiguous_time_policy: str = "reject",
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
                supplied. Used both for the plain (non-timezone-aware)
                branch and, together with ``timezone``, for the
                DST-classified naive-local-time branch (fixture 55) -- a
                column with time components and no ``timezone`` declared
                and no ``%z``/``%Z`` in its own format is rejected outright
                (fixture 56: a naive timestamp must not inherit a
                neighboring row's offset, default to UTC, or default to
                the server's zone).
            two_digit_year_pivot: The last two-digit year (``0``-``99``)
                that resolves to the 2000s; required when ``format``
                contains ``%y`` (fixture 53: a missing pivot is a
                contract-authoring mistake, not a row-level problem).
            spreadsheet_epoch: ``"1900"`` or ``"1904"`` -- the spreadsheet
                epoch system a bare serial-integer column is declared under
                (fixture 54). Mutually exclusive with ``format``.
            timezone: An IANA zone name (e.g. ``"Europe/Warsaw"``). When
                set, ``format``-parsed values are treated as naive local
                times and classified via :func:`classify_naive_local`
                (fixture 55) rather than accepted as-is. Requires
                ``format`` to also be set -- a naive local time still needs
                a strptime format to parse in the first place.
            ambiguous_time_policy: ``"reject"`` (default), ``"earliest"``
                or ``"latest"`` -- how an ambiguous naive local time (a DST
                autumn-overlap hour) resolves when ``timezone`` is set.
                ``"reject"`` never silently takes the first fold (fixture
                55's own framing); ``"earliest"``/``"latest"`` resolve via
                ``fold=0``/``fold=1`` respectively.

        Raises:
            ValueError: ``format``/``spreadsheet_epoch`` are both supplied
                or neither is; ``spreadsheet_epoch`` names an unsupported
                epoch; ``format`` contains ``%y`` with no declared
                ``two_digit_year_pivot``; ``timezone`` is set with no
                ``format``; or ``ambiguous_time_policy`` names an
                unsupported policy.
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
        if ambiguous_time_policy not in _AMBIGUOUS_TIME_POLICIES:
            msg = (
                f"ambiguous_time_policy must be one of "
                f"{sorted(_AMBIGUOUS_TIME_POLICIES)}, got {ambiguous_time_policy!r}"
            )
            raise ValueError(msg)
        if timezone is not None and format is None:
            msg = (
                f"{type(self).__name__}(timezone=...) requires `format` to "
                "also be set -- a naive local time still needs a strptime "
                "format to parse in the first place"
            )
            raise ValueError(msg)
        if (
            timezone is not None
            and format is not None
            and any(directive in format for directive in _OFFSET_DIRECTIVES)
        ):
            msg = (
                f"{type(self).__name__}: `timezone` is for a column whose "
                f"values are naive local times (fixture 55); format {format!r} "
                "already carries its own %z/%Z offset, which would conflict "
                "with -- and be silently overwritten by -- the declared zone. "
                "Omit `timezone` and let the offset in the data decide "
                "(fixture 56 rows 1/2), or drop %z/%Z from `format` if every "
                "value is genuinely naive local time"
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
        self.timezone_name = timezone
        self.ambiguous_time_policy = ambiguous_time_policy
        self._zone = ZoneInfo(timezone) if timezone is not None else None
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
        kept: list[tuple[str | bool | None, ...]] = []
        rejected: list[RejectedRecord] = []
        for i, row in enumerate(chunk.rows):
            row_number = chunk.first_ordinal + i
            # RecordChunk.rows is declared tuple[str | bool | None, ...]
            # (plan 06-11 Task 1's platform-wide convention), but THIS
            # stage's declared column is never boolean-typed -- only a
            # NullTokenNormalizer for this same nullable column (run before
            # this stage, plan 06-16's wiring) can have already replaced its
            # field with the real Python None. cast() makes that
            # column-scoped narrowing explicit for mypy.
            raw_value = cast("str | None", row[self.column_index])
            if raw_value is None:
                kept.append(row)
                continue

            if self.spreadsheet_epoch is not None:
                outcome = self._parse_spreadsheet_serial(raw_value, row_number, row)
            elif self._zone is not None:
                outcome = self._parse_naive_local(raw_value, row_number, row, self._zone)
            else:
                outcome = self._parse_plain_format(raw_value, row_number, row)
            if isinstance(outcome, RejectedRecord):
                rejected.append(outcome)
            else:
                kept.append(outcome)

        metrics.increment("rows_rejected", len(rejected))
        metrics.increment("rows_kept", len(kept))
        return StageResult(chunk=chunk.replace(rows=tuple(kept)), rejected=rejected, findings=[])

    def _parse_plain_format(
        self, raw_value: str, row_number: int, row: tuple[str | bool | None, ...]
    ) -> _RowOutcome:
        """Parse ``raw_value`` under ``self.format``, returning the updated row or a rejection.

        No ``timezone`` is declared for this column (the ``self._zone is not
        None`` case dispatches to :meth:`_parse_naive_local` instead). If
        ``self.format`` includes a ``%z``/``%Z`` offset directive,
        ``strptime`` itself produces an aware datetime and this method
        resolves it to UTC (fixture 56 rows 1/2). If it has time components
        but no offset directive, the result is unavoidably naive with no
        way to resolve it to an instant -- rejected (fixture 56 row 3: a
        naive timestamp must not inherit a neighboring row's offset,
        default to UTC, or default to the server's zone).
        """
        try:
            # naive on purpose here when self.format has no %z/%Z: this
            # branch's whole job is to preserve whatever
            # naive-vs-aware-ness strptime itself produces from the
            # declared format, never to invent an offset.
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

        if self._has_time_component and parsed.tzinfo is None:
            return RejectedRecord(
                source_row_number=row_number,
                error_type="naive-timestamp-without-a-declared-zone",
                error_message=(
                    f"{raw_value!r} has no UTC offset and no `timezone` was "
                    "declared for this column -- a naive timestamp must not "
                    "inherit a neighboring row's offset, default to UTC, or "
                    "default to the server's zone"
                ),
                raw_line=raw_value,
                error_column=self.column_name,
            )

        if parsed.tzinfo is not None:
            iso_value = parsed.astimezone(dt.UTC).isoformat()
        else:
            iso_value = parsed.date().isoformat()
        return _replace_field(row, self.column_index, iso_value)

    def _parse_naive_local(
        self, raw_value: str, row_number: int, row: tuple[str | bool | None, ...], zone: ZoneInfo
    ) -> _RowOutcome:
        """Parse ``raw_value`` as a naive local time and classify/resolve it against ``zone``.

        Fixture 55: a nonexistent local time (DST spring-forward gap) is
        rejected; an ambiguous local time (DST autumn-overlap) is rejected
        by default (``ambiguous_time_policy="reject"``) or resolved via
        ``fold=0``/``fold=1`` under ``"earliest"``/``"latest"``; an
        unambiguous local time resolves directly. The result is always the
        UTC ISO-8601 instant, never the local wall-clock string.
        """
        try:
            # naive on purpose: this is the whole point of this branch --
            # classify_naive_local decides the zone attachment explicitly,
            # never strptime guessing one.
            naive = dt.datetime.strptime(raw_value, cast("str", self.format))  # noqa: DTZ007
        except ValueError as exc:
            return RejectedRecord(
                source_row_number=row_number,
                error_type="invalid-calendar-date",
                error_message=f"{raw_value!r} does not match format {self.format!r}: {exc}",
                raw_line=raw_value,
                error_column=self.column_name,
            )
        if self._has_two_digit_year:
            naive = _apply_two_digit_year_pivot(naive, cast("int", self.two_digit_year_pivot))

        classification = classify_naive_local(naive, zone)
        if classification == "nonexistent":
            return RejectedRecord(
                source_row_number=row_number,
                error_type="nonexistent-local-time",
                error_message=(
                    f"{raw_value!r} does not exist in {self.timezone_name} "
                    "(falls in a daylight-saving spring-forward gap)"
                ),
                raw_line=raw_value,
                error_column=self.column_name,
            )
        if classification == "ambiguous" and self.ambiguous_time_policy == "reject":
            return RejectedRecord(
                source_row_number=row_number,
                error_type="ambiguous-local-time-requires-a-declared-fold-policy",
                error_message=(
                    f"{raw_value!r} is ambiguous in {self.timezone_name} "
                    "(falls in a daylight-saving autumn-overlap hour); "
                    "declare csv.ambiguous_time_policy to resolve it"
                ),
                raw_line=raw_value,
                error_column=self.column_name,
            )

        fold = 1 if classification == "ambiguous" and self.ambiguous_time_policy == "latest" else 0
        resolved_utc = naive.replace(tzinfo=zone, fold=fold).astimezone(dt.UTC)
        return _replace_field(row, self.column_index, resolved_utc.isoformat())

    def _parse_spreadsheet_serial(
        self, raw_value: str, row_number: int, row: tuple[str | bool | None, ...]
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
