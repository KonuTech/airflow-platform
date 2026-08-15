"""``NumericNormalizer`` -- CSV-10's numeric half: locale-aware ``Decimal`` parsing.

Handles the accounting-convention negative styles (parentheses, trailing
minus, leading minus), currency-symbol and percent stripping, and two
UNRECOVERABLE-damage rejections that must never be silently coerced:
spreadsheet scientific-notation identifier truncation and fixed-width
leading-zero stripping. Every parsed value is an exact ``decimal.Decimal``,
never a binary ``float`` -- this module never converts a parsed value
through ``float``.

Built against corpus fixtures 20/21 (the comma/point-locale pair -- the
SAME four quantities, denoted under opposite decimal-separator
conventions), 50 ("THE MOST IMPORTANT DECLARATION IN THIS PLAN" --
``1.23457E+14`` has irreversibly lost nine significant digits and must be
rejected, never expanded and treated as the original), 51 (a postcode
narrower than its declared fixed width has irreversibly lost its leading
zero(s) and must be rejected, never silently re-padded), 57 (both
accounting negative conventions plus a positive control, in one file, so a
naive "strip every non-numeric character" implementation's sign-flip bug
cannot hide), 58 (currency symbols, a grouping separator that is NOT the
decimal point, and percent-as-fraction handling), and 59 (numeric null
sentinels -- a contract declaration, never a universal rule; ``-1`` means
absent only for the columns that declare it).

Mirrors ``dataplat.pipeline.engine.RaggedRowGuard``'s ``StreamingStage``
shape and constructor-injection convention exactly: every locale/contract
parameter arrives via the constructor, already resolved to this one column
by whatever assembles the pipeline (never read from a global or re-derived
internally).
"""

from __future__ import annotations

import re
from decimal import Context, Decimal, DivisionByZero, InvalidOperation, localcontext
from typing import TYPE_CHECKING, Final, cast

from dataplat.models.record import RejectedRecord, StageResult
from dataplat.observability import metrics
from dataplat.pipeline.protocol import StreamingStage

if TYPE_CHECKING:
    from dataplat.models.record import RecordChunk
    from dataplat.pipeline.protocol import PipelineContext

# Matches an optionally-signed decimal mantissa in exponent notation, e.g.
# "1.23457E+14" or "-9.8E-3" -- deliberately NOT a general "looks numeric"
# pattern: only the scientific-notation shape corpus fixture 50 pins as
# unrecoverable spreadsheet damage.
_SCIENTIFIC_NOTATION_RE: Final = re.compile(r"^-?\d(\.\d+)?[eE][+-]?\d+$")

# The percent divisor, constructed once at import time. A `Decimal` object
# is not itself bound to any context -- only the arithmetic operation that
# consumes it is -- so this is safe to reuse across every `localcontext()`
# block `_parse_decimal` opens, regardless of which context is ambient when
# this module is imported.
_PERCENT_DIVISOR: Final = Decimal(100)

# D-01's hardcoded comma dialect, matching `RaggedRowGuard`'s own default
# `field_delimiter` (`pipeline/engine.py`) -- reconstructs a rejected row's
# text for `RejectedRecord.raw_line` from already-parsed fields. This stage
# takes no `field_delimiter` constructor parameter (unlike `RaggedRowGuard`)
# because this plan's own constructor-parameter list does not include one;
# a future caller that knows the real detected delimiter can add one the
# same way `RaggedRowGuard.__init__` does.
_RAW_LINE_DELIMITER: Final = ","


def _replace_field(row: tuple[str, ...], index: int, value: str | None) -> tuple[str, ...]:
    """Return ``row`` with the field at ``index`` replaced by ``value``.

    ``value`` may be ``None`` when a contract-declared null sentinel
    (corpus fixture 59) matches. ``RecordChunk.rows`` is typed
    ``tuple[tuple[str, ...], ...]`` within this plan's own file scope
    (``packages/dataplat/src/dataplat/normalize/numeric.py`` and its test
    only); plan 06-11 Task 1 widens that element type to
    ``tuple[str | None, ...]`` in ``dataplat.models.record`` itself -- the
    platform-wide ``None`` convention this stage participates in without
    owning. The ``# type: ignore`` below documents that expected, temporary
    mismatch rather than an oversight; it is expected to become unnecessary
    once plan 06-11 merges.

    Args:
        row: The row to copy. Never mutated.
        index: The 0-based position of the field to replace.
        value: The replacement value, or ``None`` to mark the field absent.

    Returns:
        A new row tuple with only the field at ``index`` changed.
    """
    fields: list[str | None] = list(row)
    fields[index] = value
    return tuple(fields)  # type: ignore[arg-type]


class NumericNormalizer(StreamingStage):
    """Locale-aware ``Decimal`` normalization for one numeric/identifier column.

    A malformed or unrecoverably-damaged value becomes a ``RejectedRecord``
    (QUAL-03's errors-as-values mechanism) -- this stage never raises for a
    row-level problem. A surviving field's value is replaced with the
    canonical ``str(decimal.Decimal(...))`` form: never round-tripped
    through ``float``.
    """

    name = "numeric_normalizer"

    # `__init__` takes ten keyword-only parameters, one flat slice per
    # `NormalizationConfig`/`ColumnContract` field this stage's single
    # column needs (`RaggedRowGuard.__init__`'s constructor-injection
    # precedent, `pipeline/engine.py` lines 47-63) -- deliberately never a
    # config object, which would require `dataplat.normalize` to import
    # `dataplat.config.model` or invent a second, parallel shape. This
    # plan's own <interfaces>/<acceptance_criteria> pin this exact flat
    # parameter list.
    def __init__(  # noqa: PLR0913
        self,
        *,
        column_index: int,
        column_name: str,
        decimal_separator: str,
        thousands_separator: str | None,
        currency_symbols: tuple[str, ...],
        percent_as_fraction: bool,
        negative_style: str,
        reject_scientific_notation: bool = False,
        fixed_width: int | None = None,
        null_sentinels: tuple[str, ...] = (),
    ) -> None:
        """Configure this stage for one column of one dataset's locale profile.

        Every parameter is already resolved to this one column by whatever
        assembles the pipeline from the dataset's ``NormalizationConfig``/
        ``ColumnContract`` (``dataplat.config.model``) -- this class takes a
        flat, single-column slice of each, never a whole dict keyed by
        column name, matching every other constructor parameter's
        single-responsibility shape (``RaggedRowGuard.__init__`` precedent).

        Args:
            column_index: The 0-based position of this column within a row.
            column_name: The column's name, used in rejection messages.
            decimal_separator: Character separating the integer and
                fractional parts of a number, e.g. ``","`` or ``"."``.
            thousands_separator: Character grouping digits in a large
                number, or ``None`` when this column's numbers carry no
                grouping separator. Removed from a value BEFORE
                ``decimal_separator`` substitution, so a locale where the
                two differ never collides.
            currency_symbols: Currency symbols/codes stripped from a value
                before parsing, e.g. ``("EUR", "zl")``.
            percent_as_fraction: Whether a value carrying a trailing ``%``
                is stored as a fraction of 1 (``True``) or as the literal
                number before the sign (``False``).
            negative_style: How a negative value is rendered -- one of
                ``"leading-minus"``, ``"trailing-minus"``, ``"parentheses"``.
            reject_scientific_notation: Whether a value rendered in
                scientific notation (e.g. a spreadsheet re-export of a long
                numeric identifier) is rejected as unrecoverable rather
                than expanded and parsed (corpus fixture 50). Defaults to
                ``False``.
            fixed_width: The declared character width a value must reach
                or be rejected as unrecoverable (corpus fixture 51), or
                ``None`` when this column has no fixed width. Defaults to
                ``None``.
            null_sentinels: The per-column slice of
                ``NormalizationConfig.null_sentinels`` for this column --
                raw values that normalize to absent rather than a parsed
                ``Decimal`` (corpus fixture 59). Matched by EXACT string
                equality only, never substring/prefix. Defaults to ``()``.
        """
        self._column_index = column_index
        self._column_name = column_name
        self._decimal_separator = decimal_separator
        self._thousands_separator = thousands_separator
        self._currency_symbols = currency_symbols
        self._percent_as_fraction = percent_as_fraction
        self._negative_style = negative_style
        self._reject_scientific_notation = reject_scientific_notation
        self._fixed_width = fixed_width
        self._null_sentinels = null_sentinels
        # A Context local to this stage instance, never the global/
        # thread-local default (STACK.md "Numerics" section) -- scoped via
        # `localcontext()` inside `_parse_decimal` for exactly one field's
        # parse (and its percent division, if any), then restored. prec=28
        # matches `decimal`'s own `DefaultContext` precision.
        self._decimal_context = Context(prec=28, traps=[InvalidOperation, DivisionByZero])

    def apply(self, ctx: PipelineContext, chunk: RecordChunk) -> StageResult:  # noqa: ARG002
        """Normalize this stage's declared column across every row in ``chunk``.

        Never raises for a row-level problem (QUAL-03): a value this stage
        cannot parse, or judges unrecoverably damaged, becomes a
        ``RejectedRecord`` in the returned ``StageResult`` instead.

        Args:
            ctx: The current pipeline context. Unused: this stage's decision
                depends only on ``chunk`` and its own constructor
                configuration, and the parameter exists to satisfy
                ``StreamingStage``.
            chunk: The chunk to normalize.

        Returns:
            A ``StageResult`` whose ``chunk`` holds every row that survived
            (with this stage's column replaced by its canonical, exact
            ``Decimal`` string form, or ``None`` when the value is a
            declared null sentinel) and whose ``rejected`` holds one
            ``RejectedRecord`` per row this stage could not accept.
        """
        kept: list[tuple[str, ...]] = []
        rejected: list[RejectedRecord] = []

        for i, row in enumerate(chunk.rows):
            source_row_number = chunk.first_ordinal + i

            # `row`'s element type is nominally `str` (see `_replace_field`'s
            # docstring for why), but at runtime this field may already be
            # `None` -- normalized to absent upstream by a
            # `NullTokenNormalizer` for this same nullable column (plan
            # 06-11 Task 1's platform-wide `None` convention; plan 06-16
            # orders that stage before this one). Reading through `object`
            # keeps this check meaningful, and `# type: ignore`-free, under
            # both the current and the post-06-11-merge element type.
            cell: object = row[self._column_index]
            if cell is None:
                kept.append(row)
                continue
            raw_value = cast("str", cell)

            # (1) contract-declared null sentinel -- EXACT match only,
            # never a substring/prefix match (fixture 59: "-1.50" must not
            # match the declared sentinel "-1"; "0" is a real value unless
            # "0" itself is declared).
            if raw_value in self._null_sentinels:
                kept.append(_replace_field(row, self._column_index, None))
                continue

            # (2) UNRECOVERABLE: spreadsheet scientific-notation identifier
            # truncation (fixture 50). Rejected before any parse is
            # attempted -- the damaged value is never expanded and treated
            # as the original.
            if self._reject_scientific_notation and _SCIENTIFIC_NOTATION_RE.match(raw_value):
                rejected.append(
                    RejectedRecord(
                        source_row_number=source_row_number,
                        error_type="scientific-notation-identifier-unrecoverable",
                        error_message=(
                            f"{self._column_name}={raw_value!r} is rendered in scientific "
                            "notation; expanding it would produce a value with more "
                            "significant digits than the file actually carries, which "
                            "is a different, wrong identifier -- never coerced"
                        ),
                        raw_line=_RAW_LINE_DELIMITER.join(row),
                        error_column=self._column_name,
                    )
                )
                continue

            # (3) UNRECOVERABLE: fixed-width identifier narrower than its
            # declared width (fixture 51). Left-padding would manufacture a
            # digit the file does not contain -- rejected, never padded.
            if self._fixed_width is not None and len(raw_value) < self._fixed_width:
                rejected.append(
                    RejectedRecord(
                        source_row_number=source_row_number,
                        error_type="fixed-width-identifier-below-declared-width",
                        error_message=(
                            f"{self._column_name}={raw_value!r} has width {len(raw_value)}, "
                            f"below the declared width {self._fixed_width}; the missing "
                            "character(s) are not recoverable from the file"
                        ),
                        raw_line=_RAW_LINE_DELIMITER.join(row),
                        error_column=self._column_name,
                    )
                )
                continue

            # (4) normal path: locale-aware, exact Decimal parsing.
            parsed = self._parse_decimal(raw_value)
            if parsed is None:
                rejected.append(
                    RejectedRecord(
                        source_row_number=source_row_number,
                        error_type="invalid-numeric-value",
                        error_message=(
                            f"{self._column_name}={raw_value!r} is not a valid numeric "
                            "value under this column's declared locale profile"
                        ),
                        raw_line=_RAW_LINE_DELIMITER.join(row),
                        error_column=self._column_name,
                    )
                )
                continue

            kept.append(_replace_field(row, self._column_index, str(parsed)))

        metrics.increment("rows_rejected", len(rejected))
        metrics.increment("rows_kept", len(kept))
        return StageResult(chunk=chunk.replace(rows=tuple(kept)), rejected=rejected, findings=[])

    def _strip_negative_style(self, value: str) -> str:
        """Rewrite ``value``'s declared negative convention into leading-minus form.

        A naive "strip every non-numeric character" implementation would
        turn ``"(123.45)"`` and ``"123.45-"`` into ``"123.45"`` -- a silent
        SIGN FLIP, not a parse error (corpus fixture 57). This method never
        does that: parentheses/trailing-minus are rewritten to an explicit
        leading ``-``, and a value carrying none of this stage's declared
        negative marker is returned unchanged (covers both a genuinely
        positive value and, for ``"leading-minus"``, every value -- a
        leading ``-`` already survives untouched all the way to
        ``Decimal()``, which parses it correctly with no rewriting needed).

        Args:
            value: The value to rewrite, after currency/whitespace/percent
                stripping and before thousands/decimal-separator
                substitution.

        Returns:
            ``value``, rewritten to a leading-minus (or unsigned) form.
        """
        if self._negative_style == "parentheses":
            if value.startswith("(") and value.endswith(")"):
                return "-" + value[1:-1]
            return value
        if self._negative_style == "trailing-minus":
            if value.endswith("-"):
                return "-" + value[:-1]
            return value
        return value  # "leading-minus": already correct, nothing to rewrite

    def _parse_decimal(self, raw_value: str) -> Decimal | None:
        """Locale-normalize ``raw_value`` and parse it as an exact ``Decimal``.

        Never round-trips through ``float``. Strips currency symbols and
        surrounding whitespace, rewrites the declared negative style,
        strips a trailing ``%`` (dividing by 100 afterward when
        ``percent_as_fraction``), then substitutes ``thousands_separator``
        (removed) and ``decimal_separator`` (replaced with ``"."``) --
        thousands BEFORE decimal, so a locale where the two differ (fixture
        58: ``"."`` groups, ``","`` is the decimal point) never collides.

        Args:
            raw_value: The field value, already cleared of the null-
                sentinel/scientific-notation/fixed-width guards.

        Returns:
            The parsed ``Decimal``, or ``None`` when ``raw_value`` cannot
            be parsed under this stage's declared locale profile --
            ``decimal.InvalidOperation``/``DivisionByZero`` are caught here
            and never escape this method.
        """
        cleaned = raw_value
        for symbol in self._currency_symbols:
            cleaned = cleaned.replace(symbol, "")
        cleaned = cleaned.strip()

        percent_present = cleaned.endswith("%")
        if percent_present:
            cleaned = cleaned[:-1].strip()

        cleaned = self._strip_negative_style(cleaned)

        if self._thousands_separator is not None:
            cleaned = cleaned.replace(self._thousands_separator, "")
        cleaned = cleaned.replace(self._decimal_separator, ".")

        try:
            with localcontext(self._decimal_context):
                parsed = Decimal(cleaned)
                if percent_present and self._percent_as_fraction:
                    parsed = parsed / _PERCENT_DIVISOR
        except (InvalidOperation, DivisionByZero):
            return None
        return parsed
