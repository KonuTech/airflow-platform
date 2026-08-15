"""Conservative, bootstrap-only column type inference (SCHEMA-01).

``infer_column_type`` looks at a column's sampled raw string values and
suggests one of ``ColumnContract``'s closed type set --
``string``/``integer``/``decimal``/``date``/``timestamp``/``boolean`` --
biased hard toward ``string`` whenever a candidate type could be wrong in a
way that loses information. ``001234`` stays a string even though every
character is a digit, because coercing it to an integer would silently strip
the leading zeros that make it a valid identifier.

This module's output is a SUGGESTION, never an authority. The design is
locked (06-RESEARCH.md's own words): type inference is "computed in-pod as a
bootstrap aid; never applied automatically to the load path (locked: contract
wins)". Nothing in this module, or anywhere downstream of it, may treat a
``TypeInference`` as sufficient reason to type or load a column -- a real
``ColumnContract`` (SCHEMA-02), written by a human, is the only thing that
does that. This module exists to help a human author that contract, and to
give a future schema-evolution classifier a fallback signal for a column the
contract does not yet declare.

Never guesses a candidate date format from the sampled data itself.
``candidate_date_formats`` are always supplied by the caller -- a
human-declared candidate, or nothing at all -- and this module only ever
confirms whether a GIVEN format parses every sampled value. Guessing an
ambiguous format from the data is exactly the load-path behavior
CLAUDE.md's Python ETL Library section forbids (STACK.md Section F); this
module carries no exception for it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Sequence

# The closed set this function recognizes as boolean-shaped, compared
# case-insensitively after stripping whitespace. Deliberately English-only
# and deliberately whole-words -- single-letter tokens like "O"/"N"/"Y" are
# excluded because they collide across locales (corpus fixture
# 60_boolean_localized.csv: "O" is French "oui"/true, but reads like
# zero/off in English). A dataset needing localized boolean tokens declares
# them explicitly via NormalizationConfig.boolean_true_tokens/
# boolean_false_tokens (CSV-10, D-14) -- this function has no dataset
# context and must not guess.
_BOOLEAN_CLOSED_SET: Final[frozenset[str]] = frozenset({"true", "false", "yes", "no", "0", "1"})

# At least one of these must be present for a boolean suggestion to fire.
# D-14/CSV-10: "1/0 must never become boolean absent evidence" -- a sample
# of only "0"/"1" is exactly as consistent with a genuine small-integer
# column as with a boolean flag, so "0"/"1" alone is not itself evidence;
# one of these four unambiguous words is.
_BOOLEAN_DISTINCTIVE_TOKENS: Final[frozenset[str]] = frozenset({"true", "false", "yes", "no"})

# strptime directives that carry a time-of-day component. A candidate format
# containing any of these infers "timestamp"; every other candidate format
# that parses the whole sample infers "date".
_TIME_DIRECTIVES: Final[tuple[str, ...]] = ("%H", "%I", "%M", "%S", "%f", "%z", "%Z", "%p", "%X")

# A value must look like a COMPLETE scientific-notation number -- not merely
# contain a substring like "e5" (e.g. "Room2E5") -- to count as the red flag
# corpus fixture 50_excel_scientific_notation_ids.csv names
# ("scientific-notation-identifier-unrecoverable"). Matched with fullmatch,
# not search, so a false positive on ordinary text cannot occur.
_SCIENTIFIC_NOTATION_PATTERN: Final = re.compile(r"-?\d+(?:\.\d+)?[eE][+-]?\d+")

# The shortest digit-sequence length a leading zero can be redundant at -- a
# bare "0" (length 1) is never flagged. SCHEMA-01's own canonical example is
# multi-digit: "001234".
_MIN_LEADING_ZERO_LENGTH: Final[int] = 2


@dataclass(frozen=True, slots=True)
class TypeInference:
    """One column's suggested type, with the evidence behind it.

    Never authoritative (module docstring) -- a suggestion only, for a human
    authoring a ``ColumnContract`` or a schema-evolution classifier's
    fallback signal.

    Attributes:
        suggested_type: One of ``ColumnContract``'s closed type set --
            ``"string"``, ``"integer"``, ``"decimal"``, ``"date"``,
            ``"timestamp"``, or ``"boolean"``.
        confidence_evidence: A short, human-readable reason for the
            suggestion, e.g. ``"all values digit-only but 2 of 3 have
            leading zeros"``.
        red_flags: Named reasons inference declined a numeric/date type in
            favor of ``"string"``, e.g. ``("leading-zero",)``. Empty when no
            named red flag applied -- including every non-``"string"``
            suggestion, and a ``"string"`` suggestion reached because
            nothing else matched rather than because of one specific
            declined type.
    """

    suggested_type: str
    confidence_evidence: str
    red_flags: tuple[str, ...]


def infer_column_type(
    values: Sequence[str], *, candidate_date_formats: Sequence[str] = ()
) -> TypeInference:
    """Suggest a conservative type for one column's sampled raw values.

    Tries, in order, boolean, then date/timestamp (only against
    ``candidate_date_formats``, never invented), then integer, then decimal
    -- falling back to ``"string"`` the moment a check is ambiguous, mixed,
    or shows a red flag. See the module docstring for why this is a
    suggestion, never an authority.

    Args:
        values: The column's sampled raw string values, in any order.
        candidate_date_formats: ``strptime``-style formats to try, in the
            given order. Never derived or guessed here -- the default empty
            tuple skips the date/timestamp check entirely.

    Returns:
        The suggested type plus the evidence and any red flags that
        produced it.
    """
    sample = tuple(values)
    if not sample:
        return TypeInference(
            suggested_type="string",
            confidence_evidence="no sampled values to infer a type from",
            red_flags=("empty-sample",),
        )

    boolean = _infer_boolean(sample)
    if boolean is not None:
        return boolean

    if candidate_date_formats:
        temporal = _infer_temporal(sample, candidate_date_formats)
        if temporal is not None:
            return temporal

    if any(_looks_like_scientific_notation(value) for value in sample):
        return TypeInference(
            suggested_type="string",
            confidence_evidence=(
                "at least one value renders in scientific notation, a red flag for a "
                "damaged numeric identifier rather than evidence of a numeric type"
            ),
            red_flags=("scientific-notation",),
        )

    integer = _infer_integer(sample)
    if integer is not None:
        return integer

    return _infer_decimal_or_string(sample)


def _infer_boolean(sample: tuple[str, ...]) -> TypeInference | None:
    """Suggest "boolean" only when the sample is closed-set AND carries a distinctive token."""
    lowered = tuple(value.strip().lower() for value in sample)
    if not all(value in _BOOLEAN_CLOSED_SET for value in lowered):
        return None
    if not any(value in _BOOLEAN_DISTINCTIVE_TOKENS for value in lowered):
        return None
    observed = sorted(set(lowered))
    return TypeInference(
        suggested_type="boolean",
        confidence_evidence=(
            f"every value is in {sorted(_BOOLEAN_CLOSED_SET)} (observed: {observed})"
        ),
        red_flags=(),
    )


def _infer_temporal(
    sample: tuple[str, ...], candidate_date_formats: Sequence[str]
) -> TypeInference | None:
    """Confirm the first candidate format that parses every sampled value, or decline."""
    for date_format in candidate_date_formats:
        if _every_value_parses(sample, date_format):
            suggested_type = "timestamp" if _has_time_component(date_format) else "date"
            return TypeInference(
                suggested_type=suggested_type,
                confidence_evidence=(
                    f"every value parses under the given candidate format {date_format!r}"
                ),
                red_flags=(),
            )
    return None


def _every_value_parses(sample: tuple[str, ...], date_format: str) -> bool:
    """Return whether every sampled value parses under one strptime format."""
    for value in sample:
        try:
            # Validates format fit only -- no datetime is kept or used naive.
            datetime.strptime(value, date_format)  # noqa: DTZ007
        except ValueError:
            return False
    return True


def _has_time_component(date_format: str) -> bool:
    """Return whether a strptime format string carries a time-of-day directive."""
    return any(directive in date_format for directive in _TIME_DIRECTIVES)


def _looks_like_scientific_notation(value: str) -> bool:
    """Return whether a value is, as a whole, shaped like scientific notation."""
    return _SCIENTIFIC_NOTATION_PATTERN.fullmatch(value) is not None


def _infer_integer(sample: tuple[str, ...]) -> TypeInference | None:
    """Suggest "integer" when digit-only with no leading zero; "string" when digit-only but flagged.

    Returns ``None`` (declining to answer at all) when at least one sampled
    value is not digit-only, so the caller moves on to the decimal check.
    """
    if not all(_is_digit_only(value) for value in sample):
        return None

    leading_zero_values = tuple(value for value in sample if _has_leading_zero(value))
    if leading_zero_values:
        return TypeInference(
            suggested_type="string",
            confidence_evidence=(
                f"all values digit-only but {len(leading_zero_values)} of {len(sample)} "
                "have leading zeros"
            ),
            red_flags=("leading-zero",),
        )
    return TypeInference(
        suggested_type="integer",
        confidence_evidence="every value is digit-only with no leading zero",
        red_flags=(),
    )


def _is_digit_only(value: str) -> bool:
    """Return whether a value is an optionally-negative run of decimal digits.

    ``isdecimal()``, not ``isdigit()``: ``isdigit()`` also accepts
    characters such as superscript digits that ``int()`` cannot actually
    consume, which would let this function call a value digit-only when it
    is not.
    """
    digits = value.removeprefix("-")
    return digits.isdecimal()


def _has_leading_zero(value: str) -> bool:
    """Return whether a digit-only value has a redundant leading zero."""
    digits = value.removeprefix("-")
    return len(digits) >= _MIN_LEADING_ZERO_LENGTH and digits[0] == "0"


def _infer_decimal_or_string(sample: tuple[str, ...]) -> TypeInference:
    """Suggest "decimal" only when every sampled value parses; otherwise "string"."""
    parses = tuple(_parses_as_decimal(value) for value in sample)
    if all(parses):
        return TypeInference(
            suggested_type="decimal",
            confidence_evidence="every value parses via decimal.Decimal",
            red_flags=(),
        )
    if any(parses):
        failing = len(parses) - sum(parses)
        return TypeInference(
            suggested_type="string",
            confidence_evidence=(
                f"{failing} of {len(parses)} values do not parse via decimal.Decimal; "
                "inference never silently drops the values it could not fit"
            ),
            red_flags=("mixed-parseable",),
        )
    return TypeInference(
        suggested_type="string",
        confidence_evidence=(
            "no candidate type (boolean, date, integer, decimal) matched every value"
        ),
        red_flags=(),
    )


def _parses_as_decimal(value: str) -> bool:
    """Return whether a value parses via ``decimal.Decimal`` without raising."""
    try:
        Decimal(value)
    except InvalidOperation:
        return False
    return True


def infer_schema(
    header: Sequence[str], sample_rows: Sequence[tuple[str, ...]]
) -> list[TypeInference]:
    """Infer one ``TypeInference`` per header column, values gathered positionally.

    Args:
        header: Column names, in file order.
        sample_rows: Sampled rows. A row shorter than ``header`` simply
            contributes no value for the columns past its own length --
            never an ``IndexError`` -- since a ragged sample row is exactly
            the kind of messy real-world input this bootstrap helper must
            survive without crashing.

    Returns:
        One ``TypeInference`` per ``header`` column, in the same order.
    """
    return [
        infer_column_type([row[index] for row in sample_rows if index < len(row)])
        for index in range(len(header))
    ]


def suggest_column_contracts(
    header: Sequence[str], sample_rows: Sequence[tuple[str, ...]]
) -> list[dict[str, object]]:
    """Shape ``infer_schema``'s output into draft ``ColumnContract`` dicts.

    A human-readable STARTING POINT for hand-authoring a dataset's
    ``columns:`` YAML block -- never wired into any automated pipeline
    (module docstring). ``nullable`` and ``required`` are always suggested
    ``True``: a sample alone carries no evidence of either, and ``True`` is
    the conservative choice for both (a column absent or empty in a
    training sample says nothing about whether it always will be).

    Args:
        header: Column names, in file order.
        sample_rows: Sampled rows, values gathered positionally per column.

    Returns:
        One dict per ``header`` column, in order, with keys matching
        ``ColumnContract``'s field names (``name``, ``type``, ``nullable``,
        ``required``) -- constructing ``ColumnContract(**suggestion)`` for
        any entry never raises.
    """
    inferences = infer_schema(header, sample_rows)
    return [
        {"name": name, "type": inference.suggested_type, "nullable": True, "required": True}
        for name, inference in zip(header, inferences, strict=True)
    ]
