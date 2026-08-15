"""CSV-04/05/06 dialect detection: a ``clevercsv`` wrapper with the single-column guard.

``clevercsv.Detector().detect(sample)`` is the verified choice over both a
hand-rolled delimiter-frequency heuristic (fails fixture
``37_delimiter_frequency_differs_header_vs_body.csv``, where the header row's
own delimiter frequency disagrees with the body's) and stdlib ``csv.Sniffer``
(documented to raise ``_csv.Error: Could not determine delimiter`` on a
genuinely single-column file, and to misidentify delimiters that appear
inside quoted fields -- STACK.md Section F). ``sample`` here is always
already-decoded text: dialect detection runs strictly after the encoding
detector's output and strictly before numeric normalization (T-06-10's
mitigation -- a crafted file whose delimiter equals a decimal separator
cannot corrupt an amount column, because the delimiter is fixed before any
column is interpreted as a number).

**Pitfall 1** (06-RESEARCH.md Common Pitfalls Number 1, reproduced live
against fixture ``38_single_column_no_delimiter.csv``): ``Detector().detect()``
returns ``SimpleDialect('', '', '')`` -- not ``None``, not an exception -- for
a genuinely single-column sample. Calling ``.to_csv_dialect()`` on that result
raises ``_csv.Error: "delimiter" must be a 1-character string``, because
Python's ``csv`` module requires a real 1-character delimiter even when
nothing will ever match it. The guard is ``dialect.delimiter == ""``, checked
immediately after ``detect()`` returns, before ``.to_csv_dialect()`` is ever
called.

**A second, related gap found live in this plan (not documented anywhere in
06-RESEARCH.md), against fixture ``36_doubled_vs_backslash_escape.csv``:**
``Detector().detect()`` can also return ``None`` outright -- a third outcome
distinct from both a clean detection and Pitfall 1's degenerate
``SimpleDialect('', '', '')``. This happens when the consistency-measure
detector cannot converge on any candidate dialect with sufficient confidence.
Verified live: fixture 36's content is deliberately, structurally
inconsistent in its quoting (one row uses RFC-4180 doubled-quote escaping,
another contains a lone backslash-quote sequence that is ambiguous between
the doubled-quote and backslash-escape conventions -- exactly the
"convention must be a declared contract parameter, not a detector's guess"
case the fixture itself documents), so a consistency measure failing to
converge on it is the *correct* outcome, not a bug in ``clevercsv``. Calling
``.to_csv_dialect()`` on ``None`` raises ``AttributeError`` rather than
producing a clean decline, so this module treats ``None`` exactly like
Pitfall 1's empty-string case: a declined detection, never a crash and never
a guessed delimiter.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from typing import TYPE_CHECKING

import clevercsv

from dataplat.errors import CsvDialectDetectionError

if TYPE_CHECKING:
    from clevercsv.dialect import SimpleDialect

# RFC 4180's own default, and the same default `dataplat.config.model
# .CsvParsingConfig.quotechar` uses -- the sensible fallback when no real
# quoting convention was observed in the sample (declined) or when a
# contract override supplies only a delimiter, not a quoting convention.
_DEFAULT_QUOTECHAR = '"'


@dataclass(frozen=True, slots=True)
class DialectDetection:
    """The outcome of dialect detection over one already-decoded sample.

    Attributes:
        delimiter: The detected or contract-supplied field delimiter, or
            ``None`` when declined -- the caller must fall back to a
            contract value or treat the file as genuinely single-column.
        quotechar: The detected or contract-supplied quote character.
            Always a real character (``'"'`` by default), never empty --
            even when ``declined`` is ``True``, since a declined detection
            still needs a well-formed placeholder a caller can inspect
            without a ``None`` check on this field specifically.
        declined: ``True`` when no usable delimiter could be determined at
            all -- CSV-05's genuinely-single-column or genuinely-ambiguous
            case, never a guess. Always paired with ``delimiter is None``.
    """

    delimiter: str | None
    quotechar: str
    declined: bool


def detect_dialect(sample: str, *, contract_delimiter: str | None = None) -> DialectDetection:
    """Detect a CSV sample's dialect, or use a contract-declared override.

    Args:
        sample: Already-decoded text -- the encoding detector's output, per
            the architecture diagram in 06-RESEARCH.md. Never raw bytes.
        contract_delimiter: A dataset contract's declared delimiter
            (``dataplat.config.model.CsvParsingConfig.delimiter``), when the
            caller wants to skip detection entirely (CSV-05). When
            provided, ``sample`` is not inspected at all -- the override is
            authoritative regardless of what the sample would otherwise
            detect.

    Returns:
        The detected or contract-supplied dialect. ``declined`` is ``True``
        only when ``contract_delimiter`` was not supplied AND detection
        could not determine a usable delimiter -- see the module docstring
        for the two distinct ways that happens (Pitfall 1's degenerate
        ``SimpleDialect('', '', '')`` and a live-verified ``None`` return).
    """
    if contract_delimiter is not None:
        return DialectDetection(
            delimiter=contract_delimiter,
            quotechar=_DEFAULT_QUOTECHAR,
            declined=False,
        )

    detected: SimpleDialect | None = clevercsv.Detector().detect(sample)

    # Two distinct "nothing usable" outcomes, one guard: Pitfall 1's
    # degenerate empty-delimiter dialect, and the live-verified `None`
    # return (module docstring). Checked BEFORE `.to_csv_dialect()` is ever
    # called -- that call is what actually raises for either case.
    if detected is None or detected.delimiter == "":
        return DialectDetection(delimiter=None, quotechar=_DEFAULT_QUOTECHAR, declined=True)

    csv_dialect = detected.to_csv_dialect()
    # `to_csv_dialect()`'s implementation always sets a real character here
    # (`'"'` when no quoting convention was observed -- see the source
    # excerpt in this module's docstring), but typeshed's `csv.Dialect`
    # stub declares `quotechar: str | None`, so this stays defensive for
    # mypy's benefit rather than assuming the implementation detail holds
    # forever.
    quotechar = csv_dialect.quotechar if csv_dialect.quotechar is not None else _DEFAULT_QUOTECHAR
    return DialectDetection(
        delimiter=csv_dialect.delimiter,
        quotechar=quotechar,
        declined=False,
    )


def to_stdlib_dialect(detection: DialectDetection) -> type[csv.Dialect]:
    """Build a real ``csv.Dialect`` a ``csv.reader`` can be constructed from.

    This is the function a later wiring plan (06-14) calls to construct the
    actual streaming reader (CSV-06: quoted delimiters, escaped quotes,
    multiline fields and inconsistent quoting are handled by a real CSV
    parser, never string splitting). The returned class works identically
    whether passed to ``csv.reader(..., dialect=...)`` as the class itself
    or as an instance of it -- verified live, since ``csv.reader`` only ever
    reads attributes off whatever ``dialect`` value it is given.

    Args:
        detection: A prior ``detect_dialect`` result, or an equivalent
            manually-constructed value.

    Returns:
        A ``csv.Dialect`` subclass carrying ``detection``'s delimiter and
        quotechar, RFC 4180 doubled-quote escaping (``doublequote=True``,
        ``escapechar=None``), and ``csv.QUOTE_MINIMAL``.

    Raises:
        CsvDialectDetectionError: ``detection.declined`` is ``True`` (or,
            defensively, ``detection.delimiter`` is ``None`` regardless of
            the flag) -- there is genuinely no usable delimiter and no
            contract fallback was ever supplied. This is the boundary
            between "declined, but the caller has a contract fallback" (not
            an error -- see ``contract_delimiter`` above) and "declined,
            with no fallback at all" (a real file/run-fatal condition).
    """
    if detection.declined or detection.delimiter is None:
        msg = (
            "dialect detection declined and no contract delimiter was ever "
            "supplied -- there is genuinely no usable delimiter for this sample"
        )
        raise CsvDialectDetectionError(
            msg, context={"diagnostic_code": "dialect-detection-declined"}
        )

    # Named distinctly from the class body's own attributes below on
    # purpose: a class body that both reads AND assigns the same name (e.g.
    # `delimiter = delimiter`) treats every reference to that name as local
    # to the class body being built, never a closure over the enclosing
    # function -- the read then fails with `NameError` before the
    # assignment ever runs. `SimpleDialect.to_csv_dialect()` avoids this by
    # reading through `self.delimiter` instead (`self` is never reassigned
    # in its class body); this uses distinctly-named locals for the same
    # reason.
    detected_delimiter = detection.delimiter
    detected_quotechar = detection.quotechar

    class _DetectedDialect(csv.Dialect):
        """A concrete ``csv.Dialect`` closing over one detection's values."""

        delimiter = detected_delimiter
        quotechar = detected_quotechar
        escapechar = None
        doublequote = True
        quoting = csv.QUOTE_MINIMAL
        skipinitialspace = False
        lineterminator = "\n"

    return _DetectedDialect
