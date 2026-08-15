"""``NullTokenNormalizer`` + ``BooleanNormalizer`` -- CSV-10's boolean/NULL half.

Both are ``StreamingStage``s mirroring ``dataplat.pipeline.engine.RaggedRowGuard``'s
exact shape: a ``name`` class attribute, constructor-injected runtime parameters
(never read from a global or re-derived internally), and an
``apply(ctx, chunk) -> StageResult`` that never raises for a row-level problem
(QUAL-03).

**Matching is exact, never substring.** Corpus fixture ``24_null_values.csv``
exists specifically to prove this: its row 8, ``"NULL Industries"``, is a
company name that *contains* the four-letter token ``"NULL"`` but is not
itself the token. A substring check (``"NULL" in value``) would destroy that
row's real data by mistaking it for an absent value. Every comparison in
``NullTokenNormalizer`` below is therefore whole-field equality
(``value in self._null_tokens``), never ``token in value``.

**An unmapped boolean token is a rejection, never a default.** Corpus fixture
``60_boolean_localized.csv`` row 8, ``"Maybe"``, is declared in neither
``true_tokens`` nor ``false_tokens`` and must produce a ``RejectedRecord``
with ``error_type == "unmapped-boolean-token"`` -- never silently coerced to
``False``. A boolean has exactly two values, so any default is always half
wrong (the fixture's own framing), and the fixture's row 5 (``"O"``, French
``Oui``, meaning ``True``) is the concrete trap this project must not invert:
folding an unrecognised token to zero-or-off would silently invert a record
that was actually ``True`` -- and an inverted boolean is invisible to every
row count, checksum and reconciliation total downstream. ``"0"``/``"1"``
receive no special-cased handling anywhere in ``BooleanNormalizer``: they
participate in exact-match lookup exactly like any other string and are
rejected as unmapped unless a caller explicitly listed them in one of the
two declared token tuples (CSV-10's own named risk: "1/0 must never become
boolean absent evidence").

Platform-wide ``None`` convention (this module's own load-bearing decision;
see ``dataplat.models.record.RecordChunk.rows``'s docstring for the full
citation): an absent field is represented as Python ``None`` placed directly
into the row tuple. Every normalizer staged AFTER a ``NullTokenNormalizer``
for the same column -- including ``BooleanNormalizer`` below, and
``dataplat.normalize.unicode.UnicodeNormalizer`` for every column -- treats
an already-``None`` field as "already normalized, pass through unchanged",
never re-parsed, transformed or rejected.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dataplat.models.record import RejectedRecord, StageResult
from dataplat.pipeline.protocol import StreamingStage

if TYPE_CHECKING:
    from dataplat.models.record import RecordChunk
    from dataplat.pipeline.protocol import PipelineContext


class NullTokenNormalizer(StreamingStage):
    """Replaces a field whose value exactly matches a declared NULL token with ``None``.

    One instance handles one column. ``null_tokens`` is a per-dataset
    contract decision, never a platform-wide default beyond the empty string
    (D-14) -- a reader that hardcodes a NULL-token list is wrong for the
    next dataset (fixture 24's own framing: ``"NA"`` is also the ISO 3166
    country code for Namibia, so a country dataset must not declare it
    absent).
    """

    name = "null_token_normalizer"

    def __init__(
        self,
        *,
        column_index: int,
        column_name: str,
        null_tokens: tuple[str, ...],
    ) -> None:
        """Configure which column this stage normalizes and against which tokens.

        Args:
            column_index: The 0-based position of the column this stage
                normalizes, within each row tuple.
            column_name: The column's name, for diagnostics/logging.
            null_tokens: The exact strings that mean "absent" for this
                column, e.g. ``("", "NULL", "N/A")``. Compared by whole-field
                equality only -- never a substring/contains check (fixture
                24's ``"NULL Industries"`` trap).
        """
        self._column_index = column_index
        self._column_name = column_name
        self._null_tokens = null_tokens

    def apply(self, ctx: PipelineContext, chunk: RecordChunk) -> StageResult:  # noqa: ARG002
        """Replace this column's field with ``None`` wherever it exactly matches a NULL token.

        Never rejects a row: every row that enters this stage survives it,
        possibly with one field replaced.

        Args:
            ctx: The current pipeline context. Unused: this stage's decision
                depends only on ``chunk``, and the parameter exists to
                satisfy ``StreamingStage``.
            chunk: The chunk to normalize.

        Returns:
            A ``StageResult`` whose ``chunk`` holds every input row, each
            with this column's field replaced by ``None`` where it was an
            exact NULL-token match. ``rejected`` and ``findings`` are always
            empty.
        """
        idx = self._column_index
        tokens = self._null_tokens
        new_rows: list[tuple[str | bool | None, ...]] = []
        for row in chunk.rows:
            value = row[idx]
            # Exact whole-field match only -- `value in tokens` compares
            # equality against each token, never `token in value`
            # (fixture 24). An already-non-str value (e.g. `None` from a
            # re-run of this stage, or a `bool` from a BooleanNormalizer
            # that ran first) is safely never `in` a tuple of strings, so it
            # is left untouched without any special-casing.
            if value in tokens:
                new_rows.append((*row[:idx], None, *row[idx + 1 :]))
            else:
                new_rows.append(row)
        return StageResult(chunk=chunk.replace(rows=tuple(new_rows)), rejected=[], findings=[])


class BooleanNormalizer(StreamingStage):
    """Maps a column's declared true/false tokens to Python ``bool``; rejects everything else.

    One instance handles one column. An unmapped value is always a
    ``RejectedRecord`` with ``error_type == "unmapped-boolean-token"`` --
    never a silent default to ``False`` (fixture 60's own framing: "a
    boolean has two values so a default is always half wrong").
    """

    name = "boolean_normalizer"

    def __init__(
        self,
        *,
        column_index: int,
        column_name: str,
        true_tokens: tuple[str, ...],
        false_tokens: tuple[str, ...],
    ) -> None:
        """Configure which column this stage normalizes and its declared token maps.

        Args:
            column_index: The 0-based position of the column this stage
                normalizes, within each row tuple.
            column_name: The column's name, for diagnostics and the
                resulting ``RejectedRecord.error_column``.
            true_tokens: The exact strings that mean ``True`` for this
                column, e.g. ``("Tak", "Ja", "O", "Y")``. Locale-specific and
                declared, never inferred (fixture 60).
            false_tokens: The exact strings that mean ``False`` for this
                column, e.g. ``("Nie", "Nein", "N")``.
        """
        self._column_index = column_index
        self._column_name = column_name
        self._true_tokens = true_tokens
        self._false_tokens = false_tokens

    def apply(self, ctx: PipelineContext, chunk: RecordChunk) -> StageResult:  # noqa: ARG002
        """Map this column's field to ``True``/``False``/a rejection, or pass ``None`` through.

        Args:
            ctx: The current pipeline context. Unused: this stage's decision
                depends only on ``chunk``, and the parameter exists to
                satisfy ``StreamingStage``.
            chunk: The chunk to normalize.

        Returns:
            A ``StageResult`` whose ``chunk`` holds every row whose value
            was ``None``, ``True``-mapped or ``False``-mapped, and whose
            ``rejected`` holds one ``RejectedRecord`` per row whose value
            matched neither declared token list. ``"0"``/``"1"`` receive no
            special-cased handling: they are rejected as unmapped unless a
            caller explicitly listed them in ``true_tokens``/``false_tokens``.
        """
        idx = self._column_index
        true_tokens = self._true_tokens
        false_tokens = self._false_tokens
        kept: list[tuple[str | bool | None, ...]] = []
        rejected: list[RejectedRecord] = []
        for i, row in enumerate(chunk.rows):
            value = row[idx]
            if value is None:
                # Already normalized to absent upstream (NullTokenNormalizer
                # for this same nullable column) -- pass through unchanged,
                # never re-processed as if it were a string (this module's
                # platform-wide None convention).
                kept.append(row)
                continue
            if value in true_tokens:
                kept.append((*row[:idx], True, *row[idx + 1 :]))
            elif value in false_tokens:
                kept.append((*row[:idx], False, *row[idx + 1 :]))
            else:
                rejected.append(
                    RejectedRecord(
                        source_row_number=chunk.first_ordinal + i,
                        error_type="unmapped-boolean-token",
                        error_message=(
                            f"value {value!r} at column {self._column_name!r} is not a "
                            f"declared boolean token (true_tokens={true_tokens!r}, "
                            f"false_tokens={false_tokens!r})"
                        ),
                        raw_line=_reconstruct_raw_line(row),
                        error_column=self._column_name,
                    )
                )
        return StageResult(
            chunk=chunk.replace(rows=tuple(kept)),
            rejected=rejected,
            findings=[],
        )


def _reconstruct_raw_line(
    row: tuple[str | bool | None, ...],
    *,
    field_delimiter: str = ",",
) -> str:
    """Rejoin an already-parsed row's fields into a best-effort diagnostic line.

    Mirrors ``RaggedRowGuard``'s reconstruction convention
    (``dataplat.models.record.RejectedRecord.raw_line``'s docstring, WR-04:
    a best-effort aid, not a guaranteed byte-for-byte copy of the source
    row), extended to tolerate a non-``str`` field -- a row reaching
    ``BooleanNormalizer`` may already carry ``None`` (absent) or ``bool``
    (an already-normalized boolean from another column) from an upstream
    normalizer, neither of which ``str.join`` can accept directly.
    """
    return field_delimiter.join(
        "" if field is None else (field if isinstance(field, str) else str(field)) for field in row
    )
