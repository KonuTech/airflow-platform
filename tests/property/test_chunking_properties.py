"""Property test for ``csv_processor.source.chunked_records`` — CSV-13's general proof.

``tests/unit/test_csv_chunking.py`` proves six specific behaviors at fixed
chunk sizes 1, 2 and 3. This file generalizes the record-preservation and
contiguous-ordinal guarantees (its Test 6) across *arbitrary* chunk sizes
and record sets: chunking never drops, reorders or splits a record, no
matter how the input table or the chunk size varies (03-CONTEXT.md's
Claude's Discretion item; PITFALLS.md E1's "parameterised over chunk sizes"
guidance taken to its generalized conclusion).
"""

from __future__ import annotations

import csv
import io
import string

from hypothesis import given, settings
from hypothesis import strategies as st

from csv_processor.source import chunked_records

# `string.printable` -- letters, digits, punctuation and whitespace -- keeps
# generated fields inside what csv.writer/csv.reader are documented to
# round-trip exactly (with the one exception filtered out below), rather
# than chasing every corner of arbitrary Unicode (surrogates, exotic
# categories) that this property has no interest in exercising.
_FIELD_TEXT = st.text(alphabet=string.printable, max_size=20)

# A single-column table whose one field is the empty string serializes to a
# genuinely blank physical line -- which csv.reader reads back as a
# *zero*-field row, not a one-empty-field row (a documented CSV format
# ambiguity, not a chunked_records() bug). Excluded only for k == 1, see
# _csv_table below -- for k >= 2 a row always contains at least one comma,
# so it can never serialize to a blank line.
_NON_EMPTY_FIELD_TEXT = _FIELD_TEXT.filter(lambda s: s != "")


@st.composite
def _csv_table(draw: st.DrawFn) -> tuple[int, list[tuple[str, ...]]]:
    """Draw a fixed column count ``k`` and a list of well-formed ``k``-field rows."""
    k = draw(st.integers(min_value=1, max_value=5))
    field = _FIELD_TEXT if k > 1 else _NON_EMPTY_FIELD_TEXT
    rows = draw(st.lists(st.tuples(*[field] * k), max_size=25))
    return k, rows


def _build_csv_bytes(k: int, rows: list[tuple[str, ...]]) -> bytes:
    """Serialize ``rows`` under a ``k``-column header via ``csv.writer`` (always well-quoted)."""
    header = tuple(f"col{i}" for i in range(k))
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(header)
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")


# max_examples=200: each example is a couple dozen short in-memory strings
# with no I/O, so 200 examples run in well under a second -- comfortably
# inside the phase's ~90-second feedback-latency budget (03-VALIDATION.md),
# which is dominated elsewhere by testcontainers startup, not by this test.
@settings(max_examples=200)
@given(table=_csv_table(), chunk_size=st.integers(min_value=1, max_value=10))
def test_chunking_preserves_record_set_and_order(
    table: tuple[int, list[tuple[str, ...]]], chunk_size: int
) -> None:
    k, rows = table
    csv_bytes = _build_csv_bytes(k, rows)
    stream = io.TextIOWrapper(io.BytesIO(csv_bytes), encoding="utf-8", newline="")

    chunks = list(chunked_records(stream, chunk_size=chunk_size))

    # No drop, no reorder, no split: the flattened chunk rows equal the
    # generated rows exactly, in the same order.
    flattened = [row for chunk in chunks for row in chunk.rows]
    assert flattened == rows

    # Grouping actually respects chunk_size: every chunk but the last holds
    # exactly chunk_size rows, and the last holds between 1 and chunk_size.
    # Flattening (above) and ordinal contiguity (below) both stay true even
    # if chunk_size were silently ignored and every chunk forced to size 1
    # -- this is the assertion that would actually fail in that case, so it
    # is what keeps this property non-vacuous with respect to grouping.
    for chunk in chunks[:-1]:
        assert len(chunk.rows) == chunk_size
    if chunks:
        assert 1 <= len(chunks[-1].rows) <= chunk_size

    # Ordinals are contiguous and non-overlapping: each chunk's
    # first_ordinal is exactly the running total of rows seen so far.
    running_total = 0
    for chunk in chunks:
        assert chunk.first_ordinal == running_total
        running_total += len(chunk.rows)
    assert running_total == len(rows)
