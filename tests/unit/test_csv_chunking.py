"""Unit tests for ``csv_processor.source.chunked_records`` — CSV-13's chunking proof.

Six behaviors, one function under test: chunking a CSV file in RECORDS
(never lines, never byte offsets) means an embedded newline inside a quoted
field can never land on a chunk boundary, a NUL byte never reaches a parsed
field, and a ragged row is neither padded nor truncated. Every fixture below
is a hand-built, in-file byte string (03-08-PLAN.md Task 2's action) --
none of them depend on ``tests/fixtures/csv/``, which is Phase 1's
seed-generated corpus for Phase 6's detection engine, not this pure unit
test. No MinIO and no ``dataplat.storage.objectstore`` are involved: each
fixture is wrapped directly in ``io.TextIOWrapper(io.BytesIO(...), ...)``.
"""

from __future__ import annotations

import io
import itertools
from typing import TYPE_CHECKING

from csv_processor.source import chunked_records
from dataplat.observability import metrics

if TYPE_CHECKING:
    import pytest

    from dataplat.models.record import RecordChunk


def _stream(data: bytes) -> io.TextIOWrapper:
    return io.TextIOWrapper(io.BytesIO(data), encoding="utf-8", newline="")


def _flatten(chunks: list[RecordChunk]) -> list[tuple[str, ...]]:
    return [row for chunk in chunks for row in chunk.rows]


# Header plus 5 data rows; the second data row's `note` field embeds a LF
# inside quotes (PITFALLS.md E1's boundary case).
_LF_FIXTURE = b'id,name,note\n1,Alice,ok\n2,Bob,"line1\nline2"\n3,Carol,ok\n4,Dave,ok\n5,Eve,ok\n'

# A quoted field embedding CRLF, not just LF -- proves newline="" was
# honored, since universal-newline translation would rewrite \r\n to \n.
_CRLF_FIXTURE = b'id,note\n1,ok\n2,"line1\r\nline2"\n3,ok\n'

# A NUL byte embedded directly in an unquoted field's raw bytes.
_NUL_FIXTURE = b"id,name\n1,Al\x00ice\n2,Bob\n"

# Header has 3 columns; the second data row has only 2 -- ragged.
_RAGGED_FIXTURE = b"id,name,note\n1,Alice,ok\n2,Bob\n3,Carol,ok\n"


# Test 1: an embedded LF inside quotes survives chunking at chunk_size=1,
# producing exactly 5 chunks (one record per chunk) with the field intact.
def test_embedded_lf_survives_chunking_at_size_one() -> None:
    chunks = list(chunked_records(_stream(_LF_FIXTURE), chunk_size=1))

    assert len(chunks) == 5
    all_rows = _flatten(chunks)
    assert all_rows[1] == ("2", "Bob", "line1\nline2")


# Test 2: chunk_size changes only grouping, never parsed content -- sizes 2
# and 3 both yield the same 5 records with byte-identical field content.
def test_chunk_size_never_changes_parsed_content() -> None:
    rows_at_2 = _flatten(list(chunked_records(_stream(_LF_FIXTURE), chunk_size=2)))
    rows_at_3 = _flatten(list(chunked_records(_stream(_LF_FIXTURE), chunk_size=3)))

    assert rows_at_2 == rows_at_3
    assert len(rows_at_2) == 5
    assert rows_at_2[1] == ("2", "Bob", "line1\nline2")


# Test 3: an embedded CRLF inside quotes round-trips intact -- proves the
# caller's newline="" was honored (fails if the stream drops that option).
def test_embedded_crlf_round_trips_intact() -> None:
    chunks = list(chunked_records(_stream(_CRLF_FIXTURE), chunk_size=2))

    all_rows = _flatten(chunks)
    assert all_rows[1] == ("2", "line1\r\nline2")


# Test 4: a NUL byte in the raw source bytes never reaches a parsed field.
def test_nul_byte_never_reaches_a_parsed_field() -> None:
    chunks = list(chunked_records(_stream(_NUL_FIXTURE), chunk_size=10))

    all_rows = _flatten(chunks)
    assert all_rows[0] == ("1", "Alice")
    assert "\x00" not in all_rows[0][1]


# Test 5: a ragged row passes through unpadded and untruncated, in exactly
# one RecordChunk.rows entry, with expected_field_count from the header.
def test_ragged_row_passes_through_unpadded_and_untruncated() -> None:
    chunks = list(chunked_records(_stream(_RAGGED_FIXTURE), chunk_size=10))

    assert len(chunks) == 1
    chunk = chunks[0]
    assert chunk.expected_field_count == 3
    ragged = [row for row in chunk.rows if len(row) != chunk.expected_field_count]
    assert ragged == [("2", "Bob")]


# Test 6: record ordinals across consecutive chunks are contiguous and
# non-overlapping, at every chunk size.
def test_ordinals_are_contiguous_and_non_overlapping() -> None:
    for chunk_size in (1, 2, 3):
        chunks = list(chunked_records(_stream(_LF_FIXTURE), chunk_size=chunk_size))
        for earlier, later in itertools.pairwise(chunks):
            assert later.first_ordinal == earlier.first_ordinal + len(earlier.rows)


# Test 7 (CR-02): a genuinely empty (zero-byte) stream -- no header, no rows
# -- yields zero chunks rather than crashing with an opaque RuntimeError
# (PEP 479 turning next(reader)'s uncaught StopIteration into
# "generator raised StopIteration" inside chunked_records's generator body).
def test_empty_stream_yields_no_chunks_and_does_not_raise() -> None:
    chunks = list(chunked_records(_stream(b""), chunk_size=10))

    assert chunks == []


# Test 8 (WR-05): stripping a NUL byte is observable, not silent -- exactly
# one metrics.increment("lines_with_nul_stripped") per physical line that
# actually contained a NUL, and none for the NUL-free lines in the same file.
def test_nul_stripping_increments_a_metric_once_per_affected_line(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, int]] = []

    def _fake_increment(name: str, value: int = 1, **_labels: str) -> None:
        calls.append((name, value))

    monkeypatch.setattr(metrics, "increment", _fake_increment)

    list(chunked_records(_stream(_NUL_FIXTURE), chunk_size=10))

    # _NUL_FIXTURE (see fixture above) has exactly one physical line
    # containing a NUL byte ("1,Al\x00ice"); the header line and "2,Bob" do
    # not.
    assert calls == [("lines_with_nul_stripped", 1)]


def test_nul_free_stream_never_increments_the_metric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, int]] = []

    def _fake_increment(name: str, value: int = 1, **_labels: str) -> None:
        calls.append((name, value))

    monkeypatch.setattr(metrics, "increment", _fake_increment)

    list(chunked_records(_stream(_LF_FIXTURE), chunk_size=10))

    assert calls == []
