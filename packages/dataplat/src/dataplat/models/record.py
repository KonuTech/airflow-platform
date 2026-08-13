"""Row-vocabulary value objects: chunks, rejects, and per-stage results.

``RecordChunk`` and ``RejectedRecord`` are frozen — genuinely immutable value
objects. ``StageResult`` is not: ARCHITECTURE.md declares it without
``frozen=True`` (Q4.3) because a stage accumulates ``rejected``/``findings``
while it runs.
"""

from __future__ import annotations

import dataclasses
from collections import Counter
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from dataplat.models.report import ValidationResult


@dataclass(frozen=True, slots=True)
class RecordChunk:
    """A bounded slice of parsed rows, addressed by ordinal, not byte offset.

    Attributes:
        rows: The parsed rows in this chunk, each a tuple of raw field
            strings in source column order.
        first_ordinal: The 0-based row ordinal of ``rows[0]`` within the
            file. This is the resume/checkpoint value — never a byte offset,
            since resuming inside a quoted multiline field is not possible.
        expected_field_count: The field count every row in this chunk is
            expected to have, from the detected or configured header.
    """

    rows: tuple[tuple[str, ...], ...]
    first_ordinal: int
    expected_field_count: int

    def replace(self, **changes: object) -> RecordChunk:
        """Return a new ``RecordChunk`` with only the given fields changed.

        The sole functional-update mechanism for this type: every later
        stage narrows a chunk through this method instead of hand-rolling a
        three-argument constructor call at each call site.

        Args:
            **changes: Field names and their replacement values.

        Returns:
            A new ``RecordChunk``. The original is never mutated.
        """
        # mypy's dataclasses.replace() special-casing unifies each **kwarg
        # against its field's declared type, and rejects `object` outright
        # (it is not `Any`) even though every field type is a valid `object`.
        # The public signature stays `object` — genuinely unknown input is
        # what a caller passes here — and this cast is the narrow, internal
        # workaround for that mypy behavior, not a relaxation of the contract.
        return dataclasses.replace(self, **cast("dict[str, Any]", changes))


@dataclass(frozen=True, slots=True)
class RejectedRecord:
    """One source row that could not be kept, and why.

    Data, not an exception (QUAL-03) — a malformed row is recorded here
    instead of aborting the run.

    Attributes:
        source_row_number: The row's ordinal position in the source file.
        error_type: A short, stable, machine-readable reason code, e.g.
            ``"RAGGED_ROW"``.
        error_message: A human-readable description of the failure.
        raw_line: The row's source text, for audit/reprocessing purposes.
            Populated by whichever stage created this ``RejectedRecord`` --
            today that is only ``dataplat.pipeline.engine.RaggedRowGuard``,
            which reconstructs it by rejoining the row's already-*parsed*
            fields (quoting/escaping already resolved), not by capturing the
            true original source bytes (WR-04). That reconstruction is exact
            only when none of the row's fields contain the join delimiter,
            an embedded newline, or a quote character -- exactly the cases
            CSV quoting exists to handle in the first place. Treat this
            field as a best-effort diagnostic aid, not a guaranteed
            byte-for-byte copy of the source row, unless/until a future
            stage threads the true raw physical line through instead.
        error_column: The column the failure is attributed to, when the
            failure is column-specific. ``None`` for row-level failures.
    """

    source_row_number: int
    error_type: str
    error_message: str
    raw_line: str
    error_column: str | None = None


@dataclass
class StageResult:
    """The outcome of one pipeline stage applied to one chunk.

    Deliberately not frozen: a stage mutates ``rejected`` and ``findings``
    while it runs (ARCHITECTURE.md Q4.3).

    Attributes:
        chunk: The chunk that survived this stage.
        rejected: Rows this stage removed from ``chunk``, with reasons.
        findings: Validation findings this stage raised.
        metrics: Row-count deltas this stage produced, e.g.
            ``rows_valid``/``rows_invalid``.
    """

    chunk: RecordChunk
    rejected: list[RejectedRecord] = field(default_factory=list)
    findings: list[ValidationResult] = field(default_factory=list)
    metrics: Counter[str] = field(default_factory=Counter)
