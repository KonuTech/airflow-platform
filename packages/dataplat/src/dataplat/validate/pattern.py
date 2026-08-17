"""``PatternRule`` -- VALID-02's regex-match half of the minimum quality rule set.

Mirrors ``CompletenessRule``'s/``ValidityRangeRule``'s exact shape: a
``name`` class attribute, constructor-injected runtime parameters, and an
``apply(ctx, chunk) -> StageResult`` that never raises for a row-level
problem (QUAL-03) -- a non-matching value becomes a ``RejectedRecord``
instead.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from dataplat.models.record import RejectedRecord, StageResult
from dataplat.observability import metrics
from dataplat.pipeline.protocol import StreamingStage

if TYPE_CHECKING:
    from dataplat.models.record import RecordChunk
    from dataplat.pipeline.protocol import PipelineContext


class PatternRule(StreamingStage):
    """Rejects a row whose value at a column does not fully match a configured regex.

    One instance handles one column. The pattern is compiled once at
    construction (never per-row, T-08-09) -- it is developer-authored
    dataset config, never attacker input, so this platform's threat model
    has no ReDoS concern from an untrusted regex source.
    """

    name = "pattern_rule"

    def __init__(
        self,
        *,
        column_index: int,
        column_name: str,
        strategy: str,
        rule_id: str,
        pattern: str,
    ) -> None:
        """Configure which column this rule guards and its declared pattern/strategy/identity.

        Args:
            column_index: The 0-based position of the guarded column within
                each row tuple.
            column_name: The column's name, for diagnostics and the
                resulting ``RejectedRecord.error_column``.
            strategy: The dataset config's declared bad-record strategy for
                this rule (e.g. ``"REJECT_RECORD"``). Stored but not yet
                dispatched on -- plan 08-10 wires strategy-based branching.
            rule_id: The dataset config's stable identifier for this rule
                instance. Stored but not yet emitted anywhere in this plan.
            pattern: The regex a value must fully match (``re.fullmatch``)
                to survive, e.g. ``r"^[A-Z]{2}$"``.
        """
        self._column_index = column_index
        self._column_name = column_name
        self._strategy = strategy
        self._rule_id = rule_id
        self._pattern = pattern
        self._compiled = re.compile(pattern)

    def apply(self, ctx: PipelineContext, chunk: RecordChunk) -> StageResult:
        """Reject every row whose value at this rule's column does not fully match the pattern.

        Never raises: any value -- including ``None`` or a ``bool`` from an
        upstream normalizer -- is safely stringified before matching
        (QUAL-03).

        Args:
            ctx: The current pipeline context. ``ctx.config.dataset`` labels
                this stage's ``metrics.increment()`` calls (D-04's bounded
                label set: ``dataset``+``stage``+``status``).
            chunk: The chunk to check.

        Returns:
            A ``StageResult`` whose ``chunk`` holds only the matching rows,
            and whose ``rejected`` holds one ``RejectedRecord`` per
            non-matching row. ``findings`` is always empty -- strategy-based
            warning emission is plan 08-10's job.
        """
        idx = self._column_index
        compiled = self._compiled
        kept: list[tuple[str | bool | None, ...]] = []
        rejected: list[RejectedRecord] = []
        for i, row in enumerate(chunk.rows):
            raw_value = row[idx]
            text_value = "" if raw_value is None else str(raw_value)
            if compiled.fullmatch(text_value) is None:
                rejected.append(
                    RejectedRecord(
                        source_row_number=chunk.first_ordinal + i,
                        error_type="PATTERN_VIOLATION",
                        error_message=(
                            f"value {raw_value!r} at column {self._column_name!r} does not "
                            f"match pattern {self._pattern!r}"
                        ),
                        raw_line=_reconstruct_raw_line(row),
                        error_column=self._column_name,
                    )
                )
                continue
            kept.append(row)

        # D-04's bounded label set: dataset+stage+status, never an unbounded
        # identity like run_id/file_id/batch_id.
        metrics.increment(
            "rows_rejected",
            len(rejected),
            dataset=ctx.config.dataset,
            stage=self.name,
            status="rejected",
        )
        metrics.increment(
            "rows_kept",
            len(kept),
            dataset=ctx.config.dataset,
            stage=self.name,
            status="kept",
        )
        return StageResult(chunk=chunk.replace(rows=tuple(kept)), rejected=rejected, findings=[])


def _reconstruct_raw_line(
    row: tuple[str | bool | None, ...],
    *,
    field_delimiter: str = ",",
) -> str:
    """Rejoin an already-parsed row's fields into a best-effort diagnostic line.

    Mirrors ``RaggedRowGuard``'s, ``BooleanNormalizer``'s, ``CompletenessRule``'s
    and ``ValidityRangeRule``'s reconstruction convention (``RejectedRecord.raw_line``'s
    docstring, WR-04: a best-effort aid, not a guaranteed byte-for-byte copy
    of the source row), tolerating a non-``str`` field.
    """
    return field_delimiter.join(
        "" if field is None else (field if isinstance(field, str) else str(field)) for field in row
    )
