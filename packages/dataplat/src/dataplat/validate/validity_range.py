"""``ValidityRangeRule`` -- VALID-02's numeric-bounds half of the minimum quality rule set.

Mirrors ``CompletenessRule``'s exact shape (which itself mirrors
``RaggedRowGuard``'s): a ``name`` class attribute, constructor-injected
runtime parameters, and an ``apply(ctx, chunk) -> StageResult`` that never
raises for a row-level problem (QUAL-03) -- an out-of-range or unparseable
value becomes a ``RejectedRecord`` instead, distinguished by ``error_type``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dataplat.models.record import RejectedRecord, StageResult
from dataplat.observability import metrics
from dataplat.pipeline.protocol import StreamingStage

if TYPE_CHECKING:
    from dataplat.models.record import RecordChunk
    from dataplat.pipeline.protocol import PipelineContext


class ValidityRangeRule(StreamingStage):
    """Rejects a row whose numeric value at a column falls outside ``[minimum, maximum]``.

    One instance handles one column. A value this rule cannot parse as a
    number is a distinct rejection (``VALIDITY_RANGE_UNPARSEABLE``), never
    conflated with an in-range-check failure (``VALIDITY_RANGE_VIOLATION``)
    -- and never a raised exception (QUAL-03). This rule only classifies; it
    never coerces the field's own string representation -- ``StagingLoader``'s
    existing normalizers own actual type coercion.
    """

    name = "validity_range_rule"

    def __init__(  # noqa: PLR0913 -- seven keyword-only config fields, all load-bearing
        self,
        *,
        column_index: int,
        column_name: str,
        strategy: str,
        rule_id: str,
        business_key_index: int | None = None,
        minimum: float | None = None,
        maximum: float | None = None,
    ) -> None:
        """Configure which column this rule bounds and its declared strategy/identity.

        Args:
            column_index: The 0-based position of the bounded column within
                each row tuple.
            column_name: The column's name, for diagnostics and the
                resulting ``RejectedRecord.error_column``.
            strategy: The dataset config's declared bad-record strategy for
                this rule (e.g. ``"REJECT_RECORD"``). Stored but not yet
                dispatched on -- plan 08-10 wires strategy-based branching.
            rule_id: The dataset config's stable identifier for this rule
                instance. Stored but not yet emitted anywhere in this plan.
            business_key_index: The 0-based position of the dataset's
                configured business-key column within each row tuple (D-23),
                distinct from this rule's own ``column_index``. ``None``
                when the dataset declares no ``business_key`` column.
            minimum: The inclusive lower bound, or ``None`` for no lower
                bound.
            maximum: The inclusive upper bound, or ``None`` for no upper
                bound.
        """
        self._column_index = column_index
        self._column_name = column_name
        self._strategy = strategy
        self._rule_id = rule_id
        self._business_key_index = business_key_index
        self._minimum = minimum
        self._maximum = maximum

    def apply(self, ctx: PipelineContext, chunk: RecordChunk) -> StageResult:
        """Reject a row whose value at this rule's column is unparseable or out-of-range.

        Never raises: a non-numeric value never propagates a ``ValueError``
        past this method (QUAL-03).

        Args:
            ctx: The current pipeline context. ``ctx.config.dataset`` labels
                this stage's ``metrics.increment()`` calls (D-04's bounded
                label set: ``dataset``+``stage``+``status``).
            chunk: The chunk to check.

        Returns:
            A ``StageResult`` whose ``chunk`` holds only the in-range rows,
            and whose ``rejected`` holds one ``RejectedRecord`` per
            unparseable or out-of-range row. ``findings`` is always empty --
            strategy-based warning emission is plan 08-10's job.
        """
        idx = self._column_index
        minimum = self._minimum
        maximum = self._maximum
        kept: list[tuple[str | bool | None, ...]] = []
        rejected: list[RejectedRecord] = []
        for i, row in enumerate(chunk.rows):
            raw_value = row[idx]
            try:
                # `float()` accepts `str`/`bool` (`SupportsFloat`) but not
                # `None` -- deliberately narrow: both an unparseable string
                # and an absent (`None`) value are meant to land here as
                # "unparseable", never propagate a raw exception (QUAL-03).
                value = float(raw_value)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                rejected.append(
                    RejectedRecord(
                        source_row_number=chunk.first_ordinal + i,
                        error_type="VALIDITY_RANGE_UNPARSEABLE",
                        error_message=(
                            f"value {raw_value!r} at column {self._column_name!r} is not numeric"
                        ),
                        raw_line=_reconstruct_raw_line(row),
                        error_column=self._column_name,
                        business_key=_extract_business_key(row, self._business_key_index),
                    )
                )
                continue
            if (minimum is not None and value < minimum) or (
                maximum is not None and value > maximum
            ):
                rejected.append(
                    RejectedRecord(
                        source_row_number=chunk.first_ordinal + i,
                        error_type="VALIDITY_RANGE_VIOLATION",
                        error_message=(
                            f"value {value!r} at column {self._column_name!r} is outside "
                            f"the configured bound [{minimum!r}, {maximum!r}]"
                        ),
                        raw_line=_reconstruct_raw_line(row),
                        error_column=self._column_name,
                        business_key=_extract_business_key(row, self._business_key_index),
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

    Mirrors ``RaggedRowGuard``'s, ``BooleanNormalizer``'s and
    ``CompletenessRule``'s reconstruction convention (``RejectedRecord.raw_line``'s
    docstring, WR-04: a best-effort aid, not a guaranteed byte-for-byte copy
    of the source row), tolerating a non-``str`` field.
    """
    return field_delimiter.join(
        "" if field is None else (field if isinstance(field, str) else str(field)) for field in row
    )


# Duplicated per-file rather than imported, mirroring `_reconstruct_raw_line`'s
# own established convention in this codebase.
def _extract_business_key(
    row: tuple[str | bool | None, ...],
    business_key_index: int | None,
) -> str | None:
    """Extract this row's business-key column value, or ``None`` when unreliable.

    Returns ``None`` when ``business_key_index`` is ``None`` (no
    ``business_key`` column configured for this dataset), or when the value
    at that position is ``None``/``""`` (D-25: an empty/absent business-key
    value is exactly as unreliable as a missing one). Otherwise returns the
    value as a ``str``, stringifying a non-``str`` field (``_reconstruct_raw_line``'s
    own non-str-tolerance idiom).
    """
    if business_key_index is None:
        return None
    value = row[business_key_index]
    if value is None or value == "":
        return None
    return value if isinstance(value, str) else str(value)
