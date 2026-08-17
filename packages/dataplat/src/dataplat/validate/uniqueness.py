"""``UniquenessRule`` -- VALID-02's within-chunk business-key uniqueness rule.

Mirrors ``dataplat.validate.completeness.CompletenessRule``'s exact shape: a
``name`` class attribute, constructor-injected runtime parameters, and an
``apply(ctx, chunk) -> StageResult`` that never raises for a row-level
problem (QUAL-03) -- a duplicate value becomes a ``RejectedRecord`` instead.

Scope limit, deliberate and documented (RESEARCH.md Pattern 1's "within-chunk
vs cross-chunk" framing): this rule holds NO cross-chunk state. Its "seen"
set is rebuilt fresh on every ``apply()`` call, so a value repeated across
two different chunks is NOT caught here. True whole-file/whole-run
business-key uniqueness is already enforced at PUBLISH time by
``deduplication.strategy: business_key_latest`` (wired since Phase 4) -- this
rule is a pre-publish, in-stream diagnostic surface only, never a
replacement for that publish-time guarantee.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dataplat.models.record import RejectedRecord, StageResult
from dataplat.observability import metrics
from dataplat.pipeline.protocol import StreamingStage

if TYPE_CHECKING:
    from dataplat.models.record import RecordChunk
    from dataplat.pipeline.protocol import PipelineContext


class UniquenessRule(StreamingStage):
    """Rejects a row whose value at a business-key column already occurred earlier in this chunk.

    One instance handles one column. The FIRST occurrence of a value within
    a chunk is kept; every later occurrence in the SAME chunk is rejected.
    Scope is strictly within-chunk -- see the module docstring.
    """

    name = "uniqueness_rule"

    def __init__(
        self,
        *,
        column_index: int,
        column_name: str,
        strategy: str,
        rule_id: str,
        business_key_index: int | None = None,
    ) -> None:
        """Configure which column this rule guards and its declared strategy/identity.

        Args:
            column_index: The 0-based position of the business-key column
                within each row tuple.
            column_name: The column's name, for diagnostics and the
                resulting ``RejectedRecord.error_column``.
            strategy: The dataset config's declared bad-record strategy for
                this rule (e.g. ``"REJECT_RECORD"``). Stored but not yet
                dispatched on -- plan 08-10 wires strategy-based branching.
            rule_id: The dataset config's stable identifier for this rule
                instance, for diagnostics and future audit trails. Stored
                but not yet emitted anywhere in this plan.
            business_key_index: The 0-based position of the dataset's
                configured business-key column within each row tuple (D-23).
                For the real ``customers`` uniqueness rule, this equals
                ``column_index`` (the rule targets ``customer_id`` itself),
                but the parameter is still distinct and independently
                extracted -- matching every other rule in this phase's
                design. ``None`` when the dataset declares no
                ``business_key`` column.
        """
        self._column_index = column_index
        self._column_name = column_name
        self._strategy = strategy
        self._rule_id = rule_id
        self._business_key_index = business_key_index

    def apply(self, ctx: PipelineContext, chunk: RecordChunk) -> StageResult:
        """Reject every row whose value at this rule's column already occurred earlier this chunk.

        A local ``set`` of already-seen values is built fresh on every call
        -- never persisted across calls -- which is what makes this rule
        within-chunk-scoped, not cross-chunk (module docstring).

        Never raises: every row resolves to either a kept row or a
        ``RejectedRecord`` (QUAL-03).

        Args:
            ctx: The current pipeline context. ``ctx.config.dataset`` labels
                this stage's ``metrics.increment()`` calls (D-04's bounded
                label set: ``dataset``+``stage``+``status``).
            chunk: The chunk to check.

        Returns:
            A ``StageResult`` whose ``chunk`` holds only the first
            occurrence of each value at this rule's column, and whose
            ``rejected`` holds one ``RejectedRecord`` per later duplicate.
            ``findings`` is always empty -- strategy-based warning emission
            is plan 08-10's job.
        """
        idx = self._column_index
        seen: set[object] = set()
        kept: list[tuple[str | bool | None, ...]] = []
        rejected: list[RejectedRecord] = []
        for i, row in enumerate(chunk.rows):
            value = row[idx]
            if value in seen:
                rejected.append(
                    RejectedRecord(
                        source_row_number=chunk.first_ordinal + i,
                        error_type="UNIQUENESS_VIOLATION",
                        error_message=(
                            f"duplicate value {value!r} for business-key column "
                            f"{self._column_name!r} within this chunk"
                        ),
                        raw_line=_reconstruct_raw_line(row),
                        error_column=self._column_name,
                        business_key=_extract_business_key(row, self._business_key_index),
                    )
                )
                continue
            seen.add(value)
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

    Mirrors ``CompletenessRule``'s/``RaggedRowGuard``'s reconstruction
    convention (``RejectedRecord.raw_line``'s docstring, WR-04: a
    best-effort aid, not a guaranteed byte-for-byte copy of the source row).
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
