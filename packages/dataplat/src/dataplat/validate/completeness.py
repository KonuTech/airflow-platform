"""``CompletenessRule`` -- VALID-02's required-column half of the minimum quality rule set.

Mirrors ``dataplat.pipeline.engine.RaggedRowGuard``'s exact shape: a ``name``
class attribute, constructor-injected runtime parameters (never read from a
global or re-derived internally), and an ``apply(ctx, chunk) -> StageResult``
that never raises for a row-level problem (QUAL-03) -- an empty required
value becomes a ``RejectedRecord`` instead.

``strategy``/``rule_id`` are stored but not yet dispatched on here: deciding
``REJECT_RECORD`` vs ``WARN_AND_CONTINUE`` vs ``QUARANTINE_FILE`` behavior
for a completeness violation is plan 08-10's wiring job. This plan proves the
detection logic in isolation -- every violation always populates
``StageResult.rejected``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dataplat.models.record import RejectedRecord, StageResult
from dataplat.observability import metrics
from dataplat.pipeline.protocol import StreamingStage

if TYPE_CHECKING:
    from dataplat.models.record import RecordChunk
    from dataplat.pipeline.protocol import PipelineContext


class CompletenessRule(StreamingStage):
    """Rejects a row whose value at a required, non-nullable column is empty.

    One instance handles one column. "Empty" means ``None`` (the
    platform-wide absent-value representation, ``RecordChunk.rows``'s
    docstring) or the empty string ``""`` -- never a substring or whitespace
    heuristic.
    """

    name = "completeness_rule"

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
            column_index: The 0-based position of the required column within
                each row tuple.
            column_name: The column's name, for diagnostics and the
                resulting ``RejectedRecord.error_column``.
            strategy: The dataset config's declared bad-record strategy for
                this rule (e.g. ``"REJECT_RECORD"``). Stored but not yet
                dispatched on -- plan 08-10 wires strategy-based branching.
            rule_id: The dataset config's stable identifier for this rule
                instance, for diagnostics and future audit trails. Stored
                but not yet emitted anywhere in this plan.
            business_key_index: The 0-based position of the dataset's
                configured business-key column within each row tuple (D-23),
                distinct from this rule's own ``column_index`` -- a rule may
                check a non-business-key column (e.g. ``customers_name_completeness``
                checks ``name`` but must still capture ``customer_id``).
                ``None`` when the dataset declares no ``business_key`` column.
        """
        self._column_index = column_index
        self._column_name = column_name
        self._strategy = strategy
        self._rule_id = rule_id
        self._business_key_index = business_key_index

    def apply(self, ctx: PipelineContext, chunk: RecordChunk) -> StageResult:
        """Reject every row whose value at this rule's column is empty.

        Never raises: every row resolves to either a kept row or a
        ``RejectedRecord`` (QUAL-03).

        Args:
            ctx: The current pipeline context. ``ctx.config.dataset`` labels
                this stage's ``metrics.increment()`` calls (D-04's bounded
                label set: ``dataset``+``stage``+``status``).
            chunk: The chunk to check.

        Returns:
            A ``StageResult`` whose ``chunk`` holds only the rows with a
            non-empty value at this rule's column, and whose ``rejected``
            holds one ``RejectedRecord`` per empty-value row.
            ``findings`` is always empty -- strategy-based warning emission
            is plan 08-10's job.
        """
        idx = self._column_index
        kept: list[tuple[str | bool | None, ...]] = []
        rejected: list[RejectedRecord] = []
        for i, row in enumerate(chunk.rows):
            value = row[idx]
            if value is None or value == "":
                rejected.append(
                    RejectedRecord(
                        source_row_number=chunk.first_ordinal + i,
                        error_type="COMPLETENESS_VIOLATION",
                        error_message=f"required column {self._column_name!r} is empty",
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

    Mirrors ``RaggedRowGuard``'s and ``BooleanNormalizer``'s reconstruction
    convention (``RejectedRecord.raw_line``'s docstring, WR-04: a best-effort
    aid, not a guaranteed byte-for-byte copy of the source row), tolerating a
    non-``str`` field -- a row reaching this rule may already carry ``None``
    or ``bool`` from an upstream normalizer.
    """
    return field_delimiter.join(
        "" if field is None else (field if isinstance(field, str) else str(field)) for field in row
    )


# Duplicated per-file rather than imported, mirroring `_reconstruct_raw_line`'s
# own established convention in this codebase (each of `completeness.py`,
# `pattern.py`, `validity_range.py`, `uniqueness.py` carries its own copy).
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
