"""``Publisher`` — how a staging table's rows are committed to their target.

This phase defines the protocol only. ``merge`` (README §36's default
publication strategy) arrives in Phase 4; SCD/CDC publishers arrive in Phase
10 (03-CONTEXT.md, deferred section). ``PublishResult`` is kept deliberately
minimal here — ``rows_affected``/``outcome`` — because the richer
``MERGE ... RETURNING merge_action()`` shape (STACK.md, PostgreSQL 17) is
Phase 4's ``merge`` Publisher's job to populate, not this protocol's to
over-specify before a single concrete ``Publisher`` exists.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from psycopg import Connection

    from dataplat.pipeline.protocol import PipelineContext


@dataclass(frozen=True, slots=True)
class PublishResult:
    """The outcome of one ``Publisher.publish()`` call.

    Attributes:
        rows_affected: The number of target-table rows this publish call
            inserted, updated or otherwise affected.
        outcome: A short, stable, machine-readable outcome code, e.g.
            ``"PUBLISHED"``.
    """

    rows_affected: int
    outcome: str


class Publisher(Protocol):
    """A pluggable strategy for committing a staging table's rows to their target.

    Attributes:
        name: The strategy's stable, human-readable identifier, e.g.
            ``"merge"``.
    """

    name: str

    def publish(
        self,
        ctx: PipelineContext,
        staging_table: str,
        conn: Connection[Any],
    ) -> PublishResult:
        """Commit ``staging_table``'s rows to this dataset's target table.

        ``conn`` carries an already-open transaction that this call must not
        commit or roll back itself: the engine owns the transaction
        boundary, so watermark and run-status updates land in the same
        transaction as the data (ARCHITECTURE.md Q3, README §28).

        Args:
            ctx: The current pipeline context.
            staging_table: The fully-qualified staging table this call reads
                from.
            conn: An open connection, inside an open transaction.

        Returns:
            The outcome of this publish call.
        """
        ...
