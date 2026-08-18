"""``Publisher`` — how a staging table's rows are committed to their target.

This phase defines the protocol only. ``merge`` (README §36's default
publication strategy) arrives in Phase 4; SCD/CDC publishers arrive in Phase
10 (03-CONTEXT.md, deferred section). ``PublishResult`` is kept deliberately
minimal here — ``rows_affected``/``outcome`` — because the richer
``MERGE ... RETURNING merge_action()`` shape (STACK.md, PostgreSQL 17) is
Phase 4's ``merge`` Publisher's job to populate, not this protocol's to
over-specify before a single concrete ``Publisher`` exists.

``published_business_keys`` (CR-01, phase-08 code review) was added once a
concrete correctness bug surfaced: a business key that merely *staged*
successfully is not the same as one this publish call actually
inserted/updated. Both concrete ``Publisher``s (``merge.py``/
``merge_orders.py``) have a conflict-guard ``WHERE`` clause on their
``ON CONFLICT DO UPDATE`` that can leave a conflicting row "locked but
unchanged" -- excluded from ``rows_affected`` -- whenever the incoming
staged row does not actually improve on what's already published. Callers
that need to know which business keys THIS publish call actually affected
(e.g. ``run.py``'s post-publish reject-resolution barrier) must use
``published_business_keys``, never a blind read of the staging table.
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
        published_business_keys: The business-key values this publish call
            ACTUALLY inserted or updated in the target table (i.e. every row
            the ``ON CONFLICT DO UPDATE ... WHERE`` guard did not silently
            leave "locked but unchanged"), as strings, deduplicated. Distinct
            from -- and always a subset of -- whatever business keys merely
            survived streaming validation and landed in the staging table.
            Defaults to an empty tuple for any ``Publisher`` that has not
            been updated to populate it.
    """

    rows_affected: int
    outcome: str
    published_business_keys: tuple[str, ...] = ()


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
        source_table: str,
        conn: Connection[Any],
    ) -> PublishResult:
        """Commit ``source_table``'s rows to this dataset's target table.

        ``conn`` carries an already-open transaction that this call must not
        commit or roll back itself: the engine owns the transaction
        boundary, so watermark and run-status updates land in the same
        transaction as the data (ARCHITECTURE.md Q3, README §28).

        Args:
            ctx: The current pipeline context.
            source_table: The fully-qualified table this call reads from --
                a per-run scratch staging table before plan 08.1-10,
                ``silver.<dataset>`` from plan 08.1-10 onward.
            conn: An open connection, inside an open transaction.

        Returns:
            The outcome of this publish call.
        """
        ...
