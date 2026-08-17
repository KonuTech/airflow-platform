"""``ReferentialIntegrityBarrier`` -- VALID-07's second concrete ``BarrierStage``.

Mirrors ``circuit_breaker.py``'s exact shape (a ``BarrierStage`` whose
``apply(ctx) -> StageResult`` takes no chunk parameter, runs once per run,
after every chunk has already been staged -- ``pipeline/protocol.py``'s own
docstring) and ``merge_orders.py``'s parameterized-identifier discipline
(T-04-01/T-08-15: every interpolated SQL fragment is a config/run-derived
identifier -- dataset name, numeric run id, or a fixed config-declared
column name -- never CSV row content).

D-16's text, proven here: an ``orders`` row whose ``customer_id`` has no
matching ``normalized.customers`` row is classified ``REFERENTIAL_ORPHAN``
and quarantined at the ROW level -- every other row in the same file still
publishes. This is also Pitfall 5's guard (08-RESEARCH.md): a race between
an ``orders`` file and a not-yet-loaded ``customers`` batch is a normal,
expected orphan case, never a whole-run/whole-file failure.

Deliberately single-dataset, matching ``OrdersMergePublisher``'s own
precedent (module docstring): the anti-join's SELECT list names
``customer_id``/``order_id`` literally, matching ``normalized.orders``'s
real business columns -- a generic "any staging table, any column" barrier
remains future work, not this plan's scope. ``target_table``/
``target_column``/``staging_column`` are still parameterized (not hardcoded
to ``normalized.customers``) so the JOIN condition stays config-driven even
though the SELECT list does not.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from psycopg.rows import dict_row

from dataplat.models.record import RecordChunk, RejectedRecord, StageResult
from dataplat.models.report import ValidationResult
from dataplat.pipeline.protocol import BarrierStage

if TYPE_CHECKING:
    from dataplat.pipeline.protocol import PipelineContext

# The four interpolated fragments below (`staging_table`, `target_table`,
# `target_column`, `staging_column`) are ALWAYS identifiers built from
# config/run identity -- `staging_table` from `ctx.config.dataset` + a
# numeric `run_id` (matching `merge.py`'s own T-04-01 precedent, cited
# verbatim here), `target_table`/`target_column`/`staging_column` from a
# dataset config's own `quality.rules[].column`/hardcoded target -- never
# from CSV row content. The query takes NO bound parameters: nothing here
# is a value.
_ANTI_JOIN_SQL = """
SELECT s.customer_id, s._source_row_number, s.order_id
FROM   {staging_table} s
LEFT JOIN {target_table} t
       ON s.{staging_column}::int = t.{target_column}
WHERE  t.{target_column} IS NULL
"""

_COUNT_SQL = "SELECT COUNT(*) AS total FROM {staging_table}"


class ReferentialIntegrityBarrier(BarrierStage):
    """Anti-joins a run's staged ``customer_id`` values against ``normalized.customers``.

    Every orphaned row (no matching ``target_column`` value in
    ``target_table``) becomes a ``REFERENTIAL_ORPHAN`` ``RejectedRecord``.
    Every non-orphan row is left entirely alone -- this stage never touches
    ``chunk``, it only reads and reports.
    """

    name = "referential_integrity_barrier"

    def __init__(  # noqa: PLR0913 -- one keyword per config-derived identifier, mirrors merge_orders.py's shape
        self,
        *,
        staging_table: str,
        target_table: str,
        target_column: str,
        staging_column: str,
        strategy: str,
        rule_id: str,
    ) -> None:
        """Configure which staging table this run's rows live in and what they're checked against.

        Args:
            staging_table: The fully-qualified staging table to read from,
                e.g. ``"staging.orders__r8123"``. Passed by the caller
                (plan 08-11's wiring), which already knows
                ``staging_result.staging_table`` at the point this barrier
                runs (Pattern 2's "after publisher.publish() ... staged
                rows already visible" note) -- interpolated as an
                identifier only, never a value.
            target_table: The fully-qualified table to check against, e.g.
                ``"normalized.customers"``.
            target_column: The column on ``target_table`` a staged row's
                value must match, e.g. ``"customer_id"``.
            staging_column: The column on ``staging_table`` holding the
                value to check, e.g. ``"customer_id"``.
            strategy: The dataset config's declared bad-record strategy for
                this rule (e.g. ``"QUARANTINE_RECORD"``). Stored for
                diagnostics/future strategy dispatch -- this plan always
                quarantines the orphan row regardless of this value (D-16's
                default), matching ``UniquenessRule``'s own "stored but not
                yet dispatched on" precedent.
            rule_id: Stable identifier of this rule instance, matching
                ``ValidationResult.rule_id`` and
                ``meta.validation_results.rule_id``.
        """
        self._staging_table = staging_table
        self._target_table = target_table
        self._target_column = target_column
        self._staging_column = staging_column
        self._strategy = strategy
        self._rule_id = rule_id

    def apply(self, ctx: PipelineContext) -> StageResult:
        """Anti-join this run's staged rows against ``target_table``, quarantining every orphan.

        Opens a NEW connection from ``ctx.db`` -- deliberately separate from
        the run's own staging connection, since a barrier reads live
        target-table state. The caller decides whether this runs before or
        after the publish transaction commits, per the wiring plan's own
        ordering requirements (Pattern 2, 08-11).

        Never raises for a row-level problem (QUAL-03): every orphan
        becomes a ``RejectedRecord`` instead of aborting the run -- this is
        also Pitfall 5's guard, a not-yet-arrived customer is an expected,
        row-level condition, never a whole-run failure.

        Args:
            ctx: The current pipeline context.

        Returns:
            A ``StageResult`` whose ``rejected`` names every orphan row
            (``error_type="REFERENTIAL_ORPHAN"``) and whose single
            ``findings`` entry records this barrier's own PASS/QUARANTINE
            outcome and counts.
        """
        with ctx.db.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                _ANTI_JOIN_SQL.format(
                    staging_table=self._staging_table,
                    target_table=self._target_table,
                    target_column=self._target_column,
                    staging_column=self._staging_column,
                )
            )
            orphan_rows = cur.fetchall()

            cur.execute(_COUNT_SQL.format(staging_table=self._staging_table))
            total_row = cur.fetchone()
            evaluated_count = int(total_row["total"]) if total_row is not None else 0

        rejected = [
            RejectedRecord(
                source_row_number=row["_source_row_number"],
                error_type="REFERENTIAL_ORPHAN",
                error_message=(
                    f"customer_id {row['customer_id']} not found in {self._target_table}"
                ),
                raw_line=str(row["order_id"]),
                error_column=self._staging_column,
                business_key=str(row["order_id"]),
            )
            for row in orphan_rows
        ]

        outcome = "QUARANTINE" if rejected else "PASS"
        placeholder_chunk = RecordChunk(rows=(), first_ordinal=0, expected_field_count=0)
        return StageResult(
            chunk=placeholder_chunk,
            rejected=rejected,
            findings=[
                ValidationResult(
                    rule_id=self._rule_id,
                    rule_type="REFERENTIAL",
                    severity="ERROR",
                    outcome=outcome,
                    evaluated_count=evaluated_count,
                    failed_count=len(rejected),
                    message=(
                        f"{len(rejected)} orphan row(s) with no matching "
                        f"{self._target_column!r} in {self._target_table}"
                        if rejected
                        else "no referential orphans found"
                    ),
                    threshold={},
                    observed={"orphan_count": len(rejected)},
                )
            ],
        )
