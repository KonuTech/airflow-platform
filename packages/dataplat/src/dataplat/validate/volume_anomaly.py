"""``VolumeAnomalyBarrier`` -- VALID-09's minimal, resolved-scope slice.

Mirrors ``circuit_breaker.py``/``referential.py``'s exact shape (a
``BarrierStage`` whose ``apply(ctx) -> StageResult`` takes no chunk
parameter, runs once per run, after every chunk has already been staged).

Phase 8's resolved scope for VALID-09 is deliberately narrow: persist a
``row_count`` metric per run (a ``VOLUME`` ``rule_type`` row on
``meta.validation_results``) and one plain SQL comparison against
``avg(historical row_count) * multiplier`` -- no forecasting, no ML, no
historical-trend analysis (that remains Phase 9's VALID-05/06 territory).

Self-referential by design: every ``ValidationResult`` this barrier's
``findings`` produces has ``rule_type="VOLUME"`` and ``evaluated_count`` set
to the CURRENT run's own row count. Once persisted (plan 08-11's wiring),
that same row becomes a LATER run's own historical-average input -- there is
no separate "write the row_count metric" code path; the anomaly-comparison
output IS the persisted history.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dataplat.models.record import RecordChunk, StageResult
from dataplat.models.report import ValidationResult
from dataplat.pipeline.protocol import BarrierStage

if TYPE_CHECKING:
    from collections.abc import Callable

    from dataplat.pipeline.protocol import PipelineContext

# `dataset_id` is ALWAYS bound via a `%s` placeholder -- never string-
# formatted into this query (T-08-17). `evaluated_count` on a prior VOLUME
# row IS that prior run's own row count, by this same class's own writing
# convention (module docstring) -- self-referential by design, matching how
# a moving average is meant to work.
_HISTORICAL_AVERAGE_SQL = """
SELECT AVG(vr.evaluated_count), COUNT(*)
FROM   meta.validation_results vr
JOIN   meta.ingestion_runs ir ON vr.run_id = ir.run_id
WHERE  ir.dataset_id = %s
AND    vr.rule_type = 'VOLUME'
AND    ir.status = 'SUCCEEDED'
"""

# Cold start: fewer than this many prior SUCCEEDED VOLUME rows means there is
# no meaningful historical baseline yet -- a structural PASS, never a false
# positive (this plan's own second must_haves.truths line).
_MIN_PRIOR_RUNS_FOR_COMPARISON = 2

# D-07's per-rule-type-strategy framing, matching circuit_breaker.py/
# referential.py's own "strategy stored, small local outcome mapping"
# precedent.
_STRATEGY_TO_OUTCOME = {
    "QUARANTINE_FILE": "QUARANTINE",
    "QUARANTINE_RECORD": "QUARANTINE",
    "FAIL_FILE": "FAIL",
    "WARN_AND_CONTINUE": "PASS_WITH_WARNING",
}


class VolumeAnomalyBarrier(BarrierStage):
    """Flags a run whose row count is an outlier against its dataset's persisted history.

    ``current_row_count > avg(historical row_count) * multiplier`` is the
    entire anomaly rule -- a plain SQL comparison against already-persisted
    ``meta.validation_results``/``meta.ingestion_runs`` rows, never a
    Python-side historical-trend model.
    """

    name = "volume_anomaly_barrier"

    def __init__(  # noqa: PLR0913 -- one keyword per config-derived value, mirrors referential.py's shape
        self,
        *,
        ctx_db_query: Callable[[], tuple[float | None, int]] | None = None,
        dataset_id: int,
        current_row_count: int,
        multiplier: float,
        rule_id: str,
        strategy: str,
    ) -> None:
        """Configure this run's own row count and the dataset it is compared against.

        Args:
            ctx_db_query: Testing seam only. When provided, ``apply()``
                calls this instead of issuing the real
                ``_HISTORICAL_AVERAGE_SQL`` query against ``ctx.db`` --
                lets unit tests exercise the anomaly/cold-start arithmetic
                without a live PostgreSQL connection. Left ``None`` in every
                real caller (the real per-run query is always issued in that
                case).
            dataset_id: The dataset this run's row count is compared
                against, via ``meta.ingestion_runs.dataset_id``.
            current_row_count: This run's own row count -- both the value
                being checked AND, via this barrier's own
                ``evaluated_count`` output, the metric a LATER run's
                historical average will include once persisted.
            multiplier: The anomaly threshold multiplier, e.g. ``10.0`` for
                "10x historical average".
            rule_id: Stable identifier of this rule instance, matching
                ``ValidationResult.rule_id`` and
                ``meta.validation_results.rule_id``.
            strategy: The dataset config's declared bad-record strategy for
                this rule (e.g. ``"QUARANTINE_FILE"``), mapped to an
                ``outcome`` via ``_STRATEGY_TO_OUTCOME``.
        """
        self._ctx_db_query = ctx_db_query
        self._dataset_id = dataset_id
        self._current_row_count = current_row_count
        self._multiplier = multiplier
        self._rule_id = rule_id
        self._strategy = strategy

    def apply(self, ctx: PipelineContext) -> StageResult:
        """Compare this run's row count against its dataset's persisted historical average.

        Never raises for a row-level/run-level problem (QUAL-03): an
        anomalous run is reported via the configured ``strategy``'s mapped
        ``outcome`` on the returned ``findings`` entry, never an exception.

        Args:
            ctx: The current pipeline context. ``ctx.db`` is queried
                directly unless ``ctx_db_query`` was supplied at
                construction (testing seam).

        Returns:
            A ``StageResult`` with no ``rejected`` rows (this barrier never
            classifies individual rows) and a single ``findings`` entry
            (``rule_type="VOLUME"``) recording the comparison outcome.
        """
        if self._ctx_db_query is not None:
            historical_average, prior_run_count = self._ctx_db_query()
        else:
            with ctx.db.connection() as conn, conn.cursor() as cur:
                cur.execute(_HISTORICAL_AVERAGE_SQL, (self._dataset_id,))
                row = cur.fetchone()
                historical_average = (
                    float(row[0]) if row is not None and row[0] is not None else None
                )
                prior_run_count = int(row[1]) if row is not None else 0

        placeholder_chunk = RecordChunk(rows=(), first_ordinal=0, expected_field_count=0)

        if prior_run_count < _MIN_PRIOR_RUNS_FOR_COMPARISON:
            return StageResult(
                chunk=placeholder_chunk,
                rejected=[],
                findings=[
                    ValidationResult(
                        rule_id=self._rule_id,
                        rule_type="VOLUME",
                        severity="WARNING",
                        outcome="PASS",
                        evaluated_count=self._current_row_count,
                        failed_count=0,
                        message=(
                            f"only {prior_run_count} prior SUCCEEDED VOLUME run(s) for "
                            "this dataset -- no meaningful historical baseline yet, "
                            "skipping comparison"
                        ),
                        threshold={"multiplier": self._multiplier},
                        observed={
                            "historical_average": None,
                            "prior_run_count": prior_run_count,
                        },
                    )
                ],
            )

        assert historical_average is not None  # noqa: S101 -- prior_run_count >= 2 guarantees AVG is non-NULL

        if self._current_row_count > historical_average * self._multiplier:
            outcome = _STRATEGY_TO_OUTCOME.get(self._strategy, "FAIL")
            return StageResult(
                chunk=placeholder_chunk,
                rejected=[],
                findings=[
                    ValidationResult(
                        rule_id=self._rule_id,
                        rule_type="VOLUME",
                        severity="ERROR",
                        outcome=outcome,
                        evaluated_count=self._current_row_count,
                        failed_count=1,
                        message=(
                            f"row count {self._current_row_count} exceeds "
                            f"{self._multiplier}x historical average "
                            f"{historical_average}"
                        ),
                        threshold={"multiplier": self._multiplier},
                        observed={
                            "current_row_count": self._current_row_count,
                            "historical_average": historical_average,
                            "multiplier": self._multiplier,
                        },
                    )
                ],
            )

        return StageResult(
            chunk=placeholder_chunk,
            rejected=[],
            findings=[
                ValidationResult(
                    rule_id=self._rule_id,
                    rule_type="VOLUME",
                    severity="ERROR",
                    outcome="PASS",
                    evaluated_count=self._current_row_count,
                    failed_count=0,
                    message="row count within historical bounds",
                    threshold={"multiplier": self._multiplier},
                    observed={
                        "current_row_count": self._current_row_count,
                        "historical_average": historical_average,
                        "multiplier": self._multiplier,
                    },
                )
            ],
        )
