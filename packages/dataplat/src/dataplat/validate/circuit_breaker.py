"""``RejectionRateCircuitBreaker`` -- D-10's run-level rejection-rate threshold.

This codebase's FIRST concrete ``BarrierStage`` (``pipeline/protocol.py``'s
``BarrierStage.apply(self, ctx: PipelineContext) -> StageResult``, no chunk
parameter -- runs once per run, after every chunk has already been staged).

This stage does NOT itself count rows -- ``BarrierStage.apply()`` takes no
parameter beyond ``ctx``, and ``PipelineContext`` has no row-count field, so
the run's own totals (``StagingResult.rows_read``/``.rows_rejected``) are
threaded in at CONSTRUCTION time instead. The caller (plan 08-11's
``pipeline/run.py`` wiring) constructs a fresh
``RejectionRateCircuitBreaker`` per run, after ``StagingLoader.load()`` has
already returned its totals -- this stage is a pure, already-parameterized
threshold check, not a stateful accumulator, and never re-derives its totals
from ``ctx``.

Raising ``QualityThresholdExceeded`` here is the actual mechanism that makes
D-11 ("nothing publishes on FAIL") real -- this is the one place that
exception is actually raised; a later plan (08-11) wires the raise into a
live publication transaction so it rolls the whole thing back.

T-08-14 (accepted): a misconfigured ``rejection_rate_threshold`` of ``0.0``
on a dataset with any imperfect data is a deliberate, developer-configured
strictness choice (D-10 makes the threshold dataset-configurable), not a
platform defect.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dataplat.errors import QualityThresholdExceeded
from dataplat.models.record import RecordChunk, StageResult
from dataplat.models.report import ValidationResult
from dataplat.pipeline.protocol import BarrierStage

if TYPE_CHECKING:
    from dataplat.pipeline.protocol import PipelineContext


class RejectionRateCircuitBreaker(BarrierStage):
    """Raises ``QualityThresholdExceeded`` when a run's rejection rate breaches its threshold.

    One instance is constructed per run, already parameterized with that
    run's own totals -- see the module docstring for why ``apply()`` never
    reads row counts from ``ctx``.
    """

    name = "rejection_rate_circuit_breaker"

    def __init__(
        self,
        *,
        threshold: float,
        total_rows_read: int,
        total_rows_rejected: int,
        rule_id: str = "rejection_rate_circuit_breaker",
    ) -> None:
        """Configure this run's rejection-rate threshold and already-known totals.

        Args:
            threshold: The dataset's configured
                ``quality.rejection_rate_threshold`` (D-10), e.g. ``0.10``
                for 10%.
            total_rows_read: The total number of rows this run read, across
                every chunk (``StagingResult.rows_read``).
            total_rows_rejected: The total number of rows this run rejected,
                across every chunk (``StagingResult.rows_rejected``).
            rule_id: Stable identifier for this rule instance, for
                diagnostics and the resulting ``ValidationResult``.
        """
        self._threshold = threshold
        self._total_rows_read = total_rows_read
        self._total_rows_rejected = total_rows_rejected
        self._rule_id = rule_id

    def apply(self, ctx: PipelineContext) -> StageResult:
        """Evaluate this run's aggregate rejection rate against its threshold.

        Args:
            ctx: The current pipeline context. Unused for row counts (see
                module docstring) -- present only to satisfy the
                ``BarrierStage`` Protocol.

        Returns:
            A trivial-PASS ``StageResult`` when ``total_rows_read == 0``
            (an empty file can never breach) or when the observed ratio is
            at or below ``threshold``.

        Raises:
            QualityThresholdExceeded: The observed rejected/total ratio
                exceeds ``threshold``. ``context`` names the observed ratio
                and configured threshold (D-10).
        """
        del ctx  # unused -- totals come from the constructor, see module docstring
        placeholder_chunk = RecordChunk(rows=(), first_ordinal=0, expected_field_count=0)

        if self._total_rows_read == 0:
            return StageResult(
                chunk=placeholder_chunk,
                rejected=[],
                findings=[
                    ValidationResult(
                        rule_id=self._rule_id,
                        rule_type="QUALITY",
                        severity="ERROR",
                        outcome="PASS",
                        evaluated_count=0,
                        failed_count=0,
                        message="no rows read; rejection rate trivially within threshold",
                        threshold={"rejection_rate_threshold": self._threshold},
                        observed={"ratio": 0.0},
                    )
                ],
            )

        ratio = self._total_rows_rejected / self._total_rows_read
        if ratio > self._threshold:
            msg = f"rejection rate {ratio:.2%} exceeds configured threshold {self._threshold:.2%}"
            raise QualityThresholdExceeded(
                msg,
                context={
                    "rule_id": self._rule_id,
                    "observed_ratio": ratio,
                    "threshold": self._threshold,
                    "total_rows_read": self._total_rows_read,
                    "total_rows_rejected": self._total_rows_rejected,
                },
            )

        return StageResult(
            chunk=placeholder_chunk,
            rejected=[],
            findings=[
                ValidationResult(
                    rule_id=self._rule_id,
                    rule_type="QUALITY",
                    severity="ERROR",
                    outcome="PASS",
                    evaluated_count=self._total_rows_read,
                    failed_count=self._total_rows_rejected,
                    message="rejection rate within threshold",
                    threshold={"rejection_rate_threshold": self._threshold},
                    observed={"ratio": ratio},
                )
            ],
        )
