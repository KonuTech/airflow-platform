"""``StrategyDispatchStage`` -- the generic D-07 per-rule-type strategy-outcome wrapper.

This is the concrete landing point 08-04's/08-07's ``CompletenessRule``/
``ValidityRangeRule``/``PatternRule``/``UniquenessRule`` docstrings all name
("strategy-based branching lands in plan 08-10"): a single, reusable
``StreamingStage`` decorator that wraps any other ``StreamingStage`` rule and
turns its stored-but-inert ``strategy`` value into a genuine behavioral
difference, without every rule class re-implementing the same branching
logic itself.

D-07's five ``strategy`` values, and this stage's exact outcome for each
(the concrete design decision this plan's own ``<interfaces>`` section
documents verbatim):

- ``REJECT_RECORD`` / ``QUARANTINE_RECORD``: pass the wrapped rule's own
  ``StageResult`` straight through, unmodified. The violating row is
  excluded from what publishes -- existing detection behavior from
  08-04/08-07, unchanged. Both strategy values produce the SAME
  streaming-stage-level outcome in this phase: there is no
  "quarantine vs. hard-reject" distinction expressible below the level of
  ``meta.rejected_records`` itself -- both land the row there, excluded
  from the target table, backfillable per D-01.

- ``WARN_AND_CONTINUE``: the wrapped rule's rejected rows are NOT excluded
  -- this stage returns the ORIGINAL, untouched chunk (every row kept,
  including ones the inner rule flagged) plus one ``ValidationResult``
  finding (``outcome="PASS_WITH_WARNING"``) summarizing the violation count
  for this chunk. The row genuinely proceeds to publish.

- ``FAIL_FILE`` / ``QUARANTINE_FILE``: any violation the wrapped rule found
  in this chunk immediately raises ``dataplat.errors.QualityThresholdExceeded``
  -- the SAME exception class the run-level circuit breaker
  (``RejectionRateCircuitBreaker``, plan 08-07) already raises for D-11's
  "nothing publishes" guarantee. Raised here, during STAGING (before the
  publish transaction in ``pipeline/run.py`` is ever opened -- staging runs
  on its own connection, outside that transaction), this is an even more
  direct satisfaction of D-11 than the circuit breaker's mid-transaction
  rollback: nothing has been staged-and-ready-to-publish yet, so there is
  nothing to roll back. This phase's architecture has no "skip this file,
  try the next one" loop inside ``run_ingest`` (D-01: one file -> one run),
  so ``FAIL_FILE`` and ``QUARANTINE_FILE`` deliberately produce the
  IDENTICAL concrete outcome here -- both abort the run before publish, and
  the run-fatal-exception contract already documented in ``pipeline/run.py``'s
  own module docstring ("this function catches nothing") propagates it to
  the CLI call site, which marks the run FAILED exactly as a circuit-breaker
  trip would. This is an honest, deliberate simplification -- no
  differentiated file-level skip exists yet, not an oversight.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dataplat.errors import ConfigurationError, QualityThresholdExceeded
from dataplat.models.record import StageResult
from dataplat.models.report import ValidationResult
from dataplat.pipeline.protocol import StreamingStage

if TYPE_CHECKING:
    from dataplat.models.record import RecordChunk
    from dataplat.pipeline.protocol import PipelineContext

# D-07's closed 5-value strategy set. Validated at construction time
# (`__init__`), never silently defaulted at `apply()` time -- mirrors
# `resolve_validation_rule`'s own fail-fast-at-construction convention.
_KNOWN_STRATEGIES: frozenset[str] = frozenset(
    {
        "REJECT_RECORD",
        "QUARANTINE_RECORD",
        "WARN_AND_CONTINUE",
        "FAIL_FILE",
        "QUARANTINE_FILE",
    },
)

# The two strategies that produce a pure pass-through of the wrapped rule's
# own StageResult -- both exclude the violating row identically.
_PASSTHROUGH_STRATEGIES: frozenset[str] = frozenset({"REJECT_RECORD", "QUARANTINE_RECORD"})

# The two strategies that escalate to a run-fatal QualityThresholdExceeded --
# deliberately identical outcomes in this phase (see module docstring).
_ESCALATING_STRATEGIES: frozenset[str] = frozenset({"FAIL_FILE", "QUARANTINE_FILE"})


class StrategyDispatchStage(StreamingStage):
    """Wraps any ``StreamingStage`` rule, turning its ``strategy`` into a real outcome.

    A decorator, not a new stage type: implements the exact same
    ``StreamingStage`` protocol the wrapped rule itself implements, so
    ``StagingLoader._build_stages`` can treat a wrapped rule identically to
    an unwrapped one (append to the same stage list, call ``.apply()`` the
    same way).
    """

    def __init__(
        self,
        *,
        inner: StreamingStage,
        strategy: str,
        rule_id: str,
        rule_type: str,
    ) -> None:
        """Configure which rule this stage wraps and which D-07 strategy governs its outcome.

        Args:
            inner: The wrapped rule -- any ``StreamingStage`` (e.g. a
                ``CompletenessRule``/``ValidityRangeRule``/``PatternRule``/
                ``UniquenessRule`` instance).
            strategy: The dataset config's declared bad-record strategy for
                this rule, one of D-07's five values. Validated here, at
                construction time.
            rule_id: The dataset config's stable identifier for this rule
                instance -- carried into the ``WARN_AND_CONTINUE`` finding
                and the ``FAIL_FILE``/``QUARANTINE_FILE`` exception context.
            rule_type: The wrapped rule's own category (e.g.
                ``"QUALITY_COMPLETENESS"``) -- carried into the same two
                places as ``rule_id``.

        Raises:
            ConfigurationError: ``strategy`` is not one of D-07's five known
                values -- fails fast at construction, never silently falls
                through to a default at ``apply()`` time.
        """
        if strategy not in _KNOWN_STRATEGIES:
            msg = f"unknown strategy {strategy!r}"
            raise ConfigurationError(
                msg,
                context={
                    "strategy": strategy,
                    "rule_id": rule_id,
                    "known": sorted(_KNOWN_STRATEGIES),
                },
            )
        self._inner = inner
        self._strategy = strategy
        self._rule_id = rule_id
        self._rule_type = rule_type

    @property
    def name(self) -> str:
        """This stage's identifier, tracing back to the wrapped rule's own name."""
        return f"strategy_dispatch[{self._inner.name}]"

    def apply(self, ctx: PipelineContext, chunk: RecordChunk) -> StageResult:
        """Apply the wrapped rule, then branch on this stage's configured D-07 strategy.

        Never raises when the wrapped rule found no violation this chunk --
        a strict strategy (``FAIL_FILE``/``QUARANTINE_FILE``) only escalates
        on an ACTUAL violation, never unconditionally.

        Args:
            ctx: The current pipeline context, passed through to the
                wrapped rule's own ``apply()``.
            chunk: The chunk to process, passed through to the wrapped
                rule's own ``apply()``.

        Returns:
            The wrapped rule's own ``StageResult`` (``REJECT_RECORD``/
            ``QUARANTINE_RECORD``, or no violation this chunk under any
            strategy), or a ``StageResult`` with every row kept plus one
            warning finding (``WARN_AND_CONTINUE``).

        Raises:
            QualityThresholdExceeded: ``strategy`` is ``FAIL_FILE`` or
                ``QUARANTINE_FILE`` and the wrapped rule rejected at least
                one row in this chunk.
        """
        result = self._inner.apply(ctx, chunk)
        if not result.rejected:
            return result

        if self._strategy in _PASSTHROUGH_STRATEGIES:
            return result

        if self._strategy == "WARN_AND_CONTINUE":
            violation_count = len(result.rejected)
            warning_finding = ValidationResult(
                rule_id=self._rule_id,
                rule_type=self._rule_type,
                severity="WARNING",
                outcome="PASS_WITH_WARNING",
                evaluated_count=len(chunk.rows),
                failed_count=violation_count,
                message=(
                    f"{violation_count} row(s) violated {self._rule_id} but "
                    "strategy=WARN_AND_CONTINUE kept them"
                ),
                threshold={},
                observed={"violation_count": violation_count},
            )
            # `chunk` here is the ORIGINAL, untouched chunk passed into this
            # method -- never `result.chunk` (which already excluded the
            # violating rows) -- this is what makes the row genuinely
            # proceed to publish.
            return StageResult(chunk=chunk, rejected=[], findings=[*result.findings, warning_finding])

        # `self._strategy` in _ESCALATING_STRATEGIES (FAIL_FILE/QUARANTINE_FILE)
        # -- the only remaining branch, since `__init__` already rejected any
        # value outside `_KNOWN_STRATEGIES`.
        violation_count = len(result.rejected)
        msg = (
            f"strategy={self._strategy} rule {self._rule_id} rejected "
            f"{violation_count} row(s) in this chunk -- escalating to run failure "
            "before publish"
        )
        raise QualityThresholdExceeded(
            msg,
            context={
                "rule_id": self._rule_id,
                "rule_type": self._rule_type,
                "strategy": self._strategy,
                "violation_count": violation_count,
            },
        )
