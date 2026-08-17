"""``VALIDATION_RULE_REGISTRY`` -- resolves a dataset config's ``rule_type`` key to a rule class.

Mirrors ``dataplat.load.publish.registry.PUBLISHER_REGISTRY``'s exact shape
and rationale: a plain module-level dict, no entry-points machinery, for a
registry whose growth (five rule families total, VALID-02's DoD-mandated
minimum set) is small and known in advance.

One structural difference from ``PUBLISHER_REGISTRY``: this registry maps to
CLASSES, not instances (``PUBLISHER_REGISTRY`` maps to a shared, stateless
``MergePublisher()`` singleton) -- because every rule here takes per-rule
configuration (``column_index``, ``strategy``, bounds, pattern, ...) at
construction, so the registry can only resolve WHICH class to instantiate,
never a ready-made instance.

This is the single, append-only home every later rule plan (08-07's
uniqueness, 08-08's referential integrity, 08-09's volume anomaly) adds its
own key to -- never a parallel registry.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dataplat.errors import ConfigurationError
from dataplat.pipeline.engine import RaggedRowGuard
from dataplat.validate.circuit_breaker import RejectionRateCircuitBreaker
from dataplat.validate.completeness import CompletenessRule
from dataplat.validate.pattern import PatternRule
from dataplat.validate.validity_range import ValidityRangeRule

if TYPE_CHECKING:
    from dataplat.pipeline.protocol import BarrierStage, StreamingStage

VALIDATION_RULE_REGISTRY: dict[str, type[StreamingStage | BarrierStage]] = {
    "STRUCTURAL": RaggedRowGuard,
    "QUALITY_COMPLETENESS": CompletenessRule,
    "QUALITY_VALIDITY_RANGE": ValidityRangeRule,
    "QUALITY_PATTERN": PatternRule,
    "CIRCUIT_BREAKER": RejectionRateCircuitBreaker,
}


def resolve_validation_rule(rule_type: str) -> type[StreamingStage | BarrierStage]:
    """Resolve a ``configs/datasets/*.yaml`` ``rule_type`` key to its rule class.

    Mirrors ``resolve_publisher``'s exact shape (``dataplat.load.publish.registry``).

    Args:
        rule_type: The rule type key to resolve, e.g. ``"STRUCTURAL"`` or
            ``"QUALITY_COMPLETENESS"``.

    Returns:
        The registered rule CLASS for ``rule_type`` (not an instance --
        callers construct it with their own per-rule configuration).

    Raises:
        ConfigurationError: ``rule_type`` names no registry entry.
    """
    try:
        return VALIDATION_RULE_REGISTRY[rule_type]
    except KeyError:
        msg = (
            "a config names a validation rule_type key that has no registry "
            f"entry: {rule_type!r}"
        )
        raise ConfigurationError(
            msg,
            context={"rule_type": rule_type, "known": sorted(VALIDATION_RULE_REGISTRY)},
        ) from None
