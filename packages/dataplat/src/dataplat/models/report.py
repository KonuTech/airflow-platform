"""Row/run-level validation findings — the real, phase-8 shape backing `meta.validation_results`.

This was CONTEXT.md D-05's originally "minimal" shape (``rule_id``/``outcome``/
``message`` only, no live caller constructing one). Phase 8's own
``meta.validation_results`` DDL (migration 0014) and this widened
``ValidationResult`` are that phase's job — every field below has a direct,
same-named column on that table, so a ``MetadataRepository.
record_validation_results()`` call can persist a list of these with no
reshaping in between.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """One validation rule's outcome against a chunk, row, or run.

    Attributes:
        rule_id: Stable identifier of the validation rule that produced this
            result.
        rule_type: The rule's category — one of ``"FILE"``, ``"STRUCTURAL"``,
            ``"SCHEMA"``, ``"TYPE"``, ``"QUALITY"``, ``"REFERENTIAL"``,
            ``"VOLUME"``. Deliberately a plain string, not an enum — see the
            module docstring and this repo's "config not code, strings not
            enums" convention (``config/model.py``'s own documented
            rationale).
        severity: The rule's configured severity for this outcome.
        outcome: The rule's outcome, e.g. ``"PASS"``, ``"FAIL"``,
            ``"WARNING"``. Deliberately a plain string, not an enum — see the
            module docstring.
        message: A human-readable description of the outcome.
        evaluated_count: The number of rows/records this rule evaluated.
        failed_count: The number of rows/records that failed this rule.
        threshold: The rule's configured threshold value(s), when
            applicable — e.g. ``{"min": 0, "max": 100}`` for a
            validity-range rule. Empty when the rule has no threshold
            concept.
        observed: The actually-observed value(s) this rule measured, when
            applicable — e.g. ``{"failed_ratio": 0.03}``. Empty when the
            rule has no observed-value concept.
    """

    rule_id: str
    outcome: str
    message: str
    rule_type: str
    severity: str
    evaluated_count: int
    failed_count: int
    threshold: dict[str, object] = field(default_factory=dict)
    observed: dict[str, object] = field(default_factory=dict)
