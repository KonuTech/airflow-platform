"""Row-level validation findings — the minimal D-05 shape.

This is CONTEXT.md D-05's explicitly "minimal" shape. Phase 8's
``meta.validation_results`` DDL and a richer, enum-typed ``ValidationResult``
are that phase's job, not this one's — keeping ``outcome`` a plain ``str`` now
avoids a premature enum this phase would only have to widen later.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """One validation rule's outcome against a chunk, row, or run.

    Attributes:
        rule_id: Stable identifier of the validation rule that produced this
            result.
        outcome: The rule's outcome, e.g. ``"PASS"``, ``"FAIL"``,
            ``"WARNING"``. Deliberately a plain string, not an enum — see the
            module docstring.
        message: A human-readable description of the outcome.
    """

    rule_id: str
    outcome: str
    message: str
