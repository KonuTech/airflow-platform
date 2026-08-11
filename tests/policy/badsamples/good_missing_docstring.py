# GOOD SAMPLE — the positive control for the docstring rule.
# Proves: ruff D103 is silent when the public function is documented.
# Consumed by: tests/policy/test_gates_actually_fail.py::test_good_missing_docstring_is_accepted
"""A library module whose public function carries a docstring."""

from __future__ import annotations


def normalise(value: str) -> str:
    """Strip surrounding whitespace from a value.

    Args:
        value: The value to normalise.

    Returns:
        The value without leading or trailing whitespace.
    """
    return value.strip()
