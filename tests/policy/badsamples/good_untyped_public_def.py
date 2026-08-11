# GOOD SAMPLE — the positive control for the untyped-definition check.
# Proves: mypy --strict is silent when the public function is fully annotated.
# Consumed by: tests/policy/test_gates_actually_fail.py::test_good_untyped_public_def_is_accepted
"""A library module exposing a fully annotated public function."""

from __future__ import annotations


def identity(value: str) -> str:
    """Return the value unchanged.

    Args:
        value: The value to return.

    Returns:
        The value, unchanged.
    """
    return value
