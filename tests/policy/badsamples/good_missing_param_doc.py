# GOOD SAMPLE — the positive control for the undocumented-parameter rule.
# Proves: ruff D417 is silent when the `Args:` section names every parameter.
# Consumed by: tests/policy/test_gates_actually_fail.py::test_good_missing_param_doc_is_accepted
"""A library module documenting both of its parameters."""

from __future__ import annotations


def combine(prefix: str, suffix: str) -> str:
    """Join two fragments.

    Args:
        prefix: The leading fragment.
        suffix: The trailing fragment.

    Returns:
        The two fragments concatenated.
    """
    return prefix + suffix
