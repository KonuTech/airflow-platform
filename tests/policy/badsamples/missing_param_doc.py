# BAD SAMPLE — deliberately broken. Do not "fix" this file.
# Trips: ruff D417 (Missing argument description in the docstring) — QUAL-02's
# parameter half. Known limit, recorded in the consuming test: D417 fires only
# when an `Args:` section EXISTS and omits a parameter. A docstring with no
# `Args:` section at all is not flagged by any rule.
# Consumed by: tests/policy/test_gates_actually_fail.py::test_missing_param_doc_is_rejected
# Excluded from the main ruff/mypy runs by pyproject.toml.
"""A library module documenting only one of two parameters."""

from __future__ import annotations


def combine(prefix: str, suffix: str) -> str:
    """Join two fragments.

    Args:
        prefix: The leading fragment.

    Returns:
        The two fragments concatenated.
    """
    return prefix + suffix
