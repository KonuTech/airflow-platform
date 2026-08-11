# BAD SAMPLE — deliberately broken. Do not "fix" this file.
# Trips: ruff D103 (Missing docstring in public function) — QUAL-02.
# Consumed by: tests/policy/test_gates_actually_fail.py::test_missing_docstring_is_rejected
# Excluded from the main ruff/mypy runs by pyproject.toml.
"""A library module whose public function carries no docstring."""

from __future__ import annotations


def normalise(value: str) -> str:
    return value.strip()
