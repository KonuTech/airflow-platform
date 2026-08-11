# BAD SAMPLE — deliberately broken. Do not "fix" this file.
# Trips: ruff T201 (`print` found) — the OBS-03 console-write ban.
# Consumed by: tests/policy/test_gates_actually_fail.py::test_print_in_library_is_rejected
# Excluded from the main ruff/mypy runs by pyproject.toml; removing that
# exclusion makes `make lint` permanently red.
"""A library module that writes to the console instead of logging."""

from __future__ import annotations


def emit(value: str) -> None:
    """Emit a value.

    Args:
        value: The value to emit.
    """
    print(value)
