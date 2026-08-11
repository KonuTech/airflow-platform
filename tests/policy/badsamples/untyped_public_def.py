# BAD SAMPLE — deliberately broken. Do not "fix" this file.
# Trips: mypy no-untyped-def ("Function is missing a type annotation") under
# `strict = true` — QUAL-01 / CICD-04.
# Consumed by: tests/policy/test_gates_actually_fail.py::test_untyped_public_def_is_rejected
# Excluded from the main ruff/mypy runs by pyproject.toml.
"""A library module exposing an unannotated public function."""


def identity(value):
    """Return the value unchanged.

    Args:
        value: The value to return.

    Returns:
        The value, unchanged.
    """
    return value
