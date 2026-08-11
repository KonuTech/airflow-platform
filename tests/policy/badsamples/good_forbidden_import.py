# GOOD SAMPLE — the positive control for the import contract.
# Proves: the forbidden contract is KEPT when the core package does not import
# the plugin package, so "the contract is broken" is a statement about this
# import and not about the checker failing on everything.
# Consumed by: tests/policy/test_gates_actually_fail.py::test_good_forbidden_import_is_accepted
#
# Copied into a throwaway package inside tmp_path, exactly like its bad
# counterpart, so the only difference between the two runs is the import.
"""A core module that knows nothing about the plugin package."""

from __future__ import annotations

__all__ = ["describe"]


def describe() -> str:
    """Describe the core package without consulting the plugin.

    Returns:
        The core package's own name.
    """
    return "gatecheck_core"
