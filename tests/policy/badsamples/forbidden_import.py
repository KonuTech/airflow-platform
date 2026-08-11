# BAD SAMPLE — deliberately broken. Do not "fix" this file.
# Trips: import-linter forbidden contract — the core package importing the
# plugin package. Mirrors setup.cfg contract 1 ("dataplat core must not depend
# on the CSV plugin"), README §6.4 / §68.
# Consumed by: tests/policy/test_gates_actually_fail.py::test_forbidden_import_is_rejected
#
# This file is never imported by the test suite. It is COPIED into a throwaway
# package inside tmp_path and analysed there, so the repository's own contract
# file is never mutated to produce a failure.
# Excluded from the main ruff/mypy runs by pyproject.toml.
"""A core module that reaches into the plugin package it must not know about."""

from __future__ import annotations

import gatecheck_plugin

__all__ = ["describe"]


def describe() -> str:
    """Describe the plugin from inside the core package.

    Returns:
        The plugin's name.
    """
    return gatecheck_plugin.NAME
