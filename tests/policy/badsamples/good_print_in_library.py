# GOOD SAMPLE — the positive control for the console-write ban.
# Proves: ruff T201 is silent on a library module that logs rather than prints.
# Consumed by: tests/policy/test_gates_actually_fail.py::test_good_print_in_library_is_accepted
"""A library module that logs instead of writing to the console."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def emit(value: str) -> None:
    """Emit a value.

    Args:
        value: The value to emit.
    """
    logger.info("%s", value)
