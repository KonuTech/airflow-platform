"""Shared pytest fixtures.

The repository root is resolved once, from this file's own location, so a policy
test never depends on the working directory pytest happened to be started from.
"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def repo_root() -> Path:
    """Return the absolute path of the repository root."""
    return REPO_ROOT
