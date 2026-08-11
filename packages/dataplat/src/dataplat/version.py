"""Version resolution for the installed ``dataplat`` distribution.

Every loaded row must be attributable to the processor version that produced it,
so the version is read from installed distribution metadata rather than being
duplicated as a module-level literal that can drift from ``pyproject.toml``.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as distribution_version

DISTRIBUTION_NAME = "dataplat"
"""Name of the distribution whose metadata carries the platform version."""

UNKNOWN_VERSION = "0.0.0+unknown"
"""Sentinel returned when a distribution is not installed in the environment."""


def resolve_version(distribution: str = DISTRIBUTION_NAME) -> str:
    """Resolve the version of an installed distribution.

    Reads the version recorded in the installed distribution metadata. The
    lookup never raises: an absent distribution yields an explicit sentinel, so
    a caller recording provenance always has a value to record.

    Args:
        distribution: Name of the distribution to look up. Defaults to
            ``dataplat`` itself.

    Returns:
        The version string from the installed distribution metadata, or
        ``UNKNOWN_VERSION`` when no such distribution is installed.
    """
    try:
        return distribution_version(distribution)
    except PackageNotFoundError:
        return UNKNOWN_VERSION
