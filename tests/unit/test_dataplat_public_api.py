"""The first real unit test: dataplat's public API returns real values.

This exists so `make test` collects at least one test from the first commit and
never exits 5 (no tests collected), which pytest reports as a distinct exit code
that a naive `&&` chain would treat as failure.
"""

from __future__ import annotations

import dataplat
from dataplat import UNKNOWN_VERSION, resolve_version


def test_resolve_version_returns_the_installed_version() -> None:
    assert resolve_version() == "0.1.0"


def test_resolve_version_of_an_absent_distribution_returns_the_sentinel() -> None:
    assert resolve_version("dataplat-no-such-distribution") == UNKNOWN_VERSION


def test_public_api_is_exported_from_the_package_root() -> None:
    assert dataplat.resolve_version is resolve_version
    assert set(dataplat.__all__) == {"DISTRIBUTION_NAME", "UNKNOWN_VERSION", "resolve_version"}
