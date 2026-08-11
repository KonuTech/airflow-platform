"""Source-agnostic ETL platform core.

This package must never import ``csv_processor``: the CSV engine is a plugin of
the core, not a peer of it. The rule is enforced mechanically by the
import-linter contract in ``setup.cfg``.
"""

from __future__ import annotations

from dataplat.version import DISTRIBUTION_NAME, UNKNOWN_VERSION, resolve_version

__all__ = ["DISTRIBUTION_NAME", "UNKNOWN_VERSION", "resolve_version"]
