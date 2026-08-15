"""Unit tests for `dataplat.schema` — the SCHEMA-03/04/05/06 versioning/evolution/repository
modules.

Present for the same reason as the other test packages: ruff's ``INP001``
(under ``select = ["ALL"]``) rejects an implicit namespace package, and
making the directory a real package also removes the module-basename
collision hazard that pytest's default import mode has for identically-named
files in different test directories.
"""

from __future__ import annotations
