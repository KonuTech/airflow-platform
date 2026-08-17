"""Unit tests for `dataplat.validate` -- the VALID-01/02/03 `StreamingStage` rules.

Present for the same reason as the other test packages: ruff's ``INP001``
(under ``select = ["ALL"]``) rejects an implicit namespace package, and
making the directory a real package also removes the module-basename
collision hazard that pytest's default import mode has for identically-named
files in different test directories.
"""

from __future__ import annotations
