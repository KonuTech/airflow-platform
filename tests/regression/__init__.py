"""Regression tests: one permanent test per fixed bug (QUAL-07).

Present for the same reason as the other test packages: ruff's ``INP001`` (under
``select = ["ALL"]``) rejects an implicit namespace package, and making the
directory a real package also removes the module-basename collision hazard that
pytest's default import mode has for identically-named files in different test
directories.

Intentionally empty of tests. See ``README.md`` in this directory.
"""
