"""Deterministic generation of the CSV fixture corpus (QUAL-08).

The corpus is the specification for the CSV engine. Two artifacts are committed
and everything else is generated:

* ``tests/fixtures/corpus.yaml`` — the manifest, which states what every fixture
  *means* as well as how to build it.
* ``tests/fixtures/CORPUS.sha256`` — the oracle, in standard ``sha256sum``
  format so an independent tool can check it.

``python -m tools.corpus generate`` materialises the corpus and rewrites the
oracle. ``python -m tools.corpus verify`` regenerates into a temporary directory
and compares against the oracle without touching it. That asymmetry is what
makes a generator change a reviewable diff rather than a silent re-baseline.
"""

from __future__ import annotations

__all__ = ["__doc__"]
