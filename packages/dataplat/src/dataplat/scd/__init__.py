"""``dataplat.scd`` -- pure-function Slowly Changing Dimension (SCD) building blocks.

Deliberately package-marker only: the functions living in this package
(``hashing.tracked_attribute_hash``, ``recompute.recompute_version_chain``)
have no database connection, no ``ctx``, and perform no I/O -- plan 10-04's
``SCDPublisher`` assembles the real, DB-touching pipeline around them.
"""

from __future__ import annotations
