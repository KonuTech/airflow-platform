"""Date/numeric/boolean/NULL/whitespace normalization — the CSV-09/10/12 normalizers.

Home to the `StreamingStage` normalizers landing in Wave 2 of this phase.
Callers import from the submodule directly, e.g.
``from dataplat.normalize.dates import normalize_date`` — this package
marker re-exports nothing, matching ``dataplat/config/__init__.py``'s and
``dataplat/sources/__init__.py``'s shallow re-export convention.
"""

from __future__ import annotations
