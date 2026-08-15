"""Filename/encoding/dialect/header-footer detection: the CSV-01/02/03/04/05/06/07/08 detectors.

Callers import from the submodule directly, e.g.
``from csv_processor.detect.encoding import detect_encoding`` — this package
marker re-exports nothing, matching ``dataplat/config/__init__.py``'s and
``dataplat/sources/__init__.py``'s shallow re-export convention.
"""

from __future__ import annotations
