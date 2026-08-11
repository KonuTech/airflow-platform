"""The csv_processor member is importable and exports nothing yet.

Phase 1 ships this package as a marker. Asserting it here keeps the second
workspace member inside the coverage report from the first commit, so a member
that is installed but never exercised is visible rather than silently absent.
"""

from __future__ import annotations

import csv_processor


def test_csv_processor_is_importable() -> None:
    assert csv_processor.__name__ == "csv_processor"


def test_csv_processor_exports_nothing_yet() -> None:
    assert csv_processor.__all__ == []
