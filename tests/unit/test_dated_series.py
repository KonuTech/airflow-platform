"""Tests for the dated backfill corpus generator (INCR-06, QUAL-11).

Covers behaviors 1-4 from 09-05-PLAN.md's Task 1: determinism (R1/R2), the
cadence+gap combination (D-06/D-10), the schema-change boundary (D-10), and
the late/out-of-order event (D-10). Behavior 5 (R6, no wall-clock) is proven
by `tests/policy/test_generator_determinism_rules.py`, whose module scan
already walks the whole `tools/corpus/` package — this module included — by
directory listing, so no separate wiring was needed for that guard to cover
it.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest
from tools.corpus.dated_series import generate_dated_series

_CUSTOMERS_COLUMNS = ("customer_id", "name", "country", "birth_date", "event_ts")
_ORDERS_COLUMNS = ("order_id", "customer_id", "order_date", "amount")
_START = date(2024, 1, 1)


def _filename(dataset: str, day_index: int) -> str:
    day = _START + timedelta(days=day_index)
    return f"{dataset}_{day.strftime('%Y%m%d')}.csv"


def test_generate_dated_series_is_byte_deterministic() -> None:
    """Test 1: two identical calls produce byte-identical output per file (R1/R2)."""
    kwargs: dict[str, object] = {
        "master_seed": "x",
        "start_date": _START,
        "num_days": 30,
        "gap_day_index": 5,
        "schema_change_day_index": 10,
        "late_event_day_index": 15,
    }

    files_a, manifest_a = generate_dated_series("customers", **kwargs)  # type: ignore[arg-type]
    files_b, manifest_b = generate_dated_series("customers", **kwargs)  # type: ignore[arg-type]

    assert files_a.keys() == files_b.keys()
    assert files_a, "expected at least one generated file"
    for name, content in files_a.items():
        assert content == files_b[name], f"{name}: bytes differ between two identical calls"
    assert manifest_a == manifest_b


def test_generate_dated_series_cadence_and_gap() -> None:
    """Test 2: the gap day emits no file; every other day emits exactly one (D-06/D-10)."""
    files, manifest = generate_dated_series(
        "customers",
        master_seed="x",
        start_date=_START,
        num_days=30,
        gap_day_index=10,
        schema_change_day_index=20,
        late_event_day_index=25,
    )

    assert manifest.gap_day_index == 10
    gap_filename = _filename("customers", 10)
    assert gap_filename not in files
    assert gap_filename not in manifest.filenames

    # Every other day 0..29 (excluding 10) has exactly one file.
    assert len(files) == 29
    assert len(manifest.filenames) == 29
    assert len(set(manifest.filenames)) == 29


def test_generate_dated_series_schema_change_boundary() -> None:
    """Test 3: header gains one column at schema_change_day_index (D-10)."""
    files, manifest = generate_dated_series(
        "customers",
        master_seed="x",
        start_date=_START,
        num_days=30,
        gap_day_index=29,
        schema_change_day_index=15,
        late_event_day_index=20,
    )

    assert manifest.schema_change_day_index == 15

    for day_index in range(15):
        filename = _filename("customers", day_index)
        header = files[filename].split(b"\r\n", 1)[0].decode("utf-8")
        assert header == ",".join(_CUSTOMERS_COLUMNS), (
            f"day {day_index} (< schema_change_day_index) must match the current schema exactly"
        )

    for day_index in range(15, 29):  # 29 is the gap day, excluded
        filename = _filename("customers", day_index)
        header = files[filename].split(b"\r\n", 1)[0].decode("utf-8")
        assert header == ",".join((*_CUSTOMERS_COLUMNS, "loyalty_tier")), (
            f"day {day_index} (>= schema_change_day_index) must carry the extra column"
        )
        # The extra column's own value must never be empty.
        rows = files[filename].decode("utf-8").split("\r\n")
        for row in rows[1:]:
            if not row:
                continue
            assert row.split(",")[-1], f"day {day_index}: extra column value must not be empty"


def test_generate_dated_series_late_event() -> None:
    """Test 4: one row in the late day is backdated; every other row is not (D-10)."""
    late_event_offset_days = 90
    files, manifest = generate_dated_series(
        "orders",
        master_seed="x",
        start_date=_START,
        num_days=30,
        gap_day_index=5,
        schema_change_day_index=30,  # never triggers within 0..29
        late_event_day_index=20,
        late_event_offset_days=late_event_offset_days,
    )

    assert manifest.late_event_day_index == 20
    assert manifest.late_event_row_index == 25  # default rows_per_day // 2 == 50 // 2

    day_20 = _START + timedelta(days=20)
    late_date = day_20 - timedelta(days=late_event_offset_days)
    filename = _filename("orders", 20)

    rows = files[filename].decode("utf-8").split("\r\n")
    header = rows[0].split(",")
    assert header == list(_ORDERS_COLUMNS)
    date_column_index = header.index("order_date")

    data_rows = [row for row in rows[1:] if row]
    assert len(data_rows) == 50

    for row_index, row in enumerate(data_rows):
        fields = row.split(",")
        row_date = fields[date_column_index]
        if row_index == manifest.late_event_row_index:
            assert row_date == late_date.strftime("%Y-%m-%d"), (
                "the designated late row must carry the backdated date"
            )
        else:
            assert row_date == day_20.strftime("%Y-%m-%d"), (
                f"row {row_index} must carry day 20's own date, not the backdated one"
            )


def test_generate_dated_series_rejects_unknown_dataset() -> None:
    """A dataset name outside the two known configs fails loudly, not silently."""
    with pytest.raises(ValueError, match="unknown dataset"):
        generate_dated_series(
            "not_a_real_dataset",
            master_seed="x",
            start_date=_START,
            num_days=1,
            gap_day_index=-1,
            schema_change_day_index=1,
            late_event_day_index=0,
        )
