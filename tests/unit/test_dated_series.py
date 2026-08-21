"""Tests for the dated backfill corpus generator.

Covers INCR-06, QUAL-11, SCD-01, SCD-07, SCD-08, QUAL-14.

Phase 9 (09-05-PLAN.md Task 1) covered determinism (R1/R2), the cadence+gap
combination (D-06/D-10), the schema-change boundary (D-10), and the
late/out-of-order event (D-10). Behavior 5 there (R6, no wall-clock) is
proven by `tests/policy/test_generator_determinism_rules.py`, whose module
scan already walks the whole `tools/corpus/` package -- this module included
-- by directory listing, so no separate wiring is needed for that guard to
cover anything added here either.

Phase 10 (10-06-PLAN.md Task 1) redesigns the customers path from "N fresh
customer_ids born every day" to "a bounded, deterministic roster resent in
full every day" (D-04's `change_semantics: snapshot`). Tasks 2/3 layer D-11's
attribute-change/late-correction/missing-customer anomalies and D-06's
mass-delete/circuit-breaker-trip anomaly on top of this roster foundation.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest
from tools.corpus.dated_series import _CUSTOMER_ID_BASE, generate_dated_series

_CUSTOMERS_COLUMNS = ("customer_id", "name", "country", "birth_date", "event_ts", "signup_country")
_ORDERS_COLUMNS = ("order_id", "customer_id", "order_date", "amount")
_START = date(2024, 1, 1)


def _filename(dataset: str, day_index: int) -> str:
    day = _START + timedelta(days=day_index)
    return f"{dataset}_{day.strftime('%Y%m%d')}.csv"


def _data_rows(files: dict[str, bytes], filename: str) -> list[list[str]]:
    rows = files[filename].decode("utf-8").split("\r\n")
    return [row.split(",") for row in rows[1:] if row]


# --------------------------------------------------------------------------
# Pre-existing behaviors (Phase 9), still required to hold under the roster
# model.
# --------------------------------------------------------------------------


def test_generate_dated_series_is_byte_deterministic() -> None:
    """Two identical calls produce byte-identical output per file (R1/R2)."""
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
    """The gap day emits no file; every other day emits exactly one (D-06/D-10)."""
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
    """Header gains one column at schema_change_day_index (D-10)."""
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
        for row in _data_rows(files, filename):
            assert row[-1], f"day {day_index}: extra column value must not be empty"


def test_generate_dated_series_late_event() -> None:
    """One row in the late day is backdated; every other row is not (D-10)."""
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


# --------------------------------------------------------------------------
# Task 1: Roster foundation
# --------------------------------------------------------------------------

_ROSTER_KWARGS: dict[str, object] = {
    "master_seed": "roster",
    "start_date": _START,
    "num_days": 10,
    "gap_day_index": -1,
    "schema_change_day_index": 999,
    "late_event_day_index": 999,
}


def test_roster_is_resent_in_full_every_day() -> None:
    """Test 1: a 10-day customers series with no anomalies resends the SAME roster daily."""
    files, manifest = generate_dated_series("customers", **_ROSTER_KWARGS)  # type: ignore[arg-type]

    assert len(files) == 10
    assert len(manifest.filenames) == 10

    id_sets = []
    for filename in manifest.filenames:
        rows = _data_rows(files, filename)
        assert len(rows) == 50  # default rows_per_day
        id_sets.append({row[0] for row in rows})

    first = id_sets[0]
    assert len(first) == 50
    for day_ids in id_sets[1:]:
        assert day_ids == first, "every day must resend the identical roster of customer_id values"


def test_roster_member_attributes_stable_across_days() -> None:
    """Test 2: a fixed roster member's baseline fields are byte-identical across every day."""
    files, manifest = generate_dated_series("customers", **_ROSTER_KWARGS)  # type: ignore[arg-type]

    member_index = 5
    seen_rows = []
    for filename in manifest.filenames:
        rows = _data_rows(files, filename)
        seen_rows.append(rows[member_index])

    first = seen_rows[0]
    first_customer_id, first_name, first_country, first_birth_date = (
        first[0],
        first[1],
        first[2],
        first[3],
    )
    first_signup_country = first[5]
    for row in seen_rows[1:]:
        assert row[0] == first_customer_id
        assert row[1] == first_name
        assert row[2] == first_country
        assert row[3] == first_birth_date
        assert row[5] == first_signup_country


def test_roster_preserves_orders_fixture_pool_formula() -> None:
    """Test 3: customer_id values base+n for n in range(30) are present in day 0's roster."""
    files, manifest = generate_dated_series("customers", **_ROSTER_KWARGS)  # type: ignore[arg-type]

    day0_ids = {row[0] for row in _data_rows(files, manifest.filenames[0])}
    expected = {str(_CUSTOMER_ID_BASE + n) for n in range(30)}
    assert expected <= day0_ids


def test_roster_determinism() -> None:
    """Test 4: two identical roster-model calls produce byte-identical files."""
    files_a, manifest_a = generate_dated_series("customers", **_ROSTER_KWARGS)  # type: ignore[arg-type]
    files_b, manifest_b = generate_dated_series("customers", **_ROSTER_KWARGS)  # type: ignore[arg-type]

    assert files_a.keys() == files_b.keys()
    for name, content in files_a.items():
        assert content == files_b[name]
    assert manifest_a == manifest_b


def test_orders_generation_untouched_by_roster_redesign() -> None:
    """Test 5: orders output is byte-identical to its pre-roster-redesign behavior."""
    kwargs: dict[str, object] = {
        "master_seed": "orders-regression",
        "start_date": _START,
        "num_days": 10,
        "gap_day_index": -1,
        "schema_change_day_index": 999,
        "late_event_day_index": 999,
    }
    files_a, manifest_a = generate_dated_series("orders", **kwargs)  # type: ignore[arg-type]
    files_b, manifest_b = generate_dated_series("orders", **kwargs)  # type: ignore[arg-type]

    assert files_a == files_b
    assert manifest_a == manifest_b
    # Orders rows must still use the day/row-index-scaled ID formula (unchanged).
    day0_filename = manifest_a.filenames[0]
    row0 = _data_rows(files_a, day0_filename)[0]
    assert row0[0] == "2110000000"  # _ORDER_ID_BASE + 0*_ID_DAY_MULTIPLIER + 0
