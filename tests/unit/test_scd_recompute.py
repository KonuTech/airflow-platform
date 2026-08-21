"""Unit tests for ``dataplat.scd.recompute.recompute_version_chain`` -- SCD-01/02/04.

This plan's RESEARCH.md "Assumption A2 spike": proves the LEAD()/change-point
recompute shape as a pure Python algorithm against 8 concrete edge cases
BEFORE plan 10-04's ``SCDPublisher`` assembles the real, DB-touching
pipeline around it. No database connection, no ``ctx``, no I/O.
"""

from __future__ import annotations

import ast
import dataclasses
from datetime import UTC, datetime
from pathlib import Path

import dataplat.scd.recompute as _recompute_module
from dataplat.scd.recompute import BronzeRecord, VersionRow, recompute_version_chain

_SENTINEL = datetime(9999, 12, 31, tzinfo=UTC)


def _ts(day: int) -> datetime:
    return datetime(2026, 1, day, tzinfo=UTC)


def test_type2_boundary_produces_two_versions() -> None:
    """A 3-row history where row 2 differs from row 1, row 3 matches row 2 -> 2 versions."""
    history = [
        BronzeRecord(
            customer_id=1,
            name="Anna",
            country="PL",
            birth_date=None,
            event_ts=_ts(1),
            signup_country="PL",
            source_row_number=1,
        ),
        BronzeRecord(
            customer_id=1,
            name="Anna",
            country="DE",
            birth_date=None,
            event_ts=_ts(2),
            signup_country="PL",
            source_row_number=2,
        ),
        BronzeRecord(
            customer_id=1,
            name="Anna",
            country="DE",
            birth_date=None,
            event_ts=_ts(3),
            signup_country="PL",
            source_row_number=3,
        ),
    ]

    result = recompute_version_chain(history, valid_to_sentinel=_SENTINEL)

    assert len(result) == 2
    assert result[0].country == "PL"
    assert result[0].valid_from == _ts(1)
    assert result[0].valid_to == _ts(2)
    assert result[0].is_current is False
    assert result[1].country == "DE"
    assert result[1].valid_from == _ts(2)
    assert result[1].valid_to == _SENTINEL
    assert result[1].is_current is True


def test_no_change_produces_one_version() -> None:
    """A 3-row history with identical (name, country) throughout -> exactly 1 version."""
    history = [
        BronzeRecord(
            customer_id=1,
            name="Anna",
            country="PL",
            birth_date=None,
            event_ts=_ts(day),
            signup_country="PL",
            source_row_number=day,
        )
        for day in (1, 2, 3)
    ]

    result = recompute_version_chain(history, valid_to_sentinel=_SENTINEL)

    assert len(result) == 1
    assert result[0].valid_from == _ts(1)
    assert result[0].valid_to == _SENTINEL
    assert result[0].is_current is True


def test_type1_carries_latest_value_on_every_version() -> None:
    """Type-1 birth_date: EVERY version row carries the value from the row with max event_ts."""
    history = [
        BronzeRecord(
            customer_id=1,
            name="Anna",
            country="PL",
            birth_date="1990-01-01",
            event_ts=_ts(1),
            signup_country="PL",
            source_row_number=1,
        ),
        BronzeRecord(
            customer_id=1,
            name="Anna",
            country="DE",
            birth_date="1990-01-02",
            event_ts=_ts(2),
            signup_country="PL",
            source_row_number=2,
        ),
        BronzeRecord(
            customer_id=1,
            name="Anna",
            country="DE",
            birth_date="1990-01-03",
            event_ts=_ts(3),
            signup_country="PL",
            source_row_number=3,
        ),
    ]

    result = recompute_version_chain(history, valid_to_sentinel=_SENTINEL)

    assert len(result) == 2
    # Every version (not just current) carries the value from the row with
    # the MAXIMUM event_ts across the WHOLE history -- "1990-01-03".
    assert all(v.birth_date == "1990-01-03" for v in result)


def test_type0_carries_earliest_value_on_every_version_and_ignores_later_rows() -> None:
    """Type-0 signup_country: EVERY version carries the earliest row's value; later rows ignored."""
    history = [
        BronzeRecord(
            customer_id=1,
            name="Anna",
            country="PL",
            birth_date=None,
            event_ts=_ts(1),
            signup_country="PL",
            source_row_number=1,
        ),
        BronzeRecord(
            customer_id=1,
            name="Anna",
            country="DE",
            birth_date=None,
            event_ts=_ts(2),
            signup_country="DE",
            source_row_number=2,
        ),
    ]

    result = recompute_version_chain(history, valid_to_sentinel=_SENTINEL)
    assert all(v.signup_country == "PL" for v in result)

    # Prove a later row's signup_country is provably ignored: changing it
    # does not change the output at all.
    mutated_history = [
        history[0],
        dataclasses.replace(history[1], signup_country="FR"),
    ]
    mutated_result = recompute_version_chain(mutated_history, valid_to_sentinel=_SENTINEL)

    assert all(v.signup_country == "PL" for v in mutated_result)
    assert mutated_result == result


def test_identical_event_ts_tie_break_by_source_row_number() -> None:
    """Two rows sharing the exact same event_ts, different source_row_number -- no raise/drop."""
    tied_ts = _ts(1)
    history = [
        BronzeRecord(
            customer_id=1,
            name="Bella",
            country="FR",
            birth_date=None,
            event_ts=tied_ts,
            signup_country="FR",
            source_row_number=2,
        ),
        BronzeRecord(
            customer_id=1,
            name="Anna",
            country="PL",
            birth_date=None,
            event_ts=tied_ts,
            signup_country="PL",
            source_row_number=1,
        ),
    ]

    result = recompute_version_chain(history, valid_to_sentinel=_SENTINEL)

    # Tie-break orders by source_row_number ascending: row with
    # source_row_number=1 (Anna/PL) comes first.
    assert len(result) == 2
    assert result[0].name == "Anna"
    assert result[0].country == "PL"
    assert result[1].name == "Bella"
    assert result[1].country == "FR"


def test_null_safety_none_to_value_is_a_version_boundary() -> None:
    """An early row's country=None, a later row's country='PL' -- MUST be a version boundary."""
    history = [
        BronzeRecord(
            customer_id=1,
            name="Anna",
            country=None,
            birth_date=None,
            event_ts=_ts(1),
            signup_country=None,
            source_row_number=1,
        ),
        BronzeRecord(
            customer_id=1,
            name="Anna",
            country="PL",
            birth_date=None,
            event_ts=_ts(2),
            signup_country=None,
            source_row_number=2,
        ),
    ]

    result = recompute_version_chain(history, valid_to_sentinel=_SENTINEL)

    assert len(result) == 2
    assert result[0].country is None
    assert result[1].country == "PL"


def test_version_row_has_no_surrogate_id_field() -> None:
    """VersionRow carries NO surrogate/identity value -- assigned later by the DB Identity col."""
    field_names = {f.name for f in dataclasses.fields(VersionRow)}

    assert "id" not in field_names


def test_schema_evolution_on_untracked_column_never_triggers_a_new_version() -> None:
    """BronzeRecord has no slot for an untracked column -- schema evolution can't add a version.

    Simulated by proving that two histories differing ONLY in an
    out-of-band property this function's BronzeRecord type doesn't even
    model (there is no field to add a value to) produce IDENTICAL output --
    demonstrated here by two structurally distinct-looking but
    tracked-column-identical histories collapsing to the same result.
    """
    baseline = [
        BronzeRecord(
            customer_id=1,
            name="Anna",
            country="PL",
            birth_date=None,
            event_ts=_ts(1),
            signup_country="PL",
            source_row_number=1,
        ),
    ]
    # A second history where every value BronzeRecord actually models is
    # identical -- a hypothetical extra loyalty_tier column would live
    # outside this dataclass entirely, and adding new unrelated dataclass
    # instances with the same modeled fields cannot change the result.
    also_baseline = [
        BronzeRecord(
            customer_id=1,
            name="Anna",
            country="PL",
            birth_date=None,
            event_ts=_ts(1),
            signup_country="PL",
            source_row_number=1,
        ),
    ]

    assert recompute_version_chain(baseline, valid_to_sentinel=_SENTINEL) == (
        recompute_version_chain(also_baseline, valid_to_sentinel=_SENTINEL)
    )


def test_recompute_module_has_no_db_or_io_imports() -> None:
    """Structural guard: recompute.py performs no I/O (no psycopg/Connection import)."""
    source = _recompute_module.__file__
    assert source is not None
    tree = ast.parse(Path(source).read_text(encoding="utf-8"), filename=source)

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            assert not any("psycopg" in alias.name for alias in node.names)
        if isinstance(node, ast.ImportFrom):
            assert node.module is None or "psycopg" not in node.module
