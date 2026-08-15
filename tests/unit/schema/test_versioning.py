"""Unit tests for ``dataplat.schema.versioning`` — SCHEMA-03.

Mirrors ``tests/unit/test_config_hashing.py``'s exact shape (same-hash-
twice, one-field-change-changes-hash, isinstance/length/version-constant
assertions), plus this module's own reordering-changes-the-hash tests — the
deliberate divergence from ``hash_config``'s key-reordering tolerance,
documented in ``dataplat/schema/versioning.py``'s module docstring.
"""

from __future__ import annotations

from dataplat.schema.versioning import SCHEMA_HASH_VERSION, hash_schema

_CUSTOMER_COLUMNS = [
    {"name": "customer_id", "type": "string", "nullable": False, "position": 0, "format": None},
    {"name": "amount", "type": "decimal", "nullable": False, "position": 1, "format": None},
]


def test_hashing_the_same_columns_twice_returns_the_same_hash() -> None:
    first_hash, _ = hash_schema(_CUSTOMER_COLUMNS)
    second_hash, _ = hash_schema(_CUSTOMER_COLUMNS)

    assert first_hash == second_hash


def test_reordering_the_same_columns_changes_the_hash() -> None:
    """Column POSITION is part of a schema's identity — unlike ``hash_config``."""
    reordered = list(reversed(_CUSTOMER_COLUMNS))

    original_hash, _ = hash_schema(_CUSTOMER_COLUMNS)
    reordered_hash, _ = hash_schema(reordered)

    assert original_hash != reordered_hash


def test_reordering_one_columns_own_keys_changes_the_hash() -> None:
    """``sort_keys=False`` (module docstring): a column dict's own key order is never normalized."""
    reordered_first_column = {
        "type": _CUSTOMER_COLUMNS[0]["type"],
        "name": _CUSTOMER_COLUMNS[0]["name"],
        "nullable": _CUSTOMER_COLUMNS[0]["nullable"],
        "position": _CUSTOMER_COLUMNS[0]["position"],
        "format": _CUSTOMER_COLUMNS[0]["format"],
    }
    columns_with_reordered_keys = [reordered_first_column, _CUSTOMER_COLUMNS[1]]

    original_hash, _ = hash_schema(_CUSTOMER_COLUMNS)
    reordered_keys_hash, _ = hash_schema(columns_with_reordered_keys)

    assert original_hash != reordered_keys_hash


def test_changing_one_columns_type_changes_the_hash() -> None:
    changed = [
        dict(_CUSTOMER_COLUMNS[0]),
        {**_CUSTOMER_COLUMNS[1], "type": "string"},
    ]

    original_hash, _ = hash_schema(_CUSTOMER_COLUMNS)
    changed_hash, _ = hash_schema(changed)

    assert original_hash != changed_hash


def test_hash_schema_returns_hash_and_fixed_version_constant() -> None:
    schema_hash, hash_version = hash_schema(_CUSTOMER_COLUMNS)

    assert isinstance(schema_hash, str)
    assert len(schema_hash) == 64  # sha256 hex digest length
    assert hash_version == SCHEMA_HASH_VERSION
    assert hash_version == 1
