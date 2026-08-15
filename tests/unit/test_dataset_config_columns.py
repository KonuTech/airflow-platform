"""Unit tests for ``DatasetConfig.columns`` and its two new model validators (06-02 Task 1/3).

Mirrors ``tests/unit/test_batching_config.py``'s shape: a ``_VALID_DOCUMENT``
built from ``configs/datasets/customers.yaml``'s real shape (SCHEMA-02), a
happy-path test, and one ``pytest.raises`` test per failure mode Task 1's
``<behavior>`` spec names -- a missing ``columns:`` block (D-18, matching
``BatchingConfig``'s "required, never defaulted" precedent), a colliding
``csv.delimiter``/``normalization.decimal_separator`` (STACK.md Section 15),
and a ``deduplication.keys`` entry that does not name a ``business_key: true``
column (D-18's cross-check).
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dataplat.config.model import DatasetConfig

_VALID_DOCUMENT = {
    "dataset": "customers",
    "config_schema_version": 1,
    "source": {
        "type": "csv",
        "bucket": "raw",
        "path": "customers/",
        "change_semantics": "snapshot",
        "duplicate_policy": "skip",
    },
    "deduplication": {
        "strategy": "business_key_latest",
        "keys": ["customer_id"],
        "order_by": ["event_ts desc"],
    },
    "load": {"strategy": "merge", "target": "normalized.customers"},
    "batching": {"max_units_per_run": 100},
    "columns": [
        {
            "name": "customer_id",
            "type": "string",
            "nullable": False,
            "required": True,
            "business_key": True,
            "description": "Natural business key for a customer record",
        },
        {"name": "name", "type": "string", "nullable": False, "required": True},
        {"name": "country", "type": "string", "nullable": False, "required": True},
        {
            "name": "birth_date",
            "type": "date",
            "nullable": True,
            "required": True,
            "format": "%Y-%m-%d",
        },
        {
            "name": "event_ts",
            "type": "timestamp",
            "nullable": False,
            "required": True,
            "format": "%Y-%m-%dT%H:%M:%S%z",
        },
    ],
}


def test_dataset_config_validates_with_a_real_columns_block() -> None:
    config = DatasetConfig.model_validate(_VALID_DOCUMENT)

    assert len(config.columns) == 5
    assert config.columns[0].name == "customer_id"
    assert config.columns[0].business_key is True
    assert config.filename is None
    assert config.normalization is None
    assert config.csv.delimiter is None
    assert config.schema_evolution_on_new_column == "evolve"
    assert config.schema_evolution_on_missing_or_retyped_column == "freeze"


def test_dataset_config_fails_loudly_when_columns_is_omitted() -> None:
    """A missing columns: block must fail validation, matching batching's own precedent."""
    document_without_columns = {k: v for k, v in _VALID_DOCUMENT.items() if k != "columns"}

    with pytest.raises(ValidationError, match="columns"):
        DatasetConfig.model_validate(document_without_columns)


def test_dataset_config_rejects_delimiter_colliding_with_decimal_separator() -> None:
    document = {
        **_VALID_DOCUMENT,
        "csv": {"delimiter": ";"},
        "normalization": {"decimal_separator": ";"},
    }

    with pytest.raises(ValidationError, match="delimiter"):
        DatasetConfig.model_validate(document)


def test_dataset_config_allows_delimiter_and_decimal_separator_that_differ() -> None:
    document = {
        **_VALID_DOCUMENT,
        "csv": {"delimiter": ";"},
        "normalization": {"decimal_separator": ","},
    }

    config = DatasetConfig.model_validate(document)

    assert config.csv.delimiter == ";"
    assert config.normalization is not None
    assert config.normalization.decimal_separator == ","


def test_dataset_config_rejects_dedup_key_not_declared_in_columns() -> None:
    document = {
        **_VALID_DOCUMENT,
        "deduplication": {
            "strategy": "business_key_latest",
            "keys": ["not_a_real_column"],
            "order_by": ["event_ts desc"],
        },
    }

    with pytest.raises(ValidationError, match="not_a_real_column"):
        DatasetConfig.model_validate(document)


def test_dataset_config_rejects_dedup_key_whose_column_is_not_a_business_key() -> None:
    document = {
        **_VALID_DOCUMENT,
        "deduplication": {
            "strategy": "business_key_latest",
            "keys": ["name"],
            "order_by": ["event_ts desc"],
        },
    }

    with pytest.raises(ValidationError, match="name"):
        DatasetConfig.model_validate(document)
