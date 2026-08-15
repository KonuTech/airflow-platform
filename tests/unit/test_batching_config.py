"""Unit tests for ``DatasetConfig.batching``/``SourceConfig.duplicate_policy`` (04-03 Task 1).

A missing ``batching`` block must fail validation loudly -- ``max_units_per_run``
is a required cap (ORCH-03), never a silently-unbounded default.

``_VALID_DOCUMENT`` carries a ``columns:`` block (06-02 Task 1) purely to stay
valid: Phase 6 makes ``columns:`` a required ``DatasetConfig`` field (D-18,
the same "required, never defaulted" precedent this file's own docstring
already names for ``batching``), so any fixture predating that change needs
the same update this file needed when ``batching`` itself was added. This
file's own scope is still batching/duplicate_policy -- the ``columns:``
shape here mirrors ``configs/datasets/customers.yaml``'s real block so the
suite has one consistent "customers" shape, not two competing ones.
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


def test_dataset_config_validates_with_duplicate_policy_and_batching_cap() -> None:
    config = DatasetConfig.model_validate(_VALID_DOCUMENT)

    assert config.source.duplicate_policy == "skip"
    assert config.batching.max_units_per_run == 100


def test_dataset_config_fails_loudly_when_batching_is_omitted() -> None:
    """A missing cap must fail validation, never silently default to unbounded."""
    document_without_batching = {k: v for k, v in _VALID_DOCUMENT.items() if k != "batching"}

    with pytest.raises(ValidationError, match="batching"):
        DatasetConfig.model_validate(document_without_batching)
