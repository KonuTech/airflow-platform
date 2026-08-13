"""Unit tests for ``DatasetConfig.batching``/``SourceConfig.duplicate_policy`` (04-03 Task 1).

A missing ``batching`` block must fail validation loudly -- ``max_units_per_run``
is a required cap (ORCH-03), never a silently-unbounded default.
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
