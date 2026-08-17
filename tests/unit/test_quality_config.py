"""Unit tests for ``DatasetConfig.quality``/``SourceConfig.batch_complete_marker`` (08-01 Task 2).

Mirrors ``tests/unit/test_dataset_config_columns.py``'s shape: a
``_VALID_DOCUMENT`` built from ``customers.yaml``'s real shape plus a
``quality:`` block with one rule of each of the 5 ``rule_type`` values used
later in this phase, a happy-path test, an ``extra="forbid"`` rejection test
for an unknown key inside a ``QualityRuleConfig``, and a
``batch_complete_marker`` default/round-trip test.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dataplat.config.model import DatasetConfig, SourceConfig

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
    "quality": {
        "rules": [
            {
                "rule_id": "row_is_well_formed",
                "rule_type": "STRUCTURAL",
                "strategy": "REJECT_RECORD",
            },
            {
                "rule_id": "customer_id_not_null",
                "rule_type": "QUALITY_COMPLETENESS",
                "strategy": "REJECT_RECORD",
                "column": "customer_id",
            },
            {
                "rule_id": "birth_date_in_range",
                "rule_type": "QUALITY_VALIDITY_RANGE",
                "strategy": "REJECT_RECORD",
                "column": "birth_date",
                "params": {"min": "1900-01-01", "max": "2026-01-01"},
            },
            {
                "rule_id": "customer_id_pattern",
                "rule_type": "QUALITY_PATTERN",
                "strategy": "WARN_AND_CONTINUE",
                "column": "customer_id",
                "params": {"pattern": "^[A-Z0-9]+$"},
            },
            {
                "rule_id": "customer_exists",
                "rule_type": "REFERENTIAL",
                "strategy": "QUARANTINE_RECORD",
            },
        ],
        "rejection_rate_threshold": 0.10,
    },
}


def test_dataset_config_validates_a_real_quality_block_with_all_five_rule_types() -> None:
    config = DatasetConfig.model_validate(_VALID_DOCUMENT)

    assert config.quality is not None
    assert len(config.quality.rules) == 5
    assert {rule.rule_type for rule in config.quality.rules} == {
        "STRUCTURAL",
        "QUALITY_COMPLETENESS",
        "QUALITY_VALIDITY_RANGE",
        "QUALITY_PATTERN",
        "REFERENTIAL",
    }
    assert config.quality.rejection_rate_threshold == 0.10

    referential_rule = next(
        rule for rule in config.quality.rules if rule.rule_type == "REFERENTIAL"
    )
    assert referential_rule.column is None

    pattern_rule = next(
        rule for rule in config.quality.rules if rule.rule_type == "QUALITY_PATTERN"
    )
    assert pattern_rule.params == {"pattern": "^[A-Z0-9]+$"}


def test_dataset_config_allows_quality_to_be_omitted() -> None:
    document_without_quality = {k: v for k, v in _VALID_DOCUMENT.items() if k != "quality"}

    config = DatasetConfig.model_validate(document_without_quality)

    assert config.quality is None


def test_quality_rule_config_rejects_an_unknown_key() -> None:
    document = {
        **_VALID_DOCUMENT,
        "quality": {
            "rules": [
                {
                    "rule_id": "row_is_well_formed",
                    "rule_type": "STRUCTURAL",
                    "strategy": "REJECT_RECORD",
                    "unknown_key": "typo",
                },
            ],
        },
    }

    with pytest.raises(ValidationError, match="unknown_key"):
        DatasetConfig.model_validate(document)


def test_source_config_batch_complete_marker_defaults_to_none() -> None:
    source = SourceConfig(
        type="csv",
        bucket="raw",
        path="customers/",
        change_semantics="snapshot",
        duplicate_policy="skip",
    )

    assert source.batch_complete_marker is None


def test_source_config_batch_complete_marker_round_trips_a_set_value() -> None:
    source = SourceConfig(
        type="csv",
        bucket="raw",
        path="orders/",
        change_semantics="snapshot",
        duplicate_policy="skip",
        batch_complete_marker="_BATCH_COMPLETE",
    )

    assert source.batch_complete_marker == "_BATCH_COMPLETE"
