"""Unit tests for ``dataplat.config.hashing`` and ``dataplat.config.loader`` — SCHEMA-07.

Covers ``hash_config()``'s canonicalization stability (same hash across
repeated calls and key-reordered YAML, a different hash on any value
change) and ``load_config()``'s merge-then-validate-then-wrap-errors
behavior.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import yaml

from dataplat.config.hashing import CONFIG_HASH_VERSION, hash_config
from dataplat.config.loader import load_config
from dataplat.config.model import DatasetConfig
from dataplat.errors import ConfigurationError

if TYPE_CHECKING:
    from pathlib import Path

_CUSTOMERS_DOCUMENT = {
    "dataset": "customers",
    "config_schema_version": 1,
    "source": {
        "type": "csv",
        "bucket": "raw",
        "path": "customers/",
        "change_semantics": "snapshot",
    },
    "deduplication": {
        "strategy": "business_key_latest",
        "keys": ["customer_id"],
        "order_by": ["event_ts desc"],
    },
    "load": {"strategy": "merge", "target": "normalized.customers"},
}

# Same document, deliberately reordered at every dict level: dataplat.config.
# hashing.hash_config must produce an identical hash for this against
# _CUSTOMERS_DOCUMENT (Test 2 below) — only dict key order changes here, list
# element order/content stays identical, which is what isolates "key order"
# as the only variable.
_CUSTOMERS_DOCUMENT_REORDERED = {
    "load": {"target": "normalized.customers", "strategy": "merge"},
    "deduplication": {
        "order_by": ["event_ts desc"],
        "keys": ["customer_id"],
        "strategy": "business_key_latest",
    },
    "source": {
        "change_semantics": "snapshot",
        "path": "customers/",
        "bucket": "raw",
        "type": "csv",
    },
    "config_schema_version": 1,
    "dataset": "customers",
}


def _customers_config() -> DatasetConfig:
    return DatasetConfig.model_validate(_CUSTOMERS_DOCUMENT)


def _write_defaults(tmp_path: Path) -> Path:
    defaults_path = tmp_path / "defaults.yaml"
    defaults_path.write_text("config_schema_version: 1\n", encoding="utf-8")
    return defaults_path


def test_hashing_the_same_config_twice_returns_the_same_hash() -> None:
    cfg = _customers_config()

    first_hash, _ = hash_config(cfg)
    second_hash, _ = hash_config(cfg)

    assert first_hash == second_hash


def test_key_order_in_source_yaml_does_not_change_the_hash(tmp_path: Path) -> None:
    defaults_path = _write_defaults(tmp_path)
    ordered_path = tmp_path / "ordered.yaml"
    ordered_path.write_text(yaml.safe_dump(_CUSTOMERS_DOCUMENT), encoding="utf-8")
    reordered_path = tmp_path / "reordered.yaml"
    reordered_path.write_text(yaml.safe_dump(_CUSTOMERS_DOCUMENT_REORDERED), encoding="utf-8")

    ordered_cfg = load_config(ordered_path, defaults_path=defaults_path)
    reordered_cfg = load_config(reordered_path, defaults_path=defaults_path)

    assert hash_config(ordered_cfg)[0] == hash_config(reordered_cfg)[0]


def test_changing_one_field_value_changes_the_hash() -> None:
    cfg = _customers_config()
    changed_load = cfg.load.model_copy(update={"strategy": "full_swap"})
    changed = cfg.model_copy(update={"load": changed_load})

    assert hash_config(cfg)[0] != hash_config(changed)[0]


def test_hash_config_returns_hash_and_fixed_version_constant() -> None:
    cfg = _customers_config()

    config_hash, hash_version = hash_config(cfg)

    assert isinstance(config_hash, str)
    assert len(config_hash) == 64  # sha256 hex digest length
    assert hash_version == CONFIG_HASH_VERSION
    assert hash_version == 1


def test_load_config_returns_a_validated_dataset_config(tmp_path: Path) -> None:
    defaults_path = _write_defaults(tmp_path)
    dataset_path = tmp_path / "customers.yaml"
    document = {k: v for k, v in _CUSTOMERS_DOCUMENT.items() if k != "config_schema_version"}
    dataset_path.write_text(yaml.safe_dump(document), encoding="utf-8")

    cfg = load_config(dataset_path, defaults_path=defaults_path)

    assert isinstance(cfg, DatasetConfig)
    assert cfg.dataset == "customers"
    assert cfg.config_schema_version == 1


def test_load_config_wraps_an_unknown_key_as_configuration_error_naming_the_path(
    tmp_path: Path,
) -> None:
    defaults_path = _write_defaults(tmp_path)
    bad_path = tmp_path / "bad.yaml"
    bad_document = {**_CUSTOMERS_DOCUMENT, "unknown_key": 1}
    bad_path.write_text(yaml.safe_dump(bad_document), encoding="utf-8")

    with pytest.raises(ConfigurationError) as exc_info:
        load_config(bad_path, defaults_path=defaults_path)

    assert str(bad_path) in str(exc_info.value)
