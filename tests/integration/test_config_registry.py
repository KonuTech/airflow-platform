"""Integration tests for ``dataplat.config.registry.ConfigRegistry`` — SCHEMA-07.

Proves ARCHITECTURE.md §5.1's exact sync rule against a real, migrated
PostgreSQL: creating a dataset's first config version, a no-op on an
unchanged config, and versioning on a changed one. The three tests below
run in file order against ONE shared ``customers`` dataset row — pytest's
default same-module execution order — matching this suite's narrative
("call ``sync()`` a second time...", "mutate one field..."): each test's
assertions describe the database state *after* the tests that precede it,
not an isolated fixture.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import psycopg
import pytest

from dataplat.config.loader import load_config
from dataplat.config.registry import ConfigRegistry
from dataplat.errors import StorageError
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    from collections.abc import Iterator

    from dataplat.config.model import DatasetConfig

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULTS_PATH = REPO_ROOT / "configs" / "defaults.yaml"
CUSTOMERS_PATH = REPO_ROOT / "configs" / "datasets" / "customers.yaml"


@pytest.fixture(scope="module")
def customers_config() -> DatasetConfig:
    return load_config(CUSTOMERS_PATH, defaults_path=DEFAULTS_PATH)


@pytest.fixture(scope="module")
def registry(migrated_dsn: str) -> Iterator[ConfigRegistry]:
    with create_pool(migrated_dsn) as pool:
        yield ConfigRegistry(pool)


def _config_version_rows(dsn: str, dataset_name: str) -> list[tuple[int, int, object]]:
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            """
            SELECT cv.config_version_id, cv.version, cv.valid_to
              FROM meta.config_versions cv
              JOIN meta.datasets d ON d.dataset_id = cv.dataset_id
             WHERE d.dataset_name = %s
             ORDER BY cv.version
            """,
            (dataset_name,),
        ).fetchall()
    return [(row[0], row[1], row[2]) for row in rows]


def test_sync_creates_dataset_and_first_version(
    registry: ConfigRegistry,
    customers_config: DatasetConfig,
    migrated_dsn: str,
) -> None:
    record = registry.sync("customers", customers_config)

    assert record.is_new is True
    assert record.version == 1

    with psycopg.connect(migrated_dsn) as conn:
        dataset_rows = conn.execute(
            "SELECT dataset_id FROM meta.datasets WHERE dataset_name = %s",
            ("customers",),
        ).fetchall()
    assert len(dataset_rows) == 1

    rows = _config_version_rows(migrated_dsn, "customers")
    assert len(rows) == 1
    _, version, valid_to = rows[0]
    assert version == 1
    assert valid_to is None


def test_sync_is_a_noop_on_unchanged_config(
    registry: ConfigRegistry,
    customers_config: DatasetConfig,
    migrated_dsn: str,
) -> None:
    before = _config_version_rows(migrated_dsn, "customers")

    record = registry.sync("customers", customers_config)

    after = _config_version_rows(migrated_dsn, "customers")
    assert record.is_new is False
    assert after == before


def test_sync_versions_on_changed_config(
    registry: ConfigRegistry,
    customers_config: DatasetConfig,
    migrated_dsn: str,
) -> None:
    changed_load = customers_config.load.model_copy(update={"strategy": "full_swap"})
    changed = customers_config.model_copy(update={"load": changed_load})

    record = registry.sync("customers", changed)

    assert record.is_new is True
    assert record.version == 2

    rows = _config_version_rows(migrated_dsn, "customers")
    assert len(rows) == 2

    open_rows = [row for row in rows if row[2] is None]
    assert len(open_rows) == 1
    assert open_rows[0][1] == 2

    closed_rows = [row for row in rows if row[2] is not None]
    assert len(closed_rows) == 1
    assert closed_rows[0][1] == 1


def test_get_by_id_returns_the_exact_config_that_was_synced(
    registry: ConfigRegistry,
    customers_config: DatasetConfig,
) -> None:
    """The reprocessing seam: resolve a historical config by id, never by reading YAML."""
    record = registry.sync("get_by_id_round_trip", customers_config)

    resolved = registry.get_by_id(record.config_version_id)

    assert resolved == customers_config


def test_get_by_id_raises_storage_error_for_an_unknown_id(registry: ConfigRegistry) -> None:
    with pytest.raises(StorageError):
        registry.get_by_id(999_999_999)
