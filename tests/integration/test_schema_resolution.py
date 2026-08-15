"""Integration tests for ``dataplat.schema.repository.SchemaRepository`` — SCHEMA-03/06.

Proves the exact versioned-upsert rule ``dataplat.config.registry.
ConfigRegistry`` already proved correct (``tests/integration/
test_config_registry.py``), transposed onto ``meta.schema_versions``, plus
SCHEMA-06's own D-16 proof: ``resolve_by_hash`` finding a CLOSED (non-
current) historical row, not only the dataset's current schema version. The
tests below run in file order against ONE shared dataset row — pytest's
default same-module execution order — matching ``test_config_registry.py``'s
own narrative convention: each test's assertions describe the database state
*after* the tests that precede it, not an isolated fixture.

Verified without the plan-specified ``-m integration`` filter, following
06-01-SUMMARY.md's own established precedent: repo-wide grep confirms no
``integration`` pytest marker is registered in ``pyproject.toml``'s markers
list or applied anywhere via decorator, so ``-m integration`` silently
deselects every test here and exits 0 having run nothing. Verified instead
via the same invocation ``make test-integration`` actually uses
(``pytest tests/integration/test_schema_resolution.py -x -q``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import psycopg
import pytest

from dataplat.errors import StorageError
from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.schema.repository import SchemaRepository
from dataplat.schema.versioning import hash_schema
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    from collections.abc import Iterator

_ORIGINAL_COLUMNS = [
    {"name": "customer_id", "type": "string", "nullable": False, "position": 0, "format": None},
    {"name": "amount", "type": "decimal", "nullable": False, "position": 1, "format": None},
]

# One column's type differs from _ORIGINAL_COLUMNS (amount: decimal -> string)
# — the "changing one column's type differs" case the plan's action text
# names explicitly.
_CHANGED_COLUMNS = [
    {"name": "customer_id", "type": "string", "nullable": False, "position": 0, "format": None},
    {"name": "amount", "type": "string", "nullable": False, "position": 1, "format": None},
]


@pytest.fixture(scope="module")
def dataset_id(migrated_dsn: str) -> int:
    """A real ``meta.datasets`` row's id — ``SchemaRepository.sync()`` never creates one itself."""
    with create_pool(migrated_dsn) as pool:
        return PostgresMetadataRepository(pool).get_or_create_dataset("schema_resolution_proof")


@pytest.fixture(scope="module")
def repository(migrated_dsn: str) -> Iterator[SchemaRepository]:
    with create_pool(migrated_dsn) as pool:
        yield SchemaRepository(pool)


def _schema_version_rows(dsn: str, dataset_id: int) -> list[tuple[int, int, object]]:
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            """
            SELECT schema_version_id, version, valid_to
              FROM meta.schema_versions
             WHERE dataset_id = %s
             ORDER BY version
            """,
            (dataset_id,),
        ).fetchall()
    return [(row[0], row[1], row[2]) for row in rows]


def test_sync_creates_first_schema_version(
    repository: SchemaRepository,
    dataset_id: int,
    migrated_dsn: str,
) -> None:
    record = repository.sync(dataset_id, columns=_ORIGINAL_COLUMNS, derived_from="CONTRACT")

    assert record.is_new is True
    assert record.version == 1
    assert record.compatibility == "COMPATIBLE"

    rows = _schema_version_rows(migrated_dsn, dataset_id)
    assert len(rows) == 1
    _, version, valid_to = rows[0]
    assert version == 1
    assert valid_to is None


def test_sync_is_a_noop_on_unchanged_schema(
    repository: SchemaRepository,
    dataset_id: int,
    migrated_dsn: str,
) -> None:
    before = _schema_version_rows(migrated_dsn, dataset_id)
    first = repository.get_current(dataset_id)
    assert first is not None

    record = repository.sync(dataset_id, columns=_ORIGINAL_COLUMNS, derived_from="CONTRACT")

    after = _schema_version_rows(migrated_dsn, dataset_id)
    assert record.is_new is False
    assert record.schema_version_id == first.schema_version_id
    assert after == before


def test_sync_versions_on_changed_schema(
    repository: SchemaRepository,
    dataset_id: int,
    migrated_dsn: str,
) -> None:
    record = repository.sync(
        dataset_id,
        columns=_CHANGED_COLUMNS,
        derived_from="CONTRACT",
        compatibility="BREAKING",
        breaking_changes={"column": "amount", "reason": "type changed decimal -> string"},
    )

    assert record.is_new is True
    assert record.version == 2
    assert record.compatibility == "BREAKING"

    rows = _schema_version_rows(migrated_dsn, dataset_id)
    assert len(rows) == 2

    open_rows = [row for row in rows if row[2] is None]
    assert len(open_rows) == 1
    assert open_rows[0][1] == 2

    closed_rows = [row for row in rows if row[2] is not None]
    assert len(closed_rows) == 1
    assert closed_rows[0][1] == 1


def test_get_current_returns_the_open_row(
    repository: SchemaRepository,
    dataset_id: int,
) -> None:
    current = repository.get_current(dataset_id)

    assert current is not None
    assert current.version == 2
    assert current.compatibility == "BREAKING"


def test_get_current_returns_none_for_a_dataset_with_no_schema_history(
    repository: SchemaRepository,
) -> None:
    assert repository.get_current(999_999_999) is None


def test_resolve_by_hash_finds_a_closed_historical_row(
    repository: SchemaRepository,
    dataset_id: int,
    migrated_dsn: str,
) -> None:
    """SCHEMA-06's D-16 proof: a file matching an OLD structure resolves to its own
    historical version, not the dataset's current one."""
    original_hash, _ = hash_schema(_ORIGINAL_COLUMNS)

    resolved = repository.resolve_by_hash(dataset_id, original_hash)

    assert resolved.version == 1
    assert resolved.schema_hash == original_hash

    current = repository.get_current(dataset_id)
    assert current is not None
    assert current.version != resolved.version

    # Explicit proof the resolved row is genuinely CLOSED (valid_to IS NOT
    # NULL), not accidentally the dataset's current row under a different
    # code path.
    rows = _schema_version_rows(migrated_dsn, dataset_id)
    resolved_row = next(row for row in rows if row[0] == resolved.schema_version_id)
    assert resolved_row[2] is not None


def test_resolve_by_hash_raises_storage_error_for_an_unrecorded_hash(
    repository: SchemaRepository,
    dataset_id: int,
) -> None:
    with pytest.raises(StorageError):
        repository.resolve_by_hash(dataset_id, "0" * 64)
