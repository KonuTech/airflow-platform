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

The second half of this file (06-15-PLAN.md Task 2) is a DIFFERENT, higher-
level proof: not ``SchemaRepository`` called directly with hand-built
column dicts, but the REAL, wired ``csv_processor.source.CsvSource.
inspect()`` call chain -- against a real uploaded MinIO object and a real
``dataset_id`` -- driving ``dataplat.schema.evolution.classify_schema_change``
and ``SchemaRepository`` together for the first time, live. Its own
dedicated dataset row (``_E2E_DATASET``) never shares schema-version
history with the ``dataset_id`` fixture above, or with the real
``customers`` dataset other integration test files use.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import psycopg
import pytest

from csv_processor.source import CsvSource
from dataplat.config.model import (
    BatchingConfig,
    ColumnContract,
    DatasetConfig,
    DeduplicationConfig,
    LoadConfig,
    SourceConfig,
)
from dataplat.errors import IncompatibleSchemaError, StorageError
from dataplat.load.staging import StagingLoader
from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.models.identity import RunContext
from dataplat.observability.logging import get_logger
from dataplat.pipeline.protocol import PipelineContext
from dataplat.schema.repository import SchemaRepository
from dataplat.schema.versioning import hash_schema
from dataplat.storage.db import create_pool
from dataplat.storage.objectstore import S3ObjectStore

if TYPE_CHECKING:
    from collections.abc import Iterator

    from psycopg_pool import ConnectionPool

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


# =============================================================================
# 06-15-PLAN.md Task 2: the real, wired CsvSource.inspect() call chain, live
# =============================================================================
#
# Four scenarios, run in file order against ONE shared dataset row
# (`_E2E_DATASET`) -- each test's assertions describe the database state
# *after* the scenarios that precede it, matching this file's own established
# narrative convention (module docstring).
#
# 1. A file matching the CONTRACT exactly, called twice -- a true no-op the
#    second time (bootstrap then genuine no-op).
# 2. A file whose header adds ONE new trailing column -- COMPATIBLE,
#    recorded as version=2, and D-01's "loads using known columns" proven
#    literally through a real StagingLoader.load() call.
# 3. A file missing a required contract column -- BREAKING, raises before
#    any row stages, records no new row.
# 4. A file matching the ORIGINAL (version=1) shape again, AFTER version=2
#    is current -- SCHEMA-06: resolves to version=1's own schema_version_id
#    via resolve_by_hash, not sync(), so no spurious version=3 is created.

_E2E_DATASET = "schema_resolution_csv_source"
_E2E_BUCKET = "schema-resolution-e2e"

_E2E_HEADER = "customer_id,name,country,birth_date,event_ts"
_E2E_WIDER_HEADER = "customer_id,name,country,birth_date,event_ts,loyalty_tier"
_E2E_NARROWER_HEADER = "customer_id,name,country,birth_date"
_E2E_TARGET_COLUMNS = ("customer_id", "name", "country", "birth_date", "event_ts")


def _e2e_columns() -> list[ColumnContract]:
    """The 5 ``ColumnContract``s matching ``configs/datasets/customers.yaml``'s real shape.

    A locally-built, independent equivalent -- not ``load_config`` against
    the real file -- so ``_E2E_DATASET`` never shares a dataset row (and
    never shares schema-version history) with the real ``customers``
    dataset other integration test files use against this SAME session-
    scoped Postgres container (``tests/integration/test_run_ingest.py``'s
    own docstring names this exact shared-container collision risk).
    """
    return [
        ColumnContract(
            name="customer_id",
            type="string",
            nullable=False,
            required=True,
            business_key=True,
        ),
        ColumnContract(name="name", type="string", nullable=False, required=True),
        ColumnContract(name="country", type="string", nullable=False, required=True),
        ColumnContract(
            name="birth_date",
            type="date",
            nullable=True,
            required=True,
            format="%Y-%m-%d",
        ),
        ColumnContract(
            name="event_ts",
            type="timestamp",
            nullable=False,
            required=True,
            format="%Y-%m-%dT%H:%M:%S%z",
        ),
    ]


def _e2e_config() -> DatasetConfig:
    return DatasetConfig(
        dataset=_E2E_DATASET,
        config_schema_version=1,
        source=SourceConfig(
            type="csv",
            bucket=_E2E_BUCKET,
            path="schema-resolution/",
            change_semantics="snapshot",
            duplicate_policy="skip",
        ),
        deduplication=DeduplicationConfig(
            strategy="business_key_latest",
            keys=["customer_id"],
            order_by=["event_ts desc"],
        ),
        load=LoadConfig(strategy="merge", target="normalized.customers"),
        batching=BatchingConfig(max_units_per_run=100),
        columns=_e2e_columns(),
    )


@pytest.fixture(scope="module")
def e2e_pool(migrated_dsn: str) -> Iterator[ConnectionPool]:
    with create_pool(migrated_dsn) as pool:
        yield pool


@pytest.fixture(scope="module")
def e2e_dataset_id(e2e_pool: ConnectionPool) -> int:
    """A dataset row dedicated to this section's live ``CsvSource.inspect()`` proofs.

    Deliberately distinct from both the real ``customers`` dataset and this
    file's own ``dataset_id`` fixture above (``schema_resolution_proof``,
    used by the ``SchemaRepository``-only tests) -- these scenarios drive
    the REAL, wired call chain end to end and must not share schema-version
    history with either.
    """
    return PostgresMetadataRepository(e2e_pool).get_or_create_dataset(_E2E_DATASET)


@pytest.fixture(scope="module")
def e2e_object_store(s3_client: Any, minio_config: dict[str, str]) -> S3ObjectStore:
    """A real ``S3ObjectStore`` over a throwaway bucket dedicated to this section's proofs."""
    existing = {bucket["Name"] for bucket in s3_client.list_buckets().get("Buckets", [])}
    if _E2E_BUCKET not in existing:
        s3_client.create_bucket(Bucket=_E2E_BUCKET)
    return S3ObjectStore(
        endpoint_url=f"http://{minio_config['endpoint']}",
        access_key=minio_config["access_key"],
        secret_key=minio_config["secret_key"],
    )


def _upload_e2e_csv(object_store: S3ObjectStore, *, key: str, header: str, data_row: str) -> None:
    body = f"{header}\n{data_row}\n".encode()
    object_store.put_object(_E2E_BUCKET, key, body)


def _e2e_source_and_ctx(
    *,
    dataset_id: int,
    object_store: S3ObjectStore,
    pool: ConnectionPool,
    key: str,
    run_id: int,
) -> tuple[CsvSource, PipelineContext]:
    """Build a real ``CsvSource``/``PipelineContext`` pair -- ``dataset_id`` wired through."""
    source = CsvSource(bucket=_E2E_BUCKET, key=key, dataset_id=dataset_id)
    ctx = PipelineContext(
        run=RunContext(run_id=run_id, idempotency_key=f"schema-resolution-e2e:{run_id}"),
        config=_e2e_config(),
        metadata=None,  # type: ignore[arg-type]  # unused by CsvSource.inspect()/StagingLoader.load()
        objects=object_store,
        db=pool,
        log=get_logger(),
        source=source,
    )
    return source, ctx


def _schema_version_columns(dsn: str, schema_version_id: int) -> list[dict[str, object]]:
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            "SELECT columns FROM meta.schema_versions WHERE schema_version_id = %s",
            (schema_version_id,),
        ).fetchone()
    assert row is not None
    return list(row[0])


# --- Scenario 1: a file matching the contract exactly is a true no-op the second time ---


def test_inspect_matching_contract_exactly_resolves_the_same_version_twice(
    e2e_dataset_id: int,
    e2e_object_store: S3ObjectStore,
    e2e_pool: ConnectionPool,
    migrated_dsn: str,
) -> None:
    """The bootstrap case (first-ever file, D-03) then a genuine ``sync()`` no-op."""
    _upload_e2e_csv(
        e2e_object_store,
        key="scenario1_a.csv",
        header=_E2E_HEADER,
        data_row="1,Alice,US,1990-01-01,2026-01-01T00:00:00+00:00",
    )
    _upload_e2e_csv(
        e2e_object_store,
        key="scenario1_b.csv",
        header=_E2E_HEADER,
        data_row="1,Alice,US,1990-01-01,2026-01-01T00:00:00+00:00",
    )
    source_a, ctx_a = _e2e_source_and_ctx(
        dataset_id=e2e_dataset_id,
        object_store=e2e_object_store,
        pool=e2e_pool,
        key="scenario1_a.csv",
        run_id=95_101,
    )
    source_b, ctx_b = _e2e_source_and_ctx(
        dataset_id=e2e_dataset_id,
        object_store=e2e_object_store,
        pool=e2e_pool,
        key="scenario1_b.csv",
        run_id=95_102,
    )

    profile_a = source_a.inspect(ctx_a)
    profile_b = source_b.inspect(ctx_b)

    assert profile_a.schema_version_id is not None
    assert profile_a.compatibility == "COMPATIBLE"
    assert profile_b.schema_version_id == profile_a.schema_version_id
    assert profile_b.compatibility == "COMPATIBLE"

    rows = _schema_version_rows(migrated_dsn, e2e_dataset_id)
    assert len(rows) == 1


# --- Scenario 2: a new trailing column is COMPATIBLE, loads using known columns only ---


def test_inspect_with_a_new_trailing_column_is_compatible_and_records_a_second_version(
    e2e_dataset_id: int,
    e2e_object_store: S3ObjectStore,
    e2e_pool: ConnectionPool,
    migrated_dsn: str,
) -> None:
    before = _schema_version_rows(migrated_dsn, e2e_dataset_id)
    assert len(before) == 1  # scenario 1's baseline

    _upload_e2e_csv(
        e2e_object_store,
        key="scenario2.csv",
        header=_E2E_WIDER_HEADER,
        data_row="2,Bob,US,1985-05-05,2026-02-01T00:00:00+00:00,GOLD",
    )
    source, ctx = _e2e_source_and_ctx(
        dataset_id=e2e_dataset_id,
        object_store=e2e_object_store,
        pool=e2e_pool,
        key="scenario2.csv",
        run_id=95_201,
    )

    profile = source.inspect(ctx)

    assert profile.compatibility == "COMPATIBLE"
    assert profile.schema_version_id is not None
    assert profile.schema_version_id != before[0][0]

    after = _schema_version_rows(migrated_dsn, e2e_dataset_id)
    assert len(after) == 2

    new_version_columns = _schema_version_columns(migrated_dsn, profile.schema_version_id)
    assert any(column["name"] == "loyalty_tier" for column in new_version_columns)


def test_staging_loads_the_wider_file_using_only_its_known_columns(
    e2e_dataset_id: int,
    e2e_object_store: S3ObjectStore,
    e2e_pool: ConnectionPool,
    migrated_dsn: str,
) -> None:
    """D-01's literal, live proof: ``loyalty_tier``'s values never reach the staging table.

    Drives scenario 2's SAME uploaded object all the way through a real
    ``StagingLoader.load()`` -- not merely asserting ``inspect()``'s own
    return value, but proving the file genuinely stages using only its
    known/contract columns, with every other column's value intact.
    """
    # `source` itself is unused below: `StagingLoader.load()` only ever
    # reads `ctx.source` (this same object, since `_e2e_source_and_ctx`
    # wires it in) -- it is never called through the local `source` binding
    # in this particular test.
    _source, ctx = _e2e_source_and_ctx(
        dataset_id=e2e_dataset_id,
        object_store=e2e_object_store,
        pool=e2e_pool,
        key="scenario2.csv",
        run_id=95_202,
    )
    loader = StagingLoader(target_columns=_E2E_TARGET_COLUMNS)

    with psycopg.connect(migrated_dsn) as conn:
        result = loader.load(ctx, conn)
        conn.commit()

        assert result.rows_read == 1
        assert result.rows_parsed == 1
        assert result.rows_rejected == 0

        staged_columns = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'staging' AND table_name = %s",
                (f"{_E2E_DATASET}__r95202",),
            ).fetchall()
        }
        assert "loyalty_tier" not in staged_columns
        assert set(_E2E_TARGET_COLUMNS) <= staged_columns

        staged_row = conn.execute(
            f"SELECT customer_id, name, country, birth_date, event_ts FROM {result.staging_table}",  # noqa: S608 -- result.staging_table is config/run-identity-derived, never row content
        ).fetchone()

    assert staged_row is not None
    customer_id, name, country, birth_date, event_ts = staged_row
    assert customer_id == "2"
    assert name == "Bob"
    assert country == "US"
    assert birth_date == "1985-05-05"
    assert event_ts == "2026-02-01T00:00:00+00:00"


# --- Scenario 3: a missing contract column is BREAKING, records no new row ---


def test_inspect_with_a_missing_contract_column_raises_and_records_no_new_row(
    e2e_dataset_id: int,
    e2e_object_store: S3ObjectStore,
    e2e_pool: ConnectionPool,
    migrated_dsn: str,
) -> None:
    before = _schema_version_rows(migrated_dsn, e2e_dataset_id)
    assert len(before) == 2  # scenario 2's state

    _upload_e2e_csv(
        e2e_object_store,
        key="scenario3.csv",
        header=_E2E_NARROWER_HEADER,
        data_row="3,Carol,CA,1992-03-03",
    )
    source, ctx = _e2e_source_and_ctx(
        dataset_id=e2e_dataset_id,
        object_store=e2e_object_store,
        pool=e2e_pool,
        key="scenario3.csv",
        run_id=95_301,
    )

    with pytest.raises(IncompatibleSchemaError) as exc_info:
        source.inspect(ctx)

    assert exc_info.value.context["diagnostic_code"] == "schema-column-disappeared"
    assert exc_info.value.context["column"] == "event_ts"

    after = _schema_version_rows(migrated_dsn, e2e_dataset_id)
    assert after == before


# --- Scenario 4 (SCHEMA-06): a file matching an OLDER schema resolves to it, live ---


def test_inspect_matching_a_historical_schema_resolves_to_the_older_version(
    e2e_dataset_id: int,
    e2e_object_store: S3ObjectStore,
    e2e_pool: ConnectionPool,
    migrated_dsn: str,
) -> None:
    """A file matching version=1's ORIGINAL shape, uploaded AFTER version=2 is current,
    resolves to version=1's own ``schema_version_id`` -- not the dataset's CURRENT
    version=2 -- and writes no new (spurious) row doing it."""
    before = _schema_version_rows(migrated_dsn, e2e_dataset_id)
    assert len(before) == 2
    version_one_id = next(row[0] for row in before if row[1] == 1)
    version_two_id = next(row[0] for row in before if row[1] == 2)

    _upload_e2e_csv(
        e2e_object_store,
        key="scenario4.csv",
        header=_E2E_HEADER,
        data_row="4,Dave,DE,1975-07-07,2026-04-01T00:00:00+00:00",
    )
    source, ctx = _e2e_source_and_ctx(
        dataset_id=e2e_dataset_id,
        object_store=e2e_object_store,
        pool=e2e_pool,
        key="scenario4.csv",
        run_id=95_401,
    )

    profile = source.inspect(ctx)

    assert profile.schema_version_id == version_one_id
    assert profile.schema_version_id != version_two_id
    assert profile.compatibility == "COMPATIBLE"

    after = _schema_version_rows(migrated_dsn, e2e_dataset_id)
    assert after == before  # no new row -- resolved via resolve_by_hash, not sync()
