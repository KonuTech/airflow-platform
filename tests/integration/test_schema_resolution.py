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
from dataplat.diagnostics import DIAGNOSTIC_CODES
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


# --- Scenario 5 (orchestrator fix, post-wave-5 code review CR-01): a file whose columns
# match the contract's names but not their physical order is rejected loudly, never
# silently staged into the wrong target columns ---


_E2E_REORDERED_HEADER = "name,customer_id,country,birth_date,event_ts"


def test_inspect_with_reordered_columns_raises_and_records_no_new_row(
    e2e_dataset_id: int,
    e2e_object_store: S3ObjectStore,
    e2e_pool: ConnectionPool,
    migrated_dsn: str,
) -> None:
    """``customer_id``/``name`` swapped in the header, every name/type otherwise unchanged.

    ``classify_schema_change`` alone would call this COMPATIBLE (it compares by name, never
    position -- ``evolution.py``'s own docstring/tests). ``StagingLoader`` maps a row's
    fields to ``target_columns`` by position alone, with no header-to-contract name
    remapping anywhere in this codebase, so silently accepting this file would swap every
    row's ``customer_id``/``name`` values into each other's target columns -- undetectable
    even by ``_record_hash``, since that hash is computed over the already-misaligned row.
    ``CsvSource._resolve_schema`` must catch this itself, before delegating to
    ``classify_schema_change``'s name-only comparison.
    """
    before = _schema_version_rows(migrated_dsn, e2e_dataset_id)
    assert len(before) == 2  # scenario 4 resolved via resolve_by_hash -- no new row

    _upload_e2e_csv(
        e2e_object_store,
        key="scenario5.csv",
        header=_E2E_REORDERED_HEADER,
        data_row="Eve,5,FR,1988-05-05,2026-05-01T00:00:00+00:00",
    )
    source, ctx = _e2e_source_and_ctx(
        dataset_id=e2e_dataset_id,
        object_store=e2e_object_store,
        pool=e2e_pool,
        key="scenario5.csv",
        run_id=95_501,
    )

    with pytest.raises(IncompatibleSchemaError) as exc_info:
        source.inspect(ctx)

    assert exc_info.value.context["diagnostic_code"] == "schema-columns-reordered"
    assert exc_info.value.context["expected_order"] == [
        "customer_id",
        "name",
        "country",
        "birth_date",
        "event_ts",
    ]
    assert exc_info.value.context["observed_order"] == [
        "name",
        "customer_id",
        "country",
        "birth_date",
        "event_ts",
    ]

    after = _schema_version_rows(migrated_dsn, e2e_dataset_id)
    assert after == before  # rejected before any schema_versions write


def test_schema_columns_reordered_diagnostic_code_is_in_the_shared_catalog() -> None:
    """D-24's drift guard, applied to this module's reordered-columns raise site.

    Keeps ``CsvSource._resolve_schema``'s literal in sync with
    ``dataplat.diagnostics.DIAGNOSTIC_CODES`` — a rename on either side
    without the other becomes a failing test here, not a silent mismatch.
    """
    assert "schema-columns-reordered" in DIAGNOSTIC_CODES


# =============================================================================
# D-13 optional-column absence (debug/ci-pipeline-ingestion-timeout ROUND 15,
# finding 20): a file whose header is a STRICT PREFIX of the contract's column
# order, where every contract column beyond the prefix is `required: false`,
# is COMPATIBLE and resolves to the CONTRACT's own schema version -- never a
# BREAKING schema-column-disappeared raise (the crash that wedged every 5-col
# e2e customers fixture on CI after Phase 10 added the optional
# signup_country as a 6th contract column), and never a sync(INFERRED) that
# would flip the dataset's CURRENT version and re-key every file.
#
# Own dedicated dataset row (`_D13_DATASET`) -- same isolation reasoning as
# `_E2E_DATASET` above.
# =============================================================================

_D13_DATASET = "schema_resolution_optional_prefix"

_D13_FULL_HEADER = "customer_id,name,country,birth_date,event_ts,signup_country"
_D13_PREFIX_HEADER = "customer_id,name,country,birth_date,event_ts"


def _d13_columns() -> list[ColumnContract]:
    """``_e2e_columns()`` plus a trailing OPTIONAL column -- customers.yaml's real post-Phase-10 shape."""  # noqa: E501, W505
    return [
        *_e2e_columns(),
        ColumnContract(
            name="signup_country",
            type="string",
            nullable=True,
            required=False,
            description="Trailing optional column (D-13: accept absence, files predate it)",
        ),
    ]


def _d13_config(columns: list[ColumnContract] | None = None) -> DatasetConfig:
    return _e2e_config().model_copy(
        update={"dataset": _D13_DATASET, "columns": columns or _d13_columns()},
    )


@pytest.fixture(scope="module")
def d13_dataset_id(e2e_pool: ConnectionPool) -> int:
    return PostgresMetadataRepository(e2e_pool).get_or_create_dataset(_D13_DATASET)


def _d13_source_and_ctx(  # noqa: PLR0913 -- one keyword per identity/config value, mirrors _e2e_source_and_ctx plus the per-test columns override
    *,
    dataset_id: int,
    object_store: S3ObjectStore,
    pool: ConnectionPool,
    key: str,
    run_id: int,
    columns: list[ColumnContract] | None = None,
) -> tuple[CsvSource, PipelineContext]:
    source = CsvSource(bucket=_E2E_BUCKET, key=key, dataset_id=dataset_id)
    ctx = PipelineContext(
        run=RunContext(run_id=run_id, idempotency_key=f"schema-resolution-d13:{run_id}"),
        config=_d13_config(columns),
        metadata=None,  # type: ignore[arg-type]  # unused by CsvSource.inspect()
        objects=object_store,
        db=pool,
        log=get_logger(),
        source=source,
    )
    return source, ctx


def test_a_file_missing_only_a_trailing_optional_column_resolves_to_the_contract_version(
    d13_dataset_id: int,
    e2e_object_store: S3ObjectStore,
    e2e_pool: ConnectionPool,
    migrated_dsn: str,
) -> None:
    """The finding-(20) repro: 5-col file vs 6-col contract with an optional 6th column.

    Bootstraps the dataset's history with a full-width contract-matching
    file first (mirroring CI, where the sweep corpus's 6-col files always
    arrive before any e2e fixture), then inspects the 5-col file: it must
    resolve to the SAME (CONTRACT) schema version, recording NO new
    version row -- the exact call that crashed every e2e single-file
    customers stage pod on CI with `schema-column-disappeared:
    signup_country`.
    """
    _upload_e2e_csv(
        e2e_object_store,
        key="d13_bootstrap.csv",
        header=_D13_FULL_HEADER,
        data_row="1,Alice,US,1990-01-01,2026-01-01T00:00:00+00:00,PL",
    )
    bootstrap_source, bootstrap_ctx = _d13_source_and_ctx(
        dataset_id=d13_dataset_id,
        object_store=e2e_object_store,
        pool=e2e_pool,
        key="d13_bootstrap.csv",
        run_id=95_601,
    )
    bootstrap_profile = bootstrap_source.inspect(bootstrap_ctx)
    assert bootstrap_profile.schema_version_id is not None

    _upload_e2e_csv(
        e2e_object_store,
        key="d13_prefix.csv",
        header=_D13_PREFIX_HEADER,
        data_row="2,Bob,US,1985-05-05,2026-02-01T00:00:00+00:00",
    )
    source, ctx = _d13_source_and_ctx(
        dataset_id=d13_dataset_id,
        object_store=e2e_object_store,
        pool=e2e_pool,
        key="d13_prefix.csv",
        run_id=95_602,
    )

    profile = source.inspect(ctx)

    assert profile.compatibility == "COMPATIBLE"
    assert profile.schema_version_id == bootstrap_profile.schema_version_id

    rows = _schema_version_rows(migrated_dsn, d13_dataset_id)
    assert len(rows) == 1  # no INFERRED flip, no spurious version


def test_a_file_missing_a_non_trailing_optional_column_still_raises(
    d13_dataset_id: int,
    e2e_object_store: S3ObjectStore,
    e2e_pool: ConnectionPool,
) -> None:
    """A MIDDLE optional column absent breaks positional loading -- must stay rejected.

    Contract order [customer_id, name, optional-middle, country, ...]; the
    file omits the middle column, so every later value would land one
    position early under the loader's positional COPY. Loudly rejected,
    never guessed (D-02).
    """
    middle_optional_columns = [
        _e2e_columns()[0],  # customer_id
        _e2e_columns()[1],  # name
        ColumnContract(
            name="middle_optional",
            type="string",
            nullable=True,
            required=False,
        ),
        *_e2e_columns()[2:],  # country, birth_date, event_ts
    ]
    _upload_e2e_csv(
        e2e_object_store,
        key="d13_middle_missing.csv",
        header=_D13_PREFIX_HEADER,
        data_row="3,Carol,GB,1970-07-07,2026-03-01T00:00:00+00:00",
    )
    source, ctx = _d13_source_and_ctx(
        dataset_id=d13_dataset_id,
        object_store=e2e_object_store,
        pool=e2e_pool,
        key="d13_middle_missing.csv",
        run_id=95_603,
        columns=middle_optional_columns,
    )

    with pytest.raises(IncompatibleSchemaError):
        source.inspect(ctx)


def test_a_new_column_combined_with_a_missing_optional_column_still_raises(
    d13_dataset_id: int,
    e2e_object_store: S3ObjectStore,
    e2e_pool: ConnectionPool,
    migrated_dsn: str,
) -> None:
    """The positional-corruption hole the prefix guard closes.

    A file missing the optional trailing contract column but carrying a
    genuinely NEW column in its position would -- if accepted as an
    INFERRED evolution -- have the new column's values positionally
    COPY'd into the absent contract column's slot by the loader's
    truncate-to-target-width behavior. Must raise, and must record no
    schema version.
    """
    before = _schema_version_rows(migrated_dsn, d13_dataset_id)
    _upload_e2e_csv(
        e2e_object_store,
        key="d13_new_col_in_optional_slot.csv",
        header=f"{_D13_PREFIX_HEADER},brand_new_column",
        data_row="4,Dave,PL,1966-06-06,2026-04-01T00:00:00+00:00,surprise",
    )
    source, ctx = _d13_source_and_ctx(
        dataset_id=d13_dataset_id,
        object_store=e2e_object_store,
        pool=e2e_pool,
        key="d13_new_col_in_optional_slot.csv",
        run_id=95_604,
    )

    with pytest.raises(IncompatibleSchemaError):
        source.inspect(ctx)

    after = _schema_version_rows(migrated_dsn, d13_dataset_id)
    assert after == before
