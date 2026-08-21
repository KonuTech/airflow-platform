"""Integration tests for ``dataplat.pipeline.run.publish_ingest`` (plan 08.1-10 Task 2).

Every test drives a real ``publish_ingest`` against real testcontainers
PostgreSQL -- no MinIO/CsvSource needed, since ``publish_ingest`` never reads
``ctx.source`` or ``ctx.objects`` at all (it operates purely on
``silver.<dataset>``, already resolved by ``ctx.config.dataset``).

Behaviors 2 and 3 below (``silver.<dataset>``/``staging.<dataset>`` seeding,
one call finalizing both `STAGED` runs, a second call proving idempotency)
are exercised as ONE test function rather than two, deliberately: both the
publish target (`normalized.customers`) and `publish_ingest`'s own source
table (`silver.customers`) are hardcoded, session-shared tables across the
WHOLE `tests/integration/` collection -- `publish_ingest`'s
`ctx.config.dataset` must therefore be the literal string `"customers"` for
these two tests, unlike Behavior 1's own isolated, never-touched-elsewhere
dataset name. Combining Behaviors 2/3 into one continuous test function keeps
the "call publish_ingest twice in a row, nothing else runs in between"
sequencing that Behavior 3 needs to prove genuinely un-ambiguous, without
depending on pytest's file-definition-order execution (this suite runs
sequentially, never under `-n auto`, but a single self-contained function is
more robust than relying on that convention alone).

``silver.<dataset>``/``staging.<dataset>`` rows are seeded directly via raw
SQL (never via a real `dbt build`), keeping this file Docker-only, not
`dbt`-marked (registered in pyproject.toml) -- isolating `publish_ingest`'s
own logic from plan 08.1-08's already-separately-tested dbt mechanism, per
this plan's own action text.

**[Rule 1 fix, plan 10-04]** ``_make_config``'s ``load.strategy`` was
``"merge"`` (resolving to ``MergePublisher``), which is UNCONDITIONALLY
broken against ``normalized.customers`` since migration 0035 (plan 10-01):
PostgreSQL rejects ``ON CONFLICT DO UPDATE`` against an exclusion-constraint
arbiter outright -- live-confirmed via `InvalidColumnReference` when this
file's own Behavior 2/3 test ran against the pre-fix config. Switched to
``"scd"`` (``SCDPublisher``, this plan's own Task 1), matching
``customers.yaml``'s real, live production strategy (this plan's Task 2).
``SCDPublisher``'s touched-key discovery/recompute read ``staging.customers``
(bronze) directly, never the caller-supplied ``source_table`` -- so this
file's own Behavior 2/3 test now ALSO seeds ``staging.customers`` (see
``_insert_bronze_row``), matching what a real ``stage_ingest()`` call would
have already promoted there. ``ScdConfig(delete_semantics="ignore",
mass_delete_threshold=1.0)`` -- never ``customers.yaml``'s own real
``"invalidate"``/``0.10`` -- for the identical session-shared-table reason
``test_publish_scd.py``'s own module docstring documents: this file's
``normalized.customers`` rows coexist with every OTHER test file's own rows
in the same table, and a real DELETE-semantics dispatch would incorrectly
act on keys this file never touched.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import psycopg
import pytest

from dataplat.config.model import (
    BatchingConfig,
    ColumnContract,
    DatasetConfig,
    DeduplicationConfig,
    LoadConfig,
    ScdConfig,
    SourceConfig,
)
from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.models.identity import RunContext
from dataplat.observability.logging import get_logger
from dataplat.pipeline.protocol import PipelineContext
from dataplat.pipeline.run import publish_ingest
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration


def _make_config(*, dataset: str) -> DatasetConfig:
    """A minimal `DatasetConfig` -- `publish_ingest` never reads `ctx.source`/`columns`
    for real work, but `DatasetConfig.columns` is required (never defaulted), so this
    still needs a well-formed, if unused, column list -- mirrors `test_run_ingest.py`'s
    own `_make_config()` shape.

    `load.strategy="scd"`/`scd=ScdConfig(...)` -- see this module's own
    docstring (Rule 1 fix, plan 10-04) for why `"merge"` no longer works
    here.
    """
    return DatasetConfig(
        dataset=dataset,
        config_schema_version=1,
        source=SourceConfig(
            type="csv",
            bucket="publish-ingest-test",
            path="customers/",
            change_semantics="snapshot",
            duplicate_policy="skip",
        ),
        deduplication=DeduplicationConfig(
            strategy="business_key_latest",
            keys=["customer_id"],
            order_by=["event_ts desc"],
        ),
        load=LoadConfig(strategy="scd", target="normalized.customers"),
        batching=BatchingConfig(max_units_per_run=100),
        columns=[
            ColumnContract(
                name="customer_id",
                type="string",
                nullable=False,
                required=True,
                business_key=True,
                description="Natural business key for a customer record",
            ),
            ColumnContract(
                name="name", type="string", nullable=False, required=True, scd_type="type_2"
            ),
            ColumnContract(
                name="country", type="string", nullable=False, required=True, scd_type="type_2"
            ),
            ColumnContract(
                name="birth_date",
                type="date",
                nullable=True,
                required=True,
                format="%Y-%m-%d",
                scd_type="type_1",
            ),
            ColumnContract(
                name="event_ts",
                type="timestamp",
                nullable=False,
                required=True,
                format="%Y-%m-%dT%H:%M:%S%z",
            ),
            ColumnContract(
                name="signup_country",
                type="string",
                nullable=True,
                required=False,
                scd_type="type_0",
            ),
        ],
        scd=ScdConfig(delete_semantics="ignore", mass_delete_threshold=1.0),
    )


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Get-or-insert a synthetic, CURRENT `meta.config_versions` row -- mirrors
    `test_publish_transaction_wiring.py`'s own helper: this file's Behavior 2/3 test
    seeds two runs under the SAME `dataset_id` ("customers"), so a second call must
    REUSE the first call's CURRENT row, not attempt a second one (migration 0001's
    `uq_config_versions_current_per_dataset` partial unique index).
    """
    with psycopg.connect(dsn) as conn:
        existing = conn.execute(
            """
            SELECT config_version_id
              FROM meta.config_versions
             WHERE dataset_id = %(dataset_id)s AND valid_to IS NULL
            """,
            {"dataset_id": dataset_id},
        ).fetchone()
        if existing is not None:
            return int(existing[0])
        row = conn.execute(
            """
            INSERT INTO meta.config_versions (
                dataset_id, version, config_hash, config_document,
                config_schema_version, valid_from
            ) VALUES (
                %(dataset_id)s,
                (
                    SELECT COALESCE(MAX(version), 0) + 1
                    FROM meta.config_versions
                    WHERE dataset_id = %(dataset_id)s
                ),
                %(config_hash)s, %(config_document)s::jsonb, %(config_schema_version)s, now()
            )
            RETURNING config_version_id
            """,
            {
                "dataset_id": dataset_id,
                "config_hash": "synthetic-hash-for-test",
                "config_document": json.dumps({"synthetic": True}),
                "config_schema_version": 1,
            },
        ).fetchone()
        assert row is not None
        return int(row[0])


def _seed_staged_run(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
    *,
    dataset_name: str,
    key_suffix: str,
) -> tuple[int, int, int, int]:
    """Create dataset/config_version/file/batch/STAGED run; return `(dataset_id, run_id,
    file_id, batch_id)`. `status="STAGED"` is inserted directly (`create_ingestion_run`
    accepts an initial `status`, mirroring `test_publish_merge.py`'s own `_seed_run`
    precedent) -- this file deliberately never drives a real `stage_ingest` call, keeping
    `publish_ingest`'s own tests isolated from plan 08.1-10 Task 1's already-separately-
    tested staging mechanism.
    """
    dataset_id = repository.get_or_create_dataset(dataset_name)
    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id)
    file_id = repository.create_file(
        dataset_id=dataset_id,
        object_uri=f"s3://raw/customers/{key_suffix}.csv",
        content_sha256=hashlib.sha256(key_suffix.encode()).digest(),
        hash_version=1,
        size_bytes=10,
        filename=f"{key_suffix}.csv",
        status="DISCOVERED",
    )
    batch_id = repository.create_batch(
        dataset_id=dataset_id,
        batch_key=f"{key_suffix}:2026-08-18:1",
        status="OPEN",
    )
    run_id = repository.create_ingestion_run(
        idempotency_key=f"{key_suffix}:1",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        status="STAGED",
        file_id=file_id,
        batch_id=batch_id,
    )
    return dataset_id, run_id, file_id, batch_id


def _insert_silver_row(  # noqa: PLR0913 -- one keyword per silver column, mirrors test_scd_delete_detection.py's helper shape
    conn: psycopg.Connection[Any],
    *,
    customer_id: str,
    name: str,
    country: str,
    birth_date: str,
    event_ts: str,
    run_id: int,
    file_id: int,
    batch_id: int,
    source_row_number: int,
    record_hash: bytes,
) -> None:
    """Seed one `silver.customers` row directly via SQL -- never via a real `dbt build`."""
    conn.execute(
        """
        INSERT INTO silver.customers (
            customer_id, name, country, birth_date, event_ts,
            _run_id, _file_id, _batch_id, _source_row_number,
            _record_hash, _record_hash_version
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
        """,
        (
            customer_id,
            name,
            country,
            birth_date,
            event_ts,
            run_id,
            file_id,
            batch_id,
            source_row_number,
            record_hash,
        ),
    )


def _insert_bronze_row(  # noqa: PLR0913 -- one keyword per bronze column, mirrors _insert_silver_row's own shape
    conn: psycopg.Connection[Any],
    *,
    customer_id: str,
    name: str,
    country: str,
    birth_date: str,
    event_ts: str,
    run_id: int,
    file_id: int,
    batch_id: int,
    source_row_number: int,
    record_hash: bytes,
) -> None:
    """Seed one `staging.customers` (durable bronze) row directly via SQL.

    ``SCDPublisher`` (plan 10-04) reads bronze directly for its per-key
    recompute -- never the caller-supplied ``source_table`` argument -- so
    this file's own Behavior 2/3 test needs a matching bronze row for every
    silver row it seeds, mirroring what a real `stage_ingest()` call would
    already have promoted (`Rule 1 fix` -- see this module's own docstring).
    """
    conn.execute(
        """
        INSERT INTO staging.customers (
            customer_id, name, country, birth_date, event_ts,
            _run_id, _file_id, _batch_id, _source_row_number,
            _record_hash, _record_hash_version
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
        """,
        (
            customer_id,
            name,
            country,
            birth_date,
            event_ts,
            run_id,
            file_id,
            batch_id,
            source_row_number,
            record_hash,
        ),
    )


@dataclass
class _Env:
    """This file's fixtures, bundled."""

    metadata: PostgresMetadataRepository
    pool: Any
    migrated_dsn: str


@pytest.fixture
def _pool(migrated_dsn: str) -> Iterator[Any]:
    opened_pool = create_pool(migrated_dsn)
    opened_pool.open(wait=True)
    try:
        yield opened_pool
    finally:
        opened_pool.close()


@pytest.fixture
def env(_pool: Any, migrated_dsn: str) -> _Env:
    return _Env(metadata=PostgresMetadataRepository(_pool), pool=_pool, migrated_dsn=migrated_dsn)


def _make_ctx(env: _Env, *, dataset: str) -> PipelineContext:
    """`publish_ingest` never reads `ctx.run`/`ctx.objects`/`ctx.log`'s own values -- a
    placeholder `RunContext`/`None` objects mirror `test_publish_merge.py`'s own
    `_make_context()` "fully placeholder" convention for fields the function under test
    never touches.
    """
    return PipelineContext(
        run=RunContext(run_id=0, idempotency_key="publish-ingest-test-placeholder"),
        config=_make_config(dataset=dataset),
        metadata=env.metadata,
        objects=None,  # type: ignore[arg-type] -- unused by publish_ingest
        db=env.pool,
        log=get_logger(),
    )


def _read_run_status(migrated_dsn: str, run_id: int) -> str:
    with psycopg.connect(migrated_dsn) as conn:
        row = conn.execute(
            "SELECT status FROM meta.ingestion_runs WHERE run_id = %s",
            (run_id,),
        ).fetchone()
    assert row is not None
    return str(row[0])


def _read_file_and_batch_status(
    migrated_dsn: str,
    *,
    file_id: int,
    batch_id: int,
) -> tuple[str, str]:
    with psycopg.connect(migrated_dsn) as conn:
        file_status = conn.execute(
            "SELECT status FROM meta.files WHERE file_id = %s",
            (file_id,),
        ).fetchone()
        batch_status = conn.execute(
            "SELECT status FROM meta.batches WHERE batch_id = %s",
            (batch_id,),
        ).fetchone()
    assert file_status is not None
    assert batch_status is not None
    return str(file_status[0]), str(batch_status[0])


def _read_run_stage_status(migrated_dsn: str, *, run_id: int, stage_name: str) -> str | None:
    with psycopg.connect(migrated_dsn) as conn:
        row = conn.execute(
            "SELECT status FROM meta.run_stages WHERE run_id = %s AND stage_name = %s",
            (run_id, stage_name),
        ).fetchone()
    return None if row is None else str(row[0])


def _read_customer_name(migrated_dsn: str, *, customer_id: int) -> str | None:
    with psycopg.connect(migrated_dsn) as conn:
        row = conn.execute(
            "SELECT name FROM normalized.customers WHERE customer_id = %s",
            (customer_id,),
        ).fetchone()
    return None if row is None else str(row[0])


def _normalized_customers_count(migrated_dsn: str) -> int:
    """Count DISTINCT `customer_id`s in `normalized.customers` (cardinality-aware, plan 10-04).

    Since migration 0035, `normalized.customers` may legitimately hold more
    than one physical ROW per `customer_id` (an SCD2 version chain) -- a
    plain `COUNT(*)` would conflate "how many distinct customers exist" with
    "how many SCD2 version rows exist," two different questions. Every call
    site below asks the former ("did this call add any NEW rows / customers
    at all") -- `COUNT(DISTINCT customer_id)` is the cardinality-safe answer.
    """
    with psycopg.connect(migrated_dsn) as conn:
        row = conn.execute(
            "SELECT COUNT(DISTINCT customer_id) FROM normalized.customers",
        ).fetchone()
    assert row is not None
    return int(row[0])


# --- Behavior 1: zero STAGED runs -> a clean, cheap no-op ------------------


def test_zero_staged_runs_is_a_clean_no_op(env: _Env) -> None:
    """No advisory lock, no publish SQL, no connection even opened -- an isolated,
    never-touched-elsewhere dataset name proves this without depending on any other
    test's STAGED-run state.
    """
    ctx = _make_ctx(env, dataset="publish_ingest_never_staged")

    result = publish_ingest(ctx)

    assert result == {
        "status": "SUCCEEDED",
        "runs_finalized": [],
        "rows_loaded": 0,
        "duration_ms": result["duration_ms"],
    }
    assert isinstance(result["duration_ms"], int)
    assert result["duration_ms"] >= 0


# --- Behaviors 2 & 3: two STAGED runs finalize together; a second call -----
# --- immediately afterward is idempotent ------------------------------------


def test_two_staged_runs_finalize_together_and_a_second_call_is_idempotent(
    env: _Env,
) -> None:
    ctx = _make_ctx(env, dataset="customers")

    _dataset_id_a, run_id_a, file_id_a, batch_id_a = _seed_staged_run(
        env.metadata,
        env.migrated_dsn,
        dataset_name="customers",
        key_suffix="publish_ingest_two_a",
    )
    _dataset_id_b, run_id_b, file_id_b, batch_id_b = _seed_staged_run(
        env.metadata,
        env.migrated_dsn,
        dataset_name="customers",
        key_suffix="publish_ingest_two_b",
    )

    customer_id_a = "9600001"
    customer_id_b = "9600002"
    with psycopg.connect(env.migrated_dsn) as conn:
        _insert_silver_row(
            conn,
            customer_id=customer_id_a,
            name="FromRunA",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-08-18T10:00:00+00:00",
            run_id=run_id_a,
            file_id=file_id_a,
            batch_id=batch_id_a,
            source_row_number=1,
            record_hash=hashlib.sha256(b"publish-ingest-a").digest(),
        )
        _insert_silver_row(
            conn,
            customer_id=customer_id_b,
            name="FromRunB",
            country="CA",
            birth_date="1990-01-01",
            event_ts="2026-08-18T10:00:00+00:00",
            run_id=run_id_b,
            file_id=file_id_b,
            batch_id=batch_id_b,
            source_row_number=1,
            record_hash=hashlib.sha256(b"publish-ingest-b").digest(),
        )
        # SCDPublisher (plan 10-04) reads bronze, not silver, for its
        # touched-key discovery/recompute -- see this module's own docstring.
        _insert_bronze_row(
            conn,
            customer_id=customer_id_a,
            name="FromRunA",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-08-18T10:00:00+00:00",
            run_id=run_id_a,
            file_id=file_id_a,
            batch_id=batch_id_a,
            source_row_number=1,
            record_hash=hashlib.sha256(b"publish-ingest-a").digest(),
        )
        _insert_bronze_row(
            conn,
            customer_id=customer_id_b,
            name="FromRunB",
            country="CA",
            birth_date="1990-01-01",
            event_ts="2026-08-18T10:00:00+00:00",
            run_id=run_id_b,
            file_id=file_id_b,
            batch_id=batch_id_b,
            source_row_number=1,
            record_hash=hashlib.sha256(b"publish-ingest-b").digest(),
        )
        conn.commit()

    result = publish_ingest(ctx)

    assert result["status"] == "SUCCEEDED"
    runs_finalized = result["runs_finalized"]
    assert isinstance(runs_finalized, list)
    assert set(runs_finalized) == {run_id_a, run_id_b}
    assert isinstance(result["rows_loaded"], int)
    assert result["rows_loaded"] >= 2  # aggregate, per-pass count -- see run.py's own note

    assert _read_run_status(env.migrated_dsn, run_id_a) == "SUCCEEDED"
    assert _read_run_status(env.migrated_dsn, run_id_b) == "SUCCEEDED"

    file_status_a, batch_status_a = _read_file_and_batch_status(
        env.migrated_dsn,
        file_id=file_id_a,
        batch_id=batch_id_a,
    )
    assert file_status_a == "PROCESSED"
    assert batch_status_a == "PUBLISHED"
    file_status_b, batch_status_b = _read_file_and_batch_status(
        env.migrated_dsn,
        file_id=file_id_b,
        batch_id=batch_id_b,
    )
    assert file_status_b == "PROCESSED"
    assert batch_status_b == "PUBLISHED"

    assert _read_run_stage_status(env.migrated_dsn, run_id=run_id_a, stage_name="PUBLISH") == (
        "SUCCEEDED"
    )
    assert _read_run_stage_status(env.migrated_dsn, run_id=run_id_b, stage_name="PUBLISH") == (
        "SUCCEEDED"
    )

    assert _read_customer_name(env.migrated_dsn, customer_id=int(customer_id_a)) == "FromRunA"
    assert _read_customer_name(env.migrated_dsn, customer_id=int(customer_id_b)) == "FromRunB"

    # --- Behavior 3: re-running immediately afterward, no new STAGED runs, ---
    # --- silver unchanged -- must be a no-op -----------------------------------
    before_count = _normalized_customers_count(env.migrated_dsn)

    second_result = publish_ingest(ctx)

    assert second_result == {
        "status": "SUCCEEDED",
        "runs_finalized": [],
        "rows_loaded": 0,
        "duration_ms": second_result["duration_ms"],
    }
    after_count = _normalized_customers_count(env.migrated_dsn)
    assert after_count == before_count
    assert _read_customer_name(env.migrated_dsn, customer_id=int(customer_id_a)) == "FromRunA"
    assert _read_customer_name(env.migrated_dsn, customer_id=int(customer_id_b)) == "FromRunB"
