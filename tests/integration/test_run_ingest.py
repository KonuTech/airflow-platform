"""Integration tests for ``dataplat.pipeline.run.stage_ingest``/``publish_ingest``.

Originally written for the single ``run_ingest`` function (plan 04-05 Task
1); migrated here after plan 08.1-10 split it into ``stage_ingest`` (claim,
stream through ``StagingLoader``, quality-gate, promote to durable bronze,
mark ``STAGED``) and ``publish_ingest`` (claim every currently-``STAGED``
run for one dataset, publish ``silver.<dataset>`` into ``normalized.
<dataset>``, finalize). A real ``dbt build`` normally bridges the two in
production; these tests seed ``silver.customers`` directly via SQL instead
(mirroring ``test_publish_ingest.py``'s own convention) wherever a test
needs the full stage-then-publish path, keeping this file's own proofs
isolated from the separately-tested dbt hop (plan 08.1-08). A test whose own
point is purely about staging (schema-version resolution, claim-refusal
skip logic, the in-flight heartbeat, trace-context propagation at claim
time) calls ``stage_ingest`` alone and never touches ``publish_ingest``.

Every test drives real functions against real testcontainers PostgreSQL +
MinIO -- the exact claim/stage/publish/receipt orchestration a real
``stage``/``publish`` pod pair executes, using a real
``csv_processor.source.CsvSource`` (never a fake/in-memory source): this
file's whole point is proving the FULL, source-to-database path, not merely
a mocked slice of it.

``_seed_pending_run`` mirrors the real discovery-time flow
(``get_or_create_dataset`` -> a synthetic config version ->
``create_file``/``create_batch`` -> ``get_or_create_ingestion_run``) closely
enough that every FK ``stage_ingest``/``publish_ingest`` touch is real,
matching ``test_publish_merge.py``'s own ``_seed_run`` precedent. It accepts
an optional ``dataset_name`` override, defaulting to a per-test-unique name
(``run_ingest_<key_suffix>``): a test that never calls ``publish_ingest``
keeps that isolation, but any test that DOES call ``publish_ingest`` must
pass ``dataset_name="customers"`` -- ``publish_ingest`` resolves
``ctx.config.dataset`` ("customers", ``_make_config``'s own hardcoded
value) to a ``dataset_id`` and looks up currently-``STAGED`` runs by THAT
id (``list_staged_run_ids``), so a run created under any other dataset name
would never be found as staged for "customers", even though staging itself
(which only needs ``ctx.config.dataset`` to resolve target columns, a
string lookup) would have succeeded either way.

Every test uses its own, widely-separated ``customer_id`` range
(9_100_0xx and up) -- ``normalized.customers.customer_id`` is unique across
the WHOLE table, not scoped per dataset, and `tests/integration/`'s
Postgres container is session-scoped and shared with every other file in
this directory (`test_publish_merge.py` already occupies the 2000/3001/4001/
5001 range) -- so collisions are a real risk, not a theoretical one.

``_Env`` bundles this file's own fixtures (``metadata``/``objects``/``pool``/
``migrated_dsn``/``s3_client``/``scratch_bucket``) into one object so each
test function takes one parameter instead of six -- a plain pytest style
choice, not a behavior change; every field is still a real, independently
resolvable fixture underneath.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import psycopg
import pytest
from opentelemetry import context as otel_context
from opentelemetry import propagate
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import dataplat.pipeline.run as run_module
from csv_processor.source import CsvSource
from dataplat.config.model import (
    BatchingConfig,
    ColumnContract,
    DatasetConfig,
    DeduplicationConfig,
    LoadConfig,
    SourceConfig,
)
from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.models.identity import RunContext
from dataplat.observability import tracing
from dataplat.observability.logging import get_logger
from dataplat.pipeline.protocol import PipelineContext
from dataplat.pipeline.run import publish_ingest, stage_ingest
from dataplat.storage.db import create_pool
from dataplat.storage.objectstore import S3ObjectStore

if TYPE_CHECKING:
    from collections.abc import Iterator

_BUCKET = "run-ingest-test"
_CSV_HEADER = "customer_id,name,country,birth_date,event_ts\n"
# plan 08-11: every STAGED run now writes a report.json artifact to the
# real "validated" bucket (VALID-04's MinIO-artifact half) -- this file's
# throwaway MinioContainer is a bare instance, not the Helm-provisioned
# cluster, so the bucket must be created here too, mirroring
# `_scratch_bucket`'s own create-if-absent pattern.
_VALIDATED_BUCKET = "validated"


def _csv_bytes(rows: int, *, start_id: int) -> bytes:
    """Build a tiny, well-formed customers CSV: `rows` records starting at `start_id`."""
    lines = [_CSV_HEADER]
    for offset in range(rows):
        customer_id = start_id + offset
        lines.append(
            f"{customer_id},Name{customer_id},US,1990-01-01,2026-01-01T00:00:00+00:00\n",
        )
    return "".join(lines).encode("utf-8")


def _make_config() -> DatasetConfig:
    """A `DatasetConfig` matching `stage_ingest`/`publish_ingest`'s own hardcoded customers assumptions.

    columns= is required (06-02 Task 1/3, D-18) -- added here purely to stay
    constructible; neither function reads DatasetConfig.columns.
    """  # noqa: E501, W505
    return DatasetConfig(
        dataset="customers",
        config_schema_version=1,
        source=SourceConfig(
            type="csv",
            bucket=_BUCKET,
            path="customers/",
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
        columns=[
            ColumnContract(
                name="customer_id",
                type="string",
                nullable=False,
                required=True,
                business_key=True,
                description="Natural business key for a customer record",
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
        ],
    )


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Get-or-insert a synthetic, CURRENT `meta.config_versions` row.

    A per-test-unique dataset (the default in `_seed_pending_run`) never
    has an existing CURRENT row, so this always falls through to the
    `INSERT` below for those callers -- but tests that pass
    `dataset_name="customers"` share that dataset's `dataset_id` across
    this WHOLE file (and several sibling files), so a second call for it
    must REUSE the first call's row rather than attempt a second CURRENT
    insert (migration 0001's `uq_config_versions_current_per_dataset`
    partial unique index) -- mirrors `test_publish_ingest.py`'s own helper.
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


@dataclass
class _Env:
    """This file's fixtures, bundled -- see the module docstring."""

    metadata: PostgresMetadataRepository
    objects: S3ObjectStore
    pool: Any
    migrated_dsn: str
    s3_client: Any
    scratch_bucket: str


@pytest.fixture
def _pool(migrated_dsn: str) -> Iterator[Any]:
    """An opened `ConnectionPool` over the migrated database, closed after the test."""
    opened_pool = create_pool(migrated_dsn)
    opened_pool.open(wait=True)
    try:
        yield opened_pool
    finally:
        opened_pool.close()


@pytest.fixture
def _scratch_bucket(s3_client: Any) -> str:
    """Ensure this file's scratch bucket exists; return its name (mirrors `test_objectstore.py`)."""
    existing = {bucket["Name"] for bucket in s3_client.list_buckets().get("Buckets", [])}
    if _BUCKET not in existing:
        s3_client.create_bucket(Bucket=_BUCKET)
    return _BUCKET


@pytest.fixture
def _validated_bucket(s3_client: Any) -> str:
    """Ensure the real "validated" bucket exists; return its name (plan 08-11's report artifact)."""
    existing = {bucket["Name"] for bucket in s3_client.list_buckets().get("Buckets", [])}
    if _VALIDATED_BUCKET not in existing:
        s3_client.create_bucket(Bucket=_VALIDATED_BUCKET)
    return _VALIDATED_BUCKET


@pytest.fixture
def env(
    _pool: Any,
    migrated_dsn: str,
    s3_client: Any,
    minio_config: dict[str, str],
    _scratch_bucket: str,
    _validated_bucket: str,
) -> _Env:
    """Compose every fixture this file's tests need into one `_Env`."""
    return _Env(
        metadata=PostgresMetadataRepository(_pool),
        objects=S3ObjectStore(
            endpoint_url=f"http://{minio_config['endpoint']}",
            access_key=minio_config["access_key"],
            secret_key=minio_config["secret_key"],
        ),
        pool=_pool,
        migrated_dsn=migrated_dsn,
        s3_client=s3_client,
        scratch_bucket=_scratch_bucket,
    )


def _seed_pending_run(
    env: _Env,
    *,
    key_suffix: str,
    csv_bytes: bytes,
    dataset_name: str | None = None,
) -> tuple[int, int, int, str]:
    """Seed dataset/config/file/batch/PENDING-run and upload `csv_bytes`.

    Mirrors the real discover_files flow closely enough that every FK
    `stage_ingest`/`publish_ingest` touch (file_id, batch_id,
    config_version_id) is real.

    Args:
        env: This file's bundled fixtures.
        key_suffix: Uniquifies this run's object key/batch key/idempotency
            key.
        csv_bytes: The file body to upload.
        dataset_name: The `meta.datasets` row this run is created under.
            Defaults to a per-test-unique name (`run_ingest_<key_suffix>`)
            -- pass `"customers"` explicitly for any test that goes on to
            call `publish_ingest` (see the module docstring for why).

    Returns:
        `(run_id, file_id, batch_id, object_key)`.
    """
    resolved_dataset_name = dataset_name if dataset_name is not None else f"run_ingest_{key_suffix}"
    dataset_id = env.metadata.get_or_create_dataset(resolved_dataset_name)
    config_version_id = _insert_config_version(env.migrated_dsn, dataset_id=dataset_id)
    object_key = f"customers/{key_suffix}.csv"
    env.s3_client.put_object(Bucket=env.scratch_bucket, Key=object_key, Body=csv_bytes)
    file_id = env.metadata.create_file(
        dataset_id=dataset_id,
        object_uri=f"s3://{env.scratch_bucket}/{object_key}",
        content_sha256=hashlib.sha256(csv_bytes).digest(),
        hash_version=1,
        size_bytes=len(csv_bytes),
        filename=f"{key_suffix}.csv",
        status="DISCOVERED",
    )
    batch_id = env.metadata.create_batch(
        dataset_id=dataset_id,
        batch_key=f"{key_suffix}:2026-08-13:1",
        status="OPEN",
    )
    run_id, _ = env.metadata.get_or_create_ingestion_run(
        idempotency_key=f"run_ingest_{key_suffix}:1",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        file_id=file_id,
        batch_id=batch_id,
    )
    return run_id, file_id, batch_id, object_key


def _make_ctx(
    env: _Env,
    *,
    run_id: int,
    file_id: int,
    batch_id: int,
    object_key: str,
) -> PipelineContext:
    """Build a `PipelineContext` wired to a real `CsvSource` over `env`'s scratch bucket."""
    key_suffix = object_key.rsplit("/", 1)[-1].removesuffix(".csv")
    return PipelineContext(
        run=RunContext(
            run_id=run_id,
            idempotency_key=f"run_ingest_{key_suffix}:1",
            file_id=file_id,
            batch_id=batch_id,
        ),
        config=_make_config(),
        metadata=env.metadata,
        objects=env.objects,
        db=env.pool,
        log=get_logger(),
        source=CsvSource(bucket=env.scratch_bucket, key=object_key),
    )


def _seed_and_build_ctx(
    env: _Env,
    *,
    key_suffix: str,
    csv_bytes: bytes,
    dataset_name: str | None = None,
) -> tuple[PipelineContext, int, int, int]:
    """`_seed_pending_run` + `_make_ctx` in one call -- every test's setup boils down to this."""
    run_id, file_id, batch_id, object_key = _seed_pending_run(
        env,
        key_suffix=key_suffix,
        csv_bytes=csv_bytes,
        dataset_name=dataset_name,
    )
    ctx = _make_ctx(env, run_id=run_id, file_id=file_id, batch_id=batch_id, object_key=object_key)
    return ctx, run_id, file_id, batch_id


def _read_run_status(migrated_dsn: str, run_id: int) -> str:
    with psycopg.connect(migrated_dsn) as conn:
        row = conn.execute(
            "SELECT status FROM meta.ingestion_runs WHERE run_id = %s",
            (run_id,),
        ).fetchone()
    assert row is not None
    return str(row[0])


def _read_customers_count_for_run(migrated_dsn: str, run_id: int) -> int:
    with psycopg.connect(migrated_dsn) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM normalized.customers WHERE _run_id = %s",
            (run_id,),
        ).fetchone()
    assert row is not None
    return int(row[0])


def _durable_bronze_count_for_run(migrated_dsn: str, run_id: int) -> int:
    """Count `staging.customers` (the durable, cumulative bronze table, migration 0022) rows."""
    with psycopg.connect(migrated_dsn) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM staging.customers WHERE _run_id = %s",
            (run_id,),
        ).fetchone()
    assert row is not None
    return int(row[0])


def _staging_table_exists(migrated_dsn: str, run_id: int) -> bool:
    with psycopg.connect(migrated_dsn) as conn:
        row = conn.execute(
            "SELECT to_regclass(%s)",
            (f"staging.customers__r{run_id}",),
        ).fetchone()
    assert row is not None
    return row[0] is not None


def _insert_silver_row(  # noqa: PLR0913 -- one keyword per silver column, mirrors test_publish_ingest.py's own helper
    conn: psycopg.Connection[Any],
    *,
    customer_id: str,
    name: str,
    run_id: int,
    file_id: int,
    batch_id: int,
    source_row_number: int,
) -> None:
    """Seed one `silver.customers` row directly via SQL -- never via a real `dbt build`.

    Post-08.1-10, `publish_ingest` reads FROM `silver.<dataset>`, never
    from the durable bronze table `stage_ingest` itself writes -- this
    file's own tests seed silver directly (mirrors `test_publish_ingest.py`
    's own convention) wherever a test needs a real publish, isolating
    this file's proofs from the separately-tested dbt hop (plan 08.1-08).
    """
    conn.execute(
        """
        INSERT INTO silver.customers (
            customer_id, name, country, birth_date, event_ts,
            _run_id, _file_id, _batch_id, _source_row_number,
            _record_hash, _record_hash_version
        ) VALUES (%s, %s, 'US', '1990-01-01', '2026-01-01T00:00:00+00:00', %s, %s, %s, %s, %s, 1)
        """,
        (
            customer_id,
            name,
            run_id,
            file_id,
            batch_id,
            source_row_number,
            hashlib.sha256(f"{customer_id}:{name}".encode()).digest(),
        ),
    )


def _seed_silver_rows_matching_csv(  # noqa: PLR0913 -- one keyword per silver-seed identity/range value
    env: _Env,
    *,
    run_id: int,
    file_id: int,
    batch_id: int,
    start_id: int,
    count: int,
) -> None:
    """Seed `count` `silver.customers` rows mirroring `_csv_bytes`'s own row shape."""
    with psycopg.connect(env.migrated_dsn) as conn:
        for offset in range(count):
            customer_id = start_id + offset
            _insert_silver_row(
                conn,
                customer_id=str(customer_id),
                name=f"Name{customer_id}",
                run_id=run_id,
                file_id=file_id,
                batch_id=batch_id,
                source_row_number=offset + 1,
            )
        conn.commit()


# --- Behavior 1: the full success path -------------------------------------


def test_successful_run_publishes_and_marks_everything_succeeded(env: _Env) -> None:
    ctx, run_id, file_id, batch_id = _seed_and_build_ctx(
        env,
        key_suffix="happy",
        csv_bytes=_csv_bytes(3, start_id=9_100_001),
        dataset_name="customers",
    )

    stage_receipt = stage_ingest(ctx)

    assert stage_receipt.status == "STAGED"
    assert stage_receipt.run_id == run_id
    assert stage_receipt.rows_read == 3
    assert stage_receipt.rows_loaded == 0  # staging never writes to gold
    assert stage_receipt.rows_invalid == 0

    # A real `dbt build` normally bridges bronze -> silver between the two
    # calls; seeded directly here (module docstring) so `publish_ingest`
    # has something to actually publish.
    _seed_silver_rows_matching_csv(
        env,
        run_id=run_id,
        file_id=file_id,
        batch_id=batch_id,
        start_id=9_100_001,
        count=3,
    )

    publish_result = publish_ingest(ctx)

    assert publish_result["status"] == "SUCCEEDED"
    assert run_id in publish_result["runs_finalized"]
    assert publish_result["rows_loaded"] >= 3  # aggregate, per-pass count -- see run.py's own note

    with psycopg.connect(env.migrated_dsn) as conn:
        file_status = conn.execute(
            "SELECT status FROM meta.files WHERE file_id = %s",
            (file_id,),
        ).fetchone()
        batch_status = conn.execute(
            "SELECT status FROM meta.batches WHERE batch_id = %s",
            (batch_id,),
        ).fetchone()
    assert _read_run_status(env.migrated_dsn, run_id) == "SUCCEEDED"
    assert file_status is not None
    assert file_status[0] == "PROCESSED"
    assert batch_status is not None
    assert batch_status[0] == "PUBLISHED"
    # dropped after promotion to durable bronze, inside stage_ingest itself
    assert not _staging_table_exists(env.migrated_dsn, run_id)


def test_staged_run_records_its_resolved_schema_version_on_the_run(env: _Env) -> None:
    """Post-wave-5 code review verification Gap 1: ``meta.ingestion_runs.schema_version_id``
    must actually be populated by a real ``stage_ingest()`` call, not just resolved and
    discarded inside ``CsvSource.open()``.

    A pure staging concern (``stage_ingest``'s own ``STAGED`` status update sets
    ``schema_version_id`` -- see ``run.py``'s own ``update_ingestion_run_status`` call);
    this test never needs ``publish_ingest`` at all. Mirrors ``_seed_pending_run``/
    ``_make_ctx`` but wires ``CsvSource(dataset_id=...)`` -- the existing shared helpers
    deliberately omit ``dataset_id`` (skips schema resolution entirely), so this test
    builds its own context rather than changing behavior every other test in this file
    relies on.
    """
    dataset_id = env.metadata.get_or_create_dataset("run_ingest_schema_version_proof")
    config_version_id = _insert_config_version(env.migrated_dsn, dataset_id=dataset_id)
    csv_bytes = _csv_bytes(2, start_id=9_200_001)
    object_key = "customers/schema_version_proof.csv"
    env.s3_client.put_object(Bucket=env.scratch_bucket, Key=object_key, Body=csv_bytes)
    file_id = env.metadata.create_file(
        dataset_id=dataset_id,
        object_uri=f"s3://{env.scratch_bucket}/{object_key}",
        content_sha256=hashlib.sha256(csv_bytes).digest(),
        hash_version=1,
        size_bytes=len(csv_bytes),
        filename="schema_version_proof.csv",
        status="DISCOVERED",
    )
    batch_id = env.metadata.create_batch(
        dataset_id=dataset_id,
        batch_key="schema_version_proof:2026-08-15:1",
        status="OPEN",
    )
    run_id, _ = env.metadata.get_or_create_ingestion_run(
        idempotency_key="run_ingest_schema_version_proof:1",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        file_id=file_id,
        batch_id=batch_id,
    )
    ctx = PipelineContext(
        run=RunContext(
            run_id=run_id,
            idempotency_key="run_ingest_schema_version_proof:1",
            file_id=file_id,
            batch_id=batch_id,
        ),
        config=_make_config(),
        metadata=env.metadata,
        objects=env.objects,
        db=env.pool,
        log=get_logger(),
        source=CsvSource(bucket=env.scratch_bucket, key=object_key, dataset_id=dataset_id),
    )

    receipt = stage_ingest(ctx)

    assert receipt.status == "STAGED"
    with psycopg.connect(env.migrated_dsn) as conn:
        row = conn.execute(
            "SELECT schema_version_id FROM meta.ingestion_runs WHERE run_id = %s",
            (run_id,),
        ).fetchone()
        version_row = conn.execute(
            "SELECT dataset_id FROM meta.schema_versions WHERE schema_version_id = %s",
            (row[0] if row else None,),
        ).fetchone()
    assert row is not None
    assert row[0] is not None  # NOT NULL -- the exact gap this test closes
    assert version_row is not None
    assert version_row[0] == dataset_id  # points at THIS run's own dataset's schema history


# --- Behavior 2: already-terminal -> SKIPPED_DUPLICATE ---------------------


def test_already_succeeded_run_returns_skipped_duplicate_and_touches_no_staging_table(
    env: _Env,
) -> None:
    ctx, run_id, _file_id, _batch_id = _seed_and_build_ctx(
        env,
        key_suffix="dup",
        csv_bytes=_csv_bytes(1, start_id=9_100_101),
    )
    env.metadata.update_ingestion_run_status(run_id=run_id, status="SUCCEEDED")

    receipt = stage_ingest(ctx)

    assert receipt.status == "SKIPPED_DUPLICATE"
    assert receipt.run_id == run_id
    assert receipt.rows_loaded == 0
    assert not _staging_table_exists(env.migrated_dsn, run_id)
    assert _read_customers_count_for_run(env.migrated_dsn, run_id) == 0
    assert _durable_bronze_count_for_run(env.migrated_dsn, run_id) == 0


# --- Behavior 3: RUNNING + live lease -> SKIPPED_CONCURRENT -----------------


def test_running_run_with_a_live_lease_returns_skipped_concurrent(env: _Env) -> None:
    ctx, run_id, _file_id, _batch_id = _seed_and_build_ctx(
        env,
        key_suffix="concurrent",
        csv_bytes=_csv_bytes(1, start_id=9_100_201),
    )
    live_lease = datetime.now(tz=UTC) + timedelta(minutes=5)
    env.metadata.update_ingestion_run_status(
        run_id=run_id,
        status="RUNNING",
        lease_expires_at=live_lease,
        k8s_pod_name="other-pod-holding-the-lease",
    )

    receipt = stage_ingest(ctx)

    assert receipt.status == "SKIPPED_CONCURRENT"
    assert receipt.run_id == run_id
    assert receipt.rows_loaded == 0
    assert not _staging_table_exists(env.migrated_dsn, run_id)
    assert _durable_bronze_count_for_run(env.migrated_dsn, run_id) == 0


# --- Behavior 4: crash between staging and publish, then a clean retry -----


def test_crash_between_staging_and_publish_leaves_no_partial_state_and_retry_succeeds(
    env: _Env,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``resolve_publisher`` lives inside ``publish_ingest`` only, post-08.1-10 -- the
    "crash between staging and publish" fault this test injects is now literally the
    boundary between the two calls: ``stage_ingest`` (real, unpatched) completes and
    reaches ``STAGED`` before the patch is even installed, then ``publish_ingest`` raises
    before its transaction opens.

    Unlike the pre-split version, no lease-expiry simulation is needed for the retry: a
    failed ``publish_ingest`` call's own ``claim_run_stage`` INSERT lives inside the SAME
    transaction as the fault (never committed), so a second ``publish_ingest`` call finds
    the run still cleanly ``STAGED`` and claims its ``PUBLISH`` stage fresh -- there is no
    lease to have gone stale.
    """
    ctx, run_id, file_id, batch_id = _seed_and_build_ctx(
        env,
        key_suffix="crash",
        csv_bytes=_csv_bytes(2, start_id=9_100_301),
        dataset_name="customers",
    )

    stage_receipt = stage_ingest(ctx)
    assert stage_receipt.status == "STAGED"

    _seed_silver_rows_matching_csv(
        env,
        run_id=run_id,
        file_id=file_id,
        batch_id=batch_id,
        start_id=9_100_301,
        count=2,
    )

    def _simulate_crash_before_publish_begins(strategy: str) -> Any:
        del strategy
        msg = "simulated crash before the publish transaction begins"
        raise RuntimeError(msg)

    monkeypatch.setattr(run_module, "resolve_publisher", _simulate_crash_before_publish_begins)

    with pytest.raises(RuntimeError, match="simulated crash"):
        publish_ingest(ctx)

    # Publish never began -- normalized.customers must be untouched and the
    # run must still be STAGED, never SUCCEEDED.
    assert _read_customers_count_for_run(env.migrated_dsn, run_id) == 0
    assert _read_run_status(env.migrated_dsn, run_id) == "STAGED"

    monkeypatch.undo()  # restore the real resolve_publisher for the retry

    publish_result = publish_ingest(ctx)

    assert publish_result["status"] == "SUCCEEDED"
    assert run_id in publish_result["runs_finalized"]
    # Exactly 2 rows land, never 4 -- the retry re-publishes, it never re-stages.
    assert _read_customers_count_for_run(env.migrated_dsn, run_id) == 2
    assert _read_run_status(env.migrated_dsn, run_id) == "SUCCEEDED"


# --- Behavior 5: the heartbeat keeps rows_read/rows_parsed genuinely live --


def test_heartbeat_writes_a_live_nonzero_rows_read_while_running_before_return(
    env: _Env,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Heartbeat behavior during staging is purely a ``stage_ingest`` concern
    (D-11) -- this test never needs ``publish_ingest``.
    """
    ctx, run_id, _file_id, _batch_id = _seed_and_build_ctx(
        env,
        key_suffix="heartbeat",
        csv_bytes=_csv_bytes(1, start_id=9_100_401),
    )

    resume_staging = threading.Event()
    real_staging_loader_cls = run_module.StagingLoader

    class _PausesAfterStagingLoader:
        """Delegates to the REAL StagingLoader, then blocks before returning.

        The real load() has already fired on_progress with the true,
        final counts by the time this blocks -- the heartbeat thread (a
        short, test-only interval) gets a deterministic window to observe
        and persist them while stage_ingest is provably still RUNNING,
        before stage_ingest can proceed to promote-to-durable-bronze and
        return.
        """

        def __init__(self, *, target_columns: tuple[str, ...]) -> None:
            self._inner = real_staging_loader_cls(target_columns=target_columns)

        def load(self, ctx: Any, conn: Any, *, on_progress: Any = None) -> Any:
            result = self._inner.load(ctx, conn, on_progress=on_progress)
            resume_staging.wait(timeout=10)
            return result

        def promote_to_durable_bronze(self, ctx: Any, conn: Any, staging_result: Any) -> Any:
            return self._inner.promote_to_durable_bronze(ctx, conn, staging_result)

    monkeypatch.setattr(run_module, "StagingLoader", _PausesAfterStagingLoader)

    result_holder: list[Any] = []
    error_holder: list[Exception] = []

    def _run_in_background() -> None:
        try:
            result_holder.append(stage_ingest(ctx, heartbeat_interval_seconds=0.05))
        except Exception as exc:  # noqa: BLE001 -- captured so the main thread can assert on it
            error_holder.append(exc)

    thread = threading.Thread(target=_run_in_background)
    thread.start()
    try:
        deadline = time.monotonic() + 10
        observed_live_progress = False
        while time.monotonic() < deadline:
            with psycopg.connect(env.migrated_dsn) as probe:
                row = probe.execute(
                    "SELECT rows_read, status FROM meta.ingestion_runs WHERE run_id = %s",
                    (run_id,),
                ).fetchone()
            if row is not None and row[0] is not None and row[0] > 0 and row[1] == "RUNNING":
                observed_live_progress = True
                break
            time.sleep(0.02)
    finally:
        resume_staging.set()
        thread.join(timeout=10)

    assert not error_holder, f"stage_ingest raised on the background thread: {error_holder}"
    assert observed_live_progress, "heartbeat never wrote a live rows_read while status=RUNNING"
    assert len(result_holder) == 1
    assert result_holder[0].status == "STAGED"
    assert result_holder[0].rows_read == 1
    assert result_holder[0].rows_loaded == 0  # staging never writes to gold
    assert _durable_bronze_count_for_run(env.migrated_dsn, run_id) == 1


# --- The publish transaction's four effects are invisible until commit -----


def test_publish_transaction_effects_are_invisible_to_another_connection_until_commit(
    env: _Env,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx, run_id, file_id, batch_id = _seed_and_build_ctx(
        env,
        key_suffix="atomic",
        csv_bytes=_csv_bytes(2, start_id=9_100_501),
        dataset_name="customers",
    )

    stage_receipt = stage_ingest(ctx)
    assert stage_receipt.status == "STAGED"

    _seed_silver_rows_matching_csv(
        env,
        run_id=run_id,
        file_id=file_id,
        batch_id=batch_id,
        start_id=9_100_501,
        count=2,
    )

    ready = threading.Event()
    proceed = threading.Event()
    real_resolve_publisher = run_module.resolve_publisher

    class _BlocksAfterInsertingPublisher:
        """Runs the REAL MergePublisher's INSERT, then blocks before returning.

        `finalize_publication` (the caller's next statement, in the SAME
        still-open transaction) has therefore also not run yet at the
        pause point -- so a probe connection reading meta.files/meta.batches
        while paused sees state genuinely unchanged from before publish
        began, not merely "not yet visible".
        """

        name = "merge"

        def publish(self, ctx: Any, staging_table: str, conn: Any) -> Any:
            inner = real_resolve_publisher("merge")
            result = inner.publish(ctx, staging_table, conn)
            ready.set()
            proceed.wait(timeout=10)
            return result

    def _fake_resolve_publisher(strategy: str) -> Any:
        del strategy
        return _BlocksAfterInsertingPublisher()

    monkeypatch.setattr(run_module, "resolve_publisher", _fake_resolve_publisher)

    result_holder: list[Any] = []

    def _run_in_background() -> None:
        result_holder.append(publish_ingest(ctx))

    thread = threading.Thread(target=_run_in_background)
    thread.start()
    try:
        assert ready.wait(timeout=10), "publish never reached the blocking point"

        with psycopg.connect(env.migrated_dsn) as probe:
            customers_count = probe.execute(
                "SELECT COUNT(*) FROM normalized.customers WHERE _run_id = %s",
                (run_id,),
            ).fetchone()
            file_status = probe.execute(
                "SELECT status FROM meta.files WHERE file_id = %s",
                (file_id,),
            ).fetchone()
            batch_status = probe.execute(
                "SELECT status FROM meta.batches WHERE batch_id = %s",
                (batch_id,),
            ).fetchone()
        assert customers_count is not None
        assert customers_count[0] == 0
        # STAGED, not RUNNING -- publish_ingest never touches
        # meta.ingestion_runs.status until finalize_publication, which has
        # not run yet at this pause point.
        assert _read_run_status(env.migrated_dsn, run_id) == "STAGED"
        assert file_status is not None
        assert file_status[0] == "DISCOVERED"
        assert batch_status is not None
        assert batch_status[0] == "OPEN"
    finally:
        proceed.set()
        thread.join(timeout=10)

    assert len(result_holder) == 1
    assert result_holder[0]["status"] == "SUCCEEDED"
    assert run_id in result_holder[0]["runs_finalized"]
    assert _read_customers_count_for_run(env.migrated_dsn, run_id) == 2
    assert _read_run_status(env.migrated_dsn, run_id) == "SUCCEEDED"


# --- heartbeat_loop terminal-status safety (04-10 gap closure: CR-01) ------


class _HeartbeatCallSpy:
    """Wraps a real `MetadataRepository`, recording `heartbeat_ingestion_run` calls.

    Delegates every call -- including `heartbeat_ingestion_run` itself --
    unchanged to the wrapped repository, so the real, guarded SQL still runs
    against the real database on every tick. This proxy exists only to
    observe that `_heartbeat_loop` genuinely calls `heartbeat_ingestion_run`
    (not `update_ingestion_run_status`) with the test's sentinel values --
    CR-01's own "the production call-site swap actually took effect, not
    merely that a new, unused repository method exists" requirement.

    The write itself is correctly a no-op against an already-terminal run
    (proven independently by this test's own DB poll loop, which asserts
    `status` never leaves `SUCCEEDED`), so a sentinel value can never become
    visible in `meta.ingestion_runs.rows_read` for a run that is already
    SUCCEEDED when the loop starts -- observing the call itself is the only
    way to prove the loop genuinely ticked at least once without
    contradicting that guarantee.
    """

    def __init__(self, inner: PostgresMetadataRepository) -> None:
        self._inner = inner
        self.heartbeat_calls: list[tuple[int, int, int]] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def heartbeat_ingestion_run(
        self,
        *,
        run_id: int,
        lease_expires_at: datetime,
        rows_read: int,
        rows_parsed: int,
    ) -> None:
        self.heartbeat_calls.append((run_id, rows_read, rows_parsed))
        self._inner.heartbeat_ingestion_run(
            run_id=run_id,
            lease_expires_at=lease_expires_at,
            rows_read=rows_read,
            rows_parsed=rows_parsed,
        )


def test_heartbeat_loop_tick_against_a_terminal_run_never_regresses_status(
    env: _Env,
) -> None:
    """CR-01: a stray heartbeat tick against an already-terminal run must never regress it.

    Targets `_heartbeat_loop` directly on its own thread (not through
    `stage_ingest`) against a run already marked SUCCEEDED -- the exact
    post-publish-commit race window CR-01 closes. See `_HeartbeatCallSpy`'s
    own docstring for why "the loop genuinely ticked" is proven via a call
    spy rather than a `rows_read` value becoming visible in the database.
    """
    ctx, run_id, _file_id, _batch_id = _seed_and_build_ctx(
        env,
        key_suffix="heartbeat_terminal",
        csv_bytes=_csv_bytes(1, start_id=9_100_601),
    )
    env.metadata.claim_ingestion_run(
        idempotency_key=ctx.run.idempotency_key,
        try_number=1,
        pod_name="pod-heartbeat-terminal",
    )
    env.metadata.update_ingestion_run_status(run_id=run_id, status="SUCCEEDED")

    spy = _HeartbeatCallSpy(env.metadata)
    spy_ctx = replace(ctx, metadata=spy)

    # Deliberately targets the module's own private implementation details
    # (this plan's own Task 1 spec): a thread-level test of the heartbeat
    # loop itself, not merely its public entry point via stage_ingest.
    progress = run_module._Progress()  # noqa: SLF001
    progress.rows_read = 777
    progress.rows_parsed = 777
    stop_event = threading.Event()
    thread = threading.Thread(
        target=run_module._heartbeat_loop,  # noqa: SLF001
        args=(spy_ctx, run_id, progress, stop_event, 0.02),
        name=f"test-heartbeat-terminal-{run_id}",
        daemon=True,
    )
    thread.start()
    try:
        deadline = time.monotonic() + 5
        sentinel_observed = False
        while time.monotonic() < deadline:
            with psycopg.connect(env.migrated_dsn) as probe:
                row = probe.execute(
                    "SELECT status FROM meta.ingestion_runs WHERE run_id = %s",
                    (run_id,),
                ).fetchone()
            assert row is not None
            assert row[0] == "SUCCEEDED", (
                f"CR-01 regression: a stray heartbeat tick against a terminal run "
                f"changed status to {row[0]!r} -- must stay SUCCEEDED"
            )
            if any(call == (run_id, 777, 777) for call in spy.heartbeat_calls):
                sentinel_observed = True
                break
            time.sleep(0.01)
    finally:
        stop_event.set()
        thread.join(timeout=10)

    assert sentinel_observed, (
        "heartbeat loop never ticked (no heartbeat_ingestion_run call observed)"
    )
    assert _read_run_status(env.migrated_dsn, run_id) == "SUCCEEDED"


# --- Behavior 6: an incoming TRACEPARENT round-trips into meta.ingestion_runs (plan 07-05) --

_TRACE_ROUNDTRIP_TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
_TRACE_ROUNDTRIP_TRACE_ID_HEX = "4bf92f3577b34da6a3ce929d0e0e4736"
_TRACE_ROUNDTRIP_PARENT_SPAN_ID_HEX = "00f067aa0ba902b7"


def _read_run_trace_columns(migrated_dsn: str, run_id: int) -> tuple[str | None, str | None]:
    with psycopg.connect(migrated_dsn) as conn:
        row = conn.execute(
            "SELECT trace_id, span_id FROM meta.ingestion_runs WHERE run_id = %s",
            (run_id,),
        ).fetchone()
    assert row is not None
    return row[0], row[1]


def test_traceparent_round_trips_into_meta_ingestion_runs_via_a_real_claim(env: _Env) -> None:
    """OBS-10: an incoming TRACEPARENT survives claim -> stage, unmodified in trace_id.

    The claim itself (and the trace_id/span_id write into
    `meta.ingestion_runs`) happens entirely inside `stage_ingest` (module
    docstring: "Read BEFORE the claim below, so the SAME trace_id/span_id
    land on the claimed row") -- this test never needs `publish_ingest`.

    Extracts a known-valid, hand-constructed W3C ``traceparent`` into the
    active OTel context the same way ``dataplat.cli``'s own
    ``_extract_incoming_trace_context()`` does (07-05 Task 1): a direct
    ``propagate.extract()`` + ``context.attach()`` call, right here in the
    test -- less invasive than invoking ``dataplat.cli.main()``'s full
    dispatch just to reach the same two lines. ``tracing`` is wired directly
    to an in-memory-exporter-backed real ``TracerProvider`` (bypassing the
    public ``configure(otlp_endpoint=...)``, which always builds a real
    network-bound ``OTLPSpanExporter`` -- mirrors ``tests/unit/observability/
    test_tracing.py``'s own sanctioned direct-poke-at-``tracing._provider``
    pattern) so ``stage_ingest()``'s own ``pipeline.stage_ingest`` span
    (renamed from ``pipeline.run_ingest``, OBS-10) is genuinely recording
    and genuinely nests under the extracted parent.

    Proves, via a direct SQL read of ``meta.ingestion_runs`` (never trusting
    the in-process ``Receipt`` alone): ``trace_id`` equals the trace ID
    encoded in the injected ``traceparent`` (cross-process continuity), and
    ``span_id`` is a well-formed 16-hex-character value that does NOT equal
    the injected ``traceparent``'s own parent span ID (this run's own child
    span, never a copy of the parent's).
    """
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracing._provider = provider  # noqa: SLF001 -- see docstring above

    ctx, run_id, _file_id, _batch_id = _seed_and_build_ctx(
        env,
        key_suffix="traceparent_roundtrip",
        csv_bytes=_csv_bytes(2, start_id=9_100_701),
    )

    parent_ctx = propagate.extract({"traceparent": _TRACE_ROUNDTRIP_TRACEPARENT})
    token = otel_context.attach(parent_ctx)
    try:
        receipt = stage_ingest(ctx)
    finally:
        otel_context.detach(token)
        tracing.configure(otlp_endpoint=None)  # restore a genuine no-op for later tests

    assert receipt.status == "STAGED"

    db_trace_id, db_span_id = _read_run_trace_columns(env.migrated_dsn, run_id)
    assert db_trace_id == _TRACE_ROUNDTRIP_TRACE_ID_HEX  # SAME trace: cross-process continuity
    assert db_span_id is not None
    assert len(db_span_id) == 16
    assert db_span_id != _TRACE_ROUNDTRIP_PARENT_SPAN_ID_HEX  # NEW span: stage_ingest's own child

    # Bonus rigor: a "pipeline.stage_ingest" span was genuinely recorded --
    # catches a silent tracing-setup mistake that the two DB assertions
    # above alone could not distinguish from "tracing never actually ran."
    assert any(span.name == "pipeline.stage_ingest" for span in exporter.get_finished_spans())
