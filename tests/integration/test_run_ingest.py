"""Integration tests for ``dataplat.pipeline.run.run_ingest`` (plan 04-05 Task 1).

Every test drives a real ``run_ingest`` against real testcontainers
PostgreSQL + MinIO -- the exact claim/stage/publish/receipt orchestration a
real ``ingest`` pod executes, using a real ``csv_processor.source.CsvSource``
(never a fake/in-memory source): this file's whole point is proving the
FULL, source-to-database path, not merely a mocked slice of it.

``_seed_pending_run`` mirrors the real discovery-time flow
(``get_or_create_dataset`` -> a synthetic config version ->
``create_file``/``create_batch`` -> ``get_or_create_ingestion_run``) closely
enough that every FK ``run_ingest`` touches is real, matching
``test_publish_merge.py``'s own ``_seed_run`` precedent.

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
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import psycopg
import pytest

import dataplat.pipeline.run as run_module
from csv_processor.source import CsvSource
from dataplat.config.model import (
    BatchingConfig,
    DatasetConfig,
    DeduplicationConfig,
    LoadConfig,
    SourceConfig,
)
from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.models.identity import RunContext
from dataplat.observability.logging import get_logger
from dataplat.pipeline.protocol import PipelineContext
from dataplat.pipeline.run import run_ingest
from dataplat.storage.db import create_pool
from dataplat.storage.objectstore import S3ObjectStore

if TYPE_CHECKING:
    from collections.abc import Iterator

_BUCKET = "run-ingest-test"
_CSV_HEADER = "customer_id,name,country,birth_date,event_ts\n"


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
    """A `DatasetConfig` matching `run_ingest`'s own hardcoded customers assumptions."""
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
    )


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Insert a synthetic `meta.config_versions` row -- mirrors `test_metadata_repository.py`."""
    with psycopg.connect(dsn) as conn:
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
def env(
    _pool: Any,
    migrated_dsn: str,
    s3_client: Any,
    minio_config: dict[str, str],
    _scratch_bucket: str,
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


def _seed_pending_run(env: _Env, *, key_suffix: str, csv_bytes: bytes) -> tuple[int, int, int, str]:
    """Seed dataset/config/file/batch/PENDING-run and upload `csv_bytes`.

    Mirrors the real discover_files flow closely enough that every FK
    `run_ingest` touches (file_id, batch_id, config_version_id) is real.

    Returns:
        `(run_id, file_id, batch_id, object_key)`.
    """
    dataset_id = env.metadata.get_or_create_dataset(f"run_ingest_{key_suffix}")
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
) -> tuple[PipelineContext, int, int, int]:
    """`_seed_pending_run` + `_make_ctx` in one call -- every test's setup boils down to this."""
    run_id, file_id, batch_id, object_key = _seed_pending_run(
        env,
        key_suffix=key_suffix,
        csv_bytes=csv_bytes,
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


def _staging_table_exists(migrated_dsn: str, run_id: int) -> bool:
    with psycopg.connect(migrated_dsn) as conn:
        row = conn.execute(
            "SELECT to_regclass(%s)",
            (f"staging.customers__r{run_id}",),
        ).fetchone()
    assert row is not None
    return row[0] is not None


# --- Behavior 1: the full success path -------------------------------------


def test_successful_run_publishes_and_marks_everything_succeeded(env: _Env) -> None:
    ctx, run_id, file_id, batch_id = _seed_and_build_ctx(
        env,
        key_suffix="happy",
        csv_bytes=_csv_bytes(3, start_id=9_100_001),
    )

    receipt = run_ingest(ctx)

    assert receipt.status == "SUCCEEDED"
    assert receipt.run_id == run_id
    assert receipt.rows_read == 3
    assert receipt.rows_loaded == 3
    assert receipt.rows_invalid == 0

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
    assert not _staging_table_exists(env.migrated_dsn, run_id)  # dropped after publish (Pitfall 2)


# --- Behavior 2: already-SUCCEEDED -> SKIPPED_DUPLICATE --------------------


def test_already_succeeded_run_returns_skipped_duplicate_and_touches_no_staging_table(
    env: _Env,
) -> None:
    ctx, run_id, _file_id, _batch_id = _seed_and_build_ctx(
        env,
        key_suffix="dup",
        csv_bytes=_csv_bytes(1, start_id=9_100_101),
    )
    env.metadata.update_ingestion_run_status(run_id=run_id, status="SUCCEEDED")

    receipt = run_ingest(ctx)

    assert receipt.status == "SKIPPED_DUPLICATE"
    assert receipt.run_id == run_id
    assert receipt.rows_loaded == 0
    assert not _staging_table_exists(env.migrated_dsn, run_id)
    assert _read_customers_count_for_run(env.migrated_dsn, run_id) == 0


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

    receipt = run_ingest(ctx)

    assert receipt.status == "SKIPPED_CONCURRENT"
    assert receipt.run_id == run_id
    assert receipt.rows_loaded == 0
    assert not _staging_table_exists(env.migrated_dsn, run_id)


# --- Behavior 4: crash between staging and publish, then a clean retry -----


def test_crash_between_staging_and_publish_leaves_no_partial_state_and_retry_succeeds(
    env: _Env,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx, run_id, _file_id, _batch_id = _seed_and_build_ctx(
        env,
        key_suffix="crash",
        csv_bytes=_csv_bytes(2, start_id=9_100_301),
    )

    def _simulate_crash_before_publish_begins(strategy: str) -> Any:
        del strategy
        msg = "simulated crash before the publish transaction begins"
        raise RuntimeError(msg)

    monkeypatch.setattr(run_module, "resolve_publisher", _simulate_crash_before_publish_begins)

    with pytest.raises(RuntimeError, match="simulated crash"):
        run_ingest(ctx)

    # Staging succeeded (it ran before the injected fault); publish never
    # began -- normalized.customers must be untouched and the run must NOT
    # be SUCCEEDED.
    assert _read_customers_count_for_run(env.migrated_dsn, run_id) == 0
    assert _read_run_status(env.migrated_dsn, run_id) != "SUCCEEDED"

    # Simulate real time having passed (this test cannot wait out the real
    # 5-minute lease): force it into an expired state directly, exactly as
    # a genuinely crashed pod's lease would eventually look to a retrier.
    expired = datetime.now(tz=UTC) - timedelta(minutes=1)
    env.metadata.update_ingestion_run_status(
        run_id=run_id,
        status="RUNNING",
        lease_expires_at=expired,
    )

    monkeypatch.undo()  # restore the real resolve_publisher for the retry

    receipt = run_ingest(ctx)

    assert receipt.status == "SUCCEEDED"
    assert receipt.rows_loaded == 2
    # Proves staging.py's own DROP-IF-EXISTS-first behavior composed
    # correctly with this retry: exactly 2 rows land, never 4.
    assert _read_customers_count_for_run(env.migrated_dsn, run_id) == 2
    assert not _staging_table_exists(env.migrated_dsn, run_id)


# --- Behavior 5: the heartbeat keeps rows_read/rows_parsed genuinely live --


def test_heartbeat_writes_a_live_nonzero_rows_read_while_running_before_return(
    env: _Env,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
        and persist them while run_ingest is provably still RUNNING,
        before run_ingest can proceed to publish and return.
        """

        def __init__(self, *, target_columns: tuple[str, ...]) -> None:
            self._inner = real_staging_loader_cls(target_columns=target_columns)

        def load(self, ctx: Any, conn: Any, *, on_progress: Any = None) -> Any:
            result = self._inner.load(ctx, conn, on_progress=on_progress)
            resume_staging.wait(timeout=10)
            return result

    monkeypatch.setattr(run_module, "StagingLoader", _PausesAfterStagingLoader)

    result_holder: list[Any] = []
    error_holder: list[Exception] = []

    def _run_in_background() -> None:
        try:
            result_holder.append(run_ingest(ctx, heartbeat_interval_seconds=0.05))
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

    assert not error_holder, f"run_ingest raised on the background thread: {error_holder}"
    assert observed_live_progress, "heartbeat never wrote a live rows_read while status=RUNNING"
    assert len(result_holder) == 1
    assert result_holder[0].status == "SUCCEEDED"
    assert result_holder[0].rows_loaded == 1


# --- The publish transaction's four effects are invisible until commit -----


def test_publish_transaction_effects_are_invisible_to_another_connection_until_commit(
    env: _Env,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx, run_id, file_id, batch_id = _seed_and_build_ctx(
        env,
        key_suffix="atomic",
        csv_bytes=_csv_bytes(2, start_id=9_100_501),
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
        result_holder.append(run_ingest(ctx))

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
        assert _read_run_status(env.migrated_dsn, run_id) == "RUNNING"
        assert file_status is not None
        assert file_status[0] == "DISCOVERED"
        assert batch_status is not None
        assert batch_status[0] == "OPEN"
    finally:
        proceed.set()
        thread.join(timeout=10)

    assert len(result_holder) == 1
    assert result_holder[0].status == "SUCCEEDED"
    assert _read_customers_count_for_run(env.migrated_dsn, run_id) == 2
    assert _read_run_status(env.migrated_dsn, run_id) == "SUCCEEDED"
