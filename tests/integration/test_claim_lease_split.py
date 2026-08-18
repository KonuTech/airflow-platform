"""Integration tests for `meta.run_stages`'s claim/heartbeat/complete cycle (D-17, 08.1-07 Task 1).

`publish_ingest` (plan 08.1-10) needs a claim mechanism against
`meta.run_stages` that is fully independent of `stage_ingest`'s own claim
against `meta.ingestion_runs`, yet is gated so a stage hop that has not
genuinely completed can never be published (Pitfall 2). This file proves
`claim_run_stage`'s cross-table guard, `heartbeat_run_stage`'s self-guarded
no-op contract, `complete_run_stage`'s terminal transition, and
`list_staged_run_ids`'s observability query -- all against a real
testcontainers PostgreSQL, migrated to head (migration 0025's
`meta.run_stages`).

Mirrors `tests/integration/test_publish_merge.py`'s own raw-SQL seeding
convention: every test builds its own dataset/config_version/file/batch/run
via `_seed_run`, never touching another test's rows.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import psycopg
import pytest

from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Insert a synthetic `meta.config_versions` row directly via SQL.

    Mirrors `tests/integration/test_publish_merge.py`'s helper of the same
    name/shape -- duplicated locally rather than imported, matching this
    test suite's existing per-file helper convention.
    """
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


def _seed_run(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
    *,
    key_suffix: str,
    status: str = "RUNNING",
) -> tuple[int, int, int, int]:
    """Create dataset+config_version+file+batch+run at `status`.

    Returns `(run_id, dataset_id, file_id, batch_id)` -- `list_staged_run_ids`
    tests need `dataset_id`/`file_id`/`batch_id` alongside `run_id` to build
    the expected quadruple.
    """
    dataset_id = repository.get_or_create_dataset(f"claim_lease_split_{key_suffix}")
    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id)
    file_id = repository.create_file(
        dataset_id=dataset_id,
        object_uri=f"s3://raw/claim_lease_split/{key_suffix}.csv",
        content_sha256=hashlib.sha256(key_suffix.encode()).digest(),
        hash_version=1,
        size_bytes=10,
        filename=f"{key_suffix}.csv",
        status="DISCOVERED",
    )
    batch_id = repository.create_batch(
        dataset_id=dataset_id,
        batch_key=f"{key_suffix}:1",
        status="OPEN",
    )
    run_id = repository.create_ingestion_run(
        idempotency_key=f"{key_suffix}:1",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        status=status,
        file_id=file_id,
        batch_id=batch_id,
    )
    return run_id, dataset_id, file_id, batch_id


@pytest.fixture
def repository(migrated_dsn: str) -> Iterator[PostgresMetadataRepository]:
    """A `PostgresMetadataRepository` backed by an opened pool over the migrated database."""
    pool = create_pool(migrated_dsn)
    pool.open(wait=True)
    try:
        yield PostgresMetadataRepository(pool)
    finally:
        pool.close()


def test_claim_run_stage_refused_when_owning_run_is_not_staged(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    """T-08.1-15: `publish` can never claim a run whose stage hop has not genuinely completed.

    Refused even though no `run_stages` row exists yet for `(run_id,
    "PUBLISH")` -- the `INSERT`'s own `WHERE EXISTS` guard on
    `meta.ingestion_runs.status` applies on a first-ever claim too.
    """
    run_id, _dataset_id, _file_id, _batch_id = _seed_run(
        repository,
        migrated_dsn,
        key_suffix="not_staged",
        status="RUNNING",
    )

    claimed = repository.claim_run_stage(
        run_id=run_id,
        stage_name="PUBLISH",
        try_number=1,
        pod_name="pod-a",
    )

    assert claimed is None
    with psycopg.connect(migrated_dsn) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM meta.run_stages WHERE run_id = %s",
            (run_id,),
        ).fetchone()
    assert count is not None
    assert count[0] == 0


def test_claim_run_stage_succeeds_once_staged_and_refuses_a_concurrent_claim(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    """Once the owning run reaches `STAGED`, the claim succeeds; a racing claim is refused."""
    run_id, _dataset_id, _file_id, _batch_id = _seed_run(
        repository,
        migrated_dsn,
        key_suffix="staged",
        status="RUNNING",
    )
    repository.update_ingestion_run_status(run_id=run_id, status="STAGED")

    claimed = repository.claim_run_stage(
        run_id=run_id,
        stage_name="PUBLISH",
        try_number=1,
        pod_name="pod-a",
    )
    assert claimed is not None

    # A racing pod's claim, while the first claim's lease is still live, must
    # be refused.
    concurrent = repository.claim_run_stage(
        run_id=run_id,
        stage_name="PUBLISH",
        try_number=1,
        pod_name="pod-b",
    )
    assert concurrent is None

    with psycopg.connect(migrated_dsn) as conn:
        row = conn.execute(
            "SELECT status, pod_name FROM meta.run_stages WHERE run_id = %s AND stage_name = %s",
            (run_id, "PUBLISH"),
        ).fetchone()
    assert row is not None
    assert row[0] == "RUNNING"
    assert row[1] == "pod-a"


def test_heartbeat_run_stage_is_a_silent_noop_once_succeeded(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    """A stray heartbeat tick after `complete_run_stage` must never regress the row to RUNNING."""
    run_id, _dataset_id, _file_id, _batch_id = _seed_run(
        repository,
        migrated_dsn,
        key_suffix="heartbeat_noop",
        status="RUNNING",
    )
    repository.update_ingestion_run_status(run_id=run_id, status="STAGED")
    claimed = repository.claim_run_stage(
        run_id=run_id,
        stage_name="STAGE_LOAD",
        try_number=1,
        pod_name="pod-a",
    )
    assert claimed is not None
    repository.complete_run_stage(
        run_id=run_id,
        stage_name="STAGE_LOAD",
        status="SUCCEEDED",
        finished_at=datetime.now(tz=UTC),
    )

    with psycopg.connect(migrated_dsn) as conn:
        pre_row = conn.execute(
            "SELECT lease_expires_at FROM meta.run_stages WHERE run_id = %s AND stage_name = %s",
            (run_id, "STAGE_LOAD"),
        ).fetchone()
    assert pre_row is not None
    pre_lease = pre_row[0]

    stray_lease = datetime.now(tz=UTC) + timedelta(minutes=5)
    repository.heartbeat_run_stage(
        run_id=run_id,
        stage_name="STAGE_LOAD",
        lease_expires_at=stray_lease,
    )

    assert repository.get_run_stage_status(run_id=run_id, stage_name="STAGE_LOAD") == "SUCCEEDED"
    with psycopg.connect(migrated_dsn) as conn:
        post_row = conn.execute(
            "SELECT lease_expires_at FROM meta.run_stages WHERE run_id = %s AND stage_name = %s",
            (run_id, "STAGE_LOAD"),
        ).fetchone()
    assert post_row is not None
    # The stray heartbeat must have affected zero rows -- lease_expires_at
    # stays exactly what it was at claim time, never the stray tick's value.
    assert post_row[0] == pre_lease
    assert post_row[0] != stray_lease


def test_complete_run_stage_and_list_staged_run_ids(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    """`complete_run_stage` transitions the row; `list_staged_run_ids` tracks the owning run status.

    `list_staged_run_ids` returns the `(run_id, file_id, batch_id,
    report_uri)` quadruple while `meta.ingestion_runs.status='STAGED'`, and
    no longer returns it once the run later reaches `'SUCCEEDED'` -- the
    already-set `report_uri` (written by `stage_ingest`'s own
    `update_ingestion_run_status` call) passes straight back through
    unchanged.
    """
    run_id, dataset_id, file_id, batch_id = _seed_run(
        repository,
        migrated_dsn,
        key_suffix="complete_list",
        status="RUNNING",
    )
    report_uri = "s3://processed/claim_lease_split/complete_list-report.json"
    repository.update_ingestion_run_status(run_id=run_id, status="STAGED", report_uri=report_uri)

    claimed = repository.claim_run_stage(
        run_id=run_id,
        stage_name="STAGE_LOAD",
        try_number=1,
        pod_name="pod-a",
    )
    assert claimed is not None

    repository.complete_run_stage(
        run_id=run_id,
        stage_name="STAGE_LOAD",
        status="SUCCEEDED",
        finished_at=datetime.now(tz=UTC),
    )
    assert repository.get_run_stage_status(run_id=run_id, stage_name="STAGE_LOAD") == "SUCCEEDED"

    staged = repository.list_staged_run_ids(dataset_id=dataset_id)
    assert (run_id, file_id, batch_id, report_uri) in staged

    # Once the owning run itself reaches SUCCEEDED (publish_ingest's own
    # eventual transition, out of this plan's scope), it must no longer be
    # offered as staged.
    repository.update_ingestion_run_status(run_id=run_id, status="SUCCEEDED")
    staged_after = repository.list_staged_run_ids(dataset_id=dataset_id)
    assert run_id not in {row[0] for row in staged_after}
