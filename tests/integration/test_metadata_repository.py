"""META-01 usability proof: a full dataset->file->batch->ingestion_run chain, typed, no hand SQL.

`test_full_slice_round_trip` proves every FK in the slice resolves through
`PostgresMetadataRepository` alone, against the schema plan 03-02's
migrations created -- the mechanical proof that META-01's schema is not
just DDL-valid but genuinely usable.
`test_resolved_env_secret_yields_a_live_metadata_connection` is ROADMAP
Phase 3 success criterion 4 / SEC-15's proof: a connection pool built from
`resolve_secret("env://...")`'s output, not a raw DSN literal, executes a
real query against the migrated database (03-05's Task 3, the first place
in this phase the resolver and the pool factory are proven wired together
end to end).
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import psycopg
import pytest

from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.secrets.resolver import resolve_secret
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    from collections.abc import Iterator


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Insert a synthetic `meta.config_versions` row directly via SQL.

    This test file does not depend on plan 03-04's `ConfigRegistry` — every
    `meta.ingestion_runs` row still requires a real, FK-valid
    `config_version_id`, so each test that creates a run seeds one row here
    for its own freshly-created dataset.
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


def _read_ingestion_run_status(dsn: str, run_id: int) -> str:
    """Read back `meta.ingestion_runs.status` directly via SQL (bypassing the repository)."""
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            "SELECT status FROM meta.ingestion_runs WHERE run_id = %s",
            (run_id,),
        ).fetchone()
        assert row is not None
        return str(row[0])


def _count_ingestion_runs_for_key(dsn: str, idempotency_key: str) -> int:
    """Count `meta.ingestion_runs` rows for one `idempotency_key`, bypassing the repository."""
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM meta.ingestion_runs WHERE idempotency_key = %s",
            (idempotency_key,),
        ).fetchone()
        assert row is not None
        return int(row[0])


def _read_run_row(dsn: str, run_id: int) -> tuple[str, str | None, datetime | None]:
    """Read `(status, k8s_pod_name, lease_expires_at)` for one run, bypassing the repository."""
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            """
            SELECT status, k8s_pod_name, lease_expires_at
              FROM meta.ingestion_runs WHERE run_id = %s
            """,
            (run_id,),
        ).fetchone()
        assert row is not None
        return str(row[0]), row[1], row[2]


def _read_run_progress(
    dsn: str,
    run_id: int,
) -> tuple[str, str | None, datetime | None, int | None, int | None]:
    """Read `(status, k8s_pod_name, lease_expires_at, rows_read, rows_parsed)`, bypassing the repo.

    A NEW, separate helper (04-10 gap closure: CR-01) -- `_read_run_row`'s own
    3-tuple return shape is unpacked positionally at two existing call sites
    (`test_claim_ingestion_run_claims_a_pending_run`,
    `test_claim_ingestion_run_claims_a_failed_run`) and must not be widened.
    """
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            """
            SELECT status, k8s_pod_name, lease_expires_at, rows_read, rows_parsed
              FROM meta.ingestion_runs WHERE run_id = %s
            """,
            (run_id,),
        ).fetchone()
        assert row is not None
        return str(row[0]), row[1], row[2], row[3], row[4]


def _read_file_status(dsn: str, file_id: int) -> str:
    """Read back `meta.files.status` directly via SQL (bypassing the repository)."""
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            "SELECT status FROM meta.files WHERE file_id = %s",
            (file_id,),
        ).fetchone()
        assert row is not None
        return str(row[0])


def _read_batch_status(dsn: str, batch_id: int) -> str:
    """Read back `meta.batches.status` directly via SQL (bypassing the repository)."""
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            "SELECT status FROM meta.batches WHERE batch_id = %s",
            (batch_id,),
        ).fetchone()
        assert row is not None
        return str(row[0])


def _count_files(dsn: str, *, dataset_id: int, object_uri: str) -> int:
    """Count `meta.files` rows for one `(dataset_id, object_uri)`, bypassing the repository."""
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM meta.files WHERE dataset_id = %s AND object_uri = %s",
            (dataset_id, object_uri),
        ).fetchone()
        assert row is not None
        return int(row[0])


def _read_duplicate_of_file_id(dsn: str, file_id: int) -> int | None:
    """Read back `meta.files.duplicate_of_file_id` directly via SQL (bypassing the repository)."""
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            "SELECT duplicate_of_file_id FROM meta.files WHERE file_id = %s",
            (file_id,),
        ).fetchone()
        assert row is not None
        return None if row[0] is None else int(row[0])


@pytest.fixture
def repository(migrated_dsn: str) -> Iterator[PostgresMetadataRepository]:
    """A `PostgresMetadataRepository` backed by an opened pool over the migrated database."""
    pool = create_pool(migrated_dsn)
    pool.open(wait=True)
    try:
        yield PostgresMetadataRepository(pool)
    finally:
        pool.close()


def test_full_slice_round_trip(repository: PostgresMetadataRepository, migrated_dsn: str) -> None:
    dataset_id_first = repository.get_or_create_dataset("customers_slice_proof")
    dataset_id_second = repository.get_or_create_dataset("customers_slice_proof")
    assert dataset_id_first == dataset_id_second

    content_sha256 = hashlib.sha256(b"synthetic customers file content").digest()
    file_id = repository.create_file(
        dataset_id=dataset_id_first,
        object_uri="s3://raw/customers/2026/08/13/customers_20260813.csv",
        content_sha256=content_sha256,
        hash_version=1,
        size_bytes=1234,
        filename="customers_20260813.csv",
        status="DISCOVERED",
    )
    found_file_id = repository.find_file_by_content_hash(
        dataset_id=dataset_id_first,
        content_sha256=content_sha256,
    )
    assert found_file_id == file_id

    never_inserted_hash = hashlib.sha256(b"content that was never uploaded").digest()
    assert (
        repository.find_file_by_content_hash(
            dataset_id=dataset_id_first,
            content_sha256=never_inserted_hash,
        )
        is None
    )

    batch_id = repository.create_batch(
        dataset_id=dataset_id_first,
        batch_key="customers:2026-08-13:1",
        status="OPEN",
    )
    repository.link_batch_file(batch_id=batch_id, file_id=file_id, sequence_no=1)

    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id_first)

    run_id = repository.create_ingestion_run(
        idempotency_key="customers:2026-08-13:1:attempt-1",
        dataset_id=dataset_id_first,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        status="RUNNING",
        file_id=file_id,
        batch_id=batch_id,
    )
    assert _read_ingestion_run_status(migrated_dsn, run_id) == "RUNNING"

    repository.update_ingestion_run_status(
        run_id=run_id,
        status="SUCCEEDED",
        finished_at=datetime.now(tz=UTC),
    )
    assert _read_ingestion_run_status(migrated_dsn, run_id) == "SUCCEEDED"


def test_update_ingestion_run_status_rejects_unknown_field(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    dataset_id = repository.get_or_create_dataset("rejects_unknown_field")
    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id)
    run_id = repository.create_ingestion_run(
        idempotency_key="rejects_unknown_field:attempt-1",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        status="RUNNING",
    )

    with pytest.raises(ValueError, match="unknown"):
        repository.update_ingestion_run_status(
            run_id=run_id,
            status="SUCCEEDED",
            not_a_real_column="this must be rejected",
        )

    # The rejected call must not have executed any SQL against the row.
    assert _read_ingestion_run_status(migrated_dsn, run_id) == "RUNNING"


def test_create_file_is_idempotent_on_identical_business_identity(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    """04-03-PLAN.md Task 2's own dependency: re-discovery must not duplicate a row."""
    dataset_id = repository.get_or_create_dataset("create_file_idempotency_proof")
    content_sha256 = hashlib.sha256(b"idempotent content").digest()
    # A real, already-existing file row for duplicate_of_file_id to reference
    # -- meta.files.duplicate_of_file_id is a real self-FK (migration 0002),
    # so a fabricated id would (correctly) be rejected.
    original_file_id = repository.create_file(
        dataset_id=dataset_id,
        object_uri="s3://raw/customers/original.csv",
        content_sha256=hashlib.sha256(b"the original content").digest(),
        hash_version=1,
        size_bytes=7,
        filename="original.csv",
        status="DISCOVERED",
    )

    first_id = repository.create_file(
        dataset_id=dataset_id,
        object_uri="s3://raw/customers/idempotent.csv",
        content_sha256=content_sha256,
        hash_version=1,
        size_bytes=42,
        filename="idempotent.csv",
        status="DISCOVERED",
    )
    second_id = repository.create_file(
        dataset_id=dataset_id,
        object_uri="s3://raw/customers/idempotent.csv",
        content_sha256=content_sha256,
        hash_version=1,
        size_bytes=42,
        filename="idempotent.csv",
        status="DISCOVERED",
        duplicate_of_file_id=original_file_id,
    )

    assert first_id == second_id
    with psycopg.connect(migrated_dsn) as conn:
        rows = conn.execute(
            "SELECT COUNT(*) FROM meta.files WHERE dataset_id = %s AND object_uri = %s",
            (dataset_id, "s3://raw/customers/idempotent.csv"),
        ).fetchone()
        assert rows is not None
        assert rows[0] == 1
        duplicate_col = conn.execute(
            "SELECT duplicate_of_file_id FROM meta.files WHERE file_id = %s",
            (first_id,),
        ).fetchone()
        assert duplicate_col is not None
        assert duplicate_col[0] == original_file_id


def test_get_or_create_ingestion_run_is_idempotent_and_reports_current_status(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    dataset_id = repository.get_or_create_dataset("get_or_create_run_proof")
    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id)

    first_run_id, first_status = repository.get_or_create_ingestion_run(
        idempotency_key="get_or_create_run_proof:attempt-1",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
    )
    assert first_status == "PENDING"

    second_run_id, second_status = repository.get_or_create_ingestion_run(
        idempotency_key="get_or_create_run_proof:attempt-1",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
    )
    assert second_run_id == first_run_id
    assert second_status == "PENDING"

    with psycopg.connect(migrated_dsn) as conn:
        rows = conn.execute(
            "SELECT COUNT(*) FROM meta.ingestion_runs WHERE idempotency_key = %s",
            ("get_or_create_run_proof:attempt-1",),
        ).fetchone()
        assert rows is not None
        assert rows[0] == 1

    repository.update_ingestion_run_status(run_id=first_run_id, status="SUCCEEDED")
    third_run_id, third_status = repository.get_or_create_ingestion_run(
        idempotency_key="get_or_create_run_proof:attempt-1",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
    )
    assert third_run_id == first_run_id
    assert third_status == "SUCCEEDED"


def test_resolved_env_secret_yields_a_live_metadata_connection(
    migrated_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ROADMAP success criterion 4 / SEC-15: resolve_secret() -> create_pool(), proven live."""
    monkeypatch.setenv("DATAPLAT_TEST_DB_DSN", migrated_dsn)

    resolved = resolve_secret("env://DATAPLAT_TEST_DB_DSN")
    assert resolved == migrated_dsn

    pool = create_pool(resolved)
    pool.open(wait=True)
    try:
        repository = PostgresMetadataRepository(pool)
        dataset_id = repository.get_or_create_dataset("resolver_wiring_proof")
        assert isinstance(dataset_id, int)
    finally:
        pool.close()


# --- get_or_create_ingestion_run (04-01 Task 2) ---------------------------
#
# Discovery-time pre-allocation: a no-op upsert on `idempotency_key`,
# tolerating repeat calls -- distinct from `claim_ingestion_run` below
# (Pitfall 5). Never conflate the two: they are different SQL statements
# doing different jobs.


def test_get_or_create_ingestion_run_is_idempotent(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    dataset_id = repository.get_or_create_dataset("get_or_create_run_idempotent")
    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id)
    key = "get_or_create_run_idempotent:1"

    run_id_first, status_first = repository.get_or_create_ingestion_run(
        idempotency_key=key,
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
    )
    assert status_first == "PENDING"

    run_id_second, status_second = repository.get_or_create_ingestion_run(
        idempotency_key=key,
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
    )

    assert run_id_second == run_id_first
    assert status_second == "PENDING"
    # The second call must not have executed an INSERT.
    assert _count_ingestion_runs_for_key(migrated_dsn, key) == 1


def test_get_or_create_ingestion_run_returns_the_rows_current_status(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    """A repeat call reflects whatever status the row already has -- not always PENDING."""
    dataset_id = repository.get_or_create_dataset("get_or_create_run_reflects_status")
    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id)
    key = "get_or_create_run_reflects_status:1"

    run_id, _ = repository.get_or_create_ingestion_run(
        idempotency_key=key,
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
    )
    repository.update_ingestion_run_status(run_id=run_id, status="SUCCEEDED")

    run_id_again, status = repository.get_or_create_ingestion_run(
        idempotency_key=key,
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
    )

    assert run_id_again == run_id
    assert status == "SUCCEEDED"


# --- claim_ingestion_run (04-01 Task 2) -----------------------------------
#
# Pod-startup-time exclusive claim: a conditional `UPDATE ... WHERE`, never
# an INSERT -- distinct from `get_or_create_ingestion_run` above.


def _seed_pending_run(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
    *,
    key_suffix: str,
) -> int:
    """Create a fresh dataset + config version + PENDING run; return its `run_id`."""
    dataset_id = repository.get_or_create_dataset(f"claim_run_{key_suffix}")
    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id)
    run_id, _ = repository.get_or_create_ingestion_run(
        idempotency_key=f"claim_run_{key_suffix}:1",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
    )
    return run_id


def test_claim_ingestion_run_claims_a_pending_run(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    run_id = _seed_pending_run(repository, migrated_dsn, key_suffix="pending")

    claimed = repository.claim_ingestion_run(
        idempotency_key="claim_run_pending:1",
        try_number=1,
        pod_name="pod-a",
    )

    assert claimed == (run_id, "RUNNING")
    status, pod_name, _ = _read_run_row(migrated_dsn, run_id)
    assert status == "RUNNING"
    assert pod_name == "pod-a"


def test_claim_ingestion_run_persists_dag_run_task_map_index_and_namespace(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    """OBS-07 gap closure (07-09): the same UPDATE that sets trace_id/span_id also
    persists dag_id/dag_run_id/task_id/map_index/k8s_namespace, so a genuine
    Airflow-triggered run's identity round-trips into meta.ingestion_runs.
    """
    run_id = _seed_pending_run(repository, migrated_dsn, key_suffix="dag_ctx")

    claimed = repository.claim_ingestion_run(
        idempotency_key="claim_run_dag_ctx:1",
        try_number=1,
        pod_name="pod-dag-ctx",
        dag_id="csv_ingest_customers",
        dag_run_id="manual__2026-01-01T00:00:00+00:00",
        task_id="ingest",
        map_index=4,
        k8s_namespace="etl",
    )

    assert claimed == (run_id, "RUNNING")
    with psycopg.connect(migrated_dsn) as conn:
        row = conn.execute(
            """
            SELECT dag_id, dag_run_id, task_id, map_index, k8s_namespace
              FROM meta.ingestion_runs WHERE run_id = %s
            """,
            (run_id,),
        ).fetchone()
        assert row is not None
        assert row[0] == "csv_ingest_customers"
        assert row[1] == "manual__2026-01-01T00:00:00+00:00"
        assert row[2] == "ingest"
        assert row[3] == 4
        assert row[4] == "etl"


def test_claim_ingestion_run_claims_a_failed_run(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    run_id = _seed_pending_run(repository, migrated_dsn, key_suffix="failed")
    repository.update_ingestion_run_status(run_id=run_id, status="FAILED")

    claimed = repository.claim_ingestion_run(
        idempotency_key="claim_run_failed:1",
        try_number=2,
        pod_name="pod-b",
    )

    assert claimed == (run_id, "RUNNING")


def test_claim_ingestion_run_claims_a_running_run_with_an_expired_lease(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    run_id = _seed_pending_run(repository, migrated_dsn, key_suffix="expired_lease")
    expired = datetime.now(tz=UTC) - timedelta(minutes=10)
    repository.update_ingestion_run_status(
        run_id=run_id,
        status="RUNNING",
        lease_expires_at=expired,
        k8s_pod_name="pod-dead",
    )

    claimed = repository.claim_ingestion_run(
        idempotency_key="claim_run_expired_lease:1",
        try_number=2,
        pod_name="pod-c",
    )

    assert claimed == (run_id, "RUNNING")
    status, pod_name, _ = _read_run_row(migrated_dsn, run_id)
    assert status == "RUNNING"
    assert pod_name == "pod-c"


def test_claim_ingestion_run_refuses_a_succeeded_run(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    run_id = _seed_pending_run(repository, migrated_dsn, key_suffix="succeeded")
    repository.update_ingestion_run_status(run_id=run_id, status="SUCCEEDED")
    before = _read_run_row(migrated_dsn, run_id)

    claimed = repository.claim_ingestion_run(
        idempotency_key="claim_run_succeeded:1",
        try_number=1,
        pod_name="pod-d",
    )

    assert claimed is None
    assert _read_run_row(migrated_dsn, run_id) == before


def test_claim_ingestion_run_refuses_a_running_run_with_a_live_lease(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    run_id = _seed_pending_run(repository, migrated_dsn, key_suffix="live_lease")
    live = datetime.now(tz=UTC) + timedelta(minutes=5)
    repository.update_ingestion_run_status(
        run_id=run_id,
        status="RUNNING",
        lease_expires_at=live,
        k8s_pod_name="pod-live-owner",
    )
    before = _read_run_row(migrated_dsn, run_id)

    claimed = repository.claim_ingestion_run(
        idempotency_key="claim_run_live_lease:1",
        try_number=2,
        pod_name="pod-e",
    )

    assert claimed is None
    assert _read_run_row(migrated_dsn, run_id) == before


def test_claim_ingestion_run_returns_none_for_an_unknown_idempotency_key(
    repository: PostgresMetadataRepository,
) -> None:
    claimed = repository.claim_ingestion_run(
        idempotency_key="never-created-by-any-test",
        try_number=1,
        pod_name="pod-f",
    )

    assert claimed is None


# --- heartbeat_ingestion_run (04-10 gap closure: CR-01) ---------------------
#
# Terminal-status-safe heartbeat write: distinct from `update_ingestion_run_
# status` (no status guard, reserved for genuine status transitions) -- a
# stray heartbeat tick landing after a run's publish transaction has already
# committed SUCCEEDED must be a silent no-op, never a status regression back
# to RUNNING.


def test_heartbeat_ingestion_run_updates_a_running_row(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    run_id = _seed_pending_run(repository, migrated_dsn, key_suffix="heartbeat_running")
    repository.claim_ingestion_run(
        idempotency_key="claim_run_heartbeat_running:1",
        try_number=1,
        pod_name="pod-heartbeat-running",
    )
    new_lease = datetime.now(tz=UTC) + timedelta(minutes=7)

    repository.heartbeat_ingestion_run(
        run_id=run_id,
        lease_expires_at=new_lease,
        rows_read=42,
        rows_parsed=41,
    )

    status, _pod_name, lease_expires_at, rows_read, rows_parsed = _read_run_progress(
        migrated_dsn,
        run_id,
    )
    assert status == "RUNNING"
    assert lease_expires_at is not None
    assert abs((lease_expires_at - new_lease).total_seconds()) < 1
    assert rows_read == 42
    assert rows_parsed == 41


def test_heartbeat_ingestion_run_is_a_noop_once_the_run_is_no_longer_running(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    run_id = _seed_pending_run(repository, migrated_dsn, key_suffix="heartbeat_terminal")
    repository.claim_ingestion_run(
        idempotency_key="claim_run_heartbeat_terminal:1",
        try_number=1,
        pod_name="pod-heartbeat-terminal",
    )
    repository.update_ingestion_run_status(run_id=run_id, status="SUCCEEDED")
    before = _read_run_progress(migrated_dsn, run_id)

    repository.heartbeat_ingestion_run(
        run_id=run_id,
        lease_expires_at=datetime.now(tz=UTC) + timedelta(days=1),
        rows_read=999_999,
        rows_parsed=999_999,
    )

    assert _read_run_progress(migrated_dsn, run_id) == before


# --- finalize_publication (04-01 Task 2) ----------------------------------
#
# The one MetadataRepository method that never opens its own connection: it
# must land inside the SAME transaction as Publisher.publish's own
# INSERT ... ON CONFLICT (META-03).


def test_finalize_publication_updates_are_invisible_until_the_callers_commit(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    dataset_id = repository.get_or_create_dataset("finalize_publication_atomicity")
    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id)
    content_sha256 = hashlib.sha256(b"finalize publication content").digest()
    file_id = repository.create_file(
        dataset_id=dataset_id,
        object_uri="s3://raw/customers/finalize_publication.csv",
        content_sha256=content_sha256,
        hash_version=1,
        size_bytes=42,
        filename="finalize_publication.csv",
        status="DISCOVERED",
    )
    batch_id = repository.create_batch(
        dataset_id=dataset_id,
        batch_key="finalize_publication:2026-08-13:1",
        status="OPEN",
    )
    run_id, _ = repository.get_or_create_ingestion_run(
        idempotency_key="finalize_publication:1",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        file_id=file_id,
        batch_id=batch_id,
    )
    repository.claim_ingestion_run(
        idempotency_key="finalize_publication:1",
        try_number=1,
        pod_name="pod-finalize",
    )
    finished_at = datetime.now(tz=UTC)

    with psycopg.connect(migrated_dsn) as publish_conn:
        repository.finalize_publication(
            conn=publish_conn,
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            rows_loaded=7,
            finished_at=finished_at,
            duration_ms=1234,
            report_uri="s3://processed/customers/finalize_publication-report.json",
        )

        # Invisible to a separate connection until publish_conn commits --
        # finalize_publication must never commit or roll back its own conn.
        assert _read_file_status(migrated_dsn, file_id) == "DISCOVERED"
        assert _read_batch_status(migrated_dsn, batch_id) == "OPEN"
        assert _read_ingestion_run_status(migrated_dsn, run_id) == "RUNNING"

        publish_conn.commit()

    assert _read_file_status(migrated_dsn, file_id) == "PROCESSED"
    assert _read_batch_status(migrated_dsn, batch_id) == "PUBLISHED"
    assert _read_ingestion_run_status(migrated_dsn, run_id) == "SUCCEEDED"


# --- create_file, duplicate-aware (04-01 Task 2) --------------------------


def test_create_file_is_idempotent_and_stores_duplicate_of_file_id(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    dataset_id = repository.get_or_create_dataset("create_file_idempotent")
    original_file_id = repository.create_file(
        dataset_id=dataset_id,
        object_uri="s3://raw/customers/original.csv",
        content_sha256=hashlib.sha256(b"original content").digest(),
        hash_version=1,
        size_bytes=10,
        filename="original.csv",
        status="DISCOVERED",
    )
    content_sha256 = hashlib.sha256(b"create_file idempotency content").digest()
    object_uri = "s3://raw/customers/duplicate.csv"

    first_file_id = repository.create_file(
        dataset_id=dataset_id,
        object_uri=object_uri,
        content_sha256=content_sha256,
        hash_version=1,
        size_bytes=99,
        filename="duplicate.csv",
        status="DISCOVERED",
        duplicate_of_file_id=original_file_id,
    )
    second_file_id = repository.create_file(
        dataset_id=dataset_id,
        object_uri=object_uri,
        content_sha256=content_sha256,
        hash_version=1,
        size_bytes=99,
        filename="duplicate.csv",
        status="DISCOVERED",
        duplicate_of_file_id=original_file_id,
    )

    assert first_file_id == second_file_id
    assert _count_files(migrated_dsn, dataset_id=dataset_id, object_uri=object_uri) == 1
    assert _read_duplicate_of_file_id(migrated_dsn, first_file_id) == original_file_id


# --- find_file_by_content_hash (04-10 gap closure: CR-02) -------------------
#
# Deterministic duplicate-file resolution: PostgreSQL documents `LIMIT 1`
# with no `ORDER BY` as unspecified once more than one row matches a WHERE
# clause -- `discovery.py`'s rediscovery-correction logic depends on this
# method returning the SAME row across repeated calls for the same content,
# and the live cluster's `file_id=10` orphan (04-VERIFICATION.md) is what
# happens when that assumption breaks.


def test_find_file_by_content_hash_resolves_to_the_lowest_file_id_deterministically(
    repository: PostgresMetadataRepository,
) -> None:
    dataset_id = repository.get_or_create_dataset("find_file_deterministic_proof")
    content_sha256 = hashlib.sha256(b"three-way duplicate content, CR-02").digest()

    file_ids = [
        repository.create_file(
            dataset_id=dataset_id,
            object_uri=f"s3://raw/customers/deterministic-{index}.csv",
            content_sha256=content_sha256,
            hash_version=1,
            size_bytes=10,
            filename=f"deterministic-{index}.csv",
            status="DISCOVERED",
        )
        for index in range(3)
    ]

    expected = min(file_ids)
    for _ in range(5):
        resolved = repository.find_file_by_content_hash(
            dataset_id=dataset_id,
            content_sha256=content_sha256,
        )
        assert resolved == expected
