"""Out-of-order staging must never be silently dropped by the silver models (finding 21).

Red/green regression for debug/ci-pipeline-ingestion-timeout ROUND 16
finding (21), observed live on CI run 33103279876: the silver models'
incremental filter was `_run_id > max(_run_id) from {{ this }}` -- a GLOBAL
max watermark -- so any run whose bronze rows commit AFTER a higher
`_run_id` has already been dbt-built fell below the floor forever and was
never selected by any later build. Out-of-order staging is routine on the
real platform (capped claim batches, stage retries, lease reclaims, replay
waves), so this was a silent-drop-class correctness bug, not a lineage
nicety: the sweep's late-file replay run (run 42, SUCCEEDED, 50 bronze rows)
was permanently absent from `silver.customers`.

The fix (migration 0040 + dbt/macros/claim_dbt_processed_runs.sql): each
build CLAIMS every not-yet-claimed bronze `_run_id` into
`meta.dbt_processed_runs` via a same-transaction pre-hook, and the model
selects exactly its own transaction's claimed set (`claimed_txid =
txid_current()`).

This test reproduces the exact out-of-order shape: run A and run B are
pre-allocated in ascending `run_id` order (exactly what discovery does), but
run B's bronze rows land and get dbt-built FIRST; run A's bronze rows land
only afterwards. Pre-fix, the second build's floor (`max(_run_id)` = B)
excluded run A's rows and the `OOOA` key never reached silver -- this test
failed on exactly that assertion (RED confirmed against the pre-fix models).

Follows `test_dbt_silver_incremental.py`'s harness shapes verbatim
(`_seed_ingestion_run`/`_insert_bronze_customer` copied per this repo's
per-tier-copy convention for small helpers).

FILENAME IS ORDER-SENSITIVE: `test_dbt_silver_out_of_order.py` deliberately
sorts AFTER `test_dbt_reconciliation.py` in pytest's alphabetical file
collection. `test_reconciliation_post_hook_writes_a_row_with_the_correct_
discrepancy_formula` asserts `discrepancy == 0` against the SESSION-SHARED
whole-table counts, and the per-test cleanup fixture (`conftest.py`) deletes
every non-numeric silver key while its bronze rows remain -- so any
dbt-marked test that runs BEFORE the reconciliation test and leaves
cleaned-up winner keys behind shifts that discrepancy off zero (a
pre-existing property of this suite's shared-database design, not something
this file introduces; verified empirically while adding this file under its
original, earlier-sorting name).
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

import psycopg
import pytest

from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

pytestmark = [pytest.mark.dbt, pytest.mark.integration]


def _get_or_create_config_version(
    conn: psycopg.Connection, *, dataset_id: int, key_suffix: str
) -> int:
    """See `test_dbt_silver_dedup.py`'s identical helper for the full rationale."""
    existing = conn.execute(
        "SELECT config_version_id FROM meta.config_versions "
        "WHERE dataset_id = %s AND valid_to IS NULL",
        (dataset_id,),
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
                SELECT COALESCE(MAX(version), 0) + 1 FROM meta.config_versions
                WHERE dataset_id = %(dataset_id)s
            ),
            %(config_hash)s, %(config_document)s::jsonb, %(config_schema_version)s, now()
        )
        RETURNING config_version_id
        """,
        {
            "dataset_id": dataset_id,
            "config_hash": f"dbt-test-hash-{key_suffix}",
            "config_document": '{"synthetic": true}',
            "config_schema_version": 1,
        },
    ).fetchone()
    assert row is not None
    return int(row[0])


def _seed_ingestion_run(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
    *,
    dataset_name: str,
    key_suffix: str,
    run_number: int,
) -> tuple[int, int, int, int]:
    """See `test_dbt_silver_dedup.py`'s identical helper for the full rationale."""
    dataset_id = repository.get_or_create_dataset(dataset_name)
    with psycopg.connect(migrated_dsn, autocommit=True) as conn:
        config_version_id = _get_or_create_config_version(
            conn, dataset_id=dataset_id, key_suffix=key_suffix
        )
    file_id = repository.create_file(
        dataset_id=dataset_id,
        object_uri=f"s3://raw/{dataset_name}/{key_suffix}-{run_number}.csv",
        content_sha256=hashlib.sha256(f"{key_suffix}-{run_number}".encode()).digest(),
        hash_version=1,
        size_bytes=10,
        filename=f"{key_suffix}-{run_number}.csv",
        status="DISCOVERED",
    )
    batch_id = repository.create_batch(
        dataset_id=dataset_id,
        batch_key=f"{key_suffix}:{run_number}",
        status="OPEN",
    )
    run_id = repository.create_ingestion_run(
        idempotency_key=f"{key_suffix}:{run_number}",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        status="RUNNING",
        file_id=file_id,
        batch_id=batch_id,
    )
    return dataset_id, run_id, file_id, batch_id


def _insert_bronze_customer(  # noqa: PLR0913 -- one keyword per staging column
    conn: psycopg.Connection,
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


@pytest.fixture
def repository(migrated_dsn: str) -> Iterator[PostgresMetadataRepository]:
    """A `PostgresMetadataRepository` backed by an opened pool over the migrated database."""
    pool = create_pool(migrated_dsn)
    pool.open(wait=True)
    try:
        yield PostgresMetadataRepository(pool)
    finally:
        pool.close()


def test_lower_run_id_staged_after_higher_run_still_reaches_silver(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
    run_dbt_build: Callable[..., object],
) -> None:
    """Finding 21's exact shape: run A (lower id) stages AFTER run B (higher id) was built.

    Pre-allocate runs A then B (ascending run_id, exactly discovery's own
    order); stage bronze for B only; `dbt build` (silver gains B's key);
    stage bronze for A; `dbt build` again. A's genuinely-new business key
    MUST reach silver -- under the old global-max watermark it never could
    (A's run_id < the floor B set), which is the silent drop this guards.
    """
    _dataset_id, run_a_id, file_a_id, batch_a_id = _seed_ingestion_run(
        repository, migrated_dsn, dataset_name="customers", key_suffix="ooo", run_number=1
    )
    _dataset_id, run_b_id, file_b_id, batch_b_id = _seed_ingestion_run(
        repository, migrated_dsn, dataset_name="customers", key_suffix="ooo", run_number=2
    )
    assert run_a_id < run_b_id, (
        f"harness precondition: run A ({run_a_id}) must have the LOWER run_id "
        f"(B: {run_b_id}) for this to exercise the out-of-order shape at all"
    )

    # Step 1: run B's bronze lands FIRST and gets built.
    with psycopg.connect(migrated_dsn, autocommit=True) as conn:
        _insert_bronze_customer(
            conn,
            customer_id="OOOB",
            name="Later Run First",
            country="US",
            birth_date="1980-01-01",
            event_ts="2026-04-02T00:00:00+00:00",
            run_id=run_b_id,
            file_id=file_b_id,
            batch_id=batch_b_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"ooo-b-1").digest(),
        )
    run_dbt_build(migrated_dsn, select="silver_customers")

    with psycopg.connect(migrated_dsn) as verify_conn:
        b_row = verify_conn.execute(
            "SELECT _run_id FROM silver.customers WHERE customer_id = 'OOOB'",
        ).fetchone()
    assert b_row is not None, "run B's key never reached silver -- harness broken"
    assert int(b_row[0]) == run_b_id

    # Step 2: run A's bronze lands SECOND -- a stage retry / lease reclaim /
    # replay-wave completion, in real-platform terms.
    with psycopg.connect(migrated_dsn, autocommit=True) as conn:
        _insert_bronze_customer(
            conn,
            customer_id="OOOA",
            name="Earlier Run Late",
            country="GB",
            birth_date="1975-06-06",
            event_ts="2026-04-01T00:00:00+00:00",
            run_id=run_a_id,
            file_id=file_a_id,
            batch_id=batch_a_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"ooo-a-1").digest(),
        )
    run_dbt_build(migrated_dsn, select="silver_customers")

    # THE regression assertion: pre-fix, OOOA is permanently absent (RED).
    with psycopg.connect(migrated_dsn) as verify_conn:
        a_row = verify_conn.execute(
            "SELECT _run_id FROM silver.customers WHERE customer_id = 'OOOA'",
        ).fetchone()
        claims = verify_conn.execute(
            "SELECT run_id FROM meta.dbt_processed_runs "
            "WHERE dataset_name = 'customers' AND run_id IN (%s, %s) ORDER BY run_id",
            (run_a_id, run_b_id),
        ).fetchall()
    assert a_row is not None, (
        f"silver.customers has no row for customer_id='OOOA' (run {run_a_id}, staged after "
        f"run {run_b_id} was already built) -- the out-of-order silent drop is back: the "
        f"incremental eligibility filter excluded a run staged below the current floor "
        f"(finding 21, debug/ci-pipeline-ingestion-timeout ROUND 16)"
    )
    assert int(a_row[0]) == run_a_id
    assert [int(r[0]) for r in claims] == [run_a_id, run_b_id], (
        f"meta.dbt_processed_runs should hold exactly one claim per built run, got {claims!r}"
    )
