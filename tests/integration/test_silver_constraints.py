"""Integration test proving D-14/Pitfall 5: silver's UNIQUE constraint is genuinely load-bearing.

Bypassing dbt entirely, a direct `psycopg` INSERT of a second row sharing an
already-present `customer_id`/`order_id` into `silver.customers`/
`silver.orders` raises `psycopg.errors.UniqueViolation`. This is the DB-level
guarantee migration 0023 creates independently of dbt's own `unique_key`
incremental-model logic -- Pitfall 5 (08.1-RESEARCH.md): `dbt build`'s test
step does not block a bad model write (the model's data is already committed
by the time a `unique`/`not_null` *test* would report failure), so the real
constraint, not dbt's own test exit code, is what's load-bearing.

No `run_dbt_build` call anywhere in this file -- it proves a property of the
Alembic-created schema itself, independent of any dbt invocation -- but every
test here still carries both markers (`dbt`/`integration`) per this plan's
own file-grouping convention.
"""

from __future__ import annotations

import hashlib

import psycopg
import pytest

from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.storage.db import create_pool

pytestmark = [pytest.mark.dbt, pytest.mark.integration]


def _get_or_create_config_version(
    conn: psycopg.Connection, *, dataset_id: int, key_suffix: str
) -> int:
    """Return the dataset's single CURRENT config_version_id, creating one if none exists yet.

    `meta.config_versions` has a partial UNIQUE index allowing at most one
    `valid_to IS NULL` ("current") row per `dataset_id` (migration 0001).
    Since `migrated_dsn` is session-scoped and shared across every test file
    in this `-m "dbt and integration"` collection, an earlier test file may
    already have created a "customers"/"orders" dataset's current config
    version -- this helper reuses it instead of colliding with it.
    """
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
            "config_hash": f"dbt-constraint-test-{key_suffix}",
            "config_document": '{"synthetic": true}',
            "config_schema_version": 1,
        },
    ).fetchone()
    assert row is not None
    return int(row[0])


def _seed_dataset_and_run(
    migrated_dsn: str, *, dataset_name: str, key_suffix: str
) -> tuple[int, int, int]:
    """Create dataset+config_version+file+batch+RUNNING run; return `(run_id, file_id, batch_id)`.

    `silver.customers`/`silver.orders`' six lineage columns (migration
    0023) are real foreign keys, so even a hand-crafted constraint-violation
    probe row needs FK-satisfying lineage values.
    """
    pool = create_pool(migrated_dsn)
    pool.open(wait=True)
    try:
        repository = PostgresMetadataRepository(pool)
        dataset_id = repository.get_or_create_dataset(dataset_name)
        with psycopg.connect(migrated_dsn, autocommit=True) as conn:
            config_version_id = _get_or_create_config_version(
                conn, dataset_id=dataset_id, key_suffix=key_suffix
            )
        file_id = repository.create_file(
            dataset_id=dataset_id,
            object_uri=f"s3://raw/{dataset_name}/{key_suffix}.csv",
            content_sha256=hashlib.sha256(key_suffix.encode()).digest(),
            hash_version=1,
            size_bytes=10,
            filename=f"{key_suffix}.csv",
            status="DISCOVERED",
        )
        batch_id = repository.create_batch(
            dataset_id=dataset_id, batch_key=key_suffix, status="OPEN"
        )
        run_id = repository.create_ingestion_run(
            idempotency_key=key_suffix,
            dataset_id=dataset_id,
            config_version_id=config_version_id,
            processor_version="0.1.0",
            processor_image_digest="sha256:testdigest",
            status="RUNNING",
            file_id=file_id,
            batch_id=batch_id,
        )
        return run_id, file_id, batch_id
    finally:
        pool.close()


def test_silver_customers_customer_id_unique_constraint_is_real(migrated_dsn: str) -> None:
    """D-14: a duplicate `customer_id` row raises `UniqueViolation`, never a silent overwrite."""
    run_id, file_id, batch_id = _seed_dataset_and_run(
        migrated_dsn,
        dataset_name="customers",
        key_suffix="constraint-customers",
    )

    with psycopg.connect(migrated_dsn, autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO silver.customers (
                customer_id, name, country, birth_date, event_ts,
                _run_id, _file_id, _batch_id, _source_row_number,
                _record_hash, _record_hash_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
            """,
            (
                "CONST1",
                "First",
                "US",
                "1990-01-01",
                "2026-01-01T00:00:00+00:00",
                run_id,
                file_id,
                batch_id,
                1,
                hashlib.sha256(b"const1-first").digest(),
            ),
        )

        with pytest.raises(psycopg.errors.UniqueViolation):
            conn.execute(
                """
                INSERT INTO silver.customers (
                    customer_id, name, country, birth_date, event_ts,
                    _run_id, _file_id, _batch_id, _source_row_number,
                    _record_hash, _record_hash_version
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
                """,
                (
                    "CONST1",
                    "Duplicate",
                    "US",
                    "1990-01-01",
                    "2026-01-02T00:00:00+00:00",
                    run_id,
                    file_id,
                    batch_id,
                    2,
                    hashlib.sha256(b"const1-dup").digest(),
                ),
            )


def test_silver_orders_order_id_unique_constraint_is_real(migrated_dsn: str) -> None:
    """A hand-crafted second `order_id` row raises `UniqueViolation`, never a silent overwrite."""
    run_id, file_id, batch_id = _seed_dataset_and_run(
        migrated_dsn,
        dataset_name="orders",
        key_suffix="constraint-orders",
    )

    with psycopg.connect(migrated_dsn, autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO silver.orders (
                order_id, customer_id, order_date, amount,
                _run_id, _file_id, _batch_id, _source_row_number,
                _record_hash, _record_hash_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
            """,
            (
                "OCONST1",
                "CONST1",
                "2026-01-01",
                "10.00",
                run_id,
                file_id,
                batch_id,
                1,
                hashlib.sha256(b"oconst1-first").digest(),
            ),
        )

        with pytest.raises(psycopg.errors.UniqueViolation):
            conn.execute(
                """
                INSERT INTO silver.orders (
                    order_id, customer_id, order_date, amount,
                    _run_id, _file_id, _batch_id, _source_row_number,
                    _record_hash, _record_hash_version
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
                """,
                (
                    "OCONST1",
                    "CONST1",
                    "2026-01-02",
                    "20.00",
                    run_id,
                    file_id,
                    batch_id,
                    2,
                    hashlib.sha256(b"oconst1-dup").digest(),
                ),
            )
