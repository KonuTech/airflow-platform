"""Quarantined runs' staged rows must never reach gold (finding 20b's exclusion half).

Red/green regression for debug/ci-pipeline-ingestion-timeout ROUND 16
finding (20b): a run terminally QUARANTINED at publish time (ROUND 14's
breaker semantics) has already staged bronze rows -- and, pre-fix, two live
leak paths delivered them to gold anyway: ``SCDPublisher``'s per-key
recompute reads a key's ENTIRE bronze history unscoped (Finding F-1), and
``OrdersMergePublisher``/``MergePublisher`` publish the WHOLE cumulative
silver table. Quarantine blocked the PASS, never the DATA. The fix adds
``_run_id NOT IN (SELECT run_id FROM meta.ingestion_runs WHERE status =
'QUARANTINED')`` to all three publishers' reads; retention/identifiability
of the excluded rows is migration 0041's ``meta.v_quarantined_artifacts``
(full disposition design: docs/adr/0012-quarantined-run-artifact-
disposition.md).

Harness shapes (``_seed_run``/``_insert_bronze_customer``/``_make_context``)
are copied from ``test_publish_scd.py``/``test_publish_orders.py`` per this
suite's own per-file helper-duplication convention.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any

import psycopg
import pytest

from dataplat.config.model import (
    BatchingConfig,
    ColumnContract,
    DatasetConfig,
    LoadConfig,
    ScdConfig,
    SourceConfig,
)
from dataplat.load.publish.merge_orders import OrdersMergePublisher
from dataplat.load.publish.scd import SCDPublisher
from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.models.identity import RunContext
from dataplat.pipeline.protocol import PipelineContext
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration


def _insert_config_version(migrated_dsn: str, *, dataset_id: int) -> int:
    """See ``test_publish_scd.py``'s identical helper for the full rationale."""
    with psycopg.connect(migrated_dsn, autocommit=True) as conn:
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
                "config_hash": "synthetic-hash-for-quarantine-test",
                "config_document": '{"synthetic": true}',
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
) -> int:
    """Create dataset+config_version+file+batch+RUNNING run; return ``run_id``."""
    dataset_id = repository.get_or_create_dataset(f"quarantine_excl_{key_suffix}")
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
        batch_key=f"{key_suffix}:2026-08-28:1",
        status="OPEN",
    )
    return repository.create_ingestion_run(
        idempotency_key=f"{key_suffix}:1",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        status="RUNNING",
        file_id=file_id,
        batch_id=batch_id,
    )


def _insert_bronze_customer(  # noqa: PLR0913 -- one keyword per column, mirrors test_publish_scd.py
    conn: psycopg.Connection[Any],
    *,
    customer_id: str,
    name: str,
    country: str,
    event_ts: str,
    run_id: int,
    source_row_number: int,
) -> None:
    conn.execute(
        """
        INSERT INTO staging.customers (
            customer_id, name, country, birth_date, event_ts, signup_country,
            _run_id, _file_id, _batch_id, _source_row_number,
            _record_hash, _record_hash_version
        ) VALUES (%s, %s, %s, '1990-01-01', %s, NULL, %s, 1, 1, %s, %s, 1)
        """,
        (
            customer_id,
            name,
            country,
            event_ts,
            run_id,
            source_row_number,
            hashlib.sha256(f"{customer_id}:{run_id}:{source_row_number}".encode()).digest(),
        ),
    )


def _quarantine(migrated_dsn: str, run_id: int) -> None:
    with psycopg.connect(migrated_dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE meta.ingestion_runs SET status = 'QUARANTINED' WHERE run_id = %s",
            (run_id,),
        )


@pytest.fixture
def repository(migrated_dsn: str) -> Iterator[PostgresMetadataRepository]:
    pool = create_pool(migrated_dsn)
    pool.open(wait=True)
    try:
        yield PostgresMetadataRepository(pool)
    finally:
        pool.close()


def _make_scd_context() -> PipelineContext:
    """A ``PipelineContext`` with a real ``scd:`` block -- ``test_publish_scd.py``'s shape."""
    return PipelineContext(
        run=RunContext(run_id=0, idempotency_key="quarantine-excl-placeholder"),
        config=DatasetConfig(
            dataset="customers",
            config_schema_version=1,
            source=SourceConfig(
                type="csv",
                bucket="quarantine-excl-test",
                path="customers/",
                change_semantics="snapshot",
                duplicate_policy="skip",
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
                ),
                ColumnContract(
                    name="name", type="string", nullable=False, required=True, scd_type="type_2"
                ),
                ColumnContract(
                    name="country",
                    type="string",
                    nullable=False,
                    required=True,
                    scd_type="type_2",
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
        ),
        metadata=None,  # type: ignore[arg-type] -- unused by SCDPublisher.publish()
        objects=None,  # type: ignore[arg-type] -- unused by SCDPublisher.publish()
        db=None,  # type: ignore[arg-type] -- unused by SCDPublisher.publish()
        log=None,  # type: ignore[arg-type] -- unused by SCDPublisher.publish()
    )


def _make_orders_context() -> PipelineContext:
    """Fully placeholder -- ``OrdersMergePublisher.publish()`` uses no ctx field."""
    return PipelineContext(
        run=RunContext(run_id=0, idempotency_key="quarantine-excl-orders-placeholder"),
        config=None,  # type: ignore[arg-type] -- unused by OrdersMergePublisher.publish()
        metadata=None,  # type: ignore[arg-type] -- unused
        objects=None,  # type: ignore[arg-type] -- unused
        db=None,  # type: ignore[arg-type] -- unused
        log=None,  # type: ignore[arg-type] -- unused
    )


def test_scd_recompute_never_folds_a_quarantined_runs_bronze_rows_into_gold(
    repository: PostgresMetadataRepository, migrated_dsn: str
) -> None:
    """The RED shape pre-fix: a quarantined delivery's row appeared in the gold version chain.

    Run Q stages a (poisoned) observation for key K, then is terminally
    QUARANTINED at publish. Run R later stages a clean observation for the
    SAME key and publishes. Step C's full-history recompute must fold ONLY
    non-quarantined bronze -- gold must contain exactly one clean version,
    never a version carrying Q's values or Q's lineage.
    """
    customer_id = 973001
    quarantined_run = _seed_run(repository, migrated_dsn, key_suffix="scd-q")
    clean_run = _seed_run(repository, migrated_dsn, key_suffix="scd-r")

    with psycopg.connect(migrated_dsn) as conn:
        _insert_bronze_customer(
            conn,
            customer_id=str(customer_id),
            name="Poisoned Delivery",
            country="XX",
            event_ts="2026-05-01T00:00:00+00:00",
            run_id=quarantined_run,
            source_row_number=1,
        )
        conn.commit()
    _quarantine(migrated_dsn, quarantined_run)

    with psycopg.connect(migrated_dsn) as conn:
        _insert_bronze_customer(
            conn,
            customer_id=str(customer_id),
            name="Clean Delivery",
            country="US",
            event_ts="2026-05-02T00:00:00+00:00",
            run_id=clean_run,
            source_row_number=1,
        )
        conn.commit()

        SCDPublisher().publish(
            _make_scd_context(), "silver.customers", conn, staged_run_ids=[clean_run]
        )
        conn.commit()

    with psycopg.connect(migrated_dsn) as conn:
        versions = conn.execute(
            "SELECT name, _run_id, is_current FROM normalized.customers "
            "WHERE customer_id = %s ORDER BY event_ts",
            (customer_id,),
        ).fetchall()
    assert len(versions) == 1, (
        f"expected exactly ONE gold version for customer_id={customer_id} (the clean run's), "
        f"got {versions!r} -- a second version means the QUARANTINED run's bronze row was "
        f"folded into the recomputed chain (finding 20b's SCD leak path is back)"
    )
    name, run_id_lineage, is_current = versions[0]
    assert name == "Clean Delivery"
    assert int(run_id_lineage) == clean_run, (
        f"gold lineage points at run {run_id_lineage}, expected the clean run {clean_run} -- "
        f"quarantined run {quarantined_run} must never appear in gold lineage"
    )
    assert is_current is True

    # The retained artifacts are identifiable, not silently resident (migration 0041).
    with psycopg.connect(migrated_dsn) as conn:
        artifact = conn.execute(
            "SELECT bronze_rows FROM meta.v_quarantined_artifacts WHERE run_id = %s",
            (quarantined_run,),
        ).fetchone()
    assert artifact is not None, (
        f"meta.v_quarantined_artifacts has no row for quarantined run {quarantined_run}"
    )
    assert int(artifact[0]) == 1


def test_orders_merge_never_publishes_a_quarantined_runs_silver_rows(
    repository: PostgresMetadataRepository, migrated_dsn: str
) -> None:
    """The merge-side leak: a whole-silver upsert must skip QUARANTINED runs' rows.

    A clean sibling run's row in the same silver table must still publish
    (the exclusion is run-scoped, never table-scoped). The NOT-IN
    default-include shape (a ``_run_id`` with no ``meta.ingestion_runs`` row
    at all publishes normally) is proven by ``test_publish_orders.py``'s
    pre-existing scratch-table tests, which carry exactly such run_ids --
    ``silver.orders`` itself cannot host one (its ``_run_id`` FK).
    """
    quarantined_run = _seed_run(repository, migrated_dsn, key_suffix="ord-q")
    _quarantine(migrated_dsn, quarantined_run)
    clean_run = _seed_run(repository, migrated_dsn, key_suffix="ord-r")

    quarantined_order = 973101
    unregistered_order = 973102  # staged by clean_run; name kept for the shared cleanup below

    with psycopg.connect(migrated_dsn) as conn:
        for order_id, run_id in (
            (quarantined_order, quarantined_run),
            (unregistered_order, clean_run),
        ):
            conn.execute(
                """
                INSERT INTO silver.orders (
                    order_id, customer_id, order_date, amount,
                    _run_id, _file_id, _batch_id, _source_row_number,
                    _record_hash, _record_hash_version, _dbt_loaded_at
                ) VALUES (%s, '1', '2026-05-01', '10.00', %s, 1, 1, 1, %s, 1, now())
                """,
                (
                    str(order_id),
                    run_id,
                    hashlib.sha256(f"q-excl:{order_id}".encode()).digest(),
                ),
            )
        conn.commit()

        try:
            OrdersMergePublisher().publish(
                _make_orders_context(),
                "silver.orders",
                conn,
                staged_run_ids=[quarantined_run],
            )
            conn.commit()

            with psycopg.connect(migrated_dsn) as verify:
                published = {
                    int(row[0])
                    for row in verify.execute(
                        "SELECT order_id FROM normalized.orders WHERE order_id IN (%s, %s)",
                        (quarantined_order, unregistered_order),
                    ).fetchall()
                }
            assert unregistered_order in published, (
                "the clean sibling run's silver row must publish -- the quarantine "
                "exclusion must be run-scoped, never table-scoped"
            )
            assert quarantined_order not in published, (
                f"order_id={quarantined_order} (staged by QUARANTINED run "
                f"{quarantined_run}) reached normalized.orders -- finding 20b's merge "
                f"leak path is back"
            )
        finally:
            # Session-shared tables: remove this test's own silver/gold rows so
            # later whole-table publishes/counts are unaffected (same discipline
            # as test_migrations.py's own silver cleanup comments).
            with psycopg.connect(migrated_dsn, autocommit=True) as cleanup:
                cleanup.execute(
                    "DELETE FROM silver.orders WHERE order_id IN (%s, %s)",
                    (str(quarantined_order), str(unregistered_order)),
                )
                cleanup.execute(
                    "DELETE FROM normalized.orders WHERE order_id IN (%s, %s)",
                    (quarantined_order, unregistered_order),
                )
