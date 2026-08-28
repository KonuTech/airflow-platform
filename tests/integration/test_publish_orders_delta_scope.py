"""Red/green proof for ``OrdersMergePublisher``'s delta-scoped publish (ROUND 17, finding 25).

debug/ci-pipeline-ingestion-timeout ROUND 16 finding (25): the live orders
pipeline's publish read the WHOLE silver table each pass, so publish cost
scaled with ACCUMULATED silver mass, not the pass's delta -- every retained
1M-row fixture taxed every later publish, collapsing the serialized
(max_active_runs=1) orders pipe on CI. ROUND 17 scopes ``_PUBLISH_SQL`` to
``_run_id = ANY(staged_run_ids)``.

Two proofs here, both against a real testcontainers PostgreSQL migrated to
head (this suite's own conventions -- helpers duplicated locally per the
per-file helper convention, see ``test_publish_orders.py``):

1. **Equivalence** (regression guard): the same input sequence published
   delta-scoped, pass by pass, produces byte-identical
   ``normalized.orders`` rows to a single legacy whole-table merge over the
   final silver state -- the delta scoping changes COST, never RESULT.
2. **No-rescan** (the red/green core): a pass whose ``staged_run_ids``
   exclude an already-published key must not read or row-lock that key's
   gold row. Proven deterministically via a concurrently-held ``FOR
   UPDATE`` lock on the old key plus ``lock_timeout`` on the publishing
   connection: the pre-fix whole-table merge blocks on the held lock (RED
   -- ``ON CONFLICT`` takes the row lock even when its ``DO UPDATE ...
   WHERE`` guard would leave the row unchanged); the delta-scoped merge
   never touches the row and completes (GREEN). No timing flake: the lock
   is held for the duration of the statement, and ``lock_timeout`` turns
   "blocked" into a prompt, named error.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

import psycopg
import pytest

from dataplat.load.publish.merge_orders import OrdersMergePublisher
from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.models.identity import RunContext
from dataplat.pipeline.protocol import PipelineContext
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    from collections.abc import Iterator

_STAGING_COLUMNS_DDL = """
    order_id text, customer_id text, order_date text, amount text,
    _run_id bigint, _file_id bigint, _batch_id bigint,
    _source_row_number bigint, _record_hash bytea, _record_hash_version smallint
"""

# The pre-ROUND-17 publish statement, verbatim minus the delta predicate --
# the REFERENCE the equivalence proof compares against. Kept inline here (a
# test-owned fossil, never imported from production code) so the equivalence
# claim stays checkable even after the production SQL evolves further.
_LEGACY_FULL_TABLE_PUBLISH_SQL = """
INSERT INTO normalized.orders (
    order_id, customer_id, order_date, amount,
    _run_id, _file_id, _batch_id, _source_row_number,
    _record_hash, _record_hash_version
)
SELECT DISTINCT ON (order_id)
       order_id::int, customer_id::int, order_date::date, amount::numeric,
       _run_id, _file_id, _batch_id, _source_row_number,
       _record_hash, _record_hash_version
FROM   {staging_table}
WHERE  _run_id NOT IN (
           SELECT run_id FROM meta.ingestion_runs WHERE status = 'QUARANTINED'
       )
ORDER  BY order_id, order_date DESC, _source_row_number DESC
ON CONFLICT (order_id) DO UPDATE
   SET customer_id = EXCLUDED.customer_id, order_date = EXCLUDED.order_date,
       amount = EXCLUDED.amount,
       _record_hash = EXCLUDED._record_hash,
       _record_hash_version = EXCLUDED._record_hash_version,
       _run_id = EXCLUDED._run_id, _file_id = EXCLUDED._file_id,
       _batch_id = EXCLUDED._batch_id, _source_row_number = EXCLUDED._source_row_number
 WHERE normalized.orders._record_hash IS DISTINCT FROM EXCLUDED._record_hash
   AND (normalized.orders.order_date IS NULL
        OR EXCLUDED.order_date >= normalized.orders.order_date)
RETURNING order_id
"""

# Disjoint from every other integration test's order_id choices (they use
# small 4-digit ids); high enough to never collide in the shared
# session-scoped database.
_EQUIV_KEY_BASE = 987_654_000
_LOCK_KEY_BASE = 987_655_000


def _make_context() -> PipelineContext:
    """Fully placeholder -- ``OrdersMergePublisher.publish()`` uses no ctx field."""
    return PipelineContext(
        run=RunContext(run_id=1, idempotency_key="test-run"),
        config=None,  # type: ignore[arg-type] -- unused by OrdersMergePublisher.publish()
        metadata=None,  # type: ignore[arg-type] -- unused by OrdersMergePublisher.publish()
        objects=None,  # type: ignore[arg-type] -- unused by OrdersMergePublisher.publish()
        db=None,  # type: ignore[arg-type] -- unused by OrdersMergePublisher.publish()
        log=None,  # type: ignore[arg-type] -- unused by OrdersMergePublisher.publish()
    )


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Insert a synthetic `meta.config_versions` row (per-file helper convention)."""
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
) -> tuple[int, int, int]:
    """Create dataset+config_version+file+batch+RUNNING run -> ``(run_id, file_id, batch_id)``."""
    dataset_id = repository.get_or_create_dataset(f"orders_test_{key_suffix}")
    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id)
    file_id = repository.create_file(
        dataset_id=dataset_id,
        object_uri=f"s3://raw/orders/{key_suffix}.csv",
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
    run_id = repository.create_ingestion_run(
        idempotency_key=f"{key_suffix}:1",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        status="RUNNING",
        file_id=file_id,
        batch_id=batch_id,
    )
    return run_id, file_id, batch_id


def _create_staging_table(conn: psycopg.Connection[Any], table_name: str) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(f"CREATE UNLOGGED TABLE {table_name} ({_STAGING_COLUMNS_DDL})")


def _insert_staging_row(  # noqa: PLR0913 -- one keyword per staging column, mirrors staging.py's shape
    conn: psycopg.Connection[Any],
    table_name: str,
    *,
    order_id: int,
    customer_id: str,
    order_date: str | None,
    amount: str,
    run_id: int,
    file_id: int,
    batch_id: int,
    source_row_number: int,
    record_hash: bytes,
) -> None:
    conn.execute(
        f"""
        INSERT INTO {table_name} (
            order_id, customer_id, order_date, amount,
            _run_id, _file_id, _batch_id, _source_row_number,
            _record_hash, _record_hash_version
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
        """,  # noqa: S608 -- test-controlled identifier only; every value crosses via %s
        (
            str(order_id),
            customer_id,
            order_date,
            amount,
            run_id,
            file_id,
            batch_id,
            source_row_number,
            record_hash,
        ),
    )


def _gold_rows(conn: psycopg.Connection[Any], *, low: int, high: int) -> list[tuple[Any, ...]]:
    """Every ``normalized.orders`` column for keys in ``[low, high]``, deterministically ordered."""
    return conn.execute(
        """
        SELECT order_id, customer_id, order_date, amount,
               _run_id, _file_id, _batch_id, _source_row_number,
               _record_hash, _record_hash_version
          FROM normalized.orders
         WHERE order_id BETWEEN %s AND %s
         ORDER BY order_id
        """,
        (low, high),
    ).fetchall()


@pytest.fixture
def repository(migrated_dsn: str) -> Iterator[PostgresMetadataRepository]:
    """A `PostgresMetadataRepository` backed by an opened pool over the migrated database."""
    pool = create_pool(migrated_dsn)
    pool.open(wait=True)
    try:
        yield PostgresMetadataRepository(pool)
    finally:
        pool.close()


@pytest.fixture(autouse=True)
def _drop_scratch_tables(migrated_dsn: str) -> Iterator[None]:
    """Drop this module's silver-mimic scratch tables after each test.

    The session-scoped database's schema-level default privileges mean a
    leftover table in ``staging`` shows up in ``information_schema.
    role_table_grants`` and would trip ``test_migrations.py``'s exact
    dbt_app staging-grant enumeration if this module ever ran first --
    dropping eagerly removes the ordering coupling instead of relying on
    alphabetical collection order.
    """
    yield
    with psycopg.connect(migrated_dsn, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS staging.orders_test_delta_equiv")
        conn.execute("DROP TABLE IF EXISTS staging.orders_test_delta_lock")
        conn.execute(
            "DELETE FROM normalized.orders WHERE order_id BETWEEN %s AND %s",
            (_EQUIV_KEY_BASE, _LOCK_KEY_BASE + 10),
        )


@pytest.mark.integration
def test_delta_scoped_passes_produce_gold_identical_to_a_full_table_merge(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    """Equivalence: delta scoping changes publish COST, never gold CONTENT.

    Sequence (silver is one row per business key -- dbt's own
    delete+insert/unique_key shape, mirrored here by hand):

    - pass 1 (run r1): keys K0 (later untouched), K1 (later updated).
    - pass 2 (run r2): K1 updated (newer order_date, new hash), K2 inserted;
      K0 carried in silver still attributed to r1 (out of pass 2's delta).

    The delta-scoped result after both passes must be byte-identical to a
    single legacy whole-table merge over the FINAL silver state.
    """
    r1, f1, b1 = _seed_run(repository, migrated_dsn, key_suffix="orders_delta_equiv_r1")
    r2, f2, b2 = _seed_run(repository, migrated_dsn, key_suffix="orders_delta_equiv_r2")
    k0, k1, k2 = _EQUIV_KEY_BASE, _EQUIV_KEY_BASE + 1, _EQUIV_KEY_BASE + 2
    silver_mimic = "staging.orders_test_delta_equiv"
    hash_k0 = hashlib.sha256(b"k0-v1").digest()
    hash_k1_v1 = hashlib.sha256(b"k1-v1").digest()
    hash_k1_v2 = hashlib.sha256(b"k1-v2").digest()
    hash_k2 = hashlib.sha256(b"k2-v1").digest()

    def _insert(  # noqa: PLR0913 -- one keyword per column, mirrors _insert_staging_row
        conn: psycopg.Connection[Any],
        *,
        order_id: int,
        order_date: str,
        amount: str,
        run_id: int,
        file_id: int,
        batch_id: int,
        row_number: int,
        record_hash: bytes,
    ) -> None:
        _insert_staging_row(
            conn,
            silver_mimic,
            order_id=order_id,
            customer_id="5001",
            order_date=order_date,
            amount=amount,
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=row_number,
            record_hash=record_hash,
        )

    def _fill_silver_state_1(conn: psycopg.Connection[Any]) -> None:
        _create_staging_table(conn, silver_mimic)
        _insert(
            conn,
            order_id=k0,
            order_date="2026-01-10",
            amount="10.00",
            run_id=r1,
            file_id=f1,
            batch_id=b1,
            row_number=1,
            record_hash=hash_k0,
        )
        _insert(
            conn,
            order_id=k1,
            order_date="2026-01-11",
            amount="11.00",
            run_id=r1,
            file_id=f1,
            batch_id=b1,
            row_number=2,
            record_hash=hash_k1_v1,
        )

    def _fill_silver_state_2(conn: psycopg.Connection[Any]) -> None:
        # One row per key: K1's winner is now r2's row; K0 unchanged (r1).
        _create_staging_table(conn, silver_mimic)
        _insert(
            conn,
            order_id=k0,
            order_date="2026-01-10",
            amount="10.00",
            run_id=r1,
            file_id=f1,
            batch_id=b1,
            row_number=1,
            record_hash=hash_k0,
        )
        _insert(
            conn,
            order_id=k1,
            order_date="2026-02-01",
            amount="22.00",
            run_id=r2,
            file_id=f2,
            batch_id=b2,
            row_number=1,
            record_hash=hash_k1_v2,
        )
        _insert(
            conn,
            order_id=k2,
            order_date="2026-02-02",
            amount="33.00",
            run_id=r2,
            file_id=f2,
            batch_id=b2,
            row_number=2,
            record_hash=hash_k2,
        )

    # --- Delta-scoped sequence: pass 1 over state 1, pass 2 over state 2. ---
    with psycopg.connect(migrated_dsn) as conn:
        _fill_silver_state_1(conn)
        pass1 = OrdersMergePublisher().publish(
            _make_context(), silver_mimic, conn, staged_run_ids=[r1]
        )
        conn.commit()
    assert pass1.rows_affected == 2
    assert set(pass1.published_business_keys) == {str(k0), str(k1)}

    with psycopg.connect(migrated_dsn) as conn:
        _fill_silver_state_2(conn)
        pass2 = OrdersMergePublisher().publish(
            _make_context(), silver_mimic, conn, staged_run_ids=[r2]
        )
        conn.commit()
    # Pass 2's affected set is exactly its own delta -- K0 is not re-touched.
    assert pass2.rows_affected == 2
    assert set(pass2.published_business_keys) == {str(k1), str(k2)}

    with psycopg.connect(migrated_dsn) as conn:
        delta_gold = _gold_rows(conn, low=k0, high=k2)
        assert len(delta_gold) == 3

        # --- Reference: wipe these keys, replay ONE legacy whole-table merge
        #     over the final silver state, snapshot again. ---
        conn.execute("DELETE FROM normalized.orders WHERE order_id BETWEEN %s AND %s", (k0, k2))
        _fill_silver_state_2(conn)
        conn.execute(_LEGACY_FULL_TABLE_PUBLISH_SQL.format(staging_table=silver_mimic))
        full_gold = _gold_rows(conn, low=k0, high=k2)
        conn.commit()

    assert delta_gold == full_gold, (
        "delta-scoped pass-by-pass publish and the legacy whole-table merge over the same "
        "final silver state produced DIFFERENT normalized.orders rows -- the delta scoping "
        "must change cost only, never content"
    )


@pytest.mark.integration
def test_publish_never_rescans_or_locks_keys_outside_the_pass_delta(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    """No-rescan proof (finding 25's exact mechanism, deterministically observable).

    A concurrent transaction holds ``FOR UPDATE`` on an already-published
    key's gold row while a new pass publishes a DIFFERENT run's delta. The
    pre-ROUND-17 whole-table merge re-upserted every silver key each pass
    and therefore blocked on that held lock (``ON CONFLICT`` locks the
    conflicting row even when its ``DO UPDATE ... WHERE`` guard is false --
    "locked but left unchanged"); the delta-scoped merge never selects the
    old key and completes immediately. ``lock_timeout`` on the publishing
    connection turns the pre-fix behavior into a prompt
    ``LockNotAvailable`` instead of a hang, so this test is RED on the
    pre-fix SQL and GREEN on the delta-scoped SQL with zero timing
    sensitivity.
    """
    r_old, f_old, b_old = _seed_run(repository, migrated_dsn, key_suffix="orders_lock_old")
    r_new, f_new, b_new = _seed_run(repository, migrated_dsn, key_suffix="orders_lock_new")
    k_old, k_new = _LOCK_KEY_BASE, _LOCK_KEY_BASE + 1
    silver_mimic = "staging.orders_test_delta_lock"

    # Pass 1: publish the old key under r_old.
    with psycopg.connect(migrated_dsn) as conn:
        _create_staging_table(conn, silver_mimic)
        _insert_staging_row(
            conn,
            silver_mimic,
            order_id=k_old,
            customer_id="5002",
            order_date="2026-03-01",
            amount="10.00",
            run_id=r_old,
            file_id=f_old,
            batch_id=b_old,
            source_row_number=1,
            record_hash=hashlib.sha256(b"lock-old-v1").digest(),
        )
        first = OrdersMergePublisher().publish(
            _make_context(), silver_mimic, conn, staged_run_ids=[r_old]
        )
        conn.commit()
    assert first.rows_affected == 1

    # Cumulative silver now carries BOTH keys (the old winner row is
    # retained, exactly as the live silver.orders retains every key).
    with psycopg.connect(migrated_dsn) as conn:
        _insert_staging_row(
            conn,
            silver_mimic,
            order_id=k_new,
            customer_id="5002",
            order_date="2026-03-02",
            amount="20.00",
            run_id=r_new,
            file_id=f_new,
            batch_id=b_new,
            source_row_number=1,
            record_hash=hashlib.sha256(b"lock-new-v1").digest(),
        )
        conn.commit()

    # A concurrent holder pins the OLD key's gold row for the whole pass-2
    # publish. Held via an explicit open transaction on a second connection.
    holder = psycopg.connect(migrated_dsn)
    try:
        holder.execute("SELECT 1 FROM normalized.orders WHERE order_id = %s FOR UPDATE", (k_old,))
        with psycopg.connect(migrated_dsn) as conn:
            conn.execute("SET lock_timeout = '2000ms'")
            second = OrdersMergePublisher().publish(
                _make_context(), silver_mimic, conn, staged_run_ids=[r_new]
            )
            conn.commit()
    finally:
        holder.rollback()
        holder.close()

    assert second.rows_affected == 1
    assert set(second.published_business_keys) == {str(k_new)}, (
        "pass 2 (staged_run_ids=[r_new]) affected keys outside its own delta -- the publish "
        "statement is rescanning accumulated silver mass"
    )
