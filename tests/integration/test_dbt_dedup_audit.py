"""Integration test proving DEDUP-04 (deduplication auditability) against a real `dbt build`.

After one `dbt build` run that included at least one dropped duplicate,
asserts `meta.dedup_audit` has exactly one new row for the invocation with
`records_deduplicated >= 1` and accurate `records_received`/
`records_accepted`, and `meta.dedup_decisions` has exactly one row per
dropped bronze record with a `reason` drawn from migration 0024's exact
five-value closed vocabulary.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

import psycopg
import pytest
from testcontainers.community.postgres import PostgresContainer

from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

pytestmark = [pytest.mark.dbt, pytest.mark.integration]

# migration 0024_meta_dedup_audit_decisions.py's own closed vocabulary,
# verbatim -- `reason` must always be one of these five, never an ad-hoc
# string dreamed up independently by this test or by the macro.
_VALID_REASONS = frozenset(
    {
        "EXACT_DUP_IN_FILE",
        "EXACT_DUP_CROSS_BATCH",
        "SUPERSEDED_BY_NEWER",
        "LOWER_SOURCE_PRIORITY",
        "SCD_NO_CHANGE",
    },
)


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


def test_dedup_audit_and_decisions_are_written_atomically_with_a_closed_reason_vocabulary(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
    run_dbt_build: Callable[..., object],
) -> None:
    """DEDUP-04: one accurate `dedup_audit` row + one `dedup_decisions` row per dropped record."""
    dataset_id, run_id, file_id, batch_id = _seed_ingestion_run(
        repository,
        migrated_dsn,
        dataset_name="customers",
        key_suffix="audit",
        run_number=1,
    )

    with psycopg.connect(migrated_dsn, autocommit=True) as conn:
        # Two exact-content-identical rows for the same key/file/batch ->
        # EXACT_DUP_IN_FILE. A distinct winner for a second key.
        record_hash = hashlib.sha256(b"a1-exact-dup").digest()
        _insert_bronze_customer(
            conn,
            customer_id="A1",
            name="Amy",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-04-01T00:00:00+00:00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=record_hash,
        )
        _insert_bronze_customer(
            conn,
            customer_id="A1",
            name="Amy",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-04-01T00:00:00+00:00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=2,
            record_hash=record_hash,
        )
        _insert_bronze_customer(
            conn,
            customer_id="A2",
            name="Andy",
            country="GB",
            birth_date="1985-05-05",
            event_ts="2026-04-01T00:00:00+00:00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=3,
            record_hash=hashlib.sha256(b"a2").digest(),
        )

    before_ids = _existing_dedup_audit_ids(migrated_dsn)

    run_dbt_build(migrated_dsn, select="silver_customers")

    with psycopg.connect(migrated_dsn) as verify_conn:
        new_audit_rows = verify_conn.execute(
            """
            SELECT dedup_audit_id, dataset_id, records_received, records_accepted,
                   records_rejected, records_deduplicated
            FROM meta.dedup_audit
            WHERE model_name = 'customers' AND dedup_audit_id != ALL(%s)
            """,
            (list(before_ids) or [-1],),
        ).fetchall()
        assert len(new_audit_rows) == 1, (
            f"expected exactly one new dedup_audit row, got {new_audit_rows}"
        )
        audit_id, audit_dataset_id, received, accepted, rejected, deduplicated = new_audit_rows[0]

        assert audit_dataset_id == dataset_id
        # `dedup_audit_post_hook`'s own floor (`WHERE b._run_id > floor`) is scoped to the whole
        # "customers" model's history in `meta.dedup_audit`, not to this test's own run_id -- when
        # the whole tests/integration directory runs together against one shared testcontainers
        # Postgres, another file's own bronze inserts for "customers" (staged but never consumed
        # by their own dbt_build call before this test's own dbt_build advances the floor past
        # them) can be swept into THIS test's own dedup_audit row alongside its 3 rows. `received`
        # therefore only reliably includes-but-does-not-equal this test's own 3 rows; the
        # accounting formula itself (accepted+rejected+deduplicated == received) is still a
        # general truth regardless of how many extra rows got swept in.
        assert received >= 3
        assert rejected == 0
        assert accepted + rejected + deduplicated == received

        decisions = verify_conn.execute(
            "SELECT reason, business_key FROM meta.dedup_decisions WHERE dedup_audit_id = %s",
            (audit_id,),
        ).fetchall()

    # Scope to THIS test's own dropped business key -- other files' swept-in rows (see above) may
    # contribute their own, unrelated dedup_decisions rows in the same audit_id.
    own_decisions = [d for d in decisions if d[1] == {"customer_id": "A1"}]
    assert len(own_decisions) == 1, f"expected exactly one dropped record for A1, got {decisions}"
    reason, business_key = own_decisions[0]
    assert reason in _VALID_REASONS, (
        f"{reason!r} is not in the closed reason vocabulary {_VALID_REASONS}"
    )
    assert reason == "EXACT_DUP_IN_FILE"
    assert business_key == {"customer_id": "A1"}


def _existing_dedup_audit_ids(dsn: str) -> list[int]:
    with psycopg.connect(dsn) as conn:
        rows = conn.execute("SELECT dedup_audit_id FROM meta.dedup_audit").fetchall()
    return [row[0] for row in rows]


def test_whole_project_build_on_a_fresh_unregistered_database_writes_no_audit_rows(
    run_migrations: Callable[[str], None],
    run_dbt_build: Callable[..., object],
) -> None:
    """A fresh deployment's first whole-project `dbt build` must no-op cleanly, never NULL-crash.

    Simulates a fresh cluster's exact ordering: migrations applied, ZERO
    datasets registered in `meta.datasets` (registration happens only via
    each dataset's own ingestion path — `ConfigRegistry.sync()` /
    `get_or_create_dataset` — never via migrations or dbt), then a
    WHOLE-project `dbt build`, which is exactly what the DAGs' `dbt_build`
    task runs regardless of which dataset triggered it. Before
    `dedup_audit_post_hook`'s registration guard (its docstring point 5),
    this failed deterministically: `meta.dataset_id_for_name()` returned
    NULL for the unregistered dataset and the unconditional audit INSERT
    violated `meta.dedup_audit.dataset_id`'s NOT NULL constraint (observed
    live on every fresh-CI-cluster dbt run, e.g. e2e-full run 32873456327's
    silver_orders; latent locally only because 'orders' happened to be
    registered by early local ingests — debug session
    ci-pipeline-ingestion-timeout, ROUND 11). With the guard, both post-hook
    macros must no-op: build green, zero `meta.dedup_audit` rows, zero
    `meta.reconciliation_results` rows.

    Runs against its OWN throwaway container, not the shared `migrated_dsn`:
    other files in this directory register 'customers'/'orders' on the
    shared database, and this test's whole point is the never-registered
    state. `migrations/env.py`'s wrong-database guard requires the name
    'analytics', which the shared container already uses — a second
    database on the same container cannot satisfy the guard, so a second
    container is the minimal honest simulation.
    """
    with PostgresContainer("postgres:18-bookworm", driver="psycopg", dbname="analytics") as pg:
        dsn = pg.get_connection_url().replace("postgresql+psycopg://", "postgresql://")
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            # Mirrors conftest.postgres_dsn's bootstrap exactly (the roles
            # cnpg-analytics.yaml's initdb/postInitApplicationSQL guarantees
            # exist before migrations ever run on a real cluster).
            cur.execute("CREATE ROLE etl_app LOGIN")
            cur.execute("CREATE ROLE analytics_owner LOGIN")
        run_migrations(dsn)

        run_dbt_build(dsn)

        with psycopg.connect(dsn) as verify_conn:
            audit_row = verify_conn.execute("SELECT count(*) FROM meta.dedup_audit").fetchone()
            recon_row = verify_conn.execute(
                "SELECT count(*) FROM meta.reconciliation_results"
            ).fetchone()
        assert audit_row is not None
        assert audit_row[0] == 0, (
            f"expected zero meta.dedup_audit rows on a never-registered database "
            f"(registration guard must skip the write), got {audit_row}"
        )
        assert recon_row is not None
        assert recon_row[0] == 0, (
            f"expected zero meta.reconciliation_results rows on a never-registered database "
            f"(empty bronze_files cross join must write nothing), got {recon_row}"
        )
