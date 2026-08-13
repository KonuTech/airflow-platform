"""Integration tests for ``dataplat.load.publish.merge.MergePublisher`` (LOAD-09, 04-04 Task 2).

Every positive-path test drives a real ``MergePublisher`` against a real
testcontainers PostgreSQL, migrated to head, publishing hand-built staging
tables (raw SQL, independent of ``dataplat.load.staging.StagingLoader``'s
own implementation -- keeping this task's tests self-contained) into
``normalized.customers``.

The concurrency case PITFALLS C1 names (two overlapping publish attempts
against the same dataset) was deliberately NOT tested by 04-04's own tests
above -- 04-RESEARCH.md assigned it to plan 04-06's integration-test suite;
04-04's tests exist only to prove ``MergePublisher``'s SQL would not need to
change for that later test to pass. The tests below `test_atomic_commit`
onward are 04-06's own additions: the concurrency case itself
(`test_advisory_lock_serializes_concurrent_publishers`), plus META-03's
atomic-visibility claim, LOAD-04's lineage-queryability claim, and LOAD-08's
batch-uniqueness claim, all proven against this same real database.
"""

from __future__ import annotations

import hashlib
import json
import threading
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import psycopg
import pytest

from dataplat.load.publish.merge import MergePublisher
from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.models.identity import RunContext
from dataplat.pipeline.protocol import PipelineContext
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    from collections.abc import Iterator

# The shared resource two concurrent publishers serialize over in
# `test_advisory_lock_serializes_concurrent_publishers` -- this phase's
# `MergePublisher` is single-dataset/single-target (its own module
# docstring), so one key naming the target table is sufficient for this
# plan's proof; a later multi-table publisher would need a per-target key,
# which is 04-05/future-work's concern, not this test's.
_ADVISORY_LOCK_KEY = "normalized.customers"

_STAGING_COLUMNS_DDL = """
    customer_id text, name text, country text, birth_date text, event_ts text,
    _run_id bigint, _file_id bigint, _batch_id bigint,
    _source_row_number bigint, _record_hash bytea, _record_hash_version smallint
"""


def _make_context() -> PipelineContext:
    """A fully placeholder ``PipelineContext`` -- ``MergePublisher.publish()`` uses no field on it.

    Mirrors ``tests/unit/test_pipeline_errors.py``'s ``_make_context()``
    convention -- ``MergePublisher``'s target/columns are hardcoded (see its
    module docstring), so not even ``config`` needs a real value here.
    """
    return PipelineContext(
        run=RunContext(run_id=1, idempotency_key="test-run"),
        config=None,  # type: ignore[arg-type] -- unused by MergePublisher.publish()
        metadata=None,  # type: ignore[arg-type] -- unused by MergePublisher.publish()
        objects=None,  # type: ignore[arg-type] -- unused by MergePublisher.publish()
        db=None,  # type: ignore[arg-type] -- unused by MergePublisher.publish()
        log=None,  # type: ignore[arg-type] -- unused by MergePublisher.publish()
    )


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Insert a synthetic `meta.config_versions` row directly via SQL.

    Mirrors `tests/integration/test_metadata_repository.py`'s helper of the
    same name/shape -- duplicated locally rather than imported, matching
    this test suite's existing per-file helper convention.
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
) -> tuple[int, int, int]:
    """Create dataset+config_version+file+batch+RUNNING run; return ``(run_id, file_id, batch_id)``.

    ``normalized.customers._run_id``/``_file_id``/``_batch_id`` are real
    foreign keys (migration 0005) -- unlike the staging table, which carries
    none -- so publish tests need real, FK-satisfying rows to publish
    against.
    """
    dataset_id = repository.get_or_create_dataset(f"merge_test_{key_suffix}")
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
        batch_key=f"{key_suffix}:2026-08-13:1",
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
        f"""
        INSERT INTO {table_name} (
            customer_id, name, country, birth_date, event_ts,
            _run_id, _file_id, _batch_id, _source_row_number,
            _record_hash, _record_hash_version
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
        """,  # noqa: S608 -- test-controlled identifier only; every value crosses via %s
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


def test_publish_inserts_distinct_customer_rows(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="distinct")
    staging_table = "staging.merge_test_distinct"

    with psycopg.connect(migrated_dsn) as conn:
        _create_staging_table(conn, staging_table)
        for i in range(3):
            _insert_staging_row(
                conn,
                staging_table,
                customer_id=str(2000 + i),
                name=f"Name{i}",
                country="US",
                birth_date="1990-01-01",
                event_ts="2026-08-13T10:00:00+00:00",
                run_id=run_id,
                file_id=file_id,
                batch_id=batch_id,
                source_row_number=i + 1,
                record_hash=hashlib.sha256(f"row-{i}".encode()).digest(),
            )
        result = MergePublisher().publish(_make_context(), staging_table, conn)
        conn.commit()

    assert result.outcome == "PUBLISHED"
    assert result.rows_affected == 3

    with psycopg.connect(migrated_dsn) as verify_conn:
        count = verify_conn.execute(
            "SELECT COUNT(*) FROM normalized.customers WHERE _run_id = %s",
            (run_id,),
        ).fetchone()
    assert count is not None
    assert count[0] == 3


def test_publish_deduplicates_same_customer_id_keeping_the_latest_event_ts(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="dedup")
    staging_table = "staging.merge_test_dedup"
    customer_id = "3001"

    with psycopg.connect(migrated_dsn) as conn:
        _create_staging_table(conn, staging_table)
        _insert_staging_row(
            conn,
            staging_table,
            customer_id=customer_id,
            name="Older",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-01-01T00:00:00+00:00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"older").digest(),
        )
        _insert_staging_row(
            conn,
            staging_table,
            customer_id=customer_id,
            name="Newer",
            country="CA",
            birth_date="1990-01-01",
            event_ts="2026-06-01T00:00:00+00:00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=2,
            record_hash=hashlib.sha256(b"newer").digest(),
        )

        # Must not raise "ON CONFLICT DO UPDATE command cannot affect row a
        # second time" (PITFALLS C1) -- the DISTINCT ON in MergePublisher's
        # own SQL is what prevents that.
        result = MergePublisher().publish(_make_context(), staging_table, conn)
        conn.commit()

    assert result.rows_affected == 1

    with psycopg.connect(migrated_dsn) as verify_conn:
        row = verify_conn.execute(
            "SELECT name, country FROM normalized.customers WHERE customer_id = %s",
            (int(customer_id),),
        ).fetchone()
    assert row is not None
    assert row[0] == "Newer"
    assert row[1] == "CA"


def test_republishing_identical_content_is_a_no_op(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="noop_republish")
    customer_id = "4001"
    record_hash = hashlib.sha256(b"identical-content").digest()

    with psycopg.connect(migrated_dsn) as conn:
        first_table = "staging.merge_test_noop_republish_1"
        _create_staging_table(conn, first_table)
        _insert_staging_row(
            conn,
            first_table,
            customer_id=customer_id,
            name="Same",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-08-13T10:00:00+00:00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=record_hash,
        )
        first_result = MergePublisher().publish(_make_context(), first_table, conn)
        conn.commit()
    assert first_result.rows_affected == 1

    with psycopg.connect(migrated_dsn) as conn:
        second_table = "staging.merge_test_noop_republish_2"
        _create_staging_table(conn, second_table)
        _insert_staging_row(
            conn,
            second_table,
            customer_id=customer_id,
            name="Same",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-08-13T10:00:00+00:00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=record_hash,  # identical hash -- the WHERE guard must suppress this write
        )
        second_result = MergePublisher().publish(_make_context(), second_table, conn)
        conn.commit()

    assert second_result.rows_affected == 0

    with psycopg.connect(migrated_dsn) as verify_conn:
        count = verify_conn.execute(
            "SELECT COUNT(*) FROM normalized.customers WHERE customer_id = %s",
            (int(customer_id),),
        ).fetchone()
    assert count is not None
    assert count[0] == 1


def test_older_event_ts_never_clobbers_a_newer_stored_row(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="no_clobber")
    customer_id = "5001"

    with psycopg.connect(migrated_dsn) as conn:
        first_table = "staging.merge_test_no_clobber_1"
        _create_staging_table(conn, first_table)
        _insert_staging_row(
            conn,
            first_table,
            customer_id=customer_id,
            name="Newer",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-06-01T00:00:00+00:00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"newer-content").digest(),
        )
        MergePublisher().publish(_make_context(), first_table, conn)
        conn.commit()

    with psycopg.connect(migrated_dsn) as conn:
        second_table = "staging.merge_test_no_clobber_2"
        _create_staging_table(conn, second_table)
        _insert_staging_row(
            conn,
            second_table,
            customer_id=customer_id,
            name="StaleLateArrival",
            country="ZZ",
            birth_date="1990-01-01",
            event_ts="2026-01-01T00:00:00+00:00",  # OLDER than the already-stored row
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"older-content").digest(),
        )
        result = MergePublisher().publish(_make_context(), second_table, conn)
        conn.commit()

    assert result.rows_affected == 0  # the event_ts guard suppressed the write entirely

    with psycopg.connect(migrated_dsn) as verify_conn:
        row = verify_conn.execute(
            "SELECT name, country FROM normalized.customers WHERE customer_id = %s",
            (int(customer_id),),
        ).fetchone()
    assert row is not None
    assert row[0] == "Newer"
    assert row[1] == "US"


def test_on_conflict_fails_without_the_unique_constraint_migration_0006_adds(
    migrated_dsn: str,
) -> None:
    """Negative case: proves migration 0006's UNIQUE constraint is load-bearing, not decorative.

    Temporarily drops ``uq_customers_customer_id`` (reproducing exactly the
    pre-migration-0006 state migration 0005's own docstring describes) on
    the shared, already-fully-migrated database, asserts the documented
    PostgreSQL failure, then restores the constraint in a ``finally`` block
    -- cheaper than provisioning a second dedicated container for one
    negative assertion, and every other test in this session still sees a
    fully-migrated schema. Safe because ``tests/integration/`` runs
    sequentially, never under ``-n auto`` (pyproject.toml's own
    xdist guidance): no other test can observe the dropped-constraint
    window.
    """
    staging_table = "staging.merge_test_premigration_check"
    with psycopg.connect(migrated_dsn) as conn:
        conn.execute("ALTER TABLE normalized.customers DROP CONSTRAINT uq_customers_customer_id")
        conn.commit()

        try:
            _create_staging_table(conn, staging_table)
            conn.commit()

            with pytest.raises(
                psycopg.errors.ProgrammingError,
                match="no unique or exclusion constraint",
            ):
                MergePublisher().publish(_make_context(), staging_table, conn)
            conn.rollback()
        finally:
            conn.execute(
                "ALTER TABLE normalized.customers "
                "ADD CONSTRAINT uq_customers_customer_id UNIQUE (customer_id)",
            )
            conn.commit()


# --- 04-06 Task 2: atomicity, concurrency, lineage, batch uniqueness ------
#
# The four tests below are 04-06's own contribution -- proving META-03's
# one-transaction claim, LOAD-09's single-writer claim, LOAD-04's
# lineage-queryability claim and LOAD-08's batch-uniqueness claim against
# this same real, migrated Postgres. Every helper above (`_seed_run`,
# `_create_staging_table`, `_insert_staging_row`, `_make_context`,
# `repository`) is reused as-is; no new fixture is introduced.


def _read_publication_state(
    conn: psycopg.Connection[Any],
    *,
    customer_id: int,
    file_id: int,
    batch_id: int,
    run_id: int,
) -> tuple[int, str, str, str]:
    """Read `(normalized.customers row count, file status, batch status, run status)` as one tuple.

    A single helper used from both sides of `test_atomic_commit`'s
    visibility boundary, so the pre-commit and post-commit reads are
    guaranteed to check the exact same four things.
    """
    row_count = conn.execute(
        "SELECT COUNT(*) FROM normalized.customers WHERE customer_id = %s",
        (customer_id,),
    ).fetchone()
    file_status = conn.execute(
        "SELECT status FROM meta.files WHERE file_id = %s",
        (file_id,),
    ).fetchone()
    batch_status = conn.execute(
        "SELECT status FROM meta.batches WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()
    run_status = conn.execute(
        "SELECT status FROM meta.ingestion_runs WHERE run_id = %s",
        (run_id,),
    ).fetchone()
    assert row_count is not None
    assert file_status is not None
    assert batch_status is not None
    assert run_status is not None
    return int(row_count[0]), str(file_status[0]), str(batch_status[0]), str(run_status[0])


def test_atomic_commit(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    """META-03: rows becoming visible and file/batch/run status flips are ONE atomic transition.

    `MergePublisher.publish()` and `MetadataRepository.finalize_publication()`
    both run inside the SAME open transaction, uncommitted -- a second,
    independent connection must see the complete pre-publish state for ALL
    FOUR effects at once, then, after exactly one commit, the complete
    post-publish state for all four at once. Never a partial mix of the two.
    """
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="atomic_commit")
    staging_table = "staging.merge_test_atomic_commit"
    customer_id = 6001

    with psycopg.connect(migrated_dsn) as conn:
        _create_staging_table(conn, staging_table)
        _insert_staging_row(
            conn,
            staging_table,
            customer_id=str(customer_id),
            name="Atomic",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-08-13T10:00:00+00:00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"atomic-commit").digest(),
        )

        MergePublisher().publish(_make_context(), staging_table, conn)
        repository.finalize_publication(
            conn=conn,
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            rows_loaded=1,
            finished_at=datetime.now(tz=UTC),
            report_uri="s3://processed/customers/atomic_commit-report.json",
        )

        # conn's transaction is still open -- a SEPARATE connection must see
        # the pre-publish state for every one of the four effects.
        with psycopg.connect(migrated_dsn) as observer:
            state = _read_publication_state(
                observer,
                customer_id=customer_id,
                file_id=file_id,
                batch_id=batch_id,
                run_id=run_id,
            )
        assert state == (0, "DISCOVERED", "OPEN", "RUNNING")

        conn.commit()

    # Now a fresh connection sees all four effects simultaneously.
    with psycopg.connect(migrated_dsn) as observer:
        state = _read_publication_state(
            observer,
            customer_id=customer_id,
            file_id=file_id,
            batch_id=batch_id,
            run_id=run_id,
        )
    assert state == (1, "PROCESSED", "PUBLISHED", "SUCCEEDED")


def test_advisory_lock_serializes_concurrent_publishers(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    """LOAD-09/PITFALLS C1: two publishers with overlapping `customer_id`s serialize, never race.

    Connection A takes `pg_advisory_xact_lock` and publishes, but does not
    commit. Connection B -- from a background thread -- attempts the exact
    same lock-then-publish sequence (the documented caller contract
    `merge.py`'s own module docstring specifies). B must not return while A
    still holds the lock (checked via a `threading.Event`, never a fixed
    `sleep`); once A commits (releasing the lock), B must complete promptly,
    and the final state must reflect BOTH publishes with no
    `UniqueViolation` or cardinality-violation raised on either side --
    exactly the failure mode literal SQL `MERGE` has under concurrent access
    (`merge.py`'s own module docstring, PostgreSQL BUG #18279) and
    `INSERT ... ON CONFLICT` does not.

    Negative-case check performed once during development (this plan's own
    acceptance criteria): with BOTH `pg_advisory_xact_lock` calls below
    temporarily commented out, this test still PASSED, reproducibly, across
    repeated runs -- it did not flake and did not raise a constraint
    violation. Root cause, confirmed by direct trace of `_PUBLISH_SQL`
    (`merge.py`): `_PUBLISH_SQL` arbitrates on exactly ONE unique index
    (`customer_id`) via a SINGLE `INSERT ... SELECT ... ORDER BY customer_id
    ...` statement, so PostgreSQL's OWN unique-index insert-conflict
    handling already forces connection B to block on the SAME row until
    connection A's transaction resolves -- deterministically, with no
    deadlock possible, because both statements process any overlapping keys
    in the SAME fixed order (`ORDER BY customer_id`), so a crossed-lock-order
    deadlock (the failure mode that WOULD require an external lock to
    prevent) cannot arise. This is a real difference from literal `MERGE`
    (BUG #18279): `INSERT ... ON CONFLICT` was designed to be safe here,
    `MERGE` was not (PITFALLS.md C1) -- so for THIS single-arbiter-index,
    single-statement publisher, `pg_advisory_xact_lock` is not
    independently load-bearing for the specific race this test constructs.
    It is kept here anyway (a) because it is the documented calling
    convention `merge.py` specifies for its caller and this test exercises
    that real contract, not a simplified stand-in, and (b) per PITFALLS.md
    C1's own recommendation, as defense-in-depth that remains load-bearing
    the moment a future change adds a second arbiter index or turns
    publication into more than one statement -- at which point removing it
    would silently reopen the exact race class this test's name promises to
    catch.
    """
    run_id_a, file_id_a, batch_id_a = _seed_run(repository, migrated_dsn, key_suffix="lock_a")
    run_id_b, file_id_b, batch_id_b = _seed_run(repository, migrated_dsn, key_suffix="lock_b")
    customer_id = "7001"
    table_a = "staging.merge_test_lock_a"
    table_b = "staging.merge_test_lock_b"

    conn_a = psycopg.connect(migrated_dsn, autocommit=False)
    conn_b = psycopg.connect(migrated_dsn, autocommit=False)
    try:
        _create_staging_table(conn_a, table_a)
        _insert_staging_row(
            conn_a,
            table_a,
            customer_id=customer_id,
            name="FromA",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-01-01T00:00:00+00:00",
            run_id=run_id_a,
            file_id=file_id_a,
            batch_id=batch_id_a,
            source_row_number=1,
            record_hash=hashlib.sha256(b"lock-a").digest(),
        )
        conn_a.commit()

        _create_staging_table(conn_b, table_b)
        _insert_staging_row(
            conn_b,
            table_b,
            customer_id=customer_id,
            name="FromB",
            country="CA",
            birth_date="1990-01-01",
            event_ts="2026-06-01T00:00:00+00:00",
            run_id=run_id_b,
            file_id=file_id_b,
            batch_id=batch_id_b,
            source_row_number=1,
            record_hash=hashlib.sha256(b"lock-b").digest(),
        )
        conn_b.commit()

        # Connection A: take the lock, publish, but do NOT commit yet.
        conn_a.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (_ADVISORY_LOCK_KEY,))
        MergePublisher().publish(_make_context(), table_a, conn_a)

        b_returned = threading.Event()
        b_errors: list[BaseException] = []

        def _publish_b() -> None:
            try:
                conn_b.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))",
                    (_ADVISORY_LOCK_KEY,),
                )
                MergePublisher().publish(_make_context(), table_b, conn_b)
            except BaseException as exc:  # noqa: BLE001 -- re-raised on the main thread below
                b_errors.append(exc)
            finally:
                b_returned.set()

        thread = threading.Thread(target=_publish_b)
        thread.start()

        # B must not complete while A still holds the lock, uncommitted.
        # This window is NOT a race: A does not commit until AFTER this
        # wait() call returns, so the lock is deterministically still held
        # for this entire timeout, not merely "usually" held.
        still_blocked = not b_returned.wait(timeout=1.0)
        assert still_blocked, "connection B's publish() returned before connection A committed"

        conn_a.commit()

        thread.join(timeout=10.0)
        assert not thread.is_alive(), "connection B's publish() never returned after A committed"
        if b_errors:
            raise b_errors[0]

        conn_b.commit()
    finally:
        conn_a.close()
        conn_b.close()

    with psycopg.connect(migrated_dsn) as verify_conn:
        row = verify_conn.execute(
            "SELECT name, country, _run_id FROM normalized.customers WHERE customer_id = %s",
            (int(customer_id),),
        ).fetchone()
    assert row is not None
    # B is forced (by the lock) to execute strictly after A committed, and
    # B's event_ts (2026-06-01) is newer than A's (2026-01-01) -- so
    # MergePublisher's own event_ts guard lets B's write win. This proves
    # both writes were APPLIED IN SEQUENCE with no error -- not that one
    # silently clobbered the other via an unserialized race.
    assert row[0] == "FromB"
    assert row[1] == "CA"
    assert row[2] == run_id_b


def test_lineage_columns_populated(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    """LOAD-04: "which file, which batch, which run" is answerable by SQL alone.

    Per this phase's own success criterion 4.
    """
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="lineage")
    staging_table = "staging.merge_test_lineage"
    customer_id = "8001"
    record_hash = hashlib.sha256(b"lineage-content").digest()

    with psycopg.connect(migrated_dsn) as conn:
        _create_staging_table(conn, staging_table)
        _insert_staging_row(
            conn,
            staging_table,
            customer_id=customer_id,
            name="Lineage",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-08-13T10:00:00+00:00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=42,
            record_hash=record_hash,
        )
        MergePublisher().publish(_make_context(), staging_table, conn)
        conn.commit()

    with psycopg.connect(migrated_dsn) as verify_conn:
        row = verify_conn.execute(
            """
            SELECT _run_id, _file_id, _batch_id, _source_row_number,
                   _record_hash, _record_hash_version
              FROM normalized.customers WHERE customer_id = %s
            """,
            (int(customer_id),),
        ).fetchone()

    assert row is not None
    (
        lineage_run_id,
        lineage_file_id,
        lineage_batch_id,
        source_row_number,
        stored_hash,
        hash_version,
    ) = row
    assert lineage_run_id == run_id
    assert lineage_file_id == file_id
    assert lineage_batch_id == batch_id
    assert source_row_number == 42
    assert bytes(stored_hash) == record_hash
    assert hash_version == 1


def test_duplicate_batch_key_rejected(repository: PostgresMetadataRepository) -> None:
    """LOAD-08: `uq_batches_dataset_batch_key` (migration 0003) is real, enforced, not decorative.

    `PostgresMetadataRepository.create_batch` does a plain
    ``INSERT ... RETURNING`` (04-01's untouched Protocol) -- never an
    upsert -- so a second call with the identical `(dataset_id, batch_key)`
    lets the underlying `psycopg.errors.UniqueViolation` propagate directly,
    uncaught and unwrapped.
    """
    dataset_id = repository.get_or_create_dataset("merge_test_duplicate_batch_key")

    repository.create_batch(dataset_id=dataset_id, batch_key="same-key", status="OPEN")

    with pytest.raises(psycopg.errors.UniqueViolation):
        repository.create_batch(dataset_id=dataset_id, batch_key="same-key", status="OPEN")
