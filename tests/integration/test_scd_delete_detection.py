"""Integration tests for ``dataplat.scd.delete_detection`` (Phase 10 plan 03).

Proves this plan's own ``must_haves.truths`` against a real testcontainers
PostgreSQL: Finding F-2's run-scoped snapshot diff never misclassifies a
key that is merely absent from silver's whole cumulative history (only one
absent from THIS pass's own staged runs counts as vanished); the
mass-delete circuit breaker raises exactly above threshold and passes
exactly at/below it; and all three ``ScdConfig.delete_semantics`` values
(ignore/invalidate/new_record) act correctly and use the snapshot's own
``event_ts``, never wall-clock time, for effective dating.

Every ``customer_id`` used below is drawn from disjoint, high (900000+)
ranges per scenario -- ``normalized.customers``/``silver.customers`` are
single, SESSION-scoped tables shared across the whole ``tests/integration/``
collection (``conftest.py``'s own documented convention, echoed in
``test_referential_integrity.py``/``test_publish_ingest.py``), so each test
below picks its own disjoint range rather than relying on any cross-test
isolation this suite does not provide.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import psycopg
import pytest

from dataplat.errors import ConfigurationError, QualityThresholdExceeded
from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.models.identity import RunContext
from dataplat.pipeline.protocol import PipelineContext
from dataplat.scd.delete_detection import (
    MassDeleteCircuitBreaker,
    apply_delete_semantics,
    find_vanished_customer_ids,
)
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    from collections.abc import Iterator


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Insert a synthetic ``meta.config_versions`` row directly via SQL.

    Duplicated locally rather than imported, matching this test suite's
    existing per-file helper convention (``test_referential_integrity.py``'s
    own ``_insert_config_version``).
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
) -> tuple[int, int, int]:
    """Create dataset+config_version+file+batch+RUNNING run; return ``(run_id, file_id, batch_id)``.

    Mirrors ``test_referential_integrity.py``'s own ``_seed_run`` -- duplicated
    locally rather than imported, per this test suite's own per-file helper
    convention.
    """
    dataset_id = repository.get_or_create_dataset(f"scd_delete_detection_{key_suffix}")
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
        batch_key=f"{key_suffix}:2026-08-21:1",
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


def _insert_silver_customer(  # noqa: PLR0913 -- one keyword per column, mirrors test_publish_ingest.py's own helper
    conn: psycopg.Connection[Any],
    *,
    customer_id: str,
    run_id: int,
    file_id: int,
    batch_id: int,
    source_row_number: int,
) -> None:
    """Insert one ``silver.customers`` row directly via SQL -- never via a real ``dbt build``."""
    conn.execute(
        """
        INSERT INTO silver.customers (
            customer_id, name, country, birth_date, event_ts,
            _run_id, _file_id, _batch_id, _source_row_number,
            _record_hash, _record_hash_version
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
        """,
        (
            customer_id,
            f"silver-customer-{customer_id}",
            "US",
            "1990-01-01",
            "2026-08-21T00:00:00+00:00",
            run_id,
            file_id,
            batch_id,
            source_row_number,
            hashlib.sha256(f"silver:{customer_id}".encode()).digest(),
        ),
    )


def _insert_staging_customer(  # noqa: PLR0913 -- one keyword per column, mirrors _insert_silver_customer's own shape
    conn: psycopg.Connection[Any],
    *,
    customer_id: str,
    run_id: int,
    file_id: int,
    batch_id: int,
    source_row_number: int,
) -> None:
    """Insert one ``staging.customers`` (bronze) row directly via SQL -- never via a real ``stage``.

    10-07-PLAN.md Task 1 (Rule 4, user-approved live finding): ``find_vanished_customer_ids`` is
    now scoped to ``customer_id``s that have EVER appeared in ``staging.customers`` -- every test
    below asserting a customer_id vanishes (or does not) must seed a matching bronze row here, or
    that key is correctly excluded from consideration entirely (see
    ``test_pre_bronze_legacy_rows_are_never_reported_vanished`` for the dedicated regression proof
    of that exclusion itself).
    """
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
            f"bronze-customer-{customer_id}",
            "US",
            "1990-01-01",
            "2026-08-21T00:00:00+00:00",
            run_id,
            file_id,
            batch_id,
            source_row_number,
            hashlib.sha256(f"bronze:{customer_id}".encode()).digest(),
        ),
    )


def _insert_normalized_customer(  # noqa: PLR0913 -- one keyword per column, mirrors test_referential_integrity.py's own helper
    conn: psycopg.Connection[Any],
    *,
    customer_id: int,
    run_id: int,
    file_id: int,
    batch_id: int,
    source_row_number: int,
    is_current: bool = True,
    event_ts: datetime = datetime(2020, 1, 1, tzinfo=UTC),
) -> None:
    """Insert one real ``normalized.customers`` row directly via SQL, satisfying its FK columns.

    ``event_ts`` defaults to a fixed, deliberately-far-past instant (never
    ``now()``) -- the exclusion constraint's generated ``validity`` column
    (``tstzrange(event_ts, valid_to, '[)')``) requires ``event_ts <=
    valid_to``, so tests that later close this row via
    ``apply_delete_semantics`` need a ``snapshot_max_event_ts`` they control
    to be safely, deterministically AFTER this row's own ``event_ts`` --
    tying this to the real wall clock (``now()``) would make that ordering
    depend on exactly when the test happens to run.
    """
    conn.execute(
        """
        INSERT INTO normalized.customers (
            customer_id, name, country, birth_date, event_ts, is_current,
            _run_id, _file_id, _batch_id, _source_row_number,
            _record_hash, _record_hash_version
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, 1
        )
        """,
        (
            customer_id,
            f"customer-{customer_id}",
            "US",
            "1990-01-01",
            event_ts,
            is_current,
            run_id,
            file_id,
            batch_id,
            source_row_number,
            hashlib.sha256(f"customer:{customer_id}".encode()).digest(),
        ),
    )


@pytest.fixture
def repository(migrated_dsn: str) -> Iterator[PostgresMetadataRepository]:
    """A ``PostgresMetadataRepository`` backed by an opened pool over the migrated database."""
    pool = create_pool(migrated_dsn)
    pool.open(wait=True)
    try:
        yield PostgresMetadataRepository(pool)
    finally:
        pool.close()


def _make_context() -> PipelineContext:
    """A placeholder ``PipelineContext`` -- ``MassDeleteCircuitBreaker.apply()`` never reads it."""
    return PipelineContext(
        run=RunContext(run_id=1, idempotency_key="test-run"),
        config=None,  # type: ignore[arg-type] -- unused by MassDeleteCircuitBreaker.apply()
        metadata=None,  # type: ignore[arg-type] -- unused by MassDeleteCircuitBreaker.apply()
        objects=None,  # type: ignore[arg-type] -- unused by MassDeleteCircuitBreaker.apply()
        db=None,  # type: ignore[arg-type] -- unused by MassDeleteCircuitBreaker.apply()
        log=None,  # type: ignore[arg-type] -- unused by MassDeleteCircuitBreaker.apply()
    )


# ---------------------------------------------------------------------------
# Task 1: find_vanished_customer_ids -- run-scoped snapshot diff (Finding F-2)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_a_real_vanished_customer_id_is_correctly_detected(
    repository: PostgresMetadataRepository, migrated_dsn: str
) -> None:
    """3 gold-current customers, 2 confirmed by this pass's silver snapshot -> the 3rd vanished.

    All 3 also get a ``staging.customers`` (bronze) row -- 10-07-PLAN.md Task 1's own
    ``bronze_known`` scoping fix means a customer_id with no bronze presence is excluded from
    vanished-detection entirely, regardless of its silver/gold state (see
    ``test_pre_bronze_legacy_rows_are_never_reported_vanished`` for that exclusion's own proof).
    """
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="real_vanish")

    with psycopg.connect(migrated_dsn) as conn:
        _insert_normalized_customer(
            conn, customer_id=900001, run_id=run_id, file_id=file_id, batch_id=batch_id,
            source_row_number=1,
        )
        _insert_normalized_customer(
            conn, customer_id=900002, run_id=run_id, file_id=file_id, batch_id=batch_id,
            source_row_number=2,
        )
        _insert_normalized_customer(
            conn, customer_id=900003, run_id=run_id, file_id=file_id, batch_id=batch_id,
            source_row_number=3,
        )
        _insert_staging_customer(
            conn, customer_id="900001", run_id=run_id, file_id=file_id, batch_id=batch_id,
            source_row_number=1,
        )
        _insert_staging_customer(
            conn, customer_id="900002", run_id=run_id, file_id=file_id, batch_id=batch_id,
            source_row_number=2,
        )
        _insert_staging_customer(
            conn, customer_id="900003", run_id=run_id, file_id=file_id, batch_id=batch_id,
            source_row_number=3,
        )
        _insert_silver_customer(
            conn, customer_id="900001", run_id=run_id, file_id=file_id, batch_id=batch_id,
            source_row_number=1,
        )
        _insert_silver_customer(
            conn, customer_id="900002", run_id=run_id, file_id=file_id, batch_id=batch_id,
            source_row_number=2,
        )
        conn.commit()

        vanished = find_vanished_customer_ids(conn, staged_run_ids=[run_id])

    # `find_vanished_customer_ids` is deliberately unscoped by dataset
    # (single-dataset system, module docstring) and `normalized.customers`
    # is a session-shared table (this file's own module docstring) -- so
    # membership, not exact-set equality, is what this test can safely
    # assert without depending on isolation this suite does not provide.
    assert "900003" in vanished
    assert "900001" not in vanished
    assert "900002" not in vanished


@pytest.mark.integration
def test_unscoped_silver_rows_never_count_as_present_for_vanished_detection(
    repository: PostgresMetadataRepository, migrated_dsn: str
) -> None:
    """F-2's regression guard: a silver row tagged with an OLDER, un-staged run still vanishes.

    All 3 also get a ``staging.customers`` (bronze) row -- 10-07-PLAN.md Task 1's own
    ``bronze_known`` scoping fix means a customer_id with no bronze presence is excluded from
    vanished-detection entirely (see ``test_pre_bronze_legacy_rows_are_never_reported_vanished``).
    """
    old_run_id, old_file_id, old_batch_id = _seed_run(
        repository, migrated_dsn, key_suffix="unscoped_old"
    )
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="unscoped_new")

    with psycopg.connect(migrated_dsn) as conn:
        _insert_normalized_customer(
            conn, customer_id=900011, run_id=run_id, file_id=file_id, batch_id=batch_id,
            source_row_number=1,
        )
        _insert_normalized_customer(
            conn, customer_id=900012, run_id=run_id, file_id=file_id, batch_id=batch_id,
            source_row_number=2,
        )
        _insert_normalized_customer(
            conn, customer_id=900013, run_id=run_id, file_id=file_id, batch_id=batch_id,
            source_row_number=3,
        )
        _insert_staging_customer(
            conn, customer_id="900011", run_id=run_id, file_id=file_id, batch_id=batch_id,
            source_row_number=1,
        )
        _insert_staging_customer(
            conn, customer_id="900012", run_id=run_id, file_id=file_id, batch_id=batch_id,
            source_row_number=2,
        )
        _insert_staging_customer(
            conn, customer_id="900013", run_id=run_id, file_id=file_id, batch_id=batch_id,
            source_row_number=3,
        )
        _insert_silver_customer(
            conn, customer_id="900011", run_id=run_id, file_id=file_id, batch_id=batch_id,
            source_row_number=1,
        )
        _insert_silver_customer(
            conn, customer_id="900012", run_id=run_id, file_id=file_id, batch_id=batch_id,
            source_row_number=2,
        )
        # 900013 IS present in silver, but tagged with an older, un-staged
        # run -- an unscoped read would (wrongly) see it as "still present".
        _insert_silver_customer(
            conn, customer_id="900013", run_id=old_run_id, file_id=old_file_id,
            batch_id=old_batch_id, source_row_number=1,
        )
        conn.commit()

        vanished = find_vanished_customer_ids(conn, staged_run_ids=[run_id])

    assert "900013" in vanished
    assert "900011" not in vanished
    assert "900012" not in vanished


@pytest.mark.integration
def test_pre_bronze_legacy_rows_are_never_reported_vanished(
    repository: PostgresMetadataRepository, migrated_dsn: str
) -> None:
    """10-07-PLAN.md Task 1 (Rule 4, user-approved live finding).

    Live-discovered against the real kind cluster: ``normalized.customers`` has accumulated
    12,001,043 ``is_current=true`` rows, the overwhelming majority inserted by Phase 4's original
    vertical-slice proof (``MergePublisher``, weeks before ``staging.customers``/``silver.
    customers`` existed at all). Those rows can NEVER appear in ANY ``staged_snapshot`` (they never
    went through the bronze pipeline), so an unscoped ``find_vanished_customer_ids`` reported ALL
    of them vanished on every single call -- tripping ``MassDeleteCircuitBreaker`` permanently, not
    because anything was mass-deleted, but because the denominator/numerator both included keys
    this mechanism was never designed to reason about.

    This customer_id (900041) is ``is_current=true`` in ``normalized.customers`` -- exactly like a
    Phase-4-era legacy row -- but has NO corresponding ``staging.customers`` row at all (never
    seeded here, deliberately). It must NEVER be reported vanished, for ANY ``staged_run_ids``,
    since it was never observed by the bronze-fed SCD pipeline in the first place.
    """
    run_id, _file_id, _batch_id = _seed_run(repository, migrated_dsn, key_suffix="pre_bronze")

    with psycopg.connect(migrated_dsn) as conn:
        # Deliberately uses a DIFFERENT run's identity for this row's own lineage columns --
        # mirrors a pre-bronze-era row's real shape (inserted by an entirely different,
        # now-defunct write path) more faithfully than reusing `run_id`, and proves the
        # exclusion holds regardless of which run_id the legacy row happens to carry.
        legacy_run_id, legacy_file_id, legacy_batch_id = _seed_run(
            repository, migrated_dsn, key_suffix="pre_bronze_legacy_writer"
        )
        _insert_normalized_customer(
            conn, customer_id=900041, run_id=legacy_run_id, file_id=legacy_file_id,
            batch_id=legacy_batch_id, source_row_number=1,
        )
        # No _insert_staging_customer call for 900041 -- this IS the scenario: a gold-current
        # row with zero bronze presence, exactly like Phase 4's own legacy data.
        conn.commit()

        vanished = find_vanished_customer_ids(conn, staged_run_ids=[run_id])

    assert "900041" not in vanished, (
        "a normalized.customers row with NO corresponding staging.customers (bronze) row must "
        "never be reported vanished -- it was never observed by the bronze-fed SCD pipeline at "
        "all, exactly like Phase 4's own pre-bronze legacy data (the live-discovered bug this "
        "test guards against)"
    )


@pytest.mark.integration
def test_nothing_vanished_returns_empty_set(
    repository: PostgresMetadataRepository, migrated_dsn: str
) -> None:
    """Every gold-current key confirmed by this pass's silver snapshot -> never reported vanished.

    Asserts non-membership rather than whole-set emptiness -- see this
    file's own module docstring on why exact-set equality is unsafe here.
    """
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="nothing_vanished")

    with psycopg.connect(migrated_dsn) as conn:
        _insert_normalized_customer(
            conn, customer_id=900021, run_id=run_id, file_id=file_id, batch_id=batch_id,
            source_row_number=1,
        )
        _insert_normalized_customer(
            conn, customer_id=900022, run_id=run_id, file_id=file_id, batch_id=batch_id,
            source_row_number=2,
        )
        _insert_silver_customer(
            conn, customer_id="900021", run_id=run_id, file_id=file_id, batch_id=batch_id,
            source_row_number=1,
        )
        _insert_silver_customer(
            conn, customer_id="900022", run_id=run_id, file_id=file_id, batch_id=batch_id,
            source_row_number=2,
        )
        conn.commit()

        vanished = find_vanished_customer_ids(conn, staged_run_ids=[run_id])

    assert "900021" not in vanished
    assert "900022" not in vanished


@pytest.mark.integration
def test_empty_gold_returns_no_vanished_customer_ids_without_error(
    repository: PostgresMetadataRepository, migrated_dsn: str
) -> None:
    """Zero gold-current rows at all -> empty set, no error (nothing can vanish from nothing).

    Proven inside a transaction that is always rolled back -- ``normalized.
    customers`` is a single, session-shared table (module docstring), so
    this test must never permanently delete other tests' rows.
    """
    run_id, _file_id, _batch_id = _seed_run(repository, migrated_dsn, key_suffix="empty_gold")

    conn = psycopg.connect(migrated_dsn)
    try:
        conn.execute("DELETE FROM normalized.customers WHERE is_current")
        vanished = find_vanished_customer_ids(conn, staged_run_ids=[run_id])
        assert vanished == set()
    finally:
        conn.rollback()
        conn.close()


# ---------------------------------------------------------------------------
# Task 2: MassDeleteCircuitBreaker and delete-semantics dispatch (D-05/D-06)
# ---------------------------------------------------------------------------


def test_mass_delete_breach_raises_quality_threshold_exceeded_with_ratio_and_threshold() -> None:
    breaker = MassDeleteCircuitBreaker(threshold=0.10, current_count=100, vanished_count=15)

    with pytest.raises(QualityThresholdExceeded) as exc_info:
        breaker.apply(_make_context())

    assert exc_info.value.context["observed_ratio"] == pytest.approx(0.15)
    assert exc_info.value.context["threshold"] == 0.10


def test_mass_delete_within_threshold_passes() -> None:
    breaker = MassDeleteCircuitBreaker(threshold=0.10, current_count=100, vanished_count=5)

    result = breaker.apply(_make_context())

    assert len(result.findings) == 1
    finding = result.findings[0]
    assert finding.outcome == "PASS"
    assert finding.evaluated_count == 100
    assert finding.failed_count == 5


def test_mass_delete_empty_gold_is_a_trivial_pass_regardless_of_vanished_count() -> None:
    breaker = MassDeleteCircuitBreaker(threshold=0.10, current_count=0, vanished_count=999)

    result = breaker.apply(_make_context())

    assert result.findings[0].outcome == "PASS"


def test_mass_delete_exactly_at_threshold_does_not_breach() -> None:
    breaker = MassDeleteCircuitBreaker(threshold=0.10, current_count=100, vanished_count=10)

    result = breaker.apply(_make_context())

    assert result.findings[0].outcome == "PASS"


@pytest.mark.integration
def test_delete_semantics_ignore_is_a_no_op_and_writes_nothing(migrated_dsn: str) -> None:
    with psycopg.connect(migrated_dsn) as conn:
        acted_on = apply_delete_semantics(
            conn,
            delete_semantics="ignore",
            vanished_ids={"900201", "900202"},
            snapshot_max_event_ts=datetime(2026, 8, 21, tzinfo=UTC),
        )

    assert acted_on == ()


@pytest.mark.integration
def test_delete_semantics_invalidate_closes_the_current_row_at_the_snapshot_event_ts(
    repository: PostgresMetadataRepository, migrated_dsn: str
) -> None:
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="invalidate")
    # after _insert_normalized_customer's own fixed event_ts (2020-01-01)
    snapshot_ts = datetime(2020, 6, 1, 12, 0, 0, tzinfo=UTC)

    with psycopg.connect(migrated_dsn) as conn:
        _insert_normalized_customer(
            conn, customer_id=900211, run_id=run_id, file_id=file_id, batch_id=batch_id,
            source_row_number=1,
        )
        _insert_normalized_customer(
            conn, customer_id=900212, run_id=run_id, file_id=file_id, batch_id=batch_id,
            source_row_number=2,
        )
        conn.commit()

        acted_on = apply_delete_semantics(
            conn,
            delete_semantics="invalidate",
            vanished_ids={"900211"},
            snapshot_max_event_ts=snapshot_ts,
        )
        conn.commit()

    assert acted_on == ("900211",)

    with psycopg.connect(migrated_dsn) as verify_conn:
        closed_row = verify_conn.execute(
            "SELECT is_current, valid_to FROM normalized.customers WHERE customer_id = %s",
            (900211,),
        ).fetchone()
        untouched_row = verify_conn.execute(
            "SELECT is_current FROM normalized.customers WHERE customer_id = %s",
            (900212,),
        ).fetchone()

    assert closed_row is not None
    assert closed_row[0] is False
    assert closed_row[1] == snapshot_ts

    assert untouched_row is not None
    assert untouched_row[0] is True


@pytest.mark.integration
def test_delete_semantics_new_record_opens_a_new_current_version_and_closes_the_old_one(
    repository: PostgresMetadataRepository, migrated_dsn: str
) -> None:
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="new_record")
    # after _insert_normalized_customer's own fixed event_ts (2020-01-01)
    snapshot_ts = datetime(2020, 6, 1, 12, 0, 0, tzinfo=UTC)

    with psycopg.connect(migrated_dsn) as conn:
        _insert_normalized_customer(
            conn, customer_id=900221, run_id=run_id, file_id=file_id, batch_id=batch_id,
            source_row_number=1,
        )
        conn.commit()

        old_row = conn.execute(
            "SELECT id FROM normalized.customers WHERE customer_id = %s AND is_current",
            (900221,),
        ).fetchone()
        assert old_row is not None
        old_id = old_row[0]

        acted_on = apply_delete_semantics(
            conn,
            delete_semantics="new_record",
            vanished_ids={"900221"},
            snapshot_max_event_ts=snapshot_ts,
        )
        conn.commit()

    assert acted_on == ("900221",)

    with psycopg.connect(migrated_dsn) as verify_conn:
        rows = verify_conn.execute(
            "SELECT id, is_current, valid_to, event_ts FROM normalized.customers "
            "WHERE customer_id = %s ORDER BY id",
            (900221,),
        ).fetchall()

    assert len(rows) == 2
    current_rows = [row for row in rows if row[1] is True]
    assert len(current_rows) == 1

    old_row_after = next(row for row in rows if row[0] == old_id)
    new_row_after = next(row for row in rows if row[0] != old_id)

    assert old_row_after[1] is False  # is_current = false
    assert old_row_after[2] == snapshot_ts  # valid_to = snapshot_max_event_ts

    assert new_row_after[1] is True  # is_current = true
    # event_ts (this row's valid_from) = snapshot_max_event_ts
    assert new_row_after[3] == snapshot_ts


@pytest.mark.integration
def test_delete_semantics_out_of_vocabulary_value_raises_configuration_error_before_any_write(
    migrated_dsn: str,
) -> None:
    with psycopg.connect(migrated_dsn) as conn, pytest.raises(ConfigurationError) as exc_info:
        apply_delete_semantics(
            conn,
            delete_semantics="delete_forever",  # not in {ignore, invalidate, new_record}
            vanished_ids={"900231"},
            snapshot_max_event_ts=datetime(2026, 8, 21, tzinfo=UTC),
        )

    assert exc_info.value.context["delete_semantics"] == "delete_forever"
