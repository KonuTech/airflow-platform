"""Integration tests for ``dataplat.load.publish.scd.SCDPublisher`` (Phase 10 plan 04).

Proves this plan's own ``must_haves.truths`` against a real testcontainers
PostgreSQL: Finding F-1's per-key recompute genuinely reads its full ordered
history from ``staging.customers`` (bronze), never the collapsed
``silver.customers``; SCD-07's late-arriving correction lands chronologically
by ``event_ts``, never by arrival order; SCD-09's replay is idempotent;
SCD-06's effective dating always traces to a bronze ``event_ts``, never
``now()``; SCD-11's per-key replace never touches an untouched key; and
SCD-01/02/03's Type-0/1/2 dispatch matches ``recompute_version_chain``'s own
already-unit-tested behavior exactly.

Every ``customer_id`` used below is drawn from a disjoint, high (970000+)
range per scenario -- ``normalized.customers``/``staging.customers``/
``silver.customers`` are single, SESSION-scoped tables shared across the
whole ``tests/integration/`` collection (``conftest.py``'s own documented
convention, echoed in ``test_scd_delete_detection.py``/
``test_publish_ingest.py``), so each test below picks its own disjoint range
rather than relying on any cross-test isolation this suite does not provide.

``SCDPublisher.publish()``'s Step A (DELETE-detection + circuit breaker +
delete-semantics dispatch) is deliberately configured ``delete_semantics=
"ignore"`` with a permissive ``mass_delete_threshold=1.0`` in every context
this file builds -- NOT because Step A is untested (plan 10-03's
``test_scd_delete_detection.py`` already proves ``find_vanished_customer_ids``/
``MassDeleteCircuitBreaker``/``apply_delete_semantics`` standalone, in full),
but because Step A's real behavior actively WRITES to ``normalized.customers``
based on an UNSCOPED-by-dataset snapshot diff (``find_vanished_customer_ids``'s
own module docstring: this is a single-dataset system, so it never filters
by dataset). In this session-shared-table suite, every OTHER test file's own
``is_current`` rows would be reported "vanished" under THIS file's own narrow
``staged_run_ids``, and a non-``"ignore"`` semantics would incorrectly
invalidate/duplicate them. ``delete_semantics="ignore"`` makes Step A's
dispatch a genuine no-op regardless of how many keys it reports as vanished,
and ``mass_delete_threshold=1.0`` guarantees the circuit breaker itself never
raises (the vanished/current ratio can never exceed ``1.0`` by construction,
since vanished keys are always a subset of current ones) -- keeping this
file's own scope to the 7 behaviors the plan actually assigns it (Steps
B/C/D), without corrupting or being corrupted by any sibling test file's own
``normalized.customers`` state.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
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
from dataplat.load.publish.scd import SCDPublisher
from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.models.identity import RunContext
from dataplat.pipeline.protocol import PipelineContext
from dataplat.scd.recompute import BronzeRecord, recompute_version_chain
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration

_VALID_TO_SENTINEL = datetime(9999, 12, 31, tzinfo=UTC)


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Insert a synthetic ``meta.config_versions`` row directly via SQL.

    Duplicated locally rather than imported, matching this test suite's
    existing per-file helper convention (``test_scd_delete_detection.py``'s
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

    Mirrors ``test_scd_delete_detection.py``'s own ``_seed_run`` -- duplicated
    locally rather than imported, per this test suite's own per-file helper
    convention.
    """
    dataset_id = repository.get_or_create_dataset(f"publish_scd_{key_suffix}")
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


def _insert_bronze_customer(  # noqa: PLR0913 -- one keyword per column, mirrors test_scd_delete_detection.py's own helpers
    conn: psycopg.Connection[Any],
    *,
    customer_id: str,
    name: str,
    country: str,
    birth_date: str,
    event_ts: str,
    signup_country: str | None,
    run_id: int,
    file_id: int,
    batch_id: int,
    source_row_number: int,
    record_hash: bytes,
) -> None:
    """Insert one ``staging.customers`` (durable bronze) row directly via SQL."""
    conn.execute(
        """
        INSERT INTO staging.customers (
            customer_id, name, country, birth_date, event_ts, signup_country,
            _run_id, _file_id, _batch_id, _source_row_number,
            _record_hash, _record_hash_version
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
        """,
        (
            customer_id,
            name,
            country,
            birth_date,
            event_ts,
            signup_country,
            run_id,
            file_id,
            batch_id,
            source_row_number,
            record_hash,
        ),
    )


def _insert_silver_customer(  # noqa: PLR0913 -- one keyword per column, mirrors test_scd_delete_detection.py's own helper
    conn: psycopg.Connection[Any],
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
            name,
            country,
            birth_date,
            event_ts,
            run_id,
            file_id,
            batch_id,
            source_row_number,
            hashlib.sha256(f"silver:{customer_id}:{source_row_number}".encode()).digest(),
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
    """A ``PipelineContext`` carrying a real ``scd:`` config block.

    ``delete_semantics="ignore"``/``mass_delete_threshold=1.0`` -- see this
    module's own docstring for why every context this file builds uses
    these exact values.
    """
    return PipelineContext(
        run=RunContext(run_id=0, idempotency_key="publish-scd-test-placeholder"),
        config=DatasetConfig(
            dataset="customers",
            config_schema_version=1,
            source=SourceConfig(
                type="csv",
                bucket="publish-scd-test",
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


def _fetch_versions(migrated_dsn: str, *, customer_id: int) -> list[tuple[Any, ...]]:
    """Read every ``normalized.customers`` version row for ``customer_id``, oldest first."""
    with psycopg.connect(migrated_dsn) as conn:
        return conn.execute(
            """
            SELECT customer_id, name, country, birth_date, signup_country,
                   event_ts, valid_to, is_current
              FROM normalized.customers
             WHERE customer_id = %s
             ORDER BY event_ts
            """,
            (customer_id,),
        ).fetchall()


# ---------------------------------------------------------------------------
# Test 1: basic publish, no prior state
# ---------------------------------------------------------------------------


def test_basic_publish_with_no_prior_state_inserts_one_current_version(
    repository: PostgresMetadataRepository, migrated_dsn: str
) -> None:
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="basic")
    customer_id = 970001

    with psycopg.connect(migrated_dsn) as conn:
        _insert_bronze_customer(
            conn,
            customer_id=str(customer_id),
            name="Basic",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-01-01T00:00:00+00:00",
            signup_country="US",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"basic").digest(),
        )
        conn.commit()

        result = SCDPublisher().publish(
            _make_context(), "silver.customers", conn, staged_run_ids=[run_id]
        )
        conn.commit()

    assert result.outcome == "PUBLISHED"
    assert str(customer_id) in result.published_business_keys

    versions = _fetch_versions(migrated_dsn, customer_id=customer_id)
    assert len(versions) == 1
    (_cid, name, country, _bd, signup_country, event_ts, valid_to, is_current) = versions[0]
    assert name == "Basic"
    assert country == "US"
    assert signup_country == "US"
    assert is_current is True
    assert event_ts == datetime(2026, 1, 1, tzinfo=UTC)
    assert valid_to == _VALID_TO_SENTINEL


# ---------------------------------------------------------------------------
# Test 2: Finding F-1's proof -- reads staging.customers, never silver.customers
# ---------------------------------------------------------------------------


def test_recompute_reads_full_bronze_history_never_the_collapsed_silver_row(
    repository: PostgresMetadataRepository, migrated_dsn: str
) -> None:
    """2 bronze rows (differing country) but silver holds only the collapsed latest-winner row.

    Publish must still produce 2 version rows -- proving the recompute
    source is ``staging.customers``, not the (collapsed) ``source_table``
    argument this call passes as ``"silver.customers"``.
    """
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="finding_f1")
    customer_id = 970002

    with psycopg.connect(migrated_dsn) as conn:
        _insert_bronze_customer(
            conn,
            customer_id=str(customer_id),
            name="F1Customer",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-01-01T00:00:00+00:00",
            signup_country="US",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"f1-first").digest(),
        )
        _insert_bronze_customer(
            conn,
            customer_id=str(customer_id),
            name="F1Customer",
            country="CA",
            birth_date="1990-01-01",
            event_ts="2026-02-01T00:00:00+00:00",
            signup_country="US",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=2,
            record_hash=hashlib.sha256(b"f1-second").digest(),
        )
        # dbt's own delete+insert incremental strategy: silver holds ONLY
        # the collapsed, latest-winner row -- never the full history.
        _insert_silver_customer(
            conn,
            customer_id=str(customer_id),
            name="F1Customer",
            country="CA",
            birth_date="1990-01-01",
            event_ts="2026-02-01T00:00:00+00:00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=2,
        )
        conn.commit()

        SCDPublisher().publish(
            _make_context(), "silver.customers", conn, staged_run_ids=[run_id]
        )
        conn.commit()

    versions = _fetch_versions(migrated_dsn, customer_id=customer_id)
    assert len(versions) == 2
    assert versions[0][2] == "US"  # country
    assert versions[0][7] is False  # is_current
    assert versions[1][2] == "CA"
    assert versions[1][7] is True


# ---------------------------------------------------------------------------
# Test 3: late-arriving correction lands chronologically, not by arrival order (SCD-07)
# ---------------------------------------------------------------------------


def test_late_arriving_correction_lands_between_two_existing_versions_by_event_ts(
    repository: PostgresMetadataRepository, migrated_dsn: str
) -> None:
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="late_correction")
    customer_id = 970003

    with psycopg.connect(migrated_dsn) as conn:
        # day 1 and day 10 share country "US"; day 5 (staged LAST, i.e. the
        # HIGHEST _source_row_number, but an event_ts strictly between day 1
        # and day 10) carries "CA" -- a genuine late-arriving correction.
        _insert_bronze_customer(
            conn,
            customer_id=str(customer_id),
            name="LateCorrection",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-01-01T00:00:00+00:00",
            signup_country="US",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"day1").digest(),
        )
        _insert_bronze_customer(
            conn,
            customer_id=str(customer_id),
            name="LateCorrection",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-01-10T00:00:00+00:00",
            signup_country="US",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=2,
            record_hash=hashlib.sha256(b"day10").digest(),
        )
        _insert_bronze_customer(
            conn,
            customer_id=str(customer_id),
            name="LateCorrection",
            country="CA",
            birth_date="1990-01-01",
            event_ts="2026-01-05T00:00:00+00:00",
            signup_country="US",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=3,  # arrived/staged LAST -- highest source_row_number
            record_hash=hashlib.sha256(b"day5-correction").digest(),
        )
        conn.commit()

        SCDPublisher().publish(
            _make_context(), "silver.customers", conn, staged_run_ids=[run_id]
        )
        conn.commit()

    versions = _fetch_versions(migrated_dsn, customer_id=customer_id)
    assert len(versions) == 3

    countries = [v[2] for v in versions]
    event_timestamps = [v[5] for v in versions]
    assert countries == ["US", "CA", "US"]
    assert event_timestamps == [
        datetime(2026, 1, 1, tzinfo=UTC),
        datetime(2026, 1, 5, tzinfo=UTC),
        datetime(2026, 1, 10, tzinfo=UTC),
    ]
    assert versions[0][6] == datetime(2026, 1, 5, tzinfo=UTC)  # valid_to
    assert versions[1][6] == datetime(2026, 1, 10, tzinfo=UTC)
    assert versions[2][6] == _VALID_TO_SENTINEL
    assert [v[7] for v in versions] == [False, False, True]  # is_current


# ---------------------------------------------------------------------------
# Test 4: idempotent replay (SCD-09)
# ---------------------------------------------------------------------------


def test_replaying_the_identical_batch_twice_produces_exactly_one_logical_version_set(
    repository: PostgresMetadataRepository, migrated_dsn: str
) -> None:
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="idempotent_replay")
    customer_id = 970004

    with psycopg.connect(migrated_dsn) as conn:
        _insert_bronze_customer(
            conn,
            customer_id=str(customer_id),
            name="Replay",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-01-01T00:00:00+00:00",
            signup_country="US",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"replay").digest(),
        )
        conn.commit()

        SCDPublisher().publish(
            _make_context(), "silver.customers", conn, staged_run_ids=[run_id]
        )
        conn.commit()

    first_versions = _fetch_versions(migrated_dsn, customer_id=customer_id)
    assert len(first_versions) == 1

    with psycopg.connect(migrated_dsn) as conn:
        SCDPublisher().publish(
            _make_context(), "silver.customers", conn, staged_run_ids=[run_id]
        )
        conn.commit()

    second_versions = _fetch_versions(migrated_dsn, customer_id=customer_id)
    assert second_versions == first_versions


# ---------------------------------------------------------------------------
# Test 5: effective dating traces to bronze event_ts, never now()/_ingested_at (SCD-06)
# ---------------------------------------------------------------------------


def test_effective_dating_traces_to_the_versions_first_bronze_event_ts(
    repository: PostgresMetadataRepository, migrated_dsn: str
) -> None:
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="effective_dating")
    customer_id = 970005
    first_row_event_ts = "2020-03-14T00:00:00+00:00"
    second_row_event_ts = "2021-07-04T00:00:00+00:00"

    with psycopg.connect(migrated_dsn) as conn:
        _insert_bronze_customer(
            conn,
            customer_id=str(customer_id),
            name="EffectiveDating",
            country="US",
            birth_date="1990-01-01",
            event_ts=first_row_event_ts,
            signup_country="US",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"eff-dating-1").digest(),
        )
        _insert_bronze_customer(
            conn,
            customer_id=str(customer_id),
            name="EffectiveDating",
            country="CA",  # Type-2 change -- opens a new version
            birth_date="1990-01-01",
            event_ts=second_row_event_ts,
            signup_country="US",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=2,
            record_hash=hashlib.sha256(b"eff-dating-2").digest(),
        )
        conn.commit()

        before = datetime.now(tz=UTC)
        SCDPublisher().publish(
            _make_context(), "silver.customers", conn, staged_run_ids=[run_id]
        )
        conn.commit()
        after = datetime.now(tz=UTC)

    versions = _fetch_versions(migrated_dsn, customer_id=customer_id)
    assert len(versions) == 2
    assert versions[0][5] == datetime.fromisoformat(first_row_event_ts)
    assert versions[1][5] == datetime.fromisoformat(second_row_event_ts)
    # Neither valid_from ever falls inside this call's own wall-clock window
    # -- proving neither is now()/_ingested_at.
    for version in versions:
        valid_from = version[5]
        assert not (before <= valid_from <= after)


# ---------------------------------------------------------------------------
# Test 6: backfill-safety -- publishing a new key never touches another key (SCD-11)
# ---------------------------------------------------------------------------


def test_publishing_a_new_touched_key_never_modifies_an_untouched_keys_rows(
    repository: PostgresMetadataRepository, migrated_dsn: str
) -> None:
    run_id_a, file_id_a, batch_id_a = _seed_run(repository, migrated_dsn, key_suffix="backfill_a")
    customer_id_a = 970006
    customer_id_b = 970007

    with psycopg.connect(migrated_dsn) as conn:
        _insert_bronze_customer(
            conn,
            customer_id=str(customer_id_a),
            name="UntouchedA",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-01-01T00:00:00+00:00",
            signup_country="US",
            run_id=run_id_a,
            file_id=file_id_a,
            batch_id=batch_id_a,
            source_row_number=1,
            record_hash=hashlib.sha256(b"backfill-a").digest(),
        )
        conn.commit()

        SCDPublisher().publish(
            _make_context(), "silver.customers", conn, staged_run_ids=[run_id_a]
        )
        conn.commit()

    versions_a_before = _fetch_versions(migrated_dsn, customer_id=customer_id_a)
    assert len(versions_a_before) == 1

    run_id_b, file_id_b, batch_id_b = _seed_run(repository, migrated_dsn, key_suffix="backfill_b")
    with psycopg.connect(migrated_dsn) as conn:
        _insert_bronze_customer(
            conn,
            customer_id=str(customer_id_b),
            name="NewB",
            country="CA",
            birth_date="1990-01-01",
            event_ts="2026-02-01T00:00:00+00:00",
            signup_country="CA",
            run_id=run_id_b,
            file_id=file_id_b,
            batch_id=batch_id_b,
            source_row_number=1,
            record_hash=hashlib.sha256(b"backfill-b").digest(),
        )
        conn.commit()

        # staged_run_ids scoped to run_id_b ONLY -- run_id_a's key must never
        # be touched by this second, unrelated publish pass.
        SCDPublisher().publish(
            _make_context(), "silver.customers", conn, staged_run_ids=[run_id_b]
        )
        conn.commit()

    versions_a_after = _fetch_versions(migrated_dsn, customer_id=customer_id_a)
    versions_b = _fetch_versions(migrated_dsn, customer_id=customer_id_b)
    assert versions_a_after == versions_a_before
    assert len(versions_b) == 1


# ---------------------------------------------------------------------------
# Test 7: Type-0/1/2 end to end, matching recompute_version_chain's own dispatch (SCD-01/02)
# ---------------------------------------------------------------------------


def test_type_0_1_2_dispatch_matches_recompute_version_chains_own_behavior(
    repository: PostgresMetadataRepository, migrated_dsn: str
) -> None:
    run_id, file_id, batch_id = _seed_run(repository, migrated_dsn, key_suffix="type_dispatch")
    customer_id = 970008

    # Each entry is name/country/birth_date/event_ts/signup_country/source_row_number.
    # Row 2's birth_date changes (Type-1, latest-wins globally); country
    # changes (Type-2, opens a new version); signup_country changes too, but
    # Type-0 keeps the EARLIEST value ("US") forever.
    rows = [
        ("Dispatch", "US", "1980-01-01", "2026-01-01T00:00:00+00:00", "US", 1),
        ("Dispatch", "CA", "1990-06-15", "2026-02-01T00:00:00+00:00", "CA", 2),
    ]

    with psycopg.connect(migrated_dsn) as conn:
        for name, country, birth_date, event_ts, signup_country, source_row_number in rows:
            _insert_bronze_customer(
                conn,
                customer_id=str(customer_id),
                name=name,
                country=country,
                birth_date=birth_date,
                event_ts=event_ts,
                signup_country=signup_country,
                run_id=run_id,
                file_id=file_id,
                batch_id=batch_id,
                source_row_number=source_row_number,
                record_hash=hashlib.sha256(f"type-dispatch-{source_row_number}".encode()).digest(),
            )
        conn.commit()

        SCDPublisher().publish(
            _make_context(), "silver.customers", conn, staged_run_ids=[run_id]
        )
        conn.commit()

    published_versions = _fetch_versions(migrated_dsn, customer_id=customer_id)

    # The independent oracle: recompute_version_chain's own already-unit-
    # tested (plan 10-02) dispatch, fed the identical history.
    history = [
        BronzeRecord(
            customer_id=customer_id,
            name=name,
            country=country,
            birth_date=birth_date,
            event_ts=datetime.fromisoformat(event_ts),
            signup_country=signup_country,
            source_row_number=source_row_number,
        )
        for name, country, birth_date, event_ts, signup_country, source_row_number in rows
    ]
    expected_versions = recompute_version_chain(history, valid_to_sentinel=_VALID_TO_SENTINEL)

    assert len(published_versions) == len(expected_versions)
    for published, expected in zip(published_versions, expected_versions, strict=True):
        (
            _cid,
            name,
            country,
            birth_date,
            signup_country,
            event_ts,
            valid_to,
            is_current,
        ) = published
        assert name == expected.name
        assert country == expected.country
        assert str(birth_date) == expected.birth_date
        assert signup_country == expected.signup_country
        assert event_ts == expected.valid_from
        assert valid_to == expected.valid_to
        assert is_current == expected.is_current

    # Type-0 (signup_country): earliest value ("US") kept on EVERY version,
    # never the later incoming "CA".
    assert all(version[4] == "US" for version in published_versions)
    # Type-1 (birth_date): latest value applied uniformly to EVERY version.
    assert all(str(version[3]) == "1990-06-15" for version in published_versions)
    # Type-2 (country): a genuine new version per change.
    assert [version[2] for version in published_versions] == ["US", "CA"]
