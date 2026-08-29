"""SQL-layer integration proof for the ``rebuild-scd2-reconciliation`` fix (commit a0cc2f5).

Closes the debug session ``.planning/debug/rebuild-scd2-reconciliation.md`` after six
consecutive live-CI verification attempts each failed for a DIFFERENT infrastructure/
orchestration reason (never once implicating ``recompute.py``/``scd.py``'s own logic --
see that file's Resolution section). This module is the decided replacement for a seventh
live-CI dispatch: a self-contained testcontainers-Postgres reproduction of the ORIGINAL
failure shape that exercises the REAL production code path end to end.

What "real code path" means here, precisely: unlike
``tests/unit/test_scd_recompute.py::test_cross_file_event_ts_and_source_row_number_tie_is_order_independent``
(which calls ``recompute_version_chain`` directly with a hand-built Python list, proving the
pure function is order-independent given a list already in memory), every test below drives
``dataplat.load.publish.scd.SCDPublisher.publish()`` -- the actual production entry point --
against real, hand-seeded ``staging.customers`` rows in a real PostgreSQL instance. That means
the REAL ``_BRONZE_HISTORY_SQL`` (no ``ORDER BY``, by design) is the thing that actually reads
these rows back, and Postgres' own (unspecified, implementation-dependent) row-return order for
an unordered ``SELECT`` is what the fix's ``(event_ts, file_id, source_row_number)`` sort key
must tame -- not just a Python list literal's order.

Bug shape reproduced (debug file's Symptoms/Evidence): a single ``customer_id`` has two bronze
rows contributed by TWO DIFFERENT source files that happen to share both ``event_ts`` AND
``_source_row_number`` (the row's ordinal position WITHIN its own file, per
``models/record.py``'s docstring -- not a cross-file-unique value) but carry DIFFERING business
content. Before the fix, the tie was broken by whatever arbitrary order the un-ordered SQL
query happened to return; after the fix, ``file_id`` (globally unique, assigned in
``discover_files``'s own deterministic sorted-manifest order) makes the tie-break total and
stable regardless of physical row order.

Every ``customer_id`` used below is drawn from a disjoint range (976001-976003) -- see this
suite's own established per-file-disjoint-range convention (``test_publish_scd.py``'s
970000s, ``test_scd_delete_detection.py``'s 900000s, ``test_publish_quarantine_exclusion.py``'s
973000s, ``test_rebuild_reconciliation.py``'s 9704000s, ``test_reconciliation.py``'s 9995000s)
-- ``normalized.customers``/``staging.customers`` are single, SESSION-scoped tables shared
across the whole ``tests/integration/`` collection.

Helpers (``_insert_config_version``, ``_seed_run``, ``_insert_bronze_customer``,
``_make_context``, ``_fetch_versions``) are duplicated locally rather than imported, mirroring
``test_publish_scd.py``'s/``test_scd_delete_detection.py``'s own established per-file-helper
convention for this suite.
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
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

pytestmark = pytest.mark.integration

_VALID_TO_SENTINEL = datetime(9999, 12, 31, tzinfo=UTC)

# The two literal event_ts values used throughout this file: an unambiguous baseline
# observation, and the LATER, genuinely-tied event_ts two different files both deliver for
# the same customer at the same in-file row position (source_row_number=30 in both).
_BASELINE_EVENT_TS = "2026-01-01T00:00:00+00:00"
_TIED_EVENT_TS = "2026-01-10T00:00:00+00:00"
_TIED_SOURCE_ROW_NUMBER = 30

# Business content literals for the two tied files -- shared verbatim across both sub-cases
# (A and B below) so their published output can be compared directly for equality.
_LOW_FILE_NAME, _LOW_FILE_COUNTRY = "LegacyBranch", "FR"
_HIGH_FILE_NAME, _HIGH_FILE_COUNTRY = "CorrectedBranch", "DE"


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Insert a synthetic ``meta.config_versions`` row directly via SQL.

    Duplicated from ``test_publish_scd.py``'s own helper of the same name, per this suite's
    established per-file-helper convention.
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

    Mirrors ``test_publish_scd.py``'s/``test_scd_delete_detection.py``'s own ``_seed_run``.
    ``file_id`` is a session-wide identity column -- calling this repeatedly, in a controlled
    order, is exactly how this file manufactures two files with a KNOWN, strictly-increasing
    ``file_id`` relationship (the ``low``/``high`` naming below refers to this numeric order).
    """
    dataset_id = repository.get_or_create_dataset(f"scd2_tie_determinism_{key_suffix}")
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
        batch_key=f"{key_suffix}:2026-08-30:1",
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


def _insert_bronze_customer(  # noqa: PLR0913 -- one keyword per column, mirrors test_publish_scd.py's own helper
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

    ``delete_semantics="ignore"``/``mass_delete_threshold=1.0`` -- identical to
    ``test_publish_scd.py``'s own ``_make_context``, for the identical reason (see that
    module's own docstring): this session-shared-table suite must not let Step A's
    DELETE-detection treat every OTHER test file's own ``is_current`` rows as "vanished".
    """
    return PipelineContext(
        run=RunContext(run_id=0, idempotency_key="scd2-tie-determinism-test-placeholder"),
        config=DatasetConfig(
            dataset="customers",
            config_schema_version=1,
            source=SourceConfig(
                type="csv",
                bucket="scd2-tie-determinism-test",
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


def _seed_cross_file_tie(  # noqa: PLR0913 -- one keyword per seeding concern, mirrors this suite's own convention
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
    conn: psycopg.Connection[Any],
    *,
    customer_id: int,
    key_prefix: str,
    physical_insert_order: str,
) -> list[int]:
    """Seed one baseline row + the genuine cross-file tie, in a CONTROLLED physical order.

    Creates three files (baseline, low, high -- ``low``/``high`` naming refers to the
    resulting ``file_id`` numeric relationship, guaranteed by call order) and physically
    ``INSERT``s their bronze rows into ``staging.customers`` in the order
    ``physical_insert_order`` dictates:

    * ``"ascending"``: baseline, then LOW-file_id row, then HIGH-file_id row -- physical
      insertion order matches file_id order.
    * ``"descending"``: baseline, then HIGH-file_id row, then LOW-file_id row -- physical
      insertion order is the REVERSE of file_id order.

    This is what makes the two sub-cases below a genuine test of Postgres' own real,
    unordered-read behavior: the SAME logical tie (same two files, same content, same
    event_ts/source_row_number) is delivered to a real heap in two different physical
    orders, exactly mirroring how an original incrementally-loaded run and a from-scratch
    rebuild-from-raw bulk reload can physically stage the SAME bronze rows in DIFFERENT
    orders (the debug file's own root_cause).

    Returns:
        ``staged_run_ids`` -- every run_id touched, for the caller's ``publish()`` call.
    """
    run_baseline, file_baseline, batch_baseline = _seed_run(
        repository, migrated_dsn, key_suffix=f"{key_prefix}_baseline"
    )
    run_low, file_low, batch_low = _seed_run(
        repository, migrated_dsn, key_suffix=f"{key_prefix}_filelow"
    )
    run_high, file_high, batch_high = _seed_run(
        repository, migrated_dsn, key_suffix=f"{key_prefix}_filehigh"
    )
    assert file_high > file_low, (
        "test setup invariant: the 'high' file must be created (and so identity-assigned a "
        "numerically higher file_id) AFTER the 'low' file -- otherwise this test's own "
        "'ascending'/'descending' physical-order framing is meaningless"
    )

    _insert_bronze_customer(
        conn,
        customer_id=str(customer_id),
        name="Baseline",
        country="US",
        birth_date="1985-05-05",
        event_ts=_BASELINE_EVENT_TS,
        signup_country="US",
        run_id=run_baseline,
        file_id=file_baseline,
        batch_id=batch_baseline,
        source_row_number=1,
        record_hash=hashlib.sha256(f"{key_prefix}-baseline".encode()).digest(),
    )

    def _insert_low_row() -> None:
        _insert_bronze_customer(
            conn,
            customer_id=str(customer_id),
            name=_LOW_FILE_NAME,
            country=_LOW_FILE_COUNTRY,
            birth_date="1985-05-05",
            event_ts=_TIED_EVENT_TS,
            signup_country="US",
            run_id=run_low,
            file_id=file_low,
            batch_id=batch_low,
            source_row_number=_TIED_SOURCE_ROW_NUMBER,
            record_hash=hashlib.sha256(f"{key_prefix}-filelow".encode()).digest(),
        )

    def _insert_high_row() -> None:
        _insert_bronze_customer(
            conn,
            customer_id=str(customer_id),
            name=_HIGH_FILE_NAME,
            country=_HIGH_FILE_COUNTRY,
            birth_date="1985-05-05",
            event_ts=_TIED_EVENT_TS,
            signup_country="US",
            run_id=run_high,
            file_id=file_high,
            batch_id=batch_high,
            source_row_number=_TIED_SOURCE_ROW_NUMBER,
            record_hash=hashlib.sha256(f"{key_prefix}-filehigh".encode()).digest(),
        )

    if physical_insert_order == "ascending":
        _insert_low_row()
        _insert_high_row()
    elif physical_insert_order == "descending":
        _insert_high_row()
        _insert_low_row()
    else:  # pragma: no cover -- defensive, test-internal only
        msg = f"unknown physical_insert_order: {physical_insert_order!r}"
        raise ValueError(msg)

    return [run_baseline, run_low, run_high]


# ---------------------------------------------------------------------------
# Test 1: the REAL SCDPublisher path is deterministic regardless of physical
# insertion order into Postgres (the fix's own core claim, at the SQL layer).
# ---------------------------------------------------------------------------


def test_real_sql_path_cross_file_tie_resolves_identically_regardless_of_insertion_order(
    repository: PostgresMetadataRepository, migrated_dsn: str
) -> None:
    """Reproduces the exact bug shape and drives the REAL SCDPublisher/_BRONZE_HISTORY_SQL path.

    Sub-case A (customer_id=976001): the tied rows are physically INSERTed in file_id-ASCENDING
    order (low file first, high file second) -- the "natural" order.
    Sub-case B (customer_id=976002): the SAME logical tie, but physically INSERTed in
    file_id-DESCENDING order (high file first, low file second) -- the REVERSED order.

    Both sub-cases must publish an IDENTICAL version chain (business content + temporal
    boundaries, ignoring only the differing customer_id), and the winning "current" version
    must carry the HIGH file's content in BOTH cases -- proving ``_BRONZE_HISTORY_SQL``'s
    genuinely un-ordered real-Postgres read, combined with the fix's
    ``(event_ts, file_id, source_row_number)`` sort key, produces a result that does not
    depend on the physical order rows were staged in. This is the SQL-layer analogue of
    README §67 determinism the debug file's root_cause identifies as broken pre-fix.
    """
    customer_id_a = 976001
    customer_id_b = 976002

    with psycopg.connect(migrated_dsn) as conn:
        staged_run_ids_a = _seed_cross_file_tie(
            repository,
            migrated_dsn,
            conn,
            customer_id=customer_id_a,
            key_prefix="tie_order_a",
            physical_insert_order="ascending",
        )
        conn.commit()

        SCDPublisher().publish(
            _make_context(), "silver.customers", conn, staged_run_ids=staged_run_ids_a
        )
        conn.commit()

    with psycopg.connect(migrated_dsn) as conn:
        staged_run_ids_b = _seed_cross_file_tie(
            repository,
            migrated_dsn,
            conn,
            customer_id=customer_id_b,
            key_prefix="tie_order_b",
            physical_insert_order="descending",
        )
        conn.commit()

        SCDPublisher().publish(
            _make_context(), "silver.customers", conn, staged_run_ids=staged_run_ids_b
        )
        conn.commit()

    versions_a = _fetch_versions(migrated_dsn, customer_id=customer_id_a)
    versions_b = _fetch_versions(migrated_dsn, customer_id=customer_id_b)

    assert len(versions_a) == 3, versions_a
    assert len(versions_b) == 3, versions_b

    # Drop customer_id (index 0) before comparing -- everything else (name, country,
    # birth_date, signup_country, event_ts/valid_from, valid_to, is_current) must match
    # byte-for-byte between the two insertion orders.
    business_content_a = [row[1:] for row in versions_a]
    business_content_b = [row[1:] for row in versions_b]
    assert business_content_a == business_content_b, (
        "SCDPublisher.publish() produced DIFFERENT version chains for the identical "
        "logical cross-file tie depending purely on physical INSERT order into Postgres -- "
        f"the a0cc2f5 fix has regressed. order-ascending={business_content_a!r} "
        f"order-descending={business_content_b!r}"
    )

    # The winning ("current") row in BOTH sub-cases must carry the HIGH file's content --
    # matching recompute.py's own documented tie-break direction (higher file_id wins),
    # already established by tests/unit/test_scd_recompute.py's pure-function regression test.
    current_a = next(v for v in versions_a if v[7] is True)
    current_b = next(v for v in versions_b if v[7] is True)
    assert (current_a[1], current_a[2]) == (_HIGH_FILE_NAME, _HIGH_FILE_COUNTRY)
    assert (current_b[1], current_b[2]) == (_HIGH_FILE_NAME, _HIGH_FILE_COUNTRY)


# ---------------------------------------------------------------------------
# Test 2: pre-fix reconstruction against the SAME real, Postgres-fetched rows --
# proportionate SQL-layer strengthening of the existing pure-function proof
# that the OLD (event_ts, source_row_number)-only key was order-dependent.
# ---------------------------------------------------------------------------


def _pre_fix_current_winner(rows: Sequence[tuple[str, str, datetime, int]]) -> tuple[str, str]:
    """Reconstruct the PRE-a0cc2f5 tie-break (``(event_ts, source_row_number)``, no ``file_id``).

    Deliberately re-implemented locally rather than reverted-to via ``git stash``/``git
    checkout`` of ``recompute.py`` -- doing so mid-integration-test-run would be fragile
    (working-tree churn during a live test session, exactly the hazard this debug session's
    own Evidence already documents happening once from a CONCURRENT sibling session) for
    marginal proof value: ``tests/unit/test_scd_recompute.py``'s own
    ``test_cross_file_event_ts_and_source_row_number_tie_is_order_independent`` already proves,
    in isolation, that the pre-fix key is order-dependent. This function exists ONLY to show
    that property holds against the SAME real, Postgres-fetched rows this file's Test 1 uses
    (SQL-layer data, not a hand-built list) -- strengthening, not duplicating, that proof.

    Valid specifically for this file's own 3-row shape (one unambiguous baseline row plus two
    tied rows with DIFFERING content, so all three consecutive rows have distinct
    tracked-attribute hashes): under Python's stable sort, the tied pair's relative order in
    the OUTPUT is exactly their relative order in ``rows`` (the input), so the row LAST in
    sorted order is that group's own "current"/winning row -- reproducing
    ``recompute_version_chain``'s pre-fix grouping result without needing its hash-boundary
    machinery restated here.

    Args:
        rows: ``(name, country, event_ts, source_row_number)`` tuples, in whatever order the
            caller supplies (e.g. as actually fetched from Postgres, or that fetch reversed).

    Returns:
        ``(name, country)`` of the pre-fix algorithm's "current" row for this input order.
    """
    ordered = sorted(rows, key=lambda r: (r[2], r[3]))
    winner = ordered[-1]
    return (winner[0], winner[1])


def test_pre_fix_tie_break_reconstruction_is_order_dependent_against_the_same_real_rows(
    repository: PostgresMetadataRepository, migrated_dsn: str
) -> None:
    """A fresh customer (976003), seeded with the SAME tie shape/order as Test 1's sub-case B.

    physically staged the HIGH-file row BEFORE the LOW-file row (the "descending"
    order) -- a fresh ``customer_id`` is used rather than reusing Test 1's own 976002, because
    ``staging.customers`` is durable/cumulative and would otherwise silently accumulate a
    SECOND copy of the tie on top of Test 1's rows. This test fetches the rows back via a real,
    unordered ``SELECT`` (mirroring
    ``_BRONZE_HISTORY_SQL``'s own shape: no ``ORDER BY``), then evaluates what the PRE-FIX
    ``(event_ts, source_row_number)``-only tie-break would have concluded from (a) the rows in
    whatever order this fetch actually returned them, and (b) that same fetch reversed --
    a legitimate alternative, since an un-ordered SQL query's result order is, by definition,
    not guaranteed. If these two hypothetical orderings disagree with each other, and at least
    one of them disagrees with the fix's own correct, real answer (established in Test 1:
    the HIGH file's content must win), the pre-fix mechanism is confirmed unsafe against real
    SQL-layer data -- not just against a hand-built Python list.
    """
    # A THIRD, disjoint customer_id (976003) -- deliberately NOT 976002 (Test 1's own
    # sub-case B) -- staging.customers is durable/cumulative (never deduplicated), so reusing
    # 976002 here would append a SECOND copy of the same tie on top of Test 1's own rows
    # rather than seeding a fresh, isolated one.
    customer_id_c = 976003

    with psycopg.connect(migrated_dsn) as conn:
        staged_run_ids_c = _seed_cross_file_tie(
            repository,
            migrated_dsn,
            conn,
            customer_id=customer_id_c,
            key_prefix="prefix_recon_c",
            physical_insert_order="descending",
        )
        conn.commit()

        # Real, unordered SELECT -- mirrors _BRONZE_HISTORY_SQL's own no-ORDER-BY shape.
        fetched_rows = conn.execute(
            """
            SELECT name, country, event_ts::timestamptz, _source_row_number
              FROM staging.customers
             WHERE customer_id = %(customer_id)s
            """,
            {"customer_id": str(customer_id_c)},
        ).fetchall()

    assert len(fetched_rows) == 3, fetched_rows

    winner_as_fetched = _pre_fix_current_winner(fetched_rows)
    winner_reversed = _pre_fix_current_winner(list(reversed(fetched_rows)))

    assert winner_as_fetched != winner_reversed, (
        "the pre-fix (event_ts, source_row_number)-only tie-break should be order-dependent "
        f"against these real rows, but both orderings agreed on {winner_as_fetched!r} -- "
        "this test's own fixture no longer reproduces a genuine tie"
    )

    # At least one of the two hypothetical retrieval orders disagrees with the FIX's own
    # correct, real answer (Test 1: the HIGH file's content, established against this exact
    # staged data) -- direct evidence the pre-fix mechanism could silently diverge from
    # correct behavior depending purely on Postgres' unordered-read order.
    correct_answer = (_HIGH_FILE_NAME, _HIGH_FILE_COUNTRY)
    assert correct_answer in (winner_as_fetched, winner_reversed)
    assert not (winner_as_fetched == correct_answer and winner_reversed == correct_answer)

    staged_run_ids_c_marker = staged_run_ids_c  # keep return value referenced, for clarity
    assert len(staged_run_ids_c_marker) == 3
