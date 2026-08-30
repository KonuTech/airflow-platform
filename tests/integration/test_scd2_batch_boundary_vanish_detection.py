"""SQL-layer integration proof for the ROUND 26/27 batch-boundary vanish hypothesis, and its fix.

Debug session ``.planning/debug/rebuild-scd2-reconciliation.md`` (REOPENED, ROUND 26; CONFIRMED,
ROUND 27; FIXED, ROUND 28): after two independent live reproductions of the pre-fix mismatch
signature with the a0cc2f5 fix genuinely deployed, this session identified a SECOND,
previously-untouched candidate mechanism -- completely independent of a0cc2f5's own sort-key
fix -- that could plausibly explain customer_id 2100100032's full-field mismatch
(``current_valid_from``, ``current_valid_to``, ``current_is_current`` all differing, vs. member
30's ``valid_from``-only mismatch): ``dataplat.scd.delete_detection.find_vanished_customer_ids``
scoped its "is this key present in THIS pass" check to the UNION of every bronze row tagged with
ANY of ``staged_run_ids`` -- whatever set of runs happens to be staged-but-unpublished at the
moment ``publish_ingest`` calls ``ctx.metadata.list_staged_run_ids(...)`` and hands the WHOLE
list into ONE ``SCDPublisher.publish()`` call (``pipeline/run.py:1338``). This batch composition
is a TIMING-DEPENDENT quantity, not a fixed per-file granularity.

ROUND 27 CONFIRMED the mechanism at the real SQL layer: during the ORIGINAL live incremental run
(one ``*/1 * * * *`` scheduler tick at a time, over real wall-clock hours), the file that omits a
vanishing customer is very likely published ALONE or with very few companions -- correctly
registering that customer as vanished. During a bulk ``rebuild-from-raw`` backfill, many files
get discovered+staged in quick succession, so ``list_staged_run_ids`` can return a substantially
LARGER batch spanning many files into ONE publish pass -- and the PRE-FIX
``_VANISHED_SQL``'s own union-of-this-pass's-files semantics meant that if the omitting file was
co-batched with ANY OTHER file that still included the customer (even an OLDER, already-
superseded day), that customer was NEVER detected as vanished in that pass at all -- a genuinely
different terminal ``is_current``/``valid_to`` state than the original run.

ROUND 28 FIXED it (SUPERSEDED by the ROUND 29 correction below): ``_VANISHED_SQL`` first
restricted ``staged_snapshot`` to bronze rows whose OWN ``event_ts`` equalled the MAXIMUM
``event_ts`` among ``staged_run_ids``' own bronze rows -- this pass's own freshest staged
snapshot day. An older, already-superseded day's file merely co-staged in the same batch could
no longer resurrect a key the freshest day's own file omits. The tests below, UPDATED for this
fix, assert that the small-batch and large-batch scenarios agree (both correctly detect the
vanish) -- the batch-boundary sensitivity is closed.

ROUND 29 (specialist code review of commit e614a64, SUGGEST_CHANGE, addressed before any live
confirmation) found the ROUND 28 query's per-ROW ``event_ts``-equality comparison silently
assumed every row belonging to the freshest file shares one identical ``event_ts`` value -- a
contract this dataset does not make (``configs/datasets/customers.yaml`` declares ``event_ts``
as an ordinary per-row timestamp with no uniformity constraint, and
``tests/fixtures/slice-corpus.yaml`` generates it independently per row). The ROUND 28 tests
above only happened to pass because their own fixture data
(``tools/corpus/dated_series.py``'s uniform per-day timestamp) coincidentally shares one
``event_ts`` value across an entire file -- none of them exercise a file with intra-file
``event_ts`` variance. ``_VANISHED_SQL`` is now rescoped to per-RUN (i.e. per-file) granularity
(``GROUP BY _run_id``, comparing each run's own maximum ``event_ts`` to the batch's overall
maximum) rather than per-row value equality, with a ``customer_id IS NOT NULL`` guard applied
consistently to both the freshness computation and the snapshot selection.
``test_intra_file_varying_event_ts_does_not_misclassify_current_customers`` below is the new
regression test this correction adds: it seeds ONE run/file whose own two bronze rows carry
genuinely DIFFERENT ``event_ts`` values (the exact shape ROUND 28's query could not handle) and
proves both customers stay correctly non-vanished.

This module isolates that ONE variable -- the ``staged_run_ids`` argument passed to
``SCDPublisher.publish()`` -- while holding the underlying ``staging.customers`` (bronze) rows
and the pre-pass ``normalized.customers`` state completely fixed between the two scenarios
below, by running each scenario against the SAME seeded rows inside its own transaction and
rolling back before the next scenario runs. This calls the REAL production entry point
(``SCDPublisher.publish()``, which internally calls the real
``dataplat.scd.delete_detection.find_vanished_customer_ids``), not a hand-isolated unit call,
mirroring this suite's own ``test_scd2_cross_file_tie_determinism.py`` precedent for driving
the real path against a real Postgres instance.

Every ``customer_id`` used below is drawn from a disjoint range
(977001-977002/978001-978012/979001-979002) -- see this suite's own established
per-file-disjoint-range convention
(``test_scd2_cross_file_tie_determinism.py``'s 976001-976003, ``test_scd_delete_detection.py``'s
900000s, ``test_publish_scd.py``'s 970000s, ``test_rebuild_reconciliation.py``'s 9704000s,
``test_reconciliation.py``'s 9995000s) -- ``normalized.customers``/``staging.customers`` are
single, SESSION-scoped tables shared across the whole ``tests/integration/`` collection.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
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
    from collections.abc import Iterator

pytestmark = pytest.mark.integration

# "Day 11" -- an adjacent day's file, still delivering BOTH the target (vanishing) customer
# and a roster-covering companion customer.
_DAY11_EVENT_TS = "2026-08-19T00:00:00+00:00"
# "Day 12" -- the LAST day's file, which omits the target customer entirely (the real-world
# DELETE-detection anomaly shape: `test_backfill_2year_sweep.py`'s own
# `_MISSING_CUSTOMER_MEMBER_INDEX` is omitted specifically on the sweep's final day).
_DAY12_EVENT_TS = "2026-08-20T00:00:00+00:00"

_TARGET_CUSTOMER_ID = 978001
_COMPANION_CUSTOMER_ID = 978002
# Disjoint pair for test_large_batch_... -- see _seed_fixed_scenario's own docstring on why
# reusing _TARGET_CUSTOMER_ID/_COMPANION_CUSTOMER_ID across two different test functions would
# collide on normalized.customers's durable, session-shared state.
_TARGET_CUSTOMER_ID_LARGE_BATCH = 978011
_COMPANION_CUSTOMER_ID_LARGE_BATCH = 978012

# ROUND 29 regression pair (specialist review of commit e614a64): ONE run/file's own two bronze
# rows carry genuinely DIFFERENT event_ts values -- "early" and "late" within the SAME file --
# to prove _VANISHED_SQL's rescoped, per-RUN freshness comparison does not misclassify a
# freshest run's own earlier-timestamped row as vanished merely because it does not carry that
# run's single latest timestamp (the ROUND 28 per-row-event_ts-equality shape would have).
_INTRA_FILE_EARLY_CUSTOMER_ID = 977001
_INTRA_FILE_LATE_CUSTOMER_ID = 977002


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Insert a synthetic ``meta.config_versions`` row directly via SQL.

    Duplicated from ``test_scd2_cross_file_tie_determinism.py``'s own helper of the same name,
    per this suite's established per-file-helper convention.
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

    Mirrors ``test_scd2_cross_file_tie_determinism.py``'s own ``_seed_run`` -- duplicated
    locally rather than imported, per this suite's established per-file-helper convention.
    """
    dataset_id = repository.get_or_create_dataset(f"scd2_batch_boundary_{key_suffix}")
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


def _insert_bronze_customer(  # noqa: PLR0913 -- one keyword per column, mirrors this suite's own convention
    conn: psycopg.Connection[Any],
    *,
    customer_id: str,
    event_ts: str,
    run_id: int,
    file_id: int,
    batch_id: int,
    source_row_number: int,
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
            f"bronze-customer-{customer_id}",
            "US",
            "1985-05-05",
            event_ts,
            "US",
            run_id,
            file_id,
            batch_id,
            source_row_number,
            hashlib.sha256(f"{customer_id}:{run_id}".encode()).digest(),
        ),
    )


def _insert_normalized_customer(  # noqa: PLR0913 -- one keyword per column, mirrors test_scd_delete_detection.py's own helper
    conn: psycopg.Connection[Any],
    *,
    customer_id: int,
    event_ts: datetime,
    run_id: int,
    file_id: int,
    batch_id: int,
    source_row_number: int,
) -> None:
    """Insert one real, ``is_current=true`` ``normalized.customers`` row directly via SQL."""
    conn.execute(
        """
        INSERT INTO normalized.customers (
            customer_id, name, country, birth_date, event_ts, is_current,
            _run_id, _file_id, _batch_id, _source_row_number,
            _record_hash, _record_hash_version
        ) VALUES (
            %s, %s, %s, %s, %s, true,
            %s, %s, %s, %s,
            %s, 1
        )
        """,
        (
            customer_id,
            f"customer-{customer_id}",
            "US",
            "1985-05-05",
            event_ts,
            run_id,
            file_id,
            batch_id,
            source_row_number,
            hashlib.sha256(f"gold:{customer_id}".encode()).digest(),
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
    """A ``PipelineContext`` with ``delete_semantics="invalidate"``.

    ``"invalidate"`` (not ``"new_record"``/``"ignore"``) is deliberately chosen: it is the
    simplest semantics to assert on (the existing current row's own ``is_current``/``valid_to``
    flip in place, no new row is inserted), and this test's own hypothesis is entirely about
    WHETHER a key is detected as vanished at all, not about which delete-semantics variant
    then acts on it (that dispatch is already covered by ``test_scd_delete_detection.py``).
    ``mass_delete_threshold=1.0`` -- identical to ``test_scd2_cross_file_tie_determinism.py``'s
    own ``_make_context``, for the identical reason: this session-shared-table suite must not
    let Step A's DELETE-detection trip the breaker on every OTHER test file's own
    ``is_current`` rows.
    """
    return PipelineContext(
        run=RunContext(run_id=0, idempotency_key="scd2-batch-boundary-test-placeholder"),
        config=DatasetConfig(
            dataset="customers",
            config_schema_version=1,
            source=SourceConfig(
                type="csv",
                bucket="scd2-batch-boundary-test",
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
            scd=ScdConfig(delete_semantics="invalidate", mass_delete_threshold=1.0),
        ),
        metadata=None,  # type: ignore[arg-type] -- unused by SCDPublisher.publish()
        objects=None,  # type: ignore[arg-type] -- unused by SCDPublisher.publish()
        db=None,  # type: ignore[arg-type] -- unused by SCDPublisher.publish()
        log=None,  # type: ignore[arg-type] -- unused by SCDPublisher.publish()
    )


def _seed_fixed_scenario(  # noqa: PLR0913 -- one keyword per seeding concern, mirrors this suite's own convention
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
    conn: psycopg.Connection[Any],
    *,
    key_prefix: str,
    target_customer_id: int,
    companion_customer_id: int,
) -> tuple[int, int]:
    """Seed the ONE fixed bronze/gold shape both scenarios below evaluate, unmodified.

    Two bronze files:

    * "day11" (an adjacent day): delivers BOTH ``target_customer_id`` and
      ``companion_customer_id``.
    * "day12" (the final day): delivers ONLY ``companion_customer_id`` -- the target customer
      is omitted, mirroring the real anomaly shape (a customer entirely absent from a sweep's
      own final day's file).

    Plus a pre-existing ``normalized.customers`` ``is_current=true`` row for BOTH customers,
    representing the gold state as it stood immediately before "day12"'s own publish pass.

    ``key_prefix`` MUST be unique per calling test -- ``_seed_run``'s own
    ``_insert_config_version`` helper hardcodes a single literal ``config_hash`` string, and
    ``meta.config_versions`` enforces ``UNIQUE (dataset_id, config_hash)``; reusing the same
    ``key_prefix`` (and so the same derived dataset name/dataset_id) across two different test
    functions would collide on that constraint the second time this is called. Likewise,
    ``target_customer_id``/``companion_customer_id`` MUST be disjoint per calling test --
    ``normalized.customers`` is a durable, session-shared table (this file's own module
    docstring), so two tests both writing an ``is_current=true`` row for the same customer_id
    would interfere with each other's starting state.

    Returns:
        ``(day11_run_id, day12_run_id)``.
    """
    day11_run_id, day11_file_id, day11_batch_id = _seed_run(
        repository, migrated_dsn, key_suffix=f"{key_prefix}_day11"
    )
    day12_run_id, day12_file_id, day12_batch_id = _seed_run(
        repository, migrated_dsn, key_suffix=f"{key_prefix}_day12"
    )

    _insert_bronze_customer(
        conn,
        customer_id=str(target_customer_id),
        event_ts=_DAY11_EVENT_TS,
        run_id=day11_run_id,
        file_id=day11_file_id,
        batch_id=day11_batch_id,
        source_row_number=1,
    )
    _insert_bronze_customer(
        conn,
        customer_id=str(companion_customer_id),
        event_ts=_DAY11_EVENT_TS,
        run_id=day11_run_id,
        file_id=day11_file_id,
        batch_id=day11_batch_id,
        source_row_number=2,
    )
    # day12's own file: ONLY the companion -- the target is entirely absent from this file.
    _insert_bronze_customer(
        conn,
        customer_id=str(companion_customer_id),
        event_ts=_DAY12_EVENT_TS,
        run_id=day12_run_id,
        file_id=day12_file_id,
        batch_id=day12_batch_id,
        source_row_number=1,
    )

    _insert_normalized_customer(
        conn,
        customer_id=target_customer_id,
        event_ts=datetime.fromisoformat(_DAY11_EVENT_TS),
        run_id=day11_run_id,
        file_id=day11_file_id,
        batch_id=day11_batch_id,
        source_row_number=1,
    )
    _insert_normalized_customer(
        conn,
        customer_id=companion_customer_id,
        event_ts=datetime.fromisoformat(_DAY11_EVENT_TS),
        run_id=day11_run_id,
        file_id=day11_file_id,
        batch_id=day11_batch_id,
        source_row_number=2,
    )

    return day11_run_id, day12_run_id


def _current_state(migrated_dsn: str, *, customer_id: int) -> tuple[bool, Any]:
    """Return ``(is_current, valid_to)`` of ``customer_id``'s row(s) currently flagged current.

    Asserts exactly one such row exists -- both scenarios below only ever produce 0 or 1
    ``is_current=true`` rows per customer_id (``"invalidate"`` never inserts a new row).
    """
    with psycopg.connect(migrated_dsn) as conn:
        rows = conn.execute(
            "SELECT is_current, valid_to FROM normalized.customers "
            "WHERE customer_id = %s ORDER BY event_ts DESC LIMIT 1",
            (customer_id,),
        ).fetchall()
    assert len(rows) == 1, rows
    return bool(rows[0][0]), rows[0][1]


def test_small_batch_correctly_detects_the_vanish(
    repository: PostgresMetadataRepository, migrated_dsn: str
) -> None:
    """``staged_run_ids`` scoped to day12's file ALONE -- mirrors one live scheduler tick.

    day12's own file omits the target customer entirely, and no OTHER staged file in this
    pass's batch re-covers it, so the union-of-this-pass's-files snapshot genuinely does not
    contain the target -- ``find_vanished_customer_ids`` must report it vanished, and
    ``SCDPublisher.publish()``'s own ``"invalidate"`` dispatch must close its current row.
    """
    with psycopg.connect(migrated_dsn) as conn:
        _day11_run_id, day12_run_id = _seed_fixed_scenario(
            repository,
            migrated_dsn,
            conn,
            key_prefix="small_batch",
            target_customer_id=_TARGET_CUSTOMER_ID,
            companion_customer_id=_COMPANION_CUSTOMER_ID,
        )
        conn.commit()

        SCDPublisher().publish(
            _make_context(), "silver.customers", conn, staged_run_ids=[day12_run_id]
        )
        conn.commit()

    is_current, valid_to = _current_state(migrated_dsn, customer_id=_TARGET_CUSTOMER_ID)
    assert is_current is False, (
        "small-batch (day12 staged ALONE): the target customer's day12-omitting file was the "
        "ENTIRE staged snapshot for this pass, yet it was not detected as vanished -- the "
        "batch-boundary hypothesis's own baseline expectation (correct detection under a "
        "narrow, live-like batch) does not hold"
    )
    assert valid_to is not None


def test_large_batch_co_staged_with_a_covering_file_still_detects_the_vanish_post_fix(
    repository: PostgresMetadataRepository, migrated_dsn: str
) -> None:
    """``staged_run_ids`` spans BOTH day11 AND day12 -- mirrors a bulk rebuild's batched staging.

    The SAME underlying bronze rows as the small-batch scenario above (a fresh customer_id
    pair is used only because ``normalized.customers``/``staging.customers`` are durable,
    cumulative, session-shared tables -- reusing the prior test's own rows would silently
    layer a second pass on top of the first's already-committed state rather than presenting
    an identical, untouched starting point). day11's file (co-staged in the SAME pass as
    day12's) still delivers the target customer, but day11 is an OLDER, already-superseded
    day relative to day12 -- POST-FIX, ``staged_snapshot`` is restricted to this pass's own
    FRESHEST staged day (day12), so day11's stale co-presence must no longer resurrect the
    target: ``find_vanished_customer_ids`` must report it vanished here, exactly as the
    small-batch scenario above does. PRE-FIX, this scenario incorrectly reported ``is_current``
    still ``True`` (the batch-boundary defect this test file was built to confirm, ROUND 27) --
    that pre-fix behavior is preserved in this test's own git history for reference.
    """
    with psycopg.connect(migrated_dsn) as conn:
        day11_run_id, day12_run_id = _seed_fixed_scenario(
            repository,
            migrated_dsn,
            conn,
            key_prefix="large_batch",
            target_customer_id=_TARGET_CUSTOMER_ID_LARGE_BATCH,
            companion_customer_id=_COMPANION_CUSTOMER_ID_LARGE_BATCH,
        )
        conn.commit()

        SCDPublisher().publish(
            _make_context(),
            "silver.customers",
            conn,
            staged_run_ids=[day11_run_id, day12_run_id],
        )
        conn.commit()

    is_current, valid_to = _current_state(migrated_dsn, customer_id=_TARGET_CUSTOMER_ID_LARGE_BATCH)
    assert is_current is False, (
        "large-batch (day11+day12 co-staged): the target customer's day12-omitting file was "
        "co-batched with day11's own OLDER, already-superseded file, yet the target was NOT "
        "reported vanished -- the ROUND 28 fix (restricting staged_snapshot to this pass's own "
        "freshest staged event_ts) is not closing the batch-boundary defect"
    )
    assert valid_to is not None


def test_same_bronze_rows_only_staged_run_ids_composition_differs_yields_same_correct_outcome(
    repository: PostgresMetadataRepository, migrated_dsn: str
) -> None:
    """The direct, side-by-side confirmation: ONE fixed bronze/gold seed, TWO publish() calls.

    Unlike the two tests above (which use disjoint customer_id pairs to avoid cross-test
    state interference), this test seeds ONE scenario and calls
    ``SCDPublisher.publish()`` TWICE against the literal SAME rows -- first with the
    small-batch ``staged_run_ids``, observing the outcome, then ROLLING BACK (so neither the
    bronze rows nor the gold state nor the small-batch call's own writes persist) before
    re-running with the large-batch ``staged_run_ids`` against the SAME pre-pass state. This
    isolates ``staged_run_ids`` composition as the ONLY variable between the two calls -- not
    merely "the same shape of data" (the two tests above) but the literal same rows.

    POST-FIX (ROUND 28): both calls must now agree (both detect the vanish) -- this is the
    core invariant the fix restores: vanish-detection must not depend on how many runs/days
    happen to be co-staged into one pass. PRE-FIX, this assertion was inverted (the two
    outcomes were required to DIFFER, confirming the defect existed at the SQL layer, ROUND
    27) -- see this test's own git history for that confirmatory shape.
    """
    target_id = _TARGET_CUSTOMER_ID + 1000  # disjoint from the two tests above (979001)
    companion_id = _COMPANION_CUSTOMER_ID + 1000  # 979002

    conn = psycopg.connect(migrated_dsn)
    try:
        day11_run_id, day11_file_id, day11_batch_id = _seed_run(
            repository, migrated_dsn, key_suffix="same_rows_day11"
        )
        day12_run_id, day12_file_id, day12_batch_id = _seed_run(
            repository, migrated_dsn, key_suffix="same_rows_day12"
        )
        _insert_bronze_customer(
            conn,
            customer_id=str(target_id),
            event_ts=_DAY11_EVENT_TS,
            run_id=day11_run_id,
            file_id=day11_file_id,
            batch_id=day11_batch_id,
            source_row_number=1,
        )
        _insert_bronze_customer(
            conn,
            customer_id=str(companion_id),
            event_ts=_DAY11_EVENT_TS,
            run_id=day11_run_id,
            file_id=day11_file_id,
            batch_id=day11_batch_id,
            source_row_number=2,
        )
        _insert_bronze_customer(
            conn,
            customer_id=str(companion_id),
            event_ts=_DAY12_EVENT_TS,
            run_id=day12_run_id,
            file_id=day12_file_id,
            batch_id=day12_batch_id,
            source_row_number=1,
        )
        _insert_normalized_customer(
            conn,
            customer_id=target_id,
            event_ts=datetime.fromisoformat(_DAY11_EVENT_TS),
            run_id=day11_run_id,
            file_id=day11_file_id,
            batch_id=day11_batch_id,
            source_row_number=1,
        )
        _insert_normalized_customer(
            conn,
            customer_id=companion_id,
            event_ts=datetime.fromisoformat(_DAY11_EVENT_TS),
            run_id=day11_run_id,
            file_id=day11_file_id,
            batch_id=day11_batch_id,
            source_row_number=2,
        )
        conn.commit()

        # --- Scenario 1: small batch (day12 alone) -- run inside a savepoint-free
        # transaction, then ROLL BACK so scenario 2 sees the identical pre-pass state. ---
        SCDPublisher().publish(
            _make_context(), "silver.customers", conn, staged_run_ids=[day12_run_id]
        )
        small_batch_is_current = conn.execute(
            "SELECT is_current FROM normalized.customers "
            "WHERE customer_id = %s ORDER BY event_ts DESC LIMIT 1",
            (target_id,),
        ).fetchone()
        assert small_batch_is_current is not None
        conn.rollback()

        # --- Scenario 2: large batch (day11 + day12 together) -- against the SAME rows,
        # since the rollback above restored the exact pre-pass state. ---
        SCDPublisher().publish(
            _make_context(),
            "silver.customers",
            conn,
            staged_run_ids=[day11_run_id, day12_run_id],
        )
        large_batch_is_current = conn.execute(
            "SELECT is_current FROM normalized.customers "
            "WHERE customer_id = %s ORDER BY event_ts DESC LIMIT 1",
            (target_id,),
        ).fetchone()
        assert large_batch_is_current is not None
    finally:
        conn.rollback()
        conn.close()

    assert small_batch_is_current[0] is False, (
        "small-batch scenario: expected the target to be detected vanished (is_current=False)"
    )
    assert large_batch_is_current[0] is False, (
        "large-batch scenario: expected the target to ALSO be detected vanished "
        "(is_current=False), post-fix -- day11's co-staged file is an OLDER, already-superseded "
        "day and must no longer resurrect a key day12's own freshest file omits"
    )
    assert small_batch_is_current[0] == large_batch_is_current[0], (
        "FIX-VERIFICATION marker for the ROUND 26/27 batch-boundary defect (ROUND 28 fix): "
        "against the literal SAME staging.customers bronze rows and the literal SAME pre-pass "
        "normalized.customers state, changing ONLY the staged_run_ids composition passed to "
        "SCDPublisher.publish() must NO LONGER flip the vanish outcome -- vanish-detection must "
        "depend solely on this pass's own freshest staged snapshot day, never on how many "
        "runs/days happen to be co-staged alongside it. If this assertion ever fails, the "
        "batch-boundary fix has regressed."
    )


def test_intra_file_varying_event_ts_does_not_misclassify_current_customers(
    repository: PostgresMetadataRepository, migrated_dsn: str
) -> None:
    """ROUND 29 regression: ONE run/file whose own rows carry DIFFERING ``event_ts`` values.

    Specialist code review of commit e614a64 (see this module's own docstring, ROUND 29
    paragraph) found the ROUND 28 fix's ``_VANISHED_SQL`` compared each individual bronze
    ROW's own ``event_ts`` against a single scalar maximum computed over the WHOLE staged
    batch -- silently assuming every row belonging to the freshest file shares one identical
    ``event_ts``. Every OTHER test in this module only ever seeds files whose rows share one
    uniform per-file ``event_ts`` (mirroring ``tools/corpus/dated_series.py``'s own real-world
    generator), so none of them can catch this: they would all have passed under the ROUND 28
    query too.

    This test seeds a SINGLE run/file (the only staged run, hence trivially this pass's own
    freshest run) whose two bronze rows carry genuinely DIFFERENT ``event_ts`` values --
    ``_INTRA_FILE_EARLY_CUSTOMER_ID`` at the earlier ``_DAY11_EVENT_TS``, and
    ``_INTRA_FILE_LATE_CUSTOMER_ID`` at the later ``_DAY12_EVENT_TS``, both delivered by the
    SAME file. Both customers have a pre-existing ``is_current=true`` row in
    ``normalized.customers``. Since BOTH rows belong to the pass's one and only (and therefore
    freshest) run, ``find_vanished_customer_ids`` must report NEITHER as vanished.

    Under the PRE-CORRECTION (ROUND 28) per-row-``event_ts``-equality shape, only
    ``_INTRA_FILE_LATE_CUSTOMER_ID``'s row would have matched the batch's own single maximum
    ``event_ts``, silently excluding ``_INTRA_FILE_EARLY_CUSTOMER_ID``'s row from
    ``staged_snapshot`` even though its own file delivered it -- misclassifying it as vanished
    and flipping its ``is_current`` to ``False``. Confirmed via a genuine RED run against the
    ROUND 28 query (reverted locally, not committed) before this correction: exactly that row
    flipped. This test is now GREEN against the ROUND 29 per-run-scoped query.
    """
    with psycopg.connect(migrated_dsn) as conn:
        run_id, file_id, batch_id = _seed_run(
            repository, migrated_dsn, key_suffix="intra_file_varying"
        )

        _insert_bronze_customer(
            conn,
            customer_id=str(_INTRA_FILE_EARLY_CUSTOMER_ID),
            event_ts=_DAY11_EVENT_TS,
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
        )
        _insert_bronze_customer(
            conn,
            customer_id=str(_INTRA_FILE_LATE_CUSTOMER_ID),
            event_ts=_DAY12_EVENT_TS,
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=2,
        )
        _insert_normalized_customer(
            conn,
            customer_id=_INTRA_FILE_EARLY_CUSTOMER_ID,
            event_ts=datetime.fromisoformat(_DAY11_EVENT_TS),
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
        )
        _insert_normalized_customer(
            conn,
            customer_id=_INTRA_FILE_LATE_CUSTOMER_ID,
            event_ts=datetime.fromisoformat(_DAY12_EVENT_TS),
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=2,
        )
        conn.commit()

        SCDPublisher().publish(_make_context(), "silver.customers", conn, staged_run_ids=[run_id])
        conn.commit()

    early_is_current, _ = _current_state(migrated_dsn, customer_id=_INTRA_FILE_EARLY_CUSTOMER_ID)
    late_is_current, _ = _current_state(migrated_dsn, customer_id=_INTRA_FILE_LATE_CUSTOMER_ID)

    assert early_is_current is True, (
        "the EARLY-timestamped customer, delivered by this pass's own (only, hence freshest) "
        "file, was misclassified as vanished -- _VANISHED_SQL is comparing per-row event_ts "
        "against a batch-wide scalar maximum again (the ROUND 28 defect this test guards "
        "against), rather than scoping freshness to the whole RUN/FILE"
    )
    assert late_is_current is True, (
        "the LATE-timestamped customer, delivered by this pass's own (only, hence freshest) "
        "file, was unexpectedly reported vanished -- unrelated to the ROUND 29 defect, but "
        "still a regression in find_vanished_customer_ids for this scenario"
    )
