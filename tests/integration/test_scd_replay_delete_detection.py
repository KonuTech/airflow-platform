"""Replay-wave DELETE-detection: re-staged identical content must never read as vanished.

Reproduces debug session ci-pipeline-ingestion-timeout residual (16), observed
live on e2e-full run 32884691063: after one successful publish, a full replay
wave (identical file content re-staged under NEW run_ids -- exactly what
D-18's idempotency-key formula produces after `meta.schema_versions` gains its
first/changed version, since discovery's `schema_version_term` changes and
every already-SUCCEEDED file becomes eligible again) deterministically tripped
`mass_delete_circuit_breaker` at 54% (vanished 27 / current 50) and wedged
every subsequent DagRun to `dagrun_timeout`.

Mechanism (all confirmed by direct source reads):

1. A replayed bronze row is byte-identical to its wave-1 sibling -- same
   `event_ts`, same `_source_row_number`, same `_file_id` (discovery's
   `create_file` is idempotent by `object_uri`) -- only `_run_id` differs.
2. `silver_customers.sql` ranks contenders by `event_ts desc,
   _source_row_number desc, _file_id desc`: for every business key the
   resident wave-1 row and its wave-2 replay tie on ALL THREE terms, so the
   winner (whose `_run_id` the silver row keeps) is arbitrary.
3. `dataplat.scd.delete_detection._VANISHED_SQL` defined "this pass's staged
   snapshot" as `silver.customers WHERE _run_id = ANY(staged_run_ids)` -- a
   PROXY for "the keys this pass's files delivered" that breaks whenever a
   pass's rows tie-lose: every tie-loser key reads as vanished even though
   the pass's own files contain it (live: 27 of 50 keys -> 54% > 10%).

The right-layer fix asserted here: the staged snapshot must be read from
`staging.customers` (bronze) scoped to `staged_run_ids` -- bronze holds
exactly what this pass's files delivered, immune to silver's dedup-tie
lineage. With that, a replay of identical content has vanished == 0 by
construction, regardless of how silver's ties break.

Runs against its OWN throwaway container (same reasoning as
`test_dbt_dedup_audit.py`'s fresh-database test): the vanished computation is
deliberately whole-table (`normalized`/`staging`/`silver`), so the shared
`migrated_dsn`'s cross-file data would pollute the ratio.
"""

from __future__ import annotations

import hashlib
import logging
from typing import TYPE_CHECKING

import psycopg
import pytest
from testcontainers.community.postgres import PostgresContainer

from dataplat.config.model import (
    BatchingConfig,
    ColumnContract,
    DatasetConfig,
    LoadConfig,
    ScdConfig,
    SourceConfig,
)
from dataplat.errors import PublicationError
from dataplat.load.publish.scd import SCDPublisher
from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.models.identity import RunContext
from dataplat.pipeline.protocol import PipelineContext
from dataplat.pipeline.run import publish_ingest
from dataplat.scd.delete_detection import find_vanished_customer_ids
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    from collections.abc import Callable

pytestmark = [pytest.mark.dbt, pytest.mark.integration]

# Mirrors the live CI shape that tripped (run 32884691063): a 50-member
# roster resent in full across 10 day-files per pass (test_backfill_2year_
# sweep.py's seed-v5 corpus under discovery's max_units_per_run=10 cap).
_ROSTER_SIZE = 50
_NUM_DAY_FILES = 10
# configs/datasets/customers.yaml's own configured threshold -- the value the
# live trip breached.
_MASS_DELETE_THRESHOLD = 0.10


def _make_context() -> PipelineContext:
    """A ``PipelineContext`` with customers' REAL scd config values (threshold 0.10)."""
    return PipelineContext(
        run=RunContext(run_id=0, idempotency_key="replay-delete-detection-placeholder"),
        config=DatasetConfig(
            dataset="customers",
            config_schema_version=1,
            source=SourceConfig(
                type="csv",
                bucket="replay-test",
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
            ],
            scd=ScdConfig(delete_semantics="ignore", mass_delete_threshold=_MASS_DELETE_THRESHOLD),
        ),
        metadata=None,  # type: ignore[arg-type] -- unused by SCDPublisher.publish()
        objects=None,  # type: ignore[arg-type] -- unused by SCDPublisher.publish()
        db=None,  # type: ignore[arg-type] -- unused by SCDPublisher.publish()
        log=None,  # type: ignore[arg-type] -- unused by SCDPublisher.publish()
    )


def _get_or_create_config_version(conn: psycopg.Connection, *, dataset_id: int) -> int:
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
        ) VALUES (%s, 1, 'replay-test-hash', '{"synthetic": true}'::jsonb, 1, now())
        RETURNING config_version_id
        """,
        (dataset_id,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _seed_wave(
    repository: PostgresMetadataRepository,
    dsn: str,
    *,
    wave: int,
    file_ids: list[int] | None,
    batch_ids: list[int] | None,
) -> tuple[list[int], list[int], list[int]]:
    """Insert one full pass: ``_NUM_DAY_FILES`` runs, each a full-roster daily snapshot.

    ``wave=1`` creates the files/batches; ``wave=2`` REUSES the given
    ``file_ids``/``batch_ids`` verbatim (discovery's ``create_file``/
    ``get_or_create_batch`` are idempotent by object_uri/content, so a
    replay's bronze rows carry the SAME ``_file_id``/``_batch_id`` -- only
    ``_run_id`` differs; that identity is exactly what produces the silver
    ranking tie this test exists for).

    Returns:
        ``(run_ids, file_ids, batch_ids)`` for the wave, in day order.
    """
    dataset_id = repository.get_or_create_dataset("customers")
    with psycopg.connect(dsn, autocommit=True) as conn:
        config_version_id = _get_or_create_config_version(conn, dataset_id=dataset_id)

    run_ids: list[int] = []
    out_file_ids: list[int] = []
    out_batch_ids: list[int] = []
    for day in range(_NUM_DAY_FILES):
        if file_ids is None or batch_ids is None:
            file_id = repository.create_file(
                dataset_id=dataset_id,
                object_uri=f"s3://raw/customers/replay-test-day{day}.csv",
                content_sha256=hashlib.sha256(f"replay-day{day}".encode()).digest(),
                hash_version=1,
                size_bytes=10,
                filename=f"replay-test-day{day}.csv",
                status="DISCOVERED",
            )
            batch_id = repository.create_batch(
                dataset_id=dataset_id,
                batch_key=f"replay-test:day{day}",
                status="OPEN",
            )
        else:
            file_id = file_ids[day]
            batch_id = batch_ids[day]
        run_id = repository.create_ingestion_run(
            idempotency_key=f"replay-test:wave{wave}:day{day}",
            dataset_id=dataset_id,
            config_version_id=config_version_id,
            processor_version="0.1.0",
            processor_image_digest="sha256:testdigest",
            status="RUNNING",
            file_id=file_id,
            batch_id=batch_id,
        )
        run_ids.append(run_id)
        out_file_ids.append(file_id)
        out_batch_ids.append(batch_id)

        with psycopg.connect(dsn, autocommit=True) as conn:
            for member in range(_ROSTER_SIZE):
                customer_id = str(1000 + member)
                # Byte-identical across waves BY DESIGN: same event_ts, same
                # _source_row_number, same _file_id/_batch_id/_record_hash --
                # the replay differs ONLY in _run_id, mirroring a real D-18
                # formula-driven reprocess of an unchanged object.
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
                        f"Name {member}",
                        "US",
                        "1990-01-01",
                        f"2024-01-{day + 1:02d}T08:15:00+00:00",
                        run_id,
                        file_id,
                        batch_id,
                        member + 1,
                        hashlib.sha256(f"replay-row:{day}:{member}".encode()).digest(),
                    ),
                )
    return run_ids, out_file_ids, out_batch_ids


def _seed_truncated_single_file_pass(
    repository: PostgresMetadataRepository,
    dsn: str,
    *,
    wave: int,
    kept_members: int,
) -> int:
    """Seed ONE later-day snapshot file delivering only the first ``kept_members`` roster keys.

    Mirrors the live mass-delete breaker fixture's shape (test_backfill_2year_sweep.py Task 3):
    a single-day snapshot whose roster is deliberately truncated, staged as its own pass. The
    run is left in status ``STAGED`` -- exactly what ``publish_ingest``'s
    ``list_staged_run_ids`` claims -- so a subsequent ``publish_ingest(ctx)`` call exercises
    the REAL pass-claiming + breaker + quarantine path, not a hand-fed ``staged_run_ids``
    list.

    Returns:
        The staged run's ``run_id``.
    """
    dataset_id = repository.get_or_create_dataset("customers")
    with psycopg.connect(dsn, autocommit=True) as conn:
        config_version_id = _get_or_create_config_version(conn, dataset_id=dataset_id)

    file_id = repository.create_file(
        dataset_id=dataset_id,
        object_uri=f"s3://raw/customers/replay-test-truncated-wave{wave}.csv",
        content_sha256=hashlib.sha256(f"replay-truncated-{wave}".encode()).digest(),
        hash_version=1,
        size_bytes=10,
        filename=f"replay-test-truncated-wave{wave}.csv",
        status="DISCOVERED",
    )
    batch_id = repository.create_batch(
        dataset_id=dataset_id,
        batch_key=f"replay-test:truncated-wave{wave}",
        status="OPEN",
    )
    run_id = repository.create_ingestion_run(
        idempotency_key=f"replay-test:truncated-wave{wave}",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        status="STAGED",
        file_id=file_id,
        batch_id=batch_id,
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        for member in range(kept_members):
            conn.execute(
                """
                INSERT INTO staging.customers (
                    customer_id, name, country, birth_date, event_ts,
                    _run_id, _file_id, _batch_id, _source_row_number,
                    _record_hash, _record_hash_version
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
                """,
                (
                    str(1000 + member),
                    f"Name {member}",
                    "US",
                    "1990-01-01",
                    # Strictly later than every _seed_wave day (Jan 1-10) --
                    # a genuinely NEWER snapshot, mirroring the live
                    # fixture's day-after-the-sweep placement.
                    "2024-01-15T08:15:00+00:00",
                    run_id,
                    file_id,
                    batch_id,
                    member + 1,
                    hashlib.sha256(f"replay-truncated-row:{wave}:{member}".encode()).digest(),
                ),
            )
    return run_id


def _run_statuses(dsn: str, *, run_ids: list[int]) -> set[str]:
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            "SELECT status FROM meta.ingestion_runs WHERE run_id = ANY(%s)",
            (run_ids,),
        ).fetchall()
    return {str(row[0]) for row in rows}


def _silver_run_id_split(dsn: str, *, wave2_run_ids: list[int]) -> tuple[int, int]:
    """(keys whose silver row carries a wave-2 _run_id, keys carrying an older one)."""
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            """
            SELECT count(*) FILTER (WHERE _run_id = ANY(%(wave2)s)),
                   count(*) FILTER (WHERE _run_id != ALL(%(wave2)s))
              FROM silver.customers
            """,
            {"wave2": wave2_run_ids},
        ).fetchone()
    assert row is not None
    return int(row[0]), int(row[1])


def test_replay_of_identical_content_never_reads_as_vanished(
    run_migrations: Callable[[str], None],
    run_dbt_build: Callable[..., object],
) -> None:
    """A full replay wave must publish cleanly: vanished == 0, breaker never trips.

    Wave 1: fresh database, 10 day-files x 50-key roster staged + dbt-built +
    published (gold ends at 50 current keys). Wave 2: the identical content
    re-staged under new run_ids (the D-18 replay shape), dbt-built, published.
    Pre-fix, wave 2's publish read every silver dedup tie-loser as vanished
    (live CI: 27/50 = 54% > 10% -> `QualityThresholdExceeded`, poison wedge);
    post-fix (bronze-scoped staged snapshot) vanished is 0 by construction.
    """
    with PostgresContainer("postgres:18-bookworm", driver="psycopg", dbname="analytics") as pg:
        dsn = pg.get_connection_url().replace("postgresql+psycopg://", "postgresql://")
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            # The roles cnpg-analytics.yaml's initdb guarantees exist before
            # migrations run on a real cluster (conftest.postgres_dsn's own
            # bootstrap, mirrored by test_dbt_dedup_audit.py's precedent).
            cur.execute("CREATE ROLE etl_app LOGIN")
            cur.execute("CREATE ROLE analytics_owner LOGIN")
        run_migrations(dsn)

        pool = create_pool(dsn)
        pool.open(wait=True)
        try:
            repository = PostgresMetadataRepository(pool)

            # --- Wave 1: first-ever pass over the corpus -----------------
            wave1_run_ids, file_ids, batch_ids = _seed_wave(
                repository, dsn, wave=1, file_ids=None, batch_ids=None
            )
            run_dbt_build(dsn, select="silver_customers")

            ctx = _make_context()
            publisher = SCDPublisher()
            with psycopg.connect(dsn) as conn, conn.transaction():
                result1 = publisher.publish(
                    ctx, "silver.customers", conn, staged_run_ids=wave1_run_ids
                )
            assert sorted(int(k) for k in result1.published_business_keys) == [
                1000 + m for m in range(_ROSTER_SIZE)
            ]

            with psycopg.connect(dsn) as conn:
                gold_current = conn.execute(
                    "SELECT count(*) FROM normalized.customers WHERE is_current"
                ).fetchone()
            assert gold_current is not None
            assert gold_current[0] == _ROSTER_SIZE

            # --- Wave 2: the replay (identical content, new run_ids) ----
            wave2_run_ids, _, _ = _seed_wave(
                repository, dsn, wave=2, file_ids=file_ids, batch_ids=batch_ids
            )
            run_dbt_build(dsn, select="silver_customers")

            # Diagnostic, not an assertion: how silver's full ties actually
            # broke in THIS environment (live CI run 32884691063 split
            # 23 new / 27 old -> the 54% trip). Recorded so a future reader
            # of this test's output can see the tie behavior directly.
            confirmed, tie_losers = _silver_run_id_split(dsn, wave2_run_ids=wave2_run_ids)
            logging.getLogger(__name__).info(
                "silver dedup tie split after replay build: "
                "%d keys carry a wave-2 _run_id, %d kept wave-1",
                confirmed,
                tie_losers,
            )

            # The core regression assertion: bronze-scoped DELETE-detection
            # sees every roster key in wave 2's own staged rows -> nothing
            # vanished, regardless of how the silver ties broke above.
            with psycopg.connect(dsn) as conn:
                vanished = find_vanished_customer_ids(conn, staged_run_ids=wave2_run_ids)
            assert vanished == set(), (
                f"replay of identical content read {len(vanished)}/{_ROSTER_SIZE} keys as "
                f"vanished ({len(vanished) / _ROSTER_SIZE:.0%}) -- the staged-snapshot "
                f"proxy is leaking silver dedup-tie lineage into DELETE-detection "
                f"(live CI signature: 27/50 = 54% > 10% breaker trip)"
            )

            # And the full publish path must not trip the breaker (threshold
            # 0.10 -- customers.yaml's real value).
            with psycopg.connect(dsn) as conn, conn.transaction():
                result2 = publisher.publish(
                    ctx, "silver.customers", conn, staged_run_ids=wave2_run_ids
                )
            assert result2.outcome == "PUBLISHED"

            with psycopg.connect(dsn) as conn:
                gold_after = conn.execute(
                    "SELECT count(*) FROM normalized.customers WHERE is_current"
                ).fetchone()
            assert gold_after is not None
            assert gold_after[0] == _ROSTER_SIZE
        finally:
            pool.close()


def test_breaker_trip_quarantines_the_pass_while_transient_errors_still_raise(
    run_migrations: Callable[[str], None],
    run_dbt_build: Callable[..., object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Debug ci-pipeline-ingestion-timeout ROUND 14 (finding 18a): trip -> quarantine, not retry.

    Asserts BOTH sides of `publish_ingest`'s deterministic-vs-transient classification
    boundary (the line `errors.PublicationError`'s own docstring draws -- 'a deliberate
    business-rule rollback, not an infrastructure failure'):

    PHASE 1 (deterministic quality-gate trip -> section-51 quarantine disposition): after a
    clean wave-1 publish establishes 50 current gold keys, a deliberately-truncated later-day
    snapshot (35/50 keys -> 30% vanished > the real 0.10 threshold, the live mass-delete
    fixture's exact shape) is staged and `publish_ingest` is invoked through its REAL
    pass-claiming path. Pre-ROUND-14 this raised `QualityThresholdExceeded` out of the CLI --
    observed live (run 33062702180) burning 7 Airflow retries x 42min per poisoned DagRun
    while the runs stayed `STAGED`, re-poisoning every later pass. Post-fix it must: return
    `{"status": "QUARANTINED", ...}` (exit-0 disposition, no retries), mark the pass's runs
    terminally `QUARANTINED` (so `list_staged_run_ids` never re-claims them and discovery
    never re-offers them), and leave gold BYTE-FOR-BYTE untouched (the breaker is a
    pre-mutation barrier; the transaction rolled back before the quarantine bookkeeping).

    PHASE 2 (transient-infrastructure failure -> propagates, retry budget intact): a second
    staged pass whose publisher raises `PublicationError` (the hierarchy's own infrastructure
    class) must propagate OUT of `publish_ingest` unchanged, with the pass's runs still
    `STAGED` -- eligible for the Airflow retry that a genuinely transient failure deserves.
    """
    with PostgresContainer("postgres:18-bookworm", driver="psycopg", dbname="analytics") as pg:
        dsn = pg.get_connection_url().replace("postgresql+psycopg://", "postgresql://")
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute("CREATE ROLE etl_app LOGIN")
            cur.execute("CREATE ROLE analytics_owner LOGIN")
        run_migrations(dsn)

        pool = create_pool(dsn)
        pool.open(wait=True)
        try:
            repository = PostgresMetadataRepository(pool)

            # --- Wave 1: clean full-roster publish -> gold at 50 current --
            wave1_run_ids, _, _ = _seed_wave(repository, dsn, wave=1, file_ids=None, batch_ids=None)
            run_dbt_build(dsn, select="silver_customers")
            base = _make_context()
            with psycopg.connect(dsn) as conn, conn.transaction():
                SCDPublisher().publish(base, "silver.customers", conn, staged_run_ids=wave1_run_ids)

            def _gold_snapshot() -> list[tuple[object, ...]]:
                # `event_ts` doubles as SCD2's valid_from (migration 0035, D-03)
                # -- there is no separate valid_from column.
                with psycopg.connect(dsn) as conn:
                    return conn.execute(
                        """
                        SELECT customer_id, name, country, event_ts, valid_to, is_current
                          FROM normalized.customers
                         ORDER BY customer_id, event_ts
                        """
                    ).fetchall()

            gold_before = _gold_snapshot()
            assert sum(1 for row in gold_before if row[5]) == _ROSTER_SIZE

            # Real metadata/db wiring: publish_ingest must claim the pass
            # itself via list_staged_run_ids, not be hand-fed run ids.
            ctx = PipelineContext(
                run=RunContext(run_id=0, idempotency_key="publish:customers"),
                config=base.config,
                metadata=repository,
                objects=None,  # type: ignore[arg-type] -- publish_ingest never reads ctx.objects
                db=pool,
                log=None,  # type: ignore[arg-type] -- publish_ingest uses get_logger() internally
            )

            # --- PHASE 1: deterministic trip -> quarantine ----------------
            truncated_run_id = _seed_truncated_single_file_pass(
                repository, dsn, wave=3, kept_members=35
            )

            result = publish_ingest(ctx)

            assert result["status"] == "QUARANTINED", (
                f"a 15/50 = 30% vanished pass at threshold 10% must be quarantined, got {result!r}"
            )
            assert result["runs_quarantined"] == [truncated_run_id]
            assert result["runs_finalized"] == []
            assert _run_statuses(dsn, run_ids=[truncated_run_id]) == {"QUARANTINED"}
            dataset_id = repository.get_or_create_dataset("customers")
            assert repository.list_staged_run_ids(dataset_id=dataset_id) == [], (
                "a quarantined pass must never re-enter a later publish pass -- "
                "QUARANTINED is terminal, unlike the pre-fix STAGED wedge"
            )
            assert _gold_snapshot() == gold_before, (
                "the breaker is a PRE-mutation barrier: a quarantined pass must leave "
                "gold byte-for-byte unchanged"
            )

            # A follow-up publish pass with nothing staged is a clean no-op
            # -- the poison is gone from the dataset's pipeline entirely.
            followup = publish_ingest(ctx)
            assert followup["status"] == "SUCCEEDED"
            assert followup["runs_finalized"] == []

            # --- PHASE 2: transient infrastructure error -> raises --------
            transient_run_id = _seed_truncated_single_file_pass(
                repository, dsn, wave=4, kept_members=35
            )

            class _InfraFailingPublisher:
                name = "scd"

                def publish(self, *_args: object, **_kwargs: object) -> object:
                    msg = "simulated infrastructure failure (connection loss mid-transaction)"
                    raise PublicationError(msg)

            monkeypatch.setattr(
                "dataplat.pipeline.run.resolve_publisher",
                lambda _strategy: _InfraFailingPublisher(),
            )
            with pytest.raises(PublicationError):
                publish_ingest(ctx)

            assert _run_statuses(dsn, run_ids=[transient_run_id]) == {"STAGED"}, (
                "a transient-class failure must leave the pass STAGED -- fully eligible "
                "for the Airflow retry budget the quarantine carve-out deliberately "
                "preserves for infrastructure errors"
            )
        finally:
            pool.close()
