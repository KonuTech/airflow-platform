"""Integration test proving VALID-05 (bronze->silver reconciliation) against a real `dbt build`.

Structural mirror of `tests/integration/test_dbt_dedup_audit.py` (same
`pytestmark`, same `_get_or_create_config_version`/`_seed_ingestion_run`
helper duplication convention -- this repo's own documented "duplicate
small helpers across dbt-marker test files rather than a shared module"
precedent).

Asserts D-26's "both" decision is real: a `dbt build` writes a durable,
per-file `meta.reconciliation_results` row via `reconciliation_post_hook`
(Task 1) AND runs a native `severity: warn` dbt test
(`dbt/tests/reconciliation_customers.sql`, Task 2) that surfaces as a
`warn` outcome -- never `error` -- in `dbt build`'s own JSON run-results
output when deliberately violated.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING

import psycopg
import pytest

from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    import subprocess
    from collections.abc import Callable, Iterator
    from pathlib import Path

pytestmark = [pytest.mark.dbt, pytest.mark.integration]

# tests/integration/conftest.py's own DBT_PROJECT_DIR (REPO_ROOT / "dbt") --
# duplicated here rather than imported, matching this file's own
# small-helper-duplication convention. `dbt build`'s run-results JSON
# artifact always lands at <project-dir>/target/run_results.json.
from tests.integration.conftest import DBT_PROJECT_DIR  # noqa: E402


def _get_or_create_config_version(
    conn: psycopg.Connection, *, dataset_id: int, key_suffix: str
) -> int:
    """See `test_dbt_dedup_audit.py`'s identical helper for the full rationale."""
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
    """See `test_dbt_dedup_audit.py`'s identical helper for the full rationale."""
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


def _run_results_outcome(node_name: str) -> str:
    """The `status` dbt's own `target/run_results.json` recorded for `node_name`'s last run.

    Reads the artifact `run_dbt_build`'s subprocess just wrote to
    `<DBT_PROJECT_DIR>/target/run_results.json` -- the same JSON output
    Task 2's own acceptance criteria (`severity: warn` surfaces as a `warn`
    outcome, never `error`) is stated in terms of.
    """
    run_results_path: Path = DBT_PROJECT_DIR / "target" / "run_results.json"
    run_results = json.loads(run_results_path.read_text())
    for result in run_results["results"]:
        if result["unique_id"].endswith(f".{node_name}"):
            return str(result["status"])
    msg = f"no run_results.json entry found for node {node_name!r}"
    raise AssertionError(msg)


@pytest.fixture
def repository(migrated_dsn: str) -> Iterator[PostgresMetadataRepository]:
    """A `PostgresMetadataRepository` backed by an opened pool over the migrated database."""
    pool = create_pool(migrated_dsn)
    pool.open(wait=True)
    try:
        yield PostgresMetadataRepository(pool)
    finally:
        pool.close()


def _latest_bronze_silver_row(
    dsn: str, *, dataset_name: str
) -> tuple[int, int, int, int, int, int]:
    """`(reconciliation_id, file_id, input_count, output_count, dedup_count, discrepancy)`
    for the most recent `bronze_silver` `meta.reconciliation_results` row for `dataset_name`.
    """
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            """
            SELECT rr.reconciliation_id, rr.file_id, rr.input_count, rr.output_count,
                   rr.dedup_count, rr.discrepancy
            FROM meta.reconciliation_results rr
            JOIN meta.datasets d ON d.dataset_id = rr.dataset_id
            WHERE d.dataset_name = %s AND rr.hop = 'bronze_silver'
            ORDER BY rr.reconciliation_id DESC
            LIMIT 1
            """,
            (dataset_name,),
        ).fetchone()
    assert row is not None, f"expected at least one bronze_silver row for {dataset_name!r}"
    return (int(row[0]), int(row[1]), int(row[2]), int(row[3]), int(row[4]), int(row[5]))


def test_reconciliation_post_hook_writes_a_row_with_the_correct_discrepancy_formula(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
    run_dbt_build: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    """D-22's exact formula: `discrepancy = input_count - (output_count + dedup_count)`."""
    _dataset_id, run_id, file_id, batch_id = _seed_ingestion_run(
        repository,
        migrated_dsn,
        dataset_name="customers",
        key_suffix="reconformula",
        run_number=1,
    )

    with psycopg.connect(migrated_dsn, autocommit=True) as conn:
        _insert_bronze_customer(
            conn,
            customer_id="1",
            name="Amy",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-04-01T00:00:00+00:00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"reconformula-1").digest(),
        )
        _insert_bronze_customer(
            conn,
            customer_id="2",
            name="Bob",
            country="GB",
            birth_date="1985-05-05",
            event_ts="2026-04-01T00:00:00+00:00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=2,
            record_hash=hashlib.sha256(b"reconformula-2").digest(),
        )

    run_dbt_build(migrated_dsn, select="silver_customers")

    _, written_file_id, input_count, output_count, dedup_count, discrepancy = (
        _latest_bronze_silver_row(migrated_dsn, dataset_name="customers")
    )
    assert written_file_id == file_id
    assert discrepancy == input_count - (output_count + dedup_count)
    assert discrepancy == 0


def test_reconciliation_post_hook_writes_one_row_per_file_grain(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
    run_dbt_build: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    """D-24's grain: one `bronze_silver` row per contributing `_file_id`, never per build."""
    file_ids: list[int] = []
    for i in range(3):
        _dataset_id, run_id, file_id, batch_id = _seed_ingestion_run(
            repository,
            migrated_dsn,
            dataset_name="customers",
            key_suffix="pergraincust",
            run_number=i + 1,
        )
        file_ids.append(file_id)
        with psycopg.connect(migrated_dsn, autocommit=True) as conn:
            _insert_bronze_customer(
                conn,
                customer_id=f"pg{i}",
                name=f"Name{i}",
                country="US",
                birth_date="1990-01-01",
                event_ts="2026-04-01T00:00:00+00:00",
                run_id=run_id,
                file_id=file_id,
                batch_id=batch_id,
                source_row_number=1,
                record_hash=hashlib.sha256(f"pergraincust-{i}".encode()).digest(),
            )

    before_ids = _existing_reconciliation_ids(migrated_dsn)

    run_dbt_build(migrated_dsn, select="silver_customers")

    with psycopg.connect(migrated_dsn) as conn:
        new_rows = conn.execute(
            """
            SELECT rr.file_id, rr.input_count, rr.output_count, rr.dedup_count
            FROM meta.reconciliation_results rr
            JOIN meta.datasets d ON d.dataset_id = rr.dataset_id
            WHERE d.dataset_name = 'customers' AND rr.hop = 'bronze_silver'
              AND rr.reconciliation_id != ALL(%s)
            """,
            (list(before_ids) or [-1],),
        ).fetchall()

    assert len(new_rows) == 3, f"expected exactly 3 new rows (one per file), got {new_rows}"
    written_file_ids = {row[0] for row in new_rows}
    assert written_file_ids == set(file_ids), (
        f"expected one row per seeded file_id {file_ids}, got {written_file_ids}"
    )
    assert all(row[0] is not None for row in new_rows), (
        "D-24's grain requires every bronze_silver row to carry a non-NULL file_id"
    )
    aggregate_tuples = {(row[1], row[2], row[3]) for row in new_rows}
    assert len(aggregate_tuples) == 1, (
        f"expected all 3 rows to share identical aggregate counts, got {aggregate_tuples}"
    )

    # A second, immediate re-run with no new bronze rows must write zero
    # ADDITIONAL bronze_silver rows -- the bronze_files CTE correctly scopes
    # to only newly-processed files, never re-attributing already-reconciled
    # ones.
    before_second_run = _existing_reconciliation_ids(migrated_dsn)
    run_dbt_build(migrated_dsn, select="silver_customers")
    after_second_run = _existing_reconciliation_ids(migrated_dsn)
    assert after_second_run == before_second_run, (
        "expected zero additional bronze_silver rows from a no-new-data rerun"
    )


def test_severity_warn_test_surfaces_as_warn_never_error_on_a_violated_fixture(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
    run_dbt_build: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    """D-26's 'both': the native test warns (never errors/blocks) on a deliberate mismatch.

    Deliberately uses an ALL-NUMERIC `customer_id` ("5") -- `conftest.py`'s
    session-wide, autouse `_clean_up_non_numeric_silver_business_keys`
    fixture deletes any NON-numeric-key silver row after every `dbt`-marked
    test, but never touches `staging.customers` (bronze is cumulative/
    append-only by design, migration 0022). A non-numeric key here would
    itself become a future orphaned-bronze-row source for whichever
    `dbt`-marked test runs next in the same session -- exactly the
    mechanism this docstring's own baseline note below describes.

    This test does NOT assert the first build's own outcome is `pass`:
    `tests/integration/test_dbt_dedup_audit.py`/`test_dbt_silver_dedup.py`
    (this SAME shared, session-scoped `migrated_dsn`) deliberately seed
    NON-numeric business keys for their own purposes, and the cleanup
    fixture above deletes their resulting silver rows post-test while their
    bronze rows remain forever -- a genuine, expected bronze/silver count
    mismatch this macro is correctly designed to flag, not a bug. Whatever
    that baseline discrepancy is when this test's suite-ordering happens to
    run, deliberately decrementing `output_count` by 1 from it can only
    ever make the discrepancy MORE non-zero, never mask it back to 0 -- so
    the post-corruption `warn` assertion below is robust regardless of
    whatever the pre-existing baseline was.
    """
    _dataset_id, run_id, file_id, batch_id = _seed_ingestion_run(
        repository,
        migrated_dsn,
        dataset_name="customers",
        key_suffix="reconwarn",
        run_number=1,
    )
    with psycopg.connect(migrated_dsn, autocommit=True) as conn:
        _insert_bronze_customer(
            conn,
            customer_id="5",
            name="Wanda",
            country="US",
            birth_date="1990-01-01",
            event_ts="2026-04-01T00:00:00+00:00",
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            source_row_number=1,
            record_hash=hashlib.sha256(b"reconwarn-1").digest(),
        )

    # First build: writes a real bronze_silver row for this test's own seeded
    # file (returncode 0 either way -- see docstring for why this build's own
    # warn/pass outcome is deliberately not asserted here).
    build_result = run_dbt_build(migrated_dsn, select="silver_customers")
    assert build_result.returncode == 0

    # Deliberately corrupt the most recent row so input_count != output_count + dedup_count.
    reconciliation_id, *_ = _latest_bronze_silver_row(migrated_dsn, dataset_name="customers")
    with psycopg.connect(migrated_dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE meta.reconciliation_results SET output_count = output_count - 1 "
            "WHERE reconciliation_id = %s",
            (reconciliation_id,),
        )

    # Second build: no new bronze rows, so the post-hook writes nothing new and the
    # corrupted row remains the "most recent" one the test evaluates. `dbt build`
    # must still exit 0 -- severity: warn never blocks the build.
    second_build_result = run_dbt_build(migrated_dsn, select="silver_customers")
    assert second_build_result.returncode == 0
    assert _run_results_outcome("reconciliation_customers") == "warn"


def _existing_reconciliation_ids(dsn: str) -> list[int]:
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            """
            SELECT rr.reconciliation_id
            FROM meta.reconciliation_results rr
            JOIN meta.datasets d ON d.dataset_id = rr.dataset_id
            WHERE d.dataset_name = 'customers' AND rr.hop = 'bronze_silver'
            """
        ).fetchall()
    return [row[0] for row in rows]
