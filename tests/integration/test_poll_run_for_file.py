"""Integration test for `tests/e2e/slice/conftest.py::poll_run_for_file`'s ROUND 27 hardening.

debug/ci-pipeline-ingestion-timeout ROUND 27: ROUND 25's fix made the query itself correct
(prefer the earliest non-replay row when BOTH a replay and a non-replay row are visible in the
SAME poll), but the polling LOOP still returned on the FIRST iteration that found ANY row at
all -- even when that row was itself a replay. ROUND 26's own live evidence (u3's `file_id=104`
recurrence) showed a replay row can apparently be the ONLY row visible to a given poll iteration
even though a genuinely-fresh upload's file_id "should" only ever have one (non-replay) row at
first-poll time. This test proves the hardened loop closes that gap directly, against a REAL
testcontainers Postgres (not a mock/fake) -- `poll_run_for_file` issues real SQL against
`meta.ingestion_runs`, and this is the one directory in this repo where that's exercised for
real (`tests/integration/conftest.py`'s own `migrated_dsn` fixture).

Uses a real Docker Postgres (`_require_docker`/`migrated_dsn`, `tests/integration/conftest.py`)
plus a background thread that inserts the SAME file_id's non-replay row after a short, real
delay -- reproducing "only a replay row is visible for several poll iterations" without needing
a live Airflow/K8s cluster, mirroring `tests/integration/test_scd2_cross_file_tie_determinism.py`
and `test_claim_lease_split.py`'s own precedent of using a real database (not fakes) to prove
timing/ordering-sensitive claims.

`replay_of_run_id`'s own FK (migration 0004) targets `meta.ingestion_runs.run_id` directly --
NOT scoped to the same `file_id` at the database level (that invariant is an APPLICATION
convention, enforced by `dataplat.discovery`, never a constraint) -- so the "anchor" run below
can legitimately live under a throwaway `file_id=None` row without weakening what this test
proves about `poll_run_for_file`'s own SQL, which only ever inspects `replay_of_run_id IS
[NOT] NULL` for rows matching the `file_id` under test.
"""

from __future__ import annotations

import json
import threading
import time
from typing import TYPE_CHECKING

import psycopg
import pytest

from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.storage.db import create_pool
from tests.e2e.slice.conftest import poll_run_for_file

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration

_DATASET_NAME = "poll_run_for_file_round27"
_PROCESSOR_VERSION = "0.1.0"
_PROCESSOR_IMAGE = "sha256:test-processor-image"


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Insert a synthetic `meta.config_versions` row directly via SQL.

    Mirrors `tests/integration/test_discover_files.py`/`test_run_recovery_view.py`'s own
    helper of the same name/shape -- duplicated locally per this test suite's established
    per-file helper convention.
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
                "config_hash": "synthetic-hash-for-poll-run-for-file-test",
                "config_document": json.dumps({"synthetic": True}),
                "config_schema_version": 1,
            },
        ).fetchone()
        assert row is not None
        return int(row[0])


@pytest.fixture
def repository(migrated_dsn: str) -> Iterator[PostgresMetadataRepository]:
    """A `PostgresMetadataRepository` backed by an opened pool over the migrated database."""
    pool = create_pool(migrated_dsn)
    pool.open(wait=True)
    try:
        yield PostgresMetadataRepository(pool)
    finally:
        pool.close()


def _seed_dataset_and_file(
    repository: PostgresMetadataRepository, migrated_dsn: str, *, name_suffix: str
) -> tuple[int, int, int]:
    """Create a dataset/config_version/file row.

    Returns `(dataset_id, config_version_id, file_id)`.
    """
    dataset_id = repository.get_or_create_dataset(f"{_DATASET_NAME}_{name_suffix}")
    config_version_id = _insert_config_version(migrated_dsn, dataset_id=dataset_id)
    file_id = repository.create_file(
        dataset_id=dataset_id,
        object_uri=f"s3://raw/{_DATASET_NAME}/{name_suffix}.csv",
        content_sha256=f"sha-{name_suffix}".encode() * 4,
        hash_version=1,
        size_bytes=123,
        filename=f"{name_suffix}.csv",
        status="DISCOVERED",
    )
    return dataset_id, config_version_id, file_id


def test_prefers_non_replay_row_even_when_only_a_replay_is_initially_visible(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    """ROUND 27 RED/GREEN: a replay row visible FIRST must not win once the real row lands.

    Seeds the SAME file_id's replay row immediately (visible to the very first poll
    iteration) and its own non-replay ("genuine fresh upload") row only after a short, real
    delay via a background thread -- exactly the ordering ROUND 26's own live evidence showed
    can occur (a replay row the only one visible to an early poll iteration). Before this
    round's fix, `poll_run_for_file` returned on the FIRST row found, regardless of whether it
    was a replay -- it would have returned the replay row here and never seen the real one.
    The hardened loop must keep polling (within its own budget) and return the non-replay row.
    """
    dataset_id, config_version_id, file_id = _seed_dataset_and_file(
        repository, migrated_dsn, name_suffix="hardening"
    )

    # The anchor a replay's own `replay_of_run_id` points at -- deliberately under a
    # throwaway `file_id=None` row (see module docstring: the FK is not file_id-scoped).
    anchor_run_id = repository.create_ingestion_run(
        idempotency_key=f"{_DATASET_NAME}:hardening:anchor",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version=_PROCESSOR_VERSION,
        processor_image_digest=_PROCESSOR_IMAGE,
        status="SUCCEEDED",
    )

    replay_run_id = repository.create_ingestion_run(
        idempotency_key=f"{_DATASET_NAME}:hardening:replay",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version=_PROCESSOR_VERSION,
        processor_image_digest=_PROCESSOR_IMAGE,
        status="SUCCEEDED",
        file_id=file_id,
        replay_of_run_id=anchor_run_id,
    )

    original_run_id_holder: dict[str, int] = {}

    def _insert_original_after_delay() -> None:
        # Real delay, real second connection -- not a mock: at least 2 full
        # `_POLL_INTERVAL_SECONDS` (0.5s) ticks must pass with ONLY the replay row visible
        # before the genuine non-replay row lands, so the pre-fix "return on first row"
        # behavior would have already returned (and locked in) the replay row.
        time.sleep(1.5)
        original_run_id_holder["run_id"] = repository.create_ingestion_run(
            idempotency_key=f"{_DATASET_NAME}:hardening:original",
            dataset_id=dataset_id,
            config_version_id=config_version_id,
            processor_version=_PROCESSOR_VERSION,
            processor_image_digest=_PROCESSOR_IMAGE,
            status="SUCCEEDED",
            file_id=file_id,
        )

    delayed_insert = threading.Thread(target=_insert_original_after_delay, daemon=True)
    delayed_insert.start()
    try:
        with psycopg.connect(migrated_dsn) as poll_conn:
            result = poll_run_for_file(poll_conn, file_id=file_id, timeout=10)
    finally:
        delayed_insert.join(timeout=5)

    assert "run_id" in original_run_id_holder, (
        "the background delayed-insert thread never completed -- test setup itself is broken, "
        "not the assertion under test"
    )
    assert result["run_id"] == original_run_id_holder["run_id"], (
        f"poll_run_for_file returned run_id={result['run_id']!r} "
        f"(idempotency_key={result['idempotency_key']!r}) -- expected the NON-REPLAY "
        f"original run_id={original_run_id_holder['run_id']!r}, not replay_run_id="
        f"{replay_run_id!r}, even though the replay row was the only one visible for the "
        f"first ~1.5s of polling"
    )


def test_falls_back_to_newest_replay_row_when_timeout_elapses_with_no_non_replay_row(
    repository: PostgresMetadataRepository,
    migrated_dsn: str,
) -> None:
    """The ROUND 25 defensive fallback still fires when NO non-replay row ever appears.

    Seeds ONLY a replay row for `file_id` -- no non-replay row is ever inserted. The hardened
    loop must not raise (it has SOMETHING to return) and must return that replay row once its
    own `timeout` elapses, exactly like the pre-ROUND-27 behavior's own defensive path.
    """
    dataset_id, config_version_id, file_id = _seed_dataset_and_file(
        repository, migrated_dsn, name_suffix="fallback"
    )

    anchor_run_id = repository.create_ingestion_run(
        idempotency_key=f"{_DATASET_NAME}:fallback:anchor",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version=_PROCESSOR_VERSION,
        processor_image_digest=_PROCESSOR_IMAGE,
        status="SUCCEEDED",
    )
    replay_run_id = repository.create_ingestion_run(
        idempotency_key=f"{_DATASET_NAME}:fallback:replay",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version=_PROCESSOR_VERSION,
        processor_image_digest=_PROCESSOR_IMAGE,
        status="SUCCEEDED",
        file_id=file_id,
        replay_of_run_id=anchor_run_id,
    )

    with psycopg.connect(migrated_dsn) as poll_conn:
        result = poll_run_for_file(poll_conn, file_id=file_id, timeout=2)

    assert result["run_id"] == replay_run_id, (
        f"expected the defensive fallback to return the only row that ever existed "
        f"(replay_run_id={replay_run_id!r}), got run_id={result['run_id']!r}"
    )


def test_raises_when_no_row_appears_at_all(migrated_dsn: str) -> None:
    """No `meta.ingestion_runs` row for `file_id` ever -- still raises, unchanged behavior."""
    with (
        psycopg.connect(migrated_dsn) as poll_conn,
        pytest.raises(AssertionError, match="has no row for file_id"),
    ):
        poll_run_for_file(poll_conn, file_id=-1, timeout=1)
