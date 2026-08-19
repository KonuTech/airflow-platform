"""Integration tests for `meta.v_run_recovery` (LOAD-06, migration 0033, plan 09-06).

Proves the view answers LOAD-06's "what succeeded, what remains, retry-or-rollback" question
across all 3 pipeline stages (`STAGE_LOAD`, `DBT_BUILD`, `PUBLISH`) purely from directly-seeded
`meta.ingestion_runs`/`meta.run_stages` rows -- no live DAG or dbt invocation needed, exactly as
the plan's own acceptance criteria describe, proving the view's join logic independent of plan
09-09's DAG wiring.

Mirrors `test_watermarks.py`'s own fixture/helper shape (`env`/`_Env`, `_pool`,
`_insert_config_version`) -- duplicated locally rather than imported, matching this test suite's
established per-file helper convention.

D-15 (retry-only recovery, rollback structurally never applies): Test 5 asserts directly, across
every scenario exercised by Tests 1-4, that the substring "rollback" never appears in any observed
`next_action` value -- not merely implied by which CASE branches happen to be exercised.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import psycopg
import pytest

from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.storage.db import create_pool

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration


def _insert_config_version(dsn: str, *, dataset_id: int) -> int:
    """Get-or-insert a synthetic, CURRENT `meta.config_versions` row (mirrors test_watermarks.py)."""  # noqa: E501, W505
    with psycopg.connect(dsn) as conn:
        existing = conn.execute(
            """
            SELECT config_version_id
              FROM meta.config_versions
             WHERE dataset_id = %(dataset_id)s AND valid_to IS NULL
            """,
            {"dataset_id": dataset_id},
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
                "config_hash": "synthetic-hash-for-run-recovery-test",
                "config_document": json.dumps({"synthetic": True}),
                "config_schema_version": 1,
            },
        ).fetchone()
        assert row is not None
        return int(row[0])


def _seed_run_stage(dsn: str, *, run_id: int, stage_name: str, status: str) -> None:
    """Insert one `meta.run_stages` row directly via raw SQL -- no `claim_run_stage` needed.

    `claim_run_stage` is gated on its owning run's own `status = 'STAGED'`, which would force
    every scenario onto the same run status. This view test needs independent control over
    `run_stages.status` per stage, so it seeds directly, matching the plan's own guidance.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO meta.run_stages (run_id, stage_name, status)
            VALUES (%s, %s, %s)
            """,
            (run_id, stage_name, status),
        )


@dataclass
class _Env:
    metadata: PostgresMetadataRepository
    migrated_dsn: str


@pytest.fixture
def _pool(migrated_dsn: str) -> Iterator[Any]:
    opened_pool = create_pool(migrated_dsn)
    opened_pool.open(wait=True)
    try:
        yield opened_pool
    finally:
        opened_pool.close()


@pytest.fixture
def env(_pool: Any, migrated_dsn: str) -> _Env:
    return _Env(metadata=PostgresMetadataRepository(_pool), migrated_dsn=migrated_dsn)


def _seed_run(env: _Env, *, key_suffix: str, run_status: str) -> tuple[int, int]:
    """Create a dataset/config_version/`meta.ingestion_runs` row with the given `status`.

    Returns `(dataset_id, run_id)`. `file_id`/`batch_id` are deliberately omitted (both
    nullable on `meta.ingestion_runs`, migration 0004) -- this view's columns never read
    them, so seeding them would be dead setup.
    """
    dataset_id = env.metadata.get_or_create_dataset("run-recovery-test")
    config_version_id = _insert_config_version(env.migrated_dsn, dataset_id=dataset_id)
    run_id = env.metadata.create_ingestion_run(
        idempotency_key=f"run-recovery:{key_suffix}",
        dataset_id=dataset_id,
        config_version_id=config_version_id,
        processor_version="0.1.0",
        processor_image_digest="sha256:testdigest",
        status=run_status,
    )
    return dataset_id, run_id


# --- Test 1: every stage SUCCEEDED + run SUCCEEDED -> 'complete' -----------


def test_all_stages_succeeded_reports_complete(env: _Env) -> None:
    _, run_id = _seed_run(env, key_suffix="all-succeeded", run_status="SUCCEEDED")
    for stage_name in ("STAGE_LOAD", "DBT_BUILD", "PUBLISH"):
        _seed_run_stage(env.migrated_dsn, run_id=run_id, stage_name=stage_name, status="SUCCEEDED")

    status = env.metadata.get_run_recovery_status(run_id=run_id)
    assert status is not None
    assert status["next_action"] == "complete"


# --- Test 2: STAGE_LOAD SUCCEEDED, no DBT_BUILD row -> retry DBT_BUILD -----


def test_missing_dbt_build_row_reports_retry_dbt_build(env: _Env) -> None:
    _, run_id = _seed_run(env, key_suffix="missing-dbt-build", run_status="RUNNING")
    _seed_run_stage(env.migrated_dsn, run_id=run_id, stage_name="STAGE_LOAD", status="SUCCEEDED")

    status = env.metadata.get_run_recovery_status(run_id=run_id)
    assert status is not None
    assert status["next_action"] == "retry stage DBT_BUILD"


# --- Test 3: no run_stages rows at all -> retry STAGE_LOAD ------------------


def test_no_stage_rows_reports_retry_stage_load(env: _Env) -> None:
    _, run_id = _seed_run(env, key_suffix="no-stage-rows", run_status="RUNNING")

    status = env.metadata.get_run_recovery_status(run_id=run_id)
    assert status is not None
    assert status["next_action"] == "retry stage STAGE_LOAD"


# --- Test 4: STAGE_LOAD/DBT_BUILD SUCCEEDED, PUBLISH FAILED -> retry PUBLISH


def test_failed_publish_reports_retry_publish(env: _Env) -> None:
    _, run_id = _seed_run(env, key_suffix="failed-publish", run_status="RUNNING")
    _seed_run_stage(env.migrated_dsn, run_id=run_id, stage_name="STAGE_LOAD", status="SUCCEEDED")
    _seed_run_stage(env.migrated_dsn, run_id=run_id, stage_name="DBT_BUILD", status="SUCCEEDED")
    _seed_run_stage(env.migrated_dsn, run_id=run_id, stage_name="PUBLISH", status="FAILED")

    status = env.metadata.get_run_recovery_status(run_id=run_id)
    assert status is not None
    assert status["next_action"] == "retry stage PUBLISH"


# --- Test 5: 'rollback' never appears in any next_action across all 4 above


def test_next_action_never_implies_rollback(env: _Env) -> None:
    all_succeeded = [
        ("STAGE_LOAD", "SUCCEEDED"),
        ("DBT_BUILD", "SUCCEEDED"),
        ("PUBLISH", "SUCCEEDED"),
    ]
    load_then_dbt_succeeded_publish_failed = [
        ("STAGE_LOAD", "SUCCEEDED"),
        ("DBT_BUILD", "SUCCEEDED"),
        ("PUBLISH", "FAILED"),
    ]
    scenarios: list[tuple[str, str, list[tuple[str, str]]]] = [
        ("rollback-check-complete", "SUCCEEDED", all_succeeded),
        ("rollback-check-missing-dbt", "RUNNING", [("STAGE_LOAD", "SUCCEEDED")]),
        ("rollback-check-no-stages", "RUNNING", []),
        ("rollback-check-failed-publish", "RUNNING", load_then_dbt_succeeded_publish_failed),
    ]
    for key_suffix, run_status, stages in scenarios:
        _, run_id = _seed_run(env, key_suffix=key_suffix, run_status=run_status)
        for stage_name, stage_status in stages:
            _seed_run_stage(
                env.migrated_dsn,
                run_id=run_id,
                stage_name=stage_name,
                status=stage_status,
            )

        status = env.metadata.get_run_recovery_status(run_id=run_id)
        assert status is not None
        next_action = str(status["next_action"])
        assert "rollback" not in next_action.lower()


# --- Bonus: a nonexistent run_id returns None, mirroring get_run_stage_status


def test_nonexistent_run_id_returns_none(env: _Env) -> None:
    assert env.metadata.get_run_recovery_status(run_id=-1) is None
