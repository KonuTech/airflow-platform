"""Integration test for `meta.v_customers_lineage` (OBS-07).

Proves the view answers OBS-07's literal wording: a platform operator can
run one SQL query and get a row's source file, object path, checksum,
batch, ingestion timestamp, DAG/run/task ID, processor version, schema
version and config version -- for a row genuinely published by
`dataplat.pipeline.run.run_ingest()`, not a hand-seeded row.

Reuses `test_run_ingest.py`'s own `env`/`_seed_pending_run` fixture chain
(imported, not reimplemented) rather than hand-writing raw INSERT statements
across five tables -- the existing chain already produces one genuinely
published `normalized.customers` row with every lineage column
(`_run_id`/`_file_id`/`_batch_id`) populated exactly as production code
populates them, a stronger and lower-drift-risk proof than a hand-seeded
row. The context is built with `CsvSource(..., dataset_id=...)` -- mirroring
`test_run_ingest.py::test_successful_run_records_its_resolved_schema_version_on_the_run`'s
own precedent, not the shared `_make_ctx` helper, which deliberately omits
`dataset_id` and therefore skips schema resolution -- because this test
needs `schema_version_id` genuinely populated to prove the view's
`schema_version`/`schema_hash` columns are not vacuously NULL.
"""

from __future__ import annotations

import hashlib

import psycopg
from psycopg.rows import dict_row

from csv_processor.source import CsvSource
from dataplat.models.identity import RunContext
from dataplat.observability.logging import get_logger
from dataplat.pipeline.protocol import PipelineContext
from dataplat.pipeline.run import run_ingest
from tests.integration.test_run_ingest import (  # noqa: F401 -- re-exported as pytest fixtures
    _csv_bytes,
    _Env,
    _make_config,
    _pool,
    _scratch_bucket,
    _seed_pending_run,
    _validated_bucket,
    env,
)


def test_lineage_view_returns_every_obs_07_named_column_for_a_published_row(
    env: _Env,  # noqa: F811 -- pytest fixture-injection param name, not a real redefinition
) -> None:
    customer_id = 9_300_001
    csv_bytes = _csv_bytes(1, start_id=customer_id)

    run_id, file_id, batch_id, object_key = _seed_pending_run(
        env,
        key_suffix="lineage",
        csv_bytes=csv_bytes,
    )
    # `_seed_pending_run` created the dataset under this name -- re-resolving
    # it is idempotent (INSERT ... ON CONFLICT DO UPDATE) and returns the
    # same `dataset_id`, needed here (unlike `_make_ctx`) so `CsvSource`
    # actually runs schema resolution.
    dataset_id = env.metadata.get_or_create_dataset("run_ingest_lineage")
    ctx = PipelineContext(
        run=RunContext(
            run_id=run_id,
            idempotency_key="run_ingest_lineage:1",
            file_id=file_id,
            batch_id=batch_id,
            # OBS-07 gap closure (07-09): values representative of what a
            # real Airflow-triggered run would carry -- this test proves the
            # RunContext -> claim_ingestion_run -> view plumbing this plan's
            # Task 1 builds, independent of the pod-boundary env-var
            # mechanism Task 2 builds, which gets its own live proof in
            # Task 3.
            dag_id="csv_ingest_customers",
            dag_run_id="manual__2026-01-01T00:00:00+00:00",
            task_id="ingest",
            map_index=7,
            k8s_namespace="etl",
        ),
        config=_make_config(),
        metadata=env.metadata,
        objects=env.objects,
        db=env.pool,
        log=get_logger(),
        source=CsvSource(bucket=env.scratch_bucket, key=object_key, dataset_id=dataset_id),
    )

    receipt = run_ingest(ctx)
    assert receipt.status == "SUCCEEDED"

    expected_content_sha256 = hashlib.sha256(csv_bytes).digest()

    with psycopg.connect(env.migrated_dsn, row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT * FROM meta.v_customers_lineage WHERE customer_id = %s",
            (customer_id,),
        ).fetchone()

    assert row is not None, f"no meta.v_customers_lineage row for customer_id={customer_id}"

    # OBS-07's literal wording: source file, object path, checksum, batch,
    # ingestion timestamp, DAG/run/task ID, processor version, schema
    # version and config version -- every one present in this single query.
    assert row["run_id"] == run_id
    assert row["file_id"] == file_id
    assert row["batch_id"] == batch_id
    assert row["object_uri"] == f"s3://{env.scratch_bucket}/{object_key}"
    assert bytes(row["content_sha256"]) == expected_content_sha256
    assert row["file_hash_version"] == 1
    assert row["run_started_at"] is not None
    assert row["run_finished_at"] is not None
    assert row["processor_version"] == "0.1.0"
    assert row["processor_image_digest"] == "sha256:testdigest"
    assert row["config_version"] is not None
    assert row["config_hash"] == "synthetic-hash-for-test"
    assert row["schema_version"] is not None
    assert row["schema_hash"] is not None
    # OBS-07 gap closure (07-09): these are now populated because RunContext
    # was constructed with them above -- claim_ingestion_run() persists them
    # in the same UPDATE as trace_id/span_id, and the view SELECTs them
    # straight through from meta.ingestion_runs.
    assert row["dag_id"] == "csv_ingest_customers"
    assert row["dag_run_id"] == "manual__2026-01-01T00:00:00+00:00"
    assert row["task_id"] == "ingest"
    assert row["map_index"] == 7
    assert row["k8s_namespace"] == "etl"

    # error_detail must never surface through this view (Security Domain
    # finding, migration 0012's own docstring) -- assert the raw exception
    # JSONB column is not even a key in the fetched row.
    assert "error_detail" not in row


def test_lineage_view_is_absent_for_an_unpublished_customer_id(
    env: _Env,  # noqa: F811 -- pytest fixture-injection param name, not a real redefinition
) -> None:
    """A `customer_id` no run has ever published returns zero rows, not an error."""
    with psycopg.connect(env.migrated_dsn) as conn:
        rows = conn.execute(
            "SELECT * FROM meta.v_customers_lineage WHERE customer_id = %s",
            (9_300_999,),
        ).fetchall()
    assert rows == []
