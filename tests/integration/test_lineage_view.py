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
import pytest
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

# Matches every sibling tests/integration/*.py module's own
# `pytestmark = pytest.mark.integration` idiom (e.g. test_migrations.py's own
# fix, documented in its module docstring) -- without it, `-m integration`
# (this plan's own <verify> command) silently selects zero tests instead of
# the intended subset.
pytestmark = pytest.mark.integration


def _seed_silver_customer_and_dedup_audit(
    dsn: str,
    *,
    customer_id: int,
    run_id: int,
    dataset_id: int,
) -> None:
    """Seed one `silver.customers` row + a covering `meta.dedup_audit` row (raw SQL).

    Mirrors this file's own convention of seeding directly against the
    migrated schema rather than going through dbt (D-12's own bridge is
    proven at the view-SQL level here; dbt's own execution is proven
    elsewhere, plan 08.1-08).
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO silver.customers
                (customer_id, name, country, birth_date, event_ts,
                 _run_id, _file_id, _batch_id, _source_row_number, _record_hash,
                 _dbt_loaded_at)
            SELECT %s, 'Ada Lovelace', 'GB', NULL, NULL,
                   c._run_id, c._file_id, c._batch_id, c._source_row_number,
                   c._record_hash, now()
              FROM normalized.customers c
             WHERE c.customer_id = %s
             LIMIT 1
            """,
            (str(customer_id), customer_id),
        )
        conn.execute(
            """
            INSERT INTO meta.dedup_audit
                (dataset_id, dbt_invocation_id, model_name,
                 min_run_id, max_run_id, records_received, records_accepted,
                 records_deduplicated)
            VALUES (%s, %s, 'silver_customers', %s, %s, 5, 3, 2)
            """,
            (dataset_id, "22222222-2222-2222-2222-222222222222", run_id, run_id),
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

    # D-12: seed a corresponding silver row + covering dedup_audit row
    # directly via raw SQL (mirroring this file's own established seeding
    # convention) so the view's new LEFT JOINs resolve to real, non-NULL
    # data for this customer_id.
    _seed_silver_customer_and_dedup_audit(
        env.migrated_dsn,
        customer_id=customer_id,
        run_id=run_id,
        dataset_id=dataset_id,
    )

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

    # D-12: the dbt/silver hop's four new columns, populated from the
    # silver.customers + meta.dedup_audit rows seeded above.
    assert row["silver_loaded_at"] is not None
    assert row["dbt_invocation_id"] == "22222222-2222-2222-2222-222222222222"
    assert row["dbt_run_at"] is not None
    assert row["dbt_invocation_records_deduplicated"] == 2


def test_lineage_view_returns_null_dbt_hop_columns_when_no_silver_data_exists(
    env: _Env,  # noqa: F811 -- pytest fixture-injection param name, not a real redefinition
) -> None:
    """A gold row with no corresponding silver/dedup_audit data still returns a row.

    Proves the `LEFT JOIN` contract: never an error, never a dropped row --
    the four new dbt/silver-hop columns simply come back `NULL`.
    """
    customer_id = 9_300_002
    csv_bytes = _csv_bytes(1, start_id=customer_id)
    key_suffix = "lineage_no_silver"

    run_id, file_id, batch_id, object_key = _seed_pending_run(
        env,
        key_suffix=key_suffix,
        csv_bytes=csv_bytes,
    )
    dataset_id = env.metadata.get_or_create_dataset(f"run_ingest_{key_suffix}")
    ctx = PipelineContext(
        run=RunContext(
            run_id=run_id,
            idempotency_key=f"run_ingest_{key_suffix}:1",
            file_id=file_id,
            batch_id=batch_id,
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

    # Deliberately no silver.customers/meta.dedup_audit seeding -- this
    # customer_id has no corresponding silver history.
    with psycopg.connect(env.migrated_dsn, row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT * FROM meta.v_customers_lineage WHERE customer_id = %s",
            (customer_id,),
        ).fetchone()

    assert row is not None, f"no meta.v_customers_lineage row for customer_id={customer_id}"
    assert row["silver_loaded_at"] is None
    assert row["dbt_invocation_id"] is None
    assert row["dbt_run_at"] is None
    assert row["dbt_invocation_records_deduplicated"] is None


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
