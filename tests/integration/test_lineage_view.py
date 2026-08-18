"""Integration test for `meta.v_customers_lineage` (OBS-07).

Proves the view answers OBS-07's literal wording: a platform operator can
run one SQL query and get a row's source file, object path, checksum,
batch, ingestion timestamp, DAG/run/task ID, processor version, schema
version and config version -- for a row genuinely published by
`dataplat.pipeline.run.stage_ingest()` + `publish_ingest()`, not a
hand-seeded row.

Originally written against the single `run_ingest` function; migrated here
after plan 08.1-10 split it into `stage_ingest` (claim, stage, quality-gate,
promote to durable bronze, mark `STAGED`) and `publish_ingest` (claim every
currently-`STAGED` run for one dataset, publish `silver.<dataset>` into
`normalized.<dataset>`, finalize). `publish_ingest` reads FROM
`silver.<dataset>`, never from bronze directly, so a genuine publish now
needs a `silver.customers` row to exist first -- this file already seeded
`silver.customers` directly via SQL (D-12, never via a real `dbt build`;
this test proves the view-SQL bridge, dbt's own execution is proven
separately, plan 08.1-08), so that seed now runs BEFORE `publish_ingest`
rather than after, and carries the row's own real lineage FKs directly
instead of copying them from an already-published `normalized.customers`
row (there is none yet at that point). Both `stage_ingest`/`publish_ingest`
calls use the shared "customers" dataset (`_make_config`'s own hardcoded
`ctx.config.dataset`) for the seeded run's own `meta.datasets` FK too --
`publish_ingest` resolves `ctx.config.dataset` to a `dataset_id` and looks
up currently-`STAGED` runs by THAT id (`list_staged_run_ids`), so a run
created under any other dataset name would never be found as staged.

Reuses `test_run_ingest.py`'s own `env`/`_seed_pending_run` fixture chain
(imported, not reimplemented) rather than hand-writing raw INSERT statements
across five tables -- the existing chain already produces one genuinely
published `normalized.customers` row with every lineage column
(`_run_id`/`_file_id`/`_batch_id`) populated exactly as production code
populates them, a stronger and lower-drift-risk proof than a hand-seeded
row. The context is built with `CsvSource(..., dataset_id=...)` -- mirroring
`test_run_ingest.py::test_staged_run_records_its_resolved_schema_version_on_the_run`'s
own precedent, not the shared `_make_ctx` helper, which deliberately omits
`dataset_id` and therefore skips schema resolution -- because this test
needs `schema_version_id` genuinely populated to prove the view's
`schema_version`/`schema_hash` columns are not vacuously NULL.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime

import psycopg
import pytest
from psycopg.rows import dict_row

from csv_processor.source import CsvSource
from dataplat.models.identity import RunContext
from dataplat.observability.logging import get_logger
from dataplat.pipeline.protocol import PipelineContext
from dataplat.pipeline.run import publish_ingest, stage_ingest
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


def _seed_silver_customer_row(  # noqa: PLR0913 -- one keyword per silver/lineage identity value
    dsn: str,
    *,
    customer_id: int,
    run_id: int,
    file_id: int,
    batch_id: int,
    dbt_loaded: bool,
) -> None:
    """Seed one `silver.customers` row directly via raw SQL -- never via a real `dbt build`.

    Post-08.1-10, this row is no longer purely a lineage-enrichment side seed: it is what
    `publish_ingest` itself reads to actually publish this `customer_id` into
    `normalized.customers` (D-12's own bridge is proven at the view-SQL level here; dbt's
    own execution is proven elsewhere, plan 08.1-08). `dbt_loaded=False` leaves
    `_dbt_loaded_at` NULL -- a silver row must structurally exist for a real publish to
    happen, but this represents "this row's own silver history was never actually
    dbt-processed", for the no-dbt-hop-data test below.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO silver.customers (
                customer_id, name, country, birth_date, event_ts,
                _run_id, _file_id, _batch_id, _source_row_number, _record_hash,
                _dbt_loaded_at
            ) VALUES (%s, 'Ada Lovelace', 'GB', NULL, NULL, %s, %s, %s, 1, %s, %s)
            """,
            (
                str(customer_id),
                run_id,
                file_id,
                batch_id,
                hashlib.sha256(f"lineage-silver-{customer_id}".encode()).digest(),
                datetime.now(UTC) if dbt_loaded else None,
            ),
        )


def _seed_dedup_audit(dsn: str, *, dataset_id: int, run_id: int) -> None:
    """Seed one covering `meta.dedup_audit` row directly via raw SQL."""
    with psycopg.connect(dsn, autocommit=True) as conn:
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
        dataset_name="customers",
    )
    # `_seed_pending_run` created the run's FK dataset as "customers" above
    # (needed for `publish_ingest` to find this run as staged) --
    # re-resolving it is idempotent (INSERT ... ON CONFLICT DO UPDATE) and
    # returns the same `dataset_id`, needed here (unlike `_make_ctx`) so
    # `CsvSource` actually runs schema resolution.
    dataset_id = env.metadata.get_or_create_dataset("customers")
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

    stage_receipt = stage_ingest(ctx)
    assert stage_receipt.status == "STAGED"

    # D-12: seed a corresponding silver row (this run's real publish source,
    # post-08.1-10) + a covering dedup_audit row directly via raw SQL, so
    # both `publish_ingest` has something to publish AND the view's dbt-hop
    # LEFT JOINs resolve to real, non-NULL data for this customer_id.
    _seed_silver_customer_row(
        env.migrated_dsn,
        customer_id=customer_id,
        run_id=run_id,
        file_id=file_id,
        batch_id=batch_id,
        dbt_loaded=True,
    )
    _seed_dedup_audit(env.migrated_dsn, dataset_id=dataset_id, run_id=run_id)

    publish_result = publish_ingest(ctx)
    assert publish_result["status"] == "SUCCEEDED"
    assert run_id in publish_result["runs_finalized"]

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
    # A literal expected value isn't safe here anymore: "customers" is now a
    # SHARED, session-wide dataset (needed for `publish_ingest` to find this
    # run as staged -- module docstring), so `meta.config_versions`'s CURRENT
    # row for it may already have been created by an earlier-running test
    # (`_insert_config_version`'s own get-or-reuse pattern) with a different
    # hash. OBS-07's own requirement is only that the view SURFACES a
    # config_hash, not any particular literal value.
    assert row["config_hash"] is not None
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
    """A gold row whose own silver history was never dbt-processed still returns a row.

    Proves the `LEFT JOIN` contract: never an error, never a dropped row --
    the four new dbt/silver-hop columns simply come back `NULL`. Post-08.1-10, a silver
    row must structurally exist for `publish_ingest` to publish this customer_id at all
    (unlike the pre-split flow, where silver was a purely optional lineage-enrichment
    seed) -- so "no silver data" is now represented as a silver row with `_dbt_loaded_at
    IS NULL` and no covering `meta.dedup_audit` row, rather than no silver row at all.
    """
    customer_id = 9_300_002
    csv_bytes = _csv_bytes(1, start_id=customer_id)
    key_suffix = "lineage_no_silver"

    run_id, file_id, batch_id, object_key = _seed_pending_run(
        env,
        key_suffix=key_suffix,
        csv_bytes=csv_bytes,
        dataset_name="customers",
    )
    dataset_id = env.metadata.get_or_create_dataset("customers")
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

    stage_receipt = stage_ingest(ctx)
    assert stage_receipt.status == "STAGED"

    # A silver row must exist for publish_ingest to publish anything at all
    # (see module/function docstrings) -- seeded here with `_dbt_loaded_at`
    # left NULL and deliberately no meta.dedup_audit row, so the view's
    # dbt-hop columns still resolve to NULL for this customer_id.
    _seed_silver_customer_row(
        env.migrated_dsn,
        customer_id=customer_id,
        run_id=run_id,
        file_id=file_id,
        batch_id=batch_id,
        dbt_loaded=False,
    )

    publish_result = publish_ingest(ctx)
    assert publish_result["status"] == "SUCCEEDED"
    assert run_id in publish_result["runs_finalized"]

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
