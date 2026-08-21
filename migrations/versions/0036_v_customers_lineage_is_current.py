"""meta.v_customers_lineage -- filter to the current SCD2 version per customer_id (D-08, SCD-03).

Migration 0035 (plan 10-01) moved `normalized.customers` from "exactly one
row per `customer_id`" to "one or more rows per `customer_id`, one of which
is the current version (`is_current`)". `meta.v_customers_lineage`
(migrations 0012/0026/0030's join chain) was built and has always been
queried under the former assumption -- once the SCD Publisher (plan 10-04)
actually starts writing multi-version gold state, this view would silently
start returning one row PER SCD2 VERSION instead of one row per customer, a
regression for every consumer that expects "current lineage for this
customer" (`grafana_reader`'s dashboards, any ad-hoc lineage query).

This migration adds exactly one change: `AND c.is_current` folded into the
view's base `FROM normalized.customers c` filter, so the view returns AT
MOST one row per `customer_id` -- the current SCD2 version -- never a
historical one. Every other column and join is copied verbatim from
migration 0030's `_FIXED_CREATE_VIEW` text. Historical versions remain
queryable directly against `normalized.customers` by `etl_app` (T-10-10,
this plan's threat model); this view is a reporting surface, not the only
way to reach SCD2 history.

Same "drop + recreate the full view" pattern as migrations 0012/0026/0030
(Postgres has no `ALTER VIEW` primitive for changing a `WHERE` filter).
`downgrade()` restores migration 0030's view verbatim -- a genuine, working
reversal, matching this repo's own established convention.

Revision ID: 0036
Revises: 0035
Create Date: 2026-08-21
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "0036"
down_revision = "0035"
branch_labels = None
depends_on = None

# Migration 0030's fixed (but pre-this-change) view, verbatim -- used by this
# migration's downgrade() as a genuine reversal.
_PRIOR_CREATE_VIEW = """
CREATE VIEW meta.v_customers_lineage AS
SELECT
    c.id                       AS customer_row_id,
    c.customer_id,
    c._source_row_number,
    c._record_hash,
    c._record_hash_version,
    c._ingested_at,
    f.file_id,
    f.object_uri,
    f.content_sha256,
    f.hash_version             AS file_hash_version,
    f.filename,
    f.business_date,
    b.batch_id,
    b.batch_key,
    r.run_id,
    r.idempotency_key,
    r.dag_id,
    r.dag_run_id,
    r.task_id,
    r.map_index,
    r.try_number,
    r.k8s_namespace,
    r.k8s_pod_name,
    r.trace_id,
    r.span_id,
    r.processor_version,
    r.processor_image_digest,
    r.started_at                AS run_started_at,
    r.finished_at                AS run_finished_at,
    cv.version                   AS config_version,
    cv.config_hash,
    sv.version                   AS schema_version,
    sv.schema_hash,
    sc._dbt_loaded_at            AS silver_loaded_at,
    da.dbt_invocation_id,
    da.run_at                    AS dbt_run_at,
    da.records_deduplicated      AS dbt_invocation_records_deduplicated
FROM normalized.customers c
JOIN meta.ingestion_runs r        ON r.run_id = c._run_id
JOIN meta.files f                 ON f.file_id = c._file_id
JOIN meta.batches b                ON b.batch_id = c._batch_id
JOIN meta.config_versions cv       ON cv.config_version_id = r.config_version_id
LEFT JOIN meta.schema_versions sv  ON sv.schema_version_id = r.schema_version_id
LEFT JOIN silver.customers sc      ON sc.customer_id = c.customer_id::text
LEFT JOIN meta.dedup_audit da      ON da.model_name = 'customers'
                                   AND c._run_id BETWEEN da.min_run_id AND da.max_run_id
"""

# This migration's view -- identical to _PRIOR_CREATE_VIEW except the base
# FROM clause's filter now excludes historical (non-current) SCD2 versions.
_CURRENT_CREATE_VIEW = """
CREATE VIEW meta.v_customers_lineage AS
SELECT
    c.id                       AS customer_row_id,
    c.customer_id,
    c._source_row_number,
    c._record_hash,
    c._record_hash_version,
    c._ingested_at,
    f.file_id,
    f.object_uri,
    f.content_sha256,
    f.hash_version             AS file_hash_version,
    f.filename,
    f.business_date,
    b.batch_id,
    b.batch_key,
    r.run_id,
    r.idempotency_key,
    r.dag_id,
    r.dag_run_id,
    r.task_id,
    r.map_index,
    r.try_number,
    r.k8s_namespace,
    r.k8s_pod_name,
    r.trace_id,
    r.span_id,
    r.processor_version,
    r.processor_image_digest,
    r.started_at                AS run_started_at,
    r.finished_at                AS run_finished_at,
    cv.version                   AS config_version,
    cv.config_hash,
    sv.version                   AS schema_version,
    sv.schema_hash,
    sc._dbt_loaded_at            AS silver_loaded_at,
    da.dbt_invocation_id,
    da.run_at                    AS dbt_run_at,
    da.records_deduplicated      AS dbt_invocation_records_deduplicated
FROM normalized.customers c
JOIN meta.ingestion_runs r        ON r.run_id = c._run_id
JOIN meta.files f                 ON f.file_id = c._file_id
JOIN meta.batches b                ON b.batch_id = c._batch_id
JOIN meta.config_versions cv       ON cv.config_version_id = r.config_version_id
LEFT JOIN meta.schema_versions sv  ON sv.schema_version_id = r.schema_version_id
LEFT JOIN silver.customers sc      ON sc.customer_id = c.customer_id::text
LEFT JOIN meta.dedup_audit da      ON da.model_name = 'customers'
                                   AND c._run_id BETWEEN da.min_run_id AND da.max_run_id
WHERE c.is_current
"""


def upgrade() -> None:
    """Drop + recreate the view filtered to each customer_id's current SCD2 version."""
    op.execute("DROP VIEW meta.v_customers_lineage")
    op.execute(_CURRENT_CREATE_VIEW)
    op.execute("GRANT SELECT ON meta.v_customers_lineage TO etl_app")
    op.execute("GRANT SELECT ON meta.v_customers_lineage TO grafana_reader")


def downgrade() -> None:
    """Drop + recreate migration 0030's (pre-``is_current``-filter) view verbatim -- a reversal."""
    op.execute("DROP VIEW meta.v_customers_lineage")
    op.execute(_PRIOR_CREATE_VIEW)
    op.execute("GRANT SELECT ON meta.v_customers_lineage TO etl_app")
    op.execute("GRANT SELECT ON meta.v_customers_lineage TO grafana_reader")
