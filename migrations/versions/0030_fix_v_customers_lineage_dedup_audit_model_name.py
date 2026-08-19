"""Fix meta.v_customers_lineage's dedup_audit join predicate (08.1-13 live E2E finding).

Migration 0026 joined `meta.dedup_audit` on `da.model_name = 'silver_customers'`,
but the actual literal value dbt writes to `meta.dedup_audit.model_name` is
`'customers'` -- `dedup_audit_post_hook`'s own docstring (`dbt/macros/
dedup_audit_post_hook.sql` lines 96-97) is explicit that `model_name` is
populated from `target_identifier` (the calling model's *configured alias*),
and `dbt/models/silver/silver_customers.sql` calls the macro with
`target_identifier='customers'` (its `config(alias='customers')`), never
`'silver_customers'`. `silver_customers` is the dbt *model file's* name, not
the alias/`model_name` value -- migration 0026's join predicate confused the
two.

Found live: 08.1-13's own Task 2 E2E proof (`tests/e2e/slice/
test_dbt_silver_pipeline.py`) ran a real file through the whole
`discover -> stage -> dbt_build -> publish` chain and got `silver.customers`,
`normalized.customers` and `meta.dedup_audit` rows all correct (the fourth
query's own now-corrected literal, `model_name = 'customers'`, is what proved
the mismatch) -- but `meta.v_customers_lineage`'s `silver_loaded_at` /
`dbt_invocation_id` / `dbt_run_at` / `dbt_invocation_records_deduplicated`
columns were all NULL for a row that unambiguously HAD a matching, just-run
dbt invocation. The view's `LEFT JOIN meta.dedup_audit da ON da.model_name =
'silver_customers'` predicate never matched any row, because no row's
`model_name` is ever `'silver_customers'`. This is Rule 1 (auto-fix bugs) --
existing behavior is broken, not a new feature.

Same "drop + recreate the full view" pattern as migrations 0012/0026 (Postgres
has no `ALTER VIEW ... ADD COLUMN`, and no equivalent for changing a JOIN
predicate either). Only the `dedup_audit` join predicate changes; every other
column and join is copied verbatim from migration 0026's `_EXTENDED_CREATE_VIEW`.
`downgrade()` restores migration 0026's (buggy) view verbatim -- a genuine,
working reversal, matching this repo's own established convention (migration
0026's own downgrade() restores 0012's verbatim, bug-for-bug when applicable).

Revision ID: 0030
Revises: 0029
Create Date: 2026-08-19
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "0030"
down_revision = "0029"
branch_labels = None
depends_on = None

# Migration 0026's buggy view, verbatim -- used by this migration's downgrade().
_BUGGY_CREATE_VIEW = """
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
LEFT JOIN meta.dedup_audit da      ON da.model_name = 'silver_customers'
                                   AND c._run_id BETWEEN da.min_run_id AND da.max_run_id
"""

_FIXED_CREATE_VIEW = """
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


def upgrade() -> None:
    """Drop + recreate the view with the corrected `dedup_audit` join predicate."""
    op.execute("DROP VIEW meta.v_customers_lineage")
    op.execute(_FIXED_CREATE_VIEW)
    op.execute("GRANT SELECT ON meta.v_customers_lineage TO etl_app")
    op.execute("GRANT SELECT ON meta.v_customers_lineage TO grafana_reader")


def downgrade() -> None:
    """Drop + recreate migration 0026's (buggy) view verbatim -- a genuine reversal."""
    op.execute("DROP VIEW meta.v_customers_lineage")
    op.execute(_BUGGY_CREATE_VIEW)
    op.execute("GRANT SELECT ON meta.v_customers_lineage TO etl_app")
    op.execute("GRANT SELECT ON meta.v_customers_lineage TO grafana_reader")
