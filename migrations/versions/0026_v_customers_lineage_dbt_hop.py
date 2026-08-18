"""meta.v_customers_lineage — bridge the dbt/silver hop (D-12, extends OBS-07).

Extends the OBS-07 lineage view (migration 0012) across the new dbt/silver
hop this phase introduces: a `LEFT JOIN silver.customers` (the current
silver-owned business-key winner, if one exists for this gold row's
`customer_id`) and a `LEFT JOIN meta.dedup_audit` (which dbt invocation's
audit summary covered the bronze run that fed this gold row).

Both new joins are `LEFT JOIN`, never plain `JOIN`:

- `silver.customers`: a gold row published before this phase shipped (or
  before dbt's first run since) has no corresponding silver history to join
  against. This is also a *business-key* match (`sc.customer_id =
  c.customer_id::text`), not a lineage-column match — `_record_hash` alone
  cannot distinguish "the current silver winner for this key" from an older,
  already-superseded silver write, so no stronger join predicate is
  available or needed here.
- `meta.dedup_audit`: resolves via a *range* join
  (`c._run_id BETWEEN da.min_run_id AND da.max_run_id`), not an equality
  join, per D-05's watermark-driven dbt batching — one `dbt build`
  invocation can span multiple bronze commits, so `meta.dedup_audit`'s own
  schema-shape correction (migration 0024's docstring, Pattern 2) uses a
  `min_run_id`/`max_run_id` range instead of a single FK. A gold row whose
  covering bronze run predates dbt's first invocation naturally finds no
  matching range and returns `NULL`, consistent with the `LEFT JOIN`
  contract.

The new `dbt_invocation_records_deduplicated` column directly strengthens
DEDUP-04's "enough information to explain why a record was removed": a
viewer of any gold row can see, in the SAME query, how many sibling rows the
covering dbt invocation deduplicated away.

`dbt_app` is deliberately NOT granted `SELECT` here (planner's own flagged
call in 08.1-PATTERNS.md, resolved as "no") — it has no legitimate need to
read its own lineage bridge. Grants stay exactly `etl_app`/`grafana_reader`,
identical to migration 0012's own trailing two `GRANT` statements.

Postgres has no `ALTER VIEW ... ADD COLUMN`, so this migration follows
migration 0012's own established pattern: `DROP VIEW` then re-`CREATE VIEW`
with the full SELECT list, not an incremental change. `downgrade()` restores
migration 0012's ORIGINAL view verbatim — a genuine, working downgrade.

Revision ID: 0026
Revises: 0025
Create Date: 2026-08-18
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "0026"
down_revision = "0025"
branch_labels = None
depends_on = None

# Migration 0012's ORIGINAL view, verbatim — used by both this migration's
# downgrade() and as the base this upgrade() extends.
_ORIGINAL_CREATE_VIEW = """
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
    sv.schema_hash
FROM normalized.customers c
JOIN meta.ingestion_runs r        ON r.run_id = c._run_id
JOIN meta.files f                 ON f.file_id = c._file_id
JOIN meta.batches b                ON b.batch_id = c._batch_id
JOIN meta.config_versions cv       ON cv.config_version_id = r.config_version_id
LEFT JOIN meta.schema_versions sv  ON sv.schema_version_id = r.schema_version_id
"""

_EXTENDED_CREATE_VIEW = """
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


def upgrade() -> None:
    """Drop + recreate the view, bridging the dbt/silver hop (D-12)."""
    op.execute("DROP VIEW meta.v_customers_lineage")
    op.execute(_EXTENDED_CREATE_VIEW)
    op.execute("GRANT SELECT ON meta.v_customers_lineage TO etl_app")
    op.execute("GRANT SELECT ON meta.v_customers_lineage TO grafana_reader")


def downgrade() -> None:
    """Drop + recreate migration 0012's ORIGINAL view, verbatim — a genuine reversal."""
    op.execute("DROP VIEW meta.v_customers_lineage")
    op.execute(_ORIGINAL_CREATE_VIEW)
    op.execute("GRANT SELECT ON meta.v_customers_lineage TO etl_app")
    op.execute("GRANT SELECT ON meta.v_customers_lineage TO grafana_reader")
