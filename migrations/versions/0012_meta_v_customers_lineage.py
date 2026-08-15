"""meta.v_customers_lineage — the OBS-07 lineage view, one query for the full chain.

Delivers OBS-07 completely: a platform operator can run one SQL query
(`SELECT * FROM meta.v_customers_lineage WHERE customer_id = ...`) and get
that row's source file, object path, checksum, batch, ingestion timestamp,
DAG/run/task ID, processor version, schema version and config version — the
requirement's literal wording, satisfied by a single wide view joining
`normalized.customers`'s already-embedded lineage columns (`_run_id`/
`_file_id`/`_batch_id`, per ARCHITECTURE.md §2.3's "embed lineage as columns
on target tables" design) out to the full `meta.*` chain (`ingestion_runs` ->
`files` -> `batches` -> `config_versions` -> `schema_versions`), per
07-CONTEXT.md D-13. Zero new schema was needed to build this — every named
column already exists (migrations 0002/0003/0004/0005/0009); this migration
only adds the view.

Must chain after migration 0011 (`down_revision = "0011"`): this view grants
SELECT to `grafana_reader`, which 0011 creates. Every join except the one to
`meta.schema_versions` is a plain `JOIN`, never `LEFT JOIN`: `_run_id`/
`_file_id`/`_batch_id`/`config_version_id` are all `NOT NULL` on their
respective tables (verified directly against migrations 0004/0005's own
DDL). The join to `meta.schema_versions` is a `LEFT JOIN` because
`meta.ingestion_runs.schema_version_id` is nullable by migration 0004's own
deliberate, documented design (closed by migration 0009, still nullable).

Security Domain finding (07-RESEARCH.md): the raw-exception JSONB column on
`meta.ingestion_runs` — which may carry stack/context text — is deliberately
excluded from this view's column list. It must never reach a dashboard/view
without the same redaction discipline `observability/logging.py`'s
`_redact()` already applies at the logging layer.

Revision ID: 0012
Revises: 0011
Create Date: 2026-08-16
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "0012"
down_revision = "0011"
branch_labels = None
depends_on = None

_CREATE_VIEW = """
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


def upgrade() -> None:
    """Create the lineage view, then grant SELECT to `etl_app` and `grafana_reader`."""
    op.execute(_CREATE_VIEW)
    op.execute("GRANT SELECT ON meta.v_customers_lineage TO etl_app")
    op.execute("GRANT SELECT ON meta.v_customers_lineage TO grafana_reader")


def downgrade() -> None:
    """Drop the lineage view."""
    op.execute("DROP VIEW meta.v_customers_lineage")
