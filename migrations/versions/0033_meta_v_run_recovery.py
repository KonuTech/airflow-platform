"""meta.v_run_recovery — D-14/D-15/D-16's single-query recovery answer across all 3 stages.

D-15 established recovery is retry-only: rollback structurally cannot apply here, because every
stage (`STAGE_LOAD`, `DBT_BUILD`, `PUBLISH`) either commits atomically or does not commit at all
(META-03's own single-transaction publish guarantee, and `meta.run_stages`' claim/complete
lifecycle from migration 0025). This view makes that fact directly queryable: `next_action` always
reads `'retry stage <NAME>'` or `'complete'`, and the literal word "rollback" never appears in any
value it can produce (proven by `tests/integration/test_run_recovery_view.py`'s Test 5).

D-16 requires this be surfaced via a SQL view, not application code, so any consumer (Grafana's
Postgres datasource, an ad-hoc `psql` session, this migration's own repository read helper) gets
the identical, single-source-of-truth answer without hand-writing the 3-way join over
`meta.run_stages`.

Same "drop + recreate the full view" pattern as migrations 0012/0026/0030: Postgres has no
`ALTER VIEW ... ADD COLUMN`, and no equivalent for changing a JOIN predicate either. This is a
first creation, but a future 4th `run_stages` `stage_name` value's migration should follow the
same convention rather than attempting an in-place `ALTER VIEW`.

Grants: `etl_app` and `grafana_reader` get `SELECT` (mirroring `meta.v_customers_lineage`'s exact
grant pattern from migrations 0012/0026/0030). Zero grant to `dbt_app` — this view is a
Python/Airflow-side observability surface only, matching D-14's stage-write ownership split
(T-09-12: Elevation of Privilege, mitigated by omission).

Revision ID: 0033
Revises: 0032
Create Date: 2026-08-19
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "0033"
down_revision = "0032"
branch_labels = None
depends_on = None

_CREATE_VIEW = """
CREATE VIEW meta.v_run_recovery AS
SELECT
    r.run_id,
    r.dataset_id,
    r.status                    AS run_status,
    r.logical_date,
    r.dag_id,
    r.dag_run_id,
    sl.status                   AS stage_load_status,
    sl.lease_expires_at         AS stage_load_lease,
    db.status                   AS dbt_build_status,
    pb.status                   AS publish_status,
    CASE
        WHEN r.status = 'SUCCEEDED' AND pb.status = 'SUCCEEDED' THEN 'complete'
        WHEN sl.status IN ('FAILED', 'PENDING') OR sl.status IS NULL THEN 'retry stage STAGE_LOAD'
        WHEN db.status IN ('FAILED', 'PENDING') OR db.status IS NULL THEN 'retry stage DBT_BUILD'
        WHEN pb.status IN ('FAILED', 'PENDING') OR pb.status IS NULL THEN 'retry stage PUBLISH'
        ELSE 'in progress'
    END                          AS next_action
FROM meta.ingestion_runs r
LEFT JOIN meta.run_stages sl ON sl.run_id = r.run_id AND sl.stage_name = 'STAGE_LOAD'
LEFT JOIN meta.run_stages db ON db.run_id = r.run_id AND db.stage_name = 'DBT_BUILD'
LEFT JOIN meta.run_stages pb ON pb.run_id = r.run_id AND pb.stage_name = 'PUBLISH'
"""


def upgrade() -> None:
    """Create `meta.v_run_recovery`, spanning all 3 pipeline stages."""
    op.execute(_CREATE_VIEW)
    op.execute("GRANT SELECT ON meta.v_run_recovery TO etl_app")
    op.execute("GRANT SELECT ON meta.v_run_recovery TO grafana_reader")


def downgrade() -> None:
    """Drop `meta.v_run_recovery`."""
    op.execute("DROP VIEW meta.v_run_recovery")
