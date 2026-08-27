"""meta.v_quarantined_artifacts -- first-class visibility of quarantined runs' retained rows.

Half of debug/ci-pipeline-ingestion-timeout ROUND 16 finding (20b): a run
terminally QUARANTINED at publish time (ROUND 14's breaker semantics) has
already staged its bronze rows and usually already has silver rows
attributed to it by the dbt models -- quarantine blocks the PASS, not the
DATA, and CI run 33103279876 measured 3,000,000+ silver rows retained from
quarantined runs. The exclusion half of the fix (same round) keeps those
rows out of GOLD forever (`scd.py`/`merge.py`/`merge_orders.py`'s
`_run_id NOT IN (... status = 'QUARANTINED')` predicates); THIS view is the
identifiability half the platform's Core Value demands -- every retained
quarantined artifact must be traceable and explainable, never silently
resident. One row per QUARANTINED run with its bronze/silver retained-row
counts; an operator re-opening a run (status flip away from QUARANTINED)
drops it from this view and simultaneously re-includes its rows in the
publishers' NOT-IN predicates -- the two halves share one source of truth.

The remaining disposition (whether/how silver rows from quarantined runs are
retro-excluded or cleaned, which requires dbt-side status visibility and
re-materialization of displaced keys) is deliberately deferred --
docs/adr/0012-quarantined-run-artifact-disposition.md records the design.

The view is owned by the migration role (superuser), so its readers do not
need direct grants on `staging.*`/`silver.*` (dbt_app-owned) -- same
security-boundary shape as `meta.dataset_id_for_name`'s SECURITY DEFINER
precedent (migration 0028): the view narrows access to exactly the counts.

Revision ID: 0041
Revises: 0040
Create Date: 2026-08-28
"""

from __future__ import annotations

from alembic import op

revision = "0041"
down_revision = "0040"
branch_labels = None
depends_on = None

_CREATE_VIEW = """
CREATE VIEW meta.v_quarantined_artifacts AS
WITH bronze AS (
    SELECT _run_id AS run_id, count(*) AS bronze_rows
      FROM staging.customers GROUP BY _run_id
    UNION ALL
    SELECT _run_id, count(*)
      FROM staging.orders GROUP BY _run_id
),
silver_rows AS (
    SELECT _run_id AS run_id, count(*) AS silver_rows
      FROM silver.customers GROUP BY _run_id
    UNION ALL
    SELECT _run_id, count(*)
      FROM silver.orders GROUP BY _run_id
)
SELECT r.run_id,
       d.dataset_name,
       r.file_id,
       r.batch_id,
       r.finished_at,
       coalesce(b.bronze_rows, 0)  AS bronze_rows,
       coalesce(s.silver_rows, 0)  AS silver_rows
  FROM meta.ingestion_runs r
  JOIN meta.datasets d ON d.dataset_id = r.dataset_id
  LEFT JOIN bronze b ON b.run_id = r.run_id
  LEFT JOIN silver_rows s ON s.run_id = r.run_id
 WHERE r.status = 'QUARANTINED'
"""


def upgrade() -> None:
    """Create meta.v_quarantined_artifacts; grant read to the three reader roles."""
    op.execute(_CREATE_VIEW)
    op.execute(
        "GRANT SELECT ON meta.v_quarantined_artifacts "
        "TO etl_app, analytics_owner, grafana_reader"
    )


def downgrade() -> None:
    """Drop meta.v_quarantined_artifacts (grants fall with the view)."""
    op.execute("DROP VIEW meta.v_quarantined_artifacts")
