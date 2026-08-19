"""D-06's explicit "no file found" gap recording, for a backfill DagRun that matches nothing.

A FOURTH, narrowly-scoped exception to "the DAG folder never touches business logic or the
analytical database" (ADR-0004), after ``integrity_gate.py``, ``kpo.py``/``tracing_kpo.py``, and
``run_stage_recorder.py``. Same shape as those three: a plain Airflow ``@task`` function running
in the scheduler/worker process, resolving its own DSN via the same ``analytics_db_default``
Connection, writing raw ``psycopg`` SQL rather than importing ``dataplat``.

A live scheduled run finding zero new files on any given minute-poke is the ordinary, expected
steady state — recording that as a "gap" would make the table noise, not signal. A gap is only
meaningful for a backfill run deliberately re-processing a specific historical window: if THAT
run finds nothing, the window's file genuinely does not exist (or was never delivered), and that
fact deserves its own explicit, queryable record (D-06) distinct from a failure. `dag_run.
backfill_id` (Airflow 3.3.0's own `DagRun` model) is the discriminator: `NULL` for a live run,
set for every backfill-triggered `DagRun`.

`record_processing_gap_if_empty` is inserted immediately after `matched_keys = list_matched_keys
(...)` in both `csv_ingest_customers.py`/`csv_ingest_orders.py`, reading that SAME return value
without altering `list_matched_keys` itself or the existing `matched_keys >> gate >> discover`
edges.
"""

from __future__ import annotations

import psycopg
from airflow.sdk import task
from airflow.sdk.bases.hook import BaseHook

# The Airflow Connection this module resolves its own DSN through -- the SAME Connection ID
# `integrity_gate.py`/`run_stage_recorder.py` already resolve (itself Vault-backed via SEC-05's
# AIRFLOW__SECRETS__BACKEND=VaultBackend wiring, Phase 5), never a literal. This module runs in
# the scheduler/worker process, which never imports `dataplat` (ADR-0004).
_ANALYTICS_DB_CONN_ID = "analytics_db_default"


@task
def record_processing_gap_if_empty(
    matched_keys: list[str],
    dataset_name: str,
    dag_run=None,  # noqa: ANN001 -- Airflow-injected context param, untyped upstream too
) -> None:
    """No-op unless THIS is a backfill run that genuinely matched zero keys (D-06).

    Three conditions all skip the write entirely -- no connection even opened: `matched_keys` is
    non-empty (a real file existed, nothing to record); `dag_run is None` (no context to read a
    `backfill_id` from at all); or `dag_run.backfill_id is None` (an ordinary live/scheduled run
    finding nothing new this minute is the expected steady state, not a gap). Otherwise, an
    idempotent `ON CONFLICT (dataset_id, dag_run_id) DO NOTHING` upsert -- a retried, still-empty
    backfill DagRun never duplicates its own gap row.
    """
    if matched_keys or dag_run is None or dag_run.backfill_id is None:
        return

    dsn = BaseHook.get_connection(_ANALYTICS_DB_CONN_ID).get_uri()
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO meta.processing_gaps (dataset_id, dag_id, dag_run_id, backfill_id)
            SELECT dataset_id, %(dag_id)s, %(dag_run_id)s, %(backfill_id)s
              FROM meta.datasets
             WHERE dataset_name = %(dataset_name)s
            ON CONFLICT (dataset_id, dag_run_id) DO NOTHING
            """,
            {
                "dag_id": dag_run.dag_id,
                "dag_run_id": dag_run.run_id,
                "backfill_id": dag_run.backfill_id,
                "dataset_name": dataset_name,
            },
        )
