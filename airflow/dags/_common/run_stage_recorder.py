"""LOAD-06's whole-pipeline recovery visibility: the `DBT_BUILD` `meta.run_stages` writer (D-14).

A THIRD, narrowly-scoped exception to "the DAG folder never touches business
logic or the analytical database", alongside ``integrity_gate.py`` and
``kpo.py``/``tracing_kpo.py``. `dbt_app` has zero grant on `meta.run_stages`
(migration 0025's deliberate D-02 decoupling), and dbt's own idempotency
(its incremental `is_incremental()` logic) must stay fully decoupled from
the Python claim/lease/heartbeat mechanism `claim_run_stage`/
`complete_run_stage` implement for `STAGE_LOAD`/`PUBLISH` (D-14). This
module is therefore the Airflow-side (never `dataplat`-importing) mechanism
that records a `DBT_BUILD` stage's observable status directly, via plain
`psycopg` -- it ONLY records status; it never gates whether `dbt build`
itself runs, and it never claims/heartbeats/leases anything the way
`claim_run_stage` does for the other two stages.

Today `meta.run_stages` only ever gets `STAGE_LOAD`/`PUBLISH` rows written
(by `PostgresMetadataRepository.claim_run_stage`/`complete_run_stage`,
`packages/dataplat/src/dataplat/metadata/postgres.py`), so a single query
cannot answer "what succeeded, what remains" for the WHOLE pipeline
(LOAD-06's gap). This module closes that gap for `DBT_BUILD` specifically.
DAG wiring (calling these tasks from `csv_ingest_customers`/
`csv_ingest_orders`) is a later plan (09-09), once this module and
`meta.v_run_recovery` (plan 09-06) both exist.

Unlike `STAGE_LOAD`/`PUBLISH` (each claimed by exactly one pod per
`run_id`), `dbt_build` is a SINGLE KPO task per DagRun that processes
potentially MANY staged `run_id`s in one `dbt build` invocation -- so
`record_dbt_build_stage` writes ONCE PER `run_id` currently eligible, not
once per pod. `record_dbt_build_stage` is a defensive `ON CONFLICT` upsert,
not a strict state-machine claim: `dbt_build` has no lease to steal (D-14),
so there is no equivalent of `claim_run_stage`'s claimability predicate
here.

``list_run_ids_pending_dbt_build``/``record_dbt_build_stage``'s exact SQL
shapes duplicate ``claim_run_stage``/``complete_run_stage``'s own SQL
vocabulary (`packages/dataplat/src/dataplat/metadata/postgres.py`, lines
~458-556) as raw SQL here -- never imported, per ADR-0004.
"""

from __future__ import annotations

import psycopg
from airflow.sdk import task
from airflow.sdk.bases.hook import BaseHook

# The Airflow Connection this module resolves its own DSN through -- the
# SAME Connection ID `integrity_gate.py` already resolves (itself
# Vault-backed via SEC-05's AIRFLOW__SECRETS__BACKEND=VaultBackend wiring,
# Phase 5), never a literal. This is this module's OWN, independent DSN
# resolution path: it runs in the scheduler/worker process, which never
# imports `dataplat` (ADR-0004), so `common_kpo_kwargs()`'s in-pod
# `vault://etl/analytics-db#dsn` resolution mechanism is unusable here.
_ANALYTICS_DB_CONN_ID = "analytics_db_default"

# App-validated vocabulary, matching `meta.run_stages.stage_name`'s existing
# STAGE_LOAD/PUBLISH convention (migration 0025) -- never a native Postgres
# ENUM.
_STAGE_NAME = "DBT_BUILD"


@task
def list_run_ids_pending_dbt_build(dataset_name: str) -> list[int]:
    """Every `run_id` currently eligible for a `dbt build` pass over `dataset_name`.

    Eligible means: `STAGE_LOAD` has reached `'SUCCEEDED'` for that run, AND
    either no `DBT_BUILD` row exists yet, or its most recent `DBT_BUILD` row
    is `'FAILED'` or `'RUNNING'` (a retry candidate -- Airflow's own task
    retry, not a lease-expiry timer, is what re-drives a `RUNNING` row that
    never reached a terminal status; `dbt_build` has no heartbeat/lease
    mechanism per D-14). A run whose `DBT_BUILD` row is already
    `'SUCCEEDED'` is never returned.
    """
    dsn = BaseHook.get_connection(_ANALYTICS_DB_CONN_ID).get_uri()
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.run_id
              FROM meta.ingestion_runs r
              JOIN meta.datasets d
                ON d.dataset_id = r.dataset_id
              JOIN meta.run_stages sl
                ON sl.run_id = r.run_id
               AND sl.stage_name = 'STAGE_LOAD'
               AND sl.status = 'SUCCEEDED'
              LEFT JOIN meta.run_stages db
                ON db.run_id = r.run_id
               AND db.stage_name = %(stage_name)s
             WHERE d.dataset_name = %(dataset_name)s
               AND (db.run_id IS NULL OR db.status IN ('FAILED', 'RUNNING'))
             ORDER BY r.run_id ASC
            """,
            {"dataset_name": dataset_name, "stage_name": _STAGE_NAME},
        )
        return [int(row[0]) for row in cur.fetchall()]


@task
def record_dbt_build_stage(run_ids: list[int], status: str) -> None:
    """Record `status` for `_STAGE_NAME` against every `run_id` in `run_ids`.

    An empty `run_ids` list (the common case when a DagRun has nothing newly
    staged) is a safe no-op -- zero rows written, no exception, no
    connection even opened. Otherwise, for each `run_id`, an `INSERT ...
    ON CONFLICT (run_id, stage_name) DO UPDATE` upsert -- duplicating
    `claim_run_stage`/`complete_run_stage`'s exact SQL vocabulary, never
    importing `dataplat`. `pod_name` is deliberately `NULL`: this
    Airflow-side task runs BEFORE the `dbt_build` KPO pod exists, so no real
    pod name is knowable yet -- an accepted, documented gap (D-18's live
    pod-kill proof locates the real pod via Airflow's own `dag_id`/
    `task_id` pod labels, not this column). `finished_at` is only ever set
    when the incoming `status` is a terminal one (anything other than
    `'RUNNING'`); a `'RUNNING'` write leaves any prior `finished_at`
    untouched.
    """
    if not run_ids:
        return

    dsn = BaseHook.get_connection(_ANALYTICS_DB_CONN_ID).get_uri()
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        for run_id in run_ids:
            cur.execute(
                """
                INSERT INTO meta.run_stages (
                    run_id, stage_name, status, pod_name, started_at, finished_at
                ) VALUES (
                    %(run_id)s, %(stage_name)s, %(status)s, NULL, now(),
                    CASE WHEN %(status)s != 'RUNNING' THEN now() ELSE NULL END
                )
                ON CONFLICT (run_id, stage_name) DO UPDATE
                    SET status = EXCLUDED.status,
                        finished_at = CASE WHEN EXCLUDED.status != 'RUNNING'
                                           THEN now()
                                           ELSE meta.run_stages.finished_at
                                      END
                """,
                {"run_id": run_id, "stage_name": _STAGE_NAME, "status": status},
            )
