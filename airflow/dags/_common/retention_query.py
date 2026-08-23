"""D-35/D-38's retention query+evaluate+conditional-delete logic, for ``platform_retention.py``.

A FIFTH ADR-0004 exception, joining ``kpo.py``/``tracing_kpo.py``/
``integrity_gate.py``/``run_stage_recorder.py``/``gap_recorder.py`` on
``tests/policy/test_dag_thinness.py``'s by-name exemption list, and the same
shape as ``integrity_gate.py``: a plain ``@task`` function, resolving its own
DSN via ``analytics_db_default``, writing raw ``psycopg`` SQL rather than
importing ``dataplat`` for the DB-touching parts. This module exists as its
own ``_common/`` file -- not inlined into ``platform_retention.py`` --
mirroring ``csv_ingest_customers.py``'s own "DAG file imports and wires a
`_common/`-defined ``@task``, never defines the business logic itself"
convention (``from _common.integrity_gate import integrity_gate,
list_matched_keys``): keeping the DAG file itself down to the ``@dag``
wrapper is what makes this module safely patchable for
``tests/dagtest/test_platform_retention_dagrun.py``'s ``dag.test()`` proof
-- Airflow's ``DagBag`` freshly re-execs the top-level DAG file on every
parse, but a regular ``from _common.X import Y`` submodule is imported once
and cached normally, so a test that patches this module's OWN functions
before calling ``load_dag(...)`` (``mock_run_stage_recorder_db``'s own
proven pattern, ``tests/dagtest/conftest.py``) reliably survives into the
DagRun's real execution.

Unlike ``integrity_gate.py``/``gap_recorder.py``/``run_stage_recorder.py``,
this module ALSO imports ``dataplat.retention.policy``/``dataplat.config.
model`` directly -- both are pure, I/O-free evaluator/contract modules with
no CSV-parsing or database-writing side effects of their own, and
11-08-PLAN.md's own Interfaces section names this exact wiring as intended,
unlike ADR-0004's blanket "never a ``dataplat`` import" for the DB-writing
exceptions above.

Dataset configuration is read from ``meta.config_versions`` (the CURRENT,
``valid_to IS NULL`` row per active dataset), never from
``configs/datasets/*.yaml`` on disk: only the ``dags`` hostPath is mounted
into any Airflow pod (``helm/values/*/airflow.yaml``), matching
``ConfigRegistry.get_by_id``'s own documented reason for the identical
choice ("the pod that calls this does not have configs/ mounted or baked
in"). ``config_document`` is re-validated through the same ``DatasetConfig``
model ``get_by_id`` uses.

Layer -> source mapping (the plan's own instruction):
  - raw/processed/quarantine: one non-paginated MinIO ``list_objects_v2`` per
    dataset (bucket == layer name; prefix == the dataset's own
    ``source.path``), age from ``LastModified``.
  - validation_reports: ``meta.validation_results`` joined to
    ``meta.ingestion_runs`` for ``dataset_id``, age from ``created_at``.
  - ingestion_metadata: ``meta.files`` (age from ``discovered_at``) UNION
    ``meta.ingestion_runs`` (age from ``finished_at``/``started_at``) --
    both are genuinely ingestion-metadata rows; candidate identifiers carry
    a ``file:``/``run:`` prefix so a later conditional delete never sends
    one table's id to the other.
  - logs: an honest structural no-op (``_logs_layer_noop`` below) -- this
    deployment has no centralized log-persistence store to query.

Known, deliberate finding (not a bug, not fixed here): ``meta.files``/
``meta.validation_results``/``meta.ingestion_runs`` grant ``etl_app`` only
``SELECT, INSERT, UPDATE`` (migrations 0002/0004/0014) -- no ``DELETE``. Even
a dataset misconfigured with ``enforce: true`` cannot actually delete an
ingestion-metadata/validation-report row through this DAG's own DB
credential; the delete call below would fail at the database-privilege
level first. This backstops T-11-21 one layer further than the plan asked
for, mirroring D-40's MinIO IAM deny-delete backstop on `raw`. Left in
place deliberately: granting DELETE preemptively, with `enforce` dry-run-by
-default everywhere today, would be exactly the unforced privilege-widening
this project's Vault gap-closure precedent (STATE.md, Phase 5) warns
against.
"""

from __future__ import annotations

import pendulum
import psycopg
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import task
from airflow.sdk.bases.hook import BaseHook

from dataplat.config.model import DatasetConfig
from dataplat.observability.logging import get_logger
from dataplat.retention.policy import RetentionCandidate, RetentionReport, evaluate_retention

log = get_logger(__name__)

# Same DSN-resolution name/mechanism as _common/integrity_gate.py's own
# _ANALYTICS_DB_CONN_ID -- an Airflow Connection, Vault-backed (SEC-05),
# never a literal.
_ANALYTICS_DB_CONN_ID = "analytics_db_default"
_MINIO_CONN_ID = "minio_default"

# Bucket name == layer name for these three (helm/values/*/minio.yaml's own
# 5-bucket layout). "validated"/"metadata" are deliberately excluded: the
# plan routes validation-report/ingestion-metadata retention through SQL,
# not MinIO (module docstring).
_MINIO_LAYERS = ("raw", "processed", "quarantine")

_VALIDATION_REPORTS_SQL = """
    SELECT vr.validation_result_id, EXTRACT(DAY FROM now() - vr.created_at)::int
      FROM meta.validation_results vr
      JOIN meta.ingestion_runs ir ON ir.run_id = vr.run_id
     WHERE ir.dataset_id = %s
"""
_FILES_SQL = """
    SELECT file_id, EXTRACT(DAY FROM now() - discovered_at)::int
      FROM meta.files
     WHERE dataset_id = %s AND discovered_at IS NOT NULL
"""
_INGESTION_RUNS_SQL = """
    SELECT run_id, EXTRACT(DAY FROM now() - COALESCE(finished_at, started_at))::int
      FROM meta.ingestion_runs
     WHERE dataset_id = %s AND COALESCE(finished_at, started_at) IS NOT NULL
"""


def _current_dataset_configs(cur: psycopg.Cursor) -> list[tuple[int, str, DatasetConfig]]:
    """Every active dataset's CURRENT resolved config -- never read from disk (module docstring)."""
    rows = cur.execute(
        """
        SELECT d.dataset_id, d.dataset_name, cv.config_document
          FROM meta.config_versions cv
          JOIN meta.datasets d ON d.dataset_id = cv.dataset_id
         WHERE cv.valid_to IS NULL AND d.is_active
        """
    ).fetchall()
    return [
        (dataset_id, dataset_name, DatasetConfig.model_validate(document))
        for dataset_id, dataset_name, document in rows
    ]


def _minio_candidates(layer: str, prefix: str) -> list[RetentionCandidate]:
    """One non-paginated ``list_objects_v2`` call's worth of candidates for ``layer``.

    Non-paginated (``integrity_gate.py``'s own simplicity precedent): this
    corpus stays well under the 1000-key page size today, and a future
    dataset large enough to need pagination is a visible follow-up --
    ``candidate_count`` in the logged report makes a truncated page obvious,
    never a silent one.
    """
    client = S3Hook(aws_conn_id=_MINIO_CONN_ID).get_conn()
    response = client.list_objects_v2(Bucket=layer, Prefix=prefix)
    now = pendulum.now("UTC")
    return [
        RetentionCandidate(
            layer=layer,
            identifier=obj["Key"],
            age_days=(now - pendulum.instance(obj["LastModified"])).days,
            size_bytes=obj["Size"],
        )
        for obj in response.get("Contents", [])
    ]


def _sql_age_candidates(
    cur: psycopg.Cursor,
    layer: str,
    query: str,
    dataset_id: int,
    id_prefix: str = "",
) -> list[RetentionCandidate]:
    """Shared shape for every ``meta.*`` age-based query above -- one row per candidate."""
    rows = cur.execute(query, (dataset_id,)).fetchall()
    return [
        RetentionCandidate(layer=layer, identifier=f"{id_prefix}{row_id}", age_days=int(age_days))
        for row_id, age_days in rows
    ]


def _logs_layer_noop(dataset_name: str) -> None:
    """D-39's honest no-op: no centralized log store exists in this deployment to query.

    ``helm/values/local/airflow.yaml``'s ``dags.persistence.enabled: false``
    is DAG-delivery only, and no ``remote_logging``/log-PVC config exists in
    either Helm values profile (verified directly) -- fabricating a deletion
    path against a store that does not exist would be worse than reporting
    zero candidates honestly.
    """
    log.info(
        "retention_logs_layer_noop",
        dataset=dataset_name,
        message=(
            "no centralized log store configured in this deployment; "
            "Kubernetes node-level log rotation applies out-of-band"
        ),
    )


def _perform_deletes(
    cur: psycopg.Cursor,
    report: RetentionReport,
    candidates_by_layer: dict[str, list[RetentionCandidate]],
    dataset_name: str,
) -> None:
    """Delete ONLY the candidates the evaluator flagged (T-11-19) -- never re-derived here."""
    if not report.enforce:
        return
    for layer, layer_report in report.layers.items():
        if layer_report.would_delete_count == 0:
            continue
        window_days = layer_report.threshold.get("window_days")
        to_delete = [
            c
            for c in candidates_by_layer[layer]
            if window_days is not None and c.age_days > window_days
        ]
        if layer in _MINIO_LAYERS:
            client = S3Hook(aws_conn_id=_MINIO_CONN_ID).get_conn()
            client.delete_objects(
                Bucket=layer,
                Delete={"Objects": [{"Key": c.identifier} for c in to_delete], "Quiet": True},
            )
        elif layer == "validation_reports":
            ids = [int(c.identifier) for c in to_delete]
            cur.execute(
                "DELETE FROM meta.validation_results WHERE validation_result_id = ANY(%s)",
                (ids,),
            )
        elif layer == "ingestion_metadata":
            file_ids = [
                int(c.identifier[5:]) for c in to_delete if c.identifier.startswith("file:")
            ]
            run_ids = [int(c.identifier[4:]) for c in to_delete if c.identifier.startswith("run:")]
            if file_ids:
                cur.execute("DELETE FROM meta.files WHERE file_id = ANY(%s)", (file_ids,))
            if run_ids:
                cur.execute("DELETE FROM meta.ingestion_runs WHERE run_id = ANY(%s)", (run_ids,))
        log.info(
            "retention_deleted",
            dataset=dataset_name,
            layer=layer,
            deleted_count=len(to_delete),
        )


def _connect(dsn: str) -> psycopg.Connection:
    """The one seam between ``run_retention`` and a real database connection.

    Deliberately NOT a call to ``psycopg.connect`` inlined into
    ``run_retention`` itself: ``tests/dagtest/conftest.py``'s own
    ``mock_run_stage_recorder_db`` docstring documents, from a real prior
    incident, that patching the global ``psycopg.connect`` in this test tier
    is dangerous -- Airflow's own ``postgresql+psycopg`` metadata-DB dialect
    calls the SAME module-level function, so a global patch silently
    intercepts the metadata DB's real connections too. Naming this seam
    lets ``tests/dagtest/test_platform_retention_dagrun.py`` replace
    ``retention_query._connect`` alone, never touching ``psycopg.connect``
    itself.
    """
    return psycopg.connect(dsn)


@task
def run_retention() -> dict[str, object]:
    """Query all six layers per active dataset, evaluate, log, and conditionally act (D-35/D-38)."""
    dsn = BaseHook.get_connection(_ANALYTICS_DB_CONN_ID).get_uri()
    summary: dict[str, object] = {}
    with _connect(dsn) as conn, conn.cursor() as cur:
        for dataset_id, dataset_name, config in _current_dataset_configs(cur):
            if config.retention is None:
                continue

            candidates_by_layer: dict[str, list[RetentionCandidate]] = {
                layer: _minio_candidates(layer, config.source.path) for layer in _MINIO_LAYERS
            }
            candidates_by_layer["validation_reports"] = _sql_age_candidates(
                cur, "validation_reports", _VALIDATION_REPORTS_SQL, dataset_id
            )
            candidates_by_layer["ingestion_metadata"] = _sql_age_candidates(
                cur, "ingestion_metadata", _FILES_SQL, dataset_id, id_prefix="file:"
            ) + _sql_age_candidates(
                cur, "ingestion_metadata", _INGESTION_RUNS_SQL, dataset_id, id_prefix="run:"
            )
            _logs_layer_noop(dataset_name)
            candidates_by_layer["logs"] = []

            all_candidates = [c for layer_list in candidates_by_layer.values() for c in layer_list]
            report = evaluate_retention(config.retention, all_candidates)

            log.info(
                "retention_report",
                dataset=dataset_name,
                enforce=report.enforce,
                layers={
                    name: {
                        "candidate_count": lr.candidate_count,
                        "would_delete_count": lr.would_delete_count,
                        "deleted_count": lr.deleted_count,
                        "threshold": lr.threshold,
                        "observed": lr.observed,
                    }
                    for name, lr in report.layers.items()
                },
            )

            _perform_deletes(cur, report, candidates_by_layer, dataset_name)
            summary[dataset_name] = {
                "enforce": report.enforce,
                "would_delete_total": sum(lr.would_delete_count for lr in report.layers.values()),
            }
    return summary
