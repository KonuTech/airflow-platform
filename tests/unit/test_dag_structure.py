"""Structural proof of every ORCH-01..09 requirement -- pure ``DagBag`` parsing, no live cluster.

The ``dagbag`` fixture lives in ``tests/unit/conftest.py`` -- see that
module's docstring for why ``DagBag`` needs ``sys.path``/env-var setup
before it can parse ``airflow/dags/`` cleanly. ``test_resolve_window.py``
reuses the exact same fixture (04-07-PLAN.md Task 2: "reuse the SAME
loading mechanism, do not invent a second one").
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk.definitions.asset import Asset
from airflow.sdk.definitions.mappedoperator import MappedOperator
from airflow.sdk.definitions.timetables.assets import AssetTriggeredTimetable

if TYPE_CHECKING:
    from airflow.models import DagBag

_BOTH_DAG_IDS = {"smoke_kubernetes_pod", "csv_ingest_customers", "csv_ingest_orders"}

_CUSTOMERS_ASSET_URI = "s3://normalized/customers"


def _kpo_attrs(task: object) -> dict[str, object] | None:
    """Return the KPO-relevant attrs for `task`, mapped or plain; `None` if not a KPO task."""
    if isinstance(task, MappedOperator) and issubclass(task.operator_class, KubernetesPodOperator):
        return dict(task.partial_kwargs)
    if isinstance(task, KubernetesPodOperator):
        return {
            "namespace": task.namespace,
            "service_account_name": task.service_account_name,
            "container_resources": task.container_resources,
            "retries": task.retries,
        }
    return None


def test_no_import_errors(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}


def test_both_dags_present(dagbag: DagBag) -> None:
    assert set(dagbag.dags) >= _BOTH_DAG_IDS


def test_retries_set(dagbag: DagBag) -> None:
    dag = dagbag.dags["csv_ingest_customers"]
    checked = 0
    for task in dag.tasks:
        if isinstance(task, S3KeySensor):
            assert task.retries is not None
            assert task.retries > 0, f"{task.task_id} has no positive retries"
            checked += 1
            continue
        attrs = _kpo_attrs(task)
        if attrs is not None:
            retries = attrs.get("retries")
            assert retries is not None
            assert retries > 0, f"{task.task_id} has no positive retries"
            checked += 1
    assert checked >= 3, "expected wait_for_files, discover and ingest to all be checked"


def test_kpo_resources(dagbag: DagBag) -> None:
    checked = 0
    for dag_id in _BOTH_DAG_IDS:
        for task in dagbag.dags[dag_id].tasks:
            attrs = _kpo_attrs(task)
            if attrs is None:
                continue
            resources = attrs.get("container_resources")
            assert resources is not None, f"{task.task_id} has no container_resources"
            assert resources.requests, f"{task.task_id}'s container_resources has no requests"
            assert resources.limits, f"{task.task_id}'s container_resources has no limits"
            checked += 1
    assert checked >= 3, "expected discover, ingest and the smoke pod to all be checked"


def test_uses_s3_key_sensor(dagbag: DagBag) -> None:
    dag = dagbag.dags["csv_ingest_customers"]
    sensors = [t for t in dag.tasks if isinstance(t, S3KeySensor)]
    assert len(sensors) == 1
    assert sensors[0].deferrable is True
    assert sensors[0].poke_interval == 30


def test_max_active_runs_is_one(dagbag: DagBag) -> None:
    assert dagbag.dags["csv_ingest_customers"].max_active_runs == 1


def test_namespace_and_service_account(dagbag: DagBag) -> None:
    checked = 0
    for dag_id in _BOTH_DAG_IDS:
        for task in dagbag.dags[dag_id].tasks:
            attrs = _kpo_attrs(task)
            if attrs is None:
                continue
            assert attrs.get("namespace") == "etl", f"{task.task_id} has the wrong namespace"
            assert attrs.get("service_account_name") == "csv-processor", (
                f"{task.task_id} has the wrong service_account_name"
            )
            checked += 1
    assert checked >= 3, "expected discover, ingest and the smoke pod to all be checked"


def test_integrity_gate_upstream_of_discover(dagbag: DagBag) -> None:
    """D-18: `wait_for_files >> list_matched_keys >> integrity_gate >> discover` holds in both DAGs.

    Walks each DAG's real task-dependency graph (`task_dict`/
    `upstream_task_ids`) -- proven structurally, no live cluster needed.
    """
    for dag_id in ("csv_ingest_customers", "csv_ingest_orders"):
        dag = dagbag.dags[dag_id]
        discover = dag.task_dict["discover"]
        gate = dag.task_dict["integrity_gate"]
        matched_keys = dag.task_dict["list_matched_keys"]
        sensor = dag.task_dict["wait_for_files"]

        assert "integrity_gate" in discover.upstream_task_ids, (
            f"{dag_id}: discover is not gated by integrity_gate"
        )
        assert "list_matched_keys" in gate.upstream_task_ids, (
            f"{dag_id}: integrity_gate does not fan out over list_matched_keys"
        )
        assert "wait_for_files" in matched_keys.upstream_task_ids, (
            f"{dag_id}: list_matched_keys is not gated by wait_for_files"
        )
        assert isinstance(sensor, S3KeySensor), f"{dag_id}: wait_for_files is not an S3KeySensor"


def test_orders_dag_present_and_asset_scheduled(dagbag: DagBag) -> None:
    """D-14/D-15: `csv_ingest_orders` exists, scheduled off the customers Asset, not a cron."""
    dag = dagbag.dags["csv_ingest_orders"]
    timetable = dag.timetable
    assert isinstance(timetable, AssetTriggeredTimetable), (
        f"csv_ingest_orders is scheduled by {type(timetable).__name__}, not an Asset"
    )
    asset_uris = {asset.uri for asset in timetable.asset_condition.objects}
    assert _CUSTOMERS_ASSET_URI in asset_uris, (
        f"csv_ingest_orders is not scheduled off {_CUSTOMERS_ASSET_URI}"
    )


def test_customers_ingest_declares_outlets(dagbag: DagBag) -> None:
    """D-15: `csv_ingest_customers`'s `ingest` task publishes the customers Asset via outlets."""
    dag = dagbag.dags["csv_ingest_customers"]
    ingest = dag.task_dict["ingest"]
    outlets = ingest.outlets
    assert outlets, "ingest declares no outlets"
    outlet_uris = {asset.uri for asset in outlets if isinstance(asset, Asset)}
    assert _CUSTOMERS_ASSET_URI in outlet_uris, (
        f"ingest's outlets do not include {_CUSTOMERS_ASSET_URI}"
    )
