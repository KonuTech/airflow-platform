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
from airflow.sdk.definitions.mappedoperator import MappedOperator

if TYPE_CHECKING:
    from airflow.models import DagBag

_BOTH_DAG_IDS = {"smoke_kubernetes_pod", "csv_ingest_customers"}


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
