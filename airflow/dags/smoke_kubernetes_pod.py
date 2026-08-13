"""U1 (ORCH-01/06): permanent smoke-test fixture -- can KPO run a pod here?"""

import pendulum
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import dag
from kubernetes.client import models as k8s

from _common.kpo import common_kpo_kwargs


@dag(schedule="@daily", start_date=pendulum.datetime(2026, 1, 1, tz="UTC"), catchup=False)
def smoke_kubernetes_pod() -> None:
    """Run one pod; write the built image's git SHA to XCom -- U1's pass criteria."""
    resources = k8s.V1ResourceRequirements(
        requests={"cpu": "100m", "memory": "128Mi"}, limits={"cpu": "500m", "memory": "256Mi"}
    )
    KubernetesPodOperator(
        task_id="print_version_to_xcom",
        cmds=["sh", "-c"],
        arguments=['printf \'{"git_sha": "%s"}\' "$GIT_SHA" > /airflow/xcom/return.json'],
        retries=1,
        **common_kpo_kwargs(resources=resources),
    )


smoke_kubernetes_pod()
