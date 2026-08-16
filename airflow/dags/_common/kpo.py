"""``common_kpo_kwargs`` -- the ONLY shared code between this phase's two DAGs.

This module exists solely to remove boilerplate duplication between
``discover`` and ``ingest``'s ``KubernetesPodOperator`` construction inside
``csv_ingest_customers.py`` (04-RESEARCH.md's own "Recommended Project
Structure" note: ``_common/`` is for a shared KPO-builder helper, "NEVER
business logic"). Every value it returns is either a Kubernetes API object
(``kubernetes.client.models``) or a literal naming an existing namespace,
service account, ``vault://`` reference or non-secret configuration value --
nothing here parses CSV, validates a row, or writes to a database, so it does
not violate ORCH-02/ORCH-06's DAG-thinness rule. ``tests/policy/
test_dag_thinness.py`` exempts this file BY NAME from its import-based scan
for exactly this reason: the exemption reflects that this file genuinely
contains no business logic, not that the scan overlooked it.
"""

from __future__ import annotations

from airflow.sdk import Variable
from kubernetes.client import models as k8s

# The exact four credential env-var names csv_processor.cli._build_common()
# resolves via SecretsResolver's scheme dispatch (established across this
# phase's prior plans, 04-02/04-05) -- this module only sets their VALUES,
# it never reads or handles a credential value itself. Three of the four now
# hold a vault:// literal (plan 05-02): resolve_secret() resolves it inside
# the pod, authenticating via the csv-processor ServiceAccount identity
# below -- never a Kubernetes Secret. DATAPLAT_S3_ENDPOINT_URL is unchanged:
# it was never a Secret, and stays a plain, non-secret configuration value,
# the same shape VAULT_ADDR/VAULT_K8S_ROLE below now also use.
_S3_ENDPOINT_URL = "http://minio.data.svc.cluster.local:9000"
_VAULT_ADDR = "http://vault.vault.svc.cluster.local:8200"
_VAULT_K8S_ROLE = "csv-processor"

# OBS-08/OBS-10 (plan 07-04): the standalone OTel Collector chart's real
# Service DNS -- verified live in 07-03-SUMMARY.md's own Key Decisions
# (`<release-name>-<chart-name>` fullname convention, NOT the shorter
# `otel-collector` name a naive guess would produce). Genuinely static: this
# endpoint never varies per-execution, unlike a per-task TRACEPARENT value
# (RESEARCH.md Pitfall 2 -- exactly why it belongs here, in the DAG-parse-
# time dict, and TRACEPARENT does not). Both `discover` and `ingest` get
# this -- real `dataplat` tracing/metrics available in every task pod; only
# `ingest` additionally receives a per-execution TRACEPARENT, injected by
# `TracingKubernetesPodOperator` (tracing_kpo.py), not by this function.
_OTEL_COLLECTOR_ENDPOINT = (
    "http://otel-collector-opentelemetry-collector.monitoring.svc.cluster.local:4318"
)


def common_kpo_kwargs(
    *,
    resources: k8s.V1ResourceRequirements,
    extra_env_vars: list[k8s.V1EnvVar] | None = None,
) -> dict[str, object]:
    """Build the ``KubernetesPodOperator`` kwargs every task pod in this phase shares.

    Every value below is either verified live against this phase's prior
    plans (namespace/service account: 04-02's ``kubernetes/rbac-etl.yaml``;
    the three ``vault://`` credential references: plan 05-02, resolved
    inside the pod by ``resolve_secret()`` via the ``csv-processor``
    ServiceAccount's own Kubernetes-auth identity, never a Kubernetes
    Secret) or an explicit override of a provider default that
    04-RESEARCH.md's Pattern 5 confirms is NOT the default
    (``on_finish_action``, ``do_xcom_push``) -- nothing here is left to
    chance.

    Args:
        resources: The per-task CPU/memory requests and limits. Callers pass
            a lighter profile for ``discover`` and a heavier one for
            ``ingest`` (04-07-PLAN.md Interfaces) -- this function does not
            choose a default, so every call site is explicit (T-04-03).
        extra_env_vars: Appended after the six shared env vars below.
            ``None`` (the default) adds nothing -- ``discover`` never passes
            this. ``ingest`` uses it for
            ``DATAPLAT_HEARTBEAT_INTERVAL_SECONDS`` (discovered live, 04-08
            verification: the 60s production default never fires even once
            during this fixture's real COPY duration, so D-11's mid-load
            proof could never observe a heartbeat through the real pod path
            -- `dataplat.pipeline.run.run_ingest`'s own default is
            unchanged; only this one task's env shrinks the interval).

    Returns:
        A kwargs mapping suitable for ``KubernetesPodOperator(**kwargs)`` or
        ``KubernetesPodOperator.partial(**kwargs)``, covering everything
        that does NOT vary between ``discover`` and ``ingest``
        (``task_id``, ``cmds``, ``arguments`` and ``retries`` stay
        per-call-site).
    """
    return {
        "namespace": "etl",
        "service_account_name": "csv-processor",
        "image": Variable.get("csv_processor_image"),
        "do_xcom_push": True,
        "on_finish_action": "delete_succeeded_pod",
        "get_logs": True,
        "container_resources": resources,
        "env_vars": [
            k8s.V1EnvVar(name="DATAPLAT_DB_DSN", value="vault://etl/analytics-db#dsn"),
            k8s.V1EnvVar(name="DATAPLAT_S3_ACCESS_KEY", value="vault://etl/minio#access_key"),
            k8s.V1EnvVar(name="DATAPLAT_S3_SECRET_KEY", value="vault://etl/minio#secret_key"),
            k8s.V1EnvVar(name="DATAPLAT_S3_ENDPOINT_URL", value=_S3_ENDPOINT_URL),
            k8s.V1EnvVar(name="VAULT_ADDR", value=_VAULT_ADDR),
            k8s.V1EnvVar(name="VAULT_K8S_ROLE", value=_VAULT_K8S_ROLE),
            k8s.V1EnvVar(name="OTEL_EXPORTER_OTLP_ENDPOINT", value=_OTEL_COLLECTOR_ENDPOINT),
            *(extra_env_vars or []),
        ],
    }
