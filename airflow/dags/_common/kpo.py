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


def common_kpo_kwargs(  # noqa: PLR0913 -- all 6 are keyword-only (08.1-12 generalization; see Args)
    *,
    resources: k8s.V1ResourceRequirements,
    extra_env_vars: list[k8s.V1EnvVar] | None = None,
    service_account_name: str = "csv-processor",
    image_variable: str = "csv_processor_image",
    vault_k8s_role: str = _VAULT_K8S_ROLE,
    include_dataplat_credentials: bool = True,
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
        extra_env_vars: Appended after the shared env vars below.
            ``None`` (the default) adds nothing -- ``discover`` never passes
            this. ``ingest`` uses it for
            ``DATAPLAT_HEARTBEAT_INTERVAL_SECONDS`` (discovered live, 04-08
            verification: the 60s production default never fires even once
            during this fixture's real COPY duration, so D-11's mid-load
            proof could never observe a heartbeat through the real pod path
            -- `dataplat.pipeline.run.run_ingest`'s own default is
            unchanged; only this one task's env shrinks the interval).
        service_account_name: Defaults to today's exact ``"csv-processor"``
            value, so ``discover``/``stage``'s call sites need zero changes.
            Plan 08.1-12 introduces the one caller that overrides this:
            ``dbt_build`` runs as its own ``"dbt"`` ServiceAccount (plan
            08.1-03), never ``csv-processor``'s, so its pod's Kubernetes-auth
            identity can only ever resolve ``dbt``'s own narrow Vault grants.
        image_variable: Name of the Airflow Variable this function reads via
            ``Variable.get(...)`` for the pod's image. Defaults to today's
            exact ``"csv_processor_image"``. Plan 08.1-12's ``dbt_build``
            overrides this to ``"dbt_image"`` (plan 08.1-02) -- the dbt
            image is a separate build, never the csv-processor one.
        vault_k8s_role: The ``VAULT_K8S_ROLE`` env var's value -- which Vault
            Kubernetes-auth role ``resolve_secrets.py``/the pod's own
            ``hvac`` login authenticates as. Defaults to today's exact
            ``"csv-processor"`` value (via the ``_VAULT_K8S_ROLE`` module
            constant). Plan 08.1-12's ``dbt_build`` overrides this to
            ``"dbt"`` (plan 08.1-03) so it authenticates as its own
            least-privilege Vault identity, never csv-processor's.
        include_dataplat_credentials: When ``True`` (the default, matching
            every call site before plan 08.1-12), includes the four
            ``DATAPLAT_DB_DSN``/``DATAPLAT_S3_*`` credential env vars. Plan
            08.1-12's ``dbt_build`` passes ``False``: the dbt image's own
            ``ENTRYPOINT`` resolves its own ``etl/dbt-db`` credential
            directly, and never needs (or should be able to attempt to
            resolve) the ``csv-processor`` credential set at all -- a
            structural, not merely conventional, privilege boundary
            (T-08.1-28). ``VAULT_ADDR``/``VAULT_K8S_ROLE``/
            ``OTEL_EXPORTER_OTLP_ENDPOINT`` stay unconditional regardless:
            every pod this function serves needs Vault + tracing.

    Returns:
        A kwargs mapping suitable for ``KubernetesPodOperator(**kwargs)`` or
        ``KubernetesPodOperator.partial(**kwargs)``, covering everything
        that does NOT vary between ``discover``/``stage``/``dbt_build``/
        ``publish`` (``task_id``, ``cmds``, ``arguments`` and ``retries``
        stay per-call-site).
    """
    dataplat_credential_env_vars = (
        [
            k8s.V1EnvVar(name="DATAPLAT_DB_DSN", value="vault://etl/analytics-db#dsn"),
            k8s.V1EnvVar(name="DATAPLAT_S3_ACCESS_KEY", value="vault://etl/minio#access_key"),
            k8s.V1EnvVar(name="DATAPLAT_S3_SECRET_KEY", value="vault://etl/minio#secret_key"),
            k8s.V1EnvVar(name="DATAPLAT_S3_ENDPOINT_URL", value=_S3_ENDPOINT_URL),
        ]
        if include_dataplat_credentials
        else []
    )
    return {
        "namespace": "etl",
        "service_account_name": service_account_name,
        "image": Variable.get(image_variable),
        "do_xcom_push": True,
        # Debug session airflow-scheduler-stuck-tasks (2026-08-16): was
        # "delete_succeeded_pod" (keep failed pods for debugging). Confirmed
        # live that this leaks CPU/memory permanently: KubernetesPodOperator's
        # default startup_timeout_seconds=120 fires when the launched pod
        # can't get scheduled in time (routine under this cluster's tight
        # node CPU budget -- kind/cluster.yaml), the operator raises and the
        # task attempt is marked failed, and "keep on failure" means the pod
        # is never deleted -- even once it LATER gets scheduled by
        # Kubernetes and its main container completes successfully, nothing
        # is left to extract XCom or terminate the do_xcom_push sidecar, so
        # it sits in Running phase forever, permanently pinning its full
        # resource request. "keep failed pods for debugging" buys nothing
        # for this specific failure mode -- a pod that never started has no
        # container logs, and get_logs=True below already streams any real
        # container logs into Airflow's own task log before the pod would be
        # deleted, so genuine application failures stay fully debuggable.
        "on_finish_action": "delete_pod",
        "get_logs": True,
        "container_resources": resources,
        "env_vars": [
            *dataplat_credential_env_vars,
            k8s.V1EnvVar(name="VAULT_ADDR", value=_VAULT_ADDR),
            k8s.V1EnvVar(name="VAULT_K8S_ROLE", value=vault_k8s_role),
            k8s.V1EnvVar(name="OTEL_EXPORTER_OTLP_ENDPOINT", value=_OTEL_COLLECTOR_ENDPOINT),
            *(extra_env_vars or []),
        ],
    }
