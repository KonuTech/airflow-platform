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

from datetime import timedelta

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

# debug/ci-pipeline-ingestion-timeout ROUND 10 (root cause 14): the local
# profile's 500m stage/publish CPU REQUEST is structurally unschedulable on
# CI's single ~3-CPU node (~220m free at steady state pre-fix), turning every
# stage attempt into a deterministic ~129s startup_timeout failure -- no
# stage container had EVER run on CI. A request is a scheduling reservation,
# not a cap: the 2-CPU LIMIT below is identical in both profiles, so a pod
# bursts the same once placed. Per-profile request via an Airflow Variable
# follows the csv_processor_image precedent exactly: CI sets
# stage_cpu_request=200m post-cluster-up (scripts/ci-set-workload-images.sh);
# local never sets it and gets this default -- byte-identical to the value
# both DAGs hardcoded before this fix. Resolved at DAG-parse time through
# the same layered secrets chain (AIRFLOW_VAR_STAGE_CPU_REQUEST env backend
# first) the image Variables already exercise offline.
_DEFAULT_STAGE_CPU_REQUEST = "500m"

# debug/ci-pipeline-ingestion-timeout ROUND 14 (finding 18a, trim iii): CI
# trims customers' publish retries via Airflow Variable publish_retries=3
# (scripts/ci-set-workload-images.sh, the stage_cpu_request precedent above);
# local never sets it and keeps this default. Post-ROUND-14, publish
# retries exist ONLY for the transient-infrastructure class (the
# KubernetesJobWatcher 30s-read-timeout race, Kyverno admission hiccups,
# co-scheduling CPU bursts) -- deterministic quality-gate trips no longer
# consume retries at all (publish_ingest quarantines the batch and exits 0),
# so the CI value is sized against the measured transient burst window
# (~5min self-healed, ROUND 13), not against breaker-trip burn. ROUND 21
# (timeout-budget rebalance): "6" -> "4", matching stage/dbt_build's own
# ROUND 21 cut -- see HEAVY_TASK_EXECUTION_TIMEOUT's comment below for the
# full worst-case-arithmetic justification. CI's own publish_retries=3 stays
# unchanged (already the safest of every combination under the new budget).
_DEFAULT_PUBLISH_RETRIES = "4"

# debug/ci-pipeline-ingestion-timeout ROUND 20 (finding: podkill zombie-detection gap):
# `stage`/`dbt_build`/`publish` had NO per-task wall-clock ceiling of their own -- the only
# ceiling in force was the whole DagRun's blunt `dagrun_timeout=45min`. Live-reproduced this
# round (direct source read of the INSTALLED apache-airflow==3.3.0, cross-checked against a
# real `kubectl delete pod --wait=false` against the REAL KubernetesPodOperator/PodManager code
# on the local cluster): Airflow's own zombie/orphan-task detection
# (`[scheduler] task_instance_heartbeat_timeout`, the Airflow-3 rename of the Airflow-2
# `scheduler_zombie_task_threshold` -- see `airflow.cli.commands.config_command.py`'s own
# `ConfigChange` table) is heartbeat-based: it fires only when the TASK PROCESS ITSELF stops
# heartbeating to the metadata DB. A KubernetesPodOperator task whose pod is deleted out-of-band
# does NOT stop heartbeating -- the task process stays fully alive, polling/streaming logs for a
# pod that no longer exists -- so this detection class can structurally never catch this failure
# mode, on ANY executor. Separately, direct live reproduction proved `KubernetesPodOperator`'s
# OWN pod-vanish detection is NOT broken: with `do_xcom_push=True` (this project's universal
# setting, see `common_kpo_kwargs` below), `PodManager.await_xcom_sidecar_container_start`
# reliably raises `AirflowException` ("Xcom sidecar container is already terminated!") within
# roughly one Kubernetes termination-grace-period window (~30-40s) of the delete -- the
# exception fires quickly and correctly at the KPO layer. What was missing is a task-level
# wall-clock guillotine INDEPENDENT of that exception ever being observed/reported by whatever
# CI-specific supervisor path might occasionally lose it under contention (an honestly-declared
# residual unknown -- this session's live LocalExecutor reproduction was blocked by an unrelated,
# pre-existing local-cluster scheduling stall): Airflow's `execution_timeout` is enforced via a
# real POSIX `SIGALRM` wall-clock timer (`airflow.sdk.execution_time.timeout.TimeoutPosix`,
# confirmed via direct source read) that fires unconditionally after N real seconds regardless of
# what the task's own code is doing, converts to `AirflowTaskTimeout`, and drives the SAME
# `on_kill()` + retry path a normal task failure would. 10 minutes is sized with generous
# headroom over every legitimate single-attempt duration measured this session (ROUND 17/18's
# live-measured ~5.6min for a full 1M-row restage + dbt_build + publish cycle COMBINED) while
# staying well inside `dagrun_timeout=45min`, so a killed pod is force-failed and retried
# (`stage`/`dbt_build`/`publish` all carry `retries` >= 2) WITHIN the DagRun, multiple times over,
# before the DagRun-level ceiling would ever need to fire. Applied identically in both profiles
# (D-06: behavioral task configuration, not a resource-sizing divergence axis) and to BOTH DAGs'
# `stage`/`dbt_build`/`publish` tasks -- the same three tasks ROUND 19 named for the podkill gap
# and the OOMKilled-publish bonus finding, both of which share this exact missing-ceiling
# mechanism.
#
# debug/ci-pipeline-ingestion-timeout ROUND 21 (dbtkill: internally-inconsistent timeout-budget
# hierarchy): ROUND 20's own combination could exceed dagrun_timeout=45min in the worst case --
# e.g. customers.stage's old retries=6 x execution_timeout=10min alone is 70min before any
# retry_delay is even added. Live-confirmed via dbtkill: stage try=3/up_for_retry rode toward its
# OWN dagrun_timeout without ever reaching a terminal state, under real ~420m-CPU-headroom
# contention (see kind/cluster.yaml capacity vs baseline platform-pod requests; tracked as its
# own out-of-scope follow-up, .planning/todos/pending/). SUB-FINDING (direct source read of the
# installed apache-airflow==3.3.0 `taskinstance.py::next_retry_datetime`): `retry_exponential_
# backoff=True` -- the literal value every task below passes -- is a SILENT NO-OP in this Airflow
# version. The field is now a literal float MULTIPLIER, not a bool flag; `multiplier = task.
# retry_exponential_backoff if ... != 0 else 1.0`, and the growth branch only runs `if multiplier
# != 1.0`. Python's `bool` is an `int` subclass (`True == 1`), so `multiplier == 1.0` and every
# retry uses a CONSTANT, un-jittered `retry_delay` regardless of try_number -- confirmed via a
# standalone reproduction of the exact formula. There is therefore no "exponential growth" to cap;
# the only real levers are execution_timeout, retries, and retry_delay itself. 10min -> 6min:
# justified against the only direct single-attempt evidence available (ROUND 17 live measurement:
# dbt_build+publish COMBINED on a real 1M-row file took <=2min; the full stage+dbt_build+publish
# cycle took ~5.6min, implying stage alone is on the order of ~3.6min) -- 6min leaves ~2.4min
# (67%) headroom over that estimate, and execution_timeout wraps the ENTIRE KPO.execute() call
# (confirmed via TimeoutPosix source read, ROUND 20), so any CPU-starvation-induced pod-scheduling
# wait is already inside this budget, not on top of it. More, shorter-ceiling attempts fit more
# retries inside the SAME 45min ceiling under contention than fewer, longer ones ever did.
# Worst-case math (attempts=retries+1, CONSTANT delay per the sub-finding above), computed for
# every call site below post-fix: customers.stage/dbt_build (retries=4, delay=30s) = 5x360s +
# 4x30s = 1920s = 32.0min (13.0min/28.9% margin, the TIGHTEST of all combinations); customers.
# publish-local (retries=4) = same, 32.0min; customers.publish-CI (retries=3) = 4x360s + 3x30s =
# 1530s = 25.5min (19.5min/43.3% margin); orders.stage/publish (retries=3, delay=30s) = 25.5min
# (19.5min/43.3% margin); orders.dbt_build (retries=2) = 3x360s + 2x30s = 1140s = 19.0min
# (26.0min/57.8% margin). Minimum margin across every combination: 13.0min (28.9% of
# dagrun_timeout) -- a real, substantial margin, not a barely-under fit. See `tests/unit/
# test_dag_structure.py::test_worst_case_retry_budget_has_real_margin` for the enforced regression
# guard (fails if any future retries/execution_timeout/retry_delay edit erodes this margin below
# 10 minutes).
#
# debug/ci-pipeline-ingestion-timeout ROUND 22 (retry-exhaustion under CI contention): dbtkill
# and orphan's own live evidence (both orders `stage` TIs reaching try=4/state=failed, a genuine
# exhaustion of the OLD retries=3 budget under real ~420m-headroom CPU-starvation contention) showed
# orders hitting the SAME KubernetesJobWatcher request-timeout race customers' `stage`/`dbt_build`
# already compensate for with retries=4 -- orders never got that back-port when it was written to
# mirror customers' shape (the same recurring gap class as ROUND 20's publish-resources finding and
# ROUND 21's retry_delay finding). Bumped `csv_ingest_orders.py`'s `stage`/`dbt_build` retries
# 3/2 -> 4 and `publish` from a hardcoded 3 to `publish_retries()` (this module's own function,
# below) -- now byte-identical to customers' own retries treatment, matching D-06 instead of
# leaving orders on a divergent, unjustified cut. Post-bump worst case: orders.stage/dbt_build
# (retries=4) = 1920s = 32.0min (13.0min/28.9% margin, now identical to customers.stage/dbt_build);
# orders.publish-local (retries=4) = 32.0min (13.0min/28.9% margin); orders.publish-CI (retries=3,
# via the SAME `publish_retries()` Variable customers already reads) = 1530s = 25.5min (19.5min/
# 43.3% margin) -- every orders combination now shares the exact same margin as its customers
# counterpart, computed and enforced by the SAME `test_worst_case_retry_budget_has_real_margin`
# regression guard above (no test change needed -- it already iterates both DAGs' 3 heavy tasks).
HEAVY_TASK_EXECUTION_TIMEOUT = timedelta(minutes=6)

# debug/ci-pipeline-ingestion-timeout ROUND 21: renamed from customers-only `_KYVERNO_RETRY_DELAY`
# (identical 30s value, identical Kyverno-admission-flakiness rationale from ROUND 6 -- see that
# round's own comment history) and promoted to a SHARED constant so `csv_ingest_orders.py`'s
# stage/dbt_build/publish can use it too. Orders never had an explicit retry_delay at all before
# this round, silently inheriting Airflow's DEFAULT_RETRY_DELAY=300s/5min -- the exact same shape
# of back-port gap as ROUND 20's own orders-publish-resources finding (a customers-side
# improvement never mirrored into orders when orders was written to mirror customers' shape). A
# shorter, uniform delay is a pure win under CPU-starvation contention: it does not shrink actual
# single-attempt processing time, only how quickly a FAILED attempt re-enters the scheduling
# queue, so it strictly helps cycle through the fixed retries budget faster.
HEAVY_TASK_RETRY_DELAY = timedelta(seconds=30)


def publish_retries() -> int:
    """Resolve the publish task's Airflow ``retries`` count, per profile.

    CI sets the ``publish_retries`` Variable to a smaller value (see the
    module comment above); local falls back to the historical default of 6.
    Resolved at DAG-parse time through the same layered chain
    (``AIRFLOW_VAR_PUBLISH_RETRIES`` env backend first) the image Variables
    already exercise offline.
    """
    return int(Variable.get("publish_retries", default=_DEFAULT_PUBLISH_RETRIES))


def stage_pod_resources() -> k8s.V1ResourceRequirements:
    """Build the heavy stage/publish pod resource profile, CPU request per profile.

    Used by both DAGs' ``stage`` (and customers' ``publish``, which
    deliberately reuses the heavy profile since plan 10-07's publish-OOM
    fix). Only the CPU *request* varies by profile (see the module comment
    above); memory request and both limits are identical everywhere.
    """
    return k8s.V1ResourceRequirements(
        requests={
            "cpu": Variable.get("stage_cpu_request", default=_DEFAULT_STAGE_CPU_REQUEST),
            "memory": "1Gi",
        },
        limits={"cpu": "2", "memory": "4Gi"},
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
