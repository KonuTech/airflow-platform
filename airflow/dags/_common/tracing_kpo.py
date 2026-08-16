"""``TracingKubernetesPodOperator`` -- ``traceparent`` (OBS-10) + Airflow/K8s identity (OBS-07).

This module exists solely to close two gaps CLAUDE.md/STACK.md and
07-VERIFICATION.md both flag explicitly: "Trace context does NOT propagate
into ``KubernetesPodOperator`` pods automatically... you must do it
yourself," and Airflow's ``KubernetesPodOperator`` does not auto-inject
``AIRFLOW_CTX_*`` identity env vars into launched pods either. The override
below writes nothing that parses CSV, validates a row, or talks to a
database -- it reads whichever OTel span is currently active in the Airflow
worker process (for ``TRACEPARENT``) and the Airflow task context passed
into ``execute()`` (for dag/run/task/map-index/namespace identity), and
copies both into the pod spec ``KubernetesPodOperator`` is about to launch,
exactly the same "Kubernetes API object or a literal" scope ``kpo.py``
itself is limited to. ``tests/policy/test_dag_thinness.py`` exempts this
file BY NAME from its import-based scan for the same reason it exempts
``kpo.py``: this file legitimately imports ``opentelemetry.propagate`` and
``kubernetes.client.models``, neither of which is business logic.

Why a subclass overriding ``build_pod_request_obj()``, not ``env_vars``
templating and not ``common_kpo_kwargs()`` (RESEARCH.md Pitfalls 2/3):
``common_kpo_kwargs()`` runs once, at DAG-PARSE time, when
``csv_ingest_customers.py`` is imported -- long before any task attempt, and
therefore before any span exists, so no per-execution trace ID could ever be
baked into its returned dict. ``env_vars`` is a documented
``template_fields`` member of ``KubernetesPodOperator``, but Jinja templating
of that specific field has a multi-year history of not working reliably
(``apache/airflow`` issues #13348, #25841), and no Airflow Jinja macro
exposes a trace/span ID regardless. ``build_pod_request_obj()`` is different:
Airflow calls it from inside ``execute()``, i.e. at task-RUN time, inside the
very process holding the active Airflow-managed task span -- the only place
that can both read the active span and write the launched pod's spec in one
step.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from opentelemetry import propagate

if TYPE_CHECKING:
    from airflow.sdk import Context


class TracingKubernetesPodOperator(KubernetesPodOperator):
    """Injects the active span's W3C ``traceparent`` and Airflow/K8s task identity.

    When available, also appends Airflow's own dag/run/task/map-index
    identity plus the launched pod's resolved Kubernetes namespace.

    Per D-12, this operator is used ONLY for ``csv_ingest_customers.py``'s
    ``ingest`` task -- the mapped per-file task instance that becomes the
    trace root, one trace per ``meta.ingestion_runs`` row. ``discover`` stays
    a plain ``KubernetesPodOperator``, unchanged.
    """

    def build_pod_request_obj(self, context: Context | None = None) -> k8s.V1Pod:
        """Append ``TRACEPARENT`` and ``AIRFLOW_CTX_*`` env vars to the launched pod.

        Args:
            context: The Airflow task context, forwarded unchanged to
                ``KubernetesPodOperator.build_pod_request_obj()`` -- typed
                to match that parent signature exactly (``airflow.sdk
                .Context``, a ``TypedDict``, imported only under
                ``TYPE_CHECKING`` since this module never needs it at
                runtime). The ``TRACEPARENT`` injection never reads it
                directly (it only reads the currently active OTel span via
                ``opentelemetry.propagate.inject()``); the
                ``AIRFLOW_CTX_*`` injection reads ``context["ti"]`` --
                Airflow's real task context always carries a ``"ti"`` key
                (the ``TaskInstance``) at task-run time, the same object
                Jinja templates read as ``{{ ti }}``, stable across Airflow
                2.x and 3.x. ``isinstance(context, dict)`` below is a
                runtime defensive check only (a ``TypedDict`` is a plain
                ``dict`` at runtime, so this guards against a genuinely
                malformed context Airflow's own type hint doesn't rule out)
                -- it does not affect, and is not needed for, the
                ``super().build_pod_request_obj(context)`` call above, which
                type-checks directly against the parameter's own
                ``Context | None`` annotation.

        Returns:
            The already-built ``V1Pod``, with an additional ``TRACEPARENT``
            env var appended to its first container's env list when an
            active span exists (genuinely no-op-safe when tracing is
            disabled or no span is active -- ``opentelemetry.propagate.
            inject()``'s own documented no-op-when-no-context behavior,
            verified empirically in this plan's own unit tests), and up to
            five additional ``AIRFLOW_CTX_DAG_ID``/``_TASK_ID``/
            ``_DAG_RUN_ID``/``_MAP_INDEX``/``_K8S_NAMESPACE`` env vars
            appended when ``context["ti"]`` is present and well-formed
            (T-07-26: a shape mismatch -- ``context`` is ``None``,
            ``context["ti"]`` is absent, or it lacks an expected attribute
            -- degrades to zero ``AIRFLOW_CTX_*`` vars appended, never an
            exception, mirroring ``TRACEPARENT``'s own no-op-safe contract
            above). The pod is returned completely unmodified when neither
            mechanism has anything to contribute, matching every other task
            pod's shape.
        """
        pod = super().build_pod_request_obj(context)
        carrier: dict[str, str] = {}
        propagate.inject(carrier)
        if "traceparent" in carrier:
            pod.spec.containers[0].env.append(
                k8s.V1EnvVar(name="TRACEPARENT", value=carrier["traceparent"]),
            )

        ti = context.get("ti") if isinstance(context, dict) else None
        if ti is not None:
            try:
                dag_context_env_vars = [
                    k8s.V1EnvVar(name="AIRFLOW_CTX_DAG_ID", value=str(ti.dag_id)),
                    k8s.V1EnvVar(name="AIRFLOW_CTX_TASK_ID", value=str(ti.task_id)),
                    k8s.V1EnvVar(name="AIRFLOW_CTX_DAG_RUN_ID", value=str(ti.run_id)),
                    k8s.V1EnvVar(
                        name="AIRFLOW_CTX_K8S_NAMESPACE",
                        value=str(pod.metadata.namespace),
                    ),
                ]
                # map_index is `int | None` on TaskInstance (None for an
                # unmapped task instance) -- unlike the fields above, it has
                # no sensible str() fallback the CLI side (csv_processor/
                # cli.py) can round-trip through int(), so omit the env var
                # entirely rather than inject the literal string "None".
                if ti.map_index is not None:
                    dag_context_env_vars.append(
                        k8s.V1EnvVar(
                            name="AIRFLOW_CTX_MAP_INDEX",
                            value=str(ti.map_index),
                        ),
                    )
            except AttributeError:
                # T-07-26: a shape mismatch in context["ti"] must never fail
                # pod launch for the whole `ingest` task -- degrade to
                # appending nothing, mirroring the TRACEPARENT block above.
                dag_context_env_vars = []
            pod.spec.containers[0].env.extend(dag_context_env_vars)

        return pod
