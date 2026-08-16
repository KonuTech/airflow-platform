"""``TracingKubernetesPodOperator`` -- the W3C ``traceparent`` propagation mechanism (OBS-10).

This module exists solely to close the one gap CLAUDE.md/STACK.md both flag
explicitly: "Trace context does NOT propagate into ``KubernetesPodOperator``
pods automatically... you must do it yourself." The override below writes
nothing that parses CSV, validates a row, or talks to a database -- it reads
whichever OTel span is currently active in the Airflow worker process and
copies its W3C trace context into the pod spec ``KubernetesPodOperator`` is
about to launch, exactly the same "Kubernetes API object or a literal"
scope ``kpo.py`` itself is limited to. ``tests/policy/test_dag_thinness.py``
exempts this file BY NAME from its import-based scan for the same reason it
exempts ``kpo.py``: this file legitimately imports ``opentelemetry.propagate``
and ``kubernetes.client.models``, neither of which is business logic.

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

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from opentelemetry import propagate


class TracingKubernetesPodOperator(KubernetesPodOperator):
    """A ``KubernetesPodOperator`` that injects the active span's W3C ``traceparent``.

    Per D-12, this operator is used ONLY for ``csv_ingest_customers.py``'s
    ``ingest`` task -- the mapped per-file task instance that becomes the
    trace root, one trace per ``meta.ingestion_runs`` row. ``discover`` stays
    a plain ``KubernetesPodOperator``, unchanged.
    """

    def build_pod_request_obj(self, context: object = None) -> k8s.V1Pod:
        """Append a ``TRACEPARENT`` env var to the launched pod, when a span is active.

        Args:
            context: The Airflow task context, forwarded unchanged to
                ``KubernetesPodOperator.build_pod_request_obj()`` -- this
                override never reads it directly; it only reads the
                currently active OTel span via
                ``opentelemetry.propagate.inject()``.

        Returns:
            The already-built ``V1Pod``, with exactly one additional
            ``TRACEPARENT`` env var appended to its first container's env
            list when an active span exists. Genuinely no-op-safe when
            tracing is disabled or no span is active
            (``opentelemetry.propagate.inject()``'s own documented
            no-op-when-no-context behavior, verified empirically in this
            plan's own unit tests): the pod is returned completely
            unmodified in that case, matching every other task pod's shape.
        """
        pod = super().build_pod_request_obj(context)
        carrier: dict[str, str] = {}
        propagate.inject(carrier)
        if "traceparent" in carrier:
            pod.spec.containers[0].env.append(
                k8s.V1EnvVar(name="TRACEPARENT", value=carrier["traceparent"]),
            )
        return pod
