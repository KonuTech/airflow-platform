"""Proof-over-prose for OBS-10's Airflow-side half (07-04-PLAN.md Task 3's ``<behavior>``).

No live cluster and no real pod anywhere in this module: `opentelemetry.
propagate.inject()`'s "current span" is `contextvars`-based and independent
of which `TracerProvider` produced the `Tracer` (the same insight `dataplat.
observability.tracing`'s own module docstring documents) -- a locally
constructed `opentelemetry.sdk.trace.TracerProvider` inside
`start_as_current_span()` is exactly the "real backend" RESEARCH.md Pattern
1 asks this test to simulate, proven directly against the real, installed
`opentelemetry-sdk`/`apache-airflow-providers-cncf-kubernetes` packages, not
a mock of either.

`TracingKubernetesPodOperator.build_pod_request_obj(context=None)` was
confirmed empirically (this plan's own execution) to build a complete `V1Pod`
with `context=None` and no live Kubernetes connection -- `KubernetesHook.
is_in_cluster` is a local environment/file check, not a network call -- so
every test below constructs the operator directly rather than going through
`DagBag`/`dag.test()`.
"""

from __future__ import annotations

import re
from types import SimpleNamespace

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from opentelemetry import propagate
from opentelemetry import trace as otel_trace
from opentelemetry.sdk.trace import TracerProvider

from _common.tracing_kpo import TracingKubernetesPodOperator

# W3C Trace Context traceparent header: "00-<32 hex trace id>-<16 hex span id>-<2 hex flags>"
# https://www.w3.org/TR/trace-context/#traceparent-header-field-values
_TRACEPARENT_RE = re.compile(r"^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$")


def _build_operator(task_id: str) -> TracingKubernetesPodOperator:
    """One minimal, otherwise-unremarkable operator instance per test -- no shared state."""
    return TracingKubernetesPodOperator(
        task_id=task_id,
        namespace="etl",
        image="localhost:5001/csv-processor:test-fixture",
        cmds=["dataplat"],
        name=f"{task_id}-pod",
        do_xcom_push=False,
        env_vars=[k8s.V1EnvVar(name="DATAPLAT_DB_DSN", value="vault://etl/analytics-db#dsn")],
    )


def _traceparent_env_vars(pod: k8s.V1Pod) -> list[k8s.V1EnvVar]:
    return [env for env in pod.spec.containers[0].env if env.name == "TRACEPARENT"]


def _airflow_ctx_env_var_names(pod: k8s.V1Pod) -> list[str]:
    return [env.name for env in pod.spec.containers[0].env if env.name.startswith("AIRFLOW_CTX_")]


def _airflow_ctx_env_var_map(pod: k8s.V1Pod) -> dict[str, str]:
    return {
        env.name: env.value
        for env in pod.spec.containers[0].env
        if env.name.startswith("AIRFLOW_CTX_")
    }


def test_no_active_span_injects_nothing() -> None:
    """No active span, otel_on effectively disabled: no TRACEPARENT env var appended.

    No `TracerProvider`/`start_as_current_span()` anywhere in this test --
    genuinely no-op-safe, matching `opentelemetry.propagate.inject()`'s own
    documented no-op-when-no-context behavior (confirmed empirically this
    plan's own execution: an un-configured process's `inject()` call leaves
    its carrier dict entirely empty).
    """
    operator = _build_operator("ingest_no_span")
    pod = operator.build_pod_request_obj(context=None)

    assert _traceparent_env_vars(pod) == []
    # The pod is otherwise built normally -- the override changed nothing else about it.
    env_names = [env.name for env in pod.spec.containers[0].env]
    assert "DATAPLAT_DB_DSN" in env_names


def test_active_span_injects_one_well_formed_traceparent() -> None:
    """An active span (real SDK TracerProvider, matching Plan 07-02's own real backend):

    the override appends exactly one TRACEPARENT env var, W3C-formatted and
    correctly encoding the active span's trace/span IDs (proven by
    round-tripping the SAME carrier through `opentelemetry.propagate.
    extract()` and comparing IDs, not merely regex-matching the shape).
    """
    provider = TracerProvider()
    tracer = provider.get_tracer(__name__)
    operator = _build_operator("ingest_one_span")

    with tracer.start_as_current_span("pipeline.run_streaming.chunk") as span:
        pod = operator.build_pod_request_obj(context=None)
        expected_trace_id = format(span.get_span_context().trace_id, "032x")
        expected_span_id = format(span.get_span_context().span_id, "016x")

    traceparent_vars = _traceparent_env_vars(pod)
    assert len(traceparent_vars) == 1, (
        f"expected exactly one TRACEPARENT env var, found {len(traceparent_vars)}"
    )
    value = traceparent_vars[0].value
    assert _TRACEPARENT_RE.match(value), f"{value!r} is not a well-formed W3C traceparent header"

    # Round-trip through the real extract() API -- the load-bearing proof
    # that this is a genuine, correctly-encoded parent context, not merely a
    # string that happens to match the regex shape.
    extracted_context = propagate.extract({"traceparent": value})
    extracted_span_context = otel_trace.get_current_span(extracted_context).get_span_context()
    assert format(extracted_span_context.trace_id, "032x") == expected_trace_id
    assert format(extracted_span_context.span_id, "016x") == expected_span_id


def test_two_different_spans_produce_two_different_traceparents() -> None:
    """Proves the value is genuinely per-execution, not a DAG-parse-time constant.

    The exact defect RESEARCH.md Pitfall 2 warns against: injecting inside
    `common_kpo_kwargs()` would bake ONE value into a dict built once at
    DAG-parse time, so every task instance's launched pod would carry either
    the same TRACEPARENT or none at all, regardless of which DagRun/file it
    belongs to. Two independently-active spans, two independent
    `build_pod_request_obj()` calls, and two DIFFERENT operator instances
    (matching how two mapped task instances are two distinct operator
    invocations, never one shared object) must produce two DIFFERENT values.
    """
    provider = TracerProvider()
    tracer = provider.get_tracer(__name__)

    with tracer.start_as_current_span("run-a"):
        pod_a = _build_operator("ingest_span_a").build_pod_request_obj(context=None)
    with tracer.start_as_current_span("run-b"):
        pod_b = _build_operator("ingest_span_b").build_pod_request_obj(context=None)

    traceparent_a = _traceparent_env_vars(pod_a)[0].value
    traceparent_b = _traceparent_env_vars(pod_b)[0].value
    assert traceparent_a != traceparent_b, (
        "two different active spans produced the SAME TRACEPARENT value -- "
        "injection is not genuinely per-execution"
    )


def test_discover_is_unaffected_by_this_module() -> None:
    """False-positive control: a plain KubernetesPodOperator has no such override.

    `TracingKubernetesPodOperator` only ever wraps `ingest` (D-12); this
    guards against a future accidental import-order or monkeypatch mistake
    silently making `discover` trace-root-capable too.
    """
    assert TracingKubernetesPodOperator is not KubernetesPodOperator
    assert issubclass(TracingKubernetesPodOperator, KubernetesPodOperator)
    assert "build_pod_request_obj" in TracingKubernetesPodOperator.__dict__, (
        "TracingKubernetesPodOperator no longer overrides build_pod_request_obj directly"
    )


def _make_airflow_context(
    ti: object,
    *,
    run_id: str = "manual__2026-01-01T00:00:00+00:00",
) -> dict[str, object]:
    """Build a context dict shaped enough for the REAL, installed `KubernetesPodOperator.
    build_pod_request_obj()` to succeed, not merely `{"ti": ...}` alone.

    The installed `apache-airflow-providers-cncf-kubernetes`'s own
    `_get_ti_pod_labels()` reads `context["ti"]`/`context["run_id"]`/
    `context["dag"]` directly and unconditionally whenever `context` is
    truthy (confirmed empirically: a bare `{"ti": ...}` dict raises
    `KeyError: 'run_id'` inside the base class, before this override's own
    code ever runs) -- `run_id`/`dag` are therefore part of every context
    this file passes to `build_pod_request_obj`, exactly as a real Airflow
    task context always provides them alongside `ti`.
    """
    return {"ti": ti, "run_id": run_id, "dag": SimpleNamespace()}


def test_airflow_context_injects_five_dag_identity_env_vars() -> None:
    """OBS-07 gap closure (07-09): a real-shaped Airflow task context (`context["ti"]`)
    results in all five `AIRFLOW_CTX_*` env vars on the launched pod, with exact
    string values -- including `AIRFLOW_CTX_K8S_NAMESPACE`, taken from the pod's
    own already-built `metadata.namespace`, not from `context` at all.
    """
    ti = SimpleNamespace(
        dag_id="csv_ingest_customers",
        task_id="ingest",
        run_id="manual__2026-01-01T00:00:00+00:00",
        map_index=2,
        try_number=1,
    )
    context = _make_airflow_context(ti)
    pod = _build_operator("ingest_dag_ctx").build_pod_request_obj(context=context)

    env_vars = _airflow_ctx_env_var_map(pod)
    assert env_vars["AIRFLOW_CTX_DAG_ID"] == "csv_ingest_customers"
    assert env_vars["AIRFLOW_CTX_TASK_ID"] == "ingest"
    assert env_vars["AIRFLOW_CTX_DAG_RUN_ID"] == "manual__2026-01-01T00:00:00+00:00"
    assert env_vars["AIRFLOW_CTX_MAP_INDEX"] == "2"
    assert env_vars["AIRFLOW_CTX_K8S_NAMESPACE"] == "etl"


def test_malformed_airflow_context_injects_nothing_and_does_not_raise() -> None:
    """T-07-26: a `ti` satisfying the REAL base operator's own `_get_ti_pod_labels()`
    requirements (`dag_id`/`task_id`/`map_index`/`try_number`, so `super().
    build_pod_request_obj()` itself succeeds) but missing `run_id` -- the one
    extra attribute only THIS override's own `AIRFLOW_CTX_DAG_RUN_ID` injection
    reads -- must never crash pod launch. It degrades to zero `AIRFLOW_CTX_*`
    vars appended (the whole five-var list is built inside one `try` block, so
    an `AttributeError` on any single read discards all five), mirroring
    `TRACEPARENT`'s own no-op-safe contract.
    """
    ti = SimpleNamespace(dag_id="csv_ingest_customers", task_id="ingest", map_index=0, try_number=1)
    context = _make_airflow_context(ti)

    pod = _build_operator("ingest_malformed_ctx").build_pod_request_obj(context=context)

    assert _airflow_ctx_env_var_names(pod) == []


def test_context_none_still_injects_no_dag_identity_env_vars() -> None:
    """Regression guard: `context=None` -- this file's own pre-existing convention
    for every TRACEPARENT-only test above -- still injects zero `AIRFLOW_CTX_*`
    vars, proving this task's change did not alter that pre-existing behavior.
    """
    pod = _build_operator("ingest_no_ctx").build_pod_request_obj(context=None)

    assert _airflow_ctx_env_var_names(pod) == []
