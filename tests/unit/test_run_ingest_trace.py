"""Unit tests for ``dataplat.pipeline.run.run_ingest``'s tracing/metrics (plan 07-05 Task 2).

Covers: ``run_ingest`` opens its own ``pipeline.run_ingest`` span and passes
its ``trace_id``/``span_id`` into ``claim_ingestion_run`` (``None``/``None``
when no active parent context and tracing is unconfigured -- an invalid span
context, never a garbage all-zero hex string; a genuine CHILD of an
extracted parent context otherwise -- same ``trace_id``, a NEW ``span_id``);
``runs_started``/``runs_finished`` are emitted exactly once each around a
claimed run, on both the success and run-fatal-exception paths, but NEVER on
a refused claim; and the publish transaction's ``pipeline.publish`` span is
a genuine child of ``pipeline.run_ingest``.

Everything below the claim (staging, publish) runs against fakes, never a
real database or object store -- this file is offline/``tests/unit``-tier by
design. ``run_module.StagingLoader``/``run_module.resolve_publisher`` are
monkeypatched exactly like ``tests/integration/test_run_ingest.py``'s own
``test_crash_between_staging_and_publish_leaves_no_partial_state_and_retry_succeeds``
patches ``resolve_publisher`` to simulate a crash -- this file mirrors that
same style at the unit-test tier.

Parent-child span relationships are proven against real, recording OTel SDK
spans captured by an ``InMemorySpanExporter`` wired directly onto
``tracing._provider`` (bypassing the public ``configure(otlp_endpoint=...)``,
which always builds a real ``OTLPSpanExporter`` -- mirrors
``test_tracing.py``'s own sanctioned direct-poke-at-``tracing._provider``
pattern, SLF001-suppressed there for the identical reason). Every test
resets ``tracing``/``metrics`` back to a genuine no-op afterward so no state
leaks into sibling test files.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Self

import pytest
from opentelemetry import context as otel_context
from opentelemetry import propagate
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import dataplat.pipeline.run as run_module
from dataplat.config.model import (
    BatchingConfig,
    ColumnContract,
    DatasetConfig,
    DeduplicationConfig,
    LoadConfig,
    SourceConfig,
)
from dataplat.load.staging import StagingResult
from dataplat.models.identity import RunContext
from dataplat.observability import metrics, tracing
from dataplat.observability.logging import get_logger
from dataplat.pipeline.protocol import PipelineContext
from dataplat.pipeline.run import run_ingest

if TYPE_CHECKING:
    from collections.abc import Iterator

_WELL_FORMED_TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
_INJECTED_TRACE_ID_HEX = "0af7651916cd43dd8448eb211c80319c"
_INJECTED_PARENT_SPAN_ID_HEX = "b7ad6b7169203331"


@pytest.fixture(autouse=True)
def _reset_observability_after_test() -> Iterator[None]:
    """Reset ``tracing``/``metrics`` back to a genuine no-op after every test.

    Neither module's module-owned singleton has built-in per-test isolation
    (``tracing.py``/``metrics.py``'s own module docstrings) -- a test that
    configures a real/in-memory provider here must not leak that state into
    a sibling test in this file, ``test_tracing.py``, ``test_metrics.py``, or
    ``test_cli_trace_extraction.py``.
    """
    yield
    tracing.configure(otlp_endpoint=None)
    metrics.configure(otlp_endpoint=None)


def _configure_in_memory_tracing() -> InMemorySpanExporter:
    """Wire ``tracing``'s module-owned provider directly to an in-memory exporter.

    Bypasses the public ``configure(otlp_endpoint=...)`` (which always builds
    a real ``OTLPSpanExporter``) so span parent/child relationships and IDs
    can be asserted directly against recorded ``ReadableSpan`` objects.
    """
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracing._provider = provider  # noqa: SLF001 -- see docstring above
    return exporter


class _FakeObjectStore:
    """A stand-in ``ObjectStore`` -- ``run_ingest`` (08-11) writes a report via ``put_object``."""

    def put_object(self, bucket: str, key: str, body: bytes) -> None:
        del bucket, key, body


class _FakeCursor:
    def execute(self, *args: object, **kwargs: object) -> _FakeCursor:
        del args, kwargs
        return self

    def fetchone(self) -> None:
        return None


class _FakeConnection:
    def execute(self, *args: object, **kwargs: object) -> _FakeCursor:
        del args, kwargs
        return _FakeCursor()

    def transaction(self) -> Self:
        return self

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        del exc


class _FakeConnectionCtx:
    def __enter__(self) -> _FakeConnection:
        return _FakeConnection()

    def __exit__(self, *exc: object) -> None:
        del exc


class _FakePool:
    def connection(self) -> _FakeConnectionCtx:
        return _FakeConnectionCtx()


class _FakeStagingLoader:
    """Stands in for ``dataplat.load.staging.StagingLoader`` -- never touches a real DB."""

    def __init__(self, *, target_columns: tuple[str, ...], chunk_size: int = 1000) -> None:
        del target_columns, chunk_size

    def load(self, ctx: PipelineContext, staging_conn: object, on_progress: Any) -> StagingResult:
        del ctx, staging_conn
        on_progress(2, 2)
        return StagingResult(
            staging_table="staging.fake__test",
            rows_read=2,
            rows_parsed=2,
            rows_rejected=0,
            schema_version_id=None,
        )


@dataclass
class _FakePublishResult:
    rows_affected: int = 2


class _FakePublisher:
    def publish(self, ctx: PipelineContext, staging_table: str, conn: object) -> _FakePublishResult:
        del ctx, staging_table, conn
        return _FakePublishResult()


def _fake_resolve_publisher(strategy: str) -> _FakePublisher:
    del strategy
    return _FakePublisher()


def _boom_resolve_publisher(strategy: str) -> _FakePublisher:
    del strategy
    msg = "simulated crash before the publish transaction begins"
    raise RuntimeError(msg)


@dataclass
class _FakeMetadataRepository:
    """An in-memory ``MetadataRepository`` double covering exactly what ``run_ingest`` calls."""

    claim_result: tuple[int, str] | None
    status_for_skip: str | None = None
    claim_calls: list[dict[str, object]] = field(default_factory=list)

    def claim_ingestion_run(  # noqa: PLR0913 -- mirrors the real Protocol's column set
        self,
        *,
        idempotency_key: str,
        try_number: int,
        pod_name: str,
        trace_id: str | None = None,
        span_id: str | None = None,
        dag_id: str | None = None,
        dag_run_id: str | None = None,
        task_id: str | None = None,
        map_index: int | None = None,
        k8s_namespace: str | None = None,
    ) -> tuple[int, str] | None:
        self.claim_calls.append(
            {
                "idempotency_key": idempotency_key,
                "try_number": try_number,
                "pod_name": pod_name,
                "trace_id": trace_id,
                "span_id": span_id,
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "task_id": task_id,
                "map_index": map_index,
                "k8s_namespace": k8s_namespace,
            },
        )
        return self.claim_result

    def get_ingestion_run_status(self, *, run_id: int) -> str | None:
        del run_id
        return self.status_for_skip

    def finalize_publication(  # noqa: PLR0913 -- mirrors the real Protocol's column set
        self,
        *,
        conn: object,
        run_id: int,
        file_id: int,
        batch_id: int,
        rows_loaded: int,
        finished_at: object,
        duration_ms: int,
        report_uri: str | None,
        schema_version_id: int | None = None,
    ) -> None:
        del conn, run_id, file_id, batch_id, rows_loaded
        del finished_at, duration_ms, report_uri, schema_version_id

    def heartbeat_ingestion_run(
        self,
        *,
        run_id: int,
        lease_expires_at: object,
        rows_read: int,
        rows_parsed: int,
    ) -> None:
        del run_id, lease_expires_at, rows_read, rows_parsed

    def record_validation_results(
        self,
        *,
        conn: object,
        run_id: int,
        results: object,
    ) -> None:
        del conn, run_id, results

    def record_rejected_records(
        self,
        *,
        conn: object,
        run_id: int,
        file_id: int,
        batch_id: int,
        rejected: object,
    ) -> None:
        del conn, run_id, file_id, batch_id, rejected

    def resolve_rejected_records_for_batch(
        self,
        *,
        conn: object,
        batch_id: int,
        resolved_by_run_id: int,
        resolution_type: str,
    ) -> int:
        del conn, batch_id, resolved_by_run_id, resolution_type
        return 0


def _make_config() -> DatasetConfig:
    """A minimal, valid `DatasetConfig` -- `run_ingest` itself never reads `.columns`."""
    return DatasetConfig(
        dataset="customers",
        config_schema_version=1,
        source=SourceConfig(
            type="csv",
            bucket="unit-test-bucket",
            path="customers/",
            change_semantics="snapshot",
            duplicate_policy="skip",
        ),
        deduplication=DeduplicationConfig(
            strategy="business_key_latest",
            keys=["customer_id"],
            order_by=["event_ts desc"],
        ),
        load=LoadConfig(strategy="merge", target="normalized.customers"),
        batching=BatchingConfig(max_units_per_run=100),
        columns=[
            ColumnContract(
                name="customer_id",
                type="string",
                nullable=False,
                required=True,
                business_key=True,
                description="Natural business key for a customer record",
            ),
        ],
    )


def _make_ctx(*, metadata: _FakeMetadataRepository) -> PipelineContext:
    # Every fake below is a structural double satisfying its Protocol/class
    # shape at runtime (see each class's own docstring) but not nominally --
    # `type: ignore[arg-type]` on each is expected, not a real type error.
    return PipelineContext(
        run=RunContext(run_id=1, idempotency_key="run_ingest_trace_unit:1", file_id=1, batch_id=1),
        config=_make_config(),
        metadata=metadata,  # type: ignore[arg-type]
        objects=_FakeObjectStore(),  # type: ignore[arg-type]
        db=_FakePool(),  # type: ignore[arg-type]
        log=get_logger(),
    )


def _record_increment_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> list[tuple[str, int, dict[str, str]]]:
    """Monkeypatch ``metrics.increment`` to record every call made through it.

    Patches the SAME module object ``run_module.metrics`` refers to
    (``dataplat.pipeline.run``'s own ``from dataplat.observability import
    metrics, tracing``) -- imported directly here, rather than reached via
    ``run_module.metrics``, since ``strict = true`` (``no_implicit_reexport``)
    does not consider ``metrics`` an explicitly re-exported attribute of
    ``dataplat.pipeline.run``.
    """
    calls: list[tuple[str, int, dict[str, str]]] = []

    def _record(name: str, value: int = 1, **labels: str) -> None:
        calls.append((name, value, labels))

    monkeypatch.setattr(metrics, "increment", _record)
    return calls


# --- Behavior 1: no active parent context -> None/None ---------------------


def test_claim_receives_none_trace_id_and_span_id_with_no_parent_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracing.configure(otlp_endpoint=None)
    monkeypatch.setattr(run_module, "StagingLoader", _FakeStagingLoader)
    monkeypatch.setattr(run_module, "resolve_publisher", _fake_resolve_publisher)
    metadata = _FakeMetadataRepository(claim_result=(1, "RUNNING"))
    ctx = _make_ctx(metadata=metadata)

    receipt = run_ingest(ctx)

    assert receipt.status == "SUCCEEDED"
    assert len(metadata.claim_calls) == 1
    assert metadata.claim_calls[0]["trace_id"] is None
    assert metadata.claim_calls[0]["span_id"] is None


# --- Behavior 2: a valid parent context -> genuine child span --------------


def test_claim_receives_a_child_span_matching_trace_id_and_a_new_span_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_in_memory_tracing()
    monkeypatch.setattr(run_module, "StagingLoader", _FakeStagingLoader)
    monkeypatch.setattr(run_module, "resolve_publisher", _fake_resolve_publisher)
    metadata = _FakeMetadataRepository(claim_result=(1, "RUNNING"))
    ctx = _make_ctx(metadata=metadata)

    parent_ctx = propagate.extract({"traceparent": _WELL_FORMED_TRACEPARENT})
    token = otel_context.attach(parent_ctx)
    try:
        run_ingest(ctx)
    finally:
        otel_context.detach(token)

    assert len(metadata.claim_calls) == 1
    trace_id = metadata.claim_calls[0]["trace_id"]
    span_id = metadata.claim_calls[0]["span_id"]
    assert isinstance(trace_id, str)
    assert isinstance(span_id, str)
    assert trace_id == _INJECTED_TRACE_ID_HEX  # SAME trace -- cross-process continuity
    assert span_id != _INJECTED_PARENT_SPAN_ID_HEX  # NEW span -- never a copy of the parent's
    assert len(trace_id) == 32
    assert len(span_id) == 16
    assert trace_id == trace_id.lower()
    assert span_id == span_id.lower()


# --- Behavior 3: runs_started/runs_finished on a normal success ------------


def test_runs_started_and_runs_finished_succeeded_are_each_emitted_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracing.configure(otlp_endpoint=None)
    calls = _record_increment_calls(monkeypatch)
    monkeypatch.setattr(run_module, "StagingLoader", _FakeStagingLoader)
    monkeypatch.setattr(run_module, "resolve_publisher", _fake_resolve_publisher)
    metadata = _FakeMetadataRepository(claim_result=(1, "RUNNING"))
    ctx = _make_ctx(metadata=metadata)

    receipt = run_ingest(ctx)

    assert receipt.status == "SUCCEEDED"
    started = [c for c in calls if c[0] == "runs_started"]
    finished = [c for c in calls if c[0] == "runs_finished"]
    assert len(started) == 1
    assert len(finished) == 1
    assert started[0][2]["dataset"] == "customers"
    assert started[0][2]["status"] == "running"
    assert finished[0][2]["dataset"] == "customers"
    assert finished[0][2]["status"] == "succeeded"


# --- Behavior 4: run-fatal exception -> runs_finished(failed) + propagates -


def test_runs_finished_failed_is_emitted_and_the_exception_still_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracing.configure(otlp_endpoint=None)
    calls = _record_increment_calls(monkeypatch)
    monkeypatch.setattr(run_module, "StagingLoader", _FakeStagingLoader)
    monkeypatch.setattr(run_module, "resolve_publisher", _boom_resolve_publisher)
    metadata = _FakeMetadataRepository(claim_result=(1, "RUNNING"))
    ctx = _make_ctx(metadata=metadata)

    with pytest.raises(RuntimeError, match="simulated crash"):
        run_ingest(ctx)

    started = [c for c in calls if c[0] == "runs_started"]
    finished = [c for c in calls if c[0] == "runs_finished"]
    assert len(started) == 1
    assert len(finished) == 1
    assert finished[0][2]["status"] == "failed"


# --- Behavior 5: a refused claim emits neither metric -----------------------


def test_refused_claim_emits_neither_runs_started_nor_runs_finished(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracing.configure(otlp_endpoint=None)
    calls = _record_increment_calls(monkeypatch)
    metadata = _FakeMetadataRepository(claim_result=None, status_for_skip="SUCCEEDED")
    ctx = _make_ctx(metadata=metadata)

    receipt = run_ingest(ctx)

    assert receipt.status == "SKIPPED_DUPLICATE"
    assert calls == []


# --- Behavior 6: pipeline.publish is a genuine child of pipeline.run_ingest -


def test_publish_span_is_a_genuine_child_of_the_run_ingest_span(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exporter = _configure_in_memory_tracing()
    monkeypatch.setattr(run_module, "StagingLoader", _FakeStagingLoader)
    monkeypatch.setattr(run_module, "resolve_publisher", _fake_resolve_publisher)
    metadata = _FakeMetadataRepository(claim_result=(1, "RUNNING"))
    ctx = _make_ctx(metadata=metadata)

    run_ingest(ctx)

    spans = {span.name: span for span in exporter.get_finished_spans()}
    assert "pipeline.run_ingest" in spans
    assert "pipeline.publish" in spans
    run_span = spans["pipeline.run_ingest"]
    publish_span = spans["pipeline.publish"]
    assert publish_span.parent is not None
    assert publish_span.parent.span_id == run_span.context.span_id
