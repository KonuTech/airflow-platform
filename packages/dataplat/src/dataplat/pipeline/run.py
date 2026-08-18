"""``stage_ingest``/``publish_ingest`` -- the pod-side orchestration, split (D-02/D-04).

This module used to define one function, ``run_ingest``: claim, stage,
publish (one atomic transaction), receipt -- all in a single pod invocation.
Plan 08.1-10 splits it into two, matching this phase's dbt bronze-to-silver
architecture:

- ``stage_ingest`` -- claim (``MetadataRepository.claim_ingestion_run``),
  stream through ``StagingLoader`` into a per-run scratch buffer, run the
  referential-integrity/circuit-breaker/volume-anomaly quality gate BEFORE
  any of it becomes durable, promote whatever survives into the durable,
  cumulative bronze table (``StagingLoader.promote_to_durable_bronze``,
  plan 08.1-06) and mark the run ``STAGED`` -- never ``SUCCEEDED``. dbt reads
  bronze between here and ``publish_ingest`` (plan 08.1-08's incremental
  models, orchestrated by a dedicated DAG task, plan 08.1-12), consolidating
  possibly-several ``stage_ingest`` runs' bronze contributions into
  ``silver.<dataset>``.
- ``publish_ingest`` -- a single, unmapped, per-dataset invocation (never
  per-run): claims every currently-``STAGED`` run via ``meta.run_stages``
  (plan 08.1-07), publishes ``silver.<dataset>``'s current state into
  ``normalized.<dataset>`` through the SAME ``Publisher``/advisory-lock
  mechanism ``run_ingest`` always used (unchanged from before this plan --
  only the source table argument differs), and finalizes every claimed run
  in ONE atomic transaction (META-03's single-transaction guarantee,
  unchanged).

Why the quality-gate logic moved to ``stage_ingest`` (not incidental scope
creep -- a genuine consequence of D-04's split): before this plan,
``_apply_referential_barrier`` and the circuit breaker ran inside the publish
transaction, filtering the PER-RUN SCRATCH staging table immediately before
the publish ``SELECT`` read it. After this plan, that scratch table's whole
lifecycle (create -> COPY -> promote -> drop) collapses into ``stage_ingest``
alone -- by the time ``publish_ingest`` ever runs, the scratch table is long
gone, and ``publish_ingest`` may be finalizing SEVERAL ``stage_ingest`` runs'
worth of dbt-consolidated silver data in one pass, so a per-run filtering
step no longer has a single scratch table to operate against. Filtering at
staging time -- before any of it reaches durable bronze -- is also strictly
*more* correct: bad data never even enters the append-only, cumulative
bronze table dbt reads from. ``resolve_rejected_records_for_business_keys``
is the ONE exception that stays in ``publish_ingest``: it must use
``published_business_keys`` from the actual publish result (a
publish-time-only concept, CR-01's own already-proven reasoning), never from
rows that merely survived staging.

Both functions are source-agnostic: neither knows or cares whether
``ctx.source`` is a CSV, a future JSON/Parquet source, or anything else --
that knowledge lives entirely behind the ``Source``/``StreamingStage``
protocol seam (ADR-0008). ``publish_ingest`` additionally never even reads
``ctx.source`` at all -- it operates purely on ``silver.<dataset>``, already
resolved by ``ctx.config.dataset``.

The heartbeat thread (``stage_ingest`` only -- ``publish_ingest`` never
claims ``meta.ingestion_runs`` directly, so it has no lease to heartbeat) is
D-11's actual mechanism: ``meta.ingestion_runs.rows_read``/``rows_parsed``
are kept genuinely live during a long staging load (refreshed on an
interval, alongside the crash-recovery lease), not left ``NULL`` until the
final status update runs at the very end.

Neither function catches anything: a run-fatal exception (a genuinely broken
staging table create, a claim upsert failing for a reason other than
"already claimed", a publish-transaction failure) propagates OUT, uncaught,
to whichever CLI command called it -- the "always write a receipt, even on a
run-fatal failure" contract belongs to that call site, not to this module. On
every exit path, success or failure, ``stage_ingest``/``publish_ingest`` each
guarantee two things: their own heartbeat/claim-adjacent state is left
consistent, and -- once real work has genuinely begun -- a ``runs_finished``
counter increment is observed (D-03's live "runs currently in-flight"/"recent
failure rate" gauges). The latter is emitted from a ``finally`` block, never
an ``except``, so a run-fatal exception is a pure side-effect observation on
the way through -- never swallowed, never converted.

``stage_ingest`` runs inside its own ``pipeline.stage_ingest`` span (renamed
from ``pipeline.run_ingest``, OBS-10): a genuine CHILD of whatever parent
context ``dataplat.cli``'s ``TRACEPARENT`` extraction attached, or a fresh
root span when no parent was ever extracted. Its ``trace_id``/``span_id`` are
captured and passed straight into ``claim_ingestion_run``, so
``meta.ingestion_runs.trace_id`` carries the SAME trace id Airflow's own task
span started with. ``publish_ingest`` keeps the SAME ``pipeline.publish``
child span name ``run_ingest`` always used for its own atomic publish
transaction -- unchanged, since that segment's shape (advisory lock ->
Publisher -> finalize) is otherwise identical to before.
"""

from __future__ import annotations

import dataclasses
import json
import os
import threading
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

from opentelemetry import trace as otel_trace

from dataplat.errors import ConfigurationError, DataPlatformError
from dataplat.load.publish.registry import resolve_publisher
from dataplat.load.staging import StagingLoader
from dataplat.models.receipt import Receipt
from dataplat.observability import metrics, tracing
from dataplat.observability.logging import get_logger
from dataplat.validate.circuit_breaker import RejectionRateCircuitBreaker
from dataplat.validate.referential import ReferentialIntegrityBarrier
from dataplat.validate.volume_anomaly import VolumeAnomalyBarrier

if TYPE_CHECKING:
    from psycopg import Connection

    from dataplat.config.model import QualityRuleConfig
    from dataplat.load.staging import StagingResult
    from dataplat.models.record import RejectedRecord
    from dataplat.models.report import ValidationResult
    from dataplat.pipeline.protocol import PipelineContext

# Well under the 5-minute lease `claim_ingestion_run`/this module's own
# heartbeat both use (04-RESEARCH.md Pattern 1) -- a keyword-only override
# point (not a hardcoded literal) exists solely so this module's own
# integration tests can shrink it far below 60s and observe a heartbeat
# fire without the test itself taking a minute; production call sites never
# pass anything but the default.
_DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 60.0
_LEASE_DURATION = timedelta(minutes=5)

# A dataset-keyed lookup, not yet a generic config-derived resolution --
# `DatasetConfig` carries no canonical "business columns in order" field yet
# (08-05-PLAN.md's own framing, mirroring `MergePublisher`'s/
# `OrdersMergePublisher`'s own hardcoded per-dataset business-column lists,
# `load/publish/merge.py`/`merge_orders.py`'s module docstrings). Phase 4
# started this as a single hardcoded `_CUSTOMERS_TARGET_COLUMNS` constant
# ("one dataset (customers)", 04-CONTEXT.md); this phase adds `orders` as a
# second real dataset (08-CONTEXT.md D-13..D-17) without inventing the
# generic resolution -- now keyed by dataset name so a second real dataset
# does not silently reuse the first one's column list. A third dataset added
# later without an entry here fails loudly at the lookup site below (a named
# `DataPlatformError`), never a bare `KeyError` and never a silent fallback
# onto another dataset's columns.
_TARGET_COLUMNS_BY_DATASET: dict[str, tuple[str, ...]] = {
    "customers": ("customer_id", "name", "country", "birth_date", "event_ts"),
    "orders": ("order_id", "customer_id", "order_date", "amount"),
}


def _target_columns_for_dataset(dataset: str) -> tuple[str, ...]:
    """Resolve ``dataset`` through ``_TARGET_COLUMNS_BY_DATASET``, or fail loudly.

    Split out of ``stage_ingest`` itself purely to keep that function's own
    statement count under ``PLR0915``'s threshold -- no behavior change from
    an inlined lookup.

    Args:
        dataset: ``ctx.config.dataset``, e.g. ``"customers"``/``"orders"``.

    Returns:
        That dataset's ordered staging target columns.

    Raises:
        DataPlatformError: ``dataset`` has no entry -- named and structured,
            never a bare ``KeyError`` and never a silent fallback onto
            another dataset's columns.
    """
    try:
        return _TARGET_COLUMNS_BY_DATASET[dataset]
    except KeyError:
        msg = f"stage_ingest has no _TARGET_COLUMNS_BY_DATASET entry for dataset {dataset!r}"
        raise DataPlatformError(
            msg,
            context={"dataset": dataset, "known_datasets": sorted(_TARGET_COLUMNS_BY_DATASET)},
        ) from None


class _Progress:
    """A small, mutable holder for the heartbeat's live row counts.

    Written by the staging loader's ``on_progress`` callback (the thread
    running ``stage_ingest`` itself), read by the heartbeat thread -- plain
    attribute assignment from one writer is safe to read from another
    thread under the GIL for this monitoring-only, eventually-consistent
    use case (D-11); no lock is needed.

    Attributes:
        rows_read: The cumulative rows read so far, as of the last
            ``on_progress`` call. Starts at ``0``.
        rows_parsed: The cumulative rows parsed (staged) so far, as of the
            last ``on_progress`` call. Starts at ``0``.
    """

    __slots__ = ("rows_parsed", "rows_read")

    def __init__(self) -> None:
        """Initialize both counters to ``0``."""
        self.rows_read = 0
        self.rows_parsed = 0


def _skipped_receipt(ctx: PipelineContext) -> Receipt:
    """Build the ``Receipt`` for a claim that ``claim_ingestion_run`` refused.

    No staging is ever attempted on this path (behavior spec) -- this is
    the very first thing ``stage_ingest`` may do, before any connection to
    ``ctx.db`` is opened for staging.

    A ``'STAGED'`` row (this plan's own new terminal status for
    ``stage_ingest``) is treated identically to ``'SUCCEEDED'`` here (plan
    08.1-10 Task 1 Test 3's own decision, recorded in this plan's SUMMARY):
    both mean "this run's ``stage_ingest`` work already genuinely completed,
    a repeat call is a legitimate duplicate, not an error" --
    ``claim_ingestion_run``'s own claimability predicate (``status IN
    ('PENDING', 'FAILED') OR (status = 'RUNNING' AND lease expired)``)
    already excludes ``'STAGED'`` exactly the same way it excludes
    ``'SUCCEEDED'``, so the two statuses share the same "already done, do
    not redo" semantics from this function's point of view.

    Args:
        ctx: The current pipeline context. Only ``ctx.run``/``ctx.metadata``
            are read.

    Returns:
        A ``Receipt`` whose ``status`` is ``"SKIPPED_DUPLICATE"`` (the run
        already ``SUCCEEDED`` or ``STAGED``) or ``"SKIPPED_CONCURRENT"``
        (the run is ``RUNNING`` under a still-live lease held by another
        claimant).

    Raises:
        DataPlatformError: ``claim_ingestion_run`` refused the claim but
            ``get_ingestion_run_status`` finds none of ``SUCCEEDED``,
            ``STAGED`` or ``RUNNING`` explaining the refusal (including no
            row at all) -- a genuine run-fatal condition: this phase's
            ``ingest`` CLI always pre-allocates the run at discovery time
            before ``stage_ingest`` is ever called, so an unexplained
            refusal means that invariant did not hold.
    """
    log = get_logger()
    status = ctx.metadata.get_ingestion_run_status(run_id=ctx.run.run_id)
    if status in ("SUCCEEDED", "STAGED"):
        receipt_status = "SKIPPED_DUPLICATE"
    elif status == "RUNNING":
        receipt_status = "SKIPPED_CONCURRENT"
    else:
        msg = (
            "claim_ingestion_run refused the claim but no SUCCEEDED/STAGED/RUNNING "
            "row explains why"
        )
        raise DataPlatformError(
            msg,
            context={
                "idempotency_key": ctx.run.idempotency_key,
                "run_id": ctx.run.run_id,
                "status": status,
            },
        )
    log.info(
        "stage_ingest.skipped",
        run_id=ctx.run.run_id,
        idempotency_key=ctx.run.idempotency_key,
        status=receipt_status,
    )
    return Receipt(
        run_id=ctx.run.run_id,
        status=receipt_status,
        rows_read=0,
        rows_loaded=0,
        rows_invalid=0,
        rows_deduplicated=0,
        duration_ms=0,
        rows_quarantined=0,
        report_uri=None,
    )


def _find_quality_rule(ctx: PipelineContext, rule_type: str) -> QualityRuleConfig | None:
    """Return this run's first ``ctx.config.quality.rules`` entry matching ``rule_type``.

    Split out purely to keep the caller's own statement count under
    ``PLR0915``'s threshold -- no behavior change from an inlined loop.
    Returns ``None`` both when ``ctx.config.quality`` itself is unset and
    when it is set but declares no rule of this type -- every caller treats
    both cases identically ("this barrier is not configured for this
    dataset").

    Args:
        ctx: The current pipeline context. Only ``ctx.config.quality`` is
            read.
        rule_type: The ``QualityRuleConfig.rule_type`` to look for, e.g.
            ``"REFERENTIAL"`` or ``"VOLUME"``.

    Returns:
        The first matching rule, in declared order, or ``None``.
    """
    if ctx.config.quality is None:
        return None
    for rule in ctx.config.quality.rules:
        if rule.rule_type == rule_type:
            return rule
    return None


def _apply_referential_barrier(
    ctx: PipelineContext,
    conn: Connection[Any],
    staging_result: StagingResult,
) -> tuple[list[RejectedRecord], list[ValidationResult]]:
    """Run ``ReferentialIntegrityBarrier`` when configured, deleting every orphan from staging.

    Split out purely to keep the caller's own statement count under
    ``PLR0915``'s threshold. A no-op (returns two empty lists, no query
    issued) when ``ctx.config.quality`` declares no ``REFERENTIAL`` rule --
    this is what keeps a dataset with no referential relationship
    (``customers``) behaving exactly as before.

    Args:
        ctx: The current pipeline context.
        conn: ``stage_ingest``'s own ``staging_conn`` (plan 08.1-10) --
            before this plan, this ran against a SEPARATE publish
            transaction's connection; there is no separate publish
            transaction anymore at this point in the control flow, so this
            now runs on the SAME connection ``StagingLoader.load()`` already
            used. Every orphan-row ``DELETE`` this function issues lands
            inside that same connection's implicit transaction, so a later
            exception (e.g. the circuit breaker tripping) rolls back
            everything on it, including the just-completed COPY, when the
            caller's ``with`` block exits.
        staging_result: This run's already-completed ``StagingLoader.load()``
            result -- ``staging_result.staging_table`` names the table to
            anti-join and delete orphan rows from.

    Returns:
        ``(rejected, findings)``: one ``RejectedRecord`` per orphan row
        (already deleted from the staging table by this call, via ``conn``)
        and this barrier's own single ``ValidationResult`` finding. Both
        empty when no ``REFERENTIAL`` rule is configured.

    Raises:
        ConfigurationError: A configured ``REFERENTIAL`` rule declares no
            ``column``, or no ``params.target_table`` -- both are required
            for this barrier to know what to check.
    """
    rule = _find_quality_rule(ctx, "REFERENTIAL")
    if rule is None:
        return [], []

    if rule.column is None:
        msg = f"quality rule {rule.rule_id!r} (rule_type=REFERENTIAL) declares no column"
        raise ConfigurationError(msg, context={"rule_id": rule.rule_id})

    target_table = rule.params.get("target_table")
    if target_table is None:
        msg = (
            f"quality rule {rule.rule_id!r} (rule_type=REFERENTIAL) declares no params.target_table"
        )
        raise ConfigurationError(msg, context={"rule_id": rule.rule_id})

    barrier = ReferentialIntegrityBarrier(
        staging_table=staging_result.staging_table,
        target_table=str(target_table),
        target_column=rule.column,
        staging_column=rule.column,
        strategy=rule.strategy,
        rule_id=rule.rule_id,
    )
    barrier_result = barrier.apply(ctx)

    for orphan in barrier_result.rejected:
        conn.execute(
            f"DELETE FROM {staging_result.staging_table} WHERE _source_row_number = %s",  # noqa: S608 -- staging_result.staging_table is a config/run-derived identifier (T-04-01), the bound value is an internal ordinal, never CSV content
            (orphan.source_row_number,),
        )

    return list(barrier_result.rejected), list(barrier_result.findings)


def _apply_staging_quality_gate_and_persist(  # noqa: PLR0913 -- one keyword per already-known run identity/result value, mirrors merge_orders.py's shape
    ctx: PipelineContext,
    conn: Connection[Any],
    *,
    run_id: int,
    file_id: int,
    batch_id: int,
    finished_at: datetime,
    staging_result: StagingResult,
    all_rejected: list[RejectedRecord],
    all_findings: list[ValidationResult],
) -> str:
    """Run circuit-breaker/volume barriers, persist findings/rejects, write the MinIO report.

    Renamed from ``_apply_post_publish_barriers_and_persist`` (plan 08.1-10):
    every step here now runs BEFORE ``StagingLoader.promote_to_durable_bronze``,
    inside ``stage_ingest``'s own ``staging_conn`` -- matching this plan's own
    ordering rationale that bad data must never reach durable bronze, not
    merely never reach gold. The one piece that stayed behind in
    ``publish_ingest`` is ``resolve_rejected_records_for_business_keys``: it
    needs ``published_business_keys``, a publish-time-only value this
    function has no access to (``Publisher.publish()`` has not run yet at
    staging time).

    Args:
        ctx: The current pipeline context.
        conn: ``stage_ingest``'s own ``staging_conn`` -- every write here
            (validation results, rejected records) lands inside that same
            connection's implicit transaction. A ``QualityThresholdExceeded``
            raised by the circuit breaker propagates straight out of this
            function uncaught, rolling back everything written here plus
            everything the caller already wrote on ``conn`` -- including the
            just-completed COPY (D-11), since ``promote_to_durable_bronze``
            has not run yet at this point in ``stage_ingest``'s own control
            flow.
        run_id: This run's ``meta.ingestion_runs.run_id``.
        file_id: This run's ``meta.files.file_id``.
        batch_id: This run's ``meta.batches.batch_id`` -- only drives
            ``record_rejected_records``' own insert here; D-05's resolution
            scope moved off this parameter under D-23 (business-key-scoped,
            not batch-scoped), and resolution itself now lives in
            ``publish_ingest``, not here.
        finished_at: This run's staging-completion timestamp, reused for the
            report artifact's ``generated_at`` field -- the SAME value
            ``stage_ingest``'s own status update receives, never
            independently computed.
        staging_result: This run's already-completed ``StagingLoader.load()``
            result.
        all_rejected: Every ``RejectedRecord`` known so far this run --
            seeded by the caller from staging-time rejections plus the
            referential barrier's own orphans.
        all_findings: Every ``ValidationResult`` known so far this run --
            seeded by the caller from the referential barrier's own finding,
            when one ran. Mutated in place (extended) by this function.

    Returns:
        This run's report artifact URI, e.g.
        ``"s3://validated/customers/8123/report.json"`` -- always non-``None``
        for a run that reaches this call (VALID-04's MinIO-artifact half).
    """
    # D-23: computed unconditionally, once, at the top of this function --
    # the VOLUME barrier below needs it, and it must never be queried twice
    # for one run.
    dataset_id = ctx.metadata.get_or_create_dataset(ctx.config.dataset)

    if ctx.config.quality is not None and ctx.config.quality.rejection_rate_threshold is not None:
        circuit_breaker = RejectionRateCircuitBreaker(
            threshold=ctx.config.quality.rejection_rate_threshold,
            total_rows_read=staging_result.rows_read,
            total_rows_rejected=len(all_rejected),
        )
        all_findings.extend(circuit_breaker.apply(ctx).findings)

    volume_rule = _find_quality_rule(ctx, "VOLUME")
    if volume_rule is not None:
        volume_barrier = VolumeAnomalyBarrier(
            dataset_id=dataset_id,
            current_row_count=staging_result.rows_parsed,
            multiplier=float(
                cast("float | int | str", volume_rule.params.get("multiplier", 10.0)),
            ),
            rule_id=volume_rule.rule_id,
            strategy=volume_rule.strategy,
        )
        all_findings.extend(volume_barrier.apply(ctx).findings)

    ctx.metadata.record_validation_results(conn=conn, run_id=run_id, results=all_findings)
    if all_rejected:
        ctx.metadata.record_rejected_records(
            conn=conn,
            run_id=run_id,
            file_id=file_id,
            batch_id=batch_id,
            rejected=all_rejected,
        )

    # VALID-04's MinIO-artifact half -- the SAME all_findings/all_rejected
    # objects just persisted to Postgres above, never a second,
    # independently-computed view.
    report = {
        "run_id": run_id,
        "dataset": ctx.config.dataset,
        "file_id": file_id,
        "batch_id": batch_id,
        "generated_at": finished_at.isoformat(),
        "validation_results": [dataclasses.asdict(finding) for finding in all_findings],
        "rejected_records": [dataclasses.asdict(record) for record in all_rejected],
    }
    report_key = f"{ctx.config.dataset}/{run_id}/report.json"
    ctx.objects.put_object(
        bucket="validated",
        key=report_key,
        body=json.dumps(report, default=str).encode("utf-8"),
    )
    return f"s3://validated/{report_key}"


def _heartbeat_loop(
    ctx: PipelineContext,
    run_id: int,
    progress: _Progress,
    stop_event: threading.Event,
    interval_seconds: float,
) -> None:
    """Refresh ``run_id``'s lease and live row counts on ``interval_seconds``, until stopped.

    Runs on its own daemon thread. ``stop_event.wait(interval_seconds)``
    both provides the sleep between heartbeats AND the immediate-wake stop
    signal -- it returns ``True`` (loop exits, no further heartbeat) the
    moment ``stop_event`` is set, and ``False`` (one more heartbeat runs)
    after a full interval with no stop signal.

    CR-01 (``04-REVIEW.md``): this is exactly the call site where a stray
    tick could otherwise regress a just-committed terminal status (today,
    ``STAGED`` -- ``stage_ingest`` is this loop's only caller) back to
    ``RUNNING``. ``stop_event.set()`` (in ``stage_ingest``'s ``finally``
    block) only fires AFTER staging's own connection has already committed
    -- a tick landing in that narrow window still calls this loop's body one
    more time. Calling ``ctx.metadata.heartbeat_ingestion_run`` (not the
    generic, unconditional ``update_ingestion_run_status``) makes that stray
    call a silent no-op instead of a status regression: its ``WHERE status =
    'RUNNING'`` guard means a run that has already reached a terminal status
    is left untouched.

    Args:
        ctx: The current pipeline context. Only ``ctx.metadata`` is used.
        run_id: The claimed run this heartbeat keeps alive.
        progress: The shared, mutable holder ``on_progress`` writes and this
            loop reads -- never mutated here, only read.
        stop_event: Set by ``stage_ingest``'s ``finally`` block to stop this
            loop.
        interval_seconds: Seconds between heartbeats. Production callers
            never override ``stage_ingest``'s default; this module's own
            integration tests shrink it so a heartbeat is observable
            without a real-time wait.
    """
    while not stop_event.wait(interval_seconds):
        ctx.metadata.heartbeat_ingestion_run(
            run_id=run_id,
            lease_expires_at=datetime.now(tz=UTC) + _LEASE_DURATION,
            rows_read=progress.rows_read,
            rows_parsed=progress.rows_parsed,
        )


def stage_ingest(
    ctx: PipelineContext,
    *,
    heartbeat_interval_seconds: float = _DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
) -> Receipt:
    """Claim, stage, quality-gate, promote to durable bronze, and mark one run ``STAGED``.

    Replaces ``run_ingest``'s claim-through-publish body up to (but not
    including) publication itself (D-04): this function's own terminal
    status is ``"STAGED"``, never ``"SUCCEEDED"`` -- publication is
    ``publish_ingest``'s job, run separately, possibly consolidating several
    ``stage_ingest`` runs' bronze contributions (via dbt) into one pass.

    Args:
        ctx: The current pipeline context -- ``ctx.run.idempotency_key``
            names the run to claim; ``ctx.run.file_id``/``ctx.run.batch_id``
            (populated by the ``ingest``/``stage`` CLI from the frozen
            ``AssignmentDocument`` before this function is ever called) are
            the file/batch this run's staging progress is recorded against;
            ``ctx.source`` is the already source-specific reader this
            function streams through ``StagingLoader`` without ever
            inspecting.
        heartbeat_interval_seconds: Seconds between lease/row-count
            heartbeats, well under the 5-minute lease duration. Defaults to
            60s. This module's own tests override it directly.

    Returns:
        A ``Receipt``: ``status="STAGED"`` after a genuine claim, stage and
        durable-bronze promotion (``rows_loaded=0`` -- staging never writes
        to gold); ``status="SKIPPED_DUPLICATE"`` or
        ``status="SKIPPED_CONCURRENT"`` immediately, with no staging
        attempted, when the claim was refused because the run already
        reached ``STAGED``/``SUCCEEDED`` or is concurrently in progress
        elsewhere.

    Raises:
        DataPlatformError: The claim was refused for a reason
            ``_skipped_receipt`` cannot explain (see its own docstring), or
            ``ctx.run.file_id``/``ctx.run.batch_id`` is unset. Any other
            run-fatal exception raised while staging propagates unmodified
            -- this function adds no second catch-once boundary; see the
            module docstring.
    """
    log = get_logger()
    start = time.monotonic()

    with tracing.start_span("pipeline.stage_ingest"):
        # This run's own span -- a genuine CHILD of whatever parent context
        # `dataplat.cli`'s TRACEPARENT extraction attached (OBS-10), or a
        # fresh root span when no parent was ever extracted. Read BEFORE the
        # claim below, so the SAME trace_id/span_id land on the claimed row.
        span_context = otel_trace.get_current_span().get_span_context()
        trace_id = (
            otel_trace.format_trace_id(span_context.trace_id) if span_context.is_valid else None
        )
        span_id = otel_trace.format_span_id(span_context.span_id) if span_context.is_valid else None

        claimed = ctx.metadata.claim_ingestion_run(
            idempotency_key=ctx.run.idempotency_key,
            try_number=ctx.run.attempt,
            pod_name=os.environ.get("HOSTNAME", "unknown"),
            trace_id=trace_id,
            span_id=span_id,
            dag_id=ctx.run.dag_id,
            dag_run_id=ctx.run.dag_run_id,
            task_id=ctx.run.task_id,
            map_index=ctx.run.map_index,
            k8s_namespace=ctx.run.k8s_namespace,
        )
        if claimed is None:
            return _skipped_receipt(ctx)
        run_id, _ = claimed

        # D-04's bounded label set: dataset+stage+status, never an unbounded
        # identity like run_id/file_id/batch_id. Emitted only once a claim
        # has genuinely succeeded -- a skip is not "a run in flight."
        metrics.increment(
            "runs_started",
            1,
            dataset=ctx.config.dataset,
            stage="stage_ingest",
            status="running",
        )
        # Set to "staged" only as the LAST statement before this function's
        # normal return, below -- any exception raised anywhere in between
        # (file_id/batch_id validation, staging, the quality gate, or the
        # log.info computation) leaves this at "failed", observed by the
        # `finally` below.
        run_status = "failed"
        try:
            if ctx.run.file_id is None or ctx.run.batch_id is None:
                msg = "stage_ingest requires ctx.run.file_id and ctx.run.batch_id to be set"
                raise DataPlatformError(
                    msg,
                    context={"run_id": run_id, "idempotency_key": ctx.run.idempotency_key},
                )
            file_id = ctx.run.file_id
            batch_id = ctx.run.batch_id

            progress = _Progress()

            def _on_progress(rows_read: int, rows_parsed: int) -> None:
                progress.rows_read = rows_read
                progress.rows_parsed = rows_parsed

            stop_heartbeat = threading.Event()
            heartbeat_thread = threading.Thread(
                target=_heartbeat_loop,
                args=(ctx, run_id, progress, stop_heartbeat, heartbeat_interval_seconds),
                name=f"stage-ingest-heartbeat-{run_id}",
                daemon=True,
            )
            heartbeat_thread.start()

            try:
                loader = StagingLoader(
                    target_columns=_target_columns_for_dataset(ctx.config.dataset),
                )
                # Staging, the quality gate and the durable-bronze promotion
                # all now live on this ONE connection, in its ONE implicit
                # transaction (plan 08.1-10's whole point): a
                # QualityThresholdExceeded raised by the quality gate below
                # rolls back the COPY, the referential barrier's DELETEs and
                # the just-recorded validation/reject rows together, and
                # `promote_to_durable_bronze` -- reached only when the gate
                # does NOT raise -- never runs at all (Test 2's exact
                # guarantee). `pool.connection()`'s own context-manager
                # behavior commits on clean exit.
                with ctx.db.connection() as staging_conn:
                    staging_result = loader.load(
                        ctx,
                        staging_conn,
                        on_progress=_on_progress,
                    )

                    finished_at = datetime.now(tz=UTC)
                    all_rejected: list[RejectedRecord] = list(staging_result.rejected_records)
                    all_findings: list[ValidationResult] = []
                    referential_rejected, referential_findings = _apply_referential_barrier(
                        ctx,
                        staging_conn,
                        staging_result,
                    )
                    all_rejected.extend(referential_rejected)
                    all_findings.extend(referential_findings)

                    report_uri = _apply_staging_quality_gate_and_persist(
                        ctx,
                        staging_conn,
                        run_id=run_id,
                        file_id=file_id,
                        batch_id=batch_id,
                        finished_at=finished_at,
                        staging_result=staging_result,
                        all_rejected=all_rejected,
                        all_findings=all_findings,
                    )

                    # The ONE genuinely new call this plan adds (plan
                    # 08.1-06): everything that survived the quality gate
                    # above -- never the raw, pre-filtered scratch contents
                    # -- is appended into the durable, cumulative bronze
                    # table dbt reads from.
                    loader.promote_to_durable_bronze(ctx, staging_conn, staging_result)

                duration_ms = int((time.monotonic() - start) * 1000)
                # Replaces `finalize_publication`'s three-table UPDATE with
                # this ONE-table status transition -- files/batches stay
                # untouched here; they move to PROCESSED/PUBLISHED inside
                # `publish_ingest`'s own `finalize_publication` call instead.
                # Issued AFTER `staging_conn`'s own transaction has already
                # committed (the `with` block above has exited cleanly) --
                # `update_ingestion_run_status` opens its own separate
                # connection (the `MetadataRepository` Protocol's contract),
                # so ordering it after that commit, not before or "inside"
                # it, is what keeps a crash between the two from ever
                # showing STAGED without the durable bronze rows it claims
                # to describe already being visible.
                ctx.metadata.update_ingestion_run_status(
                    run_id=run_id,
                    status="STAGED",
                    finished_at=finished_at,
                    duration_ms=duration_ms,
                    rows_read=staging_result.rows_read,
                    rows_parsed=staging_result.rows_parsed,
                    rows_invalid=staging_result.rows_rejected,
                    report_uri=report_uri,
                    schema_version_id=staging_result.schema_version_id,
                )
            finally:
                stop_heartbeat.set()
                heartbeat_thread.join(timeout=heartbeat_interval_seconds + 5)

            log.info(
                "stage_ingest.staged",
                run_id=run_id,
                rows_read=staging_result.rows_read,
                rows_parsed=staging_result.rows_parsed,
                rows_invalid=staging_result.rows_rejected,
                duration_ms=duration_ms,
            )
            run_status = "staged"
            return Receipt(
                run_id=run_id,
                status="STAGED",
                rows_read=staging_result.rows_read,
                rows_loaded=0,
                rows_invalid=staging_result.rows_rejected,
                rows_deduplicated=0,
                duration_ms=duration_ms,
                rows_quarantined=len(all_rejected),
                report_uri=report_uri,
            )
        finally:
            # Never an `except` -- a run-fatal exception is a pure
            # side-effect observation on the way through, never swallowed or
            # converted (module docstring's "catches nothing" contract).
            metrics.increment(
                "runs_finished",
                1,
                dataset=ctx.config.dataset,
                stage="stage_ingest",
                status=run_status,
            )


def publish_ingest(ctx: PipelineContext) -> dict[str, object]:
    """Claim every currently-``STAGED`` run for one dataset, publish from silver, finalize.

    A single, unmapped invocation per dataset -- never per-run (RESEARCH.md
    Open Question 1, resolved by plan 08.1-07's ``meta.run_stages``/
    ``list_staged_run_ids`` machinery): dbt's own watermark-driven batching
    (D-05) may consolidate several ``stage_ingest`` runs' bronze
    contributions into one deduplicated silver pass, so this function reads
    ``silver.<dataset>`` unconditionally -- the SAME idempotent ``INSERT ...
    ON CONFLICT`` upsert ``merge.py`` already proves safe to re-run -- and
    finalizes every currently-``STAGED`` run in one atomic transaction
    (META-03, unchanged from ``run_ingest``'s own guarantee).

    Args:
        ctx: The current pipeline context. ``ctx.run`` is never read (this
            function is not scoped to one run); ``ctx.config.dataset``
            resolves both the dataset to query and the ``silver.<dataset>``
            source table; ``ctx.config.load.strategy``/``.target`` resolve
            the ``Publisher`` and the advisory-lock key, unchanged from
            ``run_ingest``'s own usage.

    Returns:
        A plain ``dict``, not a ``Receipt`` -- a ``Receipt`` is
        single-``run_id``-shaped, and this function may finalize several
        runs per invocation. Keys: ``"status"`` (always ``"SUCCEEDED"`` on a
        normal return -- a run-fatal exception propagates uncaught, same
        "catches nothing" contract as ``stage_ingest``), ``"runs_finalized"``
        (the list of ``run_id``s this call finalized, possibly empty),
        ``"rows_loaded"`` (this pass's total affected-row count -- see the
        aggregate-attribution note at this function's own
        ``finalize_publication`` call site), ``"duration_ms"``.
    """
    log = get_logger()
    start = time.monotonic()

    dataset_id = ctx.metadata.get_or_create_dataset(ctx.config.dataset)
    # Taken BEFORE opening any connection or transaction (Test 1's own
    # requirement) -- a dataset with nothing currently STAGED costs this
    # function exactly one read, never an advisory lock or a publish
    # statement.
    staged = ctx.metadata.list_staged_run_ids(dataset_id=dataset_id)
    if not staged:
        duration_ms = int((time.monotonic() - start) * 1000)
        log.info("publish_ingest.no_op", dataset=ctx.config.dataset)
        return {
            "status": "SUCCEEDED",
            "runs_finalized": [],
            "rows_loaded": 0,
            "duration_ms": duration_ms,
        }

    # D-04's bounded label set, mirroring `stage_ingest`'s own metrics
    # discipline -- emitted only once there is genuinely staged work to
    # finalize, matching "a skip is not a run in flight" above.
    metrics.increment(
        "runs_started",
        1,
        dataset=ctx.config.dataset,
        stage="publish_ingest",
        status="running",
    )
    run_status = "failed"
    finalized_run_ids: list[int] = []
    try:
        # `pipeline.publish` -- the SAME child span name `run_ingest` always
        # used for its own atomic publish transaction segment, unchanged.
        with (
            tracing.start_span("pipeline.publish"),
            ctx.db.connection() as conn,
            conn.transaction(),
        ):
            # Single-writer publication per dataset (LOAD-09): every writer
            # to this target serializes on the SAME advisory-lock key before
            # touching it -- UNCHANGED from `run_ingest`'s own mechanism.
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"publish:{ctx.config.load.target}",),
            )
            publisher = resolve_publisher(ctx.config.load.strategy)
            # The retargeted call (plan 08.1-06's renamed `source_table`
            # parameter): `merge.py`'s own SQL text requires ZERO change --
            # only this call site's argument value differs from before this
            # plan (a per-run scratch table, then; the dbt-consolidated
            # silver table, now).
            source_table = f"silver.{ctx.config.dataset}"
            result = publisher.publish(ctx, source_table, conn)

            finished_at = datetime.now(tz=UTC)
            duration_ms = int((time.monotonic() - start) * 1000)

            for run_id, file_id, batch_id, report_uri in staged:
                claimed_stage_id = ctx.metadata.claim_run_stage(
                    run_id=run_id,
                    stage_name="PUBLISH",
                    try_number=1,
                    pod_name=os.environ.get("HOSTNAME", "unknown"),
                )
                if claimed_stage_id is None:
                    # A concurrent claim already owns this run's publish hop
                    # -- defensive, should not happen under the advisory
                    # lock already held above, but never assumed.
                    continue

                # Aggregate, per-PASS (not per-run) attribution, documented
                # explicitly here rather than left as an unstated gap:
                # `publisher.publish()` above ran ONCE per `publish_ingest`
                # invocation as a single upsert pass over the ENTIRE
                # cumulative `silver.<dataset>` table (never scoped to one
                # run's own `_run_id` range -- `merge.py`'s `_PUBLISH_SQL`
                # has no such filter, and adding one is out of this plan's
                # scope, see 08.1-06's "merge.py's own SQL text requires
                # ZERO change" constraint), so `result.rows_affected` is
                # this pass's TOTAL affected-row count, not any single run's
                # own contribution -- every run finalized in this SAME loop
                # is attributed the SAME aggregate value. This does not
                # corrupt `normalized.<dataset>`/`silver.<dataset>` data --
                # it only means `meta.ingestion_runs.rows_loaded`, summed
                # across a multi-run finalize pass, over-counts relative to
                # any one run's true contribution. A finer, genuinely
                # per-run count would require `merge.py`'s `_PUBLISH_SQL` to
                # `RETURNING _run_id` and a client-side `GROUP BY` -- real
                # added complexity, out of this plan's scope; this is the
                # same class of accepted, documented aggregate-metric
                # imprecision as `run_ingest`'s own `rows_deduplicated`
                # precedent (see this module's own git history), not an
                # unstated gap.
                ctx.metadata.finalize_publication(
                    conn=conn,
                    run_id=run_id,
                    file_id=file_id,
                    batch_id=batch_id,
                    rows_loaded=result.rows_affected,
                    finished_at=finished_at,
                    duration_ms=duration_ms,
                    report_uri=report_uri,
                    schema_version_id=None,
                )
                ctx.metadata.complete_run_stage(
                    run_id=run_id,
                    stage_name="PUBLISH",
                    status="SUCCEEDED",
                    finished_at=finished_at,
                )
                finalized_run_ids.append(run_id)

            if finalized_run_ids:
                # D-05/D-23: resolve PENDING rejects by the business key(s)
                # THIS pass actually published, mirroring
                # `run_ingest`'s own pre-08.1-10 call exactly, except for
                # `resolved_by_run_id`'s own attribution -- documented here
                # as this plan's own deliberate simplification, mirroring
                # `rows_deduplicated`'s own approximate-but-documented
                # aggregate-attribution precedent above: a multi-run
                # finalize pass attributes resolution to the LATEST run
                # finalized this pass, never re-derived per business key.
                ctx.metadata.resolve_rejected_records_for_business_keys(
                    conn=conn,
                    dataset_id=dataset_id,
                    business_keys=result.published_business_keys,
                    resolved_by_run_id=max(finalized_run_ids),
                    resolution_type="REDRIVEN",
                )

        log.info(
            "publish_ingest.succeeded",
            dataset=ctx.config.dataset,
            runs_finalized=finalized_run_ids,
            rows_loaded=result.rows_affected,
            duration_ms=duration_ms,
        )
        run_status = "succeeded"
        return {
            "status": "SUCCEEDED",
            "runs_finalized": finalized_run_ids,
            "rows_loaded": result.rows_affected,
            "duration_ms": duration_ms,
        }
    finally:
        # Never an `except` -- same "catches nothing" contract as
        # `stage_ingest` (module docstring).
        metrics.increment(
            "runs_finished",
            1,
            dataset=ctx.config.dataset,
            stage="publish_ingest",
            status=run_status,
        )
