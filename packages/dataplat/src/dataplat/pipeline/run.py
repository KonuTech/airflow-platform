"""``run_ingest`` -- claim, stage, publish-transaction, receipt: the pod-side orchestration.

This is the one place every idempotency guarantee this platform makes
actually executes: claim-once (``MetadataRepository.claim_ingestion_run``),
single-writer publication (``pg_advisory_xact_lock`` + a ``Publisher``), and
atomic status commit (``MetadataRepository.finalize_publication`` inside the
SAME transaction as the ``Publisher``'s own write -- META-03). It is
source-agnostic: nothing here knows or cares whether ``ctx.source`` is a CSV,
a future JSON/Parquet source, or anything else -- that knowledge lives
entirely behind the ``Source``/``StreamingStage`` protocol seam (ADR-0008).

Two connections cross this function, deliberately never the same one
(ARCHITECTURE.md's checkpointing-vs-transactions split, 04-RESEARCH.md
Pattern 1): staging is checkpointed per chunk and lives OUTSIDE the publish
transaction, so a crash mid-staging never rolls back rows already COPY-ed;
publication is the one atomic barrier -- ``pg_advisory_xact_lock``, the
``Publisher``'s ``INSERT ... ON CONFLICT``, and ``finalize_publication``'s
three status ``UPDATE``s all commit or roll back together, in one
transaction, or none of them land at all.

The heartbeat thread is D-11's actual mechanism: ``meta.ingestion_runs.
rows_read``/``rows_parsed`` are kept genuinely live during a long staging
load (refreshed on an interval, alongside the crash-recovery lease), not
left ``NULL`` until ``finalize_publication`` runs at the very end -- so a
poller (an E2E test, an operator, ``make ingest-demo``) watching this run's
progress mid-load sees real, moving numbers, not silence.

This function catches nothing: a run-fatal exception (a genuinely broken
staging table create, a claim upsert failing for a reason other than
"already claimed", a publish-transaction failure) propagates OUT of
``run_ingest`` uncaught, to whichever CLI command called it (``csv_processor.
cli.ingest`` -- a later task in this plan) -- the "always write a receipt,
even on a run-fatal failure" contract belongs to that call site, not to this
one. On every exit path, success or failure, this function guarantees two
things: its own heartbeat thread is stopped, and -- once a claim has
genuinely succeeded -- a ``runs_finished`` counter increment is observed
(D-03's live "runs currently in-flight"/"recent failure rate" gauges, plan
07-05). The latter is emitted from a ``finally`` block, never an ``except``,
so a run-fatal exception is a pure side-effect observation on the way
through -- never swallowed, never converted -- and this function still
"catches nothing" in the sense above.

The whole claim-through-return body also runs inside its own
``pipeline.run_ingest`` span (OBS-10): a genuine CHILD of whatever parent
context ``dataplat.cli``'s ``TRACEPARENT`` extraction attached, or a fresh
root span when no parent was ever extracted. Its ``trace_id``/``span_id``
are captured and passed straight into ``claim_ingestion_run``, so
``meta.ingestion_runs.trace_id`` carries the SAME trace id Airflow's own
task span started with -- proving cross-process trace continuity -- while
``span_id`` is always a genuinely new value, this run's own. The atomic
publish transaction additionally gets its own nested ``pipeline.publish``
child span, the "-> PostgreSQL" segment of that same trace.
"""

from __future__ import annotations

import os
import threading
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from opentelemetry import trace as otel_trace

from dataplat.errors import DataPlatformError
from dataplat.load.publish.registry import resolve_publisher
from dataplat.load.staging import StagingLoader
from dataplat.models.receipt import Receipt
from dataplat.observability import metrics, tracing
from dataplat.observability.logging import get_logger

if TYPE_CHECKING:
    from dataplat.pipeline.protocol import PipelineContext

# Well under the 5-minute lease `claim_ingestion_run`/this module's own
# heartbeat both use (04-RESEARCH.md Pattern 1) -- a keyword-only override
# point (not a hardcoded literal) exists solely so this module's own
# integration tests can shrink it far below 60s and observe a heartbeat
# fire without the test itself taking a minute; production call sites never
# pass anything but the default.
_DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 60.0
_LEASE_DURATION = timedelta(minutes=5)

# This phase is deliberately single-dataset (04-CONTEXT.md: "one dataset
# (customers)"), mirroring `MergePublisher`'s own hardcoded business-column
# list against `normalized.customers` (`load/publish/merge.py`'s module
# docstring) -- `DatasetConfig` carries no generic "business columns" field
# to resolve this from yet. A later, generic multi-dataset design resolves
# both from config together; changing one without the other would silently
# desync staging's column list from publication's.
_CUSTOMERS_TARGET_COLUMNS: tuple[str, ...] = (
    "customer_id",
    "name",
    "country",
    "birth_date",
    "event_ts",
)


class _Progress:
    """A small, mutable holder for the heartbeat's live row counts.

    Written by the staging loader's ``on_progress`` callback (the thread
    running ``run_ingest`` itself), read by the heartbeat thread -- plain
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
    the very first thing ``run_ingest`` may do, before any connection to
    ``ctx.db`` is opened for staging or publication.

    Args:
        ctx: The current pipeline context. Only ``ctx.run``/``ctx.metadata``
            are read.

    Returns:
        A ``Receipt`` whose ``status`` is ``"SKIPPED_DUPLICATE"`` (the run
        already ``SUCCEEDED``) or ``"SKIPPED_CONCURRENT"`` (the run is
        ``RUNNING`` under a still-live lease held by another claimant).

    Raises:
        DataPlatformError: ``claim_ingestion_run`` refused the claim but
            ``get_ingestion_run_status`` finds neither a ``SUCCEEDED`` nor a
            ``RUNNING`` row explaining the refusal (including no row at
            all) -- a genuine run-fatal condition: this phase's ``ingest``
            CLI always pre-allocates the run at discovery time before
            ``run_ingest`` is ever called, so an unexplained refusal means
            that invariant did not hold.
    """
    log = get_logger()
    status = ctx.metadata.get_ingestion_run_status(run_id=ctx.run.run_id)
    if status == "SUCCEEDED":
        receipt_status = "SKIPPED_DUPLICATE"
    elif status == "RUNNING":
        receipt_status = "SKIPPED_CONCURRENT"
    else:
        msg = "claim_ingestion_run refused the claim but no SUCCEEDED/RUNNING row explains why"
        raise DataPlatformError(
            msg,
            context={
                "idempotency_key": ctx.run.idempotency_key,
                "run_id": ctx.run.run_id,
                "status": status,
            },
        )
    log.info(
        "run_ingest.skipped",
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
        report_uri=None,
    )


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
    tick could otherwise regress a just-committed ``SUCCEEDED`` status back
    to ``RUNNING``. ``stop_event.set()`` (in ``run_ingest``'s ``finally``
    block) only fires AFTER the publish transaction has already committed
    and the trailing staging-table drop has run -- a tick landing in that
    narrow window still calls this loop's body one more time. Calling
    ``ctx.metadata.heartbeat_ingestion_run`` (not the generic, unconditional
    ``update_ingestion_run_status``) makes that stray call a silent no-op
    instead of a status regression: its ``WHERE status = 'RUNNING'`` guard
    means a run that has already reached a terminal status is left
    untouched.

    Args:
        ctx: The current pipeline context. Only ``ctx.metadata`` is used.
        run_id: The claimed run this heartbeat keeps alive.
        progress: The shared, mutable holder ``on_progress`` writes and this
            loop reads -- never mutated here, only read.
        stop_event: Set by ``run_ingest``'s ``finally`` block to stop this
            loop.
        interval_seconds: Seconds between heartbeats. Production callers
            never override ``run_ingest``'s default; this module's own
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


def run_ingest(
    ctx: PipelineContext,
    *,
    heartbeat_interval_seconds: float = _DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
) -> Receipt:
    """Claim, stage, publish (one atomic transaction), and report on one ingestion run.

    Args:
        ctx: The current pipeline context -- ``ctx.run.idempotency_key``
            names the run to claim; ``ctx.run.file_id``/``ctx.run.batch_id``
            (populated by the ``ingest`` CLI from the frozen
            ``AssignmentDocument`` before this function is ever called) are
            the file/batch this run's publication marks
            ``PROCESSED``/``PUBLISHED``; ``ctx.source`` is the already
            source-specific reader this function streams through
            ``StagingLoader`` without ever inspecting.
        heartbeat_interval_seconds: Seconds between lease/row-count
            heartbeats, well under the 5-minute lease duration. Defaults to
            60s. This module's own tests override it directly; the real
            `ingest` CLI (`csv_processor.cli.ingest`) reads it from
            `DATAPLAT_HEARTBEAT_INTERVAL_SECONDS` (unset -- i.e. every
            production KPO pod except the one `csv_ingest_customers.py`'s
            `ingest` task sets it on -- falls back to this same 60s
            default), so a live E2E run can shrink it without changing
            production behavior anywhere else.

    Returns:
        A ``Receipt``: ``status="SUCCEEDED"`` after a genuine claim, stage
        and publish; ``status="SKIPPED_DUPLICATE"`` or
        ``status="SKIPPED_CONCURRENT"`` immediately, with no staging
        attempted, when the claim was refused because the run already
        succeeded or is concurrently in progress elsewhere.

    Raises:
        DataPlatformError: The claim was refused for a reason
            ``_skipped_receipt`` cannot explain (see its own docstring), or
            ``ctx.run.file_id``/``ctx.run.batch_id`` is unset. Any other
            run-fatal exception raised while staging or publishing
            propagates unmodified -- this function adds no second
            catch-once boundary; see the module docstring.
    """
    log = get_logger()
    start = time.monotonic()

    with tracing.start_span("pipeline.run_ingest"):
        # This run's own span -- a genuine CHILD of whatever parent context
        # `dataplat.cli`'s TRACEPARENT extraction attached (OBS-10), or a
        # fresh root span when no parent was ever extracted. Read BEFORE the
        # claim below, so the SAME trace_id/span_id land on the claimed row.
        span_context = otel_trace.get_current_span().get_span_context()
        trace_id = (
            otel_trace.format_trace_id(span_context.trace_id)
            if span_context.is_valid
            else None
        )
        span_id = (
            otel_trace.format_span_id(span_context.span_id) if span_context.is_valid else None
        )

        claimed = ctx.metadata.claim_ingestion_run(
            idempotency_key=ctx.run.idempotency_key,
            try_number=ctx.run.attempt,
            pod_name=os.environ.get("HOSTNAME", "unknown"),
            trace_id=trace_id,
            span_id=span_id,
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
            stage="run_ingest",
            status="running",
        )
        # Set to "succeeded" only as the LAST statement before this
        # function's normal return, below -- any exception raised anywhere
        # in between (file_id/batch_id validation, staging, publish, the
        # trailing DROP TABLE, or the rows_deduplicated/log.info
        # computation) leaves this at "failed", observed by the `finally`
        # below. This is a SECOND, OUTER try/finally -- distinct from and
        # surrounding the inner staging/publish try/finally below; the two
        # are never conflated.
        run_status = "failed"
        try:
            if ctx.run.file_id is None or ctx.run.batch_id is None:
                msg = "run_ingest requires ctx.run.file_id and ctx.run.batch_id to be set"
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
                name=f"run-ingest-heartbeat-{run_id}",
                daemon=True,
            )
            heartbeat_thread.start()

            try:
                # Staging runs OUTSIDE the publish transaction, on its own
                # connection (ARCHITECTURE.md's checkpointing-vs-transactions
                # split): checkpointed per chunk, never part of the atomic
                # barrier below. `pool.connection()`'s own context-manager
                # behavior commits on clean exit, making the staged table
                # visible to the separate connection the publish transaction
                # opens next.
                with ctx.db.connection() as staging_conn:
                    staging_result = StagingLoader(
                        target_columns=_CUSTOMERS_TARGET_COLUMNS,
                    ).load(
                        ctx,
                        staging_conn,
                        on_progress=_on_progress,
                    )

                finished_at = datetime.now(tz=UTC)
                # OBS-10's "-> PostgreSQL" segment: opens strictly inside the
                # outer `pipeline.run_ingest` span above, so it becomes a
                # genuine child span automatically -- no extra
                # parent-context plumbing needed. Covers only the atomic
                # publish transaction itself -- never the staging load above
                # or the trailing DROP TABLE below, each on its own,
                # separate connection.
                with (
                    tracing.start_span("pipeline.publish"),
                    ctx.db.connection() as conn,
                    conn.transaction(),
                ):
                    # Single-writer publication per dataset (LOAD-09): every
                    # writer to this target serializes on the SAME
                    # advisory-lock key before touching it.
                    conn.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                        (f"publish:{ctx.config.load.target}",),
                    )
                    publisher = resolve_publisher(ctx.config.load.strategy)
                    result = publisher.publish(ctx, staging_result.staging_table, conn)
                    # Measured HERE, immediately after the publish that does
                    # the actual work, not after the trailing DROP TABLE
                    # below: this is the number `finalize_publication`
                    # persists inside the SAME transaction as that publish,
                    # so it must exist before that call and before the
                    # transaction commits. Reused as-is for the Receipt/log
                    # after the `with` block exits -- one canonical
                    # duration, never two slightly different numbers for one
                    # run.
                    duration_ms = int((time.monotonic() - start) * 1000)
                    # META-03: lands inside the SAME transaction as the
                    # Publisher's own write -- the `with` block's exit
                    # commits both together, or rolls back both together on
                    # any exception.
                    ctx.metadata.finalize_publication(
                        conn=conn,
                        run_id=run_id,
                        file_id=file_id,
                        batch_id=batch_id,
                        rows_loaded=result.rows_affected,
                        finished_at=finished_at,
                        duration_ms=duration_ms,
                        report_uri=None,
                        schema_version_id=staging_result.schema_version_id,
                    )

                # Pitfall 2: an explicit DROP after the publish transaction
                # has committed, on a fresh connection -- never ON COMMIT
                # DROP (invalid for an UNLOGGED table; only TEMPORARY tables
                # support it).
                with ctx.db.connection() as drop_conn:
                    drop_conn.execute(f"DROP TABLE IF EXISTS {staging_result.staging_table}")
            finally:
                stop_heartbeat.set()
                heartbeat_thread.join(timeout=heartbeat_interval_seconds + 5)

            # duration_ms was already computed above, right after publish,
            # and reused here as-is (see the comment at its assignment) --
            # never recomputed against `time.monotonic()` again, which would
            # silently fold in the trailing DROP TABLE's time and produce a
            # second, slightly larger number for the same run.
            # This phase does not separately track "collapsed by DISTINCT ON
            # / duplicate customer_id within one batch" from "suppressed as
            # a no-op write by the WHERE guard" -- both reduce rows_parsed to
            # a smaller rows_affected, and a finer split is Phase 9's
            # meta.dedup_decisions territory (merge.py's own module
            # docstring), not this phase's. Clamped at 0 because a later,
            # larger customer_id set touching already-published rows (an
            # UPDATE, not an INSERT) can make rows_affected exceed this
            # run's own rows_parsed with no dedup having happened at all.
            rows_deduplicated = max(staging_result.rows_parsed - result.rows_affected, 0)
            log.info(
                "run_ingest.succeeded",
                run_id=run_id,
                rows_read=staging_result.rows_read,
                rows_loaded=result.rows_affected,
                rows_invalid=staging_result.rows_rejected,
                rows_deduplicated=rows_deduplicated,
                duration_ms=duration_ms,
            )
            run_status = "succeeded"
            return Receipt(
                run_id=run_id,
                status="SUCCEEDED",
                rows_read=staging_result.rows_read,
                rows_loaded=result.rows_affected,
                rows_invalid=staging_result.rows_rejected,
                rows_deduplicated=rows_deduplicated,
                duration_ms=duration_ms,
                report_uri=None,
            )
        finally:
            # Never an `except` -- a run-fatal exception is a pure
            # side-effect observation on the way through, never swallowed or
            # converted (module docstring's "catches nothing" contract).
            metrics.increment(
                "runs_finished",
                1,
                dataset=ctx.config.dataset,
                stage="run_ingest",
                status=run_status,
            )
