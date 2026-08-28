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

Neither function catches anything -- with ONE deliberate, narrowly-scoped
carve-out (debug session ci-pipeline-ingestion-timeout ROUND 14, finding 18a,
user-approved production-semantics change): ``publish_ingest`` catches
``QualityThresholdExceeded`` -- and ONLY that class -- because the platform's
own error hierarchy already declares it "a deliberate business-rule rollback,
not an infrastructure failure" (``errors.PublicationError``'s docstring). A
breaker trip is DETERMINISTIC: a pure function of (this pass's staged bronze
keys, gold's current keys, the configured threshold) -- an Airflow-level
retry re-runs an identical computation, observed live burning 7 tries x 42
minutes per poisoned pass while holding the dag's ``max_active_runs=1`` slot.
On a trip, ``publish_ingest`` quarantines the pass (every claimed run ->
``status='QUARANTINED'``, a terminal status discovery never re-offers) and
returns a ``{"status": "QUARANTINED", ...}`` payload -- the section-51
quarantine disposition: bad data is refused, diverted and RECORDED (loudness
lives in ``meta``, the platform's system of record), and the pipeline
continues. Every OTHER run-fatal exception (a genuinely broken staging table
create, a claim upsert failing for a reason other than "already claimed", a
``PublicationError``/connection loss mid-transaction) propagates OUT,
uncaught, to whichever CLI command called it -- the transient-infrastructure
class keeps its full retry budget, and the "always write a receipt, even on
a run-fatal failure" contract belongs to that call site, not to this module.
On every exit path, success or failure, ``stage_ingest``/``publish_ingest``
each guarantee two things: their own heartbeat/claim-adjacent state is left
consistent, and -- once real work has genuinely begun -- a ``runs_finished``
counter increment is observed (D-03's live "runs currently in-flight"/"recent
failure rate" gauges). The latter is emitted from a ``finally`` block, so a
run-fatal exception is a pure side-effect observation on the way through --
never swallowed, never converted (the quarantine carve-out above CONVERTS
deliberately, by design, for exactly one exception class).

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

from dataplat.errors import ConfigurationError, DataPlatformError, QualityThresholdExceeded
from dataplat.load.publish.registry import resolve_publisher
from dataplat.load.staging import StagingLoader
from dataplat.models.receipt import Receipt
from dataplat.observability import metrics, tracing
from dataplat.observability.logging import get_logger
from dataplat.validate.circuit_breaker import RejectionRateCircuitBreaker
from dataplat.validate.referential import ReferentialIntegrityBarrier
from dataplat.validate.volume_anomaly import VolumeAnomalyBarrier

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from decimal import Decimal

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

# (20a) LEG 2 (debug/ci-pipeline-ingestion-timeout ROUND 15): how long a
# claim-refused `stage_ingest` call waits for a live foreign lease to resolve
# before FAILING (never silently "succeeding"). Sized to one full lease
# duration plus margin: a dead claimant's lease expires within 5 minutes (the
# reclaim then happens inside this same call), while a live claimant keeps
# heartbeating its lease forward and normally reaches STAGED well within this
# budget. On expiry the raise fails the Airflow task, whose own retry/backoff
# machinery re-enters this wait later -- honest at every layer. Overridable
# per-call for tests, and via DATAPLAT_STAGE_CONCURRENT_WAIT_SECONDS in the
# `stage` CLI (the DATAPLAT_HEARTBEAT_INTERVAL_SECONDS precedent).
_DEFAULT_CONCURRENT_WAIT_SECONDS = 420.0
_CONCURRENT_CLAIM_POLL_SECONDS = 2.0

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
    # signup_country (D-13, plan 10-01) appended: customers.yaml's own
    # `columns:` block gained this Type-0 column in migration 0035, and
    # staging.customers (durable bronze) gained the matching nullable
    # column in migration 0037 (plan 10-04) -- but this lookup itself was
    # never updated, a genuine pre-existing gap plan 10-04's own SUMMARY
    # flagged as "will block plan 10-07's live 2-year backfill sweep until
    # fixed" (Rule 3: every real stage_ingest() call for customers would
    # otherwise raise ValueError at StagingLoader._build_stages, since
    # ColumnContract("signup_country") has no entry in this tuple). Fixed
    # here, appended last to match the CSV/staging-table column order
    # tools/corpus/dated_series.py's _DATASET_COLUMNS and customers.yaml's
    # own columns: block both already use.
    "customers": ("customer_id", "name", "country", "birth_date", "event_ts", "signup_country"),
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


# D-02's exact `order_by` columns (each dataset's own `DeduplicationConfig.
# order_by`, e.g. `"event_ts desc"`/`"order_date desc"`): the column
# `record_watermark`'s own `SELECT max(...)` advances the observational
# watermark by. A dataset-keyed lookup, mirroring `_TARGET_COLUMNS_BY_DATASET`
# above and `resolve_publisher`'s own "small module-level dict keyed by a
# config value" convention (`load/publish/registry.py`) -- `DatasetConfig`
# carries no canonical "the one column that orders this dataset" field yet, so
# this stays a small, explicit table rather than a generic derivation.
_WATERMARK_COLUMN_BY_DATASET: dict[str, str] = {
    "customers": "event_ts",
    "orders": "order_date",
}


def _watermark_column_for_dataset(dataset: str) -> str:
    """Resolve ``dataset`` through ``_WATERMARK_COLUMN_BY_DATASET``, or fail loudly.

    Mirrors ``_target_columns_for_dataset``'s own split-out-for-PLR0915
    reasoning and ``resolve_publisher``'s own ``KeyError``-to-
    ``ConfigurationError`` translation (this is a config-shape problem, not a
    run-fatal data problem, so it raises the same error class
    ``resolve_publisher`` does, not ``DataPlatformError``).

    Args:
        dataset: ``ctx.config.dataset``, e.g. ``"customers"``/``"orders"``.

    Returns:
        That dataset's watermark column, e.g. ``"event_ts"``/``"order_date"``.

    Raises:
        ConfigurationError: ``dataset`` has no entry.
    """
    try:
        return _WATERMARK_COLUMN_BY_DATASET[dataset]
    except KeyError:
        msg = f"publish_ingest has no _WATERMARK_COLUMN_BY_DATASET entry for dataset {dataset!r}"
        raise ConfigurationError(
            msg,
            context={"dataset": dataset, "known_datasets": sorted(_WATERMARK_COLUMN_BY_DATASET)},
        ) from None


@dataclasses.dataclass(slots=True, frozen=True)
class _ReconciliationAggregates:
    """This pass's silver->gold reconciliation figures (D-20/D-21/D-22).

    Computed ONCE, against the WHOLE cumulative ``source_table`` (silver)
    and ``ctx.config.load.target`` (gold) tables -- never a per-run slice:
    reconciliation's question is "do silver and gold agree IN TOTAL", which
    stays a whole-table comparison even though ``publisher.publish()``
    itself is delta-scoped since debug/ci-pipeline-ingestion-timeout ROUND
    17 (finding 25) -- a read-only ``count(*)``/``count(DISTINCT ...)``
    pass takes no row locks and does not carry the O(accumulated) upsert
    cost the publish statement used to. Attributed identically to every
    file finalized this pass, mirroring ``rows_loaded``'s own
    aggregate-attribution precedent (see ``publish_ingest``'s own
    ``finalize_publication`` call site).

    D-08 (Phase 10, SCD-03): ``output_count`` keeps its literal, unchanged
    meaning -- ``count(*) FROM target_table``, every physical row -- and is
    NEVER redefined here. For a Type-2 SCD dimension (``normalized.
    customers``) this legitimately grows past ``key_count_output`` (already
    ``count(DISTINCT business_key_column) FROM target_table``, unchanged)
    once more than one SCD2 version exists for the same business key: a
    clean publish with zero rejects/dedups against a multi-versioned
    customers table is NOT expected to show zero discrepancy the way it
    does for a Type-1/Type-0-only dataset like ``orders``. Any comparison
    whose intent is "does the target hold the same SET of business keys as
    the source" must use ``key_count_input``/``key_count_output``, never
    ``input_count``/``output_count``.
    """

    input_count: int
    output_count: int
    sum_column: str | None
    sum_input: Decimal | None
    sum_output: Decimal | None
    checksum_input: str | None
    checksum_output: str | None
    min_input: datetime | None
    max_input: datetime | None
    min_output: datetime | None
    max_output: datetime | None
    key_count_input: int | None
    key_count_output: int | None


def _scalar(conn: Connection[Any], query: str) -> Any:
    """Run one aggregate ``SELECT`` (no ``GROUP BY``) and return its single column-0 value.

    Every call site in this module passes a literal aggregate expression
    (``count``/``sum``/``min``/``max``) with no ``GROUP BY`` -- PostgreSQL
    always returns exactly one row for that shape, even against an empty
    table (``count`` = 0, ``sum``/``min``/``max`` = ``NULL``), so the
    defensive ``None`` branch below documents that invariant rather than
    silently swallowing a genuinely unexpected shape.

    Args:
        conn: An already-open connection, inside the caller's own
            transaction.
        query: A complete, caller-built SQL statement. May embed
            config-resolved identifiers (T-09-03) -- never row content or
            user input; every genuine value in a query built by this
            module's own callers still crosses via `%s`/`%()s` placeholders
            where one is needed.

    Returns:
        The single row's column-0 value.
    """
    row = conn.execute(query).fetchone()
    # An aggregate SELECT with no GROUP BY always returns exactly one row,
    # even against an empty table (count=0, sum/min/max=NULL) -- this
    # documents that invariant rather than silently swallowing a genuinely
    # unexpected shape.
    if row is None:  # pragma: no cover
        msg = f"aggregate query returned no row: {query!r}"
        raise RuntimeError(msg)
    return row[0]


def _table_checksum(
    conn: Connection[Any],
    table: str,
    *,
    columns: Sequence[str] | None = None,
) -> str | None:
    """Compute an order-independent aggregate hash over rows of ``table`` (D-21, D-29).

    ``bit_xor`` is commutative -- the result does not depend on row order,
    so two tables holding the SAME rows in a DIFFERENT physical order
    produce the SAME checksum. `table` is a config-resolved identifier
    (T-09-03), interpolated as an identifier only.

    Args:
        conn: An already-open connection, inside the caller's own open
            transaction.
        table: A config-resolved table identifier (T-09-03), e.g.
            ``"silver.customers"``. Interpolated as an identifier only,
            never row content.
        columns: ``None`` (the default) hashes every column of ``table`` --
            byte-for-byte identical to this function's behavior before D-29,
            and its original caller (``_compute_silver_gold_reconciliation``,
            below) always passes this default, so its behavior is unchanged.
            When provided, hashes ONLY the named columns instead of ``SELECT
            *`` -- ``dataplat.pipeline.rebuild_reconciliation``'s
            ``snapshot_table_state`` (D-29 point 2, Pitfall 7) passes a
            dataset's BUSINESS columns here, explicitly excluding the six
            embedded lineage columns a rebuild-from-raw deliberately
            re-mints with fresh identity/timestamp values: ``_run_id``,
            ``_file_id``, ``_batch_id``, ``_source_row_number``,
            ``_ingested_at`` (staging/bronze tables) and ``_dbt_loaded_at``
            (silver tables only) -- see ``.planning/research/
            ARCHITECTURE.md`` §2.3. ``_record_hash``/``_record_hash_version``
            are deliberately NOT excluded by this module -- they are
            deterministic functions of business data and SHOULD match
            across a rebuild (a useful extra determinism check, not noise);
            a caller wanting that check simply includes them in ``columns``.
            Every name in ``columns`` is a config-resolved identifier
            (T-09-03), interpolated as an identifier only, never row
            content.

    Returns:
        The hex-encoded aggregate hash, or ``None`` for an empty table (or
        an empty ``columns`` selection).
    """
    # `table`/`columns` are config-resolved identifiers (T-09-03), never row content.
    source = f"{table} t" if columns is None else f"(SELECT {', '.join(columns)} FROM {table}) t"  # noqa: S608
    query = (
        f"SELECT to_hex(bit_xor(('x' || substr(md5(t::text), 1, 16))::bit(64)::bigint)) "  # noqa: S608
        f"FROM {source}"
    )
    result = _scalar(conn, query)
    return None if result is None else str(result)


def _compute_silver_gold_reconciliation(
    ctx: PipelineContext,
    conn: Connection[Any],
    *,
    source_table: str,
    watermark_column: str,
) -> _ReconciliationAggregates:
    """Compute this pass's silver->gold reconciliation figures (D-20/D-21/D-22).

    Reads the ENTIRE cumulative ``source_table`` (silver) and
    ``ctx.config.load.target`` (gold) tables, an apples-to-apples full-table
    row-count comparison (D-20's "source-to-target" fidelity) -- deliberately
    NOT ``result.rows_affected`` (the ``MERGE``'s own affected-row count,
    which ``finalize_publication``'s pre-existing ``rows_loaded`` reporting
    already uses unchanged, a DIFFERENT metric with a different meaning:
    reconciliation counts total rows, publish reporting counts rows touched
    by this pass).

    ``source_table``/``ctx.config.load.target``/``watermark_column``/the
    resolved sum/business-key column names are all config-resolved
    identifiers (T-09-03), interpolated as identifiers only, never row
    content.

    Args:
        ctx: The current pipeline context. ``ctx.config.load.target``,
            ``ctx.config.columns`` and ``ctx.config.reconciliation`` are
            read.
        conn: An already-open connection, inside the caller's own open
            transaction.
        source_table: The silver table this pass published from, e.g.
            ``"silver.customers"``.
        watermark_column: This dataset's watermark column (D-02), reused
            here for ``min``/``max`` -- the SAME column
            ``_watermark_column_for_dataset`` resolved for the watermark
            advance above.

    Returns:
        This pass's reconciliation figures.
    """
    target_table = ctx.config.load.target
    business_key_column = next((c for c in ctx.config.columns if c.business_key), None)
    sum_column = ctx.config.reconciliation.sum_columns[0] if ctx.config.reconciliation else None

    input_count = int(_scalar(conn, f"SELECT count(*) FROM {source_table}"))  # noqa: S608
    output_count = int(_scalar(conn, f"SELECT count(*) FROM {target_table}"))  # noqa: S608

    sum_input: Decimal | None = None
    sum_output: Decimal | None = None
    if sum_column is not None:
        sum_input = _scalar(conn, f"SELECT sum({sum_column}::numeric) FROM {source_table}")  # noqa: S608
        sum_output = _scalar(conn, f"SELECT sum({sum_column}::numeric) FROM {target_table}")  # noqa: S608

    # Both sides cast to `::timestamptz`: silver's own watermark column is
    # always TEXT (unparsed CSV content, D-02), while gold's is already
    # typed (timestamptz for `event_ts`, date for `order_date`) -- casting
    # both sides identically keeps this one query shape correct for either.
    min_input = _scalar(
        conn,
        f"SELECT min({watermark_column}::timestamptz) FROM {source_table}",  # noqa: S608
    )
    max_input = _scalar(
        conn,
        f"SELECT max({watermark_column}::timestamptz) FROM {source_table}",  # noqa: S608
    )
    min_output = _scalar(
        conn,
        f"SELECT min({watermark_column}::timestamptz) FROM {target_table}",  # noqa: S608
    )
    max_output = _scalar(
        conn,
        f"SELECT max({watermark_column}::timestamptz) FROM {target_table}",  # noqa: S608
    )

    key_count_input: int | None = None
    key_count_output: int | None = None
    if business_key_column is not None:
        key_count_input = int(
            _scalar(
                conn,
                f"SELECT count(DISTINCT {business_key_column.name}) FROM {source_table}",  # noqa: S608
            ),
        )
        key_count_output = int(
            _scalar(
                conn,
                f"SELECT count(DISTINCT {business_key_column.name}) FROM {target_table}",  # noqa: S608
            ),
        )

    return _ReconciliationAggregates(
        input_count=input_count,
        output_count=output_count,
        sum_column=sum_column,
        sum_input=sum_input,
        sum_output=sum_output,
        checksum_input=_table_checksum(conn, source_table),
        checksum_output=_table_checksum(conn, target_table),
        min_input=min_input,
        max_input=max_input,
        min_output=min_output,
        max_output=max_output,
        key_count_input=key_count_input,
        key_count_output=key_count_output,
    )


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
            "claim_ingestion_run refused the claim but no SUCCEEDED/STAGED/RUNNING row explains why"
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


def _await_concurrent_claim(
    ctx: PipelineContext,
    *,
    attempt_claim: Callable[[], tuple[int, str] | None],
    wait_seconds: float,
) -> tuple[int, str] | Receipt:
    """Wait out a refused claim honestly: verified-complete, reclaimed, or ERROR.

    (20a) LEG 2 (debug/ci-pipeline-ingestion-timeout ROUND 15): a claim
    refused because another pod holds a live lease used to return
    ``SKIPPED_CONCURRENT`` immediately -- task SUCCESS with nothing staged
    and nothing verified. When the lease-holder had actually CRASHED
    (SIGKILL/OOM -- the exception class is released by ``stage_ingest``'s
    own ``finally``), that converted a crash into a silent drop: the run
    wedged ``RUNNING`` forever once the source file was cleaned up.

    This loop replaces that shortcut with the only three honest outcomes:

    1. The run reaches ``STAGED``/``SUCCEEDED`` -- the concurrent
       claimant's work verifiably exists; return the skipped ``Receipt``
       (``SKIPPED_DUPLICATE`` via ``_skipped_receipt``'s own mapping).
    2. The claim starts succeeding (lease expired, or the crashed claimant
       was released to ``FAILED``) -- return the claimed ``(run_id,
       status)`` so the caller stages genuinely.
    3. ``wait_seconds`` elapses with neither -- raise: the Airflow task
       FAILS and its own retry/backoff machinery re-enters this wait later.
       A live, heartbeating-but-stuck claimant therefore costs this task
       its wait budget and a loud failure -- honest, never a silent pass.

    Args:
        ctx: The current pipeline context (``ctx.run``/``ctx.metadata``).
        attempt_claim: Re-invokes ``claim_ingestion_run`` with the caller's
            exact claim identity (a closure, so this helper never re-builds
            the claim's trace/dag-context arguments).
        wait_seconds: Total wait budget before outcome 3.

    Returns:
        Outcome 1's ``Receipt`` or outcome 2's claimed ``(run_id, status)``.

    Raises:
        DataPlatformError: Outcome 3 -- the run is still held under a live
            foreign lease after ``wait_seconds``; reporting success would be
            a silent drop.
    """
    log = get_logger()
    deadline = time.monotonic() + wait_seconds
    log.info(
        "stage_ingest.claim_refused_waiting",
        run_id=ctx.run.run_id,
        idempotency_key=ctx.run.idempotency_key,
        wait_seconds=wait_seconds,
    )
    while True:
        status = ctx.metadata.get_ingestion_run_status(run_id=ctx.run.run_id)
        if status in ("SUCCEEDED", "STAGED"):
            return _skipped_receipt(ctx)
        claimed = attempt_claim()
        if claimed is not None:
            log.info(
                "stage_ingest.claim_recovered_after_wait",
                run_id=ctx.run.run_id,
                previous_status=status,
            )
            return claimed
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            msg = (
                "run is still RUNNING under a live lease held elsewhere after the "
                "full wait budget -- refusing to report success without staged data"
            )
            raise DataPlatformError(
                msg,
                context={
                    "run_id": ctx.run.run_id,
                    "idempotency_key": ctx.run.idempotency_key,
                    "wait_seconds": wait_seconds,
                    "last_observed_status": status,
                },
            )
        time.sleep(min(_CONCURRENT_CLAIM_POLL_SECONDS, remaining))


def _release_failed_claim(ctx: PipelineContext, *, run_id: int, pod_name: str) -> None:
    """Best-effort release of this pod's own crashed claim ((20a) LEG 1).

    Called from ``stage_ingest``'s ``finally`` when the claimed body did NOT
    reach STAGED: this pod committed ``RUNNING`` + a 5-minute lease at claim
    time, and leaving that in place made the Airflow retry's refused claim
    convert into a silent ``SKIPPED_CONCURRENT`` "success" with nothing
    staged. Releasing to ``FAILED`` lets the retry claim genuinely.

    Best-effort by necessity: raising inside a ``finally`` would REPLACE the
    true in-flight exception, and if the crash's own cause is an unreachable
    database this write fails too -- the 5-minute lease expiry remains the
    backstop, exactly as for a SIGKILL'd pod that never runs this line at
    all. Guarded server-side (``status='RUNNING' AND k8s_pod_name=this
    pod``, ``fail_ingestion_run_claim``), so it can never stomp another
    claimant or regress a terminal status.

    Args:
        ctx: The current pipeline context. Only ``ctx.metadata`` is used.
        run_id: The run this pod claimed.
        pod_name: The exact ``pod_name`` this pod claimed with.
    """
    log = get_logger()
    try:
        released = ctx.metadata.fail_ingestion_run_claim(run_id=run_id, pod_name=pod_name)
        log.info(
            "stage_ingest.claim_released_on_failure",
            run_id=run_id,
            released=released,
        )
    except Exception:  # noqa: BLE001 -- must never mask the in-flight exception (docstring above)
        log.warning(
            "stage_ingest.claim_release_failed_lease_expiry_is_backstop",
            run_id=run_id,
        )


def _claim_or_await(
    ctx: PipelineContext,
    *,
    trace_id: str | None,
    span_id: str | None,
    concurrent_wait_seconds: float,
) -> tuple[int, str] | Receipt:
    """Attempt the claim; on refusal, wait it out honestly ((20a) LEG 2).

    Split out of ``stage_ingest`` purely to keep that function's statement
    count under ``PLR0915``'s threshold (the same reasoning
    ``_find_quality_rule`` records for itself) -- no behavior lives here
    that ``stage_ingest``'s docstring does not already describe.

    Args:
        ctx: The current pipeline context.
        trace_id: The claim's trace id, or ``None`` outside a valid span.
        span_id: The claim's span id, or ``None`` outside a valid span.
        concurrent_wait_seconds: ``_await_concurrent_claim``'s wait budget.

    Returns:
        On a successful (possibly awaited) claim: ``(run_id, pod_name)`` --
        ``pod_name`` is the exact value the claim row's ``k8s_pod_name`` now
        holds, which ``stage_ingest``'s ``finally`` later passes to
        ``fail_ingestion_run_claim`` so the crash-release guard always
        matches this claim's own row. Otherwise the skipped ``Receipt``
        (the run's work verifiably already exists).

    Raises:
        DataPlatformError: Propagated from ``_await_concurrent_claim``'s
            timeout arm, or from ``_skipped_receipt``'s unexplained-refusal
            arm.
    """
    pod_name = os.environ.get("HOSTNAME", "unknown")

    def _attempt_claim() -> tuple[int, str] | None:
        return ctx.metadata.claim_ingestion_run(
            idempotency_key=ctx.run.idempotency_key,
            try_number=ctx.run.attempt,
            pod_name=pod_name,
            trace_id=trace_id,
            span_id=span_id,
            dag_id=ctx.run.dag_id,
            dag_run_id=ctx.run.dag_run_id,
            task_id=ctx.run.task_id,
            map_index=ctx.run.map_index,
            k8s_namespace=ctx.run.k8s_namespace,
        )

    claimed = _attempt_claim()
    if claimed is None:
        # (20a) LEG 2: never an immediate SKIPPED_CONCURRENT "success" --
        # wait for the foreign claim to resolve, reclaim, or raise.
        awaited = _await_concurrent_claim(
            ctx,
            attempt_claim=_attempt_claim,
            wait_seconds=concurrent_wait_seconds,
        )
        if isinstance(awaited, Receipt):
            return awaited
        claimed = awaited
    run_id, _ = claimed
    return run_id, pod_name


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
    barrier_result = barrier.apply(ctx, conn)

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
    concurrent_wait_seconds: float = _DEFAULT_CONCURRENT_WAIT_SECONDS,
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
        concurrent_wait_seconds: (20a) LEG 2 -- total budget for
            ``_await_concurrent_claim``'s wait-and-reclaim loop when the
            claim is refused under a live foreign lease. Defaults to one
            lease duration plus margin (420s); the ``stage`` CLI overrides
            it via ``DATAPLAT_STAGE_CONCURRENT_WAIT_SECONDS``; tests shrink
            it directly.

    Returns:
        A ``Receipt``: ``status="STAGED"`` after a genuine claim, stage and
        durable-bronze promotion (``rows_loaded=0`` -- staging never writes
        to gold); ``status="SKIPPED_DUPLICATE"``, with no staging attempted,
        when the run already VERIFIABLY reached ``STAGED``/``SUCCEEDED`` --
        including via a concurrent claimant this call observed finishing
        during its wait loop. ``status="SKIPPED_CONCURRENT"`` is no longer a
        possible return: a claim refused under a live foreign lease now
        WAITS (``_await_concurrent_claim``) and either reclaims-and-stages,
        returns the verified duplicate, or RAISES -- never task success with
        nothing staged (debug/ci-pipeline-ingestion-timeout ROUND 15,
        finding 20a).

    Raises:
        DataPlatformError: The claim was refused for a reason
            ``_skipped_receipt`` cannot explain (see its own docstring); the
            claim stayed refused under a live foreign lease for the whole
            ``concurrent_wait_seconds`` budget (see
            ``_await_concurrent_claim`` -- the Airflow retry re-enters the
            wait later); or ``ctx.run.file_id``/``ctx.run.batch_id`` is
            unset. Any other run-fatal exception raised while staging
            propagates unmodified -- this function adds no second catch-once
            boundary (see the module docstring), though its ``finally`` now
            best-effort releases this pod's own claim to ``FAILED``
            (``fail_ingestion_run_claim``) on the way through, so an Airflow
            retry can genuinely re-stage instead of being refused by the
            crashed attempt's still-live lease ((20a) LEG 1).
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

        claim_outcome = _claim_or_await(
            ctx,
            trace_id=trace_id,
            span_id=span_id,
            concurrent_wait_seconds=concurrent_wait_seconds,
        )
        if isinstance(claim_outcome, Receipt):
            return claim_outcome
        run_id, pod_name = claim_outcome

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
            if run_status == "failed":
                _release_failed_claim(ctx, run_id=run_id, pod_name=pod_name)
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
    ``list_staged_run_ids`` machinery): dbt's own batching (D-05) may
    consolidate several ``stage_ingest`` runs' bronze contributions into
    one deduplicated silver pass, so this function claims and finalizes
    every currently-``STAGED`` run in one atomic transaction (META-03,
    unchanged from ``run_ingest``'s own guarantee). The ``Publisher`` is
    handed this pass's exact ``staged_run_ids`` and scopes its read of
    ``silver.<dataset>`` to them (debug/ci-pipeline-ingestion-timeout
    ROUND 17, finding 25 -- exact since ROUND 16's ``meta.
    dbt_processed_runs`` claim ledger made dbt eligibility precise; the
    pre-ledger design read silver unconditionally as compensation for
    inexact eligibility). The upsert stays the SAME idempotent ``INSERT
    ... ON CONFLICT`` shape ``merge.py`` already proves safe to re-run.

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
        runs per invocation. Keys: ``"status"`` (``"SUCCEEDED"`` on a normal
        return; ``"QUARANTINED"`` when this pass tripped a deterministic
        quality gate -- see the module docstring's ROUND 14 carve-out; any
        OTHER run-fatal exception propagates uncaught, same "catches
        nothing" contract as ``stage_ingest``), ``"runs_finalized"`` (the
        list of ``run_id``s this call finalized, possibly empty),
        ``"rows_loaded"`` (this pass's total affected-row count -- see the
        aggregate-attribution note at this function's own
        ``finalize_publication`` call site), ``"duration_ms"``. The
        ``"QUARANTINED"`` shape additionally carries ``"runs_quarantined"``
        (every run of the tripped pass -- attribution is PASS-scoped by
        necessity: the vanished mass is the absence of keys from the pass's
        union, structurally unattributable to a single run) and
        ``"reason"`` (the breaker's own message, with its observed
        ratio/threshold context logged alongside).
    """
    log = get_logger()
    start = time.monotonic()

    dataset_id = ctx.metadata.get_or_create_dataset(ctx.config.dataset)
    # Taken BEFORE opening any connection or transaction (Test 1's own
    # requirement) -- a dataset with nothing currently STAGED costs this
    # function exactly one read, never an advisory lock or a publish
    # statement.
    staged = ctx.metadata.list_staged_run_ids(dataset_id=dataset_id)
    # Computed here, immediately after `staged` is first assigned (Phase 10,
    # 10-01-PLAN.md Task 3): this is the exact list of run_ids this publish
    # pass is finalizing, needed by `publisher.publish()` BEFORE any
    # Publisher whose DELETE-detection/recompute logic must be scoped to
    # "this pass's own files" runs (Finding F-2) -- reused unchanged at its
    # original call sites below (record_watermark, the finalize loop).
    staged_run_ids = [run_id for run_id, _, _, _ in staged]
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
            result = publisher.publish(
                ctx, source_table, conn, staged_run_ids=staged_run_ids
            )

            # D-01/D-02/D-04: advance this dataset's observational watermark
            # inside the SAME transaction as the merge upsert above -- never
            # a separate transaction, which could observe a partial publish
            # (AP4 avoidance, ARCHITECTURE.md line 1333). GREATEST() inside
            # `record_watermark`'s own SQL enforces INCR-02's "`>=`, never
            # `>`" rule structurally; `meta.watermark_history` is appended
            # unconditionally either way (D-04).
            watermark_column = _watermark_column_for_dataset(ctx.config.dataset)
            ctx.metadata.record_watermark(
                conn=conn,
                dataset_id=dataset_id,
                target_key="default",
                source_table=source_table,
                watermark_column=watermark_column,
                run_id=max(staged_run_ids),
                run_ids=staged_run_ids,
            )

            # D-20/D-21/D-22: this pass's silver->gold reconciliation
            # figures, computed ONCE against the whole cumulative
            # silver/gold tables (never a per-run slice -- "do silver and
            # gold agree in total" is inherently a whole-table question,
            # and this read-only count pass stays cheap even now that
            # `publisher.publish()` above is delta-scoped per ROUND 17
            # finding 25) and attributed identically to every file
            # finalized this pass below, mirroring `rows_loaded`'s own
            # aggregate-attribution precedent.
            reconciliation = _compute_silver_gold_reconciliation(
                ctx,
                conn,
                source_table=source_table,
                watermark_column=watermark_column,
            )

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
                # D-21/D-24: one silver->gold reconciliation row per
                # finalized file -- reusing the SAME pass-level aggregate
                # values computed above for every file in this pass (same
                # aggregate-attribution reasoning as `rows_loaded` above).
                ctx.metadata.record_reconciliation(
                    conn=conn,
                    dataset_id=dataset_id,
                    file_id=file_id,
                    hop="silver_gold",
                    input_count=reconciliation.input_count,
                    output_count=reconciliation.output_count,
                    sum_column=reconciliation.sum_column,
                    sum_input=reconciliation.sum_input,
                    sum_output=reconciliation.sum_output,
                    checksum_input=reconciliation.checksum_input,
                    checksum_output=reconciliation.checksum_output,
                    min_input=reconciliation.min_input,
                    max_input=reconciliation.max_input,
                    min_output=reconciliation.min_output,
                    max_output=reconciliation.max_output,
                    key_count_input=reconciliation.key_count_input,
                    key_count_output=reconciliation.key_count_output,
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

    except QualityThresholdExceeded as exc:
        # The ONE deliberate carve-out from this module's "catches nothing"
        # contract -- see the module docstring's ROUND 14 paragraph for the
        # full classification rationale (deterministic business-rule trip vs
        # transient infrastructure failure; the error hierarchy itself draws
        # this exact line in `errors.PublicationError`'s docstring). The
        # publish transaction has ALREADY rolled back by the time control
        # reaches here (the `conn.transaction()` context exited on the
        # raise; the breaker is a pre-mutation barrier, so gold is
        # untouched). Quarantine is TERMINAL and PASS-scoped: every run this
        # pass claimed goes to 'QUARANTINED' (never re-offered by discovery,
        # never claimable by stage, never re-listed by list_staged_run_ids)
        # -- leaving them 'STAGED' would re-inject the identical poisoned
        # union into EVERY later publish pass of this dataset (ROUND 11's
        # self-sustaining-poison shape), and retrying is re-running an
        # identical pure computation. Recovery is a recorded operator
        # action: re-open the run (status flip) after investigating, or land
        # a corrected file as a NEW raw object (section-63: corrections
        # arrive as new files, never overwrites).
        quarantine_duration_ms = int((time.monotonic() - start) * 1000)
        for run_id, _file_id, _batch_id, _report_uri in staged:
            ctx.metadata.update_ingestion_run_status(run_id=run_id, status="QUARANTINED")
        log.warning(
            "publish_ingest.quarantined",
            dataset=ctx.config.dataset,
            runs_quarantined=staged_run_ids,
            reason=str(exc),
            breaker_context=exc.context,
            duration_ms=quarantine_duration_ms,
        )
        run_status = "quarantined"
        return {
            "status": "QUARANTINED",
            "runs_finalized": [],
            "runs_quarantined": staged_run_ids,
            "rows_loaded": 0,
            "reason": str(exc),
            "duration_ms": quarantine_duration_ms,
        }
    else:
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
        # Never an `except` for the transient-infrastructure class -- same
        # "catches nothing" contract as `stage_ingest` (module docstring;
        # the QualityThresholdExceeded quarantine branch above is that
        # contract's one documented, deliberate carve-out).
        metrics.increment(
            "runs_finished",
            1,
            dataset=ctx.config.dataset,
            stage="publish_ingest",
            status=run_status,
        )
