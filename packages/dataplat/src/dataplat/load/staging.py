"""``StagingLoader`` — chunked ``COPY`` into a clean, retry-safe, all-TEXT staging table.

Streams whatever ``Source`` ``ctx.source`` resolves to through
``[RaggedRowGuard()]`` via ``dataplat.pipeline.engine.run_streaming``, and
``COPY``-ies every surviving chunk's rows into
``staging.<dataset>__r<run_id>`` -- one throwaway, ``UNLOGGED`` table per
ingestion-run attempt (LOAD-05).

Two corrections this module encodes, both verified against real PostgreSQL
this phase (04-RESEARCH.md):

- **Pitfall 2** -- ``CREATE UNLOGGED TABLE ... ON COMMIT DROP`` is not valid
  syntax (``ON COMMIT`` only applies to ``TEMPORARY`` tables) and would be
  semantically wrong even if it parsed: this table must survive across
  multiple chunked-``COPY`` calls and into the *separate* publish
  transaction that reads from it. Cleanup is an explicit ``DROP TABLE``,
  issued by the caller after publication commits -- never ``ON COMMIT``.
  Every attempt also begins with its own ``DROP TABLE IF EXISTS`` (C5: "an
  idempotent undo of its own prior partial work"), so a retry always starts
  from a clean table regardless of what a crashed prior attempt left behind.
- **Pitfall 9** -- "all-TEXT" applies only to the *business* columns
  (``target_columns``). The six lineage columns are populated entirely in
  Python, never parsed from unreliable source text, so they keep their real
  target types (``bigint``/``bytea``/``smallint``) -- weakening them would
  only add avoidable cast bugs at publish time.

``_record_hash`` is computed exactly once, here, in Python (Pitfall 10 /
PITFALLS C6: never recomputed in SQL) -- a canonical, pipe-joined encoding
over the row's business values in ``target_columns`` order.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dataplat.observability.logging import get_logger
from dataplat.pipeline.engine import RaggedRowGuard, run_streaming

if TYPE_CHECKING:
    from collections.abc import Callable

    from psycopg import Connection

    from dataplat.pipeline.protocol import PipelineContext

# The six embedded lineage columns every staged/normalized row carries,
# verbatim from `migrations/versions/0005_normalized_customers.py` --
# `(column_name, staging_sql_type)`. Kept as their real target types (never
# weakened to `text`) per Pitfall 9: they are populated entirely in Python,
# never parsed from unreliable source text.
_LINEAGE_COLUMN_TYPES: tuple[tuple[str, str], ...] = (
    ("_run_id", "bigint"),
    ("_file_id", "bigint"),
    ("_batch_id", "bigint"),
    ("_source_row_number", "bigint"),
    ("_record_hash", "bytea"),
    ("_record_hash_version", "smallint"),
)
_LINEAGE_COLUMN_NAMES: tuple[str, ...] = tuple(name for name, _ in _LINEAGE_COLUMN_TYPES)


@dataclass(frozen=True, slots=True)
class StagingResult:
    """The outcome of one ``StagingLoader.load()`` call.

    Attributes:
        staging_table: The fully-qualified staging table rows were COPY-ed
            into, e.g. ``"staging.customers__r8123"``.
        rows_read: Total rows encountered from the source across every
            chunk, before ``RaggedRowGuard`` filtering (``rows_parsed +
            rows_rejected``).
        rows_parsed: Rows that survived ``RaggedRowGuard`` and were actually
            COPY-ed into ``staging_table``.
        rows_rejected: Rows ``RaggedRowGuard`` rejected (field-count
            mismatch) and did not stage.
    """

    staging_table: str
    rows_read: int
    rows_parsed: int
    rows_rejected: int


class StagingLoader:
    """Streams a ``Source`` through ``RaggedRowGuard`` into a per-run staging table."""

    name = "staging"

    def __init__(self, *, target_columns: tuple[str, ...], chunk_size: int = 1000) -> None:
        """Configure which business columns this loader stages, and in what order.

        Args:
            target_columns: The ordered business-column tuple this dataset
                stages, e.g. ``("customer_id", "name", "country",
                "birth_date", "event_ts")`` for ``customers``. Passed in by
                the caller, never hardcoded here, since a future dataset's
                columns differ. Each surviving row's fields are assumed to
                already be in this exact order (this phase's naive
                ``CsvSource`` has no header-to-column name mapping --
                CONTEXT.md's "no header edge cases" scope note -- so
                positional correspondence is the only contract this phase
                offers; per-name reordering is Phase 6's header-detection
                territory).
            chunk_size: Mirrors ``csv_processor.source.CsvSource``'s own
                ``chunk_size`` knob and shares its default. Stored for
                forward compatibility / symmetry only: COPY/on_progress/
                log-line granularity in ``load()`` is owned entirely by
                whichever chunks ``ctx.source`` itself yields (e.g.
                ``CsvSource(..., chunk_size=...)``), never re-batched here
                -- see ``load()``'s docstring.
        """
        self._target_columns = target_columns
        self._chunk_size = chunk_size

    def load(
        self,
        ctx: PipelineContext,
        conn: Connection[Any],
        *,
        on_progress: Callable[[int, int], None] | None = None,
    ) -> StagingResult:
        """Stage ``ctx.source``'s rows into a clean ``staging.<dataset>__r<run_id>`` table.

        Never commits or rolls back ``conn`` itself (same ownership split as
        ``Publisher.publish``) -- the caller decides when the staged table
        becomes visible to other connections.

        One ``COPY`` runs per chunk ``ctx.source`` yields -- this method
        never re-batches rows by ``self._chunk_size``; chunk boundaries are
        entirely the ``Source``'s own concern.

        Args:
            ctx: The current pipeline context. ``ctx.source`` must be an
                opened-able ``Source`` (not ``None``); ``ctx.config.dataset``
                and ``ctx.run.run_id``/``.file_id``/``.batch_id`` name the
                staging table and populate every staged row's lineage
                columns.
            conn: An open connection this method issues DDL/``COPY``
                against. Never committed or rolled back here.
            on_progress: Invoked after every chunk's ``COPY`` completes, with
                the cumulative ``(rows_read, rows_parsed)`` staged so far as
                two positional ``int`` arguments. This is plan 04-05's
                ``run_ingest`` heartbeat mechanism: it keeps
                ``meta.ingestion_runs.rows_read``/``rows_parsed`` genuinely
                live during a long staging load (D-11), not left ``NULL``
                until ``finalize_publication``. Omitting it (the default,
                ``None``) changes no other behavior.

        Returns:
            A ``StagingResult`` describing the staged table and its row
            counts.

        Raises:
            ValueError: ``ctx.source`` is ``None``.
        """
        source = ctx.source
        if source is None:
            msg = "ctx.source is None; StagingLoader.load() requires PipelineContext.source"
            raise ValueError(msg)

        staging_table = f"staging.{ctx.config.dataset}__r{ctx.run.run_id}"
        log = get_logger()

        # C5 / Pitfall 2: every attempt begins with an idempotent undo of any
        # prior partial work, so a retry always starts clean regardless of
        # what a crashed prior attempt left behind.
        conn.execute(f"DROP TABLE IF EXISTS {staging_table}")
        # `target_columns` and `staging_table` are the only two dynamic SQL
        # fragments below, and both are IDENTIFIERS derived from config/run
        # identity (`ctx.config.dataset`, `ctx.run.run_id`, and the
        # dataset's configured column list) -- never from CSV row content
        # (T-04-01, this plan's threat model). Business columns are TEXT
        # (Pitfall 9: a COPY never fails on a bad date/number here -- that
        # becomes a later, set-based validation pass); lineage columns keep
        # their real types.
        business_columns_ddl = ", ".join(f"{column} text" for column in self._target_columns)
        lineage_columns_ddl = ", ".join(
            f"{column} {sql_type}" for column, sql_type in _LINEAGE_COLUMN_TYPES
        )
        conn.execute(
            f"CREATE UNLOGGED TABLE {staging_table} "
            f"({business_columns_ddl}, {lineage_columns_ddl})",
        )
        column_list = ", ".join((*self._target_columns, *_LINEAGE_COLUMN_NAMES))

        rows_read = 0
        rows_parsed = 0
        rows_rejected = 0
        next_source_row_number = 1

        with source.open(ctx) as stream:
            for chunk_ordinal, result in run_streaming(
                ctx,
                stream.chunks(),
                stages=[RaggedRowGuard()],
            ):
                surviving_rows = result.chunk.rows
                rows_in_chunk = len(surviving_rows)
                rows_read += rows_in_chunk + len(result.rejected)
                rows_rejected += len(result.rejected)

                enriched_rows: list[tuple[Any, ...]] = []
                for row in surviving_rows:
                    # C6 / Pitfall 10: computed exactly once, in Python, here
                    # -- canonical pipe-joined encoding, fixed column order
                    # from `target_columns`. Never recomputed in SQL at
                    # publish time.
                    record_hash = hashlib.sha256("|".join(row).encode("utf-8")).digest()
                    enriched_rows.append(
                        (
                            *row,
                            ctx.run.run_id,
                            ctx.run.file_id,
                            ctx.run.batch_id,
                            next_source_row_number,
                            record_hash,
                            1,  # _record_hash_version (META-02)
                        ),
                    )
                    next_source_row_number += 1

                with conn.cursor().copy(
                    f"COPY {staging_table} ({column_list}) FROM STDIN",
                ) as copy:
                    for enriched_row in enriched_rows:
                        copy.write_row(enriched_row)

                rows_parsed += len(enriched_rows)
                # PITFALLS B4: one log line per chunk -- silence during a
                # long COPY is indistinguishable from a hang.
                log.info(
                    "staging chunk copied",
                    dataset=ctx.config.dataset,
                    run_id=ctx.run.run_id,
                    chunk_ordinal=chunk_ordinal,
                    rows_in_chunk=rows_in_chunk,
                )
                if on_progress is not None:
                    on_progress(rows_read, rows_parsed)

        return StagingResult(
            staging_table=staging_table,
            rows_read=rows_read,
            rows_parsed=rows_parsed,
            rows_rejected=rows_rejected,
        )
